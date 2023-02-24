use std::borrow::Cow;
use std::io;
use std::sync::Arc;
use std::time::Instant;

use futures_lite::io::AsyncWrite;
use mqtt_proto::{
    v5::{
        Connack, ConnackProperties, ConnectReasonCode, Disconnect, DisconnectProperties,
        DisconnectReasonCode, ErrorV5, Packet, Publish, Pubrel, PubrelProperties, PubrelReasonCode,
    },
    QoS, QosPid,
};

use crate::protocols::mqtt::{get_unix_ts, PendingPacketStatus};
use crate::state::ClientId;

use super::super::Session;

// TODO: move RecvPublish/SendPublish/recv_publish/send_publish to publish.rs
#[inline]
pub(crate) async fn after_handle_packet<T: AsyncWrite + Unpin>(
    session: &mut Session,
    conn: &mut T,
) -> io::Result<()> {
    *session.last_packet_time.write() = Instant::now();
    handle_pendings(session, conn).await?;
    Ok(())
}

#[inline]
pub(crate) async fn send_error_connack<'a, T: AsyncWrite + Unpin, R: Into<Cow<'a, str>>>(
    conn: &'a mut T,
    session: &'a mut Session,
    session_present: bool,
    reason_code: ConnectReasonCode,
    reason_string: R,
) -> io::Result<()> {
    let reason_string = if session.request_problem_info {
        Some(Arc::new(reason_string.into().into_owned()))
    } else {
        None
    };
    let mut rv_packet: Packet = Connack {
        session_present,
        reason_code,
        properties: ConnackProperties {
            reason_string,
            ..Default::default()
        },
    }
    .into();
    if session.request_problem_info {
        let encode_len = rv_packet.encode_len().map_err(|_| {
            log::warn!("connack packet size too large");
            io::Error::from(io::ErrorKind::InvalidData)
        })? as u32;
        if encode_len > session.max_packet_size {
            rv_packet = Connack {
                session_present,
                reason_code,
                properties: ConnackProperties::default(),
            }
            .into();
        }
    }
    write_packet(session.client_id, conn, &rv_packet).await?;
    session.disconnected = true;
    Ok(())
}

#[inline]
pub(crate) async fn send_error_disconnect<'a, T: AsyncWrite + Unpin, R: Into<Cow<'a, str>>>(
    conn: &'a mut T,
    session: &'a mut Session,
    reason_code: DisconnectReasonCode,
    reason_string: R,
) -> io::Result<()> {
    let reason_string = if session.request_problem_info {
        Some(Arc::new(reason_string.into().into_owned()))
    } else {
        None
    };
    let mut rv_packet: Packet = Disconnect {
        reason_code,
        properties: DisconnectProperties {
            reason_string,
            ..Default::default()
        },
    }
    .into();
    if session.request_problem_info {
        let encode_len = rv_packet.encode_len().map_err(|_| {
            // not likely to happen
            log::warn!("disconnect packet too large");
            io::Error::from(io::ErrorKind::InvalidData)
        })? as u32;
        if encode_len > session.max_packet_size {
            rv_packet = Disconnect {
                reason_code,
                properties: DisconnectProperties::default(),
            }
            .into();
        }
    }
    write_packet(session.client_id, conn, &rv_packet).await?;
    session.disconnected = true;
    Ok(())
}

#[inline]
pub(crate) async fn handle_pendings<T: AsyncWrite + Unpin>(
    session: &mut Session,
    conn: &mut T,
) -> io::Result<()> {
    session.pending_packets.clean_complete();
    let mut expired_packets = Vec::new();
    let mut start_idx = 0;
    while let Some((idx, packet_status)) = session.pending_packets.get_ready_packet(start_idx) {
        start_idx = idx + 1;
        match packet_status {
            PendingPacketStatus::New {
                added_at,
                last_sent,
                dup,
                pid,
                packet,
                ..
            } => {
                let mut message_expiry_interval = None;
                if let Some(value) = packet.properties.message_expiry_interval {
                    let passed_secs = get_unix_ts() - *added_at;
                    if *last_sent == 0 && passed_secs >= value as u64 {
                        expired_packets.push(*pid);
                        continue;
                    }
                    message_expiry_interval = Some(value.saturating_sub(passed_secs as u32));
                }
                let qos_pid = match packet.qos {
                    QoS::Level0 => QosPid::Level0,
                    QoS::Level1 => QosPid::Level1(*pid),
                    QoS::Level2 => QosPid::Level2(*pid),
                };
                let mut properties = packet.properties.clone();
                properties.message_expiry_interval = message_expiry_interval;
                let rv_packet = Publish {
                    dup: *dup,
                    retain: packet.retain,
                    qos_pid,
                    topic_name: packet.topic_name.clone(),
                    payload: packet.payload.clone(),
                    properties,
                };
                *dup = true;
                write_packet(session.client_id, conn, &rv_packet.into()).await?;
            }
            PendingPacketStatus::Pubrec { pid, .. } => {
                let rv_packet = Pubrel {
                    pid: *pid,
                    reason_code: PubrelReasonCode::Success,
                    properties: PubrelProperties::default(),
                };
                write_packet(session.client_id, conn, &rv_packet.into()).await?;
            }
            PendingPacketStatus::Complete => unreachable!(),
        }
    }
    for pid in &expired_packets {
        // If the QoS2 message exipred, it's MUST also treated as QoS1 message
        session.pending_packets.complete(*pid, QoS::Level1);
    }
    if !expired_packets.is_empty() {
        session.pending_packets.clean_complete();
    }
    Ok(())
}

#[inline]
pub(crate) async fn write_packet<T: AsyncWrite + Unpin>(
    client_id: ClientId,
    conn: &mut T,
    packet: &Packet,
) -> io::Result<()> {
    log::debug!("write to {} with packet: {:#?}", client_id, packet);
    packet.encode_async(conn).await.map_err(|err| match err {
        ErrorV5::Common(err) => io::Error::from(err),
        _ => io::ErrorKind::InvalidData.into(),
    })?;
    Ok(())
}
