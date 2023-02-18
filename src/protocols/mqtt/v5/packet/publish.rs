use std::hash::{Hash, Hasher};
use std::io;
use std::sync::Arc;

use ahash::AHasher;
use futures_lite::io::AsyncWrite;
use mqtt_proto::{
    v5::{
        DisconnectReasonCode, Puback, PubackProperties, PubackReasonCode, Pubcomp,
        PubcompProperties, PubcompReasonCode, Publish, Pubrec, PubrecProperties, PubrecReasonCode,
        Pubrel, PubrelProperties, PubrelReasonCode,
    },
    QoS, QosPid,
};

use crate::state::GlobalState;

use super::super::Session;
use super::common::{send_error_disconnect, send_publish, write_packet, SendPublish};

#[inline]
pub(crate) async fn handle_publish<T: AsyncWrite + Unpin>(
    session: &mut Session,
    packet: Publish,
    conn: &mut T,
    global: &Arc<GlobalState>,
) -> io::Result<()> {
    log::debug!(
        r#"{} received a publish packet:
topic name : {}
   payload : {:?}
     flags : qos={:?}, retain={}, dup={}"#,
        session.client_id,
        packet.topic_name,
        packet.payload,
        packet.qos_pid,
        packet.retain,
        packet.dup,
    );

    if packet.topic_name.starts_with('$') {
        log::warn!(
            "publish to topic name start with '$' is not allowed: {}",
            packet.topic_name
        );
        send_error_disconnect(
            conn,
            session,
            DisconnectReasonCode::TopicNameInvalid,
            "publish to topic name start with '$' is not allowed",
        )
        .await?;
        return Ok(());
    }
    if packet.qos_pid == QosPid::Level0 && packet.dup {
        log::debug!("invalid dup flag in qos0 message");
        return Err(io::ErrorKind::InvalidData.into());
    }

    let properties = &packet.properties;
    let mut topic_name = packet.topic_name.clone();
    if let Some(alias) = properties.topic_alias {
        if alias == 0 || alias > global.config.topic_alias_max {
            send_error_disconnect(
                conn,
                session,
                DisconnectReasonCode::TopicAliasInvalid,
                "topic alias too large or is 0",
            )
            .await?;
            return Ok(());
        }
        if packet.topic_name.is_empty() {
            if let Some(name) = session.topic_aliases.get(&alias) {
                topic_name = name.clone();
            } else {
                send_error_disconnect(
                    conn,
                    session,
                    DisconnectReasonCode::ProtocolError,
                    "topic alias not found",
                )
                .await?;
                return Ok(());
            }
        } else {
            session
                .topic_aliases
                .insert(alias, packet.topic_name.clone());
        }
    }
    if properties.subscription_id.is_some() {
        send_error_disconnect(
            conn,
            session,
            DisconnectReasonCode::ProtocolError,
            "subscription identifier can't in publish",
        )
        .await?;
        return Ok(());
    }

    if let QosPid::Level2(pid) = packet.qos_pid {
        let mut hasher = AHasher::default();
        packet.hash(&mut hasher);
        let current_hash = hasher.finish();

        if let Some(previous_hash) = session.qos2_pids.get(&pid) {
            // hash collision is acceptable here, since u16 packet identifier is a small range
            if current_hash != *previous_hash {
                log::info!("packet identifier in use: {}", pid.value());
                let reason_code = PubrecReasonCode::PacketIdentifierInUse;
                let rv_packet = Pubrec {
                    pid,
                    reason_code,
                    properties: PubrecProperties::default(),
                };
                write_packet(session.client_id, conn, &rv_packet.into()).await?;
                return Ok(());
            }
            if !packet.dup {
                log::info!(
                    "dup flag must be true for re-deliver packet: {}",
                    pid.value()
                );
                send_error_disconnect(
                    conn,
                    session,
                    DisconnectReasonCode::ProtocolError,
                    "dup flag must be true for re-deliver packet",
                )
                .await?;
                return Ok(());
            }
        } else {
            session.qos2_pids.insert(pid, current_hash);
        }
    }

    let matched_len = send_publish(
        session,
        SendPublish {
            qos: packet.qos_pid.qos(),
            retain: packet.retain,
            topic_name: &topic_name,
            payload: &packet.payload,
            properties,
        },
        global,
    )
    .await;
    match packet.qos_pid {
        QosPid::Level0 => {}
        QosPid::Level1(pid) => {
            let reason_code = if matched_len > 0 {
                PubackReasonCode::Success
            } else {
                PubackReasonCode::NoMatchingSubscribers
            };
            let rv_packet = Puback {
                pid,
                reason_code,
                properties: PubackProperties::default(),
            };
            write_packet(session.client_id, conn, &rv_packet.into()).await?
        }
        QosPid::Level2(pid) => {
            let reason_code = if matched_len > 0 {
                PubrecReasonCode::Success
            } else {
                PubrecReasonCode::NoMatchingSubscribers
            };
            let rv_packet = Pubrec {
                pid,
                reason_code,
                properties: PubrecProperties::default(),
            };
            write_packet(session.client_id, conn, &rv_packet.into()).await?
        }
    }
    Ok(())
}

#[inline]
pub(crate) async fn handle_puback<T: AsyncWrite + Unpin>(
    session: &mut Session,
    packet: Puback,
    _conn: &mut T,
    _global: &Arc<GlobalState>,
) -> io::Result<()> {
    log::debug!(
        "{} received a puback packet: id={}",
        session.client_id,
        packet.pid.value(),
    );
    let _matched = session.pending_packets.complete(packet.pid, QoS::Level1);
    Ok(())
}

#[inline]
pub(crate) async fn handle_pubrec<T: AsyncWrite + Unpin>(
    session: &mut Session,
    packet: Pubrec,
    conn: &mut T,
    _global: &Arc<GlobalState>,
) -> io::Result<()> {
    log::debug!(
        "{} received a pubrec  packet: id={}",
        session.client_id,
        packet.pid.value()
    );
    let reason_code = if session.pending_packets.pubrec(packet.pid) {
        PubrelReasonCode::Success
    } else {
        PubrelReasonCode::PacketIdentifierNotFound
    };
    let rv_packet = Pubrel {
        pid: packet.pid,
        reason_code,
        properties: PubrelProperties::default(),
    };
    write_packet(session.client_id, conn, &rv_packet.into()).await?;
    Ok(())
}

#[inline]
pub(crate) async fn handle_pubrel<T: AsyncWrite + Unpin>(
    session: &mut Session,
    packet: Pubrel,
    conn: &mut T,
    _global: &Arc<GlobalState>,
) -> io::Result<()> {
    log::debug!(
        "{} received a pubrel  packet: id={}",
        session.client_id,
        packet.pid.value()
    );
    let reason_code = if session.qos2_pids.remove(&packet.pid).is_some() {
        PubcompReasonCode::Success
    } else {
        PubcompReasonCode::PacketIdentifierNotFound
    };
    let rv_packet = Pubcomp {
        pid: packet.pid,
        reason_code,
        properties: PubcompProperties::default(),
    };
    write_packet(session.client_id, conn, &rv_packet.into()).await?;
    Ok(())
}

#[inline]
pub(crate) async fn handle_pubcomp<T: AsyncWrite + Unpin>(
    session: &mut Session,
    packet: Pubcomp,
    _conn: &mut T,
    _global: &Arc<GlobalState>,
) -> io::Result<()> {
    log::debug!(
        "{} received a pubcomp packet: id={}",
        session.client_id,
        packet.pid.value()
    );
    let _matched = session.pending_packets.complete(packet.pid, QoS::Level2);
    Ok(())
}
