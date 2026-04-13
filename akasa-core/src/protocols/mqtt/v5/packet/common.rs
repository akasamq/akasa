use std::borrow::Cow;
use std::io;
use std::sync::Arc;
use std::time::Instant;

use hashbrown::HashMap;
use mqtt_proto::{
    v5::{
        Connack, ConnackProperties, ConnectReasonCode, Disconnect, DisconnectProperties,
        DisconnectReasonCode, ErrorV5, Packet, Publish, Pubrel, PubrelProperties, PubrelReasonCode,
    },
    QoS, QosPid, TopicName,
};
use tokio::io::AsyncWrite;

use crate::protocols::mqtt::{get_unix_ts, PendingPacketStatus};
use crate::state::ClientId;

use super::Session;

static EMPTY_TOPIC_NAME: std::sync::OnceLock<TopicName> = std::sync::OnceLock::new();

pub(super) fn empty_topic_name() -> &'static TopicName {
    EMPTY_TOPIC_NAME.get_or_init(|| TopicName::try_from("").expect("empty topic name is valid"))
}

#[inline]
pub(crate) fn after_handle_packet(session: &mut Session) -> Vec<Packet> {
    *session.last_packet_time.write() = Instant::now();
    handle_pendings(session)
}

#[inline]
pub(crate) fn build_error_connack<'a, R: Into<Cow<'a, str>>>(
    session: &'a mut Session,
    session_present: bool,
    reason_code: ConnectReasonCode,
    reason_string: R,
) -> Packet {
    let reason_string = if session.request_problem_info {
        Some(Arc::from(reason_string.into()))
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
        // NOTE: the reason string is given by server, so it's safe here.
        let encode_len = rv_packet
            .encode_len()
            .expect("connack packet size too large") as u32;
        if encode_len > session.max_packet_size {
            rv_packet = Connack {
                session_present,
                reason_code,
                properties: ConnackProperties::default(),
            }
            .into();
        }
    }
    session.client_disconnected = true;
    session.server_disconnected = true;
    rv_packet
}

#[inline]
pub(crate) fn build_error_disconnect<'a, R: Into<Cow<'a, str>>>(
    session: &'a mut Session,
    reason_code: DisconnectReasonCode,
    reason_string: R,
) -> Packet {
    let reason_string = if session.request_problem_info {
        Some(Arc::from(reason_string.into()))
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
        // NOTE: the reason string is given by server, so it's safe here.
        let encode_len = rv_packet
            .encode_len()
            .expect("disconnect packet size too large") as u32;
        if encode_len > session.max_packet_size {
            rv_packet = Disconnect {
                reason_code,
                properties: DisconnectProperties::default(),
            }
            .into();
        }
    }
    session.server_disconnected = true;
    rv_packet
}

pub(super) fn get_or_assign_server_topic_alias(
    topic_alias_max: u16,
    aliases_by_topic: &mut HashMap<TopicName, u16>,
    aliases_by_alias: &mut HashMap<u16, (TopicName, u64)>,
    tick: &mut u64,
    topic_name: &TopicName,
) -> (u16, bool) {
    if topic_alias_max == 0 {
        return (0, false);
    }

    *tick += 1;
    let current_tick = *tick;

    if let Some(&alias) = aliases_by_topic.get(topic_name) {
        if let Some(entry) = aliases_by_alias.get_mut(&alias) {
            entry.1 = current_tick;
        }
        return (alias, false);
    }

    let alias = if aliases_by_alias.len() < topic_alias_max as usize {
        (1..=topic_alias_max)
            .find(|a| !aliases_by_alias.contains_key(a))
            .unwrap_or(1)
    } else {
        aliases_by_alias
            .iter()
            .min_by_key(|(_, (_, t))| *t)
            .map(|(&a, _)| a)
            .unwrap_or(1)
    };

    if let Some((previous_topic, _)) = aliases_by_alias.remove(&alias) {
        aliases_by_topic.remove(&previous_topic);
    }
    aliases_by_alias.insert(alias, (topic_name.clone(), current_tick));
    aliases_by_topic.insert(topic_name.clone(), alias);

    (alias, true)
}

#[inline]
pub(crate) fn handle_pendings(session: &mut Session) -> Vec<Packet> {
    let topic_alias_max = session.topic_alias_max;
    let (pending_packets, aliases_by_topic, aliases_by_alias, tick) = (
        &mut session.pending_packets,
        &mut session.server_topic_aliases_by_topic,
        &mut session.server_topic_aliases_by_alias,
        &mut session.server_alias_tick,
    );

    pending_packets.clean_complete();
    let mut packets = Vec::new();
    let mut expired_packets = Vec::new();
    let mut start_idx = 0;
    while let Some((idx, packet_status)) = pending_packets.get_ready_packet(start_idx) {
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
                let now_ts = get_unix_ts();
                let mut message_expiry_interval = None;
                if let Some(value) = packet.properties.message_expiry_interval {
                    let passed_secs = now_ts - *added_at;
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
                let mut topic_name = packet.topic_name.clone();
                properties.message_expiry_interval = message_expiry_interval;
                if !*dup && topic_alias_max > 0 {
                    let (alias, is_new_mapping) = get_or_assign_server_topic_alias(
                        topic_alias_max,
                        aliases_by_topic,
                        aliases_by_alias,
                        tick,
                        &topic_name,
                    );
                    if alias > 0 {
                        properties.topic_alias = Some(alias);
                        if !is_new_mapping {
                            topic_name = empty_topic_name().clone();
                        }
                    }
                }
                let rv_packet = Publish {
                    dup: *dup,
                    retain: packet.retain,
                    qos_pid,
                    topic_name,
                    payload: packet.payload.clone(),
                    properties,
                };
                *dup = true;
                *last_sent = now_ts;
                packets.push(rv_packet.into());
            }
            PendingPacketStatus::Pubrec { pid, last_sent, .. } => {
                let rv_packet = Pubrel {
                    pid: *pid,
                    reason_code: PubrelReasonCode::Success,
                    properties: PubrelProperties::default(),
                };
                *last_sent = get_unix_ts();
                packets.push(rv_packet.into());
            }
            PendingPacketStatus::Complete => unreachable!(),
        }
    }
    for pid in &expired_packets {
        // If the QoS2 message exipred, it's MUST also treated as QoS1 message
        pending_packets.complete(*pid, QoS::Level1);
    }
    if !expired_packets.is_empty() {
        pending_packets.clean_complete();
    }
    packets
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
