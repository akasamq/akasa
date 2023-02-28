use std::cmp;
use std::hash::{Hash, Hasher};
use std::ops::Deref;
use std::sync::Arc;

use ahash::AHasher;
use bytes::Bytes;
use mqtt_proto::{
    v5::{
        DisconnectReasonCode, Packet, Puback, PubackProperties, PubackReasonCode, Pubcomp,
        PubcompProperties, PubcompReasonCode, Publish, PublishProperties, Pubrec, PubrecProperties,
        PubrecReasonCode, Pubrel, PubrelProperties, PubrelReasonCode,
    },
    QoS, QosPid, TopicFilter, TopicName, SHARED_PREFIX,
};
use rand::{thread_rng, Rng};

use crate::config::SharedSubscriptionMode;
use crate::protocols::mqtt::retain::RetainContent;
use crate::state::{GlobalState, NormalMessage};

use super::super::{PubPacket, Session};
use super::common::build_error_disconnect;

#[inline]
pub(crate) fn handle_publish(
    session: &mut Session,
    packet: Publish,
    global: &Arc<GlobalState>,
) -> Result<Option<Packet>, Packet> {
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
        let err_pkt = build_error_disconnect(
            session,
            DisconnectReasonCode::TopicNameInvalid,
            "publish to topic name start with '$' is not allowed",
        );
        return Err(err_pkt);
    }
    if packet.qos_pid == QosPid::Level0 && packet.dup {
        log::debug!("invalid dup flag in qos0 message");
        let err_pkt = build_error_disconnect(
            session,
            DisconnectReasonCode::ProtocolError,
            "invalid dup flag in qos0 message",
        );
        return Err(err_pkt);
    }

    let properties = &packet.properties;
    let mut topic_name = packet.topic_name.clone();
    if let Some(alias) = properties.topic_alias {
        if alias == 0 || alias > global.config.topic_alias_max {
            let err_pkt = build_error_disconnect(
                session,
                DisconnectReasonCode::TopicAliasInvalid,
                "topic alias too large or is 0",
            );
            return Err(err_pkt);
        }
        if packet.topic_name.is_empty() {
            if let Some(name) = session.topic_aliases.get(&alias) {
                topic_name = name.clone();
            } else {
                let err_pkt = build_error_disconnect(
                    session,
                    DisconnectReasonCode::ProtocolError,
                    "topic alias not found",
                );
                return Err(err_pkt);
            }
        } else {
            session
                .topic_aliases
                .insert(alias, packet.topic_name.clone());
        }
    }
    if properties.subscription_id.is_some() {
        let err_pkt = build_error_disconnect(
            session,
            DisconnectReasonCode::ProtocolError,
            "subscription identifier can't in publish",
        );
        return Err(err_pkt);
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
                return Ok(Some(rv_packet.into()));
            }
            if !packet.dup {
                log::info!(
                    "dup flag must be true for re-deliver packet: {}",
                    pid.value()
                );
                let err_pkt = build_error_disconnect(
                    session,
                    DisconnectReasonCode::ProtocolError,
                    "dup flag must be true for re-deliver packet",
                );
                return Err(err_pkt);
            }
        } else {
            // FIXME: check qos2_pids limit
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
    );
    match packet.qos_pid {
        QosPid::Level0 => Ok(None),
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
            Ok(Some(rv_packet.into()))
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
            Ok(Some(rv_packet.into()))
        }
    }
}

#[inline]
pub(crate) fn handle_puback(session: &mut Session, packet: Puback) {
    log::debug!(
        "{} received a puback packet: id={}",
        session.client_id,
        packet.pid.value(),
    );
    let _matched = session.pending_packets.complete(packet.pid, QoS::Level1);
}

#[inline]
pub(crate) fn handle_pubrec(session: &mut Session, packet: Pubrec) -> Packet {
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
    Pubrel {
        pid: packet.pid,
        reason_code,
        properties: PubrelProperties::default(),
    }
    .into()
}

#[inline]
pub(crate) fn handle_pubrel(session: &mut Session, packet: Pubrel) -> Packet {
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
    Pubcomp {
        pid: packet.pid,
        reason_code,
        properties: PubcompProperties::default(),
    }
    .into()
}

#[inline]
pub(crate) fn handle_pubcomp(session: &mut Session, packet: Pubcomp) {
    log::debug!(
        "{} received a pubcomp packet: id={}",
        session.client_id,
        packet.pid.value()
    );
    let _matched = session.pending_packets.complete(packet.pid, QoS::Level2);
}

// ====================
// ==== Utils code ====
// ====================

#[derive(Debug)]
pub(crate) struct RecvPublish<'a> {
    pub topic_name: &'a TopicName,
    pub qos: QoS,
    pub retain: bool,
    pub payload: &'a Bytes,
    // None for v3 publish packet
    pub properties: Option<&'a PublishProperties>,
    pub subscribe_filter: &'a TopicFilter,
    // [MQTTv5.0-3.8.4] keyword: downgraded
    pub subscribe_qos: QoS,
}

#[derive(Debug)]
pub(crate) struct SendPublish<'a> {
    pub topic_name: &'a TopicName,
    pub retain: bool,
    pub qos: QoS,
    pub payload: &'a Bytes,
    pub properties: &'a PublishProperties,
}

// TODO: change to broadcast_publish()
// matched clients, return the matched subscriptions length.
pub(crate) fn send_publish(
    session: &mut Session,
    msg: SendPublish,
    global: &Arc<GlobalState>,
) -> usize {
    if msg.retain {
        if let Some(old_content) = if msg.payload.is_empty() {
            log::debug!("retain message removed");
            global.retain_table.remove(msg.topic_name)
        } else {
            let content = Arc::new(RetainContent::new(
                session.client_identifier.clone(),
                msg.qos,
                msg.topic_name.clone(),
                msg.payload.clone(),
                Some(msg.properties.clone()),
            ));
            log::debug!("retain message inserted");
            global.retain_table.insert(content)
        } {
            log::debug!(
                r#"old retain content:
 client identifier : {}
        topic name : {}
           payload : {:?}
               qos : {:?}"#,
                old_content.client_identifier,
                &old_content.topic_name.deref().deref(),
                old_content.payload.as_ref(),
                old_content.qos,
            );
        }
    }

    // TODO: enable subscription identifier list by config.
    //   It is also an opinioned optimization.

    let matches = global.route_table.get_matches(msg.topic_name);
    let matched_len = matches.len();
    let mut senders = Vec::with_capacity(matched_len);
    for content in matches {
        let content = content.read();
        let subscribe_filter = content.topic_filter.as_ref().unwrap();
        for (client_id, subscribe_qos) in &content.clients {
            if *client_id == session.client_id {
                if let Some(sub) = session.subscribes.get(subscribe_filter) {
                    if sub.options.no_local {
                        continue;
                    }
                } else {
                    // already unsubscribed
                    continue;
                }
            }
            senders.push((*client_id, subscribe_filter.clone(), *subscribe_qos));
        }
        for (group_name, shared_clients) in &content.groups {
            let (client_id, subscribe_qos) = match global.config.shared_subscription_mode {
                SharedSubscriptionMode::Random => shared_clients.get_by_number(thread_rng().gen()),
                SharedSubscriptionMode::HashClientId => {
                    shared_clients.get_by_hash(&session.client_identifier)
                }
                SharedSubscriptionMode::HashTopicName => shared_clients.get_by_hash(msg.topic_name),
            };
            // TODO: optimize this alloc later
            let full_filter =
                TopicFilter::try_from(format!("{SHARED_PREFIX}{group_name}/{subscribe_filter}"))
                    .expect("full topic filter");
            senders.push((client_id, full_filter, subscribe_qos));
        }
    }

    session.broadcast_packets_cnt += senders.len();
    for (receiver_client_id, subscribe_filter, subscribe_qos) in senders {
        let publish = NormalMessage::PublishV5 {
            retain: msg.retain,
            qos: msg.qos,
            topic_name: msg.topic_name.clone(),
            payload: msg.payload.clone(),
            subscribe_filter,
            subscribe_qos,
            properties: msg.properties.clone(),
        };
        session
            .broadcast_packets
            .entry(receiver_client_id)
            .or_default()
            .push_back(publish);
    }
    matched_len
}

// Got a publish message from retain message or subscribed topic, then send the publish message to client.
pub(crate) fn recv_publish(
    session: &mut Session,
    msg: RecvPublish,
) -> Option<(QoS, Option<Packet>)> {
    let subscription_id = if let Some(sub) = session.subscribes.get(msg.subscribe_filter) {
        sub.id
    } else {
        // the client already unsubscribed.
        return None;
    };

    // TODO: detect costly topic name and enable topic alias
    // TODO: support multiple subscription identifiers
    //       (shared subscription not support multiple subscription identifiers)

    let mut properties = msg.properties.cloned().unwrap_or_default();
    properties.topic_alias = None;
    properties.subscription_id = subscription_id;

    let final_qos = cmp::min(msg.qos, msg.subscribe_qos);
    if final_qos != QoS::Level0 {
        let pid = session.incr_server_packet_id();
        session.pending_packets.clean_complete();
        // TODO: proper handle this error
        let _is_full = session.pending_packets.push_back(
            pid,
            PubPacket {
                topic_name: msg.topic_name.clone(),
                qos: final_qos,
                retain: msg.retain,
                payload: msg.payload.clone(),
                properties,
            },
        );
        Some((final_qos, None))
    } else if !session.disconnected() {
        let rv_packet = Publish {
            dup: false,
            qos_pid: QosPid::Level0,
            retain: msg.retain,
            topic_name: msg.topic_name.clone(),
            payload: msg.payload.clone(),
            properties,
        };
        Some((final_qos, Some(rv_packet.into())))
    } else {
        None
    }
}
