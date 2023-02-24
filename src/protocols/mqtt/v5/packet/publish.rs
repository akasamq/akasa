use std::cmp;
use std::hash::{Hash, Hasher};
use std::io;
use std::ops::Deref;
use std::sync::Arc;

use ahash::AHasher;
use bytes::Bytes;
use futures_lite::io::AsyncWrite;
use mqtt_proto::{
    v5::{
        DisconnectReasonCode, Puback, PubackProperties, PubackReasonCode, Pubcomp,
        PubcompProperties, PubcompReasonCode, Publish, PublishProperties, Pubrec, PubrecProperties,
        PubrecReasonCode, Pubrel, PubrelProperties, PubrelReasonCode,
    },
    QoS, QosPid, TopicFilter, TopicName, SHARED_PREFIX,
};
use rand::{thread_rng, Rng};

use crate::config::SharedSubscriptionMode;
use crate::protocols::mqtt::retain::RetainContent;
use crate::state::{GlobalState, InternalMessage};

use super::super::{PubPacket, Session};
use super::common::{handle_pendings, send_error_disconnect, write_packet};

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

// ====================
// ==== Utils code ====
// ====================

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

pub(crate) struct SendPublish<'a> {
    pub topic_name: &'a TopicName,
    pub retain: bool,
    pub qos: QoS,
    pub payload: &'a Bytes,
    pub properties: &'a PublishProperties,
}

// Received a publish message from client or will, then publish the message to
// matched clients, return the matched subscriptions length.
pub(crate) async fn send_publish<'a>(
    session: &mut Session,
    msg: SendPublish<'a>,
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
            if let Some(sender) = global.get_client_sender(client_id) {
                senders.push((*client_id, subscribe_filter.clone(), *subscribe_qos, sender));
            }
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
            let full_filter = TopicFilter::try_from(format!(
                "{}{}/{}",
                SHARED_PREFIX, group_name, subscribe_filter
            ))
            .expect("full topic filter");
            if let Some(sender) = global.get_client_sender(&client_id) {
                senders.push((client_id, full_filter, subscribe_qos, sender));
            }
        }
    }

    for (receiver_client_id, subscribe_filter, subscribe_qos, sender) in senders {
        let publish = InternalMessage::PublishV5 {
            retain: msg.retain,
            qos: msg.qos,
            topic_name: msg.topic_name.clone(),
            payload: msg.payload.clone(),
            subscribe_filter,
            subscribe_qos,
            properties: msg.properties.clone(),
        };
        if let Err(err) = sender.send_async((session.client_id, publish)).await {
            log::info!(
                "send publish to connection {} failed: {}",
                receiver_client_id,
                err
            );
        }
    }
    matched_len
}

// Got a publish message from retain message or subscribed topic, then send the publish message to client.
pub(crate) async fn recv_publish<'a, T: AsyncWrite + Unpin>(
    session: &mut Session,
    msg: RecvPublish<'a>,
    conn: Option<&mut T>,
) -> io::Result<()> {
    let subscription_id = if let Some(sub) = session.subscribes.get(msg.subscribe_filter) {
        sub.id
    } else {
        // the client already unsubscribed.
        return Ok(());
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
        if let Err(err) = session.pending_packets.push_back(
            pid,
            PubPacket {
                topic_name: msg.topic_name.clone(),
                qos: final_qos,
                retain: msg.retain,
                payload: msg.payload.clone(),
                properties,
            },
        ) {
            // TODO: proper handle this error
            log::warn!("push pending packets error: {}", err);
        }
        if let Some(conn) = conn {
            handle_pendings(session, conn).await?;
        }
    } else if let Some(conn) = conn {
        let rv_packet = Publish {
            dup: false,
            qos_pid: QosPid::Level0,
            retain: msg.retain,
            topic_name: msg.topic_name.clone(),
            payload: msg.payload.clone(),
            properties,
        };
        write_packet(session.client_id, conn, &rv_packet.into()).await?;
    }

    Ok(())
}
