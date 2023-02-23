use std::borrow::Cow;
use std::cmp;
use std::io;
use std::ops::Deref;
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use futures_lite::io::AsyncWrite;
use mqtt_proto::{
    v5::{
        Connack, ConnackProperties, ConnectReasonCode, Disconnect, DisconnectProperties,
        DisconnectReasonCode, ErrorV5, Packet, Publish, PublishProperties, Pubrel,
        PubrelProperties, PubrelReasonCode,
    },
    QoS, QosPid, TopicFilter, TopicName, SHARED_PREFIX,
};
use rand::{thread_rng, Rng};

use crate::config::SharedSubscriptionMode;
use crate::protocols::mqtt::{get_unix_ts, PendingPacketStatus, RetainContent};
use crate::state::{ClientId, GlobalState, InternalMessage};

use super::super::{PubPacket, Session};

// TODO: move RecvPublish/SendPublish/recv_publish/send_publish to publish.rs

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

#[inline]
pub(crate) async fn after_handle_packet<T: AsyncWrite + Unpin>(
    session: &mut Session,
    conn: &mut T,
) -> io::Result<()> {
    *session.last_packet_time.write() = Instant::now();
    handle_pendings(session, conn).await?;
    Ok(())
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
