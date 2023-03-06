use std::cmp;
use std::hash::{Hash, Hasher};
use std::io;
use std::ops::Deref;
use std::sync::Arc;

use ahash::AHasher;
use bytes::Bytes;
use mqtt_proto::{
    total_len,
    v3::{Packet, Publish},
    Encodable, Pid, QoS, QosPid, TopicFilter, TopicName,
};

use crate::protocols::mqtt::{BroadcastPackets, RetainContent};
use crate::state::{GlobalState, NormalMessage};

use super::super::{PubPacket, Session};

#[inline]
pub(crate) fn handle_publish(
    session: &mut Session,
    packet: Publish,
    global: &Arc<GlobalState>,
) -> io::Result<Option<Packet>> {
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
        log::debug!("invalid topic name: {}", packet.topic_name);
        return Err(io::ErrorKind::InvalidData.into());
    }
    if packet.qos_pid == QosPid::Level0 && packet.dup {
        log::debug!("invalid dup flag");
        return Err(io::ErrorKind::InvalidData.into());
    }

    if let QosPid::Level2(pid) = packet.qos_pid {
        let mut hasher = AHasher::default();
        packet.hash(&mut hasher);
        let current_hash = hasher.finish();

        if let Some(previous_hash) = session.qos2_pids.get(&pid) {
            // hash collision is acceptable here
            if current_hash != *previous_hash {
                log::info!("packet identifier in use: {}", pid.value());
                return Err(io::ErrorKind::InvalidData.into());
            }
            if !packet.dup {
                log::info!(
                    "dup flag must be true for re-deliver packet: {}",
                    pid.value()
                );
                return Err(io::ErrorKind::InvalidData.into());
            }
        } else {
            // FIXME: check qos2_pids limit
            session.qos2_pids.insert(pid, current_hash);
        }
    }

    let encode_len = total_len(packet.encode_len()).expect("packet too large");
    send_publish(
        session,
        SendPublish {
            topic_name: &packet.topic_name,
            retain: packet.retain,
            qos: packet.qos_pid.qos(),
            payload: &packet.payload,
            encode_len,
        },
        global,
    );
    match packet.qos_pid {
        QosPid::Level0 => Ok(None),
        QosPid::Level1(pid) => Ok(Some(Packet::Puback(pid))),
        QosPid::Level2(pid) => Ok(Some(Packet::Pubrec(pid))),
    }
}

#[inline]
pub(crate) fn handle_puback(session: &mut Session, pid: Pid) {
    log::debug!(
        "{} received a puback packet: id={}",
        session.client_id,
        pid.value()
    );
    let _matched = session.pending_packets.complete(pid, QoS::Level1);
}

#[inline]
pub(crate) fn handle_pubrec(session: &mut Session, pid: Pid) -> Packet {
    log::debug!(
        "{} received a pubrec  packet: id={}",
        session.client_id,
        pid.value()
    );

    let _matched = session.pending_packets.pubrec(pid);
    Packet::Pubrel(pid)
}

#[inline]
pub(crate) fn handle_pubrel(session: &mut Session, pid: Pid) -> io::Result<Packet> {
    log::debug!(
        "{} received a pubrel  packet: id={}",
        session.client_id,
        pid.value()
    );
    if session.qos2_pids.remove(&pid).is_none() {
        log::warn!("packet identifier not found: {}", pid.value());
        return Err(io::ErrorKind::InvalidData.into());
    }
    Ok(Packet::Pubcomp(pid))
}

#[inline]
pub(crate) fn handle_pubcomp(session: &mut Session, pid: Pid) {
    log::debug!(
        "{} received a pubcomp packet: id={}",
        session.client_id,
        pid.value()
    );
    let _matched = session.pending_packets.complete(pid, QoS::Level2);
}

// ====================
// ==== Utils code ====
// ====================

pub(crate) struct SendPublish<'a> {
    pub topic_name: &'a TopicName,
    pub retain: bool,
    pub qos: QoS,
    pub payload: &'a Bytes,
    pub encode_len: usize,
}

pub(crate) struct RecvPublish<'a> {
    pub topic_name: &'a TopicName,
    pub qos: QoS,
    pub retain: bool,
    pub payload: &'a Bytes,
    pub subscribe_filter: &'a TopicFilter,
    pub subscribe_qos: QoS,
}

// Received a publish message from client or will, then publish the message to matched clients
pub(crate) fn send_publish(session: &mut Session, msg: SendPublish, global: &Arc<GlobalState>) {
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
                None,
                msg.encode_len,
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

    let matches = global.route_table.get_matches(msg.topic_name);
    let mut senders = Vec::with_capacity(matches.len());
    for content in matches {
        let content = content.read();
        let subscribe_filter = content.topic_filter.as_ref().unwrap();
        for (client_id, subscribe_qos) in &content.clients {
            senders.push((*client_id, subscribe_filter.clone(), *subscribe_qos));
        }
    }

    session.broadcast_packets_cnt += senders.len();
    for (receiver_client_id, subscribe_filter, subscribe_qos) in senders {
        let publish = NormalMessage::PublishV3 {
            retain: msg.retain,
            qos: msg.qos,
            topic_name: msg.topic_name.clone(),
            payload: msg.payload.clone(),
            subscribe_filter,
            subscribe_qos,
            encode_len: msg.encode_len,
        };
        if !session.broadcast_packets.contains_key(&receiver_client_id) {
            if let Some(sender) = global.get_client_normal_sender(&receiver_client_id) {
                session.broadcast_packets.insert(
                    receiver_client_id,
                    BroadcastPackets {
                        sink: sender.into_sink(),
                        msgs: Default::default(),
                        flushed: true,
                    },
                );
            }
        }
        if let Some(info) = session.broadcast_packets.get_mut(&receiver_client_id) {
            info.msgs.push_back(publish);
        } else {
            session.broadcast_packets_cnt -= 1;
        }
    }
}

// Got a publish message from retain message or subscribed topic, then send the publish message to client.
pub(crate) fn recv_publish(
    session: &mut Session,
    msg: RecvPublish,
) -> Option<(QoS, Option<Packet>)> {
    if !session.subscribes.contains_key(msg.subscribe_filter) {
        // the client already unsubscribed.
        return None;
    }

    let final_qos = cmp::min(msg.qos, msg.subscribe_qos);
    if final_qos != QoS::Level0 {
        let pid = session.incr_server_packet_id();
        session.pending_packets.clean_complete();
        if session.pending_packets.push_back(
            pid,
            PubPacket {
                topic_name: msg.topic_name.clone(),
                qos: final_qos,
                retain: msg.retain,
                payload: msg.payload.clone(),
            },
        ) {
            // TODO: pending messages queue is full
        }
        Some((final_qos, None))
    } else if !session.disconnected {
        let rv_packet = Publish {
            dup: false,
            qos_pid: QosPid::Level0,
            retain: msg.retain,
            topic_name: msg.topic_name.clone(),
            payload: msg.payload.clone(),
        };
        Some((final_qos, Some(rv_packet.into())))
    } else {
        None
    }
}
