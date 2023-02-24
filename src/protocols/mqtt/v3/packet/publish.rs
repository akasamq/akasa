use std::cmp;
use std::hash::{Hash, Hasher};
use std::io;
use std::ops::Deref;
use std::sync::Arc;

use ahash::AHasher;
use bytes::Bytes;
use futures_lite::io::AsyncWrite;
use mqtt_proto::{
    v3::{Packet, Publish},
    Pid, QoS, QosPid, TopicFilter, TopicName,
};

use crate::protocols::mqtt::RetainContent;
use crate::state::{GlobalState, InternalMessage};

use super::super::{PubPacket, Session};
use super::common::{handle_pendings, write_packet};

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

    send_publish(
        session,
        SendPublish {
            topic_name: &packet.topic_name,
            retain: packet.retain,
            qos: packet.qos_pid.qos(),
            payload: &packet.payload,
        },
        global,
    )
    .await;
    match packet.qos_pid {
        QosPid::Level0 => {}
        QosPid::Level1(pid) => write_packet(session.client_id, conn, &Packet::Puback(pid)).await?,
        QosPid::Level2(pid) => write_packet(session.client_id, conn, &Packet::Pubrec(pid)).await?,
    }
    Ok(())
}

#[inline]
pub(crate) async fn handle_puback<T: AsyncWrite + Unpin>(
    session: &mut Session,
    pid: Pid,
    _conn: &mut T,
    _global: &Arc<GlobalState>,
) -> io::Result<()> {
    log::debug!(
        "{} received a puback packet: id={}",
        session.client_id,
        pid.value()
    );
    let _matched = session.pending_packets.complete(pid, QoS::Level1);
    Ok(())
}

#[inline]
pub(crate) async fn handle_pubrec<T: AsyncWrite + Unpin>(
    session: &mut Session,
    pid: Pid,
    conn: &mut T,
    _global: &Arc<GlobalState>,
) -> io::Result<()> {
    log::debug!(
        "{} received a pubrec  packet: id={}",
        session.client_id,
        pid.value()
    );

    let _matched = session.pending_packets.pubrec(pid);
    write_packet(session.client_id, conn, &Packet::Pubrel(pid)).await?;
    Ok(())
}

#[inline]
pub(crate) async fn handle_pubrel<T: AsyncWrite + Unpin>(
    session: &mut Session,
    pid: Pid,
    conn: &mut T,
    _global: &Arc<GlobalState>,
) -> io::Result<()> {
    log::debug!(
        "{} received a pubrel  packet: id={}",
        session.client_id,
        pid.value()
    );
    if session.qos2_pids.remove(&pid).is_none() {
        log::warn!("packet identifier not found: {}", pid.value());
        return Err(io::ErrorKind::InvalidData.into());
    }
    write_packet(session.client_id, conn, &Packet::Pubcomp(pid)).await?;
    Ok(())
}

#[inline]
pub(crate) async fn handle_pubcomp<T: AsyncWrite + Unpin>(
    session: &mut Session,
    pid: Pid,
    _conn: &mut T,
    _global: &Arc<GlobalState>,
) -> io::Result<()> {
    log::debug!(
        "{} received a pubcomp packet: id={}",
        session.client_id,
        pid.value()
    );
    let _matched = session.pending_packets.complete(pid, QoS::Level2);
    Ok(())
}

// ====================
// ==== Utils code ====
// ====================

pub(crate) struct SendPublish<'a> {
    pub topic_name: &'a TopicName,
    pub retain: bool,
    pub qos: QoS,
    pub payload: &'a Bytes,
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
pub(crate) async fn send_publish<'a>(
    session: &mut Session,
    msg: SendPublish<'a>,
    global: &Arc<GlobalState>,
) {
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
        for (client_id, subscribe_qos) in &content.clients {
            if let Some(sender) = global.get_client_sender(client_id) {
                let subscribe_filter = content.topic_filter.clone().unwrap();
                senders.push((*client_id, subscribe_filter, *subscribe_qos, sender));
            }
        }
    }

    for (receiver_client_id, subscribe_filter, subscribe_qos, sender) in senders {
        let publish = InternalMessage::PublishV3 {
            retain: msg.retain,
            qos: msg.qos,
            topic_name: msg.topic_name.clone(),
            payload: msg.payload.clone(),
            subscribe_filter,
            subscribe_qos,
        };
        if let Err(err) = sender.send_async((session.client_id, publish)).await {
            log::error!(
                "send publish to connection {} failed: {}",
                receiver_client_id,
                err
            );
        }
    }
}

// Got a publish message from retain message or subscribed topic, then send the publish message to client.
pub(crate) async fn recv_publish<'a, T: AsyncWrite + Unpin>(
    session: &mut Session,
    msg: RecvPublish<'a>,
    conn: Option<&mut T>,
) -> io::Result<()> {
    if !session.subscribes.contains_key(msg.subscribe_filter) {
        // the client already unsubscribed.
        return Ok(());
    }

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
        };
        write_packet(session.client_id, conn, &rv_packet.into()).await?;
    }

    Ok(())
}
