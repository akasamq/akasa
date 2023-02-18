use std::cmp;
use std::io;
use std::ops::Deref;
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use futures_lite::io::AsyncWrite;
use mqtt_proto::{
    v3::{Packet, Publish},
    QoS, QosPid, TopicFilter, TopicName,
};

use crate::protocols::mqtt::{PendingPacketStatus, RetainContent};
use crate::state::{ClientId, GlobalState, InternalMessage};

use super::super::{PubPacket, Session};

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

#[inline]
pub(crate) async fn after_handle_packet<T: AsyncWrite + Unpin>(
    session: &mut Session,
    conn: &mut T,
) -> io::Result<()> {
    *session.last_packet_time.write() = Instant::now();
    handle_pendings(session, conn).await?;
    Ok(())
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

    for (sender_client_id, subscribe_filter, subscribe_qos, sender) in senders {
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
                sender_client_id,
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

#[inline]
pub(super) async fn write_packet<T: AsyncWrite + Unpin>(
    client_id: ClientId,
    conn: &mut T,
    packet: &Packet,
) -> io::Result<()> {
    log::debug!("write to {:?} with packet: {:#?}", client_id, packet);
    packet.encode_async(conn).await?;
    Ok(())
}

#[inline]
pub(super) async fn handle_pendings<T: AsyncWrite + Unpin>(
    session: &mut Session,
    conn: &mut T,
) -> io::Result<()> {
    session.pending_packets.clean_complete();
    let mut start_idx = 0;
    while let Some((idx, packet_status)) = session.pending_packets.get_ready_packet(start_idx) {
        start_idx = idx + 1;
        match packet_status {
            PendingPacketStatus::New {
                dup, pid, packet, ..
            } => {
                let qos_pid = match packet.qos {
                    QoS::Level0 => QosPid::Level0,
                    QoS::Level1 => QosPid::Level1(*pid),
                    QoS::Level2 => QosPid::Level2(*pid),
                };
                let rv_packet = Publish {
                    dup: *dup,
                    retain: packet.retain,
                    qos_pid,
                    topic_name: packet.topic_name.clone(),
                    payload: packet.payload.clone(),
                };
                *dup = true;
                write_packet(session.client_id, conn, &rv_packet.into()).await?;
            }
            PendingPacketStatus::Pubrec { pid, .. } => {
                write_packet(session.client_id, conn, &Packet::Pubrel(*pid)).await?;
            }
            PendingPacketStatus::Complete => unreachable!(),
        }
    }
    Ok(())
}
