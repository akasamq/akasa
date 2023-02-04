use std::cmp;
use std::io;
use std::mem;
use std::net::SocketAddr;
use std::ops::Deref;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use flume::Receiver;
use futures_lite::io::{AsyncRead, AsyncWrite};
use mqtt_proto::{
    v3::{Connack, Connect, ConnectReturnCode, Packet, Publish, Suback, Subscribe, Unsubscribe},
    Pid, Protocol, QoS, QosPid, TopicFilter, TopicName,
};

use crate::config::AuthType;
use crate::state::{AddClientReceipt, ClientId, Executor, GlobalState, InternalMessage};

use super::super::{PendingPacketStatus, PendingPackets, RetainContent};
use super::{PubPacket, Session, SessionState, Will};

pub async fn handle_connection<T: AsyncRead + AsyncWrite + Unpin, E: Executor>(
    session: &mut Session,
    receiver: &mut Option<Receiver<(ClientId, InternalMessage)>>,
    conn: &mut T,
    peer: &SocketAddr,
    executor: &E,
    global: &Arc<GlobalState>,
) -> io::Result<()> {
    let packet = match Packet::decode_async(conn).await {
        Ok(packet) => packet,
        Err(err) => {
            if err.is_eof() {
                if !session.disconnected {
                    return Err(io::Error::from(io::ErrorKind::UnexpectedEof));
                } else {
                    return Ok(());
                }
            } else {
                return Err(io::Error::from(io::ErrorKind::InvalidData));
            }
        }
    };
    if !matches!(packet, Packet::Connect(..)) && !session.connected {
        log::info!("{} not connected", peer);
        return Err(io::Error::from(io::ErrorKind::InvalidData));
    }
    match packet {
        Packet::Connect(pkt) => {
            handle_connect(session, receiver, pkt, conn, peer, executor, global).await?;
        }
        Packet::Disconnect => handle_disconnect(session, conn, global).await?,
        Packet::Publish(pkt) => handle_publish(session, pkt, conn, global).await?,
        Packet::Puback(pid) => handle_puback(session, pid, conn, global).await?,
        Packet::Pubrec(pid) => handle_pubrec(session, pid, conn, global).await?,
        Packet::Pubrel(pid) => handle_pubrel(session, pid, conn, global).await?,
        Packet::Pubcomp(pid) => handle_pubcomp(session, pid, conn, global).await?,
        Packet::Subscribe(pkt) => handle_subscribe(session, pkt, conn, global).await?,
        Packet::Unsubscribe(packet) => handle_unsubscribe(session, packet, conn, global).await?,
        Packet::Pingreq => handle_pingreq(session, conn, global).await?,
        _ => return Err(io::Error::from(io::ErrorKind::InvalidData)),
    }
    *session.last_packet_time.write() = Instant::now();

    handle_pendings(session, conn).await?;
    Ok(())
}

#[inline]
async fn handle_connect<T: AsyncWrite + Unpin, E: Executor>(
    session: &mut Session,
    receiver: &mut Option<Receiver<(ClientId, InternalMessage)>>,
    packet: Connect,
    conn: &mut T,
    peer: &SocketAddr,
    executor: &E,
    global: &Arc<GlobalState>,
) -> io::Result<()> {
    log::debug!(
        r#"{} received a connect packet:
     protocol : {}
    client_id : {}
clean session : {}
     username : {:?}
     password : {:?}
   keep-alive : {}s
         will : {:?}"#,
        peer,
        packet.protocol,
        packet.client_id,
        packet.clean_session,
        packet.username,
        packet.password,
        packet.keep_alive,
        packet.last_will,
    );
    if packet.protocol != Protocol::V310 && packet.protocol != Protocol::V311 {
        let rv_packet = Connack::new(false, ConnectReturnCode::UnacceptableProtocolVersion);
        write_packet(session.client_id, conn, &rv_packet.into()).await?;
        session.disconnected = true;
        return Ok(());
    }

    if packet.protocol == Protocol::V310
        && (packet.client_id.is_empty() || packet.client_id.len() > 23)
    {
        let rv_packet = Connack::new(false, ConnectReturnCode::IdentifierRejected);
        write_packet(session.client_id, conn, &rv_packet.into()).await?;
        session.disconnected = true;
        return Ok(());
    }

    let mut return_code = ConnectReturnCode::Accepted;
    // FIXME: auth by plugin
    for auth_type in &global.config.auth_types {
        match auth_type {
            AuthType::UsernamePassword => {
                if let Some(username) = packet.username.as_ref() {
                    if global
                        .config
                        .users
                        .get(username.as_str())
                        .map(|s| s.as_bytes())
                        != packet.password.as_ref().map(|s| s.as_ref())
                    {
                        log::debug!("incorrect password for user: {}", username);
                        return_code = ConnectReturnCode::BadUsernameOrPassword;
                    }
                } else {
                    log::debug!("username password not set for client: {}", packet.client_id);
                    return_code = ConnectReturnCode::BadUsernameOrPassword;
                }
            }
            _ => panic!("auth method not supported: {:?}", auth_type),
        }
    }
    // FIXME: permission check and return "not authorized"
    if return_code != ConnectReturnCode::Accepted {
        let rv_packet = Connack::new(false, return_code);
        write_packet(session.client_id, conn, &rv_packet.into()).await?;
        session.disconnected = true;
        return Ok(());
    }

    // FIXME: if connection reach rate limit return "Server unavailable"

    session.protocol = packet.protocol;
    session.clean_session = packet.clean_session;
    session.client_identifier = if packet.client_id.is_empty() {
        Arc::new(uuid::Uuid::new_v4().to_string())
    } else {
        Arc::clone(&packet.client_id)
    };
    session.username = packet.username.map(|name| Arc::clone(&name));
    session.keep_alive = packet.keep_alive;

    // FIXME: if kee_alive is zero, set a default keep_alive value from config
    if session.keep_alive > 0 {
        let interval = Duration::from_millis(session.keep_alive as u64 * 500);
        let client_id = session.client_id;
        log::debug!("{} keep alive: {:?}", client_id, interval * 2);
        let last_packet_time = Arc::clone(&session.last_packet_time);
        let global = Arc::clone(global);
        if let Err(err) = executor.spawn_timer(move || {
            // Need clone twice: https://stackoverflow.com/a/68462908/1274372
            let last_packet_time = Arc::clone(&last_packet_time);
            let global = Arc::clone(&global);
            async move {
                {
                    let last_packet_time = last_packet_time.read();
                    if last_packet_time.elapsed() <= interval * 2 {
                        return Some(interval);
                    }
                }
                // timeout, kick it out
                if let Some(sender) = global.get_client_sender(&client_id) {
                    let msg = InternalMessage::Kick {
                        reason: "timeout".to_owned(),
                    };
                    if let Err(err) = sender.send_async((client_id, msg)).await {
                        log::warn!(
                            "send timeout kick message to {:?} error: {:?}",
                            client_id,
                            err
                        );
                    }
                }
                None
            }
        }) {
            log::warn!("spawn executor timer failed: {:?}", err);
            return Err(err);
        }
    }

    if let Some(last_will) = packet.last_will {
        if last_will.topic_name.starts_with('$') {
            return Err(io::Error::from(io::ErrorKind::InvalidData));
        }
        let will = Will {
            retain: last_will.retain,
            qos: last_will.qos,
            topic: last_will.topic_name.clone(),
            message: last_will.message,
        };
        session.will = Some(will);
    }

    let mut session_present = false;
    match global.add_client(session.client_identifier.as_str()).await {
        AddClientReceipt::Present(old_state) => {
            log::debug!("Got exists session for {}", old_state.client_id);
            session.client_id = old_state.client_id;
            *receiver = Some(old_state.receiver);
            // TODO: if protocol level is compatiable, copy the session state?
            if !session.clean_session && session.protocol == old_state.protocol {
                session.server_packet_id = old_state.server_packet_id;
                session.pending_packets = old_state.pending_packets;
                session.subscribes = old_state.subscribes;
                session_present = true;
            } else {
                log::info!(
                    "{} session state removed due to reconnect with a different protocol version, new: {}, old: {}, or clean session: {}",
                    old_state.pending_packets.len(),
                    session.protocol,
                    old_state.protocol,
                    session.clean_session,
                );
                session_present = false;
            }
        }
        AddClientReceipt::New {
            client_id,
            receiver: new_receiver,
        } => {
            log::debug!("Create new session for {}", client_id);
            session.client_id = client_id;
            *receiver = Some(new_receiver);
        }
    }

    log::debug!("Socket {} assgined to: {}", peer, session.client_id);

    let rv_packet = Connack::new(session_present, return_code);
    write_packet(session.client_id, conn, &rv_packet.into()).await?;
    session.connected = true;
    Ok(())
}

#[inline]
async fn handle_disconnect<T: AsyncWrite + Unpin>(
    session: &mut Session,
    _conn: &mut T,
    _global: &Arc<GlobalState>,
) -> io::Result<()> {
    log::debug!("{} received a disconnect packet", session.client_id);
    session.will = None;
    session.disconnected = true;
    Ok(())
}

#[inline]
async fn handle_publish<T: AsyncWrite + Unpin>(
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
        return Err(io::Error::from(io::ErrorKind::InvalidData));
    }
    if packet.qos_pid == QosPid::Level0 && packet.dup {
        log::debug!("invalid dup flag");
        return Err(io::Error::from(io::ErrorKind::InvalidData));
    }
    // FIXME: handle dup flag
    send_publish(
        session,
        &packet.topic_name,
        packet.retain,
        packet.qos_pid.qos(),
        &packet.payload,
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
async fn handle_puback<T: AsyncWrite + Unpin>(
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
    session.pending_packets.complete(pid);
    Ok(())
}

#[inline]
async fn handle_pubrec<T: AsyncWrite + Unpin>(
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
    session.pending_packets.pubrec(pid);
    write_packet(session.client_id, conn, &Packet::Pubrel(pid)).await?;
    Ok(())
}

#[inline]
async fn handle_pubrel<T: AsyncWrite + Unpin>(
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
    write_packet(session.client_id, conn, &Packet::Pubcomp(pid)).await?;
    Ok(())
}

#[inline]
async fn handle_pubcomp<T: AsyncWrite + Unpin>(
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
    session.pending_packets.complete(pid);
    Ok(())
}

#[inline]
async fn handle_subscribe<T: AsyncWrite + Unpin>(
    session: &mut Session,
    packet: Subscribe,
    conn: &mut T,
    global: &Arc<GlobalState>,
) -> io::Result<()> {
    log::debug!(
        r#"{} received a subscribe packet:
packet id : {}
   topics : {:?}"#,
        session.client_id,
        packet.pid.value(),
        packet.topics,
    );
    let mut return_codes = Vec::with_capacity(packet.topics.len());
    for (filter, qos) in packet.topics {
        let allowed_qos = cmp::min(qos, global.config.max_allowed_qos());
        session.subscribes.insert(filter.clone(), allowed_qos);
        global
            .route_table
            .subscribe(&filter, session.client_id, allowed_qos);

        for retain in global.retain_table.get_matches(&filter) {
            recv_publish(
                session,
                &retain.topic_name,
                retain.qos,
                true,
                &retain.payload,
                (&filter, allowed_qos),
                Some(conn),
            )
            .await?;
        }
        return_codes.push(allowed_qos.into());
    }
    let rv_packet = Suback::new(packet.pid, return_codes);
    write_packet(session.client_id, conn, &rv_packet.into()).await?;
    Ok(())
}

#[inline]
async fn handle_unsubscribe<T: AsyncWrite + Unpin>(
    session: &mut Session,
    packet: Unsubscribe,
    conn: &mut T,
    global: &Arc<GlobalState>,
) -> io::Result<()> {
    log::debug!(
        r#"{} received a unsubscribe packet:
packet id : {}
   topics : {:?}"#,
        session.client_id,
        packet.pid.value(),
        packet.topics,
    );
    for filter in packet.topics {
        global.route_table.unsubscribe(&filter, session.client_id);
        session.subscribes.remove(&filter);
    }
    write_packet(session.client_id, conn, &Packet::Unsuback(packet.pid)).await?;
    Ok(())
}

#[inline]
async fn handle_pingreq<T: AsyncWrite + Unpin>(
    session: &mut Session,
    conn: &mut T,
    _global: &Arc<GlobalState>,
) -> io::Result<()> {
    log::debug!("{} received a ping packet", session.client_id);
    write_packet(session.client_id, conn, &Packet::Pingresp).await?;
    Ok(())
}

#[inline]
pub async fn handle_will<T: AsyncWrite + Unpin>(
    session: &mut Session,
    _conn: &mut T,
    global: &Arc<GlobalState>,
) -> io::Result<()> {
    if let Some(will) = session.will.take() {
        send_publish(
            session,
            &will.topic,
            will.retain,
            will.qos,
            &will.message,
            global,
        )
        .await;
    }
    Ok(())
}

/// return if the offline client loop should stop
#[inline]
pub async fn handle_internal<T: AsyncWrite + Unpin>(
    session: &mut Session,
    receiver: &Receiver<(ClientId, InternalMessage)>,
    sender: ClientId,
    msg: InternalMessage,
    conn: Option<&mut T>,
    _global: &Arc<GlobalState>,
) -> io::Result<bool> {
    match msg {
        InternalMessage::Online { sender } => {
            let mut pending_packets = PendingPackets::new(0, 0, 0);
            mem::swap(&mut session.pending_packets, &mut pending_packets);
            let old_state = SessionState {
                protocol: session.protocol,
                server_packet_id: session.server_packet_id,
                pending_packets,
                receiver: receiver.clone(),
                client_id: session.client_id,
                subscribes: session.subscribes.clone(),
            };
            // FIXME: will panic here, handle the error
            sender.send_async(old_state).await.unwrap();
            Ok(true)
        }
        InternalMessage::Kick { reason } => {
            log::info!(
                "kick {}, reason: {}, offline: {}, network: {}",
                session.client_id,
                reason,
                session.disconnected,
                conn.is_some(),
            );
            Ok(conn.is_some())
        }
        InternalMessage::Publish {
            ref topic_name,
            qos,
            ref payload,
            ref subscribe_filter,
            subscribe_qos,
        } => {
            log::debug!(
                "{:?} received publish message from {:?}",
                session.client_id,
                sender
            );
            recv_publish(
                session,
                topic_name,
                qos,
                false,
                payload,
                (subscribe_filter, subscribe_qos),
                conn,
            )
            .await?;
            Ok(false)
        }
    }
}

// ===========================
// ==== Private Functions ====
// ===========================

// Received a publish message from client or will, then publish the message to matched clients
async fn send_publish(
    session: &mut Session,
    topic_name: &TopicName,
    retain: bool,
    qos: QoS,
    payload: &Bytes,
    global: &Arc<GlobalState>,
) {
    if retain {
        if let Some(old_content) = if payload.is_empty() {
            log::debug!("retain message removed");
            global.retain_table.remove(topic_name)
        } else {
            let content = Arc::new(RetainContent::new(
                topic_name.clone(),
                qos,
                payload.clone(),
                session.client_id,
            ));
            log::debug!("retain message inserted");
            global.retain_table.insert(content)
        } {
            log::debug!(
                r#"old retain content:
 client id : {}
topic name : {}
   payload : {:?}
       qos : {:?}"#,
                old_content.client_id,
                &old_content.topic_name.deref().deref(),
                old_content.payload.as_ref(),
                old_content.qos,
            );
        }
    }

    let matches = global.route_table.get_matches(topic_name);
    let mut senders = Vec::new();
    for content in matches {
        let content = content.read();
        for (client_id, subscribe_qos) in &content.clients {
            if let Some(sender) = global.get_client_sender(client_id) {
                let subscribe_filter = content.topic_filter.clone().unwrap();
                senders.push((*client_id, subscribe_filter, *subscribe_qos, sender));
            }
        }
    }

    if !senders.is_empty() {
        for (sender_client_id, subscribe_filter, subscribe_qos, sender) in senders {
            let msg = InternalMessage::Publish {
                topic_name: topic_name.clone(),
                qos,
                payload: payload.clone(),
                subscribe_filter,
                subscribe_qos,
            };
            if let Err(err) = sender.send_async((session.client_id, msg)).await {
                log::error!(
                    "send publish to connection {} failed: {}",
                    sender_client_id,
                    err
                );
            }
        }
    }
}

// Got a publish message from retain message or subscribed topic, then send the publish message to client.
async fn recv_publish<T: AsyncWrite + Unpin>(
    session: &mut Session,
    topic_name: &TopicName,
    qos: QoS,
    retain: bool,
    payload: &Bytes,
    (subscribe_filter, subscribe_qos): (&TopicFilter, QoS),
    conn: Option<&mut T>,
) -> io::Result<()> {
    if !session.subscribes.contains_key(subscribe_filter) {
        let filter_str: &str = subscribe_filter.deref();
        log::warn!(
            "received a publish message that is not subscribe: {}, session.subscribes: {:?}",
            filter_str,
            session.subscribes
        );
    }
    let final_qos = cmp::min(qos, subscribe_qos);
    if final_qos != QoS::Level0 {
        // The packet_id equals to: `self.server_packet_id % 65536`
        let pid = session.incr_server_packet_id();
        session.pending_packets.clean_complete();
        if let Err(err) = session.pending_packets.push_back(
            pid,
            PubPacket {
                topic_name: topic_name.clone(),
                qos: final_qos,
                retain,
                payload: payload.clone(),
                subscribe_filter: subscribe_filter.clone(),
                subscribe_qos,
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
            retain,
            topic_name: topic_name.clone(),
            payload: payload.clone(),
        };
        write_packet(session.client_id, conn, &rv_packet.into()).await?;
    }

    Ok(())
}

#[inline]
async fn handle_pendings<T: AsyncWrite + Unpin>(
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

#[inline]
async fn write_packet<T: AsyncWrite + Unpin>(
    client_id: ClientId,
    conn: &mut T,
    packet: &Packet,
) -> io::Result<()> {
    log::debug!("write to {:?} with packet: {:#?}", client_id, packet);
    packet.encode_async(conn).await?;
    Ok(())
}
