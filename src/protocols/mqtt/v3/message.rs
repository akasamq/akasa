use std::cmp;
use std::hash::{Hash, Hasher};
use std::io;
use std::mem;
use std::net::SocketAddr;
use std::ops::Deref;
use std::sync::Arc;
use std::time::Instant;

use ahash::AHasher;
use bytes::Bytes;
use flume::Receiver;
use futures_lite::{
    io::{AsyncRead, AsyncWrite},
    FutureExt,
};
use hashbrown::HashMap;
use mqtt_proto::{
    v3::{
        Connack, Connect, ConnectReturnCode, Header, Packet, Publish, Suback, Subscribe,
        Unsubscribe,
    },
    Pid, Protocol, QoS, QosPid, TopicFilter, TopicName,
};

use crate::config::AuthType;
use crate::state::{AddClientReceipt, ClientId, Executor, GlobalState, InternalMessage};

use super::super::{start_keep_alive_timer, PendingPacketStatus, PendingPackets, RetainContent};
use super::{PubPacket, Session, SessionState};

pub async fn handle_connection<T: AsyncRead + AsyncWrite + Unpin, E: Executor>(
    conn: T,
    peer: SocketAddr,
    header: Header,
    protocol: Protocol,
    executor: E,
    global: Arc<GlobalState>,
) -> io::Result<()> {
    match handle_online(conn, peer, header, protocol, &executor, &global).await {
        Ok(Some((session, receiver))) => {
            log::info!(
                "executor {:03}, {} go to offline, total {} clients ({} online)",
                executor.id(),
                peer,
                global.clients_count(),
                global.online_clients_count(),
            );
            executor.spawn_local(handle_offline(session, receiver, global));
        }
        Ok(None) => {
            log::info!(
                "executor {:03}, {} finished, total {} clients ({} online)",
                executor.id(),
                peer,
                global.clients_count(),
                global.online_clients_count(),
            );
        }
        Err(err) => {
            log::info!(
                "executor {:03}, {} error: {}, total {} clients ({} online)",
                executor.id(),
                peer,
                err,
                global.clients_count(),
                global.online_clients_count(),
            );
            return Err(err);
        }
    }
    Ok(())
}

async fn handle_online<T: AsyncRead + AsyncWrite + Unpin, E: Executor>(
    mut conn: T,
    peer: SocketAddr,
    _header: Header,
    protocol: Protocol,
    executor: &E,
    global: &Arc<GlobalState>,
) -> io::Result<Option<(Session, Receiver<(ClientId, InternalMessage)>)>> {
    enum Msg {
        Socket(()),
        Internal((ClientId, InternalMessage)),
    }

    let mut session = Session::new(&global.config, peer);
    let mut receiver = None;
    let mut io_error = None;

    let packet = match Connect::decode_with_protocol(&mut conn, protocol).await {
        Ok(packet) => packet,
        Err(err) => {
            log::debug!("mqtt v3.x connect codec error: {}", err);
            return Err(io::ErrorKind::InvalidData.into());
        }
    };
    // FIXME: if client not send any data for a long time, disconnect it.
    handle_connect(
        &mut session,
        &mut receiver,
        packet,
        &mut conn,
        executor,
        global,
    )
    .await?;

    if !session.connected() {
        log::info!("{} not connected", session.peer());
        return Err(io::ErrorKind::InvalidData.into());
    }
    let receiver = receiver.expect("receiver");
    log::info!(
        "executor {:03}, {} connected, total {} clients ({} online) ",
        executor.id(),
        session.peer(),
        global.clients_count(),
        global.online_clients_count(),
    );

    while !session.disconnected() {
        // Online client logic
        let recv_data = async {
            handle_packet(&mut session, &mut conn, executor, global)
                .await
                .map(Msg::Socket)
        };
        let recv_msg = async {
            receiver
                .recv_async()
                .await
                .map(Msg::Internal)
                .map_err(|_| io::ErrorKind::BrokenPipe.into())
        };
        match recv_data.or(recv_msg).await {
            Ok(Msg::Socket(())) => {}
            Ok(Msg::Internal((sender, msg))) => {
                let is_kick = matches!(msg, InternalMessage::Kick { .. });
                match handle_internal(
                    &mut session,
                    &receiver,
                    sender,
                    msg,
                    Some(&mut conn),
                    global,
                )
                .await
                {
                    Ok(true) => {
                        if is_kick && !session.disconnected() {
                            // Offline client logic
                            io_error = Some(io::ErrorKind::BrokenPipe.into());
                        }
                        // Been occupied by newly connected client or kicked out after disconnected
                        break;
                    }
                    Ok(false) => {}
                    // Currently, this error can only happend when write data to connection
                    Err(err) => {
                        // An error in online mode should also check clean_session value
                        io_error = Some(err);
                        break;
                    }
                }
            }
            Err(err) => {
                // An error in online mode should also check clean_session value
                io_error = Some(err);
                break;
            }
        }
    }

    if !session.disconnected() {
        handle_will(&mut session, &mut conn, global).await?;
    }
    if session.clean_session() {
        global.remove_client(session.client_id(), session.subscribes().keys());
        if let Some(err) = io_error {
            return Err(err);
        }
    } else {
        // become a offline client, but session keep updating
        global.offline_client(session.client_id());
        session.connected = false;
        return Ok(Some((session, receiver)));
    }

    Ok(None)
}

async fn handle_offline(
    mut session: Session,
    receiver: Receiver<(ClientId, InternalMessage)>,
    global: Arc<GlobalState>,
) {
    let mut conn: Option<Vec<u8>> = None;
    loop {
        let (sender, msg) = match receiver.recv_async().await {
            Ok((sender, msg)) => (sender, msg),
            Err(err) => {
                log::warn!("offline client receive internal message error: {:?}", err);
                break;
            }
        };
        match handle_internal(&mut session, &receiver, sender, msg, conn.as_mut(), &global).await {
            Ok(true) => {
                // Been occupied by newly connected client
                break;
            }
            Ok(false) => {}
            Err(err) => {
                // An error in offline mode should immediately return it
                log::error!("offline client error: {:?}", err);
                break;
            }
        }
    }
    log::debug!("offline client finished: {:?}", session.client_id());
}

async fn handle_connect<T: AsyncWrite + Unpin, E: Executor>(
    session: &mut Session,
    receiver: &mut Option<Receiver<(ClientId, InternalMessage)>>,
    packet: Connect,
    conn: &mut T,
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
        session.peer,
        packet.protocol,
        packet.client_id,
        packet.clean_session,
        packet.username,
        packet.password,
        packet.keep_alive,
        packet.last_will,
    );
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
                        return_code = ConnectReturnCode::BadUserNameOrPassword;
                    }
                } else {
                    log::debug!("username password not set for client: {}", packet.client_id);
                    return_code = ConnectReturnCode::BadUserNameOrPassword;
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

    start_keep_alive_timer(
        session.keep_alive,
        session.client_id,
        &session.last_packet_time,
        executor,
        global,
    )?;

    if let Some(last_will) = packet.last_will {
        if last_will.topic_name.starts_with('$') {
            return Err(io::ErrorKind::InvalidData.into());
        }
        session.last_will = Some(last_will);
    }

    let mut session_present = false;
    match global
        .add_client(session.client_identifier.as_str(), session.protocol)
        .await
    {
        AddClientReceipt::PresentV3(old_state) => {
            log::debug!("Got exists session for {}", old_state.client_id);
            session.client_id = old_state.client_id;
            *receiver = Some(old_state.receiver);
            // TODO: if protocol level is compatiable, copy the session state?
            if !session.clean_session && session.protocol == old_state.protocol {
                session.server_packet_id = old_state.server_packet_id;
                session.pending_packets = old_state.pending_packets;
                session.qos2_pids = old_state.qos2_pids;
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
        AddClientReceipt::PresentV5(_) => {
            // not allowed, so this is dead branch.
            unreachable!();
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

    log::debug!("Socket {} assgined to: {}", session.peer, session.client_id);

    let rv_packet = Connack::new(session_present, return_code);
    write_packet(session.client_id, conn, &rv_packet.into()).await?;
    session.connected = true;
    after_handle_packet(session, conn).await?;
    Ok(())
}

async fn handle_packet<T: AsyncRead + AsyncWrite + Unpin, E: Executor>(
    session: &mut Session,
    conn: &mut T,
    _executor: &E,
    global: &Arc<GlobalState>,
) -> io::Result<()> {
    let packet = match Packet::decode_async(conn).await {
        Ok(packet) => packet,
        Err(err) => {
            log::debug!("mqtt v3.x codec error: {}", err);
            if err.is_eof() {
                if !session.disconnected {
                    return Err(io::ErrorKind::UnexpectedEof.into());
                } else {
                    return Ok(());
                }
            } else {
                return Err(io::ErrorKind::InvalidData.into());
            }
        }
    };
    match packet {
        Packet::Disconnect => handle_disconnect(session, conn, global).await?,
        Packet::Publish(pkt) => handle_publish(session, pkt, conn, global).await?,
        Packet::Puback(pid) => handle_puback(session, pid, conn, global).await?,
        Packet::Pubrec(pid) => handle_pubrec(session, pid, conn, global).await?,
        Packet::Pubrel(pid) => handle_pubrel(session, pid, conn, global).await?,
        Packet::Pubcomp(pid) => handle_pubcomp(session, pid, conn, global).await?,
        Packet::Subscribe(pkt) => handle_subscribe(session, pkt, conn, global).await?,
        Packet::Unsubscribe(pkt) => handle_unsubscribe(session, pkt, conn, global).await?,
        Packet::Pingreq => handle_pingreq(session, conn, global).await?,
        _ => return Err(io::ErrorKind::InvalidData.into()),
    }
    after_handle_packet(session, conn).await?;
    Ok(())
}

#[inline]
async fn handle_disconnect<T: AsyncWrite + Unpin>(
    session: &mut Session,
    _conn: &mut T,
    _global: &Arc<GlobalState>,
) -> io::Result<()> {
    log::debug!("{} received a disconnect packet", session.client_id);
    session.last_will = None;
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
        return Err(io::ErrorKind::InvalidData.into());
    }
    if packet.qos_pid == QosPid::Level0 && packet.dup {
        log::debug!("invalid dup flag");
        return Err(io::ErrorKind::InvalidData.into());
    }

    // FIXME: handle dup flag

    if let QosPid::Level2(pid) = packet.qos_pid {
        let mut hasher = AHasher::default();
        packet.hash(&mut hasher);
        let current_hash = hasher.finish();

        if let Some(previous_hash) = session.qos2_pids.get(&pid) {
            // hash collision is acceptable here
            if current_hash != *previous_hash {
                log::warn!("packet identifier in use: {}", pid.value());
                return Err(io::ErrorKind::InvalidData.into());
            }
        } else {
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
    let _matched = session.pending_packets.complete(pid, QoS::Level1);
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

    let _matched = session.pending_packets.pubrec(pid);
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
    if session.qos2_pids.remove(&pid).is_none() {
        log::warn!("packet identifier not found: {}", pid.value());
        return Err(io::ErrorKind::InvalidData.into());
    }
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
    let _matched = session.pending_packets.complete(pid, QoS::Level2);
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
        if filter.is_shared() {
            log::info!("mqtt v3.x don't support shared subscription");
            return Err(io::ErrorKind::InvalidData.into());
        }
        let granted_qos = cmp::min(qos, global.config.max_allowed_qos());
        session.subscribes.insert(filter.clone(), granted_qos);
        global
            .route_table
            .subscribe(&filter, session.client_id, granted_qos);

        for retain in global.retain_table.get_matches(&filter) {
            if retain.qos <= granted_qos {
                recv_publish(
                    session,
                    RecvPublish {
                        topic_name: &retain.topic_name,
                        qos: retain.qos,
                        retain: true,
                        payload: &retain.payload,
                        subscribe_filter: &filter,
                        subscribe_qos: granted_qos,
                    },
                    Some(conn),
                )
                .await?;
            }
        }
        return_codes.push(granted_qos.into());
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
async fn after_handle_packet<T: AsyncWrite + Unpin>(
    session: &mut Session,
    conn: &mut T,
) -> io::Result<()> {
    *session.last_packet_time.write() = Instant::now();
    handle_pendings(session, conn).await?;
    Ok(())
}

#[inline]
async fn handle_will<T: AsyncWrite + Unpin>(
    session: &mut Session,
    _conn: &mut T,
    global: &Arc<GlobalState>,
) -> io::Result<()> {
    if let Some(last_will) = session.last_will.take() {
        send_publish(
            session,
            SendPublish {
                topic_name: &last_will.topic_name,
                retain: last_will.retain,
                qos: last_will.qos,
                payload: &last_will.message,
            },
            global,
        )
        .await;
    }
    Ok(())
}

/// return if the offline client loop should stop
#[inline]
async fn handle_internal<T: AsyncWrite + Unpin>(
    session: &mut Session,
    receiver: &Receiver<(ClientId, InternalMessage)>,
    sender: ClientId,
    msg: InternalMessage,
    conn: Option<&mut T>,
    _global: &Arc<GlobalState>,
) -> io::Result<bool> {
    // FIXME: call receiver.try_recv() to clear the channel, if the pending
    // queue is full, set a marker to the global state so that the sender stop
    // sending qos0 messages to this client.
    let mut stop = false;
    match msg {
        InternalMessage::OnlineV3 { sender } => {
            let mut pending_packets = PendingPackets::new(0, 0, 0);
            let mut qos2_pids = HashMap::new();
            let mut subscribes = HashMap::new();
            mem::swap(&mut session.pending_packets, &mut pending_packets);
            mem::swap(&mut session.qos2_pids, &mut qos2_pids);
            mem::swap(&mut session.subscribes, &mut subscribes);
            let old_state = SessionState {
                protocol: session.protocol,
                server_packet_id: session.server_packet_id,
                pending_packets,
                qos2_pids,
                receiver: receiver.clone(),
                client_id: session.client_id,
                subscribes,
            };
            if sender.send_async(old_state).await.is_ok() {
                stop = true;
            } else {
                log::info!("the connection want take over current session already ended");
            }
        }
        InternalMessage::OnlineV5 { .. } => {
            log::info!("take over v3.x by v5.x client is not allowed");
        }
        InternalMessage::PublishV3 {
            ref topic_name,
            qos,
            retain,
            ref payload,
            ref subscribe_filter,
            subscribe_qos,
        } => {
            log::debug!(
                "{:?} received a v3.x publish message from {:?}",
                session.client_id,
                sender
            );
            recv_publish(
                session,
                RecvPublish {
                    topic_name,
                    qos,
                    retain,
                    payload,
                    subscribe_filter,
                    subscribe_qos,
                },
                conn,
            )
            .await?;
        }
        InternalMessage::PublishV5 {
            ref topic_name,
            qos,
            retain,
            ref payload,
            ref subscribe_filter,
            subscribe_qos,
            properties: _,
        } => {
            log::debug!(
                "{:?} received a v5.x publish message from {:?}",
                session.client_id,
                sender
            );
            recv_publish(
                session,
                RecvPublish {
                    topic_name,
                    qos,
                    retain,
                    payload,
                    subscribe_filter,
                    subscribe_qos,
                },
                conn,
            )
            .await?;
        }
        InternalMessage::Kick { reason } => {
            log::info!(
                "kick {}, reason: {}, offline: {}, network: {}",
                session.client_id,
                reason,
                session.disconnected,
                conn.is_some(),
            );
            stop = conn.is_some();
        }
        InternalMessage::WillDelayReached { .. } | InternalMessage::SessionExpired { .. } => {
            unreachable!();
        }
    }
    Ok(stop)
}

// ===========================
// ==== Private Functions ====
// ===========================

// Received a publish message from client or will, then publish the message to matched clients
async fn send_publish<'a>(session: &mut Session, msg: SendPublish<'a>, global: &Arc<GlobalState>) {
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
async fn recv_publish<'a, T: AsyncWrite + Unpin>(
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

struct RecvPublish<'a> {
    topic_name: &'a TopicName,
    qos: QoS,
    retain: bool,
    payload: &'a Bytes,
    subscribe_filter: &'a TopicFilter,
    subscribe_qos: QoS,
}

struct SendPublish<'a> {
    topic_name: &'a TopicName,
    retain: bool,
    qos: QoS,
    payload: &'a Bytes,
}
