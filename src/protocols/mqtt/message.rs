use std::cmp;
use std::fmt::Debug;
use std::io::{self, Cursor};
use std::mem;
use std::net::SocketAddr;
use std::ops::Deref;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use flume::Receiver;
use futures_lite::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use mqtt::{
    control::{
        fixed_header::FixedHeader, packet_type::PacketType, variable_header::ConnectReturnCode,
        ProtocolLevel,
    },
    packet::{
        ConnackPacket, ConnectPacket, DisconnectPacket, PingreqPacket, PingrespPacket,
        PubackPacket, PubcompPacket, PublishPacket, PubrecPacket, PubrelPacket,
        QoSWithPacketIdentifier, SubackPacket, SubscribePacket, UnsubackPacket, UnsubscribePacket,
        VariablePacket,
    },
    qos::QualityOfService,
    Decodable, Encodable, TopicFilter, TopicName,
};

use crate::config::AuthType;
use crate::state::{AddClientReceipt, ClientId, Executor, GlobalState, InternalMessage};

use super::{
    PendingPacketStatus, PendingPackets, PubPacket, RetainContent, Session, SessionState, Will,
};

pub async fn handle_connection<T: AsyncRead + AsyncWrite + Unpin, E: Executor>(
    session: &mut Session,
    receiver: &mut Option<Receiver<(ClientId, InternalMessage)>>,
    conn: &mut T,
    peer: &SocketAddr,
    executor: &E,
    global: &Arc<GlobalState>,
) -> io::Result<()> {
    let mut buf = [0u8; 1];
    if conn.read(&mut buf).await? == 0 {
        if !session.disconnected {
            return Err(io::Error::from(io::ErrorKind::UnexpectedEof));
        }
        return Ok(());
    }
    let type_val = buf[0];

    let mut remaining_len = 0;
    let mut i = 0;
    loop {
        if conn.read(&mut buf).await? == 0 {
            if !session.disconnected {
                return Err(io::Error::from(io::ErrorKind::UnexpectedEof));
            }
            return Ok(());
        }

        remaining_len |= (u32::from(buf[0]) & 0x7F) << (7 * i);

        if i >= 4 {
            // FIXME: return Err(FixedHeaderError::MalformedRemainingLength);
            return Err(io::Error::from(io::ErrorKind::InvalidData));
        }

        if buf[0] & 0x80 == 0 {
            break;
        } else {
            i += 1;
        }
    }
    let fixed_header = match PacketType::from_u8(type_val) {
        Ok(packet_type) => FixedHeader::new(packet_type, remaining_len),
        Err(_err) => {
            return Err(io::Error::from(io::ErrorKind::InvalidData));
        }
    };

    // FIXME: rewrite with better performance
    let mut buffer = vec![0u8; fixed_header.remaining_length as usize];
    conn.read_exact(&mut buffer).await?;
    let packet = VariablePacket::decode_with(&mut Cursor::new(buffer), Some(fixed_header))
        .map_err(|_err| io::Error::from(io::ErrorKind::InvalidData))?;
    handle_packet(session, receiver, packet, conn, peer, executor, global).await?;
    Ok(())
}

#[inline]
async fn handle_packet<T: AsyncWrite + Unpin, E: Executor>(
    session: &mut Session,
    receiver: &mut Option<Receiver<(ClientId, InternalMessage)>>,
    packet: VariablePacket,
    conn: &mut T,
    peer: &SocketAddr,
    executor: &E,
    global: &Arc<GlobalState>,
) -> io::Result<()> {
    if !matches!(packet, VariablePacket::ConnectPacket(..)) && !session.connected {
        log::info!("{} not connected", peer);
        return Err(io::Error::from(io::ErrorKind::InvalidData));
    }
    match packet {
        VariablePacket::ConnectPacket(packet) => {
            handle_connect(session, receiver, packet, conn, peer, executor, global).await?;
        }
        VariablePacket::DisconnectPacket(packet) => {
            handle_disconnect(session, packet, conn, global).await?;
        }
        VariablePacket::PublishPacket(packet) => {
            handle_publish(session, packet, conn, global).await?;
        }
        VariablePacket::PubackPacket(packet) => {
            handle_puback(session, packet, conn, global).await?;
        }
        VariablePacket::PubrecPacket(packet) => {
            handle_pubrec(session, packet, conn, global).await?;
        }
        VariablePacket::PubrelPacket(packet) => {
            handle_pubrel(session, packet, conn, global).await?;
        }
        VariablePacket::PubcompPacket(packet) => {
            handle_pubcomp(session, packet, conn, global).await?;
        }
        VariablePacket::SubscribePacket(packet) => {
            handle_subscribe(session, packet, conn, global).await?;
        }
        VariablePacket::UnsubscribePacket(packet) => {
            handle_unsubscribe(session, packet, conn, global).await?;
        }
        VariablePacket::PingreqPacket(packet) => {
            handle_pingreq(session, packet, conn, global).await?;
        }
        _ => {
            return Err(io::Error::from(io::ErrorKind::InvalidData));
        }
    }
    *session.last_packet_time.write() = Instant::now();

    session.pending_packets.clean_complete();
    let mut start_idx = 0;
    while let Some((idx, packet_status)) = session.pending_packets.get_ready_packet(start_idx) {
        start_idx = idx + 1;
        match packet_status {
            PendingPacketStatus::New {
                dup,
                packet_id,
                packet,
                ..
            } => {
                let qos_with_id = match packet.qos {
                    QualityOfService::Level0 => QoSWithPacketIdentifier::Level0,
                    QualityOfService::Level1 => QoSWithPacketIdentifier::Level1(*packet_id),
                    QualityOfService::Level2 => QoSWithPacketIdentifier::Level2(*packet_id),
                };
                let mut rv_packet = PublishPacket::new(
                    TopicName::clone(&packet.topic_name),
                    qos_with_id,
                    packet.payload.clone(),
                );
                rv_packet.set_dup(*dup);
                rv_packet.set_retain(packet.retain);
                *dup = true;
                write_packet(session.client_id, conn, &rv_packet).await?;
            }
            PendingPacketStatus::Pubrec { packet_id, .. } => {
                let rv_packet = PubrelPacket::new(*packet_id);
                write_packet(session.client_id, conn, &rv_packet).await?;
            }
            PendingPacketStatus::Complete => unreachable!(),
        }
    }

    Ok(())
}

#[inline]
async fn handle_connect<T: AsyncWrite + Unpin, E: Executor>(
    session: &mut Session,
    receiver: &mut Option<Receiver<(ClientId, InternalMessage)>>,
    packet: ConnectPacket,
    conn: &mut T,
    peer: &SocketAddr,
    executor: &E,
    global: &Arc<GlobalState>,
) -> io::Result<()> {
    log::debug!(
        r#"{} received a connect packet:
     protocol : {} {:?}
    client_id : {}
clean session : {}
     username : {:?}
     password : {:?}
   keep-alive : {}s
         will : {:?}, retain: {}, qos: {:?}
     reserved : {}"#,
        peer,
        packet.protocol_name(),
        packet.protocol_level(),
        packet.client_identifier(),
        packet.clean_session(),
        packet.user_name(),
        packet.password(),
        packet.keep_alive(),
        packet.will(),
        packet.will_retain(),
        packet.will_qos(),
        packet.reserved_flag(),
    );
    if packet.reserved_flag() {
        return Err(io::Error::from(io::ErrorKind::InvalidData));
    }
    let protocol_level = packet.protocol_level();
    let client_identifier = packet.client_identifier();
    match (packet.protocol_name(), protocol_level) {
        ("MQIsdp", ProtocolLevel::Version310) => {}
        ("MQTT", ProtocolLevel::Version311) => {}
        _ => {
            let rv_packet =
                ConnackPacket::new(false, ConnectReturnCode::UnacceptableProtocolVersion);
            write_packet(session.client_id, conn, &rv_packet).await?;
            session.disconnected = true;
            return Ok(());
        }
    }

    if (protocol_level == ProtocolLevel::Version310
        && (client_identifier.is_empty() || client_identifier.len() > 23))
        || (protocol_level == ProtocolLevel::Version311 && client_identifier.len() > 65535)
    {
        let rv_packet = ConnackPacket::new(false, ConnectReturnCode::IdentifierRejected);
        write_packet(session.client_id, conn, &rv_packet).await?;
        session.disconnected = true;
        return Ok(());
    }

    let mut return_code = ConnectReturnCode::ConnectionAccepted;
    // FIXME: auth by plugin
    for auth_type in &global.config.auth_types {
        match auth_type {
            AuthType::UsernamePassword => {
                if let Some(username) = packet.user_name() {
                    if global.config.users.get(username).map(|s| s.as_str()) != packet.password() {
                        log::debug!("incorrect password for user: {}", username);
                        return_code = ConnectReturnCode::BadUserNameOrPassword;
                    }
                } else {
                    log::debug!(
                        "username password not set for client: {}",
                        client_identifier
                    );
                    return_code = ConnectReturnCode::BadUserNameOrPassword;
                }
            }
            _ => panic!("auth method not supported: {:?}", auth_type),
        }
    }
    // FIXME: permission check and return "not authorized"
    if return_code != ConnectReturnCode::ConnectionAccepted {
        let rv_packet = ConnackPacket::new(false, return_code);
        write_packet(session.client_id, conn, &rv_packet).await?;
        session.disconnected = true;
        return Ok(());
    }

    // FIXME: if connection reach rate limit return "Server unavailable"

    session.protocol_level = packet.protocol_level();
    session.clean_session = packet.clean_session();
    session.client_identifier = if client_identifier.is_empty() {
        uuid::Uuid::new_v4().to_string()
    } else {
        client_identifier.to_string()
    };
    session.username = packet.user_name().map(|name| name.to_string());
    session.keep_alive = packet.keep_alive();

    // FIXME: if kee_alive is zero, set a default keep_alive value from config
    if session.keep_alive > 0 {
        let interval = Duration::from_millis(session.keep_alive as u64 * 500);
        let client_id = session.client_id;
        log::debug!("{:?} keep alive: {:?}", client_id, interval * 2);
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

    if let Some((topic, message)) = packet.will() {
        let will_qos = match packet.will_qos() {
            0 => QualityOfService::Level0,
            1 => QualityOfService::Level1,
            2 => QualityOfService::Level2,
            _ => unreachable!(),
        };
        if topic.starts_with('$') {
            return Err(io::Error::from(io::ErrorKind::InvalidData));
        }
        let will = Will {
            retain: packet.will_retain(),
            qos: will_qos,
            topic: TopicName::new(topic).expect("topic name"),
            message: message.to_vec(),
        };
        session.will = Some(will);
    }

    let mut session_present = false;
    match global.add_client(session.client_identifier.as_str()).await {
        AddClientReceipt::Present(old_state) => {
            log::debug!("Got exists session for {:?}", old_state.client_id);
            session.client_id = old_state.client_id;
            *receiver = Some(old_state.receiver);
            // TODO: if protocol level is compatiable, copy the session state?
            if !session.clean_session && session.protocol_level == old_state.protocol_level {
                session.server_packet_id = old_state.server_packet_id;
                session.pending_packets = old_state.pending_packets;
                session.subscribes = old_state.subscribes;
                session_present = true;
            } else {
                log::info!(
                    "all {} session state removed due to reconnect with a different protocol level, new level: {:?}, old level: {:?}, or clean session: {}",
                    old_state.pending_packets.len(),
                    session.protocol_level,
                    old_state.protocol_level,
                    session.clean_session,
                );
                session_present = false;
            }
        }
        AddClientReceipt::New {
            client_id,
            receiver: new_receiver,
        } => {
            log::debug!("Create new session for {:?}", client_id);
            session.client_id = client_id;
            *receiver = Some(new_receiver);
        }
    }

    log::debug!("Socket {} assgined to: {:?}", peer, session.client_id);

    let rv_packet = ConnackPacket::new(session_present, return_code);
    write_packet(session.client_id, conn, &rv_packet).await?;
    session.connected = true;
    Ok(())
}

#[inline]
async fn handle_disconnect<T: AsyncWrite + Unpin>(
    session: &mut Session,
    packet: DisconnectPacket,
    _conn: &mut T,
    _global: &Arc<GlobalState>,
) -> io::Result<()> {
    log::debug!(
        "{:?} received a disconnect packet: {:#?}",
        session.client_id,
        packet
    );
    session.will = None;
    session.disconnected = true;
    Ok(())
}

#[inline]
async fn handle_publish<T: AsyncWrite + Unpin>(
    session: &mut Session,
    packet: PublishPacket,
    conn: &mut T,
    global: &Arc<GlobalState>,
) -> io::Result<()> {
    log::debug!(
        r#"{:?} received a publish packet:
topic name : {}
   payload : {:?}
     flags : qos={:?}, retain={}, dup={}"#,
        session.client_id,
        packet.topic_name(),
        packet.payload(),
        packet.qos(),
        packet.retain(),
        packet.dup(),
    );
    if packet.topic_name().starts_with('$') {
        return Err(io::Error::from(io::ErrorKind::InvalidData));
    }
    // FIXME: handle dup flag
    send_publish(
        session,
        packet.topic_name(),
        packet.retain(),
        packet.qos().split().0,
        packet.payload(),
        global,
    )
    .await;
    match packet.qos() {
        QoSWithPacketIdentifier::Level0 => {}
        QoSWithPacketIdentifier::Level1(pkid) => {
            write_packet(session.client_id, conn, &PubackPacket::new(pkid)).await?;
        }
        QoSWithPacketIdentifier::Level2(pkid) => {
            write_packet(session.client_id, conn, &PubrecPacket::new(pkid)).await?;
        }
    }
    Ok(())
}

#[inline]
async fn handle_puback<T: AsyncWrite + Unpin>(
    session: &mut Session,
    packet: PubackPacket,
    _conn: &mut T,
    _global: &Arc<GlobalState>,
) -> io::Result<()> {
    log::debug!(
        "{:?} received a puback packet: id={}",
        session.client_id,
        packet.packet_identifier()
    );
    session.pending_packets.complete(packet.packet_identifier());
    Ok(())
}

#[inline]
async fn handle_pubrec<T: AsyncWrite + Unpin>(
    session: &mut Session,
    packet: PubrecPacket,
    conn: &mut T,
    _global: &Arc<GlobalState>,
) -> io::Result<()> {
    log::debug!(
        "{:?} received a pubrec  packet: id={}",
        session.client_id,
        packet.packet_identifier()
    );
    session.pending_packets.pubrec(packet.packet_identifier());
    let rv_packet = PubrelPacket::new(packet.packet_identifier());
    write_packet(session.client_id, conn, &rv_packet).await?;
    Ok(())
}

#[inline]
async fn handle_pubrel<T: AsyncWrite + Unpin>(
    session: &mut Session,
    packet: PubrelPacket,
    conn: &mut T,
    _global: &Arc<GlobalState>,
) -> io::Result<()> {
    log::debug!(
        "{:?} received a pubrel  packet: id={}",
        session.client_id,
        packet.packet_identifier()
    );
    let rv_packet = PubcompPacket::new(packet.packet_identifier());
    write_packet(session.client_id, conn, &rv_packet).await?;
    Ok(())
}

#[inline]
async fn handle_pubcomp<T: AsyncWrite + Unpin>(
    session: &mut Session,
    packet: PubcompPacket,
    _conn: &mut T,
    _global: &Arc<GlobalState>,
) -> io::Result<()> {
    log::debug!(
        "{:?} received a pubcomp packet: id={}",
        session.client_id,
        packet.packet_identifier()
    );
    session.pending_packets.complete(packet.packet_identifier());
    Ok(())
}

#[inline]
async fn handle_subscribe<T: AsyncWrite + Unpin>(
    session: &mut Session,
    packet: SubscribePacket,
    conn: &mut T,
    global: &Arc<GlobalState>,
) -> io::Result<()> {
    log::debug!(
        r#"{:?} received a subscribe packet:
packet id : {}
   topics : {:?}"#,
        session.client_id,
        packet.packet_identifier(),
        packet.subscribes(),
    );
    if packet.subscribes().is_empty() {
        return Err(io::Error::from(io::ErrorKind::InvalidData));
    }
    let mut return_codes = Vec::with_capacity(packet.subscribes().len());
    for (filter, qos) in packet.subscribes() {
        let allowed_qos = cmp::min(*qos, global.config.max_allowed_qos());
        session.subscribes.insert(filter.clone(), allowed_qos);
        global
            .route_table
            .subscribe(filter, session.client_id, allowed_qos);

        for retain in global.retain_table.get_matches(filter) {
            let topic_name = Arc::new(retain.topic_name.clone());
            let subscribe_filter = Arc::new(filter.clone());
            recv_publish(
                session,
                &topic_name,
                retain.qos,
                true,
                &retain.payload,
                (&subscribe_filter, allowed_qos),
                Some(conn),
            )
            .await?;
        }
        return_codes.push(allowed_qos.into());
    }
    let rv_packet = SubackPacket::new(packet.packet_identifier(), return_codes);
    write_packet(session.client_id, conn, &rv_packet).await?;
    Ok(())
}

#[inline]
async fn handle_unsubscribe<T: AsyncWrite + Unpin>(
    session: &mut Session,
    packet: UnsubscribePacket,
    conn: &mut T,
    global: &Arc<GlobalState>,
) -> io::Result<()> {
    log::debug!(
        r#"{:?} received a unsubscribe packet:
packet id : {}
   topics : {:?}"#,
        session.client_id,
        packet.packet_identifier(),
        packet.subscribes(),
    );
    for filter in packet.subscribes() {
        global.route_table.unsubscribe(filter, session.client_id);
        session.subscribes.remove(filter);
    }
    let rv_packet = UnsubackPacket::new(packet.packet_identifier());
    write_packet(session.client_id, conn, &rv_packet).await?;
    Ok(())
}

#[inline]
async fn handle_pingreq<T: AsyncWrite + Unpin>(
    session: &mut Session,
    _packet: PingreqPacket,
    conn: &mut T,
    _global: &Arc<GlobalState>,
) -> io::Result<()> {
    log::debug!("{:?} received a ping packet", session.client_id);
    let rv_packet = PingrespPacket::new();
    write_packet(session.client_id, conn, &rv_packet).await?;
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
            // FIXME: read max inflight and max packets from config
            let mut pending_packets = PendingPackets::new(10, 1000, 15);
            mem::swap(&mut session.pending_packets, &mut pending_packets);
            let old_state = SessionState {
                protocol_level: session.protocol_level,
                server_packet_id: session.server_packet_id,
                pending_packets,
                receiver: receiver.clone(),
                client_id: session.client_id,
                subscribes: session.subscribes.clone(),
            };
            sender.send_async(old_state).await.unwrap();
            Ok(true)
        }
        InternalMessage::Kick { reason } => {
            log::info!(
                "kick {:?}, reason: {}, offline: {}, network: {}",
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
    topic_name: &str,
    retain: bool,
    qos: QualityOfService,
    payload: &[u8],
    global: &Arc<GlobalState>,
) {
    if retain {
        if let Some(old_content) = if payload.is_empty() {
            log::debug!("retain message removed");
            global.retain_table.remove(topic_name)
        } else {
            let content = Arc::new(RetainContent::new(
                TopicName::new(topic_name).unwrap(),
                qos,
                Bytes::from(payload.to_vec()),
                session.client_id,
            ));
            log::debug!("retain message inserted");
            global.retain_table.insert(content)
        } {
            log::debug!(
                r#"old retain content:
 client id : {:?}
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
                let subscribe_filter = Arc::new(content.topic_filter.clone().unwrap());
                senders.push((*client_id, subscribe_filter, *subscribe_qos, sender));
            }
        }
    }

    if !senders.is_empty() {
        let topic_name = Arc::new(TopicName::new(topic_name).unwrap());
        let payload = Bytes::from(payload.to_vec());
        for (sender_client_id, subscribe_filter, subscribe_qos, sender) in senders {
            let msg = InternalMessage::Publish {
                topic_name: Arc::clone(&topic_name),
                qos,
                payload: payload.clone(),
                subscribe_filter,
                subscribe_qos,
            };
            if let Err(err) = sender.send_async((session.client_id, msg)).await {
                log::error!(
                    "send publish to connection {:?} failed: {}",
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
    topic_name: &Arc<TopicName>,
    qos: QualityOfService,
    retain: bool,
    payload: &Bytes,
    (subscribe_filter, subscribe_qos): (&Arc<TopicFilter>, QualityOfService),
    conn: Option<&mut T>,
) -> io::Result<()> {
    if !session.subscribes.contains_key(subscribe_filter.as_ref()) {
        let filter_str: &str = subscribe_filter.as_ref().deref();
        log::warn!(
            "received a publish message that is not subscribe: {}, session.subscribes: {:?}",
            filter_str,
            session.subscribes
        );
    }
    let final_qos = cmp::min(qos, subscribe_qos);
    let packet_sent = conn.is_some();
    let mut packet_id = 0;
    if final_qos != QualityOfService::Level0 {
        // The packet_id equals to: `self.server_packet_id % 65536`
        packet_id = session.incr_server_packet_id() as u16;
        session.pending_packets.clean_complete();
        if let Err(err) = session.pending_packets.push_back(
            packet_id,
            PubPacket {
                topic_name: Arc::clone(topic_name),
                qos: final_qos,
                retain,
                payload: payload.clone(),
                subscribe_filter: Arc::clone(subscribe_filter),
                subscribe_qos,
            },
            packet_sent,
        ) {
            // TODO: proper handle this error
            log::warn!("push pending packets error: {}", err);
        };
    }

    if let Some(conn) = conn {
        let qos_with_id = match final_qos {
            QualityOfService::Level0 => QoSWithPacketIdentifier::Level0,
            QualityOfService::Level1 => QoSWithPacketIdentifier::Level1(packet_id),
            QualityOfService::Level2 => QoSWithPacketIdentifier::Level2(packet_id),
        };
        let mut rv_packet = PublishPacket::new(
            TopicName::new(topic_name.to_string()).unwrap(),
            qos_with_id,
            payload.to_vec(),
        );
        rv_packet.set_dup(false);
        rv_packet.set_retain(retain);
        write_packet(session.client_id, conn, &rv_packet).await?;
    }

    Ok(())
}

#[inline]
async fn write_packet<P: Encodable + Debug, T: AsyncWrite + Unpin>(
    client_id: ClientId,
    conn: &mut T,
    packet: &P,
) -> io::Result<()> {
    log::debug!("write to {:?} with packet: {:?}", client_id, packet);
    let mut buf = Vec::with_capacity(packet.encoded_length() as usize);
    packet.encode(&mut buf)?;
    conn.write_all(&buf).await?;
    Ok(())
}
