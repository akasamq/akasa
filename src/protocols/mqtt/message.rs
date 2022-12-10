use std::cmp;
use std::io::{self, Cursor};
use std::mem;
use std::ops::Deref;
use std::os::unix::io::RawFd;
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use flume::Receiver;
use futures_lite::io::{AsyncReadExt, AsyncWriteExt};
use glommio::{
    net::{Preallocated, TcpStream},
    timer::TimerActionRepeat,
};
use mqtt::{
    control::{
        fixed_header::FixedHeader, packet_type::PacketType, variable_header::ConnectReturnCode,
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
use crate::state::{AddClientReceipt, ClientId, ExecutorState, GlobalState, InternalMessage};

use super::{
    PendingPacketStatus, PendingPackets, PubPacket, RetainContent, Session, SessionState, Will,
};

pub async fn handle_connection(
    session: &mut Session,
    receiver: &mut Option<Receiver<(ClientId, InternalMessage)>>,
    conn: &mut TcpStream<Preallocated>,
    current_fd: RawFd,
    executor: &Rc<ExecutorState>,
    global: &Arc<GlobalState>,
) -> io::Result<()> {
    let mut buf = [0u8; 1];
    let n = conn.read(&mut buf).await?;
    if n == 0 {
        if !session.disconnected {
            session.io_error = Some(io::Error::from(io::ErrorKind::UnexpectedEof));
        }
        return Ok(());
    }
    let type_val = buf[0];

    let mut remaining_len = 0;
    let mut i = 0;
    loop {
        let n = conn.read(&mut buf).await?;
        if n == 0 {
            if !session.disconnected {
                session.io_error = Some(io::Error::from(io::ErrorKind::UnexpectedEof));
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
        Err(err) => {
            return Err(io::Error::from(io::ErrorKind::InvalidData));
        }
    };

    // FIXME: rewrite with better performance
    let mut buffer = vec![0u8; fixed_header.remaining_length as usize];
    conn.read_exact(&mut buffer).await?;
    let packet = VariablePacket::decode_with(&mut Cursor::new(buffer), Some(fixed_header))
        .map_err(|err| io::Error::from(io::ErrorKind::InvalidData))?;
    handle_packet(
        session, receiver, packet, conn, current_fd, executor, global,
    )
    .await?;
    Ok(())
}

#[inline]
async fn handle_packet(
    session: &mut Session,
    receiver: &mut Option<Receiver<(ClientId, InternalMessage)>>,
    packet: VariablePacket,
    conn: &mut TcpStream<Preallocated>,
    current_fd: RawFd,
    executor: &Rc<ExecutorState>,
    global: &Arc<GlobalState>,
) -> io::Result<()> {
    if !matches!(packet, VariablePacket::ConnectPacket(..)) && !session.connected {
        log::info!("#{} not connected", current_fd);
        return Err(io::Error::from(io::ErrorKind::InvalidData));
    }
    match packet {
        VariablePacket::ConnectPacket(packet) => {
            handle_connect(
                session, receiver, packet, conn, current_fd, executor, global,
            )
            .await?;
            session.connected = true;
        }
        VariablePacket::DisconnectPacket(packet) => {
            handle_disconnect(session, packet, conn, current_fd, global).await?;
            session.disconnected = true;
        }
        VariablePacket::PublishPacket(packet) => {
            handle_publish(session, packet, conn, current_fd, global).await?;
        }
        VariablePacket::PubackPacket(packet) => {
            handle_puback(session, packet, conn, current_fd, global).await?;
        }
        VariablePacket::PubrecPacket(packet) => {
            handle_pubrec(session, packet, conn, current_fd, global).await?;
        }
        VariablePacket::PubrelPacket(packet) => {
            handle_pubrel(session, packet, conn, current_fd, global).await?;
        }
        VariablePacket::PubcompPacket(packet) => {
            handle_pubcomp(session, packet, conn, current_fd, global).await?;
        }
        VariablePacket::SubscribePacket(packet) => {
            handle_subscribe(session, packet, conn, current_fd, global).await?;
        }
        VariablePacket::UnsubscribePacket(packet) => {
            handle_unsubscribe(session, packet, conn, current_fd, global).await?;
        }
        VariablePacket::PingreqPacket(packet) => {
            handle_pingreq(session, packet, conn, current_fd, global).await?;
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
                let mut buf = Vec::with_capacity(rv_packet.encoded_length() as usize);
                rv_packet.encode(&mut buf)?;
                {
                    let _permit = session.write_lock.acquire_permit(1).await.unwrap();
                    conn.write_all(&buf).await?;
                }
            }
            PendingPacketStatus::Pubrec { packet_id, .. } => {
                let rv_packet = PubrelPacket::new(*packet_id);
                let mut buf = Vec::with_capacity(rv_packet.encoded_length() as usize);
                rv_packet.encode(&mut buf)?;
                {
                    let _permit = session.write_lock.acquire_permit(1).await.unwrap();
                    conn.write_all(&buf).await?;
                }
            }
            PendingPacketStatus::Complete => unreachable!(),
        }
    }

    Ok(())
}

#[inline]
async fn handle_connect(
    session: &mut Session,
    receiver: &mut Option<Receiver<(ClientId, InternalMessage)>>,
    packet: ConnectPacket,
    conn: &mut TcpStream<Preallocated>,
    current_fd: RawFd,
    executor: &Rc<ExecutorState>,
    global: &Arc<GlobalState>,
) -> io::Result<()> {
    log::debug!("#{} received a connect packet: {:#?}", current_fd, packet);
    let mut return_code = ConnectReturnCode::ConnectionAccepted;
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
                        packet.client_identifier()
                    );
                    return_code = ConnectReturnCode::BadUserNameOrPassword;
                }
            }
            _ => panic!("auth method not supported: {:?}", auth_type),
        }
    }

    session.clean_session = packet.clean_session();
    session.client_identifier = packet.client_identifier().to_string();
    session.username = packet.user_name().map(|name| name.to_string());
    session.keep_alive = packet.keep_alive();

    if session.keep_alive > 0 {
        let interval = Duration::from_millis(session.keep_alive as u64 * 500);
        let client_id = session.client_id;
        let last_packet_time = Arc::clone(&session.last_packet_time);
        let global = Arc::clone(global);
        if let Err(err) = TimerActionRepeat::repeat_into(
            move || {
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
                        sender.send_async((client_id, msg)).await;
                    }
                    None
                }
            },
            executor.gc_queue,
        ) {
            log::warn!("spawn executor timer failed: {:?}", err);
            return Err(io::Error::from(io::ErrorKind::Other));
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
            session.server_packet_id = old_state.server_packet_id;
            // FIXME: handle pending messages
            session.pending_packets = old_state.pending_packets;
            *receiver = Some(old_state.receiver);

            session.client_id = old_state.client_id;
            session.subscribes = old_state.subscribes;

            session_present = true;
        }
        AddClientReceipt::New {
            client_id,
            receiver: new_receiver,
        } => {
            session.client_id = client_id;
            *receiver = Some(new_receiver);
        }
    }

    log::debug!(
        "socket fd#{} assgined to: {:?}",
        current_fd,
        session.client_id
    );

    let rv_packet = ConnackPacket::new(session_present, return_code);
    let mut buf = Vec::with_capacity(rv_packet.encoded_length() as usize);
    rv_packet.encode(&mut buf)?;
    {
        let _permit = session.write_lock.acquire_permit(1).await.unwrap();
        conn.write_all(&buf).await?;
    }
    Ok(())
}

#[inline]
async fn handle_disconnect(
    session: &mut Session,
    packet: DisconnectPacket,
    conn: &mut TcpStream<Preallocated>,
    current_fd: RawFd,
    global: &Arc<GlobalState>,
) -> io::Result<()> {
    log::debug!(
        "#{} received a disconnect packet: {:#?}",
        current_fd,
        packet
    );
    session.will = None;
    Ok(())
}

#[inline]
async fn handle_publish(
    session: &mut Session,
    packet: PublishPacket,
    conn: &mut TcpStream<Preallocated>,
    current_fd: RawFd,
    global: &Arc<GlobalState>,
) -> io::Result<()> {
    log::debug!("#{} received a publish packet: {:#?}", current_fd, packet);
    if packet.topic_name().starts_with('$') {
        return Err(io::Error::from(io::ErrorKind::InvalidData));
    }
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
            let rv_packet = PubackPacket::new(pkid);
            let mut buf = Vec::with_capacity(rv_packet.encoded_length() as usize);
            rv_packet.encode(&mut buf)?;
            {
                let _permit = session.write_lock.acquire_permit(1).await.unwrap();
                conn.write_all(&buf).await?;
            }
        }
        QoSWithPacketIdentifier::Level2(pkid) => {
            let rv_packet = PubrecPacket::new(pkid);
            let mut buf = Vec::with_capacity(rv_packet.encoded_length() as usize);
            rv_packet.encode(&mut buf)?;
            {
                let _permit = session.write_lock.acquire_permit(1).await.unwrap();
                conn.write_all(&buf).await?;
            }
        }
    }
    Ok(())
}

#[inline]
async fn handle_puback(
    session: &mut Session,
    packet: PubackPacket,
    conn: &mut TcpStream<Preallocated>,
    current_fd: RawFd,
    global: &Arc<GlobalState>,
) -> io::Result<()> {
    log::debug!("#{} received a puback packet: {:#?}", current_fd, packet);
    session.pending_packets.complete(packet.packet_identifier());
    Ok(())
}

#[inline]
async fn handle_pubrec(
    session: &mut Session,
    packet: PubrecPacket,
    conn: &mut TcpStream<Preallocated>,
    current_fd: RawFd,
    global: &Arc<GlobalState>,
) -> io::Result<()> {
    log::debug!("#{} received a pubrec packet: {:#?}", current_fd, packet);
    session.pending_packets.pubrec(packet.packet_identifier());
    let rv_packet = PubrelPacket::new(packet.packet_identifier());
    let mut buf = Vec::with_capacity(rv_packet.encoded_length() as usize);
    rv_packet.encode(&mut buf)?;
    {
        let _permit = session.write_lock.acquire_permit(1).await.unwrap();
        conn.write_all(&buf).await?;
    }
    Ok(())
}

#[inline]
async fn handle_pubrel(
    session: &mut Session,
    packet: PubrelPacket,
    conn: &mut TcpStream<Preallocated>,
    current_fd: RawFd,
    global: &Arc<GlobalState>,
) -> io::Result<()> {
    log::debug!("#{} received a pubrel packet: {:#?}", current_fd, packet);
    let rv_packet = PubcompPacket::new(packet.packet_identifier());
    let mut buf = Vec::with_capacity(rv_packet.encoded_length() as usize);
    rv_packet.encode(&mut buf)?;
    {
        let _permit = session.write_lock.acquire_permit(1).await.unwrap();
        conn.write_all(&buf).await?;
    }
    Ok(())
}

#[inline]
async fn handle_pubcomp(
    session: &mut Session,
    packet: PubcompPacket,
    conn: &mut TcpStream<Preallocated>,
    current_fd: RawFd,
    global: &Arc<GlobalState>,
) -> io::Result<()> {
    log::debug!("#{} received a pubcomp packet: {:#?}", current_fd, packet);
    session.pending_packets.complete(packet.packet_identifier());
    Ok(())
}

#[inline]
async fn handle_subscribe(
    session: &mut Session,
    packet: SubscribePacket,
    conn: &mut TcpStream<Preallocated>,
    current_fd: RawFd,
    global: &Arc<GlobalState>,
) -> io::Result<()> {
    log::debug!("#{} received a subscribe packet: {:#?}", current_fd, packet);
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
    let mut buf = Vec::with_capacity(rv_packet.encoded_length() as usize);
    rv_packet.encode(&mut buf)?;
    {
        let _permit = session.write_lock.acquire_permit(1).await.unwrap();
        conn.write_all(&buf).await?;
    }
    Ok(())
}

#[inline]
async fn handle_unsubscribe(
    session: &mut Session,
    packet: UnsubscribePacket,
    conn: &mut TcpStream<Preallocated>,
    current_fd: RawFd,
    global: &Arc<GlobalState>,
) -> io::Result<()> {
    log::debug!(
        "#{} received a unsubscribe packet: {:#?}",
        current_fd,
        packet
    );
    for filter in packet.subscribes() {
        global.route_table.unsubscribe(filter, session.client_id);
        session.subscribes.remove(filter);
    }
    let rv_packet = UnsubackPacket::new(packet.packet_identifier());
    let mut buf = Vec::with_capacity(rv_packet.encoded_length() as usize);
    rv_packet.encode(&mut buf)?;
    {
        let _permit = session.write_lock.acquire_permit(1).await.unwrap();
        conn.write_all(&buf).await?;
    }
    Ok(())
}

#[inline]
async fn handle_pingreq(
    session: &mut Session,
    packet: PingreqPacket,
    conn: &mut TcpStream<Preallocated>,
    current_fd: RawFd,
    global: &Arc<GlobalState>,
) -> io::Result<()> {
    log::debug!("#{} received a ping packet", current_fd);
    let rv_packet = PingrespPacket::new();
    let mut buf = Vec::with_capacity(rv_packet.encoded_length() as usize);
    rv_packet.encode(&mut buf)?;
    {
        let _permit = session.write_lock.acquire_permit(1).await.unwrap();
        conn.write_all(&buf).await?;
    }
    Ok(())
}

#[inline]
pub async fn handle_will(
    session: &mut Session,
    conn: &mut TcpStream<Preallocated>,
    current_fd: RawFd,
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

#[inline]
pub async fn handle_internal(
    session: &mut Session,
    receiver: &Receiver<(ClientId, InternalMessage)>,
    sender: ClientId,
    msg: InternalMessage,
    conn: Option<&mut TcpStream<Preallocated>>,
    global: &Arc<GlobalState>,
) -> io::Result<bool> {
    match msg {
        InternalMessage::Online { sender } => {
            // FIXME: read max inflight and max packets from config
            let mut pending_packets = PendingPackets::new(10, 1000, 15);
            mem::swap(&mut session.pending_packets, &mut pending_packets);
            let old_state = SessionState {
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
                "kick client {:?}, reason: {}, offline: {}",
                session.client_id,
                reason,
                session.disconnected
            );
            Ok(!session.disconnected)
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
        if payload.is_empty() {
            if let Some(old_retain_content) = global.retain_table.remove(topic_name) {
                log::debug!(
                    "retain message removed, old retain content: {:?}",
                    old_retain_content
                );
            }
        } else {
            let content = Arc::new(RetainContent::new(
                TopicName::new(topic_name).unwrap(),
                qos,
                Bytes::from(payload.to_vec()),
                session.client_id,
            ));
            if let Some(old_retain_content) = global.retain_table.insert(content) {
                log::debug!(
                    "retain message insert, old retain content: {:?}",
                    old_retain_content
                );
            }
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
async fn recv_publish(
    session: &mut Session,
    topic_name: &Arc<TopicName>,
    qos: QualityOfService,
    retain: bool,
    payload: &Bytes,
    (subscribe_filter, subscribe_qos): (&Arc<TopicFilter>, QualityOfService),
    conn: Option<&mut TcpStream<Preallocated>>,
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
            // TODO:
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
        let mut buf = Vec::with_capacity(rv_packet.encoded_length() as usize);
        rv_packet.encode(&mut buf)?;
        {
            let _permit = session.write_lock.acquire_permit(1).await.unwrap();
            conn.write_all(&buf).await?;
        }
    }

    Ok(())
}
