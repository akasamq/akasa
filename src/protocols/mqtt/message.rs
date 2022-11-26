use std::collections::LinkedList;
use std::io::{self, Cursor};
use std::mem;
use std::os::unix::io::RawFd;
use std::sync::Arc;

use flume::Receiver;
use futures_lite::io::{AsyncReadExt, AsyncWriteExt};
use glommio::{
    net::{Preallocated, TcpStream},
    sync::Semaphore,
};

use mqtt::{
    control::{
        fixed_header::FixedHeader, packet_type::PacketType, variable_header::ConnectReturnCode,
    },
    packet::{
        suback::SubscribeReturnCode, ConnackPacket, ConnectPacket, DisconnectPacket, PingreqPacket,
        PingrespPacket, PubackPacket, PublishPacket, QoSWithPacketIdentifier, SubackPacket,
        SubscribePacket, UnsubackPacket, UnsubscribePacket, VariablePacket,
    },
    qos::QualityOfService,
    Decodable, Encodable, TopicName,
};

use crate::config::{AuthType, Config};
use crate::state::{AddClientReceipt, ClientId, GlobalState, InternalMsg};

pub struct Session {
    pub io_error: Option<io::Error>,
    connected: bool,
    disconnected: bool,
    write_lock: Semaphore,
    // for record packet id send from server to client
    packet_id: u16,
    // For assign a message id received from inernal sender (pub/sub)
    message_id: u64,
    pending_messages: LinkedList<(u64, Arc<InternalMsg>)>,

    client_id: ClientId,
    client_identifier: String,
    username: Option<String>,
    clean_session: bool,
    will: Option<Will>,
}

pub struct SessionState {
    // for record packet id send from server to client
    packet_id: u16,
    // For assign a message id received from inernal sender (pub/sub)
    message_id: u64,
    pending_messages: LinkedList<(u64, Arc<InternalMsg>)>,
    receiver: Receiver<(ClientId, Arc<InternalMsg>)>,

    client_id: ClientId,
}

impl Session {
    pub fn new() -> Session {
        Session {
            io_error: None,
            connected: false,
            disconnected: false,
            write_lock: Semaphore::new(1),
            packet_id: 0,
            message_id: 0,
            pending_messages: LinkedList::new(),

            client_id: ClientId::default(),
            client_identifier: String::new(),
            username: None,
            clean_session: true,
            will: None,
        }
    }

    pub fn clean_session(&self) -> bool {
        self.clean_session
    }
    pub fn disconnected(&self) -> bool {
        self.disconnected
    }
    pub fn client_id(&self) -> ClientId {
        self.client_id
    }

    fn push_message(&mut self, msg: Arc<InternalMsg>) {
        self.pending_messages.push_back((self.message_id, msg));
        self.message_id += 1;
    }
    fn incr_packet_id(&mut self) -> u16 {
        let old_value = self.packet_id;
        self.packet_id = self.packet_id.wrapping_add(1);
        old_value
    }
}

pub struct Will {
    retain: bool,
    qos: QualityOfService,
    topic: TopicName,
    message: Vec<u8>,
}

pub async fn handle_connection(
    session: &mut Session,
    receiver: &mut Option<Receiver<(ClientId, Arc<InternalMsg>)>>,
    conn: &mut TcpStream<Preallocated>,
    current_fd: RawFd,
    config: &Arc<Config>,
    global_state: &Arc<GlobalState>,
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
        session,
        receiver,
        packet,
        conn,
        current_fd,
        config,
        global_state,
    )
    .await?;
    Ok(())
}

#[inline]
async fn handle_packet(
    session: &mut Session,
    receiver: &mut Option<Receiver<(ClientId, Arc<InternalMsg>)>>,
    packet: VariablePacket,
    conn: &mut TcpStream<Preallocated>,
    current_fd: RawFd,
    config: &Arc<Config>,
    global_state: &Arc<GlobalState>,
) -> io::Result<()> {
    if !matches!(packet, VariablePacket::ConnectPacket(..)) && !session.connected {
        log::info!("#{} not connected", current_fd);
        return Err(io::Error::from(io::ErrorKind::InvalidData));
    }
    match packet {
        VariablePacket::ConnectPacket(packet) => {
            handle_connect(
                session,
                receiver,
                packet,
                conn,
                current_fd,
                config,
                global_state,
            )
            .await?;
            session.connected = true;
        }
        VariablePacket::DisconnectPacket(packet) => {
            handle_disconnect(session, packet, conn, current_fd, global_state).await?;
            session.disconnected = true;
        }
        VariablePacket::PublishPacket(packet) => {
            handle_publish(session, packet, conn, current_fd, global_state).await?;
        }
        VariablePacket::SubscribePacket(packet) => {
            handle_subscribe(session, packet, conn, current_fd, global_state).await?;
        }
        VariablePacket::UnsubscribePacket(packet) => {
            handle_unsubscribe(session, packet, conn, current_fd, global_state).await?;
        }
        VariablePacket::PingreqPacket(packet) => {
            handle_pingreq(session, packet, conn, current_fd, global_state).await?;
        }
        _ => {
            return Err(io::Error::from(io::ErrorKind::InvalidData));
        }
    }
    Ok(())
}

#[inline]
async fn handle_connect(
    session: &mut Session,
    receiver: &mut Option<Receiver<(ClientId, Arc<InternalMsg>)>>,
    packet: ConnectPacket,
    conn: &mut TcpStream<Preallocated>,
    current_fd: RawFd,
    config: &Arc<Config>,
    global_state: &Arc<GlobalState>,
) -> io::Result<()> {
    log::debug!("#{} received a connect packet: {:#?}", current_fd, packet);
    let mut return_code = ConnectReturnCode::ConnectionAccepted;
    for auth_type in &config.auth_types {
        match auth_type {
            AuthType::UsernamePassword => {
                if let Some(username) = packet.user_name() {
                    if config.users.get(username).map(|s| s.as_str()) != packet.password() {
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
    if let Some((topic, message)) = packet.will() {
        let will_qos = match packet.will_qos() {
            0 => QualityOfService::Level0,
            1 => QualityOfService::Level1,
            2 => QualityOfService::Level2,
            _ => unreachable!(),
        };
        let will = Will {
            retain: packet.will_retain(),
            qos: will_qos,
            topic: TopicName::new(topic).expect("topic name"),
            message: message.to_vec(),
        };
        session.will = Some(will);
    }

    let mut session_present = false;
    match global_state
        .add_client(session.client_identifier.as_str())
        .await
    {
        AddClientReceipt::Present(old_state) => {
            session.packet_id = old_state.packet_id;
            session.message_id = old_state.message_id;
            // FIXME: handle pending messages
            session.pending_messages = old_state.pending_messages;
            *receiver = Some(old_state.receiver);

            session.client_id = old_state.client_id;

            session_present = true;
        }
        AddClientReceipt::New {
            client_id,
            receiver: new_receiver,
        } => {
            *receiver = Some(new_receiver);
            session.client_id = client_id;
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
    global_state: &Arc<GlobalState>,
) -> io::Result<()> {
    log::debug!(
        "#{} received a disconnect packet: {:#?}",
        current_fd,
        packet
    );
    Ok(())
}

#[inline]
async fn handle_publish(
    session: &mut Session,
    packet: PublishPacket,
    conn: &mut TcpStream<Preallocated>,
    current_fd: RawFd,
    global_state: &Arc<GlobalState>,
) -> io::Result<()> {
    log::debug!("#{} received a publish packet: {:#?}", current_fd, packet);
    let msg = Arc::new(InternalMsg::Publish {
        topic_name: TopicName::new(packet.topic_name()).unwrap(),
        qos: packet.qos().split().0,
        payload: packet.payload().to_vec(),
    });
    let matches = global_state.route_table.get_matches(packet.topic_name());
    let mut senders = Vec::new();
    for content in matches {
        let content = content.read();
        for (client_id, qos) in &content.clients {
            if let Some((sender_client_id, sender)) = global_state.get_client_sender(client_id) {
                senders.push((sender_client_id, sender));
            }
        }
    }

    for (sender_client_id, sender) in senders {
        if let Err(err) = sender
            .send_async((session.client_id, Arc::clone(&msg)))
            .await
        {
            log::error!(
                "send publish to connection {:?} failed: {}",
                sender_client_id,
                err
            );
        }
    }

    if let Some(pkid) = packet.qos().split().1 {
        let rv_packet = PubackPacket::new(pkid);
        let mut buf = Vec::with_capacity(rv_packet.encoded_length() as usize);
        rv_packet.encode(&mut buf)?;
        {
            let _permit = session.write_lock.acquire_permit(1).await.unwrap();
            conn.write_all(&buf).await?;
        }
    }
    Ok(())
}

#[inline]
async fn handle_subscribe(
    session: &mut Session,
    packet: SubscribePacket,
    conn: &mut TcpStream<Preallocated>,
    current_fd: RawFd,
    global_state: &Arc<GlobalState>,
) -> io::Result<()> {
    log::debug!("#{} received a subscribe packet: {:#?}", current_fd, packet);
    let mut return_codes = Vec::with_capacity(packet.subscribes().len());
    for (filter, qos) in packet.subscribes() {
        global_state
            .route_table
            .subscribe(filter, session.client_id, *qos);
        // FIXME: max qos
        return_codes.push(SubscribeReturnCode::MaximumQoSLevel0);
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
    global_state: &Arc<GlobalState>,
) -> io::Result<()> {
    log::debug!(
        "#{} received a unsubscribe packet: {:#?}",
        current_fd,
        packet
    );
    for filter in packet.subscribes() {
        global_state
            .route_table
            .unsubscribe(filter, session.client_id);
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
    global_state: &Arc<GlobalState>,
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
    global_state: &Arc<GlobalState>,
) -> io::Result<()> {
    if let Some(will) = session.will.take() {
        // FIXME: handle will message
    }
    Ok(())
}

#[inline]
pub async fn handle_internal(
    session: &mut Session,
    receiver: &Receiver<(ClientId, Arc<InternalMsg>)>,
    sender: ClientId,
    msg: Arc<InternalMsg>,
    conn: Option<&mut TcpStream<Preallocated>>,
    config: &Arc<Config>,
    global_state: &Arc<GlobalState>,
) -> io::Result<bool> {
    match msg.as_ref() {
        InternalMsg::Online { sender } => {
            let mut pending_messages = LinkedList::new();
            mem::swap(&mut session.pending_messages, &mut pending_messages);
            let old_state = SessionState {
                packet_id: session.packet_id,
                message_id: session.message_id,
                pending_messages,
                receiver: receiver.clone(),
                client_id: session.client_id,
            };
            sender.send_async(old_state).await.unwrap();
            return Ok(true);
        }
        InternalMsg::Publish {
            topic_name,
            qos,
            payload,
        } => {
            if let Some(conn) = conn {
                log::debug!(
                    "{:?} received publish message from {:?}",
                    session.client_id,
                    sender
                );
                // FIXME: qos 1/2 message queue
                let qos_with_id = match qos {
                    QualityOfService::Level0 => QoSWithPacketIdentifier::Level0,
                    QualityOfService::Level1 => {
                        let packet_id = session.incr_packet_id();
                        QoSWithPacketIdentifier::Level1(packet_id)
                    }
                    QualityOfService::Level2 => {
                        let packet_id = session.incr_packet_id();
                        QoSWithPacketIdentifier::Level2(packet_id)
                    }
                };
                let rv_packet =
                    PublishPacket::new(topic_name.clone(), qos_with_id, payload.to_vec());
                let mut buf = Vec::with_capacity(rv_packet.encoded_length() as usize);
                rv_packet.encode(&mut buf)?;
                {
                    let _permit = session.write_lock.acquire_permit(1).await.unwrap();
                    conn.write_all(&buf).await?;
                }
            } else if qos != &QualityOfService::Level0 {
                session.push_message(msg);
            }
        }
    }
    Ok(false)
}
