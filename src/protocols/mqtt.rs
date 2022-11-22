use std::io::{self, Cursor};
use std::os::unix::io::RawFd;
use std::sync::Arc;

use futures_lite::io::{AsyncReadExt, AsyncWriteExt};
use glommio::net::{Preallocated, TcpStream};

use mqtt::{
    control::{
        fixed_header::FixedHeader, packet_type::PacketType, variable_header::ConnectReturnCode,
    },
    packet::{
        suback::SubscribeReturnCode, ConnackPacket, ConnectPacket, DisconnectPacket, PingreqPacket,
        PingrespPacket, PubackPacket, PublishPacket, SubackPacket, SubscribePacket, UnsubackPacket,
        UnsubscribePacket, VariablePacket,
    },
    Decodable, Encodable, TopicName,
};

use crate::state::{GlobalState, InternalMsg};

#[derive(Default)]
pub struct Session {
    connected: bool,
    disconnected: bool,
}

pub async fn handle_conntion(
    session: &mut Session,
    conn: &mut TcpStream<Preallocated>,
    current_fd: RawFd,
    global_state: &Arc<GlobalState>,
) -> io::Result<bool> {
    let mut buf = [0u8; 1];
    let n = conn.read(&mut buf).await?;
    if n == 0 {
        return Ok(true);
    }
    let type_val = buf[0];

    let mut remaining_len = 0;
    let mut i = 0;
    loop {
        let n = conn.read(&mut buf).await?;
        if n == 0 {
            return Ok(true);
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
    handle_packet(session, packet, conn, current_fd, global_state).await?;
    Ok(session.disconnected)
}

#[inline]
async fn handle_packet(
    session: &mut Session,
    packet: VariablePacket,
    conn: &mut TcpStream<Preallocated>,
    current_fd: RawFd,
    global_state: &Arc<GlobalState>,
) -> io::Result<()> {
    if !matches!(packet, VariablePacket::ConnectPacket(..)) && !session.connected {
        log::info!("#{} not connected", current_fd);
        return Err(io::Error::from(io::ErrorKind::InvalidData));
    }
    match packet {
        VariablePacket::ConnectPacket(packet) => {
            handle_connect(packet, conn, current_fd, global_state).await?;
            session.connected = true;
        }
        VariablePacket::DisconnectPacket(packet) => {
            handle_disconnect(packet, conn, current_fd, global_state).await?;
            session.disconnected = true;
        }
        VariablePacket::PublishPacket(packet) => {
            handle_publish(packet, conn, current_fd, global_state).await?;
        }
        VariablePacket::SubscribePacket(packet) => {
            handle_subscribe(packet, conn, current_fd, global_state).await?;
        }
        VariablePacket::UnsubscribePacket(packet) => {
            handle_unsubscribe(packet, conn, current_fd, global_state).await?;
        }
        VariablePacket::PingreqPacket(packet) => {
            handle_pingreq(packet, conn, current_fd, global_state).await?;
        }
        _ => {
            return Err(io::Error::from(io::ErrorKind::InvalidData));
        }
    }
    Ok(())
}

#[inline]
async fn handle_connect(
    packet: ConnectPacket,
    conn: &mut TcpStream<Preallocated>,
    current_fd: RawFd,
    global_state: &Arc<GlobalState>,
) -> io::Result<()> {
    log::debug!("#{} received a connect packet: {:#?}", current_fd, packet);
    let rv_packet = ConnackPacket::new(false, ConnectReturnCode::ConnectionAccepted);
    let mut buf = Vec::with_capacity(rv_packet.encoded_length() as usize);
    rv_packet.encode(&mut buf)?;
    conn.write_all(&buf).await?;
    Ok(())
}

#[inline]
async fn handle_disconnect(
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
    packet: PublishPacket,
    conn: &mut TcpStream<Preallocated>,
    current_fd: RawFd,
    global_state: &Arc<GlobalState>,
) -> io::Result<()> {
    log::debug!("#{} received a publish packet: {:#?}", current_fd, packet);
    let msg = Arc::new(InternalMsg::Publish {
        topic_name: TopicName::new(packet.topic_name()).unwrap(),
        qos: packet.qos(),
        payload: packet.payload().to_vec(),
    });
    let matches = global_state.route_table.get_matches(packet.topic_name());
    let mut senders = Vec::new();
    for content in matches {
        let content = content.read();
        for (fd, qos) in &content.clients {
            if let Some(pair) = global_state.connections.get(fd) {
                senders.push((*pair.key(), pair.value().clone()));
            }
        }
    }

    for (sender_fd, sender) in senders {
        if let Err(err) = sender.send_async((current_fd, Arc::clone(&msg))).await {
            log::error!("send publish to connection #{} failed: {}", sender_fd, err);
        }
    }

    if let Some(pkid) = packet.qos().split().1 {
        let rv_packet = PubackPacket::new(pkid);
        let mut buf = Vec::with_capacity(rv_packet.encoded_length() as usize);
        rv_packet.encode(&mut buf)?;
        conn.write_all(&buf).await?;
    }
    Ok(())
}

#[inline]
async fn handle_subscribe(
    packet: SubscribePacket,
    conn: &mut TcpStream<Preallocated>,
    current_fd: RawFd,
    global_state: &Arc<GlobalState>,
) -> io::Result<()> {
    log::debug!("#{} received a subscribe packet: {:#?}", current_fd, packet);
    let mut return_codes = Vec::with_capacity(packet.subscribes().len());
    for (filter, qos) in packet.subscribes() {
        global_state.route_table.subscribe(filter, current_fd, *qos);
        return_codes.push(SubscribeReturnCode::MaximumQoSLevel0);
    }
    let rv_packet = SubackPacket::new(packet.packet_identifier(), return_codes);
    let mut buf = Vec::with_capacity(rv_packet.encoded_length() as usize);
    rv_packet.encode(&mut buf)?;
    conn.write_all(&buf).await?;
    Ok(())
}

#[inline]
async fn handle_unsubscribe(
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
        global_state.route_table.unsubscribe(filter, current_fd);
    }
    let rv_packet = UnsubackPacket::new(packet.packet_identifier());
    let mut buf = Vec::with_capacity(rv_packet.encoded_length() as usize);
    rv_packet.encode(&mut buf)?;
    conn.write_all(&buf).await?;
    Ok(())
}

#[inline]
async fn handle_pingreq(
    packet: PingreqPacket,
    conn: &mut TcpStream<Preallocated>,
    current_fd: RawFd,
    global_state: &Arc<GlobalState>,
) -> io::Result<()> {
    log::debug!("#{} received a ping packet", current_fd);
    let rv_packet = PingrespPacket::new();
    let mut buf = Vec::with_capacity(rv_packet.encoded_length() as usize);
    rv_packet.encode(&mut buf)?;
    conn.write_all(&buf).await?;
    Ok(())
}

#[inline]
pub async fn handle_internal(
    sender: RawFd,
    msg: Arc<InternalMsg>,
    conn: &mut TcpStream<Preallocated>,
    current_fd: RawFd,
    global_state: &Arc<GlobalState>,
) -> io::Result<()> {
    match msg.as_ref() {
        InternalMsg::Publish {
            topic_name,
            qos,
            payload,
        } => {
            log::debug!("#{} received publish message from #{}", current_fd, sender);
            let rv_packet = PublishPacket::new(topic_name.clone(), *qos, payload.to_vec());
            let mut buf = Vec::with_capacity(rv_packet.encoded_length() as usize);
            rv_packet.encode(&mut buf)?;
            conn.write_all(&buf).await?;
        }
    }
    Ok(())
}
