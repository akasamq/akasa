use std::io::{self, Cursor};
use std::os::unix::io::RawFd;
use std::sync::Arc;

use bytes::Bytes;
use dashmap::DashMap;
use flume::Sender;
use futures_lite::io::AsyncReadExt;
use glommio::net::{Preallocated, TcpStream};

use mqtt::{
    control::{
        fixed_header::FixedHeader,
        packet_type::{ControlType, PacketType},
    },
    packet::{
        ConnectPacket, DisconnectPacket, PingreqPacket, PublishPacket, SubscribePacket,
        UnsubscribePacket, VariablePacket,
    },
    Decodable,
};

use crate::state::{GlobalState, InternalMsg};

pub async fn handle_conntion(
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

    let mut buffer = vec![0u8; fixed_header.remaining_length as usize];
    conn.read_exact(&mut buffer).await?;
    let packet = VariablePacket::decode_with(&mut Cursor::new(buffer), Some(fixed_header))
        .map_err(|err| io::Error::from(io::ErrorKind::InvalidData))?;
    handle_packet(packet, current_fd, global_state).await?;
    Ok(false)
}

async fn handle_packet(
    packet: VariablePacket,
    current_fd: RawFd,
    global_state: &Arc<GlobalState>,
) -> io::Result<()> {
    match packet {
        VariablePacket::ConnectPacket(packet) => {
            handle_connect(packet, current_fd, global_state).await?;
        }
        VariablePacket::DisconnectPacket(packet) => {
            handle_disconnect(packet, current_fd, global_state).await?;
        }
        VariablePacket::PublishPacket(packet) => {
            handle_publish(packet, current_fd, global_state).await?;
        }
        VariablePacket::SubscribePacket(packet) => {
            handle_subscribe(packet, current_fd, global_state).await?;
        }
        VariablePacket::UnsubscribePacket(packet) => {
            handle_unsubscribe(packet, current_fd, global_state).await?;
        }
        VariablePacket::PingreqPacket(packet) => {
            handle_pingreq(packet, current_fd, global_state).await?;
        }
        _ => {
            return Err(io::Error::from(io::ErrorKind::InvalidData));
        }
    }
    Ok(())
}

async fn handle_connect(
    packet: ConnectPacket,
    current_fd: RawFd,
    global_state: &Arc<GlobalState>,
) -> io::Result<()> {
    Ok(())
}
async fn handle_disconnect(
    packet: DisconnectPacket,
    current_fd: RawFd,
    global_state: &Arc<GlobalState>,
) -> io::Result<()> {
    Ok(())
}
async fn handle_publish(
    packet: PublishPacket,
    current_fd: RawFd,
    global_state: &Arc<GlobalState>,
) -> io::Result<()> {
    Ok(())
}
async fn handle_subscribe(
    packet: SubscribePacket,
    current_fd: RawFd,
    global_state: &Arc<GlobalState>,
) -> io::Result<()> {
    Ok(())
}
async fn handle_unsubscribe(
    packet: UnsubscribePacket,
    current_fd: RawFd,
    global_state: &Arc<GlobalState>,
) -> io::Result<()> {
    Ok(())
}
async fn handle_pingreq(
    packet: PingreqPacket,
    current_fd: RawFd,
    global_state: &Arc<GlobalState>,
) -> io::Result<()> {
    Ok(())
}

pub async fn handle_internal(
    sender: RawFd,
    msg: InternalMsg,
    conn: &mut TcpStream<Preallocated>,
    current_fd: RawFd,
    global_state: &Arc<GlobalState>,
) -> io::Result<()> {
    Ok(())
}
