pub mod rt_glommio;
pub mod rt_tokio;

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;

use futures_lite::io::{AsyncRead, AsyncWrite};
use mqtt_proto::{decode_raw_header, v3, v5, Protocol};

use crate::protocols::mqtt;
use crate::state::{Executor, GlobalState};

pub async fn handle_accept<T: AsyncRead + AsyncWrite + Unpin, E: Executor>(
    mut conn: T,
    peer: SocketAddr,
    executor: E,
    global: Arc<GlobalState>,
) -> io::Result<()> {
    let (packet_type, remaining_len) = decode_raw_header(&mut conn).await?;
    if packet_type != 0b00010000 {
        log::debug!("first packet is not CONNECT packet: {}", packet_type);
        return Err(io::ErrorKind::InvalidData.into());
    }
    let protocol = Protocol::decode_async(&mut conn).await?;
    match protocol {
        Protocol::V310 | Protocol::V311 => {
            let header = v3::Header::new_with(packet_type, remaining_len).expect("v3 header");
            mqtt::v3::handle_connection(conn, peer, header, protocol, executor, global).await?;
        }
        Protocol::V500 => {
            let header = v5::Header::new_with(packet_type, remaining_len).expect("v5 header");
            mqtt::v5::handle_connection(conn, peer, header, protocol, executor, global).await?;
        }
    }
    Ok(())
}
