#[cfg(target_os = "linux")]
pub mod rt_glommio;
pub mod rt_tokio;

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use flume::bounded;
use futures_lite::{
    io::{AsyncRead, AsyncWrite},
    FutureExt,
};
use mqtt_proto::{decode_raw_header, v3, v5, Error, Protocol};

use crate::protocols::mqtt;
use crate::state::{Executor, GlobalState};

const CONNECT_TIMEOUT_SECS: u64 = 5;

pub async fn handle_accept<T: AsyncRead + AsyncWrite + Unpin, E: Executor>(
    mut conn: T,
    peer: SocketAddr,
    executor: E,
    global: Arc<GlobalState>,
) -> io::Result<()> {
    // If the client don't send enough data in 5 seconds, disconnect it.
    let (timeout_sender, timeout_receiver) = bounded(1);
    executor.spawn_sleep(Duration::from_secs(CONNECT_TIMEOUT_SECS), async move {
        if timeout_sender.send_async(()).await.is_ok() {
            log::info!("connection timeout: {}", peer);
        }
    });

    let (packet_type, remaining_len) = decode_raw_header(&mut conn)
        .or(async {
            let _ = timeout_receiver.recv_async().await;
            Err(Error::IoError(io::ErrorKind::TimedOut, String::new()))
        })
        .await?;
    if packet_type != 0b00010000 {
        log::debug!("first packet is not CONNECT packet: {}", packet_type);
        return Err(io::ErrorKind::InvalidData.into());
    }
    let protocol = Protocol::decode_async(&mut conn)
        .or(async {
            let _ = timeout_receiver.recv_async().await;
            Err(Error::IoError(io::ErrorKind::TimedOut, String::new()))
        })
        .await?;
    match protocol {
        Protocol::V310 | Protocol::V311 => {
            let header = v3::Header::new_with(packet_type, remaining_len).expect("v3 header");
            mqtt::v3::handle_connection(
                conn,
                peer,
                header,
                protocol,
                timeout_receiver,
                executor,
                global,
            )
            .await?;
        }
        Protocol::V500 => {
            let header = v5::Header::new_with(packet_type, remaining_len).expect("v5 header");
            mqtt::v5::handle_connection(
                conn,
                peer,
                header,
                protocol,
                timeout_receiver,
                executor,
                global,
            )
            .await?;
        }
    }
    Ok(())
}
