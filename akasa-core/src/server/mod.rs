pub mod proxy;
#[cfg(target_os = "linux")]
pub mod rt_glommio;
pub mod rt_tokio;

use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use async_native_tls::{TlsAcceptor, TlsStream};
use flume::{bounded, Sender};
use futures_lite::{
    io::{AsyncRead, AsyncWrite},
    FutureExt,
};
use mqtt_proto::{decode_raw_header, v3, v5, Error, Protocol};

use crate::hook::HookRequest;
use crate::protocols::mqtt;
use crate::state::{Executor, GlobalState};

use proxy::{parse_header, Addresses};

const CONNECT_TIMEOUT_SECS: u64 = 5;

pub async fn handle_accept<T: AsyncRead + AsyncWrite + Unpin, E: Executor>(
    mut conn: T,
    conn_args: ConnectionArgs,
    mut peer: SocketAddr,
    hook_requests: Sender<HookRequest>,
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

    // Handle proxy protocol
    let mut tls_sni = None;
    if conn_args.proxy {
        // TODO: handle timeout
        if let Some(header) = parse_header(&mut conn, conn_args.proxy_tls_termination).await? {
            peer = match header.addresses {
                Addresses::IPv4 {
                    source_address,
                    source_port,
                    ..
                } => (source_address, source_port).into(),
                Addresses::IPv6 {
                    source_address,
                    source_port,
                    ..
                } => (source_address, source_port).into(),
                Addresses::Unix { .. } => {
                    log::error!(
                        "Proxy unix address({:?}) is not supported!",
                        header.addresses
                    );
                    return Err(io::ErrorKind::InvalidData.into());
                }
            };
            log::debug!("Proxy protocol TLS SNI: {:?}", header.tls_sni);
            tls_sni = header.tls_sni;
        } else {
            log::debug!("proxy health check");
            return Ok(());
        }
    }

    // TODO: handle timeout
    let mut tls_wrapper = if let Some(tls_acceptor) = conn_args.tls_acceptor {
        let tls_stream = tls_acceptor.accept(conn).await.map_err(|err| {
            log::debug!("accept tls connection error: {:?}", err);
            io::Error::from(io::ErrorKind::InvalidData)
        })?;
        TlsWrapper::Tls(tls_stream)
    } else {
        TlsWrapper::Raw(conn)
    };

    let (packet_type, remaining_len) = decode_raw_header(&mut tls_wrapper)
        .or(async {
            let _ = timeout_receiver.recv_async().await;
            Err(Error::IoError(io::ErrorKind::TimedOut, String::new()))
        })
        .await?;
    if packet_type != 0b00010000 {
        log::debug!("first packet is not CONNECT packet: {}", packet_type);
        return Err(io::ErrorKind::InvalidData.into());
    }
    let protocol = Protocol::decode_async(&mut tls_wrapper)
        .or(async {
            let _ = timeout_receiver.recv_async().await;
            Err(Error::IoError(io::ErrorKind::TimedOut, String::new()))
        })
        .await?;
    match protocol {
        Protocol::V310 | Protocol::V311 => {
            let header = v3::Header::new_with(packet_type, remaining_len).expect("v3 header");
            mqtt::v3::handle_connection(
                tls_wrapper,
                peer,
                header,
                protocol,
                timeout_receiver,
                hook_requests,
                executor,
                global,
            )
            .await?;
        }
        Protocol::V500 => {
            let header = v5::Header::new_with(packet_type, remaining_len).expect("v5 header");
            mqtt::v5::handle_connection(
                tls_wrapper,
                peer,
                header,
                protocol,
                timeout_receiver,
                hook_requests,
                executor,
                global,
            )
            .await?;
        }
    }
    Ok(())
}

#[derive(Clone, Debug)]
pub struct ConnectionArgs {
    proxy: bool,
    proxy_tls_termination: bool,
    websocket: bool,
    tls_acceptor: Option<TlsAcceptor>,
}

enum TlsWrapper<S> {
    Raw(S),
    Tls(TlsStream<S>),
}

impl<S: AsyncRead + AsyncWrite + Unpin> AsyncRead for TlsWrapper<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            TlsWrapper::Raw(conn) => Pin::new(conn).poll_read(cx, buf),
            TlsWrapper::Tls(tls_stream) => Pin::new(tls_stream).poll_read(cx, buf),
        }
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> AsyncWrite for TlsWrapper<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            TlsWrapper::Raw(conn) => Pin::new(conn).poll_write(cx, buf),
            TlsWrapper::Tls(tls_stream) => Pin::new(tls_stream).poll_write(cx, buf),
        }
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            TlsWrapper::Raw(conn) => Pin::new(conn).poll_flush(cx),
            TlsWrapper::Tls(tls_stream) => Pin::new(tls_stream).poll_flush(cx),
        }
    }
    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            TlsWrapper::Raw(conn) => Pin::new(conn).poll_close(cx),
            TlsWrapper::Tls(tls_stream) => Pin::new(tls_stream).poll_close(cx),
        }
    }
}
