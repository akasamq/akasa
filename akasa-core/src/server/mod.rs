mod io_compat;
mod proxy;
#[cfg(target_os = "linux")]
pub mod rt_glommio;
pub mod rt_tokio;
#[allow(dead_code)]
mod tls;

use std::cmp;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use async_tungstenite::{tungstenite::Message, WebSocketStream};
use flume::{bounded, Sender};
use futures_lite::{
    io::{AsyncRead, AsyncWrite},
    FutureExt, Stream,
};
use futures_sink::Sink;
use futures_util::TryFutureExt;
use mqtt_proto::{decode_raw_header, v3, v5, Error, Protocol};
use openssl::ssl::{NameType, Ssl, SslAcceptor, SslFiletype, SslMethod, SslVerifyMode};

use crate::config::TlsListener;
use crate::hook::HookRequest;
use crate::protocols::mqtt;
use crate::state::{Executor, GlobalState};

use io_compat::IoWrapper;
use proxy::{parse_header, Addresses};
use tls::SslStream;

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
        if let Some(header) = parse_header(&mut conn, conn_args.proxy_tls_termination)
            .or(async {
                let _ = timeout_receiver.recv_async().await;
                Err(io::ErrorKind::TimedOut.into())
            })
            .await?
        {
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
            log::info!("Proxy protocol health check");
            return Ok(());
        }
    }

    // Handle TLS
    let tls_wrapper = if let Some(acceptor) = conn_args.tls_acceptor {
        let ssl = Ssl::new(acceptor.context()).map_err(|err| {
            log::error!("Create TLS session failed: {:?}", err);
            io::Error::from(io::ErrorKind::BrokenPipe)
        })?;
        let mut tls_stream = SslStream::new(ssl, conn).map_err(|err| {
            log::error!("create TLS stream error: {:?}", err);
            io::Error::from(io::ErrorKind::BrokenPipe)
        })?;
        Pin::new(&mut tls_stream)
            .accept()
            .map_err(|err| {
                log::debug!("accept tls connection error: {:?}", err);
                io::Error::from(io::ErrorKind::InvalidData)
            })
            .or(async {
                let _ = timeout_receiver.recv_async().await;
                Err(io::ErrorKind::TimedOut.into())
            })
            .await?;
        tls_sni = tls_stream
            .ssl()
            .servername(NameType::HOST_NAME)
            .map(ToOwned::to_owned);
        TlsWrapper::Tls(tls_stream)
    } else {
        TlsWrapper::Raw(conn)
    };

    log::debug!("TLS host name(SNI): {:?}", tls_sni);

    // Handle WebSocket
    let mut ws_wrapper = if conn_args.websocket {
        let stream = match async_tungstenite::accept_async(tls_wrapper).await {
            Ok(stream) => stream,
            Err(err) => {
                log::warn!("Accept websocket connection error: {:?}", err);
                return Err(io::ErrorKind::InvalidData.into());
            }
        };
        WebSocketWrapper::WebSocket {
            stream,
            read_data: Vec::new(),
            read_data_idx: 0,
            pending_pong: None,
            closed: false,
        }
    } else {
        WebSocketWrapper::Raw(tls_wrapper)
    };

    let (packet_type, remaining_len) = decode_raw_header(&mut ws_wrapper)
        .or(async {
            let _ = timeout_receiver.recv_async().await;
            Err(Error::IoError(io::ErrorKind::TimedOut, String::new()))
        })
        .await?;
    if packet_type != 0b00010000 {
        log::debug!("first packet is not CONNECT packet: {}", packet_type);
        return Err(io::ErrorKind::InvalidData.into());
    }
    let protocol = Protocol::decode_async(&mut ws_wrapper)
        .or(async {
            let _ = timeout_receiver.recv_async().await;
            Err(Error::IoError(io::ErrorKind::TimedOut, String::new()))
        })
        .await?;
    match protocol {
        Protocol::V310 | Protocol::V311 => {
            let header = v3::Header::new_with(packet_type, remaining_len).expect("v3 header");
            mqtt::v3::handle_connection(
                ws_wrapper,
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
                ws_wrapper,
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

fn build_tls_context(listener: &TlsListener) -> io::Result<SslAcceptor> {
    if listener.verify_peer && listener.ca_file.is_none() {
        log::error!("When `verify_peer` is true `ca_file` must be presented!");
        return Err(io::Error::from(io::ErrorKind::InvalidInput));
    }
    let mut acceptor = SslAcceptor::mozilla_intermediate(SslMethod::tls()).map_err(|err| {
        log::error!("Initialize SslAcceptor failed: {:?}", err);
        io::Error::from(io::ErrorKind::InvalidInput)
    })?;
    if let Some(ca_file) = listener.ca_file.as_ref() {
        acceptor.set_ca_file(ca_file).map_err(|err| {
            log::error!("Invalid CA-certfile: {}", err);
            io::Error::from(io::ErrorKind::InvalidInput)
        })?;
    }
    acceptor
        .set_private_key_file(&listener.key_file, SslFiletype::PEM)
        .map_err(|err| {
            log::error!("Invalid keyfile: {}", err);
            io::Error::from(io::ErrorKind::InvalidInput)
        })?;
    acceptor
        .set_certificate_chain_file(&listener.cert_file)
        .map_err(|err| {
            log::error!("Invalid certfile: {}", err);
            io::Error::from(io::ErrorKind::InvalidInput)
        })?;
    let mut verify_mode = SslVerifyMode::NONE;
    if listener.verify_peer {
        verify_mode.insert(SslVerifyMode::PEER);
    }
    if listener.fail_if_no_peer_cert {
        verify_mode.insert(SslVerifyMode::FAIL_IF_NO_PEER_CERT);
    }
    acceptor.set_verify(verify_mode);
    Ok(acceptor.build())
}

#[derive(Clone)]
pub struct ConnectionArgs {
    pub(crate) addr: SocketAddr,
    pub(crate) proxy: bool,
    pub(crate) proxy_tls_termination: bool,
    pub(crate) websocket: bool,
    pub(crate) tls_acceptor: Option<SslAcceptor>,
}

enum TlsWrapper<S> {
    Raw(S),
    Tls(SslStream<S>),
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

enum WebSocketWrapper<S> {
    Raw(TlsWrapper<S>),
    WebSocket {
        stream: WebSocketStream<TlsWrapper<S>>,
        read_data: Vec<u8>,
        read_data_idx: usize,
        pending_pong: Option<Vec<u8>>,
        closed: bool,
    },
}

fn ws_send_pong<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut WebSocketStream<TlsWrapper<S>>,
    pong: &mut Option<Vec<u8>>,
    cx: &mut Context<'_>,
) -> io::Result<()> {
    if let Some(data) = pong.take() {
        let mut sink = Pin::new(stream);
        match sink.as_mut().poll_ready(cx) {
            Poll::Ready(Ok(_)) => {
                sink.as_mut()
                    .start_send(Message::Pong(data))
                    .map_err(|err| {
                        log::debug!("WebSocket send pong error: {:?}", err);
                        io::Error::from(io::ErrorKind::BrokenPipe)
                    })?;
                let _ignore = sink.as_mut().poll_flush(cx).map_err(|err| {
                    log::debug!("WebSocket flush pong error: {:?}", err);
                    io::Error::from(io::ErrorKind::BrokenPipe)
                })?;
                Ok(())
            }
            Poll::Pending => {
                *pong = Some(data);
                Ok(())
            }
            Poll::Ready(Err(_)) => Err(Into::into(io::ErrorKind::BrokenPipe)),
        }
    } else {
        Ok(())
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> AsyncRead for WebSocketWrapper<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            WebSocketWrapper::Raw(conn) => Pin::new(conn).poll_read(cx, buf),
            WebSocketWrapper::WebSocket {
                stream,
                read_data,
                read_data_idx,
                pending_pong,
                closed,
            } => {
                fn copy_data(buf: &mut [u8], data: &[u8], data_idx: &mut usize) -> usize {
                    let amt = cmp::min(data.len() - *data_idx, buf.len());
                    buf.copy_from_slice(&data[*data_idx..*data_idx + amt]);
                    *data_idx += amt;
                    amt
                }
                if !*closed {
                    ws_send_pong(stream, pending_pong, cx)?;
                }
                if *read_data_idx < read_data.len() {
                    return Poll::Ready(Ok(copy_data(buf, read_data, read_data_idx)));
                }
                loop {
                    match Pin::new(&mut *stream).poll_next(cx) {
                        Poll::Ready(Some(Ok(msg))) => match msg {
                            Message::Binary(bin) => {
                                *read_data = bin;
                                *read_data_idx = 0;
                                return Poll::Ready(Ok(copy_data(buf, read_data, read_data_idx)));
                            }
                            Message::Close(_) => return Poll::Ready(Ok(0)),
                            Message::Ping(data) => {
                                *pending_pong = Some(data);
                                ws_send_pong(stream, pending_pong, cx)?;
                            }
                            Message::Pong(_) => {
                                log::debug!("WebSocket pong message not allowed!");
                                return Poll::Ready(Err(io::ErrorKind::InvalidData.into()));
                            }
                            Message::Text(_) => {
                                log::debug!("WebSocket text message not allowed!");
                                return Poll::Ready(Err(io::ErrorKind::InvalidData.into()));
                            }
                            Message::Frame(_) => {
                                log::debug!("WebSocket frame message not allowed!");
                                return Poll::Ready(Err(io::ErrorKind::InvalidData.into()));
                            }
                        },
                        Poll::Ready(Some(Err(err))) => {
                            log::debug!("WebSocket read error: {:?}", err);
                            return Poll::Ready(Err(io::ErrorKind::BrokenPipe.into()));
                        }
                        Poll::Ready(None) => return Poll::Ready(Ok(0)),
                        Poll::Pending => return Poll::Pending,
                    }
                }
            }
        }
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> AsyncWrite for WebSocketWrapper<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            WebSocketWrapper::Raw(conn) => Pin::new(conn).poll_write(cx, buf),
            WebSocketWrapper::WebSocket {
                stream,
                pending_pong,
                closed,
                ..
            } => {
                if !*closed {
                    ws_send_pong(stream, pending_pong, cx)?;
                }
                match Pin::new(&mut *stream).poll_ready(cx) {
                    Poll::Ready(Ok(())) => {}
                    Poll::Ready(Err(err)) => {
                        log::debug!("WebSocket write error: {:?}", err);
                        return Poll::Ready(Err(io::ErrorKind::BrokenPipe.into()));
                    }
                    Poll::Pending => return Poll::Pending,
                }
                let message = Message::Binary(buf.to_vec());
                if let Err(err) = Pin::new(&mut *stream).start_send(message) {
                    log::debug!("WebSocket write error: {:?}", err);
                    return Poll::Ready(Err(io::ErrorKind::BrokenPipe.into()));
                }
                Poll::Ready(Ok(buf.len()))
            }
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            WebSocketWrapper::Raw(conn) => Pin::new(conn).poll_flush(cx),
            WebSocketWrapper::WebSocket { stream, .. } => {
                Pin::new(stream).poll_flush(cx).map_err(|err| {
                    log::debug!("WebSocket flush error: {:?}", err);
                    io::ErrorKind::BrokenPipe.into()
                })
            }
        }
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            WebSocketWrapper::Raw(conn) => Pin::new(conn).poll_close(cx),
            WebSocketWrapper::WebSocket { stream, closed, .. } => {
                if !*closed {
                    let mut sink = Pin::new(&mut *stream);
                    match sink.as_mut().poll_ready(cx) {
                        Poll::Ready(Ok(())) => {
                            sink.as_mut()
                                .start_send(Message::Close(None))
                                .map_err(|err| {
                                    log::debug!("WebSocket send close error: {:?}", err);
                                    io::Error::from(io::ErrorKind::BrokenPipe)
                                })?;
                            let _ = sink.as_mut().poll_flush(cx).map_err(|err| {
                                log::debug!("WebSocket flush close error: {:?}", err);
                                io::Error::from(io::ErrorKind::BrokenPipe)
                            })?;
                            *closed = true;
                        }
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(Err(_)) => {
                            return Poll::Ready(Err(Into::into(io::ErrorKind::BrokenPipe)))
                        }
                    }
                }
                Pin::new(&mut *stream).poll_close(cx).map_err(|err| {
                    log::debug!("WebSocket poll_close error: {:?}", err);
                    io::ErrorKind::BrokenPipe.into()
                })
            }
        }
    }
}
