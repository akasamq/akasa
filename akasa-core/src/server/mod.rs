#[cfg(feature = "http")]
mod http_api;
mod proxy;
pub mod rt;

use std::cmp;
use std::fs::File;
use std::io;
use std::io::BufReader;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use flume::bounded;
use futures_lite::{FutureExt, Stream};
use futures_sink::Sink;
use futures_util::TryFutureExt;
use mqtt_proto::{decode_raw_header_async, v3, v5, Error, IoErrorKind, Protocol};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::WebPkiClientVerifier;
use rustls::{RootCertStore, ServerConfig};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio_rustls::server::TlsStream;
use tokio_rustls::TlsAcceptor;
use tokio_tungstenite::{
    accept_hdr_async,
    tungstenite::{http, Message},
    WebSocketStream,
};

use crate::config::TlsListener;
use crate::hook::Hook;
use crate::protocols::mqtt;
use crate::state::GlobalState;

use proxy::{parse_header, Addresses};

const CONNECT_TIMEOUT_SECS: u64 = 5;

pub async fn handle_accept<
    T: AsyncRead + AsyncWrite + Unpin,
    H: Hook + Clone + Send + Sync + 'static,
>(
    mut conn: T,
    conn_args: ConnectionArgs,
    mut peer: SocketAddr,
    hook_handler: H,
    global: Arc<GlobalState>,
) -> io::Result<()> {
    // If the client don't send enough data in 5 seconds, disconnect it.
    let (timeout_sender, timeout_receiver) = bounded(1);
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(CONNECT_TIMEOUT_SECS)).await;
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
                log::info!("timeout when parse proxy header: {}", peer);
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
        let tls_stream = acceptor
            .accept(conn)
            .map_err(|err| {
                log::debug!("accept tls connection error: {:?}", err);
                io::Error::from(io::ErrorKind::InvalidData)
            })
            .or(async {
                let _ = timeout_receiver.recv_async().await;
                log::info!("timeout when tls accept: {}", peer);
                Err(io::ErrorKind::TimedOut.into())
            })
            .await?;
        tls_sni = tls_stream.get_ref().1.server_name().map(ToOwned::to_owned);
        TlsWrapper::Tls(Box::new(tls_stream))
    } else {
        TlsWrapper::Raw(conn)
    };

    log::debug!("TLS host name(SNI): {:?}", tls_sni);

    // Handle WebSocket
    let mut ws_wrapper = if conn_args.websocket {
        let handler = |req: &http::Request<_>, mut resp: http::Response<_>| {
            if let Some(protocol) = req.headers().get("Sec-WebSocket-Protocol") {
                // see: [MQTT-6.0.0-3]
                if protocol != "mqtt" {
                    log::info!("invalid WebSocket subprotocol name: {:?}", protocol);
                    return Err(http::Response::new(Some(
                        "invalid WebSocket subprotocol name".to_string(),
                    )));
                }
                resp.headers_mut()
                    .insert("Sec-WebSocket-Protocol", protocol.clone());
            }
            Ok(resp)
        };
        let stream = match accept_hdr_async(tls_wrapper, handler).await {
            Ok(stream) => stream,
            Err(err) => {
                log::warn!("Accept websocket connection error: {:?}", err);
                return Err(io::ErrorKind::InvalidData.into());
            }
        };
        WebSocketWrapper::WebSocket {
            stream: Box::new(stream),
            read_data: Bytes::new(),
            read_data_idx: 0,
            pending_pong: None,
            closed: false,
        }
    } else {
        WebSocketWrapper::Raw(Box::new(tls_wrapper))
    };

    let (packet_type, remaining_len, total_len) = decode_raw_header_async(&mut ws_wrapper)
        .or(async {
            let _ = timeout_receiver.recv_async().await;
            log::info!("timeout when decode raw mqtt header: {}", peer);
            Err(Error::IoError(IoErrorKind::TimedOut))
        })
        .await?;
    if packet_type != 0b00010000 {
        log::debug!("first packet is not CONNECT packet: {}", packet_type);
        return Err(io::ErrorKind::InvalidData.into());
    }
    let protocol = Protocol::decode_async(&mut ws_wrapper)
        .or(async {
            let _ = timeout_receiver.recv_async().await;
            log::info!("timeout when decode mqtt protocol: {}", peer);
            Err(Error::IoError(IoErrorKind::TimedOut))
        })
        .await?;
    match protocol {
        Protocol::V310 | Protocol::V311 => {
            let header = v3::Header::new_with(packet_type, remaining_len, total_len as u32)
                .expect("v3 header");
            mqtt::v3::handle_connection(
                ws_wrapper,
                peer,
                header,
                protocol,
                timeout_receiver,
                hook_handler,
                global,
            )
            .await?;
        }
        Protocol::V500 => {
            let header = v5::Header::new_with(packet_type, remaining_len, total_len as u32)
                .expect("v5 header");
            mqtt::v5::handle_connection(
                ws_wrapper,
                peer,
                header,
                protocol,
                timeout_receiver,
                hook_handler,
                global,
            )
            .await?;
        }
    }
    Ok(())
}

fn build_tls_context(listener: &TlsListener) -> io::Result<TlsAcceptor> {
    if listener.verify_peer && listener.ca_file.is_none() {
        log::error!("When `verify_peer` is true `ca_file` must be presented!");
        return Err(io::Error::from(io::ErrorKind::InvalidInput));
    }

    // Load certificate chain
    let cert_reader = File::open(&listener.cert_file).map_err(|err| {
        log::error!("Cannot open certfile: {}", err);
        io::Error::from(io::ErrorKind::InvalidInput)
    })?;
    let cert_chain: Vec<CertificateDer<'static>> =
        CertificateDer::pem_reader_iter(&mut BufReader::new(cert_reader))
            .collect::<Result<_, _>>()
            .map_err(|err| {
                log::error!("Invalid certfile: {}", err);
                io::Error::from(io::ErrorKind::InvalidInput)
            })?;

    // Load private key
    let key_reader = File::open(&listener.key_file).map_err(|err| {
        log::error!("Cannot open keyfile: {}", err);
        io::Error::from(io::ErrorKind::InvalidInput)
    })?;
    let key = PrivateKeyDer::from_pem_reader(&mut BufReader::new(key_reader)).map_err(|err| {
        log::error!("Invalid keyfile: {}", err);
        io::Error::from(io::ErrorKind::InvalidInput)
    })?;

    let server_config = if listener.verify_peer {
        // Load CA certs for client verification
        let ca_file = listener.ca_file.as_ref().unwrap();
        let ca_reader = File::open(ca_file).map_err(|err| {
            log::error!("Cannot open CA-certfile: {}", err);
            io::Error::from(io::ErrorKind::InvalidInput)
        })?;
        let mut root_store = RootCertStore::empty();
        for cert in CertificateDer::pem_reader_iter(&mut BufReader::new(ca_reader)) {
            let cert = cert.map_err(|err| {
                log::error!("Invalid CA-certfile: {}", err);
                io::Error::from(io::ErrorKind::InvalidInput)
            })?;
            root_store.add(cert).map_err(|err| {
                log::error!("Failed to add CA certificate: {}", err);
                io::Error::from(io::ErrorKind::InvalidInput)
            })?;
        }
        let client_verifier = if listener.fail_if_no_peer_cert {
            WebPkiClientVerifier::builder(Arc::new(root_store))
                .build()
                .map_err(|err| {
                    log::error!("Failed to build client verifier: {}", err);
                    io::Error::from(io::ErrorKind::InvalidInput)
                })?
        } else {
            WebPkiClientVerifier::builder(Arc::new(root_store))
                .allow_unauthenticated()
                .build()
                .map_err(|err| {
                    log::error!("Failed to build client verifier: {}", err);
                    io::Error::from(io::ErrorKind::InvalidInput)
                })?
        };
        ServerConfig::builder()
            .with_client_cert_verifier(client_verifier)
            .with_single_cert(cert_chain, key)
            .map_err(|err| {
                log::error!("Failed to build ServerConfig: {}", err);
                io::Error::from(io::ErrorKind::InvalidInput)
            })?
    } else {
        ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(cert_chain, key)
            .map_err(|err| {
                log::error!("Failed to build ServerConfig: {}", err);
                io::Error::from(io::ErrorKind::InvalidInput)
            })?
    };

    Ok(TlsAcceptor::from(Arc::new(server_config)))
}

#[derive(Clone)]
pub struct ConnectionArgs {
    pub(crate) addr: SocketAddr,
    pub(crate) reuse_port: bool,
    pub(crate) proxy: bool,
    pub(crate) proxy_tls_termination: bool,
    pub(crate) websocket: bool,
    pub(crate) tls_acceptor: Option<TlsAcceptor>,
}

enum TlsWrapper<S> {
    Raw(S),
    Tls(Box<TlsStream<S>>),
}

impl<S: AsyncRead + AsyncWrite + Unpin> AsyncRead for TlsWrapper<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf,
    ) -> Poll<io::Result<()>> {
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
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            TlsWrapper::Raw(conn) => Pin::new(conn).poll_shutdown(cx),
            TlsWrapper::Tls(tls_stream) => Pin::new(tls_stream).poll_shutdown(cx),
        }
    }
}

enum WebSocketWrapper<S> {
    Raw(Box<TlsWrapper<S>>),
    WebSocket {
        stream: Box<WebSocketStream<TlsWrapper<S>>>,
        read_data: Bytes,
        read_data_idx: usize,
        pending_pong: Option<Bytes>,
        closed: bool,
    },
}

fn ws_send_pong<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut Box<WebSocketStream<TlsWrapper<S>>>,
    pong: &mut Option<Bytes>,
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
        buf: &mut ReadBuf,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            WebSocketWrapper::Raw(conn) => Pin::new(conn).poll_read(cx, buf),
            WebSocketWrapper::WebSocket {
                stream,
                read_data,
                read_data_idx,
                pending_pong,
                closed,
            } => {
                fn copy_data(buf: &mut ReadBuf, data: &[u8], data_idx: &mut usize) -> usize {
                    let amt = cmp::min(data.len() - *data_idx, buf.remaining());
                    buf.put_slice(&data[*data_idx..*data_idx + amt]);
                    *data_idx += amt;
                    amt
                }
                if !*closed {
                    ws_send_pong(stream, pending_pong, cx)?;
                }
                if *read_data_idx < read_data.len() {
                    copy_data(buf, read_data, read_data_idx);
                    return Poll::Ready(Ok(()));
                }
                loop {
                    match Pin::new(&mut *stream).poll_next(cx) {
                        Poll::Ready(Some(Ok(msg))) => match msg {
                            Message::Binary(bin) => {
                                if bin.is_empty() {
                                    continue;
                                }
                                *read_data = bin;
                                *read_data_idx = 0;
                                copy_data(buf, read_data, read_data_idx);
                                return Poll::Ready(Ok(()));
                            }
                            Message::Close(_) => return Poll::Ready(Ok(())),
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
                        Poll::Ready(None) => return Poll::Ready(Ok(())),
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
                let message = Message::Binary(buf.to_vec().into());
                if let Err(err) = Pin::new(&mut *stream).start_send(message) {
                    log::debug!("WebSocket write error: {:?}", err);
                    return Poll::Ready(Err(io::ErrorKind::BrokenPipe.into()));
                }
                let _ignore = Pin::new(&mut *stream)
                    .as_mut()
                    .poll_flush(cx)
                    .map_err::<io::Error, _>(|_| Into::into(io::ErrorKind::BrokenPipe))?;
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

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            WebSocketWrapper::Raw(conn) => Pin::new(conn).poll_shutdown(cx),
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
