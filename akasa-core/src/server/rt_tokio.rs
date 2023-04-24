use std::cmp;
use std::fs;
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use flume::{bounded, Sender};
use futures_lite::io::{AsyncRead, AsyncWrite};
use tokio::{
    io::{self as tokio_io, ReadBuf},
    net::{TcpListener, TcpStream},
    runtime::Runtime,
    task::JoinHandle,
};

use super::{handle_accept, ConnectionArgs};
use crate::config::{Listener, ProxyMode, TlsListener};
use crate::hook::{Hook, HookRequest, HookService};
use crate::state::{Executor, GlobalState};

pub fn start<H>(hook_handler: H, global: Arc<GlobalState>) -> io::Result<()>
where
    H: Hook + Clone + Send + Sync + 'static,
{
    let rt = Runtime::new()?;
    let (hook_sender, hook_receiver) = bounded(64);

    let mqtts_tls_acceptor = if let Some(TlsListener {
        identity,
        identity_password,
        ..
    }) = &global.config.listeners.mqtts
    {
        let identity_content = fs::read(identity)?;
        let identity =
            native_tls::Identity::from_pkcs12(&identity_content, identity_password.as_str())
                .map_err(|err| {
                    log::error!("Parse mqtts TLS identity file failed: {:?}", err);
                    io::Error::from(io::ErrorKind::InvalidData)
                })?;
        let tls_acceptor = native_tls::TlsAcceptor::new(identity).map_err(|err| {
            log::error!("Create mqtts TLS acceptor failed: {:?}", err);
            io::Error::from(io::ErrorKind::InvalidData)
        })?;
        Some(tls_acceptor)
    } else {
        None
    };

    rt.block_on(async move {
        let executor = Arc::new(TokioExecutor {});

        let hook_service_tasks = cmp::max(num_cpus::get() / 2, 1);
        for _ in 0..hook_service_tasks {
            let hook_service = HookService::new(
                Arc::clone(&executor),
                hook_handler.clone(),
                hook_receiver.clone(),
                Arc::clone(&global),
            );
            tokio::spawn(hook_service.start());
        }

        let listeners = &global.config.listeners;
        let mut last_task = None;
        if let Some(Listener { addr, proxy_mode }) = listeners.mqtt {
            let global = Arc::clone(&global);
            let hook_sender = hook_sender.clone();
            let executor = Arc::clone(&executor);
            let conn_args = ConnectionArgs {
                proxy: proxy_mode.is_some(),
                proxy_tls_termination: proxy_mode == Some(ProxyMode::TlsTermination),
                websocket: false,
                tls_acceptor: None,
            };
            let task = listen(addr, conn_args, hook_sender, executor, global);
            last_task = Some(task);
        }
        if let Some(TlsListener { addr, proxy, .. }) = listeners.mqtts {
            let global = Arc::clone(&global);
            let hook_sender = hook_sender.clone();
            let executor = Arc::clone(&executor);
            let conn_args = ConnectionArgs {
                proxy,
                proxy_tls_termination: false,
                websocket: false,
                tls_acceptor: mqtts_tls_acceptor.clone().map(Into::into),
            };
            let task = listen(addr, conn_args, hook_sender, executor, global);
            last_task = Some(task);
        }
        if let Some(Listener { addr, proxy_mode }) = listeners.ws {
            let global = Arc::clone(&global);
            let hook_sender = hook_sender.clone();
            let executor = Arc::clone(&executor);
            let conn_args = ConnectionArgs {
                proxy: proxy_mode.is_some(),
                proxy_tls_termination: proxy_mode == Some(ProxyMode::TlsTermination),
                websocket: true,
                tls_acceptor: None,
            };
            let task = listen(addr, conn_args, hook_sender, executor, global);
            last_task = Some(task);
        }
        if let Some(TlsListener { addr, proxy, .. }) = listeners.wss {
            let global = Arc::clone(&global);
            let hook_sender = hook_sender.clone();
            let executor = Arc::clone(&executor);
            let conn_args = ConnectionArgs {
                proxy,
                proxy_tls_termination: false,
                websocket: true,
                tls_acceptor: mqtts_tls_acceptor.map(Into::into),
            };
            let task = listen(addr, conn_args, hook_sender, executor, global);
            last_task = Some(task);
        }

        if let Some(task) = last_task {
            let _ = task.await;
        } else {
            log::error!("No binding address in config");
        }
    });
    Ok(())
}

fn listen<E: Executor + Send + Sync + 'static>(
    addr: SocketAddr,
    conn_args: ConnectionArgs,
    hook_sender: Sender<HookRequest>,
    executor: Arc<E>,
    global: Arc<GlobalState>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let listener = TcpListener::bind(addr).await.unwrap();
        let listen_type = match (conn_args.websocket, conn_args.tls_acceptor.is_some()) {
            (false, false) => "mqtt",
            (false, true) => "mqtts",
            (true, false) => "ws",
            (true, true) => "wss",
        };
        let listen_type = if conn_args.proxy {
            format!("{listen_type}(proxy)")
        } else {
            listen_type.to_owned()
        };
        log::info!("Listen {listen_type}@{addr} success!");
        loop {
            let (conn, peer) = listener.accept().await.unwrap();
            log::debug!("{} connected", peer,);
            let conn_wrapper = ConnWrapper(conn);
            let conn_args = conn_args.clone();
            let hook_requests = hook_sender.clone();
            let executor = Arc::clone(&executor);
            let global = Arc::clone(&global);
            tokio::spawn(async move {
                let _ = handle_accept(
                    conn_wrapper,
                    conn_args,
                    peer,
                    hook_requests,
                    executor,
                    Arc::clone(&global),
                )
                .await;
            });
        }
    })
}

struct ConnWrapper(TcpStream);

impl AsyncRead for ConnWrapper {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        let mut read_buf = ReadBuf::new(buf);
        tokio_io::AsyncRead::poll_read(Pin::new(&mut self.0), cx, &mut read_buf)
            .map_ok(|()| read_buf.capacity() - read_buf.remaining())
    }
}

impl AsyncWrite for ConnWrapper {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        tokio_io::AsyncWrite::poll_write(Pin::new(&mut self.0), cx, buf)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        tokio_io::AsyncWrite::poll_flush(Pin::new(&mut self.0), cx)
    }
    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        tokio_io::AsyncWrite::poll_shutdown(Pin::new(&mut self.0), cx)
    }
}

pub struct TokioExecutor {}

impl Executor for TokioExecutor {
    fn spawn_local<F>(&self, future: F)
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        tokio::spawn(future);
    }

    fn spawn_sleep<F>(&self, duration: Duration, task: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        tokio::spawn(async move {
            tokio::time::sleep(duration).await;
            task.await;
        });
    }

    fn spawn_interval<G, F>(&self, action_gen: G) -> io::Result<()>
    where
        G: (Fn() -> F) + Send + Sync + 'static,
        F: Future<Output = Option<Duration>> + Send + 'static,
    {
        tokio::spawn(async move {
            while let Some(duration) = action_gen().await {
                tokio::time::sleep(duration).await;
            }
        });
        Ok(())
    }
}
