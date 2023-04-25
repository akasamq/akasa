use std::cmp;
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use flume::{bounded, Sender};
use openssl::ssl::{SslAcceptor, SslFiletype, SslMethod, SslVerifyMode};
use tokio::{net::TcpListener, runtime::Runtime, task::JoinHandle};

use super::{handle_accept, ConnectionArgs, IoWrapper};
use crate::config::{Listener, ProxyMode, TlsListener};
use crate::hook::{Hook, HookRequest, HookService};
use crate::state::{Executor, GlobalState};

pub fn start<H>(hook_handler: H, global: Arc<GlobalState>) -> io::Result<()>
where
    H: Hook + Clone + Send + Sync + 'static,
{
    let rt = Runtime::new()?;
    let (hook_sender, hook_receiver) = bounded(64);

    let mqtts_tls_acceptor = global
        .config
        .listeners
        .mqtts
        .as_ref()
        .map(|listener| {
            log::info!("Building TLS context for mqtts...");
            build_tls_context(listener)
        })
        .transpose()?;
    let wss_tls_acceptor = global
        .config
        .listeners
        .wss
        .as_ref()
        .map(|listener| {
            log::info!("Building TLS context for wss...");
            build_tls_context(listener)
        })
        .transpose()?;

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
                tls_acceptor: mqtts_tls_acceptor.map(Into::into),
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
                tls_acceptor: wss_tls_acceptor.map(Into::into),
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
            let conn_wrapper = IoWrapper::new(conn);
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
