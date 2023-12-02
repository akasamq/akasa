use std::io;
use std::sync::Arc;
use std::time::Duration;

use tokio::{net::TcpSocket, runtime::Runtime};

use super::{build_tls_context, handle_accept, ConnectionArgs};
use crate::config::{Listener, ProxyMode, TlsListener};
use crate::hook::Hook;
use crate::state::GlobalState;

pub fn start<H>(hook_handler: H, global: Arc<GlobalState>) -> io::Result<()>
where
    H: Hook + Clone + Send + Sync + 'static,
{
    let rt = Runtime::new()?;

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

    cfg_if::cfg_if! {
        if #[cfg(all(unix, not(target_os = "solaris"), not(target_os = "illumos")))] {
            let reuse_port_available = true;
        } else {
            let reuse_port_available = false;
        }
    }

    rt.block_on(async move {
        let listeners = &global.config.listeners;
        let tasks: Vec<_> = [
            listeners.mqtt.as_ref().map(
                |Listener {
                     addr,
                     reuse_port,
                     proxy_mode,
                 }| ConnectionArgs {
                    addr: *addr,
                    reuse_port: *reuse_port,
                    proxy: proxy_mode.is_some(),
                    proxy_tls_termination: *proxy_mode == Some(ProxyMode::TlsTermination),
                    websocket: false,
                    tls_acceptor: None,
                },
            ),
            listeners.mqtts.as_ref().map(
                |TlsListener {
                     addr,
                     reuse_port,
                     proxy,
                     ..
                 }| ConnectionArgs {
                    addr: *addr,
                    reuse_port: *reuse_port,
                    proxy: *proxy,
                    proxy_tls_termination: false,
                    websocket: false,
                    tls_acceptor: mqtts_tls_acceptor.map(Into::into),
                },
            ),
            listeners.ws.as_ref().map(
                |Listener {
                     addr,
                     reuse_port,
                     proxy_mode,
                 }| ConnectionArgs {
                    addr: *addr,
                    reuse_port: *reuse_port,
                    proxy: proxy_mode.is_some(),
                    proxy_tls_termination: *proxy_mode == Some(ProxyMode::TlsTermination),
                    websocket: true,
                    tls_acceptor: None,
                },
            ),
            listeners.wss.as_ref().map(
                |TlsListener {
                     addr,
                     reuse_port,
                     proxy,
                     ..
                 }| ConnectionArgs {
                    addr: *addr,
                    reuse_port: *reuse_port,
                    proxy: *proxy,
                    proxy_tls_termination: false,
                    websocket: true,
                    tls_acceptor: wss_tls_acceptor.map(Into::into),
                },
            ),
        ]
        .into_iter()
        .flatten()
        .flat_map(|conn_args| {
            let reuse_port = reuse_port_available && conn_args.reuse_port;
            let global = Arc::clone(&global);
            let hook_handler = hook_handler.clone();
            let conn_args = conn_args.clone();
            let n = if reuse_port { 4 } else { 1 };
            (0..n).map(move |_| {
                let global = Arc::clone(&global);
                let hook_handler = hook_handler.clone();
                let conn_args = conn_args.clone();
                tokio::spawn(async move {
                    loop {
                        let global = Arc::clone(&global);
                        let hook_handler = hook_handler.clone();
                        if let Err(err) =
                            listen(conn_args.clone(), reuse_port, hook_handler, global).await
                        {
                            log::error!("Listen error: {:?}", err);
                            tokio::time::sleep(Duration::from_secs(1)).await;
                        }
                    }
                })
            })
        })
        .collect();

        if tasks.is_empty() {
            log::error!("No binding address in config");
        }
        for task in tasks {
            let _ = task.await;
        }
    });
    Ok(())
}

async fn listen<H: Hook + Clone + Send + Sync + 'static>(
    conn_args: ConnectionArgs,
    reuse_port: bool,
    hook_handler: H,
    global: Arc<GlobalState>,
) -> io::Result<()> {
    let addr = conn_args.addr;
    let socket = if addr.is_ipv4() {
        TcpSocket::new_v4()?
    } else {
        TcpSocket::new_v6()?
    };
    socket.set_reuseaddr(true)?;
    if reuse_port {
        socket.set_reuseport(true)?;
    }
    socket.bind(addr)?;
    let listener = socket.listen(1024)?;

    let listen_type = match (conn_args.websocket, conn_args.tls_acceptor.is_some()) {
        (false, false) => "mqtt",
        (false, true) => "mqtts",
        (true, false) => "ws",
        (true, true) => "wss",
    };
    let labels = [(conn_args.proxy, "proxy"), (reuse_port, "reuseport")]
        .into_iter()
        .filter(|(flag, _)| *flag)
        .map(|(_, text)| text)
        .collect::<Vec<_>>();
    log::info!(
        "Listen {listen_type}@{addr} ({}) success!",
        labels.join(",")
    );

    loop {
        let (conn, peer) = listener.accept().await?;
        log::debug!("{} connected", peer,);
        let conn_args = conn_args.clone();
        let hook_handler = hook_handler.clone();
        let global = Arc::clone(&global);
        tokio::spawn(async move {
            let _ = handle_accept(conn, conn_args, peer, hook_handler, Arc::clone(&global)).await;
        });
    }
}
