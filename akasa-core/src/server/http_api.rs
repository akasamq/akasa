use std::sync::Arc;

use tokio::net::TcpSocket;

use crate::{GlobalState, protocols::http::get_router};

pub async fn serve(cfg: crate::config::Http, global: Arc<GlobalState>) -> std::io::Result<()> {
    let addr = cfg.addr;
    let mut labels = Vec::new();
    let router = get_router(&cfg, global.clone());

    let socket = if addr.is_ipv4() {
        TcpSocket::new_v4()?
    } else {
        TcpSocket::new_v6()?
    };
    socket.set_reuseaddr(true)?;
    #[cfg(all(
        unix,
        not(target_os = "solaris"),
        not(target_os = "illumos"),
        not(target_os = "cygwin"),
    ))]
    if cfg.reuse_port {
        labels.push("reuseport");
        socket.set_reuseport(true)?;
    }

    if cfg.prometheus {
        labels.push("prometheus");
    }

    socket.bind(addr)?;
    let listener = socket.listen(128)?;

    log::info!("Listen http@{} ({}) success!", addr, labels.join(","),);
    axum::serve(listener, router).await?;

    Ok(())
}
