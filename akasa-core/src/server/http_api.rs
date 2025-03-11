use std::sync::Arc;

use tokio::net::TcpSocket;

use crate::{protocols::http::get_router, GlobalState};

pub async fn serve(
    cfg: crate::config::Http,
    reuse_port: bool,
    global: Arc<GlobalState>,
) -> std::io::Result<()> {
    let addr = cfg.addr;
    let mut labels = Vec::new();
    let router = get_router(&cfg, global.clone());

    let socket = if addr.is_ipv4() {
        TcpSocket::new_v4()?
    } else {
        TcpSocket::new_v6()?
    };
    socket.set_reuseaddr(true)?;
    if reuse_port {
        labels.push("reuseport");
        socket.set_reuseport(true)?;
    }

    if cfg.prometheus {
        labels.push("prometheus");
    }

    #[cfg(feature = "swagger-ui")]
    if cfg.swagger_ui {
        labels.push("swagger-ui");
    }

    socket.bind(addr)?;
    let listener = socket.listen(128)?;

    log::info!("Listen http@{} ({}) success!", addr, labels.join(","),);
    axum::serve(listener, router).await?;

    Ok(())
}
