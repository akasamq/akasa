use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use futures_lite::io::{AsyncRead, AsyncWrite};
use tokio::{
    io::{self as tokio_io, ReadBuf},
    net::{TcpListener, TcpStream},
    runtime::Runtime,
};

use super::handle_accept;
use crate::state::{Executor, GlobalState};

pub fn start(global: Arc<GlobalState>) -> io::Result<()> {
    let rt = Runtime::new()?;
    rt.block_on(async move {
        let executor = Arc::new(TokioExecutor {});
        let listener = TcpListener::bind(global.bind).await.unwrap();
        log::info!("Listen success!");
        loop {
            let (conn, peer) = listener.accept().await.unwrap();
            log::info!(
                "{} connected, total {} clients ({} online) ",
                peer,
                global.clients_count(),
                global.online_clients_count(),
            );
            let conn_wrapper = ConnWrapper(conn);
            let executor = Arc::clone(&executor);
            let global = Arc::clone(&global);
            tokio::spawn(handle_accept(conn_wrapper, peer, executor, global));
        }
    });
    Ok(())
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

struct TokioExecutor {}

impl Executor for TokioExecutor {
    fn spawn_local<F>(&self, future: F)
    where
        F: Future + 'static,
        F::Output: 'static,
    {
        tokio::task::spawn_local(future);
    }

    fn spawn_timer<G, F>(&self, action_gen: G) -> io::Result<()>
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
