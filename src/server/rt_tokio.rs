use std::cmp;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use flume::bounded;
use futures_lite::io::{AsyncRead, AsyncWrite};
use tokio::{
    io::{self as tokio_io, ReadBuf},
    net::{TcpListener, TcpStream},
    runtime::Runtime,
};

use super::handle_accept;
use crate::hook::{DefaultHook, HookService};
use crate::state::{Executor, GlobalState};

pub fn start(global: Arc<GlobalState>) -> io::Result<()> {
    let rt = Runtime::new()?;
    let (hook_sender, hook_receiver) = bounded(64);
    rt.block_on(async move {
        let executor = Arc::new(TokioExecutor {});
        let listener = TcpListener::bind(global.bind).await.unwrap();
        log::info!("Listen success!");

        let hook_service_tasks = cmp::max(num_cpus::get() / 2, 1);
        for _ in 0..hook_service_tasks {
            let hook_handler = DefaultHook::new(Arc::clone(&global));
            let hook_service = HookService::new(
                Arc::clone(&executor),
                hook_handler,
                hook_receiver.clone(),
                Arc::clone(&global),
            );
            tokio::spawn(hook_service.start());
        }

        loop {
            let (conn, peer) = listener.accept().await.unwrap();
            log::debug!("{} connected", peer,);
            let conn_wrapper = ConnWrapper(conn);
            let hook_requests = hook_sender.clone();
            let executor = Arc::clone(&executor);
            let global = Arc::clone(&global);
            tokio::spawn(async move {
                let _ = handle_accept(
                    conn_wrapper,
                    peer,
                    hook_requests,
                    executor,
                    Arc::clone(&global),
                )
                .await;
            });
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
