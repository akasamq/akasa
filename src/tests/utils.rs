use std::cmp;
use std::io::{self, IoSlice};
use std::mem;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures_lite::{
    io::{AsyncRead, AsyncWrite},
    FutureExt,
};
use tokio::{
    sync::mpsc::{channel, error::TryRecvError, Receiver, Sender},
    task::JoinHandle,
};

use crate::config::Config;
use crate::server::{handle_accept, rt_tokio::TokioExecutor};
use crate::state::GlobalState;

pub struct MockConnControl {
    pub chan_in: Sender<Vec<u8>>,
    pub chan_out: Receiver<Vec<u8>>,
    pub global: Arc<GlobalState>,
}

pub struct MockConn {
    pub bind: SocketAddr,
    pub peer: SocketAddr,
    data_in: Vec<u8>,
    chan_in: Receiver<Vec<u8>>,
    chan_out: Sender<Vec<u8>>,
}

impl MockConn {
    pub fn new_with_global(port: u16, global: Arc<GlobalState>) -> (MockConn, MockConnControl) {
        // FIXME: if the channel size is small, some tests will fail
        //   (cased by wrong implementation of poll_write)
        let (in_tx, in_rx) = channel(512);
        let (out_tx, out_rx) = channel(512);
        let conn = MockConn {
            bind: global.bind,
            peer: format!("127.0.0.1:{}", port).parse().unwrap(),
            data_in: Vec::new(),
            chan_in: in_rx,
            chan_out: out_tx,
        };
        let control = MockConnControl {
            chan_in: in_tx,
            chan_out: out_rx,
            global,
        };
        (conn, control)
    }

    pub fn new(port: u16, config: Config) -> (MockConn, MockConnControl) {
        let bind = "127.0.0.1:1883".parse().unwrap();
        let global = Arc::new(GlobalState::new(bind, config));
        Self::new_with_global(port, global)
    }

    pub fn start_with_global(
        port: u16,
        global: Arc<GlobalState>,
    ) -> (JoinHandle<io::Result<()>>, MockConnControl) {
        let (conn, control) = Self::new_with_global(port, global);
        let task = control.start(conn);
        (task, control)
    }

    pub fn start(port: u16, config: Config) -> (JoinHandle<io::Result<()>>, MockConnControl) {
        let (conn, control) = Self::new(port, config);
        let task = control.start(conn);
        (task, control)
    }
}

impl MockConnControl {
    pub fn start(&self, conn: MockConn) -> JoinHandle<io::Result<()>> {
        let peer = conn.peer;
        let executor = TokioExecutor {};
        let global = Arc::clone(&self.global);
        tokio::spawn(handle_accept(conn, peer, executor, global))
    }

    pub fn try_read_packet_is_empty(&mut self) -> bool {
        self.chan_out.try_recv() == Err(TryRecvError::Empty)
    }

    pub async fn write_data(&self, data: Vec<u8>) {
        self.chan_in.send(data).await.unwrap();
    }
}

impl AsyncRead for MockConn {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        if self.data_in.is_empty() {
            self.data_in = match self.chan_in.poll_recv(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Some(v)) => v,
                Poll::Ready(None) => {
                    return Poll::Ready(Err(io::Error::from(io::ErrorKind::BrokenPipe)))
                }
            };
        }
        if self.data_in.is_empty() {
            return Poll::Ready(Ok(0));
        }
        let amt = cmp::min(buf.len(), self.data_in.len());
        let mut other = self.data_in.split_off(amt);
        mem::swap(&mut other, &mut self.data_in);
        buf[..amt].copy_from_slice(&other);
        Poll::Ready(Ok(amt))
    }
}

impl AsyncWrite for MockConn {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let fut = self.chan_out.send(buf.to_vec());
        Box::pin(fut)
            .poll(cx)
            .map_ok(|_| buf.len())
            .map_err(|_| io::Error::from(io::ErrorKind::BrokenPipe))
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let mut nwritten = 0;
        for buf in bufs {
            nwritten += match Pin::new(&mut *self).poll_write(cx, buf) {
                Poll::Pending => {
                    if nwritten > 0 {
                        return Poll::Ready(Ok(nwritten));
                    } else {
                        return Poll::Pending;
                    }
                }
                Poll::Ready(Ok(len)) => len,
                Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
            }
        }
        Poll::Ready(Ok(nwritten))
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.poll_flush(cx)
    }
}
