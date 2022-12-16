use std::cmp;
use std::io::{self, Cursor, IoSlice};
use std::mem;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures_lite::{
    io::{AsyncRead, AsyncWrite},
    FutureExt,
};
use mqtt::{packet::VariablePacket, Decodable, Encodable};
use tokio::{
    sync::mpsc::{channel, Receiver, Sender},
    task::JoinHandle,
};

use crate::config::Config;
use crate::server::{handle_accept, rt_tokio::TokioExecutor};
use crate::state::GlobalState;

pub struct MockConnControl {
    chan_in: Sender<Vec<u8>>,
    chan_out: Receiver<Vec<u8>>,
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
    pub fn new(port: u16, config: Config) -> (MockConn, MockConnControl) {
        let (in_tx, in_rx) = channel(32);
        let (out_tx, out_rx) = channel(32);
        let conn = MockConn {
            bind: "127.0.0.1:1883".parse().unwrap(),
            peer: format!("127.0.0.1:{}", port).parse().unwrap(),
            data_in: Vec::new(),
            chan_in: in_rx,
            chan_out: out_tx,
        };

        let global = Arc::new(GlobalState::new(conn.bind, config));
        let control = MockConnControl {
            chan_in: in_tx,
            chan_out: out_rx,
            global,
        };
        (conn, control)
    }
}

impl MockConnControl {
    pub fn start(&self, conn: MockConn) -> JoinHandle<io::Result<()>> {
        let peer = conn.peer;
        let executor = TokioExecutor {};
        let global = Arc::clone(&self.global);
        tokio::spawn(handle_accept(conn, peer, executor, global))
    }

    pub fn try_read_packet(&mut self) -> Result<VariablePacket, String> {
        self.chan_out
            .try_recv()
            .map(|data| {
                let mut data = Cursor::new(data);
                VariablePacket::decode(&mut data).unwrap()
            })
            .map_err(|err| err.to_string())
    }

    pub async fn read_packet(&mut self) -> VariablePacket {
        let mut data = Cursor::new(self.chan_out.recv().await.unwrap());
        VariablePacket::decode(&mut data).unwrap()
    }

    pub async fn write_packet(&self, packet: VariablePacket) {
        let mut buf = Vec::new();
        packet.encode(&mut buf).unwrap();
        self.write_data(buf).await;
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
