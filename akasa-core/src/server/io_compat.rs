use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures_lite::io::{AsyncRead, AsyncWrite};
use tokio::io::{self as tokio_io, ReadBuf};

pub struct IoWrapper<S>(S);

impl<S> IoWrapper<S> {
    pub fn new(inner: S) -> IoWrapper<S> {
        IoWrapper(inner)
    }
}

impl<S: tokio_io::AsyncRead + tokio_io::AsyncWrite + Unpin> AsyncRead for IoWrapper<S> {
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

impl<S: tokio_io::AsyncRead + tokio_io::AsyncWrite + Unpin> AsyncWrite for IoWrapper<S> {
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
