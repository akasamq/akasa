//! NOTE: The code is copied from tokio-rs/tokio-openssl v0.6.3
//!
//! Async TLS streams backed by OpenSSL.
//!
//! This crate provides a wrapper around the [`openssl`] crate's [`SslStream`](ssl::SslStream) type
//! that works with with [`tokio`]'s [`AsyncRead`] and [`AsyncWrite`] traits rather than std's
//! blocking [`Read`] and [`Write`] traits.
#![warn(missing_docs)]

use futures_lite::io::{AsyncRead, AsyncWrite};
use futures_util::future;
use openssl::error::ErrorStack;
use openssl::ssl::{self, ErrorCode, ShutdownResult, Ssl, SslRef};
use std::fmt;
use std::io::{self, Read, Write};
use std::pin::Pin;
use std::task::{Context, Poll};

struct StreamWrapper<S> {
    stream: S,
    context: usize,
}

impl<S> fmt::Debug for StreamWrapper<S>
where
    S: fmt::Debug,
{
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.stream, fmt)
    }
}

impl<S> StreamWrapper<S> {
    /// # Safety
    ///
    /// Must be called with `context` set to a valid pointer to a live `Context` object, and the
    /// wrapper must be pinned in memory.
    unsafe fn parts(&mut self) -> (Pin<&mut S>, &mut Context<'_>) {
        debug_assert_ne!(self.context, 0);
        let stream = Pin::new_unchecked(&mut self.stream);
        let context = &mut *(self.context as *mut _);
        (stream, context)
    }
}

impl<S> Read for StreamWrapper<S>
where
    S: AsyncRead,
{
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let (stream, cx) = unsafe { self.parts() };
        match stream.poll_read(cx, buf)? {
            Poll::Ready(nread) => Ok(nread),
            Poll::Pending => Err(io::Error::from(io::ErrorKind::WouldBlock)),
        }
    }
}

impl<S> Write for StreamWrapper<S>
where
    S: AsyncWrite,
{
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let (stream, cx) = unsafe { self.parts() };
        match stream.poll_write(cx, buf) {
            Poll::Ready(r) => r,
            Poll::Pending => Err(io::Error::from(io::ErrorKind::WouldBlock)),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        let (stream, cx) = unsafe { self.parts() };
        match stream.poll_flush(cx) {
            Poll::Ready(r) => r,
            Poll::Pending => Err(io::Error::from(io::ErrorKind::WouldBlock)),
        }
    }
}

fn cvt<T>(r: io::Result<T>) -> Poll<io::Result<T>> {
    match r {
        Ok(v) => Poll::Ready(Ok(v)),
        Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => Poll::Pending,
        Err(e) => Poll::Ready(Err(e)),
    }
}

fn cvt_ossl<T>(r: Result<T, ssl::Error>) -> Poll<Result<T, ssl::Error>> {
    match r {
        Ok(v) => Poll::Ready(Ok(v)),
        Err(e) => match e.code() {
            ErrorCode::WANT_READ | ErrorCode::WANT_WRITE => Poll::Pending,
            _ => Poll::Ready(Err(e)),
        },
    }
}

/// An asynchronous version of [`openssl::ssl::SslStream`].
#[derive(Debug)]
pub struct SslStream<S>(ssl::SslStream<StreamWrapper<S>>);

impl<S> SslStream<S>
where
    S: AsyncRead + AsyncWrite,
{
    /// Like [`SslStream::new`](ssl::SslStream::new).
    pub fn new(ssl: Ssl, stream: S) -> Result<Self, ErrorStack> {
        ssl::SslStream::new(ssl, StreamWrapper { stream, context: 0 }).map(SslStream)
    }

    /// Like [`SslStream::connect`](ssl::SslStream::connect).
    pub fn poll_connect(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), ssl::Error>> {
        self.with_context(cx, |s| cvt_ossl(s.connect()))
    }

    /// A convenience method wrapping [`poll_connect`](Self::poll_connect).
    pub async fn connect(mut self: Pin<&mut Self>) -> Result<(), ssl::Error> {
        future::poll_fn(|cx| self.as_mut().poll_connect(cx)).await
    }

    /// Like [`SslStream::accept`](ssl::SslStream::accept).
    pub fn poll_accept(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), ssl::Error>> {
        self.with_context(cx, |s| cvt_ossl(s.accept()))
    }

    /// A convenience method wrapping [`poll_accept`](Self::poll_accept).
    pub async fn accept(mut self: Pin<&mut Self>) -> Result<(), ssl::Error> {
        future::poll_fn(|cx| self.as_mut().poll_accept(cx)).await
    }

    /// Like [`SslStream::do_handshake`](ssl::SslStream::do_handshake).
    pub fn poll_do_handshake(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), ssl::Error>> {
        self.with_context(cx, |s| cvt_ossl(s.do_handshake()))
    }

    /// A convenience method wrapping [`poll_do_handshake`](Self::poll_do_handshake).
    pub async fn do_handshake(mut self: Pin<&mut Self>) -> Result<(), ssl::Error> {
        future::poll_fn(|cx| self.as_mut().poll_do_handshake(cx)).await
    }

    /// Like [`SslStream::read_early_data`](ssl::SslStream::read_early_data).
    #[cfg(ossl111)]
    pub fn poll_read_early_data(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<Result<usize, ssl::Error>> {
        self.with_context(cx, |s| cvt_ossl(s.read_early_data(buf)))
    }

    /// A convenience method wrapping [`poll_read_early_data`](Self::poll_read_early_data).
    #[cfg(ossl111)]
    pub async fn read_early_data(
        mut self: Pin<&mut Self>,
        buf: &mut [u8],
    ) -> Result<usize, ssl::Error> {
        future::poll_fn(|cx| self.as_mut().poll_read_early_data(cx, buf)).await
    }

    /// Like [`SslStream::write_early_data`](ssl::SslStream::write_early_data).
    #[cfg(ossl111)]
    pub fn poll_write_early_data(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, ssl::Error>> {
        self.with_context(cx, |s| cvt_ossl(s.write_early_data(buf)))
    }

    /// A convenience method wrapping [`poll_write_early_data`](Self::poll_write_early_data).
    #[cfg(ossl111)]
    pub async fn write_early_data(
        mut self: Pin<&mut Self>,
        buf: &[u8],
    ) -> Result<usize, ssl::Error> {
        future::poll_fn(|cx| self.as_mut().poll_write_early_data(cx, buf)).await
    }
}

impl<S> SslStream<S> {
    /// Returns a shared reference to the `Ssl` object associated with this stream.
    pub fn ssl(&self) -> &SslRef {
        self.0.ssl()
    }

    /// Returns a shared reference to the underlying stream.
    pub fn get_ref(&self) -> &S {
        &self.0.get_ref().stream
    }

    /// Returns a mutable reference to the underlying stream.
    pub fn get_mut(&mut self) -> &mut S {
        &mut self.0.get_mut().stream
    }

    /// Returns a pinned mutable reference to the underlying stream.
    pub fn get_pin_mut(self: Pin<&mut Self>) -> Pin<&mut S> {
        unsafe { Pin::new_unchecked(&mut self.get_unchecked_mut().0.get_mut().stream) }
    }

    fn with_context<F, R>(self: Pin<&mut Self>, ctx: &mut Context<'_>, f: F) -> R
    where
        F: FnOnce(&mut ssl::SslStream<StreamWrapper<S>>) -> R,
    {
        let this = unsafe { self.get_unchecked_mut() };
        this.0.get_mut().context = ctx as *mut _ as usize;
        let r = f(&mut this.0);
        this.0.get_mut().context = 0;
        r
    }
}

impl<S> AsyncRead for SslStream<S>
where
    S: AsyncRead + AsyncWrite,
{
    fn poll_read(
        self: Pin<&mut Self>,
        ctx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        self.with_context(ctx, |s| {
            // This isn't really "proper", but rust-openssl doesn't currently expose a suitable interface even though
            // OpenSSL itself doesn't require the buffer to be initialized. So this is good enough for now.
            match cvt(s.read(buf))? {
                Poll::Ready(nread) => Poll::Ready(Ok(nread)),
                Poll::Pending => Poll::Pending,
            }
        })
    }
}

impl<S> AsyncWrite for SslStream<S>
where
    S: AsyncRead + AsyncWrite,
{
    fn poll_write(self: Pin<&mut Self>, ctx: &mut Context, buf: &[u8]) -> Poll<io::Result<usize>> {
        self.with_context(ctx, |s| cvt(s.write(buf)))
    }

    fn poll_flush(self: Pin<&mut Self>, ctx: &mut Context) -> Poll<io::Result<()>> {
        self.with_context(ctx, |s| cvt(s.flush()))
    }

    fn poll_close(mut self: Pin<&mut Self>, ctx: &mut Context) -> Poll<io::Result<()>> {
        match self.as_mut().with_context(ctx, |s| s.shutdown()) {
            Ok(ShutdownResult::Sent) | Ok(ShutdownResult::Received) => {}
            Err(ref e) if e.code() == ErrorCode::ZERO_RETURN => {}
            Err(ref e) if e.code() == ErrorCode::WANT_READ || e.code() == ErrorCode::WANT_WRITE => {
                return Poll::Pending;
            }
            Err(e) => {
                return Poll::Ready(Err(e
                    .into_io_error()
                    .unwrap_or_else(|e| io::Error::new(io::ErrorKind::Other, e))));
            }
        }

        self.get_pin_mut().poll_close(ctx)
    }
}

#[cfg(test)]
mod test {
    use super::super::IoWrapper;
    use super::SslStream;
    use futures_lite::io::{AsyncReadExt, AsyncWrite, AsyncWriteExt};
    use futures_util::future;
    use openssl::ssl::{Ssl, SslAcceptor, SslConnector, SslFiletype, SslMethod};
    use std::pin::Pin;
    use tokio::net::{TcpListener, TcpStream};

    #[tokio::test]
    async fn server() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = async move {
            let mut acceptor = SslAcceptor::mozilla_intermediate(SslMethod::tls()).unwrap();
            acceptor
                .set_private_key_file("src/server/tls-test-keys/key.pem", SslFiletype::PEM)
                .unwrap();
            acceptor
                .set_certificate_chain_file("src/server/tls-test-keys/cert.pem")
                .unwrap();
            let acceptor = acceptor.build();

            let ssl = Ssl::new(acceptor.context()).unwrap();
            let stream = listener.accept().await.unwrap().0;
            let mut stream = SslStream::new(ssl, IoWrapper::new(stream)).unwrap();

            Pin::new(&mut stream).accept().await.unwrap();

            let mut buf = [0; 4];
            stream.read_exact(&mut buf).await.unwrap();
            assert_eq!(&buf, b"asdf");

            stream.write_all(b"jkl;").await.unwrap();

            future::poll_fn(|ctx| Pin::new(&mut stream).poll_close(ctx))
                .await
                .unwrap()
        };

        let client = async {
            let mut connector = SslConnector::builder(SslMethod::tls()).unwrap();
            connector
                .set_ca_file("src/server/tls-test-keys/cert.pem")
                .unwrap();
            let ssl = connector
                .build()
                .configure()
                .unwrap()
                .into_ssl("localhost")
                .unwrap();

            let stream = TcpStream::connect(&addr).await.unwrap();
            let mut stream = SslStream::new(ssl, IoWrapper::new(stream)).unwrap();

            Pin::new(&mut stream).connect().await.unwrap();

            stream.write_all(b"asdf").await.unwrap();

            let mut buf = vec![];
            stream.read_to_end(&mut buf).await.unwrap();
            assert_eq!(buf, b"jkl;");
        };

        future::join(server, client).await;
    }
}
