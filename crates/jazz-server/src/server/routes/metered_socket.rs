//! A server WebSocket that reports raw socket progress below its framing.
//!
//! A WebSocket message is only yielded once all of its bytes are in, so a
//! large message on a slow uplink can take longer than the session's idle
//! timeout while bytes keep arriving the whole time. [`MeteredWebSocket`]
//! wraps the upgraded IO and records every read that fills bytes in a
//! [`PeerActivity`], which the session loop reads to tell a slow peer from a
//! silent one.

use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll, ready};
use std::time::Duration;

use axum::extract::ws::{CloseFrame, Message, Utf8Bytes};
use futures::{Sink, Stream};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio_tungstenite::WebSocketStream;
use tungstenite::protocol::{Role, WebSocketConfig};

/// The last moment bytes arrived from the peer on the raw socket.
pub(crate) struct PeerActivity {
    origin: tokio::time::Instant,
    /// Nanoseconds after `origin`.
    last: AtomicU64,
}

impl PeerActivity {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            origin: tokio::time::Instant::now(),
            last: AtomicU64::new(0),
        })
    }

    fn touch(&self) {
        let nanos = u64::try_from(self.origin.elapsed().as_nanos()).unwrap_or(u64::MAX);
        self.last.fetch_max(nanos, Ordering::Relaxed);
    }

    /// When the peer last sent any bytes, whole message or not.
    pub(crate) fn last_heard(&self) -> tokio::time::Instant {
        self.origin + Duration::from_nanos(self.last.load(Ordering::Relaxed))
    }
}

/// Upgraded IO that touches its [`PeerActivity`] on every read that fills
/// bytes. Writes are passed through: bytes the kernel accepts say nothing
/// about the peer.
struct Metered<S> {
    inner: S,
    activity: Arc<PeerActivity>,
}

impl<S: AsyncRead + Unpin> AsyncRead for Metered<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        let before = buf.filled().len();
        let polled = Pin::new(&mut this.inner).poll_read(cx, buf);
        if buf.filled().len() > before {
            this.activity.touch();
        }
        polled
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for Metered<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.get_mut().inner).poll_write(cx, buf)
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[std::io::IoSlice<'_>],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.get_mut().inner).poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}

/// The server end of an upgraded WebSocket, speaking axum's [`Message`] like
/// `axum::extract::ws::WebSocket`, over [`Metered`] IO.
pub(crate) struct MeteredWebSocket<S> {
    inner: WebSocketStream<Metered<S>>,
}

impl<S: AsyncRead + AsyncWrite + Unpin> MeteredWebSocket<S> {
    /// Speak WebSocket as the server over `io`, whose HTTP upgrade is done.
    pub(crate) async fn accept(io: S, config: WebSocketConfig) -> (Self, Arc<PeerActivity>) {
        let activity = PeerActivity::new();
        let io = Metered {
            inner: io,
            activity: Arc::clone(&activity),
        };
        let inner = WebSocketStream::from_raw_socket(io, Role::Server, Some(config)).await;
        (Self { inner }, activity)
    }
}

fn into_tungstenite(message: Message) -> tungstenite::Message {
    match message {
        Message::Text(text) => tungstenite::Message::Text(text.as_str().into()),
        Message::Binary(bytes) => tungstenite::Message::Binary(bytes),
        Message::Ping(bytes) => tungstenite::Message::Ping(bytes),
        Message::Pong(bytes) => tungstenite::Message::Pong(bytes),
        Message::Close(frame) => {
            tungstenite::Message::Close(frame.map(|frame| tungstenite::protocol::CloseFrame {
                code: frame.code.into(),
                reason: frame.reason.as_str().into(),
            }))
        }
    }
}

fn from_tungstenite(message: tungstenite::Message) -> Option<Message> {
    Some(match message {
        tungstenite::Message::Text(text) => Message::Text(Utf8Bytes::from(text.as_str())),
        tungstenite::Message::Binary(bytes) => Message::Binary(bytes),
        tungstenite::Message::Ping(bytes) => Message::Ping(bytes),
        tungstenite::Message::Pong(bytes) => Message::Pong(bytes),
        tungstenite::Message::Close(frame) => Message::Close(frame.map(|frame| CloseFrame {
            code: frame.code.into(),
            reason: Utf8Bytes::from(frame.reason.as_str()),
        })),
        // A raw frame is never yielded while reading; axum skips it too.
        tungstenite::Message::Frame(_) => return None,
    })
}

impl<S: AsyncRead + AsyncWrite + Unpin> Stream for MeteredWebSocket<S> {
    type Item = Result<Message, axum::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match ready!(Pin::new(&mut self.inner).poll_next(cx)) {
                Some(Ok(message)) => {
                    if let Some(message) = from_tungstenite(message) {
                        return Poll::Ready(Some(Ok(message)));
                    }
                }
                Some(Err(error)) => return Poll::Ready(Some(Err(axum::Error::new(error)))),
                None => return Poll::Ready(None),
            }
        }
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> Sink<Message> for MeteredWebSocket<S> {
    type Error = axum::Error;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.inner)
            .poll_ready(cx)
            .map_err(axum::Error::new)
    }

    fn start_send(mut self: Pin<&mut Self>, item: Message) -> Result<(), Self::Error> {
        Pin::new(&mut self.inner)
            .start_send(into_tungstenite(item))
            .map_err(axum::Error::new)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.inner)
            .poll_flush(cx)
            .map_err(axum::Error::new)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.inner)
            .poll_close(cx)
            .map_err(axum::Error::new)
    }
}
