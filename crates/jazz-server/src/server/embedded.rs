//! In-process serving for an embedding host.
//!
//! A host such as a JavaScript route handler owns the HTTP server. It passes
//! requests to [`EmbeddedServer::handle`] and hands each socket it has already
//! upgraded to [`EmbeddedServer::open_socket`]. The core binds no port; the
//! routes, admission and the sync session loop are the same as behind a
//! listener.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use axum::body::Body;
use axum::extract::ws::Message;
use axum::http::{HeaderMap, Request};
use axum::response::Response;
use futures::channel::mpsc;
use futures::{Sink, Stream};
use jazz::protocol_limits::MAX_WIRE_FRAME_BYTES;
use tower::ServiceExt as _;

use super::{BuiltServer, ServerState, ShutdownPhase, WebSocketEntryRefused};

/// A built server served by its host instead of a listener.
pub struct EmbeddedServer {
    built: BuiltServer,
    runtime: tokio::runtime::Handle,
}

/// Why a socket was not opened. The host closes its socket with this reason.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OpenSocketRefused {
    ShuttingDown,
    AtCapacity,
}

impl std::fmt::Display for OpenSocketRefused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ShuttingDown => f.write_str("server is shutting down"),
            Self::AtCapacity => f.write_str("server is at its websocket connection cap"),
        }
    }
}

impl EmbeddedServer {
    /// Serve `built` from the current tokio runtime.
    pub fn new(built: BuiltServer) -> Self {
        Self {
            built,
            runtime: tokio::runtime::Handle::current(),
        }
    }

    /// The runtime the server's tasks run on, for host-side tasks that
    /// belong to it (such as pumping a socket's outgoing messages).
    pub fn runtime(&self) -> &tokio::runtime::Handle {
        &self.runtime
    }

    pub fn state(&self) -> &Arc<ServerState> {
        &self.built.state
    }

    /// Route one HTTP request, exactly as the listener would.
    pub async fn handle(&self, request: Request<Body>) -> Response {
        match self.built.app.clone().oneshot(request).await {
            Ok(response) => response,
            Err(never) => match never {},
        }
    }

    /// [`Self::handle`] for hosts without `http` types: the method, the path
    /// and query, header pairs and the body in; status, header pairs and body
    /// out.
    pub async fn handle_parts(
        &self,
        method: &str,
        uri: &str,
        headers: &[(String, String)],
        body: Vec<u8>,
    ) -> Result<(u16, Vec<(String, String)>, Vec<u8>), String> {
        let mut request = Request::builder().method(method).uri(uri);
        for (name, value) in headers {
            request = request.header(name.as_str(), value.as_str());
        }
        let request = request
            .body(Body::from(body))
            .map_err(|error| format!("invalid request: {error}"))?;
        let response = self.handle(request).await;
        let status = response.status().as_u16();
        let headers = response
            .headers()
            .iter()
            .filter_map(|(name, value)| {
                value
                    .to_str()
                    .ok()
                    .map(|value| (name.as_str().to_owned(), value.to_owned()))
            })
            .collect();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .map_err(|error| format!("read response body: {error}"))?;
        Ok((status, headers, body.to_vec()))
    }

    /// [`Self::open_socket`] with the upgrade request's headers as pairs.
    /// Pairs that aren't valid header names or values are left out.
    pub fn open_socket_with(
        &self,
        headers: &[(String, String)],
    ) -> Result<EmbeddedSocketPeer, OpenSocketRefused> {
        let mut map = HeaderMap::new();
        for (name, value) in headers {
            if let (Ok(name), Ok(value)) = (
                axum::http::HeaderName::from_bytes(name.as_bytes()),
                axum::http::HeaderValue::from_str(value),
            ) {
                map.append(name, value);
            }
        }
        self.open_socket(map)
    }

    /// Start a sync session over a socket the host already upgraded.
    /// `headers` are the upgrade request's headers, used for admission the
    /// same way as on a listener (cookies, origin).
    pub fn open_socket(&self, headers: HeaderMap) -> Result<EmbeddedSocketPeer, OpenSocketRefused> {
        let state = self.built.state.clone();
        let guard = state
            .shutdown
            .try_enter_websocket_within(state.max_websocket_connections)
            .map_err(|refused| match refused {
                WebSocketEntryRefused::ShuttingDown => OpenSocketRefused::ShuttingDown,
                WebSocketEntryRefused::AtCapacity => OpenSocketRefused::AtCapacity,
            })?;
        let (peer, socket) = socket_pair();
        self.runtime.spawn(super::routes::handle_ws_connection(
            socket, state, headers, guard,
        ));
        Ok(peer)
    }

    /// Stop serving and close storage. Open sessions are closed first.
    pub async fn shutdown(&self) -> ShutdownPhase {
        self.built.shutdown().await
    }
}

/// The host's and the core's ends of one embedded socket.
fn socket_pair() -> (EmbeddedSocketPeer, EmbeddedSocket) {
    let (to_core, incoming) = mpsc::unbounded();
    let (outgoing, from_core) = mpsc::unbounded();
    let room = Arc::new(tokio::sync::Semaphore::new(EMBEDDED_INBOUND_BUDGET));
    let socket = EmbeddedSocket {
        incoming,
        outgoing,
        room: Arc::clone(&room),
    };
    let peer = EmbeddedSocketPeer {
        sender: EmbeddedSocketSender {
            to_core,
            room,
            unpaced: Arc::default(),
        },
        from_core,
    };
    (peer, socket)
}

/// The host's end of an embedded socket: messages for the client come out of
/// [`Self::recv`]; [`Self::sender`] passes the client's messages in.
pub struct EmbeddedSocketPeer {
    sender: EmbeddedSocketSender,
    from_core: mpsc::UnboundedReceiver<Message>,
}

/// Bytes of client messages [`EmbeddedSocketSender::send_with_room`] lets
/// wait for the core before it waits too: two of the largest messages.
const EMBEDDED_INBOUND_BUDGET: usize = 2 * MAX_WIRE_FRAME_BYTES;

/// Bytes [`EmbeddedSocketSender::send`] may queue past the inbound budget
/// for a host that does not wait for room: past it the session ends, as a
/// listener ends one whose peer ignores flow control.
const EMBEDDED_UNPACED_LIMIT: usize = 8 * EMBEDDED_INBOUND_BUDGET;

/// The room a client message holds until the session reads it.
enum Room {
    None,
    Budget {
        _permit: tokio::sync::OwnedSemaphorePermit,
    },
    Unpaced(usize, Arc<std::sync::atomic::AtomicUsize>),
}

impl Drop for Room {
    fn drop(&mut self) {
        if let Self::Unpaced(bytes, queued) = self {
            queued.fetch_sub(*bytes, std::sync::atomic::Ordering::AcqRel);
        }
    }
}

/// A client message on its way to the core, with its room.
type Inbound = (Message, Room);

/// Passes the client's messages to the core. Cheap to clone.
#[derive(Clone)]
pub struct EmbeddedSocketSender {
    to_core: mpsc::UnboundedSender<Inbound>,
    /// [`EMBEDDED_INBOUND_BUDGET`], as permits of one byte.
    room: Arc<tokio::sync::Semaphore>,
    /// Bytes [`Self::send`] queued past the budget, not read yet.
    unpaced: Arc<std::sync::atomic::AtomicUsize>,
}

/// What the core sends to the host's socket.
#[derive(Debug, PartialEq, Eq)]
pub enum EmbeddedOutgoing {
    Binary(Vec<u8>),
    Close { code: u16, reason: String },
}

impl EmbeddedSocketSender {
    /// Pass one binary message from the client without waiting. Returns
    /// `false` once the session has ended, or for a message larger than a
    /// listener accepts. A host that sends faster than the session reads,
    /// past [`EMBEDDED_UNPACED_LIMIT`], ends it: either ends the session, as
    /// a listener would.
    pub fn send(&self, bytes: Vec<u8>) -> bool {
        use std::sync::atomic::Ordering;
        if bytes.len() > MAX_WIRE_FRAME_BYTES {
            self.close();
            return false;
        }
        let permits = u32::try_from(bytes.len().max(1)).expect("a frame's size fits u32");
        let room = match Arc::clone(&self.room).try_acquire_many_owned(permits) {
            Ok(permit) => Room::Budget { _permit: permit },
            Err(tokio::sync::TryAcquireError::Closed) => return false,
            Err(tokio::sync::TryAcquireError::NoPermits) => {
                let len = bytes.len();
                if self.unpaced.fetch_add(len, Ordering::AcqRel) + len > EMBEDDED_UNPACED_LIMIT {
                    self.unpaced.fetch_sub(len, Ordering::AcqRel);
                    self.close();
                    return false;
                }
                Room::Unpaced(len, Arc::clone(&self.unpaced))
            }
        };
        self.to_core
            .unbounded_send((Message::Binary(bytes.into()), room))
            .is_ok()
    }

    /// [`Self::send`], once the session has read enough of the messages
    /// before it: a host that waits for this before reading more from its
    /// client holds a fast client back, as a socket's own flow control
    /// would, instead of queueing everything it sends.
    pub async fn send_with_room(&self, bytes: Vec<u8>) -> bool {
        if bytes.len() > MAX_WIRE_FRAME_BYTES {
            self.close();
            return false;
        }
        let permits = u32::try_from(bytes.len().max(1)).expect("a frame's size fits u32");
        let Ok(room) = Arc::clone(&self.room).acquire_many_owned(permits).await else {
            return false;
        };
        self.to_core
            .unbounded_send((
                Message::Binary(bytes.into()),
                Room::Budget { _permit: room },
            ))
            .is_ok()
    }

    /// The client's socket closed.
    pub fn close(&self) {
        let _ = self
            .to_core
            .unbounded_send((Message::Close(None), Room::None));
        self.to_core.close_channel();
        // Senders waiting for room give up.
        self.room.close();
    }
}

impl EmbeddedSocketPeer {
    pub fn sender(&self) -> EmbeddedSocketSender {
        self.sender.clone()
    }

    /// The next message for the client, or `None` once the session ended.
    /// Keepalive pings are answered here: the host's own socket keeps the
    /// client link alive, so they never reach it.
    pub async fn recv(&mut self) -> Option<EmbeddedOutgoing> {
        use futures::StreamExt as _;
        loop {
            match self.from_core.next().await? {
                Message::Binary(bytes) => return Some(EmbeddedOutgoing::Binary(bytes.to_vec())),
                Message::Text(text) => {
                    return Some(EmbeddedOutgoing::Binary(text.as_bytes().to_vec()));
                }
                Message::Ping(payload) => {
                    let _ = self
                        .sender
                        .to_core
                        .unbounded_send((Message::Pong(payload), Room::None));
                }
                Message::Pong(_) => {}
                Message::Close(frame) => {
                    let (code, reason) = frame
                        .map(|frame| (frame.code, frame.reason.to_string()))
                        .unwrap_or((1000, String::new()));
                    return Some(EmbeddedOutgoing::Close { code, reason });
                }
            }
        }
    }
}

/// The core's end: the same message stream an axum WebSocket gives.
struct EmbeddedSocket {
    incoming: mpsc::UnboundedReceiver<Inbound>,
    room: Arc<tokio::sync::Semaphore>,
    outgoing: mpsc::UnboundedSender<Message>,
}

impl Drop for EmbeddedSocket {
    /// The session ended: the room its unread messages hold never frees, so
    /// senders waiting for it give up.
    fn drop(&mut self) {
        self.room.close();
    }
}

impl Stream for EmbeddedSocket {
    type Item = Result<Message, axum::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // The message's room frees as the session takes it.
        Pin::new(&mut self.incoming)
            .poll_next(cx)
            .map(|item| item.map(|(message, _room)| Ok(message)))
    }
}

impl Sink<Message> for EmbeddedSocket {
    type Error = axum::Error;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.outgoing)
            .poll_ready(cx)
            .map_err(axum::Error::new)
    }

    fn start_send(mut self: Pin<&mut Self>, item: Message) -> Result<(), Self::Error> {
        Pin::new(&mut self.outgoing)
            .start_send(item)
            .map_err(axum::Error::new)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.outgoing)
            .poll_flush(cx)
            .map_err(axum::Error::new)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.outgoing)
            .poll_close(cx)
            .map_err(axum::Error::new)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt as _;

    fn frame(bytes: usize) -> Vec<u8> {
        vec![7; bytes]
    }

    #[tokio::test]
    async fn a_paced_send_waits_until_the_session_reads_earlier_messages() {
        let (peer, mut socket) = socket_pair();
        let sender = peer.sender();
        assert!(sender.send_with_room(frame(MAX_WIRE_FRAME_BYTES)).await);
        assert!(sender.send_with_room(frame(MAX_WIRE_FRAME_BYTES)).await);
        // The budget is full: the next send waits for the session.
        let waiting = tokio::spawn({
            let sender = sender.clone();
            async move { sender.send_with_room(frame(1024)).await }
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(!waiting.is_finished(), "a send past the budget waits");
        assert!(socket.next().await.is_some());
        assert!(waiting.await.unwrap(), "it goes once the session read one");
    }

    #[tokio::test]
    async fn a_paced_send_waiting_when_the_session_ends_gives_up() {
        let (peer, socket) = socket_pair();
        let sender = peer.sender();
        assert!(sender.send_with_room(frame(MAX_WIRE_FRAME_BYTES)).await);
        assert!(sender.send_with_room(frame(MAX_WIRE_FRAME_BYTES)).await);
        let waiting = tokio::spawn({
            let sender = sender.clone();
            async move { sender.send_with_room(frame(1024)).await }
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        drop(socket);
        assert!(!waiting.await.unwrap(), "the ended session takes nothing");
    }

    #[tokio::test]
    async fn an_unpaced_host_that_floods_the_session_ends_it() {
        let (peer, mut socket) = socket_pair();
        let sender = peer.sender();
        let mut sent = 0;
        while sender.send(frame(MAX_WIRE_FRAME_BYTES)) {
            sent += MAX_WIRE_FRAME_BYTES;
            assert!(sent <= EMBEDDED_INBOUND_BUDGET + EMBEDDED_UNPACED_LIMIT);
        }
        assert_eq!(sent, EMBEDDED_INBOUND_BUDGET + EMBEDDED_UNPACED_LIMIT);
        // What it queued still reads, then the close.
        let mut read = 0;
        while let Some(Ok(message)) = socket.next().await {
            match message {
                Message::Binary(bytes) => read += bytes.len(),
                Message::Close(_) => break,
                other => panic!("unexpected {other:?}"),
            }
        }
        assert_eq!(read, sent);
        assert!(!sender.send(frame(1)), "the session ended");
    }

    #[tokio::test]
    async fn an_unpaced_host_the_session_keeps_up_with_is_never_cut_off() {
        let (peer, mut socket) = socket_pair();
        let sender = peer.sender();
        for _ in 0..4 * (EMBEDDED_UNPACED_LIMIT / MAX_WIRE_FRAME_BYTES) {
            assert!(sender.send(frame(MAX_WIRE_FRAME_BYTES)));
            assert!(socket.next().await.is_some());
        }
    }
}
