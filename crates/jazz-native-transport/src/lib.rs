use jazz::tools::native_transport_connector::NativeTransportLink;
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::{SinkExt as _, StreamExt as _};
use jazz::ids::{AuthorSubject, NodeUuid};
use jazz::protocol_limits::{MAX_WIRE_BATCH_FRAMES, MAX_WIRE_FRAME_BYTES, validate_wire_frame_len};
use jazz::wire::{
    TransportError, WireAuthorityEndpoint, WireError, WireFrame, WireHello, WirePeerRole,
    WireTransport, current_wire_features, decode_frame, encode_frame, negotiate_wire,
};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use tokio::sync::{Semaphore, mpsc, oneshot};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;

use jazz::tools::AppId;
use jazz::tools::native_transport_connector::{
    ConnectedNativeTransport, NativeTransportConnector, NativeTransportError,
    NativeTransportFuture, NativeTransportRequest, NativeTransportTerminal,
    NativeTransportTerminalFuture,
};
use jazz::tools::websocket_prelude_auth::AuthConfig;

const WS_CLIENT_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);
/// When a link that carries nothing is given up.
///
/// A link that dies without closing (Wi-Fi off, a suspended server, a
/// half-open TCP path) reports no error and no EOF: without a deadline it
/// stays "connected" until the OS or the server's idle timeout (60 s) closes
/// the socket. "Carries nothing" means the peer gave no sign of life: no byte
/// arrived, and no write that was waiting on a full send buffer went through.
/// A large message arriving slowly is progress, and so is a slow upload, since
/// TCP only frees send buffer on the peer's ACK. A write into a buffer that
/// has room is not: the kernel takes it whether the peer is there or not.
///
/// The client never pings. The server pings an idle link every 20 s
/// (`WS_KEEPALIVE_PING_INTERVAL` in `jazz-server`), so the deadline sits above
/// two keepalive intervals, tolerating one late or lost ping, and a client
/// ping would be one more message for a server whose writer queue is already
/// full: it breaks the connection when it cannot queue a Pong.
#[derive(Clone, Copy, Debug)]
struct LivenessPolicy {
    silence_deadline: Duration,
    /// How often the pump checks. A check that comes more than two ticks
    /// late means the runtime or the process was not running (a blocked
    /// executor, an app suspended by the OS); that time is not the peer's
    /// silence.
    tick: Duration,
}

const WS_CLIENT_LIVENESS: LivenessPolicy = LivenessPolicy {
    silence_deadline: Duration::from_secs(45),
    tick: Duration::from_millis(500),
};
// The serving route caps client-to-server WebSocket messages at one MiB. Keep
// a small postcard framing reserve so a burst of individually-valid wire
// frames never becomes an invalid request message.
const WS_CLIENT_OUTBOUND_BATCH_BYTES: usize = 1 << 20;
const POSTCARD_FRAME_LENGTH_RESERVE: usize = 5;
const POSTCARD_BATCH_LENGTH_RESERVE: usize = 5;
/// Bound a malicious upstream before the wire reassembler gets a chance to
/// retain fragments. This applies equally to normal and bootstrap links. The
/// sender waits when full, so a valid fragmented maximum-size catalogue is
/// streamed through as its consumer drains rather than rejected mid-message.
const WS_CLIENT_INBOUND_FRAME_SLOTS: usize = 64;
const WS_CLIENT_MAX_QUEUED_BYTES: usize = 8 << 20;
const WS_CLIENT_MAX_OUTBOUND_QUEUED_BYTES: usize = 8 << 20;

#[derive(Debug)]
pub enum WebSocketClientError {
    Connect(tokio_tungstenite::tungstenite::Error),
    Send(tokio_tungstenite::tungstenite::Error),
    Receive(tokio_tungstenite::tungstenite::Error),
    ClosedDuringHandshake,
    /// The peer sent a Close frame before its hello, with this close code.
    ClosedBeforeHello {
        code: u16,
        reason: String,
    },
    HandshakeTimeout,
    UnexpectedHandshakeMessage,
    EncodePrelude(serde_json::Error),
    EncodeHello(postcard::Error),
    DecodeBatch(postcard::Error),
    DecodeFrame(postcard::Error),
    Negotiation(WireError),
    ServerRejected(String),
    ServerWireError(WireError),
}

impl fmt::Display for WebSocketClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Connect(error) => write!(f, "failed to connect websocket: {error}"),
            Self::Send(error) => write!(f, "failed to send websocket frame: {error}"),
            Self::Receive(error) => write!(f, "failed to receive websocket frame: {error}"),
            Self::ClosedDuringHandshake => {
                write!(f, "websocket closed during handshake")
            }
            Self::ClosedBeforeHello { code, reason } => {
                write!(
                    f,
                    "websocket closed before hello (code={code}, reason={reason})"
                )
            }
            Self::HandshakeTimeout => write!(f, "websocket handshake timed out"),
            Self::UnexpectedHandshakeMessage => {
                write!(f, "websocket returned an unexpected handshake message")
            }
            Self::EncodePrelude(error) => write!(f, "failed to encode websocket prelude: {error}"),
            Self::EncodeHello(error) => write!(f, "failed to encode websocket hello: {error}"),
            Self::DecodeBatch(error) => write!(f, "failed to decode frame batch: {error}"),
            Self::DecodeFrame(error) => write!(f, "failed to decode frame: {error}"),
            Self::Negotiation(error) => write!(f, "websocket negotiation failed: {error:?}"),
            Self::ServerRejected(reason) => write!(f, "websocket rejected: {reason}"),
            Self::ServerWireError(error) => write!(f, "websocket rejected: {error:?}"),
        }
    }
}

impl std::error::Error for WebSocketClientError {}

/// Connect failures retain whether an I/O error came from name resolution.
/// Rust's platform resolver may report DNS failures as `Uncategorized`, which
/// stable Rust intentionally does not expose for matching.
#[derive(Debug)]
enum NativeConnectFailure {
    Resolution(std::io::Error),
    WebSocket(WebSocketClientError),
}

impl From<WebSocketClientError> for NativeConnectFailure {
    fn from(error: WebSocketClientError) -> Self {
        Self::WebSocket(error)
    }
}

impl NativeConnectFailure {
    fn into_client_error(self) -> WebSocketClientError {
        match self {
            Self::Resolution(error) => {
                WebSocketClientError::Connect(tokio_tungstenite::tungstenite::Error::Io(error))
            }
            Self::WebSocket(error) => error,
        }
    }
}

fn native_connect_error(error: NativeConnectFailure) -> NativeTransportError {
    match error {
        NativeConnectFailure::Resolution(error) => {
            NativeTransportError::Retryable(format!("failed to resolve websocket host: {error}"))
        }
        NativeConnectFailure::WebSocket(error) => native_transport_error(error),
    }
}

/// Match typed transport/wire causes, never diagnostic strings. Retry only
/// failures that may resolve without changing the request or credentials.
fn native_transport_error(error: WebSocketClientError) -> NativeTransportError {
    let retryable = match &error {
        // A proxy may accept TCP while its upstream is restarting and close
        // before HTTP upgrade completes. No rejected response was received.
        WebSocketClientError::Connect(tokio_tungstenite::tungstenite::Error::Protocol(
            tokio_tungstenite::tungstenite::error::ProtocolError::HandshakeIncomplete,
        )) => true,
        // Tungstenite represents an established TCP EOF without a Close frame
        // as a protocol error. It is connection loss, unlike malformed frames
        // or rejected authentication, and must not poison the local runtime.
        WebSocketClientError::Send(tokio_tungstenite::tungstenite::Error::Protocol(
            tokio_tungstenite::tungstenite::error::ProtocolError::ResetWithoutClosingHandshake,
        ))
        | WebSocketClientError::Receive(tokio_tungstenite::tungstenite::Error::Protocol(
            tokio_tungstenite::tungstenite::error::ProtocolError::ResetWithoutClosingHandshake,
        )) => true,
        WebSocketClientError::Connect(tokio_tungstenite::tungstenite::Error::Io(error))
        | WebSocketClientError::Send(tokio_tungstenite::tungstenite::Error::Io(error))
        | WebSocketClientError::Receive(tokio_tungstenite::tungstenite::Error::Io(error)) => {
            // The rustls connector also returns certificate/protocol failures
            // as Io(InvalidData), retaining rustls::Error as the source. An Io
            // wrapper alone is therefore not evidence of network unavailability.
            matches!(
                error.kind(),
                std::io::ErrorKind::ConnectionRefused
                    | std::io::ErrorKind::ConnectionReset
                    | std::io::ErrorKind::BrokenPipe
                    | std::io::ErrorKind::ConnectionAborted
                    | std::io::ErrorKind::NotConnected
                    | std::io::ErrorKind::TimedOut
                    | std::io::ErrorKind::WouldBlock
                    | std::io::ErrorKind::Interrupted
                    | std::io::ErrorKind::NetworkDown
                    | std::io::ErrorKind::NetworkUnreachable
                    | std::io::ErrorKind::HostUnreachable
                    | std::io::ErrorKind::AddrNotAvailable
                    // rustls reports a peer that closes TLS without
                    // close_notify as UnexpectedEof. A cleanly reconnectable
                    // idle peer must not turn a foreground tick terminal.
                    | std::io::ErrorKind::UnexpectedEof
            )
        }
        WebSocketClientError::HandshakeTimeout => true,
        // A relay (the app's backend) accepts the upgrade and then closes
        // when it cannot reach its core. Like the HTTP statuses below, only
        // close codes that describe transient availability retry: going
        // away, server error, restart, try again later and bad gateway. A
        // refused sign-in arrives as an auth-failed wire error first.
        WebSocketClientError::ClosedBeforeHello { code, .. } => {
            matches!(code, 1001 | 1011 | 1012 | 1013 | 1014)
        }
        // Only HTTP statuses that describe transient availability are
        // retryable. This includes request timeout, too early, throttling,
        // and common temporary server failures. Authorization, unsupported
        // features, and other unclassified responses remain terminal.
        WebSocketClientError::Connect(tokio_tungstenite::tungstenite::Error::Http(response)) => {
            matches!(
                response.status().as_u16(),
                408 | 425 | 429 | 500 | 502 | 503 | 504
            )
        }
        WebSocketClientError::ServerWireError(error) => {
            error.code == jazz::wire::WireErrorCode::NotReady
                && error.retry == jazz::wire::WireRetry::Later
        }
        _ => false,
    };
    // A refused credential stays refused until the caller brings a new one,
    // whether the upgrade itself was refused or Core said so before hello.
    let auth_rejected = match &error {
        WebSocketClientError::Connect(tokio_tungstenite::tungstenite::Error::Http(response)) => {
            response.status().as_u16() == 401
        }
        WebSocketClientError::ServerWireError(error) => {
            error.code == jazz::wire::WireErrorCode::AuthFailed
        }
        _ => false,
    };
    if retryable {
        NativeTransportError::Retryable(error.to_string())
    } else if auth_rejected {
        NativeTransportError::AuthRejected(error.to_string())
    } else {
        NativeTransportError::Terminal(error.to_string())
    }
}

pub struct WebSocketTransport {
    inbound: Arc<Mutex<mpsc::Receiver<InboundFrame>>>,
    outbound: BoundedOutbound,
    task: tokio::task::JoinHandle<()>,
    terminal: Option<oneshot::Receiver<NativeTransportTerminal>>,
    terminal_publisher: Arc<Mutex<Option<oneshot::Sender<NativeTransportTerminal>>>>,
    protocol_version: u16,
    features: u64,
}

/// Tokio/TLS WebSocket implementation selected by native process and binding
/// shells.  Core code receives this through `NativeTransportConnector` and
/// never names this adapter crate.
#[derive(Clone, Copy, Debug, Default)]
pub struct NativeWebSocketConnector;

impl NativeTransportConnector for NativeWebSocketConnector {
    fn connect(&self, request: NativeTransportRequest) -> NativeTransportFuture {
        Box::pin(async move {
            let mut transport = WebSocketTransport::connect_with_link(
                request.server_url,
                request.app_id,
                request.peer_identity,
                request.auth,
                Upgrade {
                    authorization: request.authorization,
                    bundle: request.bundle,
                },
                request.wake,
                request.requested_link,
            )
            .await
            .map_err(native_connect_error)?;
            let (protocol_version, features) = transport.negotiated_transport_metadata();
            let terminal = transport.take_terminal_future();
            Ok(ConnectedNativeTransport {
                transport: Box::new(transport),
                protocol_version,
                features,
                terminal,
            })
        })
    }
}

struct InboundFrame {
    bytes: Vec<u8>,
    // Keeping the permit with the queued frame makes the byte bound apply
    // until the synchronous wire consumer has actually removed the frame.
    _budget: tokio::sync::OwnedSemaphorePermit,
}

struct QueuedOutboundFrame {
    bytes: Vec<u8>,
    charge: usize,
    queued_bytes: Arc<AtomicUsize>,
}

impl Drop for QueuedOutboundFrame {
    fn drop(&mut self) {
        self.queued_bytes.fetch_sub(self.charge, Ordering::AcqRel);
    }
}

struct BoundedOutbound {
    sender: mpsc::UnboundedSender<QueuedOutboundFrame>,
    queued_bytes: Arc<AtomicUsize>,
    backpressured: Arc<AtomicBool>,
}

impl BoundedOutbound {
    fn channel() -> (
        Self,
        mpsc::UnboundedReceiver<QueuedOutboundFrame>,
        Arc<AtomicBool>,
    ) {
        let (sender, receiver) = mpsc::unbounded_channel();
        let queued_bytes = Arc::new(AtomicUsize::new(0));
        let backpressured = Arc::new(AtomicBool::new(false));
        (
            Self {
                sender,
                queued_bytes,
                backpressured: Arc::clone(&backpressured),
            },
            receiver,
            backpressured,
        )
    }

    fn send(&self, bytes: Vec<u8>) -> Result<(), TransportError> {
        self.send_after_backpressure_arm(bytes, || {})
    }

    fn send_after_backpressure_arm(
        &self,
        bytes: Vec<u8>,
        after_backpressure_arm: impl FnOnce(),
    ) -> Result<(), TransportError> {
        // A per-frame floor also caps tiny-frame queue cardinality at 512.
        let charge = bytes.len().max(16 * 1024);
        let reserve = || {
            self.queued_bytes
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                    current
                        .checked_add(charge)
                        .filter(|next| *next <= WS_CLIENT_MAX_OUTBOUND_QUEUED_BYTES)
                })
        };
        if reserve().is_err() {
            // Arm before checking capacity again. If the pump drains between
            // the failed reservation and this store, the second reservation
            // succeeds; if it drains afterwards, it observes this flag and
            // wakes the producer. Neither ordering loses the wake.
            self.backpressured.store(true, Ordering::Release);
            after_backpressure_arm();
            if reserve().is_err() {
                return Err(TransportError::Backpressure);
            }
        }
        let frame = QueuedOutboundFrame {
            bytes,
            charge,
            queued_bytes: Arc::clone(&self.queued_bytes),
        };
        self.sender.send(frame).map_err(|error| {
            drop(error.0);
            TransportError::Failed("websocket pump is closed".to_owned())
        })
    }
}

impl fmt::Debug for BoundedOutbound {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BoundedOutbound")
            .field("queued_bytes", &self.queued_bytes.load(Ordering::Acquire))
            .field("max_bytes", &WS_CLIENT_MAX_OUTBOUND_QUEUED_BYTES)
            .finish_non_exhaustive()
    }
}

struct OutboundBatch<'a>(&'a [QueuedOutboundFrame]);

impl serde::Serialize for OutboundBatch<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeSeq as _;

        let mut sequence = serializer.serialize_seq(Some(self.0.len()))?;
        for frame in self.0 {
            sequence.serialize_element(&frame.bytes)?;
        }
        sequence.end()
    }
}

/// Serialize only a carrier the peer-side bounded decoder can admit. The
/// physical limit includes postcard's count and byte-string length prefixes.
fn encode_outbound_batch(batch: &[QueuedOutboundFrame]) -> Result<Vec<u8>, postcard::Error> {
    if batch.is_empty()
        || batch.len() > MAX_WIRE_BATCH_FRAMES
        || batch
            .iter()
            .any(|frame| validate_wire_frame_len(frame.bytes.len()).is_err())
    {
        return Err(postcard::Error::SerializeBufferFull);
    }
    let encoded = postcard::to_allocvec(&OutboundBatch(batch))?;
    if validate_wire_frame_len(encoded.len()).is_err() {
        return Err(postcard::Error::SerializeBufferFull);
    }
    Ok(encoded)
}

impl fmt::Debug for WebSocketTransport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WebSocketTransport")
            .field("inbound", &"bounded streaming channel")
            .field("outbound", &self.outbound)
            .field("task", &self.task)
            .finish_non_exhaustive()
    }
}

impl WebSocketTransport {
    pub async fn connect(
        base_url: impl AsRef<str>,
        app_id: AppId,
        peer_identity: AuthorSubject,
        auth: AuthConfig,
    ) -> Result<Self, WebSocketClientError> {
        Self::connect_with_wake(base_url, app_id, peer_identity, auth, Arc::new(|| {})).await
    }

    pub async fn connect_with_wake(
        base_url: impl AsRef<str>,
        app_id: AppId,
        peer_identity: AuthorSubject,
        auth: AuthConfig,
        wake: Arc<dyn Fn() + Send + Sync>,
    ) -> Result<Self, WebSocketClientError> {
        Self::connect_with_link(
            base_url,
            app_id,
            peer_identity,
            auth,
            Upgrade::default(),
            wake,
            NativeTransportLink::OrdinarySession,
        )
        .await
        .map_err(NativeConnectFailure::into_client_error)
    }

    async fn connect_with_link(
        base_url: impl AsRef<str>,
        app_id: AppId,
        peer_identity: AuthorSubject,
        auth: AuthConfig,
        upgrade: Upgrade,
        wake: Arc<dyn Fn() + Send + Sync>,
        requested_link: NativeTransportLink,
    ) -> Result<Self, NativeConnectFailure> {
        let Upgrade {
            authorization,
            bundle,
        } = upgrade;
        let deadline = tokio::time::Instant::now() + WS_CLIENT_HANDSHAKE_TIMEOUT;
        let url = ws_url(base_url.as_ref(), app_id);
        let activity = LinkActivity::new();
        let (mut ws, _) = tokio::time::timeout_at(
            deadline,
            open_client_websocket(url, authorization.as_deref(), Arc::clone(&activity)),
        )
        .await
        .map_err(|_| NativeConnectFailure::WebSocket(WebSocketClientError::HandshakeTimeout))??;

        let prelude = encode_prelude(peer_identity, auth, bundle, requested_link)?;
        tokio::time::timeout_at(deadline, ws.send(Message::Binary(prelude.into())))
            .await
            .map_err(|_| WebSocketClientError::HandshakeTimeout)?
            .map_err(WebSocketClientError::Send)?;

        let client_endpoint = WireAuthorityEndpoint::fresh(
            // The server authenticates the session subject separately. This
            // endpoint only binds a fresh wire link and is never trusted as a
            // semantic identity.
            NodeUuid(uuid::Uuid::new_v5(
                &uuid::Uuid::NAMESPACE_URL,
                peer_identity.canonical().as_bytes(),
            )),
        );
        let hello = WireFrame::Hello(
            WireHello::current(WirePeerRole::Client, current_wire_features())
                .with_authority(client_endpoint.node, client_endpoint.epoch),
        );
        let encoded_hello = encode_frame(&hello).map_err(WebSocketClientError::EncodeHello)?;
        let batch = postcard::to_allocvec(&vec![encoded_hello])
            .map_err(WebSocketClientError::EncodeHello)?;
        tokio::time::timeout_at(deadline, ws.send(Message::Binary(batch.into())))
            .await
            .map_err(|_| WebSocketClientError::HandshakeTimeout)?
            .map_err(WebSocketClientError::Send)?;

        let server_hello = receive_server_hello(&mut ws, deadline).await?;
        let negotiated = negotiate_wire(&server_hello, current_wire_features())
            .map_err(WebSocketClientError::Negotiation)?;
        if requested_link == NativeTransportLink::ScopeIsolatedClientRelay
            && negotiated.features & jazz::wire::FEATURE_SCOPE_ISOLATED_CLIENT_RELAY == 0
        {
            return Err(WebSocketClientError::ServerRejected(
                "upstream does not support requested scope_isolated_client_relay link".to_owned(),
            )
            .into());
        }

        let (inbound_tx, inbound_rx) = mpsc::channel(WS_CLIENT_INBOUND_FRAME_SLOTS);
        let inbound = Arc::new(Mutex::new(inbound_rx));
        let inbound_budget = Arc::new(Semaphore::new(WS_CLIENT_MAX_QUEUED_BYTES));
        let (outbound, outbound_rx, outbound_backpressured) = BoundedOutbound::channel();
        let (terminal_tx, terminal) = oneshot::channel();
        let terminal_publisher = Arc::new(Mutex::new(Some(terminal_tx)));
        let pump_terminal_publisher = Arc::clone(&terminal_publisher);
        let task = tokio::spawn(async move {
            let terminal = run_ws_pump(
                ws,
                Liveness::new(activity, WS_CLIENT_LIVENESS),
                inbound_tx,
                inbound_budget,
                outbound_rx,
                outbound_backpressured,
                wake,
            )
            .await;
            if let Some(publisher) = pump_terminal_publisher
                .lock()
                .expect("terminal publisher lock")
                .take()
            {
                let _ = publisher.send(terminal);
            }
        });

        Ok(Self {
            inbound,
            outbound,
            task,
            terminal: Some(terminal),
            terminal_publisher,
            protocol_version: negotiated.protocol_version,
            features: negotiated.features,
        })
    }

    /// Negotiated metadata authenticated during the websocket handshake.
    pub fn negotiated_transport_metadata(&self) -> (u16, u64) {
        (self.protocol_version, self.features)
    }

    fn take_terminal_future(&mut self) -> NativeTransportTerminalFuture {
        let terminal = self
            .terminal
            .take()
            .expect("native transport terminal future is taken exactly once");
        Box::pin(async move {
            terminal.await.unwrap_or_else(|_| {
                NativeTransportTerminal::Failed(NativeTransportError::Terminal(
                    "websocket pump stopped without a terminal reason".to_owned(),
                ))
            })
        })
    }
}

impl Drop for WebSocketTransport {
    fn drop(&mut self) {
        if let Some(publisher) = self
            .terminal_publisher
            .lock()
            .expect("terminal publisher lock")
            .take()
        {
            let _ = publisher.send(NativeTransportTerminal::OwnerDropped);
        }
        self.task.abort();
    }
}

impl WireTransport for WebSocketTransport {
    fn send_frame(&mut self, frame: Vec<u8>) -> Result<(), TransportError> {
        // A failed reservation is retried after the pump releases a sent batch
        // and wakes the synchronous owner. Ordinary sends do not wake it, which
        // avoids turning outbound draining into an empty-tick feedback loop.
        self.outbound.send(frame)
    }

    fn try_recv_frame(&mut self) -> Option<Vec<u8>> {
        let mut inbound = self.inbound.lock().ok()?;
        let frame = inbound.try_recv().ok();
        frame.map(|frame| frame.bytes)
    }
}

#[derive(serde::Serialize)]
struct WebSocketClientPrelude {
    peer_identity: String,
    auth: AuthConfig,
    /// The published bundle a stated session is pinned to.
    #[serde(skip_serializing_if = "Option::is_none")]
    bundle: Option<String>,

    #[serde(skip_serializing_if = "is_ordinary_link")]
    requested_link: NativeTransportLink,
}

/// What a socket carries beside its auth: the upgrade's `Authorization`
/// header (for the app's own backend) and the bundle a stated session is
/// pinned to.
#[derive(Default)]
struct Upgrade {
    authorization: Option<String>,
    bundle: Option<String>,
}

fn is_ordinary_link(value: &NativeTransportLink) -> bool {
    *value == NativeTransportLink::OrdinarySession
}

fn encode_prelude(
    peer_identity: AuthorSubject,
    auth: AuthConfig,
    bundle: Option<String>,

    requested_link: NativeTransportLink,
) -> Result<Vec<u8>, WebSocketClientError> {
    serde_json::to_vec(&WebSocketClientPrelude {
        peer_identity: peer_identity.canonical().to_owned(),
        auth,
        bundle,

        requested_link,
    })
    .map_err(WebSocketClientError::EncodePrelude)
}

fn ws_url(base_url: &str, app_id: AppId) -> String {
    let base = base_url
        .replace("http://", "ws://")
        .replace("https://", "wss://")
        .trim_end_matches('/')
        .to_owned();
    format!("{base}/apps/{app_id}/ws")
}

/// Open the client socket with Nagle disabled. The client sends small
/// multiplexed frames in pairs (a header then its payload, a mutation then a
/// read), which otherwise wait on the server's delayed ACK. Mirrors the
/// server-side accept policy in `jazz-server`'s `tcp` module (#3268).
/// The socket is dialed by hand (to keep resolver errors typed), so
/// `set_nodelay` is applied here rather than via `connect_async`'s flag.
///
/// Every byte the socket moves, TLS records included, is reported to
/// `activity`, which the pump's silence deadline reads.
async fn open_client_websocket(
    url: String,
    authorization: Option<&str>,
    activity: Arc<LinkActivity>,
) -> Result<
    (
        ClientSocket,
        tokio_tungstenite::tungstenite::handshake::client::Response,
    ),
    NativeConnectFailure,
> {
    let mut request = url
        .into_client_request()
        .map_err(WebSocketClientError::Connect)?;
    if let Some(authorization) = authorization {
        let value = authorization.parse().map_err(|_| {
            WebSocketClientError::Connect(tokio_tungstenite::tungstenite::Error::HttpFormat(
                tokio_tungstenite::tungstenite::http::Error::from(
                    tokio_tungstenite::tungstenite::http::header::HeaderValue::from_bytes(b"\n")
                        .unwrap_err(),
                ),
            ))
        })?;
        request.headers_mut().insert(
            tokio_tungstenite::tungstenite::http::header::AUTHORIZATION,
            value,
        );
    }
    let host = request.uri().host().ok_or_else(|| {
        WebSocketClientError::Connect(tokio_tungstenite::tungstenite::Error::Url(
            tokio_tungstenite::tungstenite::error::UrlError::NoHostName,
        ))
    })?;
    let port = request
        .uri()
        .port_u16()
        .or_else(|| match request.uri().scheme_str() {
            Some("wss") => Some(443),
            Some("ws") => Some(80),
            _ => None,
        })
        .ok_or_else(|| {
            WebSocketClientError::Connect(tokio_tungstenite::tungstenite::Error::Url(
                tokio_tungstenite::tungstenite::error::UrlError::UnsupportedUrlScheme,
            ))
        })?;
    let addresses = tokio::net::lookup_host(format!("{host}:{port}"))
        .await
        .map_err(NativeConnectFailure::Resolution)?;
    let mut stream = None;
    let mut last_error = None;
    for address in addresses {
        match tokio::net::TcpStream::connect(address).await {
            Ok(connected) => {
                connected.set_nodelay(true).map_err(|error| {
                    WebSocketClientError::Connect(tokio_tungstenite::tungstenite::Error::Io(error))
                })?;
                stream = Some(connected);
                break;
            }
            Err(error) => last_error = Some(error),
        }
    }
    let stream = match (stream, last_error) {
        (Some(stream), _) => stream,
        (None, Some(error)) => {
            return Err(
                WebSocketClientError::Connect(tokio_tungstenite::tungstenite::Error::Io(error))
                    .into(),
            );
        }
        (None, None) => {
            return Err(NativeConnectFailure::Resolution(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "could not resolve to any address",
            )));
        }
    };
    let connection = tokio_tungstenite::client_async_tls_with_config(
        request,
        Metered::new(stream, activity),
        Some(client_websocket_config()),
        None,
    )
    .await
    .map_err(WebSocketClientError::Connect)?;
    Ok(connection)
}

fn client_websocket_config() -> WebSocketConfig {
    WebSocketConfig::default()
        // The server can batch raw wire frames up to this protocol boundary;
        // logical catalogue snapshots still span many such WebSocket messages.
        .max_message_size(Some(MAX_WIRE_FRAME_BYTES))
        .max_frame_size(Some(MAX_WIRE_FRAME_BYTES))
}

type ClientSocket = tokio_tungstenite::WebSocketStream<
    tokio_tungstenite::MaybeTlsStream<Metered<tokio::net::TcpStream>>,
>;

/// The last moment the peer gave a sign of life on the socket.
struct LinkActivity {
    origin: tokio::time::Instant,
    /// Nanoseconds after `origin`.
    last: AtomicU64,
}

impl LinkActivity {
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

    /// Move the last sign of life forward by `gap`, never past now: time
    /// the pump could not have heard anything in is not the peer's silence.
    fn excuse(&self, gap: Duration) {
        let gap = u64::try_from(gap.as_nanos()).unwrap_or(u64::MAX);
        let now = u64::try_from(self.origin.elapsed().as_nanos()).unwrap_or(u64::MAX);
        let _ = self
            .last
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |last| {
                Some(last.saturating_add(gap).min(now))
            });
    }

    fn quiet_for(&self, now: tokio::time::Instant) -> Duration {
        let last = self.origin + Duration::from_nanos(self.last.load(Ordering::Relaxed));
        now.saturating_duration_since(last)
    }
}

/// A socket that reports the peer's signs of life to a [`LinkActivity`]:
/// every byte read, and every write that goes through after the send buffer
/// was full.
struct Metered<S> {
    inner: S,
    activity: Arc<LinkActivity>,
    /// The last write found the send buffer full. The next one that goes
    /// through means the peer acknowledged data.
    write_blocked: bool,
}

impl<S> Metered<S> {
    fn new(inner: S, activity: Arc<LinkActivity>) -> Self {
        activity.touch();
        Self {
            inner,
            activity,
            write_blocked: false,
        }
    }

    fn observe_write(&mut self, polled: &std::task::Poll<std::io::Result<usize>>) {
        match polled {
            std::task::Poll::Pending => self.write_blocked = true,
            std::task::Poll::Ready(Ok(written)) if *written > 0 => {
                if std::mem::take(&mut self.write_blocked) {
                    self.activity.touch();
                }
            }
            std::task::Poll::Ready(_) => {}
        }
    }

    #[cfg(test)]
    fn get_ref(&self) -> &S {
        &self.inner
    }
}

impl<S: tokio::io::AsyncRead + Unpin> tokio::io::AsyncRead for Metered<S> {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let this = self.get_mut();
        let before = buf.filled().len();
        let polled = std::pin::Pin::new(&mut this.inner).poll_read(cx, buf);
        if buf.filled().len() > before {
            this.activity.touch();
        }
        polled
    }
}

impl<S: tokio::io::AsyncWrite + Unpin> tokio::io::AsyncWrite for Metered<S> {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        let this = self.get_mut();
        let polled = std::pin::Pin::new(&mut this.inner).poll_write(cx, buf);
        this.observe_write(&polled);
        polled
    }

    fn poll_write_vectored(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        bufs: &[std::io::IoSlice<'_>],
    ) -> std::task::Poll<std::io::Result<usize>> {
        let this = self.get_mut();
        let polled = std::pin::Pin::new(&mut this.inner).poll_write_vectored(cx, bufs);
        this.observe_write(&polled);
        polled
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}

/// The pump's silence check over a socket's [`LinkActivity`].
struct Liveness {
    activity: Arc<LinkActivity>,
    policy: LivenessPolicy,
    checked_at: tokio::time::Instant,
}

impl Liveness {
    fn new(activity: Arc<LinkActivity>, policy: LivenessPolicy) -> Self {
        Self {
            activity,
            policy,
            checked_at: tokio::time::Instant::now(),
        }
    }

    fn ticker(&self) -> tokio::time::Interval {
        let mut ticker = tokio::time::interval(self.policy.tick);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        ticker
    }

    /// Why the link is given up, if it is.
    fn check(&mut self) -> Option<String> {
        let now = tokio::time::Instant::now();
        let since_last_check = now.saturating_duration_since(self.checked_at);
        self.checked_at = now;
        if since_last_check > self.policy.tick * 2 {
            // The pump was not running, so nothing could have been read in
            // that gap: excuse it, and only it. Silence from before the gap
            // still counts, and the judgement waits a tick, so data that
            // arrived meanwhile is read first.
            self.activity
                .excuse(since_last_check.saturating_sub(self.policy.tick));
            return None;
        }
        let quiet = self.activity.quiet_for(now);
        (quiet >= self.policy.silence_deadline).then(|| {
            format!(
                "websocket peer silent for {} ms, no byte moved either way",
                quiet.as_millis()
            )
        })
    }

    /// The pump was busy handing frames to the application, which is not the
    /// peer's silence.
    fn resume(&mut self) {
        self.activity.touch();
        self.checked_at = tokio::time::Instant::now();
    }
}

/// How long a pump that is ending waits for its Close frame to be written. A
/// peer that has gone would otherwise hold the ending, and with it the
/// terminal the link waits on, for ever.
const WS_CLIENT_CLOSE_TIMEOUT: Duration = Duration::from_secs(1);

type ClientSink<S> = futures::stream::SplitSink<tokio_tungstenite::WebSocketStream<S>, Message>;
type ClientSource<S> = futures::stream::SplitStream<tokio_tungstenite::WebSocketStream<S>>;

/// How a pump ends: with this terminal, closing the socket first or not.
struct Ending {
    terminal: NativeTransportTerminal,
    close: bool,
}

impl Ending {
    fn now(terminal: NativeTransportTerminal) -> Self {
        Self {
            terminal,
            close: false,
        }
    }

    fn closing(terminal: NativeTransportTerminal) -> Self {
        Self {
            terminal,
            close: true,
        }
    }
}

/// Where received frames go: the application's bounded queue.
struct InboundSide {
    frames: mpsc::Sender<InboundFrame>,
    budget: Arc<Semaphore>,
    wake: Arc<dyn Fn() + Send + Sync>,
}

impl InboundSide {
    /// Hand one received message to the application.
    async fn deliver(
        &self,
        message: Option<Result<Message, tokio_tungstenite::tungstenite::Error>>,
    ) -> Result<(), Ending> {
        let bytes = match message {
            Some(Ok(Message::Binary(bytes))) => bytes,
            Some(Ok(Message::Ping(_) | Message::Pong(_))) => return Ok(()),
            Some(Ok(Message::Close(frame))) => {
                let reason = format!("websocket peer closed: {frame:?}");
                return Err(Ending::now(NativeTransportTerminal::PeerClosed(reason)));
            }
            Some(Ok(_)) => {
                let reason = "websocket peer sent a non-binary wire batch".to_owned();
                return Err(Ending::closing(NativeTransportTerminal::Failed(
                    NativeTransportError::Terminal(reason),
                )));
            }
            Some(Err(error)) => {
                let classified = native_transport_error(WebSocketClientError::Receive(error));
                return Err(Ending::now(NativeTransportTerminal::Failed(classified)));
            }
            None => {
                let reason = "websocket peer closed before completing wire exchange".to_owned();
                return Err(Ending::now(NativeTransportTerminal::PeerClosed(reason)));
            }
        };
        let frames = decode_inbound_batch(&bytes).map_err(|error| {
            Ending::closing(NativeTransportTerminal::Failed(
                NativeTransportError::Terminal(error),
            ))
        })?;
        for frame in frames {
            validate_wire_frame_len(frame.len()).map_err(|error| {
                Ending::closing(NativeTransportTerminal::Failed(
                    NativeTransportError::Terminal(error),
                ))
            })?;
            let permits = u32::try_from(frame.len()).expect("wire frame limit fits u32");
            let Ok(budget) = Arc::clone(&self.budget).acquire_many_owned(permits).await else {
                return Err(Ending::now(NativeTransportTerminal::OwnerDropped));
            };
            if self
                .frames
                .send(InboundFrame {
                    bytes: frame,
                    _budget: budget,
                })
                .await
                .is_err()
            {
                return Err(Ending::now(NativeTransportTerminal::OwnerDropped));
            }
            // A maximum logical message can exceed the bounded channel. Wake
            // on every frame so its consumer drains before the producer
            // blocks on a later fragment.
            (self.wake)();
        }
        Ok(())
    }
}

/// Send one message, reading while it is sent, and giving the link up when
/// the peer gives no sign of life for the silence deadline.
///
/// Reading goes on during the send because a send can wait a long time on a
/// live peer: a server busy with a backlog stops reading the socket (its TCP
/// window closes) but keeps sending, its keepalive pings included. Those are
/// the signs of life, and frames it sends meanwhile reach the application, so
/// neither side waits on the other. A socket whose peer has gone leaves the
/// send pending with a full kernel buffer and nothing to read.
async fn send_watched<S>(
    sink: &mut ClientSink<S>,
    source: &mut ClientSource<S>,
    inbound: &InboundSide,
    message: Message,
    liveness: &mut Liveness,
    ticker: &mut tokio::time::Interval,
) -> Result<(), Ending>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let send = sink.send(message);
    tokio::pin!(send);
    loop {
        tokio::select! {
            biased;
            sent = &mut send => {
                return sent.map_err(|error| {
                    Ending::now(NativeTransportTerminal::Failed(native_transport_error(
                        WebSocketClientError::Send(error),
                    )))
                });
            }
            message = source.next() => {
                inbound.deliver(message).await?;
                liveness.resume();
            }
            _ = ticker.tick() => {
                if let Some(reason) = liveness.check() {
                    return Err(Ending::now(NativeTransportTerminal::Failed(
                        NativeTransportError::Retryable(format!("{reason} while sending")),
                    )));
                }
            }
        }
    }
}

async fn receive_server_hello<S>(
    ws: &mut tokio_tungstenite::WebSocketStream<S>,
    deadline: tokio::time::Instant,
) -> Result<WireHello, WebSocketClientError>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let message = tokio::time::timeout_at(deadline, ws.next())
        .await
        .map_err(|_| WebSocketClientError::HandshakeTimeout)?
        .ok_or(WebSocketClientError::ClosedDuringHandshake)?
        .map_err(WebSocketClientError::Receive)?;
    let bytes = match message {
        Message::Binary(bytes) => bytes,
        Message::Close(frame) => {
            let (code, reason) = frame
                .map(|frame| (u16::from(frame.code), frame.reason.to_string()))
                // A Close frame without a status is "no status received".
                .unwrap_or((1005, String::new()));
            return Err(WebSocketClientError::ClosedBeforeHello { code, reason });
        }
        _ => return Err(WebSocketClientError::UnexpectedHandshakeMessage),
    };
    let encoded = jazz::wire::decode_websocket_frame_batch(&bytes)
        .map_err(WebSocketClientError::DecodeBatch)?;
    if encoded.len() != 1 {
        return Err(WebSocketClientError::UnexpectedHandshakeMessage);
    }
    if jazz::wire::wire_hello_version_range(&encoded[0]).is_none() {
        let frame = decode_frame(&encoded[0]).map_err(WebSocketClientError::DecodeFrame)?;
        if let WireFrame::Error(error) = frame {
            return Err(WebSocketClientError::ServerWireError(error));
        }
        return Err(WebSocketClientError::UnexpectedHandshakeMessage);
    }
    // Read the version range before the version-scoped body, so a server on
    // another wire version is reported as a version mismatch.
    let hello =
        jazz::wire::decode_hello_frame(&encoded[0]).map_err(WebSocketClientError::Negotiation)?;
    if hello.role != WirePeerRole::Core {
        return Err(WebSocketClientError::UnexpectedHandshakeMessage);
    }
    Ok(hello)
}

fn finish_outbound_pump(
    outbound: mpsc::UnboundedReceiver<QueuedOutboundFrame>,
    backpressured: &AtomicBool,
    wake: &(dyn Fn() + Send + Sync),
) {
    // A backpressured producer retries when woken. Make the terminal channel
    // state visible first, so that retry fails rather than re-entering the
    // still-full queue with no pump left to wake it again.
    drop(outbound);
    if backpressured.swap(false, Ordering::AcqRel) {
        wake();
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_ws_pump<S>(
    ws: tokio_tungstenite::WebSocketStream<S>,
    mut liveness: Liveness,
    inbound: mpsc::Sender<InboundFrame>,
    inbound_budget: Arc<Semaphore>,
    mut outbound: mpsc::UnboundedReceiver<QueuedOutboundFrame>,
    outbound_backpressured: Arc<AtomicBool>,
    wake: Arc<dyn Fn() + Send + Sync>,
) -> NativeTransportTerminal
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let (mut sink, mut source) = ws.split();
    let inbound = InboundSide {
        frames: inbound,
        budget: inbound_budget,
        wake: Arc::clone(&wake),
    };
    let ending = async {
        let mut ticker = liveness.ticker();
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    if let Some(reason) = liveness.check() {
                        return Ending::now(NativeTransportTerminal::Failed(
                            NativeTransportError::Retryable(reason),
                        ));
                    }
                }
                maybe_frame = outbound.recv() => {
                    let Some(first_frame) = maybe_frame else {
                        return Ending::closing(NativeTransportTerminal::OwnerDropped);
                    };
                    let mut batch = vec![first_frame];
                    let mut batch_bytes = POSTCARD_BATCH_LENGTH_RESERVE
                        + batch[0].bytes.len()
                        + POSTCARD_FRAME_LENGTH_RESERVE;
                    while let Ok(frame) = outbound.try_recv() {
                        let frame_bytes = frame.bytes.len() + POSTCARD_FRAME_LENGTH_RESERVE;
                        if batch.len() >= MAX_WIRE_BATCH_FRAMES
                            || batch_bytes.saturating_add(frame_bytes)
                                > WS_CLIENT_OUTBOUND_BATCH_BYTES
                        {
                            let bytes = match encode_outbound_batch(&batch) {
                                Ok(bytes) => bytes,
                                Err(error) => {
                                    return Ending::now(NativeTransportTerminal::Failed(
                                        NativeTransportError::Terminal(format!(
                                            "failed to encode websocket wire batch: {error}"
                                        )),
                                    ));
                                }
                            };
                            if let Err(ending) = send_watched(
                                &mut sink,
                                &mut source,
                                &inbound,
                                Message::Binary(bytes.into()),
                                &mut liveness,
                                &mut ticker,
                            )
                            .await
                            {
                                return ending;
                            }
                            batch.clear();
                            if outbound_backpressured.swap(false, Ordering::AcqRel) {
                                wake();
                            }
                            batch_bytes = POSTCARD_BATCH_LENGTH_RESERVE;
                        }
                        batch_bytes = batch_bytes.saturating_add(frame_bytes);
                        batch.push(frame);
                    }
                    let bytes = match encode_outbound_batch(&batch) {
                        Ok(bytes) => bytes,
                        Err(error) => {
                            return Ending::now(NativeTransportTerminal::Failed(
                                NativeTransportError::Terminal(format!(
                                    "failed to encode websocket wire batch: {error}"
                                )),
                            ));
                        }
                    };
                    if let Err(ending) = send_watched(
                        &mut sink,
                        &mut source,
                        &inbound,
                        Message::Binary(bytes.into()),
                        &mut liveness,
                        &mut ticker,
                    )
                    .await
                    {
                        return ending;
                    }
                    drop(batch);
                    if outbound_backpressured.swap(false, Ordering::AcqRel) {
                        wake();
                    }
                }
                message = source.next() => {
                    if let Err(ending) = inbound.deliver(message).await {
                        return ending;
                    }
                    // Waiting for the application to take frames is not silence.
                    liveness.resume();
                }
            }
        }
    }
    .await;
    if ending.close {
        let _ = tokio::time::timeout(WS_CLIENT_CLOSE_TIMEOUT, sink.close()).await;
    }
    finish_outbound_pump(outbound, &outbound_backpressured, wake.as_ref());
    ending.terminal
}

fn decode_inbound_batch(bytes: &[u8]) -> Result<Vec<Vec<u8>>, String> {
    let frames = jazz::wire::decode_websocket_frame_batch(bytes)
        .map_err(|_| "websocket peer sent malformed wire batch".to_owned())?;
    Ok(frames)
}

#[cfg(test)]
mod tests {
    /// #4248: Core refusing the bearer (an `AuthFailed` error before hello,
    /// or a 401 on upgrade) is an auth rejection a fresh credential can
    /// clear, distinct from other terminal failures and from a retryable
    /// `NotReady`.
    #[test]
    fn a_refused_credential_is_an_auth_rejection() {
        use jazz::wire::{WireError, WireErrorCode, WireRetry};
        let wire = |code, retry| {
            native_transport_error(WebSocketClientError::ServerWireError(WireError::new(
                code,
                retry,
                "token expired",
            )))
        };
        assert!(matches!(
            wire(WireErrorCode::AuthFailed, WireRetry::Never),
            NativeTransportError::AuthRejected(_)
        ));
        assert!(wire(WireErrorCode::NotReady, WireRetry::Later).is_retryable());
        assert!(matches!(
            wire(WireErrorCode::MalformedFrame, WireRetry::Never),
            NativeTransportError::Terminal(_)
        ));
        let http = |status: u16| {
            let response = tokio_tungstenite::tungstenite::http::Response::builder()
                .status(status)
                .body(None)
                .unwrap();
            native_transport_error(WebSocketClientError::Connect(
                tokio_tungstenite::tungstenite::Error::Http(Box::new(response)),
            ))
        };
        assert!(matches!(http(401), NativeTransportError::AuthRejected(_)));
        assert!(matches!(http(403), NativeTransportError::Terminal(_)));
        assert!(http(503).is_retryable());
    }

    // A proxy can accept TCP while its upstream is down, then close before
    // completing HTTP upgrade. Observe the real socket error, not its text.
    #[tokio::test]
    async fn incomplete_websocket_upgrade_is_retryable() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            use tokio::io::AsyncReadExt;
            let mut request = Vec::new();
            while !request.ends_with(b"\r\n\r\n") {
                let mut byte = [0_u8; 1];
                stream.read_exact(&mut byte).await.unwrap();
                request.push(byte[0]);
                assert!(request.len() < 4096, "bounded upgrade request");
            }
            drop(stream);
        });
        let error = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            tokio_tungstenite::connect_async(format!("ws://{address}")),
        )
        .await
        .expect("bounded incomplete-upgrade receipt")
        .expect_err("server never completed HTTP upgrade");
        assert!(
            matches!(
                error,
                tokio_tungstenite::tungstenite::Error::Protocol(
                    tokio_tungstenite::tungstenite::error::ProtocolError::HandshakeIncomplete
                )
            ),
            "unexpected incomplete upgrade: {error:?}"
        );
        let classified = native_transport_error(WebSocketClientError::Connect(error));
        assert!(
            classified.is_retryable(),
            "incomplete upgrade must reconnect: {classified:?}"
        );
        server.await.unwrap();
    }

    // A relay that accepts the upgrade and closes when its core is
    // unreachable sends a real Close frame before any hello.
    #[tokio::test]
    async fn relay_close_before_hello_is_retryable() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
            ws.send(Message::Close(Some(
                tokio_tungstenite::tungstenite::protocol::CloseFrame {
                    code: 1011.into(),
                    reason: "admission failed".into(),
                },
            )))
            .await
            .unwrap();
        });
        let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{address}"))
            .await
            .unwrap();
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
        let error = receive_server_hello(&mut ws, deadline)
            .await
            .expect_err("closed before hello");
        assert!(
            matches!(
                &error,
                WebSocketClientError::ClosedBeforeHello { code: 1011, reason }
                    if reason == "admission failed"
            ),
            "unexpected close: {error:?}"
        );
        assert!(native_transport_error(error).is_retryable());
        server.await.unwrap();
    }

    /// Short timings, so a test sees the deadline in about a second.
    const TEST_LIVENESS: LivenessPolicy = LivenessPolicy {
        silence_deadline: Duration::from_millis(1_000),
        tick: Duration::from_millis(50),
    };

    struct PumpUnderTest {
        pump: tokio::task::JoinHandle<NativeTransportTerminal>,
        outbound: BoundedOutbound,
        inbound: mpsc::Receiver<InboundFrame>,
    }

    /// Runs the pump as a client over `io`, metered the way a real socket is.
    async fn pump_over(io: tokio::io::DuplexStream) -> PumpUnderTest {
        let activity = LinkActivity::new();
        let ws = tokio_tungstenite::WebSocketStream::from_raw_socket(
            Metered::new(io, Arc::clone(&activity)),
            tokio_tungstenite::tungstenite::protocol::Role::Client,
            Some(client_websocket_config()),
        )
        .await;
        let (inbound_tx, inbound) = mpsc::channel(WS_CLIENT_INBOUND_FRAME_SLOTS);
        let (outbound, outbound_rx, backpressured) = BoundedOutbound::channel();
        let pump = tokio::spawn(run_ws_pump(
            ws,
            Liveness::new(activity, TEST_LIVENESS),
            inbound_tx,
            Arc::new(Semaphore::new(WS_CLIENT_MAX_QUEUED_BYTES)),
            outbound_rx,
            backpressured,
            Arc::new(|| {}),
        ));
        PumpUnderTest {
            pump,
            outbound,
            inbound,
        }
    }

    async fn server_over(
        io: tokio::io::DuplexStream,
    ) -> tokio_tungstenite::WebSocketStream<tokio::io::DuplexStream> {
        tokio_tungstenite::WebSocketStream::from_raw_socket(
            io,
            tokio_tungstenite::tungstenite::protocol::Role::Server,
            Some(client_websocket_config()),
        )
        .await
    }

    /// The server's keepalive, at a cadence under the test deadline. Ends,
    /// saying why, if the link breaks or the client sends anything but a Pong:
    /// a client ping is one more message for a server whose writer queue may
    /// be full, and the server breaks the connection when it cannot queue the
    /// Pong.
    async fn keepalive(
        mut ws: tokio_tungstenite::WebSocketStream<tokio::io::DuplexStream>,
    ) -> String {
        loop {
            if let Err(error) = ws.send(Message::Ping(Vec::new().into())).await {
                return format!("keepalive send failed: {error}");
            }
            let until = tokio::time::Instant::now() + Duration::from_millis(300);
            while let Ok(Some(message)) = tokio::time::timeout_at(until, ws.next()).await {
                match message {
                    Ok(Message::Pong(_)) => {}
                    other => return format!("client sent {other:?}"),
                }
            }
        }
    }

    fn assert_given_up_retryably(terminal: NativeTransportTerminal) {
        match terminal {
            NativeTransportTerminal::Failed(error) => assert!(
                error.is_retryable(),
                "a dead link must be retried: {error:?}"
            ),
            other => panic!("a dead link must fail retryably, got {other:?}"),
        }
    }

    // A link that goes silent without closing (Wi-Fi off, a paused server, a
    // half-open TCP path) delivers no error and no EOF: only the absence of
    // traffic says it is gone. The pump must give it up on its own, as a
    // failure the link retries, instead of reporting it connected until the
    // OS or the server's idle timeout (60 s) closes the socket.
    #[tokio::test]
    async fn a_silent_upstream_is_given_up_and_retried() {
        let (client_io, _server_io) = tokio::io::duplex(64 << 10);
        let under_test = pump_over(client_io).await;
        let terminal = tokio::time::timeout(Duration::from_secs(5), under_test.pump)
            .await
            .expect("a silent upstream must end the pump")
            .unwrap();
        assert_given_up_retryably(terminal);
    }

    // The other side of the same rule: the server's keepalive pings are
    // enough to keep an idle link up, and the client sends nothing of its own.
    #[tokio::test]
    async fn an_idle_upstream_that_sends_keepalive_pings_stays_up() {
        let (client_io, server_io) = tokio::io::duplex(64 << 10);
        let server = tokio::spawn(async move { keepalive(server_over(server_io).await).await });
        let under_test = pump_over(client_io).await;
        tokio::time::sleep(TEST_LIVENESS.silence_deadline * 3).await;
        assert!(
            !under_test.pump.is_finished(),
            "an idle upstream with a keepalive was given up: {:?}",
            under_test.pump.await
        );
        assert!(!server.is_finished(), "{:?}", server.await);
        under_test.pump.abort();
        server.abort();
    }

    // A message counts once it is complete, but the link is alive while its
    // bytes are arriving: a catalogue of a few MiB over a slow mobile link
    // takes longer than the deadline and must not be cut off mid-message.
    #[tokio::test]
    async fn a_large_message_arriving_slowly_is_not_silence() {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
        const CHUNK: usize = 32 << 10;
        const CHUNK_EVERY: Duration = Duration::from_millis(100);
        let frame = vec![7_u8; 1 << 20];
        let message = postcard::to_allocvec(&vec![frame.clone()]).unwrap();
        // About 3.2 s on the wire, three times the deadline.
        assert!(
            Duration::from_millis(100) * u32::try_from(message.len() / CHUNK).unwrap()
                > TEST_LIVENESS.silence_deadline * 3
        );
        let (server_io, mut slow_in) = tokio::io::duplex(4 << 20);
        let (mut slow_out, client_io) = tokio::io::duplex(CHUNK);
        let throttle = tokio::spawn(async move {
            let mut chunk = vec![0_u8; CHUNK];
            loop {
                let read = match slow_in.read(&mut chunk).await {
                    Ok(0) | Err(_) => return,
                    Ok(read) => read,
                };
                if slow_out.write_all(&chunk[..read]).await.is_err() {
                    return;
                }
                tokio::time::sleep(CHUNK_EVERY).await;
            }
        });
        let server = tokio::spawn(async move {
            let mut ws = server_over(server_io).await;
            ws.send(Message::Binary(message.into())).await.unwrap();
            // Held open, silent, once the message is out.
            std::future::pending::<()>().await;
        });
        let mut under_test = pump_over(client_io).await;
        let received = tokio::time::timeout(Duration::from_secs(10), under_test.inbound.recv())
            .await
            .expect("the slow message must arrive")
            .unwrap_or_else(|| panic!("pump ended mid-message: {:?}", under_test.pump));
        assert_eq!(received.bytes, frame);
        under_test.pump.abort();
        server.abort();
        throttle.abort();
    }

    // A runtime that could not run the pump (a blocked executor, an app the OS
    // suspended) has not heard the peer, but that is not the peer's silence:
    // the link must not be given up the moment the pump runs again.
    #[tokio::test]
    async fn a_stalled_runtime_is_not_silence() {
        let (client_io, server_io) = tokio::io::duplex(64 << 10);
        // Same single-threaded runtime as the pump, so it stalls too.
        let server = tokio::spawn(async move { keepalive(server_over(server_io).await).await });
        let under_test = pump_over(client_io).await;
        tokio::time::sleep(Duration::from_millis(200)).await;
        std::thread::sleep(TEST_LIVENESS.silence_deadline * 2);
        tokio::time::sleep(TEST_LIVENESS.silence_deadline / 2).await;
        assert!(
            !under_test.pump.is_finished(),
            "a live link was given up after the runtime stalled: {:?}",
            under_test.pump.await
        );
        under_test.pump.abort();
        server.abort();
    }

    // A send to a peer that has gone stays pending once the kernel buffer is
    // full, with no error: the deadline must cover the pump while it sends.
    #[tokio::test]
    async fn a_send_that_makes_no_progress_is_given_up() {
        let (client_io, _server_io) = tokio::io::duplex(16 << 10);
        let under_test = pump_over(client_io).await;
        under_test.outbound.send(vec![1_u8; 512 << 10]).unwrap();
        let terminal = tokio::time::timeout(Duration::from_secs(5), under_test.pump)
            .await
            .expect("a send without progress must end the pump")
            .unwrap();
        assert_given_up_retryably(terminal);
    }

    // The kernel takes small writes into a send buffer that has room whether
    // the peer is there or not, so an application that keeps writing (a
    // presence beat) must not keep a dead link up.
    #[tokio::test]
    async fn writes_into_a_free_send_buffer_do_not_keep_a_dead_link_up() {
        let (client_io, _server_io) = tokio::io::duplex(1 << 20);
        let under_test = pump_over(client_io).await;
        let beat = {
            let outbound = under_test.outbound;
            tokio::spawn(async move {
                loop {
                    if outbound.send(vec![3_u8; 100]).is_err() {
                        return;
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            })
        };
        let terminal = tokio::time::timeout(Duration::from_secs(5), under_test.pump)
            .await
            .expect("a dead link must be given up while the application writes")
            .unwrap();
        assert_given_up_retryably(terminal);
        beat.abort();
    }

    // A server busy with a backlog stops reading the socket, so a large send
    // waits on it for as long as it is busy, but it keeps pinging: the link is
    // alive, and the pump must read while it sends to hear it.
    #[tokio::test]
    async fn a_busy_server_that_keeps_pinging_keeps_the_link_up_during_a_send() {
        let (client_io, server_io) = tokio::io::duplex(16 << 10);
        let server = tokio::spawn(async move {
            let mut ws = server_over(server_io).await;
            // Pings only; the socket is never read.
            loop {
                if ws.send(Message::Ping(Vec::new().into())).await.is_err() {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(300)).await;
            }
        });
        let under_test = pump_over(client_io).await;
        under_test.outbound.send(vec![1_u8; 512 << 10]).unwrap();
        tokio::time::sleep(TEST_LIVENESS.silence_deadline * 3).await;
        assert!(
            !under_test.pump.is_finished(),
            "a live, busy server was given up mid-send: {:?}",
            under_test.pump.await
        );
        under_test.pump.abort();
        server.abort();
    }

    // Excusing the time the pump could not run must not excuse the silence
    // before it: a dead link on a runtime that keeps stalling is still given
    // up.
    #[tokio::test]
    async fn repeated_stalls_do_not_keep_a_dead_link_up() {
        let (client_io, _server_io) = tokio::io::duplex(64 << 10);
        let under_test = pump_over(client_io).await;
        let staller = tokio::spawn(async {
            loop {
                tokio::time::sleep(Duration::from_millis(150)).await;
                std::thread::sleep(TEST_LIVENESS.tick * 3);
            }
        });
        let terminal = tokio::time::timeout(Duration::from_secs(10), under_test.pump)
            .await
            .expect("a dead link must be given up on a stalling runtime")
            .unwrap();
        assert_given_up_retryably(terminal);
        staller.abort();
    }

    // A slow upload is progress, as a slow download is: the peer takes the
    // bytes, so it is there, however long the whole message takes.
    #[tokio::test]
    async fn a_large_message_sent_slowly_is_not_silence() {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
        const CHUNK: usize = 32 << 10;
        let (client_io, mut slow_in) = tokio::io::duplex(CHUNK);
        let (mut slow_out, server_io) = tokio::io::duplex(4 << 20);
        let throttle = tokio::spawn(async move {
            let mut chunk = vec![0_u8; CHUNK];
            loop {
                let read = match slow_in.read(&mut chunk).await {
                    Ok(0) | Err(_) => return,
                    Ok(read) => read,
                };
                if slow_out.write_all(&chunk[..read]).await.is_err() {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        });
        let server = tokio::spawn(async move {
            let mut ws = server_over(server_io).await;
            ws.next()
                .await
                .map(|message| message.map(|message| message.len()))
        });
        let under_test = pump_over(client_io).await;
        // About 3 s through the throttle.
        under_test.outbound.send(vec![1_u8; 960 << 10]).unwrap();
        let received = tokio::time::timeout(Duration::from_secs(10), server)
            .await
            .expect("the slow upload must arrive")
            .unwrap();
        assert!(
            matches!(received, Some(Ok(len)) if len > 960 << 10),
            "upload did not arrive whole: {received:?}"
        );
        assert!(
            !under_test.pump.is_finished(),
            "the link was given up during a slow upload: {:?}",
            under_test.pump.await
        );
        under_test.pump.abort();
        throttle.abort();
    }

    // This reproduces the socket library's typed EOF result below the Db API;
    // constructing an error alone would miss the actual abrupt-close category.
    #[tokio::test]
    async fn established_socket_abrupt_eof_is_retryable() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let socket = tokio_tungstenite::accept_async(stream).await.unwrap();
            // Drop TCP without sending a WebSocket Close frame, as a proxy
            // restart or disappearing mobile connection can do.
            drop(socket);
        });
        let (mut socket, _) = tokio_tungstenite::connect_async(format!("ws://{address}"))
            .await
            .unwrap();
        let error = tokio::time::timeout(std::time::Duration::from_secs(5), socket.next())
            .await
            .expect("bounded EOF receipt")
            .expect("typed EOF error")
            .expect_err("unclean close must produce a socket error");
        assert!(
            matches!(
            error,
            tokio_tungstenite::tungstenite::Error::Protocol(
                tokio_tungstenite::tungstenite::error::ProtocolError::ResetWithoutClosingHandshake
            )
        ),
            "unexpected abrupt EOF: {error:?}"
        );
        let classified = native_transport_error(WebSocketClientError::Receive(error));
        assert!(
            classified.is_retryable(),
            "abrupt EOF must reconnect: {classified:?}"
        );
        server.await.unwrap();
    }

    // Logical sync results cannot detect this socket policy. Check the real
    // connected client socket directly instead of a timing threshold.
    #[tokio::test]
    async fn client_websocket_disables_nagle() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            tokio_tungstenite::accept_async(stream).await.unwrap()
        });
        let ws = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            open_client_websocket(format!("ws://{address}"), None, LinkActivity::new()),
        )
        .await
        .expect("bounded client connect")
        .expect("client websocket connects")
        .0;
        match ws.get_ref() {
            tokio_tungstenite::MaybeTlsStream::Plain(stream) => {
                assert!(
                    stream.get_ref().nodelay().unwrap(),
                    "client socket keeps Nagle on"
                );
            }
            _ => panic!("ws:// URL must open a plain TCP stream"),
        }
        drop(server.await.unwrap());
    }

    // Keep the exception typed and narrow: other protocol failures must still
    // retire the authenticated connection instead of retrying bad input.
    #[test]
    fn malformed_websocket_protocol_remains_terminal() {
        use tokio_tungstenite::tungstenite::{Error, error::ProtocolError};
        assert!(
            !native_transport_error(WebSocketClientError::Connect(Error::Protocol(
                ProtocolError::WrongHttpMethod,
            )))
            .is_retryable()
        );
        for error in [
            ProtocolError::UnmaskedFrameFromClient,
            ProtocolError::InvalidOpcode(3),
        ] {
            assert!(
                !native_transport_error(WebSocketClientError::Receive(Error::Protocol(error)))
                    .is_retryable()
            );
        }
        assert!(
            !native_transport_error(WebSocketClientError::Connect(Error::Protocol(
                ProtocolError::ResetWithoutClosingHandshake,
            )))
            .is_retryable(),
            "the exception applies only after socket establishment"
        );
    }

    // This adapter classification has no Db-level API: assert the typed socket
    // and wire boundary, including conflicting error guidance, directly.
    #[test]
    fn native_connection_failures_preserve_retry_and_auth_categories() {
        use jazz::wire::{WireErrorCode, WireRetry};
        let refused = WebSocketClientError::Connect(tokio_tungstenite::tungstenite::Error::Io(
            std::io::ErrorKind::ConnectionRefused.into(),
        ));
        assert!(native_transport_error(refused).is_retryable());
        assert!(native_transport_error(WebSocketClientError::HandshakeTimeout).is_retryable());
        let retry_statuses = [
            (408, true),
            (425, true),
            (429, true),
            (500, true),
            (502, true),
            (503, true),
            (504, true),
            (401, false),
            (403, false),
            (404, false),
            (501, false),
            (505, false),
            (511, false),
        ];
        let mismatches: Vec<_> = retry_statuses
            .into_iter()
            .filter_map(|(status, expected)| {
                let response = tokio_tungstenite::tungstenite::http::Response::builder()
                    .status(status)
                    .body(None::<Vec<u8>>)
                    .unwrap();
                let actual = native_transport_error(WebSocketClientError::Connect(
                    tokio_tungstenite::tungstenite::Error::Http(Box::new(response)),
                ))
                .is_retryable();
                (actual != expected).then_some((status, actual, expected))
            })
            .collect();
        assert!(
            mismatches.is_empty(),
            "unexpected HTTP retry classification (status, actual, expected): {mismatches:?}"
        );
        for (code, retry, expected) in [
            (WireErrorCode::NotReady, WireRetry::Later, true),
            (WireErrorCode::NotReady, WireRetry::AfterAuth, false),
            (WireErrorCode::AuthFailed, WireRetry::AfterAuth, false),
            (WireErrorCode::AuthFailed, WireRetry::Later, false),
            (WireErrorCode::MalformedFrame, WireRetry::Later, false),
        ] {
            assert_eq!(
                native_transport_error(WebSocketClientError::ServerWireError(WireError::new(
                    code,
                    retry,
                    "connection refused: diagnostic is not authority"
                ),))
                .is_retryable(),
                expected
            );
        }
        assert!(
            !native_transport_error(WebSocketClientError::ServerRejected(
                "connection refused".to_owned(),
            ))
            .is_retryable()
        );
        assert!(
            !native_transport_error(WebSocketClientError::UnexpectedHandshakeMessage)
                .is_retryable()
        );
    }

    #[test]
    fn close_before_hello_retries_only_transient_close_codes() {
        let closed = |code: u16| {
            native_transport_error(WebSocketClientError::ClosedBeforeHello {
                code,
                reason: "core unavailable".to_owned(),
            })
        };
        for transient in [1001, 1011, 1012, 1013, 1014] {
            assert!(closed(transient).is_retryable(), "close {transient}");
        }
        for terminal in [1000, 1002, 1003, 1005, 1007, 1008, 1009, 1010, 4000] {
            assert!(!closed(terminal).is_retryable(), "close {terminal}");
        }
    }

    #[test]
    fn broken_pipe_on_established_socket_is_retryable() {
        use tokio_tungstenite::tungstenite::Error;

        for error in [
            WebSocketClientError::Send(Error::Io(std::io::ErrorKind::BrokenPipe.into())),
            WebSocketClientError::Receive(Error::Io(std::io::ErrorKind::BrokenPipe.into())),
        ] {
            assert!(
                native_transport_error(error).is_retryable(),
                "a broken peer pipe must not poison foreground ticks"
            );
        }
    }

    // The resolver outcome, not its platform-specific I/O kind, identifies
    // name lookup failure. This boundary cannot be injected through Db APIs.
    #[test]
    fn native_dns_resolution_failures_are_retryable() {
        let lookup = NativeConnectFailure::Resolution(std::io::Error::other("name lookup failed"));
        assert!(
            native_connect_error(lookup).is_retryable(),
            "a failed hostname lookup must reconnect"
        );

        let unrelated = NativeConnectFailure::WebSocket(WebSocketClientError::Connect(
            tokio_tungstenite::tungstenite::Error::Io(std::io::ErrorKind::Other.into()),
        ));
        assert!(
            !native_connect_error(unrelated).is_retryable(),
            "an unclassified connect I/O failure must remain terminal"
        );
    }

    // rustls uses UnexpectedEof when a peer closes TLS without close_notify.
    #[test]
    fn rustls_tls_eof_is_retryable() {
        use tokio_tungstenite::tungstenite::Error;

        let eof =
            WebSocketClientError::Receive(Error::Io(std::io::ErrorKind::UnexpectedEof.into()));
        assert!(
            native_transport_error(eof).is_retryable(),
            "TLS EOF after connection establishment must reconnect"
        );

        let unrelated = WebSocketClientError::Connect(Error::Io(std::io::ErrorKind::Other.into()));
        assert!(
            !native_transport_error(unrelated).is_retryable(),
            "other unclassified connection errors must remain terminal"
        );
    }

    // The exact nested error comes from tokio-rustls before a Jazz handshake,
    // so this adapter classification must be tested below the Db API.
    #[test]
    fn rustls_certificate_io_failures_remain_terminal() {
        let certificate =
            rustls::Error::InvalidCertificate(rustls::CertificateError::UnknownIssuer);
        let error = std::io::Error::new(std::io::ErrorKind::InvalidData, certificate);
        assert!(
            error
                .get_ref()
                .unwrap()
                .downcast_ref::<rustls::Error>()
                .is_some()
        );
        assert!(
            !native_transport_error(WebSocketClientError::Connect(
                tokio_tungstenite::tungstenite::Error::Io(error)
            ))
            .is_retryable()
        );
        for kind in [
            std::io::ErrorKind::InvalidData,
            std::io::ErrorKind::PermissionDenied,
            std::io::ErrorKind::Other,
        ] {
            assert!(
                !native_transport_error(WebSocketClientError::Receive(
                    tokio_tungstenite::tungstenite::Error::Io(std::io::Error::new(
                        kind,
                        "connection refused"
                    ))
                ))
                .is_retryable()
            );
        }
        for kind in [
            std::io::ErrorKind::TimedOut,
            std::io::ErrorKind::ConnectionReset,
            std::io::ErrorKind::NetworkUnreachable,
        ] {
            assert!(
                native_transport_error(WebSocketClientError::Send(
                    tokio_tungstenite::tungstenite::Error::Io(kind.into())
                ))
                .is_retryable()
            );
        }
    }

    use super::*;
    use jazz::wire::WIRE_PROTOCOL_VERSION;

    /// Frames, each within the physical cap, that together exceed the
    /// ingress budget: an initial sync can arrive like this.
    fn frames_larger_than_ingress_budget() -> Vec<Vec<u8>> {
        let frame = MAX_WIRE_FRAME_BYTES / 2;
        let count = WS_CLIENT_MAX_QUEUED_BYTES / frame + 2;
        (0..count)
            .map(|index| vec![(index % 251) as u8; frame])
            .collect()
    }

    #[test]
    fn bootstrap_ingress_budget_is_finite() {
        let budget = Arc::new(Semaphore::new(WS_CLIENT_MAX_QUEUED_BYTES));
        let held = Arc::clone(&budget)
            .try_acquire_many_owned(WS_CLIENT_MAX_QUEUED_BYTES as u32)
            .expect("fill finite ingress byte budget");
        assert!(
            budget.try_acquire().is_err(),
            "cap+1 byte is backpressured rather than allocated"
        );
        drop(held);
    }

    #[test]
    fn websocket_rejects_oversized_physical_messages_before_batch_decode() {
        let config = client_websocket_config();
        assert_eq!(config.max_message_size, Some(MAX_WIRE_FRAME_BYTES));
        assert_eq!(config.max_frame_size, Some(MAX_WIRE_FRAME_BYTES));
    }

    #[test]
    fn outbound_batch_never_emits_a_carrier_over_the_physical_cap() {
        let queued = |bytes| QueuedOutboundFrame {
            bytes,
            charge: 0,
            queued_bytes: Arc::new(AtomicUsize::new(0)),
        };
        let largest_singleton = queued(vec![0x42; MAX_WIRE_FRAME_BYTES - 4]);
        let encoded = encode_outbound_batch(std::slice::from_ref(&largest_singleton))
            .expect("largest singleton carrier fits exactly");
        assert_eq!(encoded.len(), MAX_WIRE_FRAME_BYTES);
        assert_eq!(
            jazz::wire::decode_websocket_frame_batch(&encoded).expect("decode exact carrier"),
            vec![largest_singleton.bytes.clone()],
        );

        let raw_limit = queued(vec![0x42; MAX_WIRE_FRAME_BYTES]);
        assert!(
            encode_outbound_batch(std::slice::from_ref(&raw_limit)).is_err(),
            "raw limit frame alone cannot fit its carrier prefixes"
        );
        let small = queued(vec![0x11]);
        assert!(
            encode_outbound_batch(&[small, raw_limit]).is_err(),
            "flushing an earlier frame must not permit an invalid carrier"
        );
    }

    #[tokio::test]
    async fn a_burst_larger_than_the_ingress_budget_streams_with_backpressure() {
        let (sender, mut receiver) = mpsc::channel::<InboundFrame>(1);
        let budget = Arc::new(Semaphore::new(WS_CLIENT_MAX_QUEUED_BYTES));
        let consumer = tokio::spawn(async move {
            let mut received = 0_usize;
            while let Some(frame) = receiver.recv().await {
                received += frame.bytes.len();
            }
            received
        });
        let frames = frames_larger_than_ingress_budget();
        let expected = frames.iter().map(Vec::len).sum::<usize>();
        for frame in frames {
            let budget_permit = Arc::clone(&budget)
                .acquire_many_owned(frame.len() as u32)
                .await
                .expect("consumer makes progress under ingress backpressure");
            sender
                .send(InboundFrame {
                    bytes: frame,
                    _budget: budget_permit,
                })
                .await
                .expect("stream frame");
        }
        drop(sender);
        assert_eq!(consumer.await.expect("consumer task"), expected);
    }

    #[test]
    fn transport_count_flood_or_empty_batch_is_rejected_before_ingress_staging() {
        let empty = postcard::to_allocvec(&Vec::<Vec<u8>>::new()).expect("encode empty batch");
        assert!(
            decode_inbound_batch(&empty).is_err(),
            "transport must reject empty batches"
        );

        let flood = postcard::to_allocvec(&vec![Vec::<u8>::new(); MAX_WIRE_BATCH_FRAMES + 1])
            .expect("encode count flood below physical byte cap");
        assert!(flood.len() <= MAX_WIRE_FRAME_BYTES);
        assert!(
            decode_inbound_batch(&flood).is_err(),
            "count flood must be rejected before channel staging"
        );
    }

    #[test]
    fn inbound_batch_decoder_consumes_the_complete_carrier() {
        let valid = [0x01, 0x01, 0x42];
        assert_eq!(
            postcard::to_allocvec(&vec![vec![0x42_u8]]).expect("encode valid batch"),
            valid,
            "frozen WebSocket batch corpus must remain canonical"
        );
        assert_eq!(
            decode_inbound_batch(&valid).expect("decode complete valid batch"),
            vec![vec![0x42]]
        );

        let mut suffixed = valid.to_vec();
        suffixed.push(0x00);
        assert!(
            decode_inbound_batch(&suffixed).is_err(),
            "a valid batch plus a suffix must not acquire a second interpretation"
        );
    }

    // This is an internal test because a JSON prelude is emitted before a Db
    // exists. The shared fixture pins the native writer's exact first-message
    // contract for the server and TypeScript carrier tests.
    #[test]
    fn native_writer_matches_websocket_prelude_v1_fixture() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../jazz/fixtures/websocket_prelude_v1.json",
        ))
        .expect("parse websocket prelude fixture");
        for entry in fixture["fixtures"]
            .as_array()
            .expect("fixture entries")
            .iter()
            .filter(|entry| entry["writer"] == "rust-native")
        {
            let identity = AuthorSubject::from_canonical(
                entry["peer_identity"]
                    .as_str()
                    .expect("fixture peer identity"),
            )
            .expect("fixture canonical peer identity");
            let auth: AuthConfig =
                serde_json::from_value(entry["auth"].clone()).expect("fixture native auth");
            let actual = String::from_utf8(
                encode_prelude(
                    identity,
                    auth,
                    None,
                    if entry["requested_link"] == "scope_isolated_client_relay" {
                        NativeTransportLink::ScopeIsolatedClientRelay
                    } else {
                        NativeTransportLink::OrdinarySession
                    },
                )
                .expect("encode fixture prelude"),
            )
            .expect("native prelude is UTF-8 JSON");
            assert_eq!(
                actual,
                entry["json"].as_str().expect("fixture JSON"),
                "native writer drifted for {}",
                entry["name"].as_str().expect("fixture name")
            );
        }
    }

    // The connector handshake itself is the public observable contract here;
    // a synthetic upstream lets us withhold exactly one negotiated feature.
    #[tokio::test]
    async fn scope_link_requires_negotiated_support_and_never_grants_authority_delegation() {
        for supported in [true, false] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let server = tokio::spawn(async move {
                let (stream, _) = listener.accept().await.unwrap();
                let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
                let prelude = socket.next().await.unwrap().unwrap().into_data();
                let prelude: serde_json::Value = serde_json::from_slice(&prelude).unwrap();
                assert_eq!(prelude["requested_link"], "scope_isolated_client_relay");
                let _client_hello = socket.next().await.unwrap().unwrap();
                let mut features = current_wire_features();
                if !supported {
                    features &= !jazz::wire::FEATURE_SCOPE_ISOLATED_CLIENT_RELAY;
                }
                let hello = encode_frame(&WireFrame::Hello(WireHello::current(
                    WirePeerRole::Core,
                    features,
                )))
                .unwrap();
                socket
                    .send(Message::Binary(
                        postcard::to_allocvec(&vec![hello]).unwrap().into(),
                    ))
                    .await
                    .unwrap();
            });
            let connected = NativeWebSocketConnector
                .connect(NativeTransportRequest {
                    requested_link: NativeTransportLink::ScopeIsolatedClientRelay,
                    server_url: format!("http://{address}"),
                    app_id: AppId::random(),
                    peer_identity: AuthorSubject::SYSTEM,
                    auth: AuthConfig::default(),
                    authorization: None,
                    bundle: None,
                    wake: Arc::new(|| {}),
                })
                .await;
            if supported {
                connected.expect("supported scoped handshake");
            } else {
                let error = match connected {
                    Ok(_) => panic!("missing scoped feature must fail closed"),
                    Err(error) => error,
                };
                assert!(!error.is_retryable());
                assert!(error.to_string().contains("scope_isolated_client_relay"));
            }
            server.await.unwrap();
        }
    }

    /// Alice can connect to Bob with no optional features: semantic traffic,
    /// structured errors, and channel splitting are baseline wire behavior.
    #[tokio::test]
    async fn ordinary_connection_accepts_no_optional_features() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
            let _prelude = socket.next().await.unwrap().unwrap();
            let _hello = socket.next().await.unwrap().unwrap();
            let hello =
                encode_frame(&WireFrame::Hello(WireHello::current(WirePeerRole::Core, 0))).unwrap();
            socket
                .send(Message::Binary(
                    postcard::to_allocvec(&vec![hello]).unwrap().into(),
                ))
                .await
                .unwrap();
        });
        let connected = WebSocketTransport::connect(
            format!("http://{address}"),
            AppId::random(),
            AuthorSubject::for_test_bytes([0x42; 16]),
            AuthConfig::default(),
        )
        .await
        .expect("baseline protocol requires no feature bits");
        assert_eq!(connected.features, 0);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn outbound_queue_returns_backpressure_at_a_finite_byte_budget() {
        let (_inbound_sender, inbound) = mpsc::channel(1);
        let (outbound, mut outbound_receiver, outbound_backpressured) = BoundedOutbound::channel();
        let task = tokio::spawn(std::future::pending());
        let mut transport = WebSocketTransport {
            inbound: Arc::new(Mutex::new(inbound)),
            outbound,
            task,
            terminal: None,
            terminal_publisher: Arc::new(Mutex::new(None)),
            protocol_version: WIRE_PROTOCOL_VERSION,
            features: 0,
        };
        let frame = vec![0; MAX_WIRE_FRAME_BYTES];
        for _ in 0..(WS_CLIENT_MAX_OUTBOUND_QUEUED_BYTES / MAX_WIRE_FRAME_BYTES) {
            transport
                .send_frame(frame.clone())
                .expect("frames within the queue budget are accepted");
        }

        assert!(matches!(
            transport.send_frame(frame.clone()),
            Err(TransportError::Backpressure)
        ));
        assert!(outbound_backpressured.load(Ordering::Acquire));

        drop(
            outbound_receiver
                .recv()
                .await
                .expect("queued frame retains its byte reservation"),
        );
        transport
            .send_frame(frame)
            .expect("releasing a queued frame restores capacity");
    }

    #[test]
    fn terminal_outbound_failure_wakes_backpressured_producer_to_observe_closed_pump() {
        let (outbound, receiver, backpressured) = BoundedOutbound::channel();
        let wake_count = Arc::new(AtomicUsize::new(0));
        let retry_saw_closed_pump = Arc::new(AtomicBool::new(false));
        let wake: Arc<dyn Fn() + Send + Sync> = {
            let wake_count = Arc::clone(&wake_count);
            let retry_saw_closed_pump = Arc::clone(&retry_saw_closed_pump);
            let retry_outbound = BoundedOutbound {
                sender: outbound.sender.clone(),
                queued_bytes: Arc::clone(&outbound.queued_bytes),
                backpressured: Arc::clone(&outbound.backpressured),
            };
            Arc::new(move || {
                wake_count.fetch_add(1, Ordering::AcqRel);
                retry_saw_closed_pump.store(
                    matches!(
                        retry_outbound.send(vec![1]),
                        Err(TransportError::Failed(message)) if message == "websocket pump is closed"
                    ),
                    Ordering::Release,
                );
            })
        };

        let frame = vec![0; MAX_WIRE_FRAME_BYTES];
        for _ in 0..(WS_CLIENT_MAX_OUTBOUND_QUEUED_BYTES / MAX_WIRE_FRAME_BYTES) {
            outbound.send(frame.clone()).expect("fill outbound budget");
        }
        assert!(matches!(
            outbound.send(frame),
            Err(TransportError::Backpressure)
        ));

        // A failed websocket send ends the pump. The finish path drops the
        // receiver before waking the producer that saw Backpressure.
        finish_outbound_pump(receiver, &backpressured, wake.as_ref());

        assert_eq!(wake_count.load(Ordering::Acquire), 1);
        assert!(retry_saw_closed_pump.load(Ordering::Acquire));
        assert!(matches!(
            outbound.send(vec![1]),
            Err(TransportError::Failed(message)) if message == "websocket pump is closed"
        ));
    }

    #[test]
    fn draining_between_backpressure_reservation_and_arm_is_rechecked() {
        let (outbound, mut receiver, _backpressured) = BoundedOutbound::channel();
        let frame = vec![0; MAX_WIRE_FRAME_BYTES];
        for _ in 0..(WS_CLIENT_MAX_OUTBOUND_QUEUED_BYTES / MAX_WIRE_FRAME_BYTES) {
            outbound.send(frame.clone()).expect("fill outbound budget");
        }

        // Deterministically model the pump releasing one frame in the former
        // reservation-failure -> arm race window. The recheck accepts it
        // instead of returning an unwakeable Backpressure result.
        outbound
            .send_after_backpressure_arm(frame, || {
                drop(
                    receiver
                        .try_recv()
                        .expect("pump drains a queued frame after producer arms"),
                );
            })
            .expect("recheck observes capacity released after arming");
    }
}
