use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::{SinkExt as _, StreamExt as _};
use jazz::db::{ConnectionSessionContext, WireTransportAdapter};
use jazz::ids::{AuthorSubject, NodeUuid};
use jazz::protocol_limits::{MAX_WIRE_BATCH_FRAMES, MAX_WIRE_FRAME_BYTES, validate_wire_frame_len};
use jazz::wire::{
    FEATURE_SYNC_MESSAGE_PAYLOAD, TransportError, WIRE_PROTOCOL_VERSION, WireAuthorityEndpoint,
    WireError, WireFrame, WireHello, WirePeerRole, WireTransport, current_wire_features,
    decode_frame, encode_frame, negotiate_wire,
};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use tokio::sync::{Notify, Semaphore, mpsc};
use tokio_tungstenite::connect_async_with_config;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;

use jazz::tools::AppId;
use jazz::tools::native_transport_connector::{
    ConnectedNativeTransport, NativeCatalogueBootstrapFuture, NativeTransportConnector,
    NativeTransportFuture, NativeTransportRequest,
};
use jazz::tools::websocket_prelude_auth::AuthConfig;

const WS_CLIENT_REQUIRED_FEATURES: u64 = FEATURE_SYNC_MESSAGE_PAYLOAD;
const WS_CLIENT_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);
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
static NEXT_CLIENT_CONNECTION_EPOCH: AtomicU64 = AtomicU64::new(1);

#[derive(Debug)]
pub enum WebSocketClientError {
    Connect(tokio_tungstenite::tungstenite::Error),
    Send(tokio_tungstenite::tungstenite::Error),
    Receive(tokio_tungstenite::tungstenite::Error),
    ClosedDuringHandshake,
    HandshakeTimeout,
    UnexpectedHandshakeMessage,
    EncodePrelude(serde_json::Error),
    EncodeHello(postcard::Error),
    DecodeBatch(postcard::Error),
    DecodeFrame(postcard::Error),
    Negotiation(WireError),
    ServerRejected(String),
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
        }
    }
}

impl std::error::Error for WebSocketClientError {}

pub struct WebSocketTransport {
    inbound: Arc<Mutex<mpsc::Receiver<InboundFrame>>>,
    inbound_error: Arc<Mutex<Option<String>>>,
    inbound_notify: Arc<Notify>,
    outbound: BoundedOutbound,
    task: tokio::task::JoinHandle<()>,
    protocol_version: u16,
    features: u64,
    session_context: Option<ConnectionSessionContext>,
}

/// Tokio/TLS WebSocket implementation selected by native process and binding
/// shells.  Core code receives this through `NativeTransportConnector` and
/// never names this adapter crate.
#[derive(Clone, Copy, Debug, Default)]
pub struct NativeWebSocketConnector;

impl NativeTransportConnector for NativeWebSocketConnector {
    fn validate_catalogue_bootstrap_url(
        &self,
        server_url: &str,
        app_id: AppId,
    ) -> Result<(), jazz::tools::native_transport_connector::NativeTransportError> {
        validate_catalogue_bootstrap_upstream_url(server_url, app_id)
            .map_err(jazz::tools::native_transport_connector::NativeTransportError)
    }

    fn connect(&self, request: NativeTransportRequest) -> NativeTransportFuture {
        Box::pin(async move {
            let transport = WebSocketTransport::connect_with_wake(
                request.server_url,
                request.app_id,
                request.peer_identity,
                request.auth,
                request.wake,
            )
            .await
            .map_err(|error| {
                jazz::tools::native_transport_connector::NativeTransportError(error.to_string())
            })?;
            let (protocol_version, features, session_context) =
                transport.negotiated_transport_metadata();
            Ok(ConnectedNativeTransport {
                transport: Box::new(transport),
                protocol_version,
                features,
                session_context,
            })
        })
    }

    fn bootstrap_catalogue(
        &self,
        request: NativeTransportRequest,
    ) -> NativeCatalogueBootstrapFuture {
        Box::pin(async move {
            WebSocketTransport::connect_catalogue_bootstrap(
                request.server_url,
                request.app_id,
                request.peer_identity,
                request.auth,
            )
            .await
            .map_err(|error| {
                jazz::tools::native_transport_connector::NativeTransportError(error.to_string())
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
        let charge = bytes.len().max(1);
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
        Self::connect_with_wake_and_bootstrap(base_url, app_id, peer_identity, auth, wake, false)
            .await
    }

    /// Open the authenticated snapshot-only bootstrap exchange.  The returned
    /// transport is deliberately short-lived: after adoption the edge opens a
    /// fresh ordinary peer connection against the now-ready runtime.
    pub async fn connect_catalogue_bootstrap(
        base_url: impl AsRef<str>,
        app_id: AppId,
        peer_identity: AuthorSubject,
        auth: AuthConfig,
    ) -> Result<jazz::protocol::CatalogueSnapshot, WebSocketClientError> {
        validate_catalogue_bootstrap_upstream_url(base_url.as_ref(), app_id)
            .map_err(WebSocketClientError::ServerRejected)?;
        let transport = Self::connect_with_wake_and_bootstrap(
            base_url,
            app_id,
            peer_identity,
            auth,
            Arc::new(|| {}),
            true,
        )
        .await?;
        let (protocol_version, features, session_context) =
            transport.negotiated_transport_metadata();
        let inbound_error = Arc::clone(&transport.inbound_error);
        let inbound_notify = Arc::clone(&transport.inbound_notify);
        let mut wire = WireTransportAdapter::new_with_session_context(
            transport,
            protocol_version,
            features,
            None,
            session_context,
        );
        let deadline = tokio::time::Instant::now() + WS_CLIENT_HANDSHAKE_TIMEOUT;
        loop {
            let notified = inbound_notify.notified();
            match wire.try_recv_strict() {
                Ok(Some(message)) => {
                    return match message {
                        jazz::protocol::SyncMessage::CatalogueSnapshot(snapshot) => Ok(*snapshot),
                        _ => Err(WebSocketClientError::ServerRejected(
                            "bootstrap peer sent application traffic instead of a catalogue snapshot"
                                .to_owned(),
                        )),
                    };
                }
                Ok(None) => {}
                Err(error) => {
                    return Err(WebSocketClientError::ServerRejected(format!(
                        "bootstrap wire validation failed: {:?}: {}",
                        error.code, error.message
                    )));
                }
            }
            if let Some(error) = inbound_error.lock().ok().and_then(|error| error.clone()) {
                return Err(WebSocketClientError::ServerRejected(error));
            }
            tokio::select! {
                _ = notified => {}
                _ = tokio::time::sleep_until(deadline) => {
                    return Err(WebSocketClientError::HandshakeTimeout);
                }
            }
        }
    }

    async fn connect_with_wake_and_bootstrap(
        base_url: impl AsRef<str>,
        app_id: AppId,
        peer_identity: AuthorSubject,
        auth: AuthConfig,
        wake: Arc<dyn Fn() + Send + Sync>,
        bootstrap_catalogue: bool,
    ) -> Result<Self, WebSocketClientError> {
        let url = ws_url(base_url.as_ref(), app_id);
        let (mut ws, _) = connect_async_with_config(url, Some(client_websocket_config()), false)
            .await
            .map_err(WebSocketClientError::Connect)?;

        let prelude = encode_prelude(peer_identity, auth, bootstrap_catalogue)?;
        ws.send(Message::Binary(prelude.into()))
            .await
            .map_err(WebSocketClientError::Send)?;

        let client_endpoint = WireAuthorityEndpoint {
            // The server authenticates the session subject separately. This
            // endpoint only binds a fresh wire link and is never trusted as a
            // semantic identity.
            node: NodeUuid(uuid::Uuid::new_v5(
                &uuid::Uuid::NAMESPACE_URL,
                peer_identity.canonical().as_bytes(),
            )),
            epoch: NEXT_CLIENT_CONNECTION_EPOCH.fetch_add(1, Ordering::Relaxed),
        };
        let hello = WireFrame::Hello(
            WireHello::current(WirePeerRole::Client, current_wire_features())
                .with_authority(client_endpoint.node, client_endpoint.epoch),
        );
        let encoded_hello = encode_frame(&hello).map_err(WebSocketClientError::EncodeHello)?;
        let batch = postcard::to_allocvec(&vec![encoded_hello])
            .map_err(WebSocketClientError::EncodeHello)?;
        ws.send(Message::Binary(batch.into()))
            .await
            .map_err(WebSocketClientError::Send)?;

        let server_hello = receive_server_hello(&mut ws).await?;
        let mut negotiated = negotiate_wire(
            &server_hello,
            WIRE_PROTOCOL_VERSION,
            WIRE_PROTOCOL_VERSION,
            current_wire_features(),
        )
        .map_err(WebSocketClientError::Negotiation)?;
        // Receipt semantics require an admitted authority endpoint, not merely
        // a feature bit from a legacy hello.
        if server_hello.authority.is_none() {
            negotiated.features &= !(jazz::wire::FEATURE_AUTHORIZATION_SCOPE_RECEIPTS
                | jazz::wire::FEATURE_AUTHORIZATION_SCOPE_VIEWS);
        }
        if negotiated.features & WS_CLIENT_REQUIRED_FEATURES != WS_CLIENT_REQUIRED_FEATURES {
            return Err(WebSocketClientError::ServerRejected(
                "server did not negotiate sync message payload frames".to_owned(),
            ));
        }
        let session_context = if negotiated.features
            & (jazz::wire::FEATURE_AUTHORIZATION_SCOPE_RECEIPTS
                | jazz::wire::FEATURE_AUTHORIZATION_SCOPE_VIEWS)
            != 0
        {
            server_hello
                .authority
                .map(|remote| ConnectionSessionContext {
                    local: client_endpoint,
                    remote,
                    link_identity: peer_identity,
                    negotiated_features: negotiated.features,
                })
        } else {
            None
        };

        let (inbound_tx, inbound_rx) = mpsc::channel(WS_CLIENT_INBOUND_FRAME_SLOTS);
        let inbound = Arc::new(Mutex::new(inbound_rx));
        let inbound_error = Arc::new(Mutex::new(None));
        let inbound_notify = Arc::new(Notify::new());
        let inbound_budget = Arc::new(Semaphore::new(WS_CLIENT_MAX_QUEUED_BYTES));
        let (outbound, outbound_rx, outbound_backpressured) = BoundedOutbound::channel();
        let task = tokio::spawn(run_ws_pump(
            ws,
            inbound_tx,
            inbound_budget,
            Arc::clone(&inbound_error),
            Arc::clone(&inbound_notify),
            outbound_rx,
            outbound_backpressured,
            Arc::clone(&wake),
            bootstrap_catalogue,
        ));

        Ok(Self {
            inbound,
            inbound_error,
            inbound_notify,
            outbound,
            task,
            protocol_version: negotiated.protocol_version,
            features: negotiated.features,
            session_context,
        })
    }

    /// Negotiated metadata authenticated during the websocket handshake.
    pub fn negotiated_transport_metadata(&self) -> (u16, u64, Option<ConnectionSessionContext>) {
        (self.protocol_version, self.features, self.session_context)
    }
}

impl Drop for WebSocketTransport {
    fn drop(&mut self) {
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
    #[serde(default, skip_serializing_if = "is_false")]
    bootstrap_catalogue: bool,
}

fn is_false(value: &bool) -> bool {
    !*value
}

fn encode_prelude(
    peer_identity: AuthorSubject,
    auth: AuthConfig,
    bootstrap_catalogue: bool,
) -> Result<Vec<u8>, WebSocketClientError> {
    serde_json::to_vec(&WebSocketClientPrelude {
        peer_identity: peer_identity.canonical().to_owned(),
        auth,
        bootstrap_catalogue,
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

fn client_websocket_config() -> WebSocketConfig {
    WebSocketConfig::default()
        // The server can batch raw wire frames up to this protocol boundary;
        // logical catalogue snapshots still span many such WebSocket messages.
        .max_message_size(Some(MAX_WIRE_FRAME_BYTES))
        .max_frame_size(Some(MAX_WIRE_FRAME_BYTES))
}

fn validate_catalogue_bootstrap_upstream_url(base_url: &str, app_id: AppId) -> Result<(), String> {
    validate_bootstrap_upstream_url(&ws_url(base_url, app_id)).map_err(|error| error.to_string())
}

/// Bootstrap relies on the configured upstream transport for server identity:
/// `wss://` gets the existing WebSocket/TLS validation, while plaintext is
/// deliberately limited to loopback by default.  This is not mutual TLS or a
/// new cryptographic authority proof; operators must configure a trusted WSS
/// endpoint for remote edges.
fn validate_bootstrap_upstream_url(base_url: &str) -> Result<(), WebSocketClientError> {
    let url = reqwest::Url::parse(base_url).map_err(|error| {
        WebSocketClientError::ServerRejected(format!("invalid bootstrap upstream URL: {error}"))
    })?;
    if url.scheme() != "ws" {
        return Ok(());
    }
    let loopback = url.host_str().is_some_and(|host| {
        let host = host.trim_matches(['[', ']']);
        host.eq_ignore_ascii_case("localhost")
            || host
                .parse::<std::net::IpAddr>()
                .is_ok_and(|address| address.is_loopback())
    });
    let explicitly_allowed = std::env::var("JAZZ_ALLOW_INSECURE_EDGE_BOOTSTRAP_WS")
        .ok()
        .as_deref()
        == Some("1");
    if loopback || explicitly_allowed {
        Ok(())
    } else {
        Err(WebSocketClientError::ServerRejected(
            "plaintext ws:// bootstrap is allowed only for loopback; configure wss:// or set JAZZ_ALLOW_INSECURE_EDGE_BOOTSTRAP_WS=1 for an explicit development override"
                .to_owned(),
        ))
    }
}

fn fail_inbound(
    error_slot: &Arc<Mutex<Option<String>>>,
    notify: &Notify,
    error: impl Into<String>,
) {
    if let Ok(mut slot) = error_slot.lock() {
        *slot = Some(error.into());
    }
    notify.notify_waiters();
}

async fn receive_server_hello(
    ws: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
) -> Result<WireHello, WebSocketClientError> {
    let message = tokio::time::timeout(WS_CLIENT_HANDSHAKE_TIMEOUT, ws.next())
        .await
        .map_err(|_| WebSocketClientError::HandshakeTimeout)?
        .ok_or(WebSocketClientError::ClosedDuringHandshake)?
        .map_err(WebSocketClientError::Receive)?;

    let Message::Binary(bytes) = message else {
        return Err(WebSocketClientError::UnexpectedHandshakeMessage);
    };
    let encoded: Vec<Vec<u8>> =
        postcard::from_bytes(&bytes).map_err(WebSocketClientError::DecodeBatch)?;
    if encoded.len() != 1 {
        return Err(WebSocketClientError::UnexpectedHandshakeMessage);
    }
    let frame = decode_frame(&encoded[0]).map_err(WebSocketClientError::DecodeFrame)?;
    let WireFrame::Hello(hello) = frame else {
        if let WireFrame::Error(error) = frame {
            return Err(WebSocketClientError::ServerRejected(format!(
                "{:?}: {}",
                error.code, error.message
            )));
        }
        return Err(WebSocketClientError::UnexpectedHandshakeMessage);
    };
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
async fn run_ws_pump(
    mut ws: tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    inbound: mpsc::Sender<InboundFrame>,
    inbound_budget: Arc<Semaphore>,
    inbound_error: Arc<Mutex<Option<String>>>,
    inbound_notify: Arc<Notify>,
    mut outbound: mpsc::UnboundedReceiver<QueuedOutboundFrame>,
    outbound_backpressured: Arc<AtomicBool>,
    wake: Arc<dyn Fn() + Send + Sync>,
    bootstrap_catalogue: bool,
) {
    async {
      loop {
        tokio::select! {
            maybe_frame = outbound.recv() => {
                let Some(first_frame) = maybe_frame else {
                    let _ = ws.close(None).await;
                    return;
                };
                let mut batch = vec![first_frame];
                let mut batch_bytes = POSTCARD_BATCH_LENGTH_RESERVE
                    + batch[0].bytes.len()
                    + POSTCARD_FRAME_LENGTH_RESERVE;
                while let Ok(frame) = outbound.try_recv() {
                    let frame_bytes = frame.bytes.len() + POSTCARD_FRAME_LENGTH_RESERVE;
                    if batch.len() >= MAX_WIRE_BATCH_FRAMES
                        || batch_bytes.saturating_add(frame_bytes) > WS_CLIENT_OUTBOUND_BATCH_BYTES
                    {
                        let Ok(bytes) = postcard::to_allocvec(&OutboundBatch(&batch)) else {
                            return;
                        };
                        if ws.send(Message::Binary(bytes.into())).await.is_err() {
                            return;
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
                let Ok(bytes) = postcard::to_allocvec(&OutboundBatch(&batch)) else {
                    return;
                };
                if ws.send(Message::Binary(bytes.into())).await.is_err() {
                    return;
                }
                drop(batch);
                if outbound_backpressured.swap(false, Ordering::AcqRel) {
                    wake();
                }
            }
            message = ws.next() => {
                let bytes = match message {
                    Some(Ok(Message::Binary(bytes))) => bytes,
                    Some(Ok(_)) => {
                        fail_inbound(&inbound_error, &inbound_notify, "websocket peer sent a non-binary wire batch");
                        let _ = ws.close(None).await;
                        return;
                    }
                    Some(Err(error)) => {
                        fail_inbound(&inbound_error, &inbound_notify, format!("websocket receive failed: {error}"));
                        return;
                    }
                    None => {
                        fail_inbound(&inbound_error, &inbound_notify, "websocket peer closed before completing wire exchange");
                        return;
                    }
                };
                let frames = match decode_inbound_batch(&bytes, bootstrap_catalogue) {
                    Ok(frames) => frames,
                    Err(error) => {
                        fail_inbound(&inbound_error, &inbound_notify, error);
                        let _ = ws.close(None).await;
                        return;
                    }
                };
                for frame in frames {
                    if let Err(error) = validate_wire_frame_len(frame.len()) {
                        fail_inbound(&inbound_error, &inbound_notify, error);
                        let _ = ws.close(None).await;
                        return;
                    }
                    let permits = u32::try_from(frame.len()).expect("wire frame limit fits u32");
                    let Ok(budget) = Arc::clone(&inbound_budget).acquire_many_owned(permits).await else {
                        return;
                    };
                    if inbound.send(InboundFrame { bytes: frame, _budget: budget }).await.is_err() {
                        return;
                    }
                    // A maximum logical message can exceed the bounded
                    // channel. Wake on every frame so its consumer drains
                    // before the producer blocks on a later fragment.
                    inbound_notify.notify_one();
                    wake();
                }
            }
        }
      }
    }
    .await;
    finish_outbound_pump(outbound, &outbound_backpressured, wake.as_ref());
}

fn decode_inbound_batch(bytes: &[u8], bootstrap_catalogue: bool) -> Result<Vec<Vec<u8>>, String> {
    let frames = postcard::from_bytes::<Vec<Vec<u8>>>(bytes)
        .map_err(|_| "websocket peer sent malformed wire batch".to_owned())?;
    if frames.len() > MAX_WIRE_BATCH_FRAMES {
        return Err(format!(
            "websocket inbound batch exceeds frame-count limit of {MAX_WIRE_BATCH_FRAMES}"
        ));
    }
    if bootstrap_catalogue && frames.is_empty() {
        return Err("bootstrap peer sent an empty wire batch".to_owned());
    }
    Ok(frames)
}

#[cfg(test)]
mod tests {
    use super::*;
    use jazz::db::Transport;
    use std::collections::{BTreeMap, VecDeque};

    #[derive(Clone)]
    struct FrameSink(Arc<Mutex<VecDeque<Vec<u8>>>>);

    impl WireTransport for FrameSink {
        fn send_frame(&mut self, frame: Vec<u8>) -> Result<(), TransportError> {
            self.0.lock().expect("frame sink lock").push_back(frame);
            Ok(())
        }

        fn try_recv_frame(&mut self) -> Option<Vec<u8>> {
            None
        }
    }

    fn valid_fragmented_wire_message_larger_than_ingress_budget() -> Vec<Vec<u8>> {
        let frames = Arc::new(Mutex::new(VecDeque::new()));
        let sink = FrameSink(Arc::clone(&frames));
        let features = FEATURE_SYNC_MESSAGE_PAYLOAD | jazz::wire::FEATURE_MESSAGE_FRAGMENTATION;
        let mut sender = WireTransportAdapter::new(sink, WIRE_PROTOCOL_VERSION, features, None);
        let body = (0..(WS_CLIENT_MAX_QUEUED_BYTES + 1))
            .map(|index| char::from((index % 251) as u8))
            .collect::<String>();
        sender
            .send(jazz::protocol::SyncMessage::SessionClaims {
                identity: AuthorSubject::SYSTEM,
                claims: BTreeMap::from([(
                    "catalogue_fixture".to_owned(),
                    jazz::groove::records::Value::String(body),
                )]),
            })
            .expect("encode valid fragmented logical message");
        let frames = frames
            .lock()
            .expect("frame sink lock")
            .drain(..)
            .collect::<Vec<_>>();
        assert!(frames.len() > 1, "message must be wire fragmented");
        assert!(frames.iter().map(Vec::len).sum::<usize>() > WS_CLIENT_MAX_QUEUED_BYTES);
        frames
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

    #[tokio::test]
    async fn fragmented_bootstrap_larger_than_ingress_budget_streams_with_backpressure() {
        let (sender, mut receiver) = mpsc::channel::<InboundFrame>(1);
        let budget = Arc::new(Semaphore::new(WS_CLIENT_MAX_QUEUED_BYTES));
        let consumer = tokio::spawn(async move {
            let mut received = 0_usize;
            while let Some(frame) = receiver.recv().await {
                received += frame.bytes.len();
            }
            received
        });
        let frames = valid_fragmented_wire_message_larger_than_ingress_budget();
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
    fn malformed_or_truncated_batch_wakes_bootstrap_with_a_terminal_error() {
        let error = Arc::new(Mutex::new(None));
        let notify = Notify::new();
        fail_inbound(&error, &notify, "websocket peer sent malformed wire batch");
        assert_eq!(
            error.lock().expect("queue lock").as_deref(),
            Some("websocket peer sent malformed wire batch")
        );
    }

    #[test]
    fn bootstrap_count_flood_or_empty_batch_is_rejected_before_ingress_staging() {
        let empty = postcard::to_allocvec(&Vec::<Vec<u8>>::new()).expect("encode empty batch");
        assert!(
            decode_inbound_batch(&empty, true)
                .expect_err("bootstrap must reject empty batches")
                .contains("empty")
        );

        let flood = postcard::to_allocvec(&vec![Vec::<u8>::new(); MAX_WIRE_BATCH_FRAMES + 1])
            .expect("encode count flood below physical byte cap");
        assert!(flood.len() <= MAX_WIRE_FRAME_BYTES);
        assert!(
            decode_inbound_batch(&flood, false)
                .expect_err("count flood must be rejected before channel staging")
                .contains("frame-count limit")
        );
    }

    #[test]
    fn snapshot_bootstrap_prelude_explicitly_marks_the_snapshot_only_exchange() {
        let bytes = encode_prelude(AuthorSubject::SYSTEM, AuthConfig::default(), true)
            .expect("encode snapshot bootstrap prelude");
        let prelude: serde_json::Value =
            serde_json::from_slice(&bytes).expect("decode snapshot bootstrap prelude");
        assert_eq!(
            prelude.get("bootstrap_catalogue"),
            Some(&serde_json::Value::Bool(true)),
            "bootstrap callers must opt into the snapshot-only server exchange"
        );
    }

    #[test]
    fn remote_plaintext_bootstrap_requires_explicit_override() {
        assert!(validate_bootstrap_upstream_url("ws://127.0.0.1:4200").is_ok());
        assert!(validate_bootstrap_upstream_url("ws://[::1]:4200").is_ok());
        assert!(validate_bootstrap_upstream_url("wss://core.example.test").is_ok());
        assert!(validate_bootstrap_upstream_url("ws://[::2]:4200").is_err());
        assert!(validate_bootstrap_upstream_url("ws://core.example.test").is_err());
        assert!(
            validate_bootstrap_upstream_url("ws://core.example.test")
                .expect_err("remote plaintext bootstrap must fail")
                .to_string()
                .contains("plaintext ws:// bootstrap")
        );
    }

    #[tokio::test]
    async fn outbound_queue_returns_backpressure_at_a_finite_byte_budget() {
        let (_inbound_sender, inbound) = mpsc::channel(1);
        let (outbound, mut outbound_receiver, outbound_backpressured) = BoundedOutbound::channel();
        let task = tokio::spawn(std::future::pending());
        let mut transport = WebSocketTransport {
            inbound: Arc::new(Mutex::new(inbound)),
            inbound_error: Arc::new(Mutex::new(None)),
            inbound_notify: Arc::new(Notify::new()),
            outbound,
            task,
            protocol_version: WIRE_PROTOCOL_VERSION,
            features: FEATURE_SYNC_MESSAGE_PAYLOAD,
            session_context: None,
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
