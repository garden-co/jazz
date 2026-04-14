//! WebSocket transport layer: TransportHandle, TransportManager, StreamAdapter, TickNotifier.
//!
//! TransportHandle is held by RuntimeCore (replaces SyncSender).
//! TransportManager owns the live WebSocket connection and reconnects on failure.

use futures::StreamExt as _;
use futures::channel::mpsc;

use crate::sync_manager::types::{InboxEntry, OutboxEntry, ServerId, Source};

// ============================================================================
// Traits
// ============================================================================

/// Platform-specific: notifies the scheduler to run batched_tick() when
/// inbound transport events arrive. Intentionally NOT Send — WASM uses
/// Rc-based scheduler state on the same thread.
pub trait TickNotifier: 'static {
    fn notify(&self);
}

/// Platform-specific bidirectional byte stream.
/// Named StreamAdapter (not WebSocketAdapter) so WebTransport can slot in later.
#[allow(async_fn_in_trait)]
pub trait StreamAdapter: Sized {
    type Error: std::fmt::Display;
    async fn connect(url: &str) -> Result<Self, Self::Error>;
    async fn send(&mut self, data: &[u8]) -> Result<(), Self::Error>;
    async fn recv(&mut self) -> Result<Option<Vec<u8>>, Self::Error>;
    async fn close(&mut self);
}

// ============================================================================
// Inbound events
// ============================================================================

/// Reason a credential was rejected. Mirrors the TS `AuthFailureReason` union.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthFailureReason {
    Expired,
    Missing,
    Invalid,
    Disabled,
}

impl AuthFailureReason {
    pub fn as_str(self) -> &'static str {
        match self {
            AuthFailureReason::Expired => "expired",
            AuthFailureReason::Missing => "missing",
            AuthFailureReason::Invalid => "invalid",
            AuthFailureReason::Disabled => "disabled",
        }
    }
}

/// Inbound events from TransportManager to RuntimeCore via channel.
#[derive(Debug)]
pub enum TransportInbound {
    Connected {
        catalogue_state_hash: Option<String>,
        next_sync_seq: Option<u64>,
    },
    Sync {
        /// Boxed because `InboxEntry` contains `SyncPayload` which can be
        /// large (row blobs, catalogue entries). Boxing keeps the enum
        /// size small for the `Connected` and `Disconnected` variants that
        /// are moved through the same mpsc channel.
        entry: Box<InboxEntry>,
        sequence: Option<u64>,
    },
    Disconnected,
    /// Server rejected the auth handshake. Reconnect loop is permanently
    /// stopped after this event — the runtime must surface it to the host
    /// app (markUnauthenticated) so credentials can be refreshed.
    AuthFailure {
        reason: AuthFailureReason,
    },
}

// ============================================================================
// TransportHandle
// ============================================================================

/// Replaces SyncSender on RuntimeCore. Concrete type on all platforms.
pub struct TransportHandle {
    pub server_id: ServerId,
    /// The client ID this transport will use in the handshake and outbox frames.
    pub client_id: crate::sync_manager::types::ClientId,
    pub outbox_tx: mpsc::UnboundedSender<OutboxEntry>,
    pub inbound_rx: mpsc::UnboundedReceiver<TransportInbound>,
    /// Set to true by the manager once the first auth handshake succeeds.
    /// Callers can poll this to wait for the transport to actually be live
    /// before assuming server-bound writes will reach the server.
    pub ever_connected: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

impl TransportHandle {
    /// Non-blocking receive of the next inbound event, if any.
    pub fn try_recv_inbound(&mut self) -> Option<TransportInbound> {
        self.inbound_rx.try_recv().ok()
    }

    /// Send an outbox entry to the transport manager.
    pub fn send_outbox(&self, entry: OutboxEntry) {
        let _ = self.outbox_tx.unbounded_send(entry);
    }

    /// Returns true once the transport has successfully completed its first
    /// auth handshake with the server.
    pub fn has_ever_connected(&self) -> bool {
        self.ever_connected
            .load(std::sync::atomic::Ordering::Acquire)
    }
}

// ============================================================================
// Reconnection state
// ============================================================================

/// Reconnection state with exponential backoff.
#[derive(Default)]
pub struct ReconnectState {
    attempt: u32,
}

impl ReconnectState {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn backoff(&mut self) {
        let base_ms = 300u64.saturating_mul(1u64 << self.attempt.min(5));
        let capped = base_ms.min(10_000);
        let jitter = (rand::random::<u8>() as u64 * 200) / 255;
        let delay_ms = capped + jitter;
        #[cfg(all(not(target_arch = "wasm32"), feature = "runtime-tokio"))]
        {
            tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
        }
        #[cfg(any(target_arch = "wasm32", not(feature = "runtime-tokio")))]
        {
            // WASM or non-tokio native: yield to the event loop without a real timer.
            // A proper timer-based sleep can be added per platform later.
            let _ = delay_ms;
            futures::future::ready(()).await;
        }
        self.attempt += 1;
    }

    pub fn reset(&mut self) {
        self.attempt = 0;
    }
}

// ============================================================================
// Auth types
// ============================================================================

/// Auth config for WebSocket transport.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct AuthConfig {
    pub jwt_token: Option<String>,
    pub backend_secret: Option<String>,
    pub admin_secret: Option<String>,
    /// Session for backend impersonation.
    pub backend_session: Option<serde_json::Value>,
}

/// Wire message sent by client as first frame after WS upgrade.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct AuthHandshake {
    pub client_id: String,
    pub auth: AuthConfig,
    pub catalogue_state_hash: Option<String>,
}

/// Wire response sent by server after successful auth.
#[derive(Debug, serde::Deserialize)]
pub struct ConnectedResponse {
    pub connection_id: String,
    pub client_id: String,
    pub next_sync_seq: Option<u64>,
    pub catalogue_state_hash: Option<String>,
}

// ============================================================================
// TransportManager
// ============================================================================

pub struct TransportManager<W: StreamAdapter, T: TickNotifier> {
    pub server_id: ServerId,
    pub url: String,
    pub auth: AuthConfig,
    outbox_rx: mpsc::UnboundedReceiver<OutboxEntry>,
    inbound_tx: mpsc::UnboundedSender<TransportInbound>,
    pub tick: T,
    reconnect: ReconnectState,
    /// Client ID generated at construction; sent in handshake and used for
    /// encoding outbox frames.
    pub client_id: crate::sync_manager::types::ClientId,
    /// Shared with TransportHandle. Set once the first auth handshake succeeds.
    ever_connected: std::sync::Arc<std::sync::atomic::AtomicBool>,
    _stream: std::marker::PhantomData<W>,
}

// ============================================================================
// Constructor
// ============================================================================

/// Create a TransportHandle + TransportManager pair.
pub fn create<W: StreamAdapter, T: TickNotifier>(
    url: String,
    auth: AuthConfig,
    tick: T,
) -> (TransportHandle, TransportManager<W, T>) {
    let server_id = ServerId::new();
    let client_id = crate::sync_manager::types::ClientId::new();
    let (outbox_tx, outbox_rx) = mpsc::unbounded();
    let (inbound_tx, inbound_rx) = mpsc::unbounded();
    let ever_connected = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let handle = TransportHandle {
        server_id,
        client_id,
        outbox_tx,
        inbound_rx,
        ever_connected: ever_connected.clone(),
    };
    let manager = TransportManager {
        server_id,
        url,
        auth,
        outbox_rx,
        inbound_tx,
        tick,
        reconnect: ReconnectState::new(),
        client_id,
        ever_connected,
        _stream: std::marker::PhantomData,
    };
    (handle, manager)
}

// ============================================================================
// TransportManager::run() impl
// ============================================================================

/// Signals why `run_connected` returned.
enum ConnectedExit {
    /// Outbox channel closed — TransportHandle was dropped; stop the loop.
    HandleDropped,
    /// Network error or server close — reconnect after backoff.
    NetworkError,
}

impl<W: StreamAdapter + 'static, T: TickNotifier + 'static> TransportManager<W, T> {
    /// Main loop — runs until the outbox channel closes (TransportHandle dropped).
    pub async fn run(mut self) {
        loop {
            match W::connect(&self.url).await {
                Ok(mut ws) => {
                    // catalogue_state_hash is passed as None here; resuming from a known
                    // hash after reconnect is a future optimization (server sends full state
                    // on reconnect when hash is absent).
                    match self.perform_auth_handshake(&mut ws, None).await {
                        Ok(connected) => {
                            self.ever_connected
                                .store(true, std::sync::atomic::Ordering::Release);
                            let _ = self.inbound_tx.unbounded_send(TransportInbound::Connected {
                                catalogue_state_hash: connected.catalogue_state_hash,
                                next_sync_seq: connected.next_sync_seq,
                            });
                            self.tick.notify();
                            let exit = self.run_connected(&mut ws).await;
                            // Send Disconnected on every exit path (network failure
                            // *and* HandleDropped) so RuntimeCore can clean up its
                            // upstream state for this server_id even when the caller
                            // never invokes clear_transport.
                            let _ = self
                                .inbound_tx
                                .unbounded_send(TransportInbound::Disconnected);
                            self.tick.notify();
                            if let ConnectedExit::HandleDropped = exit {
                                return;
                            }
                        }
                        Err(HandshakeError::Auth { reason, message }) => {
                            tracing::warn!(?reason, "WebSocket auth handshake rejected: {message}");
                            let _ = self
                                .inbound_tx
                                .unbounded_send(TransportInbound::AuthFailure { reason });
                            self.tick.notify();
                            // Permanent failure — do NOT reconnect-spin. The host
                            // app must refresh credentials and call connect() again.
                            return;
                        }
                        Err(HandshakeError::Network(e)) => {
                            tracing::warn!("WebSocket auth handshake failed: {e}");
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!("WebSocket connect failed: {e}");
                }
            }
            self.reconnect.backoff().await;
        }
    }

    async fn run_connected(&mut self, ws: &mut W) -> ConnectedExit {
        use futures::future::FutureExt as _;

        loop {
            futures::select! {
                msg = self.outbox_rx.next().fuse() => {
                    let Some(entry) = msg else {
                        // Outbox channel closed — TransportHandle was dropped.
                        return ConnectedExit::HandleDropped;
                    };
                    let frame = encode_outbox_entry_as_frame(&entry, self.client_id);
                    if ws.send(&frame).await.is_err() {
                        break;
                    }
                }
                frame_result = ws.recv().fuse() => {
                    match frame_result {
                        Ok(Some(data)) => {
                            use crate::transport_protocol::ServerEvent;
                            if let Some((event, _)) = ServerEvent::decode_frame(&data) {
                                match event {
                                    ServerEvent::Heartbeat => continue,
                                    ServerEvent::Connected { .. } => continue,
                                    ServerEvent::SyncUpdate { seq, payload } => {
                                        let entry = Box::new(InboxEntry {
                                            source: Source::Server(self.server_id),
                                            payload: *payload,
                                        });
                                        let _ = self.inbound_tx.unbounded_send(TransportInbound::Sync {
                                            entry,
                                            sequence: seq,
                                        });
                                        self.tick.notify();
                                    }
                                    ServerEvent::Error { message, code } => {
                                        tracing::warn!(?code, "server sent error: {message}");
                                    }
                                    ServerEvent::Subscribed { .. } => {
                                        // Not used by this transport path.
                                    }
                                }
                            }
                        }
                        _ => break,
                    }
                }
            }
        }
        ConnectedExit::NetworkError
    }

    async fn perform_auth_handshake(
        &self,
        ws: &mut W,
        catalogue_state_hash: Option<String>,
    ) -> Result<ConnectedResponse, HandshakeError> {
        let handshake = AuthHandshake {
            client_id: self.client_id.to_string(),
            auth: self.auth.clone(),
            catalogue_state_hash,
        };
        let json =
            serde_json::to_vec(&handshake).map_err(|e| HandshakeError::Network(e.to_string()))?;
        let mut frame = Vec::with_capacity(4 + json.len());
        frame.extend_from_slice(&(json.len() as u32).to_be_bytes());
        frame.extend_from_slice(&json);
        ws.send(&frame)
            .await
            .map_err(|e| HandshakeError::Network(e.to_string()))?;

        let data = ws
            .recv()
            .await
            .map_err(|e| HandshakeError::Network(e.to_string()))?
            .ok_or_else(|| {
                HandshakeError::Network("connection closed before handshake".to_string())
            })?;

        use crate::transport_protocol::{ErrorCode, ServerEvent};
        match ServerEvent::decode_frame(&data) {
            Some((
                ServerEvent::Connected {
                    connection_id,
                    client_id,
                    next_sync_seq,
                    catalogue_state_hash,
                },
                _,
            )) => Ok(ConnectedResponse {
                connection_id: connection_id.0.to_string(),
                client_id,
                next_sync_seq,
                catalogue_state_hash,
            }),
            Some((ServerEvent::Error { message, code }, _)) => match code {
                ErrorCode::Unauthorized | ErrorCode::Forbidden => Err(HandshakeError::Auth {
                    reason: classify_auth_failure_reason(&message),
                    message,
                }),
                _ => Err(HandshakeError::Network(format!(
                    "server error during handshake: {message}"
                ))),
            },
            _ => Err(HandshakeError::Network(
                "expected Connected frame, got something else".into(),
            )),
        }
    }
}

/// Classifies the auth-error message returned by the server into a typed
/// reason. The server currently sends free-form English; we string-match
/// against the known phrases produced in `routes.rs`.
fn classify_auth_failure_reason(message: &str) -> AuthFailureReason {
    let m = message.to_ascii_lowercase();
    if m.contains("expired") {
        AuthFailureReason::Expired
    } else if m.contains("missing") || m.contains("session required") {
        AuthFailureReason::Missing
    } else if m.contains("disabled") {
        AuthFailureReason::Disabled
    } else {
        AuthFailureReason::Invalid
    }
}

#[derive(Debug)]
enum HandshakeError {
    Network(String),
    Auth {
        reason: AuthFailureReason,
        message: String,
    },
}

impl std::fmt::Display for HandshakeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HandshakeError::Network(m) => write!(f, "network: {m}"),
            HandshakeError::Auth { reason, message } => {
                write!(f, "auth ({}): {message}", reason.as_str())
            }
        }
    }
}

// ============================================================================
// Frame encoding
// ============================================================================

/// Encode an OutboxEntry as a binary frame: [4-byte u32 BE length][JSON bytes]
/// The payload is wrapped in the server's `SyncBatchRequest` envelope so the
/// server can process one outbox entry per wire message while still reusing
/// the batch sync path.
fn encode_outbox_entry_as_frame(
    entry: &OutboxEntry,
    client_id: crate::sync_manager::types::ClientId,
) -> Vec<u8> {
    use crate::transport_protocol::SyncBatchRequest;
    let batch = SyncBatchRequest {
        payloads: vec![entry.payload.clone()],
        client_id,
    };
    let json = match serde_json::to_vec(&batch) {
        Ok(j) => j,
        Err(e) => {
            tracing::error!("failed to serialize outbox entry: {e}");
            return Vec::new();
        }
    };
    let mut frame = Vec::with_capacity(4 + json.len());
    frame.extend_from_slice(&(json.len() as u32).to_be_bytes());
    frame.extend_from_slice(&json);
    frame
}
