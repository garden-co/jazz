//! WebSocket transport layer for Jazz.
//!
//! Provides a bidirectional, ordered transport that replaces the SSE + HTTP POST
//! protocol. All transport logic — framing, reconnection, auth handshake, heartbeat
//! — lives here. Platform crates provide thin `StreamAdapter` implementations.
//!
//! # Architecture
//!
//! ```text
//! RuntimeCore                          TransportManager
//!     │                                      │
//!     │── outbox_tx ──────────────────► outbox_rx
//!     │   (batched_tick pushes here)     (send loop → ws.send)
//!     │                                      │
//!     │◄── inbound_rx ◄──────────────── inbound_tx
//!     │   (batched_tick drains here)     (recv loop ← ws.recv)
//!     │                                      │
//!     │◄── tick notification ◄───────── tick.notify()
//!     │   (schedule_batched_tick)            │
//! ```
//!
//! `TransportManager` has no reference to `RuntimeCore` — only channels and a
//! `TickNotifier`. This eliminates lock contention (NAPI/RN) and borrow conflicts (WASM).

use std::fmt;
use std::future::Future;
use std::time::Duration;

use tracing::{debug, info, warn};

use crate::sync_manager::{ClientId, InboxEntry, OutboxEntry, ServerId, Source};
use crate::transport_protocol::{ServerEvent, SyncBatchRequest};

// ============================================================================
// StreamAdapter trait
// ============================================================================

/// Platform-specific bidirectional byte stream.
///
/// Named `StreamAdapter` (not `WebSocketAdapter`) because the same trait
/// works for WebSocket and single-stream WebTransport.
///
/// Implementations:
/// - `NativeWsStream`: `tokio-tungstenite` (NAPI, React Native, server, tests)
/// - `WasmWsStream`: `web-sys::WebSocket` (browser WASM) — future
pub trait StreamAdapter: Sized {
    type Error: fmt::Display + fmt::Debug;

    /// Open a connection to the given URL.
    fn connect(url: &str) -> impl Future<Output = Result<Self, Self::Error>> + Send;

    /// Send a binary message.
    fn send(&mut self, data: &[u8]) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Receive the next binary message. Returns `None` on clean close.
    fn recv(&mut self) -> impl Future<Output = Result<Option<Vec<u8>>, Self::Error>> + Send;

    /// Close the connection gracefully.
    fn close(&mut self) -> impl Future<Output = ()> + Send;
}

// ============================================================================
// TickNotifier trait
// ============================================================================

/// Notifies the runtime scheduler that inbound messages arrived.
///
/// Each platform implements this by cloning its scheduler and calling
/// `schedule_batched_tick()`. ~5 LOC per implementation.
pub trait TickNotifier: Send {
    fn notify(&self);
}

// Re-export TransportHandle from runtime_core (the canonical definition).
pub use crate::runtime_core::TransportHandle;

/// Signal that fires once the WebSocket handshake completes (server sends `Connected`).
/// Carries the server's catalogue_state_hash for delta sync.
pub struct ConnectedSignal {
    pub rx: tokio::sync::oneshot::Receiver<Option<String>>,
}

// ============================================================================
// AuthConfig — re-export from transport_protocol
// ============================================================================

/// Re-export the auth handshake types so callers can use them without
/// depending on transport_protocol directly.
pub use crate::transport_protocol::{
    AuthHandshake as WsAuthConfig, AuthHandshakePayload as AuthPayload,
};

// ============================================================================
// ReconnectState
// ============================================================================

/// Exponential backoff state for reconnection.
/// Base: 300ms, cap: 10s, random jitter: 0..200ms.
struct ReconnectState {
    attempt: u32,
    base_ms: u64,
    cap_ms: u64,
    jitter_ms: u64,
}

impl ReconnectState {
    fn new() -> Self {
        Self {
            attempt: 0,
            base_ms: 300,
            cap_ms: 10_000,
            jitter_ms: 200,
        }
    }

    fn reset(&mut self) {
        self.attempt = 0;
    }

    async fn backoff(&mut self) {
        let delay = std::cmp::min(self.base_ms * 2u64.pow(self.attempt), self.cap_ms);
        let jitter = rand::random::<u64>() % self.jitter_ms;
        let total = Duration::from_millis(delay + jitter);
        debug!(
            attempt = self.attempt,
            delay_ms = total.as_millis(),
            "reconnect backoff"
        );

        tokio::time::sleep(total).await;
        self.attempt = self.attempt.saturating_add(1);
    }
}

// ============================================================================
// TransportManager
// ============================================================================

/// Core transport logic. Owns the WebSocket connection and runs send/recv loops.
///
/// Generic over `StreamAdapter` (platform WebSocket) and `TickNotifier` (platform
/// scheduler notification). Has no reference to `RuntimeCore`.
///
/// One implementation shared across all platforms.
pub struct TransportManager<W: StreamAdapter, T: TickNotifier> {
    url: String,
    auth: WsAuthConfig,
    outbox_rx: tokio::sync::mpsc::UnboundedReceiver<OutboxEntry>,
    inbound_tx: tokio::sync::mpsc::UnboundedSender<InboxEntry>,
    tick: T,
    reconnect: ReconnectState,
    connected_tx: Option<tokio::sync::oneshot::Sender<Option<String>>>,
    _phantom: std::marker::PhantomData<W>,
}

impl<W: StreamAdapter, T: TickNotifier> TransportManager<W, T> {
    /// Main loop. Connects, runs send/recv, reconnects on failure.
    /// Exits when the outbox channel closes (TransportHandle dropped).
    pub async fn run(mut self) {
        loop {
            match W::connect(&self.url).await {
                Ok(mut ws) => {
                    self.reconnect.reset();

                    if let Err(e) = self.send_auth_handshake(&mut ws).await {
                        warn!(error = %e, "auth handshake failed");
                        continue;
                    }

                    let exit = self.run_connected(&mut ws).await;
                    let _ = ws.close().await;

                    if exit {
                        info!("transport manager exiting (channel closed)");
                        return;
                    }
                    warn!("websocket connection lost, reconnecting");
                }
                Err(e) => {
                    warn!(error = %e, url = %self.url, "websocket connect failed");
                }
            }
            self.reconnect.backoff().await;
        }
    }

    /// Send the auth handshake as the first message after connect.
    async fn send_auth_handshake(&self, ws: &mut W) -> Result<(), W::Error> {
        let handshake_json =
            serde_json::to_vec(&self.auth).expect("auth config serialization cannot fail");
        ws.send(&handshake_json).await
    }

    /// Run the bidirectional send/recv loop.
    /// Returns `true` if the outbox channel was closed (clean disconnect).
    /// Returns `false` if the WebSocket connection was lost (should reconnect).
    async fn run_connected(&mut self, ws: &mut W) -> bool {
        loop {
            tokio::select! {
                msg = self.outbox_rx.recv() => {
                    match msg {
                        Some(entry) => {
                            // Only send server-destined messages over the WebSocket.
                            // Client-destined messages (peer/worker bridge) are handled elsewhere.
                            if !matches!(entry.destination, crate::sync_manager::Destination::Server(_)) {
                                continue;
                            }
                            let frame = match self.serialize_outbox_entry(&entry) {
                                Ok(f) => f,
                                Err(e) => {
                                    warn!(error = %e, "serialize outbox entry failed");
                                    continue;
                                }
                            };
                            if let Err(e) = ws.send(&frame).await {
                                warn!(error = %e, "ws send failed");
                                return false; // reconnect
                            }
                        }
                        None => {
                            // Channel closed — TransportHandle dropped
                            return true; // exit
                        }
                    }
                }
                frame = ws.recv() => {
                    match frame {
                        Ok(Some(data)) => {
                            match self.deserialize_server_event(&data) {
                                Ok(event) => self.handle_server_event(event),
                                Err(e) => {
                                    warn!(error = %e, "failed to deserialize server event");
                                }
                            }
                        }
                        Ok(None) => {
                            // Clean close
                            debug!("websocket closed by server");
                            return false; // reconnect
                        }
                        Err(e) => {
                            warn!(error = %e, "websocket recv error");
                            return false; // reconnect
                        }
                    }
                }
            }
        }
    }

    /// Handle a received server event.
    fn handle_server_event(&mut self, event: ServerEvent) {
        match event {
            ServerEvent::Connected {
                catalogue_state_hash,
                ..
            } => {
                if let Some(tx) = self.connected_tx.take() {
                    let _ = tx.send(catalogue_state_hash);
                }
            }
            ServerEvent::SyncUpdate { payload, .. } => {
                let entry = InboxEntry {
                    source: Source::Server(ServerId::new()),
                    payload: *payload,
                };
                let _ = self.inbound_tx.send(entry);
                self.tick.notify();
            }
            ServerEvent::Heartbeat => {
                // No-op — WebSocket ping/pong handles keepalive
            }
            ServerEvent::Error { message, code } => {
                warn!(?code, %message, "server error");
            }
            ServerEvent::Subscribed { .. } => {
                // Acknowledgment — no action needed
            }
        }
    }

    /// Serialize an OutboxEntry to a binary frame (JSON).
    fn serialize_outbox_entry(&self, entry: &OutboxEntry) -> Result<Vec<u8>, serde_json::Error> {
        // Send as SyncBatchRequest with a single payload for now.
        // The client_id is taken from the auth config.
        let client_id = self
            .auth
            .client_id
            .as_ref()
            .and_then(|id| ClientId::parse(id))
            .unwrap_or_default();

        let request = SyncBatchRequest {
            payloads: vec![entry.payload.clone()],
            client_id,
        };
        serde_json::to_vec(&request)
    }

    /// Deserialize a binary frame to a ServerEvent.
    fn deserialize_server_event(&self, data: &[u8]) -> Result<ServerEvent, serde_json::Error> {
        serde_json::from_slice(data)
    }
}

// ============================================================================
// Factory function
// ============================================================================

/// Create a transport pair: `TransportHandle` for RuntimeCore, `TransportManager` to spawn.
///
/// The `TransportHandle` uses `std::sync::mpsc` (synchronous, works on all platforms).
/// The `TransportManager` uses `tokio::sync::mpsc` internally (async-compatible).
/// Bridging tasks are spawned to forward messages between the two channel types.
///
/// ```ignore
/// let (handle, manager, signal) = transport_ws::create::<NativeWsStream, MyTickNotifier>(
///     "ws://localhost:1625/ws".into(),
///     auth_config,
///     tick_notifier,
/// );
/// runtime_core.set_transport(handle);
/// tokio::spawn(manager.run());
/// ```
pub fn create<W: StreamAdapter, T: TickNotifier>(
    url: String,
    auth: WsAuthConfig,
    tick: T,
) -> (TransportHandle, TransportManager<W, T>, ConnectedSignal) {
    // RuntimeCore-facing channels (std::sync::mpsc — synchronous, always available)
    let (std_outbox_tx, std_outbox_rx) = std::sync::mpsc::channel::<OutboxEntry>();
    let (std_inbound_tx, std_inbound_rx) = std::sync::mpsc::channel::<InboxEntry>();

    // TransportManager-facing channels (tokio::sync::mpsc — async-compatible)
    let (tokio_outbox_tx, tokio_outbox_rx) = tokio::sync::mpsc::unbounded_channel::<OutboxEntry>();
    let (tokio_inbound_tx, tokio_inbound_rx) = tokio::sync::mpsc::unbounded_channel::<InboxEntry>();

    let (connected_tx, connected_rx) = tokio::sync::oneshot::channel();

    // Bridge: std outbox → tokio outbox (RuntimeCore sends → Manager receives)
    // Uses spawn_blocking because std::sync::mpsc::recv() is a blocking call.
    tokio::task::spawn_blocking(move || {
        while let Ok(entry) = std_outbox_rx.recv() {
            if tokio_outbox_tx.send(entry).is_err() {
                break; // Manager dropped
            }
        }
    });

    // Bridge: tokio inbound → std inbound (Manager sends → RuntimeCore receives)
    tokio::spawn(async move {
        let mut rx = tokio_inbound_rx;
        while let Some(entry) = rx.recv().await {
            if std_inbound_tx.send(entry).is_err() {
                break; // RuntimeCore dropped the TransportHandle
            }
        }
    });

    let handle = TransportHandle {
        outbox_tx: std_outbox_tx,
        inbound_rx: std_inbound_rx,
    };

    let manager = TransportManager {
        url,
        auth,
        outbox_rx: tokio_outbox_rx,
        inbound_tx: tokio_inbound_tx,
        tick,
        reconnect: ReconnectState::new(),
        connected_tx: Some(connected_tx),
        _phantom: std::marker::PhantomData,
    };

    let signal = ConnectedSignal { rx: connected_rx };

    (handle, manager, signal)
}
