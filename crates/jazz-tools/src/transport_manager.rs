//! WebSocket transport layer: TransportHandle, TransportManager, StreamAdapter, TickNotifier.
//!
//! TransportHandle is held by RuntimeCore (replaces SyncSender).
//! TransportManager owns the live WebSocket connection and reconnects on failure.

use futures::channel::mpsc;
use crate::sync_manager::types::{ClientId, InboxEntry, OutboxEntry, ServerId};

pub trait TickNotifier: 'static {
    fn notify(&self);
}

#[allow(async_fn_in_trait)]
pub trait StreamAdapter: Sized {
    type Error: std::fmt::Display;
    async fn connect(url: &str) -> Result<Self, Self::Error>;
    async fn send(&mut self, data: &[u8]) -> Result<(), Self::Error>;
    async fn recv(&mut self) -> Result<Option<Vec<u8>>, Self::Error>;
    async fn close(&mut self);
}

#[derive(Debug)]
pub enum TransportInbound {
    Connected {
        catalogue_state_hash: Option<String>,
        next_sync_seq: Option<u64>,
    },
    Sync {
        entry: Box<InboxEntry>,
        sequence: Option<u64>,
    },
    Disconnected,
}

// M-6: derive Debug — all fields implement Debug.
#[derive(Debug)]
pub struct TransportHandle {
    pub server_id: ServerId,
    pub client_id: ClientId,
    pub outbox_tx: mpsc::UnboundedSender<OutboxEntry>,
    pub inbound_rx: mpsc::UnboundedReceiver<TransportInbound>,
    pub ever_connected: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

impl TransportHandle {
    /// Returns None both when the channel is empty and when it's closed.
    pub fn try_recv_inbound(&mut self) -> Option<TransportInbound> {
        self.inbound_rx.try_recv().ok()
    }
    pub fn send_outbox(&self, entry: OutboxEntry) {
        let _ = self.outbox_tx.unbounded_send(entry);
    }
    pub fn has_ever_connected(&self) -> bool {
        self.ever_connected.load(std::sync::atomic::Ordering::Acquire)
    }
}

// I-4: hand-written Debug that redacts secret fields.
#[derive(Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct AuthConfig {
    pub jwt_token: Option<String>,
    pub backend_secret: Option<String>,
    pub admin_secret: Option<String>,
    pub backend_session: Option<serde_json::Value>,
    #[serde(default)]
    pub local_mode: Option<String>,
    #[serde(default)]
    pub local_token: Option<String>,
}

impl std::fmt::Debug for AuthConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuthConfig")
            .field("jwt_token", &self.jwt_token.as_ref().map(|_| "<redacted>"))
            .field("backend_secret", &self.backend_secret.as_ref().map(|_| "<redacted>"))
            .field("admin_secret", &self.admin_secret.as_ref().map(|_| "<redacted>"))
            // backend_session may itself contain secrets; redact presence only.
            .field("backend_session", &self.backend_session.as_ref().map(|_| "<redacted>"))
            .field("local_mode", &self.local_mode)
            .field("local_token", &self.local_token.as_ref().map(|_| "<redacted>"))
            .finish()
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct AuthHandshake {
    pub client_id: String,
    pub auth: AuthConfig,
    pub catalogue_state_hash: Option<String>,
}

#[derive(Debug, serde::Deserialize)]
pub struct ConnectedResponse {
    pub connection_id: String,
    pub client_id: String,
    pub next_sync_seq: Option<u64>,
    pub catalogue_state_hash: Option<String>,
}

#[derive(Default)]
pub struct ReconnectState {
    attempt: u32,
}

impl ReconnectState {
    pub fn new() -> Self { Self::default() }
    pub fn reset(&mut self) { self.attempt = 0; }

    pub async fn backoff(&mut self) {
        // I-2: cap applied AFTER adding jitter so the 10_000 ceiling is meaningful
        // at higher attempt counts if the min(5) exponent cap is ever raised.
        let base_ms = 300u64.saturating_mul(1u64 << self.attempt.min(5));
        let jitter = (rand::random::<u8>() as u64 * 200) / 255;
        let delay_ms = (base_ms + jitter).min(10_000);
        #[cfg(all(not(target_arch = "wasm32"), feature = "runtime-tokio"))]
        { tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await; }
        // M-7: WASM / no-tokio: no real sleep is available. Yield one poll cycle to avoid
        // a tight spin; outer reconnect loop relies on network I/O awaits for real backpressure.
        #[cfg(any(target_arch = "wasm32", not(feature = "runtime-tokio")))]
        { let _ = delay_ms; futures::future::ready(()).await; }
        self.attempt += 1;
    }
}

#[allow(dead_code)] // fields used in Task 3 run loop
pub struct TransportManager<W: StreamAdapter, T: TickNotifier> {
    pub server_id: ServerId,
    pub url: String,
    pub auth: AuthConfig,
    outbox_rx: mpsc::UnboundedReceiver<OutboxEntry>,
    inbound_tx: mpsc::UnboundedSender<TransportInbound>,
    pub tick: T,
    reconnect: ReconnectState,
    pub client_id: ClientId,
    ever_connected: std::sync::Arc<std::sync::atomic::AtomicBool>,
    _stream: std::marker::PhantomData<W>,
}

pub fn create<W: StreamAdapter, T: TickNotifier>(
    url: String,
    auth: AuthConfig,
    tick: T,
) -> (TransportHandle, TransportManager<W, T>) {
    let server_id = ServerId::new();
    let client_id = ClientId::new();
    let (outbox_tx, outbox_rx) = mpsc::unbounded();
    let (inbound_tx, inbound_rx) = mpsc::unbounded();
    let ever_connected = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let handle = TransportHandle {
        server_id, client_id,
        outbox_tx, inbound_rx,
        ever_connected: ever_connected.clone(),
    };
    let manager = TransportManager {
        server_id, url, auth, outbox_rx, inbound_tx,
        tick, reconnect: ReconnectState::new(),
        client_id, ever_connected,
        _stream: std::marker::PhantomData,
    };
    (handle, manager)
}
