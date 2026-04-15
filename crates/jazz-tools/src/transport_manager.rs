//! WebSocket transport layer: TransportHandle, TransportManager, StreamAdapter, TickNotifier.
//!
//! TransportHandle is held by RuntimeCore (replaces SyncSender).
//! TransportManager owns the live WebSocket connection and reconnects on failure.

use crate::sync_manager::types::{ClientId, InboxEntry, OutboxEntry, ServerId};
use futures::channel::mpsc;

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
        self.ever_connected
            .load(std::sync::atomic::Ordering::Acquire)
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
            .field(
                "backend_secret",
                &self.backend_secret.as_ref().map(|_| "<redacted>"),
            )
            .field(
                "admin_secret",
                &self.admin_secret.as_ref().map(|_| "<redacted>"),
            )
            // backend_session may itself contain secrets; redact presence only.
            .field(
                "backend_session",
                &self.backend_session.as_ref().map(|_| "<redacted>"),
            )
            .field("local_mode", &self.local_mode)
            .field(
                "local_token",
                &self.local_token.as_ref().map(|_| "<redacted>"),
            )
            .finish()
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct AuthHandshake {
    pub client_id: String,
    pub auth: AuthConfig,
    pub catalogue_state_hash: Option<String>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
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
    pub fn new() -> Self {
        Self::default()
    }
    pub fn reset(&mut self) {
        self.attempt = 0;
    }

    pub async fn backoff(&mut self) {
        // I-2: cap applied AFTER adding jitter so the 10_000 ceiling is meaningful
        // at higher attempt counts if the min(5) exponent cap is ever raised.
        let base_ms = 300u64.saturating_mul(1u64 << self.attempt.min(5));
        let jitter = (rand::random::<u8>() as u64 * 200) / 255;
        let delay_ms = (base_ms + jitter).min(10_000);
        #[cfg(all(not(target_arch = "wasm32"), feature = "runtime-tokio"))]
        {
            tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
        }
        // M-7: WASM / no-tokio: no real sleep is available. Yield one poll cycle to avoid
        // a tight spin; outer reconnect loop relies on network I/O awaits for real backpressure.
        #[cfg(any(target_arch = "wasm32", not(feature = "runtime-tokio")))]
        {
            let _ = delay_ms;
            futures::future::ready(()).await;
        }
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

/// Encode a payload as a 4-byte big-endian length-prefixed frame.
pub(crate) fn frame_encode(payload: &[u8]) -> Vec<u8> {
    debug_assert!(
        payload.len() <= u32::MAX as usize,
        "frame payload exceeds u32 limit"
    );
    let mut out = Vec::with_capacity(4 + payload.len());
    out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    out.extend_from_slice(payload);
    out
}

/// Decode a 4-byte big-endian length-prefixed frame, returning the payload slice.
pub(crate) fn frame_decode(data: &[u8]) -> Option<&[u8]> {
    if data.len() < 4 {
        return None;
    }
    let len = u32::from_be_bytes(data[0..4].try_into().unwrap()) as usize;
    if data.len() < 4 + len {
        return None;
    }
    Some(&data[4..4 + len])
}

#[cfg(feature = "runtime-tokio")]
enum ConnectedExit {
    HandleDropped,
    NetworkError,
}

#[cfg(feature = "runtime-tokio")]
impl<W: StreamAdapter + 'static, T: TickNotifier + 'static> TransportManager<W, T> {
    /// Drive the transport: connect, authenticate, relay frames, reconnect on failure.
    /// Returns only when the `TransportHandle` is dropped.
    ///
    /// Note: termination during connect/handshake/backoff requires the caller to abort the
    /// spawned task; `HandleDropped` is only detected inside `run_connected`.
    pub async fn run(mut self) {
        loop {
            match W::connect(&self.url).await {
                Ok(mut ws) => {
                    match self.perform_auth_handshake(&mut ws).await {
                        Ok(resp) => {
                            // TODO(later tasks): resp.connection_id is currently dropped; wire it
                            // through once the protocol layer consumes it.
                            self.ever_connected
                                .store(true, std::sync::atomic::Ordering::Release);
                            let _ = self.inbound_tx.unbounded_send(TransportInbound::Connected {
                                catalogue_state_hash: resp.catalogue_state_hash,
                                next_sync_seq: resp.next_sync_seq,
                            });
                            self.tick.notify();
                            self.reconnect.reset();
                            match self.run_connected(&mut ws).await {
                                ConnectedExit::HandleDropped => {
                                    ws.close().await;
                                    return;
                                }
                                ConnectedExit::NetworkError => {
                                    let _ = self
                                        .inbound_tx
                                        .unbounded_send(TransportInbound::Disconnected);
                                    self.tick.notify();
                                    ws.close().await;
                                }
                            }
                        }
                        Err(e) => {
                            tracing::warn!("ws auth handshake failed: {e}");
                            ws.close().await;
                        }
                    }
                }
                Err(e) => tracing::warn!("ws connect failed: {e}"),
            }
            self.reconnect.backoff().await;
        }
    }

    async fn perform_auth_handshake(&mut self, ws: &mut W) -> Result<ConnectedResponse, String> {
        let handshake = AuthHandshake {
            client_id: self.client_id.to_string(),
            auth: self.auth.clone(),
            catalogue_state_hash: None,
        };
        let payload = serde_json::to_vec(&handshake).map_err(|e| e.to_string())?;
        let frame = frame_encode(&payload);
        ws.send(&frame).await.map_err(|e| e.to_string())?;
        let resp_bytes = ws
            .recv()
            .await
            .map_err(|e| e.to_string())?
            .ok_or_else(|| "server closed before handshake response".to_string())?;
        let resp_payload = frame_decode(&resp_bytes).ok_or("malformed handshake response")?;
        serde_json::from_slice::<ConnectedResponse>(resp_payload).map_err(|e| e.to_string())
    }

    async fn run_connected(&mut self, ws: &mut W) -> ConnectedExit {
        use futures::StreamExt as _;
        loop {
            tokio::select! {
                out = self.outbox_rx.next() => {
                    let Some(entry) = out else { return ConnectedExit::HandleDropped; };
                    let Ok(bytes) = serde_json::to_vec(&entry) else { continue; };
                    let frame = frame_encode(&bytes);
                    if ws.send(&frame).await.is_err() { return ConnectedExit::NetworkError; }
                }
                incoming = ws.recv() => {
                    match incoming {
                        Ok(Some(data)) => {
                            let Some(payload) = frame_decode(&data) else { continue; };
                            let Ok(event) = serde_json::from_slice::<crate::transport_protocol::ServerEvent>(payload) else { continue; };
                            match event {
                                crate::transport_protocol::ServerEvent::SyncUpdate { seq, payload } => {
                                    let entry = InboxEntry {
                                        source: crate::sync_manager::types::Source::Server(self.server_id),
                                        payload: *payload,
                                    };
                                    let _ = self.inbound_tx.unbounded_send(TransportInbound::Sync {
                                        entry: Box::new(entry),
                                        sequence: seq,
                                    });
                                    self.tick.notify();
                                }
                                crate::transport_protocol::ServerEvent::Heartbeat => {}
                                crate::transport_protocol::ServerEvent::Connected { .. } => {
                                    tracing::warn!("unexpected Connected frame mid-stream; ignoring");
                                }
                                crate::transport_protocol::ServerEvent::Error { message, code } => {
                                    tracing::warn!(message, ?code, "server reported error");
                                }
                                other => {
                                    tracing::debug!(variant = other.variant_name(), "received non-sync ServerEvent; skipping");
                                }
                            }
                        }
                        Ok(None) | Err(_) => return ConnectedExit::NetworkError,
                    }
                }
            }
        }
    }
}

// WASM-compatible run() — uses `futures::select!` instead of `tokio::select!`.
// Activated when `runtime-tokio` is not (i.e. WASM).
#[cfg(not(feature = "runtime-tokio"))]
enum WasmConnectedExit {
    HandleDropped,
    NetworkError,
}

#[cfg(not(feature = "runtime-tokio"))]
impl<W: StreamAdapter + 'static, T: TickNotifier + 'static> TransportManager<W, T> {
    /// Drive the transport: connect, authenticate, relay frames, reconnect on failure.
    /// Returns only when the `TransportHandle` is dropped.
    pub async fn run(mut self) {
        loop {
            match W::connect(&self.url).await {
                Ok(mut ws) => match self.wasm_perform_auth_handshake(&mut ws).await {
                    Ok(resp) => {
                        self.ever_connected
                            .store(true, std::sync::atomic::Ordering::Release);
                        let _ = self.inbound_tx.unbounded_send(TransportInbound::Connected {
                            catalogue_state_hash: resp.catalogue_state_hash,
                            next_sync_seq: resp.next_sync_seq,
                        });
                        self.tick.notify();
                        self.reconnect.reset();
                        match self.wasm_run_connected(&mut ws).await {
                            WasmConnectedExit::HandleDropped => {
                                ws.close().await;
                                return;
                            }
                            WasmConnectedExit::NetworkError => {
                                let _ = self
                                    .inbound_tx
                                    .unbounded_send(TransportInbound::Disconnected);
                                self.tick.notify();
                                ws.close().await;
                            }
                        }
                    }
                    Err(e) => {
                        tracing::warn!("ws auth handshake failed: {e}");
                        ws.close().await;
                    }
                },
                Err(e) => tracing::warn!("ws connect failed: {e}"),
            }
            self.reconnect.backoff().await;
        }
    }

    async fn wasm_perform_auth_handshake(
        &mut self,
        ws: &mut W,
    ) -> Result<ConnectedResponse, String> {
        let handshake = AuthHandshake {
            client_id: self.client_id.to_string(),
            auth: self.auth.clone(),
            catalogue_state_hash: None,
        };
        let payload = serde_json::to_vec(&handshake).map_err(|e| e.to_string())?;
        let frame = frame_encode(&payload);
        ws.send(&frame).await.map_err(|e| e.to_string())?;
        let resp_bytes = ws
            .recv()
            .await
            .map_err(|e| e.to_string())?
            .ok_or_else(|| "server closed before handshake response".to_string())?;
        let resp_payload = frame_decode(&resp_bytes).ok_or("malformed handshake response")?;
        serde_json::from_slice::<ConnectedResponse>(resp_payload).map_err(|e| e.to_string())
    }

    async fn wasm_run_connected(&mut self, ws: &mut W) -> WasmConnectedExit {
        use futures::{FutureExt as _, StreamExt as _};
        loop {
            futures::select! {
                out = self.outbox_rx.next().fuse() => {
                    let Some(entry) = out else { return WasmConnectedExit::HandleDropped; };
                    let Ok(bytes) = serde_json::to_vec(&entry) else { continue; };
                    let frame = frame_encode(&bytes);
                    if ws.send(&frame).await.is_err() { return WasmConnectedExit::NetworkError; }
                }
                incoming = ws.recv().fuse() => {
                    match incoming {
                        Ok(Some(data)) => {
                            let Some(payload) = frame_decode(&data) else { continue; };
                            let Ok(event) = serde_json::from_slice::<crate::transport_protocol::ServerEvent>(payload) else { continue; };
                            match event {
                                crate::transport_protocol::ServerEvent::SyncUpdate { seq, payload } => {
                                    let entry = InboxEntry {
                                        source: crate::sync_manager::types::Source::Server(self.server_id),
                                        payload: *payload,
                                    };
                                    let _ = self.inbound_tx.unbounded_send(TransportInbound::Sync {
                                        entry: Box::new(entry),
                                        sequence: seq,
                                    });
                                    self.tick.notify();
                                }
                                crate::transport_protocol::ServerEvent::Heartbeat => {}
                                crate::transport_protocol::ServerEvent::Connected { .. } => {
                                    tracing::warn!("unexpected Connected frame mid-stream; ignoring");
                                }
                                crate::transport_protocol::ServerEvent::Error { message, code } => {
                                    tracing::warn!(message, ?code, "server reported error");
                                }
                                other => {
                                    tracing::debug!(variant = other.variant_name(), "received non-sync ServerEvent; skipping");
                                }
                            }
                        }
                        Ok(None) | Err(_) => return WasmConnectedExit::NetworkError,
                    }
                }
            }
        }
    }
}

#[cfg(feature = "runtime-tokio")]
#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct MockStream {
        sent: Vec<Vec<u8>>,
        inbound: VecDeque<Vec<u8>>,
    }
    impl StreamAdapter for MockStream {
        type Error = &'static str;
        async fn connect(_url: &str) -> Result<Self, Self::Error> {
            // Pre-load a valid ConnectedResponse frame so the handshake succeeds.
            let resp = ConnectedResponse {
                connection_id: "conn-1".into(),
                client_id: "client-1".into(),
                next_sync_seq: Some(0),
                catalogue_state_hash: None,
            };
            let payload = serde_json::to_vec(&resp).unwrap();
            let frame = frame_encode(&payload);
            let mut inbound = VecDeque::new();
            inbound.push_back(frame);
            Ok(MockStream {
                sent: Vec::new(),
                inbound,
            })
        }
        async fn send(&mut self, data: &[u8]) -> Result<(), Self::Error> {
            self.sent.push(data.to_vec());
            Ok(())
        }
        async fn recv(&mut self) -> Result<Option<Vec<u8>>, Self::Error> {
            Ok(self.inbound.pop_front())
        }
        async fn close(&mut self) {}
    }

    #[derive(Clone)]
    struct CountingTick(Arc<AtomicUsize>);
    impl TickNotifier for CountingTick {
        fn notify(&self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn handshake_marks_ever_connected_and_notifies_tick() {
        let counter = Arc::new(AtomicUsize::new(0));
        let tick = CountingTick(counter.clone());
        let (handle, manager) =
            create::<MockStream, CountingTick>("mock://".to_string(), AuthConfig::default(), tick);
        let task = tokio::spawn(manager.run());

        // Give the manager time to run the handshake.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        assert!(
            handle.has_ever_connected(),
            "handshake should have set ever_connected"
        );
        assert!(
            counter.load(Ordering::SeqCst) >= 1,
            "tick should have been notified"
        );

        drop(handle);
        let _ = tokio::time::timeout(std::time::Duration::from_secs(1), task).await;
    }
}
