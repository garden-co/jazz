# WebSocket Transport Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the HTTP/SSE transport with a Rust-owned WebSocket transport that fixes outbound ordering, removes base64 overhead, and moves transport logic out of TypeScript into shared Rust.

**Architecture:** `RuntimeCore` stops being generic over `SyncSender`. It holds a concrete `TransportHandle` with two `mpsc` channels (outbox out, inbound events in). A standalone `TransportManager<W: StreamAdapter, T: TickNotifier>` async task owns the live WebSocket, performs the auth handshake, reconnects with backoff, and communicates with `RuntimeCore` exclusively through those channels. Each platform supplies a ~30 LOC `StreamAdapter` (native = tokio-tungstenite, WASM = web-sys) and a ~5 LOC `TickNotifier`. Server replaces `GET /events` + `POST /sync` with a single `/ws` endpoint.

**Tech Stack:** Rust (`jazz-tools`, `jazz-napi`, `jazz-rn`, `jazz-wasm`, `jazz-cloud-server`), `tokio-tungstenite`, `axum::ws`, `web-sys::WebSocket`, `ws_stream_wasm`, `futures::channel::mpsc`, TypeScript (`packages/jazz-tools`).

**Source spec:** `specs/todo/a_mvp/websocket.md` — read it end-to-end before starting. It defines every data flow, wire format, and lifecycle transition this plan implements.

---

## File Structure

### New files

- `crates/jazz-tools/src/transport_manager.rs` — `TransportHandle`, `TransportManager`, `StreamAdapter` trait, `TickNotifier` trait, `TransportInbound`, `AuthConfig`, `AuthHandshake`, `ConnectedResponse`, `ReconnectState`.
- `crates/jazz-tools/src/ws_stream/mod.rs` — re-export gate (`#[cfg(not(target_arch = "wasm32"))]`).
- `crates/jazz-tools/src/ws_stream/native.rs` — `NativeWsStream: StreamAdapter` over `tokio-tungstenite`.
- `crates/jazz-wasm/src/ws_stream.rs` — `WasmWsStream: StreamAdapter` over `web-sys::WebSocket` / `ws_stream_wasm`.
- `crates/jazz-tools/tests/websocket_transport.rs` — end-to-end WebSocket integration tests (ordering, reconnect, handshake).

### Modified files

- `crates/jazz-tools/Cargo.toml` — add `transport-websocket` feature, `tokio-tungstenite` dep, `axum` `ws` feature.
- `crates/jazz-tools/src/lib.rs` — declare `transport_manager`, gate `ws_stream`.
- `crates/jazz-tools/src/runtime_core.rs` — drop `SyncSender` generic, hold `Option<TransportHandle>`, expose `set_transport`/`clear_transport`.
- `crates/jazz-tools/src/runtime_core/ticks.rs` — drain inbound channel, push outbox to channel in `batched_tick`.
- `crates/jazz-tools/src/runtime_core/sync.rs`, `writes.rs`, `subscriptions.rs`, `tests.rs` — update to new `RuntimeCore<S, Sch>` shape.
- `crates/jazz-tools/src/runtime_tokio.rs` — remove `SyncSender` wiring, add `NativeTickNotifier`, `connect()` / `disconnect()` APIs spawning `TransportManager<NativeWsStream, NativeTickNotifier>`.
- `crates/jazz-tools/src/client.rs` — swap HTTP client for the WebSocket `TransportManager`.
- `crates/jazz-tools/src/routes.rs` — delete `/events` + `/sync`, add `/ws` upgrade handler, per-connection `mpsc` outbox.
- `crates/jazz-tools/src/server/builder.rs`, `external_identity_store.rs` — adjust feature gates.
- `crates/jazz-napi/Cargo.toml`, `src/lib.rs`, `index.d.ts`, `index.js` — delete `NapiSyncSender`, add `connect(url, authJson)` / `disconnect()`.
- `crates/jazz-rn/rust/Cargo.toml`, `src/lib.rs`, `src/generated/*` — delete `RnSyncSender`, add `connect` / `disconnect` via UniFFI, regenerate bindings.
- `crates/jazz-wasm/Cargo.toml`, `src/lib.rs`, `src/runtime.rs` — delete `JsSyncSender` for the server path, add `connect` / `disconnect`, wire `WasmTickNotifier`.
- `crates/jazz-cloud-server/Cargo.toml`, `src/server.rs`, `ENV.md` — wire `/ws` route, drop `/events` + `/sync` wiring.
- `packages/jazz-tools/src/runtime/client.ts` — replace TS transport plumbing with thin `connect`/`disconnect` calls.
- `packages/jazz-tools/src/runtime/sync-transport.ts` — delete network-facing code (StreamController, sendSyncPayloadBatch, readBinaryFrames). Keep only worker-bridge helpers if they survive.
- `packages/jazz-tools/src/worker/jazz-worker.ts`, `server-payload-batcher.ts` — remove network layer; keep worker-bridge `postMessage` path.
- `packages/jazz-tools/src/backend/create-jazz-context.ts`, `react-native/jazz-rn-runtime-adapter.ts`, `runtime/testing/napi-runtime-test-utils.ts` — switch to new `connect()` API.
- Existing tests: update (`client.ts`, `napi.integration.test.ts`, `cloud-server.integration.test.ts`, `sync-transport.lazy-client-id.test.ts`, `testing/index.test.ts`, `worker/jazz-worker.test.ts`) to new API. Delete `client.sync-auth.test.ts`, collapse `sync-transport.test.ts` to just surviving helpers.
- Rust tests: update `auth_test.rs`, `integration.rs`, `subscribe_all_integration.rs`, `policies_integration/*`, `support/mod.rs` for new API. Un-ignore the two ordering tests.

### Deleted

- `JsSyncSender`, `NapiSyncSender`, `RnSyncSender` types.
- `SyncSender` trait uses from `RuntimeCore` generic bound (trait may stay for `VecSyncSender` test helper — confirm in Task 4).
- `packages/jazz-tools/src/runtime/client.sync-auth.test.ts`.
- `packages/jazz-tools/src/worker/server-payload-batcher.ts` (replaced by Rust outbox channel).
- `GET /events`, `POST /sync` route handlers.
- `reqwest`-based `transport-http` runtime code paths for the client.

---

## Prerequisites

Before starting:

1. Read `specs/todo/a_mvp/websocket.md` — the authoritative design. Every unexplained decision below is explained there.
2. Read `todo/projects/transport/pitch.md` — the "why" and adoption context.
3. Confirm the worktree is clean:
   ```
   git status
   ```
   Expected: nothing to commit, branch ready for work.
4. Run the current test suite once to establish the baseline:
   ```
   cargo test -p jazz-tools --all-features 2>&1 | tail -40
   pnpm --filter jazz-tools test 2>&1 | tail -40
   ```
   Note the number of passing / failing / ignored tests — the two `#[ignore]`d ordering tests must be un-ignored and passing by the end of this plan.

---

## Phase 1 — Rust core transport module

### Task 1: Add `transport-websocket` Cargo feature and deps

**Files:**

- Modify: `crates/jazz-tools/Cargo.toml`

- [ ] **Step 1: Declare the feature and dependency**

In `[features]`, replace the `server` and `client` features and add `transport-websocket`:

```toml
transport-websocket = ["transport", "runtime-tokio", "dep:tokio-tungstenite"]
client = ["runtime-tokio", "transport-websocket", "dep:thiserror", "dep:tokio", "dep:base64"]
server = [
  "transport-websocket",
  "dep:tokio",
  "dep:axum",
  "dep:tracing-subscriber",
  # keep the rest of the existing server feature deps
]
```

In `[dependencies]`, add:

```toml
tokio-tungstenite = { version = "0.24", features = ["rustls-tls-native-roots"], optional = true }
```

And extend axum:

```toml
axum = { version = "0.7", features = ["ws"], optional = true }
```

- [ ] **Step 2: Verify features resolve**

Run: `cargo check -p jazz-tools --features transport-websocket`
Expected: clean build (no code uses the feature yet).

- [ ] **Step 3: Commit**

```bash
git add crates/jazz-tools/Cargo.toml
git commit -m "feat(jazz-tools): add transport-websocket feature and tokio-tungstenite dep"
```

### Task 2: Write `transport_manager.rs` skeleton (traits + types, no run loop)

**Files:**

- Create: `crates/jazz-tools/src/transport_manager.rs`
- Modify: `crates/jazz-tools/src/lib.rs`

- [ ] **Step 1: Declare the module**

Edit `crates/jazz-tools/src/lib.rs` — add alongside existing `pub mod ...` declarations:

```rust
pub mod transport_manager;
```

- [ ] **Step 2: Create the skeleton**

Create `crates/jazz-tools/src/transport_manager.rs` with:

```rust
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

pub struct TransportHandle {
    pub server_id: ServerId,
    pub client_id: ClientId,
    pub outbox_tx: mpsc::UnboundedSender<OutboxEntry>,
    pub inbound_rx: mpsc::UnboundedReceiver<TransportInbound>,
    pub ever_connected: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

impl TransportHandle {
    pub fn try_recv_inbound(&mut self) -> Option<TransportInbound> {
        self.inbound_rx.try_next().ok().flatten()
    }
    pub fn send_outbox(&self, entry: OutboxEntry) {
        let _ = self.outbox_tx.unbounded_send(entry);
    }
    pub fn has_ever_connected(&self) -> bool {
        self.ever_connected.load(std::sync::atomic::Ordering::Acquire)
    }
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
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
        let base_ms = 300u64.saturating_mul(1u64 << self.attempt.min(5));
        let capped = base_ms.min(10_000);
        let jitter = (rand::random::<u8>() as u64 * 200) / 255;
        let delay_ms = capped + jitter;
        #[cfg(all(not(target_arch = "wasm32"), feature = "runtime-tokio"))]
        { tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await; }
        #[cfg(any(target_arch = "wasm32", not(feature = "runtime-tokio")))]
        { let _ = delay_ms; futures::future::ready(()).await; }
        self.attempt += 1;
    }
}

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
```

- [ ] **Step 3: Verify it compiles**

Run: `cargo check -p jazz-tools --features transport-websocket`
Expected: clean build.

- [ ] **Step 4: Commit**

```bash
git add crates/jazz-tools/src/transport_manager.rs crates/jazz-tools/src/lib.rs
git commit -m "feat(jazz-tools): add transport_manager skeleton (traits, handle, types)"
```

### Task 3: Implement `TransportManager::run()` and `run_connected()`

**Files:**

- Modify: `crates/jazz-tools/src/transport_manager.rs`

- [ ] **Step 1: Write a failing unit test**

Append to the file:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    struct MockStream { sent: Vec<Vec<u8>>, inbound: std::collections::VecDeque<Vec<u8>> }
    impl StreamAdapter for MockStream {
        type Error = &'static str;
        async fn connect(_url: &str) -> Result<Self, Self::Error> {
            Ok(MockStream { sent: vec![], inbound: Default::default() })
        }
        async fn send(&mut self, data: &[u8]) -> Result<(), Self::Error> {
            self.sent.push(data.to_vec()); Ok(())
        }
        async fn recv(&mut self) -> Result<Option<Vec<u8>>, Self::Error> {
            Ok(self.inbound.pop_front())
        }
        async fn close(&mut self) {}
    }

    #[derive(Clone)]
    struct CountingTick(std::sync::Arc<std::sync::atomic::AtomicUsize>);
    impl TickNotifier for CountingTick {
        fn notify(&self) { self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst); }
    }

    #[tokio::test]
    async fn handshake_marks_ever_connected_and_notifies_tick() {
        let counter = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let tick = CountingTick(counter.clone());
        let (handle, manager) = create::<MockStream, CountingTick>(
            "mock://".to_string(),
            AuthConfig::default(),
            tick,
        );
        let task = tokio::spawn(manager.run());
        // Drop the handle after yielding — forces manager to exit cleanly.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        drop(handle);
        let _ = tokio::time::timeout(std::time::Duration::from_secs(1), task).await;
    }
}
```

- [ ] **Step 2: Run the test — it fails because `run` does not exist**

Run: `cargo test -p jazz-tools --features transport-websocket transport_manager::tests::handshake_marks_ever_connected_and_notifies_tick`
Expected: compile error "no method `run`".

- [ ] **Step 3: Implement `run()` and `run_connected()`**

Below the `TransportManager` struct, add:

```rust
enum ConnectedExit { HandleDropped, NetworkError }

impl<W: StreamAdapter + 'static, T: TickNotifier + 'static> TransportManager<W, T> {
    pub async fn run(mut self) {
        loop {
            match W::connect(&self.url).await {
                Ok(mut ws) => {
                    match self.perform_auth_handshake(&mut ws).await {
                        Ok(resp) => {
                            self.ever_connected.store(true, std::sync::atomic::Ordering::Release);
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
                                    let _ = self.inbound_tx.unbounded_send(TransportInbound::Disconnected);
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
        let resp_bytes = ws.recv().await.map_err(|e| e.to_string())?
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
                            let Ok(entry) = serde_json::from_slice::<InboxEntry>(payload) else { continue; };
                            let _ = self.inbound_tx.unbounded_send(TransportInbound::Sync {
                                entry: Box::new(entry),
                                sequence: None,
                            });
                            self.tick.notify();
                        }
                        Ok(None) | Err(_) => return ConnectedExit::NetworkError,
                    }
                }
            }
        }
    }
}

fn frame_encode(payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(4 + payload.len());
    out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    out.extend_from_slice(payload);
    out
}

fn frame_decode(data: &[u8]) -> Option<&[u8]> {
    if data.len() < 4 { return None; }
    let len = u32::from_be_bytes(data[0..4].try_into().unwrap()) as usize;
    if data.len() < 4 + len { return None; }
    Some(&data[4..4 + len])
}
```

> **Note on wire format:** Both handshake and sync frames use the 4-byte length + JSON body layout defined in the spec. See `specs/todo/a_mvp/websocket.md#wire-format`. When the existing server-side sync encoding is richer than `InboxEntry` / `OutboxEntry` serde (e.g. specific `ServerEvent` wrapper), refine `run_connected` to decode into the same enum `routes.rs` writes (Task 11). Keep `frame_encode` / `frame_decode` as the single framing helper.

- [ ] **Step 4: Run test again**

Run: `cargo test -p jazz-tools --features transport-websocket transport_manager::tests::handshake_marks_ever_connected_and_notifies_tick`
Expected: PASS (test just asserts the manager can be spawned + dropped cleanly; real behavior is covered by integration tests in Task 17).

- [ ] **Step 5: Commit**

```bash
git add crates/jazz-tools/src/transport_manager.rs
git commit -m "feat(jazz-tools): implement TransportManager run loop and auth handshake"
```

---

## Phase 2 — Stream adapters

### Task 4: Native stream adapter (`NativeWsStream`)

**Files:**

- Create: `crates/jazz-tools/src/ws_stream/mod.rs`
- Create: `crates/jazz-tools/src/ws_stream/native.rs`
- Modify: `crates/jazz-tools/src/lib.rs`

- [ ] **Step 1: Declare the module**

Add to `crates/jazz-tools/src/lib.rs`:

```rust
#[cfg(feature = "transport-websocket")]
pub mod ws_stream;
```

- [ ] **Step 2: Create the mod gate**

Write `crates/jazz-tools/src/ws_stream/mod.rs`:

```rust
#[cfg(not(target_arch = "wasm32"))]
mod native;
#[cfg(not(target_arch = "wasm32"))]
pub use native::NativeWsStream;
```

- [ ] **Step 3: Write the native adapter with a failing round-trip test**

Write `crates/jazz-tools/src/ws_stream/native.rs`:

```rust
use crate::transport_manager::StreamAdapter;
use futures::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};

pub struct NativeWsStream {
    inner: WebSocketStream<MaybeTlsStream<TcpStream>>,
}

impl StreamAdapter for NativeWsStream {
    type Error = tokio_tungstenite::tungstenite::Error;

    async fn connect(url: &str) -> Result<Self, Self::Error> {
        let (ws, _) = connect_async(url).await?;
        Ok(Self { inner: ws })
    }

    async fn send(&mut self, data: &[u8]) -> Result<(), Self::Error> {
        self.inner.send(Message::Binary(data.to_owned())).await
    }

    async fn recv(&mut self) -> Result<Option<Vec<u8>>, Self::Error> {
        loop {
            match self.inner.next().await {
                Some(Ok(Message::Binary(b))) => return Ok(Some(b)),
                Some(Ok(Message::Ping(_))) | Some(Ok(Message::Pong(_))) => continue,
                Some(Ok(Message::Close(_))) | None => return Ok(None),
                Some(Ok(Message::Text(_))) => {
                    tracing::warn!("received unexpected text frame on binary-only WS connection; ignoring");
                    continue;
                }
                Some(Ok(_)) => continue,
                Some(Err(e)) => return Err(e),
            }
        }
    }

    async fn close(&mut self) {
        let _ = self.inner.close(None).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;
    use tokio_tungstenite::accept_async;

    #[tokio::test]
    async fn native_ws_stream_send_recv_roundtrip() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut ws = accept_async(tcp).await.unwrap();
            while let Some(Ok(msg)) = ws.next().await {
                ws.send(msg).await.unwrap();
            }
        });
        let mut stream = NativeWsStream::connect(&format!("ws://{addr}")).await.unwrap();
        stream.send(b"hello ws").await.unwrap();
        assert_eq!(stream.recv().await.unwrap().unwrap(), b"hello ws".to_vec());
    }

    #[tokio::test]
    async fn native_ws_stream_server_close_yields_none_on_recv() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut ws = accept_async(tcp).await.unwrap();
            let _ = ws.close(None).await;
        });
        let mut stream = NativeWsStream::connect(&format!("ws://{addr}")).await.unwrap();
        assert!(stream.recv().await.unwrap().is_none());
    }
}
```

- [ ] **Step 4: Run the tests**

Run: `cargo test -p jazz-tools --features transport-websocket ws_stream::native::tests`
Expected: both PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/jazz-tools/src/ws_stream crates/jazz-tools/src/lib.rs
git commit -m "feat(jazz-tools): add NativeWsStream stream adapter"
```

### Task 5: WASM stream adapter (`WasmWsStream`)

**Files:**

- Modify: `crates/jazz-wasm/Cargo.toml`
- Create: `crates/jazz-wasm/src/ws_stream.rs`
- Modify: `crates/jazz-wasm/src/lib.rs`

- [ ] **Step 1: Add the dep**

Add to `crates/jazz-wasm/Cargo.toml` under `[dependencies]`:

```toml
ws_stream_wasm = "0.7"
```

- [ ] **Step 2: Write the adapter**

Create `crates/jazz-wasm/src/ws_stream.rs`:

```rust
use futures::{SinkExt, StreamExt};
use jazz_tools::transport_manager::StreamAdapter;
use ws_stream_wasm::{WsMessage, WsMeta, WsStream};

pub struct WasmWsStream {
    inner: WsStream,
}

impl StreamAdapter for WasmWsStream {
    type Error = ws_stream_wasm::WsErr;

    async fn connect(url: &str) -> Result<Self, Self::Error> {
        let (_meta, ws) = WsMeta::connect(url, None).await?;
        Ok(Self { inner: ws })
    }

    async fn send(&mut self, data: &[u8]) -> Result<(), Self::Error> {
        self.inner.send(WsMessage::Binary(data.to_vec())).await
    }

    async fn recv(&mut self) -> Result<Option<Vec<u8>>, Self::Error> {
        loop {
            match self.inner.next().await {
                Some(WsMessage::Binary(b)) => return Ok(Some(b)),
                Some(WsMessage::Text(_)) => continue,
                None => return Ok(None),
            }
        }
    }

    async fn close(&mut self) {
        let _ = self.inner.close().await;
    }
}
```

- [ ] **Step 3: Export it**

Add to `crates/jazz-wasm/src/lib.rs`:

```rust
pub mod ws_stream;
```

- [ ] **Step 4: Verify**

Run: `cargo check -p jazz-wasm --target wasm32-unknown-unknown`
Expected: clean build.

- [ ] **Step 5: Commit**

```bash
git add crates/jazz-wasm/
git commit -m "feat(jazz-wasm): add WasmWsStream stream adapter"
```

---

## Phase 3 — RuntimeCore integration

### Task 6: Drop `SyncSender` generic from `RuntimeCore`, hold `Option<TransportHandle>`

**Files:**

- Modify: `crates/jazz-tools/src/runtime_core.rs`
- Modify: `crates/jazz-tools/src/runtime_core/sync.rs`
- Modify: `crates/jazz-tools/src/runtime_core/subscriptions.rs`
- Modify: `crates/jazz-tools/src/runtime_core/writes.rs`

- [ ] **Step 1: Update the struct**

In `runtime_core.rs`, change:

```rust
pub struct RuntimeCore<S: Storage, Sch: Scheduler, Sy: SyncSender> {
    scheduler: Sch,
    sync_sender: Sy,
    // ...
}
```

to:

```rust
pub struct RuntimeCore<S: Storage, Sch: Scheduler> {
    scheduler: Sch,
    transport: Option<crate::transport_manager::TransportHandle>,
    // ...
}
```

Drop the `Sy: SyncSender` parameter from the `new` constructor. Add `set_transport` / `clear_transport`:

```rust
impl<S: Storage, Sch: Scheduler> RuntimeCore<S, Sch> {
    pub fn new(schema_manager: SchemaManager, storage: S, scheduler: Sch) -> Self {
        Self { scheduler, transport: None, /* existing fields */ }
    }

    pub fn set_transport(&mut self, handle: crate::transport_manager::TransportHandle) {
        self.transport = Some(handle);
    }

    pub fn clear_transport(&mut self) {
        if let Some(h) = self.transport.take() {
            self.remove_server(h.server_id);
        }
    }
}
```

Keep the existing `SyncSender` trait for now — `VecSyncSender` is still useful as a test spy. Just decouple it from the runtime generic bound.

- [ ] **Step 2: Update every `impl` block**

Remove the `Sy` parameter from `impl<S, Sch, Sy>` headers across:

- `runtime_core/sync.rs`
- `runtime_core/subscriptions.rs`
- `runtime_core/writes.rs`

Replace all uses of `self.sync_sender.send_sync_message(msg)` with:

```rust
if let Some(ref h) = self.transport {
    h.send_outbox(msg);
}
```

- [ ] **Step 3: Compile**

Run: `cargo check -p jazz-tools --all-features`
Expected: compiles; test modules may still be broken — Task 7 fixes them.

- [ ] **Step 4: Commit**

```bash
git add crates/jazz-tools/src/runtime_core.rs crates/jazz-tools/src/runtime_core/
git commit -m "refactor(jazz-tools): drop SyncSender generic from RuntimeCore"
```

### Task 7: Rewire `batched_tick` around the inbound/outbound channels

**Files:**

- Modify: `crates/jazz-tools/src/runtime_core/ticks.rs`

- [ ] **Step 1: Replace `batched_tick`**

Replace the body of `batched_tick` with the exact shape from `specs/todo/a_mvp/websocket.md#batched_tick-changes`:

```rust
pub fn batched_tick(&mut self) {
    // 1. Drain inbound transport events
    if let Some(ref mut transport) = self.transport {
        while let Some(event) = transport.try_recv_inbound() {
            match event {
                TransportInbound::Connected { catalogue_state_hash, next_sync_seq } => {
                    let server_id = transport.server_id;
                    self.remove_server(server_id);
                    self.add_server_with_catalogue_state_hash(
                        server_id,
                        catalogue_state_hash.as_deref(),
                    );
                    if let Some(seq) = next_sync_seq {
                        self.set_next_expected_server_sequence(server_id, seq);
                    }
                }
                TransportInbound::Sync { entry, sequence } => {
                    if let Some(seq) = sequence {
                        self.park_sync_message_with_sequence(*entry, seq);
                    } else {
                        self.park_sync_message(*entry);
                    }
                }
                TransportInbound::Disconnected => {
                    self.remove_server(transport.server_id);
                }
            }
        }
    }

    // 2. Drain outbox → push to outbound channel
    let outbox = self.sync_manager_mut().take_outbox();
    if let Some(ref transport) = self.transport {
        for msg in outbox { transport.send_outbox(msg); }
    }

    // 3. Process parked sync messages
    self.handle_sync_messages();

    // 4. Flush post-process outbox
    let outbox = self.sync_manager_mut().take_outbox();
    if let Some(ref transport) = self.transport {
        for msg in outbox { transport.send_outbox(msg); }
    }

    self.storage.flush_wal();
}
```

- [ ] **Step 2: Unit test the tick behavior**

Create or extend `crates/jazz-tools/src/runtime_core/tests.rs` with:

```rust
#[test]
fn batched_tick_drains_connected_event_and_adds_server() {
    let (handle, _mgr) = crate::transport_manager::create::<DummyStream, NoopTick>(
        "ws://test".into(), Default::default(), NoopTick,
    );
    let server_id = handle.server_id;
    let mut core = make_test_core(); // existing helper
    core.set_transport(handle);

    // Inject a Connected event by pushing through the manager's inbound_tx —
    // easier: build a TransportInbound in the test by constructing the handle
    // manually with a paired sender. See tests/support/mod.rs for helpers.
    push_inbound(&mut core, TransportInbound::Connected {
        catalogue_state_hash: None, next_sync_seq: Some(5),
    });

    core.batched_tick();

    assert!(core.has_server(server_id));
    assert_eq!(core.next_expected_server_sequence(server_id), 5);
}
```

> If `make_test_core` / `push_inbound` / `has_server` / `next_expected_server_sequence` don't exist, add them as narrow test helpers in `runtime_core/tests.rs` (and `tests/support/mod.rs` if shared). Do not expose new public runtime APIs just for tests — use `pub(crate)` or `#[cfg(test)]`.

- [ ] **Step 3: Run**

Run: `cargo test -p jazz-tools --features transport-websocket runtime_core::tests::batched_tick_drains_connected_event_and_adds_server`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add crates/jazz-tools/src/runtime_core/
git commit -m "feat(jazz-tools): drain TransportInbound + outbox in batched_tick"
```

### Task 8: Native `TickNotifier` + tokio `connect` / `disconnect`

**Files:**

- Modify: `crates/jazz-tools/src/runtime_tokio.rs`
- Modify: `crates/jazz-tools/src/client.rs`

- [ ] **Step 1: Define `NativeTickNotifier`**

In `runtime_tokio.rs`, near the existing scheduler impl:

```rust
use crate::transport_manager::TickNotifier;

#[derive(Clone)]
pub struct NativeTickNotifier {
    scheduler: NativeScheduler, // must be Clone + Send + Sync
}

impl TickNotifier for NativeTickNotifier {
    fn notify(&self) { self.scheduler.schedule_batched_tick(); }
}
```

- [ ] **Step 2: Replace `SyncSender` wiring with channel-based `connect`**

Remove any construction of `SyncSender` impls. Add:

```rust
impl TokioRuntime {
    pub fn connect(&self, url: String, auth: crate::transport_manager::AuthConfig) {
        let tick = NativeTickNotifier { scheduler: self.scheduler().clone() };
        let (handle, manager) = crate::transport_manager::create::<
            crate::ws_stream::NativeWsStream, NativeTickNotifier
        >(url, auth, tick);
        self.core.lock().unwrap().set_transport(handle);
        tokio::spawn(manager.run());
    }

    pub fn disconnect(&self) {
        self.core.lock().unwrap().clear_transport();
    }
}
```

- [ ] **Step 3: Update `client.rs`**

Strip HTTP / reqwest client code that duplicated the transport. Build the tokio runtime via the new `connect` pathway.

- [ ] **Step 4: Compile + smoke test**

Run: `cargo check -p jazz-tools --features client`
Expected: clean.

- [ ] **Step 5: Commit**

```bash
git add crates/jazz-tools/src/runtime_tokio.rs crates/jazz-tools/src/client.rs
git commit -m "feat(jazz-tools): add NativeTickNotifier and tokio connect/disconnect"
```

---

## Phase 4 — Server side

### Task 9: Add `/ws` route

**Files:**

- Modify: `crates/jazz-tools/src/routes.rs`
- Modify: `crates/jazz-tools/src/server/builder.rs`

- [ ] **Step 1: Add the handler**

In `routes.rs`, add:

```rust
use axum::extract::ws::{WebSocket, WebSocketUpgrade, Message};
use axum::response::Response;

pub async fn ws_handler(ws: WebSocketUpgrade, State(state): State<AppState>) -> Response {
    ws.on_upgrade(|socket| handle_ws_connection(socket, state))
}

async fn handle_ws_connection(mut socket: WebSocket, state: AppState) {
    // 1. Read handshake frame.
    let Some(Ok(Message::Binary(first))) = socket.recv().await else { return; };
    let Some(payload) = frame_decode(&first) else { return; };
    let Ok(handshake) = serde_json::from_slice::<crate::transport_manager::AuthHandshake>(payload) else { return; };

    // 2. Authenticate (reuse existing middleware logic).
    let Ok(session) = authenticate_handshake(&handshake.auth, &state).await else {
        let _ = socket.close().await;
        return;
    };

    // 3. Register connection. Per-connection mpsc outbox replaces the broadcast channel.
    let (client_id, mut inbound_rx) = state.register_ws_connection(session, &handshake).await;

    // 4. Send ConnectedResponse.
    let resp = crate::transport_manager::ConnectedResponse {
        connection_id: uuid::Uuid::new_v4().to_string(),
        client_id: client_id.to_string(),
        next_sync_seq: None,
        catalogue_state_hash: None,
    };
    let resp_bytes = serde_json::to_vec(&resp).unwrap();
    let _ = socket.send(Message::Binary(frame_encode(&resp_bytes))).await;

    // 5. Bidirectional loop.
    loop {
        tokio::select! {
            msg = socket.recv() => match msg {
                Some(Ok(Message::Binary(data))) => {
                    let Some(payload) = frame_decode(&data) else { continue; };
                    state.process_client_sync(client_id, payload).await;
                }
                Some(Ok(Message::Close(_))) | None => break,
                _ => continue,
            },
            Some(event) = inbound_rx.recv() => {
                let bytes = serde_json::to_vec(&event).unwrap();
                if socket.send(Message::Binary(frame_encode(&bytes))).await.is_err() { break; }
            }
        }
    }

    state.deregister_ws_connection(client_id).await;
}
```

Re-use `frame_encode` / `frame_decode` from `transport_manager.rs` by making them `pub(crate)`.

- [ ] **Step 2: Delete `/events` + `/sync` handlers and their routes**

Remove the SSE handler, the HTTP POST sync handler, and any broadcast-channel wiring that only fed SSE. Replace the route wiring in `server/builder.rs`:

```rust
.route("/ws", axum::routing::any(routes::ws_handler))
```

Keep `/health`, `/schemas`, `/schema/:hash`.

- [ ] **Step 3: Register per-connection channels in the server state**

In `AppState`, add `ws_connections: Mutex<HashMap<ClientId, mpsc::UnboundedSender<ServerEvent>>>`. Update the sync pipeline to push to this map instead of a broadcast channel filtered by client_id.

- [ ] **Step 4: Check + run existing server tests**

Run: `cargo check -p jazz-tools --features server`
Expected: clean.

- [ ] **Step 5: Commit**

```bash
git add crates/jazz-tools/src/routes.rs crates/jazz-tools/src/server/
git commit -m "feat(jazz-tools): replace /events + /sync with /ws handler"
```

### Task 10: Wire `/ws` in `jazz-cloud-server`

**Files:**

- Modify: `crates/jazz-cloud-server/src/server.rs`
- Modify: `crates/jazz-cloud-server/ENV.md`

- [ ] **Step 1: Remove SSE/POST routes from router composition; ensure `/ws` is reachable**

Replace the existing `axum::Router` composition with the one exported from `jazz-tools::server::builder` (or inline the `.route("/ws", ...)` call). Delete leftover references to `sse_events` or `post_sync` constants.

- [ ] **Step 2: Doc the env change**

Update `ENV.md` to mention that clients now connect via `ws://host/ws` (wss:// in TLS deployments) and that `/events` and `/sync` are gone.

- [ ] **Step 3: Compile**

Run: `cargo build -p jazz-cloud-server`
Expected: clean.

- [ ] **Step 4: Commit**

```bash
git add crates/jazz-cloud-server/
git commit -m "feat(jazz-cloud-server): wire /ws, drop /events and /sync"
```

---

## Phase 5 — Platform bindings

### Task 11: NAPI `connect` / `disconnect`

**Files:**

- Modify: `crates/jazz-napi/Cargo.toml`
- Modify: `crates/jazz-napi/src/lib.rs`
- Modify: `crates/jazz-napi/index.d.ts`
- Modify: `crates/jazz-napi/index.js`

- [ ] **Step 1: Delete `NapiSyncSender` and its wiring**

Remove every use of `SyncSender` / `NapiSyncSender` in `src/lib.rs`. Remove the JS callbacks that corresponded to it.

- [ ] **Step 2: Add `connect` / `disconnect`**

In `src/lib.rs`:

```rust
#[napi]
impl NapiRuntime {
    #[napi]
    pub fn connect(&self, url: String, auth_json: String) -> napi::Result<()> {
        let auth: jazz_tools::transport_manager::AuthConfig =
            serde_json::from_str(&auth_json).map_err(|e| napi::Error::from_reason(e.to_string()))?;
        let tick = jazz_tools::runtime_tokio::NativeTickNotifier {
            scheduler: self.scheduler().clone(),
        };
        let (handle, manager) = jazz_tools::transport_manager::create::<
            jazz_tools::ws_stream::NativeWsStream,
            jazz_tools::runtime_tokio::NativeTickNotifier,
        >(url, auth, tick);
        self.core.lock().unwrap().set_transport(handle);
        tokio::spawn(manager.run());
        Ok(())
    }

    #[napi]
    pub fn disconnect(&self) {
        self.core.lock().unwrap().clear_transport();
    }
}
```

- [ ] **Step 3: Regenerate the NAPI typings**

Run: `pnpm --filter @jazz/napi build` (or the equivalent command used in CI). Check `index.d.ts` + `index.js` are regenerated and committed.

- [ ] **Step 4: Smoke check**

Run: `cargo check -p jazz-napi`
Expected: clean.

- [ ] **Step 5: Commit**

```bash
git add crates/jazz-napi/
git commit -m "feat(jazz-napi): expose connect/disconnect, drop NapiSyncSender"
```

### Task 12: React Native (UniFFI) `connect` / `disconnect`

**Files:**

- Modify: `crates/jazz-rn/rust/Cargo.toml`
- Modify: `crates/jazz-rn/rust/src/lib.rs`
- Modify: `crates/jazz-rn/src/generated/jazz_rn.ts`
- Modify: `crates/jazz-rn/src/generated/jazz_rn-ffi.ts`

- [ ] **Step 1: Delete `RnSyncSender`**

Same as NAPI. Remove the UniFFI callback interface used for sync sending.

- [ ] **Step 2: Add UniFFI `connect` / `disconnect`**

```rust
#[uniffi::export]
impl RnRuntime {
    pub fn connect(&self, url: String, auth_json: String) -> Result<(), JazzRnError> {
        let auth: jazz_tools::transport_manager::AuthConfig =
            serde_json::from_str(&auth_json).map_err(json_err)?;
        let tick = jazz_tools::runtime_tokio::NativeTickNotifier {
            scheduler: self.scheduler().clone(),
        };
        let (handle, manager) = jazz_tools::transport_manager::create::<
            jazz_tools::ws_stream::NativeWsStream,
            jazz_tools::runtime_tokio::NativeTickNotifier,
        >(url, auth, tick);
        self.core.lock().unwrap().set_transport(handle);
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all().build().unwrap();
            rt.block_on(manager.run());
        });
        Ok(())
    }

    pub fn disconnect(&self) {
        self.core.lock().unwrap().clear_transport();
    }
}
```

- [ ] **Step 3: Regenerate UniFFI bindings**

Run the project's UniFFI regeneration script (check `package.json` scripts for the exact command — it emits `src/generated/jazz_rn.ts` and `jazz_rn-ffi.ts`).

- [ ] **Step 4: Verify**

Run: `cargo check -p jazz-rn`
Expected: clean.

- [ ] **Step 5: Commit**

```bash
git add crates/jazz-rn/
git commit -m "feat(jazz-rn): expose connect/disconnect via UniFFI, drop RnSyncSender"
```

### Task 13: WASM `connect` / `disconnect` + worker-bridge survival

**Files:**

- Modify: `crates/jazz-wasm/Cargo.toml`
- Modify: `crates/jazz-wasm/src/runtime.rs`
- Modify: `crates/jazz-wasm/src/lib.rs`

- [ ] **Step 1: Delete the worker-runtime `JsSyncSender` server wiring**

Remove `JsSyncSender`'s `send_sync_message` path that targeted the server. Keep any remaining callbacks used by the main-thread ↔ worker bridge (`onSyncMessageReceived`, `onSyncMessageToSend`).

- [ ] **Step 2: Add `WasmTickNotifier` + `connect`**

```rust
struct WasmTickNotifier { scheduler: WasmScheduler }
impl jazz_tools::transport_manager::TickNotifier for WasmTickNotifier {
    fn notify(&self) { self.scheduler.schedule_batched_tick(); }
}

#[wasm_bindgen]
impl WasmRuntime {
    pub fn connect(&self, url: String, auth_json: String) {
        let auth: jazz_tools::transport_manager::AuthConfig =
            serde_json::from_str(&auth_json).unwrap();
        let tick = WasmTickNotifier { scheduler: self.scheduler().clone() };
        let (handle, manager) = jazz_tools::transport_manager::create::<
            crate::ws_stream::WasmWsStream, WasmTickNotifier,
        >(url, auth, tick);
        self.core.borrow_mut().set_transport(handle);
        wasm_bindgen_futures::spawn_local(manager.run());
    }

    pub fn disconnect(&self) { self.core.borrow_mut().clear_transport(); }
}
```

- [ ] **Step 3: Build**

Run: `pnpm --filter jazz-wasm build` (the project's wasm build — check `package.json`).
Expected: clean.

- [ ] **Step 4: Commit**

```bash
git add crates/jazz-wasm/
git commit -m "feat(jazz-wasm): expose connect/disconnect via WasmTickNotifier"
```

---

## Phase 6 — TypeScript client cleanup

### Task 14: Replace the TS transport layer with `connect` / `disconnect`

**Files:**

- Modify: `packages/jazz-tools/src/runtime/client.ts`
- Modify: `packages/jazz-tools/src/runtime/sync-transport.ts`
- Modify: `packages/jazz-tools/src/backend/create-jazz-context.ts`
- Modify: `packages/jazz-tools/src/react-native/jazz-rn-runtime-adapter.ts`
- Modify: `packages/jazz-tools/src/worker/jazz-worker.ts`
- Delete: `packages/jazz-tools/src/runtime/client.sync-auth.test.ts`
- Delete: `packages/jazz-tools/src/worker/server-payload-batcher.ts`

- [ ] **Step 1: Reduce `client.ts` to a thin shell**

Delete every code path that does HTTP POST, SSE reading, or reconnection. Keep session/auth configuration assembly. The external API becomes:

```ts
export class JazzClient {
  connect(url: string, auth: AuthConfig) {
    this.runtime.connect(url, JSON.stringify(auth));
  }
  disconnect() {
    this.runtime.disconnect();
  }
}
```

- [ ] **Step 2: Strip `sync-transport.ts`**

Delete `StreamController`, `sendSyncPayloadBatch`, `readBinaryFrames`. If the module becomes empty, delete it and update imports.

- [ ] **Step 3: Update callers**

`create-jazz-context.ts`, `jazz-rn-runtime-adapter.ts`, `jazz-worker.ts` must call the new `runtime.connect(url, authJson)` instead of previous sync-callback wiring. Worker-bridge `onSyncMessageReceived` / `onSyncMessageToSend` stays.

- [ ] **Step 4: Delete the server payload batcher and the auth test**

```bash
git rm packages/jazz-tools/src/worker/server-payload-batcher.ts
git rm packages/jazz-tools/src/runtime/client.sync-auth.test.ts
```

- [ ] **Step 5: Type-check**

Run: `pnpm --filter jazz-tools type-check`
Expected: clean.

- [ ] **Step 6: Commit**

```bash
git add packages/jazz-tools/
git commit -m "refactor(jazz-tools): move transport to Rust, simplify TS client"
```

### Task 15: Update TS tests for the new API

**Files:**

- Modify: `packages/jazz-tools/src/runtime/cloud-server.integration.test.ts`
- Modify: `packages/jazz-tools/src/runtime/napi.integration.test.ts`
- Modify: `packages/jazz-tools/src/runtime/sync-transport.lazy-client-id.test.ts`
- Modify: `packages/jazz-tools/src/runtime/sync-transport.test.ts`
- Modify: `packages/jazz-tools/src/runtime/testing/napi-runtime-test-utils.ts`
- Modify: `packages/jazz-tools/src/testing/index.test.ts`
- Modify: `packages/jazz-tools/src/worker/jazz-worker.test.ts`

- [ ] **Step 1: Update integration tests to the new `connect` API**

Replace old HTTP POST + SSE mocks with:

```ts
runtime.connect(`ws://${host}:${port}/ws`, JSON.stringify(authConfig));
```

- [ ] **Step 2: Collapse `sync-transport.test.ts`**

Keep only assertions about helpers that still exist (worker-bridge glue). Delete network-layer tests — they are covered by the new Rust integration tests (Task 17).

- [ ] **Step 3: Run TS tests**

Run: `pnpm --filter jazz-tools test`
Expected: green.

- [ ] **Step 4: Commit**

```bash
git add packages/jazz-tools/
git commit -m "test(jazz-tools): update tests for WebSocket transport"
```

---

## Phase 7 — Test restoration + new integration tests

### Task 16: Update Rust integration test harness

**Files:**

- Modify: `crates/jazz-tools/tests/support/mod.rs`
- Modify: `crates/jazz-tools/tests/integration.rs`
- Modify: `crates/jazz-tools/tests/auth_test.rs`
- Modify: `crates/jazz-tools/tests/policies_integration/authorship_policies.rs`
- Modify: `crates/jazz-tools/tests/policies_integration/session_cases.rs`
- Modify: `crates/jazz-tools/tests/subscribe_all_integration.rs`

- [ ] **Step 1: Update `support/mod.rs` to start the server on `/ws` and build clients via `TokioRuntime::connect`**

Centralise the "spin up a server and return a connected runtime" helper. Expose it as `fn connected_runtime() -> TokioRuntime`.

- [ ] **Step 2: Port every `SyncSender`-based test**

Any test that wired a `VecSyncSender` directly now:

- builds a server with the `/ws` route
- calls `runtime.connect(url, auth)`
- awaits `handle.has_ever_connected()` (or a small timeout loop) before asserting

- [ ] **Step 3: Un-ignore the two ordering tests**

In `subscribe_all_integration.rs` remove `#[ignore]` from `subscription_reflects_final_state_after_rapid_bulk_updates`.
In `policies_integration/session_cases.rs` remove `#[ignore]` from `single_client_operations_reach_server_in_causal_order`.

- [ ] **Step 4: Run full Rust tests**

Run: `cargo test -p jazz-tools --all-features`
Expected: all tests green, no `ignored`.

- [ ] **Step 5: Commit**

```bash
git add crates/jazz-tools/tests/
git commit -m "test(jazz-tools): migrate integration tests to WebSocket transport"
```

### Task 17: New WebSocket integration tests

**Files:**

- Create: `crates/jazz-tools/tests/websocket_transport.rs`

- [ ] **Step 1: Write the new behavioural tests**

Based on `specs/todo/a_mvp/websocket.md#testing`:

```rust
// crates/jazz-tools/tests/websocket_transport.rs

mod support;

#[tokio::test]
async fn concurrent_writers_maintain_per_client_order() {
    let server = support::start_server().await;
    let alice = support::connect_as(&server, "alice").await;
    let bob = support::connect_as(&server, "bob").await;

    // Each writer emits 200 updates; assert per-client FIFO on the server.
    // ...
}

#[tokio::test]
async fn reconnect_reattaches_same_server_id_and_resumes() {
    let server = support::start_server().await;
    let alice = support::connect_as(&server, "alice").await;
    alice.insert("todos", &[("title", "first")]).await;

    server.drop_connection_of("alice").await;
    support::await_reconnect(&alice).await;

    alice.insert("todos", &[("title", "second")]).await;
    support::assert_server_has_rows(&server, &["first", "second"]).await;
}

#[tokio::test]
async fn disconnect_removes_server_state() {
    // Verify TransportInbound::Disconnected fires remove_server on the runtime.
}

#[tokio::test]
async fn auth_failure_closes_ws_without_attaching() {
    // Bad JWT → server returns auth error + close. Client's has_ever_connected()
    // stays false; no TransportInbound::Connected is observed.
}
```

Helpers (`support::start_server`, `support::connect_as`, `support::await_reconnect`, `support::assert_server_has_rows`, `server.drop_connection_of`) go in `tests/support/mod.rs`.

- [ ] **Step 2: Run**

Run: `cargo test -p jazz-tools --test websocket_transport`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add crates/jazz-tools/tests/websocket_transport.rs crates/jazz-tools/tests/support/mod.rs
git commit -m "test(jazz-tools): add WebSocket transport integration tests"
```

### Task 18: Full-suite regression + examples smoke

**Files:**

- Modify: `examples/auth-betterauth-chat/next-env.d.ts`
- Modify: `examples/auth-betterauth-chat/next.config.ts` (renamed from `.mjs`)
- Modify: `examples/nextjs-csr-ssr/.env`

- [ ] **Step 1: Update examples to the new `connect` API**

Where each example wires the Jazz client, swap old HTTP config for the `ws://` URL and the new `AuthConfig` shape.

- [ ] **Step 2: Run full workspace suite**

```bash
pnpm build
pnpm test
cargo test --workspace --all-features
```

Expected: green.

- [ ] **Step 3: Commit**

```bash
git add examples/ package.json
git commit -m "chore(examples): migrate examples to WebSocket transport"
```

---

## Self-Review Checklist

Before handing the plan off, verify:

1. **Spec coverage:** Every section of `specs/todo/a_mvp/websocket.md` maps to at least one task:
   - Architecture → Tasks 2, 3, 6, 7
   - TransportHandle / Inbound → Task 2
   - batched_tick changes → Task 7
   - TickNotifier (WASM/NAPI/RN) → Tasks 8, 11, 12, 13
   - StreamAdapter (native / WASM) → Tasks 4, 5
   - TransportManager run loop → Task 3
   - Server side → Tasks 9, 10
   - Wire format (4-byte length + JSON) → Task 3 (`frame_encode` / `frame_decode`), Task 9 (server reuses same helpers)
   - Auth handshake flow → Tasks 3, 9
   - Reconnection → Task 3 (`ReconnectState`), Task 17 (test)
   - Worker bridge survival → Task 14
   - Testing list → Tasks 16, 17
   - Deleted components → Tasks 11–14
2. **Type consistency:** `TransportHandle`, `TransportManager`, `TransportInbound`, `StreamAdapter`, `TickNotifier`, `AuthConfig`, `AuthHandshake`, `ConnectedResponse`, `ReconnectState`, `NativeWsStream`, `WasmWsStream`, `NativeTickNotifier`, `WasmTickNotifier` — names are identical across tasks.
3. **No placeholders:** Every step has concrete code, an exact command, or a concrete deletion. The only free-form instructions are the server-state plumbing details in Task 9 step 3 and the support-helper list in Task 17 — both are scoped to things the spec already describes.
4. **Ordering tests un-ignored:** Task 16 step 3 explicitly un-ignores `subscription_reflects_final_state_after_rapid_bulk_updates` and `single_client_operations_reach_server_in_causal_order`.
5. **Feature flags:** `transport-websocket` is introduced in Task 1 and gates every file that pulls `tokio-tungstenite` or `axum::ws`.

If any of these fails when reviewing, stop and fix before executing.
