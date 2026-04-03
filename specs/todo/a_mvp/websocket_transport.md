# WebSocket Transport — Rust-Owned Connection

Replaces the [HTTP/SSE Transport](../../status-quo/http_transport.md) with a bidirectional WebSocket connection owned by Rust across all platforms (WASM, NAPI, React Native, native). The transport logic — framing, reconnection, auth handshake, heartbeat — lives once in `jazz-tools` core Rust. Each platform provides only a thin `WebSocketAdapter` (~30 LOC).

This spec defines the architecture, data flows, and platform integration for the WebSocket transport. It does not cover WebTransport (future upgrade that slots into the same abstraction).

## Problem recap

The current SSE + HTTP POST transport has three issues:

1. **Ordering** — outgoing writes are independent HTTP POSTs that can arrive out of order. This bypasses policy enforcement and corrupts subscriber state. Two tests are `#[ignore]`d: `subscription_reflects_final_state_after_rapid_bulk_updates` and `single_client_operations_reach_server_in_causal_order`.
2. **Encoding overhead** — SSE is UTF-8, forcing base64 for binary data (+33% payload size).
3. **Split ownership** — JS owns the network (HTTP POST + SSE consumption) while Rust owns the sync logic. Transport code is duplicated across TypeScript (`sync-transport.ts`, 650+ LOC), three Rust `SyncSender` impls (`JsSyncSender`, `NapiSyncSender`, `RnSyncSender`), and the Rust HTTP client (`transport.rs`).

## Architecture

### Core principle: Rust owns the connection

Today `RuntimeCore<S, Sch, Sy>` is generic over `SyncSender` — a trait that hands messages to JS via callbacks. JS then does the actual network I/O. This means transport logic is split between Rust (outbox draining) and JS (HTTP POST, SSE parsing, reconnection).

The new design replaces `SyncSender` with a channel-based `TransportHandle`. A separate `TransportManager` async task owns the WebSocket connection and runs send/recv loops. All transport logic — framing, reconnection, auth, heartbeat — lives in Rust.

```
┌─────────────────────────────────────────────────────────────────┐
│  RuntimeCore<S, Sch>                                            │
│                                                                 │
│  Scheduler (unchanged)                                          │
│  ├── schedule_batched_tick() — platform event loop              │
│  └── batched_tick():                                            │
│       ├── drain outbox → TransportHandle.send() [channel push]  │
│       └── process parked inbound messages                       │
│                                                                 │
│  TransportHandle (replaces SyncSender)                          │
│  └── send(OutboxEntry) → mpsc::UnboundedSender                  │
│       non-blocking, FIFO — preserves write order                │
│                                                                 │
│  park_sync_message(InboxEntry) ← called by TransportManager     │
└──────────────────┬───────────────────┬──────────────────────────┘
                   │ channel (outbound)│ direct call (inbound)
                   ▼                   │
┌──────────────────────────────────────┴──────────────────────────┐
│  TransportManager<W: WebSocketAdapter>                          │
│  (async task, spawned per platform)                             │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │ Send loop                                               │    │
│  │   mpsc::Receiver<OutboxEntry> → serialize → ws.send()   │    │
│  │   FIFO: channel order = wire order = server arrival     │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │ Recv loop                                               │    │
│  │   ws.recv() → deserialize → runtime.park_sync_message() │    │
│  │   → scheduler.schedule_batched_tick()                    │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                                                 │
│  Reconnection state machine                                     │
│  Auth handshake (first message after connect)                   │
│  Heartbeat (WebSocket ping/pong)                                │
└─────────────────────────────────────────────────────────────────┘
```

### What changes in RuntimeCore

`RuntimeCore<S: Storage, Sch: Scheduler, Sy: SyncSender>` becomes `RuntimeCore<S: Storage, Sch: Scheduler>`. The `SyncSender` generic parameter is removed. The runtime holds a `TransportHandle` (concrete type, not generic — it's just a channel sender).

```rust
// Before
pub struct RuntimeCore<S: Storage, Sch: Scheduler, Sy: SyncSender> {
    scheduler: Sch,
    sync_sender: Sy,
    // ...
}

// After
pub struct RuntimeCore<S: Storage, Sch: Scheduler> {
    scheduler: Sch,
    transport: Option<TransportHandle>,  // None until connect()
    // ...
}
```

`batched_tick()` changes from calling `self.sync_sender.send_sync_message(msg)` to calling `self.transport.send(msg)` — which is a non-blocking channel push.

### TransportHandle

```rust
/// Replaces SyncSender. Concrete type on all platforms — just a channel sender.
pub struct TransportHandle {
    outbox_tx: mpsc::UnboundedSender<OutboxEntry>,
}

impl TransportHandle {
    pub fn send(&self, msg: OutboxEntry) {
        // Non-blocking push. If the receiver is dropped (connection lost),
        // messages buffer until reconnection establishes a new channel.
        let _ = self.outbox_tx.send(msg);
    }
}
```

### StreamAdapter trait

Thin platform abstraction — only the raw bidirectional stream primitives. Everything else (framing, auth, reconnection) lives in `TransportManager`. Named `StreamAdapter` (not `WebSocketAdapter`) because the same trait works for WebSocket and single-stream WebTransport.

```rust
/// Platform-specific bidirectional byte stream.
///
/// Implementations (v1 — WebSocket):
/// - NativeWsStream: tokio-tungstenite (NAPI, React Native, server, tests)
/// - WasmWsStream: web-sys::WebSocket via ws_stream_wasm (browser WASM)
///
/// Future implementations (WebTransport):
/// - NativeWtStream: wtransport single bidirectional stream
/// - WasmWtStream: browser WebTransport API single bidirectional stream
pub trait StreamAdapter: Sized {
    type Error: std::fmt::Display;

    /// Open a connection to the given URL.
    async fn connect(url: &str) -> Result<Self, Self::Error>;

    /// Send a binary frame.
    async fn send(&mut self, data: &[u8]) -> Result<(), Self::Error>;

    /// Receive the next binary frame. Returns None on clean close.
    async fn recv(&mut self) -> Result<Option<Vec<u8>>, Self::Error>;

    /// Close the connection.
    async fn close(&mut self);
}
```

**v1 implementations (WebSocket):**

| Platform               | Adapter                 | Crate                             | LOC estimate |
| ---------------------- | ----------------------- | --------------------------------- | ------------ |
| Native (server, tests) | `NativeWsStream`        | `tokio-tungstenite`               | ~30          |
| NAPI (Node.js)         | `NativeWsStream` (same) | `tokio-tungstenite`               | reuse        |
| React Native           | `NativeWsStream` (same) | `tokio-tungstenite`               | reuse        |
| WASM (browser)         | `WasmWsStream`          | `ws_stream_wasm` or raw `web-sys` | ~30          |

Three out of four platforms use the same `NativeWsStream` implementation. Only WASM needs a separate adapter because it can't use TCP sockets — it must go through the browser's WebSocket API.

**WebTransport upgrade path:**

Single-stream WebTransport implements the same `StreamAdapter` trait — `connect()` opens a session and one bidirectional stream, `send()`/`recv()` operate on that stream. Same `TransportManager`, same channels, same `batched_tick()`. Just a different adapter.

Multi-stream WebTransport (dedicated streams for large data transfer vs sync control) requires a richer trait:

```rust
/// Future: multi-stream capable adapter
pub trait MultiStreamAdapter: Sized {
    type Error: std::fmt::Display;

    async fn connect(url: &str) -> Result<Self, Self::Error>;
    async fn open_stream(&mut self, purpose: StreamPurpose) -> Result<StreamHandle, Self::Error>;
    async fn send_on(&mut self, stream: StreamHandle, data: &[u8]) -> Result<(), Self::Error>;
    async fn recv_any(&mut self) -> Result<(StreamHandle, Vec<u8>), Self::Error>;
    async fn close(&mut self);
}
```

When we add multi-stream, `TransportManager` gains stream routing logic (which message types go to which stream), but the **channel architecture is unchanged** — RuntimeCore still pushes to `outbox_tx` and drains `inbound_rx`. Stream multiplexing is internal to TransportManager.

### TransportManager

The core transport logic. Generic over `WebSocketAdapter`, lives in `jazz-tools` core.

```rust
pub struct TransportManager<W: WebSocketAdapter> {
    url: String,
    auth: AuthConfig,
    outbox_rx: mpsc::UnboundedReceiver<OutboxEntry>,
    // Weak reference to RuntimeCore for parking inbound messages
    runtime: RuntimeRef,
    // Reconnection state
    reconnect: ReconnectState,
}

impl<W: WebSocketAdapter> TransportManager<W> {
    /// Main loop. Runs until explicitly stopped.
    pub async fn run(mut self) {
        loop {
            match W::connect(&self.url).await {
                Ok(ws) => {
                    self.send_auth_handshake(&ws).await;
                    self.run_connected(ws).await;
                    // Connection lost — fall through to reconnect
                }
                Err(e) => { /* log error */ }
            }
            self.reconnect.backoff().await;
        }
    }

    async fn run_connected(&mut self, mut ws: W) {
        // Two concurrent loops via select!:
        // 1. outbox_rx.recv() → serialize frame → ws.send()
        // 2. ws.recv() → deserialize → runtime.park_sync_message()
    }
}
```

## Data Flows

### Flow 1: Outbound write (client → server)

```
JS calls runtime.insert("todos", {title: "buy milk"})
  │
  ▼
RuntimeCore.insert()
  ├── Storage: commit locally
  ├── SyncManager: queue ObjectUpdated in outbox
  └── Scheduler.schedule_batched_tick()
  │
  ▼ (platform event loop fires)
RuntimeCore.batched_tick()
  ├── drain outbox
  └── for each OutboxEntry:
        TransportHandle.send(entry)
          └── mpsc::Sender.send(entry)  [non-blocking, FIFO]
  │
  ▼ (TransportManager send loop)
TransportManager.outbox_rx.recv()
  ├── serialize OutboxEntry → binary frame (4-byte length + JSON)
  └── ws.send(frame)  [WebSocket preserves order]
  │
  ▼
Server receives frame in order
```

**Ordering guarantee chain:** RuntimeCore drains outbox in insertion order → channel is FIFO → TransportManager sends in recv order → WebSocket delivers in send order → server processes in arrival order. No reordering possible at any step.

### Flow 2: Inbound update (server → client)

```
Server sends ServerEvent (SyncUpdate, Connected, etc.)
  │
  ▼
TransportManager recv loop:
  ws.recv() → binary frame
  ├── deserialize → InboxEntry
  ├── runtime.park_sync_message(entry)
  └── scheduler.schedule_batched_tick()
  │
  ▼ (platform event loop fires)
RuntimeCore.batched_tick()
  ├── handle_sync_messages()
  │     └── apply parked messages → push to SyncManager inbox
  └── immediate_tick()
        └── subscription callbacks fire with deltas
```

### Flow 3: Connection lifecycle

```
JS calls runtime.connect(url, auth_config)
  │
  ▼
Platform crate creates:
  1. TransportHandle (channel sender) → given to RuntimeCore
  2. TransportManager (channel receiver + adapter) → spawned as async task
  │
  ▼
TransportManager.run() loop:
  ┌──────────────────────────────────┐
  │ W::connect(url)                  │
  │   │                              │
  │   ├── success:                   │
  │   │   send_auth_handshake()      │
  │   │   run_connected()            │◀─┐
  │   │     ├── send loop            │  │
  │   │     └── recv loop            │  │
  │   │   connection drops ──────────┼──┘ (retry)
  │   │                              │
  │   └── failure:                   │
  │       reconnect.backoff()        │
  │       retry ─────────────────────┼──┐
  │                                  │  │
  └──────────────────────────────────┘  │
                                        ▼
                                    (loop forever)
```

### Flow 4: Authentication handshake

Currently, auth is carried via HTTP headers on each POST request and SSE connection. With WebSocket, auth happens once as the first message after connection open.

```
Client                                    Server
  │                                         │
  │── WebSocket upgrade ───────────────────>│
  │<── 101 Switching Protocols ─────────────│
  │                                         │
  │── AuthHandshake frame ────────────────>│
  │   {                                     │
  │     "client_id": "<uuid>",              │
  │     "auth": {                           │
  │       "type": "jwt" | "backend" | ...,  │
  │       "token": "...",                   │
  │       "session": {...},                 │
  │       "admin_secret": "..."             │
  │     },                                  │
  │     "catalogue_state_hash": "..."       │
  │   }                                     │
  │                                         │
  │<── Connected frame ────────────────────│
  │   {                                     │
  │     "connection_id": "<uuid>",          │
  │     "client_id": "<uuid>",             │
  │     "catalogue_state_hash": "..."       │
  │   }                                     │
  │                                         │
  │── SyncBatchRequest frames ────────────>│
  │<── ServerEvent frames ─────────────────│
  │     (bidirectional from here)           │
```

### Flow 5: Reconnection

```
Connection drops (network error, server restart, etc.)
  │
  ▼
TransportManager detects:
  ws.recv() returns error or ws.send() fails
  │
  ├── Outbox channel stays alive — messages buffer
  │   (TransportHandle.send() doesn't fail, just queues)
  │
  ├── ReconnectState.backoff():
  │   attempt 1: 300ms + random(0..200ms)
  │   attempt 2: 600ms + random(0..200ms)
  │   attempt 3: 1200ms + random(0..200ms)
  │   ...capped at 10s + random(0..200ms)
  │
  ▼
W::connect(url) succeeds
  │
  ├── send_auth_handshake() — same client_id for incremental sync
  ├── drain buffered outbox (messages queued during disconnect)
  └── run_connected() — resume normal send/recv loops
```

Key property: messages queued by RuntimeCore during disconnection are not lost. The `mpsc` channel buffers them. On reconnect, the send loop drains the buffer in order before processing new messages.

### Flow 6: Disconnect

```
JS calls runtime.disconnect()
  │
  ▼
TransportHandle is dropped
  ├── mpsc::Sender drops → mpsc::Receiver yields None
  │
  ▼
TransportManager.outbox_rx.recv() returns None
  ├── ws.close()
  └── run() exits
```

## Platform integration

### WASM (browser)

```rust
// crates/jazz-wasm/src/runtime.rs — ~10 lines of transport glue

#[wasm_bindgen]
impl WasmRuntime {
    pub fn connect(&self, url: String, auth_json: String) {
        let auth: AuthConfig = serde_json::from_str(&auth_json).unwrap();
        let (handle, manager) = transport::create::<WasmWebSocket>(url, auth, self.core_ref());
        self.core.borrow_mut().set_transport(handle);
        wasm_bindgen_futures::spawn_local(manager.run());
    }

    pub fn disconnect(&self) {
        self.core.borrow_mut().clear_transport();
    }
}
```

`WasmWebSocket` wraps `web-sys::WebSocket`:

- `connect()` → `WebSocket::new(url)`, wait for `onopen`
- `send()` → `WebSocket::send_with_u8_array()`
- `recv()` → `onmessage` events bridged to an async stream
- `close()` → `WebSocket::close()`

### NAPI (Node.js)

```rust
// crates/jazz-napi/src/lib.rs — ~10 lines of transport glue

#[napi]
impl NapiRuntime {
    #[napi]
    pub fn connect(&self, url: String, auth_json: String) {
        let auth: AuthConfig = serde_json::from_str(&auth_json).unwrap();
        let (handle, manager) = transport::create::<NativeWebSocket>(url, auth, self.core_ref());
        self.core.lock().unwrap().set_transport(handle);
        tokio::spawn(manager.run());
    }

    #[napi]
    pub fn disconnect(&self) {
        self.core.lock().unwrap().clear_transport();
    }
}
```

`NativeWebSocket` wraps `tokio-tungstenite`. Same implementation used for native server and tests.

### React Native (UniFFI)

```rust
// crates/jazz-rn/rust/src/lib.rs — ~10 lines of transport glue

#[uniffi::export]
impl RnRuntime {
    pub fn connect(&self, url: String, auth_json: String) -> Result<(), JazzRnError> {
        let auth: AuthConfig = serde_json::from_str(&auth_json).map_err(json_err)?;
        let (handle, manager) = transport::create::<NativeWebSocket>(url, auth, self.core_ref());
        self.core.lock().unwrap().set_transport(handle);
        // React Native: spawn on a background thread with a tokio runtime
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(manager.run());
        });
        Ok(())
    }
}
```

Same `NativeWebSocket` adapter as NAPI. The only difference is how the async task is spawned (dedicated thread with tokio runtime, since React Native's main thread is the JS event loop).

### Native (server-to-server, integration tests)

```rust
// crates/jazz-tools/src/transport.rs — same pattern

let (handle, manager) = transport::create::<NativeWebSocket>(url, auth, core_ref);
core.set_transport(handle);
tokio::spawn(manager.run());
```

## Server side

The server replaces `GET /events` (SSE) and `POST /sync` with a single `/ws` endpoint.

```rust
// crates/jazz-tools/src/routes.rs

async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
) -> Response {
    ws.on_upgrade(|socket| handle_ws_connection(socket, state))
}

async fn handle_ws_connection(mut socket: WebSocket, state: AppState) {
    // 1. Read first message: AuthHandshake
    let auth = read_auth_handshake(&mut socket).await;

    // 2. Authenticate (same logic as current middleware)
    let session = authenticate(auth, &state).await;

    // 3. Register connection with SyncManager
    let (client_id, connection) = state.register_connection(session).await;

    // 4. Send Connected response
    socket.send(Connected { connection_id, client_id, ... }).await;

    // 5. Bidirectional loop
    loop {
        select! {
            // Client → Server: receive sync frames
            Some(msg) = socket.recv() => {
                let payloads = deserialize_sync_batch(msg);
                for payload in payloads {
                    state.process_sync_payload(client_id, payload).await;
                }
            }
            // Server → Client: send updates from per-connection channel
            Some(event) = connection.outbox.recv() => {
                socket.send(serialize_server_event(event)).await;
            }
        }
    }
}
```

The broadcast channel currently used for SSE gets replaced with a per-connection `mpsc` channel. The server pushes `ServerEvent`s directly to each connection's channel instead of broadcasting and filtering by `client_id`.

## Wire format

Unchanged. Both directions use the same binary length-prefixed frames:

```
[4 bytes: u32 BE length][N bytes: JSON payload]
```

The transport is now binary-native (WebSocket binary frames), so the base64 encoding overhead on SSE disappears. The JSON payload format stays for now — binary serialization (MessagePack, etc.) is a future optimization that's now unblocked by the transport.

## Inbound message flow: channels, not direct RuntimeRef

The `TransportManager` never holds a reference to `RuntimeCore`. Instead it communicates via channels:

```
TransportManager                    RuntimeCore
     │                                   │
     │── outbox_rx ◄── outbox_tx ────────│  (outbound: batched_tick pushes here)
     │                                   │
     │── inbound_tx ───► inbound_rx ─────│  (inbound: batched_tick drains here)
     │                                   │
```

**Outbound:** `batched_tick()` drains the SyncManager outbox and pushes each `OutboxEntry` to `outbox_tx`. TransportManager's send loop reads `outbox_rx` and writes to WebSocket.

**Inbound:** TransportManager's recv loop reads from WebSocket and pushes each `InboxEntry` to `inbound_tx`. `batched_tick()` drains `inbound_rx` (via `try_recv()` loop) and parks each message. A small per-platform bridge (~5 lines) watches the inbound channel and calls `scheduler.schedule_batched_tick()` when new messages arrive.

This avoids the `RuntimeRef` problem entirely:

- No `Rc<RefCell<RuntimeCore>>` crossing async boundaries in WASM
- No borrow conflicts between TransportManager recv loop and `batched_tick()`
- No `Arc<Mutex<RuntimeCore>>` contention in NAPI/RN
- `TransportManager` is purely `Send` — it only holds channels and the WebSocket adapter

The per-platform bridge that triggers ticks on inbound messages:

```rust
// WASM: ~5 lines
spawn_local(async move {
    while inbound_notify_rx.recv().await.is_some() {
        scheduler.schedule_batched_tick();
    }
});

// NAPI/RN: ~5 lines
tokio::spawn(async move {
    while inbound_notify_rx.recv().await.is_some() {
        scheduler.schedule_batched_tick();
    }
});
```

### `batched_tick()` changes

```rust
pub fn batched_tick(&mut self) {
    // NEW: drain inbound channel first
    if let Some(ref transport) = self.transport {
        while let Ok(msg) = transport.inbound_rx.try_recv() {
            self.parked_sync_messages.push(msg);
        }
    }

    // EXISTING: drain outbox → send via channel (replaces SyncSender callback)
    let outbox = self.sync_manager_mut().take_outbox();
    if let Some(ref transport) = self.transport {
        for msg in outbox {
            let _ = transport.outbox_tx.send(msg);
        }
    }

    // EXISTING: process parked sync messages
    self.handle_sync_messages();

    // EXISTING: flush post-process outbox
    let outbox = self.sync_manager_mut().take_outbox();
    if let Some(ref transport) = self.transport {
        for msg in outbox {
            let _ = transport.outbox_tx.send(msg);
        }
    }

    self.storage.flush_wal();
}
```

## Worker bridge (browser): `onSyncMessageReceived()` stays for inter-runtime sync

The browser architecture runs **two WASM runtimes**: a persistent one in a dedicated worker (OPFS storage + server connection) and a lightweight one on the main thread (in-memory). They exchange sync payloads via `postMessage`.

With Rust-owned transport, the **WebSocket lives in the worker's WASM runtime** — the worker's `TransportManager` handles the server connection. But the worker-to-main-thread bridge still uses `postMessage` and `onSyncMessageReceived()` to forward sync payloads to the main thread runtime.

**What changes:** The worker no longer calls JS to send HTTP POST or read SSE. It calls `runtime.connect(url, auth)` and Rust handles the rest.

**What stays:** `onSyncMessageReceived()` on the main thread runtime — it's still called by the worker bridge (via `postMessage`), not by network code. Similarly, `onSyncMessageToSend()` callback stays for the worker bridge's main-to-worker direction (main thread mutations need to reach the worker for server sync).

This means the WASM API surface is:

- `connect(url, auth)` / `disconnect()` — **NEW**, replaces JS network code
- `onSyncMessageReceived()` — **STAYS**, used by worker bridge (main thread receives from worker)
- `onSyncMessageToSend()` — **STAYS**, used by worker bridge (main thread sends to worker)
- `onSyncMessageReceivedFromClient()` — **STAYS**, for peer/client sync

The `SyncSender` trait and `JsSyncSender` are still deleted from the **worker runtime** (which connects to the server). The main thread runtime keeps them for the worker bridge.

Alternative: unify the worker bridge under the same `TransportHandle` channel abstraction (a `PostMessageAdapter` instead of `WebSocketAdapter`). This is a clean future step but not required for v1.

## What gets deleted

| File / Component                                    | What                                              | Why                                                            |
| --------------------------------------------------- | ------------------------------------------------- | -------------------------------------------------------------- |
| `JsSyncSender` (jazz-wasm, **worker runtime only**) | WASM callback-based sync sender                   | Replaced by `TransportHandle` channels                         |
| `NapiSyncSender` (jazz-napi)                        | NAPI callback-based sync sender                   | Replaced by `TransportHandle` channels                         |
| `RnSyncSender` (jazz-rn)                            | UniFFI callback-based sync sender                 | Replaced by `TransportHandle` channels                         |
| `SyncSender` trait                                  | Generic parameter on RuntimeCore                  | Replaced by concrete `TransportHandle`                         |
| Network-facing JS in `sync-transport.ts`            | HTTP POST sender, SSE stream parser, reconnection | Transport logic moves to Rust                                  |
| `StreamController`                                  | TS SSE reconnection logic                         | Replaced by Rust `TransportManager` reconnection               |
| `sendSyncPayloadBatch()`                            | TS HTTP POST sender                               | Replaced by Rust WebSocket send loop                           |
| `readBinaryFrames()`                                | TS binary SSE frame parser                        | No longer needed — WebSocket frames are message-boundary-aware |
| `GET /events` route                                 | SSE endpoint                                      | Replaced by `/ws`                                              |
| `POST /sync` route                                  | HTTP sync endpoint                                | Replaced by `/ws`                                              |
| `reqwest`-based transport                           | Rust HTTP client                                  | Replaced by `NativeWebSocket`                                  |

## What stays

| Component                                                   | Why                                                                              |
| ----------------------------------------------------------- | -------------------------------------------------------------------------------- |
| `Scheduler` trait + all impls                               | Still needed — tick scheduling is platform-specific                              |
| `batched_tick()` / `immediate_tick()`                       | Core processing loop is unchanged                                                |
| `park_sync_message()` / `park_sync_message_with_sequence()` | Inbound message parking stays — TransportManager calls it directly instead of JS |
| `SyncManager` / outbox / inbox                              | Sync logic is transport-agnostic                                                 |
| Binary frame format (4-byte length + JSON)                  | Wire format is unchanged                                                         |
| `transport_protocol.rs` types                               | `ServerEvent`, `SyncBatchRequest`, `SyncPayload` — all stay                      |
| Auth logic in `middleware/auth.rs`                          | Reused in WebSocket handshake handler                                            |
| `/health` endpoint                                          | Stays for load balancer health checks                                            |
| `/schemas`, `/schema/:hash`                                 | Admin schema endpoints stay as HTTP GET                                          |

## Testing

- Un-ignore `subscription_reflects_final_state_after_rapid_bulk_updates` — the north-star test
- Un-ignore `single_client_operations_reach_server_in_causal_order` — policy ordering test
- New: `concurrent_writers_maintain_per_client_order` — multi-client ordering
- All existing sync/subscribe integration tests must pass unchanged
- New: reconnection test — kill WebSocket mid-sync, verify ordered resume
