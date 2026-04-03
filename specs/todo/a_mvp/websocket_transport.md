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

The new design replaces `SyncSender` with channel-based communication. `TransportManager` is a standalone async task that talks to `RuntimeCore` exclusively through two channels. It has no reference to RuntimeCore, no lock, no borrow — just channels in, channels out. This eliminates lock contention between the JS thread and the background transport thread (NAPI/RN) and borrow conflicts (WASM).

```
┌──────────────────────────────────────────────────────────────────┐
│  RuntimeCore<S, Sch>                                             │
│                                                                  │
│  Scheduler (unchanged)                                           │
│  ├── schedule_batched_tick() — platform event loop               │
│  └── batched_tick():                                             │
│       ├── drain inbound channel → park messages                  │
│       ├── drain outbox → outbound channel push                   │
│       └── handle_sync_messages() + immediate_tick()              │
│                                                                  │
│  TransportHandle (replaces SyncSender)                           │
│  ├── outbox_tx: mpsc::UnboundedSender<OutboxEntry>               │
│  └── inbound_rx: mpsc::UnboundedReceiver<InboxEntry>             │
└──────────────────┬───────────────────▲───────────────────────────┘
                   │ channel (out)     │ channel (in)
                   ▼                   │
┌──────────────────────────────────────┴───────────────────────────┐
│  TransportManager<W: StreamAdapter>                              │
│  (async task, spawned per platform — no RuntimeCore reference)   │
│                                                                  │
│  Send loop:                                                      │
│    outbox_rx.recv() → serialize → ws.send()                      │
│    FIFO: channel order = wire order = server arrival             │
│                                                                  │
│  Recv loop:                                                      │
│    ws.recv() → deserialize → inbound_tx.send(entry)              │
│    then notify scheduler via tick_notify                         │
│                                                                  │
│  Reconnection, auth handshake, heartbeat                         │
└──────────────────────────────────────────────────────────────────┘
```

When TransportManager pushes an inbound message to the channel, it also signals the scheduler to run `batched_tick()`. This signal is a per-platform tick notifier (~5 lines) — the only platform-specific piece besides the `StreamAdapter`.

### What changes in RuntimeCore

`RuntimeCore<S: Storage, Sch: Scheduler, Sy: SyncSender>` becomes `RuntimeCore<S: Storage, Sch: Scheduler>`. The `SyncSender` generic is removed. The runtime holds a `TransportHandle` — a concrete type containing both channel endpoints.

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

### TransportHandle

```rust
/// Replaces SyncSender. Concrete type on all platforms.
/// Holds both ends of the transport channels that RuntimeCore uses.
pub struct TransportHandle {
    outbox_tx: mpsc::UnboundedSender<OutboxEntry>,
    inbound_rx: mpsc::UnboundedReceiver<InboxEntry>,
}
```

### batched_tick() changes

```rust
pub fn batched_tick(&mut self) {
    // 1. Drain inbound channel → park messages
    if let Some(ref mut transport) = self.transport {
        while let Ok(msg) = transport.inbound_rx.try_recv() {
            self.parked_sync_messages.push(msg);
        }
    }

    // 2. Drain outbox → push to outbound channel
    let outbox = self.sync_manager_mut().take_outbox();
    if let Some(ref transport) = self.transport {
        for msg in outbox {
            let _ = transport.outbox_tx.send(msg);
        }
    }

    // 3. Process parked sync messages (unchanged)
    self.handle_sync_messages();

    // 4. Flush post-process outbox
    let outbox = self.sync_manager_mut().take_outbox();
    if let Some(ref transport) = self.transport {
        for msg in outbox {
            let _ = transport.outbox_tx.send(msg);
        }
    }

    self.storage.flush_wal();
}
```

### Tick notification

When TransportManager pushes to the inbound channel, it needs to tell the scheduler "there's new data — run batched_tick()". This is a per-platform tick notifier:

```rust
/// Platform-specific: notifies the scheduler that inbound messages arrived.
/// Needed because TransportManager has no reference to RuntimeCore or Scheduler.
pub trait TickNotifier: Send {
    fn notify(&self);
}
```

| Platform | Implementation                                                                                                                                    |
| -------- | ------------------------------------------------------------------------------------------------------------------------------------------------- |
| WASM     | Holds clone of `WasmScheduler` (Rc-based, !Send — but same thread). Calls `schedule_batched_tick()`.                                              |
| NAPI     | Holds clone of `NapiScheduler` (Arc-based, Send). Calls `schedule_batched_tick()` from background thread → TSFN dispatches to JS thread.          |
| RN       | Holds clone of `RnScheduler` (Arc-based, Send). Calls `schedule_batched_tick()` from background thread → UniFFI callback dispatches to JS thread. |

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

When we add multi-stream, `TransportManager` gains stream routing logic (which message types go to which stream), but the **outbound channel and RuntimeHandle are unchanged**. Stream multiplexing is internal to TransportManager.

### TransportManager

The core transport logic. Generic over `StreamAdapter` and `TickNotifier`. Lives in `jazz-tools` core — one implementation shared across all platforms. Has no reference to RuntimeCore.

```rust
pub struct TransportManager<W: StreamAdapter, T: TickNotifier> {
    url: String,
    auth: AuthConfig,
    outbox_rx: mpsc::UnboundedReceiver<OutboxEntry>,
    inbound_tx: mpsc::UnboundedSender<InboxEntry>,
    tick: T,
    reconnect: ReconnectState,
}

impl<W: StreamAdapter, T: TickNotifier> TransportManager<W, T> {
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
        loop {
            select! {
                // Outbound: channel → WebSocket
                msg = self.outbox_rx.recv() => {
                    let Some(entry) = msg else { break }; // channel closed = disconnect
                    let frame = serialize(entry);
                    if ws.send(&frame).await.is_err() { break }
                }
                // Inbound: WebSocket → channel → notify scheduler
                frame = ws.recv() => {
                    match frame {
                        Ok(Some(data)) => {
                            let entry = deserialize(&data);
                            let _ = self.inbound_tx.send(entry);
                            self.tick.notify(); // triggers schedule_batched_tick()
                        }
                        _ => break, // connection closed or error
                    }
                }
            }
        }
    }
}

/// Constructor — returns both halves
pub fn create<W: StreamAdapter, T: TickNotifier>(
    url: String,
    auth: AuthConfig,
    tick: T,
) -> (TransportHandle, TransportManager<W, T>) {
    let (outbox_tx, outbox_rx) = mpsc::unbounded_channel();
    let (inbound_tx, inbound_rx) = mpsc::unbounded_channel();
    let handle = TransportHandle { outbox_tx, inbound_rx };
    let manager = TransportManager {
        url, auth, outbox_rx, inbound_tx, tick,
        reconnect: ReconnectState::new(),
    };
    (handle, manager)
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
  ├── inbound_tx.send(entry)         [channel push]
  └── tick.notify()                  [triggers schedule_batched_tick()]
  │
  ▼ (platform event loop fires)
RuntimeCore.batched_tick()
  ├── inbound_rx.try_recv() loop     [drain inbound channel]
  │     └── push each to parked_sync_messages
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

## Execution model: single-threaded cooperative scheduling (WASM)

WASM in the browser runs on the **same thread as JS**. There are no background threads. `spawn_local` does not create a thread — it registers a Rust `Future` on the JS microtask queue, exactly like `Promise.then()` or `queueMicrotask()`.

A Rust `Future` is a state machine that **pauses** at every `.await` and **resumes** when the awaited thing completes. Between pauses, the JS event loop runs other work (React renders, user input, other promises). All futures take turns on the same thread — they're cooperative, not concurrent.

### How TransportManager runs without threads

`spawn_local(manager.run())` registers the TransportManager's async loop on the microtask queue. The loop uses `select!` to listen to both the outbox channel and the WebSocket simultaneously — not by running two threads, but by registering interest in both wakers. Whichever fires first resumes the future.

### Full cycle: React insert → server → React re-render

```
JS event loop                            WASM (same thread)
─────────────                            ──────────────────

1. User clicks "Add Todo"
   React calls db.insert(...)
   ────────────────────────────────────►
                                          insert() runs SYNCHRONOUSLY:
                                          ├── writes to storage
                                          ├── queues OutboxEntry in SyncManager
                                          └── scheduler.schedule_batched_tick()
                                                └── spawn_local(tick_future)
                                                    [registers on microtask queue,
                                                     does NOT run yet]
   ◄────────────────────────────────────
   insert() returns immediately.
   React re-renders with local state.

2. JS event loop is idle.
   Picks up batched_tick future from microtask queue.
   ────────────────────────────────────►
                                          batched_tick() runs:
                                          └── outbox_tx.send(entry)
                                              [pushes to channel — non-blocking]

                                          Channel push WAKES the TransportManager
                                          future (which was paused in select!).
   ◄────────────────────────────────────

3. JS event loop picks up TransportManager future.
   ────────────────────────────────────►
                                          select! resumes: outbox_rx has a message.
                                          ├── reads OutboxEntry from channel
                                          ├── serializes → binary frame
                                          └── ws.send(data)
                                              [calls web_sys::WebSocket::send()
                                               which is sync in the browser]

                                          Back to select! — nothing pending.
                                          .await PAUSES the future.
   ◄────────────────────────────────────
   Control returns to JS event loop.
   JS can do other things.

4. ...time passes, server processes, sends response...

   Browser WebSocket fires "onmessage".
   This WAKES the TransportManager future.
   ────────────────────────────────────►
                                          select! resumes: ws.recv() has data.
                                          ├── deserializes frame → InboxEntry
                                          ├── inbound_tx.send(entry)
                                          │   [pushes to channel — non-blocking]
                                          └── tick.notify()
                                              └── scheduler.schedule_batched_tick()
                                                  └── spawn_local(tick_future)

                                          Back to select! — .await PAUSES.
   ◄────────────────────────────────────

5. JS event loop picks up batched_tick future.
   ────────────────────────────────────►
                                          batched_tick() runs:
                                          ├── inbound_rx.try_recv() → got entry
                                          │   pushes to parked_sync_messages
                                          ├── handle_sync_messages()
                                          ├── immediate_tick()
                                          │   └── subscription callbacks fire
                                          └── React hook dispatch() called
   ◄────────────────────────────────────
   React re-renders with server data.
```

### Why this works without races

With threads, two things run **simultaneously** and need locks (`Mutex`). With `spawn_local`, futures run **interleaved** on the same thread — they take turns. Only one piece of Rust code executes at any moment. That's why WASM uses `Rc<RefCell>` (not `Arc<Mutex>`) — there's no concurrent access, just cooperative scheduling.

The `select!` macro is the key primitive: it registers wakers on multiple sources (channel + WebSocket) and pauses the future until any one of them fires. This is how TransportManager "listens" to both directions without threads.

### NAPI and React Native: real threads, no lock contention

In NAPI and React Native, `TransportManager` runs on a real async runtime (`tokio::spawn` or a background thread). Because TransportManager only touches channels (no RuntimeCore reference), there is **zero lock contention** between the JS thread and the background transport thread. Both channels are `Send` — `OutboxEntry` and `InboxEntry` are plain data types (no `Rc`, no `RefCell`).

The `TickNotifier` calls `scheduler.schedule_batched_tick()` from the background thread. In NAPI this dispatches to the JS thread via `ThreadsafeFunction`. In RN this dispatches via the UniFFI callback interface (`BatchedTickCallback: Send + Sync`). Both are designed for cross-thread calls.

## Platform integration

### WASM (browser)

```rust
// crates/jazz-wasm/src/runtime.rs

#[wasm_bindgen]
impl WasmRuntime {
    pub fn connect(&self, url: String, auth_json: String) {
        let auth: AuthConfig = serde_json::from_str(&auth_json).unwrap();
        let tick = WasmTickNotifier { scheduler: self.scheduler().clone() };
        let (transport, manager) =
            transport::create::<WasmWsStream, WasmTickNotifier>(url, auth, tick);
        self.core.borrow_mut().set_transport(transport);
        wasm_bindgen_futures::spawn_local(manager.run());
    }

    pub fn disconnect(&self) {
        self.core.borrow_mut().clear_transport();
    }
}

/// TickNotifier for WASM — calls WasmScheduler directly (same thread)
struct WasmTickNotifier {
    scheduler: WasmScheduler,  // Clone: Rc<RefCell<bool>> + Weak<RefCell<Core>>
}

impl TickNotifier for WasmTickNotifier {
    fn notify(&self) {
        self.scheduler.schedule_batched_tick();
    }
}
```

`WasmWsStream` wraps `web-sys::WebSocket` (~30 LOC):

- `connect()` → `WebSocket::new(url)`, await `onopen`
- `send()` → `WebSocket::send_with_u8_array()`
- `recv()` → `onmessage` events bridged to an async stream
- `close()` → `WebSocket::close()`

### NAPI (Node.js)

```rust
// crates/jazz-napi/src/lib.rs

#[napi]
impl NapiRuntime {
    #[napi]
    pub fn connect(&self, url: String, auth_json: String) {
        let auth: AuthConfig = serde_json::from_str(&auth_json).unwrap();
        let tick = NativeTickNotifier { scheduler: self.scheduler().clone() };
        let (transport, manager) =
            transport::create::<NativeWsStream, NativeTickNotifier>(url, auth, tick);
        self.core.lock().unwrap().set_transport(transport);
        tokio::spawn(manager.run());
    }

    #[napi]
    pub fn disconnect(&self) {
        self.core.lock().unwrap().clear_transport();
    }
}
```

### React Native (UniFFI)

```rust
// crates/jazz-rn/rust/src/lib.rs

#[uniffi::export]
impl RnRuntime {
    pub fn connect(&self, url: String, auth_json: String) -> Result<(), JazzRnError> {
        let auth: AuthConfig = serde_json::from_str(&auth_json).map_err(json_err)?;
        let tick = NativeTickNotifier { scheduler: self.scheduler().clone() };
        let (transport, manager) =
            transport::create::<NativeWsStream, NativeTickNotifier>(url, auth, tick);
        self.core.lock().unwrap().set_transport(transport);
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all().build().unwrap();
            rt.block_on(manager.run());
        });
        Ok(())
    }
}
```

Same `NativeWsStream` and `NativeTickNotifier` as NAPI. Only difference: spawns a dedicated thread with tokio runtime.

```rust
/// TickNotifier for NAPI + React Native — calls scheduler across threads
/// NapiScheduler uses ThreadsafeFunction, RnScheduler uses UniFFI callback.
/// Both are Send + Sync — designed for cross-thread calls.
struct NativeTickNotifier {
    scheduler: NativeScheduler,  // Clone: Arc<AtomicBool> + Arc<Mutex<Callback>>
}

impl TickNotifier for NativeTickNotifier {
    fn notify(&self) {
        self.scheduler.schedule_batched_tick();
    }
}
```

### Native (server-to-server, integration tests)

```rust
// crates/jazz-tools/src/transport.rs — same pattern as NAPI

let tick = NativeTickNotifier { scheduler: scheduler.clone() };
let (transport, manager) =
    transport::create::<NativeWsStream, NativeTickNotifier>(url, auth, tick);
core.lock().unwrap().set_transport(transport);
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

## Design rationale: channels both directions, no RuntimeCore reference

TransportManager communicates with RuntimeCore **exclusively through channels**. It holds no reference to RuntimeCore, no Mutex, no RefCell. This means:

- **Zero lock contention** (NAPI/RN) — background transport thread and JS thread never contend on the same lock.
- **Zero borrow conflicts** (WASM) — TransportManager's async loop can't conflict with `batched_tick()`.
- **TransportManager is fully `Send`** — it only holds channels, a stream adapter, and a tick notifier. Works on any thread, any platform.
- **Testable in isolation** — feed outbox channel, drain inbound channel, assert behavior without a RuntimeCore.

**Outbound channel:** `batched_tick()` is sync, `ws.send()` is async — the channel bridges the gap.

**Inbound channel:** TransportManager pushes received messages to a channel. `batched_tick()` drains it via `try_recv()` loop into `parked_sync_messages`. The `TickNotifier` tells the scheduler to run `batched_tick()` when new messages arrive.

**Trade-off accepted:** Channels use unbounded `mpsc` — no capacity limit, but memory grows if the consumer falls behind. In practice: outbound grows if the network is slow (same as today — outbox grows), inbound grows if `batched_tick()` is slow (unlikely — it's a tight loop). If needed, bounded channels with backpressure can be added later.

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
