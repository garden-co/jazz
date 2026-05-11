# Rust-Owned Worker Bridge — Implementation Spec

This is a clean-room, prescriptive spec. An implementer reading only this
document should be able to build the feature without consulting the existing
code. Every method signature, every wire variant, every state, every timeout,
every error path is named here.

Source of truth precedence: where this document conflicts with the legacy
`spec.md` sibling (the original planning doc), this document wins.

## 0. Goal

Move the browser dedicated-worker bridge — the `SyncSender` JS callback, the
init handshake state machine, the peer routing table, lifecycle plumbing, and
the protocol envelope — out of TypeScript and into the `jazz-wasm` Rust crate.

JavaScript retains exactly two responsibilities:

1. Constructing the `Worker` object (URL resolution is a bundler concern).
2. A worker-side bootstrap shim that loads WASM and hands off to Rust.

The public `Db` API and the on-the-wire structured-clone payload semantics are
unchanged. The TypeScript `WorkerBridge` survives as a thin adapter so
`db.ts` does not need to change.

## 1. Non-Goals

- No changes to NAPI or React Native bindings.
- No semantic changes to `RuntimeCore` or anything in `jazz-tools` beyond a
  single targeted edit (see §3).
- No new on-the-wire shape for the worker `postMessage` channel beyond what
  this doc specifies. Receivers outside `jazz-wasm`/`jazz-tools` do not exist.
- No changes to the upstream WebSocket transport (`runtime.connect`).
- No changes to leader-election (`tab-leader-election.ts`) or the
  BroadcastChannel sync protocol used between tabs.
- No support for a third-party worker entry point that hosts the WASM runtime.

## 2. Architecture Overview

```text
Main thread                                    Worker thread
-----------                                    -------------

JS:  worker = new Worker(url, { type: "module" })  ── postMessage ──▶  JS shim
                                                                       │
JS:  new WorkerBridge(worker, runtime)                                 │  posts "ready"
       .onPeerSync(...) / .on*(...) / ...                              │  buffers "init"
       .init(options)  ──▶  WasmRuntime.createWorkerBridge(...)        │  loads WASM
         │                                                             │  wasmModule.runAsWorker(init, pending)
         ▼
Rust:  WasmWorkerBridge.attach(worker, runtime, options)               ▼
       - install RustOutboxSender on RuntimeCore             Rust:  WorkerHost (thread_local)
       - install worker.onmessage (Rust closure)                    - install self.onmessage (Rust closure)
       - post {type:"init", ...} JS-object envelope                 - open WasmRuntime (persistent → ephemeral fallback)
                                                                    - bootstrap catalogue (addServer/removeServer)
                                                                    - connect upstream (WebSocket)
                                                                    - drain pre-init pending buffer
                                                                    - post InitOk
                                          ◀── postMessage ──────────
```

Everything except `init` (main → worker) and `ready`/`error` (worker → main)
is a single `Uint8Array` carrying postcard-encoded enum bytes. The
`ArrayBuffer` is added to the `postMessage` transfer list so the browser
detaches it on send.

## 3. Pre-flight Edit to `jazz-tools`

Exactly one mechanical edit in `jazz-tools` makes the rest possible.
`RuntimeCore::sync_sender` is currently typed `Box<dyn SyncSender + Send>` to
satisfy the multi-threaded Tokio backend used by NAPI and the server. On
`target_arch = "wasm32"` the runtime is single-threaded and the JS/web-sys
values the new sender holds (`JsValue`, `Function`, `Rc`) are `!Send`.

Cfg-gate the bound on the field and on the setter:

```rust
// crates/jazz-tools/src/runtime_core/mod.rs

#[cfg(target_arch = "wasm32")]
pub(crate) sync_sender: Option<Box<dyn SyncSender>>,
#[cfg(not(target_arch = "wasm32"))]
pub(crate) sync_sender: Option<Box<dyn SyncSender + Send>>,

// …

#[cfg(target_arch = "wasm32")]
pub fn set_sync_sender(&mut self, sender: Box<dyn SyncSender>) {
    self.sync_sender = Some(sender);
}
#[cfg(not(target_arch = "wasm32"))]
pub fn set_sync_sender(&mut self, sender: Box<dyn SyncSender + Send>) {
    self.sync_sender = Some(sender);
}
```

The `SyncSender` trait itself stays unchanged; do not add a `Send`
super-trait there. On wasm32 the runtime is single-threaded by construction,
so no safety property is being relaxed; the type system is being made to
express that.

## 4. Dependencies

Add to `crates/jazz-wasm/Cargo.toml`:

```toml
[dependencies]
postcard = { version = "1.1", features = ["alloc"] }

[target.'cfg(target_arch = "wasm32")'.dependencies]
web-sys = { version = "0.3", features = [
  "Worker",
  "WorkerOptions",
  "DedicatedWorkerGlobalScope",
  "MessageEvent",
  "MessagePort",
] }
```

`serde_bytes = "0.11"` is already present; if not, add it. `futures = "0.3"`
is already a runtime dep; if not, add it (needed for `oneshot` channels).

## 5. Module Layout

Create three new modules under `crates/jazz-wasm/src/`:

- `worker_protocol.rs` — wire enums, encode/decode helpers, JS test bindings.
- `worker_host.rs` — worker-side runtime host (`runAsWorker`, dispatch loop).
- `worker_bridge.rs` — main-side `WasmWorkerBridge`.

Wire them in `lib.rs`:

```rust
pub mod worker_protocol;                       // wasm + non-wasm (codec only)
#[cfg(target_arch = "wasm32")] pub mod worker_host;
#[cfg(target_arch = "wasm32")] pub mod worker_bridge;

#[cfg(target_arch = "wasm32")] pub use worker_bridge::WasmWorkerBridge;
#[cfg(target_arch = "wasm32")] pub use worker_host::run_as_worker;
```

Both `worker_host.rs` and `worker_bridge.rs` start with
`#![cfg(target_arch = "wasm32")]` so they only compile on the wasm target.

## 6. Wire Protocol

File: `crates/jazz-wasm/src/worker_protocol.rs`.

### 6.1 The two JS-object envelopes

Two messages stay as JS objects because they cannot ride on postcard:

- **`{type:"init", …}`** — main → worker, posted once. Carries
  `runtimeSources` (bundler-resolved JS module / URL / Uint8Array refs that
  have no Rust shape), plus the scalar runtime-identity fields. The shim
  consumes `runtimeSources` locally before handing off.
- **`{type:"ready"}`** — worker → main, posted once by the JS shim when WASM
  has loaded.
- **`{type:"error", message}`** — worker → main, posted by the JS shim if
  WASM load or bootstrap throws before Rust takes over. Rendered into a
  `WorkerToMainWire::Error` for the bridge.

All other traffic is binary.

### 6.2 Lifecycle event enum

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum WorkerLifecycleEvent {
    VisibilityHidden,
    VisibilityVisible,
    Pagehide,
    Freeze,
    Resume,
}
```

The TS adapter accepts these as kebab-case string literals
(`"visibility-hidden" | "visibility-visible" | "pagehide" | "freeze" |
"resume"`) and forwards them; the bridge parses the string into the enum
before posting. Unknown strings warn and drop.

### 6.3 Init payload

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InitPayloadFields {
    pub schema_json: String,
    pub app_id: String,
    pub env: String,
    pub user_branch: String,
    pub db_name: String,
    pub client_id: String,             // always "" today — placeholder
    pub server_url: Option<String>,
    pub jwt_token: Option<String>,
    pub admin_secret: Option<String>,
    pub fallback_wasm_url: Option<String>,
    pub log_level: Option<String>,
    pub telemetry_collector_url: Option<String>,
}

pub struct InitPayload {
    pub fields: InitPayloadFields,
    pub runtime_sources: JsValue,       // opaque JS pass-through
}
```

`runtime_sources` is **not** in `InitPayloadFields` because serde cannot
round-trip it. On encode, serialize `InitPayloadFields` to a JS object, then
`Reflect::set(&obj, "runtimeSources", &runtime_sources)`. On decode, do the
inverse.

### 6.4 `SyncEntry` (worker → main `Sync` heterogeneity)

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SyncEntry {
    BareBytes(ByteBuf),
    BareString(String),
    SequencedBytes { payload: ByteBuf, sequence: u64 },
    SequencedString { payload: String, sequence: u64 },
}
```

`ByteBuf` is `serde_bytes::ByteBuf` — required so postcard length-prefixes
the bytes rather than emitting a sequence of u8.

### 6.5 Main → Worker wire enum

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MainToWorkerWire {
    Sync { payloads: Vec<ByteBuf> },
    PeerOpen { peer_id: String },
    PeerSync { peer_id: String, term: u32, payloads: Vec<ByteBuf> },
    PeerClose { peer_id: String },
    LifecycleHint { event: WorkerLifecycleEvent, sent_at_ms: f64 },
    UpdateAuth { jwt_token: Option<String> },
    DisconnectUpstream,
    ReconnectUpstream,
    Shutdown,
    AcknowledgeRejectedBatch { batch_id: String },
    SimulateCrash,
    DebugSchemaState,
    DebugSeedLiveSchema { schema_json: String },
}
```

`Init` is deliberately **not** in this enum — see §6.1.

Main → worker `Sync` is homogeneous binary (server-bound payloads only —
client-bound never happens on the main side because the main runtime has no
client peers).

### 6.6 Worker → Main wire enum

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WorkerToMainWire {
    InitOk { client_id: String },
    UpstreamConnected,
    UpstreamDisconnected,
    Sync { payloads: Vec<SyncEntry> },
    PeerSync { peer_id: String, term: u32, payloads: Vec<ByteBuf> },
    LocalBatchRecordsSync { batches_json: String },
    MutationErrorReplay { batch_json: String },
    Error { message: String },
    AuthFailed { reason: String },
    ShutdownOk,
    DebugSchemaStateOk { state_json: String },
    DebugSeedLiveSchemaOk,
}
```

Heterogeneous JS-shaped payloads (`LocalBatchRecord`, `DebugSchemaState`)
ride as JSON strings — cheapest path that preserves the TS listener shapes.

### 6.7 In-process Rust enum (host dispatch)

```rust
pub enum MainToWorkerMessage {
    Init(Box<InitPayload>),
    Wire(MainToWorkerWire),
    Unknown(String),     // unrecognised JS-object type, posts Error
}
```

This is the value `parse_main_to_worker` returns to the worker host.

### 6.8 Parse functions

```rust
pub fn parse_main_to_worker(value: &JsValue) -> Result<MainToWorkerMessage, String>;
pub fn parse_worker_to_main(value: &JsValue) -> ParsedWorkerToMain;

pub enum ParsedWorkerToMain {
    Ready,
    Wire(WorkerToMainWire),
    UnknownJsObject(String),
    DecodeError(String),
    Malformed,
}
```

`parse_main_to_worker` algorithm:

1. If `value` has a string `type` property:
   - `"init"` → deserialize `InitPayloadFields` via `serde-wasm-bindgen`,
     pull `runtimeSources` via `Reflect::get`, return `Init(...)`.
   - Any other string → `Unknown(type)`.
2. Else if `value` is a `Uint8Array` → `postcard::from_bytes::<MainToWorkerWire>`
   and return `Wire(...)`.
3. Else → `Err("expected Uint8Array (binary) or `init` JS object")`.

`parse_worker_to_main` algorithm:

1. If `value` has a string `type` property:
   - `"ready"` → `ParsedWorkerToMain::Ready`.
   - `"error"` → `Wire(WorkerToMainWire::Error { message })`.
   - Any other string → `UnknownJsObject(type)`.
2. Else if `value` is a `Uint8Array` → postcard decode; success →
   `Wire(...)`, failure → `DecodeError(...)`.
3. Else → `Malformed`.

### 6.9 Encode helpers

```rust
pub fn encode_main_to_worker(msg: &MainToWorkerWire) -> Result<Vec<u8>, postcard::Error>;
pub fn encode_worker_to_main(msg: &WorkerToMainWire) -> Result<Vec<u8>, postcard::Error>;

// Build (Uint8Array JsValue, transfer Array of one ArrayBuffer)
pub fn encode_to_uint8array_with_transfer(bytes: &[u8]) -> (JsValue, Array);

// Convenience: encode then build the postMessage pair.
pub fn main_to_worker_post(msg: &MainToWorkerWire) -> Result<(JsValue, Array), postcard::Error>;
pub fn worker_to_main_post(msg: &WorkerToMainWire) -> Result<(JsValue, Array), postcard::Error>;
```

`encode_to_uint8array_with_transfer` constructs a JS-owned `Uint8Array`
from the byte slice (this copies — there is no zero-copy from wasm linear
memory because the wasm `memory.buffer` cannot be detached), then pushes
`arr.buffer()` onto the transfer `Array`. Callers pass both to
`target.postMessage(message, transfer)`.

### 6.10 JS-callable test helpers

Export four wasm-bindgen functions so browser tests can build and inspect
postcard envelopes without re-implementing the codec in TypeScript:

```rust
#[wasm_bindgen(js_name = encodeMainToWorkerJs)]
pub fn encode_main_to_worker_js(value: JsValue) -> Result<Uint8Array, JsError>;

#[wasm_bindgen(js_name = encodeWorkerToMainJs)]
pub fn encode_worker_to_main_js(value: JsValue) -> Result<Uint8Array, JsError>;

#[wasm_bindgen(js_name = decodeMainToWorkerJs)]
pub fn decode_main_to_worker_js(bytes: &Uint8Array) -> Result<JsValue, JsError>;

#[wasm_bindgen(js_name = decodeWorkerToMainJs)]
pub fn decode_worker_to_main_js(bytes: &Uint8Array) -> Result<JsValue, JsError>;
```

These take/return JS objects of the form `{ type: "kebab-case", …fields }`
that mirror the legacy TS protocol shape. They only need to cover the
variants used by tests, not the full enum — the encode helpers should return
`Err` for unsupported variants.

### 6.11 In-source tests

`#[cfg(test)]` round-trip every variant of both enums through
`postcard::to_allocvec` and back. This guards the silent-drop class of bug
where receiver expects postcard but sender emits a JS object.

## 7. `RustOutboxSender`

File: `crates/jazz-wasm/src/runtime.rs` (additions; do not break existing
APIs except the deletions in §13.7).

A single `SyncSender` implementation drives both sides of the worker
boundary. Side discrimination is by whether `main_client_id` is `Some`.

### 7.1 State

```rust
#[derive(Clone, Copy)]
struct PeerRouting {
    is_main: bool,    // entry destined for main-thread peer client
}

struct RustOutboxSenderInner {
    target: RefCell<JsValue>,
    main_client_id: RefCell<Option<String>>,
    peer_routing_lookup: RefCell<Option<Function>>,
    on_main_sync_flushed: RefCell<Option<Function>>,
    server_payload_forwarder: RefCell<Option<Function>>,
    bootstrap_catalogue_forwarding: RefCell<bool>,
    use_binary_encoding: bool,
    next_client_sequences: RefCell<HashMap<String, u64>>,
    pending_sync_entries: RefCell<Vec<SyncEntry>>,
    pending_sync_routing: RefCell<Vec<PeerRouting>>,
    flush_scheduled: RefCell<bool>,
    init_gate_open: RefCell<bool>,    // default true
}

#[derive(Clone)]
pub struct RustOutboxSender { inner: Rc<RustOutboxSenderInner> }
```

`pub(crate)` API:

```rust
fn new(use_binary_encoding: bool) -> Self;
fn set_init_gate(&self, open: bool);
fn open_init_gate_and_flush(&self);
fn flush_now(&self);                    // synchronous drain
fn attach_target(
    &self,
    target: JsValue,                    // Worker (main) or self (worker)
    main_client_id: Option<String>,
    peer_routing_lookup: Option<Function>,
    on_main_sync_flushed: Option<Function>,
);
fn set_server_payload_forwarder(&self, forwarder: Option<Function>);
fn set_bootstrap_catalogue_forwarding(&self, enabled: bool);
```

### 7.2 Hot path: `SyncSender::send_sync_message(message: OutboxEntry)`

Pseudocode (preserves all the routing rules):

```
let is_catalogue = message.payload.is_catalogue();
let (destination_kind, destination_id) = match message.destination {
    Destination::Server(s) => ("server", s.0.to_string()),
    Destination::Client(c) => ("client", c.0.to_string()),
};

// 1. Sequence numbering (client-bound only).
let sequence = if destination_kind == "client" {
    let mut seqs = next_client_sequences.borrow_mut();
    let next = seqs.entry(destination_id.clone()).and_modify(|n| *n += 1).or_insert(1);
    Some(*next)
} else { None };

// 2. QuerySettled.through_seq rewrite — for sequenced client-bound,
//    rewrite through_seq to sequence - 1 so the receiver can validate
//    "I have processed everything up to N-1 before applying this N".
let payload = match (&message.payload, sequence) {
    (SyncPayload::QuerySettled { query_id, tier, scope, .. }, Some(seq)) =>
        SyncPayload::QuerySettled {
            query_id: *query_id, tier: *tier, scope: scope.clone(),
            through_seq: seq.saturating_sub(1),
        },
    _ => message.payload,
};

// 3. Encode. Client-bound is ALWAYS binary; server-bound respects use_binary_encoding.
let use_binary = use_binary_encoding || destination_kind == "client";
let encoded: SyncEntry = if use_binary {
    let bytes = payload.to_bytes()?;
    match sequence {
        Some(seq) => SyncEntry::SequencedBytes { payload: ByteBuf::from(bytes), sequence: seq },
        None      => SyncEntry::BareBytes(ByteBuf::from(bytes)),
    }
} else {
    let json = payload.to_json()?;
    match sequence {
        Some(seq) => SyncEntry::SequencedString { payload: json, sequence: seq },
        None      => SyncEntry::BareString(json),
    }
};

// 4. Server-bound routing.
if destination_kind == "server" {
    // 4a. Forwarder takes priority on the main side.
    if let Some(fwd) = server_payload_forwarder.borrow().as_ref() {
        let js = sync_entry_payload_js(&encoded);
        fwd.call1(NULL, &js).ok();
        return;
    }
    let main_side = main_client_id.borrow().is_none();
    if main_side {
        // 4b. Main side: queue into the worker-bound sync batch.
        push(encoded, PeerRouting { is_main: false });
        schedule_flush();
    } else if *bootstrap_catalogue_forwarding.borrow() && is_catalogue {
        // 4c. Worker side bootstrap: catalogue entries forward to main.
        push(encoded, PeerRouting { is_main: true });
        schedule_flush();
    }
    // 4d. Worker side post-bootstrap: server-bound is delivered by the
    //     Rust transport (runtime.connect); drop silently from here.
    return;
}

// 5. Client-bound routing (worker side only; main side has no client peers).
let main_client_id = main_client_id.borrow().clone();
let Some(main) = main_client_id else { return; };

if destination_id == main {
    // 5a. To the main-thread peer client → batch into Sync envelope.
    push(encoded, PeerRouting { is_main: true });
    schedule_flush();
    return;
}

// 5b. To another follower-tab peer → look up (peerId, term) and post PeerSync immediately.
let lookup = peer_routing_lookup.borrow();
let Some(lookup) = lookup.as_ref() else { return; };
let routing = match lookup.call1(NULL, &JsValue::from_str(&destination_id)) {
    Ok(v) => v,
    Err(_) => { warn!("peer_routing_lookup threw"); return; }      // §7.6 contract
};
if routing.is_null() || routing.is_undefined() { return; }
let peer_id = Reflect::get(&routing, "peerId").as_string()?;
let term   = Reflect::get(&routing, "term").as_f64()?;

let bytes = match encoded {
    SyncEntry::BareBytes(b) | SyncEntry::SequencedBytes { payload: b, .. } => b,
    _ => return,                       // peer payloads are binary-only
};
let wire = WorkerToMainWire::PeerSync { peer_id, term: term as u32, payloads: vec![bytes] };
let bytes = postcard::to_allocvec(&wire)?;
let arr = Uint8Array::from(bytes.as_slice());
let transfer = Array::new();  transfer.push(&arr.buffer().into());
target.postMessage(arr, transfer);
```

### 7.3 `schedule_flush`

```
if !*init_gate_open.borrow() { return; }
if *flush_scheduled.borrow() { return; }
*flush_scheduled.borrow_mut() = true;
wasm_bindgen_futures::spawn_local(async move { flush_pending(&inner); });
```

`spawn_local` schedules the flush onto the microtask queue. Multiple
`send_sync_message` calls in the same synchronous block produce one flush.

### 7.4 `flush_pending`

```
*flush_scheduled.borrow_mut() = false;
let entries = mem::take(&mut *pending_sync_entries.borrow_mut());
let routing = mem::take(&mut *pending_sync_routing.borrow_mut());
if entries.is_empty() { return; }

let target = target.borrow().clone();
if target.is_null() || target.is_undefined() { return; }

let had_main_entry = routing.iter().any(|r| r.is_main);
let main_side = main_client_id.borrow().is_none();

let bytes = if main_side {
    // Main side: only BareBytes (server-bound, postcard-encoded payload bytes).
    let payloads: Vec<ByteBuf> = entries.into_iter()
        .filter_map(|e| if let SyncEntry::BareBytes(b) = e { Some(b) } else { None })
        .collect();
    if payloads.is_empty() { return; }
    postcard::to_allocvec(&MainToWorkerWire::Sync { payloads })?
} else {
    // Worker side: heterogeneous entries.
    postcard::to_allocvec(&WorkerToMainWire::Sync { payloads: entries })?
};

post_message_with_transfer(target, Uint8Array::from(bytes.as_slice()));

if had_main_entry {
    if let Some(cb) = on_main_sync_flushed.borrow().as_ref() {
        cb.call0(NULL).ok();
    }
}
```

### 7.5 `flush_now`

Same body as `flush_pending` but called synchronously by the bridge on
shutdown. Used to drain the queue before posting `Shutdown` — otherwise the
worker drops the runtime on `Shutdown` and the late microtask flush has
nothing to receive.

### 7.6 Callback contracts

- **`peer_routing_lookup(clientId)`** (worker side).
  - Returns `{ peerId: string, term: number }` → use as the destination.
  - Returns `null` / `undefined` → drop the entry silently.
  - Throws → log a warning, drop the entry, do **not** propagate the panic.
  - Returns any other type → treat as `null`.

- **`on_main_sync_flushed()`** (worker side). Invoked after a batch flush
  that contained at least one main-bound entry. The worker host uses it to
  schedule the rejected-batch replay walk.

- **`server_payload_forwarder(payload)`** (main side). Optional. When set,
  every server-bound outbox entry is forwarded to JS instead of being
  batched to the worker. Payload type is `Uint8Array` for postcard, `string`
  for JSON; the leader/follower coordinator on the JS side handles both.

### 7.7 `NoopSyncSender`

```rust
struct NoopSyncSender;
impl SyncSender for NoopSyncSender {
    fn send_sync_message(&self, _: OutboxEntry) {}
    fn as_any(&self) -> &dyn Any { self }
}
```

5 lines. No new method on `RuntimeCore`; the bridge swaps the active sender
wholesale via the existing `set_sync_sender`.

### 7.8 `WasmRuntime::install_noop_sync_sender`

```rust
#[cfg(target_arch = "wasm32")]
impl WasmRuntime {
    pub(crate) fn install_noop_sync_sender(&self) {
        self.core.borrow_mut().set_sync_sender(Box::new(NoopSyncSender));
    }
}
```

Not `#[wasm_bindgen]`-exported; called only by `WasmWorkerBridge`.

### 7.9 `WasmRuntime::createWorkerBridge`

```rust
#[wasm_bindgen(js_name = createWorkerBridge)]
pub fn create_worker_bridge(&self, worker: Worker, options: JsValue) -> Result<WasmWorkerBridge, JsError> {
    WasmWorkerBridge::attach(worker, self, options)
}
```

Single factory the TS adapter calls. The bridge type itself is also exported
so `WasmWorkerBridge.attach(worker, runtime, options)` is callable directly.

## 8. Main-Side: `WasmWorkerBridge`

File: `crates/jazz-wasm/src/worker_bridge.rs`.

### 8.1 Public API surface

```rust
#[wasm_bindgen]
pub struct WasmWorkerBridge { /* inner: Rc<BridgeInner> */ }

#[wasm_bindgen]
impl WasmWorkerBridge {
    #[wasm_bindgen(js_name = attach)]
    pub fn attach(worker: Worker, runtime: &WasmRuntime, options: JsValue) -> Result<WasmWorkerBridge, JsError>;

    #[wasm_bindgen]
    pub fn init(&self) -> js_sys::Promise;                         // resolves { clientId }

    #[wasm_bindgen(js_name = updateAuth)]
    pub fn update_auth(&self, jwt_token: Option<String>);

    #[wasm_bindgen(js_name = sendLifecycleHint)]
    pub fn send_lifecycle_hint(&self, event: &str);

    #[wasm_bindgen(js_name = openPeer)]
    pub fn open_peer(&self, peer_id: &str);

    #[wasm_bindgen(js_name = sendPeerSync)]
    pub fn send_peer_sync(&self, peer_id: &str, term: u32, payload: js_sys::Array);

    #[wasm_bindgen(js_name = closePeer)]
    pub fn close_peer(&self, peer_id: &str);

    #[wasm_bindgen(js_name = setServerPayloadForwarder)]
    pub fn set_server_payload_forwarder(&self, callback: Option<Function>);

    #[wasm_bindgen(js_name = applyIncomingServerPayload)]
    pub fn apply_incoming_server_payload(&self, payload: Uint8Array) -> Result<(), JsError>;

    #[wasm_bindgen(js_name = waitForUpstreamServerConnection)]
    pub async fn wait_for_upstream_server_connection(&self) -> Result<(), JsValue>;

    #[wasm_bindgen(js_name = replayServerConnection)]
    pub fn replay_server_connection(&self);

    #[wasm_bindgen(js_name = disconnectUpstream)]
    pub fn disconnect_upstream(&self);

    #[wasm_bindgen(js_name = reconnectUpstream)]
    pub fn reconnect_upstream(&self);

    #[wasm_bindgen(js_name = simulateCrash)]
    pub fn simulate_crash(&self) -> js_sys::Promise;

    #[wasm_bindgen(js_name = acknowledgeRejectedBatch)]
    pub fn acknowledge_rejected_batch(&self, batch_id: &str);

    #[wasm_bindgen(js_name = setListeners)]
    pub fn set_listeners(&self, listeners: JsValue);
        // listeners: { onPeerSync?, onAuthFailure?, onLocalBatchRecordsSync?, onMutationErrorReplay? }

    #[wasm_bindgen(js_name = getWorkerClientId)]
    pub fn get_worker_client_id(&self) -> JsValue;                 // string | null

    #[wasm_bindgen]
    pub fn shutdown(&self) -> js_sys::Promise;
}
```

### 8.2 Internal state

```rust
const INIT_RESPONSE_TIMEOUT_MS: i32 = 12_000;
const SHUTDOWN_ACK_TIMEOUT_MS: i32 = 5_000;

#[derive(Clone, Copy, PartialEq, Eq)]
enum BridgeState { Idle, Initializing, Ready, Failed, ShuttingDown, Disposed }

#[derive(Default)]
struct Listeners {
    on_peer_sync: Option<Function>,
    on_auth_failure: Option<Function>,
    on_local_batch_records_sync: Option<Function>,
    on_mutation_error_replay: Option<Function>,
}

struct BridgeInner {
    worker: Worker,
    runtime: WasmRuntime,
    sender: RustOutboxSender,
    init_message: JsValue,                   // pre-built JS-object init envelope
    state: Cell<BridgeState>,
    worker_client_id: RefCell<Option<String>>,
    listeners: RefCell<Listeners>,
    on_message_closure: RefCell<Option<Closure<dyn FnMut(MessageEvent)>>>,
    init_resolver: RefCell<Option<oneshot::Sender<Result<String, String>>>>,
    init_promise: RefCell<Option<js_sys::Promise>>,
    shutdown_resolver: RefCell<Option<oneshot::Sender<()>>>,
    expects_upstream: Cell<bool>,
    upstream_connected: Cell<bool>,
    has_forwarder: Cell<bool>,
    upstream_ready_promise: RefCell<js_sys::Promise>,
    upstream_ready_resolver: RefCell<Option<Function>>,
}

pub struct WasmWorkerBridge { inner: Rc<BridgeInner> }
```

`BridgeInitOptions` parses out of the `options` JsValue via
`serde-wasm-bindgen` (`rename_all = "camelCase"`):

```rust
struct BridgeInitOptions {
    schema_json: String,
    app_id: String,
    env: String,
    user_branch: String,
    db_name: String,
    server_url: Option<String>,
    jwt_token: Option<String>,
    admin_secret: Option<String>,
    fallback_wasm_url: Option<String>,
    log_level: Option<String>,
    telemetry_collector_url: Option<String>,
}
```

`runtimeSources` is **not** in the serde struct — it is pulled off the
original `JsValue` via `Reflect::get` and slotted into the init envelope via
`Reflect::set`.

### 8.3 State machine

| From               | Trigger                 | To           | Notes                                              |
| ------------------ | ----------------------- | ------------ | -------------------------------------------------- |
| Idle               | `init()` first call     | Initializing | resolver installed, init posted                    |
| Initializing       | `InitOk`                | Ready        | `worker_client_id` set, gate opened, queue flushed |
| Initializing       | `Error` or timeout      | Failed       | terminal                                           |
| Initializing       | duplicate `init()`      | Initializing | shared in-flight Promise                           |
| Ready              | `init()` (second time)  | Ready        | shared in-flight Promise still resolves            |
| Ready              | `shutdown()`            | ShuttingDown | drain, flush, noop sender, remove server           |
| Initializing/Ready | `Drop`                  | Disposed     | best-effort cleanup, no `Shutdown` post            |
| ShuttingDown       | `ShutdownOk` or timeout | Disposed     | clear onmessage                                    |
| Failed             | anything                | Failed       | terminal; caller must reattach to a fresh Worker   |
| Disposed           | anything                | Disposed     | all methods no-op                                  |

`shutdown()` on an already-disposed bridge returns `Promise.resolve(undefined)`
synchronously. `update_auth`, `send_lifecycle_hint`, `send_peer_sync`,
`open_peer`, `close_peer`, `disconnect_upstream`, `reconnect_upstream`,
`acknowledge_rejected_batch`, `replay_server_connection`, and
`apply_incoming_server_payload` are guarded by `is_disposed_like()` which is
`true` for `ShuttingDown | Disposed`.

### 8.4 `attach()` algorithm

```
1. Parse BridgeInitOptions via serde-wasm-bindgen.
2. Build the init JS-object envelope:
     - serde-encode the scalar fields (camelCase keys)
     - Reflect::set(envelope, "runtimeSources", original.runtimeSources) if present
     - set "type" = "init" and "clientId" = ""
3. expects_upstream = server_url.is_some()
4. Construct sender = RustOutboxSender::new(true)
   sender.attach_target(worker.clone().into(), None, None, None)
   sender.set_init_gate(false)
5. runtime.core.set_sync_sender(Box::new(sender.clone()))
6. Build BridgeInner with a fresh deferred upstream-ready Promise.
7. Install worker.onmessage as a Rust Closure<dyn FnMut(MessageEvent)>
   that calls inner.handle_message(event). Store the Closure inside
   inner.on_message_closure (so it lives as long as the bridge).
8. runtime.add_server(None, Some(1.0))
9. If expects_upstream: mark_upstream_disconnected()  (re-arms a fresh promise)
   else:                  mark_upstream_connected()    (resolves the promise immediately)
10. Return WasmWorkerBridge { inner }.
```

### 8.5 `init()` algorithm

```
1. If init_promise is Some → return cached Promise.
2. Try state transition (transition_init_called):
     Idle → Initializing  (proceed)
     Initializing | Ready → return cached promise (or re-resolve)
     Failed | ShuttingDown | Disposed → return Promise.reject("WorkerBridge has been disposed")
3. Create (tx, rx) = oneshot::channel::<Result<String, String>>().
   Store tx in init_resolver.
4. worker.postMessage(init_message).
   If postMessage throws:
     - clear init_resolver
     - transition_init_failed()
     - return Promise.reject("postMessage init: {err}")
5. Build a future_to_promise that:
     a. select(rx, make_timeout(INIT_RESPONSE_TIMEOUT_MS)).await
     b. On Ok(clientId): transition_init_ok(clientId); sender.open_init_gate_and_flush();
        resolve with { clientId }.
     c. On Err(msg) or timeout: transition_init_failed(); reject with "Worker init failed: {msg}".
6. Cache the Promise in init_promise and return it.
```

`transition_init_ok` sets `worker_client_id = Some(clientId)` and state to
`Ready`. `transition_init_failed` sets state to `Failed`.

The synchronous portion (resolver registration + postMessage) happens
**before** the returned Promise awaits anything. Tests that synthesise an
`InitOk` immediately after the call do not need a microtask yield.

### 8.6 Message dispatch (`handle_message`)

```
let data = event.data();
match parse_worker_to_main(&data) {
    Ready                  => {/* startup ping; no-op */}
    Wire(w)                => dispatch_wire(w),
    UnknownJsObject(t)     => warn!("ignoring unknown JS-object worker→main {t}"),
    DecodeError(e)         => warn!("worker→main decode error: {e}"),
    Malformed              => warn!("worker→main message neither Uint8Array nor known JS object"),
}
```

`dispatch_wire` cases:

| Variant                                        | Behaviour                                                                                                                                      |
| ---------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------- |
| `InitOk { client_id }`                         | Take init_resolver; send `Ok(client_id)`                                                                                                       |
| `Error { message }`                            | If init_resolver present, send `Err(message)`; else log warn                                                                                   |
| `UpstreamConnected`                            | `mark_upstream_connected()`                                                                                                                    |
| `UpstreamDisconnected`                         | `mark_upstream_disconnected()`                                                                                                                 |
| `AuthFailed { reason }`                        | Clone listeners.on_auth_failure, call with reason. The clone-out-then-drop-the-borrow pattern is mandatory (listener may re-enter the bridge). |
| `LocalBatchRecordsSync { batches_json }`       | `JSON.parse(batches_json)`; clone listener; call with parsed value                                                                             |
| `MutationErrorReplay { batch_json }`           | Same shape                                                                                                                                     |
| `PeerSync { peer_id, term, payloads }`         | Build `{ peerId, term, payload: Uint8Array[] }`; clone listener; call                                                                          |
| `Sync { payloads }`                            | For each entry, route to runtime.on_sync_message_received (with sequence if sequenced)                                                         |
| `ShutdownOk`                                   | Take shutdown_resolver; send `()`                                                                                                              |
| `DebugSchemaStateOk` / `DebugSeedLiveSchemaOk` | No listener; just drain (debug responses)                                                                                                      |

### 8.7 Outbound methods (concrete behaviour)

- `update_auth(jwt_token)` → post `UpdateAuth { jwt_token }`. No-op when disposed.
- `send_lifecycle_hint(event)` → parse via the `parse_lifecycle_event(&str)`
  helper (kebab-case → enum variant). Unknown strings warn and drop. Post
  `LifecycleHint { event, sent_at_ms: js_sys::Date::now() }`.
- `open_peer(peer_id)` → post `PeerOpen { peer_id }`. No-op when disposed.
- `send_peer_sync(peer_id, term, payload: Array)` → iterate the JS array,
  cast each entry to `Uint8Array`, copy to `ByteBuf`. Drop the call if the
  resulting `Vec<ByteBuf>` is empty (or the original array was empty). Post
  `PeerSync { peer_id, term, payloads }`.
- `close_peer(peer_id)` → post `PeerClose { peer_id }`.
- `disconnect_upstream()` → post `DisconnectUpstream`.
- `reconnect_upstream()` → post `ReconnectUpstream`.
- `acknowledge_rejected_batch(batch_id)` → post `AcknowledgeRejectedBatch { batch_id }`.
- `replay_server_connection()` → call `runtime.remove_server(); runtime.add_server(None, None);`
  (does not touch the worker at all — this is a main-runtime-only operation).
- `apply_incoming_server_payload(payload)` → call
  `runtime.on_sync_message_received(payload.into(), None)`. Used when a
  follower tab receives an upstream payload from the leader and needs to
  feed its main-thread runtime.
- `simulate_crash()` (test-only) → install a fresh shutdown_resolver, post
  `SimulateCrash`, and resolve when `ShutdownOk` arrives or after the
  shutdown-ack timeout. Does **not** transition the bridge state — used by
  tests to validate WAL replay.

### 8.8 Upstream-ready signalling

`make_deferred_promise()` returns `(promise, resolver)` where `resolver` is
a JS `Function` that resolves the promise when called. The bridge stores
both in `upstream_ready_promise` / `upstream_ready_resolver`.

- `mark_upstream_connected()` — sets `upstream_connected = true`, then
  calls the stored resolver to release any `waitForUpstreamServerConnection`
  awaiters.
- `mark_upstream_disconnected()` — if `expects_upstream` is `false`, set
  `upstream_connected = true` and return (the bridge is never expected to
  await upstream in no-server mode). Otherwise, if there is already an
  unresolved resolver in place, leave it alone. Otherwise, construct a
  fresh deferred promise / resolver pair and store both, then set
  `upstream_connected = false`.
- `rearm_upstream_ready_promise()` — if the resolver is already pending,
  return. Otherwise install a fresh deferred pair.
- `set_server_payload_forwarder(cb)`:
  - Track `has_forwarder` based on whether `cb.is_some()`.
  - Push `cb` into the sender.
  - If `has_forwarder` is now true → call `release_upstream_waiters()`
    (resolve any pending wait; do **not** flip `upstream_connected`).
  - If `has_forwarder` is now false **and** `expects_upstream && !upstream_connected`
    → `rearm_upstream_ready_promise()`.

`wait_for_upstream_server_connection()` returns immediately if any of:
`expects_upstream == false`, `has_forwarder == true`, or
`upstream_connected == true`. Otherwise it awaits the stored promise.

### 8.9 Shutdown

```
shutdown():
    if is_disposed_like() → return Promise.resolve(undefined)
    transition_shutdown_called()  // state → ShuttingDown (unless already Disposed)

    // 1. Drain the main runtime's outbox into the sender's pending queue.
    runtime.batched_tick()
    // 2. Synchronously flush the queue to the worker.
    sender.flush_now()
    // 3. Detach the outbox edge from the runtime.
    runtime.install_noop_sync_sender()
    sender.set_server_payload_forwarder(None)
    runtime.remove_server()

    // 4. Post Shutdown, await ShutdownOk or timeout.
    let (tx, rx) = oneshot::channel::<()>()
    *shutdown_resolver = Some(tx)
    post Shutdown to worker
    return future_to_promise(async {
        select(rx, make_timeout(SHUTDOWN_ACK_TIMEOUT_MS)).await
        // 5. Explicitly clear worker.onmessage — Closure::drop does NOT clear the JS slot.
        worker.set_onmessage(None)
        transition_shutdown_finished()
    })
```

`transition_shutdown_finished` sets state to `Disposed` and clears
`listeners` + `on_message_closure`.

### 8.10 `Drop for WasmWorkerBridge`

Runs when the JS-side wrapper is dropped without an explicit `shutdown()`
(typically an exception during init):

```
if !is_disposed_like() { dispose_internals(); }       // clear listeners & closure
runtime.install_noop_sync_sender();
sender.set_server_payload_forwarder(None);
runtime.remove_server();
worker.set_onmessage(None);
// Deliberately do NOT post Shutdown — by the time Drop runs in an exception
// path, the receiver may be gone, and posting from a destructor risks
// structured-clone errors.
```

### 8.11 Helper: `make_timeout(ms)`

`make_timeout` builds a `JsFuture` from a `setTimeout`-resolved Promise:

```
let global = js_sys::global();
let set_timeout: Function = Reflect::get(&global, "setTimeout").dyn_into()?;
let promise = js_sys::Promise::new(&mut |resolve, _| {
    set_timeout.call2(NULL, &resolve, &JsValue::from_f64(ms as f64)).ok();
});
JsFuture::from(promise)
```

Pair with `futures::future::select(future, timeout).await` for the
race-with-timeout idiom used by `init` and `shutdown`.

## 9. Worker-Side: `WorkerHost`

File: `crates/jazz-wasm/src/worker_host.rs`.

### 9.1 Public entry

```rust
#[wasm_bindgen(js_name = runAsWorker)]
pub fn run_as_worker(init_message: JsValue, pending_messages: Array) -> Result<(), JsError>;
```

Synchronously installs the Rust `self.onmessage` closure, parks the host in
the `HOST` thread_local, and spawns `run_init` via `spawn_local`. Returns
immediately. If `HOST` is already populated (a duplicate call from the JS
shim), returns `Ok(())` without side-effects.

### 9.2 thread_local layout

```rust
thread_local! {
    static HOST:         RefCell<Option<WorkerHost>>      = const { RefCell::new(None) };
    static RUNTIME:      RefCell<Option<Rc<WasmRuntime>>> = const { RefCell::new(None) };
    static PEER_ROUTING: RefCell<PeerRouting>             = RefCell::new(PeerRouting::default());
}
```

Three cells deliberately, not one. The outbox sender callbacks
(`peer_routing_lookup`, `on_main_sync_flushed`) need to borrow the peer
table without re-entering whatever borrow is currently active on `HOST`.
Splitting `RUNTIME` and `PEER_ROUTING` out of the host state lets dispatch
handlers borrow `HOST` mutably while outbox callbacks borrow `PEER_ROUTING`
mutably concurrently.

### 9.3 Host state

```rust
#[derive(Clone, Copy, PartialEq, Eq)]
enum HostState { Initializing, Ready, ShuttingDown }

struct WorkerHost {
    state: HostState,
    pending_messages: VecDeque<MainToWorkerMessage>,
    on_message_closure: Option<Closure<dyn FnMut(MessageEvent)>>,
    current_auth_jwt: Option<String>,
    current_admin_secret: Option<String>,
    current_ws_url: Option<String>,
    rejected_replay_queued: bool,
}

struct PeerRouting {
    main_client_id: Option<String>,
    peer_client_by_peer_id: HashMap<String, String>,
    peer_id_by_client: HashMap<String, String>,
    peer_terms: HashMap<String, u32>,
}
```

### 9.4 `run_as_worker` algorithm

```
1. If HOST is already Some → return Ok(()).  Idempotent.
2. Parse init synchronously:
     parse_main_to_worker(init_message)
     - Init(payload) → continue
     - Wire(_)      → post Error("first message must be `init`, got {type}"); return
     - Unknown(_)   → post Error("first message must be `init`, got <unknown>"); return
     - Err(_)       → post Error("init parse error: {e}"); return
3. Create a fresh WorkerHost (state = Initializing).
4. Drain pending_messages array:
     for entry in pending_messages.iter():
       match parse_main_to_worker(&entry):
         - Init(_)    → log warn, post Error("ignoring duplicate init") (do not buffer)
         - other      → host.pending_messages.push_back(other)
         - Err(e)     → log warn (drop)
5. Install self.onmessage closure:
     global = global_worker_scope()  // DedicatedWorkerGlobalScope
     closure = Closure<dyn FnMut(MessageEvent)>::new(|event| {
        match parse_main_to_worker(&event.data()) {
          Ok(msg) → handle_main_message(msg)
          Err(e)  → post Error("malformed worker message: {e}")
        }
     })
     global.set_onmessage(Some(closure.as_ref().unchecked_ref()))
     host.on_message_closure = Some(closure)
6. HOST = Some(host)
7. spawn_local(run_init(init.fields))
8. Return Ok(())
```

### 9.5 `run_init` algorithm

```
async fn run_init(init: InitPayload) -> Result<(), String> {

    let f = &init.fields;

    // 1. Open runtime. Persistent by default; fall back to ephemeral on SecurityError.
    let runtime = match WasmRuntime::open_persistent(
        &f.schema_json, &f.app_id, &f.env, &f.user_branch, &f.db_name,
        Some("local".into()), false,
    ).await {
        Ok(rt) => rt,
        Err(err) if is_security_error(&err) => {
            tracing::warn!("OPFS unavailable (SecurityError) — falling back to ephemeral");
            WasmRuntime::open_ephemeral(
                &f.schema_json, &f.app_id, &f.env, &f.user_branch, &f.db_name,
                Some("local".into()), false,
            ).map_err(|e| format!("ephemeral open: {e:?}"))?
        }
        Err(err) => return Err(format!("persistent open: {}", js_error_message(&err))),
    };

    // 2. Register the main thread as a peer client.
    let main_client_id = runtime.add_client();
    runtime.set_client_role(&main_client_id, "peer")?;

    // 3. Wire the auth-failure callback. It posts UpstreamDisconnected + AuthFailed.
    let auth_cb = Closure::<dyn FnMut(JsValue)>::new(|reason: JsValue| {
        let raw = reason.as_string().unwrap_or_default();
        post_to_main(&WorkerToMainWire::UpstreamDisconnected);
        post_to_main(&WorkerToMainWire::AuthFailed {
            reason: map_auth_reason(&raw).into(),
        });
    }).into_js_value();
    runtime.on_auth_failure(auth_cb.unchecked_into());

    // 4. Stash runtime + main client id atomically.
    let runtime_rc = Rc::new(runtime);
    RUNTIME.with(|c| *c.borrow_mut() = Some(Rc::clone(&runtime_rc)));
    PEER_ROUTING.with(|c| c.borrow_mut().main_client_id = Some(main_client_id.clone()));

    // 5. Construct + install the worker-side outbox sender.
    let sender = RustOutboxSender::new(true);
    sender.attach_target(
        global_worker_scope().into(),
        Some(main_client_id.clone()),
        Some(make_peer_routing_lookup()),
        Some(make_on_main_sync_flushed()),
    );
    runtime_rc.core.borrow_mut().set_sync_sender(Box::new(sender.clone()));

    // 6. Bootstrap catalogue dance — addServer/removeServer with the bootstrap-catalogue
    //    forwarding flag set, so the catalogue rows fan out to main BEFORE the
    //    upstream transport is installed.
    sender.set_bootstrap_catalogue_forwarding(true);
    let _ = runtime_rc.add_server(None, None);
    runtime_rc.remove_server();
    sender.set_bootstrap_catalogue_forwarding(false);

    // 7. Connect upstream BEFORE draining pending sync. Drained main writes can
    //    generate server-bound traffic; we want it routed via the Rust transport,
    //    not into the closed bootstrap-catalogue forwarder.
    if let Some(server_url) = &f.server_url {
        // Build auth JSON from admin_secret + jwt_token if either is present.
        // Cache both into HOST so reconnect-upstream can rebuild without re-init.
        let mut auth = serde_json::Map::new();
        if let Some(secret) = &f.admin_secret {
            auth.insert("admin_secret".into(), serde_json::Value::String(secret.clone()));
            HOST.with(|c| if let Some(h) = c.borrow_mut().as_mut() {
                h.current_admin_secret = Some(secret.clone());
            });
        }
        if let Some(jwt) = &f.jwt_token {
            auth.insert("jwt_token".into(), serde_json::Value::String(jwt.clone()));
            HOST.with(|c| if let Some(h) = c.borrow_mut().as_mut() {
                h.current_auth_jwt = Some(jwt.clone());
            });
        }
        let auth_json = serde_json::to_string(&auth).unwrap_or_else(|_| "{}".into());
        let ws_url = http_url_to_ws(server_url, &f.app_id);
        HOST.with(|c| if let Some(h) = c.borrow_mut().as_mut() {
            h.current_ws_url = Some(ws_url.clone());
        });
        perform_upstream_connect(&runtime_rc, &ws_url, &auth_json);
    }

    // 8. Sync retained local batch records; queue rejected-batch replay.
    sync_retained_local_batch_records(&runtime_rc);
    queue_rejected_batch_replay();

    // 9. Flip to Ready BEFORE draining (so handlers do not re-buffer).
    HOST.with(|c| if let Some(h) = c.borrow_mut().as_mut() {
        h.state = HostState::Ready;
    });

    // 10. Drain pending_messages in arrival order.
    drain_pending_messages();

    // 11. Post InitOk last.
    post_to_main(&WorkerToMainWire::InitOk { client_id: main_client_id });

    Ok(())
}
```

If `run_init` returns `Err(msg)`, post `WorkerToMainWire::Error { message: format!("Init failed: {msg}") }`.

### 9.6 Message dispatch

```
fn handle_main_message(msg: MainToWorkerMessage):
    if msg is Init(_):
        post Error("ignoring duplicate init"); return
    state = HOST.with(|c| c.borrow().as_ref().map(|h| h.state))
    match state:
      Some(Initializing) → push msg to host.pending_messages
      Some(Ready)        → process_main_message(msg)
      _                  → drop silently     // ShuttingDown or None

fn process_main_message(msg):
    runtime = RUNTIME.with(|c| c.borrow().clone())
    let wire = match msg {
        Init(_)    → post Error("ignoring duplicate init"); return
        Unknown(t) → warn; return
        Wire(w)    → w
    }
    dispatch on wire (see table below)
```

### 9.7 Wire dispatch table

| Variant                                 | Behaviour                                                                                                                                                        |
| --------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------- | --------------------------------------------------- |
| `Sync { payloads }`                     | For each payload: `Uint8Array::from(bytes); runtime.on_sync_message_received_from_client(main_client_id, arr.into())`.                                           |
| `PeerOpen { peer_id }`                  | `ensure_peer_client(runtime, peer_id)` (create + cache `peer-role` client).                                                                                      |
| `PeerSync { peer_id, term, payloads }`  | `ensure_peer_client(...)`; record `peer_terms.insert(peer_id, term)`; for each payload feed `on_sync_message_received_from_client(peer_client, ...)`.            |
| `PeerClose { peer_id }`                 | Remove `peer_client_by_peer_id[peer_id]` and its reverse; remove `peer_terms[peer_id]`.                                                                          |
| `LifecycleHint { event, .. }`           | `VisibilityHidden                                                                                                                                                | Pagehide | Freeze`→`runtime.flush_wal()`. Other events: no-op. |
| `UpdateAuth { jwt_token }`              | `current_auth_jwt = jwt_token`. Build auth JSON (admin_secret + jwt_token); call `runtime.update_auth(json)`. On error, post `AuthFailed { reason: "invalid" }`. |
| `DisconnectUpstream`                    | `runtime.disconnect(); post UpstreamDisconnected`.                                                                                                               |
| `ReconnectUpstream`                     | Build (ws_url, auth_json) from cached `current_ws_url` / `current_auth_jwt` / `current_admin_secret`. Call `perform_upstream_connect`.                           |
| `Shutdown`                              | `handle_shutdown(runtime, simulate_crash: false)`.                                                                                                               |
| `SimulateCrash`                         | `handle_shutdown(runtime, simulate_crash: true)`.                                                                                                                |
| `AcknowledgeRejectedBatch { batch_id }` | `runtime.acknowledge_rejected_batch(batch_id)`.                                                                                                                  |
| `DebugSchemaState`                      | `runtime.debug_schema_state()` → `DebugSchemaStateOk { state_json }`. None / error → `Error`.                                                                    |
| `DebugSeedLiveSchema { schema_json }`   | `runtime.debug_seed_live_schema(&schema_json)` → `runtime.flush_wal()` → `DebugSeedLiveSchemaOk`. Error → `Error`.                                               |

### 9.8 `handle_shutdown(runtime, _simulate_crash)`

```
HOST.state = ShuttingDown

if runtime is Some:
    runtime.batched_tick()        // drain any parked main/peer sync
    runtime.flush_wal()           // make sure writes reach OPFS
    runtime.install_noop_sync_sender()

global_worker_scope().set_onmessage(None)

RUNTIME = None
PEER_ROUTING = default()

post ShutdownOk
global_worker_scope().close()
HOST = None                       // WorkerHost drops here, closure goes with it
```

Both `Shutdown` and `SimulateCrash` follow the same path. On `opfs-btree`,
`flush_wal` is the only durability primitive (snapshot == WAL checkpoint),
so there is no distinct snapshot for the clean-shutdown flavour. The
`simulate_crash` flag is retained for future storage backends.

### 9.9 Rejected-batch replay

`sync_retained_local_batch_records(runtime)`:

```
match runtime.load_local_batch_records():
  Ok(batches)  → post LocalBatchRecordsSync { batches_json: JSON.stringify(batches) }
  Err(_)       → warn
```

`queue_rejected_batch_replay()`:

```
1. If HOST.rejected_replay_queued is already true → return (debounce).
2. Set HOST.rejected_replay_queued = true.
3. spawn_local(async {
     HOST.rejected_replay_queued = false
     let runtime = RUNTIME.clone().ok()?
     let batch_ids = runtime.drain_rejected_batch_ids()?      // returns Array
     for batch_id in batch_ids:
         let batch = runtime.load_local_batch_record(&batch_id)?
         if batch.is_null() or batch.is_undefined() → continue
         if !is_rejected_settlement(&batch) → continue
         post MutationErrorReplay { batch_json: JSON.stringify(batch) }
   })
```

`is_rejected_settlement(batch)`:

```
let s = Reflect::get(batch, "latestSettlement")?
if s.is_null() or undefined → false
Reflect::get(s, "kind").as_string() == Some("rejected")
```

### 9.10 Outbox callbacks (constructed in `run_init` step 5)

`make_peer_routing_lookup()` returns a `Function` that closes over
`PEER_ROUTING`:

```
|client_id: JsValue| -> JsValue {
    let Some(client) = client_id.as_string() else { return NULL };
    PEER_ROUTING.with(|c| {
        let g = c.borrow();
        let Some(peer_id) = g.peer_id_by_client.get(&client) else { return NULL };
        let term = g.peer_terms.get(peer_id).copied().unwrap_or(0);
        let obj = Object::new();
        Reflect::set(&obj, "peerId", &JsValue::from_str(peer_id))?;
        Reflect::set(&obj, "term",   &JsValue::from_f64(term as f64))?;
        obj.into()
    })
}
```

`make_on_main_sync_flushed()` returns a `Function` that calls
`queue_rejected_batch_replay()`.

### 9.11 URL normalisation

```rust
fn http_url_to_ws(server_url: &str, app_id: &str) -> String;
```

Rules (must match exactly — covered by in-source tests):

| Input prefix  | Output prefix                                   |
| ------------- | ----------------------------------------------- |
| `https://`    | `wss://`                                        |
| `http://`     | `ws://`                                         |
| `wss://`      | `wss://` (passthrough — must NOT double-prefix) |
| `ws://`       | `ws://` (passthrough)                           |
| anything else | `ws://` (assume plain `host:port`)              |

Then strip trailing `/` (repeatedly) and append `/apps/{app_id}/ws`.
Examples:

- `https://example.test`, `app-1` → `wss://example.test/apps/app-1/ws`
- `wss://relay.example`, `x` → `wss://relay.example/apps/x/ws`
- `https://example.test///`, `a` → `wss://example.test/apps/a/ws`
- `example.test:4000`, `a` → `ws://example.test:4000/apps/a/ws`

### 9.12 Auth-reason normalisation

```rust
fn map_auth_reason(reason: &str) -> &'static str {
    match reason {
        "Unauthorized" | "expired" => "expired",
        "missing" | "Missing token" => "missing",
        "disabled" | "Auth disabled" => "disabled",
        _ => "invalid",
    }
}
```

Exact-match only. `"Unauthorized "` (trailing space) maps to `"invalid"`.

### 9.13 In-source tests

`#[cfg(test)]` tests for both `http_url_to_ws` and `map_auth_reason`. These
cover the deleted `jazz-worker.test.ts` cases that have a Rust analogue.

## 10. JS Worker Bootstrap Shim

File: `packages/jazz-tools/src/worker/jazz-worker.ts`. ~200 lines.

Responsibilities, in order:

1. Vitest browser-mode shim (`ensureVitestWorkerImportShim`): install a
   no-op `wrapDynamicImport` on `globalThis.__vitest_browser_runner__` if
   missing.
2. `startup()`:
   - `await import("jazz-wasm")`.
   - If the worker location already pins a WASM URL via
     `readWorkerRuntimeWasmUrl(self.location?.href)`, pre-init WASM via
     `ensureWasmInitialized(wasmModule, undefined)`.
   - Post `{type:"ready"}`.
   - On error, post `{type:"error", message: "WASM load failed: …"}`.
3. `self.onmessage`:
   - If `initMessage` is `null` and the incoming `data` is a plain object
     with `type === "init"` and is not a `Uint8Array`:
     - Store `initMessage = data`.
     - Kick off `bootstrapAndHandoff(initMessage)` (do not `await`).
   - Otherwise push the raw `data` onto the `pendingMessages: unknown[]`
     queue. (Both binary `Uint8Array` and any malformed `{type:"x"}` JS
     objects pass through — Rust validates downstream.)
4. `bootstrapAndHandoff(init)`:
   - `await import("jazz-wasm")` again (cached).
   - Set `globalThis.__JAZZ_WASM_LOG_LEVEL = init.logLevel ?? "warn"`.
   - `await ensureWasmInitialized(wasmModule, init)`.
   - `installWasmTelemetry({ wasmModule, collectorUrl: init.telemetryCollectorUrl, appId: init.appId, runtimeThread: "worker" })`.
   - `wasmModule.runAsWorker(init, pendingMessages.slice()); pendingMessages.length = 0`.
   - On error, post `{type:"error", message: "Init failed: …"}`.

`ensureWasmInitialized(wasmModule, msg)`:

- If already initialised → return.
- If `resolveRuntimeConfigSyncInitInput(msg?.runtimeSources)` returns a
  non-null sync input → `wasmModule.initSync(syncInitInput)`; done.
- If `wasmModule.default` is not a function → already initialised; done.
- Try resolving the WASM URL from `runtimeSources` or
  `readWorkerRuntimeWasmUrl`. If found → `wasmModule.default({ module_or_path: url })`.
- Otherwise try `runWithRootRelativeFetchSupport(() => wasmModule.default())`
  (transparently patches `fetch` so `/foo.wasm` resolves against
  `self.location.origin` for the duration of the call).
- On the catch path, try to parse the WASM path out of the error message via
  `resolveAbsoluteWasmUrlFromInitError`, fall back to
  `msg?.fallbackWasmUrl`. If found → retry with explicit `module_or_path`.

Rust takes over `self.onmessage` synchronously inside `runAsWorker`. After
that call, the JS shim's handler is gone. Any messages that arrived
**during** the `bootstrapAndHandoff` call landed in `pendingMessages`
because the shim handler was still installed at the time of receipt; they
are forwarded to Rust via the `runAsWorker` second argument.

## 11. TypeScript Adapter

File: `packages/jazz-tools/src/runtime/worker-bridge.ts`. ~250 lines.

Preserves the historical TS surface so `db.ts` does not need to change in
this round. Internally defers Rust attach until `init()`.

### 11.1 Shape

```ts
export type WorkerLifecycleEvent =
  | "visibility-hidden"
  | "visibility-visible"
  | "pagehide"
  | "freeze"
  | "resume";

export interface WorkerBridgeOptions {
  schemaJson: string;
  appId: string;
  env: string;
  userBranch: string;
  dbName: string;
  serverUrl?: string;
  jwtToken?: string;
  adminSecret?: string;
  runtimeSources?: RuntimeSourcesConfig;
  fallbackWasmUrl?: string;
  logLevel?: "error" | "warn" | "info" | "debug" | "trace";
  telemetryCollectorUrl?: string;
}

export interface PeerSyncBatch {
  peerId: string;
  term: number;
  payload: Uint8Array[];
}

export class WorkerBridge {
  constructor(worker: Worker, runtime: Runtime);
  init(options: WorkerBridgeOptions): Promise<string>;
  updateAuth(auth: { jwtToken?: string }): void;
  sendLifecycleHint(event: WorkerLifecycleEvent): void;
  shutdown(): Promise<void>;
  getWorkerClientId(): string | null;
  setServerPayloadForwarder(fwd: ((p: Uint8Array) => void) | null): void;
  applyIncomingServerPayload(payload: Uint8Array): void;
  waitForUpstreamServerConnection(): Promise<void>;
  replayServerConnection(): void;
  disconnectUpstream(): void;
  reconnectUpstream(): void;
  acknowledgeRejectedBatch(batchId: string): void;
  simulateCrash(): Promise<void>;
  onPeerSync(listener: (batch: PeerSyncBatch) => void): void;
  onAuthFailure(listener: (reason: AuthFailureReason) => void): void;
  onLocalBatchRecordsSync(listener: (b: LocalBatchRecord[]) => void): void;
  onMutationErrorReplay(listener: (b: LocalBatchRecord) => void): void;
  openPeer(peerId: string): void;
  sendPeerSync(peerId: string, term: number, payload: Uint8Array[]): void;
  closePeer(peerId: string): void;
}
```

### 11.2 Behaviour

- `constructor` stores `(worker, runtime)`. Does **not** attach.
- Listener setters and `setServerPayloadForwarder` accumulate state locally
  in a `ListenerSlots` object plus a `pendingForwarder` field. When the Rust
  bridge attaches, the adapter replays both via
  `bridge.setListeners(listeners)` and `bridge.setServerPayloadForwarder(fn)`.
- `init(options)`:
  - Memoised via `clientIdPromise`. Concurrent calls return the same Promise.
  - If `disposed === true` → return a rejected promise.
  - Resolve `runtime.createWorkerBridge` — if missing, reject with
    `"WorkerBridge requires a WasmRuntime with `createWorkerBridge`"`.
  - Try `runtime.createWorkerBridge(this.worker, options)` synchronously. On
    throw, store the rejection.
  - On success, store the handle, call `bridge.setListeners(this.listeners)`,
    install any `pendingForwarder`, then `bridge.init().then(r => r.clientId)`.
  - Coerce non-Error rejections to `Error` before propagating.
- `shutdown()`:
  - Idempotent: if already disposed, return resolved.
  - Set `disposed = true`. If `bridge` exists, `await bridge.shutdown()` then
    set `bridge = null`. Swallow shutdown errors.
- All other methods are straight pass-throughs that no-op when `bridge` is
  null.

The adapter is explicitly a migration aid. The end state is direct
`WasmWorkerBridge` usage from `db.ts` and deletion of this file.

## 12. `db.ts` Touch-ups

`packages/jazz-tools/src/runtime/db.ts`:

1. Re-import `WorkerLifecycleEvent` from `./worker-bridge.js` (the old
   `../worker/worker-protocol.js` import is gone — see §13.4).
2. Three sites call `workerBridge.shutdown(currentWorker)` today. Change
   each to `workerBridge.shutdown()` (the Rust bridge owns the `Worker`
   handle).
3. The mutation-error-replay handler must guard against double-delivery:
   when the JazzClient has a `wait()` waiter registered for a batch, the
   waiter's reject path will deliver the error; firing the
   `onMutationError` listeners as well is a double-delivery bug.

   Add a `hasPendingBatchWaiter(batchId)` query to `JazzClient` and short-
   circuit the listener fan-out when it returns `true`:

   ```ts
   // in the `mutation-error-replay` handler:
   if (client.hasPendingBatchWaiter(batch.batchId)) return;
   ```

   `JazzClient.hasPendingBatchWaiter(batchId)` lives in
   `packages/jazz-tools/src/runtime/client.ts`:

   ```ts
   hasPendingBatchWaiter(batchId: string): boolean {
     return (this.pendingBatchWaiters.get(batchId)?.length ?? 0) > 0;
   }
   ```

`packages/jazz-tools/src/runtime/client.ts`:

- Drop the `RuntimeSyncOutboxCallback` import.
- Remove the `onSyncMessageToSend?` field from the `Runtime` interface.
- Add `createWorkerBridge?(worker: Worker, options: object): unknown` and
  `batchedTick?(): void`.
- Add the `hasPendingBatchWaiter` helper above to `JazzClient`.

`packages/jazz-tools/src/types/jazz-wasm.d.ts`:

- Drop `onSyncMessageToSend` from `WasmRuntime`.
- Add `createWorkerBridge(worker: Worker, options: unknown): WasmWorkerBridge`.
- Declare `class WasmWorkerBridge` mirroring the §8.1 surface.
- Declare `function runAsWorker(initMessage: unknown, pendingMessages: unknown[]): void`.
- Declare the four `encode*Js` / `decode*Js` test helpers.

## 13. Deletions

After all of the above lands, delete:

1. `packages/jazz-tools/src/worker/worker-protocol.ts` — the TS-side protocol
   enums are gone; `worker_protocol.rs` is the source of truth.
2. `packages/jazz-tools/src/worker/jazz-worker.test.ts` — the surface is now
   Rust-internal.
3. `packages/jazz-tools/src/runtime/worker-bridge.test.ts` — replaced by
   `crates/jazz-wasm/tests/worker_bridge.rs`.
4. `packages/jazz-tools/src/runtime/worker-bridge.race-harness.test.ts` —
   same.
5. `WasmRuntime::on_sync_message_to_send` and the `JsSyncSender` type from
   `crates/jazz-wasm/src/runtime.rs`.
6. Any `unsafe impl Send for JsSyncSender` in `runtime.rs` (now unneeded —
   see §3).
7. `createSyncOutboxRouter` from `packages/jazz-tools/src/runtime/sync-transport.ts`
   (if no other callers remain after the bridge migration).

Keep `WasmRuntime::on_sync_message_received_from_client` — it is still
public for direct-mode tests, and is called internally by the worker host.

## 14. Test Plan

The test plan is split into three layers, each with a clear ownership boundary:

1. **In-source unit tests** in `worker_protocol.rs` and `worker_host.rs` —
   pure-function coverage (codec round-trip, URL/auth-reason normalisation).
2. **Browser integration tests** in `crates/jazz-wasm/tests/worker_bridge.rs`
   driving `WasmWorkerBridge` against a synthetic-Worker JS object via
   `wasm-bindgen-test`.
3. **End-to-end browser smoke** in `packages/jazz-tools/tests/browser/*.ts`
   (Vitest browser mode) — left in place as the cross-stack regression net.

### 14.1 In-Source Codec Round-Trip (`worker_protocol.rs`)

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn rt_main(msg: &MainToWorkerWire) {
        let bytes = postcard::to_allocvec(msg).expect("encode");
        let decoded: MainToWorkerWire = postcard::from_bytes(&bytes).expect("decode");
        assert_eq!(format!("{:?}", msg), format!("{:?}", decoded));
    }

    fn rt_worker(msg: &WorkerToMainWire) {
        let bytes = postcard::to_allocvec(msg).expect("encode");
        let decoded: WorkerToMainWire = postcard::from_bytes(&bytes).expect("decode");
        assert_eq!(format!("{:?}", msg), format!("{:?}", decoded));
    }

    #[test] fn main_to_worker_round_trips() { /* every variant */ }
    #[test] fn worker_to_main_round_trips() { /* every variant, all four SyncEntry shapes */ }
}
```

`main_to_worker_round_trips` must cover, at minimum:

- `Sync { payloads: vec![ByteBuf::from(vec![1,2,3]), ByteBuf::from(vec![4,5])] }`
- `PeerOpen { peer_id: "tab-a".into() }`
- `PeerSync { peer_id: "tab-b".into(), term: 7, payloads: vec![ByteBuf::from(vec![9,8,7])] }`
- `PeerClose { peer_id: "tab-c".into() }`
- `LifecycleHint { event: WorkerLifecycleEvent::VisibilityHidden, sent_at_ms: 1_700_000_000_000.0 }`
- `UpdateAuth { jwt_token: Some("jwt".into()) }` _and_ `UpdateAuth { jwt_token: None }`
- `DisconnectUpstream`, `ReconnectUpstream`, `Shutdown`, `SimulateCrash`,
  `DebugSchemaState`
- `AcknowledgeRejectedBatch { batch_id: "b1".into() }`
- `DebugSeedLiveSchema { schema_json: "{}".into() }`

`worker_to_main_round_trips` must cover:

- `InitOk { client_id: "c1".into() }`
- `UpstreamConnected`, `UpstreamDisconnected`
- `Sync { payloads: vec![BareBytes(...), BareString(...), SequencedBytes{...}, SequencedString{...}] }`
  — all four `SyncEntry` variants in one envelope
- `PeerSync { peer_id: "p".into(), term: 1, payloads: vec![ByteBuf::from(vec![0xff])] }`
- `LocalBatchRecordsSync { batches_json: "[]".into() }`
- `MutationErrorReplay { batch_json: "{}".into() }`
- `Error { message: "oops".into() }`
- `AuthFailed { reason: "expired".into() }`
- `ShutdownOk`
- `DebugSchemaStateOk { state_json: "{}".into() }`
- `DebugSeedLiveSchemaOk`

These cases run on `cargo test -p jazz-wasm` (host toolchain, no browser
needed) because they only exercise the pure codec.

### 14.2 In-Source Worker-Host Helpers (`worker_host.rs`)

`http_url_to_ws` and `map_auth_reason` are pure functions; cover them in
the same module under `#[wasm_bindgen_test]` (they live in a wasm-only
file). Tests:

| Test                                           | Input                                                                                                  | Expected                                                                       |
| ---------------------------------------------- | ------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------ |
| `http_url_to_ws_normalises_https`              | `("https://example.test", "app-1")`                                                                    | `"wss://example.test/apps/app-1/ws"`                                           |
| `http_url_to_ws_normalises_http`               | `("http://localhost:4000", "xyz")`                                                                     | `"ws://localhost:4000/apps/xyz/ws"`                                            |
| `http_url_to_ws_passes_wss_through`            | `("wss://relay.example", "x")`                                                                         | `"wss://relay.example/apps/x/ws"` (must not double-prefix `wss://`)            |
| `http_url_to_ws_passes_ws_through`             | `("ws://relay.example", "x")`                                                                          | `"ws://relay.example/apps/x/ws"`                                               |
| `http_url_to_ws_strips_trailing_slash` (1)     | `("https://example.test/", "a")`                                                                       | `"wss://example.test/apps/a/ws"`                                               |
| `http_url_to_ws_strips_trailing_slash` (2)     | `("https://example.test///", "a")`                                                                     | `"wss://example.test/apps/a/ws"`                                               |
| `http_url_to_ws_defaults_unknown_scheme_to_ws` | `("example.test:4000", "a")`                                                                           | `"ws://example.test:4000/apps/a/ws"`                                           |
| `map_auth_reason_recognises_known_strings`     | each of `"Unauthorized"`, `"expired"`, `"missing"`, `"Missing token"`, `"disabled"`, `"Auth disabled"` | `"expired"`, `"expired"`, `"missing"`, `"missing"`, `"disabled"`, `"disabled"` |
| `map_auth_reason_falls_back_to_invalid`        | `""`, `"totally unrecognised"`, `"Unauthorized "` (trailing space)                                     | `"invalid"` (each)                                                             |

### 14.3 Browser Integration Tests — Setup

File: `crates/jazz-wasm/tests/worker_bridge.rs`.

#### 14.3.1 wasm-bindgen-test configuration

```rust
#![cfg(target_arch = "wasm32")]

use wasm_bindgen_test::*;
wasm_bindgen_test_configure!(run_in_browser);
```

Run with:

```sh
RUSTFLAGS='--cfg=web_sys_unstable_apis --cfg getrandom_backend="wasm_js"' \
  wasm-pack test --headless --chrome crates/jazz-wasm
```

#### 14.3.2 Schema fixture

Every test that constructs a `WasmRuntime` uses the same minimal schema:

```rust
const SCHEMA_JSON: &str = r#"{
    "todos": {
        "columns": [
            {"name": "title",     "column_type": {"type": "Text"},    "nullable": false},
            {"name": "completed", "column_type": {"type": "Boolean"}, "nullable": false}
        ]
    }
}"#;
```

#### 14.3.3 The `FakeWorker` harness

The bridge calls exactly two members on its `Worker` handle:
`postMessage(message, transfer?)` and `set_onmessage(handler)`. The harness
exposes both via a duck-typed JS object and downcasts to `web_sys::Worker`
via `unchecked_into`.

```rust
struct FakeWorker {
    obj: JsValue,
    posted: Rc<RefCell<Vec<JsValue>>>,
    _post_message_closure: Closure<dyn FnMut(JsValue, JsValue)>,
}

impl FakeWorker {
    fn new() -> Self {
        let posted = Rc::new(RefCell::new(Vec::<JsValue>::new()));
        let posted_clone = Rc::clone(&posted);
        let post_message_closure =
            Closure::<dyn FnMut(JsValue, JsValue)>::new(
                move |msg: JsValue, _transfer: JsValue| {
                    posted_clone.borrow_mut().push(msg);
                });
        let obj = Object::new();
        Reflect::set(&obj, &"postMessage".into(),
                     post_message_closure.as_ref().unchecked_ref()).unwrap();
        Reflect::set(&obj, &"onmessage".into(), &JsValue::NULL).unwrap();
        Self {
            obj: obj.into(),
            posted,
            _post_message_closure: post_message_closure,
        }
    }

    fn worker(&self) -> Worker { self.obj.clone().unchecked_into() }

    /// Postcard-encode a worker → main wire variant, wrap in MessageEvent-
    /// shaped `{data: Uint8Array}`, and dispatch through the bridge's
    /// installed `onmessage`.
    fn emit_wire(&self, msg: &WorkerToMainWire) {
        let bytes = encode_worker_to_main(msg).expect("encode worker→main");
        self.emit_data(Uint8Array::from(bytes.as_slice()).into());
    }

    /// Dispatch an arbitrary JS value (object or Uint8Array) as the `.data`
    /// of a synthetic MessageEvent.
    fn emit_data(&self, data: JsValue) {
        let event = Object::new();
        Reflect::set(&event, &"data".into(), &data).unwrap();
        let onmessage = Reflect::get(&self.obj, &"onmessage".into()).unwrap();
        let f: Function = onmessage.dyn_into()
            .expect("bridge has not installed an onmessage handler");
        f.call1(&JsValue::NULL, &event.into()).expect("dispatch fake message");
    }

    fn posted_decoded(&self) -> Vec<MainToWorkerWire> {
        self.posted.borrow().iter()
            .filter_map(|v| v.dyn_ref::<Uint8Array>()
                .and_then(|arr| postcard::from_bytes(&arr.to_vec()).ok()))
            .collect()
    }

    fn last_posted_decoded(&self) -> Option<MainToWorkerWire> {
        self.posted_decoded().pop()
    }
}
```

Properties the harness must preserve:

- `_post_message_closure` is kept alive on the struct (without it,
  `into_js_value` would either leak permanently or be GC'd mid-test).
- `posted` retains **every** value posted, in order, in raw `JsValue` form.
  Decoding to a typed wire is a derived view (`posted_decoded`).
- `emit_data` panics if the bridge has not installed `onmessage` yet — this
  is intentional, because every test that synthesises inbound traffic must
  first ensure the bridge has called `set_onmessage`. `attach()` does this
  before returning, so the panic is purely diagnostic.

#### 14.3.4 Options builder

```rust
fn build_options(server_url: Option<&str>) -> JsValue {
    let opts = Object::new();
    Reflect::set(&opts, &"schemaJson".into(),  &SCHEMA_JSON.into()).unwrap();
    Reflect::set(&opts, &"appId".into(),       &"test-app".into()).unwrap();
    Reflect::set(&opts, &"env".into(),         &"dev".into()).unwrap();
    Reflect::set(&opts, &"userBranch".into(),  &"main".into()).unwrap();
    Reflect::set(&opts, &"dbName".into(),      &"db".into()).unwrap();
    if let Some(u) = server_url {
        Reflect::set(&opts, &"serverUrl".into(), &u.into()).unwrap();
    }
    opts.into()
}
```

#### 14.3.5 Runtime fixture

```rust
fn fresh_runtime() -> WasmRuntime {
    WasmRuntime::new(SCHEMA_JSON, "test-app", "dev", "main", None, Some(true))
        .expect("WasmRuntime::new")
}
```

`Some(true)` requests binary encoding so the runtime emits postcard payloads
that the bridge's sender can postcard-batch without re-encoding.

#### 14.3.6 Async yield helper

The bridge schedules outbox flushes via `wasm_bindgen_futures::spawn_local`,
which lands on the microtask queue. Tests that observe a flush must yield
through a `setTimeout(0)` to let those microtasks drain:

```rust
async fn yield_once() {
    let promise = js_sys::Promise::new(&mut |resolve, _reject| {
        let global = js_sys::global();
        let set_timeout: Function = Reflect::get(&global, &"setTimeout".into())
            .unwrap().unchecked_into();
        let _ = set_timeout.call2(&JsValue::NULL, &resolve, &JsValue::from_f64(0.0));
    });
    JsFuture::from(promise).await.expect("yield");
}
```

### 14.4 Browser Integration Tests — Cases

Every case below is `#[wasm_bindgen_test]`. Where the test is `async fn`,
it `await`s a `JsFuture` somewhere in the body; sync cases do not.

#### 14.4.1 `init_resolves_with_client_id` (async)

**Purpose.** End-to-end init success path: a JS-object init envelope is
posted, the bridge resolves the returned Promise on `InitOk`, and
`getWorkerClientId()` reflects the received id.

**Body.**

1. Build `FakeWorker`, `fresh_runtime()`, `attach()` with no `serverUrl`.
2. Call `bridge.init()` and keep the returned Promise.
3. Assert one of the captured `postMessage` payloads is a JS object with
   `type === "init"` (it is the only non-binary thing the bridge posts).
4. `emit_wire(InitOk { client_id: "client-42" })`.
5. `JsFuture::from(init_promise).await.expect("init resolved")`.
6. Assert `result.clientId === "client-42"`.
7. Assert `bridge.get_worker_client_id().as_string() == Some("client-42")`.

#### 14.4.2 `init_propagates_error` (async)

**Purpose.** Init error path via binary `WorkerToMainWire::Error`.

**Body.**

1. Attach (no server url) and call `init()`.
2. `emit_wire(Error { message: "schema mismatch" })`.
3. Await the init Promise; assert it rejected.
4. Convert the rejection to a string; assert it contains `"schema mismatch"`.

#### 14.4.3 `init_propagates_js_object_error_from_shim` (async)

**Purpose.** Pre-handoff worker errors (WASM load failures inside the JS
shim) arrive as `{type:"error", message}` JS objects because the WASM
runtime is not yet loaded to encode a postcard wire. The bridge must
surface that message on the init Promise instead of dropping it and
timing out.

**Body.**

1. Attach and call `init()`.
2. Build a JS `{type:"error", message:"WASM load failed: HTTP 404"}` object
   and `emit_data(...)`.
3. Await; assert rejection contains `"WASM load failed: HTTP 404"`.

#### 14.4.4 `init_is_memoized` (async)

**Purpose.** Concurrent callers see the same Promise; only one init JS
object is posted.

**Body.**

1. Attach.
2. `let p1 = bridge.init(); let p2 = bridge.init();`
3. Compare with `JsValue::from(p1.clone()) == JsValue::from(p2.clone())` —
   `js_sys::Promise` is identity-comparable via `JsValue`-equality.
4. Filter `posted` for JS objects with `type === "init"`; assert count
   exactly `1`.
5. `emit_wire(InitOk { client_id: "c" })`; await both Promises, both must
   resolve.

#### 14.4.5 `update_auth_emits_postcard_binary` (sync)

**Purpose.** Outbound `update_auth(Some("jwt-x"))` produces a binary
`MainToWorkerWire::UpdateAuth { jwt_token: Some("jwt-x") }`.

**Body.**

1. Attach.
2. `bridge.update_auth(Some("jwt-x".into()))`.
3. `let last = fw.last_posted_decoded();`
4. Match `last` against `Some(MainToWorkerWire::UpdateAuth { jwt_token })`
   and assert `jwt_token.as_deref() == Some("jwt-x")`.

#### 14.4.6 `peer_sync_fires_listener` (sync)

**Purpose.** Inbound `WorkerToMainWire::PeerSync` invokes the `onPeerSync`
listener with the expected JS-shaped batch.

**Setup details.** Listener is a `Closure<dyn FnMut(JsValue)>` registered
via `set_listeners({onPeerSync: <closure>})`. The closure reads the JS
object back via `Reflect::get` and captures into a shared `Rc<RefCell<…>>`.

**Body.**

1. Attach; register the listener.
2. `emit_wire(PeerSync { peer_id: "tab-b", term: 7, payloads: vec![ByteBuf::from(vec![1,2,3])] })`.
3. Assert captured length `1`, and the entry equals `("tab-b", 7, 1)`.

The captured tuple is `(peerId, term, payload_array_length)` — verifying the
listener received a `{peerId, term, payload: Uint8Array[]}` shape with one
entry.

#### 14.4.7 `shutdown_resolves_on_ack` (async)

**Purpose.** `shutdown()` posts a binary `Shutdown` and resolves on
`ShutdownOk`.

**Body.**

1. Attach.
2. `let p = bridge.shutdown();`
3. Assert `fw.last_posted_decoded() == Some(MainToWorkerWire::Shutdown)`.
4. `emit_wire(ShutdownOk)`.
5. `JsFuture::from(p).await.expect("shutdown ack")`.

#### 14.4.8 `lifecycle_hint_emits_postcard_binary` (sync)

**Purpose.** Lifecycle string parses into the kebab-case enum variant.

**Body.**

1. Attach.
2. `bridge.send_lifecycle_hint("visibility-hidden")`.
3. Match the last posted wire against
   `Some(MainToWorkerWire::LifecycleHint { event, .. })` and assert
   `event == WorkerLifecycleEvent::VisibilityHidden`.

#### 14.4.9 `unknown_inbound_js_object_is_dropped_quietly` (sync)

**Purpose.** Forward-compat: a JS-object envelope with an unrecognised
`type` field must be logged-and-dropped, not panic or surface as an error.

**Body.**

1. Attach.
2. Build `{type:"some-future-message"}` and `emit_data(...)`.
3. No assertion beyond "did not panic". If the dispatch is wired wrong,
   `emit_data` raises an uncaught error and `wasm_bindgen_test` fails the
   test automatically.

#### 14.4.10 `main_to_worker_main_side_sync_envelope` (sync)

**Purpose.** Lock the wire shape of `MainToWorkerWire::Sync` independently
of the send path so a serialiser regression is caught even if every other
test happens to bypass it.

**Body.**

1. `let bytes = encode_main_to_worker(&MainToWorkerWire::Sync { payloads: vec![ByteBuf::from(vec![9])] }).expect("encode");`
2. `let decoded: MainToWorkerWire = postcard::from_bytes(&bytes).expect("decode");`
3. Match `decoded` against `Sync { payloads }`, assert `payloads.len() == 1`
   and `&*payloads[0] == &[9]`.

#### 14.4.11 `peer_open_send_close_emit_postcard_binary` (sync)

**Purpose.** All three peer-channel verbs produce the matching binary
envelope.

**Body.**

1. Attach.
2. `bridge.open_peer("peer-α")` → match `PeerOpen { peer_id }`, assert
   `peer_id == "peer-α"`.
3. Build a `js_sys::Array` with two `Uint8Array`s (`[1,2,3]` and `[4]`).
4. `bridge.send_peer_sync("peer-α", 5, array)` → match `PeerSync {peer_id,
term, payloads}`, assert `peer_id == "peer-α"`, `term == 5`,
   `payloads.len() == 2`, `&*payloads[0] == &[1,2,3]`, `&*payloads[1] == &[4]`.
5. `bridge.close_peer("peer-α")` → match `PeerClose { peer_id }`, assert
   `peer_id == "peer-α"`.

#### 14.4.12 `send_peer_sync_drops_empty_payload` (sync)

**Purpose.** Empty `payload` array is a no-op — must not produce any
`postMessage`.

**Body.**

1. Attach. Record `posted_before = fw.posted.borrow().len()`.
2. `bridge.send_peer_sync("p", 0, js_sys::Array::new())`.
3. Assert `fw.posted.borrow().len() == posted_before`.

#### 14.4.13 `acknowledge_rejected_batch_emits_postcard_binary` (sync)

**Purpose.** `acknowledge_rejected_batch(batch_id)` produces the matching
binary envelope.

**Body.**

1. Attach.
2. `bridge.acknowledge_rejected_batch("batch-7")`.
3. Match `AcknowledgeRejectedBatch { batch_id }`, assert `batch_id == "batch-7"`.

#### 14.4.14 `disconnect_and_reconnect_upstream_emit_postcard_binary` (sync)

**Body.**

1. Attach.
2. `bridge.disconnect_upstream()` → assert `last_posted_decoded() ==
Some(DisconnectUpstream)`.
3. `bridge.reconnect_upstream()` → assert `last_posted_decoded() ==
Some(ReconnectUpstream)`.

#### 14.4.15 `forwarder_routes_server_bound_through_callback` (async)

**Purpose.** Installing a `set_server_payload_forwarder` callback routes
server-bound outbox entries through the callback rather than batching them
to the worker. Removing the forwarder reverses the routing.

**Body.**

1. Attach (no `serverUrl`) and drive init to completion (`InitOk`,
   `JsFuture::from(init).await`).
2. Install a forwarder closure that captures each `Uint8Array` payload
   into `Rc<RefCell<Vec<Vec<u8>>>>`.
3. `let posted_before = fw.posted.borrow().len();`
4. `bridge.replay_server_connection()` — this calls `runtime.remove_server();
runtime.add_server(None, None)`, which emits catalogue server-bound
   traffic through the outbox.
5. `yield_once().await` to let `spawn_local` flushes run.
6. Assert `captured` is non-empty (forwarder received payloads).
7. Assert `fw.posted.borrow().len() == posted_before` (no worker
   postMessage).
8. `bridge.set_server_payload_forwarder(None)`.
9. `bridge.replay_server_connection(); yield_once().await;`
10. Assert `fw.posted.borrow().len() > posted_before` (worker received the
    traffic this time).

#### 14.4.16 `wait_for_upstream_short_circuits_without_server_url` (async)

**Purpose.** Bridge attached with no `serverUrl` resolves
`waitForUpstreamServerConnection` immediately.

**Body.**

1. Attach (no `serverUrl`).
2. `bridge.wait_for_upstream_server_connection().await.expect("resolves immediately");`

The test fails-by-timeout if the bridge fails to short-circuit (it would
block on an unresolved promise).

#### 14.4.17 `wait_for_upstream_resolves_on_connected_message` (async)

**Purpose.** With `serverUrl` set, `waitForUpstreamServerConnection` blocks
until `UpstreamConnected` arrives.

**Body.**

1. Attach with `serverUrl = Some("https://example.test")`.
2. Drive init to completion.
3. `let waiter = bridge.wait_for_upstream_server_connection();`
4. `fw.emit_wire(&UpstreamConnected);`
5. `waiter.await.expect("wait resolved");`

If the wait does not resolve on `UpstreamConnected`, the test deadlocks
(wasm_bindgen_test fails by timeout).

#### 14.4.18 `wait_for_upstream_short_circuits_when_forwarder_installed` (async)

**Purpose.** Installing a forwarder marks upstream effectively ready.

**Body.**

1. Attach with `serverUrl`.
2. Install a no-op forwarder.
3. `bridge.wait_for_upstream_server_connection().await.expect("forwarder resolves");`

Note: the test does **not** drive init first — the wait short-circuits
purely on `has_forwarder`.

#### 14.4.19 `auth_failed_fires_listener` (sync)

**Purpose.** Inbound `AuthFailed` invokes `onAuthFailure` with the reason
string.

**Body.**

1. Attach.
2. Register `onAuthFailure` via `set_listeners({onAuthFailure: <closure>})`.
3. `emit_wire(AuthFailed { reason: "expired" })`.
4. Assert captured equals `["expired"]`.

#### 14.4.20 `local_batch_records_sync_listener_decodes_json` (sync)

**Purpose.** `LocalBatchRecordsSync { batches_json }` is delivered to
`onLocalBatchRecordsSync` as the result of `JSON.parse(batches_json)`.

**Body.**

1. Attach; register `onLocalBatchRecordsSync` that captures the JsValue.
2. `emit_wire(LocalBatchRecordsSync { batches_json: r#"[{"batchId":"b1"}]"# })`.
3. Cast captured into `js_sys::Array`; assert length `1`; read
   `batches[0].batchId` via `Reflect::get`; assert `"b1"`.

#### 14.4.21 `mutation_error_replay_listener_decodes_json` (sync)

**Purpose.** `MutationErrorReplay { batch_json }` delivers
`JSON.parse(batch_json)` to `onMutationErrorReplay`.

**Body.**

1. Attach; register `onMutationErrorReplay` that captures the JsValue.
2. `emit_wire(MutationErrorReplay { batch_json: r#"{"batchId":"b9"}"# })`.
3. Read `batch.batchId` via `Reflect::get`; assert `"b9"`.

#### 14.4.22 `pre_init_outbox_traffic_is_buffered_until_init_ok` (async)

**Purpose.** Outbox traffic emitted while the bridge is `Initializing`
(or earlier — between `attach()` and `init()`) must not reach the worker
as a `Sync` envelope until after `InitOk`.

**Body.**

1. Attach. `attach()` calls `runtime.add_server(None, Some(1.0))`, which
   fires a synchronous `batched_tick`. The runtime emits catalogue traffic
   into the outbox sender; the sender's init-gate is closed, so the
   entries accumulate without scheduling a flush.
2. `yield_once().await` to let any spurious microtasks run.
3. Inspect `fw.posted_decoded()`. **It must be empty** — the only thing
   the worker should have seen at this point is the JS-object init
   envelope (filtered out by `posted_decoded` because that helper only
   decodes `Uint8Array` posts).

   _Important_: the test calls `init()` AFTER this assertion. The init
   JS-object envelope is only emitted on `init()`, not on `attach()`, so
   even though the harness counts it eventually, it is not visible to
   `posted_decoded` at step 3 because `init()` has not been called yet.
   This step asserts there were no binary `Sync` posts pre-init.

4. `let init = bridge.init(); emit_wire(InitOk { client_id: "c1" }); JsFuture::from(init).await;`
5. `yield_once().await` to let the post-init flush run.
6. Filter `fw.posted_decoded()` for `Sync { .. }` variants; assert
   non-empty.

#### 14.4.23 `ready_js_object_does_not_break_dispatch` (sync)

**Purpose.** The JS shim posts `{type:"ready"}` early. Bridge must accept
it (treated as a no-op) without panic or error.

**Body.**

1. Attach.
2. Build `{type:"ready"}` and `emit_data(...)`. No assertion beyond
   "did not panic / did not throw".

#### 14.4.24 `upstream_disconnected_rearms_wait` (async)

**Purpose.** A `UpstreamDisconnected` after `UpstreamConnected` must
re-arm the wait-for-upstream promise so a fresh
`waitForUpstreamServerConnection` call actually blocks.

**Body.**

1. Attach with `serverUrl`. Drive init.
2. `emit_wire(UpstreamConnected)`.
3. `bridge.wait_for_upstream_server_connection().await.expect("connected resolves");`
4. `emit_wire(UpstreamDisconnected)`.
5. `let waiter = bridge.wait_for_upstream_server_connection();`
6. `emit_wire(UpstreamConnected);`
7. `waiter.await.expect("re-arm resolves");`

If the bridge does not re-arm the promise on disconnect, step 5 returns an
already-resolved Promise from the previous `UpstreamConnected`, and step 7
never validates the re-arm logic. The deadlock-by-timeout shape of
`wasm_bindgen_test` flushes this out: if step 6 has no effect because step
5 already resolved, the test passes for the wrong reason — so a correct
implementation must ensure step 5 produces a fresh deferred Promise.

### 14.5 Browser-Level Smoke Tests (retained)

These Vitest browser-mode tests are **not** replaced by the Rust suite;
they remain in place as the cross-stack regression net and exercise the
full `Db` API:

- `packages/jazz-tools/tests/browser/db.worker-bootstrap.test.ts` — verifies
  end-to-end bootstrap from `createDb` through `init-ok`.
- `packages/jazz-tools/tests/browser/db.transport.test.ts` — verifies
  upstream WebSocket transport works through the new bridge.
- `packages/jazz-tools/tests/browser/db.all.test.ts` — broad smoke
  covering CRUD + sync.
- `packages/jazz-tools/tests/browser/db.auth-refresh.worker.test.ts` —
  exercises the `update_auth` + JWT refresh path.
- `packages/jazz-tools/tests/browser/worker-bridge.test.ts` — exercises
  the TS adapter surface and the postcard encode/decode JS helpers.

The retained tests should pass unchanged. Test bodies that mocked the
deleted `onSyncMessageToSend` callback must be rewritten in terms of
`fw.posted` semantics, or moved into the Rust suite.

### 14.6 Manual Smoke Matrix

Before declaring the migration complete, manually verify at least one app
from `dev/stress-tests/` _and_ one from `examples/` boots to `init-ok` in
worker mode under each supported bundler:

| Bundler        | Sample                               | Pass criterion                                       |
| -------------- | ------------------------------------ | ---------------------------------------------------- |
| Vite           | `examples/todo-client-localfirst-ts` | App loads, worker `init-ok` reaches main, CRUD works |
| Next/Turbopack | one Next-based example               | same                                                 |
| Webpack        | one Webpack-based example            | same                                                 |
| SvelteKit      | one SvelteKit-based example          | same                                                 |

The `runtimeSources.wasmModule` structured-clone behaviour varies between
bundlers and is the most likely place a smoke test will surface a
regression that the Rust suite cannot reach.

### 14.7 How to Run

```sh
# Pure codec round-trips (host toolchain):
cargo test -p jazz-wasm --lib

# Browser integration tests (24 cases + in-source worker_host helpers):
RUSTFLAGS='--cfg=web_sys_unstable_apis --cfg getrandom_backend="wasm_js"' \
  wasm-pack test --headless --chrome crates/jazz-wasm

# Workspace-wide build sanity:
cargo check --workspace --all-targets
cargo check -p jazz-wasm --target wasm32-unknown-unknown
wasm-pack build --target web --release crates/jazz-wasm

# JS-side Vitest:
pnpm -C packages/jazz-tools exec vitest run
```

### 14.8 Test Anti-Patterns to Avoid

- **Do not** drive `init()` before tests that exercise pre-init buffering
  (§14.4.22). The whole point is to observe the bridge's behaviour while
  the gate is still closed.
- **Do not** assume `postMessage` is delivered synchronously across the
  shared `Rc<RefCell<…>>` cell. The captures happen synchronously inside
  the harness, but flushes scheduled by `spawn_local` need a `yield_once()`.
- **Do not** rely on raw `posted.borrow().len()` to count "wire messages"
  — that count includes the JS-object init envelope. Use `posted_decoded`
  when you want only `Uint8Array` posts.
- **Do not** drop the listener `Closure` before the synthetic event has
  been emitted. Tests that register listeners and then immediately drop
  the `Closure` will see the listener fail silently (the wasm-bindgen
  trampoline is freed).
- **Do not** add an `await` between `let p1 = init(); let p2 = init();`
  in the memoisation test. The point is that the second call observes the
  cached `init_promise` without yielding.

## 15. Constants Summary

| Name                       | Value             | Location                                  |
| -------------------------- | ----------------- | ----------------------------------------- |
| `INIT_RESPONSE_TIMEOUT_MS` | 12_000            | `worker_bridge.rs`                        |
| `SHUTDOWN_ACK_TIMEOUT_MS`  | 5_000             | `worker_bridge.rs`                        |
| `DEFAULT_WASM_LOG_LEVEL`   | `"warn"`          | `jazz-worker.ts`                          |
| `__JAZZ_WASM_LOG_LEVEL`    | global on JS side | read by `runtime.rs` to set tracing level |

## 16. Error Handling Catalogue

| Source                                                   | Action                                                                                                |
| -------------------------------------------------------- | ----------------------------------------------------------------------------------------------------- |
| `parse_main_to_worker` returns `Err` (post-init)         | Post `WorkerToMainWire::Error { message: "malformed worker message: {e}" }`.                          |
| Unknown JS-object `type` from main → worker              | Post `Error { "first message must be `init`, got <unknown>" }` if pre-init; else log warn and ignore. |
| Duplicate `init` after host is `Initializing` or `Ready` | Post `Error { "ignoring duplicate init" }`.                                                           |
| `runtime.open_persistent` returns `SecurityError`        | Fall back to `open_ephemeral` (do not abort init).                                                    |
| Any other open error                                     | Return `Err(...)` from `run_init`; `run_as_worker` posts `Error { "Init failed: ..." }`.              |
| `runtime.connect` upstream error                         | Log error; post `UpstreamDisconnected`. (Do not block init.)                                          |
| `peer_routing_lookup` throws                             | Log warn; drop the entry; do not propagate.                                                           |
| `peer_routing_lookup` returns non-object                 | Treat as `null`; drop.                                                                                |
| `runtime.update_auth` returns `Err`                      | Log error; post `AuthFailed { reason: "invalid" }`.                                                   |
| `worker.postMessage` throws inside `init()`              | Reject `init()` with `"postMessage init: {err}"`; transition to `Failed`.                             |
| Init resolver dropped without sending                    | Reject with `"init resolver dropped"`.                                                                |
| Init timeout                                             | Reject with `"Worker init timeout"`.                                                                  |
| `Drop for WasmWorkerBridge` during exception path        | Best-effort cleanup; do **not** post `Shutdown`.                                                      |

## 17. Subtleties That Must Be Preserved

1. **Closure::drop does not clear the JS `onmessage` slot.** Both sides
   must call `target.set_onmessage(None)` before the closure goes out of
   scope, or a late inbound message hits a freed Rust trampoline.

2. **Listener reentrancy.** Every listener invocation in the message
   dispatch must clone the `Function` out of the borrow and drop the borrow
   _before_ invoking the function. Listeners may re-enter the bridge.

3. **Outbox flush before `Shutdown`.** The bridge must drain the main
   runtime (`batched_tick`) and synchronously flush the sender (`flush_now`)
   before posting `Shutdown`. The worker drops the runtime on `Shutdown`;
   any post-`Shutdown` sync arrives at a dead runtime.

4. **Init order on the worker side.** Upstream connect must run **before**
   draining pending sync messages. Drained main writes can produce
   server-bound outbox traffic; if the Rust transport is not yet installed,
   that traffic would route into the (now-closed) bootstrap-catalogue
   forwarder and be dropped.

5. **The flip to `Ready` must happen before the pending-message drain.**
   Otherwise the drain re-buffers each message it processes and the worker
   never makes progress.

6. **`InitOk` is the last thing the worker posts during init.** The bridge
   must be able to assume `Ready` is persistent by the time it dispatches
   subsequent traffic.

7. **`is_main: true` entries trigger `on_main_sync_flushed`.** This is the
   hook that schedules rejected-batch replay. The signal must fire only
   after a main-bound flush actually completes, not on every flush.

8. **The init gate is per-`RustOutboxSender` instance.** The bridge holds
   the gate closed only on its own sender; the worker-side sender starts
   with the gate open.

9. **Worker-side server-bound traffic without the bootstrap flag is
   dropped, not posted.** The runtime's `connect()` installs a transport
   handle that takes over the server-bound path; the outbox sender's
   server-bound case only kicks in during the bootstrap window.

10. **`runtimeSources` round-trip.** The init envelope is **not** decoded
    via serde end-to-end. `InitPayloadFields` serdes the scalars; the
    `runtimeSources` slot rides on the JS object via `Reflect::set` /
    `Reflect::get`. Trying to serde it round-trips through
    `serde-wasm-bindgen` and loses module identity.

## 18. Files Touched, Concrete List

Create:

- `crates/jazz-wasm/src/worker_protocol.rs`
- `crates/jazz-wasm/src/worker_host.rs`
- `crates/jazz-wasm/src/worker_bridge.rs`
- `crates/jazz-wasm/tests/worker_bridge.rs`
- `.changeset/rust-owned-worker-bridge.md`

Modify:

- `crates/jazz-tools/src/runtime_core/mod.rs` — cfg-gate `+ Send` on
  `sync_sender` (§3).
- `crates/jazz-wasm/Cargo.toml` — add `postcard` and `web-sys` features (§4).
- `crates/jazz-wasm/src/lib.rs` — module + re-exports (§5).
- `crates/jazz-wasm/src/runtime.rs` — delete `JsSyncSender`,
  `on_sync_message_to_send`, the matching wasm-bindgen export; add
  `RustOutboxSender`, `NoopSyncSender`, `install_noop_sync_sender`,
  `createWorkerBridge`.
- `packages/jazz-tools/src/runtime/worker-bridge.ts` — replace with the
  thin adapter (§11).
- `packages/jazz-tools/src/worker/jazz-worker.ts` — replace with the
  bootstrap shim (§10).
- `packages/jazz-tools/src/types/jazz-wasm.d.ts` — update declarations
  (§12).
- `packages/jazz-tools/src/runtime/client.ts` — interface + helper (§12).
- `packages/jazz-tools/src/runtime/db.ts` — `shutdown()` arity + replay
  guard (§12).

Delete:

- `packages/jazz-tools/src/worker/worker-protocol.ts`.
- `packages/jazz-tools/src/worker/jazz-worker.test.ts`.
- `packages/jazz-tools/src/runtime/worker-bridge.test.ts`.
- `packages/jazz-tools/src/runtime/worker-bridge.race-harness.test.ts`.

## 19. Acceptance Criteria

The implementation is complete when:

1. `cargo check --workspace --all-targets` is clean.
2. `cargo check -p jazz-wasm --target wasm32-unknown-unknown` is clean.
3. `wasm-pack build --target web --release` succeeds.
4. `cargo test -p jazz-tools --lib` passes.
5. All 24 `#[wasm_bindgen_test]` cases in §14.2 pass.
6. All in-source protocol/host unit tests in §14.1 pass.
7. `pnpm exec vitest run` passes (the worker-side smoke tests included).
8. Manual smoke against at least one app from `dev/stress-tests/` and one
   from `examples/` per supported bundler boots to `init-ok` in worker
   mode. Bundlers: Vite, Next/Turbopack, Webpack, SvelteKit.
9. No new `unsafe impl` anywhere in `jazz-wasm`.
10. The TS adapter is the only TS code touching the bridge protocol; all
    other call sites in `db.ts` interact with `WorkerBridge` exactly the
    way they do today.

## 20. Open Decisions Left to the Implementer

These are intentional gaps where the spec does not pin a choice:

- **Synthetic-Worker test harness shape.** The §14.2 tests assume a
  Rust-side synthetic Worker built via `js_sys::Object` + `unchecked_into`.
  If this proves unstable, a Vitest-browser harness against the public
  `WasmWorkerBridge` API is an acceptable substitute for the higher-level
  scenarios (init race, follower-tab forwarder swap, peer term changes
  during sync flushes). The protocol round-trips and outbox unit cases
  stay in `#[wasm_bindgen_test]` regardless.
- **Field order inside the postcard enums.** Postcard is positional; the
  field order in the `enum` declaration _is_ the wire format. Keep the
  order shown in §6.5 and §6.6. If you reorder fields, you break the wire.
- **Whether `WorkerBridge` (the TS adapter) survives long-term.** This spec
  keeps it as a migration aid. The follow-up rewires `db.ts` to use the
  Rust bridge directly and deletes the adapter. That follow-up is out of
  scope here.
