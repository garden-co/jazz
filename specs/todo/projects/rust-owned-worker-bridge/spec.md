# Rust-Owned Worker Bridge

Move the browser dedicated-worker bridge — the SyncSender JS callback, the init handshake, peer routing, lifecycle plumbing, the protocol envelope — out of TypeScript and into the `jazz-wasm` Rust crate. JavaScript keeps the bare minimum: constructing the `Worker` object (URL resolution is a bundler concern) and a tiny WASM-bootstrap shim on the worker side.

## Why This Exists

`jazz-wasm` is the only binding crate that exposes a `SyncSender` JS callback (`onSyncMessageToSend`). NAPI doesn't have one — Rust's `TransportManager` owns the wire on that path. The only reason `JsSyncSender` exists is to let the _worker bridge_ (`worker-bridge.ts`) and the _worker host_ (`jazz-worker.ts`) intercept `"client"`-destined outbox messages and re-route them via `postMessage`.

That JS callback layer is symptomatic of a deeper split: the worker bridge protocol is half in TypeScript and half in Rust.

- TS owns the message envelope (`worker-protocol.ts`), the init handshake state machine (`worker-bridge.ts`), peer mapping (`jazz-worker.ts`), buffering before init, lifecycle hint forwarding, and shutdown handshake.
- Rust owns the runtime semantics underneath (sync routing, persistence, transport).

The split costs us:

- **A WASM API (`onSyncMessageToSend`) that exists only to compensate for not owning the worker handle in Rust.** It pretends to be a generic outbox callback but is used by exactly two callers, both for the same job.
- **`unsafe impl Send for JsSyncSender`** in `runtime.rs` — needed because `RuntimeCore::sync_sender` is typed `Box<dyn SyncSender + Send>` for the multi-threaded Tokio backend, and a `Function`-holding struct isn't naturally `Send`. Sound under WASM's single-threaded runtime, but a smell that only goes away once the bound itself becomes cfg-gated (see "Cross-Stage Concerns → `Send` bound on `sync_sender`"). Without that one-line fix in `runtime_core`, _any_ replacement sender we write — including the Rust-owned `worker.postMessage` one — would just inherit the same `unsafe impl`.
- **Two protocol definitions kept in sync by hand.** `worker-protocol.ts` enumerates every `MainToWorkerMessage`/`WorkerToMainMessage` variant; the meaning of each variant is implemented in TS, but the _content_ is dictated by what the Rust runtime emits. Drift between the two is invisible at compile time.
- **Test surface fragmented across layers.** `worker-bridge.test.ts` and `jazz-worker.test.ts` exercise message-protocol edge cases that are conceptually internal to the bridge. They couldn't go away without ownership consolidation.

## Goals

- Delete `JsSyncSender` and the `onSyncMessageToSend` WASM API entirely.
- Move the worker bridge orchestration (init handshake, peer routing, lifecycle hints, auth/error listeners, shutdown) into Rust on both sides of the worker boundary.
- Reduce `worker-bridge.ts` to a thin TS adapter (or remove it; the call site in `db.ts` may interact with the Rust bridge directly).
- Reduce `jazz-worker.ts` to a ~30-line WASM-bootstrap shim that hands control to a Rust entry point.
- Keep `worker-protocol.ts` only if external TS consumers need the types; otherwise delete it.
- Preserve the public `Db` API and the on-the-wire structured-clone protocol shape (Uint8Array transferables, message types) — this is internal plumbing, not a behavior change.

## Non-Goals

- No changes to NAPI or React Native bindings.
- No semantic change to `RuntimeCore` or any `jazz-tools` crate. **One targeted, mechanical edit is in scope:** dropping the `+ Send` bound on `RuntimeCore::sync_sender` for `cfg(target_arch = "wasm32")` builds (see "Cross-Stage Concerns → `Send` bound on `sync_sender`"). Without this, the new Rust outbox sender just inherits `JsSyncSender`'s `unsafe impl Send` smell and the cleanup goal isn't met.
- No change to the upstream WebSocket transport (`runtime.connect`) — it's already Rust-owned.
- No change to leader election (`tab-leader-election.ts`) or the BroadcastChannel sync protocol used between tabs. Those are coordinator concerns one level up from the worker bridge.
- No new on-the-wire protocol for the worker `postMessage` channel. Message payloads (Uint8Array sync bytes) and their semantics are unchanged. Only the _encoding_ of the envelope (the JS object holding `{ type, ... }`) becomes Rust-driven.
- No attempt to support a worker whose entry point is not produced by this repo (i.e. third-party hosts of the WASM runtime).

## End-State Architecture

```text
Main thread (JS):
  const worker = new Worker(workerUrl, { type: "module" });
  // ^ bundler URL resolution stays in JS.
  const bridge = WasmWorkerBridge.attach(worker, runtime, options);
  await bridge.init();
  // bridge.update_auth(...), bridge.shutdown(), bridge.disconnect_upstream(),
  // bridge.set_listeners({ onAuthFailure, onMutationErrorReplay, ... })

Main thread (Rust, jazz-wasm):
  WasmWorkerBridge {
    worker: web_sys::Worker,
    runtime: Rc<RefCell<WasmCoreType>>,
    on_message_closure: Closure<dyn FnMut(MessageEvent)>,
    state: WorkerBridgeState,
    listeners: ListenerSlots,
  }
  - Sets worker.onmessage to a Rust closure that decodes the message and
    dispatches to the right runtime call or listener slot.
  - Outbox: WasmRuntime's SyncSender posts directly via worker.post_message_with_transfer.
  - Init: encodes InitMessage, posts, awaits init-ok via a oneshot promise.
  - Peer/auth/lifecycle/shutdown: handled inside Rust; exposes typed callback registration.

Worker (JS shim — ~50 lines, dictated by the bootstrap-handoff dance):
  import * as wasmModule from "jazz-wasm";
  let initMessage = null;
  const pendingMessages = [];
  self.onmessage = (event) => {
    if (!initMessage && event.data?.type === "init") {
      initMessage = event.data;
      bootstrapAndHandoff(initMessage);
    } else {
      pendingMessages.push(event.data);
    }
  };
  self.postMessage({ type: "ready" });
  async function bootstrapAndHandoff(init) {
    await initWasm(wasmModule, init.runtimeSources, init.fallbackWasmUrl);
    installWasmTelemetry({ ... });
    // Hand init + everything we buffered to Rust. Rust synchronously installs
    // its own self.onmessage as part of this call, so any messages arriving
    // *during* the call still hit our handler and land in pendingMessages.
    wasmModule.runAsWorker(init, pendingMessages);
  }
  // After runAsWorker returns, Rust owns self.onmessage / self.postMessage
  // entirely. The persistent runtime itself is opened lazily by the host
  // during runAsWorker, after the closure is installed.

Worker (Rust, jazz-wasm):
  WorkerHost {
    runtime: Option<Rc<RefCell<WasmCoreType>>>, // None until init lands
    main_client_id: Option<ClientId>,
    peer_clients: HashMap<String, ClientId>,
    pending_sync_messages: VecDeque<...>,
    on_message_closure: Closure<dyn FnMut(MessageEvent)>,
  }
  - The free function runAsWorker(initMessage, pendingMessages) constructs a
    WorkerHost (with the JS-buffered messages already loaded into its queue),
    stores it in a thread_local, and registers a Rust closure as self.onmessage.
  - It then opens the persistent (or ephemeral) runtime asynchronously using
    schema/db/auth fields from initMessage, registers the main-thread peer
    client, installs the outbox sender, drains pending sync, and posts init-ok.
  - Subsequent messages dispatch through the host: "sync", "peer-*",
    "lifecycle-hint", "update-auth", "disconnect-upstream"/"reconnect-upstream",
    "shutdown", "acknowledge-rejected-batch", "simulate-crash", debug messages.
  - Outbox: SyncSender posts directly via self.post_message_with_transfer.
  - Server sync stays the way it is today via runtime.connect() and the Rust transport.
```

## Public API Surface

The new Rust types exported from `jazz-wasm`:

```rust
#[wasm_bindgen]
pub struct WasmWorkerBridge { /* main-thread side */ }

#[wasm_bindgen]
impl WasmWorkerBridge {
    /// Attach a Rust bridge to an externally-constructed Worker.
    /// `options` is a JS object matching the existing WorkerBridgeOptions shape.
    #[wasm_bindgen(js_name = attach)]
    pub fn attach(worker: web_sys::Worker, runtime: &WasmRuntime, options: JsValue)
        -> Result<WasmWorkerBridge, JsError>;

    /// Send the init message and resolve when the worker reports init-ok.
    #[wasm_bindgen]
    pub async fn init(&self) -> Result<JsValue /* { clientId } */, JsError>;

    /// Push a fresh JWT into the worker.
    #[wasm_bindgen(js_name = updateAuth)]
    pub fn update_auth(&self, jwt_token: Option<String>);

    /// Forward a page lifecycle event to the worker.
    #[wasm_bindgen(js_name = sendLifecycleHint)]
    pub fn send_lifecycle_hint(&self, event: &str);

    /// Open / sync / close a follower-tab peer mapping in the worker runtime.
    #[wasm_bindgen(js_name = openPeer)]
    pub fn open_peer(&self, peer_id: &str);
    #[wasm_bindgen(js_name = sendPeerSync)]
    pub fn send_peer_sync(&self, peer_id: &str, term: u32, payload: js_sys::Array);
    // ^ `Vec<Uint8Array>` doesn't bind through wasm-bindgen for non-primitive
    //   element types — accept a JS array and unpack each entry as `Uint8Array`.
    #[wasm_bindgen(js_name = closePeer)]
    pub fn close_peer(&self, peer_id: &str);

    /// Flow-control hooks for the multi-tab leader/follower split.
    #[wasm_bindgen(js_name = setServerPayloadForwarder)]
    pub fn set_server_payload_forwarder(&self, callback: Option<Function>);
    #[wasm_bindgen(js_name = applyIncomingServerPayload)]
    pub fn apply_incoming_server_payload(&self, payload: Uint8Array);
    #[wasm_bindgen(js_name = waitForUpstreamServerConnection)]
    pub async fn wait_for_upstream_server_connection(&self) -> Result<(), JsError>;
    #[wasm_bindgen(js_name = replayServerConnection)]
    pub fn replay_server_connection(&self);
    #[wasm_bindgen(js_name = disconnectUpstream)]
    pub fn disconnect_upstream(&self);
    #[wasm_bindgen(js_name = reconnectUpstream)]
    pub fn reconnect_upstream(&self);
    #[wasm_bindgen(js_name = acknowledgeRejectedBatch)]
    pub fn acknowledge_rejected_batch(&self, batch_id: &str);

    /// Bulk listener registration. Replaces the previously registered set in
    /// one call. Designed to match `db.ts`'s actual usage pattern (all four
    /// listeners attached together, once, immediately after `attach`). Each
    /// field is optional; passing `undefined` clears the slot.
    #[wasm_bindgen(js_name = setListeners)]
    pub fn set_listeners(&self, listeners: JsValue /* { onPeerSync?, onAuthFailure?,
        onLocalBatchRecordsSync?, onMutationErrorReplay? } */);

    /// Shut down the worker and resolve when the worker confirms OPFS release.
    #[wasm_bindgen]
    pub async fn shutdown(&self) -> Result<(), JsError>;
}

/// Worker-side entry point. Free function exported from the WASM module.
///
/// Called by the JS shim *after* WASM is initialized and the first init
/// message has been read. Takes:
///
///   - `init_message`: the already-received `"init"` message (a JS object).
///   - `pending_messages`: a `js_sys::Array` of messages that arrived between
///     the JS-side init read and this call. Drained into the host's queue.
///
/// Synchronously installs a Rust closure as `self.onmessage` (replacing the
/// JS-shim handler atomically) and starts async runtime open via
/// `wasm_bindgen_futures::spawn_local`. After this returns, Rust owns
/// `self.onmessage` / `self.postMessage` entirely.
///
/// Async-init contract:
///   - The runtime is opened asynchronously by the spawned future. While that
///     runs, the host is in the `Initializing` state. Incoming "sync" /
///     "peer-sync" / "lifecycle-hint" / etc. messages buffer into the host's
///     pending queues and drain after the runtime is ready, mirroring today's
///     `initComplete` gating in `jazz-worker.ts`.
///   - Failure during async open posts `WorkerToMainMessage::Error { message }`
///     back to main. The promise from `runAsWorker` itself is *not* the right
///     surface for these errors: by the time it would reject, the JS shim has
///     long returned. All open errors are protocol errors.
///   - Idempotency: if a second `Init` message arrives over the wire while the
///     host is `Initializing` or already `Ready`, the host posts an `Error`
///     back and ignores the second init. The TS state machine treats double-
///     init as a programming error today; the Rust port preserves that.
///   - Re-entry: if `run_as_worker` itself is called twice from JS (the JS shim
///     misuses the API), the second call is a no-op once the `thread_local`
///     host slot is occupied.
#[wasm_bindgen(js_name = runAsWorker)]
pub fn run_as_worker(
    init_message: JsValue,
    pending_messages: js_sys::Array,
) -> Result<(), JsError>;
```

The deletions on the WASM API:

- `WasmRuntime::on_sync_message_to_send` — gone.
- `WasmRuntime::on_sync_message_received_from_client` — kept as a public API (still used by direct-mode tests), but the worker host no longer routes through it from JS; the Rust host calls it internally on the runtime borrow.

The deletions on the TS surface:

- `packages/jazz-tools/src/runtime/worker-bridge.ts` — gone, replaced by direct calls on `WasmWorkerBridge`. The `WorkerBridge` _class_ is replaced by direct usage in `db.ts`. If preserving an interface helps the migration, it can survive temporarily as a thin adapter and be removed in a follow-up.
- `packages/jazz-tools/src/worker/jazz-worker.ts` — replaced by a ~30-line WASM-bootstrap shim.
- `packages/jazz-tools/src/worker/worker-protocol.ts` — removed, since neither end of the protocol is constructed in TS.
- `packages/jazz-tools/src/runtime/sync-transport.ts::createSyncOutboxRouter` — gone, since there's no longer a JS-side outbox callback to normalize.

## Protocol Encoding

Messages between main and worker remain JS objects (preserves structured-clone with `Uint8Array` transferables). On the Rust side, define them as serde enums and convert to/from `JsValue` via `serde-wasm-bindgen`:

```rust
#[derive(Serialize, Deserialize)]
#[serde(
    tag = "type",
    rename_all = "kebab-case",       // variant tag: "peer-open", "update-auth", ...
    rename_all_fields = "camelCase", // inner fields: peerId, jwtToken, sentAtMs, ...
)]
enum MainToWorkerMessage {
    Init(InitPayload),
    Sync { payload: Vec<Bytes> },
    PeerOpen { peer_id: String },
    PeerSync { peer_id: String, term: u32, payload: Vec<Bytes> },
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

#[derive(Serialize, Deserialize)]
#[serde(
    tag = "type",
    rename_all = "kebab-case",
    rename_all_fields = "camelCase",
)]
enum WorkerToMainMessage {
    Ready,
    InitOk { client_id: String },
    UpstreamConnected,
    UpstreamDisconnected,
    Sync { payload: Vec<SyncEnvelopeFromWorker> }, // see "Direction asymmetry" below
    PeerSync { peer_id: String, term: u32, payload: Vec<Bytes> },
    MutationErrorReplay { batch: LocalBatchRecord },
    LocalBatchRecordsSync { batches: Vec<LocalBatchRecord> },
    Error { message: String },
    AuthFailed { reason: String },
    ShutdownOk,
    DebugSchemaStateOk { state: DebugSchemaStatePayload },
    DebugSeedLiveSchemaOk,
}
```

The discriminator (`type` field) stays kebab-case because the existing TS protocol uses `"peer-open"`, `"update-auth"`, `"upstream-connected"`, etc. Inner fields stay camelCase to match `peerId`, `jwtToken`, `sentAtMs`, `clientId`, `appId`, `userBranch`, `dbName`, `serverUrl`, `adminSecret`, `runtimeSources`, `fallbackWasmUrl`, `logLevel`, `telemetryCollectorUrl`, `schemaJson`, `batchId`, `batches`, `state`. Inside `InitPayload` and `LocalBatchRecord`, struct-level `#[serde(rename_all = "camelCase")]` accomplishes the same thing locally — `rename_all_fields` only reaches one level deep.

**Direction asymmetry — the two `Sync` variants don't carry the same shape.** Main → worker `Sync` is always client-bound binary postcard (the worker bridge only forwards `"client"`-destined messages, sequencing happens in the worker host), so `MainToWorkerMessage::Sync { payload: Vec<Bytes> }` is correct as written.

Worker → main `Sync` is heterogeneous. The current TS interface (`SyncToMainMessage.payload`) is `(Uint8Array | string | SequencedSyncPayload)[]`: bare bytes for unsequenced binary, bare strings for unsequenced JSON (server-bound traffic forwarded out of bootstrap-catalogue mode), and a `{payload: Uint8Array | string, sequence: number}` envelope for sequenced client-bound messages. The Rust shape:

```rust
#[derive(Serialize, Deserialize)]
#[serde(untagged)]
enum SyncEnvelopeFromWorker {
    Sequenced(SequencedSyncPayload),
    Bare(BarePayload),
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SequencedSyncPayload {
    payload: BarePayload,
    sequence: u64,
}

#[derive(Serialize, Deserialize)]
#[serde(untagged)]
enum BarePayload {
    Binary(Bytes),
    Json(String),
}
```

`#[serde(untagged)]` is the right choice for both — the on-the-wire object either has a `sequence` field (then it's `Sequenced`) or it's a raw `Uint8Array` / `string` (then it's `Bare`). `BarePayload` resolves at runtime by checking the JS value's type, which `serde-wasm-bindgen` handles cleanly for `Uint8Array` vs `string`.

Locking this ahead of the encoder spike: the spike validates that `Vec<SyncEnvelopeFromWorker>` round-trips through `serde-wasm-bindgen` with the byte fast-path intact. If untagged-enum dispatch is the fiddly piece (and it might be — `serde-wasm-bindgen` has historically been sensitive about untagged enums with binary types), the fallback for _both_ `Sync` variants is hand-rolled encoders, not just the binary one.

`Bytes` here is a newtype that serializes directly to a JS-owned `Uint8Array`, sidestepping `serde-wasm-bindgen`'s default `Vec<u8>` → JS-array path (which is slower and allocation-heavy).

**Important: there is no zero-copy path from wasm linear memory.** A `Uint8Array` view that points into wasm memory cannot be `postMessage`-transferred — the underlying `ArrayBuffer` is the wasm module's `memory.buffer`, which can't be detached. So every outbox payload follows the same shape today's `JsSyncSender` uses: `Uint8Array::from(&bytes[..])` constructs a _fresh, JS-owned_ `ArrayBuffer` and copies the bytes from wasm memory into it. That JS-owned buffer is then safe to transfer.

Variants that carry `Uint8Array` payloads (`Sync`, `PeerSync`) need a custom serde shape that survives `serde-wasm-bindgen` and lets Rust collect transferables before posting. The introspective approach is:

1. Serialize the enum to a `JsValue` with placeholder `null`s for the byte payloads.
2. For each payload, construct a JS-owned `Uint8Array` (`Uint8Array::from(slice)`) and slot it into the right position on the resulting object.
3. Return the `JsValue` plus a `js_sys::Array` containing each `Uint8Array.buffer` to transfer.

The browser detaches the JS-owned buffers as part of `postMessage` with the transfer list. Wasm memory is never observed in the receiving agent. This matches the current `JsSyncSender` byte-flow exactly; the only thing that changes is _who_ calls `postMessage`.

**Prototype this before locking the design.** The walk-and-slot approach is nontrivial: `Sync` carries `Vec<Bytes>` (and `Vec<SyncEnvelopeFromWorker>` for the worker→main direction) at the top level, while `PeerSync` carries it nested under additional fields. If a 30-line spike in the crate shows that the post-walk is fiddly enough to be a maintenance hazard, fall back to **hand-rolled encoders for `Sync` and `PeerSync`** (both directions, so four variants total): keep serde for everything else (`PeerOpen`, lifecycle, debug, the smaller scalar-only variants), and hand-build `{ type, payload }` / `{ type, peerId, term, payload }` objects via `js_sys::Object` / `Reflect::set`. Less elegant on paper, avoids an introspective post-walk, and the API surface to the rest of the codebase is identical.

**`Init` is also a partial-serde variant.** `InitPayload` carries `runtime_sources: JsValue` (JS module references and binary blobs that bundlers resolve and that don't have a clean Rust type), plus `fallback_wasm_url: Option<String>`. The serde round-trip can't include `runtime_sources` directly. The encoding path for `Init`:

```rust
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct InitPayload {
    schema_json: String,
    app_id: String,
    env: String,
    user_branch: String,
    db_name: String,
    client_id: String,
    server_url: Option<String>,
    jwt_token: Option<String>,
    admin_secret: Option<String>,
    fallback_wasm_url: Option<String>,
    log_level: Option<String>,
    telemetry_collector_url: Option<String>,
    #[serde(skip)]
    runtime_sources: Option<JsValue>,
}
```

Encoding: serialize `InitPayload` (everything except `runtime_sources`) via serde, then `Reflect::set(&js_value, "runtimeSources", &runtime_sources)`. Decoding: deserialize via serde, then `Reflect::get(&raw_message, "runtimeSources")` and stash. The inverse is symmetric.

This means the rule "everything except `Sync` / `PeerSync` goes through serde end-to-end" doesn't actually hold — `Init` always uses partial-serde + manual slot. Worth owning that up front so the encoder spike scopes to the _right_ thing: not "does serde-wasm-bindgen handle this enum?" but "does serde-wasm-bindgen handle this enum cleanly enough that adding two more partial-serde variants is acceptable?"

The decision lands at the start of Stage 2 implementation, not in this spec. The Stage 2 task list begins with the encoder spike and the choice between the two paths.

`InitPayload` carries the same fields as today's `InitMessage`. Pass-through fields that are not Rust-typed (`runtime_sources` — JS module references, etc.) live as `JsValue` inside `InitPayload` and skip serde.

## Stage Breakdown

The work lands in three reviewable PRs.

### Stage 1 — Drop `JsSyncSender`, route outbox through a Rust-owned target

**Scope.** Replace `JsSyncSender` with a Rust outbox sender that holds a `JsValue` reference to the postMessage target (the `Worker` on the main side, `globalThis` / `DedicatedWorkerGlobalScope` on the worker side). Add a small transitional WASM API:

```rust
impl WasmRuntime {
    #[wasm_bindgen(js_name = attachOutboxTarget)]
    pub fn attach_outbox_target(
        &self,
        target: JsValue,                  // Worker (main) or self (worker)
        main_client_id: Option<String>,   // worker-side only: local main-thread client id
        peer_id_lookup: Option<Function>, // worker-side only: (clientId: string) => peerId | null
    );
}
```

The runtime now posts directly via `target.postMessage(payload, transfer)` from inside `batched_tick`'s outbox drain. All the encoding rules in `JsSyncSender::send_sync_message` move into the new sender unchanged: binary postcard for any `"client"` destination _or_ when `use_binary_encoding` is true; JSON otherwise; per-client sequence numbering keyed by destination id; `QuerySettled.through_seq` rewrite for sequenced client-bound messages. The `peer_id_lookup` callback is the one TS-side bookkeeping seam left for the worker side; Stage 2 absorbs it.

**`peer_id_lookup` error behavior.** The callback is invoked from the outbox sender's hot path, on every client-destined outbox message. Define behavior explicitly to avoid drift:

- Returns a `string` → use it as the `peerId` for the outbound `peer-sync` envelope.
- Returns `null` / `undefined` → drop the message silently. This matches today's TS code where `peerIdByRuntimeClient.get(...)` returning `undefined` causes the routing code to early-return.
- Throws → log a warning to the wasm tracing layer, drop the message, do _not_ propagate the panic. The outbox sender must not be reentrant-unsafe under JS faults.
- Returns any other type → treat as `null` (drop + log).

This contract lets Stage 2 delete the seam without a behavior change at any call site.

`attachOutboxTarget` is **transitional**. Stage 2 stops calling it from the worker side (the worker host installs the sender internally during `run_as_worker`). Stage 3 stops calling it from the main side (`WasmWorkerBridge.attach` installs the sender internally). The API gets deleted at the end of Stage 3.

**Affected files.**

- `crates/jazz-tools/src/runtime_core/mod.rs` — cfg-gate the `+ Send` bound on `sync_sender` for `target_arch = "wasm32"`. Two-line edit (field + setter).
- `crates/jazz-wasm/src/runtime.rs` — delete `JsSyncSender` and `on_sync_message_to_send`; add `RustOutboxSender` and `attachOutboxTarget`.
- `crates/jazz-wasm/Cargo.toml` — promote `web-sys` from dev-dep to runtime dep with `Worker`, `DedicatedWorkerGlobalScope`, `MessagePort`, `MessageEvent` features.
- `packages/jazz-tools/src/runtime/worker-bridge.ts` — replace the `runtime.onSyncMessageToSend(...)` registration with a single `runtime.attachOutboxTarget(this.worker)` call.
- `packages/jazz-tools/src/worker/jazz-worker.ts` — replace `runtime.onSyncMessageToSend(...)` with `runtime.attachOutboxTarget(self, mainClientId, peerLookup)`.
- `packages/jazz-tools/src/runtime/sync-transport.ts` — keep the helper for now; it's used by the Stage-1 transitional code path. Stage 2 deletes it.
- Tests: `worker-bridge.test.ts` mocks of the outbox callback get replaced by mocks of the Worker-target's `postMessage`. Otherwise unchanged.

**Migration check.** After Stage 1 lands, the TS bridge still drives init/peer/lifecycle/shutdown. Outbox messages no longer transit a JS function; they flow Rust → `worker.postMessage`. End-to-end behavior is unchanged.

### Stage 2 — Rust owns the worker side

**Scope.** Move every `self.onmessage` branch from `jazz-worker.ts` into Rust. Add the free function `run_as_worker(initMessage, pendingMessages)` (exported from the WASM module, not a method on `WasmRuntime`). The worker JS file shrinks to a ~50-line WASM-bootstrap shim that buffers the first init message, drives WASM init using its `runtimeSources`, and hands the buffered init plus any subsequent buffered messages to Rust.

What moves:

- The init message handler (`handleInit`) — opens persistent/ephemeral runtime, registers main-thread peer client, wires the auth-failure callback, drains pending sync, performs the upstream connect, and posts `init-ok`.
- Pending sync buffering before init.
- Peer client mapping (`peerRuntimeClientByPeerId`, `peerIdByRuntimeClient`, `peerTermByPeerId`).
- Bootstrap catalogue forwarding flag and the synthetic `addServer`/`removeServer` pump.
- `flushWalBestEffort`, `nudgeReconnectAfterResume`, lifecycle hint handling.
- `update-auth`, `disconnect-upstream`, `reconnect-upstream`.
- `shutdown` and `simulate-crash`.
- `mutation-error-replay`, `local-batch-records-sync` triggers.
- `acknowledge-rejected-batch`.
- The two debug messages (`debug-schema-state`, `debug-seed-live-schema`).

The runtime sources / WASM bootstrap stays in JS. The shim still resolves `runtimeSources.wasmModule | wasmSource | wasmUrl`, calls `wasmModule.default(...)`, then enters Rust.

The `peer_id_lookup` seam from Stage 1 is removed; Rust owns the table.

**Stage 2 starts with one task before any of the file changes below: a 30-line spike in `crates/jazz-wasm/` that round-trips a `Sync { payload: Vec<Bytes> }` and a `PeerSync { peer_id, term, payload }` through `serde-wasm-bindgen` to validate the introspective walk-and-slot encoder.** If the spike is clean, the rest of Stage 2 uses serde end-to-end. If the spike shows the post-walk to be fiddly, hand-roll the encoder for those two variants and keep serde for the rest. Decide before merging the protocol module.

**Affected files.**

- `crates/jazz-wasm/src/worker_protocol.rs` — new: serde enums (with the encoder strategy chosen by the spike above).
- `crates/jazz-wasm/src/worker_host.rs` — new: free function `run_as_worker`, `WorkerHost` struct, message dispatch, peer table, init state machine, lazy runtime construction.
- `crates/jazz-wasm/src/runtime.rs` — re-export `run_as_worker`. Helpers for binary-vs-JSON encoding shared with main side.
- `packages/jazz-tools/src/worker/jazz-worker.ts` — reduced to ~30 lines.
- `packages/jazz-tools/src/runtime/worker-bridge.ts` — unchanged from Stage 1 in this stage; still owns the main-side handshake. Outbox calls keep working since Stage 1 plumbed the Rust outbox.
- Tests: `jazz-worker.test.ts` mostly deleted (its surface is now Rust-internal). Replace with `wasm-bindgen-test` cases in the crate that verify run_as_worker + a mock-Worker harness on the bridge side. `db.worker-bootstrap.test.ts` (Vitest browser) stays as the high-level smoke test.

### Stage 3 — Rust owns the main side

**Scope.** Replace `worker-bridge.ts` with `WasmWorkerBridge` (Rust). `db.ts` constructs the bridge by calling into Rust:

```ts
const worker = await Db.spawnWorker(this.config.runtimeSources);
const bridge = WasmWorkerBridge.attach(worker, mainRuntime, options);
bridge.setListeners({
  onPeerSync: (batch) => ...,
  onAuthFailure: (reason) => ...,
  onLocalBatchRecordsSync: (batches) => ...,
  onMutationErrorReplay: (batch) => ...,
});
await bridge.init();
```

What moves into Rust on this side:

- The `WorkerBridge` state machine (`idle | initializing | ready | failed | shutting-down | disposed`).
- The `worker.onmessage` handler that decodes `WorkerToMainMessage`.
- Init message construction, posting, and the `init-ok | error` await.
- Pending sync buffering before init-ok.
- Upstream connection signaling (`expectsUpstreamServer`, `upstreamServerConnected`, `waitForUpstreamServerConnection`).
- Peer routing surface (`openPeer`, `sendPeerSync`, `closePeer`).
- Lifecycle hint, auth update, disconnect/reconnect, shutdown handshake.
- A single `setListeners({...})` setter that holds the four listener slots together, replacing the four-method `on*` form to keep the wasm-bindgen surface tight. `db.ts` registers all four together once, so the bulk form maps 1:1 to caller usage.

The `runtime.addServer(null, 1)` call in the current `WorkerBridge` constructor moves into the Rust attach logic. Symmetrically, the `removeServer()` that the TS bridge does on `SHUTDOWN_CALLED` (and again on the runtime's outbox-callback teardown) must be replicated on the Rust shutdown path — see "Shutdown / Drop semantics" below.

**Affected files.**

- `crates/jazz-wasm/src/worker_bridge.rs` — new: main-side struct.
- `crates/jazz-wasm/src/runtime.rs` — exports.
- `packages/jazz-tools/src/runtime/db.ts` — replaces `new WorkerBridge(worker, runtime)` with `WasmWorkerBridge.attach(...)`.
- `packages/jazz-tools/src/runtime/worker-bridge.ts` — deleted (or stub re-export of types for one release if external code imports it; unlikely here).
- `packages/jazz-tools/src/worker/worker-protocol.ts` — deleted; the only TS consumer of these types is `worker-bridge.ts`, which is also gone. The `LocalBatchRecord` type continues to be exported from `runtime/client.ts` as it is today.
- Tests: `worker-bridge.test.ts` and `worker-bridge.race-harness.test.ts` deleted in favor of `wasm-bindgen-test` Rust cases. `db.worker-bootstrap.test.ts`, `client.mutations.test.ts`, and other higher-level tests stay.

## Cross-Stage Concerns

### `web_sys` dependency

`web-sys` features needed:

- `Worker`, `DedicatedWorkerGlobalScope`, `MessageEvent`, `WorkerOptions` (already used).
- `MessagePort` (for completeness; not strictly needed).
- `Window` (for the worker side's runtime sources lookup if we choose to do `globalThis.location` reads in Rust).

Promote from `[target.'cfg(target_arch = "wasm32")'.dev-dependencies]` to a regular target-conditional dep.

### `Send` bound on `sync_sender`

`RuntimeCore::sync_sender` is currently typed `Box<dyn SyncSender + Send>`. The `+ Send` bound exists for the multi-threaded Tokio backend (NAPI/server). It's the reason the current `JsSyncSender` carries `unsafe impl Send` — `web_sys::Worker`, `Function`, `JsValue`, `Rc`, and `RefCell` are not naturally `Send`. The new `RustOutboxSender` (Stage 1) and the `WorkerPostMessageSender` family that grow out of it have the same shape: they hold a `JsValue` reference to a `Worker` or `DedicatedWorkerGlobalScope` plus per-client sequence state in an `Rc<RefCell<HashMap<…>>>`. A simple type swap leaves the smell where it is, just renamed.

The targeted fix is to drop the `+ Send` bound on wasm32 builds:

```rust
// crates/jazz-tools/src/runtime_core/mod.rs

#[cfg(target_arch = "wasm32")]
pub(crate) sync_sender: Option<Box<dyn SyncSender>>,
#[cfg(not(target_arch = "wasm32"))]
pub(crate) sync_sender: Option<Box<dyn SyncSender + Send>>,
```

…and the matching `set_sync_sender` signature. Two cfg-gated lines per occurrence; no semantic change. The trait `SyncSender` itself stays unchanged (no `Send` super-trait there). On wasm32, the runtime is single-threaded by construction, so no actual safety is being relaxed — we're just making the type system express that.

This edit lands in **Stage 1**, alongside `JsSyncSender` deletion. Without it, the new sender either ships with its own `unsafe impl Send` (no improvement over today) or has to fake `Send` via a thread-local registry keyed by a `Send`-able token (added complexity for the same end result). Neither is a substitute for the cfg edit.

### Closure lifetime

Rust closures registered with `worker.set_onmessage(...)` and `globalThis.set_onmessage(...)` must outlive every potential dispatch. Store the `Closure<dyn FnMut(MessageEvent)>` on the bridge / host struct; drop it (and clear the `onmessage`) on `shutdown` / `Drop`.

### `WorkerHost` ownership

`run_as_worker` returns to JS — the host can't live in the call stack. WASM is single-threaded, so the host lives in a `thread_local`:

```rust
thread_local! {
    static HOST: RefCell<Option<WorkerHost>> = const { RefCell::new(None) };
}
```

`run_as_worker` populates the slot, the message-handler closure borrows from it on each dispatch, and `Shutdown` (or `SimulateCrash`) replaces the slot's contents with `None`, triggering `WorkerHost::Drop`. Drop tears down the closures, posts `ShutdownOk`, and calls `self.close()`.

`Box::leak` is not viable — it forecloses shutdown / OPFS handle release. `OnceCell` is one-shot only and doesn't support drop. `thread_local!` is the only option that gives us both ownership and lifecycle control on wasm.

### Shutdown / Drop semantics

Both the main-side bridge and the worker-side host need explicit symmetric cleanup. The current TS `WorkerBridge` does this in two phases: on `SHUTDOWN_CALLED` it calls `runtime.removeServer()`, and on `SHUTDOWN_FINISHED` (`disposeInternals`) it clears state and detaches the outbox callback via `runtime.onSyncMessageToSend?(() => undefined)`. The Rust port must mirror that lifecycle or the main runtime keeps emitting outbox traffic to a dead worker after the bridge tears down (and on follower-tab promotion).

**`WasmWorkerBridge` shutdown checklist (main side, Stage 3):**

1. Transition state to `shutting-down`. Reject new outgoing calls (`update_auth`, `send_lifecycle_hint`, `send_peer_sync`, etc.) idempotently.
2. Call `core.remove_server(server_id)` for the server edge installed by `attach`. This stops the runtime from queuing further client-bound outbox traffic for the bridge.
3. Detach the outbox sender by installing a `NoopSyncSender` (a 5-line type local to `jazz-wasm`: empty `send_sync_message`, trivial `as_any`). This _replaces_ the active sender via the existing `core.set_sync_sender` API — no `clear_sync_sender` helper is needed, so `runtime_core` stays untouched beyond the cfg-gated `Send` edit. Honoring the Non-Goals.
4. Send the `Shutdown` message; await `ShutdownOk` or the timeout.
5. On either resolution: clear listener slots, drop the `Closure` storing `worker.onmessage` (which also clears the JS-side handler — `Closure::drop` invalidates the function ref), drop the bridge.

**`WorkerHost` shutdown checklist (worker side, Stage 2):**

1. On `Shutdown`/`SimulateCrash`: call `runtime.flush_wal()` (only on `SimulateCrash`) and `runtime.free()` (Drop releases OPFS handles).
2. Clear `peer_clients`, `pending_sync_messages`, `main_client_id`.
3. Drop the worker-side `Closure` for `self.onmessage`.
4. Post `ShutdownOk` and call `self.close()`.

**`Drop` impls.** Both `WasmWorkerBridge` and `WorkerHost` get explicit `Drop` impls that perform best-effort cleanup if the user lets the wrapper drop without calling `shutdown()` (e.g. on a thrown exception during init).

- `WasmWorkerBridge::Drop`: detach the outbox sender (install `NoopSyncSender`), call `core.remove_server(server_id)`, drop the message-handler `Closure`. Do _not_ post `Shutdown` to the worker — the bridge owner gave up and main isn't waiting on `ShutdownOk` anymore.
- `WorkerHost::Drop`: drop closures, `runtime.free()` if the runtime was opened. Do _not_ post `ShutdownOk` from `Drop` — by the time `Drop` runs in an exception path, the receiver may be gone, and posting from a destructor risks structured-clone errors. Main times out on its `SHUTDOWN_ACK_TIMEOUT_MS = 5_000` and treats the worker as gone, which is the correct outcome for any unclean exit.

### Init handshake awaiting

The init result needs to be awaitable from JS as a `Promise`. Implement it as a `Rc<RefCell<Option<futures::channel::oneshot::Sender<...>>>>` plus `oneshot::Receiver`, exposed via `wasm_bindgen_futures::future_to_promise`. The `init-ok | error` branch in the `onmessage` dispatch fulfills the sender. Timeout is enforced inside the Rust async block (a hand-rolled `setTimeout` future is fine; the existing `wasm-bindgen-futures` machinery covers it without an extra dep).

`futures = "0.3"` is already a runtime dep on `jazz-wasm`, so this lands free. `tokio::sync::oneshot` would drag tokio into the wasm target — don't use it.

Preserve the timeouts the TS bridge uses today: `INIT_RESPONSE_TIMEOUT_MS = 12_000` and `SHUTDOWN_ACK_TIMEOUT_MS = 5_000`. They live as Rust constants in `worker_bridge.rs`.

### Transferables

`worker.post_message_with_transfer(&value, &transfer_array)` accepts a `js_sys::Array` of transferables. Build it once per outbox flush from the underlying `ArrayBuffer`s of the `Uint8Array`s.

### Telemetry

`installWasmTelemetry` (in `runtime/sync-telemetry.ts`) is invoked from `jazz-worker.ts` after WASM init. Keep it that way — it's a JS-side concern (it imports `wasmModule`'s `subscribeTraceEntries`). The Rust worker host doesn't need to know about it.

### Lifecycle event listeners

`document.visibilitychange` / `window.pagehide` / `freeze` / `resume` listeners live in `db.ts` today. Keep them there; they call `bridge.send_lifecycle_hint(event)` on the new Rust API. Moving the listener attachment itself into Rust would require reaching into `web_sys::window()`/`document()`, which adds complexity for no architectural gain — these are user-visible browser events, not part of the worker protocol.

### Multi-tab / leader-follower

The leader/follower routing logic (BroadcastChannel, leader election, server payload forwarding between tabs) stays in `db.ts`. The bridge exposes only the seams it owns:

- `set_server_payload_forwarder(callback)` — when a follower-tab bridge has another tab forwarding upstream traffic to it.
- `apply_incoming_server_payload(bytes)` — when the follower-tab receives an upstream payload from the leader tab and needs to feed its main-thread runtime.
- `wait_for_upstream_server_connection()` — gated against either upstream-connected from the worker or the forwarder being installed.
- `replay_server_connection()` — the runtime-side `removeServer` / `addServer` reconnect cycle.

These are all already present on the TS bridge; they map 1:1 to Rust methods.

### Tests

- High-level browser tests (`db.worker-bootstrap.test.ts`, `db.transport.test.ts`, etc.) stay and act as the regression net for end-to-end behavior across stages.
- Internal protocol tests (`worker-bridge.test.ts`, `worker-bridge.race-harness.test.ts`, `jazz-worker.test.ts`) get replaced — but the test-harness shape itself is a Stage 2 unknown (see "Test harness" below).
- `client.mutations.test.ts`, `client.build-output.test.ts`, `db.worker-bootstrap.test.ts`, `worker-bridge.race-harness.test.ts` reference symbols that may move; sweep them per-stage.

### Test harness

The current TS unit tests mock `Worker` as a plain object with `postMessage` / `onmessage` slots. Replicating that ergonomically from `#[wasm_bindgen_test]` is harder than it looks — there are two paths and neither is free:

1. **Synthetic Worker shim from Rust.** Build a JS object via `js_sys::Object` / `Reflect::set` that exposes `postMessage` / `onmessage` properties, and downcast it to `web_sys::Worker` via `JsCast::unchecked_into`. Possible, but verbose, and the cast is technically dicey — `web_sys::Worker` carries no marker the cast can verify, so we'd be relying on duck-typing inside our own crate. Acceptable in tests if we own the cast site, less acceptable if it leaks into the bridge's internal type expectations.
2. **Real Worker spawned from a built artifact.** Hits bundler-and-test-harness complexity inside `wasm-bindgen-test` — workers need a separate JS entry, and `wasm-bindgen-test` doesn't have first-class support for spinning one up.

**Decision deferred to a Stage-2 spike** (alongside the encoder spike). Build a 50-line synthetic-Worker harness in Rust and confirm it can drive `WasmWorkerBridge` cleanly. If the cast-into-Worker hack proves too unstable, the fallback is **keep some scenario tests in Vitest browser mode**, written against the new Rust API as JS callers see it. Specifically, the scenarios most likely to want Vitest:

- Init race / late `init-ok` arrivals.
- Follower-tab forwarder swap.
- Peer term changes during sync flushes.

The pure protocol tests (encoder round-trips, message dispatch) can stay in `#[wasm_bindgen_test]` regardless. Don't force every test into Rust if the harness fights us.

## Risks and Open Questions

- **Error propagation through `Closure::wrap`.** The closure's body returns a `Result` and converts any `Err(...)` into a `WorkerToMainMessage::Error { message }` posted back to main — that's what the existing TS `try/catch` in `handleInit` does (it catches JS exceptions thrown by `runtime.openPersistent`, etc., not Rust panics). `catch_unwind` is _not_ a useful tool here: `console_error_panic_hook` is enabled by default in `jazz-wasm` (`Cargo.toml` `default = ["console_error_panic_hook"]`), and wasm panics abort with a stack trace rather than unwinding. Don't try to recover from panics at the closure boundary; surface protocol-level errors via the `Error` message variant and let `console_error_panic_hook` handle the rest as today.
- **`db.ts` is already large (~90KB).** Stage 3 reduces JS surface area but adds wiring between `db.ts` and the Rust bridge. Look for opportunities to flatten — e.g., move `applyBridgeRoutingForCurrentLeader` adjacent logic into the bridge if it falls naturally, but don't pad the spec scope to chase it.
- **`onSyncMessageReceivedFromClient` keeps its public WASM API.** It's still used by the worker host (now from inside Rust) and by direct-mode tests. It's not part of the bridge protocol; keep it.
- **`runtime.connect()` from inside the worker host.** The worker host currently calls into Rust transport via the WASM API. After Stage 2 the worker host _is_ Rust, so it should call `jazz_tools::runtime_core::install_transport` directly rather than going through the JS-facing wrapper. Verified: that function is already `pub` at the crate root (`runtime_core/mod.rs:542`) and is exactly the call the current `WasmRuntime::connect` makes. Small simplification, not a behavior change.
- **Stage 1's `peer_id_lookup` JS callback is a load-bearing seam if Stage 2 slips.** The worker side asks JS "what peer is this clientId mapped to?" purely because the peer table is still TS-owned at Stage 1. Stage 2 deletes the seam by absorbing the table into Rust. If Stage 2 is delayed by more than one release cycle, this callback becomes a permanent feature of the WASM API by accident — schedule Stage 2 directly behind Stage 1 rather than treating it as a follow-up.
- **`WasmWorkerBridge` API width (16 methods after the listener collapse).** One further collapse opportunity worth considering during Stage 3 implementation, but not pre-committing here: `disconnectUpstream` / `reconnectUpstream` / `replayServerConnection` could merge into a single `setUpstreamConnectionState(state)` if the call sites in `db.ts` map cleanly to a state model. They currently feel more like commands than state transitions, so collapsing may be cosmetic at best. Decide during Stage 3 review.
- **`runtimeSources.wasmModule` traveling via `postMessage`.** Structured-clone preserves module identity within an agent for genuine ES module references, but bundlers that shim modules at build time (some Vite/Webpack/Turbopack/Svelte configurations) reify the "module" as a plain object whose identity isn't structurally meaningful. The current TS code already lives with this risk via the same field; the Rust port inherits it rather than introducing it. **Smoke-test bar at end of Stage 2:** at least one app from `dev/stress-tests/` _and_ one from `examples/` per supported bundler boots to `init-ok` in worker mode. Bundlers covered today: Vite, Next/Turbopack, Webpack, SvelteKit. A failure on any of them blocks Stage 2 merge.

## Key Files

| File                                                                              | Change                                                                                                                                               |
| --------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------- |
| `crates/jazz-tools/src/runtime_core/mod.rs`                                       | Stage 1: cfg-gate the `+ Send` bound on `sync_sender` (field + setter) for `target_arch = "wasm32"`.                                                 |
| `crates/jazz-wasm/src/runtime.rs`                                                 | Stage 1 deletes `JsSyncSender`, adds `attachOutboxTarget`. Stage 2 re-exports `runAsWorker`. Stage 3 exports `WasmWorkerBridge`.                     |
| `crates/jazz-wasm/src/worker_protocol.rs` _(new)_                                 | Stage 2: serde enums for both directions.                                                                                                            |
| `crates/jazz-wasm/src/worker_host.rs` _(new)_                                     | Stage 2: free function `run_as_worker`, `WorkerHost` struct, message dispatch, peer table, lazy runtime construction, host shutdown semantics.       |
| `crates/jazz-wasm/src/worker_bridge.rs` _(new)_                                   | Stage 3: `WasmWorkerBridge` main-side struct, init/shutdown handshake, listener slots, server-edge install/teardown, outbox detach on shutdown/Drop. |
| `crates/jazz-wasm/Cargo.toml`                                                     | Stage 1: promote `web-sys` to runtime dep; add features.                                                                                             |
| `packages/jazz-tools/src/runtime/worker-bridge.ts`                                | Stage 1: minor edits. Stage 3: deleted.                                                                                                              |
| `packages/jazz-tools/src/worker/jazz-worker.ts`                                   | Stage 1: minor edits. Stage 2: reduced to bootstrap shim.                                                                                            |
| `packages/jazz-tools/src/worker/worker-protocol.ts`                               | Stage 3: deleted.                                                                                                                                    |
| `packages/jazz-tools/src/runtime/sync-transport.ts`                               | Stage 1: keep. Stage 3: `createSyncOutboxRouter` deleted; HTTP helpers (if still referenced) stay.                                                   |
| `packages/jazz-tools/src/runtime/db.ts`                                           | Stage 3: `WorkerBridge` replaced by `WasmWorkerBridge`.                                                                                              |
| `packages/jazz-tools/src/runtime/db-runtime-module.ts` / `wasm-runtime-module.ts` | Stage 3: surface the new exports for consumers.                                                                                                      |
| `specs/status-quo/browser_adapters.md`                                            | Update once Stage 3 lands; the "What the WorkerBridge Owns" / "Key Files" sections change.                                                           |
