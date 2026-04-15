# Transport Control Channel and Worker Wiring — TODO (MVP)

Make the JS worker ↔ Rust transport bridge correct by introducing a single control primitive (`TransportControl`) that carries shutdown and auth-refresh signals through every phase of `TransportManager::run`, and by tightening the worker's URL and credential plumbing so updates from the main thread actually reach the Rust transport.

This spec folds three captured issues into one change:

- `todo/issues/update-auth-noop-breaks-jwt-refresh.md`
- `todo/issues/transport-manager-reconnect-loop-leak.md`
- `todo/issues/worker-server-url-not-converted-to-ws.md`

## Goals

- `TransportManager::run` exits promptly when the owner disconnects, from **any** lifecycle phase (connect, handshake, backoff, connected). No more indefinite reconnect loops across shutdown cycles.
- JWT refresh from `Db.applyAuthUpdate()` reaches the Rust transport without requiring worker restart. The worker-side Rust transport picks up the new credentials and continues syncing.
- Worker-initiated connections dial the correct WebSocket endpoint: HTTP/HTTPS URLs are normalized to WS/WSS, `serverPathPrefix` is honored, and the `/ws` suffix is always present.
- The two bugs above (shutdown and auth refresh) share one mechanism — a control channel — so we fix them uniformly and don't proliferate lifecycle primitives.

## Non-Goals

- Wire-protocol changes. Auth refresh is implemented as an internal reconnect inside the Rust transport; no new server-side handshake frame is introduced.
- Preserving in-flight sync state across an auth-refresh-triggered reconnect on the server side. If the server needs to smooth this gap, that is a separate design.
- React Native auth refresh end-to-end. RN uses `JazzClient.updateAuthToken()` directly and does not route through a worker; the RN binding still gains `update_auth` for symmetry, but the RN end-to-end flow is unchanged.
- Merging the Tokio and WASM `TransportManager::run` implementations. Both implementations are extended identically; the existing duality stays.
- Restructuring `TransportHandle` beyond adding one field plus two methods.

## Background: Current Failures

### Issue 1 — `update-auth` is a no-op

`packages/jazz-tools/src/worker/jazz-worker.ts` handles the `update-auth` message by falling through to `break`:

```ts
case "update-auth":
    // With the Rust-owned transport, re-connecting with a new token is handled
    // by disconnecting and reconnecting. Not yet wired; left as no-op.
    break;
```

`Db.applyAuthUpdate()` does call `workerBridge.updateAuth({ jwtToken })`, and `WorkerBridge.updateAuth` does post the message — but the worker drops it on the floor. The Rust transport's `AuthConfig` is a constructor-time parameter and has no mutation path, so once the initial JWT expires the worker's sync is broken until a full restart.

### Issue 2 — `TransportManager` reconnect loop leak

The Rust bindings (`crates/jazz-napi/src/lib.rs`, `crates/jazz-wasm/src/runtime.rs`, `crates/jazz-rn/rust/src/lib.rs`) all spawn `manager.run()` and drop the `JoinHandle`. `TransportManager::run` only observes the "shutdown" signal inside `run_connected`, where it reads `None` from `outbox_rx`. During the connect, handshake, and backoff phases the channel closure is invisible, so calling `disconnect()` before a successful connection (or during a reconnect cycle) leaves the background task retrying indefinitely. Every stop/start cycle that fails to establish a connection in time leaks a task.

### Issue 3 — Worker does not normalize the server URL

`jazz-worker.ts:handleInit` calls `runtime.connect(msg.serverUrl, JSON.stringify(auth))` with `msg.serverUrl` verbatim. The worker receives `serverUrl` and `serverPathPrefix` in `InitMessage` but uses neither `httpUrlToWs` nor the prefix. Non-worker code paths (`client.ts:1322`) already normalize via `httpUrlToWs`. In deployments where the app-level `serverUrl` is `http(s)://…` or requires a path prefix, the worker dials the wrong endpoint and upstream sync never attaches.

## Design

### Core: `TransportControl` enum and channel topology

Add to `crates/jazz-tools/src/transport_manager.rs`:

```rust
pub enum TransportControl {
    Shutdown,
    UpdateAuth(AuthConfig),
}
```

`TransportHandle` gains one field and two methods:

```rust
pub struct TransportHandle {
    outbox_tx: mpsc::UnboundedSender<OutboxEntry>,
    inbound_rx: mpsc::UnboundedReceiver<TransportInbound>,
    ever_connected: Arc<AtomicBool>,
    control_tx: mpsc::UnboundedSender<TransportControl>, // new
}

impl TransportHandle {
    pub fn disconnect(&self) {
        let _ = self.control_tx.send(TransportControl::Shutdown);
    }

    pub fn update_auth(&self, auth: AuthConfig) {
        let _ = self.control_tx.send(TransportControl::UpdateAuth(auth));
    }
}
```

`TransportManager` gains the matching `control_rx`. `create()` wires both halves. Send errors on the sender side are ignored — a closed receiver means the manager task is already gone, which is the same outcome as a successful shutdown.

### `run()` as a phase machine

The existing `run()` visits four phases per cycle: **Connect → Handshake → Connected → Backoff**. The new version wraps each phase in a `select!` that races the primary future against `control_rx.recv()`. Resolution rules:

| Phase         | On `Shutdown` / control channel closed | On `UpdateAuth(cfg)`                                                                 | On phase failure                   |
| ------------- | -------------------------------------- | ------------------------------------------------------------------------------------ | ---------------------------------- |
| Connect       | exit `run()`                           | swap `self.auth`, reset backoff, continue loop                                       | enter Backoff                      |
| Handshake     | close stream, exit                     | swap auth, close stream, reset backoff, continue (skip Backoff)                      | enter Backoff                      |
| Connected     | close stream, exit                     | emit `Disconnected`, swap auth, close stream, reset backoff, continue (skip Backoff) | emit `Disconnected`, enter Backoff |
| Backoff sleep | exit                                   | swap auth, reset backoff, continue (skip remaining sleep)                            | n/a                                |

#### Invariants

1. Control messages are honored from every phase; this is the sole shutdown mechanism.
2. `control_rx.recv()` returning `None` (because `TransportHandle` was dropped) is treated as `Shutdown` in every phase.
3. `UpdateAuth` always resets `ReconnectState` so a refresh during a long backoff triggers an immediate retry with fresh credentials.
4. `self.auth` is mutated only via `UpdateAuth`. Handshake reads it fresh each time it runs.
5. Whenever a live `Connected` stream is torn down because of a network error or an auth refresh, the manager emits `TransportInbound::Disconnected`. Shutdown does **not** emit `Disconnected` — the caller initiated the teardown and does not need to be told. Auth refresh is "seamless" from the caller's orchestration standpoint (no manual disconnect/reconnect), but observability still reflects the brief gap.
6. The existing `outbox_rx`-closed detection in `run_connected` is **removed**. The control channel is the single source of truth for shutdown. `outbox_rx` carries data only.

#### Both implementations change identically

The Tokio implementation uses `tokio::select!`; the WASM implementation uses `futures::select!`. Phase structure and resolution table are symmetric. No refactor to unify them.

### Rust bindings

All three bindings (NAPI, WASM, RN) gain one new method and modify `disconnect`:

```rust
// shape; each binding uses its own error type and lock idiom
pub fn update_auth(&self, auth_json: String) -> Result<(), _> {
    let auth: AuthConfig = serde_json::from_str(&auth_json)?;
    // fetch the current handle under lock; no-op if no transport is set
    if let Some(handle) = self.core.lock()?.transport() {
        handle.update_auth(auth);
    }
    Ok(())
}

pub fn disconnect(&self) {
    if let Ok(mut core) = self.core.lock() {
        if let Some(handle) = core.transport() {
            handle.disconnect(); // send Shutdown on control channel
        }
        core.clear_transport();
    }
}
```

`RuntimeCore` gains a `transport()` accessor (read-only handle reference). `clear_transport()` stays; it just drops the slot now that `disconnect()` on the handle drove the manager's exit via the control channel.

**TypeScript / generated surfaces:**

- `crates/jazz-napi/index.d.ts` — add `updateAuth(authJson: string): void`.
- `crates/jazz-rn/src/generated/jazz_rn.ts` — regenerated from UniFFI after adding the Rust method.
- WASM-bindgen surfaces the new method on the exported runtime class automatically.

**No-op on missing transport.** `update_auth` before `connect` is meaningless in the worker's path (worker only calls `updateAuth` after `init` established a connection), but the method should not error in that case — it just returns without doing anything.

### JS worker wiring

The worker is the only caller of `runtime.updateAuth`. It owns the "full auth blob" in JS memory so Rust can treat `AuthConfig` as an opaque whole (no partial-update type, no merge logic in Rust).

**Cached state in the worker:**

```ts
let currentAuth: Record<string, string> = {};
```

In practice this holds `jwt_token` and/or `admin_secret` — the only auth-related fields the worker sees today via `InitMessage`. The `Record<string, string>` shape is permissive so future init fields (e.g., `backend_session`) can be added without restructuring.

**In `handleInit`:** populate `currentAuth` from `msg.jwtToken` / `msg.adminSecret` (and any future auth-related init fields), then pass the JSON-serialized `currentAuth` to `runtime.connect`:

```ts
if (msg.jwtToken) currentAuth.jwt_token = msg.jwtToken;
if (msg.adminSecret) currentAuth.admin_secret = msg.adminSecret;
const wsUrl = httpUrlToWs(msg.serverUrl, msg.serverPathPrefix);
runtime.connect(wsUrl, JSON.stringify(currentAuth));
```

**In `case "update-auth":`** replace the no-op with:

```ts
case "update-auth": {
    if (msg.jwtToken) {
        currentAuth.jwt_token = msg.jwtToken;
    } else {
        delete currentAuth.jwt_token;
    }
    try {
        runtime.updateAuth(JSON.stringify(currentAuth));
    } catch (e) {
        console.error("[worker] runtime.updateAuth failed:", e);
    }
    break;
}
```

**Wire protocol between main thread and worker** stays the same. `UpdateAuthMessage` remains `{ type: "update-auth"; jwtToken?: string }`.

### URL normalization

Lift `httpUrlToWs` from `packages/jazz-tools/src/runtime/client.ts` to a shared module `packages/jazz-tools/src/runtime/url.ts`. Extend its signature with an optional path prefix:

```ts
export function httpUrlToWs(serverUrl: string, pathPrefix?: string): string;
```

Behavior:

- Normalize `serverUrl`: trim trailing slashes; require one of `http://`, `https://`, `ws://`, `wss://`.
- Normalize `pathPrefix` if present: strip leading and trailing slashes; treat empty as absent.
- Compose as `{scheme-swapped-host}{/prefix-if-any}/ws`.
- Throw for invalid `serverUrl` scheme; callers decide how to surface it.

Call-site changes:

- `client.ts:1322` — call unchanged (`httpUrlToWs(url)`), still passes a composed URL with prefix already inline.
- `jazz-worker.ts:handleInit` — compute `httpUrlToWs(msg.serverUrl, msg.serverPathPrefix)`. On throw, log and skip connect (same error surface as the current try/catch around `runtime.connect`).

#### Examples

| `serverUrl`                | `pathPrefix` | Result                            |
| -------------------------- | ------------ | --------------------------------- |
| `http://localhost:4000`    | —            | `ws://localhost:4000/ws`          |
| `https://api.example.com/` | —            | `wss://api.example.com/ws`        |
| `http://localhost:4000`    | `/apps/xyz`  | `ws://localhost:4000/apps/xyz/ws` |
| `http://localhost:4000`    | `apps/xyz/`  | `ws://localhost:4000/apps/xyz/ws` |
| `ws://host/already/ws`     | —            | `ws://host/already/ws`            |

## Testing

### Rust unit tests (`transport_manager.rs`)

Introduce a `TestStreamAdapter` implementing `StreamAdapter` with controllable hooks (pause/resume `connect`; block or inject `recv`; observe `close`). Run the suite on the Tokio implementation. On WASM, run scenarios 4 (`shutdown_during_connected`) and 6 (`update_auth_during_connected`) as `#[wasm_bindgen_test]` cases to confirm the `futures::select!` branches behave symmetrically; the other scenarios exercise the same control-channel plumbing and are sufficient to cover on Tokio only. Scenarios:

1. `shutdown_during_connect` — `connect` future never resolves; send `Shutdown`; manager exits before connect completes.
2. `shutdown_during_backoff` — first `connect` fails fast; during backoff sleep send `Shutdown`; manager exits before sleep elapses.
3. `shutdown_during_handshake` — `connect` resolves; `recv` blocks (handshake pending); send `Shutdown`; manager calls `close` and exits.
4. `shutdown_during_connected` — fully connected; send `Shutdown`; manager emits `Disconnected`, calls `close`, exits.
5. `update_auth_during_backoff` — during backoff sleep send `UpdateAuth(cfg2)`; assert sleep is skipped, backoff is reset, next `connect` carries `cfg2` into the handshake.
6. `update_auth_during_connected` — while connected, send `UpdateAuth(cfg2)`; assert `Disconnected` is emitted, `close` is called, reconnect runs, handshake uses `cfg2`, `Connected` is re-emitted.
7. `handle_dropped_is_shutdown` — drop `TransportHandle` from every phase; manager exits.

The existing `handshake_marks_ever_connected_and_notifies_tick` test stays; adjust only if `TransportHandle`/`TransportManager` constructor signatures change.

### JS unit tests

New test file for the lifted `httpUrlToWs(serverUrl, pathPrefix?)`:

- no prefix; empty prefix; leading / trailing slashes on prefix
- http → ws; https → wss; ws/wss passthrough
- invalid scheme throws

### Worker integration tests

Using a fake `runtime` stub in the worker's test harness:

- **URL wiring** — send `init` with `serverUrl: "http://localhost:4000"` + `serverPathPrefix: "/apps/xyz"`; assert `runtime.connect` is called with `"ws://localhost:4000/apps/xyz/ws"` and JSON-serialized auth.
- **Auth refresh wiring** — send `init` with `jwtToken: "old"` + `adminSecret: "s"`; send `update-auth` with `jwtToken: "new"`; assert `runtime.updateAuth` is called with JSON containing both `jwt_token: "new"` and `admin_secret: "s"` (confirms JS-side merge).
- **Auth clear** — send `update-auth` with no `jwtToken`; assert the serialized blob drops `jwt_token` but retains `admin_secret`.

### E2E auth refresh

Extend `packages/jazz-tools/tests/browser/db.auth-refresh.test.ts` (or add a sibling focused on the worker path) so the existing refresh scenario runs through `workerBridge` instead of only the main-thread client. Assert that a post-refresh write succeeds without a worker restart.

## Migration / Backcompat

- **Prototype latitude:** the project is pre-launch, so no migration shims. The `// not yet wired; left as no-op` comment in `jazz-worker.ts` is deleted outright.
- **Binding surface:** `update_auth` is purely additive. `connect` and `disconnect` keep their existing signatures; `disconnect` changes behavior (now drives shutdown through the control channel) but callers see the same effect.
- **Core surface:** adds `RuntimeCore::transport()` accessor. Existing `set_transport` / `clear_transport` stay.
- **Wire protocol:** unchanged. `UpdateAuthMessage` shape in `worker-protocol.ts` stays as-is.
- **Generated RN TS:** regenerate once the Rust side compiles.
