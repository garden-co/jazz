# WebSocket Transport — Critical Fixes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the 10 critical / must-fix findings from the review of `feat/transport-layer-ws` so the branch is shippable. Does **not** cover perf tuning, simplicity cleanups, or broader integration coverage — those ship in follow-up plans.

**Architecture:** The WebSocket transport is owned by Rust (`TransportManager` in `crates/jazz-tools/src/transport_manager.rs`). RuntimeCore holds a `TransportHandle` that communicates with the manager over `futures::channel::mpsc`. Auth-failure is surfaced through a `TransportInbound::AuthFailure` event drained by `RuntimeCore::take_auth_failure()`, which platforms (NAPI / WASM / UniFFI) expose to JS, where `JazzClient` polls every 500 ms and fires `onAuthFailure`. The worker bridge sits between the main thread and a worker that owns its own runtime + transport.

**Tech Stack:** Rust (tokio, futures, tokio-tungstenite, web-sys WebSocket), TypeScript (jazz-tools client + worker), NAPI-RS, wasm-bindgen, UniFFI.

**Commit convention:** Use the repo's existing prefixes (`fix(transport):`, `test(transport):`, `chore(transport):`). Never include any Claude attribution.

**Pre-flight:** Run `pnpm build && cargo test -p jazz-tools --features client,server` from the repo root on a clean checkout of `feat/transport-layer-ws` before starting; every task must leave the build green.

---

## File Structure

Files touched across the plan, with their one-line responsibility:

- `crates/jazz-tools/tests/websocket_transport.rs` — Rust E2E test for the WebSocket transport happy path (fix red assertion).
- `crates/jazz-tools/src/routes.rs` — axum WS handler; reject malformed client_id.
- `crates/jazz-cloud-server/src/server.rs` — cloud-server WS handler; reject malformed client_id.
- `crates/jazz-tools/src/transport_manager.rs` — TransportManager + TransportHandle; bounded channels, serde-fail handling, shutdown signal, WASM backoff fix.
- `crates/jazz-tools/src/ws_stream/wasm.rs` (new, may be stub if we fold into jazz-wasm) — WASM backoff timer via `gloo-timers`.
- `crates/jazz-wasm/Cargo.toml` + `crates/jazz-tools/Cargo.toml` — add `gloo-timers` under wasm32 target.
- `crates/jazz-tools/src/runtime_tokio.rs` — expose `is_transport_connected()` reflecting live state.
- `crates/jazz-tools/src/client.rs` — honor `wait_until_transport_connected`'s bool, fix `is_connected()`.
- `crates/jazz-napi/src/lib.rs`, `crates/jazz-wasm/src/runtime.rs`, `crates/jazz-rn/rust/src/lib.rs` — expose `isTransportConnected()` across bindings.
- `packages/jazz-tools/src/runtime/client.ts` — `asBackend()` + `updateAuthToken()` now trigger `disconnect()` + re-`setupSync()`.
- `packages/jazz-tools/src/react-native/jazz-rn-runtime-adapter.ts` — proxy `takeAuthFailure()` and `isTransportConnected()` into the adapter.
- `packages/jazz-tools/src/runtime/client.ts` (Runtime interface) — add `isTransportConnected?()` to the interface.
- `packages/jazz-tools/src/worker/jazz-worker.ts` — emit `upstream-connected` once transport is live.

---

## Task 1: Fix the red integration test

**Files:**

- Modify: `crates/jazz-tools/tests/websocket_transport.rs:199-204`

The declared schema in `todos_schema()` defines columns `title` then `completed` (lines 22-23), and the runtime returns values in declared-schema order — not alphabetical, as the comment claimed. The assertion is inverted.

- [ ] **Step 1: Run the failing test to confirm baseline**

Run:

```
cargo test -p jazz-tools --test websocket_transport --features test,client,server two_clients_sync_via_websocket
```

Expected output (abbreviated):

```
assertion `left == right` failed: bob's row values mismatch, got: [Text("buy milk"), Boolean(false)]
  left: [Text("buy milk"), Boolean(false)]
 right: [Boolean(false), Text("buy milk")]
```

- [ ] **Step 2: Fix the assertion and comment**

Edit `crates/jazz-tools/tests/websocket_transport.rs` lines 199-204 from:

```rust
    // Columns are returned in alphabetical order: completed (Boolean) then title (Text).
    assert_eq!(
        *values,
        vec![Value::Boolean(false), Value::Text("buy milk".to_string()),],
        "bob's row values mismatch, got: {values:?}"
    );
```

to:

```rust
    // Columns are returned in declared-schema order: title (Text) then completed (Boolean).
    assert_eq!(
        *values,
        vec![Value::Text("buy milk".to_string()), Value::Boolean(false)],
        "bob's row values mismatch, got: {values:?}"
    );
```

- [ ] **Step 3: Run the test to verify it passes**

Run:

```
cargo test -p jazz-tools --test websocket_transport --features test,client,server
```

Expected:

```
test client_connects_via_websocket_and_receives_connected_event ... ok
test two_clients_sync_via_websocket ... ok

test result: ok. 2 passed; 0 failed
```

- [ ] **Step 4: Commit**

```
git add crates/jazz-tools/tests/websocket_transport.rs
git commit -m "test(transport): fix websocket_transport assertion to match declared column order"
```

---

## Task 2: Reject malformed `client_id` in WS handshake

**Files:**

- Modify: `crates/jazz-tools/src/routes.rs:412`
- Modify: `crates/jazz-cloud-server/src/server.rs:3619`
- Add test: `crates/jazz-tools/tests/websocket_transport.rs` (new test function)

`ClientId::parse(...).unwrap_or_default()` collapses any malformed client_id to the zero UUID. Malicious or buggy clients collide on a single server-side identity; ack/permission accounting corrupts. We need to send an error frame and close.

- [ ] **Step 1: Write a failing test**

Append to `crates/jazz-tools/tests/websocket_transport.rs`:

```rust
#[tokio::test]
async fn server_rejects_malformed_client_id() {
    use futures::SinkExt as _;
    use futures::StreamExt as _;
    use tokio_tungstenite::tungstenite::protocol::Message;

    let schema = todos_schema();
    let app_id = AppId::new();
    let server = support::spawn_test_server(schema.clone(), app_id).await;

    let ws_url = server.ws_url();
    let (mut ws, _) = tokio_tungstenite::connect_async(&ws_url)
        .await
        .expect("ws connect");

    // Send a handshake with a malformed client_id.
    let bad = serde_json::json!({
        "client_id": "not-a-uuid",
        "auth": { "admin_secret": server.admin_secret() },
        "catalogue_state_hash": null,
    });
    let bytes = serde_json::to_vec(&bad).unwrap();
    let mut frame = Vec::with_capacity(4 + bytes.len());
    frame.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
    frame.extend_from_slice(&bytes);
    ws.send(Message::Binary(frame)).await.expect("send");

    // Expect an Error frame and close — not a Connected frame.
    let response = ws.next().await.expect("recv").expect("ws msg");
    let data = match response {
        Message::Binary(d) => d,
        other => panic!("expected binary error frame, got: {:?}", other),
    };
    let event = jazz_tools::transport_protocol::ServerEvent::decode_frame(&data)
        .expect("decode")
        .0;
    match event {
        jazz_tools::transport_protocol::ServerEvent::Error { code, .. } => {
            assert_eq!(
                code,
                jazz_tools::transport_protocol::ErrorCode::BadRequest,
                "expected BadRequest for malformed client_id"
            );
        }
        other => panic!("expected Error frame, got {:?}", other),
    }
}
```

(If `spawn_test_server` / `admin_secret` helpers don't already exist in `tests/support/mod.rs` with these exact names, use the existing equivalent — the test file's other tests already pattern the helper usage.)

- [ ] **Step 2: Run the test to verify it fails**

Run:

```
cargo test -p jazz-tools --test websocket_transport --features test,client,server server_rejects_malformed_client_id
```

Expected: FAIL — currently the handshake accepts the bad id and sends `Connected`.

- [ ] **Step 3: Implement the fix in `routes.rs`**

Replace `crates/jazz-tools/src/routes.rs:411-412`:

```rust
    // Parse client_id from handshake — generate if malformed.
    let client_id = ClientId::parse(&handshake.client_id).unwrap_or_default();
```

with:

```rust
    // Parse client_id from handshake — reject if malformed instead of
    // silently colliding on the zero UUID.
    let client_id = match ClientId::parse(&handshake.client_id) {
        Ok(id) => id,
        Err(_) => {
            let _ = send_ws_error(
                &mut socket,
                crate::transport_protocol::ErrorCode::BadRequest,
                "malformed client_id in handshake",
            )
            .await;
            return;
        }
    };
```

If the existing `send_ws_error` helper does not yet take an `ErrorCode`, adapt its signature in the same commit — search for `fn send_ws_error` in `routes.rs` and add the code parameter plumbed through to the emitted `ServerEvent::Error { code, message }` frame.

- [ ] **Step 4: Mirror the fix in the cloud-server**

Replace `crates/jazz-cloud-server/src/server.rs:3619`:

```rust
    let client_id = ClientId::parse(&handshake.client_id).unwrap_or_default();
```

with:

```rust
    let client_id = match ClientId::parse(&handshake.client_id) {
        Ok(id) => id,
        Err(_) => {
            let _ = send_ws_error(
                &mut socket,
                jazz_tools::transport_protocol::ErrorCode::BadRequest,
                "malformed client_id in handshake",
            )
            .await;
            return;
        }
    };
```

Adjust the `send_ws_error` import / path to match what's actually available in the cloud-server module. If there is no such helper in cloud-server, copy the routes.rs helper inline or promote it.

- [ ] **Step 5: Run tests to verify they pass**

Run:

```
cargo test -p jazz-tools --test websocket_transport --features test,client,server
cargo build -p jazz-cloud-server
```

Expected: all tests pass, cloud-server builds.

- [ ] **Step 6: Commit**

```
git add crates/jazz-tools/src/routes.rs crates/jazz-cloud-server/src/server.rs \
        crates/jazz-tools/tests/websocket_transport.rs
git commit -m "fix(transport): reject malformed client_id in WS handshake instead of zero-defaulting"
```

---

## Task 3: Don't emit empty frames on serde failure

**Files:**

- Modify: `crates/jazz-tools/src/transport_manager.rs:464-484` (fn `encode_outbox_entry_as_frame`) and `:323-326` (call site).

`encode_outbox_entry_as_frame` currently returns `Vec::new()` when `serde_json::to_vec` fails, and the caller unconditionally `ws.send(&frame)`s it. The server's `data.len() < 4` parse branch treats it as malformed and closes the connection — a single unserializable payload tears down the transport.

- [ ] **Step 1: Change the function to return `Option<Vec<u8>>`**

Replace `encode_outbox_entry_as_frame` (lines 464-484) with:

```rust
/// Encode an OutboxEntry as a binary frame: [4-byte u32 BE length][JSON bytes]
/// Returns `None` if the payload cannot be serialized — callers MUST skip the
/// send in that case (never emit an empty frame; the server would treat it as
/// malformed and close the connection).
fn encode_outbox_entry_as_frame(
    entry: &OutboxEntry,
    client_id: crate::sync_manager::types::ClientId,
) -> Option<Vec<u8>> {
    use crate::transport_protocol::SyncBatchRequest;
    let batch = SyncBatchRequest {
        payloads: vec![entry.payload.clone()],
        client_id,
    };
    let json = match serde_json::to_vec(&batch) {
        Ok(j) => j,
        Err(e) => {
            tracing::error!(
                "dropping outbox entry: failed to serialize SyncBatchRequest: {e}"
            );
            return None;
        }
    };
    let mut frame = Vec::with_capacity(4 + json.len());
    frame.extend_from_slice(&(json.len() as u32).to_be_bytes());
    frame.extend_from_slice(&json);
    Some(frame)
}
```

- [ ] **Step 2: Update the call site**

In `run_connected` (around line 323), replace:

```rust
                    let frame = encode_outbox_entry_as_frame(&entry, self.client_id);
                    if ws.send(&frame).await.is_err() {
                        break;
                    }
```

with:

```rust
                    let Some(frame) = encode_outbox_entry_as_frame(&entry, self.client_id) else {
                        // Drop this entry; keep the transport alive.
                        continue;
                    };
                    if ws.send(&frame).await.is_err() {
                        break;
                    }
```

- [ ] **Step 3: Verify build + tests**

Run:

```
cargo test -p jazz-tools --features client,server
```

Expected: all tests pass. (We don't add a dedicated test for serde failure because constructing an unserializable `SyncPayload` is contrived; a unit test around `encode_outbox_entry_as_frame` would only cover the happy path. The change is small and localized.)

- [ ] **Step 4: Commit**

```
git add crates/jazz-tools/src/transport_manager.rs
git commit -m "fix(transport): drop outbox entries that fail serialization instead of sending empty frames"
```

---

## Task 4: Bound the mpsc channels

**Files:**

- Modify: `crates/jazz-tools/src/transport_manager.rs` (channel construction + `send_outbox`).

`mpsc::unbounded()` has no backpressure: a stalled WS write or slow `batched_tick` grows buffers until OOM. We bound both channels to a high but finite capacity; on full-outbox we log and drop (the local store still has the write — the next reconnect resync via catalogue state hash brings the server up to date). Inbound we similarly drop with a loud log, since runaway inbound growth means the tick scheduler is wedged — a separate bug.

- [ ] **Step 1: Add a constant and switch to bounded channels**

Near the top of `crates/jazz-tools/src/transport_manager.rs`, add:

```rust
/// Capacity for the outbox channel (main thread → transport manager).
/// Sized to absorb burst writes during transient network stalls. If the
/// channel fills we drop entries with an error log — the local store
/// still has every write, and reconnect resync will bring the server up
/// to date via catalogue_state_hash.
const OUTBOX_CAPACITY: usize = 65_536;
/// Capacity for the inbound channel (transport manager → runtime tick).
/// Runaway inbound growth means the tick scheduler is wedged — we log
/// and drop so the transport itself does not OOM.
const INBOUND_CAPACITY: usize = 65_536;
```

Replace channel construction in `create()` (around line 219):

```rust
    let (outbox_tx, outbox_rx) = mpsc::unbounded();
    let (inbound_tx, inbound_rx) = mpsc::unbounded();
```

with:

```rust
    let (outbox_tx, outbox_rx) = mpsc::channel(OUTBOX_CAPACITY);
    let (inbound_tx, inbound_rx) = mpsc::channel(INBOUND_CAPACITY);
```

Update the field types on `TransportHandle` and `TransportManager`:

```rust
pub struct TransportHandle {
    pub server_id: ServerId,
    pub client_id: crate::sync_manager::types::ClientId,
    pub outbox_tx: mpsc::Sender<OutboxEntry>,
    pub inbound_rx: mpsc::Receiver<TransportInbound>,
    pub ever_connected: std::sync::Arc<std::sync::atomic::AtomicBool>,
}
```

```rust
pub struct TransportManager<W: StreamAdapter, T: TickNotifier> {
    pub server_id: ServerId,
    pub url: String,
    pub auth: AuthConfig,
    outbox_rx: mpsc::Receiver<OutboxEntry>,
    inbound_tx: mpsc::Sender<TransportInbound>,
    pub tick: T,
    reconnect: ReconnectState,
    pub client_id: crate::sync_manager::types::ClientId,
    ever_connected: std::sync::Arc<std::sync::atomic::AtomicBool>,
    _stream: std::marker::PhantomData<W>,
}
```

- [ ] **Step 2: Update `send_outbox` to use `try_send` with drop-on-full**

Replace:

```rust
    pub fn send_outbox(&self, entry: OutboxEntry) {
        let _ = self.outbox_tx.unbounded_send(entry);
    }
```

with (note: we now need `&mut self` because bounded `try_send` is `&mut`):

```rust
    pub fn send_outbox(&mut self, entry: OutboxEntry) {
        if let Err(e) = self.outbox_tx.try_send(entry) {
            if e.is_full() {
                tracing::error!(
                    "outbox channel full (capacity {OUTBOX_CAPACITY}); dropping entry — \
                     this is a transport back-pressure bug and will resync on reconnect",
                );
            }
            // If the channel is disconnected, the manager has exited; nothing to do.
        }
    }
```

- [ ] **Step 3: Audit all `send_outbox` callers for `&mut` requirement**

Run:

```
cargo build -p jazz-tools --features client,server 2>&1 | grep -i "send_outbox\|cannot borrow"
```

If any caller now fails because it holds `&self`, either:

- Change the caller chain to `&mut self`, or
- Revert to `&self` on `send_outbox` and wrap the sender in `std::sync::Mutex<mpsc::Sender<_>>` inside `TransportHandle` (the sender is small and `try_send` contention is negligible).

Prefer the Mutex approach if more than two callers need patching — it's a smaller blast radius. Sketch:

```rust
pub struct TransportHandle {
    // ...
    outbox_tx: std::sync::Mutex<mpsc::Sender<OutboxEntry>>,
    // ...
}

impl TransportHandle {
    pub fn send_outbox(&self, entry: OutboxEntry) {
        let mut tx = match self.outbox_tx.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        if let Err(e) = tx.try_send(entry) {
            if e.is_full() {
                tracing::error!(
                    "outbox channel full (capacity {OUTBOX_CAPACITY}); dropping entry"
                );
            }
        }
    }
}
```

Adapt the `create()` function to wrap the sender accordingly. Use the Mutex variant if the `&self` → `&mut self` change cascades beyond `RuntimeCore::flush_outbox`.

- [ ] **Step 4: Update `inbound_tx` callers**

Inbound sends happen only inside `TransportManager::run()` which owns `self` mutably — no caller patching needed. Update the two unbounded_send calls in run() to `try_send`; on `Err(full)` emit `tracing::error!("inbound channel full")` and drop.

- [ ] **Step 5: Verify build and tests**

Run:

```
cargo test -p jazz-tools --features client,server
cargo test -p jazz-tools --test websocket_transport --features test,client,server
```

Expected: all pass.

- [ ] **Step 6: Commit**

```
git add crates/jazz-tools/src/transport_manager.rs
git commit -m "fix(transport): bound outbox and inbound mpsc channels to prevent OOM on stalled links"
```

---

## Task 5: Real backoff timer on WASM

**Files:**

- Modify: `crates/jazz-tools/Cargo.toml` (wasm32 target deps).
- Modify: `crates/jazz-tools/src/transport_manager.rs:132-149` (`ReconnectState::backoff`).

On WASM, `backoff()` reduces to `futures::future::ready(()).await` — a failing connect hot-loops the microtask queue. Use `gloo-timers::future::TimeoutFuture` for a real sleep.

- [ ] **Step 1: Add `gloo-timers` as a wasm32 dep on `jazz-tools`**

In `crates/jazz-tools/Cargo.toml`, under the existing wasm32 target section (find the block that starts with `[target.'cfg(target_arch = "wasm32")'.dependencies]` — if absent, add one), add:

```toml
[target.'cfg(target_arch = "wasm32")'.dependencies]
gloo-timers = { version = "0.3", features = ["futures"] }
```

Keep any existing entries in that block.

- [ ] **Step 2: Swap the WASM arm of `backoff()`**

In `crates/jazz-tools/src/transport_manager.rs`, replace the body of `ReconnectState::backoff()` (lines 132-149):

```rust
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
```

with:

```rust
    pub async fn backoff(&mut self) {
        let base_ms = 300u64.saturating_mul(1u64 << self.attempt.min(5));
        let capped = base_ms.min(10_000);
        let jitter = (rand::random::<u8>() as u64 * 200) / 255;
        let delay_ms = capped + jitter;
        #[cfg(all(not(target_arch = "wasm32"), feature = "runtime-tokio"))]
        {
            tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
        }
        #[cfg(target_arch = "wasm32")]
        {
            gloo_timers::future::TimeoutFuture::new(delay_ms as u32).await;
        }
        #[cfg(all(not(target_arch = "wasm32"), not(feature = "runtime-tokio")))]
        {
            // Non-wasm, non-tokio: no supported async timer. This config is
            // not shipped; if it ever is, wire up async-std or smol here.
            let _ = delay_ms;
            futures::future::ready(()).await;
        }
        self.attempt += 1;
    }
```

- [ ] **Step 3: Verify WASM build**

Run:

```
cargo build -p jazz-wasm --target wasm32-unknown-unknown
```

Expected: builds. (If the wasm32 target isn't installed locally, run `rustup target add wasm32-unknown-unknown` first.)

Also verify native builds are still clean:

```
cargo build -p jazz-tools --features client,server
```

- [ ] **Step 4: Commit**

```
git add crates/jazz-tools/Cargo.toml crates/jazz-tools/src/transport_manager.rs
git commit -m "fix(transport): use gloo-timers for real backoff sleep on WASM"
```

---

## Task 6: Shutdown signal for `run()`

**Files:**

- Modify: `crates/jazz-tools/src/transport_manager.rs`.
- Add test: `crates/jazz-tools/tests/websocket_transport.rs`.

`run()` currently only notices `TransportHandle` drop from inside `run_connected`. During `W::connect(...).await` or `backoff().await`, a client `disconnect()` leaves the manager running until the underlying future completes (native: up to 75 s of TCP SYN timeout; WASM: now a real sleep). Add an explicit oneshot cancellation signal.

- [ ] **Step 1: Write a failing test**

Append to `crates/jazz-tools/tests/websocket_transport.rs`:

```rust
#[tokio::test]
async fn dropping_handle_during_connect_stops_manager_quickly() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    // A fake StreamAdapter whose `connect` blocks for 30 s, so we can verify
    // that dropping the handle aborts well before the connect returns.
    struct SlowAdapter;
    impl jazz_tools::transport_manager::StreamAdapter for SlowAdapter {
        type Error = String;
        async fn connect(_url: &str) -> Result<Self, Self::Error> {
            tokio::time::sleep(Duration::from_secs(30)).await;
            Err("never".into())
        }
        async fn send(&mut self, _data: &[u8]) -> Result<(), Self::Error> {
            unreachable!()
        }
        async fn recv(&mut self) -> Result<Option<Vec<u8>>, Self::Error> {
            unreachable!()
        }
        async fn close(&mut self) {}
    }

    struct TestNotifier(Arc<AtomicBool>);
    impl jazz_tools::transport_manager::TickNotifier for TestNotifier {
        fn notify(&self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    let notified = Arc::new(AtomicBool::new(false));
    let (handle, manager) = jazz_tools::transport_manager::create::<SlowAdapter, _>(
        "ws://invalid.invalid/".into(),
        jazz_tools::transport_manager::AuthConfig::default(),
        TestNotifier(notified.clone()),
    );

    let run_task = tokio::spawn(manager.run());

    // Give the manager a moment to enter W::connect.
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Drop the handle — this must cancel the in-flight connect() quickly.
    drop(handle);

    // run() should return within a second (not 30s).
    tokio::time::timeout(Duration::from_secs(2), run_task)
        .await
        .expect("manager.run() did not exit promptly after handle drop")
        .expect("run task panicked");
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run:

```
cargo test -p jazz-tools --test websocket_transport --features test,client,server \
    dropping_handle_during_connect_stops_manager_quickly
```

Expected: the test times out at 2 s because the manager is stuck in `SlowAdapter::connect` — `run()` never observes the handle drop.

- [ ] **Step 3: Add a shutdown oneshot to `TransportHandle` and `TransportManager`**

In `crates/jazz-tools/src/transport_manager.rs`, add the import at the top (it may already be re-exported):

```rust
use futures::channel::oneshot;
use futures::future::FutureExt as _;
```

Update `TransportHandle`:

```rust
pub struct TransportHandle {
    pub server_id: ServerId,
    pub client_id: crate::sync_manager::types::ClientId,
    pub outbox_tx: mpsc::Sender<OutboxEntry>,
    pub inbound_rx: mpsc::Receiver<TransportInbound>,
    pub ever_connected: std::sync::Arc<std::sync::atomic::AtomicBool>,
    /// Never `send()`-ed to — dropping this sender cancels the manager's
    /// shutdown receiver, which `run()` selects against at every .await.
    _shutdown: oneshot::Sender<()>,
}
```

Update `TransportManager`:

```rust
pub struct TransportManager<W: StreamAdapter, T: TickNotifier> {
    pub server_id: ServerId,
    pub url: String,
    pub auth: AuthConfig,
    outbox_rx: mpsc::Receiver<OutboxEntry>,
    inbound_tx: mpsc::Sender<TransportInbound>,
    pub tick: T,
    reconnect: ReconnectState,
    pub client_id: crate::sync_manager::types::ClientId,
    ever_connected: std::sync::Arc<std::sync::atomic::AtomicBool>,
    shutdown: futures::future::Fuse<oneshot::Receiver<()>>,
    _stream: std::marker::PhantomData<W>,
}
```

Update `create()`:

```rust
pub fn create<W: StreamAdapter, T: TickNotifier>(
    url: String,
    auth: AuthConfig,
    tick: T,
) -> (TransportHandle, TransportManager<W, T>) {
    let server_id = ServerId::new();
    let client_id = crate::sync_manager::types::ClientId::new();
    let (outbox_tx, outbox_rx) = mpsc::channel(OUTBOX_CAPACITY);
    let (inbound_tx, inbound_rx) = mpsc::channel(INBOUND_CAPACITY);
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let ever_connected = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let handle = TransportHandle {
        server_id,
        client_id,
        outbox_tx,
        inbound_rx,
        ever_connected: ever_connected.clone(),
        _shutdown: shutdown_tx,
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
        shutdown: shutdown_rx.fuse(),
        _stream: std::marker::PhantomData,
    };
    (handle, manager)
}
```

- [ ] **Step 4: Race the shutdown receiver at every `.await` in `run()`**

Replace `run()` body with:

```rust
    pub async fn run(mut self) {
        loop {
            let connect_fut = W::connect(&self.url).fuse();
            futures::pin_mut!(connect_fut);
            let connect_result = futures::select! {
                _ = self.shutdown => return,
                r = connect_fut => r,
            };

            match connect_result {
                Ok(mut ws) => {
                    let handshake_fut = self.perform_auth_handshake(&mut ws, None).fuse();
                    futures::pin_mut!(handshake_fut);
                    let handshake_result = futures::select! {
                        _ = self.shutdown => return,
                        r = handshake_fut => r,
                    };

                    match handshake_result {
                        Ok(connected) => {
                            self.ever_connected
                                .store(true, std::sync::atomic::Ordering::Release);
                            let _ = self.inbound_tx.try_send(TransportInbound::Connected {
                                catalogue_state_hash: connected.catalogue_state_hash,
                                next_sync_seq: connected.next_sync_seq,
                            });
                            self.tick.notify();

                            let run_fut = self.run_connected(&mut ws).fuse();
                            futures::pin_mut!(run_fut);
                            let exit = futures::select! {
                                _ = self.shutdown => {
                                    let _ = self.inbound_tx.try_send(TransportInbound::Disconnected);
                                    self.tick.notify();
                                    return;
                                }
                                e = run_fut => e,
                            };

                            let _ = self.inbound_tx.try_send(TransportInbound::Disconnected);
                            self.tick.notify();
                            if let ConnectedExit::HandleDropped = exit {
                                return;
                            }
                        }
                        Err(HandshakeError::Auth { reason, message }) => {
                            tracing::warn!(
                                ?reason,
                                "WebSocket auth handshake rejected: {message}"
                            );
                            let _ = self
                                .inbound_tx
                                .try_send(TransportInbound::AuthFailure { reason });
                            self.tick.notify();
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

            let backoff_fut = self.reconnect.backoff().fuse();
            futures::pin_mut!(backoff_fut);
            futures::select! {
                _ = self.shutdown => return,
                _ = backoff_fut => {}
            }
        }
    }
```

Notes:

- `self.shutdown` is already a `Fuse<oneshot::Receiver<()>>` so it can be polled multiple times across `select!` iterations; it completes (with `Err(Canceled)`) when the sender on the handle is dropped.
- `try_send` replaces `unbounded_send` on `self.inbound_tx` (required by Task 4's bounded channel change).

- [ ] **Step 5: Run the test to verify it passes**

Run:

```
cargo test -p jazz-tools --test websocket_transport --features test,client,server \
    dropping_handle_during_connect_stops_manager_quickly
```

Expected: PASS in well under 2 s.

Also run the full test suite:

```
cargo test -p jazz-tools --test websocket_transport --features test,client,server
```

All tests pass.

- [ ] **Step 6: Commit**

```
git add crates/jazz-tools/src/transport_manager.rs crates/jazz-tools/tests/websocket_transport.rs
git commit -m "fix(transport): shutdown signal for run() so handle drop aborts connect/backoff immediately"
```

---

## Task 7: Honor `wait_until_transport_connected` and fix `is_connected()`

**Files:**

- Modify: `crates/jazz-tools/src/client.rs:186-188` (handle the bool).
- Modify: `crates/jazz-tools/src/client.rs:50,162,195,376-378` (`has_server_url` → live state).
- Modify: `crates/jazz-tools/src/runtime_tokio.rs` (expose `is_transport_connected()`).
- Expose the same on NAPI/WASM/UniFFI bindings:
  - `crates/jazz-napi/src/lib.rs`
  - `crates/jazz-napi/index.d.ts`
  - `crates/jazz-wasm/src/runtime.rs`
  - `crates/jazz-rn/rust/src/lib.rs`
- Add test: `crates/jazz-tools/tests/websocket_transport.rs`.

`JazzClient::connect()` drops the `bool` returned by `wait_until_transport_connected()`, and `is_connected()` reads a field that's only true/false based on URL configuration. A bad URL or auth failure gives back a `JazzClient` that confidently reports itself connected.

- [ ] **Step 1: Add a failing integration test**

Append to `crates/jazz-tools/tests/websocket_transport.rs`:

```rust
#[tokio::test]
async fn connect_returns_error_when_server_unreachable() {
    use jazz_tools::{AppContext, ClientStorage, JazzClient};

    let schema = todos_schema();
    // Non-routable address — connect() will never complete within the timeout.
    let ctx = AppContext::builder()
        .server_url("ws://127.0.0.1:1/".to_string())
        .schema(schema)
        .storage(ClientStorage::Memory)
        .build();

    let result = JazzClient::connect(ctx).await;
    assert!(
        result.is_err(),
        "JazzClient::connect should return Err when the transport never comes up"
    );
}
```

Adapt `AppContext::builder()` / fields to the current public API (check `crates/jazz-tools/tests/support/mod.rs` for the exact constructor pattern used by the other tests).

- [ ] **Step 2: Run the test to verify it fails**

Run:

```
cargo test -p jazz-tools --test websocket_transport --features test,client,server \
    connect_returns_error_when_server_unreachable
```

Expected: FAIL (returns `Ok`).

- [ ] **Step 3: Expose `is_transport_connected()` on `TokioRuntime`**

In `crates/jazz-tools/src/runtime_tokio.rs`, find `transport_ever_connected` (used by `wait_until_transport_connected`). Add a sibling method:

```rust
    /// Returns true iff the transport has completed its first auth handshake.
    /// Mirrors `TransportHandle::has_ever_connected()`.
    pub fn is_transport_connected(&self) -> bool {
        self.transport_ever_connected().unwrap_or(false)
    }
```

(If `transport_ever_connected` is already public and does the same thing, just reuse it directly and skip the wrapper — check the current signature.)

- [ ] **Step 4: Honor the bool in `JazzClient::connect`**

In `crates/jazz-tools/src/client.rs:186-188`, replace:

```rust
            let _ = runtime
                .wait_until_transport_connected(Duration::from_secs(10))
                .await;
```

with:

```rust
            let connected = runtime
                .wait_until_transport_connected(Duration::from_secs(10))
                .await
                .map_err(|e| JazzError::Connection(format!("runtime error: {e}")))?;
            if !connected {
                return Err(JazzError::Connection(
                    "transport did not come up within 10s".into(),
                ));
            }
```

- [ ] **Step 5: Rewire `is_connected()` to read live state**

Remove the `has_server_url: bool` field (line 50) and its initialization (lines 162-163, 195). Replace the `is_connected` impl (lines 376-378):

```rust
    /// Check if connected to server.
    pub fn is_connected(&self) -> bool {
        self.has_server_url
    }
```

with:

```rust
    /// True iff the transport has completed an auth handshake. Flips back
    /// to false after `disconnect()` clears the transport.
    pub fn is_connected(&self) -> bool {
        self.runtime.is_transport_connected()
    }
```

Delete every remaining reference to `has_server_url` in `client.rs` — the compiler will guide you. If any non-transport code branches on it (unlikely based on the review), hoist the check to the relevant site using `!context.server_url.is_empty()`.

- [ ] **Step 6: Expose `isTransportConnected()` on NAPI**

In `crates/jazz-napi/src/lib.rs`, find the existing `take_auth_failure` NAPI method. Next to it, add:

```rust
    #[napi]
    pub fn is_transport_connected(&self) -> bool {
        self.runtime.is_transport_connected()
    }
```

And in `crates/jazz-napi/index.d.ts`, add the declaration next to `takeAuthFailure()`:

```ts
isTransportConnected(): boolean
```

- [ ] **Step 7: Expose on WASM**

In `crates/jazz-wasm/src/runtime.rs`, next to the WASM binding for `take_auth_failure`, add:

```rust
    #[wasm_bindgen(js_name = "isTransportConnected")]
    pub fn is_transport_connected(&self) -> bool {
        self.runtime.is_transport_connected()
    }
```

(Match the existing `#[wasm_bindgen(...)]` attribute style from the auth-failure method.)

- [ ] **Step 8: Expose on UniFFI (RN)**

In `crates/jazz-rn/rust/src/lib.rs`, next to the UniFFI binding for `take_auth_failure`, add:

```rust
    pub fn is_transport_connected(&self) -> bool {
        self.runtime.is_transport_connected()
    }
```

If the UniFFI export requires the method to be in a `#[uniffi::export]` block matching the rest, follow the convention used by `take_auth_failure` in that file.

- [ ] **Step 9: Regenerate UniFFI bindings**

Run the RN binding regeneration command used by the repo — check `crates/jazz-rn/package.json` or `justfile` / `Makefile` for the exact command. Typical options:

```
pnpm --filter jazz-rn run generate
```

or

```
cargo run -p jazz-rn --bin uniffi-bindgen
```

Commit the regenerated `crates/jazz-rn/src/generated/*.ts` files together with the Rust change.

- [ ] **Step 10: Add `isTransportConnected?()` to the TS `Runtime` interface**

In `packages/jazz-tools/src/runtime/client.ts` near the existing `takeAuthFailure?(): string | null;` (around line 116), add:

```ts
  isTransportConnected?(): boolean;
```

- [ ] **Step 11: Run tests to verify the failing integration test now passes**

Run:

```
cargo test -p jazz-tools --test websocket_transport --features test,client,server
cargo build -p jazz-napi
cargo build -p jazz-wasm --target wasm32-unknown-unknown
cargo build -p jazz-rn
pnpm --filter jazz-tools build
```

All green. The `connect_returns_error_when_server_unreachable` test should now pass (JazzClient returns `Err` after 10 s).

- [ ] **Step 12: Commit**

```
git add crates/jazz-tools/src/client.rs crates/jazz-tools/src/runtime_tokio.rs \
        crates/jazz-napi/src/lib.rs crates/jazz-napi/index.d.ts \
        crates/jazz-wasm/src/runtime.rs crates/jazz-rn/rust/src/lib.rs \
        crates/jazz-rn/src/generated/ \
        packages/jazz-tools/src/runtime/client.ts \
        crates/jazz-tools/tests/websocket_transport.rs
git commit -m "fix(transport): JazzClient.connect returns Err on unreachable server; is_connected reads live state"
```

---

## Task 8: `asBackend()` and `updateAuthToken()` must reach the live transport

**Files:**

- Modify: `packages/jazz-tools/src/runtime/client.ts:831-848`.
- Modify: `packages/jazz-tools/src/runtime/client.ts:1489-1509` (make `setupSync` callable multiple times).
- Add test: `packages/jazz-tools/src/runtime/napi.integration.test.ts` (or `sync-transport.lazy-client-id.test.ts` if that's the closer home).

`asBackend()` only sets an in-memory flag; `updateAuthToken()` only writes the new token into `this.context.jwtToken`. Neither disconnects and reconnects the live WS. The live handshake keeps using the old auth until the whole JazzClient is recreated.

- [ ] **Step 1: Write a failing integration test**

In `packages/jazz-tools/src/runtime/napi.integration.test.ts`, add a test that:

1. Starts a test server.
2. Creates a `JazzClient` with an initial JWT.
3. Calls `client.updateAuthToken(newJwt)`.
4. Verifies the live WS connection reconnected with the new token (e.g., by observing a `connect` event on the server, or by asserting `client.isConnected()` transitioned false → true).

Sketch (adapt to the harness conventions used elsewhere in that file):

```ts
it("updateAuthToken() reconnects the live transport with the new credentials", async () => {
  const { server, serverUrl } = await startTestServer();
  const client = await createNapiClient({ serverUrl, jwtToken: "initial" });
  const initialConns = server.countConnections();

  client.updateAuthToken("refreshed");

  await vi.waitFor(() => {
    expect(server.countConnections()).toBeGreaterThan(initialConns);
  });
  const latest = server.latestConnectionAuth();
  expect(latest.jwtToken).toBe("refreshed");

  await client.shutdown();
});
```

If the test harness has no `countConnections` / `latestConnectionAuth`, extend it in the same commit — they are thin helpers around `TestingServer`'s existing connection log. Keep the extension minimal.

- [ ] **Step 2: Run the test to verify it fails**

Run:

```
pnpm --filter jazz-tools test napi.integration.test.ts -t "updateAuthToken"
```

Expected: FAIL — connection count stays flat; latest auth still `"initial"`.

- [ ] **Step 3: Make `setupSync` safe to call multiple times**

At `packages/jazz-tools/src/runtime/client.ts:1489`, update `setupSync`:

```ts
  private setupSync(serverUrl: string, serverPathPrefix?: string): void {
    const wsUrl = buildWsUrl(serverUrl, serverPathPrefix);
    const authJson = this.buildWsAuthJson();
    this.runtime.connect!(wsUrl, authJson);
  }
```

Replace with:

```ts
  private setupSync(serverUrl: string, serverPathPrefix?: string): void {
    // Safe to call repeatedly: disconnect tears down any in-flight transport
    // so the Rust side can start a fresh one with the current auth.
    if (this.runtime.disconnect) {
      this.runtime.disconnect();
    }
    const wsUrl = buildWsUrl(serverUrl, serverPathPrefix);
    const authJson = this.buildWsAuthJson();
    this.runtime.connect!(wsUrl, authJson);
  }
```

- [ ] **Step 4: Wire `asBackend()` and `updateAuthToken()` to re-setup sync**

Replace `asBackend()` (lines 831-840):

```ts
  asBackend(): JazzClient {
    if (!this.context.backendSecret) {
      throw new Error("backendSecret required for backend mode");
    }
    if (!this.context.serverUrl) {
      throw new Error("serverUrl required for backend mode");
    }
    this.useBackendSyncAuth = true;
    return this;
  }
```

with:

```ts
  asBackend(): JazzClient {
    if (!this.context.backendSecret) {
      throw new Error("backendSecret required for backend mode");
    }
    if (!this.context.serverUrl) {
      throw new Error("serverUrl required for backend mode");
    }
    this.useBackendSyncAuth = true;
    // Re-handshake with backend credentials so the live transport actually
    // uses the backend secret instead of end-user JWT.
    if (this.context.serverUrl) {
      this.setupSync(this.context.serverUrl, this.context.serverPathPrefix);
    }
    return this;
  }
```

Replace `updateAuthToken()` (lines 842-848):

```ts
  updateAuthToken(jwtToken?: string): void {
    this.context.jwtToken = jwtToken;
    this.resolvedSession = resolveClientSessionStateSync({
      appId: this.context.appId,
      jwtToken,
    }).session;
  }
```

with:

```ts
  updateAuthToken(jwtToken?: string): void {
    this.context.jwtToken = jwtToken;
    this.resolvedSession = resolveClientSessionStateSync({
      appId: this.context.appId,
      jwtToken,
    }).session;
    // Re-handshake so the live transport picks up the refreshed token.
    // No-op when no server is configured (local-only client).
    if (this.context.serverUrl) {
      this.setupSync(this.context.serverUrl, this.context.serverPathPrefix);
    }
  }
```

(Adjust the `serverPathPrefix` field access to whatever `JazzClient` actually stores — check the constructor for the exact name.)

- [ ] **Step 5: Run the test to verify it passes**

Run:

```
pnpm --filter jazz-tools test napi.integration.test.ts -t "updateAuthToken"
```

Expected: PASS. Also run the wider suite to confirm no regressions:

```
pnpm --filter jazz-tools test
```

- [ ] **Step 6: Commit**

```
git add packages/jazz-tools/src/runtime/client.ts \
        packages/jazz-tools/src/runtime/napi.integration.test.ts \
        packages/jazz-tools/src/runtime/testing/napi-runtime-test-utils.ts
git commit -m "fix(transport): asBackend and updateAuthToken now re-handshake the live transport"
```

---

## Task 9: Proxy `takeAuthFailure` + `isTransportConnected` through the RN adapter

**Files:**

- Modify: `packages/jazz-tools/src/react-native/jazz-rn-runtime-adapter.ts:18-79,480-501`.
- Add test: alongside the existing RN adapter tests, or a unit test in the same file's `__tests__` neighbor.

The Rust/UniFFI layer already exposes `takeAuthFailure()` (commit `b627a9f2`) and — after Task 7 — `isTransportConnected()`. But `JazzRnRuntimeBinding` and `JazzRnRuntimeAdapter` don't declare or proxy them. The client-side poller (`client.ts:646`) short-circuits on `!this.runtime.takeAuthFailure`, so on RN every 401/403 is silently dropped today.

- [ ] **Step 1: Write a failing test**

If there is no existing RN adapter unit test file, create `packages/jazz-tools/src/react-native/jazz-rn-runtime-adapter.test.ts`:

```ts
import { describe, expect, it, vi } from "vitest";

import { JazzRnRuntimeAdapter, type JazzRnRuntimeBinding } from "./jazz-rn-runtime-adapter.js";

function bindingStub(overrides: Partial<JazzRnRuntimeBinding> = {}): JazzRnRuntimeBinding {
  return {
    addClient: () => "client-id",
    addServer: vi.fn(),
    batchedTick: vi.fn(),
    close: vi.fn(),
    delete_: vi.fn(),
    flush: vi.fn(),
    getSchemaHash: () => "hash",
    insert: () => "row-id",
    onBatchedTickNeeded: vi.fn(),
    onSyncMessageReceived: vi.fn(),
    onSyncMessageReceivedFromClient: vi.fn(),
    onSyncMessageToSend: vi.fn(),
    query: () => "[]",
    removeServer: vi.fn(),
    setClientRole: vi.fn(),
    createSubscription: () => 1n,
    executeSubscription: vi.fn(),
    subscribe: () => 1n,
    unsubscribe: vi.fn(),
    update: vi.fn(),
    ...overrides,
  };
}

describe("JazzRnRuntimeAdapter", () => {
  it("proxies takeAuthFailure to the binding", () => {
    const takeAuthFailure = vi.fn().mockReturnValue("expired");
    const adapter = new JazzRnRuntimeAdapter(bindingStub({ takeAuthFailure }), {} as any);
    expect(adapter.takeAuthFailure()).toBe("expired");
    expect(takeAuthFailure).toHaveBeenCalledTimes(1);
  });

  it("returns null when the binding does not expose takeAuthFailure", () => {
    const adapter = new JazzRnRuntimeAdapter(bindingStub(), {} as any);
    expect(adapter.takeAuthFailure()).toBeNull();
  });

  it("proxies isTransportConnected to the binding", () => {
    const isTransportConnected = vi.fn().mockReturnValue(true);
    const adapter = new JazzRnRuntimeAdapter(bindingStub({ isTransportConnected }), {} as any);
    expect(adapter.isTransportConnected()).toBe(true);
    expect(isTransportConnected).toHaveBeenCalledTimes(1);
  });

  it("returns false when the binding does not expose isTransportConnected", () => {
    const adapter = new JazzRnRuntimeAdapter(bindingStub(), {} as any);
    expect(adapter.isTransportConnected()).toBe(false);
  });
});
```

- [ ] **Step 2: Run the test to verify it fails**

Run:

```
pnpm --filter jazz-tools test jazz-rn-runtime-adapter.test.ts
```

Expected: fails to compile (`Property 'takeAuthFailure' does not exist`).

- [ ] **Step 3: Extend the binding interface and adapter**

In `packages/jazz-tools/src/react-native/jazz-rn-runtime-adapter.ts`, inside `JazzRnRuntimeBinding` (lines 18-79), add after `disconnect?()`:

```ts
  takeAuthFailure?(): string | null;
  isTransportConnected?(): boolean;
```

Then inside `JazzRnRuntimeAdapter` (near lines 480-486), add:

```ts
  takeAuthFailure(): string | null {
    return this.binding.takeAuthFailure?.() ?? null;
  }

  isTransportConnected(): boolean {
    return this.binding.isTransportConnected?.() ?? false;
  }
```

- [ ] **Step 4: Run the test to verify it passes**

Run:

```
pnpm --filter jazz-tools test jazz-rn-runtime-adapter.test.ts
```

Expected: PASS.

Also verify typecheck:

```
pnpm --filter jazz-tools build
```

- [ ] **Step 5: Commit**

```
git add packages/jazz-tools/src/react-native/jazz-rn-runtime-adapter.ts \
        packages/jazz-tools/src/react-native/jazz-rn-runtime-adapter.test.ts
git commit -m "fix(transport): proxy takeAuthFailure and isTransportConnected through the RN adapter"
```

---

## Task 10: Unblock worker-bridged edge/global queries

**Files:**

- Modify: `packages/jazz-tools/src/worker/jazz-worker.ts` (emit `upstream-connected` / `upstream-disconnected`).
- (Depends on Task 7 having exposed `isTransportConnected()` in WASM.)
- Add test: `packages/jazz-tools/src/runtime/cloud-server.integration.test.ts` — re-enable or add a worker-bridged edge query that completes.

`Db.ensureQueryReady()` still awaits `workerBridge.waitForUpstreamServerConnection()`, but the worker never emits `upstream-connected`/`upstream-disconnected` after the transport rewrite. Any edge/global tier query hangs forever in the worker-bridged setup. Fix: the worker polls `runtime.isTransportConnected()` and emits the state-change messages.

- [ ] **Step 1: Write a failing integration test**

In `packages/jazz-tools/src/runtime/cloud-server.integration.test.ts`, add or re-enable a test that runs an edge-tier query through a worker-bridged client:

```ts
it("resolves edge-tier queries after the worker transport connects", async () => {
  const { serverUrl, shutdown } = await startCloudServer();

  const db = await createWorkerBridgedDb({ serverUrl });
  const rows = await db.query({ table: "todos" }, { tier: "edge" });
  expect(Array.isArray(rows)).toBe(true);

  await db.shutdown();
  await shutdown();
});
```

Adapt `createWorkerBridgedDb` to the harness used elsewhere in this file. If no helper exists, use the same `Db` / `WorkerBridge` wiring used in `worker-bridge.race-harness.test.ts` as a template — but talk to a real worker, not the harness fake.

- [ ] **Step 2: Run to verify it fails**

Run:

```
pnpm --filter jazz-tools test cloud-server.integration.test.ts -t "edge-tier queries after the worker transport connects"
```

Expected: FAIL — test times out because `ensureQueryReady` never resolves.

- [ ] **Step 3: Emit `upstream-connected` from the worker when transport comes up**

In `packages/jazz-tools/src/worker/jazz-worker.ts`, find `startAuthFailurePoller` (around line 340) and the `setInterval(..., 500)` loop inside it.

Add a companion state-tracking variable at module scope near `authFailurePoller`:

```ts
let lastReportedUpstreamConnected = false;
```

Inside the existing poller's tick callback, after the auth-failure check, add:

```ts
function pollTransportState(runtime: any): void {
  if (typeof runtime?.isTransportConnected !== "function") return;
  const connected = !!runtime.isTransportConnected();
  if (connected !== lastReportedUpstreamConnected) {
    lastReportedUpstreamConnected = connected;
    post(connected ? { type: "upstream-connected" } : { type: "upstream-disconnected" });
  }
}
```

And call it from the existing interval body:

```ts
authFailurePoller = setInterval(() => {
  if (!runtime) return;
  pollTransportState(runtime);
  const reason = runtime.takeAuthFailure?.();
  if (reason) {
    post({ type: "auth-failed", reason });
  }
}, 500);
```

(Merge into the existing interval body — do not create a second `setInterval`.)

Reset `lastReportedUpstreamConnected = false` when the poller is stopped (wherever the worker tears down, usually on `shutdown` or in the `close()` path of the existing poller).

- [ ] **Step 4: Run the integration test to verify it now passes**

Run:

```
pnpm --filter jazz-tools test cloud-server.integration.test.ts -t "edge-tier queries after the worker transport connects"
```

Expected: PASS.

- [ ] **Step 5: Sanity-check the non-worker path**

Run the full test suite:

```
pnpm --filter jazz-tools test
cargo test -p jazz-tools --features client,server --test websocket_transport
```

All green.

- [ ] **Step 6: Commit**

```
git add packages/jazz-tools/src/worker/jazz-worker.ts \
        packages/jazz-tools/src/runtime/cloud-server.integration.test.ts
git commit -m "fix(transport): worker emits upstream-connected/disconnected so bridged edge queries unblock"
```

---

## Final verification

- [ ] **Run the full matrix once end-to-end**

```
cargo test -p jazz-tools --features client,server
cargo test -p jazz-tools --features test,client,server --test websocket_transport
cargo build -p jazz-napi
cargo build -p jazz-rn
cargo build -p jazz-wasm --target wasm32-unknown-unknown
pnpm --filter jazz-tools build
pnpm --filter jazz-tools test
```

All green.

- [ ] **Skim the commit log**

```
git log --oneline main..HEAD
```

Expected: 10 new commits, one per task, each with a `fix(transport):` or `test(transport):` prefix. None contain Claude attribution.

- [ ] **Update the review status**

Leave a note in `todo/projects/transport/pitch.md` (or the equivalent tracking file) listing the 10 findings this plan closed, so the next plan (hardening) starts from an accurate baseline.

---

## Out of scope (do NOT do in this plan)

These are real issues but live in follow-up plans — touching them here widens the diff and risks breaking the critical-fix landings:

- Outbox batching / payload clone elimination (P1, P2, P3).
- Typed `code` on `ServerEvent::Error` to replace English string matching (B14 / P8 / S3).
- `ReconnectState::reset()` not called after successful handshake (B3 / S16).
- 500 ms `setInterval` polling replaced with push-based callback (P5 / B7 / S13) — structurally the same as today, only the underlying cadence changes.
- Server batch `for payload in batch.payloads { push_sync_inbox(...) }` N-lock pattern (P10).
- `wait_until_transport_connected` 25 ms poll loop → `Notify` (P17 / S20).
- Heartbeat `MissedTickBehavior::Delay` (P9).
- Redundant 4-byte length prefix inside WS binary frame (P19 / B12).
- All simplicity-track cleanups (S4–S12, S17–S30).
- Spec / plan doc drift (D3–D9).

Defer to `ws-transport-hardening.md` and `ws-transport-cleanup.md`.
