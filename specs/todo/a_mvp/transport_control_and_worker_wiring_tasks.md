# Transport Control Channel and Worker Wiring — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix three bugs in the JS worker ↔ Rust transport bridge (update-auth no-op, reconnect-loop leak on early disconnect, missing `/ws` normalization in worker init) by introducing a unified `TransportControl` channel and tightening worker-side URL + auth plumbing.

**Architecture:** A new `TransportControl` enum (`Shutdown`, `UpdateAuth(AuthConfig)`) flows from `TransportHandle` to `TransportManager` over a dedicated `mpsc::UnboundedSender/Receiver`. `TransportManager::run` races every phase (Connect, Handshake, Connected, Backoff) against `control_rx.next()` using `tokio::select!` (and `futures::select!` on WASM). `self.auth` is mutated only via `UpdateAuth`. Bindings (NAPI/WASM/RN) expose one new `update_auth(auth_json)` method. The JS worker caches a full auth blob and re-sends it on every refresh. `httpUrlToWs` moves to a shared module and accepts an optional path prefix.

**Tech Stack:** Rust (`jazz-tools`, `jazz-napi`, `jazz-wasm`, `jazz-rn`), `tokio` 1.x, `futures` 0.3, `wasm-bindgen`, UniFFI, TypeScript 5 / Vitest, pnpm + turbo.

**Spec:** `specs/todo/a_mvp/transport_control_and_worker_wiring.md`.

---

## File Structure

### Modified

- `crates/jazz-tools/src/transport_manager.rs` — add `TransportControl`, extend handle/manager, rewrite `run()` (Tokio + WASM) as phase machine, extend test module with `TestStreamAdapter` and new scenarios.
- `crates/jazz-tools/src/runtime_core.rs` — add `transport()` accessor.
- `crates/jazz-napi/src/lib.rs` — modify `disconnect`, add `update_auth`.
- `crates/jazz-napi/index.d.ts` — add `updateAuth(authJson: string): void`.
- `crates/jazz-wasm/src/runtime.rs` — modify `disconnect`, add `update_auth`.
- `crates/jazz-rn/rust/src/lib.rs` — modify `disconnect`, add `update_auth`.
- `crates/jazz-rn/src/generated/jazz_rn.ts` — regenerated from UniFFI.
- `packages/jazz-tools/src/runtime/client.ts` — remove local `httpUrlToWs`, import from `./url.js`.
- `packages/jazz-tools/src/worker/jazz-worker.ts` — cache `currentAuth`, use shared `httpUrlToWs(serverUrl, serverPathPrefix)`, wire `update-auth` to `runtime.updateAuth(...)`.

### Created

- `packages/jazz-tools/src/runtime/url.ts` — shared `httpUrlToWs(serverUrl, pathPrefix?)`.
- `packages/jazz-tools/src/runtime/url.test.ts` — unit tests for the helper.
- `packages/jazz-tools/src/worker/jazz-worker.test.ts` — integration tests for URL wiring and `update-auth` dispatch (only if no equivalent exists; extend existing harness otherwise).

---

## Phase 0 — Scaffolding

### Task 1: Add `TransportControl` enum and channel

**Files:**

- Modify: `crates/jazz-tools/src/transport_manager.rs`

- [ ] **Step 1: Add the enum and extend structs**

Add near the existing `TransportInbound` definition:

```rust
#[derive(Debug)]
pub enum TransportControl {
    Shutdown,
    UpdateAuth(AuthConfig),
}
```

Extend `TransportHandle` (around line 37):

```rust
#[derive(Debug)]
pub struct TransportHandle {
    pub server_id: ServerId,
    pub client_id: ClientId,
    pub outbox_tx: mpsc::UnboundedSender<OutboxEntry>,
    pub inbound_rx: mpsc::UnboundedReceiver<TransportInbound>,
    pub ever_connected: std::sync::Arc<std::sync::atomic::AtomicBool>,
    pub control_tx: mpsc::UnboundedSender<TransportControl>,
}
```

Extend `TransportManager` (around line 148):

```rust
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
    control_rx: mpsc::UnboundedReceiver<TransportControl>,
    _stream: std::marker::PhantomData<W>,
}
```

Wire both halves in `create()` (around line 161):

```rust
let (outbox_tx, outbox_rx) = mpsc::unbounded();
let (inbound_tx, inbound_rx) = mpsc::unbounded();
let (control_tx, control_rx) = mpsc::unbounded();
let ever_connected = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
let handle = TransportHandle {
    server_id,
    client_id,
    outbox_tx,
    inbound_rx,
    ever_connected: ever_connected.clone(),
    control_tx,
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
    control_rx,
    _stream: std::marker::PhantomData,
};
```

- [ ] **Step 2: Add handle methods**

Inside `impl TransportHandle` (around line 45):

```rust
pub fn disconnect(&self) {
    let _ = self.control_tx.unbounded_send(TransportControl::Shutdown);
}

pub fn update_auth(&self, auth: AuthConfig) {
    let _ = self.control_tx.unbounded_send(TransportControl::UpdateAuth(auth));
}
```

- [ ] **Step 3: Verify the crate still builds**

Run: `cargo build -p jazz-tools`
Expected: clean build. The existing `run()` still compiles because it simply never reads `control_rx` (unused field warnings are acceptable — silence with `#[allow(dead_code)]` on the field for now; will be read in Task 3).

- [ ] **Step 4: Commit**

```bash
git add crates/jazz-tools/src/transport_manager.rs
git commit -m "feat(jazz-tools): add TransportControl enum and control channel scaffolding"
```

---

## Phase 1 — Tokio state machine

All remaining Rust tests in this phase live in `crates/jazz-tools/src/transport_manager.rs::tests` (Tokio-gated).

### Task 2: Introduce `TestStreamAdapter`

**Files:**

- Modify: `crates/jazz-tools/src/transport_manager.rs` (tests module near line 455)

- [ ] **Step 1: Add a gated test adapter**

Alongside the existing `MockStream`, introduce a `TestStreamAdapter` whose behavior is driven by a shared `TestStreamController`:

```rust
#[derive(Default)]
struct TestStreamController {
    // When true, `connect` pends forever (never resolves). When false, connect succeeds.
    pub connect_pending: std::sync::atomic::AtomicBool,
    // When true, `recv` inside run_connected/handshake pends forever.
    pub recv_pending: std::sync::atomic::AtomicBool,
    // Pre-loaded handshake response frame (optional).
    pub handshake_response: std::sync::Mutex<Option<Vec<u8>>>,
    // Frames to deliver via `recv` after handshake; when drained, behavior follows `recv_pending`.
    pub recv_queue: std::sync::Mutex<VecDeque<Vec<u8>>>,
    // Observability.
    pub close_calls: std::sync::atomic::AtomicUsize,
    pub connect_calls: std::sync::atomic::AtomicUsize,
}

struct TestStreamAdapter {
    controller: Arc<TestStreamController>,
    handshake_delivered: bool,
}

thread_local! {
    static TEST_CONTROLLER: std::cell::RefCell<Option<Arc<TestStreamController>>> =
        std::cell::RefCell::new(None);
}

fn install_controller(c: Arc<TestStreamController>) {
    TEST_CONTROLLER.with(|slot| *slot.borrow_mut() = Some(c));
}

fn take_controller() -> Arc<TestStreamController> {
    TEST_CONTROLLER.with(|slot| slot.borrow().clone()).expect("controller installed")
}

impl StreamAdapter for TestStreamAdapter {
    type Error = &'static str;

    async fn connect(_url: &str) -> Result<Self, Self::Error> {
        let controller = take_controller();
        controller.connect_calls.fetch_add(1, Ordering::SeqCst);
        if controller.connect_pending.load(Ordering::SeqCst) {
            // Pend forever.
            futures::future::pending::<()>().await;
            unreachable!();
        }
        Ok(Self { controller, handshake_delivered: false })
    }

    async fn send(&mut self, _data: &[u8]) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn recv(&mut self) -> Result<Option<Vec<u8>>, Self::Error> {
        // First call delivers the handshake response if one is staged.
        if !self.handshake_delivered {
            if let Some(frame) = self.controller.handshake_response.lock().unwrap().clone() {
                self.handshake_delivered = true;
                return Ok(Some(frame));
            }
        }
        if let Some(frame) = self.controller.recv_queue.lock().unwrap().pop_front() {
            return Ok(Some(frame));
        }
        if self.controller.recv_pending.load(Ordering::SeqCst) {
            futures::future::pending::<Option<Vec<u8>>>().await;
            unreachable!();
        }
        Ok(None)
    }

    async fn close(&mut self) {
        self.controller.close_calls.fetch_add(1, Ordering::SeqCst);
    }
}

fn make_handshake_response_frame() -> Vec<u8> {
    let resp = ConnectedResponse {
        connection_id: "conn-1".into(),
        client_id: "client-1".into(),
        next_sync_seq: Some(0),
        catalogue_state_hash: None,
    };
    let payload = serde_json::to_vec(&resp).unwrap();
    frame_encode(&payload)
}
```

- [ ] **Step 2: Verify the crate still builds**

Run: `cargo build -p jazz-tools --tests`
Expected: clean build.

- [ ] **Step 3: Commit**

```bash
git add crates/jazz-tools/src/transport_manager.rs
git commit -m "test(jazz-tools): add TestStreamAdapter scaffolding for transport_manager tests"
```

---

### Task 3: `shutdown_during_connect`

**Files:**

- Modify: `crates/jazz-tools/src/transport_manager.rs`

- [ ] **Step 1: Write the failing test**

Append to the tests module:

```rust
#[tokio::test]
async fn shutdown_during_connect() {
    let controller = Arc::new(TestStreamController::default());
    controller.connect_pending.store(true, Ordering::SeqCst);
    install_controller(controller.clone());

    let counter = Arc::new(AtomicUsize::new(0));
    let (handle, manager) = create::<TestStreamAdapter, CountingTick>(
        "mock://".to_string(),
        AuthConfig::default(),
        CountingTick(counter.clone()),
    );
    let task = tokio::spawn(manager.run());

    // Let connect() start.
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    assert!(controller.connect_calls.load(Ordering::SeqCst) >= 1);

    handle.disconnect();

    tokio::time::timeout(std::time::Duration::from_millis(200), task)
        .await
        .expect("manager should exit promptly after Shutdown during connect")
        .unwrap();
}
```

- [ ] **Step 2: Verify it fails**

Run: `cargo test -p jazz-tools shutdown_during_connect -- --nocapture`
Expected: timeout expires → test panics with "manager should exit promptly…".

- [ ] **Step 3: Implement control-aware connect phase (Tokio)**

Replace the Tokio `run()` body (around line 230) so the outer loop races each phase against `control_rx`:

```rust
pub async fn run(mut self) {
    use futures::StreamExt as _;
    loop {
        // Phase: Connect.
        let connect_outcome = tokio::select! {
            biased;
            ctrl = self.control_rx.next() => ControlOrPhase::Control(ctrl),
            res = W::connect(&self.url) => ControlOrPhase::Phase(res),
        };
        let ws = match connect_outcome {
            ControlOrPhase::Control(None) | ControlOrPhase::Control(Some(TransportControl::Shutdown)) => return,
            ControlOrPhase::Control(Some(TransportControl::UpdateAuth(auth))) => {
                self.auth = auth;
                self.reconnect.reset();
                continue;
            }
            ControlOrPhase::Phase(Ok(ws)) => ws,
            ControlOrPhase::Phase(Err(e)) => {
                tracing::warn!("ws connect failed: {e}");
                self.reconnect.backoff().await; // temporarily; control check added in Task 4.
                continue;
            }
        };

        // Handshake + Connected phases: implemented in Tasks 5-8. For now, keep existing logic.
        let mut ws = ws;
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
                    ConnectedExit::HandleDropped => { ws.close().await; return; }
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
        self.reconnect.backoff().await;
    }
}

enum ControlOrPhase<T> {
    Control(Option<TransportControl>),
    Phase(T),
}
```

Add `ControlOrPhase` near `ConnectedExit`. Keep it crate-private.

- [ ] **Step 4: Verify it passes**

Run: `cargo test -p jazz-tools shutdown_during_connect`
Expected: PASS.

Also: `cargo test -p jazz-tools handshake_marks_ever_connected_and_notifies_tick` — still green.

- [ ] **Step 5: Commit**

```bash
git add crates/jazz-tools/src/transport_manager.rs
git commit -m "feat(jazz-tools): wire control channel into TransportManager connect phase"
```

---

### Task 4: `shutdown_during_backoff`

**Files:**

- Modify: `crates/jazz-tools/src/transport_manager.rs`

- [ ] **Step 1: Write the failing test**

```rust
#[tokio::test]
async fn shutdown_during_backoff() {
    let controller = Arc::new(TestStreamController::default());
    // Make connect fail immediately — drives the loop into backoff.
    // TestStreamAdapter currently returns Ok; add a "connect_error" flag to the controller
    // if not already present. For minimal change, set handshake_response = None and let the
    // handshake fail (returns Err via None → "server closed before handshake response"),
    // which routes into backoff after close.
    install_controller(controller.clone());

    let counter = Arc::new(AtomicUsize::new(0));
    let (handle, manager) = create::<TestStreamAdapter, CountingTick>(
        "mock://".to_string(),
        AuthConfig::default(),
        CountingTick(counter.clone()),
    );
    let task = tokio::spawn(manager.run());

    // Wait for at least one failed connect/handshake cycle to enter backoff.
    tokio::time::sleep(std::time::Duration::from_millis(30)).await;

    handle.disconnect();

    tokio::time::timeout(std::time::Duration::from_millis(200), task)
        .await
        .expect("manager should exit during backoff on Shutdown")
        .unwrap();
}
```

- [ ] **Step 2: Verify it fails**

Run: `cargo test -p jazz-tools shutdown_during_backoff`
Expected: timeout panic — `backoff().await` is not control-aware.

- [ ] **Step 3: Implement control-aware backoff**

Replace both sites that call `self.reconnect.backoff().await` in the Tokio `run()` body with:

```rust
let backoff_outcome = tokio::select! {
    biased;
    ctrl = self.control_rx.next() => ControlOrPhase::Control(ctrl),
    _ = self.reconnect.backoff() => ControlOrPhase::Phase(()),
};
match backoff_outcome {
    ControlOrPhase::Control(None) | ControlOrPhase::Control(Some(TransportControl::Shutdown)) => return,
    ControlOrPhase::Control(Some(TransportControl::UpdateAuth(auth))) => {
        self.auth = auth;
        self.reconnect.reset();
        continue;
    }
    ControlOrPhase::Phase(()) => {}
}
```

- [ ] **Step 4: Verify it passes**

Run: `cargo test -p jazz-tools shutdown_during_backoff`
Expected: PASS. Also rerun prior tests — green.

- [ ] **Step 5: Commit**

```bash
git add crates/jazz-tools/src/transport_manager.rs
git commit -m "feat(jazz-tools): control channel observable during backoff"
```

---

### Task 5: `shutdown_during_handshake`

**Files:**

- Modify: `crates/jazz-tools/src/transport_manager.rs`

- [ ] **Step 1: Write the failing test**

```rust
#[tokio::test]
async fn shutdown_during_handshake() {
    let controller = Arc::new(TestStreamController::default());
    // connect succeeds; handshake pends (recv returns Pending forever until controller flips).
    controller.recv_pending.store(true, Ordering::SeqCst);
    // Do NOT pre-stage a handshake response — recv will see the queue empty and pend.
    install_controller(controller.clone());

    let counter = Arc::new(AtomicUsize::new(0));
    let (handle, manager) = create::<TestStreamAdapter, CountingTick>(
        "mock://".to_string(),
        AuthConfig::default(),
        CountingTick(counter.clone()),
    );
    let task = tokio::spawn(manager.run());

    tokio::time::sleep(std::time::Duration::from_millis(30)).await;
    handle.disconnect();

    tokio::time::timeout(std::time::Duration::from_millis(200), task)
        .await
        .expect("manager should exit during handshake on Shutdown")
        .unwrap();
    assert!(controller.close_calls.load(Ordering::SeqCst) >= 1);
}
```

- [ ] **Step 2: Verify it fails**

Run: `cargo test -p jazz-tools shutdown_during_handshake`
Expected: timeout.

- [ ] **Step 3: Implement control-aware handshake**

Wrap the `perform_auth_handshake` call:

```rust
let handshake_outcome = tokio::select! {
    biased;
    ctrl = self.control_rx.next() => ControlOrPhase::Control(ctrl),
    res = self.perform_auth_handshake(&mut ws) => ControlOrPhase::Phase(res),
};
match handshake_outcome {
    ControlOrPhase::Control(None) | ControlOrPhase::Control(Some(TransportControl::Shutdown)) => {
        ws.close().await;
        return;
    }
    ControlOrPhase::Control(Some(TransportControl::UpdateAuth(auth))) => {
        self.auth = auth;
        ws.close().await;
        self.reconnect.reset();
        continue;
    }
    ControlOrPhase::Phase(Ok(resp)) => {
        self.ever_connected.store(true, std::sync::atomic::Ordering::Release);
        let _ = self.inbound_tx.unbounded_send(TransportInbound::Connected {
            catalogue_state_hash: resp.catalogue_state_hash,
            next_sync_seq: resp.next_sync_seq,
        });
        self.tick.notify();
        self.reconnect.reset();
        // Run-connected block (same as before).
        match self.run_connected(&mut ws).await {
            ConnectedExit::HandleDropped => { ws.close().await; return; }
            ConnectedExit::NetworkError => {
                let _ = self.inbound_tx.unbounded_send(TransportInbound::Disconnected);
                self.tick.notify();
                ws.close().await;
            }
        }
    }
    ControlOrPhase::Phase(Err(e)) => {
        tracing::warn!("ws auth handshake failed: {e}");
        ws.close().await;
    }
}
```

- [ ] **Step 4: Verify it passes**

Run: `cargo test -p jazz-tools shutdown_during_handshake`
Expected: PASS. Previous tests still green.

- [ ] **Step 5: Commit**

```bash
git add crates/jazz-tools/src/transport_manager.rs
git commit -m "feat(jazz-tools): control channel observable during handshake"
```

---

### Task 6: `shutdown_during_connected`

**Files:**

- Modify: `crates/jazz-tools/src/transport_manager.rs`

- [ ] **Step 1: Write the failing test**

```rust
#[tokio::test]
async fn shutdown_during_connected() {
    let controller = Arc::new(TestStreamController::default());
    *controller.handshake_response.lock().unwrap() = Some(make_handshake_response_frame());
    controller.recv_pending.store(true, Ordering::SeqCst);
    install_controller(controller.clone());

    let counter = Arc::new(AtomicUsize::new(0));
    let (mut handle, manager) = create::<TestStreamAdapter, CountingTick>(
        "mock://".to_string(),
        AuthConfig::default(),
        CountingTick(counter.clone()),
    );
    let task = tokio::spawn(manager.run());

    // Wait for Connected.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert!(handle.has_ever_connected());

    handle.disconnect();

    tokio::time::timeout(std::time::Duration::from_millis(200), task)
        .await
        .expect("manager should exit promptly after Shutdown while connected")
        .unwrap();
    // Shutdown does NOT emit Disconnected.
    let mut saw_disconnected = false;
    while let Some(msg) = handle.try_recv_inbound() {
        if matches!(msg, TransportInbound::Disconnected) { saw_disconnected = true; }
    }
    assert!(!saw_disconnected, "Shutdown must not emit Disconnected");
    assert!(controller.close_calls.load(Ordering::SeqCst) >= 1);
}
```

- [ ] **Step 2: Verify it fails**

Run: `cargo test -p jazz-tools shutdown_during_connected`
Expected: timeout — `run_connected` has no control branch.

- [ ] **Step 3: Extend `ConnectedExit` and add control branch**

Replace the `ConnectedExit` enum:

```rust
#[cfg(feature = "runtime-tokio")]
enum ConnectedExit {
    HandleDropped,
    NetworkError,
    Shutdown,
    UpdateAuth(AuthConfig),
}
```

In `run_connected`, add a control branch to the `tokio::select!`:

```rust
ctrl = self.control_rx.next() => {
    match ctrl {
        None | Some(TransportControl::Shutdown) => return ConnectedExit::Shutdown,
        Some(TransportControl::UpdateAuth(auth)) => return ConnectedExit::UpdateAuth(auth),
    }
}
```

Update the outer `match self.run_connected(&mut ws).await` in `run()`:

```rust
match self.run_connected(&mut ws).await {
    ConnectedExit::HandleDropped | ConnectedExit::Shutdown => {
        ws.close().await;
        return;
    }
    ConnectedExit::NetworkError => {
        let _ = self.inbound_tx.unbounded_send(TransportInbound::Disconnected);
        self.tick.notify();
        ws.close().await;
    }
    ConnectedExit::UpdateAuth(auth) => {
        // Implemented fully in Task 8; placeholder path exits-and-reconnects for now.
        self.auth = auth;
        let _ = self.inbound_tx.unbounded_send(TransportInbound::Disconnected);
        self.tick.notify();
        ws.close().await;
        self.reconnect.reset();
        continue;
    }
}
```

- [ ] **Step 4: Verify it passes**

Run: `cargo test -p jazz-tools shutdown_during_connected`
Expected: PASS. Previous tests still green.

- [ ] **Step 5: Commit**

```bash
git add crates/jazz-tools/src/transport_manager.rs
git commit -m "feat(jazz-tools): control channel observable during run_connected"
```

---

### Task 7: `update_auth_during_backoff`

**Files:**

- Modify: `crates/jazz-tools/src/transport_manager.rs`

- [ ] **Step 1: Write the failing test**

```rust
#[tokio::test]
async fn update_auth_during_backoff() {
    let controller = Arc::new(TestStreamController::default());
    // No handshake response → handshake fails → enter backoff.
    install_controller(controller.clone());

    let counter = Arc::new(AtomicUsize::new(0));
    let initial_auth = AuthConfig::default();
    let (handle, manager) = create::<TestStreamAdapter, CountingTick>(
        "mock://".to_string(),
        initial_auth,
        CountingTick(counter.clone()),
    );
    let task = tokio::spawn(manager.run());

    tokio::time::sleep(std::time::Duration::from_millis(40)).await;
    let calls_before = controller.connect_calls.load(Ordering::SeqCst);

    let mut new_auth = AuthConfig::default();
    new_auth.jwt_token = Some("refreshed".into());
    handle.update_auth(new_auth);

    // Manager should skip the remaining backoff and reconnect ~immediately.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let calls_after = controller.connect_calls.load(Ordering::SeqCst);
    assert!(
        calls_after > calls_before,
        "UpdateAuth during backoff should trigger immediate reconnect (before={calls_before}, after={calls_after})"
    );

    handle.disconnect();
    let _ = tokio::time::timeout(std::time::Duration::from_millis(200), task).await;
}
```

- [ ] **Step 2: Verify it fails (or is flaky)**

Run: `cargo test -p jazz-tools update_auth_during_backoff`
Expected: without UpdateAuth handling in backoff, the assertion may hold only because normal backoff (~300ms) expires slowly. Tighten by checking connect_calls increases within 50ms — this is before natural backoff elapses. If the test passes spuriously, reduce the sleep or increase the initial backoff attempts to push the interval higher. Acceptable: the `UpdateAuth` branch in Task 4's backoff `select!` already exists — but it must `self.auth = auth`. Verify by reading state: if `auth` isn't swapped, the new connect attempt would use the stale auth. Add an additional assertion that a reconnect attempt carries the new JWT (requires observing the handshake payload — extend `TestStreamAdapter::send` to capture frames).

If the assertion passes (because Task 4 already handled UpdateAuth correctly), skip to Step 4.

- [ ] **Step 3: Tighten the test by verifying the handshake auth payload**

Extend `TestStreamAdapter::send` to capture frames into `controller.sent_frames: Mutex<Vec<Vec<u8>>>`. In the test, after the reconnect:

```rust
let frames = controller.sent_frames.lock().unwrap().clone();
let handshake_frame = frames.iter().rev().find(|f| {
    let Some(payload) = frame_decode(f) else { return false };
    serde_json::from_slice::<AuthHandshake>(payload).is_ok()
}).expect("at least one handshake frame");
let payload = frame_decode(handshake_frame).unwrap();
let handshake: AuthHandshake = serde_json::from_slice(payload).unwrap();
assert_eq!(handshake.auth.jwt_token.as_deref(), Some("refreshed"));
```

- [ ] **Step 4: Verify the whole test passes**

Run: `cargo test -p jazz-tools update_auth_during_backoff`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/jazz-tools/src/transport_manager.rs
git commit -m "test(jazz-tools): verify UpdateAuth during backoff reconnects with new credentials"
```

---

### Task 8: `update_auth_during_connected`

**Files:**

- Modify: `crates/jazz-tools/src/transport_manager.rs`

- [ ] **Step 1: Write the failing test**

```rust
#[tokio::test]
async fn update_auth_during_connected() {
    let controller = Arc::new(TestStreamController::default());
    *controller.handshake_response.lock().unwrap() = Some(make_handshake_response_frame());
    controller.recv_pending.store(true, Ordering::SeqCst);
    install_controller(controller.clone());

    let counter = Arc::new(AtomicUsize::new(0));
    let (mut handle, manager) = create::<TestStreamAdapter, CountingTick>(
        "mock://".to_string(),
        AuthConfig::default(),
        CountingTick(counter.clone()),
    );
    let task = tokio::spawn(manager.run());

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert!(handle.has_ever_connected());
    let initial_close_calls = controller.close_calls.load(Ordering::SeqCst);

    let mut new_auth = AuthConfig::default();
    new_auth.jwt_token = Some("refreshed".into());
    handle.update_auth(new_auth);

    // Expect: Disconnected emitted; stream closed; reconnect reaches Connected again.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let mut events = Vec::new();
    while let Some(msg) = handle.try_recv_inbound() {
        events.push(msg);
    }
    let has_disconnected = events.iter().any(|e| matches!(e, TransportInbound::Disconnected));
    let connected_count = events.iter().filter(|e| matches!(e, TransportInbound::Connected { .. })).count();
    assert!(has_disconnected, "UpdateAuth while connected must emit Disconnected");
    assert!(connected_count >= 1, "Expected at least one fresh Connected after auth refresh");
    assert!(controller.close_calls.load(Ordering::SeqCst) > initial_close_calls);

    handle.disconnect();
    let _ = tokio::time::timeout(std::time::Duration::from_millis(200), task).await;
}
```

Note: `connected_count` counts events _after_ `handle.has_ever_connected()` check but events received before that drain are lost. Since `try_recv_inbound` returns messages in FIFO order, and this test doesn't drain before `update_auth`, the initial `Connected` + the refresh `Connected` should both be in the queue. If the queue grows unbounded, only the latter matters; adjust the assertion to `connected_count >= 2`.

- [ ] **Step 2: Verify it fails**

Run: `cargo test -p jazz-tools update_auth_during_connected`
Expected: with only Task 6's placeholder, the auth IS swapped and a reconnect happens, so this test may pass. Verify — if it does, move to Step 4 (commit). If it fails because the re-handshake uses stale auth, proceed.

- [ ] **Step 3: Ensure the re-handshake uses the new auth**

The outer loop is already `continue` after `ConnectedExit::UpdateAuth(auth)` sets `self.auth = auth`, and the next iteration's `perform_auth_handshake` reads `self.auth.clone()`. So the plumbing is correct. No additional impl needed here.

Also consider pre-staging a second handshake response in the controller — after the first `recv` delivered the initial handshake, the queue is empty, so the reconnect's handshake needs another frame. Either reset `handshake_response` inside the test after the first handshake is delivered, or make `handshake_response` deliver on every `recv` until flipped. Easiest: after `handle.update_auth(new_auth)`, re-install the handshake response:

```rust
*controller.handshake_response.lock().unwrap() = Some(make_handshake_response_frame());
// Reset the adapter's handshake_delivered flag: since each reconnect spawns a fresh adapter
// via TestStreamAdapter::connect, the new adapter starts with handshake_delivered = false,
// so this works automatically. (Sanity-check by re-reading take_controller().)
```

- [ ] **Step 4: Verify it passes**

Run: `cargo test -p jazz-tools update_auth_during_connected`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/jazz-tools/src/transport_manager.rs
git commit -m "test(jazz-tools): verify UpdateAuth while connected reconnects with new credentials"
```

---

### Task 9: `handle_dropped_is_shutdown`

**Files:**

- Modify: `crates/jazz-tools/src/transport_manager.rs`

- [ ] **Step 1: Write the test**

```rust
#[tokio::test]
async fn handle_dropped_is_shutdown() {
    let controller = Arc::new(TestStreamController::default());
    controller.connect_pending.store(true, Ordering::SeqCst);
    install_controller(controller.clone());

    let counter = Arc::new(AtomicUsize::new(0));
    let (handle, manager) = create::<TestStreamAdapter, CountingTick>(
        "mock://".to_string(),
        AuthConfig::default(),
        CountingTick(counter.clone()),
    );
    let task = tokio::spawn(manager.run());

    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    drop(handle);

    tokio::time::timeout(std::time::Duration::from_millis(200), task)
        .await
        .expect("dropping the handle should shut down the manager")
        .unwrap();
}
```

- [ ] **Step 2: Verify it passes**

Run: `cargo test -p jazz-tools handle_dropped_is_shutdown`
Expected: PASS (control_rx.next() returns None when control_tx is dropped; all phase selects handle `None` as Shutdown).

- [ ] **Step 3: Commit**

```bash
git add crates/jazz-tools/src/transport_manager.rs
git commit -m "test(jazz-tools): dropped TransportHandle triggers manager shutdown from any phase"
```

---

### Task 10: Remove stale outbox-closed shutdown detection

**Files:**

- Modify: `crates/jazz-tools/src/transport_manager.rs`

- [ ] **Step 1: Remove the `outbox_rx.next() → None` shutdown path in `run_connected`**

The control channel is now the sole shutdown signal. Change:

```rust
out = self.outbox_rx.next() => {
    let Some(entry) = out else { return ConnectedExit::HandleDropped; };
    // ...
}
```

to

```rust
out = self.outbox_rx.next() => {
    // When the handle is dropped, control_rx.next() will also return None on the
    // control branch below. Here we treat an unexpected None as "no more writes
    // for now" — break out of this iteration instead of exiting.
    let Some(entry) = out else { continue; };
    // ...
}
```

Remove the `ConnectedExit::HandleDropped` variant entirely; update the outer match to drop the now-unused arm.

- [ ] **Step 2: Verify tests pass**

Run: `cargo test -p jazz-tools`
Expected: all transport tests green.

- [ ] **Step 3: Commit**

```bash
git add crates/jazz-tools/src/transport_manager.rs
git commit -m "refactor(jazz-tools): control channel is the sole shutdown signal for TransportManager"
```

---

## Phase 2 — WASM state machine

### Task 11: Mirror control-channel changes in WASM `run()`

**Files:**

- Modify: `crates/jazz-tools/src/transport_manager.rs` (WASM block, lines 337-453)

- [ ] **Step 1: Apply symmetric changes**

Replicate Tasks 3-6 and 10 in the `#[cfg(not(feature = "runtime-tokio"))]` block using `futures::select!`:

- Wrap `W::connect(&self.url)` with a control-racing select.
- Wrap `self.reconnect.backoff()` with a control-racing select.
- Wrap `self.wasm_perform_auth_handshake` with a control-racing select.
- Add a control branch to `wasm_run_connected`'s `futures::select!`.
- Extend `WasmConnectedExit` with `Shutdown` and `UpdateAuth(AuthConfig)` variants.
- Remove `WasmConnectedExit::HandleDropped`.

Use `futures::FutureExt::fuse()` on each phase future as required by `futures::select!`.

- [ ] **Step 2: Verify WASM target compiles**

Run: `cargo check -p jazz-tools --target wasm32-unknown-unknown --no-default-features --features runtime-wasm` (use the actual feature flag combo the crate expects).

Expected: clean build. If the crate has no separate WASM test harness, a compile check is sufficient for this commit; runtime parity is validated by Task 12.

- [ ] **Step 3: Commit**

```bash
git add crates/jazz-tools/src/transport_manager.rs
git commit -m "feat(jazz-tools): mirror control-channel phase machine in WASM run()"
```

---

### Task 12: WASM smoke tests (optional if infra absent)

**Files:**

- Modify: `crates/jazz-tools/src/transport_manager.rs`

- [ ] **Step 1: Check for `wasm-bindgen-test` infra**

Run: `grep -r "wasm_bindgen_test" crates/jazz-tools`

If absent, stop here — note in the commit message that WASM parity relies on the symmetric implementation and skip the test additions. If present, proceed.

- [ ] **Step 2: Add `#[wasm_bindgen_test]` mirrors of `shutdown_during_connected` and `update_auth_during_connected`**

Place under `#[cfg(target_arch = "wasm32")]` test module, using `futures::future::pending` etc. for gating.

- [ ] **Step 3: Run WASM tests**

Run: `wasm-pack test --headless --chrome crates/jazz-tools` (or the project's actual command).
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add crates/jazz-tools/src/transport_manager.rs
git commit -m "test(jazz-tools): wasm-bindgen-test coverage for control-channel behavior while connected"
```

---

## Phase 3 — Rust bindings

### Task 13: Expose `RuntimeCore::transport()` accessor

**Files:**

- Modify: `crates/jazz-tools/src/runtime_core.rs`

- [ ] **Step 1: Add the accessor**

Inside the `impl<S: Storage, Sch: Scheduler> RuntimeCore<S, Sch>` block around line 429:

```rust
/// Returns a reference to the active transport handle, if any.
pub fn transport(&self) -> Option<&crate::transport_manager::TransportHandle> {
    self.transport.as_ref()
}
```

- [ ] **Step 2: Verify the crate builds**

Run: `cargo build -p jazz-tools`
Expected: clean build.

- [ ] **Step 3: Commit**

```bash
git add crates/jazz-tools/src/runtime_core.rs
git commit -m "feat(jazz-tools): expose RuntimeCore::transport() accessor"
```

---

### Task 14: NAPI `update_auth` and updated `disconnect`

**Files:**

- Modify: `crates/jazz-napi/src/lib.rs`
- Modify: `crates/jazz-napi/index.d.ts`

- [ ] **Step 1: Modify `disconnect` and add `update_auth`**

Replace the existing NAPI `disconnect` and append `update_auth` (around lines 1206-1212):

```rust
/// Disconnect from the Jazz server and drop the transport handle.
#[napi]
pub fn disconnect(&self) {
    if let Ok(mut core) = self.core.lock() {
        if let Some(handle) = core.transport() {
            handle.disconnect();
        }
        core.clear_transport();
    }
}

/// Push updated auth credentials into the live transport.
#[napi]
pub fn update_auth(&self, auth_json: String) -> napi::Result<()> {
    let auth: jazz_tools::transport_manager::AuthConfig = serde_json::from_str(&auth_json)
        .map_err(|e| napi::Error::from_reason(e.to_string()))?;
    if let Ok(core) = self.core.lock() {
        if let Some(handle) = core.transport() {
            handle.update_auth(auth);
        }
    }
    Ok(())
}
```

- [ ] **Step 2: Update TypeScript typings**

In `crates/jazz-napi/index.d.ts`, add `updateAuth(authJson: string): void` to the runtime class alongside `connect`/`disconnect`.

- [ ] **Step 3: Build the NAPI addon**

Run: `pnpm --filter jazz-napi build` (or whatever the project's command is — check `package.json`).
Expected: clean build; `index.d.ts` matches the added Rust method.

- [ ] **Step 4: Commit**

```bash
git add crates/jazz-napi/src/lib.rs crates/jazz-napi/index.d.ts
git commit -m "feat(jazz-napi): add update_auth and route disconnect through control channel"
```

---

### Task 15: WASM `update_auth` and updated `disconnect`

**Files:**

- Modify: `crates/jazz-wasm/src/runtime.rs`

- [ ] **Step 1: Modify `disconnect` and add `update_auth`**

Around lines 1518-1523:

```rust
/// Disconnect from the Jazz server and drop the transport handle.
#[cfg(target_arch = "wasm32")]
#[wasm_bindgen]
pub fn disconnect(&self) {
    let mut core = self.core.borrow_mut();
    if let Some(handle) = core.transport() {
        handle.disconnect();
    }
    core.clear_transport();
}

/// Push updated auth credentials into the live transport.
#[cfg(target_arch = "wasm32")]
#[wasm_bindgen]
pub fn update_auth(&self, auth_json: String) -> Result<(), JsValue> {
    let auth: jazz_tools::transport_manager::AuthConfig =
        serde_json::from_str(&auth_json).map_err(|e| JsValue::from_str(&e.to_string()))?;
    let core = self.core.borrow();
    if let Some(handle) = core.transport() {
        handle.update_auth(auth);
    }
    Ok(())
}
```

- [ ] **Step 2: Build jazz-wasm**

Run: `pnpm --filter jazz-wasm build` (or the project's build command).
Expected: clean build; wasm-bindgen generates the JS binding for `updateAuth`.

- [ ] **Step 3: Commit**

```bash
git add crates/jazz-wasm/src/runtime.rs
git commit -m "feat(jazz-wasm): add update_auth and route disconnect through control channel"
```

---

### Task 16: RN `update_auth` and updated `disconnect`

**Files:**

- Modify: `crates/jazz-rn/rust/src/lib.rs`

- [ ] **Step 1: Modify `disconnect` and add `update_auth`**

Around lines 850-855:

```rust
pub fn disconnect(&self) {
    if let Ok(mut core) = self.core.lock() {
        if let Some(handle) = core.transport() {
            handle.disconnect();
        }
        core.clear_transport();
    }
}

pub fn update_auth(&self, auth_json: String) -> Result<(), JazzRnError> {
    with_panic_boundary("update_auth", || {
        let auth: jazz_tools::transport_manager::AuthConfig =
            serde_json::from_str(&auth_json).map_err(json_err)?;
        if let Ok(core) = self.core.lock() {
            if let Some(handle) = core.transport() {
                handle.update_auth(auth);
            }
        }
        Ok(())
    })
}
```

Also annotate `update_auth` for UniFFI export — mirror the annotations on `connect`/`disconnect` (typically `#[uniffi::export]` on the impl block; the method just needs a matching UDL entry or procedural attribute depending on the project's setup).

- [ ] **Step 2: Build jazz-rn**

Run: `pnpm --filter jazz-rn build` (or the project's command).
Expected: clean build.

- [ ] **Step 3: Commit**

```bash
git add crates/jazz-rn/rust/src/lib.rs
git commit -m "feat(jazz-rn): add update_auth and route disconnect through control channel"
```

---

### Task 17: Regenerate RN UniFFI bindings

**Files:**

- Modify: `crates/jazz-rn/src/generated/jazz_rn.ts`

- [ ] **Step 1: Run UniFFI codegen**

Run the project's standard UniFFI regeneration command (check `crates/jazz-rn/README.md` or the package.json scripts — commit `c6500b13` regenerated bindings previously and the command pattern should be present).

- [ ] **Step 2: Verify the generated file includes `updateAuth`**

Run: `grep -n updateAuth crates/jazz-rn/src/generated/jazz_rn.ts`
Expected: at least one match.

- [ ] **Step 3: Commit**

```bash
git add crates/jazz-rn/src/generated/jazz_rn.ts
git commit -m "chore(jazz-rn): regenerate UniFFI bindings for update_auth"
```

---

## Phase 4 — JS URL helper

### Task 18: Create shared `url.ts` module with tests

**Files:**

- Create: `packages/jazz-tools/src/runtime/url.ts`
- Create: `packages/jazz-tools/src/runtime/url.test.ts`

- [ ] **Step 1: Write the failing tests**

Create `packages/jazz-tools/src/runtime/url.test.ts`:

```ts
import { describe, it, expect } from "vitest";
import { httpUrlToWs } from "./url.js";

describe("httpUrlToWs", () => {
  it("http → ws and appends /ws", () => {
    expect(httpUrlToWs("http://localhost:4000")).toBe("ws://localhost:4000/ws");
  });
  it("https → wss and trims trailing slash", () => {
    expect(httpUrlToWs("https://api.example.com/")).toBe("wss://api.example.com/ws");
  });
  it("ws/wss passthrough and idempotent /ws suffix", () => {
    expect(httpUrlToWs("ws://host")).toBe("ws://host/ws");
    expect(httpUrlToWs("wss://host/ws")).toBe("wss://host/ws");
  });
  it("applies pathPrefix with leading slash handling", () => {
    expect(httpUrlToWs("http://localhost:4000", "/apps/xyz")).toBe(
      "ws://localhost:4000/apps/xyz/ws",
    );
  });
  it("applies pathPrefix tolerating extra slashes", () => {
    expect(httpUrlToWs("http://localhost:4000", "apps/xyz/")).toBe(
      "ws://localhost:4000/apps/xyz/ws",
    );
  });
  it("empty or undefined pathPrefix behaves like no prefix", () => {
    expect(httpUrlToWs("http://localhost:4000", "")).toBe("ws://localhost:4000/ws");
    expect(httpUrlToWs("http://localhost:4000", undefined)).toBe("ws://localhost:4000/ws");
  });
  it("throws on invalid scheme", () => {
    expect(() => httpUrlToWs("ftp://host")).toThrow(/Invalid server URL/);
  });
});
```

- [ ] **Step 2: Verify the test file fails to import (module missing)**

Run: `pnpm --filter jazz-tools vitest run packages/jazz-tools/src/runtime/url.test.ts`
Expected: module-not-found error for `./url.js`.

- [ ] **Step 3: Implement `httpUrlToWs`**

Create `packages/jazz-tools/src/runtime/url.ts`:

```ts
/**
 * Convert an HTTP(S) server URL to the WebSocket `/ws` endpoint URL.
 *
 * Mirrors the Rust `http_url_to_ws` helper in `crates/jazz-tools/src/client.rs`.
 *
 * - `http://host`              → `ws://host/ws`
 * - `https://host`             → `wss://host/ws`
 * - `http://host`, `/apps/xyz` → `ws://host/apps/xyz/ws`
 * - `ws://host`                → `ws://host/ws`
 * - `ws://host/ws`             → unchanged
 */
export function httpUrlToWs(serverUrl: string, pathPrefix?: string): string {
  const base = serverUrl.replace(/\/+$/, "");
  const prefix = (pathPrefix ?? "").replace(/^\/+|\/+$/g, "");
  const tail = prefix.length > 0 ? `/${prefix}/ws` : "/ws";

  if (base.startsWith("http://")) {
    return `ws://${base.slice("http://".length)}${tail}`;
  }
  if (base.startsWith("https://")) {
    return `wss://${base.slice("https://".length)}${tail}`;
  }
  if (base.startsWith("ws://") || base.startsWith("wss://")) {
    // If prefix is given, append it; otherwise preserve the existing behavior:
    // idempotent /ws suffix.
    if (prefix.length > 0) {
      const noWsSuffix = base.endsWith("/ws") ? base.slice(0, -"/ws".length) : base;
      return `${noWsSuffix}${tail}`;
    }
    return base.endsWith("/ws") ? base : `${base}/ws`;
  }
  throw new Error(
    `Invalid server URL "${serverUrl}": expected http://, https://, ws://, or wss://`,
  );
}
```

- [ ] **Step 4: Verify tests pass**

Run: `pnpm --filter jazz-tools vitest run packages/jazz-tools/src/runtime/url.test.ts`
Expected: all cases green.

- [ ] **Step 5: Commit**

```bash
git add packages/jazz-tools/src/runtime/url.ts packages/jazz-tools/src/runtime/url.test.ts
git commit -m "feat(jazz-tools): lift httpUrlToWs into shared url module with pathPrefix support"
```

---

### Task 19: Replace the local `httpUrlToWs` in `client.ts`

**Files:**

- Modify: `packages/jazz-tools/src/runtime/client.ts`

- [ ] **Step 1: Delete the local definition and import the shared one**

Remove lines 1522-1549 (the local `httpUrlToWs` function with its doc comment). Add near the top of the file:

```ts
import { httpUrlToWs } from "./url.js";
```

- [ ] **Step 2: Verify existing tests pass**

Run: `pnpm --filter jazz-tools test` (or the targeted suite if faster).
Expected: green — the old and new implementations produce identical output for single-argument calls.

- [ ] **Step 3: Commit**

```bash
git add packages/jazz-tools/src/runtime/client.ts
git commit -m "refactor(jazz-tools): import httpUrlToWs from shared url module"
```

---

## Phase 5 — JS worker wiring

### Task 20: Worker URL normalization

**Files:**

- Modify: `packages/jazz-tools/src/worker/jazz-worker.ts`

- [ ] **Step 1: Add the import and use it in `handleInit`**

Near the top of `jazz-worker.ts`:

```ts
import { httpUrlToWs } from "../runtime/url.js";
```

Replace the connect block inside `handleInit` (currently lines 307-320):

```ts
if (msg.serverUrl) {
  if (msg.jwtToken) {
    currentAuth.jwt_token = msg.jwtToken;
  }
  if (msg.adminSecret) {
    currentAuth.admin_secret = msg.adminSecret;
  }
  try {
    const wsUrl = httpUrlToWs(msg.serverUrl, msg.serverPathPrefix);
    runtime.connect(wsUrl, JSON.stringify(currentAuth));
  } catch (connectError: any) {
    console.error("[worker] runtime.connect failed:", connectError);
  }
}
```

Add the `currentAuth` module-level cache near the other top-level state (around line 60):

```ts
let currentAuth: Record<string, string> = {};
```

Reset it inside `handleInit` alongside the other state resets (around lines 213-215):

```ts
currentAuth = {};
```

- [ ] **Step 2: Verify build passes**

Run: `pnpm --filter jazz-tools build` (or `tsc --noEmit`).
Expected: no type errors.

- [ ] **Step 3: Commit**

```bash
git add packages/jazz-tools/src/worker/jazz-worker.ts
git commit -m "feat(jazz-tools): worker normalizes serverUrl via httpUrlToWs and caches auth"
```

---

### Task 21: Wire `update-auth` message through to `runtime.updateAuth`

**Files:**

- Modify: `packages/jazz-tools/src/worker/jazz-worker.ts`

- [ ] **Step 1: Replace the no-op handler**

Replace lines 421-424:

```ts
case "update-auth": {
  if (msg.jwtToken) {
    currentAuth.jwt_token = msg.jwtToken;
  } else {
    delete currentAuth.jwt_token;
  }
  if (runtime) {
    try {
      runtime.updateAuth(JSON.stringify(currentAuth));
    } catch (e) {
      console.error("[worker] runtime.updateAuth failed:", e);
    }
  }
  break;
}
```

- [ ] **Step 2: Verify build passes**

Run: `pnpm --filter jazz-tools build`
Expected: no type errors (the WASM types may need regeneration if Task 15 wasn't built yet; if missing, rebuild jazz-wasm first).

- [ ] **Step 3: Commit**

```bash
git add packages/jazz-tools/src/worker/jazz-worker.ts
git commit -m "feat(jazz-tools): route update-auth worker message to runtime.updateAuth"
```

---

### Task 22: Worker integration tests

**Files:**

- Create or extend: `packages/jazz-tools/src/worker/jazz-worker.test.ts` (check if a sibling test file exists; extend if so, create otherwise).

- [ ] **Step 1: Write tests with a fake runtime**

Sketch (adapt to the harness conventions; if `jazz-worker.ts` isn't directly unit-testable because it uses `self.onmessage` at module top level, factor the dispatch logic into an exported function first or spin up a Worker in tests):

```ts
import { describe, it, expect, vi } from "vitest";

// If the module is not testable as-is, extract the message-handling core into a helper
// and import it here. Otherwise use the browser worker-bridge test harness to drive the
// worker through postMessage.

describe("worker URL + auth wiring", () => {
  it("normalizes serverUrl with serverPathPrefix via httpUrlToWs", async () => {
    const fakeRuntime = { connect: vi.fn(), updateAuth: vi.fn() };
    // ... dispatch an init message that calls into handleInit with fakeRuntime installed.
    // ...
    expect(fakeRuntime.connect).toHaveBeenCalledWith(
      "ws://localhost:4000/apps/xyz/ws",
      expect.stringContaining('"jwt_token":"initial"'),
    );
  });

  it("merges new jwtToken into cached auth on update-auth", async () => {
    const fakeRuntime = { connect: vi.fn(), updateAuth: vi.fn() };
    // dispatch init with jwtToken="initial", adminSecret="s"
    // dispatch update-auth with jwtToken="refreshed"
    const lastCall = fakeRuntime.updateAuth.mock.calls.at(-1)?.[0];
    const auth = JSON.parse(lastCall as string);
    expect(auth.jwt_token).toBe("refreshed");
    expect(auth.admin_secret).toBe("s");
  });

  it("clears jwt_token when update-auth arrives without one", async () => {
    const fakeRuntime = { connect: vi.fn(), updateAuth: vi.fn() };
    // dispatch init with jwtToken="initial", adminSecret="s"
    // dispatch update-auth with no jwtToken
    const lastCall = fakeRuntime.updateAuth.mock.calls.at(-1)?.[0];
    const auth = JSON.parse(lastCall as string);
    expect(auth.jwt_token).toBeUndefined();
    expect(auth.admin_secret).toBe("s");
  });
});
```

If the worker module is not directly testable, extract a `handleMessage(msg, runtime, state)` helper from `jazz-worker.ts` that the tests can drive. Split this into two commits if the extraction is sizable:

1. Refactor: expose a pure `handleMessage` dispatcher.
2. Tests: cover URL + auth-merge cases.

- [ ] **Step 2: Verify tests pass**

Run: `pnpm --filter jazz-tools vitest run packages/jazz-tools/src/worker/jazz-worker.test.ts`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add packages/jazz-tools/src/worker/jazz-worker.test.ts packages/jazz-tools/src/worker/jazz-worker.ts
git commit -m "test(jazz-tools): worker URL normalization and update-auth dispatch coverage"
```

---

## Phase 6 — E2E

### Task 23: Worker-path auth refresh E2E

**Files:**

- Create or extend: `packages/jazz-tools/tests/browser/db.auth-refresh.worker.test.ts` (or extend `db.auth-refresh.test.ts` with a worker-backed variant).

- [ ] **Step 1: Write the test**

Model on the existing `db.auth-refresh.test.ts`. Boot a `Db` backed by a `workerBridge`, issue a write that fails due to an expired JWT, call `db.updateAuthToken(newJwt)`, and assert the queued write succeeds after reconnect. Use a test server or the project's standard auth-refresh harness.

Keep the test small — the goal is to exercise the full chain `Db.applyAuthUpdate → workerBridge.updateAuth → worker `update-auth` handler → runtime.updateAuth → TransportManager::UpdateAuth`. If end-to-end server infra isn't available, stub the `runtime` inside the worker and assert `updateAuth` is called with the expected auth JSON.

- [ ] **Step 2: Verify it passes**

Run: `pnpm --filter jazz-tools vitest run packages/jazz-tools/tests/browser/db.auth-refresh.worker.test.ts`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add packages/jazz-tools/tests/browser/db.auth-refresh.worker.test.ts
git commit -m "test(jazz-tools): E2E worker-path auth refresh through TransportControl"
```

---

## Phase 7 — Cleanup

### Task 24: Full workspace test sweep

- [ ] **Step 1: Run everything**

Run: `pnpm test && cargo test` (or the turbo-orchestrated equivalents).

Expected: all green. Investigate any regressions — they likely stem from removed `ConnectedExit::HandleDropped` or refactored `disconnect` behavior. Fix root cause (don't mask by skipping tests).

- [ ] **Step 2: If regressions are found, fix in place and commit**

One commit per logically distinct fix. Avoid a catch-all "fix tests" commit.

- [ ] **Step 3: Final sanity commit (if needed)**

If the sweep is clean, no commit needed.

---

## Self-Review Checklist

- [ ] Every spec section (Goals, Background, Rust core, Rust bindings, JS worker wiring, URL normalization, Testing, Backcompat) is covered by at least one task.
- [ ] No `TBD`/`TODO`/"implement later" placeholders in task steps.
- [ ] Types and method names match across tasks (`TransportControl`, `TransportHandle::disconnect`, `TransportHandle::update_auth`, `RuntimeCore::transport`, `httpUrlToWs(serverUrl, pathPrefix?)`, `runtime.updateAuth`, `currentAuth`).
- [ ] Every phase-teardown emits `Disconnected` as specified (not on Shutdown).
- [ ] `outbox_rx`-closed shortcut in `run_connected` is removed (Task 10).
- [ ] Binding changes are symmetric across NAPI, WASM, and RN.
