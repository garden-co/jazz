# Internal Server Shutdown Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `POST /internal/shutdown`, authenticated by admin secret, that starts a controlled server shutdown with WebSocket drain, runtime flush, RocksDB flush/WAL flush, and storage close.

**Architecture:** Add a reusable shutdown controller to `ServerState`, then wire HTTP, health, route gating, WebSocket close notification, and server-command lifecycle around that single controller. Request handlers only request shutdown; the server host owns finalization and Axum listener shutdown.

**Tech Stack:** Rust 2024, Axum 0.7, Tokio, `tokio::sync::watch`/`Notify`, Jazz `TokioRuntime`, existing `Storage` trait.

---

## File Structure

- Create `crates/jazz-tools/src/server/shutdown.rs`: shutdown phase enum, controller, active request/socket guards, wait helpers.
- Modify `crates/jazz-tools/src/server/mod.rs`: include the controller in `ServerState` and add finalization helpers.
- Modify `crates/jazz-tools/src/server/builder.rs`: carry shutdown timeout from builder into state.
- Modify `crates/jazz-tools/src/server/routes/http.rs`: add shutdown DTO/handler and make health state-aware.
- Modify `crates/jazz-tools/src/server/routes/mod.rs`: add `/internal/shutdown`, add app-scoped shutdown gate, and route tests.
- Modify `crates/jazz-tools/src/server/routes/websocket.rs`: reject new shutdown-era upgrades and close active sockets on shutdown notification.
- Modify `crates/jazz-tools/src/server/hosted.rs`: use the shared shutdown driver for test/dev hosted servers.
- Modify `crates/jazz-tools/src/commands/server.rs`: serve with Axum graceful shutdown driven by the shared controller.
- Modify `crates/jazz-tools/src/main.rs`: add `--shutdown-timeout-secs` / `JAZZ_SHUTDOWN_TIMEOUT_SECS`.
- Modify `crates/jazz-tools/src/server/testing.rs`: add an end-to-end hosted-server shutdown test.

---

### Task 1: Add Shutdown Controller

**Files:**

- Create: `crates/jazz-tools/src/server/shutdown.rs`
- Modify: `crates/jazz-tools/src/server/mod.rs`
- Modify: `crates/jazz-tools/src/server/builder.rs`
- Test: `crates/jazz-tools/src/server/shutdown.rs`

- [ ] **Step 1: Write failing tests for controller state and idempotency**

Add this test module to the new file `crates/jazz-tools/src/server/shutdown.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_shutdown_is_idempotent() {
        let controller = ShutdownController::new(std::time::Duration::from_secs(30));

        assert_eq!(controller.phase(), ShutdownPhase::Running);
        assert!(controller.request_shutdown());
        assert_eq!(controller.phase(), ShutdownPhase::ShuttingDown);
        assert!(!controller.request_shutdown());
        assert_eq!(controller.phase(), ShutdownPhase::ShuttingDown);
    }

    #[test]
    fn app_request_guard_rejects_after_shutdown_starts() {
        let controller = ShutdownController::new(std::time::Duration::from_secs(30));
        assert_eq!(controller.active_app_requests(), 0);

        let guard = controller.try_enter_app_request().expect("running server accepts request");
        assert_eq!(controller.active_app_requests(), 1);
        drop(guard);
        assert_eq!(controller.active_app_requests(), 0);

        assert!(controller.request_shutdown());
        assert!(controller.try_enter_app_request().is_none());
    }

    #[tokio::test]
    async fn wait_for_shutdown_request_observes_request() {
        let controller = ShutdownController::new(std::time::Duration::from_secs(30));
        let waiter = controller.clone();

        let task = tokio::spawn(async move {
            waiter.wait_requested().await;
            waiter.phase()
        });

        assert!(controller.request_shutdown());

        let phase = task.await.expect("wait task");
        assert_eq!(phase, ShutdownPhase::ShuttingDown);
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
cargo test -p jazz-tools --features test shutdown::tests
```

Expected: FAIL because `crates/jazz-tools/src/server/shutdown.rs` does not exist and `ShutdownController` is undefined.

- [ ] **Step 3: Implement the controller**

Create `crates/jazz-tools/src/server/shutdown.rs`:

```rust
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use tokio::sync::{Notify, watch};

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ShutdownPhase {
    Running,
    ShuttingDown,
    DrainingConnections,
    FlushingRuntime,
    ClosingStorage,
    StorageClosed,
    Failed,
}

impl ShutdownPhase {
    pub fn is_running(self) -> bool {
        matches!(self, Self::Running)
    }
}

#[derive(Clone)]
pub struct ShutdownController {
    inner: Arc<ShutdownInner>,
}

struct ShutdownInner {
    requested: AtomicBool,
    timeout: Duration,
    phase_tx: watch::Sender<ShutdownPhase>,
    drain_notify: Notify,
    active_app_requests: AtomicUsize,
    active_websockets: AtomicUsize,
}

pub struct ActiveAppRequestGuard {
    controller: ShutdownController,
}

pub struct ActiveWebSocketGuard {
    controller: ShutdownController,
}

impl ShutdownController {
    pub fn new(timeout: Duration) -> Self {
        let (phase_tx, _) = watch::channel(ShutdownPhase::Running);
        Self {
            inner: Arc::new(ShutdownInner {
                requested: AtomicBool::new(false),
                timeout,
                phase_tx,
                drain_notify: Notify::new(),
                active_app_requests: AtomicUsize::new(0),
                active_websockets: AtomicUsize::new(0),
            }),
        }
    }

    pub fn timeout(&self) -> Duration {
        self.inner.timeout
    }

    pub fn phase(&self) -> ShutdownPhase {
        *self.inner.phase_tx.borrow()
    }

    pub fn is_shutting_down(&self) -> bool {
        !self.phase().is_running()
    }

    pub fn subscribe(&self) -> watch::Receiver<ShutdownPhase> {
        self.inner.phase_tx.subscribe()
    }

    pub fn set_phase(&self, phase: ShutdownPhase) {
        let _ = self.inner.phase_tx.send(phase);
    }

    pub fn request_shutdown(&self) -> bool {
        let was_requested = self.inner.requested.swap(true, Ordering::SeqCst);
        if !was_requested {
            self.set_phase(ShutdownPhase::ShuttingDown);
            self.inner.drain_notify.notify_waiters();
        }
        !was_requested
    }

    pub async fn wait_requested(&self) {
        if self.inner.requested.load(Ordering::SeqCst) {
            return;
        }

        let mut rx = self.subscribe();
        while rx.borrow().is_running() {
            if rx.changed().await.is_err() {
                break;
            }
        }
    }

    pub fn try_enter_app_request(&self) -> Option<ActiveAppRequestGuard> {
        if self.is_shutting_down() {
            return None;
        }

        self.inner.active_app_requests.fetch_add(1, Ordering::SeqCst);
        if self.is_shutting_down() {
            self.inner.active_app_requests.fetch_sub(1, Ordering::SeqCst);
            self.inner.drain_notify.notify_waiters();
            return None;
        }

        Some(ActiveAppRequestGuard {
            controller: self.clone(),
        })
    }

    pub fn try_enter_websocket(&self) -> Option<ActiveWebSocketGuard> {
        if self.is_shutting_down() {
            return None;
        }

        self.inner.active_websockets.fetch_add(1, Ordering::SeqCst);
        if self.is_shutting_down() {
            self.inner.active_websockets.fetch_sub(1, Ordering::SeqCst);
            self.inner.drain_notify.notify_waiters();
            return None;
        }

        Some(ActiveWebSocketGuard {
            controller: self.clone(),
        })
    }

    pub fn active_app_requests(&self) -> usize {
        self.inner.active_app_requests.load(Ordering::SeqCst)
    }

    pub fn active_websockets(&self) -> usize {
        self.inner.active_websockets.load(Ordering::SeqCst)
    }

    pub async fn wait_for_websocket_drain(&self) -> bool {
        let deadline = tokio::time::Instant::now() + self.timeout();
        loop {
            if self.active_websockets() == 0 {
                return true;
            }

            let now = tokio::time::Instant::now();
            if now >= deadline {
                return false;
            }

            let sleep = tokio::time::sleep_until(deadline);
            tokio::select! {
                _ = self.inner.drain_notify.notified() => {}
                _ = sleep => return self.active_websockets() == 0,
            }
        }
    }

    pub async fn wait_for_app_request_drain(&self) {
        while self.active_app_requests() > 0 {
            self.inner.drain_notify.notified().await;
        }
    }
}

impl Drop for ActiveAppRequestGuard {
    fn drop(&mut self) {
        self.controller
            .inner
            .active_app_requests
            .fetch_sub(1, Ordering::SeqCst);
        self.controller.inner.drain_notify.notify_waiters();
    }
}

impl Drop for ActiveWebSocketGuard {
    fn drop(&mut self) {
        self.controller
            .inner
            .active_websockets
            .fetch_sub(1, Ordering::SeqCst);
        self.controller.inner.drain_notify.notify_waiters();
    }
}
```

- [ ] **Step 4: Wire the controller into server state and builder**

In `crates/jazz-tools/src/server/mod.rs`, add the module and imports:

```rust
mod shutdown;

pub use shutdown::{ShutdownController, ShutdownPhase};
```

Add this field to `ServerState`:

```rust
pub shutdown: ShutdownController,
```

In `crates/jazz-tools/src/server/builder.rs`, add a default:

```rust
const DEFAULT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);
```

Add this field to `ServerBuilder`:

```rust
shutdown_timeout: Duration,
```

Initialize it in `ServerBuilder::new`:

```rust
shutdown_timeout: DEFAULT_SHUTDOWN_TIMEOUT,
```

Add this builder method:

```rust
pub fn with_shutdown_timeout(mut self, timeout: Duration) -> Self {
    self.shutdown_timeout = timeout;
    self
}
```

Set the field when constructing `ServerState`:

```rust
shutdown: crate::server::ShutdownController::new(self.shutdown_timeout),
```

- [ ] **Step 5: Run tests to verify green**

Run:

```bash
cargo test -p jazz-tools --features test shutdown::tests
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/jazz-tools/src/server/shutdown.rs crates/jazz-tools/src/server/mod.rs crates/jazz-tools/src/server/builder.rs
git commit -m "feat: add server shutdown controller"
```

---

### Task 2: Add Internal Shutdown and Health Contract

**Files:**

- Modify: `crates/jazz-tools/src/server/routes/http.rs`
- Modify: `crates/jazz-tools/src/server/routes/mod.rs`
- Test: `crates/jazz-tools/src/server/routes/mod.rs`

- [ ] **Step 1: Write failing route tests**

Add these helpers inside the existing `#[cfg(test)] mod tests` in `crates/jazz-tools/src/server/routes/mod.rs`:

```rust
async fn post_internal_shutdown(
    app: axum::Router,
    admin_secret: Option<&str>,
) -> axum::response::Response {
    let mut builder = axum::http::Request::builder()
        .method("POST")
        .uri("/internal/shutdown");
    if let Some(admin_secret) = admin_secret {
        builder = builder.header("X-Jazz-Admin-Secret", admin_secret);
    }
    app.oneshot(builder.body(axum::body::Body::empty()).unwrap())
        .await
        .unwrap()
}
```

Add these tests:

```rust
#[tokio::test]
async fn internal_shutdown_requires_configured_admin_secret() {
    let auth_config = AuthConfig {
        admin_secret: None,
        allow_local_first_auth: true,
        ..Default::default()
    };
    let state = ServerBuilder::new(AppId::from_name("test-app"))
        .with_auth_config(auth_config)
        .with_storage(StorageBackend::InMemory)
        .build()
        .await
        .expect("build server without admin secret")
        .state;

    let response = post_internal_shutdown(make_test_router(state), Some("admin-secret")).await;
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn internal_shutdown_requires_admin_secret_header() {
    let state = make_state_with_schema(Schema::new()).await;
    let response = post_internal_shutdown(make_test_router(state), None).await;
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn internal_shutdown_rejects_wrong_admin_secret() {
    let state = make_state_with_schema(Schema::new()).await;
    let response = post_internal_shutdown(make_test_router(state), Some("wrong-secret")).await;
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn internal_shutdown_accepts_valid_admin_secret_and_marks_health_unhealthy() {
    let state = make_state_with_schema(Schema::new()).await;
    let app = make_test_router(state.clone());

    let response = post_internal_shutdown(app.clone(), Some("admin-secret")).await;
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let body = body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("shutdown body");
    let json: Value = serde_json::from_slice(&body).expect("shutdown json");
    assert_eq!(json["status"].as_str(), Some("shutting_down"));

    let repeated = post_internal_shutdown(app.clone(), Some("admin-secret")).await;
    assert_eq!(repeated.status(), StatusCode::ACCEPTED);
    let repeated_body = body::to_bytes(repeated.into_body(), usize::MAX)
        .await
        .expect("repeated shutdown body");
    let repeated_json: Value = serde_json::from_slice(&repeated_body).expect("repeated json");
    assert_eq!(repeated_json["status"].as_str(), Some("already_shutting_down"));

    let health = app
        .oneshot(
            axum::http::Request::builder()
                .uri("/health")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(health.status(), StatusCode::SERVICE_UNAVAILABLE);
    let health_body = body::to_bytes(health.into_body(), usize::MAX)
        .await
        .expect("health body");
    let health_json: Value = serde_json::from_slice(&health_body).expect("health json");
    assert_eq!(health_json["status"].as_str(), Some("shutting_down"));
    assert_eq!(health_json["phase"].as_str(), Some("shutting_down"));

    assert_eq!(state.shutdown.phase(), crate::server::ShutdownPhase::ShuttingDown);
}
```

- [ ] **Step 2: Run tests to verify red**

Run:

```bash
cargo test -p jazz-tools --features test internal_shutdown
```

Expected: FAIL because `/internal/shutdown` is not routed and `/health` is not state-aware.

- [ ] **Step 3: Implement route DTOs and handlers**

In `crates/jazz-tools/src/server/routes/http.rs`, add:

```rust
#[derive(Debug, Serialize)]
pub(super) struct ShutdownResponse {
    status: &'static str,
}
```

Add this handler:

```rust
pub(super) async fn internal_shutdown_handler(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let admin_secret = headers
        .get("X-Jazz-Admin-Secret")
        .and_then(|v| v.to_str().ok());

    match validate_admin_secret(admin_secret, &state.auth_config) {
        Ok(()) => {}
        Err((status, msg)) => {
            return (status, Json(ErrorResponse::unauthorized(msg))).into_response();
        }
    }

    let first_request = state.shutdown.request_shutdown();
    let status = if first_request {
        "shutting_down"
    } else {
        "already_shutting_down"
    };

    (StatusCode::ACCEPTED, Json(ShutdownResponse { status })).into_response()
}
```

Replace `health_handler` with:

```rust
pub(super) async fn health_handler(State(state): State<Arc<ServerState>>) -> impl IntoResponse {
    let phase = state.shutdown.phase();
    if phase.is_running() {
        return Json(serde_json::json!({
            "status": "healthy"
        }))
        .into_response();
    }

    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(serde_json::json!({
            "status": "shutting_down",
            "phase": phase
        })),
    )
        .into_response()
}
```

- [ ] **Step 4: Route `/internal/shutdown`**

In `crates/jazz-tools/src/server/routes/mod.rs`, include the handler in the existing grouped `use http` import:

```rust
internal_shutdown_handler,
```

Add the route to the root router before `.nest(&app_route_prefix, traced_routes)`:

```rust
.route("/internal/shutdown", post(internal_shutdown_handler))
```

- [ ] **Step 5: Run tests to verify green**

Run:

```bash
cargo test -p jazz-tools --features test internal_shutdown
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/jazz-tools/src/server/routes/http.rs crates/jazz-tools/src/server/routes/mod.rs
git commit -m "feat: add internal shutdown endpoint"
```

---

### Task 3: Gate App-Scoped Requests During Shutdown

**Files:**

- Modify: `crates/jazz-tools/src/server/routes/mod.rs`
- Test: `crates/jazz-tools/src/server/routes/mod.rs`

- [ ] **Step 1: Write failing route-gating test**

Add this test to `crates/jazz-tools/src/server/routes/mod.rs`:

```rust
#[tokio::test]
async fn shutdown_rejects_new_app_scoped_http_requests_but_keeps_internal_routes_available() {
    let state = make_state_with_schema(Schema::new()).await;
    let app = make_test_router(state);

    let shutdown = post_internal_shutdown(app.clone(), Some("admin-secret")).await;
    assert_eq!(shutdown.status(), StatusCode::ACCEPTED);

    let app_scoped = app
        .clone()
        .oneshot(
            axum::http::Request::builder()
                .uri(test_app_route("/schemas"))
                .header("X-Jazz-Admin-Secret", "admin-secret")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(app_scoped.status(), StatusCode::SERVICE_UNAVAILABLE);

    let repeated = post_internal_shutdown(app.clone(), Some("admin-secret")).await;
    assert_eq!(repeated.status(), StatusCode::ACCEPTED);

    let health = app
        .oneshot(
            axum::http::Request::builder()
                .uri("/health")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(health.status(), StatusCode::SERVICE_UNAVAILABLE);
}
```

- [ ] **Step 2: Run test to verify red**

Run:

```bash
cargo test -p jazz-tools --features test shutdown_rejects_new_app_scoped_http_requests_but_keeps_internal_routes_available
```

Expected: FAIL because app-scoped routes still execute after shutdown starts.

- [ ] **Step 3: Implement app-scoped gate middleware**

In `crates/jazz-tools/src/server/routes/mod.rs`, add imports:

```rust
use axum::{
    Router,
    body::Body,
    extract::State,
    http::{Request, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
};
```

Add this middleware function:

```rust
async fn app_shutdown_gate(
    State(state): State<Arc<ServerState>>,
    request: Request<Body>,
    next: Next,
) -> Response {
    let Some(_guard) = state.shutdown.try_enter_app_request() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            axum::Json(crate::jazz_transport::ErrorResponse::internal(
                "server is shutting down".to_string(),
            )),
        )
            .into_response();
    };

    next.run(request).await
}
```

Apply it only to the app-scoped router:

```rust
let traced_routes = Router::new()
    .route("/ws", axum::routing::any(ws_handler))
    .route("/schema/:hash", get(schema_handler))
    .route("/schemas", get(schema_hashes_handler))
    .nest("/admin", admin_routes)
    .route_layer(middleware::from_fn_with_state(
        state.clone(),
        app_shutdown_gate,
    ))
    .layer(TraceLayer::new_for_http());
```

- [ ] **Step 4: Run test to verify green**

Run:

```bash
cargo test -p jazz-tools --features test shutdown_rejects_new_app_scoped_http_requests_but_keeps_internal_routes_available
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/jazz-tools/src/server/routes/mod.rs
git commit -m "feat: gate app routes during shutdown"
```

---

### Task 4: Close Active WebSockets on Shutdown

**Files:**

- Modify: `crates/jazz-tools/src/server/routes/websocket.rs`
- Test: `crates/jazz-tools/src/server/routes/mod.rs`

- [ ] **Step 1: Write failing WebSocket shutdown test**

Add this test to `crates/jazz-tools/src/server/routes/mod.rs` near the existing WebSocket route tests:

```rust
#[tokio::test(flavor = "current_thread")]
async fn shutdown_closes_active_websocket_with_service_restart_code() {
    let state = make_sync_test_state("test-backend-secret").await;
    let app = create_router(state.clone());

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ws listener");
    let addr = listener.local_addr().expect("ws local addr");
    let server_task = tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve ws app");
    });

    let ws_url = format!("ws://{addr}{}", test_app_route("/ws"));
    let (mut ws, _) = connect_async(&ws_url).await.expect("connect ws");

    let handshake = crate::transport_manager::AuthHandshake {
        sync_protocol_version: crate::transport_manager::SYNC_PROTOCOL_VERSION,
        client_id: ClientId::new().to_string(),
        auth: crate::transport_manager::AuthConfig {
            backend_secret: Some("test-backend-secret".to_string()),
            ..Default::default()
        },
        catalogue_state_hash: None,
        declared_schema_hash: None,
    };
    let payload = serde_json::to_vec(&handshake).expect("serialize handshake");
    ws.send(WsMessage::Binary(
        crate::transport_manager::frame_encode(&payload).into(),
    ))
    .await
    .expect("send handshake");

    let connected = tokio::time::timeout(Duration::from_secs(5), ws.next())
        .await
        .expect("wait for ConnectedResponse")
        .expect("ws frame")
        .expect("ws result");
    assert!(matches!(connected, WsMessage::Binary(_)));

    assert_eq!(state.shutdown.active_websockets(), 1);
    assert!(state.shutdown.request_shutdown());

    let close_frame = tokio::time::timeout(Duration::from_secs(5), ws.next())
        .await
        .expect("wait for close")
        .expect("ws frame")
        .expect("ws result");
    let WsMessage::Close(Some(close)) = close_frame else {
        panic!("expected close frame, got {close_frame:?}");
    };
    assert_eq!(
        close.code,
        tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode::Restart
    );
    assert_eq!(close.reason.as_ref(), "server shutting down");

    tokio::time::timeout(Duration::from_secs(5), async {
        while state.shutdown.active_websockets() != 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("websocket cleanup");

    server_task.abort();
}
```

- [ ] **Step 2: Run test to verify red**

Run:

```bash
cargo test -p jazz-tools --features test shutdown_closes_active_websocket_with_service_restart_code
```

Expected: FAIL because active WebSocket loops do not subscribe to shutdown.

- [ ] **Step 3: Implement shutdown rejection and active socket close**

In `crates/jazz-tools/src/server/routes/websocket.rs`, change `ws_handler`:

```rust
pub(super) async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
) -> Response {
    if state.shutdown.is_shutting_down() {
        return (
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            axum::Json(crate::jazz_transport::ErrorResponse::internal(
                "server is shutting down".to_string(),
            )),
        )
            .into_response();
    }

    ws.on_upgrade(move |socket| handle_ws_connection(socket, state, headers))
}
```

Add the missing import:

```rust
use axum::response::IntoResponse;
```

After authentication succeeds but before registering the connection, enter the WebSocket guard:

```rust
let Some(_websocket_guard) = state.shutdown.try_enter_websocket() else {
    close_ws_for_shutdown(&mut socket).await;
    return;
};
```

Add this helper:

```rust
async fn close_ws_for_shutdown(socket: &mut WebSocket) {
    let _ = socket
        .send(Message::Close(Some(CloseFrame {
            code: close_code::RESTART,
            reason: "server shutting down".into(),
        })))
        .await;
}
```

Before the bidirectional loop, subscribe to shutdown:

```rust
let mut shutdown_rx = state.shutdown.subscribe();
```

Add this branch to the `tokio::select!` loop:

```rust
changed = shutdown_rx.changed() => {
    if changed.is_ok() && state.shutdown.is_shutting_down() {
        close_ws_for_shutdown(&mut socket).await;
        break;
    }
}
```

- [ ] **Step 4: Run test to verify green**

Run:

```bash
cargo test -p jazz-tools --features test shutdown_closes_active_websocket_with_service_restart_code
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/jazz-tools/src/server/routes/websocket.rs crates/jazz-tools/src/server/routes/mod.rs
git commit -m "feat: close websockets during shutdown"
```

---

### Task 5: Finalize Runtime and Storage from the Server Lifecycle

**Files:**

- Modify: `crates/jazz-tools/src/server/mod.rs`
- Modify: `crates/jazz-tools/src/server/hosted.rs`
- Modify: `crates/jazz-tools/src/commands/server.rs`
- Test: `crates/jazz-tools/src/server/testing.rs`

- [ ] **Step 1: Write failing hosted-server shutdown test**

Add this test to `crates/jazz-tools/src/server/testing.rs`:

```rust
#[tokio::test]
async fn internal_shutdown_stops_hosted_server() {
    let server = TestingServer::start().await;
    let base_url = server.base_url();
    let admin_secret = server.admin_secret().to_string();
    let client = reqwest::Client::new();

    let response = client
        .post(format!("{base_url}/internal/shutdown"))
        .header("X-Jazz-Admin-Secret", admin_secret)
        .send()
        .await
        .expect("shutdown request");
    assert_eq!(response.status(), StatusCode::ACCEPTED);

    let mut saw_unavailable = false;
    for _ in 0..80 {
        match client.get(format!("{base_url}/health")).send().await {
            Ok(response) if response.status() == StatusCode::SERVICE_UNAVAILABLE => {
                saw_unavailable = true;
            }
            Err(_) if saw_unavailable => return,
            _ => {}
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    panic!("hosted server did not stop after internal shutdown");
}
```

- [ ] **Step 2: Run test to verify red**

Run:

```bash
cargo test -p jazz-tools --features test internal_shutdown_stops_hosted_server
```

Expected: FAIL because shutdown request changes health but does not stop the hosted Axum server.

- [ ] **Step 3: Add server finalization helper**

In `crates/jazz-tools/src/server/mod.rs`, add:

```rust
impl ServerState {
    pub async fn run_shutdown_finalization(&self) {
        self.shutdown.set_phase(ShutdownPhase::DrainingConnections);
        #[cfg(feature = "transport-websocket")]
        self.runtime.disconnect();

        let websockets_drained = self.shutdown.wait_for_websocket_drain().await;
        if !websockets_drained {
            tracing::warn!(
                active_websockets = self.shutdown.active_websockets(),
                "shutdown websocket drain timed out"
            );
        }

        self.shutdown.wait_for_app_request_drain().await;

        self.shutdown.set_phase(ShutdownPhase::FlushingRuntime);
        if let Err(error) = self.runtime.flush().await {
            tracing::error!(%error, "shutdown runtime flush failed");
            self.shutdown.set_phase(ShutdownPhase::Failed);
            return;
        }

        self.shutdown.set_phase(ShutdownPhase::ClosingStorage);
        let storage_result = self.runtime.with_storage(|storage| {
            storage.flush();
            storage.flush_wal();
            storage.close()
        });

        match storage_result {
            Ok(Ok(())) => {
                self.shutdown.set_phase(ShutdownPhase::StorageClosed);
            }
            Ok(Err(error)) => {
                tracing::error!(%error, "shutdown storage close failed");
                self.shutdown.set_phase(ShutdownPhase::Failed);
            }
            Err(error) => {
                tracing::error!(%error, "shutdown storage lock failed");
                self.shutdown.set_phase(ShutdownPhase::Failed);
            }
        }
    }
}
```

- [ ] **Step 4: Drive shutdown from `HostedServer`**

In `crates/jazz-tools/src/server/hosted.rs`, add a field:

```rust
shutdown_task: Option<JoinHandle<()>>,
```

Remove the old field:

```rust
shutdown_tx: Option<oneshot::Sender<()>>,
```

In `HostedServer::start`, replace the existing `shutdown_tx` behavior with a serve signal and a shutdown driver:

```rust
let (serve_shutdown_tx, serve_shutdown_rx) = oneshot::channel();
let shutdown_state = built.state.clone();
let shutdown_task = tokio::spawn(async move {
    shutdown_state.shutdown.wait_requested().await;
    shutdown_state.run_shutdown_finalization().await;
    let _ = serve_shutdown_tx.send(());
});
let task = tokio::spawn(async move {
    axum::serve(listener, built.app)
        .with_graceful_shutdown(async {
            let _ = serve_shutdown_rx.await;
        })
        .await
        .expect("serve jazz server");
});
```

Update `HostedServer::shutdown`:

```rust
pub async fn shutdown(&mut self) {
    self.state.shutdown.request_shutdown();

    if let Some(mut shutdown_task) = self.shutdown_task.take()
        && tokio::time::timeout(Duration::from_secs(5), &mut shutdown_task)
            .await
            .is_err()
    {
        shutdown_task.abort();
        let _ = shutdown_task.await;
    }

    if let Some(mut task) = self.task.take()
        && tokio::time::timeout(Duration::from_millis(500), &mut task)
            .await
            .is_err()
    {
        task.abort();
        let _ = task.await;
    }
}
```

Update `Drop` to request shutdown, abort `shutdown_task`, and abort `task`:

```rust
fn drop(&mut self) {
    self.state.shutdown.request_shutdown();
    if let Some(task) = self.shutdown_task.take() {
        task.abort();
    }
    if let Some(task) = self.task.take() {
        task.abort();
    }
}
```

- [ ] **Step 5: Drive shutdown from the CLI server command**

In `crates/jazz-tools/src/commands/server.rs`, replace the final `axum::serve` call:

```rust
let state = built.state.clone();
let (serve_shutdown_tx, serve_shutdown_rx) = tokio::sync::oneshot::channel();
let shutdown_task = tokio::spawn(async move {
    state.shutdown.wait_requested().await;
    state.run_shutdown_finalization().await;
    let _ = serve_shutdown_tx.send(());
});

let serve_result = axum::serve(listener, built.app)
    .with_graceful_shutdown(async {
        let _ = serve_shutdown_rx.await;
    })
    .await;

shutdown_task.abort();
let _ = shutdown_task.await;
serve_result?;
```

- [ ] **Step 6: Run hosted shutdown test to verify green**

Run:

```bash
cargo test -p jazz-tools --features test internal_shutdown_stops_hosted_server
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add crates/jazz-tools/src/server/mod.rs crates/jazz-tools/src/server/hosted.rs crates/jazz-tools/src/commands/server.rs crates/jazz-tools/src/server/testing.rs
git commit -m "feat: finalize server shutdown lifecycle"
```

---

### Task 6: Add Shutdown Timeout CLI and Env Configuration

**Files:**

- Modify: `crates/jazz-tools/src/main.rs`
- Modify: `crates/jazz-tools/src/commands/server.rs`
- Test: `crates/jazz-tools/src/main.rs`

- [ ] **Step 1: Write failing CLI parse test**

Add this test to `crates/jazz-tools/src/main.rs`:

```rust
#[test]
fn server_command_parses_shutdown_timeout_secs() {
    let cli = Cli::try_parse_from([
        "jazz-tools",
        "server",
        "test-app",
        "--shutdown-timeout-secs",
        "7",
    ])
    .expect("server command should parse");

    match cli.command {
        Commands::Server {
            shutdown_timeout_secs,
            ..
        } => assert_eq!(shutdown_timeout_secs, 7),
        _ => panic!("expected server command"),
    }
}
```

- [ ] **Step 2: Run test to verify red**

Run:

```bash
cargo test -p jazz-tools --features test server_command_parses_shutdown_timeout_secs
```

Expected: FAIL because the CLI field does not exist.

- [ ] **Step 3: Add CLI argument and pass it through**

In the `Commands::Server` variant in `crates/jazz-tools/src/main.rs`, add:

```rust
/// Graceful shutdown network-drain timeout in seconds.
#[arg(long, env = "JAZZ_SHUTDOWN_TIMEOUT_SECS", default_value_t = 30)]
shutdown_timeout_secs: u64,
```

In the match arm destructuring, include:

```rust
shutdown_timeout_secs,
```

Pass it to `commands::server::run`:

```rust
std::time::Duration::from_secs(shutdown_timeout_secs),
```

In `crates/jazz-tools/src/commands/server.rs`, import `Duration` and update the signature:

```rust
use std::time::Duration;

pub async fn run(
    app_id_str: &str,
    port: u16,
    data_dir: &str,
    in_memory: bool,
    auth_config: AuthConfig,
    upstream_url: Option<String>,
    bound_port_file: Option<String>,
    shutdown_timeout: Duration,
) -> Result<(), Box<dyn std::error::Error>> {
```

Apply it to the builder:

```rust
let builder = ServerBuilder::new(app_id)
    .with_auth_config(auth_config)
    .with_shutdown_timeout(shutdown_timeout);
```

- [ ] **Step 4: Run CLI test to verify green**

Run:

```bash
cargo test -p jazz-tools --features test server_command_parses_shutdown_timeout_secs
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/jazz-tools/src/main.rs crates/jazz-tools/src/commands/server.rs
git commit -m "feat: configure server shutdown timeout"
```

---

### Task 7: Full Verification

**Files:**

- No code changes expected.

- [ ] **Step 1: Run focused shutdown tests**

Run:

```bash
cargo test -p jazz-tools --features test internal_shutdown
```

Expected: PASS.

- [ ] **Step 2: Run WebSocket shutdown test**

Run:

```bash
cargo test -p jazz-tools --features test shutdown_closes_active_websocket_with_service_restart_code
```

Expected: PASS.

- [ ] **Step 3: Run hosted lifecycle test**

Run:

```bash
cargo test -p jazz-tools --features test internal_shutdown_stops_hosted_server
```

Expected: PASS.

- [ ] **Step 4: Run server module tests**

Run:

```bash
cargo test -p jazz-tools --features test server::
```

Expected: PASS.

- [ ] **Step 5: Run formatting and lint hooks**

Run:

```bash
pnpm exec lefthook run pre-commit
```

Expected: PASS or only documented skips for unrelated file types.

- [ ] **Step 6: Final commit if verification required edits**

If verification forced any edits, commit them:

```bash
git add crates/jazz-tools/src docs/superpowers/plans/2026-05-17-internal-server-shutdown.md
git commit -m "test: verify internal server shutdown"
```
