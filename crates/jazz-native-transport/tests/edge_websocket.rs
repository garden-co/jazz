//! Outward integration receipts for the native connector against Jazz's
//! public server composition API.  Keeping these here avoids a `jazz` test
//! dependency back into its adapter and therefore preserves one Jazz type
//! identity in each test process.

use std::sync::Arc;
use std::time::Duration;

use jazz::tools::AppId;
use jazz::tools::native_transport_connector::{
    NativeTransportConnector as _, NativeTransportRequest, NativeTransportTerminal,
};
use jazz::tools::{AppContext, ClientStorage, JazzClient};
use jazz::wire::WireTransport as _;
use jazz_native_transport::{NativeWebSocketConnector, WebSocketClientError, WebSocketTransport};
use jazz_server::{AuthConfig, BuiltServer, ServerBuilder, ServerState, StorageBackend};

async fn serve(builder: ServerBuilder) -> (String, tokio::task::JoinHandle<()>) {
    let built = builder
        .with_storage(StorageBackend::InMemory)
        .build()
        .await
        .expect("build server");
    let (url, _state, task) = serve_built(built).await;
    (url, task)
}

async fn serve_built(
    built: BuiltServer,
) -> (String, Arc<ServerState>, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let state = Arc::clone(&built.state);
    let task = tokio::spawn(async move { axum::serve(listener, built.app).await.unwrap() });
    (format!("http://{address}"), state, task)
}

fn schema() -> jazz::tools::Schema {
    jazz::tools::SchemaBuilder::new()
        .table(
            jazz::tools::TableSchema::builder("items").column("id", jazz::tools::ColumnType::Uuid),
        )
        .build()
}

fn auth(secret: &str) -> AuthConfig {
    AuthConfig {
        admin_secret: Some(secret.to_owned()),
        ..Default::default()
    }
}

fn transport_auth(secret: &str) -> jazz::tools::websocket_prelude_auth::AuthConfig {
    jazz::tools::websocket_prelude_auth::AuthConfig {
        admin_secret: Some(secret.to_owned()),
        ..Default::default()
    }
}

fn native_connector() -> Arc<NativeWebSocketConnector> {
    Arc::new(NativeWebSocketConnector)
}

#[tokio::test]
async fn edge_builder_uses_adapter_bootstrap_url_validation() {
    let result = ServerBuilder::new(AppId::from_name("edge-plaintext-bootstrap-rejected"))
        .with_storage(StorageBackend::InMemory)
        .with_auth_config(auth("admin-secret"))
        .with_native_transport_connector(native_connector())
        .with_upstream_url("http://core.example.test")
        .build()
        .await;
    let error = match result {
        Err(error) => error,
        Ok(_) => panic!("remote plaintext bootstrap must fail configuration"),
    };
    assert!(
        error.contains("plaintext ws:// bootstrap"),
        "error: {error}"
    );
}

#[tokio::test]
async fn core_websocket_transport_helper_negotiates_route_hello() {
    let app_id = AppId::from_name("adapter-route-hello");
    let (url, task) = serve(
        ServerBuilder::new(app_id)
            .with_schema(schema())
            .with_auth_config(auth("secret")),
    )
    .await;
    let client = WebSocketTransport::connect(
        url,
        app_id,
        jazz::ids::AuthorSubject::SYSTEM,
        transport_auth("secret"),
    )
    .await
    .expect("native transport negotiates public route");
    let (_, _, session) = client.negotiated_transport_metadata();
    assert!(session.is_some(), "admitted hello carries session context");
    task.abort();
}

/// The native adapter's terminal future observes an otherwise-idle socket; no
/// semantic frame or outbound retry is needed to discover the peer close.
#[tokio::test]
async fn connected_native_transport_reports_idle_websocket_closure() {
    let app_id = AppId::from_name("adapter-idle-close-terminal");
    let built = ServerBuilder::new(app_id)
        .with_schema(schema())
        .with_auth_config(auth("secret"))
        .with_storage(StorageBackend::InMemory)
        .build()
        .await
        .expect("build core");
    let (url, state, task) = serve_built(built).await;
    let connected = NativeWebSocketConnector
        .connect(NativeTransportRequest {
            server_url: url,
            app_id,
            peer_identity: jazz::ids::AuthorSubject::SYSTEM,
            auth: transport_auth("secret"),
            wake: Arc::new(|| {}),
        })
        .await
        .expect("connect idle native transport");
    let _transport = connected.transport;
    let terminal = connected.terminal;

    state.shutdown.request_shutdown();
    let shutdown_state = Arc::clone(&state);
    let shutdown = tokio::spawn(async move { shutdown_state.run_shutdown_finalization().await });
    let reason = tokio::time::timeout(Duration::from_secs(3), terminal)
        .await
        .expect("idle websocket closure resolves terminal future");
    let diagnosis = match reason {
        NativeTransportTerminal::Closed(message) => message,
        NativeTransportTerminal::Failed(error) => error.0,
    };
    assert!(
        !diagnosis.trim().is_empty(),
        "idle websocket closure returns a terminal diagnosis"
    );
    shutdown.await.expect("shutdown task");
    task.abort();
}

#[tokio::test]
async fn websocket_transport_wakes_only_for_inbound_db_work() {
    let app_id = AppId::from_name("adapter-wake-order");
    let (url, task) = serve(
        ServerBuilder::new(app_id)
            .with_schema(schema())
            .with_auth_config(auth("secret")),
    )
    .await;
    let wakes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let callback = {
        let wakes = Arc::clone(&wakes);
        Arc::new(move || {
            wakes.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        })
    };
    let mut client = WebSocketTransport::connect_with_wake(
        url,
        app_id,
        jazz::ids::AuthorSubject::SYSTEM,
        transport_auth("secret"),
        callback,
    )
    .await
    .expect("connect");
    client.send_frame(Vec::new()).expect("queue outbound");
    assert_eq!(
        wakes.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "outbound work does not wake owner"
    );
    task.abort();
}

/// Alice's unchanged public `JazzClient::connect` call retains a real online
/// session through the temporary core WebSocket compatibility adapter.
///
/// alice ──explicit native adapter composition──► websocket ──► server
#[tokio::test]
async fn public_jazz_client_connects_through_explicit_native_adapter() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let app_id = AppId::from_name("adapter-public-client-connect");
            let (url, task) = serve(
                ServerBuilder::new(app_id)
                    .with_schema(schema())
                    .with_auth_config(auth("secret")),
            )
            .await;
            let client = JazzClient::connect_with_native_transport(
                AppContext {
                    app_id,
                    client_id: None,
                    schema: schema(),
                    server_url: url,
                    data_dir: std::env::temp_dir(),
                    storage: ClientStorage::Memory,
                    storage_factory: None,
                    jwt_token: None,
                    backend_secret: None,
                    admin_secret: Some("secret".to_owned()),
                },
                native_connector(),
            )
            .await
            .expect("public client connect retains online WebSocket compatibility");
            client.shutdown().await.expect("shutdown online client");
            task.abort();
        })
        .await;
}

/// A core authority bootstraps the edge's complete catalogue before Alice's
/// first ordinary client websocket is admitted.
///
/// authority ──snapshot bootstrap──► edge ──ordinary websocket──► alice
#[tokio::test]
async fn dynamic_edge_bootstraps_authenticated_catalogue_before_first_client() {
    let app_id = AppId::from_name("dynamic-edge-bootstrap-first-client");
    let auth = auth("bootstrap-secret");
    let core = ServerBuilder::new(app_id)
        .with_schema(schema())
        .with_auth_config(auth.clone())
        .with_storage(StorageBackend::InMemory)
        .build()
        .await
        .expect("build authority core");
    let (core_url, core_state, core_task) = serve_built(core).await;
    let expected = core_state
        .trusted_catalogue_snapshot_for_test()
        .await
        .expect("read authority catalogue");

    let edge = ServerBuilder::new(app_id)
        .with_auth_config(auth.clone())
        .with_storage(StorageBackend::InMemory)
        .with_upstream_url(core_url)
        .with_native_transport_connector(native_connector())
        .build()
        .await
        .expect("build blank dynamic edge");
    assert!(
        !edge.state.has_core_server_shell_for_test(),
        "edge starts blank before its authenticated bootstrap"
    );
    let (edge_url, edge_state, edge_task) = serve_built(edge).await;

    tokio::time::timeout(Duration::from_secs(3), async {
        while !edge_state.has_core_server_shell_for_client_for_test() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("edge becomes ready from idle bootstrap");
    assert_eq!(
        edge_state
            .trusted_catalogue_snapshot_for_test()
            .await
            .expect("read adopted edge catalogue"),
        expected,
        "edge adopts the authority genesis, lineage, and policy-bearing schema exactly"
    );

    let client = WebSocketTransport::connect(
        &edge_url,
        app_id,
        jazz::ids::AuthorSubject::for_test_bytes([0x44; 16]),
        transport_auth("bootstrap-secret"),
    )
    .await;
    assert!(
        client.is_ok(),
        "ready edge admits first normal client: {client:?}"
    );

    edge_task.abort();
    core_task.abort();
}

/// After Alice's catalogue has been installed but before the edge's normal
/// upstream session attaches, Bob receives an explicit retry-later rejection.
///
/// authority ──snapshot──► edge ──✗ normal upstream
/// bob ──websocket──► edge ──RetryLater──► bob
#[tokio::test]
async fn dynamic_edge_rejects_client_until_normal_upstream_session_is_attached() {
    let app_id = AppId::from_name("dynamic-edge-client-before-upstream");
    let auth = auth("bootstrap-secret");
    let core = ServerBuilder::new(app_id)
        .with_schema(schema())
        .with_auth_config(auth.clone())
        .with_storage(StorageBackend::InMemory)
        .build()
        .await
        .expect("build authority core");
    let (_core_url, core_state, core_task) = serve_built(core).await;
    let snapshot = core_state
        .trusted_catalogue_snapshot_for_test()
        .await
        .expect("read authority catalogue");

    let edge = ServerBuilder::new(app_id)
        .with_auth_config(auth)
        .with_storage(StorageBackend::InMemory)
        .with_upstream_url("http://127.0.0.1:9")
        .with_native_transport_connector(native_connector())
        .build()
        .await
        .expect("build blank dynamic edge");
    edge.state
        .start_dynamic_edge_shell_for_test(snapshot, None)
        .expect("adopt dynamic edge catalogue");
    assert!(edge.state.has_core_server_shell_for_test());
    assert!(
        !edge.state.has_core_server_shell_for_client_for_test(),
        "raw adopted shell is not externally Ready before normal upstream admission"
    );
    let (edge_url, _edge_state, edge_task) = serve_built(edge).await;

    let error = WebSocketTransport::connect(
        edge_url,
        app_id,
        jazz::ids::AuthorSubject::for_test_bytes([0x44; 16]),
        transport_auth("bootstrap-secret"),
    )
    .await
    .expect_err("downstream admission waits for the normal upstream route");
    assert!(
        matches!(error, WebSocketClientError::ServerRejected(ref message) if message.contains("bootstrapping") && message.contains("retry shortly")),
        "unready dynamic edge must give retryable admission failure: {error}"
    );

    edge_task.abort();
    core_task.abort();
}

/// Mallory cannot use an incorrect authority credential to read a catalogue
/// through the snapshot-only bootstrap websocket.
#[tokio::test]
async fn dynamic_catalogue_bootstrap_rejects_wrong_authority_credential() {
    let app_id = AppId::from_name("dynamic-edge-bootstrap-auth-denial");
    let (core_url, task) = serve(
        ServerBuilder::new(app_id)
            .with_schema(schema())
            .with_auth_config(auth("right-secret")),
    )
    .await;
    let error = WebSocketTransport::connect_catalogue_bootstrap(
        core_url,
        app_id,
        jazz::ids::AuthorSubject::SYSTEM,
        transport_auth("wrong-secret"),
    )
    .await
    .expect_err("wrong bootstrap credential must not obtain a catalogue");
    assert!(
        matches!(error, WebSocketClientError::ServerRejected(ref message) if message.contains("AuthFailed")),
        "unexpected bootstrap auth result: {error}"
    );
    task.abort();
}

/// Mallory cannot use an ordinary privileged identity to request the edge-only
/// snapshot bootstrap exchange.
#[tokio::test]
async fn dynamic_catalogue_bootstrap_requires_the_reserved_edge_identity() {
    let app_id = AppId::from_name("dynamic-edge-bootstrap-identity-denial");
    let (core_url, task) = serve(
        ServerBuilder::new(app_id)
            .with_schema(schema())
            .with_auth_config(auth("bootstrap-secret")),
    )
    .await;
    let error = WebSocketTransport::connect_catalogue_bootstrap(
        core_url,
        app_id,
        jazz::ids::AuthorSubject::for_test_bytes([0x46; 16]),
        transport_auth("bootstrap-secret"),
    )
    .await
    .expect_err("normal privileged client identity must not request bootstrap");
    assert!(
        matches!(error, WebSocketClientError::ServerRejected(ref message) if message.contains("AuthFailed")),
        "bootstrap identity boundary returned {error}"
    );
    task.abort();
}

/// Mallory cannot substitute a generic backend credential for the dedicated
/// authority credential on the snapshot bootstrap exchange.
#[tokio::test]
async fn dynamic_catalogue_bootstrap_rejects_generic_backend_credential() {
    let app_id = AppId::from_name("dynamic-edge-bootstrap-backend-denial");
    let (core_url, task) = serve(
        ServerBuilder::new(app_id)
            .with_schema(schema())
            .with_auth_config(AuthConfig {
                admin_secret: Some("bootstrap-admin-secret".to_owned()),
                backend_secret: Some("ordinary-backend-secret".to_owned()),
                ..Default::default()
            }),
    )
    .await;
    let error = WebSocketTransport::connect_catalogue_bootstrap(
        core_url,
        app_id,
        jazz::ids::AuthorSubject::SYSTEM,
        jazz::tools::websocket_prelude_auth::AuthConfig {
            backend_secret: Some("ordinary-backend-secret".to_owned()),
            ..Default::default()
        },
    )
    .await
    .expect_err("backend credential must not read an authority catalogue");
    assert!(
        matches!(error, WebSocketClientError::ServerRejected(ref message) if message.contains("AuthFailed")),
        "generic backend credential crossed bootstrap boundary: {error}"
    );
    task.abort();
}

/// Bob receives a blank retry-later rejection while Alice's newly started edge
/// has no authenticated catalogue or normal upstream route yet.
#[tokio::test]
async fn blank_dynamic_edge_rejects_downstream_with_retry_later_until_ready() {
    let app_id = AppId::from_name("dynamic-edge-unready-downstream");
    let edge = ServerBuilder::new(app_id)
        .with_auth_config(auth("edge-secret"))
        .with_storage(StorageBackend::InMemory)
        .with_upstream_url("ws://127.0.0.1:9")
        .with_native_transport_connector(native_connector())
        .build()
        .await
        .expect("build blank edge");
    assert!(
        !edge.state.has_core_server_shell_for_test(),
        "blank edge has no downstream runtime"
    );
    let (edge_url, _edge_state, task) = serve_built(edge).await;
    let error = WebSocketTransport::connect(
        edge_url,
        app_id,
        jazz::ids::AuthorSubject::for_test_bytes([0x45; 16]),
        transport_auth("edge-secret"),
    )
    .await
    .expect_err("unready edge must not admit a downstream session");
    assert!(
        matches!(error, WebSocketClientError::ServerRejected(ref message) if message.contains("bootstrapping") && message.contains("retry shortly")),
        "unready edge must return an explicit retry-later diagnosis: {error}"
    );
    task.abort();
}
