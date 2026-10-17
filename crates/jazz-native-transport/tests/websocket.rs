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
use tokio::sync::oneshot;

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
        .table(jazz::tools::TableSchema::builder("items"))
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

/// Accept the TCP connection but never answer its HTTP upgrade request.
async fn serve_stalled_http_upgrade() -> (String, tokio::task::JoinHandle<()>, oneshot::Receiver<()>)
{
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind stalled upgrade listener");
    let address = listener.local_addr().expect("stalled listener address");
    let (accepted_tx, accepted_rx) = oneshot::channel();
    let task = tokio::spawn(async move {
        let (_stream, _) = listener.accept().await.expect("accept stalled upgrade");
        let _ = accepted_tx.send(());
        std::future::pending::<()>().await;
    });
    (format!("http://{address}"), task, accepted_rx)
}

/// Spend most of the connection budget before completing the HTTP upgrade,
/// then keep the WebSocket open without returning the wire hello.
async fn serve_stalled_server_hello() -> (
    String,
    tokio::task::JoinHandle<()>,
    oneshot::Receiver<()>,
    oneshot::Receiver<()>,
) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind stalled hello listener");
    let address = listener
        .local_addr()
        .expect("stalled hello listener address");
    let (accepted_tx, accepted_rx) = oneshot::channel();
    let (upgraded_tx, upgraded_rx) = oneshot::channel();
    let task = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept stalled hello");
        let _ = accepted_tx.send(());
        tokio::time::sleep(Duration::from_secs(3)).await;
        let _ws = tokio_tungstenite::accept_async(stream)
            .await
            .expect("complete delayed websocket upgrade");
        let _ = upgraded_tx.send(());
        std::future::pending::<()>().await;
    });
    (format!("http://{address}"), task, accepted_rx, upgraded_rx)
}

#[tokio::test]
async fn stalled_http_upgrade_reports_handshake_timeout() {
    let app_id = AppId::from_name("adapter-stalled-http-upgrade");
    let (url, task, accepted) = serve_stalled_http_upgrade().await;
    let mut connection = tokio::spawn(async move {
        WebSocketTransport::connect(
            url,
            app_id,
            jazz::ids::AuthorSubject::SYSTEM,
            transport_auth("secret"),
        )
        .await
    });
    tokio::time::timeout(Duration::from_secs(1), accepted)
        .await
        .expect("client opens loopback TCP connection")
        .expect("stalled upgrade server remains alive");
    let result = match tokio::time::timeout(Duration::from_secs(6), &mut connection).await {
        Ok(result) => result.expect("connector task"),
        Err(_) => {
            connection.abort();
            task.abort();
            panic!("establishment timeout remains bounded");
        }
    };
    task.abort();

    let error = result.expect_err("stalled upgrade must fail");
    assert!(
        matches!(&error, WebSocketClientError::HandshakeTimeout),
        "stalled upgrade must report typed handshake timeout: {error}"
    );
}

#[tokio::test]
async fn stalled_server_hello_uses_the_original_establishment_deadline() {
    let app_id = AppId::from_name("adapter-stalled-server-hello");
    let (url, task, accepted, upgraded) = serve_stalled_server_hello().await;
    let mut connection = tokio::spawn(async move {
        WebSocketTransport::connect(
            url,
            app_id,
            jazz::ids::AuthorSubject::SYSTEM,
            transport_auth("secret"),
        )
        .await
    });
    tokio::time::timeout(Duration::from_secs(1), accepted)
        .await
        .expect("client opens loopback TCP connection")
        .expect("stalled hello server remains alive");
    tokio::time::timeout(Duration::from_secs(4), upgraded)
        .await
        .expect("server completes delayed WebSocket upgrade")
        .expect("stalled hello server remains alive");

    let result = match tokio::time::timeout(Duration::from_secs(3), &mut connection).await {
        Ok(result) => result.expect("connector task"),
        Err(_) => {
            connection.abort();
            task.abort();
            panic!("server hello must share the original establishment deadline");
        }
    };
    task.abort();

    let error = result.expect_err("missing server hello must fail");
    assert!(
        matches!(&error, WebSocketClientError::HandshakeTimeout),
        "stalled server hello must report typed handshake timeout: {error}"
    );
}

#[tokio::test]
async fn refused_tcp_connection_remains_connect_error() {
    // Reserve a loopback port, then release it without ever accepting. This
    // distinguishes an immediate TCP failure from the bounded HTTP upgrade.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind refusal test listener");
    let address = listener
        .local_addr()
        .expect("refusal test listener address");
    drop(listener);

    let error = WebSocketTransport::connect(
        format!("http://{address}"),
        AppId::from_name("adapter-refused-tcp-connection"),
        jazz::ids::AuthorSubject::SYSTEM,
        transport_auth("secret"),
    )
    .await
    .expect_err("closed loopback port must refuse TCP connection");
    assert!(
        matches!(error, WebSocketClientError::Connect(_)),
        "immediate TCP refusal must retain its connect classification: {error}"
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
            requested_link:
                jazz::tools::native_transport_connector::NativeTransportLink::OrdinarySession,
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
        NativeTransportTerminal::PeerClosed(message) => message,
        other => panic!("peer shutdown must retain its peer-close terminal outcome, got {other:?}"),
    };
    assert!(
        !diagnosis.trim().is_empty(),
        "idle websocket closure returns a terminal diagnosis"
    );
    shutdown.await.expect("shutdown task");
    task.abort();
}

#[tokio::test]
async fn connected_native_transport_reports_owner_drop() {
    let app_id = AppId::from_name("adapter-owner-drop-terminal");
    let built = ServerBuilder::new(app_id)
        .with_schema(schema())
        .with_auth_config(auth("secret"))
        .with_storage(StorageBackend::InMemory)
        .build()
        .await
        .expect("build core");
    let (url, _state, task) = serve_built(built).await;
    let connected = NativeWebSocketConnector
        .connect(NativeTransportRequest {
            requested_link:
                jazz::tools::native_transport_connector::NativeTransportLink::OrdinarySession,
            server_url: url,
            app_id,
            peer_identity: jazz::ids::AuthorSubject::SYSTEM,
            auth: transport_auth("secret"),
            wake: Arc::new(|| {}),
        })
        .await
        .expect("connect native transport");
    let transport = connected.transport;
    let terminal = connected.terminal;

    drop(transport);
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(3), terminal)
            .await
            .expect("owner drop resolves terminal future"),
        NativeTransportTerminal::OwnerDropped
    );
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
                    account_id: None,
                    jwt_token: None,
                    backend_secret: None,
                    admin_secret: Some("secret".to_owned()),
                },
                Arc::new(NativeWebSocketConnector),
            )
            .await
            .expect("public client connect retains online WebSocket compatibility");
            client.shutdown().await.expect("shutdown online client");
            task.abort();
        })
        .await;
}
