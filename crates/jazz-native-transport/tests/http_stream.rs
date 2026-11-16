//! The HTTP stream fallback against an in-process core, behind a stand-in
//! for TS `createJazz`'s stream routes: a backend that can't upgrade sockets
//! (426 with `x-jazz-stream: 1`, as `createJazz` answers where it can't) and
//! relays each stream session to the core's socket, as `createJazz` relays
//! its client sessions. The real `createJazz` is exercised by
//! `packages/jazz-tools/src/backend/rust-http-stream.e2e-local.integration.test.ts`.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use futures::{SinkExt as _, StreamExt as _};
use jazz::tools::AppId;
use jazz::tools::native_transport_connector::{
    NativeTransportConnector as _, NativeTransportError,
};
use jazz::tools::{AppContext, ClientStorage, JazzClient};
use jazz_native_transport::{HttpStreamFallbackConnector, HttpStreamTransportKind};
use jazz_server::{AuthConfig, ServerBuilder, StorageBackend};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

fn record(kind: u8, payload: &[u8]) -> Vec<u8> {
    let mut out = vec![kind];
    out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    out.extend_from_slice(payload);
    out
}

type CoreSink = futures::stream::SplitSink<
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
    Message,
>;

/// How the stand-in backend answers a socket path.
#[derive(Clone, Copy)]
enum Upgrades {
    /// It can't upgrade sockets, and says so (`createJazz` without a server attached).
    StreamInstead,
    /// It says it upgrades them, but something in between refuses each one.
    BrokenInBetween,
    /// Not a `createJazz` backend: no stream, no 426.
    NoStream,
}

struct Backend {
    core: String,
    upgrades: Upgrades,
    sessions: Mutex<HashMap<String, (String, Arc<tokio::sync::Mutex<CoreSink>>)>>,
    opened: AtomicUsize,
    sends: AtomicUsize,
    deleted: AtomicUsize,
}

async fn socket_path(State(backend): State<Arc<Backend>>) -> Response {
    match backend.upgrades {
        Upgrades::StreamInstead => (
            StatusCode::UPGRADE_REQUIRED,
            [("x-jazz-stream", "1")],
            "use the HTTP stream",
        )
            .into_response(),
        Upgrades::BrokenInBetween => StatusCode::UPGRADE_REQUIRED.into_response(),
        Upgrades::NoStream => StatusCode::NOT_FOUND.into_response(),
    }
}

/// An upgrade that a proxy in between refuses.
async fn refused_upgrade(
    State(backend): State<Arc<Backend>>,
    method: Method,
    headers: HeaderMap,
) -> Response {
    if method == Method::GET && headers.contains_key("upgrade") {
        if let Upgrades::BrokenInBetween = backend.upgrades {
            return StatusCode::BAD_GATEWAY.into_response();
        }
    }
    socket_path(State(backend)).await
}

async fn open_stream(State(backend): State<Arc<Backend>>, Path(app): Path<String>) -> Response {
    if let Upgrades::NoStream = backend.upgrades {
        return StatusCode::NOT_FOUND.into_response();
    }
    let core_url = format!("{}/apps/{app}/ws", backend.core.replace("http://", "ws://"));
    let Ok((ws, _)) = tokio_tungstenite::connect_async(core_url).await else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    let (sink, mut source) = ws.split();
    let id = uuid::Uuid::new_v4().to_string();
    let key = uuid::Uuid::new_v4().simple().to_string();
    backend.sessions.lock().unwrap().insert(
        id.clone(),
        (key.clone(), Arc::new(tokio::sync::Mutex::new(sink))),
    );
    backend.opened.fetch_add(1, Ordering::SeqCst);
    let (tx, rx) = mpsc::channel::<Result<Vec<u8>, std::io::Error>>(16);
    let session = serde_json::json!({ "id": id, "key": key }).to_string();
    tx.send(Ok(record(0, session.as_bytes()))).await.unwrap();
    tokio::spawn(async move {
        while let Some(message) = source.next().await {
            match message {
                Ok(Message::Binary(bytes)) => {
                    if tx.send(Ok(record(1, &bytes))).await.is_err() {
                        return;
                    }
                }
                Ok(Message::Close(_)) | Err(_) => break,
                Ok(_) => {}
            }
        }
        let _ = tx
            .send(Ok(record(2, br#"{"code":1000,"reason":"core closed"}"#)))
            .await;
    });
    let body = Body::from_stream(tokio_stream_from(rx));
    ([("content-type", "application/octet-stream")], body).into_response()
}

fn tokio_stream_from<T: Send + 'static>(
    mut rx: mpsc::Receiver<T>,
) -> impl futures::Stream<Item = T> + Send + 'static {
    futures::stream::poll_fn(move |cx| rx.poll_recv(cx))
}

fn owned_sink(
    backend: &Backend,
    id: &str,
    headers: &HeaderMap,
) -> Result<Arc<tokio::sync::Mutex<CoreSink>>, StatusCode> {
    let sessions = backend.sessions.lock().unwrap();
    let (key, sink) = sessions.get(id).ok_or(StatusCode::NOT_FOUND)?;
    let given = headers
        .get("x-jazz-stream-key")
        .and_then(|value| value.to_str().ok());
    if given != Some(key.as_str()) {
        return Err(StatusCode::FORBIDDEN);
    }
    Ok(Arc::clone(sink))
}

async fn stream_send(
    State(backend): State<Arc<Backend>>,
    Path((_app, id)): Path<(String, String)>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    let sink = match owned_sink(&backend, &id, &headers) {
        Ok(sink) => sink,
        Err(status) => return status.into_response(),
    };
    backend.sends.fetch_add(1, Ordering::SeqCst);
    let mut rest = &body[..];
    let mut sink = sink.lock().await;
    while rest.len() >= 5 {
        let length = u32::from_be_bytes([rest[1], rest[2], rest[3], rest[4]]) as usize;
        if rest[0] != 1 || rest.len() < 5 + length {
            return StatusCode::BAD_REQUEST.into_response();
        }
        if sink
            .send(Message::Binary(rest[5..5 + length].to_vec().into()))
            .await
            .is_err()
        {
            return StatusCode::GONE.into_response();
        }
        rest = &rest[5 + length..];
    }
    if !rest.is_empty() {
        return StatusCode::BAD_REQUEST.into_response();
    }
    StatusCode::NO_CONTENT.into_response()
}

async fn stream_end(
    State(backend): State<Arc<Backend>>,
    Path((_app, id)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    let sink = match owned_sink(&backend, &id, &headers) {
        Ok(sink) => sink,
        Err(status) => return status.into_response(),
    };
    backend.sessions.lock().unwrap().remove(&id);
    backend.deleted.fetch_add(1, Ordering::SeqCst);
    let _ = sink.lock().await.close().await;
    StatusCode::NO_CONTENT.into_response()
}

struct Served {
    url: String,
    backend: Arc<Backend>,
    tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl Drop for Served {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}

async fn serve(app_id: AppId, schema: jazz::tools::Schema, upgrades: Upgrades) -> Served {
    let built = ServerBuilder::new(app_id)
        .with_schema(schema)
        .with_auth_config(AuthConfig {
            admin_secret: Some("secret".to_owned()),
            ..Default::default()
        })
        .with_storage(StorageBackend::InMemory)
        .build()
        .await
        .expect("build server");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let core = format!("http://{}", listener.local_addr().unwrap());
    let core_task = tokio::spawn(async move { axum::serve(listener, built.app).await.unwrap() });

    let backend = Arc::new(Backend {
        core,
        upgrades,
        sessions: Mutex::default(),
        opened: AtomicUsize::new(0),
        sends: AtomicUsize::new(0),
        deleted: AtomicUsize::new(0),
    });
    let router = axum::Router::new()
        .route("/jazz/apps/{app}/ws", axum::routing::any(refused_upgrade))
        .route("/jazz/apps/{app}/stream", axum::routing::post(open_stream))
        .route(
            "/jazz/apps/{app}/stream/{id}",
            axum::routing::post(stream_send).delete(stream_end),
        )
        .with_state(Arc::clone(&backend));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/jazz", listener.local_addr().unwrap());
    let backend_task = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    Served {
        url,
        backend,
        tasks: vec![core_task, backend_task],
    }
}

fn schema() -> jazz::tools::Schema {
    jazz::tools::SchemaBuilder::new()
        .table(
            jazz::tools::TableSchema::builder("items")
                .column("title", jazz::tools::ColumnType::Text),
        )
        .build()
}

fn context(app_id: AppId, server_url: String) -> AppContext {
    AppContext {
        app_id,
        client_id: None,
        schema: schema(),
        server_url,
        data_dir: std::env::temp_dir(),
        storage: ClientStorage::Memory,
        account_id: None,
        jwt_token: None,
        backend_secret: None,
        backend_session: None,
        bundle: None,
        admin_secret: Some("secret".to_owned()),
        migrations: Default::default(),
    }
}

async fn insert_and_read_back(client: &JazzClient, title: &str) {
    let (id, _, tx) = client
        .insert(
            "items",
            HashMap::from([(
                "title".to_owned(),
                jazz::tools::Value::Text(title.to_owned()),
            )]),
        )
        .expect("insert");
    tokio::time::timeout(
        Duration::from_secs(10),
        client.wait_for_transaction(
            tx.expect("standalone insert commits"),
            jazz::tools::DurabilityTier::GlobalServer,
        ),
    )
    .await
    .expect("settles in time")
    .expect("the core accepts the insert");
    let rows = tokio::time::timeout(
        Duration::from_secs(10),
        client.query(
            jazz::query::Query::from("items"),
            jazz::tools::ReadTier::Remote,
        ),
    )
    .await
    .expect("remote read answers in time")
    .expect("remote read");
    assert!(
        rows.iter().any(|row| row.key.row_id() == Some(id)
            && row.get("title") == Some(&jazz::tools::Value::Text(title.to_owned()))),
        "the row came back from the core"
    );
}

#[tokio::test]
async fn a_backend_that_cannot_upgrade_sockets_syncs_over_the_stream() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let app_id = AppId::from_name("http-stream-fallback-sync");
            let served = serve(app_id, schema(), Upgrades::StreamInstead).await;
            let connector = Arc::new(HttpStreamFallbackConnector::new());
            let client = JazzClient::connect_with_native_transport(
                context(app_id, served.url.clone()),
                connector.clone(),
            )
            .await
            .expect("connects over the stream");
            assert!(client.is_connected());
            assert_eq!(
                connector.last_transport(),
                Some(HttpStreamTransportKind::HttpStream)
            );
            insert_and_read_back(&client, "over the stream").await;
            assert_eq!(served.backend.opened.load(Ordering::SeqCst), 1);
            assert!(served.backend.sends.load(Ordering::SeqCst) >= 1);

            client.shutdown().await.expect("shutdown");
            // Closing ends the session on the backend.
            tokio::time::timeout(Duration::from_secs(5), async {
                while served.backend.deleted.load(Ordering::SeqCst) == 0 {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            })
            .await
            .expect("the session is DELETEd");
        })
        .await;
}

#[tokio::test]
async fn upgrades_that_keep_failing_in_between_switch_to_the_stream() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let app_id = AppId::from_name("http-stream-fallback-in-between");
            let served = serve(app_id, schema(), Upgrades::BrokenInBetween).await;
            let connector = HttpStreamFallbackConnector::new();
            let request = || jazz::tools::native_transport_connector::NativeTransportRequest {
                requested_link: Default::default(),
                server_url: served.url.clone(),
                app_id,
                peer_identity: jazz::ids::AuthorSubject::SYSTEM,
                auth: jazz::tools::websocket_prelude_auth::AuthConfig {
                    admin_secret: Some("secret".to_owned()),
                    ..Default::default()
                },
                authorization: None,
                bundle: None,
                wake: Arc::new(|| {}),
            };
            // A restart or a draining instance fails a socket once or twice:
            // those fail as sockets do.
            for _ in 1..jazz_native_transport::UPGRADE_FAILURES_BEFORE_STREAM {
                let Err(error) = connector.connect(request()).await else {
                    panic!("the upgrade is refused in between");
                };
                assert!(
                    matches!(error, NativeTransportError::Retryable(_)),
                    "{error:?}"
                );
            }
            assert_eq!(served.backend.opened.load(Ordering::SeqCst), 0);
            let connected = connector
                .connect(request())
                .await
                .expect("the third failure in a row switches to the stream");
            assert_eq!(
                connector.last_transport(),
                Some(HttpStreamTransportKind::HttpStream)
            );
            assert_eq!(served.backend.opened.load(Ordering::SeqCst), 1);
            drop(connected);
            // The switch lasts: the next session opens the stream at once.
            let _again = connector.connect(request()).await.expect("reconnects");
            assert_eq!(served.backend.opened.load(Ordering::SeqCst), 2);
        })
        .await;
}

#[tokio::test]
async fn a_server_without_the_stream_fails_as_a_socket_would() {
    let app_id = AppId::from_name("http-stream-fallback-none");
    let served = serve(app_id, schema(), Upgrades::NoStream).await;
    let connector = HttpStreamFallbackConnector::new();
    for _ in 0..4 {
        let error = connector
            .connect(
                jazz::tools::native_transport_connector::NativeTransportRequest {
                    requested_link: Default::default(),
                    server_url: served.url.clone(),
                    app_id,
                    peer_identity: jazz::ids::AuthorSubject::SYSTEM,
                    auth: Default::default(),
                    authorization: None,
                    bundle: None,
                    wake: Arc::new(|| {}),
                },
            )
            .await
            .err()
            .expect("no socket, no stream");
        assert!(
            error.to_string().contains("websocket"),
            "fails as the socket did: {error}"
        );
    }
    assert_eq!(served.backend.opened.load(Ordering::SeqCst), 0);
}
