#[cfg(not(target_arch = "wasm32"))]
use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        State,
    },
    response::IntoResponse,
    routing::get,
    Router,
};
#[cfg(not(target_arch = "wasm32"))]
use futures_util::{SinkExt, StreamExt};
#[cfg(not(target_arch = "wasm32"))]
use mini_jazz_sqlite::{
    connection::UpstreamConnectionManager, protocol::ClientMessage, Runtime, Storage,
};
#[cfg(not(target_arch = "wasm32"))]
use mini_sqlite_todo_yew::{
    native_sync::{
        decode_client_frame, encode_server_frame_with_context, trace_client_messages,
        trace_server_messages, NativeTraceContext,
    },
    todo_schema::todo_schema,
};
#[cfg(not(target_arch = "wasm32"))]
use opentelemetry::trace::TracerProvider as _;
#[cfg(not(target_arch = "wasm32"))]
use opentelemetry_otlp::{Protocol, WithExportConfig};
#[cfg(not(target_arch = "wasm32"))]
use opentelemetry_sdk::trace::SdkTracerProvider;
#[cfg(not(target_arch = "wasm32"))]
use std::{
    net::SocketAddr,
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
#[cfg(not(target_arch = "wasm32"))]
use tokio::sync::broadcast;
#[cfg(not(target_arch = "wasm32"))]
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[cfg(not(target_arch = "wasm32"))]
const SUBSCRIPTION_REFRESH_DEBOUNCE: Duration = Duration::from_millis(50);

#[cfg(not(target_arch = "wasm32"))]
const SERVICE_NAME: &str = "mini-sqlite-todo-yew-server";

#[cfg(not(target_arch = "wasm32"))]
#[derive(Clone)]
struct AppState {
    runtime: Arc<Mutex<Runtime>>,
    changes: broadcast::Sender<u64>,
    next_connection_id: Arc<AtomicU64>,
    user: String,
    sync_tracing_enabled: bool,
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _ = dotenvy::dotenv();

    let addr = std::env::var("MINI_SQLITE_TODO_SERVER_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:8787".to_owned())
        .parse::<SocketAddr>()?;
    let db_path = std::env::var("MINI_SQLITE_TODO_SERVER_DB")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("mini-sqlite-todo-yew-server.sqlite3"));
    let user = std::env::var("MINI_SQLITE_TODO_USER").unwrap_or_else(|_| "alice".to_owned());
    let sync_tracing_endpoint = sync_tracing_endpoint();
    let sync_tracing_enabled = sync_tracing_enabled() && sync_tracing_endpoint.is_some();
    let _sync_tracing = init_sync_tracing(if sync_tracing_enabled {
        sync_tracing_endpoint
    } else {
        None
    })?;
    let runtime = Runtime::open_with_schema(
        Storage::File(db_path.clone()),
        "mini-sqlite-todo-yew-native",
        &user,
        todo_schema(),
    )?;
    let state = AppState {
        runtime: Arc::new(Mutex::new(runtime)),
        changes: broadcast::channel(64).0,
        next_connection_id: Arc::new(AtomicU64::new(1)),
        user,
        sync_tracing_enabled,
    };

    let app = Router::new()
        .route("/health", get(|| async { "ok" }))
        .route("/sync", get(sync_websocket))
        .with_state(state);

    eprintln!("mini-sqlite-todo-yew sync server listening on ws://{addr}/sync");
    eprintln!("database: {}", db_path.display());

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
async fn sync_websocket(ws: WebSocketUpgrade, State(state): State<AppState>) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

#[cfg(not(target_arch = "wasm32"))]
async fn handle_socket(socket: WebSocket, state: AppState) {
    let connection_id = state.next_connection_id.fetch_add(1, Ordering::Relaxed);
    let (schema_fingerprint, policy_fingerprint) = {
        let Ok(runtime) = state.runtime.lock() else {
            return;
        };
        (
            runtime.local_schema_fingerprint(),
            runtime.local_policy_fingerprint(),
        )
    };
    let mut upstream = UpstreamConnectionManager::new_authenticated(
        "mini-sqlite-todo-yew-native-session",
        "mini-sqlite-todo-yew-native",
        schema_fingerprint,
        policy_fingerprint,
        state.user.clone(),
    );
    let (mut sender, mut receiver) = socket.split();
    let mut changes = state.changes.subscribe();
    let mut last_trace_context = None::<NativeTraceContext>;

    loop {
        tokio::select! {
            message = receiver.next() => {
                let Some(message) = message else {
                    break;
                };
                let Ok(message) = message else {
                    break;
                };
                let Message::Text(encoded) = message else {
                    if matches!(message, Message::Close(_)) {
                        break;
                    }
                    continue;
                };
                let frame = match decode_client_frame(&encoded) {
                    Ok(frame) => frame,
                    Err(error) => {
                        eprintln!("invalid native sync frame: {error}");
                        break;
                    }
                };
                if state.sync_tracing_enabled {
                    trace_client_messages(
                        "server.receive",
                        frame.trace_context.as_ref(),
                        &frame.client_messages,
                    );
                }
                if frame.trace_context.is_some() {
                    last_trace_context = frame.trace_context.clone();
                }
                let should_notify = frame.client_messages.iter().any(|message| {
                    matches!(message, ClientMessage::UploadTx { .. })
                });
                let server_messages = {
                    let Ok(mut runtime) = state.runtime.lock() else {
                        break;
                    };
                    match upstream.receive(&mut runtime, frame.client_messages) {
                        Ok(server_messages) => server_messages,
                        Err(error) => {
                            eprintln!("native sync protocol error: {error}");
                            break;
                        }
                    }
                };
                if !server_messages.is_empty() && send_server_messages(
                    &mut sender,
                    server_messages,
                    state.sync_tracing_enabled,
                    last_trace_context.as_ref(),
                ).await.is_err() {
                    break;
                }
                if should_notify {
                    let _ = state.changes.send(connection_id);
                }
            }
            change = changes.recv() => {
                match change {
                    Ok(origin_connection_id) if origin_connection_id == connection_id => {
                        continue;
                    }
                    Ok(_) | Err(broadcast::error::RecvError::Lagged(_)) => {
                        tokio::time::sleep(SUBSCRIPTION_REFRESH_DEBOUNCE).await;
                        while changes.try_recv().is_ok() {}
                        let server_messages = {
                            let Ok(runtime) = state.runtime.lock() else {
                                break;
                            };
                            match upstream.refresh_active_subscriptions(&runtime) {
                                Ok(server_messages) => server_messages,
                                Err(error) => {
                                    eprintln!("native sync protocol error: {error}");
                                    break;
                                }
                            }
                        };
                        if !server_messages.is_empty() && send_server_messages(
                            &mut sender,
                            server_messages,
                            state.sync_tracing_enabled,
                            last_trace_context.as_ref(),
                        ).await.is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
async fn send_server_messages(
    sender: &mut futures_util::stream::SplitSink<WebSocket, Message>,
    server_messages: Vec<mini_jazz_sqlite::protocol::ServerMessage>,
    sync_tracing_enabled: bool,
    trace_context: Option<&NativeTraceContext>,
) -> Result<(), ()> {
    if sync_tracing_enabled {
        trace_server_messages("server.send", trace_context, &server_messages);
    }
    let encoded = encode_server_frame_with_context(server_messages, trace_context.cloned())
        .map_err(|_| ())?;
    sender.send(Message::Text(encoded)).await.map_err(|_| ())
}

#[cfg(not(target_arch = "wasm32"))]
fn sync_tracing_enabled() -> bool {
    std::env::var("MINI_SQLITE_TODO_SYNC_TRACE")
        .map(|value| !matches!(value.as_str(), "0" | "false" | "FALSE" | "off" | "OFF"))
        .unwrap_or(true)
}

#[cfg(not(target_arch = "wasm32"))]
struct SyncTracingGuard {
    provider: Option<SdkTracerProvider>,
}

#[cfg(not(target_arch = "wasm32"))]
impl SyncTracingGuard {
    fn disabled() -> Self {
        Self { provider: None }
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl Drop for SyncTracingGuard {
    fn drop(&mut self) {
        if let Some(provider) = self.provider.take() {
            let _ = provider.shutdown();
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn init_sync_tracing(
    endpoint: Option<String>,
) -> Result<SyncTracingGuard, Box<dyn std::error::Error>> {
    let Some(endpoint) = endpoint else {
        return Ok(SyncTracingGuard::disabled());
    };
    let filter = tracing_subscriber::EnvFilter::new("mini_sqlite_todo_yew::native_sync=info");
    let span_exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .with_protocol(Protocol::HttpJson)
        .with_endpoint(normalize_otlp_traces_endpoint(&endpoint))
        .build()?;
    let provider = SdkTracerProvider::builder()
        .with_resource(
            opentelemetry_sdk::Resource::builder()
                .with_service_name(SERVICE_NAME)
                .with_attribute(opentelemetry::KeyValue::new(
                    "service.version",
                    env!("CARGO_PKG_VERSION"),
                ))
                .with_attribute(opentelemetry::KeyValue::new(
                    "deployment.environment.name",
                    "local",
                ))
                .build(),
        )
        .with_batch_exporter(span_exporter)
        .build();
    let tracer = provider.tracer(SERVICE_NAME);
    let trace_layer = tracing_opentelemetry::layer().with_tracer(tracer);

    tracing_subscriber::registry()
        .with(filter)
        .with(trace_layer)
        .try_init()?;

    Ok(SyncTracingGuard {
        provider: Some(provider),
    })
}

#[cfg(not(target_arch = "wasm32"))]
fn sync_tracing_endpoint() -> Option<String> {
    std::env::var("MINI_SQLITE_TODO_OTLP_ENDPOINT")
        .or_else(|_| std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT"))
        .ok()
        .map(|endpoint| endpoint.trim().to_owned())
        .filter(|endpoint| !endpoint.is_empty())
}

#[cfg(not(target_arch = "wasm32"))]
fn normalize_otlp_traces_endpoint(endpoint: &str) -> String {
    let endpoint = endpoint.trim().trim_end_matches('/');
    if endpoint.ends_with("/v1/traces") {
        endpoint.to_owned()
    } else if let Some(base) = endpoint.strip_suffix("/v1/logs") {
        format!("{base}/v1/traces")
    } else {
        format!("{endpoint}/v1/traces")
    }
}

#[cfg(target_arch = "wasm32")]
fn main() {}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use std::sync::{Mutex, OnceLock};

    fn env_lock() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(())).lock().unwrap()
    }

    #[test]
    fn reads_sync_tracing_endpoint_from_env() {
        let _lock = env_lock();
        std::env::set_var("MINI_SQLITE_TODO_OTLP_ENDPOINT", " http://127.0.0.1:4318 ");
        std::env::remove_var("OTEL_EXPORTER_OTLP_ENDPOINT");
        assert_eq!(
            sync_tracing_endpoint().as_deref(),
            Some("http://127.0.0.1:4318")
        );
        std::env::remove_var("MINI_SQLITE_TODO_OTLP_ENDPOINT");
    }

    #[test]
    fn normalizes_otlp_trace_endpoint() {
        assert_eq!(
            normalize_otlp_traces_endpoint("http://127.0.0.1:54418"),
            "http://127.0.0.1:54418/v1/traces"
        );
        assert_eq!(
            normalize_otlp_traces_endpoint("http://127.0.0.1:54418/v1/logs"),
            "http://127.0.0.1:54418/v1/traces"
        );
        assert_eq!(
            normalize_otlp_traces_endpoint("http://127.0.0.1:54418/v1/traces"),
            "http://127.0.0.1:54418/v1/traces"
        );
    }
}
