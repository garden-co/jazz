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
    connection::UpstreamConnectionManager,
    protocol::{ClientMessage, MessageId, ReplayCursor, ServerMessage},
    sync::Bundle,
    Runtime, Storage,
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
const SYNC_TRACE_TARGET: &str = "mini_sqlite_todo_yew::native_sync";

#[cfg(not(target_arch = "wasm32"))]
#[derive(Clone)]
struct AppState {
    runtime: Arc<Mutex<Runtime>>,
    changes: broadcast::Sender<SyncChange>,
    next_connection_id: Arc<AtomicU64>,
    user: String,
    sync_tracing_enabled: bool,
}

#[cfg(not(target_arch = "wasm32"))]
#[derive(Clone, Debug)]
struct SyncChange {
    origin_connection_id: u64,
    trace_context: Option<NativeTraceContext>,
    bundles: Vec<Bundle>,
    requires_subscription_refresh: bool,
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
    let mut next_push_message_id = u64::MAX;

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
                let upload_tx_ids = frame.client_messages.iter().filter_map(upload_tx_id).collect::<Vec<_>>();
                let should_notify = !upload_tx_ids.is_empty();
                let (server_messages, upload_bundles) = {
                    let Ok(mut runtime) = state.runtime.lock() else {
                        break;
                    };
                    let server_messages = match upstream.receive(&mut runtime, frame.client_messages) {
                        Ok(server_messages) => server_messages,
                        Err(error) => {
                            eprintln!("native sync protocol error: {error}");
                            break;
                        }
                    };
                    let upload_bundles = upload_tx_ids
                        .iter()
                        .filter_map(|tx_id| export_upload_bundle(&runtime, tx_id))
                        .collect::<Vec<_>>();
                    (server_messages, upload_bundles)
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
                    let requires_subscription_refresh = upload_bundles.len() != upload_tx_ids.len();
                    let _ = state.changes.send(SyncChange {
                        origin_connection_id: connection_id,
                        trace_context: frame.trace_context.clone(),
                        bundles: upload_bundles,
                        requires_subscription_refresh,
                    });
                }
            }
            change = changes.recv() => {
                match change {
                    Ok(change) if change.origin_connection_id == connection_id => {
                        continue;
                    }
                    Ok(change) => {
                        let mut refresh_trace_context = change.trace_context;
                        let mut push_bundles = change.bundles;
                        let mut requires_subscription_refresh = change.requires_subscription_refresh;
                        tokio::time::sleep(SUBSCRIPTION_REFRESH_DEBOUNCE).await;
                        while let Ok(change) = changes.try_recv() {
                            keep_refresh_change(
                                &mut refresh_trace_context,
                                &mut push_bundles,
                                &mut requires_subscription_refresh,
                                connection_id,
                                change,
                            );
                        }
                        let plan = change_broadcast_plan(
                            !push_bundles.is_empty(),
                            requires_subscription_refresh,
                        );
                        trace_change_broadcast_plan(
                            &plan,
                            push_bundles.len(),
                            requires_subscription_refresh,
                        );
                        let trace_context = selected_refresh_trace_context(
                            &refresh_trace_context,
                            &last_trace_context,
                        )
                        .cloned();
                        if plan.send_push_bundles {
                            let server_messages =
                                push_server_messages(push_bundles, &mut next_push_message_id);
                            if !server_messages.is_empty() && send_server_messages(
                                &mut sender,
                                server_messages,
                                state.sync_tracing_enabled,
                                trace_context.as_ref(),
                            ).await.is_err() {
                                break;
                            }
                        }
                        if plan.refresh_active_subscriptions {
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
                                trace_context.as_ref(),
                            ).await.is_err() {
                                break;
                            }
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        let mut refresh_trace_context = None;
                        tokio::time::sleep(SUBSCRIPTION_REFRESH_DEBOUNCE).await;
                        while let Ok(change) = changes.try_recv() {
                            keep_refresh_trace_context(
                                &mut refresh_trace_context,
                                connection_id,
                                change,
                            );
                        }
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
                            selected_refresh_trace_context(
                                &refresh_trace_context,
                                &last_trace_context,
                            ),
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
fn selected_refresh_trace_context<'a>(
    refresh_trace_context: &'a Option<NativeTraceContext>,
    last_trace_context: &'a Option<NativeTraceContext>,
) -> Option<&'a NativeTraceContext> {
    refresh_trace_context
        .as_ref()
        .or(last_trace_context.as_ref())
}

#[cfg(not(target_arch = "wasm32"))]
fn keep_refresh_trace_context(
    refresh_trace_context: &mut Option<NativeTraceContext>,
    connection_id: u64,
    change: SyncChange,
) {
    if change.origin_connection_id != connection_id && change.trace_context.is_some() {
        *refresh_trace_context = change.trace_context;
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn keep_refresh_change(
    refresh_trace_context: &mut Option<NativeTraceContext>,
    push_bundles: &mut Vec<Bundle>,
    requires_subscription_refresh: &mut bool,
    connection_id: u64,
    change: SyncChange,
) {
    if change.origin_connection_id == connection_id {
        return;
    }
    if change.trace_context.is_some() {
        *refresh_trace_context = change.trace_context;
    }
    push_bundles.extend(change.bundles);
    *requires_subscription_refresh |= change.requires_subscription_refresh;
}

#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug, Eq, PartialEq)]
struct ChangeBroadcastPlan {
    send_push_bundles: bool,
    refresh_active_subscriptions: bool,
}

#[cfg(not(target_arch = "wasm32"))]
fn change_broadcast_plan(
    has_push_bundles: bool,
    requires_subscription_refresh: bool,
) -> ChangeBroadcastPlan {
    ChangeBroadcastPlan {
        send_push_bundles: has_push_bundles,
        refresh_active_subscriptions: requires_subscription_refresh || !has_push_bundles,
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn export_upload_bundle(runtime: &Runtime, tx_id: &str) -> Option<Bundle> {
    match runtime.export_transaction(tx_id) {
        Ok(bundle) => {
            trace_upload_bundle_export(tx_id, "ok", None, Some(&bundle));
            Some(bundle)
        }
        Err(error) => {
            let message = error.to_string();
            trace_upload_bundle_export(tx_id, "error", Some(message.as_str()), None);
            None
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn trace_upload_bundle_export(
    tx_id: &str,
    status: &'static str,
    error: Option<&str>,
    bundle: Option<&Bundle>,
) {
    let span = tracing::info_span!(
        target: SYNC_TRACE_TARGET,
        "sync.server.upload_bundle_export",
        sync_tx_id = tx_id,
        sync_export_status = status,
        sync_export_error = tracing::field::Empty,
        sync_bundle_tx_count = bundle.map(|bundle| bundle.txs.len()).unwrap_or_default(),
        sync_bundle_row_count = bundle.map(|bundle| bundle.rows.len()).unwrap_or_default(),
        sync_bundle_history_count = bundle.map(|bundle| bundle.history.len()).unwrap_or_default(),
    );
    if let Some(error) = error {
        span.record("sync_export_error", error);
    }
    let _entered = span.enter();
}

#[cfg(not(target_arch = "wasm32"))]
fn trace_change_broadcast_plan(
    plan: &ChangeBroadcastPlan,
    push_bundle_count: usize,
    requires_subscription_refresh: bool,
) {
    let span = tracing::info_span!(
        target: SYNC_TRACE_TARGET,
        "sync.server.change_broadcast_plan",
        sync_push_bundle_count = push_bundle_count,
        sync_requires_subscription_refresh = requires_subscription_refresh,
        sync_send_push_bundles = plan.send_push_bundles,
        sync_refresh_active_subscriptions = plan.refresh_active_subscriptions,
    );
    let _entered = span.enter();
}

#[cfg(not(target_arch = "wasm32"))]
fn push_server_messages(bundles: Vec<Bundle>, next_message_id: &mut u64) -> Vec<ServerMessage> {
    bundles
        .into_iter()
        .map(|bundle| {
            let id = *next_message_id;
            *next_message_id = next_message_id.saturating_sub(1);
            ServerMessage::Data {
                message_id: MessageId(id),
                subscription_id: None,
                cursor: ReplayCursor(id),
                bundle,
            }
        })
        .collect()
}

#[cfg(not(target_arch = "wasm32"))]
fn upload_tx_id(message: &ClientMessage) -> Option<String> {
    match message {
        ClientMessage::UploadTx { tx, .. } => Some(tx.tx_id.clone()),
        _ => None,
    }
}

#[cfg(not(target_arch = "wasm32"))]
async fn send_server_messages(
    sender: &mut futures_util::stream::SplitSink<WebSocket, Message>,
    server_messages: Vec<ServerMessage>,
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
    use std::collections::BTreeMap;
    use std::sync::{Mutex, OnceLock};

    fn test_bundle(schema_fingerprint: &str) -> mini_jazz_sqlite::sync::Bundle {
        mini_jazz_sqlite::sync::Bundle {
            protocol_version: mini_jazz_sqlite::sync::BUNDLE_PROTOCOL_VERSION,
            schema_fingerprint: schema_fingerprint.to_owned(),
            policy_fingerprint: "policy".to_owned(),
            branches: Vec::new(),
            txs: Vec::new(),
            reads: Vec::new(),
            query_reads: Vec::new(),
            rows: Vec::new(),
            obfuscated: Vec::new(),
            history: Vec::new(),
        }
    }

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

    #[test]
    fn refresh_uses_origin_trace_context_before_socket_last_context() {
        let origin = NativeTraceContext {
            traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".to_owned(),
            probe: Some(mini_sqlite_todo_yew::native_sync::NativeSyncProbe {
                probe_id: "probe-insert".to_owned(),
                operation: "insert".to_owned(),
                table: "todos".to_owned(),
                row_id: "todo-1".to_owned(),
                origin_browser_id: "browser-a".to_owned(),
            }),
        };
        let stale = NativeTraceContext {
            traceparent: "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01".to_owned(),
            probe: None,
        };
        let change = SyncChange {
            origin_connection_id: 7,
            trace_context: Some(origin.clone()),
            bundles: Vec::new(),
            requires_subscription_refresh: false,
        };

        let stale = Some(stale);
        let selected =
            selected_refresh_trace_context(&change.trace_context, &stale).expect("trace context");
        assert_eq!(selected.traceparent, origin.traceparent);
        assert_eq!(
            selected.probe.as_ref().map(|probe| probe.probe_id.as_str()),
            Some("probe-insert")
        );
    }

    #[test]
    fn refresh_updates_to_latest_drained_remote_trace_context() {
        let insert = NativeTraceContext {
            traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".to_owned(),
            probe: Some(mini_sqlite_todo_yew::native_sync::NativeSyncProbe {
                probe_id: "probe-insert".to_owned(),
                operation: "insert".to_owned(),
                table: "todos".to_owned(),
                row_id: "todo-1".to_owned(),
                origin_browser_id: "browser-a".to_owned(),
            }),
        };
        let delete = NativeTraceContext {
            traceparent: "00-bbf92f3577b34da6a3ce929d0e0e4736-10f067aa0ba902b7-01".to_owned(),
            probe: Some(mini_sqlite_todo_yew::native_sync::NativeSyncProbe {
                probe_id: "probe-delete".to_owned(),
                operation: "delete".to_owned(),
                table: "todos".to_owned(),
                row_id: "todo-1".to_owned(),
                origin_browser_id: "browser-a".to_owned(),
            }),
        };
        let own_change = NativeTraceContext {
            traceparent: "00-ccf92f3577b34da6a3ce929d0e0e4736-20f067aa0ba902b7-01".to_owned(),
            probe: None,
        };
        let mut selected = Some(insert);

        keep_refresh_trace_context(
            &mut selected,
            9,
            SyncChange {
                origin_connection_id: 7,
                trace_context: Some(delete),
                bundles: Vec::new(),
                requires_subscription_refresh: false,
            },
        );
        keep_refresh_trace_context(
            &mut selected,
            9,
            SyncChange {
                origin_connection_id: 9,
                trace_context: Some(own_change),
                bundles: Vec::new(),
                requires_subscription_refresh: false,
            },
        );

        assert_eq!(
            selected
                .as_ref()
                .and_then(|context| context.probe.as_ref())
                .map(|probe| probe.probe_id.as_str()),
            Some("probe-delete")
        );
    }

    #[test]
    fn drained_remote_changes_keep_bundles_for_direct_pushes() {
        let delete = NativeTraceContext {
            traceparent: "00-bbf92f3577b34da6a3ce929d0e0e4736-10f067aa0ba902b7-01".to_owned(),
            probe: Some(mini_sqlite_todo_yew::native_sync::NativeSyncProbe {
                probe_id: "probe-delete".to_owned(),
                operation: "delete".to_owned(),
                table: "todos".to_owned(),
                row_id: "todo-1".to_owned(),
                origin_browser_id: "browser-a".to_owned(),
            }),
        };
        let own_change = NativeTraceContext {
            traceparent: "00-ccf92f3577b34da6a3ce929d0e0e4736-20f067aa0ba902b7-01".to_owned(),
            probe: None,
        };
        let mut selected = None;
        let mut push_bundles = Vec::new();
        let mut requires_subscription_refresh = false;

        keep_refresh_change(
            &mut selected,
            &mut push_bundles,
            &mut requires_subscription_refresh,
            9,
            SyncChange {
                origin_connection_id: 7,
                trace_context: Some(delete),
                bundles: vec![test_bundle("remote-delete")],
                requires_subscription_refresh: false,
            },
        );
        keep_refresh_change(
            &mut selected,
            &mut push_bundles,
            &mut requires_subscription_refresh,
            9,
            SyncChange {
                origin_connection_id: 9,
                trace_context: Some(own_change),
                bundles: vec![test_bundle("own-insert")],
                requires_subscription_refresh: false,
            },
        );

        assert!(!requires_subscription_refresh);
        assert_eq!(push_bundles.len(), 1);
        assert_eq!(push_bundles[0].schema_fingerprint, "remote-delete");
        assert_eq!(
            selected
                .as_ref()
                .and_then(|context| context.probe.as_ref())
                .map(|probe| probe.probe_id.as_str()),
            Some("probe-delete")
        );
    }

    #[test]
    fn upload_bundles_become_subscriptionless_push_data_messages() {
        let mut next_message_id = u64::MAX;

        let messages =
            push_server_messages(vec![test_bundle("remote-insert")], &mut next_message_id);

        assert_eq!(messages.len(), 1);
        assert_eq!(next_message_id, u64::MAX - 1);
        match &messages[0] {
            mini_jazz_sqlite::protocol::ServerMessage::Data {
                message_id,
                subscription_id,
                cursor,
                bundle,
            } => {
                assert_eq!(message_id.0, u64::MAX);
                assert_eq!(cursor.0, u64::MAX);
                assert!(subscription_id.is_none());
                assert_eq!(bundle.schema_fingerprint, "remote-insert");
            }
            message => panic!("expected push data message, got {message:?}"),
        }
    }

    #[test]
    fn exportable_upload_bundles_are_pushed_even_when_refresh_is_needed() {
        let plan = change_broadcast_plan(true, true);

        assert!(plan.send_push_bundles);
        assert!(plan.refresh_active_subscriptions);
    }

    #[test]
    fn uploaded_todo_transaction_can_be_exported_for_direct_push() {
        use mini_jazz_sqlite::protocol::{
            ClientDataRecord, ClientHello, ClientMessage, ClientTx, DataOp, SessionId,
            TxConflictMode, SUPPORTED_PROTOCOL_VERSION,
        };

        let mut runtime = Runtime::open_with_schema(
            Storage::Memory,
            "server-upload-export",
            "alice",
            todo_schema(),
        )
        .unwrap();
        runtime
            .insert_row(
                "projects",
                "todo-list",
                BTreeMap::from([("title".to_owned(), serde_json::json!("Todo List"))]),
            )
            .unwrap();
        let schema_fingerprint = runtime.local_schema_fingerprint();
        let policy_fingerprint = runtime.local_policy_fingerprint();
        let mut upstream = UpstreamConnectionManager::new_authenticated_for_test(
            "server-session",
            "server",
            schema_fingerprint.clone(),
            policy_fingerprint.clone(),
            "alice",
        );
        let tx_id = "tx-upload-todo-insert".to_owned();

        upstream
            .receive(
                &mut runtime,
                vec![
                    ClientMessage::Hello(ClientHello {
                        protocol_version: SUPPORTED_PROTOCOL_VERSION,
                        session_id: SessionId::new("browser-session"),
                        node_id: "browser".to_owned(),
                        schema_fingerprint,
                        policy_fingerprint,
                    }),
                    ClientMessage::UploadTx {
                        tx: ClientTx {
                            tx_id: tx_id.clone(),
                            branch_id: None,
                            conflict_mode: TxConflictMode::Mergeable,
                            created_at: 1,
                            author: Some("alice".to_owned()),
                        },
                        data: vec![ClientDataRecord {
                            table: "todos".to_owned(),
                            row_id: "todo-uploaded".to_owned(),
                            op: DataOp::Insert,
                            values: BTreeMap::from([
                                ("title".to_owned(), serde_json::json!("Uploaded")),
                                ("done".to_owned(), serde_json::json!(false)),
                                ("project".to_owned(), serde_json::json!("todo-list")),
                            ]),
                        }],
                        reads: Vec::new(),
                    },
                ],
            )
            .unwrap();

        let bundle = runtime.export_transaction(&tx_id).unwrap();
        assert!(bundle
            .history
            .iter()
            .any(|row| row.row_id == "todo-uploaded" && row.op == 1));
    }

    #[test]
    fn uploaded_todo_delete_transaction_can_be_exported_for_direct_push() {
        use mini_jazz_sqlite::protocol::{
            ClientDataRecord, ClientHello, ClientMessage, ClientTx, DataOp, SessionId,
            TxConflictMode, SUPPORTED_PROTOCOL_VERSION,
        };

        let mut runtime = Runtime::open_with_schema(
            Storage::Memory,
            "server-upload-delete-export",
            "alice",
            todo_schema(),
        )
        .unwrap();
        runtime
            .insert_row(
                "projects",
                "todo-list",
                BTreeMap::from([("title".to_owned(), serde_json::json!("Todo List"))]),
            )
            .unwrap();
        let schema_fingerprint = runtime.local_schema_fingerprint();
        let policy_fingerprint = runtime.local_policy_fingerprint();
        let mut upstream = UpstreamConnectionManager::new_authenticated_for_test(
            "server-session",
            "server",
            schema_fingerprint.clone(),
            policy_fingerprint.clone(),
            "alice",
        );
        let insert_tx_id = "tx-upload-todo-insert-before-delete".to_owned();
        let delete_tx_id = "tx-upload-todo-delete".to_owned();

        upstream
            .receive(
                &mut runtime,
                vec![
                    ClientMessage::Hello(ClientHello {
                        protocol_version: SUPPORTED_PROTOCOL_VERSION,
                        session_id: SessionId::new("browser-session"),
                        node_id: "browser".to_owned(),
                        schema_fingerprint,
                        policy_fingerprint,
                    }),
                    ClientMessage::UploadTx {
                        tx: ClientTx {
                            tx_id: insert_tx_id,
                            branch_id: None,
                            conflict_mode: TxConflictMode::Mergeable,
                            created_at: 1,
                            author: Some("alice".to_owned()),
                        },
                        data: vec![ClientDataRecord {
                            table: "todos".to_owned(),
                            row_id: "todo-uploaded-delete".to_owned(),
                            op: DataOp::Insert,
                            values: BTreeMap::from([
                                ("title".to_owned(), serde_json::json!("Uploaded")),
                                ("done".to_owned(), serde_json::json!(false)),
                                ("project".to_owned(), serde_json::json!("todo-list")),
                            ]),
                        }],
                        reads: Vec::new(),
                    },
                    ClientMessage::UploadTx {
                        tx: ClientTx {
                            tx_id: delete_tx_id.clone(),
                            branch_id: None,
                            conflict_mode: TxConflictMode::Mergeable,
                            created_at: 2,
                            author: Some("alice".to_owned()),
                        },
                        data: vec![ClientDataRecord {
                            table: "todos".to_owned(),
                            row_id: "todo-uploaded-delete".to_owned(),
                            op: DataOp::Delete,
                            values: BTreeMap::new(),
                        }],
                        reads: Vec::new(),
                    },
                ],
            )
            .unwrap();

        let bundle = runtime.export_transaction(&delete_tx_id).unwrap();
        assert!(bundle
            .history
            .iter()
            .any(|row| row.row_id == "todo-uploaded-delete" && row.op == 3));
    }
}
