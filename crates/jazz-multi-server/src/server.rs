use std::collections::{HashMap, hash_map::DefaultHasher};
use std::hash::{Hash, Hasher};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use axum::{
    Router,
    extract::{Path as AxumPath, Query, State},
    http::{HeaderMap, StatusCode, header::AUTHORIZATION},
    response::{IntoResponse, Json},
    routing::{get, post},
};
use base64::Engine;
use bytes::Bytes;
use groove::query_manager::session::Session;
use groove::schema_manager::{AppId, SchemaManager};
use groove::storage::SurrealKvStorage;
use groove::sync_manager::{
    ClientId, Destination, InboxEntry, PersistenceTier, Source, SyncManager, SyncPayload,
};
use groove_tokio::TokioRuntime;
use jazz_transport::{
    ConnectionId, ErrorResponse, ServerEvent, SuccessResponse, SyncPayloadRequest,
};
use serde::{Deserialize, Serialize};
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;
use tracing::info;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub port: u16,
    pub data_root: String,
    pub internal_api_secret: String,
    pub worker_threads: usize,
}

#[derive(Debug)]
struct WorkerPool {
    workers: usize,
}

impl WorkerPool {
    fn new(workers: usize) -> Self {
        Self {
            workers: workers.max(1),
        }
    }

    fn worker_count(&self) -> usize {
        self.workers
    }

    fn worker_for_app(&self, app_id: &AppId) -> usize {
        let mut hasher = DefaultHasher::new();
        app_id.hash(&mut hasher);
        (hasher.finish() as usize) % self.workers
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
enum AppStatus {
    Active,
    Disabled,
}

impl Default for AppStatus {
    fn default() -> Self {
        Self::Active
    }
}

#[derive(Debug, Clone)]
struct AppConfig {
    app_name: String,
    jwks_endpoint: String,
    backend_secret: String,
    admin_secret: String,
    status: AppStatus,
}

#[derive(Debug)]
struct ConnectionState {
    _client_id: ClientId,
}

struct AppRuntime {
    app_id: AppId,
    runtime: TokioRuntime<SurrealKvStorage>,
    config: tokio::sync::RwLock<AppConfig>,
    connections: tokio::sync::RwLock<HashMap<u64, ConnectionState>>,
    next_connection_id: AtomicU64,
    sync_broadcast: tokio::sync::broadcast::Sender<(ClientId, SyncPayload)>,
}

impl AppRuntime {
    fn new(app_id: AppId, config: AppConfig, data_dir: &Path) -> Result<Arc<Self>, String> {
        std::fs::create_dir_all(data_dir).map_err(|e| {
            format!(
                "failed to create app data dir '{}': {e}",
                data_dir.display()
            )
        })?;

        let sync_manager = SyncManager::new().with_tier(PersistenceTier::EdgeServer);
        let schema_manager = SchemaManager::new_server(sync_manager, app_id, "prod");

        let db_path = data_dir.join("groove.surrealkv");
        let storage = SurrealKvStorage::open(&db_path, 64 * 1024 * 1024)
            .map_err(|e| format!("failed to open storage '{}': {e:?}", db_path.display()))?;

        let (sync_tx, _) = tokio::sync::broadcast::channel::<(ClientId, SyncPayload)>(256);
        let sync_tx_clone = sync_tx.clone();

        let runtime = TokioRuntime::new(schema_manager, storage, move |entry| {
            if let Destination::Client(client_id) = entry.destination {
                let _ = sync_tx_clone.send((client_id, entry.payload));
            }
        });

        Ok(Arc::new(Self {
            app_id,
            runtime,
            config: tokio::sync::RwLock::new(config),
            connections: tokio::sync::RwLock::new(HashMap::new()),
            next_connection_id: AtomicU64::new(1),
            sync_broadcast: sync_tx,
        }))
    }
}

struct ServerState {
    apps: tokio::sync::RwLock<HashMap<AppId, Arc<AppRuntime>>>,
    data_root: PathBuf,
    internal_api_secret: String,
    workers: WorkerPool,
}

impl ServerState {
    async fn get_app(&self, app_id: AppId) -> Option<Arc<AppRuntime>> {
        self.apps.read().await.get(&app_id).cloned()
    }

    fn app_data_dir(&self, app_id: AppId) -> PathBuf {
        self.data_root.join("apps").join(app_id.to_string())
    }

    async fn app_count(&self) -> usize {
        self.apps.read().await.len()
    }
}

#[derive(Debug, Deserialize)]
struct AppPath {
    app_id: String,
}

#[derive(Debug, Deserialize)]
struct EventsParams {
    client_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct CreateAppRequest {
    app_name: String,
    jwks_endpoint: String,
    backend_secret: Option<String>,
    admin_secret: Option<String>,
}

#[derive(Debug, Deserialize)]
struct UpdateAppRequest {
    app_name: Option<String>,
    jwks_endpoint: Option<String>,
    status: Option<AppStatus>,
    rotate_backend_secret: Option<bool>,
    rotate_admin_secret: Option<bool>,
}

#[derive(Debug, Serialize)]
struct AppSummaryResponse {
    app_id: String,
    app_name: String,
    jwks_endpoint: String,
    status: AppStatus,
    worker: usize,
}

#[derive(Debug, Serialize)]
struct CreateAppResponse {
    app_id: String,
    app_name: String,
    jwks_endpoint: String,
    backend_secret: String,
    admin_secret: String,
    status: AppStatus,
    worker: usize,
}

#[derive(Debug, Serialize)]
struct UpdateAppResponse {
    app_id: String,
    app_name: String,
    jwks_endpoint: String,
    status: AppStatus,
    worker: usize,
    backend_secret: Option<String>,
    admin_secret: Option<String>,
}

pub async fn run(config: ServerConfig) -> Result<(), Box<dyn std::error::Error>> {
    let data_root = PathBuf::from(&config.data_root);
    std::fs::create_dir_all(data_root.join("apps"))?;

    let state = Arc::new(ServerState {
        apps: tokio::sync::RwLock::new(HashMap::new()),
        data_root,
        internal_api_secret: config.internal_api_secret,
        workers: WorkerPool::new(config.worker_threads),
    });

    info!(
        workers = state.workers.worker_count(),
        data_root = %state.data_root.display(),
        "starting multi-tenant Jazz server"
    );

    let app = create_router(state);
    let addr = SocketAddr::from(([0, 0, 0, 0], config.port));
    info!("listening on http://{addr}");

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

fn create_router(state: Arc<ServerState>) -> Router {
    Router::new()
        .route("/apps/:app_id/events", get(events_handler))
        .route("/apps/:app_id/sync", post(sync_handler))
        .route(
            "/internal/apps",
            post(create_app_handler).get(list_apps_handler),
        )
        .route(
            "/internal/apps/:app_id",
            get(get_app_handler).patch(update_app_handler),
        )
        .route("/health", get(health_handler))
        .layer(TraceLayer::new_for_http())
        .layer(CorsLayer::permissive())
        .with_state(state)
}

fn parse_app_id(value: &str) -> Result<AppId, (StatusCode, String)> {
    AppId::from_string(value)
        .map_err(|_| (StatusCode::BAD_REQUEST, format!("invalid app_id: {value}")))
}

fn encode_frame(event: &ServerEvent) -> Bytes {
    let json = serde_json::to_vec(event).unwrap_or_default();
    let len = (json.len() as u32).to_be_bytes();
    let mut buf = Vec::with_capacity(4 + json.len());
    buf.extend_from_slice(&len);
    buf.extend_from_slice(&json);
    Bytes::from(buf)
}

fn decode_session_header(b64: &str) -> Option<Session> {
    let bytes = base64::engine::general_purpose::STANDARD.decode(b64).ok()?;
    let json_str = std::str::from_utf8(&bytes).ok()?;
    serde_json::from_str(json_str).ok()
}

fn extract_session(
    headers: &HeaderMap,
    app_config: &AppConfig,
) -> Result<Option<Session>, (StatusCode, &'static str)> {
    if let Some(session_b64) = headers.get("X-Jazz-Session").and_then(|v| v.to_str().ok()) {
        let backend_secret = headers
            .get("X-Jazz-Backend-Secret")
            .and_then(|v| v.to_str().ok());

        match backend_secret {
            Some(got) if got == app_config.backend_secret => {
                let session = decode_session_header(session_b64)
                    .ok_or((StatusCode::BAD_REQUEST, "Invalid session format"))?;
                return Ok(Some(session));
            }
            Some(_) => {
                return Err((StatusCode::UNAUTHORIZED, "Invalid backend secret"));
            }
            None => {
                return Err((
                    StatusCode::UNAUTHORIZED,
                    "Backend secret required for session impersonation",
                ));
            }
        }
    }

    if let Some(auth_value) = headers.get(AUTHORIZATION).and_then(|v| v.to_str().ok())
        && auth_value.strip_prefix("Bearer ").is_some()
    {
        return Err((
            StatusCode::UNAUTHORIZED,
            "JWT auth via per-app JWKS is not implemented yet (TODO)",
        ));
    }

    Ok(None)
}

fn validate_admin_secret(
    headers: &HeaderMap,
    app_config: &AppConfig,
) -> Result<bool, (StatusCode, &'static str)> {
    let provided = headers
        .get("X-Jazz-Admin-Secret")
        .and_then(|v| v.to_str().ok());

    match provided {
        Some(got) if got == app_config.admin_secret => Ok(true),
        Some(_) => Err((StatusCode::UNAUTHORIZED, "Invalid admin secret")),
        None => Ok(false),
    }
}

fn validate_internal_secret(
    headers: &HeaderMap,
    expected_secret: &str,
) -> Result<(), (StatusCode, &'static str)> {
    let provided = headers
        .get("X-Jazz-Internal-Secret")
        .and_then(|v| v.to_str().ok());

    match provided {
        Some(got) if got == expected_secret => Ok(()),
        _ => Err((StatusCode::UNAUTHORIZED, "Invalid internal API secret")),
    }
}

fn generate_secret() -> String {
    format!("{}{}", Uuid::now_v7().simple(), Uuid::new_v4().simple())
}

async fn app_summary(app: Arc<AppRuntime>, worker: usize) -> AppSummaryResponse {
    let cfg = app.config.read().await;
    AppSummaryResponse {
        app_id: app.app_id.to_string(),
        app_name: cfg.app_name.clone(),
        jwks_endpoint: cfg.jwks_endpoint.clone(),
        status: cfg.status,
        worker,
    }
}

async fn events_handler(
    State(state): State<Arc<ServerState>>,
    AxumPath(path): AxumPath<AppPath>,
    headers: HeaderMap,
    Query(params): Query<EventsParams>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let app_id = parse_app_id(&path.app_id)?;
    let worker = state.workers.worker_for_app(&app_id);
    let app = state.get_app(app_id).await.ok_or((
        StatusCode::NOT_FOUND,
        format!("unknown app_id: {}", path.app_id),
    ))?;

    let cfg = app.config.read().await.clone();
    if cfg.status == AppStatus::Disabled {
        return Err((
            StatusCode::FORBIDDEN,
            "app is disabled for sync traffic".to_string(),
        ));
    }

    let session = extract_session(&headers, &cfg)
        .map_err(|(status, msg)| (status, msg.to_string()))?
        .ok_or((
            StatusCode::UNAUTHORIZED,
            "session required for event stream".to_string(),
        ))?;

    let client_id = match params.client_id {
        Some(s) => ClientId::parse(&s)
            .ok_or((StatusCode::BAD_REQUEST, format!("invalid client_id: {s}")))?,
        None => ClientId::new(),
    };

    app.runtime
        .ensure_client_with_session(client_id, session)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let connection_id = app.next_connection_id.fetch_add(1, Ordering::SeqCst);
    {
        let mut connections = app.connections.write().await;
        connections.insert(
            connection_id,
            ConnectionState {
                _client_id: client_id,
            },
        );
    }

    info!(
        app_id = %app_id,
        worker,
        client_id = %client_id,
        connection_id,
        "events stream connected"
    );

    let mut sync_rx = app.sync_broadcast.subscribe();
    let app_cleanup = app.clone();
    let client_id_str = client_id.to_string();

    let stream = async_stream::stream! {
        let connected = ServerEvent::Connected {
            connection_id: ConnectionId(connection_id),
            client_id: client_id_str,
        };
        yield Ok::<Bytes, std::convert::Infallible>(encode_frame(&connected));

        let mut heartbeat_interval = tokio::time::interval(Duration::from_secs(30));

        loop {
            tokio::select! {
                result = sync_rx.recv() => {
                    match result {
                        Ok((target_client_id, payload)) => {
                            if target_client_id == client_id {
                                let event = ServerEvent::SyncUpdate { payload: Box::new(payload) };
                                yield Ok(encode_frame(&event));
                            }
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                            tracing::warn!(app_id = %app_id, connection_id, "events stream lagged");
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                            break;
                        }
                    }
                }
                _ = heartbeat_interval.tick() => {
                    yield Ok(encode_frame(&ServerEvent::Heartbeat));
                }
            }
        }

        let mut connections = app_cleanup.connections.write().await;
        connections.remove(&connection_id);
    };

    Ok(axum::response::Response::builder()
        .header("Content-Type", "application/octet-stream")
        .header("Transfer-Encoding", "chunked")
        .header("Cache-Control", "no-cache")
        .body(axum::body::Body::from_stream(stream))
        .unwrap())
}

async fn sync_handler(
    State(state): State<Arc<ServerState>>,
    AxumPath(path): AxumPath<AppPath>,
    headers: HeaderMap,
    Json(request): Json<SyncPayloadRequest>,
) -> impl IntoResponse {
    let app_id = match parse_app_id(&path.app_id) {
        Ok(id) => id,
        Err((status, msg)) => {
            return (status, Json(ErrorResponse::bad_request(msg))).into_response();
        }
    };

    let app = match state.get_app(app_id).await {
        Some(app) => app,
        None => {
            return (
                StatusCode::NOT_FOUND,
                Json(ErrorResponse::not_found(format!(
                    "unknown app_id: {}",
                    path.app_id
                ))),
            )
                .into_response();
        }
    };

    let cfg = app.config.read().await.clone();
    if cfg.status == AppStatus::Disabled {
        return (
            StatusCode::FORBIDDEN,
            Json(ErrorResponse::forbidden("app is disabled for sync traffic")),
        )
            .into_response();
    }

    let is_admin = match validate_admin_secret(&headers, &cfg) {
        Ok(value) => value,
        Err((status, msg)) => {
            return (status, Json(ErrorResponse::unauthorized(msg))).into_response();
        }
    };

    if is_admin {
        if let Err(e) = app.runtime.add_client(request.client_id, None) {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse::internal(e.to_string())),
            )
                .into_response();
        }
        if let Err(e) = app.runtime.set_client_admin(request.client_id) {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse::internal(e.to_string())),
            )
                .into_response();
        }
    } else {
        let session = match extract_session(&headers, &cfg) {
            Ok(Some(session)) => session,
            Ok(None) => {
                return (
                    StatusCode::UNAUTHORIZED,
                    Json(ErrorResponse::unauthorized(
                        "session required for sync. provide JWT or backend secret",
                    )),
                )
                    .into_response();
            }
            Err((status, msg)) => {
                return (status, Json(ErrorResponse::unauthorized(msg))).into_response();
            }
        };

        if let Err(e) = app
            .runtime
            .ensure_client_with_session(request.client_id, session)
        {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse::internal(e.to_string())),
            )
                .into_response();
        }
    }

    let entry = InboxEntry {
        source: Source::Client(request.client_id),
        payload: request.payload,
    };

    match app.runtime.push_sync_inbox(entry) {
        Ok(()) => Json(SuccessResponse::default()).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse::internal(e.to_string())),
        )
            .into_response(),
    }
}

async fn create_app_handler(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
    Json(request): Json<CreateAppRequest>,
) -> impl IntoResponse {
    if let Err((status, msg)) = validate_internal_secret(&headers, &state.internal_api_secret) {
        return (status, Json(ErrorResponse::unauthorized(msg))).into_response();
    }

    if request.app_name.trim().is_empty() || request.jwks_endpoint.trim().is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse::bad_request(
                "app_name and jwks_endpoint are required",
            )),
        )
            .into_response();
    }

    let backend_secret = request.backend_secret.unwrap_or_else(generate_secret);
    let admin_secret = request.admin_secret.unwrap_or_else(generate_secret);

    let app_id = loop {
        let candidate = AppId::random();
        if state.apps.read().await.contains_key(&candidate) {
            continue;
        }
        break candidate;
    };

    let app_config = AppConfig {
        app_name: request.app_name,
        jwks_endpoint: request.jwks_endpoint,
        backend_secret: backend_secret.clone(),
        admin_secret: admin_secret.clone(),
        status: AppStatus::Active,
    };

    let data_dir = state.app_data_dir(app_id);
    let app_runtime = match AppRuntime::new(app_id, app_config.clone(), &data_dir) {
        Ok(runtime) => runtime,
        Err(err) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse::internal(err)),
            )
                .into_response();
        }
    };

    state.apps.write().await.insert(app_id, app_runtime);

    let worker = state.workers.worker_for_app(&app_id);

    Json(CreateAppResponse {
        app_id: app_id.to_string(),
        app_name: app_config.app_name,
        jwks_endpoint: app_config.jwks_endpoint,
        backend_secret,
        admin_secret,
        status: app_config.status,
        worker,
    })
    .into_response()
}

async fn list_apps_handler(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Err((status, msg)) = validate_internal_secret(&headers, &state.internal_api_secret) {
        return (status, Json(ErrorResponse::unauthorized(msg))).into_response();
    }

    let apps: Vec<Arc<AppRuntime>> = state.apps.read().await.values().cloned().collect();
    let mut response = Vec::with_capacity(apps.len());

    for app in apps {
        let worker = state.workers.worker_for_app(&app.app_id);
        response.push(app_summary(app, worker).await);
    }

    Json(response).into_response()
}

async fn get_app_handler(
    State(state): State<Arc<ServerState>>,
    AxumPath(path): AxumPath<AppPath>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Err((status, msg)) = validate_internal_secret(&headers, &state.internal_api_secret) {
        return (status, Json(ErrorResponse::unauthorized(msg))).into_response();
    }

    let app_id = match parse_app_id(&path.app_id) {
        Ok(id) => id,
        Err((status, msg)) => {
            return (status, Json(ErrorResponse::bad_request(msg))).into_response();
        }
    };

    let app = match state.get_app(app_id).await {
        Some(app) => app,
        None => {
            return (
                StatusCode::NOT_FOUND,
                Json(ErrorResponse::not_found(format!(
                    "unknown app_id: {}",
                    path.app_id
                ))),
            )
                .into_response();
        }
    };

    let worker = state.workers.worker_for_app(&app_id);
    Json(app_summary(app, worker).await).into_response()
}

async fn update_app_handler(
    State(state): State<Arc<ServerState>>,
    AxumPath(path): AxumPath<AppPath>,
    headers: HeaderMap,
    Json(request): Json<UpdateAppRequest>,
) -> impl IntoResponse {
    if let Err((status, msg)) = validate_internal_secret(&headers, &state.internal_api_secret) {
        return (status, Json(ErrorResponse::unauthorized(msg))).into_response();
    }

    let app_id = match parse_app_id(&path.app_id) {
        Ok(id) => id,
        Err((status, msg)) => {
            return (status, Json(ErrorResponse::bad_request(msg))).into_response();
        }
    };

    let app = match state.get_app(app_id).await {
        Some(app) => app,
        None => {
            return (
                StatusCode::NOT_FOUND,
                Json(ErrorResponse::not_found(format!(
                    "unknown app_id: {}",
                    path.app_id
                ))),
            )
                .into_response();
        }
    };

    let mut new_backend_secret = None;
    let mut new_admin_secret = None;

    let (app_name, jwks_endpoint, status) = {
        let mut cfg = app.config.write().await;

        if let Some(app_name) = request.app_name {
            cfg.app_name = app_name;
        }
        if let Some(jwks_endpoint) = request.jwks_endpoint {
            cfg.jwks_endpoint = jwks_endpoint;
        }
        if let Some(status) = request.status {
            cfg.status = status;
        }
        if request.rotate_backend_secret.unwrap_or(false) {
            let secret = generate_secret();
            cfg.backend_secret = secret.clone();
            new_backend_secret = Some(secret);
        }
        if request.rotate_admin_secret.unwrap_or(false) {
            let secret = generate_secret();
            cfg.admin_secret = secret.clone();
            new_admin_secret = Some(secret);
        }

        (cfg.app_name.clone(), cfg.jwks_endpoint.clone(), cfg.status)
    };

    let worker = state.workers.worker_for_app(&app_id);

    Json(UpdateAppResponse {
        app_id: app_id.to_string(),
        app_name,
        jwks_endpoint,
        status,
        worker,
        backend_secret: new_backend_secret,
        admin_secret: new_admin_secret,
    })
    .into_response()
}

async fn health_handler(State(state): State<Arc<ServerState>>) -> impl IntoResponse {
    Json(serde_json::json!({
        "status": "healthy",
        "apps": state.app_count().await,
        "workers": state.workers.worker_count(),
    }))
}
