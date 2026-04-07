//! HTTP routes for the Jazz server.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::{
    Router,
    extract::{
        Path, Query, State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    http::{HeaderMap, StatusCode, header::AUTHORIZATION, header::CONTENT_TYPE},
    response::{IntoResponse, Json, Response},
    routing::{get, post},
};
use serde::{Deserialize, Serialize};
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;
use uuid::Uuid;

use crate::jazz_transport::{
    AuthHandshake, AuthHandshakePayload, ConnectionId, ErrorResponse, ServerEvent,
    SyncBatchRequest, SyncBatchResponse, SyncPayloadResult,
};
use crate::middleware::auth::{
    derive_local_principal_id, extract_session, parse_local_auth_headers, validate_admin_secret,
    validate_backend_secret,
};
use crate::object::ObjectId;
use crate::query_manager::types::{
    ColumnType, Schema, SchemaHash, TableName, TablePolicies, Value,
};
use crate::schema_manager::{AppId, Lens, LensOp, LensTransform};
use crate::server::{CatalogueAuthorityMode, ConnectionState, ServerState};
use crate::sync_manager::ClientId;

/// Create the router with all routes.
pub fn create_router(state: Arc<ServerState>) -> Router {
    let traced_routes = Router::new()
        .route("/schema/:hash", get(schema_handler))
        .route("/schemas", get(schema_hashes_handler))
        .route("/admin/schemas", post(publish_schema_handler))
        .route("/admin/permissions/head", get(permissions_head_handler))
        .route("/admin/permissions", post(publish_permissions_handler))
        .route("/admin/migrations", post(publish_migration_handler))
        .route(
            "/admin/introspection/subscriptions",
            get(admin_subscription_introspection_handler),
        )
        // Unified sync endpoint for all SyncPayload variants.
        .route("/sync", post(sync_handler))
        // Link a local anonymous/demo principal to an external identity.
        .route("/auth/link-external", post(link_external_handler))
        // Health check
        .route("/health", get(health_handler))
        .layer(TraceLayer::new_for_http());

    Router::new()
        .route("/ws", get(ws_handler))
        .merge(traced_routes)
        .layer(CorsLayer::permissive())
        .with_state(state)
}

#[derive(Debug, Serialize)]
struct SchemaHashesResponse {
    hashes: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct AdminSubscriptionIntrospectionParams {
    #[serde(rename = "appId")]
    app_id: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct AdminSubscriptionIntrospectionResponse {
    app_id: String,
    generated_at: u64,
    queries: Vec<crate::query_manager::manager::ServerSubscriptionTelemetryGroup>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PublishMigrationRequest {
    from_hash: String,
    to_hash: String,
    forward: Vec<PublishTableLens>,
}

#[derive(Debug, Serialize, Deserialize)]
struct PublishTableLens {
    table: String,
    operations: Vec<PublishLensOp>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
enum PublishLensOp {
    Introduce {
        column: String,
        column_type: ColumnType,
        value: Value,
    },
    Drop {
        column: String,
        column_type: ColumnType,
        value: Value,
    },
    Rename {
        column: String,
        value: String,
    },
}

#[derive(Debug, Serialize, Deserialize)]
struct PublishSchemaRequest {
    schema: Schema,
    permissions: Option<std::collections::HashMap<TableName, TablePolicies>>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PublishPermissionsRequest {
    schema_hash: String,
    permissions: std::collections::HashMap<String, TablePolicies>,
    expected_parent_bundle_object_id: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct PublishSchemaResponse {
    object_id: String,
    hash: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct PermissionsHeadView {
    schema_hash: String,
    version: u64,
    parent_bundle_object_id: Option<String>,
    bundle_object_id: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct PermissionsHeadResponse {
    head: Option<PermissionsHeadView>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct PublishMigrationResponse {
    object_id: String,
    from_hash: String,
    to_hash: String,
}

#[derive(Debug, Serialize)]
struct LinkExternalResponse {
    principal_id: String,
    issuer: String,
    subject: String,
    created: bool,
}

async fn forward_catalogue_request(
    state: &Arc<ServerState>,
    method: reqwest::Method,
    path: &str,
    body: Option<Vec<u8>>,
) -> Result<Response, (StatusCode, Json<ErrorResponse>)> {
    let (base_url, authority_admin_secret) = match &state.catalogue_authority {
        CatalogueAuthorityMode::Local => {
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse::internal(
                    "catalogue forwarding requested without a configured authority".to_string(),
                )),
            ));
        }
        CatalogueAuthorityMode::Forward {
            base_url,
            admin_secret,
        } => (base_url.as_str(), admin_secret.as_str()),
    };

    let authority_url = authority_endpoint_url(base_url, path).map_err(|message| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse::internal(message)),
        )
    })?;

    let mut request = state
        .http_client
        .request(method, authority_url)
        .header("X-Jazz-Admin-Secret", authority_admin_secret);
    if let Some(body) = body {
        request = request.header(CONTENT_TYPE, "application/json").body(body);
    }

    let response = request.send().await.map_err(|err| {
        (
            StatusCode::BAD_GATEWAY,
            Json(ErrorResponse::internal(format!(
                "failed to reach catalogue authority: {err}"
            ))),
        )
    })?;

    let status =
        StatusCode::from_u16(response.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
    let content_type = response.headers().get(CONTENT_TYPE).cloned();
    let bytes = response.bytes().await.map_err(|err| {
        (
            StatusCode::BAD_GATEWAY,
            Json(ErrorResponse::internal(format!(
                "failed to read authority response: {err}"
            ))),
        )
    })?;

    let mut response_builder = Response::builder().status(status);
    if let Some(content_type) = content_type {
        response_builder = response_builder.header(CONTENT_TYPE, content_type);
    }

    response_builder
        .body(axum::body::Body::from(bytes))
        .map_err(|err| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse::internal(format!(
                    "failed to build forwarded response: {err}"
                ))),
            )
        })
}

fn authority_endpoint_url(base_url: &str, path: &str) -> Result<String, String> {
    let parsed = reqwest::Url::parse(base_url)
        .map_err(|err| format!("invalid catalogue authority URL '{base_url}': {err}"))?;
    let mut origin = parsed.clone();
    origin.set_query(None);
    origin.set_fragment(None);

    let mut full_path = parsed.path().trim_end_matches('/').to_string();
    if full_path.is_empty() {
        full_path.push('/');
    }
    if !full_path.ends_with('/') {
        full_path.push('/');
    }
    full_path.push_str(path.trim_start_matches('/'));

    origin.set_path(&full_path);
    Ok(origin.to_string())
}

// ============================================================================
// Sync handler (HTTP POST)
// ============================================================================

/// HTTP POST `/sync` handler — unified sync endpoint for all SyncPayload variants.
///
/// Auth is carried via HTTP headers:
/// - `X-Jazz-Admin-Secret`: admin authentication (catalogue sync)
/// - `X-Jazz-Backend-Secret` + `X-Jazz-Session`: backend impersonation
/// - `Authorization: Bearer <jwt>`: frontend JWT auth
async fn sync_handler(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
    Json(request): Json<SyncBatchRequest>,
) -> Response {
    let client_id = request.client_id;

    // Check admin secret header
    let admin_secret_header = headers
        .get("X-Jazz-Admin-Secret")
        .and_then(|v| v.to_str().ok());
    let is_admin = if let Some(secret) = admin_secret_header {
        match validate_admin_secret(Some(secret), &state.auth_config) {
            Ok(()) => true,
            Err((status, msg)) => {
                return (status, Json(ErrorResponse::unauthorized(msg))).into_response();
            }
        }
    } else {
        false
    };

    // Check if any payload targets a catalogue object — require admin auth
    let has_catalogue_payload = request.payloads.iter().any(is_catalogue_payload);
    if has_catalogue_payload && !is_admin {
        return (
            StatusCode::UNAUTHORIZED,
            Json(ErrorResponse::unauthorized(
                "Admin secret required for catalogue sync",
            )),
        )
            .into_response();
    }

    // Authenticate: backend secret > JWT > local auth > admin-only
    let backend_secret_header = headers
        .get("X-Jazz-Backend-Secret")
        .and_then(|v| v.to_str().ok());
    let has_session_header = headers.get("X-Jazz-Session").is_some();

    if let Some(secret) = backend_secret_header {
        // Backend impersonation path
        if let Err((status, msg)) = validate_backend_secret(Some(secret), &state.auth_config) {
            return (status, Json(ErrorResponse::unauthorized(msg))).into_response();
        }
        // If there's also a session header, extract it
        if has_session_header {
            let external_identities = state.external_identities.read().await;
            match extract_session(
                &headers,
                state.app_id,
                &state.auth_config,
                Some(&external_identities),
                state.jwks_cache.as_ref(),
            )
            .await
            {
                Ok(Some(session)) => {
                    let _ = state.runtime.ensure_client_with_session(client_id, session);
                }
                _ => {
                    let _ = state.runtime.ensure_client_as_backend(client_id);
                }
            }
        } else {
            let _ = state.runtime.ensure_client_as_backend(client_id);
        }
    } else if headers.get(AUTHORIZATION).is_some() {
        // JWT auth path
        let session = {
            let external_identities = state.external_identities.read().await;
            match extract_session(
                &headers,
                state.app_id,
                &state.auth_config,
                Some(&external_identities),
                state.jwks_cache.as_ref(),
            )
            .await
            {
                Ok(Some(s)) => s,
                Ok(None) => {
                    return (
                        StatusCode::UNAUTHORIZED,
                        Json(ErrorResponse::unauthorized("Invalid JWT token")),
                    )
                        .into_response();
                }
                Err((status, msg)) => {
                    return (status, Json(ErrorResponse::unauthorized(msg))).into_response();
                }
            }
        };
        let _ = state.runtime.ensure_client_with_session(client_id, session);
    } else if headers
        .get("X-Jazz-Local-Mode")
        .and_then(|v| v.to_str().ok())
        .is_some()
    {
        // Local auth path
        match parse_local_auth_headers(&headers) {
            Ok(Some((local_mode, local_token))) => {
                let user_id = derive_local_principal_id(state.app_id, local_mode, &local_token);
                let session = crate::query_manager::session::Session {
                    user_id,
                    claims: serde_json::Value::Object(Default::default()),
                };
                let _ = state.runtime.ensure_client_with_session(client_id, session);
            }
            _ => {
                return (
                    StatusCode::UNAUTHORIZED,
                    Json(ErrorResponse::unauthorized("Invalid local auth")),
                )
                    .into_response();
            }
        }
    } else if is_admin {
        // Admin-only (no session auth, just admin secret)
        let _ = state.runtime.ensure_client_as_admin(client_id);
    } else {
        // No auth at all
        return (
            StatusCode::UNAUTHORIZED,
            Json(ErrorResponse::unauthorized("Authentication required")),
        )
            .into_response();
    }

    // Promote to admin if admin_secret was valid
    if is_admin {
        let _ = state.runtime.ensure_client_as_admin(client_id);
    }

    // Apply each payload sequentially
    let mut results = Vec::with_capacity(request.payloads.len());
    for payload in request.payloads {
        let entry = crate::sync_manager::InboxEntry {
            source: crate::sync_manager::Source::Client(client_id),
            payload,
        };
        match state.runtime.push_sync_inbox(entry) {
            Ok(()) => results.push(SyncPayloadResult {
                ok: true,
                error: None,
            }),
            Err(e) => {
                tracing::warn!(error = %e, "sync push_sync_inbox failed");
                results.push(SyncPayloadResult {
                    ok: false,
                    error: Some(e.to_string()),
                });
            }
        }
    }

    Json(SyncBatchResponse { results }).into_response()
}

/// Check if a sync payload targets a catalogue object.
fn is_catalogue_payload(payload: &crate::sync_manager::SyncPayload) -> bool {
    match payload {
        crate::sync_manager::SyncPayload::ObjectUpdated { metadata, .. } => {
            if let Some(meta) = metadata
                && let Some(type_str) = meta
                    .metadata
                    .get(crate::metadata::MetadataKey::Type.as_str())
            {
                return crate::metadata::ObjectType::is_catalogue_type_str(type_str);
            }
            false
        }
        _ => false,
    }
}

// ============================================================================
// WebSocket handler
// ============================================================================

/// WebSocket upgrade handler — bidirectional sync over a single ordered connection.
///
/// Replaces the SSE (`/events`) + HTTP POST (`/sync`) pair. Auth is carried in
/// the first message after upgrade (AuthHandshake), not HTTP headers.
async fn ws_handler(
    State(state): State<Arc<ServerState>>,
    ws: WebSocketUpgrade,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_ws_connection(socket, state))
}

/// Run the WebSocket connection lifecycle:
/// 1. Read AuthHandshake (first message)
/// 2. Authenticate
/// 3. Register connection
/// 4. Send Connected response
/// 5. Bidirectional select! loop: recv sync frames / send ServerEvent frames
async fn handle_ws_connection(mut socket: WebSocket, state: Arc<ServerState>) {
    // 1. Read the first message as AuthHandshake
    let handshake: AuthHandshake = match read_auth_handshake(&mut socket).await {
        Ok(h) => h,
        Err(msg) => {
            tracing::warn!(%msg, "ws auth handshake failed");
            let error = ServerEvent::Error {
                message: msg,
                code: crate::jazz_transport::ErrorCode::Unauthorized,
            };
            let _ = send_server_event(&mut socket, &error).await;
            return;
        }
    };

    // 2. Resolve client_id
    let client_id = match handshake.client_id {
        Some(ref s) => match ClientId::parse(s) {
            Some(id) => id,
            None => {
                let error = ServerEvent::Error {
                    message: format!("Invalid client_id: {}", s),
                    code: crate::jazz_transport::ErrorCode::BadRequest,
                };
                let _ = send_server_event(&mut socket, &error).await;
                return;
            }
        },
        None => ClientId::new(),
    };

    tracing::info!(%client_id, "ws client connecting");

    // 3. Authenticate based on handshake payload
    let is_admin = if let Some(ref secret) = handshake.admin_secret {
        match validate_admin_secret(Some(secret.as_str()), &state.auth_config) {
            Ok(()) => true,
            Err((_, msg)) => {
                let error = ServerEvent::Error {
                    message: msg.to_string(),
                    code: crate::jazz_transport::ErrorCode::Unauthorized,
                };
                let _ = send_server_event(&mut socket, &error).await;
                return;
            }
        }
    } else {
        false
    };

    // Authenticate the client with the runtime
    match &handshake.auth {
        AuthHandshakePayload::Backend { secret, session } => {
            if let Err((_, msg)) =
                validate_backend_secret(Some(secret.as_str()), &state.auth_config)
            {
                let error = ServerEvent::Error {
                    message: msg.to_string(),
                    code: crate::jazz_transport::ErrorCode::Unauthorized,
                };
                let _ = send_server_event(&mut socket, &error).await;
                return;
            }
            if let Ok(session) =
                serde_json::from_str::<crate::query_manager::session::Session>(session)
            {
                let _ = state.runtime.ensure_client_with_session(client_id, session);
            } else {
                let _ = state.runtime.ensure_client_as_backend(client_id);
            }
        }
        AuthHandshakePayload::Jwt { token } => {
            // Build fake headers for the existing extract_session path
            let mut headers = HeaderMap::new();
            headers.insert(AUTHORIZATION, format!("Bearer {}", token).parse().unwrap());
            let session = {
                let external_identities = state.external_identities.read().await;
                match extract_session(
                    &headers,
                    state.app_id,
                    &state.auth_config,
                    Some(&external_identities),
                    state.jwks_cache.as_ref(),
                )
                .await
                {
                    Ok(Some(s)) => s,
                    Ok(None) => {
                        let error = ServerEvent::Error {
                            message: "Invalid JWT token".to_string(),
                            code: crate::jazz_transport::ErrorCode::Unauthorized,
                        };
                        let _ = send_server_event(&mut socket, &error).await;
                        return;
                    }
                    Err((_, msg)) => {
                        let error = ServerEvent::Error {
                            message: msg.to_string(),
                            code: crate::jazz_transport::ErrorCode::Unauthorized,
                        };
                        let _ = send_server_event(&mut socket, &error).await;
                        return;
                    }
                }
            };
            let _ = state.runtime.ensure_client_with_session(client_id, session);
        }
        AuthHandshakePayload::Local { mode, token } => {
            // Build fake headers for the existing local auth path
            let mut headers = HeaderMap::new();
            headers.insert(
                axum::http::HeaderName::from_static("x-jazz-local-mode"),
                axum::http::HeaderValue::from_str(mode).unwrap(),
            );
            if let Some(token) = token {
                headers.insert(
                    axum::http::HeaderName::from_static("x-jazz-local-token"),
                    axum::http::HeaderValue::from_str(token).unwrap(),
                );
            }
            match parse_local_auth_headers(&headers) {
                Ok(Some((local_mode, local_token))) => {
                    let user_id = derive_local_principal_id(state.app_id, local_mode, &local_token);
                    let session = crate::query_manager::session::Session {
                        user_id,
                        claims: serde_json::Value::Object(Default::default()),
                    };
                    let _ = state.runtime.ensure_client_with_session(client_id, session);
                }
                _ => {
                    let error = ServerEvent::Error {
                        message: "Invalid local auth".to_string(),
                        code: crate::jazz_transport::ErrorCode::Unauthorized,
                    };
                    let _ = send_server_event(&mut socket, &error).await;
                    return;
                }
            }
        }
        AuthHandshakePayload::None => {
            if is_admin {
                let _ = state.runtime.ensure_client_as_admin(client_id);
            } else {
                let error = ServerEvent::Error {
                    message: "Authentication required".to_string(),
                    code: crate::jazz_transport::ErrorCode::Unauthorized,
                };
                let _ = send_server_event(&mut socket, &error).await;
                return;
            }
        }
    }

    // Promote to admin if admin_secret was valid (allows catalogue writes)
    if is_admin {
        let _ = state.runtime.ensure_client_as_admin(client_id);
    }

    // 4. Register connection
    let connection_id = state
        .next_connection_id
        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    {
        let mut connections = state.connections.write().await;
        connections.insert(connection_id, ConnectionState { client_id });
    }
    state.on_client_connected(client_id).await;

    let catalogue_state_hash = state.runtime.catalogue_state_hash().ok();

    // 5. Send Connected response
    let connected = ServerEvent::Connected {
        connection_id: ConnectionId(connection_id),
        client_id: client_id.to_string(),
        next_sync_seq: None,
        catalogue_state_hash,
    };
    if send_server_event(&mut socket, &connected).await.is_err() {
        return;
    }

    // 6. Subscribe to broadcast channel for this client's events
    let mut sync_rx = state.sync_broadcast.subscribe();

    // 7. Bidirectional select! loop
    loop {
        tokio::select! {
            // Client → Server: receive sync frames
            msg = socket.recv() => {
                match msg {
                    Some(Ok(msg @ (Message::Binary(_) | Message::Text(_)))) => {
                        let parse_result = match &msg {
                            Message::Binary(data) => serde_json::from_slice::<SyncBatchRequest>(data),
                            Message::Text(text) => serde_json::from_str::<SyncBatchRequest>(text),
                            _ => unreachable!(),
                        };
                        match parse_result {
                            Ok(request) => {
                                for payload in request.payloads {
                                    let entry = crate::sync_manager::InboxEntry {
                                        source: crate::sync_manager::Source::Client(client_id),
                                        payload,
                                    };
                                    if let Err(e) = state.runtime.push_sync_inbox(entry) {
                                        tracing::warn!(error = %e, "push_sync_inbox failed");
                                    }
                                }
                            }
                            Err(e) => {
                                tracing::warn!(error = %e, "invalid sync batch");
                            }
                        }
                    }
                    Some(Ok(Message::Ping(_))) | Some(Ok(Message::Pong(_))) => continue,
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Err(e)) => {
                        tracing::debug!(error = %e, "ws recv error");
                        break;
                    }
                }
            }
            // Server → Client: forward sync updates for this client
            result = sync_rx.recv() => {
                match result {
                    Ok((target_client_id, payload)) => {
                        if target_client_id == client_id {
                            let event = ServerEvent::SyncUpdate {
                                seq: None,
                                payload: Box::new(payload),
                            };
                            if send_server_event(&mut socket, &event).await.is_err() {
                                break;
                            }
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!(connection_id, lagged = n, "ws client lagged on sync updates");
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
        }
    }

    // Cleanup: remove connection, mark for TTL reaping
    {
        let mut connections = state.connections.write().await;
        connections.remove(&connection_id);
    }
    state.on_connection_closed(client_id).await;
    tracing::debug!(connection_id, %client_id, "ws connection closed");
}

/// Read the first WebSocket message as an AuthHandshake.
async fn read_auth_handshake(socket: &mut WebSocket) -> Result<AuthHandshake, String> {
    match tokio::time::timeout(Duration::from_secs(10), socket.recv()).await {
        Ok(Some(Ok(Message::Binary(data)))) => {
            serde_json::from_slice(&data).map_err(|e| format!("invalid auth handshake: {e}"))
        }
        Ok(Some(Ok(Message::Text(text)))) => {
            serde_json::from_str(&text).map_err(|e| format!("invalid auth handshake: {e}"))
        }
        Ok(Some(Ok(_))) => Err("expected binary or text message for auth handshake".into()),
        Ok(Some(Err(e))) => Err(format!("ws error during auth handshake: {e}")),
        Ok(None) => Err("connection closed before auth handshake".into()),
        Err(_) => Err("auth handshake timed out (10s)".into()),
    }
}

/// Send a ServerEvent as a JSON binary frame over WebSocket.
async fn send_server_event(socket: &mut WebSocket, event: &ServerEvent) -> Result<(), axum::Error> {
    let json = serde_json::to_vec(event).unwrap_or_default();
    socket.send(Message::Binary(json)).await
}

/// Return the catalogue schema for the given hash.
///
/// Requires a valid admin secret; returns 404 if no schema exists for the hash.
async fn schema_handler(
    State(state): State<Arc<ServerState>>,
    Path(hash_text): Path<String>,
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

    if matches!(
        &state.catalogue_authority,
        CatalogueAuthorityMode::Forward { .. }
    ) {
        return match forward_catalogue_request(
            &state,
            reqwest::Method::GET,
            &format!("/schema/{hash_text}"),
            None,
        )
        .await
        {
            Ok(response) => response,
            Err(error) => error.into_response(),
        };
    }

    let schema_hash = match parse_schema_hash_param(&hash_text) {
        Ok(hash) => hash,
        Err(message) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse::bad_request(message)),
            )
                .into_response();
        }
    };

    match state.runtime.known_schema(&schema_hash) {
        Ok(Some(schema)) => {
            tracing::info!(
                requested_hash = %schema_hash.short(),
                "schema request: returning requested hash"
            );
            let body = schema.clone();
            Json(body).into_response()
        }
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse::not_found(format!(
                "schema catalogue not found for hash {}",
                schema_hash
            ))),
        )
            .into_response(),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse::internal(format!(
                "failed to read schema catalogue: {err}"
            ))),
        )
            .into_response(),
    }
}

/// Return all known schema hashes from catalogue state.
///
/// Requires a valid admin secret.
async fn schema_hashes_handler(
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

    if matches!(
        &state.catalogue_authority,
        CatalogueAuthorityMode::Forward { .. }
    ) {
        return match forward_catalogue_request(&state, reqwest::Method::GET, "/schemas", None).await
        {
            Ok(response) => response,
            Err(error) => error.into_response(),
        };
    }

    match state.runtime.known_schema_hashes() {
        Ok(hashes) => {
            let body = SchemaHashesResponse {
                hashes: hashes.iter().map(ToString::to_string).collect(),
            };
            Json(body).into_response()
        }
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse::internal(format!(
                "failed to read schema hashes: {err}"
            ))),
        )
            .into_response(),
    }
}

/// Publish a schema object into the catalogue.
///
/// Requires a valid admin secret.
async fn publish_schema_handler(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
    Json(request): Json<PublishSchemaRequest>,
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

    if matches!(
        &state.catalogue_authority,
        CatalogueAuthorityMode::Forward { .. }
    ) {
        let body = match serde_json::to_vec(&request) {
            Ok(body) => body,
            Err(err) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse::internal(format!(
                        "failed to serialize schema publish request: {err}"
                    ))),
                )
                    .into_response();
            }
        };
        return match forward_catalogue_request(
            &state,
            reqwest::Method::POST,
            "/admin/schemas",
            Some(body),
        )
        .await
        {
            Ok(response) => response,
            Err(error) => error.into_response(),
        };
    }

    if request.permissions.is_some() {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse::bad_request(
                "schema publishing no longer accepts permissions; publish permissions via POST /admin/permissions".to_string(),
            )),
        )
            .into_response();
    }

    let schema_hash = SchemaHash::compute(&request.schema);
    let object_id = match state.runtime.publish_schema(request.schema) {
        Ok(object_id) => object_id,
        Err(err) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse::internal(format!(
                    "failed to publish schema catalogue: {err}"
                ))),
            )
                .into_response();
        }
    };

    (
        StatusCode::CREATED,
        Json(PublishSchemaResponse {
            object_id: object_id.to_string(),
            hash: schema_hash.to_string(),
        }),
    )
        .into_response()
}

async fn permissions_head_handler(
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

    if matches!(
        &state.catalogue_authority,
        CatalogueAuthorityMode::Forward { .. }
    ) {
        return match forward_catalogue_request(
            &state,
            reqwest::Method::GET,
            "/admin/permissions/head",
            None,
        )
        .await
        {
            Ok(response) => response,
            Err(error) => error.into_response(),
        };
    }

    match state.runtime.with_schema_manager(|schema_manager| {
        schema_manager
            .current_permissions_head()
            .map(permissions_head_view)
    }) {
        Ok(head) => (StatusCode::OK, Json(PermissionsHeadResponse { head })).into_response(),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse::internal(format!(
                "failed to read permissions head: {err}"
            ))),
        )
            .into_response(),
    }
}

async fn publish_permissions_handler(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
    Json(request): Json<PublishPermissionsRequest>,
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

    if matches!(
        &state.catalogue_authority,
        CatalogueAuthorityMode::Forward { .. }
    ) {
        let body = match serde_json::to_vec(&request) {
            Ok(body) => body,
            Err(err) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse::internal(format!(
                        "failed to serialize permissions publish request: {err}"
                    ))),
                )
                    .into_response();
            }
        };
        return match forward_catalogue_request(
            &state,
            reqwest::Method::POST,
            "/admin/permissions",
            Some(body),
        )
        .await
        {
            Ok(response) => response,
            Err(error) => error.into_response(),
        };
    }

    let schema_hash = match parse_schema_hash_param(&request.schema_hash) {
        Ok(hash) => hash,
        Err(message) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse::bad_request(message)),
            )
                .into_response();
        }
    };

    let expected_parent_bundle_object_id = match request.expected_parent_bundle_object_id {
        Some(object_id) => match parse_object_id_param(&object_id) {
            Ok(object_id) => Some(object_id),
            Err(message) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(ErrorResponse::bad_request(message)),
                )
                    .into_response();
            }
        },
        None => None,
    };

    match state.runtime.known_schema(&schema_hash) {
        Ok(Some(_)) => {}
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                Json(ErrorResponse::not_found(format!(
                    "target schema catalogue not found for hash {}",
                    schema_hash
                ))),
            )
                .into_response();
        }
        Err(err) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse::internal(format!(
                    "failed to read known schemas: {err}"
                ))),
            )
                .into_response();
        }
    }

    let permissions = request
        .permissions
        .into_iter()
        .map(|(table_name, policies)| (TableName::new(table_name), policies))
        .collect();

    match state.runtime.publish_permissions_bundle(
        schema_hash,
        permissions,
        expected_parent_bundle_object_id,
    ) {
        Ok(_) => match state.runtime.with_schema_manager(|schema_manager| {
            schema_manager
                .current_permissions_head()
                .map(permissions_head_view)
        }) {
            Ok(head) => {
                (StatusCode::CREATED, Json(PermissionsHeadResponse { head })).into_response()
            }
            Err(err) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse::internal(format!(
                    "failed to read published permissions head: {err}"
                ))),
            )
                .into_response(),
        },
        Err(crate::runtime_tokio::RuntimeError::WriteError(message))
            if message.starts_with("stale permissions parent") =>
        {
            (
                StatusCode::CONFLICT,
                Json(ErrorResponse::bad_request(message)),
            )
                .into_response()
        }
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse::internal(format!(
                "failed to publish permissions catalogue: {err}"
            ))),
        )
            .into_response(),
    }
}

/// Publish a reviewed migration edge into the catalogue.
///
/// Requires a valid admin secret. The source and target schemas must already be
/// known to the server; only the lens edge itself is created here.
async fn publish_migration_handler(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
    Json(request): Json<PublishMigrationRequest>,
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

    if matches!(
        &state.catalogue_authority,
        CatalogueAuthorityMode::Forward { .. }
    ) {
        let body = match serde_json::to_vec(&request) {
            Ok(body) => body,
            Err(err) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse::internal(format!(
                        "failed to serialize migration publish request: {err}"
                    ))),
                )
                    .into_response();
            }
        };
        return match forward_catalogue_request(
            &state,
            reqwest::Method::POST,
            "/admin/migrations",
            Some(body),
        )
        .await
        {
            Ok(response) => response,
            Err(error) => error.into_response(),
        };
    }

    let source_hash = match parse_schema_hash_param(&request.from_hash) {
        Ok(hash) => hash,
        Err(message) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse::bad_request(message)),
            )
                .into_response();
        }
    };

    let target_hash = match parse_schema_hash_param(&request.to_hash) {
        Ok(hash) => hash,
        Err(message) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse::bad_request(message)),
            )
                .into_response();
        }
    };

    match state.runtime.known_schema(&source_hash) {
        Ok(Some(_)) => {}
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                Json(ErrorResponse::not_found(format!(
                    "source schema catalogue not found for hash {}",
                    source_hash
                ))),
            )
                .into_response();
        }
        Err(err) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse::internal(format!(
                    "failed to read source schema catalogue: {err}"
                ))),
            )
                .into_response();
        }
    }

    match state.runtime.known_schema(&target_hash) {
        Ok(Some(_)) => {}
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                Json(ErrorResponse::not_found(format!(
                    "target schema catalogue not found for hash {}",
                    target_hash
                ))),
            )
                .into_response();
        }
        Err(err) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse::internal(format!(
                    "failed to read target schema catalogue: {err}"
                ))),
            )
                .into_response();
        }
    }

    let mut forward = LensTransform::new();
    for table_lens in request.forward {
        let table_name = table_lens.table;
        for operation in table_lens.operations {
            let op = match operation {
                PublishLensOp::Introduce {
                    column,
                    column_type,
                    value,
                } => LensOp::AddColumn {
                    table: table_name.clone(),
                    column,
                    column_type,
                    default: value,
                },
                PublishLensOp::Drop {
                    column,
                    column_type,
                    value,
                } => LensOp::RemoveColumn {
                    table: table_name.clone(),
                    column,
                    column_type,
                    default: value,
                },
                PublishLensOp::Rename { column, value } => LensOp::RenameColumn {
                    table: table_name.clone(),
                    old_name: column,
                    new_name: value,
                },
            };
            forward.push(op, false);
        }
    }

    let lens = Lens::new(source_hash, target_hash, forward);
    let object_id = match state.runtime.publish_lens(&lens) {
        Ok(object_id) => object_id,
        Err(err) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse::internal(format!(
                    "failed to publish migration lens: {err}"
                ))),
            )
                .into_response();
        }
    };

    if let Err(err) = state.runtime.flush().await {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse::internal(format!(
                "failed to flush published migration lens: {err}"
            ))),
        )
            .into_response();
    }

    (
        StatusCode::CREATED,
        Json(PublishMigrationResponse {
            object_id: object_id.to_string(),
            from_hash: request.from_hash,
            to_hash: request.to_hash,
        }),
    )
        .into_response()
}

async fn admin_subscription_introspection_handler(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
    Query(params): Query<AdminSubscriptionIntrospectionParams>,
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

    let Some(app_id_text) = params.app_id.as_deref() else {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse::bad_request(
                "appId query parameter is required",
            )),
        )
            .into_response();
    };

    let requested_app_id = match parse_app_id_param(app_id_text) {
        Ok(app_id) => app_id,
        Err(message) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse::bad_request(message)),
            )
                .into_response();
        }
    };

    if requested_app_id != state.app_id {
        return (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse::not_found(format!(
                "app not found: {}",
                app_id_text.trim()
            ))),
        )
            .into_response();
    }

    match state.runtime.server_subscription_telemetry() {
        Ok(queries) => Json(AdminSubscriptionIntrospectionResponse {
            app_id: state.app_id.to_string(),
            generated_at: unix_timestamp_millis(),
            queries,
        })
        .into_response(),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse::internal(format!(
                "failed to read subscription telemetry: {err}"
            ))),
        )
            .into_response(),
    }
}

fn parse_schema_hash_param(hash_text: &str) -> Result<SchemaHash, String> {
    let decoded_hash_bytes = hex::decode(hash_text)
        .map_err(|_| "invalid schema hash: expected hex string".to_string())?;
    if decoded_hash_bytes.len() != 32 {
        return Err("invalid schema hash: expected 64 hex chars".to_string());
    }

    let mut hash_bytes = [0u8; 32];
    hash_bytes.copy_from_slice(&decoded_hash_bytes);
    Ok(SchemaHash::from_bytes(hash_bytes))
}

fn parse_object_id_param(object_id_text: &str) -> Result<ObjectId, String> {
    let uuid = Uuid::parse_str(object_id_text)
        .map_err(|_| "invalid object id: expected UUID".to_string())?;
    Ok(ObjectId::from_uuid(uuid))
}

fn parse_app_id_param(app_id_text: &str) -> Result<AppId, String> {
    let trimmed = app_id_text.trim();
    if trimmed.is_empty() {
        return Err("invalid appId: expected UUID or app identifier".to_string());
    }

    if let Ok(app_id) = AppId::from_string(trimmed) {
        return Ok(app_id);
    }

    if trimmed
        .chars()
        .all(|char| char.is_ascii_alphanumeric() || matches!(char, '-' | '_' | '.'))
    {
        return Ok(AppId::from_name(trimmed));
    }

    Err("invalid appId: expected UUID or app identifier".to_string())
}

fn permissions_head_view(
    head: crate::schema_manager::manager::PermissionsHeadSummary,
) -> PermissionsHeadView {
    PermissionsHeadView {
        schema_hash: head.schema_hash.to_string(),
        version: head.version,
        parent_bundle_object_id: head
            .parent_bundle_object_id
            .map(|object_id| object_id.to_string()),
        bundle_object_id: head.bundle_object_id.to_string(),
    }
}

fn unix_timestamp_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}

async fn link_external_handler(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let (local_mode, local_token) = match parse_local_auth_headers(&headers) {
        Ok(Some(local)) => local,
        Ok(None) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse::bad_request(
                    "Local auth headers are required for link-external",
                )),
            )
                .into_response();
        }
        Err((status, msg)) => {
            return (status, Json(ErrorResponse::bad_request(msg))).into_response();
        }
    };

    if !state.auth_config.is_local_mode_enabled(local_mode) {
        let message = match local_mode {
            crate::middleware::auth::LocalAuthMode::Anonymous => "Anonymous auth disabled",
            crate::middleware::auth::LocalAuthMode::Demo => "Demo auth disabled",
        };
        return (
            StatusCode::FORBIDDEN,
            Json(ErrorResponse::unauthorized(message)),
        )
            .into_response();
    }

    let auth_value = match headers.get(AUTHORIZATION).and_then(|v| v.to_str().ok()) {
        Some(value) => value,
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse::bad_request(
                    "Authorization bearer token is required",
                )),
            )
                .into_response();
        }
    };
    let token = match auth_value.strip_prefix("Bearer ") {
        Some(token) if !token.trim().is_empty() => token.trim(),
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse::bad_request(
                    "Invalid Authorization header format",
                )),
            )
                .into_response();
        }
    };

    let jwt_result = if let Some(ref cache) = state.jwks_cache {
        crate::middleware::auth::validate_jwt_with_cache(token, cache).await
    } else {
        Err(crate::middleware::auth::JwtError::NoKeyConfigured)
    };

    let verified = match jwt_result {
        Ok(verified) => verified,
        Err(crate::middleware::auth::JwtError::NoKeyConfigured) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse::internal(
                    "JWT validation not configured".to_string(),
                )),
            )
                .into_response();
        }
        Err(crate::middleware::auth::JwtError::Invalid(_)) => {
            return (
                StatusCode::UNAUTHORIZED,
                Json(ErrorResponse::unauthorized("Invalid JWT")),
            )
                .into_response();
        }
    };

    let issuer = match verified.issuer.as_deref().map(str::trim) {
        Some(iss) if !iss.is_empty() => iss.to_string(),
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse::bad_request(
                    "JWT issuer (iss) is required for link-external",
                )),
            )
                .into_response();
        }
    };
    let subject = verified.subject.trim().to_string();
    if subject.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse::bad_request("JWT subject (sub) is required")),
        )
            .into_response();
    }

    let local_principal_id = derive_local_principal_id(state.app_id, local_mode, &local_token);
    if let Some(claim_principal) = verified
        .principal_id_claim
        .as_deref()
        .map(str::trim)
        .filter(|v| !v.is_empty())
        && claim_principal != local_principal_id
    {
        return (
            StatusCode::CONFLICT,
            Json(ErrorResponse::bad_request(
                "JWT jazz_principal_id claim does not match local principal",
            )),
        )
            .into_response();
    }

    let existing = match state
        .external_identity_store
        .get_external_identity(state.app_id, &issuer, &subject)
        .await
    {
        Ok(row) => row,
        Err(err) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse::internal(err)),
            )
                .into_response();
        }
    };

    let mut created = false;

    if let Some(row) = existing {
        if row.principal_id != local_principal_id {
            return (
                StatusCode::CONFLICT,
                Json(ErrorResponse::bad_request(
                    "external identity is already linked to a different principal",
                )),
            )
                .into_response();
        }
    } else {
        if let Err(err) = state
            .external_identity_store
            .create_external_identity(state.app_id, &issuer, &subject, &local_principal_id)
            .await
        {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse::internal(err)),
            )
                .into_response();
        }
        created = true;
    }

    {
        let mut mappings = state.external_identities.write().await;
        match mappings.get(&(issuer.clone(), subject.clone())) {
            Some(existing_principal) if existing_principal != &local_principal_id => {
                return (
                    StatusCode::CONFLICT,
                    Json(ErrorResponse::bad_request(
                        "external identity is already linked to a different principal",
                    )),
                )
                    .into_response();
            }
            _ => {
                mappings.insert(
                    (issuer.clone(), subject.clone()),
                    local_principal_id.clone(),
                );
            }
        }
    }

    Json(LinkExternalResponse {
        principal_id: local_principal_id,
        issuer,
        subject,
        created,
    })
    .into_response()
}

/// Health check endpoint.
async fn health_handler() -> impl IntoResponse {
    Json(serde_json::json!({
        "status": "healthy"
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::query_manager::query::QueryBuilder;
    use crate::query_manager::types::{
        ColumnType, SchemaBuilder, TableSchema, Value as QueryValue,
    };
    use crate::sync_manager::{
        ClientId, InboxEntry, QueryId, QueryPropagation, Source, SyncPayload,
    };
    use axum::body;
    use axum::routing::{get, post};
    use serde_json::Value;
    use tower::ServiceExt;

    use crate::middleware::AuthConfig;
    use crate::server::{CatalogueAuthorityMode, ServerBuilder, ServerState};

    fn test_auth_config() -> AuthConfig {
        AuthConfig {
            backend_secret: None,
            admin_secret: Some("admin-secret".to_string()),
            allow_anonymous: true,
            allow_demo: true,
            jwks_url: None,
        }
    }

    async fn make_state_with_schema(
        schema: crate::query_manager::types::Schema,
    ) -> Arc<ServerState> {
        ServerBuilder::new(AppId::from_name("test-app"))
            .with_auth_config(test_auth_config())
            .with_in_memory_storage()
            .with_schema(schema)
            .build()
            .await
            .expect("build state with schema")
            .state
    }

    async fn make_state_with_schema_and_authority(
        schema: crate::query_manager::types::Schema,
        catalogue_authority: CatalogueAuthorityMode,
    ) -> Arc<ServerState> {
        ServerBuilder::new(AppId::from_name("test-app"))
            .with_auth_config(test_auth_config())
            .with_catalogue_authority(catalogue_authority)
            .with_in_memory_storage()
            .with_schema(schema)
            .build()
            .await
            .expect("build state with schema and authority")
            .state
    }

    fn make_test_router(state: Arc<ServerState>) -> axum::Router {
        axum::Router::new()
            .route("/schema/:hash", get(schema_handler))
            .route("/schemas", get(schema_hashes_handler))
            .route("/admin/schemas", post(publish_schema_handler))
            .route("/admin/permissions/head", get(permissions_head_handler))
            .route("/admin/permissions", post(publish_permissions_handler))
            .route("/admin/migrations", post(publish_migration_handler))
            .route(
                "/admin/introspection/subscriptions",
                get(admin_subscription_introspection_handler),
            )
            .with_state(state)
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct ForwardedAdminRequest {
        method: String,
        path: String,
        admin_secret: Option<String>,
        body: Option<Value>,
    }

    #[tokio::test]
    async fn schema_handler_requires_admin_secret() {
        let state = ServerBuilder::new(AppId::from_name("test-app"))
            .with_auth_config(AuthConfig {
                backend_secret: None,
                admin_secret: Some("admin-secret".to_string()),
                allow_anonymous: true,
                allow_demo: true,
                jwks_url: None,
            })
            .with_in_memory_storage()
            .build()
            .await
            .expect("build server state")
            .state;

        let app = axum::Router::new()
            .route("/schema/:hash", get(schema_handler))
            .route("/schemas", get(schema_hashes_handler))
            .with_state(state);

        let placeholder_hash = "0000000000000000000000000000000000000000000000000000000000000000";
        let response = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri(format!("/schema/{placeholder_hash}"))
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

        let response_with_admin = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri(format!("/schema/{placeholder_hash}"))
                    .header("X-Jazz-Admin-Secret", "admin-secret")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response_with_admin.status(), StatusCode::NOT_FOUND);

        let hashes_without_admin = app
            .oneshot(
                axum::http::Request::builder()
                    .uri("/schemas")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(hashes_without_admin.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn schema_handlers_return_hashes_and_requested_schema() {
        let schema = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("name", ColumnType::Text),
            )
            .build();
        let schema_hash = SchemaHash::compute(&schema);
        let state = make_state_with_schema(schema).await;

        let app = make_test_router(state);

        let hashes_response = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri("/schemas")
                    .header("X-Jazz-Admin-Secret", "admin-secret")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(hashes_response.status(), StatusCode::OK);
        let hashes_body = body::to_bytes(hashes_response.into_body(), usize::MAX)
            .await
            .expect("hashes body");
        let hashes_json: Value = serde_json::from_slice(&hashes_body).expect("hashes json");
        let expected_hash = schema_hash.to_string();
        assert_eq!(
            hashes_json["hashes"][0].as_str(),
            Some(expected_hash.as_str())
        );

        let schema_response = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri(format!("/schema/{}", schema_hash))
                    .header("X-Jazz-Admin-Secret", "admin-secret")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(schema_response.status(), StatusCode::OK);

        let bad_hash_response = app
            .oneshot(
                axum::http::Request::builder()
                    .uri("/schema/invalid")
                    .header("X-Jazz-Admin-Secret", "admin-secret")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(bad_hash_response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn catalogue_authority_forwarding_proxies_schema_and_permissions_requests() {
        use std::sync::{Arc, Mutex};

        let forwarded = Arc::new(Mutex::new(Vec::<ForwardedAdminRequest>::new()));
        let forwarded_for_router = forwarded.clone();
        let expected_hash =
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string();
        let authority_routes = axum::Router::new()
            .route(
                "/schemas",
                get({
                    let forwarded = forwarded_for_router.clone();
                    let expected_hash = expected_hash.clone();
                    move |headers: HeaderMap| {
                        let forwarded = forwarded.clone();
                        let expected_hash = expected_hash.clone();
                        async move {
                            forwarded.lock().unwrap().push(ForwardedAdminRequest {
                                method: "GET".to_string(),
                                path: "/schemas".to_string(),
                                admin_secret: headers
                                    .get("X-Jazz-Admin-Secret")
                                    .and_then(|value| value.to_str().ok())
                                    .map(str::to_string),
                                body: None,
                            });
                            Json(serde_json::json!({ "hashes": [expected_hash] }))
                        }
                    }
                }),
            )
            .route(
                "/schema/:hash",
                get({
                    let forwarded = forwarded_for_router.clone();
                    move |Path(hash): Path<String>, headers: HeaderMap| {
                        let forwarded = forwarded.clone();
                        async move {
                            forwarded.lock().unwrap().push(ForwardedAdminRequest {
                                method: "GET".to_string(),
                                path: format!("/schema/{hash}"),
                                admin_secret: headers
                                    .get("X-Jazz-Admin-Secret")
                                    .and_then(|value| value.to_str().ok())
                                    .map(str::to_string),
                                body: None,
                            });
                            Json(serde_json::json!({
                                "users": {
                                    "columns": [
                                        { "name": "id", "column_type": { "type": "Uuid" }, "nullable": false },
                                        { "name": "name", "column_type": { "type": "Text" }, "nullable": false }
                                    ]
                                }
                            }))
                        }
                    }
                }),
            )
            .route(
                "/admin/schemas",
                post({
                    let forwarded = forwarded_for_router.clone();
                    let expected_hash = expected_hash.clone();
                    move |headers: HeaderMap, body: Json<Value>| {
                        let forwarded = forwarded.clone();
                        let expected_hash = expected_hash.clone();
                        async move {
                            forwarded.lock().unwrap().push(ForwardedAdminRequest {
                                method: "POST".to_string(),
                                path: "/admin/schemas".to_string(),
                                admin_secret: headers
                                    .get("X-Jazz-Admin-Secret")
                                    .and_then(|value| value.to_str().ok())
                                    .map(str::to_string),
                                body: Some(body.0),
                            });
                            (
                                StatusCode::CREATED,
                                Json(serde_json::json!({
                                    "objectId": "11111111-1111-1111-1111-111111111111",
                                    "hash": expected_hash,
                                })),
                            )
                        }
                    }
                }),
            )
            .route(
                "/admin/migrations",
                post({
                    let forwarded = forwarded_for_router.clone();
                    move |headers: HeaderMap, body: Json<Value>| {
                        let forwarded = forwarded.clone();
                        async move {
                            forwarded.lock().unwrap().push(ForwardedAdminRequest {
                                method: "POST".to_string(),
                                path: "/admin/migrations".to_string(),
                                admin_secret: headers
                                    .get("X-Jazz-Admin-Secret")
                                    .and_then(|value| value.to_str().ok())
                                    .map(str::to_string),
                                body: Some(body.0),
                            });
                            (
                                StatusCode::CREATED,
                                Json(serde_json::json!({
                                    "objectId": "22222222-2222-2222-2222-222222222222",
                                    "fromHash": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                                    "toHash": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                                })),
                            )
                        }
                    }
                }),
            )
            .route(
                "/admin/permissions/head",
                get({
                    let forwarded = forwarded_for_router.clone();
                    move |headers: HeaderMap| {
                        let forwarded = forwarded.clone();
                        async move {
                            forwarded.lock().unwrap().push(ForwardedAdminRequest {
                                method: "GET".to_string(),
                                path: "/admin/permissions/head".to_string(),
                                admin_secret: headers
                                    .get("X-Jazz-Admin-Secret")
                                    .and_then(|value| value.to_str().ok())
                                    .map(str::to_string),
                                body: None,
                            });
                            Json(serde_json::json!({
                                "head": {
                                    "schemaHash": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                                    "version": 4,
                                    "parentBundleObjectId": "33333333-3333-3333-3333-333333333333",
                                    "bundleObjectId": "44444444-4444-4444-4444-444444444444"
                                }
                            }))
                        }
                    }
                }),
            )
            .route(
                "/admin/permissions",
                post({
                    let forwarded = forwarded_for_router.clone();
                    move |headers: HeaderMap, body: Json<Value>| {
                        let forwarded = forwarded.clone();
                        async move {
                            forwarded.lock().unwrap().push(ForwardedAdminRequest {
                                method: "POST".to_string(),
                                path: "/admin/permissions".to_string(),
                                admin_secret: headers
                                    .get("X-Jazz-Admin-Secret")
                                    .and_then(|value| value.to_str().ok())
                                    .map(str::to_string),
                                body: Some(body.0),
                            });
                            (
                                StatusCode::CONFLICT,
                                Json(serde_json::json!({
                                    "error": {
                                        "code": "bad_request",
                                        "message": "stale permissions parent"
                                    }
                                })),
                            )
                        }
                    }
                }),
            );
        let authority_app = axum::Router::new().nest("/authority-prefix", authority_routes);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind authority listener");
        let authority_addr = listener.local_addr().expect("authority local addr");
        let authority_task = tokio::spawn(async move {
            axum::serve(listener, authority_app)
                .await
                .expect("serve authority app");
        });

        let schema = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("name", ColumnType::Text),
            )
            .build();
        let state = make_state_with_schema_and_authority(
            schema.clone(),
            CatalogueAuthorityMode::Forward {
                base_url: format!("http://{authority_addr}/authority-prefix"),
                admin_secret: "authority-secret".to_string(),
            },
        )
        .await;
        let app = make_test_router(state);

        let schemas_response = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri("/schemas")
                    .header("X-Jazz-Admin-Secret", "admin-secret")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(schemas_response.status(), StatusCode::OK);

        let schema_response = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri(format!("/schema/{expected_hash}"))
                    .header("X-Jazz-Admin-Secret", "admin-secret")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(schema_response.status(), StatusCode::OK);

        let publish_schema_response = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/admin/schemas")
                    .header("Content-Type", "application/json")
                    .header("X-Jazz-Admin-Secret", "admin-secret")
                    .body(axum::body::Body::from(
                        serde_json::json!({ "schema": schema }).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(publish_schema_response.status(), StatusCode::CREATED);

        let publish_migration_response = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/admin/migrations")
                    .header("Content-Type", "application/json")
                    .header("X-Jazz-Admin-Secret", "admin-secret")
                    .body(axum::body::Body::from(
                        serde_json::json!({
                            "fromHash": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                            "toHash": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                            "forward": [{
                                "table": "users",
                                "operations": [{
                                    "type": "rename",
                                    "column": "name",
                                    "value": "full_name"
                                }]
                            }]
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(publish_migration_response.status(), StatusCode::CREATED);

        let permissions_head_response = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri("/admin/permissions/head")
                    .header("X-Jazz-Admin-Secret", "admin-secret")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(permissions_head_response.status(), StatusCode::OK);

        let publish_permissions_response = app
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/admin/permissions")
                    .header("Content-Type", "application/json")
                    .header("X-Jazz-Admin-Secret", "admin-secret")
                    .body(axum::body::Body::from(
                        serde_json::json!({
                            "schemaHash": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                            "permissions": {
                                "users": {
                                    "select": { "using": { "type": "True" } }
                                }
                            },
                            "expectedParentBundleObjectId": "44444444-4444-4444-4444-444444444444"
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(publish_permissions_response.status(), StatusCode::CONFLICT);

        let forwarded = forwarded.lock().unwrap().clone();
        assert_eq!(forwarded.len(), 6);
        assert!(
            forwarded
                .iter()
                .all(|request| request.admin_secret.as_deref() == Some("authority-secret"))
        );
        assert_eq!(forwarded[0].path, "/schemas");
        assert_eq!(forwarded[1].path, format!("/schema/{expected_hash}"));
        assert_eq!(forwarded[2].path, "/admin/schemas");
        assert_eq!(forwarded[3].path, "/admin/migrations");
        assert_eq!(forwarded[4].path, "/admin/permissions/head");
        assert_eq!(forwarded[5].path, "/admin/permissions");
        assert_eq!(
            forwarded[5]
                .body
                .as_ref()
                .and_then(|body| body.get("expectedParentBundleObjectId"))
                .and_then(Value::as_str),
            Some("44444444-4444-4444-4444-444444444444")
        );

        authority_task.abort();
    }

    #[tokio::test]
    async fn permissions_handlers_publish_linear_head_and_reject_stale_parent() {
        let schema = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("name", ColumnType::Text),
            )
            .build();
        let schema_hash = SchemaHash::compute(&schema);
        let state = make_state_with_schema(schema).await;
        let app = make_test_router(state.clone());

        let initial_head = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri("/admin/permissions/head")
                    .header("X-Jazz-Admin-Secret", "admin-secret")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(initial_head.status(), StatusCode::OK);
        let initial_body = body::to_bytes(initial_head.into_body(), usize::MAX)
            .await
            .expect("initial permissions head body");
        let initial_json: Value =
            serde_json::from_slice(&initial_body).expect("initial permissions head json");
        assert!(initial_json["head"].is_null());

        let first_request_body = serde_json::json!({
            "schemaHash": schema_hash.to_string(),
            "permissions": {
                "users": {
                    "select": { "using": { "type": "True" } }
                }
            }
        });
        let first_response = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/admin/permissions")
                    .header("Content-Type", "application/json")
                    .header("X-Jazz-Admin-Secret", "admin-secret")
                    .body(axum::body::Body::from(first_request_body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(first_response.status(), StatusCode::CREATED);
        let first_body = body::to_bytes(first_response.into_body(), usize::MAX)
            .await
            .expect("first publish body");
        let first_json: Value = serde_json::from_slice(&first_body).expect("first publish json");
        let first_bundle_object_id = first_json["head"]["bundleObjectId"]
            .as_str()
            .expect("first bundle object id")
            .to_string();
        assert_eq!(first_json["head"]["version"].as_u64(), Some(1));
        assert_eq!(first_json["head"]["parentBundleObjectId"], Value::Null);

        let second_request_body = serde_json::json!({
            "schemaHash": schema_hash.to_string(),
            "permissions": {
                "users": {
                    "select": { "using": { "type": "False" } }
                }
            },
            "expectedParentBundleObjectId": first_bundle_object_id,
        });
        let second_response = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/admin/permissions")
                    .header("Content-Type", "application/json")
                    .header("X-Jazz-Admin-Secret", "admin-secret")
                    .body(axum::body::Body::from(second_request_body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(second_response.status(), StatusCode::CREATED);
        let second_body = body::to_bytes(second_response.into_body(), usize::MAX)
            .await
            .expect("second publish body");
        let second_json: Value = serde_json::from_slice(&second_body).expect("second publish json");
        let second_bundle_object_id = second_json["head"]["bundleObjectId"]
            .as_str()
            .expect("second bundle object id")
            .to_string();
        assert_eq!(second_json["head"]["version"].as_u64(), Some(2));
        assert_eq!(
            second_json["head"]["parentBundleObjectId"].as_str(),
            Some(first_bundle_object_id.as_str())
        );

        let stale_request_body = serde_json::json!({
            "schemaHash": schema_hash.to_string(),
            "permissions": {
                "users": {
                    "select": { "using": { "type": "True" } }
                }
            },
            "expectedParentBundleObjectId": first_bundle_object_id,
        });
        let stale_response = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/admin/permissions")
                    .header("Content-Type", "application/json")
                    .header("X-Jazz-Admin-Secret", "admin-secret")
                    .body(axum::body::Body::from(stale_request_body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(stale_response.status(), StatusCode::CONFLICT);

        let head_response = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri("/admin/permissions/head")
                    .header("X-Jazz-Admin-Secret", "admin-secret")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(head_response.status(), StatusCode::OK);
        let head_body = body::to_bytes(head_response.into_body(), usize::MAX)
            .await
            .expect("current permissions head body");
        let head_json: Value =
            serde_json::from_slice(&head_body).expect("current permissions head json");
        assert_eq!(head_json["head"]["version"].as_u64(), Some(2));
        assert_eq!(
            head_json["head"]["bundleObjectId"].as_str(),
            Some(second_bundle_object_id.as_str())
        );
    }

    #[tokio::test]
    async fn publish_schema_rejects_inline_permissions() {
        let schema = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("name", ColumnType::Text),
            )
            .build();
        let state = make_state_with_schema(schema.clone()).await;
        let app = make_test_router(state);

        let request_body = serde_json::json!({
            "schema": schema,
            "permissions": {
                "users": {
                    "select": { "using": { "type": "True" } }
                }
            }
        });

        let response = app
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/admin/schemas")
                    .header("Content-Type", "application/json")
                    .header("X-Jazz-Admin-Secret", "admin-secret")
                    .body(axum::body::Body::from(request_body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn publish_migration_requires_admin_and_persists_lens() {
        let v1 = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("email", ColumnType::Text),
            )
            .build();
        let v2 = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("email_address", ColumnType::Text),
            )
            .build();

        let v1_hash = SchemaHash::compute(&v1);
        let v2_hash = SchemaHash::compute(&v2);

        let state = make_state_with_schema(v2).await;
        state
            .runtime
            .add_known_schema(v1)
            .expect("seed known schema for publish test");
        let app = make_test_router(state.clone());

        let request_body = serde_json::json!({
            "fromHash": v1_hash.to_string(),
            "toHash": v2_hash.to_string(),
            "forward": [{
                "table": "users",
                "operations": [{
                    "type": "rename",
                    "column": "email",
                    "value": "email_address"
                }]
            }]
        });

        let unauthorized = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/admin/migrations")
                    .header("Content-Type", "application/json")
                    .body(axum::body::Body::from(request_body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);

        let created = app
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/admin/migrations")
                    .header("Content-Type", "application/json")
                    .header("X-Jazz-Admin-Secret", "admin-secret")
                    .body(axum::body::Body::from(request_body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(created.status(), StatusCode::CREATED);

        let lens = state
            .runtime
            .with_schema_manager(|schema_manager| {
                schema_manager.get_lens(&v1_hash, &v2_hash).cloned()
            })
            .expect("read schema manager lens");
        assert!(
            lens.is_some(),
            "published lens should be registered in schema manager"
        );
    }

    #[tokio::test]
    async fn admin_subscription_introspection_requires_admin_secret_and_valid_app_id() {
        let schema = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("name", ColumnType::Text),
            )
            .build();
        let state = make_state_with_schema(schema).await;
        let app = make_test_router(state.clone());

        let without_secret = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri("/admin/introspection/subscriptions?appId=test-app")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(without_secret.status(), StatusCode::UNAUTHORIZED);

        let wrong_secret = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri("/admin/introspection/subscriptions?appId=test-app")
                    .header("X-Jazz-Admin-Secret", "wrong-secret")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(wrong_secret.status(), StatusCode::UNAUTHORIZED);

        let missing_app_id = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri("/admin/introspection/subscriptions")
                    .header("X-Jazz-Admin-Secret", "admin-secret")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(missing_app_id.status(), StatusCode::BAD_REQUEST);

        let invalid_app_id = app
            .oneshot(
                axum::http::Request::builder()
                    .uri("/admin/introspection/subscriptions?appId=bad/id")
                    .header("X-Jazz-Admin-Secret", "admin-secret")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(invalid_app_id.status(), StatusCode::BAD_REQUEST);

        let mismatched_app_id = make_test_router(state)
            .oneshot(
                axum::http::Request::builder()
                    .uri("/admin/introspection/subscriptions?appId=other-app")
                    .header("X-Jazz-Admin-Secret", "admin-secret")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(mismatched_app_id.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn admin_subscription_introspection_groups_active_server_subscriptions() {
        let schema = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("name", ColumnType::Text),
            )
            .build();
        let state = make_state_with_schema(schema).await;

        let repeated_query = QueryBuilder::new("users").build();
        let filtered_query = QueryBuilder::new("users")
            .filter_eq("name", QueryValue::Text("Alice".to_string()))
            .build();

        for (index, query, propagation) in [
            (1_u64, repeated_query.clone(), QueryPropagation::Full),
            (2_u64, repeated_query.clone(), QueryPropagation::Full),
            (3_u64, repeated_query.clone(), QueryPropagation::LocalOnly),
            (4_u64, filtered_query, QueryPropagation::Full),
        ] {
            let client_id = ClientId::new();
            state.runtime.add_client(client_id, None).unwrap();
            state
                .runtime
                .push_sync_inbox(InboxEntry {
                    source: Source::Client(client_id),
                    payload: SyncPayload::QuerySubscription {
                        query_id: QueryId(index),
                        query: Box::new(query),
                        session: None,
                        propagation,
                    },
                })
                .unwrap();
        }
        state.runtime.flush().await.unwrap();

        let response = make_test_router(state.clone())
            .oneshot(
                axum::http::Request::builder()
                    .uri("/admin/introspection/subscriptions?appId=test-app")
                    .header("X-Jazz-Admin-Secret", "admin-secret")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("telemetry body");
        let json: Value = serde_json::from_slice(&body).expect("telemetry json");

        let expected_app_id = state.app_id.to_string();
        assert_eq!(json["appId"].as_str(), Some(expected_app_id.as_str()));
        assert!(json["generatedAt"].as_u64().is_some());

        let groups = json["queries"].as_array().expect("queries array");
        assert_eq!(groups.len(), 3);

        let repeated_full = groups.iter().find(|group| {
            group["count"].as_u64() == Some(2) && group["propagation"].as_str() == Some("full")
        });
        let repeated_full = repeated_full.expect("expected grouped full subscriptions");
        assert_eq!(repeated_full["table"].as_str(), Some("users"));
        assert_eq!(
            repeated_full["branches"]
                .as_array()
                .map(|branches| branches.len())
                .unwrap_or_default(),
            1
        );
        assert!(repeated_full["groupKey"].as_str().is_some());

        assert!(groups.iter().any(|group| {
            group["count"].as_u64() == Some(1)
                && group["propagation"].as_str() == Some("local-only")
        }));
        assert!(groups.iter().any(|group| {
            group["count"].as_u64() == Some(1)
                && group["query"]
                    .as_str()
                    .map(|query| query.contains("\"name\""))
                    .unwrap_or(false)
        }));
    }
}
