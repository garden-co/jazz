//! HTTP routes for the Jazz server.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::{
    Router,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode, header::AUTHORIZATION},
    response::{IntoResponse, Json},
    routing::{get, post},
};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;

use crate::jazz_transport::{
    ConnectionId, ErrorResponse, ServerEvent, SyncBatchRequest, SyncBatchResponse,
    SyncPayloadResult,
};
use crate::middleware::auth::{
    derive_local_principal_id, extract_session, parse_local_auth_headers, validate_admin_secret,
    validate_backend_secret, validate_jwt_identity,
};
use crate::query_manager::types::SchemaHash;
use crate::schema_manager::{AppId, Lens, parse_lens};
use crate::server::{ConnectionState, ServerState};
use crate::sync_manager::ClientId;

/// Create the router with all routes.
pub fn create_router(state: Arc<ServerState>) -> Router {
    let traced_routes = Router::new()
        .route("/sync", post(sync_handler))
        .route("/schema/:hash", get(schema_handler))
        .route("/schemas", get(schema_hashes_handler))
        .route("/admin/migrations", post(publish_migration_handler))
        .route(
            "/admin/introspection/subscriptions",
            get(admin_subscription_introspection_handler),
        )
        // Link a local anonymous/demo principal to an external identity.
        .route("/auth/link-external", post(link_external_handler))
        // Health check
        .route("/health", get(health_handler))
        .layer(TraceLayer::new_for_http());

    Router::new()
        .route("/events", get(events_handler))
        .merge(traced_routes)
        .layer(CorsLayer::permissive())
        .with_state(state)
}

/// Query parameters for events endpoint.
#[derive(Debug, Deserialize)]
struct EventsParams {
    /// Client-provided ID for reconnect support.
    client_id: Option<String>,
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

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PublishMigrationRequest {
    from_hash: String,
    to_hash: String,
    forward_sql: String,
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

/// Encode a ServerEvent as a length-prefixed binary frame.
///
/// Format: [4 bytes: u32 big-endian length][N bytes: JSON]
fn encode_frame(event: &ServerEvent) -> Bytes {
    let json = serde_json::to_vec(event).unwrap_or_default();
    let len = (json.len() as u32).to_be_bytes();
    let mut buf = Vec::with_capacity(4 + json.len());
    buf.extend_from_slice(&len);
    buf.extend_from_slice(&json);
    Bytes::from(buf)
}

/// Binary streaming events endpoint - clients connect here for all updates.
///
/// Uses length-prefixed binary frames over a chunked HTTP response.
/// Auth via Authorization header (JWT) or X-Jazz-Backend-Secret.
async fn events_handler(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
    Query(params): Query<EventsParams>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    // Parse client_id from query param - error if malformed, generate if missing
    let client_id = match params.client_id {
        Some(s) => ClientId::parse(&s)
            .ok_or((StatusCode::BAD_REQUEST, format!("Invalid client_id: {}", s)))?,
        None => ClientId::new(),
    };

    {
        let _span = tracing::debug_span!("events_handler", %client_id).entered();
        tracing::info!(%client_id, "events stream connecting");
    }

    let backend_secret = headers
        .get("X-Jazz-Backend-Secret")
        .and_then(|v| v.to_str().ok());
    let has_session_header = headers.get("X-Jazz-Session").is_some();

    if backend_secret.is_some() && !has_session_header {
        if let Err((status, msg)) = validate_backend_secret(backend_secret, &state.auth_config) {
            return Err((status, msg.to_string()));
        }
        let _ = state.runtime.add_client(client_id, None);
        let _ = state.runtime.set_client_backend(client_id);
    } else {
        // Extract session from headers (JWT, local auth, or backend impersonation)
        let session = {
            let external_identities = state.external_identities.read().await;
            match extract_session(
                &headers,
                state.app_id,
                &state.auth_config,
                Some(&external_identities),
            ) {
                Ok(s) => s,
                Err((status, msg)) => {
                    return Err((status, msg.to_string()));
                }
            }
        };

        // Require a valid session — reject connections without authentication.
        let session = match session {
            Some(s) => s,
            None => {
                tracing::error!(
                    "Stream connection rejected: no session (client_id={}). Client must send auth headers.",
                    client_id
                );
                return Err((
                    StatusCode::UNAUTHORIZED,
                    "Session required for event stream. Provide JWT, local auth headers, or backend secret."
                        .to_string(),
                ));
            }
        };

        // Ensure client is registered with session (idempotent — won't overwrite
        // existing role if client was already registered by a /sync request).
        let _ = state.runtime.ensure_client_with_session(client_id, session);
    }

    // Generate connection ID
    let connection_id = state
        .next_connection_id
        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

    // Subscribe to broadcast channel for this client's events
    let mut sync_rx = state.sync_broadcast.subscribe();

    // Store connection state
    {
        let mut connections = state.connections.write().await;
        connections.insert(
            connection_id,
            ConnectionState {
                _client_id: client_id,
            },
        );
    }

    // Clone state for cleanup on drop
    let state_cleanup = state.clone();
    let connection_id_cleanup = connection_id;

    // Capture client_id string for stream
    let client_id_str = client_id.to_string();
    let catalogue_state_hash = state.runtime.catalogue_state_hash().ok();

    // Create stream that emits length-prefixed binary frames
    let stream = async_stream::stream! {
        // Send Connected frame
        let connected = ServerEvent::Connected {
            connection_id: ConnectionId(connection_id),
            client_id: client_id_str.clone(),
            next_sync_seq: None,
            catalogue_state_hash: catalogue_state_hash.clone(),
        };
        yield Ok::<Bytes, std::convert::Infallible>(encode_frame(&connected));

        // Heartbeat interval
        let mut heartbeat_interval = tokio::time::interval(Duration::from_secs(30));

        loop {
            tokio::select! {
                // Check for sync updates for this client
                result = sync_rx.recv() => {
                    match result {
                        Ok((target_client_id, payload)) => {
                            // Only emit if this is for our client
                            if target_client_id == client_id {
                                let event = ServerEvent::SyncUpdate {
                                    seq: None,
                                    payload: Box::new(payload),
                                };
                                yield Ok(encode_frame(&event));
                            }
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                            // We fell behind, continue
                            tracing::warn!("Stream client {} lagged behind on sync updates", connection_id);
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                            // Channel closed, exit
                            break;
                        }
                    }
                }
                // Send periodic heartbeat
                _ = heartbeat_interval.tick() => {
                    let heartbeat = ServerEvent::Heartbeat;
                    yield Ok(encode_frame(&heartbeat));
                }
            }
        }

        // Cleanup on stream close
        {
            let mut connections = state_cleanup.connections.write().await;
            connections.remove(&connection_id_cleanup);
        }
        // Keep logical client state across disconnects so reconnect with the same
        // client_id can resume query forwarding state.
        tracing::debug!(
            "Stream connection {} closed (client state retained for resume)",
            connection_id_cleanup
        );
    };

    Ok(axum::response::Response::builder()
        .header("Content-Type", "application/octet-stream")
        .header("Transfer-Encoding", "chunked")
        .header("Cache-Control", "no-cache")
        .body(axum::body::Body::from_stream(stream))
        .unwrap())
}

/// Push an ordered batch of sync payloads to the server's inbox.
///
/// Auth is checked once per request. Payloads are applied sequentially and
/// a per-payload result is returned for each entry in the batch.
async fn sync_handler(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
    Json(request): Json<SyncBatchRequest>,
) -> impl IntoResponse {
    use crate::sync_manager::{InboxEntry, Source};

    tracing::debug!(
        client_id = %request.client_id,
        payload_count = request.payloads.len(),
        "sync batch request",
    );

    // Check admin secret — if present and valid, promote client to Admin role
    let is_admin = {
        let admin_secret = headers
            .get("X-Jazz-Admin-Secret")
            .and_then(|v| v.to_str().ok());

        if admin_secret.is_some() {
            if let Err((status, msg)) = validate_admin_secret(admin_secret, &state.auth_config) {
                return (status, Json(ErrorResponse::unauthorized(msg))).into_response();
            }
            true
        } else {
            false
        }
    };

    // Admin-authenticated requests (server-to-server catalogue sync) don't need a session.
    // Regular clients must provide JWT or backend secret.
    if is_admin {
        if let Err(e) = state.runtime.add_client(request.client_id, None) {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse::internal(e.to_string())),
            )
                .into_response();
        }
        if let Err(e) = state.runtime.set_client_admin(request.client_id) {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse::internal(e.to_string())),
            )
                .into_response();
        }
    } else if headers.get("X-Jazz-Backend-Secret").is_some()
        && headers.get("X-Jazz-Session").is_none()
    {
        let backend_secret = headers
            .get("X-Jazz-Backend-Secret")
            .and_then(|v| v.to_str().ok());
        if let Err((status, msg)) = validate_backend_secret(backend_secret, &state.auth_config) {
            return (status, Json(ErrorResponse::unauthorized(msg))).into_response();
        }
        if let Err(e) = state.runtime.add_client(request.client_id, None) {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse::internal(e.to_string())),
            )
                .into_response();
        }
        if let Err(e) = state.runtime.set_client_backend(request.client_id) {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse::internal(e.to_string())),
            )
                .into_response();
        }
    } else {
        // Extract session from headers (JWT or backend impersonation)
        let session = {
            let external_identities = state.external_identities.read().await;
            match extract_session(
                &headers,
                state.app_id,
                &state.auth_config,
                Some(&external_identities),
            ) {
                Ok(Some(s)) => s,
                Ok(None) => {
                    tracing::error!(
                        "Sync request rejected: no session (client_id={}). Client must send auth headers.",
                        request.client_id
                    );
                    return (
                        StatusCode::UNAUTHORIZED,
                        Json(ErrorResponse::unauthorized(
                            "Session required for sync. Provide JWT, local auth headers, or backend secret.",
                        )),
                    )
                        .into_response();
                }
                Err((status, msg)) => {
                    return (status, Json(ErrorResponse::unauthorized(msg))).into_response();
                }
            }
        };

        // Ensure client is registered with session bound
        if let Err(e) = state
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

    // Apply each payload in order, collecting per-payload results.
    let mut results = Vec::with_capacity(request.payloads.len());
    for payload in request.payloads {
        let entry = InboxEntry {
            source: Source::Client(request.client_id),
            payload,
        };
        match state.runtime.push_sync_inbox(entry) {
            Ok(()) => results.push(SyncPayloadResult {
                ok: true,
                error: None,
            }),
            Err(e) => results.push(SyncPayloadResult {
                ok: false,
                error: Some(e.to_string()),
            }),
        }
    }

    Json(SyncBatchResponse { results }).into_response()
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

    let forward = match parse_lens(&request.forward_sql) {
        Ok(transform) => transform,
        Err(err) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse::bad_request(format!(
                    "invalid forward migration SQL: {err}"
                ))),
            )
                .into_response();
        }
    };

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

    let verified = match validate_jwt_identity(token, &state.auth_config) {
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
    use crate::server::{ServerBuilder, ServerState};

    fn test_auth_config() -> AuthConfig {
        AuthConfig {
            backend_secret: None,
            admin_secret: Some("admin-secret".to_string()),
            allow_anonymous: true,
            allow_demo: true,
            jwks_url: None,
            jwks_set: None,
        }
    }

    /// Spin up the full router backed by an in-process runtime.
    /// `backend_secret` is wired into `AuthConfig` so tests can authenticate
    /// via the `X-Jazz-Backend-Secret` header without needing JWT setup.
    async fn make_sync_test_app(backend_secret: &str) -> axum::Router {
        let auth_config = AuthConfig {
            backend_secret: Some(backend_secret.to_string()),
            admin_secret: None,
            allow_anonymous: true,
            allow_demo: true,
            jwks_url: None,
            jwks_set: None,
        };

        ServerBuilder::new(AppId::from_name("test-app"))
            .with_auth_config(auth_config)
            .with_in_memory_storage()
            .build()
            .await
            .expect("build sync test app")
            .app
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

    fn make_test_router(state: Arc<ServerState>) -> axum::Router {
        axum::Router::new()
            .route("/schema/:hash", get(schema_handler))
            .route("/schemas", get(schema_hashes_handler))
            .route("/admin/migrations", post(publish_migration_handler))
            .route(
                "/admin/introspection/subscriptions",
                get(admin_subscription_introspection_handler),
            )
            .with_state(state)
    }

    /// A minimal valid `SyncPayload::ObjectUpdated` as a `serde_json::Value`,
    /// suitable for embedding in batch request bodies.
    fn object_updated_payload(object_id: &str) -> Value {
        serde_json::json!({
            "ObjectUpdated": {
                "object_id": object_id,
                "metadata": null,
                "branch_name": "main",
                "commits": []
            }
        })
    }

    // -------------------------------------------------------------------------
    // Batch sync handler tests
    //
    // These are RED: the /sync endpoint currently expects
    // {"payload":…,"client_id":…} (singular). Sending the new always-array
    // {"payloads":[…],"client_id":…} body currently returns 422. These tests
    // will go green once SyncPayloadRequest is replaced with SyncBatchRequest
    // and sync_handler returns {"results":[…]}.
    // -------------------------------------------------------------------------

    #[tokio::test]
    async fn sync_batch_accepts_two_payloads_and_returns_ok_results() {
        // alice fires two position updates in the same tick — they should land
        // in a single POST and both be acknowledged
        //
        //  alice (client)          server
        //    ──[p1, p2]──────────► /sync
        //                          apply p1 → ok
        //                          apply p2 → ok
        //    ◄────────────────── {results:[ok,ok]}

        let app = make_sync_test_app("test-backend-secret").await;
        let client_id = ClientId::new();

        let body = serde_json::json!({
            "payloads": [
                object_updated_payload("00000000-0000-0000-0000-000000000001"),
                object_updated_payload("00000000-0000-0000-0000-000000000002"),
            ],
            "client_id": client_id,
        });

        let response = app
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/sync")
                    .header("Content-Type", "application/json")
                    .header("X-Jazz-Backend-Secret", "test-backend-secret")
                    .body(axum::body::Body::from(serde_json::to_vec(&body).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let bytes = body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: Value = serde_json::from_slice(&bytes).unwrap();

        let results = json["results"].as_array().expect("results array");
        assert_eq!(results.len(), 2);
        assert_eq!(results[0]["ok"], true);
        assert_eq!(results[1]["ok"], true);
    }

    #[tokio::test]
    async fn sync_batch_requires_auth() {
        // No auth headers → 401 before any payloads are applied
        let app = make_sync_test_app("test-backend-secret").await;

        let body = serde_json::json!({
            "payloads": [object_updated_payload("00000000-0000-0000-0000-000000000001")],
            "client_id": ClientId::new(),
        });

        let response = app
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/sync")
                    .header("Content-Type", "application/json")
                    // deliberately no X-Jazz-Backend-Secret
                    .body(axum::body::Body::from(serde_json::to_vec(&body).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn sync_batch_returns_one_result_per_payload() {
        // bob sends 60 position updates in one tick — server must return
        // exactly 60 results, one per payload, in order
        let app = make_sync_test_app("test-backend-secret").await;
        let client_id = ClientId::new();

        let payloads: Vec<Value> = (0..60)
            .map(|i| object_updated_payload(&format!("00000000-0000-0000-0000-{:012}", i)))
            .collect();

        let body = serde_json::json!({
            "payloads": payloads,
            "client_id": client_id,
        });

        let response = app
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/sync")
                    .header("Content-Type", "application/json")
                    .header("X-Jazz-Backend-Secret", "test-backend-secret")
                    .body(axum::body::Body::from(serde_json::to_vec(&body).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let bytes = body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: Value = serde_json::from_slice(&bytes).unwrap();

        let results = json["results"].as_array().expect("results array");
        assert_eq!(results.len(), 60);
        for result in results {
            assert_eq!(result["ok"], true);
        }
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
                jwks_set: None,
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
            "forwardSql": "ALTER TABLE users RENAME COLUMN email TO email_address;"
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
