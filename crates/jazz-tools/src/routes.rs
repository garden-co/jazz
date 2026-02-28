//! HTTP routes for the Jazz server.

use std::sync::Arc;
use std::time::Duration;

use axum::{
    Router,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode, header::AUTHORIZATION},
    response::{IntoResponse, Json},
    routing::{get, post},
};
use bytes::Bytes;
use jazz_tools::jazz_transport::{
    ConnectionId, ErrorResponse, ServerEvent, SuccessResponse, SyncPayloadRequest,
};
use jazz_tools::sync_manager::ClientId;
use serde::{Deserialize, Serialize};
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;

use crate::commands::server::{ConnectionState, ServerState};
use crate::middleware::auth::{
    derive_local_principal_id, extract_session, parse_local_auth_headers, validate_admin_secret,
    validate_jwt_identity,
};
use jazz_tools::query_manager::types::SchemaHash;
use jazz_tools::schema_manager::CatalogueSchemaResponse;

/// Create the router with all routes.
pub fn create_router(state: Arc<ServerState>) -> Router {
    let traced_routes = Router::new()
        .route("/sync", post(sync_handler))
        .route("/schema/:hash", get(schema_handler))
        .route("/schemas", get(schema_hashes_handler))
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

    // Extract session from headers (JWT or backend impersonation)
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

    // Generate connection ID
    let connection_id = state
        .next_connection_id
        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

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

    // Create stream that emits length-prefixed binary frames
    let stream = async_stream::stream! {
        // Send Connected frame
        let connected = ServerEvent::Connected {
            connection_id: ConnectionId(connection_id),
            client_id: client_id_str.clone(),
            next_sync_seq: None,
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

/// Push a sync payload to the server's inbox.
///
/// Admin clients (with valid admin secret) can write catalogue objects.
/// Session is extracted from headers and bound to the client_id.
async fn sync_handler(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
    Json(request): Json<SyncPayloadRequest>,
) -> impl IntoResponse {
    use jazz_tools::sync_manager::{InboxEntry, Source};

    let payload_size = serde_json::to_vec(&request.payload)
        .map(|v| v.len())
        .unwrap_or(0);
    {
        let _span = tracing::debug_span!(
            "sync_handler",
            client_id = %request.client_id,
            payload_size,
        )
        .entered();
        tracing::info!(client_id = %request.client_id, payload = request.payload.variant_name(), "sync request");
    }

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

    let entry = InboxEntry {
        source: Source::Client(request.client_id),
        payload: request.payload,
    };

    match state.runtime.push_sync_inbox(entry) {
        Ok(()) => Json(SuccessResponse::default()).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse::internal(e.to_string())),
        )
            .into_response(),
    }
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
            let body = CatalogueSchemaResponse::from(&schema);
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
    use std::collections::HashMap;

    use axum::body;
    use axum::routing::get;
    use groove::query_manager::types::{ColumnType, SchemaBuilder, TableSchema};
    use groove::runtime_tokio::TokioRuntime;
    use groove::schema_manager::{AppId, SchemaManager};
    use groove::storage::SurrealKvStorage;
    use groove::sync_manager::{ClientId, PersistenceTier, SyncManager, SyncPayload};
    use serde_json::Value;
    use tempfile::TempDir;
    use tokio::sync::{RwLock, broadcast};
    use tower::util::ServiceExt;

    use crate::commands::server::{ExternalIdentityStore, ServerState};
    use crate::middleware::AuthConfig;

    #[tokio::test]
    async fn schema_handler_requires_admin_secret() {
        let data_dir = TempDir::new().expect("temp dir");
        let db_path = data_dir.path().join("groove.surrealkv");
        let storage =
            SurrealKvStorage::open(&db_path, 64 * 1024 * 1024).expect("open test storage");

        let sync_manager = SyncManager::new().with_tier(PersistenceTier::EdgeServer);
        let schema_manager =
            SchemaManager::new_server(sync_manager, AppId::from_name("test-app"), "prod");
        let runtime = TokioRuntime::new(schema_manager, storage, |_entry| {});

        let auth_config = AuthConfig {
            backend_secret: None,
            admin_secret: Some("admin-secret".to_string()),
            allow_anonymous: true,
            allow_demo: true,
            jwks_url: None,
            jwks_set: None,
        };

        let (sync_tx, _) = broadcast::channel::<(ClientId, SyncPayload)>(16);

        let state = Arc::new(ServerState {
            runtime,
            app_id: AppId::from_name("test-app"),
            connections: RwLock::new(HashMap::new()),
            next_connection_id: std::sync::atomic::AtomicU64::new(1),
            sync_broadcast: sync_tx,
            auth_config,
            external_identity_store: Arc::new(
                ExternalIdentityStore::new(data_dir.path().to_str().unwrap()).unwrap(),
            ),
            external_identities: RwLock::new(HashMap::new()),
        });

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
        let data_dir = TempDir::new().expect("temp dir");
        let db_path = data_dir.path().join("groove.surrealkv");
        let storage =
            SurrealKvStorage::open(&db_path, 64 * 1024 * 1024).expect("open test storage");

        let schema = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("name", ColumnType::Text),
            )
            .build();
        let schema_hash = SchemaHash::compute(&schema);
        let sync_manager = SyncManager::new().with_tier(PersistenceTier::EdgeServer);
        let schema_manager = SchemaManager::new(
            sync_manager,
            schema,
            AppId::from_name("test-app"),
            "prod",
            "main",
        )
        .expect("schema manager");
        let runtime = TokioRuntime::new(schema_manager, storage, |_entry| {});

        let auth_config = AuthConfig {
            backend_secret: None,
            admin_secret: Some("admin-secret".to_string()),
            allow_anonymous: true,
            allow_demo: true,
            jwks_url: None,
            jwks_set: None,
        };

        let (sync_tx, _) = broadcast::channel::<(ClientId, SyncPayload)>(16);

        let state = Arc::new(ServerState {
            runtime,
            app_id: AppId::from_name("test-app"),
            connections: RwLock::new(HashMap::new()),
            next_connection_id: std::sync::atomic::AtomicU64::new(1),
            sync_broadcast: sync_tx,
            auth_config,
            external_identity_store: Arc::new(
                ExternalIdentityStore::new(data_dir.path().to_str().unwrap()).unwrap(),
            ),
            external_identities: RwLock::new(HashMap::new()),
        });

        let app = axum::Router::new()
            .route("/schema/:hash", get(schema_handler))
            .route("/schemas", get(schema_hashes_handler))
            .with_state(state);

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
}
