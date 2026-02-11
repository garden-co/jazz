//! HTTP routes for the Jazz server.

use std::sync::Arc;
use std::time::Duration;

use axum::{
    Router,
    extract::{Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Json},
    routing::{get, post},
};
use bytes::Bytes;
use jazz_transport::{
    ConnectionId, ErrorResponse, ServerEvent, SuccessResponse, SyncPayloadRequest,
};
use serde::Deserialize;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;

use crate::commands::server::{ConnectionState, ServerState};
use crate::middleware::auth::{extract_session, validate_admin_secret};

/// Create the router with all routes.
pub fn create_router(state: Arc<ServerState>) -> Router {
    Router::new()
        // Binary streaming events endpoint
        .route("/events", get(events_handler))
        // Unified sync endpoint - all client→server communication flows through here
        .route("/sync", post(sync_handler))
        // Health check
        .route("/health", get(health_handler))
        // Add middleware
        .layer(TraceLayer::new_for_http())
        .layer(CorsLayer::permissive())
        .with_state(state)
}

/// Query parameters for events endpoint.
#[derive(Debug, Deserialize)]
struct EventsParams {
    /// Client-provided ID for reconnect support.
    client_id: Option<String>,
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
        Some(s) => groove::sync_manager::ClientId::parse(&s)
            .ok_or((StatusCode::BAD_REQUEST, format!("Invalid client_id: {}", s)))?,
        None => groove::sync_manager::ClientId::new(),
    };

    // Extract session from headers (JWT or backend impersonation)
    let session = match extract_session(&headers, &state.auth_config) {
        Ok(s) => s,
        Err((status, msg)) => {
            return Err((status, msg.to_string()));
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
                "Session required for event stream. Provide a JWT token or backend secret."
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
    let client_id_cleanup = client_id;
    let connection_id_cleanup = connection_id;

    // Capture client_id string for stream
    let client_id_str = client_id.to_string();

    // Create stream that emits length-prefixed binary frames
    let stream = async_stream::stream! {
        // Send Connected frame
        let connected = ServerEvent::Connected {
            connection_id: ConnectionId(connection_id),
            client_id: client_id_str.clone(),
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
                                let event = ServerEvent::SyncUpdate { payload: Box::new(payload) };
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
        let _ = state_cleanup.runtime.remove_client(client_id_cleanup);
        tracing::debug!("Stream connection {} closed, cleaned up", connection_id_cleanup);
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
    use groove::sync_manager::{InboxEntry, Source};

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
        let session = match extract_session(&headers, &state.auth_config) {
            Ok(Some(s)) => s,
            Ok(None) => {
                tracing::error!(
                    "Sync request rejected: no session (client_id={}). Client must send auth headers.",
                    request.client_id
                );
                return (
                    StatusCode::UNAUTHORIZED,
                    Json(ErrorResponse::unauthorized(
                        "Session required for sync. Provide a JWT token or backend secret.",
                    )),
                )
                    .into_response();
            }
            Err((status, msg)) => {
                return (status, Json(ErrorResponse::unauthorized(msg))).into_response();
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

    eprintln!(
        "DEBUG [server /sync]: client_id={}, payload={}",
        request.client_id,
        request.payload.variant_name()
    );

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

/// Health check endpoint.
async fn health_handler() -> impl IntoResponse {
    Json(serde_json::json!({
        "status": "healthy"
    }))
}
