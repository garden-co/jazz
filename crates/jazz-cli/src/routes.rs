//! HTTP routes for the Jazz server.

use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use axum::{
    Router,
    extract::{Query, State},
    http::StatusCode,
    response::{
        IntoResponse, Json,
        sse::{Event, KeepAlive, Sse},
    },
    routing::{get, post},
};
use futures::stream::Stream;
use jazz_transport::{
    ConnectionId, CreateObjectRequest, CreateObjectResponse, DeleteObjectRequest, ErrorResponse,
    ServerEvent, SubscribeRequest, SubscribeResponse, SuccessResponse, SyncPayloadRequest,
    UnsubscribeRequest, UpdateObjectRequest,
};
use serde::Deserialize;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;

use crate::commands::server::{ConnectionState, ServerState};

/// Create the router with all routes.
pub fn create_router(state: Arc<ServerState>) -> Router {
    Router::new()
        // SSE events endpoint
        .route("/events", get(events_handler))
        // Sync endpoints
        .route("/sync", post(sync_handler))
        .route("/sync/subscribe", post(subscribe_handler))
        .route("/sync/unsubscribe", post(unsubscribe_handler))
        .route("/sync/object", post(create_object_handler))
        .route("/sync/object", axum::routing::put(update_object_handler))
        .route("/sync/object/delete", post(delete_object_handler))
        // Health check
        .route("/health", get(health_handler))
        // Add middleware
        .layer(TraceLayer::new_for_http())
        .layer(CorsLayer::permissive())
        .with_state(state)
}

/// Query parameters for SSE events endpoint.
#[derive(Debug, Deserialize)]
struct EventsParams {
    /// Client-provided ID for reconnect support.
    client_id: Option<String>,
}

/// SSE events endpoint - clients connect here for all updates.
async fn events_handler(
    State(state): State<Arc<ServerState>>,
    Query(params): Query<EventsParams>,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, (StatusCode, String)> {
    // Parse client_id from query param - error if malformed, generate if missing
    let client_id = match params.client_id {
        Some(s) => groove::sync_manager::ClientId::parse(&s)
            .ok_or((StatusCode::BAD_REQUEST, format!("Invalid client_id: {}", s)))?,
        None => groove::sync_manager::ClientId::new(),
    };

    // Generate connection ID
    let connection_id = state
        .next_connection_id
        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

    // Register client with runtime and sync all data to them
    let _ = state
        .runtime_handle
        .add_client_with_full_sync(client_id, None)
        .await;

    // Subscribe to broadcast channel for this client's events
    let mut sync_rx = state.sync_broadcast.subscribe();

    // Store connection state
    {
        let mut connections = state.connections.write().await;
        connections.insert(connection_id, ConnectionState { client_id });
    }

    // Clone state for cleanup on drop
    let state_cleanup = state.clone();
    let client_id_cleanup = client_id;
    let connection_id_cleanup = connection_id;

    // Capture client_id string for stream
    let client_id_str = client_id.to_string();

    // Create stream that emits events
    let stream = async_stream::stream! {
        // Send connection ID and client ID first
        let connected = ServerEvent::Connected {
            connection_id: ConnectionId(connection_id),
            client_id: client_id_str.clone(),
        };
        yield Ok(Event::default().data(serde_json::to_string(&connected).unwrap_or_default()));

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
                                let event = ServerEvent::SyncUpdate { payload };
                                yield Ok(Event::default().data(serde_json::to_string(&event).unwrap_or_default()));
                            }
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                            // We fell behind, continue
                            tracing::warn!("SSE client {} lagged behind on sync updates", connection_id);
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
                    yield Ok(Event::default().data(serde_json::to_string(&heartbeat).unwrap_or_default()));
                }
            }
        }

        // Cleanup on stream close
        {
            let mut connections = state_cleanup.connections.write().await;
            connections.remove(&connection_id_cleanup);
        }
        let _ = state_cleanup.runtime_handle.remove_client(client_id_cleanup).await;
        tracing::debug!("SSE connection {} closed, cleaned up", connection_id_cleanup);
    };

    Ok(Sse::new(stream).keep_alive(KeepAlive::default()))
}

/// Push a sync payload to the server's inbox.
async fn sync_handler(
    State(state): State<Arc<ServerState>>,
    Json(request): Json<SyncPayloadRequest>,
) -> impl IntoResponse {
    use groove::sync_manager::{InboxEntry, Source};

    let entry = InboxEntry {
        source: Source::Client(request.client_id),
        payload: request.payload,
    };

    match state.runtime_handle.push_sync_inbox(entry).await {
        Ok(()) => Json(SuccessResponse::default()).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse::internal(e.to_string())),
        )
            .into_response(),
    }
}

/// Subscribe to a query.
async fn subscribe_handler(
    State(state): State<Arc<ServerState>>,
    Json(request): Json<SubscribeRequest>,
) -> impl IntoResponse {
    match state
        .runtime_handle
        .subscribe_with_schema_context(request.query, &request.schema_context, request.session)
        .await
    {
        Ok(query_id) => Json(SubscribeResponse { query_id }).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse::internal(e.to_string())),
        )
            .into_response(),
    }
}

/// Unsubscribe from a query.
async fn unsubscribe_handler(
    State(_state): State<Arc<ServerState>>,
    Json(_request): Json<UnsubscribeRequest>,
) -> impl IntoResponse {
    // TODO: Implement unsubscribe through runtime
    Json(SuccessResponse::default())
}

/// Create a new object.
async fn create_object_handler(
    State(state): State<Arc<ServerState>>,
    Json(request): Json<CreateObjectRequest>,
) -> impl IntoResponse {
    match state
        .runtime_handle
        .insert(&request.table, request.values, request.session)
        .await
    {
        Ok(object_id) => (
            StatusCode::CREATED,
            Json(CreateObjectResponse { object_id }),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse::internal(e.to_string())),
        )
            .into_response(),
    }
}

/// Update an existing object.
async fn update_object_handler(
    State(state): State<Arc<ServerState>>,
    Json(request): Json<UpdateObjectRequest>,
) -> impl IntoResponse {
    match state
        .runtime_handle
        .update(request.object_id, request.updates, request.session)
        .await
    {
        Ok(()) => Json(SuccessResponse::default()).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse::internal(e.to_string())),
        )
            .into_response(),
    }
}

/// Delete an object.
async fn delete_object_handler(
    State(state): State<Arc<ServerState>>,
    Json(request): Json<DeleteObjectRequest>,
) -> impl IntoResponse {
    match state
        .runtime_handle
        .delete(request.object_id, request.session)
        .await
    {
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
