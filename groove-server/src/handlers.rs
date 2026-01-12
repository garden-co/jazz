//! HTTP handlers for sync server endpoints.
//!
//! Endpoints:
//! - POST /sync/subscribe - Subscribe to a query, returns SSE stream
//! - POST /sync/unsubscribe - Stop receiving updates for a query
//! - POST /sync/push - Send new commits for an object
//! - POST /sync/reconcile - Request full reconciliation for an object
//! - GET /api/schema/:table - Get current schema for a table
//! - POST /api/schema/:table/deploy - Deploy a new schema version

use std::sync::Arc;

use axum::{
    Json, Router,
    body::Body,
    extract::{Path, State},
    http::{StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use futures::stream::StreamExt;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use groove::Environment;
use groove::sql::{ColumnDef, ColumnType, LensGenerationOptions, TableSchema};
use groove::sync::{
    ActiveQuery, ClientIdentity, Decode, Encode, PushRequest, PushResponse, QueryId,
    ReconcileRequest, SchemaRegistry, ServerEnv, SseEvent, SubscribeRequest, SyncServer,
    TokenValidator, UnsubscribeRequest,
};

use crate::AxumServerEnv;

// ============================================================================
// App State and Router
// ============================================================================

/// Shared state for the sync server.
pub struct AppState<E: Environment> {
    pub server: RwLock<SyncServer<E>>,
    pub schema_registry: RwLock<SchemaRegistry>,
}

impl<E: Environment> AppState<E> {
    pub fn new(env: Arc<E>, token_validator: Arc<dyn TokenValidator>) -> Self {
        Self {
            server: RwLock::new(SyncServer::new(env, token_validator)),
            schema_registry: RwLock::new(SchemaRegistry::new()),
        }
    }

    /// Create app state with a custom schema registry (e.g., with API key validator).
    pub fn with_schema_registry(
        env: Arc<E>,
        token_validator: Arc<dyn TokenValidator>,
        schema_registry: SchemaRegistry,
    ) -> Self {
        Self {
            server: RwLock::new(SyncServer::new(env, token_validator)),
            schema_registry: RwLock::new(schema_registry),
        }
    }
}

/// Create the axum router for sync endpoints.
pub fn sync_router<E: Environment + 'static>() -> Router<Arc<AppState<E>>> {
    Router::new()
        .route("/", get(handle_health))
        .route("/sync/subscribe", post(handle_subscribe::<E>))
        .route("/sync/unsubscribe", post(handle_unsubscribe::<E>))
        .route("/sync/push", post(handle_push::<E>))
        .route("/sync/reconcile", post(handle_reconcile::<E>))
        .route("/sync/events", get(handle_events::<E>))
        // Schema management endpoints
        .route("/api/schema/:table", get(handle_schema_get::<E>))
        .route("/api/schema/:table/deploy", post(handle_schema_deploy::<E>))
}

/// Health check endpoint for load balancers and orchestration tools.
async fn handle_health() -> &'static str {
    "OK"
}

/// Error response for sync endpoints.
#[derive(Debug)]
pub struct SyncError {
    pub status: StatusCode,
    pub message: String,
}

impl SyncError {
    pub fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: message.into(),
        }
    }

    pub fn unauthorized(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::UNAUTHORIZED,
            message: message.into(),
        }
    }

    #[allow(dead_code)]
    pub fn internal(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: message.into(),
        }
    }
}

impl IntoResponse for SyncError {
    fn into_response(self) -> Response {
        let error_event = SseEvent::Error {
            code: self.status.as_u16(),
            message: self.message,
        };
        let body = error_event.to_bytes();
        Response::builder()
            .status(self.status)
            .header(header::CONTENT_TYPE, "application/octet-stream")
            .body(Body::from(body))
            .unwrap()
    }
}

/// Extract bearer token from Authorization header.
fn extract_bearer_token(headers: &axum::http::HeaderMap) -> Option<&str> {
    headers
        .get(header::AUTHORIZATION)?
        .to_str()
        .ok()?
        .strip_prefix("Bearer ")
}

/// Authenticate request and return client identity.
async fn authenticate<E: Environment>(
    state: &AppState<E>,
    headers: &axum::http::HeaderMap,
) -> Result<ClientIdentity, SyncError> {
    let token = extract_bearer_token(headers)
        .ok_or_else(|| SyncError::unauthorized("Missing or invalid Authorization header"))?;

    let server = state.server.read().await;
    server
        .token_validator
        .validate(token)
        .ok_or_else(|| SyncError::unauthorized("Invalid token"))
}

/// Handle POST /sync/subscribe
///
/// Creates a new query subscription and returns an SSE stream.
async fn handle_subscribe<E: Environment + 'static>(
    State(state): State<Arc<AppState<E>>>,
    headers: axum::http::HeaderMap,
    body: axum::body::Bytes,
) -> Result<Response, SyncError> {
    // Authenticate
    let identity = authenticate(&*state, &headers).await?;

    // Decode request
    let request = SubscribeRequest::from_bytes(&body)
        .map_err(|e| SyncError::bad_request(format!("Invalid request: {}", e)))?;

    // Create SSE channel using ServerEnv
    let (tx, stream) = AxumServerEnv::create_sse_channel();

    // Create session and get query info
    let (session_id, query_id) = {
        let mut server = state.server.write().await;
        let session_id = server.create_session(identity.clone(), tx.clone());

        // Register query subscription
        let session = server.get_session_mut(&session_id).unwrap();
        let query_id = session.next_query_id();
        session.queries.insert(
            query_id,
            ActiveQuery::new(request.query.clone(), request.options.clone()),
        );

        (session_id, query_id)
    };

    // Send initial data for matching objects
    // For MVP: if query is "*", send all objects; otherwise skip initial data
    // (Full implementation would execute SQL query here)
    if request.query == "*" || request.query.to_lowercase().contains("select * from") {
        let state_clone = Arc::clone(&state);
        let tx_clone = tx.clone();

        tokio::spawn(async move {
            // Get all objects from storage
            let object_ids: Vec<u128> = {
                let server = state_clone.server.read().await;
                server.env.list_objects().collect().await
            };

            for oid in object_ids {
                let object_id = groove::ObjectId(oid);

                // Get frontier, commits, and metadata for this object
                let (frontier, commits, object_meta) = {
                    let server = state_clone.server.read().await;
                    let frontier = server.env.get_frontier(oid, "main").await;
                    if frontier.is_empty() {
                        continue;
                    }

                    // Load all commits for this object
                    let commit_ids: Vec<_> = server.env.list_commits(oid, "main").collect().await;

                    let mut commits = Vec::new();
                    for commit_id in commit_ids {
                        if let Some(commit) = server.env.get_commit(&commit_id).await {
                            commits.push(commit);
                        }
                    }

                    // Get cached object metadata
                    let object_meta = server.get_object_meta(&object_id);

                    (frontier, commits, object_meta)
                };

                if !commits.is_empty() {
                    let event = SseEvent::Commits {
                        object_id,
                        commits,
                        frontier: frontier.clone(),
                        object_meta,
                    };

                    // Send to client (ignore errors - client may have disconnected)
                    let _ = tx_clone.send(event).await;
                }

                // Register this object for the session
                {
                    let mut server = state_clone.server.write().await;
                    if let Some(session) = server.get_session_mut(&session_id) {
                        session.add_object_to_query(object_id, query_id);
                        session.client_known_state.insert(object_id, frontier);
                    }
                    server.register_object_session(object_id, session_id);
                }
            }
        });
    }

    // Convert stream to SSE response using ServerEnv
    Ok(AxumServerEnv::sse_response(stream))
}

/// Handle POST /sync/unsubscribe
async fn handle_unsubscribe<E: Environment>(
    State(state): State<Arc<AppState<E>>>,
    headers: axum::http::HeaderMap,
    body: axum::body::Bytes,
) -> Result<Response, SyncError> {
    // Authenticate
    let identity = authenticate(&*state, &headers).await?;

    // Decode request
    let request = UnsubscribeRequest::from_bytes(&body)
        .map_err(|e| SyncError::bad_request(format!("Invalid request: {}", e)))?;

    // Find sessions for this identity and remove the subscription
    let mut server = state.server.write().await;
    let session_ids = server.sessions_for_identity(&identity.id);

    let query_id = QueryId(request.subscription_id);

    // First pass: find the session with this query and collect cleanup info
    let mut cleanup_info: Option<(groove::sync::SessionId, Vec<groove::ObjectId>)> = None;

    for session_id in session_ids {
        if let Some(session) = server.get_session_mut(&session_id) {
            if session.queries.remove(&query_id).is_some() {
                // Found and removed the subscription
                // Collect objects that need cleanup
                let objects_to_check: Vec<_> = session.object_queries.keys().copied().collect();
                let mut objects_to_unregister = Vec::new();

                for object_id in objects_to_check {
                    if session.remove_object_from_query(object_id, query_id) {
                        // Object no longer needed by any query
                        objects_to_unregister.push(object_id);
                    }
                }

                cleanup_info = Some((session_id, objects_to_unregister));
                break;
            }
        }
    }

    // Second pass: unregister objects from server
    if let Some((session_id, objects_to_unregister)) = cleanup_info {
        for object_id in objects_to_unregister {
            server.unregister_object_session(&object_id, &session_id);
        }
    }

    Ok(Response::builder()
        .status(StatusCode::OK)
        .body(Body::empty())
        .unwrap())
}

/// Handle POST /sync/push
async fn handle_push<E: Environment>(
    State(state): State<Arc<AppState<E>>>,
    headers: axum::http::HeaderMap,
    body: axum::body::Bytes,
) -> Result<Response, SyncError> {
    // Authenticate
    let identity = authenticate(&*state, &headers).await?;

    // Decode request
    let request = PushRequest::from_bytes(&body)
        .map_err(|e| SyncError::bad_request(format!("Invalid request: {}", e)))?;

    if request.commits.is_empty() {
        return Ok(Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "application/octet-stream")
            .body(Body::from(
                PushResponse {
                    object_id: request.object_id,
                    accepted: true,
                    frontier: vec![],
                }
                .to_bytes(),
            ))
            .unwrap());
    }

    // Find the sender's session to exclude from broadcast
    let sender_session = {
        let server = state.server.read().await;
        server
            .sessions_for_identity(&identity.id)
            .into_iter()
            .next()
    };

    // Store commits and get new frontier
    let frontier = {
        let server = state.server.read().await;
        server
            .store_commits(request.object_id, &request.commits, "main")
            .await
    };

    // Broadcast to ALL other sessions (MVP: no query filtering)
    {
        let server = state.server.read().await;
        server
            .broadcast_commits_to_all(
                request.object_id,
                request.commits.clone(),
                frontier.clone(),
                request.object_meta.clone(),
                sender_session,
            )
            .await;
    }

    // Update sender's known state and cache object metadata
    {
        let mut server = state.server.write().await;
        if let Some(session_id) = sender_session {
            server.update_client_known_state(&session_id, request.object_id, frontier.clone());
        }
        // Cache object metadata for future subscribers
        if let Some(meta) = request.object_meta {
            server.store_object_meta(request.object_id, meta);
        }
    }

    let response = PushResponse {
        object_id: request.object_id,
        accepted: true,
        frontier,
    };

    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .body(Body::from(response.to_bytes()))
        .unwrap())
}

/// Handle POST /sync/reconcile
async fn handle_reconcile<E: Environment>(
    State(state): State<Arc<AppState<E>>>,
    headers: axum::http::HeaderMap,
    body: axum::body::Bytes,
) -> Result<Response, SyncError> {
    // Authenticate
    let identity = authenticate(&*state, &headers).await?;

    // Decode request
    let request = ReconcileRequest::from_bytes(&body)
        .map_err(|e| SyncError::bad_request(format!("Invalid request: {}", e)))?;

    // Get server frontier and commits
    let server = state.server.read().await;
    let server_frontier = server.env.get_frontier(request.object_id.0, "main").await;

    // If server has no commits, return empty
    if server_frontier.is_empty() {
        let event = SseEvent::Commits {
            object_id: request.object_id,
            commits: vec![],
            frontier: vec![],
            object_meta: None,
        };

        return Ok(Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "application/octet-stream")
            .body(Body::from(event.to_bytes()))
            .unwrap());
    }

    // Build set of commits client claims to have
    let client_known: std::collections::HashSet<_> =
        request.local_frontier.iter().copied().collect();

    // Collect all commits from server for this object
    let commit_ids: Vec<_> = server
        .env
        .list_commits(request.object_id.0, "main")
        .collect()
        .await;

    // Load commits that client doesn't have
    // Simple approach: send all commits not in client's frontier ancestors
    // (In a full implementation, we'd walk the graph to find exactly what's missing)
    let mut commits_to_send = Vec::new();
    for commit_id in &commit_ids {
        if !client_known.contains(commit_id) {
            if let Some(commit) = server.env.get_commit(commit_id).await {
                commits_to_send.push(commit);
            }
        }
    }

    // Get object metadata and update client's known state
    let object_meta = server.get_object_meta(&request.object_id);
    drop(server);

    if let Some(session_id) = {
        let server = state.server.read().await;
        server
            .sessions_for_identity(&identity.id)
            .into_iter()
            .next()
    } {
        let mut server = state.server.write().await;
        server.update_client_known_state(&session_id, request.object_id, server_frontier.clone());
    }

    let event = SseEvent::Commits {
        object_id: request.object_id,
        commits: commits_to_send,
        frontier: server_frontier,
        object_meta,
    };

    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .body(Body::from(event.to_bytes()))
        .unwrap())
}

/// Handle GET /sync/events
///
/// Opens an SSE connection for a client. Uses token query param for auth.
/// This is separate from subscribe because EventSource only supports GET.
async fn handle_events<E: Environment + 'static>(
    State(state): State<Arc<AppState<E>>>,
    axum::extract::Query(params): axum::extract::Query<EventsParams>,
) -> Result<Response, SyncError> {
    // Validate token
    let server = state.server.read().await;
    let identity = server
        .token_validator
        .validate(&params.token)
        .ok_or_else(|| SyncError::unauthorized("Invalid token"))?;
    drop(server);

    // Create SSE channel
    let (tx, stream) = AxumServerEnv::create_sse_channel();

    // Create session
    let session_id = {
        let mut server = state.server.write().await;
        server.create_session(identity, tx.clone())
    };

    web_sys_log(&format!("Created SSE session: {:?}", session_id));

    // Send initial data (all objects) to this session
    // This is MVP behavior - in production we'd send based on query subscriptions
    let state_clone = Arc::clone(&state);
    let tx_clone = tx;

    tokio::spawn(async move {
        // Get all objects from storage
        let object_ids: Vec<u128> = {
            let server = state_clone.server.read().await;
            server.env.list_objects().collect().await
        };

        for oid in object_ids {
            let object_id = groove::ObjectId(oid);

            // Get frontier, commits, and metadata for this object
            let (frontier, commits, object_meta) = {
                let server = state_clone.server.read().await;
                let frontier = server.env.get_frontier(oid, "main").await;
                if frontier.is_empty() {
                    continue;
                }

                // Load all commits for this object
                let commit_ids: Vec<_> = server.env.list_commits(oid, "main").collect().await;

                let mut commits = Vec::new();
                for commit_id in commit_ids {
                    if let Some(commit) = server.env.get_commit(&commit_id).await {
                        commits.push(commit);
                    }
                }

                // Get cached object metadata
                let object_meta = server.get_object_meta(&object_id);

                (frontier, commits, object_meta)
            };

            if !commits.is_empty() {
                let event = SseEvent::Commits {
                    object_id,
                    commits,
                    frontier: frontier.clone(),
                    object_meta,
                };

                // Send to client (ignore errors - client may have disconnected)
                let _ = tx_clone.send(event).await;
            }

            // Register this object for the session
            {
                let mut server = state_clone.server.write().await;
                if let Some(session) = server.get_session_mut(&session_id) {
                    session.add_object_to_query(object_id, QueryId(1));
                    session.client_known_state.insert(object_id, frontier);
                }
                server.register_object_session(object_id, session_id);
            }
        }
    });

    // Return SSE response
    Ok(AxumServerEnv::sse_response(stream))
}

/// Query parameters for /sync/events
#[derive(Debug, serde::Deserialize)]
struct EventsParams {
    token: String,
}

/// Log helper (only prints in debug builds)
#[allow(dead_code)]
fn web_sys_log(_msg: &str) {
    #[cfg(debug_assertions)]
    eprintln!("{}", _msg);
}

// ============================================================================
// Schema Management Types
// ============================================================================

/// Response for GET /api/schema/:table
#[derive(Debug, Serialize)]
pub struct SchemaResponse {
    pub descriptor_id: String,
    pub columns: Vec<ColumnInfo>,
    pub parent_descriptors: Option<Vec<String>>,
}

/// Column information in schema response
#[derive(Debug, Serialize)]
pub struct ColumnInfo {
    pub name: String,
    pub column_type: String,
    pub nullable: bool,
}

/// Request for POST /api/schema/:table/deploy
#[derive(Debug, Deserialize)]
pub struct DeployRequest {
    pub schema: SchemaForDeploy,
    pub environment: String,
    #[serde(default)]
    pub lens: Option<LensForDeploy>,
}

/// Schema definition for deployment
#[derive(Debug, Deserialize)]
pub struct SchemaForDeploy {
    pub columns: Vec<ColumnForDeploy>,
}

/// Column definition for deployment
#[derive(Debug, Deserialize)]
pub struct ColumnForDeploy {
    pub name: String,
    pub column_type: String,
    pub nullable: bool,
}

/// Optional lens override for deployment
#[derive(Debug, Deserialize)]
pub struct LensForDeploy {
    pub forward: Vec<TransformForDeploy>,
    pub backward: Vec<TransformForDeploy>,
}

/// Transform definition for lens
#[derive(Debug, Deserialize)]
pub struct TransformForDeploy {
    pub transform_type: String,
    pub from: Option<String>,
    pub to: Option<String>,
    pub column: Option<String>,
    pub default_value: Option<String>,
}

/// Response for POST /api/schema/:table/deploy
#[derive(Debug, Serialize)]
pub struct DeployResponse {
    pub new_descriptor_id: String,
    pub rows_migrated: u64,
    pub warnings: Vec<WarningInfo>,
}

/// Warning information in deploy response
#[derive(Debug, Serialize)]
pub struct WarningInfo {
    pub kind: String,
    pub message: String,
    pub column: Option<String>,
}

// ============================================================================
// Schema Management Handlers
// ============================================================================

/// Handle GET /api/schema/:table
///
/// Returns the current schema for a table.
async fn handle_schema_get<E: Environment>(
    State(state): State<Arc<AppState<E>>>,
    Path(table): Path<String>,
    headers: axum::http::HeaderMap,
) -> Result<Json<SchemaResponse>, (StatusCode, String)> {
    // Optional authentication (read access may be public)
    let _token = extract_bearer_token(&headers);

    let registry = state.schema_registry.read().await;

    // Get current descriptor ID
    let descriptor_id = registry.get_current_descriptor_id(&table).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            format!("Table '{}' not found", table),
        )
    })?;

    // Get descriptor
    let descriptor = registry.get_descriptor(&descriptor_id).ok_or_else(|| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Descriptor not found".to_string(),
        )
    })?;

    // Build response
    let columns: Vec<ColumnInfo> = descriptor
        .schema
        .columns
        .iter()
        .map(|col| ColumnInfo {
            name: col.name.clone(),
            column_type: format!("{:?}", col.ty),
            nullable: col.nullable,
        })
        .collect();

    let parent_descriptors = if descriptor.parent_descriptors.is_empty() {
        None
    } else {
        Some(
            descriptor
                .parent_descriptors
                .iter()
                .map(|id| id.to_string())
                .collect(),
        )
    };

    Ok(Json(SchemaResponse {
        descriptor_id: descriptor_id.to_string(),
        columns,
        parent_descriptors,
    }))
}

/// Handle POST /api/schema/:table/deploy
///
/// Deploys a new schema version for a table.
async fn handle_schema_deploy<E: Environment>(
    State(state): State<Arc<AppState<E>>>,
    Path(table): Path<String>,
    headers: axum::http::HeaderMap,
    Json(request): Json<DeployRequest>,
) -> Result<Json<DeployResponse>, (StatusCode, String)> {
    // Extract API key from Authorization header
    let api_key = extract_bearer_token(&headers).ok_or_else(|| {
        (
            StatusCode::UNAUTHORIZED,
            "Missing or invalid Authorization header".to_string(),
        )
    })?;

    // Parse schema from request
    let columns: Vec<ColumnDef> = request
        .schema
        .columns
        .iter()
        .map(|col| {
            let ty = parse_column_type(&col.column_type);
            ColumnDef::new(col.name.clone(), ty, col.nullable)
        })
        .collect();

    let new_schema = TableSchema::new(&table, columns);

    // Deploy schema
    let mut registry = state.schema_registry.write().await;
    let result = registry
        .deploy_schema(
            &table,
            new_schema,
            LensGenerationOptions::default(),
            Some(api_key),
            Some(&request.environment),
        )
        .map_err(|e| {
            let status = match &e {
                groove::sync::SchemaRegistryError::TableNotFound(_) => StatusCode::NOT_FOUND,
                groove::sync::SchemaRegistryError::Unauthorized(_) => StatusCode::UNAUTHORIZED,
                groove::sync::SchemaRegistryError::InvalidEnvironment(_) => StatusCode::BAD_REQUEST,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            };
            (status, e.to_string())
        })?;

    // Build response
    let warnings: Vec<WarningInfo> = result
        .warnings
        .iter()
        .map(|msg| WarningInfo {
            kind: "lens_generation".to_string(),
            message: msg.clone(),
            column: None,
        })
        .collect();

    Ok(Json(DeployResponse {
        new_descriptor_id: result.descriptor_id.to_string(),
        rows_migrated: 0, // TODO: Actually migrate rows when Database is integrated
        warnings,
    }))
}

/// Parse column type from string
fn parse_column_type(s: &str) -> ColumnType {
    match s.to_uppercase().as_str() {
        "I64" => ColumnType::I64,
        "F64" => ColumnType::F64,
        "STRING" => ColumnType::String,
        "BOOL" => ColumnType::Bool,
        "BYTES" => ColumnType::Bytes,
        _ => ColumnType::String, // fallback
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use groove::sync::AcceptAllTokens;
    use groove::{Commit, MemoryEnvironment};
    use tokio::sync::mpsc;

    fn make_state() -> Arc<AppState<MemoryEnvironment>> {
        let env = Arc::new(MemoryEnvironment::new());
        let validator = Arc::new(AcceptAllTokens);
        Arc::new(AppState::new(env, validator))
    }

    fn make_headers(token: &str) -> axum::http::HeaderMap {
        let mut headers = axum::http::HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            format!("Bearer {}", token).parse().unwrap(),
        );
        headers
    }

    #[test]
    fn test_extract_bearer_token() {
        let mut headers = axum::http::HeaderMap::new();

        // No header
        assert_eq!(extract_bearer_token(&headers), None);

        // Invalid format
        headers.insert(header::AUTHORIZATION, "Basic abc".parse().unwrap());
        assert_eq!(extract_bearer_token(&headers), None);

        // Valid bearer
        headers.insert(header::AUTHORIZATION, "Bearer mytoken123".parse().unwrap());
        assert_eq!(extract_bearer_token(&headers), Some("mytoken123"));
    }

    #[tokio::test]
    async fn test_push_stores_commits() {
        use groove::CommitStore;

        let state = make_state();
        let headers = make_headers("user1");

        // Create a commit
        let commit = Commit {
            parents: vec![],
            content: b"hello world".to_vec().into_boxed_slice(),
            author: "alice".to_string(),
            timestamp: 1234567890,
            meta: None,
        };
        let commit_id = commit.compute_id();

        // Create push request
        let request = PushRequest {
            object_id: groove::ObjectId(42),
            commits: vec![commit],
            object_meta: None,
        };

        // Call handler
        let response = handle_push(
            State(Arc::clone(&state)),
            headers,
            axum::body::Bytes::from(request.to_bytes()),
        )
        .await
        .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        // Decode response
        let body = axum::body::to_bytes(response.into_body(), 1024)
            .await
            .unwrap();
        let push_response = PushResponse::from_bytes(&body).unwrap();

        assert!(push_response.accepted);
        assert_eq!(push_response.object_id, groove::ObjectId(42));
        assert_eq!(push_response.frontier, vec![commit_id]);

        // Verify commit was stored
        let server = state.server.read().await;
        let stored = server.env.get_commit(&commit_id).await;
        assert!(stored.is_some());
        assert_eq!(stored.unwrap().author, "alice");
    }

    #[tokio::test]
    async fn test_push_broadcasts_to_other_sessions() {
        let state = make_state();

        // Create two sessions
        let (tx1, mut rx1) = mpsc::channel::<SseEvent>(16);
        let (tx2, mut rx2) = mpsc::channel::<SseEvent>(16);

        let object_id = groove::ObjectId(42);

        {
            let mut server = state.server.write().await;

            // Session 1 - the pusher
            let s1 = server.create_session(
                ClientIdentity {
                    id: "user1".to_string(),
                    name: None,
                },
                tx1,
            );

            // Session 2 - should receive the broadcast
            let s2 = server.create_session(
                ClientIdentity {
                    id: "user2".to_string(),
                    name: None,
                },
                tx2,
            );

            // Both sessions track the same object
            server.register_object_session(object_id, s1);
            server.register_object_session(object_id, s2);
        }

        // User1 pushes a commit
        let headers = make_headers("user1");
        let commit = Commit {
            parents: vec![],
            content: b"test data".to_vec().into_boxed_slice(),
            author: "user1".to_string(),
            timestamp: 1000,
            meta: None,
        };

        let request = PushRequest {
            object_id,
            commits: vec![commit.clone()],
            object_meta: None,
        };

        let _ = handle_push(
            State(Arc::clone(&state)),
            headers,
            axum::body::Bytes::from(request.to_bytes()),
        )
        .await
        .unwrap();

        // User1 (pusher) should NOT receive the broadcast
        assert!(rx1.try_recv().is_err());

        // User2 should receive the broadcast
        let event = rx2.try_recv().unwrap();
        match event {
            SseEvent::Commits {
                object_id: oid,
                commits,
                ..
            } => {
                assert_eq!(oid, object_id);
                assert_eq!(commits.len(), 1);
                assert_eq!(commits[0].author, "user1");
            }
            _ => panic!("Expected Commits event"),
        }
    }

    #[tokio::test]
    async fn test_reconcile_sends_missing_commits() {
        let state = make_state();
        let headers = make_headers("user1");
        let object_id = groove::ObjectId(99);

        // First push some commits
        let commit1 = Commit {
            parents: vec![],
            content: b"first".to_vec().into_boxed_slice(),
            author: "alice".to_string(),
            timestamp: 1000,
            meta: None,
        };
        let commit1_id = commit1.compute_id();

        let push_req = PushRequest {
            object_id,
            commits: vec![commit1],
            object_meta: None,
        };

        let _ = handle_push(
            State(Arc::clone(&state)),
            headers.clone(),
            axum::body::Bytes::from(push_req.to_bytes()),
        )
        .await
        .unwrap();

        // Now reconcile with empty frontier (client has nothing)
        let reconcile_req = ReconcileRequest {
            object_id,
            local_frontier: vec![],
        };

        let response = handle_reconcile(
            State(Arc::clone(&state)),
            headers,
            axum::body::Bytes::from(reconcile_req.to_bytes()),
        )
        .await
        .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        // Decode response
        let body = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        let event = SseEvent::from_bytes(&body).unwrap();

        match event {
            SseEvent::Commits {
                object_id: oid,
                commits,
                frontier,
                ..
            } => {
                assert_eq!(oid, object_id);
                assert_eq!(commits.len(), 1);
                assert_eq!(commits[0].author, "alice");
                assert_eq!(frontier, vec![commit1_id]);
            }
            _ => panic!("Expected Commits event"),
        }
    }

    #[tokio::test]
    async fn test_unsubscribe_removes_subscription() {
        let state = make_state();
        let headers = make_headers("user1");

        // First subscribe
        let (tx, _rx) = mpsc::channel::<SseEvent>(16);
        let session_id;
        let query_id;
        {
            let mut server = state.server.write().await;
            session_id = server.create_session(
                ClientIdentity {
                    id: "user1".to_string(),
                    name: None,
                },
                tx,
            );
            let session = server.get_session_mut(&session_id).unwrap();
            query_id = session.next_query_id();
            session.queries.insert(
                query_id,
                ActiveQuery::new("*".to_string(), Default::default()),
            );
        }

        // Verify subscription exists
        {
            let server = state.server.read().await;
            let session = server.get_session(&session_id).unwrap();
            assert!(session.queries.contains_key(&query_id));
        }

        // Unsubscribe
        let unsub_req = UnsubscribeRequest {
            subscription_id: query_id.0,
        };

        let response = handle_unsubscribe(
            State(Arc::clone(&state)),
            headers,
            axum::body::Bytes::from(unsub_req.to_bytes()),
        )
        .await
        .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        // Verify subscription is removed
        {
            let server = state.server.read().await;
            let session = server.get_session(&session_id).unwrap();
            assert!(!session.queries.contains_key(&query_id));
        }
    }
}
