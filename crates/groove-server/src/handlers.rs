//! HTTP handlers for sync server endpoints using hyper.
//!
//! Endpoints:
//! - POST /sync/subscribe - Subscribe to a query, returns SSE stream
//! - POST /sync/unsubscribe - Stop receiving updates for a query
//! - POST /sync/push - Send new commits for an object
//! - POST /sync/reconcile - Request full reconciliation for an object
//! - GET /sync/events - SSE event stream (for EventSource)
//! - GET /api/schema/:table - Get current schema for a table
//! - POST /api/schema/:table/deploy - Deploy a new schema version

use std::cell::RefCell;
use std::collections::HashSet;
use std::rc::Rc;

use bytes::Bytes;
use futures::StreamExt;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::{Method, Request, Response};
use serde::{Deserialize, Serialize};

use groove::Environment;
use groove::sql::{ColumnDef, ColumnType, LensGenerationOptions, TableSchema};
use groove::sync::{
    ActiveQuery, ClientIdentity, Decode, Encode, PushRequest, PushResponse, QueryId,
    ReconcileRequest, SchemaRegistry, SseEvent, SubscribeRequest, SyncServer, TokenValidator,
    UnsubscribeRequest,
};

use crate::hyper_env::{
    SseSender, SseStreamBody as HyperSseStreamBody, cors_preflight, create_sse_channel,
    error_response, extract_bearer_token, ok_response, sse_response,
};

// ============================================================================
// App State
// ============================================================================

/// Shared state for the sync server (single-threaded with Rc<RefCell>).
pub struct AppState<E: Environment> {
    pub server: RefCell<SyncServer<E>>,
    pub schema_registry: RefCell<SchemaRegistry>,
}

impl<E: Environment> AppState<E> {
    pub fn new(env: Rc<E>, token_validator: Rc<dyn TokenValidator>) -> Self {
        Self {
            server: RefCell::new(SyncServer::new(env, token_validator)),
            schema_registry: RefCell::new(SchemaRegistry::new()),
        }
    }

    /// Create app state with a custom schema registry.
    pub fn with_schema_registry(
        env: Rc<E>,
        token_validator: Rc<dyn TokenValidator>,
        schema_registry: SchemaRegistry,
    ) -> Self {
        Self {
            server: RefCell::new(SyncServer::new(env, token_validator)),
            schema_registry: RefCell::new(schema_registry),
        }
    }
}

// ============================================================================
// Router
// ============================================================================

/// Response type that can be either a regular response or an SSE stream.
pub enum SyncResponse {
    Regular(Response<Full<Bytes>>),
    Sse(Response<HyperSseStreamBody>),
}

impl SyncResponse {
    fn regular(resp: Response<Full<Bytes>>) -> Self {
        SyncResponse::Regular(resp)
    }
}

/// Handle an incoming HTTP request.
pub async fn handle_request<E: Environment + 'static>(
    state: Rc<AppState<E>>,
    req: Request<Incoming>,
) -> SyncResponse {
    let method = req.method().clone();
    let path = req.uri().path().to_string();

    // Handle CORS preflight
    if method == Method::OPTIONS {
        return SyncResponse::regular(cors_preflight());
    }

    // Route the request
    match (method, path.as_str()) {
        (Method::GET, "/") => SyncResponse::regular(handle_health()),
        (Method::POST, "/sync/subscribe") => handle_subscribe(state, req).await,
        (Method::POST, "/sync/unsubscribe") => {
            SyncResponse::regular(handle_unsubscribe(state, req).await)
        }
        (Method::POST, "/sync/push") => SyncResponse::regular(handle_push(state, req).await),
        (Method::POST, "/sync/reconcile") => {
            SyncResponse::regular(handle_reconcile(state, req).await)
        }
        (Method::GET, "/sync/events") => handle_events(state, req).await,
        (Method::GET, p) if p.starts_with("/api/schema/") => {
            let table = p.strip_prefix("/api/schema/").unwrap_or("");
            if !table.contains('/') && !table.is_empty() {
                SyncResponse::regular(handle_schema_get(state, table, req).await)
            } else {
                SyncResponse::regular(error_response(404, "Not found"))
            }
        }
        (Method::POST, p) if p.starts_with("/api/schema/") && p.ends_with("/deploy") => {
            let table = p
                .strip_prefix("/api/schema/")
                .and_then(|s| s.strip_suffix("/deploy"))
                .unwrap_or("");
            if !table.is_empty() {
                SyncResponse::regular(handle_schema_deploy(state, table, req).await)
            } else {
                SyncResponse::regular(error_response(404, "Not found"))
            }
        }
        _ => SyncResponse::regular(error_response(404, "Not found")),
    }
}

// ============================================================================
// Sync Handlers
// ============================================================================

fn handle_health() -> Response<Full<Bytes>> {
    Response::builder()
        .status(200)
        .header("Content-Type", "text/plain")
        .header("Access-Control-Allow-Origin", "*")
        .body(Full::new(Bytes::from("OK")))
        .unwrap()
}

/// Authenticate request and return client identity.
fn authenticate<E: Environment>(
    state: &AppState<E>,
    headers: &hyper::HeaderMap,
) -> Result<ClientIdentity, Response<Full<Bytes>>> {
    let token = extract_bearer_token(headers)
        .ok_or_else(|| error_response(401, "Missing or invalid Authorization header"))?;

    let server = state.server.borrow();
    server
        .token_validator
        .validate(token)
        .ok_or_else(|| error_response(401, "Invalid token"))
}

/// Handle POST /sync/subscribe
async fn handle_subscribe<E: Environment + 'static>(
    state: Rc<AppState<E>>,
    req: Request<Incoming>,
) -> SyncResponse {
    let headers = req.headers().clone();

    // Authenticate
    let identity = match authenticate(&state, &headers) {
        Ok(id) => id,
        Err(resp) => return SyncResponse::regular(resp),
    };

    // Read body
    let body = match req.collect().await {
        Ok(b) => b.to_bytes(),
        Err(_) => return SyncResponse::regular(error_response(400, "Failed to read body")),
    };

    // Decode request
    let request = match SubscribeRequest::from_bytes(&body) {
        Ok(r) => r,
        Err(e) => {
            return SyncResponse::regular(error_response(400, &format!("Invalid request: {}", e)));
        }
    };

    // Create SSE channel
    let (tx, rx) = create_sse_channel();

    // Create session and register query
    let (session_id, query_id) = {
        let mut server = state.server.borrow_mut();
        let session_id = server.create_session(identity.clone(), tx.clone());
        let session = server.get_session_mut(&session_id).unwrap();
        let query_id = session.next_query_id();
        session.queries.insert(
            query_id,
            ActiveQuery::new(request.query.clone(), request.options.clone()),
        );
        (session_id, query_id)
    };

    // Send initial data for wildcard queries
    if request.query == "*" || request.query.to_lowercase().contains("select * from") {
        let state_clone = Rc::clone(&state);
        let tx_clone = tx;

        // Use spawn_local since we're single-threaded
        tokio::task::spawn_local(async move {
            send_initial_objects(state_clone, session_id, query_id, tx_clone).await;
        });
    }

    SyncResponse::Sse(sse_response(rx))
}

/// Send initial objects to a newly subscribed client.
async fn send_initial_objects<E: Environment>(
    state: Rc<AppState<E>>,
    session_id: groove::sync::SessionId,
    query_id: QueryId,
    tx: SseSender,
) {
    // Get all object IDs
    let object_ids: Vec<u128> = {
        let server = state.server.borrow();
        server.env.list_objects().collect().await
    };

    for oid in object_ids {
        let object_id = groove::ObjectId(oid);

        // Get frontier, commits, and metadata
        let (frontier, commits, object_meta) = {
            let server = state.server.borrow();
            let frontier = server.env.get_frontier(oid, "main").await;
            if frontier.is_empty() {
                continue;
            }

            let commit_ids: Vec<_> = server.env.list_commits(oid, "main").collect().await;
            let mut commits = Vec::new();
            for commit_id in commit_ids {
                if let Some(commit) = server.env.get_commit(&commit_id).await {
                    commits.push(commit);
                }
            }

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
            let _ = tx.send(event).await;
        }

        // Register object for session
        {
            let mut server = state.server.borrow_mut();
            if let Some(session) = server.get_session_mut(&session_id) {
                session.add_object_to_query(object_id, query_id);
                session.client_known_state.insert(object_id, frontier);
            }
            server.register_object_session(object_id, session_id);
        }
    }
}

/// Handle POST /sync/unsubscribe
async fn handle_unsubscribe<E: Environment>(
    state: Rc<AppState<E>>,
    req: Request<Incoming>,
) -> Response<Full<Bytes>> {
    let headers = req.headers().clone();

    // Authenticate
    let identity = match authenticate(&state, &headers) {
        Ok(id) => id,
        Err(resp) => return resp,
    };

    // Read body
    let body = match req.collect().await {
        Ok(b) => b.to_bytes(),
        Err(_) => return error_response(400, "Failed to read body"),
    };

    // Decode request
    let request = match UnsubscribeRequest::from_bytes(&body) {
        Ok(r) => r,
        Err(e) => return error_response(400, &format!("Invalid request: {}", e)),
    };

    let mut server = state.server.borrow_mut();
    let session_ids = server.sessions_for_identity(&identity.external_id);
    let query_id = QueryId(request.subscription_id);

    // Find session with this query and collect cleanup info
    let mut cleanup_info: Option<(groove::sync::SessionId, Vec<groove::ObjectId>)> = None;

    for session_id in session_ids {
        if let Some(session) = server.get_session_mut(&session_id) {
            if session.queries.remove(&query_id).is_some() {
                let objects_to_check: Vec<_> = session.object_queries.keys().copied().collect();
                let mut objects_to_unregister = Vec::new();

                for object_id in objects_to_check {
                    if session.remove_object_from_query(object_id, query_id) {
                        objects_to_unregister.push(object_id);
                    }
                }

                cleanup_info = Some((session_id, objects_to_unregister));
                break;
            }
        }
    }

    // Unregister objects
    if let Some((session_id, objects_to_unregister)) = cleanup_info {
        for object_id in objects_to_unregister {
            server.unregister_object_session(&object_id, &session_id);
        }
    }

    ok_response(vec![])
}

/// Handle POST /sync/push
async fn handle_push<E: Environment>(
    state: Rc<AppState<E>>,
    req: Request<Incoming>,
) -> Response<Full<Bytes>> {
    let headers = req.headers().clone();

    // Authenticate
    let identity = match authenticate(&state, &headers) {
        Ok(id) => id,
        Err(resp) => return resp,
    };

    // Read body
    let body = match req.collect().await {
        Ok(b) => b.to_bytes(),
        Err(_) => return error_response(400, "Failed to read body"),
    };

    // Decode request
    let request = match PushRequest::from_bytes(&body) {
        Ok(r) => r,
        Err(e) => return error_response(400, &format!("Invalid request: {}", e)),
    };

    if request.commits.is_empty() {
        return ok_response(
            PushResponse {
                object_id: request.object_id,
                accepted: true,
                frontier: vec![],
            }
            .to_bytes(),
        );
    }

    // Find sender's session
    let sender_session = {
        let server = state.server.borrow();
        server
            .sessions_for_identity(&identity.external_id)
            .into_iter()
            .next()
    };

    // Store commits
    let frontier = {
        let server = state.server.borrow();
        server
            .store_commits(request.object_id, &request.commits, "main")
            .await
    };

    // Broadcast to other sessions
    {
        let server = state.server.borrow();
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

    // Update sender's known state
    {
        let mut server = state.server.borrow_mut();
        if let Some(session_id) = sender_session {
            server.update_client_known_state(&session_id, request.object_id, frontier.clone());
        }
        if let Some(meta) = request.object_meta {
            server.store_object_meta(request.object_id, meta);
        }
    }

    ok_response(
        PushResponse {
            object_id: request.object_id,
            accepted: true,
            frontier,
        }
        .to_bytes(),
    )
}

/// Handle POST /sync/reconcile
async fn handle_reconcile<E: Environment>(
    state: Rc<AppState<E>>,
    req: Request<Incoming>,
) -> Response<Full<Bytes>> {
    let headers = req.headers().clone();

    // Authenticate
    let identity = match authenticate(&state, &headers) {
        Ok(id) => id,
        Err(resp) => return resp,
    };

    // Read body
    let body = match req.collect().await {
        Ok(b) => b.to_bytes(),
        Err(_) => return error_response(400, "Failed to read body"),
    };

    // Decode request
    let request = match ReconcileRequest::from_bytes(&body) {
        Ok(r) => r,
        Err(e) => return error_response(400, &format!("Invalid request: {}", e)),
    };

    let server = state.server.borrow();
    let server_frontier = server.env.get_frontier(request.object_id.0, "main").await;

    if server_frontier.is_empty() {
        let event = SseEvent::Commits {
            object_id: request.object_id,
            commits: vec![],
            frontier: vec![],
            object_meta: None,
        };
        return ok_response(event.to_bytes());
    }

    let client_known: HashSet<_> = request.local_frontier.iter().copied().collect();
    let commit_ids: Vec<_> = server
        .env
        .list_commits(request.object_id.0, "main")
        .collect()
        .await;

    let mut commits_to_send = Vec::new();
    for commit_id in &commit_ids {
        if !client_known.contains(commit_id) {
            if let Some(commit) = server.env.get_commit(commit_id).await {
                commits_to_send.push(commit);
            }
        }
    }

    let object_meta = server.get_object_meta(&request.object_id);
    drop(server);

    // Update client's known state
    if let Some(session_id) = {
        let server = state.server.borrow();
        server
            .sessions_for_identity(&identity.external_id)
            .into_iter()
            .next()
    } {
        let mut server = state.server.borrow_mut();
        server.update_client_known_state(&session_id, request.object_id, server_frontier.clone());
    }

    let event = SseEvent::Commits {
        object_id: request.object_id,
        commits: commits_to_send,
        frontier: server_frontier,
        object_meta,
    };

    ok_response(event.to_bytes())
}

/// Handle GET /sync/events (EventSource endpoint)
async fn handle_events<E: Environment + 'static>(
    state: Rc<AppState<E>>,
    req: Request<Incoming>,
) -> SyncResponse {
    // Extract token from query string
    let query = req.uri().query().unwrap_or("");
    let token = query
        .split('&')
        .find_map(|pair| {
            let mut parts = pair.split('=');
            if parts.next() == Some("token") {
                parts.next()
            } else {
                None
            }
        })
        .unwrap_or("");

    // Validate token
    let identity = {
        let server = state.server.borrow();
        match server.token_validator.validate(token) {
            Some(id) => id,
            None => return SyncResponse::regular(error_response(401, "Invalid token")),
        }
    };

    // Create SSE channel
    let (tx, rx) = create_sse_channel();

    // Create session
    let session_id = {
        let mut server = state.server.borrow_mut();
        server.create_session(identity, tx.clone())
    };

    // Send initial objects
    let state_clone = Rc::clone(&state);
    tokio::task::spawn_local(async move {
        send_initial_objects(state_clone, session_id, QueryId(1), tx).await;
    });

    SyncResponse::Sse(sse_response(rx))
}

// ============================================================================
// Schema Management Types
// ============================================================================

#[derive(Debug, Serialize)]
pub struct SchemaResponse {
    pub descriptor_id: String,
    pub columns: Vec<ColumnInfo>,
    pub has_parent: bool,
}

#[derive(Debug, Serialize)]
pub struct ColumnInfo {
    pub name: String,
    pub column_type: String,
    pub nullable: bool,
}

#[derive(Debug, Deserialize)]
pub struct DeployRequest {
    pub schema: SchemaForDeploy,
    pub environment: String,
    #[serde(default)]
    pub lens: Option<LensForDeploy>,
}

#[derive(Debug, Deserialize)]
pub struct SchemaForDeploy {
    pub columns: Vec<ColumnForDeploy>,
}

#[derive(Debug, Deserialize)]
pub struct ColumnForDeploy {
    pub name: String,
    pub column_type: String,
    pub nullable: bool,
}

#[derive(Debug, Deserialize)]
pub struct LensForDeploy {
    pub forward: Vec<TransformForDeploy>,
    pub backward: Vec<TransformForDeploy>,
}

#[derive(Debug, Deserialize)]
pub struct TransformForDeploy {
    pub transform_type: String,
    pub from: Option<String>,
    pub to: Option<String>,
    pub column: Option<String>,
    pub default_value: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct DeployResponse {
    pub new_descriptor_id: String,
    pub rows_migrated: u64,
    pub warnings: Vec<WarningInfo>,
}

#[derive(Debug, Serialize)]
pub struct WarningInfo {
    pub kind: String,
    pub message: String,
    pub column: Option<String>,
}

// ============================================================================
// Schema Management Handlers
// ============================================================================

async fn handle_schema_get<E: Environment>(
    state: Rc<AppState<E>>,
    table: &str,
    _req: Request<Incoming>,
) -> Response<Full<Bytes>> {
    let registry = state.schema_registry.borrow();

    let descriptor_id = match registry.get_current_descriptor_id(table) {
        Some(id) => id,
        None => return error_response(404, &format!("Table '{}' not found", table)),
    };

    let descriptor = match registry.get_descriptor(&descriptor_id) {
        Some(d) => d,
        None => return error_response(500, "Descriptor not found"),
    };

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

    let response = SchemaResponse {
        descriptor_id: descriptor_id.to_string(),
        columns,
        has_parent: descriptor.lens_from_parent.is_some(),
    };

    let body = serde_json::to_vec(&response).unwrap_or_default();
    Response::builder()
        .status(200)
        .header("Content-Type", "application/json")
        .header("Access-Control-Allow-Origin", "*")
        .body(Full::new(Bytes::from(body)))
        .unwrap()
}

async fn handle_schema_deploy<E: Environment>(
    state: Rc<AppState<E>>,
    table: &str,
    req: Request<Incoming>,
) -> Response<Full<Bytes>> {
    let headers = req.headers().clone();

    // Extract API key
    let api_key = match extract_bearer_token(&headers) {
        Some(key) => key,
        None => return error_response(401, "Missing or invalid Authorization header"),
    };

    // Read body
    let body = match req.collect().await {
        Ok(b) => b.to_bytes(),
        Err(_) => return error_response(400, "Failed to read body"),
    };

    // Parse request
    let request: DeployRequest = match serde_json::from_slice(&body) {
        Ok(r) => r,
        Err(e) => return error_response(400, &format!("Invalid JSON: {}", e)),
    };

    // Parse schema
    let columns: Vec<ColumnDef> = request
        .schema
        .columns
        .iter()
        .map(|col| {
            let ty = parse_column_type(&col.column_type);
            ColumnDef::new(col.name.clone(), ty, col.nullable)
        })
        .collect();

    let new_schema = TableSchema::new(table, columns);

    // Deploy
    let mut registry = state.schema_registry.borrow_mut();
    let result = match registry.deploy_schema(
        table,
        new_schema,
        LensGenerationOptions::default(),
        Some(api_key),
        Some(&request.environment),
    ) {
        Ok(r) => r,
        Err(e) => {
            let status = match &e {
                groove::sync::SchemaRegistryError::TableNotFound(_) => 404,
                groove::sync::SchemaRegistryError::Unauthorized(_) => 401,
                groove::sync::SchemaRegistryError::InvalidEnvironment(_) => 400,
                _ => 500,
            };
            return error_response(status, &e.to_string());
        }
    };

    let warnings: Vec<WarningInfo> = result
        .warnings
        .iter()
        .map(|msg| WarningInfo {
            kind: "lens_generation".to_string(),
            message: msg.clone(),
            column: None,
        })
        .collect();

    let response = DeployResponse {
        new_descriptor_id: result.descriptor_id.to_string(),
        rows_migrated: 0,
        warnings,
    };

    let body = serde_json::to_vec(&response).unwrap_or_default();
    Response::builder()
        .status(200)
        .header("Content-Type", "application/json")
        .header("Access-Control-Allow-Origin", "*")
        .body(Full::new(Bytes::from(body)))
        .unwrap()
}

fn parse_column_type(s: &str) -> ColumnType {
    match s.to_uppercase().as_str() {
        "I64" => ColumnType::I64,
        "F64" => ColumnType::F64,
        "STRING" => ColumnType::String,
        "BOOL" => ColumnType::Bool,
        "BYTES" => ColumnType::Bytes,
        _ => ColumnType::String,
    }
}
