//! HTTP handlers for sync server endpoints.
//!
//! Endpoints:
//! - POST /sync/subscribe - Subscribe to a query, returns SSE stream
//! - POST /sync/unsubscribe - Stop receiving updates for a query
//! - POST /sync/push - Send new commits for an object
//! - POST /sync/reconcile - Request full reconciliation for an object

use std::convert::Infallible;
use std::sync::Arc;

use axum::{
    body::Body,
    extract::State,
    http::{header, StatusCode},
    response::{sse::Event, IntoResponse, Response, Sse},
    routing::post,
    Router,
};
// StreamExt is imported locally in handlers that need it
use tokio::sync::{mpsc, RwLock};
use tokio_stream::wrappers::ReceiverStream;

use crate::storage::Environment;

use super::protocol::{
    Decode, Encode, PushRequest, PushResponse, ReconcileRequest, SseEvent, SubscribeRequest,
    UnsubscribeRequest,
};
use super::server::{ClientIdentity, SyncServer, TokenValidator};

/// Shared state for the sync server.
pub struct AppState<E: Environment> {
    pub server: RwLock<SyncServer<E>>,
}

impl<E: Environment> AppState<E> {
    pub fn new(env: Arc<E>, token_validator: Arc<dyn TokenValidator>) -> Self {
        Self {
            server: RwLock::new(SyncServer::new(env, token_validator)),
        }
    }
}

/// Create the axum router for sync endpoints.
pub fn sync_router<E: Environment + 'static>() -> Router<Arc<AppState<E>>> {
    Router::new()
        .route("/sync/subscribe", post(handle_subscribe::<E>))
        .route("/sync/unsubscribe", post(handle_unsubscribe::<E>))
        .route("/sync/push", post(handle_push::<E>))
        .route("/sync/reconcile", post(handle_reconcile::<E>))
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
) -> Result<impl IntoResponse, SyncError> {
    use futures::stream::StreamExt as _;

    // Authenticate
    let identity = authenticate(&*state, &headers).await?;

    // Decode request
    let request = SubscribeRequest::from_bytes(&body)
        .map_err(|e| SyncError::bad_request(format!("Invalid request: {}", e)))?;

    // Create SSE channel
    let (tx, rx) = mpsc::channel::<SseEvent>(32);

    // Create session and get query info
    let (session_id, query_id) = {
        let mut server = state.server.write().await;
        let session_id = server.create_session(identity.clone(), tx.clone());

        // Register query subscription
        let session = server.get_session_mut(&session_id).unwrap();
        let query_id = session.next_query_id();
        session.queries.insert(
            query_id,
            super::server::ActiveQuery::new(request.query.clone(), request.options.clone()),
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
                let object_id = crate::object::ObjectId(oid);

                // Get frontier and commits for this object
                let (frontier, commits) = {
                    let server = state_clone.server.read().await;
                    let frontier = server.env.get_frontier(oid, "main").await;
                    if frontier.is_empty() {
                        continue;
                    }

                    // Load all commits for this object
                    let commit_ids: Vec<_> = server
                        .env
                        .list_commits(oid, "main")
                        .collect()
                        .await;

                    let mut commits = Vec::new();
                    for commit_id in commit_ids {
                        if let Some(commit) = server.env.get_commit(&commit_id).await {
                            commits.push(commit);
                        }
                    }

                    (frontier, commits)
                };

                if !commits.is_empty() {
                    let event = SseEvent::Commits {
                        object_id,
                        commits,
                        frontier: frontier.clone(),
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

    // Create SSE stream
    let stream = ReceiverStream::new(rx);
    let sse_stream = stream.map(move |event| {
        // Encode event as base64 for SSE data field
        let bytes = event.to_bytes();
        let encoded = base64_encode(&bytes);
        Ok::<_, Infallible>(Event::default().data(encoded))
    });

    // Return SSE response with keep-alive
    Ok(Sse::new(sse_stream).keep_alive(
        axum::response::sse::KeepAlive::new()
            .interval(std::time::Duration::from_secs(15))
            .text("ping"),
    ))
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

    let query_id = super::server::QueryId(request.subscription_id);

    // First pass: find the session with this query and collect cleanup info
    let mut cleanup_info: Option<(super::server::SessionId, Vec<crate::object::ObjectId>)> = None;

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

    // Broadcast to other sessions tracking this object
    {
        let server = state.server.read().await;
        server
            .broadcast_commits(
                request.object_id,
                request.commits.clone(),
                frontier.clone(),
                sender_session,
            )
            .await;
    }

    // Update sender's known state
    if let Some(session_id) = sender_session {
        let mut server = state.server.write().await;
        server.update_client_known_state(&session_id, request.object_id, frontier.clone());
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
    use futures::stream::StreamExt as _;

    // Authenticate
    let identity = authenticate(&*state, &headers).await?;

    // Decode request
    let request = ReconcileRequest::from_bytes(&body)
        .map_err(|e| SyncError::bad_request(format!("Invalid request: {}", e)))?;

    // Get server frontier and commits
    let server = state.server.read().await;
    let server_frontier = server
        .env
        .get_frontier(request.object_id.0, "main")
        .await;

    // If server has no commits, return empty
    if server_frontier.is_empty() {
        let event = SseEvent::Commits {
            object_id: request.object_id,
            commits: vec![],
            frontier: vec![],
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

    // Update client's known state
    drop(server);
    if let Some(session_id) = {
        let server = state.server.read().await;
        server
            .sessions_for_identity(&identity.id)
            .into_iter()
            .next()
    } {
        let mut server = state.server.write().await;
        server.update_client_known_state(
            &session_id,
            request.object_id,
            server_frontier.clone(),
        );
    }

    let event = SseEvent::Commits {
        object_id: request.object_id,
        commits: commits_to_send,
        frontier: server_frontier,
    };

    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .body(Body::from(event.to_bytes()))
        .unwrap())
}

/// Simple base64 encoding (for SSE data which must be text).
fn base64_encode(data: &[u8]) -> String {
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

    let mut result = String::new();
    let mut i = 0;

    while i < data.len() {
        let b0 = data[i] as usize;
        let b1 = data.get(i + 1).copied().unwrap_or(0) as usize;
        let b2 = data.get(i + 2).copied().unwrap_or(0) as usize;

        result.push(ALPHABET[b0 >> 2] as char);
        result.push(ALPHABET[((b0 & 0x03) << 4) | (b1 >> 4)] as char);

        if i + 1 < data.len() {
            result.push(ALPHABET[((b1 & 0x0f) << 2) | (b2 >> 6)] as char);
        } else {
            result.push('=');
        }

        if i + 2 < data.len() {
            result.push(ALPHABET[b2 & 0x3f] as char);
        } else {
            result.push('=');
        }

        i += 3;
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commit::Commit;
    use crate::object::ObjectId;
    use crate::storage::{CommitStore, MemoryEnvironment};
    use crate::sync::server::AcceptAllTokens;

    #[test]
    fn test_base64_encode() {
        assert_eq!(base64_encode(b""), "");
        assert_eq!(base64_encode(b"f"), "Zg==");
        assert_eq!(base64_encode(b"fo"), "Zm8=");
        assert_eq!(base64_encode(b"foo"), "Zm9v");
        assert_eq!(base64_encode(b"foob"), "Zm9vYg==");
        assert_eq!(base64_encode(b"fooba"), "Zm9vYmE=");
        assert_eq!(base64_encode(b"foobar"), "Zm9vYmFy");
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
        headers.insert(
            header::AUTHORIZATION,
            "Bearer mytoken123".parse().unwrap(),
        );
        assert_eq!(extract_bearer_token(&headers), Some("mytoken123"));
    }

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

    #[tokio::test]
    async fn test_push_stores_commits() {
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
            object_id: ObjectId(42),
            commits: vec![commit],
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
        assert_eq!(push_response.object_id, ObjectId(42));
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

        let object_id = ObjectId(42);

        {
            let mut server = state.server.write().await;

            // Session 1 - the pusher
            let s1 = server.create_session(
                super::super::server::ClientIdentity {
                    id: "user1".to_string(),
                    name: None,
                },
                tx1,
            );

            // Session 2 - should receive the broadcast
            let s2 = server.create_session(
                super::super::server::ClientIdentity {
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
            SseEvent::Commits { object_id: oid, commits, .. } => {
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
        let object_id = ObjectId(99);

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
            SseEvent::Commits { object_id: oid, commits, frontier } => {
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
                super::super::server::ClientIdentity {
                    id: "user1".to_string(),
                    name: None,
                },
                tx,
            );
            let session = server.get_session_mut(&session_id).unwrap();
            query_id = session.next_query_id();
            session.queries.insert(
                query_id,
                super::super::server::ActiveQuery::new("*".to_string(), Default::default()),
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
