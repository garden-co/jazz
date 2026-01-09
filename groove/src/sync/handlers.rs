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
use futures::stream::StreamExt;
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
    // Authenticate
    let identity = authenticate(&*state, &headers).await?;

    // Decode request
    let request = SubscribeRequest::from_bytes(&body)
        .map_err(|e| SyncError::bad_request(format!("Invalid request: {}", e)))?;

    // Create SSE channel
    let (tx, rx) = mpsc::channel::<SseEvent>(32);

    // Create session
    let _session_id = {
        let mut server = state.server.write().await;
        let session_id = server.create_session(identity.clone(), tx);

        // Register query subscription
        let session = server.get_session_mut(&session_id).unwrap();
        let query_id = session.next_query_id();
        session.queries.insert(
            query_id,
            super::server::ActiveQuery::new(request.query.clone(), request.options.clone()),
        );

        session_id
    };

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
    let _identity = authenticate(&*state, &headers).await?;

    // Decode request
    let _request = UnsubscribeRequest::from_bytes(&body)
        .map_err(|e| SyncError::bad_request(format!("Invalid request: {}", e)))?;

    // TODO: Find session by identity and remove subscription
    // For now, just return success
    // In a real implementation, we'd need to track session IDs per identity

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
    let _identity = authenticate(&*state, &headers).await?;

    // Decode request
    let request = PushRequest::from_bytes(&body)
        .map_err(|e| SyncError::bad_request(format!("Invalid request: {}", e)))?;

    // TODO: Apply commits to storage and update session state
    // For now, just accept all pushes

    let response = PushResponse {
        object_id: request.object_id,
        accepted: true,
        frontier: request.commits.iter().map(|c| c.compute_id()).collect(),
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
    let _identity = authenticate(&*state, &headers).await?;

    // Decode request
    let request = ReconcileRequest::from_bytes(&body)
        .map_err(|e| SyncError::bad_request(format!("Invalid request: {}", e)))?;

    // TODO: Compare frontiers and determine what commits to send
    // For now, just return empty commits event

    let event = SseEvent::Commits {
        object_id: request.object_id,
        commits: vec![],
        frontier: request.local_frontier,
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
}
