//! Environment traits for sync transport abstraction.
//!
//! These traits abstract the HTTP/SSE transport layer from the sync logic,
//! allowing different platforms to provide their own implementations:
//! - WASM: fetch + EventSource
//! - Native: reqwest + SSE client
//! - Server: axum or other HTTP frameworks

use async_trait::async_trait;
use futures::stream::BoxStream;

use super::protocol::{PushRequest, PushResponse, ReconcileRequest, SseEvent, SubscribeRequest};

// ============================================================================
// Client Environment
// ============================================================================

/// Error type for client environment operations.
#[derive(Debug, Clone)]
pub struct ClientError {
    /// HTTP status code (0 for non-HTTP errors).
    pub code: u16,
    /// Error message.
    pub message: String,
}

impl ClientError {
    /// Create a new client error.
    pub fn new(code: u16, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    /// Create an error for non-HTTP failures.
    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(0, message)
    }
}

impl std::fmt::Display for ClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.code > 0 {
            write!(f, "HTTP {}: {}", self.code, self.message)
        } else {
            write!(f, "{}", self.message)
        }
    }
}

impl std::error::Error for ClientError {}

/// Configuration for ClientEnv implementations.
#[derive(Debug, Clone)]
pub struct ClientEnvConfig {
    /// Base URL of the sync server (e.g., "http://localhost:8080").
    pub base_url: String,
    /// Authentication token.
    pub auth_token: String,
}

impl ClientEnvConfig {
    /// Create a new client environment configuration.
    pub fn new(base_url: impl Into<String>, auth_token: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into(),
            auth_token: auth_token.into(),
        }
    }
}

/// Transport abstraction for sync client.
///
/// Implementations provide HTTP requests and SSE streaming
/// for specific platforms (WASM, native, etc.).
///
/// # Platform Support
///
/// - **WASM**: Use `fetch` for HTTP and `EventSource` for SSE
/// - **Native**: Use `reqwest` for HTTP and an SSE client library
///
/// # Example
///
/// ```ignore
/// struct MyClientEnv { config: ClientEnvConfig }
///
/// #[async_trait(?Send)]
/// impl ClientEnv for MyClientEnv {
///     async fn subscribe(&self, req: SubscribeRequest)
///         -> Result<BoxStream<'static, Result<SseEvent, ClientError>>, ClientError>
///     {
///         // POST to /sync/subscribe, then open SSE stream
///     }
///     // ... other methods
/// }
/// ```
///
/// No Send+Sync bounds - the sync layer is single-threaded on all platforms.
/// SyncedNode uses Rc<RefCell> internally and spawns with spawn_local.
#[async_trait(?Send)]
pub trait ClientEnv: Clone {
    /// Subscribe to a query, returning a stream of SSE events.
    ///
    /// The stream stays open for real-time updates until dropped or disconnected.
    async fn subscribe(
        &self,
        request: SubscribeRequest,
    ) -> Result<BoxStream<'static, Result<SseEvent, ClientError>>, ClientError>;

    /// Push commits to the server.
    async fn push(&self, request: PushRequest) -> Result<PushResponse, ClientError>;

    /// Request reconciliation for an object.
    ///
    /// Returns commits the client is missing.
    async fn reconcile(&self, request: ReconcileRequest) -> Result<SseEvent, ClientError>;

    /// Unsubscribe from a query.
    async fn unsubscribe(&self, subscription_id: u32) -> Result<(), ClientError>;
}

// ============================================================================
// Server Environment
// ============================================================================

/// Marker type for SSE events that have been encoded for transport.
/// This is the item type that SSE streams should yield.
#[cfg(not(target_arch = "wasm32"))]
pub struct SseEncodedEvent {
    /// The encoded event data (typically base64-encoded binary)
    pub data: String,
}

/// Transport abstraction for sync server.
///
/// Implementations provide HTTP request/response handling
/// for specific frameworks (axum, actix, etc.).
///
/// # Associated Types
///
/// - `Request`: The HTTP request type from the framework
/// - `Response`: The HTTP response type for the framework
/// - `SseStream`: The inner stream type that yields encoded SSE events
///
/// # SSE Stream Design
///
/// The `SseStream` associated type represents the *inner* stream before
/// framework-specific wrapping. The `sse_response()` method then wraps
/// this stream with framework-specific SSE handling (e.g., axum's `Sse`
/// wrapper with keep-alive).
///
/// This separation allows the associated type to be expressible without
/// including framework-internal wrapper types.
#[cfg(not(target_arch = "wasm32"))]
#[async_trait]
pub trait ServerEnv: Send + Sync + 'static {
    /// Request type from the transport layer.
    type Request: Send;
    /// Response type for the transport layer.
    type Response: Send;
    /// Inner SSE stream type that yields encoded events.
    /// This is the stream before framework-specific wrapping (Sse, KeepAlive, etc.).
    type SseStream: futures::Stream<Item = SseEncodedEvent> + Send + 'static;

    // === Request Processing ===

    /// Extract bearer token from request headers.
    fn extract_auth_token(req: &Self::Request) -> Option<String>;

    /// Get request body as bytes.
    async fn request_body(req: Self::Request) -> Result<Vec<u8>, String>;

    // === Response Building ===

    /// Build a success response with binary body.
    fn ok_response(body: Vec<u8>) -> Self::Response;

    /// Build an error response.
    fn error_response(status: u16, message: &str) -> Self::Response;

    /// Convert an SSE stream to a framework Response.
    ///
    /// This method handles framework-specific wrapping such as:
    /// - Wrapping in SSE response type
    /// - Adding keep-alive/heartbeat
    /// - Setting appropriate headers
    fn sse_response(stream: Self::SseStream) -> Self::Response;

    // === SSE Stream Management ===

    /// Create an SSE channel. Returns sender and the inner stream.
    ///
    /// The sender is used by sync logic to push `SseEvent`s.
    /// The stream yields encoded events ready for SSE transport.
    /// Use `sse_response()` to convert the stream to a framework Response.
    fn create_sse_channel() -> (tokio::sync::mpsc::Sender<SseEvent>, Self::SseStream);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_error_display() {
        let err = ClientError::new(404, "Not found");
        assert_eq!(format!("{}", err), "HTTP 404: Not found");

        let internal = ClientError::internal("Connection failed");
        assert_eq!(format!("{}", internal), "Connection failed");
    }

    #[test]
    fn test_client_env_config() {
        let config = ClientEnvConfig::new("http://localhost:8080", "my-token");
        assert_eq!(config.base_url, "http://localhost:8080");
        assert_eq!(config.auth_token, "my-token");
    }
}
