//! Environment traits for sync transport abstraction.
//!
//! These traits abstract the HTTP/SSE transport layer from the sync logic,
//! allowing different platforms to provide their own implementations.

use async_trait::async_trait;

use super::protocol::SseEvent;

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
#[cfg(not(target_arch = "wasm32"))]
#[async_trait]
pub trait ServerEnv: Send + Sync + 'static {
    /// Request type from the transport layer.
    type Request: Send;
    /// Response type for the transport layer.
    type Response: Send;
    /// Inner SSE stream type that yields encoded events.
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
    fn sse_response(stream: Self::SseStream) -> Self::Response;

    // === SSE Stream Management ===

    /// Create an SSE channel. Returns sender and the inner stream.
    fn create_sse_channel() -> (tokio::sync::mpsc::Sender<SseEvent>, Self::SseStream);
}
