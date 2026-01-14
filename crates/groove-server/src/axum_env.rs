//! Axum implementation of ServerEnv.

use std::convert::Infallible;

use async_trait::async_trait;
use axum::{
    body::Body,
    http::{StatusCode, header},
    response::{IntoResponse, Response, Sse, sse::Event},
};
use futures::stream::StreamExt;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use groove::sync::{Encode, ServerEnv, SseEncodedEvent, SseEvent};

// ============================================================================
// AxumServerEnv - ServerEnv implementation for axum
// ============================================================================

/// Axum-specific request wrapper for ServerEnv.
pub struct AxumRequest {
    pub headers: axum::http::HeaderMap,
    pub body: axum::body::Bytes,
}

/// The inner SSE stream type for axum - yields encoded events.
pub type AxumSseStream =
    futures::stream::Map<ReceiverStream<SseEvent>, fn(SseEvent) -> SseEncodedEvent>;

/// Implementation of ServerEnv for the axum HTTP framework.
///
/// This implementation uses axum's SSE response type and provides
/// the HTTP transport layer for the sync server.
pub struct AxumServerEnv;

/// Encode an SseEvent to an SseEncodedEvent (base64-encoded binary).
fn encode_sse_event(event: SseEvent) -> SseEncodedEvent {
    let bytes = event.to_bytes();
    SseEncodedEvent {
        data: base64_encode(&bytes),
    }
}

#[async_trait]
impl ServerEnv for AxumServerEnv {
    type Request = AxumRequest;
    type Response = Response;
    type SseStream = AxumSseStream;

    fn extract_auth_token(req: &Self::Request) -> Option<String> {
        req.headers
            .get(header::AUTHORIZATION)?
            .to_str()
            .ok()?
            .strip_prefix("Bearer ")
            .map(|s| s.to_string())
    }

    async fn request_body(req: Self::Request) -> Result<Vec<u8>, String> {
        Ok(req.body.to_vec())
    }

    fn ok_response(body: Vec<u8>) -> Self::Response {
        Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "application/octet-stream")
            .body(Body::from(body))
            .unwrap()
    }

    fn error_response(status: u16, message: &str) -> Self::Response {
        let error_event = SseEvent::Error {
            code: status,
            message: message.to_string(),
        };
        let body = error_event.to_bytes();
        Response::builder()
            .status(StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR))
            .header(header::CONTENT_TYPE, "application/octet-stream")
            .body(Body::from(body))
            .unwrap()
    }

    fn sse_response(stream: Self::SseStream) -> Self::Response {
        // Convert SseEncodedEvent stream to axum's Event stream
        let event_stream =
            stream.map(|encoded| Ok::<_, Infallible>(Event::default().data(encoded.data)));

        // Wrap in Sse with keep-alive
        Sse::new(event_stream)
            .keep_alive(
                axum::response::sse::KeepAlive::new()
                    .interval(std::time::Duration::from_secs(15))
                    .text("ping"),
            )
            .into_response()
    }

    fn create_sse_channel() -> (mpsc::Sender<SseEvent>, Self::SseStream) {
        let (tx, rx) = mpsc::channel::<SseEvent>(32);
        let stream = ReceiverStream::new(rx).map(encode_sse_event as fn(_) -> _);
        (tx, stream)
    }
}

/// Simple base64 encoding (for SSE data which must be text).
pub fn base64_encode(data: &[u8]) -> String {
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
}
