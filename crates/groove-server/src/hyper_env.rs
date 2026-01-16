//! Hyper implementation of ServerEnv.

use std::pin::Pin;

use bytes::Bytes;
use futures::Stream;
use futures::stream::StreamExt;
use http_body_util::{Full, StreamBody};
use hyper::body::Frame;
use hyper::header;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use groove::sync::{Encode, SseEvent};

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

/// Type alias for SSE sender.
pub type SseSender = mpsc::Sender<SseEvent>;

/// Create an SSE channel for streaming events to clients.
pub fn create_sse_channel() -> (SseSender, mpsc::Receiver<SseEvent>) {
    mpsc::channel::<SseEvent>(32)
}

/// Type alias for the SSE stream body (must match handlers::SseStreamBody).
pub type SseStreamBody = StreamBody<
    Pin<Box<dyn Stream<Item = Result<Frame<Bytes>, std::convert::Infallible>> + 'static>>,
>;

/// Build an SSE response from a receiver.
pub fn sse_response(rx: mpsc::Receiver<SseEvent>) -> hyper::Response<SseStreamBody> {
    let stream = ReceiverStream::new(rx).map(|event| {
        let bytes = event.to_bytes();
        let encoded = base64_encode(&bytes);
        let sse_data = format!("data: {}\n\n", encoded);
        Ok::<_, std::convert::Infallible>(Frame::data(Bytes::from(sse_data)))
    });

    // Box the stream for type erasure
    let boxed: Pin<Box<dyn Stream<Item = Result<Frame<Bytes>, std::convert::Infallible>>>> =
        Box::pin(stream);

    hyper::Response::builder()
        .status(200)
        .header(header::CONTENT_TYPE, "text/event-stream")
        .header(header::CACHE_CONTROL, "no-cache")
        .header(header::CONNECTION, "keep-alive")
        .header("Access-Control-Allow-Origin", "*")
        .header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        .header(
            "Access-Control-Allow-Headers",
            "Content-Type, Authorization",
        )
        .body(StreamBody::new(boxed))
        .unwrap()
}

/// Build an OK response with binary body.
pub fn ok_response(body: Vec<u8>) -> hyper::Response<Full<Bytes>> {
    hyper::Response::builder()
        .status(200)
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .header("Access-Control-Allow-Origin", "*")
        .header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        .header(
            "Access-Control-Allow-Headers",
            "Content-Type, Authorization",
        )
        .body(Full::new(Bytes::from(body)))
        .unwrap()
}

/// Build an error response.
pub fn error_response(status: u16, message: &str) -> hyper::Response<Full<Bytes>> {
    let error_event = SseEvent::Error {
        code: status,
        message: message.to_string(),
    };
    let body = error_event.to_bytes();
    hyper::Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .header("Access-Control-Allow-Origin", "*")
        .header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        .header(
            "Access-Control-Allow-Headers",
            "Content-Type, Authorization",
        )
        .body(Full::new(Bytes::from(body)))
        .unwrap()
}

/// Build a CORS preflight response.
pub fn cors_preflight() -> hyper::Response<Full<Bytes>> {
    hyper::Response::builder()
        .status(204)
        .header("Access-Control-Allow-Origin", "*")
        .header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        .header(
            "Access-Control-Allow-Headers",
            "Content-Type, Authorization",
        )
        .header("Access-Control-Max-Age", "86400")
        .body(Full::new(Bytes::new()))
        .unwrap()
}

/// Extract bearer token from Authorization header.
pub fn extract_bearer_token(headers: &hyper::HeaderMap) -> Option<&str> {
    headers
        .get(header::AUTHORIZATION)?
        .to_str()
        .ok()?
        .strip_prefix("Bearer ")
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
