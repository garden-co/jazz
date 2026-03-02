//! Binary HTTP streaming transport protocol types for Jazz.
//!
//! This crate defines the wire format for communication between Jazz clients
//! and servers over HTTP with length-prefixed binary streaming.
//!
//! # Protocol Overview
//!
//! - Clients connect to `/events` for a long-lived binary stream (length-prefixed frames)
//! - All client→server communication flows through a single `/sync` endpoint
//! - Session is bound at connection time via HTTP headers
//!
//! # Wire Format
//!
//! Each frame: `[4 bytes: u32 big-endian length][N bytes: binary ServerEvent payload]`
//!
//! # Endpoints
//!
//! | Route | Method | Description |
//! |-------|--------|-------------|
//! | `/events` | GET | Binary streaming for all subscription updates |
//! | `/sync` | POST | Unified sync endpoint for all SyncPayload variants |

use serde::{Deserialize, Serialize};

use crate::sync_manager::QueryId;

/// Unique identifier for a client's streaming connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ConnectionId(pub u64);

// ============================================================================
// Server -> Client Events
// ============================================================================

/// Event sent over the binary streaming connection.
///
/// Note: Query results are NOT sent here directly. The server syncs the
/// underlying objects, and the client's local QueryManager handles query
/// notifications based on the synced data.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ServerEvent {
    /// Connection established, server sends connection ID and confirms client ID.
    Connected {
        connection_id: ConnectionId,
        /// The client ID the server is using for this connection.
        client_id: String,
        /// Next stream sequence expected from server for this connection.
        next_sync_seq: Option<u64>,
    },

    /// Subscription created successfully.
    Subscribed { query_id: QueryId },

    /// Sync update - object data changed.
    SyncUpdate {
        /// Per-connection stream sequence, if provided by the server.
        seq: Option<u64>,
        /// Postcard-encoded sync payload bytes.
        payload: Vec<u8>,
    },

    /// Error response.
    Error { message: String, code: ErrorCode },

    /// Heartbeat to keep connection alive.
    Heartbeat,
}

impl ServerEvent {
    /// Get the variant name for debugging.
    pub fn variant_name(&self) -> &'static str {
        match self {
            ServerEvent::Connected { .. } => "Connected",
            ServerEvent::Subscribed { .. } => "Subscribed",
            ServerEvent::SyncUpdate { .. } => "SyncUpdate",
            ServerEvent::Error { .. } => "Error",
            ServerEvent::Heartbeat => "Heartbeat",
        }
    }
}

const EVENT_CONNECTED: u8 = 1;
const EVENT_SUBSCRIBED: u8 = 2;
const EVENT_SYNC_UPDATE: u8 = 3;
const EVENT_ERROR: u8 = 4;
const EVENT_HEARTBEAT: u8 = 5;

/// Error codes for server errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ErrorCode {
    /// Invalid request format.
    BadRequest,
    /// Authentication required or failed.
    Unauthorized,
    /// Permission denied by policy.
    Forbidden,
    /// Resource not found.
    NotFound,
    /// Internal server error.
    Internal,
    /// Rate limit exceeded.
    RateLimited,
}

// ============================================================================
// HTTP Response Types
// ============================================================================

/// Generic success response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SuccessResponse {
    pub success: bool,
}

impl Default for SuccessResponse {
    fn default() -> Self {
        Self { success: true }
    }
}

/// Generic error response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorResponse {
    pub error: String,
    pub code: ErrorCode,
}

impl ErrorResponse {
    pub fn bad_request(message: impl Into<String>) -> Self {
        Self {
            error: message.into(),
            code: ErrorCode::BadRequest,
        }
    }

    pub fn unauthorized(message: impl Into<String>) -> Self {
        Self {
            error: message.into(),
            code: ErrorCode::Unauthorized,
        }
    }

    pub fn forbidden(message: impl Into<String>) -> Self {
        Self {
            error: message.into(),
            code: ErrorCode::Forbidden,
        }
    }

    pub fn not_found(message: impl Into<String>) -> Self {
        Self {
            error: message.into(),
            code: ErrorCode::NotFound,
        }
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self {
            error: message.into(),
            code: ErrorCode::Internal,
        }
    }
}

// ============================================================================
// Binary Frame Encoding/Decoding Helpers
// ============================================================================

impl ServerEvent {
    /// Encode as a length-prefixed binary frame.
    ///
    /// Format: `[4 bytes: u32 big-endian length][N bytes: binary event payload]`
    pub fn encode_frame(&self) -> Vec<u8> {
        let payload = self.encode_payload();
        let len = (payload.len() as u32).to_be_bytes();
        let mut buf = Vec::with_capacity(4 + payload.len());
        buf.extend_from_slice(&len);
        buf.extend_from_slice(&payload);
        buf
    }

    /// Decode a single frame from a buffer.
    ///
    /// Returns `Some((event, bytes_consumed))` if a complete frame was available,
    /// or `None` if the buffer doesn't contain a complete frame yet.
    pub fn decode_frame(buf: &[u8]) -> Option<(Self, usize)> {
        if buf.len() < 4 {
            return None;
        }
        let len = u32::from_be_bytes(buf[..4].try_into().unwrap()) as usize;
        if buf.len() < 4 + len {
            return None;
        }
        let event = ServerEvent::decode_payload(&buf[4..4 + len])?;
        Some((event, 4 + len))
    }

    fn encode_payload(&self) -> Vec<u8> {
        let mut out = Vec::new();
        match self {
            ServerEvent::Connected {
                connection_id,
                client_id,
                next_sync_seq,
            } => {
                out.push(EVENT_CONNECTED);
                out.extend_from_slice(&connection_id.0.to_be_bytes());
                match next_sync_seq {
                    Some(seq) => {
                        out.push(1);
                        out.extend_from_slice(&seq.to_be_bytes());
                    }
                    None => out.push(0),
                }
                let client_id_bytes = client_id.as_bytes();
                out.extend_from_slice(&(client_id_bytes.len() as u32).to_be_bytes());
                out.extend_from_slice(client_id_bytes);
            }
            ServerEvent::Subscribed { query_id } => {
                out.push(EVENT_SUBSCRIBED);
                out.extend_from_slice(&query_id.0.to_be_bytes());
            }
            ServerEvent::SyncUpdate { seq, payload } => {
                out.push(EVENT_SYNC_UPDATE);
                match seq {
                    Some(seq) => {
                        out.push(1);
                        out.extend_from_slice(&seq.to_be_bytes());
                    }
                    None => out.push(0),
                }
                out.extend_from_slice(payload);
            }
            ServerEvent::Error { message, code } => {
                out.push(EVENT_ERROR);
                out.push(code.to_u8());
                let message_bytes = message.as_bytes();
                out.extend_from_slice(&(message_bytes.len() as u32).to_be_bytes());
                out.extend_from_slice(message_bytes);
            }
            ServerEvent::Heartbeat => out.push(EVENT_HEARTBEAT),
        }
        out
    }

    fn decode_payload(payload: &[u8]) -> Option<Self> {
        if payload.is_empty() {
            return None;
        }
        let mut idx = 0usize;
        let tag = read_u8(payload, &mut idx)?;
        match tag {
            EVENT_CONNECTED => {
                let connection_id = ConnectionId(read_u64(payload, &mut idx)?);
                let has_seq = read_u8(payload, &mut idx)? != 0;
                let next_sync_seq = if has_seq {
                    Some(read_u64(payload, &mut idx)?)
                } else {
                    None
                };
                let client_id_len = read_u32(payload, &mut idx)? as usize;
                let client_id_bytes = read_exact(payload, &mut idx, client_id_len)?;
                let client_id = String::from_utf8(client_id_bytes.to_vec()).ok()?;
                if idx != payload.len() {
                    return None;
                }
                Some(ServerEvent::Connected {
                    connection_id,
                    client_id,
                    next_sync_seq,
                })
            }
            EVENT_SUBSCRIBED => {
                let query_id = QueryId(read_u64(payload, &mut idx)?);
                if idx != payload.len() {
                    return None;
                }
                Some(ServerEvent::Subscribed { query_id })
            }
            EVENT_SYNC_UPDATE => {
                let has_seq = read_u8(payload, &mut idx)? != 0;
                let seq = if has_seq {
                    Some(read_u64(payload, &mut idx)?)
                } else {
                    None
                };
                let remaining = payload.get(idx..)?.to_vec();
                Some(ServerEvent::SyncUpdate {
                    seq,
                    payload: remaining,
                })
            }
            EVENT_ERROR => {
                let code = ErrorCode::from_u8(read_u8(payload, &mut idx)?)?;
                let message_len = read_u32(payload, &mut idx)? as usize;
                let message_bytes = read_exact(payload, &mut idx, message_len)?;
                let message = String::from_utf8(message_bytes.to_vec()).ok()?;
                if idx != payload.len() {
                    return None;
                }
                Some(ServerEvent::Error { message, code })
            }
            EVENT_HEARTBEAT => {
                if idx != payload.len() {
                    return None;
                }
                Some(ServerEvent::Heartbeat)
            }
            _ => None,
        }
    }
}

impl ErrorCode {
    fn to_u8(self) -> u8 {
        match self {
            ErrorCode::BadRequest => 1,
            ErrorCode::Unauthorized => 2,
            ErrorCode::Forbidden => 3,
            ErrorCode::NotFound => 4,
            ErrorCode::Internal => 5,
            ErrorCode::RateLimited => 6,
        }
    }

    fn from_u8(value: u8) -> Option<Self> {
        match value {
            1 => Some(ErrorCode::BadRequest),
            2 => Some(ErrorCode::Unauthorized),
            3 => Some(ErrorCode::Forbidden),
            4 => Some(ErrorCode::NotFound),
            5 => Some(ErrorCode::Internal),
            6 => Some(ErrorCode::RateLimited),
            _ => None,
        }
    }
}

fn read_u8(buf: &[u8], idx: &mut usize) -> Option<u8> {
    let byte = *buf.get(*idx)?;
    *idx += 1;
    Some(byte)
}

fn read_u32(buf: &[u8], idx: &mut usize) -> Option<u32> {
    let bytes = read_exact(buf, idx, 4)?;
    Some(u32::from_be_bytes(bytes.try_into().ok()?))
}

fn read_u64(buf: &[u8], idx: &mut usize) -> Option<u64> {
    let bytes = read_exact(buf, idx, 8)?;
    Some(u64::from_be_bytes(bytes.try_into().ok()?))
}

fn read_exact<'a>(buf: &'a [u8], idx: &mut usize, len: usize) -> Option<&'a [u8]> {
    let end = idx.checked_add(len)?;
    let bytes = buf.get(*idx..end)?;
    *idx = end;
    Some(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync_manager::ClientId;

    #[test]
    fn test_server_event_frame_roundtrip() {
        let event = ServerEvent::Connected {
            connection_id: ConnectionId(42),
            client_id: "test-client-id".to_string(),
            next_sync_seq: None,
        };

        let frame = event.encode_frame();
        assert!(frame.len() > 4);

        let (decoded, consumed) = ServerEvent::decode_frame(&frame).unwrap();
        assert_eq!(consumed, frame.len());
        assert!(matches!(decoded, ServerEvent::Connected { .. }));
    }

    #[test]
    fn test_heartbeat_frame_roundtrip() {
        let event = ServerEvent::Heartbeat;
        let frame = event.encode_frame();

        let (decoded, consumed) = ServerEvent::decode_frame(&frame).unwrap();
        assert_eq!(consumed, frame.len());
        assert!(matches!(decoded, ServerEvent::Heartbeat));
    }

    #[test]
    fn test_decode_frame_incomplete() {
        // Too short for length prefix
        assert!(ServerEvent::decode_frame(&[0, 0]).is_none());

        // Length says 100 bytes but only 4 available
        let buf = [0, 0, 0, 100, 1, 2, 3, 4];
        assert!(ServerEvent::decode_frame(&buf).is_none());
    }

    #[test]
    fn test_error_response_constructors() {
        let err = ErrorResponse::bad_request("invalid query");
        assert_eq!(err.code, ErrorCode::BadRequest);
        assert_eq!(err.error, "invalid query");

        let err = ErrorResponse::forbidden("not allowed");
        assert_eq!(err.code, ErrorCode::Forbidden);
    }

    #[test]
    fn test_sync_payload_request_serialization() {
        let client_id = ClientId::new();
        assert!(!client_id.to_string().is_empty());
    }
}
