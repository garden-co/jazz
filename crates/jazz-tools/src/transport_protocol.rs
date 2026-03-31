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
//! Each frame: `[4 bytes: u32 big-endian length][N bytes: JSON-encoded ServerEvent]`
//!
//! # Endpoints
//!
//! | Route | Method | Description |
//! |-------|--------|-------------|
//! | `/events` | GET | Binary streaming for all subscription updates |
//! | `/sync` | POST | Unified sync endpoint for all SyncPayload variants |

use serde::{Deserialize, Serialize};

use crate::sync_manager::{ClientId, QueryId, SyncPayload};

/// Unique identifier for a client's streaming connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ConnectionId(pub u64);

// ============================================================================
// Client -> Server Requests
// ============================================================================

/// Request to push an ordered batch of sync payloads to the server's inbox.
///
/// All payloads share the same auth context (one auth check per POST).
/// The server applies them sequentially and returns one result per payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncBatchRequest {
    /// Ordered list of payloads from the client's outbox.
    pub payloads: Vec<SyncPayload>,
    /// Client ID for source tracking.
    pub client_id: ClientId,
}

/// Binary sync batch body used on network paths that already carry encoded
/// `SyncPayload` bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BinarySyncBatchRequest {
    pub client_id: ClientId,
    pub payloads: Vec<Vec<u8>>,
}

/// Per-payload result within a `SyncBatchResponse`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncPayloadResult {
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Response to a `SyncBatchRequest` — one result per input payload, in order.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncBatchResponse {
    pub results: Vec<SyncPayloadResult>,
}

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
        /// Canonical digest of the server's catalogue state, when available.
        #[serde(skip_serializing_if = "Option::is_none")]
        catalogue_state_hash: Option<String>,
    },

    /// Subscription created successfully.
    Subscribed { query_id: QueryId },

    /// Sync update - object data changed.
    SyncUpdate {
        /// Per-connection stream sequence, if provided by the server.
        seq: Option<u64>,
        payload: Box<SyncPayload>,
    },

    /// Error response.
    Error { message: String, code: ErrorCode },

    /// Heartbeat to keep connection alive.
    Heartbeat,
}

/// Decoded binary stream event. `SyncUpdate` carries raw encoded sync bytes so
/// each transport can apply its own connection-local `SyncConnectionCodec`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecodedServerEvent {
    Connected {
        connection_id: ConnectionId,
        client_id: String,
        next_sync_seq: Option<u64>,
        catalogue_state_hash: Option<String>,
    },
    Subscribed {
        query_id: QueryId,
    },
    SyncUpdate {
        seq: Option<u64>,
        payload: Vec<u8>,
    },
    Error {
        message: String,
        code: ErrorCode,
    },
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BinaryTransportError {
    Truncated(&'static str),
    InvalidTag(u8),
    InvalidUtf8(&'static str),
    InvalidUuid,
    InvalidLength(&'static str),
}

impl std::fmt::Display for BinaryTransportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Truncated(label) => write!(f, "truncated binary transport field: {label}"),
            Self::InvalidTag(tag) => write!(f, "invalid binary transport tag: {tag}"),
            Self::InvalidUtf8(label) => {
                write!(f, "invalid utf-8 in binary transport field: {label}")
            }
            Self::InvalidUuid => write!(f, "invalid uuid in binary transport field"),
            Self::InvalidLength(label) => write!(f, "invalid binary transport length: {label}"),
        }
    }
}

impl std::error::Error for BinaryTransportError {}

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

const SERVER_EVENT_CONNECTED_TAG: u8 = 1;
const SERVER_EVENT_SUBSCRIBED_TAG: u8 = 2;
const SERVER_EVENT_SYNC_UPDATE_TAG: u8 = 3;
const SERVER_EVENT_ERROR_TAG: u8 = 4;
const SERVER_EVENT_HEARTBEAT_TAG: u8 = 5;

fn push_u32(buf: &mut Vec<u8>, value: u32) {
    buf.extend_from_slice(&value.to_be_bytes());
}

fn push_u64(buf: &mut Vec<u8>, value: u64) {
    buf.extend_from_slice(&value.to_be_bytes());
}

fn push_optional_u64(buf: &mut Vec<u8>, value: Option<u64>) {
    match value {
        Some(value) => {
            buf.push(1);
            push_u64(buf, value);
        }
        None => buf.push(0),
    }
}

fn push_bytes(buf: &mut Vec<u8>, bytes: &[u8]) {
    push_u32(buf, bytes.len() as u32);
    buf.extend_from_slice(bytes);
}

fn push_string(buf: &mut Vec<u8>, value: &str) {
    push_bytes(buf, value.as_bytes());
}

fn push_optional_string(buf: &mut Vec<u8>, value: Option<&str>) {
    match value {
        Some(value) => {
            buf.push(1);
            push_string(buf, value);
        }
        None => buf.push(0),
    }
}

struct BinaryCursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> BinaryCursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn remaining(&self) -> usize {
        self.bytes.len().saturating_sub(self.offset)
    }

    fn read_u8(&mut self, label: &'static str) -> Result<u8, BinaryTransportError> {
        if self.remaining() < 1 {
            return Err(BinaryTransportError::Truncated(label));
        }
        let value = self.bytes[self.offset];
        self.offset += 1;
        Ok(value)
    }

    fn read_u32(&mut self, label: &'static str) -> Result<u32, BinaryTransportError> {
        if self.remaining() < 4 {
            return Err(BinaryTransportError::Truncated(label));
        }
        let value = u32::from_be_bytes(
            self.bytes[self.offset..self.offset + 4]
                .try_into()
                .expect("slice length checked"),
        );
        self.offset += 4;
        Ok(value)
    }

    fn read_u64(&mut self, label: &'static str) -> Result<u64, BinaryTransportError> {
        if self.remaining() < 8 {
            return Err(BinaryTransportError::Truncated(label));
        }
        let value = u64::from_be_bytes(
            self.bytes[self.offset..self.offset + 8]
                .try_into()
                .expect("slice length checked"),
        );
        self.offset += 8;
        Ok(value)
    }

    fn read_bytes(&mut self, label: &'static str) -> Result<Vec<u8>, BinaryTransportError> {
        let len = self.read_u32(label)? as usize;
        if self.remaining() < len {
            return Err(BinaryTransportError::Truncated(label));
        }
        let bytes = self.bytes[self.offset..self.offset + len].to_vec();
        self.offset += len;
        Ok(bytes)
    }

    fn read_string(&mut self, label: &'static str) -> Result<String, BinaryTransportError> {
        let bytes = self.read_bytes(label)?;
        String::from_utf8(bytes).map_err(|_| BinaryTransportError::InvalidUtf8(label))
    }

    fn read_optional_u64(
        &mut self,
        label: &'static str,
    ) -> Result<Option<u64>, BinaryTransportError> {
        match self.read_u8(label)? {
            0 => Ok(None),
            1 => Ok(Some(self.read_u64(label)?)),
            _ => Err(BinaryTransportError::InvalidLength(label)),
        }
    }

    fn read_optional_string(
        &mut self,
        label: &'static str,
    ) -> Result<Option<String>, BinaryTransportError> {
        match self.read_u8(label)? {
            0 => Ok(None),
            1 => Ok(Some(self.read_string(label)?)),
            _ => Err(BinaryTransportError::InvalidLength(label)),
        }
    }
}

impl BinarySyncBatchRequest {
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(
            16 + 4
                + self
                    .payloads
                    .iter()
                    .map(|payload| 4 + payload.len())
                    .sum::<usize>(),
        );
        buf.extend_from_slice(self.client_id.0.as_bytes());
        push_u32(&mut buf, self.payloads.len() as u32);
        for payload in &self.payloads {
            push_bytes(&mut buf, payload);
        }
        buf
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, BinaryTransportError> {
        let mut cursor = BinaryCursor::new(bytes);
        if cursor.remaining() < 16 {
            return Err(BinaryTransportError::Truncated("client_id"));
        }
        let mut client_bytes = [0u8; 16];
        client_bytes.copy_from_slice(&bytes[..16]);
        cursor.offset = 16;
        let payload_count = cursor.read_u32("payload_count")? as usize;
        let mut payloads = Vec::with_capacity(payload_count);
        for _ in 0..payload_count {
            payloads.push(cursor.read_bytes("payload")?);
        }
        if cursor.remaining() != 0 {
            return Err(BinaryTransportError::InvalidLength(
                "sync_batch_request_tail",
            ));
        }
        let client_id = uuid::Uuid::from_slice(&client_bytes)
            .map(ClientId)
            .map_err(|_| BinaryTransportError::InvalidUuid)?;
        Ok(Self {
            client_id,
            payloads,
        })
    }
}

pub fn encode_binary_server_event_frame(
    event: &ServerEvent,
    payload_bytes: Option<&[u8]>,
) -> Vec<u8> {
    let mut body = Vec::new();
    match event {
        ServerEvent::Connected {
            connection_id,
            client_id,
            next_sync_seq,
            catalogue_state_hash,
        } => {
            body.push(SERVER_EVENT_CONNECTED_TAG);
            push_u64(&mut body, connection_id.0);
            push_string(&mut body, client_id);
            push_optional_u64(&mut body, *next_sync_seq);
            push_optional_string(&mut body, catalogue_state_hash.as_deref());
        }
        ServerEvent::Subscribed { query_id } => {
            body.push(SERVER_EVENT_SUBSCRIBED_TAG);
            push_u64(&mut body, query_id.0);
        }
        ServerEvent::SyncUpdate { seq, .. } => {
            body.push(SERVER_EVENT_SYNC_UPDATE_TAG);
            push_optional_u64(&mut body, *seq);
            push_bytes(
                &mut body,
                payload_bytes.expect("sync update frames require encoded payload bytes"),
            );
        }
        ServerEvent::Error { message, code } => {
            body.push(SERVER_EVENT_ERROR_TAG);
            body.push(*code as u8);
            push_string(&mut body, message);
        }
        ServerEvent::Heartbeat => body.push(SERVER_EVENT_HEARTBEAT_TAG),
    }

    let mut frame = Vec::with_capacity(4 + body.len());
    push_u32(&mut frame, body.len() as u32);
    frame.extend_from_slice(&body);
    frame
}

pub fn decode_binary_server_event_frame(
    buf: &[u8],
) -> Result<Option<(DecodedServerEvent, usize)>, BinaryTransportError> {
    if buf.len() < 4 {
        return Ok(None);
    }
    let frame_len = u32::from_be_bytes(buf[..4].try_into().expect("slice length checked")) as usize;
    if buf.len() < 4 + frame_len {
        return Ok(None);
    }
    let body = &buf[4..4 + frame_len];
    let mut cursor = BinaryCursor::new(body);
    let tag = cursor.read_u8("server_event_tag")?;
    let event = match tag {
        SERVER_EVENT_CONNECTED_TAG => DecodedServerEvent::Connected {
            connection_id: ConnectionId(cursor.read_u64("connection_id")?),
            client_id: cursor.read_string("client_id")?,
            next_sync_seq: cursor.read_optional_u64("next_sync_seq")?,
            catalogue_state_hash: cursor.read_optional_string("catalogue_state_hash")?,
        },
        SERVER_EVENT_SUBSCRIBED_TAG => DecodedServerEvent::Subscribed {
            query_id: QueryId(cursor.read_u64("query_id")?),
        },
        SERVER_EVENT_SYNC_UPDATE_TAG => DecodedServerEvent::SyncUpdate {
            seq: cursor.read_optional_u64("seq")?,
            payload: cursor.read_bytes("sync_payload")?,
        },
        SERVER_EVENT_ERROR_TAG => {
            let code = match cursor.read_u8("error_code")? {
                0 => ErrorCode::BadRequest,
                1 => ErrorCode::Unauthorized,
                2 => ErrorCode::Forbidden,
                3 => ErrorCode::NotFound,
                4 => ErrorCode::Internal,
                5 => ErrorCode::RateLimited,
                other => return Err(BinaryTransportError::InvalidTag(other)),
            };
            DecodedServerEvent::Error {
                message: cursor.read_string("error_message")?,
                code,
            }
        }
        SERVER_EVENT_HEARTBEAT_TAG => DecodedServerEvent::Heartbeat,
        other => return Err(BinaryTransportError::InvalidTag(other)),
    };
    if cursor.remaining() != 0 {
        return Err(BinaryTransportError::InvalidLength("server_event_tail"));
    }
    Ok(Some((event, 4 + frame_len)))
}

impl ServerEvent {
    /// Encode as a length-prefixed binary frame.
    ///
    /// Format: `[4 bytes: u32 big-endian length][N bytes: JSON]`
    pub fn encode_frame(&self) -> Vec<u8> {
        let json = serde_json::to_vec(self).unwrap_or_default();
        let len = (json.len() as u32).to_be_bytes();
        let mut buf = Vec::with_capacity(4 + json.len());
        buf.extend_from_slice(&len);
        buf.extend_from_slice(&json);
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
        let event: ServerEvent = serde_json::from_slice(&buf[4..4 + len]).ok()?;
        Some((event, 4 + len))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_server_event_frame_roundtrip() {
        let event = ServerEvent::Connected {
            connection_id: ConnectionId(42),
            client_id: "test-client-id".to_string(),
            next_sync_seq: None,
            catalogue_state_hash: Some("digest-123".to_string()),
        };

        let frame = event.encode_frame();
        assert!(frame.len() > 4);

        let (decoded, consumed) = ServerEvent::decode_frame(&frame).unwrap();
        assert_eq!(consumed, frame.len());
        match decoded {
            ServerEvent::Connected {
                catalogue_state_hash,
                ..
            } => {
                assert_eq!(catalogue_state_hash.as_deref(), Some("digest-123"));
            }
            other => panic!("Expected Connected event, got {:?}", other.variant_name()),
        }
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
    fn test_sync_batch_request_serialization() {
        use crate::object::ObjectId;
        use crate::query_manager::types::{BatchId, BranchPrefixName, SchemaHash};
        use crate::sync_manager::ClientId;

        let branch_name = BranchPrefixName::new("dev", SchemaHash::from_bytes([7; 32]), "main")
            .with_batch_id(BatchId::from_uuid(uuid::Uuid::from_u128(1)))
            .to_branch_name();
        let payload = SyncPayload::ObjectUpdated {
            object_id: ObjectId::new(),
            metadata: None,
            branch_name,
            commits: vec![],
        };
        let request = SyncBatchRequest {
            payloads: vec![payload],
            client_id: ClientId::new(),
        };

        let json = serde_json::to_string(&request).unwrap();
        assert!(json.contains("payloads"));
        assert!(json.contains("ObjectUpdated"));
        assert!(json.contains("main"));

        let parsed: SyncBatchRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.payloads.len(), 1);
        assert!(matches!(
            parsed.payloads[0],
            SyncPayload::ObjectUpdated { .. }
        ));
    }

    #[test]
    fn test_sync_batch_response_serialization() {
        let response = SyncBatchResponse {
            results: vec![
                SyncPayloadResult {
                    ok: true,
                    error: None,
                },
                SyncPayloadResult {
                    ok: false,
                    error: Some("bad payload".into()),
                },
            ],
        };
        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("results"));
        assert!(json.contains("\"ok\":true"));
        assert!(json.contains("bad payload"));

        // ok:true entries must not include the error field
        assert!(!json.contains("\"error\":null"));
    }

    #[test]
    fn test_binary_sync_batch_roundtrip() {
        let request = BinarySyncBatchRequest {
            client_id: ClientId::new(),
            payloads: vec![b"alpha".to_vec(), b"beta".to_vec()],
        };

        let encoded = request.encode();
        let decoded = BinarySyncBatchRequest::decode(&encoded).expect("decode binary sync batch");

        assert_eq!(decoded, request);
    }

    #[test]
    fn test_binary_server_event_sync_update_roundtrip() {
        let payload = vec![1, 2, 3, 4];
        let frame = encode_binary_server_event_frame(
            &ServerEvent::SyncUpdate {
                seq: Some(7),
                payload: Box::new(SyncPayload::QueryUnsubscription {
                    query_id: QueryId(5),
                }),
            },
            Some(&payload),
        );

        let (decoded, consumed) = decode_binary_server_event_frame(&frame)
            .expect("decode binary server frame")
            .expect("frame should be complete");

        assert_eq!(consumed, frame.len());
        assert_eq!(
            decoded,
            DecodedServerEvent::SyncUpdate {
                seq: Some(7),
                payload,
            }
        );
    }
}
