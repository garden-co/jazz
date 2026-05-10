//! Binary WebSocket transport protocol types for Jazz.
//!
//! This module defines the wire format for communication between Jazz clients
//! and servers over a single bidirectional WebSocket with length-prefixed
//! binary frames.
//!
//! # Protocol Overview
//!
//! - Clients connect to `/ws` and authenticate via an initial `AuthHandshake` frame
//! - Both directions use the same length-prefixed binary framing
//! - Server → client frames carry [`ServerEvent`] values
//! - Client → server frames carry [`SyncBatchRequest`] payloads
//!
//! # Wire Format
//!
//! Handshake frames are length-prefixed JSON so pre-versioned peers can report
//! readable protocol errors. After both sides confirm `SYNC_PROTOCOL_VERSION`,
//! sync transport frames are length-prefixed postcard payloads.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

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

    /// Multiple ordered sync updates in one transport frame.
    ///
    /// Each item keeps its own stream sequence so clients can preserve the
    /// exact same ordering/watermark semantics as individual `SyncUpdate`
    /// frames while avoiding thousands of tiny websocket messages.
    SyncUpdateBatch { updates: Vec<SequencedSyncPayload> },

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
            ServerEvent::SyncUpdateBatch { .. } => "SyncUpdateBatch",
            ServerEvent::Error { .. } => "Error",
            ServerEvent::Heartbeat => "Heartbeat",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SequencedSyncPayload {
    pub seq: Option<u64>,
    pub payload: SyncPayload,
}

/// Error codes for server errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    /// Invalid request format.
    BadRequest,
    /// Client and server do not share a compatible sync protocol version.
    IncompatibleProtocol,
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

/// Auth failure reasons returned by runtime-facing HTTP endpoints.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UnauthenticatedCode {
    Expired,
    Missing,
    Invalid,
    Disabled,
}

/// Structured unauthenticated response for the WebSocket transport.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UnauthenticatedResponse {
    pub error: &'static str,
    pub code: UnauthenticatedCode,
    pub message: String,
}

impl UnauthenticatedResponse {
    pub fn expired(message: impl Into<String>) -> Self {
        Self {
            error: "unauthenticated",
            code: UnauthenticatedCode::Expired,
            message: message.into(),
        }
    }

    pub fn missing(message: impl Into<String>) -> Self {
        Self {
            error: "unauthenticated",
            code: UnauthenticatedCode::Missing,
            message: message.into(),
        }
    }

    pub fn invalid(message: impl Into<String>) -> Self {
        Self {
            error: "unauthenticated",
            code: UnauthenticatedCode::Invalid,
            message: message.into(),
        }
    }

    pub fn disabled(message: impl Into<String>) -> Self {
        Self {
            error: "unauthenticated",
            code: UnauthenticatedCode::Disabled,
            message: message.into(),
        }
    }
}

// ============================================================================
// Binary Frame Encoding/Decoding Helpers
// ============================================================================

const SERVER_CONNECTED: u8 = 0;
const SERVER_SUBSCRIBED: u8 = 1;
const SERVER_SYNC_UPDATE: u8 = 2;
const SERVER_SYNC_UPDATE_BATCH: u8 = 3;
const SERVER_ERROR: u8 = 4;
const SERVER_HEARTBEAT: u8 = 5;

const CLIENT_OUTBOX_ENTRY: u8 = 1;
const CLIENT_SYNC_BATCH_REQUEST: u8 = 2;

#[derive(Debug, Clone)]
pub enum DecodeError {
    UnexpectedEof,
    InvalidTag(u8),
    InvalidUtf8,
    InvalidUuid,
    InvalidPayload,
    TrailingBytes,
    LengthOverflow,
}

impl std::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for DecodeError {}

type DecodeResult<T> = Result<T, DecodeError>;

struct WireReader<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> WireReader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }

    fn read_exact(&mut self, len: usize) -> DecodeResult<&'a [u8]> {
        let end = self
            .pos
            .checked_add(len)
            .ok_or(DecodeError::LengthOverflow)?;
        if end > self.bytes.len() {
            return Err(DecodeError::UnexpectedEof);
        }
        let slice = &self.bytes[self.pos..end];
        self.pos = end;
        Ok(slice)
    }

    fn read_u8(&mut self) -> DecodeResult<u8> {
        Ok(self.read_exact(1)?[0])
    }

    fn read_u32(&mut self) -> DecodeResult<u32> {
        Ok(u32::from_be_bytes(
            self.read_exact(4)?.try_into().expect("fixed length"),
        ))
    }

    fn read_u64(&mut self) -> DecodeResult<u64> {
        Ok(u64::from_be_bytes(
            self.read_exact(8)?.try_into().expect("fixed length"),
        ))
    }

    fn read_string(&mut self) -> DecodeResult<String> {
        let bytes = self.read_bytes()?;
        std::str::from_utf8(bytes)
            .map(str::to_owned)
            .map_err(|_| DecodeError::InvalidUtf8)
    }

    fn read_bytes(&mut self) -> DecodeResult<&'a [u8]> {
        let len = self.read_u32()? as usize;
        self.read_exact(len)
    }

    fn read_uuid(&mut self) -> DecodeResult<Uuid> {
        Uuid::from_slice(self.read_exact(16)?).map_err(|_| DecodeError::InvalidUuid)
    }

    fn read_option_u64(&mut self) -> DecodeResult<Option<u64>> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.read_u64()?)),
            tag => Err(DecodeError::InvalidTag(tag)),
        }
    }

    fn read_option_string(&mut self) -> DecodeResult<Option<String>> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.read_string()?)),
            tag => Err(DecodeError::InvalidTag(tag)),
        }
    }

    fn read_sync_payload(&mut self) -> DecodeResult<SyncPayload> {
        SyncPayload::from_bytes(self.read_bytes()?).map_err(|_| DecodeError::InvalidPayload)
    }

    fn finish(self) -> DecodeResult<()> {
        if self.pos == self.bytes.len() {
            Ok(())
        } else {
            Err(DecodeError::TrailingBytes)
        }
    }
}

fn push_u8(out: &mut Vec<u8>, value: u8) {
    out.push(value);
}

fn push_u32(out: &mut Vec<u8>, value: usize) -> Result<(), DecodeError> {
    let value = u32::try_from(value).map_err(|_| DecodeError::LengthOverflow)?;
    out.extend_from_slice(&value.to_be_bytes());
    Ok(())
}

fn push_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_be_bytes());
}

fn push_string(out: &mut Vec<u8>, value: &str) -> Result<(), DecodeError> {
    push_bytes(out, value.as_bytes())
}

fn push_bytes(out: &mut Vec<u8>, bytes: &[u8]) -> Result<(), DecodeError> {
    push_u32(out, bytes.len())?;
    out.extend_from_slice(bytes);
    Ok(())
}

fn push_uuid(out: &mut Vec<u8>, uuid: Uuid) {
    out.extend_from_slice(uuid.as_bytes());
}

fn push_option_u64(out: &mut Vec<u8>, value: Option<u64>) {
    match value {
        Some(value) => {
            push_u8(out, 1);
            push_u64(out, value);
        }
        None => push_u8(out, 0),
    }
}

fn push_option_string(out: &mut Vec<u8>, value: Option<&str>) -> Result<(), DecodeError> {
    match value {
        Some(value) => {
            push_u8(out, 1);
            push_string(out, value)
        }
        None => {
            push_u8(out, 0);
            Ok(())
        }
    }
}

fn push_sync_payload(out: &mut Vec<u8>, payload: &SyncPayload) -> Result<(), DecodeError> {
    let bytes = payload
        .to_bytes()
        .map_err(|_| DecodeError::InvalidPayload)?;
    push_bytes(out, &bytes)
}

fn encode_error_code(code: ErrorCode) -> u8 {
    match code {
        ErrorCode::BadRequest => 0,
        ErrorCode::IncompatibleProtocol => 1,
        ErrorCode::Unauthorized => 2,
        ErrorCode::Forbidden => 3,
        ErrorCode::NotFound => 4,
        ErrorCode::Internal => 5,
        ErrorCode::RateLimited => 6,
    }
}

fn decode_error_code(tag: u8) -> DecodeResult<ErrorCode> {
    match tag {
        0 => Ok(ErrorCode::BadRequest),
        1 => Ok(ErrorCode::IncompatibleProtocol),
        2 => Ok(ErrorCode::Unauthorized),
        3 => Ok(ErrorCode::Forbidden),
        4 => Ok(ErrorCode::NotFound),
        5 => Ok(ErrorCode::Internal),
        6 => Ok(ErrorCode::RateLimited),
        tag => Err(DecodeError::InvalidTag(tag)),
    }
}

impl ServerEvent {
    /// Encode the post-handshake event payload as a compact binary envelope.
    ///
    /// The envelope mirrors the worker bridge: sync rows remain postcard
    /// `SyncPayload` bytes, while transport metadata is a tiny explicit binary
    /// header around those payload bytes.
    pub fn encode_payload(&self) -> DecodeResult<Vec<u8>> {
        let mut out = Vec::new();
        match self {
            ServerEvent::Connected {
                connection_id,
                client_id,
                next_sync_seq,
                catalogue_state_hash,
            } => {
                push_u8(&mut out, SERVER_CONNECTED);
                push_u64(&mut out, connection_id.0);
                push_string(&mut out, client_id)?;
                push_option_u64(&mut out, *next_sync_seq);
                push_option_string(&mut out, catalogue_state_hash.as_deref())?;
            }
            ServerEvent::Subscribed { query_id } => {
                push_u8(&mut out, SERVER_SUBSCRIBED);
                push_u64(&mut out, query_id.0);
            }
            ServerEvent::SyncUpdate { seq, payload } => {
                push_u8(&mut out, SERVER_SYNC_UPDATE);
                push_option_u64(&mut out, *seq);
                push_sync_payload(&mut out, payload)?;
            }
            ServerEvent::SyncUpdateBatch { updates } => {
                push_u8(&mut out, SERVER_SYNC_UPDATE_BATCH);
                push_u32(&mut out, updates.len())?;
                for update in updates {
                    push_option_u64(&mut out, update.seq);
                    push_sync_payload(&mut out, &update.payload)?;
                }
            }
            ServerEvent::Error { message, code } => {
                push_u8(&mut out, SERVER_ERROR);
                push_u8(&mut out, encode_error_code(*code));
                push_string(&mut out, message)?;
            }
            ServerEvent::Heartbeat => {
                push_u8(&mut out, SERVER_HEARTBEAT);
            }
        }
        Ok(out)
    }

    /// Decode a post-handshake event payload from the compact binary envelope.
    pub fn decode_payload(bytes: &[u8]) -> DecodeResult<Self> {
        let mut reader = WireReader::new(bytes);
        let event = match reader.read_u8()? {
            SERVER_CONNECTED => ServerEvent::Connected {
                connection_id: ConnectionId(reader.read_u64()?),
                client_id: reader.read_string()?,
                next_sync_seq: reader.read_option_u64()?,
                catalogue_state_hash: reader.read_option_string()?,
            },
            SERVER_SUBSCRIBED => ServerEvent::Subscribed {
                query_id: QueryId(reader.read_u64()?),
            },
            SERVER_SYNC_UPDATE => ServerEvent::SyncUpdate {
                seq: reader.read_option_u64()?,
                payload: Box::new(reader.read_sync_payload()?),
            },
            SERVER_SYNC_UPDATE_BATCH => {
                let len = reader.read_u32()? as usize;
                let mut updates = Vec::with_capacity(len);
                for _ in 0..len {
                    updates.push(SequencedSyncPayload {
                        seq: reader.read_option_u64()?,
                        payload: reader.read_sync_payload()?,
                    });
                }
                ServerEvent::SyncUpdateBatch { updates }
            }
            SERVER_ERROR => ServerEvent::Error {
                code: decode_error_code(reader.read_u8()?)?,
                message: reader.read_string()?,
            },
            SERVER_HEARTBEAT => ServerEvent::Heartbeat,
            tag => return Err(DecodeError::InvalidTag(tag)),
        };
        reader.finish()?;
        Ok(event)
    }

    /// Test/helper convenience: encode as a length-prefixed post-handshake frame.
    pub fn encode_frame(&self) -> Vec<u8> {
        crate::transport_manager::frame_encode(&self.encode_payload().unwrap_or_default())
    }

    /// Test/helper convenience: decode a length-prefixed post-handshake frame.
    pub fn decode_frame(buf: &[u8]) -> Option<(Self, usize)> {
        let payload = crate::transport_manager::frame_decode(buf)?;
        let event = Self::decode_payload(payload).ok()?;
        Some((event, 4 + payload.len()))
    }
}

impl SyncBatchRequest {
    pub fn encode_payload(&self) -> DecodeResult<Vec<u8>> {
        let mut out = Vec::new();
        push_u8(&mut out, CLIENT_SYNC_BATCH_REQUEST);
        push_uuid(&mut out, self.client_id.0);
        push_u32(&mut out, self.payloads.len())?;
        for payload in &self.payloads {
            push_sync_payload(&mut out, payload)?;
        }
        Ok(out)
    }

    pub fn decode_payload(bytes: &[u8]) -> DecodeResult<Self> {
        let mut reader = WireReader::new(bytes);
        match reader.read_u8()? {
            CLIENT_SYNC_BATCH_REQUEST => {}
            tag => return Err(DecodeError::InvalidTag(tag)),
        }
        let client_id = ClientId(reader.read_uuid()?);
        let len = reader.read_u32()? as usize;
        let mut payloads = Vec::with_capacity(len);
        for _ in 0..len {
            payloads.push(reader.read_sync_payload()?);
        }
        reader.finish()?;
        Ok(Self {
            client_id,
            payloads,
        })
    }
}

impl SyncBatchResponse {
    pub fn encode_payload(&self) -> DecodeResult<Vec<u8>> {
        let mut out = Vec::new();
        push_u32(&mut out, self.results.len())?;
        for result in &self.results {
            push_u8(&mut out, u8::from(result.ok));
            push_option_string(&mut out, result.error.as_deref())?;
        }
        Ok(out)
    }

    pub fn decode_payload(bytes: &[u8]) -> DecodeResult<Self> {
        let mut reader = WireReader::new(bytes);
        let len = reader.read_u32()? as usize;
        let mut results = Vec::with_capacity(len);
        for _ in 0..len {
            results.push(SyncPayloadResult {
                ok: reader.read_u8()? != 0,
                error: reader.read_option_string()?,
            });
        }
        reader.finish()?;
        Ok(Self { results })
    }
}

pub fn encode_outbox_entry_payload(
    entry: &crate::sync_manager::types::OutboxEntry,
) -> DecodeResult<Vec<u8>> {
    let mut out = Vec::new();
    push_u8(&mut out, CLIENT_OUTBOX_ENTRY);
    push_sync_payload(&mut out, &entry.payload)?;
    Ok(out)
}

pub fn decode_outbox_entry_payload(bytes: &[u8]) -> DecodeResult<SyncPayload> {
    let mut reader = WireReader::new(bytes);
    match reader.read_u8()? {
        CLIENT_OUTBOX_ENTRY => {}
        tag => return Err(DecodeError::InvalidTag(tag)),
    }
    let payload = reader.read_sync_payload()?;
    reader.finish()?;
    Ok(payload)
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

        let err = UnauthenticatedResponse::expired("JWT expired");
        assert_eq!(err.error, "unauthenticated");
        assert_eq!(err.code, UnauthenticatedCode::Expired);
        assert_eq!(err.message, "JWT expired");
    }

    #[test]
    fn test_sync_batch_request_postcard_roundtrip() {
        use crate::metadata::RowProvenance;
        use crate::object::ObjectId;
        use crate::row_histories::{RowState, StoredRowBatch};
        use crate::sync_manager::ClientId;

        let row_id = ObjectId::new();
        let payload = SyncPayload::RowBatchCreated {
            metadata: None,
            row: StoredRowBatch::new(
                row_id,
                "main",
                Vec::new(),
                b"alice".to_vec(),
                RowProvenance::for_insert(row_id.to_string(), 1_000),
                Default::default(),
                RowState::VisibleDirect,
                None,
            ),
        };
        let request = SyncBatchRequest {
            payloads: vec![payload],
            client_id: ClientId::new(),
        };

        let bytes = request.encode_payload().unwrap();
        assert!(bytes.len() < 512);

        let parsed = SyncBatchRequest::decode_payload(&bytes).unwrap();
        assert_eq!(parsed.payloads.len(), 1);
        assert!(matches!(
            parsed.payloads[0],
            SyncPayload::RowBatchCreated { .. }
        ));
    }

    #[test]
    fn test_sync_batch_response_postcard_roundtrip() {
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
        let bytes = response.encode_payload().unwrap();
        let decoded = SyncBatchResponse::decode_payload(&bytes).unwrap();
        assert_eq!(decoded.results.len(), 2);
        assert!(decoded.results[0].ok);
        assert_eq!(decoded.results[1].error.as_deref(), Some("bad payload"));
    }
}
