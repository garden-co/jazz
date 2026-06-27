//! Binary WebSocket transport protocol types for Jazz.
//!
//! This module defines the wire format for communication between Jazz clients
//! and servers over a single bidirectional WebSocket with length-prefixed
//! binary frames.
//!
//! # Protocol Overview
//!
//! - Clients connect to `/ws` and authenticate via an initial `AuthHandshake` frame
//! - Both directions send each WebSocket message as a length-prefixed LZ4 frame
//! - Server → client frames carry [`ServerEvent`] values
//! - Client → server frames carry [`SyncBatchRequest`] payloads
//!
//! # Wire Format
//!
//! Each WebSocket message is `[4 bytes: u32 big-endian compressed length][N bytes:
//! LZ4-compressed payload]`. Handshake payloads are JSON before compression.
//! Once both sides confirm `SYNC_PROTOCOL_VERSION`, sync payloads use postcard.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::sync_manager::{ClientId, QueryId, SyncPayload};
use crate::transport_error::ErrorCode;

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
    InvalidPayload,
    TrailingBytes,
    LengthOverflow,
}

impl std::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DecodeError::UnexpectedEof => f.write_str("unexpected end of transport frame"),
            DecodeError::InvalidTag(tag) => write!(f, "invalid transport frame tag {tag}"),
            DecodeError::InvalidUtf8 => f.write_str("invalid transport frame utf-8"),
            DecodeError::InvalidPayload => f.write_str("invalid transport sync payload"),
            DecodeError::TrailingBytes => f.write_str("trailing bytes in transport frame"),
            DecodeError::LengthOverflow => f.write_str("transport frame length overflow"),
        }
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

fn push_bytes(out: &mut Vec<u8>, bytes: &[u8]) -> Result<(), DecodeError> {
    push_u32(out, bytes.len())?;
    out.extend_from_slice(bytes);
    Ok(())
}

fn push_uuid(out: &mut Vec<u8>, uuid: Uuid) {
    out.extend_from_slice(uuid.as_bytes());
}

fn push_sync_payload(out: &mut Vec<u8>, payload: &SyncPayload) -> Result<(), DecodeError> {
    let bytes = payload
        .to_bytes()
        .map_err(|_| DecodeError::InvalidPayload)?;
    push_bytes(out, &bytes)
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
}

pub fn encode_outbox_entry_payload(
    entry: &crate::sync_manager::types::OutboxEntry,
) -> DecodeResult<Vec<u8>> {
    let mut out = Vec::new();
    push_u8(&mut out, CLIENT_OUTBOX_ENTRY);
    push_sync_payload(&mut out, &entry.payload)?;
    Ok(out)
}
