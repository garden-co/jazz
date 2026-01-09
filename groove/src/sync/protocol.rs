//! Sync protocol message types and binary serialization.
//!
//! Wire format uses a custom compact binary encoding for cross-language compatibility:
//! - ObjectId: 16 bytes (raw u128 big-endian)
//! - CommitId: 32 bytes (BLAKE3 hash)
//! - String: u16 length (big-endian) + UTF-8 bytes
//! - Vec<T>: u32 count (big-endian) + T repeated
//! - Commit: see Commit encoding

use std::collections::BTreeMap;
use std::io::{self, Read, Write};

use crate::commit::{Commit, CommitId};
use crate::object::ObjectId;

// ============================================================================
// Request types (Client → Server via HTTP POST)
// ============================================================================

/// Request to subscribe to a query.
/// POST /sync/subscribe - returns SSE stream
#[derive(Debug, Clone)]
pub struct SubscribeRequest {
    /// SQL query to subscribe to
    pub query: String,
    /// Subscription options
    pub options: SubscriptionOptions,
}

/// Options for a query subscription.
#[derive(Debug, Clone, Default)]
pub struct SubscriptionOptions {
    /// Whether to stream blob content (default: false)
    pub stream_blobs: bool,
}

/// Request to unsubscribe from a query.
/// POST /sync/unsubscribe
#[derive(Debug, Clone)]
pub struct UnsubscribeRequest {
    /// Query subscription ID to unsubscribe from
    pub subscription_id: u32,
}

/// Request to push commits to the server.
/// POST /sync/push
#[derive(Debug, Clone)]
pub struct PushRequest {
    /// Object being pushed
    pub object_id: ObjectId,
    /// Commits to push (topologically sorted, parents first)
    pub commits: Vec<Commit>,
}

/// Response to a push request.
#[derive(Debug, Clone)]
pub struct PushResponse {
    /// Object that was pushed
    pub object_id: ObjectId,
    /// Whether the push was accepted
    pub accepted: bool,
    /// Server's frontier after applying (if accepted)
    pub frontier: Vec<CommitId>,
}

/// Request full reconciliation for an object.
/// POST /sync/reconcile
#[derive(Debug, Clone)]
pub struct ReconcileRequest {
    /// Object to reconcile
    pub object_id: ObjectId,
    /// Client's current frontier
    pub local_frontier: Vec<CommitId>,
}

// ============================================================================
// SSE Event types (Server → Client)
// ============================================================================

/// SSE event types sent from server to client.
#[derive(Debug, Clone)]
pub enum SseEvent {
    /// Deliver commits for an object
    Commits {
        object_id: ObjectId,
        commits: Vec<Commit>,
        /// Server's frontier after these commits
        frontier: Vec<CommitId>,
    },
    /// Object no longer matches any active query
    Excluded { object_id: ObjectId },
    /// History truncation point
    Truncate {
        object_id: ObjectId,
        truncate_at: CommitId,
    },
    /// Request commits from client
    Request {
        object_id: ObjectId,
        commit_ids: Vec<CommitId>,
    },
    /// Error notification
    Error { code: u16, message: String },
}

/// SSE event type identifiers for wire format.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SseEventType {
    Commits = 1,
    Excluded = 2,
    Truncate = 3,
    Request = 4,
    Error = 5,
}

impl SseEventType {
    fn from_u8(value: u8) -> Result<Self, DecodeError> {
        match value {
            1 => Ok(SseEventType::Commits),
            2 => Ok(SseEventType::Excluded),
            3 => Ok(SseEventType::Truncate),
            4 => Ok(SseEventType::Request),
            5 => Ok(SseEventType::Error),
            _ => Err(DecodeError::InvalidEventType(value)),
        }
    }
}

// ============================================================================
// Encoding/Decoding traits
// ============================================================================

/// Error during decoding.
#[derive(Debug)]
pub enum DecodeError {
    /// Unexpected end of input
    UnexpectedEof,
    /// Invalid UTF-8 string
    InvalidUtf8,
    /// Invalid event type
    InvalidEventType(u8),
    /// IO error
    Io(io::Error),
}

impl From<io::Error> for DecodeError {
    fn from(e: io::Error) -> Self {
        if e.kind() == io::ErrorKind::UnexpectedEof {
            DecodeError::UnexpectedEof
        } else {
            DecodeError::Io(e)
        }
    }
}

impl std::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DecodeError::UnexpectedEof => write!(f, "unexpected end of input"),
            DecodeError::InvalidUtf8 => write!(f, "invalid UTF-8 string"),
            DecodeError::InvalidEventType(t) => write!(f, "invalid event type: {}", t),
            DecodeError::Io(e) => write!(f, "IO error: {}", e),
        }
    }
}

impl std::error::Error for DecodeError {}

/// Trait for types that can be encoded to binary format.
pub trait Encode {
    fn encode<W: Write>(&self, writer: &mut W) -> io::Result<()>;

    fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        self.encode(&mut buf).expect("encoding to Vec should not fail");
        buf
    }
}

/// Trait for types that can be decoded from binary format.
pub trait Decode: Sized {
    fn decode<R: Read>(reader: &mut R) -> Result<Self, DecodeError>;

    fn from_bytes(bytes: &[u8]) -> Result<Self, DecodeError> {
        Self::decode(&mut &bytes[..])
    }
}

// ============================================================================
// Primitive encodings
// ============================================================================

impl Encode for u8 {
    fn encode<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        writer.write_all(&[*self])
    }
}

impl Decode for u8 {
    fn decode<R: Read>(reader: &mut R) -> Result<Self, DecodeError> {
        let mut buf = [0u8; 1];
        reader.read_exact(&mut buf)?;
        Ok(buf[0])
    }
}

impl Encode for u16 {
    fn encode<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        writer.write_all(&self.to_be_bytes())
    }
}

impl Decode for u16 {
    fn decode<R: Read>(reader: &mut R) -> Result<Self, DecodeError> {
        let mut buf = [0u8; 2];
        reader.read_exact(&mut buf)?;
        Ok(u16::from_be_bytes(buf))
    }
}

impl Encode for u32 {
    fn encode<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        writer.write_all(&self.to_be_bytes())
    }
}

impl Decode for u32 {
    fn decode<R: Read>(reader: &mut R) -> Result<Self, DecodeError> {
        let mut buf = [0u8; 4];
        reader.read_exact(&mut buf)?;
        Ok(u32::from_be_bytes(buf))
    }
}

impl Encode for u64 {
    fn encode<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        writer.write_all(&self.to_be_bytes())
    }
}

impl Decode for u64 {
    fn decode<R: Read>(reader: &mut R) -> Result<Self, DecodeError> {
        let mut buf = [0u8; 8];
        reader.read_exact(&mut buf)?;
        Ok(u64::from_be_bytes(buf))
    }
}

impl Encode for bool {
    fn encode<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        (*self as u8).encode(writer)
    }
}

impl Decode for bool {
    fn decode<R: Read>(reader: &mut R) -> Result<Self, DecodeError> {
        Ok(u8::decode(reader)? != 0)
    }
}

impl Encode for String {
    fn encode<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        let len = self.len() as u16;
        len.encode(writer)?;
        writer.write_all(self.as_bytes())
    }
}

impl Decode for String {
    fn decode<R: Read>(reader: &mut R) -> Result<Self, DecodeError> {
        let len = u16::decode(reader)? as usize;
        let mut buf = vec![0u8; len];
        reader.read_exact(&mut buf)?;
        String::from_utf8(buf).map_err(|_| DecodeError::InvalidUtf8)
    }
}

impl<T: Encode> Encode for Vec<T> {
    fn encode<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        let len = self.len() as u32;
        len.encode(writer)?;
        for item in self {
            item.encode(writer)?;
        }
        Ok(())
    }
}

impl<T: Decode> Decode for Vec<T> {
    fn decode<R: Read>(reader: &mut R) -> Result<Self, DecodeError> {
        let len = u32::decode(reader)? as usize;
        let mut items = Vec::with_capacity(len);
        for _ in 0..len {
            items.push(T::decode(reader)?);
        }
        Ok(items)
    }
}

impl<T: Encode> Encode for Option<T> {
    fn encode<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        match self {
            Some(v) => {
                1u8.encode(writer)?;
                v.encode(writer)
            }
            None => 0u8.encode(writer),
        }
    }
}

impl<T: Decode> Decode for Option<T> {
    fn decode<R: Read>(reader: &mut R) -> Result<Self, DecodeError> {
        let tag = u8::decode(reader)?;
        match tag {
            0 => Ok(None),
            1 => Ok(Some(T::decode(reader)?)),
            _ => Err(DecodeError::InvalidEventType(tag)),
        }
    }
}

// ============================================================================
// Core type encodings
// ============================================================================

impl Encode for ObjectId {
    fn encode<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        // Encode as big-endian u128 (16 bytes)
        writer.write_all(&self.0.to_be_bytes())
    }
}

impl Decode for ObjectId {
    fn decode<R: Read>(reader: &mut R) -> Result<Self, DecodeError> {
        let mut buf = [0u8; 16];
        reader.read_exact(&mut buf)?;
        Ok(ObjectId(u128::from_be_bytes(buf)))
    }
}

impl Encode for CommitId {
    fn encode<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        writer.write_all(self.as_bytes())
    }
}

impl Decode for CommitId {
    fn decode<R: Read>(reader: &mut R) -> Result<Self, DecodeError> {
        let mut buf = [0u8; 32];
        reader.read_exact(&mut buf)?;
        Ok(CommitId::from_bytes(buf))
    }
}

impl Encode for BTreeMap<String, String> {
    fn encode<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        let len = self.len() as u32;
        len.encode(writer)?;
        for (k, v) in self {
            k.encode(writer)?;
            v.encode(writer)?;
        }
        Ok(())
    }
}

impl Decode for BTreeMap<String, String> {
    fn decode<R: Read>(reader: &mut R) -> Result<Self, DecodeError> {
        let len = u32::decode(reader)? as usize;
        let mut map = BTreeMap::new();
        for _ in 0..len {
            let k = String::decode(reader)?;
            let v = String::decode(reader)?;
            map.insert(k, v);
        }
        Ok(map)
    }
}

impl Encode for Commit {
    fn encode<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        // Parents count as u8 (merge commits rarely have >2 parents)
        (self.parents.len() as u8).encode(writer)?;
        for parent in &self.parents {
            parent.encode(writer)?;
        }

        // Content length as u32 + content bytes
        (self.content.len() as u32).encode(writer)?;
        writer.write_all(&self.content)?;

        // Author string
        self.author.encode(writer)?;

        // Timestamp
        self.timestamp.encode(writer)?;

        // Metadata
        self.meta.encode(writer)?;

        Ok(())
    }
}

impl Decode for Commit {
    fn decode<R: Read>(reader: &mut R) -> Result<Self, DecodeError> {
        // Parents
        let parents_len = u8::decode(reader)? as usize;
        let mut parents = Vec::with_capacity(parents_len);
        for _ in 0..parents_len {
            parents.push(CommitId::decode(reader)?);
        }

        // Content
        let content_len = u32::decode(reader)? as usize;
        let mut content = vec![0u8; content_len];
        reader.read_exact(&mut content)?;

        // Author
        let author = String::decode(reader)?;

        // Timestamp
        let timestamp = u64::decode(reader)?;

        // Metadata
        let meta = Option::<BTreeMap<String, String>>::decode(reader)?;

        Ok(Commit {
            parents,
            content: content.into_boxed_slice(),
            author,
            timestamp,
            meta,
        })
    }
}

// ============================================================================
// Request/Response encodings
// ============================================================================

impl Encode for SubscriptionOptions {
    fn encode<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        self.stream_blobs.encode(writer)
    }
}

impl Decode for SubscriptionOptions {
    fn decode<R: Read>(reader: &mut R) -> Result<Self, DecodeError> {
        Ok(SubscriptionOptions {
            stream_blobs: bool::decode(reader)?,
        })
    }
}

impl Encode for SubscribeRequest {
    fn encode<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        self.query.encode(writer)?;
        self.options.encode(writer)
    }
}

impl Decode for SubscribeRequest {
    fn decode<R: Read>(reader: &mut R) -> Result<Self, DecodeError> {
        Ok(SubscribeRequest {
            query: String::decode(reader)?,
            options: SubscriptionOptions::decode(reader)?,
        })
    }
}

impl Encode for UnsubscribeRequest {
    fn encode<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        self.subscription_id.encode(writer)
    }
}

impl Decode for UnsubscribeRequest {
    fn decode<R: Read>(reader: &mut R) -> Result<Self, DecodeError> {
        Ok(UnsubscribeRequest {
            subscription_id: u32::decode(reader)?,
        })
    }
}

impl Encode for PushRequest {
    fn encode<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        self.object_id.encode(writer)?;
        self.commits.encode(writer)
    }
}

impl Decode for PushRequest {
    fn decode<R: Read>(reader: &mut R) -> Result<Self, DecodeError> {
        Ok(PushRequest {
            object_id: ObjectId::decode(reader)?,
            commits: Vec::<Commit>::decode(reader)?,
        })
    }
}

impl Encode for PushResponse {
    fn encode<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        self.object_id.encode(writer)?;
        self.accepted.encode(writer)?;
        self.frontier.encode(writer)
    }
}

impl Decode for PushResponse {
    fn decode<R: Read>(reader: &mut R) -> Result<Self, DecodeError> {
        Ok(PushResponse {
            object_id: ObjectId::decode(reader)?,
            accepted: bool::decode(reader)?,
            frontier: Vec::<CommitId>::decode(reader)?,
        })
    }
}

impl Encode for ReconcileRequest {
    fn encode<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        self.object_id.encode(writer)?;
        self.local_frontier.encode(writer)
    }
}

impl Decode for ReconcileRequest {
    fn decode<R: Read>(reader: &mut R) -> Result<Self, DecodeError> {
        Ok(ReconcileRequest {
            object_id: ObjectId::decode(reader)?,
            local_frontier: Vec::<CommitId>::decode(reader)?,
        })
    }
}

// ============================================================================
// SSE Event encoding
// ============================================================================

impl Encode for SseEvent {
    fn encode<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        match self {
            SseEvent::Commits {
                object_id,
                commits,
                frontier,
            } => {
                (SseEventType::Commits as u8).encode(writer)?;
                object_id.encode(writer)?;
                commits.encode(writer)?;
                frontier.encode(writer)
            }
            SseEvent::Excluded { object_id } => {
                (SseEventType::Excluded as u8).encode(writer)?;
                object_id.encode(writer)
            }
            SseEvent::Truncate {
                object_id,
                truncate_at,
            } => {
                (SseEventType::Truncate as u8).encode(writer)?;
                object_id.encode(writer)?;
                truncate_at.encode(writer)
            }
            SseEvent::Request {
                object_id,
                commit_ids,
            } => {
                (SseEventType::Request as u8).encode(writer)?;
                object_id.encode(writer)?;
                commit_ids.encode(writer)
            }
            SseEvent::Error { code, message } => {
                (SseEventType::Error as u8).encode(writer)?;
                code.encode(writer)?;
                message.encode(writer)
            }
        }
    }
}

impl Decode for SseEvent {
    fn decode<R: Read>(reader: &mut R) -> Result<Self, DecodeError> {
        let event_type = SseEventType::from_u8(u8::decode(reader)?)?;
        match event_type {
            SseEventType::Commits => Ok(SseEvent::Commits {
                object_id: ObjectId::decode(reader)?,
                commits: Vec::<Commit>::decode(reader)?,
                frontier: Vec::<CommitId>::decode(reader)?,
            }),
            SseEventType::Excluded => Ok(SseEvent::Excluded {
                object_id: ObjectId::decode(reader)?,
            }),
            SseEventType::Truncate => Ok(SseEvent::Truncate {
                object_id: ObjectId::decode(reader)?,
                truncate_at: CommitId::decode(reader)?,
            }),
            SseEventType::Request => Ok(SseEvent::Request {
                object_id: ObjectId::decode(reader)?,
                commit_ids: Vec::<CommitId>::decode(reader)?,
            }),
            SseEventType::Error => Ok(SseEvent::Error {
                code: u16::decode(reader)?,
                message: String::decode(reader)?,
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_u8_roundtrip() {
        for val in [0u8, 1, 127, 255] {
            let bytes = val.to_bytes();
            assert_eq!(u8::from_bytes(&bytes).unwrap(), val);
        }
    }

    #[test]
    fn test_u16_roundtrip() {
        for val in [0u16, 1, 256, 65535] {
            let bytes = val.to_bytes();
            assert_eq!(u16::from_bytes(&bytes).unwrap(), val);
        }
    }

    #[test]
    fn test_u32_roundtrip() {
        for val in [0u32, 1, 65536, u32::MAX] {
            let bytes = val.to_bytes();
            assert_eq!(u32::from_bytes(&bytes).unwrap(), val);
        }
    }

    #[test]
    fn test_u64_roundtrip() {
        for val in [0u64, 1, u32::MAX as u64 + 1, u64::MAX] {
            let bytes = val.to_bytes();
            assert_eq!(u64::from_bytes(&bytes).unwrap(), val);
        }
    }

    #[test]
    fn test_string_roundtrip() {
        for val in ["", "hello", "hello world!", "unicode: \u{1F600}"] {
            let bytes = val.to_string().to_bytes();
            assert_eq!(String::from_bytes(&bytes).unwrap(), val);
        }
    }

    #[test]
    fn test_vec_roundtrip() {
        let vals: Vec<u32> = vec![1, 2, 3, 4, 5];
        let bytes = vals.to_bytes();
        assert_eq!(Vec::<u32>::from_bytes(&bytes).unwrap(), vals);
    }

    #[test]
    fn test_object_id_roundtrip() {
        for val in [0u128, 1, u128::MAX, 0x0123456789ABCDEF0123456789ABCDEF] {
            let id = ObjectId(val);
            let bytes = id.to_bytes();
            assert_eq!(ObjectId::from_bytes(&bytes).unwrap(), id);
        }
    }

    #[test]
    fn test_commit_id_roundtrip() {
        let hash = [42u8; 32];
        let id = CommitId::from_bytes(hash);
        let bytes = id.to_bytes();
        let decoded = <CommitId as Decode>::from_bytes(&bytes).unwrap();
        assert_eq!(decoded, id);
    }

    #[test]
    fn test_commit_roundtrip() {
        let parent = CommitId::from_bytes([1u8; 32]);
        let commit = Commit {
            parents: vec![parent],
            content: b"hello world".to_vec().into_boxed_slice(),
            author: "alice".to_string(),
            timestamp: 1234567890,
            meta: None,
        };

        let bytes = commit.to_bytes();
        let decoded = Commit::from_bytes(&bytes).unwrap();

        assert_eq!(decoded.parents, commit.parents);
        assert_eq!(decoded.content, commit.content);
        assert_eq!(decoded.author, commit.author);
        assert_eq!(decoded.timestamp, commit.timestamp);
        assert_eq!(decoded.meta, commit.meta);
    }

    #[test]
    fn test_commit_with_meta_roundtrip() {
        let mut meta = BTreeMap::new();
        meta.insert("key".to_string(), "value".to_string());

        let commit = Commit {
            parents: vec![],
            content: b"data".to_vec().into_boxed_slice(),
            author: "bob".to_string(),
            timestamp: 9999,
            meta: Some(meta.clone()),
        };

        let bytes = commit.to_bytes();
        let decoded = Commit::from_bytes(&bytes).unwrap();

        assert_eq!(decoded.meta, Some(meta));
    }

    #[test]
    fn test_subscribe_request_roundtrip() {
        let req = SubscribeRequest {
            query: "SELECT * FROM users".to_string(),
            options: SubscriptionOptions { stream_blobs: true },
        };

        let bytes = req.to_bytes();
        let decoded = SubscribeRequest::from_bytes(&bytes).unwrap();

        assert_eq!(decoded.query, req.query);
        assert_eq!(decoded.options.stream_blobs, req.options.stream_blobs);
    }

    #[test]
    fn test_push_request_roundtrip() {
        let commit = Commit {
            parents: vec![],
            content: b"content".to_vec().into_boxed_slice(),
            author: "test".to_string(),
            timestamp: 12345,
            meta: None,
        };

        let req = PushRequest {
            object_id: ObjectId(42),
            commits: vec![commit],
        };

        let bytes = req.to_bytes();
        let decoded = PushRequest::from_bytes(&bytes).unwrap();

        assert_eq!(decoded.object_id, req.object_id);
        assert_eq!(decoded.commits.len(), 1);
        assert_eq!(decoded.commits[0].author, "test");
    }

    #[test]
    fn test_sse_event_commits_roundtrip() {
        let commit = Commit {
            parents: vec![],
            content: b"data".to_vec().into_boxed_slice(),
            author: "alice".to_string(),
            timestamp: 1000,
            meta: None,
        };
        let commit_id = commit.compute_id();

        let event = SseEvent::Commits {
            object_id: ObjectId(123),
            commits: vec![commit],
            frontier: vec![commit_id],
        };

        let bytes = event.to_bytes();
        let decoded = SseEvent::from_bytes(&bytes).unwrap();

        match decoded {
            SseEvent::Commits {
                object_id,
                commits,
                frontier,
            } => {
                assert_eq!(object_id, ObjectId(123));
                assert_eq!(commits.len(), 1);
                assert_eq!(frontier.len(), 1);
            }
            _ => panic!("wrong event type"),
        }
    }

    #[test]
    fn test_sse_event_excluded_roundtrip() {
        let event = SseEvent::Excluded {
            object_id: ObjectId(456),
        };

        let bytes = event.to_bytes();
        let decoded = SseEvent::from_bytes(&bytes).unwrap();

        match decoded {
            SseEvent::Excluded { object_id } => {
                assert_eq!(object_id, ObjectId(456));
            }
            _ => panic!("wrong event type"),
        }
    }

    #[test]
    fn test_sse_event_truncate_roundtrip() {
        let commit_id = CommitId::from_bytes([99u8; 32]);
        let event = SseEvent::Truncate {
            object_id: ObjectId(789),
            truncate_at: commit_id,
        };

        let bytes = event.to_bytes();
        let decoded = SseEvent::from_bytes(&bytes).unwrap();

        match decoded {
            SseEvent::Truncate {
                object_id,
                truncate_at,
            } => {
                assert_eq!(object_id, ObjectId(789));
                assert_eq!(truncate_at, commit_id);
            }
            _ => panic!("wrong event type"),
        }
    }

    #[test]
    fn test_sse_event_request_roundtrip() {
        let ids = vec![
            CommitId::from_bytes([1u8; 32]),
            CommitId::from_bytes([2u8; 32]),
        ];
        let event = SseEvent::Request {
            object_id: ObjectId(100),
            commit_ids: ids.clone(),
        };

        let bytes = event.to_bytes();
        let decoded = SseEvent::from_bytes(&bytes).unwrap();

        match decoded {
            SseEvent::Request {
                object_id,
                commit_ids,
            } => {
                assert_eq!(object_id, ObjectId(100));
                assert_eq!(commit_ids, ids);
            }
            _ => panic!("wrong event type"),
        }
    }

    #[test]
    fn test_sse_event_error_roundtrip() {
        let event = SseEvent::Error {
            code: 404,
            message: "not found".to_string(),
        };

        let bytes = event.to_bytes();
        let decoded = SseEvent::from_bytes(&bytes).unwrap();

        match decoded {
            SseEvent::Error { code, message } => {
                assert_eq!(code, 404);
                assert_eq!(message, "not found");
            }
            _ => panic!("wrong event type"),
        }
    }
}
