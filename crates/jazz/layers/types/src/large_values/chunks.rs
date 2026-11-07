//! Exact, policy-blind access to immutable large-value nodes.
//!
//! A node is named by its object hash, the hash its parent (or descriptor)
//! commits to, and verified against it. A hash grants nothing: disclosure
//! of a descriptor, and of the nodes beneath it, is gated by ordinary row
//! authorization.

use std::future::Future;
use std::pin::Pin;

use bytes::Bytes;
use thiserror::Error;

use super::ContentHash;

/// A node to retrieve, by the object hash the reader verifies.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ChunkRequest {
    /// The object hash of the encoded node.
    pub object_hash: [u8; 32],
}

/// Executor-local future returned by a chunk capability.
pub type ChunkFuture<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;

/// Why a local chunk store refused a request.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum ChunkStorageError {
    /// The node is not stored here.
    #[error("chunk is unavailable")]
    Unavailable,
    /// The stored bytes do not match the requested object hash.
    #[error("staged chunk failed integrity validation")]
    Integrity,
    /// The backend failed.
    #[error("chunk storage failed: {0}")]
    Backend(String),
}

/// Why a chunk could not be retrieved, locally or from a peer.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum ChunkError {
    /// No reachable store has the node.
    #[error("chunk is unavailable")]
    Unavailable,
    /// The peer asked the reader to retry later.
    #[error("chunk retrieval is retryable after {retry_after_ms}ms")]
    Retryable {
        /// How long to wait before retrying.
        retry_after_ms: u32,
    },
    /// The backend or transport failed.
    #[error("chunk retrieval failed: {0}")]
    Backend(String),
    /// The bytes do not match the requested object hash.
    #[error("chunk bytes do not match the requested object hash")]
    Integrity,
}

impl From<ChunkStorageError> for ChunkError {
    fn from(error: ChunkStorageError) -> Self {
        match error {
            ChunkStorageError::Unavailable => Self::Unavailable,
            ChunkStorageError::Integrity => Self::Integrity,
            error => Self::Backend(error.to_string()),
        }
    }
}

/// Exact retrieval of one node, wherever it lives.
pub trait ChunkProvider {
    /// The encoded node `request` names.
    fn get(&self, request: ChunkRequest) -> ChunkFuture<'_, Result<Bytes, ChunkError>>;
}

/// Exact reads from the local chunk store only, used to validate an upload
/// before its staging receipt is issued.
pub trait ChunkReader {
    /// The encoded node whose object hash is `hash`.
    fn get(&self, hash: ContentHash) -> ChunkFuture<'_, Result<Bytes, ChunkStorageError>>;
}
