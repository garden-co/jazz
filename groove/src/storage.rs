use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;

use crate::commit::CommitId;

/// Threshold for inline content storage (bytes).
/// Content at or below this size is stored directly in the commit.
/// Content above this size is chunked and stored separately.
/// TODO: Reduce back to 1KB once sorted chunk indices are implemented (see specs/sorted-chunk-indices.md)
pub const INLINE_THRESHOLD: usize = 1024 * 1024; // 1MB (temporary)

/// A chunk hash is the BLAKE3 hash of a content chunk.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ChunkHash([u8; 32]);

impl ChunkHash {
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        ChunkHash(bytes)
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Compute hash of content chunk.
    pub fn compute(data: &[u8]) -> Self {
        ChunkHash(*blake3::hash(data).as_bytes())
    }
}

/// Reference to commit content - either inline or chunked.
#[derive(Debug, Clone, PartialEq)]
pub enum ContentRef {
    /// Small content stored directly in the commit.
    Inline(Box<[u8]>),
    /// Large content split into chunks, referenced by hash.
    Chunked(Vec<ChunkHash>),
}

impl ContentRef {
    /// Create a ContentRef from bytes.
    /// Uses inline storage if within threshold, otherwise caller must chunk.
    pub fn inline(data: impl Into<Box<[u8]>>) -> Self {
        ContentRef::Inline(data.into())
    }

    /// Create a chunked ContentRef from chunk hashes.
    pub fn chunked(hashes: Vec<ChunkHash>) -> Self {
        ContentRef::Chunked(hashes)
    }

    /// Returns true if content is stored inline.
    pub fn is_inline(&self) -> bool {
        matches!(self, ContentRef::Inline(_))
    }

    /// Returns inline content if available.
    pub fn as_inline(&self) -> Option<&[u8]> {
        match self {
            ContentRef::Inline(data) => Some(data),
            ContentRef::Chunked(_) => None,
        }
    }

    /// Returns chunk hashes if content is chunked.
    pub fn as_chunks(&self) -> Option<&[ChunkHash]> {
        match self {
            ContentRef::Inline(_) => None,
            ContentRef::Chunked(hashes) => Some(hashes),
        }
    }

    /// Serialize ContentRef for embedding in row data.
    ///
    /// Format:
    /// - Inline:  0x00 + varint(length) + bytes
    /// - Chunked: 0x01 + varint(chunk_count) + [32-byte ChunkHash]*count
    pub fn to_row_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        match self {
            ContentRef::Inline(data) => {
                buf.push(0x00); // Tag: inline
                encode_varint(data.len(), &mut buf);
                buf.extend_from_slice(data);
            }
            ContentRef::Chunked(hashes) => {
                buf.push(0x01); // Tag: chunked
                encode_varint(hashes.len(), &mut buf);
                for hash in hashes {
                    buf.extend_from_slice(hash.as_bytes());
                }
            }
        }
        buf
    }

    /// Deserialize ContentRef from row data.
    ///
    /// Returns the ContentRef and the number of bytes consumed.
    pub fn from_row_bytes(data: &[u8]) -> Result<(Self, usize), ContentRefError> {
        if data.is_empty() {
            return Err(ContentRefError::UnexpectedEof);
        }

        let tag = data[0];
        let mut pos = 1;

        match tag {
            0x00 => {
                // Inline
                let (len, consumed) = decode_varint(&data[pos..])?;
                pos += consumed;

                if data.len() < pos + len {
                    return Err(ContentRefError::UnexpectedEof);
                }
                let content = data[pos..pos + len].to_vec().into_boxed_slice();
                pos += len;

                Ok((ContentRef::Inline(content), pos))
            }
            0x01 => {
                // Chunked
                let (count, consumed) = decode_varint(&data[pos..])?;
                pos += consumed;

                let mut hashes = Vec::with_capacity(count);
                for _ in 0..count {
                    if data.len() < pos + 32 {
                        return Err(ContentRefError::UnexpectedEof);
                    }
                    let mut hash_bytes = [0u8; 32];
                    hash_bytes.copy_from_slice(&data[pos..pos + 32]);
                    hashes.push(ChunkHash::from_bytes(hash_bytes));
                    pos += 32;
                }

                Ok((ContentRef::Chunked(hashes), pos))
            }
            _ => Err(ContentRefError::InvalidTag(tag)),
        }
    }
}

/// Errors during ContentRef deserialization.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ContentRefError {
    UnexpectedEof,
    VarintOverflow,
    InvalidTag(u8),
}

impl std::fmt::Display for ContentRefError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ContentRefError::UnexpectedEof => write!(f, "unexpected end of ContentRef data"),
            ContentRefError::VarintOverflow => write!(f, "varint overflow"),
            ContentRefError::InvalidTag(tag) => write!(f, "invalid ContentRef tag: {}", tag),
        }
    }
}

impl std::error::Error for ContentRefError {}

/// Encode a varint (LEB128 unsigned).
fn encode_varint(mut value: usize, buf: &mut Vec<u8>) {
    loop {
        let mut byte = (value & 0x7f) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        buf.push(byte);
        if value == 0 {
            break;
        }
    }
}

/// Decode a varint (LEB128 unsigned). Returns (value, bytes_consumed).
fn decode_varint(data: &[u8]) -> Result<(usize, usize), ContentRefError> {
    let mut result: usize = 0;
    let mut shift = 0;

    for (i, &byte) in data.iter().enumerate() {
        result |= ((byte & 0x7f) as usize) << shift;
        if byte & 0x80 == 0 {
            return Ok((result, i + 1));
        }
        shift += 7;
        if shift >= 64 {
            return Err(ContentRefError::VarintOverflow);
        }
    }

    Err(ContentRefError::UnexpectedEof)
}

/// Metadata about a commit (without content).
#[derive(Debug, Clone)]
pub struct CommitMeta {
    pub id: CommitId,
    pub parents: Vec<CommitId>,
    pub author: String,
    pub timestamp: u64,
    /// Whether content is inline or chunked.
    pub content_ref: ContentRef,
}

/// Storage interface for content chunks.
#[async_trait]
pub trait ContentStore: Send + Sync {
    /// Get chunk by hash, returns None if not found.
    async fn get_chunk(&self, hash: &ChunkHash) -> Option<Bytes>;

    /// Store chunk, returns its hash.
    async fn put_chunk(&self, data: Bytes) -> ChunkHash;

    /// Check if chunk exists.
    async fn has_chunk(&self, hash: &ChunkHash) -> bool;
}

/// Storage interface for commits.
#[async_trait]
pub trait CommitStore: Send + Sync {
    /// Get commit metadata (without loading chunked content).
    async fn get_commit_meta(&self, id: &CommitId) -> Option<CommitMeta>;

    /// Get full commit (loads inline content, but not chunked).
    async fn get_commit(&self, id: &CommitId) -> Option<crate::commit::Commit>;

    /// Store commit.
    async fn put_commit(&self, commit: &crate::commit::Commit) -> CommitId;

    /// Get frontier commit IDs for a branch.
    async fn get_frontier(&self, object_id: u128, branch: &str) -> Vec<CommitId>;

    /// Update frontier for a branch.
    async fn set_frontier(&self, object_id: u128, branch: &str, frontier: &[CommitId]);

    /// Get truncation point for a branch.
    async fn get_truncation(&self, object_id: u128, branch: &str) -> Option<CommitId>;

    /// Set truncation point for a branch.
    async fn set_truncation(&self, object_id: u128, branch: &str, truncation: Option<CommitId>);

    /// Stream commit IDs for an object's branch (for partial loading).
    fn list_commits(&self, object_id: u128, branch: &str) -> BoxStream<'_, CommitId>;
}

/// Combined storage interface (legacy alias).
#[async_trait]
pub trait Storage: ContentStore + CommitStore {}

// Blanket impl
impl<T: ContentStore + CommitStore> Storage for T {}

/// Environment trait - combines all storage capabilities.
/// This is the main trait that LocalNode uses for storage.
pub trait Environment: ContentStore + CommitStore + Send + Sync + std::fmt::Debug {}

// Blanket impl for Environment
impl<T: ContentStore + CommitStore + Send + Sync + std::fmt::Debug> Environment for T {}

// ========== In-Memory Environment for Testing ==========

use std::collections::HashMap;
use std::sync::RwLock;

/// In-memory environment for testing.
/// Implements both ContentStore and CommitStore.
#[derive(Debug, Default)]
pub struct MemoryEnvironment {
    chunks: RwLock<HashMap<ChunkHash, Bytes>>,
    commits: RwLock<HashMap<CommitId, crate::commit::Commit>>,
    frontiers: RwLock<HashMap<(u128, String), Vec<CommitId>>>,
    truncations: RwLock<HashMap<(u128, String), CommitId>>,
}

impl MemoryEnvironment {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl ContentStore for MemoryEnvironment {
    async fn get_chunk(&self, hash: &ChunkHash) -> Option<Bytes> {
        self.chunks.read().unwrap().get(hash).cloned()
    }

    async fn put_chunk(&self, data: Bytes) -> ChunkHash {
        let hash = ChunkHash::compute(&data);
        self.chunks.write().unwrap().insert(hash, data);
        hash
    }

    async fn has_chunk(&self, hash: &ChunkHash) -> bool {
        self.chunks.read().unwrap().contains_key(hash)
    }
}

#[async_trait]
impl CommitStore for MemoryEnvironment {
    async fn get_commit_meta(&self, id: &CommitId) -> Option<CommitMeta> {
        self.commits.read().unwrap().get(id).map(|c| CommitMeta {
            id: *id,
            parents: c.parents.clone(),
            author: c.author.clone(),
            timestamp: c.timestamp,
            content_ref: c.content.clone(),
        })
    }

    async fn get_commit(&self, id: &CommitId) -> Option<crate::commit::Commit> {
        self.commits.read().unwrap().get(id).cloned()
    }

    async fn put_commit(&self, commit: &crate::commit::Commit) -> CommitId {
        let id = commit.compute_id();
        self.commits.write().unwrap().insert(id, commit.clone());
        id
    }

    async fn get_frontier(&self, object_id: u128, branch: &str) -> Vec<CommitId> {
        self.frontiers
            .read()
            .unwrap()
            .get(&(object_id, branch.to_string()))
            .cloned()
            .unwrap_or_default()
    }

    async fn set_frontier(&self, object_id: u128, branch: &str, frontier: &[CommitId]) {
        self.frontiers
            .write()
            .unwrap()
            .insert((object_id, branch.to_string()), frontier.to_vec());
    }

    async fn get_truncation(&self, object_id: u128, branch: &str) -> Option<CommitId> {
        self.truncations
            .read()
            .unwrap()
            .get(&(object_id, branch.to_string()))
            .copied()
    }

    async fn set_truncation(&self, object_id: u128, branch: &str, truncation: Option<CommitId>) {
        let mut truncations = self.truncations.write().unwrap();
        let key = (object_id, branch.to_string());
        match truncation {
            Some(id) => {
                truncations.insert(key, id);
            }
            None => {
                truncations.remove(&key);
            }
        }
    }

    fn list_commits(&self, object_id: u128, branch: &str) -> BoxStream<'_, CommitId> {
        // Walk back from frontier through parent links to find all commits
        let frontier = self
            .frontiers
            .read()
            .unwrap()
            .get(&(object_id, branch.to_string()))
            .cloned()
            .unwrap_or_default();

        let commits = self.commits.read().unwrap();
        let mut all_commits = Vec::new();
        let mut visited = std::collections::HashSet::new();
        let mut to_visit: Vec<CommitId> = frontier;

        while let Some(id) = to_visit.pop() {
            if visited.contains(&id) {
                continue;
            }
            visited.insert(id);
            all_commits.push(id);

            if let Some(commit) = commits.get(&id) {
                for parent in &commit.parents {
                    if !visited.contains(parent) {
                        to_visit.push(*parent);
                    }
                }
            }
        }

        Box::pin(futures::stream::iter(all_commits))
    }
}

/// Simple in-memory content store (legacy, for backwards compatibility).
#[derive(Debug, Default)]
pub struct MemoryContentStore {
    chunks: RwLock<HashMap<ChunkHash, Bytes>>,
}

impl MemoryContentStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl ContentStore for MemoryContentStore {
    async fn get_chunk(&self, hash: &ChunkHash) -> Option<Bytes> {
        self.chunks.read().unwrap().get(hash).cloned()
    }

    async fn put_chunk(&self, data: Bytes) -> ChunkHash {
        let hash = ChunkHash::compute(&data);
        self.chunks.write().unwrap().insert(hash, data);
        hash
    }

    async fn has_chunk(&self, hash: &ChunkHash) -> bool {
        self.chunks.read().unwrap().contains_key(hash)
    }
}

// Tests have been moved to tests/storage.rs
