//! Content and commit storage traits.
//!
//! See: docs/content/docs/internals/streaming-and-persistence.mdx

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;

use crate::commit::CommitId;
use crate::object::ObjectId;

/// Threshold for inline content storage (bytes).
/// Content at or below this size is stored directly in the commit.
/// Content above this size is chunked and stored separately.
/// TODO: Reduce back to 1KB once sorted chunk indices are implemented
/// See: docs/content/docs/internals/sorted-chunk-indices.mdx
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
    /// - Inline:  0x00 + u32(length) + bytes
    /// - Chunked: 0x01 + u32(chunk_count) + [32-byte ChunkHash]*count
    pub fn to_row_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        match self {
            ContentRef::Inline(data) => {
                buf.push(0x00); // Tag: inline
                buf.extend_from_slice(&(data.len() as u32).to_le_bytes());
                buf.extend_from_slice(data);
            }
            ContentRef::Chunked(hashes) => {
                buf.push(0x01); // Tag: chunked
                buf.extend_from_slice(&(hashes.len() as u32).to_le_bytes());
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
                // Inline: tag + u32(len) + bytes
                if data.len() < pos + 4 {
                    return Err(ContentRefError::UnexpectedEof);
                }
                let len_bytes: [u8; 4] = data[pos..pos + 4].try_into().unwrap();
                let len = u32::from_le_bytes(len_bytes) as usize;
                pos += 4;

                if data.len() < pos + len {
                    return Err(ContentRefError::UnexpectedEof);
                }
                let content = data[pos..pos + len].to_vec().into_boxed_slice();
                pos += len;

                Ok((ContentRef::Inline(content), pos))
            }
            0x01 => {
                // Chunked: tag + u32(count) + [32-byte hash]*count
                if data.len() < pos + 4 {
                    return Err(ContentRefError::UnexpectedEof);
                }
                let count_bytes: [u8; 4] = data[pos..pos + 4].try_into().unwrap();
                let count = u32::from_le_bytes(count_bytes) as usize;
                pos += 4;

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
    InvalidTag(u8),
}

impl std::fmt::Display for ContentRefError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ContentRefError::UnexpectedEof => write!(f, "unexpected end of ContentRef data"),
            ContentRefError::InvalidTag(tag) => write!(f, "invalid ContentRef tag: {}", tag),
        }
    }
}

impl std::error::Error for ContentRefError {}

/// Metadata about a commit (without full content bytes).
#[derive(Debug, Clone)]
pub struct CommitMeta {
    pub id: CommitId,
    pub parents: Vec<CommitId>,
    pub author: String,
    pub timestamp: u64,
    /// Size of the commit content in bytes.
    pub content_size: usize,
}

/// Storage interface for content chunks.
///
/// On native targets, implementations must be Send + Sync.
/// On WASM, these bounds are relaxed since it's single-threaded.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg(not(target_arch = "wasm32"))]
pub trait ChunkStore: Send + Sync {
    /// Get chunk by hash, returns None if not found.
    async fn get_chunk(&self, hash: &ChunkHash) -> Option<Bytes>;

    /// Store chunk, returns its hash.
    async fn put_chunk(&self, data: Bytes) -> ChunkHash;

    /// Check if chunk exists.
    async fn has_chunk(&self, hash: &ChunkHash) -> bool;
}

/// Storage interface for content chunks (WASM version without Send + Sync).
#[cfg(target_arch = "wasm32")]
#[async_trait(?Send)]
pub trait ChunkStore {
    /// Get chunk by hash, returns None if not found.
    async fn get_chunk(&self, hash: &ChunkHash) -> Option<Bytes>;

    /// Store chunk, returns its hash.
    async fn put_chunk(&self, data: Bytes) -> ChunkHash;

    /// Check if chunk exists.
    async fn has_chunk(&self, hash: &ChunkHash) -> bool;
}

/// Storage interface for commits.
///
/// On native targets, implementations must be Send + Sync.
/// On WASM, these bounds are relaxed since it's single-threaded.
#[cfg(not(target_arch = "wasm32"))]
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

    /// List all object IDs that have data in this store.
    fn list_objects(&self) -> BoxStream<'_, u128>;

    /// List all branch names for an object.
    async fn list_branches(&self, object_id: u128) -> Vec<String>;
}

/// Storage interface for commits (WASM version without Send + Sync).
#[cfg(target_arch = "wasm32")]
#[async_trait(?Send)]
pub trait CommitStore {
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

    /// List all object IDs that have data in this store.
    fn list_objects(&self) -> BoxStream<'_, u128>;

    /// List all branch names for an object.
    async fn list_branches(&self, object_id: u128) -> Vec<String>;
}

/// Storage interface for sync-related persistent state.
///
/// This trait handles persistence of sync metadata that must survive restarts,
/// like which objects have unsynced local changes that need to be pushed to server.
#[cfg(not(target_arch = "wasm32"))]
#[async_trait]
pub trait SyncStateStore: Send + Sync {
    /// Mark an object as having unsynced local changes.
    async fn mark_unsynced(&self, object_id: ObjectId);

    /// Clear the unsynced flag for an object (after server acknowledgment).
    async fn clear_unsynced(&self, object_id: &ObjectId);

    /// Get all objects with unsynced local changes.
    async fn get_unsynced_objects(&self) -> Vec<ObjectId>;

    /// Check if an object has unsynced changes.
    async fn is_unsynced(&self, object_id: &ObjectId) -> bool;
}

/// Storage interface for sync state (WASM version without Send + Sync).
#[cfg(target_arch = "wasm32")]
#[async_trait(?Send)]
pub trait SyncStateStore {
    /// Mark an object as having unsynced local changes.
    async fn mark_unsynced(&self, object_id: ObjectId);

    /// Clear the unsynced flag for an object (after server acknowledgment).
    async fn clear_unsynced(&self, object_id: &ObjectId);

    /// Get all objects with unsynced local changes.
    async fn get_unsynced_objects(&self) -> Vec<ObjectId>;

    /// Check if an object has unsynced changes.
    async fn is_unsynced(&self, object_id: &ObjectId) -> bool;
}

/// Combined storage interface (legacy alias).
#[cfg(not(target_arch = "wasm32"))]
#[async_trait]
pub trait Storage: ChunkStore + CommitStore {}

#[cfg(target_arch = "wasm32")]
#[async_trait(?Send)]
pub trait Storage: ChunkStore + CommitStore {}

// Blanket impl
impl<T: ChunkStore + CommitStore> Storage for T {}

/// Environment trait - combines all storage capabilities.
/// This is the main trait that ObjectManager uses for storage.
#[cfg(not(target_arch = "wasm32"))]
pub trait Environment:
    ChunkStore + CommitStore + SyncStateStore + Send + Sync + std::fmt::Debug
{
}

#[cfg(target_arch = "wasm32")]
pub trait Environment: ChunkStore + CommitStore + SyncStateStore + std::fmt::Debug {}

// Blanket impl for Environment
#[cfg(not(target_arch = "wasm32"))]
impl<T: ChunkStore + CommitStore + SyncStateStore + Send + Sync + std::fmt::Debug> Environment
    for T
{
}

#[cfg(target_arch = "wasm32")]
impl<T: ChunkStore + CommitStore + SyncStateStore + std::fmt::Debug> Environment for T {}

// ========== In-Memory Environment for Testing ==========

use std::collections::{HashMap, HashSet};
use std::sync::RwLock;

/// In-memory environment for testing.
/// Implements ChunkStore, CommitStore, and SyncStateStore.
#[derive(Debug, Default)]
pub struct MemoryEnvironment {
    chunks: RwLock<HashMap<ChunkHash, Bytes>>,
    commits: RwLock<HashMap<CommitId, crate::commit::Commit>>,
    frontiers: RwLock<HashMap<(u128, String), Vec<CommitId>>>,
    truncations: RwLock<HashMap<(u128, String), CommitId>>,
    /// Track which objects exist (object IDs that have at least one branch).
    objects: RwLock<HashSet<u128>>,
    /// Track objects with unsynced local changes (for sync state persistence).
    unsynced: RwLock<HashSet<ObjectId>>,
}

impl MemoryEnvironment {
    pub fn new() -> Self {
        Self::default()
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl ChunkStore for MemoryEnvironment {
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

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl CommitStore for MemoryEnvironment {
    async fn get_commit_meta(&self, id: &CommitId) -> Option<CommitMeta> {
        self.commits.read().unwrap().get(id).map(|c| CommitMeta {
            id: *id,
            parents: c.parents.clone(),
            author: c.author.clone(),
            timestamp: c.timestamp,
            content_size: c.content.len(),
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
        // Track that this object exists
        self.objects.write().unwrap().insert(object_id);
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

    fn list_objects(&self) -> BoxStream<'_, u128> {
        let objects: Vec<u128> = self.objects.read().unwrap().iter().copied().collect();
        Box::pin(futures::stream::iter(objects))
    }

    async fn list_branches(&self, object_id: u128) -> Vec<String> {
        self.frontiers
            .read()
            .unwrap()
            .keys()
            .filter_map(|(oid, branch)| {
                if *oid == object_id {
                    Some(branch.clone())
                } else {
                    None
                }
            })
            .collect()
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl SyncStateStore for MemoryEnvironment {
    async fn mark_unsynced(&self, object_id: ObjectId) {
        self.unsynced.write().unwrap().insert(object_id);
    }

    async fn clear_unsynced(&self, object_id: &ObjectId) {
        self.unsynced.write().unwrap().remove(object_id);
    }

    async fn get_unsynced_objects(&self) -> Vec<ObjectId> {
        self.unsynced.read().unwrap().iter().copied().collect()
    }

    async fn is_unsynced(&self, object_id: &ObjectId) -> bool {
        self.unsynced.read().unwrap().contains(object_id)
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

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl ChunkStore for MemoryContentStore {
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
