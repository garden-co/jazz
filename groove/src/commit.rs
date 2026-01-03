use std::collections::BTreeMap;

use crate::storage::ContentRef;

/// A commit ID is the BLAKE3 hash of the commit's canonical representation.
/// Using full 256-bit hash for now (naive implementation).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CommitId([u8; 32]);

impl CommitId {
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        CommitId(bytes)
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

/// A commit in the object's history.
/// Contains a full snapshot of the object state.
#[derive(Debug, Clone)]
pub struct Commit {
    /// Parent commit IDs (empty for root commits, multiple for merge commits)
    pub parents: Vec<CommitId>,
    /// Snapshot of the object state (inline for small, chunked for large)
    pub content: ContentRef,
    /// Author identifier (account/device ID)
    pub author: String,
    /// Timestamp (milliseconds since epoch)
    pub timestamp: u64,
    /// Optional metadata
    pub meta: Option<BTreeMap<String, String>>,
}

impl Commit {
    /// Compute the commit ID by hashing the commit's canonical representation.
    pub fn compute_id(&self) -> CommitId {
        let mut hasher = blake3::Hasher::new();

        // Hash parents
        hasher.update(&(self.parents.len() as u64).to_le_bytes());
        for parent in &self.parents {
            hasher.update(parent.as_bytes());
        }

        // Hash content ref
        match &self.content {
            ContentRef::Inline(data) => {
                hasher.update(&[0u8]); // Tag for inline
                hasher.update(&(data.len() as u64).to_le_bytes());
                hasher.update(data);
            }
            ContentRef::Chunked(hashes) => {
                hasher.update(&[1u8]); // Tag for chunked
                hasher.update(&(hashes.len() as u64).to_le_bytes());
                for hash in hashes {
                    hasher.update(hash.as_bytes());
                }
            }
        }

        // Hash author
        hasher.update(&(self.author.len() as u64).to_le_bytes());
        hasher.update(self.author.as_bytes());

        // Hash timestamp
        hasher.update(&self.timestamp.to_le_bytes());

        // Hash metadata (simplified: just presence for now)
        hasher.update(&[self.meta.is_some() as u8]);
        if let Some(meta) = &self.meta {
            hasher.update(&(meta.len() as u64).to_le_bytes());
            for (k, v) in meta {
                hasher.update(&(k.len() as u64).to_le_bytes());
                hasher.update(k.as_bytes());
                hasher.update(&(v.len() as u64).to_le_bytes());
                hasher.update(v.as_bytes());
            }
        }

        CommitId(*hasher.finalize().as_bytes())
    }
}

// Tests have been moved to tests/commit.rs
