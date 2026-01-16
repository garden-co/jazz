//! In-memory storage for drivers.
//!
//! This module provides synchronous in-memory storage that can be used by
//! both TestDriver and NativeDriver. It implements storage operations
//! through the StorageRequest/StorageResponse pattern.

use std::collections::{BTreeMap, HashMap, HashSet};

use crate::commit::{Commit, CommitId};
use crate::object::ObjectId;
use crate::storage::ChunkHash;

use super::engine::{StorageRequest, StorageResponse};

// ============================================================================
// MemoryStorage
// ============================================================================

/// In-memory storage for drivers.
///
/// This provides synchronous storage operations that match the
/// StorageRequest/StorageResponse pattern. It stores commits, frontiers,
/// chunks, and object metadata.
#[derive(Debug, Default)]
pub struct MemoryStorage {
    /// Commits by ID.
    commits: HashMap<CommitId, Commit>,
    /// Frontiers by (object_id, branch).
    frontiers: HashMap<(ObjectId, String), Vec<CommitId>>,
    /// Chunks by hash.
    chunks: HashMap<ChunkHash, Vec<u8>>,
    /// Object metadata by object_id.
    object_meta: HashMap<ObjectId, BTreeMap<String, String>>,
    /// Track which objects exist (for list_objects).
    objects: HashSet<ObjectId>,
}

impl MemoryStorage {
    /// Create new empty storage.
    pub fn new() -> Self {
        Self::default()
    }

    /// Execute a storage request.
    ///
    /// Returns a StorageResponse for load operations, None for fire-and-forget operations.
    pub fn execute(&mut self, request: StorageRequest) -> Option<StorageResponse> {
        match request {
            StorageRequest::PutCommit { commit } => {
                let id = commit.compute_id();
                self.commits.insert(id, commit);
                None
            }
            StorageRequest::SetFrontier {
                object_id,
                branch,
                frontier,
            } => {
                self.frontiers.insert((object_id, branch), frontier);
                self.objects.insert(object_id);
                None
            }
            StorageRequest::PutChunk { data, .. } => {
                let hash = ChunkHash::compute(&data);
                self.chunks.insert(hash, data);
                None
            }
            StorageRequest::GetChunk { request_id, hash } => {
                let data = self.chunks.get(&hash).cloned();
                Some(StorageResponse::ChunkLoaded {
                    request_id,
                    hash,
                    data,
                })
            }
            StorageRequest::LoadObject {
                request_id,
                object_id,
                branch,
            } => {
                let frontier = self.get_frontier(object_id, &branch);
                let commits = self.get_commits_for_frontier(&frontier);
                let object_meta = self.object_meta.get(&object_id).cloned();
                Some(StorageResponse::ObjectLoaded {
                    request_id,
                    object_id,
                    branch,
                    object_meta,
                    frontier,
                    commits,
                })
            }
        }
    }

    /// Get a commit by ID.
    pub fn get_commit(&self, id: &CommitId) -> Option<&Commit> {
        self.commits.get(id)
    }

    /// Get frontier for an object's branch.
    pub fn get_frontier(&self, object_id: ObjectId, branch: &str) -> Vec<CommitId> {
        self.frontiers
            .get(&(object_id, branch.to_string()))
            .cloned()
            .unwrap_or_default()
    }

    /// Get a chunk by hash.
    pub fn get_chunk(&self, hash: &ChunkHash) -> Option<&Vec<u8>> {
        self.chunks.get(hash)
    }

    /// Store object metadata.
    pub fn set_object_meta(&mut self, object_id: ObjectId, meta: BTreeMap<String, String>) {
        self.object_meta.insert(object_id, meta);
        self.objects.insert(object_id);
    }

    /// Get object metadata.
    pub fn get_object_meta(&self, object_id: ObjectId) -> Option<&BTreeMap<String, String>> {
        self.object_meta.get(&object_id)
    }

    /// List all known object IDs.
    pub fn list_objects(&self) -> impl Iterator<Item = ObjectId> + '_ {
        self.objects.iter().copied()
    }

    /// List all branches for an object.
    pub fn list_branches(&self, object_id: ObjectId) -> Vec<String> {
        self.frontiers
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

    /// Get total commit count (for testing).
    pub fn commit_count(&self) -> usize {
        self.commits.len()
    }

    /// Get all commits reachable from a frontier.
    fn get_commits_for_frontier(&self, frontier: &[CommitId]) -> Vec<Commit> {
        let mut result = Vec::new();
        let mut visited = HashSet::new();
        let mut to_visit: Vec<CommitId> = frontier.to_vec();

        while let Some(id) = to_visit.pop() {
            if visited.contains(&id) {
                continue;
            }
            visited.insert(id);

            if let Some(commit) = self.commits.get(&id) {
                result.push(commit.clone());
                for parent in &commit.parents {
                    if !visited.contains(parent) {
                        to_visit.push(*parent);
                    }
                }
            }
        }

        result
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_put_and_get_commit() {
        let mut storage = MemoryStorage::new();

        let commit = Commit {
            parents: vec![],
            content: b"test".to_vec().into_boxed_slice(),
            author: "test".to_string(),
            timestamp: 1000,
            meta: None,
        };
        let commit_id = commit.compute_id();

        storage.execute(StorageRequest::PutCommit {
            commit: commit.clone(),
        });

        assert!(storage.get_commit(&commit_id).is_some());
        assert_eq!(storage.get_commit(&commit_id).unwrap().author, "test");
    }

    #[test]
    fn test_set_and_get_frontier() {
        let mut storage = MemoryStorage::new();
        let object_id = ObjectId::new(12345);
        let commit_id = CommitId::from_bytes([1; 32]);

        storage.execute(StorageRequest::SetFrontier {
            object_id,
            branch: "main".to_string(),
            frontier: vec![commit_id],
        });

        assert_eq!(storage.get_frontier(object_id, "main"), vec![commit_id]);
        assert!(storage.objects.contains(&object_id));
    }

    #[test]
    fn test_put_and_get_chunk() {
        let mut storage = MemoryStorage::new();
        let data = b"chunk data".to_vec();
        let hash = ChunkHash::compute(&data);

        storage.execute(StorageRequest::PutChunk {
            request_id: None,
            data: data.clone(),
        });

        assert_eq!(storage.get_chunk(&hash), Some(&data));
    }

    #[test]
    fn test_get_chunk_request_response() {
        let mut storage = MemoryStorage::new();
        let data = b"chunk data".to_vec();
        let hash = ChunkHash::compute(&data);

        // Store chunk
        storage.execute(StorageRequest::PutChunk {
            request_id: None,
            data: data.clone(),
        });

        // Request chunk
        let response = storage.execute(StorageRequest::GetChunk {
            request_id: 42,
            hash,
        });

        assert!(matches!(
            response,
            Some(StorageResponse::ChunkLoaded {
                request_id: 42,
                data: Some(d),
                ..
            }) if d == data
        ));
    }

    #[test]
    fn test_load_object_request_response() {
        let mut storage = MemoryStorage::new();
        let object_id = ObjectId::new(12345);

        // Create a commit chain
        let commit1 = Commit {
            parents: vec![],
            content: b"first".to_vec().into_boxed_slice(),
            author: "test".to_string(),
            timestamp: 1000,
            meta: None,
        };
        let commit1_id = commit1.compute_id();

        let commit2 = Commit {
            parents: vec![commit1_id],
            content: b"second".to_vec().into_boxed_slice(),
            author: "test".to_string(),
            timestamp: 2000,
            meta: None,
        };
        let commit2_id = commit2.compute_id();

        storage.execute(StorageRequest::PutCommit { commit: commit1 });
        storage.execute(StorageRequest::PutCommit { commit: commit2 });
        storage.execute(StorageRequest::SetFrontier {
            object_id,
            branch: "main".to_string(),
            frontier: vec![commit2_id],
        });

        // Load object
        let response = storage.execute(StorageRequest::LoadObject {
            request_id: 99,
            object_id,
            branch: "main".to_string(),
        });

        match response {
            Some(StorageResponse::ObjectLoaded {
                request_id,
                object_id: oid,
                branch,
                frontier,
                commits,
                ..
            }) => {
                assert_eq!(request_id, 99);
                assert_eq!(oid, object_id);
                assert_eq!(branch, "main");
                assert_eq!(frontier, vec![commit2_id]);
                assert_eq!(commits.len(), 2);
            }
            _ => panic!("Expected ObjectLoaded response"),
        }
    }
}
