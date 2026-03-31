use std::collections::{BTreeMap, HashSet};

use blake3::Hasher;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;

use crate::metadata::{DeleteKind, MetadataKey, RowProvenance, row_provenance_from_metadata};
use crate::sync_manager::DurabilityTier;

/// BLAKE3 hash identifying a commit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct CommitId(pub [u8; 32]);

/// Persistence acknowledgment state (runtime only, not serialized).
///
/// Tracks which persistence tiers have confirmed storing this commit.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CommitAckState {
    pub confirmed_tiers: HashSet<DurabilityTier>,
}

/// Storage state of a commit (runtime only, not serialized).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum StoredState {
    #[default]
    Pending,
    Stored,
    Errored(String),
    PendingDelete,
}

/// A commit in an object's history.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Commit {
    /// Parent commit IDs. Inline storage for 0-2 parents (root, regular, merge).
    pub parents: SmallVec<[CommitId; 2]>,
    pub content: Vec<u8>,
    /// Microseconds since Unix epoch.
    pub timestamp: u64,
    pub author: String,
    pub metadata: Option<BTreeMap<String, String>>,
    /// Storage state (runtime only, not serialized).
    #[serde(skip, default)]
    pub stored_state: StoredState,
    /// Persistence acknowledgment state (runtime only, not serialized).
    #[serde(skip, default)]
    pub ack_state: CommitAckState,
}

impl Commit {
    /// Compute the CommitId by hashing the serialized commit data.
    pub fn id(&self) -> CommitId {
        let mut hasher = Hasher::new();

        // Hash parents
        hasher.update(&(self.parents.len() as u64).to_le_bytes());
        for parent in &self.parents {
            hasher.update(&parent.0);
        }

        // Hash content
        hasher.update(&(self.content.len() as u64).to_le_bytes());
        hasher.update(&self.content);

        // Hash timestamp
        hasher.update(&self.timestamp.to_le_bytes());

        // Hash author
        hasher.update(self.author.as_bytes());

        // Hash metadata
        if let Some(meta) = &self.metadata {
            hasher.update(&[1u8]); // presence marker
            hasher.update(&(meta.len() as u64).to_le_bytes());
            for (k, v) in meta {
                hasher.update(&(k.len() as u64).to_le_bytes());
                hasher.update(k.as_bytes());
                hasher.update(&(v.len() as u64).to_le_bytes());
                hasher.update(v.as_bytes());
            }
        } else {
            hasher.update(&[0u8]); // absence marker
        }

        CommitId(*hasher.finalize().as_bytes())
    }

    pub fn is_soft_deleted(&self) -> bool {
        self.metadata
            .as_ref()
            .and_then(|m| m.get(MetadataKey::Delete.as_str()))
            .map(|v| v == DeleteKind::Soft.as_str())
            .unwrap_or(false)
    }

    pub fn is_hard_deleted(&self) -> bool {
        self.metadata
            .as_ref()
            .and_then(|m| m.get(MetadataKey::Delete.as_str()))
            .map(|v| v == DeleteKind::Hard.as_str())
            .unwrap_or(false)
    }

    pub fn row_provenance(&self) -> Option<RowProvenance> {
        row_provenance_from_metadata(self.metadata.as_ref(), &self.author, self.timestamp)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use smallvec::smallvec;

    #[test]
    fn commit_id_is_deterministic() {
        let commit = Commit {
            parents: smallvec![],
            content: b"hello".to_vec(),
            timestamp: 1234567890,
            author: "jazz:test".to_string(),
            metadata: None,
            stored_state: StoredState::default(),
            ack_state: CommitAckState::default(),
        };

        let id1 = commit.id();
        let id2 = commit.id();
        assert_eq!(id1, id2);
    }

    #[test]
    fn different_commits_have_different_ids() {
        let commit1 = Commit {
            parents: smallvec![],
            content: b"hello".to_vec(),
            timestamp: 1234567890,
            author: "jazz:test".to_string(),
            metadata: None,
            stored_state: StoredState::default(),
            ack_state: CommitAckState::default(),
        };

        let commit2 = Commit {
            parents: smallvec![],
            content: b"world".to_vec(),
            timestamp: 1234567890,
            author: "jazz:test".to_string(),
            metadata: None,
            stored_state: StoredState::default(),
            ack_state: CommitAckState::default(),
        };

        assert_ne!(commit1.id(), commit2.id());
    }

    #[test]
    fn stored_state_does_not_affect_commit_id() {
        let commit1 = Commit {
            parents: smallvec![],
            content: b"hello".to_vec(),
            timestamp: 1234567890,
            author: "jazz:test".to_string(),
            metadata: None,
            stored_state: StoredState::Pending,
            ack_state: CommitAckState::default(),
        };

        let commit2 = Commit {
            parents: smallvec![],
            content: b"hello".to_vec(),
            timestamp: 1234567890,
            author: "jazz:test".to_string(),
            metadata: None,
            stored_state: StoredState::Stored,
            ack_state: CommitAckState::default(),
        };

        assert_eq!(commit1.id(), commit2.id());
    }

    #[test]
    fn ack_state_does_not_affect_commit_id() {
        let commit1 = Commit {
            parents: smallvec![],
            content: b"hello".to_vec(),
            timestamp: 1234567890,
            author: "jazz:test".to_string(),
            metadata: None,
            stored_state: StoredState::default(),
            ack_state: CommitAckState::default(),
        };

        let mut ack_state = CommitAckState::default();
        ack_state.confirmed_tiers.insert(DurabilityTier::Worker);
        ack_state.confirmed_tiers.insert(DurabilityTier::EdgeServer);

        let commit2 = Commit {
            parents: smallvec![],
            content: b"hello".to_vec(),
            timestamp: 1234567890,
            author: "jazz:test".to_string(),
            metadata: None,
            stored_state: StoredState::default(),
            ack_state,
        };

        assert_eq!(commit1.id(), commit2.id());
    }

    #[test]
    fn row_provenance_requires_explicit_edit_metadata() {
        let commit = Commit {
            parents: smallvec![],
            content: b"hello".to_vec(),
            timestamp: 1234567890,
            author: "jazz:test".to_string(),
            metadata: None,
            stored_state: StoredState::default(),
            ack_state: CommitAckState::default(),
        };

        assert_eq!(commit.row_provenance(), None);
    }
}
