use std::collections::BTreeMap;

use blake3::Hasher;

use crate::object::ObjectId;

/// BLAKE3 hash identifying a commit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CommitId(pub [u8; 32]);

/// Storage state of a commit.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum StoredState {
    #[default]
    Pending,
    Stored,
    Errored(String),
    PendingDelete,
}

/// A commit in an object's history.
#[derive(Debug, Clone)]
pub struct Commit {
    pub parents: Vec<CommitId>,
    pub content: Vec<u8>,
    /// Microseconds since Unix epoch.
    pub timestamp: u64,
    pub author: ObjectId,
    pub metadata: Option<BTreeMap<String, String>>,
    /// Storage state (not included in commit hash).
    pub stored_state: StoredState,
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
        hasher.update(self.author.0.as_bytes());

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
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[test]
    fn commit_id_is_deterministic() {
        let commit = Commit {
            parents: vec![],
            content: b"hello".to_vec(),
            timestamp: 1234567890,
            author: ObjectId(Uuid::nil()),
            metadata: None,
            stored_state: StoredState::default(),
        };

        let id1 = commit.id();
        let id2 = commit.id();
        assert_eq!(id1, id2);
    }

    #[test]
    fn different_commits_have_different_ids() {
        let commit1 = Commit {
            parents: vec![],
            content: b"hello".to_vec(),
            timestamp: 1234567890,
            author: ObjectId(Uuid::nil()),
            metadata: None,
            stored_state: StoredState::default(),
        };

        let commit2 = Commit {
            parents: vec![],
            content: b"world".to_vec(),
            timestamp: 1234567890,
            author: ObjectId(Uuid::nil()),
            metadata: None,
            stored_state: StoredState::default(),
        };

        assert_ne!(commit1.id(), commit2.id());
    }

    #[test]
    fn stored_state_does_not_affect_commit_id() {
        let commit1 = Commit {
            parents: vec![],
            content: b"hello".to_vec(),
            timestamp: 1234567890,
            author: ObjectId(Uuid::nil()),
            metadata: None,
            stored_state: StoredState::Pending,
        };

        let commit2 = Commit {
            parents: vec![],
            content: b"hello".to_vec(),
            timestamp: 1234567890,
            author: ObjectId(Uuid::nil()),
            metadata: None,
            stored_state: StoredState::Stored,
        };

        assert_eq!(commit1.id(), commit2.id());
    }
}
