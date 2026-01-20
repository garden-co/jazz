use std::collections::{HashMap, HashSet};

use uuid::Uuid;

use crate::commit::{Commit, CommitId};

/// UUIDv7 identifying an object.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ObjectId(pub Uuid);

/// How deeply a branch has been loaded from storage.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum BranchLoadedState {
    #[default]
    NotLoaded,
    TipIdsOnly,
    TipsOnly,
    AllCommits,
}

/// State of an object in the manager.
#[derive(Debug, Clone)]
pub enum ObjectState {
    /// Created locally, persistence pending. Operations work immediately.
    Creating(Object),
    /// Being loaded from storage. Operations must wait/poll.
    Loading,
    /// Fully persisted/loaded. Operations work immediately.
    Available(Object),
}

impl ObjectId {
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }
}

impl Default for ObjectId {
    fn default() -> Self {
        Self::new()
    }
}

/// Name identifying a branch within an object.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BranchName(pub String);

impl BranchName {
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }
}

impl<T: Into<String>> From<T> for BranchName {
    fn from(s: T) -> Self {
        Self(s.into())
    }
}

/// A branch containing commits and tracking unmerged tips.
#[derive(Debug, Clone, Default)]
pub struct Branch {
    pub commits: HashMap<CommitId, Commit>,
    pub tips: HashSet<CommitId>,
    /// Truncation boundary. None = full history from roots.
    /// Some(tails) = history only includes tails and their descendants.
    pub tails: Option<HashSet<CommitId>>,
    pub loaded_state: BranchLoadedState,
}

/// An object with metadata and named branches.
#[derive(Debug, Clone)]
pub struct Object {
    pub id: ObjectId,
    pub metadata: HashMap<String, String>,
    pub branches: HashMap<BranchName, Branch>,
}

impl Object {
    pub fn new(metadata: Option<HashMap<String, String>>) -> Self {
        Self {
            id: ObjectId::new(),
            metadata: metadata.unwrap_or_default(),
            branches: HashMap::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn object_id_generates_unique_values() {
        let id1 = ObjectId::new();
        let id2 = ObjectId::new();
        assert_ne!(id1, id2);
    }
}
