use std::collections::HashMap;

use internment::Intern;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use smolset::SmolSet;
use uuid::Uuid;

use crate::{
    commit::{Commit, CommitId},
    metadata::MetadataKey,
};

/// Interned UUIDv7 identifying an object.
/// Pointer-sized (8 bytes), Copy, fast equality via pointer comparison.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ObjectId(pub Intern<Uuid>);

impl Serialize for ObjectId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.uuid().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ObjectId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let uuid = Uuid::deserialize(deserializer)?;
        Ok(ObjectId::from_uuid(uuid))
    }
}

/// How deeply a branch has been loaded from storage.
/// Note: With sync storage, this is mainly used to track whether branch data exists.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum BranchLoadedState {
    #[default]
    NotLoaded,
    TipIdsOnly,
    TipsOnly,
    AllCommits,
}

impl std::fmt::Display for ObjectId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl ObjectId {
    pub fn new() -> Self {
        Self(Intern::new(Uuid::now_v7()))
    }

    /// Get the underlying UUID reference.
    pub fn uuid(&self) -> &Uuid {
        &self.0
    }

    /// Create an ObjectId from a raw Uuid.
    pub fn from_uuid(uuid: Uuid) -> Self {
        Self(Intern::new(uuid))
    }
}

impl Default for ObjectId {
    fn default() -> Self {
        Self::new()
    }
}

impl PartialOrd for ObjectId {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ObjectId {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.uuid().cmp(other.uuid())
    }
}

/// Interned name identifying a branch within an object.
/// Pointer-sized (8 bytes), Copy, fast equality via pointer comparison.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BranchName(pub Intern<String>);

impl Serialize for BranchName {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.as_str().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for BranchName {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Ok(BranchName::new(s))
    }
}

impl BranchName {
    pub fn new(name: impl Into<String>) -> Self {
        Self(Intern::new(name.into()))
    }

    /// Get the underlying string reference.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<T: Into<String>> From<T> for BranchName {
    fn from(s: T) -> Self {
        Self(Intern::new(s.into()))
    }
}

impl std::fmt::Display for BranchName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// A branch containing commits and tracking unmerged tips.
#[derive(Debug, Clone, Default)]
pub struct Branch {
    pub commits: HashMap<CommitId, Commit>,
    /// Current tips (unmerged heads). Inline storage for ≤2 tips.
    pub tips: SmolSet<[CommitId; 2]>,
    /// Truncation boundary. None = full history from roots.
    /// Some(tails) = history only includes tails and their descendants.
    pub tails: Option<SmolSet<[CommitId; 2]>>,
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

    pub fn table_name(&self) -> &str {
        self.metadata
            .get(MetadataKey::Table.as_str())
            .expect("unexpected object with no table name")
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
