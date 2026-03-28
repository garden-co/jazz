use std::collections::HashMap;

use internment::Intern;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use smolset::SmolSet;
use uuid::Uuid;

use crate::commit::{Commit, CommitId};
use crate::query_manager::types::BatchId;

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

/// Persisted metadata for one batch under a shared branch prefix.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrefixBatchMeta {
    pub batch_id: BatchId,
    pub batch_ord: u32,
    pub root_commit_id: CommitId,
    pub head_commit_id: CommitId,
    pub first_timestamp: u64,
    pub last_timestamp: u64,
    pub parent_batch_ords: Vec<u32>,
    pub child_count: u32,
}

/// In-memory per-prefix batch catalog.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct PrefixBatchCatalog {
    batch_ord_by_id: HashMap<BatchId, u32>,
    batches_by_ord: Vec<PrefixBatchMeta>,
    leaf_batch_ords: SmolSet<[u32; 4]>,
}

impl PrefixBatchCatalog {
    pub fn next_batch_ord(&self) -> u32 {
        self.batches_by_ord.len() as u32
    }

    pub fn batch_meta(&self, batch_id: &BatchId) -> Option<&PrefixBatchMeta> {
        let batch_ord = *self.batch_ord_by_id.get(batch_id)?;
        self.batch_meta_by_ord(batch_ord)
    }

    pub fn batch_meta_mut(&mut self, batch_id: &BatchId) -> Option<&mut PrefixBatchMeta> {
        let batch_ord = *self.batch_ord_by_id.get(batch_id)?;
        self.batch_meta_by_ord_mut(batch_ord)
    }

    pub fn batch_meta_by_ord(&self, batch_ord: u32) -> Option<&PrefixBatchMeta> {
        self.batches_by_ord.get(batch_ord as usize)
    }

    pub fn batch_meta_by_ord_mut(&mut self, batch_ord: u32) -> Option<&mut PrefixBatchMeta> {
        self.batches_by_ord.get_mut(batch_ord as usize)
    }

    pub fn insert_batch_meta(&mut self, meta: PrefixBatchMeta) {
        let batch_ord = meta.batch_ord as usize;
        if let Some(existing_ord) = self.batch_ord_by_id.insert(meta.batch_id, meta.batch_ord) {
            debug_assert_eq!(
                existing_ord, meta.batch_ord,
                "batch {} changed ord from {} to {}",
                meta.batch_id, existing_ord, meta.batch_ord
            );
        }
        match batch_ord.cmp(&self.batches_by_ord.len()) {
            std::cmp::Ordering::Less => {
                let replaced_batch_id = self.batches_by_ord[batch_ord].batch_id;
                if replaced_batch_id != meta.batch_id {
                    self.batch_ord_by_id.remove(&replaced_batch_id);
                }
                self.batches_by_ord[batch_ord] = meta;
            }
            std::cmp::Ordering::Equal => self.batches_by_ord.push(meta),
            std::cmp::Ordering::Greater => {
                panic!("non-dense batch_ord insertion: {}", meta.batch_ord)
            }
        }
    }

    pub fn insert_leaf_batch(&mut self, batch_id: BatchId) {
        if let Some(batch_ord) = self.batch_ord_by_id.get(&batch_id).copied() {
            self.leaf_batch_ords.insert(batch_ord);
        }
    }

    pub fn remove_leaf_batch(&mut self, batch_id: &BatchId) {
        if let Some(batch_ord) = self.batch_ord_by_id.get(batch_id).copied() {
            self.leaf_batch_ords.remove(&batch_ord);
        }
    }

    pub fn contains_leaf_batch(&self, batch_id: &BatchId) -> bool {
        self.batch_ord_by_id
            .get(batch_id)
            .map(|batch_ord| self.leaf_batch_ords.contains(batch_ord))
            .unwrap_or(false)
    }

    pub fn leaf_batch_ids(&self) -> impl Iterator<Item = BatchId> + '_ {
        self.leaf_batch_ords
            .iter()
            .filter_map(|batch_ord| self.batch_meta_by_ord(*batch_ord).map(|meta| meta.batch_id))
    }

    pub fn leaf_batch_count(&self) -> usize {
        self.leaf_batch_ords.len()
    }

    pub fn batch_metas(&self) -> impl Iterator<Item = &PrefixBatchMeta> {
        self.batches_by_ord.iter()
    }
}

/// An object with metadata and named branches.
#[derive(Debug, Clone)]
pub struct Object {
    pub id: ObjectId,
    pub metadata: HashMap<String, String>,
    pub branches: HashMap<BranchName, Branch>,
    pub commit_branches: HashMap<CommitId, BranchName>,
    pub prefix_batches: HashMap<String, PrefixBatchCatalog>,
}

impl Object {
    pub fn new(metadata: Option<HashMap<String, String>>) -> Self {
        Self {
            id: ObjectId::new(),
            metadata: metadata.unwrap_or_default(),
            branches: HashMap::new(),
            commit_branches: HashMap::new(),
            prefix_batches: HashMap::new(),
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
