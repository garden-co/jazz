use std::collections::HashMap;
use std::ops::Index;

use internment::Intern;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use smolset::SmolSet;
use uuid::Uuid;

use crate::commit::{Commit, CommitId};
use crate::query_manager::types::{
    BatchBranchKey, BatchId, BatchOrd, ComposedBranchName, QueryBranchRef,
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

#[derive(Debug, Clone, Default)]
struct PrefixBranchStore {
    branches_by_batch: HashMap<BatchId, Branch>,
}

/// Per-object branch storage organized by `(prefix, batch)` instead of full branch name.
#[derive(Debug, Clone, Default)]
pub struct ObjectBranches {
    branches_by_prefix: HashMap<BranchName, PrefixBranchStore>,
    branch_count: usize,
}

impl ObjectBranches {
    fn split_branch_name(branch_name: &BranchName) -> Option<(BranchName, BatchId)> {
        let composed = ComposedBranchName::parse(branch_name)?;
        Some((
            BranchName::new(composed.prefix().branch_prefix()),
            composed.batch_id,
        ))
    }

    pub fn is_empty(&self) -> bool {
        self.branch_count == 0
    }

    pub fn len(&self) -> usize {
        self.branch_count
    }

    pub fn contains_key(&self, branch_name: &BranchName) -> bool {
        self.get(branch_name).is_some()
    }

    pub fn get(&self, branch_name: &BranchName) -> Option<&Branch> {
        let (prefix_name, batch_id) = Self::split_branch_name(branch_name)?;
        self.branches_by_prefix
            .get(&prefix_name)?
            .branches_by_batch
            .get(&batch_id)
    }

    pub fn get_mut(&mut self, branch_name: &BranchName) -> Option<&mut Branch> {
        let (prefix_name, batch_id) = Self::split_branch_name(branch_name)?;
        self.branches_by_prefix
            .get_mut(&prefix_name)?
            .branches_by_batch
            .get_mut(&batch_id)
    }

    pub fn insert(&mut self, branch_name: BranchName, branch: Branch) -> Option<Branch> {
        let (prefix_name, batch_id) =
            Self::split_branch_name(&branch_name).expect("branch storage requires composed names");
        let prefix_store = self.branches_by_prefix.entry(prefix_name).or_default();
        let previous = prefix_store.branches_by_batch.insert(batch_id, branch);
        if previous.is_none() {
            self.branch_count += 1;
        }
        previous
    }

    pub fn get_or_insert_with(
        &mut self,
        branch_name: BranchName,
        default: impl FnOnce() -> Branch,
    ) -> &mut Branch {
        let (prefix_name, batch_id) =
            Self::split_branch_name(&branch_name).expect("branch storage requires composed names");
        let prefix_store = self.branches_by_prefix.entry(prefix_name).or_default();
        match prefix_store.branches_by_batch.entry(batch_id) {
            std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
            std::collections::hash_map::Entry::Vacant(entry) => {
                self.branch_count += 1;
                entry.insert(default())
            }
        }
    }

    pub fn values(&self) -> impl Iterator<Item = &Branch> {
        self.branches_by_prefix
            .values()
            .flat_map(|prefix_store| prefix_store.branches_by_batch.values())
    }

    pub fn iter(&self) -> impl Iterator<Item = (BranchName, &Branch)> + '_ {
        self.branches_by_prefix
            .iter()
            .flat_map(|(prefix_name, prefix_store)| {
                prefix_store
                    .branches_by_batch
                    .iter()
                    .map(move |(batch_id, branch)| {
                        (
                            QueryBranchRef::from_prefix_name_and_batch(*prefix_name, *batch_id)
                                .branch_name(),
                            branch,
                        )
                    })
            })
    }
}

impl Index<&BranchName> for ObjectBranches {
    type Output = Branch;

    fn index(&self, branch_name: &BranchName) -> &Self::Output {
        self.get(branch_name)
            .unwrap_or_else(|| panic!("branch not found: {branch_name}"))
    }
}

/// Persisted metadata for one batch under a shared branch prefix.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrefixBatchMeta {
    pub batch_id: BatchId,
    pub batch_ord: BatchOrd,
    pub root_commit_id: CommitId,
    pub head_commit_id: CommitId,
    pub first_timestamp: u64,
    pub last_timestamp: u64,
    pub parent_batch_ords: Vec<BatchOrd>,
    pub child_count: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct BatchOrdLookupEntry {
    batch_id: BatchId,
    batch_ord: BatchOrd,
}

/// In-memory per-prefix batch catalog.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct PrefixBatchCatalog {
    lookup_by_id: Vec<BatchOrdLookupEntry>,
    batches_by_ord: Vec<PrefixBatchMeta>,
    leaf_batch_ords: SmolSet<[BatchOrd; 4]>,
}

impl PrefixBatchCatalog {
    fn lookup_index(&self, batch_id: &BatchId) -> Result<usize, usize> {
        let key = *batch_id.as_bytes();
        self.lookup_by_id
            .binary_search_by_key(&key, |entry| *entry.batch_id.as_bytes())
    }

    fn remove_lookup(&mut self, batch_id: &BatchId) {
        if let Ok(index) = self.lookup_index(batch_id) {
            self.lookup_by_id.remove(index);
        }
    }

    fn upsert_lookup(&mut self, batch_id: BatchId, batch_ord: BatchOrd) {
        match self.lookup_index(&batch_id) {
            Ok(index) => self.lookup_by_id[index].batch_ord = batch_ord,
            Err(index) => self.lookup_by_id.insert(
                index,
                BatchOrdLookupEntry {
                    batch_id,
                    batch_ord,
                },
            ),
        }
    }

    pub fn next_batch_ord(&self) -> BatchOrd {
        BatchOrd(self.batches_by_ord.len() as u32)
    }

    pub fn batch_ord(&self, batch_id: &BatchId) -> Option<BatchOrd> {
        self.lookup_index(batch_id)
            .ok()
            .map(|index| self.lookup_by_id[index].batch_ord)
    }

    pub fn batch_meta(&self, batch_id: &BatchId) -> Option<&PrefixBatchMeta> {
        let batch_ord = self.batch_ord(batch_id)?;
        self.batch_meta_by_ord(batch_ord)
    }

    pub fn batch_meta_mut(&mut self, batch_id: &BatchId) -> Option<&mut PrefixBatchMeta> {
        let batch_ord = self.batch_ord(batch_id)?;
        self.batch_meta_by_ord_mut(batch_ord)
    }

    pub fn batch_meta_by_ord(&self, batch_ord: BatchOrd) -> Option<&PrefixBatchMeta> {
        self.batches_by_ord.get(batch_ord.as_usize())
    }

    pub fn batch_meta_by_ord_mut(&mut self, batch_ord: BatchOrd) -> Option<&mut PrefixBatchMeta> {
        self.batches_by_ord.get_mut(batch_ord.as_usize())
    }

    pub fn insert_batch_meta(&mut self, meta: PrefixBatchMeta) {
        let batch_id = meta.batch_id;
        let batch_ord_value = meta.batch_ord;
        let batch_ord = meta.batch_ord.as_usize();
        if let Some(existing_ord) = self.batch_ord(&batch_id) {
            debug_assert_eq!(
                existing_ord, batch_ord_value,
                "batch {} changed ord from {} to {}",
                batch_id, existing_ord.0, batch_ord_value.0
            );
        }
        match batch_ord.cmp(&self.batches_by_ord.len()) {
            std::cmp::Ordering::Less => {
                let replaced_batch_id = self.batches_by_ord[batch_ord].batch_id;
                if replaced_batch_id != batch_id {
                    self.remove_lookup(&replaced_batch_id);
                }
                self.batches_by_ord[batch_ord] = meta;
            }
            std::cmp::Ordering::Equal => self.batches_by_ord.push(meta),
            std::cmp::Ordering::Greater => {
                panic!("non-dense batch_ord insertion: {}", batch_ord_value.0)
            }
        }
        self.upsert_lookup(batch_id, batch_ord_value);
    }

    pub fn insert_leaf_batch_ord(&mut self, batch_ord: BatchOrd) {
        self.leaf_batch_ords.insert(batch_ord);
    }

    pub fn remove_leaf_batch_ord(&mut self, batch_ord: BatchOrd) {
        self.leaf_batch_ords.remove(&batch_ord);
    }

    pub fn contains_leaf_batch(&self, batch_id: &BatchId) -> bool {
        self.batch_ord(batch_id)
            .map(|batch_ord| self.leaf_batch_ords.contains(&batch_ord))
            .unwrap_or(false)
    }

    pub fn leaf_batch_ids(&self) -> impl Iterator<Item = BatchId> + '_ {
        self.leaf_batch_ords
            .iter()
            .filter_map(|batch_ord| self.batch_meta_by_ord(*batch_ord).map(|meta| meta.batch_id))
    }

    pub fn leaf_batch_ords(&self) -> impl Iterator<Item = BatchOrd> + '_ {
        self.leaf_batch_ords.iter().copied()
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
    pub branches: ObjectBranches,
    pub commit_branches: HashMap<CommitId, BatchBranchKey>,
    pub prefix_batches: HashMap<String, PrefixBatchCatalog>,
}

impl Object {
    pub fn new(metadata: Option<HashMap<String, String>>) -> Self {
        Self {
            id: ObjectId::new(),
            metadata: metadata.unwrap_or_default(),
            branches: ObjectBranches::default(),
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
