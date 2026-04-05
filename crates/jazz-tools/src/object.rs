use std::collections::HashMap;
use std::ops::Index;

use internment::Intern;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use smallvec::SmallVec;
use smolset::SmolSet;
use uuid::Uuid;

use crate::commit::{Commit, CommitId};
use crate::query_manager::types::{BatchBranchKey, BatchId, BatchOrd, QueryBranchRef};

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct BatchSlotLookupEntry {
    batch_id: BatchId,
    slot_index: u32,
}

#[derive(Debug, Clone)]
struct BatchBranchSlot {
    batch_id: BatchId,
    branch: Branch,
}

#[derive(Debug, Clone, Default)]
struct PrefixBranchStore {
    branch_slots: Vec<BatchBranchSlot>,
    lookup_by_id: Vec<BatchSlotLookupEntry>,
}

impl PrefixBranchStore {
    fn reserve_additional(&mut self, additional: usize) {
        self.branch_slots.reserve(additional);
        self.lookup_by_id.reserve(additional);
    }

    fn lookup_index(&self, batch_id: &BatchId) -> Result<usize, usize> {
        let key = *batch_id.as_bytes();
        self.lookup_by_id
            .binary_search_by_key(&key, |entry| *entry.batch_id.as_bytes())
    }

    fn slot_index(&self, batch_id: &BatchId) -> Option<usize> {
        self.lookup_index(batch_id)
            .ok()
            .map(|index| self.lookup_by_id[index].slot_index as usize)
    }

    fn get(&self, batch_id: &BatchId) -> Option<&Branch> {
        let slot_index = self.slot_index(batch_id)?;
        self.branch_slots.get(slot_index).map(|slot| &slot.branch)
    }

    fn get_mut(&mut self, batch_id: &BatchId) -> Option<&mut Branch> {
        let slot_index = self.slot_index(batch_id)?;
        self.branch_slots
            .get_mut(slot_index)
            .map(|slot| &mut slot.branch)
    }

    fn insert(&mut self, batch_id: BatchId, branch: Branch) -> Option<Branch> {
        if let Some(slot_index) = self.slot_index(&batch_id) {
            let slot = &mut self.branch_slots[slot_index];
            debug_assert_eq!(slot.batch_id, batch_id);
            return Some(std::mem::replace(&mut slot.branch, branch));
        }

        let slot_index = self.branch_slots.len() as u32;
        self.branch_slots.push(BatchBranchSlot { batch_id, branch });
        let insert_index = self
            .lookup_index(&batch_id)
            .expect_err("batch lookup should be absent before insert");
        self.lookup_by_id.insert(
            insert_index,
            BatchSlotLookupEntry {
                batch_id,
                slot_index,
            },
        );
        None
    }

    fn get_or_insert_with(
        &mut self,
        batch_id: BatchId,
        default: impl FnOnce() -> Branch,
    ) -> &mut Branch {
        if let Some(slot_index) = self.slot_index(&batch_id) {
            return &mut self.branch_slots[slot_index].branch;
        }

        let slot_index = self.branch_slots.len() as u32;
        self.branch_slots.push(BatchBranchSlot {
            batch_id,
            branch: default(),
        });
        let insert_index = self
            .lookup_index(&batch_id)
            .expect_err("batch lookup should be absent before insert");
        self.lookup_by_id.insert(
            insert_index,
            BatchSlotLookupEntry {
                batch_id,
                slot_index,
            },
        );
        &mut self.branch_slots[slot_index as usize].branch
    }

    fn values(&self) -> impl Iterator<Item = &Branch> {
        self.branch_slots.iter().map(|slot| &slot.branch)
    }

    fn iter(&self) -> impl Iterator<Item = (BatchId, &Branch)> {
        self.branch_slots
            .iter()
            .map(|slot| (slot.batch_id, &slot.branch))
    }
}

#[derive(Debug, Clone, Default)]
struct PrefixBranchState {
    branches: PrefixBranchStore,
    batch_catalog: Option<PrefixBatchCatalog>,
}

#[derive(Debug, Clone, Default, PartialEq)]
struct BatchOrdBitSet {
    words: Vec<u64>,
    len: usize,
}

impl BatchOrdBitSet {
    fn insert(&mut self, batch_ord: BatchOrd) {
        let index = batch_ord.as_usize();
        let word_index = index / u64::BITS as usize;
        let bit_index = index % u64::BITS as usize;
        if self.words.len() <= word_index {
            self.words.resize(word_index + 1, 0);
        }
        let mask = 1_u64 << bit_index;
        if self.words[word_index] & mask == 0 {
            self.words[word_index] |= mask;
            self.len += 1;
        }
    }

    fn remove(&mut self, batch_ord: BatchOrd) {
        let index = batch_ord.as_usize();
        let word_index = index / u64::BITS as usize;
        let bit_index = index % u64::BITS as usize;
        let Some(word) = self.words.get_mut(word_index) else {
            return;
        };
        let mask = 1_u64 << bit_index;
        if *word & mask == 0 {
            return;
        }

        *word &= !mask;
        self.len -= 1;
        while self.words.last().copied() == Some(0) {
            self.words.pop();
        }
    }

    fn contains(&self, batch_ord: BatchOrd) -> bool {
        let index = batch_ord.as_usize();
        let word_index = index / u64::BITS as usize;
        let bit_index = index % u64::BITS as usize;
        self.words
            .get(word_index)
            .map(|word| word & (1_u64 << bit_index) != 0)
            .unwrap_or(false)
    }

    fn len(&self) -> usize {
        self.len
    }

    fn iter(&self) -> BatchOrdBitSetIter<'_> {
        BatchOrdBitSetIter {
            words: &self.words,
            word_index: 0,
            current_word: self.words.first().copied().unwrap_or(0),
        }
    }
}

struct BatchOrdBitSetIter<'a> {
    words: &'a [u64],
    word_index: usize,
    current_word: u64,
}

impl Iterator for BatchOrdBitSetIter<'_> {
    type Item = BatchOrd;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.current_word != 0 {
                let bit_index = self.current_word.trailing_zeros() as usize;
                self.current_word &= self.current_word - 1;
                let ord = self.word_index * u64::BITS as usize + bit_index;
                return Some(BatchOrd(ord as u32));
            }

            self.word_index += 1;
            self.current_word = *self.words.get(self.word_index)?;
        }
    }
}

/// Per-object branch storage organized by `(prefix, batch)` instead of full branch name.
#[derive(Debug, Clone, Default)]
pub struct ObjectBranches {
    branches_by_prefix: HashMap<BranchName, PrefixBranchState>,
    branch_count: usize,
}

impl ObjectBranches {
    fn split_branch_name(branch_name: &BranchName) -> Option<(BranchName, BatchId)> {
        let (prefix_name, batch_segment) = branch_name.as_str().rsplit_once('-')?;
        Some((
            BranchName::new(prefix_name),
            BatchId::parse_segment(batch_segment)?,
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
        self.get_by_key(BatchBranchKey::from_prefix_name_and_batch(
            prefix_name,
            batch_id,
        ))
    }

    pub fn get_mut(&mut self, branch_name: &BranchName) -> Option<&mut Branch> {
        let (prefix_name, batch_id) = Self::split_branch_name(branch_name)?;
        self.get_mut_by_key(BatchBranchKey::from_prefix_name_and_batch(
            prefix_name,
            batch_id,
        ))
    }

    pub fn get_by_key(&self, branch_key: BatchBranchKey) -> Option<&Branch> {
        self.branches_by_prefix
            .get(&branch_key.prefix_name())?
            .branches
            .get(&branch_key.batch_id())
    }

    pub fn get_mut_by_key(&mut self, branch_key: BatchBranchKey) -> Option<&mut Branch> {
        self.branches_by_prefix
            .get_mut(&branch_key.prefix_name())?
            .branches
            .get_mut(&branch_key.batch_id())
    }

    pub fn reserve_prefix_additional(&mut self, prefix_name: BranchName, additional: usize) {
        self.branches_by_prefix
            .entry(prefix_name)
            .or_default()
            .branches
            .reserve_additional(additional);
    }

    pub fn insert(&mut self, branch_name: BranchName, branch: Branch) -> Option<Branch> {
        let (prefix_name, batch_id) =
            Self::split_branch_name(&branch_name).expect("branch storage requires composed names");
        self.insert_by_key(
            BatchBranchKey::from_prefix_name_and_batch(prefix_name, batch_id),
            branch,
        )
    }

    pub fn insert_by_key(&mut self, branch_key: BatchBranchKey, branch: Branch) -> Option<Branch> {
        let prefix_store = self
            .branches_by_prefix
            .entry(branch_key.prefix_name())
            .or_default();
        let previous = prefix_store.branches.insert(branch_key.batch_id(), branch);
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
        self.get_or_insert_with_key(
            BatchBranchKey::from_prefix_name_and_batch(prefix_name, batch_id),
            default,
        )
    }

    pub fn get_or_insert_with_key(
        &mut self,
        branch_key: BatchBranchKey,
        default: impl FnOnce() -> Branch,
    ) -> &mut Branch {
        let prefix_store = self
            .branches_by_prefix
            .entry(branch_key.prefix_name())
            .or_default();
        let was_present = prefix_store.branches.get(&branch_key.batch_id()).is_some();
        let branch = prefix_store
            .branches
            .get_or_insert_with(branch_key.batch_id(), default);
        if !was_present {
            self.branch_count += 1;
        }
        branch
    }

    pub fn values(&self) -> impl Iterator<Item = &Branch> {
        self.branches_by_prefix
            .values()
            .flat_map(|state| state.branches.values())
    }

    pub fn iter(&self) -> impl Iterator<Item = (BranchName, &Branch)> + '_ {
        self.branches_by_prefix
            .iter()
            .flat_map(|(prefix_name, prefix_state)| {
                prefix_state.branches.iter().map(move |(batch_id, branch)| {
                    (
                        QueryBranchRef::from_prefix_name_and_batch(*prefix_name, batch_id)
                            .branch_name(),
                        branch,
                    )
                })
            })
    }

    pub fn prefix_catalog(&self, prefix_name: &BranchName) -> Option<&PrefixBatchCatalog> {
        self.branches_by_prefix
            .get(prefix_name)?
            .batch_catalog
            .as_ref()
    }

    pub fn prefix_catalog_mut_or_default(
        &mut self,
        prefix_name: BranchName,
    ) -> &mut PrefixBatchCatalog {
        self.branches_by_prefix
            .entry(prefix_name)
            .or_default()
            .batch_catalog
            .get_or_insert_with(PrefixBatchCatalog::default)
    }

    pub fn set_prefix_catalog(&mut self, prefix_name: BranchName, catalog: PrefixBatchCatalog) {
        self.branches_by_prefix
            .entry(prefix_name)
            .or_default()
            .batch_catalog = Some(catalog);
    }

    pub fn ensure_prefix_catalog(&mut self, prefix_name: BranchName, catalog: PrefixBatchCatalog) {
        self.branches_by_prefix
            .entry(prefix_name)
            .or_default()
            .batch_catalog
            .get_or_insert(catalog);
    }

    pub fn prefix_catalogs(&self) -> impl Iterator<Item = (&BranchName, &PrefixBatchCatalog)> {
        self.branches_by_prefix
            .iter()
            .filter_map(|(prefix, state)| {
                state
                    .batch_catalog
                    .as_ref()
                    .map(|catalog| (prefix, catalog))
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
    pub parent_batch_ords: SmallVec<[BatchOrd; 4]>,
    pub child_count: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BatchOrdLookupEntry {
    batch_id: BatchId,
    batch_ord: BatchOrd,
}

/// In-memory per-prefix batch catalog.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct PrefixBatchCatalog {
    lookup_by_id: Vec<BatchOrdLookupEntry>,
    batches_by_ord: Vec<PrefixBatchMeta>,
    leaf_batch_ords: BatchOrdBitSet,
}

impl PrefixBatchCatalog {
    pub(crate) fn from_persisted_parts(
        batches_by_ord: Vec<PrefixBatchMeta>,
        leaf_batch_ords: impl IntoIterator<Item = BatchOrd>,
    ) -> Self {
        let mut lookup_by_id: Vec<BatchOrdLookupEntry> = batches_by_ord
            .iter()
            .map(|meta| BatchOrdLookupEntry {
                batch_id: meta.batch_id,
                batch_ord: meta.batch_ord,
            })
            .collect();
        lookup_by_id.sort_by_key(|entry| *entry.batch_id.as_bytes());

        let mut compact_leaf_batch_ords = BatchOrdBitSet::default();
        for batch_ord in leaf_batch_ords {
            if batches_by_ord.get(batch_ord.as_usize()).is_some() {
                compact_leaf_batch_ords.insert(batch_ord);
            }
        }

        Self {
            lookup_by_id,
            batches_by_ord,
            leaf_batch_ords: compact_leaf_batch_ords,
        }
    }

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
        self.leaf_batch_ords.remove(batch_ord);
    }

    pub fn contains_leaf_batch(&self, batch_id: &BatchId) -> bool {
        self.batch_ord(batch_id)
            .map(|batch_ord| self.leaf_batch_ords.contains(batch_ord))
            .unwrap_or(false)
    }

    pub fn leaf_batch_ids(&self) -> impl Iterator<Item = BatchId> + '_ {
        self.leaf_batch_ords
            .iter()
            .filter_map(|batch_ord| self.batch_meta_by_ord(batch_ord).map(|meta| meta.batch_id))
    }

    pub fn leaf_batch_ords(&self) -> impl Iterator<Item = BatchOrd> + '_ {
        self.leaf_batch_ords.iter()
    }

    pub fn leaf_batch_count(&self) -> usize {
        self.leaf_batch_ords.len()
    }

    pub fn batch_count(&self) -> usize {
        self.batches_by_ord.len()
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
}

impl Object {
    pub fn new(metadata: Option<HashMap<String, String>>) -> Self {
        Self {
            id: ObjectId::new(),
            metadata: metadata.unwrap_or_default(),
            branches: ObjectBranches::default(),
            commit_branches: HashMap::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_manager::types::{BranchPrefixName, SchemaHash};

    #[test]
    fn object_id_generates_unique_values() {
        let id1 = ObjectId::new();
        let id2 = ObjectId::new();
        assert_ne!(id1, id2);
    }

    #[test]
    fn batch_ord_bitset_tracks_sparse_ordinals() {
        let mut bitset = BatchOrdBitSet::default();
        bitset.insert(BatchOrd(0));
        bitset.insert(BatchOrd(65));
        bitset.insert(BatchOrd(130));
        bitset.insert(BatchOrd(65));

        assert_eq!(bitset.len(), 3);
        assert!(bitset.contains(BatchOrd(0)));
        assert!(bitset.contains(BatchOrd(65)));
        assert!(bitset.contains(BatchOrd(130)));
        assert_eq!(
            bitset.iter().collect::<Vec<_>>(),
            vec![BatchOrd(0), BatchOrd(65), BatchOrd(130)]
        );

        bitset.remove(BatchOrd(65));
        assert_eq!(bitset.len(), 2);
        assert!(!bitset.contains(BatchOrd(65)));
        assert_eq!(
            bitset.iter().collect::<Vec<_>>(),
            vec![BatchOrd(0), BatchOrd(130)]
        );
    }

    #[test]
    fn object_branches_store_and_iter_by_composed_batch_key() {
        let prefix = BranchPrefixName::new("dev", SchemaHash::from_bytes([7; 32]), "main");
        let batch_a = BatchId::from_uuid(Uuid::now_v7());
        let batch_b = BatchId::from_uuid(Uuid::now_v7());
        let branch_a = QueryBranchRef::from_prefix_and_batch(&prefix, batch_a).branch_name();
        let branch_b = QueryBranchRef::from_prefix_and_batch(&prefix, batch_b).branch_name();

        let mut branches = ObjectBranches::default();
        branches.insert(branch_a, Branch::default());
        branches.insert(branch_b, Branch::default());

        assert_eq!(branches.len(), 2);
        assert!(branches.contains_key(&branch_a));
        assert!(branches.contains_key(&branch_b));

        let iterated: Vec<_> = branches.iter().map(|(name, _)| name).collect();
        assert_eq!(iterated.len(), 2);
        assert!(iterated.contains(&branch_a));
        assert!(iterated.contains(&branch_b));

        let replacement = Branch {
            loaded_state: BranchLoadedState::TipsOnly,
            ..Branch::default()
        };
        let previous = branches.insert(branch_a, replacement).unwrap();
        assert_eq!(previous.loaded_state, BranchLoadedState::NotLoaded);
        assert_eq!(
            branches.get(&branch_a).unwrap().loaded_state,
            BranchLoadedState::TipsOnly
        );
        assert_eq!(branches.len(), 2);
    }

    #[test]
    fn object_branches_keep_prefix_catalog_optional_per_prefix() {
        let prefix = BranchPrefixName::new("dev", SchemaHash::from_bytes([9; 32]), "main");
        let prefix_name = BranchName::new(prefix.branch_prefix());
        let batch = BatchId::from_uuid(Uuid::from_bytes([1; 16]));
        let branch_name = QueryBranchRef::from_prefix_and_batch(&prefix, batch).branch_name();

        let mut branches = ObjectBranches::default();
        branches.insert(branch_name, Branch::default());

        assert!(branches.prefix_catalog(&prefix_name).is_none());

        let catalog = PrefixBatchCatalog::from_persisted_parts(
            vec![PrefixBatchMeta {
                batch_id: batch,
                batch_ord: BatchOrd(0),
                root_commit_id: CommitId([3; 32]),
                head_commit_id: CommitId([4; 32]),
                first_timestamp: 11,
                last_timestamp: 13,
                parent_batch_ords: SmallVec::new(),
                child_count: 0,
            }],
            [BatchOrd(0)],
        );
        branches.set_prefix_catalog(prefix_name, catalog.clone());

        assert_eq!(branches.prefix_catalog(&prefix_name), Some(&catalog));
    }
}
