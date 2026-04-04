use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap, HashSet};
use web_time::{SystemTime, UNIX_EPOCH};

use smallvec::smallvec;
use smolset::SmolSet;

use crate::commit::{Commit, CommitId, StoredState};
use crate::object::{
    Branch, BranchLoadedState, BranchName, Object, ObjectBranches, ObjectId, PrefixBatchCatalog,
    PrefixBatchMeta,
};
use crate::query_manager::types::{
    BatchBranchKey, BranchPrefixName, ComposedBranchName, QueryBranchRef,
};
use crate::storage::{LoadedBranch, LoadedBranchTips, PrefixBatchUpdate, Storage, StorageError};

/// Unique identifier for a subscription.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SubscriptionId(pub u64);

/// Unique identifier for a global (all-objects) subscription.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AllObjectsSubscriptionId(pub u64);

/// Internal tracking of a subscription.
#[derive(Debug, Clone)]
struct Subscription {
    object_id: ObjectId,
    branch_name: BranchName,
    normalized_branch_key: BatchBranchKey,
}

/// Update sent to subscribers when commits are added or loaded.
///
/// Contains the current branch head(s) sorted by timestamp (oldest first).
/// Branches are intended to stay internally linear; multiple ids here indicate
/// older data or out-of-order remote history that has not been compacted yet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubscriptionUpdate {
    pub subscription_id: SubscriptionId,
    pub object_id: ObjectId,
    pub branch_name: BranchName,
    /// Current frontier commit IDs, sorted by timestamp (oldest first).
    pub commit_ids: Vec<CommitId>,
}

/// Update sent to global (all-objects) subscribers when any object changes.
///
/// Fires on: create(), receive_object(), add_commit(), receive_commit()
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AllObjectUpdate {
    pub object_id: ObjectId,
    pub metadata: HashMap<String, String>,
    pub branch_name: BranchName,
    /// Current branch head commit IDs, sorted by timestamp.
    pub commit_ids: Vec<CommitId>,
    /// True if this is a newly created/received object, false if existing object.
    pub is_new_object: bool,
    /// Previous tip commit IDs before this update (empty for new objects).
    pub previous_commit_ids: Vec<CommitId>,
    /// Content of previous "winning" tip (newest by timestamp). None if new object.
    /// Used by QueryManager to compute index deltas for synced updates.
    pub old_content: Option<Vec<u8>>,
}

/// Errors that can occur when managing objects and commits.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    ObjectNotFound(ObjectId),
    BranchNotFound(BranchName),
    InvalidBranchName(BranchName),
    ParentNotFound(CommitId),
    /// Storage operation failed.
    StorageError(StorageError),
}

/// Result of a branch truncation operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TruncateResult {
    Success { deleted_commits: usize },
    PermanentError(TruncateError),
}

/// Errors specific to branch truncation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TruncateError {
    ObjectNotFound(ObjectId),
    BranchNotFound(BranchName),
    InvalidBranchName(BranchName),
    TailNotFound(CommitId),
    /// Can't truncate past the frontier - tip is not a descendant of any tail.
    TipBeforeTail(CommitId),
}

#[derive(Debug, Clone)]
struct PendingBatchCatalogUpdate {
    prefix: String,
    storage_update: PrefixBatchUpdate,
    resolved_parent_branches: Vec<(CommitId, BatchBranchKey)>,
}

/// Manages a collection of objects.
///
/// With sync storage (Phase 2), objects are stored directly in the HashMap -
/// no ObjectState enum, no Loading state, no async request/response cycle.
#[derive(Debug, Clone, Default)]
pub struct ObjectManager {
    pub objects: HashMap<ObjectId, Object>,
    next_subscription_id: u64,
    subscriptions: HashMap<SubscriptionId, Subscription>,
    /// Index: (ObjectId, BatchBranchKey) → set of SubscriptionIds
    branch_subscribers: HashMap<(ObjectId, BatchBranchKey), HashSet<SubscriptionId>>,
    pub subscription_outbox: Vec<SubscriptionUpdate>,
    /// Global (all-objects) subscriptions.
    all_object_subscriptions: HashSet<AllObjectsSubscriptionId>,
    next_all_objects_subscription_id: u64,
    /// Outbox for global subscription updates.
    pub all_objects_outbox: Vec<AllObjectUpdate>,
    /// Last timestamp used, for monotonic ordering.
    last_timestamp: u64,
}

impl ObjectManager {
    fn normalize_branch_name(branch_name: BranchName) -> Result<BranchName, Error> {
        BatchBranchKey::try_from_branch_name(branch_name)
            .map(|_| branch_name)
            .ok_or(Error::InvalidBranchName(branch_name))
    }

    fn normalize_branch_name_for_truncate(
        branch_name: BranchName,
    ) -> Result<BranchName, TruncateError> {
        Self::normalize_branch_name(branch_name).map_err(|error| match error {
            Error::InvalidBranchName(branch_name) => TruncateError::InvalidBranchName(branch_name),
            Error::BranchNotFound(branch_name) => TruncateError::BranchNotFound(branch_name),
            Error::ObjectNotFound(object_id) => TruncateError::ObjectNotFound(object_id),
            Error::ParentNotFound(_) | Error::StorageError(_) => unreachable!(),
        })
    }

    fn normalize_loaded_branch_name(branch_name: BranchName) -> Option<BranchName> {
        BatchBranchKey::try_from_branch_name(branch_name).map(|_| branch_name)
    }

    pub fn new() -> Self {
        Self::default()
    }

    fn branch_from_loaded(loaded: LoadedBranch) -> Branch {
        let mut commits = HashMap::new();
        let mut all_ids: HashSet<CommitId> = HashSet::new();
        let mut parent_ids: HashSet<CommitId> = HashSet::new();

        for commit in loaded.commits {
            let commit_id = commit.id();
            all_ids.insert(commit_id);
            for parent in &commit.parents {
                parent_ids.insert(*parent);
            }
            commits.insert(commit_id, commit);
        }

        let mut tips: SmolSet<[CommitId; 2]> = SmolSet::new();
        for commit_id in &all_ids {
            if !parent_ids.contains(commit_id) {
                tips.insert(*commit_id);
            }
        }

        Branch {
            commits,
            tips,
            tails: if loaded.tails.is_empty() {
                None
            } else {
                Some(loaded.tails.into_iter().collect())
            },
            loaded_state: BranchLoadedState::AllCommits,
        }
    }

    fn branch_from_loaded_tips(loaded: LoadedBranchTips) -> Branch {
        let mut commits = HashMap::new();
        let mut tips: SmolSet<[CommitId; 2]> = SmolSet::new();

        for commit in loaded.tips {
            let commit_id = commit.id();
            tips.insert(commit_id);
            commits.insert(commit_id, commit);
        }

        Branch {
            commits,
            tips,
            tails: None,
            loaded_state: BranchLoadedState::TipsOnly,
        }
    }

    fn newest_tip_commit(branch: &Branch) -> Option<(CommitId, &Commit)> {
        branch
            .tips
            .iter()
            .filter_map(|tip_id| branch.commits.get(tip_id).map(|commit| (*tip_id, commit)))
            .max_by_key(|(tip_id, commit)| (commit.timestamp, *tip_id))
    }

    /// Get next monotonic timestamp (microseconds since epoch).
    /// Guarantees each call returns a value greater than the previous.
    fn next_timestamp(&mut self) -> u64 {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time went backwards")
            .as_micros() as u64;

        self.last_timestamp = if now > self.last_timestamp {
            now
        } else {
            self.last_timestamp + 1
        };

        self.last_timestamp
    }

    pub fn reserve_timestamp(&mut self) -> u64 {
        self.next_timestamp()
    }

    /// Create a new object with optional metadata, returning its id.
    /// Persists to storage via Storage synchronously.
    pub fn create<H: Storage>(
        &mut self,
        io: &mut H,
        metadata: Option<HashMap<String, String>>,
    ) -> ObjectId {
        let _span = tracing::debug_span!("OM::create").entered();
        let object = Object::new(metadata.clone());
        let id = object.id;

        // Sync storage - returns immediately
        let _ = io.create_object(id, metadata.clone().unwrap_or_default());

        self.objects.insert(id, object);
        tracing::debug!(%id, "created object");
        id
    }

    /// Create an object with a specific ObjectId (for deterministic IDs).
    ///
    /// Unlike `create`, this uses the provided ObjectId rather than generating a new one.
    /// Used for index root nodes that have deterministic IDs based on table/column name.
    /// Persists to storage via Storage synchronously.
    pub fn create_with_id<H: Storage>(
        &mut self,
        io: &mut H,
        id: ObjectId,
        metadata: Option<HashMap<String, String>>,
    ) -> ObjectId {
        let object = Object {
            id,
            metadata: metadata.clone().unwrap_or_default(),
            branches: ObjectBranches::default(),
            commit_branches: HashMap::new(),
            prefix_batches: HashMap::new(),
        };

        // Sync storage - returns immediately
        let _ = io.create_object(id, metadata.unwrap_or_default());

        self.objects.insert(id, object);
        id
    }

    /// Get an object by id.
    pub fn get(&self, id: ObjectId) -> Option<&Object> {
        self.objects.get(&id)
    }

    fn ensure_object_cached(&mut self, id: ObjectId, storage: &dyn Storage) -> Option<()> {
        if self.objects.contains_key(&id) {
            return Some(());
        }

        let metadata = match storage.load_object_metadata(id) {
            Ok(Some(m)) => m,
            Ok(None) => {
                tracing::trace!(%id, "get_or_load: no metadata in storage");
                return None;
            }
            Err(e) => {
                tracing::warn!(%id, error = ?e, "get_or_load: storage error");
                return None;
            }
        };

        self.objects.insert(
            id,
            Object {
                id,
                metadata,
                branches: ObjectBranches::default(),
                commit_branches: HashMap::new(),
                prefix_batches: HashMap::new(),
            },
        );
        Some(())
    }

    fn get_or_load_with_mode_keys(
        &mut self,
        id: ObjectId,
        storage: &dyn Storage,
        branch_keys: &[BatchBranchKey],
        full_history: bool,
    ) -> Option<&Object> {
        let _span = tracing::trace_span!("OM::get_or_load", %id, full_history).entered();
        self.ensure_object_cached(id, storage)?;

        let object = self.objects.get_mut(&id)?;
        for branch_key in branch_keys {
            let needs_load = match object
                .branches
                .get_by_key(*branch_key)
                .map(|branch| branch.loaded_state)
            {
                Some(BranchLoadedState::AllCommits) => false,
                Some(BranchLoadedState::TipsOnly) => full_history,
                Some(BranchLoadedState::NotLoaded) => true,
                None => true,
            };
            if !needs_load {
                continue;
            }
            let branch_ref = branch_key.as_query_branch_ref();

            if full_history {
                match storage.load_branch(id, &branch_ref) {
                    Ok(Some(loaded)) => {
                        let branch = Self::branch_from_loaded(loaded);
                        let commit_ids_for_branch: Vec<_> =
                            branch.commits.keys().copied().collect();
                        object.branches.insert_by_key(*branch_key, branch);
                        for commit_id in commit_ids_for_branch {
                            object.commit_branches.insert(commit_id, *branch_key);
                        }
                    }
                    Ok(None) => {}
                    Err(err) => {
                        tracing::warn!(
                            %id,
                            branch = %branch_ref,
                            error = ?err,
                            "get_or_load: failed to hydrate requested branch"
                        );
                    }
                }
            } else {
                match storage.load_branch_tips(id, &branch_ref) {
                    Ok(Some(loaded)) => {
                        let branch = Self::branch_from_loaded_tips(loaded);
                        let commit_ids_for_branch: Vec<_> =
                            branch.commits.keys().copied().collect();
                        object.branches.insert_by_key(*branch_key, branch);
                        for commit_id in commit_ids_for_branch {
                            object.commit_branches.insert(commit_id, *branch_key);
                        }
                    }
                    Ok(None) => {}
                    Err(err) => {
                        tracing::warn!(
                            %id,
                            branch = %branch_ref,
                            error = ?err,
                            "get_or_load: failed to hydrate requested branch"
                        );
                    }
                }
            }
        }

        let object = self.objects.get(&id)?;
        let branch_count = object.branches.len();
        let commit_count: usize = object.branches.values().map(|b| b.commits.len()).sum();
        tracing::trace!(%id, branch_count, commit_count, "get_or_load: loaded from storage");
        Some(object)
    }

    fn get_or_load_with_mode(
        &mut self,
        id: ObjectId,
        storage: &dyn Storage,
        branches: &[String],
        full_history: bool,
    ) -> Option<&Object> {
        let mut branch_keys = Vec::with_capacity(branches.len());
        for branch_name in branches {
            let Some(bn) = Self::normalize_loaded_branch_name(BranchName::new(branch_name)) else {
                tracing::warn!(%id, branch = %branch_name, "skipping invalid branch name");
                continue;
            };
            branch_keys.push(BatchBranchKey::from_branch_name(bn));
        }
        self.get_or_load_with_mode_keys(id, storage, &branch_keys, full_history)
    }

    /// Get an object, loading full branch history from storage if needed.
    pub fn get_or_load(
        &mut self,
        id: ObjectId,
        storage: &dyn Storage,
        branches: &[String],
    ) -> Option<&Object> {
        self.get_or_load_with_mode(id, storage, branches, true)
    }

    /// Get an object, loading only branch tip commits from storage if needed.
    pub fn get_or_load_tips(
        &mut self,
        id: ObjectId,
        storage: &dyn Storage,
        branches: &[String],
    ) -> Option<&Object> {
        self.get_or_load_with_mode(id, storage, branches, false)
    }

    fn get_or_load_keys(
        &mut self,
        id: ObjectId,
        storage: &dyn Storage,
        branch_keys: &[BatchBranchKey],
    ) -> Option<&Object> {
        self.get_or_load_with_mode_keys(id, storage, branch_keys, true)
    }

    fn get_or_load_tips_keys(
        &mut self,
        id: ObjectId,
        storage: &dyn Storage,
        branch_keys: &[BatchBranchKey],
    ) -> Option<&Object> {
        self.get_or_load_with_mode_keys(id, storage, branch_keys, false)
    }

    /// Get mutable object by id.
    fn get_mut(&mut self, id: ObjectId) -> Option<&mut Object> {
        self.objects.get_mut(&id)
    }

    fn resolve_commit_branch_key<H: Storage + ?Sized>(
        object: &Object,
        io: &H,
        object_id: ObjectId,
        commit_id: CommitId,
    ) -> Result<Option<BatchBranchKey>, Error> {
        if let Some(branch_key) = object.commit_branches.get(&commit_id) {
            return Ok(Some(*branch_key));
        }

        io.load_commit_branch(object_id, commit_id)
            .map(|branch| branch.map(|branch| branch.batch_branch_key()))
            .map_err(Error::StorageError)
    }

    fn load_prefix_batch_catalog<'a, H: Storage + ?Sized>(
        object: &'a Object,
        io: &H,
        object_id: ObjectId,
        prefix: &str,
    ) -> Result<Cow<'a, PrefixBatchCatalog>, Error> {
        if let Some(catalog) = object.prefix_batches.get(prefix) {
            Ok(Cow::Borrowed(catalog))
        } else {
            Ok(Cow::Owned(
                io.load_prefix_batch_catalog(object_id, prefix)
                    .map_err(Error::StorageError)?
                    .unwrap_or_default(),
            ))
        }
    }

    fn apply_prefix_batch_update(catalog: &mut PrefixBatchCatalog, update: &PrefixBatchUpdate) {
        for parent_batch_ord in &update.increment_parent_child_counts {
            if let Some(parent_meta) = catalog.batch_meta_by_ord_mut(*parent_batch_ord) {
                parent_meta.child_count = parent_meta.child_count.saturating_add(1);
            }
        }
        for removed_batch_ord in &update.remove_leaf_batch_ords {
            catalog.remove_leaf_batch_ord(*removed_batch_ord);
        }
        catalog.insert_batch_meta(update.batch_meta.clone());
        catalog.insert_leaf_batch_ord(update.batch_meta.batch_ord);
    }

    fn plan_prefix_batch_update<H: Storage + ?Sized>(
        object: &Object,
        io: &H,
        object_id: ObjectId,
        branch_key: BatchBranchKey,
        commit: &Commit,
    ) -> Result<PendingBatchCatalogUpdate, Error> {
        let prefix_name = branch_key.prefix_name();
        let prefix = prefix_name.as_str().to_string();
        let loaded_catalog = Self::load_prefix_batch_catalog(object, io, object_id, &prefix)?;
        let existing_meta = loaded_catalog.batch_meta(&branch_key.batch_id()).cloned();
        let mut resolved_parent_branches = Vec::new();
        let mut parent_batch_ords = Vec::new();

        for parent in &commit.parents {
            let Some(parent_branch_key) =
                Self::resolve_commit_branch_key(object, io, object_id, *parent)?
            else {
                continue;
            };
            resolved_parent_branches.push((*parent, parent_branch_key));

            if parent_branch_key.prefix_name() == prefix_name
                && let Some(parent_batch_ord) =
                    loaded_catalog.batch_ord(&parent_branch_key.batch_id())
            {
                parent_batch_ords.push(parent_batch_ord);
            }
        }

        let (batch_meta, remove_leaf_batch_ords, increment_parent_child_counts) =
            if let Some(mut batch_meta) = existing_meta {
                batch_meta.head_commit_id = commit.id();
                batch_meta.last_timestamp = commit.timestamp;
                (batch_meta, SmolSet::new(), Vec::new())
            } else {
                let batch_meta = PrefixBatchMeta {
                    batch_id: branch_key.batch_id(),
                    batch_ord: loaded_catalog.next_batch_ord(),
                    root_commit_id: commit.id(),
                    head_commit_id: commit.id(),
                    first_timestamp: commit.timestamp,
                    last_timestamp: commit.timestamp,
                    parent_batch_ords: parent_batch_ords.clone(),
                    child_count: 0,
                };
                (
                    batch_meta,
                    parent_batch_ords.iter().copied().collect(),
                    parent_batch_ords,
                )
            };

        Ok(PendingBatchCatalogUpdate {
            prefix,
            storage_update: PrefixBatchUpdate {
                prefix: prefix_name.as_str().to_string(),
                batch_meta,
                remove_leaf_batch_ords,
                increment_parent_child_counts,
            },
            resolved_parent_branches,
        })
    }

    fn ensure_branch_ready_for_add_commit<H: Storage>(
        &mut self,
        io: &H,
        object_id: ObjectId,
        branch_key: BatchBranchKey,
        parents: &[CommitId],
    ) -> Result<(), Error> {
        if let Some(object) = self.get(object_id) {
            match object
                .branches
                .get_by_key(branch_key)
                .map(|branch| branch.loaded_state)
            {
                Some(BranchLoadedState::AllCommits) => return Ok(()),
                None if parents.is_empty() => return Ok(()),
                None => {
                    let has_same_branch_parent =
                        parents.iter().try_fold(false, |found, parent| {
                            if found {
                                return Ok(true);
                            }
                            Ok(
                                Self::resolve_commit_branch_key(object, io, object_id, *parent)?
                                    .is_some_and(|parent_branch_key| {
                                        parent_branch_key == branch_key
                                    }),
                            )
                        })?;
                    if !has_same_branch_parent {
                        return Ok(());
                    }
                }
                Some(BranchLoadedState::TipsOnly) | Some(BranchLoadedState::NotLoaded) => {}
            }
        }

        let requested_branches = [branch_key];
        let _ = self.get_or_load_keys(object_id, io, &requested_branches);
        self.get(object_id)
            .map(|_| ())
            .ok_or(Error::ObjectNotFound(object_id))
    }

    fn ensure_branch_ready_for_received_commit<H: Storage>(
        &mut self,
        io: &H,
        object_id: ObjectId,
        branch_key: BatchBranchKey,
        commit_id: CommitId,
        parents: &[CommitId],
    ) -> Result<(), Error> {
        if let Some(object) = self.get(object_id) {
            match object
                .branches
                .get_by_key(branch_key)
                .map(|branch| branch.loaded_state)
            {
                Some(BranchLoadedState::AllCommits) => return Ok(()),
                None => {
                    if io
                        .load_commit_branch(object_id, commit_id)
                        .map_err(Error::StorageError)?
                        .is_none()
                    {
                        let has_same_branch_parent =
                            parents.iter().try_fold(false, |found, parent| {
                                if found {
                                    return Ok(true);
                                }
                                Ok(
                                    Self::resolve_commit_branch_key(
                                        object, io, object_id, *parent,
                                    )?
                                    .is_some_and(
                                        |parent_branch_key| parent_branch_key == branch_key,
                                    ),
                                )
                            })?;
                        if !has_same_branch_parent {
                            return Ok(());
                        }
                    }
                }
                Some(BranchLoadedState::TipsOnly) | Some(BranchLoadedState::NotLoaded) => {}
            }
        }

        let requested_branches = [branch_key];
        let _ = self.get_or_load_keys(object_id, io, &requested_branches);
        self.get(object_id)
            .map(|_| ())
            .ok_or(Error::ObjectNotFound(object_id))
    }

    /// Get a mutable reference to a specific commit.
    pub fn get_commit_mut(
        &mut self,
        object_id: ObjectId,
        branch_name: &BranchName,
        commit_id: CommitId,
    ) -> Option<&mut Commit> {
        let branch_name = Self::normalize_loaded_branch_name(*branch_name)?;
        self.objects
            .get_mut(&object_id)?
            .branches
            .get_mut(&branch_name)?
            .commits
            .get_mut(&commit_id)
    }

    /// Add a commit to an object's branch.
    ///
    /// - Creates the branch automatically if parents is empty
    /// - Rejects if object doesn't exist
    /// - Rejects if parents are specified but branch doesn't exist
    /// - Rejects if any parent doesn't exist in the branch
    /// - Updates tips: removes parents from tips, adds new commit as tip
    /// - Persists to storage via Storage synchronously
    #[allow(clippy::too_many_arguments)]
    pub fn add_commit<H: Storage, A: ToString>(
        &mut self,
        io: &mut H,
        object_id: ObjectId,
        branch_name: impl Into<BranchName>,
        parents: Vec<CommitId>,
        content: Vec<u8>,
        author: A,
        metadata: Option<BTreeMap<String, String>>,
    ) -> Result<CommitId, Error> {
        let timestamp = self.next_timestamp();
        self.add_commit_with_timestamp(
            io,
            object_id,
            branch_name,
            parents,
            content,
            timestamp,
            author,
            metadata,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn add_commit_with_timestamp<H: Storage, A: ToString>(
        &mut self,
        io: &mut H,
        object_id: ObjectId,
        branch_name: impl Into<BranchName>,
        parents: Vec<CommitId>,
        content: Vec<u8>,
        timestamp: u64,
        author: A,
        metadata: Option<BTreeMap<String, String>>,
    ) -> Result<CommitId, Error> {
        let branch_name = Self::normalize_branch_name(branch_name.into())?;
        let branch_key = BatchBranchKey::from_branch_name(branch_name);
        let _span = tracing::debug_span!("OM::add_commit", %object_id, %branch_name).entered();
        self.ensure_branch_ready_for_add_commit(io, object_id, branch_key, &parents)?;

        // Capture previous state BEFORE mutation for AllObjectUpdate and validate
        // parent visibility before we mutate storage/memory.
        let (previous_commit_ids, old_content) = {
            let object = self
                .get(object_id)
                .ok_or(Error::ObjectNotFound(object_id))?;
            let branch_exists = object.branches.contains_key(&branch_name);

            if !parents.is_empty() {
                if branch_exists {
                    let branch = object
                        .branches
                        .get(&branch_name)
                        .ok_or(Error::BranchNotFound(branch_name))?;

                    for parent in &parents {
                        if !branch.commits.contains_key(parent) {
                            return Err(Error::ParentNotFound(*parent));
                        }
                    }

                    for parent in &parents {
                        if let Some(tails) = &branch.tails
                            && !Self::is_descendant_of_any(&branch.commits, *parent, tails)
                        {
                            return Err(Error::ParentNotFound(*parent));
                        }
                    }
                } else {
                    for parent in &parents {
                        if Self::resolve_commit_branch_key(object, io, object_id, *parent)?
                            .is_none()
                        {
                            return Err(Error::ParentNotFound(*parent));
                        }
                    }
                }
            }

            // Capture previous tips and "winning" tip content before mutation
            if let Some(branch) = object.branches.get(&branch_name) {
                let prev_tips = Self::tips_by_timestamp(&branch.commits, &branch.tips);
                // Last tip in sorted order is the "winner" (newest by timestamp)
                let old_content = prev_tips
                    .last()
                    .and_then(|tip_id| branch.commits.get(tip_id))
                    .map(|commit| commit.content.clone());
                (prev_tips, old_content)
            } else {
                // New branch - no previous state
                (vec![], None)
            }
        };

        let mut commit = Commit {
            parents: parents.clone().into(),
            content,
            timestamp,
            author: author.to_string(),
            metadata,
            stored_state: StoredState::Pending,
            ack_state: Default::default(),
        };

        let commit_id = commit.id();

        let pending_batch_update = {
            let object = self
                .get(object_id)
                .ok_or(Error::ObjectNotFound(object_id))?;
            Some(Self::plan_prefix_batch_update(
                object, io, object_id, branch_key, &commit,
            )?)
        };

        // Sync storage - returns immediately
        if io
            .append_commit(
                object_id,
                &QueryBranchRef::from_batch_branch_key(branch_key),
                commit.clone(),
                pending_batch_update
                    .as_ref()
                    .map(|update| update.storage_update.clone()),
            )
            .is_ok()
        {
            commit.stored_state = StoredState::Stored;
        }

        // Now mutably borrow and update
        let object = self
            .get_mut(object_id)
            .expect("object existence already validated");

        if let Some(update) = &pending_batch_update {
            let catalog = object
                .prefix_batches
                .entry(update.prefix.clone())
                .or_default();
            Self::apply_prefix_batch_update(catalog, &update.storage_update);
            for (parent_commit_id, parent_branch_key) in &update.resolved_parent_branches {
                object
                    .commit_branches
                    .entry(*parent_commit_id)
                    .or_insert(*parent_branch_key);
            }
        }

        // Create branch if it doesn't exist (only valid for parentless commits
        // or a new batch root merge).
        let branch = object.branches.get_or_insert_with(branch_name, || Branch {
            loaded_state: BranchLoadedState::AllCommits,
            ..Default::default()
        });

        // Update tips: remove parents, add new commit
        for parent in &parents {
            branch.tips.remove(parent);
        }
        branch.tips.insert(commit_id);

        branch.commits.insert(commit_id, commit);
        object.commit_branches.insert(commit_id, branch_key);

        tracing::trace!(?commit_id, "commit applied");

        // Notify subscribers of updated frontier
        self.notify_subscribers_of_commit(object_id, branch_name);

        // Notify global subscribers - with sync storage, objects are never "new/creating"
        // for the purpose of this notification (they're immediately persisted)
        self.notify_all_object_subscribers(
            object_id,
            branch_name,
            false, // is_new - always false with sync storage
            previous_commit_ids,
            old_content,
        );

        Ok(commit_id)
    }

    /// Replace all content on a branch with a single new commit, discarding history.
    ///
    /// This is useful for indices and other derived data that don't need history.
    /// Unlike `add_commit`, this method:
    /// - Removes all existing commits from memory immediately
    /// - Creates a new commit with no parents
    /// - Does NOT call Storage (caller should handle persistence if needed)
    ///
    /// Returns the new commit ID.
    pub fn replace_content<A: ToString>(
        &mut self,
        object_id: ObjectId,
        branch_name: impl Into<BranchName>,
        content: Vec<u8>,
        author: A,
    ) -> Result<CommitId, Error> {
        let branch_name = Self::normalize_branch_name(branch_name.into())?;

        let object = self
            .get_mut(object_id)
            .ok_or(Error::ObjectNotFound(object_id))?;

        let branch = object
            .branches
            .get_mut(&branch_name)
            .ok_or(Error::BranchNotFound(branch_name))?;

        // Clear all existing commits and tips
        branch.commits.clear();
        branch.tips.clear();
        branch.tails = None;

        // Create new commit with no parents
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;

        let commit = Commit {
            parents: smallvec![],
            content,
            timestamp,
            author: author.to_string(),
            metadata: None,
            stored_state: StoredState::Pending,
            ack_state: Default::default(),
        };

        let commit_id = commit.id();
        branch.tips.insert(commit_id);
        branch.commits.insert(commit_id, commit);

        Ok(commit_id)
    }

    /// Get tip IDs for a branch.
    pub fn get_tip_ids(
        &self,
        object_id: ObjectId,
        branch_name: impl Into<BranchName>,
    ) -> Result<&SmolSet<[CommitId; 2]>, Error> {
        let branch_name = Self::normalize_branch_name(branch_name.into())?;

        let object = self
            .get(object_id)
            .ok_or(Error::ObjectNotFound(object_id))?;

        let branch = object
            .branches
            .get(&branch_name)
            .ok_or(Error::BranchNotFound(branch_name))?;

        Ok(&branch.tips)
    }

    /// Get the tips (frontier commits) as full Commit structs for a branch.
    pub fn get_tips(
        &self,
        object_id: ObjectId,
        branch_name: impl Into<BranchName>,
    ) -> Result<HashMap<CommitId, &Commit>, Error> {
        let branch_name = Self::normalize_branch_name(branch_name.into())?;

        let object = self
            .get(object_id)
            .ok_or(Error::ObjectNotFound(object_id))?;

        let branch = object
            .branches
            .get(&branch_name)
            .ok_or(Error::BranchNotFound(branch_name))?;

        let tips: HashMap<CommitId, &Commit> = branch
            .tips
            .iter()
            .filter_map(|id| branch.commits.get(id).map(|c| (*id, c)))
            .collect();

        Ok(tips)
    }

    /// Get all commits in a branch.
    pub fn get_commits(
        &self,
        object_id: ObjectId,
        branch_name: impl Into<BranchName>,
    ) -> Result<&HashMap<CommitId, Commit>, Error> {
        let branch_name = Self::normalize_branch_name(branch_name.into())?;

        let object = self
            .get(object_id)
            .ok_or(Error::ObjectNotFound(object_id))?;

        let branch = object
            .branches
            .get(&branch_name)
            .ok_or(Error::BranchNotFound(branch_name))?;

        Ok(&branch.commits)
    }

    pub fn get_leaf_head_ids_for_prefix(
        &mut self,
        object_id: ObjectId,
        prefix: &BranchPrefixName,
        storage: &dyn Storage,
    ) -> Result<HashMap<BranchName, CommitId>, Error> {
        let prefix_key = prefix.branch_prefix();
        self.ensure_prefix_batch_catalog_loaded(object_id, prefix, storage)?;
        let Some(catalog) = self
            .get(object_id)
            .and_then(|object| object.prefix_batches.get(&prefix_key))
        else {
            return Ok(HashMap::new());
        };

        let mut head_ids = HashMap::new();
        for batch_id in catalog.leaf_batch_ids() {
            if let Some(batch_meta) = catalog.batch_meta(&batch_id) {
                head_ids.insert(
                    prefix.with_batch_id(batch_id).to_branch_name(),
                    batch_meta.head_commit_id,
                );
            }
        }

        Ok(head_ids)
    }

    pub fn get_head_ids_for_prefix(
        &mut self,
        object_id: ObjectId,
        prefix: &BranchPrefixName,
        storage: &dyn Storage,
    ) -> Result<HashMap<BranchName, CommitId>, Error> {
        let prefix_key = prefix.branch_prefix();
        self.ensure_prefix_batch_catalog_loaded(object_id, prefix, storage)?;
        let Some(catalog) = self
            .get(object_id)
            .and_then(|object| object.prefix_batches.get(&prefix_key))
        else {
            return Ok(HashMap::new());
        };

        let mut head_ids = HashMap::new();
        for batch_meta in catalog.batch_metas() {
            head_ids.insert(
                prefix.with_batch_id(batch_meta.batch_id).to_branch_name(),
                batch_meta.head_commit_id,
            );
        }

        Ok(head_ids)
    }

    pub fn ensure_prefix_batch_catalog_loaded(
        &mut self,
        object_id: ObjectId,
        prefix: &BranchPrefixName,
        storage: &dyn Storage,
    ) -> Result<(), Error> {
        let prefix_key = prefix.branch_prefix();

        if self.get(object_id).is_none() && self.get_or_load(object_id, storage, &[]).is_none() {
            return Err(Error::ObjectNotFound(object_id));
        }

        if self
            .get(object_id)
            .and_then(|object| object.prefix_batches.get(&prefix_key))
            .is_some()
        {
            return Ok(());
        }

        let catalog = storage
            .load_prefix_batch_catalog(object_id, &prefix_key)
            .map_err(Error::StorageError)?
            .unwrap_or_default();

        if let Some(object) = self.get_mut(object_id) {
            object.prefix_batches.entry(prefix_key).or_insert(catalog);
        }

        Ok(())
    }

    pub fn resolve_latest_visible_tip(
        &mut self,
        object_id: ObjectId,
        branch_name: impl Into<BranchName>,
        storage: &dyn Storage,
    ) -> Result<Option<(BranchName, CommitId, Commit)>, Error> {
        let branch_name = Self::normalize_branch_name(branch_name.into())?;
        let branch_key = BatchBranchKey::from_branch_name(branch_name);
        let requested_branches = [branch_key];
        if self
            .get_or_load_tips_keys(object_id, storage, &requested_branches)
            .is_none()
        {
            return Ok(None);
        }

        if let Some((commit_id, commit)) = self
            .get(object_id)
            .and_then(|object| object.branches.get(&branch_name))
            .and_then(Self::newest_tip_commit)
        {
            return Ok(Some((branch_name, commit_id, commit.clone())));
        }

        let composed_branch =
            ComposedBranchName::parse(&branch_name).ok_or(Error::InvalidBranchName(branch_name))?;
        let leaf_heads =
            self.get_leaf_head_ids_for_prefix(object_id, &composed_branch.prefix(), storage)?;

        let missing_leaf_branches: Vec<BatchBranchKey> = {
            let object = self
                .get(object_id)
                .ok_or(Error::ObjectNotFound(object_id))?;
            leaf_heads
                .keys()
                .filter(|leaf_branch_name| !object.branches.contains_key(leaf_branch_name))
                .map(|leaf_branch_name| BatchBranchKey::from_branch_name(*leaf_branch_name))
                .collect()
        };
        if !missing_leaf_branches.is_empty() {
            self.get_or_load_tips_keys(object_id, storage, &missing_leaf_branches);
        }

        let object = self
            .get(object_id)
            .ok_or(Error::ObjectNotFound(object_id))?;
        Ok(leaf_heads
            .into_iter()
            .filter_map(|(leaf_branch_name, head_id)| {
                object
                    .branches
                    .get(&leaf_branch_name)
                    .and_then(|branch| branch.commits.get(&head_id))
                    .map(|commit| (leaf_branch_name, head_id, commit.clone()))
            })
            .max_by_key(|(_, head_id, commit)| (commit.timestamp, *head_id)))
    }

    pub fn resolve_visible_parent_ids(
        &mut self,
        object_id: ObjectId,
        branch_name: impl Into<BranchName>,
        storage: &dyn Storage,
    ) -> Result<Vec<CommitId>, Error> {
        let branch_name = Self::normalize_branch_name(branch_name.into())?;
        let branch_key = BatchBranchKey::from_branch_name(branch_name);
        let requested_branches = [branch_key];
        if self
            .get_or_load_tips_keys(object_id, storage, &requested_branches)
            .is_none()
        {
            return Err(Error::ObjectNotFound(object_id));
        }

        if let Ok(tips) = self.get_tip_ids(object_id, branch_name) {
            return Ok(tips.iter().copied().collect());
        }

        let composed_branch =
            ComposedBranchName::parse(&branch_name).ok_or(Error::InvalidBranchName(branch_name))?;
        let mut parents: Vec<_> = self
            .get_leaf_head_ids_for_prefix(object_id, &composed_branch.prefix(), storage)?
            .into_iter()
            .filter_map(|(leaf_branch_name, head_id)| {
                (leaf_branch_name != branch_name).then_some(head_id)
            })
            .collect();
        parents.sort();
        parents.dedup();
        Ok(parents)
    }

    /// Receive an object from a remote source (with specified ID).
    ///
    /// Unlike `create`, this uses the provided ObjectId rather than generating a new one.
    /// Used by sync layer to receive objects from peers.
    /// Persists to storage via Storage synchronously.
    pub fn receive_object<H: Storage>(
        &mut self,
        io: &mut H,
        object_id: ObjectId,
        metadata: HashMap<String, String>,
    ) {
        let object = Object {
            id: object_id,
            metadata: metadata.clone(),
            branches: ObjectBranches::default(),
            commit_branches: HashMap::new(),
            prefix_batches: HashMap::new(),
        };

        // Sync storage - returns immediately
        let _ = io.create_object(object_id, metadata);

        self.objects.insert(object_id, object);
    }

    /// Receive a commit from a remote source.
    ///
    /// Unlike `add_commit`, this accepts a pre-built Commit (with existing timestamp/id).
    /// Validates parent references but doesn't require parents to be tips.
    /// Persists to storage via Storage synchronously.
    pub fn receive_commit<H: Storage>(
        &mut self,
        io: &mut H,
        object_id: ObjectId,
        branch_name: impl Into<BranchName>,
        commit: Commit,
    ) -> Result<CommitId, Error> {
        let branch_name = Self::normalize_branch_name(branch_name.into())?;
        let branch_key = BatchBranchKey::from_branch_name(branch_name);
        let commit_id = commit.id();
        self.ensure_branch_ready_for_received_commit(
            io,
            object_id,
            branch_key,
            commit_id,
            &commit.parents,
        )?;

        // Capture previous state BEFORE mutation for AllObjectUpdate.
        let (previous_commit_ids, old_content, already_exists) = {
            let object = self
                .get(object_id)
                .ok_or(Error::ObjectNotFound(object_id))?;

            if let Some(branch) = object.branches.get(&branch_name) {
                // Check if commit already exists (idempotent)
                if branch.commits.contains_key(&commit_id) {
                    (vec![], None, true)
                } else {
                    let prev_tips = Self::tips_by_timestamp(&branch.commits, &branch.tips);
                    // Last tip in sorted order is the "winner" (newest by timestamp)
                    let old_content = prev_tips
                        .last()
                        .and_then(|tip_id| branch.commits.get(tip_id))
                        .map(|commit| commit.content.clone());
                    (prev_tips, old_content, false)
                }
            } else {
                // New branch - no previous state
                (vec![], None, false)
            }
        };

        // Skip notification and mutation for duplicate commits
        if already_exists {
            return Ok(commit_id);
        }

        // Sync storage - returns immediately
        let mut commit = commit;
        let pending_batch_update = {
            let object = self
                .get(object_id)
                .ok_or(Error::ObjectNotFound(object_id))?;
            Some(Self::plan_prefix_batch_update(
                object, io, object_id, branch_key, &commit,
            )?)
        };
        if io
            .append_commit(
                object_id,
                &QueryBranchRef::from_batch_branch_key(branch_key),
                commit.clone(),
                pending_batch_update
                    .as_ref()
                    .map(|update| update.storage_update.clone()),
            )
            .is_ok()
        {
            commit.stored_state = StoredState::Stored;
        }

        // Get mutable reference to update
        let object = self.get_mut(object_id).expect("validated above");

        if let Some(update) = &pending_batch_update {
            let catalog = object
                .prefix_batches
                .entry(update.prefix.clone())
                .or_default();
            Self::apply_prefix_batch_update(catalog, &update.storage_update);
            for (parent_commit_id, parent_branch_key) in &update.resolved_parent_branches {
                object
                    .commit_branches
                    .entry(*parent_commit_id)
                    .or_insert(*parent_branch_key);
            }
        }

        // Create branch if needed
        let branch = object.branches.get_or_insert_with(branch_name, || Branch {
            loaded_state: BranchLoadedState::AllCommits,
            ..Default::default()
        });

        // Update tips: remove any parents that are tips, add this commit as tip
        for parent in &commit.parents {
            branch.tips.remove(parent);
        }
        branch.tips.insert(commit_id);

        branch.commits.insert(commit_id, commit);
        object.commit_branches.insert(commit_id, branch_key);

        // Notify subscribers
        self.notify_subscribers_of_commit(object_id, branch_name);

        // Notify global subscribers (received objects are never "new" from our perspective)
        self.notify_all_object_subscribers(
            object_id,
            branch_name,
            false,
            previous_commit_ids,
            old_content,
        );

        Ok(commit_id)
    }

    /// Subscribe to updates on a branch.
    ///
    /// With sync storage, the branch is always immediately available if the object exists.
    /// Queues an immediate update with existing commits if the branch exists.
    pub fn subscribe(
        &mut self,
        object_id: ObjectId,
        branch_name: impl Into<BranchName>,
    ) -> SubscriptionId {
        let branch_name = branch_name.into();
        let normalized_branch_name = Self::normalize_loaded_branch_name(branch_name)
            .expect("subscriptions require composed batch branches");
        let normalized_branch_key = BatchBranchKey::from_branch_name(normalized_branch_name);
        let id = SubscriptionId(self.next_subscription_id);
        self.next_subscription_id += 1;

        let subscription = Subscription {
            object_id,
            branch_name,
            normalized_branch_key,
        };
        self.subscriptions.insert(id, subscription);

        self.branch_subscribers
            .entry((object_id, normalized_branch_key))
            .or_default()
            .insert(id);

        // With sync storage, branch is immediately available if object exists
        if let Some(object) = self.get(object_id)
            && let Some(branch) = object.branches.get(&normalized_branch_name)
        {
            let commit_ids = Self::tips_by_timestamp(&branch.commits, &branch.tips);
            self.subscription_outbox.push(SubscriptionUpdate {
                subscription_id: id,
                object_id,
                branch_name,
                commit_ids,
            });
        }

        id
    }

    /// Unsubscribe from updates.
    ///
    /// Removes the subscription and any pending updates for it.
    pub fn unsubscribe(&mut self, subscription_id: SubscriptionId) {
        if let Some(sub) = self.subscriptions.remove(&subscription_id)
            && let Some(subscribers) = self
                .branch_subscribers
                .get_mut(&(sub.object_id, sub.normalized_branch_key))
        {
            subscribers.remove(&subscription_id);
        }

        // Remove pending updates for this subscription
        self.subscription_outbox
            .retain(|u| u.subscription_id != subscription_id);
    }

    /// Take all pending subscription updates.
    pub fn take_subscription_updates(&mut self) -> Vec<SubscriptionUpdate> {
        std::mem::take(&mut self.subscription_outbox)
    }

    /// Subscribe to all object changes globally.
    ///
    /// Returns updates for any create(), receive_object(), add_commit(), receive_commit().
    pub fn subscribe_all(&mut self) -> AllObjectsSubscriptionId {
        let id = AllObjectsSubscriptionId(self.next_all_objects_subscription_id);
        self.next_all_objects_subscription_id += 1;
        self.all_object_subscriptions.insert(id);
        id
    }

    /// Unsubscribe from global object updates.
    pub fn unsubscribe_all(&mut self, id: AllObjectsSubscriptionId) {
        self.all_object_subscriptions.remove(&id);
    }

    /// Take all pending global object updates.
    pub fn take_all_object_updates(&mut self) -> Vec<AllObjectUpdate> {
        std::mem::take(&mut self.all_objects_outbox)
    }

    /// Emit an update to all global subscribers.
    fn notify_all_object_subscribers(
        &mut self,
        object_id: ObjectId,
        branch_name: BranchName,
        is_new_object: bool,
        previous_commit_ids: Vec<CommitId>,
        old_content: Option<Vec<u8>>,
    ) {
        if self.all_object_subscriptions.is_empty() {
            return;
        }

        let (metadata, commit_ids) = if let Some(object) = self.get(object_id) {
            let commit_ids = if let Some(branch) = object.branches.get(&branch_name) {
                Self::tips_by_timestamp(&branch.commits, &branch.tips)
            } else {
                vec![]
            };
            (object.metadata.clone(), commit_ids)
        } else {
            (HashMap::new(), vec![])
        };

        self.all_objects_outbox.push(AllObjectUpdate {
            object_id,
            metadata,
            branch_name,
            commit_ids,
            is_new_object,
            previous_commit_ids,
            old_content,
        });
    }

    /// Get the current frontier (tips) sorted by timestamp (oldest first).
    fn tips_by_timestamp(
        commits: &HashMap<CommitId, Commit>,
        tips: &SmolSet<[CommitId; 2]>,
    ) -> Vec<CommitId> {
        let mut tip_vec: Vec<_> = tips.iter().copied().collect();
        tip_vec.sort_by_key(|id| (commits.get(id).map(|c| c.timestamp).unwrap_or(0), *id));
        tip_vec
    }

    /// Notify subscribers about a commit change - sends current frontier sorted by timestamp.
    fn notify_subscribers_of_commit(&mut self, object_id: ObjectId, branch_name: BranchName) {
        let key = (object_id, BatchBranchKey::from_branch_name(branch_name));
        if let Some(subscriber_ids) = self.branch_subscribers.get(&key).cloned() {
            // Get current tips from the branch
            let commit_ids = if let Some(object) = self.get(object_id) {
                if let Some(branch) = object.branches.get(&branch_name) {
                    Self::tips_by_timestamp(&branch.commits, &branch.tips)
                } else {
                    vec![]
                }
            } else {
                vec![]
            };

            for sub_id in subscriber_ids {
                let display_branch_name = self
                    .subscriptions
                    .get(&sub_id)
                    .map(|subscription| subscription.branch_name)
                    .unwrap_or(branch_name);
                self.subscription_outbox.push(SubscriptionUpdate {
                    subscription_id: sub_id,
                    object_id,
                    branch_name: display_branch_name,
                    commit_ids: commit_ids.clone(),
                });
            }
        }
    }

    /// Truncate a branch by removing commits topologically earlier than the specified tails.
    ///
    /// All tips must be descendants of (or equal to) some tail. Commits before the tails
    /// are deleted. Operations are persisted synchronously via Storage.
    pub fn truncate_branch<H: Storage>(
        &mut self,
        io: &mut H,
        object_id: ObjectId,
        branch_name: impl Into<BranchName>,
        tail_ids: HashSet<CommitId>,
    ) -> TruncateResult {
        let branch_name = match Self::normalize_branch_name_for_truncate(branch_name.into()) {
            Ok(branch_name) => branch_name,
            Err(error) => return TruncateResult::PermanentError(error),
        };
        let requested_branches = [branch_name.as_str().to_string()];
        let _ = self.get_or_load(object_id, io, &requested_branches);

        // Validate object exists
        let object = match self.get(object_id) {
            Some(obj) => obj,
            None => {
                return TruncateResult::PermanentError(TruncateError::ObjectNotFound(object_id));
            }
        };

        // Validate branch exists
        let branch = match object.branches.get(&branch_name) {
            Some(b) => b,
            None => {
                return TruncateResult::PermanentError(TruncateError::BranchNotFound(branch_name));
            }
        };

        // Validate all tail_ids exist in branch
        for tail_id in &tail_ids {
            if !branch.commits.contains_key(tail_id) {
                return TruncateResult::PermanentError(TruncateError::TailNotFound(*tail_id));
            }
        }

        // Convert to SmolSet for use in Branch
        let tail_smolset: SmolSet<[CommitId; 2]> = tail_ids.iter().copied().collect();

        // Check invariant: all tips must be descendants of (or equal to) some tail
        for tip in &branch.tips {
            if !Self::is_descendant_of_any(&branch.commits, *tip, &tail_smolset) {
                return TruncateResult::PermanentError(TruncateError::TipBeforeTail(*tip));
            }
        }

        // Find all commits to delete (ancestors of tails, not including tails themselves)
        let commits_to_delete = Self::find_ancestors(&branch.commits, &tail_ids);

        // If nothing to delete and tails already set to same value, return success immediately
        if commits_to_delete.is_empty() && branch.tails.as_ref() == Some(&tail_smolset) {
            return TruncateResult::Success { deleted_commits: 0 };
        }

        let retained_commits: Vec<Commit> = branch
            .commits
            .iter()
            .filter(|(commit_id, _)| !commits_to_delete.contains(commit_id))
            .map(|(_, commit)| commit.clone())
            .collect();
        let _ = io.replace_branch(
            object_id,
            &QueryBranchRef::from_branch_name(branch_name),
            retained_commits,
            tail_ids.clone(),
        );

        // Update in-memory state
        let object = self
            .get_mut(object_id)
            .expect("object existence already validated");
        let branch = object.branches.get_mut(&branch_name).unwrap();

        // Set tails
        branch.tails = Some(tail_smolset);

        // Remove deleted commits from memory
        for commit_id in &commits_to_delete {
            branch.commits.remove(commit_id);
            object.commit_branches.remove(commit_id);
        }

        TruncateResult::Success {
            deleted_commits: commits_to_delete.len(),
        }
    }

    /// Check if `commit_id` is a descendant of any commit in `ancestors` (or is in ancestors itself).
    fn is_descendant_of_any(
        commits: &HashMap<CommitId, Commit>,
        commit_id: CommitId,
        ancestors: &SmolSet<[CommitId; 2]>,
    ) -> bool {
        if ancestors.contains(&commit_id) {
            return true;
        }

        // BFS from commit_id backwards through parents
        let mut visited = HashSet::new();
        let mut queue = std::collections::VecDeque::new();
        queue.push_back(commit_id);

        while let Some(current) = queue.pop_front() {
            if !visited.insert(current) {
                continue;
            }

            if let Some(commit) = commits.get(&current) {
                for parent in &commit.parents {
                    if ancestors.contains(parent) {
                        return true;
                    }
                    queue.push_back(*parent);
                }
            }
        }

        false
    }

    /// Find all ancestors of the given commits (not including the commits themselves).
    /// Only returns commits that actually exist in the commits map.
    fn find_ancestors(
        commits: &HashMap<CommitId, Commit>,
        starting_points: &HashSet<CommitId>,
    ) -> HashSet<CommitId> {
        let mut ancestors = HashSet::new();
        let mut queue = std::collections::VecDeque::new();

        // Start from parents of the starting points
        for start in starting_points {
            if let Some(commit) = commits.get(start) {
                for parent in &commit.parents {
                    // Only consider parents that exist in the commits map
                    if commits.contains_key(parent) {
                        queue.push_back(*parent);
                    }
                }
            }
        }

        while let Some(current) = queue.pop_front() {
            if !ancestors.insert(current) {
                continue;
            }

            if let Some(commit) = commits.get(&current) {
                for parent in &commit.parents {
                    // Only consider parents that exist in the commits map
                    if commits.contains_key(parent) {
                        queue.push_back(*parent);
                    }
                }
            }
        }

        ancestors
    }

    // ========================================================================
    // No-op storage driver (for tests)
    // ========================================================================
    // ========================================================================
    // Memory profiling
    // ========================================================================

    /// Calculate memory usage breakdown for profiling.
    ///
    /// Returns a tuple: (row_objects, index_objects, subscriptions, other, total)
    pub fn memory_size(&self) -> (usize, usize, usize, usize, usize) {
        let mut row_objects = 0usize;
        let mut index_objects = 0usize;

        for obj in self.objects.values() {
            let obj_size = self.estimate_object_size(obj);
            let is_index = obj
                .metadata
                .get(crate::metadata::MetadataKey::Type.as_str())
                .is_some_and(|t| t == crate::metadata::ObjectType::Index.as_str());
            // Add HashMap entry overhead: ~48 bytes per entry
            let entry_overhead = std::mem::size_of::<ObjectId>() + 48;
            if is_index {
                index_objects += obj_size + entry_overhead;
            } else {
                row_objects += obj_size + entry_overhead;
            }
        }

        // Subscriptions
        let subscriptions = self.subscriptions.len() * 80  // ~80 bytes per subscription
            + self.branch_subscribers.len() * 96  // ~96 bytes per branch subscriber entry
            + self.all_object_subscriptions.len() * 16
            + self.subscription_outbox.len() * 128; // SubscriptionUpdate ~128 bytes

        // Other (subscription outbox for all objects)
        let other = self.all_objects_outbox.len() * 200; // AllObjectUpdate ~200 bytes

        let total = row_objects + index_objects + subscriptions + other;
        (row_objects, index_objects, subscriptions, other, total)
    }

    /// Estimate memory size of an Object.
    fn estimate_object_size(&self, obj: &Object) -> usize {
        let mut size = std::mem::size_of::<Object>();

        // Metadata HashMap
        for (k, v) in &obj.metadata {
            size += k.len() + v.len() + 48; // String overhead + HashMap entry
        }

        // Branches
        for (name, branch) in obj.branches.iter() {
            size += name.0.len() + 24; // BranchName (String) + overhead
            size += 48; // HashMap entry overhead

            // Branch struct base size
            size += std::mem::size_of::<Branch>();

            // Commits HashMap
            for (commit_id, commit) in &branch.commits {
                size += std::mem::size_of_val(commit_id);
                size += self.estimate_commit_size(commit);
                size += 48; // HashMap entry overhead
            }

            // Tips HashSet
            size += branch.tips.len() * (32 + 16); // CommitId + HashSet entry overhead

            // Tails Option<HashSet>
            if let Some(tails) = &branch.tails {
                size += tails.len() * (32 + 16);
            }
        }

        for (commit_id, branch_key) in &obj.commit_branches {
            size += std::mem::size_of_val(commit_id);
            size += branch_key.prefix_name().0.len()
                + std::mem::size_of::<crate::query_manager::types::BatchId>()
                + 24;
            size += 48;
        }

        for (prefix, catalog) in &obj.prefix_batches {
            size += prefix.len() + 24;
            size += 48;
            size += catalog.leaf_batch_count() * (std::mem::size_of::<u32>() + 16);
            for batch_meta in catalog.batch_metas() {
                size += std::mem::size_of::<PrefixBatchMeta>() + 48;
                size += batch_meta.parent_batch_ords.capacity() * std::mem::size_of::<u32>();
            }
        }

        size
    }

    /// Estimate memory size of a Commit.
    fn estimate_commit_size(&self, commit: &Commit) -> usize {
        let mut size = std::mem::size_of::<Commit>();

        // Parents vec
        size += commit.parents.capacity() * std::mem::size_of::<CommitId>();

        // Content vec (this is often the biggest part)
        size += commit.content.capacity();

        // Metadata BTreeMap
        if let Some(meta) = &commit.metadata {
            for (k, v) in meta {
                size += k.len() + v.len() + 64; // String overhead + BTreeMap node overhead
            }
        }

        size
    }
}

#[cfg(test)]
mod tests;
