use std::collections::{HashMap, HashSet};
use web_time::{SystemTime, UNIX_EPOCH};

use smolset::SmolSet;

use crate::commit::{Commit, CommitId};
use crate::metadata::MetadataKey;
use crate::object::{Branch, BranchName, Object, ObjectId};
use crate::row_regions::{HistoryScan, RowState, StoredRowVersion};
use crate::storage::{Storage, StorageError};
use crate::sync_manager::DurabilityTier;

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
}

/// Update sent to subscribers when commits are added or loaded.
///
/// Contains the current frontier (tips) sorted by timestamp (oldest first).
/// When twigs diverge, you'll see multiple commits in the frontier.
/// When they merge, the frontier consolidates back to one.
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
    /// Current frontier commit IDs for this branch, sorted by timestamp.
    pub commit_ids: Vec<CommitId>,
    /// True if this is a newly created/received object, false if existing object.
    pub is_new_object: bool,
    /// Previous tip commit IDs before this update (empty for new objects).
    pub previous_commit_ids: Vec<CommitId>,
    /// Content of previous "winning" tip (newest by timestamp). None if new object.
    /// Used by QueryManager to compute index deltas for synced updates.
    pub old_content: Option<Vec<u8>>,
}

/// Visible row change emitted when a row object's winning version changes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowObjectUpdate {
    pub object_id: ObjectId,
    pub metadata: HashMap<String, String>,
    pub row: StoredRowVersion,
    pub previous_row: Option<StoredRowVersion>,
    pub is_new_object: bool,
}

/// Errors that can occur when managing objects and commits.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    ObjectNotFound(ObjectId),
    BranchNotFound(BranchName),
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
    TailNotFound(CommitId),
    /// Can't truncate past the frontier - tip is not a descendant of any tail.
    TipBeforeTail(CommitId),
}

#[derive(Debug, Clone, Default)]
struct RowBranch {
    versions: HashMap<CommitId, StoredRowVersion>,
    tips: SmolSet<[CommitId; 2]>,
    current_visible: Option<CommitId>,
}

#[derive(Debug, Clone)]
struct RowBranchApply {
    previous_visible: Option<StoredRowVersion>,
    current_visible: Option<StoredRowVersion>,
    visible_changed: bool,
}

impl RowBranch {
    fn visible_row_wins(incoming: &StoredRowVersion, current: &StoredRowVersion) -> bool {
        (incoming.updated_at, incoming.version_id()) > (current.updated_at, current.version_id())
    }

    fn current_visible_row(&self) -> Option<StoredRowVersion> {
        let version_id = self.current_visible?;
        self.versions.get(&version_id).cloned()
    }

    fn contains(&self, version_id: CommitId) -> bool {
        self.versions.contains_key(&version_id)
    }

    fn tip_ids_by_timestamp(&self) -> Vec<CommitId> {
        let mut tip_vec: Vec<_> = self.tips.iter().copied().collect();
        tip_vec.sort_by_key(|id| {
            self.versions
                .get(id)
                .map(|row| (row.updated_at, *id))
                .unwrap_or((0, *id))
        });
        tip_vec
    }

    fn recompute_current_visible(&mut self) -> Option<CommitId> {
        let current = self
            .versions
            .values()
            .filter(|row| row.state.is_visible())
            .max_by_key(|row| (row.updated_at, row.version_id()))
            .map(StoredRowVersion::version_id);
        self.current_visible = current;
        current
    }

    fn remember(&mut self, row: StoredRowVersion) -> RowBranchApply {
        let previous_visible = self.current_visible_row();
        let version_id = row.version_id();

        if self.versions.contains_key(&version_id) {
            let current_visible = self.current_visible_row();
            return RowBranchApply {
                visible_changed: previous_visible != current_visible,
                previous_visible,
                current_visible,
            };
        }

        for parent in &row.parents {
            self.tips.remove(parent);
        }
        self.tips.insert(version_id);
        self.versions.insert(version_id, row);

        let current_visible_id = match previous_visible.as_ref() {
            Some(current) => {
                let incoming = self
                    .versions
                    .get(&version_id)
                    .expect("just inserted row version");
                if incoming.state.is_visible() && Self::visible_row_wins(incoming, current) {
                    Some(version_id)
                } else {
                    self.current_visible
                }
            }
            None => self.recompute_current_visible(),
        };
        self.current_visible = current_visible_id;

        let current_visible = self.current_visible_row();
        RowBranchApply {
            visible_changed: previous_visible != current_visible,
            previous_visible,
            current_visible,
        }
    }

    fn patch_state(
        &mut self,
        version_id: CommitId,
        state: Option<RowState>,
        confirmed_tier: Option<DurabilityTier>,
    ) -> Option<RowBranchApply> {
        let previous_visible = self.current_visible_row();
        let row = self.versions.get_mut(&version_id)?;

        if let Some(state) = state {
            row.state = state;
        }
        row.confirmed_tier = match (row.confirmed_tier, confirmed_tier) {
            (Some(existing), Some(incoming)) => Some(existing.max(incoming)),
            (Some(existing), None) => Some(existing),
            (None, incoming) => incoming,
        };

        self.recompute_current_visible();
        let current_visible = self.current_visible_row();
        Some(RowBranchApply {
            visible_changed: previous_visible != current_visible,
            previous_visible,
            current_visible,
        })
    }
}

/// Manages a collection of objects.
///
/// With sync storage (Phase 2), objects are stored directly in the HashMap -
/// no ObjectState enum, no Loading state, no async request/response cycle.
#[derive(Debug, Clone, Default)]
pub struct ObjectManager {
    pub objects: HashMap<ObjectId, Object>,
    row_branches: HashMap<(ObjectId, BranchName), RowBranch>,
    next_subscription_id: u64,
    subscriptions: HashMap<SubscriptionId, Subscription>,
    /// Index: (ObjectId, BranchName) → set of SubscriptionIds
    branch_subscribers: HashMap<(ObjectId, BranchName), HashSet<SubscriptionId>>,
    pub subscription_outbox: Vec<SubscriptionUpdate>,
    /// Global (all-objects) subscriptions.
    all_object_subscriptions: HashSet<AllObjectsSubscriptionId>,
    next_all_objects_subscription_id: u64,
    /// Outbox for global subscription updates.
    pub all_objects_outbox: Vec<AllObjectUpdate>,
    /// Outbox for visible row changes.
    pub row_objects_outbox: Vec<RowObjectUpdate>,
    /// Last timestamp used, for monotonic ordering.
    last_timestamp: u64,
}

impl ObjectManager {
    pub fn new() -> Self {
        Self::default()
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
            branches: HashMap::new(),
        };

        // Sync storage - returns immediately
        let _ = io.create_object(id, metadata.unwrap_or_default());

        self.objects.insert(id, object);
        id
    }

    fn is_row_metadata(metadata: &HashMap<String, String>) -> bool {
        metadata.contains_key(MetadataKey::Table.as_str())
    }

    fn row_branch(&self, object_id: ObjectId, branch_name: &BranchName) -> Option<&RowBranch> {
        self.row_branches.get(&(object_id, *branch_name))
    }

    fn row_branch_mut(
        &mut self,
        object_id: ObjectId,
        branch_name: &BranchName,
    ) -> Option<&mut RowBranch> {
        self.row_branches.get_mut(&(object_id, *branch_name))
    }

    fn apply_row_version<H: Storage>(
        &mut self,
        io: &mut H,
        object_id: ObjectId,
        branch_name: BranchName,
        row: StoredRowVersion,
    ) -> Result<CommitId, Error> {
        let object_metadata = self
            .get(object_id)
            .ok_or(Error::ObjectNotFound(object_id))?
            .metadata
            .clone();

        debug_assert!(
            Self::is_row_metadata(&object_metadata),
            "apply_row_version should only be used for row-backed objects"
        );

        if !row.parents.is_empty() {
            let branch = self
                .row_branch(object_id, &branch_name)
                .ok_or(Error::BranchNotFound(branch_name))?;
            for parent in &row.parents {
                if !branch.contains(*parent) {
                    return Err(Error::ParentNotFound(*parent));
                }
            }
        }

        let version_id = row.version_id();
        if self.row_version_exists(object_id, &branch_name, version_id) {
            return Ok(version_id);
        }

        let previous_row = self.visible_row(object_id, branch_name);
        let applied = self.remember_row_version(object_id, branch_name, row.clone());
        tracing::trace!(?version_id, "row version applied");
        self.notify_subscribers_of_commit(object_id, branch_name);
        self.notify_row_object_update(
            io,
            &object_metadata,
            previous_row,
            applied.current_visible,
            row,
            applied.visible_changed,
        );

        Ok(version_id)
    }

    fn persist_row_version<H: Storage>(io: &mut H, table: &str, row: &StoredRowVersion) {
        if let Err(error) = io.append_history_region_rows(table, std::slice::from_ref(row)) {
            tracing::warn!(
                table,
                branch = row.branch,
                row_id = %row.row_id,
                %error,
                "failed to append row-region history row"
            );
        }
    }

    fn upsert_visible_row<H: Storage>(io: &mut H, table: &str, row: &StoredRowVersion) {
        if let Err(error) = io.upsert_visible_region_rows(table, std::slice::from_ref(row)) {
            tracing::warn!(
                table,
                branch = row.branch,
                row_id = %row.row_id,
                %error,
                "failed to upsert row-region visible row"
            );
        }
    }

    fn notify_row_object_update<H: Storage>(
        &mut self,
        io: &mut H,
        metadata: &HashMap<String, String>,
        previous_row: Option<StoredRowVersion>,
        current_row: Option<StoredRowVersion>,
        history_row: StoredRowVersion,
        visible_changed: bool,
    ) {
        let Some(table) = metadata.get(MetadataKey::Table.as_str()).cloned() else {
            return;
        };

        Self::persist_row_version(io, &table, &history_row);

        let Some(current_row) = current_row else {
            return;
        };

        if visible_changed {
            Self::upsert_visible_row(io, &table, &current_row);
        } else {
            return;
        }

        let is_new_object = previous_row.is_none();
        self.row_objects_outbox.push(RowObjectUpdate {
            object_id: current_row.row_id,
            metadata: metadata.clone(),
            row: current_row,
            previous_row,
            is_new_object,
        });
    }

    fn remember_row_version(
        &mut self,
        object_id: ObjectId,
        branch_name: BranchName,
        row: StoredRowVersion,
    ) -> RowBranchApply {
        self.row_branches
            .entry((object_id, branch_name))
            .or_default()
            .remember(row)
    }

    pub fn remember_remote_row_version(
        &mut self,
        object_id: ObjectId,
        branch_name: BranchName,
        row: StoredRowVersion,
    ) {
        let _ = self.remember_row_version(object_id, branch_name, row);
    }

    pub fn add_row_version<H: Storage>(
        &mut self,
        io: &mut H,
        object_id: ObjectId,
        branch_name: impl Into<BranchName>,
        row: StoredRowVersion,
    ) -> Result<CommitId, Error> {
        self.apply_row_version(io, object_id, branch_name.into(), row)
    }

    pub fn visible_row(
        &self,
        object_id: ObjectId,
        branch_name: impl Into<BranchName>,
    ) -> Option<StoredRowVersion> {
        let branch_name = branch_name.into();
        self.row_branch(object_id, &branch_name)
            .and_then(RowBranch::current_visible_row)
    }

    pub fn row_version_exists(
        &self,
        object_id: ObjectId,
        branch_name: &BranchName,
        version_id: CommitId,
    ) -> bool {
        self.row_branch(object_id, branch_name)
            .is_some_and(|branch| branch.contains(version_id))
    }

    pub fn patch_row_version_state(
        &mut self,
        object_id: ObjectId,
        branch_name: &BranchName,
        version_id: CommitId,
        state: Option<RowState>,
        confirmed_tier: Option<DurabilityTier>,
    ) -> Option<RowObjectUpdate> {
        let metadata = self.get(object_id)?.metadata.clone();
        let applied = self.row_branch_mut(object_id, branch_name)?.patch_state(
            version_id,
            state,
            confirmed_tier,
        )?;

        if !applied.visible_changed {
            return None;
        }

        let row = applied.current_visible?;
        let is_new_object = applied.previous_visible.is_none();
        Some(RowObjectUpdate {
            object_id,
            metadata,
            row,
            previous_row: applied.previous_visible,
            is_new_object,
        })
    }

    pub fn row_versions_for_sync(
        &self,
    ) -> Vec<(ObjectId, HashMap<String, String>, StoredRowVersion)> {
        let mut rows = Vec::new();
        for ((object_id, _branch_name), branch) in &self.row_branches {
            let Some(object) = self.objects.get(object_id) else {
                continue;
            };
            let metadata = object.metadata.clone();
            for row in branch.versions.values() {
                rows.push((*object_id, metadata.clone(), row.clone()));
            }
        }
        rows.sort_by_key(|(object_id, _, row)| {
            (
                *object_id,
                row.branch.clone(),
                row.updated_at,
                row.version_id(),
            )
        });
        rows
    }

    /// Get an object by id.
    pub fn get(&self, id: ObjectId) -> Option<&Object> {
        self.objects.get(&id)
    }

    /// Get an object, loading from storage if not in memory (lazy cold-start load).
    pub fn get_or_load(
        &mut self,
        id: ObjectId,
        storage: &dyn Storage,
        branches: &[String],
    ) -> Option<&Object> {
        let _span = tracing::trace_span!("OM::get_or_load", %id).entered();
        if let std::collections::hash_map::Entry::Vacant(entry) = self.objects.entry(id) {
            // Load metadata
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

            entry.insert(Object {
                id,
                metadata,
                branches: HashMap::new(),
            });
        }

        let (metadata, loaded_row_versions) = {
            let object = self.objects.get(&id)?;
            (object.metadata.clone(), self.row_branches.len())
        };

        if Self::is_row_metadata(&metadata) {
            let Some(table) = metadata.get(MetadataKey::Table.as_str()).cloned() else {
                return self.objects.get(&id);
            };

            for branch_name in branches {
                let bn = BranchName::new(branch_name);
                if self.row_branches.contains_key(&(id, bn)) {
                    continue;
                }

                let history_rows =
                    match storage.scan_history_region(&table, bn.as_str(), HistoryScan::Branch) {
                        Ok(rows) => rows,
                        Err(err) => {
                            tracing::warn!(
                                %id,
                                branch = %bn,
                                error = ?err,
                                "get_or_load: failed to hydrate row history"
                            );
                            Vec::new()
                        }
                    };

                for row in history_rows {
                    self.remember_remote_row_version(id, bn, row);
                }

                if let Ok(Some(row)) = storage.load_visible_region_row(&table, bn.as_str(), id) {
                    self.remember_remote_row_version(id, bn, row);
                }
            }
        }

        let object = self.objects.get(&id)?;
        let row_branch_count = self
            .row_branches
            .keys()
            .filter(|(object_id, _)| *object_id == id)
            .count();
        let row_version_count: usize = self
            .row_branches
            .iter()
            .filter(|((object_id, _), _)| *object_id == id)
            .map(|(_, branch)| branch.versions.len())
            .sum();
        tracing::trace!(
            %id,
            row_branch_count,
            row_version_count,
            previous_row_branch_count = loaded_row_versions,
            "get_or_load: loaded from storage"
        );
        Some(object)
    }

    /// Get tip IDs for a branch.
    pub fn get_tip_ids(
        &self,
        object_id: ObjectId,
        branch_name: impl Into<BranchName>,
    ) -> Result<&SmolSet<[CommitId; 2]>, Error> {
        let branch_name = branch_name.into();

        if let Some(branch) = self.row_branch(object_id, &branch_name) {
            return Ok(&branch.tips);
        }

        if self.get(object_id).is_none() {
            return Err(Error::ObjectNotFound(object_id));
        }

        Err(Error::BranchNotFound(branch_name))
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
        // Sync storage - returns immediately
        let _ = io.create_object(object_id, metadata.clone());
        self.objects.entry(object_id).or_insert(Object {
            id: object_id,
            metadata,
            branches: HashMap::new(),
        });
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
        let id = SubscriptionId(self.next_subscription_id);
        self.next_subscription_id += 1;

        let subscription = Subscription {
            object_id,
            branch_name,
        };
        self.subscriptions.insert(id, subscription);

        self.branch_subscribers
            .entry((object_id, branch_name))
            .or_default()
            .insert(id);

        // With sync storage, branch is immediately available if object exists
        if let Some(branch) = self.row_branch(object_id, &branch_name) {
            self.subscription_outbox.push(SubscriptionUpdate {
                subscription_id: id,
                object_id,
                branch_name,
                commit_ids: branch.tip_ids_by_timestamp(),
            });
        } else if let Some(object) = self.get(object_id)
            && let Some(branch) = object.branches.get(&branch_name)
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
                .get_mut(&(sub.object_id, sub.branch_name))
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

    /// Take all pending visible row updates.
    pub fn take_row_object_updates(&mut self) -> Vec<RowObjectUpdate> {
        std::mem::take(&mut self.row_objects_outbox)
    }

    /// Take one pending visible row update for the given concrete row version.
    pub fn take_row_object_update_for(
        &mut self,
        object_id: ObjectId,
        branch_name: &BranchName,
        version_id: CommitId,
    ) -> Option<RowObjectUpdate> {
        let index = self.row_objects_outbox.iter().position(|update| {
            update.object_id == object_id
                && update.row.branch == branch_name.as_str()
                && update.row.version_id() == version_id
        })?;
        Some(self.row_objects_outbox.remove(index))
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
        let key = (object_id, branch_name);
        if let Some(subscriber_ids) = self.branch_subscribers.get(&key).cloned() {
            // Get current tips from the branch
            let commit_ids = if let Some(branch) = self.row_branch(object_id, &branch_name) {
                branch.tip_ids_by_timestamp()
            } else if let Some(object) = self.get(object_id) {
                if let Some(branch) = object.branches.get(&branch_name) {
                    Self::tips_by_timestamp(&branch.commits, &branch.tips)
                } else {
                    vec![]
                }
            } else {
                vec![]
            };

            for sub_id in subscriber_ids {
                self.subscription_outbox.push(SubscriptionUpdate {
                    subscription_id: sub_id,
                    object_id,
                    branch_name,
                    commit_ids: commit_ids.clone(),
                });
            }
        }
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
        for (name, branch) in &obj.branches {
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
