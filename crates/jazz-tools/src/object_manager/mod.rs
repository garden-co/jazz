use std::collections::{BTreeMap, HashMap, HashSet};
use web_time::{SystemTime, UNIX_EPOCH};

use smallvec::smallvec;
use smolset::SmolSet;

use crate::commit::{Commit, CommitId, StoredState};
use crate::object::{Branch, BranchLoadedState, BranchName, Object, ObjectId};
use crate::storage::{Storage, StorageError};

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

/// Manages a collection of objects.
///
/// With sync storage (Phase 2), objects are stored directly in the HashMap -
/// no ObjectState enum, no Loading state, no async request/response cycle.
#[derive(Debug, Clone, Default)]
pub struct ObjectManager {
    pub objects: HashMap<ObjectId, Object>,
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
        if self.objects.contains_key(&id) {
            return self.objects.get(&id);
        }

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

        // Build Object with branches
        let mut object = Object {
            id,
            metadata,
            branches: HashMap::new(),
        };
        for branch_name in branches {
            let bn = BranchName::new(branch_name);
            if let Ok(Some(loaded)) = storage.load_branch(id, &bn) {
                let mut commits = HashMap::new();
                let mut tips: SmolSet<[CommitId; 2]> = SmolSet::new();
                for commit in loaded.commits {
                    let cid = commit.id();
                    tips.insert(cid);
                    commits.insert(cid, commit);
                }
                object.branches.insert(
                    bn,
                    Branch {
                        commits,
                        tips,
                        tails: if loaded.tails.is_empty() {
                            None
                        } else {
                            Some(loaded.tails.into_iter().collect())
                        },
                        loaded_state: BranchLoadedState::AllCommits,
                    },
                );
            }
        }

        let branch_count = object.branches.len();
        let commit_count: usize = object.branches.values().map(|b| b.commits.len()).sum();
        tracing::trace!(%id, branch_count, commit_count, "get_or_load: loaded from storage");
        self.objects.insert(id, object);
        self.objects.get(&id)
    }

    /// Get mutable object by id.
    fn get_mut(&mut self, id: ObjectId) -> Option<&mut Object> {
        self.objects.get_mut(&id)
    }

    /// Get a mutable reference to a specific commit.
    pub fn get_commit_mut(
        &mut self,
        object_id: ObjectId,
        branch_name: &BranchName,
        commit_id: CommitId,
    ) -> Option<&mut Commit> {
        self.objects
            .get_mut(&object_id)?
            .branches
            .get_mut(branch_name)?
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
    pub fn add_commit<H: Storage>(
        &mut self,
        io: &mut H,
        object_id: ObjectId,
        branch_name: impl Into<BranchName>,
        parents: Vec<CommitId>,
        content: Vec<u8>,
        author: ObjectId,
        metadata: Option<BTreeMap<String, String>>,
    ) -> Result<CommitId, Error> {
        let branch_name = branch_name.into();
        let _span = tracing::debug_span!("OM::add_commit", %object_id, %branch_name).entered();

        // Capture previous state BEFORE mutation for AllObjectUpdate
        // (previous_commit_ids, old_content)
        let (previous_commit_ids, old_content) = {
            let object = self
                .get(object_id)
                .ok_or(Error::ObjectNotFound(object_id))?;

            // If parents is non-empty, branch must exist and contain all parents
            if !parents.is_empty() {
                let branch = object
                    .branches
                    .get(&branch_name)
                    .ok_or(Error::BranchNotFound(branch_name))?;

                for parent in &parents {
                    if !branch.commits.contains_key(parent) {
                        return Err(Error::ParentNotFound(*parent));
                    }
                }

                // After truncation, reject commits whose parents are before tails
                if let Some(tails) = &branch.tails {
                    for parent in &parents {
                        if !Self::is_descendant_of_any(&branch.commits, *parent, tails) {
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

        let timestamp = self.next_timestamp();

        let mut commit = Commit {
            parents: parents.clone().into(),
            content,
            timestamp,
            author,
            metadata,
            stored_state: StoredState::Pending,
            ack_state: Default::default(),
        };

        let commit_id = commit.id();

        // Sync storage - returns immediately
        if io
            .append_commit(object_id, &branch_name, commit.clone())
            .is_ok()
        {
            commit.stored_state = StoredState::Stored;
        }

        // Now mutably borrow and update
        let object = self
            .get_mut(object_id)
            .expect("object existence already validated");

        // Create branch if it doesn't exist (only valid for parentless commits)
        let branch = object
            .branches
            .entry(branch_name)
            .or_insert_with(|| Branch {
                loaded_state: BranchLoadedState::AllCommits,
                ..Default::default()
            });

        // Update tips: remove parents, add new commit
        for parent in &parents {
            branch.tips.remove(parent);
        }
        branch.tips.insert(commit_id);

        branch.commits.insert(commit_id, commit);

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
    pub fn replace_content(
        &mut self,
        object_id: ObjectId,
        branch_name: impl Into<BranchName>,
        content: Vec<u8>,
        author: ObjectId,
    ) -> Result<CommitId, Error> {
        let branch_name = branch_name.into();

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
            author,
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
        let branch_name = branch_name.into();

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
        let branch_name = branch_name.into();

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
        let branch_name = branch_name.into();

        let object = self
            .get(object_id)
            .ok_or(Error::ObjectNotFound(object_id))?;

        let branch = object
            .branches
            .get(&branch_name)
            .ok_or(Error::BranchNotFound(branch_name))?;

        Ok(&branch.commits)
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
            branches: HashMap::new(),
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
        let branch_name = branch_name.into();
        let commit_id = commit.id();

        // Capture previous state BEFORE mutation for AllObjectUpdate
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
        if io
            .append_commit(object_id, &branch_name, commit.clone())
            .is_ok()
        {
            commit.stored_state = StoredState::Stored;
        }

        // Get mutable reference to update
        let object = self.get_mut(object_id).expect("validated above");

        // Create branch if needed
        let branch = object
            .branches
            .entry(branch_name)
            .or_insert_with(|| Branch {
                loaded_state: BranchLoadedState::AllCommits,
                ..Default::default()
            });

        // Update tips: remove any parents that are tips, add this commit as tip
        for parent in &commit.parents {
            branch.tips.remove(parent);
        }
        branch.tips.insert(commit_id);

        branch.commits.insert(commit_id, commit);

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
        if let Some(object) = self.get(object_id)
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
        tip_vec.sort_by_key(|id| commits.get(id).map(|c| c.timestamp).unwrap_or(0));
        tip_vec
    }

    /// Notify subscribers about a commit change - sends current frontier sorted by timestamp.
    fn notify_subscribers_of_commit(&mut self, object_id: ObjectId, branch_name: BranchName) {
        let key = (object_id, branch_name);
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
                self.subscription_outbox.push(SubscriptionUpdate {
                    subscription_id: sub_id,
                    object_id,
                    branch_name,
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
        let branch_name = branch_name.into();

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

        // Sync storage: set branch tails
        let _ = io.set_branch_tails(object_id, &branch_name, Some(tail_ids));

        // Sync storage: delete commits
        for commit_id in &commits_to_delete {
            let _ = io.delete_commit(object_id, &branch_name, *commit_id);
        }

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
