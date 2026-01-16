use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;
use std::sync::RwLock;

use bytes::Bytes;

use crate::commit::{Commit, CommitId};
use crate::listener::ListenerError;
use crate::object::{Object, ObjectId};
use crate::sql::row_buffer::RowDescriptor;
use crate::storage::{Environment, MemoryEnvironment};
use crate::sync::StorageRequest;

/// Record of an object that changed during this pass.
/// Used by SyncEngine to know which objects need to be pushed upstream.
#[derive(Debug, Clone)]
pub struct ObjectChange {
    /// The object that changed.
    pub object_id: ObjectId,
    /// The branch that changed.
    pub branch: String,
    /// Timestamp of the change (for debounce tracking).
    pub timestamp: u64,
}

/// Callback type for when commits are applied to an object.
///
/// This callback is invoked after `apply_commits` adds commits to an object's branch.
/// It receives the object ID, branch name, and the commits that were applied.
///
/// This is used by the Database layer to be notified when sync applies commits,
/// allowing it to update query graphs and column change metadata.
///
/// Single-threaded: no Send/Sync bounds. The sync layer is single-threaded on all platforms.
pub type CommitsAppliedCallback = Rc<dyn Fn(ObjectId, &str, &[Commit])>;

/// Generate a new UUIDv7 as ObjectId.
#[cfg(not(feature = "wasm"))]
pub fn generate_object_id() -> ObjectId {
    ObjectId::new(uuid::Uuid::now_v7().as_u128())
}

/// Generate a new UUIDv7 as ObjectId (WASM version using web-time).
#[cfg(feature = "wasm")]
pub fn generate_object_id() -> ObjectId {
    use uuid::{NoContext, Timestamp, Uuid};
    let now = web_time::SystemTime::now();
    let duration = now
        .duration_since(web_time::UNIX_EPOCH)
        .expect("time went backwards");
    let ts = Timestamp::from_unix(NoContext, duration.as_secs(), duration.subsec_nanos());
    ObjectId::new(Uuid::new_v7(ts).as_u128())
}

/// A local node managing multiple objects.
///
/// LocalNode is primarily an in-memory store for objects. Storage persistence
/// is handled by the driver via the outbox system (StorageRequest).
///
/// The optional Environment reference is only needed for:
/// - `load_object`: loading objects from storage at startup
/// - Legacy compatibility with code that accesses env directly
///
/// For new code, use the driver pattern where the driver owns storage and
/// uses `restore_object` to populate LocalNode.
pub struct LocalNode {
    objects: RwLock<BTreeMap<ObjectId, Rc<RwLock<Object>>>>,
    /// Optional environment for legacy load_object support.
    /// New code should use restore_object instead.
    env: Option<Rc<dyn Environment>>,
    /// Optional callback invoked when commits are applied via `apply_commits`.
    /// Used by Database to be notified of sync-applied commits.
    /// Single-threaded: uses RefCell on all platforms.
    on_commits_applied: RefCell<Option<CommitsAppliedCallback>>,
    /// Pending storage requests (for inbox/outbox architecture).
    /// When using SyncEngine, these are drained into outboxes.
    /// For standalone use, call `drain_storage_requests()` and execute them.
    pending_storage: RefCell<Vec<StorageRequest>>,
    /// Objects that changed during this pass (for sync).
    /// SyncEngine drains these to know which objects to push upstream.
    changed_objects: RefCell<Vec<ObjectChange>>,
    /// Pending load requests (for lazy loading).
    /// When an object is needed but not in memory, add a request here.
    /// SyncEngine drains these into storage outbox.
    pending_load_requests: RefCell<Vec<LoadRequest>>,
}

/// A request to load an object from storage.
#[derive(Debug, Clone)]
pub struct LoadRequest {
    /// Object ID to load.
    pub object_id: ObjectId,
    /// Branch to load.
    pub branch: String,
}

impl Default for LocalNode {
    fn default() -> Self {
        Self::in_memory()
    }
}

impl std::fmt::Debug for LocalNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocalNode")
            .field("objects", &self.objects.read().unwrap().len())
            .finish()
    }
}

impl LocalNode {
    /// Create a new LocalNode with the given environment.
    ///
    /// The environment is used for `load_object` to read from storage.
    /// For new code using the driver pattern, consider using `new_without_env`
    /// and `restore_object` instead.
    pub fn new(env: Rc<dyn Environment>) -> Self {
        LocalNode {
            objects: RwLock::new(BTreeMap::new()),
            env: Some(env),
            on_commits_applied: RefCell::new(None),
            pending_storage: RefCell::new(Vec::new()),
            changed_objects: RefCell::new(Vec::new()),
            pending_load_requests: RefCell::new(Vec::new()),
        }
    }

    /// Create a new LocalNode without environment.
    ///
    /// Use this when the driver owns storage and will use `restore_object`
    /// to populate the node. Storage writes go through outboxes.
    pub fn new_without_env() -> Self {
        LocalNode {
            objects: RwLock::new(BTreeMap::new()),
            env: None,
            on_commits_applied: RefCell::new(None),
            pending_storage: RefCell::new(Vec::new()),
            changed_objects: RefCell::new(Vec::new()),
            pending_load_requests: RefCell::new(Vec::new()),
        }
    }

    /// Drain pending storage requests.
    ///
    /// Called by SyncEngine during `pass()` to collect storage operations.
    /// Also used by tests to execute storage directly.
    pub fn drain_storage_requests(&self) -> Vec<StorageRequest> {
        std::mem::take(&mut *self.pending_storage.borrow_mut())
    }

    /// Queue a storage request.
    fn queue_storage(&self, request: StorageRequest) {
        self.pending_storage.borrow_mut().push(request);
    }

    /// Drain changed objects (for sync).
    ///
    /// Called by SyncEngine during `pass()` to collect objects that need pushing.
    pub fn drain_changed_objects(&self) -> Vec<ObjectChange> {
        std::mem::take(&mut *self.changed_objects.borrow_mut())
    }

    /// Record that an object changed (for sync tracking).
    fn record_change(&self, object_id: ObjectId, branch: &str, timestamp: u64) {
        self.changed_objects.borrow_mut().push(ObjectChange {
            object_id,
            branch: branch.to_string(),
            timestamp,
        });
    }

    /// Drain pending load requests (for lazy loading).
    ///
    /// Called by SyncEngine during `pass()` to collect objects that need loading.
    pub fn drain_load_requests(&self) -> Vec<LoadRequest> {
        std::mem::take(&mut *self.pending_load_requests.borrow_mut())
    }

    /// Request an object to be loaded from storage.
    ///
    /// This queues a load request that will be processed by the driver.
    /// Use this for lazy loading instead of calling `load_object` directly.
    pub fn request_load(&self, object_id: ObjectId, branch: &str) {
        // Don't request if already loaded
        if self.objects.read().unwrap().contains_key(&object_id) {
            return;
        }

        self.pending_load_requests.borrow_mut().push(LoadRequest {
            object_id,
            branch: branch.to_string(),
        });
    }

    /// Check if an object is loaded in memory.
    pub fn is_loaded(&self, object_id: ObjectId) -> bool {
        self.objects.read().unwrap().contains_key(&object_id)
    }

    /// Set a callback to be invoked when commits are applied via `apply_commits`.
    ///
    /// This is used by the Database layer to be notified when sync applies commits,
    /// allowing it to update query graphs and rebuild column change metadata.
    ///
    /// The callback receives (object_id, branch, commits) after commits are successfully applied.
    pub fn set_on_commits_applied(&self, callback: Option<CommitsAppliedCallback>) {
        *self.on_commits_applied.borrow_mut() = callback;
    }

    /// Create a new LocalNode with an in-memory environment.
    ///
    /// This creates a MemoryEnvironment for load_object support.
    /// For driver-owned storage, use `new_without_env()` instead.
    pub fn in_memory() -> Self {
        Self::new(Rc::new(MemoryEnvironment::new()))
    }

    /// Get the environment, if one was provided.
    ///
    /// Returns None if the node was created with `new_without_env()`.
    /// Prefer using the driver pattern with `restore_object` for new code.
    pub fn env(&self) -> Option<&Rc<dyn Environment>> {
        self.env.as_ref()
    }

    /// Load an object from the Environment by reading its commits and frontier.
    /// Returns the object ID if the object was found and loaded successfully.
    ///
    /// This is async because it reads from the Environment, which may be backed
    /// by IndexedDB or other async storage.
    ///
    /// Returns None if no environment is available (use `restore_object` instead).
    pub async fn load_object(
        &self,
        id: ObjectId,
        prefix: impl Into<String>,
        branch: &str,
    ) -> Option<ObjectId> {
        let env = self.env.as_ref()?;

        // Get frontier from Environment
        let frontier = env.get_frontier(id.into(), branch).await;
        if frontier.is_empty() {
            return None;
        }

        // Load all commits from the Environment first (without holding any locks)
        let mut to_load = frontier.clone();
        let mut loaded_ids = std::collections::HashSet::new();
        let mut commits = Vec::new();

        while let Some(commit_id) = to_load.pop() {
            if loaded_ids.contains(&commit_id) {
                continue;
            }
            loaded_ids.insert(commit_id);

            if let Some(commit) = env.get_commit(&commit_id).await {
                // Add parent commits to load queue
                for parent_id in &commit.parents {
                    if !loaded_ids.contains(parent_id) {
                        to_load.push(*parent_id);
                    }
                }
                commits.push(commit);
            }
        }

        // Create a new Object and add all commits (now synchronously)
        let object = Object::new(id, prefix);
        let branch_ref = object.branch_ref(branch)?;
        {
            let mut branch_guard = branch_ref.write().unwrap();

            // Restore all commits
            for commit in commits {
                branch_guard.restore_commit(commit);
            }

            // Set frontier explicitly from Environment
            branch_guard.set_frontier(frontier);
        }

        // Register the object
        self.objects
            .write()
            .unwrap()
            .insert(id, Rc::new(RwLock::new(object)));

        Some(id)
    }

    /// Restore an object from pre-loaded commits.
    ///
    /// This is the synchronous version of `load_object` - the caller (driver) is
    /// responsible for loading the frontier and commits from storage.
    ///
    /// Returns the object ID if restoration succeeded.
    pub fn restore_object(
        &self,
        id: ObjectId,
        prefix: impl Into<String>,
        branch: &str,
        frontier: Vec<CommitId>,
        commits: Vec<Commit>,
    ) -> Option<ObjectId> {
        if frontier.is_empty() {
            return None;
        }

        // Create a new Object and add all commits
        let object = Object::new(id, prefix);
        let branch_ref = object.branch_ref(branch)?;
        {
            let mut branch_guard = branch_ref.write().unwrap();

            // Restore all commits
            for commit in commits {
                branch_guard.restore_commit(commit);
            }

            // Set frontier explicitly
            branch_guard.set_frontier(frontier);
        }

        // Register the object
        self.objects
            .write()
            .unwrap()
            .insert(id, Rc::new(RwLock::new(object)));

        Some(id)
    }

    /// Create a new object with the given prefix. Returns the object ID.
    /// Uses internal mutability so it can be called with just &self.
    pub fn create_object(&self, prefix: impl Into<String>) -> ObjectId {
        let id = generate_object_id();
        let object = Object::new(id, prefix);
        self.objects
            .write()
            .unwrap()
            .insert(id, Rc::new(RwLock::new(object)));
        id
    }

    /// Create a new object with the given prefix and metadata. Returns the object ID.
    pub fn create_object_with_meta(
        &self,
        prefix: impl Into<String>,
        meta: std::collections::BTreeMap<String, String>,
    ) -> ObjectId {
        let id = generate_object_id();
        let object = Object::new_with_meta(id, prefix, Some(meta));
        self.objects
            .write()
            .unwrap()
            .insert(id, Rc::new(RwLock::new(object)));
        id
    }

    /// Create or get an object with a specific ID.
    /// If the object already exists, returns false. If created, returns true.
    /// Useful for testing and sync scenarios where object IDs are known.
    pub fn ensure_object(&self, id: ObjectId, prefix: impl Into<String>) -> bool {
        let mut objects = self.objects.write().unwrap();
        if let std::collections::btree_map::Entry::Vacant(e) = objects.entry(id) {
            let object = Object::new(id, prefix);
            e.insert(Rc::new(RwLock::new(object)));
            true
        } else {
            false
        }
    }

    /// Create or get an object with a specific ID and metadata.
    /// If the object already exists, returns false. If created, returns true.
    /// Useful for creating node-private objects that should not sync.
    pub fn ensure_object_with_meta(
        &self,
        id: ObjectId,
        prefix: impl Into<String>,
        meta: std::collections::BTreeMap<String, String>,
    ) -> bool {
        let mut objects = self.objects.write().unwrap();
        if let std::collections::btree_map::Entry::Vacant(e) = objects.entry(id) {
            let object = Object::new_with_meta(id, prefix, Some(meta));
            e.insert(Rc::new(RwLock::new(object)));
            true
        } else {
            false
        }
    }

    /// Get an object by ID.
    pub fn get_object(&self, id: ObjectId) -> Option<Rc<RwLock<Object>>> {
        self.objects.read().unwrap().get(&id).cloned()
    }

    // ========== Read API ==========

    /// Read content from the frontier of an object's branch.
    /// Returns None if the branch is empty or has multiple tips.
    pub fn read(
        &self,
        object_id: ObjectId,
        branch: &str,
    ) -> Result<Option<Vec<u8>>, ListenerError> {
        let obj_lock = self
            .objects
            .read()
            .unwrap()
            .get(&object_id)
            .cloned()
            .ok_or(ListenerError::NotFound)?;

        let obj = obj_lock.read().unwrap();
        Ok(obj.read_sync(branch))
    }

    // ========== Write API with Auto-Notify ==========

    /// Write content to an object's branch.
    /// Returns the new commit ID.
    pub fn write(
        &self,
        object_id: ObjectId,
        branch: &str,
        content: &[u8],
        author: &str,
        timestamp: u64,
    ) -> Result<CommitId, ListenerError> {
        self.write_with_meta(object_id, branch, content, author, timestamp, None)
    }

    /// Write content to an object's branch with optional metadata.
    /// Returns the new commit ID.
    ///
    /// The write is synchronous to in-memory state,
    /// but persistence to the Environment happens asynchronously in the background.
    pub fn write_with_meta(
        &self,
        object_id: ObjectId,
        branch: &str,
        content: &[u8],
        author: &str,
        timestamp: u64,
        meta: Option<std::collections::BTreeMap<String, String>>,
    ) -> Result<CommitId, ListenerError> {
        let obj_lock = self
            .objects
            .read()
            .unwrap()
            .get(&object_id)
            .cloned()
            .ok_or(ListenerError::NotFound)?;

        let obj = obj_lock.read().unwrap();
        let commit_id = obj.write_sync_with_meta(branch, content, author, timestamp, meta);

        // Collect data for storage
        let persist_data = if let Some(branch_ref) = obj.branch_ref(branch) {
            let branch_guard = branch_ref.read().unwrap();
            branch_guard.get_commit(&commit_id).map(|commit| {
                let frontier = branch_guard.frontier().to_vec();
                (commit.clone(), frontier)
            })
        } else {
            None
        };

        // Queue storage requests (driver will execute asynchronously)
        if let Some((commit, frontier)) = persist_data {
            self.queue_storage(StorageRequest::PutCommit { commit });
            self.queue_storage(StorageRequest::SetFrontier {
                object_id,
                branch: branch.to_string(),
                frontier,
            });
        }

        // Record change for sync
        self.record_change(object_id, branch, timestamp);

        Ok(commit_id)
    }

    /// Write content to an object's branch with per-column change tracking.
    ///
    /// This is like `write_with_meta` but also computes and stores per-column LWW
    /// metadata for proper merge behavior. The descriptor is used to determine
    /// which columns changed between the parent commit(s) and this new commit.
    ///
    /// Returns the new commit ID.
    pub fn write_with_tracking(
        &self,
        object_id: ObjectId,
        branch: &str,
        content: &[u8],
        author: &str,
        timestamp: u64,
        descriptor: &RowDescriptor,
    ) -> Result<CommitId, ListenerError> {
        let obj_lock = self
            .objects
            .read()
            .unwrap()
            .get(&object_id)
            .cloned()
            .ok_or(ListenerError::NotFound)?;

        let obj = obj_lock.read().unwrap();
        let commit_id =
            obj.write_sync_with_tracking(branch, content, author, timestamp, None, descriptor);

        // Collect data for storage
        let persist_data = if let Some(branch_ref) = obj.branch_ref(branch) {
            let branch_guard = branch_ref.read().unwrap();
            branch_guard.get_commit(&commit_id).map(|commit| {
                let frontier = branch_guard.frontier().to_vec();
                (commit.clone(), frontier)
            })
        } else {
            None
        };

        // Queue storage requests (driver will execute asynchronously)
        if let Some((commit, frontier)) = persist_data {
            self.queue_storage(StorageRequest::PutCommit { commit });
            self.queue_storage(StorageRequest::SetFrontier {
                object_id,
                branch: branch.to_string(),
                frontier,
            });
        }

        // Record change for sync
        self.record_change(object_id, branch, timestamp);

        Ok(commit_id)
    }

    /// Get the frontier commit IDs for an object's branch.
    pub fn frontier(
        &self,
        object_id: ObjectId,
        branch: &str,
    ) -> Result<Option<Vec<CommitId>>, ListenerError> {
        let obj_lock = self
            .objects
            .read()
            .unwrap()
            .get(&object_id)
            .cloned()
            .ok_or(ListenerError::NotFound)?;

        let obj = obj_lock.read().unwrap();
        Ok(obj.frontier(branch))
    }

    /// Truncate history at the given commit ID and prune older commits.
    /// The commit must be an ancestor of all frontier commits.
    /// After truncation:
    /// - All commits before the truncation point are removed from memory
    /// - Future commits with parents before the truncation point will be rejected
    ///
    /// Returns the number of commits pruned.
    pub fn truncate_at(
        &self,
        object_id: ObjectId,
        branch: &str,
        commit_id: CommitId,
    ) -> Result<usize, ListenerError> {
        let obj_lock = self
            .objects
            .read()
            .unwrap()
            .get(&object_id)
            .cloned()
            .ok_or(ListenerError::NotFound)?;

        let obj = obj_lock.read().unwrap();
        let branch_ref = obj.branch_ref(branch).ok_or(ListenerError::NotFound)?;

        let mut branch_guard = branch_ref.write().unwrap();
        branch_guard
            .truncate_at(commit_id)
            .map_err(|e| ListenerError::StorageError(e.to_string()))
    }

    // ========== Helper: Load content for a commit ==========

    /// Load content for a commit.
    pub fn load_content(
        &self,
        object_id: ObjectId,
        branch: &str,
        commit_id: &CommitId,
    ) -> Option<Bytes> {
        let obj_lock = self.objects.read().unwrap().get(&object_id).cloned()?;
        let obj = obj_lock.read().unwrap();
        let branch = obj.branch(branch)?;
        let commit = branch.get_commit(commit_id)?;
        Some(Bytes::copy_from_slice(&commit.content))
    }

    // ========== Sync API ==========

    /// Apply commits received from sync (from other peers).
    ///
    /// This method:
    /// 1. Creates the object if it doesn't exist
    /// 2. Adds all commits to the branch
    /// 3. Persists commits and frontier to storage
    /// 4. Invokes the on_commits_applied callback if set
    ///
    /// Returns the new frontier after applying commits.
    pub fn apply_commits(
        &self,
        object_id: ObjectId,
        branch: &str,
        commits: Vec<crate::commit::Commit>,
    ) -> Vec<CommitId> {
        use crate::commit::Commit;

        if commits.is_empty() {
            // Return current frontier if no commits to apply
            return self
                .frontier(object_id, branch)
                .ok()
                .flatten()
                .unwrap_or_default();
        }

        // Get or create the object
        let obj_lock = {
            let objects = self.objects.read().unwrap();
            if let Some(obj) = objects.get(&object_id).cloned() {
                obj
            } else {
                drop(objects);
                // Create new object with empty prefix
                let object = Object::new(object_id, "");
                let arc = Rc::new(RwLock::new(object));
                self.objects.write().unwrap().insert(object_id, arc.clone());
                arc
            }
        };

        let obj = obj_lock.read().unwrap();
        let branch_ref = obj.branch_ref(branch).expect("branch should exist");

        // Collect commits for persistence
        let mut commits_to_persist: Vec<Commit> = Vec::new();

        // Add all commits to the branch
        {
            let mut branch_guard = branch_ref.write().unwrap();
            for commit in commits {
                // Only add if we don't already have this commit
                let commit_id = commit.compute_id();
                if branch_guard.get_commit(&commit_id).is_none() {
                    commits_to_persist.push(commit.clone());
                    // Use add_commit to properly update frontier
                    let _ = branch_guard.add_commit(commit);
                }
            }
        }

        // Get the new frontier
        let frontier = {
            let branch_guard = branch_ref.read().unwrap();
            branch_guard.frontier().to_vec()
        };

        // Drop the object read lock before calling the callback.
        // The callback may need to acquire its own lock on the same object,
        // and std::sync::RwLock is not guaranteed to be re-entrant.
        drop(obj);

        // Invoke the commits-applied callback if set (used by Database for query graph updates)
        if !commits_to_persist.is_empty()
            && let Some(callback) = self.on_commits_applied.borrow().as_ref()
        {
            callback(object_id, branch, &commits_to_persist);
        }

        // Queue storage requests (driver will execute asynchronously)
        if !commits_to_persist.is_empty() {
            for commit in commits_to_persist {
                self.queue_storage(StorageRequest::PutCommit { commit });
            }
            self.queue_storage(StorageRequest::SetFrontier {
                object_id,
                branch: branch.to_string(),
                frontier: frontier.clone(),
            });
        }

        frontier
    }

    /// Check if we have a commit by ID.
    pub fn has_commit(&self, object_id: ObjectId, branch: &str, commit_id: &CommitId) -> bool {
        let obj_lock = match self.objects.read().unwrap().get(&object_id).cloned() {
            Some(o) => o,
            None => return false,
        };
        let obj = obj_lock.read().unwrap();
        let branch_ref = match obj.branch_ref(branch) {
            Some(b) => b,
            None => return false,
        };
        let branch_guard = branch_ref.read().unwrap();
        branch_guard.get_commit(commit_id).is_some()
    }

    /// Get a commit by ID.
    pub fn get_commit(
        &self,
        object_id: ObjectId,
        branch: &str,
        commit_id: &CommitId,
    ) -> Option<crate::commit::Commit> {
        let obj_lock = self.objects.read().unwrap().get(&object_id).cloned()?;
        let obj = obj_lock.read().unwrap();
        let branch_ref = obj.branch_ref(branch)?;
        let branch_guard = branch_ref.read().unwrap();
        branch_guard.get_commit(commit_id).cloned()
    }
}

// Tests have been moved to tests/node.rs
