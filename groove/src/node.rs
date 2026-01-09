use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use bytes::Bytes;

use crate::commit::CommitId;
use crate::listener::{ListenerId, ListenerError, ObjectCallback, ObjectKey, ObjectListenerRegistry, ObjectState};
use crate::object::{Object, ObjectId};
use crate::storage::{Environment, MemoryEnvironment};

/// Spawn an async task for background persistence.
/// In WASM, uses spawn_local. In native, uses block_on (for now).
#[cfg(target_arch = "wasm32")]
fn spawn_persist<F>(future: F)
where
    F: std::future::Future<Output = ()> + 'static,
{
    wasm_bindgen_futures::spawn_local(future);
}

#[cfg(not(target_arch = "wasm32"))]
fn spawn_persist<F>(future: F)
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    // For native, we run synchronously since MemoryEnvironment is instant.
    // This ensures tests can verify persistence immediately.
    // Real async backends (RocksDB, SQLite) would use a proper async runtime.
    futures::executor::block_on(future);
}

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

/// A local node managing multiple objects with listener support.
///
/// The node owns the Environment (storage) and ObjectListenerRegistry, providing methods
/// for subscribing to objects and reading/writing with automatic listener notification.
///
/// Uses internal mutability so that objects can be created and written to
/// without requiring exclusive access to the node.
pub struct LocalNode {
    objects: RwLock<BTreeMap<ObjectId, Arc<RwLock<Object>>>>,
    listeners: ObjectListenerRegistry,
    env: Arc<dyn Environment>,
}

impl Default for LocalNode {
    fn default() -> Self {
        Self::new(Arc::new(MemoryEnvironment::new()))
    }
}

impl std::fmt::Debug for LocalNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocalNode")
            .field("objects", &self.objects.read().unwrap().len())
            .field("listeners", &self.listeners)
            .finish()
    }
}

impl LocalNode {
    /// Create a new LocalNode with the given environment.
    pub fn new(env: Arc<dyn Environment>) -> Self {
        LocalNode {
            objects: RwLock::new(BTreeMap::new()),
            listeners: ObjectListenerRegistry::new(),
            env,
        }
    }

    /// Create a new LocalNode with an in-memory environment (for testing).
    pub fn in_memory() -> Self {
        Self::default()
    }

    /// Get the environment.
    pub fn env(&self) -> &Arc<dyn Environment> {
        &self.env
    }

    /// Load an object from the Environment by reading its commits and frontier.
    /// Returns the object ID if the object was found and loaded successfully.
    ///
    /// This is async because it reads from the Environment, which may be backed
    /// by IndexedDB or other async storage.
    pub async fn load_object(&self, id: ObjectId, prefix: impl Into<String>, branch: &str) -> Option<ObjectId> {
        // Get frontier from Environment
        let frontier = self.env.get_frontier(id.into(), branch).await;
        if frontier.is_empty() {
            return None;
        }

        // Create a new Object
        let object = Object::new(id, prefix);

        // Load all commits from the Environment and add them to the Object
        let branch_ref = object.branch_ref(branch)?;
        {
            let mut branch_guard = branch_ref.write().unwrap();

            // Walk the commit graph starting from frontier
            let mut to_load = frontier.clone();
            let mut loaded = std::collections::HashSet::new();

            while let Some(commit_id) = to_load.pop() {
                if loaded.contains(&commit_id) {
                    continue;
                }
                loaded.insert(commit_id);

                if let Some(commit) = self.env.get_commit(&commit_id).await {
                    // Add parent commits to load queue
                    for parent_id in &commit.parents {
                        if !loaded.contains(parent_id) {
                            to_load.push(*parent_id);
                        }
                    }
                    // Restore commit without updating frontier
                    branch_guard.restore_commit(commit);
                }
            }

            // Set frontier explicitly from Environment
            branch_guard.set_frontier(frontier);
        }

        // Register the object
        self.objects.write().unwrap().insert(id, Arc::new(RwLock::new(object)));

        Some(id)
    }

    /// Create a new object with the given prefix. Returns the object ID.
    /// Uses internal mutability so it can be called with just &self.
    pub fn create_object(&self, prefix: impl Into<String>) -> ObjectId {
        let id = generate_object_id();
        let object = Object::new(id, prefix);
        self.objects.write().unwrap().insert(id, Arc::new(RwLock::new(object)));
        id
    }

    /// Get an object by ID.
    pub fn get_object(&self, id: ObjectId) -> Option<Arc<RwLock<Object>>> {
        self.objects.read().unwrap().get(&id).cloned()
    }

    /// Get a reference to the listener registry.
    pub fn listeners(&self) -> &ObjectListenerRegistry {
        &self.listeners
    }

    // ========== Subscription API ==========

    /// Subscribe to an object's branch with a callback.
    /// The callback is called immediately with current state (if any),
    /// and then synchronously on every subsequent write.
    /// Returns a listener ID that can be used to unsubscribe.
    pub fn subscribe(
        &self,
        object_id: ObjectId,
        branch: &str,
        callback: ObjectCallback,
    ) -> Result<ListenerId, ListenerError> {
        let obj_lock = self
            .objects
            .read()
            .unwrap()
            .get(&object_id)
            .cloned()
            .ok_or(ListenerError::NotFound)?;

        let key = ObjectKey::new(object_id, branch);

        // Verify branch exists
        let obj = obj_lock.read().unwrap();
        let branch_ref = obj
            .branch_ref(branch)
            .ok_or(ListenerError::BranchNotFound)?;

        // Get current tips
        let tips = {
            let b = branch_ref.read().unwrap();
            b.frontier().to_vec()
        };

        // Ensure initial state is set (only if not already set)
        // This must happen BEFORE subscribe so the callback gets called with initial state
        self.listeners.ensure_initial_state(&key, self.env.clone(), tips, branch_ref);

        // Subscribe with the callback - it will be called immediately with current state
        let id = self.listeners.subscribe(key, self.env.clone(), callback);

        Ok(id)
    }

    /// Unsubscribe a listener by ID.
    pub fn unsubscribe(&self, object_id: ObjectId, branch: &str, listener_id: ListenerId) -> bool {
        let key = ObjectKey::new(object_id, branch);
        self.listeners.unsubscribe(&key, listener_id)
    }

    /// Get current cached state for an object's branch.
    pub fn get_current_state(&self, object_id: ObjectId, branch: &str) -> Option<Arc<ObjectState>> {
        let key = ObjectKey::new(object_id, branch);
        self.listeners.get_current(&key)
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

    /// Write content to an object's branch and notify all listeners.
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

    /// Write content to an object's branch with optional metadata and notify all listeners.
    /// Returns the new commit ID.
    ///
    /// The write is synchronous to in-memory state (and listeners are notified immediately),
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

        // Collect data for async persist
        let persist_data = if let Some(branch_ref) = obj.branch_ref(branch) {
            let branch_guard = branch_ref.read().unwrap();
            branch_guard.get_commit(&commit_id).map(|commit| {
                let frontier = branch_guard.frontier().to_vec();
                (commit.clone(), frontier)
            })
        } else {
            None
        };

        // Notify listeners synchronously (before persist, so UI updates immediately)
        self.notify_listeners(object_id, branch, &obj);

        // Persist asynchronously in background
        if let Some((commit, frontier)) = persist_data {
            let env = self.env.clone();
            let branch = branch.to_string();
            spawn_persist(async move {
                env.put_commit(&commit).await;
                env.set_frontier(object_id.into(), &branch, &frontier).await;
            });
        }

        Ok(commit_id)
    }

    /// Get the frontier commit IDs for an object's branch.
    pub fn frontier(&self, object_id: ObjectId, branch: &str) -> Result<Option<Vec<CommitId>>, ListenerError> {
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
        let branch_ref = obj
            .branch_ref(branch)
            .ok_or(ListenerError::NotFound)?;

        let mut branch_guard = branch_ref.write().unwrap();
        branch_guard
            .truncate_at(commit_id)
            .map_err(|e| ListenerError::StorageError(e.to_string()))
    }

    /// Internal: notify listeners for a branch update.
    fn notify_listeners(&self, object_id: ObjectId, branch: &str, obj: &Object) {
        let key = ObjectKey::new(object_id, branch);

        if let Some(branch_ref) = obj.branch_ref(branch) {
            let tips = {
                let b = branch_ref.read().unwrap();
                b.frontier().to_vec()
            };
            self.listeners.notify(&key, tips, branch_ref);
        }
    }

    /// Notify all listeners for an object.
    /// Use this after external changes to the object (e.g., sync from peers).
    pub fn notify_object(&self, object_id: ObjectId) {
        let obj_lock = match self.objects.read().unwrap().get(&object_id).cloned() {
            Some(o) => o,
            None => return,
        };

        let obj = obj_lock.read().unwrap();

        // Find all active listeners for this object
        let keys = self.listeners.keys_for_object(object_id);

        for key in keys {
            if let Some(branch_ref) = obj.branch_ref(&key.branch) {
                let tips = {
                    let b = branch_ref.read().unwrap();
                    b.frontier().to_vec()
                };
                self.listeners.notify(&key, tips, branch_ref);
            }
        }
    }

    // ========== Helper: Load content for a commit ==========

    /// Load content for a commit.
    pub fn load_content(&self, object_id: ObjectId, branch: &str, commit_id: &CommitId) -> Option<Bytes> {
        let obj_lock = self.objects.read().unwrap().get(&object_id).cloned()?;
        let obj = obj_lock.read().unwrap();
        let branch = obj.branch(branch)?;
        let commit = branch.get_commit(commit_id)?;
        Some(Bytes::copy_from_slice(&commit.content))
    }
}

// Tests have been moved to tests/node.rs
