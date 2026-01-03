use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use bytes::Bytes;
use futures::io::AsyncRead;

use crate::commit::CommitId;
use crate::listener::{ListenerId, ListenerError, ObjectCallback, ObjectKey, ObjectListenerRegistry, ObjectState};
use crate::object::Object;
use crate::storage::{Environment, MemoryEnvironment};

/// Generate a new UUIDv7 as u128.
#[cfg(not(feature = "wasm"))]
pub fn generate_object_id() -> u128 {
    uuid::Uuid::now_v7().as_u128()
}

/// Generate a new UUIDv7 as u128 (WASM version using web-time).
#[cfg(feature = "wasm")]
pub fn generate_object_id() -> u128 {
    use uuid::{NoContext, Timestamp, Uuid};
    let now = web_time::SystemTime::now();
    let duration = now
        .duration_since(web_time::UNIX_EPOCH)
        .expect("time went backwards");
    let ts = Timestamp::from_unix(NoContext, duration.as_secs(), duration.subsec_nanos());
    Uuid::new_v7(ts).as_u128()
}

/// A local node managing multiple objects with listener support.
///
/// The node owns the Environment (storage) and ObjectListenerRegistry, providing methods
/// for subscribing to objects and reading/writing with automatic listener notification.
///
/// Uses internal mutability so that objects can be created and written to
/// without requiring exclusive access to the node.
pub struct LocalNode {
    objects: RwLock<BTreeMap<u128, Arc<RwLock<Object>>>>,
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

    /// Create a new object with the given prefix. Returns the object ID.
    /// Uses internal mutability so it can be called with just &self.
    pub fn create_object(&self, prefix: impl Into<String>) -> u128 {
        let id = generate_object_id();
        let object = Object::new(id, prefix);
        self.objects.write().unwrap().insert(id, Arc::new(RwLock::new(object)));
        id
    }

    /// Get an object by ID.
    pub fn get_object(&self, id: u128) -> Option<Arc<RwLock<Object>>> {
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
        object_id: u128,
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
    pub fn unsubscribe(&self, object_id: u128, branch: &str, listener_id: ListenerId) -> bool {
        let key = ObjectKey::new(object_id, branch);
        self.listeners.unsubscribe(&key, listener_id)
    }

    /// Get current cached state for an object's branch.
    pub fn get_current_state(&self, object_id: u128, branch: &str) -> Option<Arc<ObjectState>> {
        let key = ObjectKey::new(object_id, branch);
        self.listeners.get_current(&key)
    }

    // ========== Read API ==========

    /// Read content from the frontier of an object's branch (sync).
    /// Returns None if the branch is empty, has multiple tips, or content is chunked.
    pub fn read_sync(
        &self,
        object_id: u128,
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

    /// Read content from the frontier of an object's branch (async).
    /// Loads chunked content from storage if needed.
    pub async fn read(
        &self,
        object_id: u128,
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
        Ok(obj.read(branch, self.env.as_ref()).await)
    }

    // Note: Streaming read methods are available directly on Object.
    // Due to lifetime constraints with Arc<RwLock<Object>>, streaming
    // must be done by obtaining the object reference directly:
    //   let obj = node.get_object(id)?;
    //   let guard = obj.read().unwrap();
    //   let stream = guard.read_stream(branch, env);

    // ========== Write API with Auto-Notify ==========

    /// Write content to an object's branch and notify all listeners.
    /// Returns the new commit ID.
    /// Panics if content exceeds INLINE_THRESHOLD.
    pub fn write_sync(
        &self,
        object_id: u128,
        branch: &str,
        content: &[u8],
        author: &str,
        timestamp: u64,
    ) -> Result<CommitId, ListenerError> {
        let obj_lock = self
            .objects
            .read()
            .unwrap()
            .get(&object_id)
            .cloned()
            .ok_or(ListenerError::NotFound)?;

        let obj = obj_lock.read().unwrap();
        let commit_id = obj.write_sync(branch, content, author, timestamp);

        // Notify listeners synchronously
        self.notify_listeners(object_id, branch, &obj);

        Ok(commit_id)
    }

    /// Write content to an object's branch (async) and notify all listeners.
    /// Automatically chunks content that exceeds INLINE_THRESHOLD.
    pub async fn write(
        &self,
        object_id: u128,
        branch: &str,
        content: &[u8],
        author: &str,
        timestamp: u64,
    ) -> Result<CommitId, ListenerError> {
        let obj_lock = self
            .objects
            .read()
            .unwrap()
            .get(&object_id)
            .cloned()
            .ok_or(ListenerError::NotFound)?;

        let commit_id = {
            let obj = obj_lock.read().unwrap();
            obj.write(branch, content, author, timestamp, self.env.as_ref())
                .await
        };

        // Notify listeners synchronously
        {
            let obj = obj_lock.read().unwrap();
            self.notify_listeners(object_id, branch, &obj);
        }

        Ok(commit_id)
    }

    /// Write content from an async reader to an object's branch.
    /// Chunks the content as it streams in.
    pub async fn write_stream<R: AsyncRead + Unpin>(
        &self,
        object_id: u128,
        branch: &str,
        reader: R,
        author: &str,
        timestamp: u64,
    ) -> Result<CommitId, ListenerError> {
        let obj_lock = self
            .objects
            .read()
            .unwrap()
            .get(&object_id)
            .cloned()
            .ok_or(ListenerError::NotFound)?;

        let commit_id = {
            let obj = obj_lock.read().unwrap();
            obj.write_stream(branch, reader, author, timestamp, self.env.as_ref())
                .await
                .map_err(|e| ListenerError::StorageError(e.to_string()))?
        };

        // Notify listeners synchronously
        {
            let obj = obj_lock.read().unwrap();
            self.notify_listeners(object_id, branch, &obj);
        }

        Ok(commit_id)
    }

    /// Get the frontier commit IDs for an object's branch.
    pub fn frontier(&self, object_id: u128, branch: &str) -> Result<Option<Vec<CommitId>>, ListenerError> {
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

    /// Internal: notify listeners for a branch update.
    fn notify_listeners(&self, object_id: u128, branch: &str, obj: &Object) {
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
    pub fn notify_object(&self, object_id: u128) {
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

    /// Load content for a commit (handles both inline and chunked).
    pub async fn load_content(&self, object_id: u128, branch: &str, commit_id: &CommitId) -> Option<Bytes> {
        let obj_lock = self.objects.read().unwrap().get(&object_id).cloned()?;
        let obj = obj_lock.read().unwrap();
        let branch = obj.branch(branch)?;
        let commit = branch.get_commit(commit_id)?;

        match &commit.content {
            crate::storage::ContentRef::Inline(data) => Some(Bytes::copy_from_slice(&data)),
            crate::storage::ContentRef::Chunked(hashes) => {
                let hashes = hashes.clone();
                drop(branch);
                drop(obj);

                let mut result = Vec::new();
                for hash in hashes {
                    let chunk = self.env.get_chunk(&hash).await?;
                    result.extend_from_slice(&chunk);
                }
                Some(Bytes::from(result))
            }
        }
    }
}

// Tests have been moved to tests/node.rs
