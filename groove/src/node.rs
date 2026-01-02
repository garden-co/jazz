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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn local_node_create_and_get_objects() {
        let node = LocalNode::in_memory();

        let id1 = node.create_object("chat");
        let id2 = node.create_object("message");

        assert!(node.get_object(id1).is_some());
        assert!(node.get_object(id2).is_some());
        assert!(node.get_object(999).is_none());

        assert_eq!(node.get_object(id1).unwrap().read().unwrap().prefix, "chat");
        assert_eq!(
            node.get_object(id2).unwrap().read().unwrap().prefix,
            "message"
        );
    }

    #[test]
    fn uuidv7_is_unique_and_ordered() {
        let id1 = generate_object_id();
        std::thread::sleep(std::time::Duration::from_millis(1));
        let id2 = generate_object_id();

        assert_ne!(id1, id2);
        assert!(id2 > id1);
    }

    #[test]
    fn local_node_uses_uuidv7() {
        let node = LocalNode::in_memory();

        let id1 = node.create_object("test1");
        std::thread::sleep(std::time::Duration::from_millis(1));
        let id2 = node.create_object("test2");

        assert!(id2 > id1);
        assert!(id1 > 1000);
    }

    #[test]
    fn subscribe_to_empty_object() {
        let node = LocalNode::in_memory();
        let id = node.create_object("test");

        let call_count = Arc::new(AtomicUsize::new(0));
        let call_count_clone = call_count.clone();

        let _listener_id = node.subscribe(id, "main", Box::new(move |state| {
            call_count_clone.fetch_add(1, Ordering::SeqCst);
            // Empty branch has no tips
            assert!(state.tips.is_empty());
        })).unwrap();

        // Should be called once for initial state
        assert_eq!(call_count.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn subscribe_nonexistent_object_errors() {
        let node = LocalNode::in_memory();

        let result = node.subscribe(999, "main", Box::new(|_| {}));
        assert!(result.is_err());
    }

    #[test]
    fn subscribe_nonexistent_branch_errors() {
        let node = LocalNode::in_memory();
        let id = node.create_object("test");

        let result = node.subscribe(id, "nonexistent", Box::new(|_| {}));
        assert!(result.is_err());
    }

    #[test]
    fn write_sync_notifies_listener() {
        let node = LocalNode::in_memory();
        let id = node.create_object("test");

        let call_count = Arc::new(AtomicUsize::new(0));
        let tip_counts = Arc::new(RwLock::new(Vec::new()));
        let call_count_clone = call_count.clone();
        let tip_counts_clone = tip_counts.clone();

        let _listener_id = node.subscribe(id, "main", Box::new(move |state| {
            call_count_clone.fetch_add(1, Ordering::SeqCst);
            tip_counts_clone.write().unwrap().push(state.tips.len());
        })).unwrap();

        // Initial call
        assert_eq!(call_count.load(Ordering::SeqCst), 1);
        assert_eq!(*tip_counts.read().unwrap(), vec![0]); // empty initially

        // Write through node (auto-notifies)
        node.write_sync(id, "main", b"hello", "alice", 1000).unwrap();

        // Callback should be called synchronously
        assert_eq!(call_count.load(Ordering::SeqCst), 2);
        assert_eq!(*tip_counts.read().unwrap(), vec![0, 1]); // now has 1 tip

        // Write again
        node.write_sync(id, "main", b"world", "alice", 2000).unwrap();

        assert_eq!(call_count.load(Ordering::SeqCst), 3);
        assert_eq!(*tip_counts.read().unwrap(), vec![0, 1, 1]); // still 1 tip
    }

    #[test]
    fn write_without_subscriber() {
        let node = LocalNode::in_memory();
        let id = node.create_object("test");

        // Write without subscribing - should not error
        let commit_id = node.write_sync(id, "main", b"hello", "alice", 1000).unwrap();

        // Now subscribe and verify content in callback
        let received_tips = Arc::new(RwLock::new(Vec::new()));
        let received_tips_clone = received_tips.clone();

        let _listener_id = node.subscribe(id, "main", Box::new(move |state| {
            received_tips_clone.write().unwrap().extend(state.tips.clone());
        })).unwrap();

        let tips = received_tips.read().unwrap();
        assert_eq!(tips.len(), 1);
        assert_eq!(tips[0], commit_id);
    }

    #[test]
    fn notify_object() {
        let node = LocalNode::in_memory();
        let id = node.create_object("test");

        let call_count = Arc::new(AtomicUsize::new(0));
        let call_count_clone = call_count.clone();

        let _listener_id = node.subscribe(id, "main", Box::new(move |_state| {
            call_count_clone.fetch_add(1, Ordering::SeqCst);
        })).unwrap();

        assert_eq!(call_count.load(Ordering::SeqCst), 1); // initial

        // Write directly to object (bypassing node's write method)
        {
            let obj_lock = node.get_object(id).unwrap();
            let obj = obj_lock.read().unwrap();
            obj.write_sync("main", b"direct write", "alice", 1000);
        }

        // Listener not notified yet
        assert_eq!(call_count.load(Ordering::SeqCst), 1);

        // Now notify
        node.notify_object(id);

        // Listener should be notified
        assert_eq!(call_count.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn multiple_subscribers_all_notified() {
        let node = LocalNode::in_memory();
        let id = node.create_object("test");

        let count1 = Arc::new(AtomicUsize::new(0));
        let count2 = Arc::new(AtomicUsize::new(0));
        let count1_clone = count1.clone();
        let count2_clone = count2.clone();

        let _id1 = node.subscribe(id, "main", Box::new(move |_| {
            count1_clone.fetch_add(1, Ordering::SeqCst);
        })).unwrap();
        let _id2 = node.subscribe(id, "main", Box::new(move |_| {
            count2_clone.fetch_add(1, Ordering::SeqCst);
        })).unwrap();

        // Both called for initial state
        assert_eq!(count1.load(Ordering::SeqCst), 1);
        assert_eq!(count2.load(Ordering::SeqCst), 1);

        // Write
        node.write_sync(id, "main", b"hello", "alice", 1000).unwrap();

        // Both should be notified
        assert_eq!(count1.load(Ordering::SeqCst), 2);
        assert_eq!(count2.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn read_write_roundtrip() {
        let node = LocalNode::in_memory();
        let id = node.create_object("test");

        node.write_sync(id, "main", b"hello world", "alice", 1000).unwrap();

        let content = node.read_sync(id, "main").unwrap().unwrap();
        assert_eq!(content, b"hello world");
    }

    #[test]
    fn unsubscribe_stops_notifications() {
        let node = LocalNode::in_memory();
        let id = node.create_object("test");

        let call_count = Arc::new(AtomicUsize::new(0));
        let call_count_clone = call_count.clone();

        let listener_id = node.subscribe(id, "main", Box::new(move |_| {
            call_count_clone.fetch_add(1, Ordering::SeqCst);
        })).unwrap();

        assert_eq!(call_count.load(Ordering::SeqCst), 1); // initial

        node.write_sync(id, "main", b"hello", "alice", 1000).unwrap();
        assert_eq!(call_count.load(Ordering::SeqCst), 2);

        // Unsubscribe
        assert!(node.unsubscribe(id, "main", listener_id));

        // Write again - should not notify
        node.write_sync(id, "main", b"world", "alice", 2000).unwrap();
        assert_eq!(call_count.load(Ordering::SeqCst), 2); // still 2
    }

    #[test]
    fn callback_called_synchronously() {
        use std::sync::atomic::AtomicBool;

        let node = LocalNode::in_memory();
        let id = node.create_object("test");

        let was_called = Arc::new(AtomicBool::new(false));
        let was_called_clone = was_called.clone();

        let _listener_id = node.subscribe(id, "main", Box::new(move |_| {
            was_called_clone.store(true, Ordering::SeqCst);
        })).unwrap();

        was_called.store(false, Ordering::SeqCst);

        // Write - callback should be called SYNCHRONOUSLY (before write_sync returns)
        node.write_sync(id, "main", b"test", "alice", 1000).unwrap();

        // This assertion happens IMMEDIATELY after write_sync returns
        // If callback was async, this would fail
        assert!(was_called.load(Ordering::SeqCst), "Callback must be called synchronously");
    }
}
