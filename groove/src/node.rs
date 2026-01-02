use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use bytes::Bytes;
use futures::io::AsyncRead;

use crate::commit::CommitId;
use crate::object::Object;
use crate::signal::{ObjectSignal, SignalError, SignalKey, SignalRegistry};
use crate::storage::{Environment, MemoryEnvironment};

/// Generate a new UUIDv7 as u128.
pub fn generate_object_id() -> u128 {
    uuid::Uuid::now_v7().as_u128()
}

/// A local node managing multiple objects with signal support.
///
/// The node owns the Environment (storage) and SignalRegistry, providing methods
/// for subscribing to objects and reading/writing with automatic signal notification.
///
/// Uses internal mutability so that objects can be created and written to
/// without requiring exclusive access to the node.
#[derive(Debug)]
pub struct LocalNode {
    objects: RwLock<BTreeMap<u128, Arc<RwLock<Object>>>>,
    signals: SignalRegistry,
    env: Arc<dyn Environment>,
}

impl Default for LocalNode {
    fn default() -> Self {
        Self::new(Arc::new(MemoryEnvironment::new()))
    }
}

impl LocalNode {
    /// Create a new LocalNode with the given environment.
    pub fn new(env: Arc<dyn Environment>) -> Self {
        LocalNode {
            objects: RwLock::new(BTreeMap::new()),
            signals: SignalRegistry::new(),
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

    /// Get a reference to the signal registry.
    pub fn signals(&self) -> &SignalRegistry {
        &self.signals
    }

    // ========== Subscription API ==========

    /// Subscribe to an object's branch.
    /// Creates a signal and immediately updates it with current state.
    /// Returns an error if the object or branch doesn't exist.
    pub fn subscribe(&self, object_id: u128, branch: &str) -> Result<ObjectSignal, SignalError> {
        let obj_lock = self
            .objects
            .read()
            .unwrap()
            .get(&object_id)
            .cloned()
            .ok_or(SignalError::NotFound)?;

        let key = SignalKey::new(object_id, branch);
        let signal = self.signals.get_or_create(key.clone(), self.env.clone());

        // Get branch reference and current tips
        let obj = obj_lock.read().unwrap();
        let branch_ref = obj
            .branch_ref(branch)
            .ok_or(SignalError::BranchNotFound)?;

        let tips = {
            let b = branch_ref.read().unwrap();
            b.frontier().to_vec()
        };

        // Update signal with current state
        self.signals.update(&key, tips, branch_ref);

        Ok(signal)
    }

    // ========== Read API ==========

    /// Read content from the frontier of an object's branch (sync).
    /// Returns None if the branch is empty, has multiple tips, or content is chunked.
    pub fn read_sync(
        &self,
        object_id: u128,
        branch: &str,
    ) -> Result<Option<Vec<u8>>, SignalError> {
        let obj_lock = self
            .objects
            .read()
            .unwrap()
            .get(&object_id)
            .cloned()
            .ok_or(SignalError::NotFound)?;

        let obj = obj_lock.read().unwrap();
        Ok(obj.read_sync(branch))
    }

    /// Read content from the frontier of an object's branch (async).
    /// Loads chunked content from storage if needed.
    pub async fn read(
        &self,
        object_id: u128,
        branch: &str,
    ) -> Result<Option<Vec<u8>>, SignalError> {
        let obj_lock = self
            .objects
            .read()
            .unwrap()
            .get(&object_id)
            .cloned()
            .ok_or(SignalError::NotFound)?;

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

    /// Write content to an object's branch and notify all signals.
    /// Returns the new commit ID.
    /// Panics if content exceeds INLINE_THRESHOLD.
    pub fn write_sync(
        &self,
        object_id: u128,
        branch: &str,
        content: &[u8],
        author: &str,
        timestamp: u64,
    ) -> Result<CommitId, SignalError> {
        let obj_lock = self
            .objects
            .read()
            .unwrap()
            .get(&object_id)
            .cloned()
            .ok_or(SignalError::NotFound)?;

        let obj = obj_lock.read().unwrap();
        let commit_id = obj.write_sync(branch, content, author, timestamp);

        // Notify signal if it exists
        self.notify_signal(object_id, branch, &obj);

        Ok(commit_id)
    }

    /// Write content to an object's branch (async) and notify all signals.
    /// Automatically chunks content that exceeds INLINE_THRESHOLD.
    pub async fn write(
        &self,
        object_id: u128,
        branch: &str,
        content: &[u8],
        author: &str,
        timestamp: u64,
    ) -> Result<CommitId, SignalError> {
        let obj_lock = self
            .objects
            .read()
            .unwrap()
            .get(&object_id)
            .cloned()
            .ok_or(SignalError::NotFound)?;

        let commit_id = {
            let obj = obj_lock.read().unwrap();
            obj.write(branch, content, author, timestamp, self.env.as_ref())
                .await
        };

        // Notify signal if it exists
        {
            let obj = obj_lock.read().unwrap();
            self.notify_signal(object_id, branch, &obj);
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
    ) -> Result<CommitId, SignalError> {
        let obj_lock = self
            .objects
            .read()
            .unwrap()
            .get(&object_id)
            .cloned()
            .ok_or(SignalError::NotFound)?;

        let commit_id = {
            let obj = obj_lock.read().unwrap();
            obj.write_stream(branch, reader, author, timestamp, self.env.as_ref())
                .await
                .map_err(|e| SignalError::StorageError(e.to_string()))?
        };

        // Notify signal if it exists
        {
            let obj = obj_lock.read().unwrap();
            self.notify_signal(object_id, branch, &obj);
        }

        Ok(commit_id)
    }

    /// Get the frontier commit IDs for an object's branch.
    pub fn frontier(&self, object_id: u128, branch: &str) -> Result<Option<Vec<CommitId>>, SignalError> {
        let obj_lock = self
            .objects
            .read()
            .unwrap()
            .get(&object_id)
            .cloned()
            .ok_or(SignalError::NotFound)?;

        let obj = obj_lock.read().unwrap();
        Ok(obj.frontier(branch))
    }

    /// Internal: notify signal for a branch update.
    fn notify_signal(&self, object_id: u128, branch: &str, obj: &Object) {
        let key = SignalKey::new(object_id, branch);

        if let Some(branch_ref) = obj.branch_ref(branch) {
            let tips = {
                let b = branch_ref.read().unwrap();
                b.frontier().to_vec()
            };
            self.signals.update(&key, tips, branch_ref);
        }
    }

    /// Notify all signals for an object.
    /// Use this after external changes to the object (e.g., sync from peers).
    pub fn notify_object(&self, object_id: u128) {
        let obj_lock = match self.objects.read().unwrap().get(&object_id).cloned() {
            Some(o) => o,
            None => return,
        };

        let obj = obj_lock.read().unwrap();

        // Find all active signals for this object
        let keys = self.signals.keys_for_object(object_id);

        for key in keys {
            if let Some(branch_ref) = obj.branch_ref(&key.branch) {
                let tips = {
                    let b = branch_ref.read().unwrap();
                    b.frontier().to_vec()
                };
                self.signals.update(&key, tips, branch_ref);
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
    use crate::signal::SignalState;

    #[test]
    fn local_node_create_and_get_objects() {
        let node = LocalNode::in_memory();

        let id1 = node.create_object("chat");
        let id2 = node.create_object("message");

        assert!(node.get_object(id1).is_some());
        assert!(node.get_object(id2).is_some());
        assert!(node.get_object(999).is_none());

        // Access through Arc<RwLock<>>
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
        // UUIDv7 should be roughly time-ordered
        assert!(id2 > id1);
    }

    #[test]
    fn local_node_uses_uuidv7() {
        let node = LocalNode::in_memory();

        let id1 = node.create_object("test1");
        std::thread::sleep(std::time::Duration::from_millis(1));
        let id2 = node.create_object("test2");

        // IDs should be valid UUIDv7 (time-ordered)
        assert!(id2 > id1);

        // Should be large numbers (not sequential 1, 2, 3...)
        assert!(id1 > 1000);
    }

    #[test]
    fn subscribe_to_empty_object() {
        let node = LocalNode::in_memory();
        let id = node.create_object("test");

        let signal = node.subscribe(id, "main").unwrap();

        // Should be loaded (even though empty)
        assert!(signal.get().is_loaded());

        if let SignalState::Loaded(state) = signal.get() {
            // Empty branch has no tips
            assert!(state.tips.is_empty());
        }
    }

    #[test]
    fn subscribe_nonexistent_object_errors() {
        let node = LocalNode::in_memory();

        let result = node.subscribe(999, "main");
        assert!(result.is_err());
    }

    #[test]
    fn subscribe_nonexistent_branch_errors() {
        let node = LocalNode::in_memory();
        let id = node.create_object("test");

        let result = node.subscribe(id, "nonexistent");
        assert!(result.is_err());
    }

    #[test]
    fn write_sync_and_notify() {
        let node = LocalNode::in_memory();
        let id = node.create_object("test");

        // Subscribe before writing
        let signal = node.subscribe(id, "main").unwrap();

        // Initial state - empty
        if let SignalState::Loaded(state) = signal.get() {
            assert!(state.tips.is_empty());
            assert!(!state.has_previous());
        }

        // Write through node (auto-notifies)
        let commit_id = node.write_sync(id, "main", b"hello", "alice", 1000).unwrap();

        // Signal should be updated
        if let SignalState::Loaded(state) = signal.get() {
            assert_eq!(state.tips.len(), 1);
            assert_eq!(state.tips[0], commit_id);
            // Now has previous (the empty tips)
            assert!(state.has_previous());
        }

        // Write again
        let commit_id2 = node.write_sync(id, "main", b"world", "alice", 2000).unwrap();

        // Signal should track the change
        if let SignalState::Loaded(state) = signal.get() {
            assert_eq!(state.tips.len(), 1);
            assert_eq!(state.tips[0], commit_id2);

            // Previous should be the first commit
            let prev = state.previous_tips.as_ref().unwrap();
            assert_eq!(prev.len(), 1);
            assert_eq!(prev[0], commit_id);

            // Diff should show change
            let diff = state.diff_raw();
            assert!(diff.is_changed());
        }
    }

    #[test]
    fn write_without_subscriber() {
        let node = LocalNode::in_memory();
        let id = node.create_object("test");

        // Write without subscribing - should not error
        let commit_id = node.write_sync(id, "main", b"hello", "alice", 1000).unwrap();

        // Now subscribe and verify content
        let signal = node.subscribe(id, "main").unwrap();

        if let SignalState::Loaded(state) = signal.get() {
            assert_eq!(state.tips.len(), 1);
            assert_eq!(state.tips[0], commit_id);
        }
    }

    #[test]
    fn notify_object() {
        let node = LocalNode::in_memory();
        let id = node.create_object("test");

        // Subscribe
        let signal = node.subscribe(id, "main").unwrap();

        // Write directly to object (bypassing node's write method)
        {
            let obj_lock = node.get_object(id).unwrap();
            let obj = obj_lock.read().unwrap();
            obj.write_sync("main", b"direct write", "alice", 1000);
        }

        // Signal not updated yet
        if let SignalState::Loaded(state) = signal.get() {
            assert!(state.tips.is_empty());
        }

        // Now notify
        node.notify_object(id);

        // Signal should be updated
        if let SignalState::Loaded(state) = signal.get() {
            assert_eq!(state.tips.len(), 1);
        }
    }

    #[test]
    fn multiple_subscribers_share_signal() {
        let node = LocalNode::in_memory();
        let id = node.create_object("test");

        let signal1 = node.subscribe(id, "main").unwrap();
        let signal2 = node.subscribe(id, "main").unwrap();

        // Write
        node.write_sync(id, "main", b"hello", "alice", 1000).unwrap();

        // Both signals should see the update
        if let SignalState::Loaded(state1) = signal1.get() {
            if let SignalState::Loaded(state2) = signal2.get() {
                assert_eq!(state1.tips, state2.tips);
            }
        }
    }

    #[test]
    fn read_write_roundtrip() {
        let node = LocalNode::in_memory();
        let id = node.create_object("test");

        // Write through node
        node.write_sync(id, "main", b"hello world", "alice", 1000).unwrap();

        // Read through node
        let content = node.read_sync(id, "main").unwrap().unwrap();
        assert_eq!(content, b"hello world");
    }
}
