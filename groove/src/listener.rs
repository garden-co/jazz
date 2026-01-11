//! Synchronous callback-based subscriptions to object states.
//!
//! This module provides a layered listener system for reactive subscriptions:
//! - Layer 0: Object listeners (raw object state changes)
//! - Layer 1: Parsed row listeners (in Database)
//! - Layer 2: Table membership listeners (in Database)
//! - Layer 3: Query listeners (in Database)
//!
//! All callbacks are synchronous - they fire immediately when state changes.
//! Each layer caches its last value so new subscribers get the current state.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use bytes::Bytes;

use crate::branch::Branch;
use crate::commit::CommitId;
use crate::object::ObjectId;
use crate::storage::Environment;

/// Unique ID for a listener subscription.
/// Uses the newtype pattern to keep the internal representation opaque.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ListenerId(u64);

impl ListenerId {
    /// Create a new ListenerId from a raw id.
    pub(crate) fn new(id: u64) -> Self {
        ListenerId(id)
    }
}

/// Unique key for object listeners.
/// Listeners with the same key share the same cached state.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ObjectKey {
    /// Object ID (UUIDv7)
    pub object_id: ObjectId,
    /// Branch name
    pub branch: String,
}

impl ObjectKey {
    pub fn new(object_id: ObjectId, branch: impl Into<String>) -> Self {
        ObjectKey {
            object_id,
            branch: branch.into(),
        }
    }
}

/// Error types for listeners.
#[derive(Debug, Clone)]
pub enum ListenerError {
    /// Object not found.
    NotFound,
    /// Branch not found.
    BranchNotFound,
    /// Failed to load content from storage.
    StorageError(String),
    /// Merge failed.
    MergeError(String),
}

/// The current state of an object (branch).
/// Contains commit IDs for previous and current tips, plus references to branch and environment.
#[derive(Clone)]
pub struct ObjectState {
    /// The previous tip commit IDs (None if this is the first load).
    pub previous_tips: Option<Vec<CommitId>>,
    /// The current tip commit IDs.
    pub tips: Vec<CommitId>,
    /// Reference to the branch for resolving commits.
    branch: Arc<RwLock<Branch>>,
    /// Reference to the environment for loading chunked content.
    env: Arc<dyn Environment>,
}

impl std::fmt::Debug for ObjectState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObjectState")
            .field("previous_tips", &self.previous_tips)
            .field("tips", &self.tips)
            .finish()
    }
}

impl ObjectState {
    /// Create a new state with no previous tips.
    pub fn new(
        tips: Vec<CommitId>,
        branch: Arc<RwLock<Branch>>,
        env: Arc<dyn Environment>,
    ) -> Self {
        ObjectState {
            previous_tips: None,
            tips,
            branch,
            env,
        }
    }

    /// Create a new state with previous tips.
    pub fn with_previous(
        previous_tips: Option<Vec<CommitId>>,
        tips: Vec<CommitId>,
        branch: Arc<RwLock<Branch>>,
        env: Arc<dyn Environment>,
    ) -> Self {
        ObjectState {
            previous_tips,
            tips,
            branch,
            env,
        }
    }

    /// Returns true if there are multiple tips (needs merge).
    pub fn needs_merge(&self) -> bool {
        self.tips.len() > 1
    }

    /// Returns the number of tips.
    pub fn tip_count(&self) -> usize {
        self.tips.len()
    }

    /// Returns true if there's a previous state.
    pub fn has_previous(&self) -> bool {
        self.previous_tips.is_some()
    }

    /// Get the branch reference.
    pub fn branch(&self) -> &Arc<RwLock<Branch>> {
        &self.branch
    }

    /// Get the environment reference.
    pub fn env(&self) -> &Arc<dyn Environment> {
        &self.env
    }

    /// Compute merged content using a merge strategy.
    pub fn merge(
        &self,
        strategy: &dyn crate::merge::MergeStrategy,
    ) -> Result<Bytes, ListenerError> {
        let branch = self.branch.read().unwrap();
        merge_commit_ids(&self.tips, strategy, &branch)
    }

    /// Compute byte-level diff between previous and current merged content.
    pub fn diff(
        &self,
        strategy: &dyn crate::merge::MergeStrategy,
    ) -> Result<ByteDiff, ListenerError> {
        let branch = self.branch.read().unwrap();
        let current = merge_commit_ids(&self.tips, strategy, &branch)?;

        match &self.previous_tips {
            None => Ok(ByteDiff::Initial(current)),
            Some(prev_tips) => {
                if prev_tips.is_empty() {
                    return Ok(ByteDiff::Initial(current));
                }
                let previous = merge_commit_ids(prev_tips, strategy, &branch)?;
                if previous == current {
                    Ok(ByteDiff::Unchanged)
                } else {
                    Ok(ByteDiff::Changed {
                        old: previous.clone(),
                        new: current.clone(),
                        ranges: compute_change_ranges(&previous, &current),
                    })
                }
            }
        }
    }

    /// Compute byte-level diff using raw tip content without merging.
    pub fn diff_raw(&self) -> ByteDiff {
        let branch = self.branch.read().unwrap();

        if self.tips.is_empty() {
            return ByteDiff::Initial(Bytes::new());
        }

        let current = self
            .tips
            .first()
            .and_then(|id| branch.get_commit(id))
            .map(|c| Bytes::copy_from_slice(&c.content))
            .unwrap_or_default();

        match &self.previous_tips {
            None => ByteDiff::Initial(current),
            Some(prev_tips) => {
                if prev_tips.is_empty() {
                    return ByteDiff::Initial(current);
                }
                let previous = prev_tips
                    .first()
                    .and_then(|id| branch.get_commit(id))
                    .map(|c| Bytes::copy_from_slice(&c.content))
                    .unwrap_or_else(Bytes::new);

                if previous == current {
                    ByteDiff::Unchanged
                } else {
                    ByteDiff::Changed {
                        old: previous.clone(),
                        new: current.clone(),
                        ranges: compute_change_ranges(&previous, &current),
                    }
                }
            }
        }
    }

    /// Get the content of a specific tip by commit ID.
    pub fn get_tip_content(&self, commit_id: &CommitId) -> Option<Bytes> {
        let branch = self.branch.read().unwrap();
        branch
            .get_commit(commit_id)
            .map(|c| Bytes::copy_from_slice(&c.content))
    }

    /// Get author of a specific tip.
    pub fn get_tip_author(&self, commit_id: &CommitId) -> Option<String> {
        let branch = self.branch.read().unwrap();
        branch.get_commit(commit_id).map(|c| c.author.clone())
    }

    /// Get timestamp of a specific tip.
    pub fn get_tip_timestamp(&self, commit_id: &CommitId) -> Option<u64> {
        let branch = self.branch.read().unwrap();
        branch.get_commit(commit_id).map(|c| c.timestamp)
    }

    /// Load content for all current tips.
    pub fn load_all_tips(&self) -> Vec<(CommitId, Bytes)> {
        self.tips
            .iter()
            .filter_map(|tip| self.get_tip_content(tip).map(|content| (*tip, content)))
            .collect()
    }

    /// Read the current content (merged if multiple tips, or single tip content).
    /// Returns None if no tips or content is chunked.
    pub fn read_content(&self) -> Option<Bytes> {
        if self.tips.is_empty() {
            return None;
        }
        if self.tips.len() == 1 {
            return self.get_tip_content(&self.tips[0]);
        }
        // Multiple tips - use LastWriterWins as default
        self.merge(&crate::merge::LastWriterWins).ok()
    }
}

/// Result of a byte-level diff.
#[derive(Debug, Clone)]
pub enum ByteDiff {
    /// First state, no previous to diff against.
    Initial(Bytes),
    /// Content unchanged.
    Unchanged,
    /// Content changed.
    Changed {
        old: Bytes,
        new: Bytes,
        /// Byte ranges that changed: (start, old_len, new_len)
        ranges: Vec<DiffRange>,
    },
}

impl ByteDiff {
    pub fn is_changed(&self) -> bool {
        matches!(self, ByteDiff::Changed { .. })
    }

    pub fn is_initial(&self) -> bool {
        matches!(self, ByteDiff::Initial(_))
    }

    pub fn is_unchanged(&self) -> bool {
        matches!(self, ByteDiff::Unchanged)
    }
}

/// A range of bytes that changed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiffRange {
    pub old_start: usize,
    pub old_len: usize,
    pub new_start: usize,
    pub new_len: usize,
}

/// Compute simple change ranges between two byte sequences.
pub fn compute_change_ranges(old: &[u8], new: &[u8]) -> Vec<DiffRange> {
    if old == new {
        return vec![];
    }

    let first_diff = old.iter().zip(new.iter()).position(|(a, b)| a != b);
    let (start_old, start_new) = match first_diff {
        Some(pos) => (pos, pos),
        None => {
            let min_len = old.len().min(new.len());
            (min_len, min_len)
        }
    };

    let old_rev = old.iter().rev();
    let new_rev = new.iter().rev();
    let last_diff_from_end = old_rev.zip(new_rev).position(|(a, b)| a != b);

    let (end_old, end_new) = match last_diff_from_end {
        Some(pos) => (old.len() - pos, new.len() - pos),
        None => (old.len(), new.len()),
    };

    let end_old = end_old.max(start_old);
    let end_new = end_new.max(start_new);

    vec![DiffRange {
        old_start: start_old,
        old_len: end_old - start_old,
        new_start: start_new,
        new_len: end_new - start_new,
    }]
}

/// Type alias for object listener callbacks.
pub type ObjectCallback = Box<dyn Fn(Arc<ObjectState>) + Send + Sync>;

/// Internal state for a single object key.
struct ObjectListenerState {
    /// Cached current state (None if never set).
    current: Option<Arc<ObjectState>>,
    /// Previous tips for computing diffs.
    previous_tips: Option<Vec<CommitId>>,
    /// Environment reference.
    env: Arc<dyn Environment>,
    /// Active listeners.
    listeners: HashMap<ListenerId, ObjectCallback>,
}

impl ObjectListenerState {
    fn new(env: Arc<dyn Environment>) -> Self {
        ObjectListenerState {
            current: None,
            previous_tips: None,
            env,
            listeners: HashMap::new(),
        }
    }

    /// Update state and notify all listeners.
    fn update(&mut self, tips: Vec<CommitId>, branch: Arc<RwLock<Branch>>) {
        let prev = self.previous_tips.take();
        self.previous_tips = Some(tips.clone());

        let state = Arc::new(ObjectState::with_previous(
            prev,
            tips,
            branch,
            self.env.clone(),
        ));
        self.current = Some(state.clone());

        // Notify all listeners synchronously
        for callback in self.listeners.values() {
            callback(state.clone());
        }
    }

    /// Add a listener, calling it immediately with current state if available.
    fn add_listener(&mut self, id: ListenerId, callback: ObjectCallback) {
        if let Some(ref current) = self.current {
            callback(current.clone());
        }
        self.listeners.insert(id, callback);
    }

    /// Remove a listener.
    fn remove_listener(&mut self, id: ListenerId) -> bool {
        self.listeners.remove(&id).is_some()
    }
}

/// Registry for object listeners.
/// Manages subscriptions to object state changes with synchronous callbacks.
pub struct ObjectListenerRegistry {
    /// Listener states by object key.
    states: RwLock<HashMap<ObjectKey, ObjectListenerState>>,
    /// Next listener ID counter.
    next_id: RwLock<u64>,
}

impl std::fmt::Debug for ObjectListenerRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let states = self.states.read().unwrap();
        f.debug_struct("ObjectListenerRegistry")
            .field("key_count", &states.len())
            .finish()
    }
}

impl Default for ObjectListenerRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl ObjectListenerRegistry {
    /// Create a new registry.
    pub fn new() -> Self {
        ObjectListenerRegistry {
            states: RwLock::new(HashMap::new()),
            next_id: RwLock::new(1),
        }
    }

    /// Subscribe to an object's state changes.
    /// The callback is called immediately with the current state (if available),
    /// and then on every subsequent update.
    /// Returns a listener ID that can be used to unsubscribe.
    ///
    /// Use `ensure_initial_state` before calling this if you need to set initial state.
    pub fn subscribe(
        &self,
        key: ObjectKey,
        env: Arc<dyn Environment>,
        callback: ObjectCallback,
    ) -> ListenerId {
        let id = {
            let mut next = self.next_id.write().unwrap();
            let id = ListenerId::new(*next);
            *next += 1;
            id
        };

        let mut states = self.states.write().unwrap();
        let state = states
            .entry(key)
            .or_insert_with(|| ObjectListenerState::new(env));
        state.add_listener(id, callback);
        id
    }

    /// Ensure initial state is set for a key (only if not already set).
    /// This is used to set the initial state before subscribing, so that
    /// the first subscriber gets the initial state callback.
    /// Unlike `notify`, this does NOT call existing listeners.
    pub fn ensure_initial_state(
        &self,
        key: &ObjectKey,
        env: Arc<dyn Environment>,
        tips: Vec<CommitId>,
        branch: Arc<RwLock<Branch>>,
    ) {
        let mut states = self.states.write().unwrap();
        let state = states
            .entry(key.clone())
            .or_insert_with(|| ObjectListenerState::new(env));
        if state.current.is_none() {
            // Set initial state without notifying (there are no listeners yet)
            state.previous_tips = Some(tips.clone());
            state.current = Some(Arc::new(ObjectState::with_previous(
                None,
                tips,
                branch,
                state.env.clone(),
            )));
        }
    }

    /// Unsubscribe a listener by ID.
    /// Returns true if the listener was found and removed.
    /// Safe to call from within a callback.
    pub fn unsubscribe(&self, key: &ObjectKey, id: ListenerId) -> bool {
        let mut states = self.states.write().unwrap();
        if let Some(state) = states.get_mut(key) {
            let removed = state.remove_listener(id);
            // Clean up empty states
            if state.listeners.is_empty() {
                states.remove(key);
            }
            removed
        } else {
            false
        }
    }

    /// Notify all listeners for an object that its state has changed.
    /// This is called by LocalNode when an object is written to.
    pub fn notify(&self, key: &ObjectKey, tips: Vec<CommitId>, branch: Arc<RwLock<Branch>>) {
        let mut states = self.states.write().unwrap();
        if let Some(state) = states.get_mut(key) {
            state.update(tips, branch);
        }
    }

    /// Get the current cached state for an object (if any).
    pub fn get_current(&self, key: &ObjectKey) -> Option<Arc<ObjectState>> {
        let states = self.states.read().unwrap();
        states.get(key).and_then(|s| s.current.clone())
    }

    /// Get the number of active listeners for an object.
    pub fn listener_count(&self, key: &ObjectKey) -> usize {
        let states = self.states.read().unwrap();
        states.get(key).map(|s| s.listeners.len()).unwrap_or(0)
    }

    /// Get all object keys that have active listeners.
    pub fn active_keys(&self) -> Vec<ObjectKey> {
        let states = self.states.read().unwrap();
        states.keys().cloned().collect()
    }

    /// Get all keys for a specific object ID (across all branches).
    pub fn keys_for_object(&self, object_id: ObjectId) -> Vec<ObjectKey> {
        let states = self.states.read().unwrap();
        states
            .keys()
            .filter(|k| k.object_id == object_id)
            .cloned()
            .collect()
    }
}

// ========== Helper functions ==========

/// Helper to compute merged content from commit IDs using a merge strategy.
pub fn merge_commit_ids(
    tips: &[CommitId],
    strategy: &dyn crate::merge::MergeStrategy,
    branch: &Branch,
) -> Result<Bytes, ListenerError> {
    if tips.is_empty() {
        return Err(ListenerError::BranchNotFound);
    }

    let tip_contents: Vec<Vec<u8>> = tips
        .iter()
        .filter_map(|id| branch.get_commit(id))
        .map(|c| c.content.to_vec())
        .collect();

    if tip_contents.len() != tips.len() {
        return Err(ListenerError::NotFound);
    }

    if tip_contents.len() == 1 {
        return Ok(Bytes::from(tip_contents.into_iter().next().unwrap()));
    }

    let lca = if tips.len() >= 2 {
        branch.find_lca(&tips[0], &tips[1])
    } else {
        vec![]
    };

    let base: Option<Vec<u8>> = lca
        .first()
        .and_then(|id| branch.get_commit(id))
        .map(|c| c.content.to_vec());

    let tip_refs: Vec<&[u8]> = tip_contents.iter().map(|v| v.as_slice()).collect();

    strategy
        .merge(base.as_deref(), &tip_refs)
        .map(Bytes::from)
        .map_err(|e| ListenerError::MergeError(e.to_string()))
}

// Tests have been moved to tests/listener.rs
