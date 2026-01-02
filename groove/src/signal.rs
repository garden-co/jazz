//! Signal-based subscriptions to object states.
//!
//! This module provides reactive signals for subscribing to object state changes.
//! Signals use `futures-signals` for Rust-native reactivity with good WASM support.
//!
//! Key concepts:
//! - Signals are deduped by (object_id, branch)
//! - Loaded state stores commit IDs, branch reference, and environment reference
//! - Merge preview and diff are computed via helper methods using internal references
//! - Loading states: Loading, Loaded, Error
//! - Active subscriptions will trigger sync with peers (later)

use std::collections::HashMap;
use std::sync::{Arc, RwLock, Weak};

use bytes::Bytes;
use futures_signals::signal::{Mutable, ReadOnlyMutable};

use crate::branch::Branch;
use crate::commit::CommitId;
use crate::storage::Environment;

/// Unique key for deduplicating signals.
/// Signals with the same key share the same underlying subscription.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SignalKey {
    /// Object ID (UUIDv7)
    pub object_id: u128,
    /// Branch name
    pub branch: String,
}

impl SignalKey {
    pub fn new(object_id: u128, branch: impl Into<String>) -> Self {
        SignalKey {
            object_id,
            branch: branch.into(),
        }
    }
}

/// The state of a signal subscription.
#[derive(Debug, Clone)]
pub enum SignalState {
    /// Signal is loading the object state.
    Loading,
    /// Signal has loaded successfully.
    Loaded(LoadedState),
    /// Signal encountered an error.
    Error(SignalError),
}

impl SignalState {
    /// Returns true if the signal is in the Loading state.
    pub fn is_loading(&self) -> bool {
        matches!(self, SignalState::Loading)
    }

    /// Returns true if the signal is in the Loaded state.
    pub fn is_loaded(&self) -> bool {
        matches!(self, SignalState::Loaded(_))
    }

    /// Returns true if the signal is in the Error state.
    pub fn is_error(&self) -> bool {
        matches!(self, SignalState::Error(_))
    }

    /// Returns the loaded state if available.
    pub fn as_loaded(&self) -> Option<&LoadedState> {
        match self {
            SignalState::Loaded(state) => Some(state),
            _ => None,
        }
    }
}

/// Error types for signals.
#[derive(Debug, Clone)]
pub enum SignalError {
    /// Object not found.
    NotFound,
    /// Branch not found.
    BranchNotFound,
    /// Failed to load content from storage.
    StorageError(String),
    /// Merge failed.
    MergeError(String),
}

/// The loaded state of a signal.
/// Contains commit IDs for previous and current tips, plus references to branch and environment.
/// Merge preview and diff are computed on demand via helper methods.
#[derive(Clone)]
pub struct LoadedState {
    /// The previous tip commit IDs (None if this is the first load).
    pub previous_tips: Option<Vec<CommitId>>,
    /// The current tip commit IDs.
    pub tips: Vec<CommitId>,
    /// Reference to the branch for resolving commits.
    branch: Arc<RwLock<Branch>>,
    /// Reference to the environment for loading chunked content.
    env: Arc<dyn Environment>,
}

impl std::fmt::Debug for LoadedState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LoadedState")
            .field("previous_tips", &self.previous_tips)
            .field("tips", &self.tips)
            .field("branch", &"<Arc<RwLock<Branch>>>")
            .field("env", &"<Arc<dyn Environment>>")
            .finish()
    }
}

impl LoadedState {
    /// Create a new loaded state with no previous tips.
    pub fn new(tips: Vec<CommitId>, branch: Arc<RwLock<Branch>>, env: Arc<dyn Environment>) -> Self {
        LoadedState {
            previous_tips: None,
            tips,
            branch,
            env,
        }
    }

    /// Create a new loaded state with previous tips.
    pub fn with_previous(
        previous_tips: Option<Vec<CommitId>>,
        tips: Vec<CommitId>,
        branch: Arc<RwLock<Branch>>,
        env: Arc<dyn Environment>,
    ) -> Self {
        LoadedState {
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

    /// Compute merged content using a merge strategy (sync, inline content only).
    /// Returns the merged bytes, or the single tip's content if there's only one.
    pub fn merge(&self, strategy: &dyn crate::merge::MergeStrategy) -> Result<Bytes, SignalError> {
        let branch = self.branch.read().unwrap();
        merge_commit_ids(&self.tips, strategy, &branch)
    }

    /// Compute merged content using a merge strategy (async, supports chunked content).
    /// Returns the merged bytes, or the single tip's content if there's only one.
    pub async fn merge_async(
        &self,
        strategy: &dyn crate::merge::MergeStrategy,
    ) -> Result<Bytes, SignalError> {
        let branch = self.branch.read().unwrap();
        merge_commit_ids_async(&self.tips, strategy, &branch, self.env.as_ref()).await
    }

    /// Compute byte-level diff between previous and current merged content.
    /// Requires a merge strategy to compute the merged content for comparison.
    pub fn diff(&self, strategy: &dyn crate::merge::MergeStrategy) -> Result<ByteDiff, SignalError> {
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

    /// Compute byte-level diff between previous and current tips directly.
    /// Uses raw tip content without merging - useful when there's a single tip.
    pub fn diff_raw(&self) -> ByteDiff {
        let branch = self.branch.read().unwrap();

        if self.tips.is_empty() {
            return ByteDiff::Initial(Bytes::new());
        }

        // Get current tip content
        let current = self.tips.first()
            .and_then(|id| branch.get_commit(id))
            .and_then(|c| c.content.as_inline())
            .map(|b| Bytes::copy_from_slice(b))
            .unwrap_or_else(Bytes::new);

        match &self.previous_tips {
            None => ByteDiff::Initial(current),
            Some(prev_tips) => {
                if prev_tips.is_empty() {
                    return ByteDiff::Initial(current);
                }
                let previous = prev_tips.first()
                    .and_then(|id| branch.get_commit(id))
                    .and_then(|c| c.content.as_inline())
                    .map(|b| Bytes::copy_from_slice(b))
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

    /// Get the content of a specific tip by commit ID (sync, inline only).
    pub fn get_tip_content(&self, commit_id: &CommitId) -> Option<Bytes> {
        let branch = self.branch.read().unwrap();
        branch.get_commit(commit_id)
            .and_then(|c| c.content.as_inline())
            .map(|b| Bytes::copy_from_slice(b))
    }

    /// Get the content of a specific tip by commit ID (async, supports chunked).
    pub async fn get_tip_content_async(&self, commit_id: &CommitId) -> Option<Bytes> {
        let content_ref = {
            let branch = self.branch.read().unwrap();
            branch.get_commit(commit_id).map(|c| c.content.clone())
        }?;

        match content_ref {
            crate::storage::ContentRef::Inline(data) => Some(Bytes::copy_from_slice(&data)),
            crate::storage::ContentRef::Chunked(hashes) => {
                let mut result = Vec::new();
                for hash in hashes {
                    let chunk = self.env.get_chunk(&hash).await?;
                    result.extend_from_slice(&chunk);
                }
                Some(Bytes::from(result))
            }
        }
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

    /// Load content for all current tips (async, supports chunked).
    /// Returns a vec of (CommitId, Bytes) pairs.
    pub async fn load_all_tips(&self) -> Vec<(CommitId, Bytes)> {
        let mut results = Vec::new();
        for tip in &self.tips {
            if let Some(content) = self.get_tip_content_async(tip).await {
                results.push((*tip, content));
            }
        }
        results
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
    /// Returns true if content changed.
    pub fn is_changed(&self) -> bool {
        matches!(self, ByteDiff::Changed { .. })
    }

    /// Returns true if this is the initial state (no previous).
    pub fn is_initial(&self) -> bool {
        matches!(self, ByteDiff::Initial(_))
    }

    /// Returns true if content is unchanged.
    pub fn is_unchanged(&self) -> bool {
        matches!(self, ByteDiff::Unchanged)
    }
}

/// A range of bytes that changed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiffRange {
    /// Start offset in the old content.
    pub old_start: usize,
    /// Length in old content.
    pub old_len: usize,
    /// Start offset in the new content.
    pub new_start: usize,
    /// Length in new content.
    pub new_len: usize,
}

/// Compute simple change ranges between two byte sequences.
/// This is a naive implementation that just finds the first and last differing bytes.
pub fn compute_change_ranges(old: &[u8], new: &[u8]) -> Vec<DiffRange> {
    if old == new {
        return vec![];
    }

    // Find first differing byte
    let first_diff = old.iter().zip(new.iter()).position(|(a, b)| a != b);

    let (start_old, start_new) = match first_diff {
        Some(pos) => (pos, pos),
        None => {
            // One is a prefix of the other
            let min_len = old.len().min(new.len());
            (min_len, min_len)
        }
    };

    // Find last differing byte (from the end)
    let old_rev = old.iter().rev();
    let new_rev = new.iter().rev();
    let last_diff_from_end = old_rev.zip(new_rev).position(|(a, b)| a != b);

    let (end_old, end_new) = match last_diff_from_end {
        Some(pos) => (old.len() - pos, new.len() - pos),
        None => {
            // One is a prefix of the other
            (old.len(), new.len())
        }
    };

    // Ensure end is after start
    let end_old = end_old.max(start_old);
    let end_new = end_new.max(start_new);

    vec![DiffRange {
        old_start: start_old,
        old_len: end_old - start_old,
        new_start: start_new,
        new_len: end_new - start_new,
    }]
}

/// Internal signal data that's shared across subscribers.
struct SignalData {
    /// The current state (mutable for updates).
    state: Mutable<SignalState>,
    /// Key for this signal.
    key: SignalKey,
    /// Previous tip IDs for diffing.
    previous_tips: RwLock<Option<Vec<CommitId>>>,
    /// Environment reference for loading content.
    env: Arc<dyn Environment>,
}

impl std::fmt::Debug for SignalData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SignalData")
            .field("key", &self.key)
            .field("previous_tips", &self.previous_tips)
            .finish()
    }
}

impl SignalData {
    fn new(key: SignalKey, env: Arc<dyn Environment>) -> Self {
        SignalData {
            state: Mutable::new(SignalState::Loading),
            key,
            previous_tips: RwLock::new(None),
            env,
        }
    }

    fn update(&self, tips: Vec<CommitId>, branch: Arc<RwLock<Branch>>) {
        let prev = {
            let mut prev_lock = self.previous_tips.write().unwrap();
            let old = prev_lock.take();
            *prev_lock = Some(tips.clone());
            old
        };

        let loaded = LoadedState::with_previous(prev, tips, branch, self.env.clone());
        self.state.set(SignalState::Loaded(loaded));
    }

    fn set_error(&self, error: SignalError) {
        self.state.set(SignalState::Error(error));
    }
}

/// A handle to a signal subscription.
/// When all handles are dropped, the signal is cleaned up.
#[derive(Clone)]
pub struct ObjectSignal {
    data: Arc<SignalData>,
}

impl std::fmt::Debug for ObjectSignal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObjectSignal")
            .field("key", &self.data.key)
            .finish()
    }
}

impl ObjectSignal {
    /// Get the current state.
    pub fn get(&self) -> SignalState {
        self.data.state.get_cloned()
    }

    /// Get a read-only signal for use with futures-signals combinators.
    pub fn signal(&self) -> ReadOnlyMutable<SignalState> {
        self.data.state.read_only()
    }

    /// Get the signal key.
    pub fn key(&self) -> &SignalKey {
        &self.data.key
    }
}

/// Registry for signal deduplication.
/// Signals are shared when they have the same key.
#[derive(Debug, Default)]
pub struct SignalRegistry {
    /// Active signals, keyed by SignalKey.
    /// Uses Weak references so signals are cleaned up when all handles are dropped.
    signals: RwLock<HashMap<SignalKey, Weak<SignalData>>>,
}

impl SignalRegistry {
    /// Create a new signal registry.
    pub fn new() -> Self {
        SignalRegistry {
            signals: RwLock::new(HashMap::new()),
        }
    }

    /// Get or create a signal for the given key.
    /// If a signal already exists for this key, returns a new handle to it.
    /// Otherwise, creates a new signal and returns a handle.
    pub fn get_or_create(&self, key: SignalKey, env: Arc<dyn Environment>) -> ObjectSignal {
        // First, try to get existing signal
        {
            let signals = self.signals.read().unwrap();
            if let Some(weak) = signals.get(&key) {
                if let Some(data) = weak.upgrade() {
                    return ObjectSignal { data };
                }
            }
        }

        // Need to create new signal
        {
            let mut signals = self.signals.write().unwrap();

            // Double-check (another thread may have created it)
            if let Some(weak) = signals.get(&key) {
                if let Some(data) = weak.upgrade() {
                    return ObjectSignal { data };
                }
            }

            // Create new signal
            let data = Arc::new(SignalData::new(key.clone(), env));
            signals.insert(key, Arc::downgrade(&data));
            ObjectSignal { data }
        }
    }

    /// Update a signal with new tip IDs.
    /// Returns true if the signal exists and was updated.
    pub fn update(&self, key: &SignalKey, tips: Vec<CommitId>, branch: Arc<RwLock<Branch>>) -> bool {
        let signals = self.signals.read().unwrap();
        if let Some(weak) = signals.get(key) {
            if let Some(data) = weak.upgrade() {
                data.update(tips, branch);
                return true;
            }
        }
        false
    }

    /// Set error state for a signal.
    /// Returns true if the signal exists and was updated.
    pub fn set_error(&self, key: &SignalKey, error: SignalError) -> bool {
        let signals = self.signals.read().unwrap();
        if let Some(weak) = signals.get(key) {
            if let Some(data) = weak.upgrade() {
                data.set_error(error);
                return true;
            }
        }
        false
    }

    /// Clean up expired signals (those with no remaining handles).
    pub fn cleanup(&self) {
        let mut signals = self.signals.write().unwrap();
        signals.retain(|_, weak| weak.strong_count() > 0);
    }

    /// Get the number of active signals.
    pub fn active_count(&self) -> usize {
        let signals = self.signals.read().unwrap();
        signals.values().filter(|w| w.strong_count() > 0).count()
    }

    /// Get all active keys for a given object ID.
    /// Useful for notifying all signals when an object is updated.
    pub fn keys_for_object(&self, object_id: u128) -> Vec<SignalKey> {
        let signals = self.signals.read().unwrap();
        signals
            .iter()
            .filter(|(k, w)| k.object_id == object_id && w.strong_count() > 0)
            .map(|(k, _)| k.clone())
            .collect()
    }
}

/// Helper to compute merged content from commit IDs using a merge strategy (sync, inline only).
/// Returns the merged bytes.
pub fn merge_commit_ids(
    tips: &[CommitId],
    strategy: &dyn crate::merge::MergeStrategy,
    branch: &Branch,
) -> Result<Bytes, SignalError> {
    if tips.is_empty() {
        return Err(SignalError::BranchNotFound);
    }

    // Get tip contents
    let tip_contents: Vec<Vec<u8>> = tips
        .iter()
        .filter_map(|id| branch.get_commit(id))
        .filter_map(|c| c.content.as_inline().map(|b| b.to_vec()))
        .collect();

    if tip_contents.len() != tips.len() {
        return Err(SignalError::StorageError(
            "Some commits have chunked content".to_string(),
        ));
    }

    if tip_contents.len() == 1 {
        return Ok(Bytes::from(tip_contents.into_iter().next().unwrap()));
    }

    // Find LCA for base content
    let lca = if tips.len() >= 2 {
        branch.find_lca(&tips[0], &tips[1])
    } else {
        vec![]
    };

    let base: Option<&[u8]> = lca
        .first()
        .and_then(|id| branch.get_commit(id))
        .and_then(|c| c.content.as_inline());

    let tip_refs: Vec<&[u8]> = tip_contents.iter().map(|v| v.as_slice()).collect();

    strategy
        .merge(base, &tip_refs)
        .map(Bytes::from)
        .map_err(|e| SignalError::MergeError(e.to_string()))
}

/// Helper to compute merged content from commit IDs using a merge strategy (async, supports chunked).
/// Returns the merged bytes.
pub async fn merge_commit_ids_async(
    tips: &[CommitId],
    strategy: &dyn crate::merge::MergeStrategy,
    branch: &Branch,
    env: &dyn Environment,
) -> Result<Bytes, SignalError> {
    if tips.is_empty() {
        return Err(SignalError::BranchNotFound);
    }

    // Get tip contents (loading chunked if needed)
    let mut tip_contents: Vec<Vec<u8>> = Vec::new();
    for id in tips {
        let commit = branch.get_commit(id).ok_or(SignalError::NotFound)?;
        let content = load_content_ref(&commit.content, env).await?;
        tip_contents.push(content);
    }

    if tip_contents.len() == 1 {
        return Ok(Bytes::from(tip_contents.into_iter().next().unwrap()));
    }

    // Find LCA for base content
    let lca = if tips.len() >= 2 {
        branch.find_lca(&tips[0], &tips[1])
    } else {
        vec![]
    };

    let base: Option<Vec<u8>> = if let Some(lca_id) = lca.first() {
        if let Some(commit) = branch.get_commit(lca_id) {
            Some(load_content_ref(&commit.content, env).await?)
        } else {
            None
        }
    } else {
        None
    };

    let tip_refs: Vec<&[u8]> = tip_contents.iter().map(|v| v.as_slice()).collect();

    strategy
        .merge(base.as_deref(), &tip_refs)
        .map(Bytes::from)
        .map_err(|e| SignalError::MergeError(e.to_string()))
}

/// Load content from a ContentRef, handling both inline and chunked content.
async fn load_content_ref(
    content: &crate::storage::ContentRef,
    env: &dyn Environment,
) -> Result<Vec<u8>, SignalError> {
    match content {
        crate::storage::ContentRef::Inline(data) => Ok(data.to_vec()),
        crate::storage::ContentRef::Chunked(hashes) => {
            let mut result = Vec::new();
            for hash in hashes {
                let chunk = env
                    .get_chunk(hash)
                    .await
                    .ok_or_else(|| SignalError::StorageError("Chunk not found".to_string()))?;
                result.extend_from_slice(&chunk);
            }
            Ok(result)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commit::Commit;
    use crate::storage::{ContentRef, MemoryEnvironment};

    fn make_env() -> Arc<dyn Environment> {
        Arc::new(MemoryEnvironment::new())
    }

    fn make_branch_with_commit(content: &[u8]) -> (Arc<RwLock<Branch>>, CommitId) {
        let mut branch = Branch::new("main");
        let commit = Commit {
            parents: vec![],
            content: ContentRef::inline(content.to_vec()),
            author: "alice".to_string(),
            timestamp: 1000,
            meta: None,
        };
        let id = branch.add_commit(commit);
        (Arc::new(RwLock::new(branch)), id)
    }

    #[test]
    fn signal_key_equality() {
        let key1 = SignalKey::new(1, "main");
        let key2 = SignalKey::new(1, "main");
        let key3 = SignalKey::new(2, "main");
        let key4 = SignalKey::new(1, "feature");

        assert_eq!(key1, key2);
        assert_ne!(key1, key3);
        assert_ne!(key1, key4);
    }

    #[test]
    fn signal_state_transitions() {
        let env = make_env();
        let (branch, id) = make_branch_with_commit(b"test");

        let state = SignalState::Loading;
        assert!(state.is_loading());
        assert!(!state.is_loaded());
        assert!(!state.is_error());

        let loaded = SignalState::Loaded(LoadedState::new(vec![id], branch, env));
        assert!(!loaded.is_loading());
        assert!(loaded.is_loaded());

        let error = SignalState::Error(SignalError::NotFound);
        assert!(error.is_error());
    }

    #[test]
    fn loaded_state_needs_merge() {
        let env = make_env();
        let (branch, id1) = make_branch_with_commit(b"a");

        let single = LoadedState::new(vec![id1], branch.clone(), env.clone());
        assert!(!single.needs_merge());

        // Add another commit to create conflict
        {
            let mut b = branch.write().unwrap();
            let commit2 = Commit {
                parents: vec![],
                content: ContentRef::inline(b"b".to_vec()),
                author: "bob".to_string(),
                timestamp: 2000,
                meta: None,
            };
            b.add_commit(commit2);
        }

        let id2 = CommitId::from_bytes([2; 32]); // Fake ID for test
        let multiple = LoadedState::new(vec![id1, id2], branch, env);
        assert!(multiple.needs_merge());
        assert_eq!(multiple.tip_count(), 2);
    }

    #[test]
    fn loaded_state_diff_raw() {
        let env = make_env();
        let (branch, id1) = make_branch_with_commit(b"hello");

        // No previous
        let state = LoadedState::new(vec![id1], branch.clone(), env.clone());
        let diff = state.diff_raw();
        assert!(diff.is_initial());

        // Add another commit with same content
        let id2 = {
            let mut b = branch.write().unwrap();
            let commit = Commit {
                parents: vec![id1],
                content: ContentRef::inline(b"hello".to_vec()),
                author: "alice".to_string(),
                timestamp: 2000,
                meta: None,
            };
            b.add_commit(commit)
        };

        // With previous, unchanged
        let state = LoadedState::with_previous(Some(vec![id1]), vec![id2], branch.clone(), env.clone());
        let diff = state.diff_raw();
        assert!(diff.is_unchanged());

        // Add commit with different content
        let id3 = {
            let mut b = branch.write().unwrap();
            let commit = Commit {
                parents: vec![id2],
                content: ContentRef::inline(b"world".to_vec()),
                author: "alice".to_string(),
                timestamp: 3000,
                meta: None,
            };
            b.add_commit(commit)
        };

        // With previous, changed
        let state = LoadedState::with_previous(Some(vec![id2]), vec![id3], branch, env);
        let diff = state.diff_raw();
        assert!(diff.is_changed());
    }

    #[test]
    fn compute_change_ranges_identical() {
        let data = b"hello";
        let ranges = compute_change_ranges(data, data);
        assert!(ranges.is_empty());
    }

    #[test]
    fn compute_change_ranges_prefix_extension() {
        let old = b"hello";
        let new = b"hello world";
        let ranges = compute_change_ranges(old, new);

        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0].old_start, 5);
        assert_eq!(ranges[0].old_len, 0);
        assert_eq!(ranges[0].new_len, 6);
    }

    #[test]
    fn compute_change_ranges_middle_change() {
        let old = b"abcdef";
        let new = b"abXXef";
        let ranges = compute_change_ranges(old, new);

        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0].old_start, 2);
        assert_eq!(ranges[0].old_len, 2);
        assert_eq!(ranges[0].new_start, 2);
        assert_eq!(ranges[0].new_len, 2);
    }

    #[test]
    fn registry_deduplication() {
        let env = make_env();
        let registry = SignalRegistry::new();

        let key = SignalKey::new(1, "main");
        let signal1 = registry.get_or_create(key.clone(), env.clone());
        let signal2 = registry.get_or_create(key.clone(), env);

        // Both should be the same underlying signal
        assert!(Arc::ptr_eq(&signal1.data, &signal2.data));
        assert_eq!(registry.active_count(), 1);
    }

    #[test]
    fn registry_different_keys() {
        let env = make_env();
        let registry = SignalRegistry::new();

        let key1 = SignalKey::new(1, "main");
        let key2 = SignalKey::new(2, "main");

        let signal1 = registry.get_or_create(key1, env.clone());
        let signal2 = registry.get_or_create(key2, env);

        // Should be different signals
        assert!(!Arc::ptr_eq(&signal1.data, &signal2.data));
        assert_eq!(registry.active_count(), 2);
    }

    #[test]
    fn registry_cleanup() {
        let env = make_env();
        let registry = SignalRegistry::new();

        let key = SignalKey::new(1, "main");

        {
            let _signal = registry.get_or_create(key.clone(), env.clone());
            assert_eq!(registry.active_count(), 1);
        }

        // Signal dropped, should be cleaned up
        registry.cleanup();
        assert_eq!(registry.active_count(), 0);

        // Creating again should make a new signal
        let _signal = registry.get_or_create(key, env);
        assert_eq!(registry.active_count(), 1);
    }

    #[test]
    fn registry_update() {
        let env = make_env();
        let registry = SignalRegistry::new();
        let (branch, id) = make_branch_with_commit(b"hello");

        let key = SignalKey::new(1, "main");
        let signal = registry.get_or_create(key.clone(), env);

        // Initially loading
        assert!(signal.get().is_loading());

        // Update with tips
        let updated = registry.update(&key, vec![id], branch);

        assert!(updated);
        assert!(signal.get().is_loaded());

        if let SignalState::Loaded(state) = signal.get() {
            assert_eq!(state.tip_count(), 1);
            assert!(!state.has_previous());
        }
    }

    #[test]
    fn registry_set_error() {
        let env = make_env();
        let registry = SignalRegistry::new();

        let key = SignalKey::new(1, "main");
        let signal = registry.get_or_create(key.clone(), env);

        registry.set_error(&key, SignalError::NotFound);

        assert!(signal.get().is_error());
    }

    #[test]
    fn signal_tracks_previous_tips() {
        let env = make_env();
        let registry = SignalRegistry::new();
        let (branch, id1) = make_branch_with_commit(b"first");

        let key = SignalKey::new(1, "main");
        let signal = registry.get_or_create(key.clone(), env);

        // First update
        registry.update(&key, vec![id1], branch.clone());

        if let SignalState::Loaded(state) = signal.get() {
            assert!(!state.has_previous());
            assert!(state.diff_raw().is_initial());
        }

        // Add second commit
        let id2 = {
            let mut b = branch.write().unwrap();
            let commit = Commit {
                parents: vec![id1],
                content: ContentRef::inline(b"second".to_vec()),
                author: "alice".to_string(),
                timestamp: 2000,
                meta: None,
            };
            b.add_commit(commit)
        };

        // Second update
        registry.update(&key, vec![id2], branch);

        if let SignalState::Loaded(state) = signal.get() {
            assert!(state.has_previous());
            assert!(state.diff_raw().is_changed());

            // Verify previous tips
            let prev = state.previous_tips.as_ref().unwrap();
            assert_eq!(prev.len(), 1);
            assert_eq!(prev[0], id1);
        }
    }

    #[test]
    fn keys_for_object() {
        let env = make_env();
        let registry = SignalRegistry::new();

        // Create signals for different objects and branches
        let _s1 = registry.get_or_create(SignalKey::new(1, "main"), env.clone());
        let _s2 = registry.get_or_create(SignalKey::new(1, "feature"), env.clone());
        let _s3 = registry.get_or_create(SignalKey::new(2, "main"), env);

        // Should find 2 signals for object 1
        let keys = registry.keys_for_object(1);
        assert_eq!(keys.len(), 2);
        assert!(keys.iter().any(|k| k.branch == "main"));
        assert!(keys.iter().any(|k| k.branch == "feature"));

        // Should find 1 signal for object 2
        let keys = registry.keys_for_object(2);
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0].branch, "main");
    }

    #[test]
    fn loaded_state_get_tip_content() {
        let env = make_env();
        let (branch, id) = make_branch_with_commit(b"hello");
        let state = LoadedState::new(vec![id], branch, env);

        let content = state.get_tip_content(&id).unwrap();
        assert_eq!(&content[..], b"hello");

        let author = state.get_tip_author(&id).unwrap();
        assert_eq!(author, "alice");

        let timestamp = state.get_tip_timestamp(&id).unwrap();
        assert_eq!(timestamp, 1000);
    }

    use crate::merge::LastWriterWins;

    #[test]
    fn loaded_state_merge_single() {
        let env = make_env();
        let (branch, id) = make_branch_with_commit(b"single");
        let state = LoadedState::new(vec![id], branch, env);

        let strategy = LastWriterWins;
        let merged = state.merge(&strategy).unwrap();
        assert_eq!(&merged[..], b"single");
    }

    #[test]
    fn loaded_state_diff_with_merge() {
        let env = make_env();
        let (branch, id1) = make_branch_with_commit(b"first");

        let id2 = {
            let mut b = branch.write().unwrap();
            let commit = Commit {
                parents: vec![id1],
                content: ContentRef::inline(b"second".to_vec()),
                author: "alice".to_string(),
                timestamp: 2000,
                meta: None,
            };
            b.add_commit(commit)
        };

        let state = LoadedState::with_previous(Some(vec![id1]), vec![id2], branch, env);

        let strategy = LastWriterWins;
        let diff = state.diff(&strategy).unwrap();
        assert!(diff.is_changed());
    }

    /// Verify that futures_signals::Mutable wakes the waker synchronously when set() is called.
    #[test]
    fn mutable_wakes_on_set() {
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
        use std::task::{Context, Wake, Waker};
        use std::future::Future;
        use futures_signals::signal::{Mutable, SignalExt};

        let mutable = Mutable::new(1u32);
        let waker_called = Arc::new(AtomicBool::new(false));
        let call_count = Arc::new(AtomicUsize::new(0));
        let waker_called_clone = waker_called.clone();
        let call_count_clone = call_count.clone();

        struct TrackingWaker { called: Arc<AtomicBool> }
        impl Wake for TrackingWaker {
            fn wake(self: Arc<Self>) { self.called.store(true, Ordering::SeqCst); }
        }

        let future = mutable.signal().for_each(move |_| {
            call_count_clone.fetch_add(1, Ordering::SeqCst);
            async {}
        });

        let waker = Waker::from(Arc::new(TrackingWaker { called: waker_called_clone }));
        let mut cx = Context::from_waker(&waker);
        let mut future = std::pin::pin!(future);

        let _ = future.as_mut().poll(&mut cx);
        assert_eq!(call_count.load(Ordering::SeqCst), 1);

        waker_called.store(false, Ordering::SeqCst);
        mutable.set(42);

        assert!(waker_called.load(Ordering::SeqCst), "Mutable.set() should wake synchronously");
        let _ = future.as_mut().poll(&mut cx);
        assert_eq!(call_count.load(Ordering::SeqCst), 2);
    }

    /// Verify that ObjectSignal properly wakes when updated via SignalRegistry.
    #[test]
    fn object_signal_wakes_on_update() {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::task::{Context, Wake, Waker};
        use std::future::Future;
        use futures_signals::signal::SignalExt;
        use crate::storage::MemoryEnvironment;
        use crate::commit::Commit;
        use crate::storage::ContentRef;

        let env: Arc<dyn Environment> = Arc::new(MemoryEnvironment::new());
        let registry = SignalRegistry::new();

        let mut branch = Branch::new("main");
        let commit1 = Commit {
            parents: vec![],
            content: ContentRef::inline(b"initial".to_vec()),
            author: "alice".to_string(),
            timestamp: 1000,
            meta: None,
        };
        let id1 = branch.add_commit(commit1);
        let branch_ref = Arc::new(RwLock::new(branch));

        let key = SignalKey::new(1, "main");
        let signal = registry.get_or_create(key.clone(), env.clone());
        registry.update(&key, vec![id1], branch_ref.clone());

        let waker_called = Arc::new(AtomicBool::new(false));
        let call_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let waker_called_clone = waker_called.clone();
        let call_count_clone = call_count.clone();

        struct TrackingWaker { called: Arc<AtomicBool> }
        impl Wake for TrackingWaker {
            fn wake(self: Arc<Self>) { self.called.store(true, Ordering::SeqCst); }
        }

        let future = signal.signal().signal_cloned().for_each(move |_| {
            call_count_clone.fetch_add(1, Ordering::SeqCst);
            async {}
        });

        let waker = Waker::from(Arc::new(TrackingWaker { called: waker_called_clone }));
        let mut cx = Context::from_waker(&waker);
        let mut future = std::pin::pin!(future);

        let _ = future.as_mut().poll(&mut cx);
        waker_called.store(false, Ordering::SeqCst);

        let id2 = {
            let mut b = branch_ref.write().unwrap();
            b.add_commit(Commit {
                parents: vec![id1],
                content: ContentRef::inline(b"updated".to_vec()),
                author: "alice".to_string(),
                timestamp: 2000,
                meta: None,
            })
        };

        registry.update(&key, vec![id2], branch_ref);
        assert!(waker_called.load(Ordering::SeqCst), "ObjectSignal should wake on registry.update()");

        let _ = future.as_mut().poll(&mut cx);
        assert!(call_count.load(Ordering::SeqCst) >= 2, "Callback should receive update");
    }
}
