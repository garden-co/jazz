//! Listener types used across the crate.

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

/// Error types for node operations.
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
