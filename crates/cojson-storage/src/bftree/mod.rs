//! BF-Tree backed storage implementation.
//!
//! This module provides a high-performance storage backend using BF-Tree-like
//! data structures. Currently uses Rust's BTreeMap as a stand-in while the
//! upstream bf-tree crate's dependency conflict is resolved.
//!
//! # Architecture
//!
//! The storage is organized into multiple indexes:
//!
//! - **covalues**: Primary index mapping CoValue IDs to headers
//! - **sessions**: Index mapping (covalue_id, session_id) to session data
//! - **transactions**: Index mapping (session_id, idx) to transactions
//! - **signatures**: Index mapping (session_id, idx) to signature checkpoints
//! - **unsynced**: Secondary index for tracking unsynced CoValues
//! - **pending_deletions**: Work queue for CoValue deletions
//!
//! # Features
//!
//! - Thread-safe concurrent access via RwLock
//! - Efficient range queries for session iteration
//! - Optional persistence via FileIO abstraction
//! - Memory-efficient storage with configurable page sizes

mod backend;

pub use backend::BTreeStorage;

/// Configuration for BTree storage.
#[derive(Debug, Clone)]
pub struct BTreeConfig {
    /// Whether to enable persistence (default: true)
    pub persist: bool,
    /// Sync interval in milliseconds (default: 1000)
    pub sync_interval_ms: u64,
    /// Maximum memory usage before flushing to disk (default: 64MB)
    pub max_memory_bytes: usize,
}

impl Default for BTreeConfig {
    fn default() -> Self {
        Self {
            persist: true,
            sync_interval_ms: 1000,
            max_memory_bytes: 64 * 1024 * 1024, // 64MB
        }
    }
}
