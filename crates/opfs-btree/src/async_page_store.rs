//! The persistence boundary for the asynchronous browser B-tree experiment.
//!
//! A store is deliberately page-addressed: it may not use key ordering, scans,
//! or indexes to implement B-tree behavior.  Page `0` and `1` are the existing
//! superblock slots; all other bytes and page ids retain the on-file format
//! defined by `page`, `superblock`, and `wal`.

use crate::BTreeError;
use serde::{Deserialize, Serialize};

/// Metadata needed to reconstruct the exact sparse page file on open.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PageStoreMetadata {
    /// Fixed byte width of every stored page.
    pub page_size: u32,
    /// Logical file length, including the two superblock pages and any WAL tail.
    pub logical_len: u64,
}

/// One opaque page blob addressed only by its page identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StoredPage {
    pub page_id: u64,
    pub bytes: Vec<u8>,
}

/// The complete persistence unit for a dirty-page checkpoint.
///
/// IndexedDB makes these records transaction-atomic.  The experimental OPFS
/// adapter writes and flushes the same records but does **not** claim multi-page
/// crash atomicity. Relaxed physical durability is allowed. In both adapters a
/// successful awaited commit is visible to subsequent program-order reads and
/// to a later clean reopen; callers must not infer stronger crash guarantees.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PageStoreCommit {
    pub metadata: PageStoreMetadata,
    pub writes: Vec<StoredPage>,
    pub deleted_page_ids: Vec<u64>,
}

/// Async page persistence for `AsyncPageBTree`.
///
/// This intentionally has no `get(key)`, range, cursor, or ordered iteration:
/// tree descent, split, scan, cache eviction, and WAL interpretation belong to
/// the B-tree implementation, not its backing store.
// Browser stores are intentionally `!Send` (IndexedDB lives on its worker), so
// spelling the futures as `+ Send` would make the boundary less useful.
#[allow(async_fn_in_trait)]
pub trait AsyncPageStore {
    /// Opens the store and returns its current logical-file metadata.
    async fn metadata(&mut self) -> Result<Option<PageStoreMetadata>, BTreeError>;

    /// Loads opaque pages by identity. The returned entries must correspond
    /// exactly to `page_ids`; a missing page is an I/O/corruption error.
    async fn read_pages(&mut self, page_ids: &[u64]) -> Result<Vec<StoredPage>, BTreeError>;

    /// Persists an incremental dirty-page commit. IndexedDB provides
    /// transaction atomicity; other stores need only meet the documented
    /// awaited visibility and clean-reopen boundary.
    async fn commit(&mut self, commit: PageStoreCommit) -> Result<(), BTreeError>;
}
