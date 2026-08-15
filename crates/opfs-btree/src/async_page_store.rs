//! The persistence boundary for the asynchronous browser B-tree experiment.
//!
//! A store is deliberately page-addressed: it may not use key ordering, scans,
//! or indexes to implement B-tree behavior.  Page `0` and `1` are the existing
//! superblock slots; all other bytes and page ids retain the on-file format
//! defined by `page`, `superblock`, and `wal`.

use crate::BTreeError;

/// Metadata needed to reconstruct the exact sparse page file on open.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PageStoreMetadata {
    /// Fixed byte width of every stored page.
    pub page_size: u32,
    /// Logical file length, including the two superblock pages and any WAL tail.
    pub logical_len: u64,
}

/// One opaque page blob addressed only by its page identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredPage {
    pub page_id: u64,
    pub bytes: Vec<u8>,
}

/// The complete atomic persistence unit for a dirty-page checkpoint.
///
/// Implementations must make `metadata`, all page writes, and deletions visible
/// together.  An IndexedDB implementation uses one `readwrite` transaction;
/// OPFS uses a matching checkpoint/flush boundary.  Relaxed *physical*
/// durability is allowed, but a successful commit must be visible to a later
/// open and to all program-order reads after the awaited write resolves.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PageStoreCommit {
    pub metadata: PageStoreMetadata,
    pub writes: Vec<StoredPage>,
    pub deleted_page_ids: Vec<u64>,
}

/// Async page persistence for `AsyncOpfsBTree`.
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

    /// Atomically persists an incremental dirty-page commit.
    async fn commit(&mut self, commit: PageStoreCommit) -> Result<(), BTreeError>;
}
