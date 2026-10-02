//! Memory RocksDB holds outside Rust's allocator, for every store open in
//! this process.
//!
//! The server's heap profile only sees Rust allocations, so the block cache,
//! memtables and table readers that RocksDB allocates in C++ are reported
//! here instead, from RocksDB's own accounting.

use std::sync::{Arc, Mutex, Weak};

use rocksdb::{Cache, DB};

/// Bytes RocksDB holds in memory across all open stores.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RocksDbMemoryUsage {
    /// Blocks in the block caches, including pinned ones.
    pub block_cache_bytes: u64,
    /// Block cache entries in use by readers or iterators, which cannot be
    /// evicted.
    pub block_cache_pinned_bytes: u64,
    /// Mutable and immutable memtables.
    pub memtable_bytes: u64,
    /// Index and filter blocks of open SST files held outside the block
    /// cache.
    pub table_reader_bytes: u64,
}

struct OpenStore {
    id: u64,
    db: Weak<DB>,
    block_cache: Cache,
}

struct Registry {
    next_id: u64,
    stores: Vec<OpenStore>,
}

static OPEN_STORES: Mutex<Registry> = Mutex::new(Registry {
    next_id: 0,
    stores: Vec::new(),
});

/// A store's entry in the registry, removed when the store is dropped.
pub(crate) struct Registration(u64);

impl Registration {
    pub(crate) fn new(db: &Arc<DB>, block_cache: &Cache) -> Self {
        let mut registry = OPEN_STORES
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let id = registry.next_id;
        registry.next_id += 1;
        registry.stores.push(OpenStore {
            id,
            db: Arc::downgrade(db),
            block_cache: block_cache.clone(),
        });
        Self(id)
    }
}

impl Drop for Registration {
    fn drop(&mut self) {
        // Taking the lock waits out a concurrent `process_memory_usage`, so
        // once this returns no other thread holds the store's database and
        // it closes when the store drops it.
        let mut registry = OPEN_STORES
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        registry.stores.retain(|store| store.id != self.0);
    }
}

/// Memory held by every RocksDB store open in this process, or `None` if
/// RocksDB could not report it.
pub fn process_memory_usage() -> Option<RocksDbMemoryUsage> {
    let registry = OPEN_STORES
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let dbs: Vec<Arc<DB>> = registry
        .stores
        .iter()
        .filter_map(|store| store.db.upgrade())
        .collect();
    let caches: Vec<&Cache> = registry
        .stores
        .iter()
        .map(|store| &store.block_cache)
        .collect();
    let db_refs: Vec<&DB> = dbs.iter().map(Arc::as_ref).collect();
    // RocksDB counts each distinct cache once, however many stores share it.
    let stats = rocksdb::perf::get_memory_usage_stats(Some(&db_refs), Some(&caches)).ok()?;
    let block_cache_pinned_bytes = caches
        .iter()
        .map(|cache| cache.get_pinned_usage() as u64)
        .sum();
    Some(RocksDbMemoryUsage {
        block_cache_bytes: stats.cache_total,
        block_cache_pinned_bytes,
        memtable_bytes: stats.mem_table_total,
        table_reader_bytes: stats.mem_table_readers_total,
    })
}
