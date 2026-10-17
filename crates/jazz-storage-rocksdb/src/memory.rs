//! Memory RocksDB holds outside Rust's allocator, for every store open in
//! this process.
//!
//! The server's heap profile only sees Rust allocations, so the block cache,
//! memtables and table readers that RocksDB allocates in C++ are reported
//! here instead, from RocksDB's own accounting.

use std::sync::{Arc, Mutex, OnceLock, Weak};

use rocksdb::{Cache, DB, WriteBufferManager};

/// One RocksDB memory budget for the whole process.
///
/// Every store this process opens shares one block cache and one write
/// buffer manager, and memtable memory is charged to that cache, so
/// `block_cache_bytes` caps block cache plus memtables across all stores.
/// Index and filter blocks are charged to the same cache. A server that
/// opens several stores therefore holds one budget, not one per store.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RocksDbMemoryBudget {
    /// Capacity of the shared block cache, including memtable reservations
    /// and index/filter blocks.
    pub block_cache_bytes: usize,
    /// Memtable size across all stores at which RocksDB starts flushing.
    pub write_buffer_bytes: usize,
}

impl Default for RocksDbMemoryBudget {
    fn default() -> Self {
        Self {
            block_cache_bytes: crate::ROCKSDB_BLOCK_CACHE_BYTES,
            write_buffer_bytes: crate::ROCKSDB_WRITE_BUFFER_MANAGER_BYTES,
        }
    }
}

pub(crate) struct SharedMemory {
    budget: RocksDbMemoryBudget,
    pub(crate) block_cache: Cache,
    pub(crate) write_buffer_manager: WriteBufferManager,
}

static SHARED_MEMORY: OnceLock<SharedMemory> = OnceLock::new();

impl SharedMemory {
    fn new(budget: RocksDbMemoryBudget) -> Self {
        let block_cache = Cache::new_lru_cache(budget.block_cache_bytes);
        let write_buffer_manager = WriteBufferManager::new_write_buffer_manager_with_cache(
            budget.write_buffer_bytes,
            false,
            block_cache.clone(),
        );
        Self {
            budget,
            block_cache,
            write_buffer_manager,
        }
    }
}

/// The process-wide cache and write buffer manager, created with the default
/// budget unless [`configure_process_memory_budget`] ran first.
pub(crate) fn shared_memory() -> &'static SharedMemory {
    SHARED_MEMORY.get_or_init(|| SharedMemory::new(RocksDbMemoryBudget::default()))
}

/// Set the process-wide RocksDB memory budget.
///
/// Call this once, before the first store opens. Once the shared cache
/// exists its budget is fixed: a later call with the same budget succeeds,
/// and a different one returns the budget already in force.
pub fn configure_process_memory_budget(
    budget: RocksDbMemoryBudget,
) -> Result<(), RocksDbMemoryBudget> {
    let shared = SHARED_MEMORY.get_or_init(|| SharedMemory::new(budget));
    if shared.budget == budget {
        Ok(())
    } else {
        Err(shared.budget)
    }
}

/// The budget the process-wide cache was created with.
pub fn process_memory_budget() -> RocksDbMemoryBudget {
    shared_memory().budget
}

/// Bytes RocksDB holds in memory across all open stores.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RocksDbMemoryUsage {
    /// Usage of the shared block cache, including pinned blocks, index and
    /// filter blocks, and the reservations charged for memtables.
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
    /// Exclude reporters while expanding the sole live database handle. The
    /// registry's weak pointer must also be absent for `Arc::get_mut`.
    pub(crate) fn with_db_mut<T>(
        &self,
        db: &mut Arc<DB>,
        operation: impl FnOnce(&mut DB) -> T,
    ) -> T {
        let mut registry = OPEN_STORES
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let store = registry
            .stores
            .iter_mut()
            .find(|store| store.id == self.0)
            .expect("an open store retains its memory registration");
        store.db = Weak::new();
        struct Restore<'a> {
            store: &'a mut OpenStore,
            db: &'a mut Arc<DB>,
        }
        impl Drop for Restore<'_> {
            fn drop(&mut self) {
                self.store.db = Arc::downgrade(self.db);
            }
        }
        let restore = Restore { store, db };
        operation(
            Arc::get_mut(restore.db).expect("reporters released the exclusive database handle"),
        )
    }

    pub(crate) fn new(db: &Arc<DB>) -> Self {
        let mut registry = OPEN_STORES
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let id = registry.next_id;
        registry.next_id += 1;
        registry.stores.push(OpenStore {
            id,
            db: Arc::downgrade(db),
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
    let shared = shared_memory();
    let caches: Vec<&Cache> = vec![&shared.block_cache];
    let db_refs: Vec<&DB> = dbs.iter().map(Arc::as_ref).collect();
    let stats = rocksdb::perf::get_memory_usage_stats(Some(&db_refs), Some(&caches)).ok()?;
    let block_cache_pinned_bytes = shared.block_cache.get_pinned_usage() as u64;
    Some(RocksDbMemoryUsage {
        block_cache_bytes: stats.cache_total,
        block_cache_pinned_bytes,
        memtable_bytes: stats.mem_table_total,
        table_reader_bytes: stats.mem_table_readers_total,
    })
}
