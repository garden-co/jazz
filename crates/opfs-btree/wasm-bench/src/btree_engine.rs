#![cfg(target_arch = "wasm32")]

//! The opfs-btree side of the benchmark: a thin [`BenchEngine`] over
//! [`OpfsBTree`]. All workload/timing/checksum logic lives in `bench-core`;
//! this file only maps the contract onto the b-tree's API.

use bench_core::{BenchEngine, EngineError, PhaseKind};
use opfs_btree::{BTreeOptions, OpfsBTree, OpfsFile};

const BENCH_CACHE_BYTES: usize = 32 * 1024 * 1024;
const BENCH_OVERFLOW_THRESHOLD: usize = 4 * 1024;
const BENCH_PIN_INTERNAL_PAGES: bool = true;
const BENCH_READ_COALESCE_PAGES: usize = 4;

const NAMESPACE: &str = "bench-dataset";

fn benchmark_options() -> BTreeOptions {
    BTreeOptions {
        page_size: 16 * 1024,
        cache_bytes: BENCH_CACHE_BYTES,
        overflow_threshold: BENCH_OVERFLOW_THRESHOLD,
        pin_internal_pages: BENCH_PIN_INTERNAL_PAGES,
        read_coalesce_pages: BENCH_READ_COALESCE_PAGES,
        compress_overflow: true,
    }
}

fn err(e: opfs_btree::BTreeError) -> EngineError {
    EngineError::new(e.to_string())
}

pub struct BtreeEngine {
    // `Option` so a cold reopen can drop the old handle (OPFS access handles are
    // exclusive) before opening a fresh one.
    db: Option<OpfsBTree<OpfsFile>>,
}

impl BtreeEngine {
    /// Open a fresh, empty store (wipes any previous benchmark file).
    pub async fn open() -> Result<Self, EngineError> {
        OpfsFile::destroy(NAMESPACE).await.ok();
        let db = open_db().await?;
        Ok(Self { db: Some(db) })
    }

    fn db(&mut self) -> &mut OpfsBTree<OpfsFile> {
        self.db.as_mut().expect("btree engine is open")
    }
}

async fn open_db() -> Result<OpfsBTree<OpfsFile>, EngineError> {
    let file = OpfsFile::open(NAMESPACE).await.map_err(err)?;
    OpfsBTree::open(file, benchmark_options()).map_err(err)
}

impl BenchEngine for BtreeEngine {
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), EngineError> {
        self.db().put(key, value).map_err(err)
    }

    fn get(&mut self, key: &[u8]) -> Result<Option<u8>, EngineError> {
        Ok(self
            .db()
            .get(key)
            .map_err(err)?
            .map(|v| v.first().copied().unwrap_or(0)))
    }

    fn delete(&mut self, key: &[u8]) -> Result<(), EngineError> {
        self.db().delete(key).map_err(err)
    }

    fn range(&mut self, lo: &[u8], hi: &[u8], limit: usize) -> Result<usize, EngineError> {
        Ok(self.db().range(lo, hi, limit).map_err(err)?.len())
    }

    fn begin_phase(&mut self, _kind: PhaseKind) -> Result<(), EngineError> {
        Ok(())
    }

    fn end_phase(&mut self, kind: PhaseKind) -> Result<(), EngineError> {
        if kind.is_write() {
            self.db().checkpoint().map_err(err)?;
        }
        Ok(())
    }

    async fn reopen(&mut self) -> Result<(), EngineError> {
        if let Some(db) = self.db.as_mut() {
            db.checkpoint().map_err(err)?;
        }
        // Drop the current handle before reopening; data persists on disk.
        self.db = None;
        self.db = Some(open_db().await?);
        Ok(())
    }
}
