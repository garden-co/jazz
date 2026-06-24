use std::collections::HashMap;
use std::path::{Path, PathBuf};

use ::rocksdb::{
    BlockBasedOptions, Cache, DB, DBCompressionType, Direction, IteratorMode, Options, ReadOptions,
    WriteBatch,
};
use bench_core::{BenchEngine, EngineError, PhaseKind};
use tempfile::TempDir;

use super::{CACHE_BYTES, NativeEngine, eng};

pub(super) struct RocksDbEngine {
    path: PathBuf,
    db: Option<DB>,
    pending: Option<PendingBatch>,
    _dir: TempDir,
}

struct PendingBatch {
    batch: WriteBatch,
    overlay: HashMap<Vec<u8>, Option<Vec<u8>>>,
}

impl RocksDbEngine {
    fn db(&self) -> Result<&DB, EngineError> {
        self.db
            .as_ref()
            .ok_or_else(|| EngineError::new("rocksdb is closed"))
    }
}

impl NativeEngine for RocksDbEngine {
    const NAME: &'static str = "rocksdb";

    fn open_fresh() -> Result<Self, EngineError> {
        let dir = tempfile::tempdir().map_err(eng)?;
        let path = dir.path().join("bench.rocksdb");
        let db = open_rocksdb(&path)?;
        Ok(Self {
            path,
            db: Some(db),
            pending: None,
            _dir: dir,
        })
    }
}

impl BenchEngine for RocksDbEngine {
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), EngineError> {
        if let Some(pending) = self.pending.as_mut() {
            pending.batch.put(key, value);
            pending.overlay.insert(key.to_vec(), Some(value.to_vec()));
        } else {
            self.db()?.put(key, value).map_err(eng)?;
        }
        Ok(())
    }

    fn get(&mut self, key: &[u8]) -> Result<Option<u8>, EngineError> {
        if let Some(pending) = &self.pending {
            if let Some(value) = pending.overlay.get(key) {
                return Ok(value.as_ref().and_then(|bytes| bytes.first().copied()));
            }
        }
        Ok(self
            .db()?
            .get(key)
            .map_err(eng)?
            .and_then(|value| value.first().copied()))
    }

    fn delete(&mut self, key: &[u8]) -> Result<(), EngineError> {
        if let Some(pending) = self.pending.as_mut() {
            pending.batch.delete(key);
            pending.overlay.insert(key.to_vec(), None);
        } else {
            self.db()?.delete(key).map_err(eng)?;
        }
        Ok(())
    }

    fn range(&mut self, lo: &[u8], hi: &[u8], limit: usize) -> Result<usize, EngineError> {
        if limit == 0 {
            return Ok(0);
        }

        let mut read_options = ReadOptions::default();
        read_options.set_iterate_upper_bound(hi.to_vec());
        let iter = self
            .db()?
            .iterator_opt(IteratorMode::From(lo, Direction::Forward), read_options);

        let mut rows = 0usize;
        for item in iter {
            item.map_err(eng)?;
            rows += 1;
            if rows == limit {
                break;
            }
        }
        Ok(rows)
    }

    fn begin_phase(&mut self, kind: PhaseKind) -> Result<(), EngineError> {
        if kind.is_write() {
            if self.pending.is_some() {
                return Err(EngineError::new("rocksdb write batch is already open"));
            }
            self.pending = Some(PendingBatch {
                batch: WriteBatch::default(),
                overlay: HashMap::new(),
            });
        }
        Ok(())
    }

    fn end_phase(&mut self, kind: PhaseKind) -> Result<(), EngineError> {
        if kind.is_write() {
            let pending = self
                .pending
                .take()
                .ok_or_else(|| EngineError::new("rocksdb write batch is not open"))?;
            self.db()?.write(&pending.batch).map_err(eng)?;
        }
        Ok(())
    }

    async fn reopen(&mut self) -> Result<(), EngineError> {
        self.pending = None;
        self.db = None;
        self.db = Some(open_rocksdb(&self.path)?);
        Ok(())
    }
}

fn open_rocksdb(path: &Path) -> Result<DB, EngineError> {
    let mut block_options = BlockBasedOptions::default();
    block_options.set_bloom_filter(10.0, false);
    let cache = Cache::new_lru_cache(CACHE_BYTES);
    block_options.set_block_cache(&cache);

    let mut options = Options::default();
    options.create_if_missing(true);
    options.set_block_based_table_factory(&block_options);
    options.set_compression_type(DBCompressionType::Lz4);
    options.set_bottommost_compression_type(DBCompressionType::Zstd);

    DB::open(&options, path).map_err(eng)
}
