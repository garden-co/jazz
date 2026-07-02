//! RocksDB implementation of Groove's ordered key/value storage seam.
//!
//! This module owns opening RocksDB with the requested column families,
//! durability tier, ordered iterators, and atomic write batches. It implements
//! [`OrderedKvStorage`] but does not understand schemas, records, query graphs,
//! or IVM ticks; callers provide already-encoded keys and values. In-memory
//! storage for tests lives in [`super`], and all schema-aware behavior lives
//! above this adapter.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use rocksdb::{
    BlockBasedOptions, Cache, ColumnFamilyDescriptor, DB, DBCompressionType, Direction,
    IteratorMode, Options, ReadOptions, WriteBatch, WriteBufferManager, WriteOptions,
};

use super::{ColumnFamilyName, Error, Key, OrderedKvStorage, ScanVisitor, Value, WriteOperation};

const ROCKSDB_BLOCK_CACHE_BYTES: usize = 256 * 1024 * 1024;
const ROCKSDB_WRITE_BUFFER_MANAGER_BYTES: usize = 256 * 1024 * 1024;

/// RocksDB durability tier used for writes.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Durability {
    /// Sync every write batch through the OS for the strongest local durability.
    #[default]
    FullSync,
    /// Keep WAL atomicity but do not fsync every commit, like SQLite WAL/NORMAL.
    WalNoSync,
}

/// RocksDB implementation of the ordered KV storage trait.
pub struct RocksDbStorage {
    path: PathBuf,
    durability: Durability,
    column_families: BTreeSet<String>,
    db: DB,
    write_options: WriteOptions,
}

impl RocksDbStorage {
    /// Open with the default durability tier.
    ///
    /// Default is [`Durability::WalNoSync`] (WAL on, no per-commit fsync —
    /// crash-safe, never corrupts, bounded power-loss window; cf. Postgres
    /// `synchronous_commit=off`). Callers that need strict per-commit power-loss
    /// durability opt in via [`Self::open_with_durability`] with
    /// [`Durability::FullSync`].
    pub fn open(path: impl AsRef<Path>, column_families: &[&str]) -> Result<Self, Error> {
        Self::open_with_durability(path, column_families, Durability::WalNoSync)
    }

    pub fn open_with_durability(
        path: impl AsRef<Path>,
        column_families: &[&str],
        durability: Durability,
    ) -> Result<Self, Error> {
        let path = path.as_ref().to_path_buf();
        // Share one 256MB block cache and one 256MB write-buffer budget across
        // all column families opened by this storage instance.
        let block_cache = Cache::new_lru_cache(ROCKSDB_BLOCK_CACHE_BYTES);
        let write_buffer_manager =
            WriteBufferManager::new_write_buffer_manager(ROCKSDB_WRITE_BUFFER_MANAGER_BYTES, false);
        let mut options = rocksdb_options(&block_cache, &write_buffer_manager);
        options.create_if_missing(true);
        options.create_missing_column_families(true);
        if matches!(durability, Durability::FullSync) {
            options.set_use_fsync(true);
        }
        if matches!(durability, Durability::WalNoSync) {
            options.set_wal_bytes_per_sync(1 << 20);
        }

        let mut opened_column_families = column_families
            .iter()
            .map(|name| (*name).to_owned())
            .collect::<BTreeSet<_>>();
        if path.exists()
            && let Ok(existing) = DB::list_cf(&options, &path)
        {
            opened_column_families.extend(existing);
        }
        let descriptors = opened_column_families
            .iter()
            .map(String::as_str)
            .filter(|name| *name != "default")
            .map(|name| {
                ColumnFamilyDescriptor::new(
                    name,
                    rocksdb_options(&block_cache, &write_buffer_manager),
                )
            });

        let db = DB::open_cf_descriptors(&options, &path, descriptors)?;

        let mut write_options = WriteOptions::default();
        write_options.disable_wal(false);
        write_options.set_sync(matches!(durability, Durability::FullSync));

        Ok(Self {
            path,
            durability,
            column_families: opened_column_families,
            db,
            write_options,
        })
    }

    fn cf_handle(&self, cf: &ColumnFamilyName) -> Result<&rocksdb::ColumnFamily, Error> {
        self.db
            .cf_handle(cf)
            .ok_or_else(|| Error::ColumnFamilyNotFound(cf.to_owned()))
    }
}

fn rocksdb_options(block_cache: &Cache, write_buffer_manager: &WriteBufferManager) -> Options {
    let mut block_options = BlockBasedOptions::default();
    block_options.set_bloom_filter(10.0, false);
    block_options.set_block_cache(block_cache);

    let mut options = Options::default();
    options.set_block_based_table_factory(&block_options);
    options.set_write_buffer_manager(write_buffer_manager);
    options.set_compression_type(DBCompressionType::Lz4);
    options.set_bottommost_compression_type(DBCompressionType::Zstd);
    options
}

impl super::ReopenableStorage for RocksDbStorage {
    fn reopen(self, column_families: &[&str]) -> Result<Self, Error> {
        if column_families
            .iter()
            .all(|name| self.column_families.contains(*name))
        {
            return Ok(self);
        }
        let path = self.path.clone();
        let durability = self.durability;
        drop(self);
        Self::open_with_durability(path, column_families, durability)
    }
}

impl OrderedKvStorage for RocksDbStorage {
    fn get(&self, cf: &ColumnFamilyName, key: &Key) -> Result<Option<Value>, Error> {
        Ok(self.db.get_cf(self.cf_handle(cf)?, key)?)
    }

    fn set(&self, cf: &ColumnFamilyName, key: &Key, value: &[u8]) -> Result<(), Error> {
        Ok(self
            .db
            .put_cf_opt(self.cf_handle(cf)?, key, value, &self.write_options)?)
    }

    fn delete(&self, cf: &ColumnFamilyName, key: &Key) -> Result<(), Error> {
        Ok(self
            .db
            .delete_cf_opt(self.cf_handle(cf)?, key, &self.write_options)?)
    }

    fn scan_range(
        &self,
        cf: &ColumnFamilyName,
        start: &Key,
        end: &Key,
        visit: &mut ScanVisitor<'_>,
    ) -> Result<(), Error> {
        let mut options = ReadOptions::default();
        options.set_iterate_upper_bound(end.to_vec());

        for item in self.db.iterator_cf_opt(
            self.cf_handle(cf)?,
            options,
            IteratorMode::From(start, Direction::Forward),
        ) {
            let (key, value) = item?;
            visit(&key, &value)?;
        }
        Ok(())
    }

    fn scan_prefix(
        &self,
        cf: &ColumnFamilyName,
        prefix: &Key,
        visit: &mut ScanVisitor<'_>,
    ) -> Result<(), Error> {
        let mut upper_bound = prefix.to_vec();
        if !advance_prefix_upper_bound(&mut upper_bound) {
            for item in self.db.iterator_cf(
                self.cf_handle(cf)?,
                IteratorMode::From(prefix, Direction::Forward),
            ) {
                let (key, value) = item?;
                if !key.starts_with(prefix) {
                    break;
                }
                visit(&key, &value)?;
            }
            return Ok(());
        }

        let mut options = ReadOptions::default();
        options.set_iterate_upper_bound(upper_bound);

        for item in self.db.iterator_cf_opt(
            self.cf_handle(cf)?,
            options,
            IteratorMode::From(prefix, Direction::Forward),
        ) {
            let (key, value) = item?;
            visit(&key, &value)?;
        }
        Ok(())
    }

    fn scan_prefix_reverse(
        &self,
        cf: &ColumnFamilyName,
        prefix: &Key,
        visit: &mut ScanVisitor<'_>,
    ) -> Result<(), Error> {
        let handle = self.cf_handle(cf)?;
        let mut upper_bound = prefix.to_vec();
        if advance_prefix_upper_bound(&mut upper_bound) {
            for item in self
                .db
                .iterator_cf(handle, IteratorMode::From(&upper_bound, Direction::Reverse))
            {
                let (key, value) = item?;
                if key.starts_with(prefix) {
                    visit(&key, &value)?;
                } else if key.as_ref() < prefix {
                    break;
                }
            }
            return Ok(());
        }

        for item in self.db.iterator_cf(handle, IteratorMode::End) {
            let (key, value) = item?;
            if key.starts_with(prefix) {
                visit(&key, &value)?;
            } else if key.as_ref() < prefix {
                break;
            }
        }
        Ok(())
    }

    fn last_with_prefix(
        &self,
        cf: &ColumnFamilyName,
        prefix: &Key,
    ) -> Result<Option<super::KeyValue>, Error> {
        let handle = self.cf_handle(cf)?;
        let mut upper_bound = prefix.to_vec();
        let iterator_mode = if advance_prefix_upper_bound(&mut upper_bound) {
            IteratorMode::From(&upper_bound, Direction::Reverse)
        } else {
            IteratorMode::End
        };
        for item in self.db.iterator_cf(handle, iterator_mode) {
            let (key, value) = item?;
            if key.starts_with(prefix) {
                return Ok(Some((key.to_vec(), value.to_vec())));
            }
            if key.as_ref() < prefix {
                break;
            }
        }
        Ok(None)
    }

    fn last_with_prefix_before_or_at(
        &self,
        cf: &ColumnFamilyName,
        prefix: &Key,
        upper: &Key,
    ) -> Result<Option<super::KeyValue>, Error> {
        let handle = self.cf_handle(cf)?;
        for item in self
            .db
            .iterator_cf(handle, IteratorMode::From(upper, Direction::Reverse))
        {
            let (key, value) = item?;
            if key.starts_with(prefix) {
                return Ok(Some((key.to_vec(), value.to_vec())));
            }
            if key.as_ref() < prefix {
                break;
            }
        }
        Ok(None)
    }

    fn write_many(&self, operations: &[WriteOperation<'_>]) -> Result<(), Error> {
        let mut batch = WriteBatch::default();

        for operation in operations {
            match operation {
                WriteOperation::Set { cf, key, value } => {
                    batch.put_cf(self.cf_handle(cf)?, key, value);
                }
                WriteOperation::Delete { cf, key } => {
                    batch.delete_cf(self.cf_handle(cf)?, key);
                }
            }
        }

        Ok(self.db.write_opt(&batch, &self.write_options)?)
    }
}

fn advance_prefix_upper_bound(prefix: &mut [u8]) -> bool {
    for byte in prefix.iter_mut().rev() {
        if *byte != u8::MAX {
            *byte += 1;
            return true;
        }
        *byte = 0;
    }

    false
}
