//! RocksDB implementation of Groove's ordered key/value storage seam.
//!
//! This module owns opening RocksDB with the requested column families,
//! durability tier, ordered iterators, and atomic write batches. It implements
//! [`OrderedKvStorage`] but does not understand schemas, records, query graphs,
//! or IVM ticks; callers provide already-encoded keys and values. In-memory
//! storage for tests lives in [`super`], and all schema-aware behavior lives
//! above this adapter.

#[cfg(test)]
use std::cell::Cell;
use std::cell::RefCell;
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use rocksdb::{
    BlockBasedOptions, Cache, ColumnFamilyDescriptor, DB, DBCompactionStyle, DBCompressionType,
    DBIteratorWithThreadMode, Direction, IteratorMode, MergeOperands, Options, ReadOptions,
    UniversalCompactOptions, WriteBatch, WriteBufferManager, WriteOptions, properties,
};
use serde::Serialize;

use groove::storage::{
    BoxedStorage, ColumnFamilyName, Error, KeyValue, OrderedKvStorage, OwnedWriteOperation,
    ReopenableStorage, ScanBounds, ScanDirection, ScanRequest, StorageCursor, StorageFactory,
    StorageFuture, StorageScan, Value, apply_storage_delta, compact_storage_delta_operand,
    storage_delta_requires_full_merge,
};

trait RocksResultExt<T> {
    fn storage(self) -> Result<T, Error>;
}

impl<T> RocksResultExt<T> for Result<T, rocksdb::Error> {
    fn storage(self) -> Result<T, Error> {
        self.map_err(|error| Error::Backend {
            backend: "rocksdb",
            message: error.to_string(),
        })
    }
}

const ROCKSDB_BLOCK_CACHE_BYTES: usize = 256 * 1024 * 1024;
const ROCKSDB_WRITE_BUFFER_MANAGER_BYTES: usize = 256 * 1024 * 1024;
const ROCKSDB_DEFAULT_BLOCK_BYTES: usize = 16 * 1024;
const ROCKSDB_LARGE_BLOCK_BYTES: usize = 64 * 1024;
const ROCKSDB_APPEND_TARGET_FILE_BYTES: u64 = 128 * 1024 * 1024;
const ROCKSDB_OVERWRITE_TARGET_FILE_BYTES: u64 = 64 * 1024 * 1024;

const CLASS_HISTORY_CF: &str = "__groove_class_history";
const CLASS_REGISTER_CF: &str = "__groove_class_register";
const CLASS_GLOBAL_CURRENT_CF: &str = "__groove_class_global_current";
const CLASS_AHEAD_CURRENT_CF: &str = "__groove_class_ahead_current";
const CLASS_CHANGES_CF: &str = "__groove_class_changes";
const CLASS_INDICES_CF: &str = "__groove_class_indices";
const CLASS_META_CF: &str = "__groove_class_meta";

// RocksDB merge operators must return a value, even when a delta removes a
// key. Keep that internal state in an adapter-only value codec rather than
// reserving a logical byte string. Every visible value is encoded, so either
// tag can represent every possible user byte sequence without ambiguity.
const ROCKSDB_INTERNAL_CF: &str = "__groove_storage_internal_v1";
const ROCKSDB_VALUE_FORMAT_KEY: &[u8] = b"value-format";
const ROCKSDB_VALUE_FORMAT_V2: &[u8] = b"v2";
const ROCKSDB_VALUE_LIVE_TAG: u8 = 0;
const ROCKSDB_VALUE_TOMBSTONE_TAG: u8 = 1;

fn encode_rocks_value(value: Option<&[u8]>) -> Vec<u8> {
    match value {
        Some(value) => {
            let mut encoded = Vec::with_capacity(1 + value.len());
            encoded.push(ROCKSDB_VALUE_LIVE_TAG);
            encoded.extend_from_slice(value);
            encoded
        }
        None => vec![ROCKSDB_VALUE_TOMBSTONE_TAG],
    }
}

fn decode_rocks_value(value: &[u8]) -> Result<Option<Vec<u8>>, Error> {
    match value.split_first() {
        Some((&ROCKSDB_VALUE_LIVE_TAG, value)) => Ok(Some(value.to_vec())),
        Some((&ROCKSDB_VALUE_TOMBSTONE_TAG, [])) => Ok(None),
        Some((tag, _)) => Err(Error::InvalidStorageDelta(format!(
            "invalid RocksDB private value tag {tag}"
        ))),
        None => Err(Error::InvalidStorageDelta(
            "empty RocksDB private value".to_owned(),
        )),
    }
}

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
    write_flush_cadence: RefCell<Option<WriteFlushCadence>>,
    #[cfg(test)]
    last_wal_flush_sync: Cell<Option<bool>>,
}

/// Opens a native persistent store at the exact shell-provided path.
#[derive(Clone, Copy, Debug, Default)]
pub struct RocksDbStorageFactory;

impl StorageFactory for RocksDbStorageFactory {
    fn open(
        &self,
        path: PathBuf,
        column_families: Vec<String>,
    ) -> StorageFuture<'_, Result<BoxedStorage, Error>> {
        Box::pin(async move {
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).map_err(|error| Error::Backend {
                    backend: "rocksdb",
                    message: error.to_string(),
                })?;
            }
            let column_families = column_families
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>();
            Ok(BoxedStorage::new(RocksDbStorage::open(
                path,
                &column_families,
            )?))
        })
    }
}

struct RocksDbCursor<'a> {
    iterator: DBIteratorWithThreadMode<'a, DB>,
    prefix: Option<Vec<u8>>,
    lower_bound: Vec<u8>,
    upper_bound: Option<Vec<u8>>,
    done: bool,
    remaining: Option<usize>,
}

impl<'a> RocksDbCursor<'a> {
    fn new(
        iterator: DBIteratorWithThreadMode<'a, DB>,
        prefix: Option<Vec<u8>>,
        lower_bound: Vec<u8>,
        upper_bound: Option<Vec<u8>>,
        remaining: Option<usize>,
    ) -> Self {
        Self {
            iterator,
            prefix,
            lower_bound,
            upper_bound,
            done: false,
            remaining,
        }
    }
}

impl StorageCursor for RocksDbCursor<'_> {
    fn next_batch(&mut self) -> StorageFuture<'_, Result<Option<Vec<KeyValue>>, Error>> {
        Box::pin(async move {
            if self.done || self.remaining == Some(0) {
                return Ok(None);
            }
            let batch_limit = self.remaining.unwrap_or(256).min(256);
            let mut batch = Vec::with_capacity(batch_limit);
            while batch.len() < batch_limit {
                let Some(item) = self.iterator.next() else {
                    self.done = true;
                    break;
                };
                let (key, value) = item.storage()?;
                if key.as_ref() < self.lower_bound.as_slice() {
                    self.done = true;
                    break;
                }
                if self
                    .upper_bound
                    .as_ref()
                    .is_some_and(|upper_bound| key.as_ref() >= upper_bound.as_slice())
                {
                    // A reverse iterator positioned at an exclusive end first
                    // yields that key if it exists. Skip it before traversal.
                    continue;
                }
                if let Some(prefix) = &self.prefix
                    && !key.starts_with(prefix)
                {
                    self.done = true;
                    break;
                }
                let Some(value) = decode_rocks_value(&value)? else {
                    continue;
                };
                batch.push((key.into_vec(), value));
            }
            if let Some(remaining) = &mut self.remaining {
                *remaining -= batch.len();
            }
            Ok((!batch.is_empty()).then_some(batch))
        })
    }
}

/// A best-effort, allocation-free snapshot of the RocksDB counters that are
/// useful when attributing a storage receipt.  These are backend counters, not
/// process memory measurements: in particular `memtable_bytes` excludes the
/// shared block cache and Rust allocations.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct RocksDbMetrics {
    /// Total bytes in all SST files, including files no longer live.
    pub total_sst_bytes: Option<u64>,
    /// Bytes in SST files reachable from the latest LSM version.
    pub live_sst_bytes: Option<u64>,
    /// Estimated bytes of live key/value data.
    pub estimated_live_data_bytes: Option<u64>,
    /// Bytes in mutable and immutable memtables; not the block cache.
    pub memtable_bytes: Option<u64>,
    /// Estimated bytes awaiting compaction (where supported by the profile).
    pub pending_compaction_bytes: Option<u64>,
    pub running_flushes: Option<u64>,
    pub running_compactions: Option<u64>,
    pub flush_pending: Option<bool>,
    pub compaction_pending: Option<bool>,
}

#[derive(Clone, Copy, Debug)]
struct WriteFlushCadence {
    every: usize,
    pending: usize,
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
        if column_families.contains(&ROCKSDB_INTERNAL_CF) {
            return Err(Error::InvalidStorageLayout(
                "RocksDB internal column family name is reserved".to_owned(),
            ));
        }
        // Share one 256MB block cache and one 256MB write-buffer budget across
        // all column families opened by this storage instance.
        let block_cache = Cache::new_lru_cache(ROCKSDB_BLOCK_CACHE_BYTES);
        let write_buffer_manager =
            WriteBufferManager::new_write_buffer_manager(ROCKSDB_WRITE_BUFFER_MANAGER_BYTES, false);
        let requested_column_families = column_families
            .iter()
            .map(|name| (*name).to_owned())
            .collect::<BTreeSet<_>>();

        let mut write_options = WriteOptions::default();
        write_options.disable_wal(false);
        write_options.set_sync(matches!(durability, Durability::FullSync));

        let listed_column_families = DB::list_cf(&Options::default(), &path).ok();
        let is_new_store = listed_column_families.is_none();
        if let Some(existing) = &listed_column_families {
            validate_or_initialize_rocks_value_format(
                &path,
                existing,
                &block_cache,
                &write_buffer_manager,
                &write_options,
            )?;
        }

        let mut opened_column_families = requested_column_families;
        opened_column_families.insert("default".to_owned());
        if let Some(existing) = listed_column_families {
            opened_column_families.extend(existing);
        }
        opened_column_families.insert(ROCKSDB_INTERNAL_CF.to_owned());

        let mut final_options = rocksdb_options(&block_cache, &write_buffer_manager);
        final_options.create_if_missing(true);
        final_options.create_missing_column_families(true);
        if matches!(durability, Durability::FullSync) {
            final_options.set_use_fsync(true);
        }
        if matches!(durability, Durability::WalNoSync) {
            final_options.set_wal_bytes_per_sync(1 << 20);
        }
        let descriptors = opened_column_families
            .iter()
            .map(String::as_str)
            .filter(|name| *name != "default")
            .map(|name| {
                ColumnFamilyDescriptor::new(
                    name,
                    rocksdb_options_for_cf(name, &block_cache, &write_buffer_manager),
                )
            });
        let db = DB::open_cf_descriptors(&final_options, &path, descriptors).storage()?;
        let internal_cf = db
            .cf_handle(ROCKSDB_INTERNAL_CF)
            .expect("RocksDB internal column family was opened");
        match db.get_cf(internal_cf, ROCKSDB_VALUE_FORMAT_KEY).storage()? {
            Some(marker) if marker.as_slice() == ROCKSDB_VALUE_FORMAT_V2 => {}
            Some(_) => {
                return Err(Error::InvalidStorageLayout(
                    "incompatible RocksDB storage format: unsupported private value format marker"
                        .to_owned(),
                ));
            }
            None if is_new_store => db
                .put_cf_opt(
                    internal_cf,
                    ROCKSDB_VALUE_FORMAT_KEY,
                    ROCKSDB_VALUE_FORMAT_V2,
                    &write_options,
                )
                .storage()?,
            None => {
                return Err(Error::InvalidStorageLayout(
                    "incompatible RocksDB storage format: private value format marker is missing"
                        .to_owned(),
                ));
            }
        }

        Ok(Self {
            path,
            durability,
            column_families: opened_column_families
                .into_iter()
                .filter(|name| name != ROCKSDB_INTERNAL_CF)
                .collect(),
            db,
            write_options,
            write_flush_cadence: RefCell::new(None),
            #[cfg(test)]
            last_wal_flush_sync: Cell::new(None),
        })
    }

    fn cf_handle(&self, cf: &ColumnFamilyName) -> Result<&rocksdb::ColumnFamily, Error> {
        if !self.column_families.contains(cf) {
            return Err(Error::ColumnFamilyNotFound(cf.to_owned()));
        }
        self.db
            .cf_handle(cf)
            .ok_or_else(|| Error::ColumnFamilyNotFound(cf.to_owned()))
    }

    fn flush_wal(&self, sync: bool) -> Result<(), Error> {
        #[cfg(test)]
        self.last_wal_flush_sync.set(Some(sync));
        self.db.flush_wal(sync).storage()
    }

    /// Snapshot RocksDB's per-column-family size and background-work
    /// properties.  This intentionally does not enable RocksDB statistics:
    /// enabling counters changes the workload being measured.  Receipts should
    /// record these snapshots before and after a workload alongside recursive
    /// on-disk directory bytes and machine metadata.
    pub fn metrics(&self) -> Result<RocksDbMetrics, Error> {
        let mut total_sst = Vec::new();
        let mut live_sst = Vec::new();
        let mut live_data = Vec::new();
        let mut memtables = Vec::new();
        let mut pending_compaction = Vec::new();
        let mut flush_pending = Vec::new();
        let mut compaction_pending = Vec::new();
        for name in &self.column_families {
            let Some(handle) = self.db.cf_handle(name) else {
                continue;
            };
            let property = |property| self.db.property_int_value_cf(handle, property);
            total_sst.push(property(properties::TOTAL_SST_FILES_SIZE).storage()?);
            live_sst.push(property(properties::LIVE_SST_FILES_SIZE).storage()?);
            live_data.push(property(properties::ESTIMATE_LIVE_DATA_SIZE).storage()?);
            memtables.push(property(properties::SIZE_ALL_MEM_TABLES).storage()?);
            pending_compaction
                .push(property(properties::ESTIMATE_PENDING_COMPACTION_BYTES).storage()?);
            flush_pending.push(property(properties::MEM_TABLE_FLUSH_PENDING).storage()?);
            compaction_pending.push(property(properties::COMPACTION_PENDING).storage()?);
        }
        let global = |property| self.db.property_int_value(property);
        Ok(RocksDbMetrics {
            total_sst_bytes: sum_available(&total_sst),
            live_sst_bytes: sum_available(&live_sst),
            estimated_live_data_bytes: sum_available(&live_data),
            memtable_bytes: sum_available(&memtables),
            pending_compaction_bytes: sum_available(&pending_compaction),
            running_flushes: global(properties::NUM_RUNNING_FLUSHES).storage()?,
            running_compactions: global(properties::NUM_RUNNING_COMPACTIONS).storage()?,
            flush_pending: any_available(&flush_pending),
            compaction_pending: any_available(&compaction_pending),
        })
    }
}

fn sum_available(values: &[Option<u64>]) -> Option<u64> {
    values.iter().try_fold(0u64, |sum, value| {
        value.map(|value| sum.saturating_add(value))
    })
}

fn any_available(values: &[Option<u64>]) -> Option<bool> {
    values
        .iter()
        .try_fold(false, |any, value| value.map(|value| any || value != 0))
}

fn rocksdb_options(block_cache: &Cache, write_buffer_manager: &WriteBufferManager) -> Options {
    rocksdb_options_for_profile(
        RocksDbClassProfile::Default,
        block_cache,
        write_buffer_manager,
    )
}

fn rocksdb_options_for_cf(
    cf: &str,
    block_cache: &Cache,
    write_buffer_manager: &WriteBufferManager,
) -> Options {
    rocksdb_options_for_profile(rocksdb_class_profile(cf), block_cache, write_buffer_manager)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RocksDbClassProfile {
    Default,
    AppendRange,
    OverwriteHot,
    Meta,
}

fn rocksdb_class_profile(cf: &str) -> RocksDbClassProfile {
    match cf {
        CLASS_HISTORY_CF | CLASS_REGISTER_CF | CLASS_CHANGES_CF => RocksDbClassProfile::AppendRange,
        CLASS_GLOBAL_CURRENT_CF | CLASS_AHEAD_CURRENT_CF | CLASS_INDICES_CF => {
            RocksDbClassProfile::OverwriteHot
        }
        CLASS_META_CF => RocksDbClassProfile::Meta,
        _ => RocksDbClassProfile::Default,
    }
}

fn rocksdb_options_for_profile(
    profile: RocksDbClassProfile,
    block_cache: &Cache,
    write_buffer_manager: &WriteBufferManager,
) -> Options {
    let mut block_options = BlockBasedOptions::default();
    if profile.uses_blooms() {
        block_options.set_bloom_filter(10.0, false);
    }
    block_options.set_block_size(profile.block_size());
    block_options.set_block_cache(block_cache);

    let mut options = Options::default();
    options.set_block_based_table_factory(&block_options);
    options.set_write_buffer_manager(write_buffer_manager);
    options.set_target_file_size_base(profile.target_file_size());
    options.set_compression_type(profile.compression());
    options.set_bottommost_compression_type(profile.bottommost_compression());
    options.set_merge_operator(
        "groove_delta",
        rocksdb_full_merge_delta,
        rocksdb_partial_merge_delta,
    );
    if matches!(profile, RocksDbClassProfile::AppendRange) {
        let mut universal = UniversalCompactOptions::default();
        universal.set_size_ratio(20);
        universal.set_min_merge_width(4);
        universal.set_max_size_amplification_percent(50);
        universal.set_compression_size_percent(-1);
        options.set_compaction_style(DBCompactionStyle::Universal);
        options.set_universal_compaction_options(&universal);
    }
    options
}

impl RocksDbClassProfile {
    fn uses_blooms(self) -> bool {
        match self {
            // History/register/changes are consumed as prefix/range/latest scans.
            // Current/index/meta classes still have real point probes.
            Self::AppendRange => false,
            Self::Default | Self::OverwriteHot | Self::Meta => true,
        }
    }

    fn block_size(self) -> usize {
        match self {
            Self::AppendRange => ROCKSDB_LARGE_BLOCK_BYTES,
            Self::Default | Self::OverwriteHot | Self::Meta => ROCKSDB_DEFAULT_BLOCK_BYTES,
        }
    }

    fn target_file_size(self) -> u64 {
        match self {
            Self::AppendRange => ROCKSDB_APPEND_TARGET_FILE_BYTES,
            Self::Default | Self::OverwriteHot | Self::Meta => ROCKSDB_OVERWRITE_TARGET_FILE_BYTES,
        }
    }

    fn compression(self) -> DBCompressionType {
        match self {
            Self::AppendRange => DBCompressionType::Zstd,
            Self::Default | Self::OverwriteHot | Self::Meta => DBCompressionType::Lz4,
        }
    }

    fn bottommost_compression(self) -> DBCompressionType {
        DBCompressionType::Zstd
    }
}

impl ReopenableStorage for RocksDbStorage {
    fn reopen(self, column_families: Vec<String>) -> StorageFuture<'static, Result<Self, Error>> {
        Box::pin(async move {
            if column_families
                .iter()
                .all(|name| self.column_families.contains(name))
            {
                return Ok(self);
            }
            let path = self.path.clone();
            let durability = self.durability;
            drop(self);
            let column_families = column_families
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>();
            Self::open_with_durability(path, &column_families, durability)
        })
    }
}

fn validate_or_initialize_rocks_value_format(
    path: &Path,
    column_families: &[String],
    block_cache: &Cache,
    write_buffer_manager: &WriteBufferManager,
    write_options: &WriteOptions,
) -> Result<(), Error> {
    let mut options = rocksdb_options(block_cache, write_buffer_manager);
    options.create_if_missing(false);
    options.create_missing_column_families(false);
    let descriptors = column_families
        .iter()
        .map(String::as_str)
        .filter(|name| *name != "default")
        .map(|name| {
            ColumnFamilyDescriptor::new(
                name,
                rocksdb_options_for_cf(name, block_cache, write_buffer_manager),
            )
        });
    let mut db = DB::open_cf_descriptors(&options, path, descriptors).storage()?;

    if column_families.iter().any(|cf| cf == ROCKSDB_INTERNAL_CF) {
        let internal_cf = db
            .cf_handle(ROCKSDB_INTERNAL_CF)
            .expect("listed RocksDB column family was opened");
        match db
            .get_cf(internal_cf, ROCKSDB_VALUE_FORMAT_KEY)
            .storage()?
            .as_deref()
        {
            Some(ROCKSDB_VALUE_FORMAT_V2) => return Ok(()),
            Some(_) => {
                return Err(Error::InvalidStorageLayout(
                    "incompatible RocksDB storage format: unsupported private value format marker"
                        .to_owned(),
                ));
            }
            None => {}
        }
    }

    let is_empty = column_families.iter().all(|cf| {
        let first = if cf == "default" {
            db.iterator(IteratorMode::Start).next()
        } else {
            db.iterator_cf(
                db.cf_handle(cf)
                    .expect("listed RocksDB column family was opened"),
                IteratorMode::Start,
            )
            .next()
        };
        first.is_none()
    });
    if !is_empty {
        return Err(Error::InvalidStorageLayout(
            "incompatible RocksDB storage format: non-empty store is missing the v2 private value format marker"
                .to_owned(),
        ));
    }

    if db.cf_handle(ROCKSDB_INTERNAL_CF).is_none() {
        db.create_cf(
            ROCKSDB_INTERNAL_CF,
            &rocksdb_options_for_cf(ROCKSDB_INTERNAL_CF, block_cache, write_buffer_manager),
        )
        .storage()?;
    }
    db.put_cf_opt(
        db.cf_handle(ROCKSDB_INTERNAL_CF)
            .expect("new RocksDB internal column family has a handle"),
        ROCKSDB_VALUE_FORMAT_KEY,
        ROCKSDB_VALUE_FORMAT_V2,
        write_options,
    )
    .storage()
}

impl OrderedKvStorage for RocksDbStorage {
    fn get(&self, cf: String, key: Vec<u8>) -> StorageFuture<'_, Result<Option<Value>, Error>> {
        Box::pin(async move {
            let value = if cf == "default" {
                self.db.get(key).storage()
            } else {
                self.db.get_cf(self.cf_handle(&cf)?, key).storage()
            }?;
            value
                .map(|value| decode_rocks_value(&value))
                .transpose()
                .map(Option::flatten)
        })
    }

    fn approximate_class_bytes(&self, cf: String) -> StorageFuture<'_, Result<Option<u64>, Error>> {
        Box::pin(async move {
            if cf == "default" {
                let sst = self
                    .db
                    .property_int_value(properties::TOTAL_SST_FILES_SIZE)
                    .storage()?
                    .unwrap_or(0);
                let mem = self
                    .db
                    .property_int_value(properties::SIZE_ALL_MEM_TABLES)
                    .storage()?
                    .unwrap_or(0);
                return Ok(Some(sst.saturating_add(mem)));
            }
            let handle = self.cf_handle(&cf)?;
            let sst = self
                .db
                .property_int_value_cf(handle, properties::TOTAL_SST_FILES_SIZE)
                .storage()?
                .unwrap_or(0);
            let mem = self
                .db
                .property_int_value_cf(handle, properties::SIZE_ALL_MEM_TABLES)
                .storage()?
                .unwrap_or(0);
            Ok(Some(sst.saturating_add(mem)))
        })
    }

    fn set(
        &self,
        cf: String,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> StorageFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            let value = encode_rocks_value(Some(&value));
            if cf == "default" {
                self.db.put_opt(key, value, &self.write_options).storage()
            } else {
                self.db
                    .put_cf_opt(self.cf_handle(&cf)?, key, value, &self.write_options)
                    .storage()
            }
        })
    }

    fn delete(&self, cf: String, key: Vec<u8>) -> StorageFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            if cf == "default" {
                self.db.delete_opt(key, &self.write_options).storage()
            } else {
                self.db
                    .delete_cf_opt(self.cf_handle(&cf)?, key, &self.write_options)
                    .storage()
            }
        })
    }

    fn close(&self) -> StorageFuture<'_, Result<(), Error>> {
        self.flush_write_boundary()
    }

    fn set_write_flush_cadence(&self, every: usize) -> StorageFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            assert!(every > 0, "write flush cadence must be non-zero");
            *self.write_flush_cadence.borrow_mut() = Some(WriteFlushCadence { every, pending: 0 });
            Ok(())
        })
    }

    fn flush_write_boundary(&self) -> StorageFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            self.flush_wal(true)?;
            if let Some(cadence) = self.write_flush_cadence.borrow_mut().as_mut() {
                cadence.pending = 0;
            }
            Ok(())
        })
    }

    fn scan(&self, request: ScanRequest) -> StorageFuture<'_, Result<StorageScan<'_>, Error>> {
        Box::pin(async move {
            let ScanRequest {
                cf,
                bounds,
                direction,
                max_items,
            } = request;
            if max_items == Some(0) {
                if cf != "default" {
                    self.cf_handle(&cf)?;
                }
                return Ok(
                    Box::new(groove::storage::ReadyStorageCursor::new(Vec::new()))
                        as StorageScan<'_>,
                );
            }
            let (start, upper_bound, prefix) = match bounds {
                ScanBounds::Range { start, end } => (start, Some(end), None),
                ScanBounds::Prefix(prefix) => {
                    let mut upper_bound = prefix.clone();
                    let upper_bound =
                        advance_prefix_upper_bound(&mut upper_bound).then_some(upper_bound);
                    (prefix.clone(), upper_bound, Some(prefix))
                }
            };
            let iterator = match (direction, upper_bound.as_deref()) {
                (ScanDirection::Reverse, Some(end)) => {
                    let mode = IteratorMode::From(end, Direction::Reverse);
                    if cf == "default" {
                        self.db.iterator(mode)
                    } else {
                        self.db.iterator_cf(self.cf_handle(&cf)?, mode)
                    }
                }
                (ScanDirection::Reverse, None) => {
                    let mode = IteratorMode::End;
                    if cf == "default" {
                        self.db.iterator(mode)
                    } else {
                        self.db.iterator_cf(self.cf_handle(&cf)?, mode)
                    }
                }
                (ScanDirection::Forward, Some(end)) => {
                    let mut options = ReadOptions::default();
                    options.set_iterate_upper_bound(end.to_vec());
                    let mode = IteratorMode::From(&start, Direction::Forward);
                    if cf == "default" {
                        self.db.iterator_opt(mode, options)
                    } else {
                        self.db.iterator_cf_opt(self.cf_handle(&cf)?, options, mode)
                    }
                }
                (ScanDirection::Forward, None) => {
                    let mode = IteratorMode::From(&start, Direction::Forward);
                    if cf == "default" {
                        self.db.iterator(mode)
                    } else {
                        self.db.iterator_cf(self.cf_handle(&cf)?, mode)
                    }
                }
            };
            Ok(Box::new(RocksDbCursor::new(
                iterator,
                prefix,
                start,
                upper_bound,
                max_items,
            )) as StorageScan<'_>)
        })
    }

    fn write_many(
        &self,
        operations: Vec<OwnedWriteOperation>,
    ) -> StorageFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            let mut batch = WriteBatch::default();

            for operation in operations {
                match operation {
                    OwnedWriteOperation::Set { cf, key, value } => {
                        let value = encode_rocks_value(Some(&value));
                        if cf == "default" {
                            batch.put(key, value);
                        } else {
                            batch.put_cf(self.cf_handle(&cf)?, key, value);
                        }
                    }
                    OwnedWriteOperation::Delete { cf, key } => {
                        if cf == "default" {
                            batch.delete(key);
                        } else {
                            batch.delete_cf(self.cf_handle(&cf)?, key);
                        }
                    }
                    OwnedWriteOperation::Delta { cf, key, delta } => {
                        if cf == "default" {
                            batch.merge(key, delta.encode()?);
                        } else {
                            batch.merge_cf(self.cf_handle(&cf)?, key, delta.encode()?);
                        }
                    }
                }
            }

            let should_flush = match self.write_flush_cadence.borrow_mut().as_mut() {
                Some(cadence) => {
                    cadence.pending += 1;
                    if cadence.pending == cadence.every {
                        cadence.pending = 0;
                        true
                    } else {
                        false
                    }
                }
                None => return self.db.write_opt(&batch, &self.write_options).storage(),
            };
            let mut write_options = WriteOptions::default();
            write_options.disable_wal(false);
            self.db.write_opt(&batch, &write_options).storage()?;
            if should_flush {
                self.flush_wal(true)?;
            }
            Ok(())
        })
    }

    fn column_family_names(&self) -> Option<Vec<String>> {
        Some(self.column_families.iter().cloned().collect())
    }
}

fn rocksdb_full_merge_delta(
    _key: &[u8],
    old_value: Option<&[u8]>,
    operands: &MergeOperands,
) -> Option<Vec<u8>> {
    apply_merge_operands(old_value, operands).ok()
}

fn rocksdb_partial_merge_delta(
    _key: &[u8],
    left_operand: Option<&[u8]>,
    operands: &MergeOperands,
) -> Option<Vec<u8>> {
    if left_operand
        .is_some_and(|operand| !matches!(storage_delta_requires_full_merge(operand), Ok(false)))
        || operands
            .iter()
            .any(|operand| !matches!(storage_delta_requires_full_merge(operand), Ok(false)))
    {
        return None;
    }
    let mut value = match left_operand {
        Some(operand) => apply_storage_delta(None, operand).ok()?,
        None => None,
    };
    let template = left_operand.or_else(|| operands.iter().next())?;
    for operand in operands {
        value = apply_storage_delta(value.as_deref(), operand).ok()?;
    }
    compact_storage_delta_operand(template, value?).ok()
}

fn apply_merge_operands(
    initial: Option<&[u8]>,
    operands: &MergeOperands,
) -> Result<Vec<u8>, Error> {
    let mut value = initial.map(decode_rocks_value).transpose()?.flatten();
    for operand in operands {
        value = apply_storage_delta(value.as_deref(), operand)?;
    }
    Ok(encode_rocks_value(value.as_deref()))
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

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::pin;
    use std::task::{Context, Poll, Waker};

    use crate::{
        CLASS_AHEAD_CURRENT_CF, CLASS_CHANGES_CF, CLASS_GLOBAL_CURRENT_CF, CLASS_HISTORY_CF,
        CLASS_INDICES_CF, CLASS_META_CF, CLASS_REGISTER_CF, Cache, ColumnFamilyDescriptor, DB,
        Options, ROCKSDB_BLOCK_CACHE_BYTES, ROCKSDB_INTERNAL_CF, ROCKSDB_VALUE_FORMAT_KEY,
        ROCKSDB_VALUE_FORMAT_V2, ROCKSDB_WRITE_BUFFER_MANAGER_BYTES, RocksDbClassProfile,
        RocksDbStorage, WriteBufferManager, any_available, rocksdb_class_profile, rocksdb_options,
        rocksdb_options_for_cf, sum_available,
    };
    use groove::storage::{OwnedWriteOperation, StorageDelta, storage_delta_requires_full_merge};

    fn ready<F: Future>(future: F) -> F::Output {
        let mut future = pin!(future);
        let mut cx = Context::from_waker(Waker::noop());
        match future.as_mut().poll(&mut cx) {
            Poll::Ready(output) => output,
            Poll::Pending => panic!("RocksDB storage operation unexpectedly suspended"),
        }
    }

    #[test]
    fn class_cfs_select_storage_physics_profiles() {
        for cf in [CLASS_HISTORY_CF, CLASS_REGISTER_CF, CLASS_CHANGES_CF] {
            let profile = rocksdb_class_profile(cf);
            assert_eq!(profile, RocksDbClassProfile::AppendRange);
            assert!(
                !profile.uses_blooms(),
                "{cf} should not build point-probe blooms"
            );
        }

        for cf in [
            CLASS_GLOBAL_CURRENT_CF,
            CLASS_AHEAD_CURRENT_CF,
            CLASS_INDICES_CF,
        ] {
            let profile = rocksdb_class_profile(cf);
            assert_eq!(profile, RocksDbClassProfile::OverwriteHot);
            assert!(profile.uses_blooms(), "{cf} should keep point-probe blooms");
        }

        assert_eq!(
            rocksdb_class_profile(CLASS_META_CF),
            RocksDbClassProfile::Meta
        );
        assert_eq!(
            rocksdb_class_profile("ordinary"),
            RocksDbClassProfile::Default
        );
    }

    #[test]
    fn ordinary_rocksdb_open_does_not_enable_client_flush_cadence() {
        // Server storage follows this ordinary open path. The client-only
        // cadence must stay opt-in so its durability behavior is unchanged.
        let dir = tempfile::tempdir().unwrap();
        let storage = RocksDbStorage::open(dir.path(), &["records"]).unwrap();
        assert!(storage.write_flush_cadence.borrow().is_none());
    }

    #[test]
    fn close_flushes_a_partial_write_cadence() {
        use groove::storage::{OrderedKvStorage, OwnedWriteOperation};

        let dir = tempfile::tempdir().unwrap();
        let storage = RocksDbStorage::open(dir.path(), &["records"]).unwrap();
        ready(storage.set_write_flush_cadence(8)).unwrap();
        ready(storage.write_many(vec![OwnedWriteOperation::Set {
            cf: "records".to_owned(),
            key: b"pending".to_vec(),
            value: b"value".to_vec(),
        }]))
        .unwrap();
        assert_eq!(
            storage
                .write_flush_cadence
                .borrow()
                .as_ref()
                .map(|cadence| cadence.pending),
            Some(1)
        );

        ready(storage.close()).unwrap();

        assert_eq!(
            storage.last_wal_flush_sync.get(),
            Some(true),
            "close must synchronously flush the RocksDB WAL"
        );

        assert_eq!(
            storage
                .write_flush_cadence
                .borrow()
                .as_ref()
                .map(|cadence| cadence.pending),
            Some(0)
        );
    }

    #[test]
    fn conditional_insert_merge_keeps_the_first_durable_value() {
        use groove::storage::OrderedKvStorage;

        let dir = tempfile::tempdir().unwrap();
        let storage = RocksDbStorage::open(dir.path(), &["chunks"]).unwrap();
        let key = b"same-opaque-locator".to_vec();
        ready(storage.write_many(vec![OwnedWriteOperation::Delta {
            cf: "chunks".to_owned(),
            key: key.clone(),
            delta: StorageDelta::set_if_absent(b"first authenticated bytes".to_vec()),
        }]))
        .unwrap();
        ready(storage.write_many(vec![OwnedWriteOperation::Delta {
            cf: "chunks".to_owned(),
            key: key.clone(),
            delta: StorageDelta::set_if_absent(b"second conflicting bytes".to_vec()),
        }]))
        .unwrap();

        assert_eq!(
            ready(storage.get("chunks".to_owned(), key)).unwrap(),
            Some(b"first authenticated bytes".to_vec())
        );
    }

    #[test]
    fn conditional_operands_defer_to_full_merges() {
        assert!(
            storage_delta_requires_full_merge(
                &StorageDelta::set_if_absent(b"first authenticated bytes".to_vec())
                    .encode()
                    .unwrap()
            )
            .unwrap()
        );
        assert!(
            storage_delta_requires_full_merge(
                &StorageDelta::delete_if_value_matches(b"first authenticated bytes".to_vec())
                    .encode()
                    .unwrap()
            )
            .unwrap()
        );
    }

    #[test]
    fn conditional_delete_merge_removes_only_the_matching_durable_value() {
        use groove::storage::OrderedKvStorage;

        let dir = tempfile::tempdir().unwrap();
        let storage = RocksDbStorage::open(dir.path(), &["chunks"]).unwrap();
        let key = b"same-opaque-locator".to_vec();
        let old = b"old authenticated bytes".to_vec();
        let new = b"new authenticated bytes".to_vec();
        ready(storage.set("chunks".to_owned(), key.clone(), old.clone())).unwrap();

        ready(storage.write_many(vec![OwnedWriteOperation::Delta {
            cf: "chunks".to_owned(),
            key: key.clone(),
            delta: StorageDelta::delete_if_value_matches(b"different bytes".to_vec()),
        }]))
        .unwrap();
        assert_eq!(
            ready(storage.get("chunks".to_owned(), key.clone())).unwrap(),
            Some(old.clone())
        );

        ready(storage.write_many(vec![OwnedWriteOperation::Delta {
            cf: "chunks".to_owned(),
            key: key.clone(),
            delta: StorageDelta::delete_if_value_matches(old),
        }]))
        .unwrap();
        assert_eq!(
            ready(storage.get("chunks".to_owned(), key.clone())).unwrap(),
            None
        );

        ready(storage.write_many(vec![OwnedWriteOperation::Delta {
            cf: "chunks".to_owned(),
            key: key.clone(),
            delta: StorageDelta::set_if_absent(new.clone()),
        }]))
        .unwrap();
        assert_eq!(
            ready(storage.get("chunks".to_owned(), key)).unwrap(),
            Some(new)
        );
    }

    #[test]
    fn former_delta_tombstone_bytes_are_an_ordinary_value_in_get_and_scan() {
        use groove::storage::{OrderedKvStorage, ScanRequest, collect_scan};

        let dir = tempfile::tempdir().unwrap();
        let storage = RocksDbStorage::open(dir.path(), &["records"]).unwrap();
        let key = b"former-tombstone".to_vec();
        let value = b"\0groove-storage-delta-tombstone-v1".to_vec();

        ready(storage.set("records".to_owned(), key.clone(), value.clone())).unwrap();
        assert_eq!(
            ready(storage.get("records".to_owned(), key.clone())).unwrap(),
            Some(value.clone())
        );
        assert_eq!(
            ready(async {
                collect_scan(
                    storage
                        .scan(ScanRequest::prefix("records".to_owned(), Vec::new()))
                        .await
                        .unwrap(),
                )
                .await
                .unwrap()
            }),
            vec![(key.clone(), value.clone())]
        );

        ready(storage.write_many(vec![OwnedWriteOperation::Delta {
            cf: "records".to_owned(),
            key: key.clone(),
            delta: StorageDelta::delete_if_value_matches(value.clone()),
        }]))
        .unwrap();
        assert_eq!(ready(storage.get("records".to_owned(), key)).unwrap(), None);
        assert!(
            ready(async {
                collect_scan(
                    storage
                        .scan(ScanRequest::prefix("records".to_owned(), Vec::new()))
                        .await
                        .unwrap(),
                )
                .await
                .unwrap()
            })
            .is_empty()
        );
    }

    #[test]
    fn unmarked_nonempty_store_is_rejected_repeatedly_without_mutation() {
        const FORMER_SENTINEL: &[u8] = b"\0groove-storage-delta-tombstone-v1";
        let dir = tempfile::tempdir().unwrap();
        let block_cache = Cache::new_lru_cache(ROCKSDB_BLOCK_CACHE_BYTES);
        let write_buffer_manager =
            WriteBufferManager::new_write_buffer_manager(ROCKSDB_WRITE_BUFFER_MANAGER_BYTES, false);
        let mut options = rocksdb_options(&block_cache, &write_buffer_manager);
        options.create_if_missing(true);
        options.create_missing_column_families(true);
        let db = DB::open_cf_descriptors(
            &options,
            dir.path(),
            [ColumnFamilyDescriptor::new(
                "records",
                rocksdb_options_for_cf("records", &block_cache, &write_buffer_manager),
            )],
        )
        .unwrap();
        let records = db.cf_handle("records").unwrap();
        db.put_cf(records, b"ordinary", FORMER_SENTINEL).unwrap();
        drop(db);

        for _ in 0..2 {
            let error = match RocksDbStorage::open(dir.path(), &["records", "must-not-be-created"])
            {
                Ok(_) => panic!("an unmarked non-empty store must fail closed"),
                Err(error) => error,
            };
            assert!(matches!(
                error,
                groove::storage::Error::InvalidStorageLayout(message)
                    if message.contains("incompatible RocksDB storage format")
                        && message.contains("non-empty")
            ));

            let cfs = DB::list_cf(&Options::default(), dir.path()).unwrap();
            assert!(!cfs.iter().any(|cf| cf == ROCKSDB_INTERNAL_CF));
            assert!(!cfs.iter().any(|cf| cf == "must-not-be-created"));
            let db = DB::open_cf_descriptors(
                &options,
                dir.path(),
                [ColumnFamilyDescriptor::new(
                    "records",
                    rocksdb_options_for_cf("records", &block_cache, &write_buffer_manager),
                )],
            )
            .unwrap();
            assert_eq!(
                db.get_cf(db.cf_handle("records").unwrap(), b"ordinary")
                    .unwrap()
                    .as_deref(),
                Some(FORMER_SENTINEL)
            );
            drop(db);
        }
    }

    #[test]
    fn empty_unmarked_store_initializes_v2_and_reopens() {
        use groove::storage::OrderedKvStorage;

        let dir = tempfile::tempdir().unwrap();
        let block_cache = Cache::new_lru_cache(ROCKSDB_BLOCK_CACHE_BYTES);
        let write_buffer_manager =
            WriteBufferManager::new_write_buffer_manager(ROCKSDB_WRITE_BUFFER_MANAGER_BYTES, false);
        let mut options = rocksdb_options(&block_cache, &write_buffer_manager);
        options.create_if_missing(true);
        options.create_missing_column_families(true);
        let db = DB::open_cf_descriptors(
            &options,
            dir.path(),
            [ColumnFamilyDescriptor::new(
                "records",
                rocksdb_options_for_cf("records", &block_cache, &write_buffer_manager),
            )],
        )
        .unwrap();
        drop(db);

        let storage = RocksDbStorage::open(dir.path(), &["records"]).unwrap();
        ready(storage.set("records".to_owned(), b"empty".to_vec(), Vec::new())).unwrap();
        drop(storage);
        let storage = RocksDbStorage::open(dir.path(), &["records"]).unwrap();
        assert_eq!(
            ready(storage.get("records".to_owned(), b"empty".to_vec())).unwrap(),
            Some(Vec::new())
        );
        assert_eq!(
            storage
                .db
                .get_cf(
                    storage.db.cf_handle(ROCKSDB_INTERNAL_CF).unwrap(),
                    ROCKSDB_VALUE_FORMAT_KEY,
                )
                .unwrap()
                .as_deref(),
            Some(ROCKSDB_VALUE_FORMAT_V2)
        );
    }

    #[test]
    fn pending_conditional_merge_survives_v2_reopen() {
        use groove::storage::OrderedKvStorage;

        let dir = tempfile::tempdir().unwrap();
        let storage = RocksDbStorage::open(dir.path(), &["records"]).unwrap();
        let operand = StorageDelta::set_if_absent(b"installed".to_vec())
            .encode()
            .unwrap();
        storage
            .db
            .merge_cf(
                storage.db.cf_handle("records").unwrap(),
                b"pending",
                operand,
            )
            .unwrap();
        drop(storage);

        let storage = RocksDbStorage::open(dir.path(), &["records"]).unwrap();
        assert_eq!(
            ready(storage.get("records".to_owned(), b"pending".to_vec())).unwrap(),
            Some(b"installed".to_vec())
        );
    }

    #[test]
    fn approximate_class_bytes_reports_populated_family() {
        use groove::storage::OrderedKvStorage;

        let dir = tempfile::tempdir().unwrap();
        let storage = RocksDbStorage::open(dir.path(), &["records"]).unwrap();
        ready(storage.set("records".into(), b"a".to_vec(), b"one".to_vec())).unwrap();
        assert!(
            ready(storage.approximate_class_bytes("records".into()))
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn metrics_include_memtable_bytes_written_to_each_column_family() {
        use groove::storage::OrderedKvStorage;

        let dir = tempfile::tempdir().unwrap();
        let storage = RocksDbStorage::open(dir.path(), &["left", "right"]).unwrap();
        let before = storage.metrics().unwrap();
        ready(storage.set("left".into(), b"a".to_vec(), vec![7; 32 * 1024])).unwrap();
        ready(storage.set("right".into(), b"b".to_vec(), vec![9; 32 * 1024])).unwrap();
        let after = storage.metrics().unwrap();

        assert!(
            after.memtable_bytes.unwrap() > before.memtable_bytes.unwrap(),
            "two writes must be visible to the per-CF metric aggregation: before={before:?}, after={after:?}"
        );
    }

    #[test]
    fn metrics_include_default_cf_and_keep_unavailable_aggregates_unknown() {
        assert_eq!(sum_available(&[Some(2), Some(3)]), Some(5));
        assert_eq!(sum_available(&[Some(2), None, Some(3)]), None);
        assert_eq!(any_available(&[Some(0), Some(1)]), Some(true));
        assert_eq!(any_available(&[Some(0), None]), None);

        let dir = tempfile::tempdir().unwrap();
        let storage = RocksDbStorage::open(dir.path(), &["left", "right"]).unwrap();
        assert!(storage.column_families.contains("default"));
        assert_eq!(
            storage.metrics().unwrap().running_flushes,
            storage
                .db
                .property_int_value(rocksdb::properties::NUM_RUNNING_FLUSHES)
                .unwrap()
        );
    }
}
