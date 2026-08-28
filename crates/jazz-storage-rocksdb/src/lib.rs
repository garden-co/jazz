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
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use rocksdb::{
    BlockBasedOptions, Cache, ColumnFamilyDescriptor, DB, DBCompactionStyle, DBCompressionType,
    DBIteratorWithThreadMode, Direction, IteratorMode, Options, ReadOptions,
    UniversalCompactOptions, WriteBatch, WriteBufferManager, WriteOptions, properties,
};
use serde::Serialize;

use groove::storage::{
    BoxedStorage, ColumnFamilyName, Error, KeyValue, OrderedKvStorage, OwnedWriteOperation,
    ReopenableStorage, ScanBounds, ScanDirection, ScanRequest, StorageCursor, StorageEpochManifest,
    StorageFactory, StorageFuture, StorageScan, Value, WriteManyOutcome,
    validate_physical_storage_names,
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
// Keep desktop reopen recovery debt bounded by allowing RocksDB to flush
// incrementally during sustained writes. A 256 MiB budget left realistic
// local datasets entirely in the WAL until exit; 16 MiB is the measured knee
// between reopen latency and foreground ingest cost (see #2104).
const ROCKSDB_WRITE_BUFFER_MANAGER_BYTES: usize = 16 * 1024 * 1024;
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
const ROCKSDB_INTERNAL_CF: &str = "__groove_storage_internal_v3";
const ROCKSDB_VALUE_FORMAT_KEY: &[u8] = b"value-format";
const ROCKSDB_VALUE_FORMAT_V3: &[u8] = b"raw-v3";
const ROCKSDB_EPOCH_MANIFEST_KEY: &[u8] = b"epoch-manifest";

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
    mutation_gate: Mutex<()>,
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
                batch.push((key.into_vec(), value.into_vec()));
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
        validate_physical_storage_names(column_families)?;
        let path = path.as_ref().to_path_buf();
        if column_families.contains(&ROCKSDB_INTERNAL_CF) {
            return Err(Error::InvalidStorageLayout(
                "RocksDB internal column family name is reserved".to_owned(),
            ));
        }
        // Share one 256MB block cache and one bounded write-buffer budget across
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

        let listed_column_families = inspect_existing_column_families(&path)?;
        let initialize_format = match &listed_column_families {
            Some(existing) => {
                validate_physical_storage_names(existing)?;
                validate_raw_v3_store(&path, existing, &block_cache, &write_buffer_manager)?
            }
            None => true,
        };

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
            .expect("internal RocksDB column family was opened");
        if initialize_format {
            let mut batch = WriteBatch::default();
            batch.put_cf(
                internal_cf,
                ROCKSDB_VALUE_FORMAT_KEY,
                ROCKSDB_VALUE_FORMAT_V3,
            );
            batch.put_cf(
                internal_cf,
                ROCKSDB_EPOCH_MANIFEST_KEY,
                rocksdb_manifest()?.encode()?,
            );
            db.write_opt(&batch, &write_options).storage()?;
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
            mutation_gate: Mutex::new(()),
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

/// Distinguish a root that is genuinely absent or directory-empty from one
/// RocksDB cannot inspect. Treating every `list_cf` failure as a fresh store
/// would let a malformed existing root reach `create_if_missing`.
fn inspect_existing_column_families(path: &Path) -> Result<Option<Vec<String>>, Error> {
    if !path.try_exists().map_err(|error| Error::Backend {
        backend: "rocksdb",
        message: format!("could not inspect RocksDB storage root: {error}"),
    })? {
        return Ok(None);
    }

    let mut entries = std::fs::read_dir(path).map_err(|error| {
        Error::InvalidStorageLayout(format!(
            "existing RocksDB storage root is not an inspectable directory: {error}"
        ))
    })?;
    if entries
        .next()
        .transpose()
        .map_err(|error| {
            Error::InvalidStorageLayout(format!(
                "could not inspect existing RocksDB storage root: {error}"
            ))
        })?
        .is_none()
    {
        return Ok(None);
    }

    DB::list_cf(&Options::default(), path)
        .map(Some)
        .map_err(|error| {
            Error::InvalidStorageLayout(format!(
                "could not inspect existing RocksDB storage manifest: {error}"
            ))
        })
}

fn validate_raw_v3_store(
    path: &Path,
    column_families: &[String],
    block_cache: &Cache,
    write_buffer_manager: &WriteBufferManager,
) -> Result<bool, Error> {
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
    let db = DB::open_cf_descriptors(&options, path, descriptors).storage()?;
    if let Some(internal) = db.cf_handle(ROCKSDB_INTERNAL_CF) {
        match db
            .get_cf(internal, ROCKSDB_VALUE_FORMAT_KEY)
            .storage()?
            .as_deref()
        {
            Some(ROCKSDB_VALUE_FORMAT_V3) => {
                let manifest = db
                    .get_cf(internal, ROCKSDB_EPOCH_MANIFEST_KEY)
                    .storage()?
                    .ok_or_else(|| {
                        Error::InvalidStorageLayout("missing RocksDB epoch manifest".to_owned())
                    })?;
                rocksdb_manifest()?.admit_existing(&manifest)?;
                return Ok(false);
            }
            Some(_) => {
                return Err(Error::InvalidStorageLayout(
                    "incompatible RocksDB storage format marker".to_owned(),
                ));
            }
            None => {}
        }
    }
    let empty = column_families.iter().all(|cf| {
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
    if empty {
        Ok(true)
    } else {
        Err(Error::InvalidStorageLayout(
            "unmarked non-empty RocksDB store cannot be opened as raw-v3".to_owned(),
        ))
    }
}

fn rocksdb_manifest() -> Result<StorageEpochManifest, Error> {
    StorageEpochManifest::epoch_1(
        "rocksdb",
        3,
        BTreeMap::from([
            (
                "internal-cf".to_owned(),
                ROCKSDB_INTERNAL_CF.as_bytes().to_vec(),
            ),
            ("key-order".to_owned(), b"unsigned-lexicographic".to_vec()),
            (
                "rocksdb-comparator".to_owned(),
                b"rocksdb.bytewise.v1".to_vec(),
            ),
            ("value-format".to_owned(), ROCKSDB_VALUE_FORMAT_V3.to_vec()),
        ]),
    )
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
            validate_physical_storage_names(&column_families)?;
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

impl OrderedKvStorage for RocksDbStorage {
    fn get(&self, cf: String, key: Vec<u8>) -> StorageFuture<'_, Result<Option<Value>, Error>> {
        Box::pin(async move {
            let value = if cf == "default" {
                self.db.get(key).storage()
            } else {
                self.db.get_cf(self.cf_handle(&cf)?, key).storage()
            }?;
            Ok(value)
        })
    }

    fn put_if_absent(
        &self,
        cf: String,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> StorageFuture<'_, Result<Option<Value>, Error>> {
        Box::pin(async move {
            let _guard = self
                .mutation_gate
                .lock()
                .expect("RocksDB mutation gate poisoned");
            let existing = if cf == "default" {
                self.db.get(&key).storage()?
            } else {
                self.db.get_cf(self.cf_handle(&cf)?, &key).storage()?
            };
            if existing.is_some() {
                return Ok(existing);
            }
            if cf == "default" {
                self.db.put_opt(key, value, &self.write_options).storage()?;
            } else {
                self.db
                    .put_cf_opt(self.cf_handle(&cf)?, key, value, &self.write_options)
                    .storage()?;
            }
            Ok(None)
        })
    }

    fn compare_and_delete(
        &self,
        cf: String,
        key: Vec<u8>,
        expected: Vec<u8>,
    ) -> StorageFuture<'_, Result<bool, Error>> {
        Box::pin(async move {
            let _guard = self
                .mutation_gate
                .lock()
                .expect("RocksDB mutation gate poisoned");
            let existing = if cf == "default" {
                self.db.get(&key).storage()?
            } else {
                self.db.get_cf(self.cf_handle(&cf)?, &key).storage()?
            };
            if existing.as_deref() != Some(expected.as_slice()) {
                return Ok(false);
            }
            if cf == "default" {
                self.db.delete_opt(key, &self.write_options).storage()?;
            } else {
                self.db
                    .delete_cf_opt(self.cf_handle(&cf)?, key, &self.write_options)
                    .storage()?;
            }
            Ok(true)
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
            let _guard = self
                .mutation_gate
                .lock()
                .expect("RocksDB mutation gate poisoned");
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
            let _guard = self
                .mutation_gate
                .lock()
                .expect("RocksDB mutation gate poisoned");
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
                    let upper_bound = groove::storage::prefix_successor(&prefix);
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
            let _guard = self
                .mutation_gate
                .lock()
                .expect("RocksDB mutation gate poisoned");
            let mut batch = WriteBatch::default();

            for operation in operations {
                match operation {
                    OwnedWriteOperation::Set { cf, key, value } => {
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

    fn write_many_outcome(
        &self,
        operations: Vec<OwnedWriteOperation>,
    ) -> StorageFuture<'_, WriteManyOutcome> {
        Box::pin(async move {
            for operation in &operations {
                let cf = match operation {
                    OwnedWriteOperation::Set { cf, .. }
                    | OwnedWriteOperation::Delete { cf, .. } => cf,
                };
                if cf != "default"
                    && let Err(error) = self.cf_handle(cf)
                {
                    return WriteManyOutcome::Uncommitted(error);
                }
            }
            match self.write_many(operations).await {
                Ok(()) => WriteManyOutcome::Committed,
                Err(error) => WriteManyOutcome::PossiblyCommitted(error),
            }
        })
    }

    fn column_family_names(&self) -> Option<Vec<String>> {
        Some(self.column_families.iter().cloned().collect())
    }
}

#[cfg(test)]
mod tests {
    use groove::storage::{Error, OrderedKvStorage, ReopenableStorage};
    use std::future::Future;
    use std::pin::pin;
    use std::task::{Context, Poll, Waker};

    use crate::{
        CLASS_AHEAD_CURRENT_CF, CLASS_CHANGES_CF, CLASS_GLOBAL_CURRENT_CF, CLASS_HISTORY_CF,
        CLASS_INDICES_CF, CLASS_META_CF, CLASS_REGISTER_CF, ROCKSDB_EPOCH_MANIFEST_KEY,
        ROCKSDB_INTERNAL_CF, ROCKSDB_VALUE_FORMAT_KEY, ROCKSDB_VALUE_FORMAT_V3,
        RocksDbClassProfile, RocksDbStorage, any_available, inspect_existing_column_families,
        rocksdb_class_profile, rocksdb_manifest, sum_available,
    };
    use rocksdb::{ColumnFamilyDescriptor, DB, Options};

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
    fn open_rejects_nul_column_family_without_creating_or_panicking() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("must-not-exist");
        let too_long = "a".repeat(groove::storage::MAX_APPLICATION_STORAGE_NAME_BYTES + 1);
        for invalid in ["records\0evil", too_long.as_str()] {
            assert!(RocksDbStorage::open(&path, &[invalid]).is_err());
        }
        assert!(
            !path.exists(),
            "invalid family name created a RocksDB store or panicked"
        );
    }

    #[test]
    fn reopen_fast_path_rejects_invalid_existing_family_without_mutation() {
        let dir = tempfile::tempdir().unwrap();
        let invalid = "records\0evil";
        let mut storage = RocksDbStorage::open(dir.path(), &["records"]).unwrap();
        // Simulate a legacy/injected in-memory family catalogue: this used to
        // take the all-existing early return before physical-name validation.
        storage.column_families.insert(invalid.to_owned());
        assert!(ready(storage.reopen(vec![invalid.to_owned()])).is_err());
        assert!(
            !DB::list_cf(&Options::default(), dir.path())
                .unwrap()
                .iter()
                .any(|name| name == invalid),
            "reopen must reject before creating an invalid physical family"
        );
    }

    #[test]
    fn open_rejects_invalid_listed_family_before_opening_store() {
        let dir = tempfile::tempdir().unwrap();
        let invalid = "a".repeat(groove::storage::MAX_APPLICATION_STORAGE_NAME_BYTES + 1);
        let mut options = Options::default();
        options.create_if_missing(true);
        options.create_missing_column_families(true);
        let db = DB::open_cf_descriptors(
            &options,
            dir.path(),
            [ColumnFamilyDescriptor::new(&invalid, Options::default())],
        )
        .unwrap();
        drop(db);

        assert!(RocksDbStorage::open(dir.path(), &["must-not-be-admitted"]).is_err());
        assert!(
            !DB::list_cf(&Options::default(), dir.path())
                .unwrap()
                .iter()
                .any(|name| name == "must-not-be-admitted"),
            "open must reject before admitting requested families"
        );
    }

    #[test]
    fn raw_v3_round_trips_arbitrary_bytes_and_hides_its_marker() {
        use groove::storage::OrderedKvStorage;
        let dir = tempfile::tempdir().unwrap();
        let storage = RocksDbStorage::open(dir.path(), &["records"]).unwrap();
        for (key, value) in [
            (b"empty".to_vec(), Vec::new()),
            (b"former-sentinel".to_vec(), vec![0xff, 0, 0xff, 17]),
        ] {
            ready(storage.set("records".into(), key, value)).unwrap();
        }
        assert!(
            !storage
                .column_family_names()
                .unwrap()
                .iter()
                .any(|cf| cf == ROCKSDB_INTERNAL_CF)
        );
        drop(storage);
        let reopened = RocksDbStorage::open(dir.path(), &["records"]).unwrap();
        assert_eq!(
            ready(reopened.get("records".into(), b"empty".to_vec())).unwrap(),
            Some(Vec::new())
        );
        assert_eq!(
            ready(reopened.get("records".into(), b"former-sentinel".to_vec())).unwrap(),
            Some(vec![0xff, 0, 0xff, 17])
        );
    }

    #[test]
    fn rocksdb_epoch_manifest_freezes_marker_comparator_and_namespaces() {
        let dir = tempfile::tempdir().unwrap();
        let storage = RocksDbStorage::open(dir.path(), &[CLASS_HISTORY_CF, "records"]).unwrap();
        drop(storage);

        let db = DB::open_cf(
            &Options::default(),
            dir.path(),
            [CLASS_HISTORY_CF, "records", ROCKSDB_INTERNAL_CF],
        )
        .unwrap();
        let internal = db.cf_handle(ROCKSDB_INTERNAL_CF).unwrap();
        assert_eq!(
            db.get_cf(internal, ROCKSDB_VALUE_FORMAT_KEY).unwrap(),
            Some(ROCKSDB_VALUE_FORMAT_V3.to_vec())
        );
        assert_eq!(
            db.get_cf(internal, ROCKSDB_EPOCH_MANIFEST_KEY).unwrap(),
            Some(rocksdb_manifest().unwrap().encode().unwrap())
        );
        let families = DB::list_cf(&Options::default(), dir.path()).unwrap();
        assert!(families.contains(&CLASS_HISTORY_CF.to_owned()));
        assert!(families.contains(&ROCKSDB_INTERNAL_CF.to_owned()));
    }

    #[test]
    fn corrupt_epoch_manifest_is_rejected_before_admitting_requested_family() {
        let dir = tempfile::tempdir().unwrap();
        let storage = RocksDbStorage::open(dir.path(), &["records"]).unwrap();
        drop(storage);
        let db = DB::open_cf(
            &Options::default(),
            dir.path(),
            ["records", ROCKSDB_INTERNAL_CF],
        )
        .unwrap();
        db.put_cf(
            db.cf_handle(ROCKSDB_INTERNAL_CF).unwrap(),
            ROCKSDB_EPOCH_MANIFEST_KEY,
            b"not-jsm1",
        )
        .unwrap();
        drop(db);

        assert!(RocksDbStorage::open(dir.path(), &["records", "must-not-be-created"]).is_err());
        assert!(
            !DB::list_cf(&Options::default(), dir.path())
                .unwrap()
                .contains(&"must-not-be-created".to_owned())
        );
    }

    #[test]
    fn malformed_existing_root_is_rejected_before_rocksdb_can_mutate_it() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("malformed-rocksdb");
        std::fs::create_dir(&path).unwrap();
        let current = path.join("CURRENT");
        let contents = b"this is not a RocksDB manifest\n";
        std::fs::write(&current, contents).unwrap();

        // This is intentionally internal: the distinction between an absent
        // root and a failed physical inspection must hold before RocksDB's
        // mutating open path is reachable.
        assert!(matches!(
            inspect_existing_column_families(&path),
            Err(Error::InvalidStorageLayout(_))
        ));
        assert!(matches!(
            RocksDbStorage::open(&path, &["records"]),
            Err(Error::InvalidStorageLayout(_))
        ));
        assert_eq!(std::fs::read(&current).unwrap(), contents);
        let entries = std::fs::read_dir(&path)
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect::<Vec<_>>();
        assert_eq!(entries, vec![std::ffi::OsString::from("CURRENT")]);
    }

    #[test]
    fn unmarked_nonempty_store_is_rejected_without_mutation() {
        let dir = tempfile::tempdir().unwrap();
        let mut options = Options::default();
        options.create_if_missing(true);
        options.create_missing_column_families(true);
        let db = DB::open_cf_descriptors(
            &options,
            dir.path(),
            [ColumnFamilyDescriptor::new("records", Options::default())],
        )
        .unwrap();
        db.put_cf(db.cf_handle("records").unwrap(), b"key", b"value")
            .unwrap();
        drop(db);
        for _ in 0..2 {
            assert!(RocksDbStorage::open(dir.path(), &["records", "must-not-be-created"]).is_err());
        }
        assert!(
            !DB::list_cf(&options, dir.path())
                .unwrap()
                .iter()
                .any(|cf| cf == "must-not-be-created")
        );
        let db = DB::open_cf(&options, dir.path(), ["records"]).unwrap();
        assert_eq!(
            db.get_cf(db.cf_handle("records").unwrap(), b"key").unwrap(),
            Some(b"value".to_vec())
        );
        assert!(db.cf_handle(ROCKSDB_INTERNAL_CF).is_none());
    }

    #[test]
    fn unknown_raw_format_marker_is_rejected_repeatedly() {
        let dir = tempfile::tempdir().unwrap();
        let mut options = Options::default();
        options.create_if_missing(true);
        options.create_missing_column_families(true);
        let db = DB::open_cf_descriptors(
            &options,
            dir.path(),
            [
                ColumnFamilyDescriptor::new("records", Options::default()),
                ColumnFamilyDescriptor::new(ROCKSDB_INTERNAL_CF, Options::default()),
            ],
        )
        .unwrap();
        db.put_cf(
            db.cf_handle(ROCKSDB_INTERNAL_CF).unwrap(),
            ROCKSDB_VALUE_FORMAT_KEY,
            b"v2",
        )
        .unwrap();
        drop(db);
        for _ in 0..2 {
            assert!(RocksDbStorage::open(dir.path(), &["records", "must-not-be-created"]).is_err());
        }
        assert!(
            !DB::list_cf(&options, dir.path())
                .unwrap()
                .iter()
                .any(|cf| cf == "must-not-be-created")
        );
    }

    #[test]
    fn interrupted_initialization_with_only_empty_families_is_recoverable() {
        let dir = tempfile::tempdir().unwrap();
        let mut options = Options::default();
        options.create_if_missing(true);
        options.create_missing_column_families(true);
        drop(
            DB::open_cf_descriptors(
                &options,
                dir.path(),
                [
                    ColumnFamilyDescriptor::new("records", Options::default()),
                    ColumnFamilyDescriptor::new(ROCKSDB_INTERNAL_CF, Options::default()),
                ],
            )
            .unwrap(),
        );
        let storage = RocksDbStorage::open(dir.path(), &["records", "added"]).unwrap();
        assert!(
            storage
                .column_family_names()
                .unwrap()
                .iter()
                .any(|cf| cf == "added")
        );
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
    fn pure_batches_match_memory_and_survive_reopen() {
        use groove::storage::{MemoryStorage, OrderedKvStorage, OwnedWriteOperation};

        let dir = tempfile::tempdir().unwrap();
        let rocks = RocksDbStorage::open(dir.path(), &["records"]).unwrap();
        let memory = MemoryStorage::new(&["records"]).expect("valid memory storage families");
        ready(groove::storage::conformance::persistence_order_and_batch_atomicity(&rocks));
        ready(
            groove::storage::conformance::atomic_conditionals_preserve_winners_and_reject_stale_deletes(
                &rocks,
            ),
        );
        ready(groove::storage::conformance::invalid_batch_is_proven_uncommitted(&rocks));
        assert_eq!(
            ready(rocks.put_if_absent(
                "records".to_owned(),
                b"locator".to_vec(),
                b"receipt-a".to_vec(),
            ))
            .unwrap(),
            None
        );
        assert_eq!(
            ready(rocks.put_if_absent(
                "records".to_owned(),
                b"locator".to_vec(),
                b"receipt-b".to_vec(),
            ))
            .unwrap(),
            Some(b"receipt-a".to_vec())
        );
        assert!(
            !ready(rocks.compare_and_delete(
                "records".to_owned(),
                b"locator".to_vec(),
                b"receipt-b".to_vec(),
            ))
            .unwrap()
        );
        assert!(
            ready(rocks.compare_and_delete(
                "records".to_owned(),
                b"locator".to_vec(),
                b"receipt-a".to_vec(),
            ))
            .unwrap()
        );
        assert_eq!(
            ready(rocks.put_if_absent(
                "records".to_owned(),
                b"locator".to_vec(),
                b"receipt-c".to_vec(),
            ))
            .unwrap(),
            None
        );
        assert!(
            !ready(rocks.compare_and_delete(
                "records".to_owned(),
                b"locator".to_vec(),
                b"receipt-a".to_vec(),
            ))
            .unwrap()
        );
        let operations = vec![
            OwnedWriteOperation::Set {
                cf: "records".to_owned(),
                key: b"same-key".to_vec(),
                value: b"first".to_vec(),
            },
            OwnedWriteOperation::Set {
                cf: "records".to_owned(),
                key: b"same-key".to_vec(),
                value: b"second".to_vec(),
            },
            OwnedWriteOperation::Delete {
                cf: "records".to_owned(),
                key: b"same-key".to_vec(),
            },
            OwnedWriteOperation::Set {
                cf: "records".to_owned(),
                key: b"same-key".to_vec(),
                value: b"final".to_vec(),
            },
        ];
        ready(memory.write_many(operations.clone())).unwrap();
        ready(rocks.write_many(operations)).unwrap();
        let expected = ready(memory.get("records".to_owned(), b"same-key".to_vec())).unwrap();
        assert_eq!(expected, Some(b"final".to_vec()));
        assert_eq!(
            ready(rocks.get("records".to_owned(), b"same-key".to_vec())).unwrap(),
            expected
        );

        let rejected = vec![
            OwnedWriteOperation::Set {
                cf: "records".to_owned(),
                key: b"must-not-leak".to_vec(),
                value: b"value".to_vec(),
            },
            OwnedWriteOperation::Set {
                cf: "missing".to_owned(),
                key: b"invalid".to_vec(),
                value: b"value".to_vec(),
            },
        ];
        assert!(ready(rocks.write_many(rejected)).is_err());
        assert_eq!(
            ready(rocks.get("records".to_owned(), b"must-not-leak".to_vec())).unwrap(),
            None
        );
        drop(rocks);

        let reopened = RocksDbStorage::open(dir.path(), &["records"]).unwrap();
        assert_eq!(
            ready(reopened.get("records".to_owned(), b"same-key".to_vec())).unwrap(),
            Some(b"final".to_vec())
        );
        assert_eq!(
            ready(reopened.get("records".to_owned(), b"must-not-leak".to_vec())).unwrap(),
            None
        );
    }

    #[test]
    fn generic_reopen_conformance_preserves_data_and_adds_families() {
        let dir = tempfile::tempdir().unwrap();
        let storage = RocksDbStorage::open(dir.path(), &["records"]).unwrap();
        ready(groove::storage::conformance::reopen_preserves_data_and_adds_families(storage));
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
