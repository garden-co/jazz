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
    BlockBasedOptions, Cache, ColumnFamilyDescriptor, DB, DBCompactionStyle, DBCompressionType,
    Direction, IteratorMode, MergeOperands, Options, ReadOptions, UniversalCompactOptions,
    WriteBatch, WriteBufferManager, WriteOptions, properties,
};

use super::{
    ColumnFamilyName, Error, Key, OrderedKvStorage, ScanVisitor, Value, WriteOperation,
    apply_storage_delta, compact_storage_delta_operand,
};

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
const CLASS_CONTENT_CF: &str = "__groove_class_content";
const CLASS_META_CF: &str = "__groove_class_meta";

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
    /// The on-disk directory, kept so [`super::ReopenableStorage::reopen`]
    /// can reopen with more column families.
    path: PathBuf,
    /// The durability tier writes use.
    durability: Durability,
    /// The column families currently open.
    column_families: BTreeSet<String>,
    /// The open database handle.
    db: DB,
    /// Write options derived from `durability` (WAL/sync flags).
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

    /// Opens (creating if missing) the store at `path` with the given
    /// `column_families` and durability tier.
    ///
    /// Any column families already on disk are opened too, so reopening an
    /// existing store never hides data. Each family is tuned by its storage
    /// class profile (see the private `rocksdb_class_profile`).
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
                    rocksdb_options_for_cf(name, &block_cache, &write_buffer_manager),
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

    /// Looks up the open handle for a column family, failing with
    /// [`Error::ColumnFamilyNotFound`] when it is not open.
    fn cf_handle(&self, cf: &ColumnFamilyName) -> Result<&rocksdb::ColumnFamily, Error> {
        self.db
            .cf_handle(cf)
            .ok_or_else(|| Error::ColumnFamilyNotFound(cf.to_owned()))
    }
}

/// Database-wide options, using the default storage-class profile.
fn rocksdb_options(block_cache: &Cache, write_buffer_manager: &WriteBufferManager) -> Options {
    rocksdb_options_for_profile(
        RocksDbClassProfile::Default,
        block_cache,
        write_buffer_manager,
    )
}

/// Per-column-family options, using the profile inferred from the CF name.
fn rocksdb_options_for_cf(
    cf: &str,
    block_cache: &Cache,
    write_buffer_manager: &WriteBufferManager,
) -> Options {
    rocksdb_options_for_profile(rocksdb_class_profile(cf), block_cache, write_buffer_manager)
}

/// A storage-physics profile for a column family: how it is accessed decides
/// its block size, compression, compaction style, and whether bloom filters
/// are worth building.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RocksDbClassProfile {
    /// General-purpose defaults for ordinary tables.
    Default,
    /// Append-and-range-scan classes (history, register, changes): large
    /// blocks, zstd, universal compaction, no point-probe blooms.
    AppendRange,
    /// Overwrite-heavy hot classes (current/index): small blocks, lz4, blooms.
    OverwriteHot,
    /// Large-value content classes: large blocks, zstd, blooms.
    Content,
    /// Metadata class: default physics with blooms.
    Meta,
}

/// Maps a class column-family name to its access profile (ordinary tables get
/// [`RocksDbClassProfile::Default`]).
fn rocksdb_class_profile(cf: &str) -> RocksDbClassProfile {
    match cf {
        CLASS_HISTORY_CF | CLASS_REGISTER_CF | CLASS_CHANGES_CF => RocksDbClassProfile::AppendRange,
        CLASS_GLOBAL_CURRENT_CF | CLASS_AHEAD_CURRENT_CF | CLASS_INDICES_CF => {
            RocksDbClassProfile::OverwriteHot
        }
        CLASS_CONTENT_CF => RocksDbClassProfile::Content,
        CLASS_META_CF => RocksDbClassProfile::Meta,
        _ => RocksDbClassProfile::Default,
    }
}

/// Builds the RocksDB `Options` for a profile: table factory, shared cache
/// and write-buffer budget, file sizing, compression, the `groove_delta`
/// merge operator, and universal compaction for append-range classes.
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
    /// Whether to build point-lookup bloom filters — worthwhile only for
    /// classes with real point probes, not pure scan classes.
    fn uses_blooms(self) -> bool {
        match self {
            // History/register/changes are consumed as prefix/range/latest scans.
            // Current/index/content-meta classes still have real point probes.
            Self::AppendRange => false,
            Self::Default | Self::OverwriteHot | Self::Content | Self::Meta => true,
        }
    }

    /// Table block size: larger for scan/large-value classes, smaller for
    /// point-lookup classes.
    fn block_size(self) -> usize {
        match self {
            Self::AppendRange | Self::Content => ROCKSDB_LARGE_BLOCK_BYTES,
            Self::Default | Self::OverwriteHot | Self::Meta => ROCKSDB_DEFAULT_BLOCK_BYTES,
        }
    }

    /// Target SST file size: larger for append/content classes.
    fn target_file_size(self) -> u64 {
        match self {
            Self::AppendRange | Self::Content => ROCKSDB_APPEND_TARGET_FILE_BYTES,
            Self::Default | Self::OverwriteHot | Self::Meta => ROCKSDB_OVERWRITE_TARGET_FILE_BYTES,
        }
    }

    /// Live-tier compression: zstd for bulk classes, lz4 for hot ones.
    fn compression(self) -> DBCompressionType {
        match self {
            Self::AppendRange | Self::Content => DBCompressionType::Zstd,
            Self::Default | Self::OverwriteHot | Self::Meta => DBCompressionType::Lz4,
        }
    }

    /// Bottommost-tier compression: always zstd, since cold data compresses
    /// best and is read rarely.
    fn bottommost_compression(self) -> DBCompressionType {
        DBCompressionType::Zstd
    }
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

    fn approximate_class_bytes(&self, cf: &ColumnFamilyName) -> Result<Option<u64>, Error> {
        let handle = self.cf_handle(cf)?;
        let sst = self
            .db
            .property_int_value_cf(handle, properties::TOTAL_SST_FILES_SIZE)?
            .unwrap_or(0);
        let mem = self
            .db
            .property_int_value_cf(handle, properties::SIZE_ALL_MEM_TABLES)?
            .unwrap_or(0);
        Ok(Some(sst.saturating_add(mem)))
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
                WriteOperation::Delta { cf, key, delta } => {
                    batch.merge_cf(self.cf_handle(cf)?, key, delta.encode()?);
                }
            }
        }

        Ok(self.db.write_opt(&batch, &self.write_options)?)
    }

    fn column_family_names(&self) -> Option<Vec<String>> {
        Some(self.column_families.iter().cloned().collect())
    }
}

/// RocksDB full-merge callback: folds every buffered delta operand into the
/// existing value in order (see [`super::apply_storage_delta`]). This is how
/// `WriteOperation::Delta` writes are resolved without a read-modify-write.
fn rocksdb_full_merge_delta(
    _key: &[u8],
    old_value: Option<&[u8]>,
    operands: &MergeOperands,
) -> Option<Vec<u8>> {
    apply_merge_operands(old_value, operands).ok()
}

/// RocksDB partial-merge callback: collapses several delta operands (with no
/// base value yet) into one equivalent operand, re-tagged for the merged
/// record so a later full merge stays correct.
fn rocksdb_partial_merge_delta(
    _key: &[u8],
    left_operand: Option<&[u8]>,
    operands: &MergeOperands,
) -> Option<Vec<u8>> {
    let mut value = match left_operand {
        Some(operand) => Some(apply_storage_delta(None, operand).ok()?),
        None => None,
    };
    let template = left_operand.or_else(|| operands.iter().next())?;
    for operand in operands {
        value = Some(apply_storage_delta(value.as_deref(), operand).ok()?);
    }
    compact_storage_delta_operand(template, value?).ok()
}

/// Folds a sequence of delta operands onto an optional starting value,
/// returning the final merged bytes.
fn apply_merge_operands(
    initial: Option<&[u8]>,
    operands: &MergeOperands,
) -> Result<Vec<u8>, Error> {
    let mut value = initial.map(<[u8]>::to_vec);
    for operand in operands {
        value = Some(apply_storage_delta(value.as_deref(), operand)?);
    }
    value.ok_or_else(|| Error::InvalidStorageDelta("merge operator received no value".to_owned()))
}

/// Turns `prefix` into an exclusive upper bound for a prefix scan by
/// incrementing its last non-`0xff` byte in place. Returns `false` when the
/// prefix is all `0xff` and has no finite upper bound (the caller then scans
/// to the end and stops on the first non-matching key).
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
    use super::{
        CLASS_AHEAD_CURRENT_CF, CLASS_CHANGES_CF, CLASS_CONTENT_CF, CLASS_GLOBAL_CURRENT_CF,
        CLASS_HISTORY_CF, CLASS_INDICES_CF, CLASS_META_CF, CLASS_REGISTER_CF, RocksDbClassProfile,
        rocksdb_class_profile,
    };

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

        let content = rocksdb_class_profile(CLASS_CONTENT_CF);
        assert_eq!(content, RocksDbClassProfile::Content);
        assert!(
            content.uses_blooms(),
            "content class includes content_meta/checkpoint point probes today"
        );

        assert_eq!(
            rocksdb_class_profile(CLASS_META_CF),
            RocksDbClassProfile::Meta
        );
        assert_eq!(
            rocksdb_class_profile("ordinary"),
            RocksDbClassProfile::Default
        );
    }
}
