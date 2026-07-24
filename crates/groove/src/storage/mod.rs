//! Ordered key/value storage seam used behind record stores.
//!
//! This module owns the backing-implementation contract: column families, point
//! reads, ordered range/prefix scans, reverse/prefix helpers, and atomic write
//! batches. Storage backends only need to provide [`OrderedKvStorage`]. Higher
//! layers should work through record-store handles such as [`RecordStore`] and
//! directly exposed direct stores rather than reaching through to column
//! families or raw ordered-KV operations.
//!
//! The storage layer deliberately does not know about schemas, query graphs,
//! records beyond typed convenience wrappers, or Jazz semantics. The RocksDB
//! implementation lives in [`rocksdb_storage`]; higher layers decide when a
//! batch is durable and how storage writes relate to an IVM tick.

#[cfg_attr(not(target_arch = "wasm32"), allow(dead_code))]
mod key_codec;
mod memory;
mod opfs;
#[cfg(feature = "rocksdb")]
#[path = "rocksdb.rs"]
pub mod rocksdb_storage;

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::sync::{Mutex, OnceLock};

use crate::records::{OwnedRecord, Record, RecordDescriptor, Value as RecordValue, ValueType};
use crate::window_codec::{
    WindowRecord, WindowSchema, decode_window, encode_window, lookup_window,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub use memory::MemoryStorage;
#[cfg(target_arch = "wasm32")]
pub use opfs::OpfsStorage;
#[cfg(not(target_arch = "wasm32"))]
pub use opfs::{BtreeSyncPolicy, NativeBtreeStorage};
#[cfg(feature = "rocksdb")]
pub use rocksdb_storage::{Durability, RocksDbStorage};

pub type ColumnFamilyName = str;
pub type Key = [u8];
pub type Value = Vec<u8>;
pub type KeyValue = (Vec<u8>, Vec<u8>);
const DECODED_WINDOW_CACHE_CAPACITY: usize = 32;
const STAGED_POINT_READS_BEFORE_INDEX: usize = 16;
const STAGED_OPS_BEFORE_POINT_INDEX: usize = 64;
/// Callback form used by scans so storage implementations do not have to
/// materialize large ranges before the caller can process them.
pub type ScanVisitor<'visitor> =
    dyn for<'a, 'b> FnMut(&'a [u8], &'b [u8]) -> Result<(), Error> + 'visitor;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct DecodedWindowCacheKey {
    storage_token: usize,
    column_family: String,
    window_key: Vec<u8>,
}

#[derive(Default)]
struct DecodedWindowCache {
    values: HashMap<DecodedWindowCacheKey, Vec<KeyValue>>,
    lru: VecDeque<DecodedWindowCacheKey>,
}

static DECODED_WINDOW_CACHE: OnceLock<Mutex<DecodedWindowCache>> = OnceLock::new();
static WINDOW_PROBE_TRACKER: OnceLock<Mutex<Option<(DecodedWindowCacheKey, usize)>>> =
    OnceLock::new();

impl DecodedWindowCache {
    // Window records are immutable in v1: consolidation writes each window once,
    // tail records stay plain, and no later maintenance rewrites window values.
    // Therefore decoded entries need no invalidation beyond LRU eviction. The
    // storage token keeps separate test/store instances with the same logical CF
    // and window key from colliding.
    fn get(&mut self, key: &DecodedWindowCacheKey) -> Option<Vec<KeyValue>> {
        let value = self.values.get(key)?.clone();
        self.touch(key);
        Some(value)
    }

    fn get_record(
        &mut self,
        key: &DecodedWindowCacheKey,
        record_key: &[u8],
    ) -> Option<Option<Vec<u8>>> {
        let records = self.values.get(key)?;
        let value = records
            .binary_search_by(|(key, _)| key.as_slice().cmp(record_key))
            .ok()
            .map(|idx| records[idx].1.clone());
        self.touch(key);
        Some(value)
    }

    fn insert(&mut self, key: DecodedWindowCacheKey, value: Vec<KeyValue>) {
        if self.values.contains_key(&key) {
            self.values.insert(key.clone(), value);
            self.touch(&key);
            return;
        }
        self.values.insert(key.clone(), value);
        self.lru.push_back(key);
        while self.values.len() > DECODED_WINDOW_CACHE_CAPACITY {
            if let Some(oldest) = self.lru.pop_front() {
                self.values.remove(&oldest);
            }
        }
    }

    fn touch(&mut self, key: &DecodedWindowCacheKey) {
        if let Some(index) = self.lru.iter().position(|candidate| candidate == key)
            && let Some(existing) = self.lru.remove(index)
        {
            self.lru.push_back(existing);
        }
    }
}

/// Typed storage delta appended through backends that can durably merge without
/// first reading the existing value.
///
/// Instead of read-modify-write, the caller writes a delta and the backend
/// merges it against whatever is stored (see [`apply_storage_delta`]). The
/// `kind` selects the merge rule; `payload` is its serialized operand.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StorageDelta {
    /// Which merge rule to apply.
    pub kind: StorageDeltaKind,
    /// The rule's serialized operand.
    pub payload: Vec<u8>,
}

/// The available storage-delta merge rules.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum StorageDeltaKind {
    /// "Keep the causally-latest writer" merge; see [`CurrentWinnerDelta`].
    CurrentWinnerV1,
}

/// Operand of a `CurrentWinnerV1` merge: a candidate record tagged with its
/// writer's `(tx_time, tx_node_uuid)`.
///
/// On merge the candidate wins if it is a causal descendant of the stored
/// winner (`parents` lists known predecessors) or has a strictly greater
/// `(tx_time, tx_node_uuid)` key. The `*_offset` fields say where those key
/// bytes live inside `record`, so the winner can be recomputed after
/// compaction.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CurrentWinnerDelta {
    /// The writer's transaction time.
    pub tx_time: u64,
    /// The writer's node UUID (ties tx_time).
    pub tx_node_uuid: [u8; 16],
    /// Known predecessor keys; the candidate wins over any of them.
    pub parents: Vec<(u64, [u8; 16])>,
    /// Byte offset of `tx_time` inside `record`.
    pub tx_time_offset: u32,
    /// Byte offset of `tx_node_uuid` inside `record`.
    pub tx_node_uuid_offset: u32,
    /// The candidate record itself.
    pub record: Vec<u8>,
}

impl StorageDelta {
    /// Builds a `CurrentWinnerV1` delta from its operand.
    pub fn current_winner(delta: CurrentWinnerDelta) -> Result<Self, Error> {
        Ok(Self {
            kind: StorageDeltaKind::CurrentWinnerV1,
            payload: postcard::to_allocvec(&delta)
                .map_err(|error| Error::InvalidStorageDelta(error.to_string()))?,
        })
    }

    /// Serializes the delta (kind + payload) for storage as an operand.
    pub(crate) fn encode(&self) -> Result<Vec<u8>, Error> {
        postcard::to_allocvec(&(self.kind, &self.payload))
            .map_err(|error| Error::InvalidStorageDelta(error.to_string()))
    }

    /// Reverses [`Self::encode`].
    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, Error> {
        let (kind, payload): (StorageDeltaKind, Vec<u8>) = postcard::from_bytes(bytes)
            .map_err(|error| Error::InvalidStorageDelta(error.to_string()))?;
        Ok(Self { kind, payload })
    }
}

/// Rebuilds a delta operand after compaction: takes the already-merged
/// record and re-derives its winner key from the template's offsets, so the
/// stored operand stays self-describing without carrying stale parent lists.
pub(crate) fn compact_storage_delta_operand(
    template_operand: &[u8],
    merged_record: Vec<u8>,
) -> Result<Vec<u8>, Error> {
    let template = StorageDelta::decode(template_operand)?;
    match template.kind {
        StorageDeltaKind::CurrentWinnerV1 => {
            let template: CurrentWinnerDelta = postcard::from_bytes(&template.payload)
                .map_err(|error| Error::InvalidStorageDelta(error.to_string()))?;
            let (tx_time, tx_node_uuid) = current_winner_key(
                &merged_record,
                template.tx_time_offset as usize,
                template.tx_node_uuid_offset as usize,
            )?;
            StorageDelta::current_winner(CurrentWinnerDelta {
                tx_time,
                tx_node_uuid,
                parents: Vec::new(),
                tx_time_offset: template.tx_time_offset,
                tx_node_uuid_offset: template.tx_node_uuid_offset,
                record: merged_record,
            })?
            .encode()
        }
    }
}

/// The merge function backends call to fold one delta into a stored value:
/// decodes the delta and applies its rule to `existing` (`None` when the key
/// is new), returning the new stored bytes.
pub fn apply_storage_delta(
    existing: Option<&[u8]>,
    encoded_delta: &[u8],
) -> Result<Vec<u8>, Error> {
    let delta = StorageDelta::decode(encoded_delta)?;
    match delta.kind {
        StorageDeltaKind::CurrentWinnerV1 => {
            let candidate: CurrentWinnerDelta = postcard::from_bytes(&delta.payload)
                .map_err(|error| Error::InvalidStorageDelta(error.to_string()))?;
            apply_current_winner_delta(existing, &candidate)
        }
    }
}

/// The `CurrentWinnerV1` merge rule: the candidate replaces the stored record
/// when the store is empty, when the candidate lists the stored winner as a
/// parent, or when its `(tx_time, tx_node_uuid)` key is greater; otherwise the
/// stored record stays.
fn apply_current_winner_delta(
    existing: Option<&[u8]>,
    candidate: &CurrentWinnerDelta,
) -> Result<Vec<u8>, Error> {
    let Some(existing) = existing else {
        return Ok(candidate.record.clone());
    };
    let existing_key = current_winner_key(
        existing,
        candidate.tx_time_offset as usize,
        candidate.tx_node_uuid_offset as usize,
    )?;
    let candidate_key = (candidate.tx_time, candidate.tx_node_uuid);
    if candidate.parents.contains(&existing_key) || candidate_key > existing_key {
        Ok(candidate.record.clone())
    } else {
        Ok(existing.to_vec())
    }
}

/// Reads the `(tx_time, tx_node_uuid)` winner key out of a record at the
/// given byte offsets.
fn current_winner_key(
    record: &[u8],
    tx_time_offset: usize,
    tx_node_uuid_offset: usize,
) -> Result<(u64, [u8; 16]), Error> {
    let time_bytes = record
        .get(tx_time_offset..tx_time_offset + 8)
        .ok_or_else(|| {
            Error::InvalidStorageDelta("current-winner tx_time offset out of bounds".to_owned())
        })?;
    let uuid_bytes = record
        .get(tx_node_uuid_offset..tx_node_uuid_offset + 16)
        .ok_or_else(|| {
            Error::InvalidStorageDelta(
                "current-winner tx_node_uuid offset out of bounds".to_owned(),
            )
        })?;
    let mut uuid = [0; 16];
    uuid.copy_from_slice(uuid_bytes);
    Ok((
        u64::from_le_bytes(time_bytes.try_into().expect("slice length checked")),
        uuid,
    ))
}

/// Backing-implementation interface for ordered key/value storage.
///
/// This is the only trait a storage backend must implement. Its column-family
/// names are backing details consumed by record-store plumbing; higher layers
/// should use typed record-store handles instead of calling these methods
/// directly. The trait intentionally exposes batch atomicity but no higher
/// transaction semantics; `commit_batch` owns the tick ordering above this
/// layer.
pub trait OrderedKvStorage {
    /// Begin an encoded storage transaction over this backend.
    ///
    /// The transaction buffers already-encoded key/value writes and presents
    /// read-your-own-writes semantics for point reads and ordered scans. Commit
    /// applies the buffered operations through one backend `write_many` call,
    /// preserving the caller's higher-level tick/commit boundary.
    fn begin_txn(&self) -> StorageTransaction<'_, Self>
    where
        Self: Sized,
    {
        StorageTransaction::new(self)
    }

    /// Point read of one key in a column family; `None` when absent.
    fn get(&self, cf: &ColumnFamilyName, key: &Key) -> Result<Option<Value>, Error>;
    /// Writes (inserts or overwrites) one key/value.
    fn set(&self, cf: &ColumnFamilyName, key: &Key, value: &[u8]) -> Result<(), Error>;
    /// Removes one key; a no-op when it is absent.
    fn delete(&self, cf: &ColumnFamilyName, key: &Key) -> Result<(), Error>;
    /// Flush and close any backend resources that require an explicit clean
    /// shutdown boundary. Backends without close-time work may keep the default.
    fn close(&self) -> Result<(), Error> {
        Ok(())
    }
    /// Process-local identity for cache partitioning. Backends may override
    /// this when cheap clones should share cache entries.
    fn cache_token(&self) -> usize
    where
        Self: Sized,
    {
        self as *const Self as usize
    }
    /// Return approximate live bytes for one storage class/column family when
    /// the backend can expose them cheaply.
    ///
    /// Backends that cannot meter a family return `Ok(None)`, allowing higher
    /// layers to leave byte-budget features disabled rather than relying on
    /// invented accounting.
    fn approximate_class_bytes(&self, _cf: &ColumnFamilyName) -> Result<Option<u64>, Error> {
        Ok(None)
    }
    /// Visits every key/value with `start <= key < end`, in ascending key
    /// order, calling `visit` per entry (which may abort with an error).
    fn scan_range(
        &self,
        cf: &ColumnFamilyName,
        start: &Key,
        end: &Key,
        visit: &mut ScanVisitor<'_>,
    ) -> Result<(), Error>;
    /// Visits every key/value whose key starts with `prefix`, ascending.
    fn scan_prefix(
        &self,
        cf: &ColumnFamilyName,
        prefix: &Key,
        visit: &mut ScanVisitor<'_>,
    ) -> Result<(), Error>;
    /// Like [`Self::scan_prefix`] but descending. The default buffers the
    /// prefix and reverses; backends with native reverse iteration should
    /// override it.
    fn scan_prefix_reverse(
        &self,
        cf: &ColumnFamilyName,
        prefix: &Key,
        visit: &mut ScanVisitor<'_>,
    ) -> Result<(), Error> {
        let mut values = Vec::new();
        self.scan_prefix(cf, prefix, &mut |key, value| {
            values.push((key.to_vec(), value.to_vec()));
            Ok(())
        })?;
        for (key, value) in values.into_iter().rev() {
            visit(&key, &value)?;
        }
        Ok(())
    }
    /// The greatest key/value under `prefix`, or `None` when none exist.
    fn last_with_prefix(
        &self,
        cf: &ColumnFamilyName,
        prefix: &Key,
    ) -> Result<Option<KeyValue>, Error> {
        let mut last = None;
        self.scan_prefix(cf, prefix, &mut |key, value| {
            last = Some((key.to_vec(), value.to_vec()));
            Ok(())
        })?;
        Ok(last)
    }
    /// The greatest key/value under `prefix` that is `<= upper` — a
    /// point-in-time "current value at or before" lookup over versioned keys.
    fn last_with_prefix_before_or_at(
        &self,
        cf: &ColumnFamilyName,
        prefix: &Key,
        upper: &Key,
    ) -> Result<Option<KeyValue>, Error> {
        let mut last = None;
        self.scan_prefix(cf, prefix, &mut |key, value| {
            if key <= upper {
                last = Some((key.to_vec(), value.to_vec()));
            }
            Ok(())
        })?;
        Ok(last)
    }
    /// Applies a batch of writes atomically: either all take effect or none
    /// do. This is the batch-atomicity guarantee the tick/commit boundary
    /// above relies on.
    fn write_many(&self, operations: &[WriteOperation<'_>]) -> Result<(), Error>;

    /// Return known column-family names when the backend can enumerate them.
    ///
    /// This is intentionally optional so the ordered-KV contract stays small.
    /// Layout validation uses it to reject pre-release physical-layout changes
    /// loudly instead of opening an old store as if it were empty.
    fn column_family_names(&self) -> Option<Vec<String>> {
        None
    }

    /// Collects [`Self::scan_range`] into an owned vector — convenient when
    /// the range is small and callback streaming is not worth it.
    fn range(&self, cf: &ColumnFamilyName, start: &Key, end: &Key) -> Result<Vec<KeyValue>, Error> {
        let mut values = Vec::new();
        self.scan_range(cf, start, end, &mut |key, value| {
            values.push((key.to_vec(), value.to_vec()));
            Ok(())
        })?;
        Ok(values)
    }

    /// Collects [`Self::scan_prefix`] into an owned vector.
    fn prefix(&self, cf: &ColumnFamilyName, prefix: &Key) -> Result<Vec<KeyValue>, Error> {
        let mut values = Vec::new();
        self.scan_prefix(cf, prefix, &mut |key, value| {
            values.push((key.to_vec(), value.to_vec()));
            Ok(())
        })?;
        Ok(values)
    }
}

const CLASS_HISTORY_CF: &str = "__groove_class_history";
const CLASS_REGISTER_CF: &str = "__groove_class_register";
const CLASS_GLOBAL_CURRENT_CF: &str = "__groove_class_global_current";
const CLASS_AHEAD_CURRENT_CF: &str = "__groove_class_ahead_current";
const CLASS_CHANGES_CF: &str = "__groove_class_changes";
const CLASS_INDICES_CF: &str = "__groove_class_indices";
const CLASS_CONTENT_CF: &str = "__groove_class_content";
const CLASS_META_CF: &str = "__groove_class_meta";
const CLASS_LAYOUT_MARKER_KEY: &[u8] = b"groove-storage-layout";
const CLASS_LAYOUT_MARKER_VALUE: &[u8] = b"class-cf-v1";

/// Logical-to-physical storage layout used by [`LayoutStorage`].
///
/// The identity layout preserves the historical one-logical-table-per-CF
/// mapping. The class layout maps selected logical tables into shared physical
/// class CFs while prefixing keys with a length-framed logical table name.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum StorageLayout {
    /// One logical table per physical column family (the historical
    /// mapping).
    #[default]
    Identity,
    /// Jazz "class" layout: several logical tables share one physical class
    /// CF, with each key prefixed by its length-framed logical table name.
    JazzClassV1 {
        /// The logical CFs actually mapped into shared class CFs; empty means
        /// "map every eligible table".
        mapped_logical_cfs: BTreeSet<String>,
    },
}

impl StorageLayout {
    /// The class layout mapping every eligible logical table.
    pub fn jazz_class_v1() -> Self {
        Self::JazzClassV1 {
            mapped_logical_cfs: BTreeSet::new(),
        }
    }

    /// The class layout restricted to the eligible tables among
    /// `logical_column_families` (others stay one-CF-each).
    pub fn jazz_class_v1_for<'a>(
        logical_column_families: impl IntoIterator<Item = &'a str>,
    ) -> Self {
        let mapped_logical_cfs = logical_column_families
            .into_iter()
            .filter(|name| jazz_physical_class(name).is_some())
            .map(str::to_owned)
            .collect();
        Self::JazzClassV1 { mapped_logical_cfs }
    }

    /// The distinct physical CFs a set of logical tables maps to under this
    /// layout — what the backend must actually open (plus the class meta CF
    /// for class layouts).
    pub fn physical_column_families<'a>(
        &self,
        logical_column_families: impl IntoIterator<Item = &'a str>,
    ) -> Vec<String> {
        let mut names = BTreeSet::new();
        if matches!(self, Self::JazzClassV1 { .. }) {
            names.insert(CLASS_META_CF.to_owned());
        }
        for logical in logical_column_families {
            names.insert(self.map_cf(logical).physical_cf.to_owned());
        }
        names.into_iter().collect()
    }

    /// Resolves a logical CF name to its physical CF plus the optional
    /// logical-table key prefix used inside a shared class CF.
    fn map_cf<'a>(&'a self, logical_cf: &'a str) -> PhysicalCf<'a> {
        match self {
            Self::Identity => PhysicalCf {
                physical_cf: logical_cf,
                logical_prefix: None,
            },
            Self::JazzClassV1 { mapped_logical_cfs } => {
                let should_map =
                    mapped_logical_cfs.is_empty() || mapped_logical_cfs.contains(logical_cf);
                if should_map && let Some(physical_cf) = jazz_physical_class(logical_cf) {
                    PhysicalCf {
                        physical_cf,
                        logical_prefix: Some(logical_cf),
                    }
                } else {
                    PhysicalCf {
                        physical_cf: logical_cf,
                        logical_prefix: None,
                    }
                }
            }
        }
    }

    fn validates_marker(&self) -> bool {
        matches!(self, Self::JazzClassV1 { .. })
    }

    fn mapped_legacy_cf(&self, cf: &str) -> bool {
        match self {
            Self::Identity => false,
            Self::JazzClassV1 { mapped_logical_cfs } => {
                (mapped_logical_cfs.is_empty() || mapped_logical_cfs.contains(cf))
                    && jazz_physical_class(cf).is_some()
            }
        }
    }
}

/// Where one logical CF physically lives: the physical CF name, and the
/// logical-table prefix to stamp on keys when several tables share it
/// (`None` for one-CF-each).
struct PhysicalCf<'a> {
    physical_cf: &'a str,
    logical_prefix: Option<&'a str>,
}

/// `true` for Jazz windowed-history tables (`jazz_*_history`), whose values
/// are consolidated windows read through [`WindowConsolidation`].
pub(crate) fn is_windowed_history_table(name: &str) -> bool {
    name.starts_with("jazz_") && name.ends_with("_history")
}

fn is_jazz_register_table(name: &str) -> bool {
    name.starts_with("jazz_")
        && name.ends_with("_register")
        && !name.ends_with("_register_global_current")
        && !name.ends_with("_register_ahead_current")
}

fn is_jazz_global_current_table(name: &str) -> bool {
    name.starts_with("jazz_")
        && (name.ends_with("_global_current") || name.ends_with("_register_global_current"))
        && !name.contains("_ahead_current")
}

fn is_jazz_ahead_current_table(name: &str) -> bool {
    name.starts_with("jazz_")
        && (name.ends_with("_ahead_current") || name.ends_with("_register_ahead_current"))
}

fn is_jazz_content_store(name: &str) -> bool {
    matches!(
        name,
        "jazz_content_extents" | "jazz_content_meta" | "jazz_content_checkpoints"
    )
}

/// The shared class CF a Jazz logical table maps to under `JazzClassV1`, or
/// `None` for tables that keep their own CF.
fn jazz_physical_class(logical_cf: &str) -> Option<&'static str> {
    if is_windowed_history_table(logical_cf) {
        Some(CLASS_HISTORY_CF)
    } else if is_jazz_register_table(logical_cf) {
        Some(CLASS_REGISTER_CF)
    } else if is_jazz_global_current_table(logical_cf) {
        Some(CLASS_GLOBAL_CURRENT_CF)
    } else if is_jazz_ahead_current_table(logical_cf) {
        Some(CLASS_AHEAD_CURRENT_CF)
    } else if logical_cf == "jazz_global_changes" {
        Some(CLASS_CHANGES_CF)
    } else if logical_cf == "indices" {
        // The class prefix wraps the existing durable-index key. That key
        // already starts with table/index identity, so this avoids introducing
        // a second table prefix while keeping one physical index CF.
        Some(CLASS_INDICES_CF)
    } else if is_jazz_content_store(logical_cf) {
        Some(CLASS_CONTENT_CF)
    } else if logical_cf.starts_with("jazz_") {
        Some(CLASS_META_CF)
    } else {
        None
    }
}

/// Storage view that keeps logical CF names at the database boundary while
/// reading and writing a physical class-CF layout below it.
///
/// It wraps any [`OrderedKvStorage`] and rewrites `(cf, key)` pairs through a
/// [`StorageLayout`] on the way down (prefixing keys inside shared class CFs)
/// and strips the prefix back off on the way up, so callers above never see
/// the physical layout.
pub struct LayoutStorage<S> {
    inner: S,
    layout: StorageLayout,
}

impl<S> LayoutStorage<S>
where
    S: OrderedKvStorage,
{
    /// Wraps `inner` with `layout`, validating (or writing) the layout marker
    /// so an existing store is never silently reinterpreted under a different
    /// physical layout.
    pub fn new(inner: S, layout: StorageLayout) -> Result<Self, Error> {
        let storage = Self { inner, layout };
        storage.ensure_layout_marker()?;
        Ok(storage)
    }

    /// Unwraps back to the underlying storage.
    pub fn into_inner(self) -> S {
        self.inner
    }

    /// Checks the on-disk layout marker for class layouts: it must be present
    /// and correct on a non-empty store, and is written on a fresh one — this
    /// is the guard against opening an old-layout store as if it were empty.
    fn ensure_layout_marker(&self) -> Result<(), Error> {
        if !self.layout.validates_marker() {
            return Ok(());
        }

        match self.inner.get(CLASS_META_CF, CLASS_LAYOUT_MARKER_KEY)? {
            Some(value) if value == CLASS_LAYOUT_MARKER_VALUE => Ok(()),
            Some(_) => Err(Error::InvalidStorageLayout(
                "unsupported class-CF storage layout marker".to_owned(),
            )),
            None => {
                if self.has_class_data_or_legacy_layout()? {
                    return Err(Error::InvalidStorageLayout(
                        "missing class-CF storage layout marker in non-empty store".to_owned(),
                    ));
                }
                self.inner.set(
                    CLASS_META_CF,
                    CLASS_LAYOUT_MARKER_KEY,
                    CLASS_LAYOUT_MARKER_VALUE,
                )
            }
        }
    }

    fn has_class_data_or_legacy_layout(&self) -> Result<bool, Error> {
        if let Some(names) = self.inner.column_family_names()
            && names.iter().any(|name| self.layout.mapped_legacy_cf(name))
        {
            return Ok(true);
        }
        for cf in [
            CLASS_HISTORY_CF,
            CLASS_REGISTER_CF,
            CLASS_GLOBAL_CURRENT_CF,
            CLASS_AHEAD_CURRENT_CF,
            CLASS_CHANGES_CF,
            CLASS_INDICES_CF,
            CLASS_CONTENT_CF,
            CLASS_META_CF,
        ] {
            match self.inner.last_with_prefix(cf, b"") {
                Ok(Some(_)) => return Ok(true),
                Ok(None) | Err(Error::ColumnFamilyNotFound(_)) => {}
                Err(error) => return Err(error),
            }
        }
        Ok(false)
    }

    /// Maps a logical `(cf, key)` to its physical `(cf, key)`: inside a shared
    /// class CF the key gets a `u32` length-framed logical-table prefix so
    /// different tables never collide.
    fn physical_key(&self, cf: &ColumnFamilyName, key: &Key) -> (String, Vec<u8>) {
        let mapping = self.layout.map_cf(cf);
        let Some(logical_prefix) = mapping.logical_prefix else {
            return (mapping.physical_cf.to_owned(), key.to_vec());
        };
        let mut physical_key = Vec::with_capacity(4 + logical_prefix.len() + key.len());
        physical_key.extend_from_slice(&(logical_prefix.len() as u32).to_be_bytes());
        physical_key.extend_from_slice(logical_prefix.as_bytes());
        physical_key.extend_from_slice(key);
        (mapping.physical_cf.to_owned(), physical_key)
    }

    /// Like [`Self::physical_key`], for scan prefixes; also returns how many
    /// leading bytes to strip from physical keys to recover logical ones.
    fn physical_prefix(&self, cf: &ColumnFamilyName, prefix: &Key) -> (String, Vec<u8>, usize) {
        let mapping = self.layout.map_cf(cf);
        let Some(logical_prefix) = mapping.logical_prefix else {
            return (mapping.physical_cf.to_owned(), prefix.to_vec(), 0);
        };
        let mut physical_prefix = Vec::with_capacity(4 + logical_prefix.len() + prefix.len());
        physical_prefix.extend_from_slice(&(logical_prefix.len() as u32).to_be_bytes());
        physical_prefix.extend_from_slice(logical_prefix.as_bytes());
        physical_prefix.extend_from_slice(prefix);
        let strip_len = 4 + logical_prefix.len();
        (mapping.physical_cf.to_owned(), physical_prefix, strip_len)
    }

    /// Removes the `strip_len`-byte logical prefix from a physical key,
    /// recovering the logical key returned to callers.
    fn strip_key<'a>(&self, key: &'a [u8], strip_len: usize) -> Result<&'a [u8], Error> {
        key.get(strip_len..).ok_or_else(|| {
            Error::InvalidStorageKey("physical layout key shorter than logical prefix".to_owned())
        })
    }
}

impl<S> OrderedKvStorage for LayoutStorage<S>
where
    S: OrderedKvStorage,
{
    fn get(&self, cf: &ColumnFamilyName, key: &Key) -> Result<Option<Value>, Error> {
        let (physical_cf, physical_key) = self.physical_key(cf, key);
        self.inner.get(&physical_cf, &physical_key)
    }

    fn set(&self, cf: &ColumnFamilyName, key: &Key, value: &[u8]) -> Result<(), Error> {
        let (physical_cf, physical_key) = self.physical_key(cf, key);
        self.inner.set(&physical_cf, &physical_key, value)
    }

    fn delete(&self, cf: &ColumnFamilyName, key: &Key) -> Result<(), Error> {
        let (physical_cf, physical_key) = self.physical_key(cf, key);
        self.inner.delete(&physical_cf, &physical_key)
    }

    fn close(&self) -> Result<(), Error> {
        self.inner.close()
    }

    fn scan_range(
        &self,
        cf: &ColumnFamilyName,
        start: &Key,
        end: &Key,
        visit: &mut ScanVisitor<'_>,
    ) -> Result<(), Error> {
        let (physical_cf, physical_start, strip_len) = self.physical_prefix(cf, start);
        let (_, physical_end, _) = self.physical_prefix(cf, end);
        self.inner.scan_range(
            &physical_cf,
            &physical_start,
            &physical_end,
            &mut |key, value| visit(self.strip_key(key, strip_len)?, value),
        )
    }

    fn scan_prefix(
        &self,
        cf: &ColumnFamilyName,
        prefix: &Key,
        visit: &mut ScanVisitor<'_>,
    ) -> Result<(), Error> {
        let (physical_cf, physical_prefix, strip_len) = self.physical_prefix(cf, prefix);
        self.inner
            .scan_prefix(&physical_cf, &physical_prefix, &mut |key, value| {
                visit(self.strip_key(key, strip_len)?, value)
            })
    }

    fn scan_prefix_reverse(
        &self,
        cf: &ColumnFamilyName,
        prefix: &Key,
        visit: &mut ScanVisitor<'_>,
    ) -> Result<(), Error> {
        let (physical_cf, physical_prefix, strip_len) = self.physical_prefix(cf, prefix);
        self.inner
            .scan_prefix_reverse(&physical_cf, &physical_prefix, &mut |key, value| {
                visit(self.strip_key(key, strip_len)?, value)
            })
    }

    fn last_with_prefix(
        &self,
        cf: &ColumnFamilyName,
        prefix: &Key,
    ) -> Result<Option<KeyValue>, Error> {
        let (physical_cf, physical_prefix, strip_len) = self.physical_prefix(cf, prefix);
        Ok(self
            .inner
            .last_with_prefix(&physical_cf, &physical_prefix)?
            .map(|(key, value)| (key[strip_len..].to_vec(), value)))
    }

    fn last_with_prefix_before_or_at(
        &self,
        cf: &ColumnFamilyName,
        prefix: &Key,
        upper: &Key,
    ) -> Result<Option<KeyValue>, Error> {
        let (physical_cf, physical_prefix, strip_len) = self.physical_prefix(cf, prefix);
        let (_, physical_upper, _) = self.physical_prefix(cf, upper);
        Ok(self
            .inner
            .last_with_prefix_before_or_at(&physical_cf, &physical_prefix, &physical_upper)?
            .map(|(key, value)| (key[strip_len..].to_vec(), value)))
    }

    fn write_many(&self, operations: &[WriteOperation<'_>]) -> Result<(), Error> {
        let translated = operations
            .iter()
            .map(|operation| match operation {
                WriteOperation::Set { cf, key, value } => {
                    let (physical_cf, physical_key) = self.physical_key(cf, key);
                    OwnedWriteOperation::Set {
                        cf: physical_cf,
                        key: physical_key,
                        value: (*value).to_vec(),
                    }
                }
                WriteOperation::Delete { cf, key } => {
                    let (physical_cf, physical_key) = self.physical_key(cf, key);
                    OwnedWriteOperation::Delete {
                        cf: physical_cf,
                        key: physical_key,
                    }
                }
                WriteOperation::Delta { cf, key, delta } => {
                    let (physical_cf, physical_key) = self.physical_key(cf, key);
                    OwnedWriteOperation::Delta {
                        cf: physical_cf,
                        key: physical_key,
                        delta: (*delta).clone(),
                    }
                }
            })
            .collect::<Vec<_>>();
        let borrowed = translated
            .iter()
            .map(OwnedWriteOperation::as_write_operation)
            .collect::<Vec<_>>();
        self.inner.write_many(&borrowed)
    }

    fn column_family_names(&self) -> Option<Vec<String>> {
        self.inner.column_family_names()
    }

    fn approximate_class_bytes(&self, cf: &ColumnFamilyName) -> Result<Option<u64>, Error> {
        let physical_cf = self.layout.map_cf(cf).physical_cf.to_owned();
        self.inner.approximate_class_bytes(&physical_cf)
    }
}

/// Storage that can be reconstructed with an expanded table/column-family set.
pub trait ReopenableStorage: OrderedKvStorage + Sized {
    /// Reopens the store so it also has the given column families, keeping
    /// existing data. Used when the schema gains tables/indices.
    fn reopen(self, column_families: &[&str]) -> Result<Self, Error>;
}

/// Typed view over one storage column family.
///
/// Wraps an [`OrderedKvStorage`] plus the record descriptor for one table (or
/// index) column family, so callers read and write typed rows instead of raw
/// bytes. Windowed history tables are handled transparently via the window
/// codec.
pub struct RecordStore<'a, S> {
    storage: &'a S,
    /// One table or durable index column family.
    column_family: &'a str,
    key_descriptor: Option<RecordDescriptor>,
    /// Interprets stored bytes without copying until a caller asks for values.
    descriptor: &'a RecordDescriptor,
    windowed: bool,
}

/// Counts produced when consolidating windowed-history records into windows.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct WindowConsolidation {
    /// Number of window values written.
    pub windows: usize,
    /// Number of individual records folded into those windows.
    pub records: usize,
}

impl<'a, S> RecordStore<'a, S>
where
    S: OrderedKvStorage,
{
    /// A typed store over one plain column family.
    ///
    /// * `storage` — the backing store.
    /// * `column_family` — the CF this store reads/writes.
    /// * `descriptor` — the row layout stored there.
    pub fn new(storage: &'a S, column_family: &'a str, descriptor: &'a RecordDescriptor) -> Self {
        Self {
            storage,
            column_family,
            key_descriptor: None,
            descriptor,
            windowed: false,
        }
    }

    /// A typed store over a windowed history CF, where reads and writes go
    /// through the window codec. `key_descriptor` describes the per-record
    /// key used inside a window.
    pub fn new_windowed(
        storage: &'a S,
        column_family: &'a str,
        key_descriptor: RecordDescriptor,
        descriptor: &'a RecordDescriptor,
    ) -> Self {
        Self {
            storage,
            column_family,
            key_descriptor: Some(key_descriptor),
            descriptor,
            windowed: true,
        }
    }

    /// The row layout of this store.
    pub fn descriptor(&self) -> &RecordDescriptor {
        self.descriptor
    }

    /// The column family this store targets.
    pub fn column_family(&self) -> &str {
        self.column_family
    }

    /// Reads one row's raw encoded bytes (window-aware for history stores).
    pub fn get_raw(&self, key: &Key) -> Result<Option<Vec<u8>>, Error> {
        if !self.windowed {
            return self.storage.get(self.column_family, key);
        }
        self.get_raw_run_aware(key)
    }

    /// Reads one row as a typed [`Record`] bound to this store's descriptor.
    pub fn get(&self, key: &Key) -> Result<Option<Record<'_>>, Error> {
        self.get_raw(key)
            .map(|record| record.map(|record| self.descriptor.bind_owned(record)))
    }

    /// Collects the `start..end` key range (window-aware for history stores).
    pub fn range(&self, start: &Key, end: &Key) -> Result<Vec<KeyValue>, Error> {
        if !self.windowed {
            return self.storage.range(self.column_family, start, end);
        }
        self.run_aware_bounded_records(start, Some(end), |key| key >= start && key < end)
    }

    /// Collects every row whose key starts with `prefix` (window-aware).
    pub fn prefix(&self, prefix: &Key) -> Result<Vec<KeyValue>, Error> {
        if !self.windowed {
            return self.storage.prefix(self.column_family, prefix);
        }
        self.run_aware_prefix_records(prefix)
    }

    pub fn range_reverse(&self, start: &Key, end: &Key) -> Result<Vec<KeyValue>, Error> {
        let mut records = self.range(start, end)?;
        records.reverse();
        Ok(records)
    }

    /// Streams the `start..end` key range through `visit` (window-aware).
    pub fn scan_range(
        &self,
        start: &Key,
        end: &Key,
        visit: &mut ScanVisitor<'_>,
    ) -> Result<(), Error> {
        if !self.windowed {
            return self
                .storage
                .scan_range(self.column_family, start, end, visit);
        }
        for (key, value) in self.range(start, end)? {
            visit(&key, &value)?;
        }
        Ok(())
    }

    /// Streams every row whose key starts with `prefix` through `visit`.
    pub fn scan_prefix(&self, prefix: &Key, visit: &mut ScanVisitor<'_>) -> Result<(), Error> {
        if !self.windowed {
            return self.storage.scan_prefix(self.column_family, prefix, visit);
        }
        for (key, value) in self.prefix(prefix)? {
            visit(&key, &value)?;
        }
        Ok(())
    }

    /// [`Self::scan_prefix`] in descending key order.
    pub fn scan_prefix_reverse(
        &self,
        prefix: &Key,
        visit: &mut ScanVisitor<'_>,
    ) -> Result<(), Error> {
        if !self.windowed {
            return self
                .storage
                .scan_prefix_reverse(self.column_family, prefix, visit);
        }
        for (key, value) in self.prefix(prefix)?.into_iter().rev() {
            visit(&key, &value)?;
        }
        Ok(())
    }

    /// The greatest row under `prefix`, or `None`.
    pub fn last_with_prefix(&self, prefix: &Key) -> Result<Option<KeyValue>, Error> {
        if !self.windowed {
            return self.storage.last_with_prefix(self.column_family, prefix);
        }
        self.last_logical_with_prefix(prefix)
    }

    /// The greatest row under `prefix` with key `<= upper` — a
    /// point-in-time "current at or before" read over versioned keys.
    pub fn last_with_prefix_before_or_at(
        &self,
        prefix: &Key,
        upper: &Key,
    ) -> Result<Option<KeyValue>, Error> {
        if !self.windowed {
            return self
                .storage
                .last_with_prefix_before_or_at(self.column_family, prefix, upper);
        }
        Ok(self
            .prefix(prefix)?
            .into_iter()
            .rfind(|(key, _)| key.as_slice() <= upper))
    }

    /// Builds a set/insert operation for this store's CF (apply via
    /// [`Self::write_many`]).
    pub fn set<'op>(&'op self, key: &'op Key, record: &'op [u8]) -> WriteOperation<'op> {
        WriteOperation::set(self.column_family, key, record)
    }

    /// Builds a delete operation for this store's CF.
    pub fn delete<'op>(&'op self, key: &'op Key) -> WriteOperation<'op> {
        WriteOperation::delete(self.column_family, key)
    }

    /// Builds a merge-delta operation for this store's CF (see
    /// [`StorageDelta`]).
    pub fn delta<'op>(&'op self, key: &'op Key, delta: &'op StorageDelta) -> WriteOperation<'op> {
        WriteOperation::delta(self.column_family, key, delta)
    }

    /// Applies a batch of operations atomically through the backend.
    pub fn write_many(&self, operations: &[WriteOperation<'_>]) -> Result<(), Error> {
        self.storage.write_many(operations)
    }

    /// Folds up to `max_records` loose history records into consolidated
    /// window values (unbounded window count).
    pub fn consolidate_windows(&self, max_records: usize) -> Result<WindowConsolidation, Error> {
        self.consolidate_windows_bounded(max_records, usize::MAX)
    }

    /// [`Self::consolidate_windows`] capped at `max_windows` windows, folding
    /// the trailing partial window too.
    pub fn consolidate_windows_bounded(
        &self,
        max_records: usize,
        max_windows: usize,
    ) -> Result<WindowConsolidation, Error> {
        self.consolidate_windows_bounded_inner(max_records, max_windows, true)
    }

    /// Like [`Self::consolidate_windows_bounded`] but only folds *full*
    /// windows, leaving the tail records loose for later appends.
    pub fn consolidate_full_windows_bounded(
        &self,
        max_records: usize,
        max_windows: usize,
    ) -> Result<WindowConsolidation, Error> {
        self.consolidate_windows_bounded_inner(max_records, max_windows, false)
    }

    fn consolidate_windows_bounded_inner(
        &self,
        max_records: usize,
        max_windows: usize,
        consolidate_tail: bool,
    ) -> Result<WindowConsolidation, Error> {
        let use_cursor = !consolidate_tail;
        if !self.windowed || max_records == 0 || max_windows == 0 {
            return Ok(WindowConsolidation::default());
        }
        let raw = if use_cursor {
            self.storage.range(
                self.column_family,
                &self.window_consolidation_cursor()?,
                WINDOW_MARKER_KEY,
            )?
        } else {
            self.storage.prefix(self.column_family, b"")?
        };
        let mut owned_operations = Vec::new();
        let mut plain_run = Vec::<KeyValue>::new();
        let mut consolidated = WindowConsolidation::default();
        let mut cursor = None;
        for (key, value) in raw {
            if is_window_meta_key(&key) {
                continue;
            }
            if decode_window_value(&value)?.is_some() {
                append_window_operations(
                    self,
                    &mut owned_operations,
                    &mut plain_run,
                    max_records,
                    max_windows,
                    &mut consolidated,
                )?;
                cursor = Some(key);
                if consolidated.windows >= max_windows {
                    break;
                }
                continue;
            }
            plain_run.push((key, value));
            if plain_run.len() >= max_records {
                let next_cursor = plain_run.last().map(|(key, _)| key.clone());
                append_window_operations(
                    self,
                    &mut owned_operations,
                    &mut plain_run,
                    max_records,
                    max_windows,
                    &mut consolidated,
                )?;
                cursor = next_cursor;
                if consolidated.windows >= max_windows {
                    break;
                }
            }
        }
        if consolidate_tail && consolidated.windows < max_windows {
            let next_cursor = plain_run.last().map(|(key, _)| key.clone());
            append_window_operations(
                self,
                &mut owned_operations,
                &mut plain_run,
                max_records,
                max_windows,
                &mut consolidated,
            )?;
            cursor = next_cursor;
        }
        if consolidated.records > 0 && !self.window_marker_present()? {
            owned_operations.push(OwnedWriteOperation::Set {
                cf: self.column_family.to_owned(),
                key: WINDOW_MARKER_KEY.to_vec(),
                value: Vec::new(),
            });
        }
        if use_cursor && let Some(last_seen) = cursor {
            let cursor = cursor_after_last_seen(last_seen.as_slice());
            let current_cursor = self.window_consolidation_cursor()?;
            if cursor.as_slice() > current_cursor.as_slice() {
                owned_operations.push(OwnedWriteOperation::Set {
                    cf: self.column_family.to_owned(),
                    key: WINDOW_CURSOR_KEY.to_vec(),
                    value: cursor,
                });
            }
        }
        if consolidated.records > 0 {
            self.append_window_range_index_operations(&mut owned_operations)?;
        }
        if owned_operations.is_empty() {
            return Ok(WindowConsolidation::default());
        }
        let operations = owned_operations
            .iter()
            .map(OwnedWriteOperation::as_write_operation)
            .collect::<Vec<_>>();
        self.storage.write_many(&operations)?;
        Ok(consolidated)
    }

    fn get_raw_run_aware(&self, key: &Key) -> Result<Option<Vec<u8>>, Error> {
        if is_window_meta_key(key) {
            return Ok(None);
        }
        if let Some(value) = self.storage.get(self.column_family, key)? {
            if let Some(window) = decode_window_value(&value)? {
                return self.lookup_window_record(key, window.codec, key);
            }
            return Ok(Some(value));
        }
        if !self.window_marker_present()? {
            return Ok(None);
        }
        if let Some(value) = self.get_raw_from_window_range_index(key)? {
            return Ok(value);
        }
        if self
            .storage
            .get(self.column_family, WINDOW_RANGE_INDEX_KEY)?
            .is_some()
        {
            return Ok(None);
        }
        let value = self.get_raw_run_aware_legacy_walk(key)?;
        self.write_window_range_index()?;
        Ok(value)
    }

    fn get_raw_run_aware_legacy_walk(&self, key: &Key) -> Result<Option<Vec<u8>>, Error> {
        let mut upper = key.to_vec();
        while let Some((raw_key, raw_value)) =
            self.storage
                .last_with_prefix_before_or_at(self.column_family, b"", &upper)?
        {
            if is_window_meta_key(&raw_key) {
                let Some(previous) = lexicographic_predecessor(&raw_key) else {
                    return Ok(None);
                };
                upper = previous;
                continue;
            }
            let Some(window) = decode_window_value(&raw_value)? else {
                let Some(previous) = lexicographic_predecessor(&raw_key) else {
                    return Ok(None);
                };
                upper = previous;
                continue;
            };
            if key <= window.max_key
                && let Some(value) = self.lookup_window_record(&raw_key, window.codec, key)?
            {
                return Ok(Some(value));
            }
            let Some(previous) = lexicographic_predecessor(&raw_key) else {
                return Ok(None);
            };
            upper = previous;
        }
        Ok(None)
    }

    fn get_raw_from_window_range_index(&self, key: &Key) -> Result<Option<Option<Vec<u8>>>, Error> {
        let Some(index) = self.window_range_index()? else {
            return Ok(None);
        };
        for entry in index
            .entries
            .iter()
            .filter(|entry| entry.window_key.as_slice() <= key && key <= entry.max_key.as_slice())
            .rev()
        {
            let Some(raw_value) = self.storage.get(self.column_family, &entry.window_key)? else {
                continue;
            };
            let Some(window) = decode_window_value(&raw_value)? else {
                continue;
            };
            if key <= window.max_key
                && let Some(value) =
                    self.lookup_window_record(&entry.window_key, window.codec, key)?
            {
                return Ok(Some(Some(value)));
            }
        }
        Ok(Some(None))
    }

    fn last_logical_with_prefix(&self, prefix: &Key) -> Result<Option<KeyValue>, Error> {
        let mut candidate = self.storage.last_with_prefix(self.column_family, prefix)?;
        while let Some((raw_key, _)) = candidate
            .as_ref()
            .filter(|(key, _)| is_window_meta_key(key))
        {
            let Some(upper) = lexicographic_predecessor(raw_key) else {
                return Ok(None);
            };
            candidate =
                self.storage
                    .last_with_prefix_before_or_at(self.column_family, prefix, &upper)?;
        }
        let Some((raw_key, raw_value)) = candidate else {
            return Ok(None);
        };
        let Some(window) = decode_window_value(&raw_value)? else {
            return Ok(Some((raw_key, raw_value)));
        };
        self.last_window_record_before_or_at(window.codec, prefix, None)
    }

    fn last_window_record_before_or_at(
        &self,
        window: &[u8],
        prefix: &Key,
        upper: Option<&Key>,
    ) -> Result<Option<KeyValue>, Error> {
        Ok(self
            .decode_window_records(window)?
            .into_iter()
            .rev()
            .find(|(key, _)| {
                key.starts_with(prefix) && upper.is_none_or(|upper| key.as_slice() <= upper)
            }))
    }

    fn run_aware_records(
        &self,
        include: impl Fn(&[u8]) -> bool,
        scan_raw: impl FnOnce(&mut ScanVisitor<'_>) -> Result<(), Error>,
    ) -> Result<Vec<KeyValue>, Error> {
        let mut records = BTreeMap::<Vec<u8>, Vec<u8>>::new();
        scan_raw(&mut |raw_key, raw_value| {
            if is_window_meta_key(raw_key) {
                return Ok(());
            }
            if let Some(window) = decode_window_value(raw_value)? {
                for (key, value) in self.decode_window_records_cached(raw_key, window.codec)? {
                    if include(&key) {
                        records.insert(key, value);
                    }
                }
            } else if include(raw_key) {
                records.insert(raw_key.to_vec(), raw_value.to_vec());
            }
            Ok(())
        })?;
        Ok(records.into_iter().collect())
    }

    fn run_aware_prefix_records(&self, prefix: &Key) -> Result<Vec<KeyValue>, Error> {
        if let Some(end) = key_codec::prefix_upper_bound(prefix) {
            self.run_aware_bounded_records(prefix, Some(&end), |key| key.starts_with(prefix))
        } else {
            self.run_aware_bounded_records(prefix, None, |key| key.starts_with(prefix))
        }
    }

    fn run_aware_bounded_records(
        &self,
        start: &Key,
        end: Option<&Key>,
        include: impl Fn(&[u8]) -> bool,
    ) -> Result<Vec<KeyValue>, Error> {
        if !self.window_marker_present()? {
            return self.scan_plain_bounded_records(start, end, include);
        }
        let Some(index) = self.window_range_index()? else {
            let records = self.run_aware_records(include, |visit| {
                self.storage.scan_prefix(self.column_family, b"", visit)
            })?;
            self.write_window_range_index()?;
            return Ok(records);
        };

        let mut physical_records = Vec::<(Vec<u8>, Vec<KeyValue>)>::new();
        self.scan_raw_bounded(start, end, &mut |raw_key, raw_value| {
            if is_window_meta_key(raw_key) || decode_window_value(raw_value)?.is_some() {
                return Ok(());
            }
            if include(raw_key) {
                physical_records.push((
                    raw_key.to_vec(),
                    vec![(raw_key.to_vec(), raw_value.to_vec())],
                ));
            }
            Ok(())
        })?;

        for entry in index.entries.iter().filter(|entry| {
            entry.max_key.as_slice() >= start
                && end.is_none_or(|end| entry.window_key.as_slice() < end)
        }) {
            let Some(raw_value) = self.storage.get(self.column_family, &entry.window_key)? else {
                continue;
            };
            let Some(window) = decode_window_value(&raw_value)? else {
                continue;
            };
            let records = self
                .decode_window_records_cached(&entry.window_key, window.codec)?
                .into_iter()
                .filter(|(key, _)| include(key))
                .collect::<Vec<_>>();
            if !records.is_empty() {
                physical_records.push((entry.window_key.clone(), records));
            }
        }

        physical_records.sort_by(|(left, _), (right, _)| left.cmp(right));
        let mut records = BTreeMap::<Vec<u8>, Vec<u8>>::new();
        for (_, logical_records) in physical_records {
            for (key, value) in logical_records {
                records.insert(key, value);
            }
        }
        Ok(records.into_iter().collect())
    }

    fn scan_plain_bounded_records(
        &self,
        start: &Key,
        end: Option<&Key>,
        include: impl Fn(&[u8]) -> bool,
    ) -> Result<Vec<KeyValue>, Error> {
        let mut records = Vec::new();
        self.scan_raw_bounded(start, end, &mut |key, value| {
            if !is_window_meta_key(key) && include(key) {
                records.push((key.to_vec(), value.to_vec()));
            }
            Ok(())
        })?;
        Ok(records)
    }

    fn scan_raw_bounded(
        &self,
        start: &Key,
        end: Option<&Key>,
        visit: &mut ScanVisitor<'_>,
    ) -> Result<(), Error> {
        if let Some(end) = end {
            self.storage
                .scan_range(self.column_family, start, end, visit)
        } else {
            self.storage.scan_prefix(self.column_family, start, visit)
        }
    }

    fn window_marker_present(&self) -> Result<bool, Error> {
        Ok(self
            .storage
            .get(self.column_family, WINDOW_MARKER_KEY)?
            .is_some())
    }

    fn window_consolidation_cursor(&self) -> Result<Vec<u8>, Error> {
        Ok(self
            .storage
            .get(self.column_family, WINDOW_CURSOR_KEY)?
            .unwrap_or_default())
    }

    fn window_range_index(&self) -> Result<Option<WindowRangeIndex>, Error> {
        self.storage
            .get(self.column_family, WINDOW_RANGE_INDEX_KEY)?
            .map(|value| decode_window_range_index(&value))
            .transpose()
    }

    fn append_window_range_index_operations(
        &self,
        operations: &mut Vec<OwnedWriteOperation>,
    ) -> Result<(), Error> {
        let mut index = match self.window_range_index()? {
            Some(index) => index,
            None => self.build_window_range_index()?,
        };
        for operation in operations.iter() {
            match operation {
                OwnedWriteOperation::Set { key, value, .. } => {
                    if let Some(window) = decode_window_value(value)? {
                        index.upsert(key.clone(), window.max_key.to_vec());
                    }
                }
                OwnedWriteOperation::Delete { key, .. } => {
                    index.remove(key);
                }
                OwnedWriteOperation::Delta { .. } => {}
            }
        }
        operations.push(OwnedWriteOperation::Set {
            cf: self.column_family.to_owned(),
            key: WINDOW_RANGE_INDEX_KEY.to_vec(),
            value: encode_window_range_index(&index)?,
        });
        Ok(())
    }

    fn write_window_range_index(&self) -> Result<(), Error> {
        let index = self.build_window_range_index()?;
        let value = encode_window_range_index(&index)?;
        self.storage.write_many(&[WriteOperation::set(
            self.column_family,
            WINDOW_RANGE_INDEX_KEY,
            &value,
        )])
    }

    fn build_window_range_index(&self) -> Result<WindowRangeIndex, Error> {
        let mut index = WindowRangeIndex::default();
        self.storage
            .scan_prefix(self.column_family, b"", &mut |key, value| {
                if is_window_meta_key(key) {
                    return Ok(());
                }
                if let Some(window) = decode_window_value(value)? {
                    index.upsert(key.to_vec(), window.max_key.to_vec());
                }
                Ok(())
            })?;
        Ok(index)
    }

    fn lookup_window_record(
        &self,
        window_key: &[u8],
        window: &[u8],
        key: &Key,
    ) -> Result<Option<Vec<u8>>, Error> {
        let schema = self.window_schema()?;
        let key_record = self.key_record_from_bytes(key)?;
        let cache_key = DecodedWindowCacheKey {
            storage_token: self.storage.cache_token(),
            column_family: self.column_family.to_owned(),
            window_key: window_key.to_vec(),
        };
        let cache = DECODED_WINDOW_CACHE.get_or_init(|| Mutex::new(DecodedWindowCache::default()));
        if let Some(value) = cache
            .lock()
            .expect("decoded window cache poisoned")
            .get_record(&cache_key, key)
        {
            return Ok(value);
        }
        if should_decode_full_window_for_probe(&cache_key) {
            let records = self.decode_window_records(window)?;
            let value = records
                .binary_search_by(|(record_key, _)| record_key.as_slice().cmp(key))
                .ok()
                .map(|idx| records[idx].1.clone());
            cache
                .lock()
                .expect("decoded window cache poisoned")
                .insert(cache_key, records);
            return Ok(value);
        }
        Ok(lookup_window(&schema, window, &key_record)?.map(|record| record.value.into_raw()))
    }

    fn decode_window_records(&self, window: &[u8]) -> Result<Vec<KeyValue>, Error> {
        let schema = self.window_schema()?;
        decode_window(&schema, window)?
            .into_iter()
            .map(|record| {
                let key = encode_key_record(record.key.borrowed().to_values()?)?;
                Ok((key, record.value.into_raw()))
            })
            .collect()
    }

    fn decode_window_records_cached(
        &self,
        window_key: &[u8],
        window: &[u8],
    ) -> Result<Vec<KeyValue>, Error> {
        let key = DecodedWindowCacheKey {
            storage_token: self.storage.cache_token(),
            column_family: self.column_family.to_owned(),
            window_key: window_key.to_vec(),
        };
        let cache = DECODED_WINDOW_CACHE.get_or_init(|| Mutex::new(DecodedWindowCache::default()));
        if let Some(cached) = cache
            .lock()
            .expect("decoded window cache poisoned")
            .get(&key)
        {
            return Ok(cached);
        }
        let records = self.decode_window_records(window)?;
        cache
            .lock()
            .expect("decoded window cache poisoned")
            .insert(key, records.clone());
        Ok(records)
    }

    fn encode_window_records(&self, records: &[KeyValue]) -> Result<Vec<u8>, Error> {
        let schema = self.window_schema()?;
        let records = records
            .iter()
            .map(|(key, value)| {
                Ok(WindowRecord::new(
                    self.key_record_from_bytes(key)?,
                    OwnedRecord::new(value.clone(), *self.descriptor),
                ))
            })
            .collect::<Result<Vec<_>, Error>>()?;
        Ok(encode_window(&schema, &records)?)
    }

    fn window_schema(&self) -> Result<WindowSchema, Error> {
        let key = self
            .key_descriptor
            .ok_or_else(|| Error::InvalidWindowRecord("missing key descriptor".to_owned()))?;
        Ok(WindowSchema::new(key, *self.descriptor))
    }

    fn key_record_from_bytes(&self, key: &Key) -> Result<OwnedRecord, Error> {
        let descriptor = self
            .key_descriptor
            .ok_or_else(|| Error::InvalidWindowRecord("missing key descriptor".to_owned()))?;
        let values = decode_key_record(key, &descriptor)?;
        Ok(OwnedRecord::new(descriptor.create(&values)?, descriptor))
    }
}

fn should_decode_full_window_for_probe(key: &DecodedWindowCacheKey) -> bool {
    let tracker = WINDOW_PROBE_TRACKER.get_or_init(|| Mutex::new(None));
    let mut tracker = tracker.lock().expect("window probe tracker poisoned");
    match tracker.as_mut() {
        Some((last, count)) if last == key => {
            *count += 1;
            *count >= 2
        }
        _ => {
            *tracker = Some((key.clone(), 1));
            false
        }
    }
}

const WINDOW_VALUE_MAGIC: &[u8] = b"GWIN2\0";
const WINDOW_MARKER_KEY: &[u8] = b"\xffGWIN2-META";
const WINDOW_CURSOR_KEY: &[u8] = b"\xffGWIN2-CURSOR";
const WINDOW_RANGE_INDEX_KEY: &[u8] = b"\xffGWIN2-RANGE-INDEX";

fn is_window_meta_key(key: &[u8]) -> bool {
    key == WINDOW_MARKER_KEY || key == WINDOW_CURSOR_KEY || key == WINDOW_RANGE_INDEX_KEY
}

fn lexicographic_predecessor(key: &[u8]) -> Option<Vec<u8>> {
    let mut predecessor = key.to_vec();
    for byte in predecessor.iter_mut().rev() {
        if *byte > 0 {
            *byte -= 1;
            return Some(predecessor);
        }
    }
    None
}

fn cursor_after_last_seen(key: &[u8]) -> Vec<u8> {
    let mut cursor = key.to_vec();
    cursor.push(0);
    cursor
}

struct DecodedWindowValue<'a> {
    max_key: &'a [u8],
    codec: &'a [u8],
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
struct WindowRangeIndex {
    entries: Vec<WindowRangeIndexEntry>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct WindowRangeIndexEntry {
    window_key: Vec<u8>,
    max_key: Vec<u8>,
}

impl WindowRangeIndex {
    fn upsert(&mut self, window_key: Vec<u8>, max_key: Vec<u8>) {
        match self
            .entries
            .binary_search_by(|entry| entry.window_key.cmp(&window_key))
        {
            Ok(index) => self.entries[index].max_key = max_key,
            Err(index) => self.entries.insert(
                index,
                WindowRangeIndexEntry {
                    window_key,
                    max_key,
                },
            ),
        }
    }

    fn remove(&mut self, window_key: &[u8]) {
        if let Ok(index) = self
            .entries
            .binary_search_by(|entry| entry.window_key.as_slice().cmp(window_key))
        {
            self.entries.remove(index);
        }
    }
}

fn encode_window_value(max_key: &[u8], codec_bytes: &[u8]) -> Vec<u8> {
    let max_key_len = u32::try_from(max_key.len()).expect("window key length fits in u32");
    let mut value = Vec::with_capacity(
        WINDOW_VALUE_MAGIC.len() + std::mem::size_of::<u32>() + max_key.len() + codec_bytes.len(),
    );
    value.extend_from_slice(WINDOW_VALUE_MAGIC);
    value.extend(max_key_len.to_be_bytes());
    value.extend_from_slice(max_key);
    value.extend_from_slice(codec_bytes);
    value
}

fn decode_window_value(value: &[u8]) -> Result<Option<DecodedWindowValue<'_>>, Error> {
    if !value.starts_with(WINDOW_VALUE_MAGIC) {
        return Ok(None);
    }
    let mut remaining = &value[WINDOW_VALUE_MAGIC.len()..];
    let max_key_len = u32::from_be_bytes(
        take_key_bytes(&mut remaining, std::mem::size_of::<u32>())?
            .try_into()
            .expect("slice has u32 length"),
    ) as usize;
    if remaining.len() < max_key_len {
        return Err(Error::InvalidWindowRecord(
            "window value has truncated max key".to_owned(),
        ));
    }
    let (max_key, codec) = remaining.split_at(max_key_len);
    Ok(Some(DecodedWindowValue { max_key, codec }))
}

fn encode_window_range_index(index: &WindowRangeIndex) -> Result<Vec<u8>, Error> {
    postcard::to_allocvec(index).map_err(|error| Error::InvalidWindowRecord(error.to_string()))
}

fn decode_window_range_index(value: &[u8]) -> Result<WindowRangeIndex, Error> {
    postcard::from_bytes(value).map_err(|error| Error::InvalidWindowRecord(error.to_string()))
}

fn append_window_operations<S>(
    store: &RecordStore<'_, S>,
    operations: &mut Vec<OwnedWriteOperation>,
    plain_run: &mut Vec<KeyValue>,
    max_records: usize,
    max_windows: usize,
    consolidated: &mut WindowConsolidation,
) -> Result<(), Error>
where
    S: OrderedKvStorage,
{
    if plain_run.len() <= 1 {
        plain_run.clear();
        return Ok(());
    }
    for chunk in plain_run.chunks(max_records) {
        if consolidated.windows >= max_windows {
            break;
        }
        if chunk.len() <= 1 {
            continue;
        }
        for (key, _) in chunk {
            operations.push(OwnedWriteOperation::Delete {
                cf: store.column_family.to_owned(),
                key: key.clone(),
            });
        }
        let window = store.encode_window_records(chunk)?;
        operations.push(OwnedWriteOperation::Set {
            cf: store.column_family.to_owned(),
            key: chunk[0].0.clone(),
            value: encode_window_value(&chunk[chunk.len() - 1].0, &window),
        });
        consolidated.windows += 1;
        consolidated.records += chunk.len();
    }
    plain_run.clear();
    Ok(())
}

fn decode_key_record(key: &[u8], descriptor: &RecordDescriptor) -> Result<Vec<RecordValue>, Error> {
    let mut remaining = key;
    let mut values = Vec::with_capacity(descriptor.fields().len());
    for field in descriptor.fields() {
        values.push(decode_key_part(&mut remaining, &field.value_type)?);
    }
    if !remaining.is_empty() {
        return Err(Error::InvalidStorageKey(
            "encoded key has trailing bytes".to_owned(),
        ));
    }
    Ok(values)
}

fn encode_key_record(values: Vec<RecordValue>) -> Result<Vec<u8>, Error> {
    let mut key = Vec::new();
    for value in values {
        encode_key_part(&mut key, &value)?;
    }
    Ok(key)
}

fn encode_key_part(key: &mut Vec<u8>, value: &RecordValue) -> Result<(), Error> {
    match value {
        RecordValue::U8(value) => {
            key.push(0);
            key.push(*value);
        }
        RecordValue::U16(value) => {
            key.push(1);
            key.extend(value.to_be_bytes());
        }
        RecordValue::U32(value) => {
            key.push(2);
            key.extend(value.to_be_bytes());
        }
        RecordValue::U64(value) => {
            key.push(3);
            key.extend(value.to_be_bytes());
        }
        RecordValue::I64(value) => {
            key.push(13);
            key.extend(order_preserving_i64_bits(*value).to_be_bytes());
        }
        RecordValue::Bool(value) => {
            key.push(5);
            key.push(u8::from(*value));
        }
        RecordValue::String(value) => {
            key.push(6);
            encode_ordered_bytes(key, value.as_bytes());
        }
        RecordValue::Enum(value) => {
            key.push(0);
            key.push(*value);
        }
        RecordValue::Bytes(value) => {
            key.push(7);
            encode_ordered_bytes(key, value);
        }
        RecordValue::Uuid(value) => {
            key.push(10);
            key.extend_from_slice(value.as_bytes());
        }
        RecordValue::F64(_)
        | RecordValue::Tuple(_)
        | RecordValue::Array(_)
        | RecordValue::Nullable(_) => {
            return Err(Error::InvalidStorageKey(
                "unsupported window key value type".to_owned(),
            ));
        }
    }
    Ok(())
}

fn decode_key_part(bytes: &mut &[u8], value_type: &ValueType) -> Result<RecordValue, Error> {
    match value_type {
        ValueType::U8 => {
            expect_key_tag(bytes, 0)?;
            Ok(RecordValue::U8(take_key_bytes(bytes, 1)?[0]))
        }
        ValueType::U16 => {
            expect_key_tag(bytes, 1)?;
            Ok(RecordValue::U16(u16::from_be_bytes(
                take_key_bytes(bytes, 2)?
                    .try_into()
                    .expect("slice has u16 length"),
            )))
        }
        ValueType::U32 => {
            expect_key_tag(bytes, 2)?;
            Ok(RecordValue::U32(u32::from_be_bytes(
                take_key_bytes(bytes, 4)?
                    .try_into()
                    .expect("slice has u32 length"),
            )))
        }
        ValueType::U64 => {
            expect_key_tag(bytes, 3)?;
            Ok(RecordValue::U64(u64::from_be_bytes(
                take_key_bytes(bytes, 8)?
                    .try_into()
                    .expect("slice has u64 length"),
            )))
        }
        ValueType::I64 => {
            expect_key_tag(bytes, 13)?;
            Ok(RecordValue::I64(
                (u64::from_be_bytes(
                    take_key_bytes(bytes, 8)?
                        .try_into()
                        .expect("slice has i64 length"),
                ) ^ (1_u64 << 63)) as i64,
            ))
        }
        ValueType::Bool => {
            expect_key_tag(bytes, 5)?;
            match take_key_bytes(bytes, 1)?[0] {
                0 => Ok(RecordValue::Bool(false)),
                1 => Ok(RecordValue::Bool(true)),
                _ => Err(Error::InvalidStorageKey(
                    "invalid bool key payload".to_owned(),
                )),
            }
        }
        ValueType::String => {
            expect_key_tag(bytes, 6)?;
            Ok(RecordValue::String(
                String::from_utf8(decode_ordered_bytes(bytes)?)
                    .map_err(|_| Error::InvalidStorageKey("invalid string key".to_owned()))?,
            ))
        }
        ValueType::Bytes => {
            expect_key_tag(bytes, 7)?;
            Ok(RecordValue::Bytes(decode_ordered_bytes(bytes)?))
        }
        ValueType::Uuid => {
            expect_key_tag(bytes, 10)?;
            Ok(RecordValue::Uuid(uuid::Uuid::from_bytes(
                take_key_bytes(bytes, 16)?
                    .try_into()
                    .expect("slice has uuid length"),
            )))
        }
        ValueType::Enum(_) => {
            expect_key_tag(bytes, 0)?;
            Ok(RecordValue::Enum(take_key_bytes(bytes, 1)?[0]))
        }
        ValueType::F64 | ValueType::Tuple(_) | ValueType::Array(_) | ValueType::Nullable(_) => Err(
            Error::InvalidStorageKey("unsupported window key type".to_owned()),
        ),
    }
}

fn order_preserving_i64_bits(value: i64) -> u64 {
    (value as u64) ^ (1_u64 << 63)
}

fn encode_ordered_bytes(key: &mut Vec<u8>, value: &[u8]) {
    for byte in value {
        if *byte == 0 {
            key.extend([0, 0xff]);
        } else {
            key.push(*byte);
        }
    }
    key.extend([0, 0]);
}

fn decode_ordered_bytes(bytes: &mut &[u8]) -> Result<Vec<u8>, Error> {
    let mut value = Vec::new();
    loop {
        let byte = take_key_bytes(bytes, 1)?[0];
        if byte != 0 {
            value.push(byte);
            continue;
        }
        let escaped = take_key_bytes(bytes, 1)?[0];
        match escaped {
            0 => return Ok(value),
            0xff => value.push(0),
            _ => return Err(Error::InvalidStorageKey("invalid escaped key".to_owned())),
        }
    }
}

fn expect_key_tag(bytes: &mut &[u8], expected: u8) -> Result<(), Error> {
    let actual = take_key_bytes(bytes, 1)?[0];
    if actual == expected {
        Ok(())
    } else {
        Err(Error::InvalidStorageKey(format!(
            "expected key tag {expected}, got {actual}"
        )))
    }
}

fn take_key_bytes<'a>(bytes: &mut &'a [u8], count: usize) -> Result<&'a [u8], Error> {
    if bytes.len() < count {
        return Err(Error::InvalidStorageKey("truncated key".to_owned()));
    }
    let (head, tail) = bytes.split_at(count);
    *bytes = tail;
    Ok(head)
}

/// One entry of a [`OrderedKvStorage::write_many`] batch, borrowing its bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WriteOperation<'a> {
    /// Borrowed operation so callers can build a RocksDB batch without cloning
    /// already-owned encoded records.
    Set {
        cf: &'a str,
        key: &'a Key,
        value: &'a [u8],
    },
    /// Remove a key.
    Delete { cf: &'a str, key: &'a Key },
    /// Merge a [`StorageDelta`] into the key's current value.
    Delta {
        cf: &'a str,
        key: &'a Key,
        delta: &'a StorageDelta,
    },
}

/// An owned [`WriteOperation`], for staging writes that must outlive the
/// borrows they were built from.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OwnedWriteOperation {
    /// Owned form of [`WriteOperation::Set`].
    Set {
        cf: String,
        key: Vec<u8>,
        value: Vec<u8>,
    },
    /// Owned form of [`WriteOperation::Delete`].
    Delete { cf: String, key: Vec<u8> },
    /// Owned form of [`WriteOperation::Delta`].
    Delta {
        cf: String,
        key: Vec<u8>,
        delta: StorageDelta,
    },
}

impl OwnedWriteOperation {
    /// Borrows this owned operation as a [`WriteOperation`] for a batch call.
    pub fn as_write_operation(&self) -> WriteOperation<'_> {
        match self {
            Self::Set { cf, key, value } => WriteOperation::set(cf, key, value),
            Self::Delete { cf, key } => WriteOperation::delete(cf, key),
            Self::Delta { cf, key, delta } => WriteOperation::delta(cf, key, delta),
        }
    }

    /// The target column family, whatever the variant.
    fn cf(&self) -> &str {
        match self {
            Self::Set { cf, .. } | Self::Delete { cf, .. } | Self::Delta { cf, .. } => cf,
        }
    }

    /// The target key, whatever the variant.
    fn key(&self) -> &[u8] {
        match self {
            Self::Set { key, .. } | Self::Delete { key, .. } | Self::Delta { key, .. } => key,
        }
    }
}

/// A read-your-writes view: reads see `staged_writes` layered on top of
/// `base`, but the staged writes are not yet committed to the backend. The
/// tick engine uses this so index writes computed during a tick are visible
/// to that same tick before they are flushed atomically with the batch.
pub struct StagedWriteOverlay<'a, S> {
    base: &'a S,
    staged_writes: &'a RefCell<StagedWriteState>,
}

/// The buffered writes behind a [`StagedWriteOverlay`], in application order,
/// with a lazily-built per-(cf, key) index so repeated point reads over a
/// large staging set stay fast.
#[derive(Clone, Debug, Default)]
pub(crate) struct StagedWriteState {
    operations: Vec<OwnedWriteOperation>,
    latest_by_cf_key: Option<BTreeMap<String, BTreeMap<Vec<u8>, usize>>>,
    point_reads_without_index: usize,
}

impl StagedWriteState {
    pub(crate) fn is_empty(&self) -> bool {
        self.operations.is_empty()
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.operations.len()
    }

    pub(crate) fn stage(&mut self, operation: OwnedWriteOperation) {
        let index = self.operations.len();
        if let Some(latest_by_cf_key) = &mut self.latest_by_cf_key {
            latest_by_cf_key
                .entry(operation.cf().to_owned())
                .or_default()
                .insert(operation.key().to_vec(), index);
        }
        self.operations.push(operation);
    }

    pub(crate) fn extend(&mut self, operations: impl IntoIterator<Item = OwnedWriteOperation>) {
        for operation in operations {
            self.stage(operation);
        }
    }

    pub(crate) fn into_operations(self) -> Vec<OwnedWriteOperation> {
        self.operations
    }

    fn latest_index(&mut self, cf: &ColumnFamilyName, key: &Key) -> Option<usize> {
        if self.latest_by_cf_key.is_none() {
            if self.operations.len() < STAGED_OPS_BEFORE_POINT_INDEX
                && self.point_reads_without_index < STAGED_POINT_READS_BEFORE_INDEX
            {
                self.point_reads_without_index += 1;
                return self
                    .operations
                    .iter()
                    .enumerate()
                    .rev()
                    .find_map(|(index, operation)| {
                        (operation.cf() == cf && operation.key() == key).then_some(index)
                    });
            }

            let mut latest_by_cf_key: BTreeMap<String, BTreeMap<Vec<u8>, usize>> = BTreeMap::new();
            for (index, operation) in self.operations.iter().enumerate() {
                latest_by_cf_key
                    .entry(operation.cf().to_owned())
                    .or_default()
                    .insert(operation.key().to_vec(), index);
            }
            self.latest_by_cf_key = Some(latest_by_cf_key);
        }

        self.latest_by_cf_key
            .as_ref()
            .and_then(|latest_by_cf_key| latest_by_cf_key.get(cf))
            .and_then(|latest_by_key| latest_by_key.get(key).copied())
    }

    pub(crate) fn contains_key(&mut self, cf: &ColumnFamilyName, key: &Key) -> bool {
        self.latest_index(cf, key).is_some()
    }
}

impl From<Vec<OwnedWriteOperation>> for StagedWriteState {
    fn from(operations: Vec<OwnedWriteOperation>) -> Self {
        let mut state = Self::default();
        state.extend(operations);
        state
    }
}

/// Encoded storage transaction with read-your-own-writes semantics.
///
/// This type is intentionally storage-shaped: it knows only column-family
/// names plus encoded keys/values. It does not understand record descriptors,
/// schemas, IVM deltas, or Jazz transaction semantics.
pub struct StorageTransaction<'a, S> {
    base: &'a S,
    staged_writes: RefCell<StagedWriteState>,
}

impl<'a, S> StorageTransaction<'a, S>
where
    S: OrderedKvStorage,
{
    /// Opens a transaction buffering writes over `base` (see
    /// [`OrderedKvStorage::begin_txn`]).
    pub fn new(base: &'a S) -> Self {
        Self {
            base,
            staged_writes: RefCell::new(StagedWriteState::default()),
        }
    }

    /// Flushes all buffered writes to the backend in one atomic
    /// [`OrderedKvStorage::write_many`]. Dropping without committing discards
    /// them.
    pub fn commit(self) -> Result<(), Error> {
        let operations = self.staged_writes.into_inner().into_operations();
        let borrowed = operations
            .iter()
            .map(OwnedWriteOperation::as_write_operation)
            .collect::<Vec<_>>();
        self.base.write_many(&borrowed)
    }

    /// `true` when nothing has been staged yet.
    pub fn is_empty(&self) -> bool {
        self.staged_writes.borrow().is_empty()
    }

    /// Stages already-owned operations into the transaction.
    pub fn stage_owned_operations(
        &self,
        operations: impl IntoIterator<Item = OwnedWriteOperation>,
    ) {
        self.staged_writes.borrow_mut().extend(operations);
    }
}

impl<S> OrderedKvStorage for StorageTransaction<'_, S>
where
    S: OrderedKvStorage,
{
    fn get(&self, cf: &ColumnFamilyName, key: &Key) -> Result<Option<Value>, Error> {
        StagedWriteOverlay::new(self.base, &self.staged_writes).get(cf, key)
    }

    fn set(&self, cf: &ColumnFamilyName, key: &Key, value: &[u8]) -> Result<(), Error> {
        StagedWriteOverlay::new(self.base, &self.staged_writes).set(cf, key, value)
    }

    fn delete(&self, cf: &ColumnFamilyName, key: &Key) -> Result<(), Error> {
        StagedWriteOverlay::new(self.base, &self.staged_writes).delete(cf, key)
    }

    fn scan_range(
        &self,
        cf: &ColumnFamilyName,
        start: &Key,
        end: &Key,
        visit: &mut ScanVisitor<'_>,
    ) -> Result<(), Error> {
        StagedWriteOverlay::new(self.base, &self.staged_writes).scan_range(cf, start, end, visit)
    }

    fn scan_prefix(
        &self,
        cf: &ColumnFamilyName,
        prefix: &Key,
        visit: &mut ScanVisitor<'_>,
    ) -> Result<(), Error> {
        StagedWriteOverlay::new(self.base, &self.staged_writes).scan_prefix(cf, prefix, visit)
    }

    fn scan_prefix_reverse(
        &self,
        cf: &ColumnFamilyName,
        prefix: &Key,
        visit: &mut ScanVisitor<'_>,
    ) -> Result<(), Error> {
        StagedWriteOverlay::new(self.base, &self.staged_writes)
            .scan_prefix_reverse(cf, prefix, visit)
    }

    fn last_with_prefix(
        &self,
        cf: &ColumnFamilyName,
        prefix: &Key,
    ) -> Result<Option<KeyValue>, Error> {
        StagedWriteOverlay::new(self.base, &self.staged_writes).last_with_prefix(cf, prefix)
    }

    fn last_with_prefix_before_or_at(
        &self,
        cf: &ColumnFamilyName,
        prefix: &Key,
        upper: &Key,
    ) -> Result<Option<KeyValue>, Error> {
        StagedWriteOverlay::new(self.base, &self.staged_writes)
            .last_with_prefix_before_or_at(cf, prefix, upper)
    }

    fn write_many(&self, operations: &[WriteOperation<'_>]) -> Result<(), Error> {
        StagedWriteOverlay::new(self.base, &self.staged_writes).write_many(operations)
    }

    fn approximate_class_bytes(&self, cf: &ColumnFamilyName) -> Result<Option<u64>, Error> {
        self.base.approximate_class_bytes(cf)
    }

    fn column_family_names(&self) -> Option<Vec<String>> {
        self.base.column_family_names()
    }
}

impl<'a, S> StagedWriteOverlay<'a, S> {
    pub(crate) fn new(base: &'a S, staged_writes: &'a RefCell<StagedWriteState>) -> Self {
        Self {
            base,
            staged_writes,
        }
    }

    pub fn stage(&self, operation: OwnedWriteOperation) {
        self.staged_writes.borrow_mut().stage(operation);
    }

    pub fn drain_into(&self, target: &mut Vec<OwnedWriteOperation>) {
        let state = std::mem::take(&mut *self.staged_writes.borrow_mut());
        target.extend(state.into_operations());
    }
}

impl<S> OrderedKvStorage for StagedWriteOverlay<'_, S>
where
    S: OrderedKvStorage,
{
    fn get(&self, cf: &ColumnFamilyName, key: &Key) -> Result<Option<Value>, Error> {
        if self.staged_writes.borrow().is_empty() {
            return self.base.get(cf, key);
        }

        let mut staged_writes = self.staged_writes.borrow_mut();
        if let Some(index) = staged_writes.latest_index(cf, key) {
            let operation = &staged_writes.operations[index];
            if operation.cf() == cf && operation.key() == key {
                match operation {
                    OwnedWriteOperation::Set { value, .. } => {
                        return Ok(Some(value.clone()));
                    }
                    OwnedWriteOperation::Delete { .. } => {
                        return Ok(None);
                    }
                    OwnedWriteOperation::Delta { .. } => {}
                }
            }
        }
        drop(staged_writes);

        let mut value = self.base.get(cf, key)?;
        for operation in self.staged_writes.borrow().operations.iter() {
            if operation.cf() != cf || operation.key() != key {
                continue;
            }
            match operation {
                OwnedWriteOperation::Set { value: set, .. } => value = Some(set.clone()),
                OwnedWriteOperation::Delete { .. } => value = None,
                OwnedWriteOperation::Delta { delta, .. } => {
                    let encoded = delta.encode()?;
                    value = Some(apply_storage_delta(value.as_deref(), &encoded)?);
                }
            }
        }
        Ok(value)
    }

    fn set(&self, cf: &ColumnFamilyName, key: &Key, value: &[u8]) -> Result<(), Error> {
        self.stage(OwnedWriteOperation::Set {
            cf: cf.to_owned(),
            key: key.to_vec(),
            value: value.to_vec(),
        });
        Ok(())
    }

    fn delete(&self, cf: &ColumnFamilyName, key: &Key) -> Result<(), Error> {
        self.stage(OwnedWriteOperation::Delete {
            cf: cf.to_owned(),
            key: key.to_vec(),
        });
        Ok(())
    }

    fn scan_range(
        &self,
        cf: &ColumnFamilyName,
        start: &Key,
        end: &Key,
        visit: &mut ScanVisitor<'_>,
    ) -> Result<(), Error> {
        if self.staged_writes.borrow().is_empty() {
            return self.base.scan_range(cf, start, end, visit);
        }

        let mut values = self.base.range(cf, start, end)?;
        overlay_values(
            &mut values,
            cf,
            |key| key >= start && key < end,
            self.staged_writes,
        )?;
        for (key, value) in values {
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
        if self.staged_writes.borrow().is_empty() {
            return self.base.scan_prefix(cf, prefix, visit);
        }

        let mut values = self.base.prefix(cf, prefix)?;
        overlay_values(
            &mut values,
            cf,
            |key| key.starts_with(prefix),
            self.staged_writes,
        )?;
        for (key, value) in values {
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
        if self.staged_writes.borrow().is_empty() {
            return self.base.scan_prefix_reverse(cf, prefix, visit);
        }

        if self
            .staged_writes
            .borrow()
            .operations
            .iter()
            .any(|operation| {
                operation.cf() == cf
                    && operation.key().starts_with(prefix)
                    && matches!(operation, OwnedWriteOperation::Delta { .. })
            })
        {
            let mut values = self.base.prefix(cf, prefix)?;
            overlay_values(
                &mut values,
                cf,
                |key| key.starts_with(prefix),
                self.staged_writes,
            )?;
            for (key, value) in values.into_iter().rev() {
                visit(&key, &value)?;
            }
            return Ok(());
        }

        let staged = staged_prefix_overlay_desc(cf, prefix, self.staged_writes);
        let mut staged_index = 0;
        self.base
            .scan_prefix_reverse(cf, prefix, &mut |base_key, base_value| {
                while let Some((staged_key, staged_value)) = staged.get(staged_index) {
                    if staged_key.as_slice() <= base_key {
                        break;
                    }
                    if let Some(value) = staged_value {
                        visit(staged_key, value)?;
                    }
                    staged_index += 1;
                }

                if let Some((staged_key, staged_value)) = staged.get(staged_index)
                    && staged_key.as_slice() == base_key
                {
                    if let Some(value) = staged_value {
                        visit(staged_key, value)?;
                    }
                    staged_index += 1;
                    return Ok(());
                }

                visit(base_key, base_value)
            })?;

        while let Some((staged_key, staged_value)) = staged.get(staged_index) {
            if let Some(value) = staged_value {
                visit(staged_key, value)?;
            }
            staged_index += 1;
        }
        Ok(())
    }

    fn last_with_prefix(
        &self,
        cf: &ColumnFamilyName,
        prefix: &Key,
    ) -> Result<Option<KeyValue>, Error> {
        if self.staged_writes.borrow().is_empty() {
            return self.base.last_with_prefix(cf, prefix);
        }

        let mut needs_full_merge = false;
        for operation in self.staged_writes.borrow().operations.iter() {
            if operation.cf() != cf || !operation.key().starts_with(prefix) {
                continue;
            }
            match operation {
                OwnedWriteOperation::Set { .. } => {}
                OwnedWriteOperation::Delete { .. } | OwnedWriteOperation::Delta { .. } => {
                    needs_full_merge = true
                }
            }
        }

        if !needs_full_merge {
            let largest_staged_set = staged_prefix_overlay_desc(cf, prefix, self.staged_writes)
                .into_iter()
                .find_map(|(key, value)| value.map(|value| (key, value)));
            return match (self.base.last_with_prefix(cf, prefix)?, largest_staged_set) {
                (Some(base), Some(staged)) if staged.0 >= base.0 => Ok(Some(staged)),
                (Some(base), _) => Ok(Some(base)),
                (None, staged) => Ok(staged),
            };
        }

        let mut values = self.base.prefix(cf, prefix)?;
        overlay_values(
            &mut values,
            cf,
            |key| key.starts_with(prefix),
            self.staged_writes,
        )?;
        Ok(values.pop())
    }

    fn last_with_prefix_before_or_at(
        &self,
        cf: &ColumnFamilyName,
        prefix: &Key,
        upper: &Key,
    ) -> Result<Option<KeyValue>, Error> {
        if self.staged_writes.borrow().is_empty() {
            return self.base.last_with_prefix_before_or_at(cf, prefix, upper);
        }

        let mut needs_full_merge = false;
        for operation in self.staged_writes.borrow().operations.iter() {
            if operation.cf() != cf
                || !operation.key().starts_with(prefix)
                || operation.key() > upper
            {
                continue;
            }
            match operation {
                OwnedWriteOperation::Set { .. } => {}
                OwnedWriteOperation::Delete { .. } | OwnedWriteOperation::Delta { .. } => {
                    needs_full_merge = true
                }
            }
        }

        if !needs_full_merge {
            let largest_staged_set =
                staged_prefix_overlay_desc_before_or_at(cf, prefix, upper, self.staged_writes)
                    .into_iter()
                    .find_map(|(key, value)| value.map(|value| (key, value)));
            return match (
                self.base.last_with_prefix_before_or_at(cf, prefix, upper)?,
                largest_staged_set,
            ) {
                (Some(base), Some(staged)) if staged.0 >= base.0 => Ok(Some(staged)),
                (Some(base), _) => Ok(Some(base)),
                (None, staged) => Ok(staged),
            };
        }

        let mut values = Vec::new();
        self.base.scan_range(
            cf,
            prefix,
            &exclusive_upper_bound(upper),
            &mut |key, value| {
                if key.starts_with(prefix) {
                    values.push((key.to_vec(), value.to_vec()));
                }
                Ok(())
            },
        )?;
        overlay_values(
            &mut values,
            cf,
            |key| key.starts_with(prefix) && key <= upper,
            self.staged_writes,
        )?;
        Ok(values.pop())
    }

    fn write_many(&self, operations: &[WriteOperation<'_>]) -> Result<(), Error> {
        for operation in operations {
            match operation {
                WriteOperation::Set { cf, key, value } => self.set(cf, key, value)?,
                WriteOperation::Delete { cf, key } => self.delete(cf, key)?,
                WriteOperation::Delta { cf, key, delta } => {
                    self.stage(OwnedWriteOperation::Delta {
                        cf: (*cf).to_owned(),
                        key: (*key).to_vec(),
                        delta: (*delta).clone(),
                    });
                }
            }
        }
        Ok(())
    }
}

fn overlay_values(
    values: &mut Vec<KeyValue>,
    cf: &ColumnFamilyName,
    include: impl Fn(&[u8]) -> bool,
    staged_writes: &RefCell<StagedWriteState>,
) -> Result<(), Error> {
    let mut overlay = values
        .drain(..)
        .map(|(key, value)| (key, Some(value)))
        .collect::<BTreeMap<_, _>>();
    for operation in staged_writes.borrow().operations.iter() {
        if operation.cf() != cf || !include(operation.key()) {
            continue;
        }
        match operation {
            OwnedWriteOperation::Set { key, value, .. } => {
                overlay.insert(key.clone(), Some(value.clone()));
            }
            OwnedWriteOperation::Delete { key, .. } => {
                overlay.insert(key.clone(), None);
            }
            OwnedWriteOperation::Delta { key, delta, .. } => {
                let encoded = delta.encode()?;
                let existing = overlay.get(key).and_then(Option::as_deref);
                overlay.insert(key.clone(), Some(apply_storage_delta(existing, &encoded)?));
            }
        }
    }
    values.extend(
        overlay
            .into_iter()
            .filter_map(|(key, value)| value.map(|value| (key, value))),
    );
    Ok(())
}

fn staged_prefix_overlay_desc(
    cf: &ColumnFamilyName,
    prefix: &Key,
    staged_writes: &RefCell<StagedWriteState>,
) -> Vec<(Vec<u8>, Option<Vec<u8>>)> {
    let mut overlay = BTreeMap::new();
    for operation in staged_writes.borrow().operations.iter() {
        if operation.cf() != cf || !operation.key().starts_with(prefix) {
            continue;
        }
        match operation {
            OwnedWriteOperation::Set { key, value, .. } => {
                overlay.insert(key.clone(), Some(value.clone()));
            }
            OwnedWriteOperation::Delete { key, .. } => {
                overlay.insert(key.clone(), None);
            }
            OwnedWriteOperation::Delta { .. } => {}
        }
    }
    overlay.into_iter().rev().collect()
}

fn staged_prefix_overlay_desc_before_or_at(
    cf: &ColumnFamilyName,
    prefix: &Key,
    upper: &Key,
    staged_writes: &RefCell<StagedWriteState>,
) -> Vec<(Vec<u8>, Option<Vec<u8>>)> {
    let mut overlay = BTreeMap::new();
    for operation in staged_writes.borrow().operations.iter() {
        if operation.cf() != cf || !operation.key().starts_with(prefix) || operation.key() > upper {
            continue;
        }
        match operation {
            OwnedWriteOperation::Set { key, value, .. } => {
                overlay.insert(key.clone(), Some(value.clone()));
            }
            OwnedWriteOperation::Delete { key, .. } => {
                overlay.insert(key.clone(), None);
            }
            OwnedWriteOperation::Delta { .. } => {}
        }
    }
    overlay.into_iter().rev().collect()
}

fn exclusive_upper_bound(key: &[u8]) -> Vec<u8> {
    let mut upper = key.to_vec();
    if let Some(index) = upper.iter().rposition(|byte| *byte != 0xFF) {
        upper[index] += 1;
        upper.truncate(index + 1);
        upper
    } else {
        let mut end = key.to_vec();
        end.push(0);
        end
    }
}

impl<'a> WriteOperation<'a> {
    /// A set/insert operation.
    pub fn set(cf: &'a str, key: &'a Key, value: &'a [u8]) -> Self {
        Self::Set { cf, key, value }
    }

    /// A delete operation.
    pub fn delete(cf: &'a str, key: &'a Key) -> Self {
        Self::Delete { cf, key }
    }

    /// A merge-delta operation.
    pub fn delta(cf: &'a str, key: &'a Key, delta: &'a StorageDelta) -> Self {
        Self::Delta { cf, key, delta }
    }
}

/// Errors from the storage layer: unknown column families, malformed
/// keys/deltas/windows, and passed-through backend errors (records, window
/// codec, RocksDB, OPFS).
#[derive(Debug, Error)]
pub enum Error {
    #[error("column family not found: {0}")]
    ColumnFamilyNotFound(String),
    #[error("invalid storage layout: {0}")]
    InvalidStorageLayout(String),
    #[error("invalid storage key: {0}")]
    InvalidStorageKey(String),
    #[error("invalid storage delta: {0}")]
    InvalidStorageDelta(String),
    #[error("invalid window record: {0}")]
    InvalidWindowRecord(String),
    #[error("window codec error: {0}")]
    WindowCodec(#[from] crate::window_codec::WindowCodecError),
    #[error("record error: {0}")]
    Record(#[from] crate::records::Error),
    #[cfg(feature = "rocksdb")]
    #[error(transparent)]
    RocksDb(#[from] ::rocksdb::Error),
    #[error(transparent)]
    Opfs(#[from] opfs_btree::BTreeError),
}

#[cfg(test)]
pub(crate) mod conformance {
    use super::*;

    pub(crate) fn persistence_order_and_batch_atomicity<S>(storage: S)
    where
        S: OrderedKvStorage,
    {
        storage.set("records", b"user:2", b"two").unwrap();
        storage.set("records", b"user:1", b"one").unwrap();
        storage.set("records", b"user:10", b"ten").unwrap();
        storage.set("records", &[0xff, 0x00], b"ff-zero").unwrap();
        storage.set("records", &[0xff, 0x01], b"ff-one").unwrap();

        assert_eq!(
            storage.range("records", b"user:", b"user;").unwrap(),
            vec![
                (b"user:1".to_vec(), b"one".to_vec()),
                (b"user:10".to_vec(), b"ten".to_vec()),
                (b"user:2".to_vec(), b"two".to_vec()),
            ]
        );
        assert_eq!(
            storage.prefix("records", &[0xff]).unwrap(),
            vec![
                (vec![0xff, 0x00], b"ff-zero".to_vec()),
                (vec![0xff, 0x01], b"ff-one".to_vec()),
            ]
        );

        let error = storage
            .write_many(&[
                WriteOperation::set("records", b"user:3", b"three"),
                WriteOperation::set("missing", b"user:4", b"four"),
            ])
            .unwrap_err();
        assert!(matches!(error, Error::ColumnFamilyNotFound(_)));
        assert_eq!(storage.get("records", b"user:3").unwrap(), None);

        storage
            .write_many(&[
                WriteOperation::set("records", b"user:3", b"three"),
                WriteOperation::delete("records", b"user:2"),
            ])
            .unwrap();
        assert_eq!(
            storage.prefix("records", b"user:").unwrap(),
            vec![
                (b"user:1".to_vec(), b"one".to_vec()),
                (b"user:10".to_vec(), b"ten".to_vec()),
                (b"user:3".to_vec(), b"three".to_vec()),
            ]
        );
    }

    pub(crate) fn reopen_preserves_data_and_adds_families<S>(storage: S)
    where
        S: ReopenableStorage,
    {
        storage.set("records", b"1", b"record").unwrap();

        let storage = storage.reopen(&["records", "indices"]).unwrap();
        storage.set("indices", b"name:record", b"1").unwrap();

        assert_eq!(
            storage.get("records", b"1").unwrap(),
            Some(b"record".to_vec())
        );
        assert_eq!(
            storage.get("indices", b"name:record").unwrap(),
            Some(b"1".to_vec())
        );
    }

    pub(crate) fn delta_append_current_winner_observes_merged_state<S>(storage: S)
    where
        S: OrderedKvStorage,
    {
        fn record(time: u64, node: u8, payload: &[u8]) -> Vec<u8> {
            let mut bytes = Vec::new();
            bytes.extend(time.to_le_bytes());
            bytes.extend([node; 16]);
            bytes.extend(payload);
            bytes
        }

        fn delta(time: u64, node: u8, parents: Vec<(u64, u8)>, record: Vec<u8>) -> StorageDelta {
            StorageDelta::current_winner(CurrentWinnerDelta {
                tx_time: time,
                tx_node_uuid: [node; 16],
                parents: parents
                    .into_iter()
                    .map(|(time, node)| (time, [node; 16]))
                    .collect(),
                tx_time_offset: 0,
                tx_node_uuid_offset: 8,
                record,
            })
            .unwrap()
        }

        let older = record(10, 1, b"older");
        let newer = record(20, 1, b"newer");
        let child = record(11, 2, b"child");
        let loser = record(9, 9, b"loser");

        storage
            .write_many(&[WriteOperation::delta(
                "records",
                b"row",
                &delta(10, 1, Vec::new(), older.clone()),
            )])
            .unwrap();
        assert_eq!(storage.get("records", b"row").unwrap(), Some(older.clone()));

        storage
            .write_many(&[WriteOperation::delta(
                "records",
                b"row",
                &delta(20, 1, Vec::new(), newer.clone()),
            )])
            .unwrap();
        assert_eq!(storage.get("records", b"row").unwrap(), Some(newer.clone()));

        storage
            .write_many(&[WriteOperation::delta(
                "records",
                b"row",
                &delta(11, 2, vec![(20, 1)], child.clone()),
            )])
            .unwrap();
        assert_eq!(storage.get("records", b"row").unwrap(), Some(child.clone()));

        storage
            .write_many(&[WriteOperation::delta(
                "records",
                b"row",
                &delta(9, 9, Vec::new(), loser),
            )])
            .unwrap();
        assert_eq!(storage.get("records", b"row").unwrap(), Some(child));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::{MeteredStorage, StorageReadMetrics};
    use crate::records::{EnumSchema, Value, ValueType};
    use std::cell::Cell;

    fn reverse_prefix_values<S: OrderedKvStorage>(
        storage: &S,
        cf: &str,
        prefix: &[u8],
    ) -> Result<Vec<KeyValue>, Error> {
        let mut values = Vec::new();
        storage.scan_prefix_reverse(cf, prefix, &mut |key, value| {
            values.push((key.to_vec(), value.to_vec()));
            Ok(())
        })?;
        Ok(values)
    }

    fn window_key_descriptor() -> RecordDescriptor {
        RecordDescriptor::new([
            ("row", ValueType::Bytes),
            ("time", ValueType::U64),
            ("node", ValueType::U64),
        ])
    }

    fn window_value_descriptor() -> RecordDescriptor {
        RecordDescriptor::new([("title", ValueType::String), ("payload", ValueType::Bytes)])
    }

    fn window_key(row: u8, time: u64, node: u64) -> Vec<u8> {
        encode_key_record(vec![
            Value::Bytes(vec![row; 2]),
            Value::U64(time),
            Value::U64(node),
        ])
        .unwrap()
    }

    fn window_value(descriptor: RecordDescriptor, title: &str, payload: &[u8]) -> Vec<u8> {
        descriptor
            .create(&[
                Value::String(title.to_owned()),
                Value::Bytes(payload.to_vec()),
            ])
            .unwrap()
    }

    fn complex_record_descriptor_and_values() -> (RecordDescriptor, Vec<Value>) {
        let status = EnumSchema::new("status", ["draft", "ready", "done"]).unwrap();
        let row = uuid::Uuid::from_bytes([0x54; 16]);
        let ref_row = uuid::Uuid::from_bytes([0x75; 16]);
        (
            RecordDescriptor::new([
                ("u8", ValueType::U8),
                ("u16", ValueType::U16),
                ("u32", ValueType::U32),
                ("u64_max", ValueType::U64),
                ("f64", ValueType::F64),
                ("bool", ValueType::Bool),
                ("text", ValueType::String),
                ("bytes", ValueType::Bytes),
                ("uuid", ValueType::Uuid),
                ("enum", ValueType::Enum(status)),
                (
                    "nullable_tuple",
                    ValueType::Nullable(Box::new(ValueType::Tuple(vec![
                        ValueType::Uuid,
                        ValueType::U64,
                    ]))),
                ),
                (
                    "nested_array",
                    ValueType::Array(Box::new(ValueType::Array(Box::new(ValueType::U8)))),
                ),
            ]),
            vec![
                Value::U8(u8::MAX),
                Value::U16(u16::MAX),
                Value::U32(u32::MAX),
                Value::U64(u64::MAX),
                Value::F64(0.125),
                Value::Bool(false),
                Value::String("stored value".to_owned()),
                Value::Bytes(vec![9, 8, 7, 6]),
                Value::Uuid(row),
                Value::Enum(1),
                Value::Nullable(Some(Box::new(Value::Tuple(vec![
                    Value::Uuid(ref_row),
                    Value::U64(u64::MAX - 7),
                ])))),
                Value::Array(vec![
                    Value::Array(vec![Value::U8(1), Value::U8(2)]),
                    Value::Array(vec![]),
                    Value::Array(vec![Value::U8(3)]),
                ]),
            ],
        )
    }

    fn seed_window_records<S: OrderedKvStorage>(
        storage: &S,
        value_descriptor: RecordDescriptor,
        count: u64,
    ) -> Vec<KeyValue> {
        (0..count)
            .map(|idx| {
                let key = window_key(1, 100 + idx, 7);
                let value = window_value(
                    value_descriptor,
                    &format!("title-{idx}"),
                    &[idx as u8, (idx + 1) as u8],
                );
                storage.set("jazz_docs_history", &key, &value).unwrap();
                (key, value)
            })
            .collect::<Vec<_>>()
    }

    fn seed_window_records_from<S: OrderedKvStorage>(
        storage: &S,
        value_descriptor: RecordDescriptor,
        start: u64,
        count: u64,
    ) -> Vec<KeyValue> {
        (start..start + count)
            .map(|idx| {
                let key = window_key(1, 100 + idx, 7);
                let value = window_value(
                    value_descriptor,
                    &format!("title-{idx}"),
                    &[idx as u8, (idx + 1) as u8],
                );
                storage.set("jazz_docs_history", &key, &value).unwrap();
                (key, value)
            })
            .collect::<Vec<_>>()
    }

    fn window_store<'a, S: OrderedKvStorage>(
        storage: &'a S,
        value_descriptor: &'a RecordDescriptor,
    ) -> RecordStore<'a, S> {
        RecordStore::new_windowed(
            storage,
            "jazz_docs_history",
            window_key_descriptor(),
            value_descriptor,
        )
    }

    #[test]
    fn record_store_round_trips_exhaustive_record_descriptor() {
        let storage = MemoryStorage::new(&["records"]);
        let (descriptor, values) = complex_record_descriptor_and_values();
        let raw = descriptor.create(&values).unwrap();
        storage.set("records", b"row:1", &raw).unwrap();
        let store = RecordStore::new(&storage, "records", &descriptor);

        let record = store.get(b"row:1").unwrap().unwrap();
        assert_eq!(record.to_values().unwrap(), values);
        assert_eq!(store.get_raw(b"row:1").unwrap().unwrap(), raw);

        let prefix = store.prefix(b"row:").unwrap();
        assert_eq!(prefix, vec![(b"row:1".to_vec(), raw.clone())]);
        let ranged = store.range(b"row:", b"row;").unwrap();
        assert_eq!(ranged, vec![(b"row:1".to_vec(), raw)]);
    }

    #[test]
    fn class_layout_keeps_logical_keys_isolated_inside_shared_physical_cf() {
        let physical_cfs = StorageLayout::jazz_class_v1().physical_column_families([
            "jazz_albums_history",
            "jazz_tracks_history",
            "jazz_albums_register",
        ]);
        let refs = physical_cfs.iter().map(String::as_str).collect::<Vec<_>>();
        let storage =
            LayoutStorage::new(MemoryStorage::new(&refs), StorageLayout::jazz_class_v1()).unwrap();

        storage
            .set("jazz_albums_history", b"row:1", b"album-one")
            .unwrap();
        storage
            .set("jazz_albums_history", b"row:3", b"album-three")
            .unwrap();
        storage
            .set("jazz_tracks_history", b"row:2", b"track-two")
            .unwrap();
        storage
            .set("jazz_albums_register", b"row:2", b"album-register")
            .unwrap();

        assert_eq!(
            storage.prefix("jazz_albums_history", b"row:").unwrap(),
            vec![
                (b"row:1".to_vec(), b"album-one".to_vec()),
                (b"row:3".to_vec(), b"album-three".to_vec()),
            ]
        );
        assert_eq!(
            reverse_prefix_values(&storage, "jazz_albums_history", b"row:").unwrap(),
            vec![
                (b"row:3".to_vec(), b"album-three".to_vec()),
                (b"row:1".to_vec(), b"album-one".to_vec()),
            ]
        );
        assert_eq!(
            storage
                .last_with_prefix("jazz_albums_history", b"row:")
                .unwrap(),
            Some((b"row:3".to_vec(), b"album-three".to_vec()))
        );
        assert_eq!(
            storage
                .range("jazz_albums_history", b"row:1", b"row:4")
                .unwrap(),
            vec![
                (b"row:1".to_vec(), b"album-one".to_vec()),
                (b"row:3".to_vec(), b"album-three".to_vec()),
            ]
        );
    }

    #[test]
    fn class_layout_isolates_every_jazz_physical_class() {
        let logical_cfs = [
            ("jazz_albums_history", "jazz_tracks_history"),
            ("jazz_albums_register", "jazz_tracks_register"),
            ("jazz_albums_global_current", "jazz_tracks_global_current"),
            (
                "jazz_albums_register_global_current",
                "jazz_tracks_register_global_current",
            ),
            ("jazz_albums_ahead_current", "jazz_tracks_ahead_current"),
            (
                "jazz_albums_register_ahead_current",
                "jazz_tracks_register_ahead_current",
            ),
            ("jazz_global_changes", "jazz_known_state_facts"),
            ("indices", "jazz_content_extents"),
            ("jazz_content_meta", "jazz_content_checkpoints"),
            ("jazz_nodes", "jazz_transactions"),
        ];
        let all_logical = logical_cfs
            .iter()
            .flat_map(|(left, right)| [*left, *right])
            .collect::<Vec<_>>();
        let layout = StorageLayout::jazz_class_v1_for(all_logical.iter().copied());
        let physical_cfs = layout.physical_column_families(all_logical.iter().copied());
        let refs = physical_cfs.iter().map(String::as_str).collect::<Vec<_>>();
        let storage = LayoutStorage::new(MemoryStorage::new(&refs), layout).unwrap();

        for (left, right) in logical_cfs {
            storage.set(left, b"k:1", left.as_bytes()).unwrap();
            storage.set(right, b"k:2", right.as_bytes()).unwrap();

            assert_eq!(
                storage.prefix(left, b"k:").unwrap(),
                vec![(b"k:1".to_vec(), left.as_bytes().to_vec())],
                "{left} must not read rows from {right}"
            );
            assert_eq!(
                reverse_prefix_values(&storage, right, b"k:").unwrap(),
                vec![(b"k:2".to_vec(), right.as_bytes().to_vec())],
                "{right} reverse scan must not read rows from {left}"
            );
            assert_eq!(
                storage.last_with_prefix(left, b"k:").unwrap(),
                Some((b"k:1".to_vec(), left.as_bytes().to_vec())),
                "{left} last_with_prefix must stay within its logical prefix"
            );
        }
    }

    #[test]
    fn class_layout_rejects_missing_marker_with_legacy_mapped_families() {
        let storage = MemoryStorage::new(&["__groove_class_meta", "jazz_albums_history"]);
        assert!(matches!(
            LayoutStorage::new(storage, StorageLayout::jazz_class_v1()),
            Err(Error::InvalidStorageLayout(_))
        ));
    }

    #[test]
    fn class_layout_accepts_truly_empty_store_and_writes_marker() {
        let storage = MemoryStorage::new(&["__groove_class_meta", "__groove_class_history"]);
        let storage = LayoutStorage::new(storage, StorageLayout::jazz_class_v1()).unwrap();
        assert_eq!(
            storage
                .inner
                .get("__groove_class_meta", CLASS_LAYOUT_MARKER_KEY)
                .unwrap(),
            Some(CLASS_LAYOUT_MARKER_VALUE.to_vec())
        );
    }

    #[test]
    fn class_layout_preserves_missing_logical_cf_errors_when_declared_set_is_known() {
        let layout = StorageLayout::jazz_class_v1_for(["jazz_albums_history"]);
        let physical_cfs = layout.physical_column_families(["jazz_albums_history"]);
        let refs = physical_cfs.iter().map(String::as_str).collect::<Vec<_>>();
        let storage = LayoutStorage::new(MemoryStorage::new(&refs), layout).unwrap();

        storage
            .set("jazz_albums_history", b"row:1", b"album-one")
            .unwrap();
        assert!(matches!(
            storage.get("jazz_tracks_history", b"row:1"),
            Err(Error::ColumnFamilyNotFound(_))
        ));
    }

    #[test]
    fn get_set_and_delete_values() {
        let temp_dir = tempfile::tempdir().unwrap();
        let storage = RocksDbStorage::open(temp_dir.path(), &["records"]).unwrap();

        storage.set("records", b"a", b"one").unwrap();
        assert_eq!(storage.get("records", b"a").unwrap(), Some(b"one".to_vec()));

        storage.delete("records", b"a").unwrap();
        assert_eq!(storage.get("records", b"a").unwrap(), None);
    }

    #[test]
    fn wal_no_sync_durability_mode_keeps_wal_writes_enabled() {
        let temp_dir = tempfile::tempdir().unwrap();
        let storage = RocksDbStorage::open_with_durability(
            temp_dir.path(),
            &["records"],
            Durability::WalNoSync,
        )
        .unwrap();

        storage.set("records", b"a", b"one").unwrap();

        assert_eq!(storage.get("records", b"a").unwrap(), Some(b"one".to_vec()));
    }

    #[test]
    fn rocksdb_approximate_class_bytes_reports_populated_family() {
        let temp_dir = tempfile::tempdir().unwrap();
        let storage = RocksDbStorage::open(temp_dir.path(), &["records"]).unwrap();

        storage.set("records", b"a", b"one").unwrap();

        assert!(
            storage
                .approximate_class_bytes("records")
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn range_returns_ordered_values_between_start_and_end() {
        let temp_dir = tempfile::tempdir().unwrap();
        let storage = RocksDbStorage::open(temp_dir.path(), &["records"]).unwrap();

        storage.set("records", b"a", b"one").unwrap();
        storage.set("records", b"b", b"two").unwrap();
        storage.set("records", b"c", b"three").unwrap();

        assert_eq!(
            storage.range("records", b"a", b"c").unwrap(),
            vec![
                (b"a".to_vec(), b"one".to_vec()),
                (b"b".to_vec(), b"two".to_vec())
            ]
        );
    }

    #[test]
    fn prefix_returns_ordered_values_with_matching_prefix() {
        let temp_dir = tempfile::tempdir().unwrap();
        let storage = RocksDbStorage::open(temp_dir.path(), &["records"]).unwrap();

        storage.set("records", b"user:1", b"a").unwrap();
        storage.set("records", b"user:2", b"b").unwrap();
        storage.set("records", b"view:1", b"c").unwrap();

        assert_eq!(
            storage.prefix("records", b"user:").unwrap(),
            vec![
                (b"user:1".to_vec(), b"a".to_vec()),
                (b"user:2".to_vec(), b"b".to_vec())
            ]
        );
    }

    #[test]
    fn prefix_handles_prefixes_without_a_finite_upper_bound() {
        let temp_dir = tempfile::tempdir().unwrap();
        let storage = RocksDbStorage::open(temp_dir.path(), &["records"]).unwrap();

        storage.set("records", &[0xfe], b"before").unwrap();
        storage.set("records", &[0xff, 0x00], b"a").unwrap();
        storage.set("records", &[0xff, 0x01], b"b").unwrap();

        assert_eq!(
            storage.prefix("records", &[0xff]).unwrap(),
            vec![
                (vec![0xff, 0x00], b"a".to_vec()),
                (vec![0xff, 0x01], b"b".to_vec())
            ]
        );
    }

    #[test]
    fn direct_operations_report_missing_column_families() {
        let temp_dir = tempfile::tempdir().unwrap();
        let storage = RocksDbStorage::open(temp_dir.path(), &["records"]).unwrap();

        assert!(matches!(
            storage.get("missing", b"a"),
            Err(Error::ColumnFamilyNotFound(cf)) if cf == "missing"
        ));
        assert!(matches!(
            storage.set("missing", b"a", b"one"),
            Err(Error::ColumnFamilyNotFound(cf)) if cf == "missing"
        ));
        assert!(matches!(
            storage.delete("missing", b"a"),
            Err(Error::ColumnFamilyNotFound(cf)) if cf == "missing"
        ));
        assert!(matches!(
            storage.range("missing", b"a", b"z"),
            Err(Error::ColumnFamilyNotFound(cf)) if cf == "missing"
        ));
        assert!(matches!(
            storage.prefix("missing", b"a"),
            Err(Error::ColumnFamilyNotFound(cf)) if cf == "missing"
        ));
        assert!(matches!(
            storage.scan_range("missing", b"a", b"z", &mut |_, _| Ok(())),
            Err(Error::ColumnFamilyNotFound(cf)) if cf == "missing"
        ));
        assert!(matches!(
            storage.scan_prefix("missing", b"a", &mut |_, _| Ok(())),
            Err(Error::ColumnFamilyNotFound(cf)) if cf == "missing"
        ));
    }

    #[test]
    fn scans_visit_ordered_values_without_materializing_in_storage_api() {
        let temp_dir = tempfile::tempdir().unwrap();
        let storage = RocksDbStorage::open(temp_dir.path(), &["records"]).unwrap();

        storage.set("records", b"a", b"one").unwrap();
        storage.set("records", b"b", b"two").unwrap();
        storage.set("records", b"c", b"three").unwrap();

        let mut visited = Vec::new();
        storage
            .scan_range("records", b"a", b"c", &mut |key, value| {
                visited.push((key.to_vec(), value.to_vec()));
                Ok(())
            })
            .unwrap();

        assert_eq!(
            visited,
            vec![
                (b"a".to_vec(), b"one".to_vec()),
                (b"b".to_vec(), b"two".to_vec())
            ]
        );
    }

    #[test]
    fn write_many_writes_all_operations_atomically() {
        let temp_dir = tempfile::tempdir().unwrap();
        let storage = RocksDbStorage::open(temp_dir.path(), &["records", "indices"]).unwrap();

        storage
            .write_many(&[
                WriteOperation::set("records", b"1", b"record"),
                WriteOperation::set("indices", b"name:record", b"1"),
            ])
            .unwrap();

        assert_eq!(
            storage.get("records", b"1").unwrap(),
            Some(b"record".to_vec())
        );
        assert_eq!(
            storage.get("indices", b"name:record").unwrap(),
            Some(b"1".to_vec())
        );
    }

    #[test]
    fn staged_overlay_reads_staged_sets_and_deletes_before_base_storage() {
        let storage = MemoryStorage::new(&["indices"]);
        storage.set("indices", b"a", b"base-a").unwrap();
        storage.set("indices", b"b", b"base-b").unwrap();
        let staged = RefCell::new(StagedWriteState::from(vec![
            OwnedWriteOperation::Set {
                cf: "indices".to_owned(),
                key: b"a".to_vec(),
                value: b"staged-a".to_vec(),
            },
            OwnedWriteOperation::Delete {
                cf: "indices".to_owned(),
                key: b"b".to_vec(),
            },
            OwnedWriteOperation::Set {
                cf: "indices".to_owned(),
                key: b"c".to_vec(),
                value: b"staged-c".to_vec(),
            },
        ]));
        let overlay = StagedWriteOverlay::new(&storage, &staged);

        assert_eq!(
            overlay.get("indices", b"a").unwrap(),
            Some(b"staged-a".to_vec())
        );
        assert_eq!(overlay.get("indices", b"b").unwrap(), None);
        assert_eq!(
            overlay.get("indices", b"c").unwrap(),
            Some(b"staged-c".to_vec())
        );
        assert_eq!(
            overlay.prefix("indices", b"").unwrap(),
            vec![
                (b"a".to_vec(), b"staged-a".to_vec()),
                (b"c".to_vec(), b"staged-c".to_vec()),
            ]
        );
        assert_eq!(
            storage.get("indices", b"a").unwrap(),
            Some(b"base-a".to_vec())
        );
    }

    #[test]
    fn storage_transaction_reads_own_writes_and_commits_atomically() {
        let storage = MemoryStorage::new(&["records"]);
        storage.set("records", b"a", b"base-a").unwrap();
        storage.set("records", b"b", b"base-b").unwrap();

        let txn = storage.begin_txn();
        txn.set("records", b"a", b"txn-a").unwrap();
        txn.delete("records", b"b").unwrap();
        txn.set("records", b"c", b"txn-c").unwrap();

        assert_eq!(txn.get("records", b"a").unwrap(), Some(b"txn-a".to_vec()));
        assert_eq!(txn.get("records", b"b").unwrap(), None);
        assert_eq!(txn.get("records", b"c").unwrap(), Some(b"txn-c".to_vec()));
        assert_eq!(
            txn.prefix("records", b"").unwrap(),
            vec![
                (b"a".to_vec(), b"txn-a".to_vec()),
                (b"c".to_vec(), b"txn-c".to_vec()),
            ]
        );

        assert_eq!(
            storage.get("records", b"a").unwrap(),
            Some(b"base-a".to_vec())
        );
        assert_eq!(
            storage.get("records", b"b").unwrap(),
            Some(b"base-b".to_vec())
        );
        assert_eq!(storage.get("records", b"c").unwrap(), None);

        txn.commit().unwrap();

        assert_eq!(
            storage.get("records", b"a").unwrap(),
            Some(b"txn-a".to_vec())
        );
        assert_eq!(storage.get("records", b"b").unwrap(), None);
        assert_eq!(
            storage.get("records", b"c").unwrap(),
            Some(b"txn-c".to_vec())
        );
    }

    #[test]
    fn write_many_fails_without_writing_when_column_family_is_missing() {
        let temp_dir = tempfile::tempdir().unwrap();
        let storage = RocksDbStorage::open(temp_dir.path(), &["records"]).unwrap();

        let error = storage
            .write_many(&[
                WriteOperation::set("records", b"1", b"record"),
                WriteOperation::set("missing", b"2", b"nope"),
            ])
            .unwrap_err();

        assert!(matches!(error, Error::ColumnFamilyNotFound(_)));
        assert_eq!(storage.get("records", b"1").unwrap(), None);
    }

    #[test]
    fn write_many_can_mix_sets_and_deletes_atomically() {
        let temp_dir = tempfile::tempdir().unwrap();
        let storage = RocksDbStorage::open(temp_dir.path(), &["records"]).unwrap();

        storage.set("records", b"old", b"value").unwrap();
        storage
            .write_many(&[
                WriteOperation::set("records", b"new", b"value"),
                WriteOperation::delete("records", b"old"),
            ])
            .unwrap();

        assert_eq!(
            storage.get("records", b"new").unwrap(),
            Some(b"value".to_vec())
        );
        assert_eq!(storage.get("records", b"old").unwrap(), None);
    }

    #[test]
    fn memory_storage_orders_scans_and_errors_on_missing_column_families() {
        let storage = MemoryStorage::new(&["records"]);
        storage.set("records", b"b", b"two").unwrap();
        storage.set("records", b"a", b"one").unwrap();
        storage.set("records", b"aa", b"one-one").unwrap();

        assert!(matches!(
            storage.get("missing", b"a"),
            Err(Error::ColumnFamilyNotFound(_))
        ));
        assert!(matches!(
            storage.set("missing", b"a", b"one"),
            Err(Error::ColumnFamilyNotFound(_))
        ));

        let mut range = Vec::new();
        storage
            .scan_range("records", b"a", b"b", &mut |key, value| {
                range.push((key.to_vec(), value.to_vec()));
                Ok(())
            })
            .unwrap();
        assert_eq!(
            range,
            vec![
                (b"a".to_vec(), b"one".to_vec()),
                (b"aa".to_vec(), b"one-one".to_vec())
            ]
        );

        let mut prefix = Vec::new();
        storage
            .scan_prefix("records", b"a", &mut |key, value| {
                prefix.push((key.to_vec(), value.to_vec()));
                Ok(())
            })
            .unwrap();
        assert_eq!(
            prefix,
            vec![
                (b"a".to_vec(), b"one".to_vec()),
                (b"aa".to_vec(), b"one-one".to_vec())
            ]
        );
    }

    #[test]
    fn staged_overlay_reverse_prefix_scans_match_trait_default() {
        struct DefaultReverse<'a, S>(&'a StagedWriteOverlay<'a, S>);

        impl<S> OrderedKvStorage for DefaultReverse<'_, S>
        where
            S: OrderedKvStorage,
        {
            fn get(&self, cf: &ColumnFamilyName, key: &Key) -> Result<Option<Vec<u8>>, Error> {
                self.0.get(cf, key)
            }

            fn set(&self, cf: &ColumnFamilyName, key: &Key, value: &[u8]) -> Result<(), Error> {
                self.0.set(cf, key, value)
            }

            fn delete(&self, cf: &ColumnFamilyName, key: &Key) -> Result<(), Error> {
                self.0.delete(cf, key)
            }

            fn scan_range(
                &self,
                cf: &ColumnFamilyName,
                start: &Key,
                end: &Key,
                visit: &mut ScanVisitor<'_>,
            ) -> Result<(), Error> {
                self.0.scan_range(cf, start, end, visit)
            }

            fn scan_prefix(
                &self,
                cf: &ColumnFamilyName,
                prefix: &Key,
                visit: &mut ScanVisitor<'_>,
            ) -> Result<(), Error> {
                self.0.scan_prefix(cf, prefix, visit)
            }

            fn write_many(&self, operations: &[WriteOperation<'_>]) -> Result<(), Error> {
                self.0.write_many(operations)
            }
        }

        fn assert_case(
            name: &str,
            base_rows: &[(&[u8], &[u8])],
            staged_rows: Vec<OwnedWriteOperation>,
            prefix: &[u8],
            expected: Vec<KeyValue>,
        ) {
            let storage = MemoryStorage::new(&["indices"]);
            for (key, value) in base_rows {
                storage.set("indices", key, value).unwrap();
            }
            storage.set("indices", b"view:1", b"base-view").unwrap();
            let staged = RefCell::new(StagedWriteState::from(staged_rows));
            let overlay = StagedWriteOverlay::new(&storage, &staged);
            let default_reverse = DefaultReverse(&overlay);

            let mut optimized = Vec::new();
            overlay
                .scan_prefix_reverse("indices", prefix, &mut |key, value| {
                    optimized.push((key.to_vec(), value.to_vec()));
                    Ok(())
                })
                .unwrap();

            let mut defaulted = Vec::new();
            default_reverse
                .scan_prefix_reverse("indices", prefix, &mut |key, value| {
                    defaulted.push((key.to_vec(), value.to_vec()));
                    Ok(())
                })
                .unwrap();

            assert_eq!(optimized, defaulted, "{name}");
            assert_eq!(optimized, expected, "{name}");
            assert_eq!(
                overlay.last_with_prefix("indices", prefix).unwrap(),
                default_reverse.last_with_prefix("indices", prefix).unwrap(),
                "{name}"
            );
        }

        assert_case(
            "mixed staged overrides and deletes",
            &[
                (b"user:1", b"base-1"),
                (b"user:2", b"base-2"),
                (b"user:4", b"base-4"),
            ],
            vec![
                OwnedWriteOperation::Set {
                    cf: "indices".to_owned(),
                    key: b"user:2".to_vec(),
                    value: b"staged-2".to_vec(),
                },
                OwnedWriteOperation::Delete {
                    cf: "indices".to_owned(),
                    key: b"user:4".to_vec(),
                },
                OwnedWriteOperation::Set {
                    cf: "indices".to_owned(),
                    key: b"user:3".to_vec(),
                    value: b"staged-3".to_vec(),
                },
                OwnedWriteOperation::Set {
                    cf: "indices".to_owned(),
                    key: b"view:2".to_vec(),
                    value: b"staged-view".to_vec(),
                },
            ],
            b"user:",
            vec![
                (b"user:3".to_vec(), b"staged-3".to_vec()),
                (b"user:2".to_vec(), b"staged-2".to_vec()),
                (b"user:1".to_vec(), b"base-1".to_vec()),
            ],
        );

        assert_case(
            "staged delete of base last key",
            &[
                (b"user:1", b"base-1"),
                (b"user:2", b"base-2"),
                (b"user:4", b"base-4"),
            ],
            vec![OwnedWriteOperation::Delete {
                cf: "indices".to_owned(),
                key: b"user:4".to_vec(),
            }],
            b"user:",
            vec![
                (b"user:2".to_vec(), b"base-2".to_vec()),
                (b"user:1".to_vec(), b"base-1".to_vec()),
            ],
        );

        assert_case(
            "empty staged buffer",
            &[(b"user:1", b"base-1"), (b"user:2", b"base-2")],
            Vec::new(),
            b"user:",
            vec![
                (b"user:2".to_vec(), b"base-2".to_vec()),
                (b"user:1".to_vec(), b"base-1".to_vec()),
            ],
        );

        assert_case(
            "staged-only prefix",
            &[(b"user:1", b"base-1")],
            vec![OwnedWriteOperation::Set {
                cf: "indices".to_owned(),
                key: b"team:1".to_vec(),
                value: b"staged-team".to_vec(),
            }],
            b"team:",
            vec![(b"team:1".to_vec(), b"staged-team".to_vec())],
        );

        assert_case(
            "base empty for prefix",
            &[(b"view:1", b"base-view")],
            vec![
                OwnedWriteOperation::Set {
                    cf: "indices".to_owned(),
                    key: b"user:1".to_vec(),
                    value: b"staged-1".to_vec(),
                },
                OwnedWriteOperation::Set {
                    cf: "indices".to_owned(),
                    key: b"user:3".to_vec(),
                    value: b"staged-3".to_vec(),
                },
            ],
            b"user:",
            vec![
                (b"user:3".to_vec(), b"staged-3".to_vec()),
                (b"user:1".to_vec(), b"staged-1".to_vec()),
            ],
        );
    }

    #[test]
    fn staged_overlay_last_with_prefix_no_delete_uses_one_base_seek() {
        struct CountingStorage<S> {
            inner: S,
            prefix_scans: Cell<usize>,
            reverse_prefix_scans: Cell<usize>,
            last_with_prefix_calls: Cell<usize>,
        }

        impl<S> CountingStorage<S> {
            fn new(inner: S) -> Self {
                Self {
                    inner,
                    prefix_scans: Cell::new(0),
                    reverse_prefix_scans: Cell::new(0),
                    last_with_prefix_calls: Cell::new(0),
                }
            }
        }

        impl<S> OrderedKvStorage for CountingStorage<S>
        where
            S: OrderedKvStorage,
        {
            fn get(&self, cf: &ColumnFamilyName, key: &Key) -> Result<Option<Vec<u8>>, Error> {
                self.inner.get(cf, key)
            }

            fn set(&self, cf: &ColumnFamilyName, key: &Key, value: &[u8]) -> Result<(), Error> {
                self.inner.set(cf, key, value)
            }

            fn delete(&self, cf: &ColumnFamilyName, key: &Key) -> Result<(), Error> {
                self.inner.delete(cf, key)
            }

            fn scan_range(
                &self,
                cf: &ColumnFamilyName,
                start: &Key,
                end: &Key,
                visit: &mut ScanVisitor<'_>,
            ) -> Result<(), Error> {
                self.inner.scan_range(cf, start, end, visit)
            }

            fn scan_prefix(
                &self,
                cf: &ColumnFamilyName,
                prefix: &Key,
                visit: &mut ScanVisitor<'_>,
            ) -> Result<(), Error> {
                self.prefix_scans.set(self.prefix_scans.get() + 1);
                self.inner.scan_prefix(cf, prefix, visit)
            }

            fn scan_prefix_reverse(
                &self,
                cf: &ColumnFamilyName,
                prefix: &Key,
                visit: &mut ScanVisitor<'_>,
            ) -> Result<(), Error> {
                self.reverse_prefix_scans
                    .set(self.reverse_prefix_scans.get() + 1);
                self.inner.scan_prefix_reverse(cf, prefix, visit)
            }

            fn last_with_prefix(
                &self,
                cf: &ColumnFamilyName,
                prefix: &Key,
            ) -> Result<Option<KeyValue>, Error> {
                self.last_with_prefix_calls
                    .set(self.last_with_prefix_calls.get() + 1);
                self.inner.last_with_prefix(cf, prefix)
            }

            fn write_many(&self, operations: &[WriteOperation<'_>]) -> Result<(), Error> {
                self.inner.write_many(operations)
            }
        }

        let storage = CountingStorage::new(MemoryStorage::new(&["indices"]));
        storage.set("indices", b"user:1", b"base-1").unwrap();
        storage.set("indices", b"user:2", b"base-2").unwrap();
        let staged = RefCell::new(StagedWriteState::from(vec![
            OwnedWriteOperation::Set {
                cf: "indices".to_owned(),
                key: b"user:3".to_vec(),
                value: b"staged-3".to_vec(),
            },
            OwnedWriteOperation::Set {
                cf: "indices".to_owned(),
                key: b"user:0".to_vec(),
                value: b"staged-0".to_vec(),
            },
        ]));
        let overlay = StagedWriteOverlay::new(&storage, &staged);

        assert_eq!(
            overlay.last_with_prefix("indices", b"user:").unwrap(),
            Some((b"user:3".to_vec(), b"staged-3".to_vec()))
        );
        assert_eq!(storage.last_with_prefix_calls.get(), 1);
        assert_eq!(storage.prefix_scans.get(), 0);
        assert_eq!(storage.reverse_prefix_scans.get(), 0);
    }

    #[test]
    fn memory_storage_write_many_validates_column_families_before_writing() {
        let storage = MemoryStorage::new(&["records"]);
        let error = storage
            .write_many(&[
                WriteOperation::set("records", b"1", b"record"),
                WriteOperation::set("missing", b"2", b"nope"),
            ])
            .unwrap_err();

        assert!(matches!(error, Error::ColumnFamilyNotFound(_)));
        assert_eq!(storage.get("records", b"1").unwrap(), None);
    }

    #[test]
    fn memory_storage_conforms_to_order_and_atomic_batch_contract() {
        let storage = MemoryStorage::new(&["records"]);
        conformance::persistence_order_and_batch_atomicity(storage);
    }

    #[test]
    fn memory_storage_reopen_adds_column_families_without_losing_data() {
        let storage = MemoryStorage::new(&["records"]);
        conformance::reopen_preserves_data_and_adds_families(storage);
    }

    #[test]
    fn memory_storage_conforms_to_delta_append_contract() {
        let storage = MemoryStorage::new(&["records"]);
        conformance::delta_append_current_winner_observes_merged_state(storage);
    }

    #[test]
    fn record_store_writes_and_reads_typed_records() {
        let temp_dir = tempfile::tempdir().unwrap();
        let storage = RocksDbStorage::open(temp_dir.path(), &["records"]).unwrap();
        let descriptor = RecordDescriptor::new([("id", ValueType::U64)]);
        let store = RecordStore::new(&storage, "records", &descriptor);
        let key = b"1".as_slice();
        let record = descriptor.create(&[Value::U64(42)]).unwrap();
        let op = store.set(key, &record);

        storage.write_many(&[op]).unwrap();

        let stored = store.get(key).unwrap().unwrap();
        assert_eq!(stored.get_idx(0).unwrap(), Value::U64(42));
    }

    #[test]
    fn windowed_record_store_reads_match_plain_after_arbitrary_consolidation() {
        let storage = MemoryStorage::new(&["jazz_docs_history"]);
        let descriptor = window_value_descriptor();
        let expected = seed_window_records(&storage, descriptor, 9);
        let store = window_store(&storage, &descriptor);

        assert_eq!(store.prefix(&[]).unwrap(), expected);
        assert_eq!(
            store.consolidate_windows(3).unwrap(),
            WindowConsolidation {
                windows: 3,
                records: 9
            }
        );

        let mixed_key = window_key(1, 200, 7);
        let mixed_value = window_value(descriptor, "late-plain", b"late");
        storage
            .set("jazz_docs_history", &mixed_key, &mixed_value)
            .unwrap();
        let interleaved_key = window_key(1, 103, 8);
        let interleaved_value = window_value(descriptor, "interleaved-plain", b"interleaved");
        storage
            .set("jazz_docs_history", &interleaved_key, &interleaved_value)
            .unwrap();
        let mut expected = expected;
        expected.push((mixed_key.clone(), mixed_value.clone()));
        expected.push((interleaved_key.clone(), interleaved_value.clone()));
        expected.sort_by(|left, right| left.0.cmp(&right.0));

        assert_eq!(
            store.get_raw(&expected[5].0).unwrap(),
            Some(expected[5].1.clone())
        );
        assert_eq!(
            store
                .range(&window_key(1, 102, 7), &window_key(1, 107, 7))
                .unwrap(),
            expected[2..8].to_vec()
        );
        assert_eq!(store.prefix(&window_key(1, 10, 7)[..1]).unwrap(), expected);

        let mut reversed = Vec::new();
        store
            .scan_prefix_reverse(&window_key(1, 10, 7)[..1], &mut |key, value| {
                reversed.push((key.to_vec(), value.to_vec()));
                Ok(())
            })
            .unwrap();
        let mut expected_reversed = expected.clone();
        expected_reversed.reverse();
        assert_eq!(reversed, expected_reversed);

        assert_eq!(
            store.last_with_prefix(&window_key(1, 10, 7)[..1]).unwrap(),
            expected_reversed.first().cloned()
        );
        assert_eq!(
            store
                .last_with_prefix_before_or_at(&window_key(1, 10, 7)[..1], &window_key(1, 105, 7))
                .unwrap(),
            Some(expected[6].clone())
        );
    }

    #[test]
    fn windowed_record_store_get_skips_later_window_that_sorts_before_target() {
        let storage = MemoryStorage::new(&["jazz_docs_history"]);
        let descriptor = window_value_descriptor();
        let store = window_store(&storage, &descriptor);

        let first_run = [
            (1, 11, "row-1-initial"),
            (2, 12, "row-2-initial"),
            (3, 13, "row-3-initial"),
            (4, 14, "row-4-initial"),
        ];
        let expected_target = first_run[2];
        for (row, time, title) in first_run {
            storage
                .set(
                    "jazz_docs_history",
                    &window_key(row, time, 7),
                    &window_value(descriptor, title, title.as_bytes()),
                )
                .unwrap();
        }
        assert_eq!(
            store.consolidate_windows(10).unwrap(),
            WindowConsolidation {
                windows: 1,
                records: 4
            }
        );

        for (row, time, title) in [(1, 100, "row-1-later"), (2, 101, "row-2-later")] {
            storage
                .set(
                    "jazz_docs_history",
                    &window_key(row, time, 7),
                    &window_value(descriptor, title, title.as_bytes()),
                )
                .unwrap();
        }
        assert_eq!(
            store.consolidate_windows(10).unwrap(),
            WindowConsolidation {
                windows: 1,
                records: 2
            }
        );

        let target_key = window_key(expected_target.0, expected_target.1, 7);
        assert_eq!(
            store.get_raw(&target_key).unwrap(),
            Some(window_value(
                descriptor,
                expected_target.2,
                expected_target.2.as_bytes()
            ))
        );
    }

    #[test]
    fn windowed_record_store_get_skips_sparse_overlapping_window_miss() {
        let storage = MemoryStorage::new(&["jazz_docs_history"]);
        let descriptor = window_value_descriptor();
        let store = window_store(&storage, &descriptor);

        for (row, time, title) in [
            (1, 10, "base-1"),
            (2, 10, "base-2"),
            (3, 10, "base-3"),
            (4, 10, "base-4"),
            (5, 10, "base-5"),
        ] {
            storage
                .set(
                    "jazz_docs_history",
                    &window_key(row, time, 7),
                    &window_value(descriptor, title, title.as_bytes()),
                )
                .unwrap();
        }
        assert_eq!(
            store.consolidate_windows(10).unwrap(),
            WindowConsolidation {
                windows: 1,
                records: 5
            }
        );

        for (row, time, title) in [(2, 99, "sparse-2"), (5, 99, "sparse-5")] {
            storage
                .set(
                    "jazz_docs_history",
                    &window_key(row, time, 7),
                    &window_value(descriptor, title, title.as_bytes()),
                )
                .unwrap();
        }
        assert_eq!(
            store.consolidate_windows(10).unwrap(),
            WindowConsolidation {
                windows: 1,
                records: 2
            }
        );

        let target_key = window_key(3, 10, 7);
        let target_value = window_value(descriptor, "base-3", b"base-3");
        assert_eq!(
            store.prefix(&target_key).unwrap(),
            vec![(target_key.clone(), target_value.clone())]
        );
        assert_eq!(store.get_raw(&target_key).unwrap(), Some(target_value));
    }

    #[test]
    fn windowed_record_store_exact_get_uses_range_index_not_prefix_scan() {
        struct CountingStorage<S> {
            inner: S,
            point_reads: Cell<usize>,
            prefix_scans: Cell<usize>,
            reverse_prefix_scans: Cell<usize>,
            last_calls: Cell<usize>,
            last_before_calls: Cell<usize>,
        }

        impl<S> CountingStorage<S> {
            fn new(inner: S) -> Self {
                Self {
                    inner,
                    point_reads: Cell::new(0),
                    prefix_scans: Cell::new(0),
                    reverse_prefix_scans: Cell::new(0),
                    last_calls: Cell::new(0),
                    last_before_calls: Cell::new(0),
                }
            }
        }

        impl<S> OrderedKvStorage for CountingStorage<S>
        where
            S: OrderedKvStorage,
        {
            fn get(&self, cf: &ColumnFamilyName, key: &Key) -> Result<Option<Vec<u8>>, Error> {
                self.point_reads.set(self.point_reads.get() + 1);
                self.inner.get(cf, key)
            }

            fn set(&self, cf: &ColumnFamilyName, key: &Key, value: &[u8]) -> Result<(), Error> {
                self.inner.set(cf, key, value)
            }

            fn delete(&self, cf: &ColumnFamilyName, key: &Key) -> Result<(), Error> {
                self.inner.delete(cf, key)
            }

            fn scan_range(
                &self,
                cf: &ColumnFamilyName,
                start: &Key,
                end: &Key,
                visit: &mut ScanVisitor<'_>,
            ) -> Result<(), Error> {
                self.inner.scan_range(cf, start, end, visit)
            }

            fn scan_prefix(
                &self,
                cf: &ColumnFamilyName,
                prefix: &Key,
                visit: &mut ScanVisitor<'_>,
            ) -> Result<(), Error> {
                self.prefix_scans.set(self.prefix_scans.get() + 1);
                self.inner.scan_prefix(cf, prefix, visit)
            }

            fn scan_prefix_reverse(
                &self,
                cf: &ColumnFamilyName,
                prefix: &Key,
                visit: &mut ScanVisitor<'_>,
            ) -> Result<(), Error> {
                self.reverse_prefix_scans
                    .set(self.reverse_prefix_scans.get() + 1);
                self.inner.scan_prefix_reverse(cf, prefix, visit)
            }

            fn last_with_prefix(
                &self,
                cf: &ColumnFamilyName,
                prefix: &Key,
            ) -> Result<Option<KeyValue>, Error> {
                self.last_calls.set(self.last_calls.get() + 1);
                self.inner.last_with_prefix(cf, prefix)
            }

            fn last_with_prefix_before_or_at(
                &self,
                cf: &ColumnFamilyName,
                prefix: &Key,
                upper: &Key,
            ) -> Result<Option<KeyValue>, Error> {
                self.last_before_calls.set(self.last_before_calls.get() + 1);
                self.inner.last_with_prefix_before_or_at(cf, prefix, upper)
            }

            fn write_many(&self, operations: &[WriteOperation<'_>]) -> Result<(), Error> {
                self.inner.write_many(operations)
            }
        }

        let storage = CountingStorage::new(MemoryStorage::new(&["jazz_docs_history"]));
        let descriptor = window_value_descriptor();
        let expected = seed_window_records(&storage, descriptor, 12);
        let store = window_store(&storage, &descriptor);
        assert_eq!(
            store.consolidate_windows(12).unwrap(),
            WindowConsolidation {
                windows: 1,
                records: 12
            }
        );
        storage.point_reads.set(0);
        storage.prefix_scans.set(0);
        storage.reverse_prefix_scans.set(0);
        storage.last_calls.set(0);
        storage.last_before_calls.set(0);

        assert_eq!(
            store.get_raw(&expected[8].0).unwrap(),
            Some(expected[8].1.clone())
        );
        assert!(storage.point_reads.get() <= 4);
        assert_eq!(storage.last_before_calls.get(), 0);
        assert_eq!(storage.prefix_scans.get(), 0);
        assert_eq!(storage.reverse_prefix_scans.get(), 0);

        storage.point_reads.set(0);
        storage.prefix_scans.set(0);
        storage.reverse_prefix_scans.set(0);
        storage.last_calls.set(0);
        storage.last_before_calls.set(0);
        assert_eq!(
            store.last_with_prefix(b"").unwrap(),
            expected.last().cloned()
        );
        assert_eq!(storage.last_calls.get(), 1);
        assert_eq!(storage.last_before_calls.get(), 2);
        assert_eq!(storage.prefix_scans.get(), 0);
        assert_eq!(storage.reverse_prefix_scans.get(), 0);
    }

    #[test]
    fn windowed_absent_get_uses_range_index_instead_of_plain_tail_walk() {
        let storage = MemoryStorage::new(&["jazz_docs_history"]);
        let descriptor = window_value_descriptor();
        let mut expected = Vec::new();
        expected.extend(seed_window_records_from(&storage, descriptor, 0, 9));
        let store = window_store(&storage, &descriptor);
        assert_eq!(
            store.consolidate_windows(3).unwrap(),
            WindowConsolidation {
                windows: 3,
                records: 9
            }
        );
        expected.extend(seed_window_records_from(
            &storage, descriptor, 10_000, 5_000,
        ));

        let present_in_window = expected[4].0.clone();
        let present_plain = expected.last().unwrap().0.clone();
        let absent_in_window_range = window_key(1, 104, 8);
        let absent_after_plain_tail = window_key(1, 20_000, 7);
        for key in [
            present_in_window,
            present_plain,
            absent_in_window_range,
            absent_after_plain_tail.clone(),
        ] {
            let indexed = store
                .get_raw_from_window_range_index(&key)
                .unwrap()
                .unwrap_or(None);
            assert_eq!(
                indexed,
                store.get_raw_run_aware_legacy_walk(&key).unwrap(),
                "indexed and legacy paths must agree for key {key:?}"
            );
        }

        let metrics = RefCell::new(StorageReadMetrics::default());
        let metered = MeteredStorage::new(&storage, &metrics);
        let metered_store = window_store(&metered, &descriptor);
        assert_eq!(
            metered_store.get_raw(&absent_after_plain_tail).unwrap(),
            None
        );
        let reads = metrics.borrow().history_rows.reads;
        assert!(
            reads <= 4,
            "absent lookup should not walk the plain tail; reads={reads}, metrics={:?}",
            metrics.borrow()
        );

        for tail in [1_000, 5_000, 20_000] {
            let (legacy_reads, indexed_reads) = absent_lookup_reads_for_plain_tail(tail);
            eprintln!(
                "windowed_absent_lookup_scaling tail={tail} legacy_reads={legacy_reads} indexed_reads={indexed_reads}"
            );
            assert!(
                indexed_reads <= 4,
                "indexed absent lookup should stay constant; tail={tail}, reads={indexed_reads}"
            );
        }
    }

    fn absent_lookup_reads_for_plain_tail(tail: u64) -> (usize, usize) {
        let storage = MemoryStorage::new(&["jazz_docs_history"]);
        let descriptor = window_value_descriptor();
        seed_window_records_from(&storage, descriptor, 0, 9);
        let store = window_store(&storage, &descriptor);
        assert_eq!(
            store.consolidate_windows(3).unwrap(),
            WindowConsolidation {
                windows: 3,
                records: 9
            }
        );
        seed_window_records_from(&storage, descriptor, 10_000, tail);
        let absent_after_plain_tail = window_key(1, 40_000, 7);

        let metrics = RefCell::new(StorageReadMetrics::default());
        let metered = MeteredStorage::new(&storage, &metrics);
        let metered_store = window_store(&metered, &descriptor);
        assert_eq!(
            metered_store
                .get_raw_run_aware_legacy_walk(&absent_after_plain_tail)
                .unwrap(),
            None
        );
        let legacy_reads = metrics.borrow().history_rows.reads;

        *metrics.borrow_mut() = StorageReadMetrics::default();
        assert_eq!(
            metered_store.get_raw(&absent_after_plain_tail).unwrap(),
            None
        );
        let indexed_reads = metrics.borrow().history_rows.reads;
        (legacy_reads, indexed_reads)
    }

    #[test]
    fn windowed_run_aware_ranges_use_bounded_plain_and_candidate_window_reads() {
        let storage = MemoryStorage::new(&["jazz_docs_history"]);
        let descriptor = window_value_descriptor();
        let mut expected = Vec::new();
        expected.extend(seed_window_records_from(&storage, descriptor, 0, 9));
        let store = window_store(&storage, &descriptor);
        assert_eq!(
            store.consolidate_windows(3).unwrap(),
            WindowConsolidation {
                windows: 3,
                records: 9
            }
        );
        expected.extend(seed_window_records_from(
            &storage, descriptor, 10_000, 5_000,
        ));

        let cases = [
            (
                "plain-only",
                window_key(1, 10_150, 7),
                window_key(1, 10_153, 7),
            ),
            ("window-only", window_key(1, 103, 7), window_key(1, 106, 7)),
            ("mixed", window_key(1, 106, 7), window_key(1, 10_002, 7)),
            ("empty", window_key(1, 9_000, 7), window_key(1, 9_003, 7)),
        ];
        for (name, start, end) in cases {
            let indexed_forward = store.range(&start, &end).unwrap();
            storage
                .delete("jazz_docs_history", WINDOW_RANGE_INDEX_KEY)
                .unwrap();
            let legacy_forward = store.range(&start, &end).unwrap();
            assert_eq!(
                indexed_forward, legacy_forward,
                "forward range mismatch for {name}"
            );

            let indexed_reverse = store.range_reverse(&start, &end).unwrap();
            storage
                .delete("jazz_docs_history", WINDOW_RANGE_INDEX_KEY)
                .unwrap();
            let legacy_reverse = store.range_reverse(&start, &end).unwrap();
            assert_eq!(
                indexed_reverse, legacy_reverse,
                "reverse range mismatch for {name}"
            );
        }

        let prefix_key = window_key(1, 10_250, 7);
        let indexed_prefix = store.prefix(&prefix_key).unwrap();
        storage
            .delete("jazz_docs_history", WINDOW_RANGE_INDEX_KEY)
            .unwrap();
        let legacy_prefix = store.prefix(&prefix_key).unwrap();
        assert_eq!(indexed_prefix, legacy_prefix);
        assert_eq!(
            indexed_prefix,
            expected
                .iter()
                .filter(|(key, _)| key.starts_with(&prefix_key))
                .cloned()
                .collect::<Vec<_>>()
        );

        let metrics = RefCell::new(StorageReadMetrics::default());
        let metered = MeteredStorage::new(&storage, &metrics);
        let metered_store = window_store(&metered, &descriptor);
        let narrow_start = window_key(1, 10_300, 7);
        let narrow_end = window_key(1, 10_303, 7);
        let rows = metered_store.range(&narrow_start, &narrow_end).unwrap();
        assert_eq!(rows.len(), 3);
        let reads = metrics.borrow().history_rows.reads;
        assert!(
            reads <= 8,
            "narrow range should read only bounded plain rows and metadata; reads={reads}, metrics={:?}",
            metrics.borrow()
        );

        *metrics.borrow_mut() = StorageReadMetrics::default();
        let rows = metered_store.prefix(&prefix_key).unwrap();
        assert_eq!(rows.len(), 1);
        let reads = metrics.borrow().history_rows.reads;
        assert!(
            reads <= 6,
            "narrow prefix should read only matching plain rows and metadata; reads={reads}, metrics={:?}",
            metrics.borrow()
        );

        *metrics.borrow_mut() = StorageReadMetrics::default();
        let rows = metered_store
            .range_reverse(&narrow_start, &narrow_end)
            .unwrap();
        assert_eq!(rows.len(), 3);
        let reads = metrics.borrow().history_rows.reads;
        assert!(
            reads <= 8,
            "narrow reverse range should read only bounded plain rows and metadata; reads={reads}, metrics={:?}",
            metrics.borrow()
        );

        for tail in [1_000, 5_000, 20_000] {
            let (legacy_reads, indexed_reads) = reverse_range_reads_for_plain_tail(tail);
            eprintln!(
                "windowed_reverse_range_scaling tail={tail} legacy_reads={legacy_reads} indexed_reads={indexed_reads}"
            );
            assert!(
                indexed_reads <= 8,
                "indexed reverse range should stay bounded; tail={tail}, reads={indexed_reads}"
            );
        }
    }

    fn reverse_range_reads_for_plain_tail(tail: u64) -> (usize, usize) {
        let storage = MemoryStorage::new(&["jazz_docs_history"]);
        let descriptor = window_value_descriptor();
        seed_window_records_from(&storage, descriptor, 0, 9);
        let store = window_store(&storage, &descriptor);
        assert_eq!(
            store.consolidate_windows(3).unwrap(),
            WindowConsolidation {
                windows: 3,
                records: 9
            }
        );
        seed_window_records_from(&storage, descriptor, 10_000, tail);
        let start = window_key(1, 10_300, 7);
        let end = window_key(1, 10_303, 7);

        let metrics = RefCell::new(StorageReadMetrics::default());
        let metered = MeteredStorage::new(&storage, &metrics);
        let metered_store = window_store(&metered, &descriptor);
        let mut legacy = metered_store
            .run_aware_records(
                |key| key >= start.as_slice() && key < end.as_slice(),
                |visit| metered.scan_prefix("jazz_docs_history", b"", visit),
            )
            .unwrap();
        legacy.reverse();
        assert_eq!(legacy.len(), 3);
        let legacy_reads = metrics.borrow().history_rows.reads;

        *metrics.borrow_mut() = StorageReadMetrics::default();
        let indexed = metered_store.range_reverse(&start, &end).unwrap();
        assert_eq!(indexed, legacy);
        let indexed_reads = metrics.borrow().history_rows.reads;
        (legacy_reads, indexed_reads)
    }

    #[test]
    fn windowed_record_store_survives_reopen_mid_consolidated_history() {
        let temp_dir = tempfile::tempdir().unwrap();
        let descriptor = window_value_descriptor();
        let expected = {
            let storage = RocksDbStorage::open(temp_dir.path(), &["jazz_docs_history"]).unwrap();
            let expected = seed_window_records(&storage, descriptor, 12);
            let store = window_store(&storage, &descriptor);
            assert_eq!(
                store.consolidate_windows(5).unwrap(),
                WindowConsolidation {
                    windows: 3,
                    records: 12
                }
            );
            assert_eq!(store.prefix(&[]).unwrap(), expected);
            expected
        };

        let storage = RocksDbStorage::open(temp_dir.path(), &["jazz_docs_history"]).unwrap();
        let store = window_store(&storage, &descriptor);
        assert_eq!(store.prefix(&[]).unwrap(), expected);
        assert_eq!(
            store.get_raw(&expected[8].0).unwrap(),
            Some(expected[8].1.clone())
        );
    }

    #[test]
    fn full_window_consolidation_cursor_advances_past_encoded_windows() {
        let storage = MemoryStorage::new(&["jazz_docs_history"]);
        let descriptor = window_value_descriptor();
        seed_window_records(&storage, descriptor, 12);
        let store = window_store(&storage, &descriptor);

        assert_eq!(
            store.consolidate_full_windows_bounded(3, 1).unwrap(),
            WindowConsolidation {
                windows: 1,
                records: 3
            }
        );
        assert_eq!(
            store.consolidate_full_windows_bounded(3, 1).unwrap(),
            WindowConsolidation {
                windows: 1,
                records: 3
            }
        );
        assert_eq!(
            store.consolidate_full_windows_bounded(3, 10).unwrap(),
            WindowConsolidation {
                windows: 2,
                records: 6
            }
        );

        let cursor_before = storage
            .get("jazz_docs_history", WINDOW_CURSOR_KEY)
            .unwrap()
            .expect("cursor should be persisted");
        let remaining = storage
            .range("jazz_docs_history", &cursor_before, WINDOW_MARKER_KEY)
            .unwrap();
        assert!(
            remaining
                .iter()
                .all(|(key, _)| key == WINDOW_CURSOR_KEY || key == WINDOW_MARKER_KEY),
            "cursor should sit past all already encoded windows"
        );
        assert_eq!(
            store.consolidate_full_windows_bounded(3, 10).unwrap(),
            WindowConsolidation::default()
        );
        let cursor_after = storage
            .get("jazz_docs_history", WINDOW_CURSOR_KEY)
            .unwrap()
            .expect("cursor should still be persisted");
        assert_eq!(cursor_after, cursor_before);
        assert_eq!(
            store.consolidate_full_windows_bounded(3, 10).unwrap(),
            WindowConsolidation::default()
        );
        assert_eq!(
            storage.get("jazz_docs_history", WINDOW_CURSOR_KEY).unwrap(),
            Some(cursor_after)
        );
    }

    #[test]
    fn rocksdb_storage_conforms_to_delta_append_contract() {
        let temp_dir = tempfile::tempdir().unwrap();
        let storage = RocksDbStorage::open(temp_dir.path(), &["records"]).unwrap();
        conformance::delta_append_current_winner_observes_merged_state(storage);
    }

    #[test]
    fn rocksdb_delta_append_survives_reopen_with_pending_operands() {
        fn record(time: u64, node: u8, payload: &[u8]) -> Vec<u8> {
            let mut bytes = Vec::new();
            bytes.extend(time.to_le_bytes());
            bytes.extend([node; 16]);
            bytes.extend(payload);
            bytes
        }
        fn delta(time: u64, node: u8, record: Vec<u8>) -> StorageDelta {
            StorageDelta::current_winner(CurrentWinnerDelta {
                tx_time: time,
                tx_node_uuid: [node; 16],
                parents: Vec::new(),
                tx_time_offset: 0,
                tx_node_uuid_offset: 8,
                record,
            })
            .unwrap()
        }

        let temp_dir = tempfile::tempdir().unwrap();
        {
            let storage = RocksDbStorage::open(temp_dir.path(), &["records"]).unwrap();
            storage
                .write_many(&[WriteOperation::delta(
                    "records",
                    b"row",
                    &delta(10, 1, record(10, 1, b"older")),
                )])
                .unwrap();
            storage
                .write_many(&[WriteOperation::delta(
                    "records",
                    b"row",
                    &delta(20, 2, record(20, 2, b"newer")),
                )])
                .unwrap();
        }
        let reopened = RocksDbStorage::open(temp_dir.path(), &["records"]).unwrap();
        assert_eq!(
            reopened.get("records", b"row").unwrap(),
            Some(record(20, 2, b"newer"))
        );
    }
}
