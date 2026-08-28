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
//! records beyond typed convenience wrappers, or Jazz semantics. Physical
//! adapters live in outward crates; higher layers decide when a batch is
//! durable and how storage writes relate to an IVM tick.

mod idb;
#[cfg_attr(not(target_arch = "wasm32"), allow(dead_code))]
mod key_codec;
mod manifest;
mod memory;
#[cfg(any(test, feature = "test"))]
mod test;

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;

use crate::records::{Record, RecordDescriptor};
use thiserror::Error;

pub use idb::IdbStorage;
pub use manifest::{
    AdapterFormat, ManifestOpenReceipt, MigrationJournal, MigrationRegistry, STORAGE_EPOCH_1,
    StorageEpochManifest, StorageMigration,
};
pub use memory::MemoryStorage;
#[cfg(any(test, feature = "test"))]
pub use test::{TestStorage, TestStorageControl, TestStorageOperation, YieldingStorage};

pub type ColumnFamilyName = str;
pub type Key = [u8];
pub type Value = Vec<u8>;
pub type KeyValue = (Vec<u8>, Vec<u8>);

/// Return the smallest unsigned-lexicographic key strictly after every key
/// beginning with `prefix`.
///
/// `[0x12, 0xff]` therefore has successor `[0x13]`. A prefix consisting only
/// of `0xff` has no finite successor; scans must retain their prefix predicate
/// while traversing to the end of the column family.
pub fn prefix_successor(prefix: &[u8]) -> Option<Vec<u8>> {
    key_codec::increment_bytes(prefix)
}

/// Maximum UTF-8 byte length of an application-owned storage name.
///
/// The bound is part of the cross-backend storage contract: IndexedDB frames
/// a column-family name with an unsigned 16-bit length, while the native
/// backends store the same logical names directly. Keeping one smaller-layer
/// bound here makes schema admission portable across those backends.
pub const MAX_APPLICATION_STORAGE_NAME_BYTES: usize = u16::MAX as usize;

/// Validate a physical column-family name accepted by a storage backend.
///
/// This is deliberately narrower than [`validate_application_storage_name`]:
/// engine-owned families also pass through backend open calls. It captures the
/// portable framing/FFI requirements that every backend must enforce before it
/// opens, reopens, or mutates its storage.
pub fn validate_physical_storage_name(name: &str) -> Result<(), Error> {
    if name.len() > MAX_APPLICATION_STORAGE_NAME_BYTES {
        return Err(Error::InvalidStorageLayout(format!(
            "storage name exceeds the {MAX_APPLICATION_STORAGE_NAME_BYTES}-byte limit: {} bytes",
            name.len()
        )));
    }
    if name.contains('\0') {
        return Err(Error::InvalidStorageLayout(
            "storage name contains an embedded NUL".to_owned(),
        ));
    }
    Ok(())
}

/// Validate a complete backend-open family set before the backend has touched
/// its durable state.
pub fn validate_physical_storage_names(
    names: impl IntoIterator<Item = impl AsRef<str>>,
) -> Result<(), Error> {
    for name in names {
        validate_physical_storage_name(name.as_ref())?;
    }
    Ok(())
}

/// Validate a table or direct-record-store name supplied by an application.
///
/// Names are case-sensitive, matching every supported backend. The lowercase
/// `__groove_` plane belongs to Groove (including the class layout and
/// large-value metadata); `indices` and RocksDB's `default` family are also
/// engine-owned. Reserving them here, rather than independently in backends,
/// prevents a schema from opening successfully on one backend while aliasing
/// engine state on another. Embedded NUL is rejected because RocksDB passes
/// family names through C strings.
pub fn validate_application_storage_name(name: &str) -> Result<(), Error> {
    validate_physical_storage_name(name)?;

    if name.starts_with("__groove_") || matches!(name, "indices" | "default") {
        return Err(Error::InvalidStorageLayout(format!(
            "application storage name is reserved by Groove: {name:?}"
        )));
    }

    Ok(())
}

/// The key interval for an ordered scan.
///
/// `Prefix` includes every key beginning with the supplied bytes. `Range` is
/// half-open (`start <= key < end`). Keeping this as data rather than a family
/// of methods makes it possible to carry scan semantics, notably a hard item
/// bound, through layouts and storage adapters without inventing a new API for
/// each combination.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum ScanBounds {
    Prefix(Vec<u8>),
    Range { start: Vec<u8>, end: Vec<u8> },
}

/// Canonical key order for an ordered scan.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ScanDirection {
    Forward,
    Reverse,
}

/// A complete ordered scan request.
///
/// `max_items` is a semantic bound, not a batching preference: a successful
/// cursor may yield at most this many logical entries in total. Backends should
/// stop traversal at that boundary; callers must not use it until lowering has
/// proved that no later operator can discard or reorder a candidate.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ScanRequest {
    pub cf: String,
    pub bounds: ScanBounds,
    pub direction: ScanDirection,
    pub max_items: Option<usize>,
}

impl ScanRequest {
    pub fn prefix(cf: String, prefix: Vec<u8>) -> Self {
        Self {
            cf,
            bounds: ScanBounds::Prefix(prefix),
            direction: ScanDirection::Forward,
            max_items: None,
        }
    }

    pub fn range(cf: String, start: Vec<u8>, end: Vec<u8>) -> Self {
        Self {
            cf,
            bounds: ScanBounds::Range { start, end },
            direction: ScanDirection::Forward,
            max_items: None,
        }
    }

    pub fn with_max_items(mut self, max_items: usize) -> Self {
        self.max_items = Some(max_items);
        self
    }

    pub fn reversed(mut self) -> Self {
        self.direction = ScanDirection::Reverse;
        self
    }
}
/// Object-safe future returned by ordered storage operations.
///
/// Storage is permitted to be executor-local (notably in browsers), so this
/// deliberately does not impose `Send`.
pub type StorageFuture<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;
pub type StorageScan<'a> = Box<dyn StorageCursor + 'a>;

/// The settled acknowledgement of one atomic [`OrderedKvStorage::write_many`]
/// attempt.
///
/// A caller that drops the write future after it has started has no receipt at
/// all and must conservatively act as though the batch may have committed.
/// Likewise, adapters must use [`WriteManyOutcome::PossiblyCommitted`] whenever
/// their native API cannot prove that a failed acknowledgement happened before
/// its atomic commit boundary.  Only [`WriteManyOutcome::Uncommitted`] permits
/// a caller to discard in-memory state or retry the identical batch.
#[derive(Debug)]
pub enum WriteManyOutcome {
    /// The atomic batch crossed the backend's commit boundary.
    Committed,
    /// The adapter proved that no operation in the batch reached its commit
    /// boundary, such as validation failing before a native write begins.
    Uncommitted(Error),
    /// The native operation returned an error without proving that the batch
    /// was not committed. This is deliberately fail-closed.
    PossiblyCommitted(Error),
}

impl WriteManyOutcome {
    pub fn is_committed(&self) -> bool {
        matches!(self, Self::Committed)
    }

    pub fn error(&self) -> Option<&Error> {
        match self {
            Self::Committed => None,
            Self::Uncommitted(error) | Self::PossiblyCommitted(error) => Some(error),
        }
    }

    pub fn may_have_committed(&self) -> bool {
        !matches!(self, Self::Uncommitted(_))
    }
}

/// Executor-local ownership boundary used by interruptible engine work.
///
/// `OrderedKvStorage` deliberately permits futures and cursors that borrow an
/// adapter. An evaluation session cannot retain such a borrow into its own
/// fields, so it owns the adapter through `Rc` and completes cursor iteration
/// inside the outer owned future. The result crossing back into the evaluator
/// is always owned.
#[derive(Clone)]
pub(crate) struct OwnedStorage<'a>(Rc<dyn OrderedKvStorage + 'a>);

impl<'a> OwnedStorage<'a> {
    pub(crate) fn new<S>(storage: Rc<S>) -> Self
    where
        S: OrderedKvStorage + 'a,
    {
        Self(storage)
    }

    pub(crate) fn as_ref(&self) -> &(dyn OrderedKvStorage + 'a) {
        self.0.as_ref()
    }

    pub(crate) fn get(
        &self,
        cf: String,
        key: Vec<u8>,
    ) -> StorageFuture<'a, Result<Option<Value>, Error>> {
        let storage = Rc::clone(&self.0);
        Box::pin(async move { storage.get(cf, key).await })
    }

    pub(crate) fn scan(
        &self,
        request: ScanRequest,
    ) -> StorageFuture<'a, Result<Vec<KeyValue>, Error>> {
        let storage = Rc::clone(&self.0);
        Box::pin(async move { collect_scan(storage.scan(request).await?).await })
    }
}

/// Owned, executor-local cursor over an ordered scan.
///
/// Backends choose their own batch size. An empty batch is not an end marker;
/// only `None` completes the scan.
pub trait StorageCursor {
    fn next_batch(&mut self) -> StorageFuture<'_, Result<Option<Vec<KeyValue>>, Error>>;
}

pub struct ReadyStorageCursor {
    values: std::vec::IntoIter<KeyValue>,
}

impl ReadyStorageCursor {
    pub fn new(values: Vec<KeyValue>) -> Self {
        Self {
            values: values.into_iter(),
        }
    }
}

impl StorageCursor for ReadyStorageCursor {
    fn next_batch(&mut self) -> StorageFuture<'_, Result<Option<Vec<KeyValue>>, Error>> {
        Box::pin(async move {
            let batch = self.values.by_ref().take(256).collect::<Vec<_>>();
            Ok((!batch.is_empty()).then_some(batch))
        })
    }
}

pub async fn collect_scan(mut scan: StorageScan<'_>) -> Result<Vec<KeyValue>, Error> {
    let mut values = Vec::new();
    while let Some(batch) = scan.next_batch().await? {
        values.extend(batch);
    }
    Ok(values)
}
const STAGED_POINT_READS_BEFORE_INDEX: usize = 16;
const STAGED_OPS_BEFORE_POINT_INDEX: usize = 64;
/// Callback form used by scans so storage implementations do not have to
/// materialize large ranges before the caller can process them.
pub type ScanVisitor<'visitor> =
    dyn for<'a, 'b> FnMut(&'a [u8], &'b [u8]) -> Result<(), Error> + 'visitor;

/// Backing-implementation interface for ordered key/value storage.
///
/// This is the only trait a storage backend must implement. Its column-family
/// names are backing details consumed by record-store plumbing; higher layers
/// should use typed record-store handles instead of calling these methods
/// directly. The trait intentionally exposes batch atomicity but no higher
/// transaction semantics; the database apply/persist/finish lifecycle owns
/// tick and durability ordering above this layer.
///
/// A read future's first poll also communicates residency. When a backend has
/// retained a point or complete scan region, reads fully covered by that
/// resident data must return `Poll::Ready` on their first poll, including known
/// absences. Successful writes through the same storage instance must be
/// reflected by those resident reads. A backend may evict retained data; after
/// eviction, a later read may become pending again.
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

    fn get(&self, cf: String, key: Vec<u8>) -> StorageFuture<'_, Result<Option<Value>, Error>>;
    /// Atomically install `value` only when `key` is absent. Returns the
    /// pre-existing value when another writer already installed one.
    fn put_if_absent(
        &self,
        cf: String,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> StorageFuture<'_, Result<Option<Value>, Error>>;
    /// Atomically delete `key` only when its bytes still equal `expected`.
    /// Returns whether this call removed the value.
    fn compare_and_delete(
        &self,
        cf: String,
        key: Vec<u8>,
        expected: Vec<u8>,
    ) -> StorageFuture<'_, Result<bool, Error>>;
    fn set(&self, cf: String, key: Vec<u8>, value: Vec<u8>)
    -> StorageFuture<'_, Result<(), Error>>;
    fn delete(&self, cf: String, key: Vec<u8>) -> StorageFuture<'_, Result<(), Error>>;
    /// Flush and close any backend resources that require an explicit clean
    /// shutdown boundary. Backends without close-time work may keep the default.
    fn close(&self) -> StorageFuture<'_, Result<(), Error>> {
        Box::pin(async { Ok(()) })
    }
    /// Configure the number of committed write batches between explicit local
    /// durability boundaries. Backends that do not require an explicit boundary
    /// may keep the default no-op implementation.
    fn set_write_flush_cadence(&self, _every: usize) -> StorageFuture<'_, Result<(), Error>> {
        Box::pin(async { Ok(()) })
    }
    /// Finish any pending write cadence and make all preceding writes locally
    /// durable. Backends that do not require an explicit boundary may keep the
    /// default no-op implementation.
    fn flush_write_boundary(&self) -> StorageFuture<'_, Result<(), Error>> {
        Box::pin(async { Ok(()) })
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
    fn approximate_class_bytes(
        &self,
        _cf: String,
    ) -> StorageFuture<'_, Result<Option<u64>, Error>> {
        Box::pin(async { Ok(None) })
    }

    /// Begin one explicit ordered scan. `max_items` is part of the storage
    /// contract: the backend must not decode or retain candidates beyond it.
    fn scan(&self, request: ScanRequest) -> StorageFuture<'_, Result<StorageScan<'_>, Error>>;
    fn last_with_prefix(
        &self,
        cf: String,
        prefix: Vec<u8>,
    ) -> StorageFuture<'_, Result<Option<KeyValue>, Error>> {
        Box::pin(async move {
            Ok(collect_scan(
                self.scan(ScanRequest::prefix(cf, prefix).reversed())
                    .await?,
            )
            .await?
            .into_iter()
            .next())
        })
    }
    fn last_with_prefix_before_or_at(
        &self,
        cf: String,
        prefix: Vec<u8>,
        upper: Vec<u8>,
    ) -> StorageFuture<'_, Result<Option<KeyValue>, Error>> {
        Box::pin(async move {
            Ok(
                collect_scan(self.scan(ScanRequest::prefix(cf, prefix)).await?)
                    .await?
                    .into_iter()
                    .take_while(|(key, _)| key <= &upper)
                    .last(),
            )
        })
    }
    fn write_many(
        &self,
        operations: Vec<OwnedWriteOperation>,
    ) -> StorageFuture<'_, Result<(), Error>>;

    /// Submit one atomic batch with an acknowledgement class.
    ///
    /// This is the portable commit-result boundary. The legacy [`Self::write_many`]
    /// convenience method remains for callers that do not advance in-memory
    /// state before persistence. Its default adapter is conservative: a plain
    /// error has no proof that the native commit did not happen.
    fn write_many_outcome(
        &self,
        operations: Vec<OwnedWriteOperation>,
    ) -> StorageFuture<'_, WriteManyOutcome> {
        Box::pin(async move {
            match self.write_many(operations).await {
                Ok(()) => WriteManyOutcome::Committed,
                Err(error) => WriteManyOutcome::PossiblyCommitted(error),
            }
        })
    }

    /// Return known column-family names when the backend can enumerate them.
    ///
    /// This is intentionally optional so the ordered-KV contract stays small.
    /// Layout validation uses it to reject pre-release physical-layout changes
    /// loudly instead of opening an old store as if it were empty.
    fn column_family_names(&self) -> Option<Vec<String>> {
        None
    }

    fn range(
        &self,
        cf: String,
        start: Vec<u8>,
        end: Vec<u8>,
    ) -> StorageFuture<'_, Result<Vec<KeyValue>, Error>> {
        Box::pin(
            async move { collect_scan(self.scan(ScanRequest::range(cf, start, end)).await?).await },
        )
    }

    fn prefix(
        &self,
        cf: String,
        prefix: Vec<u8>,
    ) -> StorageFuture<'_, Result<Vec<KeyValue>, Error>> {
        Box::pin(
            async move { collect_scan(self.scan(ScanRequest::prefix(cf, prefix)).await?).await },
        )
    }
}

impl<S> OrderedKvStorage for Rc<S>
where
    S: OrderedKvStorage,
{
    fn scan(&self, request: ScanRequest) -> StorageFuture<'_, Result<StorageScan<'_>, Error>> {
        self.as_ref().scan(request)
    }

    fn get(&self, cf: String, key: Vec<u8>) -> StorageFuture<'_, Result<Option<Value>, Error>> {
        self.as_ref().get(cf, key)
    }

    fn put_if_absent(
        &self,
        cf: String,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> StorageFuture<'_, Result<Option<Value>, Error>> {
        self.as_ref().put_if_absent(cf, key, value)
    }

    fn compare_and_delete(
        &self,
        cf: String,
        key: Vec<u8>,
        expected: Vec<u8>,
    ) -> StorageFuture<'_, Result<bool, Error>> {
        self.as_ref().compare_and_delete(cf, key, expected)
    }

    fn set(
        &self,
        cf: String,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> StorageFuture<'_, Result<(), Error>> {
        self.as_ref().set(cf, key, value)
    }

    fn delete(&self, cf: String, key: Vec<u8>) -> StorageFuture<'_, Result<(), Error>> {
        self.as_ref().delete(cf, key)
    }

    fn close(&self) -> StorageFuture<'_, Result<(), Error>> {
        self.as_ref().close()
    }

    fn set_write_flush_cadence(&self, every: usize) -> StorageFuture<'_, Result<(), Error>> {
        self.as_ref().set_write_flush_cadence(every)
    }

    fn flush_write_boundary(&self) -> StorageFuture<'_, Result<(), Error>> {
        self.as_ref().flush_write_boundary()
    }

    fn cache_token(&self) -> usize {
        self.as_ref().cache_token()
    }

    fn approximate_class_bytes(&self, cf: String) -> StorageFuture<'_, Result<Option<u64>, Error>> {
        self.as_ref().approximate_class_bytes(cf)
    }

    fn last_with_prefix(
        &self,
        cf: String,
        prefix: Vec<u8>,
    ) -> StorageFuture<'_, Result<Option<KeyValue>, Error>> {
        self.as_ref().last_with_prefix(cf, prefix)
    }

    fn write_many(
        &self,
        operations: Vec<OwnedWriteOperation>,
    ) -> StorageFuture<'_, Result<(), Error>> {
        self.as_ref().write_many(operations)
    }

    fn write_many_outcome(
        &self,
        operations: Vec<OwnedWriteOperation>,
    ) -> StorageFuture<'_, WriteManyOutcome> {
        self.as_ref().write_many_outcome(operations)
    }

    fn column_family_names(&self) -> Option<Vec<String>> {
        self.as_ref().column_family_names()
    }
}

impl<S> OrderedKvStorage for &S
where
    S: OrderedKvStorage,
{
    fn scan(&self, request: ScanRequest) -> StorageFuture<'_, Result<StorageScan<'_>, Error>> {
        S::scan(*self, request)
    }

    fn get(&self, cf: String, key: Vec<u8>) -> StorageFuture<'_, Result<Option<Value>, Error>> {
        S::get(*self, cf, key)
    }

    fn put_if_absent(
        &self,
        cf: String,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> StorageFuture<'_, Result<Option<Value>, Error>> {
        S::put_if_absent(*self, cf, key, value)
    }

    fn compare_and_delete(
        &self,
        cf: String,
        key: Vec<u8>,
        expected: Vec<u8>,
    ) -> StorageFuture<'_, Result<bool, Error>> {
        S::compare_and_delete(*self, cf, key, expected)
    }

    fn set(
        &self,
        cf: String,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> StorageFuture<'_, Result<(), Error>> {
        S::set(*self, cf, key, value)
    }

    fn delete(&self, cf: String, key: Vec<u8>) -> StorageFuture<'_, Result<(), Error>> {
        S::delete(*self, cf, key)
    }

    fn write_many(
        &self,
        operations: Vec<OwnedWriteOperation>,
    ) -> StorageFuture<'_, Result<(), Error>> {
        S::write_many(*self, operations)
    }

    fn write_many_outcome(
        &self,
        operations: Vec<OwnedWriteOperation>,
    ) -> StorageFuture<'_, WriteManyOutcome> {
        S::write_many_outcome(*self, operations)
    }

    fn column_family_names(&self) -> Option<Vec<String>> {
        S::column_family_names(*self)
    }
}

const CLASS_HISTORY_CF: &str = "__groove_class_history";
const CLASS_REGISTER_CF: &str = "__groove_class_register";
const CLASS_GLOBAL_CURRENT_CF: &str = "__groove_class_global_current";
const CLASS_AHEAD_CURRENT_CF: &str = "__groove_class_ahead_current";
const CLASS_CHANGES_CF: &str = "__groove_class_changes";
const CLASS_INDICES_CF: &str = "__groove_class_indices";
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
    #[default]
    Identity,
    JazzClassV1,
}

impl StorageLayout {
    pub fn jazz_class_v1() -> Self {
        Self::JazzClassV1
    }

    pub fn physical_column_families<'a>(
        &self,
        logical_column_families: impl IntoIterator<Item = &'a str>,
    ) -> Vec<String> {
        let mut names = BTreeSet::new();
        if matches!(self, Self::JazzClassV1) {
            names.insert(CLASS_META_CF.to_owned());
        }
        for logical in logical_column_families {
            let physical = match self {
                Self::Identity => logical,
                Self::JazzClassV1 => jazz_physical_class(logical).unwrap_or(logical),
            };
            names.insert(physical.to_owned());
        }
        names.into_iter().collect()
    }

    fn map_cf<'a>(&'a self, logical_cf: &'a str) -> Result<PhysicalCf<'a>, Error> {
        // A Jazz class key embeds this name. Validate before calculating its
        // byte length or framing it, so no invalid logical CF can acquire a
        // durable spelling even through this lower-level storage view.
        validate_physical_storage_name(logical_cf)?;
        match self {
            Self::Identity => Ok(PhysicalCf {
                physical_cf: logical_cf,
                logical_prefix: None,
            }),
            Self::JazzClassV1 => {
                if let Some(physical_cf) = jazz_physical_class(logical_cf) {
                    Ok(PhysicalCf {
                        physical_cf,
                        logical_prefix: Some(logical_cf),
                    })
                } else {
                    Ok(PhysicalCf {
                        physical_cf: logical_cf,
                        logical_prefix: None,
                    })
                }
            }
        }
    }

    fn validates_marker(&self) -> bool {
        matches!(self, Self::JazzClassV1)
    }
}

struct PhysicalCf<'a> {
    physical_cf: &'a str,
    logical_prefix: Option<&'a str>,
}

struct MappedStorageCursor<'a> {
    inner: StorageScan<'a>,
    strip_len: usize,
}

impl StorageCursor for MappedStorageCursor<'_> {
    fn next_batch(&mut self) -> StorageFuture<'_, Result<Option<Vec<KeyValue>>, Error>> {
        Box::pin(async move {
            let Some(batch) = self.inner.next_batch().await? else {
                return Ok(None);
            };
            batch
                .into_iter()
                .map(|(key, value)| {
                    let logical = key.get(self.strip_len..).ok_or_else(|| {
                        Error::InvalidStorageKey(
                            "physical layout key shorter than logical prefix".to_owned(),
                        )
                    })?;
                    Ok((logical.to_vec(), value))
                })
                .collect::<Result<Vec<_>, Error>>()
                .map(Some)
        })
    }
}

fn is_jazz_history_table(name: &str) -> bool {
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

fn jazz_physical_class(logical_cf: &str) -> Option<&'static str> {
    if is_jazz_history_table(logical_cf) {
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
    } else if logical_cf.starts_with("jazz_") {
        Some(CLASS_META_CF)
    } else {
        None
    }
}

/// Storage view that keeps logical CF names at the database boundary while
/// reading and writing a physical class-CF layout below it.
pub struct LayoutStorage {
    inner: BoxedStorage,
    layout: StorageLayout,
}

impl LayoutStorage {
    pub async fn new<S>(inner: S, layout: StorageLayout) -> Result<Self, Error>
    where
        S: ReopenableStorage + 'static,
    {
        Self::new_boxed(BoxedStorage::new(inner), layout).await
    }

    pub async fn new_boxed(inner: BoxedStorage, layout: StorageLayout) -> Result<Self, Error> {
        let storage = Self { inner, layout };
        storage.ensure_layout_marker().await?;
        Ok(storage)
    }

    pub fn into_inner(self) -> BoxedStorage {
        self.inner
    }

    async fn ensure_layout_marker(&self) -> Result<(), Error> {
        if !self.layout.validates_marker() {
            return Ok(());
        }

        match self
            .inner
            .get(CLASS_META_CF.to_owned(), CLASS_LAYOUT_MARKER_KEY.to_vec())
            .await?
        {
            Some(value) if value == CLASS_LAYOUT_MARKER_VALUE => Ok(()),
            Some(_) => Err(Error::InvalidStorageLayout(
                "unsupported class-CF storage layout marker".to_owned(),
            )),
            None => {
                if self.has_class_data_or_legacy_layout().await? {
                    return Err(Error::InvalidStorageLayout(
                        "missing class-CF storage layout marker in non-empty store".to_owned(),
                    ));
                }
                self.inner
                    .set(
                        CLASS_META_CF.to_owned(),
                        CLASS_LAYOUT_MARKER_KEY.to_vec(),
                        CLASS_LAYOUT_MARKER_VALUE.to_vec(),
                    )
                    .await
            }
        }
    }

    async fn has_class_data_or_legacy_layout(&self) -> Result<bool, Error> {
        if self.layout.validates_marker() {
            // An unenumerable adapter cannot distinguish a fresh class layout
            // from a legacy logical-CF store. Creating the marker would make
            // that ambiguity durable, so V1 deliberately refuses it.
            let names = self.inner.column_family_names().ok_or_else(|| {
                Error::InvalidStorageLayout(
                    "class-CF v1 requires enumerable physical column families".to_owned(),
                )
            })?;
            if names.iter().any(|name| jazz_physical_class(name).is_some()) {
                return Ok(true);
            }
        }
        for cf in [
            CLASS_HISTORY_CF,
            CLASS_REGISTER_CF,
            CLASS_GLOBAL_CURRENT_CF,
            CLASS_AHEAD_CURRENT_CF,
            CLASS_CHANGES_CF,
            CLASS_INDICES_CF,
            CLASS_META_CF,
        ] {
            match self.inner.last_with_prefix(cf.to_owned(), Vec::new()).await {
                Ok(Some(_)) => return Ok(true),
                Ok(None) | Err(Error::ColumnFamilyNotFound(_)) => {}
                Err(error) => return Err(error),
            }
        }
        Ok(false)
    }

    fn physical_key(&self, cf: &ColumnFamilyName, key: &Key) -> Result<(String, Vec<u8>), Error> {
        let mapping = self.layout.map_cf(cf)?;
        let Some(logical_prefix) = mapping.logical_prefix else {
            return Ok((mapping.physical_cf.to_owned(), key.to_vec()));
        };
        let mut physical_key = Vec::with_capacity(4 + logical_prefix.len() + key.len());
        physical_key.extend_from_slice(&(logical_prefix.len() as u32).to_be_bytes());
        physical_key.extend_from_slice(logical_prefix.as_bytes());
        physical_key.extend_from_slice(key);
        Ok((mapping.physical_cf.to_owned(), physical_key))
    }

    fn physical_prefix(
        &self,
        cf: &ColumnFamilyName,
        prefix: &Key,
    ) -> Result<(String, Vec<u8>, usize), Error> {
        let mapping = self.layout.map_cf(cf)?;
        let Some(logical_prefix) = mapping.logical_prefix else {
            return Ok((mapping.physical_cf.to_owned(), prefix.to_vec(), 0));
        };
        let mut physical_prefix = Vec::with_capacity(4 + logical_prefix.len() + prefix.len());
        physical_prefix.extend_from_slice(&(logical_prefix.len() as u32).to_be_bytes());
        physical_prefix.extend_from_slice(logical_prefix.as_bytes());
        physical_prefix.extend_from_slice(prefix);
        let strip_len = 4 + logical_prefix.len();
        Ok((mapping.physical_cf.to_owned(), physical_prefix, strip_len))
    }

    fn physical_operations(
        &self,
        operations: Vec<OwnedWriteOperation>,
    ) -> Result<Vec<OwnedWriteOperation>, Error> {
        operations
            .into_iter()
            .map(|operation| {
                Ok(match operation {
                    OwnedWriteOperation::Set { cf, key, value } => {
                        let (cf, key) = self.physical_key(&cf, &key)?;
                        OwnedWriteOperation::Set { cf, key, value }
                    }
                    OwnedWriteOperation::Delete { cf, key } => {
                        let (cf, key) = self.physical_key(&cf, &key)?;
                        OwnedWriteOperation::Delete { cf, key }
                    }
                })
            })
            .collect()
    }
}

impl OrderedKvStorage for LayoutStorage {
    fn get(&self, cf: String, key: Vec<u8>) -> StorageFuture<'_, Result<Option<Value>, Error>> {
        Box::pin(async move {
            let (physical_cf, physical_key) = self.physical_key(&cf, &key)?;
            self.inner.get(physical_cf, physical_key).await
        })
    }

    fn put_if_absent(
        &self,
        cf: String,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> StorageFuture<'_, Result<Option<Value>, Error>> {
        Box::pin(async move {
            let (physical_cf, physical_key) = self.physical_key(&cf, &key)?;
            self.inner
                .put_if_absent(physical_cf, physical_key, value)
                .await
        })
    }

    fn compare_and_delete(
        &self,
        cf: String,
        key: Vec<u8>,
        expected: Vec<u8>,
    ) -> StorageFuture<'_, Result<bool, Error>> {
        Box::pin(async move {
            let (physical_cf, physical_key) = self.physical_key(&cf, &key)?;
            self.inner
                .compare_and_delete(physical_cf, physical_key, expected)
                .await
        })
    }

    fn set(
        &self,
        cf: String,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> StorageFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            let (physical_cf, physical_key) = self.physical_key(&cf, &key)?;
            self.inner.set(physical_cf, physical_key, value).await
        })
    }

    fn delete(&self, cf: String, key: Vec<u8>) -> StorageFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            let (physical_cf, physical_key) = self.physical_key(&cf, &key)?;
            self.inner.delete(physical_cf, physical_key).await
        })
    }

    fn close(&self) -> StorageFuture<'_, Result<(), Error>> {
        self.inner.close()
    }

    fn set_write_flush_cadence(&self, every: usize) -> StorageFuture<'_, Result<(), Error>> {
        self.inner.set_write_flush_cadence(every)
    }

    fn flush_write_boundary(&self) -> StorageFuture<'_, Result<(), Error>> {
        self.inner.flush_write_boundary()
    }

    fn scan(&self, request: ScanRequest) -> StorageFuture<'_, Result<StorageScan<'_>, Error>> {
        let ScanRequest {
            cf,
            bounds,
            direction,
            max_items,
        } = request;
        Box::pin(async move {
            let (physical_cf, physical_bounds, strip_len) = match bounds {
                ScanBounds::Prefix(prefix) => {
                    let (physical_cf, physical_prefix, strip_len) =
                        self.physical_prefix(&cf, &prefix)?;
                    (physical_cf, ScanBounds::Prefix(physical_prefix), strip_len)
                }
                ScanBounds::Range { start, end } => {
                    let (physical_cf, physical_start, strip_len) =
                        self.physical_prefix(&cf, &start)?;
                    let (_, physical_end, _) = self.physical_prefix(&cf, &end)?;
                    (
                        physical_cf,
                        ScanBounds::Range {
                            start: physical_start,
                            end: physical_end,
                        },
                        strip_len,
                    )
                }
            };
            let inner = self
                .inner
                .scan(ScanRequest {
                    cf: physical_cf,
                    bounds: physical_bounds,
                    direction,
                    max_items,
                })
                .await?;
            Ok(Box::new(MappedStorageCursor { inner, strip_len }) as StorageScan<'_>)
        })
    }

    fn last_with_prefix(
        &self,
        cf: String,
        prefix: Vec<u8>,
    ) -> StorageFuture<'_, Result<Option<KeyValue>, Error>> {
        Box::pin(async move {
            let (physical_cf, physical_prefix, strip_len) = self.physical_prefix(&cf, &prefix)?;
            Ok(collect_scan(
                self.inner
                    .scan(ScanRequest::prefix(physical_cf, physical_prefix).reversed())
                    .await?,
            )
            .await?
            .into_iter()
            .next()
            .map(|(key, value)| (key[strip_len..].to_vec(), value)))
        })
    }

    fn last_with_prefix_before_or_at(
        &self,
        cf: String,
        prefix: Vec<u8>,
        upper: Vec<u8>,
    ) -> StorageFuture<'_, Result<Option<KeyValue>, Error>> {
        Box::pin(async move {
            let (physical_cf, physical_prefix, strip_len) = self.physical_prefix(&cf, &prefix)?;
            let (_, physical_upper, _) = self.physical_prefix(&cf, &upper)?;
            Ok(collect_scan(
                self.inner
                    .scan(ScanRequest::prefix(physical_cf, physical_prefix))
                    .await?,
            )
            .await?
            .into_iter()
            .rfind(|(key, _)| key <= &physical_upper)
            .map(|(key, value)| (key[strip_len..].to_vec(), value)))
        })
    }

    fn write_many(
        &self,
        operations: Vec<OwnedWriteOperation>,
    ) -> StorageFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            self.inner
                .write_many(self.physical_operations(operations)?)
                .await
        })
    }

    fn write_many_outcome(
        &self,
        operations: Vec<OwnedWriteOperation>,
    ) -> StorageFuture<'_, WriteManyOutcome> {
        Box::pin(async move {
            match self.physical_operations(operations) {
                Ok(operations) => self.inner.write_many_outcome(operations).await,
                Err(error) => WriteManyOutcome::Uncommitted(error),
            }
        })
    }

    fn column_family_names(&self) -> Option<Vec<String>> {
        self.inner.column_family_names()
    }

    fn approximate_class_bytes(&self, cf: String) -> StorageFuture<'_, Result<Option<u64>, Error>> {
        Box::pin(async move {
            let physical_cf = self.layout.map_cf(&cf)?.physical_cf.to_owned();
            self.inner.approximate_class_bytes(physical_cf).await
        })
    }
}

/// Storage that can be reconstructed with an expanded table/column-family set.
pub trait ReopenableStorage: OrderedKvStorage + Sized {
    fn reopen(self, column_families: Vec<String>) -> StorageFuture<'static, Result<Self, Error>>
    where
        Self: 'static;
}

/// Object-safe form of [`ReopenableStorage`] used at runtime adapter
/// boundaries.
pub trait ErasedReopenableStorage: OrderedKvStorage {
    fn reopen_boxed(
        self: Box<Self>,
        column_families: Vec<String>,
    ) -> StorageFuture<'static, Result<Box<dyn ErasedReopenableStorage>, Error>>;
}

impl<S> ErasedReopenableStorage for S
where
    S: ReopenableStorage + 'static,
{
    fn reopen_boxed(
        self: Box<Self>,
        column_families: Vec<String>,
    ) -> StorageFuture<'static, Result<Box<dyn ErasedReopenableStorage>, Error>> {
        Box::pin(async move {
            Ok(Box::new((*self).reopen(column_families).await?)
                as Box<dyn ErasedReopenableStorage>)
        })
    }
}

/// Type-erased, reopenable ordered-KV backend.
pub struct BoxedStorage {
    inner: Box<dyn ErasedReopenableStorage>,
}

impl BoxedStorage {
    pub fn new<S>(storage: S) -> Self
    where
        S: ReopenableStorage + 'static,
    {
        Self {
            inner: Box::new(storage),
        }
    }
}

impl OrderedKvStorage for BoxedStorage {
    fn scan(&self, request: ScanRequest) -> StorageFuture<'_, Result<StorageScan<'_>, Error>> {
        self.inner.scan(request)
    }

    fn get(&self, cf: String, key: Vec<u8>) -> StorageFuture<'_, Result<Option<Value>, Error>> {
        self.inner.get(cf, key)
    }

    fn put_if_absent(
        &self,
        cf: String,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> StorageFuture<'_, Result<Option<Value>, Error>> {
        self.inner.put_if_absent(cf, key, value)
    }

    fn compare_and_delete(
        &self,
        cf: String,
        key: Vec<u8>,
        expected: Vec<u8>,
    ) -> StorageFuture<'_, Result<bool, Error>> {
        self.inner.compare_and_delete(cf, key, expected)
    }

    fn set(
        &self,
        cf: String,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> StorageFuture<'_, Result<(), Error>> {
        self.inner.set(cf, key, value)
    }

    fn delete(&self, cf: String, key: Vec<u8>) -> StorageFuture<'_, Result<(), Error>> {
        self.inner.delete(cf, key)
    }

    fn close(&self) -> StorageFuture<'_, Result<(), Error>> {
        self.inner.close()
    }

    fn set_write_flush_cadence(&self, every: usize) -> StorageFuture<'_, Result<(), Error>> {
        self.inner.set_write_flush_cadence(every)
    }

    fn flush_write_boundary(&self) -> StorageFuture<'_, Result<(), Error>> {
        self.inner.flush_write_boundary()
    }

    fn approximate_class_bytes(&self, cf: String) -> StorageFuture<'_, Result<Option<u64>, Error>> {
        self.inner.approximate_class_bytes(cf)
    }

    fn last_with_prefix(
        &self,
        cf: String,
        prefix: Vec<u8>,
    ) -> StorageFuture<'_, Result<Option<KeyValue>, Error>> {
        self.inner.last_with_prefix(cf, prefix)
    }

    fn last_with_prefix_before_or_at(
        &self,
        cf: String,
        prefix: Vec<u8>,
        upper: Vec<u8>,
    ) -> StorageFuture<'_, Result<Option<KeyValue>, Error>> {
        self.inner.last_with_prefix_before_or_at(cf, prefix, upper)
    }

    fn write_many(
        &self,
        operations: Vec<OwnedWriteOperation>,
    ) -> StorageFuture<'_, Result<(), Error>> {
        self.inner.write_many(operations)
    }

    fn write_many_outcome(
        &self,
        operations: Vec<OwnedWriteOperation>,
    ) -> StorageFuture<'_, WriteManyOutcome> {
        self.inner.write_many_outcome(operations)
    }

    fn column_family_names(&self) -> Option<Vec<String>> {
        self.inner.column_family_names()
    }
}

impl ReopenableStorage for BoxedStorage {
    fn reopen(self, column_families: Vec<String>) -> StorageFuture<'static, Result<Self, Error>> {
        Box::pin(async move {
            Ok(Self {
                inner: self.inner.reopen_boxed(column_families).await?,
            })
        })
    }
}

/// Opens a persistent ordered-KV backend at an exact target-owned path.
pub trait StorageFactory: std::fmt::Debug + Send + Sync {
    fn open(
        &self,
        path: std::path::PathBuf,
        column_families: Vec<String>,
    ) -> StorageFuture<'_, Result<BoxedStorage, Error>>;
}

/// Typed view over one storage column family.
pub struct RecordStore<'a, S: ?Sized> {
    storage: &'a S,
    /// One table or durable index column family.
    column_family: &'a str,
    /// Interprets stored bytes without copying until a caller asks for values.
    descriptor: &'a RecordDescriptor,
}

impl<'a, S: ?Sized> RecordStore<'a, S>
where
    S: OrderedKvStorage,
{
    pub fn new(storage: &'a S, column_family: &'a str, descriptor: &'a RecordDescriptor) -> Self {
        Self {
            storage,
            column_family,
            descriptor,
        }
    }

    pub fn descriptor(&self) -> &RecordDescriptor {
        self.descriptor
    }

    pub fn column_family(&self) -> &str {
        self.column_family
    }

    pub async fn get_raw(&self, key: &Key) -> Result<Option<Vec<u8>>, Error> {
        self.storage
            .get(self.column_family.to_owned(), key.to_vec())
            .await
    }

    pub async fn get(&self, key: &Key) -> Result<Option<Record<'_>>, Error> {
        self.get_raw(key)
            .await
            .map(|record| record.map(|record| self.descriptor.bind_owned(record)))
    }

    pub async fn range(&self, start: &Key, end: &Key) -> Result<Vec<KeyValue>, Error> {
        self.storage
            .range(self.column_family.to_owned(), start.to_vec(), end.to_vec())
            .await
    }

    pub async fn prefix(&self, prefix: &Key) -> Result<Vec<KeyValue>, Error> {
        self.storage
            .prefix(self.column_family.to_owned(), prefix.to_vec())
            .await
    }

    pub async fn range_reverse(&self, start: &Key, end: &Key) -> Result<Vec<KeyValue>, Error> {
        let mut records = self.range(start, end).await?;
        records.reverse();
        Ok(records)
    }

    pub fn scan(&self, bounds: ScanBounds) -> StorageFuture<'_, Result<StorageScan<'_>, Error>> {
        self.storage.scan(ScanRequest {
            cf: self.column_family.to_owned(),
            bounds,
            direction: ScanDirection::Forward,
            max_items: None,
        })
    }

    pub async fn last_with_prefix(&self, prefix: &Key) -> Result<Option<KeyValue>, Error> {
        self.storage
            .last_with_prefix(self.column_family.to_owned(), prefix.to_vec())
            .await
    }

    pub async fn last_with_prefix_before_or_at(
        &self,
        prefix: &Key,
        upper: &Key,
    ) -> Result<Option<KeyValue>, Error> {
        self.storage
            .last_with_prefix_before_or_at(
                self.column_family.to_owned(),
                prefix.to_vec(),
                upper.to_vec(),
            )
            .await
    }

    pub fn set(&self, key: &Key, record: &[u8]) -> OwnedWriteOperation {
        OwnedWriteOperation::Set {
            cf: self.column_family.to_owned(),
            key: key.to_vec(),
            value: record.to_vec(),
        }
    }

    pub fn delete(&self, key: &Key) -> OwnedWriteOperation {
        OwnedWriteOperation::Delete {
            cf: self.column_family.to_owned(),
            key: key.to_vec(),
        }
    }

    pub fn write_many(
        &self,
        operations: Vec<OwnedWriteOperation>,
    ) -> StorageFuture<'_, Result<(), Error>> {
        self.storage.write_many(operations)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WriteOperation<'a> {
    /// Borrowed operation so callers can build a RocksDB batch without cloning
    /// already-owned encoded records.
    Set {
        cf: &'a str,
        key: &'a Key,
        value: &'a [u8],
    },
    Delete {
        cf: &'a str,
        key: &'a Key,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OwnedWriteOperation {
    Set {
        cf: String,
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Delete {
        cf: String,
        key: Vec<u8>,
    },
}

impl OwnedWriteOperation {
    #[cfg(test)]
    pub(crate) fn set(
        cf: impl Into<String>,
        key: impl Into<Vec<u8>>,
        value: impl Into<Vec<u8>>,
    ) -> Self {
        Self::Set {
            cf: cf.into(),
            key: key.into(),
            value: value.into(),
        }
    }

    #[cfg(test)]
    pub(crate) fn delete(cf: impl Into<String>, key: impl Into<Vec<u8>>) -> Self {
        Self::Delete {
            cf: cf.into(),
            key: key.into(),
        }
    }

    pub fn as_write_operation(&self) -> WriteOperation<'_> {
        match self {
            Self::Set { cf, key, value } => WriteOperation::set(cf, key, value),
            Self::Delete { cf, key } => WriteOperation::delete(cf, key),
        }
    }

    fn cf(&self) -> &str {
        match self {
            Self::Set { cf, .. } | Self::Delete { cf, .. } => cf,
        }
    }

    fn key(&self) -> &[u8] {
        match self {
            Self::Set { key, .. } | Self::Delete { key, .. } => key,
        }
    }
}

enum OverlayHandle<'a, T> {
    Borrowed(&'a T),
    Owned(Rc<T>),
}

impl<T> Clone for OverlayHandle<'_, T> {
    fn clone(&self) -> Self {
        match self {
            Self::Borrowed(value) => Self::Borrowed(value),
            Self::Owned(value) => Self::Owned(Rc::clone(value)),
        }
    }
}

impl<T> std::ops::Deref for OverlayHandle<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        match self {
            Self::Borrowed(value) => value,
            Self::Owned(value) => value,
        }
    }
}

pub struct StagedWriteOverlay<'a, S> {
    base: OverlayHandle<'a, S>,
    staged_writes: OverlayHandle<'a, RefCell<StagedWriteState>>,
}

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
    pub fn new(base: &'a S) -> Self {
        Self {
            base,
            staged_writes: RefCell::new(StagedWriteState::default()),
        }
    }

    pub async fn commit(self) -> Result<(), Error> {
        let operations = self.staged_writes.into_inner().into_operations();
        self.base.write_many(operations).await
    }

    pub fn is_empty(&self) -> bool {
        self.staged_writes.borrow().is_empty()
    }

    pub fn stage_owned_operations(
        &self,
        operations: impl IntoIterator<Item = OwnedWriteOperation>,
    ) {
        self.staged_writes.borrow_mut().extend(operations);
    }
}

impl<'a, S> StagedWriteOverlay<'a, S> {
    pub(crate) fn new(base: &'a S, staged_writes: &'a RefCell<StagedWriteState>) -> Self {
        Self {
            base: OverlayHandle::Borrowed(base),
            staged_writes: OverlayHandle::Borrowed(staged_writes),
        }
    }

    pub(crate) fn new_owned(
        base: Rc<S>,
        staged_writes: Rc<RefCell<StagedWriteState>>,
    ) -> StagedWriteOverlay<'static, S>
    where
        S: 'static,
    {
        StagedWriteOverlay {
            base: OverlayHandle::Owned(base),
            staged_writes: OverlayHandle::Owned(staged_writes),
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

fn overlay_point_value(
    mut value: Option<Value>,
    operations: &[OwnedWriteOperation],
    cf: &str,
    key: &[u8],
) -> Result<Option<Value>, Error> {
    for operation in operations {
        if operation.cf() != cf || operation.key() != key {
            continue;
        }
        match operation {
            OwnedWriteOperation::Set { value: set, .. } => value = Some(set.clone()),
            OwnedWriteOperation::Delete { .. } => value = None,
        }
    }
    Ok(value)
}

fn snapshot_staged_operations(
    staged_writes: &RefCell<StagedWriteState>,
    include: impl Fn(&OwnedWriteOperation) -> bool,
) -> Vec<OwnedWriteOperation> {
    staged_writes
        .borrow()
        .operations
        .iter()
        .filter(|operation| include(operation))
        .cloned()
        .collect()
}

/// Ordered cursor which merges the durable base with the staged transaction
/// writes as the caller asks for batches.  It intentionally owns only the
/// in-range staged keys; its base cursor remains lazy, so a logical limit does
/// not turn a sparse staged overlay into a full base-prefix materialization.
struct OverlayScanCursor<'a> {
    base: StorageScan<'a>,
    staged: VecDeque<(Vec<u8>, Vec<OwnedWriteOperation>)>,
    base_entries: VecDeque<KeyValue>,
    base_done: bool,
    cf: String,
    direction: ScanDirection,
    remaining: Option<usize>,
}

impl OverlayScanCursor<'_> {
    fn next_entry(&mut self) -> StorageFuture<'_, Result<Option<KeyValue>, Error>> {
        Box::pin(async move {
            loop {
                while self.base_entries.is_empty() && !self.base_done {
                    match self.base.next_batch().await? {
                        Some(entries) => self.base_entries.extend(entries),
                        None => self.base_done = true,
                    }
                }

                let choice = match (self.base_entries.front(), self.staged.front()) {
                    (None, None) => return Ok(None),
                    (Some(_), None) => (true, false),
                    (None, Some(_)) => (false, true),
                    (Some((base_key, _)), Some((staged_key, _))) if base_key == staged_key => {
                        (true, true)
                    }
                    (Some((base_key, _)), Some((staged_key, _))) => {
                        let staged_first = match self.direction {
                            ScanDirection::Forward => staged_key < base_key,
                            ScanDirection::Reverse => staged_key > base_key,
                        };
                        (!staged_first, staged_first)
                    }
                };
                let base_entry = choice.0.then(|| self.base_entries.pop_front()).flatten();
                let staged_entry = choice.1.then(|| self.staged.pop_front()).flatten();

                match staged_entry {
                    Some((key, operations)) => {
                        let base_value = base_entry.map(|(_, value)| value);
                        if let Some(value) =
                            overlay_point_value(base_value, &operations, &self.cf, &key)?
                        {
                            return Ok(Some((key, value)));
                        }
                    }
                    None => {
                        if let Some(entry) = base_entry {
                            return Ok(Some(entry));
                        }
                    }
                }
            }
        })
    }
}

impl StorageCursor for OverlayScanCursor<'_> {
    fn next_batch(&mut self) -> StorageFuture<'_, Result<Option<Vec<KeyValue>>, Error>> {
        Box::pin(async move {
            let Some(batch_len) = self.remaining.map_or(Some(256), |remaining| {
                (remaining > 0).then_some(remaining.min(256))
            }) else {
                return Ok(None);
            };

            let mut values = Vec::with_capacity(batch_len);
            while values.len() < batch_len {
                let Some(entry) = self.next_entry().await? else {
                    break;
                };
                values.push(entry);
                if let Some(remaining) = &mut self.remaining {
                    *remaining -= 1;
                }
            }
            Ok((!values.is_empty()).then_some(values))
        })
    }
}

fn overlay_scan<'a, S>(
    base: &'a S,
    staged_writes: &RefCell<StagedWriteState>,
    request: ScanRequest,
) -> StorageFuture<'a, Result<StorageScan<'a>, Error>>
where
    S: OrderedKvStorage,
{
    let cf = request.cf.clone();
    let bounds = request.bounds.clone();
    let operations = snapshot_staged_operations(staged_writes, |operation| {
        operation.cf() == cf
            && match &bounds {
                ScanBounds::Prefix(prefix) => operation.key().starts_with(prefix),
                ScanBounds::Range { start, end } => {
                    operation.key() >= start.as_slice() && operation.key() < end.as_slice()
                }
            }
    });
    Box::pin(async move {
        let mut staged = BTreeMap::<Vec<u8>, Vec<OwnedWriteOperation>>::new();
        for operation in operations {
            staged
                .entry(operation.key().to_vec())
                .or_default()
                .push(operation);
        }

        // A base limit of only the logical result size is unsound: every
        // staged key whose final operation can remove it may consume one of
        // those physical entries without producing a logical result. A
        // Thus `limit + removals` is both a hard physical ceiling and enough base entries to fill the
        // requested logical result when they exist.
        let physical_max_items = request.max_items.map(|limit| {
            let final_removals = staged
                .values()
                .filter(|operations| {
                    matches!(operations.last(), Some(OwnedWriteOperation::Delete { .. }))
                })
                .count();
            limit.saturating_add(final_removals)
        });
        let base = base
            .scan(ScanRequest {
                max_items: physical_max_items,
                ..request.clone()
            })
            .await?;
        let mut staged = staged.into_iter().collect::<VecDeque<_>>();
        if request.direction == ScanDirection::Reverse {
            staged.make_contiguous().reverse();
        }
        Ok(Box::new(OverlayScanCursor {
            base,
            staged,
            base_entries: VecDeque::new(),
            base_done: false,
            cf: request.cf,
            direction: request.direction,
            remaining: request.max_items,
        }) as StorageScan<'a>)
    })
}

impl<S> OrderedKvStorage for StagedWriteOverlay<'_, S>
where
    S: OrderedKvStorage,
{
    fn put_if_absent(
        &self,
        _cf: String,
        _key: Vec<u8>,
        _value: Vec<u8>,
    ) -> StorageFuture<'_, Result<Option<Value>, Error>> {
        Box::pin(async { Err(Error::ConditionalMutationInTransaction) })
    }

    fn compare_and_delete(
        &self,
        _cf: String,
        _key: Vec<u8>,
        _expected: Vec<u8>,
    ) -> StorageFuture<'_, Result<bool, Error>> {
        Box::pin(async { Err(Error::ConditionalMutationInTransaction) })
    }
    fn scan(&self, request: ScanRequest) -> StorageFuture<'_, Result<StorageScan<'_>, Error>> {
        overlay_scan(&*self.base, &self.staged_writes, request)
    }

    fn get(&self, cf: String, key: Vec<u8>) -> StorageFuture<'_, Result<Option<Value>, Error>> {
        let mut staged_writes = self.staged_writes.borrow_mut();
        if staged_writes.is_empty() {
            drop(staged_writes);
            return self.base.get(cf, key);
        }

        let Some(index) = staged_writes.latest_index(&cf, &key) else {
            drop(staged_writes);
            return self.base.get(cf, key);
        };
        match &staged_writes.operations[index] {
            OwnedWriteOperation::Set { value, .. } => {
                let value = value.clone();
                Box::pin(async move { Ok(Some(value)) })
            }
            OwnedWriteOperation::Delete { .. } => Box::pin(async { Ok(None) }),
        }
    }

    fn set(
        &self,
        cf: String,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> StorageFuture<'_, Result<(), Error>> {
        self.stage(OwnedWriteOperation::Set { cf, key, value });
        Box::pin(async { Ok(()) })
    }

    fn delete(&self, cf: String, key: Vec<u8>) -> StorageFuture<'_, Result<(), Error>> {
        self.stage(OwnedWriteOperation::Delete { cf, key });
        Box::pin(async { Ok(()) })
    }

    fn write_many(
        &self,
        operations: Vec<OwnedWriteOperation>,
    ) -> StorageFuture<'_, Result<(), Error>> {
        self.staged_writes.borrow_mut().extend(operations);
        Box::pin(async { Ok(()) })
    }

    fn approximate_class_bytes(&self, cf: String) -> StorageFuture<'_, Result<Option<u64>, Error>> {
        self.base.approximate_class_bytes(cf)
    }

    fn column_family_names(&self) -> Option<Vec<String>> {
        self.base.column_family_names()
    }
}

impl<S> OrderedKvStorage for StorageTransaction<'_, S>
where
    S: OrderedKvStorage,
{
    fn put_if_absent(
        &self,
        _cf: String,
        _key: Vec<u8>,
        _value: Vec<u8>,
    ) -> StorageFuture<'_, Result<Option<Value>, Error>> {
        Box::pin(async { Err(Error::ConditionalMutationInTransaction) })
    }

    fn compare_and_delete(
        &self,
        _cf: String,
        _key: Vec<u8>,
        _expected: Vec<u8>,
    ) -> StorageFuture<'_, Result<bool, Error>> {
        Box::pin(async { Err(Error::ConditionalMutationInTransaction) })
    }
    fn scan(&self, request: ScanRequest) -> StorageFuture<'_, Result<StorageScan<'_>, Error>> {
        overlay_scan(self.base, &self.staged_writes, request)
    }

    fn get(&self, cf: String, key: Vec<u8>) -> StorageFuture<'_, Result<Option<Value>, Error>> {
        Box::pin(async move {
            StagedWriteOverlay::new(self.base, &self.staged_writes)
                .get(cf, key)
                .await
        })
    }

    fn set(
        &self,
        cf: String,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> StorageFuture<'_, Result<(), Error>> {
        self.staged_writes
            .borrow_mut()
            .stage(OwnedWriteOperation::Set { cf, key, value });
        Box::pin(async { Ok(()) })
    }

    fn delete(&self, cf: String, key: Vec<u8>) -> StorageFuture<'_, Result<(), Error>> {
        self.staged_writes
            .borrow_mut()
            .stage(OwnedWriteOperation::Delete { cf, key });
        Box::pin(async { Ok(()) })
    }

    fn write_many(
        &self,
        operations: Vec<OwnedWriteOperation>,
    ) -> StorageFuture<'_, Result<(), Error>> {
        self.staged_writes.borrow_mut().extend(operations);
        Box::pin(async { Ok(()) })
    }

    fn approximate_class_bytes(&self, cf: String) -> StorageFuture<'_, Result<Option<u64>, Error>> {
        self.base.approximate_class_bytes(cf)
    }

    fn column_family_names(&self) -> Option<Vec<String>> {
        self.base.column_family_names()
    }
}

impl<'a> WriteOperation<'a> {
    pub fn set(cf: &'a str, key: &'a Key, value: &'a [u8]) -> Self {
        Self::Set { cf, key, value }
    }

    pub fn delete(cf: &'a str, key: &'a Key) -> Self {
        Self::Delete { cf, key }
    }
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("column family not found: {0}")]
    ColumnFamilyNotFound(String),
    #[error("invalid storage layout: {0}")]
    InvalidStorageLayout(String),
    #[error("invalid storage key: {0}")]
    InvalidStorageKey(String),
    #[error("conditional mutations require a direct storage boundary")]
    ConditionalMutationInTransaction,
    #[error("IndexedDB storage remained contended after {retries} generation-conflict retries")]
    IdbGenerationContention { retries: usize },
    #[error("record error: {0}")]
    Record(#[source] Box<crate::records::Error>),
    #[error("{backend} storage error: {message}")]
    Backend {
        backend: &'static str,
        message: String,
    },
    #[error(transparent)]
    IdbTree(#[from] idb_tree::Error),
}

impl From<crate::records::Error> for Error {
    fn from(error: crate::records::Error) -> Self {
        Self::Record(Box::new(error))
    }
}

/// Backend-neutral ordered-KV conformance scenarios.
///
/// These test the raw storage seam. Raw cursors are deliberately non-snapshot:
/// stable repeatable evaluation belongs to the higher-level evaluator.
#[cfg(any(test, feature = "test"))]
pub mod conformance {
    use super::*;

    pub async fn persistence_order_and_batch_atomicity<S>(storage: S)
    where
        S: OrderedKvStorage,
    {
        storage
            .set("records".into(), b"user:2".to_vec(), b"two".to_vec())
            .await
            .unwrap();
        storage
            .set("records".into(), b"user:1".to_vec(), b"one".to_vec())
            .await
            .unwrap();
        storage
            .set("records".into(), b"user:10".to_vec(), b"ten".to_vec())
            .await
            .unwrap();
        storage
            .set("records".into(), vec![0xff, 0x00], b"ff-zero".to_vec())
            .await
            .unwrap();
        storage
            .set("records".into(), vec![0xff, 0x01], b"ff-one".to_vec())
            .await
            .unwrap();
        for (key, value) in [
            (vec![0x12, 0xfe], b"interior-before".to_vec()),
            (vec![0x12, 0xff], b"interior-root".to_vec()),
            (vec![0x12, 0xff, 0x00], b"interior-zero".to_vec()),
            (vec![0x12, 0xff, 0xff], b"interior-ff".to_vec()),
            (vec![0x13, 0x00], b"interior-after".to_vec()),
        ] {
            storage.set("records".into(), key, value).await.unwrap();
        }

        assert_eq!(
            storage
                .range("records".into(), b"user:".to_vec(), b"user;".to_vec())
                .await
                .unwrap(),
            vec![
                (b"user:1".to_vec(), b"one".to_vec()),
                (b"user:10".to_vec(), b"ten".to_vec()),
                (b"user:2".to_vec(), b"two".to_vec()),
            ]
        );
        assert_eq!(
            storage.prefix("records".into(), vec![0xff]).await.unwrap(),
            vec![
                (vec![0xff, 0x00], b"ff-zero".to_vec()),
                (vec![0xff, 0x01], b"ff-one".to_vec()),
            ]
        );
        assert_eq!(
            collect_scan(
                storage
                    .scan(ScanRequest::prefix("records".into(), Vec::new()))
                    .await
                    .unwrap(),
            )
            .await
            .unwrap(),
            vec![
                (vec![0x12, 0xfe], b"interior-before".to_vec()),
                (vec![0x12, 0xff], b"interior-root".to_vec()),
                (vec![0x12, 0xff, 0x00], b"interior-zero".to_vec()),
                (vec![0x12, 0xff, 0xff], b"interior-ff".to_vec()),
                (vec![0x13, 0x00], b"interior-after".to_vec()),
                (b"user:1".to_vec(), b"one".to_vec()),
                (b"user:10".to_vec(), b"ten".to_vec()),
                (b"user:2".to_vec(), b"two".to_vec()),
                (vec![0xff, 0x00], b"ff-zero".to_vec()),
                (vec![0xff, 0x01], b"ff-one".to_vec()),
            ]
        );
        assert_eq!(
            collect_scan(
                storage
                    .scan(
                        ScanRequest::prefix("records".into(), vec![0x12, 0xff])
                            .reversed()
                            .with_max_items(2),
                    )
                    .await
                    .unwrap(),
            )
            .await
            .unwrap(),
            vec![
                (vec![0x12, 0xff, 0xff], b"interior-ff".to_vec()),
                (vec![0x12, 0xff, 0x00], b"interior-zero".to_vec()),
            ]
        );
        assert_eq!(
            collect_scan(
                storage
                    .scan(
                        ScanRequest::prefix("records".into(), Vec::new())
                            .reversed()
                            .with_max_items(3),
                    )
                    .await
                    .unwrap(),
            )
            .await
            .unwrap(),
            vec![
                (vec![0xff, 0x01], b"ff-one".to_vec()),
                (vec![0xff, 0x00], b"ff-zero".to_vec()),
                (b"user:2".to_vec(), b"two".to_vec()),
            ]
        );

        let error = storage
            .write_many(vec![
                OwnedWriteOperation::Set {
                    cf: "records".into(),
                    key: b"user:3".to_vec(),
                    value: b"three".to_vec(),
                },
                OwnedWriteOperation::Set {
                    cf: "missing".into(),
                    key: b"user:4".to_vec(),
                    value: b"four".to_vec(),
                },
            ])
            .await
            .unwrap_err();
        assert!(matches!(error, Error::ColumnFamilyNotFound(_)));
        assert_eq!(
            storage
                .get("records".into(), b"user:3".to_vec())
                .await
                .unwrap(),
            None
        );

        storage
            .write_many(vec![
                OwnedWriteOperation::Set {
                    cf: "records".into(),
                    key: b"user:3".to_vec(),
                    value: b"three".to_vec(),
                },
                OwnedWriteOperation::Delete {
                    cf: "records".into(),
                    key: b"user:2".to_vec(),
                },
            ])
            .await
            .unwrap();
        assert_eq!(
            storage
                .prefix("records".into(), b"user:".to_vec())
                .await
                .unwrap(),
            vec![
                (b"user:1".to_vec(), b"one".to_vec()),
                (b"user:10".to_vec(), b"ten".to_vec()),
                (b"user:3".to_vec(), b"three".to_vec()),
            ]
        );
    }

    pub async fn reopen_preserves_data_and_adds_families<S>(storage: S)
    where
        S: ReopenableStorage + 'static,
    {
        storage
            .set("records".into(), b"1".to_vec(), b"record".to_vec())
            .await
            .unwrap();

        let storage = storage
            .reopen(vec!["records".into(), "indices".into()])
            .await
            .unwrap();
        storage
            .set("indices".into(), b"name:record".to_vec(), b"1".to_vec())
            .await
            .unwrap();

        assert_eq!(
            storage.get("records".into(), b"1".to_vec()).await.unwrap(),
            Some(b"record".to_vec())
        );
        assert_eq!(
            storage
                .get("indices".into(), b"name:record".to_vec())
                .await
                .unwrap(),
            Some(b"1".to_vec())
        );
    }

    pub async fn atomic_conditionals_preserve_winners_and_reject_stale_deletes<S>(storage: S)
    where
        S: OrderedKvStorage,
    {
        let key = b"conditional".to_vec();
        let first = b"first-install-receipt".to_vec();
        let second = b"second-install-receipt".to_vec();
        assert_eq!(
            storage
                .put_if_absent("records".into(), key.clone(), first.clone())
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            storage
                .put_if_absent("records".into(), key.clone(), second.clone())
                .await
                .unwrap(),
            Some(first.clone())
        );
        assert!(
            !storage
                .compare_and_delete("records".into(), key.clone(), second)
                .await
                .unwrap()
        );
        assert!(
            storage
                .compare_and_delete("records".into(), key.clone(), first.clone())
                .await
                .unwrap()
        );
        assert_eq!(
            storage
                .put_if_absent("records".into(), key.clone(), b"reinstalled".to_vec())
                .await
                .unwrap(),
            None
        );
        assert!(
            !storage
                .compare_and_delete("records".into(), key.clone(), first)
                .await
                .unwrap()
        );
        assert_eq!(
            storage.get("records".into(), key).await.unwrap(),
            Some(b"reinstalled".to_vec())
        );
    }

    /// Invalid operations are rejected before an atomic submission begins.
    /// This is intentionally below the public database API: only an adapter
    /// can prove the pre-commit acknowledgement classification.
    pub async fn invalid_batch_is_proven_uncommitted<S>(storage: S)
    where
        S: OrderedKvStorage,
    {
        let outcome = storage
            .write_many_outcome(vec![OwnedWriteOperation::Set {
                cf: "missing".into(),
                key: b"key".to_vec(),
                value: b"value".to_vec(),
            }])
            .await;
        assert!(matches!(
            outcome,
            WriteManyOutcome::Uncommitted(Error::ColumnFamilyNotFound(name)) if name == "missing"
        ));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Deliberately hides its family catalogue to prove that class-layout V1
    /// never treats an uninspectable store as fresh.
    struct NonEnumeratingStorage(MemoryStorage);

    impl OrderedKvStorage for NonEnumeratingStorage {
        fn get(
            &self,
            cf: String,
            key: Vec<u8>,
        ) -> StorageFuture<'_, Result<Option<super::Value>, Error>> {
            self.0.get(cf, key)
        }

        fn put_if_absent(
            &self,
            cf: String,
            key: Vec<u8>,
            value: Vec<u8>,
        ) -> StorageFuture<'_, Result<Option<super::Value>, Error>> {
            self.0.put_if_absent(cf, key, value)
        }

        fn compare_and_delete(
            &self,
            cf: String,
            key: Vec<u8>,
            expected: Vec<u8>,
        ) -> StorageFuture<'_, Result<bool, Error>> {
            self.0.compare_and_delete(cf, key, expected)
        }

        fn set(
            &self,
            cf: String,
            key: Vec<u8>,
            value: Vec<u8>,
        ) -> StorageFuture<'_, Result<(), Error>> {
            self.0.set(cf, key, value)
        }

        fn delete(&self, cf: String, key: Vec<u8>) -> StorageFuture<'_, Result<(), Error>> {
            self.0.delete(cf, key)
        }

        fn scan(&self, request: ScanRequest) -> StorageFuture<'_, Result<StorageScan<'_>, Error>> {
            self.0.scan(request)
        }

        fn write_many(
            &self,
            operations: Vec<OwnedWriteOperation>,
        ) -> StorageFuture<'_, Result<(), Error>> {
            self.0.write_many(operations)
        }
    }

    impl ReopenableStorage for NonEnumeratingStorage {
        fn reopen(
            self,
            column_families: Vec<String>,
        ) -> StorageFuture<'static, Result<Self, Error>> {
            Box::pin(async move { Ok(Self(self.0.reopen(column_families).await?)) })
        }
    }

    #[test]
    fn physical_storage_names_reject_nonportable_forms() {
        assert!(validate_physical_storage_name("rows").is_ok());
        assert!(validate_physical_storage_name("rows\0evil").is_err());
        assert!(
            validate_physical_storage_name(&"a".repeat(MAX_APPLICATION_STORAGE_NAME_BYTES + 1))
                .is_err()
        );
    }

    #[test]
    fn prefix_successor_is_the_exact_unsigned_lexicographic_bound() {
        assert_eq!(prefix_successor(&[0x12, 0xff]), Some(vec![0x13]));
        assert_eq!(prefix_successor(&[0xff, 0xff]), None);
    }
    use crate::records::{ScalarEnumSchema, Value, ValueType};
    use std::cell::Cell;
    use std::error::Error as _;

    #[test]
    fn record_errors_keep_display_and_source_after_storage_conversion() {
        let error = Error::from(crate::records::Error::InvalidOffset);

        assert_eq!(error.to_string(), "record error: invalid offset");
        assert_eq!(error.source().unwrap().to_string(), "invalid offset");
        assert!(matches!(
            error,
            Error::Record(source) if *source == crate::records::Error::InvalidOffset
        ));
    }

    async fn reverse_prefix_values<S: OrderedKvStorage>(
        storage: &S,
        cf: &str,
        prefix: &[u8],
    ) -> Result<Vec<KeyValue>, Error> {
        collect_scan(
            storage
                .scan(ScanRequest::prefix(cf.to_owned(), prefix.to_vec()).reversed())
                .await?,
        )
        .await
    }

    // This is deliberately an internal contract test: ordered scans are the
    // storage seam below every public query, and no application-level API can
    // observe whether a backend obeys the hard cursor bound.
    #[futures_test::test]
    async fn explicit_scan_request_preserves_bounds_direction_and_hard_limit() {
        let storage = MemoryStorage::new(&["records"]).expect("valid memory storage families");
        for key in [
            b"a/1".as_slice(),
            b"a/2".as_slice(),
            b"a/3".as_slice(),
            b"b/1".as_slice(),
        ] {
            storage
                .set("records".into(), key.to_vec(), key.to_vec())
                .await
                .unwrap();
        }

        let forward = collect_scan(
            storage
                .scan(ScanRequest::prefix("records".into(), b"a/".to_vec()).with_max_items(2))
                .await
                .unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(
            forward
                .iter()
                .map(|(key, _)| key.as_slice())
                .collect::<Vec<_>>(),
            vec![b"a/1".as_slice(), b"a/2".as_slice()]
        );

        let reverse = collect_scan(
            storage
                .scan(
                    ScanRequest::range("records".into(), b"a/1".to_vec(), b"b".to_vec())
                        .reversed()
                        .with_max_items(2),
                )
                .await
                .unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(
            reverse
                .iter()
                .map(|(key, _)| key.as_slice())
                .collect::<Vec<_>>(),
            vec![b"a/3".as_slice(), b"a/2".as_slice()]
        );

        let empty = collect_scan(
            storage
                .scan(ScanRequest::prefix("records".into(), Vec::new()).with_max_items(0))
                .await
                .unwrap(),
        )
        .await
        .unwrap();
        assert!(empty.is_empty());
        assert!(matches!(
            storage
                .scan(ScanRequest::prefix("missing".into(), Vec::new()).with_max_items(0))
                .await,
            Err(Error::ColumnFamilyNotFound(cf)) if cf == "missing"
        ));
    }

    // A wrapper must preserve the hard cursor boundary too: once a bounded
    // scan has produced its final item, later backend batch failures are not
    // observable because it must not ask the backend for another batch.
    #[futures_test::test]
    async fn bounded_scan_stops_before_a_later_wrapped_batch_failure() {
        let (storage, control) = TestStorage::controlled(&["records"]);
        for key in [b"a/1".as_slice(), b"a/2".as_slice(), b"a/3".as_slice()] {
            storage
                .set("records".into(), key.to_vec(), key.to_vec())
                .await
                .unwrap();
        }
        control.take_observed();

        let mut scan = storage
            .scan(ScanRequest::prefix("records".into(), b"a/".to_vec()).with_max_items(1))
            .await
            .unwrap();
        assert_eq!(
            scan.next_batch().await.unwrap(),
            Some(vec![(b"a/1".to_vec(), b"a/1".to_vec())])
        );
        control.take_observed();

        control.fail_next(TestStorageOperation::ScanBatch);
        assert_eq!(scan.next_batch().await.unwrap(), None);
        assert!(
            control.take_observed().is_empty(),
            "the wrapper must not open a batch beyond the bounded cursor"
        );

        let mut next_scan = storage
            .scan(ScanRequest::prefix("records".into(), b"a/".to_vec()))
            .await
            .unwrap();
        assert!(matches!(
            next_scan.next_batch().await,
            Err(Error::Backend {
                backend: "test",
                ..
            })
        ));
    }

    fn complex_record_descriptor_and_values() -> (RecordDescriptor, Vec<Value>) {
        let status = ScalarEnumSchema::new("status", ["draft", "ready", "done"]).unwrap();
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
                ("enum", ValueType::EnumTag(status)),
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
                Value::EnumTag(1),
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

    #[futures_test::test]
    async fn record_store_round_trips_exhaustive_record_descriptor() {
        let storage = MemoryStorage::new(&["records"]).expect("valid memory storage families");
        let (descriptor, values) = complex_record_descriptor_and_values();
        let raw = descriptor.create(&values).unwrap();
        storage
            .set("records".into(), b"row:1".to_vec(), raw.clone())
            .await
            .unwrap();
        let store = RecordStore::new(&storage, "records", &descriptor);

        let record = store.get(b"row:1").await.unwrap().unwrap();
        assert_eq!(record.to_values().unwrap(), values);
        assert_eq!(store.get_raw(b"row:1").await.unwrap().unwrap(), raw);

        let prefix = store.prefix(b"row:").await.unwrap();
        assert_eq!(prefix, vec![(b"row:1".to_vec(), raw.clone())]);
        let ranged = store.range(b"row:", b"row;").await.unwrap();
        assert_eq!(ranged, vec![(b"row:1".to_vec(), raw)]);
    }

    #[futures_test::test]
    async fn class_layout_keeps_logical_keys_isolated_inside_shared_physical_cf() {
        let physical_cfs = StorageLayout::jazz_class_v1().physical_column_families([
            "jazz_albums_history",
            "jazz_tracks_history",
            "jazz_albums_register",
        ]);
        let refs = physical_cfs.iter().map(String::as_str).collect::<Vec<_>>();
        let storage = LayoutStorage::new(
            MemoryStorage::new(&refs).expect("valid memory storage families"),
            StorageLayout::jazz_class_v1(),
        )
        .await
        .unwrap();

        storage
            .set(
                "jazz_albums_history".into(),
                b"row:1".to_vec(),
                b"album-one".to_vec(),
            )
            .await
            .unwrap();
        storage
            .set(
                "jazz_albums_history".into(),
                b"row:3".to_vec(),
                b"album-three".to_vec(),
            )
            .await
            .unwrap();
        storage
            .set(
                "jazz_tracks_history".into(),
                b"row:2".to_vec(),
                b"track-two".to_vec(),
            )
            .await
            .unwrap();
        storage
            .set(
                "jazz_albums_register".into(),
                b"row:2".to_vec(),
                b"album-register".to_vec(),
            )
            .await
            .unwrap();

        assert_eq!(
            storage
                .prefix("jazz_albums_history".into(), b"row:".to_vec())
                .await
                .unwrap(),
            vec![
                (b"row:1".to_vec(), b"album-one".to_vec()),
                (b"row:3".to_vec(), b"album-three".to_vec()),
            ]
        );
        assert_eq!(
            reverse_prefix_values(&storage, "jazz_albums_history", b"row:")
                .await
                .unwrap(),
            vec![
                (b"row:3".to_vec(), b"album-three".to_vec()),
                (b"row:1".to_vec(), b"album-one".to_vec()),
            ]
        );
        assert_eq!(
            storage
                .last_with_prefix("jazz_albums_history".into(), b"row:".to_vec())
                .await
                .unwrap(),
            Some((b"row:3".to_vec(), b"album-three".to_vec()))
        );
        assert_eq!(
            storage
                .range(
                    "jazz_albums_history".into(),
                    b"row:1".to_vec(),
                    b"row:4".to_vec()
                )
                .await
                .unwrap(),
            vec![
                (b"row:1".to_vec(), b"album-one".to_vec()),
                (b"row:3".to_vec(), b"album-three".to_vec()),
            ]
        );
    }

    #[futures_test::test]
    async fn class_layout_isolates_every_jazz_physical_class() {
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
            ("jazz_nodes", "jazz_transactions"),
        ];
        let all_logical = logical_cfs
            .iter()
            .flat_map(|(left, right)| [*left, *right])
            .collect::<Vec<_>>();
        let layout = StorageLayout::jazz_class_v1();
        let physical_cfs = layout.physical_column_families(all_logical.iter().copied());
        let refs = physical_cfs.iter().map(String::as_str).collect::<Vec<_>>();
        let storage = LayoutStorage::new(
            MemoryStorage::new(&refs).expect("valid memory storage families"),
            layout,
        )
        .await
        .unwrap();

        for (left, right) in logical_cfs {
            storage
                .set(left.into(), b"k:1".to_vec(), left.as_bytes().to_vec())
                .await
                .unwrap();
            storage
                .set(right.into(), b"k:2".to_vec(), right.as_bytes().to_vec())
                .await
                .unwrap();

            assert_eq!(
                storage.prefix(left.into(), b"k:".to_vec()).await.unwrap(),
                vec![(b"k:1".to_vec(), left.as_bytes().to_vec())],
                "{left} must not read rows from {right}"
            );
            assert_eq!(
                reverse_prefix_values(&storage, right, b"k:").await.unwrap(),
                vec![(b"k:2".to_vec(), right.as_bytes().to_vec())],
                "{right} reverse scan must not read rows from {left}"
            );
            assert_eq!(
                storage
                    .last_with_prefix(left.into(), b"k:".to_vec())
                    .await
                    .unwrap(),
                Some((b"k:1".to_vec(), left.as_bytes().to_vec())),
                "{left} last_with_prefix must stay within its logical prefix"
            );
        }
    }

    #[futures_test::test]
    async fn class_layout_rejects_missing_marker_with_legacy_mapped_families() {
        let storage = MemoryStorage::new(&["__groove_class_meta", "jazz_albums_history"])
            .expect("valid memory storage families");
        assert!(matches!(
            LayoutStorage::new(storage, StorageLayout::jazz_class_v1()).await,
            Err(Error::InvalidStorageLayout(_))
        ));
    }

    #[futures_test::test]
    async fn class_layout_accepts_truly_empty_store_and_writes_marker() {
        let storage = MemoryStorage::new(&["__groove_class_meta", "__groove_class_history"])
            .expect("valid memory storage families");
        let storage = LayoutStorage::new(storage, StorageLayout::jazz_class_v1())
            .await
            .unwrap();
        assert_eq!(
            storage
                .inner
                .get(
                    "__groove_class_meta".into(),
                    CLASS_LAYOUT_MARKER_KEY.to_vec()
                )
                .await
                .unwrap(),
            Some(CLASS_LAYOUT_MARKER_VALUE.to_vec())
        );
    }

    #[futures_test::test]
    async fn class_layout_marker_and_mapped_key_receipt_is_exact() {
        let logical_cf = "jazz_albums_history";
        let physical_cf = "__groove_class_history";
        let raw =
            MemoryStorage::new(&["__groove_class_meta", physical_cf, "__groove_class_indices"])
                .expect("valid physical class families");
        let storage = LayoutStorage::new(raw.clone(), StorageLayout::jazz_class_v1())
            .await
            .expect("fresh class layout initializes its one marker");

        storage
            .set(logical_cf.into(), b"row\0key".to_vec(), b"value".to_vec())
            .await
            .unwrap();

        assert_eq!(
            raw.get(
                "__groove_class_meta".into(),
                b"groove-storage-layout".to_vec()
            )
            .await
            .unwrap(),
            Some(b"class-cf-v1".to_vec()),
            "the marker has one frozen physical key and value"
        );
        let mut expected_key = (logical_cf.len() as u32).to_be_bytes().to_vec();
        expected_key.extend_from_slice(logical_cf.as_bytes());
        expected_key.extend_from_slice(b"row\0key");
        assert_eq!(
            raw.get(physical_cf.into(), expected_key).await.unwrap(),
            Some(b"value".to_vec()),
            "mapped keys are exactly u32be(UTF-8 logical-CF length) | logical-CF | key"
        );

        storage
            .set(
                "indices".into(),
                b"existing-index-key".to_vec(),
                b"index-value".to_vec(),
            )
            .await
            .unwrap();
        assert_eq!(
            storage
                .prefix("indices".into(), b"existing-".to_vec())
                .await
                .unwrap(),
            vec![(b"existing-index-key".to_vec(), b"index-value".to_vec())],
            "the public index path must retain its pre-existing logical key"
        );
        let mut expected_index_key = ("indices".len() as u32).to_be_bytes().to_vec();
        expected_index_key.extend_from_slice(b"indicesexisting-index-key");
        assert_eq!(
            raw.get("__groove_class_indices".into(), expected_index_key)
                .await
                .unwrap(),
            Some(b"index-value".to_vec()),
            "indices use the same one class framing, not a second table prefix"
        );

        assert_eq!(
            jazz_physical_class("jazz_album_history"),
            Some(CLASS_HISTORY_CF)
        );
        assert_eq!(
            jazz_physical_class("jazz_album_register"),
            Some(CLASS_REGISTER_CF)
        );
        assert_eq!(
            jazz_physical_class("jazz_album_register_global_current"),
            Some(CLASS_GLOBAL_CURRENT_CF)
        );
        assert_eq!(
            jazz_physical_class("jazz_album_register_ahead_current"),
            Some(CLASS_AHEAD_CURRENT_CF)
        );
        assert_eq!(
            jazz_physical_class("jazz_global_changes"),
            Some(CLASS_CHANGES_CF)
        );
        assert_eq!(jazz_physical_class("indices"), Some(CLASS_INDICES_CF));
        assert_eq!(jazz_physical_class("jazz_catalogue"), Some(CLASS_META_CF));
        assert_eq!(jazz_physical_class("application_rows"), None);
    }

    #[futures_test::test]
    async fn class_layout_rejects_unknown_old_future_and_malformed_markers_before_logical_access() {
        for invalid_marker in [
            b"".as_slice(),
            b"class-cf-v0".as_slice(),
            b"class-cf-v2".as_slice(),
            b"class-cf-v1\0".as_slice(),
        ] {
            let raw = MemoryStorage::new(&["__groove_class_meta", "__groove_class_history"])
                .expect("valid class families");
            raw.set(
                "__groove_class_meta".into(),
                CLASS_LAYOUT_MARKER_KEY.to_vec(),
                invalid_marker.to_vec(),
            )
            .await
            .unwrap();
            raw.set(
                "__groove_class_history".into(),
                b"existing-physical-data".to_vec(),
                b"must-not-be-read".to_vec(),
            )
            .await
            .unwrap();

            assert!(matches!(
                LayoutStorage::new(raw.clone(), StorageLayout::jazz_class_v1()).await,
                Err(Error::InvalidStorageLayout(_))
            ));
            assert_eq!(
                raw.get(
                    "__groove_class_meta".into(),
                    CLASS_LAYOUT_MARKER_KEY.to_vec()
                )
                .await
                .unwrap(),
                Some(invalid_marker.to_vec()),
                "rejection must not normalize or replace {invalid_marker:?}"
            );
            assert_eq!(
                raw.get(
                    "__groove_class_history".into(),
                    b"existing-physical-data".to_vec()
                )
                .await
                .unwrap(),
                Some(b"must-not-be-read".to_vec()),
                "rejection occurs before logical access or mutation"
            );
        }
    }

    #[futures_test::test]
    async fn class_layout_requires_an_enumerable_catalogue_before_writing_its_marker() {
        let raw = MemoryStorage::new(&["__groove_class_meta", "__groove_class_history"])
            .expect("valid class families");
        assert!(matches!(
            LayoutStorage::new(
                NonEnumeratingStorage(raw.clone()),
                StorageLayout::jazz_class_v1()
            )
            .await,
            Err(Error::InvalidStorageLayout(_))
        ));
        assert_eq!(
            raw.get(
                "__groove_class_meta".into(),
                CLASS_LAYOUT_MARKER_KEY.to_vec()
            )
            .await
            .unwrap(),
            None,
            "an uninspectable legacy store must not acquire a V1 marker"
        );
    }

    #[futures_test::test]
    async fn class_layout_rejects_invalid_logical_names_before_key_framing() {
        let raw = MemoryStorage::new(&["__groove_class_meta", "__groove_class_history"])
            .expect("valid class families");
        let storage = LayoutStorage::new(raw.clone(), StorageLayout::jazz_class_v1())
            .await
            .unwrap();
        let overlong = format!(
            "jazz_{}_history",
            "x".repeat(MAX_APPLICATION_STORAGE_NAME_BYTES)
        );
        for invalid in ["jazz_bad\0_history".to_owned(), overlong] {
            assert!(matches!(
                storage
                    .set(invalid, b"key".to_vec(), b"value".to_vec())
                    .await,
                Err(Error::InvalidStorageLayout(_))
            ));
        }
        assert_eq!(
            raw.prefix("__groove_class_history".into(), Vec::new())
                .await
                .unwrap(),
            Vec::<KeyValue>::new(),
            "invalid names fail before a framed durable key is written"
        );
    }

    #[futures_test::test]
    async fn class_layout_maps_every_classifier_match_and_preserves_unmapped_missing_cf_errors() {
        let layout = StorageLayout::jazz_class_v1();
        let physical_cfs = layout.physical_column_families(["jazz_albums_history"]);
        let refs = physical_cfs.iter().map(String::as_str).collect::<Vec<_>>();
        let storage = LayoutStorage::new(
            MemoryStorage::new(&refs).expect("valid memory storage families"),
            layout,
        )
        .await
        .unwrap();

        storage
            .set(
                "jazz_albums_history".into(),
                b"row:1".to_vec(),
                b"album-one".to_vec(),
            )
            .await
            .unwrap();
        // This family was not used to compute the physical open set. V1 still
        // maps it into the existing history class rather than interpreting it
        // as its own legacy logical CF under the same marker.
        storage
            .set(
                "jazz_tracks_history".into(),
                b"row:2".to_vec(),
                b"track-two".to_vec(),
            )
            .await
            .unwrap();
        assert_eq!(
            storage
                .get("jazz_tracks_history".into(), b"row:2".to_vec())
                .await
                .unwrap(),
            Some(b"track-two".to_vec())
        );
        assert!(matches!(
            storage
                .get("application_rows".into(), b"row:1".to_vec())
                .await,
            Err(Error::ColumnFamilyNotFound(_))
        ));
    }

    #[futures_test::test]
    async fn get_set_and_delete_values() {
        let storage = MemoryStorage::new(&["records"]).expect("valid memory storage families");

        storage
            .set("records".into(), b"a".to_vec(), b"one".to_vec())
            .await
            .unwrap();
        assert_eq!(
            storage.get("records".into(), b"a".to_vec()).await.unwrap(),
            Some(b"one".to_vec())
        );

        storage
            .delete("records".into(), b"a".to_vec())
            .await
            .unwrap();
        assert_eq!(
            storage.get("records".into(), b"a".to_vec()).await.unwrap(),
            None
        );
    }

    #[futures_test::test]
    async fn memory_test_store_keeps_writes_enabled() {
        let storage = MemoryStorage::new(&["records"]).expect("valid memory storage families");

        storage
            .set("records".into(), b"a".to_vec(), b"one".to_vec())
            .await
            .unwrap();

        assert_eq!(
            storage.get("records".into(), b"a".to_vec()).await.unwrap(),
            Some(b"one".to_vec())
        );
    }

    #[futures_test::test]
    async fn range_returns_ordered_values_between_start_and_end() {
        let storage = MemoryStorage::new(&["records"]).expect("valid memory storage families");

        storage
            .set("records".into(), b"a".to_vec(), b"one".to_vec())
            .await
            .unwrap();
        storage
            .set("records".into(), b"b".to_vec(), b"two".to_vec())
            .await
            .unwrap();
        storage
            .set("records".into(), b"c".to_vec(), b"three".to_vec())
            .await
            .unwrap();

        assert_eq!(
            storage
                .range("records".into(), b"a".to_vec(), b"c".to_vec())
                .await
                .unwrap(),
            vec![
                (b"a".to_vec(), b"one".to_vec()),
                (b"b".to_vec(), b"two".to_vec())
            ]
        );
    }

    #[futures_test::test]
    async fn prefix_returns_ordered_values_with_matching_prefix() {
        let storage = MemoryStorage::new(&["records"]).expect("valid memory storage families");

        storage
            .set("records".into(), b"user:1".to_vec(), b"a".to_vec())
            .await
            .unwrap();
        storage
            .set("records".into(), b"user:2".to_vec(), b"b".to_vec())
            .await
            .unwrap();
        storage
            .set("records".into(), b"view:1".to_vec(), b"c".to_vec())
            .await
            .unwrap();

        assert_eq!(
            storage
                .prefix("records".into(), b"user:".to_vec())
                .await
                .unwrap(),
            vec![
                (b"user:1".to_vec(), b"a".to_vec()),
                (b"user:2".to_vec(), b"b".to_vec())
            ]
        );
    }

    #[futures_test::test]
    async fn prefix_handles_prefixes_without_a_finite_upper_bound() {
        let storage = MemoryStorage::new(&["records"]).expect("valid memory storage families");

        storage
            .set("records".into(), vec![0xfe], b"before".to_vec())
            .await
            .unwrap();
        storage
            .set("records".into(), vec![0xff, 0x00], b"a".to_vec())
            .await
            .unwrap();
        storage
            .set("records".into(), vec![0xff, 0x01], b"b".to_vec())
            .await
            .unwrap();

        assert_eq!(
            storage.prefix("records".into(), vec![0xff]).await.unwrap(),
            vec![
                (vec![0xff, 0x00], b"a".to_vec()),
                (vec![0xff, 0x01], b"b".to_vec())
            ]
        );
    }

    #[futures_test::test]
    async fn direct_operations_report_missing_column_families() {
        let storage = MemoryStorage::new(&["records"]).expect("valid memory storage families");

        assert!(matches!(
            storage.get("missing".into(), b"a".to_vec()).await,
            Err(Error::ColumnFamilyNotFound(cf)) if cf == "missing"
        ));
        assert!(matches!(
            storage.set("missing".into(), b"a".to_vec(), b"one".to_vec()).await,
            Err(Error::ColumnFamilyNotFound(cf)) if cf == "missing"
        ));
        assert!(matches!(
            storage.delete("missing".into(), b"a".to_vec()).await,
            Err(Error::ColumnFamilyNotFound(cf)) if cf == "missing"
        ));
        assert!(matches!(
            storage.range("missing".into(), b"a".to_vec(), b"z".to_vec()).await,
            Err(Error::ColumnFamilyNotFound(cf)) if cf == "missing"
        ));
        assert!(matches!(
            storage.prefix("missing".into(), b"a".to_vec()).await,
            Err(Error::ColumnFamilyNotFound(cf)) if cf == "missing"
        ));
        assert!(matches!(
            storage.scan(ScanRequest::range("missing".into(), b"a".to_vec(), b"z".to_vec())).await,
            Err(Error::ColumnFamilyNotFound(cf)) if cf == "missing"
        ));
        assert!(matches!(
            storage.scan(ScanRequest::prefix("missing".into(), b"a".to_vec())).await,
            Err(Error::ColumnFamilyNotFound(cf)) if cf == "missing"
        ));
    }

    // This is deliberately an internal contract test: only the ordered-KV
    // seam knows whether a backend acknowledgement proves that an atomic
    // batch was not committed. Higher-level APIs must receive that proof, not
    // infer it from an arbitrary backend error string.
    #[futures_test::test]
    async fn write_many_outcome_default_is_conservative_after_an_error() {
        let (storage, control) = TestStorage::controlled(&["records"]);
        control.fail_next(TestStorageOperation::WriteMany);

        let outcome = storage
            .write_many_outcome(vec![OwnedWriteOperation::set("records", b"key", b"value")])
            .await;

        assert!(matches!(&outcome, WriteManyOutcome::PossiblyCommitted(_)));
        assert!(outcome.may_have_committed());
        assert!(!outcome.is_committed());
    }

    #[futures_test::test]
    async fn memory_write_many_outcome_proves_prevalidation_errors_uncommitted() {
        let storage = MemoryStorage::new(&["records"]).expect("valid memory storage families");

        let outcome = storage
            .write_many_outcome(vec![OwnedWriteOperation::set("missing", b"key", b"value")])
            .await;

        assert!(matches!(
            &outcome,
            WriteManyOutcome::Uncommitted(Error::ColumnFamilyNotFound(cf)) if cf == "missing"
        ));
        assert!(!outcome.may_have_committed());
    }

    #[futures_test::test]
    async fn layout_storage_preserves_backend_commit_classification() {
        let storage = LayoutStorage::new(
            MemoryStorage::new(&["records"]).expect("valid memory storage families"),
            StorageLayout::Identity,
        )
        .await
        .unwrap();

        let outcome = storage
            .write_many_outcome(vec![OwnedWriteOperation::set("missing", b"key", b"value")])
            .await;

        assert!(matches!(
            outcome,
            WriteManyOutcome::Uncommitted(Error::ColumnFamilyNotFound(cf)) if cf == "missing"
        ));
    }

    #[futures_test::test]
    async fn scans_visit_ordered_values_without_materializing_in_storage_api() {
        let storage = MemoryStorage::new(&["records"]).expect("valid memory storage families");

        storage
            .set("records".into(), b"a".to_vec(), b"one".to_vec())
            .await
            .unwrap();
        storage
            .set("records".into(), b"b".to_vec(), b"two".to_vec())
            .await
            .unwrap();
        storage
            .set("records".into(), b"c".to_vec(), b"three".to_vec())
            .await
            .unwrap();

        let visited = collect_scan(
            storage
                .scan(ScanRequest::range(
                    "records".into(),
                    b"a".to_vec(),
                    b"c".to_vec(),
                ))
                .await
                .unwrap(),
        )
        .await
        .unwrap();

        assert_eq!(
            visited,
            vec![
                (b"a".to_vec(), b"one".to_vec()),
                (b"b".to_vec(), b"two".to_vec())
            ]
        );
    }

    #[futures_test::test]
    async fn write_many_writes_all_operations_atomically() {
        let storage =
            MemoryStorage::new(&["records", "indices"]).expect("valid memory storage families");

        storage
            .write_many(vec![
                OwnedWriteOperation::set("records", b"1", b"record"),
                OwnedWriteOperation::set("indices", b"name:record", b"1"),
            ])
            .await
            .unwrap();

        assert_eq!(
            storage.get("records".into(), b"1".to_vec()).await.unwrap(),
            Some(b"record".to_vec())
        );
        assert_eq!(
            storage
                .get("indices".into(), b"name:record".to_vec())
                .await
                .unwrap(),
            Some(b"1".to_vec())
        );
    }

    #[futures_test::test]
    async fn staged_overlay_reads_staged_sets_and_deletes_before_base_storage() {
        let storage = MemoryStorage::new(&["indices"]).expect("valid memory storage families");
        storage
            .set("indices".into(), b"a".to_vec(), b"base-a".to_vec())
            .await
            .unwrap();
        storage
            .set("indices".into(), b"b".to_vec(), b"base-b".to_vec())
            .await
            .unwrap();
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
            overlay.get("indices".into(), b"a".to_vec()).await.unwrap(),
            Some(b"staged-a".to_vec())
        );
        assert_eq!(
            overlay.get("indices".into(), b"b".to_vec()).await.unwrap(),
            None
        );
        assert_eq!(
            overlay.get("indices".into(), b"c".to_vec()).await.unwrap(),
            Some(b"staged-c".to_vec())
        );
        assert_eq!(
            overlay.prefix("indices".into(), Vec::new()).await.unwrap(),
            vec![
                (b"a".to_vec(), b"staged-a".to_vec()),
                (b"c".to_vec(), b"staged-c".to_vec()),
            ]
        );
        assert_eq!(
            collect_scan(
                overlay
                    .scan(ScanRequest::prefix("indices".into(), Vec::new()).with_max_items(2))
                    .await
                    .unwrap(),
            )
            .await
            .unwrap(),
            vec![
                (b"a".to_vec(), b"staged-a".to_vec()),
                (b"c".to_vec(), b"staged-c".to_vec()),
            ],
            "the bounded merged scan applies staged override/delete/insert before its logical cap"
        );
        assert_eq!(
            storage.get("indices".into(), b"a".to_vec()).await.unwrap(),
            Some(b"base-a".to_vec())
        );
    }

    #[futures_test::test]
    async fn bounded_transaction_scan_stops_base_cursor_after_logical_output_is_full() {
        let (storage, control) = TestStorage::controlled(&["records"]);
        for key in [b"a".as_slice(), b"b".as_slice(), b"c".as_slice()] {
            storage
                .set("records".into(), key.to_vec(), key.to_vec())
                .await
                .unwrap();
        }
        control.take_observed();

        let transaction = storage.begin_txn();
        transaction
            .delete("records".into(), b"a".to_vec())
            .await
            .unwrap();
        let mut scan = transaction
            .scan(ScanRequest::prefix("records".into(), Vec::new()).with_max_items(1))
            .await
            .unwrap();
        assert_eq!(
            scan.next_batch().await.unwrap(),
            Some(vec![(b"b".to_vec(), b"b".to_vec())]),
            "a staged delete must not under-fill the logical limit"
        );
        control.take_observed();

        control.fail_next(TestStorageOperation::ScanBatch);
        assert_eq!(scan.next_batch().await.unwrap(), None);
        assert!(
            control.take_observed().is_empty(),
            "a filled overlay limit must not pull another base batch"
        );
    }

    #[futures_test::test]
    async fn overlay_limit_caps_physical_memory_entries_after_staged_deletes() {
        let storage = MemoryStorage::new(&["records"]).expect("valid memory storage families");
        for index in 0..300 {
            let key = format!("row:{index:03}").into_bytes();
            storage
                .set("records".into(), key.clone(), key)
                .await
                .unwrap();
        }
        assert_eq!(storage.take_scan_entries_materialized(), 0);

        let forward = storage.begin_txn();
        forward
            .write_many(vec![
                OwnedWriteOperation::delete("records", b"row:000"),
                OwnedWriteOperation::set("records", b"row:001", b"forward-override"),
            ])
            .await
            .unwrap();
        assert_eq!(
            collect_scan(
                forward
                    .scan(
                        ScanRequest::prefix("records".into(), b"row:".to_vec())
                            .with_max_items(1),
                    )
                    .await
                    .unwrap(),
            )
            .await
            .unwrap(),
            vec![(b"row:001".to_vec(), b"forward-override".to_vec())]
        );
        assert_eq!(
            storage.take_scan_entries_materialized(),
            2,
            "limit one plus one staged delete must not hydrate Memory's default 256-entry batch"
        );

        let reverse = storage.begin_txn();
        reverse
            .write_many(vec![
                OwnedWriteOperation::delete("records", b"row:299"),
                OwnedWriteOperation::set("records", b"row:298", b"reverse-override"),
            ])
            .await
            .unwrap();
        assert_eq!(
            collect_scan(
                reverse
                    .scan(
                        ScanRequest::prefix("records".into(), b"row:".to_vec())
                            .reversed()
                            .with_max_items(1),
                    )
                    .await
                    .unwrap(),
            )
            .await
            .unwrap(),
            vec![(b"row:298".to_vec(), b"reverse-override".to_vec())]
        );
        assert_eq!(storage.take_scan_entries_materialized(), 2);
    }

    // Internal receipt: the regression is work performed inside the storage overlay and is not
    // observable through a higher-level public API except as elapsed time.
    #[futures_test::test]
    #[ignore = "#1787: manual scaling receipt for narrow reads over a large staged transaction"]
    async fn staged_overlay_narrow_scan_scaling_receipt() {
        const UNRELATED_ROWS: usize = 20_000;
        const REPETITIONS: usize = 20;

        let storage = MemoryStorage::new(&["records"]).expect("valid memory storage families");
        let transaction = StorageTransaction::new(&storage);
        let payload = vec![7; 512];
        transaction.stage_owned_operations((0..UNRELATED_ROWS).map(|index| {
            OwnedWriteOperation::set(
                "records",
                format!("unrelated:{index:05}").as_bytes(),
                payload.clone(),
            )
        }));
        transaction.stage_owned_operations((0..10).map(|index| {
            OwnedWriteOperation::set(
                "records",
                format!("target:{index:05}").as_bytes(),
                index.to_string().as_bytes(),
            )
        }));

        let whole_snapshot_started = std::time::Instant::now();
        for _ in 0..(REPETITIONS * 5) {
            std::hint::black_box(transaction.staged_writes.borrow().operations.clone());
        }
        let whole_snapshot_elapsed = whole_snapshot_started.elapsed();

        let started = std::time::Instant::now();
        for _ in 0..REPETITIONS {
            let range = collect_scan(
                transaction
                    .scan(ScanRequest::range(
                        "records".into(),
                        b"target:".to_vec(),
                        b"target;".to_vec(),
                    ))
                    .await
                    .unwrap(),
            )
            .await
            .unwrap();
            assert_eq!(range.len(), 10);

            let prefix = collect_scan(
                transaction
                    .scan(ScanRequest::prefix("records".into(), b"target:".to_vec()))
                    .await
                    .unwrap(),
            )
            .await
            .unwrap();
            assert_eq!(prefix.len(), 10);

            let reverse = collect_scan(
                transaction
                    .scan(ScanRequest::prefix("records".into(), b"target:".to_vec()).reversed())
                    .await
                    .unwrap(),
            )
            .await
            .unwrap();
            assert_eq!(reverse.len(), 10);

            assert_eq!(
                transaction
                    .last_with_prefix("records".into(), b"target:".to_vec())
                    .await
                    .unwrap()
                    .unwrap()
                    .0,
                b"target:00009"
            );
            assert_eq!(
                transaction
                    .last_with_prefix_before_or_at(
                        "records".into(),
                        b"target:".to_vec(),
                        b"target:00004".to_vec(),
                    )
                    .await
                    .unwrap()
                    .unwrap()
                    .0,
                b"target:00004"
            );
        }

        println!(
            "staged_overlay_narrow_scan_scaling_receipt unrelated_rows={UNRELATED_ROWS} repetitions={REPETITIONS} removed_whole_snapshot_ms={:.3} five_filtered_read_shapes_ms={:.3}",
            whole_snapshot_elapsed.as_secs_f64() * 1000.0,
            started.elapsed().as_secs_f64() * 1000.0
        );
    }

    #[futures_test::test]
    async fn storage_transaction_reads_own_writes_and_commits_atomically() {
        let storage = MemoryStorage::new(&["records"]).expect("valid memory storage families");
        storage
            .set("records".into(), b"a".to_vec(), b"base-a".to_vec())
            .await
            .unwrap();
        storage
            .set("records".into(), b"b".to_vec(), b"base-b".to_vec())
            .await
            .unwrap();

        let txn = storage.begin_txn();
        txn.set("records".into(), b"a".to_vec(), b"txn-a".to_vec())
            .await
            .unwrap();
        txn.delete("records".into(), b"b".to_vec()).await.unwrap();
        txn.set("records".into(), b"c".to_vec(), b"txn-c".to_vec())
            .await
            .unwrap();

        assert_eq!(
            txn.get("records".into(), b"a".to_vec()).await.unwrap(),
            Some(b"txn-a".to_vec())
        );
        assert_eq!(
            txn.get("records".into(), b"b".to_vec()).await.unwrap(),
            None
        );
        assert_eq!(
            txn.get("records".into(), b"c".to_vec()).await.unwrap(),
            Some(b"txn-c".to_vec())
        );
        assert_eq!(
            txn.prefix("records".into(), Vec::new()).await.unwrap(),
            vec![
                (b"a".to_vec(), b"txn-a".to_vec()),
                (b"c".to_vec(), b"txn-c".to_vec()),
            ]
        );

        assert_eq!(
            storage.get("records".into(), b"a".to_vec()).await.unwrap(),
            Some(b"base-a".to_vec())
        );
        assert_eq!(
            storage.get("records".into(), b"b".to_vec()).await.unwrap(),
            Some(b"base-b".to_vec())
        );
        assert_eq!(
            storage.get("records".into(), b"c".to_vec()).await.unwrap(),
            None
        );

        txn.commit().await.unwrap();

        assert_eq!(
            storage.get("records".into(), b"a".to_vec()).await.unwrap(),
            Some(b"txn-a".to_vec())
        );
        assert_eq!(
            storage.get("records".into(), b"b".to_vec()).await.unwrap(),
            None
        );
        assert_eq!(
            storage.get("records".into(), b"c".to_vec()).await.unwrap(),
            Some(b"txn-c".to_vec())
        );
    }

    #[futures_test::test]
    async fn write_many_fails_without_writing_when_column_family_is_missing() {
        let storage = MemoryStorage::new(&["records"]).expect("valid memory storage families");

        let error = storage
            .write_many(vec![
                OwnedWriteOperation::set("records", b"1", b"record"),
                OwnedWriteOperation::set("missing", b"2", b"nope"),
            ])
            .await
            .unwrap_err();

        assert!(matches!(error, Error::ColumnFamilyNotFound(_)));
        assert_eq!(
            storage.get("records".into(), b"1".to_vec()).await.unwrap(),
            None
        );
    }

    #[futures_test::test]
    async fn write_many_can_mix_sets_and_deletes_atomically() {
        let storage = MemoryStorage::new(&["records"]).expect("valid memory storage families");

        storage
            .set("records".into(), b"old".to_vec(), b"value".to_vec())
            .await
            .unwrap();
        storage
            .write_many(vec![
                OwnedWriteOperation::set("records", b"new", b"value"),
                OwnedWriteOperation::delete("records", b"old"),
            ])
            .await
            .unwrap();

        assert_eq!(
            storage
                .get("records".into(), b"new".to_vec())
                .await
                .unwrap(),
            Some(b"value".to_vec())
        );
        assert_eq!(
            storage
                .get("records".into(), b"old".to_vec())
                .await
                .unwrap(),
            None
        );
    }

    #[futures_test::test]
    async fn memory_storage_orders_scans_and_errors_on_missing_column_families() {
        let storage = MemoryStorage::new(&["records"]).expect("valid memory storage families");
        storage
            .set("records".into(), b"b".to_vec(), b"two".to_vec())
            .await
            .unwrap();
        storage
            .set("records".into(), b"a".to_vec(), b"one".to_vec())
            .await
            .unwrap();
        storage
            .set("records".into(), b"aa".to_vec(), b"one-one".to_vec())
            .await
            .unwrap();

        assert!(matches!(
            storage.get("missing".into(), b"a".to_vec()).await,
            Err(Error::ColumnFamilyNotFound(_))
        ));
        assert!(matches!(
            storage
                .set("missing".into(), b"a".to_vec(), b"one".to_vec())
                .await,
            Err(Error::ColumnFamilyNotFound(_))
        ));

        let range = collect_scan(
            storage
                .scan(ScanRequest::range(
                    "records".into(),
                    b"a".to_vec(),
                    b"b".to_vec(),
                ))
                .await
                .unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(
            range,
            vec![
                (b"a".to_vec(), b"one".to_vec()),
                (b"aa".to_vec(), b"one-one".to_vec())
            ]
        );

        let prefix = collect_scan(
            storage
                .scan(ScanRequest::prefix("records".into(), b"a".to_vec()))
                .await
                .unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(
            prefix,
            vec![
                (b"a".to_vec(), b"one".to_vec()),
                (b"aa".to_vec(), b"one-one".to_vec())
            ]
        );
    }

    #[futures_test::test]
    async fn staged_overlay_reverse_prefix_scans_match_trait_default() {
        struct DefaultReverse<'a, S>(&'a StagedWriteOverlay<'a, S>);

        impl<S> OrderedKvStorage for DefaultReverse<'_, S>
        where
            S: OrderedKvStorage,
        {
            fn scan(
                &self,
                request: ScanRequest,
            ) -> StorageFuture<'_, Result<StorageScan<'_>, Error>> {
                self.0.scan(request)
            }

            fn get(
                &self,
                cf: String,
                key: Vec<u8>,
            ) -> StorageFuture<'_, Result<Option<Vec<u8>>, Error>> {
                self.0.get(cf, key)
            }

            fn put_if_absent(
                &self,
                cf: String,
                key: Vec<u8>,
                value: Vec<u8>,
            ) -> StorageFuture<'_, Result<Option<Vec<u8>>, Error>> {
                self.0.put_if_absent(cf, key, value)
            }

            fn compare_and_delete(
                &self,
                cf: String,
                key: Vec<u8>,
                expected: Vec<u8>,
            ) -> StorageFuture<'_, Result<bool, Error>> {
                self.0.compare_and_delete(cf, key, expected)
            }

            fn set(
                &self,
                cf: String,
                key: Vec<u8>,
                value: Vec<u8>,
            ) -> StorageFuture<'_, Result<(), Error>> {
                self.0.set(cf, key, value)
            }

            fn delete(&self, cf: String, key: Vec<u8>) -> StorageFuture<'_, Result<(), Error>> {
                self.0.delete(cf, key)
            }

            fn write_many(
                &self,
                operations: Vec<OwnedWriteOperation>,
            ) -> StorageFuture<'_, Result<(), Error>> {
                self.0.write_many(operations)
            }
        }

        async fn assert_case(
            name: &str,
            base_rows: &[(&[u8], &[u8])],
            staged_rows: Vec<OwnedWriteOperation>,
            prefix: &[u8],
            expected: Vec<KeyValue>,
        ) {
            let storage = MemoryStorage::new(&["indices"]).expect("valid memory storage families");
            for (key, value) in base_rows {
                storage
                    .set("indices".into(), key.to_vec(), value.to_vec())
                    .await
                    .unwrap();
            }
            storage
                .set("indices".into(), b"view:1".to_vec(), b"base-view".to_vec())
                .await
                .unwrap();
            let staged = RefCell::new(StagedWriteState::from(staged_rows));
            let overlay = StagedWriteOverlay::new(&storage, &staged);
            let default_reverse = DefaultReverse(&overlay);

            let optimized = collect_scan(
                overlay
                    .scan(ScanRequest::prefix("indices".into(), prefix.to_vec()).reversed())
                    .await
                    .unwrap(),
            )
            .await
            .unwrap();

            let defaulted = collect_scan(
                default_reverse
                    .scan(ScanRequest::prefix("indices".into(), prefix.to_vec()).reversed())
                    .await
                    .unwrap(),
            )
            .await
            .unwrap();

            assert_eq!(optimized, defaulted, "{name}");
            assert_eq!(optimized, expected, "{name}");
            assert_eq!(
                overlay
                    .last_with_prefix("indices".into(), prefix.to_vec())
                    .await
                    .unwrap(),
                default_reverse
                    .last_with_prefix("indices".into(), prefix.to_vec())
                    .await
                    .unwrap(),
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
        )
        .await;

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
        )
        .await;

        assert_case(
            "empty staged buffer",
            &[(b"user:1", b"base-1"), (b"user:2", b"base-2")],
            Vec::new(),
            b"user:",
            vec![
                (b"user:2".to_vec(), b"base-2".to_vec()),
                (b"user:1".to_vec(), b"base-1".to_vec()),
            ],
        )
        .await;

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
        )
        .await;

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
        )
        .await;
    }

    #[futures_test::test]
    async fn staged_overlay_last_with_prefix_no_delete_uses_one_base_seek() {
        struct CountingStorage<S> {
            inner: S,
            scans: Cell<usize>,
        }

        impl<S> CountingStorage<S> {
            fn new(inner: S) -> Self {
                Self {
                    inner,
                    scans: Cell::new(0),
                }
            }
        }

        impl<S> OrderedKvStorage for CountingStorage<S>
        where
            S: OrderedKvStorage,
        {
            fn get(
                &self,
                cf: String,
                key: Vec<u8>,
            ) -> StorageFuture<'_, Result<Option<Vec<u8>>, Error>> {
                self.inner.get(cf, key)
            }

            fn put_if_absent(
                &self,
                cf: String,
                key: Vec<u8>,
                value: Vec<u8>,
            ) -> StorageFuture<'_, Result<Option<Vec<u8>>, Error>> {
                self.inner.put_if_absent(cf, key, value)
            }

            fn compare_and_delete(
                &self,
                cf: String,
                key: Vec<u8>,
                expected: Vec<u8>,
            ) -> StorageFuture<'_, Result<bool, Error>> {
                self.inner.compare_and_delete(cf, key, expected)
            }

            fn set(
                &self,
                cf: String,
                key: Vec<u8>,
                value: Vec<u8>,
            ) -> StorageFuture<'_, Result<(), Error>> {
                self.inner.set(cf, key, value)
            }

            fn delete(&self, cf: String, key: Vec<u8>) -> StorageFuture<'_, Result<(), Error>> {
                self.inner.delete(cf, key)
            }

            fn scan(
                &self,
                request: ScanRequest,
            ) -> StorageFuture<'_, Result<StorageScan<'_>, Error>> {
                self.scans.set(self.scans.get() + 1);
                self.inner.scan(request)
            }

            fn write_many(
                &self,
                operations: Vec<OwnedWriteOperation>,
            ) -> StorageFuture<'_, Result<(), Error>> {
                self.inner.write_many(operations)
            }
        }

        let storage = CountingStorage::new(
            MemoryStorage::new(&["indices"]).expect("valid memory storage families"),
        );
        storage
            .set("indices".into(), b"user:1".to_vec(), b"base-1".to_vec())
            .await
            .unwrap();
        storage
            .set("indices".into(), b"user:2".to_vec(), b"base-2".to_vec())
            .await
            .unwrap();
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
            overlay
                .last_with_prefix("indices".into(), b"user:".to_vec())
                .await
                .unwrap(),
            Some((b"user:3".to_vec(), b"staged-3".to_vec()))
        );
        assert_eq!(storage.scans.get(), 1);
    }

    #[futures_test::test]
    async fn memory_storage_write_many_validates_column_families_before_writing() {
        let storage = MemoryStorage::new(&["records"]).expect("valid memory storage families");
        let error = storage
            .write_many(vec![
                OwnedWriteOperation::set("records", b"1", b"record"),
                OwnedWriteOperation::set("missing", b"2", b"nope"),
            ])
            .await
            .unwrap_err();

        assert!(matches!(error, Error::ColumnFamilyNotFound(_)));
        assert_eq!(
            storage.get("records".into(), b"1".to_vec()).await.unwrap(),
            None
        );
    }

    #[futures_test::test]
    async fn memory_storage_conforms_to_order_and_atomic_batch_contract() {
        let storage = MemoryStorage::new(&["records"]).expect("valid memory storage families");
        conformance::persistence_order_and_batch_atomicity(storage).await;
    }

    #[futures_test::test]
    async fn memory_storage_reopen_adds_column_families_without_losing_data() {
        let storage = MemoryStorage::new(&["records"]).expect("valid memory storage families");
        conformance::reopen_preserves_data_and_adds_families(storage).await;
    }

    #[futures_test::test]
    async fn memory_storage_conditionals_are_atomic_and_aba_safe() {
        let storage = MemoryStorage::new(&["records"]).expect("valid memory storage families");
        conformance::atomic_conditionals_preserve_winners_and_reject_stale_deletes(storage.clone())
            .await;
        conformance::invalid_batch_is_proven_uncommitted(storage).await;
    }

    #[futures_test::test]
    async fn record_store_writes_and_reads_typed_records() {
        let storage = MemoryStorage::new(&["records"]).expect("valid memory storage families");
        let descriptor = RecordDescriptor::new([("id", ValueType::U64)]);
        let store = RecordStore::new(&storage, "records", &descriptor);
        let key = b"1".as_slice();
        let record = descriptor.create(&[Value::U64(42)]).unwrap();
        let op = store.set(key, &record);

        storage.write_many(vec![op]).await.unwrap();

        let stored = store.get(key).await.unwrap().unwrap();
        assert_eq!(stored.get_idx(0).unwrap(), Value::U64(42));
    }
}
