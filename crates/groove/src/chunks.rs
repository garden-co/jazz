//! Host-provided immutable chunk retrieval for interruptible evaluation.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::rc::Weak;
use std::task::{Poll, Waker};

use bytes::Bytes;
use thiserror::Error;

use crate::large_values::{ContentHash, Locator, StagedChunk, object_hash};
use crate::storage::{LayoutStorage, OrderedKvStorage};

/// Opaque retrieval identity paired with the hash Groove must verify.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ChunkRequest {
    pub object_hash: [u8; 32],
    pub locator: Locator,
}

/// Executor-local future returned by a chunk capability.
pub type ChunkFuture<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;

/// Policy-blind async immutable byte-KV implemented by host storage adapters.
/// Groove, not the backend, validates object hashes and orchestrates staging.
pub trait ChunkKvStorage {
    fn get_exact(
        &self,
        locator: Locator,
    ) -> ChunkFuture<'_, Result<Option<(ContentHash, Bytes)>, ChunkStorageError>>;

    /// Install one immutable mapping or return the mapping already present.
    fn put_if_absent(
        &self,
        locator: Locator,
        hash: ContentHash,
        bytes: Bytes,
    ) -> ChunkFuture<'_, Result<Option<(ContentHash, Bytes)>, ChunkStorageError>>;

    fn delete_exact(
        &self,
        locator: Locator,
        expected_hash: ContentHash,
    ) -> ChunkFuture<'_, Result<(), ChunkStorageError>>;
}

/// Groove-owned integrity and staging layer over a policy-blind byte KV.
///
/// Every batch's size and object hashes are mechanically prevalidated before
/// its first backend put. This layer cannot itself make separate blob puts and
/// metadata writes atomic; durable callers must journal every locator before
/// invoking it. Groove's database upload facade does so with pending-upload
/// metadata and per-node upload references.
pub struct ManagedChunkStorage {
    backend: Rc<dyn ChunkKvStorage>,
}

impl ManagedChunkStorage {
    pub fn new(backend: Rc<dyn ChunkKvStorage>) -> Self {
        Self { backend }
    }
}

impl ChunkStorage for ManagedChunkStorage {
    fn get(
        &self,
        locator: Locator,
        expected_hash: ContentHash,
    ) -> ChunkFuture<'_, Result<Bytes, ChunkStorageError>> {
        Box::pin(async move {
            let Some((hash, bytes)) = self.backend.get_exact(locator).await? else {
                return Err(ChunkStorageError::Unavailable);
            };
            if hash != expected_hash || object_hash(&bytes) != expected_hash {
                return Err(ChunkStorageError::Integrity);
            }
            Ok(bytes)
        })
    }

    fn stage(
        &self,
        chunks: Vec<StagedChunk>,
    ) -> ChunkFuture<'_, Result<crate::large_values::StagedLargeValueAccounting, ChunkStorageError>>
    {
        Box::pin(async move {
            // Check every mechanically-verifiable property before the first
            // put. A later malformed member must not leave an earlier member
            // durable without its upload metadata.
            for chunk in &chunks {
                if chunk.encoded.len() > crate::large_values::MAX_ENCODED_NODE_BYTES {
                    return Err(ChunkStorageError::Integrity);
                }
                if object_hash(&chunk.encoded) != chunk.node_ref.object_hash {
                    return Err(ChunkStorageError::Integrity);
                }
            }
            let mut accounting = crate::large_values::StagedLargeValueAccounting::default();
            for chunk in chunks {
                let encoded_len = chunk.encoded.len() as u64;
                let existing = self
                    .backend
                    .put_if_absent(
                        chunk.node_ref.locator,
                        chunk.node_ref.object_hash,
                        Bytes::from(chunk.encoded),
                    )
                    .await?;
                if let Some((hash, ref bytes)) = existing
                    && (hash != chunk.node_ref.object_hash
                        || object_hash(bytes) != chunk.node_ref.object_hash)
                {
                    return Err(ChunkStorageError::LocatorConflict);
                }
                if existing.is_none() {
                    accounting.encoded_bytes = accounting.encoded_bytes.saturating_add(encoded_len);
                    accounting.node_count = accounting.node_count.saturating_add(1);
                }
            }
            Ok(accounting)
        })
    }

    fn delete(
        &self,
        locator: Locator,
        expected_hash: ContentHash,
    ) -> ChunkFuture<'_, Result<(), ChunkStorageError>> {
        self.backend.delete_exact(locator, expected_hash)
    }
}

/// Policy-blind immutable byte storage owned and orchestrated by Groove.
pub trait ChunkStorage {
    fn get(
        &self,
        locator: Locator,
        expected_hash: ContentHash,
    ) -> ChunkFuture<'_, Result<Bytes, ChunkStorageError>>;

    /// Install immutable mappings. Equal restaging is idempotent. Callers must
    /// prevalidate the complete batch before invoking this capability; generic
    /// byte backends need not make the individual puts crash-atomic.
    fn stage(
        &self,
        chunks: Vec<StagedChunk>,
    ) -> ChunkFuture<'_, Result<crate::large_values::StagedLargeValueAccounting, ChunkStorageError>>;

    /// Delete one exact immutable mapping after Groove has durably proven it
    /// orphaned. A mismatched hash must not delete a reused locator.
    fn delete(
        &self,
        _locator: Locator,
        _expected_hash: ContentHash,
    ) -> ChunkFuture<'_, Result<(), ChunkStorageError>> {
        Box::pin(async {
            Err(ChunkStorageError::Backend(
                "chunk deletion is not implemented by this backend".to_owned(),
            ))
        })
    }
}

/// Cloneable Groove-owned local lookup service for Jazz's auxiliary transport.
/// It exposes exact reads but never the backend object or staging/deletion.
#[derive(Clone)]
pub struct LocalChunkReader {
    // Peer I/O pumps outlive a single in-memory `Database` facade: a
    // catalogue update can rebuild that facade over the same durable store
    // while existing browser/socket links remain attached. Keep the lookup
    // service stable and retarget its backend at that boundary instead of
    // leaving those links with OrderedChunkStorage's deliberately weak old
    // storage handle.
    storage: Rc<RefCell<Rc<dyn ChunkStorage>>>,
}

impl LocalChunkReader {
    pub(crate) fn new(storage: Rc<dyn ChunkStorage>) -> Self {
        Self {
            storage: Rc::new(RefCell::new(storage)),
        }
    }

    /// Retarget all clones of this local-only reader to the storage selected
    /// by a rebuilt database facade.
    ///
    /// This does not expose staging or deletion. It is intentionally separate
    /// from [`ChunkStorage`] so a peer transport can keep serving exact reads
    /// across a host-side runtime rebuild without extending the old storage
    /// lifetime.
    pub fn refresh_from(&self, replacement: &Self) {
        let storage = replacement.storage.borrow().clone();
        self.storage.replace(storage);
    }

    pub async fn get(
        &self,
        locator: Locator,
        expected_hash: ContentHash,
    ) -> Result<Bytes, ChunkStorageError> {
        // Do not retain the RefCell borrow across the asynchronous backend
        // operation: a catalogue rebuild may retarget this reader while an
        // unrelated peer request is in flight.
        let storage = self.storage.borrow().clone();
        storage.get(locator, expected_hash).await
    }
}

/// Transport fallback for an exact chunk absent from local Groove storage.
/// Jazz implements this with its peer protocol; Groove retains request,
/// verification, persistence, coalescing, and wakeup ownership.
pub trait MissingChunkResolver {
    fn resolve(&self, request: ChunkRequest) -> ChunkFuture<'_, Result<Bytes, ChunkError>>;
}

/// Groove-internal metadata hook invoked after a remotely resolved immutable
/// mapping is installed. It is policy-blind and never exposed to Jazz.
pub trait ChunkInstallObserver {
    fn installed(
        &self,
        node_ref: crate::large_values::NodeRef,
        encoded: Bytes,
    ) -> ChunkFuture<'_, Result<(), ChunkError>>;
}

#[derive(Clone, Default)]
pub struct NoopChunkInstallObserver;

impl ChunkInstallObserver for NoopChunkInstallObserver {
    fn installed(
        &self,
        _node_ref: crate::large_values::NodeRef,
        _encoded: Bytes,
    ) -> ChunkFuture<'_, Result<(), ChunkError>> {
        Box::pin(async { Ok(()) })
    }
}

#[derive(Clone, Default)]
pub struct UnavailableChunkResolver;

impl MissingChunkResolver for UnavailableChunkResolver {
    fn resolve(&self, _request: ChunkRequest) -> ChunkFuture<'_, Result<Bytes, ChunkError>> {
        Box::pin(async { Err(ChunkError::Unavailable) })
    }
}

impl<S> ChunkStorage for Rc<S>
where
    S: ChunkStorage + ?Sized,
{
    fn get(
        &self,
        locator: Locator,
        expected_hash: ContentHash,
    ) -> ChunkFuture<'_, Result<Bytes, ChunkStorageError>> {
        (**self).get(locator, expected_hash)
    }

    fn stage(
        &self,
        chunks: Vec<StagedChunk>,
    ) -> ChunkFuture<'_, Result<crate::large_values::StagedLargeValueAccounting, ChunkStorageError>>
    {
        (**self).stage(chunks)
    }

    fn delete(
        &self,
        locator: Locator,
        expected_hash: ContentHash,
    ) -> ChunkFuture<'_, Result<(), ChunkStorageError>> {
        (**self).delete(locator, expected_hash)
    }
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum ChunkStorageError {
    #[error("chunk is unavailable")]
    Unavailable,
    #[error("opaque locator already names different content")]
    LocatorConflict,
    #[error("staged chunk failed integrity validation")]
    Integrity,
    #[error("chunk storage failed: {0}")]
    Backend(String),
}

/// In-memory Groove chunk storage used by ephemeral databases and tests.
type MemoryChunks = BTreeMap<Locator, (ContentHash, Bytes)>;

#[derive(Clone, Default)]
pub struct MemoryChunkStorage {
    chunks: Rc<RefCell<MemoryChunks>>,
}

impl MemoryChunkStorage {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.chunks.borrow().len()
    }

    pub fn is_empty(&self) -> bool {
        self.chunks.borrow().is_empty()
    }
}

impl ChunkStorage for MemoryChunkStorage {
    fn get(
        &self,
        locator: Locator,
        expected_hash: ContentHash,
    ) -> ChunkFuture<'_, Result<Bytes, ChunkStorageError>> {
        Box::pin(async move {
            self.chunks
                .borrow()
                .get(&locator)
                .filter(|(hash, _)| *hash == expected_hash)
                .map(|(_, bytes)| bytes.clone())
                .ok_or(ChunkStorageError::Unavailable)
        })
    }

    fn stage(
        &self,
        chunks: Vec<StagedChunk>,
    ) -> ChunkFuture<'_, Result<crate::large_values::StagedLargeValueAccounting, ChunkStorageError>>
    {
        Box::pin(async move {
            let existing = self.chunks.borrow();
            for chunk in &chunks {
                if chunk.encoded.len() > crate::large_values::MAX_ENCODED_NODE_BYTES {
                    return Err(ChunkStorageError::Integrity);
                }
                if object_hash(&chunk.encoded) != chunk.node_ref.object_hash {
                    return Err(ChunkStorageError::Integrity);
                }
                if existing
                    .get(&chunk.node_ref.locator)
                    .is_some_and(|(hash, bytes)| {
                        *hash != chunk.node_ref.object_hash
                            || bytes.as_ref() != chunk.encoded.as_slice()
                    })
                {
                    return Err(ChunkStorageError::LocatorConflict);
                }
            }
            drop(existing);
            let mut stored = self.chunks.borrow_mut();
            let mut accounting = crate::large_values::StagedLargeValueAccounting::default();
            for chunk in chunks {
                if let std::collections::btree_map::Entry::Vacant(entry) =
                    stored.entry(chunk.node_ref.locator)
                {
                    accounting.encoded_bytes = accounting
                        .encoded_bytes
                        .saturating_add(chunk.encoded.len() as u64);
                    accounting.node_count = accounting.node_count.saturating_add(1);
                    entry.insert((chunk.node_ref.object_hash, Bytes::from(chunk.encoded)));
                }
            }
            Ok(accounting)
        })
    }

    fn delete(
        &self,
        locator: Locator,
        expected_hash: ContentHash,
    ) -> ChunkFuture<'_, Result<(), ChunkStorageError>> {
        Box::pin(async move {
            let mut chunks = self.chunks.borrow_mut();
            match chunks.get(&locator) {
                Some((hash, _)) if *hash == expected_hash => {
                    chunks.remove(&locator);
                    Ok(())
                }
                Some(_) => Err(ChunkStorageError::Integrity),
                None => Ok(()),
            }
        })
    }
}

impl ChunkKvStorage for MemoryChunkStorage {
    fn get_exact(
        &self,
        locator: Locator,
    ) -> ChunkFuture<'_, Result<Option<(ContentHash, Bytes)>, ChunkStorageError>> {
        Box::pin(async move { Ok(self.chunks.borrow().get(&locator).cloned()) })
    }

    fn put_if_absent(
        &self,
        locator: Locator,
        hash: ContentHash,
        bytes: Bytes,
    ) -> ChunkFuture<'_, Result<Option<(ContentHash, Bytes)>, ChunkStorageError>> {
        Box::pin(async move {
            let mut chunks = self.chunks.borrow_mut();
            if let Some(existing) = chunks.get(&locator) {
                return Ok(Some(existing.clone()));
            }
            chunks.insert(locator, (hash, bytes));
            Ok(None)
        })
    }

    fn delete_exact(
        &self,
        locator: Locator,
        expected_hash: ContentHash,
    ) -> ChunkFuture<'_, Result<(), ChunkStorageError>> {
        <Self as ChunkStorage>::delete(self, locator, expected_hash)
    }
}

/// Groove's default durable byte plane, colocated with its ordered metadata
/// store. The weak handle avoids extending the database storage lifecycle;
/// all chunk operations fail safely once the owning database is closed.
pub(crate) struct OrderedChunkStorage {
    storage: Weak<LayoutStorage>,
}

impl OrderedChunkStorage {
    pub(crate) fn new(storage: Weak<LayoutStorage>) -> Self {
        Self { storage }
    }

    fn key(locator: &[u8]) -> Vec<u8> {
        let mut key = b"chunk/".to_vec();
        key.extend_from_slice(locator);
        key
    }

    fn encode(hash: ContentHash, bytes: &[u8]) -> Vec<u8> {
        let mut value = Vec::with_capacity(32 + bytes.len());
        value.extend_from_slice(&hash.0);
        value.extend_from_slice(bytes);
        value
    }

    const INSTALL_RECEIPT_MAGIC: [u8; 32] = *b"\0groove-chunk-install-receipt-v1";

    fn encode_with_install_receipt(hash: ContentHash, bytes: &[u8], receipt: [u8; 16]) -> Vec<u8> {
        let encoded = Self::encode(hash, bytes);
        let mut value =
            Vec::with_capacity(Self::INSTALL_RECEIPT_MAGIC.len() + receipt.len() + encoded.len());
        value.extend_from_slice(&Self::INSTALL_RECEIPT_MAGIC);
        value.extend_from_slice(&receipt);
        value.extend_from_slice(&encoded);
        value
    }

    fn split_install_receipt(value: &[u8]) -> (Option<[u8; 16]>, &[u8]) {
        let Some((magic, remainder)) = value.split_at_checked(Self::INSTALL_RECEIPT_MAGIC.len())
        else {
            return (None, value);
        };
        if magic != Self::INSTALL_RECEIPT_MAGIC {
            return (None, value);
        }
        let Some((receipt, encoded)) = remainder.split_at_checked(16) else {
            return (None, value);
        };
        let mut receipt_bytes = [0; 16];
        receipt_bytes.copy_from_slice(receipt);
        (Some(receipt_bytes), encoded)
    }

    fn decode(value: Vec<u8>) -> Result<(ContentHash, Bytes), ChunkStorageError> {
        let (_, value) = Self::split_install_receipt(&value);
        let (hash, bytes) = value
            .split_at_checked(32)
            .ok_or(ChunkStorageError::Integrity)?;
        let mut expected = [0_u8; 32];
        expected.copy_from_slice(hash);
        Ok((ContentHash(expected), Bytes::copy_from_slice(bytes)))
    }

    fn storage(&self) -> Result<Rc<LayoutStorage>, ChunkStorageError> {
        self.storage
            .upgrade()
            .ok_or_else(|| ChunkStorageError::Backend("owning Groove storage is closed".to_owned()))
    }
}

impl ChunkKvStorage for OrderedChunkStorage {
    fn get_exact(
        &self,
        locator: Locator,
    ) -> ChunkFuture<'_, Result<Option<(ContentHash, Bytes)>, ChunkStorageError>> {
        Box::pin(async move {
            let storage = self.storage()?;
            let Some(value) = storage
                .get(
                    crate::db::LARGE_VALUE_METADATA_CF.to_owned(),
                    Self::key(locator.as_bytes()),
                )
                .await
                .map_err(|error| ChunkStorageError::Backend(error.to_string()))?
            else {
                return Ok(None);
            };
            Self::decode(value).map(Some)
        })
    }

    fn put_if_absent(
        &self,
        locator: Locator,
        hash: ContentHash,
        bytes: Bytes,
    ) -> ChunkFuture<'_, Result<Option<(ContentHash, Bytes)>, ChunkStorageError>> {
        Box::pin(async move {
            let storage = self.storage()?;
            let key = Self::key(locator.as_bytes());
            let receipt = *uuid::Uuid::new_v4().as_bytes();
            let candidate = Self::encode_with_install_receipt(hash, &bytes, receipt);
            let existing = storage
                .put_if_absent(
                    crate::db::LARGE_VALUE_METADATA_CF.to_owned(),
                    key,
                    candidate,
                )
                .await
                .map_err(|error| ChunkStorageError::Backend(error.to_string()))?;
            existing.map(Self::decode).transpose()
        })
    }

    fn delete_exact(
        &self,
        locator: Locator,
        expected_hash: ContentHash,
    ) -> ChunkFuture<'_, Result<(), ChunkStorageError>> {
        Box::pin(async move {
            let storage = self.storage()?;
            let key = Self::key(locator.as_bytes());
            let Some(existing) = storage
                .get(crate::db::LARGE_VALUE_METADATA_CF.to_owned(), key.clone())
                .await
                .map_err(|error| ChunkStorageError::Backend(error.to_string()))?
            else {
                return Ok(());
            };
            let (hash, bytes) = Self::decode(existing.clone())?;
            if hash != expected_hash || object_hash(&bytes) != expected_hash {
                return Err(ChunkStorageError::Integrity);
            }
            storage
                .compare_and_delete(crate::db::LARGE_VALUE_METADATA_CF.to_owned(), key, existing)
                .await
                .map(|_| ())
                .map_err(|error| ChunkStorageError::Backend(error.to_string()))
        })
    }
}

/// Direct retrieval adapter over Groove-owned storage. Authorization is locator
/// discovery, not mutable provider state.
#[derive(Clone)]
pub struct StorageChunkProvider<S> {
    storage: S,
    resolver: Rc<dyn MissingChunkResolver>,
    observer: Rc<dyn ChunkInstallObserver>,
}

impl<S> StorageChunkProvider<S> {
    pub fn new(storage: S) -> Self {
        Self {
            storage,
            resolver: Rc::new(UnavailableChunkResolver),
            observer: Rc::new(NoopChunkInstallObserver),
        }
    }

    pub fn with_resolver(storage: S, resolver: Rc<dyn MissingChunkResolver>) -> Self {
        Self {
            storage,
            resolver,
            observer: Rc::new(NoopChunkInstallObserver),
        }
    }

    pub fn with_resolver_and_observer(
        storage: S,
        resolver: Rc<dyn MissingChunkResolver>,
        observer: Rc<dyn ChunkInstallObserver>,
    ) -> Self {
        Self {
            storage,
            resolver,
            observer,
        }
    }
}

impl<S> ChunkProvider for StorageChunkProvider<S>
where
    S: ChunkStorage,
{
    fn get(&self, request: ChunkRequest) -> ChunkFuture<'_, Result<Bytes, ChunkError>> {
        self.get_with_install_observer(request, Rc::clone(&self.observer))
    }

    fn get_with_install_observer(
        &self,
        request: ChunkRequest,
        observer: Rc<dyn ChunkInstallObserver>,
    ) -> ChunkFuture<'_, Result<Bytes, ChunkError>> {
        Box::pin(async move {
            match self
                .storage
                .get(request.locator, ContentHash(request.object_hash))
                .await
            {
                Ok(bytes) => Ok(bytes),
                Err(ChunkStorageError::Unavailable) => {
                    let bytes = self.resolver.resolve(request.clone()).await?;
                    self.storage
                        .stage(vec![StagedChunk {
                            node_ref: crate::large_values::NodeRef {
                                object_hash: ContentHash(request.object_hash),
                                locator: request.locator,
                            },
                            encoded: bytes.to_vec(),
                        }])
                        .await
                        .map_err(ChunkError::from)?;
                    observer
                        .installed(
                            crate::large_values::NodeRef {
                                object_hash: ContentHash(request.object_hash),
                                locator: request.locator,
                            },
                            bytes.clone(),
                        )
                        .await?;
                    Ok(bytes)
                }
                Err(error) => Err(ChunkError::from(error)),
            }
        })
    }
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum ChunkError {
    #[error("chunk is unavailable")]
    Unavailable,
    #[error("chunk retrieval is retryable after {retry_after_ms}ms")]
    Retryable { retry_after_ms: u32 },
    #[error("chunk retrieval failed: {0}")]
    Backend(String),
    #[error("chunk bytes do not match the requested object hash")]
    Integrity,
    #[error("chunk request re-entered while its backing request was being polled")]
    Reentrant,
}

#[derive(Clone, Default)]
pub(crate) struct PublicationInstallFailures {
    failures: Rc<RefCell<BTreeMap<ChunkRequest, ChunkError>>>,
}

impl PublicationInstallFailures {
    pub(crate) fn record(&self, node_ref: crate::large_values::NodeRef, error: ChunkError) {
        self.failures.borrow_mut().insert(
            ChunkRequest {
                object_hash: node_ref.object_hash.0,
                locator: node_ref.locator,
            },
            error,
        );
    }

    fn take(&self, request: &ChunkRequest) -> Option<ChunkError> {
        self.failures.borrow_mut().remove(request)
    }
}

impl From<ChunkStorageError> for ChunkError {
    fn from(error: ChunkStorageError) -> Self {
        match error {
            ChunkStorageError::Unavailable => Self::Unavailable,
            ChunkStorageError::Integrity => Self::Integrity,
            error => Self::Backend(error.to_string()),
        }
    }
}

/// Exact, policy-blind retrieval interface owned by Groove.
///
/// Groove never lists locators or looks up chunks by content hash. Jazz gates
/// descriptor disclosure through ordinary row/view authorization rather than
/// maintaining mutable authorization state in this provider.
pub trait ChunkProvider {
    fn get(&self, request: ChunkRequest) -> ChunkFuture<'_, Result<Bytes, ChunkError>>;

    fn get_with_install_observer(
        &self,
        request: ChunkRequest,
        _observer: Rc<dyn ChunkInstallObserver>,
    ) -> ChunkFuture<'_, Result<Bytes, ChunkError>> {
        self.get(request)
    }
}

#[derive(Default)]
struct VerifiedChunkCache {
    entries: BTreeMap<ChunkRequest, (Bytes, u64)>,
    bytes: usize,
    clock: u64,
    budget: usize,
}

#[derive(Clone)]
pub struct OwnedChunkProvider {
    provider: Rc<dyn ChunkProvider>,
    cache: Rc<RefCell<VerifiedChunkCache>>,
    leases: Rc<RefCell<ChunkLeaseStats>>,
    activity: Rc<RefCell<ChunkActivityState>>,
    in_flight: Rc<RefCell<InFlightChunks>>,
    install_observer: Option<Rc<dyn ChunkInstallObserver>>,
    install_failures: Option<PublicationInstallFailures>,
}

#[derive(Default, Debug)]
struct ChunkLeaseStats {
    active: usize,
    bytes: usize,
}

#[derive(Debug)]
pub(crate) struct ChunkLease {
    bytes: Bytes,
    stats: Rc<RefCell<ChunkLeaseStats>>,
}

impl PartialEq for ChunkLease {
    fn eq(&self, other: &Self) -> bool {
        self.bytes == other.bytes
    }
}

impl PartialEq<Bytes> for ChunkLease {
    fn eq(&self, other: &Bytes) -> bool {
        self.bytes == *other
    }
}

impl ChunkLease {
    fn new(bytes: Bytes, stats: Rc<RefCell<ChunkLeaseStats>>) -> Self {
        let mut counters = stats.borrow_mut();
        counters.active += 1;
        counters.bytes = counters.bytes.saturating_add(bytes.len());
        drop(counters);
        Self { bytes, stats }
    }
    pub(crate) fn bytes(&self) -> &Bytes {
        &self.bytes
    }
}

impl Drop for ChunkLease {
    fn drop(&mut self) {
        let mut counters = self.stats.borrow_mut();
        counters.active = counters.active.saturating_sub(1);
        counters.bytes = counters.bytes.saturating_sub(self.bytes.len());
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ChunkCacheStats {
    pub entries: usize,
    pub owned_bytes: usize,
    pub budget_bytes: usize,
    pub active_leases: usize,
    pub leased_bytes: usize,
    /// Admitted chunk-read consumers, including callers sharing one backing
    /// load and callers served from the verified cache. Reclamation is deferred
    /// while this is non-zero because a requested locator may have been
    /// discovered from an active root before its bytes were leased.
    pub active_requests: usize,
}

#[derive(Default)]
struct ChunkActivityState {
    active_requests: usize,
    reclaiming: bool,
    waiters: Vec<Waker>,
}

/// The durable evaluation-request registry is per database rather than per
/// evaluation session. Keep the provider future here too: two independently
/// installed terminals can discover the same cold immutable node before either
/// session has a result to cache.
#[derive(Default)]
struct InFlightChunks {
    entries: BTreeMap<ChunkRequest, InFlightChunk>,
}

struct InFlightChunk {
    /// Temporarily `None` only while a consumer is polling it outside the
    /// registry borrow. A synchronous reentrant request for this exact key is
    /// a request cycle and fails deterministically.
    future: Option<ChunkFuture<'static, Result<Bytes, OwnedChunkError>>>,
    result: Option<Result<Bytes, OwnedChunkError>>,
    /// Each consumer has at most one replaceable registered waker. Futures may
    /// legally be re-polled with a different waker, so retaining a bare list
    /// would keep every old task allocation alive until completion.
    waiters: Vec<ChunkWaiter>,
    consumers: usize,
    next_consumer_id: u64,
}

struct ChunkWaiter {
    consumer_id: u64,
    waker: Waker,
}

/// One caller's lease-producing view of a shared exact chunk request.
///
/// The wrapper owns its consumer count. If every blocked evaluation is
/// cancelled, dropping the last wrapper removes and drops the backing request,
/// so a later evaluation creates a fresh request instead of joining a future
/// that no executor will ever poll.
struct CoalescedChunkGet {
    request: ChunkRequest,
    consumer_id: u64,
    in_flight: Rc<RefCell<InFlightChunks>>,
    leases: Rc<RefCell<ChunkLeaseStats>>,
    done: bool,
}

#[derive(Clone, Debug)]
pub(crate) struct OwnedChunkError {
    error: ChunkError,
    publication_metadata_durability: bool,
}

impl OwnedChunkError {
    pub(crate) fn into_parts(self) -> (ChunkError, bool) {
        (self.error, self.publication_metadata_durability)
    }
}

impl From<ChunkError> for OwnedChunkError {
    fn from(error: ChunkError) -> Self {
        Self {
            error,
            publication_metadata_durability: false,
        }
    }
}

impl From<OwnedChunkError> for ChunkError {
    fn from(error: OwnedChunkError) -> Self {
        error.error
    }
}

impl CoalescedChunkGet {
    fn finish(&mut self) {
        if self.done {
            return;
        }
        self.done = true;
        let (remove, wake) = {
            let mut in_flight = self.in_flight.borrow_mut();
            let Some(entry) = in_flight.entries.get_mut(&self.request) else {
                return;
            };
            entry.consumers = entry.consumers.saturating_sub(1);
            entry
                .waiters
                .retain(|waiter| waiter.consumer_id != self.consumer_id);
            // The consumer which last polled the backing future may be the
            // one being cancelled. Wake remaining consumers so one of them
            // installs its waker on the single shared future.
            let wake = if entry.consumers != 0 && entry.result.is_none() {
                std::mem::take(&mut entry.waiters)
            } else {
                Vec::new()
            };
            (entry.consumers == 0, wake)
        };
        if remove {
            self.in_flight.borrow_mut().entries.remove(&self.request);
        }
        for waiter in wake {
            waiter.waker.wake();
        }
    }
}

impl Future for CoalescedChunkGet {
    type Output = Result<ChunkLease, OwnedChunkError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        enum Next {
            Complete(Result<Bytes, OwnedChunkError>),
            Poll(ChunkFuture<'static, Result<Bytes, OwnedChunkError>>),
            Reentrant,
        }

        let next = {
            let mut in_flight = self.in_flight.borrow_mut();
            let entry = in_flight
                .entries
                .get_mut(&self.request)
                .expect("coalesced chunk request remains registered while a consumer exists");
            if let Some(result) = &entry.result {
                Next::Complete(result.clone())
            } else {
                entry.future.take().map_or(Next::Reentrant, Next::Poll)
            }
        };

        let mut wake = Vec::new();
        let result = match next {
            Next::Complete(result) => Some(result),
            Next::Reentrant => Some(Err(ChunkError::Reentrant.into())),
            Next::Poll(mut future) => match future.as_mut().poll(cx) {
                Poll::Pending => {
                    let mut in_flight = self.in_flight.borrow_mut();
                    let entry = in_flight.entries.get_mut(&self.request).expect(
                        "coalesced chunk request remains registered while a consumer exists",
                    );
                    debug_assert!(entry.future.is_none());
                    entry.future = Some(future);
                    None
                }
                Poll::Ready(result) => {
                    let mut in_flight = self.in_flight.borrow_mut();
                    let entry = in_flight.entries.get_mut(&self.request).expect(
                        "coalesced chunk request remains registered while a consumer exists",
                    );
                    entry.result = Some(result.clone());
                    wake = std::mem::take(&mut entry.waiters)
                        .into_iter()
                        .filter(|waiter| waiter.consumer_id != self.consumer_id)
                        .collect();
                    Some(result)
                }
            },
        };
        if result.is_none() {
            let mut in_flight = self.in_flight.borrow_mut();
            let entry = in_flight
                .entries
                .get_mut(&self.request)
                .expect("coalesced chunk request remains registered while a consumer exists");
            if let Some(waiter) = entry
                .waiters
                .iter_mut()
                .find(|waiter| waiter.consumer_id == self.consumer_id)
            {
                if !waiter.waker.will_wake(cx.waker()) {
                    waiter.waker = cx.waker().clone();
                }
            } else {
                entry.waiters.push(ChunkWaiter {
                    consumer_id: self.consumer_id,
                    waker: cx.waker().clone(),
                });
            }
        }
        for waiter in wake {
            waiter.waker.wake();
        }
        let Some(result) = result else {
            return Poll::Pending;
        };
        self.as_mut().get_mut().finish();
        Poll::Ready(result.map(|bytes| ChunkLease::new(bytes, Rc::clone(&self.leases))))
    }
}

impl Drop for CoalescedChunkGet {
    fn drop(&mut self) {
        self.finish();
    }
}

struct ActiveChunkRequest {
    activity: Rc<RefCell<ChunkActivityState>>,
}

impl ActiveChunkRequest {
    async fn acquire(activity: Rc<RefCell<ChunkActivityState>>) -> Self {
        std::future::poll_fn(|context| {
            let mut state = activity.borrow_mut();
            if state.reclaiming {
                if !state
                    .waiters
                    .iter()
                    .any(|waiter| waiter.will_wake(context.waker()))
                {
                    state.waiters.push(context.waker().clone());
                }
                return Poll::Pending;
            }
            state.active_requests = state.active_requests.saturating_add(1);
            Poll::Ready(())
        })
        .await;
        Self { activity }
    }
}

impl Drop for ActiveChunkRequest {
    fn drop(&mut self) {
        let waiters = {
            let mut state = self.activity.borrow_mut();
            state.active_requests = state.active_requests.saturating_sub(1);
            if state.active_requests == 0 {
                std::mem::take(&mut state.waiters)
            } else {
                Vec::new()
            }
        };
        for waiter in waiters {
            waiter.wake();
        }
    }
}

pub(crate) struct ChunkReclamationGuard {
    activity: Rc<RefCell<ChunkActivityState>>,
}

impl Drop for ChunkReclamationGuard {
    fn drop(&mut self) {
        let waiters = {
            let mut state = self.activity.borrow_mut();
            state.reclaiming = false;
            std::mem::take(&mut state.waiters)
        };
        for waiter in waiters {
            waiter.wake();
        }
    }
}

impl std::fmt::Debug for OwnedChunkProvider {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("OwnedChunkProvider(..)")
    }
}

impl OwnedChunkProvider {
    pub fn new(provider: Rc<dyn ChunkProvider>) -> Self {
        Self::new_with_budget(provider, 16 * 1024 * 1024)
    }

    pub fn new_with_budget(provider: Rc<dyn ChunkProvider>, budget_bytes: usize) -> Self {
        Self {
            provider,
            cache: Rc::new(RefCell::new(VerifiedChunkCache {
                budget: budget_bytes,
                ..VerifiedChunkCache::default()
            })),
            leases: Rc::new(RefCell::new(ChunkLeaseStats::default())),
            activity: Rc::new(RefCell::new(ChunkActivityState::default())),
            in_flight: Rc::new(RefCell::new(InFlightChunks::default())),
            install_observer: None,
            install_failures: None,
        }
    }

    pub(crate) fn with_install_observer(
        &self,
        observer: Rc<dyn ChunkInstallObserver>,
        failures: PublicationInstallFailures,
    ) -> Self {
        Self {
            provider: Rc::clone(&self.provider),
            cache: Rc::clone(&self.cache),
            leases: Rc::clone(&self.leases),
            activity: Rc::clone(&self.activity),
            in_flight: Rc::new(RefCell::new(InFlightChunks::default())),
            install_observer: Some(observer),
            install_failures: Some(failures),
        }
    }

    pub fn cache_stats(&self) -> ChunkCacheStats {
        let cache = self.cache.borrow();
        let leases = self.leases.borrow();
        ChunkCacheStats {
            entries: cache.entries.len(),
            owned_bytes: cache.bytes,
            budget_bytes: cache.budget,
            active_leases: leases.active,
            leased_bytes: leases.bytes,
            active_requests: self.activity.borrow().active_requests,
        }
    }

    /// Start an exclusive reclamation pass only when no request or verified
    /// lease can still depend on a descendant that has not been fetched yet.
    /// New requests wait until the guard is dropped.
    pub(crate) fn try_begin_reclamation(&self) -> Option<ChunkReclamationGuard> {
        if self.leases.borrow().active != 0 {
            return None;
        }
        let mut state = self.activity.borrow_mut();
        if state.reclaiming || state.active_requests != 0 {
            return None;
        }
        state.reclaiming = true;
        drop(state);
        Some(ChunkReclamationGuard {
            activity: Rc::clone(&self.activity),
        })
    }

    pub(crate) fn get(
        &self,
        request: ChunkRequest,
    ) -> ChunkFuture<'static, Result<ChunkLease, ChunkError>> {
        let future = self.get_tracked(request);
        Box::pin(async move { future.await.map_err(Into::into) })
    }

    pub(crate) fn get_tracked(
        &self,
        request: ChunkRequest,
    ) -> ChunkFuture<'static, Result<ChunkLease, OwnedChunkError>> {
        let provider = Rc::clone(&self.provider);
        let cache = Rc::clone(&self.cache);
        let leases = Rc::clone(&self.leases);
        let activity = Rc::clone(&self.activity);
        let in_flight = Rc::clone(&self.in_flight);
        let install_observer = self.install_observer.clone();
        let install_failures = self.install_failures.clone();
        Box::pin(async move {
            // Admission must precede even a verified-cache hit. A reclamation
            // pass may start after the last lease is dropped, and every new
            // reader must remain pending until that pass releases its guard.
            let _request_guard = ActiveChunkRequest::acquire(activity).await;
            let cached = {
                let mut cache = cache.borrow_mut();
                cache.clock = cache.clock.wrapping_add(1);
                let clock = cache.clock;
                if let Some((bytes, last_use)) = cache.entries.get_mut(&request) {
                    *last_use = clock;
                    Some(bytes.clone())
                } else {
                    None
                }
            };
            if let Some(bytes) = cached {
                return Ok(ChunkLease::new(bytes, leases));
            }
            let consumer_id = {
                let mut entries = in_flight.borrow_mut();
                if let Some(entry) = entries.entries.get_mut(&request) {
                    entry.consumers = entry.consumers.saturating_add(1);
                    let consumer_id = entry.next_consumer_id;
                    entry.next_consumer_id = entry.next_consumer_id.wrapping_add(1);
                    consumer_id
                } else {
                    entries.entries.insert(
                        request.clone(),
                        InFlightChunk {
                            future: Some(load_and_verify_chunk(
                                provider,
                                cache,
                                request.clone(),
                                install_observer,
                                install_failures,
                            )),
                            result: None,
                            waiters: Vec::new(),
                            consumers: 1,
                            next_consumer_id: 1,
                        },
                    );
                    0
                }
            };
            CoalescedChunkGet {
                request,
                consumer_id,
                in_flight,
                leases,
                done: false,
            }
            .await
        })
    }
}

fn load_and_verify_chunk(
    provider: Rc<dyn ChunkProvider>,
    cache: Rc<RefCell<VerifiedChunkCache>>,
    request: ChunkRequest,
    install_observer: Option<Rc<dyn ChunkInstallObserver>>,
    install_failures: Option<PublicationInstallFailures>,
) -> ChunkFuture<'static, Result<Bytes, OwnedChunkError>> {
    Box::pin(async move {
        let bytes = if let Some(observer) = install_observer {
            let result = provider
                .get_with_install_observer(request.clone(), observer)
                .await;
            if let Some(error) = install_failures
                .as_ref()
                .and_then(|failures| failures.take(&request))
            {
                return Err(OwnedChunkError {
                    error,
                    publication_metadata_durability: true,
                });
            }
            match result {
                Ok(bytes) => bytes,
                Err(error) => {
                    return Err(OwnedChunkError {
                        error,
                        publication_metadata_durability: false,
                    });
                }
            }
        } else {
            provider
                .get(request.clone())
                .await
                .map_err(OwnedChunkError::from)?
        };
        if bytes.len() > crate::large_values::MAX_ENCODED_NODE_BYTES
            || crate::large_values::object_hash(&bytes).0 != request.object_hash
        {
            return Err(ChunkError::Integrity.into());
        }
        let mut cache = cache.borrow_mut();
        let length = bytes.len();
        if length <= cache.budget {
            while cache.bytes.saturating_add(length) > cache.budget {
                let Some(oldest) = cache
                    .entries
                    .iter()
                    .min_by_key(|(_, (_, last_use))| *last_use)
                    .map(|(request, _)| request.clone())
                else {
                    break;
                };
                if let Some((evicted, _)) = cache.entries.remove(&oldest) {
                    cache.bytes = cache.bytes.saturating_sub(evicted.len());
                }
            }
            cache.clock = cache.clock.wrapping_add(1);
            let clock = cache.clock;
            cache.entries.insert(request, (bytes.clone(), clock));
            cache.bytes = cache.bytes.saturating_add(length);
        }
        Ok(bytes)
    })
}

#[derive(Debug)]
struct UnavailableChunkProvider;

impl ChunkProvider for UnavailableChunkProvider {
    fn get(&self, _request: ChunkRequest) -> ChunkFuture<'_, Result<Bytes, ChunkError>> {
        Box::pin(async { Err(ChunkError::Unavailable) })
    }
}

impl Default for OwnedChunkProvider {
    fn default() -> Self {
        Self::new(Rc::new(UnavailableChunkProvider))
    }
}

#[derive(Default)]
struct TestChunkControlState {
    paused: bool,
    permits: usize,
    fail_next: Option<ChunkError>,
    observed: Vec<ChunkRequest>,
    waiters: Vec<Waker>,
}

/// Deterministic controller for [`TestChunkProvider`].
#[derive(Clone, Default)]
pub struct TestChunkProviderControl {
    state: Rc<RefCell<TestChunkControlState>>,
}

impl TestChunkProviderControl {
    pub fn pause(&self) {
        self.state.borrow_mut().paused = true;
    }

    pub fn release_one(&self) {
        let waiters = {
            let mut state = self.state.borrow_mut();
            state.permits = state.permits.saturating_add(1);
            std::mem::take(&mut state.waiters)
        };
        for waiter in waiters {
            waiter.wake();
        }
    }

    pub fn resume(&self) {
        let waiters = {
            let mut state = self.state.borrow_mut();
            state.paused = false;
            state.permits = 0;
            std::mem::take(&mut state.waiters)
        };
        for waiter in waiters {
            waiter.wake();
        }
    }

    pub fn fail_next(&self, error: ChunkError) {
        self.state.borrow_mut().fail_next = Some(error);
    }

    pub fn observed(&self) -> Vec<ChunkRequest> {
        self.state.borrow().observed.clone()
    }
}

/// In-memory chunk capability with explicit suspension and failure control.
/// Cold requests yield at least once; successfully loaded requests are resident.
#[derive(Clone, Default)]
pub struct TestChunkProvider {
    chunks: Rc<RefCell<BTreeMap<ChunkRequest, Bytes>>>,
    resident: Rc<RefCell<BTreeSet<ChunkRequest>>>,
    control: TestChunkProviderControl,
}

impl TestChunkProvider {
    pub fn controlled(
        chunks: impl IntoIterator<Item = (ChunkRequest, Bytes)>,
    ) -> (Self, TestChunkProviderControl) {
        let provider = Self {
            chunks: Rc::new(RefCell::new(chunks.into_iter().collect())),
            ..Self::default()
        };
        (provider.clone(), provider.control.clone())
    }

    pub fn evict(&self, request: &ChunkRequest) {
        self.resident.borrow_mut().remove(request);
    }
}

impl ChunkProvider for TestChunkProvider {
    fn get(&self, request: ChunkRequest) -> ChunkFuture<'_, Result<Bytes, ChunkError>> {
        let chunks = Rc::clone(&self.chunks);
        let resident = Rc::clone(&self.resident);
        let control = self.control.clone();
        Box::pin(async move {
            let cold = !resident.borrow().contains(&request);
            let mut yielded = false;
            std::future::poll_fn(|cx| {
                let mut state = control.state.borrow_mut();
                if !yielded {
                    state.observed.push(request.clone());
                }
                if cold && !yielded {
                    yielded = true;
                    cx.waker().wake_by_ref();
                    return Poll::Pending;
                }
                if state.paused && state.permits == 0 {
                    if !state
                        .waiters
                        .iter()
                        .any(|waiter| waiter.will_wake(cx.waker()))
                    {
                        state.waiters.push(cx.waker().clone());
                    }
                    return Poll::Pending;
                }
                if state.paused {
                    state.permits -= 1;
                }
                Poll::Ready(state.fail_next.take())
            })
            .await
            .map_or(Ok(()), Err)?;
            let bytes = chunks
                .borrow()
                .get(&request)
                .cloned()
                .ok_or(ChunkError::Unavailable)?;
            resident.borrow_mut().insert(request);
            Ok(bytes)
        })
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::pin::Pin;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use futures::executor::block_on;
    use futures::task::{ArcWake, noop_waker, waker};

    use super::*;

    struct StaticResolver(Bytes);

    impl MissingChunkResolver for StaticResolver {
        fn resolve(&self, _request: ChunkRequest) -> ChunkFuture<'_, Result<Bytes, ChunkError>> {
            let bytes = self.0.clone();
            Box::pin(async move { Ok(bytes) })
        }
    }

    struct CountingProvider {
        calls: Cell<usize>,
        bytes: Bytes,
    }

    impl ChunkProvider for CountingProvider {
        fn get(&self, _request: ChunkRequest) -> ChunkFuture<'_, Result<Bytes, ChunkError>> {
            self.calls.set(self.calls.get() + 1);
            let bytes = self.bytes.clone();
            Box::pin(async move { Ok(bytes) })
        }
    }

    #[test]
    fn verified_chunks_are_shared_across_request_sessions() {
        let bytes = Bytes::from_static(b"authenticated chunk bytes");
        let provider = Rc::new(CountingProvider {
            calls: Cell::new(0),
            bytes: bytes.clone(),
        });
        let chunks = OwnedChunkProvider::new(provider.clone());
        let request = ChunkRequest {
            object_hash: crate::large_values::object_hash(&bytes).0,
            locator: Locator::from_seed(b"opaque"),
        };

        assert_eq!(block_on(chunks.get(request.clone())).unwrap(), bytes);
        assert_eq!(block_on(chunks.get(request)).unwrap(), bytes);
        assert_eq!(provider.calls.get(), 1);
    }

    #[test]
    fn concurrent_cold_sessions_share_one_provider_request_and_cancel_cleanly() {
        let bytes = Bytes::from_static(b"one cold authenticated chunk");
        let request = ChunkRequest {
            object_hash: crate::large_values::object_hash(&bytes).0,
            locator: Locator::from_seed(b"one-cold-request"),
        };
        let (provider, control) = TestChunkProvider::controlled([(request.clone(), bytes)]);
        let chunks = OwnedChunkProvider::new(Rc::new(provider));
        control.pause();
        let mut first = chunks.get(request.clone());
        let mut second = chunks.get(request.clone());
        let waker = noop_waker();
        let mut context = std::task::Context::from_waker(&waker);

        assert!(matches!(
            Pin::new(&mut first).poll(&mut context),
            Poll::Pending
        ));
        assert!(matches!(
            Pin::new(&mut second).poll(&mut context),
            Poll::Pending
        ));
        assert_eq!(control.observed(), vec![request.clone()]);
        assert_eq!(chunks.cache_stats().active_requests, 2);

        // No blocked consumer remains, so the shared request is dropped rather
        // than becoming a permanently unpolled registry entry.
        drop(first);
        drop(second);
        assert_eq!(chunks.cache_stats().active_requests, 0);
        let mut retry = chunks.get(request.clone());
        assert!(matches!(
            Pin::new(&mut retry).poll(&mut context),
            Poll::Pending
        ));
        assert_eq!(control.observed(), vec![request.clone(), request]);
    }

    #[test]
    fn cancelling_the_backing_poller_wakes_a_remaining_consumer() {
        struct WakeCounter(AtomicUsize);

        impl ArcWake for WakeCounter {
            fn wake_by_ref(arc_self: &Arc<Self>) {
                arc_self.0.fetch_add(1, Ordering::SeqCst);
            }
        }

        let bytes = Bytes::from_static(b"handoff the backing poller");
        let request = ChunkRequest {
            object_hash: crate::large_values::object_hash(&bytes).0,
            locator: Locator::from_seed(b"cancellation-handoff"),
        };
        let (provider, control) = TestChunkProvider::controlled([(request.clone(), bytes.clone())]);
        let chunks = OwnedChunkProvider::new(Rc::new(provider));
        control.pause();
        let mut first = chunks.get(request.clone());
        let mut second = chunks.get(request.clone());
        let first_wakes = Arc::new(WakeCounter(AtomicUsize::new(0)));
        let second_wakes = Arc::new(WakeCounter(AtomicUsize::new(0)));
        let first_waker = waker(Arc::clone(&first_wakes));
        let second_waker = waker(Arc::clone(&second_wakes));
        let mut first_context = std::task::Context::from_waker(&first_waker);
        let mut second_context = std::task::Context::from_waker(&second_waker);

        assert!(matches!(
            Pin::new(&mut first).poll(&mut first_context),
            Poll::Pending
        ));
        assert!(matches!(
            Pin::new(&mut second).poll(&mut second_context),
            Poll::Pending
        ));
        assert_eq!(second_wakes.0.load(Ordering::SeqCst), 0);

        // `first` owns the backing future's last waker. Its cancellation must
        // explicitly wake `second` so the shared future keeps making progress.
        drop(first);
        assert_eq!(second_wakes.0.load(Ordering::SeqCst), 1);
        assert!(
            chunks
                .in_flight
                .borrow()
                .entries
                .get(&request)
                .expect("remaining consumer keeps the shared request")
                .waiters
                .is_empty()
        );
        assert!(matches!(
            Pin::new(&mut second).poll(&mut second_context),
            Poll::Pending
        ));
        assert_eq!(
            chunks
                .in_flight
                .borrow()
                .entries
                .get(&request)
                .expect("remaining consumer re-registers its waker")
                .waiters
                .len(),
            1
        );
        control.release_one();
        assert!(matches!(
            Pin::new(&mut second).poll(&mut second_context),
            Poll::Ready(Ok(_))
        ));
    }

    // This must stay internal: public query APIs cannot observe an executor's
    // replacement waker identity or prove that obsolete task allocations are
    // released while a shared request remains pending.
    #[test]
    fn replacing_a_pending_consumers_waker_drops_stale_wakers() {
        struct NeverReadyProvider;

        impl ChunkProvider for NeverReadyProvider {
            fn get(&self, _request: ChunkRequest) -> ChunkFuture<'_, Result<Bytes, ChunkError>> {
                Box::pin(std::future::pending())
            }
        }

        struct WakeToken;

        impl ArcWake for WakeToken {
            fn wake_by_ref(_: &Arc<Self>) {}
        }

        let bytes = Bytes::from_static(b"replace stale consumer wakers");
        let request = ChunkRequest {
            object_hash: crate::large_values::object_hash(&bytes).0,
            locator: Locator::from_seed(b"replace-stale-consumer-wakers"),
        };
        let chunks = OwnedChunkProvider::new(Rc::new(NeverReadyProvider));
        let mut first = chunks.get(request.clone());
        let mut second = chunks.get(request.clone());
        let mut weak_wakers = Vec::new();

        // Futures may be polled repeatedly by executors which install a fresh
        // task waker after every suspension. The registry must retain only the
        // current waker for this one consumer, not one clone per poll.
        for _ in 0..8 {
            let token = Arc::new(WakeToken);
            weak_wakers.push(Arc::downgrade(&token));
            let waker = waker(token);
            let mut context = std::task::Context::from_waker(&waker);
            assert!(matches!(
                Pin::new(&mut first).poll(&mut context),
                Poll::Pending
            ));
        }

        let second_token = Arc::new(WakeToken);
        let second_waker = waker(second_token);
        let mut second_context = std::task::Context::from_waker(&second_waker);
        assert!(matches!(
            Pin::new(&mut second).poll(&mut second_context),
            Poll::Pending
        ));
        assert_eq!(
            chunks
                .in_flight
                .borrow()
                .entries
                .get(&request)
                .expect("both pending consumers retain the request")
                .waiters
                .len(),
            2
        );
        assert!(
            weak_wakers[..7]
                .iter()
                .all(|waker| waker.upgrade().is_none())
        );
        assert!(weak_wakers[7].upgrade().is_some());

        // Dropping the backing poller releases its current waiter and wakes
        // the remaining consumer to register itself as the new poller.
        drop(first);
        assert_eq!(
            chunks
                .in_flight
                .borrow()
                .entries
                .get(&request)
                .expect("remaining consumer retains the request")
                .waiters
                .len(),
            0,
            "handoff wakes and clears the remaining consumer for re-registration"
        );
        assert!(weak_wakers[7].upgrade().is_none());

        drop(second);
        assert!(chunks.in_flight.borrow().entries.is_empty());
    }

    #[test]
    fn cached_get_waits_for_reclamation_admission_before_returning_a_lease() {
        let bytes = Bytes::from_static(b"warm verified chunk");
        let provider = Rc::new(CountingProvider {
            calls: Cell::new(0),
            bytes: bytes.clone(),
        });
        let chunks = OwnedChunkProvider::new(provider);
        let request = ChunkRequest {
            object_hash: crate::large_values::object_hash(&bytes).0,
            locator: Locator::from_seed(b"warm-cache-reclamation"),
        };

        // Warm the verified cache, then leave no lease that would prevent a
        // reclamation pass from beginning.
        drop(block_on(chunks.get(request.clone())).unwrap());
        let reclamation = chunks
            .try_begin_reclamation()
            .expect("a lease-free warm cache may begin reclamation");
        let mut cached_get = chunks.get(request);
        let waker = noop_waker();
        let mut context = std::task::Context::from_waker(&waker);

        // This is the cache-before-admission regression: returning here would
        // let a new read overlap a pass that has exclusive reclamation access.
        assert!(matches!(
            Pin::new(&mut cached_get).poll(&mut context),
            Poll::Pending
        ));
        assert_eq!(chunks.cache_stats().active_requests, 0);
        drop(reclamation);
        assert!(matches!(
            Pin::new(&mut cached_get).poll(&mut context),
            Poll::Ready(Ok(_))
        ));
    }

    #[test]
    fn same_key_fanout_receives_one_error_and_a_later_retry_starts_fresh() {
        let bytes = Bytes::from_static(b"retry after shared failure");
        let request = ChunkRequest {
            object_hash: crate::large_values::object_hash(&bytes).0,
            locator: Locator::from_seed(b"shared-failure"),
        };
        let (provider, control) = TestChunkProvider::controlled([(request.clone(), bytes.clone())]);
        let chunks = OwnedChunkProvider::new(Rc::new(provider));
        control.pause();
        control.fail_next(ChunkError::Unavailable);
        let mut first = chunks.get(request.clone());
        let mut second = chunks.get(request.clone());
        let waker = noop_waker();
        let mut context = std::task::Context::from_waker(&waker);

        assert!(matches!(
            Pin::new(&mut first).poll(&mut context),
            Poll::Pending
        ));
        assert!(matches!(
            Pin::new(&mut second).poll(&mut context),
            Poll::Pending
        ));
        control.release_one();
        assert!(matches!(
            Pin::new(&mut first).poll(&mut context),
            Poll::Ready(Err(ChunkError::Unavailable))
        ));
        assert!(matches!(
            Pin::new(&mut second).poll(&mut context),
            Poll::Ready(Err(ChunkError::Unavailable))
        ));
        assert!(chunks.in_flight.borrow().entries.is_empty());
        control.resume();
        assert_eq!(block_on(chunks.get(request.clone())).unwrap(), bytes);
        assert_eq!(control.observed(), vec![request.clone(), request]);
    }

    #[test]
    fn concurrent_cold_sessions_receive_independent_leases_from_one_completion() {
        let bytes = Bytes::from_static(b"shared completion bytes");
        let request = ChunkRequest {
            object_hash: crate::large_values::object_hash(&bytes).0,
            locator: Locator::from_seed(b"shared-completion"),
        };
        let (provider, control) = TestChunkProvider::controlled([(request.clone(), bytes.clone())]);
        let chunks = OwnedChunkProvider::new(Rc::new(provider));
        control.pause();
        let mut first = chunks.get(request.clone());
        let mut second = chunks.get(request.clone());
        let waker = noop_waker();
        let mut context = std::task::Context::from_waker(&waker);

        assert!(matches!(
            Pin::new(&mut first).poll(&mut context),
            Poll::Pending
        ));
        assert!(matches!(
            Pin::new(&mut second).poll(&mut context),
            Poll::Pending
        ));
        control.release_one();
        let Poll::Ready(Ok(first)) = Pin::new(&mut first).poll(&mut context) else {
            panic!("first coalesced consumer must complete after the shared request");
        };
        let Poll::Ready(Ok(second)) = Pin::new(&mut second).poll(&mut context) else {
            panic!("second coalesced consumer must complete after the shared request");
        };
        assert_eq!(first, bytes);
        assert_eq!(second, bytes);
        assert_eq!(control.observed(), vec![request]);
        assert_eq!(chunks.cache_stats().active_requests, 0);
        assert_eq!(chunks.cache_stats().active_leases, 2);
        assert!(chunks.in_flight.borrow().entries.is_empty());
    }

    #[test]
    fn provider_may_reentrantly_request_a_different_cold_chunk() {
        struct ReentrantProvider {
            chunks: Rc<RefCell<Option<OwnedChunkProvider>>>,
            first: ChunkRequest,
            second: ChunkRequest,
            first_bytes: Bytes,
            second_bytes: Bytes,
        }

        impl ChunkProvider for ReentrantProvider {
            fn get(&self, request: ChunkRequest) -> ChunkFuture<'_, Result<Bytes, ChunkError>> {
                let chunks = Rc::clone(&self.chunks);
                let first = self.first.clone();
                let second = self.second.clone();
                let first_bytes = self.first_bytes.clone();
                let second_bytes = self.second_bytes.clone();
                Box::pin(async move {
                    if request == first {
                        let nested = chunks
                            .borrow()
                            .as_ref()
                            .expect("provider is installed")
                            .get(second);
                        let _nested_lease = nested.await?;
                        Ok(first_bytes)
                    } else {
                        Ok(second_bytes)
                    }
                })
            }
        }

        let first_bytes = Bytes::from_static(b"outer chunk");
        let second_bytes = Bytes::from_static(b"reentrant chunk");
        let first = ChunkRequest {
            object_hash: crate::large_values::object_hash(&first_bytes).0,
            locator: Locator::from_seed(b"reentrant-first"),
        };
        let second = ChunkRequest {
            object_hash: crate::large_values::object_hash(&second_bytes).0,
            locator: Locator::from_seed(b"reentrant-second"),
        };
        let slot = Rc::new(RefCell::new(None));
        let chunks = OwnedChunkProvider::new(Rc::new(ReentrantProvider {
            chunks: Rc::clone(&slot),
            first: first.clone(),
            second,
            first_bytes: first_bytes.clone(),
            second_bytes,
        }));
        *slot.borrow_mut() = Some(chunks.clone());

        assert_eq!(block_on(chunks.get(first)).unwrap(), first_bytes);
    }

    #[test]
    fn synchronously_reentrant_same_key_fails_and_cleans_up() {
        struct CycleProvider {
            chunks: Rc<RefCell<Option<OwnedChunkProvider>>>,
        }

        impl ChunkProvider for CycleProvider {
            fn get(&self, request: ChunkRequest) -> ChunkFuture<'_, Result<Bytes, ChunkError>> {
                let chunks = Rc::clone(&self.chunks);
                Box::pin(async move {
                    let nested = chunks
                        .borrow()
                        .as_ref()
                        .expect("provider is installed")
                        .get(request);
                    let _nested_lease = nested.await?;
                    Ok(Bytes::new())
                })
            }
        }

        let bytes = Bytes::from_static(b"same-key request cycle");
        let request = ChunkRequest {
            object_hash: crate::large_values::object_hash(&bytes).0,
            locator: Locator::from_seed(b"same-key-cycle"),
        };
        let slot = Rc::new(RefCell::new(None));
        let chunks = OwnedChunkProvider::new(Rc::new(CycleProvider {
            chunks: Rc::clone(&slot),
        }));
        *slot.borrow_mut() = Some(chunks.clone());

        assert_eq!(block_on(chunks.get(request)), Err(ChunkError::Reentrant));
        assert!(chunks.in_flight.borrow().entries.is_empty());
        assert_eq!(chunks.cache_stats().active_requests, 0);
    }

    #[test]
    fn corrupt_bytes_never_enter_the_verified_cache() {
        let bytes = Bytes::from_static(b"wrong bytes");
        let provider = Rc::new(CountingProvider {
            calls: Cell::new(0),
            bytes,
        });
        let chunks = OwnedChunkProvider::new(provider.clone());
        let request = ChunkRequest {
            object_hash: [9; 32],
            locator: Locator::from_seed(b"opaque"),
        };

        assert_eq!(
            block_on(chunks.get(request.clone())),
            Err(ChunkError::Integrity)
        );
        assert_eq!(block_on(chunks.get(request)), Err(ChunkError::Integrity));
        assert_eq!(provider.calls.get(), 2);
    }

    #[test]
    fn oversized_encoded_nodes_are_rejected_before_staging() {
        let storage = MemoryChunkStorage::new();
        let encoded = vec![0; crate::large_values::MAX_ENCODED_NODE_BYTES + 1];
        let hash = object_hash(&encoded);
        let result = block_on(storage.stage(vec![StagedChunk {
            node_ref: crate::large_values::NodeRef {
                object_hash: hash,
                locator: crate::large_values::Locator::random(),
            },
            encoded,
        }]));
        assert_eq!(result, Err(ChunkStorageError::Integrity));
        assert!(storage.is_empty());
    }

    #[test]
    fn local_chunk_reader_refreshes_existing_clones() {
        let locator = Locator::from_seed(b"retargeted-reader");
        let first_bytes = Bytes::from_static(b"first backing store");
        let first_hash = object_hash(&first_bytes);
        let first = Rc::new(MemoryChunkStorage::new());
        block_on(first.stage(vec![StagedChunk {
            node_ref: crate::large_values::NodeRef {
                object_hash: first_hash,
                locator,
            },
            encoded: first_bytes.to_vec(),
        }]))
        .unwrap();

        let reader = LocalChunkReader::new(first);
        let retained_by_peer = reader.clone();
        assert_eq!(
            block_on(retained_by_peer.get(locator, first_hash)).unwrap(),
            first_bytes
        );

        let second_bytes = Bytes::from_static(b"replacement backing store");
        let second_hash = object_hash(&second_bytes);
        let second = Rc::new(MemoryChunkStorage::new());
        block_on(second.stage(vec![StagedChunk {
            node_ref: crate::large_values::NodeRef {
                object_hash: second_hash,
                locator,
            },
            encoded: second_bytes.to_vec(),
        }]))
        .unwrap();
        let replacement = LocalChunkReader::new(second);

        reader.refresh_from(&replacement);
        assert_eq!(
            block_on(retained_by_peer.get(locator, second_hash)).unwrap(),
            second_bytes
        );
        assert_eq!(
            block_on(retained_by_peer.get(locator, first_hash)),
            Err(ChunkStorageError::Unavailable)
        );
    }

    #[test]
    fn byte_budget_evicts_verified_ownership_without_invalidating_live_bytes() {
        struct MapProvider(BTreeMap<crate::large_values::Locator, Bytes>);
        impl ChunkProvider for MapProvider {
            fn get(&self, request: ChunkRequest) -> ChunkFuture<'_, Result<Bytes, ChunkError>> {
                let value = self.0.get(&request.locator).cloned();
                Box::pin(async move { value.ok_or(ChunkError::Unavailable) })
            }
        }
        let first = Bytes::from_static(b"first-live-buffer");
        let second = Bytes::from_static(b"second-buffer");
        let first_request = ChunkRequest {
            object_hash: crate::large_values::object_hash(&first).0,
            locator: Locator::from_seed(b"first"),
        };
        let second_request = ChunkRequest {
            object_hash: crate::large_values::object_hash(&second).0,
            locator: Locator::from_seed(b"second"),
        };
        let provider = Rc::new(MapProvider(BTreeMap::from([
            (first_request.locator, first.clone()),
            (second_request.locator, second.clone()),
        ])));
        let budget = first.len().max(second.len());
        let chunks = OwnedChunkProvider::new_with_budget(provider, budget);

        let live = block_on(chunks.get(first_request)).unwrap();
        assert_eq!(block_on(chunks.get(second_request)).unwrap(), second);
        assert_eq!(chunks.cache_stats().owned_bytes, second.len());
        assert_eq!(chunks.cache_stats().entries, 1);
        assert_eq!(chunks.cache_stats().active_leases, 1);
        assert_eq!(chunks.cache_stats().leased_bytes, first.len());
        assert_eq!(live, first);
        drop(live);
        assert_eq!(chunks.cache_stats().active_leases, 0);
        assert_eq!(chunks.cache_stats().leased_bytes, 0);
    }

    #[test]
    fn managed_storage_keeps_integrity_out_of_the_byte_kv_backend() {
        let backend = Rc::new(MemoryChunkStorage::new());
        let managed = ManagedChunkStorage::new(backend.clone());
        let bytes = Bytes::from_static(b"managed immutable bytes");
        let hash = object_hash(&bytes);
        let locator = Locator::from_seed(b"opaque-locator");
        let installed = block_on(managed.stage(vec![StagedChunk {
            node_ref: crate::large_values::NodeRef {
                object_hash: hash,
                locator,
            },
            encoded: bytes.to_vec(),
        }]))
        .unwrap();
        let restaged = block_on(managed.stage(vec![StagedChunk {
            node_ref: crate::large_values::NodeRef {
                object_hash: hash,
                locator,
            },
            encoded: bytes.to_vec(),
        }]))
        .unwrap();

        assert!(installed.encoded_bytes > 0);
        assert_eq!(restaged, Default::default());
        assert_eq!(block_on(managed.get(locator.clone(), hash)).unwrap(), bytes);
        assert_eq!(
            block_on(managed.get(locator, ContentHash([7; 32]))),
            Err(ChunkStorageError::Integrity)
        );
    }

    #[test]
    fn ordered_managed_storage_equal_restaging_is_not_new_storage() {
        // This is intentionally an internal receipt: staging/accounting is
        // below public row APIs, and it needs the durable ordered chunk plane.
        let storage = crate::storage::MemoryStorage::new(&[crate::db::LARGE_VALUE_METADATA_CF]);
        let layout = Rc::new(
            block_on(LayoutStorage::new(
                storage,
                crate::storage::StorageLayout::Identity,
            ))
            .unwrap(),
        );
        let backend = Rc::new(OrderedChunkStorage::new(Rc::downgrade(&layout)));
        let managed = ManagedChunkStorage::new(backend);
        let bytes = Bytes::from_static(b"ordered managed immutable bytes");
        let hash = object_hash(&bytes);
        let locator = Locator::from_seed(b"ordered-managed-opaque-locator");
        let chunk = StagedChunk {
            node_ref: crate::large_values::NodeRef {
                object_hash: hash,
                locator,
            },
            encoded: bytes.to_vec(),
        };

        let installed = block_on(managed.stage(vec![chunk.clone()])).unwrap();
        let restaged = block_on(managed.stage(vec![chunk])).unwrap();

        assert!(installed.encoded_bytes > 0);
        assert_eq!(restaged, Default::default());
    }

    #[test]
    fn ordered_chunk_storage_classifies_legacy_equal_mapping_as_existing() {
        // This is intentionally an internal compatibility receipt: the
        // legacy on-disk chunk representation predates the install receipt.
        let storage = crate::storage::MemoryStorage::new(&[crate::db::LARGE_VALUE_METADATA_CF]);
        let layout = Rc::new(
            block_on(LayoutStorage::new(
                storage,
                crate::storage::StorageLayout::Identity,
            ))
            .unwrap(),
        );
        let backend = OrderedChunkStorage::new(Rc::downgrade(&layout));
        let bytes = Bytes::from_static(b"legacy immutable chunk bytes");
        let hash = object_hash(&bytes);
        let locator = Locator::from_seed(b"legacy-ordered-chunk-locator");
        block_on(layout.set(
            crate::db::LARGE_VALUE_METADATA_CF.to_owned(),
            OrderedChunkStorage::key(locator.as_bytes()),
            OrderedChunkStorage::encode(hash, &bytes),
        ))
        .unwrap();

        assert_eq!(
            block_on(backend.put_if_absent(locator, hash, bytes.clone())).unwrap(),
            Some((hash, bytes))
        );
    }

    #[test]
    fn concurrent_remote_resolution_never_overwrites_an_immutable_locator() {
        // This is intentionally an internal receipt: the race belongs to the
        // Groove-owned blob plane below Database's row APIs. It nevertheless
        // exercises the public-to-Jazz StorageChunkProvider remote-resolution
        // path, which is where two independent requests meet that plane.
        let (inner, _control) =
            crate::storage::TestStorage::controlled(&[crate::db::LARGE_VALUE_METADATA_CF]);
        let layout = Rc::new(
            block_on(LayoutStorage::new(
                inner,
                crate::storage::StorageLayout::Identity,
            ))
            .unwrap(),
        );
        let backend = Rc::new(OrderedChunkStorage::new(Rc::downgrade(&layout)));
        let managed = Rc::new(ManagedChunkStorage::new(backend.clone()));
        let locator = Locator::from_seed(b"one-locator-two-remote-winners");
        let first = Bytes::from_static(b"first authenticated remote chunk");
        let second = Bytes::from_static(b"second authenticated remote chunk");
        let first_request = ChunkRequest {
            object_hash: object_hash(&first).0,
            locator,
        };
        let second_request = ChunkRequest {
            object_hash: object_hash(&second).0,
            locator,
        };
        let first_provider = StorageChunkProvider::with_resolver(
            managed.clone(),
            Rc::new(StaticResolver(first.clone())),
        );
        let second_provider =
            StorageChunkProvider::with_resolver(managed, Rc::new(StaticResolver(second.clone())));

        // TestStorage makes the two initial absence reads, and then each
        // conditional write, yield. The old get-then-Set implementation let
        // both requests observe absence and overwrote the first bytes here.
        let (first_result, second_result) = block_on(async {
            futures::join!(
                first_provider.get(first_request.clone()),
                second_provider.get(second_request.clone()),
            )
        });

        let winner = match (&first_result, &second_result) {
            (Ok(bytes), Err(ChunkError::Backend(message))) => {
                assert!(message.contains("opaque locator already names different content"));
                bytes.clone()
            }
            (Err(ChunkError::Backend(message)), Ok(bytes)) => {
                assert!(message.contains("opaque locator already names different content"));
                bytes.clone()
            }
            other => panic!("exactly one remote resolution must win: {other:?}"),
        };
        let (stored_hash, stored_bytes) =
            block_on(ChunkKvStorage::get_exact(backend.as_ref(), locator))
                .unwrap()
                .expect("the winner must remain durable");
        assert_eq!(stored_bytes, winner);
        assert_eq!(stored_hash, object_hash(&winner));
    }

    #[test]
    fn stale_chunk_delete_cannot_remove_a_newer_durable_mapping() {
        block_on(async {
            let (inner, control) =
                crate::storage::TestStorage::controlled(&[crate::db::LARGE_VALUE_METADATA_CF]);
            let layout = Rc::new(
                LayoutStorage::new(inner, crate::storage::StorageLayout::Identity)
                    .await
                    .unwrap(),
            );
            let backend = OrderedChunkStorage::new(Rc::downgrade(&layout));
            let locator = Locator::from_seed(b"conditional-chunk-delete-race");
            let old_bytes = Bytes::from_static(b"old authenticated chunk");
            let old_hash = object_hash(&old_bytes);
            let new_bytes = Bytes::from_static(b"new authenticated chunk");
            let new_hash = object_hash(&new_bytes);

            assert_eq!(
                backend
                    .put_if_absent(locator, old_hash, old_bytes.clone())
                    .await
                    .unwrap(),
                None
            );
            control.take_observed();

            // Freeze the stale cleanup after its integrity read but before its
            // durable compare-and-delete. Another completed reclamation frees
            // the old locator and a new owner reuses it before this stale
            // cleanup reaches the durable compare.
            control.pause_on(crate::storage::TestStorageOperation::WriteMany);
            let mut delete = Box::pin(backend.delete_exact(locator, old_hash));
            assert!(futures::poll!(delete.as_mut()).is_pending());
            assert_eq!(
                control.take_observed(),
                vec![crate::storage::TestStorageOperation::WriteMany]
            );

            layout
                .delete(
                    crate::db::LARGE_VALUE_METADATA_CF.to_owned(),
                    OrderedChunkStorage::key(locator.as_bytes()),
                )
                .await
                .unwrap();
            control.resume_operation(crate::storage::TestStorageOperation::WriteMany);
            assert_eq!(
                backend
                    .put_if_absent(locator, new_hash, new_bytes.clone())
                    .await
                    .unwrap(),
                None
            );
            delete.await.unwrap();

            assert_eq!(
                backend.get_exact(locator).await.unwrap(),
                Some((new_hash, new_bytes))
            );
        });
    }
}
