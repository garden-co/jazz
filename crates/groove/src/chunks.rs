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

use crate::large_values::{ContentHash, StagedChunk, object_hash};
use crate::storage::{LayoutStorage, OrderedKvStorage, OwnedWriteOperation};

/// Opaque retrieval identity paired with the hash Groove must verify.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ChunkRequest {
    pub object_hash: [u8; 32],
    pub locator: Vec<u8>,
}

/// Executor-local future returned by a chunk capability.
pub type ChunkFuture<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;

/// Policy-blind async immutable byte-KV implemented by host storage adapters.
/// Groove, not the backend, validates object hashes and orchestrates staging.
pub trait ChunkKvStorage {
    fn get_exact(
        &self,
        locator: Vec<u8>,
    ) -> ChunkFuture<'_, Result<Option<(ContentHash, Bytes)>, ChunkStorageError>>;

    /// Install one immutable mapping or return the mapping already present.
    fn put_if_absent(
        &self,
        locator: Vec<u8>,
        hash: ContentHash,
        bytes: Bytes,
    ) -> ChunkFuture<'_, Result<Option<(ContentHash, Bytes)>, ChunkStorageError>>;

    fn delete_exact(
        &self,
        locator: Vec<u8>,
        expected_hash: ContentHash,
    ) -> ChunkFuture<'_, Result<(), ChunkStorageError>>;
}

/// Groove-owned integrity and staging layer over a policy-blind byte KV.
///
/// Every batch's size and object hashes are mechanically prevalidated before
/// its first backend put. A backend process crash can nevertheless occur after
/// one immutable put and before later puts or upload metadata: those bytes are
/// unreachable and have no metadata reclaimer entry. Closing that residual
/// crash-only orphan window requires a backend transaction spanning chunk puts
/// and metadata writes.
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
        locator: Vec<u8>,
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
                        chunk.node_ref.locator.0,
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
        locator: Vec<u8>,
        expected_hash: ContentHash,
    ) -> ChunkFuture<'_, Result<(), ChunkStorageError>> {
        self.backend.delete_exact(locator, expected_hash)
    }
}

/// Policy-blind immutable byte storage owned and orchestrated by Groove.
pub trait ChunkStorage {
    fn get(
        &self,
        locator: Vec<u8>,
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
        _locator: Vec<u8>,
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
    storage: Rc<dyn ChunkStorage>,
}

impl LocalChunkReader {
    pub(crate) fn new(storage: Rc<dyn ChunkStorage>) -> Self {
        Self { storage }
    }

    pub async fn get(
        &self,
        locator: Vec<u8>,
        expected_hash: ContentHash,
    ) -> Result<Bytes, ChunkStorageError> {
        self.storage.get(locator, expected_hash).await
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
        locator: Vec<u8>,
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
        locator: Vec<u8>,
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
type MemoryChunks = BTreeMap<Vec<u8>, (ContentHash, Bytes)>;

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
        locator: Vec<u8>,
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
                    .get(&chunk.node_ref.locator.0)
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
                    stored.entry(chunk.node_ref.locator.0)
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
        locator: Vec<u8>,
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
        locator: Vec<u8>,
    ) -> ChunkFuture<'_, Result<Option<(ContentHash, Bytes)>, ChunkStorageError>> {
        Box::pin(async move { Ok(self.chunks.borrow().get(&locator).cloned()) })
    }

    fn put_if_absent(
        &self,
        locator: Vec<u8>,
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
        locator: Vec<u8>,
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

    fn decode(value: Vec<u8>) -> Result<(ContentHash, Bytes), ChunkStorageError> {
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
        locator: Vec<u8>,
    ) -> ChunkFuture<'_, Result<Option<(ContentHash, Bytes)>, ChunkStorageError>> {
        Box::pin(async move {
            let storage = self.storage()?;
            let Some(value) = storage
                .get(
                    crate::db::LARGE_VALUE_METADATA_CF.to_owned(),
                    Self::key(&locator),
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
        locator: Vec<u8>,
        hash: ContentHash,
        bytes: Bytes,
    ) -> ChunkFuture<'_, Result<Option<(ContentHash, Bytes)>, ChunkStorageError>> {
        Box::pin(async move {
            let storage = self.storage()?;
            let key = Self::key(&locator);
            if let Some(existing) = storage
                .get(crate::db::LARGE_VALUE_METADATA_CF.to_owned(), key.clone())
                .await
                .map_err(|error| ChunkStorageError::Backend(error.to_string()))?
            {
                return Self::decode(existing).map(Some);
            }
            storage
                .write_many(vec![OwnedWriteOperation::Set {
                    cf: crate::db::LARGE_VALUE_METADATA_CF.to_owned(),
                    key,
                    value: Self::encode(hash, &bytes),
                }])
                .await
                .map_err(|error| ChunkStorageError::Backend(error.to_string()))?;
            Ok(None)
        })
    }

    fn delete_exact(
        &self,
        locator: Vec<u8>,
        expected_hash: ContentHash,
    ) -> ChunkFuture<'_, Result<(), ChunkStorageError>> {
        Box::pin(async move {
            let storage = self.storage()?;
            let key = Self::key(&locator);
            let Some(existing) = storage
                .get(crate::db::LARGE_VALUE_METADATA_CF.to_owned(), key.clone())
                .await
                .map_err(|error| ChunkStorageError::Backend(error.to_string()))?
            else {
                return Ok(());
            };
            let (hash, bytes) = Self::decode(existing)?;
            if hash != expected_hash || object_hash(&bytes) != expected_hash {
                return Err(ChunkStorageError::Integrity);
            }
            storage
                .delete(crate::db::LARGE_VALUE_METADATA_CF.to_owned(), key)
                .await
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
        Box::pin(async move {
            match self
                .storage
                .get(request.locator.clone(), ContentHash(request.object_hash))
                .await
            {
                Ok(bytes) => Ok(bytes),
                Err(ChunkStorageError::Unavailable) => {
                    let bytes = self.resolver.resolve(request.clone()).await?;
                    self.storage
                        .stage(vec![StagedChunk {
                            node_ref: crate::large_values::NodeRef {
                                object_hash: ContentHash(request.object_hash),
                                locator: crate::large_values::Locator(request.locator.clone()),
                            },
                            encoded: bytes.to_vec(),
                        }])
                        .await
                        .map_err(ChunkError::from)?;
                    self.observer
                        .installed(
                            crate::large_values::NodeRef {
                                object_hash: ContentHash(request.object_hash),
                                locator: crate::large_values::Locator(request.locator),
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
    #[error("chunk retrieval failed: {0}")]
    Backend(String),
    #[error("chunk bytes do not match the requested object hash")]
    Integrity,
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
    /// Requests currently suspended in the backing provider. Reclamation is
    /// deferred while this is non-zero because the requested locator may have
    /// been discovered from an active root before its bytes were leased.
    pub active_requests: usize,
}

#[derive(Default)]
struct ChunkActivityState {
    active_requests: usize,
    reclaiming: bool,
    waiters: Vec<Waker>,
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
        let provider = Rc::clone(&self.provider);
        let cache = Rc::clone(&self.cache);
        let leases = Rc::clone(&self.leases);
        let activity = Rc::clone(&self.activity);
        Box::pin(async move {
            let request_guard = ActiveChunkRequest::acquire(activity).await;
            {
                let mut cache = cache.borrow_mut();
                cache.clock = cache.clock.wrapping_add(1);
                let clock = cache.clock;
                if let Some((bytes, last_use)) = cache.entries.get_mut(&request) {
                    *last_use = clock;
                    let lease = ChunkLease::new(bytes.clone(), leases);
                    drop(request_guard);
                    return Ok(lease);
                }
            }
            let bytes = provider.get(request.clone()).await?;
            if bytes.len() > crate::large_values::MAX_ENCODED_NODE_BYTES {
                return Err(ChunkError::Integrity);
            }
            if crate::large_values::object_hash(&bytes).0 != request.object_hash {
                return Err(ChunkError::Integrity);
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
            let lease = ChunkLease::new(bytes, leases);
            drop(request_guard);
            Ok(lease)
        })
    }
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

    use futures::executor::block_on;

    use super::*;

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
            locator: b"opaque".to_vec(),
        };

        assert_eq!(block_on(chunks.get(request.clone())).unwrap(), bytes);
        assert_eq!(block_on(chunks.get(request)).unwrap(), bytes);
        assert_eq!(provider.calls.get(), 1);
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
            locator: b"opaque".to_vec(),
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
                locator: crate::large_values::Locator(vec![9; 16]),
            },
            encoded,
        }]));
        assert_eq!(result, Err(ChunkStorageError::Integrity));
        assert!(storage.is_empty());
    }

    #[test]
    fn byte_budget_evicts_verified_ownership_without_invalidating_live_bytes() {
        struct MapProvider(BTreeMap<Vec<u8>, Bytes>);
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
            locator: b"first".to_vec(),
        };
        let second_request = ChunkRequest {
            object_hash: crate::large_values::object_hash(&second).0,
            locator: b"second".to_vec(),
        };
        let provider = Rc::new(MapProvider(BTreeMap::from([
            (first_request.locator.clone(), first.clone()),
            (second_request.locator.clone(), second.clone()),
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
        let locator = b"opaque-locator".to_vec();
        let installed = block_on(managed.stage(vec![StagedChunk {
            node_ref: crate::large_values::NodeRef {
                object_hash: hash,
                locator: crate::large_values::Locator(locator.clone()),
            },
            encoded: bytes.to_vec(),
        }]))
        .unwrap();
        let restaged = block_on(managed.stage(vec![StagedChunk {
            node_ref: crate::large_values::NodeRef {
                object_hash: hash,
                locator: crate::large_values::Locator(locator.clone()),
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
}
