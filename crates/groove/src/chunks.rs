//! Host-provided immutable chunk retrieval for interruptible evaluation.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Poll, Waker};

use bytes::Bytes;
use thiserror::Error;

use crate::large_values::{ContentHash, StagedChunk, object_hash};

/// Opaque retrieval identity paired with the hash Groove must verify.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ChunkRequest {
    pub object_hash: [u8; 32],
    pub locator: Vec<u8>,
}

/// Executor-local future returned by a chunk capability.
pub type ChunkFuture<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;

/// Policy-blind immutable byte storage owned and orchestrated by Groove.
pub trait ChunkStorage {
    fn get(
        &self,
        locator: Vec<u8>,
        expected_hash: ContentHash,
    ) -> ChunkFuture<'_, Result<Bytes, ChunkStorageError>>;

    /// Atomically install immutable mappings. Equal restaging is idempotent.
    fn stage(&self, chunks: Vec<StagedChunk>) -> ChunkFuture<'_, Result<(), ChunkStorageError>>;

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

    fn stage(&self, chunks: Vec<StagedChunk>) -> ChunkFuture<'_, Result<(), ChunkStorageError>> {
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

    fn stage(&self, chunks: Vec<StagedChunk>) -> ChunkFuture<'_, Result<(), ChunkStorageError>> {
        Box::pin(async move {
            let existing = self.chunks.borrow();
            for chunk in &chunks {
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
            for chunk in chunks {
                stored
                    .entry(chunk.node_ref.locator.0)
                    .or_insert((chunk.node_ref.object_hash, Bytes::from(chunk.encoded)));
            }
            Ok(())
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
        }
    }

    pub(crate) fn get(
        &self,
        request: ChunkRequest,
    ) -> ChunkFuture<'static, Result<ChunkLease, ChunkError>> {
        let provider = Rc::clone(&self.provider);
        let cache = Rc::clone(&self.cache);
        let leases = Rc::clone(&self.leases);
        Box::pin(async move {
            {
                let mut cache = cache.borrow_mut();
                cache.clock = cache.clock.wrapping_add(1);
                let clock = cache.clock;
                if let Some((bytes, last_use)) = cache.entries.get_mut(&request) {
                    *last_use = clock;
                    return Ok(ChunkLease::new(bytes.clone(), leases));
                }
            }
            let bytes = provider.get(request.clone()).await?;
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
            Ok(ChunkLease::new(bytes, leases))
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
}
