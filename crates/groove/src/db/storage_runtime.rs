//! Resident Groove state paired with one pollable persistence scheduler.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};

use crate::storage::pollable::{
    OwnedStorageOperation, OwnedStorageRequest, OwnedStorageResponse, PollableOrderedKvStorage,
};

use super::*;

static NEXT_PERSISTENCE_UNIT_ID: AtomicU64 = AtomicU64::new(1);

/// Identity of one ordered, indivisible host persistence closure.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct PersistenceUnitId(u64);

struct QueuedPersistenceRequest {
    unit: PersistenceUnitId,
    request: OwnedStorageRequest,
    completes_unit: bool,
}

/// FIFO scheduler shared by immediate and suspending durable backends.
///
/// Units may contain several storage-atomic batches. Their completion is
/// reported only after the final batch succeeds, allowing hosts to release a
/// Fate/ViewUpdate closure at exactly the durable boundary.
#[doc(hidden)]
pub struct PersistenceQueue {
    persistence: Box<dyn PollableOrderedKvStorage>,
    pending: VecDeque<QueuedPersistenceRequest>,
    completed_without_io: VecDeque<PersistenceUnitId>,
}

impl PersistenceQueue {
    #[doc(hidden)]
    pub fn new(persistence: Box<dyn PollableOrderedKvStorage>) -> Self {
        Self {
            persistence,
            pending: VecDeque::new(),
            completed_without_io: VecDeque::new(),
        }
    }

    #[doc(hidden)]
    pub fn enqueue_unit(&mut self, batches: Vec<PendingPersistenceBatch>) -> PersistenceUnitId {
        let unit = PersistenceUnitId(NEXT_PERSISTENCE_UNIT_ID.fetch_add(1, Ordering::Relaxed));
        let batch_count = batches.len();
        if batch_count == 0 {
            self.completed_without_io.push_back(unit);
        }
        for (index, batch) in batches.into_iter().enumerate() {
            self.pending.push_back(QueuedPersistenceRequest {
                unit,
                request: OwnedStorageRequest::new(OwnedStorageOperation::Commit(
                    batch.into_operations(),
                )),
                completes_unit: index + 1 == batch_count,
            });
        }
        unit
    }

    /// Poll in FIFO order and return every whole unit completed in this call.
    #[doc(hidden)]
    pub fn poll(
        &mut self,
        context: &mut Context<'_>,
    ) -> Poll<Result<Vec<PersistenceUnitId>, Error>> {
        let mut completed = self.completed_without_io.drain(..).collect::<Vec<_>>();
        while let Some(queued) = self.pending.front() {
            match self.persistence.poll_request(&queued.request, context) {
                Poll::Pending => {
                    return if completed.is_empty() {
                        Poll::Pending
                    } else {
                        Poll::Ready(Ok(completed))
                    };
                }
                Poll::Ready(Ok(OwnedStorageResponse::Committed)) => {
                    let queued = self
                        .pending
                        .pop_front()
                        .expect("front request remains queued");
                    if queued.completes_unit {
                        completed.push(queued.unit);
                    }
                }
                Poll::Ready(Ok(response)) => {
                    return Poll::Ready(Err(crate::storage::Error::Backend {
                        backend: "pollable",
                        message: format!("commit returned unexpected response {response:?}"),
                    }
                    .into()));
                }
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error.into())),
            }
        }
        Poll::Ready(Ok(completed))
    }

    #[doc(hidden)]
    pub fn is_empty(&self) -> bool {
        self.pending.is_empty() && self.completed_without_io.is_empty()
    }

    #[doc(hidden)]
    pub fn cancel_all(&mut self) -> Result<(), Error> {
        let mut first_error = None;
        for queued in self.pending.drain(..) {
            if let Err(error) = self.persistence.cancel_request(queued.request.id()) {
                first_error.get_or_insert(error);
            }
        }
        self.completed_without_io.clear();
        first_error.map_or(Ok(()), |error| Err(error.into()))
    }
}

/// One Groove evaluator whose local state stays resident while persistence may
/// suspend.
///
/// This is an internal migration surface. Immediate and asynchronous durable
/// stores use the same queue and poll path; their only difference is whether
/// [`PollableOrderedKvStorage::poll_request`] returns `Ready` on its first poll.
#[doc(hidden)]
pub struct PollableDatabase<S>
where
    S: OrderedKvStorage,
{
    resident: Database<S>,
    persistence: PersistenceQueue,
}

/// Query-driven database over a synchronous resident working set and a
/// pollable durable source.
///
/// Reads run normally until the cache reports one exact missing input. The
/// same operation then owns that request across suspension, admits its result,
/// and retries evaluation. No startup-wide scan is performed.
#[doc(hidden)]
pub struct DemandDrivenDatabase {
    database: Database<crate::storage::DemandLoadedStorage>,
    cache: crate::storage::DemandLoadedStorage,
    persistence: Box<dyn PollableOrderedKvStorage>,
    pending_read: Option<OwnedStorageRequest>,
    pending_persistence: VecDeque<OwnedStorageRequest>,
}

impl DemandDrivenDatabase {
    #[doc(hidden)]
    pub fn new(
        schema: crate::schema::DatabaseSchema,
        persistence: Box<dyn PollableOrderedKvStorage>,
    ) -> Result<Self, Error> {
        let column_families = schema.column_families();
        let cache = crate::storage::DemandLoadedStorage::new(&column_families);
        let database = Database::new(schema, cache.clone())?;
        Ok(Self {
            database,
            cache,
            persistence,
            pending_read: None,
            pending_persistence: VecDeque::new(),
        })
    }

    /// Poll a retryable read. The closure may execute more than once but must
    /// not mutate external state.
    #[doc(hidden)]
    pub fn poll_read<T>(
        &mut self,
        context: &mut Context<'_>,
        mut read: impl FnMut(&Database<crate::storage::DemandLoadedStorage>) -> Result<T, Error>,
    ) -> Poll<Result<T, Error>> {
        loop {
            if let Some(request) = self.pending_read.as_ref() {
                match self.persistence.poll_request(request, context) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Ok(response)) => {
                        let request = self
                            .pending_read
                            .take()
                            .expect("polled read request remains pending");
                        self.cache.admit(request.operation().clone(), response)?;
                    }
                    Poll::Ready(Err(error)) => return Poll::Ready(Err(error.into())),
                }
            }
            match read(&self.database) {
                Ok(value) => return Poll::Ready(Ok(value)),
                Err(Error::Storage(error)) => match *error {
                    crate::storage::Error::NotResident { request } => {
                        self.pending_read = Some(OwnedStorageRequest::new(*request));
                    }
                    error => return Poll::Ready(Err(error.into())),
                },
                Err(Error::IvmRuntime(crate::ivm::IvmRuntimeError::Storage(
                    crate::storage::Error::NotResident { request },
                ))) => {
                    self.pending_read = Some(OwnedStorageRequest::new(*request));
                }
                Err(error) => return Poll::Ready(Err(error)),
            }
        }
    }

    /// Poll prerequisite durable reads, then execute exactly one real resident
    /// commit and return its owned persistence batch. The subscription effects
    /// of that commit are published synchronously before this method returns
    /// `Ready`; only prerequisite cache filling may suspend it.
    #[doc(hidden)]
    pub fn poll_commit_batch(
        &mut self,
        context: &mut Context<'_>,
        batch: &mut Option<DatabaseBatch>,
    ) -> Poll<Result<PendingPersistenceBatch, Error>> {
        loop {
            if let Some(request) = self.pending_read.as_ref() {
                match self.persistence.poll_request(request, context) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Ok(response)) => {
                        let request = self
                            .pending_read
                            .take()
                            .expect("polled commit input remains pending");
                        self.cache.admit(request.operation().clone(), response)?;
                    }
                    Poll::Ready(Err(error)) => return Poll::Ready(Err(error.into())),
                }
            }
            let pending_batch = batch
                .as_ref()
                .expect("commit batch is consumed exactly once after Ready");
            match self.database.preflight_batch_storage_inputs(pending_batch) {
                Ok(()) => {
                    let batch = batch.take().expect("preflight retains commit batch");
                    return Poll::Ready(self.database.commit_batch_for_async_persistence(batch));
                }
                Err(Error::Storage(error)) => match *error {
                    crate::storage::Error::NotResident { request } => {
                        self.pending_read = Some(OwnedStorageRequest::new(*request));
                    }
                    error => return Poll::Ready(Err(error.into())),
                },
                Err(Error::IvmRuntime(crate::ivm::IvmRuntimeError::Storage(
                    crate::storage::Error::NotResident { request },
                ))) => {
                    self.pending_read = Some(OwnedStorageRequest::new(*request));
                }
                Err(error) => return Poll::Ready(Err(error)),
            }
        }
    }

    /// Poll the cold inputs for a subscription, then register it exactly once
    /// in the resident runtime.
    #[doc(hidden)]
    pub fn poll_subscribe_one_sink(
        &mut self,
        context: &mut Context<'_>,
        graph: &mut Option<GraphBuilder>,
    ) -> Poll<Result<Subscription, Error>> {
        loop {
            if let Some(request) = self.pending_read.as_ref() {
                match self.persistence.poll_request(request, context) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Ok(response)) => {
                        let request = self
                            .pending_read
                            .take()
                            .expect("polled subscription input remains pending");
                        self.cache.admit(request.operation().clone(), response)?;
                    }
                    Poll::Ready(Err(error)) => return Poll::Ready(Err(error.into())),
                }
            }
            let pending_graph = graph
                .as_ref()
                .expect("subscription graph is consumed exactly once after Ready");
            match self
                .database
                .preflight_subscription_storage_inputs(pending_graph)
            {
                Ok(()) => {
                    let graph = graph.take().expect("preflight retains subscription graph");
                    return Poll::Ready(self.database.subscribe_one_sink(graph));
                }
                Err(Error::Storage(error)) => match *error {
                    crate::storage::Error::NotResident { request } => {
                        self.pending_read = Some(OwnedStorageRequest::new(*request));
                    }
                    error => return Poll::Ready(Err(error.into())),
                },
                Err(Error::IvmRuntime(crate::ivm::IvmRuntimeError::Storage(
                    crate::storage::Error::NotResident { request },
                ))) => {
                    self.pending_read = Some(OwnedStorageRequest::new(*request));
                }
                Err(error) => return Poll::Ready(Err(error)),
            }
        }
    }

    /// Enqueue a resident-visible commit for ordered durable persistence.
    #[doc(hidden)]
    pub fn enqueue_persistence(&mut self, batch: PendingPersistenceBatch) {
        self.pending_persistence.push_back(OwnedStorageRequest::new(
            OwnedStorageOperation::Commit(batch.into_operations()),
        ));
    }

    /// Poll queued durable writes in FIFO order. Immediate backends drain in
    /// this call; asynchronous backends retain the queue head across wakeups.
    #[doc(hidden)]
    pub fn poll_persistence(&mut self, context: &mut Context<'_>) -> Poll<Result<(), Error>> {
        while let Some(request) = self.pending_persistence.front() {
            match self.persistence.poll_request(request, context) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Ok(OwnedStorageResponse::Committed)) => {
                    self.pending_persistence.pop_front();
                }
                Poll::Ready(Ok(response)) => {
                    self.database.mark_async_persistence_failed();
                    return Poll::Ready(Err(crate::storage::Error::Backend {
                        backend: "pollable",
                        message: format!("commit returned unexpected response {response:?}"),
                    }
                    .into()));
                }
                Poll::Ready(Err(error)) => {
                    self.database.mark_async_persistence_failed();
                    return Poll::Ready(Err(error.into()));
                }
            }
        }
        Poll::Ready(Ok(()))
    }

    #[doc(hidden)]
    pub fn resident(&self) -> &Database<crate::storage::DemandLoadedStorage> {
        &self.database
    }

    #[doc(hidden)]
    pub fn resident_mut(&mut self) -> &mut Database<crate::storage::DemandLoadedStorage> {
        &mut self.database
    }
}

impl<S> PollableDatabase<S>
where
    S: OrderedKvStorage,
{
    #[doc(hidden)]
    pub fn new(resident: Database<S>, persistence: Box<dyn PollableOrderedKvStorage>) -> Self {
        Self {
            resident,
            persistence: PersistenceQueue::new(persistence),
        }
    }

    /// Apply one batch locally now and enqueue its exact durable operations.
    #[doc(hidden)]
    pub fn commit_batch(&mut self, batch: DatabaseBatch) -> Result<(), Error> {
        let persistence = self.resident.commit_batch_for_async_persistence(batch)?;
        self.persistence.enqueue_unit(vec![persistence]);
        Ok(())
    }

    /// Poll queued persistence in commit order, draining every immediate
    /// operation in the same call.
    #[doc(hidden)]
    pub fn poll_persistence(&mut self, context: &mut Context<'_>) -> Poll<Result<(), Error>> {
        loop {
            match self.persistence.poll(context) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Ok(_)) if self.persistence.is_empty() => {
                    return Poll::Ready(Ok(()));
                }
                Poll::Ready(Ok(_)) => continue,
                Poll::Ready(Err(error)) => {
                    self.resident.mark_async_persistence_failed();
                    return Poll::Ready(Err(error));
                }
            }
        }
    }

    #[doc(hidden)]
    pub fn has_pending_persistence(&self) -> bool {
        !self.persistence.is_empty()
    }

    /// Cancel queued persistence and poison resident state. Optimistic local
    /// publication cannot be represented as a rollback after cancellation.
    #[doc(hidden)]
    pub fn cancel_pending_persistence(&mut self) -> Result<(), Error> {
        let result = self.persistence.cancel_all();
        self.resident.mark_async_persistence_failed();
        result
    }

    #[doc(hidden)]
    pub fn resident(&self) -> &Database<S> {
        &self.resident
    }

    #[doc(hidden)]
    pub fn resident_mut(&mut self) -> &mut Database<S> {
        &mut self.resident
    }
}
