//! Resident Groove state paired with one pollable persistence scheduler.

use std::collections::VecDeque;
use std::task::{Context, Poll};

use crate::storage::async_ordered::{
    OrderedKvStorage, OwnedStorageOperation, OwnedStorageRequest, OwnedStorageResponse,
};

use super::*;

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
    persistence: Box<dyn OrderedKvStorage>,
    acquisition: StorageAcquisition,
    pending_persistence: VecDeque<OwnedStorageRequest>,
}

/// Owns the one durable input request that prevents a resident core operation
/// from making progress. The evaluator remains synchronous; this driver
/// admits the missing input and reruns the operation from its explicit
/// pre-publication boundary.
#[doc(hidden)]
#[derive(Default)]
pub struct StorageAcquisition {
    pending: Option<OwnedStorageRequest>,
}

impl StorageAcquisition {
    /// Whether this driver already owns an issued backend request.
    pub fn is_pending(&self) -> bool {
        self.pending.is_some()
    }

    /// Poll one restartable resident operation, acquiring exact durable inputs
    /// until it can complete synchronously.
    pub fn poll<T, E>(
        &mut self,
        persistence: &mut dyn OrderedKvStorage,
        cache: &crate::storage::DemandLoadedStorage,
        context: &mut Context<'_>,
        mut attempt: impl FnMut() -> Result<T, E>,
        missing_input: impl Fn(E) -> Result<OwnedStorageOperation, E>,
    ) -> Poll<Result<T, E>>
    where
        E: From<crate::storage::Error>,
    {
        loop {
            if let Some(request) = self.pending.as_ref() {
                match persistence.poll_request(request, context) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Ok(response)) => {
                        let request = self
                            .pending
                            .take()
                            .expect("polled acquisition request remains pending");
                        cache
                            .admit(request.operation().clone(), response)
                            .map_err(E::from)?;
                    }
                    Poll::Ready(Err(error)) => {
                        // A backend error terminalizes this request identity.
                        // Retaining it would let a later, independent owner
                        // operation accidentally restart the completed request.
                        self.pending
                            .take()
                            .expect("failed acquisition request remains pending");
                        return Poll::Ready(Err(E::from(error)));
                    }
                }
            }
            match attempt() {
                Ok(value) => return Poll::Ready(Ok(value)),
                Err(error) => match missing_input(error) {
                    Ok(request) => {
                        self.pending = Some(OwnedStorageRequest::new(request));
                    }
                    Err(error) => return Poll::Ready(Err(error)),
                },
            }
        }
    }

    /// Cancel any backend work retained for the suspended operation.
    pub fn cancel(
        &mut self,
        persistence: &mut dyn OrderedKvStorage,
    ) -> Result<(), crate::storage::Error> {
        if let Some(request) = self.pending.take() {
            persistence.cancel_request(request.id())?;
        }
        Ok(())
    }
}

fn missing_storage_input(error: Error) -> Result<OwnedStorageOperation, Error> {
    match error {
        Error::Storage(error) => match *error {
            crate::storage::Error::NotResident { request } => Ok(*request),
            error => Err(error.into()),
        },
        Error::IvmRuntime(crate::ivm::IvmRuntimeError::Storage(
            crate::storage::Error::NotResident { request },
        )) => Ok(*request),
        error => Err(error),
    }
}

impl DemandDrivenDatabase {
    #[doc(hidden)]
    pub fn new(
        schema: crate::schema::DatabaseSchema,
        persistence: Box<dyn OrderedKvStorage>,
    ) -> Result<Self, Error> {
        let column_families = schema.column_families();
        let cache = crate::storage::DemandLoadedStorage::new(&column_families);
        let database = Database::new(schema, cache.clone())?;
        Ok(Self {
            database,
            cache,
            persistence,
            acquisition: StorageAcquisition::default(),
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
        self.poll_acquisition(context, |database| read(database))
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
        let pending_batch = batch
            .as_ref()
            .expect("commit batch is consumed exactly once after Ready");
        match self.poll_acquisition(context, |database| {
            database.prepare_batch_storage_inputs(pending_batch)
        }) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
            Poll::Ready(Ok(prepared)) => {
                batch.take().expect("preparation retains commit batch");
                Poll::Ready(
                    self.database
                        .commit_prepared_batch_for_async_persistence(prepared),
                )
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
        let pending_graph = graph
            .as_ref()
            .expect("subscription graph is consumed exactly once after Ready");
        match self.poll_acquisition(context, |database| {
            database.subscribe_one_sink(pending_graph.clone())
        }) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
            Poll::Ready(Ok(subscription)) => {
                graph
                    .take()
                    .expect("subscription graph remains owned until opening succeeds");
                Poll::Ready(Ok(subscription))
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

    /// Run one restartable resident operation. Fully resident work completes
    /// without touching persistence; a genuine miss is fenced behind every
    /// older queued commit before its read request reaches the backend.
    fn poll_acquisition<T>(
        &mut self,
        context: &mut Context<'_>,
        mut operation: impl FnMut(
            &mut Database<crate::storage::DemandLoadedStorage>,
        ) -> Result<T, Error>,
    ) -> Poll<Result<T, Error>> {
        if !self.acquisition.is_pending() {
            match operation(&mut self.database) {
                Ok(value) => return Poll::Ready(Ok(value)),
                Err(error) => {
                    if let Err(error) = missing_storage_input(error) {
                        return Poll::Ready(Err(error));
                    }
                }
            }
            match self.poll_persistence(context) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Ready(Ok(())) => {}
            }
        }
        self.acquisition.poll(
            self.persistence.as_mut(),
            &self.cache,
            context,
            || operation(&mut self.database),
            missing_storage_input,
        )
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
                        backend: "demand-driven",
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
