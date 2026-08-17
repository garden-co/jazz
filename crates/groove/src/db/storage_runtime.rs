//! Resident Groove state paired with one pollable persistence scheduler.

use std::collections::VecDeque;
use std::task::{Context, Poll};

use crate::storage::pollable::{
    OwnedStorageOperation, OwnedStorageRequest, OwnedStorageResponse, PollableOrderedKvStorage,
};

use super::*;

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
    persistence: Box<dyn PollableOrderedKvStorage>,
    pending: VecDeque<OwnedStorageRequest>,
}

impl<S> PollableDatabase<S>
where
    S: OrderedKvStorage,
{
    #[doc(hidden)]
    pub fn new(resident: Database<S>, persistence: Box<dyn PollableOrderedKvStorage>) -> Self {
        Self {
            resident,
            persistence,
            pending: VecDeque::new(),
        }
    }

    /// Apply one batch locally now and enqueue its exact durable operations.
    #[doc(hidden)]
    pub fn commit_batch(&mut self, batch: DatabaseBatch) -> Result<(), Error> {
        let persistence = self.resident.commit_batch_for_async_persistence(batch)?;
        self.pending
            .push_back(OwnedStorageRequest::new(OwnedStorageOperation::Commit(
                persistence.into_operations(),
            )));
        Ok(())
    }

    /// Poll queued persistence in commit order, draining every immediate
    /// operation in the same call.
    #[doc(hidden)]
    pub fn poll_persistence(&mut self, context: &mut Context<'_>) -> Poll<Result<(), Error>> {
        while let Some(request) = self.pending.front() {
            match self.persistence.poll_request(request, context) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Ok(OwnedStorageResponse::Committed)) => {
                    self.pending.pop_front();
                }
                Poll::Ready(Ok(response)) => {
                    self.resident.mark_async_persistence_failed();
                    return Poll::Ready(Err(crate::storage::Error::Backend {
                        backend: "pollable",
                        message: format!("commit returned unexpected response {response:?}"),
                    }
                    .into()));
                }
                Poll::Ready(Err(error)) => {
                    self.resident.mark_async_persistence_failed();
                    return Poll::Ready(Err(error.into()));
                }
            }
        }
        Poll::Ready(Ok(()))
    }

    #[doc(hidden)]
    pub fn has_pending_persistence(&self) -> bool {
        !self.pending.is_empty()
    }

    /// Cancel queued persistence and poison resident state. Optimistic local
    /// publication cannot be represented as a rollback after cancellation.
    #[doc(hidden)]
    pub fn cancel_pending_persistence(&mut self) -> Result<(), Error> {
        let mut first_error = None;
        for request in self.pending.drain(..) {
            if let Err(error) = self.persistence.cancel_request(request.id()) {
                first_error.get_or_insert(error);
            }
        }
        self.resident.mark_async_persistence_failed();
        first_error.map_or(Ok(()), |error| Err(error.into()))
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
