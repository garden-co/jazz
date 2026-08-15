//! Executable design spike for an async persistent Jazz runtime.
//!
//! This is intentionally a small state machine rather than a premature async
//! conversion of `Db`. It fixes three non-negotiable boundaries for that
//! conversion: page loads may suspend, one received Jazz commit is persisted
//! as one encoded batch, and no protocol publication is released before that
//! batch has committed. `MemoryStorage` can drive the same interface to
//! completion in one poll.
//!
//! `OrderedKvStorage` is synchronous today, and `NodeState`/`Db::tick` call it
//! while computing maintained views and assembling `CommitUnit` consequences.
//! A production conversion must retain the pending state shown here at the
//! `NodeState` tick boundary, rather than letting a storage future escape from
//! an IVM operator or a borrowed scan callback.

use std::cell::RefCell;
use std::rc::Rc;
use std::task::Poll;

use groove::storage::OwnedWriteOperation;

/// A storage page read which may need an asynchronous backing-store round trip.
pub trait PollablePageStore {
    /// Opaque identity for one cold-page request.
    type Request: Copy + Eq;

    /// Start a page load. The store owns a copy of the key once it returns a
    /// token; a `Pending` result has retained no caller borrow.
    fn try_begin_page(&mut self, key: &[u8]) -> Poll<Result<Self::Request, String>>;

    /// Poll exactly the load identified by `request`.
    fn poll_page(&mut self, request: Self::Request) -> Poll<Result<Option<Vec<u8>>, String>>;

    /// Discard a request whose completion is no longer useful. A failed cancel
    /// is ambiguous and callers must not resume that token after restart.
    fn cancel_page(&mut self, request: Self::Request) -> Result<(), String>;
}

/// Owns a cold page request across polls; an in-memory store resolves it in the
/// first call, while an IndexedDB page store can return `Pending`.
#[derive(Debug)]
pub struct PollablePageLoad<R> {
    requested: Option<Vec<u8>>,
    request: Option<R>,
    terminal_error: Option<String>,
}

impl<R> Default for PollablePageLoad<R> {
    fn default() -> Self {
        Self {
            requested: None,
            request: None,
            terminal_error: None,
        }
    }
}

impl<R: Copy> PollablePageLoad<R> {
    /// Start or resume a page load. Resuming a different key before completion
    /// is a scheduler bug, so it fails closed.
    pub fn poll(
        &mut self,
        store: &mut impl PollablePageStore<Request = R>,
        key: &[u8],
    ) -> Poll<Result<Option<Vec<u8>>, String>> {
        if let Some(error) = &self.terminal_error {
            return Poll::Ready(Err(error.clone()));
        }
        match &self.requested {
            Some(requested) if requested != key => {
                return Poll::Ready(Err("attempted to replace an in-flight page load".into()));
            }
            Some(_) => {}
            None => self.requested = Some(key.to_vec()),
        }

        let request = match self.request {
            Some(request) => request,
            None => match store.try_begin_page(key) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Ok(request)) => {
                    self.request = Some(request);
                    request
                }
                Poll::Ready(Err(error)) => return self.fail(error),
            },
        };

        match store.poll_page(request) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(result) => {
                self.requested = None;
                self.request = None;
                Poll::Ready(result)
            }
        }
    }

    /// Terminalize before cancellation: an ambiguous backend cancellation may
    /// still complete later, but that old result cannot be resumed/published.
    pub fn cancel(
        &mut self,
        store: &mut impl PollablePageStore<Request = R>,
    ) -> Result<(), String> {
        let request = self.request.take();
        self.requested = None;
        self.terminal_error = Some("page load cancelled".into());
        request.map_or(Ok(()), |request| store.cancel_page(request))
    }

    fn fail(&mut self, error: String) -> Poll<Result<Option<Vec<u8>>, String>> {
        self.requested = None;
        self.request = None;
        self.terminal_error = Some(error.clone());
        Poll::Ready(Err(error))
    }
}

/// Scheduler-owned cancellation queue for dropped cold-page continuations.
///
/// A Rust `Drop` implementation cannot borrow the asynchronous page store, so
/// it must not claim to abort IndexedDB directly. The scheduler drains this
/// queue before starting replacement work (and during worker shutdown); a
/// cancellation error leaves the store outcome ambiguous and is surfaced to
/// the scheduler's recovery path.
#[derive(Debug, Clone)]
pub struct PageLoadCancellationRegistry<R> {
    cancelled: Rc<RefCell<Vec<R>>>,
}

impl<R> Default for PageLoadCancellationRegistry<R> {
    fn default() -> Self {
        Self {
            cancelled: Rc::new(RefCell::new(Vec::new())),
        }
    }
}

impl<R> PageLoadCancellationRegistry<R> {
    /// Create an empty scheduler-owned cancellation registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Drain dropped request tokens into the page store. This is deliberately
    /// explicit: dropping a continuation only queues cancellation.
    pub fn drain(&self, store: &mut impl PollablePageStore<Request = R>) -> Result<(), String>
    where
        R: Copy,
    {
        let cancelled = std::mem::take(&mut *self.cancelled.borrow_mut());
        let mut pending = cancelled.into_iter();
        while let Some(request) = pending.next() {
            if let Err(error) = store.cancel_page(request) {
                // Preserve the ambiguous token and every later token. A
                // scheduler must never lose cancellation ownership merely
                // because an earlier IndexedDB abort has an unknown outcome.
                let mut queued = self.cancelled.borrow_mut();
                queued.push(request);
                queued.extend(pending);
                return Err(error);
            }
        }
        Ok(())
    }
}

/// A page continuation owned by the persistent-runtime scheduler.
///
/// Its `Drop` only queues the in-flight request for the registry above; it
/// cannot synchronously abort IndexedDB. Callers must let the scheduler drain
/// the registry before accepting replacement cold loads.
#[derive(Debug)]
pub struct ScheduledPageLoad<R> {
    load: PollablePageLoad<R>,
    cancellations: PageLoadCancellationRegistry<R>,
}

impl<R> ScheduledPageLoad<R> {
    /// Register a new scheduler-owned page continuation.
    pub fn new(cancellations: PageLoadCancellationRegistry<R>) -> Self {
        Self {
            load: PollablePageLoad::default(),
            cancellations,
        }
    }
}

impl<R: Copy> ScheduledPageLoad<R> {
    /// Start or resume the owned load.
    pub fn poll(
        &mut self,
        store: &mut impl PollablePageStore<Request = R>,
        key: &[u8],
    ) -> Poll<Result<Option<Vec<u8>>, String>> {
        self.load.poll(store, key)
    }
}

impl<R> Drop for ScheduledPageLoad<R> {
    fn drop(&mut self) {
        if let Some(request) = self.load.request.take() {
            self.load.requested = None;
            self.load.terminal_error = Some("page load dropped by scheduler".into());
            self.cancellations.cancelled.borrow_mut().push(request);
        }
    }
}

/// A backend whose atomic encoded write batch can be pending.
pub trait PollableBatchStore {
    /// Opaque identity for one backend write request.
    type Request: Copy + Eq;

    /// Start an atomic encoded batch when no other batch is in flight.
    ///
    /// `Pending` means another request owns the serialized backend slot; the
    /// caller retains the batch and must retry this method. Once `Ready`, the
    /// backend has copied/taken all data it needs, so later polls are made with
    /// the returned token rather than a borrowed batch.
    fn try_begin_write_many(
        &mut self,
        batch: &[OwnedWriteOperation],
    ) -> Poll<Result<Self::Request, String>>;

    /// Poll the particular request started above. Implementations must reject
    /// a stale or foreign token rather than accidentally advancing another
    /// transaction's in-flight batch.
    fn poll_write_many(&mut self, request: Self::Request) -> Poll<Result<(), String>>;

    /// Cancel only a request which has not committed. Production IndexedDB
    /// cancellation maps to aborting its transaction; after restart the
    /// durable recovery record, not this in-memory token, decides retry.
    fn cancel_write_many(&mut self, request: Self::Request) -> Result<(), String>;
}

/// An owned subscription delivery computed by the maintained IVM graph.
///
/// This mirrors the output queued by `IvmRuntime::tick_with_params` immediately
/// after `TickEvaluator::update_node` (the `QueuedMultisinkDeltas::new` path).
/// The current runtime sends it synchronously there; a persistent runtime must
/// collect it here until the enclosing worker commit has completed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IvmSubscriptionNotification {
    /// Opaque subscription identity; production uses the runtime subscription id.
    pub subscription_id: u64,
    /// Owned encoded delta, so suspension never retains evaluator/storage borrows.
    pub encoded_delta: Vec<u8>,
}

/// Protocol consequences of the same received `CommitUnit`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkerSyncPublication {
    /// A view update for a downstream browser client.
    DownstreamViewUpdate,
    /// The worker's Local durability/fate receipt for the sender.
    DownstreamLocalFate,
    /// A core-facing upload, receipt, or broadcast consequence.
    UpstreamCore,
}

/// Everything `Db::tick` would otherwise release after one node/IVM tick.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerTickOutput {
    /// Outputs computed from `IvmRuntime::tick_with_params`.
    pub subscription_notifications: Vec<IvmSubscriptionNotification>,
    /// Normal sync-protocol messages, separated by destination semantics.
    pub sync_publications: Vec<WorkerSyncPublication>,
}

/// Pollable persistence gate around one fully assembled Jazz `CommitUnit`.
///
/// The caller computes the unit, maintained-view consequences, and encoded
/// writes first. It then gives this gate the complete owned batch plus every
/// resulting protocol message. `poll` returns those messages exactly once,
/// only after the backend reports a successful atomic batch commit.
#[derive(Debug)]
pub struct CommitPublicationGate<R> {
    batch: Vec<OwnedWriteOperation>,
    output: Option<WorkerTickOutput>,
    request: Option<R>,
    terminal_error: Option<String>,
}

impl<R: Copy> CommitPublicationGate<R> {
    /// Create a gate for the final encoded writes of one Jazz-level commit.
    pub fn new(batch: Vec<OwnedWriteOperation>, output: WorkerTickOutput) -> Self {
        Self {
            batch,
            output: Some(output),
            request: None,
            terminal_error: None,
        }
    }

    /// Drive the persistent boundary. Failure is terminal and fail-closed:
    /// callers receive an error and never receive the withheld messages.
    pub fn poll(
        &mut self,
        store: &mut impl PollableBatchStore<Request = R>,
    ) -> Poll<Result<WorkerTickOutput, String>> {
        if let Some(error) = &self.terminal_error {
            return Poll::Ready(Err(error.clone()));
        }
        if self.output.is_none() {
            return Poll::Ready(Err("commit publication was already released".into()));
        }

        let request = match self.request {
            Some(request) => request,
            None => match store.try_begin_write_many(&self.batch) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Ok(request)) => {
                    self.request = Some(request);
                    request
                }
                Poll::Ready(Err(error)) => return self.fail(error),
            },
        };

        match store.poll_write_many(request) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(())) => Poll::Ready(Ok(self.output.take().expect("checked above"))),
            Poll::Ready(Err(error)) => self.fail(error),
        }
    }

    /// Fail closed and give a persistent runtime a cancellation point for a
    /// shutdown before the backend reports completion.
    pub fn cancel(
        &mut self,
        store: &mut impl PollableBatchStore<Request = R>,
    ) -> Result<(), String> {
        // Terminalize first. `cancel_write_many` can itself fail because a
        // browser transaction is already committing and its outcome is
        // ambiguous. In that case this gate must remain permanently unable to
        // retry or release the precomputed IVM/protocol output.
        let request = self.request.take();
        self.output = None;
        self.terminal_error = Some("commit publication cancelled".into());
        request.map_or(Ok(()), |request| store.cancel_write_many(request))
    }

    fn fail(&mut self, error: String) -> Poll<Result<WorkerTickOutput, String>> {
        self.output = None;
        self.terminal_error = Some(error.clone());
        Poll::Ready(Err(error))
    }
}

#[cfg(test)]
mod tests {
    // This is intentionally an internal test: existing Db tick APIs cannot
    // suspend yet, so the public observable boundary does not exist. The test
    // proves the required state-machine invariant before that refactor.
    use std::collections::BTreeMap;
    use std::task::Poll;

    use super::*;

    #[derive(Default)]
    struct ControlledStore {
        ready: bool,
        fail: bool,
        cancel_fail: bool,
        pages: BTreeMap<Vec<u8>, Vec<u8>>,
        next_page_request: u64,
        page_in_flight: Option<(u64, Vec<u8>)>,
        committed: BTreeMap<(String, Vec<u8>), Vec<u8>>,
        calls: usize,
        next_request: u64,
        in_flight: Option<(u64, Vec<OwnedWriteOperation>)>,
    }

    impl PollablePageStore for ControlledStore {
        type Request = u64;

        fn try_begin_page(&mut self, key: &[u8]) -> Poll<Result<Self::Request, String>> {
            if self.page_in_flight.is_some() {
                return Poll::Pending;
            }
            self.next_page_request += 1;
            let request = self.next_page_request;
            self.page_in_flight = Some((request, key.to_vec()));
            Poll::Ready(Ok(request))
        }

        fn poll_page(&mut self, request: Self::Request) -> Poll<Result<Option<Vec<u8>>, String>> {
            if !self.ready {
                return Poll::Pending;
            }
            let Some((in_flight, key)) = self.page_in_flight.take() else {
                return Poll::Ready(Err("no in-flight page request".into()));
            };
            if in_flight != request {
                self.page_in_flight = Some((in_flight, key));
                return Poll::Ready(Err("stale page request token".into()));
            }
            Poll::Ready(Ok(self.pages.get(&key).cloned()))
        }

        fn cancel_page(&mut self, request: Self::Request) -> Result<(), String> {
            match self.page_in_flight.as_ref() {
                Some((in_flight, _)) if *in_flight == request => {
                    self.page_in_flight = None;
                    Ok(())
                }
                _ => Err("cannot cancel stale page request token".into()),
            }
        }
    }

    impl PollableBatchStore for ControlledStore {
        type Request = u64;

        fn try_begin_write_many(
            &mut self,
            batch: &[OwnedWriteOperation],
        ) -> Poll<Result<Self::Request, String>> {
            self.calls += 1;
            if self.in_flight.is_some() {
                return Poll::Pending;
            }
            self.next_request += 1;
            let request = self.next_request;
            self.in_flight = Some((request, batch.to_vec()));
            Poll::Ready(Ok(request))
        }

        fn poll_write_many(&mut self, request: Self::Request) -> Poll<Result<(), String>> {
            self.calls += 1;
            let Some((in_flight, batch)) = &self.in_flight else {
                return Poll::Ready(Err("no in-flight batch".into()));
            };
            if *in_flight != request {
                return Poll::Ready(Err("stale batch request token".into()));
            }
            if !self.ready {
                return Poll::Pending;
            }
            if self.fail {
                self.in_flight = None;
                return Poll::Ready(Err("injected IndexedDB transaction abort".into()));
            }

            // Work on a clone, then install once: either the entire encoded
            // Jazz commit is visible or none is.
            let mut next = self.committed.clone();
            for operation in batch {
                match operation {
                    OwnedWriteOperation::Set { cf, key, value } => {
                        next.insert((cf.clone(), key.clone()), value.clone());
                    }
                    OwnedWriteOperation::Delete { cf, key } => {
                        next.remove(&(cf.clone(), key.clone()));
                    }
                    OwnedWriteOperation::Delta { .. } => {
                        return Poll::Ready(Err("delta unsupported in spike store".into()));
                    }
                }
            }
            self.committed = next;
            self.in_flight = None;
            Poll::Ready(Ok(()))
        }

        fn cancel_write_many(&mut self, request: Self::Request) -> Result<(), String> {
            match self.in_flight.as_ref() {
                Some((in_flight, _)) if *in_flight == request => {
                    if self.cancel_fail {
                        return Err("injected ambiguous IndexedDB abort".into());
                    }
                    self.in_flight = None;
                    Ok(())
                }
                _ => Err("cannot cancel stale batch request token".into()),
            }
        }
    }

    #[derive(Default)]
    struct CancellationProbe {
        fail_next_cancel: bool,
        cancelled: Vec<u64>,
    }

    impl PollablePageStore for CancellationProbe {
        type Request = u64;

        fn try_begin_page(&mut self, _key: &[u8]) -> Poll<Result<Self::Request, String>> {
            unreachable!("cancellation probe never starts pages")
        }

        fn poll_page(&mut self, _request: Self::Request) -> Poll<Result<Option<Vec<u8>>, String>> {
            unreachable!("cancellation probe never polls pages")
        }

        fn cancel_page(&mut self, request: Self::Request) -> Result<(), String> {
            if self.fail_next_cancel {
                self.fail_next_cancel = false;
                return Err("injected ambiguous first cancellation".into());
            }
            self.cancelled.push(request);
            Ok(())
        }
    }

    fn two_row_jazz_transaction() -> Vec<OwnedWriteOperation> {
        vec![
            OwnedWriteOperation::Set {
                cf: "rows".into(),
                key: b"transaction-row-a".to_vec(),
                value: b"first".to_vec(),
            },
            OwnedWriteOperation::Set {
                cf: "rows".into(),
                key: b"transaction-row-b".to_vec(),
                value: b"second".to_vec(),
            },
        ]
    }

    fn worker_tick_output() -> WorkerTickOutput {
        WorkerTickOutput {
            subscription_notifications: vec![IvmSubscriptionNotification {
                subscription_id: 7,
                encoded_delta: b"maintained IVM delta".to_vec(),
            }],
            sync_publications: vec![
                WorkerSyncPublication::DownstreamViewUpdate,
                WorkerSyncPublication::DownstreamLocalFate,
                WorkerSyncPublication::UpstreamCore,
            ],
        }
    }

    #[test]
    fn cold_page_load_suspends_then_resumes_without_borrowing_storage() {
        let mut store = ControlledStore::default();
        store
            .pages
            .insert(b"page/7".to_vec(), b"resident page".to_vec());
        let mut load = PollablePageLoad::default();

        assert!(matches!(load.poll(&mut store, b"page/7"), Poll::Pending));
        store.ready = true;
        assert_eq!(
            load.poll(&mut store, b"page/7"),
            Poll::Ready(Ok(Some(b"resident page".to_vec())))
        );
    }

    #[test]
    fn cancelled_cold_load_cannot_resume_and_restart_uses_fresh_request_token() {
        let mut store = ControlledStore::default();
        store
            .pages
            .insert(b"page/7".to_vec(), b"resident page".to_vec());
        let mut abandoned = PollablePageLoad::default();

        assert!(matches!(
            abandoned.poll(&mut store, b"page/7"),
            Poll::Pending
        ));
        assert_eq!(store.next_page_request, 1);
        abandoned
            .cancel(&mut store)
            .expect("cancel pending page load");
        store.ready = true;
        assert!(
            matches!(abandoned.poll(&mut store, b"page/7"), Poll::Ready(Err(_))),
            "cancelled request resumed"
        );

        let mut restarted = PollablePageLoad::default();
        assert_eq!(
            restarted.poll(&mut store, b"page/7"),
            Poll::Ready(Ok(Some(b"resident page".to_vec())))
        );
        assert_eq!(
            store.next_page_request, 2,
            "restart reused ambiguous page token"
        );
    }

    #[test]
    fn dropped_pending_load_requires_scheduler_cleanup_before_fresh_token_can_complete() {
        let mut store = ControlledStore::default();
        store
            .pages
            .insert(b"page/9".to_vec(), b"new scheduler load".to_vec());
        let cancellations = PageLoadCancellationRegistry::new();
        let mut abandoned = ScheduledPageLoad::new(cancellations.clone());

        assert!(matches!(
            abandoned.poll(&mut store, b"page/9"),
            Poll::Pending
        ));
        drop(abandoned);
        store.ready = true;

        // Sensitivity plant: without the scheduler drain, the old token still
        // occupies the backing-store request slot and a new continuation cannot
        // pretend it owns that request.
        let mut fresh = ScheduledPageLoad::new(cancellations.clone());
        assert!(matches!(fresh.poll(&mut store, b"page/9"), Poll::Pending));
        cancellations
            .drain(&mut store)
            .expect("scheduler cancels dropped request before retrying");
        assert_eq!(
            fresh.poll(&mut store, b"page/9"),
            Poll::Ready(Ok(Some(b"new scheduler load".to_vec())))
        );
        assert_eq!(
            store.next_page_request, 2,
            "fresh load reused dropped token"
        );
    }

    #[test]
    fn cancellation_drain_requeues_failed_and_later_tokens_after_ambiguous_abort() {
        let registry = PageLoadCancellationRegistry::new();
        registry.cancelled.borrow_mut().extend([11_u64, 12_u64]);
        let mut store = CancellationProbe {
            fail_next_cancel: true,
            ..Default::default()
        };

        assert!(registry.drain(&mut store).is_err());
        assert_eq!(
            *registry.cancelled.borrow(),
            vec![11, 12],
            "failed or later cancellation token was dropped"
        );
        assert!(store.cancelled.is_empty());

        registry
            .drain(&mut store)
            .expect("retry queued cancellations");
        assert_eq!(store.cancelled, vec![11, 12]);
        assert!(registry.cancelled.borrow().is_empty());
    }

    #[test]
    fn pending_storage_holds_view_fate_and_core_messages_until_atomic_commit() {
        let mut store = ControlledStore::default();
        let mut gate = CommitPublicationGate::new(two_row_jazz_transaction(), worker_tick_output());

        assert!(matches!(gate.poll(&mut store), Poll::Pending));
        assert!(
            store.committed.is_empty(),
            "pending commit leaked storage visibility"
        );

        store.ready = true;
        let released = match gate.poll(&mut store) {
            Poll::Ready(Ok(messages)) => messages,
            other => panic!("expected committed publication, got {other:?}"),
        };
        assert_eq!(released.subscription_notifications.len(), 1);
        assert_eq!(released.sync_publications.len(), 3);
        assert_eq!(
            store.committed.len(),
            2,
            "whole Jazz transaction committed together"
        );
    }

    #[test]
    fn failed_storage_commit_leaks_neither_publication_nor_partial_transaction() {
        let mut store = ControlledStore {
            ready: true,
            fail: true,
            ..Default::default()
        };
        let mut gate = CommitPublicationGate::new(two_row_jazz_transaction(), worker_tick_output());

        assert!(matches!(gate.poll(&mut store), Poll::Ready(Err(_))));
        assert!(
            store.committed.is_empty(),
            "failed batch partially committed a Jazz transaction"
        );
        assert!(
            matches!(gate.poll(&mut store), Poll::Ready(Err(_))),
            "failure must remain fail-closed"
        );

        // A retry is a new durable request, never a second publication from
        // the failed gate. The backend slot is free only after its abort.
        store.fail = false;
        let mut retry =
            CommitPublicationGate::new(two_row_jazz_transaction(), worker_tick_output());
        assert!(matches!(retry.poll(&mut store), Poll::Ready(Ok(_))));
        assert_eq!(store.committed.len(), 2, "fresh retry commits atomically");
    }

    #[test]
    fn concurrent_gates_serialize_and_a_cancelled_first_batch_never_leaks() {
        let mut store = ControlledStore::default();
        let mut first =
            CommitPublicationGate::new(two_row_jazz_transaction(), worker_tick_output());
        let mut second = CommitPublicationGate::new(
            vec![OwnedWriteOperation::Set {
                cf: "rows".into(),
                key: b"later-transaction".to_vec(),
                value: b"later".to_vec(),
            }],
            worker_tick_output(),
        );

        assert!(matches!(first.poll(&mut store), Poll::Pending));
        assert!(matches!(second.poll(&mut store), Poll::Pending));
        assert_eq!(
            store.next_request, 1,
            "second gate must not start while first is pending"
        );

        first.cancel(&mut store).expect("cancel first batch");
        assert!(store.committed.is_empty(), "cancelled batch became visible");
        store.ready = true;
        let second_output = second.poll(&mut store);
        assert!(matches!(second_output, Poll::Ready(Ok(_))));
        assert_eq!(store.committed.len(), 1);
        assert!(
            matches!(first.poll(&mut store), Poll::Ready(Err(_))),
            "cancelled output retries fail closed"
        );
    }

    #[test]
    fn ambiguous_cancel_quarantines_precomputed_output_before_backend_abort_returns() {
        let mut store = ControlledStore {
            cancel_fail: true,
            ..Default::default()
        };
        let mut gate = CommitPublicationGate::new(two_row_jazz_transaction(), worker_tick_output());

        assert!(matches!(gate.poll(&mut store), Poll::Pending));
        assert!(
            gate.cancel(&mut store).is_err(),
            "injected abort outcome is ambiguous"
        );
        // The backend still owns request 1, but the Jazz gate has permanently
        // dropped its outputs. It cannot begin a replacement request or leak a
        // ViewUpdate/Fate/core message if that original request later commits.
        assert!(store.in_flight.is_some());
        store.ready = true;
        store.cancel_fail = false;
        assert!(
            matches!(store.poll_write_many(1), Poll::Ready(Ok(()))),
            "original backend request may still commit"
        );
        assert!(
            matches!(gate.poll(&mut store), Poll::Ready(Err(_))),
            "later commit must not release quarantined output"
        );
        assert_eq!(
            store.committed.len(),
            2,
            "only the original batch committed"
        );
        assert_eq!(
            store.next_request, 1,
            "gate started a second write after cancel failure"
        );
    }
}
