//! Serialize transaction reads and keep their lifetime inside the open batch.
//!
//! A caller can stage more writes while waiting, but each admitted read keeps
//! its original staging prefix. This module owns only in-memory read state.
use super::*;
use std::collections::VecDeque;
use std::future::poll_fn;
use std::task::{Poll, Waker};

#[derive(Default)]
pub(super) struct ReadState {
    next: u64,
    queue: VecDeque<u64>,
    wakers: Vec<Waker>,
    closed: bool,
    failure: Option<Error>,
}

fn unfinished() -> Error {
    Error::new(
        ErrorCode::NotObserved,
        "transaction closed with an unfinished read",
    )
}

impl ReadState {
    pub(super) fn check_commit(&self) -> Result<(), Error> {
        if self.closed || !self.queue.is_empty() {
            return Err(unfinished());
        }
        if let Some(error) = &self.failure {
            return Err(error.clone());
        }
        Ok(())
    }

    pub(super) fn fail(&mut self, error: Error) {
        self.failure.get_or_insert(error);
    }
}

pub(super) struct ReadGuard {
    state: Rc<RefCell<ReadState>>,
    ticket: u64,
    requires_evidence: bool,
    finished: bool,
}

impl ReadGuard {
    pub(super) fn require_evidence(&mut self) {
        self.requires_evidence = true;
        self.finished = false;
    }

    pub(super) fn finish<T>(&mut self, result: &Result<T, Error>) {
        if let Err(error) = result {
            self.state.borrow_mut().fail(error.clone());
        }
        self.finished = true;
    }

    pub(super) fn watch(&self, waker: &Waker) {
        let mut state = self.state.borrow_mut();
        if !state.wakers.iter().any(|prior| prior.will_wake(waker)) {
            state.wakers.push(waker.clone());
        }
    }

    pub(super) async fn wait(&self) -> Result<(), Error> {
        poll_fn(|cx| {
            let mut state = self.state.borrow_mut();
            if state.closed {
                return Poll::Ready(Err(unfinished()));
            }
            if state.queue.front() == Some(&self.ticket) {
                return Poll::Ready(Ok(()));
            }
            if !state.wakers.iter().any(|prior| prior.will_wake(cx.waker())) {
                state.wakers.push(cx.waker().clone());
            }
            Poll::Pending
        })
        .await
    }
}

impl Drop for ReadGuard {
    fn drop(&mut self) {
        // Ordinary reads may be cancelled without invalidating the batch.
        // Wake after releasing the state borrow: an executor can poll eagerly.
        let wakers = {
            let mut state = self.state.borrow_mut();
            if self.requires_evidence && !self.finished {
                state.fail(Error::new(
                    ErrorCode::NotObserved,
                    "an accepted transaction read was cancelled",
                ));
            }
            state.queue.retain(|ticket| *ticket != self.ticket);
            std::mem::take(&mut state.wakers)
        };
        for waker in wakers {
            waker.wake();
        }
    }
}

impl Db {
    pub(super) fn transaction_read_guard(&self, id: OpenTransactionId) -> Result<ReadGuard, Error> {
        let inner = self.inner.staging();
        let batch = inner
            .batches
            .get(&id)
            .ok_or_else(|| rejected("transaction is not open"))?;
        let state = batch.reads_state.clone();
        let ticket = {
            let mut state = state.borrow_mut();
            if state.closed {
                return Err(unfinished());
            }
            let ticket = state.next;
            state.next += 1;
            state.queue.push_back(ticket);
            ticket
        };
        Ok(ReadGuard {
            state,
            ticket,
            requires_evidence: false,
            finished: true,
        })
    }

    pub(super) fn check_read_guard(
        &self,
        id: OpenTransactionId,
        guard: &ReadGuard,
    ) -> Result<(), Error> {
        let inner = self.inner.staging();
        if guard.state.borrow().closed
            || !inner
                .batches
                .get(&id)
                .is_some_and(|batch| Rc::ptr_eq(&batch.reads_state, &guard.state))
        {
            return Err(unfinished());
        }
        if let Some(error) = &guard.state.borrow().failure {
            return Err(error.clone());
        }
        Ok(())
    }
}

pub(super) fn close_pending_reads(state: &Rc<RefCell<ReadState>>) {
    let wakers = {
        let mut state = state.borrow_mut();
        state.closed |= !state.queue.is_empty();
        std::mem::take(&mut state.wakers)
    };
    for waker in wakers {
        waker.wake();
    }
}

/// Notify failed readers only after the caller has released the engine borrow.
pub(super) fn fail_pending_reads(state: &Rc<RefCell<ReadState>>, error: Error) {
    let wakers = {
        let mut state = state.borrow_mut();
        state.fail(error);
        std::mem::take(&mut state.wakers)
    };
    for waker in wakers {
        waker.wake();
    }
}
