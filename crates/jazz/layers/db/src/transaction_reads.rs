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
        Ok(())
    }
}

pub(super) struct ReadGuard {
    state: Rc<RefCell<ReadState>>,
    ticket: u64,
}

impl ReadGuard {
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
        self.ensure_storage_owner_active()?;
        let inner = self.inner();
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
        Ok(ReadGuard { state, ticket })
    }

    pub(super) fn check_read_guard(
        &self,
        id: OpenTransactionId,
        guard: &ReadGuard,
    ) -> Result<(), Error> {
        self.ensure_storage_owner_active()?;
        let inner = self.inner();
        if guard.state.borrow().closed
            || !inner
                .batches
                .get(&id)
                .is_some_and(|batch| Rc::ptr_eq(&batch.reads_state, &guard.state))
        {
            return Err(unfinished());
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
