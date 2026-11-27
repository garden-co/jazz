//! Per-call control: cancelling a call, or giving it a deadline, ends its
//! waits on the server on the client's thread, as closing ends them for
//! every call. What the call wrote locally stays, and may still apply: its
//! refusal, if one comes, is reported as a mutation error and by the next
//! `wait_for_sync`, as a plain write's is.

use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use tokio::sync::watch;

/// Why a controlled call was cut short.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Interruption {
    /// [`CallControl::cancel`] was called.
    Cancelled,
    /// The control's timeout passed.
    Timeout,
}

impl Interruption {
    /// `"cancelled"` or `"timeout"`, as the bindings name it.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Cancelled => "cancelled",
            Self::Timeout => "timeout",
        }
    }
}

impl std::fmt::Display for Interruption {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Cancels the calls made through [`crate::Jazz::controlled`], or ends them
/// at a deadline: a binding makes one per suspending call and cancels it
/// when its coroutine or task is cancelled. Cheap to clone; every clone is
/// the same control.
///
/// A call made once the control has ended does nothing and fails with
/// [`crate::Error::Interrupted`]. A call it ends while waiting on the server
/// fails with [`crate::Error::Interrupted`], or
/// [`crate::Error::StillQueued`] when the call committed a write locally,
/// which may still apply.
#[derive(Clone, Debug)]
pub struct CallControl {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    ended: watch::Sender<bool>,
    reason: OnceLock<Interruption>,
    deadline: Option<Instant>,
}

impl CallControl {
    /// A control, ending its calls after `timeout` from now if given.
    pub fn new(timeout: Option<Duration>) -> Self {
        Self {
            inner: Arc::new(Inner {
                ended: watch::Sender::new(false),
                reason: OnceLock::new(),
                deadline: timeout.map(|timeout| Instant::now() + timeout),
            }),
        }
    }

    /// Ends its calls' waits now: they fail with
    /// [`Interruption::Cancelled`] (unless the timeout ended them first).
    pub fn cancel(&self) {
        self.end(Interruption::Cancelled);
    }

    /// Why its calls are cut short, once they are.
    pub fn interruption(&self) -> Option<Interruption> {
        if self.inner.reason.get().is_none()
            && self
                .inner
                .deadline
                .is_some_and(|deadline| Instant::now() >= deadline)
        {
            self.end(Interruption::Timeout);
        }
        self.inner.reason.get().copied()
    }

    fn end(&self, reason: Interruption) {
        if self.inner.reason.set(reason).is_ok() {
            self.inner.ended.send_replace(true);
        }
    }

    /// Reads true once it ended.
    pub(crate) fn signal(&self) -> watch::Receiver<bool> {
        self.inner.ended.subscribe()
    }

    /// Ends it at its deadline, if it has one: run on the client's thread
    /// beside a call it controls.
    pub(crate) async fn time_out(self) {
        match self.inner.deadline {
            Some(deadline) => {
                tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)).await;
                self.end(Interruption::Timeout);
            }
            None => std::future::pending().await,
        }
    }
}

/// Ends the waits of a call its caller dropped, as the drop happens; a call
/// that answered is disarmed first, so what it left running (a
/// subscription's stream) runs on.
pub(crate) struct EndOnDrop(Option<watch::Sender<bool>>);

impl EndOnDrop {
    pub(crate) fn new() -> (Self, watch::Receiver<bool>) {
        let (end, ends) = watch::channel(false);
        (Self(Some(end)), ends)
    }

    pub(crate) fn disarm(&mut self) {
        // Dropped unsent: its receivers never read true.
        self.0.take();
    }
}

impl Drop for EndOnDrop {
    fn drop(&mut self) {
        if let Some(end) = self.0.take() {
            end.send_replace(true);
        }
    }
}
