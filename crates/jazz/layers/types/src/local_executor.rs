//! The tiny executor that drives ready-immediate thread-affine futures.

use std::future::Future;
use std::pin::pin;
use std::task::{Context, Poll, Waker};

/// Poll a ready-immediate thread-affine database future to completion.
///
/// This helper is intentionally tiny: it drives local-lane futures that are
/// expected to complete without an async runtime by using a no-op waker and
/// yielding the current thread when a future reports `Pending`.
pub fn block_on<F: Future>(future: F) -> F::Output {
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    let mut future = pin!(future);

    loop {
        match future.as_mut().poll(&mut cx) {
            Poll::Ready(value) => return value,
            Poll::Pending => std::thread::yield_now(),
        }
    }
}
