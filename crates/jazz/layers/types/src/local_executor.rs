//! Blocking execution of thread-affine futures without an async runtime.

use std::future::Future;
use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll, Wake, Waker};

/// Complete a future on the calling thread, parking until woken when it suspends.
///
/// Immediately ready futures avoid allocating a waker. Nested calls are allowed;
/// each waiter tracks its own notifications because thread park tokens are shared.
/// This does not drive an async runtime: any I/O or tasks needed to wake the
/// future must progress independently of this thread. On targets that cannot
/// park (including browser WASM), callers must use immediately ready futures.
pub fn block_on<F: Future>(future: F) -> F::Output {
    let mut future = pin!(future);
    if let Poll::Ready(value) = future
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    {
        return value;
    }
    struct ThreadWake {
        thread: std::thread::Thread,
        notified: AtomicBool,
    }
    impl Wake for ThreadWake {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.notified.store(true, Ordering::Release);
            self.thread.unpark();
        }
    }
    let waiter = Arc::new(ThreadWake {
        thread: std::thread::current(),
        notified: AtomicBool::new(false),
    });
    let waker = Waker::from(waiter.clone());
    let mut cx = Context::from_waker(&waker);
    loop {
        match future.as_mut().poll(&mut cx) {
            Poll::Ready(value) => return value,
            Poll::Pending => {
                while !waiter.notified.swap(false, Ordering::Acquire) {
                    std::thread::park();
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Executor-level tests are necessary to exercise wake ordering independently
    // of SQL providers, whose operations can complete without ever suspending.

    /// Alice receives a result without needing any wakeup.
    #[test]
    fn immediate_result() {
        assert_eq!(block_on(std::future::ready(42)), 42);
    }

    /// Bob delivers a result on another thread while Alice waits for it.
    /// alice polls -> bob sends + wakes -> alice receives
    #[test]
    fn cross_thread_wakeup() {
        let (send, receive) = futures::channel::oneshot::channel();
        let (started, start) = std::sync::mpsc::channel();
        let bob = std::thread::spawn(move || {
            start.recv().unwrap();
            send.send(42).unwrap();
        });
        let mut receive = pin!(receive);
        let mut started = Some(started);
        let mut polls = 0;
        let result = block_on(std::future::poll_fn(|cx| {
            polls += 1;
            let result = receive.as_mut().poll(cx);
            // Register the real waker before allowing Bob to reply.
            if polls == 2 {
                started.take().unwrap().send(()).unwrap();
            }
            result
        }));
        assert_eq!(result.unwrap(), 42);
        bob.join().unwrap();
    }

    /// Alice's nested call consumes the thread token, but her outer wake survives.
    /// outer wake -> nested consumes token -> outer Pending -> outer completes
    #[test]
    fn nested_call_preserves_outer_notification() {
        let mut polls = 0;
        let value = block_on(std::future::poll_fn(|cx| {
            polls += 1;
            // First poll uses the allocation-free fast path. On the second,
            // deliberately consume the park token belonging to the outer wake.
            if polls == 2 {
                cx.waker().wake_by_ref();
                block_on(async { std::thread::park() });
            }
            if polls >= 3 {
                Poll::Ready(42)
            } else {
                Poll::Pending
            }
        }));
        assert_eq!(value, 42);
    }
}
