//! What a binding's host needs to run the runtime for an app, the same for
//! every binding that exposes it to JavaScript (Node's native module and the
//! browser's WebAssembly module):
//!
//! - [`bridge`]: the private store an app supplies from JavaScript, as a
//!   queue of requests that JavaScript answers.
//! - [`runtime`]: the runtime built from an app's own declarations over that
//!   store, its long-lived loop (account verification, key watches, the
//!   delivery pass), the calls the TypeScript handle makes, and the reports.
//!
//! - [`phrase`]: the recovery phrase, 24 words of a 32-byte recovery secret.
//!
//! A binding adds only what is its own: how a future is polled and how a
//! result becomes a promise.

pub mod bridge;
pub mod phrase;
pub mod runtime;

/// The device account's secret as native hosts pass it: its bytes are zeroized when it is dropped.
pub use jazz_e2ee::protocol::derive::RecoverySecret;

/// Futures that settle JavaScript promises on the binding's owning thread.
#[derive(Default)]
pub struct PromiseQueue(std::cell::RefCell<Vec<futures::future::LocalBoxFuture<'static, ()>>>);

impl PromiseQueue {
    pub fn submit(&self, future: impl std::future::Future<Output = ()> + 'static) {
        let mut future = Box::pin(future);
        if future
            .as_mut()
            .poll(&mut std::task::Context::from_waker(std::task::Waker::noop()))
            .is_pending()
        {
            self.0.borrow_mut().push(future);
        }
    }

    pub fn settle(&self) {
        // Settling a promise can synchronously submit another one.
        let mut pending = self.0.take();
        let mut context = std::task::Context::from_waker(std::task::Waker::noop());
        pending.retain_mut(|future| future.as_mut().poll(&mut context).is_pending());
        pending.append(&mut self.0.borrow_mut());
        self.0.replace(pending);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::channel::oneshot;
    use std::cell::Cell;
    use std::rc::Rc;

    /// Alice's pending call can submit Bob's promise as it settles. The queue
    /// must poll Bob again after his answer arrives. A queue-level test can
    /// synchronously re-enter settlement without relying on a JS scheduler.
    #[test]
    fn promises_submitted_during_settlement_survive() {
        let queue = Rc::new(PromiseQueue::default());
        let (alice, first) = oneshot::channel::<()>();
        let (bob, second) = oneshot::channel::<()>();
        let settled = Rc::new(Cell::new(false));
        let observed = settled.clone();
        let nested = queue.clone();
        queue.submit(async move {
            first.await.unwrap();
            nested.submit(async move {
                second.await.unwrap();
                observed.set(true);
            });
        });
        alice.send(()).unwrap();
        queue.settle();
        assert!(!settled.get());
        bob.send(()).expect("Bob's pending promise was dropped");
        queue.settle();
        assert!(settled.get());
    }
}
