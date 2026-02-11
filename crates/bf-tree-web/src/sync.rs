// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#[cfg(all(feature = "shuttle", test))]
pub(crate) use shuttle::sync::*;

#[cfg(all(feature = "shuttle", test))]
pub(crate) use shuttle::thread;

#[cfg(not(all(feature = "shuttle", test)))]
pub(crate) use std::sync::*;

#[cfg(all(not(all(feature = "shuttle", test)), not(target_arch = "wasm32")))]
#[allow(unused_imports)]
pub(crate) use std::thread;

/// WASM thread stubs - WASM is single-threaded and doesn't support std::thread
#[cfg(all(target_arch = "wasm32", not(all(feature = "shuttle", test))))]
pub(crate) mod thread {
    use std::io;
    use std::num::NonZeroUsize;

    /// Thread handle stub for WASM
    pub struct JoinHandle<T> {
        _marker: std::marker::PhantomData<T>,
    }

    impl<T> JoinHandle<T> {
        /// Join is a no-op panic since we can't actually spawn threads
        pub fn join(self) -> std::thread::Result<T> {
            panic!("Cannot join threads in WASM - threads are not supported")
        }
    }

    /// Spawn panics in WASM since threads are not supported
    pub fn spawn<F, T>(_f: F) -> JoinHandle<T>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        panic!("Cannot spawn threads in WASM - threads are not supported")
    }

    /// Yield is a no-op in WASM
    #[inline]
    pub fn yield_now() {
        // No-op: WASM is single-threaded
    }

    /// Always returns 1 for WASM since it's single-threaded
    pub fn available_parallelism() -> io::Result<NonZeroUsize> {
        Ok(NonZeroUsize::new(1).unwrap())
    }

    /// Thread ID stub for WASM
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
    pub struct ThreadId(u64);

    /// Current thread stub for WASM
    pub struct Thread {
        id: ThreadId,
    }

    impl Thread {
        pub fn id(&self) -> ThreadId {
            self.id
        }
    }

    /// Returns a stub for the current thread
    pub fn current() -> Thread {
        Thread {
            id: ThreadId(0), // Single thread, always ID 0
        }
    }
}
