//! Platform-agnostic shared state wrapper.
//!
//! Provides a unified API for shared mutable state that works on both
//! native (multi-threaded) and WASM (single-threaded) platforms.
//!
//! - Native: Uses `Arc<RwLock<T>>` for thread-safe sharing
//! - WASM: Uses `Rc<RefCell<T>>` for single-threaded sharing
//!
//! Both implementations provide identical `read()` and `write()` methods,
//! with guards that implement `Deref`/`DerefMut` for transparent access.

#[cfg(not(target_arch = "wasm32"))]
mod inner {
    use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

    /// Thread-safe shared state wrapper (native implementation).
    pub struct Shared<T>(Arc<RwLock<T>>);

    impl<T> Shared<T> {
        /// Create a new shared state container.
        pub fn new(value: T) -> Self {
            Self(Arc::new(RwLock::new(value)))
        }

        /// Acquire a read lock on the shared state.
        ///
        /// Panics if the lock is poisoned.
        pub fn read(&self) -> RwLockReadGuard<'_, T> {
            self.0.read().unwrap()
        }

        /// Acquire a write lock on the shared state.
        ///
        /// Panics if the lock is poisoned.
        pub fn write(&self) -> RwLockWriteGuard<'_, T> {
            self.0.write().unwrap()
        }
    }

    impl<T> Clone for Shared<T> {
        fn clone(&self) -> Self {
            Self(Arc::clone(&self.0))
        }
    }

    impl<T: Default> Default for Shared<T> {
        fn default() -> Self {
            Self::new(T::default())
        }
    }

    impl<T: std::fmt::Debug> std::fmt::Debug for Shared<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self.0.try_read() {
                Ok(guard) => f.debug_tuple("Shared").field(&*guard).finish(),
                Err(_) => f.debug_tuple("Shared").field(&"<locked>").finish(),
            }
        }
    }

    // Safety: Arc<RwLock<T>> is Send + Sync when T is Send + Sync
    unsafe impl<T: Send + Sync> Send for Shared<T> {}
    unsafe impl<T: Send + Sync> Sync for Shared<T> {}
}

#[cfg(target_arch = "wasm32")]
mod inner {
    use std::cell::{Ref, RefCell, RefMut};
    use std::rc::Rc;

    /// Single-threaded shared state wrapper (WASM implementation).
    pub struct Shared<T>(Rc<RefCell<T>>);

    impl<T> Shared<T> {
        /// Create a new shared state container.
        pub fn new(value: T) -> Self {
            Self(Rc::new(RefCell::new(value)))
        }

        /// Borrow the shared state immutably.
        ///
        /// Panics if the state is currently mutably borrowed.
        pub fn read(&self) -> Ref<'_, T> {
            self.0.borrow()
        }

        /// Borrow the shared state mutably.
        ///
        /// Panics if the state is currently borrowed.
        pub fn write(&self) -> RefMut<'_, T> {
            self.0.borrow_mut()
        }
    }

    impl<T> Clone for Shared<T> {
        fn clone(&self) -> Self {
            Self(Rc::clone(&self.0))
        }
    }

    impl<T: Default> Default for Shared<T> {
        fn default() -> Self {
            Self::new(T::default())
        }
    }

    impl<T: std::fmt::Debug> std::fmt::Debug for Shared<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self.0.try_borrow() {
                Ok(guard) => f.debug_tuple("Shared").field(&*guard).finish(),
                Err(_) => f.debug_tuple("Shared").field(&"<borrowed>").finish(),
            }
        }
    }
}

pub use inner::Shared;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shared_read_write() {
        let shared = Shared::new(42);
        assert_eq!(*shared.read(), 42);

        *shared.write() = 100;
        assert_eq!(*shared.read(), 100);
    }

    #[test]
    fn test_shared_clone() {
        let shared1 = Shared::new(vec![1, 2, 3]);
        let shared2 = shared1.clone();

        shared1.write().push(4);
        assert_eq!(*shared2.read(), vec![1, 2, 3, 4]);
    }

    #[test]
    fn test_shared_default() {
        let shared: Shared<Vec<i32>> = Shared::default();
        assert!(shared.read().is_empty());
    }
}
