//! Single-threaded shared state wrapper.
//!
//! Uses `Rc<RefCell<T>>` for interior mutability. This is a deliberate
//! simplification - the sync layer is single-threaded on all platforms.
//! Multi-threading can be added later if needed.

use std::cell::{Ref, RefCell, RefMut};
use std::rc::Rc;

/// Single-threaded shared state wrapper.
pub struct Shared<T>(Rc<RefCell<T>>);

impl<T> Shared<T> {
    /// Create a new shared state container.
    pub fn new(value: T) -> Self {
        Self(Rc::new(RefCell::new(value)))
    }

    /// Borrow the shared state immutably.
    ///
    /// Panics if the state is currently mutably borrowed.
    #[track_caller]
    pub fn read(&self) -> Ref<'_, T> {
        match self.0.try_borrow() {
            Ok(guard) => guard,
            Err(_) => {
                let loc = std::panic::Location::caller();
                panic!(
                    "Shared<{}>::read() failed at {}:{} - RefCell already mutably borrowed",
                    std::any::type_name::<T>(),
                    loc.file(),
                    loc.line()
                );
            }
        }
    }

    /// Borrow the shared state mutably.
    ///
    /// Panics if the state is currently borrowed.
    #[track_caller]
    pub fn write(&self) -> RefMut<'_, T> {
        match self.0.try_borrow_mut() {
            Ok(guard) => guard,
            Err(_) => {
                let loc = std::panic::Location::caller();
                panic!(
                    "Shared<{}>::write() failed at {}:{} - RefCell already borrowed",
                    std::any::type_name::<T>(),
                    loc.file(),
                    loc.line()
                );
            }
        }
    }

    /// Try to borrow the shared state immutably.
    ///
    /// Returns `None` if the state is currently mutably borrowed.
    pub fn try_read(&self) -> Option<Ref<'_, T>> {
        self.0.try_borrow().ok()
    }

    /// Try to borrow the shared state mutably.
    ///
    /// Returns `None` if the state is currently borrowed.
    pub fn try_write(&self) -> Option<RefMut<'_, T>> {
        self.0.try_borrow_mut().ok()
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
