//! A host callback's gate: while open, calls pass and are counted; closing
//! it waits until no call is running (but the caller's own, when it closes
//! from inside a call), and after that no call passes. A C host may free the
//! callback's `ctx` once close returns.

use std::cell::Cell;
use std::sync::{Condvar, Mutex};

pub(crate) struct Gate<T> {
    state: Mutex<State<T>>,
    idle: Condvar,
}

struct State<T> {
    value: Option<T>,
    active: usize,
}

thread_local! {
    /// How many gated calls this thread is inside.
    static INSIDE: Cell<usize> = const { Cell::new(0) };
}

impl<T: Clone> Gate<T> {
    pub fn new(value: T) -> Self {
        Self {
            state: Mutex::new(State {
                value: Some(value),
                active: 0,
            }),
            idle: Condvar::new(),
        }
    }

    /// Runs `call` with the value, unless the gate is closed (then `None`).
    pub fn call<R>(&self, call: impl FnOnce(&T) -> R) -> Option<R> {
        let value = {
            let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            let value = state.value.clone()?;
            state.active += 1;
            value
        };
        struct Leave<'a, T>(&'a Gate<T>);
        impl<T> Drop for Leave<'_, T> {
            fn drop(&mut self) {
                INSIDE.with(|inside| inside.set(inside.get() - 1));
                let mut state = self.0.state.lock().unwrap_or_else(|e| e.into_inner());
                state.active -= 1;
                self.0.idle.notify_all();
            }
        }
        INSIDE.with(|inside| inside.set(inside.get() + 1));
        let _leave = Leave(self);
        Some(call(&value))
    }

    /// Closes the gate and waits until no call is running but this
    /// thread's own (when closed from inside a call).
    pub fn close(&self) {
        let own = INSIDE.with(Cell::get);
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state.value = None;
        while state.active > own {
            state = self.idle.wait(state).unwrap_or_else(|e| e.into_inner());
        }
    }
}

/// White-box: the gate's waiting is what C hosts rely on, and its "from
/// inside a call" case can't be reached through a host without a callback
/// that frees its own handle.
#[cfg(test)]
mod tests {
    use super::Gate;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    /// Closed from inside one call, the gate waits for the other calls
    /// still running (not for its own), and admits none after.
    #[test]
    fn close_from_inside_waits_for_the_others() {
        let gate = Arc::new(Gate::new(()));
        let running = Arc::new(AtomicUsize::new(0));
        let others: Vec<_> = (0..4)
            .map(|_| {
                let (gate, running) = (gate.clone(), running.clone());
                std::thread::spawn(move || {
                    gate.call(|_| {
                        running.fetch_add(1, Ordering::SeqCst);
                        std::thread::sleep(Duration::from_millis(300));
                        running.fetch_sub(1, Ordering::SeqCst);
                    })
                })
            })
            .collect();
        while running.load(Ordering::SeqCst) < 4 {
            std::thread::sleep(Duration::from_millis(5));
        }
        gate.call(|_| {
            gate.close();
            assert_eq!(
                running.load(Ordering::SeqCst),
                0,
                "close returned while others ran"
            );
        });
        assert!(gate.call(|_| ()).is_none());
        for other in others {
            other.join().unwrap();
        }
    }
}
