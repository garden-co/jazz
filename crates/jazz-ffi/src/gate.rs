//! A host callback's gate: while open, calls pass and are counted; closing
//! it waits until no call is running (but the caller's own, when it closes
//! from inside a call), and after that no call passes. A C host may free the
//! callback's `ctx` once close returns.

use std::cell::RefCell;
use std::sync::{Condvar, Mutex};

pub(crate) struct Gate<T> {
    state: Mutex<State<T>>,
    idle: Condvar,
}

struct State<T> {
    value: Option<T>,
    active: usize,
}

/// A host callback's gate, whatever the callback: what closing needs.
pub(crate) trait HostGate: Send + Sync {
    /// No call passes any more; doesn't wait for the running ones.
    fn shut(&self);
    /// [`Self::shut`], then waits until none runs but the caller's own.
    /// Closing again waits again.
    fn close(&self);
}

impl<T: Clone + Send> HostGate for Gate<T> {
    fn shut(&self) {
        Gate::shut(self);
    }

    fn close(&self) {
        Gate::close(self);
    }
}

thread_local! {
    /// The gates this thread is inside a call of (by address; a gate may
    /// appear more than once for nested calls).
    static INSIDE: RefCell<Vec<usize>> = const { RefCell::new(Vec::new()) };
}

impl<T> Gate<T> {
    fn address(&self) -> usize {
        self as *const Self as *const () as usize
    }
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
                let me = self.0.address();
                INSIDE.with(|inside| {
                    let mut inside = inside.borrow_mut();
                    if let Some(at) = inside.iter().rposition(|gate| *gate == me) {
                        inside.remove(at);
                    }
                });
                let mut state = self.0.state.lock().unwrap_or_else(|e| e.into_inner());
                state.active -= 1;
                self.0.idle.notify_all();
            }
        }
        INSIDE.with(|inside| inside.borrow_mut().push(self.address()));
        let _leave = Leave(self);
        Some(call(&value))
    }

    /// Lets no call pass from now on, without waiting for the running ones.
    pub fn shut(&self) {
        self.state.lock().unwrap_or_else(|e| e.into_inner()).value = None;
    }

    /// Closes the gate and waits until no call is running but this
    /// thread's own (when closed from inside a call).
    pub fn close(&self) {
        // Only this thread's calls into this gate: closing another gate from
        // inside a call of this one waits for all of that gate's calls.
        let me = self.address();
        let own = INSIDE.with(|inside| inside.borrow().iter().filter(|gate| **gate == me).count());
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

    /// Closing gate B from inside a call of gate A still waits for B's
    /// calls on other threads (A's call isn't one of B's).
    #[test]
    fn close_from_inside_another_gate_waits_for_all_of_its_calls() {
        let (a, b) = (Arc::new(Gate::new(())), Arc::new(Gate::new(())));
        let running = Arc::new(AtomicUsize::new(0));
        let other = {
            let (b, running) = (b.clone(), running.clone());
            std::thread::spawn(move || {
                b.call(|_| {
                    running.fetch_add(1, Ordering::SeqCst);
                    std::thread::sleep(Duration::from_millis(400));
                    running.fetch_sub(1, Ordering::SeqCst);
                })
            })
        };
        while running.load(Ordering::SeqCst) < 1 {
            std::thread::sleep(Duration::from_millis(5));
        }
        a.call(|_| {
            b.close();
            assert_eq!(
                running.load(Ordering::SeqCst),
                0,
                "B closed while its call ran"
            );
        });
        other.join().unwrap();
    }
}
