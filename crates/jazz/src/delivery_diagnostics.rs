//! Test-only flight recorder. Entries contain lifecycle metadata, never values or claims.

use std::collections::VecDeque;
use std::sync::{
    Mutex,
    atomic::{AtomicUsize, Ordering},
};

static RECORDERS: AtomicUsize = AtomicUsize::new(0);
static SEQUENCE: AtomicUsize = AtomicUsize::new(0);
static EVENTS: Mutex<VecDeque<String>> = Mutex::new(VecDeque::new());

pub struct Recording;

impl Drop for Recording {
    fn drop(&mut self) {
        RECORDERS.fetch_sub(1, Ordering::Relaxed);
    }
}

pub fn start() -> Recording {
    RECORDERS.fetch_add(1, Ordering::Relaxed);
    Recording
}

pub(crate) fn record(event: impl FnOnce() -> String) {
    if RECORDERS.load(Ordering::Relaxed) == 0 {
        return;
    }
    let mut events = EVENTS.lock().unwrap_or_else(|error| error.into_inner());
    if events.len() == 4096 {
        events.pop_front();
    }
    let sequence = SEQUENCE.fetch_add(1, Ordering::Relaxed);
    events.push_back(format!("{sequence}: {}", event()));
}

pub fn snapshot() -> String {
    EVENTS
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .iter()
        .cloned()
        .collect::<Vec<_>>()
        .join("\n")
}

pub(crate) fn opaque_hash(value: &impl std::hash::Hash) -> u64 {
    use std::hash::Hasher;
    let mut hash = std::collections::hash_map::DefaultHasher::new();
    value.hash(&mut hash);
    hash.finish()
}
