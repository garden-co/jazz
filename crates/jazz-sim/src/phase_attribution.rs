//! Opt-in attribution of a cold settle's wall time.
//!
//! With the `cold-settle-attribution` feature, [`Sim`] times every node's
//! tick and the transport codec's encode and decode. [`attribute`] runs a
//! phase and partitions its wall time into those buckets plus the rest
//! (the scenario's own work between ticks). Thread-local and single-driver:
//! this is a benchmark aid, not a production profiler.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use serde_json::{Value, json};

use crate::net::Sim;

thread_local! {
    static BUCKETS: RefCell<Option<BTreeMap<String, Duration>>> = const { RefCell::new(None) };
}

/// Add `elapsed` to `bucket` while an attribution is running.
pub(crate) fn record(bucket: &str, elapsed: Duration) {
    BUCKETS.with(|buckets| {
        if let Some(buckets) = buckets.borrow_mut().as_mut() {
            *buckets.entry(bucket.to_owned()).or_default() += elapsed;
        }
    });
}

/// Codec time recorded so far: a tick's own time excludes the codec work
/// nested in it, so the buckets partition the wall time.
pub(crate) fn codec_so_far() -> Duration {
    BUCKETS.with(|buckets| {
        buckets.borrow().as_ref().map_or(Duration::ZERO, |buckets| {
            buckets
                .iter()
                .filter(|(bucket, _)| bucket.starts_with("codec_"))
                .map(|(_, elapsed)| *elapsed)
                .sum()
        })
    })
}

/// Time `work` and attribute its wall time to node ticks and the codec.
pub fn attribute<T>(sim: &Sim, work: impl FnOnce(&Sim) -> T) -> (T, Value) {
    BUCKETS.with(|buckets| *buckets.borrow_mut() = Some(BTreeMap::new()));
    let start = Instant::now();
    let rounds = sim.rounds();
    let output = work(sim);
    let total = start.elapsed();
    let buckets = BUCKETS
        .with(|buckets| buckets.borrow_mut().take())
        .unwrap_or_default();
    let attributed: Duration = buckets.values().sum();
    let mut phases = serde_json::Map::new();
    for (bucket, elapsed) in &buckets {
        phases.insert(format!("{bucket}_us"), json!(elapsed.as_micros() as u64));
    }
    phases.insert(
        "outside_ticks_us".to_owned(),
        json!(total.saturating_sub(attributed).as_micros() as u64),
    );
    let report = json!({
        "total_us": total.as_micros() as u64,
        "rounds": sim.rounds() - rounds,
        "phases": phases,
    });
    (output, report)
}
