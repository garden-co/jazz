//! Disabled `cold-settle-attribution` counters for IVM cardinality.

#![allow(missing_docs)]

use std::sync::atomic::{AtomicU64, Ordering};

const BUCKETS: usize = 4;

static MAP_CALLS: [AtomicU64; BUCKETS] = [const { AtomicU64::new(0) }; BUCKETS];
static MAP_INPUT_RECORDS: [AtomicU64; BUCKETS] = [const { AtomicU64::new(0) }; BUCKETS];
static MAP_OUTPUT_RECORDS: [AtomicU64; BUCKETS] = [const { AtomicU64::new(0) }; BUCKETS];
static JOIN_CALLS: [AtomicU64; BUCKETS] = [const { AtomicU64::new(0) }; BUCKETS];
static JOIN_LEFT_RECORDS: [AtomicU64; BUCKETS] = [const { AtomicU64::new(0) }; BUCKETS];
static JOIN_RIGHT_RECORDS: [AtomicU64; BUCKETS] = [const { AtomicU64::new(0) }; BUCKETS];
static JOIN_OUTPUT_RECORDS: [AtomicU64; BUCKETS] = [const { AtomicU64::new(0) }; BUCKETS];

#[derive(Clone, Copy, Debug, Default)]
pub struct Snapshot {
    pub map_calls: [u64; BUCKETS],
    pub map_input_records: [u64; BUCKETS],
    pub map_output_records: [u64; BUCKETS],
    pub join_calls: [u64; BUCKETS],
    pub join_left_records: [u64; BUCKETS],
    pub join_right_records: [u64; BUCKETS],
    pub join_output_records: [u64; BUCKETS],
}

fn bucket(hydrate: bool, dominant_child: bool) -> usize {
    (usize::from(hydrate) << 1) | usize::from(dominant_child)
}

pub fn reset() {
    for counters in [
        &MAP_CALLS,
        &MAP_INPUT_RECORDS,
        &MAP_OUTPUT_RECORDS,
        &JOIN_CALLS,
        &JOIN_LEFT_RECORDS,
        &JOIN_RIGHT_RECORDS,
        &JOIN_OUTPUT_RECORDS,
    ] {
        for counter in counters.iter() {
            counter.store(0, Ordering::Relaxed);
        }
    }
}

pub fn snapshot() -> Snapshot {
    fn load(counters: &[AtomicU64; BUCKETS]) -> [u64; BUCKETS] {
        std::array::from_fn(|index| counters[index].load(Ordering::Relaxed))
    }
    Snapshot {
        map_calls: load(&MAP_CALLS),
        map_input_records: load(&MAP_INPUT_RECORDS),
        map_output_records: load(&MAP_OUTPUT_RECORDS),
        join_calls: load(&JOIN_CALLS),
        join_left_records: load(&JOIN_LEFT_RECORDS),
        join_right_records: load(&JOIN_RIGHT_RECORDS),
        join_output_records: load(&JOIN_OUTPUT_RECORDS),
    }
}

pub fn record_map(
    hydrate: bool,
    dominant_child: bool,
    input_records: usize,
    output_records: usize,
) {
    let index = bucket(hydrate, dominant_child);
    MAP_CALLS[index].fetch_add(1, Ordering::Relaxed);
    MAP_INPUT_RECORDS[index].fetch_add(input_records as u64, Ordering::Relaxed);
    MAP_OUTPUT_RECORDS[index].fetch_add(output_records as u64, Ordering::Relaxed);
}

pub fn record_join(
    hydrate: bool,
    dominant_child: bool,
    left_records: usize,
    right_records: usize,
    output_records: usize,
) {
    let index = bucket(hydrate, dominant_child);
    JOIN_CALLS[index].fetch_add(1, Ordering::Relaxed);
    JOIN_LEFT_RECORDS[index].fetch_add(left_records as u64, Ordering::Relaxed);
    JOIN_RIGHT_RECORDS[index].fetch_add(right_records as u64, Ordering::Relaxed);
    JOIN_OUTPUT_RECORDS[index].fetch_add(output_records as u64, Ordering::Relaxed);
}
