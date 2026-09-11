//! Disabled `cold-settle-attribution` counters for IVM cardinality.

#![allow(missing_docs)]

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

const BUCKETS: usize = 2;

static MAP_BUFFER_CAPACITY: AtomicU64 = AtomicU64::new(0);
static MAP_BUFFER_USED: AtomicU64 = AtomicU64::new(0);

static MAP_CALLS: [AtomicU64; BUCKETS] = [const { AtomicU64::new(0) }; BUCKETS];
static MAP_INPUT_RECORDS: [AtomicU64; BUCKETS] = [const { AtomicU64::new(0) }; BUCKETS];
static MAP_OUTPUT_RECORDS: [AtomicU64; BUCKETS] = [const { AtomicU64::new(0) }; BUCKETS];
static JOIN_CALLS: [AtomicU64; BUCKETS] = [const { AtomicU64::new(0) }; BUCKETS];
static JOIN_LEFT_RECORDS: [AtomicU64; BUCKETS] = [const { AtomicU64::new(0) }; BUCKETS];
static JOIN_RIGHT_RECORDS: [AtomicU64; BUCKETS] = [const { AtomicU64::new(0) }; BUCKETS];
static JOIN_OUTPUT_RECORDS: [AtomicU64; BUCKETS] = [const { AtomicU64::new(0) }; BUCKETS];

#[derive(Clone, Copy, Debug, Default)]
pub struct Snapshot {
    pub map_buffer_capacity: u64,
    pub map_buffer_used: u64,
    pub map_calls: [u64; BUCKETS],
    pub map_input_records: [u64; BUCKETS],
    pub map_output_records: [u64; BUCKETS],
    pub join_calls: [u64; BUCKETS],
    pub join_left_records: [u64; BUCKETS],
    pub join_right_records: [u64; BUCKETS],
    pub join_output_records: [u64; BUCKETS],
}

fn bucket(hydrate: bool) -> usize {
    usize::from(hydrate)
}

pub fn reset() {
    MAP_NODES.lock().unwrap().clear();
    MAP_BUFFER_CAPACITY.store(0, Ordering::Relaxed);
    MAP_BUFFER_USED.store(0, Ordering::Relaxed);
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
        map_buffer_capacity: MAP_BUFFER_CAPACITY.load(Ordering::Relaxed),
        map_buffer_used: MAP_BUFFER_USED.load(Ordering::Relaxed),
        map_calls: load(&MAP_CALLS),
        map_input_records: load(&MAP_INPUT_RECORDS),
        map_output_records: load(&MAP_OUTPUT_RECORDS),
        join_calls: load(&JOIN_CALLS),
        join_left_records: load(&JOIN_LEFT_RECORDS),
        join_right_records: load(&JOIN_RIGHT_RECORDS),
        join_output_records: load(&JOIN_OUTPUT_RECORDS),
    }
}

pub fn record_map(hydrate: bool, input_records: usize, output_records: usize) {
    let index = bucket(hydrate);
    MAP_CALLS[index].fetch_add(1, Ordering::Relaxed);
    MAP_INPUT_RECORDS[index].fetch_add(input_records as u64, Ordering::Relaxed);
    MAP_OUTPUT_RECORDS[index].fetch_add(output_records as u64, Ordering::Relaxed);
}

pub fn record_join(
    hydrate: bool,
    left_records: usize,
    right_records: usize,
    output_records: usize,
) {
    let index = bucket(hydrate);
    JOIN_CALLS[index].fetch_add(1, Ordering::Relaxed);
    JOIN_LEFT_RECORDS[index].fetch_add(left_records as u64, Ordering::Relaxed);
    JOIN_RIGHT_RECORDS[index].fetch_add(right_records as u64, Ordering::Relaxed);
    JOIN_OUTPUT_RECORDS[index].fetch_add(output_records as u64, Ordering::Relaxed);
}

/// Sum completed newly encoded projection buffers, excluding byte-reuse paths.
/// Capacity is retained allocation capacity, not cumulative realloc traffic.
pub fn record_map_buffer(capacity: usize, used: usize) {
    MAP_BUFFER_CAPACITY.fetch_add(capacity as u64, Ordering::Relaxed);
    MAP_BUFFER_USED.fetch_add(used as u64, Ordering::Relaxed);
}

#[derive(Clone, Debug, Default)]
pub struct MapNodeWork {
    pub node: u64,
    pub hydrate: bool,
    pub calls: u64,
    pub input_records: u64,
    pub output_records: u64,
    /// Projection preparation and execution only, excluding upstream evaluation.
    pub elapsed_ns: u64,
    pub plan: String,
}

static MAP_NODES: Mutex<BTreeMap<(u64, bool), MapNodeWork>> = Mutex::new(BTreeMap::new());

pub fn record_map_node(
    node: u64,
    hydrate: bool,
    input_records: usize,
    output_records: usize,
    elapsed_ns: u64,
    plan: impl FnOnce() -> String,
) {
    let mut nodes = MAP_NODES.lock().unwrap();
    let entry = nodes.entry((node, hydrate)).or_insert_with(|| MapNodeWork {
        node,
        hydrate,
        plan: plan(),
        ..MapNodeWork::default()
    });
    entry.calls += 1;
    entry.input_records += input_records as u64;
    entry.output_records += output_records as u64;
    entry.elapsed_ns += elapsed_ns;
}

pub fn map_node_work() -> Vec<MapNodeWork> {
    MAP_NODES.lock().unwrap().values().cloned().collect()
}
