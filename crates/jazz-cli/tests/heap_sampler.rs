//! The sampling allocator's estimate of live heap bytes, measured through
//! a real global allocator because the sampler only sees memory allocated
//! through it.
#![cfg(heap_profiling)]

use std::alloc::System;

use jazz_cli::heap_profiling::{
    DEFAULT_SAMPLE_INTERVAL, SamplingAllocator, enable_sampling, for_each_live_sample,
};

#[global_allocator]
static ALLOCATOR: SamplingAllocator<System> = SamplingAllocator::new(System);

/// Estimated live bytes and the deepest sampled stack.
fn live_estimate() -> (f64, usize) {
    let mut bytes = 0.0;
    let mut depth = 0;
    for_each_live_sample(|sample| {
        bytes += sample.weight;
        depth = depth.max(sample.stack.len());
    });
    (bytes, depth)
}

#[inline(never)]
fn allocate(count: usize, size: usize) -> Vec<Vec<u8>> {
    (0..count).map(|_| vec![1u8; size]).collect()
}

/// About 256 MiB allocated in mixed sizes, half of it kept: the samples'
/// weights add up to the bytes still in use, and they are forgotten once
/// that memory is freed.
#[test]
fn estimates_live_bytes_and_forgets_freed_memory() {
    enable_sampling(DEFAULT_SAMPLE_INTERVAL);
    let (baseline, _) = live_estimate();
    let mut kept = Vec::new();
    for round in 0..64 {
        let small = allocate(2048, 1024 + round);
        let large = allocate(2, 1 << 20);
        kept.extend(small.into_iter().step_by(2));
        kept.extend(large.into_iter().step_by(2));
    }
    let live: usize = kept.iter().map(Vec::capacity).sum();

    let (estimate, depth) = live_estimate();
    let error = (estimate - baseline - live as f64).abs() / live as f64;
    assert!(
        error < 0.15,
        "estimated {estimate} bytes above a {baseline} baseline for {live} live bytes"
    );
    assert!(depth > 3, "samples carry the allocating stack");

    drop(kept);
    let (after, _) = live_estimate();
    assert!(
        after - baseline < live as f64 * 0.02,
        "freed samples are forgotten: {after} bytes remain above {baseline}"
    );
}
