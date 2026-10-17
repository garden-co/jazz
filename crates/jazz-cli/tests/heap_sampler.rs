//! The sampling allocator's estimate of live heap bytes, measured through
//! a real global allocator because the sampler only sees memory allocated
//! through it.
#![cfg(heap_profiling)]

use std::alloc::System;

use jazz_cli::heap_profiling::{SamplingAllocator, enable_sampling, for_each_live_sample};

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

/// Standard deviation of the estimate of `sizes` live bytes when sampling
/// about once per `mean` bytes: each allocation is sampled with probability
/// `p = 1 - exp(-size / mean)` and then stands for `size / p` bytes, so it
/// adds `size² (1 - p) / p` to the variance.
fn estimate_deviation(sizes: impl Iterator<Item = usize>, mean: u64) -> f64 {
    sizes
        .map(|size| {
            let size = size as f64;
            let p = -(-size / mean as f64).exp_m1();
            size * size * (1.0 - p) / p
        })
        .sum::<f64>()
        .sqrt()
}

#[inline(never)]
fn allocate(count: usize, size: usize) -> Vec<Vec<u8>> {
    (0..count).map(|_| vec![1u8; size]).collect()
}

/// About 264 MiB allocated in mixed sizes, half of it kept: the samples'
/// weights add up to the bytes still in use, and they are forgotten once
/// that memory is freed.
///
/// The estimate is random, so it is checked against its own standard
/// deviation rather than a fixed tolerance, which six of them leave about
/// once in hundreds of millions of runs. Sampling every 64 KiB rather than the server's
/// 512 KiB takes enough samples that six deviations stay under 10% of the
/// live bytes; at 512 KiB a 15% bound was three deviations and failed about
/// once in 370 runs.
#[test]
fn estimates_live_bytes_and_forgets_freed_memory() {
    const MEAN: u64 = 64 * 1024;
    enable_sampling(MEAN);
    let (baseline, _) = live_estimate();
    let mut kept = Vec::new();
    for round in 0..64 {
        let small = allocate(2048, 1024 + round);
        let near_mean = allocate(2, MEAN as usize + round);
        let large = allocate(2, 1 << 20);
        kept.extend(small.into_iter().step_by(2));
        kept.extend(near_mean.into_iter().step_by(2));
        kept.extend(large.into_iter().step_by(2));
    }
    // `kept`'s own buffer is live too, and big enough to be sampled.
    let sizes = || {
        let buffer = kept.capacity() * size_of::<Vec<u8>>();
        kept.iter().map(Vec::capacity).chain([buffer])
    };
    let live: usize = sizes().sum();
    let deviation = estimate_deviation(sizes(), MEAN);
    assert!(
        6.0 * deviation < live as f64 * 0.1,
        "enough samples to estimate within 10%"
    );

    let (estimate, depth) = live_estimate();
    let error = (estimate - baseline - live as f64).abs();
    assert!(
        error < 6.0 * deviation,
        "estimated {estimate} bytes above a {baseline} baseline for {live} live bytes, \
         {:.1} standard deviations of {deviation:.0} bytes off",
        error / deviation
    );
    assert!(depth > 3, "samples carry the allocating stack");

    drop(kept);
    let (after, _) = live_estimate();
    assert!(
        after - baseline < live as f64 * 0.02,
        "freed samples are forgotten: {after} bytes remain above {baseline}"
    );
}
