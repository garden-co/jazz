use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};
use opfs_btree::{BTreeOptions, MemoryFile, OpfsBTree};

const N: usize = 50_000;

fn key(i: usize) -> Vec<u8> {
    format!("row/{:08}", i).into_bytes()
}

/// Builds a tree with N ~100-byte rows, flushing the WAL every 1000 puts.
/// With `checkpoint: false` all data stays in the WAL tail, so `open()`
/// has to replay every commit (the startup path seen in the trace).
fn build_tree(checkpoint: bool) -> MemoryFile {
    let file = MemoryFile::new();
    let mut tree = OpfsBTree::open(file.clone(), BTreeOptions::default()).expect("open");
    let value = vec![0xabu8; 100];
    for i in 0..N {
        tree.put(&key(i), &value).expect("put");
        if i % 1000 == 999 {
            tree.flush_wal().expect("flush_wal");
        }
    }
    tree.flush_wal().expect("flush_wal");
    if checkpoint {
        tree.checkpoint().expect("checkpoint");
    }
    file
}

// Pre-built outside the timed loops so per-iteration `format!` allocations
// don't dilute the measured tree work.
fn all_keys() -> Vec<Vec<u8>> {
    (0..N).map(key).collect()
}

fn bench_get(c: &mut Criterion) {
    let file = build_tree(true);
    let mut tree = OpfsBTree::open(file, BTreeOptions::default()).expect("open");
    let keys = all_keys();
    let mut i = 0usize;
    c.bench_function("get_random", |b| {
        b.iter(|| {
            i = (i + 7919) % N;
            black_box(tree.get(&keys[i]).expect("get").expect("present"))
        })
    });
    let mut j = 0usize;
    c.bench_function("get_sequential", |b| {
        b.iter(|| {
            j = (j + 1) % N;
            black_box(tree.get(&keys[j]).expect("get").expect("present"))
        })
    });
}

fn bench_range(c: &mut Criterion) {
    let file = build_tree(true);
    let mut tree = OpfsBTree::open(file, BTreeOptions::default()).expect("open");
    let keys = all_keys();
    let mut i = 0usize;
    c.bench_function("range_100", |b| {
        b.iter(|| {
            i = (i + 7919) % (N - 200);
            black_box(tree.range(&keys[i], &keys[i + 200], 100).expect("range"))
        })
    });
}

fn small_cache_options() -> BTreeOptions {
    BTreeOptions {
        // 64 pages of 16 KiB: far below the ~400-page tree, so puts constantly
        // contend with eviction and cache reloads, like the traced workload.
        cache_bytes: 1024 * 1024,
        ..BTreeOptions::default()
    }
}

fn bench_put(c: &mut Criterion) {
    let file = build_tree(true);
    let mut tree = OpfsBTree::open(file, BTreeOptions::default()).expect("open");
    let keys = all_keys();
    let value = vec![0xcdu8; 100];
    let mut i = 0usize;
    c.bench_function("put_overwrite", |b| {
        b.iter(|| {
            i = (i + 7919) % N;
            tree.put(&keys[i], &value).expect("put");
        })
    });
}

fn bench_put_churn(c: &mut Criterion) {
    let file = build_tree(true);
    let mut tree = OpfsBTree::open(file, small_cache_options()).expect("open");
    let keys = all_keys();
    let value = vec![0xcdu8; 100];
    let mut i = 0usize;
    let mut ops = 0usize;
    c.bench_function("put_churn_small_cache", |b| {
        b.iter(|| {
            i = (i + 7919) % N;
            tree.put(&keys[i], &value).expect("put");
            ops += 1;
            if ops.is_multiple_of(1000) {
                tree.flush_wal().expect("flush_wal");
            }
            if ops.is_multiple_of(10_000) {
                tree.checkpoint().expect("checkpoint");
            }
        })
    });
}

fn bench_put_get_interleaved(c: &mut Criterion) {
    let file = build_tree(true);
    let mut tree = OpfsBTree::open(file, BTreeOptions::default()).expect("open");
    let keys = all_keys();
    let value = vec![0xcdu8; 100];
    let mut i = 0usize;
    c.bench_function("put_get_interleaved", |b| {
        b.iter(|| {
            i = (i + 1) % N;
            tree.put(&keys[i], &value).expect("put");
            black_box(tree.get(&keys[i]).expect("get").expect("present"));
        })
    });
}

fn bench_get_alternating_regions(c: &mut Criterion) {
    let file = build_tree(true);
    let mut tree = OpfsBTree::open(file, BTreeOptions::default()).expect("open");
    let keys = all_keys();
    let mut i = 0usize;
    c.bench_function("get_alternating_regions", |b| {
        b.iter(|| {
            i = (i + 1) % (N / 2);
            // Alternate between two distant key regions, like two tables
            // sharing one tree: a single-slot leaf hint misses every time.
            black_box(tree.get(&keys[i]).expect("get").expect("present"));
            black_box(tree.get(&keys[i + N / 2]).expect("get").expect("present"));
        })
    });
}

fn bench_put_append(c: &mut Criterion) {
    let file = build_tree(true);
    let mut tree = OpfsBTree::open(file, BTreeOptions::default()).expect("open");
    let value = vec![0xcdu8; 100];
    let mut i = N;
    c.bench_function("put_append", |b| {
        b.iter(|| {
            i += 1;
            // key(i) allocates per iteration, but identically before/after,
            // so relative comparisons hold.
            tree.put(&key(i), &value).expect("put");
            if i.is_multiple_of(1000) {
                tree.flush_wal().expect("flush_wal");
            }
            if i.is_multiple_of(10_000) {
                tree.checkpoint().expect("checkpoint");
            }
        })
    });
}

fn bench_range_paginate(c: &mut Criterion) {
    let file = build_tree(true);
    let mut tree = OpfsBTree::open(file, BTreeOptions::default()).expect("open");
    let keys = all_keys();
    let mut i = 0usize;
    c.bench_function("range_paginate", |b| {
        b.iter(|| {
            // Sequential pages: each scan starts where the previous ended,
            // which is the leaf the previous scan finished in, not the leaf
            // it started in.
            i = (i + 100) % (N - 200);
            black_box(tree.range(&keys[i], &keys[i + 200], 100).expect("range"))
        })
    });
}

fn bench_get_three_regions(c: &mut Criterion) {
    let file = build_tree(true);
    let mut tree = OpfsBTree::open(file, BTreeOptions::default()).expect("open");
    let keys = all_keys();
    let third = N / 3;
    let mut i = 0usize;
    c.bench_function("get_three_regions", |b| {
        b.iter(|| {
            // Three interleaved regions overflow a 2-slot hint array on
            // every rotation.
            i = (i + 1) % third;
            black_box(tree.get(&keys[i]).expect("get").expect("present"));
            black_box(tree.get(&keys[i + third]).expect("get").expect("present"));
            black_box(
                tree.get(&keys[i + 2 * third])
                    .expect("get")
                    .expect("present"),
            );
        })
    });
}

fn bench_open_replay(c: &mut Criterion) {
    let file = build_tree(false);
    c.bench_function("open_with_wal_tail", |b| {
        b.iter(|| black_box(OpfsBTree::open(file.clone(), BTreeOptions::default()).expect("open")))
    });
}

criterion_group!(
    benches,
    bench_get,
    bench_range,
    bench_put,
    bench_put_churn,
    bench_put_get_interleaved,
    bench_get_alternating_regions,
    bench_put_append,
    bench_range_paginate,
    bench_get_three_regions,
    bench_open_replay
);
criterion_main!(benches);
