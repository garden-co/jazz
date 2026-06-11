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

fn bench_get(c: &mut Criterion) {
    let file = build_tree(true);
    let mut tree = OpfsBTree::open(file, BTreeOptions::default()).expect("open");
    let mut i = 0usize;
    c.bench_function("get_random", |b| {
        b.iter(|| {
            i = (i + 7919) % N;
            black_box(tree.get(&key(i)).expect("get").expect("present"))
        })
    });
    let mut j = 0usize;
    c.bench_function("get_sequential", |b| {
        b.iter(|| {
            j = (j + 1) % N;
            black_box(tree.get(&key(j)).expect("get").expect("present"))
        })
    });
}

fn bench_range(c: &mut Criterion) {
    let file = build_tree(true);
    let mut tree = OpfsBTree::open(file, BTreeOptions::default()).expect("open");
    let mut i = 0usize;
    c.bench_function("range_100", |b| {
        b.iter(|| {
            i = (i + 7919) % (N - 200);
            black_box(tree.range(&key(i), &key(i + 200), 100).expect("range"))
        })
    });
}

fn bench_open_replay(c: &mut Criterion) {
    let file = build_tree(false);
    c.bench_function("open_with_wal_tail", |b| {
        b.iter(|| black_box(OpfsBTree::open(file.clone(), BTreeOptions::default()).expect("open")))
    });
}

criterion_group!(benches, bench_get, bench_range, bench_open_replay);
criterion_main!(benches);
