use std::hint::black_box;

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use jazz_lsm::{LsmOptions, LsmTree, StdFs, WriteDurability};
use tempfile::TempDir;

const VALUE_SIZES: [usize; 3] = [32, 256, 4096];
const KEY_COUNT: usize = 5_000;

fn bench_options() -> LsmOptions {
    LsmOptions {
        max_memtable_bytes: 512 * 1024,
        max_wal_bytes: 8 * 1024 * 1024,
        level0_file_limit: 4,
        level_fanout: 4,
        max_levels: 4,
        write_durability: WriteDurability::Buffered,
        ..Default::default()
    }
}

fn open_db(path: &std::path::Path) -> LsmTree<StdFs> {
    let fs = StdFs::new(path).expect("open std fs");
    LsmTree::open(fs, bench_options(), Vec::new()).expect("open lsm tree")
}

fn key(i: usize) -> Vec<u8> {
    format!("k{i:08}").into_bytes()
}

fn value(size: usize, seed: u8) -> Vec<u8> {
    let mut out = vec![0u8; size];
    for (i, byte) in out.iter_mut().enumerate() {
        *byte = seed.wrapping_add((i % 251) as u8);
    }
    out
}

fn shuffled_indices(n: usize) -> Vec<usize> {
    let mut out: Vec<usize> = (0..n).collect();
    let mut state: u64 = 0x9E3779B97F4A7C15;
    for i in (1..n).rev() {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
        let j = (state as usize) % (i + 1);
        out.swap(i, j);
    }
    out
}

fn populate_db(db: &mut LsmTree<StdFs>, count: usize, value_size: usize) {
    for i in 0..count {
        let k = key(i);
        let v = value(value_size, (i % 251) as u8);
        db.put(&k, &v).expect("put");
    }
    db.flush().expect("flush");
}

fn bench_seq_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("lsm_native_seq_write");

    for value_size in VALUE_SIZES {
        group.throughput(Throughput::Elements(KEY_COUNT as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("value_{value_size}")),
            &value_size,
            |b, &value_size| {
                b.iter_batched(
                    || {
                        let dir = tempfile::tempdir().expect("tempdir");
                        let db = open_db(dir.path());
                        (dir, db)
                    },
                    |(_dir, mut db): (TempDir, LsmTree<StdFs>)| {
                        for i in 0..KEY_COUNT {
                            let k = key(i);
                            let v = value(value_size, (i % 251) as u8);
                            db.put(&k, &v).expect("put");
                        }
                        db.flush().expect("flush");
                    },
                    BatchSize::LargeInput,
                )
            },
        );
    }

    group.finish();
}

fn bench_random_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("lsm_native_random_write");
    let order = shuffled_indices(KEY_COUNT);

    for value_size in VALUE_SIZES {
        group.throughput(Throughput::Elements(KEY_COUNT as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("value_{value_size}")),
            &value_size,
            |b, &value_size| {
                b.iter_batched(
                    || {
                        let dir = tempfile::tempdir().expect("tempdir");
                        let db = open_db(dir.path());
                        (dir, db)
                    },
                    |(_dir, mut db): (TempDir, LsmTree<StdFs>)| {
                        for &i in &order {
                            let k = key(i);
                            let v = value(value_size, (i % 251) as u8);
                            db.put(&k, &v).expect("put");
                        }
                        db.flush().expect("flush");
                    },
                    BatchSize::LargeInput,
                )
            },
        );
    }

    group.finish();
}

fn bench_seq_read(c: &mut Criterion) {
    let mut group = c.benchmark_group("lsm_native_seq_read");

    for value_size in VALUE_SIZES {
        group.throughput(Throughput::Elements(KEY_COUNT as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("value_{value_size}")),
            &value_size,
            |b, &value_size| {
                b.iter_batched(
                    || {
                        let dir = tempfile::tempdir().expect("tempdir");
                        let mut db = open_db(dir.path());
                        populate_db(&mut db, KEY_COUNT, value_size);
                        (dir, db)
                    },
                    |(_dir, db): (TempDir, LsmTree<StdFs>)| {
                        let mut checksum: u64 = 0;
                        for i in 0..KEY_COUNT {
                            let k = key(i);
                            let v = db.get(&k).expect("get").expect("present");
                            checksum = checksum.wrapping_add(v[0] as u64);
                        }
                        black_box(checksum);
                    },
                    BatchSize::LargeInput,
                )
            },
        );
    }

    group.finish();
}

fn bench_random_read(c: &mut Criterion) {
    let mut group = c.benchmark_group("lsm_native_random_read");
    let order = shuffled_indices(KEY_COUNT);

    for value_size in VALUE_SIZES {
        group.throughput(Throughput::Elements(KEY_COUNT as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("value_{value_size}")),
            &value_size,
            |b, &value_size| {
                b.iter_batched(
                    || {
                        let dir = tempfile::tempdir().expect("tempdir");
                        let mut db = open_db(dir.path());
                        populate_db(&mut db, KEY_COUNT, value_size);
                        (dir, db)
                    },
                    |(_dir, db): (TempDir, LsmTree<StdFs>)| {
                        let mut checksum: u64 = 0;
                        for &i in &order {
                            let k = key(i);
                            let v = db.get(&k).expect("get").expect("present");
                            checksum = checksum.wrapping_add(v[0] as u64);
                        }
                        black_box(checksum);
                    },
                    BatchSize::LargeInput,
                )
            },
        );
    }

    group.finish();
}

criterion_group!(
    lsm_native,
    bench_seq_write,
    bench_random_write,
    bench_seq_read,
    bench_random_read
);
criterion_main!(lsm_native);
