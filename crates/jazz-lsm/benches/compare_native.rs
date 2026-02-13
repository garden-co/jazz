use std::hint::black_box;
use std::path::Path;
use std::str::FromStr;

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use fjall::{Config as FjallConfig, PartitionCreateOptions, PersistMode};
use jazz_lsm::{LsmOptions, LsmTree, StdFs, WriteDurability};
use rocksdb::{Options as RocksOptions, WriteOptions};
use tempfile::TempDir;

const DEFAULT_VALUE_SIZES: [usize; 3] = [32, 256, 4096];
const DEFAULT_KEY_COUNT: usize = 5_000;

fn key_count() -> usize {
    std::env::var("JAZZ_LSM_BENCH_KEY_COUNT")
        .ok()
        .and_then(|v| usize::from_str(&v).ok())
        .filter(|&n| n > 0)
        .unwrap_or(DEFAULT_KEY_COUNT)
}

fn value_sizes() -> Vec<usize> {
    std::env::var("JAZZ_LSM_BENCH_VALUE_SIZES")
        .ok()
        .map(|raw| {
            raw.split(',')
                .filter_map(|x| usize::from_str(x.trim()).ok())
                .filter(|&n| n > 0)
                .collect::<Vec<_>>()
        })
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| DEFAULT_VALUE_SIZES.to_vec())
}

trait Engine {
    fn put(&mut self, key: &[u8], value: &[u8]);
    fn get(&mut self, key: &[u8]) -> Vec<u8>;
    fn finish_writes(&mut self);
}

struct LsmEngine {
    db: LsmTree<StdFs>,
}

impl LsmEngine {
    fn open(path: &Path) -> Self {
        let fs = StdFs::new(path).expect("open std fs");
        let options = LsmOptions {
            max_memtable_bytes: 512 * 1024,
            max_wal_bytes: 8 * 1024 * 1024,
            level0_file_limit: 4,
            level_fanout: 4,
            max_levels: 4,
            write_durability: WriteDurability::Buffered,
            ..Default::default()
        };
        let db = LsmTree::open(fs, options, Vec::new()).expect("open lsm tree");
        Self { db }
    }
}

impl Engine for LsmEngine {
    fn put(&mut self, key: &[u8], value: &[u8]) {
        self.db.put(key, value).expect("lsm put");
    }

    fn get(&mut self, key: &[u8]) -> Vec<u8> {
        self.db.get(key).expect("lsm get").expect("lsm key present")
    }

    fn finish_writes(&mut self) {
        self.db.flush().expect("lsm flush");
    }
}

struct RocksDbEngine {
    db: rocksdb::DB,
    write_options: WriteOptions,
}

impl RocksDbEngine {
    fn open(path: &Path) -> Self {
        let mut options = RocksOptions::default();
        options.create_if_missing(true);
        options.set_use_fsync(false);

        let mut write_options = WriteOptions::default();
        write_options.set_sync(false);
        write_options.disable_wal(true);

        let db_path = path.join("rocksdb");
        let db = rocksdb::DB::open(&options, db_path).expect("open rocksdb");
        Self { db, write_options }
    }
}

impl Engine for RocksDbEngine {
    fn put(&mut self, key: &[u8], value: &[u8]) {
        self.db
            .put_opt(key, value, &self.write_options)
            .expect("rocksdb put");
    }

    fn get(&mut self, key: &[u8]) -> Vec<u8> {
        self.db
            .get_pinned(key)
            .expect("rocksdb get")
            .expect("rocksdb key present")
            .as_ref()
            .to_vec()
    }

    fn finish_writes(&mut self) {
        self.db.flush().expect("rocksdb flush");
    }
}

struct FjallEngine {
    keyspace: fjall::Keyspace,
    partition: fjall::PartitionHandle,
}

impl FjallEngine {
    fn open(path: &Path) -> Self {
        let keyspace = FjallConfig::new(path.join("fjall"))
            .open()
            .expect("open fjall keyspace");
        let partition = keyspace
            .open_partition("bench", PartitionCreateOptions::default())
            .expect("open fjall partition");
        Self {
            keyspace,
            partition,
        }
    }
}

impl Engine for FjallEngine {
    fn put(&mut self, key: &[u8], value: &[u8]) {
        self.partition.insert(key, value).expect("fjall insert");
    }

    fn get(&mut self, key: &[u8]) -> Vec<u8> {
        self.partition
            .get(key)
            .expect("fjall get")
            .expect("fjall key present")
            .to_vec()
    }

    fn finish_writes(&mut self) {
        self.keyspace
            .persist(PersistMode::SyncData)
            .expect("fjall persist");
    }
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

fn engine_factories(
    _max_value_size: usize,
) -> Vec<(&'static str, Box<dyn Fn(&Path) -> Box<dyn Engine>>)> {
    let mut out: Vec<(&'static str, Box<dyn Fn(&Path) -> Box<dyn Engine>>)> = Vec::new();
    out.push(("jazz_lsm", Box::new(|path| Box::new(LsmEngine::open(path)))));
    out.push((
        "rocksdb",
        Box::new(|path| Box::new(RocksDbEngine::open(path))),
    ));
    out.push(("fjall", Box::new(|path| Box::new(FjallEngine::open(path)))));
    out
}

fn bench_seq_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("compare_native_seq_write");
    let key_count = key_count();
    let value_sizes = value_sizes();

    for value_size in value_sizes {
        let factories = engine_factories(value_size);
        group.throughput(Throughput::Elements(key_count as u64));
        for (engine_name, factory) in &factories {
            group.bench_with_input(
                BenchmarkId::new(*engine_name, format!("value_{value_size}")),
                &value_size,
                |b, &value_size| {
                    b.iter_batched(
                        || {
                            let dir = tempfile::tempdir().expect("tempdir");
                            let engine = factory(dir.path());
                            (dir, engine)
                        },
                        |(_dir, mut engine): (TempDir, Box<dyn Engine>)| {
                            for i in 0..key_count {
                                let k = key(i);
                                let v = value(value_size, (i % 251) as u8);
                                engine.put(&k, &v);
                            }
                            engine.finish_writes();
                        },
                        BatchSize::LargeInput,
                    )
                },
            );
        }
    }

    group.finish();
}

fn bench_random_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("compare_native_random_write");
    let key_count = key_count();
    let value_sizes = value_sizes();
    let order = shuffled_indices(key_count);

    for value_size in value_sizes {
        let factories = engine_factories(value_size);
        group.throughput(Throughput::Elements(key_count as u64));
        for (engine_name, factory) in &factories {
            group.bench_with_input(
                BenchmarkId::new(*engine_name, format!("value_{value_size}")),
                &value_size,
                |b, &value_size| {
                    b.iter_batched(
                        || {
                            let dir = tempfile::tempdir().expect("tempdir");
                            let engine = factory(dir.path());
                            (dir, engine)
                        },
                        |(_dir, mut engine): (TempDir, Box<dyn Engine>)| {
                            for &i in &order {
                                let k = key(i);
                                let v = value(value_size, (i % 251) as u8);
                                engine.put(&k, &v);
                            }
                            engine.finish_writes();
                        },
                        BatchSize::LargeInput,
                    )
                },
            );
        }
    }

    group.finish();
}

fn bench_seq_read(c: &mut Criterion) {
    let mut group = c.benchmark_group("compare_native_seq_read");
    let key_count = key_count();
    let value_sizes = value_sizes();

    for value_size in value_sizes {
        let factories = engine_factories(value_size);
        group.throughput(Throughput::Elements(key_count as u64));
        for (engine_name, factory) in &factories {
            group.bench_with_input(
                BenchmarkId::new(*engine_name, format!("value_{value_size}")),
                &value_size,
                |b, &value_size| {
                    b.iter_batched(
                        || {
                            let dir = tempfile::tempdir().expect("tempdir");
                            let mut engine = factory(dir.path());
                            for i in 0..key_count {
                                let k = key(i);
                                let v = value(value_size, (i % 251) as u8);
                                engine.put(&k, &v);
                            }
                            engine.finish_writes();
                            (dir, engine)
                        },
                        |(_dir, mut engine): (TempDir, Box<dyn Engine>)| {
                            let mut checksum: u64 = 0;
                            for i in 0..key_count {
                                let k = key(i);
                                let v = engine.get(&k);
                                checksum = checksum.wrapping_add(v[0] as u64);
                            }
                            black_box(checksum);
                        },
                        BatchSize::LargeInput,
                    )
                },
            );
        }
    }

    group.finish();
}

fn bench_random_read(c: &mut Criterion) {
    let mut group = c.benchmark_group("compare_native_random_read");
    let key_count = key_count();
    let value_sizes = value_sizes();
    let order = shuffled_indices(key_count);

    for value_size in value_sizes {
        let factories = engine_factories(value_size);
        group.throughput(Throughput::Elements(key_count as u64));
        for (engine_name, factory) in &factories {
            group.bench_with_input(
                BenchmarkId::new(*engine_name, format!("value_{value_size}")),
                &value_size,
                |b, &value_size| {
                    b.iter_batched(
                        || {
                            let dir = tempfile::tempdir().expect("tempdir");
                            let mut engine = factory(dir.path());
                            for i in 0..key_count {
                                let k = key(i);
                                let v = value(value_size, (i % 251) as u8);
                                engine.put(&k, &v);
                            }
                            engine.finish_writes();
                            (dir, engine)
                        },
                        |(_dir, mut engine): (TempDir, Box<dyn Engine>)| {
                            let mut checksum: u64 = 0;
                            for &i in &order {
                                let k = key(i);
                                let v = engine.get(&k);
                                checksum = checksum.wrapping_add(v[0] as u64);
                            }
                            black_box(checksum);
                        },
                        BatchSize::LargeInput,
                    )
                },
            );
        }
    }

    group.finish();
}

criterion_group!(
    compare_native,
    bench_seq_write,
    bench_random_write,
    bench_seq_read,
    bench_random_read
);
criterion_main!(compare_native);
