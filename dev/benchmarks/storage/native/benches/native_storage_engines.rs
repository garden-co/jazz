use std::future::Future;
use std::path::Path;
use std::pin::pin;
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};
use std::time::{Duration, Instant};

use bench_core::{
    BenchEngine, Benchmark, EngineError, KvDataset, PhaseKind, benchmarks, decode_kv,
    phases::replay,
};
use criterion::{
    BenchmarkGroup, Criterion, Throughput, black_box, criterion_group, criterion_main,
    measurement::WallTime,
};
#[path = "native_storage_engines/redb.rs"]
mod redb;
#[path = "native_storage_engines/rocksdb.rs"]
mod rocksdb;
#[path = "native_storage_engines/sqlite.rs"]
mod sqlite;

use self::redb::RedbEngine;
use self::rocksdb::RocksDbEngine;
use self::sqlite::SqliteEngine;

const CACHE_BYTES: usize = 32 * 1024 * 1024;
const DATA_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../data");

trait NativeEngine: BenchEngine + Sized {
    const NAME: &'static str;

    fn open_fresh() -> Result<Self, EngineError>;
}

struct DatasetView<'a> {
    keys: Vec<&'a [u8]>,
    values: Vec<&'a [u8]>,
    record_count: u32,
}

impl<'a> DatasetView<'a> {
    fn new(dataset: &'a KvDataset) -> Self {
        Self {
            keys: dataset
                .records
                .iter()
                .map(|(key, _)| key.as_slice())
                .collect(),
            values: dataset
                .records
                .iter()
                .map(|(_, value)| value.as_slice())
                .collect(),
            record_count: dataset.records.len() as u32,
        }
    }
}

fn bench_native_storage(c: &mut Criterion) {
    for benchmark in benchmarks() {
        let dataset = load_dataset(&benchmark);
        let view = DatasetView::new(&dataset);

        for (phase_index, phase) in benchmark.phases.iter().enumerate() {
            let phase_name = phase.name();
            let mut group = c.benchmark_group(format!(
                "native_storage/{}/{}",
                benchmark.profile, phase_name
            ));
            group.throughput(Throughput::Elements(
                phase.op_count(view.record_count) as u64
            ));

            bench_engine::<SqliteEngine>(&mut group, &benchmark, &view, phase_index);
            bench_engine::<RocksDbEngine>(&mut group, &benchmark, &view, phase_index);
            bench_engine::<RedbEngine>(&mut group, &benchmark, &view, phase_index);

            group.finish();
        }
    }
}

fn bench_engine<E: NativeEngine>(
    group: &mut BenchmarkGroup<'_, WallTime>,
    benchmark: &Benchmark,
    view: &DatasetView<'_>,
    phase_index: usize,
) {
    group.bench_function(E::NAME, |bencher| {
        bencher.iter_custom(|iters| {
            time_phase::<E>(benchmark, view, phase_index, iters)
                .unwrap_or_else(|error| panic!("{} benchmark failed: {error}", E::NAME))
        });
    });
}

fn time_phase<E: NativeEngine>(
    benchmark: &Benchmark,
    view: &DatasetView<'_>,
    phase_index: usize,
    iters: u64,
) -> Result<Duration, EngineError> {
    let args = benchmark.phase_args(phase_index, view.record_count);
    let repeat_same_state = can_repeat_on_same_state(benchmark.phases[phase_index].kind());
    let mut total = Duration::ZERO;
    let mut checksum = 0u64;

    if repeat_same_state {
        let mut engine = E::open_fresh()?;
        prepare_engine(&mut engine, benchmark, view, phase_index)?;
        for _ in 0..iters {
            let started = Instant::now();
            checksum = checksum.wrapping_add(run_phase_with_args(
                &mut engine,
                benchmark,
                view,
                phase_index,
                &args,
            )?);
            total += started.elapsed();
        }
    } else {
        for _ in 0..iters {
            let mut engine = E::open_fresh()?;
            prepare_engine(&mut engine, benchmark, view, phase_index)?;
            let started = Instant::now();
            checksum = checksum.wrapping_add(run_phase_with_args(
                &mut engine,
                benchmark,
                view,
                phase_index,
                &args,
            )?);
            total += started.elapsed();
        }
    }

    black_box(checksum);
    Ok(total)
}

fn prepare_engine<E: BenchEngine>(
    engine: &mut E,
    benchmark: &Benchmark,
    view: &DatasetView<'_>,
    phase_index: usize,
) -> Result<(), EngineError> {
    for prior_index in 0..phase_index {
        let args = benchmark.phase_args(prior_index, view.record_count);
        run_phase_with_args(engine, benchmark, view, prior_index, &args)?;
    }
    Ok(())
}

fn run_phase_with_args<E: BenchEngine>(
    engine: &mut E,
    benchmark: &Benchmark,
    view: &DatasetView<'_>,
    phase_index: usize,
    args: &[u32],
) -> Result<u64, EngineError> {
    let phase = &benchmark.phases[phase_index];
    let kind = phase.kind();
    if kind.is_cold() {
        block_on(engine.reopen())?;
    }

    engine.begin_phase(kind)?;
    let (_, checksum) = replay(engine, phase, args, &view.keys, &view.values)?;
    engine.end_phase(kind)?;
    Ok(checksum)
}

fn can_repeat_on_same_state(kind: PhaseKind) -> bool {
    !matches!(kind, PhaseKind::Load | PhaseKind::Mixed)
}

fn load_dataset(benchmark: &Benchmark) -> KvDataset {
    let bytes = std::fs::read(Path::new(DATA_DIR).join(&benchmark.kv_fixture))
        .unwrap_or_else(|error| panic!("read fixture {}: {error}", benchmark.kv_fixture));
    let dataset = decode_kv(&bytes)
        .unwrap_or_else(|error| panic!("decode fixture {}: {error}", benchmark.kv_fixture));
    assert_eq!(dataset.profile, benchmark.profile);
    dataset
}

fn eng<E: ToString>(error: E) -> EngineError {
    EngineError::new(error.to_string())
}

fn block_on<F: Future>(future: F) -> F::Output {
    fn raw_waker() -> RawWaker {
        fn no_op(_: *const ()) {}
        fn clone(_: *const ()) -> RawWaker {
            raw_waker()
        }
        RawWaker::new(
            std::ptr::null(),
            &RawWakerVTable::new(clone, no_op, no_op, no_op),
        )
    }

    let waker = unsafe { Waker::from_raw(raw_waker()) };
    let mut cx = Context::from_waker(&waker);
    let mut future = pin!(future);
    loop {
        if let Poll::Ready(value) = future.as_mut().poll(&mut cx) {
            return value;
        }
    }
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .sample_size(10)
        .warm_up_time(Duration::from_millis(500))
        .measurement_time(Duration::from_secs(3));
    targets = bench_native_storage
}
criterion_main!(benches);
