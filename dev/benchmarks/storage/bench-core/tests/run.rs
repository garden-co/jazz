//! Black-box integration test of the shared benchmark runner through its public
//! API. A trivial in-memory engine stands in for the real OPFS-backed stores so
//! the whole run loop (every phase, timing, checksum, retry, cold reopen) is
//! exercised on native without a browser. The real cross-engine checksum parity
//! between opfs-btree and SQLite is verified by the in-browser harness.

use std::cell::Cell;
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::pin;
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

use bench_core::{
    BenchEngine, Benchmark, EngineError, KvDataset, Phase, PhaseKind, ValueEncoding, run,
};

/// An in-memory `BenchEngine`. `reopen` keeps the data (cold cache is a no-op
/// when there is no cache), matching the persistence contract.
#[derive(Default)]
struct MemEngine {
    map: BTreeMap<Vec<u8>, Vec<u8>>,
}

impl BenchEngine for MemEngine {
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), EngineError> {
        self.map.insert(key.to_vec(), value.to_vec());
        Ok(())
    }
    fn get(&mut self, key: &[u8]) -> Result<Option<u8>, EngineError> {
        Ok(self.map.get(key).map(|v| v.first().copied().unwrap_or(0)))
    }
    fn delete(&mut self, key: &[u8]) -> Result<(), EngineError> {
        self.map.remove(key);
        Ok(())
    }
    fn range(&mut self, lo: &[u8], hi: &[u8], limit: usize) -> Result<usize, EngineError> {
        Ok(self.map.range(lo.to_vec()..hi.to_vec()).take(limit).count())
    }
    fn begin_phase(&mut self, _kind: PhaseKind) -> Result<(), EngineError> {
        Ok(())
    }
    fn end_phase(&mut self, _kind: PhaseKind) -> Result<(), EngineError> {
        Ok(())
    }
    async fn reopen(&mut self) -> Result<(), EngineError> {
        Ok(())
    }
}

fn sample_dataset(count: usize) -> KvDataset {
    let records = (0..count)
        .map(|i| {
            let key = format!("key-{i:06}").into_bytes();
            let val = format!("value-{i}-{}", "x".repeat(i % 16)).into_bytes();
            (key, val)
        })
        .collect();
    KvDataset {
        profile: "test".to_string(),
        source: "synthetic".to_string(),
        encoding: ValueEncoding::Json,
        records,
    }
}

fn small_benchmark() -> Benchmark {
    Benchmark::new("test", "test.kv", 0x1234_5678_9ABC_DEF0)
        .phase(Phase::Load)
        .phase(Phase::GetSeq)
        .phase(Phase::GetRandom { count: 500 })
        .phase(Phase::GetSkewed { count: 500 })
        .phase(Phase::RangeRandom { count: 100 })
        .phase(Phase::UpdateRandom { count: 500 })
        .phase(Phase::Mixed {
            count: 500,
            get: 70,
            put: 20,
            del: 10,
        })
        .phase(Phase::ColdGetRandom { count: 200 })
}

/// A monotonic fake clock so timing never blocks the retry logic.
fn fake_clock() -> impl Fn() -> f64 {
    let t = Cell::new(0.0);
    move || {
        let now = t.get();
        t.set(now + 1000.0);
        now
    }
}

fn block_on<F: Future>(future: F) -> F::Output {
    // The engine is fully synchronous, so the future is always immediately
    // ready; a no-op waker and a single poll suffice.
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

#[test]
fn runs_every_phase_and_loads_all_records() {
    let dataset = sample_dataset(2_000);
    let benchmark = small_benchmark();
    let mut engine = MemEngine::default();
    let clock = fake_clock();

    let result =
        block_on(run(&mut engine, "mem", &benchmark, &dataset, &clock)).expect("run benchmark");

    assert_eq!(result.engine, "mem");
    assert_eq!(result.profile, "test");
    assert_eq!(result.record_count, 2_000);
    // One result row per declared phase, each having executed operations.
    assert_eq!(result.phases.len(), benchmark.phases.len());
    for phase in &result.phases {
        assert!(phase.op_count > 0, "phase {} ran no ops", phase.phase);
    }
    // get_seq touches every record, proving Load populated the store.
    let get_seq = result
        .phases
        .iter()
        .find(|p| p.phase == "get_seq")
        .expect("get_seq phase");
    assert_eq!(get_seq.op_count, 2_000);
    // The mixed phase deletes ~10% of its keys, so the store ends below full.
    assert!(engine.map.len() <= 2_000);
    assert!(engine.map.len() >= 2_000 - 500);
}

#[test]
fn is_deterministic_across_runs() {
    let dataset = sample_dataset(1_500);
    let benchmark = small_benchmark();

    let run_once = || {
        let mut engine = MemEngine::default();
        let clock = fake_clock();
        block_on(run(&mut engine, "mem", &benchmark, &dataset, &clock)).expect("run benchmark")
    };

    let a = run_once();
    let b = run_once();
    assert_eq!(a.checksum, b.checksum);
    let a_phases: Vec<(String, u64)> = a
        .phases
        .iter()
        .map(|p| (p.phase.clone(), p.checksum))
        .collect();
    let b_phases: Vec<(String, u64)> = b
        .phases
        .iter()
        .map(|p| (p.phase.clone(), p.checksum))
        .collect();
    assert_eq!(a_phases, b_phases);
}
