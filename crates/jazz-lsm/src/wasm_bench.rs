#![cfg(target_arch = "wasm32")]

use std::cell::RefCell;
use std::rc::Rc;

use serde::Serialize;
use wasm_bindgen::{JsCast, prelude::*};

use crate::{FsError, LsmOptions, LsmTree, OpfsFs, RuntimeStats, SyncFs, WriteDurability};

#[derive(Debug, Clone, Serialize)]
struct PhaseTiming {
    phase: String,
    elapsed_ms: f64,
}

#[derive(Debug, Clone, Default, Serialize)]
struct FsOpStats {
    calls: u64,
    bytes: u64,
    elapsed_ms: f64,
}

impl FsOpStats {
    fn record(&mut self, bytes: u64, elapsed_ms: f64) {
        self.calls = self.calls.saturating_add(1);
        self.bytes = self.bytes.saturating_add(bytes);
        self.elapsed_ms += elapsed_ms;
    }
}

#[derive(Debug, Clone, Default, Serialize)]
struct FsStats {
    read_all: FsOpStats,
    read_range: FsOpStats,
    write_all: FsOpStats,
    write_atomic: FsOpStats,
    append: FsOpStats,
    file_len: FsOpStats,
    truncate: FsOpStats,
    remove_file: FsOpStats,
    list_files: FsOpStats,
    sync_file: FsOpStats,
    sync_dir: FsOpStats,
}

#[derive(Debug, Clone, Serialize)]
struct BenchmarkResult {
    operation: String,
    value_size: u32,
    count: u32,
    seed: u64,
    wall_elapsed_ms: f64,
    elapsed_ms: f64,
    ops_per_sec: f64,
    p95_op_ms: f64,
    reads: u32,
    read_hits: u32,
    read_misses: u32,
    writes: u32,
    deletes: u32,
    checksum: u64,
    phase_times_ms: Vec<PhaseTiming>,
    fs_stats: FsStats,
    runtime_stats: RuntimeStats,
}

#[derive(Debug, Clone, Copy)]
struct MixedScenario {
    name: &'static str,
    read_pct: u8,
    write_pct: u8,
    update_pct: u8,
}

const MIXED_SCENARIOS: [MixedScenario; 3] = [
    MixedScenario {
        name: "mixed_random_70r_30w",
        read_pct: 70,
        write_pct: 30,
        update_pct: 80,
    },
    MixedScenario {
        name: "mixed_random_50r_50w_with_updates",
        read_pct: 50,
        write_pct: 50,
        update_pct: 90,
    },
    MixedScenario {
        name: "mixed_random_60r_20w_20d",
        read_pct: 60,
        write_pct: 20,
        update_pct: 80,
    },
];
const DEFAULT_BASE_SEED: u64 = 0xA5A5_A5A5_0123_4567;

#[derive(Debug, Clone, Copy)]
enum OpChoice {
    Read,
    Write,
    Delete,
}

#[derive(Debug, Clone)]
struct DeterministicRng {
    state: u64,
}

impl DeterministicRng {
    fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_mul(6364136223846793005).wrapping_add(1);
        self.state
    }

    fn next_u8(&mut self) -> u8 {
        (self.next_u64() >> 56) as u8
    }

    fn next_usize(&mut self, upper: usize) -> usize {
        if upper == 0 {
            return 0;
        }
        (self.next_u64() as usize) % upper
    }
}

fn benchmark_options() -> LsmOptions {
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

fn shuffled_indices(n: usize, seed: u64) -> Vec<usize> {
    let mut out: Vec<usize> = (0..n).collect();
    let mut state: u64 = 0xD1B54A32D192ED03 ^ seed;
    for i in (1..n).rev() {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
        let j = (state as usize) % (i + 1);
        out.swap(i, j);
    }
    out
}

fn derive_seed(base_seed: u64, label: &str, value_size: u32) -> u64 {
    const MAX_JS_SAFE_INT: u64 = 9_007_199_254_740_991;
    let mut h = 0xcbf2_9ce4_8422_2325u64 ^ base_seed ^ (value_size as u64);
    for &b in label.as_bytes() {
        h ^= b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    let mut derived = (h ^ ((value_size as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15)))
        % MAX_JS_SAFE_INT;
    if derived == 0 {
        derived = 1;
    }
    derived
}

fn unique_namespace(label: &str) -> String {
    let ts = js_sys::Date::now() as u64;
    let rand = (js_sys::Math::random() * 1_000_000.0) as u64;
    format!("bench-{label}-{ts}-{rand}")
}

fn high_res_now_ms() -> f64 {
    let global = js_sys::global();
    let perf_key = JsValue::from_str("performance");
    if let Ok(perf) = js_sys::Reflect::get(&global, &perf_key)
        && !perf.is_undefined()
        && !perf.is_null()
    {
        let now_key = JsValue::from_str("now");
        if let Ok(now_fn) = js_sys::Reflect::get(&perf, &now_key)
            && let Some(now_fn) = now_fn.dyn_ref::<js_sys::Function>()
            && let Ok(v) = now_fn.call0(&perf)
            && let Some(ms) = v.as_f64()
        {
            return ms;
        }
    }

    js_sys::Date::now()
}

fn push_phase(phase_times: &mut Vec<PhaseTiming>, phase: &str, start_ms: f64) {
    phase_times.push(PhaseTiming {
        phase: phase.to_string(),
        elapsed_ms: high_res_now_ms() - start_ms,
    });
}

#[derive(Debug, Clone)]
struct TrackingFs<F: SyncFs> {
    inner: F,
    stats: Rc<RefCell<FsStats>>,
}

impl<F: SyncFs> TrackingFs<F> {
    fn new(inner: F) -> Self {
        Self {
            inner,
            stats: Rc::new(RefCell::new(FsStats::default())),
        }
    }

    fn snapshot(&self) -> FsStats {
        self.stats.borrow().clone()
    }
}

impl<F: SyncFs> SyncFs for TrackingFs<F> {
    fn read_all(&self, path: &str) -> Result<Vec<u8>, FsError> {
        let start = high_res_now_ms();
        let out = self.inner.read_all(path);
        let elapsed_ms = high_res_now_ms() - start;
        let bytes = out.as_ref().map(|buf| buf.len() as u64).unwrap_or(0);
        self.stats.borrow_mut().read_all.record(bytes, elapsed_ms);
        out
    }

    fn read_range(&self, path: &str, offset: u64, len: usize) -> Result<Vec<u8>, FsError> {
        let start = high_res_now_ms();
        let out = self.inner.read_range(path, offset, len);
        let elapsed_ms = high_res_now_ms() - start;
        let bytes = out.as_ref().map(|buf| buf.len() as u64).unwrap_or(0);
        self.stats.borrow_mut().read_range.record(bytes, elapsed_ms);
        out
    }

    fn write_all(&self, path: &str, data: &[u8]) -> Result<(), FsError> {
        let start = high_res_now_ms();
        let out = self.inner.write_all(path, data);
        let elapsed_ms = high_res_now_ms() - start;
        self.stats
            .borrow_mut()
            .write_all
            .record(data.len() as u64, elapsed_ms);
        out
    }

    fn write_atomic(&self, path: &str, data: &[u8]) -> Result<(), FsError> {
        let start = high_res_now_ms();
        let out = self.inner.write_atomic(path, data);
        let elapsed_ms = high_res_now_ms() - start;
        self.stats
            .borrow_mut()
            .write_atomic
            .record(data.len() as u64, elapsed_ms);
        out
    }

    fn append(&self, path: &str, data: &[u8]) -> Result<(), FsError> {
        let start = high_res_now_ms();
        let out = self.inner.append(path, data);
        let elapsed_ms = high_res_now_ms() - start;
        self.stats
            .borrow_mut()
            .append
            .record(data.len() as u64, elapsed_ms);
        out
    }

    fn file_len(&self, path: &str) -> Result<u64, FsError> {
        let start = high_res_now_ms();
        let out = self.inner.file_len(path);
        let elapsed_ms = high_res_now_ms() - start;
        let bytes = out.as_ref().copied().unwrap_or(0);
        self.stats.borrow_mut().file_len.record(bytes, elapsed_ms);
        out
    }

    fn truncate(&self, path: &str, len: u64) -> Result<(), FsError> {
        let start = high_res_now_ms();
        let out = self.inner.truncate(path, len);
        let elapsed_ms = high_res_now_ms() - start;
        self.stats.borrow_mut().truncate.record(len, elapsed_ms);
        out
    }

    fn remove_file(&self, path: &str) -> Result<(), FsError> {
        let start = high_res_now_ms();
        let out = self.inner.remove_file(path);
        let elapsed_ms = high_res_now_ms() - start;
        self.stats.borrow_mut().remove_file.record(0, elapsed_ms);
        out
    }

    fn list_files(&self, prefix: &str) -> Result<Vec<String>, FsError> {
        let start = high_res_now_ms();
        let out = self.inner.list_files(prefix);
        let elapsed_ms = high_res_now_ms() - start;
        let bytes = out
            .as_ref()
            .map(|files| files.iter().map(|f| f.len() as u64).sum::<u64>())
            .unwrap_or(0);
        self.stats.borrow_mut().list_files.record(bytes, elapsed_ms);
        out
    }

    fn sync_file(&self, path: &str) -> Result<(), FsError> {
        let start = high_res_now_ms();
        let out = self.inner.sync_file(path);
        let elapsed_ms = high_res_now_ms() - start;
        self.stats.borrow_mut().sync_file.record(0, elapsed_ms);
        out
    }

    fn sync_dir(&self) -> Result<(), FsError> {
        let start = high_res_now_ms();
        let out = self.inner.sync_dir();
        let elapsed_ms = high_res_now_ms() - start;
        self.stats.borrow_mut().sync_dir.record(0, elapsed_ms);
        out
    }
}

fn percentile_ms(latencies_ms: &mut [f64], percentile: f64) -> f64 {
    if latencies_ms.is_empty() {
        return 0.0;
    }

    latencies_ms.sort_by(|a, b| a.total_cmp(b));
    let pos = ((latencies_ms.len() as f64 * percentile).ceil() as usize)
        .saturating_sub(1)
        .min(latencies_ms.len() - 1);
    latencies_ms[pos]
}

fn choose_operation(scenario: MixedScenario, roll: u8) -> OpChoice {
    if roll < scenario.read_pct {
        return OpChoice::Read;
    }
    if roll < scenario.read_pct.saturating_add(scenario.write_pct) {
        return OpChoice::Write;
    }
    OpChoice::Delete
}

fn find_mixed_scenario(name: &str) -> Option<MixedScenario> {
    MIXED_SCENARIOS.iter().copied().find(|s| s.name == name)
}

async fn open_db(namespace: &str) -> Result<(LsmTree<TrackingFs<OpfsFs>>, TrackingFs<OpfsFs>), JsValue> {
    let fs = OpfsFs::open(namespace)
        .await
        .map_err(|e| JsValue::from_str(&e.to_string()))?;
    let tracked = TrackingFs::new(fs);
    let db = LsmTree::open(tracked.clone(), benchmark_options(), Vec::new())
        .map_err(|e| JsValue::from_str(&e.to_string()))?;
    Ok((db, tracked))
}

fn to_js_value(result: &BenchmarkResult) -> Result<JsValue, JsValue> {
    serde_wasm_bindgen::to_value(result)
        .map_err(|e| JsValue::from_str(&format!("serialize benchmark result: {e}")))
}

async fn run_seq_write(count: u32, value_size: u32) -> Result<BenchmarkResult, JsValue> {
    let mut phase_times_ms = Vec::new();
    let wall_start = high_res_now_ms();
    let namespace = unique_namespace("seq-write");
    let phase_start = high_res_now_ms();
    OpfsFs::destroy(&namespace)
        .await
        .map_err(|e| JsValue::from_str(&e.to_string()))?;
    push_phase(&mut phase_times_ms, "cleanup_destroy", phase_start);

    let phase_start = high_res_now_ms();
    let (mut db, tracked_fs) = open_db(&namespace).await?;
    push_phase(&mut phase_times_ms, "open_db", phase_start);
    let size = value_size as usize;
    let seed = derive_seed(DEFAULT_BASE_SEED, "seq_write", value_size);

    let op_start = high_res_now_ms();
    let mut checksum = 0u64;
    for i in 0..(count as usize) {
        let k = key(i);
        let v = value(size, (i % 251) as u8);
        checksum = checksum.wrapping_add(v[0] as u64);
        db.put(&k, &v)
            .map_err(|e| JsValue::from_str(&e.to_string()))?;
    }
    push_phase(&mut phase_times_ms, "op_put_loop", op_start);

    let flush_start = high_res_now_ms();
    db.flush().map_err(|e| JsValue::from_str(&e.to_string()))?;
    push_phase(&mut phase_times_ms, "final_flush", flush_start);
    let elapsed_ms = high_res_now_ms() - op_start;

    let runtime_stats = db.runtime_stats();
    drop(db);
    let phase_start = high_res_now_ms();
    OpfsFs::destroy(&namespace)
        .await
        .map_err(|e| JsValue::from_str(&e.to_string()))?;
    push_phase(&mut phase_times_ms, "teardown_destroy", phase_start);
    let fs_stats = tracked_fs.snapshot();
    let wall_elapsed_ms = high_res_now_ms() - wall_start;

    Ok(BenchmarkResult {
        operation: "seq_write".to_string(),
        value_size,
        count,
        seed,
        wall_elapsed_ms,
        elapsed_ms,
        ops_per_sec: (count as f64) / (elapsed_ms / 1000.0),
        p95_op_ms: 0.0,
        reads: 0,
        read_hits: 0,
        read_misses: 0,
        writes: count,
        deletes: 0,
        checksum,
        phase_times_ms,
        fs_stats,
        runtime_stats,
    })
}

async fn run_random_write(count: u32, value_size: u32) -> Result<BenchmarkResult, JsValue> {
    let mut phase_times_ms = Vec::new();
    let wall_start = high_res_now_ms();
    let namespace = unique_namespace("rand-write");
    let phase_start = high_res_now_ms();
    OpfsFs::destroy(&namespace)
        .await
        .map_err(|e| JsValue::from_str(&e.to_string()))?;
    push_phase(&mut phase_times_ms, "cleanup_destroy", phase_start);

    let phase_start = high_res_now_ms();
    let (mut db, tracked_fs) = open_db(&namespace).await?;
    push_phase(&mut phase_times_ms, "open_db", phase_start);
    let size = value_size as usize;
    let seed = derive_seed(DEFAULT_BASE_SEED, "random_write", value_size);
    let order = shuffled_indices(count as usize, seed);

    let op_start = high_res_now_ms();
    let mut checksum = 0u64;
    for &i in &order {
        let k = key(i);
        let v = value(size, (i % 251) as u8);
        checksum = checksum.wrapping_add(v[0] as u64);
        db.put(&k, &v)
            .map_err(|e| JsValue::from_str(&e.to_string()))?;
    }
    push_phase(&mut phase_times_ms, "op_put_loop", op_start);

    let flush_start = high_res_now_ms();
    db.flush().map_err(|e| JsValue::from_str(&e.to_string()))?;
    push_phase(&mut phase_times_ms, "final_flush", flush_start);
    let elapsed_ms = high_res_now_ms() - op_start;

    let runtime_stats = db.runtime_stats();
    drop(db);
    let phase_start = high_res_now_ms();
    OpfsFs::destroy(&namespace)
        .await
        .map_err(|e| JsValue::from_str(&e.to_string()))?;
    push_phase(&mut phase_times_ms, "teardown_destroy", phase_start);
    let fs_stats = tracked_fs.snapshot();
    let wall_elapsed_ms = high_res_now_ms() - wall_start;

    Ok(BenchmarkResult {
        operation: "random_write".to_string(),
        value_size,
        count,
        seed,
        wall_elapsed_ms,
        elapsed_ms,
        ops_per_sec: (count as f64) / (elapsed_ms / 1000.0),
        p95_op_ms: 0.0,
        reads: 0,
        read_hits: 0,
        read_misses: 0,
        writes: count,
        deletes: 0,
        checksum,
        phase_times_ms,
        fs_stats,
        runtime_stats,
    })
}

async fn run_seq_read(count: u32, value_size: u32) -> Result<BenchmarkResult, JsValue> {
    let mut phase_times_ms = Vec::new();
    let wall_start = high_res_now_ms();
    let namespace = unique_namespace("seq-read");
    let phase_start = high_res_now_ms();
    OpfsFs::destroy(&namespace)
        .await
        .map_err(|e| JsValue::from_str(&e.to_string()))?;
    push_phase(&mut phase_times_ms, "cleanup_destroy", phase_start);

    let phase_start = high_res_now_ms();
    let (mut db, tracked_fs) = open_db(&namespace).await?;
    push_phase(&mut phase_times_ms, "open_db", phase_start);
    let size = value_size as usize;
    let seed = derive_seed(DEFAULT_BASE_SEED, "seq_read", value_size);

    let prefill_puts_start = high_res_now_ms();
    for i in 0..(count as usize) {
        let k = key(i);
        let v = value(size, (i % 251) as u8);
        db.put(&k, &v)
            .map_err(|e| JsValue::from_str(&e.to_string()))?;
    }
    push_phase(&mut phase_times_ms, "prefill_put_loop", prefill_puts_start);
    let prefill_flush_start = high_res_now_ms();
    db.flush().map_err(|e| JsValue::from_str(&e.to_string()))?;
    push_phase(&mut phase_times_ms, "prefill_flush", prefill_flush_start);

    let op_start = high_res_now_ms();
    let mut checksum = 0u64;
    for i in 0..(count as usize) {
        let k = key(i);
        let v = db
            .get(&k)
            .map_err(|e| JsValue::from_str(&e.to_string()))?
            .ok_or_else(|| JsValue::from_str("missing key during seq_read benchmark"))?;
        checksum = checksum.wrapping_add(v[0] as u64);
    }
    push_phase(&mut phase_times_ms, "op_read_loop", op_start);
    let elapsed_ms = high_res_now_ms() - op_start;

    let runtime_stats = db.runtime_stats();
    drop(db);
    let phase_start = high_res_now_ms();
    OpfsFs::destroy(&namespace)
        .await
        .map_err(|e| JsValue::from_str(&e.to_string()))?;
    push_phase(&mut phase_times_ms, "teardown_destroy", phase_start);
    let fs_stats = tracked_fs.snapshot();
    let wall_elapsed_ms = high_res_now_ms() - wall_start;

    Ok(BenchmarkResult {
        operation: "seq_read".to_string(),
        value_size,
        count,
        seed,
        wall_elapsed_ms,
        elapsed_ms,
        ops_per_sec: (count as f64) / (elapsed_ms / 1000.0),
        p95_op_ms: 0.0,
        reads: count,
        read_hits: count,
        read_misses: 0,
        writes: 0,
        deletes: 0,
        checksum,
        phase_times_ms,
        fs_stats,
        runtime_stats,
    })
}

async fn run_random_read(count: u32, value_size: u32) -> Result<BenchmarkResult, JsValue> {
    let mut phase_times_ms = Vec::new();
    let wall_start = high_res_now_ms();
    let namespace = unique_namespace("rand-read");
    let phase_start = high_res_now_ms();
    OpfsFs::destroy(&namespace)
        .await
        .map_err(|e| JsValue::from_str(&e.to_string()))?;
    push_phase(&mut phase_times_ms, "cleanup_destroy", phase_start);

    let phase_start = high_res_now_ms();
    let (mut db, tracked_fs) = open_db(&namespace).await?;
    push_phase(&mut phase_times_ms, "open_db", phase_start);
    let size = value_size as usize;
    let seed = derive_seed(DEFAULT_BASE_SEED, "random_read", value_size);

    let prefill_puts_start = high_res_now_ms();
    for i in 0..(count as usize) {
        let k = key(i);
        let v = value(size, (i % 251) as u8);
        db.put(&k, &v)
            .map_err(|e| JsValue::from_str(&e.to_string()))?;
    }
    push_phase(&mut phase_times_ms, "prefill_put_loop", prefill_puts_start);
    let prefill_flush_start = high_res_now_ms();
    db.flush().map_err(|e| JsValue::from_str(&e.to_string()))?;
    push_phase(&mut phase_times_ms, "prefill_flush", prefill_flush_start);

    let order = shuffled_indices(count as usize, seed);

    let op_start = high_res_now_ms();
    let mut checksum = 0u64;
    for &i in &order {
        let k = key(i);
        let v = db
            .get(&k)
            .map_err(|e| JsValue::from_str(&e.to_string()))?
            .ok_or_else(|| JsValue::from_str("missing key during random_read benchmark"))?;
        checksum = checksum.wrapping_add(v[0] as u64);
    }
    push_phase(&mut phase_times_ms, "op_read_loop", op_start);
    let elapsed_ms = high_res_now_ms() - op_start;

    let runtime_stats = db.runtime_stats();
    drop(db);
    let phase_start = high_res_now_ms();
    OpfsFs::destroy(&namespace)
        .await
        .map_err(|e| JsValue::from_str(&e.to_string()))?;
    push_phase(&mut phase_times_ms, "teardown_destroy", phase_start);
    let fs_stats = tracked_fs.snapshot();
    let wall_elapsed_ms = high_res_now_ms() - wall_start;

    Ok(BenchmarkResult {
        operation: "random_read".to_string(),
        value_size,
        count,
        seed,
        wall_elapsed_ms,
        elapsed_ms,
        ops_per_sec: (count as f64) / (elapsed_ms / 1000.0),
        p95_op_ms: 0.0,
        reads: count,
        read_hits: count,
        read_misses: 0,
        writes: 0,
        deletes: 0,
        checksum,
        phase_times_ms,
        fs_stats,
        runtime_stats,
    })
}

async fn run_mixed_scenario(
    scenario: MixedScenario,
    count: u32,
    value_size: u32,
    base_seed: u64,
) -> Result<BenchmarkResult, JsValue> {
    let mut phase_times_ms = Vec::new();
    let wall_start = high_res_now_ms();
    let namespace = unique_namespace(scenario.name);
    let phase_start = high_res_now_ms();
    OpfsFs::destroy(&namespace)
        .await
        .map_err(|e| JsValue::from_str(&e.to_string()))?;
    push_phase(&mut phase_times_ms, "cleanup_destroy", phase_start);

    let phase_start = high_res_now_ms();
    let (mut db, tracked_fs) = open_db(&namespace).await?;
    push_phase(&mut phase_times_ms, "open_db", phase_start);
    let size = value_size as usize;
    let seed = derive_seed(base_seed, scenario.name, value_size);
    let initial_key_space = (count as usize).max(1);

    let prefill_puts_start = high_res_now_ms();
    for i in 0..initial_key_space {
        let k = key(i);
        let v = value(size, (i % 251) as u8);
        db.put(&k, &v)
            .map_err(|e| JsValue::from_str(&e.to_string()))?;
    }
    push_phase(&mut phase_times_ms, "prefill_put_loop", prefill_puts_start);
    let prefill_flush_start = high_res_now_ms();
    db.flush().map_err(|e| JsValue::from_str(&e.to_string()))?;
    push_phase(&mut phase_times_ms, "prefill_flush", prefill_flush_start);

    let mut rng = DeterministicRng::new(seed);
    let mut key_space = initial_key_space;

    let mut reads = 0u32;
    let mut read_hits = 0u32;
    let mut read_misses = 0u32;
    let mut writes = 0u32;
    let mut deletes = 0u32;
    let mut checksum = 0u64;
    let mut op_latencies_ms = Vec::with_capacity(count as usize);

    let op_start = high_res_now_ms();
    for step in 0..(count as usize) {
        let op = choose_operation(scenario, rng.next_u8() % 100);
        let op_start = high_res_now_ms();

        match op {
            OpChoice::Read => {
                reads += 1;
                let idx = rng.next_usize(key_space.max(1));
                let k = key(idx);
                let maybe = db.get(&k).map_err(|e| JsValue::from_str(&e.to_string()))?;
                if let Some(v) = maybe {
                    read_hits += 1;
                    checksum = checksum.wrapping_add(v[0] as u64);
                } else {
                    read_misses += 1;
                    checksum = checksum.wrapping_add(1);
                }
            }
            OpChoice::Write => {
                writes += 1;
                let update = (rng.next_u8() % 100) < scenario.update_pct;
                let idx = if update || key_space == 0 {
                    rng.next_usize(key_space.max(1))
                } else {
                    let i = key_space;
                    key_space += 1;
                    i
                };
                let k = key(idx);
                let v = value(size, ((step + idx) % 251) as u8);
                checksum = checksum.wrapping_add(v[0] as u64);
                db.put(&k, &v)
                    .map_err(|e| JsValue::from_str(&e.to_string()))?;
            }
            OpChoice::Delete => {
                deletes += 1;
                let idx = rng.next_usize(key_space.max(1));
                let k = key(idx);
                db.delete(&k)
                    .map_err(|e| JsValue::from_str(&e.to_string()))?;
                checksum = checksum.wrapping_add(idx as u64);
            }
        }

        op_latencies_ms.push(high_res_now_ms() - op_start);
    }
    push_phase(&mut phase_times_ms, "op_mixed_loop", op_start);
    let flush_start = high_res_now_ms();
    db.flush().map_err(|e| JsValue::from_str(&e.to_string()))?;
    push_phase(&mut phase_times_ms, "final_flush", flush_start);
    let elapsed_ms = high_res_now_ms() - op_start;

    let runtime_stats = db.runtime_stats();
    drop(db);
    let phase_start = high_res_now_ms();
    OpfsFs::destroy(&namespace)
        .await
        .map_err(|e| JsValue::from_str(&e.to_string()))?;
    push_phase(&mut phase_times_ms, "teardown_destroy", phase_start);
    let fs_stats = tracked_fs.snapshot();
    let wall_elapsed_ms = high_res_now_ms() - wall_start;

    Ok(BenchmarkResult {
        operation: scenario.name.to_string(),
        value_size,
        count,
        seed,
        wall_elapsed_ms,
        elapsed_ms,
        ops_per_sec: (count as f64) / (elapsed_ms / 1000.0),
        p95_op_ms: percentile_ms(&mut op_latencies_ms, 0.95),
        reads,
        read_hits,
        read_misses,
        writes,
        deletes,
        checksum,
        phase_times_ms,
        fs_stats,
        runtime_stats,
    })
}

#[wasm_bindgen]
pub async fn bench_opfs_sequential_write(count: u32, value_size: u32) -> Result<JsValue, JsValue> {
    to_js_value(&run_seq_write(count, value_size).await?)
}

#[wasm_bindgen]
pub async fn bench_opfs_random_write(count: u32, value_size: u32) -> Result<JsValue, JsValue> {
    to_js_value(&run_random_write(count, value_size).await?)
}

#[wasm_bindgen]
pub async fn bench_opfs_sequential_read(count: u32, value_size: u32) -> Result<JsValue, JsValue> {
    to_js_value(&run_seq_read(count, value_size).await?)
}

#[wasm_bindgen]
pub async fn bench_opfs_random_read(count: u32, value_size: u32) -> Result<JsValue, JsValue> {
    to_js_value(&run_random_read(count, value_size).await?)
}

#[wasm_bindgen]
pub async fn bench_opfs_mixed_scenario(
    scenario_name: String,
    count: u32,
    value_size: u32,
    base_seed: Option<u64>,
) -> Result<JsValue, JsValue> {
    let scenario = find_mixed_scenario(&scenario_name)
        .ok_or_else(|| JsValue::from_str(&format!("unknown mixed scenario: {scenario_name}")))?;
    to_js_value(&run_mixed_scenario(
        scenario,
        count,
        value_size,
        base_seed.unwrap_or(DEFAULT_BASE_SEED),
    )
    .await?)
}

#[wasm_bindgen]
pub async fn bench_opfs_matrix(count: u32) -> Result<JsValue, JsValue> {
    let sizes = [32u32, 256u32, 4096u32];
    let mut out = Vec::new();

    for value_size in sizes {
        out.push(run_seq_write(count, value_size).await?);
        out.push(run_random_write(count, value_size).await?);
        out.push(run_seq_read(count, value_size).await?);
        out.push(run_random_read(count, value_size).await?);
    }

    serde_wasm_bindgen::to_value(&out)
        .map_err(|e| JsValue::from_str(&format!("serialize benchmark matrix: {e}")))
}

#[wasm_bindgen]
pub async fn bench_opfs_mixed_matrix(count: u32) -> Result<JsValue, JsValue> {
    let sizes = [32u32, 256u32, 4096u32];
    let mut out = Vec::new();

    for value_size in sizes {
        for scenario in MIXED_SCENARIOS {
            out.push(run_mixed_scenario(scenario, count, value_size, DEFAULT_BASE_SEED).await?);
        }
    }

    serde_wasm_bindgen::to_value(&out)
        .map_err(|e| JsValue::from_str(&format!("serialize mixed benchmark matrix: {e}")))
}
