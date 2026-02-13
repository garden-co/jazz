#![cfg(target_arch = "wasm32")]

use serde::Serialize;
use wasm_bindgen::prelude::*;

use crate::{LsmOptions, LsmTree, OpfsFs, WriteDurability};

#[derive(Debug, Clone, Serialize)]
struct BenchmarkResult {
    operation: String,
    value_size: u32,
    count: u32,
    elapsed_ms: f64,
    ops_per_sec: f64,
    p95_op_ms: f64,
    reads: u32,
    read_hits: u32,
    read_misses: u32,
    writes: u32,
    deletes: u32,
    checksum: u64,
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

fn shuffled_indices(n: usize) -> Vec<usize> {
    let mut out: Vec<usize> = (0..n).collect();
    let mut state: u64 = 0xD1B54A32D192ED03;
    for i in (1..n).rev() {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
        let j = (state as usize) % (i + 1);
        out.swap(i, j);
    }
    out
}

fn unique_namespace(label: &str) -> String {
    let ts = js_sys::Date::now() as u64;
    let rand = (js_sys::Math::random() * 1_000_000.0) as u64;
    format!("bench-{label}-{ts}-{rand}")
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

async fn open_db(namespace: &str) -> Result<LsmTree<OpfsFs>, JsValue> {
    let fs = OpfsFs::open(namespace)
        .await
        .map_err(|e| JsValue::from_str(&e.to_string()))?;
    LsmTree::open(fs, benchmark_options(), Vec::new())
        .map_err(|e| JsValue::from_str(&e.to_string()))
}

fn to_js_value(result: &BenchmarkResult) -> Result<JsValue, JsValue> {
    serde_wasm_bindgen::to_value(result)
        .map_err(|e| JsValue::from_str(&format!("serialize benchmark result: {e}")))
}

async fn run_seq_write(count: u32, value_size: u32) -> Result<BenchmarkResult, JsValue> {
    let namespace = unique_namespace("seq-write");
    OpfsFs::destroy(&namespace)
        .await
        .map_err(|e| JsValue::from_str(&e.to_string()))?;

    let mut db = open_db(&namespace).await?;
    let size = value_size as usize;

    let start = js_sys::Date::now();
    let mut checksum = 0u64;
    for i in 0..(count as usize) {
        let k = key(i);
        let v = value(size, (i % 251) as u8);
        checksum = checksum.wrapping_add(v[0] as u64);
        db.put(&k, &v)
            .map_err(|e| JsValue::from_str(&e.to_string()))?;
    }
    db.flush().map_err(|e| JsValue::from_str(&e.to_string()))?;
    let elapsed_ms = js_sys::Date::now() - start;

    OpfsFs::destroy(&namespace)
        .await
        .map_err(|e| JsValue::from_str(&e.to_string()))?;

    Ok(BenchmarkResult {
        operation: "seq_write".to_string(),
        value_size,
        count,
        elapsed_ms,
        ops_per_sec: (count as f64) / (elapsed_ms / 1000.0),
        p95_op_ms: 0.0,
        reads: 0,
        read_hits: 0,
        read_misses: 0,
        writes: count,
        deletes: 0,
        checksum,
    })
}

async fn run_random_write(count: u32, value_size: u32) -> Result<BenchmarkResult, JsValue> {
    let namespace = unique_namespace("rand-write");
    OpfsFs::destroy(&namespace)
        .await
        .map_err(|e| JsValue::from_str(&e.to_string()))?;

    let mut db = open_db(&namespace).await?;
    let size = value_size as usize;
    let order = shuffled_indices(count as usize);

    let start = js_sys::Date::now();
    let mut checksum = 0u64;
    for &i in &order {
        let k = key(i);
        let v = value(size, (i % 251) as u8);
        checksum = checksum.wrapping_add(v[0] as u64);
        db.put(&k, &v)
            .map_err(|e| JsValue::from_str(&e.to_string()))?;
    }
    db.flush().map_err(|e| JsValue::from_str(&e.to_string()))?;
    let elapsed_ms = js_sys::Date::now() - start;

    OpfsFs::destroy(&namespace)
        .await
        .map_err(|e| JsValue::from_str(&e.to_string()))?;

    Ok(BenchmarkResult {
        operation: "random_write".to_string(),
        value_size,
        count,
        elapsed_ms,
        ops_per_sec: (count as f64) / (elapsed_ms / 1000.0),
        p95_op_ms: 0.0,
        reads: 0,
        read_hits: 0,
        read_misses: 0,
        writes: count,
        deletes: 0,
        checksum,
    })
}

async fn run_seq_read(count: u32, value_size: u32) -> Result<BenchmarkResult, JsValue> {
    let namespace = unique_namespace("seq-read");
    OpfsFs::destroy(&namespace)
        .await
        .map_err(|e| JsValue::from_str(&e.to_string()))?;

    let mut db = open_db(&namespace).await?;
    let size = value_size as usize;

    for i in 0..(count as usize) {
        let k = key(i);
        let v = value(size, (i % 251) as u8);
        db.put(&k, &v)
            .map_err(|e| JsValue::from_str(&e.to_string()))?;
    }
    db.flush().map_err(|e| JsValue::from_str(&e.to_string()))?;

    let start = js_sys::Date::now();
    let mut checksum = 0u64;
    for i in 0..(count as usize) {
        let k = key(i);
        let v = db
            .get(&k)
            .map_err(|e| JsValue::from_str(&e.to_string()))?
            .ok_or_else(|| JsValue::from_str("missing key during seq_read benchmark"))?;
        checksum = checksum.wrapping_add(v[0] as u64);
    }
    let elapsed_ms = js_sys::Date::now() - start;

    OpfsFs::destroy(&namespace)
        .await
        .map_err(|e| JsValue::from_str(&e.to_string()))?;

    Ok(BenchmarkResult {
        operation: "seq_read".to_string(),
        value_size,
        count,
        elapsed_ms,
        ops_per_sec: (count as f64) / (elapsed_ms / 1000.0),
        p95_op_ms: 0.0,
        reads: count,
        read_hits: count,
        read_misses: 0,
        writes: 0,
        deletes: 0,
        checksum,
    })
}

async fn run_random_read(count: u32, value_size: u32) -> Result<BenchmarkResult, JsValue> {
    let namespace = unique_namespace("rand-read");
    OpfsFs::destroy(&namespace)
        .await
        .map_err(|e| JsValue::from_str(&e.to_string()))?;

    let mut db = open_db(&namespace).await?;
    let size = value_size as usize;

    for i in 0..(count as usize) {
        let k = key(i);
        let v = value(size, (i % 251) as u8);
        db.put(&k, &v)
            .map_err(|e| JsValue::from_str(&e.to_string()))?;
    }
    db.flush().map_err(|e| JsValue::from_str(&e.to_string()))?;

    let order = shuffled_indices(count as usize);

    let start = js_sys::Date::now();
    let mut checksum = 0u64;
    for &i in &order {
        let k = key(i);
        let v = db
            .get(&k)
            .map_err(|e| JsValue::from_str(&e.to_string()))?
            .ok_or_else(|| JsValue::from_str("missing key during random_read benchmark"))?;
        checksum = checksum.wrapping_add(v[0] as u64);
    }
    let elapsed_ms = js_sys::Date::now() - start;

    OpfsFs::destroy(&namespace)
        .await
        .map_err(|e| JsValue::from_str(&e.to_string()))?;

    Ok(BenchmarkResult {
        operation: "random_read".to_string(),
        value_size,
        count,
        elapsed_ms,
        ops_per_sec: (count as f64) / (elapsed_ms / 1000.0),
        p95_op_ms: 0.0,
        reads: count,
        read_hits: count,
        read_misses: 0,
        writes: 0,
        deletes: 0,
        checksum,
    })
}

async fn run_mixed_scenario(
    scenario: MixedScenario,
    count: u32,
    value_size: u32,
) -> Result<BenchmarkResult, JsValue> {
    let namespace = unique_namespace(scenario.name);
    OpfsFs::destroy(&namespace)
        .await
        .map_err(|e| JsValue::from_str(&e.to_string()))?;

    let mut db = open_db(&namespace).await?;
    let size = value_size as usize;
    let initial_key_space = (count as usize).max(1);

    for i in 0..initial_key_space {
        let k = key(i);
        let v = value(size, (i % 251) as u8);
        db.put(&k, &v)
            .map_err(|e| JsValue::from_str(&e.to_string()))?;
    }
    db.flush().map_err(|e| JsValue::from_str(&e.to_string()))?;

    let mut rng = DeterministicRng::new(0xA5A5_A5A5_0123_4567 ^ (value_size as u64));
    let mut key_space = initial_key_space;

    let mut reads = 0u32;
    let mut read_hits = 0u32;
    let mut read_misses = 0u32;
    let mut writes = 0u32;
    let mut deletes = 0u32;
    let mut checksum = 0u64;
    let mut op_latencies_ms = Vec::with_capacity(count as usize);

    let total_start = js_sys::Date::now();
    for step in 0..(count as usize) {
        let op = choose_operation(scenario, rng.next_u8() % 100);
        let op_start = js_sys::Date::now();

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

        op_latencies_ms.push(js_sys::Date::now() - op_start);
    }
    db.flush().map_err(|e| JsValue::from_str(&e.to_string()))?;
    let elapsed_ms = js_sys::Date::now() - total_start;

    OpfsFs::destroy(&namespace)
        .await
        .map_err(|e| JsValue::from_str(&e.to_string()))?;

    Ok(BenchmarkResult {
        operation: scenario.name.to_string(),
        value_size,
        count,
        elapsed_ms,
        ops_per_sec: (count as f64) / (elapsed_ms / 1000.0),
        p95_op_ms: percentile_ms(&mut op_latencies_ms, 0.95),
        reads,
        read_hits,
        read_misses,
        writes,
        deletes,
        checksum,
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
) -> Result<JsValue, JsValue> {
    let scenario = find_mixed_scenario(&scenario_name)
        .ok_or_else(|| JsValue::from_str(&format!("unknown mixed scenario: {scenario_name}")))?;
    to_js_value(&run_mixed_scenario(scenario, count, value_size).await?)
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
            out.push(run_mixed_scenario(scenario, count, value_size).await?);
        }
    }

    serde_wasm_bindgen::to_value(&out)
        .map_err(|e| JsValue::from_str(&format!("serialize mixed benchmark matrix: {e}")))
}
