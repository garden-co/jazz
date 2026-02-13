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
    checksum: u64,
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
