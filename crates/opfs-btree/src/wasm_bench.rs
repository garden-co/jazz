#![cfg(target_arch = "wasm32")]

use serde::{Deserialize, Serialize};
use wasm_bindgen::prelude::*;

use crate::bench_dataset::{
    Phase, PhaseKind, RANGE_RESULT_LIMIT, RANGE_WINDOW_KEYS, decode_kv, decode_ops,
};
use crate::{BTreeOptions, OpfsBTree, OpfsFile};

const BENCH_CACHE_BYTES: usize = 32 * 1024 * 1024;
const BENCH_OVERFLOW_THRESHOLD: usize = 4 * 1024;
const BENCH_PIN_INTERNAL_PAGES: bool = true;
const BENCH_READ_COALESCE_PAGES: usize = 4;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatasetPhaseResult {
    pub phase: String,
    pub op_count: u32,
    pub elapsed_ms: f64,
    pub ops_per_sec: f64,
    pub checksum: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatasetRunResult {
    pub engine: String,
    pub profile: String,
    pub record_count: u32,
    pub phases: Vec<DatasetPhaseResult>,
    pub checksum: u64,
}

fn benchmark_options() -> BTreeOptions {
    BTreeOptions {
        page_size: 16 * 1024,
        cache_bytes: BENCH_CACHE_BYTES,
        overflow_threshold: BENCH_OVERFLOW_THRESHOLD,
        pin_internal_pages: BENCH_PIN_INTERNAL_PAGES,
        read_coalesce_pages: BENCH_READ_COALESCE_PAGES,
    }
}

fn now_ms() -> f64 {
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

async fn open_db(namespace: &str) -> Result<OpfsBTree<OpfsFile>, JsValue> {
    let file = OpfsFile::open(namespace)
        .await
        .map_err(|e| JsValue::from_str(&e.to_string()))?;
    OpfsBTree::open(file, benchmark_options()).map_err(|e| JsValue::from_str(&e.to_string()))
}

pub async fn run_dataset_result(
    kv_bytes: &[u8],
    ops_bytes: &[u8],
) -> Result<DatasetRunResult, JsValue> {
    let data = decode_kv(kv_bytes).map_err(|e| JsValue::from_str(&e.to_string()))?;
    let phases = decode_ops(ops_bytes).map_err(|e| JsValue::from_str(&e.to_string()))?;
    let keys: Vec<&[u8]> = data.records.iter().map(|(k, _)| k.as_slice()).collect();
    let vals: Vec<&[u8]> = data.records.iter().map(|(_, v)| v.as_slice()).collect();
    let n = keys.len() as u32;

    let namespace = "bench-dataset";
    OpfsFile::destroy(namespace).await.ok();
    let mut db = open_db(namespace).await?;

    let mut overall: u64 = n as u64;
    let mut phase_results = Vec::new();

    for phase in &phases {
        let started = now_ms();
        let (new_db, op_count, checksum) = replay_phase(db, phase, &keys, &vals, namespace).await?;
        db = new_db;
        let elapsed = now_ms() - started;
        overall = overall.wrapping_add(checksum);
        phase_results.push(DatasetPhaseResult {
            phase: phase.name.clone(),
            op_count,
            elapsed_ms: elapsed,
            ops_per_sec: if elapsed > 0.0 {
                (op_count as f64) / (elapsed / 1000.0)
            } else {
                0.0
            },
            checksum,
        });
    }

    OpfsFile::destroy(namespace).await.ok();
    Ok(DatasetRunResult {
        engine: "opfs_btree".into(),
        profile: data.profile,
        record_count: n,
        phases: phase_results,
        checksum: overall,
    })
}

async fn replay_phase(
    mut db: OpfsBTree<OpfsFile>,
    phase: &Phase,
    keys: &[&[u8]],
    vals: &[&[u8]],
    namespace: &str,
) -> Result<(OpfsBTree<OpfsFile>, u32, u64), JsValue> {
    let n = keys.len() as u32;
    let mut checksum: u64 = 0;
    let mut ops: u32 = 0;
    let map = |e: crate::BTreeError| JsValue::from_str(&e.to_string());

    match phase.kind {
        PhaseKind::LoadAll => {
            for (k, v) in keys.iter().zip(vals.iter()) {
                db.put(k, v).map_err(map)?;
                ops += 1;
            }
            db.checkpoint().map_err(map)?;
        }
        PhaseKind::GetSeq => {
            for k in keys {
                if let Some(v) = db.get(k).map_err(map)? {
                    checksum = checksum.wrapping_add(v.first().copied().unwrap_or(0) as u64);
                }
                ops += 1;
            }
        }
        PhaseKind::GetIndices | PhaseKind::ColdGetIndices => {
            if phase.kind == PhaseKind::ColdGetIndices {
                db.checkpoint().map_err(map)?;
                drop(db);
                db = open_db(namespace).await?;
            }
            for &idx in &phase.args {
                let i = (idx % n.max(1)) as usize;
                if let Some(v) = db.get(keys[i]).map_err(map)? {
                    checksum = checksum.wrapping_add(v.first().copied().unwrap_or(0) as u64);
                }
                ops += 1;
            }
        }
        PhaseKind::UpdateIndices => {
            for &idx in &phase.args {
                let i = (idx % n.max(1)) as usize;
                db.put(keys[i], vals[i]).map_err(map)?;
                ops += 1;
            }
            db.checkpoint().map_err(map)?;
        }
        PhaseKind::RangeStarts => {
            for &start in &phase.args {
                let s = (start % n.max(1)) as usize;
                let e = (s + RANGE_WINDOW_KEYS as usize).min(keys.len().saturating_sub(1));
                let rows = db
                    .range(keys[s], keys[e], RANGE_RESULT_LIMIT as usize)
                    .map_err(map)?;
                checksum = checksum.wrapping_add(rows.len() as u64);
                ops += 1;
            }
        }
        PhaseKind::Mixed => {
            for &packed in &phase.args {
                let op = packed >> 30;
                let i = ((packed & 0x3FFF_FFFF) % n.max(1)) as usize;
                match op {
                    1 => {
                        db.put(keys[i], vals[i]).map_err(map)?;
                    }
                    2 => {
                        db.delete(keys[i]).map_err(map)?;
                    }
                    _ => {
                        if let Some(v) = db.get(keys[i]).map_err(map)? {
                            checksum =
                                checksum.wrapping_add(v.first().copied().unwrap_or(0) as u64);
                        }
                    }
                }
                ops += 1;
            }
            db.checkpoint().map_err(map)?;
        }
    }
    Ok((db, ops, checksum))
}

#[wasm_bindgen]
pub async fn bench_dataset_run(kv: &[u8], ops: &[u8]) -> Result<JsValue, JsValue> {
    let result = run_dataset_result(kv, ops).await?;
    serde_wasm_bindgen::to_value(&result).map_err(|e| JsValue::from_str(&e.to_string()))
}
