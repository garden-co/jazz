//! The one run loop, shared by both engine workers. Owns timing, the
//! cross-engine checksum, phase bracketing, cold reopen, and the
//! repeat-until-measurable retry for fast read phases — so neither engine
//! crate carries any of it.

use crate::benchmarks::Benchmark;
use crate::dataset::KvDataset;
use crate::engine::{BenchEngine, EngineError};
use crate::phases::replay;
use crate::result::{DatasetPhaseResult, DatasetRunResult};
use crate::rng::SplitMix64;

/// Read phases faster than this are repeated until they reach a measurable
/// duration, so throughput is computed over a meaningful sample.
const MIN_MEASURABLE_MS: f64 = 10.0;
const MAX_LOOP_ITERATIONS: u32 = 10_000;

/// Run `benchmark` against `engine` over `dataset`, timing each phase with the
/// injected `now_ms` clock (kept out of this crate so it stays pure and
/// testable on native).
pub async fn run<E: BenchEngine>(
    engine: &mut E,
    engine_name: &str,
    benchmark: &Benchmark,
    dataset: &KvDataset,
    now_ms: &dyn Fn() -> f64,
) -> Result<DatasetRunResult, EngineError> {
    let keys: Vec<&[u8]> = dataset.records.iter().map(|(k, _)| k.as_slice()).collect();
    let vals: Vec<&[u8]> = dataset.records.iter().map(|(_, v)| v.as_slice()).collect();
    let n = keys.len() as u32;

    let mut overall: u64 = n as u64;
    let mut phase_results = Vec::with_capacity(benchmark.phases.len());

    for (i, phase) in benchmark.phases.iter().enumerate() {
        let kind = phase.kind();
        // Derive a per-phase substream so phases are independent yet stable.
        let mut rng = SplitMix64::new(
            benchmark
                .seed
                .wrapping_add((i as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15)),
        );
        let args = phase.gen_args(n, &mut rng);

        // Time the cold reopen, bracketing, and operations together — matching
        // what each engine actually pays for the phase.
        let started = now_ms();
        if kind.is_cold() {
            engine.reopen().await?;
        }
        engine.begin_phase(kind)?;
        let (op_count, checksum) = replay(engine, phase, &args, &keys, &vals)?;
        engine.end_phase(kind)?;
        let mut elapsed = now_ms() - started;
        let mut total_ops = op_count;

        if kind.is_retryable_read() && elapsed < MIN_MEASURABLE_MS {
            let mut iterations = 1u32;
            while elapsed < MIN_MEASURABLE_MS && iterations < MAX_LOOP_ITERATIONS {
                engine.begin_phase(kind)?;
                let (ops2, _) = replay(engine, phase, &args, &keys, &vals)?;
                engine.end_phase(kind)?;
                total_ops = total_ops.saturating_add(ops2);
                elapsed = now_ms() - started;
                iterations += 1;
            }
        }

        overall = overall.wrapping_add(checksum);
        phase_results.push(DatasetPhaseResult {
            phase: phase.name(),
            op_count: total_ops,
            elapsed_ms: elapsed,
            ops_per_sec: if elapsed > 0.0 {
                (total_ops as f64) / (elapsed / 1000.0)
            } else {
                0.0
            },
            checksum,
        });
    }

    Ok(DatasetRunResult {
        engine: engine_name.to_string(),
        profile: benchmark.profile.clone(),
        record_count: n,
        phases: phase_results,
        checksum: overall,
    })
}
