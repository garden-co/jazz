use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RunProfile {
    pub profile: String,
    pub base_url: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PhaseResult {
    pub phase: String,
    pub op_count: u32,
    pub elapsed_ms: f64,
    pub ops_per_sec: f64,
    pub checksum: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EngineRunResult {
    pub engine: String,
    pub profile: String,
    pub record_count: u32,
    pub phases: Vec<PhaseResult>,
    pub checksum: u64,
}

impl From<bench_core::DatasetRunResult> for EngineRunResult {
    fn from(result: bench_core::DatasetRunResult) -> Self {
        EngineRunResult {
            engine: result.engine,
            profile: result.profile,
            record_count: result.record_count,
            phases: result
                .phases
                .into_iter()
                .map(|phase| PhaseResult {
                    phase: phase.phase,
                    op_count: phase.op_count,
                    elapsed_ms: phase.elapsed_ms,
                    ops_per_sec: phase.ops_per_sec,
                    checksum: phase.checksum,
                })
                .collect(),
            checksum: result.checksum,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct WorkerFailure {
    pub engine: String,
    pub profile: String,
    pub error: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum WorkerResult {
    Ok(EngineRunResult),
    Err(WorkerFailure),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ProfileComparison {
    pub profile: String,
    pub btree: EngineRunResult,
    pub sqlite: EngineRunResult,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BenchmarkFailure {
    pub profile: Option<String>,
    pub error: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AutomationResult {
    pub ok: bool,
    pub results: Vec<ProfileComparison>,
    pub error: Option<BenchmarkFailure>,
}
