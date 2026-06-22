//! The per-run result shape, shared by both engines so the harness can compare
//! them phase-by-phase and assert identical checksums.

use serde::{Deserialize, Serialize};

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
