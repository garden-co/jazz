//! Shared simulation and benchmark harness for Jazz scenarios on jazz-db.
//!
//! A scenario builds a [`net::Sim`]: named `Db` nodes (an authority, its
//! clients, relays) joined by simulated links with a declared latency
//! profile. The simulation is deterministic: one virtual clock, seeded link
//! jitter, and every node ticked in declaration order. Scenarios assert
//! their correctness oracle on the deterministic run (`INV-BENCH-5`); the
//! timing run is the same workload at a larger scale with wall-clock
//! measurements around it.

pub mod distributions;
pub mod fixture;
pub mod mem;
pub mod net;
pub mod oracle;
#[cfg(feature = "cold-settle-attribution")]
pub mod phase_attribution;
pub mod profiling;
pub mod public_schema_fixture;
pub mod support;

use serde_json::{Map, Value, json};
use std::collections::BTreeMap;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;
use std::process::Command;
use std::sync::OnceLock;

pub use net::{LinkStats, PauseMode, Sim, SimulatorTransportCodec};

/// Network profile for one simulated link (both directions).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PeerProfile {
    /// Profile name reported in benchmark output.
    pub name: String,
    /// Base one-way latency in virtual milliseconds.
    pub one_way_latency_ms: u64,
    /// Maximum deterministic jitter added per message, in milliseconds.
    pub jitter_ms: u64,
    /// Fixed per-message delivery overhead in milliseconds.
    pub per_message_overhead_ms: u64,
}

impl PeerProfile {
    /// Construct a peer profile.
    pub fn new(
        name: impl Into<String>,
        one_way_latency_ms: u64,
        jitter_ms: u64,
        per_message_overhead_ms: u64,
    ) -> Self {
        Self {
            name: name.into(),
            one_way_latency_ms,
            jitter_ms,
            per_message_overhead_ms,
        }
    }

    /// A link with no latency at all.
    pub fn instant() -> Self {
        Self::new("instant", 0, 0, 0)
    }

    pub(crate) fn latency_ms(&self, rng: &mut distributions::Lcg) -> u64 {
        self.one_way_latency_ms
            + self.per_message_overhead_ms
            + if self.jitter_ms == 0 {
                0
            } else {
                rng.next_u64() % (self.jitter_ms + 1)
            }
    }
}

/// Benchmark scale selected by `JAZZ_BENCH_PROFILE`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BenchProfile {
    /// Fast, low-cardinality defaults for interactive iteration.
    Fast,
    /// Moderate defaults for profiler captures that need real work without full scale.
    Profile,
    /// Default product-scale-ish settings for retained snapshots and claims.
    Full,
}

impl BenchProfile {
    /// Read the current benchmark profile from the environment.
    pub fn from_env() -> Self {
        match std::env::var("JAZZ_BENCH_PROFILE").as_deref() {
            Ok("fast" | "FAST" | "interactive" | "INTERACTIVE") => Self::Fast,
            Ok("profile" | "PROFILE" | "profiling" | "PROFILING") => Self::Profile,
            _ => Self::Full,
        }
    }

    /// Select a default value for the active benchmark scale.
    pub fn select<T>(self, fast: T, profile: T, full: T) -> T {
        match self {
            Self::Fast => fast,
            Self::Profile => profile,
            Self::Full => full,
        }
    }
}

/// Active benchmark scale from `JAZZ_BENCH_PROFILE`.
pub fn bench_profile() -> BenchProfile {
    BenchProfile::from_env()
}

/// Read a `u64` knob from the environment.
pub fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

/// Read a `usize` knob from the environment.
pub fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

/// Build the shared benchmark metadata fields (`INV-BENCH-4`).
pub fn metadata_fields(
    scenario: impl Into<String>,
    driver: impl Into<String>,
    seed: u64,
    profile: impl Into<String>,
) -> Map<String, Value> {
    let mut fields = Map::new();
    fields.insert("scenario".to_owned(), json!(scenario.into()));
    fields.insert("driver".to_owned(), json!(driver.into()));
    fields.insert("seed".to_owned(), json!(seed));
    fields.insert("profile".to_owned(), json!(profile.into()));
    insert_process_metadata(&mut fields);
    fields
}

/// Emit one JSON object: the metadata fields for `scenario` plus `extra`.
pub fn emit_summary(scenario: &str, driver: &str, seed: u64, profile: &str, extra: Value) {
    let mut fields = metadata_fields(scenario, driver, seed, profile);
    if let Value::Object(extra) = extra {
        fields.extend(extra);
    }
    emit_json_line(scenario, &Value::Object(fields).to_string());
}

/// Print a JSONL line and optionally retain it under `benchmarks/results/jazz`.
pub fn emit_json_line(scenario: &str, line: &str) {
    println!("{line}");
    if retain_enabled() {
        // Anchor at the workspace root: cargo bench sets the CWD to the
        // package directory, not the workspace.
        let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        path.pop();
        path.push("benchmarks/results/jazz");
        std::fs::create_dir_all(&path).expect("create jazz results directory");
        path.push(format!("{scenario}.jsonl"));
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .expect("open retained jazz result");
        writeln!(file, "{line}").expect("append retained jazz result");
    }
}

/// Process metadata for the benchmark-metadata regression tests.
#[doc(hidden)]
pub fn process_metadata_for_test() -> Map<String, Value> {
    let mut fields = Map::new();
    insert_process_metadata(&mut fields);
    fields
}

fn git_output<const N: usize>(args: [&str; N]) -> Option<String> {
    Command::new("git")
        .args(args)
        .output()
        .ok()
        .and_then(git_output_from_output)
}

fn git_output_from_output(output: std::process::Output) -> Option<String> {
    if !output.status.success() {
        return None;
    }
    String::from_utf8(output.stdout)
        .ok()
        .map(|text| text.trim().to_owned())
}

fn git_status() -> (bool, bool) {
    match git_output(["status", "--porcelain"]) {
        Some(status) => {
            let dirty = if retain_result_dirty_ignored() {
                status.lines().any(|line| {
                    let path = line.get(3..).unwrap_or_default();
                    !(path.starts_with("benchmarks/results/")
                        || path.starts_with("\"benchmarks/results/"))
                })
            } else {
                !status.is_empty()
            };
            (true, dirty)
        }
        None => (false, true),
    }
}

fn hostname() -> String {
    std::env::var("HOSTNAME")
        .ok()
        .filter(|value| !value.is_empty())
        .or_else(|| {
            Command::new("hostname")
                .output()
                .ok()
                .filter(|output| output.status.success())
                .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_owned())
        })
        .unwrap_or_else(|| "unknown".to_owned())
}

#[derive(Clone, Debug)]
struct ProcessMetadata {
    git_sha: String,
    git_dirty: bool,
    git_status_available: bool,
    hostname: String,
    knobs: BTreeMap<String, String>,
}

static PROCESS_METADATA: OnceLock<ProcessMetadata> = OnceLock::new();

fn process_metadata() -> &'static ProcessMetadata {
    PROCESS_METADATA.get_or_init(|| {
        let (git_status_available, git_dirty) = git_status();
        ProcessMetadata {
            git_sha: git_output(["rev-parse", "HEAD"]).unwrap_or_default(),
            git_dirty,
            git_status_available,
            hostname: hostname(),
            knobs: knob_env(),
        }
    })
}

fn retain_result_dirty_ignored() -> bool {
    matches!(
        std::env::var("JAZZ_BENCH_IGNORE_RESULT_DIRTY").as_deref(),
        Ok("1" | "true" | "TRUE" | "yes" | "YES")
    )
}

fn insert_process_metadata(fields: &mut Map<String, Value>) {
    let metadata = process_metadata();
    fields.insert("git_sha".to_owned(), json!(metadata.git_sha));
    fields.insert("git_dirty".to_owned(), json!(metadata.git_dirty));
    fields.insert(
        "git_status_available".to_owned(),
        json!(metadata.git_status_available),
    );
    fields.insert("hostname".to_owned(), json!(metadata.hostname));
    fields.insert("knobs".to_owned(), json!(metadata.knobs));
}

fn knob_env() -> BTreeMap<String, String> {
    std::env::vars()
        .filter(|(key, _)| key.starts_with("JAZZ_"))
        .collect()
}

fn retain_enabled() -> bool {
    matches!(
        std::env::var("JAZZ_RETAIN").as_deref(),
        Ok("1" | "true" | "TRUE" | "yes" | "YES")
    )
}

/// A latency histogram in microseconds (or any other unit the caller picks).
pub struct Latencies {
    hist: hdrhistogram::Histogram<u64>,
}

impl Default for Latencies {
    fn default() -> Self {
        Self {
            hist: hdrhistogram::Histogram::new(3).expect("valid histogram precision"),
        }
    }
}

impl Latencies {
    pub fn record(&mut self, value: u64) {
        self.hist
            .record(value)
            .expect("sample within histogram range");
    }

    pub fn record_duration_us(&mut self, elapsed: std::time::Duration) {
        self.record(elapsed.as_micros().min(u64::MAX as u128) as u64);
    }

    pub fn len(&self) -> u64 {
        self.hist.len()
    }

    pub fn is_empty(&self) -> bool {
        self.hist.is_empty()
    }

    /// `{name}_p50` .. `{name}_max` fields.
    pub fn json_fields(&self, name: &str) -> Map<String, Value> {
        let mut fields = Map::new();
        for (suffix, quantile) in [("p50", 0.50), ("p95", 0.95), ("p99", 0.99)] {
            fields.insert(
                format!("{name}_{suffix}"),
                json!(self.hist.value_at_quantile(quantile)),
            );
        }
        fields.insert(format!("{name}_max"), json!(self.hist.max()));
        fields.insert(format!("{name}_samples"), json!(self.hist.len()));
        fields
    }
}
