//! **Declare the benchmarks here.**
//!
//! Each [`Benchmark`] names a profile, the committed `.kv` record fixture it
//! loads, a fixed RNG seed, and the ordered list of [`Phase`]s to run. The
//! phases expand to a deterministic operation stream at runtime (see
//! [`Phase::gen_args`]), so adding or tuning a benchmark is a pure source edit
//! — no fixture regeneration, and both engines stay in lock-step for the
//! checksum comparison.
//!
//! To add a phase: append `.phase(Phase::…)`. To add a profile: add a
//! `Benchmark::new(...)` and commit its `<profile>.kv` fixture.

use crate::phases::Phase;

/// One benchmark profile: which data to load and which phases to run over it.
#[derive(Debug, Clone)]
pub struct Benchmark {
    /// Profile name, matched against the worker request and shown in results.
    pub profile: String,
    /// `.kv` fixture filename under `public/data/`.
    pub kv_fixture: String,
    /// Fixed seed for this profile's operation streams (kept stable so runs are
    /// comparable over time).
    pub seed: u64,
    pub phases: Vec<Phase>,
}

impl Benchmark {
    pub fn new(profile: &str, kv_fixture: &str, seed: u64) -> Self {
        Self {
            profile: profile.to_string(),
            kv_fixture: kv_fixture.to_string(),
            seed,
            phases: Vec::new(),
        }
    }

    pub fn phase(mut self, phase: Phase) -> Self {
        self.phases.push(phase);
        self
    }
}

/// The full benchmark suite.
pub fn benchmarks() -> Vec<Benchmark> {
    vec![
        // Met museum-object metadata: ~10.7k medium structured records (~900 B).
        Benchmark::new("objects", "objects.kv", 0xB7E1_5163_2C4F_91A3)
            .phase(Phase::Load)
            .phase(Phase::GetSeq)
            .phase(Phase::GetRandom { count: 10_000 })
            .phase(Phase::GetSkewed { count: 10_000 })
            .phase(Phase::RangeRandom { count: 1_000 })
            .phase(Phase::UpdateRandom { count: 10_000 })
            .phase(Phase::Mixed {
                count: 10_000,
                get: 70,
                put: 20,
                del: 10,
            })
            .phase(Phase::ColdGetRandom { count: 5_000 }),
        // Real Wikipedia article wikitext: ~100 large text records (overflow path).
        Benchmark::new("wikipedia", "wikipedia.kv", 0x4F2A_8C19_D573_60BE)
            .phase(Phase::Load)
            .phase(Phase::GetSeq)
            .phase(Phase::GetRandom { count: 1_000 })
            .phase(Phase::GetSkewed { count: 1_000 })
            .phase(Phase::RangeRandom { count: 200 })
            .phase(Phase::UpdateRandom { count: 1_000 })
            .phase(Phase::Mixed {
                count: 1_000,
                get: 70,
                put: 20,
                del: 10,
            })
            .phase(Phase::ColdGetRandom { count: 500 }),
    ]
}

/// Look up a single benchmark by profile name.
pub fn benchmark(profile: &str) -> Option<Benchmark> {
    benchmarks().into_iter().find(|b| b.profile == profile)
}
