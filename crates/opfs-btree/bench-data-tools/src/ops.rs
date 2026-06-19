use opfs_btree::bench_dataset::{Phase, PhaseKind};

/// Splitmix64 — deterministic, dependency-free PRNG so op-scripts are stable.
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn below(&mut self, n: u32) -> u32 {
        if n == 0 {
            0
        } else {
            (self.next() % n as u64) as u32
        }
    }
    /// Zipf-ish skew: square a uniform sample so low indices dominate.
    fn skewed(&mut self, n: u32) -> u32 {
        if n == 0 {
            return 0;
        }
        let u = (self.next() as f64) / (u64::MAX as f64);
        ((u * u) * n as f64) as u32 % n
    }
}

const MIXED_GET: u32 = 0;
const MIXED_PUT: u32 = 1 << 30;
const MIXED_DEL: u32 = 2 << 30;

/// Build the standard op-script for a dataset of `count` records.
pub fn build_phases(count: u32, op_budget: u32, seed: u64) -> Vec<Phase> {
    let mut rng = Rng(seed);
    let mut phases = vec![
        Phase {
            name: "load".into(),
            kind: PhaseKind::LoadAll,
            args: vec![],
        },
        Phase {
            name: "get_seq".into(),
            kind: PhaseKind::GetSeq,
            args: vec![],
        },
    ];

    let random_gets: Vec<u32> = (0..op_budget).map(|_| rng.below(count)).collect();
    phases.push(Phase {
        name: "get_random".into(),
        kind: PhaseKind::GetIndices,
        args: random_gets,
    });

    let skewed_gets: Vec<u32> = (0..op_budget).map(|_| rng.skewed(count)).collect();
    phases.push(Phase {
        name: "get_skewed".into(),
        kind: PhaseKind::GetIndices,
        args: skewed_gets,
    });

    let max_start = count.saturating_sub(129).max(1);
    let range_starts: Vec<u32> = (0..op_budget).map(|_| rng.below(max_start)).collect();
    phases.push(Phase {
        name: "range_random".into(),
        kind: PhaseKind::RangeStarts,
        args: range_starts,
    });

    let updates: Vec<u32> = (0..op_budget).map(|_| rng.below(count)).collect();
    phases.push(Phase {
        name: "update_random".into(),
        kind: PhaseKind::UpdateIndices,
        args: updates,
    });

    let mixed: Vec<u32> = (0..op_budget)
        .map(|_| {
            let idx = rng.below(count);
            let roll = rng.below(100);
            let op = if roll < 70 {
                MIXED_GET
            } else if roll < 90 {
                MIXED_PUT
            } else {
                MIXED_DEL
            };
            op | idx
        })
        .collect();
    phases.push(Phase {
        name: "mixed_70_20_10".into(),
        kind: PhaseKind::Mixed,
        args: mixed,
    });

    let cold: Vec<u32> = (0..op_budget).map(|_| rng.below(count)).collect();
    phases.push(Phase {
        name: "cold_get_random".into(),
        kind: PhaseKind::ColdGetIndices,
        args: cold,
    });

    phases
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn op_script_is_deterministic_for_seed() {
        let a = build_phases(1000, 500, 42);
        let b = build_phases(1000, 500, 42);
        assert_eq!(a, b);
    }

    #[test]
    fn indices_stay_in_range() {
        for p in build_phases(100, 1000, 7) {
            for &arg in &p.args {
                let idx = arg & 0x3FFF_FFFF;
                assert!(idx < 100, "index {idx} out of range in phase {}", p.name);
            }
        }
    }
}
