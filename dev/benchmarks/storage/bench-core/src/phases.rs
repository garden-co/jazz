//! The phase catalog: what each benchmark phase *does*, written once and run
//! against any [`BenchEngine`]. A [`Phase`] is declared with its parameters in
//! [`crate::benchmarks`]; at runtime it expands to a deterministic operation
//! stream (`gen_args`) which [`replay`] executes against the engine.

use crate::engine::{BenchEngine, EngineError};
use crate::rng::SplitMix64;

/// Range scans walk a window of this many keys...
pub const RANGE_WINDOW_KEYS: u32 = 128;
/// ...returning at most this many rows.
pub const RANGE_RESULT_LIMIT: usize = 64;

/// The semantic family of a phase. Drives engine bracketing (write vs read),
/// cold-cache handling, and whether a too-fast read phase is repeated to reach
/// a measurable duration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PhaseKind {
    /// Bulk-insert every record.
    Load,
    /// Point-get every key in insertion order.
    GetSeq,
    /// Point-get at uniformly random keys.
    GetRandom,
    /// Point-get with a hot-key skew (most gets hit a small key subset).
    GetSkewed,
    /// Range scans from random start keys.
    RangeRandom,
    /// Overwrite values at random keys.
    UpdateRandom,
    /// A read/write/delete mix at random keys.
    Mixed,
    /// Reopen for a cold cache, then point-get at random keys.
    ColdGetRandom,
}

impl PhaseKind {
    /// Write phases (the b-tree checkpoints after these).
    pub fn is_write(self) -> bool {
        matches!(
            self,
            PhaseKind::Load | PhaseKind::UpdateRandom | PhaseKind::Mixed
        )
    }

    /// The cold phase reopens the store before running.
    pub fn is_cold(self) -> bool {
        matches!(self, PhaseKind::ColdGetRandom)
    }

    /// Read phases short enough to warrant repeat-until-measurable timing.
    /// Excludes the cold phase, whose reopen must not be repeated.
    pub fn is_retryable_read(self) -> bool {
        matches!(
            self,
            PhaseKind::GetSeq
                | PhaseKind::GetRandom
                | PhaseKind::GetSkewed
                | PhaseKind::RangeRandom
        )
    }
}

/// A benchmark phase plus its parameters. This is what you write when declaring
/// a benchmark in [`crate::benchmarks`].
#[derive(Debug, Clone)]
pub enum Phase {
    Load,
    GetSeq,
    GetRandom {
        count: u32,
    },
    GetSkewed {
        count: u32,
    },
    RangeRandom {
        count: u32,
    },
    UpdateRandom {
        count: u32,
    },
    Mixed {
        count: u32,
        get: u8,
        put: u8,
        del: u8,
    },
    ColdGetRandom {
        count: u32,
    },
}

impl Phase {
    pub fn kind(&self) -> PhaseKind {
        match self {
            Phase::Load => PhaseKind::Load,
            Phase::GetSeq => PhaseKind::GetSeq,
            Phase::GetRandom { .. } => PhaseKind::GetRandom,
            Phase::GetSkewed { .. } => PhaseKind::GetSkewed,
            Phase::RangeRandom { .. } => PhaseKind::RangeRandom,
            Phase::UpdateRandom { .. } => PhaseKind::UpdateRandom,
            Phase::Mixed { .. } => PhaseKind::Mixed,
            Phase::ColdGetRandom { .. } => PhaseKind::ColdGetRandom,
        }
    }

    /// The label shown in the results table (e.g. `mixed_70_20_10`).
    pub fn name(&self) -> String {
        match self {
            Phase::Load => "load".to_string(),
            Phase::GetSeq => "get_seq".to_string(),
            Phase::GetRandom { .. } => "get_random".to_string(),
            Phase::GetSkewed { .. } => "get_skewed".to_string(),
            Phase::RangeRandom { .. } => "range_random".to_string(),
            Phase::UpdateRandom { .. } => "update_random".to_string(),
            Phase::Mixed { get, put, del, .. } => format!("mixed_{get}_{put}_{del}"),
            Phase::ColdGetRandom { .. } => "cold_get_random".to_string(),
        }
    }

    /// Number of logical operations this phase executes for a dataset size.
    pub fn op_count(&self, record_count: u32) -> u32 {
        match self {
            Phase::Load | Phase::GetSeq => record_count,
            Phase::GetRandom { count }
            | Phase::GetSkewed { count }
            | Phase::RangeRandom { count }
            | Phase::UpdateRandom { count }
            | Phase::Mixed { count, .. }
            | Phase::ColdGetRandom { count } => *count,
        }
    }

    /// Expand this phase into its concrete operation stream for a dataset of
    /// `record_count` records. Deterministic given `rng`, so both engines
    /// generate the same stream.
    ///
    /// The encoding of each `u32` depends on the phase: a key index for the
    /// get/update/range phases, or a [`Mixed`](Phase::Mixed) packed op (2-bit
    /// op kind in the high bits, key index in the low 30). Phases with no
    /// per-op data (`Load`, `GetSeq`) return an empty stream.
    pub fn gen_args(&self, record_count: u32, rng: &mut SplitMix64) -> Vec<u32> {
        match self {
            Phase::Load | Phase::GetSeq => Vec::new(),
            Phase::GetRandom { count }
            | Phase::UpdateRandom { count }
            | Phase::ColdGetRandom { count } => {
                (0..*count).map(|_| rng.index(record_count)).collect()
            }
            Phase::RangeRandom { count } => (0..*count).map(|_| rng.index(record_count)).collect(),
            Phase::GetSkewed { count } => {
                // ~80% of gets hit the hottest ~20% of keys.
                let hot = (record_count / 5).max(1);
                (0..*count)
                    .map(|_| {
                        if rng.percent() < 80 {
                            rng.index(hot)
                        } else {
                            rng.index(record_count)
                        }
                    })
                    .collect()
            }
            Phase::Mixed {
                count,
                get,
                put,
                del: _,
            } => (0..*count)
                .map(|_| {
                    let roll = rng.percent();
                    let op: u32 = if roll < *get as u32 {
                        0 // get
                    } else if roll < (*get as u32 + *put as u32) {
                        1 // put
                    } else {
                        2 // delete
                    };
                    let idx = rng.index(record_count) & 0x3FFF_FFFF;
                    (op << 30) | idx
                })
                .collect(),
        }
    }
}

/// Execute one phase's operation stream against `engine`, returning
/// `(op_count, checksum)`. Phase bracketing (`begin_phase`/`end_phase`) and
/// cold reopen are handled by the [`runner`](crate::runner); this is purely the
/// operation loop, so the semantics of each phase read top-to-bottom here.
pub fn replay<E: BenchEngine>(
    engine: &mut E,
    phase: &Phase,
    args: &[u32],
    keys: &[&[u8]],
    vals: &[&[u8]],
) -> Result<(u32, u64), EngineError> {
    let n = keys.len() as u32;
    let idx = |raw: u32| (raw % n.max(1)) as usize;
    let mut ops: u32 = 0;
    let mut checksum: u64 = 0;
    let mut fold = |byte: Option<u8>| {
        if let Some(b) = byte {
            checksum = checksum.wrapping_add(b as u64);
        }
    };

    match phase.kind() {
        PhaseKind::Load => {
            for (k, v) in keys.iter().zip(vals.iter()) {
                engine.put(k, v)?;
                ops += 1;
            }
        }
        PhaseKind::GetSeq => {
            for k in keys {
                fold(engine.get(k)?);
                ops += 1;
            }
        }
        PhaseKind::GetRandom | PhaseKind::GetSkewed | PhaseKind::ColdGetRandom => {
            for &raw in args {
                fold(engine.get(keys[idx(raw)])?);
                ops += 1;
            }
        }
        PhaseKind::UpdateRandom => {
            for &raw in args {
                let i = idx(raw);
                engine.put(keys[i], vals[i])?;
                ops += 1;
            }
        }
        PhaseKind::RangeRandom => {
            for &raw in args {
                let s = idx(raw);
                let e = (s + RANGE_WINDOW_KEYS as usize).min(keys.len().saturating_sub(1));
                let rows = engine.range(keys[s], keys[e], RANGE_RESULT_LIMIT)?;
                checksum = checksum.wrapping_add(rows as u64);
                ops += 1;
            }
        }
        PhaseKind::Mixed => {
            for &packed in args {
                let op = packed >> 30;
                let i = idx(packed & 0x3FFF_FFFF);
                match op {
                    1 => engine.put(keys[i], vals[i])?,
                    2 => engine.delete(keys[i])?,
                    _ => fold(engine.get(keys[i])?),
                }
                ops += 1;
            }
        }
    }

    Ok((ops, checksum))
}
