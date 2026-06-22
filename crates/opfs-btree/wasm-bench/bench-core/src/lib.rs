//! Shared core of the opfs-btree browser benchmark.
//!
//! Both engine workers (the `opfs-btree` harness crate and the `wasm_sqlite`
//! sub-crate) depend on this crate so the *workload* is declared and the
//! *phase semantics* are written exactly once. Each engine only implements the
//! small [`BenchEngine`] contract; everything else — which benchmarks exist,
//! what each phase does, timing, the cross-engine checksum, and the
//! min-measurable retry — lives here.
//!
//! - [`benchmarks`] — declare the benchmarks (profiles + phase composition).
//! - [`phases`] — the [`PhaseKind`] catalog and the one generic `replay`.
//! - [`engine`] — the [`BenchEngine`] trait each store implements.
//! - [`runner`] — the generic run loop shared by both workers.
//! - [`dataset`] — the committed `.kv` record fixtures (real-world data).

pub mod benchmarks;
pub mod dataset;
pub mod engine;
pub mod phases;
pub mod result;
pub mod rng;
pub mod runner;

pub use benchmarks::{Benchmark, benchmark, benchmarks};
pub use dataset::{FormatError, KvDataset, ValueEncoding, decode_kv};
pub use engine::{BenchEngine, EngineError};
pub use phases::{Phase, PhaseKind};
pub use result::{DatasetPhaseResult, DatasetRunResult};
pub use runner::run;
