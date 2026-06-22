//! The contract every benchmarked store implements. This is the *only*
//! engine-specific surface: ~6 primitive operations plus phase bracketing.
//! Everything else (which operations run, in what order, timing, checksums)
//! is engine-agnostic and lives in [`crate::phases`] and [`crate::runner`].

use crate::phases::PhaseKind;

/// An opaque, engine-reported failure. Workers convert this to whatever their
/// host boundary needs (e.g. a `JsValue`).
#[derive(Debug, Clone)]
pub struct EngineError(pub String);

impl EngineError {
    pub fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl core::fmt::Display for EngineError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for EngineError {}

/// A key/value store driven by the shared benchmark runner.
///
/// All operations are async: the b-tree now drives the *asynchronous* OPFS
/// File System Access APIs (`getFile()` / `createWritable()`), so every read
/// and write is a future. SQLite's operations are synchronous and complete
/// immediately when awaited.
#[allow(async_fn_in_trait)]
pub trait BenchEngine {
    /// Insert or overwrite `key`.
    async fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), EngineError>;

    /// Look up `key`, returning the first byte of its value if present. The
    /// runner folds this byte into the cross-engine checksum, so engines must
    /// agree on it for identical data.
    async fn get(&mut self, key: &[u8]) -> Result<Option<u8>, EngineError>;

    /// Delete `key` (a no-op if absent).
    async fn delete(&mut self, key: &[u8]) -> Result<(), EngineError>;

    /// Count the rows in `[lo, hi)`, up to `limit`.
    async fn range(&mut self, lo: &[u8], hi: &[u8], limit: usize) -> Result<usize, EngineError>;

    /// Open a phase. SQLite begins a transaction here; the b-tree does nothing.
    async fn begin_phase(&mut self, kind: PhaseKind) -> Result<(), EngineError>;

    /// Close a phase. SQLite commits; the b-tree checkpoints after write
    /// phases. Called inside the timed region, so durability cost is measured.
    async fn end_phase(&mut self, kind: PhaseKind) -> Result<(), EngineError>;

    /// Drop and reopen the store *without* wiping it, giving a cold cache while
    /// the data persists. Used by the cold-read phase.
    async fn reopen(&mut self) -> Result<(), EngineError>;
}
