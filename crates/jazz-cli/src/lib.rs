//! Process-level shells for Jazz executables.
//!
//! Semantic database, protocol, and serving behavior lives in [`jazz`]. This
//! crate owns command dispatch, process signals, allocator selection, and the
//! executable entry points that assemble those library APIs.

#[cfg(heap_profiling)]
pub mod heap_profiling;

/// `generate` and `migrations create` on an app's directory, shared with
/// the npm CLI through jazz-napi.
pub use jazz_codegen::project as app_files;
