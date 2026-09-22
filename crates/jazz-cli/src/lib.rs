//! Process-level shells for Jazz executables.
//!
//! Semantic database, protocol, and serving behavior lives in [`jazz`]. This
//! crate owns command dispatch, process signals, allocator selection, and the
//! executable entry points that assemble those library APIs.

pub mod commands;
