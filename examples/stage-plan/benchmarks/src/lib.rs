//! StagePlan: a crew's stage-prep task board. Two workloads measure it:
//!
//! - [`tasks`]: one show's task list, synced between a RocksDB worker and an
//!   in-memory foreground (adding, checking off, bulk-completing, reopening).
//! - [`board`]: a permissioned multi-show board with task detail, discussion,
//!   activity, a crew dashboard of many live lists, and offline resume.
//!
//! Both are self-contained models of the app's data paths; neither imports the
//! app runtime.

pub mod board;
pub mod tasks;
