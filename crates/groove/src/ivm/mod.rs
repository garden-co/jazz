//! Incremental-view-maintenance planning and execution.
//!
//! This module is the IVM engine below [`crate::db`]. [`planner`] lowers the
//! query AST into graph builders, [`graph`] owns the hash-consed DAG and node
//! identities, [`op_types`] defines operator descriptors, and [`runtime`] owns
//! ticks, subscriptions, arrangements, and operator evaluation. It does not own
//! row encoding or durable storage APIs; those live in [`crate::records`] and
//! [`crate::storage`].

pub mod graph;
pub mod op_types;
pub mod planner;
pub mod runtime;

pub use graph::*;
pub use op_types::*;
pub use planner::*;
pub use runtime::*;
