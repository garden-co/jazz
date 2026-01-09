//! Incremental query graph system.
//!
//! This module provides a computation graph for evaluating SQL queries
//! incrementally. Instead of re-evaluating entire queries on every change,
//! deltas propagate through the graph, with early cutoff when a node's
//! output doesn't change.
//!
//! # Architecture
//!
//! ```text
//! IncrementalQuery (user handle)
//!         │
//!         ▼
//! GraphRegistry (manages graphs, routes changes)
//!         │
//!         ▼
//! QueryGraph (DAG of nodes)
//!         │
//!    ┌────┴────┐
//!    ▼         ▼
//! TableScan  Filter  ...  Output
//! ```
//!
//! # Example
//!
//! ```ignore
//! // Build a query graph
//! let mut builder = QueryGraphBuilder::new("users", schema);
//! let scan = builder.table_scan();
//! let filter = builder.filter(scan, Predicate::eq("active", Value::Bool(true)));
//! let graph = builder.output(filter, GraphId(1));
//!
//! // Process a change
//! let delta = graph.process_change(RowDelta::Added(row), &mut cache, &db);
//! ```

mod builder;
mod cache;
mod delta;
mod graph;
mod node;
mod predicate;
pub mod registry;

// Re-export main types
pub use builder::{JoinGraphBuilder, QueryGraphBuilder};
pub use cache::{BufferRowCache, RowCache};
pub use delta::{BufferJoinedRow, DeltaBatch, PriorState, RowDelta};
pub use graph::{GraphId, GraphState, QueryGraph};
pub use node::{AccessReason, NodeId, QueryNode};
pub use predicate::{Predicate, PredicateValue};

// Re-export from parent for internal use
use super::database::DatabaseState;
