pub mod filter;
pub mod index_scan;
pub mod limit_offset;
pub mod materialize;
pub mod output;
pub mod sort;
pub mod union;

use std::collections::HashMap;
use std::collections::HashSet;

use crate::object::ObjectId;
use crate::object_manager::ObjectManager;

use super::index::IndexState;
use super::types::{IdDelta, Row, RowDelta, RowDescriptor};

/// Unique identifier for a node in the query graph.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NodeId(pub u64);

/// Context for source nodes that need external data.
pub struct SourceContext<'a> {
    pub indices: &'a HashMap<(String, String), IndexState>,
    pub om: &'a ObjectManager,
}

/// Source nodes produce data from external state (no input nodes).
pub trait SourceNode {
    /// Scan external state and return the delta.
    fn scan(&mut self, ctx: &SourceContext) -> IdDelta;

    /// Get current set of IDs in this node's output.
    fn current_ids(&self) -> &HashSet<ObjectId>;

    /// Mark this node as dirty (needs reprocessing).
    fn mark_dirty(&mut self);

    /// Check if this node needs reprocessing.
    fn is_dirty(&self) -> bool;
}

/// Phase 1: ID-level transform nodes operate on IdDeltas (before materialization).
/// These are pure dataflow transforms that combine/filter id sets from their inputs.
pub trait IdNode {
    /// Process inputs and return the delta.
    fn process(&mut self, inputs: &[&HashSet<ObjectId>]) -> IdDelta;

    /// Get current set of IDs in this node's output.
    fn current_ids(&self) -> &HashSet<ObjectId>;

    /// Mark this node as dirty (needs reprocessing).
    fn mark_dirty(&mut self);

    /// Check if this node needs reprocessing.
    fn is_dirty(&self) -> bool;
}

/// Phase 2: Row-level nodes operate on RowDeltas (after materialization).
/// These have full row data and can filter, sort, and project.
pub trait RowNode {
    /// Get the output row descriptor for this node.
    fn output_descriptor(&self) -> &RowDescriptor;

    /// Process input delta and return output delta.
    fn process(&mut self, input: RowDelta) -> RowDelta;

    /// Get current result set.
    fn current_result(&self) -> &[Row];

    /// Mark this node as dirty.
    fn mark_dirty(&mut self);

    /// Check if this node needs reprocessing.
    fn is_dirty(&self) -> bool;
}

pub use filter::FilterNode;
pub use index_scan::IndexScanNode;
pub use limit_offset::LimitOffsetNode;
pub use materialize::MaterializeNode;
pub use output::OutputNode;
pub use sort::SortNode;
pub use union::UnionNode;
