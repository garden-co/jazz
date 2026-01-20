pub mod filter;
pub mod index_scan;
pub mod limit_offset;
pub mod materialize;
pub mod output;
pub mod sort;
pub mod union;

use std::collections::HashSet;

use crate::object::ObjectId;

use super::types::{IdDelta, Row, RowDelta, RowDescriptor};

/// Unique identifier for a node in the query graph.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NodeId(pub u64);

/// Phase 1: ID-level nodes operate on IdDeltas (before materialization).
/// These are lightweight - they only track ObjectIds, not full row data.
pub trait IdNode {
    /// Process inputs and return the delta.
    fn process(&mut self) -> IdDelta;

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
