pub mod alias;
pub mod filter;
pub mod index_scan;
pub mod join;
pub mod limit_offset;
pub mod materialize;
pub mod output;
pub mod project;
pub mod sort;
pub mod union;

use std::collections::HashMap;
use std::collections::HashSet;

use crate::object_manager::ObjectManager;

use super::index::IndexState;
use super::types::{RowDescriptor, Tuple, TupleDelta};

/// Unique identifier for a node in the query graph.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NodeId(pub u64);

/// Context for source nodes that need external data.
pub struct SourceContext<'a> {
    pub indices: &'a HashMap<(String, String), IndexState>,
    pub om: &'a ObjectManager,
}

// ============================================================================
// Tuple-based Traits (unified model for single-table and JOIN queries)
// ============================================================================

/// Source nodes produce tuples from external state (no input nodes).
/// Returns TupleDelta with length-1 tuples for single-table scans.
pub trait SourceNode {
    /// Scan external state and return the tuple delta.
    fn scan(&mut self, ctx: &SourceContext) -> TupleDelta;

    /// Get current set of tuples in this node's output.
    fn current_tuples(&self) -> &HashSet<Tuple>;

    /// Mark this node as dirty (needs reprocessing).
    fn mark_dirty(&mut self);

    /// Check if this node needs reprocessing.
    fn is_dirty(&self) -> bool;
}

/// Transform nodes that operate on tuple sets (before full materialization).
/// Used for UNION, JOIN, and other set operations on tuples.
pub trait TransformNode {
    /// Process inputs and return the tuple delta.
    fn process(&mut self, inputs: &[&HashSet<Tuple>]) -> TupleDelta;

    /// Get current set of tuples in this node's output.
    fn current_tuples(&self) -> &HashSet<Tuple>;

    /// Mark this node as dirty (needs reprocessing).
    fn mark_dirty(&mut self);

    /// Check if this node needs reprocessing.
    fn is_dirty(&self) -> bool;
}

/// Row-level nodes that operate on TupleDeltas (after materialization).
/// These have full row data and can filter, sort, and project.
pub trait RowNode {
    /// Get the output row descriptor for this node.
    fn output_descriptor(&self) -> &RowDescriptor;

    /// Process input tuple delta and return output tuple delta.
    fn process(&mut self, input: TupleDelta) -> TupleDelta;

    /// Get current result set as tuples.
    fn current_tuples(&self) -> &HashSet<Tuple>;

    /// Mark this node as dirty.
    fn mark_dirty(&mut self);

    /// Check if this node needs reprocessing.
    fn is_dirty(&self) -> bool;
}

pub use alias::AliasNode;
pub use filter::FilterNode;
pub use index_scan::IndexScanNode;
pub use join::JoinNode;
pub use limit_offset::LimitOffsetNode;
pub use materialize::MaterializeNode;
pub use output::OutputNode;
pub use project::ProjectNode;
pub use sort::SortNode;
pub use union::UnionNode;
