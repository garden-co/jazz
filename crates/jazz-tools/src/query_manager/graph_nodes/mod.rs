pub mod alias;
pub mod array_subquery;
pub mod exists_output;
pub mod filter;
pub mod index_scan;
pub mod join;
pub mod limit_offset;
pub mod magic_columns;
pub mod materialize;
pub mod output;
pub(crate) mod policy_eval;
pub mod policy_filter;
pub mod project;
pub mod recursive_relation;
pub mod select_element;
pub mod sort;
pub mod subgraph;
pub mod tuple_delta;
pub mod union;

use std::collections::HashMap;

use ahash::AHashSet;

use super::types::{RowDescriptor, Tuple, TupleDelta};
use crate::object::ObjectId;
use crate::storage::Storage;
use crate::sync_manager::RowBatchKey;

/// Unique identifier for a node in the query graph.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NodeId(pub u64);

/// Context for source nodes that need external data.
pub struct SourceContext<'a> {
    pub storage: &'a dyn Storage,
    pub local_overlay_rows: Option<&'a HashMap<ObjectId, RowBatchKey>>,
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
    fn current_tuples(&self) -> &AHashSet<Tuple>;

    /// Mark this node as dirty (needs reprocessing).
    fn mark_dirty(&mut self);

    /// Check if this node needs reprocessing.
    fn is_dirty(&self) -> bool;
}

/// Transform nodes that operate on tuple sets (before full materialization).
/// Used for UNION, JOIN, and other set operations on tuples.
pub trait TransformNode {
    /// Process inputs and return the tuple delta.
    fn process(&mut self, inputs: &[&AHashSet<Tuple>]) -> TupleDelta;

    /// Get current set of tuples in this node's output.
    fn current_tuples(&self) -> &AHashSet<Tuple>;

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
    fn current_tuples(&self) -> &AHashSet<Tuple>;

    /// Mark this node as dirty.
    fn mark_dirty(&mut self);

    /// Check if this node needs reprocessing.
    fn is_dirty(&self) -> bool;
}

pub use crate::query_manager::index::ScanCondition;
pub use alias::AliasNode;
pub use array_subquery::ArraySubqueryNode;
pub use exists_output::ExistsOutputNode;
pub use filter::FilterNode;
pub use index_scan::IndexScanNode;
pub use join::JoinNode;
pub use limit_offset::LimitOffsetNode;
pub use magic_columns::MagicColumnsNode;
pub use materialize::MaterializeNode;
pub use output::{OutputNode, QuerySubscriptionId};
pub use policy_filter::PolicyFilterNode;
pub use project::ProjectNode;
pub use recursive_relation::RecursiveRelationNode;
pub use select_element::SelectElementNode;
pub use sort::SortNode;
pub use subgraph::{SubgraphBuilder, SubgraphInstance, SubgraphTemplate};
pub use union::UnionNode;
