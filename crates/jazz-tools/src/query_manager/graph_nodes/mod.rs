pub mod alias;
pub mod array_subquery;
pub mod exists_output;
pub mod filter;
pub mod index_scan;
pub mod indexed_topk;
pub mod join;
pub mod limit_offset;
pub mod magic_columns;
pub mod materialize;
pub mod output;
mod policy_eval;
pub mod policy_filter;
pub mod project;
pub mod recursive_relation;
pub mod select_element;
pub mod sort;
pub mod subgraph;
pub mod tuple_delta;
pub mod union;

use crate::storage::Storage;

/// Unique identifier for a node in the query graph.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NodeId(pub u64);

/// Context for source nodes that need external data.
pub struct SourceContext<'a> {
    pub storage: &'a dyn Storage,
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
