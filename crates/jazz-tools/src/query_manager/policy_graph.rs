//! PolicyGraph - one-shot graphs for policy evaluation.
//!
//! Creates minimal query graphs to evaluate policy conditions like USING and INHERITS.
//! These graphs are throwaway - created, settled until complete, then discarded.

use crate::object::{BranchName, ObjectId};

use crate::storage::Storage;

use crate::schema_manager::SchemaContext;

use super::graph::{GraphNode, QueryGraph};
use super::graph_nodes::NodeId;
use super::graph_nodes::exists_output::ExistsOutputNode;
use super::graph_nodes::index_scan::IndexScanNode;
use super::graph_nodes::materialize::MaterializeNode;
use super::graph_nodes::policy_filter::PolicyFilterNode;
use super::index::ScanCondition;
use super::policy::PolicyExpr;
use super::session::Session;
use super::types::ColumnName;
use super::types::{
    LoadedRow, QueryBranchRef, Schema, TableName, TupleDescriptor, TupleProvenance, Value,
};

/// A one-shot graph for evaluating a policy condition.
///
/// Policy graphs are minimal graphs built specifically to evaluate
/// whether a condition is met (EXISTS-style check).
#[derive(Debug)]
pub struct PolicyGraph {
    /// The underlying query graph.
    graph: QueryGraph,
    /// The ExistsOutput node ID.
    exists_node: NodeId,
    /// Table name this graph operates on.
    table: TableName,
}

impl PolicyGraph {
    /// Create a graph for USING check: can session see this specific row?
    ///
    /// Graph structure: IndexScan(_id = objectId) → Materialize → PolicyFilter → ExistsOutput
    ///
    /// Returns None if the table is not in the schema.
    pub fn for_using_check(
        table: &TableName,
        object_id: ObjectId,
        policy: &PolicyExpr,
        session: &Session,
        schema: &Schema,
        branch: &str,
    ) -> Option<Self> {
        Self::for_using_check_with_depth(table, object_id, policy, session, schema, branch, 0)
    }

    /// Create a graph for USING check with an explicit initial recursion depth.
    pub fn for_using_check_with_depth(
        table: &TableName,
        object_id: ObjectId,
        policy: &PolicyExpr,
        session: &Session,
        schema: &Schema,
        branch: &str,
        initial_depth: usize,
    ) -> Option<Self> {
        let table_schema = schema.get(table)?;
        let descriptor = table_schema.columns.clone();

        let mut graph = QueryGraph::new(*table, descriptor.clone());

        // IndexScan node: scan _id index for exact match
        let id_column = ColumnName::new("_id");
        let branch_ref = QueryBranchRef::from_branch_name(branch.to_string());
        let scan_node = IndexScanNode::new_with_branch(
            *table,
            id_column,
            branch_ref,
            ScanCondition::Eq(Value::Uuid(object_id)),
            descriptor.clone(),
        );
        let scan_id = graph.add_node_with_id(GraphNode::IndexScan(scan_node));
        graph.index_scan_nodes.push((scan_id, *table, id_column));

        // Materialize node: load row content
        let tuple_desc = TupleDescriptor::single("", descriptor.clone());
        let mat_node = MaterializeNode::new_all(tuple_desc);
        let mat_id = graph.add_node_with_id(GraphNode::Materialize(mat_node));
        graph.add_edge(mat_id, scan_id);

        // PolicyFilter node: evaluate policy against row
        let policy_node = PolicyFilterNode::new_with_branch_and_depth(
            descriptor.clone(),
            policy.clone(),
            session.clone(),
            schema.clone(),
            table.as_str(),
            branch,
            initial_depth,
        );
        let policy_id = graph.add_node_with_id(GraphNode::PolicyFilter(policy_node));
        graph.add_edge(policy_id, mat_id);

        // ExistsOutput node: track whether any rows pass
        let exists_node = ExistsOutputNode::new(descriptor);
        let exists_id = graph.add_node_with_id(GraphNode::ExistsOutput(exists_node));
        graph.add_edge(exists_id, policy_id);

        graph.output_node = exists_id;

        Some(Self {
            graph,
            exists_node: exists_id,
            table: *table,
        })
    }

    /// Create a graph for INHERITS: does parent row pass parent's policy?
    ///
    /// Graph structure: IndexScan(parent_table, _id = parent_id) → Materialize → PolicyFilter → ExistsOutput
    ///
    /// Returns None if the parent table is not in the schema.
    pub fn for_inherits(
        parent_table: &TableName,
        parent_id: ObjectId,
        parent_policy: &PolicyExpr,
        session: &Session,
        schema: &Schema,
        branch: &str,
        initial_depth: usize,
    ) -> Option<Self> {
        // INHERITS is essentially the same as a USING check on the parent table
        Self::for_using_check_with_depth(
            parent_table,
            parent_id,
            parent_policy,
            session,
            schema,
            branch,
            initial_depth,
        )
    }

    /// Create a graph for EXISTS: does any row in table match condition?
    ///
    /// Graph structure: IndexScan(All) → Materialize → PolicyFilter → ExistsOutput
    ///
    /// Returns None if the table is not in the schema.
    pub fn for_exists(
        table: &TableName,
        condition: &PolicyExpr,
        session: &Session,
        schema: &Schema,
        branch: &str,
        io: &dyn Storage,
    ) -> Option<Self> {
        let table_schema = schema.get(table)?;
        let descriptor = table_schema.columns.clone();

        let mut graph = QueryGraph::new(*table, descriptor.clone());

        // IndexScan node: full table scan (check all rows)
        let id_column = ColumnName::new("_id");
        let seed_branches = vec![QueryBranchRef::from_branch_name(branch.to_string())];
        let branches = QueryGraph::resolve_active_branches_for_table(io, *table, &seed_branches)?;
        let scan_node = IndexScanNode::new_with_branches(
            *table,
            id_column,
            branches,
            ScanCondition::All,
            descriptor.clone(),
        );
        let scan_id = graph.add_node_with_id(GraphNode::IndexScan(scan_node));
        graph.index_scan_nodes.push((scan_id, *table, id_column));

        // Materialize node: load row content
        let tuple_desc = TupleDescriptor::single("", descriptor.clone());
        let mat_node = MaterializeNode::new_all(tuple_desc);
        let mat_id = graph.add_node_with_id(GraphNode::Materialize(mat_node));
        graph.add_edge(mat_id, scan_id);

        // PolicyFilter node: evaluate condition against each row
        let policy_node = PolicyFilterNode::new_with_branch(
            descriptor.clone(),
            condition.clone(),
            session.clone(),
            schema.clone(),
            table.as_str(),
            branch,
        );
        let policy_id = graph.add_node_with_id(GraphNode::PolicyFilter(policy_node));
        graph.add_edge(policy_id, mat_id);

        // ExistsOutput node: track whether any rows pass
        let exists_node = ExistsOutputNode::new(descriptor);
        let exists_id = graph.add_node_with_id(GraphNode::ExistsOutput(exists_node));
        graph.add_edge(exists_id, policy_id);

        graph.output_node = exists_id;

        Some(Self {
            graph,
            exists_node: exists_id,
            table: *table,
        })
    }

    /// Create a graph for declarative EXISTS relation checks.
    ///
    /// Compiles relation IR through the shared query planner, then appends an
    /// ExistsOutput node over the compiled query output.
    pub fn for_exists_rel(
        rel: &crate::query_manager::relation_ir::RelExpr,
        schema: &Schema,
        branch: &str,
        io: &dyn Storage,
    ) -> Option<Self> {
        let branches = vec![QueryBranchRef::from_branch_name(branch.to_string())];
        let schema_context = match crate::query_manager::types::ComposedBranchName::parse(
            &BranchName::new(branch.to_string()),
        ) {
            Some(composed) => SchemaContext::new_with_batch_id(
                schema.clone(),
                &composed.env,
                &composed.user_branch,
                composed.batch_id,
            ),
            None => SchemaContext::with_defaults(schema.clone(), "main"),
        };
        let mut graph =
            QueryGraph::compile_relation_ir_with_branch_refs_and_schema_context_using_storage(
                rel,
                schema,
                &branches,
                None,
                &schema_context,
                Some(io),
            )?;
        let output_descriptor = match graph
            .nodes
            .get(graph.output_node.0 as usize)
            .map(|c| &c.node)
        {
            Some(GraphNode::Output(node)) => node.output_tuple_descriptor().combined_descriptor(),
            _ => return None,
        };

        let exists_node = ExistsOutputNode::new(output_descriptor);
        let exists_id = graph.add_node_with_id(GraphNode::ExistsOutput(exists_node));
        graph.add_edge(exists_id, graph.output_node);
        graph.output_node = exists_id;

        Some(Self {
            table: graph.table,
            graph,
            exists_node: exists_id,
        })
    }

    /// Settle the graph. With synchronous Storage, always completes in one pass.
    ///
    /// The row_loader trait object is used to fetch row content by ObjectId.
    /// Using trait object instead of generic to avoid recursion limit when
    /// INHERITS evaluation calls this method.
    pub fn settle(
        &mut self,
        io: &dyn Storage,
        row_loader: &mut dyn FnMut(ObjectId, Option<&TupleProvenance>) -> Option<LoadedRow>,
    ) -> bool {
        let _delta = self.graph.settle(io, row_loader);
        true
    }

    /// Get result.
    ///
    /// Returns true if at least one row passed the policy check.
    pub fn result(&self) -> bool {
        match self
            .graph
            .nodes
            .get(self.exists_node.0 as usize)
            .map(|c| &c.node)
        {
            Some(GraphNode::ExistsOutput(node)) => node.exists(),
            _ => false,
        }
    }

    /// Get the table this graph operates on.
    pub fn table(&self) -> &TableName {
        &self.table
    }

    /// Mark all scan nodes dirty (for re-evaluation after data changes).
    pub fn mark_dirty(&mut self) {
        let scan_ids: Vec<NodeId> = self
            .graph
            .index_scan_nodes
            .iter()
            .map(|(node_id, _, _)| *node_id)
            .collect();
        for node_id in scan_ids {
            self.graph.mark_dirty(node_id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_manager::relation_ir::{
        ColumnRef, JoinCondition, JoinKind, KeyRef, PredicateCmpOp, PredicateExpr, ProjectColumn,
        ProjectExpr, RelExpr, RowIdRef, ValueRef,
    };
    use crate::query_manager::types::{
        BatchId, ColumnDescriptor, ColumnType, ComposedBranchName, RowDescriptor, SchemaHash,
        TablePolicies, TableSchema,
    };

    fn test_schema() -> Schema {
        let mut schema = Schema::new();

        // documents table with owner_id policy
        let docs_descriptor = RowDescriptor::new(vec![
            ColumnDescriptor::new("_id", ColumnType::Uuid),
            ColumnDescriptor::new("owner_id", ColumnType::Text),
            ColumnDescriptor::new("title", ColumnType::Text),
        ]);

        let docs_policies = TablePolicies::new()
            .with_select(PolicyExpr::eq_session("owner_id", vec!["user_id".into()]));

        schema.insert(
            TableName::new("documents"),
            TableSchema::with_policies(docs_descriptor, docs_policies),
        );

        schema
    }

    fn test_branch() -> String {
        ComposedBranchName::new(
            "dev",
            SchemaHash::from_bytes([7; 32]),
            "main",
            BatchId::from_uuid(uuid::Uuid::from_u128(1)),
        )
        .to_branch_name()
        .as_str()
        .to_string()
    }

    #[test]
    fn test_for_using_check_creates_graph() {
        let schema = test_schema();
        let session = Session::new("user1");
        let object_id = ObjectId::new();
        let table = TableName::new("documents");

        let policy = PolicyExpr::eq_session("owner_id", vec!["user_id".into()]);

        let branch = test_branch();
        let policy_graph = PolicyGraph::for_using_check(
            &table,
            object_id,
            &policy,
            &session,
            &schema,
            branch.as_str(),
        );

        assert!(policy_graph.is_some());

        let pg = policy_graph.unwrap();
        // Graph should have 4 nodes: IndexScan, Materialize, PolicyFilter, ExistsOutput
        assert_eq!(pg.graph.nodes.len(), 4);
    }

    #[test]
    fn test_for_using_check_returns_none_for_missing_table() {
        let schema = test_schema();
        let session = Session::new("user1");
        let object_id = ObjectId::new();
        let table = TableName::new("nonexistent");

        let policy = PolicyExpr::True;

        let branch = test_branch();
        let policy_graph = PolicyGraph::for_using_check(
            &table,
            object_id,
            &policy,
            &session,
            &schema,
            branch.as_str(),
        );

        assert!(policy_graph.is_none());
    }

    #[test]
    fn test_policy_graph_initial_state() {
        let schema = test_schema();
        let session = Session::new("user1");
        let object_id = ObjectId::new();
        let table = TableName::new("documents");

        let policy = PolicyExpr::True;

        let branch = test_branch();
        let pg = PolicyGraph::for_using_check(
            &table,
            object_id,
            &policy,
            &session,
            &schema,
            branch.as_str(),
        )
        .unwrap();

        // Before settling, result should be false (no rows yet)
        // But it might be pending since we haven't settled
        assert!(!pg.result());
    }

    #[test]
    fn test_policy_graph_with_true_policy() {
        let schema = test_schema();
        let session = Session::new("user1");
        let object_id = ObjectId::new();
        let table = TableName::new("documents");

        // PolicyExpr::True should always pass
        let policy = PolicyExpr::True;

        let branch = test_branch();
        let mut pg = PolicyGraph::for_using_check(
            &table,
            object_id,
            &policy,
            &session,
            &schema,
            branch.as_str(),
        )
        .unwrap();

        // With no actual data in storage, the scan will return no rows
        let storage = crate::storage::MemoryStorage::new();

        // Row loader returns None for all IDs (no data)
        let mut row_loader =
            |_id: ObjectId, _provenance: Option<&TupleProvenance>| -> Option<LoadedRow> { None };

        // Settle the graph
        pg.settle(&storage, &mut row_loader);

        // No rows found (object doesn't exist in empty OM), so result is false
        assert!(!pg.result());
    }

    #[test]
    fn test_for_exists_rel_creates_graph() {
        let schema = test_schema();
        let rel = RelExpr::Filter {
            input: Box::new(RelExpr::TableScan {
                table: TableName::new("documents"),
            }),
            predicate: PredicateExpr::Cmp {
                left: ColumnRef::unscoped("owner_id"),
                op: PredicateCmpOp::Eq,
                right: ValueRef::Literal(Value::Text("user1".to_string())),
            },
        };

        let storage = crate::storage::MemoryStorage::new();
        let branch = test_branch();
        let graph = PolicyGraph::for_exists_rel(&rel, &schema, branch.as_str(), &storage);
        assert!(graph.is_some(), "exists-rel graph should compile");

        let graph = graph.expect("graph");
        assert_eq!(graph.table().as_str(), "documents");
        assert!(matches!(
            graph
                .graph
                .nodes
                .get(graph.exists_node.0 as usize)
                .map(|c| &c.node),
            Some(GraphNode::ExistsOutput(_))
        ));
    }

    #[test]
    fn test_for_exists_rel_with_gather_post_join_compiles() {
        let mut schema = Schema::new();
        schema.insert(
            TableName::new("teams"),
            TableSchema::new(RowDescriptor::new(vec![
                ColumnDescriptor::new("_id", ColumnType::Uuid),
                ColumnDescriptor::new("name", ColumnType::Text),
            ])),
        );
        schema.insert(
            TableName::new("team_edges"),
            TableSchema::new(RowDescriptor::new(vec![
                ColumnDescriptor::new("_id", ColumnType::Uuid),
                ColumnDescriptor::new("child_team", ColumnType::Uuid),
                ColumnDescriptor::new("parent_team", ColumnType::Uuid),
            ])),
        );
        schema.insert(
            TableName::new("resource_access_edges"),
            TableSchema::new(RowDescriptor::new(vec![
                ColumnDescriptor::new("_id", ColumnType::Uuid),
                ColumnDescriptor::new("team", ColumnType::Uuid),
                ColumnDescriptor::new("resource", ColumnType::Text),
                ColumnDescriptor::new("grant_role", ColumnType::Text),
            ])),
        );

        let rel = RelExpr::Project {
            input: Box::new(RelExpr::Filter {
                input: Box::new(RelExpr::Join {
                    left: Box::new(RelExpr::Gather {
                        seed: Box::new(RelExpr::Filter {
                            input: Box::new(RelExpr::TableScan {
                                table: TableName::new("teams"),
                            }),
                            predicate: PredicateExpr::Cmp {
                                left: ColumnRef::scoped("teams", "name"),
                                op: PredicateCmpOp::Eq,
                                right: ValueRef::Literal(Value::Text("seed".to_string())),
                            },
                        }),
                        step: Box::new(RelExpr::Project {
                            input: Box::new(RelExpr::Join {
                                left: Box::new(RelExpr::Filter {
                                    input: Box::new(RelExpr::TableScan {
                                        table: TableName::new("team_edges"),
                                    }),
                                    predicate: PredicateExpr::Cmp {
                                        left: ColumnRef::scoped("team_edges", "child_team"),
                                        op: PredicateCmpOp::Eq,
                                        right: ValueRef::RowId(RowIdRef::Frontier),
                                    },
                                }),
                                right: Box::new(RelExpr::TableScan {
                                    table: TableName::new("teams"),
                                }),
                                on: vec![JoinCondition {
                                    left: ColumnRef::scoped("team_edges", "parent_team"),
                                    right: ColumnRef::scoped("__recursive_hop_0", "id"),
                                }],
                                join_kind: JoinKind::Inner,
                            }),
                            columns: vec![ProjectColumn {
                                alias: "id".to_string(),
                                expr: ProjectExpr::Column(ColumnRef::scoped(
                                    "__recursive_hop_0",
                                    "id",
                                )),
                            }],
                        }),
                        frontier_key: KeyRef::RowId(RowIdRef::Current),
                        max_depth: 5,
                        dedupe_key: vec![KeyRef::RowId(RowIdRef::Current)],
                    }),
                    right: Box::new(RelExpr::TableScan {
                        table: TableName::new("resource_access_edges"),
                    }),
                    on: vec![JoinCondition {
                        left: ColumnRef::scoped("teams", "id"),
                        right: ColumnRef::scoped("__hop_0", "team"),
                    }],
                    join_kind: JoinKind::Inner,
                }),
                predicate: PredicateExpr::Cmp {
                    left: ColumnRef::scoped("__hop_0", "grant_role"),
                    op: PredicateCmpOp::Eq,
                    right: ValueRef::Literal(Value::Text("viewer".to_string())),
                },
            }),
            columns: vec![ProjectColumn {
                alias: "id".to_string(),
                expr: ProjectExpr::Column(ColumnRef::scoped("__hop_0", "id")),
            }],
        };

        let storage = crate::storage::MemoryStorage::new();
        let branch = test_branch();
        let graph = PolicyGraph::for_exists_rel(&rel, &schema, branch.as_str(), &storage);
        assert!(
            graph.is_some(),
            "gather + post-join exists-rel should compile"
        );

        let graph = graph.expect("graph");
        assert!(
            graph
                .graph
                .nodes
                .iter()
                .any(|ctx| matches!(ctx.node, GraphNode::RecursiveRelation(_)))
        );
        assert!(
            graph
                .graph
                .nodes
                .iter()
                .any(|ctx| matches!(ctx.node, GraphNode::Join(_)))
        );
    }
}
