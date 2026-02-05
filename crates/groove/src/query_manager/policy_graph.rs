//! PolicyGraph - one-shot graphs for policy evaluation.
//!
//! Creates minimal query graphs to evaluate policy conditions like USING and INHERITS.
//! These graphs are throwaway - created, settled until complete, then discarded.

use crate::commit::CommitId;
use crate::object::ObjectId;
use crate::object_manager::ObjectManager;

use crate::io_handler::IoHandler;

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
use super::types::{Schema, TableName, TupleDescriptor, Value};

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
        let table_schema = schema.get(table)?;
        let descriptor = table_schema.descriptor.clone();

        let mut graph = QueryGraph::new(*table, descriptor.clone());

        // IndexScan node: scan _id index for exact match
        let id_column = ColumnName::new("_id");
        let scan_node = IndexScanNode::new_with_branch(
            *table,
            id_column,
            branch,
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
        let policy_node = PolicyFilterNode::new_with_branch(
            descriptor.clone(),
            policy.clone(),
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
    ) -> Option<Self> {
        // INHERITS is essentially the same as a USING check on the parent table
        Self::for_using_check(
            parent_table,
            parent_id,
            parent_policy,
            session,
            schema,
            branch,
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
    ) -> Option<Self> {
        let table_schema = schema.get(table)?;
        let descriptor = table_schema.descriptor.clone();

        let mut graph = QueryGraph::new(*table, descriptor.clone());

        // IndexScan node: full table scan (check all rows)
        let id_column = ColumnName::new("_id");
        let scan_node = IndexScanNode::new_with_branch(
            *table,
            id_column,
            branch,
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

    /// Settle the graph. With synchronous IoHandler, always completes in one pass.
    ///
    /// The row_loader trait object is used to fetch row content by ObjectId.
    /// Using trait object instead of generic to avoid recursion limit when
    /// INHERITS evaluation calls this method.
    pub fn settle(
        &mut self,
        io: &dyn IoHandler,
        om: &ObjectManager,
        row_loader: &mut dyn FnMut(ObjectId) -> Option<(Vec<u8>, CommitId)>,
    ) -> bool {
        let _delta = self.graph.settle(io, om, row_loader);
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
    use crate::object_manager::ObjectManager;
    use crate::query_manager::types::{
        ColumnDescriptor, ColumnType, RowDescriptor, TablePolicies, TableSchema,
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

    #[test]
    fn test_for_using_check_creates_graph() {
        let schema = test_schema();
        let session = Session::new("user1");
        let object_id = ObjectId::new();
        let table = TableName::new("documents");

        let policy = PolicyExpr::eq_session("owner_id", vec!["user_id".into()]);

        let policy_graph =
            PolicyGraph::for_using_check(&table, object_id, &policy, &session, &schema, "main");

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

        let policy_graph =
            PolicyGraph::for_using_check(&table, object_id, &policy, &session, &schema, "main");

        assert!(policy_graph.is_none());
    }

    #[test]
    fn test_policy_graph_initial_state() {
        let schema = test_schema();
        let session = Session::new("user1");
        let object_id = ObjectId::new();
        let table = TableName::new("documents");

        let policy = PolicyExpr::True;

        let pg =
            PolicyGraph::for_using_check(&table, object_id, &policy, &session, &schema, "main")
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

        let mut pg =
            PolicyGraph::for_using_check(&table, object_id, &policy, &session, &schema, "main")
                .unwrap();

        // With no actual data in the io/om, the scan will return no rows
        let om = ObjectManager::new();
        let io = crate::io_handler::MemoryIoHandler::new();

        // Row loader returns None for all IDs (no data)
        let mut row_loader = |_id: ObjectId| -> Option<(Vec<u8>, CommitId)> { None };

        // Settle the graph
        pg.settle(&io, &om, &mut row_loader);

        // No rows found (object doesn't exist in empty OM), so result is false
        assert!(!pg.result());
    }
}
