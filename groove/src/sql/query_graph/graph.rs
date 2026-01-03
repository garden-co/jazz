//! Query graph - the main computation DAG.

use std::collections::HashMap;

use crate::sql::query_graph::cache::RowCache;
use crate::sql::query_graph::delta::{DeltaBatch, RowDelta};
use crate::sql::query_graph::node::{NodeId, QueryNode};
use crate::sql::row::Row;
use crate::sql::schema::TableSchema;

use super::DatabaseState;

/// Unique identifier for a query graph.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct GraphId(pub u64);

/// Initialization state of a query graph.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GraphState {
    /// Not yet initialized - will load data on first access.
    Uninitialized,
    /// Currently loading initial data.
    Initializing,
    /// Ready for incremental updates.
    Ready,
}

/// A computation graph for incremental query evaluation.
///
/// The graph is a DAG of nodes in topological order (sources first,
/// output last). Deltas propagate through the nodes, with early
/// cutoff when a node's output doesn't change.
pub struct QueryGraph {
    /// Unique identifier.
    id: GraphId,

    /// Current state.
    state: GraphState,

    /// Table this graph queries.
    table: String,

    /// Schema for the table.
    schema: TableSchema,

    /// Nodes in topological order.
    nodes: Vec<QueryNode>,

    /// Map from NodeId to index in nodes vec.
    node_indices: HashMap<NodeId, usize>,

    /// The output node.
    output_node: NodeId,
}

impl std::fmt::Debug for QueryGraph {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryGraph")
            .field("id", &self.id)
            .field("state", &self.state)
            .field("table", &self.table)
            .field("nodes", &self.nodes.len())
            .finish()
    }
}

impl QueryGraph {
    /// Create a new query graph.
    ///
    /// This is typically called by `QueryGraphBuilder::output()`.
    pub(crate) fn new(
        id: GraphId,
        table: String,
        schema: TableSchema,
        nodes: Vec<QueryNode>,
        node_indices: HashMap<NodeId, usize>,
        output_node: NodeId,
    ) -> Self {
        Self {
            id,
            state: GraphState::Uninitialized,
            table,
            schema,
            nodes,
            node_indices,
            output_node,
        }
    }

    /// Get the graph ID.
    pub fn id(&self) -> GraphId {
        self.id
    }

    /// Set the graph ID (used when registering).
    pub(crate) fn set_id(&mut self, id: GraphId) {
        self.id = id;
    }

    /// Get the table this graph queries.
    pub fn table(&self) -> &str {
        &self.table
    }

    /// Get the schema.
    pub fn schema(&self) -> &TableSchema {
        &self.schema
    }

    /// Check if the graph is initialized.
    pub fn is_ready(&self) -> bool {
        self.state == GraphState::Ready
    }

    /// Get current output rows, initializing lazily if needed.
    pub fn get_output(&mut self, cache: &mut RowCache, db: &DatabaseState) -> Vec<Row> {
        self.ensure_initialized(cache, db);
        self.collect_output(cache)
    }

    /// Ensure the graph is initialized.
    fn ensure_initialized(&mut self, cache: &mut RowCache, db: &DatabaseState) {
        if self.state != GraphState::Uninitialized {
            return;
        }

        self.state = GraphState::Initializing;

        // Load all rows for this table
        let rows = db.read_all_rows(&self.table);

        // Populate cache and process through graph
        for row in rows {
            cache.insert(&self.table, row.clone());

            // Process as Added delta through all nodes
            let mut delta = DeltaBatch::added(row);
            for node in &mut self.nodes {
                if delta.is_empty() {
                    break;
                }
                delta = node.evaluate(delta, &self.schema, cache);
            }
        }

        self.state = GraphState::Ready;
    }

    /// Collect output rows from the final cached set.
    fn collect_output(&self, cache: &RowCache) -> Vec<Row> {
        // Find the node feeding into Output
        let output_idx = self.node_indices[&self.output_node];
        if let QueryNode::Output { input, .. } = &self.nodes[output_idx] {
            let input_idx = self.node_indices[input];
            if let Some(ids) = self.nodes[input_idx].cached_ids() {
                return ids
                    .iter()
                    .filter_map(|id| cache.get(&self.table, *id).flatten().cloned())
                    .collect();
            }
        }
        vec![]
    }

    /// Process a row change, returning the output delta.
    ///
    /// Initializes lazily if needed.
    pub fn process_change(
        &mut self,
        delta: RowDelta,
        cache: &mut RowCache,
        db: &DatabaseState,
    ) -> DeltaBatch {
        self.ensure_initialized(cache, db);

        // Update cache with new value
        match &delta {
            RowDelta::Added(row) => cache.insert(&self.table, row.clone()),
            RowDelta::Removed { id, .. } => cache.mark_deleted(&self.table, *id),
            RowDelta::Updated { new, .. } => cache.insert(&self.table, new.clone()),
        }

        // Propagate through nodes
        let mut current = DeltaBatch::new();
        current.push(delta);

        for node in &mut self.nodes {
            if current.is_empty() {
                break; // Early cutoff
            }
            current = node.evaluate(current, &self.schema, cache);
        }

        current
    }

    /// Get the number of nodes in the graph.
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// Get the current size of the output set.
    pub fn output_size(&self) -> usize {
        let output_idx = self.node_indices[&self.output_node];
        if let QueryNode::Output { input, .. } = &self.nodes[output_idx] {
            let input_idx = self.node_indices[input];
            self.nodes[input_idx]
                .cached_ids()
                .map(|ids| ids.len())
                .unwrap_or(0)
        } else {
            0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::query_graph::builder::QueryGraphBuilder;
    use crate::sql::query_graph::predicate::Predicate;
    use crate::sql::row::Value;
    use crate::sql::schema::{ColumnDef, ColumnType};
    use crate::sql::{Database, ObjectId};

    fn test_schema() -> TableSchema {
        TableSchema::new(
            "users",
            vec![
                ColumnDef::required("name", ColumnType::String),
                ColumnDef::required("active", ColumnType::Bool),
            ],
        )
    }

    fn make_row(id: u128, name: &str, active: bool) -> Row {
        Row::new(
            ObjectId::new(id),
            vec![Value::String(name.to_string()), Value::Bool(active)],
        )
    }

    #[test]
    fn graph_lazy_init() {
        let schema = test_schema();
        let mut builder = QueryGraphBuilder::new("users", schema);
        let scan = builder.table_scan();
        let graph = builder.output(scan, GraphId(1));

        assert_eq!(graph.state, GraphState::Uninitialized);
    }

    #[test]
    fn graph_process_change() {
        let schema = test_schema();
        let mut builder = QueryGraphBuilder::new("users", schema.clone());
        let scan = builder.table_scan();
        let filter = builder.filter(scan, Predicate::eq("active", Value::Bool(true)));
        let mut graph = builder.output(filter, GraphId(1));

        // Create a mock database state
        let db = Database::in_memory();
        db.create_table(schema).unwrap();

        let mut cache = RowCache::new();

        // Process an active user - should appear in output
        let row1 = make_row(1, "Alice", true);
        let delta = graph.process_change(RowDelta::Added(row1.clone()), &mut cache, &db.state());

        assert_eq!(delta.len(), 1);
        assert!(matches!(delta.iter().next(), Some(RowDelta::Added(_))));

        // Process an inactive user - should be filtered out
        let row2 = make_row(2, "Bob", false);
        let delta = graph.process_change(RowDelta::Added(row2), &mut cache, &db.state());

        // Early cutoff - no output
        assert!(delta.is_empty());

        // Check output size
        assert_eq!(graph.output_size(), 1);
    }

    #[test]
    fn graph_collect_output() {
        let schema = test_schema();
        let mut builder = QueryGraphBuilder::new("users", schema.clone());
        let scan = builder.table_scan();
        let filter = builder.filter(scan, Predicate::eq("active", Value::Bool(true)));
        let mut graph = builder.output(filter, GraphId(1));

        let db = Database::in_memory();
        db.create_table(schema).unwrap();

        let mut cache = RowCache::new();

        // Add some rows
        let row1 = make_row(1, "Alice", true);
        let row2 = make_row(2, "Bob", false);
        let row3 = make_row(3, "Carol", true);

        graph.process_change(RowDelta::Added(row1), &mut cache, &db.state());
        graph.process_change(RowDelta::Added(row2), &mut cache, &db.state());
        graph.process_change(RowDelta::Added(row3), &mut cache, &db.state());

        // Get output - should only have active users
        let output = graph.get_output(&mut cache, &db.state());

        assert_eq!(output.len(), 2);
        let ids: Vec<_> = output.iter().map(|r| r.id.0).collect();
        assert!(ids.contains(&1));
        assert!(ids.contains(&3));
        assert!(!ids.contains(&2));
    }
}
