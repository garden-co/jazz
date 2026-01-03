//! Query graph - the main computation DAG.

use std::collections::HashMap;

use crate::sql::query_graph::cache::RowCache;
use crate::sql::query_graph::delta::{DeltaBatch, RowDelta};
use crate::sql::query_graph::node::{NodeId, QueryNode};
use crate::sql::row::Row;
use crate::sql::schema::TableSchema;
use crate::object::ObjectId;

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

    /// Primary table this graph queries.
    table: String,

    /// All tables this graph depends on (for routing changes).
    all_tables: Vec<String>,

    /// Schema for the primary table.
    schema: TableSchema,

    /// Schemas for all tables (including joined tables).
    all_schemas: HashMap<String, TableSchema>,

    /// Whether this is a join query.
    is_join: bool,

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
    /// Create a new single-table query graph.
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
        let mut all_schemas = HashMap::new();
        all_schemas.insert(table.clone(), schema.clone());

        Self {
            id,
            state: GraphState::Uninitialized,
            table: table.clone(),
            all_tables: vec![table],
            schema,
            all_schemas,
            is_join: false,
            nodes,
            node_indices,
            output_node,
        }
    }

    /// Create a new join query graph.
    pub(crate) fn new_join(
        id: GraphId,
        left_table: String,
        left_schema: TableSchema,
        right_table: String,
        right_schema: TableSchema,
        nodes: Vec<QueryNode>,
        node_indices: HashMap<NodeId, usize>,
        output_node: NodeId,
    ) -> Self {
        use crate::sql::schema::ColumnDef;

        let mut all_schemas = HashMap::new();
        all_schemas.insert(left_table.clone(), left_schema.clone());
        all_schemas.insert(right_table.clone(), right_schema.clone());

        // Build a combined schema for filter evaluation on joined rows.
        // Columns are: [left_cols..., right_cols...] with qualified names.
        let mut combined_columns = Vec::new();
        for col in &left_schema.columns {
            combined_columns.push(ColumnDef {
                name: format!("{}.{}", left_table, col.name),
                ty: col.ty.clone(),
                nullable: col.nullable,
            });
        }
        for col in &right_schema.columns {
            combined_columns.push(ColumnDef {
                name: format!("{}.{}", right_table, col.name),
                ty: col.ty.clone(),
                nullable: col.nullable,
            });
        }
        let combined_schema = TableSchema::new("_joined", combined_columns);

        Self {
            id,
            state: GraphState::Uninitialized,
            table: left_table.clone(),
            all_tables: vec![left_table, right_table],
            schema: combined_schema, // Use combined schema for JOIN graphs
            all_schemas,
            is_join: true,
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

    /// Get the primary table this graph queries.
    pub fn table(&self) -> &str {
        &self.table
    }

    /// Get all tables this graph depends on.
    pub fn all_tables(&self) -> &[String] {
        &self.all_tables
    }

    /// Check if this graph handles a specific table.
    pub fn handles_table(&self, table: &str) -> bool {
        self.all_tables.iter().any(|t| t == table)
    }

    /// Get the primary schema.
    pub fn schema(&self) -> &TableSchema {
        &self.schema
    }

    /// Get schema for a specific table.
    pub fn get_schema(&self, table: &str) -> Option<&TableSchema> {
        self.all_schemas.get(table)
    }

    /// Check if this is a join query.
    pub fn is_join(&self) -> bool {
        self.is_join
    }

    /// Check if the graph is initialized.
    pub fn is_ready(&self) -> bool {
        self.state == GraphState::Ready
    }

    /// Get current output rows, initializing lazily if needed.
    pub fn get_output(&mut self, cache: &mut RowCache, db: &DatabaseState) -> Vec<Row> {
        self.ensure_initialized_skip(cache, db, None, None);
        self.collect_output(cache, db)
    }

    /// Ensure the graph is initialized, optionally skipping a specific row.
    ///
    /// The skip_id and skip_table are used when initializing during a row change
    /// notification - we skip that row because it will be processed as part of the delta.
    fn ensure_initialized_skip(
        &mut self,
        cache: &mut RowCache,
        db: &DatabaseState,
        skip_id: Option<ObjectId>,
        skip_table: Option<&str>,
    ) {
        if self.state != GraphState::Uninitialized {
            return;
        }

        self.state = GraphState::Initializing;

        if self.is_join {
            // For JOIN queries, we need to initialize the join node by loading
            // all left rows and joining them with corresponding right rows
            self.init_join_query(cache, db, skip_id, skip_table);
        } else {
            // Single-table query: load all rows from the primary table
            let rows = db.read_all_rows(&self.table);

            for row in rows {
                // Skip the triggering row - it will be processed as a delta
                if skip_table == Some(&self.table) && skip_id == Some(row.id) {
                    continue;
                }

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
        }

        self.state = GraphState::Ready;
    }

    /// Initialize a JOIN query by loading and joining all rows.
    fn init_join_query(
        &mut self,
        cache: &mut RowCache,
        db: &DatabaseState,
        skip_id: Option<ObjectId>,
        skip_table: Option<&str>,
    ) {
        let table = self.table.clone();

        // Load all rows from the primary (left) table
        let left_rows = db.read_all_rows(&table);

        for left_row in left_rows {
            // Skip if this is the triggering row
            if skip_table == Some(table.as_str()) && skip_id == Some(left_row.id) {
                continue;
            }

            cache.insert(&table, left_row.clone());

            // Process through nodes - the join node will look up right rows
            let delta = RowDelta::Added(left_row);
            self.process_delta_through_nodes(delta, &table, cache, db);
        }
    }

    /// Process a delta through all nodes, handling JOIN nodes specially.
    fn process_delta_through_nodes(
        &mut self,
        delta: RowDelta,
        source_table: &str,
        cache: &mut RowCache,
        db: &DatabaseState,
    ) -> DeltaBatch {
        let mut current = DeltaBatch::new();
        current.push(delta);

        for node in &mut self.nodes {
            if current.is_empty() {
                break; // Early cutoff
            }

            match node {
                QueryNode::Join { left_table, .. } => {
                    // Join nodes need special evaluation with database access
                    let left_table = left_table.clone();
                    let left_schema = self.all_schemas.get(&left_table).cloned().unwrap();

                    let mut output = DeltaBatch::new();
                    for d in current.into_iter() {
                        let batch = node.evaluate_join(
                            d,
                            source_table,
                            &left_schema,
                            |table, id| db.get_row(table, id),
                            |table, column, target_id| {
                                db.find_referencing(table, column, target_id)
                            },
                        );
                        output.extend(batch);
                    }
                    current = output;
                }
                _ => {
                    current = node.evaluate(current, &self.schema, cache);
                }
            }
        }

        current
    }

    /// Collect output rows from the final cached set.
    fn collect_output(&self, cache: &RowCache, _db: &DatabaseState) -> Vec<Row> {
        // Find the node feeding into Output
        let output_idx = self.node_indices[&self.output_node];
        if let QueryNode::Output { input, .. } = &self.nodes[output_idx] {
            let input_idx = self.node_indices[input];

            // For join queries, get the joined rows from the join node
            if self.is_join {
                if let Some(joined_rows) = self.nodes[input_idx].cached_joined() {
                    return joined_rows.values().map(|jr| jr.to_output_row()).collect();
                }
            }

            // For single-table queries, use cached IDs
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
    /// Initializes lazily if needed. For JOIN queries, `source_table` indicates
    /// which table the change came from.
    pub fn process_change(
        &mut self,
        delta: RowDelta,
        cache: &mut RowCache,
        db: &DatabaseState,
    ) -> DeltaBatch {
        self.process_change_from_table(delta, &self.table.clone(), cache, db)
    }

    /// Process a row change from a specific table.
    pub fn process_change_from_table(
        &mut self,
        delta: RowDelta,
        source_table: &str,
        cache: &mut RowCache,
        db: &DatabaseState,
    ) -> DeltaBatch {
        // Get the row ID being changed - we need to skip it during init
        let skip_id = Some(delta.row_id());
        self.ensure_initialized_skip(cache, db, skip_id, Some(source_table));

        // Update cache with new value
        match &delta {
            RowDelta::Added(row) => cache.insert(source_table, row.clone()),
            RowDelta::Removed { id, .. } => cache.mark_deleted(source_table, *id),
            RowDelta::Updated { new, .. } => cache.insert(source_table, new.clone()),
        }

        // Process through nodes
        self.process_delta_through_nodes(delta, source_table, cache, db)
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
    use crate::sql::Database;
    use crate::object::ObjectId;

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
