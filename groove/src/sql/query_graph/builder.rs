//! Builder for constructing query graphs programmatically.

use std::collections::{HashMap, HashSet};

use crate::sql::query_graph::graph::{GraphId, QueryGraph};
use crate::sql::query_graph::node::{NodeId, QueryNode};
use crate::sql::query_graph::predicate::Predicate;
use crate::sql::schema::TableSchema;
use crate::sql::types::IndexKey;
use crate::object::ObjectId;

/// Builder for constructing `QueryGraph` instances.
///
/// # Example
///
/// ```ignore
/// let schema = db.get_table("users").unwrap();
/// let mut builder = QueryGraphBuilder::new("users", schema);
///
/// // SELECT * FROM users WHERE active = true
/// let scan = builder.table_scan();
/// let filter = builder.filter(scan, Predicate::eq("active", Value::Bool(true)));
/// let graph = builder.output(filter, GraphId(1));
/// ```
pub struct QueryGraphBuilder {
    table: String,
    schema: TableSchema,
    nodes: Vec<QueryNode>,
    next_id: u32,
}

impl QueryGraphBuilder {
    /// Create a new builder for queries on the given table.
    pub fn new(table: impl Into<String>, schema: TableSchema) -> Self {
        Self {
            table: table.into(),
            schema,
            nodes: Vec::new(),
            next_id: 0,
        }
    }

    /// Allocate a new node ID.
    fn alloc_id(&mut self) -> NodeId {
        let id = NodeId(self.next_id);
        self.next_id += 1;
        id
    }

    /// Add a table scan source node.
    ///
    /// This reads all rows from the table.
    pub fn table_scan(&mut self) -> NodeId {
        let id = self.alloc_id();
        self.nodes.push(QueryNode::TableScan {
            table: self.table.clone(),
            cached_ids: HashSet::new(),
        });
        id
    }

    /// Add an index lookup source node.
    ///
    /// This reads rows that reference a specific target via a Ref column.
    /// Uses the reverse index for efficient lookup.
    pub fn index_lookup(&mut self, column: impl Into<String>, target_id: ObjectId) -> NodeId {
        let id = self.alloc_id();
        let column = column.into();
        self.nodes.push(QueryNode::IndexLookup {
            table: self.table.clone(),
            index_key: IndexKey::new(&self.table, &column),
            target_id,
            cached_ids: HashSet::new(),
        });
        id
    }

    /// Add a filter node.
    ///
    /// Filters rows from the input node using the given predicate.
    pub fn filter(&mut self, input: NodeId, predicate: Predicate) -> NodeId {
        let id = self.alloc_id();
        self.nodes.push(QueryNode::Filter {
            table: self.table.clone(),
            input,
            predicate,
            cached_ids: HashSet::new(),
        });
        id
    }

    /// Add a recursive filter node for self-referential policies.
    ///
    /// This handles policies like `owner_id = @viewer OR INHERITS SELECT FROM parent_id`
    /// where `parent_id` references the same table. The node computes the transitive
    /// closure of accessible rows using fixpoint iteration.
    ///
    /// - `base_predicate`: Condition for direct access (e.g., `owner_id = @viewer`)
    /// - `recursive_column`: Column that references parent row (e.g., `parent_id`)
    pub fn recursive_filter(
        &mut self,
        input: NodeId,
        base_predicate: Predicate,
        recursive_column: impl Into<String>,
    ) -> NodeId {
        let id = self.alloc_id();
        self.nodes.push(QueryNode::RecursiveFilter {
            table: self.table.clone(),
            input,
            base_predicate,
            recursive_column: recursive_column.into(),
            accessible: HashMap::new(),
            children_index: HashMap::new(),
            all_rows: HashMap::new(),
        });
        id
    }

    /// Add an array aggregate node for ARRAY subqueries.
    ///
    /// This aggregates rows from an inner table into arrays per outer row.
    /// Used for queries like:
    /// ```sql
    /// SELECT f.*, ARRAY(SELECT n FROM notes n WHERE n.folder_id = f.id) as notes
    /// FROM folders f
    /// ```
    ///
    /// - `input`: Input node providing outer rows
    /// - `inner_table`: Table being aggregated (e.g., "notes")
    /// - `inner_ref_column`: Column in inner table referencing outer (e.g., "folder_id")
    /// - `inner_schema`: Schema of the inner table
    /// - `array_column_index`: Index where array should be placed (-1 for append)
    pub fn array_aggregate(
        &mut self,
        input: NodeId,
        inner_table: impl Into<String>,
        inner_ref_column: impl Into<String>,
        inner_schema: TableSchema,
        array_column_index: i32,
    ) -> NodeId {
        let id = self.alloc_id();
        self.nodes.push(QueryNode::ArrayAggregate {
            outer_table: self.table.clone(),
            input,
            inner_table: inner_table.into(),
            inner_ref_column: inner_ref_column.into(),
            inner_schema,
            array_column_index,
            cached_arrays: HashMap::new(),
            inner_to_outer: HashMap::new(),
            outer_rows: HashMap::new(),
        });
        id
    }

    /// Add a limit/offset node.
    ///
    /// Applies pagination to the result set:
    /// - `limit`: Maximum number of rows to return (None = unlimited)
    /// - `offset`: Number of rows to skip from the start
    ///
    /// Without ORDER BY, uses ObjectId ordering (UUIDv7 = insertion order).
    pub fn limit_offset(&mut self, input: NodeId, limit: Option<u64>, offset: u64) -> NodeId {
        // Skip if no actual limiting (no limit and offset=0)
        if limit.is_none() && offset == 0 {
            return input;
        }

        let id = self.alloc_id();
        self.nodes.push(QueryNode::LimitOffset {
            table: self.table.clone(),
            input,
            limit,
            offset,
            all_rows: std::collections::BTreeMap::new(),
            visible_ids: HashSet::new(),
        });
        id
    }

    /// Add the output node and build the graph.
    ///
    /// This consumes the builder and returns the constructed graph.
    pub fn output(mut self, input: NodeId, graph_id: GraphId) -> QueryGraph {
        let output_id = self.alloc_id();
        self.nodes.push(QueryNode::Output {
            table: self.table.clone(),
            input,
        });

        // Build node_indices map
        let mut node_indices = HashMap::new();
        for (idx, _) in self.nodes.iter().enumerate() {
            node_indices.insert(NodeId(idx as u32), idx);
        }

        QueryGraph::new(
            graph_id,
            self.table,
            self.schema,
            self.nodes,
            node_indices,
            output_id,
        )
    }
}

/// Builder for constructing JOIN query graphs.
///
/// This builder creates graphs that join a left (primary) table with a right
/// table on a reference column. Supports chaining multiple joins.
pub struct JoinGraphBuilder {
    left_table: String,
    left_schema: TableSchema,
    right_table: String,
    right_schema: TableSchema,
    left_column: String,
    nodes: Vec<QueryNode>,
    next_id: u32,
    /// Additional schemas for chained joins (table_name → schema)
    extra_schemas: HashMap<String, TableSchema>,
    /// Combined schema after all joins (used for chained joins)
    combined_schema: TableSchema,
    /// All right tables in the chain (for multi-table graph creation)
    all_right_tables: Vec<(String, TableSchema)>,
    /// For reverse JOINs, which table's columns to output (SELECT Table.*)
    projection_table: Option<String>,
}

impl JoinGraphBuilder {
    /// Create a new builder for a JOIN query.
    ///
    /// - `left_table`: The primary table (FROM clause)
    /// - `left_schema`: Schema of the primary table
    /// - `right_table`: The joined table (JOIN clause)
    /// - `right_schema`: Schema of the joined table
    /// - `left_column`: The Ref column in left table that references right table
    pub fn new(
        left_table: impl Into<String>,
        left_schema: TableSchema,
        right_table: impl Into<String>,
        right_schema: TableSchema,
        left_column: impl Into<String>,
    ) -> Self {
        let left_table = left_table.into();
        let right_table = right_table.into();

        // Build initial combined schema for the first join
        let combined_schema = left_schema.combine(&right_schema);

        // Track right tables for multi-table joins
        let all_right_tables = vec![(right_table.clone(), right_schema.clone())];

        Self {
            left_table,
            left_schema,
            right_table,
            right_schema,
            left_column: left_column.into(),
            nodes: Vec::new(),
            next_id: 0,
            extra_schemas: HashMap::new(),
            combined_schema,
            all_right_tables,
            projection_table: None,
        }
    }

    /// Add an additional schema for chained joins.
    pub fn add_schema(&mut self, table: impl Into<String>, schema: TableSchema) {
        self.extra_schemas.insert(table.into(), schema);
    }

    /// Set the projection table for reverse JOINs.
    ///
    /// When tables are swapped for the graph (because the JOIN table has the Ref),
    /// this specifies which table's columns should appear in the output.
    pub fn set_projection(&mut self, table: impl Into<String>) {
        self.projection_table = Some(table.into());
    }

    /// Allocate a new node ID.
    fn alloc_id(&mut self) -> NodeId {
        let id = NodeId(self.next_id);
        self.next_id += 1;
        id
    }

    /// Add the join node as a source.
    ///
    /// This is the first node in a JOIN graph - it handles the join logic.
    pub fn join(&mut self) -> NodeId {
        let id = self.alloc_id();
        self.nodes.push(QueryNode::Join {
            input_tables: vec![self.left_table.clone()],
            join_table: self.right_table.clone(),
            join_column: self.left_column.clone(),
            join_schema: self.right_schema.clone(),
            cached_rows: HashMap::new(),
            reverse_index: HashMap::new(),
        });
        id
    }

    /// Add a filter node on the joined result.
    ///
    /// Note: The predicate should match the joined row structure
    /// (left columns first, then right columns).
    pub fn filter(&mut self, input: NodeId, predicate: Predicate) -> NodeId {
        let id = self.alloc_id();
        self.nodes.push(QueryNode::Filter {
            table: self.left_table.clone(), // Primary table for identification
            input,
            predicate,
            cached_ids: HashSet::new(),
        });
        id
    }

    /// Add a chain join to extend the current join.
    ///
    /// This creates another Join node that takes the output of prior joins
    /// and joins with an additional table. The join column must be qualified
    /// (e.g., "folders.workspace_id" not just "workspace_id").
    ///
    /// - `_input`: The input node (for documentation; join chains are handled specially)
    /// - `source_table`: The table in the current join that has the ref column
    /// - `ref_column`: The column in source_table that references target_table
    /// - `target_table`: The table to join with
    pub fn chain_join(
        &mut self,
        _input: NodeId,
        source_table: impl Into<String>,
        ref_column: impl Into<String>,
        target_table: impl Into<String>,
    ) -> NodeId {
        let source = source_table.into();
        let target = target_table.into();
        let column = ref_column.into();

        let target_schema = self.extra_schemas.get(&target)
            .expect("chain_join: target schema not added via add_schema")
            .clone();

        // The join_column needs to be qualified since we're joining on combined rows
        let qualified_column = format!("{}.{}", source, column);

        // Build the list of input tables (all tables joined so far)
        let mut input_tables: Vec<String> = vec![self.left_table.clone()];
        for (table, _) in &self.all_right_tables {
            input_tables.push(table.clone());
        }

        let id = self.alloc_id();
        self.nodes.push(QueryNode::Join {
            input_tables,
            join_table: target.clone(),
            join_column: qualified_column,
            join_schema: target_schema.clone(),
            cached_rows: HashMap::new(),
            reverse_index: HashMap::new(),
        });

        // Track this as an additional right table for delta routing
        self.all_right_tables.push((target.clone(), target_schema.clone()));

        // Extend the combined schema with the new table's columns
        self.combined_schema = self.combined_schema.combine(&target_schema);

        id
    }

    /// Add a limit/offset node.
    ///
    /// Applies pagination to the result set:
    /// - `limit`: Maximum number of rows to return (None = unlimited)
    /// - `offset`: Number of rows to skip from the start
    ///
    /// Without ORDER BY, uses ObjectId ordering (UUIDv7 = insertion order).
    pub fn limit_offset(&mut self, input: NodeId, limit: Option<u64>, offset: u64) -> NodeId {
        // Skip if no actual limiting (no limit and offset=0)
        if limit.is_none() && offset == 0 {
            return input;
        }

        let id = self.alloc_id();
        self.nodes.push(QueryNode::LimitOffset {
            table: self.left_table.clone(),
            input,
            limit,
            offset,
            all_rows: std::collections::BTreeMap::new(),
            visible_ids: HashSet::new(),
        });
        id
    }

    /// Add the output node and build the graph.
    pub fn output(mut self, input: NodeId, graph_id: GraphId) -> QueryGraph {
        let output_id = self.alloc_id();
        self.nodes.push(QueryNode::Output {
            table: self.left_table.clone(),
            input,
        });

        // Build node_indices map
        let mut node_indices = HashMap::new();
        for (idx, _) in self.nodes.iter().enumerate() {
            node_indices.insert(NodeId(idx as u32), idx);
        }

        // Collect additional right tables (beyond the first one)
        let additional_right_tables: Vec<_> = self.all_right_tables
            .into_iter()
            .skip(1) // Skip the first right table (it's passed separately)
            .collect();

        QueryGraph::new_chain_join(
            graph_id,
            self.left_table,
            self.left_schema,
            self.right_table,
            self.right_schema,
            additional_right_tables,
            self.projection_table,
            self.nodes,
            node_indices,
            output_id,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::row::Value;
    use crate::sql::schema::{ColumnDef, ColumnType};

    fn test_schema() -> TableSchema {
        TableSchema::new(
            "users",
            vec![
                ColumnDef::required("name", ColumnType::String),
                ColumnDef::required("active", ColumnType::Bool),
            ],
        )
    }

    #[test]
    fn build_simple_scan() {
        let schema = test_schema();
        let mut builder = QueryGraphBuilder::new("users", schema);

        let scan = builder.table_scan();
        let graph = builder.output(scan, GraphId(1));

        assert_eq!(graph.node_count(), 2); // scan + output
        assert_eq!(graph.table(), "users");
    }

    #[test]
    fn build_scan_with_filter() {
        let schema = test_schema();
        let mut builder = QueryGraphBuilder::new("users", schema);

        let scan = builder.table_scan();
        let filter = builder.filter(scan, Predicate::eq("active", Value::Bool(true)));
        let graph = builder.output(filter, GraphId(1));

        assert_eq!(graph.node_count(), 3); // scan + filter + output
    }

    #[test]
    fn build_chained_filters() {
        let schema = test_schema();
        let mut builder = QueryGraphBuilder::new("users", schema);

        let scan = builder.table_scan();
        let f1 = builder.filter(scan, Predicate::eq("active", Value::Bool(true)));
        let f2 = builder.filter(f1, Predicate::eq("name", Value::String("Alice".to_string())));
        let graph = builder.output(f2, GraphId(1));

        assert_eq!(graph.node_count(), 4); // scan + filter + filter + output
    }

    #[test]
    fn build_index_lookup() {
        let schema = TableSchema::new(
            "posts",
            vec![
                ColumnDef::required("title", ColumnType::String),
                ColumnDef::required("author", ColumnType::Ref("users".to_string())),
            ],
        );

        let mut builder = QueryGraphBuilder::new("posts", schema);

        // SELECT * FROM posts WHERE author = ?
        let lookup = builder.index_lookup("author", ObjectId::new(42));
        let graph = builder.output(lookup, GraphId(1));

        assert_eq!(graph.node_count(), 2); // lookup + output
    }

    #[test]
    fn node_ids_are_sequential() {
        let schema = test_schema();
        let mut builder = QueryGraphBuilder::new("users", schema);

        let id1 = builder.table_scan();
        let id2 = builder.filter(id1, Predicate::True);
        let id3 = builder.filter(id2, Predicate::True);

        assert_eq!(id1.0, 0);
        assert_eq!(id2.0, 1);
        assert_eq!(id3.0, 2);
    }
}
