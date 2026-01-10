//! Builder for constructing query graphs programmatically.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use crate::sql::query_graph::graph::{GraphId, QueryGraph};
use crate::sql::query_graph::node::{NodeId, QueryNode};
use crate::sql::query_graph::predicate::Predicate;
use crate::sql::row_buffer::RowDescriptor;
use crate::sql::schema::{ColumnType, TableSchema};
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
/// let filter = builder.filter(scan, Predicate::eq("active", PredicateValue::Bool(true)));
/// let graph = builder.output(filter, GraphId(1));
/// ```
pub struct QueryGraphBuilder {
    table: String,
    schema: TableSchema,
    nodes: Vec<QueryNode>,
    next_id: u32,
    /// Current output descriptor - evolves as array aggregates are added.
    /// None means use schema (no arrays added yet).
    current_descriptor: Option<Arc<RowDescriptor>>,
}

impl QueryGraphBuilder {
    /// Create a new builder for queries on the given table.
    pub fn new(table: impl Into<String>, schema: TableSchema) -> Self {
        Self {
            table: table.into(),
            schema,
            nodes: Vec::new(),
            next_id: 0,
            current_descriptor: None,
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
            descriptor: Arc::new(RowDescriptor::from_table_schema(&self.schema)),
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
        let descriptor = Arc::new(RowDescriptor::from_table_schema(&self.schema));
        self.nodes.push(QueryNode::RecursiveFilter {
            table: self.table.clone(),
            input,
            base_predicate,
            recursive_column: recursive_column.into(),
            descriptor,
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
    /// - `inner_joins`: JOINs within the subquery: Vec<(ref_column, target_table, target_schema)>
    /// - `array_column_index`: Index where array should be placed (-1 for append)
    pub fn array_aggregate(
        &mut self,
        input: NodeId,
        inner_table: impl Into<String>,
        inner_ref_column: impl Into<String>,
        inner_schema: TableSchema,
        inner_joins: Vec<(String, String, TableSchema)>,
        array_column_index: i32,
    ) -> NodeId {
        let inner_table = inner_table.into();
        let id = self.alloc_id();

        // Build inner descriptor from inner schema, accounting for inner joins
        // If inner_joins is non-empty, the join columns become nested Row/Array types
        let inner_descriptor = Self::build_inner_descriptor_with_joins(&inner_schema, &inner_joins);

        // Build output descriptor: start from current descriptor (or schema if first array)
        // This allows multiple array aggregates to accumulate their columns
        let mut output_cols: Vec<(String, ColumnType, bool)> = match &self.current_descriptor {
            Some(desc) => desc.columns.iter()
                .map(|c| (c.name.clone(), c.ty.clone(), c.nullable))
                .collect(),
            None => self.schema.columns.iter()
                .map(|c| (c.name.clone(), c.ty.clone(), c.nullable))
                .collect(),
        };

        // Add the array column at the specified index
        // Use inner_table name for the array column (e.g., "IssueLabels", "IssueAssignees")
        let array_col = (
            inner_table.clone(),
            ColumnType::Array(inner_descriptor.clone()),
            false,
        );
        // Compute the actual array column index (may differ from requested if -1 or out of range)
        let actual_array_index = if array_column_index < 0 || array_column_index as usize >= output_cols.len() {
            let idx = output_cols.len() as i32;
            output_cols.push(array_col);
            idx
        } else {
            output_cols.insert(array_column_index as usize, array_col);
            array_column_index
        };
        let output_descriptor = Arc::new(RowDescriptor::new_ordered(output_cols));

        // Update current_descriptor so next array_aggregate builds on top of this
        self.current_descriptor = Some(output_descriptor.clone());

        self.nodes.push(QueryNode::ArrayAggregate {
            outer_table: self.table.clone(),
            input,
            inner_table,
            inner_ref_column: inner_ref_column.into(),
            inner_schema,
            inner_descriptor,
            output_descriptor,
            inner_joins,
            array_column_index: actual_array_index,
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
        let descriptor = Arc::new(RowDescriptor::from_table_schema(&self.schema));
        self.nodes.push(QueryNode::LimitOffset {
            table: self.table.clone(),
            input,
            limit,
            offset,
            descriptor,
            all_rows: BTreeMap::new(),
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

    /// Build an inner descriptor that accounts for inner joins.
    ///
    /// When inner_joins is non-empty, join columns (Ref types) are converted to
    /// Array types (single-item arrays containing the resolved Row).
    fn build_inner_descriptor_with_joins(
        inner_schema: &TableSchema,
        inner_joins: &[(String, String, TableSchema)],
    ) -> Arc<RowDescriptor> {
        if inner_joins.is_empty() {
            return Arc::new(RowDescriptor::from_table_schema(inner_schema));
        }

        // Build columns, converting join ref columns to Array types
        let cols: Vec<(String, ColumnType, bool)> = inner_schema.columns.iter()
            .map(|col| {
                // Check if this column is a join column
                let join_info = inner_joins.iter().find(|(ref_col, _, _)| ref_col == &col.name);

                if let Some((_, _, target_schema)) = join_info {
                    // This is a join column - convert to Array type containing target rows
                    let target_descriptor = Arc::new(RowDescriptor::from_table_schema(target_schema));
                    (col.name.clone(), ColumnType::Array(target_descriptor), false)
                } else {
                    // Regular column - use normal type conversion
                    (col.name.clone(), col.ty.clone(), col.nullable)
                }
            })
            .collect();

        Arc::new(RowDescriptor::new_ordered(cols))
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
    /// Current output descriptor - evolves as array aggregates are added.
    /// None means use combined_schema (no arrays added yet).
    current_descriptor: Option<Arc<RowDescriptor>>,
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
            current_descriptor: None,
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
        // Build table descriptors for buffer format with qualified column names
        // This is needed because downstream Filter nodes use qualified column names
        // in predicates (e.g., "folders.owner_id = @viewer")
        let mut table_descriptors = HashMap::new();
        table_descriptors.insert(
            self.left_table.clone(),
            Arc::new(RowDescriptor::from_table_schema_qualified(&self.left_schema, &self.left_table)),
        );
        table_descriptors.insert(
            self.right_table.clone(),
            Arc::new(RowDescriptor::from_table_schema_qualified(&self.right_schema, &self.right_table)),
        );
        self.nodes.push(QueryNode::Join {
            input_tables: vec![self.left_table.clone()],
            join_table: self.right_table.clone(),
            join_column: self.left_column.clone(),
            join_schema: self.right_schema.clone(),
            table_descriptors,
            cached_rows: HashMap::new(),
            reverse_index: HashMap::new(),
            reverse_filter: None,
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
            descriptor: Arc::new(RowDescriptor::from_table_schema(&self.combined_schema)),
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

        // Build table descriptors for all tables with qualified column names
        let mut table_descriptors = HashMap::new();
        table_descriptors.insert(
            self.left_table.clone(),
            Arc::new(RowDescriptor::from_table_schema_qualified(&self.left_schema, &self.left_table)),
        );
        for (t, schema) in &self.all_right_tables {
            table_descriptors.insert(t.clone(), Arc::new(RowDescriptor::from_table_schema_qualified(schema, t)));
        }
        table_descriptors.insert(
            target.clone(),
            Arc::new(RowDescriptor::from_table_schema_qualified(&target_schema, &target)),
        );

        let id = self.alloc_id();
        self.nodes.push(QueryNode::Join {
            input_tables,
            join_table: target.clone(),
            join_column: qualified_column,
            join_schema: target_schema.clone(),
            table_descriptors,
            cached_rows: HashMap::new(),
            reverse_index: HashMap::new(),
            reverse_filter: None,
        });

        // Track this as an additional right table for delta routing
        self.all_right_tables.push((target.clone(), target_schema.clone()));

        // Extend the combined schema with the new table's columns
        // Use extend_with to preserve existing qualified column names
        self.combined_schema = self.combined_schema.extend_with(&target_schema);

        id
    }

    /// Add a reverse chain join to extend the current join.
    ///
    /// This handles the case where the new table has a ref column pointing to an
    /// existing table (1:N relationship). The join column is in the target table,
    /// not in the input.
    ///
    /// - `_input`: The input node (for documentation)
    /// - `existing_table`: The table in the current join that is referenced
    /// - `ref_column`: The column in target_table that references existing_table
    /// - `target_table`: The table to join with (has the ref column)
    /// - `filter`: Optional predicate to filter join rows (for EXISTS-style filtering)
    pub fn reverse_chain_join(
        &mut self,
        _input: NodeId,
        existing_table: impl Into<String>,
        ref_column: impl Into<String>,
        target_table: impl Into<String>,
    ) -> NodeId {
        self.reverse_chain_join_with_filter(_input, existing_table, ref_column, target_table, None)
    }

    /// Add a reverse chain join with an optional filter predicate.
    ///
    /// When a filter is provided, only join rows matching the filter are considered.
    /// This is used for EXISTS-style queries like "find Issues where any IssueLabel has label = X".
    pub fn reverse_chain_join_with_filter(
        &mut self,
        _input: NodeId,
        existing_table: impl Into<String>,
        ref_column: impl Into<String>,
        target_table: impl Into<String>,
        filter: Option<Predicate>,
    ) -> NodeId {
        let existing = existing_table.into();
        let target = target_table.into();
        let column = ref_column.into();

        let target_schema = self.extra_schemas.get(&target)
            .expect("reverse_chain_join: target schema not added via add_schema")
            .clone();

        // For reverse joins, the join_column is in the target table
        // We use a special format to indicate this: "target@existing.column"
        // This tells the evaluator to look up target rows where target.column = existing.id
        let qualified_column = format!("{}@{}.{}", target, existing, column);

        // Build the list of input tables (all tables joined so far)
        let mut input_tables: Vec<String> = vec![self.left_table.clone()];
        for (table, _) in &self.all_right_tables {
            input_tables.push(table.clone());
        }

        // Build table descriptors for all tables with qualified column names
        let mut table_descriptors = HashMap::new();
        table_descriptors.insert(
            self.left_table.clone(),
            Arc::new(RowDescriptor::from_table_schema_qualified(&self.left_schema, &self.left_table)),
        );
        for (t, schema) in &self.all_right_tables {
            table_descriptors.insert(t.clone(), Arc::new(RowDescriptor::from_table_schema_qualified(schema, t)));
        }
        table_descriptors.insert(
            target.clone(),
            Arc::new(RowDescriptor::from_table_schema_qualified(&target_schema, &target)),
        );

        let id = self.alloc_id();
        self.nodes.push(QueryNode::Join {
            input_tables,
            join_table: target.clone(),
            join_column: qualified_column,
            join_schema: target_schema.clone(),
            table_descriptors,
            cached_rows: HashMap::new(),
            reverse_index: HashMap::new(),
            reverse_filter: filter,
        });

        // Track this as an additional right table for delta routing
        self.all_right_tables.push((target.clone(), target_schema.clone()));

        // Note: For reverse joins, we DON'T extend the combined schema because
        // the reverse join table's columns are NOT added to the output row.
        // The ArrayAggregate will later re-fetch and add them as arrays.

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
        // For JOIN graphs, use combined schema for the descriptor
        let descriptor = Arc::new(RowDescriptor::from_table_schema(&self.combined_schema));
        self.nodes.push(QueryNode::LimitOffset {
            table: self.left_table.clone(),
            input,
            limit,
            offset,
            descriptor,
            all_rows: BTreeMap::new(),
            visible_ids: HashSet::new(),
        });
        id
    }

    /// Add an array aggregate node for correlated subqueries.
    ///
    /// For each outer row, collects all inner rows that reference it
    /// (via `inner_ref_column`). Returns the outer row with an appended
    /// Array column containing all matching inner rows.
    ///
    /// Note: For JOIN graphs, the outer table is the primary (left) table.
    pub fn array_aggregate(
        &mut self,
        input: NodeId,
        inner_table: impl Into<String>,
        inner_ref_column: impl Into<String>,
        inner_schema: TableSchema,
        inner_joins: Vec<(String, String, TableSchema)>,
        array_column_index: i32,
    ) -> NodeId {
        let inner_table = inner_table.into();

        // Track this inner table schema for the graph
        self.extra_schemas.insert(inner_table.clone(), inner_schema.clone());

        // Track join target schemas too
        for (_, target_table, target_schema) in &inner_joins {
            self.extra_schemas.insert(target_table.clone(), target_schema.clone());
        }

        let id = self.alloc_id();

        // Build inner descriptor from inner schema, accounting for inner joins
        // If inner_joins is non-empty, the join columns become nested Row/Array types
        let inner_descriptor = QueryGraphBuilder::build_inner_descriptor_with_joins(&inner_schema, &inner_joins);

        // Build output descriptor: start from current descriptor (or combined schema if first array)
        // This allows multiple array aggregates to accumulate their columns
        let mut output_cols: Vec<(String, ColumnType, bool)> = match &self.current_descriptor {
            Some(desc) => desc.columns.iter()
                .map(|c| (c.name.clone(), c.ty.clone(), c.nullable))
                .collect(),
            None => self.combined_schema.columns.iter()
                .map(|c| (c.name.clone(), c.ty.clone(), c.nullable))
                .collect(),
        };

        // Add the array column at the specified index
        // Use inner_table name for the array column (e.g., "IssueLabels", "IssueAssignees")
        let array_col = (
            inner_table.clone(),
            ColumnType::Array(inner_descriptor.clone()),
            false,
        );
        // Compute the actual array column index (may differ from requested if -1 or out of range)
        let actual_array_index = if array_column_index < 0 || array_column_index as usize >= output_cols.len() {
            let idx = output_cols.len() as i32;
            output_cols.push(array_col);
            idx
        } else {
            output_cols.insert(array_column_index as usize, array_col);
            array_column_index
        };
        let output_descriptor = Arc::new(RowDescriptor::new_ordered(output_cols));

        // Update current_descriptor so next array_aggregate builds on top of this
        self.current_descriptor = Some(output_descriptor.clone());

        self.nodes.push(QueryNode::ArrayAggregate {
            outer_table: self.left_table.clone(),
            input,
            inner_table,
            inner_ref_column: inner_ref_column.into(),
            inner_schema,
            inner_descriptor,
            output_descriptor,
            inner_joins,
            array_column_index: actual_array_index,
            cached_arrays: HashMap::new(),
            inner_to_outer: HashMap::new(),
            outer_rows: HashMap::new(),
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
    use crate::sql::query_graph::PredicateValue;
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
        let filter = builder.filter(scan, Predicate::eq("active", PredicateValue::Bool(true)));
        let graph = builder.output(filter, GraphId(1));

        assert_eq!(graph.node_count(), 3); // scan + filter + output
    }

    #[test]
    fn build_chained_filters() {
        let schema = test_schema();
        let mut builder = QueryGraphBuilder::new("users", schema);

        let scan = builder.table_scan();
        let f1 = builder.filter(scan, Predicate::eq("active", PredicateValue::Bool(true)));
        let f2 = builder.filter(f1, Predicate::eq("name", PredicateValue::String("Alice".to_string())));
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
