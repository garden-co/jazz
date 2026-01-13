//! Builder for constructing query graphs programmatically.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use crate::object::ObjectId;
use crate::sql::catalog::DescriptorId;
use crate::sql::lens::QueryLensContext;
use crate::sql::query_graph::graph::{GraphId, QueryGraph};
use crate::sql::query_graph::node::{NodeId, QueryNode};
use crate::sql::query_graph::predicate::Predicate;
use crate::sql::row_buffer::RowDescriptor;
use crate::sql::schema::{ColumnType, TableSchema};
use crate::sql::types::IndexKey;

/// State for JOIN queries (lazily populated when join() is called).
struct JoinState {
    /// First right table name (needed separately for QueryGraph::new_chain_join)
    first_right_table: String,
    /// First right table schema
    first_right_schema: TableSchema,
    /// Additional right tables beyond the first: (table_name, schema)
    additional_right_tables: Vec<(String, TableSchema)>,
    /// Combined schema after all joins
    combined_schema: TableSchema,
    /// Extra schemas for chain joins (table_name → schema)
    extra_schemas: HashMap<String, TableSchema>,
}

/// Builder for constructing `QueryGraph` instances.
///
/// Handles both single-table and JOIN queries. For single-table queries,
/// use `table_scan()` or `index_lookup()` as the source. For JOIN queries,
/// use `join()` to add a join (which transitions to JOIN mode).
///
/// # Example (single-table)
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
///
/// # Example (JOIN)
///
/// ```ignore
/// let mut builder = QueryGraphBuilder::new("posts", posts_schema);
/// let join_node = builder.join("users", users_schema, "author");
/// let graph = builder.output(join_node, GraphId(1));
/// ```
pub struct QueryGraphBuilder {
    primary_table: String,
    primary_schema: TableSchema,
    nodes: Vec<QueryNode>,
    next_id: u32,
    /// Current output descriptor - evolves as array aggregates are added.
    /// None means use schema (no arrays added yet).
    current_descriptor: Option<Arc<RowDescriptor>>,
    /// JOIN state (lazily populated when join() is called)
    join_state: Option<JoinState>,
    /// Branches to read from for branch-aware queries.
    branches: Vec<String>,
    /// Target schema descriptor ID for branch-aware queries.
    target_descriptor_id: Option<DescriptorId>,
}

impl QueryGraphBuilder {
    /// Create a new builder for queries on the given table.
    pub fn new(table: impl Into<String>, schema: TableSchema) -> Self {
        Self {
            primary_table: table.into(),
            primary_schema: schema,
            nodes: Vec::new(),
            next_id: 0,
            current_descriptor: None,
            join_state: None,
            branches: vec![],
            target_descriptor_id: None,
        }
    }

    /// Set the branches to read from for branch-aware queries.
    ///
    /// When branches are specified, rows are read from all branches and
    /// merged using per-column LWW before predicate evaluation.
    ///
    /// Branch names must follow the `[env]-[schemaVersion]-[userBranch]` format.
    /// The target descriptor ID specifies which schema version to merge into.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let graph = QueryGraphBuilder::new("users", schema)
    ///     .with_branches(
    ///         vec!["prod-v1-main".into(), "staging-v2-feature".into()],
    ///         target_descriptor_id,
    ///     )
    ///     .table_scan()
    ///     ...
    /// ```
    pub fn with_branches(
        mut self,
        branches: Vec<String>,
        target_descriptor_id: DescriptorId,
    ) -> Self {
        self.branches = branches;
        self.target_descriptor_id = Some(target_descriptor_id);
        self
    }

    /// Allocate a new node ID.
    fn alloc_id(&mut self) -> NodeId {
        let id = NodeId(self.next_id);
        self.next_id += 1;
        id
    }

    /// Get the current schema (combined schema if in JOIN mode, primary schema otherwise).
    fn get_schema(&self) -> &TableSchema {
        self.join_state
            .as_ref()
            .map(|js| &js.combined_schema)
            .unwrap_or(&self.primary_schema)
    }

    /// Add a table scan source node.
    ///
    /// This reads all rows from the table using BranchMerge as the unified
    /// entry point. When no branches are specified via `with_branches()`,
    /// defaults to `["main"]` with no lens transforms.
    ///
    /// This architecture ensures all queries use the same notification
    /// mechanism (`notify_object_changed`) regardless of branch configuration.
    pub fn table_scan(&mut self) -> NodeId {
        // Always use BranchMerge - default to ["main"] if no branches specified
        if self.branches.is_empty() {
            self.branches = vec!["main".to_string()];
            // Note: target_descriptor_id stays None for simple single-branch queries
        }
        self.branch_merge_scan()
    }

    /// Create a BranchMerge node for branch-aware queries.
    ///
    /// Creates a BranchMerge entry point that reads from multiple branches
    /// and performs per-column LWW merge using pre-computed metadata.
    ///
    /// When `target_descriptor_id` is None, no lens transforms are attempted.
    /// This is the common case for single-branch queries (like reading from "main").
    fn branch_merge_scan(&mut self) -> NodeId {
        let descriptor = Arc::new(RowDescriptor::from_table_schema(&self.primary_schema));
        let table = self.primary_table.clone();
        let branches: Vec<String> = self.branches.clone();
        let target_descriptor_id = self.target_descriptor_id;

        // Create BranchMerge node (entry point, no separate CommitSource nodes needed)
        let merge_id = self.alloc_id();
        self.nodes.push(QueryNode::BranchMerge {
            table,
            branch_names: branches,
            descriptor,
            target_descriptor_id,
            object_states: HashMap::new(),
        });

        merge_id
    }

    /// Add an index lookup source node.
    ///
    /// This reads rows that reference a specific target via a Ref column.
    /// Uses the reverse index for efficient lookup.
    pub fn index_lookup(&mut self, column: impl Into<String>, target_id: ObjectId) -> NodeId {
        let id = self.alloc_id();
        let column = column.into();
        self.nodes.push(QueryNode::IndexLookup {
            table: self.primary_table.clone(),
            index_key: IndexKey::new(&self.primary_table, &column),
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
        let descriptor = Arc::new(RowDescriptor::from_table_schema(self.get_schema()));
        self.nodes.push(QueryNode::Filter {
            table: self.primary_table.clone(),
            input,
            predicate,
            descriptor,
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
        let descriptor = Arc::new(RowDescriptor::from_table_schema(&self.primary_schema));
        self.nodes.push(QueryNode::RecursiveFilter {
            table: self.primary_table.clone(),
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
        let inner_ref_column = inner_ref_column.into();

        // Track inner table schema in join state if present (for graph creation)
        if let Some(js) = &mut self.join_state {
            js.extra_schemas
                .insert(inner_table.clone(), inner_schema.clone());
            for (_, target_table, target_schema) in &inner_joins {
                js.extra_schemas
                    .insert(target_table.clone(), target_schema.clone());
            }
        }

        // Create Join nodes for inner joins BEFORE the ArrayAggregate
        // These handle deltas from both inner_table and the joined tables
        for (ref_column, target_table, target_schema) in &inner_joins {
            self.inner_join_for_array(
                &inner_table,
                &inner_schema,
                ref_column,
                target_table,
                target_schema,
            );
        }

        let id = self.alloc_id();

        // Build inner descriptor from inner schema, accounting for inner joins
        // If inner_joins is non-empty, the join columns become nested Row/Array types
        let inner_descriptor = Self::build_inner_descriptor_with_joins(&inner_schema, &inner_joins);

        // Build output descriptor: start from current descriptor (or schema if first array)
        // This allows multiple array aggregates to accumulate their columns
        let mut output_cols: Vec<(String, ColumnType, bool)> = match &self.current_descriptor {
            Some(desc) => desc
                .columns
                .iter()
                .map(|c| (c.name.clone(), c.ty.clone(), c.nullable))
                .collect(),
            None => self
                .get_schema()
                .columns
                .iter()
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
        let actual_array_index =
            if array_column_index < 0 || array_column_index as usize >= output_cols.len() {
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
            outer_table: self.primary_table.clone(),
            input,
            inner_table,
            inner_ref_column,
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

    /// Create a Join node for an inner join within an ARRAY subquery.
    ///
    /// This Join node handles deltas from both the inner table (e.g., IssueAssignees)
    /// and the join table (e.g., Users). Both tables get entry points since neither
    /// is the primary table of the main query.
    fn inner_join_for_array(
        &mut self,
        inner_table: &str,
        inner_schema: &TableSchema,
        ref_column: &str,
        target_table: &str,
        target_schema: &TableSchema,
    ) -> NodeId {
        let id = self.alloc_id();

        // Build table descriptors for buffer format
        let mut table_descriptors = HashMap::new();
        table_descriptors.insert(
            inner_table.to_string(),
            Arc::new(RowDescriptor::from_table_schema(inner_schema)),
        );
        table_descriptors.insert(
            target_table.to_string(),
            Arc::new(RowDescriptor::from_table_schema(target_schema)),
        );

        self.nodes.push(QueryNode::Join {
            input_tables: vec![inner_table.to_string()],
            join_table: target_table.to_string(),
            join_column: ref_column.to_string(),
            join_schema: target_schema.clone(),
            table_descriptors,
            left_index: HashMap::new(),
            right_index: HashMap::new(),
            right_by_ref: HashMap::new(),
            cached_rows: HashMap::new(),
            reverse_index: HashMap::new(),
            reverse_filter: None,
            input_tables_need_entry: true,
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
        let descriptor = Arc::new(RowDescriptor::from_table_schema(self.get_schema()));
        self.nodes.push(QueryNode::LimitOffset {
            table: self.primary_table.clone(),
            input,
            limit,
            offset,
            descriptor,
            all_rows: BTreeMap::new(),
            visible_ids: HashSet::new(),
        });
        id
    }

    /// Add a projection node to unqualify column names.
    ///
    /// Used after INHERITS JOINs to convert qualified column names back to
    /// unqualified names (e.g., "documents.title" → "title").
    ///
    /// The `source_schema` should be the original table schema with unqualified
    /// column names.
    pub fn projection_unqualify(
        &mut self,
        input: NodeId,
        table: &str,
        source_schema: &TableSchema,
    ) -> NodeId {
        let id = self.alloc_id();

        // Build column map: "documents.title" → "title"
        let column_map: HashMap<String, String> = source_schema
            .columns
            .iter()
            .map(|col| (format!("{}.{}", table, col.name), col.name.clone()))
            .collect();

        let output_descriptor = Arc::new(RowDescriptor::from_table_schema(source_schema));

        self.nodes.push(QueryNode::Projection {
            table: table.to_string(),
            input,
            column_map,
            output_descriptor,
            cached_rows: HashMap::new(),
        });

        id
    }

    /// Add a projection node to select one table's columns from a multi-table join.
    ///
    /// Used for reverse JOINs where we need to output only the SQL FROM table's columns.
    /// Keeps qualified column names (e.g., "Issues.title" stays "Issues.title").
    pub fn projection_select_table(
        &mut self,
        input: NodeId,
        table: &str,
        source_schema: &TableSchema,
    ) -> NodeId {
        let id = self.alloc_id();

        // Build column map: keep qualified names for this table only
        // "Issues.title" → "Issues.title" (passthrough)
        let column_map: HashMap<String, String> = source_schema
            .columns
            .iter()
            .map(|col| {
                let qualified = format!("{}.{}", table, col.name);
                (qualified.clone(), qualified)
            })
            .collect();

        // Output descriptor uses qualified column names
        let output_descriptor = Arc::new(RowDescriptor::from_table_schema_qualified(
            source_schema,
            table,
        ));

        self.nodes.push(QueryNode::Projection {
            table: table.to_string(),
            input,
            column_map,
            output_descriptor,
            cached_rows: HashMap::new(),
        });

        id
    }

    /// Add a projection node to qualify column names.
    ///
    /// Used before JOINs to convert unqualified column names to qualified.
    /// (e.g., "title" → "documents.title")
    pub fn projection_qualify(
        &mut self,
        input: NodeId,
        table: &str,
        source_schema: &TableSchema,
    ) -> NodeId {
        let id = self.alloc_id();

        // Build column map: "title" → "documents.title"
        let column_map: HashMap<String, String> = source_schema
            .columns
            .iter()
            .map(|col| (col.name.clone(), format!("{}.{}", table, col.name)))
            .collect();

        // Output descriptor uses qualified column names
        let output_descriptor = Arc::new(RowDescriptor::from_table_schema_qualified(
            source_schema,
            table,
        ));

        self.nodes.push(QueryNode::Projection {
            table: table.to_string(),
            input,
            column_map,
            output_descriptor,
            cached_rows: HashMap::new(),
        });

        id
    }

    /// Build a simple branch-aware query graph (BranchMerge → Output).
    ///
    /// This is a convenience method that creates a branch-merge query without
    /// any filters or projections. Useful for tests and simple queries that
    /// just want to read from multiple branches.
    ///
    /// # Panics
    ///
    /// Panics if `with_branches` was not called to set up the branches.
    pub fn build_branch_merge_query(mut self, graph_id: GraphId) -> QueryGraph {
        assert!(
            !self.branches.is_empty(),
            "build_branch_merge_query requires branches to be set via with_branches"
        );
        let scan = self.branch_merge_scan();
        self.output(scan, graph_id)
    }

    /// Add the output node and build the graph.
    ///
    /// This consumes the builder and returns the constructed graph.
    pub fn output(mut self, input: NodeId, graph_id: GraphId) -> QueryGraph {
        let output_id = self.alloc_id();
        self.nodes.push(QueryNode::Output {
            table: self.primary_table.clone(),
            input,
        });

        // Build node_indices map
        let mut node_indices = HashMap::new();
        for (idx, _) in self.nodes.iter().enumerate() {
            node_indices.insert(NodeId(idx as u32), idx);
        }

        match self.join_state {
            None => QueryGraph::new(
                graph_id,
                self.primary_table,
                self.primary_schema,
                self.nodes,
                node_indices,
                output_id,
                self.branches,
            ),
            Some(js) => QueryGraph::new_chain_join(
                graph_id,
                self.primary_table,
                self.primary_schema,
                js.first_right_table,
                js.first_right_schema,
                js.additional_right_tables,
                self.nodes,
                node_indices,
                output_id,
                self.branches,
            ),
        }
    }

    /// Finalize the graph with the given output node and lens context.
    ///
    /// Similar to `output`, but also configures the graph with a lens context
    /// for schema-aware query evaluation. This enables queries to work with
    /// rows from different schema versions.
    ///
    /// # Arguments
    ///
    /// * `input` - The node to use as the final output
    /// * `graph_id` - The ID to assign to the graph
    /// * `target_descriptor` - The target schema version for this query
    /// * `lens_ctx` - Lens context containing transformations between schema versions
    pub fn output_with_lens(
        self,
        input: NodeId,
        graph_id: GraphId,
        target_descriptor: DescriptorId,
        lens_ctx: QueryLensContext,
    ) -> QueryGraph {
        let mut graph = self.output(input, graph_id);
        graph.set_target_descriptor(target_descriptor);
        graph.set_lens_context(lens_ctx);
        graph
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
        let cols: Vec<(String, ColumnType, bool)> = inner_schema
            .columns
            .iter()
            .map(|col| {
                // Check if this column is a join column
                let join_info = inner_joins
                    .iter()
                    .find(|(ref_col, _, _)| ref_col == &col.name);

                if let Some((_, _, target_schema)) = join_info {
                    // This is a join column - convert to Array type containing target rows
                    let target_descriptor =
                        Arc::new(RowDescriptor::from_table_schema(target_schema));
                    (
                        col.name.clone(),
                        ColumnType::Array(target_descriptor),
                        false,
                    )
                } else {
                    // Regular column - use normal type conversion
                    (col.name.clone(), col.ty.clone(), col.nullable)
                }
            })
            .collect();

        Arc::new(RowDescriptor::new_ordered(cols))
    }

    // =========================================================================
    // JOIN methods - these transition the builder to JOIN mode
    // =========================================================================

    /// Add an additional schema for chained joins.
    ///
    /// Call this before `chain_join()` to register the target table's schema.
    pub fn add_schema(&mut self, table: impl Into<String>, schema: TableSchema) {
        if let Some(js) = &mut self.join_state {
            js.extra_schemas.insert(table.into(), schema);
        } else {
            // Pre-join: initialize join_state to store the schema
            let mut extra_schemas = HashMap::new();
            extra_schemas.insert(table.into(), schema);
            self.join_state = Some(JoinState {
                first_right_table: String::new(),
                first_right_schema: TableSchema::new_raw("", vec![]),
                additional_right_tables: Vec::new(),
                combined_schema: self.primary_schema.clone(),
                extra_schemas,
            });
        }
    }

    /// Add a join node as a source.
    ///
    /// This transitions the builder to JOIN mode, creating a Join node that
    /// combines rows from the primary table with rows from the right table.
    ///
    /// - `right_table`: The table to join with
    /// - `right_schema`: Schema of the joined table
    /// - `join_column`: The Ref column in primary table that references right table
    pub fn join(
        &mut self,
        right_table: impl Into<String>,
        right_schema: TableSchema,
        join_column: impl Into<String>,
    ) -> NodeId {
        let right_table = right_table.into();
        let join_column = join_column.into();

        // Build combined schema
        let combined_schema = self.primary_schema.combine(&right_schema);

        // Initialize or update join_state
        let extra_schemas = self
            .join_state
            .take()
            .map(|js| js.extra_schemas)
            .unwrap_or_default();

        self.join_state = Some(JoinState {
            first_right_table: right_table.clone(),
            first_right_schema: right_schema.clone(),
            additional_right_tables: Vec::new(),
            combined_schema,
            extra_schemas,
        });

        // Build table descriptors for buffer format with qualified column names
        let mut table_descriptors = HashMap::new();
        table_descriptors.insert(
            self.primary_table.clone(),
            Arc::new(RowDescriptor::from_table_schema_qualified(
                &self.primary_schema,
                &self.primary_table,
            )),
        );
        table_descriptors.insert(
            right_table.clone(),
            Arc::new(RowDescriptor::from_table_schema_qualified(
                &right_schema,
                &right_table,
            )),
        );

        let id = self.alloc_id();
        self.nodes.push(QueryNode::Join {
            input_tables: vec![self.primary_table.clone()],
            join_table: right_table,
            join_column,
            join_schema: right_schema,
            table_descriptors,
            left_index: HashMap::new(),
            right_index: HashMap::new(),
            right_by_ref: HashMap::new(),
            cached_rows: HashMap::new(),
            reverse_index: HashMap::new(),
            reverse_filter: None,
            input_tables_need_entry: false,
        });
        id
    }

    /// Add a chain join to extend the current join.
    ///
    /// This creates another Join node that takes the output of prior joins
    /// and joins with an additional table. The join column must be qualified
    /// (e.g., "folders.workspace_id" not just "workspace_id").
    ///
    /// Requires: `add_schema(target_table, schema)` called first.
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

        // Copy primary table info first to avoid borrow conflicts
        let primary_table = self.primary_table.clone();
        let primary_schema = self.primary_schema.clone();

        let js = self
            .join_state
            .as_mut()
            .expect("chain_join: join_state not initialized - call join() first");

        let target_schema = js
            .extra_schemas
            .get(&target)
            .expect("chain_join: target schema not added via add_schema")
            .clone();

        // The join_column needs to be qualified since we're joining on combined rows
        let qualified_column = format!("{}.{}", source, column);

        // Build the list of input tables (all tables joined so far)
        let mut input_tables: Vec<String> = vec![primary_table.clone()];
        input_tables.push(js.first_right_table.clone());
        for (table, _) in &js.additional_right_tables {
            input_tables.push(table.clone());
        }

        // Build table descriptors for all tables with qualified column names
        let mut table_descriptors = HashMap::new();
        table_descriptors.insert(
            primary_table.clone(),
            Arc::new(RowDescriptor::from_table_schema_qualified(
                &primary_schema,
                &primary_table,
            )),
        );
        table_descriptors.insert(
            js.first_right_table.clone(),
            Arc::new(RowDescriptor::from_table_schema_qualified(
                &js.first_right_schema,
                &js.first_right_table,
            )),
        );
        for (t, schema) in &js.additional_right_tables {
            table_descriptors.insert(
                t.clone(),
                Arc::new(RowDescriptor::from_table_schema_qualified(schema, t)),
            );
        }
        table_descriptors.insert(
            target.clone(),
            Arc::new(RowDescriptor::from_table_schema_qualified(
                &target_schema,
                &target,
            )),
        );

        // Track this as an additional right table for delta routing
        js.additional_right_tables
            .push((target.clone(), target_schema.clone()));

        // Extend the combined schema with the new table's columns
        js.combined_schema = js.combined_schema.extend_with(&target_schema);

        // Now we can allocate ID and push node
        let id = self.alloc_id();
        self.nodes.push(QueryNode::Join {
            input_tables,
            join_table: target,
            join_column: qualified_column,
            join_schema: target_schema,
            table_descriptors,
            left_index: HashMap::new(),
            right_index: HashMap::new(),
            right_by_ref: HashMap::new(),
            cached_rows: HashMap::new(),
            reverse_index: HashMap::new(),
            reverse_filter: None,
            input_tables_need_entry: false,
        });

        id
    }

    /// Add a reverse chain join to extend the current join.
    ///
    /// This handles the case where the new table has a ref column pointing to an
    /// existing table (1:N relationship). The join column is in the target table,
    /// not in the input.
    ///
    /// Requires: `add_schema(target_table, schema)` called first.
    ///
    /// - `_input`: The input node (for documentation)
    /// - `existing_table`: The table in the current join that is referenced
    /// - `ref_column`: The column in target_table that references existing_table
    /// - `target_table`: The table to join with (has the ref column)
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
    ///
    /// Requires: `add_schema(target_table, schema)` called first.
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

        // Copy primary table info first to avoid borrow conflicts
        let primary_table = self.primary_table.clone();
        let primary_schema = self.primary_schema.clone();

        let js = self
            .join_state
            .as_mut()
            .expect("reverse_chain_join: join_state not initialized - call join() first");

        let target_schema = js
            .extra_schemas
            .get(&target)
            .expect("reverse_chain_join: target schema not added via add_schema")
            .clone();

        // For reverse joins, the join_column is in the target table
        // We use a special format to indicate this: "target@existing.column"
        // This tells the evaluator to look up target rows where target.column = existing.id
        let qualified_column = format!("{}@{}.{}", target, existing, column);

        // Build the list of input tables (all tables joined so far)
        let mut input_tables: Vec<String> = vec![primary_table.clone()];
        input_tables.push(js.first_right_table.clone());
        for (table, _) in &js.additional_right_tables {
            input_tables.push(table.clone());
        }

        // Build table descriptors for all tables with qualified column names
        let mut table_descriptors = HashMap::new();
        table_descriptors.insert(
            primary_table.clone(),
            Arc::new(RowDescriptor::from_table_schema_qualified(
                &primary_schema,
                &primary_table,
            )),
        );
        table_descriptors.insert(
            js.first_right_table.clone(),
            Arc::new(RowDescriptor::from_table_schema_qualified(
                &js.first_right_schema,
                &js.first_right_table,
            )),
        );
        for (t, schema) in &js.additional_right_tables {
            table_descriptors.insert(
                t.clone(),
                Arc::new(RowDescriptor::from_table_schema_qualified(schema, t)),
            );
        }
        table_descriptors.insert(
            target.clone(),
            Arc::new(RowDescriptor::from_table_schema_qualified(
                &target_schema,
                &target,
            )),
        );

        // Track this as an additional right table for delta routing
        js.additional_right_tables
            .push((target.clone(), target_schema.clone()));

        // Note: For reverse joins, we DON'T extend the combined schema because
        // the reverse join table's columns are NOT added to the output row.
        // The ArrayAggregate will later re-fetch and add them as arrays.

        // Now we can allocate ID and push node
        let id = self.alloc_id();
        self.nodes.push(QueryNode::Join {
            input_tables,
            join_table: target,
            join_column: qualified_column,
            join_schema: target_schema,
            table_descriptors,
            left_index: HashMap::new(),
            right_index: HashMap::new(),
            right_by_ref: HashMap::new(),
            cached_rows: HashMap::new(),
            reverse_index: HashMap::new(),
            reverse_filter: filter,
            input_tables_need_entry: false,
        });

        id
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
        let f2 = builder.filter(
            f1,
            Predicate::eq("name", PredicateValue::String("Alice".to_string())),
        );
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
