//! Query graph nodes.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use crate::branch::{ColumnChanges, SchemaBranchName};
use crate::commit::CommitId;
use crate::object::{Object, ObjectId};
use crate::sql::catalog::DescriptorId;
use crate::sql::lens::{ColumnMapping, Lens, LensContext, QueryLensContext};
use crate::sql::query_graph::cache::RowCache;
use crate::sql::query_graph::delta::{BufferJoinedRow, DeltaBatch, PriorState, RowDelta};
use crate::sql::query_graph::predicate::Predicate;
use crate::sql::row_buffer::{OwnedRow, RowDescriptor, RowValue};
use crate::sql::schema::{ColumnType, TableSchema};
use crate::sql::types::IndexKey;

/// Unique identifier for a node within a query graph.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct NodeId(pub u32);

/// Input port on a node - specifies which logical input an edge connects to.
///
/// Most nodes have a single input (Default), but Join and ArrayAggregate have
/// two logical inputs that need to be distinguished for correct delta routing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InputPort {
    /// Default/only input (for single-input nodes like Filter, Projection, etc.)
    Default,
    /// Left/upstream input for Join nodes
    Left,
    /// Right/join_table input for Join nodes (from entry point)
    Right,
    /// Outer table input for ArrayAggregate (upstream rows to add arrays to)
    Outer,
    /// Inner table input for ArrayAggregate (rows to aggregate into arrays)
    Inner,
}

/// Per-object merge state in BranchMerge node.
///
/// Caches the merged result for early cutoff (skip re-emitting if unchanged).
#[derive(Clone, Debug, Default)]
pub struct MergedObjectState {
    /// Cached merged result.
    pub cached_merged: Option<OwnedRow>,
}

/// Reason why a row is accessible in a RecursiveFilter.
///
/// This is crucial for correctly handling removals - if a row is only
/// accessible via inheritance and its parent loses access, the row
/// must also lose access. But if it has its own base access, it stays.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AccessReason {
    /// Row satisfies the base predicate (e.g., owner_id = @viewer)
    Base,
    /// Row is accessible because its parent is accessible
    Inherited,
    /// Row satisfies both base and inherited (redundant but safe)
    Both,
}

/// A node in the query graph.
#[derive(Debug)]
pub enum QueryNode {
    /// Source: all row IDs in a table.
    ///
    /// This is typically the starting point for a query.
    /// It maintains a cached set of all row IDs currently in the table.
    TableScan {
        table: String,
        /// Cached set of row IDs in the table.
        cached_ids: HashSet<ObjectId>,
    },

    /// Source: row IDs from a reverse index lookup.
    ///
    /// Used for queries like `SELECT * FROM posts WHERE author = ?`
    /// where we can use the reverse index on `posts.author` to find
    /// all posts referencing a specific user.
    IndexLookup {
        table: String,
        index_key: IndexKey,
        target_id: ObjectId,
        /// Cached set of row IDs pointing at the target.
        cached_ids: HashSet<ObjectId>,
    },

    /// Transform: filter rows by a predicate.
    ///
    /// Takes input from another node and filters rows that match
    /// the predicate. Maintains a cached set of matching row IDs.
    Filter {
        table: String,
        input: NodeId,
        predicate: Predicate,
        /// Row descriptor for buffer format operations.
        descriptor: Arc<RowDescriptor>,
        /// Cached set of row IDs that pass the filter.
        cached_ids: HashSet<ObjectId>,
    },

    /// Transform: join rows from input with rows from join_table.
    ///
    /// This implements an inner join where:
    /// - `join_column` is a Ref column in the input (may be qualified for chains)
    /// - The join matches `input.join_column = join_table.id`
    ///
    /// Supports chain joins where input is combined rows from prior joins.
    /// On input deltas: looks up the corresponding join_table row by ID
    /// On join_table deltas: uses reverse_index to find affected input rows
    Join {
        /// Tables contained in the input (for first join: single table; for chains: multiple).
        input_tables: Vec<String>,
        /// Table being joined in this step.
        join_table: String,
        /// Column in input that references join_table (may be qualified: "folders.workspace_id").
        join_column: String,
        /// Schema of the join table (for building joined rows).
        join_schema: TableSchema,
        /// Descriptors for each table (input tables + join table).
        /// Used for buffer format conversion.
        table_descriptors: HashMap<String, Arc<RowDescriptor>>,
        /// Left index: join_key → left rows waiting for that key.
        /// For forward joins: join_key = referenced right row ID
        /// For reverse joins: join_key = left row's own ID
        left_index: HashMap<ObjectId, HashMap<ObjectId, OwnedRow>>,
        /// Right index: right_row_id → OwnedRow (all right rows seen so far).
        right_index: HashMap<ObjectId, OwnedRow>,
        /// For reverse joins: maps referenced_left_id → right rows that reference it.
        /// When a right row has ref_column = X, it's stored in right_by_ref[X][right_id].
        /// Empty for forward joins.
        right_by_ref: HashMap<ObjectId, HashMap<ObjectId, OwnedRow>>,
        /// Cached rows: primary_id → BufferJoinedRow.
        /// primary_id is always the leftmost table's row ID.
        /// Uses buffer format for efficient storage.
        cached_rows: HashMap<ObjectId, BufferJoinedRow>,
        /// Reverse index: join_table_id → set of primary_ids.
        /// Used for handling deltas from join_table.
        reverse_index: HashMap<ObjectId, HashSet<ObjectId>>,
        /// Optional filter predicate for reverse joins.
        /// Applied to join_table rows during lookup.
        reverse_filter: Option<Predicate>,
        /// When true, input_tables also need entry points (for ARRAY inner joins).
        /// Normally only join_table gets an entry point, but for inner joins within
        /// ARRAY subqueries, the inner table (in input_tables) also needs one.
        input_tables_need_entry: bool,
    },

    /// Terminal: marks the output of the graph.
    ///
    /// This node doesn't transform data, it just marks which node's
    /// output should be used as the query result.
    Output { table: String, input: NodeId },

    /// Transform: recursive filter for self-referential policies.
    ///
    /// Handles policies like `owner_id = @viewer OR INHERITS SELECT FROM parent_id`
    /// where `parent_id` references the same table. Uses fixpoint iteration
    /// to compute the transitive closure of accessible rows.
    ///
    /// A row is accessible if:
    /// - It satisfies the base_predicate (e.g., owner_id = @viewer), OR
    /// - Its parent (via recursive_column) is accessible
    RecursiveFilter {
        table: String,
        input: NodeId,
        /// Base predicate for direct access (e.g., owner_id = @viewer)
        base_predicate: Predicate,
        /// Column that references parent row in same table (e.g., parent_id)
        recursive_column: String,
        /// Row descriptor for buffer format rows.
        descriptor: Arc<RowDescriptor>,
        /// Currently accessible rows with their access reason
        accessible: HashMap<ObjectId, AccessReason>,
        /// Reverse index: parent_id -> set of children
        /// Used for efficient cascade propagation
        children_index: HashMap<ObjectId, HashSet<ObjectId>>,
        /// All rows in the table (needed for fixpoint iteration).
        /// Uses buffer format (OwnedRow) for efficient storage.
        all_rows: HashMap<ObjectId, OwnedRow>,
    },

    /// Transform: aggregate inner rows into arrays per outer row.
    ///
    /// Used for ARRAY(SELECT ...) subqueries. Groups rows from inner_table
    /// by their reference to outer_table, producing output rows where the
    /// nested column contains Value::Array of matching inner rows.
    ///
    /// Example:
    /// ```sql
    /// SELECT f.*, ARRAY(SELECT n FROM notes n WHERE n.folder_id = f.id) as notes
    /// FROM folders f
    /// ```
    ArrayAggregate {
        /// The outer table (source of correlation, e.g., "folders").
        outer_table: String,
        /// Input node providing outer rows.
        input: NodeId,
        /// The inner table being aggregated (e.g., "notes").
        inner_table: String,
        /// Column in inner table that references outer (e.g., "folder_id").
        inner_ref_column: String,
        /// Schema of inner table (for building Row values).
        inner_schema: TableSchema,
        /// Descriptor for inner table rows (buffer format).
        inner_descriptor: Arc<RowDescriptor>,
        /// Descriptor for output rows (outer columns + array column).
        output_descriptor: Arc<RowDescriptor>,
        /// JOINs within the ARRAY subquery (e.g., JOIN Labels ON il.label = Labels.id).
        /// Each tuple: (ref_column_in_inner, target_table, target_schema)
        /// The ref column will be replaced by the joined table's columns.
        inner_joins: Vec<(String, String, TableSchema)>,
        /// Index in output row where the array should be placed.
        /// -1 means append at end.
        array_column_index: i32,
        /// Cached arrays: outer_id → (inner_id → OwnedRow).
        /// Using HashMap for inner rows allows proper update/remove by inner_id.
        cached_arrays: HashMap<ObjectId, HashMap<ObjectId, OwnedRow>>,
        /// Reverse index: inner_id → outer_id (for propagating inner changes).
        inner_to_outer: HashMap<ObjectId, ObjectId>,
        /// Cached outer rows (needed to emit Updated deltas with full row data).
        /// Uses buffer format (OwnedRow).
        outer_rows: HashMap<ObjectId, OwnedRow>,
    },

    /// Transform: apply LIMIT and OFFSET to input rows.
    ///
    /// Maintains all qualifying rows from input and tracks which subset is
    /// "visible" (within the offset+limit window). Emits deltas when the
    /// visible window changes.
    ///
    /// Without ORDER BY, uses ObjectId ordering (UUIDv7 = insertion order).
    LimitOffset {
        table: String,
        input: NodeId,
        /// Maximum number of rows to return (None = unlimited).
        limit: Option<u64>,
        /// Number of rows to skip from the start.
        offset: u64,
        /// Row descriptor for buffer format rows.
        descriptor: Arc<RowDescriptor>,
        /// All rows that passed upstream filters, sorted by ObjectId.
        /// Uses buffer format (OwnedRow) for efficient storage.
        all_rows: BTreeMap<ObjectId, OwnedRow>,
        /// Currently visible row IDs (in the window [offset, offset+limit)).
        visible_ids: HashSet<ObjectId>,
    },

    /// Transform: project and rename columns from input rows.
    ///
    /// Used at the end of INHERITS JOIN graphs to strip table qualification
    /// from column names ("documents.title" → "title"). Also useful for
    /// explicit SELECT column lists.
    Projection {
        /// Primary table name (for debugging/display).
        table: String,
        /// Input node providing rows to project.
        input: NodeId,
        /// Column mappings: input_column_name → output_column_name.
        /// Columns not in this map are excluded from output.
        column_map: HashMap<String, String>,
        /// Descriptor for output rows (with renamed columns).
        output_descriptor: Arc<RowDescriptor>,
        /// Cached projected rows.
        cached_rows: HashMap<ObjectId, OwnedRow>,
    },

    /// Entry point: merge commits from multiple branches using per-column LWW.
    ///
    /// A smart entry point that reads branch data directly and merges
    /// using pre-computed per-column change metadata. Each branch tracks when
    /// each column was last changed (timestamp + author) for its frontier commits,
    /// enabling O(columns × frontiers) merge without LCA computation.
    ///
    /// Use `evaluate_branch_merge_with_metadata()` to evaluate this node.
    BranchMerge {
        /// Table being merged.
        table: String,
        /// Branch names to merge from.
        branch_names: Vec<String>,
        /// Row descriptor for merged output rows (target schema).
        descriptor: Arc<RowDescriptor>,
        /// Target schema descriptor ID for lens lookups.
        /// When None, no lens transforms are attempted (single-schema queries).
        target_descriptor_id: Option<DescriptorId>,
        /// Per-object merge state.
        object_states: HashMap<ObjectId, MergedObjectState>,
    },
}

impl QueryNode {
    /// Get the primary table this node operates on.
    pub fn table(&self) -> &str {
        match self {
            QueryNode::TableScan { table, .. } => table,
            QueryNode::IndexLookup { table, .. } => table,
            QueryNode::Filter { table, .. } => table,
            QueryNode::Join { input_tables, .. } => {
                input_tables.first().map(|s| s.as_str()).unwrap_or("")
            }
            QueryNode::Output { table, .. } => table,
            QueryNode::RecursiveFilter { table, .. } => table,
            QueryNode::ArrayAggregate { outer_table, .. } => outer_table,
            QueryNode::LimitOffset { table, .. } => table,
            QueryNode::Projection { table, .. } => table,
            QueryNode::BranchMerge { table, .. } => table,
        }
    }

    /// Get all tables this node depends on.
    pub fn tables(&self) -> Vec<&str> {
        match self {
            QueryNode::TableScan { table, .. } => vec![table],
            QueryNode::IndexLookup { table, .. } => vec![table],
            QueryNode::Filter { table, .. } => vec![table],
            QueryNode::Join {
                input_tables,
                join_table,
                ..
            } => {
                let mut tables: Vec<&str> = input_tables.iter().map(|s| s.as_str()).collect();
                tables.push(join_table);
                tables
            }
            QueryNode::Output { table, .. } => vec![table],
            QueryNode::RecursiveFilter { table, .. } => vec![table],
            QueryNode::ArrayAggregate {
                outer_table,
                inner_table,
                ..
            } => {
                vec![outer_table, inner_table]
            }
            QueryNode::LimitOffset { table, .. } => vec![table],
            QueryNode::Projection { table, .. } => vec![table],
            QueryNode::BranchMerge { table, .. } => vec![table],
        }
    }

    /// Get the cached IDs if this node caches them (single-table nodes only).
    pub fn cached_ids(&self) -> Option<&HashSet<ObjectId>> {
        match self {
            QueryNode::TableScan { cached_ids, .. } => Some(cached_ids),
            QueryNode::IndexLookup { cached_ids, .. } => Some(cached_ids),
            QueryNode::Filter { cached_ids, .. } => Some(cached_ids),
            QueryNode::Join { .. } => None, // Uses cached_pairs instead
            QueryNode::Output { .. } => None,
            QueryNode::RecursiveFilter { .. } => None, // Uses accessible instead
            QueryNode::ArrayAggregate { .. } => None,  // Uses outer_rows instead
            QueryNode::LimitOffset { visible_ids, .. } => Some(visible_ids),
            QueryNode::Projection { .. } => None, // Uses cached_rows HashMap instead
            QueryNode::BranchMerge { .. } => None, // Uses object_states instead
        }
    }

    /// Get a mutable reference to cached IDs.
    pub fn cached_ids_mut(&mut self) -> Option<&mut HashSet<ObjectId>> {
        match self {
            QueryNode::TableScan { cached_ids, .. } => Some(cached_ids),
            QueryNode::IndexLookup { cached_ids, .. } => Some(cached_ids),
            QueryNode::Filter { cached_ids, .. } => Some(cached_ids),
            QueryNode::Join { .. } => None,
            QueryNode::Output { .. } => None,
            QueryNode::RecursiveFilter { .. } => None,
            QueryNode::ArrayAggregate { .. } => None,
            QueryNode::LimitOffset { visible_ids, .. } => Some(visible_ids),
            QueryNode::Projection { .. } => None,
            QueryNode::BranchMerge { .. } => None,
        }
    }

    /// Get the accessible rows for RecursiveFilter nodes.
    pub fn accessible(&self) -> Option<&HashMap<ObjectId, AccessReason>> {
        match self {
            QueryNode::RecursiveFilter { accessible, .. } => Some(accessible),
            _ => None,
        }
    }

    /// Get all rows stored in a RecursiveFilter node (buffer format).
    pub fn all_rows(&self) -> Option<&HashMap<ObjectId, OwnedRow>> {
        match self {
            QueryNode::RecursiveFilter { all_rows, .. } => Some(all_rows),
            _ => None,
        }
    }

    /// Get the cached joined rows keyed by primary_id (for Join nodes).
    /// Returns buffer-format joined rows.
    pub fn cached_rows(&self) -> Option<&HashMap<ObjectId, BufferJoinedRow>> {
        match self {
            QueryNode::Join { cached_rows, .. } => Some(cached_rows),
            _ => None,
        }
    }

    /// Get the reverse index for Join nodes (join_table_id → primary_ids).
    pub fn reverse_index(&self) -> Option<&HashMap<ObjectId, HashSet<ObjectId>>> {
        match self {
            QueryNode::Join { reverse_index, .. } => Some(reverse_index),
            _ => None,
        }
    }

    /// Compatibility wrapper - returns cached_rows.
    pub fn cached_joined(&self) -> Option<&HashMap<ObjectId, BufferJoinedRow>> {
        self.cached_rows()
    }

    /// Get the cached arrays for ArrayAggregate nodes.
    pub fn cached_arrays(&self) -> Option<&HashMap<ObjectId, HashMap<ObjectId, OwnedRow>>> {
        match self {
            QueryNode::ArrayAggregate { cached_arrays, .. } => Some(cached_arrays),
            _ => None,
        }
    }

    /// Get the outer rows for ArrayAggregate nodes.
    pub fn outer_rows(&self) -> Option<&HashMap<ObjectId, OwnedRow>> {
        match self {
            QueryNode::ArrayAggregate { outer_rows, .. } => Some(outer_rows),
            _ => None,
        }
    }

    /// Get the cached projected rows for Projection nodes.
    pub fn cached_projected(&self) -> Option<&HashMap<ObjectId, OwnedRow>> {
        match self {
            QueryNode::Projection { cached_rows, .. } => Some(cached_rows),
            _ => None,
        }
    }

    /// Get the input node ID if this node has one.
    pub fn input(&self) -> Option<NodeId> {
        match self {
            QueryNode::TableScan { .. } => None,
            QueryNode::IndexLookup { .. } => None,
            QueryNode::Filter { input, .. } => Some(*input),
            QueryNode::Join { .. } => None, // Join is a source-like node
            QueryNode::Output { input, .. } => Some(*input),
            QueryNode::RecursiveFilter { input, .. } => Some(*input),
            QueryNode::ArrayAggregate { input, .. } => Some(*input),
            QueryNode::LimitOffset { input, .. } => Some(*input),
            QueryNode::Projection { input, .. } => Some(*input),
            QueryNode::BranchMerge { .. } => None, // Entry point, no input node
        }
    }

    /// Check if this node handles a specific table.
    pub fn handles_table(&self, table: &str) -> bool {
        self.tables().contains(&table)
    }

    /// Get diagram information for this node.
    ///
    /// Returns (node_type_name, details_lines) for rendering in a text diagram.
    pub fn diagram_info(&self) -> (String, Vec<String>) {
        match self {
            QueryNode::TableScan { table, cached_ids } => (
                format!("TableScan [{}]", table),
                vec![format!("cached: {} rows", cached_ids.len())],
            ),

            QueryNode::IndexLookup {
                table,
                index_key,
                target_id,
                cached_ids,
            } => (
                format!("IndexLookup [{}]", table),
                vec![
                    format!("index: {:?}", index_key),
                    format!("target: {}", target_id),
                    format!("cached: {} rows", cached_ids.len()),
                ],
            ),

            QueryNode::Filter {
                table,
                predicate,
                cached_ids,
                ..
            } => (
                format!("Filter [{}]", table),
                vec![
                    format!("predicate: {}", predicate.to_display_string()),
                    format!("cached: {} rows", cached_ids.len()),
                ],
            ),

            QueryNode::Join {
                input_tables,
                join_table,
                join_column,
                cached_rows,
                reverse_index,
                ..
            } => (
                "Join".to_string(),
                vec![
                    format!("inputs: [{}]", input_tables.join(", ")),
                    format!("join: {} ON {}", join_table, join_column),
                    format!("cached: {} joined rows", cached_rows.len()),
                    format!("reverse_index: {} entries", reverse_index.len()),
                ],
            ),

            QueryNode::Output { table, input } => (
                format!("Output [{}]", table),
                vec![format!("← from node {}", input.0)],
            ),

            QueryNode::RecursiveFilter {
                table,
                base_predicate,
                recursive_column,
                accessible,
                children_index,
                all_rows,
                ..
            } => (
                format!("RecursiveFilter [{}]", table),
                vec![
                    format!("base: {}", base_predicate.to_display_string()),
                    format!("recursive on: {}", recursive_column),
                    format!("accessible: {} rows", accessible.len()),
                    format!("children_index: {} parents", children_index.len()),
                    format!("all_rows: {} total", all_rows.len()),
                ],
            ),

            QueryNode::ArrayAggregate {
                outer_table,
                inner_table,
                inner_ref_column,
                cached_arrays,
                outer_rows,
                ..
            } => (
                format!("ArrayAggregate [{}]", outer_table),
                vec![
                    format!("inner: {} via {}", inner_table, inner_ref_column),
                    format!("cached: {} arrays", cached_arrays.len()),
                    format!("outer_rows: {} rows", outer_rows.len()),
                ],
            ),

            QueryNode::LimitOffset {
                table,
                limit,
                offset,
                all_rows,
                visible_ids,
                ..
            } => {
                let limit_str = limit
                    .map(|l| l.to_string())
                    .unwrap_or_else(|| "∞".to_string());
                (
                    format!("LimitOffset [{}]", table),
                    vec![
                        format!("LIMIT {} OFFSET {}", limit_str, offset),
                        format!(
                            "all_rows: {}, visible: {}",
                            all_rows.len(),
                            visible_ids.len()
                        ),
                    ],
                )
            }

            QueryNode::Projection {
                table,
                column_map,
                cached_rows,
                ..
            } => (
                format!("Projection [{}]", table),
                vec![
                    format!("columns: {} mappings", column_map.len()),
                    format!("cached: {} rows", cached_rows.len()),
                ],
            ),

            QueryNode::BranchMerge {
                table,
                branch_names,
                object_states,
                ..
            } => (
                format!("BranchMerge [{}]", table),
                vec![
                    format!("branches: {:?}", branch_names),
                    format!("objects: {}", object_states.len()),
                ],
            ),
        }
    }

    /// Evaluate this node given input deltas.
    ///
    /// Returns output deltas (may be empty for early cutoff).
    /// Note: Join nodes should use `evaluate_join` instead.
    pub fn evaluate(&mut self, input: DeltaBatch, _cache: &RowCache) -> DeltaBatch {
        match self {
            QueryNode::TableScan { cached_ids, .. } => Self::eval_id_passthrough(cached_ids, input),

            QueryNode::IndexLookup { cached_ids, .. } => {
                Self::eval_id_passthrough(cached_ids, input)
            }

            QueryNode::Filter {
                predicate,
                cached_ids,
                ..
            } => Self::eval_filter(predicate, cached_ids, input),

            QueryNode::Join { .. } => {
                // Join nodes need special handling with database access
                // This should be called via evaluate_join instead
                DeltaBatch::new()
            }

            QueryNode::Output { .. } => input, // Passthrough

            QueryNode::RecursiveFilter { .. } => {
                // RecursiveFilter nodes need special handling
                // This should be called via evaluate_recursive instead
                DeltaBatch::new()
            }

            QueryNode::ArrayAggregate { .. } => {
                // ArrayAggregate nodes need special handling with database access
                // This should be called via evaluate_array_aggregate instead
                DeltaBatch::new()
            }

            QueryNode::LimitOffset { .. } => {
                // LimitOffset nodes need special handling
                // This should be called via evaluate_limit_offset instead
                DeltaBatch::new()
            }

            QueryNode::Projection {
                column_map,
                output_descriptor,
                cached_rows,
                ..
            } => Self::eval_projection(column_map, output_descriptor, cached_rows, input),

            QueryNode::BranchMerge { object_states, .. } => {
                // Pass through deltas and cache state, similar to TableScan
                // This enables routing through the graph for ARRAY/JOIN queries
                Self::eval_branch_merge_passthrough(object_states, input)
            }
        }
    }

    /// Evaluate this node with optional lens transformation.
    ///
    /// Similar to `evaluate`, but accepts a lens context for transforming rows
    /// from older schema versions before predicate evaluation.
    ///
    /// # Arguments
    ///
    /// * `input` - Batch of row deltas to process
    /// * `_cache` - Row cache (currently unused for most node types)
    /// * `lens_ctx` - Optional lens context for schema transformation
    /// * `get_row_descriptor` - Function to get the source descriptor ID for a row
    pub fn evaluate_with_lens<F>(
        &mut self,
        input: DeltaBatch,
        _cache: &RowCache,
        lens_ctx: Option<&QueryLensContext>,
        get_row_descriptor: F,
    ) -> DeltaBatch
    where
        F: Fn(&ObjectId) -> Option<DescriptorId>,
    {
        match self {
            QueryNode::TableScan { cached_ids, .. } => Self::eval_id_passthrough(cached_ids, input),

            QueryNode::IndexLookup { cached_ids, .. } => {
                Self::eval_id_passthrough(cached_ids, input)
            }

            QueryNode::Filter {
                predicate,
                cached_ids,
                ..
            } => {
                // Use lens-aware filter evaluation
                Self::eval_filter_with_lens(
                    predicate,
                    cached_ids,
                    input,
                    lens_ctx,
                    get_row_descriptor,
                )
            }

            QueryNode::Join { .. } => {
                // Join nodes need special handling with database access
                DeltaBatch::new()
            }

            QueryNode::Output { .. } => input, // Passthrough

            QueryNode::RecursiveFilter { .. } => {
                // RecursiveFilter nodes need special handling
                DeltaBatch::new()
            }

            QueryNode::ArrayAggregate { .. } => {
                // ArrayAggregate nodes need special handling with database access
                DeltaBatch::new()
            }

            QueryNode::LimitOffset { .. } => {
                // LimitOffset nodes need special handling
                DeltaBatch::new()
            }

            QueryNode::Projection {
                column_map,
                output_descriptor,
                cached_rows,
                ..
            } => Self::eval_projection(column_map, output_descriptor, cached_rows, input),

            QueryNode::BranchMerge { object_states, .. } => {
                // Pass through deltas and cache state, similar to TableScan
                // This enables routing through the graph for ARRAY/JOIN queries
                Self::eval_branch_merge_passthrough(object_states, input)
            }
        }
    }

    /// Evaluate a Projection node.
    ///
    /// Projects and renames columns from input rows.
    fn eval_projection(
        column_map: &HashMap<String, String>,
        output_descriptor: &Arc<RowDescriptor>,
        cached_rows: &mut HashMap<ObjectId, OwnedRow>,
        input: DeltaBatch,
    ) -> DeltaBatch {
        let mut output = DeltaBatch::new();

        for delta in input.into_iter() {
            match delta {
                RowDelta::Added { id, row } => {
                    let projected = row.project_rename(column_map, output_descriptor.clone());
                    cached_rows.insert(id, projected.clone());
                    output.push(RowDelta::Added { id, row: projected });
                }
                RowDelta::Removed { id, prior } => {
                    cached_rows.remove(&id);
                    output.push(RowDelta::Removed { id, prior });
                }
                RowDelta::Updated { id, row, prior } => {
                    let projected = row.project_rename(column_map, output_descriptor.clone());
                    cached_rows.insert(id, projected.clone());
                    output.push(RowDelta::Updated {
                        id,
                        row: projected,
                        prior,
                    });
                }
            }
        }

        output
    }

    /// Evaluate a RecursiveFilter node with fixpoint iteration.
    ///
    /// This handles self-referential policies like:
    /// `owner_id = @viewer OR INHERITS SELECT FROM parent_id`
    ///
    /// Internally stores rows in buffer format (OwnedRow) for efficiency,
    /// converting at boundaries.
    pub fn evaluate_recursive(&mut self, input: DeltaBatch) -> DeltaBatch {
        match self {
            QueryNode::RecursiveFilter {
                base_predicate,
                recursive_column,
                descriptor,
                accessible,
                children_index,
                all_rows,
                ..
            } => {
                let mut output = DeltaBatch::new();

                for delta in input.into_iter() {
                    match delta {
                        RowDelta::Added { id, row } => {
                            Self::recursive_handle_insert(
                                id,
                                row,
                                descriptor.clone(),
                                base_predicate,
                                recursive_column,
                                accessible,
                                children_index,
                                all_rows,
                                &mut output,
                            );
                        }
                        RowDelta::Removed { id, prior } => {
                            Self::recursive_handle_remove(
                                id,
                                prior,
                                recursive_column,
                                accessible,
                                children_index,
                                all_rows,
                                &mut output,
                            );
                        }
                        RowDelta::Updated { id, row, prior } => {
                            // Handle as remove + insert for simplicity
                            // (Could optimize for cases where parent_id doesn't change)
                            Self::recursive_handle_remove(
                                id,
                                prior.clone(),
                                recursive_column,
                                accessible,
                                children_index,
                                all_rows,
                                &mut output,
                            );
                            Self::recursive_handle_insert(
                                id,
                                row,
                                descriptor.clone(),
                                base_predicate,
                                recursive_column,
                                accessible,
                                children_index,
                                all_rows,
                                &mut output,
                            );
                        }
                    }
                }

                output
            }
            _ => DeltaBatch::new(),
        }
    }

    /// Handle inserting a row into a RecursiveFilter.
    #[allow(clippy::too_many_arguments)]
    fn recursive_handle_insert(
        row_id: ObjectId,
        owned_row: OwnedRow,
        descriptor: Arc<RowDescriptor>,
        base_predicate: &Predicate,
        recursive_column: &str,
        accessible: &mut HashMap<ObjectId, AccessReason>,
        children_index: &mut HashMap<ObjectId, HashSet<ObjectId>>,
        all_rows: &mut HashMap<ObjectId, OwnedRow>,
        output: &mut DeltaBatch,
    ) {
        // Store in buffer format
        all_rows.insert(row_id, owned_row.clone());

        // Update children index: this row is a child of its parent
        if let Some(parent_id) = Self::get_ref_value_buffer(&owned_row, recursive_column, None) {
            children_index.entry(parent_id).or_default().insert(row_id);
        }

        // Check if row is accessible (use buffer-based predicate matching)
        let base_match = base_predicate.matches_buffer(row_id, owned_row.as_ref(), &descriptor);
        let parent_id = Self::get_ref_value_buffer(&owned_row, recursive_column, None);
        let parent_accessible = parent_id
            .map(|pid| accessible.contains_key(&pid))
            .unwrap_or(false);

        // Null parent is treated as accessible (root node) if base matches,
        // or if we want roots to be inherently accessible
        let is_root = parent_id.is_none();

        if base_match || parent_accessible {
            let reason = match (base_match, parent_accessible) {
                (true, true) => AccessReason::Both,
                (true, false) => AccessReason::Base,
                (false, true) => AccessReason::Inherited,
                (false, false) => unreachable!(),
            };
            accessible.insert(row_id, reason);
            // Output in buffer format
            output.push(RowDelta::Added {
                id: row_id,
                row: owned_row.clone(),
            });

            // Cascade: check if any existing rows are children of this row
            // and should now become accessible
            Self::propagate_access_to_children(
                row_id,
                &descriptor,
                base_predicate,
                accessible,
                children_index,
                all_rows,
                output,
            );
        } else if is_root {
            // Root node without base access - not accessible
            // (but still in all_rows for structure tracking)
        }
        // else: not accessible, don't add to output
    }

    /// Propagate access to children of a newly-accessible row.
    #[allow(clippy::too_many_arguments)]
    fn propagate_access_to_children(
        parent_id: ObjectId,
        descriptor: &Arc<RowDescriptor>,
        base_predicate: &Predicate,
        accessible: &mut HashMap<ObjectId, AccessReason>,
        children_index: &HashMap<ObjectId, HashSet<ObjectId>>,
        all_rows: &HashMap<ObjectId, OwnedRow>,
        output: &mut DeltaBatch,
    ) {
        if let Some(children) = children_index.get(&parent_id) {
            for &child_id in children {
                // Skip if already accessible
                if accessible.contains_key(&child_id) {
                    continue;
                }

                // Child becomes accessible via inheritance
                if let Some(owned_row) = all_rows.get(&child_id) {
                    let base_match =
                        base_predicate.matches_buffer(child_id, owned_row.as_ref(), descriptor);
                    let reason = if base_match {
                        AccessReason::Both
                    } else {
                        AccessReason::Inherited
                    };
                    accessible.insert(child_id, reason);
                    // Output in buffer format
                    output.push(RowDelta::Added {
                        id: child_id,
                        row: owned_row.clone(),
                    });

                    // Recursively propagate to grandchildren
                    Self::propagate_access_to_children(
                        child_id,
                        descriptor,
                        base_predicate,
                        accessible,
                        children_index,
                        all_rows,
                        output,
                    );
                }
            }
        }
    }

    /// Handle removing a row from a RecursiveFilter.
    #[allow(clippy::too_many_arguments)]
    fn recursive_handle_remove(
        row_id: ObjectId,
        prior: PriorState,
        recursive_column: &str,
        accessible: &mut HashMap<ObjectId, AccessReason>,
        children_index: &mut HashMap<ObjectId, HashSet<ObjectId>>,
        all_rows: &mut HashMap<ObjectId, OwnedRow>,
        output: &mut DeltaBatch,
    ) {
        // Remove from all_rows
        let removed_row = all_rows.remove(&row_id);

        // Remove from children_index (this row as a child of its parent)
        if let Some(owned_row) = &removed_row
            && let Some(parent_id) = Self::get_ref_value_buffer(owned_row, recursive_column, None)
            && let Some(siblings) = children_index.get_mut(&parent_id)
        {
            siblings.remove(&row_id);
        }

        // If row was accessible, remove it and cascade to children
        if accessible.remove(&row_id).is_some() {
            output.push(RowDelta::Removed {
                id: row_id,
                prior: prior.clone(),
            });

            // Cascade removal to children that were only accessible via this parent
            Self::propagate_removal_to_children(
                row_id,
                prior,
                accessible,
                children_index,
                all_rows,
                output,
            );
        }

        // Also remove this row's entry in children_index (as a parent)
        children_index.remove(&row_id);
    }

    /// Propagate removal to children of a removed row.
    #[allow(clippy::too_many_arguments)]
    fn propagate_removal_to_children(
        removed_parent_id: ObjectId,
        prior: PriorState,
        accessible: &mut HashMap<ObjectId, AccessReason>,
        children_index: &HashMap<ObjectId, HashSet<ObjectId>>,
        _all_rows: &HashMap<ObjectId, OwnedRow>,
        output: &mut DeltaBatch,
    ) {
        if let Some(children) = children_index.get(&removed_parent_id) {
            for &child_id in children.clone().iter() {
                if let Some(reason) = accessible.get(&child_id).copied() {
                    match reason {
                        AccessReason::Inherited => {
                            // Only accessible via this parent - loses access
                            accessible.remove(&child_id);
                            output.push(RowDelta::Removed {
                                id: child_id,
                                prior: prior.clone(),
                            });

                            // Recursively remove grandchildren
                            Self::propagate_removal_to_children(
                                child_id,
                                prior.clone(),
                                accessible,
                                children_index,
                                _all_rows,
                                output,
                            );
                        }
                        AccessReason::Both => {
                            // Still accessible via base predicate - downgrade
                            accessible.insert(child_id, AccessReason::Base);
                            // No output delta - still visible
                        }
                        AccessReason::Base => {
                            // Wasn't using parent anyway - no change
                        }
                    }
                }
            }
        }
    }

    /// Evaluate a join node using port-based routing.
    ///
    /// This is the simplified version of `evaluate_join` for use with typed edges.
    /// The `is_from_input` flag is determined by the input port:
    /// - Left/Default port: `is_from_input = true` (upstream delta)
    /// - Right port: `is_from_input = false` (join_table entry delta)
    pub fn evaluate_join_by_port(&mut self, delta: RowDelta, is_from_input: bool) -> DeltaBatch {
        match self {
            QueryNode::Join {
                input_tables,
                join_table,
                join_column,
                join_schema,
                table_descriptors,
                cached_rows,
                reverse_index,
                reverse_filter,
                left_index,
                right_index,
                right_by_ref,
                ..
            } => {
                let mut output = DeltaBatch::new();

                if is_from_input {
                    // Delta from input (left side) - use streaming indexes
                    Self::eval_join_input_delta(
                        &delta,
                        input_tables,
                        join_table,
                        join_column,
                        join_schema,
                        table_descriptors,
                        cached_rows,
                        reverse_index,
                        reverse_filter.as_ref(),
                        left_index,
                        right_index,
                        right_by_ref,
                        &mut output,
                    );
                } else {
                    // Delta from join_table (right side) - use streaming indexes
                    Self::eval_join_table_delta(
                        &delta,
                        input_tables,
                        join_table,
                        join_column,
                        join_schema,
                        table_descriptors,
                        cached_rows,
                        reverse_index,
                        reverse_filter.as_ref(),
                        left_index,
                        right_index,
                        right_by_ref,
                        &mut output,
                    );
                }

                output
            }
            _ => DeltaBatch::new(),
        }
    }

    /// Handle a delta from the input (left) side using streaming indexes.
    ///
    /// For forward joins: extract join key (referenced right ID), index in left_index,
    /// check right_index for immediate join.
    ///
    /// For reverse joins: index by left row's own ID, check right_by_ref for right rows
    /// that reference this left row.
    #[allow(clippy::too_many_arguments)]
    fn eval_join_input_delta(
        delta: &RowDelta,
        input_tables: &[String],
        join_table: &str,
        join_column: &str,
        join_schema: &TableSchema,
        table_descriptors: &HashMap<String, Arc<RowDescriptor>>,
        cached_rows: &mut HashMap<ObjectId, BufferJoinedRow>,
        reverse_index: &mut HashMap<ObjectId, HashSet<ObjectId>>,
        reverse_filter: Option<&Predicate>,
        left_index: &mut HashMap<ObjectId, HashMap<ObjectId, OwnedRow>>,
        right_index: &HashMap<ObjectId, OwnedRow>,
        right_by_ref: &HashMap<ObjectId, HashMap<ObjectId, OwnedRow>>,
        output: &mut DeltaBatch,
    ) {
        let primary_table = input_tables.first().map(|s| s.as_str()).unwrap_or("");
        let is_reverse = join_column.contains('@');
        let is_chain_join = input_tables.len() > 1;

        // For chain reverse joins, extract the target table name from join_column
        // Format: "SourceTable@TargetTable.column" -> target_table = "TargetTable"
        // (The source table has a column that references the target table's id)
        let chain_target_table = if is_chain_join && is_reverse {
            join_column
                .split('@')
                .nth(1)
                .and_then(|s| s.split('.').next())
        } else {
            None
        };

        match delta {
            RowDelta::Added {
                id: left_id,
                row: left_row,
            } => {
                if is_reverse {
                    // Reverse join: need the ID of the table that right rows reference
                    // For chain joins, look up "{TargetTable}.id" from the row
                    // For simple joins, use left_id directly
                    let lookup_id = if let Some(target_table) = chain_target_table {
                        let id_col = format!("{}.id", target_table);
                        left_row
                            .get_by_name(&id_col)
                            .and_then(|v| {
                                if let crate::sql::row_buffer::RowValue::Ref(id) = v {
                                    Some(id)
                                } else {
                                    None
                                }
                            })
                            .unwrap_or(*left_id)
                    } else {
                        *left_id
                    };

                    // Index in left_index[lookup_id]
                    left_index
                        .entry(lookup_id)
                        .or_default()
                        .insert(*left_id, left_row.clone());

                    // Check right_by_ref for right rows referencing the existing table's ID
                    if let Some(right_rows) = right_by_ref.get(&lookup_id) {
                        // Get descriptor for filter matching
                        let join_descriptor = table_descriptors
                            .get(join_table)
                            .cloned()
                            .unwrap_or_else(|| {
                                Arc::new(RowDescriptor::from_table_schema(join_schema))
                            });

                        // Filter and collect matching right rows
                        let matching_rows: Vec<_> = if let Some(filter) = reverse_filter {
                            right_rows
                                .iter()
                                .filter(|(id, row)| {
                                    filter.matches_buffer(**id, row.as_ref(), &join_descriptor)
                                })
                                .collect()
                        } else {
                            right_rows.iter().collect()
                        };

                        if !matching_rows.is_empty() {
                            // Create joined output with first matching right row
                            let mut joined = BufferJoinedRow::from_single(
                                primary_table,
                                *left_id,
                                left_row.clone(),
                            );

                            let (first_right_id, first_right_row) = matching_rows[0];
                            let qualified_right_row = first_right_row.clone();
                            joined.add_joined(join_table, *first_right_id, qualified_right_row);

                            cached_rows.insert(*left_id, joined.clone());

                            // Track all matching right rows in reverse_index (keyed by left_id for reverse joins)
                            for (right_id, _) in &matching_rows {
                                reverse_index
                                    .entry(*left_id)
                                    .or_default()
                                    .insert(**right_id);
                            }

                            output.push(RowDelta::Added {
                                id: *left_id,
                                row: joined.to_output_row(),
                            });
                        }
                        // No matching right rows → left row stays pending in left_index
                    }
                    // No right rows at all → left row stays pending in left_index
                } else {
                    // Forward join: extract join key (referenced right row ID)
                    let join_key =
                        Self::get_ref_value_buffer(left_row, join_column, Some(primary_table));

                    if let Some(join_key) = join_key {
                        // Index the left row by join key
                        left_index
                            .entry(join_key)
                            .or_default()
                            .insert(*left_id, left_row.clone());

                        // Check if right row exists
                        if let Some(right_row) = right_index.get(&join_key) {
                            let qualified_right_row = right_row.clone();

                            let mut joined = if is_chain_join {
                                let mut jr = BufferJoinedRow::new(primary_table, *left_id);
                                jr.add_joined(primary_table, *left_id, left_row.clone());
                                jr
                            } else {
                                BufferJoinedRow::from_single(
                                    primary_table,
                                    *left_id,
                                    left_row.clone(),
                                )
                            };

                            joined.add_joined(join_table, join_key, qualified_right_row);
                            cached_rows.insert(*left_id, joined.clone());
                            reverse_index.entry(join_key).or_default().insert(*left_id);

                            output.push(RowDelta::Added {
                                id: *left_id,
                                row: joined.to_output_row(),
                            });
                        }
                        // No right row → left row stays pending in left_index
                    }
                    // No join key (null ref) → don't index, no output
                }
            }

            RowDelta::Removed { id: left_id, prior } => {
                // Remove from left_index
                if is_reverse {
                    // For reverse joins, key is left_id
                    if let Some(pending) = left_index.get_mut(left_id) {
                        pending.remove(left_id);
                        if pending.is_empty() {
                            left_index.remove(left_id);
                        }
                    }
                } else {
                    // For forward joins, find the join_key by scanning left_index
                    let mut found_key = None;
                    for (key, rows) in left_index.iter() {
                        if rows.contains_key(left_id) {
                            found_key = Some(*key);
                            break;
                        }
                    }
                    if let Some(key) = found_key
                        && let Some(pending) = left_index.get_mut(&key)
                    {
                        pending.remove(left_id);
                        if pending.is_empty() {
                            left_index.remove(&key);
                        }
                    }
                }

                // Remove from cached_rows and reverse_index, emit Removed
                if cached_rows.remove(left_id).is_some() {
                    if is_reverse {
                        reverse_index.remove(left_id);
                    } else {
                        // Find and remove from reverse_index (keyed by join_id)
                        let mut found_join_id = None;
                        for (join_id, set) in reverse_index.iter_mut() {
                            if set.remove(left_id) {
                                if set.is_empty() {
                                    found_join_id = Some(*join_id);
                                }
                                break;
                            }
                        }
                        if let Some(join_id) = found_join_id {
                            reverse_index.remove(&join_id);
                        }
                    }
                    output.push(RowDelta::Removed {
                        id: *left_id,
                        prior: prior.clone(),
                    });
                }
            }

            RowDelta::Updated {
                id: left_id,
                row: left_row,
                prior,
            } => {
                // For updates, we need to handle the case where the join key changed
                let existed = cached_rows.contains_key(left_id);

                if is_reverse {
                    // Reverse join: left row's ID is the key, so key doesn't change on update
                    // Update in left_index
                    left_index
                        .entry(*left_id)
                        .or_default()
                        .insert(*left_id, left_row.clone());

                    // Re-evaluate join with right_by_ref
                    cached_rows.remove(left_id);
                    reverse_index.remove(left_id);

                    if let Some(right_rows) = right_by_ref.get(left_id) {
                        let join_descriptor = table_descriptors
                            .get(join_table)
                            .cloned()
                            .unwrap_or_else(|| {
                                Arc::new(RowDescriptor::from_table_schema(join_schema))
                            });

                        let matching_rows: Vec<_> = if let Some(filter) = reverse_filter {
                            right_rows
                                .iter()
                                .filter(|(id, row)| {
                                    filter.matches_buffer(**id, row.as_ref(), &join_descriptor)
                                })
                                .collect()
                        } else {
                            right_rows.iter().collect()
                        };

                        if !matching_rows.is_empty() {
                            let mut joined = BufferJoinedRow::from_single(
                                primary_table,
                                *left_id,
                                left_row.clone(),
                            );

                            let (first_right_id, first_right_row) = matching_rows[0];
                            let qualified_right_row = first_right_row.clone();
                            joined.add_joined(join_table, *first_right_id, qualified_right_row);

                            cached_rows.insert(*left_id, joined.clone());
                            for (right_id, _) in &matching_rows {
                                reverse_index
                                    .entry(*left_id)
                                    .or_default()
                                    .insert(**right_id);
                            }

                            let output_row = joined.to_output_row();
                            if existed {
                                output.push(RowDelta::Updated {
                                    id: *left_id,
                                    row: output_row,
                                    prior: prior.clone(),
                                });
                            } else {
                                output.push(RowDelta::Added {
                                    id: *left_id,
                                    row: output_row,
                                });
                            }
                        } else if existed {
                            output.push(RowDelta::Removed {
                                id: *left_id,
                                prior: prior.clone(),
                            });
                        }
                    } else if existed {
                        output.push(RowDelta::Removed {
                            id: *left_id,
                            prior: prior.clone(),
                        });
                    }
                } else {
                    // Forward join: join key might have changed
                    // Get old join key from cached_rows or by scanning left_index
                    let old_join_key = cached_rows
                        .get(left_id)
                        .and_then(|jr| jr.get_row_id(join_table))
                        .or_else(|| {
                            // Not cached (was pending), find in left_index
                            for (key, rows) in left_index.iter() {
                                if rows.contains_key(left_id) {
                                    return Some(*key);
                                }
                            }
                            None
                        });
                    let new_join_key =
                        Self::get_ref_value_buffer(left_row, join_column, Some(primary_table));

                    // Update left_index: remove from old key, add to new key
                    if let Some(old_key) = old_join_key
                        && let Some(pending) = left_index.get_mut(&old_key)
                    {
                        pending.remove(left_id);
                        if pending.is_empty() {
                            left_index.remove(&old_key);
                        }
                    }
                    if let Some(new_key) = new_join_key {
                        left_index
                            .entry(new_key)
                            .or_default()
                            .insert(*left_id, left_row.clone());
                    }

                    // Update reverse_index if join_id changed
                    if old_join_key != new_join_key
                        && let Some(old_key) = old_join_key
                        && let Some(set) = reverse_index.get_mut(&old_key)
                    {
                        set.remove(left_id);
                        if set.is_empty() {
                            reverse_index.remove(&old_key);
                        }
                    }

                    // Remove old cached entry
                    cached_rows.remove(left_id);

                    // Try to join with new key
                    if let Some(join_key) = new_join_key {
                        if let Some(right_row) = right_index.get(&join_key) {
                            let qualified_right_row = right_row.clone();

                            let mut joined = if is_chain_join {
                                let mut jr = BufferJoinedRow::new(primary_table, *left_id);
                                jr.add_joined(primary_table, *left_id, left_row.clone());
                                jr
                            } else {
                                BufferJoinedRow::from_single(
                                    primary_table,
                                    *left_id,
                                    left_row.clone(),
                                )
                            };

                            joined.add_joined(join_table, join_key, qualified_right_row);
                            cached_rows.insert(*left_id, joined.clone());
                            reverse_index.entry(join_key).or_default().insert(*left_id);

                            let output_row = joined.to_output_row();
                            if existed {
                                output.push(RowDelta::Updated {
                                    id: *left_id,
                                    row: output_row,
                                    prior: prior.clone(),
                                });
                            } else {
                                output.push(RowDelta::Added {
                                    id: *left_id,
                                    row: output_row,
                                });
                            }
                        } else if existed {
                            output.push(RowDelta::Removed {
                                id: *left_id,
                                prior: prior.clone(),
                            });
                        }
                    } else if existed {
                        output.push(RowDelta::Removed {
                            id: *left_id,
                            prior: prior.clone(),
                        });
                    }
                }
            }
        }
    }

    /// Handle a delta from the join_table (right) side using streaming indexes.
    ///
    /// For forward joins: index in right_index, check left_index for pending left rows.
    /// For reverse joins: also index in right_by_ref by the referenced left ID.
    #[allow(clippy::too_many_arguments)]
    fn eval_join_table_delta(
        delta: &RowDelta,
        input_tables: &[String],
        join_table: &str,
        join_column: &str,
        join_schema: &TableSchema,
        table_descriptors: &HashMap<String, Arc<RowDescriptor>>,
        cached_rows: &mut HashMap<ObjectId, BufferJoinedRow>,
        reverse_index: &mut HashMap<ObjectId, HashSet<ObjectId>>,
        reverse_filter: Option<&Predicate>,
        left_index: &HashMap<ObjectId, HashMap<ObjectId, OwnedRow>>,
        right_index: &mut HashMap<ObjectId, OwnedRow>,
        right_by_ref: &mut HashMap<ObjectId, HashMap<ObjectId, OwnedRow>>,
        output: &mut DeltaBatch,
    ) {
        let primary_table = input_tables.first().map(|s| s.as_str()).unwrap_or("");
        let is_reverse = join_column.contains('@');
        let is_chain_join = input_tables.len() > 1;
        let right_id = delta.row_id();

        // For reverse joins, extract the reference column name
        let ref_col = if is_reverse {
            join_column
                .split('@')
                .nth(1)
                .and_then(|s| s.split('.').nth(1))
        } else {
            None
        };

        match delta {
            RowDelta::Added {
                id: right_id,
                row: right_row,
            } => {
                // Index the right row
                right_index.insert(*right_id, right_row.clone());

                if is_reverse {
                    // Reverse join: index by the reference column value
                    if let Some(ref_col) = ref_col
                        && let Some(ref_value) =
                            Self::get_ref_value_buffer(right_row, ref_col, Some(join_table))
                    {
                        // Store in right_by_ref[ref_value][right_id]
                        right_by_ref
                            .entry(ref_value)
                            .or_default()
                            .insert(*right_id, right_row.clone());

                        // Check if left row exists and is waiting
                        if let Some(left_rows) = left_index.get(&ref_value) {
                            // Get descriptor for filter matching
                            let join_descriptor = table_descriptors
                                .get(join_table)
                                .cloned()
                                .unwrap_or_else(|| {
                                    Arc::new(RowDescriptor::from_table_schema(join_schema))
                                });

                            // Check if this right row passes the filter
                            let passes_filter = if let Some(filter) = reverse_filter {
                                filter.matches_buffer(
                                    *right_id,
                                    right_row.as_ref(),
                                    &join_descriptor,
                                )
                            } else {
                                true
                            };

                            if passes_filter {
                                // For each pending left row, produce joined output if not already cached
                                for (left_id, left_row) in left_rows {
                                    if !cached_rows.contains_key(left_id) {
                                        let mut joined = BufferJoinedRow::from_single(
                                            primary_table,
                                            *left_id,
                                            left_row.clone(),
                                        );

                                        let qualified_right_row = right_row.clone();
                                        joined.add_joined(
                                            join_table,
                                            *right_id,
                                            qualified_right_row,
                                        );

                                        cached_rows.insert(*left_id, joined.clone());
                                        reverse_index
                                            .entry(*left_id)
                                            .or_default()
                                            .insert(*right_id);

                                        output.push(RowDelta::Added {
                                            id: *left_id,
                                            row: joined.to_output_row(),
                                        });
                                    } else {
                                        // Already cached, just add to reverse_index
                                        reverse_index
                                            .entry(*left_id)
                                            .or_default()
                                            .insert(*right_id);
                                    }
                                }
                            }
                        }
                    }
                } else {
                    // Forward join: check left_index for pending left rows waiting for this right_id
                    if let Some(pending_lefts) = left_index.get(right_id) {
                        let qualified_right_row = right_row.clone();

                        for (left_id, left_row) in pending_lefts {
                            // Skip if already cached (shouldn't happen, but be safe)
                            if cached_rows.contains_key(left_id) {
                                continue;
                            }

                            let mut joined = if is_chain_join {
                                let mut jr = BufferJoinedRow::new(primary_table, *left_id);
                                jr.add_joined(primary_table, *left_id, left_row.clone());
                                jr
                            } else {
                                BufferJoinedRow::from_single(
                                    primary_table,
                                    *left_id,
                                    left_row.clone(),
                                )
                            };

                            joined.add_joined(join_table, *right_id, qualified_right_row.clone());
                            cached_rows.insert(*left_id, joined.clone());
                            reverse_index.entry(*right_id).or_default().insert(*left_id);

                            output.push(RowDelta::Added {
                                id: *left_id,
                                row: joined.to_output_row(),
                            });
                        }
                    }
                }
            }

            RowDelta::Removed { prior, .. } => {
                // Remove from right_index
                right_index.remove(&right_id);

                if is_reverse {
                    // Remove from right_by_ref - find by scanning
                    let mut found_ref_value = None;
                    for (ref_value, ref_map) in right_by_ref.iter() {
                        if ref_map.contains_key(&right_id) {
                            found_ref_value = Some(*ref_value);
                            break;
                        }
                    }
                    if let Some(ref_value) = found_ref_value
                        && let Some(ref_map) = right_by_ref.get_mut(&ref_value)
                    {
                        ref_map.remove(&right_id);
                        if ref_map.is_empty() {
                            right_by_ref.remove(&ref_value);
                        }
                    }

                    // For reverse joins, reverse_index is keyed by left_id
                    // Find affected left rows and potentially remove them from output
                    let mut affected_left_ids = Vec::new();
                    for (left_id, right_ids) in reverse_index.iter_mut() {
                        if right_ids.remove(&right_id) {
                            affected_left_ids.push(*left_id);
                        }
                    }

                    for left_id in affected_left_ids {
                        if let Some(right_ids) = reverse_index.get(&left_id)
                            && right_ids.is_empty()
                        {
                            // No more right rows for this left row
                            reverse_index.remove(&left_id);
                            if cached_rows.remove(&left_id).is_some() {
                                output.push(RowDelta::Removed {
                                    id: left_id,
                                    prior: prior.clone(),
                                });
                            }
                        }
                        // Else: still has other right rows, keep the cached output
                    }
                } else {
                    // Forward join: reverse_index is keyed by right_id
                    if let Some(affected_left_ids) = reverse_index.remove(&right_id) {
                        for left_id in affected_left_ids {
                            if cached_rows.remove(&left_id).is_some() {
                                output.push(RowDelta::Removed {
                                    id: left_id,
                                    prior: prior.clone(),
                                });
                            }
                        }
                    }
                }
            }

            RowDelta::Updated {
                id: right_id,
                row: right_row,
                prior,
            } => {
                // Update right_index
                right_index.insert(*right_id, right_row.clone());

                if is_reverse {
                    // Handle reference value change
                    if let Some(ref_col) = ref_col {
                        // Find old ref_value by scanning right_by_ref
                        let old_ref_value = {
                            let mut found = None;
                            for (ref_value, ref_map) in right_by_ref.iter() {
                                if ref_map.contains_key(right_id) {
                                    found = Some(*ref_value);
                                    break;
                                }
                            }
                            found
                        };
                        let new_ref_value =
                            Self::get_ref_value_buffer(right_row, ref_col, Some(join_table));

                        // Update right_by_ref
                        if let Some(old_ref) = old_ref_value
                            && let Some(ref_map) = right_by_ref.get_mut(&old_ref)
                        {
                            ref_map.remove(right_id);
                            if ref_map.is_empty() {
                                right_by_ref.remove(&old_ref);
                            }
                        }
                        if let Some(new_ref) = new_ref_value {
                            right_by_ref
                                .entry(new_ref)
                                .or_default()
                                .insert(*right_id, right_row.clone());
                        }

                        // Handle reference change affecting joins
                        if old_ref_value != new_ref_value {
                            // Remove old joins
                            if let Some(old_ref) = old_ref_value
                                && let Some(right_ids) = reverse_index.get_mut(&old_ref)
                                && right_ids.remove(right_id)
                                && right_ids.is_empty()
                            {
                                reverse_index.remove(&old_ref);
                                if cached_rows.remove(&old_ref).is_some() {
                                    output.push(RowDelta::Removed {
                                        id: old_ref,
                                        prior: prior.clone(),
                                    });
                                }
                            }

                            // Add new joins if left row exists
                            if let Some(new_ref) = new_ref_value
                                && let Some(left_rows) = left_index.get(&new_ref)
                            {
                                let join_descriptor =
                                    table_descriptors.get(join_table).cloned().unwrap_or_else(
                                        || Arc::new(RowDescriptor::from_table_schema(join_schema)),
                                    );

                                let passes_filter = if let Some(filter) = reverse_filter {
                                    filter.matches_buffer(
                                        *right_id,
                                        right_row.as_ref(),
                                        &join_descriptor,
                                    )
                                } else {
                                    true
                                };

                                if passes_filter {
                                    for (left_id, left_row) in left_rows {
                                        if !cached_rows.contains_key(left_id) {
                                            let mut joined = BufferJoinedRow::from_single(
                                                primary_table,
                                                *left_id,
                                                left_row.clone(),
                                            );

                                            let qualified_right_row = right_row.clone();
                                            joined.add_joined(
                                                join_table,
                                                *right_id,
                                                qualified_right_row,
                                            );

                                            cached_rows.insert(*left_id, joined.clone());
                                            reverse_index
                                                .entry(*left_id)
                                                .or_default()
                                                .insert(*right_id);

                                            output.push(RowDelta::Added {
                                                id: *left_id,
                                                row: joined.to_output_row(),
                                            });
                                        }
                                    }
                                }
                            }
                        } else {
                            // Reference didn't change, update existing joins
                            // Find affected left rows via reverse_index
                            let mut affected_left_ids = Vec::new();
                            for (left_id, right_ids) in reverse_index.iter() {
                                if right_ids.contains(right_id) {
                                    affected_left_ids.push(*left_id);
                                }
                            }

                            for left_id in affected_left_ids {
                                if let Some(old_joined) = cached_rows.get(&left_id) {
                                    let mut new_joined = old_joined.clone();
                                    let qualified_right_row = right_row.clone();
                                    new_joined.add_joined(
                                        join_table,
                                        *right_id,
                                        qualified_right_row,
                                    );

                                    cached_rows.insert(left_id, new_joined.clone());
                                    output.push(RowDelta::Updated {
                                        id: left_id,
                                        row: new_joined.to_output_row(),
                                        prior: prior.clone(),
                                    });
                                }
                            }
                        }
                    }
                } else {
                    // Forward join: update all cached rows that used this right row
                    if let Some(affected_left_ids) = reverse_index.get(right_id) {
                        let qualified_right_row = right_row.clone();

                        for left_id in affected_left_ids.clone() {
                            if let Some(old_joined) = cached_rows.get(&left_id) {
                                let mut new_joined = old_joined.clone();
                                new_joined.add_joined(
                                    join_table,
                                    *right_id,
                                    qualified_right_row.clone(),
                                );

                                cached_rows.insert(left_id, new_joined.clone());
                                output.push(RowDelta::Updated {
                                    id: left_id,
                                    row: new_joined.to_output_row(),
                                    prior: prior.clone(),
                                });
                            }
                        }
                    }
                }
            }
        }
    }

    /// Extract a Ref value from a buffer row by column name.
    /// Handles both plain Ref values and nullable Ref values.
    /// If `table` is provided, tries looking up "table.column" first, then falls back to just "column".
    fn get_ref_value_buffer(row: &OwnedRow, column: &str, table: Option<&str>) -> Option<ObjectId> {
        use crate::sql::row_buffer::RowValue;

        // If column is already qualified (contains '.'), use it directly
        // Otherwise, try qualified name (with table prefix) first, then unqualified
        let value = if column.contains('.') {
            // Already qualified (e.g., "folders.workspace_id")
            row.get_by_name(column)
        } else if let Some(tbl) = table {
            // Try qualified name first, then fall back to unqualified
            let qualified = format!("{}.{}", tbl, column);
            row.get_by_name(&qualified)
                .or_else(|| row.get_by_name(column))
        } else {
            row.get_by_name(column)
        };

        match value? {
            RowValue::Ref(id) => Some(id),
            RowValue::Null => None,
            _ => None,
        }
    }

    /// Evaluate an ArrayAggregate node using port-based routing.
    ///
    /// Handles two types of deltas:
    /// - Outer table deltas (from input node): Add/remove/update outer rows
    /// - Inner table deltas: Update the arrays for affected outer rows
    ///
    /// The `is_outer_delta`/`is_inner_delta` flags are determined by the input port:
    /// - Outer/Default port: `is_outer_delta = true` (upstream delta)
    /// - Inner port: `is_inner_delta = true` (inner_table entry delta)
    ///
    /// `lookup_inner_rows` finds all inner rows matching an outer id.
    /// `lookup_row_by_id` looks up a row from any table by (table_name, id).
    pub fn evaluate_array_aggregate_by_port<F, G>(
        &mut self,
        delta: RowDelta,
        is_outer_delta: bool,
        is_inner_delta: bool,
        _outer_schema: &TableSchema,
        lookup_inner_rows: F,
        lookup_row_by_id: G,
    ) -> DeltaBatch
    where
        F: Fn(ObjectId) -> Vec<(ObjectId, OwnedRow)>,
        G: Fn(&str, ObjectId) -> Option<OwnedRow>,
    {
        match self {
            QueryNode::ArrayAggregate {
                inner_ref_column,
                inner_schema,
                inner_descriptor,
                inner_joins,
                output_descriptor,
                array_column_index,
                cached_arrays,
                inner_to_outer,
                outer_rows,
                ..
            } => {
                let mut output = DeltaBatch::new();

                if is_outer_delta {
                    // Delta from outer table - add/remove/update outer rows
                    Self::array_aggregate_handle_outer_delta(
                        &delta,
                        output_descriptor,
                        inner_descriptor,
                        inner_schema,
                        inner_joins,
                        *array_column_index,
                        cached_arrays,
                        inner_to_outer,
                        outer_rows,
                        &mut output,
                        &lookup_inner_rows,
                        &lookup_row_by_id,
                    );
                } else if is_inner_delta {
                    // Delta from inner table - update arrays for affected outer rows
                    Self::array_aggregate_handle_inner_delta(
                        &delta,
                        inner_ref_column,
                        inner_schema,
                        inner_joins,
                        inner_descriptor,
                        output_descriptor,
                        *array_column_index,
                        cached_arrays,
                        inner_to_outer,
                        outer_rows,
                        &mut output,
                        &lookup_row_by_id,
                    );
                }

                output
            }
            _ => DeltaBatch::new(),
        }
    }

    /// Handle a delta from the outer table in ArrayAggregate.
    #[allow(clippy::too_many_arguments)]
    fn array_aggregate_handle_outer_delta<F, G>(
        delta: &RowDelta,
        output_descriptor: &Arc<RowDescriptor>,
        inner_descriptor: &Arc<RowDescriptor>,
        inner_schema: &TableSchema,
        inner_joins: &[(String, String, TableSchema)],
        array_column_index: i32,
        cached_arrays: &mut HashMap<ObjectId, HashMap<ObjectId, OwnedRow>>,
        inner_to_outer: &mut HashMap<ObjectId, ObjectId>,
        outer_rows: &mut HashMap<ObjectId, OwnedRow>,
        output: &mut DeltaBatch,
        lookup_inner_rows: F,
        lookup_row_by_id: G,
    ) where
        F: Fn(ObjectId) -> Vec<(ObjectId, OwnedRow)>,
        G: Fn(&str, ObjectId) -> Option<OwnedRow>,
    {
        match delta {
            RowDelta::Added {
                id: outer_id,
                row: owned_row,
            } => {
                // Fetch all matching inner rows (already in buffer format)
                let raw_inner_rows = lookup_inner_rows(*outer_id);

                // Update inner_to_outer index
                for (inner_id, _) in &raw_inner_rows {
                    inner_to_outer.insert(*inner_id, *outer_id);
                }

                // Resolve inner joins (e.g., replace label ref with full Labels row)
                let resolved_rows = Self::resolve_inner_joins_buffer(
                    &raw_inner_rows,
                    inner_joins,
                    inner_schema,
                    &lookup_row_by_id,
                );

                // Store as HashMap<inner_id, OwnedRow>
                let inner_map: HashMap<ObjectId, OwnedRow> = raw_inner_rows
                    .iter()
                    .zip(resolved_rows.iter())
                    .map(|((inner_id, _), resolved)| (*inner_id, resolved.clone()))
                    .collect();
                let inner_rows: Vec<OwnedRow> = inner_map.values().cloned().collect();
                cached_arrays.insert(*outer_id, inner_map);

                // Build output row with array
                let output_row = Self::build_output_row_with_array_buffer(
                    owned_row,
                    &inner_rows,
                    array_column_index,
                    output_descriptor.clone(),
                    inner_descriptor.clone(),
                );

                outer_rows.insert(*outer_id, output_row.clone());

                output.push(RowDelta::Added {
                    id: *outer_id,
                    row: output_row,
                });
            }

            RowDelta::Removed { id, prior } => {
                let outer_id = *id;

                // Clean up inner_to_outer index (remove all entries for this outer)
                if let Some(inner_map) = cached_arrays.remove(&outer_id) {
                    for inner_id in inner_map.keys() {
                        inner_to_outer.remove(inner_id);
                    }
                }

                outer_rows.remove(&outer_id);

                output.push(RowDelta::Removed {
                    id: outer_id,
                    prior: prior.clone(),
                });
            }

            RowDelta::Updated {
                id,
                row: owned_row,
                prior,
            } => {
                let outer_id = *id;

                // Fetch updated inner rows
                let raw_inner_rows = lookup_inner_rows(outer_id);

                // Update inner_to_outer index
                if let Some(inner_map) = cached_arrays.get(&outer_id) {
                    for inner_id in inner_map.keys() {
                        inner_to_outer.remove(inner_id);
                    }
                }
                for (inner_id, _) in &raw_inner_rows {
                    inner_to_outer.insert(*inner_id, outer_id);
                }

                // Resolve inner joins
                let resolved_rows = Self::resolve_inner_joins_buffer(
                    &raw_inner_rows,
                    inner_joins,
                    inner_schema,
                    &lookup_row_by_id,
                );

                // Store as HashMap<inner_id, OwnedRow>
                let inner_map: HashMap<ObjectId, OwnedRow> = raw_inner_rows
                    .iter()
                    .zip(resolved_rows.iter())
                    .map(|((inner_id, _), resolved)| (*inner_id, resolved.clone()))
                    .collect();
                let inner_rows: Vec<OwnedRow> = inner_map.values().cloned().collect();
                cached_arrays.insert(outer_id, inner_map);

                let output_row = Self::build_output_row_with_array_buffer(
                    owned_row,
                    &inner_rows,
                    array_column_index,
                    output_descriptor.clone(),
                    inner_descriptor.clone(),
                );
                outer_rows.insert(outer_id, output_row.clone());

                output.push(RowDelta::Updated {
                    id: outer_id,
                    row: output_row,
                    prior: prior.clone(),
                });
            }
        }
    }

    /// Resolve inner joins for array rows (buffer format).
    /// For each inner row, replace ref columns with their resolved row values.
    /// Writes directly to RowBuilder instead of using intermediate Vec<Value>.
    fn resolve_inner_joins_buffer<G>(
        inner_rows: &[(ObjectId, OwnedRow)],
        inner_joins: &[(String, String, TableSchema)],
        inner_schema: &TableSchema,
        lookup_row_by_id: G,
    ) -> Vec<OwnedRow>
    where
        G: Fn(&str, ObjectId) -> Option<OwnedRow>,
    {
        use crate::sql::row_buffer::{RowBuilder, RowDescriptor, RowValue};

        if inner_joins.is_empty() {
            return inner_rows.iter().map(|(_, row)| row.clone()).collect();
        }

        // Helper to find value by name, trying both qualified and unqualified names
        // The row might have qualified names like "IssueLabels.label" when delta flows through outer JOINs
        fn find_column_name<'a>(row: &'a OwnedRow, col_name: &'a str) -> Option<&'a str> {
            // First try exact match
            if row.descriptor.columns.iter().any(|c| c.name == col_name) {
                return Some(col_name);
            }
            // If not found, try to find a qualified column that ends with ".{col_name}"
            let suffix = format!(".{}", col_name);
            for col in &row.descriptor.columns {
                if col.name.ends_with(&suffix) {
                    return Some(&col.name);
                }
            }
            None
        }

        inner_rows
            .iter()
            .map(|(_, row)| {
                // First pass: resolve refs to get target row descriptors for the output schema
                let mut resolved_targets: std::collections::HashMap<&str, OwnedRow> =
                    std::collections::HashMap::new();

                for (ref_column, target_table, _) in inner_joins {
                    // Try to find the column (might be qualified like "IssueLabels.label")
                    if let Some(actual_col_name) = find_column_name(row, ref_column)
                        && let Some(RowValue::Ref(target_id)) = row.get_by_name(actual_col_name)
                        && let Some(target_row) = lookup_row_by_id(target_table, target_id)
                    {
                        resolved_targets.insert(ref_column.as_str(), target_row);
                    }
                }

                // Build the resolved descriptor with Array types for joined columns
                let new_cols: Vec<(String, ColumnType, bool)> = inner_schema
                    .columns
                    .iter()
                    .map(|col| {
                        if let Some(target_row) = resolved_targets.get(col.name.as_str()) {
                            // This column is resolved to a nested row - use Array type
                            (
                                col.name.clone(),
                                ColumnType::Array(target_row.descriptor.clone()),
                                false,
                            )
                        } else {
                            (col.name.clone(), col.ty.clone(), col.nullable)
                        }
                    })
                    .collect();
                let resolved_descriptor = Arc::new(RowDescriptor::new_ordered(new_cols));

                // Build the output row directly
                let mut builder = RowBuilder::new(resolved_descriptor.clone());
                for (schema_idx, col) in inner_schema.columns.iter().enumerate() {
                    let col_idx = resolved_descriptor
                        .columns
                        .iter()
                        .position(|c| c.schema_index == schema_idx)
                        .unwrap_or(schema_idx);

                    if let Some(target_row) = resolved_targets.get(col.name.as_str()) {
                        // Set as single-item array containing the resolved row
                        builder = builder.set_array(col_idx, std::slice::from_ref(target_row));
                    } else if let Some(actual_col_name) = find_column_name(row, &col.name) {
                        // Copy the value directly (using flexible lookup for qualified names)
                        if let Some(rv) = row.get_by_name(actual_col_name) {
                            builder = builder.set_from_row_value(col_idx, rv);
                        }
                    }
                }

                builder.build()
            })
            .collect()
    }

    /// Handle a delta from the inner table in ArrayAggregate.
    /// Uses buffer format throughout.
    #[allow(clippy::too_many_arguments)]
    fn array_aggregate_handle_inner_delta<G>(
        delta: &RowDelta,
        inner_ref_column: &str,
        inner_schema: &TableSchema,
        inner_joins: &[(String, String, TableSchema)],
        inner_descriptor: &Arc<RowDescriptor>,
        output_descriptor: &Arc<RowDescriptor>,
        array_column_index: i32,
        cached_arrays: &mut HashMap<ObjectId, HashMap<ObjectId, OwnedRow>>,
        inner_to_outer: &mut HashMap<ObjectId, ObjectId>,
        outer_rows: &mut HashMap<ObjectId, OwnedRow>,
        output: &mut DeltaBatch,
        lookup_row_by_id: G,
    ) where
        G: Fn(&str, ObjectId) -> Option<OwnedRow>,
    {
        // Helper to resolve a single inner row
        let resolve_inner_row = |row: &OwnedRow| -> OwnedRow {
            if inner_joins.is_empty() {
                return row.clone();
            }
            let rows = vec![(ObjectId::default(), row.clone())];
            let resolved = Self::resolve_inner_joins_buffer(
                &rows,
                inner_joins,
                inner_schema,
                &lookup_row_by_id,
            );
            resolved.into_iter().next().unwrap_or_else(|| row.clone())
        };

        // Helper to rebuild output row with updated array (from HashMap values)
        let rebuild_output = |base_row: &OwnedRow,
                              inner_map: &HashMap<ObjectId, OwnedRow>,
                              out_desc: Arc<RowDescriptor>,
                              inner_desc: Arc<RowDescriptor>|
         -> OwnedRow {
            let array: Vec<OwnedRow> = inner_map.values().cloned().collect();
            Self::build_output_row_with_array_buffer(
                base_row,
                &array,
                array_column_index,
                out_desc,
                inner_desc,
            )
        };

        match delta {
            RowDelta::Added {
                id: inner_id,
                row: inner_row,
            } => {
                // Find which outer row this belongs to by looking up the ref column
                if let Some(outer_id) =
                    Self::get_ref_value_from_buffer(inner_row, inner_ref_column, inner_descriptor)
                {
                    // Update inner_to_outer index
                    inner_to_outer.insert(*inner_id, outer_id);

                    // Add resolved row to cached array (keyed by inner_id)
                    let resolved_row = resolve_inner_row(inner_row);
                    let inner_map = cached_arrays.entry(outer_id).or_default();
                    inner_map.insert(*inner_id, resolved_row);

                    // Emit updated delta for outer row
                    if let Some(base_outer_row) = outer_rows.get(&outer_id) {
                        let output_row = rebuild_output(
                            base_outer_row,
                            inner_map,
                            output_descriptor.clone(),
                            inner_descriptor.clone(),
                        );
                        outer_rows.insert(outer_id, output_row.clone());

                        output.push(RowDelta::Updated {
                            id: outer_id,
                            row: output_row,
                            prior: PriorState::empty(),
                        });
                    }
                }
            }

            RowDelta::Removed {
                id: inner_id,
                prior,
            } => {
                // Find which outer row this belonged to
                if let Some(outer_id) = inner_to_outer.remove(inner_id) {
                    // Remove from cached array by inner_id
                    if let Some(inner_map) = cached_arrays.get_mut(&outer_id) {
                        inner_map.remove(inner_id);

                        // Emit updated delta for outer row
                        if let Some(base_outer_row) = outer_rows.get(&outer_id) {
                            let output_row = rebuild_output(
                                base_outer_row,
                                inner_map,
                                output_descriptor.clone(),
                                inner_descriptor.clone(),
                            );
                            outer_rows.insert(outer_id, output_row.clone());

                            output.push(RowDelta::Updated {
                                id: outer_id,
                                row: output_row,
                                prior: prior.clone(),
                            });
                        }
                    }
                }
            }

            RowDelta::Updated {
                id: inner_id,
                row: inner_row,
                prior,
            } => {
                let old_outer_id = inner_to_outer.get(inner_id).copied();
                let new_outer_id =
                    Self::get_ref_value_from_buffer(inner_row, inner_ref_column, inner_descriptor);

                if old_outer_id != new_outer_id {
                    // Inner row moved to different outer row
                    // Remove from old outer
                    if let Some(old_id) = old_outer_id {
                        inner_to_outer.remove(inner_id);
                        if let Some(inner_map) = cached_arrays.get_mut(&old_id) {
                            inner_map.remove(inner_id);

                            if let Some(base_outer_row) = outer_rows.get(&old_id) {
                                let output_row = rebuild_output(
                                    base_outer_row,
                                    inner_map,
                                    output_descriptor.clone(),
                                    inner_descriptor.clone(),
                                );
                                outer_rows.insert(old_id, output_row.clone());

                                output.push(RowDelta::Updated {
                                    id: old_id,
                                    row: output_row,
                                    prior: prior.clone(),
                                });
                            }
                        }
                    }

                    // Add to new outer
                    if let Some(new_id) = new_outer_id {
                        inner_to_outer.insert(*inner_id, new_id);
                        let resolved_row = resolve_inner_row(inner_row);
                        let inner_map = cached_arrays.entry(new_id).or_default();
                        inner_map.insert(*inner_id, resolved_row);

                        if let Some(base_outer_row) = outer_rows.get(&new_id) {
                            let output_row = rebuild_output(
                                base_outer_row,
                                inner_map,
                                output_descriptor.clone(),
                                inner_descriptor.clone(),
                            );
                            outer_rows.insert(new_id, output_row.clone());

                            output.push(RowDelta::Updated {
                                id: new_id,
                                row: output_row,
                                prior: PriorState::empty(),
                            });
                        }
                    }
                } else if let Some(outer_id) = new_outer_id {
                    // Same outer row - update in place (replace old entry with new)
                    let resolved_row = resolve_inner_row(inner_row);
                    let inner_map = cached_arrays.entry(outer_id).or_default();
                    inner_map.insert(*inner_id, resolved_row); // Replaces existing entry

                    if let Some(base_outer_row) = outer_rows.get(&outer_id) {
                        let output_row = rebuild_output(
                            base_outer_row,
                            inner_map,
                            output_descriptor.clone(),
                            inner_descriptor.clone(),
                        );
                        outer_rows.insert(outer_id, output_row.clone());

                        output.push(RowDelta::Updated {
                            id: outer_id,
                            row: output_row,
                            prior: prior.clone(),
                        });
                    }
                }
            }
        }
    }

    /// Get a Ref value from an OwnedRow by column name.
    /// Tries looking up by name directly on the row first (handles joined rows with their own descriptors),
    /// then falls back to using the passed descriptor's index.
    fn get_ref_value_from_buffer(
        row: &OwnedRow,
        column: &str,
        descriptor: &RowDescriptor,
    ) -> Option<ObjectId> {
        // First try direct name lookup on the row (handles joined rows)
        if let Some(RowValue::Ref(id)) = row.get_by_name(column) {
            return Some(id);
        }

        // Fall back to descriptor-based index lookup
        if let Some(idx) = descriptor.column_index(column)
            && let Some(RowValue::Ref(id)) = row.get(idx)
        {
            return Some(id);
        }
        None
    }

    /// Build an output row with array column using buffer format.
    fn build_output_row_with_array_buffer(
        outer_row: &OwnedRow,
        inner_rows: &[OwnedRow],
        array_column_index: i32,
        output_descriptor: Arc<RowDescriptor>,
        _inner_descriptor: Arc<RowDescriptor>,
    ) -> OwnedRow {
        use crate::sql::row_buffer::RowBuilder;

        let mut builder = RowBuilder::new(output_descriptor.clone());

        // array_column_index should already be the actual index (computed during graph building)
        // This is the index in output_descriptor where this ArrayAggregate's array column is
        let array_idx = array_column_index as usize;

        // Copy columns from outer_row to output
        // Skip only the array column we're about to set (other arrays should be preserved)
        let mut out_idx = 0;
        for col in &output_descriptor.columns {
            if out_idx == array_idx {
                // This is the array column position - skip it, we'll set it later
                out_idx += 1;
                continue;
            }

            // Find the corresponding column in outer_row by name
            if let Some(value) = outer_row.get_by_name(&col.name) {
                // Copy the value (including any existing arrays from previous ArrayAggregates)
                builder = builder.set_from_row_value(out_idx, value);
            }
            out_idx += 1;
        }

        // Set the array column
        builder = builder.set_array(array_idx, inner_rows);

        builder.build()
    }

    /// Evaluate a node that just passes through IDs while tracking membership.
    fn eval_id_passthrough(cached_ids: &mut HashSet<ObjectId>, input: DeltaBatch) -> DeltaBatch {
        let mut output = DeltaBatch::new();

        for delta in input.into_iter() {
            match &delta {
                RowDelta::Added { id, .. } => {
                    if cached_ids.insert(*id) {
                        output.push(delta);
                    }
                }
                RowDelta::Removed { id, .. } => {
                    if cached_ids.remove(id) {
                        output.push(delta);
                    }
                }
                RowDelta::Updated { id, .. } => {
                    if cached_ids.contains(id) {
                        output.push(delta);
                    }
                }
            }
        }

        output
    }

    /// Evaluate BranchMerge as a passthrough node, updating object_states.
    ///
    /// This enables BranchMerge to participate in delta routing through the graph
    /// (for ARRAY/JOIN queries) while maintaining per-object state tracking.
    fn eval_branch_merge_passthrough(
        object_states: &mut HashMap<ObjectId, MergedObjectState>,
        input: DeltaBatch,
    ) -> DeltaBatch {
        let mut output = DeltaBatch::new();

        for delta in input.into_iter() {
            match &delta {
                RowDelta::Added { id, row } => {
                    // Track new object
                    object_states.insert(
                        *id,
                        MergedObjectState {
                            cached_merged: Some(row.clone()),
                        },
                    );
                    output.push(delta);
                }
                RowDelta::Removed { id, .. } => {
                    // Remove tracked object
                    if object_states.remove(id).is_some() {
                        output.push(delta);
                    }
                }
                RowDelta::Updated { id, row, .. } => {
                    // Update tracked object
                    if object_states.contains_key(id) {
                        object_states.insert(
                            *id,
                            MergedObjectState {
                                cached_merged: Some(row.clone()),
                            },
                        );
                        output.push(delta);
                    }
                }
            }
        }

        output
    }

    /// Evaluate a filter node using buffer format directly.
    fn eval_filter(
        predicate: &Predicate,
        cached_ids: &mut HashSet<ObjectId>,
        input: DeltaBatch,
    ) -> DeltaBatch {
        let mut output = DeltaBatch::new();

        for delta in input.into_iter() {
            match delta {
                RowDelta::Added { id, row } => {
                    // Use the row's own descriptor - it knows its own layout
                    // The row is self-describing and contains all the info needed for filtering
                    let row_descriptor = &row.descriptor;
                    if predicate.matches_buffer(id, row.as_ref(), row_descriptor) {
                        cached_ids.insert(id);
                        output.push(RowDelta::Added { id, row });
                    }
                }

                RowDelta::Removed { id, prior } => {
                    // Only emit removal if it was in our cached set
                    if cached_ids.remove(&id) {
                        output.push(RowDelta::Removed { id, prior });
                    }
                }

                RowDelta::Updated { id, row, prior } => {
                    let was_in_set = cached_ids.contains(&id);
                    // Use the row's own descriptor
                    let row_descriptor = &row.descriptor;
                    let is_match = predicate.matches_buffer(id, row.as_ref(), row_descriptor);

                    match (was_in_set, is_match) {
                        (false, true) => {
                            // Row now matches the filter - enters the set
                            cached_ids.insert(id);
                            output.push(RowDelta::Added { id, row });
                        }
                        (true, false) => {
                            // Row no longer matches - leaves the set
                            cached_ids.remove(&id);
                            output.push(RowDelta::Removed { id, prior });
                        }
                        (true, true) => {
                            // Row still matches - propagate update
                            output.push(RowDelta::Updated { id, row, prior });
                        }
                        (false, false) => {
                            // Row still doesn't match - no output (early cutoff)
                        }
                    }
                }
            }
        }

        output
    }

    /// Evaluate a Filter node with optional lens transformation.
    ///
    /// When a lens context is provided, rows from older schema versions are
    /// transformed to the target schema before predicate evaluation. Rows that
    /// cannot be transformed (incompatible) are excluded from results.
    ///
    /// # Arguments
    ///
    /// * `predicate` - The filter predicate to evaluate
    /// * `cached_ids` - Set of row IDs that currently match the filter
    /// * `input` - Batch of row deltas to process
    /// * `lens_ctx` - Optional lens context for schema transformation
    /// * `get_row_descriptor` - Function to get the source descriptor ID for a row
    ///
    /// # Schema Transformation Flow
    ///
    /// 1. For each row, get its source schema version (descriptor ID)
    /// 2. If source != target and lens_ctx is available, transform the row
    /// 3. Evaluate predicate on the (possibly transformed) row
    /// 4. If transformation fails, treat as non-matching (exclude from results)
    pub fn eval_filter_with_lens<F>(
        predicate: &Predicate,
        cached_ids: &mut HashSet<ObjectId>,
        input: DeltaBatch,
        lens_ctx: Option<&QueryLensContext>,
        get_row_descriptor: F,
    ) -> DeltaBatch
    where
        F: Fn(&ObjectId) -> Option<DescriptorId>,
    {
        let mut output = DeltaBatch::new();

        for delta in input.into_iter() {
            match delta {
                RowDelta::Added { id, row } => {
                    // Try to transform the row if lens context is available
                    let (eval_row, transformed) = match lens_ctx {
                        Some(ctx) => {
                            // Get the source descriptor for this row
                            if let Some(source_desc) = get_row_descriptor(&id) {
                                // Try to transform to target schema
                                match ctx.transform_to_target(&row, &source_desc) {
                                    Ok(transformed_row) => (transformed_row, true),
                                    Err(_) => {
                                        // Row is incompatible - exclude from results
                                        continue;
                                    }
                                }
                            } else {
                                // No source descriptor info - use row as-is
                                (row.clone(), false)
                            }
                        }
                        None => (row.clone(), false),
                    };

                    // Evaluate predicate on the (possibly transformed) row
                    let row_descriptor = &eval_row.descriptor;
                    if predicate.matches_buffer(id, eval_row.as_ref(), row_descriptor) {
                        cached_ids.insert(id);
                        // Output the original row (not transformed) to preserve original data
                        // The transformation is only for predicate evaluation
                        output.push(RowDelta::Added {
                            id,
                            row: if transformed { eval_row } else { row },
                        });
                    }
                }

                RowDelta::Removed { id, prior } => {
                    // Only emit removal if it was in our cached set
                    if cached_ids.remove(&id) {
                        output.push(RowDelta::Removed { id, prior });
                    }
                }

                RowDelta::Updated { id, row, prior } => {
                    let was_in_set = cached_ids.contains(&id);

                    // Try to transform the row if lens context is available
                    let (eval_row, transformed) = match lens_ctx {
                        Some(ctx) => {
                            if let Some(source_desc) = get_row_descriptor(&id) {
                                match ctx.transform_to_target(&row, &source_desc) {
                                    Ok(transformed_row) => (transformed_row, true),
                                    Err(_) => {
                                        // Row is now incompatible
                                        if was_in_set {
                                            // Was in set but now incompatible - remove it
                                            cached_ids.remove(&id);
                                            output.push(RowDelta::Removed { id, prior });
                                        }
                                        continue;
                                    }
                                }
                            } else {
                                (row.clone(), false)
                            }
                        }
                        None => (row.clone(), false),
                    };

                    let row_descriptor = &eval_row.descriptor;
                    let is_match = predicate.matches_buffer(id, eval_row.as_ref(), row_descriptor);
                    let output_row = if transformed { eval_row } else { row };

                    match (was_in_set, is_match) {
                        (false, true) => {
                            cached_ids.insert(id);
                            output.push(RowDelta::Added {
                                id,
                                row: output_row,
                            });
                        }
                        (true, false) => {
                            cached_ids.remove(&id);
                            output.push(RowDelta::Removed { id, prior });
                        }
                        (true, true) => {
                            output.push(RowDelta::Updated {
                                id,
                                row: output_row,
                                prior,
                            });
                        }
                        (false, false) => {
                            // Row still doesn't match - no output (early cutoff)
                        }
                    }
                }
            }
        }

        output
    }

    /// Evaluate a LimitOffset node.
    ///
    /// Maintains the full set of qualifying rows and emits deltas
    /// when the visible window changes. Uses buffer format (OwnedRow) throughout.
    pub fn evaluate_limit_offset(
        &mut self,
        input: DeltaBatch,
        _schema: &TableSchema,
        _cache: &RowCache,
    ) -> DeltaBatch {
        match self {
            QueryNode::LimitOffset {
                limit,
                offset,
                all_rows,
                visible_ids,
                ..
            } => {
                let mut output = DeltaBatch::new();

                for delta in input.into_iter() {
                    match delta {
                        RowDelta::Added { id, row } => {
                            Self::limit_offset_handle_add(
                                id,
                                row,
                                *limit,
                                *offset,
                                all_rows,
                                visible_ids,
                                &mut output,
                            );
                        }
                        RowDelta::Removed { id, prior } => {
                            Self::limit_offset_handle_remove(
                                id,
                                prior,
                                *limit,
                                *offset,
                                all_rows,
                                visible_ids,
                                &mut output,
                            );
                        }
                        RowDelta::Updated { id, row, prior } => {
                            // Update the stored row if it exists
                            if let std::collections::btree_map::Entry::Occupied(mut e) =
                                all_rows.entry(id)
                            {
                                e.insert(row.clone());
                                // If the row is visible, emit the update
                                if visible_ids.contains(&id) {
                                    output.push(RowDelta::Updated { id, row, prior });
                                }
                            }
                        }
                    }
                }

                output
            }
            _ => DeltaBatch::new(),
        }
    }

    /// Handle a row being added to the LimitOffset node.
    #[allow(clippy::too_many_arguments)]
    fn limit_offset_handle_add(
        row_id: ObjectId,
        row: OwnedRow,
        limit: Option<u64>,
        offset: u64,
        all_rows: &mut BTreeMap<ObjectId, OwnedRow>,
        visible_ids: &mut HashSet<ObjectId>,
        output: &mut DeltaBatch,
    ) {
        all_rows.insert(row_id, row);

        // Compute the new visible window and changes
        let (new_visible, changes) =
            Self::compute_window_changes(all_rows, visible_ids, limit, offset);

        // Emit deltas for changes
        for (id, change_type) in changes {
            match change_type {
                WindowChange::Added => {
                    if let Some(owned_row) = all_rows.get(&id) {
                        output.push(RowDelta::Added {
                            id,
                            row: owned_row.clone(),
                        });
                    }
                }
                WindowChange::Removed => {
                    output.push(RowDelta::Removed {
                        id,
                        prior: PriorState::empty(),
                    });
                }
            }
        }

        *visible_ids = new_visible;
    }

    /// Handle a row being removed from the LimitOffset node.
    #[allow(clippy::too_many_arguments)]
    fn limit_offset_handle_remove(
        id: ObjectId,
        prior: PriorState,
        limit: Option<u64>,
        offset: u64,
        all_rows: &mut BTreeMap<ObjectId, OwnedRow>,
        visible_ids: &mut HashSet<ObjectId>,
        output: &mut DeltaBatch,
    ) {
        let was_visible = visible_ids.contains(&id);
        all_rows.remove(&id);
        visible_ids.remove(&id);

        if was_visible {
            // Row was in visible window - emit removal
            output.push(RowDelta::Removed { id, prior });
        }

        // Recompute window - a row might be promoted or demoted
        let (new_visible, changes) =
            Self::compute_window_changes(all_rows, visible_ids, limit, offset);

        for (changed_id, change_type) in changes {
            match change_type {
                WindowChange::Added => {
                    if let Some(owned_row) = all_rows.get(&changed_id) {
                        output.push(RowDelta::Added {
                            id: changed_id,
                            row: owned_row.clone(),
                        });
                    }
                }
                WindowChange::Removed => {
                    if changed_id != id {
                        // Only emit if not already removed above
                        output.push(RowDelta::Removed {
                            id: changed_id,
                            prior: PriorState::empty(),
                        });
                    }
                }
            }
        }

        *visible_ids = new_visible;
    }

    /// Compute the new visible window and return changes from the current state.
    fn compute_window_changes(
        all_rows: &BTreeMap<ObjectId, OwnedRow>,
        current_visible: &HashSet<ObjectId>,
        limit: Option<u64>,
        offset: u64,
    ) -> (HashSet<ObjectId>, Vec<(ObjectId, WindowChange)>) {
        let offset = offset as usize;
        let limit = limit.map(|l| l as usize);

        // Compute new visible set using BTreeMap's sorted iteration
        let new_visible: HashSet<ObjectId> = all_rows
            .keys()
            .skip(offset)
            .take(limit.unwrap_or(usize::MAX))
            .copied()
            .collect();

        let mut changes = Vec::new();

        // Find removed (was visible, now not)
        for id in current_visible {
            if !new_visible.contains(id) {
                changes.push((*id, WindowChange::Removed));
            }
        }

        // Find added (was not visible, now is)
        for id in &new_visible {
            if !current_visible.contains(id) {
                changes.push((*id, WindowChange::Added));
            }
        }

        (new_visible, changes)
    }

    /// Evaluate a BranchMerge node using pre-computed per-column metadata.
    ///
    /// This is the legacy version without lens support. Use `evaluate_branch_merge_with_lenses`
    /// for cross-schema branch merges.
    ///
    /// # Algorithm
    ///
    /// 1. Collect frontier commits from all branches
    /// 2. For each frontier commit, get its ColumnChanges metadata
    /// 3. For each column, find the commit with the latest timestamp
    /// 4. Read the column value from the winning commit's content
    /// 5. Build merged row and emit delta if changed
    ///
    /// This runs in O(branches × frontier_commits × columns) without DAG traversal.
    pub fn evaluate_branch_merge_with_metadata(
        &mut self,
        object_id: ObjectId,
        object: &Object,
    ) -> DeltaBatch {
        match self {
            QueryNode::BranchMerge {
                branch_names,
                descriptor,
                object_states,
                ..
            } => {
                let mut output = DeltaBatch::new();

                // Collect frontier data from all branches:
                // (commit_id, content, column_changes)
                // We clone the data to avoid lifetime issues with branch guards
                let mut frontier_data: Vec<(CommitId, Vec<u8>, ColumnChanges)> = vec![];

                for branch_name in branch_names.iter() {
                    if let Some(branch) = object.branch(branch_name) {
                        let frontier_changes = branch.frontier_changes();
                        for commit_id in branch.frontier() {
                            if let Some(commit) = branch.get_commit(commit_id)
                                && let Some(changes) = frontier_changes.get(commit_id)
                            {
                                frontier_data.push((
                                    *commit_id,
                                    commit.content.to_vec(),
                                    changes.clone(),
                                ));
                            }
                        }
                    }
                }

                if frontier_data.is_empty() {
                    // No data from any branch - object might have been removed
                    if let Some(state) = object_states.get(&object_id)
                        && state.cached_merged.is_some()
                    {
                        output.push(RowDelta::Removed {
                            id: object_id,
                            prior: PriorState::empty(),
                        });
                    }
                    object_states.remove(&object_id);
                    return output;
                }

                // Single frontier commit: no merge needed, just use it directly
                if frontier_data.len() == 1 {
                    let (_, content, _) = &frontier_data[0];
                    let row = OwnedRow::new(descriptor.clone(), content.to_vec());

                    // Get or create state
                    let state = object_states.entry(object_id).or_default();

                    // Early cutoff: check if result changed
                    if let Some(prev) = &state.cached_merged
                        && prev.buffer == row.buffer
                    {
                        return output; // No change
                    }

                    let was_new = state.cached_merged.is_none();
                    state.cached_merged = Some(row.clone());

                    if was_new {
                        output.push(RowDelta::Added { id: object_id, row });
                    } else {
                        output.push(RowDelta::Updated {
                            id: object_id,
                            row,
                            prior: PriorState::empty(),
                        });
                    }

                    return output;
                }

                // Multiple frontier commits: merge using per-column LWW based on metadata
                let merged = Self::merge_with_metadata(&frontier_data, descriptor);

                // Get or create state
                let state = object_states.entry(object_id).or_default();

                // Early cutoff: check if result changed
                if let Some(prev) = &state.cached_merged
                    && prev.buffer == merged.buffer
                {
                    return output; // No change
                }

                let was_new = state.cached_merged.is_none();
                state.cached_merged = Some(merged.clone());

                if was_new {
                    output.push(RowDelta::Added {
                        id: object_id,
                        row: merged,
                    });
                } else {
                    output.push(RowDelta::Updated {
                        id: object_id,
                        row: merged,
                        prior: PriorState::empty(),
                    });
                }

                output
            }
            _ => DeltaBatch::new(),
        }
    }

    /// Evaluate a BranchMerge node with lens support for cross-schema merges.
    ///
    /// Similar to `evaluate_branch_merge_with_metadata`, but transforms row content
    /// and column change metadata through lenses when branches have different schemas.
    ///
    /// # Arguments
    ///
    /// * `object_id` - The object to evaluate
    /// * `object` - Reference to the Object (for reading branch data)
    /// * `lens_context` - Registry of lenses for schema transformations
    /// * `descriptor_lookup` - Function to look up RowDescriptor by DescriptorId
    ///
    /// # Algorithm
    ///
    /// 1. For each branch, parse name to extract source schema (DescriptorId)
    /// 2. If source schema differs from target, transform content and metadata through lens
    /// 3. Merge using per-column LWW on transformed data
    pub fn evaluate_branch_merge_with_lenses<F>(
        &mut self,
        object_id: ObjectId,
        object: &Object,
        lens_context: &LensContext,
        descriptor_lookup: F,
    ) -> DeltaBatch
    where
        F: Fn(DescriptorId) -> Option<Arc<RowDescriptor>>,
    {
        match self {
            QueryNode::BranchMerge {
                branch_names,
                descriptor,
                target_descriptor_id,
                object_states,
                ..
            } => {
                let mut output = DeltaBatch::new();

                // Collect frontier data from all branches, transforming through lenses as needed
                let mut frontier_data: Vec<(CommitId, Vec<u8>, ColumnChanges)> = vec![];

                for branch_name in branch_names.iter() {
                    // Parse branch name to get source schema (if available)
                    let parsed = SchemaBranchName::parse(branch_name);
                    let source_desc_id = parsed.descriptor_id();

                    if let Some(branch) = object.branch(branch_name) {
                        let frontier_changes = branch.frontier_changes();

                        for commit_id in branch.frontier() {
                            if let Some(commit) = branch.get_commit(commit_id) {
                                // Get column changes if available, otherwise use empty
                                // (simple branches like "main" may not have metadata tracking)
                                let changes =
                                    frontier_changes.get(commit_id).cloned().unwrap_or_default();

                                // Skip empty content (deleted objects)
                                if commit.content.is_empty() {
                                    #[cfg(debug_assertions)]
                                    eprintln!(
                                        "[BranchMerge] Skipping commit {:?} on branch '{}': \
                                         empty content (deleted object)",
                                        commit_id, branch_name
                                    );
                                    continue;
                                }

                                // Determine if transformation needed
                                // No transform needed when:
                                // - target_descriptor_id is None (single-schema query)
                                // - source_desc_id is None (simple branch like "main")
                                // - source == target
                                let needs_transform = matches!(
                                    (source_desc_id, target_descriptor_id.as_ref()),
                                    (Some(src), Some(tgt)) if src != *tgt
                                );

                                let (transformed_content, transformed_changes) = if needs_transform
                                {
                                    let source_id = source_desc_id.unwrap();
                                    let target_id = target_descriptor_id.as_ref().unwrap();

                                    // Different schema: transform through lens
                                    let lens = match lens_context.get_lens(&source_id, target_id) {
                                        Some(l) => l,
                                        None => {
                                            #[cfg(debug_assertions)]
                                            eprintln!(
                                                "[BranchMerge] Skipping commit {:?} on branch '{}': \
                                                 no lens found from {:?} to {:?}",
                                                commit_id, branch_name, source_id, target_id
                                            );
                                            continue;
                                        }
                                    };

                                    let src_desc = match descriptor_lookup(source_id) {
                                        Some(d) => d,
                                        None => {
                                            #[cfg(debug_assertions)]
                                            eprintln!(
                                                "[BranchMerge] Skipping commit {:?} on branch '{}': \
                                                 source descriptor {:?} not found",
                                                commit_id, branch_name, source_id
                                            );
                                            continue;
                                        }
                                    };

                                    let content = match lens
                                        .transform_buffer_forward(&commit.content, &src_desc)
                                    {
                                        Ok(c) => c,
                                        Err(e) => {
                                            #[cfg(debug_assertions)]
                                            eprintln!(
                                                "[BranchMerge] Skipping commit {:?} on branch '{}': \
                                                 lens transform failed: {:?}",
                                                commit_id, branch_name, e
                                            );
                                            continue;
                                        }
                                    };

                                    let changes = Self::transform_column_changes(&changes, lens);
                                    (content, changes)
                                } else {
                                    // Same schema or no schema info: no transform needed
                                    (commit.content.to_vec(), changes)
                                };

                                frontier_data.push((
                                    *commit_id,
                                    transformed_content,
                                    transformed_changes,
                                ));
                            }
                        }
                    }
                }

                if frontier_data.is_empty() {
                    // No data from any branch - object might have been removed
                    if let Some(state) = object_states.get(&object_id)
                        && state.cached_merged.is_some()
                    {
                        output.push(RowDelta::Removed {
                            id: object_id,
                            prior: PriorState::empty(),
                        });
                    }
                    object_states.remove(&object_id);
                    return output;
                }

                // Single frontier commit: no merge needed, just use it directly
                if frontier_data.len() == 1 {
                    let (_, content, _) = &frontier_data[0];
                    let row = OwnedRow::new(descriptor.clone(), content.to_vec());

                    let state = object_states.entry(object_id).or_default();

                    // Early cutoff: check if result changed
                    if let Some(prev) = &state.cached_merged
                        && prev.buffer == row.buffer
                    {
                        return output;
                    }

                    let was_new = state.cached_merged.is_none();
                    state.cached_merged = Some(row.clone());

                    if was_new {
                        output.push(RowDelta::Added { id: object_id, row });
                    } else {
                        output.push(RowDelta::Updated {
                            id: object_id,
                            row,
                            prior: PriorState::empty(),
                        });
                    }

                    return output;
                }

                // Multiple frontier commits: merge using per-column LWW based on metadata
                let merged = Self::merge_with_metadata(&frontier_data, descriptor);

                let state = object_states.entry(object_id).or_default();

                // Early cutoff: check if result changed
                if let Some(prev) = &state.cached_merged
                    && prev.buffer == merged.buffer
                {
                    return output;
                }

                let was_new = state.cached_merged.is_none();
                state.cached_merged = Some(merged.clone());

                if was_new {
                    output.push(RowDelta::Added {
                        id: object_id,
                        row: merged,
                    });
                } else {
                    output.push(RowDelta::Updated {
                        id: object_id,
                        row: merged,
                        prior: PriorState::empty(),
                    });
                }

                output
            }
            _ => DeltaBatch::new(),
        }
    }

    /// Transform column change metadata through a lens.
    ///
    /// When merging across schema versions, the column names in the metadata
    /// need to be transformed to match the target schema. This uses the lens's
    /// rename mappings to translate column names.
    ///
    /// For example, if a lens renames "title" → "name", metadata for "title"
    /// becomes metadata for "name".
    fn transform_column_changes(changes: &ColumnChanges, lens: &Lens) -> ColumnChanges {
        let mapping = ColumnMapping::from_transforms(&lens.forward);

        changes
            .iter()
            .map(|(col_name, change)| {
                let new_name = mapping.map_forward(col_name).to_string();
                (new_name, change.clone())
            })
            .collect()
    }

    /// Merge frontier commits using per-column LWW based on metadata.
    ///
    /// For each column, finds the frontier commit with the latest change timestamp
    /// and uses that commit's value for the column.
    fn merge_with_metadata(
        frontier_data: &[(CommitId, Vec<u8>, ColumnChanges)],
        descriptor: &Arc<RowDescriptor>,
    ) -> OwnedRow {
        use crate::sql::RowBuilder;
        use crate::sql::row_buffer::RowRef;

        let mut builder = RowBuilder::new(descriptor.clone());

        for col in &descriptor.columns {
            let col_name = &col.name;

            // Find which frontier has the latest change for this column
            let winner: Option<(u64, &str, &[u8])> = frontier_data
                .iter()
                .filter_map(|(_, content, changes)| {
                    changes
                        .get(col_name)
                        .map(|c| (c.timestamp, c.author.as_str(), content.as_slice()))
                })
                .max_by(|(ts_a, author_a, _), (ts_b, author_b, _)| {
                    // Primary: timestamp, Secondary: author (lexicographic for determinism)
                    ts_a.cmp(ts_b).then_with(|| author_a.cmp(author_b))
                });

            if let Some((_, _, content)) = winner {
                let row_ref = RowRef::new(descriptor, content);
                if let Some(value) = row_ref.get_by_name(col_name)
                    && let Some(col_idx) = descriptor.column_index(col_name)
                {
                    builder = builder.set_from_row_value(col_idx, value);
                }
            }
        }

        builder.build()
    }
}

/// Represents a change to the visible window in a LimitOffset node.
enum WindowChange {
    Added,
    Removed,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::object::ObjectId;
    use crate::sql::query_graph::PredicateValue;
    use crate::sql::row_buffer::RowBuilder;
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

    fn test_descriptor() -> Arc<RowDescriptor> {
        Arc::new(RowDescriptor::from_table_schema(&test_schema()))
    }

    fn make_owned_row(id: u128, name: &str, active: bool) -> (ObjectId, OwnedRow) {
        let descriptor = test_descriptor();
        let row = RowBuilder::new(descriptor)
            .set_string_by_name("name", name)
            .set_bool_by_name("active", active)
            .build();
        (ObjectId::new(id), row)
    }

    #[test]
    fn table_scan_add() {
        let mut node = QueryNode::TableScan {
            table: "users".to_string(),
            cached_ids: HashSet::new(),
        };

        let cache = RowCache::new();

        let (id, row) = make_owned_row(1, "Alice", true);
        let delta = DeltaBatch::added(id, row);

        let output = node.evaluate(delta, &cache);

        assert_eq!(output.len(), 1);
        assert!(node.cached_ids().unwrap().contains(&ObjectId::new(1)));
    }

    #[test]
    fn table_scan_remove() {
        let mut node = QueryNode::TableScan {
            table: "users".to_string(),
            cached_ids: HashSet::from([ObjectId::new(1)]),
        };

        let cache = RowCache::new();

        let delta = DeltaBatch::removed(ObjectId::new(1), vec![]);

        let output = node.evaluate(delta, &cache);

        assert_eq!(output.len(), 1);
        assert!(!node.cached_ids().unwrap().contains(&ObjectId::new(1)));
    }

    #[test]
    fn table_scan_remove_not_present() {
        let mut node = QueryNode::TableScan {
            table: "users".to_string(),
            cached_ids: HashSet::new(),
        };

        let cache = RowCache::new();

        let delta = DeltaBatch::removed(ObjectId::new(1), vec![]);

        let output = node.evaluate(delta, &cache);

        // Should produce no output since ID wasn't in set
        assert!(output.is_empty());
    }

    #[test]
    fn filter_add_match() {
        let mut node = QueryNode::Filter {
            table: "users".to_string(),
            input: NodeId(0),
            predicate: Predicate::eq("active", PredicateValue::Bool(true)),
            descriptor: test_descriptor(),
            cached_ids: HashSet::new(),
        };

        let cache = RowCache::new();

        let (id, row) = make_owned_row(1, "Alice", true);
        let delta = DeltaBatch::added(id, row);

        let output = node.evaluate(delta, &cache);

        assert_eq!(output.len(), 1);
        assert!(node.cached_ids().unwrap().contains(&ObjectId::new(1)));
    }

    #[test]
    fn filter_add_no_match() {
        let mut node = QueryNode::Filter {
            table: "users".to_string(),
            input: NodeId(0),
            predicate: Predicate::eq("active", PredicateValue::Bool(true)),
            descriptor: test_descriptor(),
            cached_ids: HashSet::new(),
        };

        let cache = RowCache::new();

        let (id, row) = make_owned_row(1, "Alice", false); // active = false
        let delta = DeltaBatch::added(id, row);

        let output = node.evaluate(delta, &cache);

        // Early cutoff - no output
        assert!(output.is_empty());
        assert!(!node.cached_ids().unwrap().contains(&ObjectId::new(1)));
    }

    #[test]
    fn filter_update_enters_set() {
        let mut node = QueryNode::Filter {
            table: "users".to_string(),
            input: NodeId(0),
            predicate: Predicate::eq("active", PredicateValue::Bool(true)),
            descriptor: test_descriptor(),
            cached_ids: HashSet::new(), // Not in set initially
        };

        let cache = RowCache::new();

        // Update: was inactive, now active
        let (_, new_row) = make_owned_row(1, "Alice", true);
        let delta = DeltaBatch::updated(ObjectId::new(1), new_row, vec![]);

        let output = node.evaluate(delta, &cache);

        assert_eq!(output.len(), 1);
        // Should be Added since it entered the filtered set
        assert!(matches!(output.iter().next(), Some(RowDelta::Added { .. })));
        assert!(node.cached_ids().unwrap().contains(&ObjectId::new(1)));
    }

    #[test]
    fn filter_update_leaves_set() {
        let mut node = QueryNode::Filter {
            table: "users".to_string(),
            input: NodeId(0),
            predicate: Predicate::eq("active", PredicateValue::Bool(true)),
            descriptor: test_descriptor(),
            cached_ids: HashSet::from([ObjectId::new(1)]), // In set initially
        };

        let cache = RowCache::new();

        // Update: was active, now inactive
        let (_, new_row) = make_owned_row(1, "Alice", false);
        let delta = DeltaBatch::updated(ObjectId::new(1), new_row, vec![]);

        let output = node.evaluate(delta, &cache);

        assert_eq!(output.len(), 1);
        // Should be Removed since it left the filtered set
        assert!(matches!(
            output.iter().next(),
            Some(RowDelta::Removed { .. })
        ));
        assert!(!node.cached_ids().unwrap().contains(&ObjectId::new(1)));
    }

    #[test]
    fn filter_update_stays_in_set() {
        let mut node = QueryNode::Filter {
            table: "users".to_string(),
            input: NodeId(0),
            predicate: Predicate::eq("active", PredicateValue::Bool(true)),
            descriptor: test_descriptor(),
            cached_ids: HashSet::from([ObjectId::new(1)]),
        };

        let cache = RowCache::new();

        // Update: still active, just name change
        let (_, new_row) = make_owned_row(1, "Alicia", true);
        let delta = DeltaBatch::updated(ObjectId::new(1), new_row, vec![]);

        let output = node.evaluate(delta, &cache);

        assert_eq!(output.len(), 1);
        // Should be Updated since it stayed in the set
        assert!(matches!(
            output.iter().next(),
            Some(RowDelta::Updated { .. })
        ));
    }

    #[test]
    fn filter_update_stays_out_of_set() {
        let mut node = QueryNode::Filter {
            table: "users".to_string(),
            input: NodeId(0),
            predicate: Predicate::eq("active", PredicateValue::Bool(true)),
            descriptor: test_descriptor(),
            cached_ids: HashSet::new(),
        };

        let cache = RowCache::new();

        // Update: still inactive
        let (_, new_row) = make_owned_row(1, "Alicia", false);
        let delta = DeltaBatch::updated(ObjectId::new(1), new_row, vec![]);

        let output = node.evaluate(delta, &cache);

        // Early cutoff - no output since row never matched
        assert!(output.is_empty());
    }

    // --- RecursiveFilter tests ---

    fn folder_schema() -> TableSchema {
        TableSchema::new(
            "folders",
            vec![
                ColumnDef::required("name", ColumnType::String),
                ColumnDef::required("owner_id", ColumnType::Ref("users".to_string())),
                ColumnDef::optional("parent_id", ColumnType::Ref("folders".to_string())),
            ],
        )
    }

    const ALICE: u128 = 100;
    const BOB: u128 = 200;

    fn folder_descriptor() -> Arc<RowDescriptor> {
        Arc::new(RowDescriptor::from_table_schema(&folder_schema()))
    }

    fn make_owned_folder(
        id: u128,
        name: &str,
        owner_id: u128,
        parent_id: Option<u128>,
    ) -> (ObjectId, OwnedRow) {
        let descriptor = folder_descriptor();
        let builder = RowBuilder::new(descriptor)
            .set_string_by_name("name", name)
            .set_ref_by_name("owner_id", ObjectId::new(owner_id));
        let row = match parent_id {
            Some(p) => builder.set_ref_by_name("parent_id", ObjectId::new(p)),
            None => builder.set_null_by_name("parent_id"),
        }
        .build();
        (ObjectId::new(id), row)
    }

    #[test]
    fn recursive_filter_base_access() {
        // Test: root folder owned by viewer is accessible
        let viewer = ObjectId::new(ALICE);

        let mut node = QueryNode::RecursiveFilter {
            table: "folders".to_string(),
            input: NodeId(0),
            base_predicate: Predicate::eq("owner_id", PredicateValue::Ref(viewer)),
            recursive_column: "parent_id".to_string(),
            descriptor: folder_descriptor(),
            accessible: HashMap::new(),
            children_index: HashMap::new(),
            all_rows: HashMap::new(),
        };

        // Add a root folder owned by Alice
        let (id, row) = make_owned_folder(1, "root", ALICE, None);
        let delta = DeltaBatch::added(id, row);

        let output = node.evaluate_recursive(delta);

        assert_eq!(output.len(), 1);
        assert!(matches!(output.iter().next(), Some(RowDelta::Added { id, .. }) if id.0 == 1));
        assert!(node.accessible().unwrap().contains_key(&ObjectId::new(1)));
        assert_eq!(
            node.accessible().unwrap().get(&ObjectId::new(1)),
            Some(&AccessReason::Base)
        );
    }

    #[test]
    fn recursive_filter_no_base_access() {
        // Test: folder owned by someone else is not accessible
        let viewer = ObjectId::new(ALICE);

        let mut node = QueryNode::RecursiveFilter {
            table: "folders".to_string(),
            input: NodeId(0),
            base_predicate: Predicate::eq("owner_id", PredicateValue::Ref(viewer)),
            recursive_column: "parent_id".to_string(),
            descriptor: folder_descriptor(),
            accessible: HashMap::new(),
            children_index: HashMap::new(),
            all_rows: HashMap::new(),
        };

        // Add a root folder owned by Bob (not Alice)
        let (id, row) = make_owned_folder(1, "bobs-folder", BOB, None);
        let delta = DeltaBatch::added(id, row);

        let output = node.evaluate_recursive(delta);

        assert!(output.is_empty());
        assert!(!node.accessible().unwrap().contains_key(&ObjectId::new(1)));
    }

    #[test]
    fn recursive_filter_inherited_access() {
        // Test: child folder inherits access from parent
        let viewer = ObjectId::new(ALICE);

        let mut node = QueryNode::RecursiveFilter {
            table: "folders".to_string(),
            input: NodeId(0),
            base_predicate: Predicate::eq("owner_id", PredicateValue::Ref(viewer)),
            recursive_column: "parent_id".to_string(),
            descriptor: folder_descriptor(),
            accessible: HashMap::new(),
            children_index: HashMap::new(),
            all_rows: HashMap::new(),
        };

        // Add root folder owned by Alice
        let (id, row) = make_owned_folder(1, "root", ALICE, None);
        let delta = DeltaBatch::added(id, row);
        node.evaluate_recursive(delta);

        // Add child folder owned by Bob but parented to Alice's folder
        let (id, row) = make_owned_folder(2, "child", BOB, Some(1));
        let delta = DeltaBatch::added(id, row);
        let output = node.evaluate_recursive(delta);

        assert_eq!(output.len(), 1);
        assert!(node.accessible().unwrap().contains_key(&ObjectId::new(2)));
        assert_eq!(
            node.accessible().unwrap().get(&ObjectId::new(2)),
            Some(&AccessReason::Inherited)
        );
    }

    #[test]
    fn recursive_filter_cascading_access() {
        // Test: grandchild becomes accessible when parent is added
        let viewer = ObjectId::new(ALICE);

        let mut node = QueryNode::RecursiveFilter {
            table: "folders".to_string(),
            input: NodeId(0),
            base_predicate: Predicate::eq("owner_id", PredicateValue::Ref(viewer)),
            recursive_column: "parent_id".to_string(),
            descriptor: folder_descriptor(),
            accessible: HashMap::new(),
            children_index: HashMap::new(),
            all_rows: HashMap::new(),
        };

        // Add grandchild first (parent doesn't exist yet)
        let (id, row) = make_owned_folder(3, "grandchild", BOB, Some(2));
        let delta = DeltaBatch::added(id, row);
        let output = node.evaluate_recursive(delta);
        assert!(output.is_empty()); // Not yet accessible

        // Add child (parent doesn't exist yet)
        let (id, row) = make_owned_folder(2, "child", BOB, Some(1));
        let delta = DeltaBatch::added(id, row);
        let output = node.evaluate_recursive(delta);
        assert!(output.is_empty()); // Still not accessible

        // Add root owned by Alice - should cascade to child and grandchild
        let (id, row) = make_owned_folder(1, "root", ALICE, None);
        let delta = DeltaBatch::added(id, row);
        let output = node.evaluate_recursive(delta);

        // Should have 3 added deltas: root + child + grandchild
        assert_eq!(output.len(), 3);
        assert!(node.accessible().unwrap().contains_key(&ObjectId::new(1)));
        assert!(node.accessible().unwrap().contains_key(&ObjectId::new(2)));
        assert!(node.accessible().unwrap().contains_key(&ObjectId::new(3)));
    }

    #[test]
    fn recursive_filter_removal_cascades() {
        // Test: removing parent cascades removal to children
        let viewer = ObjectId::new(ALICE);

        let mut node = QueryNode::RecursiveFilter {
            table: "folders".to_string(),
            input: NodeId(0),
            base_predicate: Predicate::eq("owner_id", PredicateValue::Ref(viewer)),
            recursive_column: "parent_id".to_string(),
            descriptor: folder_descriptor(),
            accessible: HashMap::new(),
            children_index: HashMap::new(),
            all_rows: HashMap::new(),
        };

        // Set up: root -> child -> grandchild
        let (id, row) = make_owned_folder(1, "root", ALICE, None);
        node.evaluate_recursive(DeltaBatch::added(id, row));
        let (id, row) = make_owned_folder(2, "child", BOB, Some(1));
        node.evaluate_recursive(DeltaBatch::added(id, row));
        let (id, row) = make_owned_folder(3, "grandchild", BOB, Some(2));
        node.evaluate_recursive(DeltaBatch::added(id, row));

        assert_eq!(node.accessible().unwrap().len(), 3);

        // Remove root - should cascade to child and grandchild
        let delta = DeltaBatch::removed(ObjectId::new(1), vec![]);
        let output = node.evaluate_recursive(delta);

        // Should have 3 removed deltas
        assert_eq!(output.len(), 3);
        assert!(node.accessible().unwrap().is_empty());
    }

    #[test]
    fn recursive_filter_child_keeps_base_access_after_parent_removal() {
        // Test: child with base access keeps it when parent is removed
        let viewer = ObjectId::new(ALICE);

        let mut node = QueryNode::RecursiveFilter {
            table: "folders".to_string(),
            input: NodeId(0),
            base_predicate: Predicate::eq("owner_id", PredicateValue::Ref(viewer)),
            recursive_column: "parent_id".to_string(),
            descriptor: folder_descriptor(),
            accessible: HashMap::new(),
            children_index: HashMap::new(),
            all_rows: HashMap::new(),
        };

        // Root owned by Alice
        let (id, row) = make_owned_folder(1, "root", ALICE, None);
        node.evaluate_recursive(DeltaBatch::added(id, row));
        // Child also owned by Alice (Both access)
        let (id, row) = make_owned_folder(2, "child", ALICE, Some(1));
        node.evaluate_recursive(DeltaBatch::added(id, row));

        assert_eq!(
            node.accessible().unwrap().get(&ObjectId::new(2)),
            Some(&AccessReason::Both)
        );

        // Remove root
        let delta = DeltaBatch::removed(ObjectId::new(1), vec![]);
        let output = node.evaluate_recursive(delta);

        // Only root should be removed, child keeps access
        assert_eq!(output.len(), 1);
        assert!(!node.accessible().unwrap().contains_key(&ObjectId::new(1)));
        assert!(node.accessible().unwrap().contains_key(&ObjectId::new(2)));
        // Access reason should be downgraded to Base
        assert_eq!(
            node.accessible().unwrap().get(&ObjectId::new(2)),
            Some(&AccessReason::Base)
        );
    }

    // --- Lens-aware filter tests ---

    #[test]
    fn filter_with_lens_transforms_row() {
        use crate::sql::lens::{ColumnTransform, Lens, LensContext, QueryLensContext};

        // Create two schema versions:
        // v1: { name: String, active: Bool }
        // v2: { title: String, active: Bool }  (name renamed to title)
        let v1_desc = Arc::new(RowDescriptor::new([
            ("name".to_string(), ColumnType::String, false),
            ("active".to_string(), ColumnType::Bool, false),
        ]));

        let _v2_desc = Arc::new(RowDescriptor::new([
            ("title".to_string(), ColumnType::String, false),
            ("active".to_string(), ColumnType::Bool, false),
        ]));

        // Create lens for v1 -> v2 (rename name -> title)
        let lens = Lens::from_forward(vec![ColumnTransform::rename("name", "title")]);

        // Create descriptor IDs
        let desc_v1 = DescriptorId::from_object_id(ObjectId::new(100));
        let desc_v2 = DescriptorId::from_object_id(ObjectId::new(200));

        // Build lens context
        let mut lens_ctx_inner = LensContext::new();
        lens_ctx_inner.register_lens(desc_v1, desc_v2, lens);
        let lens_ctx = QueryLensContext::with_lenses(desc_v2, lens_ctx_inner);

        // Create filter predicate that filters by title = "Alice"
        // This won't match v1 rows directly (they have 'name' not 'title')
        let predicate = Predicate::eq("title", PredicateValue::String("Alice".to_string()));
        let mut cached_ids = HashSet::new();

        // Create a v1 row with name="Alice"
        let row = RowBuilder::new(v1_desc)
            .set_string_by_name("name", "Alice")
            .set_bool_by_name("active", true)
            .build();
        let id = ObjectId::new(1);

        // Without lens, the row doesn't match (no 'title' column)
        let input_no_lens = DeltaBatch::added(id, row.clone());
        let output_no_lens = QueryNode::eval_filter_with_lens(
            &predicate,
            &mut cached_ids,
            input_no_lens,
            None,
            |_| None,
        );
        assert!(
            output_no_lens.is_empty(),
            "Without lens, v1 row shouldn't match v2 predicate"
        );

        // With lens, the row should be transformed and match
        cached_ids.clear();
        let input_with_lens = DeltaBatch::added(id, row);
        let output_with_lens = QueryNode::eval_filter_with_lens(
            &predicate,
            &mut cached_ids,
            input_with_lens,
            Some(&lens_ctx),
            |_| Some(desc_v1), // All rows are at v1
        );

        assert_eq!(
            output_with_lens.len(),
            1,
            "With lens, transformed row should match"
        );
        assert!(cached_ids.contains(&id));

        // Verify the output row has 'title' (transformed)
        if let Some(RowDelta::Added {
            row: output_row, ..
        }) = output_with_lens.iter().next()
        {
            assert_eq!(
                output_row.get_by_name("title"),
                Some(RowValue::String("Alice"))
            );
        } else {
            panic!("Expected Added delta");
        }
    }

    #[test]
    fn filter_with_lens_excludes_incompatible_rows() {
        use crate::sql::lens::{LensContext, QueryLensContext};

        // Create a row and predicate
        let predicate = Predicate::eq("active", PredicateValue::Bool(true));
        let mut cached_ids = HashSet::new();

        // Create descriptor IDs with no lens between them
        let desc_v1 = DescriptorId::from_object_id(ObjectId::new(100));
        let desc_v2 = DescriptorId::from_object_id(ObjectId::new(200));

        // Build lens context with no lenses (incompatible schemas)
        let lens_ctx = QueryLensContext::with_lenses(desc_v2, LensContext::new());

        let (id, row) = make_owned_row(1, "Alice", true);
        let input = DeltaBatch::added(id, row);

        // Row is at v1, target is v2, no lens -> should be excluded
        let output = QueryNode::eval_filter_with_lens(
            &predicate,
            &mut cached_ids,
            input,
            Some(&lens_ctx),
            |_| Some(desc_v1), // All rows are at v1, but no lens to v2
        );

        // Row is incompatible (no lens) so excluded
        assert!(output.is_empty());
        assert!(!cached_ids.contains(&id));
    }

    #[test]
    fn filter_with_lens_passes_through_same_version() {
        use crate::sql::lens::{LensContext, QueryLensContext};

        // Create a row at the target version (no transformation needed)
        let predicate = Predicate::eq("active", PredicateValue::Bool(true));
        let mut cached_ids = HashSet::new();

        let desc_v2 = DescriptorId::from_object_id(ObjectId::new(200));

        // Build lens context - target is v2
        let lens_ctx = QueryLensContext::with_lenses(desc_v2, LensContext::new());

        let (id, row) = make_owned_row(1, "Alice", true);
        let input = DeltaBatch::added(id, row);

        // Row is at v2, target is v2 -> no transformation, just evaluate
        let output = QueryNode::eval_filter_with_lens(
            &predicate,
            &mut cached_ids,
            input,
            Some(&lens_ctx),
            |_| Some(desc_v2), // Row is at target version
        );

        assert_eq!(output.len(), 1, "Same version row should pass through");
        assert!(cached_ids.contains(&id));
    }

    #[test]
    fn transform_column_changes_with_rename() {
        use crate::branch::ColumnChange;
        use crate::sql::lens::{ColumnTransform, Lens};

        // Create a lens that renames "title" → "name"
        let lens = Lens::new(
            vec![ColumnTransform::rename("title", "name")],
            vec![ColumnTransform::rename("name", "title")],
        );

        // Create column changes with "title"
        let mut changes = ColumnChanges::new();
        changes.insert(
            "title".to_string(),
            ColumnChange {
                timestamp: 1000,
                author: "alice".to_string(),
            },
        );
        changes.insert(
            "status".to_string(),
            ColumnChange {
                timestamp: 2000,
                author: "bob".to_string(),
            },
        );

        // Transform
        let transformed = QueryNode::transform_column_changes(&changes, &lens);

        // "title" should become "name", "status" unchanged
        assert_eq!(transformed.len(), 2);
        assert!(
            transformed.contains_key("name"),
            "title should be renamed to name"
        );
        assert!(
            !transformed.contains_key("title"),
            "title should no longer exist"
        );
        assert!(
            transformed.contains_key("status"),
            "status should be unchanged"
        );

        // Verify metadata preserved
        let name_change = transformed.get("name").unwrap();
        assert_eq!(name_change.timestamp, 1000);
        assert_eq!(name_change.author, "alice");

        let status_change = transformed.get("status").unwrap();
        assert_eq!(status_change.timestamp, 2000);
        assert_eq!(status_change.author, "bob");
    }

    #[test]
    fn transform_column_changes_with_identity_lens() {
        use crate::branch::ColumnChange;
        use crate::sql::lens::Lens;

        // Create an identity lens (no transforms)
        let lens = Lens::identity();

        // Create column changes
        let mut changes = ColumnChanges::new();
        changes.insert(
            "name".to_string(),
            ColumnChange {
                timestamp: 500,
                author: "carol".to_string(),
            },
        );

        // Transform
        let transformed = QueryNode::transform_column_changes(&changes, &lens);

        // Should be unchanged
        assert_eq!(transformed.len(), 1);
        assert!(transformed.contains_key("name"));
        let name_change = transformed.get("name").unwrap();
        assert_eq!(name_change.timestamp, 500);
        assert_eq!(name_change.author, "carol");
    }

    #[test]
    fn transform_column_changes_multiple_renames() {
        use crate::branch::ColumnChange;
        use crate::sql::lens::{ColumnTransform, Lens};

        // Create a lens with multiple renames
        let lens = Lens::new(
            vec![
                ColumnTransform::rename("old_name", "new_name"),
                ColumnTransform::rename("old_status", "new_status"),
            ],
            vec![
                ColumnTransform::rename("new_name", "old_name"),
                ColumnTransform::rename("new_status", "old_status"),
            ],
        );

        // Create column changes
        let mut changes = ColumnChanges::new();
        changes.insert(
            "old_name".to_string(),
            ColumnChange {
                timestamp: 100,
                author: "user1".to_string(),
            },
        );
        changes.insert(
            "old_status".to_string(),
            ColumnChange {
                timestamp: 200,
                author: "user2".to_string(),
            },
        );
        changes.insert(
            "unchanged".to_string(),
            ColumnChange {
                timestamp: 300,
                author: "user3".to_string(),
            },
        );

        // Transform
        let transformed = QueryNode::transform_column_changes(&changes, &lens);

        // Check results
        assert_eq!(transformed.len(), 3);
        assert!(transformed.contains_key("new_name"));
        assert!(transformed.contains_key("new_status"));
        assert!(transformed.contains_key("unchanged"));
        assert!(!transformed.contains_key("old_name"));
        assert!(!transformed.contains_key("old_status"));
    }

    /// Test: Cross-schema branch merge gracefully handles missing lens
    ///
    /// When a lens is not available between schemas, the commit should be
    /// skipped rather than causing a panic or incorrect results.
    #[test]
    fn branch_merge_skips_commits_when_lens_missing() {
        use crate::branch::SchemaBranchName;
        use crate::commit::Commit;
        use crate::object::Object;
        use crate::sql::RowBuilder;
        use crate::sql::catalog::DescriptorId;
        use crate::sql::lens::LensContext;
        use crate::sql::row_buffer::RowDescriptor;
        use crate::sql::schema::{ColumnDef, ColumnType, TableSchema};
        use std::collections::HashMap;
        use std::sync::Arc;

        // Two different schemas (no lens between them)
        let schema_v1 = TableSchema::new(
            "documents",
            vec![ColumnDef::required("title", ColumnType::String)],
        );
        let desc_v1 = Arc::new(RowDescriptor::from_table_schema(&schema_v1));
        let desc_v1_id = DescriptorId::from_object_id(ObjectId::new(0x100));

        let schema_v2 = TableSchema::new(
            "documents",
            vec![ColumnDef::required("name", ColumnType::String)],
        );
        let desc_v2 = Arc::new(RowDescriptor::from_table_schema(&schema_v2));
        let desc_v2_id = DescriptorId::from_object_id(ObjectId::new(0x200));

        // Create object with two branches (different schemas)
        let branch_v1_name = SchemaBranchName::from_descriptor("dev", &desc_v1_id, "branch-v1");
        let branch_v2_name = SchemaBranchName::from_descriptor("dev", &desc_v2_id, "branch-v2");

        let mut object = Object::new(ObjectId::new(1), "documents");

        // Add initial row to main
        let initial_row = RowBuilder::new(desc_v1.clone())
            .set_string_by_name("title", "Initial")
            .build();

        {
            let mut main = object.branch_mut("main").unwrap();
            main.add_commit_with_tracking(
                Commit {
                    parents: vec![],
                    content: initial_row.buffer.clone().into_boxed_slice(),
                    timestamp: 1000,
                    author: "system".to_string(),
                    meta: None,
                },
                &desc_v1,
            )
            .unwrap();
        }

        // Create branches
        let main_tip = object.branch("main").unwrap().frontier()[0];
        object
            .create_branch(branch_v1_name.to_string(), "main", &main_tip)
            .unwrap();
        object
            .create_branch(branch_v2_name.to_string(), "main", &main_tip)
            .unwrap();

        // Add a commit to v1 branch
        let row_v1 = RowBuilder::new(desc_v1.clone())
            .set_string_by_name("title", "V1 Value")
            .build();

        {
            let mut branch_v1 = object.branch_mut(&branch_v1_name.to_string()).unwrap();
            let parent = branch_v1.frontier()[0];
            branch_v1
                .add_commit_with_tracking(
                    Commit {
                        parents: vec![parent],
                        content: row_v1.buffer.into_boxed_slice(),
                        timestamp: 2000,
                        author: "alice".to_string(),
                        meta: None,
                    },
                    &desc_v1,
                )
                .unwrap();
        }

        // Add a commit to v2 branch (in v2 format)
        let row_v2 = RowBuilder::new(desc_v2.clone())
            .set_string_by_name("name", "V2 Value")
            .build();

        {
            let mut branch_v2 = object.branch_mut(&branch_v2_name.to_string()).unwrap();
            let parent = branch_v2.frontier()[0];
            branch_v2
                .add_commit_with_tracking(
                    Commit {
                        parents: vec![parent],
                        content: row_v2.buffer.into_boxed_slice(),
                        timestamp: 3000,
                        author: "bob".to_string(),
                        meta: None,
                    },
                    &desc_v2,
                )
                .unwrap();
        }

        // Create BranchMerge node targeting v2 schema
        let mut node = QueryNode::BranchMerge {
            table: "documents".to_string(),
            branch_names: vec![branch_v1_name.to_string(), branch_v2_name.to_string()],
            descriptor: desc_v2.clone(),
            target_descriptor_id: Some(desc_v2_id),
            object_states: HashMap::new(),
        };

        // Empty lens context - no lens available from v1 to v2!
        let lens_context = LensContext::new();

        // Descriptor lookup that returns our descriptors
        let descriptor_lookup = |id: DescriptorId| -> Option<Arc<RowDescriptor>> {
            if id == desc_v1_id {
                Some(desc_v1.clone())
            } else if id == desc_v2_id {
                Some(desc_v2.clone())
            } else {
                None
            }
        };

        // Evaluate - should NOT panic, and should skip the v1 commit (no lens)
        let delta = node.evaluate_branch_merge_with_lenses(
            ObjectId::new(1),
            &object,
            &lens_context,
            descriptor_lookup,
        );

        // Should have exactly one row (from v2 branch only, v1 was skipped)
        assert_eq!(delta.len(), 1, "Should have one row (v2 commit only)");

        // Verify it's the v2 value
        let deltas: Vec<_> = delta.into_iter().collect();
        match &deltas[0] {
            RowDelta::Added { row, .. } => {
                let name = row.get_by_name("name");
                assert_eq!(
                    name,
                    Some(crate::sql::row_buffer::RowValue::String("V2 Value")),
                    "Should have v2 value since v1 commit was skipped (no lens)"
                );
            }
            _ => panic!("Expected Added delta"),
        }
    }
}
