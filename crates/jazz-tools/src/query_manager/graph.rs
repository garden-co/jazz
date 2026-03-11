use ahash::{AHashMap, AHashSet};
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::ops::Bound;

use bitvec::prelude::*;
use smallvec::SmallVec;

use crate::object::{BranchName, ObjectId};
use crate::schema_manager::{SchemaContext, translate_column_for_index};

use crate::storage::{IndexScanDirection, Storage};

use super::graph_nodes::alias::AliasNode;
use super::graph_nodes::array_subquery::{ArraySubqueryNode, Correlate};
use super::graph_nodes::exists_output::ExistsOutputNode;
use super::graph_nodes::filter::{FilterNode, Predicate};
use super::graph_nodes::index_scan::IndexScanNode;
use super::graph_nodes::indexed_query::{
    DriverPlan, IndexedQueryNode, IndexedQueryNodeConfig, JoinEdgePlan, ResolvedRowKey,
    ResolvedSortKey, ResolvedSortTarget, TablePolicySpec,
};
use super::graph_nodes::join::{JoinColumnRef, JoinNode};
use super::graph_nodes::limit_offset::LimitOffsetNode;
use super::graph_nodes::materialize::MaterializeNode;
use super::graph_nodes::output::{OutputMode, OutputNode};
use super::graph_nodes::policy_filter::PolicyFilterNode;
use super::graph_nodes::project::ProjectNode;
use super::graph_nodes::recursive_relation::{
    CorrelationSource, RecursiveHop, RecursiveRelationNode,
};
use super::graph_nodes::select_element::SelectElementNode;
use super::graph_nodes::sort::{SortDirection, SortKey, SortNode, SortTarget};
use super::graph_nodes::subgraph::SubgraphTemplate;
use super::graph_nodes::union::UnionNode;
use super::graph_nodes::{NodeId, RowNode, SourceContext, SourceNode, TransformNode};
use super::index::ScanCondition;
use super::policy::PolicyExpr;
use super::query::{ArraySubquerySpec, Condition, Conjunction, Query, QueryBuilder};
use super::relation_ir::{ProjectColumn, ProjectExpr, RelExpr};
use super::relation_ir_query_plan::{ExecutionQueryPlan, lower_relation_to_execution_plan};
use super::session::Session;
use super::types::{
    ColumnDescriptor, ColumnName, ColumnType, ComposedBranchName, LoadedRow, Row, RowDelta,
    RowDescriptor, Schema, SchemaHash, TableName, Tuple, TupleDelta, TupleDescriptor,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryCompileError {
    UnknownTable(TableName),
    InvalidPlan(String),
}

impl fmt::Display for QueryCompileError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            QueryCompileError::UnknownTable(table) => {
                write!(f, "unknown table referenced in relation_ir: {}", table)
            }
            QueryCompileError::InvalidPlan(reason) => write!(f, "invalid relation plan: {reason}"),
        }
    }
}

impl std::error::Error for QueryCompileError {}

/// A node in the query graph (type-erased).
#[derive(Debug)]
pub enum GraphNode {
    IndexScan(IndexScanNode),
    Union(UnionNode),
    Alias(AliasNode),
    Join(JoinNode),
    Materialize(MaterializeNode),
    IndexedQuery(IndexedQueryNode),
    Project(ProjectNode),
    SelectElement(SelectElementNode),
    RecursiveRelation(RecursiveRelationNode),
    Filter(FilterNode),
    PolicyFilter(PolicyFilterNode),
    Sort(SortNode),
    LimitOffset(LimitOffsetNode),
    ArraySubquery(ArraySubqueryNode),
    Output(OutputNode),
    ExistsOutput(ExistsOutputNode),
}

/// Compact node with inline edge storage.
/// Most nodes have 0-2 inputs/outputs, so inline storage avoids heap allocation.
#[derive(Debug)]
pub struct CompactNode {
    pub node: GraphNode,
    /// Input edges (children/dependencies). Most nodes have 0-2 inputs.
    pub inputs: SmallVec<[NodeId; 2]>,
    /// Output edges (parents/dependents). Most nodes have 0-2 outputs.
    pub outputs: SmallVec<[NodeId; 2]>,
}

/// Compiled query graph for a single query.
#[derive(Debug)]
pub struct QueryGraph {
    /// Dense node storage (NodeId.0 is index).
    pub nodes: Vec<CompactNode>,
    /// Dirty tracking bitmap (1 bit per node, indexed by NodeId.0).
    dirty_bitmap: BitVec,
    /// The output node ID.
    pub output_node: NodeId,
    /// The pagination node, when the query applies limit/offset.
    pagination_node: Option<NodeId>,
    /// Table this query operates on.
    pub table: TableName,
    /// Index scan nodes for this query (for marking dirty on updates).
    pub index_scan_nodes: Vec<(NodeId, TableName, ColumnName)>, // (node_id, table, column)
    /// Ordered-scan source nodes for this query.
    pub ordered_scan_nodes: Vec<(NodeId, TableName, ColumnName)>,
    /// Array subquery nodes and their inner tables (for marking dirty on inner table updates).
    pub array_subquery_tables: Vec<(NodeId, TableName)>, // (node_id, inner_table)
    /// PolicyFilter nodes and their INHERITS-referenced tables (for marking dirty on table updates).
    pub policy_filter_tables: Vec<(NodeId, TableName)>, // (node_id, inherits_table)
    /// RecursiveRelation nodes and their step dependency tables (for marking dirty on table updates).
    pub recursive_relation_tables: Vec<(NodeId, TableName)>, // (node_id, step_table)
    /// Per-table descriptors in join order (for flattening multi-element tuples).
    pub table_descriptors: Vec<RowDescriptor>,
    /// Combined descriptor for output (all columns from all tables).
    pub combined_descriptor: RowDescriptor,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct RelationCompileFeatures {
    pub include_deleted: bool,
    pub array_subqueries: Vec<ArraySubquerySpec>,
    pub select_columns: Option<Vec<String>>,
}

fn natural_row_projection_element_index(
    input_tuple_descriptor: &TupleDescriptor,
    columns: &[ProjectColumn],
) -> Option<usize> {
    for element_index in 0..input_tuple_descriptor.element_count() {
        let element = input_tuple_descriptor.element(element_index)?;

        // Implicit-id tables carry row identity separately from row content. When
        // a projection simply re-exposes that natural row shape, prefer selecting
        // the tuple element directly so downstream consumers still see `row.id`
        // plus only the declared data columns.
        if element.descriptor.column_index("id").is_some() {
            continue;
        }

        let Some((projected_id, projected_columns)) = columns.split_first() else {
            continue;
        };
        let ProjectExpr::Column(projected_id_ref) = &projected_id.expr else {
            continue;
        };
        if projected_id.alias != "id"
            || projected_id_ref.column != "_id"
            || projected_id_ref.scope.as_deref() != Some(element.table.as_str())
            || projected_columns.len() != element.descriptor.columns.len()
        {
            continue;
        }

        let matches_declared_columns = projected_columns
            .iter()
            .zip(element.descriptor.columns.iter())
            .all(|(projected, declared)| {
                projected.alias == declared.name.as_str()
                    && matches!(
                        &projected.expr,
                        ProjectExpr::Column(column_ref)
                            if column_ref.scope.as_deref() == Some(element.table.as_str())
                                && column_ref.column == declared.name.as_str()
                    )
            });
        if matches_declared_columns {
            return Some(element_index);
        }
    }

    None
}

fn resolve_row_key_in_scopes(
    tuple_descriptor: &TupleDescriptor,
    raw: &str,
    candidate_scopes: &[usize],
) -> Option<(usize, ResolvedRowKey)> {
    if let Some((scope, column)) = raw.rsplit_once('.') {
        let scope_index = candidate_scopes.iter().copied().find(|index| {
            tuple_descriptor
                .element(*index)
                .is_some_and(|element| element.table == scope)
        })?;
        let descriptor = &tuple_descriptor.element(scope_index)?.descriptor;
        let key = ResolvedRowKey::from_descriptor(descriptor, column.trim())?;
        return Some((scope_index, key));
    }

    let mut matches = candidate_scopes
        .iter()
        .copied()
        .filter_map(|index| {
            let descriptor = &tuple_descriptor.element(index)?.descriptor;
            let key = ResolvedRowKey::from_descriptor(descriptor, raw)?;
            Some((index, key))
        })
        .collect::<Vec<_>>();

    (matches.len() == 1).then(|| matches.remove(0))
}

fn resolve_sort_key(
    tuple_descriptor: &TupleDescriptor,
    column: &str,
    direction: SortDirection,
) -> Option<(usize, ResolvedRowKey, ResolvedSortKey)> {
    let candidate_scopes = (0..tuple_descriptor.element_count()).collect::<Vec<_>>();
    let (scope_index, key) =
        resolve_row_key_in_scopes(tuple_descriptor, column, &candidate_scopes)?;
    let target = if key.use_row_id {
        ResolvedSortTarget::RowId {
            element_index: scope_index,
        }
    } else {
        ResolvedSortTarget::Column {
            element_index: scope_index,
            descriptor: tuple_descriptor.element(scope_index)?.descriptor.clone(),
            local_col_index: key.local_col_index?,
        }
    };

    Some((scope_index, key, ResolvedSortKey { target, direction }))
}

impl QueryGraph {
    pub fn new(table: TableName, descriptor: RowDescriptor) -> Self {
        Self {
            nodes: Vec::new(),
            dirty_bitmap: BitVec::new(),
            output_node: NodeId(0),
            pagination_node: None,
            table,
            index_scan_nodes: Vec::new(),
            ordered_scan_nodes: Vec::new(),
            array_subquery_tables: Vec::new(),
            policy_filter_tables: Vec::new(),
            recursive_relation_tables: Vec::new(),
            table_descriptors: vec![descriptor.clone()],
            combined_descriptor: descriptor,
        }
    }

    /// Mark a node as dirty using the bitmap.
    pub fn mark_dirty(&mut self, id: NodeId) {
        let idx = id.0 as usize;
        if idx >= self.dirty_bitmap.len() {
            self.dirty_bitmap.resize(idx + 1, false);
        }
        self.dirty_bitmap.set(idx, true);
    }

    /// Check if a node is dirty.
    fn is_dirty(&self, id: NodeId) -> bool {
        let idx = id.0 as usize;
        idx < self.dirty_bitmap.len() && self.dirty_bitmap[idx]
    }

    /// Check if any nodes are dirty.
    pub fn has_dirty_nodes(&self) -> bool {
        self.dirty_bitmap.any()
    }

    /// Clear all dirty flags.
    pub fn clear_dirty(&mut self) {
        self.dirty_bitmap.fill(false);
    }

    fn add_node(&mut self, node: GraphNode) -> NodeId {
        let id = NodeId(self.nodes.len() as u64);
        self.nodes.push(CompactNode {
            node,
            inputs: SmallVec::new(),
            outputs: SmallVec::new(),
        });
        // Grow dirty bitmap to accommodate new node
        self.dirty_bitmap.push(true); // New nodes start dirty
        id
    }

    /// Add an edge from one node to another.
    pub fn add_edge(&mut self, from: NodeId, to: NodeId) {
        self.nodes[from.0 as usize].inputs.push(to);
        self.nodes[to.0 as usize].outputs.push(from);
    }

    /// Add a node and return its ID.
    pub fn add_node_with_id(&mut self, node: GraphNode) -> NodeId {
        self.add_node(node)
    }

    /// Get a reference to a node by ID.
    fn get_node(&self, id: NodeId) -> Option<&GraphNode> {
        self.nodes.get(id.0 as usize).map(|c| &c.node)
    }

    /// Get a mutable reference to a node by ID.
    fn get_node_mut(&mut self, id: NodeId) -> Option<&mut GraphNode> {
        self.nodes.get_mut(id.0 as usize).map(|c| &mut c.node)
    }

    /// Get input edges for a node.
    fn get_inputs(&self, id: NodeId) -> &[NodeId] {
        &self.nodes[id.0 as usize].inputs
    }

    /// Get output edges (reverse edges) for a node.
    fn get_outputs(&self, id: NodeId) -> Option<&[NodeId]> {
        self.nodes.get(id.0 as usize).map(|c| c.outputs.as_slice())
    }

    /// Returns ObjectIds contributing to current result set along with their branches.
    ///
    /// These are the objects that, if synced, would affect query results.
    /// Only includes ObjectIds that:
    /// 1. Come from an IndexScanNode (source of all objects)
    /// 2. Survive all filtering/joins to appear in the output
    ///
    /// After calling `settle()`, this method returns the (ObjectId, BranchName) pairs
    /// for all rows currently in the query result.
    pub fn contributing_object_ids(&self) -> HashSet<(ObjectId, BranchName)> {
        self.scope_from_tuples(&self.current_output_tuples())
    }

    /// Returns ObjectIds that must be synced for the client to reproduce the
    /// current query result locally.
    pub fn sync_scope_object_ids(&self) -> HashSet<(ObjectId, BranchName)> {
        if let Some(node_id) = self.pagination_node
            && let Some(node) = self.get_node(node_id)
        {
            let mut scope = match node {
                GraphNode::LimitOffset(limit_offset) => {
                    self.scope_from_tuples(limit_offset.sync_input_tuples())
                }
                GraphNode::IndexedQuery(indexed) => {
                    self.scope_from_tuples(indexed.sync_input_tuples())
                }
                _ => HashSet::new(),
            };
            scope.extend(self.contributing_object_ids());
            if !scope.is_empty() {
                return scope;
            }
        }

        self.contributing_object_ids()
    }

    fn scope_from_tuples(&self, tuples: &[Tuple]) -> HashSet<(ObjectId, BranchName)> {
        tuples
            .iter()
            .flat_map(|tuple| tuple.provenance().iter().copied())
            .collect()
    }

    /// Compile a query into a graph (without policy filtering).
    pub fn compile(query: &Query, schema: &Schema) -> Option<Self> {
        let schema_context = Self::default_schema_context(schema);
        let mut query_with_default_branch = query.clone();
        if query_with_default_branch.branches.is_empty() {
            query_with_default_branch.branches.push("main".to_string());
        }
        Self::try_compile_with_schema_context(
            &query_with_default_branch,
            schema,
            None,
            &schema_context,
        )
        .ok()
    }

    /// Compile a query into a graph with typed errors (without policy filtering).
    pub fn try_compile(query: &Query, schema: &Schema) -> Result<Self, QueryCompileError> {
        let schema_context = Self::default_schema_context(schema);
        let mut query_with_default_branch = query.clone();
        if query_with_default_branch.branches.is_empty() {
            query_with_default_branch.branches.push("main".to_string());
        }
        Self::try_compile_with_schema_context(
            &query_with_default_branch,
            schema,
            None,
            &schema_context,
        )
    }

    /// Legacy compile sites default to querying `main` without schema fan-out.
    fn default_schema_context(schema: &Schema) -> SchemaContext {
        SchemaContext::with_defaults(schema.clone(), "main")
    }

    /// Compile relation IR directly into a graph.
    pub fn compile_relation_ir(
        relation: &RelExpr,
        schema: &Schema,
        branches: &[String],
        session: Option<Session>,
    ) -> Option<Self> {
        Self::compile_relation_ir_with_features(
            relation,
            schema,
            branches,
            session,
            RelationCompileFeatures::default(),
        )
    }

    pub(crate) fn compile_relation_ir_with_features(
        relation: &RelExpr,
        schema: &Schema,
        branches: &[String],
        session: Option<Session>,
        features: RelationCompileFeatures,
    ) -> Option<Self> {
        let default_branches = vec!["main".to_string()];
        let branches: &[String] = if branches.is_empty() {
            &default_branches
        } else {
            branches
        };
        let plan = lower_relation_to_execution_plan(
            relation,
            branches,
            features.include_deleted,
            features.array_subqueries,
            features.select_columns,
        )?;
        validate_execution_plan(&plan, schema).ok()?;
        let schema_context = Self::default_schema_context(schema);
        Self::compile_execution_plan_with_schema_context(&plan, schema, session, &schema_context)
    }

    /// Compile relation IR directly into a graph with schema context.
    pub fn compile_relation_ir_with_schema_context(
        relation: &RelExpr,
        schema: &Schema,
        branches: &[String],
        session: Option<Session>,
        schema_context: &SchemaContext,
    ) -> Option<Self> {
        Self::compile_relation_ir_with_schema_context_and_features(
            relation,
            schema,
            branches,
            session,
            schema_context,
            RelationCompileFeatures::default(),
        )
    }

    pub(crate) fn compile_relation_ir_with_schema_context_and_features(
        relation: &RelExpr,
        schema: &Schema,
        branches: &[String],
        session: Option<Session>,
        schema_context: &SchemaContext,
        features: RelationCompileFeatures,
    ) -> Option<Self> {
        let plan = lower_relation_to_execution_plan(
            relation,
            branches,
            features.include_deleted,
            features.array_subqueries,
            features.select_columns,
        )?;
        validate_execution_plan(&plan, schema).ok()?;
        Self::compile_execution_plan_with_schema_context(&plan, schema, session, schema_context)
    }

    fn compile_execution_plan_with_schema_context(
        plan: &ExecutionQueryPlan,
        schema: &Schema,
        session: Option<Session>,
        schema_context: &SchemaContext,
    ) -> Option<Self> {
        // Build branch -> schema hash map for column translation.
        // Use full hashes from SchemaContext (do not re-parse branch strings, which only encode
        // a shortened hash prefix).
        let mut branch_schema_map: HashMap<String, SchemaHash> = HashMap::new();
        for schema_hash in schema_context.all_live_hashes() {
            let branch_name = ComposedBranchName::new(
                &schema_context.env,
                schema_hash,
                &schema_context.user_branch,
            )
            .to_branch_name();
            branch_schema_map.insert(branch_name.as_str().to_string(), schema_hash);
        }

        // Expand branches to include all live schema branches if not specified
        let branches: Vec<String> = if plan.branches.is_empty() {
            schema_context
                .all_branch_names()
                .into_iter()
                .map(|b| b.as_str().to_string())
                .collect()
        } else {
            plan.branches.clone()
        };

        if let Some(graph) = Self::compile_indexed_query_plan(
            plan,
            schema,
            &branches,
            session.clone(),
            schema_context,
            &branch_schema_map,
        ) {
            return Some(graph);
        }

        if !plan.joins.is_empty() {
            return Self::compile_join_plan(
                plan,
                schema,
                &branches,
                session.clone(),
                schema_context,
            );
        }

        let table_schema = schema.get(&plan.table)?;
        let descriptor = table_schema.columns.clone();
        let select_policy = table_schema.policies.select.using.clone();

        let mut graph = QueryGraph::new(plan.table, descriptor.clone());
        let table_str = plan.table.as_str();

        // Phase 1: Build IndexScan nodes (one per disjunct per branch)
        // For multi-branch queries, we create scans for each branch and union them
        // Column names are translated for old schema branches
        let mut phase1_outputs: Vec<NodeId> = Vec::new();
        let mut index_columns: Vec<String> = Vec::new();

        for branch in &branches {
            // Get schema hash for this branch to determine if column translation is needed
            let branch_schema_hash = branch_schema_map.get(branch).copied();

            for disjunct in &plan.disjuncts {
                // Find best index condition for this disjunct
                let (scan_column, scan_condition) =
                    if let Some(cond) = disjunct.best_index_condition() {
                        let column = cond.column().to_string();
                        let scan_cond = condition_to_scan(cond);
                        (column, scan_cond)
                    } else {
                        // No index condition, use "_id" for full scan
                        ("_id".to_string(), ScanCondition::All)
                    };

                // Translate column name for old schema branches
                let translated_column = if let Some(target_hash) = branch_schema_hash {
                    if target_hash != schema_context.current_hash {
                        // This branch uses an old schema - translate column name
                        translate_column_for_index(
                            schema_context,
                            table_str,
                            &scan_column,
                            &target_hash,
                        )
                        .unwrap_or_else(|| scan_column.clone())
                    } else {
                        scan_column.clone()
                    }
                } else {
                    scan_column.clone()
                };

                index_columns.push(scan_column.clone());
                let scan_column_name = ColumnName::new(&translated_column);

                let scan_node = IndexScanNode::new_with_branch(
                    plan.table,
                    scan_column_name,
                    branch,
                    scan_condition,
                    descriptor.clone(),
                );
                let scan_id = graph.add_node(GraphNode::IndexScan(scan_node));
                graph
                    .index_scan_nodes
                    .push((scan_id, plan.table, scan_column_name));
                phase1_outputs.push(scan_id);
            }

            // If include_deleted is set, also scan _id_deleted index for this branch
            if plan.include_deleted {
                let deleted_column = ColumnName::new("_id_deleted");
                let deleted_scan_node = IndexScanNode::new_with_branch(
                    plan.table,
                    deleted_column,
                    branch,
                    ScanCondition::All,
                    descriptor.clone(),
                );
                let deleted_scan_id = graph.add_node(GraphNode::IndexScan(deleted_scan_node));
                graph
                    .index_scan_nodes
                    .push((deleted_scan_id, plan.table, deleted_column));
                phase1_outputs.push(deleted_scan_id);
            }
        }

        // If multiple outputs, add Union node
        let phase1_output = if phase1_outputs.len() > 1 {
            let union_node = UnionNode::new();
            let union_id = graph.add_node(GraphNode::Union(union_node));
            for scan_id in &phase1_outputs {
                graph.add_edge(union_id, *scan_id);
            }
            union_id
        } else if !phase1_outputs.is_empty() {
            phase1_outputs[0]
        } else {
            return None;
        };

        // Materialize node (boundary between Phase 1 and Phase 2)
        // Lens transforms are applied in the row_loader, so MaterializeNode uses current schema
        let tuple_desc = TupleDescriptor::single("", descriptor.clone());
        let materialize_node = MaterializeNode::new_all(tuple_desc);
        let materialize_id = graph.add_node(GraphNode::Materialize(materialize_node));
        graph.add_edge(materialize_id, phase1_output);

        let mut phase2_input = materialize_id;
        let mut current_descriptor = descriptor.clone();

        // Policy filter node (if session provided and table has SELECT policy)
        if let (Some(session), Some(policy)) = (&session, select_policy) {
            let branch_for_policy = branches
                .first()
                .cloned()
                .unwrap_or_else(|| "main".to_string());
            let policy_node = PolicyFilterNode::new_with_branch(
                current_descriptor.clone(),
                policy,
                session.clone(),
                schema.clone(),
                plan.table.as_str(),
                branch_for_policy,
            );
            let inherits_tables: Vec<TableName> = policy_node
                .inherits_tables()
                .iter()
                .map(TableName::new)
                .collect();
            let policy_id = graph.add_node(GraphNode::PolicyFilter(policy_node));
            graph.add_edge(policy_id, phase2_input);
            for inherits_table in inherits_tables {
                graph.policy_filter_tables.push((policy_id, inherits_table));
            }
            phase2_input = policy_id;
        }

        // Array subqueries: insert ArraySubqueryNode for each array subquery
        for subquery_spec in &plan.array_subqueries {
            if let Some((node, new_descriptor)) = graph.compile_array_subquery(
                subquery_spec,
                &current_descriptor,
                schema,
                &branches,
                schema_context,
            ) {
                let node_id = graph.add_node(GraphNode::ArraySubquery(node));
                graph.add_edge(node_id, phase2_input);
                graph
                    .array_subquery_tables
                    .push((node_id, subquery_spec.table));
                phase2_input = node_id;
                current_descriptor = new_descriptor;
            }
        }

        // Phase 2: Filter node (only if there are remaining conditions not covered by index)
        let predicate = build_remaining_predicate_from_disjuncts(
            &plan.disjuncts,
            &index_columns,
            &TupleDescriptor::single_with_materialization(
                plan.base_scope.as_str(),
                current_descriptor.clone(),
                true,
            ),
        );
        if !matches!(predicate, Predicate::True) {
            let filter_tuple_desc = TupleDescriptor::single_with_materialization(
                plan.base_scope.as_str(),
                current_descriptor.clone(),
                true,
            );
            let filter_node = FilterNode::with_tuple_descriptor(filter_tuple_desc, predicate);
            let filter_id = graph.add_node(GraphNode::Filter(filter_node));
            graph.add_edge(filter_id, phase2_input);
            phase2_input = filter_id;
        }

        // Sort node (default: id ASC when order_by is omitted)
        let sort_keys = sort_keys_from_order_by(&plan.order_by, &current_descriptor);
        if !sort_keys.is_empty() {
            let sort_tuple_desc = TupleDescriptor::single_with_materialization(
                plan.base_scope.as_str(),
                current_descriptor.clone(),
                true,
            );
            let sort_node = SortNode::with_tuple_descriptor(sort_tuple_desc, sort_keys);
            let sort_id = graph.add_node(GraphNode::Sort(sort_node));
            graph.add_edge(sort_id, phase2_input);
            phase2_input = sort_id;
        }

        // LimitOffset node (if limit or offset specified)
        if plan.limit.is_some() || plan.offset > 0 {
            let limit_tuple_desc = TupleDescriptor::single_with_materialization(
                plan.base_scope.as_str(),
                current_descriptor.clone(),
                true,
            );
            let limit_offset_node =
                LimitOffsetNode::with_tuple_descriptor(limit_tuple_desc, plan.limit, plan.offset);
            let limit_offset_id = graph.add_node(GraphNode::LimitOffset(limit_offset_node));
            graph.add_edge(limit_offset_id, phase2_input);
            graph.pagination_node = Some(limit_offset_id);
            phase2_input = limit_offset_id;
        }

        // Project node (if projection specified)
        if let Some(columns) = &plan.project_columns {
            let project_input = TupleDescriptor::single_with_materialization(
                plan.base_scope.as_str(),
                current_descriptor.clone(),
                true,
            );
            let project_node = ProjectNode::with_project_columns(project_input, columns)?;
            current_descriptor = project_node.output_descriptor().clone();
            let project_id = graph.add_node(GraphNode::Project(project_node));
            graph.add_edge(project_id, phase2_input);
            phase2_input = project_id;
        }

        // Recursive relation expansion (if configured).
        if let Some(recursive_spec) = &plan.recursive
            && let Some((node, new_descriptor, step_table)) = graph.compile_recursive_relation(
                recursive_spec,
                &current_descriptor,
                schema,
                &branches,
                schema_context,
            )
        {
            let node_id = graph.add_node(GraphNode::RecursiveRelation(node));
            graph.add_edge(node_id, phase2_input);
            graph.recursive_relation_tables.push((node_id, step_table));
            phase2_input = node_id;
            current_descriptor = new_descriptor;
        }

        // Output node
        graph.combined_descriptor = current_descriptor.clone();
        let output_tuple_desc = TupleDescriptor::single_with_materialization(
            plan.base_scope.as_str(),
            current_descriptor,
            true,
        );
        let output_node = OutputNode::with_tuple_descriptor(output_tuple_desc, OutputMode::Delta);
        let output_id = graph.add_node(GraphNode::Output(output_node));
        graph.add_edge(output_id, phase2_input);
        graph.output_node = output_id;

        Some(graph)
    }

    /// Compile a query with schema context for multi-schema queries.
    ///
    /// When schema context is provided:
    /// - Branches are automatically expanded to include all live schema branches
    /// - Column names are translated through lens chain for old schema branches
    /// - The descriptor uses the current schema (lens transforms happen at row load time)
    pub fn compile_with_schema_context(
        query: &Query,
        schema: &Schema,
        session: Option<Session>,
        schema_context: &SchemaContext,
    ) -> Option<Self> {
        Self::try_compile_with_schema_context(query, schema, session, schema_context).ok()
    }

    /// Compile a query with schema context for multi-schema queries.
    ///
    /// Returns a typed error instead of collapsing failures into `None`.
    pub fn try_compile_with_schema_context(
        query: &Query,
        schema: &Schema,
        session: Option<Session>,
        schema_context: &SchemaContext,
    ) -> Result<Self, QueryCompileError> {
        let branches: Vec<String> = if query.branches.is_empty() {
            schema_context
                .all_branch_names()
                .into_iter()
                .map(|b| b.as_str().to_string())
                .collect()
        } else {
            query.branches.clone()
        };
        ensure_relation_tables_exist(&query.relation_ir, schema)?;

        let plan = lower_relation_to_execution_plan(
            &query.relation_ir,
            &branches,
            query.include_deleted,
            query.array_subqueries.clone(),
            query.select_columns.clone(),
        )
        .ok_or_else(|| {
            QueryCompileError::InvalidPlan(
                "unsupported relation_ir shape for schema-context query compilation".to_string(),
            )
        })?;

        validate_execution_plan(&plan, schema)?;

        Self::compile_execution_plan_with_schema_context(&plan, schema, session, schema_context)
            .ok_or_else(|| {
                QueryCompileError::InvalidPlan(
                    "unsupported relation_ir shape for schema-context query compilation"
                        .to_string(),
                )
            })
    }

    /// Compile an array subquery specification into an ArraySubqueryNode.
    /// Returns the node and the new output descriptor (outer + array column).
    fn compile_array_subquery(
        &self,
        spec: &crate::query_manager::query::ArraySubquerySpec,
        outer_descriptor: &RowDescriptor,
        schema: &Schema,
        branches: &[String],
        schema_context: &SchemaContext,
    ) -> Option<(ArraySubqueryNode, RowDescriptor)> {
        // Get inner table descriptor
        let inner_descriptor = schema.get(&spec.table)?.columns.clone();

        // Find outer correlation column index
        // The outer_column may be qualified (table.column) or unqualified
        let outer_col_name = spec
            .outer_column
            .split('.')
            .next_back()
            .unwrap_or(&spec.outer_column);
        let outer_correlation = match outer_descriptor.column_index(outer_col_name) {
            Some(index) => Correlate::Col(index),
            None if outer_col_name == "id" || outer_col_name == "_id" => Correlate::Id,
            None => return None,
        };

        // Build base query for subgraph, inheriting branches from outer query.
        let mut base_builder = QueryBuilder::new(spec.table);
        if !branches.is_empty() {
            let branch_refs: Vec<&str> = branches.iter().map(String::as_str).collect();
            base_builder = base_builder.branches(&branch_refs);
        }
        for join_spec in &spec.joins {
            base_builder = base_builder.join(join_spec.table);
            if let Some(alias) = &join_spec.alias {
                base_builder = base_builder.alias(alias);
            }
            if let Some((left, right)) = &join_spec.on {
                base_builder = base_builder.on(left, right);
            }
        }
        for condition in &spec.filters {
            base_builder = apply_condition_to_builder(base_builder, condition);
        }
        for (column, direction) in &spec.order_by {
            base_builder = match direction {
                SortDirection::Ascending => base_builder.order_by(column),
                SortDirection::Descending => base_builder.order_by_desc(column),
            };
        }
        if let Some(limit) = spec.limit {
            base_builder = base_builder.limit(limit);
        }
        if let Some(cols) = &spec.select_columns {
            let col_refs: Vec<&str> = cols.iter().map(String::as_str).collect();
            base_builder = base_builder.select(&col_refs);
        }
        let mut base_query = base_builder.try_build().ok()?;
        base_query.array_subqueries = spec.nested_arrays.clone();

        // Build combined descriptor: base table + all joined tables + nested array columns
        let mut combined_columns = inner_descriptor.columns.clone();
        for join_spec in &spec.joins {
            if let Some(joined_schema) = schema.get(&join_spec.table) {
                combined_columns.extend(joined_schema.columns.columns.clone());
            }
        }

        // Add columns for nested array subqueries (recursive)
        for nested in &spec.nested_arrays {
            if let Some(nested_element_desc) = Self::build_nested_array_descriptor(nested, schema) {
                combined_columns.push(ColumnDescriptor::new(
                    &nested.column_name,
                    ColumnType::Array {
                        element: Box::new(ColumnType::Row {
                            columns: Box::new(nested_element_desc),
                        }),
                    },
                ));
            }
        }

        let combined_descriptor = RowDescriptor::new(combined_columns);

        // Build output descriptor for inner query
        let inner_output_descriptor = if let Some(cols) = &spec.select_columns {
            let columns = cols
                .iter()
                .filter_map(|name| {
                    combined_descriptor
                        .columns
                        .iter()
                        .find(|c| c.name.as_str() == name)
                        .cloned()
                })
                .collect();
            RowDescriptor::new(columns)
        } else {
            combined_descriptor
        };

        // Create subgraph template
        let subgraph_template = SubgraphTemplate::new(
            base_query,
            spec.inner_column.clone(),
            spec.select_columns.clone().unwrap_or_default(),
            inner_output_descriptor,
            schema_context.clone(),
        );

        // Create outer tuple descriptor
        let outer_tuple_descriptor =
            TupleDescriptor::single_with_materialization("", outer_descriptor.clone(), true);

        // Create node - it computes its own output descriptor with proper Array<Row> type
        let node = ArraySubqueryNode::new(
            outer_tuple_descriptor,
            subgraph_template,
            outer_correlation,
            spec.column_name.clone(),
            schema.clone(),
        );

        // Use the node's output descriptor (which has correct Array<Row> type)
        let output_descriptor = node.output_descriptor().clone();

        Some((node, output_descriptor))
    }

    /// Recursively build the element descriptor for a nested array subquery.
    fn build_nested_array_descriptor(
        spec: &crate::query_manager::query::ArraySubquerySpec,
        schema: &Schema,
    ) -> Option<RowDescriptor> {
        let inner_schema = schema.get(&spec.table)?;

        // Start with base table columns + joined table columns
        let mut columns = inner_schema.columns.columns.clone();
        for join_spec in &spec.joins {
            if let Some(joined_schema) = schema.get(&join_spec.table) {
                columns.extend(joined_schema.columns.columns.clone());
            }
        }

        // Recursively add nested array columns
        for nested in &spec.nested_arrays {
            if let Some(nested_element_desc) = Self::build_nested_array_descriptor(nested, schema) {
                columns.push(ColumnDescriptor::new(
                    &nested.column_name,
                    ColumnType::Array {
                        element: Box::new(ColumnType::Row {
                            columns: Box::new(nested_element_desc),
                        }),
                    },
                ));
            }
        }

        // Apply select_columns if specified
        let base_columns = if let Some(cols) = &spec.select_columns {
            cols.iter()
                .filter_map(|name| columns.iter().find(|c| c.name.as_str() == name).cloned())
                .collect()
        } else {
            columns
        };

        // Row id is carried in Value::Row { id: Some(...), .. } rather than
        // as a prepended column.
        Some(RowDescriptor::new(base_columns))
    }

    /// Compile a recursive relation specification into a RecursiveRelationNode.
    fn compile_recursive_relation(
        &self,
        spec: &crate::query_manager::query::RecursiveSpec,
        current_descriptor: &RowDescriptor,
        schema: &Schema,
        branches: &[String],
        schema_context: &SchemaContext,
    ) -> Option<(RecursiveRelationNode, RowDescriptor, TableName)> {
        let step_table_schema = schema.get(&spec.table)?;
        let step_table_descriptor = step_table_schema.columns.clone();

        let outer_col_name = spec
            .outer_column
            .split('.')
            .next_back()
            .unwrap_or(&spec.outer_column);
        let correlation_source = match outer_col_name {
            "id" | "_id" => CorrelationSource::ObjectId,
            _ => CorrelationSource::Column(current_descriptor.column_index(outer_col_name)?),
        };
        if spec.hop.is_some() && (!spec.joins.is_empty() || spec.result_element_index.is_some()) {
            return None;
        }
        if spec.result_element_index.is_some() && spec.select_columns.is_some() {
            return None;
        }

        // Build step query for each recursive level.
        let mut step_builder = QueryBuilder::new(spec.table);
        if !branches.is_empty() {
            let branch_refs: Vec<&str> = branches.iter().map(String::as_str).collect();
            step_builder = step_builder.branches(&branch_refs);
        }
        for join_spec in &spec.joins {
            step_builder = step_builder.join(join_spec.table);
            if let Some(alias) = &join_spec.alias {
                step_builder = step_builder.alias(alias);
            }
            if let Some((left, right)) = &join_spec.on {
                step_builder = step_builder.on(left, right);
            }
        }
        for condition in &spec.filters {
            step_builder = apply_condition_to_builder(step_builder, condition);
        }
        if let Some(cols) = &spec.select_columns {
            let col_refs: Vec<&str> = cols.iter().map(String::as_str).collect();
            step_builder = step_builder.select(&col_refs);
        }
        if let Some(index) = spec.result_element_index {
            step_builder = step_builder.result_element_index(index);
        }
        let step_query = step_builder.try_build().ok()?;

        // Build descriptor for step output.
        let mut step_table_descriptors = vec![step_table_descriptor.clone()];
        for join_spec in &spec.joins {
            let joined_descriptor = schema.get(&join_spec.table)?.columns.clone();
            step_table_descriptors.push(joined_descriptor);
        }
        let combined_step_descriptor = RowDescriptor::combine(&step_table_descriptors);
        let mut step_output_descriptor = if let Some(cols) = &spec.select_columns {
            let columns = cols
                .iter()
                .filter_map(|name| {
                    combined_step_descriptor
                        .columns
                        .iter()
                        .find(|c| c.name.as_str() == name)
                        .cloned()
                })
                .collect::<Vec<_>>();
            RowDescriptor::new(columns)
        } else {
            combined_step_descriptor
        };
        if let Some(element_index) = spec.result_element_index {
            step_output_descriptor = step_table_descriptors.get(element_index)?.clone();
        }

        let hop = if let Some(hop_spec) = &spec.hop {
            let target_schema = schema.get(&hop_spec.table)?;
            if !descriptors_compatible_by_shape(current_descriptor, &target_schema.columns) {
                return None;
            }

            let step_column_index = step_output_descriptor.column_index(&hop_spec.via_column)?;
            Some(RecursiveHop {
                table: hop_spec.table,
                step_column_index,
            })
        } else {
            // MVP constraint: recursive step projection must align with the seed descriptor by shape.
            if !descriptors_compatible_by_shape(current_descriptor, &step_output_descriptor) {
                return None;
            }
            None
        };

        let step_template = SubgraphTemplate::new(
            step_query,
            spec.inner_column.clone(),
            spec.select_columns.clone().unwrap_or_default(),
            step_output_descriptor,
            schema_context.clone(),
        );

        let input_descriptor =
            TupleDescriptor::single_with_materialization("", current_descriptor.clone(), true);
        let node = RecursiveRelationNode::new(
            input_descriptor,
            current_descriptor.clone(),
            step_template,
            correlation_source,
            hop,
            spec.max_depth,
            schema.clone(),
        );

        Some((node, current_descriptor.clone(), spec.table))
    }

    fn compile_indexed_query_plan(
        plan: &ExecutionQueryPlan,
        schema: &Schema,
        branches: &[String],
        session: Option<Session>,
        schema_context: &SchemaContext,
        branch_schema_map: &HashMap<String, SchemaHash>,
    ) -> Option<Self> {
        if plan.include_deleted || plan.recursive.is_some() {
            return None;
        }
        if plan.limit.is_none() && plan.offset == 0 {
            return None;
        }
        if !plan.joins.is_empty() && !plan.array_subqueries.is_empty() {
            return None;
        }

        let base_table_schema = schema.get(&plan.table)?;
        let base_descriptor = base_table_schema.columns.clone();

        let mut scope_names = vec![plan.base_scope.clone()];
        let mut table_names = vec![plan.table];
        let mut table_descriptors = vec![base_descriptor.clone()];
        let mut seen_tables = HashSet::new();
        seen_tables.insert(plan.table.as_str().to_string());

        for join_spec in &plan.joins {
            let join_table_name = join_spec.table.as_str().to_string();
            if seen_tables.contains(&join_table_name) {
                return None;
            }

            let joined_schema = schema.get(&join_spec.table)?;
            scope_names.push(join_spec.effective_name().to_string());
            table_names.push(join_spec.table);
            table_descriptors.push(joined_schema.columns.clone());
            seen_tables.insert(join_table_name);
        }

        let tuple_descriptor = TupleDescriptor::from_tables(
            &scope_names
                .iter()
                .cloned()
                .zip(table_descriptors.iter().cloned())
                .collect::<Vec<_>>(),
        )
        .with_all_materialized();
        let combined_descriptor = RowDescriptor::combine(&table_descriptors);

        let (driver_scope_index, driver_key, sort_keys) = if plan.order_by.is_empty() {
            let driver_key = ResolvedRowKey::from_descriptor(&base_descriptor, "_id")?;
            (
                0,
                driver_key,
                vec![ResolvedSortKey {
                    target: ResolvedSortTarget::RowId { element_index: 0 },
                    direction: SortDirection::Ascending,
                }],
            )
        } else {
            let mut resolved_sort_keys = Vec::with_capacity(plan.order_by.len());
            let mut driver_scope_index = None;
            let mut driver_key = None;

            for (column, direction) in &plan.order_by {
                let (scope_index, key, sort_key) =
                    resolve_sort_key(&tuple_descriptor, column, *direction)?;
                if driver_scope_index.is_none() {
                    driver_scope_index = Some(scope_index);
                    driver_key = Some(key.clone());
                }
                resolved_sort_keys.push(sort_key);
            }

            (driver_scope_index?, driver_key?, resolved_sort_keys)
        };

        let driver = DriverPlan {
            scope_index: driver_scope_index,
            table: table_names[driver_scope_index],
            key: driver_key.clone(),
            direction: match sort_keys.first()?.direction {
                SortDirection::Ascending => IndexScanDirection::Ascending,
                SortDirection::Descending => IndexScanDirection::Descending,
            },
            sort_keys,
        };

        let mut join_edges = Vec::with_capacity(plan.joins.len());
        for (join_index, join_spec) in plan.joins.iter().enumerate() {
            let (left_col, right_col) = join_spec.on.as_ref()?;
            let left_candidates = (0..=join_index).collect::<Vec<_>>();
            let right_scope_index = join_index + 1;
            let (left_scope_index, left_key) =
                resolve_row_key_in_scopes(&tuple_descriptor, left_col, &left_candidates)?;
            let (resolved_right_scope_index, right_key) =
                resolve_row_key_in_scopes(&tuple_descriptor, right_col, &[right_scope_index])?;
            if resolved_right_scope_index != right_scope_index {
                return None;
            }

            join_edges.push(JoinEdgePlan {
                left_scope_index,
                right_scope_index,
                left_table: table_names[left_scope_index],
                right_table: table_names[right_scope_index],
                left_key,
                right_key,
            });
        }

        let residual_filter = match disjuncts_to_predicate(&plan.disjuncts, &tuple_descriptor) {
            Predicate::True => None,
            predicate => Some(FilterNode::with_tuple_descriptor(
                tuple_descriptor.clone(),
                predicate,
            )),
        };

        let policy_branch = branches
            .first()
            .cloned()
            .unwrap_or_else(|| "main".to_string());
        let mut policies = Vec::new();
        let mut policy_dependencies = Vec::new();
        if let Some(session) = &session {
            for (scope_index, table) in table_names.iter().enumerate() {
                let table_schema = schema.get(table)?;
                let Some(policy) = table_schema.policies.select.using.clone() else {
                    continue;
                };
                if policy == PolicyExpr::True {
                    continue;
                }

                let evaluator = PolicyFilterNode::new_with_branch(
                    table_descriptors[scope_index].clone(),
                    policy.clone(),
                    session.clone(),
                    schema.clone(),
                    table.as_str(),
                    &policy_branch,
                );
                policy_dependencies.extend(
                    evaluator
                        .inherits_tables()
                        .iter()
                        .map(|table| TableName::new(table.as_str())),
                );
                let mut evaluators_by_branch = HashMap::with_capacity(branches.len());
                for branch in branches {
                    evaluators_by_branch.insert(
                        branch.clone(),
                        PolicyFilterNode::new_with_branch(
                            table_descriptors[scope_index].clone(),
                            policy.clone(),
                            session.clone(),
                            schema.clone(),
                            table.as_str(),
                            branch,
                        ),
                    );
                }
                policies.push(TablePolicySpec {
                    scope_index,
                    evaluators_by_branch,
                });
            }
        }

        let source_node = IndexedQueryNode::new(IndexedQueryNodeConfig {
            branches: branches.to_vec(),
            branch_schema_map: branch_schema_map.clone(),
            schema_context: schema_context.clone(),
            tuple_descriptor: tuple_descriptor.clone(),
            table_descriptors: table_descriptors.clone(),
            disjuncts: plan.disjuncts.clone(),
            driver: driver.clone(),
            join_edges: join_edges.clone(),
            residual_filter,
            policies,
            limit: plan.limit,
            offset: plan.offset,
        });

        let mut graph = QueryGraph::new(plan.table, base_descriptor.clone());
        graph.table_descriptors = table_descriptors.clone();

        let source_id = graph.add_node(GraphNode::IndexedQuery(source_node));
        graph.ordered_scan_nodes.push((
            source_id,
            driver.table,
            ColumnName::new(driver.key.index_column()),
        ));
        for edge in &join_edges {
            graph.index_scan_nodes.push((
                source_id,
                edge.left_table,
                ColumnName::new(edge.left_key.index_column()),
            ));
            graph.index_scan_nodes.push((
                source_id,
                edge.right_table,
                ColumnName::new(edge.right_key.index_column()),
            ));
        }
        for dependency in policy_dependencies {
            graph.policy_filter_tables.push((source_id, dependency));
        }
        if plan.limit.is_some() || plan.offset > 0 {
            graph.pagination_node = Some(source_id);
        }

        let mut phase2_input = source_id;
        let mut output_descriptor = if table_descriptors.len() == 1 {
            base_descriptor.clone()
        } else {
            combined_descriptor.clone()
        };
        let mut output_tuple_descriptor = tuple_descriptor.clone();

        if plan.joins.is_empty() {
            for subquery_spec in &plan.array_subqueries {
                if let Some((node, new_descriptor)) = graph.compile_array_subquery(
                    subquery_spec,
                    &output_descriptor,
                    schema,
                    branches,
                    schema_context,
                ) {
                    let node_id = graph.add_node(GraphNode::ArraySubquery(node));
                    graph.add_edge(node_id, phase2_input);
                    graph
                        .array_subquery_tables
                        .push((node_id, subquery_spec.table));
                    phase2_input = node_id;
                    output_descriptor = new_descriptor;
                    output_tuple_descriptor = TupleDescriptor::single_with_materialization(
                        plan.base_scope.as_str(),
                        output_descriptor.clone(),
                        true,
                    );
                }
            }

            if let Some(columns) = &plan.project_columns {
                let project_input = TupleDescriptor::single_with_materialization(
                    plan.base_scope.as_str(),
                    output_descriptor.clone(),
                    true,
                );
                let project_node = ProjectNode::with_project_columns(project_input, columns)?;
                output_descriptor = project_node.output_descriptor().clone();
                output_tuple_descriptor = project_node.output_tuple_descriptor().clone();
                let project_id = graph.add_node(GraphNode::Project(project_node));
                graph.add_edge(project_id, phase2_input);
                phase2_input = project_id;
            }
        } else {
            let natural_projection_element_index =
                plan.project_columns.as_ref().and_then(|columns| {
                    natural_row_projection_element_index(&output_tuple_descriptor, columns)
                });

            if let Some(element_index) = plan
                .result_element_index
                .or(natural_projection_element_index)
            {
                let select_node =
                    SelectElementNode::new(output_tuple_descriptor.clone(), element_index)?;
                output_descriptor = select_node.output_descriptor().clone();
                output_tuple_descriptor = TupleDescriptor::single_with_materialization(
                    "",
                    output_descriptor.clone(),
                    true,
                );
                let select_id = graph.add_node(GraphNode::SelectElement(select_node));
                graph.add_edge(select_id, phase2_input);
                phase2_input = select_id;
            }

            if let Some(columns) = &plan.project_columns
                && natural_projection_element_index.is_none()
            {
                let project_node =
                    ProjectNode::with_project_columns(output_tuple_descriptor.clone(), columns)?;
                output_descriptor = project_node.output_descriptor().clone();
                output_tuple_descriptor = project_node.output_tuple_descriptor().clone();
                let project_id = graph.add_node(GraphNode::Project(project_node));
                graph.add_edge(project_id, phase2_input);
                phase2_input = project_id;
            }
        }

        graph.combined_descriptor = output_descriptor.clone();
        let output_node =
            OutputNode::with_tuple_descriptor(output_tuple_descriptor, OutputMode::Delta);
        let output_id = graph.add_node(GraphNode::Output(output_node));
        graph.add_edge(output_id, phase2_input);
        graph.output_node = output_id;

        Some(graph)
    }

    fn compile_join_plan(
        plan: &ExecutionQueryPlan,
        schema: &Schema,
        branches: &[String],
        session: Option<Session>,
        schema_context: &SchemaContext,
    ) -> Option<Self> {
        let base_table_schema = schema.get(&plan.table)?;
        let base_descriptor = base_table_schema.columns.clone();
        let mut graph = QueryGraph::new(plan.table, base_descriptor.clone());

        let join_branches: Vec<&str> = if branches.is_empty() {
            vec!["main"]
        } else {
            branches.iter().map(String::as_str).collect()
        };

        // Track all table names and descriptors for TupleDescriptor
        let mut table_names = vec![plan.base_scope.clone()];
        let mut table_descriptors = vec![base_descriptor.clone()];
        let mut seen_tables: HashSet<String> = HashSet::new();
        seen_tables.insert(plan.table.as_str().to_string());

        // Build pipeline for base table: per-branch IndexScan (+Union) -> Materialize.
        let mut base_scan_ids = Vec::new();
        for branch in &join_branches {
            let id_column = ColumnName::new("_id");
            let base_scan = IndexScanNode::new_with_branch(
                plan.table,
                id_column,
                *branch,
                ScanCondition::All,
                base_descriptor.clone(),
            );
            let base_scan_id = graph.add_node(GraphNode::IndexScan(base_scan));
            graph
                .index_scan_nodes
                .push((base_scan_id, plan.table, id_column));
            base_scan_ids.push(base_scan_id);
        }
        let base_scan_output = if base_scan_ids.len() > 1 {
            let union_node = UnionNode::new();
            let union_id = graph.add_node(GraphNode::Union(union_node));
            for scan_id in base_scan_ids {
                graph.add_edge(union_id, scan_id);
            }
            union_id
        } else {
            *base_scan_ids.first()?
        };

        let base_tuple_desc = TupleDescriptor::single_with_materialization(
            plan.base_scope.as_str(),
            base_descriptor.clone(),
            true,
        );
        let base_mat = MaterializeNode::new_all(base_tuple_desc);
        let base_mat_id = graph.add_node(GraphNode::Materialize(base_mat));
        graph.add_edge(base_mat_id, base_scan_output);

        // Track current left side descriptor (accumulates columns from joins)
        let mut left_id = base_mat_id;
        if let (Some(session), Some(policy)) =
            (&session, base_table_schema.policies.select.using.clone())
        {
            let branch_for_policy = branches
                .first()
                .cloned()
                .unwrap_or_else(|| "main".to_string());
            let policy_node = PolicyFilterNode::new_with_branch(
                base_descriptor.clone(),
                policy,
                session.clone(),
                schema.clone(),
                plan.table.as_str(),
                branch_for_policy,
            );
            let inherits_tables: Vec<TableName> = policy_node
                .inherits_tables()
                .iter()
                .map(TableName::new)
                .collect();
            let policy_id = graph.add_node(GraphNode::PolicyFilter(policy_node));
            graph.add_edge(policy_id, left_id);
            for inherits_table in inherits_tables {
                graph.policy_filter_tables.push((policy_id, inherits_table));
            }
            left_id = policy_id;
        }
        let mut left_descriptor = base_descriptor.clone();

        if let Some(recursive_spec) = &plan.recursive
            && let Some((node, new_descriptor, step_table)) = graph.compile_recursive_relation(
                recursive_spec,
                &left_descriptor,
                schema,
                branches,
                schema_context,
            )
        {
            let node_id = graph.add_node(GraphNode::RecursiveRelation(node));
            graph.add_edge(node_id, left_id);
            graph.recursive_relation_tables.push((node_id, step_table));
            left_id = node_id;
            left_descriptor = new_descriptor;
            if let Some(first) = table_descriptors.first_mut() {
                *first = left_descriptor.clone();
            }
        }

        // Process each join
        for join_spec in &plan.joins {
            let join_table_name = join_spec.table.as_str().to_string();
            if seen_tables.contains(&join_table_name) {
                // Self/circular join chains are not yet supported in the execution graph.
                return None;
            }
            let (left_col, right_col) = join_spec.on.as_ref()?;

            let right_table_schema = schema.get(&join_spec.table)?;
            let right_descriptor = right_table_schema.columns.clone();

            // Build pipeline for right table: per-branch IndexScan (+Union) -> Materialize.
            let mut right_scan_ids = Vec::new();
            for branch in &join_branches {
                let id_column = ColumnName::new("_id");
                let right_scan = IndexScanNode::new_with_branch(
                    join_spec.table,
                    id_column,
                    *branch,
                    ScanCondition::All,
                    right_descriptor.clone(),
                );
                let right_scan_id = graph.add_node(GraphNode::IndexScan(right_scan));
                graph
                    .index_scan_nodes
                    .push((right_scan_id, join_spec.table, id_column));
                right_scan_ids.push(right_scan_id);
            }
            let right_scan_output = if right_scan_ids.len() > 1 {
                let union_node = UnionNode::new();
                let union_id = graph.add_node(GraphNode::Union(union_node));
                for scan_id in right_scan_ids {
                    graph.add_edge(union_id, scan_id);
                }
                union_id
            } else {
                *right_scan_ids.first()?
            };

            let right_tuple_desc = TupleDescriptor::single_with_materialization(
                join_spec.effective_name(),
                right_descriptor.clone(),
                true,
            );
            let right_mat = MaterializeNode::new_all(right_tuple_desc);
            let right_mat_id = graph.add_node(GraphNode::Materialize(right_mat));
            graph.add_edge(right_mat_id, right_scan_output);
            let mut right_input_id = right_mat_id;
            if let (Some(session), Some(policy)) =
                (&session, right_table_schema.policies.select.using.clone())
            {
                let branch_for_policy = branches
                    .first()
                    .cloned()
                    .unwrap_or_else(|| "main".to_string());
                let policy_node = PolicyFilterNode::new_with_branch(
                    right_descriptor.clone(),
                    policy,
                    session.clone(),
                    schema.clone(),
                    join_spec.table.as_str(),
                    branch_for_policy,
                );
                let inherits_tables: Vec<TableName> = policy_node
                    .inherits_tables()
                    .iter()
                    .map(TableName::new)
                    .collect();
                let policy_id = graph.add_node(GraphNode::PolicyFilter(policy_node));
                graph.add_edge(policy_id, right_input_id);
                for inherits_table in inherits_tables {
                    graph.policy_filter_tables.push((policy_id, inherits_table));
                }
                right_input_id = policy_id;
            }

            // Build tuple descriptors with table/alias labels so qualified ON refs can resolve.
            let left_tuple_desc = TupleDescriptor::from_tables(
                &table_names
                    .iter()
                    .cloned()
                    .zip(table_descriptors.iter().cloned())
                    .collect::<Vec<_>>(),
            )
            .with_all_materialized();
            let right_tuple_desc = TupleDescriptor::single_with_materialization(
                join_spec.effective_name(),
                right_descriptor.clone(),
                true,
            );

            let join_node = JoinNode::new_with_refs(
                left_tuple_desc,
                right_tuple_desc,
                JoinColumnRef::parse(left_col),
                JoinColumnRef::parse(right_col),
            )?;
            let join_id = graph.add_node(GraphNode::Join(join_node));

            // JoinNode takes left and right as inputs
            // Using convention: first edge is left, second is right
            graph.add_edge(join_id, left_id);
            graph.add_edge(join_id, right_input_id);

            // Update for next join in chain
            left_id = join_id;

            // Track table name and descriptor for TupleDescriptor
            table_names.push(join_spec.effective_name().to_string());
            table_descriptors.push(right_descriptor.clone());
            seen_tables.insert(join_table_name);

            // Combine descriptors for downstream nodes
            left_descriptor = RowDescriptor::combine(&[left_descriptor, right_descriptor]);
        }

        // Build combined descriptor and TupleDescriptor from all tables
        let combined_descriptor = RowDescriptor::combine(&table_descriptors);
        // For FilterNode, all elements are materialized at this point (after Materialize nodes)
        let tuple_descriptor = TupleDescriptor::from_tables(
            &table_names
                .iter()
                .cloned()
                .zip(table_descriptors.iter().cloned())
                .collect::<Vec<_>>(),
        )
        .with_all_materialized();
        graph.table_descriptors = table_descriptors;
        let mut output_descriptor = combined_descriptor.clone();
        let mut output_tuple_descriptor = tuple_descriptor.clone();

        let mut phase2_input = left_id;

        // Filter node (if conditions exist)
        // Use TupleDescriptor to enable filtering on columns from any joined table
        let predicate = disjuncts_to_predicate(&plan.disjuncts, &tuple_descriptor);
        if !matches!(predicate, Predicate::True) {
            let filter_node =
                FilterNode::with_tuple_descriptor(tuple_descriptor.clone(), predicate);
            let filter_id = graph.add_node(GraphNode::Filter(filter_node));
            graph.add_edge(filter_id, phase2_input);
            phase2_input = filter_id;
        }

        // Sort node (default: id ASC when order_by is omitted)
        let sort_keys = sort_keys_from_order_by(&plan.order_by, &combined_descriptor);
        if !sort_keys.is_empty() {
            let sort_node = SortNode::with_tuple_descriptor(tuple_descriptor.clone(), sort_keys);
            let sort_id = graph.add_node(GraphNode::Sort(sort_node));
            graph.add_edge(sort_id, phase2_input);
            phase2_input = sort_id;
        }

        // LimitOffset node (if limit or offset specified)
        if plan.limit.is_some() || plan.offset > 0 {
            let limit_offset_node = LimitOffsetNode::with_tuple_descriptor(
                tuple_descriptor.clone(),
                plan.limit,
                plan.offset,
            );
            let limit_offset_id = graph.add_node(GraphNode::LimitOffset(limit_offset_node));
            graph.add_edge(limit_offset_id, phase2_input);
            graph.pagination_node = Some(limit_offset_id);
            phase2_input = limit_offset_id;
        }

        let natural_projection_element_index = plan.project_columns.as_ref().and_then(|columns| {
            natural_row_projection_element_index(&output_tuple_descriptor, columns)
        });

        // Optional output projection to a specific joined element.
        if let Some(element_index) = plan
            .result_element_index
            .or(natural_projection_element_index)
        {
            let select_input_descriptor = output_tuple_descriptor.clone();
            let select_node = SelectElementNode::new(select_input_descriptor, element_index)?;
            output_descriptor = select_node.output_descriptor().clone();
            output_tuple_descriptor =
                TupleDescriptor::single_with_materialization("", output_descriptor.clone(), true);
            let select_id = graph.add_node(GraphNode::SelectElement(select_node));
            graph.add_edge(select_id, phase2_input);
            phase2_input = select_id;
        }

        // Project node (if projection specified)
        if let Some(columns) = &plan.project_columns
            && natural_projection_element_index.is_none()
        {
            let project_node =
                ProjectNode::with_project_columns(output_tuple_descriptor.clone(), columns)?;
            output_descriptor = project_node.output_descriptor().clone();
            output_tuple_descriptor = project_node.output_tuple_descriptor().clone();
            let project_id = graph.add_node(GraphNode::Project(project_node));
            graph.add_edge(project_id, phase2_input);
            phase2_input = project_id;
        }

        // Output node
        graph.combined_descriptor = output_descriptor;
        let output_node =
            OutputNode::with_tuple_descriptor(output_tuple_descriptor, OutputMode::Delta);
        let output_id = graph.add_node(GraphNode::Output(output_node));
        graph.add_edge(output_id, phase2_input);
        graph.output_node = output_id;

        Some(graph)
    }

    /// Mark index scan nodes dirty for a given table/column.
    /// Also propagates dirty marks to downstream nodes.
    pub fn mark_dirty_for_column(&mut self, table: &str, column: &str) {
        let affected: Vec<NodeId> = self
            .index_scan_nodes
            .iter()
            .filter(|(_, t, c)| {
                t.as_str() == table && (c.as_str() == column || c.as_str() == "_id")
            })
            .map(|(node_id, _, _)| *node_id)
            .chain(
                self.ordered_scan_nodes
                    .iter()
                    .filter(|(_, t, c)| {
                        t.as_str() == table && (c.as_str() == column || c.as_str() == "_id")
                    })
                    .map(|(node_id, _, _)| *node_id),
            )
            .collect();
        for node_id in affected {
            self.mark_dirty(node_id);
            self.mark_downstream_dirty(node_id);
        }
    }

    /// Mark all index scan nodes for a table dirty.
    /// Also marks array/recursive subquery nodes dirty if the table is their inner table.
    /// Also marks PolicyFilter nodes dirty if the table is INHERITS-referenced.
    pub fn mark_dirty_for_table(&mut self, table: &str) {
        // Mark index scan nodes and propagate downstream
        let affected_index_scans: Vec<NodeId> = self
            .index_scan_nodes
            .iter()
            .filter_map(|(node_id, t, _)| {
                if t.as_str() == table {
                    Some(*node_id)
                } else {
                    None
                }
            })
            .chain(
                self.ordered_scan_nodes
                    .iter()
                    .filter_map(|(node_id, t, _)| {
                        if t.as_str() == table {
                            Some(*node_id)
                        } else {
                            None
                        }
                    }),
            )
            .collect();

        for node_id in affected_index_scans {
            self.mark_dirty(node_id);
            self.mark_downstream_dirty(node_id);
        }
        // Mark array subquery nodes whose inner table changed
        // Collect node_ids first to avoid borrow conflict
        let affected_array_subqueries: Vec<NodeId> = self
            .array_subquery_tables
            .iter()
            .filter_map(|(node_id, inner_table)| {
                if inner_table.as_str() == table {
                    Some(*node_id)
                } else {
                    None
                }
            })
            .collect();

        for node_id in affected_array_subqueries {
            self.mark_dirty(node_id);
            // Mark the node as needing inner re-evaluation
            if let Some(GraphNode::ArraySubquery(node)) = self.get_node_mut(node_id) {
                node.mark_inner_dirty();
            }
            // Propagate dirty marks to downstream nodes (Output, etc.)
            self.mark_downstream_dirty(node_id);
        }

        // Mark PolicyFilter nodes whose policy dependency tables changed
        let affected_policy_filters: Vec<NodeId> = self
            .policy_filter_tables
            .iter()
            .filter_map(|(node_id, inherits_table)| {
                if inherits_table.as_str() == table {
                    Some(*node_id)
                } else {
                    None
                }
            })
            .collect();

        for node_id in affected_policy_filters {
            self.mark_dirty(node_id);
            // Mark the node as needing policy re-evaluation
            if let Some(GraphNode::PolicyFilter(node)) = self.get_node_mut(node_id) {
                node.mark_inherits_dirty();
            }
            // Propagate dirty marks to downstream nodes
            self.mark_downstream_dirty(node_id);
        }

        // Mark RecursiveRelation nodes whose step table changed
        let affected_recursive_relations: Vec<NodeId> = self
            .recursive_relation_tables
            .iter()
            .filter_map(|(node_id, step_table)| {
                if step_table.as_str() == table {
                    Some(*node_id)
                } else {
                    None
                }
            })
            .collect();

        for node_id in affected_recursive_relations {
            self.mark_dirty(node_id);
            if let Some(GraphNode::RecursiveRelation(node)) = self.get_node_mut(node_id) {
                node.mark_inner_dirty();
            }
            self.mark_downstream_dirty(node_id);
        }
    }

    /// Check if this graph involves a table (as index scan, array subquery inner table, or INHERITS reference).
    pub fn involves_table(&self, table: &str) -> bool {
        self.index_scan_nodes
            .iter()
            .any(|(_, t, _)| t.as_str() == table)
            || self
                .ordered_scan_nodes
                .iter()
                .any(|(_, t, _)| t.as_str() == table)
            || self
                .array_subquery_tables
                .iter()
                .any(|(_, t)| t.as_str() == table)
            || self
                .policy_filter_tables
                .iter()
                .any(|(_, t)| t.as_str() == table)
            || self
                .recursive_relation_tables
                .iter()
                .any(|(_, t)| t.as_str() == table)
    }

    /// Check if this graph uses a specific index (table + column combination).
    pub fn uses_index(&self, table: &str, column: &str) -> bool {
        self.index_scan_nodes
            .iter()
            .any(|(_, t, c)| t.as_str() == table && c.as_str() == column)
            || self
                .ordered_scan_nodes
                .iter()
                .any(|(_, t, c)| t.as_str() == table && c.as_str() == column)
    }

    /// Mark a row ID as updated for content checking.
    /// This tells MaterializeNodes to check if the row's content has changed.
    pub fn mark_row_updated(&mut self, id: ObjectId) {
        // First pass: mark the ID as updated in each MaterializeNode and collect node IDs
        let materialize_node_ids: Vec<NodeId> = self
            .nodes
            .iter_mut()
            .enumerate()
            .filter_map(|(idx, compact)| {
                if let GraphNode::Materialize(mat_node) = &mut compact.node {
                    mat_node.mark_updated(id);
                    Some(NodeId(idx as u64))
                } else {
                    None
                }
            })
            .collect();

        // Second pass: mark dirty and propagate downstream
        for node_id in materialize_node_ids {
            self.mark_dirty(node_id);
            self.mark_downstream_dirty(node_id);
        }
    }

    /// Mark a row ID as deleted for removal delta emission.
    /// This tells MaterializeNodes to emit a removal delta for this row.
    pub fn mark_row_deleted(&mut self, id: ObjectId) {
        // First pass: mark the ID as deleted in each MaterializeNode and collect node IDs
        let materialize_node_ids: Vec<NodeId> = self
            .nodes
            .iter_mut()
            .enumerate()
            .filter_map(|(idx, compact)| {
                if let GraphNode::Materialize(mat_node) = &mut compact.node {
                    mat_node.mark_deleted(id);
                    Some(NodeId(idx as u64))
                } else {
                    None
                }
            })
            .collect();

        // Second pass: mark dirty and propagate downstream
        for node_id in materialize_node_ids {
            self.mark_dirty(node_id);
            self.mark_downstream_dirty(node_id);
        }
    }

    /// Mark all nodes that depend on the given node as dirty (propagate forward).
    fn mark_downstream_dirty(&mut self, node_id: NodeId) {
        if let Some(outputs) = self.get_outputs(node_id) {
            let parents: SmallVec<[NodeId; 2]> = outputs.iter().copied().collect();
            for parent in parents {
                // Only recurse if not already dirty (avoid infinite loops)
                if !self.is_dirty(parent) {
                    self.mark_dirty(parent);
                    // Recursively mark parents of parent
                    self.mark_downstream_dirty(parent);
                }
            }
        }
    }

    /// Topological sort of dirty nodes (dependencies first).
    fn topo_sort_dirty(&self) -> Vec<NodeId> {
        let mut result = Vec::new();
        let mut visited = AHashSet::new();

        fn visit(
            node: NodeId,
            graph: &QueryGraph,
            visited: &mut AHashSet<NodeId>,
            result: &mut Vec<NodeId>,
        ) {
            if visited.contains(&node) {
                return;
            }
            visited.insert(node);

            // Visit dependencies first (inputs)
            if let Some(compact) = graph.nodes.get(node.0 as usize) {
                for dep in &compact.inputs {
                    visit(*dep, graph, visited, result);
                }
            }

            result.push(node);
        }

        // Iterate over dirty nodes using BitVec's iter_ones()
        for idx in self.dirty_bitmap.iter_ones() {
            visit(NodeId(idx as u64), self, &mut visited, &mut result);
        }

        result
    }

    fn ordered_tuples_for_node(&self, node_id: NodeId) -> Option<Vec<Tuple>> {
        match self.get_node(node_id)? {
            GraphNode::IndexedQuery(node) => Some(node.windowed_tuples().to_vec()),
            GraphNode::LimitOffset(node) => Some(node.windowed_tuples().to_vec()),
            GraphNode::Sort(node) => Some(node.sorted_tuples().to_vec()),
            GraphNode::Materialize(node) => {
                let input_id = self.get_inputs(node_id).first().copied()?;
                let ordered_input = self.ordered_tuples_for_node(input_id)?;
                Some(
                    ordered_input
                        .into_iter()
                        .filter_map(|tuple| node.current_tuples().get(&tuple).cloned())
                        .collect(),
                )
            }
            GraphNode::Filter(node) => {
                let input_id = self.get_inputs(node_id).first().copied()?;
                let ordered_input = self.ordered_tuples_for_node(input_id)?;
                Some(
                    ordered_input
                        .into_iter()
                        .filter_map(|tuple| node.current_tuples().get(&tuple).cloned())
                        .collect(),
                )
            }
            GraphNode::PolicyFilter(node) => {
                let input_id = self.get_inputs(node_id).first().copied()?;
                let ordered_input = self.ordered_tuples_for_node(input_id)?;
                Some(
                    ordered_input
                        .into_iter()
                        .filter_map(|tuple| node.current_tuples().get(&tuple).cloned())
                        .collect(),
                )
            }
            GraphNode::ArraySubquery(node) => {
                let input_id = self.get_inputs(node_id).first().copied()?;
                let ordered_input = self.ordered_tuples_for_node(input_id)?;
                Some(
                    ordered_input
                        .into_iter()
                        .filter_map(|tuple| node.current_tuples().get(&tuple).cloned())
                        .collect(),
                )
            }
            GraphNode::Project(node) => {
                let input_id = self.get_inputs(node_id).first().copied()?;
                let ordered_input = self.ordered_tuples_for_node(input_id)?;
                let mut seen = AHashSet::new();
                let mut result = Vec::new();
                for tuple in ordered_input {
                    let projected = node.project_tuple_for_output(&tuple)?;
                    let current = node.current_tuples().get(&projected)?.clone();
                    if seen.insert(current.clone()) {
                        result.push(current);
                    }
                }
                Some(result)
            }
            GraphNode::SelectElement(node) => {
                let input_id = self.get_inputs(node_id).first().copied()?;
                let ordered_input = self.ordered_tuples_for_node(input_id)?;
                let mut seen = AHashSet::new();
                let mut result = Vec::new();
                for tuple in ordered_input {
                    let selected = node.select_tuple_for_output(&tuple)?;
                    let current = node.current_tuples().get(&selected)?.clone();
                    if seen.insert(current.clone()) {
                        result.push(current);
                    }
                }
                Some(result)
            }
            _ => None,
        }
    }

    /// Settle the graph - process all dirty nodes in topological order.
    /// Uses tuple-based processing internally, converts to RowDelta for output.
    pub fn settle<F>(&mut self, storage: &dyn Storage, mut row_loader: F) -> RowDelta
    where
        F: FnMut(ObjectId) -> Option<LoadedRow>,
    {
        let order = self.topo_sort_dirty();
        if !order.is_empty() {
            tracing::trace!(dirty_nodes = order.len(), table = %self.table, "settling query graph");
        }
        let mut tuple_deltas: AHashMap<NodeId, TupleDelta> = AHashMap::new();

        let ctx = SourceContext { storage };

        for node_id in order {
            let node_type = match self.get_node(node_id) {
                Some(GraphNode::IndexScan(_)) => "IndexScan",
                Some(GraphNode::IndexedQuery(_)) => "IndexedQuery",
                Some(GraphNode::Union(_)) => "Union",
                Some(GraphNode::Alias(_)) => "Alias",
                Some(GraphNode::Join(_)) => "Join",
                Some(GraphNode::Project(_)) => "Project",
                Some(GraphNode::SelectElement(_)) => "SelectElement",
                Some(GraphNode::RecursiveRelation(_)) => "RecursiveRelation",
                Some(GraphNode::Materialize(_)) => "Materialize",
                Some(GraphNode::Filter(_)) => "Filter",
                Some(GraphNode::PolicyFilter(_)) => "PolicyFilter",
                Some(GraphNode::Sort(_)) => "Sort",
                Some(GraphNode::LimitOffset(_)) => "LimitOffset",
                Some(GraphNode::ArraySubquery(_)) => "ArraySubquery",
                Some(GraphNode::Output(_)) => "Output",
                Some(GraphNode::ExistsOutput(_)) => "ExistsOutput",
                None => "Unknown",
            };

            match self.get_node(node_id) {
                Some(GraphNode::IndexScan(_)) => {
                    if let Some(GraphNode::IndexScan(scan_node)) = self.get_node_mut(node_id) {
                        let delta = SourceNode::scan(scan_node, &ctx);
                        tracing::debug!(
                            node_id = node_id.0,
                            node_type,
                            added = delta.added.len(),
                            removed = delta.removed.len(),
                            "graph node evaluated"
                        );
                        tuple_deltas.insert(node_id, delta);
                    }
                }
                Some(GraphNode::IndexedQuery(_)) => {
                    if let Some(GraphNode::IndexedQuery(scan_node)) = self.get_node_mut(node_id) {
                        let delta = scan_node.scan_with_context(&ctx, &mut row_loader);
                        tracing::debug!(
                            node_id = node_id.0,
                            node_type,
                            added = delta.added.len(),
                            removed = delta.removed.len(),
                            "graph node evaluated"
                        );
                        tuple_deltas.insert(node_id, delta);
                    }
                }
                Some(GraphNode::Union(_)) => {
                    let inputs = self.collect_tuple_inputs(node_id);
                    if let Some(GraphNode::Union(union_node)) = self.get_node_mut(node_id) {
                        let input_refs: Vec<_> = inputs.iter().collect();
                        let delta = TransformNode::process(union_node, &input_refs);
                        tracing::debug!(
                            node_id = node_id.0,
                            node_type,
                            added = delta.added.len(),
                            removed = delta.removed.len(),
                            "graph node evaluated"
                        );
                        tuple_deltas.insert(node_id, delta);
                    }
                }
                Some(GraphNode::Alias(_)) => {
                    let input_delta = self
                        .get_inputs(node_id)
                        .first()
                        .and_then(|dep| tuple_deltas.get(dep).cloned())
                        .unwrap_or_default();

                    if let Some(GraphNode::Alias(alias_node)) = self.get_node_mut(node_id) {
                        let delta = RowNode::process(alias_node, input_delta);
                        tracing::debug!(
                            node_id = node_id.0,
                            node_type,
                            added = delta.added.len(),
                            removed = delta.removed.len(),
                            "graph node evaluated"
                        );
                        tuple_deltas.insert(node_id, delta);
                    }
                }
                Some(GraphNode::Join(_)) => {
                    // JoinNode has two inputs: left (index 0) and right (index 1)
                    let inputs = self.get_inputs(node_id);
                    let left_delta = inputs
                        .first()
                        .and_then(|dep| tuple_deltas.get(dep).cloned())
                        .unwrap_or_default();
                    let right_delta = inputs
                        .get(1)
                        .and_then(|dep| tuple_deltas.get(dep).cloned())
                        .unwrap_or_default();

                    if let Some(GraphNode::Join(join_node)) = self.get_node_mut(node_id) {
                        // Process left side first, then right side
                        let left_result = join_node.process_left(left_delta);
                        let right_result = join_node.process_right(right_delta);

                        // Merge results
                        let mut merged = TupleDelta::new();
                        merged.added.extend(left_result.added);
                        merged.added.extend(right_result.added);
                        merged.removed.extend(left_result.removed);
                        merged.removed.extend(right_result.removed);

                        tracing::debug!(
                            node_id = node_id.0,
                            node_type,
                            added = merged.added.len(),
                            removed = merged.removed.len(),
                            "graph node evaluated"
                        );
                        tuple_deltas.insert(node_id, merged);
                    }
                }
                Some(GraphNode::Project(_)) => {
                    let input_delta = self
                        .get_inputs(node_id)
                        .first()
                        .and_then(|dep| tuple_deltas.get(dep).cloned())
                        .unwrap_or_default();

                    if let Some(GraphNode::Project(project_node)) = self.get_node_mut(node_id) {
                        let delta = RowNode::process(project_node, input_delta);
                        tracing::debug!(
                            node_id = node_id.0,
                            node_type,
                            added = delta.added.len(),
                            removed = delta.removed.len(),
                            "graph node evaluated"
                        );
                        tuple_deltas.insert(node_id, delta);
                    }
                }
                Some(GraphNode::SelectElement(_)) => {
                    let input_delta = self
                        .get_inputs(node_id)
                        .first()
                        .and_then(|dep| tuple_deltas.get(dep).cloned())
                        .unwrap_or_default();

                    if let Some(GraphNode::SelectElement(select_node)) = self.get_node_mut(node_id)
                    {
                        let delta = RowNode::process(select_node, input_delta);
                        tracing::debug!(
                            node_id = node_id.0,
                            node_type,
                            added = delta.added.len(),
                            removed = delta.removed.len(),
                            "graph node evaluated"
                        );
                        tuple_deltas.insert(node_id, delta);
                    }
                }
                Some(GraphNode::RecursiveRelation(_)) => {
                    let input_delta = self
                        .get_inputs(node_id)
                        .first()
                        .and_then(|dep| tuple_deltas.get(dep).cloned())
                        .unwrap_or_default();

                    if let Some(GraphNode::RecursiveRelation(recursive_node)) =
                        self.get_node_mut(node_id)
                    {
                        let delta = recursive_node.process_with_context(
                            input_delta,
                            storage,
                            &mut row_loader,
                        );
                        tracing::debug!(
                            node_id = node_id.0,
                            node_type,
                            added = delta.added.len(),
                            removed = delta.removed.len(),
                            "graph node evaluated"
                        );
                        tuple_deltas.insert(node_id, delta);
                    }
                }
                Some(GraphNode::Materialize(_)) => {
                    let input_delta = self
                        .get_inputs(node_id)
                        .first()
                        .and_then(|dep| tuple_deltas.get(dep).cloned())
                        .unwrap_or_default();

                    if let Some(GraphNode::Materialize(mat_node)) = self.get_node_mut(node_id) {
                        let deleted_delta = mat_node.check_deleted_tuples();
                        let new_delta = mat_node.materialize_tuples(input_delta, &mut row_loader);
                        let update_delta = mat_node.check_updated_tuples(&mut row_loader);

                        let mut merged = TupleDelta::new();
                        merged.added.extend(new_delta.added);
                        merged.removed.extend(deleted_delta.removed);
                        merged.removed.extend(new_delta.removed);
                        merged.updated.extend(new_delta.updated);
                        merged.updated.extend(update_delta.updated);

                        tracing::debug!(
                            node_id = node_id.0,
                            node_type,
                            added = merged.added.len(),
                            removed = merged.removed.len(),
                            "graph node evaluated"
                        );
                        tuple_deltas.insert(node_id, merged);
                    }
                }
                Some(GraphNode::Filter(_)) => {
                    let input_delta = self
                        .get_inputs(node_id)
                        .first()
                        .and_then(|dep| tuple_deltas.get(dep).cloned())
                        .unwrap_or_default();

                    if let Some(GraphNode::Filter(filter_node)) = self.get_node_mut(node_id) {
                        let delta = RowNode::process(filter_node, input_delta);
                        tracing::debug!(
                            node_id = node_id.0,
                            node_type,
                            added = delta.added.len(),
                            removed = delta.removed.len(),
                            "graph node evaluated"
                        );
                        tuple_deltas.insert(node_id, delta);
                    }
                }
                Some(GraphNode::PolicyFilter(_)) => {
                    let input_delta = self
                        .get_inputs(node_id)
                        .first()
                        .and_then(|dep| tuple_deltas.get(dep).cloned())
                        .unwrap_or_default();

                    if let Some(GraphNode::PolicyFilter(policy_node)) = self.get_node_mut(node_id) {
                        // Use process_with_context if the policy has INHERITS clauses
                        let delta = if policy_node.has_inherits() {
                            policy_node.process_with_context(input_delta, storage, &mut |id| {
                                row_loader(id)
                            })
                        } else {
                            RowNode::process(policy_node, input_delta)
                        };
                        tracing::debug!(
                            node_id = node_id.0,
                            node_type,
                            added = delta.added.len(),
                            removed = delta.removed.len(),
                            "graph node evaluated"
                        );
                        tuple_deltas.insert(node_id, delta);
                    }
                }
                Some(GraphNode::Sort(_)) => {
                    let input_delta = self
                        .get_inputs(node_id)
                        .first()
                        .and_then(|dep| tuple_deltas.get(dep).cloned())
                        .unwrap_or_default();

                    if let Some(GraphNode::Sort(sort_node)) = self.get_node_mut(node_id) {
                        let delta = RowNode::process(sort_node, input_delta);
                        tracing::debug!(
                            node_id = node_id.0,
                            node_type,
                            added = delta.added.len(),
                            removed = delta.removed.len(),
                            "graph node evaluated"
                        );
                        tuple_deltas.insert(node_id, delta);
                    }
                }
                Some(GraphNode::LimitOffset(_)) => {
                    let input_node = self.get_inputs(node_id).first().copied();
                    let ordered_input =
                        input_node.and_then(|dep| self.ordered_tuples_for_node(dep));

                    if let Some(GraphNode::LimitOffset(lo_node)) = self.get_node_mut(node_id) {
                        let delta = if let Some(ordered) = ordered_input {
                            lo_node.process_with_ordered_input(&ordered)
                        } else {
                            let input_delta = input_node
                                .and_then(|dep| tuple_deltas.get(&dep).cloned())
                                .unwrap_or_default();
                            RowNode::process(lo_node, input_delta)
                        };
                        tracing::debug!(
                            node_id = node_id.0,
                            node_type,
                            added = delta.added.len(),
                            removed = delta.removed.len(),
                            "graph node evaluated"
                        );
                        tuple_deltas.insert(node_id, delta);
                    }
                }
                Some(GraphNode::ArraySubquery(_)) => {
                    let input_delta = self
                        .get_inputs(node_id)
                        .first()
                        .and_then(|dep| tuple_deltas.get(dep).cloned())
                        .unwrap_or_default();

                    if let Some(GraphNode::ArraySubquery(subquery_node)) =
                        self.get_node_mut(node_id)
                    {
                        // Check if inner table changed - need to reevaluate all existing instances
                        let mut delta = if subquery_node.is_inner_dirty() {
                            subquery_node.reevaluate_all(storage, &mut |id| row_loader(id))
                        } else {
                            TupleDelta::new()
                        };

                        // Process outer input changes
                        let outer_delta = subquery_node.process_with_context(
                            input_delta,
                            storage,
                            &mut row_loader,
                        );

                        // Merge outer delta into combined delta
                        delta.merge(outer_delta);
                        tracing::debug!(
                            node_id = node_id.0,
                            node_type,
                            added = delta.added.len(),
                            removed = delta.removed.len(),
                            "graph node evaluated"
                        );
                        tuple_deltas.insert(node_id, delta);
                    }
                }
                Some(GraphNode::Output(_)) => {
                    let input_node = self.get_inputs(node_id).first().copied();
                    let ordered_input =
                        input_node.and_then(|dep| self.ordered_tuples_for_node(dep));

                    if let Some(GraphNode::Output(output_node)) = self.get_node_mut(node_id) {
                        let delta = if let Some(ordered) = ordered_input {
                            output_node.process_with_ordered_input(&ordered)
                        } else {
                            let input_delta = input_node
                                .and_then(|dep| tuple_deltas.get(&dep).cloned())
                                .unwrap_or_default();
                            RowNode::process(output_node, input_delta)
                        };
                        tracing::debug!(
                            node_id = node_id.0,
                            node_type,
                            added = delta.added.len(),
                            removed = delta.removed.len(),
                            "graph node evaluated"
                        );
                        tuple_deltas.insert(node_id, delta);
                    }
                }
                Some(GraphNode::ExistsOutput(_)) => {
                    let input_delta = self
                        .get_inputs(node_id)
                        .first()
                        .and_then(|dep| tuple_deltas.get(dep).cloned())
                        .unwrap_or_default();

                    if let Some(GraphNode::ExistsOutput(exists_node)) = self.get_node_mut(node_id) {
                        let delta = RowNode::process(exists_node, input_delta);
                        tracing::debug!(
                            node_id = node_id.0,
                            node_type,
                            added = delta.added.len(),
                            removed = delta.removed.len(),
                            "graph node evaluated"
                        );
                        tuple_deltas.insert(node_id, delta);
                    }
                }
                None => {}
            }
        }

        self.dirty_bitmap.fill(false);

        // Convert TupleDelta to RowDelta for output
        // For single-table queries: use simple conversion
        // For join queries: flatten multi-element tuples using table descriptors
        tuple_deltas
            .remove(&self.output_node)
            .and_then(|td| {
                if self.table_descriptors.len() == 1 {
                    // Single-table query - direct conversion
                    td.to_row_delta()
                } else {
                    // Join query - flatten multi-element tuples
                    td.flatten_to_row_delta(&self.table_descriptors, &self.combined_descriptor)
                }
            })
            .unwrap_or_default()
    }

    /// Collect tuple sets from input nodes for a transform node.
    fn collect_tuple_inputs(&self, node_id: NodeId) -> Vec<AHashSet<Tuple>> {
        self.get_inputs(node_id)
            .iter()
            .filter_map(|dep| match &self.nodes[dep.0 as usize].node {
                GraphNode::IndexScan(n) => Some(n.current_tuples().clone()),
                GraphNode::Union(n) => Some(n.current_tuples().clone()),
                _ => None,
            })
            .collect()
    }

    /// Get current result from output node.
    pub fn current_result(&self) -> Vec<Row> {
        self.current_output_rows_with_provenance()
            .into_iter()
            .map(|(row, _)| row)
            .collect()
    }

    /// Get the current output tuples in output order.
    pub fn current_output_tuples(&self) -> Vec<Tuple> {
        match self.get_node(self.output_node) {
            Some(GraphNode::Output(node)) => node.ordered_tuples().to_vec(),
            _ => vec![],
        }
    }

    pub(crate) fn current_output_rows_with_provenance(
        &self,
    ) -> Vec<(Row, crate::query_manager::types::TupleProvenance)> {
        self.current_output_tuples()
            .into_iter()
            .filter_map(|tuple| {
                let row = if tuple.len() == 1 {
                    tuple.to_single_row()
                } else {
                    tuple
                        .flatten_with_descriptors(
                            &self.table_descriptors,
                            &self.combined_descriptor,
                        )
                        .and_then(|flattened| flattened.to_single_row())
                }?;
                Some((row, tuple.provenance().clone()))
            })
            .collect()
    }

    /// Returns all current output rows as a RowDelta with everything in `added`.
    /// Used for first delivery after tier-gated settlement.
    pub fn current_result_as_delta(&self) -> RowDelta {
        let output_tuples = self.current_output_tuples();

        if output_tuples.is_empty() {
            return RowDelta::default();
        }

        let td = TupleDelta {
            added: output_tuples,
            removed: vec![],
            moved: vec![],
            updated: vec![],
        };

        if self.table_descriptors.len() == 1 {
            td.to_row_delta().unwrap_or_default()
        } else {
            td.flatten_to_row_delta(&self.table_descriptors, &self.combined_descriptor)
                .unwrap_or_default()
        }
    }

    // ========================================================================
    // Memory profiling
    // ========================================================================

    /// Estimate memory size of this QueryGraph.
    pub fn estimate_memory_size(&self) -> usize {
        let mut size = std::mem::size_of::<Self>();

        // Nodes Vec with CompactNode (node + inline edges)
        for compact in &self.nodes {
            size += std::mem::size_of::<CompactNode>() + 512; // estimate node size
            size += compact.inputs.len() * std::mem::size_of::<NodeId>();
            size += compact.outputs.len() * std::mem::size_of::<NodeId>();
        }

        // Dirty bitmap (1 bit per node)
        size += self.dirty_bitmap.len() / 8 + 1;

        // Table name (interned - shared, but count the string length for this ref)
        size += self.table.as_str().len();

        // Index scan nodes (interned - pointer sized, but count string lengths for reference)
        for (_, table, col) in &self.index_scan_nodes {
            size += std::mem::size_of::<NodeId>() + table.as_str().len() + col.as_str().len();
        }

        // Array subquery tables
        for (_, table) in &self.array_subquery_tables {
            size += std::mem::size_of::<NodeId>() + table.as_str().len();
        }

        // Policy filter tables
        for (_, table) in &self.policy_filter_tables {
            size += std::mem::size_of::<NodeId>() + table.as_str().len();
        }

        // Recursive relation tables
        for (_, table) in &self.recursive_relation_tables {
            size += std::mem::size_of::<NodeId>() + table.as_str().len();
        }

        // Table descriptors - estimate 200 bytes per descriptor
        size += self.table_descriptors.len() * 200;

        // Combined descriptor
        size += 200;

        size
    }
}

fn ensure_relation_tables_exist(
    relation: &RelExpr,
    schema: &Schema,
) -> Result<(), QueryCompileError> {
    match relation {
        RelExpr::TableScan { table } => {
            if schema.get(table).is_some() {
                Ok(())
            } else {
                Err(QueryCompileError::UnknownTable(*table))
            }
        }
        RelExpr::Filter { input, .. }
        | RelExpr::Project { input, .. }
        | RelExpr::Distinct { input, .. }
        | RelExpr::OrderBy { input, .. }
        | RelExpr::Offset { input, .. }
        | RelExpr::Limit { input, .. } => ensure_relation_tables_exist(input, schema),
        RelExpr::Join { left, right, .. } => {
            ensure_relation_tables_exist(left, schema)?;
            ensure_relation_tables_exist(right, schema)
        }
        RelExpr::Gather { seed, step, .. } => {
            ensure_relation_tables_exist(seed, schema)?;
            ensure_relation_tables_exist(step, schema)
        }
    }
}

fn unqualify_column_name(column: &str) -> &str {
    column.split('.').next_back().unwrap_or(column)
}

fn validate_condition_for_descriptor(
    descriptor: &RowDescriptor,
    condition: &Condition,
) -> Result<(), QueryCompileError> {
    let column_name = unqualify_column_name(condition.column());
    let Some(column) = descriptor.column(column_name) else {
        return Ok(());
    };

    let is_bytea = matches!(column.column_type, ColumnType::Bytea);
    let is_ordering_cmp = matches!(
        condition,
        Condition::Lt { .. }
            | Condition::Le { .. }
            | Condition::Gt { .. }
            | Condition::Ge { .. }
            | Condition::Between { .. }
    );

    if is_bytea && is_ordering_cmp {
        return Err(QueryCompileError::InvalidPlan(format!(
            "bytea column '{}' only supports '=' and '!=' comparisons",
            column_name
        )));
    }

    Ok(())
}

fn validate_disjuncts_for_descriptor(
    disjuncts: &[Conjunction],
    descriptor: &RowDescriptor,
) -> Result<(), QueryCompileError> {
    for disjunct in disjuncts {
        for condition in &disjunct.conditions {
            validate_condition_for_descriptor(descriptor, condition)?;
        }
    }
    Ok(())
}

fn validate_order_by_for_descriptor(
    order_by: &[(String, SortDirection)],
    descriptor: &RowDescriptor,
) -> Result<(), QueryCompileError> {
    for (column, _direction) in order_by {
        let column_name = unqualify_column_name(column);
        if descriptor
            .column(column_name)
            .is_some_and(|c| matches!(c.column_type, ColumnType::Bytea))
        {
            return Err(QueryCompileError::InvalidPlan(format!(
                "bytea column '{}' cannot be used in ORDER BY",
                column_name
            )));
        }
    }
    Ok(())
}

fn descriptor_for_execution_plan(
    plan: &ExecutionQueryPlan,
    schema: &Schema,
) -> Result<RowDescriptor, QueryCompileError> {
    descriptor_for_table_with_joins(plan.table, &plan.joins, schema)
}

fn descriptor_for_table_with_joins(
    table: TableName,
    joins: &[crate::query_manager::query::JoinSpec],
    schema: &Schema,
) -> Result<RowDescriptor, QueryCompileError> {
    let base = schema
        .get(&table)
        .ok_or(QueryCompileError::UnknownTable(table))?
        .columns
        .clone();
    if joins.is_empty() {
        return Ok(base);
    }

    let mut descriptors = vec![base];
    for join in joins {
        let joined = schema
            .get(&join.table)
            .ok_or(QueryCompileError::UnknownTable(join.table))?
            .columns
            .clone();
        descriptors.push(joined);
    }

    Ok(RowDescriptor::combine(&descriptors))
}

fn validate_array_subquery_spec(
    spec: &ArraySubquerySpec,
    schema: &Schema,
) -> Result<(), QueryCompileError> {
    let descriptor = descriptor_for_table_with_joins(spec.table, &spec.joins, schema)?;
    for condition in &spec.filters {
        validate_condition_for_descriptor(&descriptor, condition)?;
    }
    validate_order_by_for_descriptor(&spec.order_by, &descriptor)?;

    for nested in &spec.nested_arrays {
        validate_array_subquery_spec(nested, schema)?;
    }

    Ok(())
}

fn validate_execution_plan(
    plan: &ExecutionQueryPlan,
    schema: &Schema,
) -> Result<(), QueryCompileError> {
    let descriptor = descriptor_for_execution_plan(plan, schema)?;
    validate_disjuncts_for_descriptor(&plan.disjuncts, &descriptor)?;
    validate_order_by_for_descriptor(&plan.order_by, &descriptor)?;

    if let Some(recursive) = &plan.recursive {
        let recursive_descriptor =
            descriptor_for_table_with_joins(recursive.table, &recursive.joins, schema)?;
        for condition in &recursive.filters {
            validate_condition_for_descriptor(&recursive_descriptor, condition)?;
        }
    }

    for subquery in &plan.array_subqueries {
        validate_array_subquery_spec(subquery, schema)?;
    }

    Ok(())
}

fn descriptors_compatible_by_shape(left: &RowDescriptor, right: &RowDescriptor) -> bool {
    if left.columns.len() != right.columns.len() {
        return false;
    }

    left.columns
        .iter()
        .zip(right.columns.iter())
        .all(|(l, r)| l.column_type == r.column_type)
}

fn disjuncts_to_predicate(
    disjuncts: &[Conjunction],
    tuple_descriptor: &TupleDescriptor,
) -> Predicate {
    if disjuncts.is_empty() {
        return Predicate::True;
    }

    let non_empty: Vec<_> = disjuncts
        .iter()
        .filter(|d| !d.conditions.is_empty())
        .collect();
    if non_empty.is_empty() {
        return Predicate::True;
    }
    if non_empty.len() == 1 {
        return non_empty[0].to_tuple_predicate(tuple_descriptor);
    }

    Predicate::Or(
        non_empty
            .iter()
            .map(|d| d.to_tuple_predicate(tuple_descriptor))
            .collect(),
    )
}

fn sort_keys_from_order_by(
    order_by: &[(String, SortDirection)],
    descriptor: &RowDescriptor,
) -> Vec<SortKey> {
    if order_by.is_empty() {
        // Deterministic default ordering when no explicit orderBy is provided.
        return vec![SortKey {
            target: SortTarget::RowId,
            direction: SortDirection::Ascending,
        }];
    }

    order_by
        .iter()
        .filter_map(|(col, dir)| {
            if col == "_id" {
                Some(SortKey {
                    target: SortTarget::RowId,
                    direction: *dir,
                })
            } else {
                descriptor
                    .column_index(col)
                    .map(|idx| SortKey {
                        target: SortTarget::Column(idx),
                        direction: *dir,
                    })
                    .or_else(|| {
                        // Backward compatibility: "id" maps to internal row id when no explicit
                        // "id" column exists on the descriptor.
                        if col == "id" {
                            Some(SortKey {
                                target: SortTarget::RowId,
                                direction: *dir,
                            })
                        } else {
                            None
                        }
                    })
            }
        })
        .collect()
}

fn build_remaining_predicate_from_disjuncts(
    disjuncts: &[Conjunction],
    index_columns: &[String],
    tuple_descriptor: &TupleDescriptor,
) -> Predicate {
    // Check if all disjuncts are fully covered by their respective index scans
    let all_fully_covered = disjuncts
        .iter()
        .zip(index_columns.iter())
        .all(|(disjunct, index_col)| disjunct.is_fully_covered_by_index(index_col));

    if all_fully_covered {
        return Predicate::True;
    }

    // Build remaining predicates for each disjunct
    let remaining_predicates: Vec<Predicate> = disjuncts
        .iter()
        .zip(index_columns.iter())
        .map(|(disjunct, index_col)| {
            disjunct.remaining_tuple_predicate(index_col, tuple_descriptor)
        })
        .filter(|p| !matches!(p, Predicate::True))
        .collect();

    // If any disjunct needs filtering, we must use the full predicate for correctness
    // (because we can't tell which disjunct a row came from after union)
    if remaining_predicates.is_empty() {
        Predicate::True
    } else {
        // Fall back to full predicate for partial coverage cases
        disjuncts_to_predicate(disjuncts, tuple_descriptor)
    }
}

fn apply_condition_to_builder(mut builder: QueryBuilder, condition: &Condition) -> QueryBuilder {
    builder = match condition {
        Condition::Eq { column, value } => builder.filter_eq(column, value.clone()),
        Condition::Ne { column, value } => builder.filter_ne(column, value.clone()),
        Condition::Lt { column, value } => builder.filter_lt(column, value.clone()),
        Condition::Le { column, value } => builder.filter_le(column, value.clone()),
        Condition::Gt { column, value } => builder.filter_gt(column, value.clone()),
        Condition::Ge { column, value } => builder.filter_ge(column, value.clone()),
        Condition::Between { column, min, max } => {
            builder.filter_between(column, min.clone(), max.clone())
        }
        Condition::Contains { column, value } => builder.filter_contains(column, value.clone()),
        Condition::IsNull { column } => builder.filter_is_null(column),
        Condition::IsNotNull { column } => builder.filter_is_not_null(column),
    };
    builder
}

/// Convert a condition to a scan condition.
fn condition_to_scan(cond: &Condition) -> ScanCondition {
    match cond {
        Condition::Eq { value, .. } => ScanCondition::Eq(value.clone()),
        Condition::Lt { value, .. } => ScanCondition::Range {
            min: Bound::Unbounded,
            max: Bound::Excluded(value.clone()),
        },
        Condition::Le { value, .. } => ScanCondition::Range {
            min: Bound::Unbounded,
            max: Bound::Included(value.clone()),
        },
        Condition::Gt { value, .. } => ScanCondition::Range {
            min: Bound::Excluded(value.clone()),
            max: Bound::Unbounded,
        },
        Condition::Ge { value, .. } => ScanCondition::Range {
            min: Bound::Included(value.clone()),
            max: Bound::Unbounded,
        },
        Condition::Between { min, max, .. } => ScanCondition::Range {
            min: Bound::Included(min.clone()),
            max: Bound::Included(max.clone()),
        },
        _ => ScanCondition::All,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_manager::query::QueryBuilder;
    use crate::query_manager::relation_ir::{
        ColumnRef, JoinCondition, KeyRef, OrderByExpr, OrderDirection, PredicateCmpOp,
        PredicateExpr, ProjectColumn, ProjectExpr, RelExpr, RowIdRef, ValueRef,
    };
    use crate::query_manager::session::Session;
    use crate::query_manager::types::{
        ColumnDescriptor, ColumnType, RowDescriptor, TablePolicies, TableSchema, Value,
    };

    fn test_schema() -> Schema {
        let mut schema = Schema::new();
        schema.insert(
            TableName::new("users"),
            RowDescriptor::new(vec![
                ColumnDescriptor::new("id", ColumnType::Integer),
                ColumnDescriptor::new("name", ColumnType::Text),
                ColumnDescriptor::new("score", ColumnType::Integer),
            ])
            .into(),
        );
        schema
    }

    fn bytea_schema() -> Schema {
        let mut schema = Schema::new();
        schema.insert(
            TableName::new("files"),
            RowDescriptor::new(vec![
                ColumnDescriptor::new("id", ColumnType::Integer),
                ColumnDescriptor::new("payload", ColumnType::Bytea),
            ])
            .into(),
        );
        schema
    }

    #[test]
    fn compile_simple_query() {
        let schema = test_schema();
        let query = QueryBuilder::new("users")
            .filter_eq("score", Value::Integer(100))
            .build();

        let graph = QueryGraph::compile(&query, &schema).unwrap();

        // Should have: IndexScan -> Materialize -> Sort(default id ASC) -> Output
        assert_eq!(graph.nodes.len(), 4);
        assert_eq!(graph.index_scan_nodes.len(), 1);
    }

    #[test]
    fn compile_query_with_or() {
        let schema = test_schema();
        let query = QueryBuilder::new("users")
            .filter_eq("score", Value::Integer(50))
            .or()
            .filter_eq("score", Value::Integer(100))
            .build();

        let graph = QueryGraph::compile(&query, &schema).unwrap();

        // Should have: 2x IndexScan -> Union -> Materialize -> Sort(default id ASC) -> Output
        assert_eq!(graph.nodes.len(), 6);
        assert_eq!(graph.index_scan_nodes.len(), 2);
    }

    #[test]
    fn compile_query_with_sort_and_limit() {
        let schema = test_schema();
        let query = QueryBuilder::new("users")
            .order_by_desc("score")
            .limit(10)
            .offset(5)
            .build();

        let graph = QueryGraph::compile(&query, &schema).unwrap();

        assert_eq!(graph.nodes.len(), 2);
        assert!(has_indexed_query_node(&graph));
    }

    #[test]
    fn compile_query_with_multi_column_sort_uses_indexed_query() {
        let schema = test_schema();
        let query = QueryBuilder::new("users")
            .order_by("score")
            .order_by("name")
            .limit(10)
            .build();

        let graph = QueryGraph::compile(&query, &schema).unwrap();

        assert!(has_indexed_query_node(&graph));
        assert!(
            !graph
                .nodes
                .iter()
                .any(|c| matches!(c.node, GraphNode::Sort(_)))
        );
        assert!(
            !graph
                .nodes
                .iter()
                .any(|c| matches!(c.node, GraphNode::LimitOffset(_)))
        );
    }

    #[test]
    fn compile_query_with_same_column_filter_and_sort_uses_indexed_query() {
        let schema = test_schema();
        let query = QueryBuilder::new("users")
            .filter_ge("score", Value::Integer(50))
            .order_by("score")
            .limit(3)
            .build();

        let graph = QueryGraph::compile(&query, &schema).unwrap();

        assert!(has_indexed_query_node(&graph));
    }

    #[test]
    fn compile_query_with_different_filter_and_sort_columns_uses_indexed_query() {
        let schema = test_schema();
        let query = QueryBuilder::new("users")
            .filter_eq("name", Value::Text("Alice".into()))
            .order_by_desc("score")
            .limit(2)
            .build();

        let graph = QueryGraph::compile(&query, &schema).unwrap();

        assert!(has_indexed_query_node(&graph));
        assert!(
            !graph
                .nodes
                .iter()
                .any(|c| matches!(c.node, GraphNode::Sort(_)))
        );
        assert!(
            !graph
                .nodes
                .iter()
                .any(|c| matches!(c.node, GraphNode::LimitOffset(_)))
        );
    }

    #[test]
    fn compile_query_with_true_select_policy_and_session_uses_indexed_query() {
        let mut schema = Schema::new();
        schema.insert(
            TableName::new("todos"),
            TableSchema {
                columns: RowDescriptor::new(vec![
                    ColumnDescriptor::new("title", ColumnType::Text),
                    ColumnDescriptor::new("done", ColumnType::Boolean),
                ]),
                policies: TablePolicies::new().with_select(PolicyExpr::True),
            },
        );

        let query = QueryBuilder::new("todos").order_by("id").limit(50).build();
        let session = Session::new("user-1");
        let schema_context = QueryGraph::default_schema_context(&schema);

        let graph = QueryGraph::try_compile_with_schema_context(
            &query,
            &schema,
            Some(session),
            &schema_context,
        )
        .unwrap();

        assert!(has_indexed_query_node(&graph));
        assert!(
            !graph
                .nodes
                .iter()
                .any(|c| matches!(c.node, GraphNode::PolicyFilter(_)))
        );
    }

    #[test]
    fn compile_query_with_select_policy_and_session_uses_indexed_query() {
        let mut schema = Schema::new();
        schema.insert(
            TableName::new("documents"),
            TableSchema {
                columns: RowDescriptor::new(vec![
                    ColumnDescriptor::new("title", ColumnType::Text),
                    ColumnDescriptor::new("owner_id", ColumnType::Text),
                ]),
                policies: TablePolicies::new()
                    .with_select(PolicyExpr::eq_session("owner_id", vec!["user_id".into()])),
            },
        );

        let query = QueryBuilder::new("documents")
            .order_by("id")
            .limit(50)
            .build();
        let session = Session::new("alice");
        let schema_context = QueryGraph::default_schema_context(&schema);

        let graph = QueryGraph::try_compile_with_schema_context(
            &query,
            &schema,
            Some(session),
            &schema_context,
        )
        .unwrap();

        assert!(has_indexed_query_node(&graph));
        assert!(
            !graph
                .nodes
                .iter()
                .any(|c| matches!(c.node, GraphNode::PolicyFilter(_)))
        );
    }

    #[test]
    fn compile_query_no_filter() {
        let schema = test_schema();
        let query = QueryBuilder::new("users").build();

        let graph = QueryGraph::compile(&query, &schema).unwrap();

        // Should have: IndexScan -> Materialize -> Sort(default id ASC) -> Output
        assert_eq!(graph.nodes.len(), 4);
    }

    #[test]
    fn compile_query_allows_bytea_eq_and_ne() {
        let schema = bytea_schema();

        let eq_query = QueryBuilder::new("files")
            .filter_eq("payload", Value::Bytea(vec![1, 2, 3]))
            .build();
        assert!(QueryGraph::try_compile(&eq_query, &schema).is_ok());

        let ne_query = QueryBuilder::new("files")
            .filter_ne("payload", Value::Bytea(vec![4, 5, 6]))
            .build();
        assert!(QueryGraph::try_compile(&ne_query, &schema).is_ok());
    }

    #[test]
    fn compile_query_rejects_bytea_range_comparisons() {
        let schema = bytea_schema();
        let query = QueryBuilder::new("files")
            .filter_lt("payload", Value::Bytea(vec![1, 2, 3]))
            .build();

        let err = QueryGraph::try_compile(&query, &schema).unwrap_err();
        assert!(
            err.to_string()
                .contains("only supports '=' and '!=' comparisons")
        );
    }

    #[test]
    fn compile_query_rejects_order_by_on_bytea() {
        let schema = bytea_schema();
        let query = QueryBuilder::new("files").order_by("payload").build();

        let err = QueryGraph::try_compile(&query, &schema).unwrap_err();
        assert!(err.to_string().contains("cannot be used in ORDER BY"));
    }

    // ========================================================================
    // FilterNode elision tests
    // ========================================================================

    fn has_filter_node(graph: &QueryGraph) -> bool {
        graph
            .nodes
            .iter()
            .any(|c| matches!(c.node, GraphNode::Filter(_)))
    }

    #[test]
    fn single_eq_condition_elides_filter() {
        let schema = test_schema();
        let query = QueryBuilder::new("users")
            .filter_eq("score", Value::Integer(100))
            .build();

        let graph = QueryGraph::compile(&query, &schema).unwrap();

        // Eq is fully covered by index scan, no FilterNode needed
        // Should have: IndexScan -> Materialize -> Sort(default id ASC) -> Output
        assert!(
            !has_filter_node(&graph),
            "FilterNode should be elided for single Eq condition"
        );
        assert_eq!(graph.nodes.len(), 4);
    }

    #[test]
    fn single_lt_condition_elides_filter() {
        let schema = test_schema();
        let query = QueryBuilder::new("users")
            .filter_lt("score", Value::Integer(50))
            .build();

        let graph = QueryGraph::compile(&query, &schema).unwrap();

        // Lt is fully covered by index scan with Bound::Excluded
        assert!(
            !has_filter_node(&graph),
            "FilterNode should be elided for single Lt condition"
        );
        assert_eq!(graph.nodes.len(), 4);
    }

    #[test]
    fn single_between_condition_elides_filter() {
        let schema = test_schema();
        let query = QueryBuilder::new("users")
            .filter_between("score", Value::Integer(10), Value::Integer(50))
            .build();

        let graph = QueryGraph::compile(&query, &schema).unwrap();

        // Between is fully covered by index scan with inclusive bounds
        assert!(
            !has_filter_node(&graph),
            "FilterNode should be elided for single Between condition"
        );
        assert_eq!(graph.nodes.len(), 4);
    }

    #[test]
    fn multiple_conditions_different_columns_keeps_filter() {
        let schema = test_schema();
        let query = QueryBuilder::new("users")
            .filter_lt("score", Value::Integer(50))
            .filter_eq("name", Value::Text("Alice".into()))
            .build();

        let graph = QueryGraph::compile(&query, &schema).unwrap();

        // Index scan covers score < 50, but name = 'Alice' still needs filtering
        // Should have: IndexScan -> Materialize -> Filter -> Sort(default id ASC) -> Output
        assert!(
            has_filter_node(&graph),
            "FilterNode needed for non-indexed condition"
        );
        assert_eq!(graph.nodes.len(), 5);
    }

    #[test]
    fn non_indexable_condition_keeps_filter() {
        let schema = test_schema();
        let query = QueryBuilder::new("users")
            .filter_ne("score", Value::Integer(50))
            .build();

        let graph = QueryGraph::compile(&query, &schema).unwrap();

        // Ne is not index-scannable, uses full scan + filter
        // Should have: IndexScan -> Materialize -> Filter -> Sort(default id ASC) -> Output
        assert!(
            has_filter_node(&graph),
            "FilterNode needed for non-indexable condition"
        );
        assert_eq!(graph.nodes.len(), 5);
    }

    #[test]
    fn or_with_single_conditions_elides_filter() {
        let schema = test_schema();
        let query = QueryBuilder::new("users")
            .filter_eq("score", Value::Integer(50))
            .or()
            .filter_eq("score", Value::Integer(100))
            .build();

        let graph = QueryGraph::compile(&query, &schema).unwrap();

        // Each disjunct has one Eq condition fully covered by its index scan
        // Union combines them, no additional filtering needed
        // Should have: 2x IndexScan -> Union -> Materialize -> Sort(default id ASC) -> Output
        assert!(
            !has_filter_node(&graph),
            "FilterNode should be elided when all disjuncts are fully covered"
        );
        assert_eq!(graph.nodes.len(), 6);
    }

    // ========================================================================
    // Join compilation tests
    // ========================================================================

    fn join_schema() -> Schema {
        let mut schema = Schema::new();
        schema.insert(
            TableName::new("users"),
            RowDescriptor::new(vec![
                ColumnDescriptor::new("id", ColumnType::Integer),
                ColumnDescriptor::new("name", ColumnType::Text),
            ])
            .into(),
        );
        schema.insert(
            TableName::new("posts"),
            RowDescriptor::new(vec![
                ColumnDescriptor::new("id", ColumnType::Integer),
                ColumnDescriptor::new("title", ColumnType::Text),
                ColumnDescriptor::new("author_id", ColumnType::Integer),
            ])
            .into(),
        );
        schema
    }

    fn implicit_id_join_schema() -> Schema {
        let mut schema = Schema::new();
        schema.insert(
            TableName::new("users"),
            RowDescriptor::new(vec![ColumnDescriptor::new("name", ColumnType::Text)]).into(),
        );
        schema.insert(
            TableName::new("posts"),
            RowDescriptor::new(vec![
                ColumnDescriptor::new("title", ColumnType::Text),
                ColumnDescriptor::new("author_id", ColumnType::Uuid),
            ])
            .into(),
        );
        schema
    }

    fn has_join_node(graph: &QueryGraph) -> bool {
        graph
            .nodes
            .iter()
            .any(|c| matches!(c.node, GraphNode::Join(_)))
    }

    fn has_project_node(graph: &QueryGraph) -> bool {
        graph
            .nodes
            .iter()
            .any(|c| matches!(c.node, GraphNode::Project(_)))
    }

    fn has_indexed_query_node(graph: &QueryGraph) -> bool {
        graph
            .nodes
            .iter()
            .any(|c| matches!(c.node, GraphNode::IndexedQuery(_)))
    }

    #[test]
    fn compile_simple_join() {
        let schema = join_schema();
        let query = QueryBuilder::new("users")
            .join("posts")
            .on("id", "author_id")
            .build();

        let graph = QueryGraph::compile(&query, &schema).unwrap();

        assert!(has_join_node(&graph), "Should have a JoinNode");
        assert!(
            !has_indexed_query_node(&graph),
            "Non-paginated joins should keep the incremental JoinNode path"
        );
        assert_eq!(graph.index_scan_nodes.len(), 2);
    }

    #[test]
    fn compile_join_with_projection() {
        let schema = join_schema();
        let query = QueryBuilder::new("users")
            .join("posts")
            .on("id", "author_id")
            .select(&["name", "title"])
            .build();

        let graph = QueryGraph::compile(&query, &schema).unwrap();

        assert!(has_join_node(&graph), "Should have a JoinNode");
        assert!(
            !has_indexed_query_node(&graph),
            "Projection alone should not switch joins onto the indexed top-k path"
        );
        assert!(has_project_node(&graph), "Should have a ProjectNode");
    }

    #[test]
    fn compile_join_with_limit_uses_indexed_query() {
        let schema = join_schema();
        let query = QueryBuilder::new("users")
            .join("posts")
            .on("id", "author_id")
            .limit(5)
            .build();

        let graph = QueryGraph::compile(&query, &schema).unwrap();

        assert!(
            has_indexed_query_node(&graph),
            "Paginated joins should use IndexedQueryNode"
        );
        assert!(!has_join_node(&graph), "JoinNode should be elided");
    }

    #[test]
    fn compile_join_returns_none_for_missing_table() {
        let schema = join_schema();
        let query = QueryBuilder::new("users")
            .join("comments") // Table doesn't exist
            .on("id", "user_id")
            .build();

        let graph = QueryGraph::compile(&query, &schema);
        assert!(graph.is_none(), "Should return None for missing table");
    }

    #[test]
    fn compile_join_returns_none_for_invalid_column() {
        let schema = join_schema();
        let query = QueryBuilder::new("users")
            .join("posts")
            .on("nonexistent", "author_id") // Column doesn't exist
            .build();

        let graph = QueryGraph::compile(&query, &schema);
        assert!(graph.is_none(), "Should return None for invalid column");
    }

    #[test]
    fn compile_join_without_on_clause_fails_query_build() {
        let query = QueryBuilder::new("users").join("posts").try_build();
        assert!(
            query.is_err(),
            "Join queries without an explicit ON clause should fail at build time"
        );
    }

    #[test]
    fn compile_join_returns_none_for_circular_join_chain() {
        let schema = join_schema();
        let query = QueryBuilder::new("users")
            .join("posts")
            .on("id", "author_id")
            .join("users")
            .on("author_id", "id")
            .build();

        let graph = QueryGraph::compile(&query, &schema);
        assert!(
            graph.is_none(),
            "Circular/self join chains are not yet supported by the execution graph"
        );
    }

    // ========================================================================
    // Array subquery compilation tests
    // ========================================================================

    fn has_array_subquery_node(graph: &QueryGraph) -> bool {
        graph
            .nodes
            .iter()
            .any(|c| matches!(c.node, GraphNode::ArraySubquery(_)))
    }

    fn has_recursive_relation_node(graph: &QueryGraph) -> bool {
        graph
            .nodes
            .iter()
            .any(|c| matches!(c.node, GraphNode::RecursiveRelation(_)))
    }

    #[test]
    fn compile_query_with_array_subquery() {
        let schema = join_schema();
        let query = QueryBuilder::new("users")
            .with_array("posts", |sub| {
                sub.from("posts")
                    .correlate("author_id", "users.id")
                    .select(&["id", "title"])
            })
            .build();

        let graph = QueryGraph::compile(&query, &schema).unwrap();

        // Should have: IndexScan -> Materialize -> ArraySubquery -> Output
        assert!(
            has_array_subquery_node(&graph),
            "Should have an ArraySubqueryNode"
        );
    }

    #[test]
    fn compile_query_with_array_subquery_and_filter() {
        let schema = join_schema();
        let query = QueryBuilder::new("users")
            .filter_eq("name", Value::Text("Alice".into()))
            .with_array("posts", |sub| {
                sub.from("posts").correlate("author_id", "users.id")
            })
            .build();

        let graph = QueryGraph::compile(&query, &schema).unwrap();

        assert!(has_array_subquery_node(&graph));
        // Filter may be elided if covered by index scan
    }

    #[test]
    fn compile_query_with_multiple_array_subqueries() {
        let mut schema = join_schema();
        schema.insert(
            TableName::new("comments"),
            RowDescriptor::new(vec![
                ColumnDescriptor::new("id", ColumnType::Integer),
                ColumnDescriptor::new("text", ColumnType::Text),
                ColumnDescriptor::new("user_id", ColumnType::Integer),
            ])
            .into(),
        );

        let query = QueryBuilder::new("users")
            .with_array("posts", |sub| {
                sub.from("posts").correlate("author_id", "users.id")
            })
            .with_array("comments", |sub| {
                sub.from("comments").correlate("user_id", "users.id")
            })
            .build();

        let graph = QueryGraph::compile(&query, &schema).unwrap();

        // Count ArraySubquery nodes
        let array_subquery_count = graph
            .nodes
            .iter()
            .filter(|c| matches!(c.node, GraphNode::ArraySubquery(_)))
            .count();
        assert_eq!(
            array_subquery_count, 2,
            "Should have two ArraySubqueryNodes"
        );
    }

    #[test]
    fn compile_array_subquery_returns_none_for_missing_inner_table() {
        let schema = join_schema();
        let query = QueryBuilder::new("users")
            .with_array("comments", |sub| {
                sub.from("comments") // Table doesn't exist
                    .correlate("user_id", "users.id")
            })
            .build();

        let graph = QueryGraph::compile(&query, &schema);
        // Execution-plan validation rejects array subqueries that reference missing tables.
        assert!(graph.is_none());
    }

    fn recursive_schema() -> Schema {
        let mut schema = Schema::new();
        schema.insert(
            TableName::new("teams"),
            RowDescriptor::new(vec![ColumnDescriptor::new("team_id", ColumnType::Integer)]).into(),
        );
        schema.insert(
            TableName::new("team_edges"),
            RowDescriptor::new(vec![
                ColumnDescriptor::new("child_team", ColumnType::Integer),
                ColumnDescriptor::new("parent_team", ColumnType::Integer),
            ])
            .into(),
        );
        schema
    }

    fn recursive_hop_schema() -> Schema {
        let mut schema = Schema::new();
        schema.insert(
            TableName::new("teams"),
            RowDescriptor::new(vec![ColumnDescriptor::new("name", ColumnType::Text)]).into(),
        );
        schema.insert(
            TableName::new("team_edges"),
            RowDescriptor::new(vec![
                ColumnDescriptor::new("child_team", ColumnType::Uuid),
                ColumnDescriptor::new("parent_team", ColumnType::Uuid),
            ])
            .into(),
        );
        schema
    }

    #[test]
    fn compile_query_with_recursive_relation() {
        let schema = recursive_schema();
        let query = QueryBuilder::new("teams")
            .select(&["team_id"])
            .with_recursive(|r| {
                r.from("team_edges")
                    .correlate("child_team", "team_id")
                    .select(&["parent_team"])
                    .max_depth(10)
            })
            .build();

        let graph = QueryGraph::compile(&query, &schema).unwrap();
        assert!(
            has_recursive_relation_node(&graph),
            "Should have a RecursiveRelationNode"
        );
        assert_eq!(graph.recursive_relation_tables.len(), 1);
        assert_eq!(
            graph.recursive_relation_tables[0].1.as_str(),
            "team_edges",
            "Should track recursive step dependency table"
        );
    }

    #[test]
    fn compile_query_with_recursive_relation_mismatched_shape_is_skipped() {
        let schema = recursive_schema();
        let query = QueryBuilder::new("teams")
            .select(&["team_id"])
            .with_recursive(|r| {
                r.from("team_edges")
                    .correlate("child_team", "team_id")
                    // two columns don't match seed shape (one column)
                    .select(&["child_team", "parent_team"])
                    .max_depth(10)
            })
            .build();

        let graph = QueryGraph::compile(&query, &schema).unwrap();
        assert!(
            !has_recursive_relation_node(&graph),
            "Mismatched recursive projection shape should be skipped in MVP compiler"
        );
    }

    #[test]
    fn compile_query_with_recursive_hop_relation() {
        let schema = recursive_hop_schema();
        let query = QueryBuilder::new("teams")
            .filter_eq("name", Value::Text("seed".into()))
            .with_recursive(|r| {
                r.from("team_edges")
                    .correlate("child_team", "_id")
                    .select(&["parent_team"])
                    .hop("teams", "parent_team")
                    .max_depth(10)
            })
            .build();

        let graph = QueryGraph::compile(&query, &schema).expect("Graph should compile");
        assert!(
            has_recursive_relation_node(&graph),
            "Recursive hop queries should compile to RecursiveRelationNode"
        );
    }

    #[test]
    fn compile_query_with_recursive_join_projection_relation_is_rejected() {
        let query_result = QueryBuilder::new("teams")
            .filter_eq("name", Value::Text("seed".into()))
            .with_recursive(|r| {
                r.from("team_edges")
                    .correlate("child_team", "_id")
                    .join("teams")
                    .alias("__recursive_hop_0")
                    .on("team_edges.parent_team", "__recursive_hop_0.id")
                    .result_element_index(1)
                    .max_depth(10)
            })
            .try_build();
        assert!(
            query_result.is_err(),
            "recursive join-projection query shape should be rejected"
        );
    }

    #[test]
    fn compile_query_with_relation_ir_uses_unified_entrypoint() {
        let schema = recursive_hop_schema();
        let mut query = QueryBuilder::new("placeholder").branch("main").build();
        query.relation_ir = RelExpr::TableScan {
            table: TableName::new("teams"),
        };

        assert!(
            QueryGraph::compile(&query, &schema).is_some(),
            "relation IR queries should compile through the same compile() entrypoint",
        );
    }

    #[test]
    fn compile_relation_ir_with_include_deleted_adds_deleted_scan() {
        let schema = test_schema();
        let relation = RelExpr::TableScan {
            table: TableName::new("users"),
        };
        let branches = vec!["main".to_string()];
        let graph = QueryGraph::compile_relation_ir_with_features(
            &relation,
            &schema,
            &branches,
            None,
            RelationCompileFeatures {
                include_deleted: true,
                array_subqueries: Vec::new(),
                select_columns: None,
            },
        )
        .expect("Graph should compile");

        assert!(
            graph
                .index_scan_nodes
                .iter()
                .any(|(_, _, column)| { column.as_str() == "_id_deleted" }),
            "include_deleted should add an _id_deleted scan in relation-ir compile path",
        );
    }

    #[test]
    fn compile_relation_ir_with_array_subqueries_adds_array_nodes() {
        let schema = join_schema();
        let relation = RelExpr::TableScan {
            table: TableName::new("users"),
        };
        let branches = vec!["main".to_string()];

        let query_with_arrays = QueryBuilder::new("users")
            .with_array("posts", |sub| {
                sub.from("posts")
                    .correlate("author_id", "users.id")
                    .select(&["id", "title"])
            })
            .build();

        let graph = QueryGraph::compile_relation_ir_with_features(
            &relation,
            &schema,
            &branches,
            None,
            RelationCompileFeatures {
                include_deleted: false,
                array_subqueries: query_with_arrays.array_subqueries,
                select_columns: None,
            },
        )
        .expect("Graph should compile");

        assert!(
            has_array_subquery_node(&graph),
            "relation-ir compile path should preserve array subqueries",
        );
        assert_eq!(graph.array_subquery_tables.len(), 1);
        assert_eq!(graph.array_subquery_tables[0].1.as_str(), "posts");
    }

    #[test]
    fn compile_relation_ir_with_select_columns_adds_project_node() {
        let schema = test_schema();
        let relation = RelExpr::TableScan {
            table: TableName::new("users"),
        };
        let branches = vec!["main".to_string()];
        let graph = QueryGraph::compile_relation_ir_with_features(
            &relation,
            &schema,
            &branches,
            None,
            RelationCompileFeatures {
                include_deleted: false,
                array_subqueries: Vec::new(),
                select_columns: Some(vec!["name".to_string()]),
            },
        )
        .expect("Graph should compile");

        assert!(
            graph
                .nodes
                .iter()
                .any(|ctx| matches!(ctx.node, GraphNode::Project(_))),
            "select_columns should insert ProjectNode in relation-ir compile path",
        );
    }

    #[test]
    fn compile_relation_ir_with_join_projection_preserves_aliases() {
        let schema = join_schema();
        let relation = RelExpr::Project {
            input: Box::new(RelExpr::Join {
                left: Box::new(RelExpr::TableScan {
                    table: TableName::new("users"),
                }),
                right: Box::new(RelExpr::TableScan {
                    table: TableName::new("posts"),
                }),
                on: vec![JoinCondition {
                    left: ColumnRef::scoped("users", "id"),
                    right: ColumnRef::scoped("posts", "author_id"),
                }],
                join_kind: crate::query_manager::relation_ir::JoinKind::Inner,
            }),
            columns: vec![
                ProjectColumn {
                    alias: "author_name".to_string(),
                    expr: ProjectExpr::Column(ColumnRef::scoped("users", "name")),
                },
                ProjectColumn {
                    alias: "post_title".to_string(),
                    expr: ProjectExpr::Column(ColumnRef::scoped("posts", "title")),
                },
            ],
        };

        let branches = vec!["main".to_string()];
        let graph = QueryGraph::compile_relation_ir(&relation, &schema, &branches, None)
            .expect("Graph should compile");

        assert!(
            graph
                .nodes
                .iter()
                .any(|ctx| matches!(ctx.node, GraphNode::Project(_))),
            "precise relation-ir projection should still compile to ProjectNode",
        );
        assert_eq!(graph.combined_descriptor.columns.len(), 2);
        assert_eq!(graph.combined_descriptor.columns[0].name, "author_name");
        assert_eq!(graph.combined_descriptor.columns[1].name, "post_title");
    }

    #[test]
    fn compile_relation_ir_with_or_filter_produces_union_plan() {
        let schema = test_schema();
        let relation = RelExpr::Filter {
            input: Box::new(RelExpr::TableScan {
                table: TableName::new("users"),
            }),
            predicate: PredicateExpr::Or(vec![
                PredicateExpr::Cmp {
                    left: ColumnRef::unscoped("name"),
                    op: PredicateCmpOp::Eq,
                    right: ValueRef::Literal(Value::Text("Alice".to_string())),
                },
                PredicateExpr::Cmp {
                    left: ColumnRef::unscoped("name"),
                    op: PredicateCmpOp::Eq,
                    right: ValueRef::Literal(Value::Text("Bob".to_string())),
                },
            ]),
        };
        let branches = vec!["main".to_string()];
        let graph = QueryGraph::compile_relation_ir(&relation, &schema, &branches, None)
            .expect("OR filter relation should compile");

        assert_eq!(graph.index_scan_nodes.len(), 2);
        assert!(
            graph
                .nodes
                .iter()
                .any(|ctx| matches!(ctx.node, GraphNode::Union(_))),
            "OR relation filters should lower to multi-disjunct union plans",
        );
    }

    #[test]
    fn compile_relation_ir_with_contains_filter_builds_filter_plan() {
        let mut schema = Schema::new();
        schema.insert(
            TableName::new("users"),
            RowDescriptor::new(vec![
                ColumnDescriptor::new("name", ColumnType::Text),
                ColumnDescriptor::new(
                    "tags",
                    ColumnType::Array {
                        element: Box::new(ColumnType::Text),
                    },
                ),
            ])
            .into(),
        );
        let relation = RelExpr::Filter {
            input: Box::new(RelExpr::TableScan {
                table: TableName::new("users"),
            }),
            predicate: PredicateExpr::Contains {
                left: ColumnRef::unscoped("tags"),
                right: ValueRef::Literal(Value::Text("admin".to_string())),
            },
        };
        let branches = vec!["main".to_string()];
        let graph = QueryGraph::compile_relation_ir(&relation, &schema, &branches, None)
            .expect("contains filter relation should compile");

        assert!(
            graph
                .nodes
                .iter()
                .any(|ctx| matches!(ctx.node, GraphNode::Filter(_))),
            "contains relation filters should lower to FilterNode",
        );
    }

    #[test]
    fn compile_query_with_relation_ir_project_join_order_limit_shape() {
        let schema = recursive_hop_schema();
        let relation = RelExpr::Limit {
            input: Box::new(RelExpr::OrderBy {
                input: Box::new(RelExpr::Project {
                    input: Box::new(RelExpr::Join {
                        left: Box::new(RelExpr::Filter {
                            input: Box::new(RelExpr::TableScan {
                                table: TableName::new("team_edges"),
                            }),
                            predicate: PredicateExpr::Cmp {
                                left: ColumnRef::scoped("team_edges", "child_team"),
                                op: PredicateCmpOp::Eq,
                                right: ValueRef::Literal(Value::Integer(7)),
                            },
                        }),
                        right: Box::new(RelExpr::TableScan {
                            table: TableName::new("teams"),
                        }),
                        on: vec![JoinCondition {
                            left: ColumnRef::scoped("team_edges", "parent_team"),
                            right: ColumnRef::scoped("__hop_0", "id"),
                        }],
                        join_kind: crate::query_manager::relation_ir::JoinKind::Inner,
                    }),
                    columns: vec![ProjectColumn {
                        alias: "id".to_string(),
                        expr: ProjectExpr::Column(ColumnRef::scoped("__hop_0", "id")),
                    }],
                }),
                terms: vec![OrderByExpr {
                    column: ColumnRef::unscoped("name"),
                    direction: OrderDirection::Desc,
                }],
            }),
            limit: 5,
        };

        let branches = vec!["main".to_string()];
        let graph = QueryGraph::compile_relation_ir(&relation, &schema, &branches, None)
            .expect("Graph should compile");
        assert!(
            has_indexed_query_node(&graph),
            "relation IR join + limit shape should compile to IndexedQueryNode",
        );
        assert!(
            !graph
                .nodes
                .iter()
                .any(|ctx| matches!(ctx.node, GraphNode::Sort(_))),
            "relation IR top-k path should not need SortNode",
        );
        assert!(
            !graph
                .nodes
                .iter()
                .any(|ctx| matches!(ctx.node, GraphNode::LimitOffset(_))),
            "relation IR top-k path should not need LimitOffsetNode",
        );
    }

    #[test]
    fn compile_query_with_relation_ir_project_join_base_element_shape() {
        let schema = join_schema();
        let relation = RelExpr::Project {
            input: Box::new(RelExpr::Join {
                left: Box::new(RelExpr::TableScan {
                    table: TableName::new("users"),
                }),
                right: Box::new(RelExpr::TableScan {
                    table: TableName::new("posts"),
                }),
                on: vec![JoinCondition {
                    left: ColumnRef::scoped("users", "id"),
                    right: ColumnRef::scoped("posts", "author_id"),
                }],
                join_kind: crate::query_manager::relation_ir::JoinKind::Inner,
            }),
            columns: vec![ProjectColumn {
                alias: "id".to_string(),
                expr: ProjectExpr::Column(ColumnRef::scoped("users", "id")),
            }],
        };

        let branches = vec!["main".to_string()];
        let graph = QueryGraph::compile_relation_ir(&relation, &schema, &branches, None)
            .expect("Graph should compile");
        assert!(
            graph
                .nodes
                .iter()
                .any(|ctx| matches!(ctx.node, GraphNode::SelectElement(_))),
            "project-to-base relation IR should compile to SelectElementNode",
        );
        assert!(
            graph.combined_descriptor.column_index("name").is_some(),
            "base element projection should keep base descriptor columns",
        );
        assert!(
            graph
                .combined_descriptor
                .column_index("author_id")
                .is_none(),
            "base element projection should not expose joined table columns",
        );
    }

    #[test]
    fn compile_query_with_relation_ir_project_join_full_implicit_id_element_shape() {
        let schema = implicit_id_join_schema();
        let relation = RelExpr::Project {
            input: Box::new(RelExpr::Join {
                left: Box::new(RelExpr::TableScan {
                    table: TableName::new("users"),
                }),
                right: Box::new(RelExpr::TableScan {
                    table: TableName::new("posts"),
                }),
                on: vec![JoinCondition {
                    left: ColumnRef::scoped("users", "id"),
                    right: ColumnRef::scoped("__hop_0", "author_id"),
                }],
                join_kind: crate::query_manager::relation_ir::JoinKind::Inner,
            }),
            columns: vec![
                ProjectColumn {
                    alias: "id".to_string(),
                    expr: ProjectExpr::Column(ColumnRef::scoped("__hop_0", "id")),
                },
                ProjectColumn {
                    alias: "title".to_string(),
                    expr: ProjectExpr::Column(ColumnRef::scoped("__hop_0", "title")),
                },
                ProjectColumn {
                    alias: "author_id".to_string(),
                    expr: ProjectExpr::Column(ColumnRef::scoped("__hop_0", "author_id")),
                },
            ],
        };

        let branches = vec!["main".to_string()];
        let graph = QueryGraph::compile_relation_ir(&relation, &schema, &branches, None)
            .expect("Graph should compile");

        assert!(
            graph
                .nodes
                .iter()
                .any(|ctx| matches!(ctx.node, GraphNode::SelectElement(_))),
            "full implicit-id element projection should compile to SelectElementNode",
        );
        assert!(
            !graph
                .nodes
                .iter()
                .any(|ctx| matches!(ctx.node, GraphNode::Project(_))),
            "full implicit-id element projection should not add a ProjectNode",
        );
        assert_eq!(graph.combined_descriptor.columns.len(), 2);
        assert!(graph.combined_descriptor.column_index("title").is_some());
        assert!(
            graph
                .combined_descriptor
                .column_index("author_id")
                .is_some()
        );
        assert!(
            graph.combined_descriptor.column_index("id").is_none(),
            "implicit row id should remain out-of-band",
        );
    }

    #[test]
    fn compile_query_with_relation_ir_gather_uses_recursive_node() {
        let schema = recursive_hop_schema();
        let relation = RelExpr::Gather {
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
                    join_kind: crate::query_manager::relation_ir::JoinKind::Inner,
                }),
                columns: vec![ProjectColumn {
                    alias: "id".to_string(),
                    expr: ProjectExpr::Column(ColumnRef::scoped("__recursive_hop_0", "id")),
                }],
            }),
            frontier_key: KeyRef::RowId(RowIdRef::Current),
            max_depth: 8,
            dedupe_key: vec![KeyRef::RowId(RowIdRef::Current)],
        };

        let branches = vec!["main".to_string()];
        let graph = QueryGraph::compile_relation_ir(&relation, &schema, &branches, None)
            .expect("Graph should compile");
        assert!(
            has_recursive_relation_node(&graph),
            "Gather relation IR should compile to RecursiveRelationNode"
        );
        assert_eq!(graph.recursive_relation_tables.len(), 1);
        assert_eq!(graph.recursive_relation_tables[0].1.as_str(), "team_edges");
    }

    #[test]
    fn compile_query_with_relation_ir_gather_hop_step_projection_uses_recursive_node() {
        let schema = recursive_hop_schema();
        let relation = RelExpr::Gather {
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
                    left: Box::new(RelExpr::Project {
                        input: Box::new(RelExpr::Filter {
                            input: Box::new(RelExpr::TableScan {
                                table: TableName::new("team_edges"),
                            }),
                            predicate: PredicateExpr::Cmp {
                                left: ColumnRef::scoped("team_edges", "child_team"),
                                op: PredicateCmpOp::Eq,
                                right: ValueRef::RowId(RowIdRef::Frontier),
                            },
                        }),
                        columns: vec![ProjectColumn {
                            alias: "parent_team".to_string(),
                            expr: ProjectExpr::Column(ColumnRef::scoped(
                                "team_edges",
                                "parent_team",
                            )),
                        }],
                    }),
                    right: Box::new(RelExpr::TableScan {
                        table: TableName::new("teams"),
                    }),
                    on: vec![JoinCondition {
                        left: ColumnRef::scoped("team_edges", "parent_team"),
                        right: ColumnRef::scoped("__recursive_hop_0", "id"),
                    }],
                    join_kind: crate::query_manager::relation_ir::JoinKind::Inner,
                }),
                columns: vec![ProjectColumn {
                    alias: "id".to_string(),
                    expr: ProjectExpr::Column(ColumnRef::scoped("__recursive_hop_0", "id")),
                }],
            }),
            frontier_key: KeyRef::RowId(RowIdRef::Current),
            max_depth: 8,
            dedupe_key: vec![KeyRef::RowId(RowIdRef::Current)],
        };

        let branches = vec!["main".to_string()];
        let graph = QueryGraph::compile_relation_ir(&relation, &schema, &branches, None)
            .expect("Graph should compile");
        assert!(
            has_recursive_relation_node(&graph),
            "Gather with projected hop step should compile to RecursiveRelationNode",
        );
        assert_eq!(graph.recursive_relation_tables.len(), 1);
        assert_eq!(graph.recursive_relation_tables[0].1.as_str(), "team_edges");
    }

    #[test]
    fn compile_query_with_relation_ir_gather_direct_step_uses_recursive_node() {
        let schema = recursive_schema();
        let relation = RelExpr::Gather {
            seed: Box::new(RelExpr::Filter {
                input: Box::new(RelExpr::TableScan {
                    table: TableName::new("teams"),
                }),
                predicate: PredicateExpr::Cmp {
                    left: ColumnRef::scoped("teams", "team_id"),
                    op: PredicateCmpOp::Eq,
                    right: ValueRef::Literal(Value::Integer(1)),
                },
            }),
            step: Box::new(RelExpr::Project {
                input: Box::new(RelExpr::Filter {
                    input: Box::new(RelExpr::TableScan {
                        table: TableName::new("team_edges"),
                    }),
                    predicate: PredicateExpr::Cmp {
                        left: ColumnRef::scoped("team_edges", "child_team"),
                        op: PredicateCmpOp::Eq,
                        right: ValueRef::RowId(RowIdRef::Frontier),
                    },
                }),
                columns: vec![ProjectColumn {
                    alias: "parent_team".to_string(),
                    expr: ProjectExpr::Column(ColumnRef::scoped("team_edges", "parent_team")),
                }],
            }),
            frontier_key: KeyRef::RowId(RowIdRef::Current),
            max_depth: 4,
            dedupe_key: vec![KeyRef::RowId(RowIdRef::Current)],
        };

        let branches = vec!["main".to_string()];
        let graph = QueryGraph::compile_relation_ir(&relation, &schema, &branches, None)
            .expect("Graph should compile");
        assert!(
            has_recursive_relation_node(&graph),
            "direct-step Gather relation IR should compile to RecursiveRelationNode"
        );
        assert_eq!(graph.recursive_relation_tables.len(), 1);
        assert_eq!(graph.recursive_relation_tables[0].1.as_str(), "team_edges");
    }

    #[test]
    fn compile_query_with_relation_ir_gather_post_join_uses_recursive_and_join() {
        let schema = recursive_hop_schema();
        let relation = RelExpr::Project {
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
                            join_kind: crate::query_manager::relation_ir::JoinKind::Inner,
                        }),
                        columns: vec![ProjectColumn {
                            alias: "id".to_string(),
                            expr: ProjectExpr::Column(ColumnRef::scoped("__recursive_hop_0", "id")),
                        }],
                    }),
                    frontier_key: KeyRef::RowId(RowIdRef::Current),
                    max_depth: 8,
                    dedupe_key: vec![KeyRef::RowId(RowIdRef::Current)],
                }),
                right: Box::new(RelExpr::TableScan {
                    table: TableName::new("team_edges"),
                }),
                on: vec![JoinCondition {
                    left: ColumnRef::scoped("teams", "id"),
                    right: ColumnRef::scoped("__hop_0", "parent_team"),
                }],
                join_kind: crate::query_manager::relation_ir::JoinKind::Inner,
            }),
            columns: vec![ProjectColumn {
                alias: "id".to_string(),
                expr: ProjectExpr::Column(ColumnRef::scoped("__hop_0", "id")),
            }],
        };

        let branches = vec!["main".to_string()];
        let graph = QueryGraph::compile_relation_ir(&relation, &schema, &branches, None)
            .expect("Graph should compile");
        assert!(
            has_recursive_relation_node(&graph),
            "Gather relation IR with post-join should compile to RecursiveRelationNode"
        );
        assert_eq!(graph.recursive_relation_tables.len(), 1);
        assert_eq!(graph.recursive_relation_tables[0].1.as_str(), "team_edges");
        assert!(graph.nodes.iter().any(|ctx| match &ctx.node {
            GraphNode::Join(_) => true,
            _ => false,
        }));
    }

    #[test]
    fn compile_query_with_unsupported_relation_ir_is_rejected() {
        let schema = test_schema();
        let relation = RelExpr::Filter {
            input: Box::new(RelExpr::TableScan {
                table: TableName::new("users"),
            }),
            predicate: PredicateExpr::Not(Box::new(PredicateExpr::Cmp {
                left: ColumnRef::unscoped("name"),
                op: PredicateCmpOp::Eq,
                right: ValueRef::Literal(Value::Text("Alice".to_string())),
            })),
        };
        let branches = vec!["main".to_string()];
        let graph = QueryGraph::compile_relation_ir(&relation, &schema, &branches, None);
        assert!(
            graph.is_none(),
            "unsupported relation_ir should not silently fallback"
        );
    }
}
