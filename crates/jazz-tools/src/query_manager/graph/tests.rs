//! Tests for QueryGraph compile and execute.

use super::*;
use crate::query_manager::index::ScanCondition;
use crate::query_manager::query::QueryBuilder;
use crate::query_manager::relation_ir::{
    ColumnRef, JoinCondition, KeyRef, OrderByExpr, OrderDirection, PredicateCmpOp, PredicateExpr,
    ProjectColumn, ProjectExpr, RelExpr, RowIdRef, ValueRef,
};
use crate::query_manager::types::{
    ColumnDescriptor, ColumnType, RowDescriptor, RowPolicyMode, Schema, Value,
};
use std::ops::Bound;

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

    // Should have: IndexScan -> Materialize -> Sort -> LimitOffset -> Output
    // (no Filter because no WHERE clause)
    assert_eq!(graph.nodes.len(), 5);
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
fn estimate_memory_size_counts_magic_column_dependencies() {
    let schema = test_schema();
    let query = QueryBuilder::new("users").build();
    let mut graph = QueryGraph::compile(&query, &schema).unwrap();
    let base_size = graph.estimate_memory_size();

    let dependency_table = TableName::new("permission_edges");
    let expected_extra = std::mem::size_of::<NodeId>() + dependency_table.as_str().len();
    graph
        .magic_column_tables
        .push((NodeId(999), dependency_table));

    assert_eq!(graph.estimate_memory_size(), base_size + expected_extra);
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

fn only_index_scan_condition(graph: &QueryGraph) -> &ScanCondition {
    let scan_conditions: Vec<_> = graph
        .nodes
        .iter()
        .filter_map(|c| match &c.node {
            GraphNode::IndexScan(scan) => Some(&scan.condition),
            _ => None,
        })
        .collect();
    assert_eq!(scan_conditions.len(), 1);
    scan_conditions[0]
}

fn only_index_scan(graph: &QueryGraph) -> &IndexScanNode {
    let scans: Vec<_> = graph
        .nodes
        .iter()
        .filter_map(|c| match &c.node {
            GraphNode::IndexScan(scan) => Some(scan),
            _ => None,
        })
        .collect();
    assert_eq!(scans.len(), 1);
    scans[0]
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
fn same_column_redundant_lower_bounds_elide_filter_with_strictest_scan() {
    let schema = test_schema();
    let query = QueryBuilder::new("users")
        .filter_gt("score", Value::Integer(10))
        .filter_ge("score", Value::Integer(5))
        .build();

    let graph = QueryGraph::compile(&query, &schema).unwrap();

    assert!(
        !has_filter_node(&graph),
        "redundant same-column lower bounds should be fully covered by the merged scan"
    );
    assert_eq!(graph.nodes.len(), 4);
    assert!(matches!(
        only_index_scan_condition(&graph),
        ScanCondition::Range {
            min: Bound::Excluded(Value::Integer(10)),
            max: Bound::Unbounded,
        }
    ));
}

#[test]
fn same_column_redundant_upper_bounds_elide_filter_with_strictest_scan() {
    let schema = test_schema();
    let query = QueryBuilder::new("users")
        .filter_lt("score", Value::Integer(20))
        .filter_le("score", Value::Integer(15))
        .build();

    let graph = QueryGraph::compile(&query, &schema).unwrap();

    assert!(
        !has_filter_node(&graph),
        "redundant same-column upper bounds should be fully covered by the merged scan"
    );
    assert_eq!(graph.nodes.len(), 4);
    assert!(matches!(
        only_index_scan_condition(&graph),
        ScanCondition::Range {
            min: Bound::Unbounded,
            max: Bound::Included(Value::Integer(15)),
        }
    ));
}

#[test]
fn same_column_eq_inside_range_elides_filter_with_eq_scan() {
    let schema = test_schema();
    let query = QueryBuilder::new("users")
        .filter_eq("score", Value::Integer(10))
        .filter_ge("score", Value::Integer(5))
        .filter_lt("score", Value::Integer(20))
        .build();

    let graph = QueryGraph::compile(&query, &schema).unwrap();

    assert!(
        !has_filter_node(&graph),
        "an equality within same-column bounds should be fully covered by the eq scan"
    );
    assert_eq!(graph.nodes.len(), 4);
    assert!(matches!(
        only_index_scan_condition(&graph),
        ScanCondition::Eq(Value::Integer(10))
    ));
}

#[test]
fn same_column_contradiction_elides_filter_with_empty_scan() {
    let schema = test_schema();
    let query = QueryBuilder::new("users")
        .filter_eq("score", Value::Integer(10))
        .filter_lt("score", Value::Integer(10))
        .build();

    let graph = QueryGraph::compile(&query, &schema).unwrap();

    assert!(
        !has_filter_node(&graph),
        "a proven same-column contradiction should scan no rows and need no residual filter"
    );
    assert_eq!(graph.nodes.len(), 4);
    assert!(matches!(
        only_index_scan_condition(&graph),
        ScanCondition::Empty
    ));
}

#[test]
fn multi_column_conjunction_prefers_eq_scan_and_keeps_filter() {
    let schema = test_schema();
    let query = QueryBuilder::new("users")
        .filter_ge("score", Value::Integer(50))
        .filter_eq("name", Value::Text("Alice".into()))
        .build();

    let graph = QueryGraph::compile(&query, &schema).unwrap();
    let scan = only_index_scan(&graph);

    assert_eq!(scan.column.as_str(), "name");
    assert!(matches!(
        &scan.condition,
        ScanCondition::Eq(Value::Text(name)) if name == "Alice"
    ));
    assert!(
        has_filter_node(&graph),
        "score >= 50 must remain as a residual predicate after the name index scan"
    );
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

#[test]
fn or_with_partial_disjunct_keeps_filter() {
    let schema = test_schema();
    let query = QueryBuilder::new("users")
        .filter_eq("score", Value::Integer(50))
        .or()
        .filter_eq("score", Value::Integer(100))
        .filter_eq("name", Value::Text("Alice".into()))
        .build();

    let graph = QueryGraph::compile(&query, &schema).unwrap();

    assert_eq!(graph.index_scan_nodes.len(), 2);
    assert!(
        has_filter_node(&graph),
        "mixed fully-covered and partial disjuncts need the full residual OR predicate"
    );
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

#[test]
fn compile_simple_join() {
    let schema = join_schema();
    let query = QueryBuilder::new("users")
        .join("posts")
        .on("id", "author_id")
        .build();

    let graph = QueryGraph::compile(&query, &schema).unwrap();

    // Should have: 2x IndexScan -> 2x Materialize -> JoinNode -> Output
    // 2 IndexScans + 2 Materializes + 1 Join + 1 Output = 6 nodes
    assert!(has_join_node(&graph), "Should have a JoinNode");
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
    assert!(has_project_node(&graph), "Should have a ProjectNode");
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
        RowPolicyMode::PermissiveLocal,
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
        RowPolicyMode::PermissiveLocal,
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
        RowPolicyMode::PermissiveLocal,
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
        graph
            .nodes
            .iter()
            .any(|ctx| matches!(ctx.node, GraphNode::Join(_))),
        "relation IR join shape should compile to JoinNode",
    );
    assert!(
        graph
            .nodes
            .iter()
            .any(|ctx| matches!(ctx.node, GraphNode::Sort(_))),
        "relation IR order by should compile to SortNode",
    );
    assert!(
        graph
            .nodes
            .iter()
            .any(|ctx| matches!(ctx.node, GraphNode::LimitOffset(_))),
        "relation IR limit should compile to LimitOffsetNode",
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
fn compile_query_with_relation_ir_gather_join_seed_uses_recursive_node() {
    let schema = recursive_hop_schema();
    let relation = RelExpr::Gather {
        seed: Box::new(RelExpr::Project {
            input: Box::new(RelExpr::Join {
                left: Box::new(RelExpr::Filter {
                    input: Box::new(RelExpr::TableScan {
                        table: TableName::new("team_edges"),
                    }),
                    predicate: PredicateExpr::Cmp {
                        left: ColumnRef::scoped("team_edges", "child_team"),
                        op: PredicateCmpOp::Eq,
                        right: ValueRef::Literal(Value::Integer(1)),
                    },
                }),
                right: Box::new(RelExpr::TableScan {
                    table: TableName::new("teams"),
                }),
                on: vec![JoinCondition {
                    left: ColumnRef::scoped("team_edges", "parent_team"),
                    right: ColumnRef::scoped("__seed_hop_0", "id"),
                }],
                join_kind: crate::query_manager::relation_ir::JoinKind::Inner,
            }),
            columns: vec![ProjectColumn {
                alias: "id".to_string(),
                expr: ProjectExpr::Column(ColumnRef::scoped("__seed_hop_0", "id")),
            }],
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
        "Gather relation IR with join seed should compile to RecursiveRelationNode"
    );
    assert_eq!(graph.recursive_relation_tables.len(), 1);
    assert_eq!(graph.recursive_relation_tables[0].1.as_str(), "team_edges");
}

#[test]
fn compile_query_with_relation_ir_gather_union_seed_uses_recursive_node() {
    let schema = recursive_hop_schema();
    let relation = RelExpr::Gather {
        seed: Box::new(RelExpr::Union {
            inputs: vec![
                RelExpr::Project {
                    input: Box::new(RelExpr::Join {
                        left: Box::new(RelExpr::Filter {
                            input: Box::new(RelExpr::TableScan {
                                table: TableName::new("team_edges"),
                            }),
                            predicate: PredicateExpr::Cmp {
                                left: ColumnRef::scoped("team_edges", "child_team"),
                                op: PredicateCmpOp::Eq,
                                right: ValueRef::Literal(Value::Integer(1)),
                            },
                        }),
                        right: Box::new(RelExpr::TableScan {
                            table: TableName::new("teams"),
                        }),
                        on: vec![JoinCondition {
                            left: ColumnRef::scoped("team_edges", "parent_team"),
                            right: ColumnRef::scoped("__seed_hop_0", "id"),
                        }],
                        join_kind: crate::query_manager::relation_ir::JoinKind::Inner,
                    }),
                    columns: vec![ProjectColumn {
                        alias: "id".to_string(),
                        expr: ProjectExpr::Column(ColumnRef::scoped("__seed_hop_0", "id")),
                    }],
                },
                RelExpr::Gather {
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
                },
            ],
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
        "Gather relation IR with union seed should compile to RecursiveRelationNode"
    );
    assert_eq!(graph.recursive_relation_tables.len(), 2);
    assert!(
        graph
            .nodes
            .iter()
            .any(|ctx| matches!(&ctx.node, GraphNode::Union(_)))
    );
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
                        expr: ProjectExpr::Column(ColumnRef::scoped("team_edges", "parent_team")),
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
    assert!(
        graph
            .nodes
            .iter()
            .any(|ctx| matches!(&ctx.node, GraphNode::Join(_)))
    );
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
