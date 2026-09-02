//! normalization query-evaluation tests.

use super::*;
use crate::node::query_eval::normalization::source_column_value;

#[test]
fn payload_enum_normalization_uses_case_local_field_types() {
    let descriptor = RecordDescriptor::new([
        ("shared", ValueType::Uuid),
        ("case_only", ValueType::String),
    ]);
    let source = root_source_id("events");
    let uuid = uuid::Uuid::from_u128(7);
    let predicate = Predicate::Eq(
        Operand::Column("shared".to_owned()),
        Operand::Literal(Value::String(uuid.to_string())),
    );

    let normalized = normalize_enum_payload_predicate(&descriptor, &source, &predicate)
        .expect("case-local field normalizes");
    let NormalizedPredicateExpr::Compare { right, .. } = normalized else {
        panic!("expected comparison");
    };
    let NormalizedValueRef::Literal(bytes) = right else {
        panic!("expected literal");
    };
    assert_eq!(
        postcard::from_bytes::<Value>(&bytes).unwrap(),
        Value::Uuid(uuid)
    );

    let outer_only = Predicate::Eq(
        Operand::Column("outer_only".to_owned()),
        Operand::Literal(Value::String("not a payload field".to_owned())),
    );
    assert!(normalize_enum_payload_predicate(&descriptor, &source, &outer_only).is_err());
}

#[test]
fn predicate_params_collects_every_operand_position_and_operator() {
    let predicates = [Predicate::All(vec![
        Predicate::Gt(param("left"), col("value")),
        Predicate::In(
            col("kind"),
            vec![lit("fixed"), param("choice"), param("second_choice")],
        ),
        Predicate::IsNull(param("nullable")),
        Predicate::Not(Box::new(Predicate::Lte(col("limit"), param("upper")))),
    ])];

    assert_eq!(
        predicate_params(&predicates),
        BTreeSet::from([
            "choice".to_owned(),
            "left".to_owned(),
            "nullable".to_owned(),
            "second_choice".to_owned(),
            "upper".to_owned(),
        ])
    );
}

/// A declared `id` remains a user field in order and aggregate lowering;
/// the physical row UUID is retained only for tables that do not declare it.
#[test]
fn declared_id_order_and_aggregate_lower_as_source_fields() {
    let schema = public_query_eval_schema(
        PublicSchemaBuilder::new().table(
            PublicTableSchemaBuilder::new("things")
                .column("id", PublicColumnType::Uuid)
                .column("label", PublicColumnType::Text),
        ),
    );
    let (_dir, node) = open_node_with_uuid(NodeUuid::from_bytes([7; 16]), schema.clone());
    let source = root_source_id("things");

    let ordered = Query::from("things")
        .order_by("id", OrderDirection::Asc)
        .validate_runtime(&schema)
        .unwrap();
    let ordered = node
        .normalized_row_set_shape(&ordered, &ordered.bind(BTreeMap::new()).unwrap())
        .unwrap();
    assert!(matches!(
        ordered.nodes.get(&RowSetNodeId("order".to_owned())),
        Some(RowSetExpr::OrderBy { keys, .. })
            if matches!(keys.as_slice(), [NormalizedOrderKey { value: NormalizedValueRef::SourceField { source: key_source, field }, .. }]
                if key_source == &source && field == "id")
    ));

    let grouped = Query::from("things")
        .count()
        .group_by("id")
        .validate_runtime(&schema)
        .unwrap();
    let grouped = node
        .normalized_row_set_shape(&grouped, &grouped.bind(BTreeMap::new()).unwrap())
        .unwrap();
    assert!(matches!(
        grouped.nodes.get(&RowSetNodeId("aggregate".to_owned())),
        Some(RowSetExpr::Aggregate { group_by, .. })
            if matches!(group_by.as_slice(), [NormalizedValueRef::SourceField { source: key_source, field }]
                if key_source == &source && field == "id")
    ));
}

/// The shared source-key resolver is used by lookup joins, reachability seeds
/// and access joins, array correlations, and flat joins. Declared `id` must
/// select the authored field while legacy tables keep their physical row id.
#[test]
fn source_key_resolution_distinguishes_declared_and_physical_ids() {
    let schema = RuntimeSchema::new([
        TableSchema::new("declared", [ColumnSchema::new("id", ColumnType::Uuid)]),
        TableSchema::new("legacy", [ColumnSchema::new("label", ColumnType::String)]),
    ]);
    let declared = root_source_id("declared");
    let legacy = root_source_id("legacy");

    assert!(matches!(
        source_column_value(&schema, &declared, "id", JoinTarget::Column),
        NormalizedValueRef::SourceField { source, field } if source == declared && field == "id"
    ));
    assert!(matches!(
        source_column_value(&schema, &legacy, "id", JoinTarget::Column),
        NormalizedValueRef::RowId(RowIdRef::Source(source)) if source == legacy
    ));
    assert!(matches!(
        source_column_value(&schema, &declared, "id", JoinTarget::RowId),
        NormalizedValueRef::RowId(RowIdRef::Source(source)) if source == declared
    ));
}

/// A declared string `id` cannot be used as a UUID foreign-key join source,
/// while FlatJoin's explicit `_id` alias remains the physical UUID row key.
#[test]
fn declared_id_join_types_and_flat_join_physical_alias_validate() {
    let schema = RuntimeSchema::new([
        TableSchema::new("parents", [ColumnSchema::new("id", ColumnType::String)]),
        TableSchema::new("children", [ColumnSchema::new("parent", ColumnType::Uuid)])
            .with_reference("parent", "parents"),
    ]);
    assert!(
        Query::from("parents")
            .join_via_column("children", "parent", "id", [])
            .validate_runtime(&schema)
            .is_err()
    );

    let flat = Query::from("parents").flat_join("children", "parents._id", "children.parent");
    assert!(flat.validate_runtime(&schema).is_ok());
}

/// A caller's top-level inherited parent remains a receiver source through
/// policy-branch normalization, while a branch's inherited proof stays
/// authority-local.
#[test]
fn policy_branch_query_keeps_explicit_inherited_parent_contribution() {
    let schema = public_query_eval_schema(
        PublicSchemaBuilder::new()
            .table(PublicTableSchemaBuilder::new("parents").column("state", PublicColumnType::Text))
            .table(PublicTableSchemaBuilder::new("children").fk_column("parent", "parents")),
    );
    let (_dir, node) = open_node_with_uuid(NodeUuid::from_bytes([0x24; 16]), schema.clone());
    let mut query = Query::from("children").inherits("parent");
    let branch_inherits = Query::from("children").inherits("parent").inherits;
    query.policy_branches = vec![crate::query::PolicyBranch {
        filters: vec![eq(col("parent"), lit(uuid::Uuid::nil()))],
        joins: Vec::new(),
        reachable: Vec::new(),
        inherits: branch_inherits,
    }];
    let shape = query.validate_runtime(&schema).unwrap();
    let normalized = node
        .normalized_row_set_shape(&shape, &shape.bind(BTreeMap::new()).unwrap())
        .unwrap();

    assert_eq!(normalized.inherited_contributions.len(), 1);
    let contribution = &normalized.inherited_contributions[0];
    assert_eq!(contribution.id, "policy_branch:base:inherits:0");
    assert_eq!(contribution.source.table, "parents");
    assert!(matches!(
        contribution.source.path.components.as_slice(),
        [SourceRole::Alias(path)] if path == "policy_branch:base:inherits:0"
    ));
    assert!(
        normalized
            .inherited_contributions
            .iter()
            .all(|contribution| !contribution.id.starts_with("policy_branch:0:")),
        "policy-branch inheritance is an authority proof, not a receiver input"
    );
}

/// Every flat join-side occurrence is an exact receiver source.  A chained
/// flat join used to exist only in the authority tuple plan, leaving the
/// second source absent from the CoveredInput closure despite a claimed reset.
#[test]
fn chained_flat_joins_register_every_receiver_contributor() {
    let schema = public_query_eval_schema(
        PublicSchemaBuilder::new()
            .table(PublicTableSchemaBuilder::new("parents"))
            .table(PublicTableSchemaBuilder::new("children").fk_column("parent", "parents"))
            .table(PublicTableSchemaBuilder::new("grandchildren").fk_column("child", "children")),
    );
    let (_dir, node) = open_node_with_uuid(NodeUuid::from_bytes([0x23; 16]), schema.clone());
    let shape = Query::from("parents")
        .flat_join("children", "parents._id", "children.parent")
        .flat_join("grandchildren", "children._id", "grandchildren.child")
        .validate_runtime(&schema)
        .expect("chained flat join validates");
    let normalized = node
        .normalized_row_set_shape(&shape, &shape.bind(BTreeMap::new()).unwrap())
        .expect("chained flat join normalizes");

    assert_eq!(normalized.join_contributions.len(), 2);
    assert_eq!(
        normalized
            .join_contributions
            .iter()
            .map(|contribution| contribution.source.table.as_str())
            .collect::<Vec<_>>(),
        vec!["children", "grandchildren"],
        "each flat join-side scan must be available to terminal lowering as a distinct exact source"
    );
}

/// Flat-join filters must route a declared string `id` to authored fields on
/// both the root and joined source, while `_id` routes to the joined row UUID.
///
/// alice ──filter parent.id──► parent source
/// alice ──filter child.id/_id──► child source
#[test]
fn flat_join_filters_preserve_declared_id_and_physical_alias_semantics() {
    let schema = public_query_eval_schema(
        PublicSchemaBuilder::new()
            .table(PublicTableSchemaBuilder::new("parents").column("id", PublicColumnType::Text))
            .table(
                PublicTableSchemaBuilder::new("children")
                    .column("id", PublicColumnType::Text)
                    .fk_column("parent", "parents"),
            ),
    );
    let (_dir, node) = open_node_with_uuid(NodeUuid::from_bytes([8; 16]), schema.clone());
    let physical_child_id = uuid::Uuid::from_u128(0x1234);
    let shape = Query::from(table("parents").alias("parent"))
        .flat_join(
            table("children").alias("child"),
            "parent._id",
            "child.parent",
        )
        .filter(eq(col("parent.id"), lit("parent-key")))
        .filter(eq(col("child.id"), lit("child-key")))
        .filter(eq(col("child._id"), lit(physical_child_id)))
        .validate(&schema)
        .expect("declared ids and physical alias validate independently");
    let normalized = node
        .normalized_row_set_shape(&shape, &shape.bind(BTreeMap::new()).unwrap())
        .unwrap();

    assert!(matches!(
        normalized.nodes.get(&RowSetNodeId("flat_join:root_filter".to_owned())),
        Some(RowSetExpr::Filter { predicate: NormalizedPredicateExpr::Compare { left: NormalizedValueRef::SourceField { field, .. }, .. }, .. })
            if field == "id"
    ));
    assert!(matches!(
        normalized.nodes.get(&RowSetNodeId("flat_join:0:filter".to_owned())),
        Some(RowSetExpr::Filter { predicate: NormalizedPredicateExpr::And(predicates), .. })
            if predicates.iter().any(|predicate| matches!(
                predicate,
                NormalizedPredicateExpr::Compare { left: NormalizedValueRef::SourceField { field, .. }, .. }
                    if field == "id"
            )) && predicates.iter().any(|predicate| matches!(
                predicate,
                NormalizedPredicateExpr::Compare { left: NormalizedValueRef::RowId(_), .. }
            ))
    ));
}

/// Outside FlatJoin, `_id` remains an ordinary authored column rather than a
/// universal alias for the physical row identity.
#[test]
fn ordinary_query_does_not_infer_flat_join_physical_id_alias() {
    let schema = public_query_eval_schema(
        PublicSchemaBuilder::new()
            .table(PublicTableSchemaBuilder::new("things").column("_id", PublicColumnType::Text)),
    );
    let (_dir, node) = open_node_with_uuid(NodeUuid::from_bytes([9; 16]), schema.clone());
    let shape = Query::from("things")
        .filter(eq(col("_id"), lit("authored-field")))
        .validate(&schema)
        .unwrap();
    let normalized = node
        .normalized_row_set_shape(&shape, &shape.bind(BTreeMap::new()).unwrap())
        .unwrap();

    assert!(matches!(
        normalized.nodes.get(&RowSetNodeId("query:filter".to_owned())),
        Some(RowSetExpr::Filter { predicate: NormalizedPredicateExpr::Compare { left: NormalizedValueRef::SourceField { field, .. }, .. }, .. })
            if field == "_id"
    ));
}

/// Lookup and reachability policies must reject declared ID joins whose
/// authored types disagree, while a matching declared-ID reachability path
/// remains valid.
#[test]
fn declared_id_lookup_and_reachable_types_validate() {
    let lookup_schema = RuntimeSchema::new([
        TableSchema::new("roots", [ColumnSchema::new("lookup", ColumnType::Uuid)])
            .with_reference("lookup", "lookups"),
        TableSchema::new("lookups", [ColumnSchema::new("id", ColumnType::String)]),
        TableSchema::new("children", [ColumnSchema::new("lookup", ColumnType::Uuid)])
            .with_reference("lookup", "lookups"),
    ]);
    assert!(
        Query::from("roots")
            .join_via_source_lookup(
                "children",
                "lookup",
                JoinSourceLookup {
                    table: "lookups".to_owned(),
                    row_id_source_column: "lookup".to_owned(),
                    value_column: "id".to_owned(),
                },
                [],
            )
            .validate_runtime(&lookup_schema)
            .is_err()
    );

    let reachable_schema = |root_id_type| {
        RuntimeSchema::new([
            TableSchema::new("roots", [ColumnSchema::new("id", root_id_type)]),
            TableSchema::new(
                "access",
                [
                    ColumnSchema::new("id", ColumnType::String),
                    ColumnSchema::new("team", ColumnType::Uuid),
                ],
            )
            .with_reference("id", "roots")
            .with_reference("team", "teams"),
            TableSchema::new("teams", [ColumnSchema::new("label", ColumnType::String)]),
            TableSchema::new(
                "edges",
                [
                    ColumnSchema::new("member", ColumnType::Uuid),
                    ColumnSchema::new("parent", ColumnType::Uuid),
                ],
            )
            .with_reference("member", "teams")
            .with_reference("parent", "teams"),
        ])
    };
    let reachable = || {
        Query::from("roots").reachable_via(
            "access",
            "id",
            "team",
            lit(Value::Uuid(uuid::Uuid::nil())),
            "edges",
            "member",
            "parent",
            [],
        )
    };
    assert!(
        reachable()
            .validate_runtime(&reachable_schema(ColumnType::Uuid))
            .is_err()
    );
    assert!(
        reachable()
            .validate_runtime(&reachable_schema(ColumnType::String))
            .is_ok()
    );
}

/// A same-table reachability seed may use `id` only when its effective
/// frontier value is a non-null UUID; string and nullable declared IDs cannot
/// drive UUID edge traversal.
#[test]
fn reachable_self_seed_id_requires_non_nullable_uuid() {
    let schema = |team_id_type| {
        RuntimeSchema::new([
            TableSchema::new("roots", [ColumnSchema::new("label", ColumnType::String)]),
            TableSchema::new(
                "access",
                [
                    ColumnSchema::new("root", ColumnType::Uuid),
                    ColumnSchema::new("team", ColumnType::Uuid),
                ],
            )
            .with_reference("root", "roots")
            .with_reference("team", "teams"),
            TableSchema::new(
                "teams",
                [
                    ColumnSchema::new("id", team_id_type),
                    ColumnSchema::new("identity", ColumnType::Uuid),
                ],
            ),
            TableSchema::new(
                "edges",
                [
                    ColumnSchema::new("member", ColumnType::Uuid),
                    ColumnSchema::new("parent", ColumnType::Uuid),
                ],
            )
            .with_reference("member", "teams")
            .with_reference("parent", "teams"),
        ])
    };
    let query = || {
        Query::from("roots")
            .reachable_via(
                "access",
                "root",
                "team",
                lit(Value::Uuid(uuid::Uuid::nil())),
                "edges",
                "member",
                "parent",
                [],
            )
            .seeded_by("teams", "identity", "sub", "id")
    };

    assert!(query().validate_runtime(&schema(ColumnType::Uuid)).is_ok());
    assert!(
        query()
            .validate_runtime(&schema(ColumnType::String))
            .is_err()
    );
    assert!(
        query()
            .validate_runtime(&schema(ColumnType::Uuid.nullable()))
            .is_err()
    );
}

#[test]
fn join_read_tables_include_source_lookups_and_nested_joins() {
    let nested = crate::query::JoinVia {
        table: "nested_junction".to_owned(),
        on_column: "target".to_owned(),
        target: Default::default(),
        source_column: None,
        source_lookup: Some(JoinSourceLookup {
            table: "nested_lookup".to_owned(),
            row_id_source_column: "lookup_id".to_owned(),
            value_column: "value".to_owned(),
        }),
        correlated_filters: vec![],
        filters: vec![],
        nested_joins: vec![],
    };
    let root = crate::query::JoinVia {
        table: "root_junction".to_owned(),
        on_column: "target".to_owned(),
        target: Default::default(),
        source_column: None,
        source_lookup: Some(JoinSourceLookup {
            table: "root_lookup".to_owned(),
            row_id_source_column: "lookup_id".to_owned(),
            value_column: "value".to_owned(),
        }),
        correlated_filters: vec![],
        filters: vec![],
        nested_joins: vec![nested],
    };
    let mut tables = BTreeSet::new();

    collect_join_read_tables(&root, &mut tables);

    assert_eq!(
        tables,
        BTreeSet::from([
            "nested_junction".to_owned(),
            "nested_lookup".to_owned(),
            "root_junction".to_owned(),
            "root_lookup".to_owned(),
        ])
    );
}

#[test]
fn aggregate_query_normalizes_to_query_engine_aggregate_node() {
    let (_dir, node) = open_node();
    let shape = Query::from("issues")
        .filter(eq(col("state"), lit("open")))
        .count()
        .validate_runtime(&schema())
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let normalized = node.normalized_row_set_shape(&shape, &binding).unwrap();
    assert!(matches!(
        normalized.nodes.get(&normalized.root),
        Some(RowSetExpr::Aggregate { .. })
    ));
}

#[test]
fn join_via_nested_joins_normalize_as_parent_projection_gate() {
    let (_dir, node) = open_node();
    let nested = Query::from("issue_members")
        .join_via_row_id("users", "user", [eq(col("name"), lit("Alice"))])
        .joins
        .into_iter()
        .next()
        .unwrap();
    let shape = Query::from("issues")
        .join_via_with_nested_joins("issue_members", "issue", [], [nested])
        .validate_runtime(&schema())
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let normalized = node.normalized_row_set_shape(&shape, &binding).unwrap();

    assert_eq!(normalized.join_contributions.len(), 1);
    let contribution = &normalized.join_contributions[0];
    assert_eq!(contribution.input.0, "join_via:0:nested:0:parent_project");
    assert!(matches!(
        normalized.nodes.get(&contribution.input),
        Some(RowSetExpr::Project { input, columns })
            if input.0 == "join_via:0:nested:0:join"
                && columns.iter().any(|column| column.output.name == "id")
                && columns.iter().any(|column| column.output.name == "issue")
                && columns.iter().any(|column| column.output.name == "user")
    ));
    assert!(matches!(
        normalized.nodes.get(&RowSetNodeId("join_via:0:nested:0:join".to_owned())),
        Some(RowSetExpr::Join { left, right, .. })
            if left.0 == "join_via:0:source"
                && right.0 == "join_via:0:nested:0:filter"
    ));
    assert!(matches!(
        normalized.nodes.get(&normalized.root),
        Some(RowSetExpr::Join { right, .. }) if right == &contribution.input
    ));
}

#[test]
fn join_via_source_lookup_normalizes_as_lookup_bridge_projection() {
    let (_dir, node) = open_node();
    let shape = Query::from("issues")
        .join_via_source_lookup(
            "issue_members",
            "user",
            JoinSourceLookup {
                table: "users".to_owned(),
                row_id_source_column: "assignee".to_owned(),
                value_column: "id".to_owned(),
            },
            [],
        )
        .validate_runtime(&schema())
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let normalized = node.normalized_row_set_shape(&shape, &binding).unwrap();

    assert_eq!(normalized.join_contributions.len(), 1);
    let contribution = &normalized.join_contributions[0];
    assert_eq!(contribution.input.0, "join_via:0:lookup_project");
    assert!(matches!(
        normalized.nodes.get(&contribution.input),
        Some(RowSetExpr::Project { input, columns })
            if input.0 == "join_via:0:lookup_join"
                && columns.iter().any(|column| column.output.name == "id")
                && columns.iter().any(|column| column.output.name == "issue")
                && columns.iter().any(|column| column.output.name == "user")
                && columns.iter().any(|column| column.output.name == "assignee")
    ));
    assert!(matches!(
        normalized.nodes.get(&normalized.root),
        Some(RowSetExpr::Join { right, on, .. })
            if right == &contribution.input
                && matches!(
                    on,
                    NormalizedPredicateExpr::Compare { left, right, .. }
                        if matches!(
                            left,
                            NormalizedValueRef::SourceField { field, .. } if field == "assignee"
                        ) && matches!(
                            right,
                            NormalizedValueRef::SourceField { field, .. } if field == "assignee"
                        )
                )
    ));
}
