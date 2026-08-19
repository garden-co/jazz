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
    let schema = JazzSchema::new([TableSchema::new(
        "things",
        [
            ColumnSchema::new("id", ColumnType::Uuid),
            ColumnSchema::new("label", ColumnType::String),
        ],
    )]);
    let (_dir, node) = open_node_with_uuid(NodeUuid::from_bytes([7; 16]), schema.clone());
    let source = root_source_id("things");

    let ordered = Query::from("things")
        .order_by("id", OrderDirection::Asc)
        .validate(&schema)
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
        .validate(&schema)
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
    let schema = JazzSchema::new([
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
    let schema = JazzSchema::new([
        TableSchema::new("parents", [ColumnSchema::new("id", ColumnType::String)]),
        TableSchema::new("children", [ColumnSchema::new("parent", ColumnType::Uuid)])
            .with_reference("parent", "parents"),
    ]);
    assert!(
        Query::from("parents")
            .join_via_column("children", "parent", "id", [])
            .validate(&schema)
            .is_err()
    );

    let mut flat = Query::from("parents");
    flat.flat_join = Some(FlatJoin {
        root_alias: None,
        sources: vec![FlatJoinSource {
            table: "children".to_owned(),
            alias: None,
            on: FlatJoinOn {
                left: "parents._id".to_owned(),
                right: "children.parent".to_owned(),
            },
        }],
    });
    assert!(flat.validate(&schema).is_ok());
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
        .validate(&schema())
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
        .validate(&schema())
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
        .validate(&schema())
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
