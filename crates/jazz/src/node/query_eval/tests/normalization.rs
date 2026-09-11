//! normalization query-evaluation tests.

use super::*;

// Public row equality cannot detect native/WASM source-name disagreement on
// one host. Pin the compiler-owned protocol identities to literal names.
#[test]
fn implicit_reference_source_identities_are_pointer_width_independent() {
    let schema = public_query_eval_schema(
        PublicSchemaBuilder::new()
            .table(PublicTableSchemaBuilder::new("projects").column("name", PublicColumnType::Text))
            .table(
                PublicTableSchemaBuilder::new("todos")
                    .nullable_fk_column("parent", "todos")
                    .nullable_fk_column("project", "projects"),
            ),
    );
    let (_dir, node) = open_node_with_uuid(NodeUuid::from_bytes([0x73; 16]), schema.clone());
    let shape = Query::from("todos").validate_runtime(&schema).unwrap();
    let normalized = node
        .normalized_row_set_shape(&shape, &shape.bind(BTreeMap::new()).unwrap())
        .unwrap();
    assert_eq!(
        normalized.auxiliary_sources,
        BTreeSet::from([
            SourceId {
                table: "todos".to_owned(),
                path: SourcePath {
                    components: vec![
                        SourceRole::Root,
                        SourceRole::Alias("reference:parent".to_owned())
                    ]
                },
            },
            SourceId {
                table: "projects".to_owned(),
                path: SourcePath {
                    components: vec![
                        SourceRole::Root,
                        SourceRole::Alias("reference:project".to_owned())
                    ]
                },
            },
        ]),
    );
}

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
