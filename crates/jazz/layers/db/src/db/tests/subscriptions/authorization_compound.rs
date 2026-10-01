use super::*;

fn compound_exists_rel_test_policy_with_secondary(evidence_right_column: &str) -> PublicPolicyExpr {
    let relation_column = |scope: &str, column: &str| PublicRelColumnRef {
        scope: Some(scope.to_owned()),
        column: column.to_owned(),
    };
    let equality = |left: (&str, &str), right: (&str, &str)| PublicRelJoinCondition {
        left: relation_column(left.0, left.1),
        right: relation_column(right.0, right.1),
    };
    PublicPolicyExpr::ExistsRel {
        rel: PublicRelExpr::Filter {
            input: Box::new(PublicRelExpr::Join {
                left: Box::new(PublicRelExpr::Join {
                    left: Box::new(PublicRelExpr::TableScan {
                        table: "left_facts".into(),
                        alias: Some("left_fact".to_owned()),
                    }),
                    right: Box::new(PublicRelExpr::TableScan {
                        table: "right_facts".into(),
                        alias: Some("right_fact".to_owned()),
                    }),
                    on: vec![equality(
                        ("left_fact", "resource_id"),
                        ("right_fact", "resource_id"),
                    )],
                    join_kind: PublicRelJoinKind::Inner,
                }),
                right: Box::new(PublicRelExpr::TableScan {
                    table: "evidence".into(),
                    alias: Some("evidence".to_owned()),
                }),
                on: vec![
                    equality(("left_fact", "left_key"), ("evidence", "left_key")),
                    equality(
                        ("right_fact", "right_key"),
                        ("evidence", evidence_right_column),
                    ),
                ],
                join_kind: PublicRelJoinKind::Inner,
            }),
            predicate: PublicRelPredicateExpr::Cmp {
                left: relation_column("left_fact", "resource_id"),
                op: PublicRelPredicateCmpOp::Eq,
                right: PublicRelValueRef::RowId(PublicRelRowIdRef::Outer),
            },
        },
    }
}

#[test]
fn identical_exists_rel_joins_keep_branch_specific_equalities() {
    use crate::model::public_schema::{CmpOp, PolicyValue, Value as PublicValue};

    let branch = |evidence_right_column: &str, label: &str| {
        PublicPolicyExpr::And(vec![
            compound_exists_rel_test_policy_with_secondary(evidence_right_column),
            PublicPolicyExpr::Cmp {
                column: "label".to_owned(),
                op: CmpOp::Eq,
                value: PolicyValue::Literal(PublicValue::Text(label.to_owned())),
            },
        ])
    };
    let policy = PublicPolicyExpr::Or(vec![
        branch("right_key", "first"),
        branch("alternate_right_key", "second"),
    ]);
    let schema = build_public_db_test_schema(
        PublicSchemaBuilder::new()
            .table(
                PublicTableSchemaBuilder::new("resources")
                    .column("label", PublicColumnType::Text)
                    .policies(PublicTablePolicies::new().with_select(policy)),
            )
            .table(
                PublicTableSchemaBuilder::new("left_facts")
                    .fk_column("resource_id", "resources")
                    .column("left_key", PublicColumnType::Text),
            )
            .table(
                PublicTableSchemaBuilder::new("right_facts")
                    .fk_column("resource_id", "resources")
                    .column("right_key", PublicColumnType::Text),
            )
            .table(
                PublicTableSchemaBuilder::new("evidence")
                    .column("left_key", PublicColumnType::Text)
                    .column("right_key", PublicColumnType::Text)
                    .column("alternate_right_key", PublicColumnType::Text),
            ),
    );
    let reader = AuthorSubject::for_test_bytes([0x9e; 16]);
    let db = open_db(0x9e, AuthorSubject::SYSTEM, &schema);
    let denied = row(0xd4);
    let allowed = row(0xd5);
    for (id, label) in [(denied, "first"), (allowed, "first")] {
        db.insert(
            "resources",
            BTreeMap::from([("label".to_owned(), Value::String(label.to_owned()))]),
            crate::db::InsertOptions {
                row_id: Some(id),
                ..Default::default()
            },
        )
        .unwrap();
    }
    for (resource, left_key, right_key, left_id, right_id) in [
        (denied, "left-denied", "right-denied", row(0xd6), row(0xd7)),
        (
            allowed,
            "left-allowed",
            "right-allowed",
            row(0xd8),
            row(0xd9),
        ),
    ] {
        db.insert(
            "left_facts",
            BTreeMap::from([
                ("resource_id".to_owned(), Value::Uuid(resource.0)),
                ("left_key".to_owned(), Value::String(left_key.to_owned())),
            ]),
            crate::db::InsertOptions {
                row_id: Some(left_id),
                ..Default::default()
            },
        )
        .unwrap();
        db.insert(
            "right_facts",
            BTreeMap::from([
                ("resource_id".to_owned(), Value::Uuid(resource.0)),
                ("right_key".to_owned(), Value::String(right_key.to_owned())),
            ]),
            crate::db::InsertOptions {
                row_id: Some(right_id),
                ..Default::default()
            },
        )
        .unwrap();
    }
    for (id, left_key, right_key, alternate_right_key) in [
        (row(0xda), "left-denied", "not-right-denied", "right-denied"),
        (
            row(0xdb),
            "left-allowed",
            "right-allowed",
            "not-right-allowed",
        ),
    ] {
        db.insert(
            "evidence",
            BTreeMap::from([
                ("left_key".to_owned(), Value::String(left_key.to_owned())),
                ("right_key".to_owned(), Value::String(right_key.to_owned())),
                (
                    "alternate_right_key".to_owned(),
                    Value::String(alternate_right_key.to_owned()),
                ),
            ]),
            crate::db::InsertOptions {
                row_id: Some(id),
                ..Default::default()
            },
        )
        .unwrap();
    }

    let prepared = db.prepare_query(&Query::from("resources")).unwrap();
    let mut subscription =
        block_on(db.subscribe_for_identity(&prepared, ReadOpts::default(), reader))
            .expect("identical compiled joins retain their branch-specific equalities");
    assert_eq!(
        row_ids(&opened_rows(block_on(subscription.next_raw()).unwrap())),
        vec![allowed],
        "the right_key equality belongs only to the first branch; the matching alternate key cannot bypass that branch's label filter"
    );
}

#[test]
fn inherited_referencing_policy_lowers_nested_compound_exists_rel() {
    let reader = AuthorSubject::for_test_bytes([0x9f; 16]);
    let schema =
        build_public_db_test_schema(
            PublicSchemaBuilder::new()
                .table(
                    PublicTableSchemaBuilder::new("resources")
                        .column("label", PublicColumnType::Text)
                        .policies(PublicTablePolicies::new().with_select(
                            PublicPolicyExpr::InheritsReferencing {
                                operation: crate::model::public_schema::Operation::Select,
                                source_table: "comments".to_owned(),
                                via_column: "parent_resource".to_owned(),
                                max_depth: None,
                            },
                        )),
                )
                .table(
                    PublicTableSchemaBuilder::new("comments")
                        .fk_column("parent_resource", "resources")
                        .policies(PublicTablePolicies::new().with_select(
                            compound_exists_rel_test_policy_with_secondary("right_key"),
                        )),
                )
                .table(
                    PublicTableSchemaBuilder::new("left_facts")
                        .fk_column("resource_id", "comments")
                        .column("left_key", PublicColumnType::Text),
                )
                .table(
                    PublicTableSchemaBuilder::new("right_facts")
                        .fk_column("resource_id", "comments")
                        .column("right_key", PublicColumnType::Text),
                )
                .table(
                    PublicTableSchemaBuilder::new("evidence")
                        .column("left_key", PublicColumnType::Text)
                        .column("right_key", PublicColumnType::Text),
                ),
        );
    let db = open_db(0x9f, AuthorSubject::SYSTEM, &schema);
    let resource = row(0xa4);
    let comment = row(0xa5);
    db.insert(
        "resources",
        BTreeMap::from([("label".to_owned(), Value::String("resource".to_owned()))]),
        crate::db::InsertOptions {
            row_id: Some(resource),
            ..Default::default()
        },
    )
    .unwrap();
    db.insert(
        "comments",
        BTreeMap::from([("parent_resource".to_owned(), Value::Uuid(resource.0))]),
        crate::db::InsertOptions {
            row_id: Some(comment),
            ..Default::default()
        },
    )
    .unwrap();
    db.insert(
        "left_facts",
        BTreeMap::from([
            ("resource_id".to_owned(), Value::Uuid(comment.0)),
            ("left_key".to_owned(), Value::String("left".to_owned())),
        ]),
        Default::default(),
    )
    .unwrap();
    db.insert(
        "right_facts",
        BTreeMap::from([
            ("resource_id".to_owned(), Value::Uuid(comment.0)),
            ("right_key".to_owned(), Value::String("right".to_owned())),
        ]),
        Default::default(),
    )
    .unwrap();
    db.insert(
        "evidence",
        BTreeMap::from([
            ("left_key".to_owned(), Value::String("left".to_owned())),
            (
                "right_key".to_owned(),
                Value::String("not-right".to_owned()),
            ),
        ]),
        Default::default(),
    )
    .unwrap();

    let prepared = db.prepare_query(&Query::from("resources")).unwrap();
    let mut subscription =
        block_on(db.subscribe_for_identity(&prepared, ReadOpts::default(), reader))
            .expect("nested inherited relation groups normalize at the source-row seam");
    assert!(
        opened_rows(block_on(subscription.next_raw()).unwrap()).is_empty(),
        "a partial witness must not authorize the inherited parent"
    );
    let complete = db
        .insert(
            "evidence",
            BTreeMap::from([
                ("left_key".to_owned(), Value::String("left".to_owned())),
                ("right_key".to_owned(), Value::String("right".to_owned())),
            ]),
            Default::default(),
        )
        .unwrap();
    let (added, updated, removed) = delta_rows(block_on(subscription.next_raw()).unwrap());
    assert_eq!(row_ids(&added), vec![resource]);
    assert!(updated.is_empty());
    assert!(removed.is_empty());

    db.delete("evidence", complete.row_uuid(), Default::default())
        .unwrap();
    let (added, updated, removed) = delta_rows(block_on(subscription.next_raw()).unwrap());
    assert!(added.is_empty());
    assert!(updated.is_empty());
    assert_eq!(
        removed.iter().map(|row| row.row_uuid).collect::<Vec<_>>(),
        vec![resource]
    );
}

#[test]
fn empty_in_normalization_keeps_compound_branch_provenance() {
    use crate::model::public_schema::{CmpOp, PolicyValue, Value as PublicValue};

    let schema = build_public_db_test_schema(
        PublicSchemaBuilder::new()
            .table(
                PublicTableSchemaBuilder::new("resources")
                    .column("label", PublicColumnType::Text)
                    .policies(
                        PublicTablePolicies::new().with_select(PublicPolicyExpr::Or(vec![
                            PublicPolicyExpr::And(vec![
                                compound_exists_rel_test_policy_with_secondary("right_key"),
                                PublicPolicyExpr::Not(Box::new(PublicPolicyExpr::Not(Box::new(
                                    PublicPolicyExpr::InList {
                                        column: "label".to_owned(),
                                        values: Vec::new(),
                                    },
                                )))),
                            ]),
                            PublicPolicyExpr::And(vec![
                                compound_exists_rel_test_policy_with_secondary(
                                    "alternate_right_key",
                                ),
                                PublicPolicyExpr::Cmp {
                                    column: "label".to_owned(),
                                    op: CmpOp::Eq,
                                    value: PolicyValue::Literal(PublicValue::Text(
                                        "allowed".to_owned(),
                                    )),
                                },
                            ]),
                        ])),
                    ),
            )
            .table(
                PublicTableSchemaBuilder::new("left_facts")
                    .fk_column("resource_id", "resources")
                    .column("left_key", PublicColumnType::Text),
            )
            .table(
                PublicTableSchemaBuilder::new("right_facts")
                    .fk_column("resource_id", "resources")
                    .column("right_key", PublicColumnType::Text),
            )
            .table(
                PublicTableSchemaBuilder::new("evidence")
                    .column("left_key", PublicColumnType::Text)
                    .column("right_key", PublicColumnType::Text)
                    .column("alternate_right_key", PublicColumnType::Text),
            ),
    );
    let reader = AuthorSubject::for_test_bytes([0xa6; 16]);
    let db = open_db(0xa6, AuthorSubject::SYSTEM, &schema);
    let resource = row(0xa7);
    db.insert(
        "resources",
        BTreeMap::from([("label".to_owned(), Value::String("allowed".to_owned()))]),
        crate::db::InsertOptions {
            row_id: Some(resource),
            ..Default::default()
        },
    )
    .unwrap();
    db.insert(
        "left_facts",
        BTreeMap::from([
            ("resource_id".to_owned(), Value::Uuid(resource.0)),
            ("left_key".to_owned(), Value::String("left".to_owned())),
        ]),
        Default::default(),
    )
    .unwrap();
    db.insert(
        "right_facts",
        BTreeMap::from([
            ("resource_id".to_owned(), Value::Uuid(resource.0)),
            ("right_key".to_owned(), Value::String("right".to_owned())),
        ]),
        Default::default(),
    )
    .unwrap();
    db.insert(
        "evidence",
        BTreeMap::from([
            ("left_key".to_owned(), Value::String("left".to_owned())),
            ("right_key".to_owned(), Value::String("right".to_owned())),
            (
                "alternate_right_key".to_owned(),
                Value::String("not-right".to_owned()),
            ),
        ]),
        Default::default(),
    )
    .unwrap();

    let prepared = db.prepare_query(&Query::from("resources")).unwrap();
    let mut subscription =
        block_on(db.subscribe_for_identity(&prepared, ReadOpts::default(), reader)).unwrap();
    assert!(
        opened_rows(block_on(subscription.next_raw()).unwrap()).is_empty(),
        "the only satisfiable boolean branch requires alternate_right_key, not right_key"
    );
}
