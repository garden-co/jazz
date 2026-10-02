use super::*;

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

fn inherited_referencing_parent_witness_schema(parent_policy: PublicPolicyExpr) -> JazzSchema {
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
                    .fk_column("permission_parent", "parents")
                    .column("scope", PublicColumnType::Text)
                    .policies(
                        PublicTablePolicies::new().with_select(PublicPolicyExpr::Inherits {
                            operation: crate::model::public_schema::Operation::Select,
                            via_column: "permission_parent".to_owned(),
                            max_depth: None,
                        }),
                    ),
            )
            .table(
                PublicTableSchemaBuilder::new("parents")
                    .column("label", PublicColumnType::Text)
                    .column("scope", PublicColumnType::Text)
                    .policies(PublicTablePolicies::new().with_select(parent_policy)),
            )
            .table(
                PublicTableSchemaBuilder::new("left_facts")
                    .fk_column("resource_id", "parents")
                    .column("scope", PublicColumnType::Text)
                    .column("left_key", PublicColumnType::Text)
                    .policies(PublicTablePolicies::new().with_select(PublicPolicyExpr::True)),
            )
            .table(
                PublicTableSchemaBuilder::new("right_facts")
                    .fk_column("resource_id", "parents")
                    .column("right_key", PublicColumnType::Text)
                    .policies(PublicTablePolicies::new().with_select(PublicPolicyExpr::True)),
            )
            .table(
                PublicTableSchemaBuilder::new("evidence")
                    .column("left_key", PublicColumnType::Text)
                    .column("right_key", PublicColumnType::Text)
                    .policies(PublicTablePolicies::new().with_select(PublicPolicyExpr::True)),
            ),
    )
}

fn progress_inherited_witness_future<F: Future>(bob: &Db, future: F, phase: &str) -> F::Output {
    let mut future = pin!(future);
    let mut tick = pin!(bob.tick());
    let mut context = Context::from_waker(Waker::noop());
    for _ in 0..20_000 {
        if let Poll::Ready(result) = future.as_mut().poll(&mut context) {
            return result;
        }
        if let Poll::Ready(result) = tick.as_mut().poll(&mut context) {
            result.expect("drive inherited witness subscription");
            tick.set(bob.tick());
        }
        std::thread::yield_now();
    }
    panic!("inherited witness subscription did not finish {phase} within 20,000 progress polls");
}

/// Alice's maintained read follows the referencing comment's permission-parent
/// FK before evaluating one compound witness, including its parent's outer
/// scope correlation. Split equalities, facts from different parents, a full
/// witness for a different parent, and a same-parent wrong-scope witness stay hidden.
///
/// Alice reads resource <-- comment --FK--> parent --> compound witness
/// Bob inserts full witness --> add resource; Bob deletes it --> remove resource
#[test]
fn inherited_referencing_parent_compound_witness_add_and_retract() {
    let alice = AuthorSubject::for_test_bytes([0xb1; 16]);
    let mut policy = compound_exists_rel_test_policy();
    if let PublicPolicyExpr::ExistsRel {
        rel: PublicRelExpr::Filter { predicate, .. },
    } = &mut policy
    {
        *predicate = PublicRelPredicateExpr::And(vec![
            predicate.clone(),
            PublicRelPredicateExpr::Cmp {
                left: PublicRelColumnRef {
                    scope: Some("left_fact".to_owned()),
                    column: "scope".to_owned(),
                },
                op: PublicRelPredicateCmpOp::Eq,
                right: PublicRelValueRef::OuterColumn(PublicRelColumnRef {
                    scope: None,
                    column: "scope".to_owned(),
                }),
            },
        ]);
    } else {
        panic!("compound witness fixture must expose its public relation filter");
    }
    let schema = inherited_referencing_parent_witness_schema(policy);
    // Bob acts as the system fixture writer; observations use Alice's serving
    // identity through public Db APIs, not the normalizer's internal graph.
    let bob = open_db(0xb1, AuthorSubject::SYSTEM, &schema);
    let target = row(0xb2);
    let partial = row(0xb3);
    let cross_parent = row(0xb4);
    let wrong_parent = row(0xb5);
    let target_parent = row(0xb6);
    let partial_parent = row(0xb7);
    let cross_left_parent = row(0xb8);
    let other_parent = row(0xb9);
    let unwitnessed_parent = row(0xba);
    let wrong_scope = row(0xc6);
    let wrong_scope_parent = row(0xc7);
    for (id, label) in [
        (target, "target"),
        (partial, "partial"),
        (cross_parent, "cross-parent"),
        (wrong_parent, "wrong-parent"),
        (wrong_scope, "wrong-scope"),
    ] {
        bob.insert(
            "resources",
            BTreeMap::from([("label".to_owned(), Value::String(label.to_owned()))]),
            crate::db::InsertOptions {
                row_id: Some(id),
                ..Default::default()
            },
        )
        .unwrap();
    }
    for (id, label) in [
        (target_parent, "target"),
        (partial_parent, "partial"),
        (cross_left_parent, "cross-left"),
        (other_parent, "other"),
        (unwitnessed_parent, "unwitnessed"),
        (wrong_scope_parent, "wrong-scope"),
    ] {
        bob.insert(
            "parents",
            BTreeMap::from([
                ("label".to_owned(), Value::String(label.to_owned())),
                ("scope".to_owned(), Value::String("parent-scope".to_owned())),
            ]),
            crate::db::InsertOptions {
                row_id: Some(id),
                ..Default::default()
            },
        )
        .unwrap();
    }
    for (comment, resource, parent) in [
        (row(0xbb), target, target_parent),
        (row(0xbc), partial, partial_parent),
        (row(0xbd), cross_parent, cross_left_parent),
        (row(0xbe), wrong_parent, unwitnessed_parent),
        (row(0xc8), wrong_scope, wrong_scope_parent),
    ] {
        bob.insert(
            "comments",
            BTreeMap::from([
                ("parent_resource".to_owned(), Value::Uuid(resource.0)),
                ("permission_parent".to_owned(), Value::Uuid(parent.0)),
                ("scope".to_owned(), Value::String("child-scope".to_owned())),
            ]),
            crate::db::InsertOptions {
                row_id: Some(comment),
                ..Default::default()
            },
        )
        .unwrap();
    }
    for (parent, key, scope) in [
        (target_parent, "left-target", "parent-scope"),
        (partial_parent, "left-partial", "parent-scope"),
        (cross_left_parent, "left-cross", "parent-scope"),
        (other_parent, "left-other", "parent-scope"),
        (wrong_scope_parent, "left-wrong-scope", "child-scope"),
    ] {
        bob.insert(
            "left_facts",
            BTreeMap::from([
                ("resource_id".to_owned(), Value::Uuid(parent.0)),
                ("left_key".to_owned(), Value::String(key.to_owned())),
                ("scope".to_owned(), Value::String(scope.to_owned())),
            ]),
            Default::default(),
        )
        .unwrap();
    }
    for (parent, key) in [
        (target_parent, "right-target"),
        (partial_parent, "right-partial"),
        (other_parent, "right-other"),
        (wrong_scope_parent, "right-wrong-scope"),
    ] {
        bob.insert(
            "right_facts",
            BTreeMap::from([
                ("resource_id".to_owned(), Value::Uuid(parent.0)),
                ("right_key".to_owned(), Value::String(key.to_owned())),
            ]),
            Default::default(),
        )
        .unwrap();
    }
    for (left_key, right_key) in [
        ("left-target", "not-right-target"),
        ("left-partial", "not-right-partial"),
        ("not-left-partial", "right-partial"),
        ("left-cross", "right-other"),
        ("left-other", "right-other"),
        ("left-wrong-scope", "right-wrong-scope"),
    ] {
        bob.insert(
            "evidence",
            BTreeMap::from([
                ("left_key".to_owned(), Value::String(left_key.to_owned())),
                ("right_key".to_owned(), Value::String(right_key.to_owned())),
            ]),
            Default::default(),
        )
        .unwrap();
    }

    let prepared = bob.prepare_query(&Query::from("resources")).unwrap();
    let mut subscription =
        block_on(bob.subscribe_for_identity(&prepared, ReadOpts::default(), alice))
            .expect("a referencing policy can inherit a parent's correlated compound witness");
    assert!(
        opened_rows(
            progress_inherited_witness_future(&bob, subscription.next_raw(), "empty opening")
                .unwrap(),
        )
        .is_empty(),
        "partial witnesses, cross-parent fact tuples, and another parent's full witness cannot authorize Alice"
    );

    let complete = bob
        .insert(
            "evidence",
            BTreeMap::from([
                (
                    "left_key".to_owned(),
                    Value::String("left-target".to_owned()),
                ),
                (
                    "right_key".to_owned(),
                    Value::String("right-target".to_owned()),
                ),
            ]),
            Default::default(),
        )
        .unwrap();
    let (added, updated, removed) = delta_rows(
        progress_inherited_witness_future(
            &bob,
            subscription.next_raw(),
            "same-parent witness addition",
        )
        .unwrap(),
    );
    assert_eq!(
        row_ids(&added),
        vec![target],
        "only the resource whose comment points to the fully witnessed parent enters"
    );
    assert!(updated.is_empty());
    assert!(removed.is_empty());

    bob.delete("evidence", complete.row_uuid(), Default::default())
        .unwrap();
    let (added, updated, removed) = delta_rows(
        progress_inherited_witness_future(
            &bob,
            subscription.next_raw(),
            "same-parent witness retraction",
        )
        .unwrap(),
    );
    assert!(added.is_empty());
    assert!(updated.is_empty());
    assert_eq!(
        removed.iter().map(|row| row.row_uuid).collect::<Vec<_>>(),
        vec![target],
        "retracting the same-parent witness removes the resource despite the remaining false witnesses"
    );
}

/// Alice's one-shot read retains the formerly supported single-ON-equality
/// ExistsRel composition through a referencing comment and its permission
/// parent. Bob's full witness for a different parent cannot authorize a sibling.
#[test]
fn inherited_referencing_parent_single_equality_witness_read() {
    let relation_column = |scope: &str, column: &str| PublicRelColumnRef {
        scope: Some(scope.to_owned()),
        column: column.to_owned(),
    };
    let policy = PublicPolicyExpr::ExistsRel {
        rel: PublicRelExpr::Filter {
            input: Box::new(PublicRelExpr::Join {
                left: Box::new(PublicRelExpr::TableScan {
                    table: "left_facts".into(),
                    alias: Some("left_fact".to_owned()),
                }),
                right: Box::new(PublicRelExpr::TableScan {
                    table: "evidence".into(),
                    alias: Some("evidence".to_owned()),
                }),
                on: vec![PublicRelJoinCondition {
                    left: relation_column("left_fact", "left_key"),
                    right: relation_column("evidence", "left_key"),
                }],
                join_kind: PublicRelJoinKind::Inner,
            }),
            predicate: PublicRelPredicateExpr::Cmp {
                left: relation_column("left_fact", "resource_id"),
                op: PublicRelPredicateCmpOp::Eq,
                right: PublicRelValueRef::RowId(PublicRelRowIdRef::Outer),
            },
        },
    };
    let schema = inherited_referencing_parent_witness_schema(policy);
    let alice = AuthorSubject::for_test_bytes([0xbf; 16]);
    let bob = open_db(0xbf, AuthorSubject::SYSTEM, &schema);
    let allowed = row(0xc0);
    let denied = row(0xc1);
    let allowed_parent = row(0xc2);
    let denied_parent = row(0xc3);
    for (resource, parent, comment, label) in [
        (allowed, allowed_parent, row(0xc4), "allowed"),
        (denied, denied_parent, row(0xc5), "denied"),
    ] {
        for (table, id) in [("resources", resource), ("parents", parent)] {
            let mut cells = BTreeMap::from([("label".to_owned(), Value::String(label.to_owned()))]);
            if table == "parents" {
                cells.insert("scope".to_owned(), Value::String("parent-scope".to_owned()));
            }
            bob.insert(
                table,
                cells,
                crate::db::InsertOptions {
                    row_id: Some(id),
                    ..Default::default()
                },
            )
            .unwrap();
        }
        bob.insert(
            "comments",
            BTreeMap::from([
                ("parent_resource".to_owned(), Value::Uuid(resource.0)),
                ("permission_parent".to_owned(), Value::Uuid(parent.0)),
                ("scope".to_owned(), Value::String("child-scope".to_owned())),
            ]),
            crate::db::InsertOptions {
                row_id: Some(comment),
                ..Default::default()
            },
        )
        .unwrap();
        bob.insert(
            "left_facts",
            BTreeMap::from([
                ("resource_id".to_owned(), Value::Uuid(parent.0)),
                ("left_key".to_owned(), Value::String(label.to_owned())),
                ("scope".to_owned(), Value::String("parent-scope".to_owned())),
            ]),
            Default::default(),
        )
        .unwrap();
    }
    bob.insert(
        "evidence",
        BTreeMap::from([
            ("left_key".to_owned(), Value::String("allowed".to_owned())),
            ("right_key".to_owned(), Value::String("unused".to_owned())),
        ]),
        Default::default(),
    )
    .unwrap();

    let prepared = bob.prepare_query(&Query::from("resources")).unwrap();
    let rows = progress_inherited_witness_future(
        &bob,
        bob.all_for_identity(&prepared, ReadOpts::default(), alice),
        "single-equality read",
    )
    .expect("single-equality ExistsRel still resolves the inherited parent's row");
    assert_eq!(
        row_ids(&rows),
        vec![allowed],
        "the full witness authorizes only the resource linked to its parent, not the unwitnessed sibling"
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
