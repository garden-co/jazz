//! Prepared reads, plan installation, runtime invalidation, and ordered snapshots.

use super::*;

fn joined_issue_query() -> Query {
    Query::from("issues").join_via("issue_tags", "issue", [eq(col("tag"), lit("prepared"))])
}

#[test]
fn prepared_query_discards_graph_handle_when_runtime_changes() {
    let schema = issue_schema();
    let db = open_db(0xb7, AuthorId::SYSTEM, &schema);
    let prepared = db.prepare_query(&joined_issue_query()).unwrap();
    let runtime_token = db.node.node.borrow().groove_runtime_token();
    assert!(
        prepared
            .plan_for_tier(DurabilityTier::Local, runtime_token)
            .is_some()
    );
    assert!(
        prepared
            .plan_for_tier(DurabilityTier::Local, runtime_token.wrapping_add(1))
            .is_none()
    );
}

fn seed_issue_project(db: &Db<RocksDbStorage>, author: AuthorId) {
    db.seed_settled_mergeable_for_bootstrap(
        "projects",
        row(10),
        author,
        BTreeMap::from([("name".to_owned(), Value::String("Platform".to_owned()))]),
    )
    .unwrap();
    db.seed_settled_mergeable_for_bootstrap(
        "issues",
        row(1),
        author,
        issue_cells("Platform", "open", author, row(10), 5, &["api"], None),
    )
    .unwrap();
    db.seed_settled_mergeable_for_bootstrap(
        "issue_tags",
        row(20),
        author,
        BTreeMap::from([
            ("issue".to_owned(), Value::Uuid(row(1).0)),
            ("tag".to_owned(), Value::String("prepared".to_owned())),
        ]),
    )
    .unwrap();
}

#[test]
fn prepared_current_write_query_installs_and_reads_non_simple_plan() {
    let schema = issue_schema();
    let author = AuthorId::from_bytes([0xa1; 16]);
    let db = open_db(0xa1, author, &schema);
    seed_issue_project(&db, author);

    let prepared = db.prepare_query(&joined_issue_query()).unwrap();
    assert!(prepared.has_plan_for_tier(DurabilityTier::Local));
    assert!(prepared.has_plan_for_tier(DurabilityTier::Global));
    db.node
        .node
        .borrow_mut()
        .clear_prepared_query_plan_cache_for_test();

    let rows = db.read(&prepared).unwrap();

    assert_eq!(row_ids(&rows), vec![row(1)]);
    assert!(
        db.node
            .node
            .borrow()
            .prepared_query_plan_cache_is_empty_for_test(),
        "stored prepared plans should be used without replanning"
    );
}

#[test]
fn local_subscribe_uses_prepared_non_simple_plan() {
    let schema = issue_schema();
    let author = AuthorId::from_bytes([0xa1; 16]);
    let db = open_db(0xa2, author, &schema);
    seed_issue_project(&db, author);

    let prepared = db.prepare_query(&joined_issue_query()).unwrap();
    db.node
        .node
        .borrow_mut()
        .clear_prepared_query_plan_cache_for_test();

    let mut subscription = block_on(db.subscribe(
        &prepared,
        ReadOpts {
            tier: DurabilityTier::Local,
            local_updates: LocalUpdates::Deferred,
            propagation: Propagation::LocalOnly,
            include_deleted: false,
            ..ReadOpts::default()
        },
    ))
    .unwrap();

    assert_eq!(
        row_ids(&opened_rows(block_on(subscription.next_raw()).unwrap())),
        vec![row(1)]
    );
    assert!(
        db.node
            .node
            .borrow()
            .prepared_query_plan_cache_is_empty_for_test(),
        "initial subscribe read should consume the stored prepared plan"
    );
}

#[test]
fn subscription_reset_preserves_ordered_window_rank() {
    let schema = schema();
    let author = AuthorId::from_bytes([0xa1; 16]);
    let db = open_db(0xa3, author, &schema);
    for (id, title) in [(4, "alpha"), (1, "bravo"), (3, "charlie"), (2, "delta")] {
        db.seed_settled_mergeable_for_bootstrap(
            "todos",
            row(id),
            author,
            cells(title, false, author),
        )
        .unwrap();
    }

    let query = Query::from("todos")
        .order_by("title", OrderDirection::Asc)
        .offset(1)
        .limit(2);
    let mut subscription = prepared_subscribe(
        &db,
        &query,
        ReadOpts {
            tier: DurabilityTier::Local,
            local_updates: LocalUpdates::Deferred,
            propagation: Propagation::LocalOnly,
            include_deleted: false,
            ..ReadOpts::default()
        },
    )
    .unwrap();

    assert_eq!(
        row_ids(&opened_rows(block_on(subscription.next_raw()).unwrap())),
        vec![row(1), row(3)],
        "reset rows must retain the selected ordered window rather than member-key order"
    );
}

#[test]
fn simple_prepared_current_write_query_uses_lowered_plan() {
    let schema = schema();
    let author = AuthorId::from_bytes([0xa1; 16]);
    let db = open_db(0xa3, author, &schema);
    db.insert_with_id("todos", row(1), cells("simple", false, author))
        .unwrap();

    let prepared = db.prepare_query(&Query::from("todos")).unwrap();
    assert!(!prepared.has_plan_for_tier(DurabilityTier::Local));
    assert!(!prepared.has_plan_for_tier(DurabilityTier::Global));

    let rows = db.read(&prepared).unwrap();

    assert_eq!(row_ids(&rows), vec![row(1)]);
    assert!(
        db.node
            .node
            .borrow()
            .prepared_query_plan_cache_is_empty_for_test(),
        "simple prepared current reads should stay on the direct lowered path without installing a shared plan"
    );
}

#[test]
fn filtered_root_prepared_query_still_reads_without_preinstalled_plan() {
    let schema = schema();
    let author = AuthorId::from_bytes([0xa1; 16]);
    let db = open_db(0xa4, author, &schema);
    db.insert_with_id("todos", row(1), cells("wanted", false, author))
        .unwrap();

    let prepared = db
        .prepare_query(&Query::from("todos").filter(eq(col("title"), lit("wanted"))))
        .unwrap();
    assert!(!prepared.has_plan_for_tier(DurabilityTier::Local));
    assert_eq!(
        db.read(&prepared)
            .unwrap()
            .into_iter()
            .map(|row| row.row_uuid())
            .collect::<Vec<_>>(),
        vec![row(1)]
    );
}

#[test]
fn branch_read_view_relation_snapshot_uses_query_engine_relation_edges() {
    let schema = relation_schema();
    let db = open_db(0xc1, AuthorId::from_bytes([0xc1; 16]), &schema);
    let branch = BranchId(uuid::Uuid::from_bytes([0x42; 16]));
    let root_versions = [
        MergeableCommit::new("users", row(0xa1), 10).cells(BTreeMap::from([(
            "name".to_owned(),
            Value::String("alice".to_owned()),
        )])),
        MergeableCommit::new("todos", row(0x11), 11).cells(BTreeMap::from([
            ("title".to_owned(), Value::String("root todo".to_owned())),
            ("owner_id".to_owned(), Value::Uuid(row(0xa1).0)),
        ])),
    ];
    for (sequence, commit) in root_versions.into_iter().enumerate() {
        let tx = db
            .node
            .node
            .borrow_mut()
            .commit_mergeable(commit)
            .expect("commit root relation row");
        db.node
            .node
            .borrow_mut()
            .apply_fate_update(
                tx,
                Fate::Accepted,
                Some(GlobalSeq(sequence as u64 + 1)),
                Some(DurabilityTier::Global),
            )
            .expect("globally accept root relation row");
    }
    db.node
        .node
        .borrow_mut()
        .create_branch(branch)
        .expect("create branch");
    db.node
        .node
        .borrow_mut()
        .commit_mergeable_on_branch(
            branch,
            MergeableCommit::new("users", row(0xa1), 10).cells(BTreeMap::from([(
                "name".to_owned(),
                Value::String("branch alice".to_owned()),
            )])),
        )
        .expect("replace only the source row in the branch overlay");

    let query = Query::from("users").array_subquery(ArraySubquery::new(
        "todosViaOwner",
        "todos",
        "owner_id",
        "id",
    ));
    let prepared_query = prepared(&db, &query);
    let snapshot =
        doctest_support::block_on(db.all_relation_snapshot(&prepared_query, branch_read_opts()))
            .unwrap();

    assert_eq!(row_ids(&snapshot.rows), vec![row(0xa1)]);
    assert!(snapshot.edges.is_empty());
    assert_eq!(
        terminal_nested_text_values(&snapshot, row(0xa1), "todosViaOwner", "title"),
        vec!["root todo".to_owned()]
    );
    let binding = prepared_query
        .shape()
        .bind(BTreeMap::new())
        .expect("bind relation snapshot shape");
    let discriminators = db
        .node
        .node
        .borrow_mut()
        .query_relation_branch_discriminators_for_test(
            prepared_query.shape(),
            &binding,
            DurabilityTier::Local,
            db.identity.author,
            &branch_read_opts().read_view,
        )
        .expect("inspect production relation-edge witnesses");
    assert_eq!(
        discriminators,
        vec![(Some(branch.0), None)],
        "the overlay source keeps its branch witness while the frozen target retains root lineage"
    );
}

#[test]
fn relation_query_one_shot_hop_uses_unified_query_path() {
    let schema = relation_schema();
    let db = open_db(0xc1, AuthorId::from_bytes([0xc1; 16]), &schema);
    db.insert_with_id(
        "users",
        row(0xa1),
        BTreeMap::from([("name".to_owned(), Value::String("alice".to_owned()))]),
    )
    .unwrap();
    db.insert_with_id(
        "users",
        row(0xb1),
        BTreeMap::from([("name".to_owned(), Value::String("bob".to_owned()))]),
    )
    .unwrap();
    db.insert_with_id(
        "todos",
        row(0x11),
        BTreeMap::from([
            ("title".to_owned(), Value::String("alice todo".to_owned())),
            ("owner_id".to_owned(), Value::Uuid(row(0xa1).0)),
        ]),
    )
    .unwrap();
    db.insert_with_id(
        "todos",
        row(0x22),
        BTreeMap::from([
            ("title".to_owned(), Value::String("bob todo".to_owned())),
            ("owner_id".to_owned(), Value::Uuid(row(0xb1).0)),
        ]),
    )
    .unwrap();

    let query = RelationQuery {
        rel: RelationExpr::Project {
            input: Box::new(RelationExpr::Join {
                left: Box::new(RelationExpr::Filter {
                    input: Box::new(RelationExpr::TableScan {
                        table: "users".to_owned(),
                        alias: None,
                    }),
                    predicate: RelationPredicate::Cmp {
                        left: RelationColumnRef {
                            scope: Some("users".to_owned()),
                            column: "name".to_owned(),
                        },
                        op: RelationCmpOp::Eq,
                        right: RelationValueRef::Literal(serde_json::Value::String(
                            "alice".to_owned(),
                        )),
                    },
                }),
                right: Box::new(RelationExpr::TableScan {
                    table: "todos".to_owned(),
                    alias: Some("__hop_0".to_owned()),
                }),
                on: vec![crate::query::RelationJoinCondition {
                    left: RelationColumnRef {
                        scope: Some("users".to_owned()),
                        column: "id".to_owned(),
                    },
                    right: RelationColumnRef {
                        scope: Some("__hop_0".to_owned()),
                        column: "owner_id".to_owned(),
                    },
                }],
                join_kind: RelationJoinKind::Inner,
            }),
            columns: vec![
                crate::query::RelationProjectColumn {
                    alias: "id".to_owned(),
                    expr: RelationProjectExpr::RowId(RelationRowIdRef::Current),
                },
                crate::query::RelationProjectColumn {
                    alias: "title".to_owned(),
                    expr: RelationProjectExpr::Column(RelationColumnRef {
                        scope: Some("__hop_0".to_owned()),
                        column: "title".to_owned(),
                    }),
                },
                crate::query::RelationProjectColumn {
                    alias: "owner_id".to_owned(),
                    expr: RelationProjectExpr::Column(RelationColumnRef {
                        scope: Some("__hop_0".to_owned()),
                        column: "owner_id".to_owned(),
                    }),
                },
            ],
        },
    };

    let snapshot = block_on(db.all_relation_query(&query, ReadOpts::default())).unwrap();
    assert_eq!(row_ids(&snapshot.rows), vec![row(0x11)]);
}

#[test]
fn relation_query_one_shot_hop_accepts_runtime_uuid_literal_filter() {
    let schema = relation_schema();
    let db = open_db(0xc1, AuthorId::from_bytes([0xc1; 16]), &schema);
    db.insert_with_id(
        "users",
        row(0xa1),
        BTreeMap::from([("name".to_owned(), Value::String("alice".to_owned()))]),
    )
    .unwrap();
    db.insert_with_id(
        "users",
        row(0xb1),
        BTreeMap::from([("name".to_owned(), Value::String("bob".to_owned()))]),
    )
    .unwrap();
    db.insert_with_id(
        "todos",
        row(0x11),
        BTreeMap::from([
            ("title".to_owned(), Value::String("alice todo".to_owned())),
            ("owner_id".to_owned(), Value::Uuid(row(0xa1).0)),
        ]),
    )
    .unwrap();
    db.insert_with_id(
        "todos",
        row(0x22),
        BTreeMap::from([
            ("title".to_owned(), Value::String("bob todo".to_owned())),
            ("owner_id".to_owned(), Value::Uuid(row(0xb1).0)),
        ]),
    )
    .unwrap();

    let query = RelationQuery {
        rel: RelationExpr::Project {
            input: Box::new(RelationExpr::Join {
                left: Box::new(RelationExpr::Filter {
                    input: Box::new(RelationExpr::TableScan {
                        table: "users".to_owned(),
                        alias: None,
                    }),
                    predicate: RelationPredicate::Cmp {
                        left: RelationColumnRef {
                            scope: Some("users".to_owned()),
                            column: "id".to_owned(),
                        },
                        op: RelationCmpOp::Eq,
                        right: RelationValueRef::Literal(serde_json::json!({
                            "type": "Uuid",
                            "value": row(0xa1).0.to_string(),
                        })),
                    },
                }),
                right: Box::new(RelationExpr::TableScan {
                    table: "todos".to_owned(),
                    alias: Some("__hop_0".to_owned()),
                }),
                on: vec![crate::query::RelationJoinCondition {
                    left: RelationColumnRef {
                        scope: Some("users".to_owned()),
                        column: "id".to_owned(),
                    },
                    right: RelationColumnRef {
                        scope: Some("__hop_0".to_owned()),
                        column: "owner_id".to_owned(),
                    },
                }],
                join_kind: RelationJoinKind::Inner,
            }),
            columns: vec![
                crate::query::RelationProjectColumn {
                    alias: "id".to_owned(),
                    expr: RelationProjectExpr::Column(RelationColumnRef {
                        scope: Some("__hop_0".to_owned()),
                        column: "id".to_owned(),
                    }),
                },
                crate::query::RelationProjectColumn {
                    alias: "title".to_owned(),
                    expr: RelationProjectExpr::Column(RelationColumnRef {
                        scope: Some("__hop_0".to_owned()),
                        column: "title".to_owned(),
                    }),
                },
                crate::query::RelationProjectColumn {
                    alias: "owner_id".to_owned(),
                    expr: RelationProjectExpr::Column(RelationColumnRef {
                        scope: Some("__hop_0".to_owned()),
                        column: "owner_id".to_owned(),
                    }),
                },
            ],
        },
    };

    let snapshot = block_on(db.all_relation_query(&query, ReadOpts::default())).unwrap();
    assert_eq!(row_ids(&snapshot.rows), vec![row(0x11)]);
}

#[test]
fn relation_query_one_shot_multi_hop_scalar_fk_uses_nested_join_path() {
    let schema = relation_hop_schema();
    let db = open_db(0xc1, AuthorId::from_bytes([0xc1; 16]), &schema);
    db.insert_with_id(
        "orgs",
        row(0x01),
        BTreeMap::from([("name".to_owned(), Value::String("Org A".to_owned()))]),
    )
    .unwrap();
    db.insert_with_id(
        "orgs",
        row(0x02),
        BTreeMap::from([("name".to_owned(), Value::String("Org B".to_owned()))]),
    )
    .unwrap();
    db.insert_with_id(
        "teams",
        row(0x11),
        BTreeMap::from([
            ("name".to_owned(), Value::String("Team A".to_owned())),
            (
                "org_id".to_owned(),
                Value::Nullable(Some(Box::new(Value::Uuid(row(0x01).0)))),
            ),
        ]),
    )
    .unwrap();
    db.insert_with_id(
        "users",
        row(0x21),
        BTreeMap::from([
            ("name".to_owned(), Value::String("User A".to_owned())),
            (
                "team_id".to_owned(),
                Value::Nullable(Some(Box::new(Value::Uuid(row(0x11).0)))),
            ),
        ]),
    )
    .unwrap();

    let query = users_to_orgs_relation_query();

    let snapshot = block_on(db.all_relation_query(&query, ReadOpts::default())).unwrap();
    assert_eq!(row_ids(&snapshot.rows), vec![row(0x01)]);
}

#[test]
fn relation_query_subscription_hop_uses_unified_query_path() {
    let schema = relation_schema();
    let db = open_db(0xc1, AuthorId::from_bytes([0xc1; 16]), &schema);
    db.insert_with_id(
        "users",
        row(0xa1),
        BTreeMap::from([("name".to_owned(), Value::String("alice".to_owned()))]),
    )
    .unwrap();
    db.insert_with_id(
        "todos",
        row(0x11),
        BTreeMap::from([
            ("title".to_owned(), Value::String("alice todo".to_owned())),
            ("owner_id".to_owned(), Value::Uuid(row(0xa1).0)),
        ]),
    )
    .unwrap();

    let query = RelationQuery {
        rel: RelationExpr::Project {
            input: Box::new(RelationExpr::Join {
                left: Box::new(RelationExpr::TableScan {
                    table: "users".to_owned(),
                    alias: None,
                }),
                right: Box::new(RelationExpr::TableScan {
                    table: "todos".to_owned(),
                    alias: Some("__hop_0".to_owned()),
                }),
                on: vec![crate::query::RelationJoinCondition {
                    left: RelationColumnRef {
                        scope: Some("users".to_owned()),
                        column: "id".to_owned(),
                    },
                    right: RelationColumnRef {
                        scope: Some("__hop_0".to_owned()),
                        column: "owner_id".to_owned(),
                    },
                }],
                join_kind: RelationJoinKind::Inner,
            }),
            columns: vec![
                crate::query::RelationProjectColumn {
                    alias: "id".to_owned(),
                    expr: RelationProjectExpr::RowId(RelationRowIdRef::Current),
                },
                crate::query::RelationProjectColumn {
                    alias: "title".to_owned(),
                    expr: RelationProjectExpr::Column(RelationColumnRef {
                        scope: Some("__hop_0".to_owned()),
                        column: "title".to_owned(),
                    }),
                },
                crate::query::RelationProjectColumn {
                    alias: "owner_id".to_owned(),
                    expr: RelationProjectExpr::Column(RelationColumnRef {
                        scope: Some("__hop_0".to_owned()),
                        column: "owner_id".to_owned(),
                    }),
                },
            ],
        },
    };

    let mut stream = block_on(db.subscribe_relation_query(&query, ReadOpts::default())).unwrap();
    let opened = opened_rows(stream.try_next_event().expect("opened event"));
    assert_eq!(row_ids(&opened), vec![row(0x11)]);
}

#[test]
fn relation_query_subscription_hop_preserves_projected_self_reference_cells() {
    let schema = relation_hop_schema();
    let db = open_db(0xc1, AuthorId::from_bytes([0xc1; 16]), &schema);
    let parent = row(0x10);
    let team = row(0x11);
    let user = row(0x21);
    db.insert_with_id(
        "teams",
        parent,
        BTreeMap::from([("name".to_owned(), Value::String("Parent".to_owned()))]),
    )
    .unwrap();
    db.insert_with_id(
        "teams",
        team,
        BTreeMap::from([
            ("name".to_owned(), Value::String("Team A".to_owned())),
            (
                "parent_id".to_owned(),
                Value::Nullable(Some(Box::new(Value::Uuid(parent.0)))),
            ),
        ]),
    )
    .unwrap();
    db.insert_with_id(
        "users",
        user,
        BTreeMap::from([
            ("name".to_owned(), Value::String("User A".to_owned())),
            (
                "team_id".to_owned(),
                Value::Nullable(Some(Box::new(Value::Uuid(team.0)))),
            ),
        ]),
    )
    .unwrap();

    let query = users_to_teams_relation_query();
    let snapshot = block_on(db.all_relation_query(&query, ReadOpts::default())).unwrap();
    assert_eq!(row_ids(&snapshot.rows), vec![team]);
    assert_eq!(
        snapshot.rows[0].cell(&schema.tables[1], "name"),
        Some(Value::String("Team A".to_owned()))
    );
    assert_eq!(
        snapshot.rows[0].cell(&schema.tables[1], "parent_id"),
        Some(Value::Nullable(Some(Box::new(Value::Uuid(parent.0)))))
    );

    let mut stream = block_on(db.subscribe_relation_query(&query, ReadOpts::default())).unwrap();
    let opened = opened_rows(stream.try_next_event().expect("opened event"));
    let opened_team = opened
        .iter()
        .find(|row| row.row_uuid() == team)
        .expect("joined team row");
    assert_eq!(
        opened_team.cell(&schema.tables[1], "name"),
        Some(Value::String("Team A".to_owned()))
    );
    assert_eq!(
        opened_team.cell(&schema.tables[1], "parent_id"),
        Some(Value::Nullable(Some(Box::new(Value::Uuid(parent.0)))))
    );

    db.update(
        "teams",
        team,
        BTreeMap::from([("name".to_owned(), Value::String("Team B".to_owned()))]),
    )
    .unwrap();
    let (_, changed, removed) = delta_rows(stream.try_next_event().expect("updated event"));
    assert!(removed.is_empty());
    assert_eq!(row_ids(&changed), vec![team]);
    assert_eq!(
        changed[0].cell(&schema.tables[1], "name"),
        Some(Value::String("Team B".to_owned()))
    );
    assert_eq!(
        changed[0].cell(&schema.tables[1], "parent_id"),
        Some(Value::Nullable(Some(Box::new(Value::Uuid(parent.0)))))
    );
}

fn users_to_teams_relation_query() -> RelationQuery {
    RelationQuery {
        rel: RelationExpr::Project {
            input: Box::new(RelationExpr::Join {
                left: Box::new(RelationExpr::Filter {
                    input: Box::new(RelationExpr::TableScan {
                        table: "users".to_owned(),
                        alias: None,
                    }),
                    predicate: RelationPredicate::Cmp {
                        left: RelationColumnRef {
                            scope: Some("users".to_owned()),
                            column: "name".to_owned(),
                        },
                        op: RelationCmpOp::Eq,
                        right: RelationValueRef::Literal(serde_json::Value::String(
                            "User A".to_owned(),
                        )),
                    },
                }),
                right: Box::new(RelationExpr::TableScan {
                    table: "teams".to_owned(),
                    alias: Some("__hop_0".to_owned()),
                }),
                on: vec![crate::query::RelationJoinCondition {
                    left: RelationColumnRef {
                        scope: Some("users".to_owned()),
                        column: "team_id".to_owned(),
                    },
                    right: RelationColumnRef {
                        scope: Some("__hop_0".to_owned()),
                        column: "id".to_owned(),
                    },
                }],
                join_kind: RelationJoinKind::Inner,
            }),
            columns: vec![
                crate::query::RelationProjectColumn {
                    alias: "id".to_owned(),
                    expr: RelationProjectExpr::Column(RelationColumnRef {
                        scope: Some("__hop_0".to_owned()),
                        column: "id".to_owned(),
                    }),
                },
                crate::query::RelationProjectColumn {
                    alias: "name".to_owned(),
                    expr: RelationProjectExpr::Column(RelationColumnRef {
                        scope: Some("__hop_0".to_owned()),
                        column: "name".to_owned(),
                    }),
                },
                crate::query::RelationProjectColumn {
                    alias: "parent_id".to_owned(),
                    expr: RelationProjectExpr::Column(RelationColumnRef {
                        scope: Some("__hop_0".to_owned()),
                        column: "parent_id".to_owned(),
                    }),
                },
            ],
        },
    }
}

#[test]
fn relation_query_subscription_multi_hop_scalar_fk_uses_nested_join_path() {
    let schema = relation_hop_schema();
    let db = open_db(0xc1, AuthorId::from_bytes([0xc1; 16]), &schema);
    let query = users_to_orgs_relation_query();
    let mut stream = block_on(db.subscribe_relation_query(&query, ReadOpts::default())).unwrap();
    assert!(opened_rows(stream.try_next_event().expect("opened event")).is_empty());

    db.insert_with_id(
        "orgs",
        row(0x01),
        BTreeMap::from([("name".to_owned(), Value::String("Org A".to_owned()))]),
    )
    .unwrap();
    db.insert_with_id(
        "teams",
        row(0x11),
        BTreeMap::from([
            ("name".to_owned(), Value::String("Team A".to_owned())),
            (
                "org_id".to_owned(),
                Value::Nullable(Some(Box::new(Value::Uuid(row(0x01).0)))),
            ),
        ]),
    )
    .unwrap();
    db.insert_with_id(
        "users",
        row(0x21),
        BTreeMap::from([
            ("name".to_owned(), Value::String("User A".to_owned())),
            (
                "team_id".to_owned(),
                Value::Nullable(Some(Box::new(Value::Uuid(row(0x11).0)))),
            ),
        ]),
    )
    .unwrap();

    let opened = opened_rows(stream.try_next_event().expect("opened event"));
    assert_eq!(row_ids(&opened), vec![row(0x01)]);
}

fn users_to_orgs_relation_query() -> RelationQuery {
    RelationQuery {
        rel: RelationExpr::Project {
            input: Box::new(RelationExpr::Join {
                left: Box::new(RelationExpr::Join {
                    left: Box::new(RelationExpr::TableScan {
                        table: "users".to_owned(),
                        alias: None,
                    }),
                    right: Box::new(RelationExpr::TableScan {
                        table: "teams".to_owned(),
                        alias: Some("__hop_0".to_owned()),
                    }),
                    on: vec![crate::query::RelationJoinCondition {
                        left: RelationColumnRef {
                            scope: Some("users".to_owned()),
                            column: "team_id".to_owned(),
                        },
                        right: RelationColumnRef {
                            scope: Some("__hop_0".to_owned()),
                            column: "id".to_owned(),
                        },
                    }],
                    join_kind: RelationJoinKind::Inner,
                }),
                right: Box::new(RelationExpr::TableScan {
                    table: "orgs".to_owned(),
                    alias: Some("__hop_1".to_owned()),
                }),
                on: vec![crate::query::RelationJoinCondition {
                    left: RelationColumnRef {
                        scope: Some("__hop_0".to_owned()),
                        column: "org_id".to_owned(),
                    },
                    right: RelationColumnRef {
                        scope: Some("__hop_1".to_owned()),
                        column: "id".to_owned(),
                    },
                }],
                join_kind: RelationJoinKind::Inner,
            }),
            columns: vec![
                crate::query::RelationProjectColumn {
                    alias: "id".to_owned(),
                    expr: RelationProjectExpr::Column(RelationColumnRef {
                        scope: Some("__hop_1".to_owned()),
                        column: "id".to_owned(),
                    }),
                },
                crate::query::RelationProjectColumn {
                    alias: "name".to_owned(),
                    expr: RelationProjectExpr::Column(RelationColumnRef {
                        scope: Some("__hop_1".to_owned()),
                        column: "name".to_owned(),
                    }),
                },
            ],
        },
    }
}

#[test]
fn relation_query_gather_uses_unified_reachable_lowering_for_reads_and_subscriptions() {
    // This is an integration-level facade test: the public relation-query read
    // and subscription APIs must both use the same maintained reachability
    // program for the canonical gather IR emitted by the TypeScript builder.
    let schema = build_public_db_test_schema(
        PublicSchemaBuilder::new().table(
            PublicTableSchemaBuilder::new("teams")
                .column("name", PublicColumnType::Text)
                .nullable_fk_column("parent_id", "teams"),
        ),
    );
    let db = open_db(0xc1, AuthorId::from_bytes([0xc1; 16]), &schema);
    let query = teams_gather_relation_query();
    let mut stream = block_on(db.subscribe_relation_query(&query, ReadOpts::default())).unwrap();
    assert!(opened_rows(stream.try_next_event().expect("opened event")).is_empty());

    let root = row(0x01);
    let middle = row(0x02);
    let leaf = row(0x03);
    db.insert_with_id(
        "teams",
        root,
        BTreeMap::from([("name".to_owned(), Value::String("root".to_owned()))]),
    )
    .unwrap();
    db.insert_with_id(
        "teams",
        middle,
        BTreeMap::from([
            ("name".to_owned(), Value::String("middle".to_owned())),
            (
                "parent_id".to_owned(),
                Value::Nullable(Some(Box::new(Value::Uuid(root.0)))),
            ),
        ]),
    )
    .unwrap();
    db.insert_with_id(
        "teams",
        leaf,
        BTreeMap::from([
            ("name".to_owned(), Value::String("leaf".to_owned())),
            (
                "parent_id".to_owned(),
                Value::Nullable(Some(Box::new(Value::Uuid(middle.0)))),
            ),
        ]),
    )
    .unwrap();

    let changed = opened_rows(stream.try_next_event().expect("gathered rows event"));
    assert_eq!(
        row_ids(&changed).into_iter().collect::<BTreeSet<_>>(),
        BTreeSet::from([root, middle, leaf])
    );

    let snapshot = block_on(db.all_relation_query(&query, ReadOpts::default())).unwrap();
    assert_eq!(
        row_ids(&snapshot.rows).into_iter().collect::<BTreeSet<_>>(),
        BTreeSet::from([root, middle, leaf])
    );

    let filtered_query = RelationQuery {
        rel: RelationExpr::Filter {
            input: Box::new(query.rel.clone()),
            predicate: RelationPredicate::Cmp {
                left: RelationColumnRef {
                    scope: Some("teams".to_owned()),
                    column: "name".to_owned(),
                },
                op: RelationCmpOp::Ne,
                right: RelationValueRef::Literal(serde_json::Value::String("middle".to_owned())),
            },
        },
    };
    let filtered = block_on(db.all_relation_query(&filtered_query, ReadOpts::default())).unwrap();
    assert_eq!(
        row_ids(&filtered.rows).into_iter().collect::<BTreeSet<_>>(),
        BTreeSet::from([root, leaf])
    );

    let or_true = RelationQuery {
        rel: RelationExpr::Filter {
            input: Box::new(query.rel.clone()),
            predicate: RelationPredicate::Or(vec![
                RelationPredicate::True,
                RelationPredicate::False,
            ]),
        },
    };
    let unfiltered = block_on(db.all_relation_query(&or_true, ReadOpts::default())).unwrap();
    assert_eq!(
        row_ids(&unfiltered.rows)
            .into_iter()
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([root, middle, leaf])
    );

    let not_true = RelationQuery {
        rel: RelationExpr::Filter {
            input: Box::new(query.rel.clone()),
            predicate: RelationPredicate::Not(Box::new(RelationPredicate::True)),
        },
    };
    let empty = block_on(db.all_relation_query(&not_true, ReadOpts::default())).unwrap();
    assert!(empty.rows.is_empty());

    let filter_after_limit = RelationQuery {
        rel: RelationExpr::Filter {
            input: Box::new(RelationExpr::Limit {
                input: Box::new(RelationExpr::OrderBy {
                    input: Box::new(query.rel.clone()),
                    terms: vec![RelationOrderBy {
                        column: RelationColumnRef {
                            scope: Some("teams".to_owned()),
                            column: "name".to_owned(),
                        },
                        direction: OrderDirection::Asc,
                    }],
                }),
                limit: 1,
            }),
            predicate: RelationPredicate::Cmp {
                left: RelationColumnRef {
                    scope: Some("teams".to_owned()),
                    column: "name".to_owned(),
                },
                op: RelationCmpOp::Eq,
                right: RelationValueRef::Literal(serde_json::Value::String("root".to_owned())),
            },
        },
    };
    let error =
        block_on(db.all_relation_query(&filter_after_limit, ReadOpts::default())).unwrap_err();
    assert_eq!(error.code, ErrorCode::Query);
    assert!(
        error
            .message
            .contains("gather output filters cannot wrap limit or offset")
    );
}

fn teams_gather_relation_query() -> RelationQuery {
    RelationQuery {
        rel: RelationExpr::Gather {
            seed: Box::new(RelationExpr::Filter {
                input: Box::new(RelationExpr::TableScan {
                    table: "teams".to_owned(),
                    alias: None,
                }),
                predicate: RelationPredicate::Cmp {
                    left: RelationColumnRef {
                        scope: Some("teams".to_owned()),
                        column: "name".to_owned(),
                    },
                    op: RelationCmpOp::Eq,
                    right: RelationValueRef::Literal(serde_json::Value::String("leaf".to_owned())),
                },
            }),
            step: Box::new(RelationExpr::Project {
                input: Box::new(RelationExpr::Join {
                    left: Box::new(RelationExpr::Filter {
                        input: Box::new(RelationExpr::TableScan {
                            table: "teams".to_owned(),
                            alias: None,
                        }),
                        predicate: RelationPredicate::And(vec![RelationPredicate::Cmp {
                            left: RelationColumnRef {
                                scope: Some("teams".to_owned()),
                                column: "id".to_owned(),
                            },
                            op: RelationCmpOp::Eq,
                            right: RelationValueRef::RowId(RelationRowIdRef::Frontier),
                        }]),
                    }),
                    right: Box::new(RelationExpr::TableScan {
                        table: "teams".to_owned(),
                        alias: Some("__recursive_hop_0".to_owned()),
                    }),
                    on: vec![crate::query::RelationJoinCondition {
                        left: RelationColumnRef {
                            scope: Some("teams".to_owned()),
                            column: "parent_id".to_owned(),
                        },
                        right: RelationColumnRef {
                            scope: Some("__recursive_hop_0".to_owned()),
                            column: "id".to_owned(),
                        },
                    }],
                    join_kind: RelationJoinKind::Inner,
                }),
                columns: vec![crate::query::RelationProjectColumn {
                    alias: "id".to_owned(),
                    expr: RelationProjectExpr::Column(RelationColumnRef {
                        scope: Some("__recursive_hop_0".to_owned()),
                        column: "id".to_owned(),
                    }),
                }],
            }),
            frontier_key: crate::query::RelationKeyRef::RowId(RelationRowIdRef::Current),
            bound: crate::query::RecursionBound::MaxDepth(10),
            dedupe_key: vec![crate::query::RelationKeyRef::RowId(
                RelationRowIdRef::Current,
            )],
        },
    }
}

#[test]
fn relation_snapshot_reverse_array_skips_deleted_children() {
    let schema = relation_schema();
    let db = open_db(0xc1, AuthorId::from_bytes([0xc1; 16]), &schema);
    db.insert_with_id(
        "users",
        row(0xa1),
        BTreeMap::from([("name".to_owned(), Value::String("alice".to_owned()))]),
    )
    .unwrap();
    db.insert_with_id(
        "todos",
        row(0x11),
        BTreeMap::from([
            ("title".to_owned(), Value::String("deleted todo".to_owned())),
            ("owner_id".to_owned(), Value::Uuid(row(0xa1).0)),
        ]),
    )
    .unwrap();
    db.insert_with_id(
        "todos",
        row(0x22),
        BTreeMap::from([
            ("title".to_owned(), Value::String("visible todo".to_owned())),
            ("owner_id".to_owned(), Value::Uuid(row(0xa1).0)),
        ]),
    )
    .unwrap();
    db.delete("todos", row(0x11)).unwrap();

    let query = Query::from("users")
        .filter(eq(col("id"), lit(Value::Uuid(row(0xa1).0))))
        .array_subquery(ArraySubquery::new(
            "todosViaOwner",
            "todos",
            "owner_id",
            "id",
        ))
        .limit(1);
    let prepared = db.prepare_query(&query).unwrap();
    let snapshot = block_on(db.all_relation_snapshot(&prepared, ReadOpts::default())).unwrap();
    assert_eq!(row_ids(&snapshot.rows), vec![row(0xa1)]);
    assert!(snapshot.edges.is_empty());
    assert_eq!(
        terminal_nested_text_values(&snapshot, row(0xa1), "todosViaOwner", "title"),
        vec!["visible todo".to_owned()]
    );
}

#[test]
fn maintained_subscription_with_two_reference_includes_opens_with_source_coverage() {
    let schema = access_edge_include_schema();
    let client_author = AuthorId::from_bytes([0xc1; 16]);
    let server = open_core(0xee, AuthorId::SYSTEM, &schema);
    server
        .insert_with_id(
            "teams",
            row(0xa1),
            BTreeMap::from([("name".to_owned(), Value::String("resource team".to_owned()))]),
        )
        .unwrap();
    server
        .insert_with_id(
            "teams",
            row(0xb1),
            BTreeMap::from([("name".to_owned(), Value::String("member team".to_owned()))]),
        )
        .unwrap();
    server
        .insert_with_id(
            "team_access_edges",
            row(0xc1),
            BTreeMap::from([
                ("resource_id".to_owned(), Value::Uuid(row(0xa1).0)),
                ("team_id".to_owned(), Value::Uuid(row(0xb1).0)),
            ]),
        )
        .unwrap();

    let query = Query::from("team_access_edges")
        .include("resource_id")
        .include("team_id");
    let shape = query.validate(&schema).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let subscription = SubscriptionKey {
        shape_id: shape.shape_id(),
        binding_id: binding.binding_id(),
        read_view: RegisterShapeOptions::default().read_view_key(),
    };

    let (mut client_transport, server_transport) = duplex();
    let subscriber = server.accept_subscriber(server_transport, client_author);
    client_transport
        .send(SyncMessage::RegisterShape {
            shape_id: shape.shape_id(),
            ast: ShapeAst::from_validated(&shape),
            opts: RegisterShapeOptions::default(),
        })
        .unwrap();
    client_transport
        .send(SyncMessage::Subscribe(Subscribe {
            shape_id: shape.shape_id(),
            subscription,
            values: Vec::new(),
            known_state: None,
        }))
        .unwrap();

    subscriber.borrow_mut().tick().unwrap();
    let message = try_recv_subscriber_payload(client_transport.as_mut())
        .expect("expected include subscription view update");
    let SyncMessage::ViewUpdate {
        subscription: served,
        result_member_adds,
        ..
    } = message
    else {
        panic!("expected include subscription view update, got {message:?}");
    };
    assert_eq!(served, subscription);
    let tables = result_member_adds
        .iter()
        .filter_map(|member| member.as_real_row().map(|row| row.table.as_str()))
        .collect::<Vec<_>>();
    assert_eq!(tables, vec!["team_access_edges", "teams", "teams"]);

    client_transport
        .send(SyncMessage::Unsubscribe { subscription })
        .unwrap();
    subscriber.borrow_mut().tick().unwrap();
    client_transport
        .send(SyncMessage::Subscribe(Subscribe {
            shape_id: shape.shape_id(),
            subscription,
            values: Vec::new(),
            known_state: None,
        }))
        .unwrap();

    subscriber.borrow_mut().tick().unwrap();
    let message = try_recv_subscriber_payload(client_transport.as_mut())
        .expect("expected reopened include subscription view update");
    let SyncMessage::ViewUpdate {
        subscription: served,
        result_member_adds,
        ..
    } = message
    else {
        panic!("expected reopened include subscription view update, got {message:?}");
    };
    assert_eq!(served, subscription);
    let tables = result_member_adds
        .iter()
        .filter_map(|member| member.as_real_row().map(|row| row.table.as_str()))
        .collect::<Vec<_>>();
    assert_eq!(tables, vec!["team_access_edges", "teams", "teams"]);
}

#[test]
fn relation_snapshot_reverse_array_skips_deleted_children_with_camel_case_ref() {
    let schema = build_public_db_test_schema(
        PublicSchemaBuilder::new()
            .table(PublicTableSchemaBuilder::new("users").column("name", PublicColumnType::Text))
            .table(
                PublicTableSchemaBuilder::new("todos")
                    .column("title", PublicColumnType::Text)
                    .column("done", PublicColumnType::Boolean)
                    .nullable_fk_column("ownerId", "users"),
            ),
    );
    let db = open_db(0xc1, AuthorId::from_bytes([0xc1; 16]), &schema);
    db.insert_with_id(
        "users",
        row(0xa1),
        BTreeMap::from([("name".to_owned(), Value::String("alice".to_owned()))]),
    )
    .unwrap();
    db.insert_with_id(
        "todos",
        row(0x11),
        BTreeMap::from([
            ("title".to_owned(), Value::String("deleted todo".to_owned())),
            ("done".to_owned(), Value::Bool(false)),
            (
                "ownerId".to_owned(),
                Value::Nullable(Some(Box::new(Value::Uuid(row(0xa1).0)))),
            ),
        ]),
    )
    .unwrap();
    db.insert_with_id(
        "todos",
        row(0x22),
        BTreeMap::from([
            ("title".to_owned(), Value::String("visible todo".to_owned())),
            ("done".to_owned(), Value::Bool(false)),
            (
                "ownerId".to_owned(),
                Value::Nullable(Some(Box::new(Value::Uuid(row(0xa1).0)))),
            ),
        ]),
    )
    .unwrap();
    let joined_before_delete = prepared_read(
        &db,
        &Query::from("users").join_via_column("todos", "ownerId", "id", []),
    );
    assert_eq!(row_ids(&joined_before_delete), vec![row(0xa1), row(0xa1)]);
    let occurrence = |joined| {
        OutputOccurrenceId::new(
            ObjectId::from_uuid(row(0xa1).0),
            [ObjectId::from_uuid(row(joined).0)],
        )
    };
    let joined_snapshot = RelationSnapshot {
        root_count: joined_before_delete.len(),
        rows: joined_before_delete.clone(),
        edges: Vec::new(),
    };
    assert!(subscription_outputs_with_occurrence_sidecar(&joined_snapshot, &[]).is_err());
    assert!(
        subscription_outputs_with_occurrence_sidecar(
            &joined_snapshot,
            &[occurrence(0x11), occurrence(0x11)],
        )
        .is_err()
    );
    assert!(
        subscription_outputs_with_occurrence_sidecar(
            &joined_snapshot,
            &[
                OutputOccurrenceId::single_source(ObjectId::from_uuid(row(0xbb).0)),
                occurrence(0x22),
            ],
        )
        .is_err()
    );
    let joined_query = Query::from("users").join_via_column("todos", "ownerId", "id", []);
    let prepared_join = prepared(&db, &joined_query);
    let mut subscription = block_on(db.subscribe(&prepared_join, ReadOpts::default())).unwrap();
    let SubscriptionEvent::Delta { added, .. } = block_on(subscription.next_raw()).unwrap() else {
        panic!("joined subscription must start with a delta");
    };
    assert_eq!(added.len(), 2);
    let occurrence_ids = added
        .iter()
        .map(|output| output.occurrence_id.clone())
        .collect::<BTreeSet<_>>();
    assert_eq!(occurrence_ids.len(), 2);
    assert_eq!(
        added
            .iter()
            .map(|output| output.occurrence_id.clone())
            .collect::<Vec<_>>(),
        vec![occurrence(0x11), occurrence(0x22)]
    );
    assert!(
        added
            .iter()
            .all(|output| output.occurrence_id.canonical_bytes().len() == 32)
    );
    db.delete("todos", row(0x11)).unwrap();
    db.tick().unwrap();
    let SubscriptionEvent::Delta { removed, .. } = block_on(subscription.next_raw()).unwrap()
    else {
        panic!("joined occurrence removal must emit a delta");
    };
    assert_eq!(removed.len(), 1);
    assert_eq!(removed[0].occurrence_id, occurrence(0x11));

    let joined = prepared_read(
        &db,
        &Query::from("users").join_via_column("todos", "ownerId", "id", []),
    );
    assert_eq!(row_ids(&joined), vec![row(0xa1)]);

    let query = Query::from("users")
        .filter(eq(col("id"), lit(Value::Uuid(row(0xa1).0))))
        .array_subquery(
            ArraySubquery::new("todosViaOwner", "todos", "ownerId", "id").select(["id"]),
        )
        .limit(1);
    let prepared = db.prepare_query(&query).unwrap();
    let snapshot = block_on(db.all_relation_snapshot(&prepared, ReadOpts::default())).unwrap();
    assert_eq!(row_ids(&snapshot.rows), vec![row(0xa1)]);
    assert!(snapshot.edges.is_empty());
    assert_eq!(
        terminal_nested_values(&snapshot, row(0xa1), "todosViaOwner", "row_uuid"),
        vec![Value::Uuid(row(0x22).0)]
    );
}

#[test]
fn relation_snapshot_reverse_array_reads_local_nullable_ref_child() {
    let schema = build_public_db_test_schema(
        PublicSchemaBuilder::new()
            .table(PublicTableSchemaBuilder::new("users").column("name", PublicColumnType::Text))
            .table(
                PublicTableSchemaBuilder::new("todos")
                    .column("title", PublicColumnType::Text)
                    .nullable_fk_column("ownerId", "users"),
            ),
    );
    let db = open_db(0xc1, AuthorId::from_bytes([0xc1; 16]), &schema);
    let user = db
        .insert(
            "users",
            BTreeMap::from([("name".to_owned(), Value::String("alice".to_owned()))]),
        )
        .unwrap()
        .row_uuid();
    let todo = db
        .insert(
            "todos",
            BTreeMap::from([
                ("title".to_owned(), Value::String("visible todo".to_owned())),
                (
                    "ownerId".to_owned(),
                    Value::Nullable(Some(Box::new(Value::Uuid(user.0)))),
                ),
            ]),
        )
        .unwrap()
        .row_uuid();

    let query = Query::from("users")
        .filter(eq(col("id"), lit(Value::Uuid(user.0))))
        .array_subquery(
            ArraySubquery::new("todosViaOwner", "todos", "ownerId", "id").select(["id"]),
        )
        .limit(1);
    let prepared = db.prepare_query(&query).unwrap();
    let snapshot = block_on(db.all_relation_snapshot(&prepared, ReadOpts::default())).unwrap();

    assert_eq!(row_ids(&snapshot.rows), vec![user]);
    assert!(snapshot.edges.is_empty());
    assert_eq!(
        terminal_nested_values(&snapshot, user, "todosViaOwner", "row_uuid"),
        vec![Value::Uuid(todo.0)]
    );
}

#[test]
fn relation_snapshot_reverse_array_limit_reads_local_child() {
    let schema = build_public_db_test_schema(
        PublicSchemaBuilder::new()
            .table(PublicTableSchemaBuilder::new("projects").column("name", PublicColumnType::Text))
            .table(
                PublicTableSchemaBuilder::new("todos")
                    .column("title", PublicColumnType::Text)
                    .fk_column("projectId", "projects"),
            ),
    );
    let db = open_db(0xc1, AuthorId::from_bytes([0xc1; 16]), &schema);
    let project = db
        .insert(
            "projects",
            BTreeMap::from([("name".to_owned(), Value::String("Announcements".to_owned()))]),
        )
        .unwrap()
        .row_uuid();
    let _todo = db
        .insert(
            "todos",
            BTreeMap::from([
                ("title".to_owned(), Value::String("visible todo".to_owned())),
                ("projectId".to_owned(), Value::Uuid(project.0)),
            ]),
        )
        .unwrap()
        .row_uuid();

    let query = Query::from("projects")
        .filter(eq(col("id"), lit(Value::Uuid(project.0))))
        .array_subquery(
            ArraySubquery::new("todosViaProject", "todos", "projectId", "id")
                .select(["title"])
                .limit(1),
        )
        .limit(1);
    let prepared = db.prepare_query(&query).unwrap();
    let snapshot = block_on(db.all_relation_snapshot(&prepared, ReadOpts::default())).unwrap();

    assert_eq!(row_ids(&snapshot.rows), vec![project]);
    assert!(snapshot.edges.is_empty());
    assert_eq!(
        terminal_nested_text_values(&snapshot, project, "todosViaProject", "title"),
        vec!["visible todo".to_owned()]
    );
}

#[test]
fn relation_snapshot_unordered_array_offset_uses_child_row_id_order() {
    let schema = relation_schema();
    let db = open_db(0xd4, AuthorId::from_bytes([0xd4; 16]), &schema);
    let parent = row(0x41);
    db.insert_with_id(
        "todos",
        parent,
        BTreeMap::from([
            ("title".to_owned(), Value::String("parent".to_owned())),
            ("owner_id".to_owned(), Value::Uuid(row(0xa1).0)),
        ]),
    )
    .unwrap();
    for id in [0xb1, 0xb2, 0xb3] {
        db.insert_with_id(
            "comments",
            row(id),
            BTreeMap::from([
                ("body".to_owned(), Value::String("tie".to_owned())),
                ("todo_id".to_owned(), Value::Uuid(parent.0)),
            ]),
        )
        .unwrap();
    }

    let query = Query::from("todos").array_subquery(
        ArraySubquery::new("comments", "comments", "todo_id", "id")
            .offset(1)
            .limit(1),
    );
    let prepared = db.prepare_query(&query).unwrap();
    let snapshot = block_on(db.all_relation_snapshot(&prepared, ReadOpts::default())).unwrap();

    assert!(snapshot.edges.is_empty());
    assert_eq!(
        terminal_nested_values(&snapshot, parent, "comments", "row_uuid"),
        vec![Value::Uuid(row(0xb2).0)]
    );
}

#[test]
fn relation_snapshot_reverse_array_projects_provenance_magic_columns() {
    let schema = build_public_db_test_schema(
        PublicSchemaBuilder::new()
            .table(PublicTableSchemaBuilder::new("projects").column("name", PublicColumnType::Text))
            .table(
                PublicTableSchemaBuilder::new("todos")
                    .column("title", PublicColumnType::Text)
                    .column("done", PublicColumnType::Boolean)
                    .column(
                        "tags",
                        PublicColumnType::Array {
                            element: Box::new(PublicColumnType::Text),
                        },
                    )
                    .fk_column("projectId", "projects")
                    .nullable_fk_column("ownerId", "users")
                    .array_fk_column("assigneesIds", "users"),
            )
            .table(PublicTableSchemaBuilder::new("users").column("name", PublicColumnType::Text)),
    );
    let db = open_db(0xc1, AuthorId::from_bytes([0xc1; 16]), &schema);
    db.insert_with_id(
        "projects",
        row(0xa1),
        BTreeMap::from([("name".to_owned(), Value::String("Announcements".to_owned()))]),
    )
    .unwrap();
    db.insert_with_id(
        "todos",
        row(0x22),
        BTreeMap::from([
            ("title".to_owned(), Value::String("Write tests".to_owned())),
            ("done".to_owned(), Value::Bool(false)),
            (
                "tags".to_owned(),
                Value::Array(vec![Value::String("dev".to_owned())]),
            ),
            ("projectId".to_owned(), Value::Uuid(row(0xa1).0)),
            ("ownerId".to_owned(), Value::Nullable(None)),
            ("assigneesIds".to_owned(), Value::Array(Vec::new())),
        ]),
    )
    .unwrap();

    let query = Query::from("projects")
        .filter(eq(col("id"), lit(Value::Uuid(row(0xa1).0))))
        .array_subquery(
            ArraySubquery::new("todosViaProject", "todos", "projectId", "id")
                .select([
                    "title",
                    "done",
                    "tags",
                    "projectId",
                    "ownerId",
                    "assigneesIds",
                    "$createdAt",
                    "$updatedAt",
                ])
                .limit(1),
        )
        .limit(1);
    let prepared = db.prepare_query(&query).unwrap();
    let snapshot = block_on(db.all_relation_snapshot(&prepared, ReadOpts::default())).unwrap();
    assert_eq!(row_ids(&snapshot.rows), vec![row(0xa1)]);
    assert!(snapshot.edges.is_empty());
    assert_eq!(
        terminal_nested_values(&snapshot, row(0xa1), "todosViaProject", "row_uuid"),
        vec![Value::Uuid(row(0x22).0)]
    );
    assert!(matches!(
        terminal_nested_values(&snapshot, row(0xa1), "todosViaProject", "$createdAt").as_slice(),
        [Value::U64(_)]
    ));
    assert!(matches!(
        terminal_nested_values(&snapshot, row(0xa1), "todosViaProject", "$updatedAt").as_slice(),
        [Value::U64(_)]
    ));
}

#[test]
fn version_bearing_current_source_preserves_provenance_timestamps() {
    let db = block_on(doctest_support::open_todos_db()).unwrap();
    let id = row(0x7a);
    db.insert_with_id_at_ms(
        "todos",
        id,
        doctest_support::todo_cells("provenance", false),
        1_234,
    )
    .unwrap();
    {
        let mut node = db.node.node.borrow_mut();
        let table = node.table("todos").unwrap().clone();
        let rows = node
            .test_content_current_with_version(&table, DurabilityTier::Local)
            .unwrap();
        let created_at = rows.descriptor.field_index("created_at").unwrap();
        let record = rows
            .iter()
            .find(|(record, weight)| *weight > 0 && record.get_uuid(0).unwrap() == id.0)
            .unwrap()
            .0;
        assert_eq!(record.get_u64(created_at).unwrap(), 1_234);
    }

    let query = db
        .table("todos")
        .select(["title", "$createdAt", "$updatedAt"])
        .filter(eq(col("id"), lit(Value::Uuid(id.0))));
    let prepared = db.prepare_query(&query).unwrap();
    let rows = block_on(db.all(&prepared, ReadOpts::default())).unwrap();
    let row = rows.iter().find(|row| row.row_uuid() == id).unwrap();
    assert_eq!(row.raw_field("$createdAt"), Some(Value::U64(1_234)));
    assert_eq!(row.raw_field("$updatedAt"), Some(Value::U64(1_234)));
    assert_eq!(row.raw_field("user_done"), None);
}

#[test]
fn db_at_reads_historical_cut_and_partial_requires_server() {
    let schema = schema();
    let author = AuthorId::from_bytes([0xa1; 16]);
    let core = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let partial = open_db(0xc1, author, &schema);
    let todo = row(0x42);

    core.insert_with_id("todos", todo, cells("draft", false, author))
        .unwrap();
    core.update(
        "todos",
        todo,
        BTreeMap::from([("title".to_owned(), Value::String("final".to_owned()))]),
    )
    .unwrap();

    let table = &schema.tables[0];
    let at_first = core.at(GlobalSeq(1), &Query::from("todos")).unwrap();
    assert_eq!(at_first.len(), 1);
    assert_eq!(
        at_first[0].cell(table, "title"),
        Some(Value::String("draft".to_owned()))
    );
    let at_second = core.at(GlobalSeq(2), &Query::from("todos")).unwrap();
    assert_eq!(
        at_second[0].cell(table, "title"),
        Some(Value::String("final".to_owned()))
    );

    let partial_todos = partial.prepare_query(&Query::from("todos")).unwrap();
    let err = partial.at(GlobalSeq(1), &partial_todos).unwrap_err();
    assert_eq!(err.code, ErrorCode::HistoricalReadRequiresServer);
    assert_eq!(err.message, "historical read requires server evaluation");
}

#[test]
fn db_query_builder_expresses_s1_shaped_filters_and_include_modes() {
    let schema = issue_schema();
    let dir = tempfile::tempdir().unwrap();
    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = RocksDbStorage::open(dir.path(), &refs).unwrap();
    let alice = AuthorId::from_bytes([0xa1; 16]);
    let bob = AuthorId::from_bytes([0xb2; 16]);
    let db = block_on(Db::open(DbConfig {
        schema: schema.clone(),
        storage,
        identity: DbIdentity {
            node: NodeUuid::from_bytes([0x22; 16]),
            author: alice,
        },
        id_source: Some(Box::new(SeededRowIdSource::new(0x22))),
    }))
    .unwrap();

    db.insert_with_id(
        "projects",
        row(10),
        BTreeMap::from([("name".to_owned(), Value::String("Platform".to_owned()))]),
    )
    .unwrap();
    db.insert_with_id(
        "issues",
        row(1),
        issue_cells(
            "ship api query builder",
            "open",
            alice,
            row(10),
            5,
            &["api", "platform"],
            None,
        ),
    )
    .unwrap();
    db.insert_with_id(
        "issues",
        row(2),
        issue_cells("closed work", "done", alice, row(10), 3, &["api"], Some(99)),
    )
    .unwrap();
    db.insert_with_id(
        "issues",
        row(3),
        issue_cells("someone else", "open", bob, row(10), 8, &["platform"], None),
    )
    .unwrap();
    db.insert_with_id(
        "issues",
        row(4),
        issue_cells("missing project", "open", alice, row(99), 6, &["api"], None),
    )
    .unwrap();

    let s1_query = db
        .table("issues")
        .filter(all_of([
            eq(col("assignee"), lit(alice.0)),
            in_list(col("state"), [lit("open"), lit("blocked")]),
            not(ne(col("state"), lit("open"))),
            any_of([
                contains(col("title"), lit("api")),
                contains(col("labels"), lit("api")),
            ]),
            gt(col("priority"), lit(4_u64)),
            lte(col("priority"), lit(6_u64)),
            is_null(col("snoozed_until")),
        ]))
        .include("project")
        .select([
            "title", "state", "assignee", "project", "priority", "labels",
        ])
        .limit(10)
        .offset(0);

    let table = schema
        .tables
        .iter()
        .find(|table| table.name == "issues")
        .unwrap();
    let read_rows = prepared_read(&db, &s1_query);
    assert_eq!(row_ids(&read_rows), vec![row(1)]);
    assert_eq!(
        read_rows[0].cell(table, "title"),
        Some(Value::String("ship api query builder".to_owned()))
    );
    assert_eq!(read_rows[0].cell(table, "snoozed_until"), None);
    let all_rows = prepared_all(&db, &s1_query, ReadOpts::default());
    assert_eq!(row_ids(&all_rows), vec![row(1)]);

    let holes_query = db
        .table("issues")
        .filter(eq(col("assignee"), lit(alice.0)))
        .filter(eq(col("state"), lit("open")))
        .include_with(Include::new("project").join_mode(JoinMode::Holes));
    assert_eq!(
        row_ids(&prepared_read(&db, &holes_query)),
        vec![row(1), row(4)]
    );

    let require_query = holes_query.clone().include_with(
        Include::new("project")
            .join_mode(JoinMode::Holes)
            .require_includes(),
    );
    assert_eq!(row_ids(&prepared_read(&db, &require_query)), vec![row(1)]);
    assert_eq!(
        row_ids(&prepared_all(&db, &require_query, ReadOpts::default())),
        vec![row(1)],
        "required scalar includes must retain public Root membership gating"
    );

    let paged = db
        .table("issues")
        .filter(eq(col("state"), lit("open")))
        .include_with(Include::new("project").join_mode(JoinMode::Holes))
        .offset(1)
        .limit(1);
    assert_eq!(row_ids(&prepared_read(&db, &paged)), vec![row(3)]);
}

#[test]
fn payload_enum_match_filters_one_shot_and_maintained_case_transitions() {
    let schema = payload_enum_query_schema();
    let db = open_db(0xe7, AuthorId::from_bytes([0xe7; 16]), &schema);
    let matching = row(0xe1);
    let other_case = row(0xe2);
    db.insert_with_id(
        "events",
        matching,
        BTreeMap::from([("event".to_owned(), payload_message(2))]),
    )
    .unwrap();
    db.insert_with_id(
        "events",
        other_case,
        BTreeMap::from([("event".to_owned(), payload_closed(2))]),
    )
    .unwrap();

    let query = Query::from("events").filter(Predicate::EnumMatch {
        column: "event".to_owned(),
        case: "message".to_owned(),
        payload: Box::new(Predicate::Eq(
            Operand::Column("level".to_owned()),
            Operand::Literal(Value::I32(2)),
        )),
    });
    assert_eq!(row_ids(&prepared_read(&db, &query)), vec![matching]);

    let prepared_query = prepared(&db, &query);
    let mut subscription = prepared_subscribe(&db, &query, ReadOpts::default()).unwrap();
    let initial = snapshot_from_event(block_on(subscription.next_raw()).unwrap());
    assert_eq!(row_ids(&initial.rows), vec![matching]);

    db.update(
        "events",
        matching,
        BTreeMap::from([("event".to_owned(), payload_closed(2))]),
    )
    .unwrap();
    let (added, updated, removed) = delta_rows(block_on(subscription.next_raw()).unwrap());
    assert!(added.is_empty());
    assert!(updated.is_empty());
    assert_eq!(
        removed
            .into_iter()
            .map(|row| row.row_uuid)
            .collect::<Vec<_>>(),
        vec![matching]
    );
    assert!(db.read(&prepared_query).unwrap().is_empty());

    db.update(
        "events",
        other_case,
        BTreeMap::from([("event".to_owned(), payload_message(2))]),
    )
    .unwrap();
    let (added, updated, removed) = delta_rows(block_on(subscription.next_raw()).unwrap());
    assert_eq!(row_ids(&added), vec![other_case]);
    assert!(updated.is_empty());
    assert!(removed.is_empty());
    assert_eq!(
        row_ids(&db.read(&prepared_query).unwrap()),
        vec![other_case]
    );
}

#[test]
fn projected_relation_edge_removal_removes_only_its_renamed_related_row() {
    let root = row(0x91);
    let target = row(0x92);
    let projected_edge = RelationEdge {
        source_table: "posts".to_owned(),
        source_row: root,
        relation: "author".to_owned(),
        target_table: "people".to_owned(),
        target_row: target,
    };
    let mut snapshot = RelationSnapshot {
        root_count: 1,
        rows: vec![
            relation_snapshot_row("posts", root),
            relation_snapshot_row("people", target),
            relation_snapshot_row("archived_people", target),
        ],
        edges: vec![projected_edge.clone()],
    };
    let mut index = RelationSnapshotIndex::from_snapshot(&snapshot);

    let event = apply_maintained_update_to_snapshot(
        &mut snapshot,
        &mut index,
        LocalMaintainedViewSubscriptionUpdate {
            authoritative_membership_changed: false,
            added: Vec::new(),
            removed: Vec::new(),
            added_edges: Vec::new(),
            removed_edges: vec![projected_edge],
            terminal_operations: Vec::new(),
            terminal_layout: None,
        },
        DurabilityTier::Edge,
        true,
        false,
    );

    assert!(matches!(event, SubscriptionEvent::Delta { .. }));
    assert!(snapshot.edges.is_empty());
    assert_eq!(snapshot.rows.len(), 2);
    assert!(
        snapshot
            .rows
            .iter()
            .any(|row| row.table() == "archived_people" && row.row_uuid() == target)
    );
    assert!(
        !snapshot
            .rows
            .iter()
            .any(|row| row.table() == "people" && row.row_uuid() == target)
    );
}

#[test]
fn client_read_advice_is_unknown_even_when_a_local_winner_exists() {
    let schema = owner_read_schema();
    let owner = AuthorId::from_bytes([0xa1; 16]);
    let other = AuthorId::from_bytes([0xb2; 16]);
    let core = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let row = row(1);
    let write = core
        .insert_with_id("todos", row, cells("private", false, owner))
        .unwrap();

    let owner_db = open_db(0xa1, owner, &schema);
    let other_db = open_db(0xb2, other, &schema);
    let unit = core
        .node()
        .borrow_mut()
        .commit_unit_for(write.mergeable_tx_id());
    let SyncMessage::CommitUnit { tx, versions } = unit.unwrap() else {
        panic!("commit unit expected");
    };
    owner_db
        .node
        .node
        .borrow_mut()
        .apply_sync_message(SyncMessage::CommitUnit {
            tx: tx.clone(),
            versions: versions.clone(),
        })
        .unwrap();
    other_db
        .node
        .node
        .borrow_mut()
        .apply_sync_message(SyncMessage::CommitUnit { tx, versions })
        .unwrap();

    assert_eq!(
        owner_db.can_read("todos", row).unwrap(),
        PermissionAdvice::Unknown
    );
    assert_eq!(
        other_db.can_read("todos", row).unwrap(),
        PermissionAdvice::Unknown
    );
    assert_eq!(
        owner_db
            .authorize_read_for_identity("todos", row, owner)
            .unwrap(),
        PermissionAdvice::Allowed,
    );
    assert_eq!(
        owner_db
            .authorize_read_for_identity("todos", row, other)
            .unwrap(),
        PermissionAdvice::Denied,
    );
}

#[test]
fn permission_introspection_magic_columns_fail_closed_on_prepare_query() {
    let db = doctest_support::block_on(doctest_support::open_todos_db()).unwrap();

    let query = db.table("todos").select(["$canRead"]);
    let error = expect_error(db.prepare_query(&query));
    assert_eq!(error.code, ErrorCode::Query);
    assert!(
        error.message.contains("unsupported")
            && error.message.contains("permission introspection")
            && error.message.contains("$canRead"),
        "unexpected error message: {}",
        error.message
    );

    let provenance_query = db.table("todos").select(["$createdAt", "$createdBy"]);
    db.prepare_query(&provenance_query).unwrap();
}

#[test]
fn read_opts_default_and_effective_tier_preserve_local_update_contract() {
    let opts = ReadOpts::default();
    assert_eq!(opts.tier, DurabilityTier::Local);
    assert_eq!(opts.local_updates, LocalUpdates::Immediate);
    assert_eq!(opts.propagation, Propagation::Full);

    assert_eq!(
        effective_read_tier(&ReadOpts {
            tier: DurabilityTier::None,
            local_updates: LocalUpdates::Immediate,
            propagation: Propagation::LocalOnly,
            include_deleted: false,
            ..ReadOpts::default()
        }),
        DurabilityTier::Local
    );
    assert_eq!(
        effective_read_tier(&ReadOpts {
            tier: DurabilityTier::Global,
            local_updates: LocalUpdates::Immediate,
            propagation: Propagation::LocalOnly,
            include_deleted: false,
            ..ReadOpts::default()
        }),
        DurabilityTier::Global
    );
    assert_eq!(
        effective_read_tier(&ReadOpts {
            tier: DurabilityTier::None,
            local_updates: LocalUpdates::Deferred,
            propagation: Propagation::Full,
            include_deleted: false,
            ..ReadOpts::default()
        }),
        DurabilityTier::None
    );
}

#[test]
fn single_branch_read_view_uses_query_engine_branch_source_for_one_shot_reads() {
    let db = doctest_support::block_on(doctest_support::open_todos_db()).unwrap();
    let branch = BranchId(uuid::Uuid::from_bytes([0x42; 16]));
    db.node
        .node
        .borrow_mut()
        .create_branch(branch)
        .expect("create branch");
    db.node
        .node
        .borrow_mut()
        .commit_mergeable_on_branch(
            branch,
            MergeableCommit::new("todos", row(0x42), 10)
                .cells(doctest_support::todo_cells("branch-only", false)),
        )
        .expect("commit branch row");
    let query = db.table("todos");
    let prepared_query = prepared(&db, &query);
    let opts = branch_read_opts();

    let rows = doctest_support::block_on(db.all(&prepared_query, opts.clone())).unwrap();
    assert_eq!(row_ids(&rows), vec![row(0x42)]);

    db.node
        .node
        .borrow_mut()
        .commit_mergeable_on_branch(
            branch,
            MergeableCommit::new("todos", row(0x42), 11).deletion(DeletionEvent::Deleted),
        )
        .expect("delete pending branch row");
    let deleted = doctest_support::block_on(db.all(&prepared_query, opts.clone())).unwrap();
    assert!(
        deleted.is_empty(),
        "Local branch reads apply pending deletion witnesses"
    );

    db.node
        .node
        .borrow_mut()
        .commit_mergeable_on_branch(
            branch,
            MergeableCommit::new("todos", row(0x42), 12).deletion(DeletionEvent::Restored),
        )
        .expect("restore pending branch row");
    let restored = doctest_support::block_on(db.all(&prepared_query, opts.clone())).unwrap();
    assert_eq!(
        row_ids(&restored),
        vec![row(0x42)],
        "Local branch reads apply pending restore witnesses"
    );

    let local_subscription_opts = ReadOpts {
        propagation: Propagation::LocalOnly,
        ..opts.clone()
    };
    let mut local_subscription =
        doctest_support::block_on(db.subscribe(&prepared_query, local_subscription_opts))
            .expect("subscribe to the locally reconstructed branch view");
    assert_eq!(
        row_ids(&opened_rows(
            block_on(local_subscription.next_raw()).unwrap()
        )),
        vec![row(0x42)]
    );

    let attachment = db
        .attach_query_with_opts(&prepared_query, opts.clone())
        .unwrap();
    db.detach_query(attachment);
    let attachment = db
        .attach_query_with_opts_for_identity(&prepared_query, opts.clone(), db.identity.author)
        .unwrap();
    db.detach_query(attachment);

    let snapshot =
        doctest_support::block_on(db.all_relation_snapshot(&prepared_query, opts.clone())).unwrap();
    assert_eq!(row_ids(&snapshot.rows), vec![row(0x42)]);
}

#[test]
fn edge_read_opts_and_wait_honor_edge_durability() {
    let db = doctest_support::block_on(doctest_support::open_todos_db()).unwrap();
    let write = db
        .insert("todos", doctest_support::todo_cells("edge observed", false))
        .unwrap();
    let query = db.table("todos");
    let prepared_query = prepared(&db, &query);

    assert_eq!(
        effective_read_tier(&ReadOpts {
            tier: DurabilityTier::Edge,
            local_updates: LocalUpdates::Immediate,
            propagation: Propagation::LocalOnly,
            include_deleted: false,
            ..ReadOpts::default()
        }),
        DurabilityTier::Edge
    );
    assert!(
        doctest_support::block_on(db.all_for_identity(
            &prepared_query,
            ReadOpts {
                tier: DurabilityTier::Edge,
                local_updates: LocalUpdates::Immediate,
                propagation: Propagation::LocalOnly,
                include_deleted: false,
                ..ReadOpts::default()
            },
            AuthorId::SYSTEM,
        ))
        .unwrap()
        .is_empty()
    );
    let not_observed = doctest_support::block_on(write.wait(DurabilityTier::Edge)).unwrap_err();
    assert_eq!(not_observed.code, ErrorCode::NotObserved);

    // E1: edge-accept produced directly; E2 wires the acceptance path.
    db.node
        .node
        .borrow_mut()
        .apply_fate_update(
            write.mergeable_tx_id(),
            Fate::Accepted,
            None,
            Some(DurabilityTier::Edge),
        )
        .unwrap();

    assert_eq!(
        doctest_support::block_on(write.wait(DurabilityTier::Edge)).unwrap(),
        write.mergeable_tx_id()
    );
    assert_eq!(
        row_ids(
            &doctest_support::block_on(db.all_for_identity(
                &prepared_query,
                ReadOpts {
                    tier: DurabilityTier::Edge,
                    local_updates: LocalUpdates::Immediate,
                    propagation: Propagation::LocalOnly,
                    include_deleted: false,
                    ..ReadOpts::default()
                },
                AuthorId::SYSTEM,
            ))
            .unwrap()
        ),
        vec![write.row_uuid()]
    );
}
