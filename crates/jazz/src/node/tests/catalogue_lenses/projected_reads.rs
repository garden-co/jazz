// Prepared, current, deleted, reachable, and historical schema-projected reads.

#[test]
fn heterogeneous_schema_projected_reads_keep_prepared_plans_valid() {
    let base = schema();
    let evolved = evolved_todos_name_body_schema();
    let evolved_payload = SchemaVersion::new(evolved.clone());
    let (_dir, mut core) = open_node_with_schema(node(0x49), base.clone());
    let shape = Query::from("todos")
        .filter(eq(col("title"), param("wanted")))
        .validate(&base)
        .unwrap();
    let binding = shape
        .bind(BTreeMap::from([(
            "wanted".to_owned(),
            Value::String("projected".to_owned()),
        )]))
        .unwrap();
    let pre_lens_plan = core
        .prepared_query_plan(&shape, &binding, DurabilityTier::Local, AuthorSubject::SYSTEM)
        .unwrap();
    publish_schema_lineage(
        &mut core,
        evolved_payload.clone(),
        MigrationLens::new(
            base.version_id(),
            evolved_payload.id,
            vec![TableLens {
                source_table: "todos".to_owned(),
                target_table: "todos".to_owned(),
                ops: vec![
                    LensOp::RenameColumn {
                        from: "title".to_owned(),
                        to: "name".to_owned(),
                    },
                    LensOp::AddColumn {
                        column: "body".to_owned(),
                        default: v("default-body"),
                    },
                ],
            }],
        ).expect("valid migration lens"),
        Vec::<String>::new(),
        Vec::<String>::new(),
    )
    .unwrap();
    core.apply_trusted_catalogue_message_settled(SyncMessage::SetCurrentWriteSchema {
        author: AuthorSubject::SYSTEM,
        pointer: CurrentWriteSchema {
            revision: 1,
            schema: evolved_payload.id,
        },
    })
    .unwrap();
    core.commit_mergeable_settled(
        MergeableCommit::new("todos", row(0x49), 10).cells(BTreeMap::from([
            ("name".to_owned(), v("projected")),
            ("body".to_owned(), v("partition")),
        ])),
    )
    .unwrap();

    let rows = core
        .query_rows(&shape, &binding, DurabilityTier::Local)
        .unwrap();

    assert_eq!(
        rows.into_iter()
            .map(current_row_pair)
            .collect::<BTreeMap<_, _>>(),
        BTreeMap::from([(row(0x49), title_cells("projected"))])
    );
    assert!(!core.uses_schema_projected_read(&shape));
    let rows = core
        .query_rows_local_preview(&shape, &binding, Some(&pre_lens_plan))
        .unwrap();
    assert_eq!(
        rows.into_iter()
            .map(current_row_pair)
            .collect::<BTreeMap<_, _>>(),
        BTreeMap::from([(row(0x49), title_cells("projected"))]),
        "a plan prepared before lens publication must accept projection cases registered by the lens"
    );

    let join_base = build_public_test_schema(
        PublicSchemaBuilder::new()
            .table(
                PublicTableSchemaBuilder::new("todos")
                    .column("title", PublicColumnType::Text),
            )
            .table(
                PublicTableSchemaBuilder::new("todo_members")
                    .fk_column("todo", "todos")
                    .column("member", PublicColumnType::Uuid),
            ),
    );
    let join_evolved = SchemaVersion::new(build_public_test_schema(
        PublicSchemaBuilder::new()
            .table(
                PublicTableSchemaBuilder::new("todos")
                    .column("title", PublicColumnType::Text)
                    .column("body", PublicColumnType::Text),
            )
            .table(
                PublicTableSchemaBuilder::new("todo_members")
                    .fk_column("todo", "todos")
                    .column("member", PublicColumnType::Uuid),
            ),
    ));
    let (_join_dir, mut join_core) = open_node_with_schema(node(0x4d), join_base.clone());
    publish_schema_lineage(
        &mut join_core,
        join_evolved.clone(),
        MigrationLens::new(
            join_base.version_id(),
            join_evolved.id,
            vec![
                TableLens {
                    source_table: "todos".to_owned(),
                    target_table: "todos".to_owned(),
                    ops: vec![LensOp::AddColumn {
                        column: "body".to_owned(),
                        default: v("default-body"),
                    }],
                },
                TableLens {
                    source_table: "todo_members".to_owned(),
                    target_table: "todo_members".to_owned(),
                    ops: vec![],
                },
            ],
        ).expect("valid migration lens"),
        Vec::<String>::new(),
        Vec::<String>::new(),
    )
    .unwrap();
    join_core
        .apply_trusted_catalogue_message_settled(SyncMessage::SetCurrentWriteSchema {
            author: AuthorSubject::SYSTEM,
            pointer: CurrentWriteSchema {
                revision: 1,
                schema: join_evolved.id,
            },
        })
        .unwrap();
    join_core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row(0x4d), 20).cells(BTreeMap::from([
                ("title".to_owned(), v("joined")),
                ("body".to_owned(), v("projected-body")),
            ])),
        )
        .unwrap();
    join_core
        .commit_mergeable_settled(MergeableCommit::new("todo_members", row(0x4e), 21).cells(
            BTreeMap::from([
                ("todo".to_owned(), Value::Uuid(row(0x4d).0)),
                ("member".to_owned(), Value::Uuid(row(0x4d).0)),
            ]),
        ))
        .unwrap();
    let projected_join = Query::from("todos")
        .join_via("todo_members", "todo", [eq(col("member"), param("wanted"))])
        .validate(&join_base)
        .unwrap();
    let projected_join_binding = projected_join
        .bind(BTreeMap::from([(
            "wanted".to_owned(),
            Value::Uuid(row(0x4d).0),
        )]))
        .unwrap();
    let rows = join_core
        .query_rows(
            &projected_join,
            &projected_join_binding,
            DurabilityTier::Local,
        )
        .unwrap();
    assert_eq!(
        rows.into_iter()
            .map(current_row_pair)
            .collect::<BTreeMap<_, _>>(),
        BTreeMap::from([(row(0x4d), title_cells("joined"))])
    );
}

#[test]
fn schema_projected_reads_ignore_settled_result_set_materialization_cache() {
    let base = schema();
    let evolved = evolved_todos_name_body_schema();
    let evolved_payload = SchemaVersion::new(evolved);
    let (_dir, mut core) = open_node_with_schema(node(0x4c), base.clone());
    publish_schema_lineage(
        &mut core,
        evolved_payload.clone(),
        MigrationLens::new(
            base.version_id(),
            evolved_payload.id,
            vec![TableLens {
                source_table: "todos".to_owned(),
                target_table: "todos".to_owned(),
                ops: vec![
                    LensOp::RenameColumn {
                        from: "title".to_owned(),
                        to: "name".to_owned(),
                    },
                    LensOp::AddColumn {
                        column: "body".to_owned(),
                        default: v("default-body"),
                    },
                ],
            }],
        ).expect("valid migration lens"),
        Vec::<String>::new(),
        Vec::<String>::new(),
    )
    .unwrap();
    core.apply_trusted_catalogue_message_settled(SyncMessage::SetCurrentWriteSchema {
        author: AuthorSubject::SYSTEM,
        pointer: CurrentWriteSchema {
            revision: 1,
            schema: evolved_payload.id,
        },
    })
    .unwrap();

    let tx_id = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row(0x4c), 10).cells(BTreeMap::from([
                ("name".to_owned(), v("projected")),
                ("body".to_owned(), v("cache-guard")),
            ])),
        )
        .unwrap();
    core.apply_fate_update(
        tx_id,
        Fate::Accepted,
        Some(GlobalTime(1)),
        Some(DurabilityTier::Global),
    )
    .unwrap();

    let shape = Query::from("todos")
        .filter(eq(col("title"), param("wanted")))
        .validate(&base)
        .unwrap();
    let binding = shape
        .bind(BTreeMap::from([(
            "wanted".to_owned(),
            Value::String("projected".to_owned()),
        )]))
        .unwrap();
    core.query.settled_result_sets.insert(
        crate::protocol::BindingViewKey {
            shape_id: shape.shape_id(),
            binding_id: binding.binding_id(),
            read_view: Default::default(),
        },
        BTreeSet::new(),
    );

    let rows = core
        .query_rows(&shape, &binding, DurabilityTier::Global)
        .unwrap();
    assert_eq!(
        rows.into_iter()
            .map(current_row_pair)
            .collect::<BTreeMap<_, _>>(),
        BTreeMap::from([(row(0x4c), title_cells("projected"))])
    );
}

#[test]
fn schema_projected_current_reachable_filters_translate_old_names() {
    let base = projected_reachable_schema(
        "teamEdges",
        "teamAccess",
        "title",
        "edge_kind",
        "access_kind",
    );
    let evolved = projected_reachable_schema(
        "teamEdges",
        "teamAccess",
        "title",
        "edge_label",
        "access_label",
    );
    let evolved_payload = SchemaVersion::new(evolved);
    let (_dir, mut core) = open_node_with_schema(node(0x4f), base.clone());
    publish_schema_lineage(
        &mut core,
        evolved_payload.clone(),
        MigrationLens::new(
            base.version_id(),
            evolved_payload.id,
            vec![
                TableLens {
                    source_table: "docs".to_owned(),
                    target_table: "docs".to_owned(),
                    ops: vec![],
                },
                TableLens {
                    source_table: "teams".to_owned(),
                    target_table: "teams".to_owned(),
                    ops: vec![],
                },
                TableLens {
                    source_table: "teamAccess".to_owned(),
                    target_table: "teamAccess".to_owned(),
                    ops: vec![LensOp::RenameColumn {
                        from: "access_kind".to_owned(),
                        to: "access_label".to_owned(),
                    }],
                },
                TableLens {
                    source_table: "teamEdges".to_owned(),
                    target_table: "teamEdges".to_owned(),
                    ops: vec![LensOp::RenameColumn {
                        from: "edge_kind".to_owned(),
                        to: "edge_label".to_owned(),
                    }],
                },
            ],
        ).expect("valid migration lens"),
        Vec::<String>::new(),
        Vec::<String>::new(),
    )
    .unwrap();
    core.apply_trusted_catalogue_message_settled(SyncMessage::SetCurrentWriteSchema {
        author: AuthorSubject::SYSTEM,
        pointer: CurrentWriteSchema {
            revision: 1,
            schema: evolved_payload.id,
        },
    })
    .unwrap();

    let team1 = row(0x51);
    let team2 = row(0x52);
    let team3 = row(0x53);
    for idx in [0x51, 0x52, 0x53] {
        core.commit_mergeable_settled(MergeableCommit::new("teams", row(idx), idx as u64).cells(
            BTreeMap::from([("name".to_owned(), v(format!("team-{idx}")))]),
        ))
        .unwrap();
    }
    core.commit_mergeable_settled(
        MergeableCommit::new("docs", row(0xd1), 20)
            .cells(BTreeMap::from([("title".to_owned(), v("reachable"))])),
    )
    .unwrap();
    core.commit_mergeable_settled(
        MergeableCommit::new("teamAccess", row(0xa1), 21).cells(BTreeMap::from([
            ("doc".to_owned(), Value::Uuid(row(0xd1).0)),
            ("team".to_owned(), Value::Uuid(team3.0)),
            ("access_label".to_owned(), v("allow")),
        ])),
    )
    .unwrap();
    for (idx, member, parent) in [(0xe1, team1, team2), (0xe2, team2, team3)] {
        core.commit_mergeable_settled(
            MergeableCommit::new("teamEdges", row(idx), idx as u64).cells(BTreeMap::from([
                ("member".to_owned(), Value::Uuid(member.0)),
                ("parent".to_owned(), Value::Uuid(parent.0)),
                ("edge_label".to_owned(), v("active")),
            ])),
        )
        .unwrap();
    }

    let shape = Query::from("docs")
        .reachable_via_with_access_filters(
            "teamAccess",
            "doc",
            "team",
            param("team"),
            [gt(col("access_kind"), param("access_kind"))],
            "teamEdges",
            "member",
            "parent",
            [gt(col("edge_kind"), param("edge_kind"))],
        )
        .validate(&base)
        .unwrap();
    let binding = shape
        .bind(BTreeMap::from([
            ("team".to_owned(), Value::Uuid(team1.0)),
            ("access_kind".to_owned(), v("a")),
            ("edge_kind".to_owned(), v("a")),
        ]))
        .unwrap();
    let rows = core
        .query_rows(&shape, &binding, DurabilityTier::Local)
        .unwrap();

    assert_eq!(
        rows.into_iter()
            .map(current_row_pair)
            .collect::<BTreeMap<_, _>>(),
        BTreeMap::from([(row(0xd1), title_cells("reachable"))])
    );
}

#[test]
fn include_deleted_schema_projected_root_filters_translate_old_names() {
    let base = schema();
    let evolved = SchemaVersion::new(evolved_todos_name_body_schema());
    let (_dir, mut core) = open_node_with_schema(node(0x59), base.clone());
    publish_schema_lineage(
        &mut core,
        evolved.clone(),
        MigrationLens::new(
            base.version_id(),
            evolved.id,
            vec![TableLens {
                source_table: "todos".to_owned(),
                target_table: "todos".to_owned(),
                ops: vec![
                    LensOp::RenameColumn {
                        from: "title".to_owned(),
                        to: "name".to_owned(),
                    },
                    LensOp::AddColumn {
                        column: "body".to_owned(),
                        default: v("default-body"),
                    },
                ],
            }],
        ).expect("valid migration lens"),
        Vec::<String>::new(),
        Vec::<String>::new(),
    )
    .unwrap();
    core.apply_trusted_catalogue_message_settled(SyncMessage::SetCurrentWriteSchema {
        author: AuthorSubject::SYSTEM,
        pointer: CurrentWriteSchema {
            revision: 1,
            schema: evolved.id,
        },
    })
    .unwrap();

    core.commit_mergeable_settled(
        MergeableCommit::new("todos", row(0x59), 10).cells(BTreeMap::from([
            ("name".to_owned(), v("deleted-root")),
            ("body".to_owned(), v("projected-body")),
        ])),
    )
    .unwrap();
    core.commit_mergeable_settled(
        MergeableCommit::new("todos", row(0x59), 11).deletion(DeletionEvent::Deleted),
    )
    .unwrap();

    let shape = Query::from("todos")
        .filter(eq(col("title"), param("wanted")))
        .validate(&base)
        .unwrap();
    let binding = shape
        .bind(BTreeMap::from([("wanted".to_owned(), v("deleted-root"))]))
        .unwrap();
    let rows = core
        .query_rows_including_deleted_in_authorization_mode(
            &shape,
            &binding,
            DurabilityTier::Local,
            None,
            AuthorSubject::SYSTEM,
            QueryAuthorizationMode::TrustedServing,
        )
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].row_uuid(), row(0x59));
    assert!(rows[0].is_deleted());
    assert_eq!(
        rows[0].cell(&base.tables[0], "title"),
        Some(v("deleted-root"))
    );
}

#[test]
fn include_deleted_schema_projected_join_filters_translate_old_names() {
    let base = build_public_test_schema(
        PublicSchemaBuilder::new()
            .table(
                PublicTableSchemaBuilder::new("issues")
                    .column("title", PublicColumnType::Text),
            )
            .table(
                PublicTableSchemaBuilder::new("issue_tags")
                    .fk_column("issue", "issues")
                    .column("tag_kind", PublicColumnType::Text),
            ),
    );
    let evolved = SchemaVersion::new(build_public_test_schema(
        PublicSchemaBuilder::new()
            .table(
                PublicTableSchemaBuilder::new("issues")
                    .column("name", PublicColumnType::Text),
            )
            .table(
                PublicTableSchemaBuilder::new("issue_tags")
                    .fk_column("issue", "issues")
                    .column("tag_label", PublicColumnType::Text),
            ),
    ));
    let (_dir, mut core) = open_node_with_schema(node(0x5a), base.clone());
    publish_schema_lineage(
        &mut core,
        evolved.clone(),
        MigrationLens::new(
            base.version_id(),
            evolved.id,
            vec![
                TableLens {
                    source_table: "issues".to_owned(),
                    target_table: "issues".to_owned(),
                    ops: vec![LensOp::RenameColumn {
                        from: "title".to_owned(),
                        to: "name".to_owned(),
                    }],
                },
                TableLens {
                    source_table: "issue_tags".to_owned(),
                    target_table: "issue_tags".to_owned(),
                    ops: vec![LensOp::RenameColumn {
                        from: "tag_kind".to_owned(),
                        to: "tag_label".to_owned(),
                    }],
                },
            ],
        ).expect("valid migration lens"),
        Vec::<String>::new(),
        Vec::<String>::new(),
    )
    .unwrap();
    core.apply_trusted_catalogue_message_settled(SyncMessage::SetCurrentWriteSchema {
        author: AuthorSubject::SYSTEM,
        pointer: CurrentWriteSchema {
            revision: 1,
            schema: evolved.id,
        },
    })
    .unwrap();

    let issue = row(0x5a);
    core.commit_mergeable_settled(
        MergeableCommit::new("issues", issue, 10)
            .cells(BTreeMap::from([("name".to_owned(), v("joined"))])),
    )
    .unwrap();
    core.commit_mergeable_settled(
        MergeableCommit::new("issues", issue, 11).deletion(DeletionEvent::Deleted),
    )
    .unwrap();
    core.commit_mergeable_settled(
        MergeableCommit::new("issue_tags", row(0x5b), 12).cells(BTreeMap::from([
            ("issue".to_owned(), Value::Uuid(issue.0)),
            ("tag_label".to_owned(), v("bug")),
        ])),
    )
    .unwrap();

    let shape = Query::from("issues")
        .join_via("issue_tags", "issue", [eq(col("tag_kind"), param("tag"))])
        .validate(&base)
        .unwrap();
    let binding = shape
        .bind(BTreeMap::from([("tag".to_owned(), v("bug"))]))
        .unwrap();
    let rows = core
        .query_rows_including_deleted_in_authorization_mode(
            &shape,
            &binding,
            DurabilityTier::Local,
            None,
            AuthorSubject::SYSTEM,
            QueryAuthorizationMode::TrustedServing,
        )
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].row_uuid(), issue);
    assert!(rows[0].is_deleted());
}

#[test]
fn include_deleted_schema_projected_reachable_filters_translate_old_names() {
    let base = projected_reachable_schema(
        "team_edges",
        "team_access",
        "title",
        "edge_kind",
        "access_kind",
    );
    let evolved = SchemaVersion::new(projected_reachable_schema(
        "team_edges",
        "team_access",
        "name",
        "edge_label",
        "access_label",
    ));
    let (_dir, mut core) = open_node_with_schema(node(0x5c), base.clone());
    publish_schema_lineage(
        &mut core,
        evolved.clone(),
        MigrationLens::new(
            base.version_id(),
            evolved.id,
            vec![
                TableLens {
                    source_table: "docs".to_owned(),
                    target_table: "docs".to_owned(),
                    ops: vec![LensOp::RenameColumn {
                        from: "title".to_owned(),
                        to: "name".to_owned(),
                    }],
                },
                TableLens {
                    source_table: "teams".to_owned(),
                    target_table: "teams".to_owned(),
                    ops: vec![],
                },
                TableLens {
                    source_table: "team_access".to_owned(),
                    target_table: "team_access".to_owned(),
                    ops: vec![LensOp::RenameColumn {
                        from: "access_kind".to_owned(),
                        to: "access_label".to_owned(),
                    }],
                },
                TableLens {
                    source_table: "team_edges".to_owned(),
                    target_table: "team_edges".to_owned(),
                    ops: vec![LensOp::RenameColumn {
                        from: "edge_kind".to_owned(),
                        to: "edge_label".to_owned(),
                    }],
                },
            ],
        ).expect("valid migration lens"),
        Vec::<String>::new(),
        Vec::<String>::new(),
    )
    .unwrap();
    core.apply_trusted_catalogue_message_settled(SyncMessage::SetCurrentWriteSchema {
        author: AuthorSubject::SYSTEM,
        pointer: CurrentWriteSchema {
            revision: 1,
            schema: evolved.id,
        },
    })
    .unwrap();

    let team1 = row(0x5c);
    let team2 = row(0x5d);
    let doc = row(0x5e);
    for (idx, team) in [(10, team1), (11, team2)] {
        core.commit_mergeable_settled(
            MergeableCommit::new("teams", team, idx).cells(BTreeMap::from([(
                "name".to_owned(),
                v(format!("team-{idx}")),
            )])),
        )
        .unwrap();
    }
    core.commit_mergeable_settled(
        MergeableCommit::new("docs", doc, 12)
            .cells(BTreeMap::from([("name".to_owned(), v("reachable"))])),
    )
    .unwrap();
    core.commit_mergeable_settled(MergeableCommit::new("docs", doc, 13).deletion(DeletionEvent::Deleted))
        .unwrap();
    core.commit_mergeable_settled(
        MergeableCommit::new("team_edges", row(0x5f), 14).cells(BTreeMap::from([
            ("member".to_owned(), Value::Uuid(team1.0)),
            ("parent".to_owned(), Value::Uuid(team2.0)),
            ("edge_label".to_owned(), v("active")),
        ])),
    )
    .unwrap();
    core.commit_mergeable_settled(MergeableCommit::new("team_access", row(0x60), 15).cells(
        BTreeMap::from([
            ("doc".to_owned(), Value::Uuid(doc.0)),
            ("team".to_owned(), Value::Uuid(team2.0)),
            ("access_label".to_owned(), v("allow")),
        ]),
    ))
    .unwrap();

    let shape = Query::from("docs")
        .filter(eq(col("title"), param("title")))
        .reachable_via_with_access_filters(
            "team_access",
            "doc",
            "team",
            param("team"),
            [eq(col("access_kind"), param("access_kind"))],
            "team_edges",
            "member",
            "parent",
            [eq(col("edge_kind"), param("edge_kind"))],
        )
        .validate(&base)
        .unwrap();
    let binding = shape
        .bind(BTreeMap::from([
            ("title".to_owned(), v("reachable")),
            ("team".to_owned(), Value::Uuid(team1.0)),
            ("access_kind".to_owned(), v("allow")),
            ("edge_kind".to_owned(), v("active")),
        ]))
        .unwrap();
    let rows = core
        .query_rows_including_deleted_in_authorization_mode(
            &shape,
            &binding,
            DurabilityTier::Local,
            None,
            AuthorSubject::SYSTEM,
            QueryAuthorizationMode::TrustedServing,
        )
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].row_uuid(), doc);
    assert!(rows[0].is_deleted());
}

#[test]
fn historical_schema_projected_reads_use_projected_snapshot_source() {
    let base = schema();
    let evolved = evolved_todos_name_body_schema();
    let evolved_payload = SchemaVersion::new(evolved);
    let (_dir, mut core) = open_node_with_schema(node(0x54), base.clone());
    publish_schema_lineage(
        &mut core,
        evolved_payload.clone(),
        MigrationLens::new(
            base.version_id(),
            evolved_payload.id,
            vec![TableLens {
                source_table: "todos".to_owned(),
                target_table: "todos".to_owned(),
                ops: vec![
                    LensOp::RenameColumn {
                        from: "title".to_owned(),
                        to: "name".to_owned(),
                    },
                    LensOp::AddColumn {
                        column: "body".to_owned(),
                        default: v("default-body"),
                    },
                ],
            }],
        ).expect("valid migration lens"),
        Vec::<String>::new(),
        Vec::<String>::new(),
    )
    .unwrap();
    core.apply_trusted_catalogue_message_settled(SyncMessage::SetCurrentWriteSchema {
        author: AuthorSubject::SYSTEM,
        pointer: CurrentWriteSchema {
            revision: 1,
            schema: evolved_payload.id,
        },
    })
    .unwrap();

    let tx_id = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row(0x54), 10).cells(BTreeMap::from([
                ("name".to_owned(), v("historical")),
                ("body".to_owned(), v("projected-body")),
            ])),
        )
        .unwrap();
    core.apply_fate_update(
        tx_id,
        Fate::Accepted,
        Some(GlobalTime(1)),
        Some(DurabilityTier::Global),
    )
    .unwrap();

    let shape = Query::from("todos")
        .filter(eq(col("title"), param("wanted")))
        .validate(&base)
        .unwrap();
    let binding = shape
        .bind(BTreeMap::from([(
            "wanted".to_owned(),
            Value::String("historical".to_owned()),
        )]))
        .unwrap();
    let unfiltered = Query::from("todos").validate(&base).unwrap();
    let unfiltered_binding = unfiltered.bind(BTreeMap::new()).unwrap();

    assert!(
        core.query_rows_at(&shape, &binding, GlobalTime(0))
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        core.query_rows_at(&unfiltered, &unfiltered_binding, GlobalTime(1))
            .unwrap()
            .into_iter()
            .map(current_row_pair)
            .collect::<BTreeMap<_, _>>(),
        BTreeMap::from([(row(0x54), title_cells("historical"))])
    );
    let rows = core.query_rows_at(&shape, &binding, GlobalTime(1)).unwrap();
    assert_eq!(
        rows.into_iter()
            .map(current_row_pair)
            .collect::<BTreeMap<_, _>>(),
        BTreeMap::from([(row(0x54), title_cells("historical"))])
    );
}

#[test]
fn global_changes_span_table_renames_for_history_and_conflict_detection() {
    // The physical key and bounded-source selection are intentionally internal;
    // user-visible historical rows and exclusive rejection cover their semantics.
    let base = schema();
    let renamed_schema = renamed_tasks_schema();
    let renamed = SchemaVersion::new(renamed_schema);
    let (dir, mut core) = open_node_with_schema(node(0x57), base.clone());

    let base_tx = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row(0x57), 10).cells(title_cells("before rename")),
        )
        .unwrap();
    core.apply_fate_update(
        base_tx,
        Fate::Accepted,
        Some(GlobalTime(1)),
        Some(DurabilityTier::Global),
    )
    .unwrap();

    let exclusive = OpenTransactionId::new();
    core.open_exclusive(exclusive).unwrap();
    assert_eq!(core.tx_current_rows(exclusive, "todos").unwrap().len(), 1);
    core.tx_write(
        exclusive,
        "todos",
        row(0x58),
        title_cells("conflicting transaction"),
        None,
    )
    .unwrap();

    publish_schema_lineage(
        &mut core,
        renamed.clone(),
        MigrationLens::new(
            base.version_id(),
            renamed.id,
            vec![TableLens {
                source_table: "todos".to_owned(),
                target_table: "tasks".to_owned(),
                ops: vec![
                    LensOp::RenameTable {
                        from: "todos".to_owned(),
                        to: "tasks".to_owned(),
                    },
                    LensOp::RenameColumn {
                        from: "title".to_owned(),
                        to: "name".to_owned(),
                    },
                ],
            }],
        ).expect("valid migration lens"),
        Vec::<String>::new(),
        Vec::<String>::new(),
    )
    .unwrap();
    core.apply_trusted_catalogue_message_settled(SyncMessage::SetCurrentWriteSchema {
        author: AuthorSubject::SYSTEM,
        pointer: CurrentWriteSchema {
            revision: 1,
            schema: renamed.id,
        },
    })
    .unwrap();

    let renamed_tx = core
        .commit_mergeable_settled(
            MergeableCommit::new("tasks", row(0x59), 11)
                .cells(BTreeMap::from([("name".to_owned(), v("after rename"))])),
        )
        .unwrap();
    core.apply_fate_update(
        renamed_tx,
        Fate::Accepted,
        Some(GlobalTime(2)),
        Some(DurabilityTier::Global),
    )
    .unwrap();

    let table_id = core.catalogue.physical_mappings[&base.version_id()].tables["todos"].table_id;
    assert_eq!(
        core.catalogue.physical_mappings[&renamed.id].tables["tasks"].table_id,
        table_id
    );
    let changes = core
        .database
        .primary_key_scan_raw("jazz_global_changes", &[])
        .unwrap();
    assert_eq!(changes.len(), 2);
    assert!(changes.iter().all(|raw| {
        raw.record()
            .get_u64(GlobalChangeRowRecord::FIELD_PHYSICAL_TABLE_ID_IDX)
            .unwrap()
            == table_id.0
    }));

    assert!(matches!(
        core.commit_exclusive_settled(exclusive, AuthorSubject::SYSTEM, 12),
        Err(Error::TransactionConflict)
    ));

    let shape = Query::from("todos").validate(&base).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    assert_eq!(
        core.query_rows_at(&shape, &binding, GlobalTime(1))
            .unwrap()
            .into_iter()
            .map(current_row_pair)
            .collect::<BTreeMap<_, _>>(),
        BTreeMap::from([(row(0x57), title_cells("before rename"))])
    );
    core.reset_query_engine_read_metrics();
    assert_eq!(
        core.query_rows_at(&shape, &binding, GlobalTime(2))
            .unwrap()
            .into_iter()
            .map(current_row_pair)
            .collect::<BTreeMap<_, _>>(),
        BTreeMap::from([
            (row(0x57), title_cells("before rename")),
            (row(0x59), title_cells("after rename")),
        ])
    );
    assert_eq!(
        core.query_engine_read_metrics()
            .source_global_time_range_scans,
        1,
        "a renamed lineage should use the bounded physical global-change source"
    );

    drop(core);
    let mut reopened = reopen_node_at(&dir, node(0x57), base.clone());
    assert!(
        reopened
            .global_currency_changed_after("todos", GlobalTime(1))
            .unwrap()
    );
    assert_eq!(
        reopened
            .query_rows_at(&shape, &binding, GlobalTime(2))
            .unwrap()
            .into_iter()
            .map(current_row_pair)
            .collect::<BTreeMap<_, _>>(),
        BTreeMap::from([
            (row(0x57), title_cells("before rename")),
            (row(0x59), title_cells("after rename")),
        ])
    );
}

#[test]
fn historical_schema_projected_reachable_filters_translate_old_names() {
    let base = projected_reachable_schema(
        "teamEdges",
        "teamAccess",
        "title",
        "edge_kind",
        "access_kind",
    );
    let evolved = projected_reachable_schema(
        "teamEdges",
        "teamAccess",
        "name",
        "edge_label",
        "access_label",
    );
    let evolved_payload = SchemaVersion::new(evolved);
    let (_dir, mut core) = open_node_with_schema(node(0x55), base.clone());
    publish_schema_lineage(
        &mut core,
        evolved_payload.clone(),
        MigrationLens::new(
            base.version_id(),
            evolved_payload.id,
            vec![
                TableLens {
                    source_table: "docs".to_owned(),
                    target_table: "docs".to_owned(),
                    ops: vec![LensOp::RenameColumn {
                        from: "title".to_owned(),
                        to: "name".to_owned(),
                    }],
                },
                TableLens {
                    source_table: "teams".to_owned(),
                    target_table: "teams".to_owned(),
                    ops: vec![],
                },
                TableLens {
                    source_table: "teamAccess".to_owned(),
                    target_table: "teamAccess".to_owned(),
                    ops: vec![LensOp::RenameColumn {
                        from: "access_kind".to_owned(),
                        to: "access_label".to_owned(),
                    }],
                },
                TableLens {
                    source_table: "teamEdges".to_owned(),
                    target_table: "teamEdges".to_owned(),
                    ops: vec![LensOp::RenameColumn {
                        from: "edge_kind".to_owned(),
                        to: "edge_label".to_owned(),
                    }],
                },
            ],
        ).expect("valid migration lens"),
        Vec::<String>::new(),
        Vec::<String>::new(),
    )
    .unwrap();
    core.apply_trusted_catalogue_message_settled(SyncMessage::SetCurrentWriteSchema {
        author: AuthorSubject::SYSTEM,
        pointer: CurrentWriteSchema {
            revision: 1,
            schema: evolved_payload.id,
        },
    })
    .unwrap();

    let team1 = row(0x56);
    let team2 = row(0x57);
    let team3 = row(0x58);
    for idx in [0x56, 0x57, 0x58] {
        let tx_id = core
            .commit_mergeable_settled(MergeableCommit::new("teams", row(idx), idx as u64).cells(
                BTreeMap::from([("name".to_owned(), v(format!("team-{idx}")))]),
            ))
            .unwrap();
        core.apply_fate_update(
            tx_id,
            Fate::Accepted,
            Some(GlobalTime(idx as u64)),
            Some(DurabilityTier::Global),
        )
        .unwrap();
    }
    let doc_tx = core
        .commit_mergeable_settled(
            MergeableCommit::new("docs", row(0xd5), 90)
                .cells(BTreeMap::from([("name".to_owned(), v("reachable"))])),
        )
        .unwrap();
    core.apply_fate_update(
        doc_tx,
        Fate::Accepted,
        Some(GlobalTime(90)),
        Some(DurabilityTier::Global),
    )
    .unwrap();
    let access_tx = core
        .commit_mergeable_settled(
            MergeableCommit::new("teamAccess", row(0xa5), 91).cells(BTreeMap::from([
                ("doc".to_owned(), Value::Uuid(row(0xd5).0)),
                ("team".to_owned(), Value::Uuid(team3.0)),
                ("access_label".to_owned(), v("allow")),
            ])),
        )
        .unwrap();
    core.apply_fate_update(
        access_tx,
        Fate::Accepted,
        Some(GlobalTime(91)),
        Some(DurabilityTier::Global),
    )
    .unwrap();
    for (idx, member, parent) in [(0xe5, team1, team2), (0xe6, team2, team3)] {
        let tx_id = core
            .commit_mergeable_settled(
                MergeableCommit::new("teamEdges", row(idx), idx as u64).cells(BTreeMap::from([
                    ("member".to_owned(), Value::Uuid(member.0)),
                    ("parent".to_owned(), Value::Uuid(parent.0)),
                    ("edge_label".to_owned(), v("active")),
                ])),
            )
            .unwrap();
        core.apply_fate_update(
            tx_id,
            Fate::Accepted,
            Some(GlobalTime(idx as u64)),
            Some(DurabilityTier::Global),
        )
        .unwrap();
    }

    let shape = Query::from("docs")
        .filter(eq(col("title"), param("title")))
        .reachable_via_with_access_filters(
            "teamAccess",
            "doc",
            "team",
            param("team"),
            [eq(col("access_kind"), param("access_kind"))],
            "teamEdges",
            "member",
            "parent",
            [eq(col("edge_kind"), param("edge_kind"))],
        )
        .validate(&base)
        .unwrap();
    let binding = shape
        .bind(BTreeMap::from([
            ("title".to_owned(), v("reachable")),
            ("team".to_owned(), Value::Uuid(team1.0)),
            ("access_kind".to_owned(), v("allow")),
            ("edge_kind".to_owned(), v("active")),
        ]))
        .unwrap();

    assert!(
        core.query_rows_at(&shape, &binding, GlobalTime(90))
            .unwrap()
            .is_empty(),
        "access and edge rows should not be visible before their historical positions"
    );
    let rows = core
        .query_rows_at(&shape, &binding, GlobalTime(230))
        .unwrap();
    assert_eq!(
        rows.into_iter()
            .map(current_row_pair)
            .collect::<BTreeMap<_, _>>(),
        BTreeMap::from([(row(0xd5), title_cells("reachable"))])
    );
    assert!(
        !core
            .query
            .query_shape_cache
            .keys()
            .any(|(shape_id, _, _)| *shape_id == shape.shape_id()),
        "historical schema-projected reachable reads must lower over inline projected sources"
    );
}
