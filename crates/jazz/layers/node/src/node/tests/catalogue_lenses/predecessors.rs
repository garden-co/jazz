// These tests use the internal catalogue boundary because the public deploy
// endpoint does not yet expose multi-predecessor publications or snapshot replay.

fn predecessor_test_schema(columns: &[&str]) -> JazzSchema {
    let mut table = PublicTableSchemaBuilder::new("notes").column("title", PublicColumnType::Text);
    for column in columns {
        table = table.column(*column, PublicColumnType::Text);
    }
    build_public_test_schema(
        PublicSchemaBuilder::new().table(table.policies(public_all_policies())),
    )
}

fn predecessor_test_lens(
    source: &JazzSchema,
    target: &JazzSchema,
    column: &str,
) -> crate::protocol::SchemaPredecessor {
    crate::protocol::SchemaPredecessor {
        lens: MigrationLens::new(
            source.version_id(),
            target.version_id(),
            vec![TableLens {
                source_table: "notes".into(),
                target_table: "notes".into(),
                ops: vec![LensOp::AddColumn {
                    column: column.into(),
                    default: if matches!(
                        &target.tables[0]
                            .columns
                            .iter()
                            .find(|c| c.name == column)
                            .unwrap()
                            .column_type,
                        records::ValueType::EnumTag(_)
                    ) {
                        Value::EnumTag(0)
                    } else {
                        v(format!("default-{column}"))
                    },
                }],
            }],
        )
        .unwrap(),
        new_tables: vec![],
        dropped_tables: vec![],
    }
}

/// Alice writes under B; Bob writes under C. D keeps both authored columns.
/// A -> B -> D, A -> C -> D; reopen authority, bootstrap edge, replay, reopen.
#[test]
fn merged_predecessors_preserve_branch_values_across_reopen_and_replication() {
    check_merged_predecessors(false);
    check_merged_predecessors(true);
}

fn check_merged_predecessors(enum_columns: bool) {
    let schema = |columns: &[&str]| {
        if !enum_columns {
            return predecessor_test_schema(columns);
        }
        let mut table =
            PublicTableSchemaBuilder::new("notes").column("title", PublicColumnType::Text);
        for column in columns {
            table = table.column(
                *column,
                public_scalar_enum(column, &["default", "authored"]),
            );
        }
        build_public_test_schema(
            PublicSchemaBuilder::new().table(table.policies(public_all_policies())),
        )
    };
    let authored = |column: &str| {
        if enum_columns {
            Value::EnumTag(1)
        } else {
            v(format!("authored-{column}"))
        }
    };
    let default = |column: &str| {
        if enum_columns {
            Value::EnumTag(0)
        } else {
            v(format!("default-{column}"))
        }
    };
    let base = schema(&[]);
    let left = schema(&["a"]);
    let right = schema(&["b"]);
    let merged = schema(&["a", "b"]);
    let (dir, mut authority) = open_node_with_schema(node(0xdb), base.clone());
    let mut messages = Vec::new();
    for (revision, schema, column, id) in [(1, &left, "a", row(0xdb)), (2, &right, "b", row(0xdc))]
    {
        publish_schema_lineage(
            &mut authority,
            SchemaVersion::new(schema.clone()),
            predecessor_test_lens(&base, schema, column).lens,
            Vec::<String>::new(),
            Vec::<String>::new(),
        )
        .unwrap();
        authority
            .activate_catalogue_schema_settled(CurrentWriteSchema {
                revision,
                schema: schema.version_id(),
            })
            .unwrap();
        let (_, message) = authority
            .commit_mergeable_unit_settled(MergeableCommit::new("notes", id, revision * 10).cells(
                BTreeMap::from([
                    ("title".into(), v(column)),
                    (column.into(), authored(column)),
                ]),
            ))
            .unwrap();
        messages.push(message);
    }
    let publication = authority
        .author_schema_from_predecessors(
            SchemaVersion::new(merged.clone()),
            vec![
                predecessor_test_lens(&left, &merged, "b"),
                predecessor_test_lens(&right, &merged, "a"),
            ],
        )
        .unwrap();
    let mut reordered = publication.clone();
    reordered.predecessors.reverse();
    assert_eq!(publication, reordered);
    assert_eq!(publication.id, reordered.content_id());
    authority
        .apply_trusted_catalogue_message_settled(SyncMessage::PublishSchemaWithLens {
            author: AuthorSubject::SYSTEM,
            catalogue_seq: 3,
            publication: Box::new(publication.clone()),
        })
        .unwrap();
    authority
        .activate_catalogue_schema_settled(CurrentWriteSchema {
            revision: 3,
            schema: merged.version_id(),
        })
        .unwrap();
    let shape = Query::from("notes").validate(&merged).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let expected = BTreeMap::from([
        (
            row(0xdb),
            BTreeMap::from([
                ("title".into(), v("a")),
                ("a".into(), authored("a")),
                ("b".into(), default("b")),
            ]),
        ),
        (
            row(0xdc),
            BTreeMap::from([
                ("title".into(), v("b")),
                ("a".into(), default("a")),
                ("b".into(), authored("b")),
            ]),
        ),
    ]);
    let assert_rows = |core: &mut NodeState| {
        let rows = core
            .query_rows(&shape, &binding, DurabilityTier::Local)
            .unwrap();
        assert_eq!(
            rows.into_iter()
                .map(current_row_pair)
                .collect::<BTreeMap<_, _>>(),
            expected
        );
    };
    assert_rows(&mut authority);
    drop(authority);
    let mut authority = reopen_node_at(&dir, node(0xdb), base);
    assert_rows(&mut authority);
    let snapshot = authority.catalogue_snapshot().unwrap();
    assert_eq!(snapshot.lineages[2].1, publication);
    let edge_dir = tempfile::tempdir().unwrap();
    let mut edge = fresh_dynamic_catalogue_open(edge_dir.path(), node(0xdd)).unwrap();
    let mut incomplete = snapshot.clone();
    let record = &mut incomplete.lineages[2].1;
    record.predecessors.pop();
    record.id = record.content_id();
    assert!(
        edge.apply_trusted_catalogue_snapshot_settled(incomplete)
            .is_err(),
        "a merge cannot reuse a sibling's column identity without its incoming migration"
    );
    assert_eq!(
        edge.active_catalogue_seq(),
        0,
        "invalid snapshot leaves no admitted prefix"
    );
    edge.apply_trusted_catalogue_snapshot_settled(snapshot.clone())
        .unwrap();
    for message in &messages {
        edge.apply_sync_message_settled(message.clone()).unwrap();
    }
    assert_rows(&mut edge);
    edge.apply_trusted_catalogue_snapshot_settled(snapshot.clone())
        .unwrap();
    drop(edge);
    let mut edge = fresh_dynamic_catalogue_open(edge_dir.path(), node(0xdd)).unwrap();
    assert_rows(&mut edge);
    assert_eq!(
        edge.catalogue_snapshot().unwrap().lineages[2].1,
        publication
    );
    let (anchored_dir, mut anchored) = open_node_with_schema(node(0xe0), merged.clone());
    anchored
        .apply_trusted_catalogue_snapshot_settled(snapshot.clone())
        .unwrap();
    for message in &messages {
        anchored
            .apply_sync_message_settled(message.clone())
            .unwrap();
    }
    assert_rows(&mut anchored);
    drop(anchored);
    let mut anchored = reopen_node_at(&anchored_dir, node(0xe0), merged);
    assert_rows(&mut anchored);

    // D can arrive before C. Reopening must retain the parked merge without
    // treating D's inherited b as an unrelated reservation against C.
    let mut prefix = snapshot.clone();
    prefix.schemas.retain(|schema| {
        schema.id == left.version_id()
            || schema.id == snapshot.lineages[0].1.predecessors[0].lens.source()
    });
    prefix.lineages.truncate(1);
    prefix.current_write_schema = CurrentWriteSchema {
        revision: 1,
        schema: left.version_id(),
    };
    let parked_dir = tempfile::tempdir().unwrap();
    let mut parked = fresh_dynamic_catalogue_open(parked_dir.path(), node(0xdf)).unwrap();
    parked
        .apply_trusted_catalogue_snapshot_settled(prefix)
        .unwrap();
    parked
        .apply_trusted_catalogue_message_settled(SyncMessage::PublishSchemaWithLens {
            author: AuthorSubject::SYSTEM,
            catalogue_seq: 3,
            publication: Box::new(publication),
        })
        .unwrap();
    assert_eq!(parked.active_catalogue_seq(), 1);
    drop(parked);
    let mut parked = fresh_dynamic_catalogue_open(parked_dir.path(), node(0xdf)).unwrap();
    parked
        .apply_trusted_catalogue_message_settled(SyncMessage::PublishSchemaWithLens {
            author: AuthorSubject::SYSTEM,
            catalogue_seq: 2,
            publication: Box::new(snapshot.lineages[1].1.clone()),
        })
        .unwrap();
    assert_eq!(parked.active_catalogue_seq(), 3);
    parked
        .apply_trusted_catalogue_snapshot_settled(snapshot)
        .unwrap();
    for message in messages {
        parked.apply_sync_message_settled(message).unwrap();
    }
    assert_rows(&mut parked);
}

/// Independently introduced columns cannot silently become the same stored column.
#[test]
fn merged_predecessors_reject_conflicting_inherited_identities() {
    let base = predecessor_test_schema(&[]);
    let left = predecessor_test_schema(&["a"]);
    let right = predecessor_test_schema(&["b"]);
    let (_dir, mut authority) = open_node_with_schema(node(0xde), base.clone());
    for (schema, column) in [(&left, "a"), (&right, "b")] {
        publish_schema_lineage(
            &mut authority,
            SchemaVersion::new(schema.clone()),
            predecessor_test_lens(&base, schema, column).lens,
            Vec::<String>::new(),
            Vec::<String>::new(),
        )
        .unwrap();
    }
    let target = predecessor_test_schema(&["combined"]);
    let predecessors = [(&left, "a"), (&right, "b")]
        .into_iter()
        .map(|(schema, column)| crate::protocol::SchemaPredecessor {
            lens: MigrationLens::new(
                schema.version_id(),
                target.version_id(),
                vec![TableLens {
                    source_table: "notes".into(),
                    target_table: "notes".into(),
                    ops: vec![LensOp::RenameColumn {
                        from: column.into(),
                        to: "combined".into(),
                    }],
                }],
            )
            .unwrap(),
            new_tables: vec![],
            dropped_tables: vec![],
        })
        .collect();
    assert!(
        authority
            .author_schema_from_predecessors(SchemaVersion::new(target.clone()), predecessors)
            .is_err()
    );
    assert!(
        !authority
            .catalogue_schemas()
            .contains_key(&target.version_id())
    );
}

/// Alice's `a` survives B -> X -> Y -> Z -> D even though B -> A -> C -> D
/// is shorter. This uses internal publication APIs until deploy accepts merges.
#[test]
fn forward_merge_path_preserves_values_when_reverse_route_is_shorter() {
    let schemas = [
        vec![],
        vec!["a"],
        vec!["a", "x"],
        vec!["a", "x", "y"],
        vec!["a", "x", "y", "z"],
        vec!["b"],
        vec!["a", "b"],
    ]
    .map(|columns| predecessor_test_schema(&columns));
    let [base, left, x, y, z, right, merged] = &schemas;
    let (_dir, mut authority) = open_node_with_schema(node(0xe1), base.clone());
    for (source, target, column) in [
        (base, left, "a"),
        (left, x, "x"),
        (x, y, "y"),
        (y, z, "z"),
        (base, right, "b"),
    ] {
        publish_schema_lineage(
            &mut authority,
            SchemaVersion::new(target.clone()),
            predecessor_test_lens(source, target, column).lens,
            Vec::<String>::new(),
            Vec::<String>::new(),
        )
        .unwrap();
    }
    authority
        .activate_catalogue_schema_settled(CurrentWriteSchema {
            revision: 1,
            schema: left.version_id(),
        })
        .unwrap();
    authority
        .commit_mergeable_settled(MergeableCommit::new("notes", row(0xe1), 10).cells(
            BTreeMap::from([("title".into(), v("alice")), ("a".into(), v("authored-a"))]),
        ))
        .unwrap();
    let mut ops = vec![LensOp::AddColumn {
        column: "b".into(),
        default: v("default-b"),
    }];
    ops.extend(["x", "y", "z"].map(|column| LensOp::DropColumn {
        column: column.into(),
        backwards_default: v(""),
    }));
    let publication = authority
        .author_schema_from_predecessors(
            SchemaVersion::new(merged.clone()),
            vec![
                crate::protocol::SchemaPredecessor {
                    lens: MigrationLens::new(
                        z.version_id(),
                        merged.version_id(),
                        vec![TableLens {
                            source_table: "notes".into(),
                            target_table: "notes".into(),
                            ops,
                        }],
                    )
                    .unwrap(),
                    new_tables: vec![],
                    dropped_tables: vec![],
                },
                predecessor_test_lens(right, merged, "a"),
            ],
        )
        .unwrap();
    authority
        .apply_trusted_catalogue_message_settled(SyncMessage::PublishSchemaWithLens {
            author: AuthorSubject::SYSTEM,
            catalogue_seq: 6,
            publication: Box::new(publication),
        })
        .unwrap();
    authority
        .activate_catalogue_schema_settled(CurrentWriteSchema {
            revision: 2,
            schema: merged.version_id(),
        })
        .unwrap();
    let shape = Query::from("notes").validate(merged).unwrap();
    let rows = authority
        .query_rows(
            &shape,
            &shape.bind(BTreeMap::new()).unwrap(),
            DurabilityTier::Local,
        )
        .unwrap();
    assert_eq!(
        rows.into_iter()
            .map(current_row_pair)
            .collect::<BTreeMap<_, _>>(),
        BTreeMap::from([(
            row(0xe1),
            BTreeMap::from([
                ("title".into(), v("alice")),
                ("a".into(), v("authored-a")),
                ("b".into(), v("default-b"))
            ])
        )])
    );
}
