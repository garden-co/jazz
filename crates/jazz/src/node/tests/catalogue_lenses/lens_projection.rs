// Lens-aware reads, policies, transforms, writes, and materialization oracles.

#[test]
fn shared_physical_reads_project_natural_lenses_after_schema_agnostic_winner() {
    let base = schema();
    let evolved = evolved_todos_name_body_schema();
    let evolved_payload = SchemaVersion::new(evolved.clone());
    let (_dir, mut core) = open_node_with_schema(node(0x3d), base.clone());
    let old_row = row(0x41);
    core.commit_mergeable_settled(
        MergeableCommit::new("todos", old_row, 10).cells(title_cells("old-title")),
    )
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
    let new_row = row(0x42);
    core.commit_mergeable_settled(
        MergeableCommit::new("todos", new_row, 11).cells(BTreeMap::from([
            ("name".to_owned(), v("new-name")),
            ("body".to_owned(), v("new-body")),
        ])),
    )
    .unwrap();

    let v2_shape = Query::from("todos").validate(&evolved).unwrap();
    let v2_rows = core
        .query_rows(
            &v2_shape,
            &v2_shape.bind(BTreeMap::new()).unwrap(),
            DurabilityTier::Local,
        )
        .unwrap()
        .into_iter()
        .map(current_row_pair)
        .collect::<BTreeMap<_, _>>();
    assert_eq!(
        v2_rows,
        BTreeMap::from([
            (
                old_row,
                BTreeMap::from([
                    ("name".to_owned(), v("old-title")),
                    ("body".to_owned(), v("default-body")),
                ]),
            ),
            (
                new_row,
                BTreeMap::from([
                    ("name".to_owned(), v("new-name")),
                    ("body".to_owned(), v("new-body")),
                ]),
            ),
        ])
    );

    let v1_shape = Query::from("todos").validate(&base).unwrap();
    let v1_rows = core
        .query_rows(
            &v1_shape,
            &v1_shape.bind(BTreeMap::new()).unwrap(),
            DurabilityTier::Local,
        )
        .unwrap()
        .into_iter()
        .map(current_row_pair)
        .collect::<BTreeMap<_, _>>();
    assert_eq!(
        v1_rows,
        BTreeMap::from([
            (old_row, title_cells("old-title")),
            (new_row, title_cells("new-name")),
        ])
    );

    core.commit_mergeable_settled(
        MergeableCommit::new("todos", new_row, 12).deletion(DeletionEvent::Deleted),
    )
    .unwrap();
    let include_deleted_shape = Query::from("todos")
        .filter(eq(col("title"), param("wanted")))
        .validate(&base)
        .unwrap();
    let include_deleted_binding = include_deleted_shape
        .bind(BTreeMap::from([("wanted".to_owned(), v("new-name"))]))
        .unwrap();
    let include_deleted_rows = core
        .query_rows_including_deleted_in_authorization_mode(
            &include_deleted_shape,
            &include_deleted_binding,
            DurabilityTier::Local,
            None,
            AuthorSubject::SYSTEM,
            QueryAuthorizationMode::TrustedServing,
        )
        .unwrap();
    assert_eq!(include_deleted_rows.len(), 1);
    assert_eq!(include_deleted_rows[0].row_uuid(), new_row);
    assert!(include_deleted_rows[0].is_deleted());
    assert_eq!(
        include_deleted_rows[0].cell(&base.tables[0], "title"),
        Some(v("new-name"))
    );
}

#[test]
fn agreeing_cross_lens_keeps_the_authoritative_physical_mapping() {
    let v1 = schema();
    let v2 = evolved_todos_name_body_schema();
    let v3 = build_public_test_schema(PublicSchemaBuilder::new().table(
        PublicTableSchemaBuilder::new("todos")
            .column("name", PublicColumnType::Text)
            .column("search_name", PublicColumnType::Text),
    ));
    let v2_payload = SchemaVersion::new(v2.clone());
    let v3_payload = SchemaVersion::new(v3.clone());
    let (_dir, mut core) = open_node_with_schema(node(0x3e), v1.clone());
    let long_first = MigrationLens::new(
        v1.version_id(),
        v2_payload.id,
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
                    default: v("via-long"),
                },
            ],
        }],
    ).expect("valid migration lens");
    let long_second = MigrationLens::new(
        v2_payload.id,
        v3_payload.id,
        vec![TableLens {
            source_table: "todos".to_owned(),
            target_table: "todos".to_owned(),
            ops: vec![
                LensOp::DropColumn {
                    column: "body".to_owned(),
                    backwards_default: v(""),
                },
                LensOp::CopyColumn {
                    from: "name".to_owned(),
                    to: "search_name".to_owned(),
                },
            ],
        }],
    ).expect("valid migration lens");
    let shortest = MigrationLens::new(
        v1.version_id(),
        v3_payload.id,
        vec![TableLens {
            source_table: "todos".to_owned(),
            target_table: "todos".to_owned(),
            ops: vec![
                LensOp::RenameColumn {
                    from: "title".to_owned(),
                    to: "name".to_owned(),
                },
                LensOp::AddColumn {
                    column: "search_name".to_owned(),
                    default: v("via-shortest"),
                },
            ],
        }],
    ).expect("valid migration lens");
    publish_schema_lineage(
        &mut core,
        v2_payload.clone(),
        long_first,
        Vec::<String>::new(),
        Vec::<String>::new(),
    )
    .unwrap();
    publish_schema_lineage(
        &mut core,
        v3_payload.clone(),
        long_second,
        Vec::<String>::new(),
        Vec::<String>::new(),
    )
    .unwrap();
    let authoritative = core.catalogue.physical_mappings[&v3_payload.id].clone();
    let published_lens_count = core.catalogue.catalogue_lenses.len();

    core.apply_trusted_catalogue_message_settled(SyncMessage::PublishLens {
        author: AuthorSubject::SYSTEM,
        lens: shortest.clone(),
    })
    .unwrap();
    assert_eq!(
        core.catalogue.physical_mappings[&v3_payload.id],
        authoritative
    );
    assert_eq!(
        core.catalogue.catalogue_lenses.len(),
        published_lens_count + 1
    );
    assert!(core.catalogue.catalogue_lenses.contains_key(&shortest.id));
}

#[test]
fn old_schema_commit_units_stay_in_authored_variant_after_pointer_flip() {
    let base = schema();
    let evolved = evolved_todos_name_body_schema();
    let evolved_payload = SchemaVersion::new(evolved.clone());
    let (_writer_dir, mut writer) = open_node_with_schema(node(0x43), base.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(0x44), base.clone());
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

    let old_row = row(0x45);
    let (_tx_id, unit) = writer
        .commit_mergeable_unit_settled(
            MergeableCommit::new("todos", old_row, 12).cells(title_cells("old-writer")),
        )
        .unwrap();
    core.apply_sync_message_settled(unit).unwrap();

    let stored = core.query_table_versions("todos").unwrap();
    assert_eq!(stored.len(), 1);
    let stored_wire = core.version_record_from_row(&stored[0]).unwrap();
    assert_eq!(stored_wire.schema_version(), base.version_id());
    assert_eq!(
        version_record_cells(&stored_wire, &base.tables[0]),
        title_cells("old-writer")
    );

    let v2_shape = Query::from("todos").validate(&evolved).unwrap();
    assert_eq!(
        core.query_rows(
            &v2_shape,
            &v2_shape.bind(BTreeMap::new()).unwrap(),
            DurabilityTier::Local,
        )
        .unwrap()
        .into_iter()
        .map(current_row_pair)
        .collect::<BTreeMap<_, _>>(),
        BTreeMap::from([(
            old_row,
            BTreeMap::from([
                ("name".to_owned(), v("old-writer")),
                ("body".to_owned(), v("default-body")),
            ]),
        )])
    );

    let v1_shape = Query::from("todos").validate(&base).unwrap();
    assert_eq!(
        core.query_rows(
            &v1_shape,
            &v1_shape.bind(BTreeMap::new()).unwrap(),
            DurabilityTier::Local,
        )
        .unwrap()
        .into_iter()
        .map(current_row_pair)
        .collect::<BTreeMap<_, _>>(),
        BTreeMap::from([(old_row, title_cells("old-writer"))])
    );
}

#[test]
fn rls_policy_under_lenses_evaluates_translated_data_against_pinned_policy() {
    let pinned = owner_policy_schema();
    let evolved = build_public_test_schema(PublicSchemaBuilder::new().table(
        PublicTableSchemaBuilder::new("todos")
            .column("name", PublicColumnType::Text)
            .column("extra_owner", PublicColumnType::Uuid)
            .column("owner_id", PublicColumnType::Uuid),
    ));
    let evolved_payload = SchemaVersion::new(evolved.clone());
    let (_writer_dir, mut writer) = open_node_with_schema(node(0x46), evolved.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(0x47), pinned.clone());
    let author = user(0xa1);
    let other = user(0xb2);
    install_test_uuid_sub_claim(&mut writer, author);
    install_test_uuid_sub_claim(&mut core, author);
    install_test_uuid_sub_claim(&mut core, other);
    publish_schema_lineage(
        &mut core,
        evolved_payload.clone(),
        MigrationLens::new(
            pinned.version_id(),
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
                        column: "extra_owner".to_owned(),
                        default: Value::Uuid(other.test_uuid()),
                    },
                    LensOp::RenameColumn {
                        from: "owner".to_owned(),
                        to: "owner_id".to_owned(),
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

    let readable_row = row(0x48);
    let (_accepted_tx, accepted_unit) = writer
        .commit_mergeable_unit_settled(
            MergeableCommit::new("todos", readable_row, 21)
                .made_by(author)
                .cells(BTreeMap::from([
                    ("name".to_owned(), v("allowed")),
                    ("extra_owner".to_owned(), Value::Uuid(other.test_uuid())),
                    ("owner_id".to_owned(), Value::Uuid(author.test_uuid())),
                ])),
        )
        .unwrap();
    let updates = core.apply_sync_message_settled(accepted_unit).unwrap();
    assert!(matches!(
        updates.as_slice(),
        [SyncMessage::FateUpdate {
            fate: Fate::Accepted,
            ..
        }]
    ));
    let shape = Query::from("todos").validate(&evolved).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    assert_eq!(
        core.query_rows_for_link(&shape, &binding, DurabilityTier::Global, author)
            .unwrap()
            .into_iter()
            .map(|row| row.row_uuid())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([readable_row])
    );
    assert!(
        core.query_rows_for_link(&shape, &binding, DurabilityTier::Global, other)
            .unwrap()
            .is_empty()
    );

    let denied_row = row(0x49);
    let (_denied_tx, denied_unit) = writer
        .commit_mergeable_unit_settled(
            MergeableCommit::new("todos", denied_row, 22)
                .made_by(author)
                .cells(BTreeMap::from([
                    ("name".to_owned(), v("denied")),
                    ("extra_owner".to_owned(), Value::Uuid(author.test_uuid())),
                    ("owner_id".to_owned(), Value::Uuid(other.test_uuid())),
                ])),
        )
        .unwrap();
    let updates = core.apply_sync_message_settled(denied_unit).unwrap();
    assert!(matches!(
        updates.as_slice(),
        [SyncMessage::FateUpdate {
            fate: Fate::Rejected(RejectionReason::AuthorizationDenied),
            ..
        }]
    ));
}

#[test]
fn registered_transform_column_identity_is_accepted_and_projected() {
    let base = schema();
    let evolved = catalogue_evolved_schema();
    let evolved_payload = SchemaVersion::new(evolved.clone());
    let (_dir, mut core) = open_node_with_schema(node(0x4a), base.clone());
    let old_row = row(0x4b);
    core.commit_mergeable_settled(
        MergeableCommit::new("todos", old_row, 30).cells(title_cells("stable-title")),
    )
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
                    LensOp::TransformColumn {
                        column: "title".to_owned(),
                        transform: "jazz.identity".to_owned(),
                    },
                    LensOp::AddColumn {
                        column: "body".to_owned(),
                        default: v("body-default"),
                    },
                ],
            }],
        ).expect("valid migration lens"),
        Vec::<String>::new(),
        Vec::<String>::new(),
    )
    .unwrap();

    let shape = Query::from("todos").validate(&evolved).unwrap();
    assert_eq!(
        core.query_rows(
            &shape,
            &shape.bind(BTreeMap::new()).unwrap(),
            DurabilityTier::Local,
        )
        .unwrap()
        .into_iter()
        .map(current_row_pair)
        .collect::<BTreeMap<_, _>>(),
        BTreeMap::from([(
            old_row,
            BTreeMap::from([
                ("title".to_owned(), v("stable-title")),
                ("body".to_owned(), v("body-default")),
            ]),
        )])
    );
}

#[test]
fn transform_column_rejects_unregistered_transform_at_publish() {
    let base = schema();
    let evolved = catalogue_evolved_schema();
    let evolved_payload = SchemaVersion::new(evolved.clone());
    let (_dir, mut core) = open_node_with_schema(node(0x4c), base.clone());
    let result = publish_schema_lineage(
        &mut core,
        evolved_payload.clone(),
        MigrationLens::new(
            base.version_id(),
            evolved_payload.id,
            vec![TableLens {
                source_table: "todos".to_owned(),
                target_table: "todos".to_owned(),
                ops: vec![
                    LensOp::TransformColumn {
                        column: "title".to_owned(),
                        transform: "unregistered".to_owned(),
                    },
                    LensOp::AddColumn {
                        column: "body".to_owned(),
                        default: v(""),
                    },
                ],
            }],
        ).expect("valid migration lens"),
        Vec::<String>::new(),
        Vec::<String>::new(),
    );
    assert!(matches!(
        result,
        Err(Error::InvalidCatalogueUpdate(
            "transform column is not registered"
        ))
    ));
}


#[test]
fn lens_parallel_materialization_oracle_matches_engine_reads_seeded() {
    let seeds = if let Ok(seed) = std::env::var("JAZZ_SEED") {
        vec![seed.parse::<u64>().expect("JAZZ_SEED must be a u64")]
    } else {
        let count = std::env::var("JAZZ_SEED_COUNT")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(8);
        (0..count).map(|idx| 0x5700_0000 + idx * 7919).collect()
    };
    for seed in seeds {
        run_lens_parallel_materialization_seed(seed);
    }
}

#[test]
fn local_writes_store_versions_under_current_write_schema_storage() {
    let base = schema();
    let evolved = catalogue_evolved_schema();
    let evolved_payload = SchemaVersion::new(evolved.clone());
    let (_dir, mut core) = open_node_with_schema(node(0x46), base.clone());

    let base_tx = core
        .commit_mergeable_settled(MergeableCommit::new("todos", row(0x46), 10).cells(title_cells("base")))
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
                ops: vec![LensOp::AddColumn {
                    column: "body".to_owned(),
                    default: v(""),
                }],
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
    let evolved_tx = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row(0x47), 11).cells(BTreeMap::from([
                ("title".to_owned(), v("evolved")),
                ("body".to_owned(), v("partition")),
            ])),
        )
        .unwrap();

    let base_history_table = physical_history_table_name(
        core.catalogue.physical_mappings[&base.version_id()].tables["todos"].table_id,
    );
    let evolved_history_table = physical_history_table_name(
        core.catalogue.physical_mappings[&evolved_payload.id].tables["todos"].table_id,
    );
    assert_eq!(base_history_table, evolved_history_table);
    let base_history = core
        .database
        .primary_key_scan_raw(&base_history_table, &[])
        .unwrap();
    let evolved_history = core
        .database
        .primary_key_scan_raw(&evolved_history_table, &[])
        .unwrap();
    assert_eq!(base_history.len(), 2);
    assert_eq!(evolved_history.len(), base_history.len());
    let stored_txs = core
        .query_table_versions("todos")
        .unwrap()
        .iter()
        .map(|version| core.version_tx_id(version).unwrap())
        .collect::<BTreeSet<_>>();
    assert_eq!(stored_txs, BTreeSet::from([base_tx, evolved_tx]));
}

#[test]
fn exclusive_writes_store_versions_under_current_write_schema_storage() {
    let base = schema();
    let evolved = catalogue_evolved_schema();
    let evolved_payload = SchemaVersion::new(evolved.clone());
    let (_dir, mut core) = open_node_with_schema(node(0x4a), base.clone());

    let base_tx = core
        .commit_mergeable_settled(MergeableCommit::new("todos", row(0x4a), 10).cells(title_cells("base")))
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
                ops: vec![LensOp::AddColumn {
                    column: "body".to_owned(),
                    default: v(""),
                }],
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

    let tx = OpenTransactionId::new();
    core.open_exclusive(tx).unwrap();
    core.tx_write(
        tx,
        "todos",
        row(0x4b),
        BTreeMap::from([
            ("title".to_owned(), v("exclusive")),
            ("body".to_owned(), v("partition")),
        ]),
        None,
    )
    .unwrap();
    let (exclusive_tx, _unit) = core.commit_exclusive_settled(tx, AuthorSubject::SYSTEM, 11).unwrap();

    let base_history_table = physical_history_table_name(
        core.catalogue.physical_mappings[&base.version_id()].tables["todos"].table_id,
    );
    let evolved_history_table = physical_history_table_name(
        core.catalogue.physical_mappings[&evolved_payload.id].tables["todos"].table_id,
    );
    assert_eq!(base_history_table, evolved_history_table);
    let base_history = core
        .database
        .primary_key_scan_raw(&base_history_table, &[])
        .unwrap();
    let evolved_history = core
        .database
        .primary_key_scan_raw(&evolved_history_table, &[])
        .unwrap();
    assert_eq!(base_history.len(), 2);
    assert_eq!(evolved_history.len(), base_history.len());
    let stored_txs = core
        .query_table_versions("todos")
        .unwrap()
        .iter()
        .map(|version| core.version_tx_id(version).unwrap())
        .collect::<BTreeSet<_>>();
    assert_eq!(stored_txs, BTreeSet::from([base_tx, exclusive_tx]));
}

#[test]
fn physical_schema_variants_survive_pointer_changes_and_reopen() {
    let base = schema();
    let evolved = catalogue_evolved_schema();
    let evolved_payload = SchemaVersion::new(evolved.clone());
    let (dir, mut core) = open_node_with_schema(node(0x48), base.clone());
    publish_schema_lineage(
        &mut core,
        evolved_payload.clone(),
        MigrationLens::new(
            base.version_id(),
            evolved_payload.id,
            vec![TableLens {
                source_table: "todos".to_owned(),
                target_table: "todos".to_owned(),
                ops: vec![LensOp::AddColumn {
                    column: "body".to_owned(),
                    default: v(""),
                }],
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
        MergeableCommit::new("todos", row(0x48), 10).cells(BTreeMap::from([
            ("title".to_owned(), v("historical")),
            ("body".to_owned(), v("kept")),
        ])),
    )
    .unwrap();
    core.apply_trusted_catalogue_message_settled(SyncMessage::SetCurrentWriteSchema {
        author: AuthorSubject::SYSTEM,
        pointer: CurrentWriteSchema {
            revision: 2,
            schema: base.version_id(),
        },
    })
    .unwrap();

    let evolved_table_id =
        core.catalogue.physical_mappings[&evolved_payload.id].tables["todos"].table_id;
    let evolved_history_table = physical_history_table_name(evolved_table_id);
    assert_eq!(
        core.database
            .primary_key_scan_raw(&evolved_history_table, &[])
            .unwrap()
            .len(),
        1
    );
    drop(core);

    let reopened = reopen_node_at(&dir, node(0x48), base);
    assert_eq!(
        reopened.catalogue.physical_mappings[&evolved_payload.id].tables["todos"].table_id,
        evolved_table_id
    );
    assert_eq!(
        reopened
            .database
            .primary_key_scan_raw(&evolved_history_table, &[])
            .unwrap()
            .len(),
        1
    );
}
