#[test]
fn schema_version_id_round_trips_through_wire_ingest_and_recovery() {
    let schema = schema();
    let expected_schema_version = schema.version_id();
    let (_writer_dir, mut writer) = open_node_with_schema(node(0x31), schema.clone());
    let (core_dir, mut core) = open_node_with_schema(node(0x32), schema.clone());

    let commit = MergeableCommit::new("todos", row(0x44), 1_000)
        .made_by(AuthorId::SYSTEM)
        .cells(BTreeMap::from([(
            "title".to_owned(),
            Value::String("lens hook".to_owned()),
        )]));
    let (_tx_id, unit) = writer.commit_mergeable_unit(commit).unwrap();
    let SyncMessage::CommitUnit { versions, .. } = &unit else {
        panic!("commit unit expected");
    };
    assert_eq!(versions.len(), 1);
    assert_eq!(versions[0].schema_version(), expected_schema_version);

    core.apply_sync_message(unit).unwrap();
    let versions = core.query_all_versions().unwrap();
    assert_eq!(versions.len(), 1);
    let wire = core.version_record_from_row(&versions[0]).unwrap();
    assert_eq!(wire.schema_version(), expected_schema_version);

    drop(core);
    let mut reopened = reopen_node_at(&core_dir, node(0x32), schema);
    let versions = reopened.query_all_versions().unwrap();
    assert_eq!(versions.len(), 1);
    let wire = reopened.version_record_from_row(&versions[0]).unwrap();
    assert_eq!(wire.schema_version(), expected_schema_version);
}
#[test]
fn catalogue_schema_publish_replicates_and_is_idempotent() {
    let base = schema();
    let evolved = catalogue_evolved_schema();
    let (_core_dir, mut core) = open_node_with_schema(node(0x33), base.clone());
    let (_client_dir, mut client) = open_node_with_schema(node(0x34), base);
    let payload = SchemaVersion::new(evolved.clone());
    let publish = SyncMessage::PublishSchema {
        author: AuthorId::SYSTEM,
        schema: Box::new(payload.clone()),
    };

    let ack = core.apply_sync_message(publish.clone()).unwrap();
    assert!(matches!(
        ack.as_slice(),
        [SyncMessage::CatalogueAck(crate::protocol::CatalogueAck {
            schema: Some(id),
            applied: true,
            ..
        })] if *id == payload.id
    ));
    assert!(core.catalogue_schemas().contains_key(&payload.id));

    let second = core.apply_sync_message(publish.clone()).unwrap();
    assert!(matches!(
        second.as_slice(),
        [SyncMessage::CatalogueAck(crate::protocol::CatalogueAck {
            schema: Some(id),
            applied: true,
            ..
        })] if *id == payload.id
    ));
    assert_eq!(core.catalogue_schemas().len(), 2);

    client.apply_sync_message(publish).unwrap();
    assert_eq!(
        client
            .catalogue_schemas()
            .get(&payload.id)
            .map(|schema| &schema.schema),
        Some(&evolved)
    );
}
#[test]
fn catalogue_lens_publish_validates_admin_id_and_known_endpoints() {
    let base = schema();
    let evolved = catalogue_evolved_schema();
    let (_dir, mut core) = open_node_with_schema(node(0x35), base.clone());
    let source = SchemaVersion::new(base);
    let target = SchemaVersion::new(evolved);
    core.apply_sync_message(SyncMessage::PublishSchema {
        author: AuthorId::SYSTEM,
        schema: Box::new(target.clone()),
    })
    .unwrap();
    let lens = MigrationLens::new(
        source.id,
        target.id,
        vec![TableLens {
            source_table: "todos".to_owned(),
            target_table: "todos".to_owned(),
            ops: vec![LensOp::DropColumn {
                column: "body".to_owned(),
                backwards_default: Value::String(String::new()),
            }],
        }],
    );

    let non_admin = core.apply_sync_message(SyncMessage::PublishLens {
        author: user(7),
        lens: lens.clone(),
    });
    assert!(matches!(non_admin, Err(Error::UnauthorizedCatalogueUpdate)));

    let unknown = MigrationLens::new(
        source.id,
        SchemaVersionId::from_bytes([0x99; 16]),
        Vec::new(),
    );
    let unknown_result = core.apply_sync_message(SyncMessage::PublishLens {
        author: AuthorId::SYSTEM,
        lens: unknown,
    });
    assert!(matches!(
        unknown_result,
        Err(Error::InvalidCatalogueUpdate("lens endpoint is unknown"))
    ));

    let ack = core
        .apply_sync_message(SyncMessage::PublishLens {
            author: AuthorId::SYSTEM,
            lens: lens.clone(),
        })
        .unwrap();
    assert!(matches!(
        ack.as_slice(),
        [SyncMessage::CatalogueAck(crate::protocol::CatalogueAck {
            lens: Some(id),
            applied: true,
            ..
        })] if *id == lens.id
    ));
    assert!(core.catalogue_lenses().contains_key(&lens.id));
}
#[test]
fn catalogue_arrival_drains_schema_orphan_commit_units() {
    let base = schema();
    let evolved = catalogue_evolved_schema();
    let evolved_id = evolved.version_id();
    let (_writer_dir, mut writer) = open_node_with_schema(node(0x36), base.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(0x37), base.clone());
    let (_tx_id, unit) = writer
        .commit_mergeable_unit(
            MergeableCommit::new("todos", row(0x55), 1_000).cells(title_cells("parked")),
        )
        .unwrap();
    let SyncMessage::CommitUnit { tx, versions } = unit else {
        panic!("commit unit expected");
    };
    let rewritten = versions
        .into_iter()
        .map(|version| {
            VersionRecord::from_cells(
                &base.tables[0],
                evolved_id,
                version.row_uuid(),
                version.parents(),
                &version_record_cells(&version, &base.tables[0]),
                version.deletion(),
            )
            .unwrap()
        })
        .collect::<Vec<_>>();

    assert!(core
        .apply_sync_message(SyncMessage::CommitUnit {
            tx: tx.clone(),
            versions: rewritten,
        })
        .unwrap()
        .is_empty());
    assert_eq!(core.sync_metrics().parked_catalogue_orphans, 1);
    assert!(core.query_transaction(tx.tx_id).unwrap().is_none());

    let updates = core
        .apply_sync_message(SyncMessage::PublishSchema {
            author: AuthorId::SYSTEM,
            schema: Box::new(SchemaVersion::new(evolved)),
        })
        .unwrap();
    assert_eq!(core.sync_metrics().parked_catalogue_orphans_resolved, 1);
    assert!(updates.iter().any(|message| matches!(
        message,
        SyncMessage::FateUpdate {
            tx_id,
            fate: Fate::Accepted,
            ..
        } if *tx_id == tx.tx_id
    )));
}
#[test]
fn catalogue_current_write_schema_revision_is_core_ordered() {
    let base = schema();
    let evolved = catalogue_evolved_schema();
    let evolved_payload = SchemaVersion::new(evolved);
    let (_dir, mut core) = open_node_with_schema(node(0x38), base.clone());
    core.apply_sync_message(SyncMessage::PublishSchema {
        author: AuthorId::SYSTEM,
        schema: Box::new(evolved_payload.clone()),
    })
    .unwrap();
    core.apply_sync_message(SyncMessage::SetCurrentWriteSchema {
        author: AuthorId::SYSTEM,
        pointer: CurrentWriteSchema {
            revision: 2,
            schema: evolved_payload.id,
        },
    })
    .unwrap();
    assert_eq!(core.current_write_schema().revision, 2);
    assert_eq!(core.current_write_schema().schema, evolved_payload.id);

    let stale = core.apply_sync_message(SyncMessage::SetCurrentWriteSchema {
        author: AuthorId::SYSTEM,
        pointer: CurrentWriteSchema {
            revision: 1,
            schema: base.version_id(),
        },
    });
    assert!(matches!(
        stale.unwrap().as_slice(),
        [SyncMessage::CatalogueAck(crate::protocol::CatalogueAck {
            revision: Some(1),
            applied: false,
            ..
        })]
    ));
    assert_eq!(core.current_write_schema().revision, 2);

    core.apply_sync_message(SyncMessage::SetCurrentWriteSchema {
        author: AuthorId::SYSTEM,
        pointer: CurrentWriteSchema {
            revision: 3,
            schema: base.version_id(),
        },
    })
    .unwrap();
    assert_eq!(core.current_write_schema().revision, 3);
    assert_eq!(core.current_write_schema().schema, base.version_id());
}
#[test]
fn durable_catalogue_values_pointer_and_partitions_survive_restart() {
    let base = schema();
    let evolved = catalogue_evolved_schema();
    let evolved_payload = SchemaVersion::new(evolved.clone());
    let (dir, mut core) = open_node_with_schema(node(0x39), base.clone());
    core.apply_sync_message(SyncMessage::PublishSchema {
        author: AuthorId::SYSTEM,
        schema: Box::new(evolved_payload.clone()),
    })
    .unwrap();
    let lens = MigrationLens::new(
        base.version_id(),
        evolved_payload.id,
        vec![TableLens {
            source_table: "todos".to_owned(),
            target_table: "todos".to_owned(),
            ops: vec![LensOp::AddColumn {
                column: "body".to_owned(),
                default: Value::String(String::new()),
            }],
        }],
    );
    core.apply_sync_message(SyncMessage::PublishLens {
        author: AuthorId::SYSTEM,
        lens: lens.clone(),
    })
    .unwrap();
    core.apply_sync_message(SyncMessage::SetCurrentWriteSchema {
        author: AuthorId::SYSTEM,
        pointer: CurrentWriteSchema {
            revision: 4,
            schema: evolved_payload.id,
        },
    })
    .unwrap();
    assert!(core
        .partitions()
        .contains(&("todos".to_owned(), evolved_payload.id)));
    drop(core);

    let reopened = reopen_node_at(&dir, node(0x39), base.clone());
    assert_eq!(
        reopened
            .catalogue_schemas()
            .get(&evolved_payload.id)
            .map(|schema| &schema.schema),
        Some(&evolved)
    );
    assert_eq!(reopened.catalogue_lenses().get(&lens.id), Some(&lens));
    assert_eq!(
        reopened.current_write_schema(),
        CurrentWriteSchema {
            revision: 4,
            schema: evolved_payload.id,
        }
    );
    assert!(reopened
        .partitions()
        .contains(&("todos".to_owned(), base.version_id())));
    assert!(reopened
        .partitions()
        .contains(&("todos".to_owned(), evolved_payload.id)));
}
#[test]
fn shape_registration_parks_until_schema_version_catalogue_arrives() {
    let base = schema();
    let evolved = JazzSchema::new([
        TableSchema::new("todos", [ColumnSchema::new("title", ColumnType::String)]),
        TableSchema::new("notes", [ColumnSchema::new("body", ColumnType::String)]),
    ]);
    let shape = Query::from("todos").validate(&evolved).unwrap();
    let (dir, mut core) = open_node_with_schema(node(0x3c), base);

    core.apply_sync_message(SyncMessage::RegisterShape {
        shape_id: shape.shape_id(),
        ast: crate::protocol::ShapeAst::from_validated(&shape),
        opts: crate::protocol::RegisterShapeOptions::default(),
    })
    .unwrap();
    assert_eq!(core.sync_metrics().parked_catalogue_shapes, 1);
    assert!(!core.query.registered_shapes.contains_key(&shape.shape_id()));

    core.apply_sync_message(SyncMessage::PublishSchema {
        author: AuthorId::SYSTEM,
        schema: Box::new(SchemaVersion::new(evolved)),
    })
    .unwrap();
    assert_eq!(core.sync_metrics().parked_catalogue_shapes_resolved, 1);
    assert!(core.query.registered_shapes.contains_key(&shape.shape_id()));

    drop(core);
    let reopened = reopen_node_at(&dir, node(0x3c), schema());
    assert!(reopened
        .catalogue_schemas()
        .contains_key(&shape.schema_version()));
}
#[test]
fn current_write_pointer_flip_reopens_with_new_partition_tables() {
    let base = schema();
    let evolved = catalogue_evolved_schema();
    let evolved_payload = SchemaVersion::new(evolved);
    let (_dir, mut core) = open_node_with_schema(node(0x3b), base);
    core.apply_sync_message(SyncMessage::PublishSchema {
        author: AuthorId::SYSTEM,
        schema: Box::new(evolved_payload.clone()),
    })
    .unwrap();
    core.apply_sync_message(SyncMessage::SetCurrentWriteSchema {
        author: AuthorId::SYSTEM,
        pointer: CurrentWriteSchema {
            revision: 1,
            schema: evolved_payload.id,
        },
    })
    .unwrap();

    let suffix = evolved_payload.id.0.simple();
    let history = format!("jazz_todos_{suffix}_history");
    let register = format!("jazz_todos_{suffix}_register");
    assert!(core.database.primary_key_scan_raw(&history, &[]).is_ok());
    assert!(core.database.primary_key_scan_raw(&register, &[]).is_ok());
}
#[test]
fn partitioned_reads_project_natural_lenses_after_schema_agnostic_winner() {
    let base = schema();
    let evolved = JazzSchema::new([TableSchema::new(
        "todos",
        [
            ColumnSchema::new("name", ColumnType::String),
            ColumnSchema::new("body", ColumnType::String),
        ],
    )]);
    let evolved_payload = SchemaVersion::new(evolved.clone());
    let (_dir, mut core) = open_node_with_schema(node(0x3d), base.clone());
    let old_row = row(0x41);
    core.commit_mergeable(
        MergeableCommit::new("todos", old_row, 10).cells(title_cells("old-title")),
    )
    .unwrap();
    core.apply_sync_message(SyncMessage::PublishSchema {
        author: AuthorId::SYSTEM,
        schema: Box::new(evolved_payload.clone()),
    })
    .unwrap();
    core.apply_sync_message(SyncMessage::PublishLens {
        author: AuthorId::SYSTEM,
        lens: MigrationLens::new(
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
        ),
    })
    .unwrap();
    core.apply_sync_message(SyncMessage::SetCurrentWriteSchema {
        author: AuthorId::SYSTEM,
        pointer: CurrentWriteSchema {
            revision: 1,
            schema: evolved_payload.id,
        },
    })
    .unwrap();
    let new_row = row(0x42);
    core.commit_mergeable(
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

    core.commit_mergeable(MergeableCommit::new("todos", new_row, 12).deletion(DeletionEvent::Deleted))
        .unwrap();
    let include_deleted_shape = Query::from("todos")
        .filter(eq(col("title"), param("wanted")))
        .validate(&base)
        .unwrap();
    let include_deleted_binding = include_deleted_shape
        .bind(BTreeMap::from([("wanted".to_owned(), v("new-name"))]))
        .unwrap();
    let include_deleted_rows = core
        .query_rows_including_deleted_for_identity(
            &include_deleted_shape,
            &include_deleted_binding,
            DurabilityTier::Local,
            None,
            AuthorId::SYSTEM,
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
fn lens_graph_uses_shortest_path_when_multiple_candidates_exist() {
    let v1 = schema();
    let v2 = catalogue_evolved_schema();
    let v3 = JazzSchema::new([TableSchema::new(
        "todos",
        [
            ColumnSchema::new("name", ColumnType::String),
            ColumnSchema::new("search_name", ColumnType::String),
        ],
    )]);
    let v2_payload = SchemaVersion::new(v2.clone());
    let v3_payload = SchemaVersion::new(v3.clone());
    let (_dir, mut core) = open_node_with_schema(node(0x3e), v1.clone());
    let old_row = row(0x4e);
    core.commit_mergeable(
        MergeableCommit::new("todos", old_row, 10).cells(title_cells("old-title")),
    )
    .unwrap();

    for payload in [&v2_payload, &v3_payload] {
        core.apply_sync_message(SyncMessage::PublishSchema {
            author: AuthorId::SYSTEM,
            schema: Box::new(payload.clone()),
        })
        .unwrap();
    }
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
    );
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
    );
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
    );
    for lens in [long_first, long_second, shortest] {
        core.apply_sync_message(SyncMessage::PublishLens {
            author: AuthorId::SYSTEM,
            lens,
        })
        .unwrap();
    }

    let v3_shape = Query::from("todos").validate(&v3).unwrap();
    assert_eq!(
        core.query_rows(
            &v3_shape,
            &v3_shape.bind(BTreeMap::new()).unwrap(),
            DurabilityTier::Local,
        )
        .unwrap()
        .into_iter()
        .map(current_row_pair)
        .collect::<BTreeMap<_, _>>(),
        BTreeMap::from([(
            old_row,
            BTreeMap::from([
                ("name".to_owned(), v("old-title")),
                ("search_name".to_owned(), v("via-shortest")),
            ]),
        )])
    );
}

#[test]
fn old_schema_commit_units_copy_on_write_into_current_schema_partition() {
    let base = schema();
    let evolved = JazzSchema::new([TableSchema::new(
        "todos",
        [
            ColumnSchema::new("name", ColumnType::String),
            ColumnSchema::new("body", ColumnType::String),
        ],
    )]);
    let evolved_payload = SchemaVersion::new(evolved.clone());
    let (_writer_dir, mut writer) = open_node_with_schema(node(0x43), base.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(0x44), base.clone());
    core.apply_sync_message(SyncMessage::PublishSchema {
        author: AuthorId::SYSTEM,
        schema: Box::new(evolved_payload.clone()),
    })
    .unwrap();
    core.apply_sync_message(SyncMessage::PublishLens {
        author: AuthorId::SYSTEM,
        lens: MigrationLens::new(
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
        ),
    })
    .unwrap();
    core.apply_sync_message(SyncMessage::SetCurrentWriteSchema {
        author: AuthorId::SYSTEM,
        pointer: CurrentWriteSchema {
            revision: 1,
            schema: evolved_payload.id,
        },
    })
    .unwrap();

    let old_row = row(0x45);
    let (_tx_id, unit) = writer
        .commit_mergeable_unit(
            MergeableCommit::new("todos", old_row, 12).cells(title_cells("old-writer")),
        )
        .unwrap();
    core.apply_sync_message(unit).unwrap();

    let stored = core.query_table_versions("todos").unwrap();
    assert_eq!(stored.len(), 1);
    let stored_wire = core.version_record_from_row(&stored[0]).unwrap();
    assert_eq!(stored_wire.schema_version(), evolved_payload.id);
    assert_eq!(
        version_record_cells(&stored_wire, &evolved.tables[0]),
        BTreeMap::from([
            ("name".to_owned(), v("old-writer")),
            ("body".to_owned(), v("default-body")),
        ])
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
    let evolved = JazzSchema::new([TableSchema::new(
        "todos",
        [
            ColumnSchema::new("name", ColumnType::String),
            ColumnSchema::new("extra_owner", ColumnType::Uuid),
            ColumnSchema::new("owner_id", ColumnType::Uuid),
        ],
    )]);
    let evolved_payload = SchemaVersion::new(evolved.clone());
    let (_writer_dir, mut writer) = open_node_with_schema(node(0x46), evolved.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(0x47), pinned.clone());
    let author = user(0xa1);
    let other = user(0xb2);
    core.apply_sync_message(SyncMessage::PublishSchema {
        author: AuthorId::SYSTEM,
        schema: Box::new(evolved_payload.clone()),
    })
    .unwrap();
    core.apply_sync_message(SyncMessage::PublishLens {
        author: AuthorId::SYSTEM,
        lens: MigrationLens::new(
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
                        default: Value::Uuid(other.0),
                    },
                    LensOp::RenameColumn {
                        from: "owner".to_owned(),
                        to: "owner_id".to_owned(),
                    },
                ],
            }],
        ),
    })
    .unwrap();
    core.apply_sync_message(SyncMessage::SetCurrentWriteSchema {
        author: AuthorId::SYSTEM,
        pointer: CurrentWriteSchema {
            revision: 1,
            schema: evolved_payload.id,
        },
    })
    .unwrap();

    let readable_row = row(0x48);
    let (accepted_tx, accepted_unit) = writer
        .commit_mergeable_unit(
            MergeableCommit::new("todos", readable_row, 21)
                .made_by(author)
                .cells(BTreeMap::from([
                    ("name".to_owned(), v("allowed")),
                    ("extra_owner".to_owned(), Value::Uuid(other.0)),
                    ("owner_id".to_owned(), Value::Uuid(author.0)),
                ])),
        )
        .unwrap();
    let updates = core.apply_sync_message(accepted_unit).unwrap();
    assert!(matches!(
        updates.as_slice(),
        [SyncMessage::FateUpdate {
            fate: Fate::Accepted,
            ..
        }]
    ));
    assert!(core
        .result_set_entry_read_policy_allows("todos", readable_row, accepted_tx, author)
        .unwrap());
    assert!(!core
        .result_set_entry_read_policy_allows("todos", readable_row, accepted_tx, other)
        .unwrap());

    let denied_row = row(0x49);
    let (_denied_tx, denied_unit) = writer
        .commit_mergeable_unit(
            MergeableCommit::new("todos", denied_row, 22)
                .made_by(author)
                .cells(BTreeMap::from([
                    ("name".to_owned(), v("denied")),
                    ("extra_owner".to_owned(), Value::Uuid(author.0)),
                    ("owner_id".to_owned(), Value::Uuid(other.0)),
                ])),
        )
        .unwrap();
    let updates = core.apply_sync_message(denied_unit).unwrap();
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
    core.commit_mergeable(
        MergeableCommit::new("todos", old_row, 30).cells(title_cells("stable-title")),
    )
    .unwrap();
    core.apply_sync_message(SyncMessage::PublishSchema {
        author: AuthorId::SYSTEM,
        schema: Box::new(evolved_payload.clone()),
    })
    .unwrap();
    core.apply_sync_message(SyncMessage::PublishLens {
        author: AuthorId::SYSTEM,
        lens: MigrationLens::new(
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
        ),
    })
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
    core.apply_sync_message(SyncMessage::PublishSchema {
        author: AuthorId::SYSTEM,
        schema: Box::new(evolved_payload.clone()),
    })
    .unwrap();
    let result = core.apply_sync_message(SyncMessage::PublishLens {
        author: AuthorId::SYSTEM,
        lens: MigrationLens::new(
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
        ),
    });
    assert!(matches!(
        result,
        Err(Error::InvalidCatalogueUpdate(
            "transform column is not registered"
        ))
    ));
}

#[test]
fn transform_column_rejects_large_value_content_transform_at_publish() {
    let base = JazzSchema::new([TableSchema::new(
        "docs",
        [crate::schema::ColumnSchema::text("body")],
    )]);
    let evolved = JazzSchema::new([TableSchema::new(
        "docs",
        [
            crate::schema::ColumnSchema::text("body"),
            crate::schema::ColumnSchema::new("title", ColumnType::String),
        ],
    )]);
    let evolved_payload = SchemaVersion::new(evolved.clone());
    let (_dir, mut core) = open_node_with_schema(node(0x4d), base.clone());
    core.apply_sync_message(SyncMessage::PublishSchema {
        author: AuthorId::SYSTEM,
        schema: Box::new(evolved_payload.clone()),
    })
    .unwrap();
    let result = core.apply_sync_message(SyncMessage::PublishLens {
        author: AuthorId::SYSTEM,
        lens: MigrationLens::new(
            base.version_id(),
            evolved_payload.id,
            vec![TableLens {
                source_table: "docs".to_owned(),
                target_table: "docs".to_owned(),
                ops: vec![
                    LensOp::TransformColumn {
                        column: "body".to_owned(),
                        transform: "jazz.identity".to_owned(),
                    },
                    LensOp::AddColumn {
                        column: "title".to_owned(),
                        default: v(""),
                    },
                ],
            }],
        ),
    });
    assert!(matches!(
        result,
        Err(Error::InvalidCatalogueUpdate(
            "large-value columns cannot be content-transformed"
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
        .commit_mergeable(MergeableCommit::new("todos", row(0x46), 10).cells(title_cells("base")))
        .unwrap();
    core.apply_sync_message(SyncMessage::PublishSchema {
        author: AuthorId::SYSTEM,
        schema: Box::new(evolved_payload.clone()),
    })
    .unwrap();
    core.apply_sync_message(SyncMessage::SetCurrentWriteSchema {
        author: AuthorId::SYSTEM,
        pointer: CurrentWriteSchema {
            revision: 1,
            schema: evolved_payload.id,
        },
    })
    .unwrap();
    let evolved_tx = core
        .commit_mergeable(
            MergeableCommit::new("todos", row(0x47), 11).cells(BTreeMap::from([
                ("title".to_owned(), v("evolved")),
                ("body".to_owned(), v("partition")),
            ])),
        )
        .unwrap();

    let base_history = core
        .database
        .primary_key_scan_raw("jazz_todos_history", &[])
        .unwrap();
    let suffix = evolved_payload.id.0.simple();
    let evolved_history = core
        .database
        .primary_key_scan_raw(&format!("jazz_todos_{suffix}_history"), &[])
        .unwrap();
    assert_eq!(base_history.len(), 1);
    assert_eq!(evolved_history.len(), 1);
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
        .commit_mergeable(MergeableCommit::new("todos", row(0x4a), 10).cells(title_cells("base")))
        .unwrap();
    core.apply_sync_message(SyncMessage::PublishSchema {
        author: AuthorId::SYSTEM,
        schema: Box::new(evolved_payload.clone()),
    })
    .unwrap();
    core.apply_sync_message(SyncMessage::SetCurrentWriteSchema {
        author: AuthorId::SYSTEM,
        pointer: CurrentWriteSchema {
            revision: 1,
            schema: evolved_payload.id,
        },
    })
    .unwrap();

    let tx = core.open_exclusive().unwrap();
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
    let (exclusive_tx, _unit) = core.commit_exclusive(tx, AuthorId::SYSTEM, 11).unwrap();

    let base_history = core
        .database
        .primary_key_scan_raw("jazz_todos_history", &[])
        .unwrap();
    let suffix = evolved_payload.id.0.simple();
    let evolved_history = core
        .database
        .primary_key_scan_raw(&format!("jazz_todos_{suffix}_history"), &[])
        .unwrap();
    assert_eq!(base_history.len(), 1);
    assert_eq!(evolved_history.len(), 1);
    let stored_txs = core
        .query_table_versions("todos")
        .unwrap()
        .iter()
        .map(|version| core.version_tx_id(version).unwrap())
        .collect::<BTreeSet<_>>();
    assert_eq!(stored_txs, BTreeSet::from([base_tx, exclusive_tx]));
}

#[test]
fn schema_version_partition_tables_survive_pointer_changes_and_reopen() {
    let base = schema();
    let evolved = catalogue_evolved_schema();
    let evolved_payload = SchemaVersion::new(evolved.clone());
    let (dir, mut core) = open_node_with_schema(node(0x48), base.clone());
    core.apply_sync_message(SyncMessage::PublishSchema {
        author: AuthorId::SYSTEM,
        schema: Box::new(evolved_payload.clone()),
    })
    .unwrap();
    core.apply_sync_message(SyncMessage::SetCurrentWriteSchema {
        author: AuthorId::SYSTEM,
        pointer: CurrentWriteSchema {
            revision: 1,
            schema: evolved_payload.id,
        },
    })
    .unwrap();
    core.commit_mergeable(
        MergeableCommit::new("todos", row(0x48), 10).cells(BTreeMap::from([
            ("title".to_owned(), v("historical")),
            ("body".to_owned(), v("kept")),
        ])),
    )
    .unwrap();
    core.apply_sync_message(SyncMessage::SetCurrentWriteSchema {
        author: AuthorId::SYSTEM,
        pointer: CurrentWriteSchema {
            revision: 2,
            schema: base.version_id(),
        },
    })
    .unwrap();

    let suffix = evolved_payload.id.0.simple();
    assert_eq!(
        core.database
            .primary_key_scan_raw(&format!("jazz_todos_{suffix}_history"), &[])
            .unwrap()
            .len(),
        1
    );
    drop(core);

    let reopened = reopen_node_at(&dir, node(0x48), base);
    assert!(reopened
        .partitions()
        .contains(&("todos".to_owned(), evolved_payload.id)));
    assert_eq!(
        reopened
            .database
            .primary_key_scan_raw(&format!("jazz_todos_{suffix}_history"), &[])
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn partitioned_schema_projected_reads_use_projected_current_source_without_prepared_fallback() {
    let base = schema();
    let evolved = JazzSchema::new([TableSchema::new(
        "todos",
        [
            ColumnSchema::new("name", ColumnType::String),
            ColumnSchema::new("body", ColumnType::String),
        ],
    )]);
    let evolved_payload = SchemaVersion::new(evolved.clone());
    let (_dir, mut core) = open_node_with_schema(node(0x49), base.clone());
    core.apply_sync_message(SyncMessage::PublishSchema {
        author: AuthorId::SYSTEM,
        schema: Box::new(evolved_payload.clone()),
    })
    .unwrap();
    core.apply_sync_message(SyncMessage::PublishLens {
        author: AuthorId::SYSTEM,
        lens: MigrationLens::new(
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
        ),
    })
    .unwrap();
    core.apply_sync_message(SyncMessage::SetCurrentWriteSchema {
        author: AuthorId::SYSTEM,
        pointer: CurrentWriteSchema {
            revision: 1,
            schema: evolved_payload.id,
        },
    })
    .unwrap();
    core.commit_mergeable(
        MergeableCommit::new("todos", row(0x49), 10).cells(BTreeMap::from([
            ("name".to_owned(), v("projected")),
            ("body".to_owned(), v("partition")),
        ])),
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
    let rows = core
        .query_rows(&shape, &binding, DurabilityTier::Local)
        .unwrap();

    assert_eq!(rows.into_iter().map(current_row_pair).collect::<BTreeMap<_, _>>(), BTreeMap::from([(row(0x49), title_cells("projected"))]));
    assert!(
        !core.query.query_shape_cache
            .keys()
            .any(|(shape_id, _)| *shape_id == shape.shape_id()),
        "partitioned/schema-projected reads must not install prepared groove plans"
    );

    let join_base = JazzSchema::new([
        TableSchema::new("todos", [ColumnSchema::new("title", ColumnType::String)]),
        TableSchema::new(
            "todo_members",
            [
                ColumnSchema::new("todo", ColumnType::Uuid),
                ColumnSchema::new("member", ColumnType::Uuid),
            ],
        )
        .with_reference("todo", "todos"),
    ]);
    let join_evolved = SchemaVersion::new(JazzSchema::new([
        TableSchema::new(
            "todos",
            [
                ColumnSchema::new("title", ColumnType::String),
                ColumnSchema::new("body", ColumnType::String),
            ],
        ),
        TableSchema::new(
            "todo_members",
            [
                ColumnSchema::new("todo", ColumnType::Uuid),
                ColumnSchema::new("member", ColumnType::Uuid),
            ],
        )
        .with_reference("todo", "todos"),
    ]));
    let (_join_dir, mut join_core) = open_node_with_schema(node(0x4d), join_base.clone());
    join_core
        .apply_sync_message(SyncMessage::PublishSchema {
            author: AuthorId::SYSTEM,
            schema: Box::new(join_evolved.clone()),
        })
        .unwrap();
    join_core
        .apply_sync_message(SyncMessage::PublishLens {
            author: AuthorId::SYSTEM,
            lens: MigrationLens::new(
                join_base.version_id(),
                join_evolved.id,
                vec![TableLens {
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
                }],
            ),
        })
        .unwrap();
    join_core
        .apply_sync_message(SyncMessage::SetCurrentWriteSchema {
            author: AuthorId::SYSTEM,
            pointer: CurrentWriteSchema {
                revision: 1,
                schema: join_evolved.id,
            },
        })
        .unwrap();
    join_core
        .commit_mergeable(
            MergeableCommit::new("todos", row(0x4d), 20).cells(BTreeMap::from([
                ("title".to_owned(), v("joined")),
                ("body".to_owned(), v("projected-body")),
            ])),
        )
        .unwrap();
    join_core
        .commit_mergeable(
            MergeableCommit::new("todo_members", row(0x4e), 21).cells(BTreeMap::from([
                ("todo".to_owned(), Value::Uuid(row(0x4d).0)),
                ("member".to_owned(), Value::Uuid(row(0x4d).0)),
            ])),
        )
        .unwrap();
    let projected_join = Query::from("todos")
        .join_via(
            "todo_members",
            "todo",
            [eq(col("member"), param("wanted"))],
        )
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
        rows.into_iter().map(current_row_pair).collect::<BTreeMap<_, _>>(),
        BTreeMap::from([(row(0x4d), title_cells("joined"))])
    );
}

#[test]
fn schema_projected_reads_ignore_settled_result_set_materialization_cache() {
    let base = schema();
    let evolved = JazzSchema::new([TableSchema::new(
        "todos",
        [
            ColumnSchema::new("name", ColumnType::String),
            ColumnSchema::new("body", ColumnType::String),
        ],
    )]);
    let evolved_payload = SchemaVersion::new(evolved);
    let (_dir, mut core) = open_node_with_schema(node(0x4c), base.clone());
    core.apply_sync_message(SyncMessage::PublishSchema {
        author: AuthorId::SYSTEM,
        schema: Box::new(evolved_payload.clone()),
    })
    .unwrap();
    core.apply_sync_message(SyncMessage::PublishLens {
        author: AuthorId::SYSTEM,
        lens: MigrationLens::new(
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
        ),
    })
    .unwrap();
    core.apply_sync_message(SyncMessage::SetCurrentWriteSchema {
        author: AuthorId::SYSTEM,
        pointer: CurrentWriteSchema {
            revision: 1,
            schema: evolved_payload.id,
        },
    })
    .unwrap();

    let tx_id = core
        .commit_mergeable(
            MergeableCommit::new("todos", row(0x4c), 10).cells(BTreeMap::from([
                ("name".to_owned(), v("projected")),
                ("body".to_owned(), v("cache-guard")),
            ])),
        )
        .unwrap();
    core.apply_fate_update(
        tx_id,
        Fate::Accepted,
        Some(GlobalSeq(1)),
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
        crate::protocol::SubscriptionKey {
            shape_id: shape.shape_id(),
            binding_id: binding.binding_id(),
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
    let base = JazzSchema::new([
        TableSchema::new("docs", [ColumnSchema::new("title", ColumnType::String)]),
        TableSchema::new("teams", [ColumnSchema::new("name", ColumnType::String)]),
        TableSchema::new(
            "teamEdges",
            [
                ColumnSchema::new("member", ColumnType::Uuid),
                ColumnSchema::new("parent", ColumnType::Uuid),
                ColumnSchema::new("edge_kind", ColumnType::String),
            ],
        )
        .with_reference("member", "teams")
        .with_reference("parent", "teams"),
        TableSchema::new(
            "teamAccess",
            [
                ColumnSchema::new("doc", ColumnType::Uuid),
                ColumnSchema::new("team", ColumnType::Uuid),
                ColumnSchema::new("access_kind", ColumnType::String),
            ],
        )
        .with_reference("doc", "docs")
        .with_reference("team", "teams"),
    ]);
    let evolved = JazzSchema::new([
        TableSchema::new("docs", [ColumnSchema::new("title", ColumnType::String)]),
        TableSchema::new("teams", [ColumnSchema::new("name", ColumnType::String)]),
        TableSchema::new(
            "teamEdges",
            [
                ColumnSchema::new("member", ColumnType::Uuid),
                ColumnSchema::new("parent", ColumnType::Uuid),
                ColumnSchema::new("edge_label", ColumnType::String),
            ],
        )
        .with_reference("member", "teams")
        .with_reference("parent", "teams"),
        TableSchema::new(
            "teamAccess",
            [
                ColumnSchema::new("doc", ColumnType::Uuid),
                ColumnSchema::new("team", ColumnType::Uuid),
                ColumnSchema::new("access_label", ColumnType::String),
            ],
        )
        .with_reference("doc", "docs")
        .with_reference("team", "teams"),
    ]);
    let evolved_payload = SchemaVersion::new(evolved);
    let (_dir, mut core) = open_node_with_schema(node(0x4f), base.clone());
    core.apply_sync_message(SyncMessage::PublishSchema {
        author: AuthorId::SYSTEM,
        schema: Box::new(evolved_payload.clone()),
    })
    .unwrap();
    core.apply_sync_message(SyncMessage::PublishLens {
        author: AuthorId::SYSTEM,
        lens: MigrationLens::new(
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
        ),
    })
    .unwrap();
    core.apply_sync_message(SyncMessage::SetCurrentWriteSchema {
        author: AuthorId::SYSTEM,
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
        core.commit_mergeable(
            MergeableCommit::new("teams", row(idx), idx as u64).cells(BTreeMap::from([(
                "name".to_owned(),
                v(format!("team-{idx}")),
            )])),
        )
        .unwrap();
    }
    core.commit_mergeable(
        MergeableCommit::new("docs", row(0xd1), 20).cells(BTreeMap::from([(
            "title".to_owned(),
            v("reachable"),
        )])),
    )
    .unwrap();
    core.commit_mergeable(
        MergeableCommit::new("teamAccess", row(0xa1), 21).cells(BTreeMap::from([
            ("doc".to_owned(), Value::Uuid(row(0xd1).0)),
            ("team".to_owned(), Value::Uuid(team3.0)),
            ("access_label".to_owned(), v("allow")),
        ])),
    )
    .unwrap();
    for (idx, member, parent) in [(0xe1, team1, team2), (0xe2, team2, team3)] {
        core.commit_mergeable(
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
            ("team".to_owned(), Value::Uuid(team1.0)),
            ("access_kind".to_owned(), v("allow")),
            ("edge_kind".to_owned(), v("active")),
        ]))
        .unwrap();
    let rows = core
        .query_rows(&shape, &binding, DurabilityTier::Local)
        .unwrap();

    assert_eq!(
        rows.into_iter().map(current_row_pair).collect::<BTreeMap<_, _>>(),
        BTreeMap::from([(row(0xd1), title_cells("reachable"))])
    );
    assert!(
        !core.query
            .query_shape_cache
            .keys()
            .any(|(shape_id, _)| *shape_id == shape.shape_id()),
        "schema-projected reachable reads must lower over inline projected sources"
    );
}

#[test]
fn include_deleted_schema_projected_root_filters_translate_old_names() {
    let base = schema();
    let evolved = SchemaVersion::new(JazzSchema::new([TableSchema::new(
        "todos",
        [
            ColumnSchema::new("name", ColumnType::String),
            ColumnSchema::new("body", ColumnType::String),
        ],
    )]));
    let (_dir, mut core) = open_node_with_schema(node(0x59), base.clone());
    core.apply_sync_message(SyncMessage::PublishSchema {
        author: AuthorId::SYSTEM,
        schema: Box::new(evolved.clone()),
    })
    .unwrap();
    core.apply_sync_message(SyncMessage::PublishLens {
        author: AuthorId::SYSTEM,
        lens: MigrationLens::new(
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
        ),
    })
    .unwrap();
    core.apply_sync_message(SyncMessage::SetCurrentWriteSchema {
        author: AuthorId::SYSTEM,
        pointer: CurrentWriteSchema {
            revision: 1,
            schema: evolved.id,
        },
    })
    .unwrap();

    core.commit_mergeable(
        MergeableCommit::new("todos", row(0x59), 10).cells(BTreeMap::from([
            ("name".to_owned(), v("deleted-root")),
            ("body".to_owned(), v("projected-body")),
        ])),
    )
    .unwrap();
    core.commit_mergeable(MergeableCommit::new("todos", row(0x59), 11).deletion(DeletionEvent::Deleted))
        .unwrap();

    let shape = Query::from("todos")
        .filter(eq(col("title"), param("wanted")))
        .validate(&base)
        .unwrap();
    let binding = shape
        .bind(BTreeMap::from([("wanted".to_owned(), v("deleted-root"))]))
        .unwrap();
    let rows = core
        .query_rows_including_deleted_for_identity(
            &shape,
            &binding,
            DurabilityTier::Local,
            None,
            AuthorId::SYSTEM,
        )
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].row_uuid(), row(0x59));
    assert!(rows[0].is_deleted());
    assert_eq!(rows[0].cell(&base.tables[0], "title"), Some(v("deleted-root")));
}

#[test]
fn include_deleted_schema_projected_join_filters_translate_old_names() {
    let base = JazzSchema::new([
        TableSchema::new("issues", [ColumnSchema::new("title", ColumnType::String)]),
        TableSchema::new(
            "issue_tags",
            [
                ColumnSchema::new("issue", ColumnType::Uuid),
                ColumnSchema::new("tag_kind", ColumnType::String),
            ],
        )
        .with_reference("issue", "issues"),
    ]);
    let evolved = SchemaVersion::new(JazzSchema::new([
        TableSchema::new("issues", [ColumnSchema::new("name", ColumnType::String)]),
        TableSchema::new(
            "issue_tags",
            [
                ColumnSchema::new("issue", ColumnType::Uuid),
                ColumnSchema::new("tag_label", ColumnType::String),
            ],
        )
        .with_reference("issue", "issues"),
    ]));
    let (_dir, mut core) = open_node_with_schema(node(0x5a), base.clone());
    core.apply_sync_message(SyncMessage::PublishSchema {
        author: AuthorId::SYSTEM,
        schema: Box::new(evolved.clone()),
    })
    .unwrap();
    core.apply_sync_message(SyncMessage::PublishLens {
        author: AuthorId::SYSTEM,
        lens: MigrationLens::new(
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
        ),
    })
    .unwrap();
    core.apply_sync_message(SyncMessage::SetCurrentWriteSchema {
        author: AuthorId::SYSTEM,
        pointer: CurrentWriteSchema {
            revision: 1,
            schema: evolved.id,
        },
    })
    .unwrap();

    let issue = row(0x5a);
    core.commit_mergeable(
        MergeableCommit::new("issues", issue, 10)
            .cells(BTreeMap::from([("name".to_owned(), v("joined"))])),
    )
    .unwrap();
    core.commit_mergeable(MergeableCommit::new("issues", issue, 11).deletion(DeletionEvent::Deleted))
        .unwrap();
    core.commit_mergeable(
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
    let binding = shape.bind(BTreeMap::from([("tag".to_owned(), v("bug"))])).unwrap();
    let rows = core
        .query_rows_including_deleted_for_identity(
            &shape,
            &binding,
            DurabilityTier::Local,
            None,
            AuthorId::SYSTEM,
        )
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].row_uuid(), issue);
    assert!(rows[0].is_deleted());
}

#[test]
fn include_deleted_schema_projected_reachable_filters_translate_old_names() {
    let base = JazzSchema::new([
        TableSchema::new("docs", [ColumnSchema::new("title", ColumnType::String)]),
        TableSchema::new("teams", [ColumnSchema::new("name", ColumnType::String)]),
        TableSchema::new(
            "team_edges",
            [
                ColumnSchema::new("member", ColumnType::Uuid),
                ColumnSchema::new("parent", ColumnType::Uuid),
                ColumnSchema::new("edge_kind", ColumnType::String),
            ],
        )
        .with_reference("member", "teams")
        .with_reference("parent", "teams"),
        TableSchema::new(
            "team_access",
            [
                ColumnSchema::new("doc", ColumnType::Uuid),
                ColumnSchema::new("team", ColumnType::Uuid),
                ColumnSchema::new("access_kind", ColumnType::String),
            ],
        )
        .with_reference("doc", "docs")
        .with_reference("team", "teams"),
    ]);
    let evolved = SchemaVersion::new(JazzSchema::new([
        TableSchema::new("docs", [ColumnSchema::new("name", ColumnType::String)]),
        TableSchema::new("teams", [ColumnSchema::new("name", ColumnType::String)]),
        TableSchema::new(
            "team_edges",
            [
                ColumnSchema::new("member", ColumnType::Uuid),
                ColumnSchema::new("parent", ColumnType::Uuid),
                ColumnSchema::new("edge_label", ColumnType::String),
            ],
        )
        .with_reference("member", "teams")
        .with_reference("parent", "teams"),
        TableSchema::new(
            "team_access",
            [
                ColumnSchema::new("doc", ColumnType::Uuid),
                ColumnSchema::new("team", ColumnType::Uuid),
                ColumnSchema::new("access_label", ColumnType::String),
            ],
        )
        .with_reference("doc", "docs")
        .with_reference("team", "teams"),
    ]));
    let (_dir, mut core) = open_node_with_schema(node(0x5c), base.clone());
    core.apply_sync_message(SyncMessage::PublishSchema {
        author: AuthorId::SYSTEM,
        schema: Box::new(evolved.clone()),
    })
    .unwrap();
    core.apply_sync_message(SyncMessage::PublishLens {
        author: AuthorId::SYSTEM,
        lens: MigrationLens::new(
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
        ),
    })
    .unwrap();
    core.apply_sync_message(SyncMessage::SetCurrentWriteSchema {
        author: AuthorId::SYSTEM,
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
        core.commit_mergeable(
            MergeableCommit::new("teams", team, idx).cells(BTreeMap::from([(
                "name".to_owned(),
                v(format!("team-{idx}")),
            )])),
        )
        .unwrap();
    }
    core.commit_mergeable(
        MergeableCommit::new("docs", doc, 12)
            .cells(BTreeMap::from([("name".to_owned(), v("reachable"))])),
    )
    .unwrap();
    core.commit_mergeable(MergeableCommit::new("docs", doc, 13).deletion(DeletionEvent::Deleted))
        .unwrap();
    core.commit_mergeable(
        MergeableCommit::new("team_edges", row(0x5f), 14).cells(BTreeMap::from([
            ("member".to_owned(), Value::Uuid(team1.0)),
            ("parent".to_owned(), Value::Uuid(team2.0)),
            ("edge_label".to_owned(), v("active")),
        ])),
    )
    .unwrap();
    core.commit_mergeable(
        MergeableCommit::new("team_access", row(0x60), 15).cells(BTreeMap::from([
            ("doc".to_owned(), Value::Uuid(doc.0)),
            ("team".to_owned(), Value::Uuid(team2.0)),
            ("access_label".to_owned(), v("allow")),
        ])),
    )
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
        .query_rows_including_deleted_for_identity(
            &shape,
            &binding,
            DurabilityTier::Local,
            None,
            AuthorId::SYSTEM,
        )
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].row_uuid(), doc);
    assert!(rows[0].is_deleted());
}

#[test]
fn historical_schema_projected_reads_use_projected_snapshot_source() {
    let base = schema();
    let evolved = JazzSchema::new([TableSchema::new(
        "todos",
        [
            ColumnSchema::new("name", ColumnType::String),
            ColumnSchema::new("body", ColumnType::String),
        ],
    )]);
    let evolved_payload = SchemaVersion::new(evolved);
    let (_dir, mut core) = open_node_with_schema(node(0x54), base.clone());
    core.apply_sync_message(SyncMessage::PublishSchema {
        author: AuthorId::SYSTEM,
        schema: Box::new(evolved_payload.clone()),
    })
    .unwrap();
    core.apply_sync_message(SyncMessage::PublishLens {
        author: AuthorId::SYSTEM,
        lens: MigrationLens::new(
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
        ),
    })
    .unwrap();
    core.apply_sync_message(SyncMessage::SetCurrentWriteSchema {
        author: AuthorId::SYSTEM,
        pointer: CurrentWriteSchema {
            revision: 1,
            schema: evolved_payload.id,
        },
    })
    .unwrap();

    let tx_id = core
        .commit_mergeable(
            MergeableCommit::new("todos", row(0x54), 10).cells(BTreeMap::from([
                ("name".to_owned(), v("historical")),
                ("body".to_owned(), v("projected-body")),
            ])),
        )
        .unwrap();
    core.apply_fate_update(
        tx_id,
        Fate::Accepted,
        Some(GlobalSeq(1)),
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
        core.query_rows_at(&shape, &binding, GlobalSeq(0))
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        core.query_rows_at(&unfiltered, &unfiltered_binding, GlobalSeq(1))
            .unwrap()
            .into_iter()
            .map(current_row_pair)
            .collect::<BTreeMap<_, _>>(),
        BTreeMap::from([(row(0x54), title_cells("historical"))])
    );
    let rows = core.query_rows_at(&shape, &binding, GlobalSeq(1)).unwrap();
    assert_eq!(
        rows.into_iter().map(current_row_pair).collect::<BTreeMap<_, _>>(),
        BTreeMap::from([(row(0x54), title_cells("historical"))])
    );
}

#[test]
fn historical_schema_projected_reachable_filters_translate_old_names() {
    let base = JazzSchema::new([
        TableSchema::new("docs", [ColumnSchema::new("title", ColumnType::String)]),
        TableSchema::new("teams", [ColumnSchema::new("name", ColumnType::String)]),
        TableSchema::new(
            "teamEdges",
            [
                ColumnSchema::new("member", ColumnType::Uuid),
                ColumnSchema::new("parent", ColumnType::Uuid),
                ColumnSchema::new("edge_kind", ColumnType::String),
            ],
        )
        .with_reference("member", "teams")
        .with_reference("parent", "teams"),
        TableSchema::new(
            "teamAccess",
            [
                ColumnSchema::new("doc", ColumnType::Uuid),
                ColumnSchema::new("team", ColumnType::Uuid),
                ColumnSchema::new("access_kind", ColumnType::String),
            ],
        )
        .with_reference("doc", "docs")
        .with_reference("team", "teams"),
    ]);
    let evolved = JazzSchema::new([
        TableSchema::new("docs", [ColumnSchema::new("name", ColumnType::String)]),
        TableSchema::new("teams", [ColumnSchema::new("name", ColumnType::String)]),
        TableSchema::new(
            "teamEdges",
            [
                ColumnSchema::new("member", ColumnType::Uuid),
                ColumnSchema::new("parent", ColumnType::Uuid),
                ColumnSchema::new("edge_label", ColumnType::String),
            ],
        )
        .with_reference("member", "teams")
        .with_reference("parent", "teams"),
        TableSchema::new(
            "teamAccess",
            [
                ColumnSchema::new("doc", ColumnType::Uuid),
                ColumnSchema::new("team", ColumnType::Uuid),
                ColumnSchema::new("access_label", ColumnType::String),
            ],
        )
        .with_reference("doc", "docs")
        .with_reference("team", "teams"),
    ]);
    let evolved_payload = SchemaVersion::new(evolved);
    let (_dir, mut core) = open_node_with_schema(node(0x55), base.clone());
    core.apply_sync_message(SyncMessage::PublishSchema {
        author: AuthorId::SYSTEM,
        schema: Box::new(evolved_payload.clone()),
    })
    .unwrap();
    core.apply_sync_message(SyncMessage::PublishLens {
        author: AuthorId::SYSTEM,
        lens: MigrationLens::new(
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
        ),
    })
    .unwrap();
    core.apply_sync_message(SyncMessage::SetCurrentWriteSchema {
        author: AuthorId::SYSTEM,
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
            .commit_mergeable(
                MergeableCommit::new("teams", row(idx), idx as u64).cells(BTreeMap::from([(
                    "name".to_owned(),
                    v(format!("team-{idx}")),
                )])),
            )
            .unwrap();
        core.apply_fate_update(
            tx_id,
            Fate::Accepted,
            Some(GlobalSeq(idx as u64)),
            Some(DurabilityTier::Global),
        )
        .unwrap();
    }
    let doc_tx = core
        .commit_mergeable(
            MergeableCommit::new("docs", row(0xd5), 90).cells(BTreeMap::from([(
                "name".to_owned(),
                v("reachable"),
            )])),
        )
        .unwrap();
    core.apply_fate_update(
        doc_tx,
        Fate::Accepted,
        Some(GlobalSeq(90)),
        Some(DurabilityTier::Global),
    )
    .unwrap();
    let access_tx = core
        .commit_mergeable(
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
        Some(GlobalSeq(91)),
        Some(DurabilityTier::Global),
    )
    .unwrap();
    for (idx, member, parent) in [(0xe5, team1, team2), (0xe6, team2, team3)] {
        let tx_id = core
            .commit_mergeable(
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
            Some(GlobalSeq(idx as u64)),
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
        core.query_rows_at(&shape, &binding, GlobalSeq(90))
            .unwrap()
            .is_empty(),
        "access and edge rows should not be visible before their historical positions"
    );
    let rows = core.query_rows_at(&shape, &binding, GlobalSeq(230)).unwrap();
    assert_eq!(
        rows.into_iter().map(current_row_pair).collect::<BTreeMap<_, _>>(),
        BTreeMap::from([(row(0xd5), title_cells("reachable"))])
    );
    assert!(
        !core.query
            .query_shape_cache
            .keys()
            .any(|(shape_id, _)| *shape_id == shape.shape_id()),
        "historical schema-projected reachable reads must lower over inline projected sources"
    );
}

#[test]
fn partitioned_inner_include_target_bypasses_prepared_lowering() {
    let base = JazzSchema::new([
        TableSchema::new(
            "todos",
            [
                ColumnSchema::new("title", ColumnType::String),
                ColumnSchema::new("project", ColumnType::Uuid),
            ],
        )
        .with_reference("project", "projects"),
        TableSchema::new(
            "projects",
            [ColumnSchema::new("title", ColumnType::String)],
        ),
    ]);
    let evolved = SchemaVersion::new(JazzSchema::new([
        TableSchema::new(
            "todos",
            [
                ColumnSchema::new("title", ColumnType::String),
                ColumnSchema::new("project", ColumnType::Uuid),
            ],
        )
        .with_reference("project", "projects"),
        TableSchema::new(
            "projects",
            [ColumnSchema::new("name", ColumnType::String)],
        ),
    ]));
    let (_dir, mut core) = open_node_with_schema(node(0x50), base.clone());
    core.catalogue.partitions.insert(("projects".to_owned(), evolved.id));

    let inner_shape = Query::from("todos")
        .include("project")
        .validate(&base)
        .unwrap();
    assert!(
        core.uses_partitioned_or_schema_projected_read(&inner_shape),
        "inner/required include targets are storage reads and must use the projected current source path when partitioned"
    );

    let holes_shape = Query::from("todos")
        .include_with(crate::query::Include::new("project").join_mode(crate::query::JoinMode::Holes))
        .validate(&base)
        .unwrap();
    assert!(
        !core.uses_partitioned_or_schema_projected_read(&holes_shape),
        "hole-only includes do not filter membership and are not a prepared-lowering bypass by themselves"
    );
}

#[test]
fn wire_commit_units_preserve_node_and_schema_uuids_not_local_aliases() {
    let schema = schema();
    let (_writer_dir, mut writer) = open_node_with_schema(node(0x4a), schema.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(0x4b), schema.clone());
    let (parent, parent_unit) = writer
        .commit_mergeable_unit(
            MergeableCommit::new("todos", row(0x4a), 10).cells(title_cells("parent")),
        )
        .unwrap();
    core.apply_sync_message(parent_unit).unwrap();
    let (_child_tx, unit) = writer
        .commit_mergeable_unit(
            MergeableCommit::new("todos", row(0x4a), 11)
                .parents(vec![parent])
                .cells(title_cells("child")),
        )
        .unwrap();
    let SyncMessage::CommitUnit { tx, versions } = &unit else {
        panic!("commit unit expected");
    };
    assert_eq!(tx.tx_id.node, node(0x4a));
    assert_eq!(versions[0].schema_version(), schema.version_id());
    assert_eq!(versions[0].parents(), vec![parent]);
    assert_eq!(versions[0].parents()[0].node, node(0x4a));

    core.apply_sync_message(unit).unwrap();
    let stored = core.query_table_versions("todos").unwrap();
    let child_row = stored
        .iter()
        .find(|version| version.parents().contains(&parent))
        .unwrap();
    let stored_wire = core.version_record_from_row(child_row).unwrap();
    assert_eq!(stored_wire.schema_version(), schema.version_id());
}
