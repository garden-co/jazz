// Branch relay, pointer ordering, live table registration, and durable mappings.

#[test]
fn commit_arrival_preserves_known_noncurrent_authored_variant() {
    // Internal because the authored physical discriminator is not meaningfully
    // distinguishable through the public read surface.
    let base = schema();
    let evolved = catalogue_evolved_schema();
    let evolved_payload = SchemaVersion::new(evolved.clone());
    let (_writer_dir, mut writer) = open_node_with_schema(node(0x5a), evolved.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(0x5b), base.clone());
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
    assert_eq!(core.current_write_schema().unwrap().schema, base.version_id());

    let row = row(0x5c);
    let (_tx_id, unit) = writer
        .commit_mergeable_unit_settled(
            MergeableCommit::new("todos", row, 10).cells(BTreeMap::from([
                ("title".to_owned(), v("newer-client")),
                ("body".to_owned(), v("authored-v2")),
            ])),
        )
        .unwrap();
    core.apply_sync_message_settled(unit).unwrap();

    assert_eq!(core.current_write_schema().unwrap().schema, base.version_id());
    let stored = core.query_table_versions("todos").unwrap();
    assert_eq!(stored.len(), 1);
    let stored_wire = core.version_record_from_row(&stored[0]).unwrap();
    assert_eq!(stored_wire.schema_version(), evolved_payload.id);
    assert_eq!(
        version_record_cells(&stored_wire, &evolved.tables[0]),
        BTreeMap::from([
            ("title".to_owned(), v("newer-client")),
            ("body".to_owned(), v("authored-v2")),
        ])
    );
}
#[test]
fn catalogue_current_write_schema_revision_is_core_ordered() {
    let base = schema();
    let evolved = catalogue_evolved_schema();
    let evolved_payload = SchemaVersion::new(evolved);
    let (_dir, mut core) = open_node_with_schema(node(0x38), base.clone());
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
                    default: Value::String(String::new()),
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
            revision: 2,
            schema: evolved_payload.id,
        },
    })
    .unwrap();
    assert_eq!(core.current_write_schema().unwrap().revision, 2);
    assert_eq!(core.current_write_schema().unwrap().schema, evolved_payload.id);

    let stale = core.apply_trusted_catalogue_message_settled(SyncMessage::SetCurrentWriteSchema {
        author: AuthorSubject::SYSTEM,
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
    assert_eq!(core.current_write_schema().unwrap().revision, 2);

    core.apply_trusted_catalogue_message_settled(SyncMessage::SetCurrentWriteSchema {
        author: AuthorSubject::SYSTEM,
        pointer: CurrentWriteSchema {
            revision: 3,
            schema: base.version_id(),
        },
    })
    .unwrap();
    assert_eq!(core.current_write_schema().unwrap().revision, 3);
    assert_eq!(core.current_write_schema().unwrap().schema, base.version_id());
}
#[test]
fn durable_catalogue_values_pointer_and_physical_mappings_survive_restart() {
    let base = schema();
    let evolved = catalogue_evolved_schema();
    let evolved_payload = SchemaVersion::new(evolved.clone());
    let (dir, mut core) = open_node_with_schema(node(0x39), base.clone());
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
    ).expect("valid migration lens");
    publish_schema_lineage(
        &mut core,
        evolved_payload.clone(),
        lens.clone(),
        Vec::<String>::new(),
        Vec::<String>::new(),
    )
    .unwrap();
    core.apply_trusted_catalogue_message_settled(SyncMessage::SetCurrentWriteSchema {
        author: AuthorSubject::SYSTEM,
        pointer: CurrentWriteSchema {
            revision: 4,
            schema: evolved_payload.id,
        },
    })
    .unwrap();
    let physical_mapping = core.catalogue.physical_mappings[&evolved_payload.id].clone();
    assert!(matches!(
        core.database.table_schema("jazz_partitions"),
        Err(GrooveDbError::TableNotFound(_))
    ));
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
        reopened.current_write_schema().unwrap(),
        CurrentWriteSchema {
            revision: 4,
            schema: evolved_payload.id,
        }
    );
    assert_eq!(
        reopened.catalogue.physical_mappings[&evolved_payload.id],
        physical_mapping
    );
    assert!(matches!(
        reopened.database.table_schema("jazz_partitions"),
        Err(GrooveDbError::TableNotFound(_))
    ));
}
#[test]
fn shape_registration_parks_until_schema_version_catalogue_arrives() {
    let base = schema();
    let evolved = todos_notes_schema();
    let shape = Query::from("todos").validate(&evolved).unwrap();
    let (dir, mut core) = open_node_with_schema(node(0x3c), base.clone());

    core.apply_sync_message_settled(SyncMessage::RegisterShape {
        shape_id: shape.shape_id(),
        ast: crate::protocol::ShapeAst::from_validated(&shape),
        opts: crate::protocol::RegisterShapeOptions::default(),
    })
    .unwrap();
    assert_eq!(core.sync_metrics().parked_catalogue_shapes, 1);
    assert!(!core.query.registered_shapes.contains_key(&shape.shape_id()));

    let evolved = SchemaVersion::new(evolved);
    publish_schema_lineage(
        &mut core,
        evolved.clone(),
        MigrationLens::new(
            base.version_id(),
            evolved.id,
            vec![TableLens {
                source_table: "todos".to_owned(),
                target_table: "todos".to_owned(),
                ops: vec![],
            }],
        ).expect("valid migration lens"),
        ["notes"],
        Vec::<String>::new(),
    )
    .unwrap();
    assert_eq!(core.sync_metrics().parked_catalogue_shapes_resolved, 1);
    assert!(core.query.registered_shapes.contains_key(&shape.shape_id()));

    drop(core);
    let reopened = reopen_node_at(&dir, node(0x3c), schema());
    assert!(
        reopened
            .catalogue_schemas()
            .contains_key(&shape.schema_version())
    );
}
#[test]
fn publishing_schema_registers_new_physical_tables_live() {
    let base = schema();
    let evolved = catalogue_evolved_schema();
    let evolved_payload = SchemaVersion::new(evolved);
    let (_dir, mut core) = open_node_with_schema(node(0x3b), base.clone());
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
                    default: Value::String(String::new()),
                }],
            }],
        ).expect("valid migration lens"),
        Vec::<String>::new(),
        Vec::<String>::new(),
    )
    .unwrap();
    let table_id = core.catalogue.physical_mappings[&evolved_payload.id].tables["todos"].table_id;
    let history = physical_history_table_name(table_id);
    let register = physical_register_table_name(table_id);
    assert!(core.database.primary_key_scan_raw(&history, &[]).is_ok());
    assert!(core.database.primary_key_scan_raw(&register, &[]).is_ok());

    core.apply_trusted_catalogue_message_settled(SyncMessage::SetCurrentWriteSchema {
        author: AuthorSubject::SYSTEM,
        pointer: CurrentWriteSchema {
            revision: 1,
            schema: evolved_payload.id,
        },
    })
    .unwrap();
    assert!(core.database.primary_key_scan_raw(&history, &[]).is_ok());
    assert!(core.database.primary_key_scan_raw(&register, &[]).is_ok());
}

#[test]
fn publishing_schema_registers_new_tables_without_storage_reopen() {
    let base = schema();
    let evolved = todos_notes_schema();
    let evolved_payload = SchemaVersion::new(evolved.clone());
    let mut core = open_reopen_refusing_node_with_schema(node(0x3e), base.clone());

    publish_schema_lineage(
        &mut core,
        evolved_payload.clone(),
        MigrationLens::new(
            base.version_id(),
            evolved_payload.id,
            vec![TableLens {
                source_table: "todos".to_owned(),
                target_table: "todos".to_owned(),
                ops: vec![],
            }],
        ).expect("valid migration lens"),
        ["notes"],
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
    assert!(core.table_in_schema("notes", evolved_payload.id).is_ok());

    let note = row(0x3e);
    assert!(
        core.advisory_mergeable_write_allows(MergeableCommit::new("notes", note, 10).cells(
            BTreeMap::from([("body".to_owned(), v("live add-table write"),)])
        ))
        .unwrap()
    );
    let tx_id = core
        .commit_mergeable_settled(
            MergeableCommit::new("notes", note, 10).cells(BTreeMap::from([(
                "body".to_owned(),
                v("live add-table write"),
            )])),
        )
        .unwrap();
    let shape = Query::from("notes").validate(&evolved).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    assert_eq!(
        core.query_rows(&shape, &binding, DurabilityTier::Local)
            .unwrap()
            .len(),
        1
    );
    core.accept_global_for_test(tx_id).unwrap();

    let rows = core
        .query_rows(&shape, &binding, DurabilityTier::Local)
        .unwrap()
        .into_iter()
        .map(current_row_pair)
        .collect::<BTreeMap<_, _>>();
    assert_eq!(
        rows,
        BTreeMap::from([(
            note,
            BTreeMap::from([("body".to_owned(), v("live add-table write"))])
        )])
    );

    let mut peer = PeerState::new();
    let (served_shape, served_binding) = core.whole_table_shape_binding("notes").unwrap();
    core.prepare_query_binding_for_link(
        &served_shape,
        &served_binding,
        DurabilityTier::Global,
        peer.permission_subject()
            .expect("standalone peer terminates SYSTEM"),
    )
    .unwrap();
    let update = peer.current_rows_update(&mut core, "notes").unwrap();
    let version_bundles = version_bundles_for_update(&update);
    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        result_member_adds,
        result_member_removes,
        program_fact_adds,
        ..
    }) = &update
    else {
        panic!("current-row subscription should produce a view update");
    };
    assert!(result_member_adds.is_empty());
    assert!(result_member_removes.is_empty());
    assert_eq!(
        program_fact_adds
            .iter()
            .filter(|fact| matches!(
                fact,
                crate::protocol::ProgramFactEntry::CoveredInput(input)
                    if input.version_table.as_str() == "notes"
                        && input.source_row == note
                        && input.version.layer == crate::protocol::ResultRowLayer::Content
            ))
            .count(),
        1,
        "the current-row update must disclose its exact notes source closure"
    );

    assert_eq!(version_bundles.len(), 1);
}

#[test]
fn transaction_version_scans_recover_table_names_from_physical_mappings() {
    let base = schema();
    let evolved = todos_notes_schema();
    let evolved_payload = SchemaVersion::new(evolved);
    let (dir, mut core) = open_node_with_schema(node(0x3f), base.clone());
    publish_schema_lineage(
        &mut core,
        evolved_payload.clone(),
        MigrationLens::new(
            base.version_id(),
            evolved_payload.id,
            vec![TableLens {
                source_table: "todos".to_owned(),
                target_table: "todos".to_owned(),
                ops: vec![],
            }],
        ).expect("valid migration lens"),
        ["notes"],
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
            MergeableCommit::new("notes", row(0x3f), 10)
                .cells(BTreeMap::from([("body".to_owned(), v("mapped scan"))])),
        )
        .unwrap();
    drop(core);

    let mut reopened = reopen_node_at(&dir, node(0x3f), base);
    let versions = reopened.query_versions_for_tx(tx_id).unwrap();
    assert_eq!(versions.len(), 1);
    assert_eq!(versions[0].table(), "notes");
    assert_eq!(versions[0].row_uuid(), row(0x3f));
}
