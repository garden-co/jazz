#[test]
fn maintained_projected_current_picks_winner_before_lens_projection() {
    let base = schema();
    let evolved = evolved_todos_name_body_schema();
    let evolved_payload = SchemaVersion::new(evolved);
    let (_dir, mut core) = open_node_with_schema(node(0x4d), base.clone());
    let shared_row = row(0x4e);

    let old_tx = core
        .commit_mergeable_settled(MergeableCommit::new("todos", shared_row, 10).cells(BTreeMap::from([
            ("title".to_owned(), v("old-title")),
        ])))
        .unwrap();
    core.accept_global_for_test(old_tx).unwrap();
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

    let new_tx = core
        .commit_mergeable_settled(MergeableCommit::new("todos", shared_row, 11).cells(BTreeMap::from([
            ("name".to_owned(), v("new-name")),
            ("body".to_owned(), v("new-body")),
        ])))
        .unwrap();
    core.accept_global_for_test(new_tx).unwrap();

    let shape = Query::from("todos").validate(&base).unwrap();
    let rows = core
        .query_rows(
            &shape,
            &shape.bind(BTreeMap::new()).unwrap(),
            DurabilityTier::Global,
        )
        .unwrap()
        .into_iter()
        .map(current_row_pair)
        .collect::<BTreeMap<_, _>>();
    assert_eq!(
        rows,
        BTreeMap::from([(
            shared_row,
            BTreeMap::from([("title".to_owned(), v("new-name"))]),
        )])
    );

    let mut peer = PeerState::new();
    let update = peer.current_rows_update(&mut core, "todos").unwrap();
    let bundles = version_bundles_for_update(&update);
    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        result_member_adds,
        result_member_removes,
        reset_result_set,
        ..
    }) = update
    else {
        panic!("current-row subscription should produce a view update");
    };
    assert!(!reset_result_set);
    assert!(result_member_removes.is_empty());
    assert_eq!(result_member_adds.len(), 1);
    let member = result_member_adds[0]
        .as_real_row()
        .expect("current-row result should be real row");
    assert_eq!(member.row_uuid, shared_row);
    assert_eq!(member.content_tx, Some(new_tx));
    assert_eq!(bundles.len(), 1);
    assert_eq!(bundles[0].versions.len(), 1);
    let shipped = &bundles[0].versions[0];
    let canonical = core
        .query_versions_for_tx(new_tx)
        .unwrap()
        .into_iter()
        .find(|version| version.row_uuid() == shared_row)
        .map(|version| core.version_record_from_row(&version).unwrap())
        .expect("new winner must remain in canonical history");
    assert_eq!(
        shipped, &canonical,
        "the title-only read projection must not change the replicated VersionRecord (INV-DATA-16/18; INV-SYNC-16; C.3)"
    );
    assert_eq!(shipped.schema_version(), evolved_payload.id);
    assert_eq!(shipped.table(), "todos");
    assert_eq!(shipped.cell_at(0), Some(v("new-name")));
    assert_eq!(shipped.cell_at(1), Some(v("new-body")));
}

/// A maintained `tasks` view may read an old `todos` row through a table-rename
/// lens, but its wire witness remains the complete `todos` history version.
#[test]
fn maintained_renamed_table_witness_reloads_the_authored_history_row() {
    let base = schema();
    let evolved = build_public_test_schema(
        PublicSchemaBuilder::new().table(
            PublicTableSchemaBuilder::new("tasks").column("title", PublicColumnType::Text),
        ),
    );
    let evolved_payload = SchemaVersion::new(evolved.clone());
    let (_dir, mut core) = open_node_with_schema(node(0x5d), base.clone());
    let shared_row = row(0x5e);

    let old_tx = core
        .commit_mergeable_settled(MergeableCommit::new("todos", shared_row, 10).cells(BTreeMap::from([
            ("title".to_owned(), v("old-title")),
        ])))
        .unwrap();
    core.accept_global_for_test(old_tx).unwrap();
    publish_schema_lineage(
        &mut core,
        evolved_payload.clone(),
        MigrationLens::new(
            base.version_id(),
            evolved_payload.id,
            vec![TableLens {
                source_table: "todos".to_owned(),
                target_table: "tasks".to_owned(),
                ops: vec![LensOp::RenameTable {
                    from: "todos".to_owned(),
                    to: "tasks".to_owned(),
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

    let shape = Query::from("tasks")
        .validate_with_schema_version(&evolved, evolved_payload.id)
        .unwrap();
    let rows = core
        .query_rows(
            &shape,
            &shape.bind(BTreeMap::new()).unwrap(),
            DurabilityTier::Global,
        )
        .unwrap();
    assert_eq!(rows.len(), 1, "the renamed view sees the old row");

    let mut peer = PeerState::new();
    let update = peer.current_rows_update(&mut core, "tasks").unwrap();
    let bundles = version_bundles_for_update(&update);
    assert_eq!(bundles.len(), 1);
    assert_eq!(bundles[0].versions.len(), 1);
    let shipped = &bundles[0].versions[0];
    assert_eq!(shipped.schema_version(), base.version_id());
    assert_eq!(shipped.table(), "todos");
    assert_eq!(shipped.cell_at(0), Some(v("old-title")));

    let authored_versions = core.query_versions_for_tx(old_tx).unwrap();
    let materialization_witness = core
        .maintained_witness_for_result_member(
            &authored_versions,
            evolved_payload.id,
            "tasks",
            shared_row,
        )
        .unwrap()
        .expect("the tasks result member recognizes its authored todos witness by physical identity");
    assert_eq!(materialization_witness.table(), "todos");
    assert_eq!(
        core.physical_table_id_for_version(materialization_witness)
            .unwrap(),
        core.physical_table_id_for_schema(evolved_payload.id, "tasks")
            .unwrap(),
        "INV-LENS-21 keeps the renamed table's physical identity stable",
    );
}

/// A projected result name may also have belonged to a different old physical
/// table. Even when both rows share an exclusive transaction and row UUID, the
/// maintained wire witness must fail closed rather than selecting by name/key.
#[test]
fn maintained_renamed_witness_rejects_reused_logical_table_collision() {
    let base = build_public_test_schema(
        PublicSchemaBuilder::new()
            .table(
                PublicTableSchemaBuilder::new("tasks")
                    .column("title", PublicColumnType::Text),
            )
            .table(
                PublicTableSchemaBuilder::new("todos")
                    .column("title", PublicColumnType::Text),
            ),
    );
    let evolved = build_public_test_schema(
        PublicSchemaBuilder::new().table(
            PublicTableSchemaBuilder::new("tasks").column("title", PublicColumnType::Text),
        ),
    );
    let evolved_payload = SchemaVersion::new(evolved.clone());
    let (_dir, mut core) = open_node_with_schema(node(0x5f), base.clone());
    let shared_row = row(0x60);

    let open = OpenTransactionId::new();
    core.open_exclusive(open).unwrap();
    core.tx_write(open, "tasks", shared_row, title_cells("old physical task"), None)
        .unwrap();
    core.tx_write(
        open,
        "todos",
        shared_row,
        title_cells("canonical renamed todo"),
        None,
    )
    .unwrap();
    let (collision_tx, _) = core.commit_exclusive_settled(open, AuthorSubject::SYSTEM, 10).unwrap();
    core.accept_global_for_test(collision_tx).unwrap();

    publish_schema_lineage(
        &mut core,
        evolved_payload.clone(),
        MigrationLens::new(
            base.version_id(),
            evolved_payload.id,
            vec![TableLens {
                source_table: "todos".to_owned(),
                target_table: "tasks".to_owned(),
                ops: vec![LensOp::RenameTable {
                    from: "todos".to_owned(),
                    to: "tasks".to_owned(),
                }],
            }],
        ).expect("valid migration lens"),
        Vec::<String>::new(),
        ["tasks"],
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

    let shape = Query::from("tasks")
        .validate_with_schema_version(&evolved, evolved_payload.id)
        .unwrap();
    assert_eq!(
        core.query_rows(
            &shape,
            &shape.bind(BTreeMap::new()).unwrap(),
            DurabilityTier::Global,
        )
        .unwrap()
        .len(),
        1,
        "the evolved tasks view contains the renamed todos row, not the dropped old tasks row"
    );

    let mut peer = PeerState::new();
    assert!(matches!(
        peer.current_rows_update(&mut core, "tasks").resolve(),
        Err(Error::InvalidStoredValue(
            "maintained witness maps to zero or multiple physical tables"
        ))
    ));
}
