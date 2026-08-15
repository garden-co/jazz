#[test]
fn maintained_projected_current_picks_winner_before_lens_projection() {
    let base = schema();
    let evolved = JazzSchema::new([TableSchema::new(
        "todos",
        [
            ColumnSchema::new("name", ColumnType::String),
            ColumnSchema::new("body", ColumnType::String),
        ],
    )]);
    let evolved_payload = SchemaVersion::new(evolved);
    let (_dir, mut core) = open_node_with_schema(node(0x4d), base.clone());
    let shared_row = row(0x4e);

    let old_tx = core
        .commit_mergeable(MergeableCommit::new("todos", shared_row, 10).cells(BTreeMap::from([
            ("title".to_owned(), v("old-title")),
        ])))
        .unwrap();
    core.apply_fate_update(
        old_tx,
        Fate::Accepted,
        Some(core.clock.next_global_seq),
        Some(DurabilityTier::Global),
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
        ),
        Vec::<String>::new(),
        Vec::<String>::new(),
    )
    .unwrap();
    core.apply_trusted_catalogue_message(SyncMessage::SetCurrentWriteSchema {
        author: AuthorId::SYSTEM,
        pointer: CurrentWriteSchema {
            revision: 1,
            schema: evolved_payload.id,
        },
    })
    .unwrap();

    let new_tx = core
        .commit_mergeable(MergeableCommit::new("todos", shared_row, 11).cells(BTreeMap::from([
            ("name".to_owned(), v("new-name")),
            ("body".to_owned(), v("new-body")),
        ])))
        .unwrap();
    core.apply_fate_update(
        new_tx,
        Fate::Accepted,
        Some(core.clock.next_global_seq),
        Some(DurabilityTier::Global),
    )
    .unwrap();

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
    let SyncMessage::ViewUpdate {
        result_member_adds,
        result_member_removes,
        reset_result_set,
        ..
    } = update
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
    let evolved = JazzSchema::new([TableSchema::new(
        "tasks",
        [ColumnSchema::new("title", ColumnType::String)],
    )]);
    let evolved_payload = SchemaVersion::new(evolved.clone());
    let (_dir, mut core) = open_node_with_schema(node(0x5d), base.clone());
    let shared_row = row(0x5e);

    let old_tx = core
        .commit_mergeable(MergeableCommit::new("todos", shared_row, 10).cells(BTreeMap::from([
            ("title".to_owned(), v("old-title")),
        ])))
        .unwrap();
    core.apply_fate_update(
        old_tx,
        Fate::Accepted,
        Some(core.clock.next_global_seq),
        Some(DurabilityTier::Global),
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
                target_table: "tasks".to_owned(),
                ops: vec![LensOp::RenameTable {
                    from: "todos".to_owned(),
                    to: "tasks".to_owned(),
                }],
            }],
        ),
        Vec::<String>::new(),
        Vec::<String>::new(),
    )
    .unwrap();
    core.apply_trusted_catalogue_message(SyncMessage::SetCurrentWriteSchema {
        author: AuthorId::SYSTEM,
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
}
