// Shared physical identities, deletion registers, merge epochs, and restart.

#[test]
fn table_and_column_rename_reuses_the_existing_physical_identities() {
    let base = schema();
    let renamed = SchemaVersion::new(renamed_tasks_schema());
    let (_dir, mut core) = open_node_with_schema(node(0x30), base.clone());
    let source = core.catalogue.physical_mappings[&base.version_id()].tables["todos"].clone();
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

    let target = &core.catalogue.physical_mappings[&renamed.id].tables["tasks"];
    assert_eq!(target.table_id, source.table_id);
    assert_eq!(target.columns["name"], source.columns["title"]);
}

#[test]
fn rejected_versions_share_physical_storage_across_renamed_schemas_and_reopen() {
    // Rejected payload storage is intentionally not public API, so this test
    // inspects its physical identity and stored schema discriminator directly.
    let base = schema();
    let renamed_schema = renamed_tasks_schema();
    let renamed = SchemaVersion::new(renamed_schema.clone());
    let (dir, mut core) = open_node_with_schema(node(0x34), base.clone());
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

    let tx = OpenTransactionId::new();
    core.open_exclusive(tx).unwrap();
    core.tx_write(
        tx,
        "tasks",
        row(0x35),
        BTreeMap::from([("name".to_owned(), v("retry renamed"))]),
        None,
    )
    .unwrap();
    let (rejected, _unit) = core.commit_exclusive_settled(tx, AuthorSubject::SYSTEM, 10).unwrap();
    core.apply_sync_message_settled(SyncMessage::FateUpdate {
        tx_id: rejected,
        fate: Fate::Rejected(RejectionReason::ExclusiveConflict),
        global_time: None,
        durability: None,
    })
    .unwrap();

    let table_id = core.catalogue.physical_mappings[&base.version_id()].tables["todos"].table_id;
    assert_eq!(
        core.catalogue.physical_mappings[&renamed.id].tables["tasks"].table_id,
        table_id
    );
    let storage_table = physical_rejected_versions_table_name(table_id);
    let rows = core
        .database
        .primary_key_scan_raw(&storage_table, &[])
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        u64::from(rows[0].variant_tag()),
        core.catalogue.schema_version_aliases[&renamed.id].0
    );
    for logical_table in [
        "jazz_todos_rejected_versions",
        "jazz_tasks_rejected_versions",
    ] {
        assert!(matches!(
            core.database.table_schema(logical_table),
            Err(GrooveDbError::TableNotFound(_))
        ));
    }
    let stored = core.rejected_transaction(rejected).unwrap();
    assert_eq!(stored.versions()[0].table(), "tasks");
    assert_eq!(
        stored.versions()[0].cell(&renamed_schema.tables[0], "name"),
        Some(v("retry renamed"))
    );

    drop(core);
    let mut reopened = reopen_node_at(&dir, node(0x34), base);
    let recovered = reopened.rejected_transaction(rejected).unwrap();
    assert_eq!(recovered.versions(), stored.versions());
    assert_eq!(recovered.versions()[0].table(), "tasks");
    assert_eq!(
        recovered.versions()[0].cell(&renamed_schema.tables[0], "name"),
        Some(v("retry renamed"))
    );

    reopened.discard_rejection(rejected).unwrap();
    assert!(
        reopened
            .database
            .primary_key_scan_raw(&storage_table, &[])
            .unwrap()
            .is_empty()
    );
    drop(reopened);
    let reopened = reopen_node_at(&dir, node(0x34), schema());
    assert!(reopened.rejected_transaction(rejected).is_none());
}

#[test]
fn physical_deletion_register_spans_renamed_schemas_and_reopens() {
    let base = schema();
    let renamed_schema = renamed_tasks_schema();
    let renamed = SchemaVersion::new(renamed_schema.clone());
    let (dir, mut core) = open_node_with_schema(node(0x2b), base.clone());
    let row_uuid = row(0x4b);
    core.commit_mergeable_settled(MergeableCommit::new("todos", row_uuid, 10).cells(title_cells("shared")))
        .unwrap();
    core.commit_mergeable_settled(
        MergeableCommit::new("todos", row_uuid, 11).deletion(DeletionEvent::Deleted),
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
    core.commit_mergeable_settled(
        MergeableCommit::new("tasks", row_uuid, 12).deletion(DeletionEvent::Restored),
    )
    .unwrap();

    let table_id = core.catalogue.physical_mappings[&base.version_id()].tables["todos"].table_id;
    assert_eq!(
        core.catalogue.physical_mappings[&renamed.id].tables["tasks"].table_id,
        table_id
    );
    for logical_table in ["todos", "tasks"] {
        assert_eq!(
            core.version_storage_sources_for_layer(logical_table, VersionLayer::Deletion)
                .unwrap(),
            vec![SHARED_DELETION_HISTORY_TABLE.to_owned()]
        );
    }
    assert_eq!(
        core.database
            .primary_key_scan_raw(
                SHARED_DELETION_HISTORY_TABLE,
                &[
                    Value::Bytes(BranchKey::default().canonical_bytes()),
                    Value::U64(table_id.0),
                ],
            )
            .unwrap()
            .len(),
        2
    );
    let deletion_versions = core
        .query_table_versions("tasks")
        .unwrap()
        .into_iter()
        .filter(|version| version.layer() == VersionLayer::Deletion)
        .collect::<Vec<_>>();
    assert_eq!(deletion_versions.len(), 2);
    assert_eq!(
        deletion_versions
            .iter()
            .map(VersionRow::schema_version_alias)
            .collect::<BTreeSet<_>>()
            .len(),
        2
    );

    let shape = Query::from("tasks").validate(&renamed_schema).unwrap();
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
        BTreeMap::from([(row_uuid, BTreeMap::from([("name".to_owned(), v("shared"))]))])
    );

    drop(core);
    let mut reopened = reopen_node_at(&dir, node(0x2b), base);
    assert_eq!(
        reopened
            .version_storage_sources_for_layer("tasks", VersionLayer::Deletion)
            .unwrap(),
        vec![SHARED_DELETION_HISTORY_TABLE.to_owned()]
    );
    assert_eq!(
        reopened
            .query_table_versions("tasks")
            .unwrap()
            .into_iter()
            .filter(|version| version.layer() == VersionLayer::Deletion)
            .count(),
        2
    );
    // Recovery must include the shared deletion history in HLC reconstruction:
    // a post-reopen deletion is a new version, not a stale-key collision.
    reopened
        .commit_mergeable_settled(
            MergeableCommit::new("tasks", row_uuid, 13).deletion(DeletionEvent::Deleted),
        )
        .unwrap();
    assert_eq!(
        reopened
            .query_table_versions("tasks")
            .unwrap()
            .into_iter()
            .filter(|version| version.layer() == VersionLayer::Deletion)
            .count(),
        3
    );
}

/// A late fate for a v1 deletion must inspect the shared physical deletion
/// register through the v1 prefix, while preserving a newer v2 winner that
/// already occupies that lineage. Looking the historical `todos` witness up
/// through ambient v2 `tasks` state used to reject the bundle before the
/// currency comparison could retain the v2 deletion.
#[test]
fn late_renamed_deletion_fate_uses_authored_prefix_and_keeps_newer_winner() {
    let base = schema();
    let renamed_schema = renamed_tasks_schema();
    let renamed = SchemaVersion::new(renamed_schema.clone());
    let (_dir, mut core) = open_node_with_schema(node(0x6d), base.clone());
    let row_uuid = row(0x6e);

    let old_delete = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row_uuid, 10).deletion(DeletionEvent::Deleted),
        )
        .expect("stage v1 deletion before its fate");
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
    .expect("publish rename");
    core.apply_trusted_catalogue_message_settled(SyncMessage::SetCurrentWriteSchema {
        author: AuthorSubject::SYSTEM,
        pointer: CurrentWriteSchema {
            revision: 1,
            schema: renamed.id,
        },
    })
    .expect("activate v2 tasks");
    // Match a v2 receiver: its ambient application schema no longer names
    // the authored v1 `todos` literal carried by the late authority bundle.
    core.catalogue.current_schema_version_id = renamed.id;
    core.catalogue.schema = renamed_schema;

    let new_delete = core
        .commit_mergeable_settled(
            MergeableCommit::new("tasks", row_uuid, 20).deletion(DeletionEvent::Deleted),
        )
        .expect("stage newer v2 deletion");
    core.apply_fate_update(
        new_delete,
        Fate::Accepted,
        Some(GlobalTime(2)),
        Some(DurabilityTier::Global),
    )
    .expect("settle v2 deletion first");

    core.apply_fate_update(
        old_delete,
        Fate::Accepted,
        Some(GlobalTime(1)),
        Some(DurabilityTier::Global),
    )
    .expect("late v1 fate must compare through its authored deletion prefix");

    let winner = core
        .query_global_layer_winner_in_schema(
            renamed.id,
            "tasks",
            row_uuid,
            VersionLayer::Deletion,
        )
        .expect("read v2 deletion winner")
        .expect("v2 deletion remains current");
    assert_eq!(winner.table(), "tasks");
    assert_eq!(core.version_tx_id(&winner).expect("winner transaction"), new_delete);
}

#[test]
fn shared_deletion_history_keeps_same_row_uuid_table_scoped() {
    let schema = todos_notes_schema();
    let (dir, mut core) = open_node_with_schema(node(0x2c), schema.clone());
    let shared_row = row(0x4c);
    core.commit_mergeable_settled(
        MergeableCommit::new("todos", shared_row, 10).cells(BTreeMap::from([(
            "title".to_owned(),
            v("todo"),
        )])),
    )
    .unwrap();
    core.commit_mergeable_settled(
        MergeableCommit::new("notes", shared_row, 11).cells(BTreeMap::from([(
            "body".to_owned(),
            v("note"),
        )])),
    )
    .unwrap();
    let deletion_tx = core
        .commit_mergeable_many_settled(vec![
            MergeableCommit::new("todos", shared_row, 12).deletion(DeletionEvent::Deleted),
            MergeableCommit::new("notes", shared_row, 13).deletion(DeletionEvent::Deleted),
        ])
        .unwrap();

    let mapping = &core.catalogue.physical_mappings[&schema.version_id()].tables;
    let todos_id = mapping["todos"].table_id;
    let notes_id = mapping["notes"].table_id;
    assert_ne!(todos_id, notes_id);
    for table_id in [todos_id, notes_id] {
        assert_eq!(
            core.database
                .primary_key_scan_raw(
                    SHARED_DELETION_HISTORY_TABLE,
                    &[
                        Value::Bytes(BranchKey::default().canonical_bytes()),
                        Value::U64(table_id.0),
                        Value::Uuid(shared_row.0),
                    ],
                )
                .unwrap()
                .len(),
            1,
            "table/row prefix must not see the other table's deletion",
        );
    }
    for table in ["todos", "notes"] {
        let shape = Query::from(table).validate(&schema).unwrap();
        assert!(core
            .query_rows(
                &shape,
                &shape.bind(BTreeMap::new()).unwrap(),
                DurabilityTier::Local,
            )
            .unwrap()
            .is_empty());
        assert_eq!(
            core.query_table_versions(table)
                .unwrap()
                .into_iter()
                .filter(|version| version.layer() == VersionLayer::Deletion)
                .count(),
            1
        );
    }

    assert_eq!(
        core.query_versions_for_tx(deletion_tx)
            .unwrap()
            .into_iter()
            .filter(|version| version.layer() == VersionLayer::Deletion)
            .count(),
        2,
        "a transaction-wide shared-index scan must decode each physical table independently",
    );
    drop(core);
    let mut reopened = reopen_node_at(&dir, node(0x2c), schema);
    assert_eq!(
        reopened
            .query_versions_for_tx(deletion_tx)
            .unwrap()
            .into_iter()
            .filter(|version| version.layer() == VersionLayer::Deletion)
            .count(),
        2,
        "recovery must preserve the transaction-wide physical-table routing",
    );
}

#[test]
fn changed_merge_semantics_start_a_new_physical_column_epoch() {
    // The stored scalar representation remains I32, but counter and LWW cells
    // cannot share one physical column identity or its derived indexes.
    let base = build_public_test_schema(
        PublicSchemaBuilder::new().table(
            PublicTableSchemaBuilder::new("counts")
                .column("value", PublicColumnType::Integer),
        ),
    );
    let evolved = SchemaVersion::new(compile_public_test_schema(
        &[(
            PublicTableName::new("counts"),
            PublicTableSchema::new(PublicRowDescriptor::new(vec![
                PublicColumnDescriptor::new("value", PublicColumnType::Integer)
                    .merge_strategy(PublicColumnMergeStrategy::Counter),
            ])),
        )]
        .into_iter()
        .collect::<PublicSchema>(),
    ));
    let (_dir, mut core) = open_node_with_schema(node(0x2c), base.clone());
    let source = core.catalogue.physical_mappings[&base.version_id()].tables["counts"].clone();
    publish_schema_lineage(
        &mut core,
        evolved.clone(),
        MigrationLens::new(
            base.version_id(),
            evolved.id,
            vec![TableLens {
                source_table: "counts".to_owned(),
                target_table: "counts".to_owned(),
                ops: vec![],
            }],
        ).expect("valid migration lens"),
        Vec::<String>::new(),
        Vec::<String>::new(),
    )
    .unwrap();

    let target = &core.catalogue.physical_mappings[&evolved.id].tables["counts"];
    assert_eq!(target.table_id, source.table_id);
    assert_ne!(target.columns["value"], source.columns["value"]);
}

/// An authority may advance its write schema while a client still has an
/// exclusive transaction authored under the prior schema. The table's logical
/// name can have changed, but the content and deletion CAS parents must still
/// be compared against the shared physical registers.
#[test]
fn old_schema_exclusive_cas_follows_renamed_table_physical_registers() {
    let base = schema();
    let renamed_schema = renamed_tasks_schema();
    let renamed = SchemaVersion::new(renamed_schema);
    let (_client_dir, mut accepted_client) = open_node_with_schema(node(0x61), base.clone());
    let (_stale_client_dir, mut stale_client) = open_node_with_schema(node(0x64), base.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(0x62), base.clone());
    let target_row = row(0x63);

    // Seed both independent registers. A replacement plus restore must CAS
    // the original content parent and the original deletion parent separately.
    for commit in [
        MergeableCommit::new("todos", target_row, 10).cells(title_cells("base")),
        MergeableCommit::new("todos", target_row, 11).deletion(DeletionEvent::Deleted),
    ] {
        let (published, unit) = accepted_client.commit_mergeable_unit(commit).unwrap();
        settle_published(&mut accepted_client, published).unwrap();
        let [fate] = core
            .apply_sync_message_settled(unit.clone())
            .unwrap()
            .try_into()
            .unwrap();
        stale_client.apply_sync_message_settled(unit).unwrap();
        stale_client.apply_sync_message_settled(fate.clone()).unwrap();
        accepted_client.apply_sync_message_settled(fate).unwrap();
    }

    let accepted = OpenTransactionId::new();
    let stale = OpenTransactionId::new();
    for (client, tx) in [(&mut accepted_client, accepted), (&mut stale_client, stale)] {
        client.open_exclusive(tx).unwrap();
        client
            .tx_write(tx, "todos", target_row, title_cells("replacement"), None)
            .unwrap();
        client
            .tx_write(
                tx,
                "todos",
                target_row,
                BTreeMap::<String, Value>::new(),
                Some(DeletionEvent::Restored),
            )
            .unwrap();
    }

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

    let (_accepted_id, accepted_unit) = accepted_client
        .commit_exclusive_settled(accepted, AuthorSubject::SYSTEM, 12)
        .unwrap();
    let [accepted_fate] = core
        .apply_sync_message_settled(accepted_unit)
        .unwrap()
        .try_into()
        .unwrap();
    assert!(matches!(
        accepted_fate,
        SyncMessage::FateUpdate {
            fate: Fate::Accepted,
            ..
        }
    ));

    // Planted stale-parent sensitivity: the second old-schema transaction has
    // exactly the pre-rename parents, so it must be rejected after the first
    // one advanced both shared physical registers.
    let (_stale_id, stale_unit) = stale_client
        .commit_exclusive_settled(stale, AuthorSubject::SYSTEM, 13)
        .unwrap();
    let [stale_fate] = core
        .apply_sync_message_settled(stale_unit)
        .unwrap()
        .try_into()
        .unwrap();
    assert!(matches!(
        stale_fate,
        SyncMessage::FateUpdate {
            fate: Fate::Rejected(RejectionReason::ExclusiveConflict),
            ..
        }
    ));
}
