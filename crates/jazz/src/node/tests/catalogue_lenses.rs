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
fn trusted_catalogue_snapshot_installs_lineage_before_authored_payloads() {
    // This is an internal transport-boundary test: public clients never apply
    // trusted upstream catalogue snapshots directly.
    let base = schema();
    let evolved = SchemaVersion::new(catalogue_evolved_schema());
    let lens = MigrationLens::new(
        base.version_id(),
        evolved.id,
        vec![TableLens {
            source_table: "todos".to_owned(),
            target_table: "todos".to_owned(),
            ops: vec![LensOp::AddColumn {
                column: "body".to_owned(),
                default: v(""),
            }],
        }],
    );
    let (_authority_dir, mut authority) = open_node_with_schema(node(0x35), base.clone());
    publish_schema_lineage(
        &mut authority,
        evolved.clone(),
        lens,
        Vec::<String>::new(),
        Vec::<String>::new(),
    )
    .unwrap();
    authority
        .apply_trusted_catalogue_message(SyncMessage::SetCurrentWriteSchema {
            author: AuthorId::SYSTEM,
            pointer: CurrentWriteSchema {
                revision: 1,
                schema: evolved.id,
            },
        })
        .unwrap();
    let (_, authored) = authority
        .commit_mergeable_unit(
            MergeableCommit::new("todos", row(0x36), 10).cells(BTreeMap::from([
                ("title".to_owned(), v("authored")),
                ("body".to_owned(), v("under-evolved-schema")),
            ])),
        )
        .unwrap();

    let (_receiver_dir, mut receiver) = open_node_with_schema(node(0x37), base);
    let snapshot = authority.catalogue_snapshot();
    assert!(matches!(
        receiver.apply_sync_message(SyncMessage::CatalogueSnapshot(Box::new(snapshot.clone()))),
        Err(Error::UnsupportedSyncMessage(
            "catalogue snapshot requires a trusted upstream link"
        ))
    ));
    receiver.apply_trusted_catalogue_snapshot(snapshot).unwrap();
    assert_eq!(receiver.current_write_schema().schema, evolved.id);
    receiver.apply_sync_message(authored).unwrap();
    let versions = receiver.query_all_versions().unwrap();
    assert_eq!(versions.len(), 1);
    assert_eq!(
        receiver
            .schema_version_for_alias(versions[0].schema_version_alias())
            .unwrap(),
        evolved.id
    );
}

#[test]
fn catalogue_snapshot_preserves_active_schema_storage_identity() {
    // Internal because schema aliases are node-local storage identities; the
    // public behavior is that writes remain valid after catalogue bootstrap.
    let base = schema();
    let evolved = SchemaVersion::new(catalogue_evolved_schema());
    let (_authority_dir, mut authority) = open_node_with_schema(node(0x60), base.clone());
    publish_schema_lineage(
        &mut authority,
        evolved.clone(),
        MigrationLens::new(
            base.version_id(),
            evolved.id,
            vec![TableLens {
                source_table: "todos".to_owned(),
                target_table: "todos".to_owned(),
                ops: vec![LensOp::AddColumn {
                    column: "body".to_owned(),
                    default: v(""),
                }],
            }],
        ),
        Vec::<String>::new(),
        Vec::<String>::new(),
    )
    .unwrap();
    authority
        .apply_trusted_catalogue_message(SyncMessage::SetCurrentWriteSchema {
            author: AuthorId::SYSTEM,
            pointer: CurrentWriteSchema {
                revision: 1,
                schema: evolved.id,
            },
        })
        .unwrap();

    let (_receiver_dir, mut receiver) =
        open_node_with_schema(node(0x61), evolved.schema.clone());
    let local_alias = receiver.catalogue.current_schema_version_alias.unwrap();
    let local_mapping = receiver.catalogue.physical_mappings[&evolved.id].clone();
    let (tx_id, _) = receiver
        .commit_mergeable_unit(
            MergeableCommit::new("todos", row(0x62), 10).cells(BTreeMap::from([
                ("title".to_owned(), v("before-snapshot")),
                ("body".to_owned(), v("authored-evolved")),
            ])),
        )
        .unwrap();
    receiver
        .apply_trusted_catalogue_snapshot(authority.catalogue_snapshot())
        .unwrap();

    assert_eq!(
        receiver.catalogue.current_schema_version_alias,
        Some(local_alias)
    );
    assert_eq!(receiver.catalogue.physical_mappings[&evolved.id], local_mapping);
    let stored = receiver.query_versions_for_tx(tx_id).unwrap();
    assert_eq!(stored.len(), 1);
    assert_eq!(stored[0].schema_version_alias(), local_alias);
    receiver.physical_table_id_for_version(&stored[0]).unwrap();
}

#[test]
fn settled_view_projects_authored_row_into_clients_active_schema() {
    // Internal because settled result-set installation is a sync receiver
    // boundary; schema projection itself is asserted through the query API.
    let base = schema();
    let evolved = SchemaVersion::new(catalogue_evolved_schema());
    let (_authority_dir, mut authority) = open_node_with_schema(node(0x63), base.clone());
    publish_schema_lineage(
        &mut authority,
        evolved.clone(),
        MigrationLens::new(
            base.version_id(),
            evolved.id,
            vec![TableLens {
                source_table: "todos".to_owned(),
                target_table: "todos".to_owned(),
                ops: vec![LensOp::AddColumn {
                    column: "body".to_owned(),
                    default: v("default-body"),
                }],
            }],
        ),
        Vec::<String>::new(),
        Vec::<String>::new(),
    )
    .unwrap();
    authority
        .apply_trusted_catalogue_message(SyncMessage::SetCurrentWriteSchema {
            author: AuthorId::SYSTEM,
            pointer: CurrentWriteSchema {
                revision: 1,
                schema: evolved.id,
            },
        })
        .unwrap();

    let (_writer_dir, mut writer) = open_node_with_schema(node(0x64), base);
    let (tx_id, unit) = writer
        .commit_mergeable_unit(
            MergeableCommit::new("todos", row(0x65), 10).cells(title_cells("authored-base")),
        )
        .unwrap();
    let SyncMessage::CommitUnit { tx, versions } = unit else {
        panic!("commit unit expected");
    };

    let (_receiver_dir, mut receiver) =
        open_node_with_schema(node(0x66), evolved.schema.clone());
    receiver
        .apply_trusted_catalogue_snapshot(authority.catalogue_snapshot())
        .unwrap();
    receiver
        .ingest_known_transaction(
            tx,
            versions,
            Fate::Accepted,
            Some(GlobalSeq(1)),
            DurabilityTier::Global,
        )
        .unwrap();

    let shape = Query::from("todos").validate(&evolved.schema).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    receiver.query.settled_result_sets.insert(
        crate::protocol::BindingViewKey {
            shape_id: shape.shape_id(),
            binding_id: binding.binding_id(),
            read_view: Default::default(),
        },
        BTreeSet::from([crate::protocol::ResultMemberEntry::row((
            groove::Intern::from("todos".to_owned()),
            row(0x65),
            tx_id,
        ))]),
    );

    let rows = receiver
        .query_rows(&shape, &binding, DurabilityTier::Global)
        .unwrap();
    assert_eq!(
        rows.into_iter()
            .map(current_row_pair)
            .collect::<BTreeMap<_, _>>(),
        BTreeMap::from([(
            row(0x65),
            BTreeMap::from([
                ("title".to_owned(), v("authored-base")),
                ("body".to_owned(), v("default-body")),
            ]),
        )])
    );
}

fn catalogue_snapshot_fixture() -> crate::protocol::CatalogueSnapshot {
    let base = schema();
    let evolved = SchemaVersion::new(catalogue_evolved_schema());
    let publication = SchemaLineagePublication::new(
        evolved.clone(),
        MigrationLens::new(
            base.version_id(),
            evolved.id,
            vec![TableLens {
                source_table: "todos".to_owned(),
                target_table: "todos".to_owned(),
                ops: vec![LensOp::AddColumn {
                    column: "body".to_owned(),
                    default: v(""),
                }],
            }],
        ),
        Vec::<String>::new(),
        Vec::<String>::new(),
    );
    crate::protocol::CatalogueSnapshot {
        schemas: vec![SchemaVersion::new(base), evolved.clone()],
        lineages: vec![(1, publication)],
        current_write_schema: CurrentWriteSchema {
            revision: 1,
            schema: evolved.id,
        },
    }
}

#[test]
fn trusted_catalogue_snapshot_rejects_invalid_later_lineage_without_prefix_activation() {
    let base = schema();
    let (dir, mut core) = open_node_with_schema(node(0x38), base.clone());
    let mut snapshot = catalogue_snapshot_fixture();
    snapshot.lineages.push((2, snapshot.lineages[0].1.clone()));

    assert!(matches!(
        core.apply_trusted_catalogue_snapshot(snapshot),
        Err(Error::InvalidCatalogueUpdate(_))
    ));
    assert_eq!(core.active_catalogue_seq(), 0);
    assert_eq!(core.catalogue_schemas().len(), 1);
    assert_eq!(core.current_write_schema().revision, 0);
    drop(core);

    let reopened = reopen_node_at(&dir, node(0x38), base);
    assert_eq!(reopened.active_catalogue_seq(), 0);
    assert_eq!(reopened.catalogue_schemas().len(), 1);
    assert_eq!(reopened.current_write_schema().revision, 0);
}

#[test]
fn trusted_catalogue_snapshot_rejects_pointer_conflict_without_lineage_activation() {
    let base = schema();
    let (dir, mut core) = open_node_with_schema(node(0x39), base.clone());
    let mut snapshot = catalogue_snapshot_fixture();
    snapshot.current_write_schema.revision = 0;

    assert!(matches!(
        core.apply_trusted_catalogue_snapshot(snapshot),
        Err(Error::InvalidCatalogueUpdate(_))
    ));
    assert_eq!(core.active_catalogue_seq(), 0);
    assert_eq!(core.catalogue_schemas().len(), 1);
    drop(core);

    let reopened = reopen_node_at(&dir, node(0x39), base);
    assert_eq!(reopened.active_catalogue_seq(), 0);
    assert_eq!(reopened.catalogue_schemas().len(), 1);
    assert_eq!(reopened.current_write_schema().revision, 0);
}

#[test]
fn trusted_catalogue_snapshot_activation_failure_never_exposes_a_prefix_and_reopens_old() {
    let base = schema();
    let (dir, mut core) = open_node_with_schema(node(0x3a), base.clone());
    core.set_catalogue_activation_failpoint(
        CatalogueActivationFailpoint::BeforeSnapshotActivationCommit,
    );

    assert!(matches!(
        core.apply_trusted_catalogue_snapshot(catalogue_snapshot_fixture()),
        Err(Error::CatalogueActivationFailed)
    ));
    assert_eq!(core.active_catalogue_seq(), 0);
    assert_eq!(core.catalogue_schemas().len(), 1);
    assert_eq!(core.current_write_schema().revision, 0);
    assert!(matches!(
        core.apply_trusted_catalogue_message(SyncMessage::SetCurrentWriteSchema {
            author: AuthorId::SYSTEM,
            pointer: CurrentWriteSchema {
                revision: 1,
                schema: base.version_id(),
            },
        }),
        Err(Error::CatalogueActivationFailed)
    ));
    drop(core);

    let mut reopened = reopen_node_at(&dir, node(0x3a), base);
    assert_eq!(reopened.active_catalogue_seq(), 0);
    assert_eq!(reopened.catalogue_schemas().len(), 1);
    assert_eq!(reopened.current_write_schema().revision, 0);
    reopened
        .apply_trusted_catalogue_snapshot(catalogue_snapshot_fixture())
        .unwrap();
    assert_eq!(reopened.active_catalogue_seq(), 1);
    assert_eq!(reopened.catalogue_schemas().len(), 2);
    assert_eq!(reopened.current_write_schema().revision, 1);
}

#[test]
fn physical_identity_mapping_and_live_id_recovery_are_durable_catalogue_metadata() {
    // Physical topology is intentionally not public API, so this internal test
    // verifies the catalogue/recovery and local-allocation invariants directly.
    let schema = schema();
    let schema_version = schema.version_id();
    let (left_dir, left) = open_node_with_schema(node(0x2d), schema.clone());

    let left_mapping = left.catalogue.physical_mappings[&schema_version].clone();
    let todos = &left_mapping.tables["todos"];
    assert_ne!(todos.table_id.0, 0);
    assert_ne!(todos.columns["title"].0, 0);
    drop(left);

    let mut reopened = reopen_node_at(&left_dir, node(0x2d), schema.clone());
    assert_eq!(
        reopened.catalogue.physical_mappings[&schema_version],
        left_mapping
    );

    let evolved = SchemaVersion::new(catalogue_evolved_schema());
    publish_schema_lineage(
        &mut reopened,
        evolved.clone(),
        MigrationLens::new(
            schema.version_id(),
            evolved.id,
            vec![TableLens {
                source_table: "todos".to_owned(),
                target_table: "todos".to_owned(),
                ops: vec![LensOp::AddColumn {
                    column: "body".to_owned(),
                    default: v(""),
                }],
            }],
        ),
        Vec::<String>::new(),
        Vec::<String>::new(),
    )
    .unwrap();
    let next = &reopened.catalogue.physical_mappings[&evolved.id].tables["todos"];
    assert_eq!(next.table_id, todos.table_id);
    assert_eq!(next.columns["title"], todos.columns["title"]);
    assert!(next.columns["body"].0 > todos.columns["title"].0);
}

#[test]
fn non_genesis_schema_activates_only_with_its_ordered_lineage_bundle() {
    // Physical topology and the staged catalogue state are not public API, so
    // this internal test pins their atomic admission boundary directly.
    let base = schema();
    let source_id = base.version_id();
    let target = SchemaVersion::new(catalogue_evolved_schema());
    let lens = MigrationLens::new(
        source_id,
        target.id,
        vec![TableLens {
            source_table: "todos".to_owned(),
            target_table: "todos".to_owned(),
            ops: vec![LensOp::AddColumn {
                column: "body".to_owned(),
                default: Value::String(String::new()),
            }],
        }],
    );
    let publication = SchemaLineagePublication::new(
        target.clone(),
        lens.clone(),
        Vec::<String>::new(),
        Vec::<String>::new(),
    );
    let (dir, mut core) = open_node_with_schema(node(0x2e), base.clone());

    let standalone = core.apply_trusted_catalogue_message(SyncMessage::PublishSchema {
        author: AuthorId::SYSTEM,
        schema: Box::new(target.clone()),
    });
    assert!(matches!(
        standalone,
        Err(Error::InvalidCatalogueUpdate(
            "non-genesis schema requires lineage publication"
        ))
    ));
    assert!(!core.catalogue_schemas().contains_key(&target.id));

    let ack = core
        .apply_trusted_catalogue_message(SyncMessage::PublishSchemaWithLens {
            author: AuthorId::SYSTEM,
            catalogue_seq: 1,
            publication: Box::new(publication.clone()),
        })
        .unwrap();
    assert!(matches!(
        ack.as_slice(),
        [SyncMessage::CatalogueAck(crate::protocol::CatalogueAck {
            revision: Some(1),
            schema: Some(schema),
            lens: Some(published_lens),
            applied: true,
        })] if *schema == target.id && *published_lens == lens.id
    ));
    let source = &core.catalogue.physical_mappings[&source_id].tables["todos"];
    let activated = &core.catalogue.physical_mappings[&target.id].tables["todos"];
    assert_eq!(activated.table_id, source.table_id);
    assert_eq!(activated.columns["title"], source.columns["title"]);

    let duplicate = core
        .apply_trusted_catalogue_message(SyncMessage::PublishSchemaWithLens {
            author: AuthorId::SYSTEM,
            catalogue_seq: 1,
            publication: Box::new(publication),
        })
        .unwrap();
    assert!(matches!(
        duplicate.as_slice(),
        [SyncMessage::CatalogueAck(_)]
    ));
    drop(core);

    let reopened = reopen_node_at(&dir, node(0x2e), base);
    assert!(reopened.catalogue_schemas().contains_key(&target.id));
    assert_eq!(
        reopened.catalogue.physical_mappings[&target.id].tables["todos"].table_id,
        reopened.catalogue.physical_mappings[&source_id].tables["todos"].table_id
    );
}

#[test]
fn durable_genesis_rejects_reopen_with_a_different_schema() {
    let base = schema();
    let (dir, core) = open_node_with_schema(node(0x2a), base);
    drop(core);

    let different = catalogue_evolved_schema();
    let cfs = different.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = RocksDbStorage::open(dir.path(), &refs).unwrap();
    let reopened = NodeState::new(node(0x2a), different, storage);
    assert!(matches!(
        reopened,
        Err(Error::InvalidStoredValue(
            "opened schema does not match durable catalogue genesis"
        ))
    ));
}

#[test]
fn pending_lineage_reserves_its_target_and_sequence() {
    let base = schema();
    let target = SchemaVersion::new(catalogue_evolved_schema());
    let lens = MigrationLens::new(
        base.version_id(),
        target.id,
        vec![TableLens {
            source_table: "todos".to_owned(),
            target_table: "todos".to_owned(),
            ops: vec![LensOp::AddColumn {
                column: "body".to_owned(),
                default: Value::String(String::new()),
            }],
        }],
    );
    let publication = SchemaLineagePublication::new(
        target.clone(),
        lens,
        Vec::<String>::new(),
        Vec::<String>::new(),
    );
    let conflicting_lens = MigrationLens::new(
        base.version_id(),
        target.id,
        vec![TableLens {
            source_table: "todos".to_owned(),
            target_table: "todos".to_owned(),
            ops: vec![LensOp::AddColumn {
                column: "body".to_owned(),
                default: Value::String("different semantic default".to_owned()),
            }],
        }],
    );
    let conflict = SchemaLineagePublication::new(
        target,
        conflicting_lens,
        Vec::<String>::new(),
        Vec::<String>::new(),
    );
    let (_dir, mut core) = open_node_with_schema(node(0x2b), base);

    assert!(
        core.apply_trusted_catalogue_message(SyncMessage::PublishSchemaWithLens {
            author: AuthorId::SYSTEM,
            catalogue_seq: 2,
            publication: Box::new(publication.clone()),
        })
        .unwrap()
        .is_empty()
    );
    assert!(matches!(
        core.apply_sync_message_with_ingest_context(
            SyncMessage::PublishSchemaWithLens {
                author: AuthorId::SYSTEM,
                catalogue_seq: 2,
                publication: Box::new(publication),
            },
            Some(CommitUnitIngestContext {
                identity: user(0x71),
                trust: CommitUnitTrust::Session,
                edge_authority: false,
            }),
        ),
        Err(Error::UnauthorizedCatalogueUpdate)
    ));
    assert!(matches!(
        core.apply_trusted_catalogue_message(SyncMessage::PublishSchemaWithLens {
            author: AuthorId::SYSTEM,
            catalogue_seq: 2,
            publication: Box::new(conflict.clone()),
        }),
        Err(Error::InvalidCatalogueUpdate(
            "schema lineage catalogue sequence conflict"
        ))
    ));
    assert!(matches!(
        core.apply_trusted_catalogue_message(SyncMessage::PublishSchemaWithLens {
            author: AuthorId::SYSTEM,
            catalogue_seq: 3,
            publication: Box::new(conflict),
        }),
        Err(Error::InvalidCatalogueUpdate(
            "schema lineage target is already reserved"
        ))
    ));
}

#[test]
fn lineage_operations_must_exhaustively_reproduce_target_columns_before_staging() {
    let base = schema();
    let target = SchemaVersion::new(catalogue_evolved_schema());
    let incomplete_lens = MigrationLens::new(
        base.version_id(),
        target.id,
        vec![TableLens {
            source_table: "todos".to_owned(),
            target_table: "todos".to_owned(),
            ops: Vec::new(),
        }],
    );
    let publication = SchemaLineagePublication::new(
        target.clone(),
        incomplete_lens,
        Vec::<String>::new(),
        Vec::<String>::new(),
    );
    let (_dir, mut core) = open_node_with_schema(node(0x29), base);
    let next_table = core.catalogue.next_physical_table_id;
    let next_column = core.catalogue.next_physical_column_id;

    assert!(matches!(
        core.apply_trusted_catalogue_message(SyncMessage::PublishSchemaWithLens {
            author: AuthorId::SYSTEM,
            catalogue_seq: 1,
            publication: Box::new(publication),
        }),
        Err(Error::InvalidCatalogueUpdate(
            "lens operations do not reproduce target columns"
        ))
    ));
    assert!(!core.catalogue_schemas().contains_key(&target.id));
    assert!(core.catalogue.staged_lineages.is_empty());
    assert_eq!(core.catalogue.next_physical_table_id, next_table);
    assert_eq!(core.catalogue.next_physical_column_id, next_column);

    let correction = SchemaLineagePublication::new(
        target.clone(),
        MigrationLens::new(
            core.catalogue.schema.version_id(),
            target.id,
            vec![TableLens {
                source_table: "todos".to_owned(),
                target_table: "todos".to_owned(),
                ops: vec![LensOp::AddColumn {
                    column: "body".to_owned(),
                    default: Value::String(String::new()),
                }],
            }],
        ),
        Vec::<String>::new(),
        Vec::<String>::new(),
    );
    core.apply_trusted_catalogue_message(SyncMessage::PublishSchemaWithLens {
        author: AuthorId::SYSTEM,
        catalogue_seq: 1,
        publication: Box::new(correction),
    })
    .unwrap();
    assert!(core.catalogue_schemas().contains_key(&target.id));
}

#[test]
fn schema_lineage_gaps_and_inactive_sources_park_durably_then_drain_in_order() {
    let v1 = schema();
    let v2 = SchemaVersion::new(catalogue_evolved_schema());
    let v3 = SchemaVersion::new(JazzSchema::new([TableSchema::new(
        "todos",
        [
            ColumnSchema::new("title", ColumnType::String),
            ColumnSchema::new("body", ColumnType::String),
            ColumnSchema::new("archived", ColumnType::Bool),
        ],
    )]));
    let lens_12 = MigrationLens::new(
        v1.version_id(),
        v2.id,
        vec![TableLens {
            source_table: "todos".to_owned(),
            target_table: "todos".to_owned(),
            ops: vec![LensOp::AddColumn {
                column: "body".to_owned(),
                default: Value::String(String::new()),
            }],
        }],
    );
    let lens_23 = MigrationLens::new(
        v2.id,
        v3.id,
        vec![TableLens {
            source_table: "todos".to_owned(),
            target_table: "todos".to_owned(),
            ops: vec![LensOp::AddColumn {
                column: "archived".to_owned(),
                default: Value::Bool(false),
            }],
        }],
    );
    let publication_1 = SchemaLineagePublication::new(
        v2.clone(),
        lens_12,
        Vec::<String>::new(),
        Vec::<String>::new(),
    );
    let publication_2 = SchemaLineagePublication::new(
        v3.clone(),
        lens_23,
        Vec::<String>::new(),
        Vec::<String>::new(),
    );
    let (dir, mut core) = open_node_with_schema(node(0x2c), v1.clone());

    let parked = core
        .apply_trusted_catalogue_message(SyncMessage::PublishSchemaWithLens {
            author: AuthorId::SYSTEM,
            catalogue_seq: 2,
            publication: Box::new(publication_2),
        })
        .unwrap();
    assert!(parked.is_empty());
    assert!(!core.catalogue_schemas().contains_key(&v2.id));
    assert!(!core.catalogue_schemas().contains_key(&v3.id));
    drop(core);

    let mut reopened = reopen_node_at(&dir, node(0x2c), v1);
    assert!(!reopened.catalogue_schemas().contains_key(&v3.id));
    let drained = reopened
        .apply_trusted_catalogue_message(SyncMessage::PublishSchemaWithLens {
            author: AuthorId::SYSTEM,
            catalogue_seq: 1,
            publication: Box::new(publication_1),
        })
        .unwrap();
    assert_eq!(
        drained
            .iter()
            .filter(|message| matches!(message, SyncMessage::CatalogueAck(_)))
            .count(),
        2
    );
    assert!(reopened.catalogue_schemas().contains_key(&v2.id));
    assert!(reopened.catalogue_schemas().contains_key(&v3.id));
    // This is the ordering evidence enum lowering consumes: the out-of-order
    // seq-2 envelope did not allocate an alias when it was merely parked.
    // After seq-1 activates, the drained child receives the later alias, so a
    // physical enum registry can append rather than reinterpret an existing
    // local tag according to network receipt order.
    assert_eq!(
        reopened.catalogue.schema_version_aliases[&v2.id],
        SchemaVersionAlias(2)
    );
    assert_eq!(
        reopened.catalogue.schema_version_aliases[&v3.id],
        SchemaVersionAlias(3)
    );
}

#[test]
fn malformed_unknown_source_bundle_is_quarantined_when_parent_arrives() {
    let v1 = schema();
    let v2 = SchemaVersion::new(catalogue_evolved_schema());
    let v3 = SchemaVersion::new(JazzSchema::new([TableSchema::new(
        "todos",
        [
            ColumnSchema::new("title", ColumnType::String),
            ColumnSchema::new("body", ColumnType::String),
            ColumnSchema::new("archived", ColumnType::Bool),
        ],
    )]));
    let parent = SchemaLineagePublication::new(
        v2.clone(),
        MigrationLens::new(
            v1.version_id(),
            v2.id,
            vec![TableLens {
                source_table: "todos".to_owned(),
                target_table: "todos".to_owned(),
                ops: vec![LensOp::AddColumn {
                    column: "body".to_owned(),
                    default: Value::String(String::new()),
                }],
            }],
        ),
        Vec::<String>::new(),
        Vec::<String>::new(),
    );
    let malformed = SchemaLineagePublication::new(
        v3.clone(),
        MigrationLens::new(
            v2.id,
            v3.id,
            vec![TableLens {
                source_table: "todos".to_owned(),
                target_table: "todos".to_owned(),
                ops: Vec::new(),
            }],
        ),
        Vec::<String>::new(),
        Vec::<String>::new(),
    );
    let valid = SchemaLineagePublication::new(
        v3.clone(),
        MigrationLens::new(
            v2.id,
            v3.id,
            vec![TableLens {
                source_table: "todos".to_owned(),
                target_table: "todos".to_owned(),
                ops: vec![LensOp::AddColumn {
                    column: "archived".to_owned(),
                    default: Value::Bool(false),
                }],
            }],
        ),
        Vec::<String>::new(),
        Vec::<String>::new(),
    );
    let (dir, mut core) = open_node_with_schema(node(0x2d), v1.clone());

    assert!(
        core.apply_trusted_catalogue_message(SyncMessage::PublishSchemaWithLens {
            author: AuthorId::SYSTEM,
            catalogue_seq: 2,
            publication: Box::new(malformed),
        })
        .unwrap()
        .is_empty()
    );
    core.apply_trusted_catalogue_message(SyncMessage::PublishSchemaWithLens {
        author: AuthorId::SYSTEM,
        catalogue_seq: 1,
        publication: Box::new(parent),
    })
    .unwrap();
    assert!(core.catalogue_schemas().contains_key(&v2.id));
    assert!(!core.catalogue_schemas().contains_key(&v3.id));
    assert!(!core.catalogue.pending_lineages.contains_key(&2));
    drop(core);

    let mut core = reopen_node_at(&dir, node(0x2d), v1);
    assert!(!core.catalogue.pending_lineages.contains_key(&2));

    core.apply_trusted_catalogue_message(SyncMessage::PublishSchemaWithLens {
        author: AuthorId::SYSTEM,
        catalogue_seq: 2,
        publication: Box::new(valid),
    })
    .unwrap();
    assert!(core.catalogue_schemas().contains_key(&v3.id));
}

#[test]
fn staged_lineage_resumes_after_each_activation_crash_boundary() {
    for (byte, failpoint) in [
        (0x25, CatalogueActivationFailpoint::AfterStaged),
        (0x26, CatalogueActivationFailpoint::AfterRegistration),
    ] {
        let base = schema();
        let target = SchemaVersion::new(catalogue_evolved_schema());
        let lens = MigrationLens::new(
            base.version_id(),
            target.id,
            vec![TableLens {
                source_table: "todos".to_owned(),
                target_table: "todos".to_owned(),
                ops: vec![LensOp::AddColumn {
                    column: "body".to_owned(),
                    default: Value::String(String::new()),
                }],
            }],
        );
        let publication = SchemaLineagePublication::new(
            target.clone(),
            lens,
            Vec::<String>::new(),
            Vec::<String>::new(),
        );
        let (dir, mut core) = open_node_with_schema(node(byte), base.clone());
        core.set_catalogue_activation_failpoint(failpoint);

        assert!(matches!(
            core.apply_trusted_catalogue_message(SyncMessage::PublishSchemaWithLens {
                author: AuthorId::SYSTEM,
                catalogue_seq: 1,
                publication: Box::new(publication),
            }),
            Err(Error::CatalogueActivationFailed)
        ));
        assert!(!core.catalogue_schemas().contains_key(&target.id));
        assert!(matches!(
            core.apply_trusted_catalogue_message(SyncMessage::PublishSchema {
                author: AuthorId::SYSTEM,
                schema: Box::new(target.clone()),
            }),
            Err(Error::CatalogueActivationFailed)
        ));
        drop(core);

        let reopened = reopen_node_at(&dir, node(byte), base);
        assert!(reopened.catalogue_schemas().contains_key(&target.id));
        assert_eq!(reopened.active_catalogue_seq(), 1);
    }
}

#[test]
fn publishing_lens_reconciles_target_table_and_column_identities_durably() {
    // Physical topology is intentionally not public API, so this internal test
    // verifies identity reconciliation and live-mapping recovery directly.
    let base = schema();
    let evolved = catalogue_evolved_schema();
    let source_id = base.version_id();
    let target = SchemaVersion::new(evolved);
    let (dir, mut core) = open_node_with_schema(node(0x2f), base.clone());
    let source_table = core.catalogue.physical_mappings[&source_id].tables["todos"].clone();

    let lens = MigrationLens::new(
        source_id,
        target.id,
        vec![TableLens {
            source_table: "todos".to_owned(),
            target_table: "todos".to_owned(),
            ops: vec![LensOp::AddColumn {
                column: "body".to_owned(),
                default: Value::String(String::new()),
            }],
        }],
    );
    publish_schema_lineage(
        &mut core,
        target.clone(),
        lens,
        Vec::<String>::new(),
        Vec::<String>::new(),
    )
    .unwrap();

    let reconciled = core.catalogue.physical_mappings[&target.id].tables["todos"].clone();
    assert_eq!(reconciled.table_id, source_table.table_id);
    assert_eq!(reconciled.columns["title"], source_table.columns["title"]);
    assert_ne!(reconciled.columns["body"], source_table.columns["title"]);
    let mapping = core.catalogue.physical_mappings[&target.id].clone();
    let max_live_table_id = mapping
        .tables
        .values()
        .map(|table| table.table_id.0)
        .max()
        .unwrap();
    let max_live_column_id = mapping
        .tables
        .values()
        .flat_map(|table| table.columns.values())
        .map(|column| column.0)
        .max()
        .unwrap();
    drop(core);

    let mut reopened = reopen_node_at(&dir, node(0x2f), base);
    assert_eq!(reopened.catalogue.physical_mappings[&target.id], mapping);
    let later = SchemaVersion::new(JazzSchema::new([TableSchema::new(
        "notes",
        [ColumnSchema::new("text", ColumnType::String)],
    )]));
    publish_schema_lineage(
        &mut reopened,
        later.clone(),
        MigrationLens::new(target.id, later.id, vec![]),
        ["notes"],
        ["todos"],
    )
    .unwrap();
    let later_table = &reopened.catalogue.physical_mappings[&later.id].tables["notes"];
    assert!(later_table.table_id.0 > max_live_table_id);
    assert!(later_table.columns["text"].0 > max_live_column_id);
}

#[test]
fn active_history_projection_accepts_a_new_schema_variant_without_rebuild() {
    let base = schema();
    let evolved = SchemaVersion::new(catalogue_evolved_schema());
    let (dir, mut core) = open_node_with_schema(node(0x2e), base.clone());
    let subscription = core.subscribe_history("todos").unwrap();
    assert!(subscription.recv().unwrap().is_empty());
    core.commit_mergeable(
        MergeableCommit::new("todos", row(0x44), 900).cells(title_cells("old-title")),
    )
    .unwrap();
    assert_eq!(
        subscription
            .recv()
            .unwrap()
            .iter()
            .filter(|(_, weight)| *weight > 0)
            .count(),
        1,
    );
    let runtime = core.groove_runtime_token();

    publish_schema_lineage(
        &mut core,
        evolved.clone(),
        MigrationLens::new(
            base.version_id(),
            evolved.id,
            vec![TableLens {
                source_table: "todos".to_owned(),
                target_table: "todos".to_owned(),
                ops: vec![LensOp::AddColumn {
                    column: "body".to_owned(),
                    default: Value::String(String::new()),
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
            schema: evolved.id,
        },
    })
    .unwrap();
    assert_ne!(
        core.groove_runtime_token(),
        runtime,
        "widening a shared current-row descriptor invalidates prepared graphs"
    );

    core.commit_mergeable(
        MergeableCommit::new("todos", row(0x45), 1_000).cells(BTreeMap::from([
            ("title".to_owned(), Value::String("new-title".to_owned())),
            ("body".to_owned(), Value::String("new-body".to_owned())),
        ])),
    )
    .unwrap();

    let deltas = subscription.recv().unwrap();
    let rows = deltas
        .iter()
        .filter(|(_, weight)| *weight > 0)
        .collect::<Vec<_>>();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].0.descriptor(),
        base.tables[0].history_storage_table().record_schema(),
    );
    assert_eq!(
        core.version_storage_sources_for_layer("todos", VersionLayer::Content)
            .unwrap()
            .len(),
        1,
    );

    drop(subscription);
    drop(core);
    let mut reopened = reopen_node_at(&dir, node(0x2e), base);
    let versions = reopened.query_table_versions("todos").unwrap();
    assert_eq!(versions.len(), 2);
    assert_eq!(
        versions
            .iter()
            .map(VersionRow::schema_version_alias)
            .collect::<BTreeSet<_>>()
            .len(),
        2,
    );
}

#[test]
fn current_write_descriptor_cache_is_dropped_before_shared_enum_registry_widens() {
    // This is an internal hot-path receipt: an old-schema physical write
    // descriptor embeds the shared enum registry as it existed when cached.
    // Activating a later case widens that same physical table. An old-schema
    // write after activation must rebuild against the widened registry even
    // though the authored value still uses the old ordinal space.
    let base = enum_projection_schema(&["open"]);
    let evolved = SchemaVersion::new(enum_projection_schema(&["open", "closed"]));
    let (_dir, mut core) = open_node_with_schema(node(0x2f), base.clone());

    accept_global(
        &mut core,
        MergeableCommit::new("items", row(0x46), 900).cells(BTreeMap::from([
            ("title".to_owned(), v("before widening")),
            ("status".to_owned(), Value::EnumTag(0)),
        ])),
    );
    assert_eq!(
        core.catalogue.physical_current_write_descriptor_cache.len(),
        2,
        "the local ahead write and its global fate use distinct physical partitions"
    );

    publish_schema_lineage(
        &mut core,
        evolved.clone(),
        enum_identity_lens(base.version_id(), evolved.id),
        Vec::<String>::new(),
        Vec::<String>::new(),
    )
    .unwrap();
    assert!(
        core.catalogue.physical_current_write_descriptor_cache.is_empty(),
        "enum-registry activation must evict pre-widening descriptors"
    );

    // Keep the base write pointer deliberately: this is the stale-descriptor
    // hazard. The old authored enum stays ordinal 0 while its physical
    // descriptor must now include both globally registered cases.
    let after = row(0x47);
    accept_global(
        &mut core,
        MergeableCommit::new("items", after, 1_000).cells(BTreeMap::from([
            ("title".to_owned(), v("old schema after widening")),
            ("status".to_owned(), Value::EnumTag(0)),
        ])),
    );
    assert_eq!(core.catalogue.physical_current_write_descriptor_cache.len(), 2);
    let rows = core
        .current_rows_for_schema("items", base.version_id(), DurabilityTier::Global)
        .unwrap();
    assert!(rows.iter().any(|current| {
        current.row_uuid() == after
            && current.cell(&base.tables[0], "status") == Some(Value::EnumTag(0))
    }));
}

#[test]
fn table_and_column_rename_reuses_the_existing_physical_identities() {
    let base = schema();
    let renamed = SchemaVersion::new(JazzSchema::new([TableSchema::new(
        "tasks",
        [ColumnSchema::new("name", ColumnType::String)],
    )]));
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
        ),
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
    let renamed_schema = JazzSchema::new([TableSchema::new(
        "tasks",
        [ColumnSchema::new("name", ColumnType::String)],
    )]);
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
        ),
        Vec::<String>::new(),
        Vec::<String>::new(),
    )
    .unwrap();
    core.apply_trusted_catalogue_message(SyncMessage::SetCurrentWriteSchema {
        author: AuthorId::SYSTEM,
        pointer: CurrentWriteSchema {
            revision: 1,
            schema: renamed.id,
        },
    })
    .unwrap();

    let tx = OpenBatchId::new();
    core.open_exclusive(tx).unwrap();
    core.tx_write(
        tx,
        "tasks",
        row(0x35),
        BTreeMap::from([("name".to_owned(), v("retry renamed"))]),
        None,
    )
    .unwrap();
    let (rejected, _unit) = core.commit_exclusive(tx, AuthorId::SYSTEM, 10).unwrap();
    core.apply_sync_message(SyncMessage::FateUpdate {
        tx_id: rejected,
        fate: Fate::Rejected(RejectionReason::ExclusiveConflict),
        global_seq: None,
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
    let renamed_schema = JazzSchema::new([TableSchema::new(
        "tasks",
        [ColumnSchema::new("name", ColumnType::String)],
    )]);
    let renamed = SchemaVersion::new(renamed_schema.clone());
    let (dir, mut core) = open_node_with_schema(node(0x2b), base.clone());
    let row_uuid = row(0x4b);
    core.commit_mergeable(MergeableCommit::new("todos", row_uuid, 10).cells(title_cells("shared")))
        .unwrap();
    core.commit_mergeable(
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
        ),
        Vec::<String>::new(),
        Vec::<String>::new(),
    )
    .unwrap();
    core.apply_trusted_catalogue_message(SyncMessage::SetCurrentWriteSchema {
        author: AuthorId::SYSTEM,
        pointer: CurrentWriteSchema {
            revision: 1,
            schema: renamed.id,
        },
    })
    .unwrap();
    core.commit_mergeable(
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
                    Value::U8(0),
                    Value::Uuid(uuid::Uuid::nil()),
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
        .commit_mergeable(
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

#[test]
fn shared_deletion_history_keeps_same_row_uuid_table_scoped() {
    let schema = JazzSchema::new([
        TableSchema::new("todos", [ColumnSchema::new("title", ColumnType::String)]),
        TableSchema::new("notes", [ColumnSchema::new("body", ColumnType::String)]),
    ]);
    let (dir, mut core) = open_node_with_schema(node(0x2c), schema.clone());
    let shared_row = row(0x4c);
    core.commit_mergeable(
        MergeableCommit::new("todos", shared_row, 10).cells(BTreeMap::from([(
            "title".to_owned(),
            v("todo"),
        )])),
    )
    .unwrap();
    core.commit_mergeable(
        MergeableCommit::new("notes", shared_row, 11).cells(BTreeMap::from([(
            "body".to_owned(),
            v("note"),
        )])),
    )
    .unwrap();
    let deletion_tx = core
        .commit_mergeable_many(vec![
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
                        Value::U8(0),
                        Value::Uuid(uuid::Uuid::nil()),
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
    // The stored scalar representation remains U64, but counter and LWW cells
    // cannot share one physical column identity or its derived indexes.
    let base = JazzSchema::new([TableSchema::new(
        "counts",
        [ColumnSchema::new("value", ColumnType::U64)],
    )]);
    let evolved = SchemaVersion::new(JazzSchema::new([TableSchema::new(
        "counts",
        [ColumnSchema::new("value", ColumnType::U64)],
    )
    .with_column_merge_strategy("value", MergeStrategy::Counter)]));
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
        ),
        Vec::<String>::new(),
        Vec::<String>::new(),
    )
    .unwrap();

    let target = &core.catalogue.physical_mappings[&evolved.id].tables["counts"];
    assert_eq!(target.table_id, source.table_id);
    assert_ne!(target.columns["value"], source.columns["value"]);
}

#[test]
fn catalogue_schema_publish_replicates_and_is_idempotent() {
    let base = schema();
    let evolved = catalogue_evolved_schema();
    let (_core_dir, mut core) = open_node_with_schema(node(0x33), base.clone());
    let (_client_dir, mut client) = open_node_with_schema(node(0x34), base.clone());
    let payload = SchemaVersion::new(evolved.clone());
    let lens = MigrationLens::new(
        base.version_id(),
        payload.id,
        vec![TableLens {
            source_table: "todos".to_owned(),
            target_table: "todos".to_owned(),
            ops: vec![LensOp::AddColumn {
                column: "body".to_owned(),
                default: Value::String(String::new()),
            }],
        }],
    );
    let publish = SyncMessage::PublishSchemaWithLens {
        author: AuthorId::SYSTEM,
        catalogue_seq: 1,
        publication: Box::new(SchemaLineagePublication::new(
            payload.clone(),
            lens,
            Vec::<String>::new(),
            Vec::<String>::new(),
        )),
    };

    let ack = core
        .apply_trusted_catalogue_message(publish.clone())
        .unwrap();
    assert!(matches!(
        ack.as_slice(),
        [SyncMessage::CatalogueAck(crate::protocol::CatalogueAck {
            schema: Some(id),
            applied: true,
            ..
        })] if *id == payload.id
    ));
    assert!(core.catalogue_schemas().contains_key(&payload.id));

    let second = core
        .apply_trusted_catalogue_message(publish.clone())
        .unwrap();
    assert!(matches!(
        second.as_slice(),
        [SyncMessage::CatalogueAck(crate::protocol::CatalogueAck {
            schema: Some(id),
            applied: true,
            ..
        })] if *id == payload.id
    ));
    assert_eq!(core.catalogue_schemas().len(), 2);

    client.apply_trusted_catalogue_message(publish).unwrap();
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
    let lens = MigrationLens::new(
        source.id,
        target.id,
        vec![TableLens {
            source_table: "todos".to_owned(),
            target_table: "todos".to_owned(),
            ops: vec![LensOp::AddColumn {
                column: "body".to_owned(),
                default: Value::String(String::new()),
            }],
        }],
    );

    let publication = SchemaLineagePublication::new(
        target.clone(),
        lens.clone(),
        Vec::<String>::new(),
        Vec::<String>::new(),
    );
    let non_admin = core.apply_sync_message(SyncMessage::PublishSchemaWithLens {
        author: user(7),
        catalogue_seq: 1,
        publication: Box::new(publication.clone()),
    });
    assert!(matches!(non_admin, Err(Error::UnauthorizedCatalogueUpdate)));

    let unknown = MigrationLens::new(
        source.id,
        SchemaVersionId::from_bytes([0x99; 16]),
        Vec::new(),
    );
    let unknown_result = core.apply_trusted_catalogue_message(SyncMessage::PublishLens {
        author: AuthorId::SYSTEM,
        lens: unknown,
    });
    assert!(matches!(
        unknown_result,
        Err(Error::InvalidCatalogueUpdate("lens endpoint is unknown"))
    ));

    let ack = core
        .apply_trusted_catalogue_message(SyncMessage::PublishSchemaWithLens {
            author: AuthorId::SYSTEM,
            catalogue_seq: 1,
            publication: Box::new(publication),
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
    let evolved_cells = BTreeMap::from([
        ("body".to_owned(), Value::String(String::new())),
        ("title".to_owned(), Value::String("parked".to_owned())),
    ]);
    // The arriving unit is authored under the as-yet unknown evolved schema.
    // Its wire record is nevertheless complete in that schema: unknown-schema
    // parking delays admission; it does not turn an older, short row into an
    // evolved row by changing only its schema id.
    let (_writer_dir, mut writer) = open_node_with_schema(node(0x36), evolved.clone());
    let (core_dir, mut core) = open_node_with_schema(node(0x37), base.clone());
    let (_tx_id, unit) = writer
        .commit_mergeable_unit(
            MergeableCommit::new("todos", row(0x55), 1_000).cells(evolved_cells.clone()),
        )
        .unwrap();
    let SyncMessage::CommitUnit { tx, versions } = unit else {
        panic!("commit unit expected");
    };

    assert!(
        core.apply_sync_message(SyncMessage::CommitUnit {
            tx: tx.clone(),
            versions,
        })
        .unwrap()
        .is_empty()
    );
    assert_eq!(core.sync_metrics().parked_catalogue_orphans, 1);
    assert!(core.query_transaction(tx.tx_id).unwrap().is_none());

    let updates = publish_schema_lineage(
        &mut core,
        SchemaVersion::new(evolved.clone()),
        MigrationLens::new(
            base.version_id(),
            evolved_id,
            vec![TableLens {
                source_table: "todos".to_owned(),
                target_table: "todos".to_owned(),
                ops: vec![LensOp::AddColumn {
                    column: "body".to_owned(),
                    default: Value::String(String::new()),
                }],
            }],
        ),
        Vec::<String>::new(),
        Vec::<String>::new(),
    )
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
    let shape = Query::from("todos").validate(&evolved).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    for tier in [
        DurabilityTier::Local,
        DurabilityTier::Edge,
        DurabilityTier::Global,
    ] {
        assert_eq!(
            core.query_rows(&shape, &binding, tier)
                .unwrap()
                .into_iter()
                .map(current_row_pair)
                .collect::<BTreeMap<_, _>>()
                .get(&row(0x55)),
            Some(&evolved_cells),
            "catalogue-drained rows retain lens defaults in {tier:?} current reads",
        );
    }
    drop(core);
    let mut reopened = reopen_node_at(&core_dir, node(0x37), base);
    assert!(
        reopened
            .query_transaction(tx.tx_id)
            .unwrap()
            .is_some_and(|stored| stored.fate == Fate::Accepted)
    );
    assert_eq!(
        reopened
            .query_rows(&shape, &binding, DurabilityTier::Global)
            .unwrap()
            .into_iter()
            .map(current_row_pair)
            .collect::<BTreeMap<_, _>>()
            .get(&row(0x55)),
        Some(&evolved_cells)
    );
}

/// An unknown-schema unit may park, but once the catalogue identifies its
/// schema the authority rejects a record whose wire descriptor belongs to an
/// older schema instead of inventing a missing evolved column from a lens.
#[test]
fn catalogue_arrival_rejects_incomplete_row_claiming_evolved_schema() {
    let base = schema();
    let evolved = catalogue_evolved_schema();
    let evolved_id = evolved.version_id();
    let (_writer_dir, mut writer) = open_node_with_schema(node(0x58), base.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(0x59), base.clone());
    let (_tx_id, unit) = writer
        .commit_mergeable_unit(
            MergeableCommit::new("todos", row(0x58), 1_003).cells(title_cells("invalid")),
        )
        .unwrap();
    let SyncMessage::CommitUnit { tx, versions } = unit else {
        panic!("commit unit expected");
    };
    let incomplete = versions
        .into_iter()
        .map(|version| {
            crate::protocol::VersionRecord::from_cells(
                &base.tables[0],
                evolved_id,
                version.row_uuid(),
                version.parents(),
                version.created_by(),
                version.created_at(),
                version.updated_by(),
                version.updated_at(),
                &version_record_cells(&version, &base.tables[0]),
                version.deletion(),
            )
            .unwrap()
        })
        .collect();

    assert!(core
        .apply_sync_message(SyncMessage::CommitUnit {
            tx: tx.clone(),
            versions: incomplete,
        })
        .unwrap()
        .is_empty());
    assert_eq!(core.sync_metrics().parked_catalogue_orphans, 1);

    let updates = publish_schema_lineage(
        &mut core,
        SchemaVersion::new(evolved),
        MigrationLens::new(
            base.version_id(),
            evolved_id,
            vec![TableLens {
                source_table: "todos".to_owned(),
                target_table: "todos".to_owned(),
                ops: vec![LensOp::AddColumn {
                    column: "body".to_owned(),
                    default: Value::String(String::new()),
                }],
            }],
        ),
        Vec::<String>::new(),
        Vec::<String>::new(),
    )
    .unwrap();

    assert!(updates.iter().any(|message| matches!(
        message,
        SyncMessage::FateUpdate {
            tx_id,
            fate: Fate::Rejected(RejectionReason::MalformedCommit(reason)),
            ..
        } if *tx_id == tx.tx_id
            && reason.contains("complete descriptor of its authored schema")
    )));
    assert!(core.row_history("todos", row(0x58)).unwrap().is_empty());
}

/// A relay has no fate authority: when schema arrival proves a parked row
/// incomplete, it drops that unit without failing the catalogue publication or
/// inventing a rejected transaction.
#[test]
fn catalogue_arrival_drops_incomplete_relay_row_without_failing_publication() {
    let base = schema();
    let evolved = catalogue_evolved_schema();
    let evolved_id = evolved.version_id();
    let (_writer_dir, mut writer) =
        open_history_complete_node_with_schema(node(0x6a), base.clone());
    let (_relay_dir, mut relay) =
        open_history_complete_node_with_schema(node(0x6b), base.clone());
    let (_tx_id, unit) = writer
        .commit_mergeable_unit(
            MergeableCommit::new("todos", row(0x6a), 1_004).cells(title_cells("invalid relay")),
        )
        .unwrap();
    let SyncMessage::CommitUnit { tx, versions } = unit else {
        panic!("commit unit expected");
    };
    let incomplete = versions
        .into_iter()
        .map(|version| {
            crate::protocol::VersionRecord::from_cells(
                &base.tables[0],
                evolved_id,
                version.row_uuid(),
                version.parents(),
                version.created_by(),
                version.created_at(),
                version.updated_by(),
                version.updated_at(),
                &version_record_cells(&version, &base.tables[0]),
                version.deletion(),
            )
            .unwrap()
        })
        .collect();

    relay.ingest_relay_commit_unit(tx.clone(), incomplete).unwrap();
    assert!(relay.query_transaction(tx.tx_id).unwrap().is_none());

    publish_schema_lineage(
        &mut relay,
        SchemaVersion::new(evolved),
        MigrationLens::new(
            base.version_id(),
            evolved_id,
            vec![TableLens {
                source_table: "todos".to_owned(),
                target_table: "todos".to_owned(),
                ops: vec![LensOp::AddColumn {
                    column: "body".to_owned(),
                    default: Value::String(String::new()),
                }],
            }],
        ),
        Vec::<String>::new(),
        Vec::<String>::new(),
    )
    .unwrap();

    assert!(relay.query_transaction(tx.tx_id).unwrap().is_none());
    assert!(relay.row_history("todos", row(0x6a)).unwrap().is_empty());
    assert_eq!(relay.sync_metrics().dropped_malformed_relay_commit_units, 1);
}

fn zero_column_version_claiming_schema(
    schema: &JazzSchema,
    version: &crate::protocol::VersionRecord,
) -> crate::protocol::VersionRecord {
    crate::protocol::VersionRecord::from_cells(
        &TableSchema::new("todos", Vec::<ColumnSchema>::new()),
        schema.version_id(),
        version.row_uuid(),
        version.parents(),
        version.created_by(),
        version.created_at(),
        version.updated_by(),
        version.updated_at(),
        &BTreeMap::<String, Value>::new(),
        version.deletion(),
    )
    .unwrap()
}

/// A non-reset ViewUpdate stages its history payload in a shared receiver
/// batch. A malformed row descriptor must reject the frame before that batch
/// writes either its transaction or version.
#[test]
fn batched_view_update_rejects_incomplete_authored_row_before_storage() {
    let base = schema();
    let (_core_dir, mut core) = open_node_with_schema(node(0x6c), base.clone());
    accept_global(
        &mut core,
        MergeableCommit::new("todos", row(0x6c), 1_005).cells(title_cells("view payload")),
    );
    let update = PeerState::new()
        .current_rows_update(&mut core, "todos")
        .unwrap();
    let mut bundles = version_bundles_for_update(&update);
    assert_eq!(bundles.len(), 1);
    let version = bundles[0].versions[0].clone();
    bundles[0].versions = vec![zero_column_version_claiming_schema(&base, &version)];
    let SyncMessage::ViewUpdate {
        subscription,
        settled_through,
        reset_result_set,
        peer_payload_inventory,
        result_member_adds,
        result_member_removes,
        terminal_operations,
        program_fact_adds,
        program_fact_removes,
        ..
    } = update
    else {
        panic!("expected view update");
    };
    assert!(!reset_result_set, "exercise shared non-reset receiver batching");

    let (_reader_dir, mut reader) = open_node_with_schema(node(0x6d), base);
    let error = reader
        .apply_view_updates_in_batch(vec![ViewUpdateParts {
            subscription,
            settled_through,
            defer_settlement: false,
            reset_result_set,
            version_carriers: Vec::new(),
            version_bundles: bundles,
            peer_complete_tx_payload_refs: peer_payload_inventory.complete_tx_payloads,
            authorization_progress: None,
            opening_pending: false,
            result_member_adds,
            result_member_removes,
            terminal_operations,
            program_fact_adds,
            program_fact_removes,
        }])
        .expect_err("malformed ViewUpdate must not stage a row");
    match error {
        Error::MalformedViewUpdate(
            "row version does not carry the complete descriptor of its authored schema",
        ) => {}
        other => panic!("expected malformed ViewUpdate, got {other:?}"),
    }
    assert!(reader.query_all_versions().unwrap().is_empty());
}

/// Ordinary ViewUpdate ingestion reaches the shared history boundary even
/// without receiver batching, so an incomplete record cannot be stored there.
#[test]
fn view_update_rejects_incomplete_authored_row_before_storage() {
    let base = schema();
    let (_core_dir, mut core) = open_node_with_schema(node(0x6e), base.clone());
    accept_global(
        &mut core,
        MergeableCommit::new("todos", row(0x6e), 1_006).cells(title_cells("ordinary view")),
    );
    let update = PeerState::new()
        .current_rows_update(&mut core, "todos")
        .unwrap();
    let mut bundles = version_bundles_for_update(&update);
    let version = bundles[0].versions[0].clone();
    bundles[0].versions = vec![zero_column_version_claiming_schema(&base, &version)];
    let SyncMessage::ViewUpdate {
        subscription,
        settled_through,
        reset_result_set,
        peer_payload_inventory,
        result_member_adds,
        result_member_removes,
        terminal_operations,
        program_fact_adds,
        program_fact_removes,
        ..
    } = update
    else {
        panic!("expected view update");
    };

    let (_reader_dir, mut reader) = open_node_with_schema(node(0x6f), base);
    assert!(matches!(
        reader.apply_sync_message(SyncMessage::ViewUpdate {
            subscription,
            settled_through,
            reset_result_set,
            version_carriers: Vec::new(),
            version_bundles: bundles,
            peer_payload_inventory,
            result_member_adds,
            result_member_removes,
            terminal_operations,
            program_fact_adds,
            program_fact_removes,
        }),
        Err(Error::MalformedViewUpdate(
            "row version does not carry the complete descriptor of its authored schema"
        ))
    ));
    assert!(reader.query_all_versions().unwrap().is_empty());
}

/// Row-version repair payloads reach the same shared history boundary and
/// reject an incomplete claimed row before transaction/history storage.
#[test]
fn row_version_repair_rejects_incomplete_authored_row_before_storage() {
    let base = schema();
    let (_core_dir, mut core) = open_node_with_schema(node(0x70), base.clone());
    accept_global(
        &mut core,
        MergeableCommit::new("todos", row(0x70), 1_007).cells(title_cells("repair payload")),
    );
    let mut bundles = version_bundles_for_update(
        &PeerState::new()
            .current_rows_update(&mut core, "todos")
            .unwrap(),
    );
    let version = bundles[0].versions[0].clone();
    let request = crate::protocol::RowVersionRef::new(
        version.table().to_owned(),
        version.row_uuid(),
        bundles[0].tx.tx_id,
    );
    bundles[0].versions = vec![zero_column_version_claiming_schema(&base, &version)];

    let (_reader_dir, mut reader) = open_node_with_schema(node(0x71), base);
    assert!(matches!(
        reader.apply_row_version_payloads_for_requests(&[request], bundles),
        Err(Error::MalformedViewUpdate(
            "row version does not carry the complete descriptor of its authored schema"
        ))
    ));
    assert!(reader.query_all_versions().unwrap().is_empty());
}

/// A rejected reset frame must not enable the relaxed initial-sync durability
/// cadence or register a hydration before all of its row payloads are valid.
#[test]
fn reset_view_update_rejection_does_not_leave_initial_sync_flush_active() {
    let base = schema();
    let (_core_dir, mut core) = open_node_with_schema(node(0x72), base.clone());
    accept_global(
        &mut core,
        MergeableCommit::new("todos", row(0x72), 1_008).cells(title_cells("reset payload")),
    );
    let update = PeerState::new()
        .current_rows_update(&mut core, "todos")
        .unwrap();
    let mut bundles = version_bundles_for_update(&update);
    let version = bundles[0].versions[0].clone();
    bundles[0].versions = vec![zero_column_version_claiming_schema(&base, &version)];
    let SyncMessage::ViewUpdate {
        subscription,
        settled_through,
        peer_payload_inventory,
        result_member_adds,
        result_member_removes,
        terminal_operations,
        program_fact_adds,
        program_fact_removes,
        ..
    } = update
    else {
        panic!("expected view update");
    };

    let (_reader_dir, mut reader) = open_node_with_schema(node(0x73), base);
    reader.set_initial_sync_flush_cadence(2).unwrap();
    assert!(!reader.initial_sync_flush_active);
    assert!(reader.query.initial_hydration_binding_views.is_empty());
    assert!(matches!(
        reader.apply_view_updates_in_batch(vec![ViewUpdateParts {
            subscription,
            settled_through,
            defer_settlement: false,
            reset_result_set: true,
            version_carriers: Vec::new(),
            version_bundles: bundles,
            peer_complete_tx_payload_refs: peer_payload_inventory.complete_tx_payloads,
            authorization_progress: None,
            opening_pending: false,
            result_member_adds,
            result_member_removes,
            terminal_operations,
            program_fact_adds,
            program_fact_removes,
        }]),
        Err(Error::MalformedViewUpdate(
            "row version does not carry the complete descriptor of its authored schema"
        ))
    ));
    assert!(!reader.initial_sync_flush_active);
    assert!(!reader.initial_sync_flush_completed);
    assert!(reader.query.initial_hydration_binding_views.is_empty());
    assert!(reader.query_all_versions().unwrap().is_empty());
}

/// A receiver frame validates every bundle before the first valid one can
/// mutate its clock, aliases, catalogue mappings, or durable history.
#[test]
fn batched_view_update_rejection_is_atomic_across_valid_and_malformed_bundles() {
    let base = schema();
    let (_core_dir, mut core) = open_node_with_schema(node(0x74), base.clone());
    accept_global(
        &mut core,
        MergeableCommit::new("todos", row(0x74), 1_009).cells(title_cells("valid first")),
    );
    accept_global(
        &mut core,
        MergeableCommit::new("todos", row(0x75), 1_010).cells(title_cells("malformed second")),
    );
    let update = PeerState::new()
        .current_rows_update(&mut core, "todos")
        .unwrap();
    let mut bundles = version_bundles_for_update(&update);
    assert_eq!(bundles.len(), 2, "one complete bundle per accepted write");
    let malformed = bundles[1].versions[0].clone();
    bundles[1].versions = vec![zero_column_version_claiming_schema(&base, &malformed)];
    let valid_tx_id = bundles[0].tx.tx_id;
    let SyncMessage::ViewUpdate {
        subscription,
        settled_through,
        peer_payload_inventory,
        result_member_adds,
        result_member_removes,
        terminal_operations,
        program_fact_adds,
        program_fact_removes,
        ..
    } = update
    else {
        panic!("expected view update");
    };

    let (_reader_dir, mut reader) = open_node_with_schema(node(0x76), base);
    let clock_before = (
        reader.clock.tx_time,
        reader.clock.next_global_seq,
        reader.clock.applied_global_watermark,
        reader.clock.applied_global_above_watermark.clone(),
    );
    let node_aliases_before = reader.node_aliases.clone();
    let schema_aliases_before = reader.catalogue.schema_version_aliases.clone();
    let catalogue_schemas_before = reader.catalogue.catalogue_schemas.clone();
    let history_before = reader.query_all_versions().unwrap();
    assert!(matches!(
        reader.apply_view_updates_in_batch(vec![ViewUpdateParts {
            subscription,
            settled_through,
            defer_settlement: false,
            reset_result_set: false,
            version_carriers: Vec::new(),
            version_bundles: bundles,
            peer_complete_tx_payload_refs: peer_payload_inventory.complete_tx_payloads,
            authorization_progress: None,
            opening_pending: false,
            result_member_adds,
            result_member_removes,
            terminal_operations,
            program_fact_adds,
            program_fact_removes,
        }]),
        Err(Error::MalformedViewUpdate(
            "row version does not carry the complete descriptor of its authored schema"
        ))
    ));
    assert_eq!(
        (
            reader.clock.tx_time,
            reader.clock.next_global_seq,
            reader.clock.applied_global_watermark,
            reader.clock.applied_global_above_watermark.clone(),
        ),
        clock_before
    );
    assert_eq!(reader.node_aliases, node_aliases_before);
    assert_eq!(reader.catalogue.schema_version_aliases, schema_aliases_before);
    assert_eq!(reader.catalogue.catalogue_schemas, catalogue_schemas_before);
    assert_eq!(reader.query_all_versions().unwrap(), history_before);
    assert!(reader.query_transaction(valid_tx_id).unwrap().is_none());
    assert!(!reader.initial_sync_flush_active);
    assert!(reader.query.initial_hydration_binding_views.is_empty());
}

#[test]
fn current_winner_projects_rename_copy_chains_across_durability_tiers() {
    let base = JazzSchema::new([TableSchema::new(
        "todos",
        [ColumnSchema::new("title", ColumnType::String)],
    )]);
    let evolved = SchemaVersion::new(JazzSchema::new([TableSchema::new(
        "todos",
        [
            ColumnSchema::new("name", ColumnType::String),
            ColumnSchema::new("body", ColumnType::String),
        ],
    )]));
    let (_dir, mut core) = open_node_with_schema(node(0x5c), base.clone());
    let old_row = row(0x5c);
    accept_global(
        &mut core,
        MergeableCommit::new("todos", old_row, 1).cells(title_cells("old title")),
    );
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
                    LensOp::CopyColumn {
                        from: "name".to_owned(),
                        to: "body".to_owned(),
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
            schema: evolved.id,
        },
    })
    .unwrap();
    let new_row = row(0x5d);
    accept_global(
        &mut core,
        MergeableCommit::new("todos", new_row, 2).cells(BTreeMap::from([
            ("name".to_owned(), v("new name")),
            ("body".to_owned(), v("new body")),
        ])),
    );

    let shape = Query::from("todos").validate(&evolved.schema).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let expected = BTreeMap::from([
        (
            old_row,
            BTreeMap::from([
                ("name".to_owned(), v("old title")),
                ("body".to_owned(), v("old title")),
            ]),
        ),
        (
            new_row,
            BTreeMap::from([
                ("name".to_owned(), v("new name")),
                ("body".to_owned(), v("new body")),
            ]),
        ),
    ]);
    for tier in [
        DurabilityTier::Local,
        DurabilityTier::Edge,
        DurabilityTier::Global,
    ] {
        assert_eq!(
            core.query_rows(&shape, &binding, tier)
                .unwrap()
                .into_iter()
                .map(current_row_pair)
                .collect::<BTreeMap<_, _>>(),
            expected,
            "Rename/Copy paths preserve old physical fields in {tier:?} reads",
        );
    }
}

fn assert_current_winner_copied_enum_remap(
    marker: u8,
    source_type: ColumnType,
    copied_type: ColumnType,
    latest_type: ColumnType,
    old_value: Value,
    unknown_value: Value,
) {
    let base = JazzSchema::new([TableSchema::new(
        "items",
        [ColumnSchema::new("source", source_type)],
    )]);
    let copied = SchemaVersion::new(JazzSchema::new([TableSchema::new(
        "items",
        [
            ColumnSchema::new("status", copied_type.clone()),
            ColumnSchema::new("status_copy", copied_type),
        ],
    )]));
    let latest = SchemaVersion::new(JazzSchema::new([TableSchema::new(
        "items",
        [
            ColumnSchema::new("status", latest_type),
            ColumnSchema::new("status_copy", copied.schema.tables[0].columns[1].column_type.clone()),
        ],
    )]));
    let (_dir, mut core) = open_node_with_schema(node(marker), base.clone());
    let old_row = row(marker);
    accept_global(
        &mut core,
        MergeableCommit::new("items", old_row, 1)
            .cells(BTreeMap::from([("source".to_owned(), old_value.clone())])),
    );
    publish_schema_lineage(
        &mut core,
        copied.clone(),
        MigrationLens::new(
            base.version_id(),
            copied.id,
            vec![TableLens {
                source_table: "items".to_owned(),
                target_table: "items".to_owned(),
                ops: vec![
                    LensOp::RenameColumn {
                        from: "source".to_owned(),
                        to: "status".to_owned(),
                    },
                    LensOp::CopyColumn {
                        from: "status".to_owned(),
                        to: "status_copy".to_owned(),
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
            schema: copied.id,
        },
    })
    .unwrap();
    let shape = Query::from("items").validate(&copied.schema).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let expected = BTreeMap::from([(
        old_row,
        BTreeMap::from([
            ("status".to_owned(), old_value.clone()),
            ("status_copy".to_owned(), old_value.clone()),
        ]),
    )]);
    for tier in [
        DurabilityTier::Local,
        DurabilityTier::Edge,
        DurabilityTier::Global,
    ] {
        assert_eq!(
            core.query_rows(&shape, &binding, tier)
                .unwrap()
                .into_iter()
                .map(current_row_pair)
                .collect::<BTreeMap<_, _>>(),
            expected,
            "copied enum row remaps through distinct registries in {tier:?}",
        );
    }
    publish_schema_lineage(
        &mut core,
        latest.clone(),
        MigrationLens::new(
            copied.id,
            latest.id,
            vec![TableLens {
                source_table: "items".to_owned(),
                target_table: "items".to_owned(),
                ops: vec![LensOp::TransformColumn {
                    column: "status".to_owned(),
                    transform: "jazz.identity".to_owned(),
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
            revision: 2,
            schema: latest.id,
        },
    })
    .unwrap();
    accept_global(
        &mut core,
        MergeableCommit::new("items", old_row, 2).cells(BTreeMap::from([
            ("status".to_owned(), unknown_value),
            ("status_copy".to_owned(), old_value),
        ])),
    );
    for tier in [
        DurabilityTier::Local,
        DurabilityTier::Edge,
        DurabilityTier::Global,
    ] {
        assert_eq!(
            core.query_rows(&shape, &binding, tier)
                .unwrap()
                .into_iter()
                .map(current_row_pair)
                .collect::<BTreeMap<_, _>>(),
            BTreeMap::new(),
            "the later unknown winner omits the same row after arg-max in {tier:?}",
        );
    }
}

#[test]
fn current_winner_remaps_copied_scalar_enums_and_omits_later_winners() {
    let base = enum_projection_schema(&["open", "selected"]);
    let latest = enum_projection_schema(&["open", "selected", "closed"]);
    assert_current_winner_copied_enum_remap(
        0x5e,
        base.tables[0].columns[1].column_type.clone(),
        base.tables[0].columns[1].column_type.clone(),
        latest.tables[0].columns[1].column_type.clone(),
        Value::EnumTag(1),
        Value::EnumTag(2),
    );
}

#[test]
fn current_winner_remaps_copied_payload_enums_and_omits_later_winners() {
    let base = payload_enum_projection_schema(&["open"]);
    let latest = payload_enum_projection_schema(&["open", "closed"]);
    let payload = groove::records::RecordDescriptor::new([("note", ColumnType::String)]);
    assert_current_winner_copied_enum_remap(
        0x61,
        base.tables[0].columns[1].column_type.clone(),
        base.tables[0].columns[1].column_type.clone(),
        latest.tables[0].columns[1].column_type.clone(),
        Value::Enum(groove::records::EnumValue::create(0, payload.clone(), &[v("open")]).unwrap()),
        Value::Enum(groove::records::EnumValue::create(1, payload, &[v("closed")]).unwrap()),
    );
}

#[test]
fn current_winner_remaps_copied_nested_scalar_enums_and_omits_later_winners() {
    let base = nested_scalar_enum_projection_schema(&["open"]);
    let latest = nested_scalar_enum_projection_schema(&["open", "closed"]);
    assert_current_winner_copied_enum_remap(
        0x63,
        base.tables[0].columns[1].column_type.clone(),
        base.tables[0].columns[1].column_type.clone(),
        latest.tables[0].columns[1].column_type.clone(),
        Value::Array(vec![Value::EnumTag(0)]),
        Value::Array(vec![Value::EnumTag(1)]),
    );
}

#[test]
fn current_winner_remaps_copied_nested_payload_enums_and_omits_later_winners() {
    let base = nested_payload_enum_projection_schema(&["open"]);
    let latest = nested_payload_enum_projection_schema(&["open", "closed"]);
    let payload = groove::records::RecordDescriptor::new([("note", ColumnType::String)]);
    assert_current_winner_copied_enum_remap(
        0x65,
        base.tables[0].columns[1].column_type.clone(),
        base.tables[0].columns[1].column_type.clone(),
        latest.tables[0].columns[1].column_type.clone(),
        Value::Array(vec![Value::Enum(
            groove::records::EnumValue::create(0, payload.clone(), &[v("open")]).unwrap(),
        )]),
        Value::Array(vec![Value::Enum(
            groove::records::EnumValue::create(1, payload, &[v("closed")]).unwrap(),
        )]),
    );
}

#[test]
fn catalogue_arrival_drains_branch_relay_into_branch_partition() {
    let base = schema();
    let evolved = catalogue_evolved_schema();
    let evolved_id = evolved.version_id();
    let (_writer_dir, mut writer) =
        open_history_complete_node_with_schema(node(0x38), evolved.clone());
    let (relay_dir, mut relay) = open_history_complete_node_with_schema(node(0x39), base.clone());
    let branch_id = branch(0x66);
    writer.create_root_branch(branch_id).unwrap();
    relay.create_root_branch(branch_id).unwrap();
    let tx_id = writer
        .commit_mergeable_on_branch(
            branch_id,
            MergeableCommit::new("todos", row(0x56), 1_001).cells(BTreeMap::from([
                ("body".to_owned(), Value::String(String::new())),
                ("title".to_owned(), Value::String("relay parked".to_owned())),
            ])),
        )
        .unwrap();
    let SyncMessage::CommitUnit { tx, versions } = writer.commit_unit_for(tx_id).unwrap() else {
        panic!("commit unit expected");
    };

    relay
        .ingest_relay_commit_unit(tx.clone(), versions)
        .unwrap();
    assert!(relay.query_transaction(tx.tx_id).unwrap().is_none());
    assert!(
        !relay
            .branches
            .branch_partitions
            .iter()
            .any(|(_, existing)| *existing == branch_id)
    );

    publish_schema_lineage(
        &mut relay,
        SchemaVersion::new(evolved.clone()),
        MigrationLens::new(
            base.version_id(),
            evolved_id,
            vec![TableLens {
                source_table: "todos".to_owned(),
                target_table: "todos".to_owned(),
                ops: vec![LensOp::AddColumn {
                    column: "body".to_owned(),
                    default: Value::String(String::new()),
                }],
            }],
        ),
        Vec::<String>::new(),
        Vec::<String>::new(),
    )
    .unwrap();
    let stored = relay.transaction_record(tx.tx_id).unwrap();
    assert_eq!(
        stored.target_lineage,
        crate::tx::BranchLineage::Branch(branch_id)
    );
    assert_eq!(stored.fate, Fate::Pending);
    // The relayed row is authored in the newly arrived schema and becomes
    // readable through the physical lineage activated with its lens.
    let shape = Query::from("todos").validate(&evolved).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let rows = relay
        .query_rows_on_branch(branch_id, &shape, &binding)
        .unwrap()
        .into_iter()
        .map(current_row_pair)
        .collect::<BTreeMap<_, _>>();
    let evolved_cells = BTreeMap::from([
        ("body".to_owned(), Value::String(String::new())),
        ("title".to_owned(), Value::String("relay parked".to_owned())),
    ]);
    assert_eq!(rows.get(&row(0x56)), Some(&evolved_cells));
    assert!(
        relay
            .current_rows("todos", DurabilityTier::Local)
            .unwrap()
            .into_iter()
            .all(|current| current.row_uuid() != row(0x56))
    );

    drop(relay);
    let mut reopened = reopen_node_at(&relay_dir, node(0x39), base);
    assert_eq!(
        reopened
            .transaction_record(tx.tx_id)
            .unwrap()
            .target_lineage,
        crate::tx::BranchLineage::Branch(branch_id)
    );
    let rows = reopened
        .query_rows_on_branch(branch_id, &shape, &binding)
        .unwrap()
        .into_iter()
        .map(current_row_pair)
        .collect::<BTreeMap<_, _>>();
    assert_eq!(rows.get(&row(0x56)), Some(&evolved_cells));
}

#[test]
fn parked_branch_ingress_role_keeps_authority_precedence_in_both_orders() {
    let base = schema();
    let evolved = catalogue_evolved_schema();
    let evolved_id = evolved.version_id();
    let (_writer_dir, mut writer) =
        open_history_complete_node_with_schema(node(0x3a), evolved.clone());
    let branch_id = branch(0x67);
    writer.create_root_branch(branch_id).unwrap();
    let tx_id = writer
        .commit_mergeable_on_branch(
            branch_id,
            MergeableCommit::new("todos", row(0x57), 1_002).cells(BTreeMap::from([
                ("body".to_owned(), Value::String(String::new())),
                ("title".to_owned(), Value::String("one unit".to_owned())),
            ])),
        )
        .unwrap();
    let SyncMessage::CommitUnit { tx, versions } = writer.commit_unit_for(tx_id).unwrap() else {
        panic!("commit unit expected");
    };
    for (idx, relay_first) in [false, true].into_iter().enumerate() {
        let node_id = node(0x3b + idx as u8);
        let (_dir, mut receiver) = open_history_complete_node_with_schema(node_id, base.clone());
        receiver.create_root_branch(branch_id).unwrap();
        let authority = || SyncMessage::CommitUnit {
            tx: tx.clone(),
            versions: versions.clone(),
        };
        if relay_first {
            receiver
                .ingest_relay_commit_unit(tx.clone(), versions.clone())
                .unwrap();
            assert!(receiver.apply_sync_message(authority()).unwrap().is_empty());
        } else {
            assert!(receiver.apply_sync_message(authority()).unwrap().is_empty());
            receiver
                .ingest_relay_commit_unit(tx.clone(), versions.clone())
                .unwrap();
        }

        let updates = publish_schema_lineage(
            &mut receiver,
            SchemaVersion::new(evolved.clone()),
            MigrationLens::new(
                base.version_id(),
                evolved_id,
                vec![TableLens {
                    source_table: "todos".to_owned(),
                    target_table: "todos".to_owned(),
                    ops: vec![LensOp::AddColumn {
                        column: "body".to_owned(),
                        default: Value::String(String::new()),
                    }],
                }],
            ),
            Vec::<String>::new(),
            Vec::<String>::new(),
        )
        .unwrap();
        assert!(updates.iter().any(|update| matches!(
            update,
            SyncMessage::FateUpdate {
                tx_id: candidate,
                fate: Fate::Accepted,
                ..
            } if *candidate == tx.tx_id
        )));
        let stored = receiver.transaction_record(tx.tx_id).unwrap();
        assert_eq!(stored.fate, Fate::Accepted);
        assert_eq!(
            stored.target_lineage,
            crate::tx::BranchLineage::Branch(branch_id)
        );
        assert_eq!(receiver.query_versions_for_tx(tx.tx_id).unwrap().len(), 1);
        assert!(
            receiver
                .current_rows("todos", DurabilityTier::Global)
                .unwrap()
                .into_iter()
                .all(|current| current.row_uuid() != row(0x57))
        );
    }
}

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
        ),
        Vec::<String>::new(),
        Vec::<String>::new(),
    )
    .unwrap();
    assert_eq!(core.current_write_schema().schema, base.version_id());

    let row = row(0x5c);
    let (_tx_id, unit) = writer
        .commit_mergeable_unit(
            MergeableCommit::new("todos", row, 10).cells(BTreeMap::from([
                ("title".to_owned(), v("newer-client")),
                ("body".to_owned(), v("authored-v2")),
            ])),
        )
        .unwrap();
    core.apply_sync_message(unit).unwrap();

    assert_eq!(core.current_write_schema().schema, base.version_id());
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
        ),
        Vec::<String>::new(),
        Vec::<String>::new(),
    )
    .unwrap();
    core.apply_trusted_catalogue_message(SyncMessage::SetCurrentWriteSchema {
        author: AuthorId::SYSTEM,
        pointer: CurrentWriteSchema {
            revision: 2,
            schema: evolved_payload.id,
        },
    })
    .unwrap();
    assert_eq!(core.current_write_schema().revision, 2);
    assert_eq!(core.current_write_schema().schema, evolved_payload.id);

    let stale = core.apply_trusted_catalogue_message(SyncMessage::SetCurrentWriteSchema {
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

    core.apply_trusted_catalogue_message(SyncMessage::SetCurrentWriteSchema {
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
    );
    publish_schema_lineage(
        &mut core,
        evolved_payload.clone(),
        lens.clone(),
        Vec::<String>::new(),
        Vec::<String>::new(),
    )
    .unwrap();
    core.apply_trusted_catalogue_message(SyncMessage::SetCurrentWriteSchema {
        author: AuthorId::SYSTEM,
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
        reopened.current_write_schema(),
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
    let evolved = JazzSchema::new([
        TableSchema::new("todos", [ColumnSchema::new("title", ColumnType::String)]),
        TableSchema::new("notes", [ColumnSchema::new("body", ColumnType::String)]),
    ]);
    let shape = Query::from("todos").validate(&evolved).unwrap();
    let (dir, mut core) = open_node_with_schema(node(0x3c), base.clone());

    core.apply_sync_message(SyncMessage::RegisterShape {
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
        ),
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
        ),
        Vec::<String>::new(),
        Vec::<String>::new(),
    )
    .unwrap();
    let table_id = core.catalogue.physical_mappings[&evolved_payload.id].tables["todos"].table_id;
    let history = physical_history_table_name(table_id);
    let register = physical_register_table_name(table_id);
    assert!(core.database.primary_key_scan_raw(&history, &[]).is_ok());
    assert!(core.database.primary_key_scan_raw(&register, &[]).is_ok());

    core.apply_trusted_catalogue_message(SyncMessage::SetCurrentWriteSchema {
        author: AuthorId::SYSTEM,
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
    let evolved = JazzSchema::new([
        TableSchema::new("todos", [ColumnSchema::new("title", ColumnType::String)]),
        TableSchema::new("notes", [ColumnSchema::new("body", ColumnType::String)]),
    ]);
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
        ),
        ["notes"],
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
    assert!(core.table_in_schema("notes", evolved_payload.id).is_ok());

    let note = row(0x3e);
    assert!(
        core.advisory_mergeable_write_allows(MergeableCommit::new("notes", note, 10).cells(
            BTreeMap::from([("body".to_owned(), v("live add-table write"),)])
        ))
        .unwrap()
    );
    let tx_id = core
        .commit_mergeable(
            MergeableCommit::new("notes", note, 10).cells(BTreeMap::from([(
                "body".to_owned(),
                v("live add-table write"),
            )])),
        )
        .unwrap();
    assert_eq!(
        core.current_rows_for_schema("notes", evolved_payload.id, DurabilityTier::Local)
            .unwrap()
            .len(),
        1
    );
    core.apply_fate_update(
        tx_id,
        Fate::Accepted,
        Some(core.clock.next_global_seq),
        Some(DurabilityTier::Global),
    )
    .unwrap();

    let shape = Query::from("notes").validate(&evolved).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
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
        peer.identity(),
    )
    .unwrap();
    let update = peer.current_rows_update(&mut core, "notes").unwrap();
    let version_bundles = version_bundles_for_update(&update);
    let SyncMessage::ViewUpdate {
        result_member_adds, ..
    } = update
    else {
        panic!("current-row subscription should produce a view update");
    };
    assert_eq!(result_member_adds.len(), 1);

    assert_eq!(version_bundles.len(), 1);
}

#[test]
fn transaction_version_scans_recover_table_names_from_physical_mappings() {
    let base = schema();
    let evolved = JazzSchema::new([
        TableSchema::new("todos", [ColumnSchema::new("title", ColumnType::String)]),
        TableSchema::new("notes", [ColumnSchema::new("body", ColumnType::String)]),
    ]);
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
        ),
        ["notes"],
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
    let tx_id = core
        .commit_mergeable(
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

#[test]
fn shared_physical_reads_project_natural_lenses_after_schema_agnostic_winner() {
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

    core.commit_mergeable(
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
            AuthorId::SYSTEM,
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
    let v2 = JazzSchema::new([TableSchema::new(
        "todos",
        [
            ColumnSchema::new("name", ColumnType::String),
            ColumnSchema::new("body", ColumnType::String),
        ],
    )]);
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

    core.apply_trusted_catalogue_message(SyncMessage::PublishLens {
        author: AuthorId::SYSTEM,
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
                        default: Value::Uuid(other.0),
                    },
                    LensOp::RenameColumn {
                        from: "owner".to_owned(),
                        to: "owner_id".to_owned(),
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

    let readable_row = row(0x48);
    let (_accepted_tx, accepted_unit) = writer
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
        ),
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
        ),
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
        .commit_mergeable(MergeableCommit::new("todos", row(0x46), 10).cells(title_cells("base")))
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
    let evolved_tx = core
        .commit_mergeable(
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
        .commit_mergeable(MergeableCommit::new("todos", row(0x4a), 10).cells(title_cells("base")))
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

    let tx = OpenBatchId::new();
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
    let (exclusive_tx, _unit) = core.commit_exclusive(tx, AuthorId::SYSTEM, 11).unwrap();

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
    core.commit_mergeable(
        MergeableCommit::new("todos", row(0x48), 10).cells(BTreeMap::from([
            ("title".to_owned(), v("historical")),
            ("body".to_owned(), v("kept")),
        ])),
    )
    .unwrap();
    core.apply_trusted_catalogue_message(SyncMessage::SetCurrentWriteSchema {
        author: AuthorId::SYSTEM,
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

fn independent_enum_schema(a: &[&str], b: &[&str]) -> JazzSchema {
    JazzSchema::new([TableSchema::new(
        "items",
        [
            ColumnSchema::new(
                "a",
                ColumnType::EnumTag(groove::records::ScalarEnumSchema::new("a", a.iter().copied()).unwrap()),
            ),
            ColumnSchema::new(
                "b",
                ColumnType::EnumTag(groove::records::ScalarEnumSchema::new("b", b.iter().copied()).unwrap()),
            ),
        ],
    )])
}

fn enum_projection_schema(statuses: &[&str]) -> JazzSchema {
    JazzSchema::new([TableSchema::new(
        "items",
        [
            ColumnSchema::new("title", ColumnType::String),
            ColumnSchema::new(
                "status",
                ColumnType::EnumTag(
                    groove::records::ScalarEnumSchema::new("status", statuses.iter().copied())
                        .unwrap(),
                ),
            ),
        ],
    )])
}

fn enum_identity_lens(source: SchemaVersionId, target: SchemaVersionId) -> MigrationLens {
    MigrationLens::new(
        source,
        target,
        vec![TableLens {
            source_table: "items".to_owned(),
            target_table: "items".to_owned(),
            ops: vec![LensOp::TransformColumn {
                column: "status".to_owned(),
                transform: "jazz.identity".to_owned(),
            }],
        }],
    )
}

/// Earlier catalogue introductions retain their physical scalar-enum tags
/// when a later sibling introduces a shallower authored ordinal.
///
/// alice publishes `base -> A -> A2`; bob later publishes `base -> B`.
///
/// ```text
/// base ──► A (+ a) ──► A2 (+ a2)
///   └────────────────► B (+ b)
/// ```
///
/// The node must append `b` after `a2`, activate the registry, and recover
/// the same mapping after reopen. (Sorting observes the local read schema;
/// the shared physical ordering is asserted below.)
#[test]
fn scalar_enum_later_sibling_appends_without_retagging_deeper_cases() {
    let base = enum_projection_schema(&["base"]);
    let a = SchemaVersion::new(enum_projection_schema(&["base", "a"]));
    let a2 = SchemaVersion::new(enum_projection_schema(&["base", "a", "a2"]));
    let b = SchemaVersion::new(enum_projection_schema(&["base", "b"]));
    let (dir, mut core) = open_node_with_schema(node(0x78), base.clone());

    publish_schema_lineage(
        &mut core,
        a.clone(),
        enum_identity_lens(base.version_id(), a.id),
        Vec::<String>::new(),
        Vec::<String>::new(),
    )
    .unwrap();
    publish_schema_lineage(
        &mut core,
        a2.clone(),
        enum_identity_lens(a.id, a2.id),
        Vec::<String>::new(),
        Vec::<String>::new(),
    )
    .unwrap();
    core.apply_trusted_catalogue_message(SyncMessage::SetCurrentWriteSchema {
        author: AuthorId::SYSTEM,
        pointer: CurrentWriteSchema {
            revision: 1,
            schema: a2.id,
        },
    })
    .unwrap();

    let base_row = row(0x78);
    let a_row = row(0x79);
    let a2_row = row(0x7a);
    for (row_uuid, title, status, tx_time) in [
        (a2_row, "a2", 2, 1),
        (base_row, "base", 0, 2),
        (a_row, "a", 1, 3),
    ] {
        core.commit_mergeable(
            MergeableCommit::new("items", row_uuid, tx_time).cells(BTreeMap::from([
                ("title".to_owned(), v(title)),
                ("status".to_owned(), Value::EnumTag(status)),
            ])),
        )
        .unwrap();
    }
    // `b` has local ordinal 1, but it is catalogue-later than A2's ordinal
    // 2. Ordinal-first physical ordering would attempt to insert it before
    // A2, which Groove correctly rejects as a retagging evolution.
    publish_schema_lineage(
        &mut core,
        b.clone(),
        enum_identity_lens(base.version_id(), b.id),
        Vec::<String>::new(),
        Vec::<String>::new(),
    )
    .unwrap();
    let b_mapping = &core.catalogue.physical_mappings[&b.id].tables["items"];
    let physical_cases = core
        .physical_scalar_enum_cases(b_mapping.table_id, b_mapping.columns["status"])
        .unwrap();
    assert_eq!(
        physical_cases,
        vec![
            GlobalScalarEnumCaseId {
                introducing_schema: base.version_id(),
                introducing_ordinal: 0,
            },
            GlobalScalarEnumCaseId {
                introducing_schema: a.id,
                introducing_ordinal: 1,
            },
            GlobalScalarEnumCaseId {
                introducing_schema: a2.id,
                introducing_ordinal: 2,
            },
            GlobalScalarEnumCaseId {
                introducing_schema: b.id,
                introducing_ordinal: 1,
            },
        ],
    );
    core.apply_trusted_catalogue_message(SyncMessage::SetCurrentWriteSchema {
        author: AuthorId::SYSTEM,
        pointer: CurrentWriteSchema {
            revision: 2,
            schema: b.id,
        },
    })
    .unwrap();
    let b_row = row(0x7b);
    core.commit_mergeable(
        MergeableCommit::new("items", b_row, 4).cells(BTreeMap::from([
            ("title".to_owned(), v("b")),
            ("status".to_owned(), Value::EnumTag(1)),
        ])),
    )
    .unwrap();
    drop(core);

    let mut reopened = reopen_node_at(&dir, node(0x78), base.clone());
    let titles = Query::from("items").select(["title"]).validate(&base).unwrap();
    assert_eq!(
        reopened
            .query_rows(
                &titles,
                &titles.bind(BTreeMap::new()).unwrap(),
                DurabilityTier::Local,
            )
            .unwrap()
            .into_iter()
            .map(|row| row.row_uuid())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([base_row, a_row, a2_row, b_row]),
    );
}

fn payload_enum_projection_schema(cases: &[&str]) -> JazzSchema {
    JazzSchema::new([TableSchema::new(
        "items",
        [
            ColumnSchema::new("title", ColumnType::String),
            ColumnSchema::new(
                "status",
                ColumnType::Enum(Box::new(
                    groove::records::EnumSchema::new(
                        "status",
                        cases.iter().map(|case| {
                            groove::records::EnumCase::new(
                                *case,
                                groove::records::RecordDescriptor::new([(
                                    "note",
                                    groove::records::ValueType::String,
                                )]),
                            )
                        }),
                    )
                    .unwrap(),
                )),
            ),
        ],
    )])
}

fn nested_scalar_enum_projection_schema(statuses: &[&str]) -> JazzSchema {
    JazzSchema::new([TableSchema::new(
        "items",
        [
            ColumnSchema::new("title", ColumnType::String),
            ColumnSchema::new(
                "statuses",
                ColumnType::Array(Box::new(ColumnType::EnumTag(
                    groove::records::ScalarEnumSchema::new("status", statuses.iter().copied())
                        .unwrap(),
                ))),
            ),
        ],
    )])
}

fn nested_payload_enum_projection_schema(cases: &[&str]) -> JazzSchema {
    JazzSchema::new([TableSchema::new(
        "items",
        [
            ColumnSchema::new("title", ColumnType::String),
            ColumnSchema::new(
                "statuses",
                ColumnType::Array(Box::new(ColumnType::Enum(Box::new(
                    groove::records::EnumSchema::new(
                        "status",
                        cases.iter().map(|case| {
                            groove::records::EnumCase::new(
                                *case,
                                groove::records::RecordDescriptor::new([(
                                    "note",
                                    groove::records::ValueType::String,
                                )]),
                            )
                        }),
                    )
                    .unwrap(),
                )))),
            ),
        ],
    )])
}

/// This is an internal physical-activation regression: public clients cannot
/// directly observe Groove's live descriptors, but a payload-enum append must
/// activate and survive recovery before a newer client can write its new case.
#[test]
fn direct_payload_enum_append_activates_and_recovers() {
    let base = payload_enum_projection_schema(&["draft"]);
    let evolved_schema = payload_enum_projection_schema(&["draft", "published"]);
    let evolved = SchemaVersion::new(evolved_schema.clone());
    let (dir, mut core) = open_node_with_schema(node(0x76), base.clone());
    publish_schema_lineage(
        &mut core,
        evolved.clone(),
        MigrationLens::new(
            base.version_id(),
            evolved.id,
            vec![TableLens {
                source_table: "items".to_owned(),
                target_table: "items".to_owned(),
                ops: vec![LensOp::TransformColumn {
                    column: "status".to_owned(),
                    transform: "jazz.identity".to_owned(),
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
            schema: evolved.id,
        },
    })
    .unwrap();
    let published_payload = groove::records::RecordDescriptor::new([(
        "note",
        groove::records::ValueType::String,
    )]);
    core.commit_mergeable(
        MergeableCommit::new("items", row(0x76), 1).cells(BTreeMap::from([
            ("title".to_owned(), v("new-case")),
            (
                "status".to_owned(),
                Value::Enum(groove::records::EnumValue::create(
                    1,
                    published_payload,
                    &[v("written after activation")],
                )
                .unwrap()),
            ),
        ])),
    )
    .unwrap();
    let table_id = core.catalogue.physical_mappings[&evolved.id].tables["items"].table_id;
    let physical_history = physical_history_table_name(table_id);
    assert_eq!(core.query_table_versions("items").unwrap().len(), 1);
    drop(core);

    let mut reopened = reopen_node_at(&dir, node(0x76), base);
    assert_eq!(reopened.query_table_versions("items").unwrap().len(), 1);
    let table = reopened.database.table_schema(&physical_history).unwrap();
    assert!(table.value_variant_registries.values().any(|registry| {
        matches!(
            registry,
            groove::records::VariantRegistry::Enum { cases }
                if cases.len() == 2
        )
    }));
}

#[test]
fn payload_enum_unknown_case_is_ignored_only_when_unselected() {
    let schema = |extra| {
        let record = groove::records::RecordDescriptor::new([("x", groove::records::ValueType::String)]);
        let mut cases = vec![groove::records::EnumCase::new("open", record.clone())];
        if extra { cases.push(groove::records::EnumCase::new("closed", record)); }
        JazzSchema::new([TableSchema::new("items", [
            ColumnSchema::new("title", ColumnType::String),
            ColumnSchema::new("status", ColumnType::Enum(Box::new(groove::records::EnumSchema::new("status", cases).unwrap()))),
        ])])
    };
    let base = schema(false); let evolved = SchemaVersion::new(schema(true));
    let (_dir, mut core) = open_node_with_schema(node(0x76), base.clone());
    publish_schema_lineage(&mut core, evolved.clone(), MigrationLens::new(base.version_id(), evolved.id, vec![TableLens { source_table: "items".into(), target_table: "items".into(), ops: vec![LensOp::TransformColumn { column: "status".into(), transform: "jazz.identity".into() }] }]), Vec::<String>::new(), Vec::<String>::new()).unwrap();
    core.apply_trusted_catalogue_message(SyncMessage::SetCurrentWriteSchema { author: AuthorId::SYSTEM, pointer: CurrentWriteSchema { revision: 1, schema: evolved.id } }).unwrap();
    let payload = groove::records::RecordDescriptor::new([("x", groove::records::ValueType::String)]);
    let unknown = row(0x76);
    core.commit_mergeable(MergeableCommit::new("items", unknown, 1).cells(BTreeMap::from([
        ("title".into(), v("ok")), ("status".into(), Value::Enum(groove::records::EnumValue::create(1, payload, &[v("closed")]).unwrap()))
    ]))).unwrap();
    let known = row(0x77);
    let known_payload = groove::records::RecordDescriptor::new([("x", groove::records::ValueType::String)]);
    core.commit_mergeable(MergeableCommit::new("items", known, 2).cells(BTreeMap::from([
        ("title".into(), v("known")), ("status".into(), Value::Enum(groove::records::EnumValue::create(0, known_payload, &[v("open")]).unwrap()))
    ]))).unwrap();
    let title = Query::from("items").select(["title"]).validate(&base).unwrap();
    assert!(core.query_rows(&title, &title.bind(BTreeMap::new()).unwrap(), DurabilityTier::Local).is_ok());
    let all = Query::from("items").validate(&base).unwrap();
    assert_eq!(
        core.query_rows(&all, &all.bind(BTreeMap::new()).unwrap(), DurabilityTier::Local)
            .unwrap()
            .into_iter()
            .map(|row| row.row_uuid())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([known]),
    );
}

/// Old-schema reads omit only rows whose nested enum occurrence has an
/// unrepresentable case; Alice's compatible row remains visible beside Bob's
/// later case.
///
/// ```text
/// alice ──writes open──► old reader ──► visible
/// bob   ──writes closed► old reader ──► omitted
/// ```
#[test]
fn nested_scalar_enum_unknown_case_omits_only_that_row() {
    let base = nested_scalar_enum_projection_schema(&["open"]);
    let evolved = SchemaVersion::new(nested_scalar_enum_projection_schema(&["open", "closed"]));
    let (_dir, mut core) = open_node_with_schema(node(0x78), base.clone());
    publish_schema_lineage(
        &mut core,
        evolved.clone(),
        MigrationLens::new(
            base.version_id(),
            evolved.id,
            vec![TableLens {
                source_table: "items".to_owned(),
                target_table: "items".to_owned(),
                ops: vec![LensOp::TransformColumn {
                    column: "statuses".to_owned(),
                    transform: "jazz.identity".to_owned(),
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
            schema: evolved.id,
        },
    })
    .unwrap();
    let unknown = row(0x78);
    let known = row(0x79);
    for (row_uuid, title, status, tx_time) in [
        (unknown, "new nested case", 1, 1),
        (known, "known nested case", 0, 2),
    ] {
        core.commit_mergeable(
            MergeableCommit::new("items", row_uuid, tx_time).cells(BTreeMap::from([
                ("title".to_owned(), v(title)),
                (
                    "statuses".to_owned(),
                    Value::Array(vec![Value::EnumTag(status)]),
                ),
            ])),
        )
        .unwrap();
    }

    let title_only = Query::from("items").select(["title"]).validate(&base).unwrap();
    assert_eq!(
        core.query_rows(
            &title_only,
            &title_only.bind(BTreeMap::new()).unwrap(),
            DurabilityTier::Local,
        )
        .unwrap()
        .len(),
        2,
    );
    let all = Query::from("items").validate(&base).unwrap();
    assert_eq!(
        core.query_rows(&all, &all.bind(BTreeMap::new()).unwrap(), DurabilityTier::Local)
            .unwrap()
            .into_iter()
            .map(|row| row.row_uuid())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([known]),
    );
}

/// A nested payload enum follows the same source-local compatibility rule as
/// a scalar enum: Bob's later case is omitted while Alice's older case stays
/// visible to an old-schema whole-row read.
#[test]
fn nested_payload_enum_unknown_case_omits_only_that_row() {
    let base = nested_payload_enum_projection_schema(&["open"]);
    let evolved = SchemaVersion::new(nested_payload_enum_projection_schema(&["open", "closed"]));
    let (_dir, mut core) = open_node_with_schema(node(0x7a), base.clone());
    publish_schema_lineage(
        &mut core,
        evolved.clone(),
        MigrationLens::new(
            base.version_id(),
            evolved.id,
            vec![TableLens {
                source_table: "items".to_owned(),
                target_table: "items".to_owned(),
                ops: vec![LensOp::TransformColumn {
                    column: "statuses".to_owned(),
                    transform: "jazz.identity".to_owned(),
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
            schema: evolved.id,
        },
    })
    .unwrap();
    let payload = groove::records::RecordDescriptor::new([(
        "note",
        groove::records::ValueType::String,
    )]);
    let unknown = row(0x7a);
    let known = row(0x7b);
    for (row_uuid, title, status, note, tx_time) in [
        (unknown, "new nested case", 1, "closed", 1),
        (known, "known nested case", 0, "open", 2),
    ] {
        core.commit_mergeable(
            MergeableCommit::new("items", row_uuid, tx_time).cells(BTreeMap::from([
                ("title".to_owned(), v(title)),
                (
                    "statuses".to_owned(),
                    Value::Array(vec![Value::Enum(
                        groove::records::EnumValue::create(status, payload, &[v(note)]).unwrap(),
                    )]),
                ),
            ])),
        )
        .unwrap();
    }

    let title_only = Query::from("items").select(["title"]).validate(&base).unwrap();
    assert_eq!(
        core.query_rows(
            &title_only,
            &title_only.bind(BTreeMap::new()).unwrap(),
            DurabilityTier::Local,
        )
        .unwrap()
        .len(),
        2,
    );
    let all = Query::from("items").validate(&base).unwrap();
    assert_eq!(
        core.query_rows(&all, &all.bind(BTreeMap::new()).unwrap(), DurabilityTier::Local)
            .unwrap()
            .into_iter()
            .map(|row| row.row_uuid())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([known]),
    );
}

#[test]
fn maintained_old_enum_subscriptions_omit_rows_that_require_new_cases() {
    // This is an internal subscription-boundary regression. PeerState is the
    // server-side maintained subscription driver; public clients receive its
    // ViewUpdates, but cannot themselves install a catalogue lineage.
    let base = enum_projection_schema(&["open"]);
    let evolved = SchemaVersion::new(enum_projection_schema(&["open", "closed"]));
    let (_dir, mut core) = open_node_with_schema(node(0x7b), base.clone());
    publish_schema_lineage(
        &mut core,
        evolved.clone(),
        MigrationLens::new(
            base.version_id(),
            evolved.id,
            vec![TableLens {
                source_table: "items".to_owned(),
                target_table: "items".to_owned(),
                ops: vec![LensOp::TransformColumn {
                    column: "status".to_owned(),
                    transform: "jazz.identity".to_owned(),
                }],
            }],
        ),
        Vec::<String>::new(),
        Vec::<String>::new(),
    )
    .unwrap();
    core.apply_trusted_catalogue_message(SyncMessage::SetCurrentWriteSchema {
        author: AuthorId::SYSTEM,
        pointer: CurrentWriteSchema { revision: 1, schema: evolved.id },
    })
    .unwrap();

    let known = row(0x7b);
    accept_global(
        &mut core,
        MergeableCommit::new("items", known, 1).cells(BTreeMap::from([
            ("title".to_owned(), v("known case")),
            ("status".to_owned(), Value::EnumTag(0)),
        ])),
    );

    let title_only = Query::from("items").select(["title"]).validate(&base).unwrap();
    let title_binding = title_only.bind(BTreeMap::new()).unwrap();
    let mut title_peer = PeerState::new();
    let initial = title_peer
        .rehydrate_query(&mut core, &title_only, &title_binding)
        .expect("old-schema title subscription opens over known case");
    let SyncMessage::ViewUpdate { reset_result_set, result_member_adds, .. } = initial else {
        panic!("expected initial maintained view update");
    };
    assert!(reset_result_set);
    assert_eq!(result_member_adds.len(), 1);

    // Recompiling exactly the same target must leave the maintained graph in
    // place: target registration is idempotent, not a hidden reset mechanism.
    let unchanged = title_peer
        .query_update(&mut core, &title_only, &title_binding)
        .expect("identical projection target remains registered");
    let SyncMessage::ViewUpdate {
        reset_result_set,
        result_member_adds,
        result_member_removes,
        terminal_operations,
        ..
    } = unchanged else {
        panic!("expected maintained view update");
    };
    assert!(!reset_result_set, "idempotent target registration must not reset");
    assert!(result_member_adds.is_empty());
    assert!(result_member_removes.is_empty());
    assert!(terminal_operations.is_empty());

    let unknown = row(0x7c);
    let unknown_tx = accept_global(
        &mut core,
        MergeableCommit::new("items", unknown, 2).cells(BTreeMap::from([
            ("title".to_owned(), v("new case is harmless when unselected")),
            ("status".to_owned(), Value::EnumTag(1)),
        ])),
    );
    let update = title_peer
        .query_update(&mut core, &title_only, &title_binding)
        .expect("unused unknown enum must not break maintained title output");
    let SyncMessage::ViewUpdate { reset_result_set, result_member_adds, .. } = update else {
        panic!("expected maintained view update");
    };
    assert!(!reset_result_set);
    assert!(result_member_adds.iter().any(|member| {
        member.as_row().is_some_and(|(_, row_uuid, tx_id)| row_uuid == unknown && tx_id == unknown_tx)
    }));

    // A separate old-schema subscription that semantically consumes status
    // must omit the same physical row. In particular, it may not reinterpret
    // the new tag as `open`, and must never error just because a newer client
    // introduced an additive case.
    let status_required = Query::from("items").validate(&base).unwrap();
    let status_binding = status_required.bind(BTreeMap::new()).unwrap();
    let status_options = crate::protocol::RegisterShapeOptions {
        tier: DurabilityTier::Local,
        read_view: Default::default(),
        ..crate::protocol::RegisterShapeOptions::default()
    };
    let status_subscription = SubscriptionKey {
        shape_id: status_required.shape_id(),
        binding_id: status_binding.binding_id(),
        read_view: status_options.read_view_key(),
    };
    let whole_rows = core.query_rows(&status_required, &status_binding, DurabilityTier::Local)
        .expect("one-shot whole-row read omits only the unknown row");
    assert_eq!(
        whole_rows.into_iter()
            .map(|row| row.row_uuid())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([known]),
    );
    let mut status_peer = PeerState::new();
    let update = status_peer
        .rehydrate_query_with_opts(
            &mut core,
            &status_required,
            &status_binding,
            status_options.clone(),
        )
        .expect("required unknown enum case is a row exclusion");
    let SyncMessage::ViewUpdate { result_member_adds, .. } = update else {
        panic!("expected initial maintained view update");
    };
    assert_eq!(result_member_adds.len(), 1);
    assert!(result_member_adds.iter().all(|member| {
        member.as_row().is_some_and(|(_, row_uuid, _)| row_uuid == known)
    }));

    // Maintained membership follows the same compatibility boundary on every
    // delta: a newer local/Ahead unknown winner retracts the older Global row,
    // then a newer known winner re-adds it without a subscription error.
    core.commit_mergeable(
        MergeableCommit::new("items", known, 3).cells(BTreeMap::from([
            ("title".to_owned(), v("now incompatible")),
            ("status".to_owned(), Value::EnumTag(1)),
        ])),
    )
    .unwrap();
    let update = status_peer
        .query_update_for_subscription_with_opts(
            &mut core,
            status_subscription,
            &status_required,
            &status_binding,
            status_options.clone(),
        )
        .expect("newly incompatible delta removes the row");
    let SyncMessage::ViewUpdate { result_member_removes, .. } = update else {
        panic!("expected maintained view update");
    };
    assert!(result_member_removes.iter().any(|member| {
        member.as_row().is_some_and(|(_, row_uuid, _)| row_uuid == known)
    }));
    core.commit_mergeable(
        MergeableCommit::new("items", known, 4).cells(BTreeMap::from([
            ("title".to_owned(), v("compatible again")),
            ("status".to_owned(), Value::EnumTag(0)),
        ])),
    )
    .unwrap();
    let update = status_peer
        .query_update_for_subscription_with_opts(
            &mut core,
            status_subscription,
            &status_required,
            &status_binding,
            status_options,
        )
        .expect("newly compatible delta re-adds the row");
    let SyncMessage::ViewUpdate { result_member_adds, .. } = update else {
        panic!("expected maintained view update");
    };
    assert!(result_member_adds.iter().any(|member| {
        member.as_row().is_some_and(|(_, row_uuid, _)| row_uuid == known)
    }));
}

#[test]
fn maintained_old_payload_enum_subscription_omits_new_case_without_aliasing() {
    let base = payload_enum_projection_schema(&["draft"]);
    let evolved = SchemaVersion::new(payload_enum_projection_schema(&["draft", "published"]));
    let (_dir, mut core) = open_node_with_schema(node(0x7d), base.clone());
    publish_schema_lineage(
        &mut core,
        evolved.clone(),
        MigrationLens::new(
            base.version_id(),
            evolved.id,
            vec![TableLens {
                source_table: "items".to_owned(),
                target_table: "items".to_owned(),
                ops: vec![LensOp::TransformColumn {
                    column: "status".to_owned(),
                    transform: "jazz.identity".to_owned(),
                }],
            }],
        ),
        Vec::<String>::new(),
        Vec::<String>::new(),
    )
    .unwrap();
    core.apply_trusted_catalogue_message(SyncMessage::SetCurrentWriteSchema {
        author: AuthorId::SYSTEM,
        pointer: CurrentWriteSchema { revision: 1, schema: evolved.id },
    })
    .unwrap();
    let payload = groove::records::RecordDescriptor::new([(
        "note",
        groove::records::ValueType::String,
    )]);
    accept_global(
        &mut core,
        MergeableCommit::new("items", row(0x7d), 1).cells(BTreeMap::from([
            ("title".to_owned(), v("new payload case")),
            (
                "status".to_owned(),
                Value::Enum(
                    groove::records::EnumValue::create(1, payload, &[v("published")]).unwrap(),
                ),
            ),
        ])),
    );

    let title_only = Query::from("items").select(["title"]).validate(&base).unwrap();
    let binding = title_only.bind(BTreeMap::new()).unwrap();
    let mut title_peer = PeerState::new();
    assert!(title_peer.rehydrate_query(&mut core, &title_only, &binding).is_ok());

    let required = Query::from("items").validate(&base).unwrap();
    let binding = required.bind(BTreeMap::new()).unwrap();
    let mut required_peer = PeerState::new();
    let update = required_peer
        .rehydrate_query(&mut core, &required, &binding)
        .expect("unknown payload case is a row exclusion");
    let SyncMessage::ViewUpdate { result_member_adds, .. } = update else {
        panic!("expected initial maintained view update");
    };
    assert!(result_member_adds.is_empty());
}

#[test]
fn old_enum_schema_only_decodes_cases_required_by_the_query() {
    // This is an internal current-source boundary test.  The public query API
    // supplies the requirement closure; the old read schema must not decode a
    // physical case it neither returns nor uses semantically.
    let base = enum_projection_schema(&["open"]);
    let evolved_schema = enum_projection_schema(&["open", "closed"]);
    let evolved = SchemaVersion::new(evolved_schema.clone());
    let (_dir, mut core) = open_node_with_schema(node(0x75), base.clone());
    // Requirement-none auxiliary sources still need a physical row shape for
    // relation closure, but their unrequested enum cell must be typed-null.
    assert!(core
        .ensure_physical_current_projection_for_enum_columns(
            base.version_id(),
            "items",
            &BTreeSet::new(),
        )
        .is_ok());
    let enum_lens = MigrationLens::new(
        base.version_id(),
        evolved.id,
        vec![TableLens {
            source_table: "items".to_owned(),
            target_table: "items".to_owned(),
            ops: vec![LensOp::TransformColumn {
                column: "status".to_owned(),
                transform: "jazz.identity".to_owned(),
            }],
        }],
    );
    publish_schema_lineage(
        &mut core,
        evolved.clone(),
        enum_lens,
        Vec::<String>::new(),
        Vec::<String>::new(),
    )
    .unwrap();
    core.apply_trusted_catalogue_message(SyncMessage::SetCurrentWriteSchema {
        author: AuthorId::SYSTEM,
        pointer: CurrentWriteSchema {
            revision: 1,
            schema: evolved.id,
        },
    })
    .unwrap();
    let closed = row(0x75);
    core.commit_mergeable(
        MergeableCommit::new("items", closed, 1).cells(BTreeMap::from([
            ("title".to_owned(), v("still-readable")),
            ("status".to_owned(), Value::EnumTag(1)),
        ])),
    )
    .unwrap();
    let open = row(0x76);
    core.commit_mergeable(
        MergeableCommit::new("items", open, 1).cells(BTreeMap::from([
            ("title".to_owned(), v("still-compatible")),
            ("status".to_owned(), Value::EnumTag(0)),
        ])),
    )
    .unwrap();

    let title_only = Query::from("items").select(["title"]).validate(&base).unwrap();
    assert_eq!(
        core.query_rows(
            &title_only,
            &title_only.bind(BTreeMap::new()).unwrap(),
            DurabilityTier::Local,
        )
        .unwrap()
        .into_iter()
        .map(current_row_pair)
        .collect::<BTreeMap<_, _>>(),
        BTreeMap::from([
            (closed, BTreeMap::from([("title".to_owned(), v("still-readable"))])),
            (open, BTreeMap::from([("title".to_owned(), v("still-compatible"))])),
        ])
    );

    // Whole-row output semantically consumes `status`, so its source boundary
    // omits this incompatible row rather than inventing an old value or
    // turning an additive schema change into an old-client query failure.
    let whole_row = Query::from("items").validate(&base).unwrap();
    assert_eq!(core
        .query_rows(
            &whole_row,
            &whole_row.bind(BTreeMap::new()).unwrap(),
            DurabilityTier::Local,
        )
        .unwrap()
        .into_iter()
        .map(|row| row.row_uuid())
        .collect::<BTreeSet<_>>(),
        BTreeSet::from([open]));

    // The closure is semantic rather than merely output-shaped: a hidden
    // predicate or order key must force the same source-local exclusion before
    // those operators, including their pagination/aggregation consumers.
    for query in [
        Query::from("items")
            .select(["title"])
            .filter(eq(col("status"), lit(Value::EnumTag(0))))
            .validate(&base)
            .unwrap(),
        Query::from("items")
            .select(["title"])
            .order_by("status", crate::query::OrderDirection::Asc)
            .validate(&base)
            .unwrap(),
    ] {
        assert_eq!(core
            .query_rows(
                &query,
                &query.bind(BTreeMap::new()).unwrap(),
                DurabilityTier::Local,
            )
            .unwrap()
            .len(),
            1,
            "known row survives a semantic enum boundary",
        );
    }
    let grouped = Query::from("items").count().group_by("status").validate(&base).unwrap();
    assert_eq!(core
        .query_rows(&grouped, &grouped.bind(BTreeMap::new()).unwrap(), DurabilityTier::Local)
        .unwrap()
        .len(),
        1,
    );
}

#[test]
fn enum_projection_requirement_closure_includes_hidden_policy_fields() {
    // A policy field is not part of the public output, but it still decides
    // whether the row exists. It therefore excludes an incompatible row
    // fail-closed rather than being treated like an unused cell.
    let base = JazzSchema::new([TableSchema::new(
        "items",
        [
            ColumnSchema::new("title", ColumnType::String),
            ColumnSchema::new(
                "status",
                ColumnType::EnumTag(
                    groove::records::ScalarEnumSchema::new("status", ["open"]).unwrap(),
                ),
            ),
        ],
    )
    .with_read_policy(Policy::shape(
        Query::from("items").filter(eq(col("status"), lit(Value::EnumTag(0)))),
    ))]);
    let evolved_schema = JazzSchema::new([TableSchema::new(
        "items",
        [
            ColumnSchema::new("title", ColumnType::String),
            ColumnSchema::new(
                "status",
                ColumnType::EnumTag(
                    groove::records::ScalarEnumSchema::new("status", ["open", "closed"])
                        .unwrap(),
                ),
            ),
        ],
    )
    .with_read_policy(Policy::shape(
        Query::from("items").filter(eq(col("status"), lit(Value::EnumTag(0)))),
    ))]);
    let evolved = SchemaVersion::new(evolved_schema);
    let (_dir, mut core) = open_node_with_schema(node(0x77), base.clone());
    publish_schema_lineage(
        &mut core,
        evolved.clone(),
        MigrationLens::new(
            base.version_id(),
            evolved.id,
            vec![TableLens {
                source_table: "items".to_owned(),
                target_table: "items".to_owned(),
                ops: vec![LensOp::TransformColumn {
                    column: "status".to_owned(),
                    transform: "jazz.identity".to_owned(),
                }],
            }],
        ),
        Vec::<String>::new(),
        Vec::<String>::new(),
    )
    .unwrap();
    core.apply_trusted_catalogue_message(SyncMessage::SetCurrentWriteSchema {
        author: AuthorId::SYSTEM,
        pointer: CurrentWriteSchema { revision: 1, schema: evolved.id },
    })
    .unwrap();
    let item = row(0x77);
    accept_global(
        &mut core,
        MergeableCommit::new("items", item, 1).cells(BTreeMap::from([
            ("title".to_owned(), v("globally allowed")),
            ("status".to_owned(), Value::EnumTag(0)),
        ])),
    );
    core.commit_mergeable(
        MergeableCommit::new("items", item, 2).cells(BTreeMap::from([
            ("title".to_owned(), v("new local case")),
            ("status".to_owned(), Value::EnumTag(1)),
        ])),
    )
    .unwrap();

    let title_only = Query::from("items").select(["title"]).validate(&base).unwrap();
    let binding = title_only.bind(BTreeMap::new()).unwrap();
    let result = core.query_rows_for_link(
        &title_only,
        &binding,
        DurabilityTier::Local,
        user(0x77),
    );
    assert!(result.unwrap().is_empty(), "policy must hide incompatible row");

    // The same post-winner boundary applies before windowing and aggregation:
    // an unknown newer local version cannot leak the older global winner into
    // a page or count.
    let whole = Query::from("items").validate(&base).unwrap();
    assert!(core
        .query_rows(
            &whole,
            &whole.bind(BTreeMap::new()).unwrap(),
            DurabilityTier::Local,
        )
        .unwrap()
        .is_empty());
    let page = Query::from("items")
        .select(["title"])
        .order_by("status", crate::query::OrderDirection::Asc)
        .offset(0)
        .limit(1)
        .validate(&base)
        .unwrap();
    assert!(core
        .query_rows(
            &page,
            &page.bind(BTreeMap::new()).unwrap(),
            DurabilityTier::Local,
        )
        .unwrap()
        .is_empty());
    let count = Query::from("items")
        .count()
        .group_by("status")
        .validate(&base)
        .unwrap();
    assert!(core
        .query_rows(
            &count,
            &count.bind(BTreeMap::new()).unwrap(),
            DurabilityTier::Local,
        )
        .unwrap()
        .is_empty());

    core.commit_mergeable(
        MergeableCommit::new("items", item, 3).cells(BTreeMap::from([
            ("title".to_owned(), v("local known again")),
            ("status".to_owned(), Value::EnumTag(0)),
        ])),
    )
    .unwrap();
    assert_eq!(
        core.query_rows(
            &whole,
            &whole.bind(BTreeMap::new()).unwrap(),
            DurabilityTier::Local,
        )
        .unwrap()
        .len(),
        1,
    );
}

#[test]
fn old_enum_schema_omits_unknown_rows_from_materialized_query_sources() {
    // Contract: an old reader must receive an empty result—not an encoding
    // failure—when a selected winner uses a later enum case, even along source
    // paths that materialize projected rows before lowering.
    //
    // Actors: an evolved writer publishes `closed`; an old reader queries the
    // same physical row through history, a branch overlay, and deleted-row
    // inspection.
    //
    // ```text
    // evolved writer: closed ──► old reader: omitted
    //                         ├─ historical cut
    //                         ├─ branch overlay
    //                         └─ including-deleted source
    // ```
    let base = enum_projection_schema(&["open"]);
    let evolved = SchemaVersion::new(enum_projection_schema(&["open", "closed"]));
    let (_dir, mut core) = open_history_complete_node_with_schema(node(0x7b), base.clone());
    publish_schema_lineage(
        &mut core,
        evolved.clone(),
        enum_identity_lens(base.version_id(), evolved.id),
        Vec::<String>::new(),
        Vec::<String>::new(),
    )
    .unwrap();
    core.apply_trusted_catalogue_message(SyncMessage::SetCurrentWriteSchema {
        author: AuthorId::SYSTEM,
        pointer: CurrentWriteSchema { revision: 1, schema: evolved.id },
    })
    .unwrap();

    let old_shape = Query::from("items").validate(&base).unwrap();
    let old_binding = old_shape.bind(BTreeMap::new()).unwrap();
    let global_row = row(0x7b);
    accept_global(
        &mut core,
        MergeableCommit::new("items", global_row, 1).cells(BTreeMap::from([
            ("title".to_owned(), v("later global case")),
            ("status".to_owned(), Value::EnumTag(1)),
        ])),
    );

    assert!(core
        .projected_historical_current_rows("items", base.version_id(), GlobalSeq(0))
        .unwrap()
        .is_empty());

    assert!(core
        .query_rows_at(&old_shape, &old_binding, GlobalSeq(0))
        .unwrap()
        .is_empty());

    core.commit_mergeable(
        MergeableCommit::new("items", global_row, 2).deletion(DeletionEvent::Deleted),
    )
    .unwrap();
    assert!(core
        .query_rows_including_deleted_in_authorization_mode(
            &old_shape,
            &old_binding,
            DurabilityTier::Local,
            None,
            AuthorId::SYSTEM,
            QueryAuthorizationMode::TrustedServing,
        )
        .unwrap()
        .is_empty());

    let branch_id = branch(0x7b);
    core.create_root_branch(branch_id).unwrap();
    core.commit_mergeable_on_branch(
        branch_id,
        MergeableCommit::new("items", row(0x7c), 3).cells(BTreeMap::from([
            ("title".to_owned(), v("later branch case")),
            ("status".to_owned(), Value::EnumTag(1)),
        ])),
    )
    .unwrap();
    assert!(core
        .query_rows_on_branch(branch_id, &old_shape, &old_binding)
        .unwrap()
        .is_empty());
}

#[test]
fn old_enum_winner_projection_refreshes_after_later_registry_append() {
    // Contract: a raw current-winner projection installed for an old reader
    // survives later append-only enum registry growth without accepting a
    // changed projection mapping or type.
    //
    // Actors: an old reader opens while `closed` is the newest case; a latest
    // writer subsequently introduces and writes `archived`.
    //
    // ```text
    // base(open) ──► middle(+closed) ──► latest(+archived)
    // old query installs raw winner target        old query skips archived
    // ```
    let base = enum_projection_schema(&["open"]);
    let middle = SchemaVersion::new(enum_projection_schema(&["open", "closed"]));
    let latest = SchemaVersion::new(enum_projection_schema(&["open", "closed", "archived"]));
    let (_dir, mut core) = open_node_with_schema(node(0x7d), base.clone());
    publish_schema_lineage(
        &mut core,
        middle.clone(),
        enum_identity_lens(base.version_id(), middle.id),
        Vec::<String>::new(),
        Vec::<String>::new(),
    )
    .unwrap();
    core.apply_trusted_catalogue_message(SyncMessage::SetCurrentWriteSchema {
        author: AuthorId::SYSTEM,
        pointer: CurrentWriteSchema { revision: 1, schema: middle.id },
    })
    .unwrap();

    let item = row(0x7d);
    accept_global(
        &mut core,
        MergeableCommit::new("items", item, 1).cells(BTreeMap::from([
            ("title".to_owned(), v("compatible middle case")),
            ("status".to_owned(), Value::EnumTag(0)),
        ])),
    );
    let old_shape = Query::from("items").validate(&base).unwrap();
    let old_binding = old_shape.bind(BTreeMap::new()).unwrap();
    assert_eq!(
        core.query_rows(&old_shape, &old_binding, DurabilityTier::Local)
            .unwrap()
            .len(),
        1,
        "old read installs a current-winner projection before later growth",
    );
    publish_schema_lineage(
        &mut core,
        latest.clone(),
        enum_identity_lens(middle.id, latest.id),
        Vec::<String>::new(),
        Vec::<String>::new(),
    )
    .unwrap();
    core.apply_trusted_catalogue_message(SyncMessage::SetCurrentWriteSchema {
        author: AuthorId::SYSTEM,
        pointer: CurrentWriteSchema { revision: 2, schema: latest.id },
    })
    .unwrap();
    core.commit_mergeable(
        MergeableCommit::new("items", item, 2).cells(BTreeMap::from([
            ("title".to_owned(), v("later archived case")),
            ("status".to_owned(), Value::EnumTag(2)),
        ])),
    )
    .unwrap();

    assert!(core
        .query_rows(&old_shape, &old_binding, DurabilityTier::Local)
        .unwrap()
        .is_empty());
}

#[test]
fn old_enum_index_read_uses_global_index_before_post_winner_omission() {
    // Contract: an indexed Global read retains its physical index access path
    // while the old-schema compatibility boundary still runs after the chosen
    // winner. The unknown row must be omitted, not force a full table scan.
    let schema = |statuses: &[&str]| {
        JazzSchema::new([TableSchema::new(
            "items",
            [
                ColumnSchema::new("title", ColumnType::String),
                ColumnSchema::new(
                    "status",
                    ColumnType::EnumTag(
                        groove::records::ScalarEnumSchema::new("status", statuses.iter().copied())
                            .unwrap(),
                    ),
                ),
            ],
        )
        .with_indexed_column("title")])
    };
    let base = schema(&["open"]);
    let evolved = SchemaVersion::new(schema(&["open", "closed"]));
    let (_dir, mut core) = open_node_with_schema(node(0x7e), base.clone());
    publish_schema_lineage(
        &mut core,
        evolved.clone(),
        enum_identity_lens(base.version_id(), evolved.id),
        Vec::<String>::new(),
        Vec::<String>::new(),
    )
    .unwrap();
    core.apply_trusted_catalogue_message(SyncMessage::SetCurrentWriteSchema {
        author: AuthorId::SYSTEM,
        pointer: CurrentWriteSchema { revision: 1, schema: evolved.id },
    })
    .unwrap();
    accept_global(
        &mut core,
        MergeableCommit::new("items", row(0x7e), 1).cells(BTreeMap::from([
            ("title".to_owned(), v("indexed")),
            ("status".to_owned(), Value::EnumTag(1)),
        ])),
    );

    let query = Query::from("items")
        .filter(eq(col("title"), lit("indexed")))
        .validate(&base)
        .unwrap();
    let binding = query.bind(BTreeMap::new()).unwrap();
    core.reset_query_engine_read_metrics();
    assert!(core
        .query_rows(&query, &binding, DurabilityTier::Global)
        .unwrap()
        .is_empty());
    let metrics = core.query_engine_read_metrics();
    assert_eq!(metrics.source_index_probes, 1);
    assert_eq!(metrics.source_full_scans, 0);
}

#[test]
fn enum_projection_requirement_none_allows_unused_relation_enum() {
    let schema = |states: &[&str]| JazzSchema::new([
        TableSchema::new("items", [
            ColumnSchema::new("title", ColumnType::String),
            ColumnSchema::new("state", ColumnType::Uuid),
        ]).with_reference("state", "states"),
        TableSchema::new("states", [ColumnSchema::new("status", ColumnType::EnumTag(
            groove::records::ScalarEnumSchema::new("status", states.iter().copied()).unwrap(),
        ))]),
    ]);
    let base = schema(&["open"]);
    let evolved = SchemaVersion::new(schema(&["open", "closed"]));
    let (_dir, mut core) = open_node_with_schema(node(0x78), base.clone());
    publish_schema_lineage(&mut core, evolved.clone(), MigrationLens::new(
        base.version_id(), evolved.id, vec![
            TableLens { source_table: "items".into(), target_table: "items".into(), ops: vec![] },
            TableLens { source_table: "states".into(), target_table: "states".into(), ops: vec![LensOp::TransformColumn { column: "status".into(), transform: "jazz.identity".into() }] },
        ],
    ), Vec::<String>::new(), Vec::<String>::new()).unwrap();
    core.apply_trusted_catalogue_message(SyncMessage::SetCurrentWriteSchema { author: AuthorId::SYSTEM, pointer: CurrentWriteSchema { revision: 1, schema: evolved.id } }).unwrap();
    let state = row(0x79);
    core.commit_mergeable(MergeableCommit::new("states", state, 1).cells(BTreeMap::from([("status".into(), Value::EnumTag(1))]))).unwrap();
    core.commit_mergeable(MergeableCommit::new("items", row(0x7a), 2).cells(BTreeMap::from([
        ("title".into(), v("root remains readable")), ("state".into(), Value::Uuid(state.0)),
    ]))).unwrap();

    let root_only = Query::from("items").select(["title"]).validate(&base).unwrap();
    assert!(core.query_rows(&root_only, &root_only.bind(BTreeMap::new()).unwrap(), DurabilityTier::Local).is_ok());
    // Includes are hydrated by a separate path; this compilation path still
    // proves the implicit relation source has no accidental enum dependency.
    let included = Query::from("items").select(["title"]).include("state").validate(&base).unwrap();
    assert!(core.query_rows(&included, &included.bind(BTreeMap::new()).unwrap(), DurabilityTier::Local).is_ok());
}

#[test]
fn independent_column_enum_registries_evolve_additively_across_reopen() {
    // Registry allocation is physical catalogue metadata, so this internal
    // test asserts both user-visible decoding and the non-Cartesian boundary.
    let base = independent_enum_schema(&["a0"], &["b0"]);
    let a_evolved = SchemaVersion::new(independent_enum_schema(&["a0", "a1"], &["b0"]));
    let b_evolved = SchemaVersion::new(independent_enum_schema(&["a0", "a1"], &["b0", "b1"]));
    let (dir, mut core) = open_node_with_schema(node(0x72), base.clone());
    core.commit_mergeable(
        MergeableCommit::new("items", row(0x72), 1).cells(BTreeMap::from([
            ("a".to_owned(), Value::EnumTag(0)),
            ("b".to_owned(), Value::EnumTag(0)),
        ])),
    )
    .unwrap();
    let enum_lens = |source, target, column: &str| {
        MigrationLens::new(
            source,
            target,
            vec![TableLens {
                source_table: "items".to_owned(),
                target_table: "items".to_owned(),
                ops: vec![LensOp::TransformColumn {
                    column: column.to_owned(),
                    transform: "jazz.identity".to_owned(),
                }],
            }],
        )
    };
    publish_schema_lineage(
        &mut core,
        a_evolved.clone(),
        enum_lens(base.version_id(), a_evolved.id, "a"),
        Vec::<String>::new(),
        Vec::<String>::new(),
    )
    .unwrap();
    core.apply_trusted_catalogue_message(SyncMessage::SetCurrentWriteSchema {
        author: AuthorId::SYSTEM,
        pointer: CurrentWriteSchema {
            revision: 1,
            schema: a_evolved.id,
        },
    })
    .unwrap();
    core.commit_mergeable(
        MergeableCommit::new("items", row(0x73), 2).cells(BTreeMap::from([
            ("a".to_owned(), Value::EnumTag(1)),
            ("b".to_owned(), Value::EnumTag(0)),
        ])),
    )
    .unwrap();
    let table_id = core.catalogue.physical_mappings[&a_evolved.id].tables["items"].table_id;
    let enum_registry_ids = core.catalogue.physical_mappings[&a_evolved.id].tables["items"]
        .columns
        .values()
        .map(|column| {
            groove::records::variant_registry_id_for_path(&format!(
                "physical-column/{}/nullable",
                column.0
            ))
        })
        .collect::<BTreeSet<_>>();
    let physical_name = physical_history_table_name(table_id);
    let after_a = core.database.table_schema(&physical_name).unwrap();
    let after_a_sizes = after_a
        .value_variant_registries
        .iter()
        .filter(|(id, _)| enum_registry_ids.contains(id))
        .map(|(_, registry)| match registry {
            groove::records::VariantRegistry::EnumTag { variants } => variants.len(),
            groove::records::VariantRegistry::Enum { cases } => cases.len(),
        })
        .collect::<BTreeSet<_>>();
    assert_eq!(after_a_sizes, BTreeSet::from([1, 2]));
    publish_schema_lineage(
        &mut core,
        b_evolved.clone(),
        enum_lens(a_evolved.id, b_evolved.id, "b"),
        Vec::<String>::new(),
        Vec::<String>::new(),
    )
    .unwrap();
    core.apply_trusted_catalogue_message(SyncMessage::SetCurrentWriteSchema {
        author: AuthorId::SYSTEM,
        pointer: CurrentWriteSchema {
            revision: 2,
            schema: b_evolved.id,
        },
    })
    .unwrap();
    core.commit_mergeable(
        MergeableCommit::new("items", row(0x74), 3).cells(BTreeMap::from([
            ("a".to_owned(), Value::EnumTag(1)),
            ("b".to_owned(), Value::EnumTag(1)),
        ])),
    )
    .unwrap();
    assert_eq!(core.query_table_versions("items").unwrap().len(), 3);

    let base_mapping = &core.catalogue.physical_mappings[&base.version_id()].tables["items"];
    let a_mapping = &core.catalogue.physical_mappings[&a_evolved.id].tables["items"];
    let b_mapping = &core.catalogue.physical_mappings[&b_evolved.id].tables["items"];
    assert_eq!(base_mapping.columns, a_mapping.columns);
    assert_eq!(a_mapping.columns, b_mapping.columns);
    assert_eq!(base_mapping.table_id, b_mapping.table_id);

    let table = core.database.table_schema(&physical_name).unwrap();
    let user_registries = table
        .value_variant_registries
        .iter()
        .filter(|(id, _)| enum_registry_ids.contains(id))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(user_registries.len(), 2);
    assert_eq!(table.variants.len(), 3);
    let stable_tags = table
        .variants
        .iter()
        .map(|variant| variant.tag)
        .collect::<Vec<_>>();
    assert_eq!(
        user_registries
            .values()
            .map(|registry| match registry {
                groove::records::VariantRegistry::EnumTag { variants } => variants.len(),
                groove::records::VariantRegistry::Enum { .. } => 0,
            })
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([2])
    );
    drop(core);

    let mut reopened = reopen_node_at(&dir, node(0x72), base);
    let table = reopened.database.table_schema(&physical_name).unwrap();
    assert_eq!(table.variants.len(), 3);
    assert_eq!(
        table
            .variants
            .iter()
            .map(|variant| variant.tag)
            .collect::<Vec<_>>(),
        stable_tags
    );
    assert_eq!(
        table
            .value_variant_registries
            .keys()
            .filter(|id| enum_registry_ids.contains(id))
            .count(),
        2
    );
    assert_eq!(reopened.query_table_versions("items").unwrap().len(), 3);
}

#[test]
fn heterogeneous_schema_projected_reads_keep_prepared_plans_valid() {
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
        .prepared_query_plan(&shape, &binding, DurabilityTier::Local, AuthorId::SYSTEM)
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
    core.commit_mergeable(
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
        ),
        Vec::<String>::new(),
        Vec::<String>::new(),
    )
    .unwrap();
    join_core
        .apply_trusted_catalogue_message(SyncMessage::SetCurrentWriteSchema {
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
        .commit_mergeable(MergeableCommit::new("todo_members", row(0x4e), 21).cells(
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
    let evolved = JazzSchema::new([TableSchema::new(
        "todos",
        [
            ColumnSchema::new("name", ColumnType::String),
            ColumnSchema::new("body", ColumnType::String),
        ],
    )]);
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

    let team1 = row(0x51);
    let team2 = row(0x52);
    let team3 = row(0x53);
    for idx in [0x51, 0x52, 0x53] {
        core.commit_mergeable(MergeableCommit::new("teams", row(idx), idx as u64).cells(
            BTreeMap::from([("name".to_owned(), v(format!("team-{idx}")))]),
        ))
        .unwrap();
    }
    core.commit_mergeable(
        MergeableCommit::new("docs", row(0xd1), 20)
            .cells(BTreeMap::from([("title".to_owned(), v("reachable"))])),
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
    let evolved = SchemaVersion::new(JazzSchema::new([TableSchema::new(
        "todos",
        [
            ColumnSchema::new("name", ColumnType::String),
            ColumnSchema::new("body", ColumnType::String),
        ],
    )]));
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
        ),
        Vec::<String>::new(),
        Vec::<String>::new(),
    )
    .unwrap();
    core.apply_trusted_catalogue_message(SyncMessage::SetCurrentWriteSchema {
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
    core.commit_mergeable(
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
            AuthorId::SYSTEM,
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
        ),
        Vec::<String>::new(),
        Vec::<String>::new(),
    )
    .unwrap();
    core.apply_trusted_catalogue_message(SyncMessage::SetCurrentWriteSchema {
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
    core.commit_mergeable(
        MergeableCommit::new("issues", issue, 11).deletion(DeletionEvent::Deleted),
    )
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
    let binding = shape
        .bind(BTreeMap::from([("tag".to_owned(), v("bug"))]))
        .unwrap();
    let rows = core
        .query_rows_including_deleted_in_authorization_mode(
            &shape,
            &binding,
            DurabilityTier::Local,
            None,
            AuthorId::SYSTEM,
            QueryAuthorizationMode::TrustedServing,
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
        ),
        Vec::<String>::new(),
        Vec::<String>::new(),
    )
    .unwrap();
    core.apply_trusted_catalogue_message(SyncMessage::SetCurrentWriteSchema {
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
    core.commit_mergeable(MergeableCommit::new("team_access", row(0x60), 15).cells(
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
            AuthorId::SYSTEM,
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
    let evolved = JazzSchema::new([TableSchema::new(
        "todos",
        [
            ColumnSchema::new("name", ColumnType::String),
            ColumnSchema::new("body", ColumnType::String),
        ],
    )]);
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
    let renamed_schema = JazzSchema::new([TableSchema::new(
        "tasks",
        [ColumnSchema::new("name", ColumnType::String)],
    )]);
    let renamed = SchemaVersion::new(renamed_schema);
    let (dir, mut core) = open_node_with_schema(node(0x57), base.clone());

    let base_tx = core
        .commit_mergeable(
            MergeableCommit::new("todos", row(0x57), 10).cells(title_cells("before rename")),
        )
        .unwrap();
    core.apply_fate_update(
        base_tx,
        Fate::Accepted,
        Some(GlobalSeq(1)),
        Some(DurabilityTier::Global),
    )
    .unwrap();

    let exclusive = OpenBatchId::new();
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
        ),
        Vec::<String>::new(),
        Vec::<String>::new(),
    )
    .unwrap();
    core.apply_trusted_catalogue_message(SyncMessage::SetCurrentWriteSchema {
        author: AuthorId::SYSTEM,
        pointer: CurrentWriteSchema {
            revision: 1,
            schema: renamed.id,
        },
    })
    .unwrap();

    let renamed_tx = core
        .commit_mergeable(
            MergeableCommit::new("tasks", row(0x59), 11)
                .cells(BTreeMap::from([("name".to_owned(), v("after rename"))])),
        )
        .unwrap();
    core.apply_fate_update(
        renamed_tx,
        Fate::Accepted,
        Some(GlobalSeq(2)),
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
        core.commit_exclusive(exclusive, AuthorId::SYSTEM, 12),
        Err(Error::TransactionConflict)
    ));

    let shape = Query::from("todos").validate(&base).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    assert_eq!(
        core.query_rows_at(&shape, &binding, GlobalSeq(1))
            .unwrap()
            .into_iter()
            .map(current_row_pair)
            .collect::<BTreeMap<_, _>>(),
        BTreeMap::from([(row(0x57), title_cells("before rename"))])
    );
    core.reset_query_engine_read_metrics();
    assert_eq!(
        core.query_rows_at(&shape, &binding, GlobalSeq(2))
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
            .source_global_seq_range_scans,
        1,
        "a renamed lineage should use the bounded physical global-change source"
    );

    drop(core);
    let mut reopened = reopen_node_at(&dir, node(0x57), base.clone());
    assert!(
        reopened
            .global_currency_changed_after("todos", GlobalSeq(1))
            .unwrap()
    );
    assert_eq!(
        reopened
            .query_rows_at(&shape, &binding, GlobalSeq(2))
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

    let team1 = row(0x56);
    let team2 = row(0x57);
    let team3 = row(0x58);
    for idx in [0x56, 0x57, 0x58] {
        let tx_id = core
            .commit_mergeable(MergeableCommit::new("teams", row(idx), idx as u64).cells(
                BTreeMap::from([("name".to_owned(), v(format!("team-{idx}")))]),
            ))
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
            MergeableCommit::new("docs", row(0xd5), 90)
                .cells(BTreeMap::from([("name".to_owned(), v("reachable"))])),
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
    let rows = core
        .query_rows_at(&shape, &binding, GlobalSeq(230))
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
use crate::node::query_engine::QueryAuthorizationMode;
