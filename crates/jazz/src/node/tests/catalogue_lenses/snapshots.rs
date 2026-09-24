// Catalogue snapshot installation, durable validation, and dynamic-catalogue bootstrap.

#[test]
fn schema_version_id_round_trips_through_wire_ingest_and_recovery() {
    let schema = schema();
    let expected_schema_version = schema.version_id();
    let (_writer_dir, mut writer) = open_node_with_schema(node(0x31), schema.clone());
    let (core_dir, mut core) = open_node_with_schema(node(0x32), schema.clone());

    let commit = MergeableCommit::new("todos", row(0x44), 1_000)
        .made_by(AuthorSubject::SYSTEM)
        .cells(BTreeMap::from([(
            "title".to_owned(),
            Value::String("lens hook".to_owned()),
        )]));
    let (_tx_id, unit) = writer.commit_mergeable_unit_settled(commit).unwrap();
    let SyncMessage::CommitUnit { versions, .. } = &unit else {
        panic!("commit unit expected");
    };
    assert_eq!(versions.len(), 1);
    assert_eq!(versions[0].schema_version(), expected_schema_version);

    core.apply_sync_message_settled(unit).unwrap();
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
fn trusted_snapshot_carries_policy_source_and_receiver_recompiles_it_after_reopen() {
    let public = crate::tools::SchemaBuilder::new()
        .table(
            crate::tools::TableSchema::builder("todos")
                .column("title", crate::tools::ColumnType::Text)
                .policies(
                    crate::tools::TablePolicies::new()
                        .with_select(crate::tools::PolicyExpr::True),
                ),
        )
        .build();
    let compiled = crate::schema::JazzSchema::new(&public)
        .expect("compile authority source");
    let (_authority_dir, authority) = open_node_with_schema(node(0x33), compiled.clone());
    let snapshot = authority.catalogue_snapshot().expect("authority snapshot");
    let encoded = postcard::to_allocvec(&snapshot).expect("encode source snapshot");
    let snapshot: crate::protocol::CatalogueSnapshot =
        postcard::from_bytes(&encoded).expect("decode and compile source snapshot");

    let empty = empty_public_test_schema();
    let receiver_dir = tempfile::tempdir().expect("create receiver store");
    let cfs = empty.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = RocksDbStorage::open(receiver_dir.path(), &refs).expect("open receiver store");
    let mut receiver = NodeState::new_catalogue_uninitialized(node(0x34), storage)
        .expect("open uninitialized receiver");
    receiver.apply_trusted_catalogue_snapshot_settled(snapshot)
        .expect("install source snapshot");
    assert!(
        receiver.try_current_schema()
            .expect("receiver has a current schema")
            .tables
            .iter()
            .find(|table| table.name == "todos")
            .and_then(|table| table.read_policy.as_ref())
            .is_some(),
        "receiver compiles the source PolicyExpr"
    );

    drop(receiver);
    let cfs = empty.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = RocksDbStorage::open(receiver_dir.path(), &refs).expect("reopen receiver store");
    let reopened = NodeState::new_catalogue_uninitialized(node(0x34), storage)
        .expect("reopen receiver from persisted source");
    assert_eq!(reopened.try_current_schema().unwrap(), &compiled);
    assert_eq!(
        reopened.try_current_schema().unwrap().public_schema(),
        compiled.public_schema()
    );
}

#[test]
fn trusted_catalogue_snapshot_installs_lineage_before_authored_payloads() {
    // This is an internal transport-boundary test: public clients never apply
    // trusted upstream catalogue snapshots directly.
    let base = schema();
    let evolved = SchemaVersion::new(catalogue_evolved_schema_with_allow_all());
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
    ).expect("valid migration lens");
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
        .activate_catalogue_schema_settled(CurrentWriteSchema {
            revision: 1,
            schema: evolved.id,
        })
        .unwrap();
    let (_, authored) = authority
        .commit_mergeable_unit_settled(
            MergeableCommit::new("todos", row(0x36), 10).cells(BTreeMap::from([
                ("title".to_owned(), v("authored")),
                ("body".to_owned(), v("under-evolved-schema")),
            ])),
        )
        .unwrap();

    let (_receiver_dir, mut receiver) = open_node_with_schema(node(0x37), base.clone());
    let subscription = receiver
        .subscribe_history("todos")
        .expect("subscribe before historical snapshot import");
    assert!(subscription
        .recv()
        .expect("initial history snapshot")
        .is_empty());
    let runtime_before_snapshot = receiver.groove_runtime_token();
    let history_table = physical_history_table_name(
        receiver
            .physical_table_id_for_schema(base.version_id(), "todos")
            .expect("base physical table"),
    );
    let history_registry_before = receiver
        .database
        .table_schema(&history_table)
        .expect("base history registry")
        .clone();
    let snapshot = authority.catalogue_snapshot().unwrap();
    assert!(matches!(
        receiver.apply_sync_message_settled(SyncMessage::CatalogueSnapshot(Box::new(snapshot.clone()))),
        Err(Error::UnsupportedSyncMessage(
            "catalogue snapshot requires a trusted upstream link"
        ))
    ));

    // First fail after physical variants have been installed but before their
    // projections.  The synchronizer itself must roll that prefix back.
    receiver.set_catalogue_activation_failpoint(
        CatalogueActivationFailpoint::AfterPhysicalRegistryRegistration,
    );
    assert!(matches!(
        receiver.apply_trusted_catalogue_snapshot_settled(snapshot.clone()),
        Err(Error::CatalogueActivationFailed)
    ));
    assert_eq!(receiver.groove_runtime_token(), runtime_before_snapshot);
    assert_eq!(receiver.active_catalogue_seq(), 0);
    assert_eq!(receiver.catalogue_schemas().len(), 1);
    assert_eq!(
        receiver
            .database
            .table_schema(&history_table)
            .expect("rolled-back history registry"),
        &history_registry_before,
        "partial variant registration must not leak a new runtime capability"
    );

    // Registration has already added the evolved physical variants and their
    // projection cases by this point.  The activation receipt is still the
    // visibility boundary: a failure immediately before it must restore the
    // complete old registry, not merely the in-memory catalogue.
    receiver.set_catalogue_activation_failpoint(
        CatalogueActivationFailpoint::BeforeSnapshotActivationCommit,
    );
    assert!(matches!(
        receiver.apply_trusted_catalogue_snapshot_settled(snapshot.clone()),
        Err(Error::CatalogueActivationFailed)
    ));
    assert_eq!(receiver.groove_runtime_token(), runtime_before_snapshot);
    assert_eq!(receiver.active_catalogue_seq(), 0);
    assert_eq!(receiver.catalogue_schemas().len(), 1);
    assert_eq!(
        receiver
            .database
            .table_schema(&history_table)
            .expect("rolled-back history registry"),
        &history_registry_before,
        "activation failure restores the complete registry, not only catalogue metadata"
    );
    assert_eq!(
        receiver.query_all_versions().expect("old history remains readable"),
        Vec::new(),
        "a failed snapshot leaves rows and their old registry interpretation untouched"
    );

    receiver.apply_trusted_catalogue_snapshot_settled(snapshot).unwrap();
    assert_eq!(
        receiver.groove_runtime_token(),
        runtime_before_snapshot,
        "a historical/write-schema import extends the live physical registry in place"
    );
    assert_eq!(receiver.current_write_schema().unwrap().schema, evolved.id);
    receiver.apply_sync_message_settled(authored).unwrap();
    assert_eq!(
        subscription
            .recv()
            .expect("the pre-import history stream receives the new authored variant")
            .iter()
            .filter(|(_, weight)| *weight > 0)
            .count(),
        1,
        "the in-place registry extension preserves the existing subscription"
    );
    let versions = receiver.query_all_versions().unwrap();
    assert_eq!(versions.len(), 1);
    assert_eq!(
        receiver
            .schema_version_for_alias(versions[0].schema_version_alias())
            .unwrap(),
        evolved.id
    );
}

/// A pre-activation failpoint is wholly in-memory and may be retried. By
/// contrast, once the catalogue batch has reached the storage persistence
/// boundary, its failure has publication-order consequences that cannot be
/// rolled back by restoring only the registry. This internal receipt injects
/// that boundary directly; public callers can only observe the fail-closed
/// node afterwards.
#[test]
fn trusted_catalogue_snapshot_persistence_failure_keeps_runtime_poisoned() {
    let (mut receiver, storage) = fail_write_many_node();
    let snapshot = catalogue_snapshot_fixture();
    storage.fail_nth_following_write_many(1);

    assert!(matches!(
        receiver.apply_trusted_catalogue_snapshot_settled(snapshot.clone()),
        Err(Error::CatalogueActivationFailed)
    ));
    assert_poisoned_node_exposes_nothing(&mut receiver);
    assert!(matches!(
        receiver.apply_trusted_catalogue_snapshot_settled(snapshot),
        Err(Error::CatalogueActivationFailed)
    ));
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
        ).expect("valid migration lens"),
        Vec::<String>::new(),
        Vec::<String>::new(),
    )
    .unwrap();
    authority
        .activate_catalogue_schema_settled(CurrentWriteSchema {
            revision: 1,
            schema: evolved.id,
        })
        .unwrap();

    let (_receiver_dir, mut receiver) =
        open_node_with_schema(node(0x61), evolved.schema.clone());
    let local_alias = receiver.catalogue.local_schema_version_alias.unwrap();
    let local_mapping = receiver.catalogue.physical_mappings[&evolved.id].clone();
    let authority_identities = authority.catalogue.physical_mappings[&evolved.id]
        .identities
        .clone();
    let (tx_id, _) = receiver
        .commit_mergeable_unit_settled(
            MergeableCommit::new("todos", row(0x62), 10).cells(BTreeMap::from([
                ("title".to_owned(), v("before-snapshot")),
                ("body".to_owned(), v("authored-evolved")),
            ])),
        )
        .unwrap();
    let subscription = receiver
        .subscribe_history("todos")
        .expect("subscribe before authority bootstrap");
    assert_eq!(
        subscription
            .recv()
            .expect("initial history snapshot")
            .deltas
            .len(),
        1,
        "the live subscription starts on the locally authored version"
    );
    let runtime_before_snapshot = receiver.groove_runtime_token();
    receiver
        .apply_trusted_catalogue_snapshot_settled(authority.catalogue_snapshot().unwrap())
        .unwrap();

    assert_eq!(
        receiver.groove_runtime_token(),
        runtime_before_snapshot,
        "learning historical authority lineage and permanent identities must not rebuild the active runtime"
    );
    receiver
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row(0x6a), 11).cells(BTreeMap::from([
                ("title".to_owned(), v("after-snapshot")),
                ("body".to_owned(), v("still-live")),
            ])),
        )
        .unwrap();
    assert_eq!(
        subscription
            .recv()
            .expect("live subscription after authority bootstrap")
            .iter()
            .filter(|(_, weight)| *weight > 0)
            .count(),
        1,
        "the pre-bootstrap subscription remains attached to the live runtime"
    );

    assert_eq!(
        receiver.catalogue.local_schema_version_alias,
        Some(local_alias)
    );
    let received_mapping = &receiver.catalogue.physical_mappings[&evolved.id];
    assert_eq!(received_mapping.tables, local_mapping.tables);
    assert_eq!(received_mapping.identities, authority_identities);
    assert_ne!(received_mapping.identities, local_mapping.identities);
    let stored = receiver.query_versions_for_tx(tx_id).unwrap();
    assert_eq!(stored.len(), 1);
    assert_eq!(stored[0].schema_version_alias(), local_alias);
    receiver.physical_table_id_for_version(&stored[0]).unwrap();

    // The converse is deliberately not a no-op: policy is part of the active
    // read schema even though it does not change that schema version's hash.
    // A trusted same-version semantic change must retire live runtime handles.
    let mut active_policy_change = authority.catalogue_snapshot().unwrap();
    active_policy_change.current_write_schema.revision += 1;
    active_policy_change
        .schemas
        .iter_mut()
        .find(|schema| schema.id == evolved.id)
        .expect("authority snapshot contains active schema")
        .schema
        .runtime_mut_for_testing()
        .tables[0]
        .read_policy = Some(Query::from("todos").filter(eq(col("title"), lit("after-snapshot"))));
    let runtime_before_active_change = receiver.groove_runtime_token();
    receiver
        .apply_trusted_catalogue_snapshot_settled(active_policy_change)
        .expect("install active-schema semantic change");
    assert_ne!(
        receiver.groove_runtime_token(),
        runtime_before_active_change,
        "an active read-schema semantic change must rebuild the Groove runtime"
    );

    // This is a planted runtime-layout change: the authority may never choose
    // this node-local alias itself, but any path which does change it must
    // force the same rebuild rather than retaining graphs compiled against the
    // old record descriptor.
    let mut changed_active_layout = receiver.catalogue.clone();
    changed_active_layout
        .physical_mappings
        .get_mut(&evolved.id)
        .expect("active mapping")
        .tables
        .get_mut("todos")
        .expect("active table mapping")
        .columns
        .insert("title".to_owned(), PhysicalColumnId(0xfeed));
    assert!(
        !super::super::catalogue_ingest::active_runtime_layouts_equal(
            &receiver.catalogue,
            &changed_active_layout,
        ),
        "a changed active physical column layout is rebuild-relevant"
    );
}

#[test]
fn authored_columns_cross_nodes_with_different_physical_column_ids() {
    // Physical column ids are deliberately node-local. This internal wire-
    // boundary test makes the same evolved schema allocate `body` differently
    // on each node, then verifies that logical names cross the wire and each
    // side persists only its own id.
    let base = schema();
    let filler = SchemaVersion::new(build_public_test_schema(
        PublicSchemaBuilder::new().table(
            PublicTableSchemaBuilder::new("todos")
                .column("title", PublicColumnType::Text)
                .column("scratch", PublicColumnType::Text),
        ),
    ));
    let evolved = SchemaVersion::new(catalogue_evolved_schema());
    let (_authority_dir, mut authority) = open_node_with_schema(node(0x63), base.clone());
    publish_schema_lineage(
        &mut authority,
        filler.clone(),
        MigrationLens::new(
            base.version_id(),
            filler.id,
            vec![TableLens {
                source_table: "todos".to_owned(),
                target_table: "todos".to_owned(),
                ops: vec![LensOp::AddColumn {
                    column: "scratch".to_owned(),
                    default: v(""),
                }],
            }],
        ).expect("valid migration lens"),
        Vec::<String>::new(),
        Vec::<String>::new(),
    )
    .unwrap();
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
        ).expect("valid migration lens"),
        Vec::<String>::new(),
        Vec::<String>::new(),
    )
    .unwrap();
    authority
        .activate_catalogue_schema_settled(CurrentWriteSchema {
            revision: 1,
            schema: evolved.id,
        })
        .unwrap();

    let (receiver_dir, mut receiver) =
        open_node_with_schema(node(0x64), evolved.schema.clone());
    let receiver_body_id =
        receiver.catalogue.physical_mappings[&evolved.id].tables["todos"].columns["body"];
    let authority_body_id =
        authority.catalogue.physical_mappings[&evolved.id].tables["todos"].columns["body"];
    assert_ne!(authority_body_id, receiver_body_id);
    receiver
        .apply_trusted_catalogue_snapshot_settled(authority.catalogue_snapshot().unwrap())
        .unwrap();
    assert_eq!(
        receiver.catalogue.physical_mappings[&evolved.id].tables["todos"].columns["body"],
        receiver_body_id
    );

    let (tx_id, unit) = authority
        .commit_mergeable_unit_settled(
            MergeableCommit::new("todos", row(0x65), 10)
                .cells(BTreeMap::from([("body".to_owned(), v("authored"))]))
                .authored_columns(BTreeSet::from(["body".to_owned()])),
        )
        .unwrap();
    // Exercise the explicit immutable-row envelope before local catalogue translation.
    let wire = crate::wire::encode_sync_message(&unit).unwrap();
    assert!(wire.windows(5).any(|bytes| bytes == b"JVRR\x01"));
    let unit = crate::wire::decode_sync_message(&wire).unwrap();
    receiver.apply_sync_message_settled(unit).unwrap();
    let stored = receiver.query_versions_for_tx(tx_id).unwrap();
    assert_eq!(stored[0].authored_column_ids().unwrap(), Some(BTreeSet::from([receiver_body_id])));
    assert_eq!(
        receiver
            .version_record_from_row(&stored[0])
            .unwrap()
            .authored_columns(),
        Some(&BTreeSet::from(["body".to_owned()]))
    );
    drop(receiver);

    let mut reopened = reopen_node_at(&receiver_dir, node(0x64), evolved.schema);
    let stored = reopened.query_versions_for_tx(tx_id).unwrap();
    assert_eq!(
        reopened
            .version_record_from_row(&stored[0])
            .unwrap()
            .authored_columns(),
        Some(&BTreeSet::from(["body".to_owned()]))
    );
}

#[test]
fn authored_columns_follow_a_renamed_column_through_wire_and_reopen() {
    // Internal because physical ids are local storage aliases. The public
    // contract under test is that a v1 `title` patch and a v2 `name` patch
    // retain their authored schema names on the wire while sharing one local
    // physical-column identity across the rename.
    let base = schema();
    let renamed_schema = evolved_todos_name_body_schema();
    let renamed = SchemaVersion::new(renamed_schema.clone());
    let (authority_dir, mut authority) = open_node_with_schema(node(0x66), base.clone());
    let (old_tx, old_unit) = authority
        .commit_mergeable_unit_settled(
            MergeableCommit::new("todos", row(0x67), 10)
                .cells(BTreeMap::from([("title".to_owned(), v("old"))]))
                .authored_columns(BTreeSet::from(["title".to_owned()])),
        )
        .unwrap();
    publish_schema_lineage(
        &mut authority,
        renamed.clone(),
        MigrationLens::new(
            base.version_id(),
            renamed.id,
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
                        default: v(""),
                    },
                ],
            }],
        ).expect("valid migration lens"),
        Vec::<String>::new(),
        Vec::<String>::new(),
    )
    .unwrap();
    authority
        .activate_catalogue_schema_settled(CurrentWriteSchema {
            revision: 1,
            schema: renamed.id,
        })
        .unwrap();
    let (new_tx, new_unit) = authority
        .commit_mergeable_unit_settled(
            MergeableCommit::new("todos", row(0x68), 11)
                .cells(BTreeMap::from([("name".to_owned(), v("new"))]))
                .authored_columns(BTreeSet::from(["name".to_owned()])),
        )
        .unwrap();

    let title_id = authority.catalogue.physical_mappings[&base.version_id()].tables["todos"]
        .columns["title"];
    let name_id = authority.catalogue.physical_mappings[&renamed.id].tables["todos"].columns["name"];
    assert_eq!(title_id, name_id, "a compatible rename retains the column id");
    for (tx_id, logical_name) in [(old_tx, "title"), (new_tx, "name")] {
        let stored = authority.query_versions_for_tx(tx_id).unwrap().remove(0);
        assert_eq!(stored.authored_column_ids().unwrap(), Some(BTreeSet::from([title_id])));
        assert_eq!(
            authority.version_record_from_row(&stored).unwrap().authored_columns(),
            Some(&BTreeSet::from([logical_name.to_owned()])),
        );
    }

    let (receiver_dir, mut receiver) = open_node_with_schema(node(0x69), renamed_schema.clone());
    receiver
        .apply_trusted_catalogue_snapshot_settled(authority.catalogue_snapshot().unwrap())
        .unwrap();
    receiver.apply_sync_message_settled(old_unit).unwrap();
    receiver.apply_sync_message_settled(new_unit).unwrap();
    drop(receiver);
    drop(authority);

    let mut reopened_authority = reopen_node_at(&authority_dir, node(0x66), base);
    let mut reopened_receiver = reopen_node_at(&receiver_dir, node(0x69), renamed_schema);
    for node in [&mut reopened_authority, &mut reopened_receiver] {
        for (tx_id, logical_name) in [(old_tx, "title"), (new_tx, "name")] {
            let stored = node.query_versions_for_tx(tx_id).unwrap().remove(0);
            assert_eq!(
                node.version_record_from_row(&stored).unwrap().authored_columns(),
                Some(&BTreeSet::from([logical_name.to_owned()])),
            );
        }
    }
}

#[test]
fn settled_view_projects_old_authored_row_into_clients_active_schema() {
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
        ).expect("valid migration lens"),
        Vec::<String>::new(),
        Vec::<String>::new(),
    )
    .unwrap();
    authority
        .activate_catalogue_schema_settled(CurrentWriteSchema {
            revision: 1,
            schema: evolved.id,
        })
        .unwrap();

    let snapshot = authority.catalogue_snapshot().unwrap();
    let (_writer_dir, mut writer) = open_node_with_schema(node(0x64), base.clone());
    writer
        .apply_trusted_catalogue_snapshot_settled(snapshot.clone())
        .unwrap();
    let tx_id = writer
        .commit_mergeable_in_schema_settled(
            base.version_id(),
            MergeableCommit::new("todos", row(0x65), 10).cells(title_cells("authored-base")),
        )
        .unwrap();
    let unit = writer.commit_unit_for(tx_id).unwrap();
    let SyncMessage::CommitUnit { tx, versions } = unit else {
        panic!("commit unit expected");
    };
    assert_eq!(versions[0].schema_version(), base.version_id());

    let (_receiver_dir, mut receiver) =
        open_node_with_schema(node(0x66), evolved.schema.clone());
    receiver
        .apply_trusted_catalogue_snapshot_settled(snapshot)
        .unwrap();
    receiver
        .ingest_known_transaction(
            tx,
            versions,
            Fate::Accepted,
            Some(GlobalTime(1)),
            DurabilityTier::Global,
        )
        .unwrap();

    let shape = Query::from("todos").validate(&evolved.schema).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();

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

#[test]
fn mergeable_commit_rejects_unadmitted_authored_schema() {
    // Internal because this verifies the node admission boundary underlying a
    // public client's fixed-schema write contract.
    let (_dir, mut writer) = open_node_with_schema(node(0x67), schema());
    let unknown = SchemaVersionId(uuid::Uuid::from_bytes([0x67; 16]));
    assert!(matches!(
        writer.commit_mergeable_in_schema_settled(
            unknown,
            MergeableCommit::new("todos", row(0x68), 10).cells(title_cells("forged")),
        ),
        Err(Error::InvalidMergeableCommit("authored schema version is not admitted"))
    ));
    assert!(writer.query_all_versions().unwrap().is_empty());
}

#[test]
fn trusted_catalogue_snapshot_imports_historical_lineage_without_rebuilding_active_runtime() {
    // A trusted snapshot is a complete authoritative prefix, not a delta. Once
    // its activation commits, reopening must retain enough canonical lineage
    // identity to recognize that same prefix on the next upstream connection.
    let base = schema();
    let snapshot = catalogue_snapshot_fixture_for_schema(catalogue_evolved_schema_with_allow_all());
    let (dir, mut receiver) = open_node_with_schema(node(0x3f), base.clone());
    // Establish the authority's UUIDs first: adding historical lineage below
    // must not accidentally also test adoption of a fresh genesis manifest.
    let mut genesis = snapshot.clone();
    genesis.lineages.clear();
    genesis.schemas.retain(|schema| schema.id == base.version_id());
    genesis.current_write_schema = CurrentWriteSchema { revision: 0, schema: base.version_id() };
    receiver.apply_trusted_catalogue_snapshot_settled(genesis).unwrap();
    let runtime_before_transition = receiver.groove_runtime_token();

    receiver
        .apply_trusted_catalogue_snapshot_settled(snapshot.clone())
        .unwrap();
    assert_eq!(receiver.active_catalogue_seq(), 1);
    assert_eq!(
        receiver.groove_runtime_token(),
        runtime_before_transition,
        "a new historical lineage does not alter the active local runtime layout"
    );

    let runtime_before_idempotent_replay = receiver.groove_runtime_token();
    receiver
        .apply_trusted_catalogue_snapshot_settled(snapshot.clone())
        .unwrap();
    assert_eq!(receiver.groove_runtime_token(), runtime_before_idempotent_replay);
    drop(receiver);

    let mut reopened = reopen_node_at(&dir, node(0x3f), base);
    assert_eq!(reopened.active_catalogue_seq(), 1);
    let runtime_before_reopen_replay = reopened.groove_runtime_token();
    reopened.apply_trusted_catalogue_snapshot_settled(snapshot).unwrap();
    assert_eq!(reopened.active_catalogue_seq(), 1);
    assert_eq!(reopened.groove_runtime_token(), runtime_before_reopen_replay);
}

fn write_catalogue_record(
    node: &mut NodeState<RocksDbStorage>,
    kind: &[u8],
    id: uuid::Uuid,
    payload: Vec<u8>,
) {
    let mut batch = node.database.open_batch();
    batch.update(
        "jazz_catalogue",
        vec![
            Value::U64(test_catalogue_kind(kind).key()),
            Value::Uuid(id),
            Value::Bytes(payload),
        ],
    );
    let applied = crate::db::block_on(node.database.apply_batch(batch)).unwrap();
let persisted = crate::db::block_on(applied.persist());
node.database.finish_persistence(persisted).unwrap();
}

/// This is intentionally an internal storage-boundary test: only a direct
/// durable-row mutation can prove malformed kernel bytes fail before open
/// returns a resident `NodeState`.
#[test]
fn catalogue_kernel_payload_corruption_rejects_reopen_before_resident_mutation() {
    let base = schema();
    let (dir, mut node_state) = open_node_with_schema(node(0xa5), base.clone());
    let durable_schema = SchemaVersion::new(base.clone());
    let mut payload = codec::encode_catalogue_schema(&durable_schema).unwrap();
    payload.push(0);
    write_catalogue_record(
        &mut node_state,
        b"schema",
        durable_schema.id.0,
        payload,
    );
    drop(node_state);

    let cfs = base.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = RocksDbStorage::open(dir.path(), &refs).unwrap();
    assert!(matches!(
        crate::db::block_on(NodeState::new(node(0xa5), base, storage)),
        Err(Error::InvalidStoredValue("invalid catalogue schema payload"))
    ));
}

/// The public-schema JSON body is a canonical byte string within the typed
/// schema envelope. A semantically equivalent spelling must not become a
/// second durable representation of the same schema.
#[test]
fn noncanonical_catalogue_public_schema_rejects_reopen_before_resident_mutation() {
    let base = schema();
    let (dir, mut node_state) = open_node_with_schema(node(0xa6), base.clone());
    let durable_schema = SchemaVersion::new(base.clone());
    let mut payload = codec::encode_catalogue_schema(&durable_schema).unwrap();
    let length = u32::from_le_bytes(payload[17..21].try_into().unwrap());
    payload[17..21].copy_from_slice(&(length + 1).to_le_bytes());
    payload.insert(21, b' ');
    write_catalogue_record(
        &mut node_state,
        b"schema",
        durable_schema.id.0,
        payload,
    );
    drop(node_state);

    let cfs = base.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = RocksDbStorage::open(dir.path(), &refs).unwrap();
    assert!(matches!(
        crate::db::block_on(NodeState::new(node(0xa6), base, storage)),
        Err(Error::InvalidStoredValue(
            "non-canonical catalogue schema public schema"
        ))
    ));
}

#[test]
fn pending_catalogue_write_pointer_reopen_requires_deterministic_row_id() {
    let base = schema();
    let (dir, mut node_state) = open_node_with_schema(node(0xa7), base.clone());
    let pointer = CurrentWriteSchema {
        revision: 9,
        schema: base.version_id(),
    };
    write_catalogue_record(
        &mut node_state,
        b"write_pointer_pending",
        uuid::Uuid::from_u128(0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa),
        codec::encode_catalogue_write_pointer(pointer),
    );
    drop(node_state);

    let cfs = base.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = RocksDbStorage::open(dir.path(), &refs).unwrap();
    assert!(matches!(
        crate::db::block_on(NodeState::new(node(0xa7), base, storage)),
        Err(Error::InvalidStoredValue(
            "pending catalogue write-pointer id mismatch"
        ))
    ));
}

#[test]
fn pending_catalogue_write_pointer_reopen_rejects_duplicate_revision() {
    let base = schema();
    let (dir, mut node_state) = open_node_with_schema(node(0xa8), base.clone());
    let first = CurrentWriteSchema {
        revision: 9,
        schema: base.version_id(),
    };
    let second = CurrentWriteSchema {
        revision: 9,
        schema: SchemaVersionId(uuid::Uuid::from_u128(
            0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb,
        )),
    };
    for pointer in [first, second] {
        write_catalogue_record(
            &mut node_state,
            b"write_pointer_pending",
            codec::catalogue_write_pointer_id(pointer),
            codec::encode_catalogue_write_pointer(pointer),
        );
    }
    drop(node_state);

    let cfs = base.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = RocksDbStorage::open(dir.path(), &refs).unwrap();
    assert!(matches!(
        crate::db::block_on(NodeState::new(node(0xa8), base, storage)),
        Err(Error::InvalidStoredValue(
            "duplicate pending catalogue write-pointer revision"
        ))
    ));
}

fn delete_catalogue_record(node: &mut NodeState<RocksDbStorage>, kind: &[u8], id: uuid::Uuid) {
    let mut batch = node.database.open_batch();
    batch.delete(
        "jazz_catalogue",
        groove::db::PrimaryKeyValue::Composite(vec![
            groove::db::PrimaryKeyValue::U64(test_catalogue_kind(kind).key()),
            groove::db::PrimaryKeyValue::Uuid(id),
        ]),
    );
    let applied = crate::db::block_on(node.database.apply_batch(batch)).unwrap();
let persisted = crate::db::block_on(applied.persist());
node.database.finish_persistence(persisted).unwrap();
}

fn test_catalogue_kind(kind: &[u8]) -> crate::node::codec::CatalogueRecordKind {
    use crate::node::codec::CatalogueRecordKind;
    match kind {
        b"genesis" => CatalogueRecordKind::Genesis,
        b"schema" => CatalogueRecordKind::Schema,
        b"lens" => CatalogueRecordKind::Lens,
        b"schema_lineage_staged" => CatalogueRecordKind::SchemaLineageStaged,
        b"schema_lineage_pending" => CatalogueRecordKind::SchemaLineagePending,
        b"schema_lineage_active" => CatalogueRecordKind::SchemaLineageActive,
        b"write_pointer_pending" => CatalogueRecordKind::WritePointerPending,
        b"bootstrap_ready" => CatalogueRecordKind::BootstrapReady,
        b"active_schema" => CatalogueRecordKind::ActiveSchema,
        _ => panic!("unknown test catalogue kind: {kind:?}"),
    }
}

fn write_raw_catalogue_kind(
    node: &mut NodeState<RocksDbStorage>,
    kind: u64,
    id: uuid::Uuid,
) {
    let mut batch = node.database.open_batch();
    batch.update(
        "jazz_catalogue",
        vec![Value::U64(kind), Value::Uuid(id), Value::Bytes(Vec::new())],
    );
    let applied = crate::db::block_on(node.database.apply_batch(batch)).unwrap();
    let persisted = crate::db::block_on(applied.persist());
    node.database.finish_persistence(persisted).unwrap();
}

/// The epoch-pinned kernel is closed.  An unrecognized record kind must not
/// be ignored as a future extension or decoded under a current descriptor.
#[test]
fn dynamic_catalogue_reopen_fails_closed_on_unknown_catalogue_kernel_kind() {
    let empty_schema = empty_public_test_schema();
    let temp_dir = tempfile::tempdir().expect("create receiver store");
    let cfs = empty_schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = RocksDbStorage::open(temp_dir.path(), &refs).expect("open empty receiver store");
    let mut receiver = NodeState::new_catalogue_uninitialized(node(0xa0), storage)
        .expect("open explicit uninitialized receiver");
    write_raw_catalogue_kind(&mut receiver, 0xff, uuid::Uuid::from_bytes([0xa0; 16]));
    drop(receiver);

    for attempt in 0..2 {
        assert!(fresh_dynamic_catalogue_open(temp_dir.path(), node(0xa0)).is_err(),
            "open attempt {attempt} must reject an unknown catalogue kernel kind");
    }
}

/// This is an internal storage-boundary receipt: the public API cannot expose
/// the epoch bootstrap row directly.  Pin every byte identity here so a future
/// Rust enum reorder cannot silently reinterpret an existing catalogue.
#[test]
fn catalogue_kernel_kind_fixture_is_exact_and_closed() {
    use crate::node::codec::CatalogueRecordKind;

    let fixture = [
        (CatalogueRecordKind::Genesis, 0),
        (CatalogueRecordKind::Schema, 1),
        (CatalogueRecordKind::Lens, 2),
        (CatalogueRecordKind::SchemaLineageStaged, 3),
        (CatalogueRecordKind::SchemaLineagePending, 4),
        (CatalogueRecordKind::SchemaLineageActive, 5),
        (CatalogueRecordKind::WritePointerPending, 6),
        (CatalogueRecordKind::BootstrapReady, 7),
        (CatalogueRecordKind::ActiveSchema, 8),
    ];

    for (kind, bytes) in fixture {
        assert_eq!(kind.key(), bytes, "epoch-pinned kind fixture changed");
        assert_eq!(CatalogueRecordKind::from_key(bytes).unwrap(), kind);
    }
    assert!(CatalogueRecordKind::from_key(9).is_err());
    assert!(CatalogueRecordKind::from_key(u64::MAX).is_err());
}

fn delete_catalogue_pointer(node: &mut NodeState<RocksDbStorage>, revision: u64) {
    let mut batch = node.database.open_batch();
    batch.delete(
        "jazz_catalogue_pointer",
        groove::db::PrimaryKeyValue::U64(revision),
    );
    let applied = crate::db::block_on(node.database.apply_batch(batch)).unwrap();
let persisted = crate::db::block_on(applied.persist());
node.database.finish_persistence(persisted).unwrap();
}

fn write_schema_mapping_record(
    node: &mut NodeState<RocksDbStorage>,
    alias: SchemaVersionAlias,
    schema: SchemaVersionId,
    mapping: &SchemaPhysicalMapping,
) {
    let mut batch = node.database.open_batch();
    NodeState::<RocksDbStorage>::write_schema_version_mapping_to_batch(
        &mut batch, alias, schema, mapping,
    )
    .unwrap();
    let applied = crate::db::block_on(node.database.apply_batch(batch)).unwrap();
let persisted = crate::db::block_on(applied.persist());
node.database.finish_persistence(persisted).unwrap();
}

fn delete_schema_mapping_record(node: &mut NodeState<RocksDbStorage>, alias: SchemaVersionAlias) {
    let mut batch = node.database.open_batch();
    batch.delete(
        "jazz_schema_versions",
        groove::db::PrimaryKeyValue::U64(alias.0),
    );
    let applied = crate::db::block_on(node.database.apply_batch(batch)).unwrap();
let persisted = crate::db::block_on(applied.persist());
node.database.finish_persistence(persisted).unwrap();
}

fn fresh_dynamic_catalogue_open(
    path: &std::path::Path,
    node_uuid: NodeUuid,
) -> Result<NodeState<RocksDbStorage>, Error> {
    let empty_schema = empty_public_test_schema();
    let cfs = empty_schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = RocksDbStorage::open(path, &refs)?;
    NodeState::new_catalogue_uninitialized(node_uuid, storage).resolve()
}

fn write_active_lineage_record(node: &mut NodeState<RocksDbStorage>, staged: &StagedSchemaLineage) {
    write_catalogue_record(
        node,
        b"schema_lineage_staged",
        staged.publication.id.0,
        codec::encode_catalogue_staged_lineage(staged).unwrap(),
    );
    write_catalogue_record(
        node,
        b"schema_lineage_active",
        staged.publication.id.0,
        codec::encode_catalogue_lineage_activation(SchemaLineageActivation {
            id: staged.publication.id,
            catalogue_seq: staged.catalogue_seq,
        }),
    );
}

fn duplicate_schema_destination_lineage(
    base: &JazzSchema,
    source_identities: &PhysicalIdentityManifest,
    original: &StagedSchemaLineage,
    catalogue_seq: u64,
) -> StagedSchemaLineage {
    let duplicate_publication = SchemaLineagePublication::author_from_prior(
        base,
        source_identities,
        original.publication.schema.clone(),
        MigrationLens::new(
            base.version_id(),
            original.publication.schema.id,
            vec![TableLens {
                source_table: "todos".to_owned(),
                target_table: "todos".to_owned(),
                ops: vec![LensOp::AddColumn {
                    column: "body".to_owned(),
                    default: Value::String("different-default".to_owned()),
                }],
            }],
        ).expect("valid migration lens"),
        Vec::<String>::new(),
        Vec::<String>::new(),
    )
    .expect("corruption fixture authors a valid competing descendant");
    assert_ne!(duplicate_publication.id, original.publication.id);
    StagedSchemaLineage {
        catalogue_seq,
        publication: duplicate_publication,
        alias: original.alias,
        mapping: original.mapping.clone(),
    }
}

fn assert_staged_corruption_rejected(
    byte: u8,
    expected: &'static str,
    mutate: impl FnOnce(&mut StagedSchemaLineage),
) {
    let base = schema();
    let snapshot = catalogue_snapshot_fixture();
    let (dir, mut receiver) = open_node_with_schema(node(byte), base.clone());
    receiver.apply_trusted_catalogue_snapshot_settled(snapshot).unwrap();
    let mut staged = receiver
        .catalogue
        .active_lineages_by_target
        .values()
        .next()
        .unwrap()
        .clone();
    let original_id = staged.publication.id;
    mutate(&mut staged);
    delete_catalogue_record(
        &mut receiver,
        b"schema_lineage_active",
        original_id.0,
    );
    delete_catalogue_record(
        &mut receiver,
        b"schema_lineage_staged",
        original_id.0,
    );
    write_catalogue_record(
        &mut receiver,
        b"schema_lineage_staged",
        staged.publication.id.0,
        codec::encode_catalogue_staged_lineage(&staged).unwrap(),
    );
    drop(receiver);

    assert_catalogue_reopen_rejected(&dir, node(byte), base, expected);
}

fn assert_catalogue_reopen_rejected(
    dir: &tempfile::TempDir,
    node_uuid: NodeUuid,
    schema: JazzSchema,
    expected: &'static str,
) {
    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = RocksDbStorage::open(dir.path(), &refs).unwrap();
    let reopened = NodeState::new(node_uuid, schema, storage).resolve();
    assert!(matches!(
        reopened,
        Err(Error::InvalidStoredValue(message)) if message == expected
    ));
}

#[test]
fn reopen_rejects_active_catalogue_marker_without_canonical_payload() {
    let base = schema();
    let snapshot = catalogue_snapshot_fixture();
    let (dir, mut receiver) = open_node_with_schema(node(0x40), base.clone());
    receiver.apply_trusted_catalogue_snapshot_settled(snapshot).unwrap();
    let publication_id = receiver
        .catalogue
        .active_lineages_by_target
        .values()
        .next()
        .unwrap()
        .publication
        .id;
    delete_catalogue_record(
        &mut receiver,
        b"schema_lineage_staged",
        publication_id.0,
    );
    drop(receiver);

    assert_catalogue_reopen_rejected(
        &dir,
        node(0x40),
        base,
        "active schema lineage is missing canonical payload",
    );
}

#[test]
fn reopen_rejects_active_catalogue_marker_with_mismatched_payload_sequence() {
    let base = schema();
    let snapshot = catalogue_snapshot_fixture();
    let (dir, mut receiver) = open_node_with_schema(node(0x41), base.clone());
    receiver.apply_trusted_catalogue_snapshot_settled(snapshot).unwrap();
    let mut staged = receiver
        .catalogue
        .active_lineages_by_target
        .values()
        .next()
        .unwrap()
        .clone();
    staged.catalogue_seq = 2;
    write_catalogue_record(
        &mut receiver,
        b"schema_lineage_staged",
        staged.publication.id.0,
        codec::encode_catalogue_staged_lineage(&staged).unwrap(),
    );
    drop(receiver);

    assert_catalogue_reopen_rejected(
        &dir,
        node(0x41),
        base,
        "active schema lineage payload conflicts with marker",
    );
}

#[test]
fn reopen_rejects_gapped_active_catalogue_sequences() {
    let base = schema();
    let snapshot = catalogue_snapshot_fixture();
    let (dir, mut receiver) = open_node_with_schema(node(0x42), base.clone());
    receiver.apply_trusted_catalogue_snapshot_settled(snapshot).unwrap();

    let v2 = SchemaVersion::new(catalogue_evolved_schema());
    let v3 = SchemaVersion::new(build_public_test_schema(
        PublicSchemaBuilder::new().table(
            PublicTableSchemaBuilder::new("todos")
                .column("title", PublicColumnType::Text)
                .column("body", PublicColumnType::Text)
                .column("archived", PublicColumnType::Boolean),
        ),
    ));
    publish_schema_lineage(
        &mut receiver,
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
        ).expect("valid migration lens"),
        Vec::<String>::new(),
        Vec::<String>::new(),
    )
    .unwrap();
    let mut staged = receiver.catalogue.active_lineages_by_target[&v3.id].clone();
    staged.catalogue_seq = 3;
    write_active_lineage_record(&mut receiver, &staged);
    drop(receiver);

    assert_catalogue_reopen_rejected(
        &dir,
        node(0x42),
        base,
        "active catalogue sequences are not contiguous",
    );
}

#[test]
fn reopen_rejects_duplicate_active_catalogue_targets() {
    let base = schema();
    let snapshot = catalogue_snapshot_fixture();
    let (dir, mut receiver) = open_node_with_schema(node(0x43), base.clone());
    receiver.apply_trusted_catalogue_snapshot_settled(snapshot).unwrap();
    let original = receiver
        .catalogue
        .active_lineages_by_target
        .values()
        .next()
        .unwrap()
        .clone();
    let duplicate = duplicate_schema_destination_lineage(
        &base,
        &receiver.catalogue.physical_mappings[&base.version_id()].identities,
        &original,
        2,
    );
    write_active_lineage_record(&mut receiver, &duplicate);
    drop(receiver);

    assert_catalogue_reopen_rejected(
        &dir,
        node(0x43),
        base,
        "duplicate durable schema lineage target",
    );
}

#[test]
fn reopen_rejects_inactive_catalogue_target_already_active() {
    let base = schema();
    let snapshot = catalogue_snapshot_fixture();
    let (dir, mut receiver) = open_node_with_schema(node(0x44), base.clone());
    receiver.apply_trusted_catalogue_snapshot_settled(snapshot).unwrap();
    let original = receiver
        .catalogue
        .active_lineages_by_target
        .values()
        .next()
        .unwrap()
        .clone();
    let duplicate = duplicate_schema_destination_lineage(
        &base,
        &receiver.catalogue.physical_mappings[&base.version_id()].identities,
        &original,
        2,
    );
    write_catalogue_record(
        &mut receiver,
        b"schema_lineage_staged",
        duplicate.publication.id.0,
        codec::encode_catalogue_staged_lineage(&duplicate).unwrap(),
    );
    drop(receiver);

    assert_catalogue_reopen_rejected(
        &dir,
        node(0x44),
        base,
        "duplicate durable schema lineage target",
    );
}

#[test]
fn reopen_rejects_duplicate_inactive_catalogue_targets() {
    let base = schema();
    let snapshot = catalogue_snapshot_fixture();
    let (dir, mut receiver) = open_node_with_schema(node(0x45), base.clone());
    receiver.apply_trusted_catalogue_snapshot_settled(snapshot).unwrap();
    let mut first = receiver
        .catalogue
        .active_lineages_by_target
        .values()
        .next()
        .unwrap()
        .clone();
    first.catalogue_seq = 1;
    let duplicate = duplicate_schema_destination_lineage(
        &base,
        &receiver.catalogue.physical_mappings[&base.version_id()].identities,
        &first,
        2,
    );
    delete_catalogue_record(
        &mut receiver,
        b"schema_lineage_active",
        first.publication.id.0,
    );
    write_catalogue_record(
        &mut receiver,
        b"schema_lineage_staged",
        first.publication.id.0,
        codec::encode_catalogue_staged_lineage(&first).unwrap(),
    );
    write_catalogue_record(
        &mut receiver,
        b"schema_lineage_staged",
        duplicate.publication.id.0,
        codec::encode_catalogue_staged_lineage(&duplicate).unwrap(),
    );
    drop(receiver);

    assert_catalogue_reopen_rejected(
        &dir,
        node(0x45),
        base,
        "duplicate durable schema lineage target",
    );
}

#[test]
fn reopen_rejects_zero_sequence_staged_lineage() {
    let base = schema();
    let snapshot = catalogue_snapshot_fixture();
    let (dir, mut receiver) = open_node_with_schema(node(0x46), base.clone());
    receiver.apply_trusted_catalogue_snapshot_settled(snapshot).unwrap();
    let mut staged = receiver
        .catalogue
        .active_lineages_by_target
        .values()
        .next()
        .unwrap()
        .clone();
    staged.catalogue_seq = 0;
    write_catalogue_record(
        &mut receiver,
        b"schema_lineage_staged",
        staged.publication.id.0,
        codec::encode_catalogue_staged_lineage(&staged).unwrap(),
    );
    drop(receiver);

    assert_catalogue_reopen_rejected(
        &dir,
        node(0x46),
        base,
        "staged schema lineage sequence must be nonzero",
    );
}

#[test]
fn reopen_rejects_staged_schema_payload_identity_mismatch() {
    let base_id = schema().version_id();
    assert_staged_corruption_rejected(
        0x47,
        "catalogue schema content id mismatch",
        |staged| {
            staged.publication.schema.id = base_id;
            staged.publication.id = staged.publication.content_id();
        },
    );
}

#[test]
fn reopen_derives_staged_lens_identity_from_canonical_payload() {
    let base = schema();
    let snapshot = catalogue_snapshot_fixture();
    let (dir, mut receiver) = open_node_with_schema(node(0x48), base.clone());
    receiver.apply_trusted_catalogue_snapshot_settled(snapshot).unwrap();
    let mut staged = receiver
        .catalogue
        .active_lineages_by_target
        .values()
        .next()
        .unwrap()
        .clone();
    let expected_lens_id = staged.publication.lens.content_id();
    // `MigrationLens::id` is derived, never serialized: stale in-memory
    // bookkeeping cannot become a distinct durable identity.
    staged.publication.lens.id = MigrationLensId(uuid::Uuid::nil());
    write_catalogue_record(
        &mut receiver,
        b"schema_lineage_staged",
        staged.publication.id.0,
        codec::encode_catalogue_staged_lineage(&staged).unwrap(),
    );
    drop(receiver);

    let reopened = reopen_node_at(&dir, node(0x48), base);
    let recovered = reopened
        .catalogue
        .active_lineages_by_target
        .values()
        .next()
        .expect("recovered staged lineage");
    assert_eq!(recovered.publication.lens.id(), expected_lens_id);
    assert_eq!(
        recovered.publication.lens.id(),
        recovered.publication.lens.content_id()
    );
}

#[test]
fn reopen_rejects_staged_lens_target_mismatch() {
    let base_id = schema().version_id();
    assert_staged_corruption_rejected(
        0x49,
        "staged schema lineage violates trusted publication invariants",
        |staged| {
            staged.publication.lens.target = base_id;
            staged.publication.lens.id = staged.publication.lens.content_id();
            staged.publication.id = staged.publication.content_id();
        },
    );
}

#[test]
fn reopen_rejects_staged_lens_operation_mismatch() {
    assert_staged_corruption_rejected(0x4a, "staged schema lineage lens is invalid", |staged| {
        staged.publication.lens.table_lenses[0].ops.clear();
        staged.publication.lens.id = staged.publication.lens.content_id();
        staged.publication.id = staged.publication.content_id();
    });
}

#[test]
fn reopen_rejects_staged_table_partition_mismatch() {
    assert_staged_corruption_rejected(
        0x4b,
        "staged schema lineage table partition is invalid",
        |staged| {
            staged.publication.new_tables.push("todos".to_owned());
            staged.publication.id = staged.publication.content_id();
        },
    );
}

/// Local integer aliases are intentionally node-owned, but the UUID manifest
/// carried alongside them is authority-owned.  This planted reopen mutation
/// replaces the descendant's manifest with a separately valid one: recovery
/// must bind it back to the exact lineage publication rather than accepting
/// two individually well-formed durable records.
#[test]
fn reopen_rejects_mapping_manifest_smuggled_under_active_lineage() {
    let snapshot = catalogue_snapshot_fixture();
    let dir = tempfile::tempdir().expect("create dynamic-catalogue node store");
    let mut receiver = fresh_dynamic_catalogue_open(dir.path(), node(0x4e))
        .expect("open uninitialized receiver");
    receiver.apply_trusted_catalogue_snapshot_settled(snapshot).unwrap();
    let active = receiver
        .catalogue
        .active_lineages_by_target
        .values()
        .next()
        .expect("fixture activates one descendant")
        .clone();
    let mut smuggled = receiver.catalogue.physical_mappings[&active.publication.schema.id].clone();
    smuggled.identities = PhysicalIdentityManifest::allocate(&active.publication.schema.schema);
    write_schema_mapping_record(
        &mut receiver,
        active.alias,
        active.publication.schema.id,
        &smuggled,
    );
    drop(receiver);

    let empty = empty_public_test_schema();
    let cfs = empty.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = RocksDbStorage::open(dir.path(), &refs).unwrap();
    assert!(matches!(
        NodeState::new_catalogue_uninitialized(node(0x4e), storage).resolve(),
        Err(Error::InvalidStoredValue("durable mapping identities disagree with authority publication"))
    ));
}

/// A schema row is independently durable from its activation receipt. Reopen
/// must recompute its content-derived ID before putting it in the resident
/// catalogue; matching the row primary key alone is not sufficient.
#[test]
fn reopen_rejects_standalone_schema_content_identity_mismatch() {
    let base = schema();
    let (dir, mut receiver) = open_node_with_schema(node(0x4c), base.clone());
    let mut tampered = SchemaVersion::new(base.clone());
    tampered.id = SchemaVersionId(uuid::Uuid::nil());
    write_catalogue_record(
        &mut receiver,
        b"schema",
        tampered.id.0,
        codec::encode_catalogue_schema(&tampered).unwrap(),
    );
    drop(receiver);

    assert_catalogue_reopen_rejected(
        &dir,
        node(0x4c),
        base,
        "catalogue schema content id mismatch",
    );
}

/// A standalone cross-lens is not covered by a lineage receipt. This planted
/// durable mutation keeps its key and content ID coherent while removing the
/// operation that makes the endpoints semantically compatible.
#[test]
fn reopen_rejects_standalone_lens_semantic_tamper() {
    let base = schema();
    let snapshot = catalogue_snapshot_fixture();
    let (dir, mut receiver) = open_node_with_schema(node(0x4d), base.clone());
    receiver.apply_trusted_catalogue_snapshot_settled(snapshot).unwrap();
    let mut tampered = receiver
        .catalogue
        .active_lineages_by_target
        .values()
        .next()
        .unwrap()
        .publication
        .lens
        .clone();
    tampered.table_lenses[0].ops.clear();
    tampered.id = tampered.content_id();
    write_catalogue_record(
        &mut receiver,
        b"lens",
        tampered.id.0,
        codec::encode_catalogue_lens(&tampered),
    );
    drop(receiver);

    assert_catalogue_reopen_rejected(
        &dir,
        node(0x4d),
        base,
        "catalogue lens violates trusted semantic invariants",
    );
}

/// A dynamic-catalogue node without a local catalogue must not manufacture the empty
/// constructor schema as durable genesis; after its trusted core snapshot it
/// atomically adopts the core lineage and survives reopen.
///
/// ```text
/// core catalogue snapshot ──trusted install──► receiver(Uninitialized -> Ready)
///                                                            │
///                                                            └──reopen──► exact core genesis
/// ```
#[test]
fn dynamic_catalogue_bootstrap_adopts_authority_genesis_atomically_and_reopens_ready() {
    let empty_schema = empty_public_test_schema();
    let temp_dir = tempfile::tempdir().expect("create receiver store");
    let cfs = empty_schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = RocksDbStorage::open(temp_dir.path(), &refs).expect("open empty receiver store");
    let mut receiver = NodeState::new_catalogue_uninitialized(node(0x91), storage)
        .expect("open explicit uninitialized receiver");

    assert_eq!(
        receiver.catalogue_bootstrap_state(),
        CatalogueBootstrapState::Uninitialized
    );
    assert!(matches!(
        receiver.try_current_write_schema(),
        Err(Error::CatalogueUninitialized)
    ));
    assert!(matches!(
        receiver.current_write_schema(),
        Err(Error::CatalogueUninitialized)
    ));
    assert!(matches!(
        receiver.try_current_schema(),
        Err(Error::CatalogueUninitialized)
    ));
    assert!(matches!(
        receiver.catalogue_snapshot(),
        Err(Error::CatalogueUninitialized)
    ));
    assert!(
        receiver.database
            .primary_key_scan_raw("jazz_catalogue", &[])
            .expect("scan empty durable catalogue")
            .is_empty(),
        "uninitialized receiver must not persist an empty-schema genesis marker"
    );
    assert!(
        receiver.database
            .primary_key_scan_raw("jazz_schema_versions", &[])
            .expect("scan empty durable physical mappings")
            .is_empty(),
        "uninitialized receiver must not persist a provisional physical mapping"
    );
    assert!(matches!(
        receiver.current_rows("todos", DurabilityTier::Local).resolve(),
        Err(Error::CatalogueUninitialized)
    ));
    assert!(matches!(
        receiver.commit_mergeable_settled(MergeableCommit::new("todos", row(0x92), 1).cells(BTreeMap::from([
            ("title".to_owned(), v("must not write before catalogue bootstrap")),
        ]))),
        Err(Error::CatalogueUninitialized)
    ));

    let snapshot = catalogue_snapshot_fixture();
    let authority_genesis = schema().version_id();
    receiver.apply_trusted_catalogue_snapshot_settled(snapshot.clone())
        .expect("install exact trusted core catalogue");
    assert_eq!(receiver.catalogue_bootstrap_state(), CatalogueBootstrapState::Ready);
    assert_eq!(receiver.catalogue.local_schema_version_id, authority_genesis);
    assert_eq!(receiver.catalogue.schema, schema());
    assert_eq!(receiver.current_write_schema().unwrap(), snapshot.current_write_schema);
    assert_eq!(receiver.active_catalogue_seq(), 1);
    assert_eq!(receiver.catalogue_schemas().len(), 2);
    drop(receiver);
    let cfs = empty_schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = RocksDbStorage::open(temp_dir.path(), &refs).expect("reopen receiver store");
    let reopened = NodeState::new_catalogue_uninitialized(node(0x91), storage)
        .expect("fresh process discovers durable authority genesis");
    assert_eq!(reopened.catalogue_bootstrap_state(), CatalogueBootstrapState::Ready);
    assert_eq!(
        reopened.catalogue.local_schema_version_id,
        authority_genesis,
        "reopen must use the authority genesis, never the empty temporary schema"
    );
    assert_eq!(
        reopened.current_write_schema().unwrap(),
        snapshot.current_write_schema
    );
    assert_eq!(reopened.active_catalogue_seq(), 1);
    assert_eq!(reopened.catalogue_schemas().len(), 2);
}

/// A failed first trusted snapshot leaves a dynamic-catalogue node uninitialized, so a
/// later reopen cannot observe a partially installed genesis or pointer.
///
/// ```text
/// core snapshot ──durable failpoint──► receiver(Uninitialized) ──reopen──► Uninitialized
/// ```
#[test]
fn dynamic_catalogue_bootstrap_failure_never_persists_a_partial_authority_catalogue() {
    let empty_schema = empty_public_test_schema();
    let temp_dir = tempfile::tempdir().expect("create receiver store");
    let cfs = empty_schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = RocksDbStorage::open(temp_dir.path(), &refs).expect("open empty receiver store");
    let mut receiver = NodeState::new_catalogue_uninitialized(node(0x93), storage)
        .expect("open explicit uninitialized receiver");
    receiver.set_catalogue_activation_failpoint(
        CatalogueActivationFailpoint::BeforeSnapshotActivationCommit,
    );

    assert!(matches!(
        receiver.apply_trusted_catalogue_snapshot_settled(catalogue_snapshot_fixture()),
        Err(Error::CatalogueActivationFailed)
    ));
    assert_eq!(
        receiver.catalogue_bootstrap_state(),
        CatalogueBootstrapState::Uninitialized
    );
    assert!(matches!(
        receiver.try_current_write_schema(),
        Err(Error::CatalogueUninitialized)
    ));
    assert!(
        receiver.database
            .primary_key_scan_raw("jazz_catalogue", &[])
            .expect("scan failed bootstrap catalogue")
            .is_empty(),
        "failed bootstrap must not leave a genesis, pointer, or lineage prefix"
    );
    assert!(
        receiver.database
            .primary_key_scan_raw("jazz_schema_versions", &[])
            .expect("scan failed bootstrap mappings")
            .is_empty(),
        "failed bootstrap must not leave a physical mapping prefix"
    );

    drop(receiver);
    let cfs = empty_schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = RocksDbStorage::open(temp_dir.path(), &refs).expect("reopen empty receiver store");
    let reopened = NodeState::new_catalogue_uninitialized(node(0x93), storage)
        .expect("fresh process retains no failed bootstrap state");
    assert_eq!(
        reopened.catalogue_bootstrap_state(),
        CatalogueBootstrapState::Uninitialized
    );
    assert!(matches!(
        reopened.try_current_write_schema(),
        Err(Error::CatalogueUninitialized)
    ));
}

/// A fresh dynamic-catalogue open treats every durable catalogue row as a completed
/// bootstrap only when its atomic completion record is present.  A raw
/// genesis/schema prefix is corrupt, not an invitation to repair it using an
/// empty local schema.
#[test]
fn dynamic_catalogue_reopen_rejects_catalogue_prefix_without_bootstrap_marker() {
    let empty_schema = empty_public_test_schema();
    let temp_dir = tempfile::tempdir().expect("create receiver store");
    let cfs = empty_schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = RocksDbStorage::open(temp_dir.path(), &refs).expect("open empty receiver store");
    let mut receiver = NodeState::new_catalogue_uninitialized(node(0x9a), storage)
        .expect("open explicit uninitialized receiver");
    let snapshot = catalogue_snapshot_fixture();
    receiver.apply_trusted_catalogue_snapshot_settled(snapshot).unwrap();
    delete_catalogue_record(
        &mut receiver,
        b"bootstrap_ready",
        schema().version_id().0,
    );
    drop(receiver);

    for attempt in 0..2 {
        assert!(matches!(
            fresh_dynamic_catalogue_open(temp_dir.path(), node(0x9a)),
            Err(Error::InvalidStoredValue(
                "dynamic catalogue state has no bootstrap completion marker"
            ))
        ), "open attempt {attempt} must reject rather than repair the prefix");
    }
}

/// Removing a normal node's catalogue cannot turn its remaining transaction
/// history into a blank dynamic-catalogue node.  Discovery must fail before an
/// uninitialized constructor can adopt a new authority over stale data.
#[test]
fn dynamic_catalogue_reopen_rejects_catalogue_stripped_history() {
    let base = schema();
    let (temp_dir, mut durable_node) = open_node_with_schema(node(0x9e), base.clone());
    durable_node.commit_mergeable_settled(
        MergeableCommit::new("todos", row(0x9f), 10).cells(title_cells("durable history")),
    )
    .unwrap();
    let alias = durable_node.catalogue.local_schema_version_alias.unwrap();
    delete_catalogue_record(&mut durable_node, b"genesis", base.version_id().0);
    delete_catalogue_record(&mut durable_node, b"schema", base.version_id().0);
    delete_schema_mapping_record(&mut durable_node, alias);
    drop(durable_node);

    for attempt in 0..2 {
        assert!(matches!(
            fresh_dynamic_catalogue_open(temp_dir.path(), node(0x9e)),
            Err(Error::InvalidStoredValue(
                "dynamic catalogue state cannot initialize over durable history"
            ))
        ), "open attempt {attempt} must reject rather than adopt over history");
    }
}

/// The completion record joins the exact write pointer and active lineage
/// high-water.  Removing either side, or changing the receipt, must reject a
/// fresh recovery before normal catalogue open can repair missing metadata.
#[test]
fn dynamic_catalogue_reopen_rejects_truncated_or_mismatched_bootstrap_marker() {
    let empty_schema = empty_public_test_schema();
    let temp_dir = tempfile::tempdir().expect("create receiver store");
    let cfs = empty_schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = RocksDbStorage::open(temp_dir.path(), &refs).expect("open empty receiver store");
    let mut receiver = NodeState::new_catalogue_uninitialized(node(0x9b), storage)
        .expect("open explicit uninitialized receiver");
    let snapshot = catalogue_snapshot_fixture();
    receiver.apply_trusted_catalogue_snapshot_settled(snapshot.clone()).unwrap();
    delete_catalogue_pointer(&mut receiver, snapshot.current_write_schema.revision);
    drop(receiver);

    assert!(matches!(
        fresh_dynamic_catalogue_open(temp_dir.path(), node(0x9b)),
        Err(Error::InvalidStoredValue(
            "catalogue bootstrap completion marker does not match durable catalogue"
        ))
    ));

    let temp_dir = tempfile::tempdir().expect("create second receiver store");
    let cfs = empty_schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = RocksDbStorage::open(temp_dir.path(), &refs).expect("open second receiver store");
    let mut receiver = NodeState::new_catalogue_uninitialized(node(0x9c), storage)
        .expect("open explicit uninitialized receiver");
    receiver.apply_trusted_catalogue_snapshot_settled(snapshot.clone()).unwrap();
    write_catalogue_record(
        &mut receiver,
        b"bootstrap_ready",
        schema().version_id().0,
        codec::encode_catalogue_bootstrap_ready(&CatalogueBootstrapReady {
            genesis: schema().version_id(),
            current_write_schema: snapshot.current_write_schema,
            active_catalogue_seq: 0,
        }),
    );
    drop(receiver);

    assert!(matches!(
        fresh_dynamic_catalogue_open(temp_dir.path(), node(0x9c)),
        Err(Error::InvalidStoredValue(
            "catalogue bootstrap completion marker does not match durable catalogue"
        ))
    ));
}

/// The bootstrap receipt does not bless arbitrary catalogue rows.  Every
/// durable schema and mapping must be the genesis or the target of a canonical
/// staged lineage payload; a raw-added standalone schema remains corrupt even
/// when it carries an otherwise valid physical mapping.
#[test]
fn dynamic_catalogue_reopen_rejects_smuggled_schema_and_mapping() {
    let empty_schema = empty_public_test_schema();
    let temp_dir = tempfile::tempdir().expect("create receiver store");
    let cfs = empty_schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = RocksDbStorage::open(temp_dir.path(), &refs).expect("open empty receiver store");
    let mut receiver = NodeState::new_catalogue_uninitialized(node(0x9d), storage)
        .expect("open explicit uninitialized receiver");
    receiver.apply_trusted_catalogue_snapshot_settled(catalogue_snapshot_fixture())
        .unwrap();

    let smuggled = SchemaVersion::new(build_public_test_schema(
        PublicSchemaBuilder::new().table(
            PublicTableSchemaBuilder::new("unrelated")
                .column("title", PublicColumnType::Text),
        ),
    ));
    let mut next_table_id = 100;
    let mut next_column_id = 100;
    let mapping = allocate_provisional_physical_mapping(
        &smuggled.schema,
        PhysicalIdentityManifest::allocate(&smuggled.schema),
        &mut next_table_id,
        &mut next_column_id,
    )
    .unwrap();
    write_catalogue_record(
        &mut receiver,
        b"schema",
        smuggled.id.0,
        codec::encode_catalogue_schema(&smuggled).unwrap(),
    );
    write_schema_mapping_record(&mut receiver, SchemaVersionAlias(99), smuggled.id, &mapping);
    drop(receiver);

    for attempt in 0..2 {
        assert!(matches!(
            fresh_dynamic_catalogue_open(temp_dir.path(), node(0x9d)),
            Err(Error::InvalidStoredValue(
                "catalogue bootstrap completion marker does not match durable catalogue"
            ))
        ), "open attempt {attempt} must reject rather than repair smuggled state");
    }
}

/// A crash after canonical lineage staging but before activation leaves no
/// target schema or mapping.  A fresh dynamic-catalogue node must accept that exact
/// durable seam, drain it into one atomic activation, and refresh its
/// bootstrap receipt for the next process open.
#[test]
fn dynamic_catalogue_reopen_drains_after_staged_lineage_crash() {
    let base = schema();
    let empty_schema = empty_public_test_schema();
    let temp_dir = tempfile::tempdir().expect("create receiver store");
    let cfs = empty_schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = RocksDbStorage::open(temp_dir.path(), &refs).expect("open empty receiver store");
    let mut receiver = NodeState::new_catalogue_uninitialized(node(0xa1), storage)
        .expect("open explicit uninitialized receiver");
    receiver.apply_trusted_catalogue_snapshot_settled(crate::protocol::CatalogueSnapshot {
        genesis_physical_identities: PhysicalIdentityManifest::allocate(&base),
        schemas: vec![SchemaVersion::new(base.clone())],
        lineages: Vec::new(),
        current_write_schema: CurrentWriteSchema {
            revision: 0,
            schema: base.version_id(),
        },
    })
    .unwrap();

    let target = SchemaVersion::new(catalogue_evolved_schema());
    let publication = receiver.author_schema_lineage_publication(
        target.clone(),
        MigrationLens::new(
            base.version_id(),
            target.id,
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
    ).unwrap();
    receiver.set_catalogue_activation_failpoint(CatalogueActivationFailpoint::AfterStaged);
    assert!(matches!(
        receiver.apply_trusted_catalogue_message_settled(SyncMessage::PublishSchemaWithLens {
            author: AuthorSubject::SYSTEM,
            catalogue_seq: 1,
            publication: Box::new(publication),
        }),
        Err(Error::CatalogueActivationFailed)
    ));
    assert!(
        receiver.database
            .primary_key_scan_raw("jazz_schema_versions", &[])
            .unwrap()
            .iter()
            .all(|raw| raw.record().get_uuid(SchemaVersionAliasRowRecord::FIELD_UUID_IDX).unwrap()
                != target.id.0),
        "AfterStaged must not persist the inactive target mapping"
    );
    drop(receiver);

    let reopened = fresh_dynamic_catalogue_open(temp_dir.path(), node(0xa1))
        .expect("fresh discovery accepts canonical inactive staging");
    assert_eq!(reopened.active_catalogue_seq(), 1);
    assert!(reopened.catalogue_schemas().contains_key(&target.id));
    drop(reopened);

    let reopened = fresh_dynamic_catalogue_open(temp_dir.path(), node(0xa1))
        .expect("activation refreshes the dynamic bootstrap receipt");
    assert_eq!(reopened.active_catalogue_seq(), 1);
    assert!(reopened.catalogue_schemas().contains_key(&target.id));
}

/// A bootstrap snapshot has exactly one non-lineage schema: the authority's
/// genesis.  Mallory cannot make a receiver choose among multiple roots.
///
/// ```text
/// malformed snapshot(two roots) ──► receiver(Uninitialized) ──reject──► no durable state
/// ```
#[test]
fn dynamic_catalogue_bootstrap_rejects_snapshot_with_ambiguous_genesis() {
    let empty_schema = empty_public_test_schema();
    let temp_dir = tempfile::tempdir().expect("create receiver store");
    let cfs = empty_schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = RocksDbStorage::open(temp_dir.path(), &refs).expect("open empty receiver store");
    let mut receiver = NodeState::new_catalogue_uninitialized(node(0x94), storage)
        .expect("open explicit uninitialized receiver");
    let mut snapshot = catalogue_snapshot_fixture();
    snapshot
        .schemas
        .push(SchemaVersion::new(build_public_test_schema(
            PublicSchemaBuilder::new().table(
                PublicTableSchemaBuilder::new("other")
                    .column("title", PublicColumnType::Text),
            ),
        )));

    assert!(matches!(
        receiver.apply_trusted_catalogue_snapshot_settled(snapshot),
        Err(Error::InvalidCatalogueUpdate(
            "trusted catalogue snapshot must contain exactly one genesis schema"
        ))
    ));
    assert_eq!(
        receiver.catalogue_bootstrap_state(),
        CatalogueBootstrapState::Uninitialized
    );
    let reopened = receiver.reopen_in_place().expect("no malformed bootstrap state persisted");
    assert_eq!(
        reopened.catalogue_bootstrap_state(),
        CatalogueBootstrapState::Uninitialized
    );
}

/// Incremental protocol traffic cannot establish a dynamic-catalogue node's catalogue;
/// only one complete trusted snapshot may cross the bootstrap boundary.
///
/// ```text
/// incremental publication ──► receiver(Uninitialized) ──reject──► no catalogue row
/// ```
#[test]
fn dynamic_catalogue_bootstrap_rejects_incremental_catalogue_messages_without_residue() {
    let empty_schema = empty_public_test_schema();
    let temp_dir = tempfile::tempdir().expect("create receiver store");
    let cfs = empty_schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = RocksDbStorage::open(temp_dir.path(), &refs).expect("open empty receiver store");
    let mut receiver = NodeState::new_catalogue_uninitialized(node(0x95), storage)
        .expect("open explicit uninitialized receiver");

    assert!(matches!(
        receiver.apply_trusted_catalogue_message_settled(SyncMessage::PublishSchema {
            author: AuthorSubject::SYSTEM,
            schema: Box::new(SchemaVersion::new(schema())),
        }),
        Err(Error::CatalogueUninitialized)
    ));
    assert!(
        receiver.database
            .primary_key_scan_raw("jazz_catalogue", &[])
            .expect("scan rejected incremental message")
            .is_empty(),
        "incremental publication must not leave a durable catalogue row"
    );
}

/// Direct public mutation APIs are the same catalogue admission boundary as
/// sync dispatch.  An uninitialized receiver must reject a structurally valid
/// commit unit and fate update before either can create transaction or parked
/// durable residue.
#[test]
fn dynamic_catalogue_bootstrap_rejects_direct_ingest_and_fate_without_residue() {
    let (_source_dir, mut source) = open_node_with_schema(node(0x97), schema());
    let (_tx_id, unit) = source
        .commit_mergeable_unit_settled(
            MergeableCommit::new("todos", row(0x98), 10).cells(title_cells("valid source unit")),
        )
        .unwrap();
    let SyncMessage::CommitUnit { tx, versions } = unit else {
        panic!("commit unit expected");
    };

    let empty_schema = empty_public_test_schema();
    let temp_dir = tempfile::tempdir().expect("create receiver store");
    let cfs = empty_schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = RocksDbStorage::open(temp_dir.path(), &refs).expect("open empty receiver store");
    let mut receiver = NodeState::new_catalogue_uninitialized(node(0x99), storage)
        .expect("open explicit uninitialized receiver");

    assert!(matches!(
        receiver.open_exclusive(OpenTransactionId::new()).resolve(),
        Err(Error::CatalogueUninitialized)
    ));
    assert!(matches!(
        receiver.ingest_commit_unit_settled(tx.clone(), versions.clone(), 20),
        Err(Error::CatalogueUninitialized)
    ));
    assert!(matches!(
        receiver.ingest_relay_commit_unit(tx.clone(), versions).resolve(),
        Err(Error::CatalogueUninitialized)
    ));
    assert!(matches!(
        receiver.apply_fate_update(tx.tx_id, Fate::Accepted, None, Some(DurabilityTier::Global)).resolve(),
        Err(Error::CatalogueUninitialized)
    ));
    for table in [
        "jazz_catalogue",
        "jazz_schema_versions",
        "jazz_transactions",
    ] {
        assert!(
            receiver.database
                .primary_key_scan_raw(table, &[])
                .expect("scan rejected direct mutation")
                .is_empty(),
            "uninitialized direct mutation must not persist {table}"
        );
    }
}

/// Build the trusted catalogue snapshot shared by bootstrap and recovery tests.
// History-only scenarios must preserve explicit permissions: omitted policies
// deny access and would constitute an authorization change from schema().
fn catalogue_evolved_schema_with_allow_all() -> JazzSchema {
    build_public_test_schema(
        PublicSchemaBuilder::new().table(
            PublicTableSchemaBuilder::new("todos")
                .column("title", PublicColumnType::Text)
                .column("body", PublicColumnType::Text),
        ).allow_all(),
    )
}

fn catalogue_snapshot_fixture() -> crate::protocol::CatalogueSnapshot {
    catalogue_snapshot_fixture_for_schema(catalogue_evolved_schema())
}

fn catalogue_snapshot_fixture_for_schema(evolved: JazzSchema) -> crate::protocol::CatalogueSnapshot {
    let base = schema();
    let evolved = SchemaVersion::new(evolved);
    let genesis_physical_identities = PhysicalIdentityManifest::allocate(&base);
    let publication = SchemaLineagePublication::author_from_prior(
        &base,
        &genesis_physical_identities,
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
        ).expect("valid migration lens"),
        Vec::<String>::new(),
        Vec::<String>::new(),
    )
    .expect("fixture authors descendant identities from the genesis authority manifest");
    crate::protocol::CatalogueSnapshot {
        genesis_physical_identities,
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
        core.apply_trusted_catalogue_snapshot_settled(snapshot),
        Err(Error::InvalidCatalogueUpdate(_))
    ));
    assert_eq!(core.active_catalogue_seq(), 0);
    assert_eq!(core.catalogue_schemas().len(), 1);
    assert_eq!(core.current_write_schema().unwrap().revision, 0);
    drop(core);

    let reopened = reopen_node_at(&dir, node(0x38), base);
    assert_eq!(reopened.active_catalogue_seq(), 0);
    assert_eq!(reopened.catalogue_schemas().len(), 1);
    assert_eq!(reopened.current_write_schema().unwrap().revision, 0);
}

#[test]
fn trusted_catalogue_snapshot_rejects_pointer_conflict_without_lineage_activation() {
    let base = schema();
    let (dir, mut core) = open_node_with_schema(node(0x39), base.clone());
    let mut snapshot = catalogue_snapshot_fixture();
    snapshot.current_write_schema.revision = 0;

    assert!(matches!(
        core.apply_trusted_catalogue_snapshot_settled(snapshot),
        Err(Error::InvalidCatalogueUpdate(_))
    ));
    assert_eq!(core.active_catalogue_seq(), 0);
    assert_eq!(core.catalogue_schemas().len(), 1);
    drop(core);

    let reopened = reopen_node_at(&dir, node(0x39), base);
    assert_eq!(reopened.active_catalogue_seq(), 0);
    assert_eq!(reopened.catalogue_schemas().len(), 1);
    assert_eq!(reopened.current_write_schema().unwrap().revision, 0);
}

#[test]
fn trusted_catalogue_snapshot_activation_failure_never_exposes_a_prefix_and_reopens_old() {
    let base = schema();
    let (dir, mut core) = open_node_with_schema(node(0x3a), base.clone());
    core.set_catalogue_activation_failpoint(
        CatalogueActivationFailpoint::BeforeSnapshotActivationCommit,
    );

    assert!(matches!(
        core.apply_trusted_catalogue_snapshot_settled(catalogue_snapshot_fixture()),
        Err(Error::CatalogueActivationFailed)
    ));
    assert_eq!(core.active_catalogue_seq(), 0);
    assert_eq!(core.catalogue_schemas().len(), 1);
    assert_eq!(core.current_write_schema().unwrap().revision, 0);
    assert!(matches!(
        core.activate_catalogue_schema_settled(CurrentWriteSchema {
            revision: 1,
            schema: base.version_id(),
        }),
        Err(Error::CatalogueActivationFailed)
    ));
    drop(core);

    let mut reopened = reopen_node_at(&dir, node(0x3a), base);
    assert_eq!(reopened.active_catalogue_seq(), 0);
    assert_eq!(reopened.catalogue_schemas().len(), 1);
    assert_eq!(reopened.current_write_schema().unwrap().revision, 0);
    reopened
        .apply_trusted_catalogue_snapshot_settled(catalogue_snapshot_fixture())
        .unwrap();
    assert_eq!(reopened.active_catalogue_seq(), 1);
    assert_eq!(reopened.catalogue_schemas().len(), 2);
    assert_eq!(reopened.current_write_schema().unwrap().revision, 1);
}

/// Alice owns an unpersisted transaction while Bob offers a catalogue snapshot.
/// The direct node API leaves its catalogue usable and returns a retryable busy
/// result; after Alice settles, replay succeeds without a runtime failure.
#[test]
fn catalogue_snapshot_waits_for_external_publication_without_mutating_catalogue() {
    let (_dir, mut alice) = open_node_with_schema(node(0x75), schema());
    let snapshot = alice.catalogue_snapshot().unwrap();
    let before = alice.groove_runtime_token();
    let published = alice.commit_mergeable(
        MergeableCommit::new("todos", row(0x76), 1_000)
            .made_by(AuthorSubject::SYSTEM)
            .cells(BTreeMap::from([("title".to_owned(), Value::String("preserved".to_owned()))])),
    ).unwrap();
    let result = crate::db::block_on(alice.apply_trusted_catalogue_snapshot(snapshot.clone()));
    assert!(matches!(result, Err(Error::Groove(groove::db::Error::UnsettledPublications))));
    assert!(!alice.catalogue_activation_failed);
    assert_eq!(alice.groove_runtime_token(), before);
    assert!(alice.defer_catalogue_for_persistence(None).unwrap());
    settle_published(&mut alice, published).unwrap();
    assert!(!alice.defer_catalogue_for_persistence(None).unwrap());
    alice.apply_trusted_catalogue_snapshot_settled(snapshot).unwrap();
    assert!(!alice.catalogue_activation_failed);
    assert_eq!(alice.query_all_versions().unwrap().len(), 1);
}

/// Alice reopens her persisted catalogue against Bob's warm authority, then
/// Bob restarts; table declaration permutations remain the same publication.
/// alice <- snapshot -- bob; alice reopen <- bob; bob restart -> alice
#[test]
fn reordered_lineage_declarations_survive_client_and_authority_reopen() {
    // Internal trusted-transport boundary coverage: public clients cannot inject
    // a historical noncanonical declaration order into an authority snapshot.
    let base = schema();
    let mut builder = crate::tools::SchemaBuilder::new()
        .table(crate::tools::TableSchema::builder("todos")
            .column("title", crate::tools::ColumnType::Text));
    for name in ["zebra", "alpha", "middle"] {
        builder = builder.table(crate::tools::TableSchema::builder(name)
            .column("title", crate::tools::ColumnType::Text));
    }
    let evolved = SchemaVersion::new(crate::schema::JazzSchema::new(&builder.build()).unwrap());
    let (bob_dir, mut bob) = open_node_with_schema(node(0xd1), base.clone());
    publish_schema_lineage(&mut bob, evolved.clone(), MigrationLens::new(
        base.version_id(), evolved.id, vec![TableLens {
            source_table: "todos".into(), target_table: "todos".into(), ops: vec![],
        }]).unwrap(), ["zebra", "alpha", "middle"], Vec::<String>::new()).unwrap();
    let snapshot = bob.catalogue_snapshot().unwrap();
    assert_eq!(snapshot.lineages[0].1.new_tables, ["alpha", "middle", "zebra"]);
    let (alice_dir, mut alice) = open_node_with_schema(node(0xd2), base.clone());
    alice.apply_trusted_catalogue_snapshot_settled(snapshot.clone()).unwrap();
    drop(alice);
    let mut alice = reopen_node_at(&alice_dir, node(0xd2), base.clone());
    // Simulate all orders produced by a pre-fix warm authority or wire sender.
    for order in [["alpha", "middle", "zebra"], ["alpha", "zebra", "middle"],
        ["middle", "alpha", "zebra"], ["middle", "zebra", "alpha"],
        ["zebra", "alpha", "middle"], ["zebra", "middle", "alpha"]] {
        let mut reordered = snapshot.clone();
        reordered.lineages[0].1.new_tables = order.map(String::from).to_vec();
        assert_eq!(reordered.lineages[0].1.content_id(), snapshot.lineages[0].1.id);
        assert_eq!(reordered.lineages[0].1, snapshot.lineages[0].1);
        alice.apply_trusted_catalogue_snapshot_settled(reordered).unwrap();
    }
    drop(bob);
    let bob = reopen_node_at(&bob_dir, node(0xd1), base);
    alice.apply_trusted_catalogue_snapshot_settled(bob.catalogue_snapshot().unwrap()).unwrap();
    assert_eq!(alice.active_catalogue_seq(), 1);

    let mut conflicting = snapshot;
    conflicting.lineages[0].1.physical_identities.tables.get_mut("alpha").unwrap().id =
        PhysicalIdentityManifest::allocate(&evolved.schema).tables["alpha"].id;
    conflicting.lineages[0].1.id = conflicting.lineages[0].1.content_id();
    assert!(matches!(alice.apply_trusted_catalogue_snapshot_settled(conflicting),
        Err(Error::InvalidCatalogueUpdate("trusted catalogue snapshot lineage conflicts with catalogue"))));
    assert_eq!(alice.active_catalogue_seq(), 1);
}

/// Alice compares full immutable publications; reordered declarations are
/// equal but changed content, claimed identity, and multiplicity remain unequal.
#[test]
fn lineage_equality_preserves_content_and_declaration_multiplicity() {
    // Internal payload contract coverage is needed for malformed payloads that
    // public authoring APIs deliberately cannot construct.
    let mut original = catalogue_snapshot_fixture().lineages.remove(0).1;
    original.new_tables = vec!["zebra".into(), "alpha".into()];
    original.dropped_tables = vec!["retired_z".into(), "retired_a".into()];
    original.id = original.content_id();
    let mut reordered = original.clone();
    reordered.new_tables.reverse();
    reordered.dropped_tables.reverse();
    assert_eq!(original.content_id(), reordered.content_id());
    assert_eq!(original, reordered);
    for field in 0..6 {
        let mut changed = original.clone();
        match field {
            0 => changed.new_tables.push("alpha".into()),
            1 => changed.dropped_tables[0] = "other".into(),
            2 => changed.schema.id = SchemaVersion::new(schema()).id,
            3 => changed.lens = MigrationLens::new(changed.lens.source(), changed.lens.target(), vec![]).unwrap(),
            4 => changed.physical_identities = PhysicalIdentityManifest::allocate(&changed.schema.schema),
            _ => changed.id = SchemaLineagePublicationId(uuid::Uuid::nil()),
        }
        assert_ne!(original, changed, "changed field {field} must remain unequal even with a copied id");
        if field != 5 {
            assert_ne!(original.content_id(), changed.content_id());
        }
    }
}

/// Opening an old receiver upgrades storage before any snapshot is received.
/// Internal because creating an old durable layout requires writing catalogue records.
#[test]
fn legacy_receiver_upgrades_active_schema_during_open() {
    let base = schema();
    let (_authority_dir, authority) = open_node_with_schema(node(0xc8), base.clone());
    let mut current = authority.catalogue_snapshot().unwrap();
    current.current_write_schema.revision = 2;
    let mut legacy = current.clone();
    legacy.current_write_schema.revision = 7;
    let dir = tempfile::tempdir().unwrap();
    let mut receiver = fresh_dynamic_catalogue_open(dir.path(), node(0xc9)).unwrap();
    receiver.apply_trusted_catalogue_snapshot_settled(legacy)
        .unwrap();
    // Old receivers stored the selected permissions in the schema payload and had no active-schema record.
    delete_catalogue_record(&mut receiver, b"active_schema", uuid::Uuid::nil());
    write_catalogue_record(
        &mut receiver,
        b"schema",
        base.version_id().0,
        codec::encode_catalogue_schema(&SchemaVersion::new(base)).unwrap(),
    );
    drop(receiver);

    let receiver = fresh_dynamic_catalogue_open(dir.path(), node(0xc9)).unwrap();
    assert_eq!(receiver.current_write_schema().unwrap().revision, 0);
    assert_eq!(receiver.catalogue_snapshot().unwrap().schemas, current.schemas);
    drop(receiver);
    // The conversion is durable even if no server has connected yet.
    let mut receiver = fresh_dynamic_catalogue_open(dir.path(), node(0xc9)).unwrap();
    assert_eq!(receiver.current_write_schema().unwrap().revision, 0);
    receiver.apply_trusted_catalogue_snapshot_settled(current.clone())
        .unwrap();
    assert_eq!(receiver.current_write_schema().unwrap().revision, 2);
    let mut conflicting = current.clone();
    conflicting.schemas[0] =
        SchemaVersion::new(conflicting.schemas[0].schema.without_permissions());
    assert!(
        receiver.apply_trusted_catalogue_snapshot_settled(conflicting)
            .is_err()
    );
    let mut stale = current;
    stale.current_write_schema.revision = 1;
    assert!(
        receiver.apply_trusted_catalogue_snapshot_settled(stale.clone())
            .is_err()
    );
    drop(receiver);
    let mut reopened = fresh_dynamic_catalogue_open(dir.path(), node(0xc9)).unwrap();
    assert_eq!(reopened.current_write_schema().unwrap().revision, 2);
    assert!(
        reopened
            .apply_trusted_catalogue_snapshot_settled(stale)
            .is_err()
    );
}

/// Uses internal APIs to recreate an older schema publication with embedded permissions.
#[test]
fn permission_bearing_lineage_snapshot_replay_reopens_without_restoring_old_grants() {
    let mut snapshot = catalogue_snapshot_fixture();
    let granted = crate::schema::JazzSchema::new(
        &crate::tools::SchemaBuilder::new()
            .table(
                crate::tools::TableSchema::builder("todos")
                    .column("title", crate::tools::ColumnType::Text)
                    .column("body", crate::tools::ColumnType::Text)
                    .policies(
                        crate::tools::TablePolicies::new()
                            .with_select(crate::tools::PolicyExpr::True),
                    ),
            )
            .build(),
    )
    .unwrap();
    let original = &snapshot.lineages[0].1;
    let publication = SchemaLineagePublication::author_from_prior(
        &snapshot.schemas[0].schema,
        &snapshot.genesis_physical_identities,
        SchemaVersion::new(granted.clone()),
        original.lens.clone(),
        Vec::<String>::new(),
        Vec::<String>::new(),
    )
    .unwrap();
    snapshot.lineages[0].1 = publication.clone();
    // The active bundle revokes the grant retained in the immutable receipt.
    let denied = granted.without_permissions();
    snapshot.schemas[1] = SchemaVersion::new(denied.clone());
    let dir = tempfile::tempdir().unwrap();
    let mut receiver = fresh_dynamic_catalogue_open(dir.path(), node(0xca)).unwrap();
    receiver.apply_trusted_catalogue_snapshot_settled(snapshot.clone())
        .unwrap();
    receiver.apply_trusted_catalogue_snapshot_settled(snapshot.clone())
        .unwrap();
    drop(receiver);

    let mut reopened = fresh_dynamic_catalogue_open(dir.path(), node(0xca))
        .expect("replay must leave a reopenable catalogue");
    assert_eq!(
        reopened.catalogue.active_schema.compiled.public_schema(),
        denied.public_schema()
    );
    assert_eq!(
        reopened.catalogue_snapshot().unwrap().lineages[0].1,
        publication
    );
    reopened
        .apply_trusted_catalogue_snapshot_settled(snapshot)
        .unwrap();
    drop(reopened);
    let reopened = fresh_dynamic_catalogue_open(dir.path(), node(0xca)).unwrap();
    assert_eq!(
        reopened.catalogue.active_schema.compiled.public_schema(),
        denied.public_schema()
    );
}

// Internal because physical source identity and its live-graph invalidation
// token are the contract under test; schemas still use the public builders.
#[test]
fn constant_policy_schema_switch_invalidates_replaced_physical_table() {
    let base = schema();
    let evolved = SchemaVersion::new(catalogue_evolved_schema_with_allow_all());
    let (_dir, mut receiver) = open_node_with_schema(node(0x75), base.clone());
    let old_table = receiver.physical_table_id_for_schema(base.version_id(), "todos").unwrap();
    let lens = MigrationLens::new(base.version_id(), evolved.id, Vec::new()).unwrap();
    publish_schema_lineage(
        &mut receiver,
        evolved.clone(),
        lens,
        vec!["todos".to_owned()],
        vec!["todos".to_owned()],
    ).unwrap();
    assert_ne!(
        receiver.physical_table_id_for_schema(evolved.id, "todos").unwrap(),
        old_table,
    );
    let before = receiver.groove_runtime_token();
    receiver.activate_catalogue_schema_settled(CurrentWriteSchema {
        revision: 1,
        schema: evolved.id,
    }).unwrap();
    assert_ne!(receiver.groove_runtime_token(), before);
}

#[test]
fn trusted_identity_rebind_updates_live_peer_support_coordinates() {
    // Real independently opened catalogues mint distinct provisional UUIDs.
    // The usual shared test catalogue helper intentionally masks this race.
    let schema = schema();
    let open = |id| {
        let dir = tempfile::tempdir().unwrap();
        let cfs = schema.column_families();
        let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
        let storage = RocksDbStorage::open(dir.path(), &refs).unwrap();
        let node = NodeState::new(node(id), schema.clone(), storage).unwrap();
        (dir, node)
    };
    let (_authority_dir, authority) = open(0x71);
    let (_relay_dir, mut relay) = open(0x72);
    let schema_id = schema.version_id();
    let expected = authority.scope_physical_table(schema_id, "todos").unwrap();
    assert_ne!(
        relay.scope_physical_table(schema_id, "todos").unwrap(),
        expected
    );
    let tx = relay
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row(0x73), 10)
                .cells(title_cells("pending-before-rebind")),
        )
        .unwrap();
    let shape = Query::from("todos").validate(&schema).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let (_receiver_dir, mut receiver) = open(0x74);
    receiver
        .apply_trusted_catalogue_snapshot_settled(relay.catalogue_snapshot().unwrap())
        .unwrap();
    register_shape_binding(&mut receiver, &shape, &binding);
    let subscription = SubscriptionKey {
        shape_id: shape.shape_id(),
        binding_id: binding.binding_id(),
        read_view: Default::default(),
    };
    let key = receiver
        .authority_result_key_for_subscription(subscription)
        .unwrap();
    let baseline_subscriptions = relay.runtime_stats_for_test().active_subscriptions;
    let mut downstream = PeerState::new();
    let initial = downstream
        .rehydrate_query(&mut relay, &shape, &binding)
        .unwrap();
    receiver.apply_sync_message_settled(initial).unwrap();
    let generation = receiver.applied_authority_result_generation(&key);
    assert!(receiver.has_settled_authority_result(&key));
    let snapshot = authority.catalogue_snapshot().unwrap();
    relay
        .apply_trusted_catalogue_snapshot_settled(snapshot.clone())
        .unwrap();
    receiver
        .apply_trusted_catalogue_snapshot_settled(snapshot.clone())
        .unwrap();
    assert_eq!(
        relay.scope_physical_table(schema_id, "todos").unwrap(),
        expected
    );
    assert!(!receiver.has_settled_authority_result(&key));
    assert_eq!(
        receiver.applied_authority_result_generation(&key),
        generation
    );
    assert!(
        receiver.query.authority_results[&key]
            .compiled_covered_input_sources
            .is_none()
    );
    let token = relay.groove_runtime_token();
    let identity_generation = relay.physical_identity_generation();
    relay
        .apply_trusted_catalogue_snapshot_settled(snapshot)
        .unwrap();
    assert_eq!(
        relay.groove_runtime_token(),
        token,
        "identical snapshot must preserve query validity"
    );
    assert_eq!(relay.physical_identity_generation(), identity_generation);
    relay.accept_global_for_test(tx).unwrap();
    let update = crate::protocol::ViewUpdatePayload::from_view_update(
        downstream
            .query_update(&mut relay, &shape, &binding)
            .unwrap(),
    )
    .unwrap();
    assert!(
        update.supporting_rows.is_snapshot(),
        "rebound peer must replace its old closure"
    );
    assert_eq!(update.supporting_rows.added_rows().len(), 1);
    assert_eq!(
        update.supporting_rows.added_rows()[0].physical_table,
        expected,
        "live peer support must use the same permanent identities as its announced catalogue"
    );
    receiver
        .apply_sync_message_settled(update.into_view_update())
        .unwrap();
    assert!(receiver.has_settled_authority_result(&key));
    assert!(receiver.applied_authority_result_generation(&key) > generation);
    assert_eq!(receiver.scalar_authority_input_rows(&key, "todos").len(), 1);
    assert_eq!(
        relay.runtime_stats_for_test().active_subscriptions,
        baseline_subscriptions + 1,
        "identity refresh must retire the old peer graph immediately"
    );
    downstream.forget_subscription_with_node(&mut relay, subscription);
    assert_eq!(
        relay.runtime_stats_for_test().active_subscriptions,
        baseline_subscriptions,
        "forget must release the replacement without waiting for another runtime tick"
    );
}
