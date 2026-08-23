// Catalogue snapshot installation, durable validation, and dynamic-edge bootstrap.

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
fn trusted_snapshot_carries_policy_source_and_edge_recompiles_it_after_reopen() {
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
    let edge_dir = tempfile::tempdir().expect("create edge store");
    let cfs = empty.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = RocksDbStorage::open(edge_dir.path(), &refs).expect("open edge store");
    let mut edge = NodeState::new_catalogue_uninitialized(node(0x34), storage)
        .expect("open uninitialized edge");
    edge.apply_trusted_catalogue_snapshot_settled(snapshot)
        .expect("install source snapshot");
    assert!(
        edge.try_current_schema()
            .expect("edge has a current schema")
            .tables
            .iter()
            .find(|table| table.name == "todos")
            .and_then(|table| table.read_policy.as_ref())
            .is_some(),
        "edge compiles the source PolicyExpr"
    );

    drop(edge);
    let cfs = empty.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = RocksDbStorage::open(edge_dir.path(), &refs).expect("reopen edge store");
    let reopened = NodeState::new_catalogue_uninitialized(node(0x34), storage)
        .expect("reopen edge from persisted source");
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
        .apply_trusted_catalogue_message_settled(SyncMessage::SetCurrentWriteSchema {
            author: AuthorSubject::SYSTEM,
            pointer: CurrentWriteSchema {
                revision: 1,
                schema: evolved.id,
            },
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

    let (_receiver_dir, mut receiver) = open_node_with_schema(node(0x37), base);
    let snapshot = authority.catalogue_snapshot().unwrap();
    assert!(matches!(
        receiver.apply_sync_message_settled(SyncMessage::CatalogueSnapshot(Box::new(snapshot.clone()))),
        Err(Error::UnsupportedSyncMessage(
            "catalogue snapshot requires a trusted upstream link"
        ))
    ));
    receiver.apply_trusted_catalogue_snapshot_settled(snapshot).unwrap();
    assert_eq!(receiver.current_write_schema().unwrap().schema, evolved.id);
    receiver.apply_sync_message_settled(authored).unwrap();
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
        .apply_trusted_catalogue_message_settled(SyncMessage::SetCurrentWriteSchema {
            author: AuthorSubject::SYSTEM,
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
        .commit_mergeable_unit_settled(
            MergeableCommit::new("todos", row(0x62), 10).cells(BTreeMap::from([
                ("title".to_owned(), v("before-snapshot")),
                ("body".to_owned(), v("authored-evolved")),
            ])),
        )
        .unwrap();
    receiver
        .apply_trusted_catalogue_snapshot_settled(authority.catalogue_snapshot().unwrap())
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
        ),
        Vec::<String>::new(),
        Vec::<String>::new(),
    )
    .unwrap();
    authority
        .apply_trusted_catalogue_message_settled(SyncMessage::SetCurrentWriteSchema {
            author: AuthorSubject::SYSTEM,
            pointer: CurrentWriteSchema {
                revision: 1,
                schema: evolved.id,
            },
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
fn trusted_catalogue_snapshot_rebuilds_transitions_but_preserves_identical_prefixes() {
    // A trusted snapshot is a complete authoritative prefix, not a delta. Once
    // its activation commits, reopening must retain enough canonical lineage
    // identity to recognize that same prefix on the next upstream connection.
    let base = schema();
    let snapshot = catalogue_snapshot_fixture();
    let (dir, mut receiver) = open_node_with_schema(node(0x3f), base.clone());
    let runtime_before_transition = receiver.groove_runtime_token();

    receiver
        .apply_trusted_catalogue_snapshot_settled(snapshot.clone())
        .unwrap();
    assert_eq!(receiver.active_catalogue_seq(), 1);
    assert_ne!(
        receiver.groove_runtime_token(),
        runtime_before_transition,
        "a v1-to-v2 catalogue transition reconstructs local IVM projections"
    );

    let evolved = catalogue_evolved_schema();
    let shape = Query::from("todos").validate(&evolved).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let cached_view = crate::protocol::BindingViewKey::new(
        shape.shape_id(),
        binding.binding_id(),
        Default::default(),
    );
    receiver
        .query
        .settled_result_sets
        .insert(cached_view, BTreeSet::new());
    let runtime_before_idempotent_replay = receiver.groove_runtime_token();
    receiver
        .apply_trusted_catalogue_snapshot_settled(snapshot.clone())
        .unwrap();
    assert_eq!(receiver.groove_runtime_token(), runtime_before_idempotent_replay);
    assert!(
        receiver.query.settled_result_sets.contains_key(&cached_view),
        "an identical trusted prefix must not clear maintained/query state"
    );
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
            Value::Bytes(kind.to_vec()),
            Value::Uuid(id),
            Value::Bytes(payload),
        ],
    );
    let applied = crate::db::block_on(node.database.apply_batch(batch)).unwrap();
let persisted = crate::db::block_on(applied.persist());
node.database.finish_persistence(persisted).unwrap();
}

fn delete_catalogue_record(node: &mut NodeState<RocksDbStorage>, kind: &[u8], id: uuid::Uuid) {
    let mut batch = node.database.open_batch();
    batch.delete(
        "jazz_catalogue",
        groove::db::PrimaryKeyValue::Composite(vec![
            groove::db::PrimaryKeyValue::Bytes(kind.to_vec()),
            groove::db::PrimaryKeyValue::Uuid(id),
        ]),
    );
    let applied = crate::db::block_on(node.database.apply_batch(batch)).unwrap();
let persisted = crate::db::block_on(applied.persist());
node.database.finish_persistence(persisted).unwrap();
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

fn fresh_dynamic_edge_open(
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
        serde_json::to_vec(staged).unwrap(),
    );
    write_catalogue_record(
        node,
        b"schema_lineage_active",
        staged.publication.id.0,
        serde_json::to_vec(&SchemaLineageActivation {
            id: staged.publication.id,
            catalogue_seq: staged.catalogue_seq,
        })
        .unwrap(),
    );
}

fn duplicate_schema_destination_lineage(
    base: &JazzSchema,
    original: &StagedSchemaLineage,
    catalogue_seq: u64,
) -> StagedSchemaLineage {
    let duplicate_publication = SchemaLineagePublication::new(
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
        ),
        Vec::<String>::new(),
        Vec::<String>::new(),
    );
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
        serde_json::to_vec(&staged).unwrap(),
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
    assert!(matches!(
        NodeState::new(node_uuid, schema, storage).resolve(),
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
        serde_json::to_vec(&staged).unwrap(),
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
        ),
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
    let duplicate = duplicate_schema_destination_lineage(&base, &original, 2);
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
    let duplicate = duplicate_schema_destination_lineage(&base, &original, 2);
    write_catalogue_record(
        &mut receiver,
        b"schema_lineage_staged",
        duplicate.publication.id.0,
        serde_json::to_vec(&duplicate).unwrap(),
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
    let duplicate = duplicate_schema_destination_lineage(&base, &first, 2);
    delete_catalogue_record(
        &mut receiver,
        b"schema_lineage_active",
        first.publication.id.0,
    );
    write_catalogue_record(
        &mut receiver,
        b"schema_lineage_staged",
        first.publication.id.0,
        serde_json::to_vec(&first).unwrap(),
    );
    write_catalogue_record(
        &mut receiver,
        b"schema_lineage_staged",
        duplicate.publication.id.0,
        serde_json::to_vec(&duplicate).unwrap(),
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
        serde_json::to_vec(&staged).unwrap(),
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
        "staged schema lineage violates trusted publication invariants",
        |staged| {
            staged.publication.schema.id = base_id;
            staged.publication.id = staged.publication.content_id();
        },
    );
}

#[test]
fn reopen_rejects_staged_lens_content_identity_mismatch() {
    assert_staged_corruption_rejected(
        0x48,
        "staged schema lineage violates trusted publication invariants",
        |staged| {
            staged.publication.lens.id = MigrationLensId(uuid::Uuid::nil());
            staged.publication.id = staged.publication.content_id();
        },
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

/// A dynamic edge without a local catalogue must not manufacture the empty
/// constructor schema as durable genesis; after its trusted core snapshot it
/// atomically adopts the core lineage and survives reopen.
///
/// ```text
/// core catalogue snapshot ──trusted install──► edge(Uninitialized -> Ready)
///                                                        │
///                                                        └──reopen──► exact core genesis
/// ```
#[test]
fn dynamic_edge_bootstrap_adopts_authority_genesis_atomically_and_reopens_ready() {
    let empty_schema = empty_public_test_schema();
    let temp_dir = tempfile::tempdir().expect("create edge store");
    let cfs = empty_schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = RocksDbStorage::open(temp_dir.path(), &refs).expect("open empty edge store");
    let mut edge = NodeState::new_catalogue_uninitialized(node(0x91), storage)
        .expect("open explicit uninitialized edge");

    assert_eq!(
        edge.catalogue_bootstrap_state(),
        CatalogueBootstrapState::Uninitialized
    );
    assert!(matches!(
        edge.try_current_write_schema(),
        Err(Error::CatalogueUninitialized)
    ));
    assert!(matches!(
        edge.current_write_schema(),
        Err(Error::CatalogueUninitialized)
    ));
    assert!(matches!(
        edge.try_current_schema(),
        Err(Error::CatalogueUninitialized)
    ));
    assert!(matches!(
        edge.catalogue_snapshot(),
        Err(Error::CatalogueUninitialized)
    ));
    assert!(
        edge.database
            .primary_key_scan_raw("jazz_catalogue", &[])
            .expect("scan empty durable catalogue")
            .is_empty(),
        "uninitialized edge must not persist an empty-schema genesis marker"
    );
    assert!(
        edge.database
            .primary_key_scan_raw("jazz_schema_versions", &[])
            .expect("scan empty durable physical mappings")
            .is_empty(),
        "uninitialized edge must not persist a provisional physical mapping"
    );
    assert!(matches!(
        edge.current_rows("todos", DurabilityTier::Local).resolve(),
        Err(Error::CatalogueUninitialized)
    ));
    assert!(matches!(
        edge.commit_mergeable_settled(MergeableCommit::new("todos", row(0x92), 1).cells(BTreeMap::from([
            ("title".to_owned(), v("must not write before catalogue bootstrap")),
        ]))),
        Err(Error::CatalogueUninitialized)
    ));

    let snapshot = catalogue_snapshot_fixture();
    let authority_genesis = schema().version_id();
    edge.apply_trusted_catalogue_snapshot_settled(snapshot.clone())
        .expect("install exact trusted core catalogue");
    assert_eq!(edge.catalogue_bootstrap_state(), CatalogueBootstrapState::Ready);
    assert_eq!(edge.catalogue.current_schema_version_id, authority_genesis);
    assert_eq!(edge.catalogue.schema, schema());
    assert_eq!(edge.current_write_schema().unwrap(), snapshot.current_write_schema);
    assert_eq!(edge.active_catalogue_seq(), 1);
    assert_eq!(edge.catalogue_schemas().len(), 2);

    drop(edge);
    let cfs = empty_schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = RocksDbStorage::open(temp_dir.path(), &refs).expect("reopen edge store");
    let reopened = NodeState::new_catalogue_uninitialized(node(0x91), storage)
        .expect("fresh process discovers durable authority genesis");
    assert_eq!(reopened.catalogue_bootstrap_state(), CatalogueBootstrapState::Ready);
    assert_eq!(
        reopened.catalogue.current_schema_version_id,
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

/// A failed first trusted snapshot leaves a dynamic edge uninitialized, so a
/// later reopen cannot observe a partially installed genesis or pointer.
///
/// ```text
/// core snapshot ──durable failpoint──► edge(Uninitialized) ──reopen──► Uninitialized
/// ```
#[test]
fn dynamic_edge_bootstrap_failure_never_persists_a_partial_authority_catalogue() {
    let empty_schema = empty_public_test_schema();
    let temp_dir = tempfile::tempdir().expect("create edge store");
    let cfs = empty_schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = RocksDbStorage::open(temp_dir.path(), &refs).expect("open empty edge store");
    let mut edge = NodeState::new_catalogue_uninitialized(node(0x93), storage)
        .expect("open explicit uninitialized edge");
    edge.set_catalogue_activation_failpoint(
        CatalogueActivationFailpoint::BeforeSnapshotActivationCommit,
    );

    assert!(matches!(
        edge.apply_trusted_catalogue_snapshot_settled(catalogue_snapshot_fixture()),
        Err(Error::CatalogueActivationFailed)
    ));
    assert_eq!(
        edge.catalogue_bootstrap_state(),
        CatalogueBootstrapState::Uninitialized
    );
    assert!(matches!(
        edge.try_current_write_schema(),
        Err(Error::CatalogueUninitialized)
    ));
    assert!(
        edge.database
            .primary_key_scan_raw("jazz_catalogue", &[])
            .expect("scan failed bootstrap catalogue")
            .is_empty(),
        "failed bootstrap must not leave a genesis, pointer, or lineage prefix"
    );
    assert!(
        edge.database
            .primary_key_scan_raw("jazz_schema_versions", &[])
            .expect("scan failed bootstrap mappings")
            .is_empty(),
        "failed bootstrap must not leave a physical mapping prefix"
    );

    drop(edge);
    let cfs = empty_schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = RocksDbStorage::open(temp_dir.path(), &refs).expect("reopen empty edge store");
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

/// A fresh dynamic-edge open treats every durable catalogue row as a completed
/// bootstrap only when its atomic completion record is present.  A raw
/// genesis/schema prefix is corrupt, not an invitation to repair it using an
/// empty local schema.
#[test]
fn dynamic_edge_reopen_rejects_catalogue_prefix_without_bootstrap_marker() {
    let empty_schema = empty_public_test_schema();
    let temp_dir = tempfile::tempdir().expect("create edge store");
    let cfs = empty_schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = RocksDbStorage::open(temp_dir.path(), &refs).expect("open empty edge store");
    let mut edge = NodeState::new_catalogue_uninitialized(node(0x9a), storage)
        .expect("open explicit uninitialized edge");
    let snapshot = catalogue_snapshot_fixture();
    edge.apply_trusted_catalogue_snapshot_settled(snapshot).unwrap();
    delete_catalogue_record(
        &mut edge,
        b"bootstrap_ready",
        schema().version_id().0,
    );
    drop(edge);

    for attempt in 0..2 {
        assert!(matches!(
            fresh_dynamic_edge_open(temp_dir.path(), node(0x9a)),
            Err(Error::InvalidStoredValue(
                "dynamic catalogue state has no bootstrap completion marker"
            ))
        ), "open attempt {attempt} must reject rather than repair the prefix");
    }
}

/// Removing a normal node's catalogue cannot turn its remaining transaction
/// history into a blank dynamic edge.  Discovery must fail before an
/// uninitialized constructor can adopt a new authority over stale data.
#[test]
fn dynamic_edge_reopen_rejects_catalogue_stripped_history() {
    let base = schema();
    let (temp_dir, mut durable_node) = open_node_with_schema(node(0x9e), base.clone());
    durable_node.commit_mergeable_settled(
        MergeableCommit::new("todos", row(0x9f), 10).cells(title_cells("durable history")),
    )
    .unwrap();
    let alias = durable_node.catalogue.current_schema_version_alias.unwrap();
    delete_catalogue_record(&mut durable_node, b"genesis", base.version_id().0);
    delete_catalogue_record(&mut durable_node, b"schema", base.version_id().0);
    delete_schema_mapping_record(&mut durable_node, alias);
    drop(durable_node);

    for attempt in 0..2 {
        assert!(matches!(
            fresh_dynamic_edge_open(temp_dir.path(), node(0x9e)),
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
fn dynamic_edge_reopen_rejects_truncated_or_mismatched_bootstrap_marker() {
    let empty_schema = empty_public_test_schema();
    let temp_dir = tempfile::tempdir().expect("create edge store");
    let cfs = empty_schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = RocksDbStorage::open(temp_dir.path(), &refs).expect("open empty edge store");
    let mut edge = NodeState::new_catalogue_uninitialized(node(0x9b), storage)
        .expect("open explicit uninitialized edge");
    let snapshot = catalogue_snapshot_fixture();
    edge.apply_trusted_catalogue_snapshot_settled(snapshot.clone()).unwrap();
    delete_catalogue_pointer(&mut edge, snapshot.current_write_schema.revision);
    drop(edge);

    assert!(matches!(
        fresh_dynamic_edge_open(temp_dir.path(), node(0x9b)),
        Err(Error::InvalidStoredValue(
            "catalogue bootstrap completion marker does not match durable catalogue"
        ))
    ));

    let temp_dir = tempfile::tempdir().expect("create second edge store");
    let cfs = empty_schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = RocksDbStorage::open(temp_dir.path(), &refs).expect("open second edge store");
    let mut edge = NodeState::new_catalogue_uninitialized(node(0x9c), storage)
        .expect("open explicit uninitialized edge");
    edge.apply_trusted_catalogue_snapshot_settled(snapshot.clone()).unwrap();
    write_catalogue_record(
        &mut edge,
        b"bootstrap_ready",
        schema().version_id().0,
        serde_json::to_vec(&CatalogueBootstrapReady {
            genesis: schema().version_id(),
            current_write_schema: snapshot.current_write_schema,
            active_catalogue_seq: 0,
        })
        .unwrap(),
    );
    drop(edge);

    assert!(matches!(
        fresh_dynamic_edge_open(temp_dir.path(), node(0x9c)),
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
fn dynamic_edge_reopen_rejects_smuggled_schema_and_mapping() {
    let empty_schema = empty_public_test_schema();
    let temp_dir = tempfile::tempdir().expect("create edge store");
    let cfs = empty_schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = RocksDbStorage::open(temp_dir.path(), &refs).expect("open empty edge store");
    let mut edge = NodeState::new_catalogue_uninitialized(node(0x9d), storage)
        .expect("open explicit uninitialized edge");
    edge.apply_trusted_catalogue_snapshot_settled(catalogue_snapshot_fixture())
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
        &mut next_table_id,
        &mut next_column_id,
    )
    .unwrap();
    write_catalogue_record(
        &mut edge,
        b"schema",
        smuggled.id.0,
        serde_json::to_vec(&smuggled).unwrap(),
    );
    write_schema_mapping_record(&mut edge, SchemaVersionAlias(99), smuggled.id, &mapping);
    drop(edge);

    for attempt in 0..2 {
        assert!(matches!(
            fresh_dynamic_edge_open(temp_dir.path(), node(0x9d)),
            Err(Error::InvalidStoredValue(
                "catalogue bootstrap completion marker does not match durable catalogue"
            ))
        ), "open attempt {attempt} must reject rather than repair smuggled state");
    }
}

/// A crash after canonical lineage staging but before activation leaves no
/// target schema or mapping.  A fresh dynamic edge must accept that exact
/// durable seam, drain it into one atomic activation, and refresh its
/// bootstrap receipt for the next process open.
#[test]
fn dynamic_edge_reopen_drains_after_staged_lineage_crash() {
    let base = schema();
    let empty_schema = empty_public_test_schema();
    let temp_dir = tempfile::tempdir().expect("create edge store");
    let cfs = empty_schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = RocksDbStorage::open(temp_dir.path(), &refs).expect("open empty edge store");
    let mut edge = NodeState::new_catalogue_uninitialized(node(0xa1), storage)
        .expect("open explicit uninitialized edge");
    edge.apply_trusted_catalogue_snapshot_settled(crate::protocol::CatalogueSnapshot {
        schemas: vec![SchemaVersion::new(base.clone())],
        lineages: Vec::new(),
        current_write_schema: CurrentWriteSchema {
            revision: 0,
            schema: base.version_id(),
        },
    })
    .unwrap();

    let target = SchemaVersion::new(catalogue_evolved_schema());
    let publication = SchemaLineagePublication::new(
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
        ),
        Vec::<String>::new(),
        Vec::<String>::new(),
    );
    edge.set_catalogue_activation_failpoint(CatalogueActivationFailpoint::AfterStaged);
    assert!(matches!(
        edge.apply_trusted_catalogue_message_settled(SyncMessage::PublishSchemaWithLens {
            author: AuthorSubject::SYSTEM,
            catalogue_seq: 1,
            publication: Box::new(publication),
        }),
        Err(Error::CatalogueActivationFailed)
    ));
    assert!(
        edge.database
            .primary_key_scan_raw("jazz_schema_versions", &[])
            .unwrap()
            .iter()
            .all(|raw| raw.record().get_uuid(SchemaVersionAliasRowRecord::FIELD_UUID_IDX).unwrap()
                != target.id.0),
        "AfterStaged must not persist the inactive target mapping"
    );
    drop(edge);

    let reopened = fresh_dynamic_edge_open(temp_dir.path(), node(0xa1))
        .expect("fresh discovery accepts canonical inactive staging");
    assert_eq!(reopened.active_catalogue_seq(), 1);
    assert!(reopened.catalogue_schemas().contains_key(&target.id));
    drop(reopened);

    let reopened = fresh_dynamic_edge_open(temp_dir.path(), node(0xa1))
        .expect("activation refreshes the dynamic bootstrap receipt");
    assert_eq!(reopened.active_catalogue_seq(), 1);
    assert!(reopened.catalogue_schemas().contains_key(&target.id));
}

/// A bootstrap snapshot has exactly one non-lineage schema: the authority's
/// genesis.  Mallory cannot make an edge choose among multiple roots.
///
/// ```text
/// malformed snapshot(two roots) ──► edge(Uninitialized) ──reject──► no durable state
/// ```
#[test]
fn dynamic_edge_bootstrap_rejects_snapshot_with_ambiguous_genesis() {
    let empty_schema = empty_public_test_schema();
    let temp_dir = tempfile::tempdir().expect("create edge store");
    let cfs = empty_schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = RocksDbStorage::open(temp_dir.path(), &refs).expect("open empty edge store");
    let mut edge = NodeState::new_catalogue_uninitialized(node(0x94), storage)
        .expect("open explicit uninitialized edge");
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
        edge.apply_trusted_catalogue_snapshot_settled(snapshot),
        Err(Error::InvalidCatalogueUpdate(
            "trusted catalogue snapshot must contain exactly one genesis schema"
        ))
    ));
    assert_eq!(
        edge.catalogue_bootstrap_state(),
        CatalogueBootstrapState::Uninitialized
    );
    let reopened = edge.reopen_in_place().expect("no malformed bootstrap state persisted");
    assert_eq!(
        reopened.catalogue_bootstrap_state(),
        CatalogueBootstrapState::Uninitialized
    );
}

/// Incremental protocol traffic cannot establish a dynamic edge's catalogue;
/// only one complete trusted snapshot may cross the bootstrap boundary.
///
/// ```text
/// stale incremental pointer ──► edge(Uninitialized) ──reject──► no pending pointer row
/// ```
#[test]
fn dynamic_edge_bootstrap_rejects_incremental_catalogue_messages_without_residue() {
    let empty_schema = empty_public_test_schema();
    let temp_dir = tempfile::tempdir().expect("create edge store");
    let cfs = empty_schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = RocksDbStorage::open(temp_dir.path(), &refs).expect("open empty edge store");
    let mut edge = NodeState::new_catalogue_uninitialized(node(0x95), storage)
        .expect("open explicit uninitialized edge");

    assert!(matches!(
        edge.apply_trusted_catalogue_message_settled(SyncMessage::SetCurrentWriteSchema {
            author: AuthorSubject::SYSTEM,
            pointer: CurrentWriteSchema {
                revision: 1,
                schema: schema().version_id(),
            },
        }),
        Err(Error::CatalogueUninitialized)
    ));
    assert!(
        edge.database
            .primary_key_scan_raw("jazz_catalogue", &[])
            .expect("scan rejected incremental message")
            .is_empty(),
        "incremental pointer must not leave a durable pending catalogue row"
    );
}

/// Direct public mutation APIs are the same catalogue admission boundary as
/// sync dispatch.  An uninitialized edge must reject a structurally valid
/// commit unit and fate update before either can create transaction or parked
/// durable residue.
#[test]
fn dynamic_edge_bootstrap_rejects_direct_ingest_and_fate_without_residue() {
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
    let temp_dir = tempfile::tempdir().expect("create edge store");
    let cfs = empty_schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = RocksDbStorage::open(temp_dir.path(), &refs).expect("open empty edge store");
    let mut edge = NodeState::new_catalogue_uninitialized(node(0x99), storage)
        .expect("open explicit uninitialized edge");

    assert!(matches!(
        edge.open_exclusive(OpenTransactionId::new()).resolve(),
        Err(Error::CatalogueUninitialized)
    ));
    assert!(matches!(
        edge.ingest_commit_unit_settled(tx.clone(), versions.clone(), 20),
        Err(Error::CatalogueUninitialized)
    ));
    assert!(matches!(
        edge.ingest_edge_authority_mergeable_commit_unit(tx.clone(), versions.clone(), 20).resolve(),
        Err(Error::CatalogueUninitialized)
    ));
    assert!(matches!(
        edge.ingest_edge_authority_mergeable_commit_unit_with_identity(
            tx.clone(),
            versions.clone(),
            20,
            AuthorSubject::SYSTEM,
        ).resolve(),
        Err(Error::CatalogueUninitialized)
    ));
    assert!(matches!(
        edge.ingest_relay_commit_unit(tx.clone(), versions).resolve(),
        Err(Error::CatalogueUninitialized)
    ));
    assert!(matches!(
        edge.apply_fate_update(tx.tx_id, Fate::Accepted, None, Some(DurabilityTier::Edge)).resolve(),
        Err(Error::CatalogueUninitialized)
    ));
    for table in [
        "jazz_catalogue",
        "jazz_schema_versions",
        "jazz_transactions",
    ] {
        assert!(
            edge.database
                .primary_key_scan_raw(table, &[])
                .expect("scan rejected direct mutation")
                .is_empty(),
            "uninitialized direct mutation must not persist {table}"
        );
    }
}

/// Build the trusted catalogue snapshot shared by bootstrap and recovery tests.
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
        core.apply_trusted_catalogue_message_settled(SyncMessage::SetCurrentWriteSchema {
            author: AuthorSubject::SYSTEM,
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
    assert_eq!(reopened.current_write_schema().unwrap().revision, 0);
    reopened
        .apply_trusted_catalogue_snapshot_settled(catalogue_snapshot_fixture())
        .unwrap();
    assert_eq!(reopened.active_catalogue_seq(), 1);
    assert_eq!(reopened.catalogue_schemas().len(), 2);
    assert_eq!(reopened.current_write_schema().unwrap().revision, 1);
}
