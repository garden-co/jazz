// Stable wire UUID identity across local alias assignment.

/// Public query rows do not expose the immutable wire schema identity. Exercise
/// the maintained-witness boundary with a read-schema projection of an unchanged
/// table: its identical layout must not let it masquerade as authored history.
#[test]
fn maintained_witness_with_projected_schema_keeps_immutable_history_identity() {
    let base = schema();
    let evolved = build_public_test_schema(
        PublicSchemaBuilder::new()
            .table(PublicTableSchemaBuilder::new("todos").column("title", PublicColumnType::Text))
            .table(
                PublicTableSchemaBuilder::new("controls").column("value", PublicColumnType::Text),
            ),
    );
    let (_dir, mut owner) = open_node_with_schema(node(0xb2), base.clone());
    let tx = owner
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row(0xb2), 10).cells(title_cells("retained")),
        )
        .unwrap();
    publish_schema_lineage(
        &mut owner,
        SchemaVersion::new(evolved.clone()),
        MigrationLens::new(
            base.version_id(),
            evolved.version_id(),
            vec![TableLens {
                source_table: "todos".to_owned(),
                target_table: "todos".to_owned(),
                ops: vec![],
            }],
        )
        .unwrap(),
        vec!["controls".to_owned()],
        Vec::<String>::new(),
    )
    .unwrap();
    let original = owner.query_versions_for_tx(tx).unwrap().remove(0);
    let alias = owner
        .ensure_schema_version_alias(evolved.version_id())
        .unwrap();
    let mut projected = original.clone();
    let mut values = projected.record.to_values().unwrap();
    values[HistoryRowRecord::FIELD_SCHEMA_VERSION_IDX] = Value::U64(alias.0);
    projected.record = owned_record_from_storage_values_with_descriptor(
        owner
            .table_in_schema_ref("todos", evolved.version_id())
            .unwrap()
            .history_storage_table()
            .record_schema(),
        values,
    )
    .unwrap();

    let canonical = owner
        .canonical_history_version_for_maintained_witness(&projected)
        .unwrap();
    assert_eq!(
        canonical.schema_version_alias(),
        original.schema_version_alias()
    );
    assert_eq!(canonical, original);
}

#[test]
fn wire_commit_units_preserve_node_and_schema_uuids_not_local_aliases() {
    let schema = schema();
    let (_writer_dir, mut writer) = open_node_with_schema(node(0x4a), schema.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(0x4b), schema.clone());
    let (parent, parent_unit) = writer
        .commit_mergeable_unit_settled(
            MergeableCommit::new("todos", row(0x4a), 10).cells(title_cells("parent")),
        )
        .unwrap();
    core.apply_sync_message_settled(parent_unit).unwrap();
    let (child_tx, unit) = writer
        .commit_mergeable_unit_settled(
            MergeableCommit::new("todos", row(0x4a), 11)
                .cells(title_cells("child")),
        )
        .unwrap();
    let SyncMessage::CommitUnit { tx, versions } = &unit else {
        panic!("commit unit expected");
    };
    assert_eq!(tx.tx_id.node, node(0x4a));
    assert_eq!(versions[0].schema_version(), schema.version_id());
    // Linear history: versions carry no parents; the wire identity is the
    // transaction's node UUID, checked above.
    assert!(versions[0].parents().is_empty());
    let _ = parent;

    core.apply_sync_message_settled(unit).unwrap();
    assert_ne!(
        writer.node_aliases[&node(0x4a)],
        core.node_aliases[&node(0x4a)],
        "replicas deliberately compress the same wire node UUID with independent local aliases"
    );
    let stored = core.query_table_versions("todos").unwrap();
    let child_row = stored
        .iter()
        .find(|version| core.version_tx_id(version).unwrap() == child_tx)
        .unwrap();
    let stored_wire = core.version_record_from_row(child_row).unwrap();
    assert_eq!(stored_wire.schema_version(), schema.version_id());
}
use crate::node::query_engine::QueryAuthorizationMode;

/// Alice's A/B-authored write keeps its immutable identity when Bob's persistent
/// owner serves an A/B read to Carol after additive migration and reopen.
/// Alice/core(A) -> Bob/owner(A or B) -> Carol/follower(A).
/// Internal because the precise wire VersionRecord identity is not exposed by
/// client row APIs; all storage and catalogue operations use real node storage.
#[test]
fn additive_migration_owner_follower_preserves_complete_commit_identity() {
    migration_owner_follower_cases("add-table");
}

/// Alice authors either schema while Bob relays an old/new read to Carol.
/// Column payload changes and table renames must preserve immutable history.
/// Alice -> Bob's authority-covered input -> Carol's existing complete unit.
/// Internal because public rows cannot expose wire history identity.
#[test]
fn migration_projection_owner_follower_preserves_authored_payload() {
    for kind in ["add-column", "rename-column", "rename-table"] {
        migration_owner_follower_cases(kind);
    }
}

fn migration_owner_follower_cases(kind: &str) {
    let base = schema();
    let target_table = if kind == "rename-table" {
        "tasks"
    } else {
        "todos"
    };
    let target_column = if kind == "rename-column" {
        "name"
    } else {
        "title"
    };
    let mut table =
        PublicTableSchemaBuilder::new(target_table).column(target_column, PublicColumnType::Text);
    if kind == "add-column" {
        table = table.column("body", PublicColumnType::Text);
    }
    let mut builder = PublicSchemaBuilder::new().table(table);
    if kind == "add-table" {
        builder = builder.table(
            PublicTableSchemaBuilder::new("controls").column("value", PublicColumnType::Text),
        );
    }
    let evolved = build_public_test_schema(builder);
    let ops = match kind {
        "add-column" => vec![LensOp::AddColumn {
            column: "body".to_owned(),
            default: v("default-body"),
        }],
        "rename-column" => vec![LensOp::RenameColumn {
            from: "title".to_owned(),
            to: "name".to_owned(),
        }],
        "rename-table" => vec![LensOp::RenameTable {
            from: "todos".to_owned(),
            to: "tasks".to_owned(),
        }],
        _ => vec![],
    };
    let mut failures = Vec::new();
    for read_new in [false, true] {
        for authored_new in [false, true] {
            for owner_uses_new_schema in [false, true] {
                for reopen in [false, true] {
                    // The renamed-owner bootstrap separately rejects retained old
                    // table payloads before relay publication (TableNotFound).
                    // This probe isolates publication with the original owner.
                    if kind == "rename-table" && owner_uses_new_schema {
                        continue;
                    }
                    eprintln!(
                        "starting kind={kind}, read_new={read_new}, authored_new={authored_new}, owner_new={owner_uses_new_schema}, reopen={reopen}"
                    );
                    let (_core_dir, mut alice) = open_node_with_schema(node(0xa1), base.clone());
                    let old_tx = if !authored_new {
                        Some(
                            alice
                                .commit_mergeable_settled(
                                    MergeableCommit::new("todos", row(0xa1), 10)
                                        .cells(title_cells("retained")),
                                )
                                .unwrap(),
                        )
                    } else {
                        None
                    };
                    publish_schema_lineage(
                        &mut alice,
                        SchemaVersion::new(evolved.clone()),
                        MigrationLens::new(
                            base.version_id(),
                            evolved.version_id(),
                            vec![TableLens {
                                source_table: "todos".to_owned(),
                                target_table: target_table.to_owned(),
                                ops: ops.clone(),
                            }],
                        )
                        .unwrap(),
                        if kind == "add-table" {
                            vec!["controls".to_owned()]
                        } else {
                            vec![]
                        },
                        Vec::<String>::new(),
                    )
                    .unwrap();
                    alice.activate_catalogue_schema_settled(CurrentWriteSchema {
                        revision: 1,
                        schema: evolved.version_id(),
                    }).unwrap();
                    let tx = if let Some(tx) = old_tx {
                        tx
                    } else {
                        alice
                            .commit_mergeable_in_schema_settled(
                                if authored_new {
                                    evolved.version_id()
                                } else {
                                    base.version_id()
                                },
                                MergeableCommit::new(
                                    if authored_new { target_table } else { "todos" },
                                    row(0xa1),
                                    10,
                                )
                                .cells({
                                    let mut cells = BTreeMap::from([(
                                        if authored_new { target_column } else { "title" }
                                            .to_owned(),
                                        v("retained"),
                                    )]);
                                    if authored_new && kind == "add-column" {
                                        cells.insert("body".to_owned(), v("new-body"));
                                    }
                                    cells
                                }),
                            )
                            .unwrap()
                    };
                    alice.accept_global_for_test(tx).unwrap();
                    let canonical = alice.commit_unit_for(tx).unwrap();
                    let owner_schema = if owner_uses_new_schema {
                        evolved.clone()
                    } else {
                        base.clone()
                    };
                    let (bob_dir, mut bob) =
                        open_node_with_schema(node(0xa2), owner_schema.clone());
                    bob.apply_trusted_catalogue_snapshot_settled(
                        alice.catalogue_snapshot().unwrap(),
                    )
                    .unwrap();
                    let SyncMessage::CommitUnit {
                        tx: header,
                        versions,
                    } = canonical.clone()
                    else {
                        panic!("commit")
                    };
                    bob.ingest_known_transaction(
                        header,
                        versions.clone(),
                        Fate::Accepted,
                        Some(GlobalTime(1)),
                        DurabilityTier::Global,
                    )
                    .unwrap();
                    if reopen {
                        drop(bob);
                        bob = reopen_node_at(&bob_dir, node(0xa2), owner_schema);
                    }
                    let (_carol_dir, mut carol) = open_node_with_schema(node(0xa3), base.clone());
                    carol
                        .apply_trusted_catalogue_snapshot_settled(
                            alice.catalogue_snapshot().unwrap(),
                        )
                        .unwrap();
                    let SyncMessage::CommitUnit {
                        tx: header,
                        versions: original,
                    } = canonical.clone()
                    else {
                        panic!("commit")
                    };
                    carol
                        .ingest_known_transaction(
                            header,
                            original,
                            Fate::Accepted,
                            Some(GlobalTime(1)),
                            DurabilityTier::Global,
                        )
                        .unwrap();
                    let shape = Query::from(if read_new { target_table } else { "todos" })
                        .validate(if read_new { &evolved } else { &base })
                        .unwrap();
                    let binding = shape.bind(BTreeMap::new()).unwrap();
                    register_shape_binding(&mut bob, &shape, &binding);
                    let mut upstream = PeerState::new();
                    let upstream_update = upstream
                        .rehydrate_query_with_opts(
                            &mut alice,
                            &shape,
                            &binding,
                            RegisterShapeOptions::default(),
                        )
                        .unwrap();
                    bob.apply_sync_message_settled(upstream_update).unwrap();
                    let subscription = SubscriptionKey {
                        shape_id: shape.shape_id(),
                        binding_id: binding.binding_id(),
                        read_view: Default::default(),
                    };
                    let source = bob
                        .authority_result_key_for_subscription(subscription)
                        .unwrap();
                    let mut peer = PeerState::new();
                    peer.set_subscription_authority_result_source(subscription, source);
                    peer.set_subscription_awaiting_selected_authority_source(subscription, true);
                    let update = peer
                        .rehydrate_query_with_opts(
                            &mut bob,
                            &shape,
                            &binding,
                            RegisterShapeOptions::default(),
                        )
                        .unwrap();
                    let bundles = version_bundles_for_update(&update);
                    assert_eq!(bundles.len(), 1);
                    assert_eq!(
                        bundles[0].scope,
                        crate::protocol::VersionBundleScope::CompleteTransaction
                    );
                    assert_eq!(bundles[0].versions.len(), 1);
                    let incoming = &bundles[0].versions[0];
                    assert_eq!(incoming.record().raw(), versions[0].record().raw());
                    let identity_preserved = incoming == &versions[0];
                    register_shape_binding(&mut carol, &shape, &binding);
                    let received = carol.apply_sync_message_settled(update);
                    let case = format!(
                        "kind={kind}, read_new={read_new}, authored_new={authored_new}, owner_new={owner_uses_new_schema}, reopen={reopen}"
                    );
                    eprintln!(
                        "{case}: identity_preserved={identity_preserved}, follower={received:?}"
                    );
                    if !identity_preserved || received.is_err() {
                        failures.push(case);
                    }
                }
            }
        }
    }
    assert!(
        failures.is_empty(),
        "immutable identity/follower failures: {failures:?}"
    );
}

/// Alice writes one row UUID in two branches in one transaction; Bob's renamed
/// read witnesses must reload each exact supplying branch rather than first-match.
/// Alice/todos(branch1,branch2) -> Bob/tasks projected witnesses -> authored rows.
/// Internal because this tests the canonical history lookup at its witness seam.
#[test]
fn renamed_maintained_witness_preserves_branch_within_one_transaction() {
    let schema_for = |name| {
        build_public_test_schema(
            PublicSchemaBuilder::new().table(
                PublicTableSchemaBuilder::new(name)
                    .column("branch_id", PublicColumnType::Uuid)
                    .column("title", PublicColumnType::Text)
                    .branch_by("branch_id"),
            ),
        )
    };
    let base = schema_for("todos");
    let evolved = schema_for("tasks");
    let (_dir, mut alice) = open_node_with_schema(node(0xb1), base.clone());
    let tx = alice
        .commit_mergeable_many_settled(vec![
            MergeableCommit::new("todos", row(0xb1), 10)
                .branch(branch_selector(1))
                .cells(title_cells("first")),
            MergeableCommit::new("todos", row(0xb1), 10)
                .branch(branch_selector(2))
                .cells(title_cells("second")),
        ])
        .unwrap();
    publish_schema_lineage(
        &mut alice,
        SchemaVersion::new(evolved.clone()),
        MigrationLens::new(
            base.version_id(),
            evolved.version_id(),
            vec![TableLens {
                source_table: "todos".to_owned(),
                target_table: "tasks".to_owned(),
                ops: vec![LensOp::RenameTable {
                    from: "todos".to_owned(),
                    to: "tasks".to_owned(),
                }],
            }],
        )
        .unwrap(),
        Vec::<String>::new(),
        Vec::<String>::new(),
    )
    .unwrap();
    let originals = alice.query_versions_for_tx(tx).unwrap();
    assert_eq!(originals.len(), 2);
    for original in originals {
        let mut projected = original.clone();
        // Maintained logical table name changes; immutable stored identity does not.
        projected.table = "tasks".to_owned().into();
        let canonical = alice
            .canonical_history_version_for_maintained_witness(&projected)
            .unwrap();
        assert_eq!(canonical.branch_key(), original.branch_key());
        assert_eq!(canonical, original);
    }
}
