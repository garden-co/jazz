// Stable wire UUID identity across local alias assignment.

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
    let (_child_tx, unit) = writer
        .commit_mergeable_unit_settled(
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

    core.apply_sync_message_settled(unit).unwrap();
    assert_ne!(
        writer.node_aliases[&node(0x4a)],
        core.node_aliases[&node(0x4a)],
        "replicas deliberately compress the same wire node UUID with independent local aliases"
    );
    let stored = core.query_table_versions("todos").unwrap();
    let child_row = stored
        .iter()
        .find(|version| version.parents().contains(&parent))
        .unwrap();
    let stored_wire = core.version_record_from_row(child_row).unwrap();
    assert_eq!(stored_wire.schema_version(), schema.version_id());
}
use crate::node::query_engine::QueryAuthorizationMode;

/// Alice's A/B-authored write keeps its immutable identity when Bob's persistent
/// owner serves an A-only read to Carol after additive migration and reopen.
/// Alice/core(A) -> Bob/owner(A or B) -> Carol/follower(A).
/// Internal because the precise wire VersionRecord identity is not exposed by
/// client row APIs; all storage and catalogue operations use real node storage.
#[test]
fn additive_migration_owner_follower_preserves_complete_commit_identity() {
    let base = schema();
    let evolved = build_public_test_schema(
        PublicSchemaBuilder::new()
            .table(PublicTableSchemaBuilder::new("todos").column("title", PublicColumnType::Text))
            .table(
                PublicTableSchemaBuilder::new("controls").column("value", PublicColumnType::Text),
            ),
    );
    let mut failures = Vec::new();
    for authored_new in [false, true] {
        for owner_uses_new_schema in [false, true] {
            for reopen in [false, true] {
                let (_core_dir, mut alice) = open_node_with_schema(node(0xa1), base.clone());
                publish_schema_lineage(
                    &mut alice,
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
                alice
                    .apply_trusted_catalogue_message_settled(SyncMessage::SetCurrentWriteSchema {
                        author: AuthorSubject::SYSTEM,
                        pointer: CurrentWriteSchema {
                            revision: 1,
                            schema: evolved.version_id(),
                        },
                    })
                    .unwrap();
                let tx = alice
                    .commit_mergeable_in_schema_settled(
                        if authored_new {
                            evolved.version_id()
                        } else {
                            base.version_id()
                        },
                        MergeableCommit::new("todos", row(0xa1), 10).cells(title_cells("retained")),
                    )
                    .unwrap();
                alice.accept_global_for_test(tx).unwrap();
                let canonical = alice.commit_unit_for(tx).unwrap();
                let owner_schema = if owner_uses_new_schema {
                    evolved.clone()
                } else {
                    base.clone()
                };
                let (bob_dir, mut bob) = open_node_with_schema(node(0xa2), owner_schema.clone());
                bob.apply_trusted_catalogue_snapshot_settled(alice.catalogue_snapshot().unwrap())
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
                    .apply_trusted_catalogue_snapshot_settled(alice.catalogue_snapshot().unwrap())
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
                let shape = Query::from("todos").validate(&base).unwrap();
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
                let subscription = bob.whole_table_subscription_key("todos").unwrap();
                let subscription = SubscriptionKey {
                    shape_id: shape.shape_id(),
                    binding_id: binding.binding_id(),
                    read_view: subscription.read_view,
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
                    "authored_new={authored_new}, owner_new={owner_uses_new_schema}, reopen={reopen}"
                );
                eprintln!("{case}: identity_preserved={identity_preserved}, follower={received:?}");
                if !identity_preserved || received.is_err() {
                    failures.push(case);
                }
            }
        }
    }
    assert!(
        failures.is_empty(),
        "immutable identity/follower failures: {failures:?}"
    );
}
