// Catalogue replication, orphan draining, and malformed authored-row rejection.

#[test]
fn catalogue_schema_publish_replicates_and_is_idempotent() {
    let base = schema();
    let evolved = catalogue_evolved_schema();
    let (_core_dir, mut core) = open_node_with_schema(node(0x33), base.clone());
    let (_client_dir, mut client) = open_node_with_schema(node(0x34), base.clone());
    let payload = SchemaVersion::new(evolved.clone());
    // A lineage carries only descendant identities; receivers first learn the
    // authority's permanent genesis manifest through its catalogue snapshot.
    client
        .apply_trusted_catalogue_snapshot_settled(core.catalogue_snapshot().unwrap())
        .unwrap();
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
    let publication = core
        .author_schema_lineage_publication(
            payload.clone(),
            lens,
            Vec::<String>::new(),
            Vec::<String>::new(),
        )
        .unwrap();
    let publish = SyncMessage::PublishSchemaWithLens {
        author: AuthorSubject::SYSTEM,
        catalogue_seq: 1,
        publication: Box::new(publication),
    };

    let ack = core
        .apply_trusted_catalogue_message_settled(publish.clone())
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
        .apply_trusted_catalogue_message_settled(publish.clone())
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

    client.apply_trusted_catalogue_message_settled(publish).unwrap();
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

    let publication = core
        .author_schema_lineage_publication(
            target.clone(),
            lens.clone(),
            Vec::<String>::new(),
            Vec::<String>::new(),
        )
        .unwrap();
    let non_admin = core.apply_sync_message_settled(SyncMessage::PublishSchemaWithLens {
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
    let unknown_result = core.apply_trusted_catalogue_message_settled(SyncMessage::PublishLens {
        author: AuthorSubject::SYSTEM,
        lens: unknown,
    });
    assert!(matches!(
        unknown_result,
        Err(Error::InvalidCatalogueUpdate("lens endpoint is unknown"))
    ));

    let ack = core
        .apply_trusted_catalogue_message_settled(SyncMessage::PublishSchemaWithLens {
            author: AuthorSubject::SYSTEM,
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
        .commit_mergeable_unit_settled(
            MergeableCommit::new("todos", row(0x55), 1_000).cells(evolved_cells.clone()),
        )
        .unwrap();
    let SyncMessage::CommitUnit { tx, versions } = unit else {
        panic!("commit unit expected");
    };

    assert!(
        core.apply_sync_message_settled(SyncMessage::CommitUnit {
            tx: tx.clone(),
            versions: versions.clone(),
        })
        .unwrap()
        .is_empty()
    );
    assert_eq!(core.sync_metrics().parked_catalogue_orphans, 1);
    assert!(core.query_transaction(tx.tx_id).unwrap().is_none());

    // Parking an unavailable authored unit never decodes it as a normal
    // transaction. A process restart therefore relies on the peer's canonical
    // reconnect retransmission; receiving that same unit again before the
    // lineage is active recreates the parked obligation rather than exposing
    // an incomplete row.
    drop(core);
    let mut core = reopen_node_at(&core_dir, node(0x37), base.clone());
    assert!(core.query_transaction(tx.tx_id).unwrap().is_none());
    assert!(
        core.apply_sync_message_settled(SyncMessage::CommitUnit {
            tx: tx.clone(),
            versions: versions.clone(),
        })
        .unwrap()
        .is_empty()
    );
    assert_eq!(core.sync_metrics().parked_catalogue_orphans, 1);

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
    // A reconnect can retransmit the exact authoritative unit after the
    // catalogue activation that originally drained it. The public storage and
    // projected read receipts remain singular: activation plus retry must not
    // create a second history record or a second projected current row.
    reopened
        .apply_sync_message_settled(SyncMessage::CommitUnit { tx, versions })
        .unwrap();
    assert_eq!(reopened.query_table_versions("todos").unwrap().len(), 1);
    assert_eq!(
        reopened
            .query_rows(&shape, &binding, DurabilityTier::Global)
            .unwrap()
            .into_iter()
            .map(current_row_pair)
            .collect::<BTreeMap<_, _>>()
            .get(&row(0x55)),
        Some(&evolved_cells),
        "reconnect retry must preserve the one projected result"
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
        .commit_mergeable_unit_settled(
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
                version.created_at_ms(),
                version.updated_by(),
                version.updated_at_ms(),
                &version_record_cells(&version, &base.tables[0]),
                version.deletion(),
            )
            .unwrap()
        })
        .collect();

    assert!(core
        .apply_sync_message_settled(SyncMessage::CommitUnit {
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
        .commit_mergeable_unit_settled(
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
                version.created_at_ms(),
                version.updated_by(),
                version.updated_at_ms(),
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
        version.created_at_ms(),
        version.updated_by(),
        version.updated_at_ms(),
        &BTreeMap::<String, Value>::new(),
        version.deletion(),
    )
    .unwrap()
}

/// Deliberately bypasses `VersionRecord::encode` to model an untrusted
/// ViewUpdate whose deferred record cannot satisfy even the fixed row receipt.
/// This stays an internal test because public APIs cannot construct malformed
/// protocol bytes; the receiver boundary must still turn them into a typed
/// error rather than reaching infallible accessors.
fn empty_wire_version_claiming_schema(
    schema: &JazzSchema,
    version: &crate::protocol::VersionRecord,
) -> crate::protocol::VersionRecord {
    crate::protocol::VersionRecord::new(
        version.table().to_owned(),
        schema.version_id(),
        OwnedRecord::new(Vec::new(), schema.tables[0].wire_record_descriptor()),
    )
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
    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
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
    }) = update
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
    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
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
    }) = update
    else {
        panic!("expected view update");
    };

    let (_reader_dir, mut reader) = open_node_with_schema(node(0x6f), base);
    assert!(matches!(
        reader.apply_sync_message_settled(SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
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
        })),
        Err(Error::MalformedViewUpdate(
            "row version does not carry the complete descriptor of its authored schema"
        ))
    ));
    assert!(reader.query_all_versions().unwrap().is_empty());
}

/// Direct internal view ingress bypasses `SyncMessage` decoding. It must still
/// reject a deferred record that would panic in `VersionRecord::row_uuid()`;
/// malformed protocol input is a typed error and cannot leave history behind.
#[test]
fn direct_view_update_rejects_malformed_deferred_record_without_panicking() {
    let base = schema();
    let (_core_dir, mut core) = open_node_with_schema(node(0x77), base.clone());
    accept_global(
        &mut core,
        MergeableCommit::new("todos", row(0x77), 1_011).cells(title_cells("bad receipt")),
    );
    let update = PeerState::new()
        .current_rows_update(&mut core, "todos")
        .unwrap();
    let mut bundles = version_bundles_for_update(&update);
    let version = bundles[0].versions[0].clone();
    bundles[0].versions = vec![empty_wire_version_claiming_schema(&base, &version)];
    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
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
    }) = update
    else {
        panic!("expected view update");
    };

    let (_reader_dir, mut reader) = open_node_with_schema(node(0x78), base);
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        reader
            .apply_view_update(ViewUpdateParts {
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
            })
            .resolve()
    }));
    assert!(result.is_ok(), "malformed direct ingress must not panic");
    assert!(matches!(
        result.unwrap(),
        Err(Error::MalformedViewUpdate("malformed version receipt"))
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
        reader
            .apply_row_version_payloads_for_requests(&[request], bundles)
            .resolve(),
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
    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        subscription,
        settled_through,
        peer_payload_inventory,
        result_member_adds,
        result_member_removes,
        terminal_operations,
        program_fact_adds,
        program_fact_removes,
        ..
    }) = update
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
        }])
        .resolve(),
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
    bundles[1].versions = vec![empty_wire_version_claiming_schema(&base, &malformed)];
    let valid_tx_id = bundles[0].tx.tx_id;
    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        subscription,
        settled_through,
        peer_payload_inventory,
        result_member_adds,
        result_member_removes,
        terminal_operations,
        program_fact_adds,
        program_fact_removes,
        ..
    }) = update
    else {
        panic!("expected view update");
    };

    let (_reader_dir, mut reader) = open_node_with_schema(node(0x76), base);
    let clock_before = (
        reader.clock.tx_time,
        reader.clock.global_time_register,
        reader.clock.committed_global_time,
        reader.clock.applied_global_times_after_frontier.clone(),
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
        }])
        .resolve(),
        Err(Error::MalformedViewUpdate("malformed version receipt"))
    ));
    assert_eq!(
        (
            reader.clock.tx_time,
            reader.clock.global_time_register,
            reader.clock.committed_global_time,
            reader.clock.applied_global_times_after_frontier.clone(),
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
