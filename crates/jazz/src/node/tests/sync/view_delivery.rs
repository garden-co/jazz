// Downstream version delivery, payload inventory, wire records, and dispatch.

#[test]
fn peer_view_updates_reject_authority_output_before_receiver_state_changes() {
    let (_reader_dir, mut reader) = open_node_with_uuid(node(0x61));
    let subscription = reader.whole_table_subscription_key("todos").unwrap();
    let member = crate::protocol::ResultMemberEntry::row((
        groove::Intern::new("todos".to_owned()),
        row(0x62),
        TxId::new(TxTime::from(1), node(0x63)),
    ));
    let base = || ViewUpdateParts {
        subscription,
        settled_through: GlobalTime(0),
        defer_settlement: false,
        reset_result_set: true,
        version_carriers: Vec::new(),
        peer_complete_tx_payload_refs: Vec::new(),
        authorization_progress: None,
        opening_pending: false,
        result_member_adds: Vec::new(),
        result_member_removes: Vec::new(),
        program_fact_adds: Vec::new(),
        program_fact_removes: Vec::new(),
    };

    let mut member_update = base();
    member_update.result_member_adds.push(member.clone());
    assert!(matches!(
        crate::db::block_on(reader.apply_view_update(member_update)),
        Err(Error::InvalidStoredValue("peer view update must not carry authority result members"))
    ));

    let mut payload_update = base();
    payload_update
        .program_fact_adds
        .push(crate::protocol::ProgramFactEntry::ResultPayload(
            crate::protocol::ResultMemberPayloadEntry {
                member,
                // Ingestion must reject this by kind before attempting to
                // interpret the payload descriptor or bytes.
                descriptor: Vec::new(),
                record: Vec::new(),
            },
        ));
    assert!(matches!(
        crate::db::block_on(reader.apply_view_update(payload_update)),
        Err(Error::InvalidStoredValue(
            "peer view update must carry only covered-input closure facts"
        ))
    ));
    assert!(reader
        .subscription_current_rows("todos", DurabilityTier::Local)
        .unwrap()
        .is_empty());
}

#[test]
fn view_updates_ship_current_versions_to_downstream_nodes() {
    let (_writer_dir, mut writer) = open_node_with_uuid(node(1));
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    let (_reader_dir, mut reader) = open_node_with_uuid(node(3));
    let row = row(7);

    let (_, commit_unit) = writer
        .commit_mergeable_unit_settled(
            MergeableCommit::new("todos", row, 10).cells(BTreeMap::from([(
                "title".to_owned(),
                "replicate me".to_owned(),
            )])),
        )
        .unwrap();
    let SyncMessage::CommitUnit { tx, versions } = commit_unit else {
        panic!("expected commit unit");
    };
    core.ingest_commit_unit_settled(tx, versions, u64::MAX - SKEW_TOLERANCE_MS)
        .unwrap();

    let (shape, binding) = reader.whole_table_shape_binding("todos").unwrap();
    register_shape_binding(&mut reader, &shape, &binding);
    let subscription = reader.whole_table_subscription_key("todos").unwrap();
    let update = system_authority_reset(&mut core, &shape, &binding, subscription);
    let version_bundles = version_bundles_for_update(&update);
    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        subscription,
        settled_through,
        reset_result_set,
        peer_payload_inventory:
            crate::protocol::PeerPayloadInventory {
                complete_tx_payloads: peer_payload_inventory_refs, ..
            },
        result_member_adds,
        result_member_removes,
        program_fact_adds,
        program_fact_removes,
        ..
    }) = update
    else {
        panic!("expected view update");
    };
    assert_eq!(
        subscription,
        core.whole_table_subscription_key("todos").unwrap()
    );
    assert!(reset_result_set);
    assert!(
        result_member_adds.is_empty(),
        "peer frames carry source/version closure, never authority result membership"
    );
    assert!(result_member_removes.is_empty());
    assert_eq!(version_bundles.len(), 1);
    assert!(peer_payload_inventory_refs.is_empty());

    reader
        .apply_view_update(ViewUpdateParts {
            subscription,
            settled_through,
            defer_settlement: false,
            reset_result_set,
            version_carriers: crate::protocol::build_version_carriers_from_singletons(
                version_bundles,
            )
            .unwrap(),
            peer_complete_tx_payload_refs: peer_payload_inventory_refs,
            authorization_progress: None,
            opening_pending: false,
            result_member_adds,
            result_member_removes,
            program_fact_adds,
            program_fact_removes,
        })
        .unwrap();

    assert_eq!(
        receiver_rows(&mut reader, &shape, &binding, DurabilityTier::Local)
            .into_iter()
            .map(current_row_pair)
            .collect::<BTreeMap<_, _>>(),
        BTreeMap::from([(row, title_cells("replicate me"))])
    );
}

#[test]
/// A global whole-table rehydration ignores a newer, unacknowledged local write.
///
/// `other` publishes the globally accepted value to `core`; `core` then makes a
/// higher-HLC local-only write for the same row. `reader` must receive the
/// authoritative global value and the global settled position, rather than the
/// local speculative value.
///
/// other ──accepted──► core ──Global view──► reader
///                         ▲
///                    local pending write
fn global_read_ignores_a_newer_unacknowledged_local_write() {
    let (_other_dir, mut other) = open_node_with_uuid(node(2));
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    let (_reader_dir, mut reader) = open_node_with_uuid(node(3));
    let target = row(0x72);

    let (_, authoritative_unit) = other
        .commit_mergeable_unit_settled(
            MergeableCommit::new("todos", target, 20)
                .cells(title_cells("authoritative remote")),
        )
        .unwrap();
    let [authoritative_fate] = core
        .apply_sync_message_settled(authoritative_unit)
        .unwrap()
        .try_into()
        .unwrap();
    let SyncMessage::FateUpdate {
        global_time: Some(authoritative_seq),
        ..
    } = authoritative_fate
    else {
        panic!("expected globally accepted authoritative fate");
    };

    let (pending_tx, _pending_unit) = core
        .commit_mergeable_unit_settled(
            MergeableCommit::new("todos", target, 30).cells(title_cells("pending local")),
        )
        .unwrap();
    assert_eq!(
        core.transaction_state_settled(pending_tx).unwrap().0,
        Fate::Pending
    );
    assert_eq!(
        core.current_rows("todos", DurabilityTier::Local)
            .unwrap()
            .into_iter()
            .map(current_row_pair)
            .collect::<BTreeMap<_, _>>(),
        BTreeMap::from([(target, title_cells("pending local"))])
    );
    assert_eq!(
        core.current_rows("todos", DurabilityTier::Global)
            .unwrap()
            .into_iter()
            .map(current_row_pair)
            .collect::<BTreeMap<_, _>>(),
        BTreeMap::from([(target, title_cells("authoritative remote"))])
    );

    let mut link = PeerState::client_link(AuthorSubject::SYSTEM);
    let update = link.current_rows_update(&mut core, "todos").unwrap();
    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        settled_through, ..
    }) = &update
    else {
        panic!("expected view update");
    };
    assert_eq!(*settled_through, authoritative_seq);
    let (shape, binding) = reader.whole_table_shape_binding("todos").unwrap();
    register_shape_binding(&mut reader, &shape, &binding);
    reader.apply_sync_message_settled(update).unwrap();

    assert_eq!(
        reader
            .subscription_current_rows("todos", DurabilityTier::Local)
            .unwrap()
            .into_iter()
            .map(current_row_pair)
            .collect::<BTreeMap<_, _>>(),
        BTreeMap::from([(target, title_cells("authoritative remote"))])
    );
}
#[test]
fn view_updates_use_peer_payload_inventory_refs_for_previously_shipped_complete_payloads() {
    let (_writer_dir, mut writer) = open_node_with_uuid(node(1));
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    let (_reader_dir, mut reader) = open_node_with_uuid(node(3));
    let row = row(7);

    let (tx_id, commit_unit) = writer
        .commit_mergeable_unit_settled(MergeableCommit::new("todos", row, 10).cells(title_cells("known")))
        .unwrap();
    let SyncMessage::CommitUnit { tx, versions } = commit_unit else {
        panic!("expected commit unit");
    };
    core.ingest_commit_unit_settled(tx, versions, u64::MAX - SKEW_TOLERANCE_MS)
        .unwrap();

    let (shape, binding) = reader.whole_table_shape_binding("todos").unwrap();
    register_shape_binding(&mut reader, &shape, &binding);
    let subscription = reader.whole_table_subscription_key("todos").unwrap();
    let initial = system_authority_reset(&mut core, &shape, &binding, subscription);
    let version_bundles = version_bundles_for_update(&initial);
    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        subscription,
        settled_through,
        reset_result_set,
        peer_payload_inventory:
            crate::protocol::PeerPayloadInventory {
                complete_tx_payloads: peer_payload_inventory_refs, ..
            },
        result_member_adds,
        result_member_removes,
        program_fact_adds,
        program_fact_removes,
        ..
    }) = initial
    else {
        panic!("expected view update");
    };
    assert!(reset_result_set);
    reader
        .apply_view_update(ViewUpdateParts {
            subscription,
            settled_through,
            defer_settlement: false,
            reset_result_set,
            version_carriers: crate::protocol::build_version_carriers_from_singletons(
                version_bundles,
            )
            .unwrap(),
            peer_complete_tx_payload_refs: peer_payload_inventory_refs,
            authorization_progress: None,
            opening_pending: false,
            result_member_adds,
            result_member_removes,
            program_fact_adds,
            program_fact_removes,
        })
        .unwrap();

    let deduped = core
        .view_update_for_current_rows_with_peer_payload_inventory(
            "todos",
            core.whole_table_subscription_key("todos").unwrap(),
            [tx_id],
            [],
            [],
            AuthorSubject::SYSTEM,
        )
        .unwrap();
    let version_bundles = version_bundles_for_update(&deduped);
    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        settled_through,
        peer_payload_inventory:
            crate::protocol::PeerPayloadInventory {
                complete_tx_payloads: peer_payload_inventory_refs, ..
            },
        result_member_adds,
        result_member_removes,
        program_fact_adds,
        program_fact_removes,
        ..
    }) = deduped
    else {
        panic!("expected view update");
    };
    assert!(version_bundles.is_empty());
    assert_eq!(peer_payload_inventory_refs, vec![tx_id]);
    assert!(result_member_adds.is_empty());
    assert!(result_member_removes.is_empty());
    assert!(program_fact_adds.iter().any(|fact| matches!(
        fact,
        crate::protocol::ProgramFactEntry::CoveredInput(input)
            if input.source_row == row && input.version.tx == tx_id
    )));
    reader
        .apply_view_update(ViewUpdateParts {
            subscription: core.whole_table_subscription_key("todos").unwrap(),
            settled_through,
            defer_settlement: false,
            reset_result_set: false,
            version_carriers: crate::protocol::build_version_carriers_from_singletons(
                version_bundles,
            )
            .unwrap(),
            peer_complete_tx_payload_refs: peer_payload_inventory_refs,
            authorization_progress: None,
            opening_pending: false,
            result_member_adds,
            result_member_removes,
            program_fact_adds,
            program_fact_removes,
        })
        .unwrap();
}
#[test]
fn view_updates_downgrade_unknown_peer_payload_inventory_refs() {
    let (_reader_dir, mut reader) = open_node_with_uuid(node(3));
    let missing = TxId {
        node: node(1),
        time: TxTime::from(99),
    };

    reader
        .apply_view_update(ViewUpdateParts {
            subscription: reader.whole_table_subscription_key("todos").unwrap(),
            settled_through: GlobalTime(0),
            defer_settlement: false,
            reset_result_set: false,
            version_carriers: Vec::new(),
            peer_complete_tx_payload_refs: vec![missing],
            authorization_progress: None,
            opening_pending: false,
            result_member_adds: Vec::new(),
            result_member_removes: Vec::new(),
            program_fact_adds: Vec::new(),
            program_fact_removes: Vec::new(),
        })
        .unwrap();

    assert_eq!(
        reader
            .sync_metrics()
            .peer_payload_inventory_missing_fallbacks,
        1
    );
    assert_eq!(reader.sync_metrics().parked_orphans, 0);
}
#[test]
fn wire_record_round_trips_through_history_bytes() {
    let (_writer_dir, mut writer) = open_node_with_uuid(node(1));
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    let row = row(7);
    let (_tx_id, message) = writer
        .commit_mergeable_unit_settled(MergeableCommit::new("todos", row, 10).cells(title_cells("wire")))
        .unwrap();
    let SyncMessage::CommitUnit { tx, versions } = message else {
        panic!("expected commit unit");
    };
    let original = versions[0].clone();
    assert_eq!(original.created_at_ms(), 10);
    assert_eq!(original.updated_at_ms(), 10);
    core.ingest_commit_unit_settled(tx, versions, u64::MAX - SKEW_TOLERANCE_MS)
        .unwrap();
    let stored = core.query_row_versions("todos", row).unwrap();
    assert_eq!(stored[0].created_at(), TxTime::from(10));
    assert_eq!(stored[0].updated_at(), TxTime::from(10));
    let projected = core.version_record_from_row(&stored[0]).unwrap();
    assert_eq!(projected.table(), original.table());
    assert_eq!(projected.record().raw(), original.record().raw());
}
#[test]
fn sync_message_dispatches_commit_fate_and_view_updates() {
    let (_writer_dir, mut writer) = open_node_with_uuid(node(1));
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    let (_reader_dir, mut reader) = open_node_with_uuid(node(3));
    let row = row(7);

    let (tx_id, commit_unit) = writer
        .commit_mergeable_unit_settled(
            MergeableCommit::new("todos", row, 10).cells(BTreeMap::from([(
                "title".to_owned(),
                "dispatch".to_owned(),
            )])),
        )
        .unwrap();

    let out = core.apply_sync_message_settled(commit_unit).unwrap();
    let [fate_update] = out.as_slice() else {
        panic!("expected one fate update");
    };
    writer.apply_sync_message_settled(fate_update.clone()).unwrap();
    let (fate, _, _) = writer.transaction_state_settled(tx_id).unwrap();
    assert_eq!(fate, Fate::Accepted);

    let (shape, binding) = reader.whole_table_shape_binding("todos").unwrap();
    register_shape_binding(&mut reader, &shape, &binding);
    let subscription = reader.whole_table_subscription_key("todos").unwrap();
    let view_update = system_authority_reset(&mut core, &shape, &binding, subscription);
    assert!(reader.apply_sync_message_settled(view_update).unwrap().is_empty());
    assert_eq!(
        receiver_rows(&mut reader, &shape, &binding, DurabilityTier::Local)
            .into_iter()
            .map(current_row_pair)
            .collect::<BTreeMap<_, _>>(),
        BTreeMap::from([(row, title_cells("dispatch"))])
    );
}
#[test]
fn duplicate_commit_units_compare_versions_without_wire_order() {
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    let tx = Transaction {
        tx_id: TxId::new(TxTime::from(10), node(1)),
        kind: TxKind::Mergeable,
        n_total_writes: 2,
        made_by: AuthorSubject::SYSTEM,
        permission_subject: None,
        base_snapshot: None,
        row_read_set: None,
        absent_read_set: None,
        predicate_read_set: None,
        user_metadata_json: None,
        contribution_merge: None,
    };
    let versions = vec![
        version_record(row(1), Vec::new(), title_cells("a"), None),
        version_record(row(2), Vec::new(), title_cells("b"), None),
    ];
    core.ingest_commit_unit_settled(tx.clone(), versions.clone(), u64::MAX - SKEW_TOLERANCE_MS)
        .unwrap();
    let mut reversed = versions;
    reversed.reverse();

    assert!(
        core.ingest_commit_unit_settled(tx, reversed, u64::MAX - SKEW_TOLERANCE_MS)
            .is_ok()
    );
}
/// A locally pending partial update is rebuilt from durable history after
/// `bob` reopens, then uploaded as a real commit unit. Its explicitly authored
/// `title` wins while `alice`'s concurrent `completed` edit survives.
///
/// ```text
/// base(base,false) ─┬─ alice(completed=true) ──┐
///                   └─ bob(title=base) ─ reopen ─ upload ─┴─► base,true
/// ```
///
/// Planted positive: force `VersionRecord::from_stored` to attach
/// `authored_columns=None`. Bob's materialized `completed=false` then appears
/// authored on the rebuilt wire unit and this test fails.
#[test]
fn reopened_pending_partial_update_upload_preserves_authored_columns() {
    let schema = build_public_test_schema(PublicSchemaBuilder::new().table(
        PublicTableSchemaBuilder::new("todos")
            .column("title", PublicColumnType::Text)
            .column("completed", PublicColumnType::Boolean),
    ));
    let (bob_dir, mut bob) = open_node_with_schema(node(0x91), schema.clone());
    let (_alice_dir, mut alice) = open_node_with_schema(node(0x92), schema.clone());
    let (_core_dir, mut core) =
        open_history_complete_node_with_schema(node(0x93), schema.clone());
    let row_uuid = row(0x91);

    let (base, base_unit) = bob
        .commit_mergeable_unit_settled(
            MergeableCommit::new("todos", row_uuid, 10).cells(BTreeMap::from([
                ("title".to_owned(), Value::String("base".to_owned())),
                ("completed".to_owned(), Value::Bool(false)),
            ])),
        )
        .unwrap();
    let [base_fate] = core.apply_sync_message_settled(base_unit).unwrap().try_into().unwrap();
    bob.apply_sync_message_settled(base_fate).unwrap();

    let (_alice_tx, alice_unit) = alice
        .commit_mergeable_unit_settled(
            MergeableCommit::new("todos", row_uuid, 20)
                .parents(vec![base])
                .cells(BTreeMap::from([(
                    "completed".to_owned(),
                    Value::Bool(true),
                )])),
        )
        .unwrap();
    core.apply_sync_message_settled(alice_unit).unwrap();

    let bob_tx = bob
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row_uuid, 30)
                .parents(vec![base])
                .cells(BTreeMap::from([
                    ("title".to_owned(), Value::String("base".to_owned())),
                    ("completed".to_owned(), Value::Bool(false)),
                ]))
                .authored_columns(BTreeSet::from(["title".to_owned()])),
        )
        .unwrap();
    drop(bob);

    let mut reopened = reopen_node_at(&bob_dir, node(0x91), schema);
    let rebuilt = reopened.commit_unit_for(bob_tx).unwrap();
    core.apply_sync_message_settled(rebuilt).unwrap();

    let rows = core.current_rows("todos", DurabilityTier::Local).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].test_cells_by_descriptor(),
        BTreeMap::from([
            ("title".to_owned(), Value::String("base".to_owned())),
            ("completed".to_owned(), Value::Bool(true)),
        ])
    );
}
