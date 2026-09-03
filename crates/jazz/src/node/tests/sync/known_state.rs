// Detached usage, exact/fast/slow known state, reopen, and eviction.

fn relay_with_system_binding(subscription: SubscriptionKey) -> PeerState {
    let mut peer = PeerState::relay();
    peer.set_subscription_policy_binding(subscription, (AuthorSubject::SYSTEM, BTreeMap::new()));
    peer
}

fn system_authority_reset(
    core: &mut NodeState<RocksDbStorage>,
    shape: &ValidatedQuery,
    binding: &Binding,
    subscription: SubscriptionKey,
) -> SyncMessage {
    let mut peer = relay_with_system_binding(subscription);
    peer.rehydrate_query_for_subscription_with_opts(
        core,
        subscription,
        shape,
        binding,
        RegisterShapeOptions::default(),
    )
    .expect("serve exact SYSTEM-scoped closure")
    .expect("authority reset must be available")
}

fn receiver_rows(
    node: &mut NodeState<RocksDbStorage>,
    shape: &ValidatedQuery,
    binding: &Binding,
    tier: DurabilityTier,
) -> Vec<CurrentRow> {
    node.query_rows_for_client(shape, binding, tier, AuthorSubject::SYSTEM)
        .resolve()
        .expect("derive receiver rows from the exact covered-input closure")
}

fn covered_input_for_row(
    update: &SyncMessage,
    row_uuid: RowUuid,
) -> crate::protocol::CoveredInputEntry {
    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        program_fact_adds,
        ..
    }) = update
    else {
        panic!("expected view update");
    };
    program_fact_adds
        .iter()
        .find_map(|fact| match fact {
            crate::protocol::ProgramFactEntry::CoveredInput(input)
                if input.source_row == row_uuid =>
            {
                Some(input.clone())
            }
            _ => None,
        })
        .expect("authority update must identify the row as an exact covered input")
}

fn view_update_parts(message: SyncMessage, defer_settlement: bool) -> ViewUpdateParts {
    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        subscription,
        settled_through,
        reset_result_set,
        version_carriers,
        peer_payload_inventory,
        result_member_adds,
        result_member_removes,
        program_fact_adds,
        program_fact_removes,
    }) = message
    else {
        panic!("expected view update");
    };
    ViewUpdateParts {
        subscription,
        settled_through,
        defer_settlement,
        reset_result_set,
        version_carriers,
        peer_complete_tx_payload_refs: peer_payload_inventory.complete_tx_payloads,
        authorization_progress: peer_payload_inventory.authorization_progress,
        opening_pending: peer_payload_inventory.opening_pending,
        result_member_adds,
        result_member_removes,
        program_fact_adds,
        program_fact_removes,
    }
}

#[test]
fn late_view_update_for_detached_subscription_is_dropped_and_counted() {
    // Internal protocol coverage: public APIs only expose this as a background
    // tick-driver stall. The protocol invariant is that unsubscribe is
    // asynchronous, so per-subscription traffic can arrive after local detach.
    let (_writer_dir, mut writer) = open_node_with_uuid(node(1));
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    let (_reader_dir, mut reader) = open_node_with_uuid(node(3));
    let row_uuid = row(10);
    let (shape, binding) = reader.whole_table_shape_binding("todos").unwrap();
    register_shape_binding(&mut reader, &shape, &binding);
    let subscription = reader.whole_table_subscription_key("todos").unwrap();
    let binding_view_key = BindingViewKey::from_canonical_subscription_key(subscription);

    let (_tx_id, visible_unit) = writer
        .commit_mergeable_unit_settled(
            MergeableCommit::new("todos", row_uuid, 10).cells(title_cells("visible")),
        )
        .unwrap();
    let SyncMessage::CommitUnit { tx, versions } = visible_unit else {
        panic!("expected commit unit");
    };
    core.ingest_commit_unit_settled(tx, versions, u64::MAX - SKEW_TOLERANCE_MS)
        .unwrap();
    reader
        .apply_sync_message_settled(system_authority_reset(
            &mut core,
            &shape,
            &binding,
            subscription,
        ))
        .unwrap();
    let before = receiver_rows(&mut reader, &shape, &binding, DurabilityTier::Global)
        .into_iter()
        .map(current_row_pair)
        .collect::<BTreeMap<_, _>>();
    assert_eq!(before, BTreeMap::from([(row_uuid, title_cells("visible"))]));

    let usage_subscription = crate::protocol::SubscriptionKey {
        shape_id: shape.shape_id(),
        binding_id: BindingId(uuid::Uuid::from_bytes([0x88; 16])),
        read_view: Default::default(),
    };
    reader
        .apply_sync_message_settled(SyncMessage::Subscribe(crate::protocol::Subscribe {
            shape_id: shape.shape_id(),
            subscription: usage_subscription,
            values: Vec::new(),
            known_state: None,
            delegated_session: None,
        }))
        .unwrap();
    assert_eq!(
        reader
            .binding_view_key_for_subscription(usage_subscription)
            .unwrap(),
        binding_view_key
    );
    reader.apply_unsubscribe(usage_subscription);
    let late = SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        subscription: usage_subscription,
        settled_through: GlobalTime(2),
        reset_result_set: false,
        version_carriers: Vec::new(),
        peer_payload_inventory: crate::protocol::PeerPayloadInventory::default(),
        result_member_adds: Vec::new(),
        result_member_removes: Vec::new(),
        program_fact_adds: Vec::new(),
        program_fact_removes: Vec::new(),
    });
    reader.apply_sync_message_settled(late).unwrap();

    assert_eq!(
        reader.sync_metrics().dropped_detached_subscription_messages,
        1
    );
    assert_eq!(
        receiver_rows(&mut reader, &shape, &binding, DurabilityTier::Global)
            .into_iter()
            .map(current_row_pair)
            .collect::<BTreeMap<_, _>>(),
        before,
        "late traffic must not mutate the shared canonical settled state"
    );
}

#[test]
fn late_view_update_for_never_registered_subscription_is_dropped_and_counted() {
    // Internal protocol coverage: the receiver cannot distinguish a never-seen
    // subscription key from a key detached before an in-flight message arrived.
    let (_reader_dir, mut reader) = open_node_with_uuid(node(3));
    let subscription = crate::protocol::SubscriptionKey {
        shape_id: ShapeId(uuid::Uuid::from_bytes([0x55; 16])),
        binding_id: BindingId(uuid::Uuid::from_bytes([0x66; 16])),
        read_view: Default::default(),
    };
    let late = SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        subscription,
        settled_through: GlobalTime(1),
        reset_result_set: true,
        version_carriers: Vec::new(),
        peer_payload_inventory: crate::protocol::PeerPayloadInventory::default(),
        result_member_adds: Vec::new(),
        result_member_removes: Vec::new(),
        program_fact_adds: Vec::new(),
        program_fact_removes: Vec::new(),
    });

    reader.apply_sync_message_settled(late).unwrap();

    assert_eq!(
        reader.sync_metrics().dropped_detached_subscription_messages,
        1
    );
    assert!(reader.query.settled_result_sets.is_empty());
}

#[test]
fn known_state_removal_without_local_body_clears_membership_without_repair() {
    // Internal protocol coverage: public APIs can observe revocation convergence,
    // but cannot assert that the receiver does not issue FetchRowVersions for a
    // removal whose body is policy-invisible.
    let (_writer_dir, mut writer) = open_node_with_uuid(node(1));
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    let (_reader_dir, mut reader) = open_node_with_uuid(node(3));
    let row_uuid = row(7);
    let (shape, binding) = reader.whole_table_shape_binding("todos").unwrap();
    register_shape_binding(&mut reader, &shape, &binding);
    let subscription = reader.whole_table_subscription_key("todos").unwrap();
    let authority_result_key = reader
        .authority_result_key_for_subscription(subscription)
        .unwrap();

    let (visible_tx, visible_unit) = writer
        .commit_mergeable_unit_settled(
            MergeableCommit::new("todos", row_uuid, 10).cells(title_cells("visible")),
        )
        .unwrap();
    let SyncMessage::CommitUnit { tx, versions } = visible_unit else {
        panic!("expected commit unit");
    };
    core.ingest_commit_unit_settled(tx, versions, u64::MAX - SKEW_TOLERANCE_MS)
        .unwrap();
    let initial = system_authority_reset(&mut core, &shape, &binding, subscription);
    let covered = covered_input_for_row(&initial, row_uuid);
    reader.apply_sync_message_settled(initial).unwrap();
    assert_eq!(
        receiver_rows(&mut reader, &shape, &binding, DurabilityTier::Global)
            .into_iter()
            .map(current_row_pair)
            .collect::<BTreeMap<_, _>>(),
        BTreeMap::from([(row_uuid, title_cells("visible"))])
    );

    let invisible_tx = TxId::new(TxTime(999), node(44));
    let removal = SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        subscription,
        settled_through: GlobalTime(2),
        reset_result_set: false,
        version_carriers: Vec::new(),
        peer_payload_inventory: crate::protocol::PeerPayloadInventory::default(),
        result_member_adds: Vec::new(),
        result_member_removes: Vec::new(),
        program_fact_adds: Vec::new(),
        program_fact_removes: vec![crate::protocol::ProgramFactEntry::CoveredInput(covered)],
    });
    assert!(
        reader
            .missing_known_state_row_version_refs(&removal)
            .unwrap()
            .is_empty(),
        "removals must not request repair bodies because the removed version may be policy-invisible"
    );
    reader.apply_sync_message_settled(removal).unwrap();
    assert!(
        receiver_rows(&mut reader, &shape, &binding, DurabilityTier::Global).is_empty()
    );
    assert_eq!(
        reader.settled_through_for_authority_result(&authority_result_key),
        Some(GlobalTime(2))
    );
    assert_ne!(visible_tx, invisible_tx);
}

#[test]
fn known_state_removal_for_never_known_row_is_noop_but_settles() {
    // Internal protocol coverage: this pins the receiver-side membership update
    // rule directly; public queries only observe the final empty set.
    let (_reader_dir, mut reader) = open_node_with_uuid(node(3));
    let (shape, binding) = reader.whole_table_shape_binding("todos").unwrap();
    register_shape_binding(&mut reader, &shape, &binding);
    let subscription = reader.whole_table_subscription_key("todos").unwrap();
    let authority_result_key = reader
        .authority_result_key_for_subscription(subscription)
        .unwrap();

    let removal = SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        subscription,
        settled_through: GlobalTime(3),
        reset_result_set: false,
        version_carriers: Vec::new(),
        peer_payload_inventory: crate::protocol::PeerPayloadInventory::default(),
        result_member_adds: Vec::new(),
        result_member_removes: Vec::new(),
        program_fact_adds: Vec::new(),
        program_fact_removes: Vec::new(),
    });

    assert!(
        reader
            .missing_known_state_row_version_refs(&removal)
            .unwrap()
            .is_empty()
    );
    reader.apply_sync_message_settled(removal).unwrap();
    assert!(
        receiver_rows(&mut reader, &shape, &binding, DurabilityTier::Global).is_empty()
    );
    assert_eq!(
        reader.settled_through_for_authority_result(&authority_result_key),
        Some(GlobalTime(3))
    );
}

#[test]
fn empty_reset_for_duplicate_usage_subscription_does_not_degrade_canonical_view() {
    // Internal protocol coverage: public one-shot queries only expose this as a
    // timeout race. This pins the shared-cache invariant directly: a short-lived
    // usage subscription must not clear canonical settled state for the same
    // shape/binding/read-view unless it carries a replacement snapshot.
    let (_writer_dir, mut writer) = open_node_with_uuid(node(1));
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    let (_reader_dir, mut reader) = open_node_with_uuid(node(3));
    let row_uuid = row(9);
    let (shape, binding) = reader.whole_table_shape_binding("todos").unwrap();
    register_shape_binding(&mut reader, &shape, &binding);
    let canonical_subscription = reader.whole_table_subscription_key("todos").unwrap();
    let binding_view_key = BindingViewKey::from_canonical_subscription_key(canonical_subscription);
    let authority_result_key = reader
        .authority_result_key_for_subscription(canonical_subscription)
        .unwrap();

    let (_tx_id, visible_unit) = writer
        .commit_mergeable_unit_settled(
            MergeableCommit::new("todos", row_uuid, 10).cells(title_cells("shared")),
        )
        .unwrap();
    let SyncMessage::CommitUnit { tx, versions } = visible_unit else {
        panic!("expected commit unit");
    };
    core.ingest_commit_unit_settled(tx, versions, u64::MAX - SKEW_TOLERANCE_MS)
        .unwrap();
    reader
        .apply_sync_message_settled(system_authority_reset(
            &mut core,
            &shape,
            &binding,
            canonical_subscription,
        ))
        .unwrap();
    assert_eq!(
        receiver_rows(&mut reader, &shape, &binding, DurabilityTier::Global)
            .into_iter()
            .map(current_row_pair)
            .collect::<BTreeMap<_, _>>(),
        BTreeMap::from([(row_uuid, title_cells("shared"))])
    );

    let duplicate_subscription = crate::protocol::SubscriptionKey {
        shape_id: shape.shape_id(),
        binding_id: BindingId(uuid::Uuid::from_bytes([0x77; 16])),
        read_view: Default::default(),
    };
    reader
        .apply_sync_message_settled(SyncMessage::Subscribe(crate::protocol::Subscribe {
            shape_id: shape.shape_id(),
            subscription: duplicate_subscription,
            values: Vec::new(),
            known_state: None,
            delegated_session: None,
        }))
        .unwrap();
    assert_eq!(
        reader
            .binding_view_key_for_subscription(duplicate_subscription)
            .unwrap(),
        binding_view_key
    );

    reader
        .apply_sync_message_settled(SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
            subscription: duplicate_subscription,
            settled_through: GlobalTime(2),
            reset_result_set: true,
            version_carriers: Vec::new(),
            peer_payload_inventory: crate::protocol::PeerPayloadInventory::default(),
            result_member_adds: Vec::new(),
            result_member_removes: Vec::new(),
            program_fact_adds: Vec::new(),
            program_fact_removes: Vec::new(),
        }))
        .unwrap();

    assert_eq!(
        receiver_rows(&mut reader, &shape, &binding, DurabilityTier::Global)
            .into_iter()
            .map(current_row_pair)
            .collect::<BTreeMap<_, _>>(),
        BTreeMap::from([(row_uuid, title_cells("shared"))])
    );
    assert_eq!(
        reader.settled_through_for_authority_result(&authority_result_key),
        Some(GlobalTime(2))
    );
}

#[test]
fn known_state_rehydrate_skips_known_bodies_and_repairs_missing_payload() {
    let (_writer_dir, mut writer) = open_node_with_uuid(node(1));
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    let (_reader_dir, mut reader) = open_node_with_uuid(node(3));
    let row_uuid = row(17);
    let (shape, binding) = core.whole_table_shape_binding("todos").unwrap();
    let subscription = core.whole_table_subscription_key("todos").unwrap();
    register_shape_binding(&mut reader, &shape, &binding);

    let (_tx_id, commit_unit) = writer
        .commit_mergeable_unit_settled(
            MergeableCommit::new("todos", row_uuid, 10).cells(title_cells("known")),
        )
        .unwrap();
    let SyncMessage::CommitUnit { tx, versions } = commit_unit else {
        panic!("expected commit unit");
    };
    core.ingest_commit_unit_settled(tx.clone(), versions, u64::MAX - SKEW_TOLERANCE_MS)
        .unwrap();
    reader
        .ingest_known_transaction(
            tx,
            Vec::new(),
            Fate::Accepted,
            Some(GlobalTime(1)),
            DurabilityTier::Global,
        )
        .unwrap();
    let mut control_peer = relay_with_system_binding(subscription);
    let control_update = control_peer
        .rehydrate_query_for_subscription_with_opts(
            &mut core,
            subscription,
            &shape,
            &binding,
            RegisterShapeOptions::default(),
        )
        .unwrap()
        .expect("expected view update");
    let control_version_bundles = version_bundles_for_update(&control_update);
    let control_input = covered_input_for_row(&control_update, row_uuid);
    assert_eq!(control_input.version.tx, _tx_id);
    assert_eq!(control_version_bundles.len(), 1);

    let mut peer = relay_with_system_binding(subscription);
    peer.declare_known_state(
        subscription,
        Some(crate::protocol::KnownStateDeclaration::Fast {
            completeness: crate::protocol::KnownStateCompleteness::FastCurrentMembership,
            position: GlobalTime::new(10, 0).unwrap(),
        }),
    );

    let update = peer
        .rehydrate_query_for_subscription_with_opts(
            &mut core,
            subscription,
            &shape,
            &binding,
            RegisterShapeOptions::default(),
        )
        .unwrap()
        .expect("expected view update");
    let version_bundles = version_bundles_for_update(&update);
    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        settled_through,
        reset_result_set,
        result_member_adds,
        ..
    }) = &update
    else {
        panic!("expected view update");
    };
    assert_eq!(*settled_through, GlobalTime::new(10, 0).unwrap());
    assert!(!reset_result_set);
    assert!(result_member_adds.is_empty());
    assert!(version_bundles.is_empty());

    let missing = reader
        .missing_known_state_row_version_refs(&update)
        .unwrap();
    assert!(missing.is_empty());
}

#[test]
fn fast_known_state_rehydrate_ships_only_members_after_declared_position() {
    let (_writer_dir, mut writer) = open_node_with_uuid(node(1));
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    let (_reader_dir, mut reader) = open_node_with_uuid(node(3));
    let row_a = row(17);
    let row_b = row(18);
    let (shape, binding) = core.whole_table_shape_binding("todos").unwrap();
    let subscription = core.whole_table_subscription_key("todos").unwrap();
    register_shape_binding(&mut reader, &shape, &binding);

    let (tx_a, unit_a) = writer
        .commit_mergeable_unit_settled(MergeableCommit::new("todos", row_a, 10).cells(title_cells("known")))
        .unwrap();
    let SyncMessage::CommitUnit {
        tx: commit_a,
        versions: versions_a,
    } = unit_a
    else {
        panic!("expected commit unit");
    };
    core.ingest_commit_unit_settled(commit_a, versions_a, u64::MAX - SKEW_TOLERANCE_MS)
        .unwrap();
    reader
        .apply_sync_message_settled(system_authority_reset(
            &mut core,
            &shape,
            &binding,
            subscription,
        ))
        .unwrap();

    let (tx_b, unit_b) = writer
        .commit_mergeable_unit_settled(MergeableCommit::new("todos", row_b, 20).cells(title_cells("new")))
        .unwrap();
    let SyncMessage::CommitUnit {
        tx: commit_b,
        versions: versions_b,
    } = unit_b
    else {
        panic!("expected commit unit");
    };
    core.ingest_commit_unit_settled(commit_b, versions_b, u64::MAX - SKEW_TOLERANCE_MS)
        .unwrap();

    let mut peer = relay_with_system_binding(subscription);
    peer.declare_known_state(
        subscription,
        Some(crate::protocol::KnownStateDeclaration::Fast {
            completeness: crate::protocol::KnownStateCompleteness::FastCurrentMembership,
            position: GlobalTime::new(10, 0).unwrap(),
        }),
    );

    let update = peer
        .rehydrate_query_for_subscription_with_opts(
            &mut core,
            subscription,
            &shape,
            &binding,
            RegisterShapeOptions::default(),
        )
        .unwrap()
        .expect("expected view update");
    let version_bundles = version_bundles_for_update(&update);
    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        settled_through,
        reset_result_set,
        result_member_adds,
        result_member_removes,
        program_fact_adds,
        ..
    }) = &update
    else {
        panic!("expected view update");
    };
    assert_eq!(*settled_through, GlobalTime::new(20, 0).unwrap());
    assert!(reset_result_set);
    assert!(result_member_adds.is_empty());
    assert!(result_member_removes.is_empty());
    assert!(program_fact_adds.iter().any(|fact| matches!(
        fact,
        crate::protocol::ProgramFactEntry::CoveredInput(input)
            if input.source_row == row_b && input.version.tx == tx_b
    )));
    assert_eq!(version_bundles.len(), 1);

    assert!(
        reader
            .missing_known_state_row_version_refs(&update)
            .unwrap()
            .is_empty()
    );
    reader.apply_sync_message_settled(update).unwrap();
    assert_eq!(
        reader
            .current_rows("todos", DurabilityTier::Local)
            .unwrap()
            .into_iter()
            .map(current_row_pair)
            .collect::<BTreeMap<_, _>>(),
        BTreeMap::from([(row_a, title_cells("known")), (row_b, title_cells("new"))])
    );

    assert_ne!(tx_a, tx_b);
}

#[test]
fn exact_known_state_rehydrate_skips_known_bodies_but_preserves_membership() {
    let (_writer_dir, mut writer) = open_node_with_uuid(node(1));
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    let row_uuid = row(19);
    let (shape, binding) = core.whole_table_shape_binding("todos").unwrap();
    let subscription = core.whole_table_subscription_key("todos").unwrap();

    let (tx_id, commit_unit) = writer
        .commit_mergeable_unit_settled(
            MergeableCommit::new("todos", row_uuid, 10).cells(title_cells("known")),
        )
        .unwrap();
    let SyncMessage::CommitUnit { tx, versions } = commit_unit else {
        panic!("expected commit unit");
    };
    core.ingest_commit_unit_settled(tx, versions, u64::MAX - SKEW_TOLERANCE_MS)
        .unwrap();

    let mut peer = relay_with_system_binding(subscription);
    peer.declare_known_state(
        subscription,
        Some(crate::protocol::KnownStateDeclaration::ExactVersionSet {
            versions: vec![crate::protocol::RowVersionRef::new(
                "todos", row_uuid, tx_id,
            )],
        }),
    );
    let update = peer
        .rehydrate_query_for_subscription_with_opts(
            &mut core,
            subscription,
            &shape,
            &binding,
            RegisterShapeOptions::default(),
        )
        .unwrap()
        .expect("expected view update");
    let version_bundles = version_bundles_for_update(&update);
    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        result_member_adds,
        program_fact_adds,
        ..
    }) = &update
    else {
        panic!("expected view update");
    };
    assert!(result_member_adds.is_empty());
    assert!(program_fact_adds.iter().any(|fact| matches!(
        fact,
        crate::protocol::ProgramFactEntry::CoveredInput(input)
            if input.source_row == row_uuid && input.version.tx == tx_id
    )));
    assert!(version_bundles.is_empty());
}

#[test]
fn fast_known_state_noop_rehydrate_is_apply_safe_for_warm_reader() {
    let (_writer_dir, mut writer) = open_node_with_uuid(node(1));
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    let (_reader_dir, mut reader) = open_node_with_uuid(node(3));
    let row_uuid = row(20);
    let (shape, binding) = core.whole_table_shape_binding("todos").unwrap();
    let subscription = core.whole_table_subscription_key("todos").unwrap();
    register_shape_binding(&mut reader, &shape, &binding);

    let (_tx_id, commit_unit) = writer
        .commit_mergeable_unit_settled(
            MergeableCommit::new("todos", row_uuid, 10).cells(title_cells("known")),
        )
        .unwrap();
    let SyncMessage::CommitUnit { tx, versions } = commit_unit else {
        panic!("expected commit unit");
    };
    core.ingest_commit_unit_settled(tx, versions, u64::MAX - SKEW_TOLERANCE_MS)
        .unwrap();
    reader
        .apply_sync_message_settled(system_authority_reset(
            &mut core,
            &shape,
            &binding,
            subscription,
        ))
        .unwrap();

    let mut peer = relay_with_system_binding(subscription);
    peer.declare_known_state(
        subscription,
        Some(crate::protocol::KnownStateDeclaration::Fast {
            completeness: crate::protocol::KnownStateCompleteness::FastCurrentMembership,
            position: GlobalTime::new(10, 0).unwrap(),
        }),
    );

    let update = peer
        .rehydrate_query_for_subscription_with_opts(
            &mut core,
            subscription,
            &shape,
            &binding,
            RegisterShapeOptions::default(),
        )
        .unwrap()
        .expect("expected view update");
    let version_bundles = version_bundles_for_update(&update);
    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        reset_result_set,
        result_member_adds,
        result_member_removes,
        ..
    }) = &update
    else {
        panic!("expected view update");
    };
    assert!(!reset_result_set);
    assert!(result_member_adds.is_empty());
    assert!(result_member_removes.is_empty());
    assert!(version_bundles.is_empty());

    reader.apply_sync_message_settled(update).unwrap();
    assert_eq!(
        receiver_rows(&mut reader, &shape, &binding, DurabilityTier::Global)
            .into_iter()
            .map(current_row_pair)
            .collect::<BTreeMap<_, _>>(),
        BTreeMap::from([(row_uuid, title_cells("known"))])
    );
}

#[test]
fn fast_known_state_noop_rehydrate_is_apply_safe_after_reader_reopen() {
    let (_writer_dir, mut writer) = open_node_with_uuid(node(1));
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    let (reader_dir, mut reader) = open_node_with_uuid(node(3));
    let row_uuid = row(22);
    let (shape, binding) = core.whole_table_shape_binding("todos").unwrap();
    let subscription = core.whole_table_subscription_key("todos").unwrap();
    register_shape_binding(&mut reader, &shape, &binding);

    let (_tx_id, commit_unit) = writer
        .commit_mergeable_unit_settled(
            MergeableCommit::new("todos", row_uuid, 10).cells(title_cells("known")),
        )
        .unwrap();
    let SyncMessage::CommitUnit { tx, versions } = commit_unit else {
        panic!("expected commit unit");
    };
    core.ingest_commit_unit_settled(tx, versions, u64::MAX - SKEW_TOLERANCE_MS)
        .unwrap();
    reader
        .apply_sync_message_settled(system_authority_reset(
            &mut core,
            &shape,
            &binding,
            subscription,
        ))
        .unwrap();
    assert_eq!(
        receiver_rows(&mut reader, &shape, &binding, DurabilityTier::Global)
            .into_iter()
            .map(current_row_pair)
            .collect::<BTreeMap<_, _>>(),
        BTreeMap::from([(row_uuid, title_cells("known"))])
    );

    drop(reader);
    let mut reader = reopen_node_at(&reader_dir, node(3), schema());
    register_shape_binding(&mut reader, &shape, &binding);
    assert_eq!(
        receiver_rows(&mut reader, &shape, &binding, DurabilityTier::Global)
            .into_iter()
            .map(current_row_pair)
            .collect::<BTreeMap<_, _>>(),
        BTreeMap::from([(row_uuid, title_cells("known"))])
    );

    let mut peer = relay_with_system_binding(subscription);
    peer.declare_known_state(
        subscription,
        Some(crate::protocol::KnownStateDeclaration::Fast {
            completeness: crate::protocol::KnownStateCompleteness::FastCurrentMembership,
            position: GlobalTime::new(10, 0).unwrap(),
        }),
    );

    let update = peer
        .rehydrate_query_for_subscription_with_opts(
            &mut core,
            subscription,
            &shape,
            &binding,
            RegisterShapeOptions::default(),
        )
        .unwrap()
        .expect("expected view update");
    let version_bundles = version_bundles_for_update(&update);
    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        reset_result_set,
        result_member_adds,
        result_member_removes,
        ..
    }) = &update
    else {
        panic!("expected view update");
    };
    assert!(!reset_result_set);
    assert!(result_member_adds.is_empty());
    assert!(result_member_removes.is_empty());
    assert!(version_bundles.is_empty());

    reader.apply_sync_message_settled(update).unwrap();
    assert_eq!(
        receiver_rows(&mut reader, &shape, &binding, DurabilityTier::Global)
            .into_iter()
            .map(current_row_pair)
            .collect::<BTreeMap<_, _>>(),
        BTreeMap::from([(row_uuid, title_cells("known"))])
    );
}

#[test]
fn exact_known_state_rehydrate_repairs_missing_payload() {
    let (_writer_dir, mut writer) = open_node_with_uuid(node(1));
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    let (_reader_dir, mut reader) = open_node_with_uuid(node(3));
    let row_uuid = row(21);
    let (shape, binding) = core.whole_table_shape_binding("todos").unwrap();
    let subscription = core.whole_table_subscription_key("todos").unwrap();
    register_shape_binding(&mut reader, &shape, &binding);

    let (tx_id, commit_unit) = writer
        .commit_mergeable_unit_settled(
            MergeableCommit::new("todos", row_uuid, 10).cells(title_cells("known")),
        )
        .unwrap();
    let SyncMessage::CommitUnit { tx, versions } = commit_unit else {
        panic!("expected commit unit");
    };
    core.ingest_commit_unit_settled(tx.clone(), versions, u64::MAX - SKEW_TOLERANCE_MS)
        .unwrap();
    reader
        .ingest_known_transaction(
            tx,
            Vec::new(),
            Fate::Accepted,
            Some(GlobalTime(1)),
            DurabilityTier::Global,
        )
        .unwrap();

    let mut peer = relay_with_system_binding(subscription);
    peer.declare_known_state(
        subscription,
        Some(crate::protocol::KnownStateDeclaration::ExactVersionSet {
            versions: vec![crate::protocol::RowVersionRef::new(
                "todos", row_uuid, tx_id,
            )],
        }),
    );
    let update = peer
        .rehydrate_query_for_subscription_with_opts(
            &mut core,
            subscription,
            &shape,
            &binding,
            RegisterShapeOptions::default(),
        )
        .unwrap()
        .expect("expected view update");
    let missing = reader
        .missing_known_state_row_version_refs(&update)
        .unwrap();
    assert_eq!(
        missing,
        vec![crate::protocol::RowVersionRef::new(
            "todos", row_uuid, tx_id
        )]
    );
    // The maintained view above deliberately models a relay and therefore
    // requires its binding explicitly. This direct repair receipt instead
    // models the one terminated SYSTEM session that asked for those visible
    // versions; relay transport repair is bound by its owner-loop request.
    let mut repair_peer = PeerState::client_link(AuthorSubject::SYSTEM);
    let messages = repair_peer
        .handle_row_versions_fetch(
            &mut core,
            SyncMessage::FetchRowVersions {
                requests: missing.clone(),
                delegated_session: None,
            },
        )
        .unwrap();
    let [SyncMessage::RowVersionPayloads { version_bundles }] = messages.as_slice() else {
        panic!("expected row-version payloads");
    };
    reader
        .apply_row_version_payloads_for_requests(&missing, version_bundles.clone())
        .unwrap();
    reader.apply_sync_message_settled(update).unwrap();
    assert_eq!(
        reader
            .current_rows("todos", DurabilityTier::Local)
            .unwrap()
            .into_iter()
            .map(current_row_pair)
            .collect::<BTreeMap<_, _>>(),
        BTreeMap::from([(row_uuid, title_cells("known"))])
    );
}

#[test]
fn slow_known_state_declaration_skips_exact_local_versions_only() {
    let (_writer_dir, mut writer) = open_node_with_uuid(node(1));
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    let (_reader_dir, mut reader) = open_node_with_uuid(node(3));
    let row_a = row(21);
    let row_b = row(22);
    let (shape, binding) = core.whole_table_shape_binding("todos").unwrap();
    let subscription = core.whole_table_subscription_key("todos").unwrap();
    register_shape_binding(&mut reader, &shape, &binding);
    let values = Vec::new();

    let (tx_a, unit_a) = writer
        .commit_mergeable_unit_settled(MergeableCommit::new("todos", row_a, 10).cells(title_cells("local")))
        .unwrap();
    let SyncMessage::CommitUnit {
        tx: tx_a_record,
        versions: versions_a,
    } = unit_a
    else {
        panic!("expected commit unit");
    };
    core.ingest_commit_unit_settled(
        tx_a_record.clone(),
        versions_a.clone(),
        u64::MAX - SKEW_TOLERANCE_MS,
    )
    .unwrap();
    reader
        .ingest_known_transaction(
            tx_a_record,
            versions_a,
            Fate::Accepted,
            Some(GlobalTime(1)),
            DurabilityTier::Global,
        )
        .unwrap();
    // Establish the exact source closure through the normal authority
    // publication path. A deferred update carries its input receipts but has
    // not completed its live authority handoff, so it cannot declare even an
    // exact known state yet.
    let authority_message = system_authority_reset(&mut core, &shape, &binding, subscription);
    assert_eq!(covered_input_for_row(&authority_message, row_a).version.tx, tx_a);
    let mut deferred_authority = view_update_parts(authority_message.clone(), true);
    deferred_authority.settled_through = GlobalTime::default();
    reader.apply_view_update(deferred_authority).unwrap();
    assert_eq!(
        reader
            .known_state_declaration_for_subscription(
                &shape,
                &binding,
                subscription,
                &values,
                AuthorSubject::SYSTEM,
                None,
            )
            .unwrap(),
        None,
        "a deferred exact receipt must not overclaim known state"
    );
    let mut settled_authority = view_update_parts(authority_message, false);
    settled_authority.settled_through = GlobalTime::default();
    reader.apply_view_update(settled_authority).unwrap();

    let (tx_b, unit_b) = writer
        .commit_mergeable_unit_settled(
            MergeableCommit::new("todos", row_b, 11).cells(title_cells("remote")),
        )
        .unwrap();
    core.apply_sync_message_settled(unit_b).unwrap();

    let declaration = reader
        .known_state_declaration_for_subscription(
            &shape,
            &binding,
            subscription,
            &values,
            AuthorSubject::SYSTEM,
            None,
        )
        .unwrap()
        .expect("reader should derive exact slow known-state");
    assert_eq!(
        declaration,
        crate::protocol::KnownStateDeclaration::ExactVersionSet {
            versions: vec![crate::protocol::RowVersionRef::new("todos", row_a, tx_a)]
        }
    );

    let mut control_peer = relay_with_system_binding(subscription);
    let control_update = control_peer
        .rehydrate_query_for_subscription_with_opts(
            &mut core,
            subscription,
            &shape,
            &binding,
            RegisterShapeOptions::default(),
        )
        .unwrap()
        .expect("expected view update");
    let control_bundles = version_bundles_for_update(&control_update);
    let control_inputs = [row_a, row_b]
        .map(|row_uuid| covered_input_for_row(&control_update, row_uuid));
    assert_eq!(control_inputs[0].version.tx, tx_a);
    assert_eq!(control_inputs[1].version.tx, tx_b);
    assert_eq!(control_bundles.len(), 2);

    let mut peer = relay_with_system_binding(subscription);
    peer.declare_known_state(subscription, Some(declaration));
    let update = peer
        .rehydrate_query_for_subscription_with_opts(
            &mut core,
            subscription,
            &shape,
            &binding,
            RegisterShapeOptions::default(),
        )
        .unwrap()
        .expect("expected view update");
    let version_bundles = version_bundles_for_update(&update);
    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        result_member_adds,
        program_fact_adds,
        ..
    }) = &update
    else {
        panic!("expected declared update");
    };
    assert!(result_member_adds.is_empty());
    assert_eq!(
        program_fact_adds
            .iter()
            .filter_map(|fact| match fact {
                crate::protocol::ProgramFactEntry::CoveredInput(input) => Some(input.source_row),
                _ => None,
            })
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([row_a, row_b]),
    );
    assert_eq!(version_bundles.len(), 1);
    assert_eq!(version_bundles[0].tx.tx_id, tx_b);
    assert!(
        reader
            .missing_known_state_row_version_refs(&update)
            .unwrap()
            .is_empty()
    );
    reader.apply_sync_message_settled(update).unwrap();
    assert_eq!(
        receiver_rows(&mut reader, &shape, &binding, DurabilityTier::Global)
            .into_iter()
            .map(current_row_pair)
            .collect::<BTreeMap<_, _>>(),
        BTreeMap::from([
            (row_a, title_cells("local")),
            (row_b, title_cells("remote")),
        ])
    );
    reader.apply_unsubscribe(subscription);
    assert_eq!(
        reader
            .known_state_declaration_for_subscription(
                &shape,
                &binding,
                subscription,
                &values,
                AuthorSubject::SYSTEM,
                None,
            )
            .unwrap(),
        None,
        "detaching the exact receipt must retire its live settlement evidence"
    );
}

#[test]
fn over_cap_slow_known_state_declaration_degrades_to_full_ship() {
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    let (shape, binding) = core.whole_table_shape_binding("todos").unwrap();
    let subscription = core.whole_table_subscription_key("todos").unwrap();
    let refs = (0..=crate::protocol_limits::MAX_KNOWN_STATE_EXACT_REFS)
        .map(|idx| {
            crate::protocol::RowVersionRef::new(
                "todos",
                row((idx % 255) as u8),
                TxId::new(TxTime(idx as u64 + 1), node(1)),
            )
        })
        .collect::<Vec<_>>();
    assert!(
        crate::node::query_eval::exact_known_state_declaration_for_test(
            shape.shape_id(),
            subscription,
            &[],
            refs,
        )
        .is_none(),
        "oversized exact declarations must degrade to no declaration, never truncate"
    );

    let mut writer = open_node_with_uuid(node(1)).1;
    let tx_id = commit_mergeable_global(
        &mut writer,
        &mut core,
        MergeableCommit::new("todos", row(23), 12).cells(title_cells("full")),
    );
    let mut peer = relay_with_system_binding(subscription);
    let update = peer
        .rehydrate_query_for_subscription_with_opts(
            &mut core,
            subscription,
            &shape,
            &binding,
            RegisterShapeOptions::default(),
        )
        .unwrap()
        .expect("expected view update");
    let version_bundles = version_bundles_for_update(&update);
    let covered = covered_input_for_row(&update, row(23));
    assert_eq!(covered.version.tx, tx_id);
    assert_eq!(version_bundles.len(), 1);
    assert_eq!(version_bundles[0].tx.tx_id, tx_id);
}

#[test]
fn fast_known_state_requires_a_live_receipt_after_reopen_and_eviction() {
    let (_writer_dir, mut writer) = open_node_with_uuid(node(1));
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    let (_reader_dir, reader) = open_node_with_uuid(node(3));
    let row_uuid = row(24);
    let (shape, binding) = core.whole_table_shape_binding("todos").unwrap();
    let subscription = core.whole_table_subscription_key("todos").unwrap();
    commit_mergeable_global(
        &mut writer,
        &mut core,
        MergeableCommit::new("todos", row_uuid, 13).cells(title_cells("persisted")),
    );
    let mut reader = reader;
    register_shape_binding(&mut reader, &shape, &binding);
    let mut peer = relay_with_system_binding(subscription);
    let update = peer
        .rehydrate_query_for_subscription_with_opts(
            &mut core,
            subscription,
            &shape,
            &binding,
            RegisterShapeOptions::default(),
        )
        .unwrap()
        .expect("expected view update");
    reader.apply_sync_message_settled(update).unwrap();

    assert!(matches!(
        reader
            .known_state_declaration_for_subscription(
                &shape,
                &binding,
                subscription,
                &[],
                AuthorSubject::SYSTEM,
                None,
            )
            .unwrap(),
        Some(crate::protocol::KnownStateDeclaration::Fast { .. })
    ));

    let mut reopened = reader.reopen_in_place().unwrap();
    let declaration = reopened
        .known_state_declaration_for_subscription(
            &shape,
            &binding,
            subscription,
            &[],
            AuthorSubject::SYSTEM,
            None,
        )
        .unwrap();
    assert_eq!(
        declaration, None,
        "durably recovered membership is cache material, not a live authority handoff"
    );

    let report = reopened.evict_cold(&PeerEvictionPins::default()).unwrap();
    assert_eq!(report.row_versions_evictable, 1);
    let declaration = reopened
        .known_state_declaration_for_subscription(
            &shape,
            &binding,
            subscription,
            &[],
            AuthorSubject::SYSTEM,
            None,
        )
        .unwrap();
    assert_eq!(declaration, None);
}

#[test]
fn storage_reopen_does_not_promote_a_durable_fast_cursor_to_live_settlement() {
    let (_writer_dir, mut writer) = open_node_with_uuid(node(1));
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    let (reader_dir, mut reader) = open_node_with_uuid(node(3));
    let row_uuid = row(25);
    let (shape, binding) = core.whole_table_shape_binding("todos").unwrap();
    let subscription = core.whole_table_subscription_key("todos").unwrap();
    register_shape_binding(&mut reader, &shape, &binding);
    commit_mergeable_global(
        &mut writer,
        &mut core,
        MergeableCommit::new("todos", row_uuid, 14).cells(title_cells("persisted storage")),
    );
    let mut peer = relay_with_system_binding(subscription);
    let update = peer
        .rehydrate_query_for_subscription_with_opts(
            &mut core,
            subscription,
            &shape,
            &binding,
            RegisterShapeOptions::default(),
        )
        .unwrap()
        .expect("expected view update");
    reader.apply_sync_message_settled(update).unwrap();
    drop(reader);

    let mut reopened = open_node_at(&reader_dir, schema());
    let declaration = reopened
        .known_state_declaration_for_subscription(
            &shape,
            &binding,
            subscription,
            &[],
            AuthorSubject::SYSTEM,
            None,
        )
        .unwrap();
    assert_eq!(declaration, None);
}

#[test]
fn settled_program_fact_add_remove_rewrite_and_reopen_use_one_durable_key_codec() {
    // Internal storage-boundary coverage: the only durable peer facts are the
    // exact source manifest and its covered inputs. Exercise add, remove, reset
    // rewrite, and reopen without reviving a result-payload compatibility path.
    let (reader_dir, mut reader) = open_node_with_uuid(node(3));
    let (_writer_dir, mut writer) = open_node_with_uuid(node(1));
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    let (shape, binding) = reader.whole_table_shape_binding("todos").unwrap();
    register_shape_binding(&mut reader, &shape, &binding);
    let subscription = reader.whole_table_subscription_key("todos").unwrap();
    commit_mergeable_global(
        &mut writer,
        &mut core,
        MergeableCommit::new("todos", row(42), 15).cells(title_cells("covered")),
    );
    let reset = system_authority_reset(&mut core, &shape, &binding, subscription);
    let SyncMessage::ViewUpdate(reset_payload) = &reset else {
        panic!("expected authority reset");
    };
    let facts = reset_payload
        .program_fact_adds
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    assert!(facts.iter().all(crate::protocol::ProgramFactEntry::is_peer_source_closure_fact));
    reader.apply_sync_message_settled(reset.clone()).unwrap();
    assert!(reader
        .query
        .authority_results
        .values()
        .any(|state| state.settled_program_facts == facts));

    let mut removal = reset_payload.clone();
    removal.reset_result_set = false;
    removal.version_carriers.clear();
    removal.program_fact_adds.clear();
    // A live transition removes covered inputs, never the compiler's manifest.
    // Even an empty result retains complete coverage of its declared sources.
    removal.program_fact_removes = facts.iter().filter(|fact| matches!(
        fact, crate::protocol::ProgramFactEntry::CoveredInput(_)
    )).cloned().collect();
    let manifest = facts.iter().filter(|fact| matches!(
        fact, crate::protocol::ProgramFactEntry::ProgramSourceCoverage(_)
    )).cloned().collect::<BTreeSet<_>>();
    reader
        .apply_sync_message_settled(SyncMessage::ViewUpdate(removal))
        .unwrap();
    assert!(reader
        .query
        .authority_results
        .values()
        .any(|state| state.settled_program_facts == manifest));
    reader.apply_sync_message_settled(reset).unwrap();
    let settled_facts_store = reader
        .database
        .direct_record_store(crate::schema::SETTLED_PROGRAM_FACTS_STORE)
        .unwrap();
    let durable_facts = futures::executor::block_on(settled_facts_store.prefix_entries(&[]))
        .unwrap();
    assert_eq!(durable_facts.len(), facts.len());
    assert!(durable_facts
        .iter()
        .all(|entry| matches!(entry.key.last(), Some(Value::Bytes(digest)) if digest.len() == 32)));
    drop(reader);
    let reopened = open_node_at(&reader_dir, schema());
    assert!(reopened
        .query
        .authority_results
        .values()
        .any(|state| state.settled_program_facts == facts));
}

#[test]
fn covered_input_reset_never_populates_retired_result_member_storage() {
    let (reader_dir, mut reader) = open_node_with_uuid(node(3));
    let (_writer_dir, mut writer) = open_node_with_uuid(node(1));
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    let (shape, binding) = reader.whole_table_shape_binding("todos").unwrap();
    register_shape_binding(&mut reader, &shape, &binding);
    let subscription = reader.whole_table_subscription_key("todos").unwrap();
    commit_mergeable_global(
        &mut writer,
        &mut core,
        MergeableCommit::new("todos", row(61), 16).cells(title_cells("source only")),
    );
    reader
        .apply_sync_message_settled(system_authority_reset(
            &mut core,
            &shape,
            &binding,
            subscription,
        ))
        .unwrap();
    let store = reader
        .database
        .direct_record_store(crate::schema::SETTLED_RESULT_MEMBERS_STORE)
        .unwrap();
    let entries = futures::executor::block_on(store.prefix_entries(&[])).unwrap();
    assert!(entries.is_empty());
    drop(reader);
    let reopened = open_node_at(&reader_dir, schema());
    assert!(reopened.query.authority_results.values().all(|state| state.settled_result_set.is_empty()));
}

#[test]
fn corrupt_settled_program_fact_recovery_does_not_publish_a_valid_prefix() {
    // Internal recovery-boundary coverage: force a valid persisted fact followed
    // by a malformed durable key and verify recovery clears the resident
    // closure rather than publishing a partially decoded prefix. A failed
    // durable recovery is fail-closed, not a request to preserve potentially
    // stale state that was loaded before the corruption was detected.
    let (_reader_dir, mut reader) = open_node_with_uuid(node(3));
    let (_writer_dir, mut writer) = open_node_with_uuid(node(1));
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    let (shape, binding) = reader.whole_table_shape_binding("todos").unwrap();
    register_shape_binding(&mut reader, &shape, &binding);
    let subscription = reader.whole_table_subscription_key("todos").unwrap();
    commit_mergeable_global(
        &mut writer,
        &mut core,
        MergeableCommit::new("todos", row(43), 17).cells(title_cells("valid")),
    );
    let reset = system_authority_reset(&mut core, &shape, &binding, subscription);
    let SyncMessage::ViewUpdate(payload) = &reset else {
        panic!("expected authority reset");
    };
    let fact = payload
        .program_fact_adds
        .iter()
        .find(|fact| matches!(fact, crate::protocol::ProgramFactEntry::CoveredInput(_)))
        .cloned()
        .expect("authority closure contains a valid covered-input fact");
    reader.apply_sync_message_settled(reset).unwrap();
    let corrupt_store = reader
        .database
        .direct_record_store(crate::schema::SETTLED_PROGRAM_FACTS_STORE)
        .unwrap();
    let mut corrupt_key = futures::executor::block_on(corrupt_store.prefix_entries(&[]))
        .unwrap()
        .into_iter()
        .next()
        .expect("valid closure fact was persisted")
        .key;
    *corrupt_key.last_mut().expect("fact digest key component") = Value::Bytes(vec![0xff; 32]);
    futures::executor::block_on(corrupt_store.set(
            &corrupt_key,
            &[Value::Bytes(
                crate::node::codec::program_fact_storage_bytes(&fact).unwrap(),
            )],
        ))
    .unwrap();
    assert!(futures::executor::block_on(reader.recover_known_state_facts()).is_err());
    assert!(reader.query.settled_program_facts.is_empty());
    assert!(reader.query.settled_through_by_binding_view.is_empty());
}

#[test]
fn known_state_declaration_never_skips_unfated_edge_members() {
    let (_writer_dir, mut writer) = open_node_with_uuid(node(1));
    let (_edge_dir, mut edge) = open_node_with_uuid(node(7));
    let row_uuid = row(18);
    // A zero-offset exact-id Edge read without a policy remains a genuinely
    // local relay evaluation: an unfated Edge member must be visible even
    // though no Global receipt exists to source it.
    let shape = Query::from("todos")
        .filter(eq(col("id"), lit(Value::Uuid(row_uuid.0))))
        .validate(&schema())
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let opts = RegisterShapeOptions {
        tier: DurabilityTier::Edge,
        ..RegisterShapeOptions::default()
    };
    let subscription = SubscriptionKey {
        shape_id: shape.shape_id(),
        binding_id: binding.binding_id(),
        read_view: opts.read_view_key(),
    };

    let (tx_id, unit) = writer
        .commit_mergeable_unit_settled(
            MergeableCommit::new("todos", row_uuid, 10).cells(title_cells("unfated")),
        )
        .unwrap();
    let SyncMessage::CommitUnit { tx, versions } = unit else {
        panic!("expected commit unit");
    };
    edge.ingest_known_transaction(tx, versions, Fate::Accepted, None, DurabilityTier::Edge)
        .unwrap();
    let mut peer = relay_with_system_binding(subscription);
    peer.declare_known_state(
        subscription,
        Some(crate::protocol::KnownStateDeclaration::Fast {
            completeness: crate::protocol::KnownStateCompleteness::FastCurrentMembership,
            position: GlobalTime(100),
        }),
    );

    let update = peer
        .rehydrate_query_for_subscription_with_opts(&mut edge, subscription, &shape, &binding, opts)
        .unwrap()
        .expect("expected view update");
    let version_bundles = version_bundles_for_update(&update);
    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        result_member_adds,
        program_fact_adds,
        ..
    }) = update
    else {
        panic!("expected view update");
    };
    assert!(result_member_adds.is_empty());
    assert!(program_fact_adds.iter().any(|fact| matches!(
        fact,
        crate::protocol::ProgramFactEntry::CoveredInput(input)
            if input.source_row == row_uuid && input.version.tx == tx_id
    )));
    assert_eq!(version_bundles.len(), 1);
    assert_eq!(version_bundles[0].tx.tx_id, tx_id);
    assert_eq!(version_bundles[0].versions.len(), 1);
}
