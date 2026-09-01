// Detached usage, exact/fast/slow known state, reopen, and eviction.

fn relay_with_system_binding(subscription: SubscriptionKey) -> PeerState {
    let mut peer = PeerState::relay();
    peer.set_subscription_policy_binding(subscription, (AuthorSubject::SYSTEM, BTreeMap::new()));
    peer
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
        .apply_sync_message_settled(core.view_update_for_current_rows("todos").unwrap())
        .unwrap();
    let before = reader
        .subscription_current_rows("todos", DurabilityTier::Global)
        .unwrap()
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
        result_member_removes: vec![crate::protocol::ResultMemberEntry::row((
            groove::Intern::from("todos".to_owned()),
            row_uuid,
            TxId::new(TxTime(777), node(44)),
        ))],
        terminal_operations: Vec::new(),
        program_fact_adds: Vec::new(),
        program_fact_removes: Vec::new(),
    });
    reader.apply_sync_message_settled(late).unwrap();

    assert_eq!(
        reader.sync_metrics().dropped_detached_subscription_messages,
        1
    );
    assert_eq!(
        reader
            .subscription_current_rows("todos", DurabilityTier::Global)
            .unwrap()
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
        terminal_operations: Vec::new(),
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
    let binding_view_key = BindingViewKey::from_canonical_subscription_key(subscription);

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
    let initial = core.view_update_for_current_rows("todos").unwrap();
    reader.apply_sync_message_settled(initial).unwrap();
    assert_eq!(
        reader
            .subscription_current_rows("todos", DurabilityTier::Global)
            .unwrap()
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
        result_member_removes: vec![crate::protocol::ResultMemberEntry::row((
            groove::Intern::from("todos".to_owned()),
            row_uuid,
            invisible_tx,
        ))],
        terminal_operations: Vec::new(),
        program_fact_adds: Vec::new(),
        program_fact_removes: Vec::new(),
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
        reader
            .subscription_current_rows("todos", DurabilityTier::Global)
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        reader.settled_through_for_binding_view(binding_view_key),
        Some(GlobalTime(2))
    );
    assert_ne!(visible_tx, invisible_tx);
}

#[test]
fn known_state_removal_for_never_known_row_is_noop_but_settles() {
    // Internal protocol coverage: this pins the receiver-side membership update
    // rule directly; public queries only observe the final empty set.
    let (_reader_dir, mut reader) = open_node_with_uuid(node(3));
    let row_uuid = row(8);
    let (shape, binding) = reader.whole_table_shape_binding("todos").unwrap();
    register_shape_binding(&mut reader, &shape, &binding);
    let subscription = reader.whole_table_subscription_key("todos").unwrap();
    let binding_view_key = BindingViewKey::from_canonical_subscription_key(subscription);

    let removal = SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        subscription,
        settled_through: GlobalTime(3),
        reset_result_set: false,
        version_carriers: Vec::new(),
        peer_payload_inventory: crate::protocol::PeerPayloadInventory::default(),
        result_member_adds: Vec::new(),
        result_member_removes: vec![crate::protocol::ResultMemberEntry::row((
            groove::Intern::from("todos".to_owned()),
            row_uuid,
            TxId::new(TxTime(1000), node(45)),
        ))],
        terminal_operations: Vec::new(),
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
        reader
            .subscription_current_rows("todos", DurabilityTier::Global)
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        reader.settled_through_for_binding_view(binding_view_key),
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
        .apply_sync_message_settled(core.view_update_for_current_rows("todos").unwrap())
        .unwrap();
    assert_eq!(
        reader
            .subscription_current_rows("todos", DurabilityTier::Global)
            .unwrap()
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
            terminal_operations: Vec::new(),
            program_fact_adds: Vec::new(),
            program_fact_removes: Vec::new(),
        }))
        .unwrap();

    assert_eq!(
        reader
            .subscription_current_rows("todos", DurabilityTier::Global)
            .unwrap()
            .into_iter()
            .map(current_row_pair)
            .collect::<BTreeMap<_, _>>(),
        BTreeMap::from([(row_uuid, title_cells("shared"))])
    );
    assert_eq!(
        reader.settled_through_for_binding_view(binding_view_key),
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
    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        result_member_adds: control_result_member_adds,
        ..
    }) = &control_update
    else {
        panic!("expected control view update");
    };
    assert_eq!(control_result_member_adds.len(), 1);
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
    assert_eq!(control_result_member_adds.len(), 1);

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
        .apply_sync_message_settled(core.view_update_for_current_rows("todos").unwrap())
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
        ..
    }) = &update
    else {
        panic!("expected view update");
    };
    assert_eq!(*settled_through, GlobalTime::new(20, 0).unwrap());
    assert!(!reset_result_set);
    assert_eq!(
        result_member_adds,
        &vec![crate::protocol::ResultMemberEntry::from(
            crate::protocol::RealRowMemberEntry::current_content((
                groove::Intern::from("todos".to_owned()),
                row_b,
                tx_b,
            ))
            .with_settle_position(Some(GlobalTime::new(20, 0).unwrap()))
        )]
    );
    assert!(result_member_removes.is_empty());
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
        ..
    }) = &update
    else {
        panic!("expected view update");
    };
    assert_eq!(result_member_adds.len(), 1);
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
        .apply_sync_message_settled(core.view_update_for_current_rows("todos").unwrap())
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
fn fast_known_state_noop_rehydrate_is_apply_safe_after_reader_reopen() {
    let (_writer_dir, mut writer) = open_node_with_uuid(node(1));
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    let (reader_dir, mut reader) = open_node_with_uuid(node(3));
    let row_uuid = row(22);
    let (shape, binding) = core.whole_table_shape_binding("todos").unwrap();
    let subscription = core.whole_table_subscription_key("todos").unwrap();

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
        .apply_sync_message_settled(core.view_update_for_current_rows("todos").unwrap())
        .unwrap();
    assert_eq!(
        reader
            .subscription_current_rows("todos", DurabilityTier::Global)
            .unwrap()
            .into_iter()
            .map(current_row_pair)
            .collect::<BTreeMap<_, _>>(),
        BTreeMap::from([(row_uuid, title_cells("known"))])
    );

    drop(reader);
    let mut reader = reopen_node_at(&reader_dir, node(3), schema());
    assert_eq!(
        reader
            .subscription_current_rows("todos", DurabilityTier::Global)
            .unwrap()
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
        reader
            .subscription_current_rows("todos", DurabilityTier::Global)
            .unwrap()
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
    let binding_view_key = BindingViewKey {
        shape_id: shape.shape_id(),
        binding_id: binding.binding_id(),
        read_view: RegisterShapeOptions::default().read_view_key(),
    };
    reader.query.settled_result_sets.insert(
        binding_view_key,
        BTreeSet::from([crate::protocol::ResultMemberEntry::row((
            groove::Intern::from("todos".to_owned()),
            row_a,
            tx_a,
        ))]),
    );

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
    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        result_member_adds: control_members,
        ..
    }) = &control_update
    else {
        panic!("expected control update");
    };
    assert_eq!(control_members.len(), 2);
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
        ..
    }) = &update
    else {
        panic!("expected declared update");
    };
    assert_eq!(result_member_adds, control_members);
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
        reader
            .current_rows("todos", DurabilityTier::Local)
            .unwrap()
            .into_iter()
            .map(current_row_pair)
            .collect::<BTreeMap<_, _>>(),
        BTreeMap::from([
            (row_a, title_cells("local")),
            (row_b, title_cells("remote")),
        ])
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
    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        result_member_adds,
        ..
    }) = update
    else {
        panic!("expected full update");
    };
    assert_eq!(result_member_adds.len(), 1);
    assert_eq!(version_bundles.len(), 1);
    assert_eq!(version_bundles[0].tx.tx_id, tx_id);
}

#[test]
fn fast_known_state_fact_survives_reopen_and_eviction_clears_it() {
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
        declaration,
        Some(crate::protocol::KnownStateDeclaration::Fast {
            completeness: crate::protocol::KnownStateCompleteness::FastCurrentMembership,
            position: GlobalTime::new(13, 0).unwrap(),
        })
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
    assert!(matches!(
        declaration,
        None | Some(crate::protocol::KnownStateDeclaration::ExactVersionSet { .. })
    ));
}

#[test]
fn fast_known_state_fact_survives_storage_reopen() {
    let (_writer_dir, mut writer) = open_node_with_uuid(node(1));
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    let (reader_dir, mut reader) = open_node_with_uuid(node(3));
    let row_uuid = row(25);
    let (shape, binding) = core.whole_table_shape_binding("todos").unwrap();
    let subscription = core.whole_table_subscription_key("todos").unwrap();
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
    assert_eq!(
        declaration,
        Some(crate::protocol::KnownStateDeclaration::Fast {
            completeness: crate::protocol::KnownStateCompleteness::FastCurrentMembership,
            position: GlobalTime::new(14, 0).unwrap(),
        })
    );
}

#[test]
fn settled_program_fact_add_remove_rewrite_and_reopen_use_one_durable_key_codec() {
    // Internal storage-boundary coverage: applications cannot observe physical
    // keys, while this verifies all delta modes -- including the nested
    // descriptor/value payload used by aggregate synthetic members -- survive
    // through the same reopen path. Codec fixtures cover every fact variant
    // separately.
    let (reader_dir, mut reader) = open_node_with_uuid(node(3));
    let (shape, binding) = reader.whole_table_shape_binding("todos").unwrap();
    register_shape_binding(&mut reader, &shape, &binding);
    let subscription = reader.whole_table_subscription_key("todos").unwrap();
    let key = BindingViewKey::from_canonical_subscription_key(subscription);
    let fact = |path: &str| crate::protocol::ProgramFactEntry::PathCorrelationCoverage(
        crate::protocol::PathCorrelationCoverageEntry {
            path: path.to_owned(),
            source_table: "todos".to_owned().into(),
            source_row: row(42),
            correlation_key: vec![path.len() as u8],
            complete: true,
        },
    );
    let update = |reset_result_set, adds, removes| {
        SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
            subscription,
            settled_through: GlobalTime(1),
            reset_result_set,
            version_carriers: Vec::new(),
            peer_payload_inventory: crate::protocol::PeerPayloadInventory::default(),
            result_member_adds: Vec::new(),
            result_member_removes: Vec::new(),
            terminal_operations: Vec::new(),
            program_fact_adds: adds,
            program_fact_removes: removes,
        })
    };
    let added = fact("add");
    reader.apply_sync_message_settled(update(false, vec![added.clone()], vec![])).unwrap();
    assert_eq!(reader.query.settled_program_facts[&key], BTreeSet::from([added.clone()]));
    reader.apply_sync_message_settled(update(false, vec![], vec![added])).unwrap();
    assert!(reader.query.settled_program_facts[&key].is_empty());
    let rewritten = fact("rewrite");
    let payload_descriptor = groove::records::RecordDescriptor::new([(
        "value",
        groove::records::ValueType::String,
    )]);
    let synthetic_row = crate::node::codec::settled_result_value_storage_bytes(
        &Value::String("group-a".to_owned()),
        &groove::records::ValueType::String,
    )
    .unwrap();
    let synthetic_replacement = crate::node::codec::settled_result_value_storage_bytes(
        &Value::U64(1),
        &groove::records::ValueType::U64,
    )
    .unwrap();
    let nested_payload = crate::protocol::ProgramFactEntry::ResultPayload(
        crate::protocol::ResultMemberPayloadEntry {
            member: crate::protocol::ResultMemberEntry::Synthetic {
                table: "totals".to_owned(),
                row: synthetic_row,
                replacement: crate::protocol::SyntheticReplacementToken::from_encoded_record(
                    synthetic_replacement,
                ),
            },
            descriptor: groove::records::encode_record_descriptor(&payload_descriptor).unwrap(),
            record: payload_descriptor
                .create(&[Value::String("large settled payload ".repeat(20_000))])
                .unwrap(),
        },
    );
    reader
        .apply_sync_message_settled(update(
            true,
            vec![rewritten.clone(), nested_payload.clone()],
            vec![],
        ))
        .unwrap();
    let settled_facts_store = reader
        .database
        .direct_record_store(crate::schema::SETTLED_PROGRAM_FACTS_STORE)
        .unwrap();
    let durable_fact = futures::executor::block_on(settled_facts_store.prefix_entries(&[]))
    .unwrap()
    .into_iter()
    .find(|entry| {
        matches!(
            entry.value.get_idx(0),
            Ok(Value::Bytes(bytes)) if bytes.len() > 64 * 1024
        )
    })
    .expect("settled program fact is durable");
    assert!(matches!(&durable_fact.key[3], Value::Bytes(digest) if digest.len() == 32));
    assert!(matches!(
        durable_fact.value.get_idx(0).unwrap(),
        Value::Bytes(bytes) if bytes.len() > 64 * 1024
    ));
    drop(reader);
    let reopened = open_node_at(&reader_dir, schema());
    assert_eq!(
        reopened.query.settled_program_facts[&key],
        BTreeSet::from([rewritten, nested_payload])
    );
}

/// Internal direct-store recovery receipt for a local reader: large synthetic,
/// path, and real result identities must reopen through bounded digest keys.
/// This stays below the public client layer because applications cannot inspect
/// the direct-store key/value boundary or inject a digest/payload mismatch.
#[test]
fn settled_result_members_use_digest_keys_and_recover_large_payloads() {
    // The member itself is application-shaped data (synthetic rows and path
    // tuples can be large).  It must therefore never become an ordered-store
    // key: every durable key has a fixed 32-byte digest and the complete
    // canonical member is retained in the value cell for exact recovery.
    let (reader_dir, mut reader) = open_node_with_uuid(node(3));
    let (shape, binding) = reader.whole_table_shape_binding("todos").unwrap();
    register_shape_binding(&mut reader, &shape, &binding);
    let subscription = reader.whole_table_subscription_key("todos").unwrap();
    let key = BindingViewKey::from_canonical_subscription_key(subscription);
    let update = |reset_result_set, adds, removes| {
        SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
            subscription,
            settled_through: GlobalTime(1),
            reset_result_set,
            version_carriers: Vec::new(),
            peer_payload_inventory: crate::protocol::PeerPayloadInventory::default(),
            result_member_adds: adds,
            result_member_removes: removes,
            terminal_operations: Vec::new(),
            program_fact_adds: Vec::new(),
            program_fact_removes: Vec::new(),
        })
    };

    let mut real = crate::protocol::RealRowMemberEntry::current_content((
        groove::Intern::from("todos".to_owned()),
        row(61),
        TxId::new(TxTime(61), node(61)),
    ));
    // Nearly the maximum member encoding: this used to make the ordered IDB
    // key too large even though the direct-store value is intentionally able
    // to carry it.
    real.branch_or_prefix = Some(vec![0x61; 900 * 1024]);
    // This is a pure output identity receipt, not a row payload delivery;
    // avoid claiming that this synthetic view update carries a transaction
    // whose version bundle was not included above.
    real.content_tx = None;
    let real = ResultMemberEntry::Row(real);
    let synthetic_row = crate::node::codec::settled_result_value_storage_bytes(
        &Value::String("synthetic ".repeat(10_000)),
        &groove::records::ValueType::String,
    )
    .unwrap();
    let synthetic_replacement = crate::node::codec::settled_result_value_storage_bytes(
        &Value::U64(1),
        &groove::records::ValueType::U64,
    )
    .unwrap();
    let synthetic = ResultMemberEntry::Synthetic {
        table: "totals".to_owned(),
        row: synthetic_row,
        replacement: crate::protocol::SyntheticReplacementToken::from_encoded_record(
            synthetic_replacement,
        ),
    };
    let path = ResultMemberEntry::PathTuple {
        path: "album.tracks".to_owned(),
        source_table: groove::Intern::from("albums".to_owned()),
        source_row: row(62),
        target_table: groove::Intern::from("tracks".to_owned()),
        target_row: row(63),
        edge_id: Some(vec![0x62; 64 * 1024]),
        revision: vec![0x63; 96 * 1024],
    };
    reader
        .apply_sync_message_settled(update(
            false,
            vec![real.clone(), synthetic.clone(), path.clone()],
            vec![],
        ))
        .unwrap();
    // Repeating an add is idempotent because the digest is a stable identity.
    reader
        .apply_sync_message_settled(update(false, vec![synthetic.clone()], vec![]))
        .unwrap();
    let store = reader
        .database
        .direct_record_store(crate::schema::SETTLED_RESULT_MEMBERS_STORE)
        .unwrap();
    let entries = futures::executor::block_on(store.prefix_entries(&[])).unwrap();
    assert_eq!(entries.len(), 3);
    assert!(entries.iter().all(|entry| {
        entry.key.len() == 4
            && matches!(&entry.key[3], Value::Bytes(digest) if digest.len() == 32)
            && matches!(entry.value.get_idx(0), Ok(Value::Bytes(bytes)) if bytes.len() > 0)
    }));
    assert!(entries.iter().any(|entry| {
        matches!(entry.value.get_idx(0), Ok(Value::Bytes(bytes)) if bytes.len() > 64 * 1024)
    }));
    assert!(entries.iter().any(|entry| {
        matches!(entry.value.get_idx(0), Ok(Value::Bytes(bytes)) if bytes.len() > 800 * 1024)
    }));
    drop(reader);

    let mut reopened = open_node_at(&reader_dir, schema());
    assert_eq!(
        reopened.query.settled_result_sets[&key],
        BTreeSet::from([real.clone(), synthetic.clone(), path.clone()])
    );
    // Exercise delete and full rewrite using the same digest-key codec.
    reopened
        .apply_sync_message_settled(update(false, vec![], vec![path]))
        .unwrap();
    reopened
        .apply_sync_message_settled(update(true, vec![synthetic.clone()], vec![]))
        .unwrap();
    drop(reopened);
    let mut reopened = open_node_at(&reader_dir, schema());
    assert_eq!(
        reopened.query.settled_result_sets[&key],
        BTreeSet::from([synthetic.clone()])
    );

    // A full-size but unrelated digest must fail closed rather than allowing a
    // corrupt record to masquerade as a different member.
    let store = reopened
        .database
        .direct_record_store(crate::schema::SETTLED_RESULT_MEMBERS_STORE)
        .unwrap();
    futures::executor::block_on(store.set(
        &[
            Value::Uuid(key.shape_id.0),
            Value::Uuid(key.binding_id.0),
            Value::Uuid(key.read_view.id),
            Value::Bytes(vec![0xff; 32]),
        ],
        &[Value::Bytes(
            crate::node::codec::result_member_storage_bytes(&synthetic).unwrap(),
        )],
    ))
    .unwrap();
    assert!(futures::executor::block_on(reopened.recover_known_state_facts()).is_err());
    assert!(reopened.query.settled_result_sets.is_empty());
    assert!(reopened.query.settled_through_by_binding_view.is_empty());
}

#[test]
fn corrupt_settled_program_fact_recovery_does_not_publish_a_valid_prefix() {
    // Internal recovery-boundary coverage: force a valid persisted fact followed
    // by a malformed durable key and verify recovery clears the resident
    // closure rather than publishing a partially decoded prefix. A failed
    // durable recovery is fail-closed, not a request to preserve potentially
    // stale state that was loaded before the corruption was detected.
    let (_reader_dir, mut reader) = open_node_with_uuid(node(3));
    let (shape, binding) = reader.whole_table_shape_binding("todos").unwrap();
    register_shape_binding(&mut reader, &shape, &binding);
    let subscription = reader.whole_table_subscription_key("todos").unwrap();
    let fact = crate::protocol::ProgramFactEntry::PathCorrelationCoverage(
        crate::protocol::PathCorrelationCoverageEntry {
            path: "valid".to_owned(),
            source_table: "todos".to_owned().into(),
            source_row: row(43),
            correlation_key: vec![1],
            complete: true,
        },
    );
    reader
        .apply_sync_message_settled(SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
            subscription,
            settled_through: GlobalTime(1),
            reset_result_set: false,
            version_carriers: Vec::new(),
            peer_payload_inventory: crate::protocol::PeerPayloadInventory::default(),
            result_member_adds: Vec::new(),
            result_member_removes: Vec::new(),
            terminal_operations: Vec::new(),
            program_fact_adds: vec![fact.clone()],
            program_fact_removes: Vec::new(),
        }))
        .unwrap();
    let corrupt_store = reader
        .database
        .direct_record_store(crate::schema::SETTLED_PROGRAM_FACTS_STORE)
        .unwrap();
    futures::executor::block_on(corrupt_store.set(
            &[
                Value::Uuid(uuid::Uuid::from_bytes([0xff; 16])),
                Value::Uuid(uuid::Uuid::from_bytes([0xff; 16])),
                Value::Uuid(uuid::Uuid::from_bytes([0xff; 16])),
                // A full-sized but wrong digest proves recovery verifies the
                // key/value binding rather than only the digest's shape.
                Value::Bytes(vec![0xff; 32]),
            ],
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
        ..
    }) = update
    else {
        panic!("expected view update");
    };
    assert_eq!(
        result_member_adds,
        vec![crate::protocol::ResultMemberEntry::from((
            groove::Intern::from("todos".to_owned()),
            row_uuid,
            tx_id,
        ))]
    );
    assert_eq!(version_bundles.len(), 1);
    assert_eq!(version_bundles[0].tx.tx_id, tx_id);
    assert_eq!(version_bundles[0].versions.len(), 1);
}
