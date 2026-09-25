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
) -> crate::protocol::SupportingRow {
    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        supporting_rows: program_fact_adds,
        ..
    }) = update
    else {
        panic!("expected view update");
    };
    program_fact_adds
        .added_rows()
        .iter()
        .find_map(|fact| match fact {
            input if input.row == row_uuid => Some(input.clone()),
            _ => None,
        })
        .expect("authority update must identify the row as an exact covered input")
}

fn view_update_parts(message: SyncMessage, defer_settlement: bool) -> ViewUpdateParts {
    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        subscription,
        settled_through,

        version_carriers,
        peer_payload_inventory,
        supporting_rows: program_fact_adds,
    }) = message
    else {
        panic!("expected view update");
    };
    ViewUpdateParts {
        wire_rows: Some(program_fact_adds),
        subscription,
        settled_through,
        defer_settlement,
        reset_input_set: true,
        version_carriers,
        peer_complete_tx_payload_refs: peer_payload_inventory.complete_tx_payloads,
        authorization_progress: peer_payload_inventory.authorization_progress,
        opening_pending: peer_payload_inventory.opening_pending,
        result_member_adds: Vec::new(),
        result_member_removes: Vec::new(),
    }
}

#[test]
fn physical_deltas_require_exact_predecessors_and_reopen_requires_snapshot() {
    // Internal protocol/work-bound coverage: public clients cannot inject a
    // wrong predecessor or inspect physical manifest size. Query assertions
    // below still check the receiver's derived rows, not just wire fields.
    use crate::protocol::SupportingRowsUpdate;
    let (reader_dir, mut reader) = open_node_with_uuid(node(0xb3));
    let (_core_dir, mut core) = open_node_with_uuid(node(0xb2));
    let (shape, binding) = core.whole_table_shape_binding("todos").unwrap();
    let subscription = core.whole_table_subscription_key("todos").unwrap();
    register_shape_binding(&mut reader, &shape, &binding);
    for index in 0..32u8 {
        accept_global(
            &mut core,
            MergeableCommit::new("todos", row(index), 1000 + u64::from(index))
                .cells(title_cells("before")),
        );
    }
    let mut peer = relay_with_system_binding(subscription);
    let initial = peer.rehydrate_query(&mut core, &shape, &binding).unwrap();
    let SyncMessage::ViewUpdate(view) = &initial else {
        unreachable!()
    };
    assert!(view.supporting_rows.is_snapshot());
    assert_eq!(view.supporting_rows.added_rows().len(), 32);
    let initial_revision = view.supporting_rows.revision();
    reader.apply_sync_message_settled(initial).unwrap();
    let key = reader
        .authority_result_key_for_subscription(subscription)
        .unwrap();
    let original_facts = reader.query.authority_results[&key]
        .covered_input_versions
        .clone();
    let noop = peer.query_update(&mut core, &shape, &binding).unwrap();
    let SyncMessage::ViewUpdate(view) = &noop else {
        unreachable!()
    };
    assert_eq!(
        view.supporting_rows.revision(),
        initial_revision,
        "discardable no-op cannot advance the chain"
    );
    reader.apply_sync_message_settled(noop).unwrap();
    let mut updates = Vec::new();
    let mut predecessor = initial_revision;
    for index in 0..2u64 {
        accept_global(
            &mut core,
            MergeableCommit::new("todos", row(7), 2000 + index).cells(title_cells(if index == 0 {
                "first"
            } else {
                "second"
            })),
        );
        let update = peer.query_update(&mut core, &shape, &binding).unwrap();
        let SyncMessage::ViewUpdate(view) = &update else {
            unreachable!()
        };
        let SupportingRowsUpdate::Delta {
            predecessor: actual,
            revision,
            adds,
            removes,
        } = &view.supporting_rows
        else {
            panic!("ordinary successor must not reconstruct a snapshot")
        };
        assert_eq!(*actual, predecessor);
        assert_eq!(adds.len(), 1);
        assert_eq!(removes.len(), 1);
        assert_eq!(adds[0].row, row(7));
        assert_eq!(removes[0].row, row(7));
        predecessor = *revision;
        updates.push(update);
    }
    assert!(
        reader
            .apply_sync_message_settled(updates[1].clone())
            .is_err(),
        "cannot skip a predecessor"
    );
    assert_eq!(
        reader.query.authority_results[&key].covered_input_versions,
        original_facts
    );
    reader
        .apply_view_updates_in_batch(
            updates
                .iter()
                .cloned()
                .map(|update| view_update_parts(update, false))
                .collect(),
        )
        .resolve()
        .unwrap();
    assert_eq!(
        reader.query.authority_results[&key].supporting_revision,
        Some(predecessor)
    );
    assert_eq!(
        receiver_rows(&mut reader, &shape, &binding, DurabilityTier::Global).len(),
        32
    );
    drop(reader);
    let mut reopened = open_node_at(&reader_dir, schema());
    register_shape_binding(&mut reopened, &shape, &binding);
    assert!(
        reopened
            .apply_sync_message_settled(updates.pop().unwrap())
            .is_err(),
        "durable facts are not a recovered transport predecessor"
    );
    reopened
        .apply_sync_message_settled(system_authority_reset(
            &mut core,
            &shape,
            &binding,
            subscription,
        ))
        .unwrap();
    assert_eq!(
        receiver_rows(&mut reopened, &shape, &binding, DurabilityTier::Global).len(),
        32
    );
}

#[test]
fn physical_manifest_normalization_reuses_only_the_existing_admitted_source_cache() {
    // Internal mechanism receipt: public rows cannot show redundant compiler
    // discovery or explicitly evict a derived authority-receipt cache.
    use crate::node::query_eval::take_covered_input_source_discoveries_for_test as discoveries;
    let (reader_dir, mut reader) = open_node_with_uuid(node(0xc3));
    let (_writer_dir, mut writer) = open_node_with_uuid(node(0xc1));
    let (_core_dir, mut core) = open_node_with_uuid(node(0xc2));
    let (shape, binding) = core.whole_table_shape_binding("todos").unwrap();
    let subscription = core.whole_table_subscription_key("todos").unwrap();
    register_shape_binding(&mut reader, &shape, &binding);
    commit_mergeable_global(
        &mut writer,
        &mut core,
        MergeableCommit::new("todos", row(0xc4), 15).cells(title_cells("source cache")),
    );
    let reset = system_authority_reset(&mut core, &shape, &binding, subscription);
    discoveries();
    reader.apply_sync_message_settled(reset.clone()).unwrap();
    assert!(
        discoveries() > 0,
        "first receipt must discover its compiler-owned sources"
    );
    let key = reader
        .authority_result_key_for_subscription(subscription)
        .unwrap();
    let facts = reader.query.authority_results[&key]
        .covered_input_versions
        .clone();
    assert!(
        reader.query.authority_results[&key]
            .compiled_covered_input_sources
            .is_some()
    );

    reader.apply_sync_message_settled(reset.clone()).unwrap();
    assert_eq!(
        discoveries(),
        0,
        "successor normalization reuses admitted capabilities"
    );
    assert_eq!(
        reader.query.authority_results[&key].covered_input_versions,
        facts
    );
    let SyncMessage::ViewUpdate(mut malformed) = reset.clone() else {
        unreachable!()
    };
    {
        let duplicate = malformed.supporting_rows.added_rows()[0].clone();
        malformed.supporting_rows.added_rows_mut().push(duplicate);
    }
    assert!(
        reader
            .apply_sync_message_settled(SyncMessage::ViewUpdate(malformed))
            .is_err()
    );
    assert_eq!(discoveries(), 0);
    assert_eq!(
        reader.query.authority_results[&key].covered_input_versions,
        facts
    );

    reader
        .query
        .authority_results
        .get_mut(&key)
        .unwrap()
        .compiled_covered_input_sources = None;
    reader.apply_sync_message_settled(reset.clone()).unwrap();
    assert!(
        discoveries() > 0,
        "cache miss follows the normal compiler path"
    );
    assert_eq!(
        receiver_rows(&mut reader, &shape, &binding, DurabilityTier::Global).len(),
        1
    );
    drop(reader);
    let mut reopened = open_node_at(&reader_dir, schema());
    register_shape_binding(&mut reopened, &shape, &binding);
    discoveries();
    reopened.apply_sync_message_settled(reset).unwrap();
    assert!(
        discoveries() > 0,
        "recovery does not persist derived compiler capabilities"
    );
    assert_eq!(
        receiver_rows(&mut reopened, &shape, &binding, DurabilityTier::Global).len(),
        1
    );
}

#[test]
fn physical_manifest_cache_is_receipt_scoped_and_cleared_by_legacy_deferred_and_reopen() {
    // Internal cache-lifetime proof: public rows cannot expose retained
    // predecessors, legacy fact frames, or recovery's derived-cache absence.
    let (reader_dir, mut reader) = open_node_with_uuid(node(0xd3));
    let (_writer_dir, mut writer) = open_node_with_uuid(node(0xd1));
    let (_core_dir, mut core) = open_node_with_uuid(node(0xd2));
    let (shape, binding) = core.whole_table_shape_binding("todos").unwrap();
    let subscription = core.whole_table_subscription_key("todos").unwrap();
    register_shape_binding(&mut reader, &shape, &binding);
    commit_mergeable_global(
        &mut writer,
        &mut core,
        MergeableCommit::new("todos", row(0xd4), 15).cells(title_cells("cached manifest")),
    );
    let reset = system_authority_reset(&mut core, &shape, &binding, subscription);
    reader.apply_sync_message_settled(reset.clone()).unwrap();
    let key = reader
        .authority_result_key_for_subscription(subscription)
        .unwrap();
    let original = reader.query.authority_results[&key]
        .supporting_revision
        .unwrap();
    let facts = reader.query.authority_results[&key]
        .covered_input_versions
        .clone();
    let SyncMessage::ViewUpdate(initial_view) = &reset else {
        unreachable!()
    };
    assert_eq!(original, initial_view.supporting_rows.revision());

    let SyncMessage::ViewUpdate(mut malformed) = reset.clone() else {
        unreachable!()
    };
    {
        let duplicate = malformed.supporting_rows.added_rows()[0].clone();
        malformed.supporting_rows.added_rows_mut().push(duplicate);
    }
    assert!(
        reader
            .apply_sync_message_settled(SyncMessage::ViewUpdate(malformed))
            .is_err()
    );
    assert_eq!(
        reader.query.authority_results[&key].supporting_revision,
        Some(original)
    );
    assert_eq!(
        reader.query.authority_results[&key].covered_input_versions,
        facts
    );

    let mut legacy = view_update_parts(reset.clone(), false);
    legacy.wire_rows = None;
    legacy.reset_input_set = false;
    legacy.version_carriers.clear();
    reader
        .apply_view_updates_in_batch(vec![view_update_parts(reset.clone(), false), legacy])
        .resolve()
        .unwrap();
    assert!(
        reader.query.authority_results[&key]
            .supporting_revision
            .is_none()
    );
    assert_eq!(
        reader.query.authority_results[&key].covered_input_versions,
        facts
    );
    reader.apply_sync_message_settled(reset.clone()).unwrap();
    assert!(
        reader.query.authority_results[&key]
            .supporting_revision
            .is_some()
    );

    reader
        .apply_view_update(view_update_parts(reset.clone(), true))
        .resolve()
        .unwrap();
    assert!(
        reader.query.authority_results[&key]
            .supporting_revision
            .is_none()
    );
    reader.apply_sync_message_settled(reset.clone()).unwrap();
    assert!(
        reader.query.authority_results[&key]
            .supporting_revision
            .is_some()
    );
    reader.clear_settled_result_view(key.clone());
    assert!(
        reader.query.authority_results[&key]
            .supporting_revision
            .is_none()
    );
    reader.apply_sync_message_settled(reset.clone()).unwrap();
    assert_eq!(
        reader.query.authority_results[&key].covered_input_versions,
        facts
    );
    assert_eq!(
        receiver_rows(&mut reader, &shape, &binding, DurabilityTier::Global).len(),
        1
    );
    drop(reader);
    let mut reopened = open_node_at(&reader_dir, schema());
    assert!(
        reopened
            .query
            .authority_results
            .values()
            .all(|state| state.supporting_revision.is_none())
    );
    register_shape_binding(&mut reopened, &shape, &binding);
    reopened.apply_sync_message_settled(reset).unwrap();
    let state = &reopened.query.authority_results[&key];
    assert!(state.supporting_revision.is_some());
    assert_eq!(state.covered_input_versions, facts);
}

#[test]
fn physical_manifest_cache_never_outlives_its_facts_across_cancelled_receive_writes() {
    // Internal cancellation proof requires pausing individual durable writes
    // and inspecting a derived predecessor, neither exposed by public clients.
    use groove::storage::{TestStorage, TestStorageOperation};
    let (_writer_dir, mut writer) = open_node_with_uuid(node(0xe3));
    let (_core_dir, mut core) = open_node_with_uuid(node(0xe4));
    let (shape, binding) = core.whole_table_shape_binding("todos").unwrap();
    let subscription = core.whole_table_subscription_key("todos").unwrap();
    commit_mergeable_global(
        &mut writer,
        &mut core,
        MergeableCommit::new("todos", row(0xe5), 10).cells(title_cells("first")),
    );
    let initial = system_authority_reset(&mut core, &shape, &binding, subscription);
    commit_mergeable_global(
        &mut writer,
        &mut core,
        MergeableCommit::new("todos", row(0xe6), 11).cells(title_cells("second")),
    );
    let successor = system_authority_reset(&mut core, &shape, &binding, subscription);
    let schema = schema();
    let families = schema.column_families();
    let family_refs = families.iter().map(String::as_str).collect::<Vec<_>>();
    let mut completed = false;
    let mut observed_invalidation = false;
    for allowed_writes in 0..24 {
        let (storage, control) = TestStorage::controlled(&family_refs);
        let reopen_handle = storage.clone();
        let mut reader =
            NodeState::new_with_shared_test_catalogue(node(0xe7), schema.clone(), storage).unwrap();
        register_shape_binding(&mut reader, &shape, &binding);
        reader.apply_sync_message_settled(initial.clone()).unwrap();
        let key = reader
            .authority_result_key_for_subscription(subscription)
            .unwrap();
        assert!(
            reader.query.authority_results[&key]
                .supporting_revision
                .is_some()
        );
        let predecessor_facts = reader.query.authority_results[&key]
            .covered_input_versions
            .clone();
        let predecessor_revision = reader.query.authority_results[&key].supporting_revision;
        control.take_observed();
        control.pause_on(TestStorageOperation::WriteMany);
        let mut receive =
            Box::pin(reader.apply_view_update(view_update_parts(successor.clone(), false)));
        let mut released = 0;
        let mut stopped = false;
        for _ in 0..50_000 {
            match std::future::Future::poll(
                receive.as_mut(),
                &mut std::task::Context::from_waker(std::task::Waker::noop()),
            ) {
                std::task::Poll::Ready(result) => {
                    result.unwrap();
                    completed = true;
                    stopped = true;
                    break;
                }
                std::task::Poll::Pending => {}
            }
            let writes = control
                .observed()
                .iter()
                .filter(|op| **op == TestStorageOperation::WriteMany)
                .count();
            if writes > allowed_writes {
                stopped = true;
                break;
            }
            if writes > released {
                control.release_one();
                released = writes;
            }
        }
        assert!(stopped, "receive did not reach a bounded write boundary");
        drop(receive);
        let state = &reader.query.authority_results[&key];
        if let Some(revision) = state.supporting_revision {
            if Some(revision) == predecessor_revision {
                assert_eq!(state.covered_input_versions, predecessor_facts);
            } else {
                assert!(
                    completed,
                    "successor revision cannot survive incomplete installation"
                );
                let SyncMessage::ViewUpdate(view) = &successor else {
                    unreachable!()
                };
                assert_eq!(revision, view.supporting_rows.revision());
            }
        } else {
            observed_invalidation = true;
        }
        if completed {
            assert!(state.supporting_revision.is_some());
        }
        drop(reader);
        control.resume();
        let storage = crate::db::block_on(reopen_handle.reopen(families.clone())).unwrap();
        let reopened =
            NodeState::new_with_shared_test_catalogue(node(0xe7), schema.clone(), storage).unwrap();
        assert!(
            reopened
                .query
                .authority_results
                .values()
                .all(|state| state.supporting_revision.is_none())
        );
        if completed {
            break;
        }
    }
    assert!(completed, "all receive writes must be covered");
    assert!(
        observed_invalidation,
        "must pause after invalidation and before success"
    );
    // An unchanged body-free confirmation is entirely in memory: even an
    // armed storage failure cannot affect it because no scope write exists.
    let (storage, control) = TestStorage::controlled(&family_refs);
    let mut reader =
        NodeState::new_with_shared_test_catalogue(node(0xe8), schema, storage).unwrap();
    register_shape_binding(&mut reader, &shape, &binding);
    reader.apply_sync_message_settled(initial.clone()).unwrap();
    let key = reader
        .authority_result_key_for_subscription(subscription)
        .unwrap();
    let mut replay = view_update_parts(initial, false);
    replay.version_carriers.clear();
    control.take_observed();
    control.fail_next(TestStorageOperation::WriteMany);
    reader.apply_view_update(replay).resolve().unwrap();
    assert!(
        !control
            .observed()
            .contains(&TestStorageOperation::WriteMany)
    );
    assert!(
        reader.query.authority_results[&key]
            .supporting_revision
            .is_some()
    );
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

        version_carriers: Vec::new(),
        peer_payload_inventory: crate::protocol::PeerPayloadInventory::default(),
        supporting_rows: crate::protocol::SupportingRowsUpdate::snapshot(Vec::new()),
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

        version_carriers: Vec::new(),
        peer_payload_inventory: crate::protocol::PeerPayloadInventory::default(),
        supporting_rows: crate::protocol::SupportingRowsUpdate::snapshot(Vec::new()),
    });

    reader.apply_sync_message_settled(late).unwrap();

    assert_eq!(
        reader.sync_metrics().dropped_detached_subscription_messages,
        1
    );
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
    let _covered = covered_input_for_row(&initial, row_uuid);
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

        version_carriers: Vec::new(),
        peer_payload_inventory: crate::protocol::PeerPayloadInventory::default(),
        supporting_rows: crate::protocol::SupportingRowsUpdate::snapshot(Vec::new()),
    });
    assert!(
        reader
            .missing_known_state_row_version_refs(&removal)
            .unwrap()
            .is_empty(),
        "removals must not request repair bodies because the removed version may be policy-invisible"
    );
    reader.apply_sync_message_settled(removal).unwrap();
    assert!(receiver_rows(&mut reader, &shape, &binding, DurabilityTier::Global).is_empty());
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

        version_carriers: Vec::new(),
        peer_payload_inventory: crate::protocol::PeerPayloadInventory::default(),
        supporting_rows: crate::protocol::SupportingRowsUpdate::snapshot(Vec::new()),
    });

    assert!(
        reader
            .missing_known_state_row_version_refs(&removal)
            .unwrap()
            .is_empty()
    );
    reader.apply_sync_message_settled(removal).unwrap();
    assert!(receiver_rows(&mut reader, &shape, &binding, DurabilityTier::Global).is_empty());
    assert_eq!(
        reader.settled_through_for_authority_result(&authority_result_key),
        Some(GlobalTime(3))
    );
}

#[test]
fn complete_empty_snapshot_for_duplicate_usage_replaces_canonical_view() {
    // INV-SYNC-44: exercise the complete-snapshot receiver contract.
    // A duplicate usage shares the canonical authority view. An explicitly
    // empty complete snapshot must clear its previous membership, even when
    // the old native row is still present in the receiver cache.
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
        .apply_sync_message_settled(SyncMessage::ViewUpdate(
            crate::protocol::ViewUpdatePayload {
                subscription: duplicate_subscription,
                settled_through: GlobalTime(2),

                version_carriers: Vec::new(),
                peer_payload_inventory: crate::protocol::PeerPayloadInventory::default(),
                supporting_rows: crate::protocol::SupportingRowsUpdate::snapshot(Vec::new()),
            },
        ))
        .unwrap();

    assert_eq!(
        receiver_rows(&mut reader, &shape, &binding, DurabilityTier::Global)
            .into_iter()
            .map(current_row_pair)
            .collect::<BTreeMap<_, _>>(),
        BTreeMap::new()
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
        peer_payload_inventory,
        ..
    }) = &update
    else {
        panic!("expected view update");
    };
    assert_eq!(*settled_through, GlobalTime::new(10, 0).unwrap());
    // A cursor deduplicates bodies, not the new usage's input manifest.
    assert!(!peer_payload_inventory.opening_pending);
    assert!(version_bundles.is_empty());

    let missing = reader
        .missing_known_state_row_version_refs(&update)
        .unwrap();
    assert_eq!(
        missing,
        vec![crate::protocol::RowVersionRef::new(
            "todos", row_uuid, _tx_id
        )]
    );
    // Alice's claimed cursor was ahead of her retained payloads. Repair the
    // actual missing body, then verify the same rows as the undeduplicated
    // control opening rather than accepting an empty apparent success.
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
        receiver_rows(&mut reader, &shape, &binding, DurabilityTier::Global)
            .into_iter()
            .map(current_row_pair)
            .collect::<BTreeMap<_, _>>(),
        BTreeMap::from([(row_uuid, title_cells("known"))])
    );
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
        .commit_mergeable_unit_settled(
            MergeableCommit::new("todos", row_a, 10).cells(title_cells("known")),
        )
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
        .commit_mergeable_unit_settled(
            MergeableCommit::new("todos", row_b, 20).cells(title_cells("new")),
        )
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
        peer_payload_inventory,
        supporting_rows: program_fact_adds,
        ..
    }) = &update
    else {
        panic!("expected view update");
    };
    assert_eq!(*settled_through, GlobalTime::new(20, 0).unwrap());
    assert!(!peer_payload_inventory.opening_pending);
    assert!(program_fact_adds.added_rows().iter().any(|fact| matches!(
        fact,
        input
            if input.row == row_b && input.version.tx == tx_b
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
        supporting_rows: program_fact_adds,
        ..
    }) = &update
    else {
        panic!("expected view update");
    };
    assert!(program_fact_adds.added_rows().iter().any(|fact| matches!(
        fact,
        input
            if input.row == row_uuid && input.version.tx == tx_id
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
        peer_payload_inventory,
        ..
    }) = &update
    else {
        panic!("expected view update");
    };
    // Reattaching restores the full input manifest without retransmitting
    // known bodies, even when Alice still holds the previous live closure.
    assert!(!peer_payload_inventory.opening_pending);
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
fn reopened_reader_keeps_local_rows_and_requires_fresh_remote_snapshot() {
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
        receiver_rows(&mut reader, &shape, &binding, DurabilityTier::Local)
            .into_iter()
            .map(current_row_pair)
            .collect::<BTreeMap<_, _>>(),
        BTreeMap::from([(row_uuid, title_cells("known"))])
    );

    let mut peer = relay_with_system_binding(subscription);
    let authority = AuthorityResultKey::unscoped(BindingViewKey {
        shape_id: shape.shape_id(),
        binding_id: binding.binding_id(),
        read_view: subscription.read_view,
    });
    let declaration = reader
        .known_state_declaration_for_subscription(
            &shape,
            &binding,
            subscription,
            &[],
            AuthorSubject::SYSTEM,
            None,
        )
        .unwrap();
    assert!(declaration.is_none(), "restart has no retained cursor");
    assert!(!reader.has_settled_authority_result(&authority));
    peer.declare_known_state(subscription, declaration);

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
        peer_payload_inventory,
        supporting_rows,
        ..
    }) = &update
    else {
        panic!("expected view update");
    };
    // Native rows survive, but no scope-dependent body cursor does. A fresh
    // remote attachment therefore receives the complete native input again.
    assert!(!peer_payload_inventory.opening_pending);
    assert_eq!(version_bundles.len(), 1);
    assert_eq!(
        supporting_rows.added_rows().len(),
        1,
        "fresh response still supplies the complete input set"
    );
    assert!(!reader.has_settled_authority_result(&authority));
    reader.apply_sync_message_settled(update).unwrap();
    assert!(reader.has_settled_authority_result(&authority));
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
fn retired_empty_read_does_not_resurrect_a_marker_without_its_source_closure() {
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    let (_reader_dir, mut reader) = open_node_with_uuid(node(3));
    let (shape, binding) = core.whole_table_shape_binding("todos").unwrap();
    let subscription = core.whole_table_subscription_key("todos").unwrap();
    register_shape_binding(&mut reader, &shape, &binding);
    let reset = system_authority_reset(&mut core, &shape, &binding, subscription);
    reader.apply_sync_message_settled(reset).unwrap();
    assert!(receiver_rows(&mut reader, &shape, &binding, DurabilityTier::Global).is_empty());
    reader.apply_unsubscribe(subscription);

    assert_eq!(
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
        None
    );
    assert!(
        reader.query.authority_results.values().all(|state| {
            !matches!(state.source_closure, AuthoritySourceClosure::Claimed { .. })
        }),
        "a retired cursor must not resurrect a claimed closure without its source manifest"
    );

    register_shape_binding(&mut reader, &shape, &binding);
    let reset = system_authority_reset(&mut core, &shape, &binding, subscription);
    reader.apply_sync_message_settled(reset).unwrap();
    assert!(receiver_rows(&mut reader, &shape, &binding, DurabilityTier::Global).is_empty());
}

#[test]
fn over_cap_slow_known_state_declaration_degrades_to_full_ship() {
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    let (shape, binding) = core.whole_table_shape_binding("todos").unwrap();
    let subscription = core.whole_table_subscription_key("todos").unwrap();
    // Exact version-set declarations are retired: without a watermark a
    // receiver declares nothing and the serving peer ships the full set.

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
fn fast_known_state_is_process_local_and_invalidated_by_eviction() {
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
    // A row-local view resumes "Q at W" from its stored watermark; it is
    // not a Fast body-dedup cursor and does not restore live settlement.
    assert!(
        matches!(
            declaration,
            Some(crate::protocol::KnownStateDeclaration::Watermark { .. })
        ),
        "restart resumes a row-local view from its stored watermark: {declaration:?}"
    );
    let authority = AuthorityResultKey::unscoped(BindingViewKey {
        shape_id: shape.shape_id(),
        binding_id: binding.binding_id(),
        read_view: subscription.read_view,
    });
    assert!(
        !reopened.has_settled_authority_result(&authority),
        "advertising cached payloads must not restore live authority"
    );

    let report = reopened.evict_cold().unwrap();
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
fn two_table_client_cache_budget_counts_shared_physical_history_once_without_eviction() {
    // This is intentionally an internal storage-boundary receipt: the client cache
    // budget is the observable policy seam, but only the backing storage can
    // provide an independent byte count for its shared physical history class.
    let test_schema = todos_notes_schema();
    let (_writer_dir, mut writer) = open_node_with_schema(node(1), test_schema.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(9), test_schema.clone());
    let column_families = test_schema.column_families();
    let refs = column_families
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    let storage = FailWriteManyMemoryStorage::new(&refs);
    let mut reader =
        NodeState::new_with_shared_test_catalogue(node(3), test_schema, storage).unwrap();
    let rows = [
        (
            "todos",
            row(0x76),
            BTreeMap::from([("title".to_owned(), v("cached todo"))]),
        ),
        (
            "notes",
            row(0x77),
            BTreeMap::from([("body".to_owned(), v("cached note"))]),
        ),
    ];

    for (table, row_uuid, cells) in &rows {
        commit_mergeable_global(
            &mut writer,
            &mut core,
            MergeableCommit::new(*table, *row_uuid, 18).cells(cells.clone()),
        );
        let (shape, binding) = core.whole_table_shape_binding(table).unwrap();
        let subscription = core.whole_table_subscription_key(table).unwrap();
        register_shape_binding(&mut reader, &shape, &binding);
        let update = relay_with_system_binding(subscription)
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
    }

    let physical_history_bytes = reader
        .database
        .approximate_class_bytes("__groove_class_history")
        .resolve()
        .unwrap()
        .expect("memory storage meters its physical history class");
    assert!(physical_history_bytes > 0);
    let report = reader
        .enforce_client_cache_budget(
            ClientCacheBudget::new(physical_history_bytes),
        )
        .resolve()
        .unwrap();

    assert!(
        report.is_none(),
        "two logical history families share one {physical_history_bytes}-byte physical class, so the exact physical budget must not trigger eviction: {report:?}"
    );
    for (table, row_uuid, _) in rows {
        assert_eq!(
            reader.row_history(table, row_uuid).unwrap().len(),
            1,
            "a non-triggering physical budget must retain the cached {table} row"
        );
    }
}

#[derive(Clone, Copy)]
enum EvictionFailurePath {
    ManualBatch,
    BudgetedPerCandidate,
}

#[derive(Clone, Copy)]
enum EvictionPersistenceOutcome {
    FailBeforeDelegation,
    WriteThroughThenError,
}

fn assert_eviction_failure_contract(
    path: EvictionFailurePath,
    outcome: EvictionPersistenceOutcome,
    row_uuid: RowUuid,
    global_time: u64,
    title: &'static str,
) {
    let (_writer_dir, mut writer) = open_node_with_uuid(node(1));
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    let (shape, binding) = core.whole_table_shape_binding("todos").unwrap();
    let subscription = core.whole_table_subscription_key("todos").unwrap();
    let tx_id = commit_mergeable_global(
        &mut writer,
        &mut core,
        MergeableCommit::new("todos", row_uuid, global_time).cells(title_cells(title)),
    );
    let column_families = schema().column_families();
    let refs = column_families
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    let storage = FailWriteManyMemoryStorage::new(&refs);
    let mut reader =
        NodeState::new_with_shared_test_catalogue(node(3), schema(), storage.clone()).unwrap();
    register_shape_binding(&mut reader, &shape, &binding);
    let update = relay_with_system_binding(subscription)
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
    let persisted_history = reader.row_history("todos", row_uuid).unwrap();
    let persisted_versions = reader.query_versions_for_tx(tx_id).unwrap();
    assert_eq!(persisted_versions.len(), 1);
    let persisted_version = persisted_versions
        .first()
        .expect("persisted version count was checked above");
    let logical_history_table = reader
        .version_storage_table_for_row(persisted_version)
        .unwrap()
        .to_string();
    let logical_history_key = history_primary_key(persisted_version).into_bytes();
    let (history_table, history_key) =
        jazz_class_v1_history_physical_target(&logical_history_table, &logical_history_key);
    assert!(reader.cached_tx_version_tables(tx_id).is_some());
    reader.cache_tx_versions(tx_id, persisted_versions.clone());
    assert!(reader.cached_tx_versions(tx_id).is_some());
    // Internal durable-boundary receipt: a reopened node intentionally lacks
    // the live authority handoff needed to declare Fast, so that public API
    // result alone cannot distinguish an evicted durable fact from a stale
    // one. This isolated store has exactly this subscription's fact.
    let known_state_facts = reader
        .database
        .direct_record_store(crate::schema::KNOWN_STATE_FACTS_STORE)
        .unwrap();
    assert_eq!(
        futures::executor::block_on(known_state_facts.prefix_entries(&[]))
            .unwrap()
            .len(),
        0,
        "settled updates never persist a subscription cursor"
    );
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

    match outcome {
        EvictionPersistenceOutcome::FailBeforeDelegation => {
            storage.fail_write_many_on_delete(history_table, history_key);
        }
        EvictionPersistenceOutcome::WriteThroughThenError => {
            storage.fail_write_many_on_delete_after_write_through(history_table, history_key);
        }
    }
    match path {
        EvictionFailurePath::ManualBatch => {
            reader
                .evict_cold()
                .resolve()
                .expect_err("manual eviction must reach the injected persistence failure");
        }
        EvictionFailurePath::BudgetedPerCandidate => {
            reader
                .enforce_client_cache_budget(ClientCacheBudget::new(0))
                .resolve()
                .expect_err("budgeted eviction must reach the injected persistence failure");
        }
    }
    // Public recovery proves durable body and known-state behaviour, but only
    // this private receipt can observe cache removal without querying a
    // persistence-poisoned live node.
    assert!(reader.cached_tx_versions(tx_id).is_none());
    assert!(reader.cached_tx_version_tables(tx_id).is_none());
    drop(reader);

    let mut reopened =
        NodeState::new_with_shared_test_catalogue(node(3), schema(), storage).unwrap();
    let known_state_facts = reopened
        .database
        .direct_record_store(crate::schema::KNOWN_STATE_FACTS_STORE)
        .unwrap();
    assert!(
        futures::executor::block_on(known_state_facts.prefix_entries(&[]))
            .unwrap()
            .is_empty(),
        "eviction must durably remove every fast known-state fact even when body persistence fails"
    );
    let reopened_history = reopened.row_history("todos", row_uuid).unwrap();
    match outcome {
        EvictionPersistenceOutcome::FailBeforeDelegation => {
            assert_eq!(reopened_history, persisted_history);
        }
        EvictionPersistenceOutcome::WriteThroughThenError => {
            assert!(reopened_history.is_empty());
        }
    }
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
    assert!(!matches!(
        declaration,
        Some(crate::protocol::KnownStateDeclaration::Fast { .. })
    ));
}

#[test]
fn manual_eviction_fail_before_error_preserves_body_and_clears_fast_known_state_and_transaction_cache()
 {
    assert_eviction_failure_contract(
        EvictionFailurePath::ManualBatch,
        EvictionPersistenceOutcome::FailBeforeDelegation,
        row(0x78),
        14,
        "fail before",
    );
}

#[test]
fn manual_eviction_write_through_error_clears_fast_known_state_and_transaction_cache() {
    assert_eviction_failure_contract(
        EvictionFailurePath::ManualBatch,
        EvictionPersistenceOutcome::WriteThroughThenError,
        row(0x79),
        15,
        "write-through",
    );
}

#[test]
fn budgeted_eviction_fail_before_error_preserves_body_and_clears_fast_known_state_and_transaction_cache()
 {
    assert_eviction_failure_contract(
        EvictionFailurePath::BudgetedPerCandidate,
        EvictionPersistenceOutcome::FailBeforeDelegation,
        row(0x7a),
        16,
        "budget fail before",
    );
}

#[test]
fn budgeted_eviction_write_through_error_removes_body_and_clears_fast_known_state_and_transaction_cache()
 {
    assert_eviction_failure_contract(
        EvictionFailurePath::BudgetedPerCandidate,
        EvictionPersistenceOutcome::WriteThroughThenError,
        row(0x7b),
        17,
        "budget write-through",
    );
}

#[test]
fn failed_body_eviction_still_invalidates_volatile_scope_and_cursors() {
    // Internal failure boundary: memory invalidation cannot fail, and happens
    // before a body write failure. Recovery retains the native body but no scope.
    let (_writer_dir, mut writer) = open_node_with_uuid(node(1));
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    let (shape, binding) = core.whole_table_shape_binding("todos").unwrap();
    let subscription = core.whole_table_subscription_key("todos").unwrap();
    let row_uuid = row(0x7c);
    let tx_id = commit_mergeable_global(
        &mut writer,
        &mut core,
        MergeableCommit::new("todos", row_uuid, 18).cells(title_cells("clear failure")),
    );
    let column_families = schema().column_families();
    let refs = column_families
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    let storage = FailWriteManyMemoryStorage::new(&refs);
    let mut reader =
        NodeState::new_with_shared_test_catalogue(node(3), schema(), storage.clone()).unwrap();
    register_shape_binding(&mut reader, &shape, &binding);
    let update = relay_with_system_binding(subscription)
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
    let persisted_history = reader.row_history("todos", row_uuid).unwrap();
    let persisted_versions = reader.query_versions_for_tx(tx_id).unwrap();
    reader.cache_tx_versions(tx_id, persisted_versions);
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
    assert!(reader.cached_tx_versions(tx_id).is_some());
    assert!(reader.cached_tx_version_tables(tx_id).is_some());

    storage.fail_nth_following_write_many(1);
    reader
        .evict_cold()
        .resolve()
        .expect_err("body deletion failure must preserve native history");

    assert!(!reader.query.authority_results.is_empty(), "live receipt sequencing survives eviction");
    for state in reader.query.authority_results.values() {
        assert_authority_proof_cleared(state);
        assert!(state.applied_view_update_generation > 0);
    }
    assert!(reader.cached_tx_versions(tx_id).is_none());
    assert!(reader.cached_tx_version_tables(tx_id).is_none());
    drop(reader);
    let mut reopened =
        NodeState::new_with_shared_test_catalogue(node(3), schema(), storage).unwrap();
    assert_eq!(
        reopened.row_history("todos", row_uuid).unwrap(),
        persisted_history
    );
    // The failed eviction deleted no body, so a stored watermark may still
    // resume the view; it never comes back as a Fast body-dedup cursor.
    assert!(!matches!(
        reopened
            .known_state_declaration_for_subscription(
                &shape,
                &binding,
                subscription,
                &[],
                AuthorSubject::SYSTEM,
                None
            )
            .unwrap(),
        Some(crate::protocol::KnownStateDeclaration::Fast { .. })
    ));
}

#[test]
fn storage_reopen_retains_rows_without_scope_or_fast_cursor() {
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
    // The row-local view resumes from its stored watermark ("Q at W"),
    // rebuilt from the retained rows; live settlement still needs Core.
    assert!(
        matches!(
            declaration,
            Some(crate::protocol::KnownStateDeclaration::Watermark { .. })
        ),
        "{declaration:?}"
    );
    assert_eq!(reopened.row_history("todos", row_uuid).unwrap().len(), 1);
    let authority = AuthorityResultKey::unscoped(BindingViewKey {
        shape_id: shape.shape_id(),
        binding_id: binding.binding_id(),
        read_view: subscription.read_view,
    });
    assert!(
        !reopened.has_settled_authority_result(&authority),
        "native data alone must not restore live settlement"
    );
}

#[test]
fn scope_updates_are_volatile_while_native_rows_survive_reopen() {
    // Internal boundary: public rows alone cannot prove there are no scope writes.
    // Public history/read assertions below also pin native-data retention.
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
    reader.apply_sync_message_settled(reset.clone()).unwrap();
    let facts = reader
        .query
        .authority_results
        .values()
        .next()
        .unwrap()
        .covered_input_versions
        .clone();
    assert_eq!(
        facts
            .values()
            .cloned()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>(),
        reset_payload.supporting_rows.added_rows(),
        "local compiled roles retain exactly the physical snapshot"
    );

    let mut removal = reset_payload.clone();
    removal.version_carriers.clear();
    removal.supporting_rows.added_rows_mut().clear();
    // The complete empty set removes every row while the receiver retains its
    // locally compiled source inventory for future snapshots.
    let manifest = BTreeMap::new();
    reader
        .apply_sync_message_settled(SyncMessage::ViewUpdate(removal))
        .unwrap();
    assert!(
        reader
            .query
            .authority_results
            .values()
            .any(|state| state.covered_input_versions == manifest)
    );
    reader.apply_sync_message_settled(reset).unwrap();
    let settled_facts_store = reader
        .database
        .direct_record_store(crate::schema::SETTLED_PROGRAM_FACTS_STORE)
        .unwrap();
    let durable_facts =
        futures::executor::block_on(settled_facts_store.prefix_entries(&[])).unwrap();
    assert!(
        durable_facts.is_empty(),
        "scope updates must not write durable membership"
    );
    assert!(
        crate::db::block_on(
            reader
                .database
                .direct_record_store(crate::schema::KNOWN_STATE_FACTS_STORE)
                .unwrap()
                .prefix_entries(&[])
        )
        .unwrap()
        .is_empty()
    );
    drop(reader);
    let mut reopened = open_node_at(&reader_dir, schema());
    assert!(reopened.query.authority_results.is_empty());
    assert_eq!(reopened.row_history("todos", row(42)).unwrap().len(), 1);
}

#[test]
fn covered_input_reset_and_reopen_have_no_result_member_store() {
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
    assert!(
        reader
            .database
            .direct_record_store("jazz_settled_result_members")
            .is_err()
    );
    drop(reader);
    let reopened = open_node_at(&reader_dir, schema());
    assert!(
        reopened
            .database
            .direct_record_store("jazz_settled_result_members")
            .is_err()
    );
    assert!(reopened.query.authority_results.is_empty());
}

#[test]
fn legacy_scope_caches_are_discarded_without_losing_native_or_pending_rows() {
    // Internal recovery boundary: public APIs cannot manufacture a retired
    // cache generation. All observable row/history assertions remain intact.
    let (reader_dir, mut reader) = open_node_with_uuid(node(3));
    let (_writer_dir, mut writer) = open_node_with_uuid(node(1));
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    let (shape, binding) = reader.whole_table_shape_binding("todos").unwrap();
    register_shape_binding(&mut reader, &shape, &binding);
    let subscription = reader.whole_table_subscription_key("todos").unwrap();
    let accepted = commit_mergeable_global(
        &mut writer,
        &mut core,
        MergeableCommit::new("todos", row(42), 15).cells(title_cells("retained")),
    );
    let reset = system_authority_reset(&mut core, &shape, &binding, subscription);
    reader.apply_sync_message_settled(reset.clone()).unwrap();
    let pending = reader
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row(43), 16).cells(title_cells("pending")),
        )
        .unwrap();
    let before_versions = reader.query_all_versions().unwrap();
    let pending_before = reader.query_transaction(pending).unwrap().unwrap();
    let store = reader
        .database
        .direct_record_store(crate::schema::SETTLED_PROGRAM_FACTS_STORE)
        .unwrap();
    // Retired JPFK and JSIR byte receipts, plus intentionally unparseable cache
    // data. Recovery must discard them without retaining any old decoder.
    let cursor_key = vec![
        Value::Uuid(shape.shape_id().0),
        Value::Uuid(binding.binding_id().0),
        Value::Uuid(subscription.read_view.id),
        Value::U8(0),
        Value::Bytes(vec![0; 32]),
    ];
    for (index, bytes) in [
        hex::decode("4a50464b010005000000746f646f73010000000001").unwrap(),
        hex::decode("4a5349520111111111111111111111111111111111050000007461736b73222222222222222222222222222222221f0000000000000033333333333333333333333333333333013434343434343434343434343434343400011f00000000000000333333333333333333333333333333330000").unwrap(),
        vec![0xff],
    ].into_iter().enumerate() {
        let mut key = cursor_key.clone();
        key.push(Value::Bytes(vec![index as u8; 32]));
        crate::db::block_on(store.set(&key, &[Value::Bytes(bytes)])).unwrap();
    }
    crate::db::block_on(
        reader
            .database
            .direct_record_store(crate::schema::KNOWN_STATE_FACTS_STORE)
            .unwrap()
            .set(
                &cursor_key,
                &[Value::U64(15), Value::U64(u64::MAX), Value::U64(1)],
            ),
    )
    .unwrap();
    drop(store);
    drop(reader);

    let mut reopened = open_node_at(&reader_dir, schema());
    assert!(reopened.query.authority_results.is_empty());
    for name in [
        crate::schema::KNOWN_STATE_FACTS_STORE,
        crate::schema::SETTLED_PROGRAM_FACTS_STORE,
    ] {
        assert!(
            crate::db::block_on(
                reopened
                    .database
                    .direct_record_store(name)
                    .unwrap()
                    .prefix_entries(&[])
            )
            .unwrap()
            .is_empty()
        );
    }
    assert_eq!(reopened.query_all_versions().unwrap(), before_versions);
    assert!(reopened.query_transaction(accepted).unwrap().is_some());
    let pending_after = reopened.query_transaction(pending).unwrap().unwrap();
    assert_eq!(pending_after.tx, pending_before.tx);
    assert_eq!(pending_after.fate, pending_before.fate);
    assert_eq!(pending_after.durability, pending_before.durability);
    register_shape_binding(&mut reopened, &shape, &binding);
    reopened.apply_sync_message_settled(reset).unwrap();
    assert!(
        reopened
            .query
            .authority_results
            .values()
            .any(|state| state.live_settled)
    );
    assert!(
        reopened
            .subscription_current_rows("todos", DurabilityTier::Global)
            .unwrap()
            .iter()
            .any(|input| input.row_uuid() == row(42))
    );
}

#[test]
fn retired_scope_payloads_never_restore_authority() {
    // Internal recovery boundary: even malformed retired cache bytes cannot
    // become authority evidence, and cleanup need not decode their payloads.
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
    reader.apply_sync_message_settled(reset).unwrap();
    let corrupt_store = reader
        .database
        .direct_record_store(crate::schema::SETTLED_PROGRAM_FACTS_STORE)
        .unwrap();
    let mut key = vec![
        Value::Uuid(shape.shape_id().0),
        Value::Uuid(binding.binding_id().0),
        Value::Uuid(subscription.read_view.id),
        Value::U8(0),
        Value::Bytes(vec![0; 32]),
    ];
    key.push(Value::Bytes(vec![0xff; 32]));
    futures::executor::block_on(corrupt_store.set(&key, &[Value::Bytes(vec![0xff])])).unwrap();
    drop(corrupt_store);
    futures::executor::block_on(reader.discard_legacy_subscription_scopes()).unwrap();
    // Cleanup discards storage only; it must not mutate an active live scope.
    assert!(!reader.query.authority_results.is_empty());
    let reopened = reader.reopen_in_place().unwrap();
    assert!(reopened.query.authority_results.is_empty());
}

#[test]
fn known_state_declaration_never_skips_pending_local_members() {
    let (_writer_dir, mut writer) = open_node_with_uuid(node(1));
    let (_relay_dir, mut relay) = open_node_with_uuid(node(7));
    let row_uuid = row(18);
    // A zero-offset exact-id Local read without a policy remains a genuinely
    // local relay evaluation: an unfated Local member must be visible even
    // though no Global receipt exists to source it.
    let shape = Query::from("todos")
        .filter(eq(col("id"), lit(Value::Uuid(row_uuid.0))))
        .validate(&schema())
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let opts = RegisterShapeOptions {
        tier: DurabilityTier::Local,
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
    relay.ingest_known_transaction(tx, versions, Fate::Pending, None, DurabilityTier::Local)
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
        .rehydrate_query_for_subscription_with_opts(&mut relay, subscription, &shape, &binding, opts)
        .unwrap()
        .expect("expected view update");
    let version_bundles = version_bundles_for_update(&update);
    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        supporting_rows: program_fact_adds,
        ..
    }) = update
    else {
        panic!("expected view update");
    };
    assert!(program_fact_adds.added_rows().iter().any(|fact| matches!(
        fact,
        input
            if input.row == row_uuid && input.version.tx == tx_id
    )));
    assert_eq!(version_bundles.len(), 1);
    assert_eq!(version_bundles[0].tx.tx_id, tx_id);
    assert_eq!(version_bundles[0].versions.len(), 1);
}

// These fields are internal authority evidence: row reads alone cannot prove
// that an invalidated predecessor or deferred publication was discarded.
fn assert_authority_proof_cleared(state: &AuthorityResultState) {
    assert_eq!(state.source_closure, AuthoritySourceClosure::Pending);
    assert!(state.source_incrementals.is_empty());
    assert!(!state.live_settled);
    assert!(state.supporting_revision.is_none());
    assert!(state.covered_input_versions.is_empty());
    assert!(state.compiled_covered_input_sources.is_none());
    assert!(state.settled_through.is_none());
    assert!(state.authorization_progress.is_none());
    assert!(!state.known_state_declared);
    assert!(!state.initial_hydration);
    assert!(!state.deferred_publication);
    assert!(state.pending_authoritative_reset.is_none());
    assert!(!state.pending_opening);
}

// The pending delivery threshold is internal; eviction and redelivery use
// real bodies so a no-op cache-budget pass cannot satisfy this regression.
#[test]
fn fresh_delivery_generation_advances_after_live_body_eviction() {
    for budgeted in [false, true] {
        let (_writer_dir, mut writer) = open_node_with_uuid(node(1));
        let (_core_dir, mut core) = open_node_with_uuid(node(9));
        let (_reader_dir, mut reader) = open_node_with_uuid(node(3));
        let (shape, binding) = core.whole_table_shape_binding("todos").unwrap();
        let subscription = core.whole_table_subscription_key("todos").unwrap();
        let row_uuid = row(0x7d);
        commit_mergeable_global(
            &mut writer,
            &mut core,
            MergeableCommit::new("todos", row_uuid, 19)
                .cells(title_cells("refresh after eviction")),
        );
        register_shape_binding(&mut reader, &shape, &binding);
        let update = relay_with_system_binding(subscription)
            .rehydrate_query_for_subscription_with_opts(
                &mut core,
                subscription,
                &shape,
                &binding,
                RegisterShapeOptions::default(),
            )
            .unwrap()
            .expect("authority reset");
        reader.apply_sync_message_settled(update.clone()).unwrap();
        let key = reader
            .authority_result_key_for_subscription(subscription)
            .unwrap();
        let required_after = reader.applied_authority_result_generation(&key);
        assert!(required_after > 0);
        if budgeted {
            reader
                .enforce_client_cache_budget(ClientCacheBudget::new(0))
                .resolve()
                .unwrap();
        } else {
            reader
                .evict_cold()
                .resolve()
                .unwrap();
        }
        assert!(
            reader.row_history("todos", row_uuid).unwrap().is_empty(),
            "actual body eviction occurred"
        );
        assert!(!reader.has_settled_authority_result(&key));
        assert!(
            reader.applied_authority_result_generation(&key) <= required_after,
            "eviction alone cannot satisfy a waiting read"
        );
        if let Some(state) = reader.query.authority_results.get(&key) {
            assert_authority_proof_cleared(state);
        }
        // The authority may now answer empty. That is a fresh delivery, not
        // permission to revive the evicted membership or its body inventory.
        reader
            .apply_sync_message_settled(SyncMessage::ViewUpdate(
                crate::protocol::ViewUpdatePayload {
                    subscription,
                    settled_through: GlobalTime(2),
                    version_carriers: Vec::new(),
                    peer_payload_inventory: crate::protocol::PeerPayloadInventory::default(),
                    supporting_rows: crate::protocol::SupportingRowsUpdate::snapshot(Vec::new()),
                },
            ))
            .unwrap();
        assert!(reader.has_settled_authority_result(&key));
        assert!(
            reader.applied_authority_result_generation(&key) > required_after,
            "fresh delivery must release a read waiting across eviction"
        );
        assert!(reader.row_history("todos", row_uuid).unwrap().is_empty());
    }
}
