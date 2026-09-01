// Usage filtering, differential convergence, restart, fate, and monotonicity behavior.

#[test]
fn view_updates_drop_unknown_usage_site_bindings() {
    let (_reader_dir, mut reader) = open_node_with_uuid(node(3));
    let canonical = reader.whole_table_subscription_key("todos").unwrap();
    let unknown_usage_site = SubscriptionKey {
        binding_id: BindingId(uuid::uuid!("77777777-7777-4777-9777-777777777777")),
        ..canonical
    };

    // Public APIs should never be able to create this packet; this is receiver
    // hardening for malformed or late wire updates. Subscription teardown races
    // in-flight traffic by design, so unknown per-subscription packets are
    // benign drops, not protocol corruption.
    reader
        .apply_sync_message_settled(SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
            subscription: unknown_usage_site,
            settled_through: GlobalTime(0),
            reset_result_set: false,
            version_carriers: Vec::new(),
            peer_payload_inventory: crate::protocol::PeerPayloadInventory::default(),
            result_member_adds: Vec::new(),
            result_member_removes: Vec::new(),
            program_fact_adds: Vec::new(),
            program_fact_removes: Vec::new(),
        }))
        .unwrap();

    assert_eq!(
        reader.sync_metrics().dropped_detached_subscription_messages,
        1
    );
    assert!(reader.query.settled_result_sets.is_empty());
    assert!(reader.query.settled_program_facts.is_empty());
}

#[test]
fn m3_seeded_sync_interleavings_converge_against_oracle() {
    // JAZZ_SEED_COUNT widens the sweep for soak runs (default: the 7
    // fixed seeds for CI speed); extra seeds are derived deterministically.
    let seeds = if let Ok(seed) = std::env::var("JAZZ_SEED") {
        vec![seed.parse::<u64>().expect("JAZZ_SEED must be a u64")]
    } else {
        const FIXED_SEEDS: [u64; 9] = [11, 29, 47, 83, 32676, 40595, 2234158, 3715011, 4372288];
        let extra = std::env::var("JAZZ_SEED_COUNT")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0)
            .saturating_sub(FIXED_SEEDS.len() as u64);
        FIXED_SEEDS
            .into_iter()
            .chain((0..extra).map(|i| 1_000 + i * 7919))
            .collect()
    };
    for seed in seeds {
        if let Err(payload) =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| run_m3_seed(seed)))
        {
            eprintln!("M3 SEED FAILED: {seed}");
            std::panic::resume_unwind(payload);
        }
    }
}
#[test]
fn m3_seeded_run_is_deterministic_for_fixed_seed() {
    let left = run_m3_seed(32676);
    let right = run_m3_seed(32676);
    assert_eq!(left.writer_a, right.writer_a);
    assert_eq!(left.writer_b, right.writer_b);
    assert_eq!(left.core, right.core);
    assert_eq!(left.reader_a, right.reader_a);
    assert_eq!(left.reader_b, right.reader_b);
    assert_eq!(left.link_a_metrics, right.link_a_metrics);
    assert_eq!(left.link_b_metrics, right.link_b_metrics);
    assert_eq!(left.message_counts, right.message_counts);
}
#[test]
fn undelivered_local_commits_are_lost_with_destroyed_client_storage() {
    let schema = schema();
    let (client_dir, mut client) = open_node_with_schema(node(1), schema.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema.clone());
    let (_reader_dir, mut reader) = open_node_with_schema(node(3), schema.clone());
    let mut peer = PeerState::new();

    let (lost_a, _lost_a_unit) = client
        .commit_mergeable_unit_settled(
            MergeableCommit::new("todos", row(1), 10).cells(title_cells("lost-a")),
        )
        .unwrap();
    let (lost_b, _lost_b_unit) = client
        .commit_mergeable_unit_settled(
            MergeableCommit::new("todos", row(2), 11).cells(title_cells("lost-b")),
        )
        .unwrap();

    for lost in [lost_a, lost_b] {
        assert_eq!(
            client.transaction_state_settled(lost),
            Some((Fate::Pending, None, DurabilityTier::Local))
        );
        assert!(core.transaction_state_settled(lost).is_none());
        assert!(reader.transaction_state_settled(lost).is_none());
    }
    assert!(
        client
            .current_rows("todos", DurabilityTier::Global)
            .unwrap()
            .is_empty()
    );

    let empty_update = peer.current_rows_update(&mut core, "todos").unwrap();
    reader.apply_sync_message_settled(empty_update).unwrap();
    assert!(
        reader
            .subscription_current_rows("todos", DurabilityTier::Global)
            .unwrap()
            .is_empty()
    );

    // README durability contract: Local is only local storage durability.
    // If commit units never reach an upstream tier and that storage is
    // destroyed, v0 has no remaining copy to recover or sync.
    drop(client);
    drop(client_dir);

    let (_replacement_dir, mut replacement) = open_node_with_schema(node(2), schema);
    let (kept, kept_unit) = replacement
        .commit_mergeable_unit_settled(MergeableCommit::new("todos", row(3), 12).cells(title_cells("kept")))
        .unwrap();
    let fates = core.apply_sync_message_settled(kept_unit).unwrap();
    assert_eq!(fates.len(), 1);
    replacement
        .apply_sync_message_settled(fates.into_iter().next().unwrap())
        .unwrap();
    let update = peer.current_rows_update(&mut core, "todos").unwrap();
    reader.apply_sync_message_settled(update).unwrap();

    for lost in [lost_a, lost_b] {
        assert!(core.transaction_state_settled(lost).is_none());
        assert!(reader.transaction_state_settled(lost).is_none());
    }
    assert_eq!(
        replacement.transaction_state_settled(kept),
        Some((Fate::Accepted, Some(GlobalTime::new(12, 0).unwrap()), DurabilityTier::Global))
    );
    assert_eq!(
        core.current_rows("todos", DurabilityTier::Global).unwrap(),
        vec![(row(3), title_cells("kept"))]
    );
    assert_eq!(
        reader
            .subscription_current_rows("todos", DurabilityTier::Global)
            .unwrap(),
        vec![(row(3), title_cells("kept"))]
    );
}
#[test]
fn accepted_fates_maintain_global_current_tables() {
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    let (_writer_dir, mut writer) = open_node_with_uuid(node(1));
    let row = row(7);
    let (first, first_message) = writer
        .commit_mergeable_unit_settled(MergeableCommit::new("todos", row, 10).cells(title_cells("first")))
        .unwrap();
    let (second, second_message) = writer
        .commit_mergeable_unit_settled(
            MergeableCommit::new("todos", row, 11)
                .parents(vec![first])
                .cells(title_cells("second")),
        )
        .unwrap();

    core.apply_sync_message_settled(first_message).unwrap();
    assert_eq!(
        global_winner_tx(&mut core, "todos", row, VersionLayer::Content),
        Some(first)
    );

    core.apply_sync_message_settled(second_message).unwrap();
    assert_eq!(
        global_winner_tx(&mut core, "todos", row, VersionLayer::Content),
        Some(second)
    );
}
#[test]
fn reopened_core_continues_sync_after_restart() {
    let schema = schema();
    let core_dir = tempfile::tempdir().unwrap();
    let (_writer_dir, mut writer) = open_node_with_uuid(node(1));
    let (_reader_dir, mut reader) = open_node_with_uuid(node(3));
    let mut peer = PeerState::new();
    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();

    let first_unit = writer
        .commit_mergeable_unit_settled(
            MergeableCommit::new("todos", row(1), 10).cells(title_cells("before")),
        )
        .unwrap()
        .1;
    {
        let storage = RocksDbStorage::open(core_dir.path(), &refs).unwrap();
        let mut core = NodeState::new(node(9), schema.clone(), storage).unwrap();
        core.apply_sync_message_settled(first_unit).unwrap();
    }

    let storage = RocksDbStorage::open(core_dir.path(), &refs).unwrap();
    let mut reopened_core = NodeState::new(node(9), schema, storage).unwrap();
    let second_unit = writer
        .commit_mergeable_unit_settled(
            MergeableCommit::new("todos", row(2), 11).cells(title_cells("after")),
        )
        .unwrap()
        .1;
    reopened_core.apply_sync_message_settled(second_unit).unwrap();
    let update = peer
        .current_rows_update(&mut reopened_core, "todos")
        .unwrap();
    reader.apply_sync_message_settled(update).unwrap();

    assert_eq!(
        reader
            .subscription_current_rows("todos", DurabilityTier::Local)
            .unwrap()
            .into_iter()
            .map(current_row_pair)
            .collect::<BTreeMap<_, _>>(),
        BTreeMap::from([
            (row(1), title_cells("before")),
            (row(2), title_cells("after")),
        ])
    );
}
#[test]
fn originating_causality_rejection_retains_child_payload() {
    let (_writer_dir, mut writer) = open_node_with_uuid(node(1));
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    let row = row(7);
    let parent = TxId::new(TxTime::from(200), node(2));
    core.ingest_commit_unit_settled(
        Transaction {
            tx_id: parent,
            kind: TxKind::Mergeable,
            n_total_writes: 1,
            made_by: AuthorSubject::SYSTEM,
            permission_subject: None,
            base_snapshot: None,
            row_read_set: None,
            absent_read_set: None,
            predicate_read_set: None,
            user_metadata_json: None,
            contribution_merge: None,
        },
        vec![version_record(row, Vec::new(), title_cells("parent"), None)],
        u64::MAX - SKEW_TOLERANCE_MS,
    )
    .unwrap();
    let child = writer
        .commit_mergeable_at_settled(
            MergeableCommit::new("todos", row, 101)
                .parents(vec![parent])
                .cells(title_cells("child")),
            TxTime::from(101),
        )
        .unwrap();
    let unit = writer.commit_unit_for(child).unwrap();
    let SyncMessage::CommitUnit { tx, versions } = unit else {
        panic!("expected commit unit");
    };
    assert!(tx.tx_id.time < parent.time);
    let [fate] = core
        .ingest_commit_unit_settled(tx, versions, u64::MAX - SKEW_TOLERANCE_MS)
        .unwrap()
        .try_into()
        .unwrap();
    assert_eq!(
        fate,
        SyncMessage::FateUpdate {
            tx_id: child,
            fate: Fate::Rejected(RejectionReason::CausalityViolation),
            global_time: None,
            durability: None,
        }
    );
    assert!(core.rejected_transaction(child).is_none());
    writer.apply_sync_message_settled(fate).unwrap();
    let stored = writer.rejected_transaction(child).unwrap();
    assert_eq!(stored.reason(), RejectionReason::CausalityViolation);
    assert_eq!(stored.versions().len(), 1);
    assert_eq!(stored.versions()[0].parents(), vec![parent]);
    assert_eq!(
        stored.versions()[0].test_cells(&schema().tables[0]),
        title_cells("child")
    );
    assert!(
        writer
            .row_history("todos", row)
            .unwrap()
            .iter()
            .all(|entry| entry.tx_id() != child)
    );
}
#[test]
fn originating_cascade_rejection_retains_root_cause() {
    let (_writer_dir, mut writer) = open_node_with_uuid(node(1));
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    let row = row(7);
    let (root, root_unit) = writer
        .commit_mergeable_unit_settled(
            MergeableCommit::new("todos", row, SKEW_TOLERANCE_MS + 1).cells(title_cells("root")),
        )
        .unwrap();
    let SyncMessage::CommitUnit { tx, versions } = root_unit else {
        panic!("expected commit unit");
    };
    let [root_fate] = core
        .ingest_commit_unit_settled(tx, versions, 0)
        .unwrap()
        .try_into()
        .unwrap();
    writer.apply_sync_message_settled(root_fate).unwrap();

    let (child, child_unit) = writer
        .commit_mergeable_unit_settled(
            MergeableCommit::new("todos", row, SKEW_TOLERANCE_MS + 2)
                .parents(vec![root])
                .cells(title_cells("child")),
        )
        .unwrap();
    let [child_fate] = core
        .apply_sync_message_settled(child_unit)
        .unwrap()
        .try_into()
        .unwrap();
    assert_eq!(
        child_fate,
        SyncMessage::FateUpdate {
            tx_id: child,
            fate: Fate::Rejected(RejectionReason::Cascade { root }),
            global_time: None,
            durability: None,
        }
    );
    writer.apply_sync_message_settled(child_fate).unwrap();
    let stored = writer.rejected_transaction(child).unwrap();
    assert_eq!(stored.reason(), RejectionReason::Cascade { root });
    assert_eq!(stored.cascade_root(), Some(root));
    assert_eq!(stored.versions()[0].parents(), vec![root]);
    assert!(core.rejected_transactions().is_empty());
}
#[test]
fn commit_units_sync_upstream_and_fates_flow_back() {
    let (_client_dir, mut client) = open_node_with_uuid(node(1));
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    let row = row(7);

    let (tx_id, message) = client
        .commit_mergeable_unit_settled(MergeableCommit::new("todos", row, 10).cells(title_cells("sync me")))
        .unwrap();

    assert_eq!(
        client.transaction_state_settled(tx_id).unwrap(),
        (Fate::Pending, None, DurabilityTier::Local)
    );

    let SyncMessage::CommitUnit { tx, versions } = message else {
        panic!("commit_mergeable_unit must emit a commit unit");
    };
    let [fate] = core
        .ingest_commit_unit_settled(tx.clone(), versions.clone(), u64::MAX - SKEW_TOLERANCE_MS)
        .unwrap()
        .try_into()
        .unwrap();
    let duplicate_fate = core
        .ingest_commit_unit_settled(tx, versions, u64::MAX - SKEW_TOLERANCE_MS)
        .unwrap();
    assert_eq!(duplicate_fate, vec![fate.clone()]);
    let SyncMessage::FateUpdate {
        tx_id: fate_tx,
        fate: accepted,
        global_time,
        durability,
    } = fate
    else {
        panic!("core must return a fate update");
    };
    assert_eq!(fate_tx, tx_id);
    assert_eq!(accepted, Fate::Accepted);
    assert_eq!(global_time, Some(GlobalTime::new(10, 0).unwrap()));
    assert_eq!(durability, Some(DurabilityTier::Global));

    assert_eq!(
        core.current_rows("todos", DurabilityTier::Local)
            .unwrap()
            .into_iter()
            .map(current_row_pair)
            .collect::<BTreeMap<_, _>>(),
        BTreeMap::from([(row, title_cells("sync me"))])
    );

    client
        .apply_fate_update(fate_tx, accepted, global_time, durability)
        .unwrap();
    assert_eq!(
        client.transaction_state_settled(tx_id).unwrap(),
        (Fate::Accepted, Some(GlobalTime::new(10, 0).unwrap()), DurabilityTier::Global)
    );
}
#[test]
fn duplicate_commit_units_must_match_original_payload() {
    let (_client_dir, mut client) = open_node_with_uuid(node(1));
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    let row = row(7);
    let (_, message) = client
        .commit_mergeable_unit_settled(MergeableCommit::new("todos", row, 10).cells(title_cells("first")))
        .unwrap();
    let SyncMessage::CommitUnit { tx, versions } = message else {
        panic!("commit_mergeable_unit must emit a commit unit");
    };
    core.ingest_commit_unit_settled(tx.clone(), versions.clone(), u64::MAX - SKEW_TOLERANCE_MS)
        .unwrap();

    let mut conflicting = versions;
    conflicting[0] = version_record(row, Vec::new(), title_cells("changed"), None);

    assert!(matches!(
        core.ingest_commit_unit_settled(tx, conflicting, u64::MAX - SKEW_TOLERANCE_MS),
        Err(Error::ConflictingCommitUnit(_))
    ));
}

#[test]
fn fate_update_rejects_backward_global_time_and_keeps_durability_monotone() {
    let (_temp_dir, mut node) = open_node();
    let tx_id = node
        .commit_mergeable_settled(MergeableCommit::new("todos", row(7), 10).cells(title_cells("base")))
        .unwrap();
    node.apply_fate_update(
        tx_id,
        Fate::Accepted,
        Some(GlobalTime(5)),
        Some(DurabilityTier::Global),
    )
    .unwrap();

    assert!(matches!(
        node.apply_fate_update(
            tx_id,
            Fate::Accepted,
            Some(GlobalTime(4)),
            Some(DurabilityTier::Global),
        ).resolve(),
        Err(Error::NonMonotoneState("global seq cannot move backwards"))
    ));
    assert_eq!(
        node.transaction_state_settled(tx_id).unwrap(),
        (Fate::Accepted, Some(GlobalTime(5)), DurabilityTier::Global)
    );

    node.apply_fate_update(
        tx_id,
        Fate::Accepted,
        Some(GlobalTime(6)),
        Some(DurabilityTier::Global),
    )
    .unwrap();
    assert_eq!(
        node.transaction_state_settled(tx_id).unwrap(),
        (Fate::Accepted, Some(GlobalTime(6)), DurabilityTier::Global)
    );
}

#[test]
fn peer_rejects_sequenced_non_global_fate_without_crashing_the_node() {
    let (_temp_dir, mut node) = open_node();
    let tx_id = node
        .commit_mergeable_settled(MergeableCommit::new("todos", row(8), 10).cells(title_cells("base")))
        .unwrap();

    let received = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        node.apply_sync_message_settled(SyncMessage::FateUpdate {
            tx_id,
            fate: Fate::Accepted,
            global_time: Some(GlobalTime(7)),
            durability: Some(DurabilityTier::Edge),
        })
    }));
    assert!(received.is_ok(), "a peer message must not panic the receiver");
    assert!(matches!(
        received.unwrap(),
        Err(Error::UnsupportedSyncMessage(
            "global timestamp requires Global durability"
        ))
    ));
    assert_eq!(
        node.transaction_state_settled(tx_id),
        Some((Fate::Pending, None, DurabilityTier::Local))
    );

    node.apply_sync_message_settled(SyncMessage::FateUpdate {
        tx_id,
        fate: Fate::Accepted,
        global_time: Some(GlobalTime(7)),
        durability: Some(DurabilityTier::Global),
    })
    .unwrap();
    assert_eq!(
        node.transaction_state_settled(tx_id),
        Some((Fate::Accepted, Some(GlobalTime(7)), DurabilityTier::Global))
    );
}

#[test]
fn peer_rejects_sequenced_non_global_view_bundle_before_persisting_it() {
    let (_temp_dir, mut receiver) = open_node();
    let bad_tx = TxId::new(TxTime::from(10), node(8));
    let subscription = receiver.whole_table_subscription_key("todos").unwrap();
    receiver
        .apply_sync_message_settled(SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
            subscription,
            settled_through: GlobalTime(0),
            reset_result_set: true,
            version_carriers: Vec::new(),
            peer_payload_inventory: crate::protocol::PeerPayloadInventory {
                opening_pending: true,
                ..Default::default()
            },
            result_member_adds: Vec::new(),
            result_member_removes: Vec::new(),
            program_fact_adds: Vec::new(),
            program_fact_removes: Vec::new(),
        }))
        .unwrap();
    let authority_result_key = receiver
        .authority_result_key_for_subscription(subscription)
        .unwrap();
    let before_generation = receiver.applied_authority_result_generation(&authority_result_key);
    assert!(receiver.opening_pending_for_authority_result(&authority_result_key));
    let received = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        receiver.apply_sync_message_settled(SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
            subscription,
            settled_through: GlobalTime(0),
            reset_result_set: true,
            version_carriers: vec![VersionCarrier::Bundle(VersionBundle {
                scope: crate::protocol::VersionBundleScope::CompleteTransaction,
                tx: Transaction {
                    tx_id: bad_tx,
                    kind: TxKind::Mergeable,
                    n_total_writes: 0,
                    made_by: AuthorSubject::SYSTEM,
                    permission_subject: None,
                    base_snapshot: None,
                    row_read_set: None,
                    absent_read_set: None,
                    predicate_read_set: None,
                    user_metadata_json: None,
                    contribution_merge: None,
                },
                versions: Vec::new(),
                fate: Fate::Accepted,
                global_time: Some(GlobalTime(7)),
                durability: DurabilityTier::Edge,
            })],
            peer_payload_inventory: crate::protocol::PeerPayloadInventory {
                opening_pending: false,
                ..Default::default()
            },
            result_member_adds: Vec::new(),
            result_member_removes: Vec::new(),
            program_fact_adds: Vec::new(),
            program_fact_removes: Vec::new(),
        }))
    }));
    assert!(received.is_ok(), "a peer view must not panic the receiver");
    assert!(matches!(
        received.unwrap(),
        Err(Error::MalformedViewUpdate(
            "global timestamp requires Global durability"
        ))
    ));
    assert!(receiver.transaction_state_settled(bad_tx).is_none());
    assert!(receiver.opening_pending_for_authority_result(&authority_result_key));
    assert_eq!(
        receiver.applied_authority_result_generation(&authority_result_key),
        before_generation
    );
    receiver
        .apply_sync_message_settled(SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
            subscription,
            settled_through: GlobalTime(1),
            reset_result_set: true,
            version_carriers: Vec::new(),
            peer_payload_inventory: crate::protocol::PeerPayloadInventory::default(),
            result_member_adds: Vec::new(),
            result_member_removes: Vec::new(),
            program_fact_adds: Vec::new(),
            program_fact_removes: Vec::new(),
        }))
        .unwrap();
    assert!(!receiver.opening_pending_for_authority_result(&authority_result_key));
    assert_eq!(
        receiver.applied_authority_result_generation(&authority_result_key),
        before_generation + 1
    );
    assert!(receiver
        .commit_mergeable_settled(MergeableCommit::new("todos", row(9), 11).cells(title_cells("alive")))
        .is_ok());
}

// This is necessarily an internal mechanism test: the public sync boundary
// returns a protocol error instead. The assertion protects locally constructed
// state without turning malformed peer input into a remote panic vector.
#[cfg(debug_assertions)]
#[test]
fn internal_sequenced_non_global_fate_trips_the_debug_assertion() {
    let (_temp_dir, mut node) = open_node();
    let tx_id = node
        .commit_mergeable_settled(MergeableCommit::new("todos", row(10), 10).cells(title_cells("base")))
        .unwrap();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        node.apply_fate_update(
            tx_id,
            Fate::Accepted,
            Some(GlobalTime(7)),
            Some(DurabilityTier::Edge),
        )
        .resolve()
    }));
    assert!(result.is_err());
}
