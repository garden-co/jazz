#[test]
fn observed_global_seq_advances_authority_allocator() {
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    let (_writer_dir, mut writer) = open_node_with_uuid(node(1));
    let fixture_tx = core
        .commit_mergeable(MergeableCommit::new("todos", row(1), 10).cells(title_cells("fixture")))
        .unwrap();
    core.apply_fate_update(
        fixture_tx,
        Fate::Accepted,
        Some(GlobalSeq(1)),
        Some(DurabilityTier::Global),
    )
    .unwrap();

    let (tx_id, unit) = writer
        .commit_mergeable_unit(MergeableCommit::new("todos", row(2), 11).cells(title_cells("new")))
        .unwrap();
    let [fate] = core.apply_sync_message(unit).unwrap().try_into().unwrap();
    assert_eq!(
        fate,
        SyncMessage::FateUpdate {
            tx_id,
            fate: Fate::Accepted,
            global_seq: Some(GlobalSeq(2)),
            durability: Some(DurabilityTier::Global),
        }
    );
    assert_eq!(core.clock.applied_global_watermark, GlobalSeq(2));
    assert_eq!(core.clock.next_global_seq, GlobalSeq(3));
}
#[test]
fn authority_rejects_later_child_of_rejected_parent_with_cascade() {
    let (_client_dir, mut client) = open_node_with_uuid(node(1));
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    let row = row(7);
    let (root, root_unit) = client
        .commit_mergeable_unit(
            MergeableCommit::new("todos", row, SKEW_TOLERANCE_MS + 1).cells(title_cells("root")),
        )
        .unwrap();
    let SyncMessage::CommitUnit { tx, versions } = root_unit else {
        panic!("expected commit unit");
    };
    let [root_fate] = core
        .ingest_commit_unit(tx, versions, 0)
        .unwrap()
        .try_into()
        .unwrap();
    assert!(matches!(
        root_fate,
        SyncMessage::FateUpdate {
            fate: Fate::Rejected(RejectionReason::ClientClockTooFarAhead),
            ..
        }
    ));

    let (child, child_unit) = client
        .commit_mergeable_unit(
            MergeableCommit::new("todos", row, 10)
                .parents(vec![root])
                .cells(title_cells("child")),
        )
        .unwrap();
    let SyncMessage::CommitUnit { tx, versions } = child_unit else {
        panic!("expected commit unit");
    };
    let [child_fate] = core
        .ingest_commit_unit(tx, versions, u64::MAX - SKEW_TOLERANCE_MS)
        .unwrap()
        .try_into()
        .unwrap();
    assert_eq!(
        child_fate,
        SyncMessage::FateUpdate {
            tx_id: child,
            fate: Fate::Rejected(RejectionReason::Cascade { root }),
            global_seq: None,
            durability: None,
        }
    );
    assert_eq!(
        core.transaction_state(child).unwrap().0,
        Fate::Rejected(RejectionReason::Cascade { root })
    );
    assert!(core
        .current_rows("todos", DurabilityTier::Local)
        .unwrap()
        .is_empty());
}
#[test]
fn client_side_rejection_cascades_to_local_mergeable_descendant() {
    let (_client_dir, mut client) = open_node_with_uuid(node(1));
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    let row = row(7);
    commit_mergeable_global(
        &mut client,
        &mut core,
        MergeableCommit::new("todos", row, 1).cells(title_cells("old")),
    );
    let tx_id = client.open_exclusive().unwrap();
    client
        .tx_write(tx_id, "todos", row, title_cells("exclusive"), None)
        .unwrap();
    let (exclusive, exclusive_unit) = client
        .commit_exclusive(tx_id, AuthorId::SYSTEM, SKEW_TOLERANCE_MS + 1)
        .unwrap();
    let (dependent, dependent_unit) = client
        .commit_mergeable_unit(
            MergeableCommit::new("todos", row, 2)
                .parents(vec![exclusive])
                .cells(BTreeMap::from([(
                    "title".to_owned(),
                    "dependent".to_owned(),
                )])),
        )
        .unwrap();
    let SyncMessage::CommitUnit { tx, versions } = exclusive_unit else {
        panic!("expected commit unit");
    };
    let [exclusive_fate] = core
        .ingest_commit_unit(tx, versions, 0)
        .unwrap()
        .try_into()
        .unwrap();
    client.apply_sync_message(exclusive_fate).unwrap();
    assert_eq!(
        client.transaction_state(exclusive).unwrap().0,
        Fate::Rejected(RejectionReason::ClientClockTooFarAhead)
    );
    assert_eq!(
        client.transaction_state(dependent).unwrap().0,
        Fate::Rejected(RejectionReason::Cascade { root: exclusive })
    );
    assert_eq!(
        client
            .current_rows("todos", DurabilityTier::Local)
            .unwrap()
            .into_iter()
            .map(current_row_pair)
            .collect::<BTreeMap<_, _>>(),
        BTreeMap::from([(row, title_cells("old"))])
    );

    let SyncMessage::CommitUnit { tx, versions } = dependent_unit else {
        panic!("expected commit unit");
    };
    let [dependent_fate] = core
        .ingest_commit_unit(tx, versions, u64::MAX - SKEW_TOLERANCE_MS)
        .unwrap()
        .try_into()
        .unwrap();
    assert_eq!(
        dependent_fate,
        SyncMessage::FateUpdate {
            tx_id: dependent,
            fate: Fate::Rejected(RejectionReason::Cascade { root: exclusive }),
            global_seq: None,
            durability: None,
        }
    );
    client.apply_sync_message(dependent_fate).unwrap();
    assert_eq!(
        client.transaction_state(dependent).unwrap().0,
        Fate::Rejected(RejectionReason::Cascade { root: exclusive })
    );
}
#[test]
fn authority_unparks_child_after_unknown_parent_accepts() {
    let (_client_dir, mut client) = open_node_with_uuid(node(1));
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    let row = row(7);
    let tx_id = client.open_exclusive().unwrap();
    client
        .tx_write(tx_id, "todos", row, title_cells("exclusive"), None)
        .unwrap();
    let (exclusive, exclusive_unit) = client.commit_exclusive(tx_id, AuthorId::SYSTEM, 1).unwrap();
    let (child, child_unit) = client
        .commit_mergeable_unit(
            MergeableCommit::new("todos", row, 2)
                .parents(vec![exclusive])
                .cells(title_cells("child")),
        )
        .unwrap();
    let SyncMessage::CommitUnit { tx, versions } = child_unit else {
        panic!("expected commit unit");
    };
    assert!(core
        .ingest_commit_unit(tx, versions, u64::MAX - SKEW_TOLERANCE_MS)
        .unwrap()
        .is_empty());

    let SyncMessage::CommitUnit { tx, versions } = exclusive_unit else {
        panic!("expected commit unit");
    };
    let updates = core
        .ingest_commit_unit(tx, versions, u64::MAX - SKEW_TOLERANCE_MS)
        .unwrap();
    assert_eq!(core.sync_metrics().parked_orphans_resolved, 1);
    assert_eq!(
        updates,
        vec![
            SyncMessage::FateUpdate {
                tx_id: exclusive,
                fate: Fate::Accepted,
                global_seq: Some(GlobalSeq(1)),
                durability: Some(DurabilityTier::Global),
            },
            SyncMessage::FateUpdate {
                tx_id: child,
                fate: Fate::Accepted,
                global_seq: Some(GlobalSeq(2)),
                durability: Some(DurabilityTier::Global),
            },
        ]
    );
    assert_eq!(
        core.current_rows("todos", DurabilityTier::Global)
            .unwrap()
            .into_iter()
            .map(current_row_pair)
            .collect::<BTreeMap<_, _>>(),
        BTreeMap::from([(row, title_cells("child"))])
    );
}
#[test]
fn duplicate_unknown_parent_commit_unit_parks_once() {
    let (_client_dir, mut client) = open_node_with_uuid(node(1));
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    let missing = TxId::new(TxTime::from(99), node(1));
    let (_child, child_unit) = client
        .commit_mergeable_unit(
            MergeableCommit::new("todos", row(7), 2)
                .parents(vec![missing])
                .cells(title_cells("child")),
        )
        .unwrap();
    let SyncMessage::CommitUnit { tx, versions } = child_unit else {
        panic!("expected commit unit");
    };
    assert!(core
        .ingest_commit_unit(tx.clone(), versions.clone(), u64::MAX - SKEW_TOLERANCE_MS)
        .unwrap()
        .is_empty());
    assert!(core
        .ingest_commit_unit(tx, versions, u64::MAX - SKEW_TOLERANCE_MS)
        .unwrap()
        .is_empty());
    assert_eq!(core.sync_metrics().parked_orphans, 1);
    assert_eq!(core.sync_metrics().parked_orphans_resolved, 0);
}
#[test]
fn m2_writer_core_reader_converges_against_oracle() {
    let (_writer_dir, mut writer) = open_node_with_uuid(node(1));
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    let (_reader_dir, mut reader) = open_node_with_uuid(node(3));
    let mut peer = PeerState::new();
    let mut oracle = Oracle::new();
    let row_a = row(1);
    let row_b = row(2);

    for commit in [
        MergeableCommit::new("todos", row_a, 10).cells(title_cells("a1")),
        MergeableCommit::new("todos", row_b, 11).cells(title_cells("b1")),
        MergeableCommit::new("todos", row_a, 12).deletion(DeletionEvent::Deleted),
        MergeableCommit::new("todos", row_a, 13).cells(title_cells("a2")),
    ] {
        let row_uuid = commit.row_uuid;
        let parents = commit.parents.clone();
        let cells = commit.cells.clone();
        let deletion = commit.deletion;
        let (tx_id, commit_unit) = writer.commit_mergeable_unit(commit).unwrap();
        let made_at = writer.transaction_record(tx_id).unwrap().tx_id.time;
        let mut version = ModelRowVersion::new(row_uuid, tx_id, made_at);
        version.parents = parents;
        version.cells = cells;
        version.deletion = deletion;
        oracle.add_version(version);

        for message in core.apply_sync_message(commit_unit).unwrap() {
            writer.apply_sync_message(message).unwrap();
        }
        assert_current_rows_match_oracle(&mut core, &oracle);
        assert_view_update_result_set_matches_current_rows(&mut core);

        let update = peer.current_rows_update(&mut core, "todos").unwrap();
        reader.apply_sync_message(update).unwrap();
        assert_current_rows_match_oracle(&mut reader, &oracle);
    }
}
#[test]
fn malformed_commit_unit_rejects_write_count_mismatch() {
    let (_writer_dir, mut writer) = open_node_with_uuid(node(1));
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    let (_tx_id, unit) = writer
        .commit_mergeable_unit(MergeableCommit::new("todos", row(1), 10).cells(title_cells("one")))
        .unwrap();
    let SyncMessage::CommitUnit { mut tx, versions } = unit else {
        panic!("expected commit unit");
    };
    tx.n_total_writes = 2;

    let [fate] = core
        .ingest_commit_unit(tx.clone(), versions, u64::MAX - SKEW_TOLERANCE_MS)
        .unwrap()
        .try_into()
        .unwrap();
    assert_eq!(
        fate,
        SyncMessage::FateUpdate {
            tx_id: tx.tx_id,
            fate: Fate::Rejected(RejectionReason::MalformedCommit(
                "commit unit version count does not match transaction n_total_writes".to_owned()
            )),
            global_seq: None,
            durability: None,
        }
    );
    assert!(core.row_history("todos", row(1)).unwrap().is_empty());
}

#[test]
fn over_limit_commit_unit_rejects_as_malformed_and_next_unit_still_applies() {
    let (_writer_dir, mut writer) = open_node_with_uuid(node(1));
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    let (_tx_id, unit) = writer
        .commit_mergeable_unit(
            MergeableCommit::new("todos", row(1), 10).cells(title_cells("oversized")),
        )
        .unwrap();
    let SyncMessage::CommitUnit {
        mut tx,
        mut versions,
    } = unit
    else {
        panic!("expected commit unit");
    };
    versions = vec![versions[0].clone(); crate::protocol_limits::MAX_COMMIT_UNIT_VERSIONS + 1];
    tx.n_total_writes = versions.len() as u32;

    let [fate] = core
        .apply_sync_message(SyncMessage::CommitUnit {
            tx: tx.clone(),
            versions,
        })
        .unwrap()
        .try_into()
        .unwrap();
    match fate {
        SyncMessage::FateUpdate {
            tx_id,
            fate: Fate::Rejected(RejectionReason::MalformedCommit(reason)),
            global_seq: None,
            durability: None,
        } => {
            assert_eq!(tx_id, tx.tx_id);
            assert!(
                reason.contains("exceeds max"),
                "unexpected malformed reason: {reason}"
            );
        }
        other => panic!("expected malformed fate update, got {other:?}"),
    }
    assert!(core.row_history("todos", row(1)).unwrap().is_empty());

    let (good_tx, good_unit) = writer
        .commit_mergeable_unit(MergeableCommit::new("todos", row(2), 11).cells(title_cells("ok")))
        .unwrap();
    let [good_fate] = core.apply_sync_message(good_unit).unwrap().try_into().unwrap();
    assert!(matches!(
        good_fate,
        SyncMessage::FateUpdate {
            tx_id,
            fate: Fate::Accepted,
            ..
        } if tx_id == good_tx
    ));
}
#[test]
fn cold_reset_bulk_ingest_matches_incremental_ingest() {
    let (_writer_dir, mut writer) = open_node_with_uuid(node(1));
    let (_core_dir, mut core) = open_node_with_uuid(node(2));
    let (_bulk_dir, mut bulk_reader) = open_node_with_uuid(node(3));
    let (_incremental_dir, mut incremental_reader) = open_node_with_uuid(node(4));

    commit_mergeable_global(
        &mut writer,
        &mut core,
        MergeableCommit::new("todos", row(1), 10).cells(title_cells("one")),
    );
    commit_mergeable_global(
        &mut writer,
        &mut core,
        MergeableCommit::new("todos", row(2), 11).cells(title_cells("two")),
    );
    commit_mergeable_global(
        &mut writer,
        &mut core,
        MergeableCommit::new("todos", row(1), 12).cells(title_cells("one newer")),
    );

    let mut peer = PeerState::new();
    let update = peer.rehydrate_current_rows(&mut core, "todos").unwrap();
    let mut incremental_update = update.clone();
    let SyncMessage::ViewUpdate {
        reset_result_set, ..
    } = &mut incremental_update
    else {
        panic!("expected view update");
    };
    *reset_result_set = false;

    bulk_reader.apply_sync_message(update).unwrap();
    incremental_reader
        .apply_sync_message(incremental_update)
        .unwrap();

    assert_eq!(
        bulk_reader
            .current_rows("todos", DurabilityTier::Global)
            .unwrap(),
        incremental_reader
            .current_rows("todos", DurabilityTier::Global)
            .unwrap()
    );
    assert_eq!(
        bulk_reader
            .current_rows("todos", DurabilityTier::Local)
            .unwrap(),
        incremental_reader
            .current_rows("todos", DurabilityTier::Local)
            .unwrap()
    );
    assert_eq!(
        bulk_reader.query_all_versions().unwrap(),
        incremental_reader.query_all_versions().unwrap()
    );
    assert_currency_tables_match_storage(&mut bulk_reader, "todos");
    assert_currency_tables_match_storage(&mut incremental_reader, "todos");
}
#[test]
fn receiver_tracks_partial_mergeable_payload_coverage() {
    let (_reader_dir, mut reader) = open_node_with_uuid(node(3));
    let subscription = reader.whole_table_subscription_key("todos").unwrap();
    let tx_id = TxId::new(TxTime::from(10), node(1));
    let tx = Transaction {
        tx_id,
        kind: TxKind::Mergeable,
        n_total_writes: 2,
        made_by: AuthorId::SYSTEM,
        permission_subject: None,
        base_snapshot: None,
        row_read_set: None,
        absent_read_set: None,
        predicate_read_set: None,
        user_metadata_json: None,
            source_branch: None,
            merge_strategy: None,
    };
    let first = version_record(row(1), Vec::new(), title_cells("one"), None);
    let second = version_record(row(2), Vec::new(), title_cells("two"), None);

    reader
        .apply_sync_message(SyncMessage::ViewUpdate {
            subscription,
            settled_through: GlobalSeq(0),
            reset_result_set: false,
            version_bundles: vec![VersionBundle {
                tx: tx.clone(),
                versions: vec![first],
                fate: Fate::Accepted,
                global_seq: Some(GlobalSeq(1)),
                durability: DurabilityTier::Global,
            }],
            peer_payload_inventory: crate::protocol::PeerPayloadInventory::default(),
            result_member_adds: vec![("todos".to_owned().into(), row(1), tx_id).into()],
            result_member_removes: Vec::new(),
                program_fact_adds: Vec::new(),
                program_fact_removes: Vec::new(),
        })
        .unwrap();
    assert_eq!(
        reader.current_rows("todos", DurabilityTier::Local).unwrap(),
        vec![(row(1), title_cells("one"))]
    );
    assert_eq!(
        reader
            .subscription_current_rows("todos", DurabilityTier::Local)
            .unwrap()
            .into_iter()
            .map(current_row_pair)
            .collect::<BTreeMap<_, _>>(),
        BTreeMap::from([(row(1), title_cells("one"))])
    );

    reader
        .apply_sync_message(SyncMessage::ViewUpdate {
            subscription,
            settled_through: GlobalSeq(0),
            reset_result_set: false,
            version_bundles: vec![VersionBundle {
                tx,
                versions: vec![second],
                fate: Fate::Accepted,
                global_seq: Some(GlobalSeq(1)),
                durability: DurabilityTier::Global,
            }],
            peer_payload_inventory: crate::protocol::PeerPayloadInventory::default(),
            result_member_adds: vec![("todos".to_owned().into(), row(2), tx_id).into()],
            result_member_removes: Vec::new(),
                program_fact_adds: Vec::new(),
                program_fact_removes: Vec::new(),
        })
        .unwrap();
    assert_eq!(
        reader.current_rows("todos", DurabilityTier::Local).unwrap(),
        vec![(row(1), title_cells("one")), (row(2), title_cells("two")),]
    );
}

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
        .apply_sync_message(SyncMessage::ViewUpdate {
        subscription: unknown_usage_site,
        settled_through: GlobalSeq(0),
        reset_result_set: false,
        version_bundles: Vec::new(),
        peer_payload_inventory: crate::protocol::PeerPayloadInventory::default(),
        result_member_adds: Vec::new(),
        result_member_removes: Vec::new(),
        program_fact_adds: Vec::new(),
        program_fact_removes: Vec::new(),
    })
    .unwrap();

    assert_eq!(reader.sync_metrics().dropped_detached_subscription_messages, 1);
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
        const FIXED_SEEDS: [u64; 8] = [11, 29, 47, 83, 32676, 40595, 2234158, 3715011];
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
        .commit_mergeable_unit(
            MergeableCommit::new("todos", row(1), 10).cells(title_cells("lost-a")),
        )
        .unwrap();
    let (lost_b, _lost_b_unit) = client
        .commit_mergeable_unit(
            MergeableCommit::new("todos", row(2), 11).cells(title_cells("lost-b")),
        )
        .unwrap();

    for lost in [lost_a, lost_b] {
        assert_eq!(
            client.transaction_state(lost),
            Some((Fate::Pending, None, DurabilityTier::Local))
        );
        assert!(core.transaction_state(lost).is_none());
        assert!(reader.transaction_state(lost).is_none());
    }
    assert!(client
        .current_rows("todos", DurabilityTier::Global)
        .unwrap()
        .is_empty());

    let empty_update = peer.current_rows_update(&mut core, "todos").unwrap();
    reader.apply_sync_message(empty_update).unwrap();
    assert!(reader
        .subscription_current_rows("todos", DurabilityTier::Global)
        .unwrap()
        .is_empty());

    // README durability contract: Local is only local storage durability.
    // If commit units never reach an upstream tier and that storage is
    // destroyed, v0 has no remaining copy to recover or sync.
    drop(client);
    drop(client_dir);

    let (_replacement_dir, mut replacement) = open_node_with_schema(node(2), schema);
    let (kept, kept_unit) = replacement
        .commit_mergeable_unit(MergeableCommit::new("todos", row(3), 12).cells(title_cells("kept")))
        .unwrap();
    let fates = core.apply_sync_message(kept_unit).unwrap();
    assert_eq!(fates.len(), 1);
    replacement
        .apply_sync_message(fates.into_iter().next().unwrap())
        .unwrap();
    let update = peer.current_rows_update(&mut core, "todos").unwrap();
    reader.apply_sync_message(update).unwrap();

    for lost in [lost_a, lost_b] {
        assert!(core.transaction_state(lost).is_none());
        assert!(reader.transaction_state(lost).is_none());
    }
    assert_eq!(
        replacement.transaction_state(kept),
        Some((Fate::Accepted, Some(GlobalSeq(1)), DurabilityTier::Global))
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
        .commit_mergeable_unit(MergeableCommit::new("todos", row, 10).cells(title_cells("first")))
        .unwrap();
    let (second, second_message) = writer
        .commit_mergeable_unit(
            MergeableCommit::new("todos", row, 11)
                .parents(vec![first])
                .cells(title_cells("second")),
        )
        .unwrap();

    core.apply_sync_message(first_message).unwrap();
    assert_eq!(
        global_winner_tx(&mut core, "todos", row, VersionLayer::Content),
        Some(first)
    );

    core.apply_sync_message(second_message).unwrap();
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
        .commit_mergeable_unit(
            MergeableCommit::new("todos", row(1), 10).cells(title_cells("before")),
        )
        .unwrap()
        .1;
    {
        let storage = RocksDbStorage::open(core_dir.path(), &refs).unwrap();
        let mut core = NodeState::new(node(9), schema.clone(), storage).unwrap();
        core.apply_sync_message(first_unit).unwrap();
    }

    let storage = RocksDbStorage::open(core_dir.path(), &refs).unwrap();
    let mut reopened_core = NodeState::new(node(9), schema, storage).unwrap();
    let second_unit = writer
        .commit_mergeable_unit(
            MergeableCommit::new("todos", row(2), 11).cells(title_cells("after")),
        )
        .unwrap()
        .1;
    reopened_core.apply_sync_message(second_unit).unwrap();
    let update = peer
        .current_rows_update(&mut reopened_core, "todos")
        .unwrap();
    reader.apply_sync_message(update).unwrap();

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
    core.ingest_commit_unit(
        Transaction {
            tx_id: parent,
            kind: TxKind::Mergeable,
            n_total_writes: 1,
            made_by: AuthorId::SYSTEM,
            permission_subject: None,
            base_snapshot: None,
            row_read_set: None,
            absent_read_set: None,
            predicate_read_set: None,
            user_metadata_json: None,
            source_branch: None,
            merge_strategy: None,
        },
        vec![version_record(row, Vec::new(), title_cells("parent"), None)],
        u64::MAX - SKEW_TOLERANCE_MS,
    )
    .unwrap();
    let child = writer
        .commit_mergeable_at(
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
        .ingest_commit_unit(tx, versions, u64::MAX - SKEW_TOLERANCE_MS)
        .unwrap()
        .try_into()
        .unwrap();
    assert_eq!(
        fate,
        SyncMessage::FateUpdate {
            tx_id: child,
            fate: Fate::Rejected(RejectionReason::CausalityViolation),
            global_seq: None,
            durability: None,
        }
    );
    assert!(core.rejected_transaction(child).is_none());
    writer.apply_sync_message(fate).unwrap();
    let stored = writer.rejected_transaction(child).unwrap();
    assert_eq!(stored.reason(), RejectionReason::CausalityViolation);
    assert_eq!(stored.versions().len(), 1);
    assert_eq!(stored.versions()[0].parents(), vec![parent]);
    assert_eq!(
        stored.versions()[0].test_cells(&schema().tables[0]),
        title_cells("child")
    );
    assert!(writer
        .row_history("todos", row)
        .unwrap()
        .iter()
        .all(|entry| entry.tx_id() != child));
}
#[test]
fn originating_cascade_rejection_retains_root_cause() {
    let (_writer_dir, mut writer) = open_node_with_uuid(node(1));
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    let row = row(7);
    let (root, root_unit) = writer
        .commit_mergeable_unit(
            MergeableCommit::new("todos", row, SKEW_TOLERANCE_MS + 1).cells(title_cells("root")),
        )
        .unwrap();
    let SyncMessage::CommitUnit { tx, versions } = root_unit else {
        panic!("expected commit unit");
    };
    let [root_fate] = core
        .ingest_commit_unit(tx, versions, 0)
        .unwrap()
        .try_into()
        .unwrap();
    writer.apply_sync_message(root_fate).unwrap();

    let (child, child_unit) = writer
        .commit_mergeable_unit(
            MergeableCommit::new("todos", row, SKEW_TOLERANCE_MS + 2)
                .parents(vec![root])
                .cells(title_cells("child")),
        )
        .unwrap();
    let [child_fate] = core
        .apply_sync_message(child_unit)
        .unwrap()
        .try_into()
        .unwrap();
    assert_eq!(
        child_fate,
        SyncMessage::FateUpdate {
            tx_id: child,
            fate: Fate::Rejected(RejectionReason::Cascade { root }),
            global_seq: None,
            durability: None,
        }
    );
    writer.apply_sync_message(child_fate).unwrap();
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
        .commit_mergeable_unit(MergeableCommit::new("todos", row, 10).cells(title_cells("sync me")))
        .unwrap();

    assert_eq!(
        client.transaction_state(tx_id).unwrap(),
        (Fate::Pending, None, DurabilityTier::Local)
    );

    let SyncMessage::CommitUnit { tx, versions } = message else {
        panic!("commit_mergeable_unit must emit a commit unit");
    };
    let [fate] = core
        .ingest_commit_unit(tx.clone(), versions.clone(), u64::MAX - SKEW_TOLERANCE_MS)
        .unwrap()
        .try_into()
        .unwrap();
    let duplicate_fate = core
        .ingest_commit_unit(tx, versions, u64::MAX - SKEW_TOLERANCE_MS)
        .unwrap();
    assert_eq!(duplicate_fate, vec![fate.clone()]);
    let SyncMessage::FateUpdate {
        tx_id: fate_tx,
        fate: accepted,
        global_seq,
        durability,
    } = fate
    else {
        panic!("core must return a fate update");
    };
    assert_eq!(fate_tx, tx_id);
    assert_eq!(accepted, Fate::Accepted);
    assert_eq!(global_seq, Some(GlobalSeq(1)));
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
        .apply_fate_update(fate_tx, accepted, global_seq, durability)
        .unwrap();
    assert_eq!(
        client.transaction_state(tx_id).unwrap(),
        (Fate::Accepted, Some(GlobalSeq(1)), DurabilityTier::Global)
    );
}
#[test]
fn duplicate_commit_units_must_match_original_payload() {
    let (_client_dir, mut client) = open_node_with_uuid(node(1));
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    let row = row(7);
    let (_, message) = client
        .commit_mergeable_unit(MergeableCommit::new("todos", row, 10).cells(title_cells("first")))
        .unwrap();
    let SyncMessage::CommitUnit { tx, versions } = message else {
        panic!("commit_mergeable_unit must emit a commit unit");
    };
    core.ingest_commit_unit(tx.clone(), versions.clone(), u64::MAX - SKEW_TOLERANCE_MS)
        .unwrap();

    let mut conflicting = versions;
    conflicting[0] = version_record(row, Vec::new(), title_cells("changed"), None);

    assert!(matches!(
        core.ingest_commit_unit(tx, conflicting, u64::MAX - SKEW_TOLERANCE_MS),
        Err(Error::ConflictingCommitUnit(_))
    ));
}

#[test]
fn fate_update_rejects_backward_global_seq_and_keeps_durability_monotone() {
    let (_temp_dir, mut node) = open_node();
    let tx_id = node
        .commit_mergeable(MergeableCommit::new("todos", row(7), 10).cells(title_cells("base")))
        .unwrap();
    node.apply_fate_update(
        tx_id,
        Fate::Accepted,
        Some(GlobalSeq(5)),
        Some(DurabilityTier::Global),
    )
    .unwrap();

    assert!(matches!(
        node.apply_fate_update(
            tx_id,
            Fate::Accepted,
            Some(GlobalSeq(4)),
            Some(DurabilityTier::Local),
        ),
        Err(Error::NonMonotoneState("global seq cannot move backwards"))
    ));
    assert_eq!(
        node.transaction_state(tx_id).unwrap(),
        (Fate::Accepted, Some(GlobalSeq(5)), DurabilityTier::Global)
    );

    node.apply_fate_update(tx_id, Fate::Accepted, Some(GlobalSeq(6)), Some(DurabilityTier::Local))
        .unwrap();
    assert_eq!(
        node.transaction_state(tx_id).unwrap(),
        (Fate::Accepted, Some(GlobalSeq(6)), DurabilityTier::Global)
    );
}

#[test]
fn content_extent_fetch_rejects_row_context_mismatch_and_invisible_content() {
    let (_temp_dir, mut node) = open_node();
    let author = user(0xa1);
    let visible_row = row(7);
    let other_row = row(8);
    let extent = node
        .content_store()
        .append(author, visible_row, "title", b"unreferenced")
        .unwrap();
    let mut peer = PeerState::for_author(author);

    assert!(matches!(
        peer.handle_content_extent_fetch(
            &mut node,
            SyncMessage::FetchContentExtent {
                owner: crate::protocol::LargeValueOwnerRef::current_row(other_row),
                extent: extent.clone(),
            },
        ),
        Err(Error::UnsupportedSyncMessage(
            "content extent row context mismatch"
        ))
    ));
    assert!(matches!(
        peer.handle_content_extent_fetch(
            &mut node,
            SyncMessage::FetchContentExtent {
                owner: crate::protocol::LargeValueOwnerRef::current_row(visible_row),
                extent,
            },
        ),
        Err(Error::UnsupportedSyncMessage(
            "content extent is not visible for row"
        ))
    ));
}

#[test]
fn row_version_fetch_returns_authorized_versions_and_omits_unauthorized_rows() {
    let schema = owner_policy_schema();
    let (_writer_dir, mut writer) = open_node_with_schema(node(1), schema.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    let alice = user(0xa1);
    let bob = user(0xb2);
    let alice_row = row(7);
    let bob_row = row(8);

    let alice_tx = commit_mergeable_global(
        &mut writer,
        &mut core,
        MergeableCommit::new("todos", alice_row, 10)
            .made_by(alice)
            .cells(owner_cells(alice, "alice")),
    );
    let bob_tx = commit_mergeable_global(
        &mut writer,
        &mut core,
        MergeableCommit::new("todos", bob_row, 11)
            .made_by(bob)
            .cells(owner_cells(bob, "bob")),
    );
    let requests = vec![
        crate::protocol::RowVersionRef::new("todos", alice_row, alice_tx),
        crate::protocol::RowVersionRef::new("todos", bob_row, bob_tx),
    ];

    let mut alice_peer = PeerState::for_author(alice);
    let messages = alice_peer
        .handle_row_versions_fetch(
            &mut core,
            SyncMessage::FetchRowVersions {
                requests: requests.clone(),
            },
        )
        .unwrap();
    let [SyncMessage::RowVersionPayloads { version_bundles }] = messages.as_slice() else {
        panic!("expected one row-version payload response");
    };
    assert_eq!(version_bundles.len(), 1);
    assert_eq!(version_bundles[0].versions.len(), 1);
    assert_eq!(version_bundles[0].versions[0].row_uuid(), alice_row);

    let mut too_many = Vec::new();
    for _ in 0..=crate::protocol_limits::MAX_FETCH_ROW_VERSIONS {
        too_many.push(crate::protocol::RowVersionRef::new("todos", alice_row, alice_tx));
    }
    assert!(matches!(
        alice_peer.handle_row_versions_fetch(
            &mut core,
            SyncMessage::FetchRowVersions { requests: too_many },
        ),
        Err(Error::UnsupportedSyncMessage(
            "row-version repair request exceeds limit"
        ))
    ));
}

#[test]
fn declared_known_state_view_update_repairs_withheld_row_version_body() {
    let (_writer_dir, mut writer) = open_node_with_uuid(node(1));
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    let (_reader_dir, mut reader) = open_node_with_uuid(node(3));
    let row_uuid = row(7);
    let (shape, binding) = reader.whole_table_shape_binding("todos").unwrap();
    register_shape_binding(&mut reader, &shape, &binding);

    let (tx_id, commit_unit) = writer
        .commit_mergeable_unit(
            MergeableCommit::new("todos", row_uuid, 10).cells(title_cells("repair me")),
        )
        .unwrap();
    let SyncMessage::CommitUnit { tx, versions } = commit_unit else {
        panic!("expected commit unit");
    };
    core.ingest_commit_unit(tx.clone(), versions, u64::MAX - SKEW_TOLERANCE_MS)
        .unwrap();
    reader
        .ingest_known_transaction(
            tx,
            Vec::new(),
            Fate::Accepted,
            Some(GlobalSeq(1)),
            DurabilityTier::Global,
        )
        .unwrap();

    let mut update = core.view_update_for_current_rows("todos").unwrap();
    let SyncMessage::ViewUpdate {
        version_bundles,
        result_member_adds,
        ..
    } = &mut update
    else {
        panic!("expected view update");
    };
    version_bundles.clear();
    assert_eq!(
        result_member_adds,
        &vec![("todos".to_owned().into(), row_uuid, tx_id)]
    );

    let missing = reader
        .missing_known_state_row_version_refs(&update)
        .unwrap();
    assert_eq!(
        missing,
        vec![crate::protocol::RowVersionRef::new("todos", row_uuid, tx_id)]
    );
    let mut peer = PeerState::relay();
    let messages = peer
        .handle_row_versions_fetch(
            &mut core,
            SyncMessage::FetchRowVersions {
                requests: missing.clone(),
            },
        )
        .unwrap();
    let [SyncMessage::RowVersionPayloads { version_bundles }] = messages.as_slice()
    else {
        panic!("expected row-version payloads");
    };
    reader
        .apply_row_version_payloads_for_requests(&missing, version_bundles.clone())
        .unwrap();
    assert!(reader
        .missing_known_state_row_version_refs(&update)
        .unwrap()
        .is_empty());
    reader.apply_sync_message(update).unwrap();
    assert_eq!(
        reader
            .current_rows("todos", DurabilityTier::Local)
            .unwrap()
            .into_iter()
            .map(current_row_pair)
            .collect::<BTreeMap<_, _>>(),
        BTreeMap::from([(row_uuid, title_cells("repair me"))])
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
        .commit_mergeable_unit(
            MergeableCommit::new("todos", row_uuid, 10).cells(title_cells("visible")),
        )
        .unwrap();
    let SyncMessage::CommitUnit { tx, versions } = visible_unit else {
        panic!("expected commit unit");
    };
    core.ingest_commit_unit(tx, versions, u64::MAX - SKEW_TOLERANCE_MS)
        .unwrap();
    reader
        .apply_sync_message(core.view_update_for_current_rows("todos").unwrap())
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
        .apply_sync_message(SyncMessage::Subscribe(crate::protocol::Subscribe {
            shape_id: shape.shape_id(),
            subscription: usage_subscription,
            values: Vec::new(),
            known_state: None,
        }))
        .unwrap();
    assert_eq!(
        reader
            .binding_view_key_for_subscription(usage_subscription)
            .unwrap(),
        binding_view_key
    );
    reader.apply_unsubscribe(usage_subscription);
    let late = SyncMessage::ViewUpdate {
        subscription: usage_subscription,
        settled_through: GlobalSeq(2),
        reset_result_set: false,
        version_bundles: Vec::new(),
        peer_payload_inventory: crate::protocol::PeerPayloadInventory::default(),
        result_member_adds: Vec::new(),
        result_member_removes: vec![crate::protocol::ResultMemberEntry::row((
            groove::Intern::from("todos".to_owned()),
            row_uuid,
            TxId::new(TxTime(777), node(44)),
        ))],
        program_fact_adds: Vec::new(),
        program_fact_removes: Vec::new(),
    };
    reader.apply_sync_message(late).unwrap();

    assert_eq!(reader.sync_metrics().dropped_detached_subscription_messages, 1);
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
    let late = SyncMessage::ViewUpdate {
        subscription,
        settled_through: GlobalSeq(1),
        reset_result_set: true,
        version_bundles: Vec::new(),
        peer_payload_inventory: crate::protocol::PeerPayloadInventory::default(),
        result_member_adds: Vec::new(),
        result_member_removes: Vec::new(),
        program_fact_adds: Vec::new(),
        program_fact_removes: Vec::new(),
    };

    reader.apply_sync_message(late).unwrap();

    assert_eq!(reader.sync_metrics().dropped_detached_subscription_messages, 1);
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
        .commit_mergeable_unit(
            MergeableCommit::new("todos", row_uuid, 10).cells(title_cells("visible")),
        )
        .unwrap();
    let SyncMessage::CommitUnit { tx, versions } = visible_unit else {
        panic!("expected commit unit");
    };
    core.ingest_commit_unit(tx, versions, u64::MAX - SKEW_TOLERANCE_MS)
        .unwrap();
    let initial = core.view_update_for_current_rows("todos").unwrap();
    reader.apply_sync_message(initial).unwrap();
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
    let removal = SyncMessage::ViewUpdate {
        subscription,
        settled_through: GlobalSeq(2),
        reset_result_set: false,
        version_bundles: Vec::new(),
        peer_payload_inventory: crate::protocol::PeerPayloadInventory::default(),
        result_member_adds: Vec::new(),
        result_member_removes: vec![crate::protocol::ResultMemberEntry::row((
            groove::Intern::from("todos".to_owned()),
            row_uuid,
            invisible_tx,
        ))],
        program_fact_adds: Vec::new(),
        program_fact_removes: Vec::new(),
    };
    assert!(
        reader
            .missing_known_state_row_version_refs(&removal)
            .unwrap()
            .is_empty(),
        "removals must not request repair bodies because the removed version may be policy-invisible"
    );
    reader.apply_sync_message(removal).unwrap();
    assert!(
        reader
            .subscription_current_rows("todos", DurabilityTier::Global)
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        reader.settled_through_for_binding_view(binding_view_key),
        Some(GlobalSeq(2))
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

    let removal = SyncMessage::ViewUpdate {
        subscription,
        settled_through: GlobalSeq(3),
        reset_result_set: false,
        version_bundles: Vec::new(),
        peer_payload_inventory: crate::protocol::PeerPayloadInventory::default(),
        result_member_adds: Vec::new(),
        result_member_removes: vec![crate::protocol::ResultMemberEntry::row((
            groove::Intern::from("todos".to_owned()),
            row_uuid,
            TxId::new(TxTime(1000), node(45)),
        ))],
        program_fact_adds: Vec::new(),
        program_fact_removes: Vec::new(),
    };

    assert!(
        reader
            .missing_known_state_row_version_refs(&removal)
            .unwrap()
            .is_empty()
    );
    reader.apply_sync_message(removal).unwrap();
    assert!(
        reader
            .subscription_current_rows("todos", DurabilityTier::Global)
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        reader.settled_through_for_binding_view(binding_view_key),
        Some(GlobalSeq(3))
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
        .commit_mergeable_unit(
            MergeableCommit::new("todos", row_uuid, 10).cells(title_cells("shared")),
        )
        .unwrap();
    let SyncMessage::CommitUnit { tx, versions } = visible_unit else {
        panic!("expected commit unit");
    };
    core.ingest_commit_unit(tx, versions, u64::MAX - SKEW_TOLERANCE_MS)
        .unwrap();
    reader
        .apply_sync_message(core.view_update_for_current_rows("todos").unwrap())
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
        .apply_sync_message(SyncMessage::Subscribe(crate::protocol::Subscribe {
            shape_id: shape.shape_id(),
            subscription: duplicate_subscription,
            values: Vec::new(),
            known_state: None,
        }))
        .unwrap();
    assert_eq!(
        reader
            .binding_view_key_for_subscription(duplicate_subscription)
            .unwrap(),
        binding_view_key
    );

    reader
        .apply_sync_message(SyncMessage::ViewUpdate {
            subscription: duplicate_subscription,
            settled_through: GlobalSeq(2),
            reset_result_set: true,
            version_bundles: Vec::new(),
            peer_payload_inventory: crate::protocol::PeerPayloadInventory::default(),
            result_member_adds: Vec::new(),
            result_member_removes: Vec::new(),
            program_fact_adds: Vec::new(),
            program_fact_removes: Vec::new(),
        })
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
        Some(GlobalSeq(2))
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

    let (tx_id, commit_unit) = writer
        .commit_mergeable_unit(
            MergeableCommit::new("todos", row_uuid, 10).cells(title_cells("known")),
        )
        .unwrap();
    let SyncMessage::CommitUnit { tx, versions } = commit_unit else {
        panic!("expected commit unit");
    };
    core.ingest_commit_unit(tx.clone(), versions, u64::MAX - SKEW_TOLERANCE_MS)
        .unwrap();
    reader
        .ingest_known_transaction(
            tx,
            Vec::new(),
            Fate::Accepted,
            Some(GlobalSeq(1)),
            DurabilityTier::Global,
        )
        .unwrap();
    let mut control_peer = PeerState::relay();
    let control_update = control_peer
        .rehydrate_query_for_subscription_with_opts(
            &mut core,
            subscription,
            &shape,
            &binding,
            RegisterShapeOptions::default(),
        )
        .unwrap();
    let SyncMessage::ViewUpdate {
        version_bundles: control_version_bundles,
        result_member_adds: control_result_member_adds,
        ..
    } = &control_update
    else {
        panic!("expected control view update");
    };
    assert_eq!(control_result_member_adds.len(), 1);
    assert_eq!(control_version_bundles.len(), 1);

    let mut peer = PeerState::relay();
    peer.declare_known_state(
        subscription,
        Some(crate::protocol::KnownStateDeclaration::Fast {
            completeness: crate::protocol::KnownStateCompleteness::FastCurrentMembership,
            position: GlobalSeq(1),
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
        .unwrap();
    let SyncMessage::ViewUpdate {
        settled_through,
        version_bundles,
        result_member_adds,
        ..
    } = &update
    else {
        panic!("expected view update");
    };
    assert_eq!(*settled_through, GlobalSeq(1));
    assert_eq!(result_member_adds, control_result_member_adds);
    assert!(version_bundles.is_empty());
    assert_eq!(
        result_member_adds,
        &vec![crate::protocol::ResultMemberEntry::from(
            crate::protocol::RealRowMemberEntry::current_content((
                groove::Intern::from("todos".to_owned()),
                row_uuid,
                tx_id,
            ))
            .with_settle_position(Some(GlobalSeq(1)))
        )]
    );

    let missing = reader
        .missing_known_state_row_version_refs(&update)
        .unwrap();
    assert_eq!(
        missing,
        vec![crate::protocol::RowVersionRef::new("todos", row_uuid, tx_id)]
    );
    let messages = peer
        .handle_row_versions_fetch(
            &mut core,
            SyncMessage::FetchRowVersions {
                requests: missing.clone(),
            },
        )
        .unwrap();
    let [SyncMessage::RowVersionPayloads { version_bundles }] = messages.as_slice()
    else {
        panic!("expected row-version payloads");
    };
    reader
        .apply_row_version_payloads_for_requests(&missing, version_bundles.clone())
        .unwrap();
    reader.apply_sync_message(update).unwrap();
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
    let values = Vec::new();

    let (tx_a, unit_a) = writer
        .commit_mergeable_unit(
            MergeableCommit::new("todos", row_a, 10).cells(title_cells("local")),
        )
        .unwrap();
    let SyncMessage::CommitUnit {
        tx: tx_a_record,
        versions: versions_a,
    } = unit_a
    else {
        panic!("expected commit unit");
    };
    core.ingest_commit_unit(
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
            Some(GlobalSeq(1)),
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
        .commit_mergeable_unit(
            MergeableCommit::new("todos", row_b, 11).cells(title_cells("remote")),
        )
        .unwrap();
    core.apply_sync_message(unit_b).unwrap();

    let declaration = reader
        .known_state_declaration_for_subscription(
            &shape,
            &binding,
            subscription,
            &values,
            AuthorId::SYSTEM,
        )
        .unwrap()
        .expect("reader should derive exact slow known-state");
    assert_eq!(
        declaration,
        crate::protocol::KnownStateDeclaration::ExactVersionSet {
            versions: vec![crate::protocol::RowVersionRef::new("todos", row_a, tx_a)]
        }
    );

    let mut control_peer = PeerState::relay();
    let control_update = control_peer
        .rehydrate_query_for_subscription_with_opts(
            &mut core,
            subscription,
            &shape,
            &binding,
            RegisterShapeOptions::default(),
        )
        .unwrap();
    let SyncMessage::ViewUpdate {
        version_bundles: control_bundles,
        result_member_adds: control_members,
        ..
    } = &control_update
    else {
        panic!("expected control update");
    };
    assert_eq!(control_members.len(), 2);
    assert_eq!(control_bundles.len(), 2);

    let mut peer = PeerState::relay();
    peer.declare_known_state(subscription, Some(declaration));
    let update = peer
        .rehydrate_query_for_subscription_with_opts(
            &mut core,
            subscription,
            &shape,
            &binding,
            RegisterShapeOptions::default(),
        )
        .unwrap();
    let SyncMessage::ViewUpdate {
        version_bundles,
        result_member_adds,
        ..
    } = &update
    else {
        panic!("expected declared update");
    };
    assert_eq!(result_member_adds, control_members);
    assert_eq!(version_bundles.len(), 1);
    assert_eq!(version_bundles[0].tx.tx_id, tx_b);
    assert!(reader
        .missing_known_state_row_version_refs(&update)
        .unwrap()
        .is_empty());
    reader.apply_sync_message(update).unwrap();
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
    let mut peer = PeerState::relay();
    let update = peer
        .rehydrate_query_for_subscription_with_opts(
            &mut core,
            subscription,
            &shape,
            &binding,
            RegisterShapeOptions::default(),
        )
        .unwrap();
    let SyncMessage::ViewUpdate {
        version_bundles,
        result_member_adds,
        ..
    } = update
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
    let mut peer = PeerState::relay();
    let update = peer
        .rehydrate_query_for_subscription_with_opts(
            &mut core,
            subscription,
            &shape,
            &binding,
            RegisterShapeOptions::default(),
        )
        .unwrap();
    reader.apply_sync_message(update).unwrap();

    let mut reopened = reader.reopen_in_place().unwrap();
    let declaration = reopened
        .known_state_declaration_for_subscription(
            &shape,
            &binding,
            subscription,
            &[],
            AuthorId::SYSTEM,
        )
        .unwrap();
    assert_eq!(
        declaration,
        Some(crate::protocol::KnownStateDeclaration::Fast {
            completeness: crate::protocol::KnownStateCompleteness::FastCurrentMembership,
            position: GlobalSeq(1),
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
            AuthorId::SYSTEM,
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
    let mut peer = PeerState::relay();
    let update = peer
        .rehydrate_query_for_subscription_with_opts(
            &mut core,
            subscription,
            &shape,
            &binding,
            RegisterShapeOptions::default(),
        )
        .unwrap();
    reader.apply_sync_message(update).unwrap();
    drop(reader);

    let mut reopened = open_node_at(&reader_dir, schema());
    let declaration = reopened
        .known_state_declaration_for_subscription(
            &shape,
            &binding,
            subscription,
            &[],
            AuthorId::SYSTEM,
        )
        .unwrap();
    assert_eq!(
        declaration,
        Some(crate::protocol::KnownStateDeclaration::Fast {
            completeness: crate::protocol::KnownStateCompleteness::FastCurrentMembership,
            position: GlobalSeq(1),
        })
    );
}

#[test]
fn known_state_declaration_never_skips_unfated_edge_members() {
    let (_writer_dir, mut writer) = open_node_with_uuid(node(1));
    let (_edge_dir, mut edge) = open_node_with_uuid(node(7));
    let row_uuid = row(18);
    let (shape, binding) = edge.whole_table_shape_binding("todos").unwrap();
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
        .commit_mergeable_unit(
            MergeableCommit::new("todos", row_uuid, 10).cells(title_cells("unfated")),
        )
        .unwrap();
    let SyncMessage::CommitUnit { tx, versions } = unit else {
        panic!("expected commit unit");
    };
    edge.ingest_known_transaction(tx, versions, Fate::Accepted, None, DurabilityTier::Edge)
        .unwrap();
    let mut peer = PeerState::relay();
    peer.declare_known_state(
        subscription,
        Some(crate::protocol::KnownStateDeclaration::Fast {
            completeness: crate::protocol::KnownStateCompleteness::FastCurrentMembership,
            position: GlobalSeq(100),
        }),
    );

    let update = peer
        .rehydrate_query_for_subscription_with_opts(&mut edge, subscription, &shape, &binding, opts)
        .unwrap();
    let SyncMessage::ViewUpdate {
        version_bundles,
        result_member_adds,
        ..
    } = update
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

#[test]
fn view_updates_ship_current_versions_to_downstream_nodes() {
    let (_writer_dir, mut writer) = open_node_with_uuid(node(1));
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    let (_reader_dir, mut reader) = open_node_with_uuid(node(3));
    let row = row(7);

    let (_, commit_unit) = writer
        .commit_mergeable_unit(
            MergeableCommit::new("todos", row, 10).cells(BTreeMap::from([(
                "title".to_owned(),
                "replicate me".to_owned(),
            )])),
        )
        .unwrap();
    let SyncMessage::CommitUnit { tx, versions } = commit_unit else {
        panic!("expected commit unit");
    };
    core.ingest_commit_unit(tx, versions, u64::MAX - SKEW_TOLERANCE_MS)
        .unwrap();

    let update = core.view_update_for_current_rows("todos").unwrap();
    let SyncMessage::ViewUpdate {
        subscription,
        settled_through,
        reset_result_set,
        version_bundles,
        peer_payload_inventory:
            crate::protocol::PeerPayloadInventory {
                complete_tx_payloads: peer_payload_inventory_refs,
        },
        result_member_adds,
        result_member_removes,
        ..
    } = update
    else {
        panic!("expected view update");
    };
    assert_eq!(
        subscription,
        core.whole_table_subscription_key("todos").unwrap()
    );
    assert!(!reset_result_set);
    assert_eq!(result_member_adds.len(), 1);
    assert!(result_member_removes.is_empty());
    assert_eq!(version_bundles.len(), 1);
    assert!(peer_payload_inventory_refs.is_empty());

    reader
        .apply_view_update(ViewUpdateParts {
            subscription,
            settled_through,
            reset_result_set: false,
            version_bundles,
            peer_complete_tx_payload_refs: peer_payload_inventory_refs,
            result_member_adds,
            result_member_removes,
            program_fact_adds: Vec::new(),
            program_fact_removes: Vec::new(),
        })
        .unwrap();

    assert_eq!(
        reader
            .subscription_current_rows("todos", DurabilityTier::Local)
            .unwrap()
            .into_iter()
            .map(current_row_pair)
            .collect::<BTreeMap<_, _>>(),
        BTreeMap::from([(row, title_cells("replicate me"))])
    );
}
#[test]
fn view_updates_use_peer_payload_inventory_refs_for_previously_shipped_complete_payloads() {
    let (_writer_dir, mut writer) = open_node_with_uuid(node(1));
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    let (_reader_dir, mut reader) = open_node_with_uuid(node(3));
    let row = row(7);

    let (tx_id, commit_unit) = writer
        .commit_mergeable_unit(MergeableCommit::new("todos", row, 10).cells(title_cells("known")))
        .unwrap();
    let SyncMessage::CommitUnit { tx, versions } = commit_unit else {
        panic!("expected commit unit");
    };
    core.ingest_commit_unit(tx, versions, u64::MAX - SKEW_TOLERANCE_MS)
        .unwrap();

    let initial = core.view_update_for_current_rows("todos").unwrap();
    let SyncMessage::ViewUpdate {
        subscription,
        settled_through,
        reset_result_set,
        version_bundles,
        peer_payload_inventory:
            crate::protocol::PeerPayloadInventory {
                complete_tx_payloads: peer_payload_inventory_refs,
        },
        result_member_adds,
        result_member_removes,
        ..
    } = initial
    else {
        panic!("expected view update");
    };
    assert!(!reset_result_set);
    reader
        .apply_view_update(ViewUpdateParts {
            subscription,
            settled_through,
            reset_result_set: false,
            version_bundles,
            peer_complete_tx_payload_refs: peer_payload_inventory_refs,
            result_member_adds,
            result_member_removes,
            program_fact_adds: Vec::new(),
            program_fact_removes: Vec::new(),
        })
        .unwrap();

    let deduped = core
        .view_update_for_current_rows_with_peer_payload_inventory(
            "todos",
            core.whole_table_subscription_key("todos").unwrap(),
            [tx_id],
            [],
            [],
            AuthorId::SYSTEM,
        )
        .unwrap();
    let SyncMessage::ViewUpdate {
        settled_through,
        version_bundles,
        peer_payload_inventory:
            crate::protocol::PeerPayloadInventory {
                complete_tx_payloads: peer_payload_inventory_refs,
            },
        result_member_adds,
        result_member_removes,
        ..
    } = deduped
    else {
        panic!("expected view update");
    };
    assert!(version_bundles.is_empty());
    assert_eq!(peer_payload_inventory_refs, vec![tx_id]);
    assert_eq!(
        result_member_adds,
        vec![("todos".to_owned().into(), row, tx_id)]
    );
    assert!(result_member_removes.is_empty());
    reader
        .apply_view_update(ViewUpdateParts {
            subscription: core.whole_table_subscription_key("todos").unwrap(),
            settled_through,
            reset_result_set: false,
            version_bundles,
            peer_complete_tx_payload_refs: peer_payload_inventory_refs,
            result_member_adds,
            result_member_removes,
            program_fact_adds: Vec::new(),
            program_fact_removes: Vec::new(),
        })
        .unwrap();
}
#[test]
fn view_updates_reject_unknown_peer_payload_inventory_refs() {
    let (_reader_dir, mut reader) = open_node_with_uuid(node(3));
    let missing = TxId {
        node: node(1),
        time: TxTime::from(99),
    };

    let err = reader
        .apply_view_update(ViewUpdateParts {
            subscription: reader.whole_table_subscription_key("todos").unwrap(),
            settled_through: GlobalSeq(0),
            reset_result_set: false,
            version_bundles: Vec::new(),
            peer_complete_tx_payload_refs: vec![missing],
            result_member_adds: Vec::new(),
            result_member_removes: Vec::new(),
            program_fact_adds: Vec::new(),
            program_fact_removes: Vec::new(),
        })
        .unwrap_err();

    assert!(matches!(err, Error::MissingTransaction(tx_id) if tx_id == missing));
    assert_eq!(reader.sync_metrics().parked_orphans, 1);
}
#[test]
fn wire_record_round_trips_through_history_bytes() {
    let (_writer_dir, mut writer) = open_node_with_uuid(node(1));
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    let row = row(7);
    let (_tx_id, message) = writer
        .commit_mergeable_unit(MergeableCommit::new("todos", row, 10).cells(title_cells("wire")))
        .unwrap();
    let SyncMessage::CommitUnit { tx, versions } = message else {
        panic!("expected commit unit");
    };
    let original = versions[0].clone();
    core.ingest_commit_unit(tx, versions, u64::MAX - SKEW_TOLERANCE_MS)
        .unwrap();
    let stored = core.query_row_versions("todos", row).unwrap();
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
        .commit_mergeable_unit(
            MergeableCommit::new("todos", row, 10).cells(BTreeMap::from([(
                "title".to_owned(),
                "dispatch".to_owned(),
            )])),
        )
        .unwrap();

    let out = core.apply_sync_message(commit_unit).unwrap();
    let [fate_update] = out.as_slice() else {
        panic!("expected one fate update");
    };
    writer.apply_sync_message(fate_update.clone()).unwrap();
    let (fate, _, _) = writer.transaction_state(tx_id).unwrap();
    assert_eq!(fate, Fate::Accepted);

    let view_update = core.view_update_for_current_rows("todos").unwrap();
    assert!(reader.apply_sync_message(view_update).unwrap().is_empty());
    assert_eq!(
        reader
            .subscription_current_rows("todos", DurabilityTier::Local)
            .unwrap()
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
        made_by: AuthorId::SYSTEM,
        permission_subject: None,
        base_snapshot: None,
        row_read_set: None,
        absent_read_set: None,
        predicate_read_set: None,
        user_metadata_json: None,
            source_branch: None,
            merge_strategy: None,
    };
    let versions = vec![
        version_record(row(1), Vec::new(), title_cells("a"), None),
        version_record(row(2), Vec::new(), title_cells("b"), None),
    ];
    core.ingest_commit_unit(tx.clone(), versions.clone(), u64::MAX - SKEW_TOLERANCE_MS)
        .unwrap();
    let mut reversed = versions;
    reversed.reverse();

    assert!(core
        .ingest_commit_unit(tx, reversed, u64::MAX - SKEW_TOLERANCE_MS)
        .is_ok());
}
