// Commit causality, parking, malformed units, and rejection cascades.

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
    assert!(
        core.current_rows("todos", DurabilityTier::Local)
            .unwrap()
            .is_empty()
    );
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
    let tx_id = OpenTransactionId::new();
    client.open_exclusive(tx_id).unwrap();
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
    let tx_id = OpenTransactionId::new();
    client.open_exclusive(tx_id).unwrap();
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
    assert!(
        core.ingest_commit_unit(tx, versions, u64::MAX - SKEW_TOLERANCE_MS)
            .unwrap()
            .is_empty()
    );

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
    assert!(
        core.ingest_commit_unit(tx.clone(), versions.clone(), u64::MAX - SKEW_TOLERANCE_MS)
            .unwrap()
            .is_empty()
    );
    assert!(
        core.ingest_commit_unit(tx, versions, u64::MAX - SKEW_TOLERANCE_MS)
            .unwrap()
            .is_empty()
    );
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
    let [good_fate] = core
        .apply_sync_message(good_unit)
        .unwrap()
        .try_into()
        .unwrap();
    assert!(matches!(
        good_fate,
        SyncMessage::FateUpdate {
            tx_id,
            fate: Fate::Accepted,
            ..
        } if tx_id == good_tx
    ));
}
