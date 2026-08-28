// Commit causality, parking, malformed units, and rejection cascades.

#[test]
fn observed_global_time_advances_authority_allocator() {
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    let (_writer_dir, mut writer) = open_node_with_uuid(node(1));
    let fixture_tx = core
        .commit_mergeable_settled(MergeableCommit::new("todos", row(1), 10).cells(title_cells("fixture")))
        .unwrap();
    let fixture_global_time = core.allocate_global_time_for_test();
    core.apply_fate_update(
        fixture_tx,
        Fate::Accepted,
        Some(fixture_global_time),
        Some(DurabilityTier::Global),
    )
    .unwrap();

    let (tx_id, unit) = writer
        .commit_mergeable_unit_settled(MergeableCommit::new("todos", row(2), 11).cells(title_cells("new")))
        .unwrap();
    let [fate] = core.apply_sync_message_settled(unit).unwrap().try_into().unwrap();
    assert_eq!(
        fate,
        SyncMessage::FateUpdate {
            tx_id,
            fate: Fate::Accepted,
            global_time: Some(GlobalTime::new(11, 0).unwrap()),
            durability: Some(DurabilityTier::Global),
        }
    );
    assert_eq!(
        core.clock.committed_global_time,
        GlobalTime::new(11, 0).unwrap()
    );
    assert_eq!(core.clock.global_time_register, core.clock.committed_global_time);
}
#[test]
fn authority_rejects_later_child_of_rejected_parent_with_cascade() {
    let (_client_dir, mut client) = open_node_with_uuid(node(1));
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    let row = row(7);
    let (root, root_unit) = client
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
    assert!(matches!(
        root_fate,
        SyncMessage::FateUpdate {
            fate: Fate::Rejected(RejectionReason::ClientClockTooFarAhead),
            ..
        }
    ));

    let (child, child_unit) = client
        .commit_mergeable_unit_settled(
            MergeableCommit::new("todos", row, 10)
                .parents(vec![root])
                .cells(title_cells("child")),
        )
        .unwrap();
    let SyncMessage::CommitUnit { tx, versions } = child_unit else {
        panic!("expected commit unit");
    };
    let [child_fate] = core
        .ingest_commit_unit_settled(tx, versions, u64::MAX - SKEW_TOLERANCE_MS)
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
    assert_eq!(
        core.transaction_state_settled(child).unwrap().0,
        Fate::Rejected(RejectionReason::Cascade { root })
    );
    assert!(
        core.current_rows("todos", DurabilityTier::Local)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn rejected_update_does_not_silence_the_next_fresh_row_commit() {
    let (_client_dir, mut client) = open_node_with_uuid(node(1));
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    let target = row(0x71);

    let (_base, base_unit) = client
        .commit_mergeable_unit_settled(
            MergeableCommit::new("todos", target, 10).cells(title_cells("base")),
        )
        .unwrap();
    let [base_fate] = core
        .apply_sync_message_settled(base_unit)
        .unwrap()
        .try_into()
        .unwrap();
    client.apply_sync_message_settled(base_fate).unwrap();

    let (rejected, rejected_unit) = client
        .commit_mergeable_unit_settled(
            MergeableCommit::new("todos", target, SKEW_TOLERANCE_MS + 1)
                .cells(title_cells("rejected")),
        )
        .unwrap();
    let SyncMessage::CommitUnit { tx, versions } = rejected_unit else {
        panic!("expected rejected update commit unit");
    };
    let [rejected_fate] = core
        .ingest_commit_unit_settled(tx, versions, 0)
        .unwrap()
        .try_into()
        .unwrap();
    assert!(matches!(
        rejected_fate,
        SyncMessage::FateUpdate {
            fate: Fate::Rejected(RejectionReason::ClientClockTooFarAhead),
            ..
        }
    ));
    client.apply_sync_message_settled(rejected_fate).unwrap();
    assert_eq!(
        client.transaction_state_settled(rejected).unwrap().0,
        Fate::Rejected(RejectionReason::ClientClockTooFarAhead)
    );
    // The rejected speculative version must be removed from the visible row,
    // leaving the previously accepted base as the current value.
    assert_eq!(
        client
            .current_rows("todos", DurabilityTier::Local)
            .unwrap()
            .into_iter()
            .map(current_row_pair)
            .collect::<BTreeMap<_, _>>(),
        BTreeMap::from([(target, title_cells("base"))])
    );

    let (fresh, fresh_unit) = client
        .commit_mergeable_unit_settled(
            MergeableCommit::new("todos", target, 20).cells(title_cells("fresh")),
        )
        .unwrap();
    let SyncMessage::CommitUnit { tx, versions } = fresh_unit else {
        panic!("expected fresh update commit unit");
    };
    let [fresh_fate] = core
        .ingest_commit_unit_settled(tx, versions, u64::MAX - SKEW_TOLERANCE_MS)
        .unwrap()
        .try_into()
        .unwrap();
    let SyncMessage::FateUpdate {
        tx_id,
        fate,
        global_time,
        durability,
    } = fresh_fate
    else {
        panic!("expected fresh fate update");
    };
    assert_eq!(tx_id, fresh);
    assert_eq!(fate, Fate::Accepted);
    assert_eq!(durability, Some(DurabilityTier::Global));
    client
        .apply_fate_update(tx_id, fate, global_time, durability)
        .unwrap();
    assert_eq!(
        client.transaction_state_settled(fresh).unwrap().0,
        Fate::Accepted
    );
    assert_eq!(ahead_current_row_count(&mut client, "todos"), 0);
    assert_eq!(
        client
            .current_rows("todos", DurabilityTier::Global)
            .unwrap()
            .into_iter()
            .map(current_row_pair)
            .collect::<BTreeMap<_, _>>(),
        BTreeMap::from([(target, title_cells("fresh"))])
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
        .commit_exclusive_settled(tx_id, AuthorSubject::SYSTEM, SKEW_TOLERANCE_MS + 1)
        .unwrap();
    let (dependent, dependent_unit) = client
        .commit_mergeable_unit_settled(
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
        .ingest_commit_unit_settled(tx, versions, 0)
        .unwrap()
        .try_into()
        .unwrap();
    client.apply_sync_message_settled(exclusive_fate).unwrap();
    assert_eq!(
        client.transaction_state_settled(exclusive).unwrap().0,
        Fate::Rejected(RejectionReason::ClientClockTooFarAhead)
    );
    assert_eq!(
        client.transaction_state_settled(dependent).unwrap().0,
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
        .ingest_commit_unit_settled(tx, versions, u64::MAX - SKEW_TOLERANCE_MS)
        .unwrap()
        .try_into()
        .unwrap();
    assert_eq!(
        dependent_fate,
        SyncMessage::FateUpdate {
            tx_id: dependent,
            fate: Fate::Rejected(RejectionReason::Cascade { root: exclusive }),
            global_time: None,
            durability: None,
        }
    );
    client.apply_sync_message_settled(dependent_fate).unwrap();
    assert_eq!(
        client.transaction_state_settled(dependent).unwrap().0,
        Fate::Rejected(RejectionReason::Cascade { root: exclusive })
    );
}

// Stack safety is an implementation-level property. This NodeState integration
// test arranges a deep speculative local chain, then asserts terminal states
// and retracted local rows through its fate entry point.
#[test]
fn client_rejects_deep_local_causal_chain_without_recursing() {
    const DEPTH: usize = 128;

    let (_client_dir, mut client) = open_node_with_uuid(node(1));
    let mut parent = None;
    let mut tx_ids = Vec::with_capacity(DEPTH);

    for time in 1..=DEPTH {
        // Version ancestry is confined to one physical row/layer.  Keep this
        // stack-safety chain in that legitimate coordinate rather than using
        // arbitrary transaction dependencies.
        let mut commit = MergeableCommit::new("todos", row(1), time as u64)
            .cells(title_cells(&format!("depth-{time}")));
        if let Some(parent) = parent {
            commit = commit.parents(vec![parent]);
        }
        let (tx_id, _) = client.commit_mergeable_unit_settled(commit).unwrap();
        parent = Some(tx_id);
        tx_ids.push(tx_id);
    }

    let root = tx_ids[0];
    client
        .apply_fate_update(
            root,
            Fate::Rejected(RejectionReason::ClientClockTooFarAhead),
            None,
            None,
        )
        .unwrap();

    for tx_id in tx_ids {
        assert_eq!(
            client.transaction_state_settled(tx_id).unwrap().0,
            if tx_id == root {
                Fate::Rejected(RejectionReason::ClientClockTooFarAhead)
            } else {
                Fate::Rejected(RejectionReason::Cascade { root })
            }
        );
    }
    assert!(client.current_rows("todos", DurabilityTier::Local).unwrap().is_empty());
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
    let (exclusive, exclusive_unit) = client.commit_exclusive_settled(tx_id, AuthorSubject::SYSTEM, 1).unwrap();
    let (child, child_unit) = client
        .commit_mergeable_unit_settled(
            MergeableCommit::new("todos", row, 2)
                .parents(vec![exclusive])
                .cells(title_cells("child")),
        )
        .unwrap();
    let SyncMessage::CommitUnit { tx, versions } = child_unit else {
        panic!("expected commit unit");
    };
    assert!(
        core.ingest_commit_unit_settled(tx, versions, u64::MAX - SKEW_TOLERANCE_MS)
            .unwrap()
            .is_empty()
    );

    let SyncMessage::CommitUnit { tx, versions } = exclusive_unit else {
        panic!("expected commit unit");
    };
    let updates = core
        .ingest_commit_unit_settled(tx, versions, u64::MAX - SKEW_TOLERANCE_MS)
        .unwrap();
    assert_eq!(core.sync_metrics().parked_orphans_resolved, 1);
    assert_eq!(
        updates,
        vec![
            SyncMessage::FateUpdate {
                tx_id: exclusive,
                fate: Fate::Accepted,
                global_time: Some(GlobalTime::new(1, 0).unwrap()),
                durability: Some(DurabilityTier::Global),
            },
            SyncMessage::FateUpdate {
                tx_id: child,
                fate: Fate::Accepted,
                global_time: Some(GlobalTime::new(2, 0).unwrap()),
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
        .commit_mergeable_unit_settled(
            MergeableCommit::new("todos", row(7), 2)
                .parents(vec![missing])
                .cells(title_cells("child")),
        )
        .unwrap();
    let SyncMessage::CommitUnit { tx, versions } = child_unit else {
        panic!("expected commit unit");
    };
    assert!(
        core.ingest_commit_unit_settled(tx.clone(), versions.clone(), u64::MAX - SKEW_TOLERANCE_MS)
            .unwrap()
            .is_empty()
    );
    assert!(
        core.ingest_commit_unit_settled(tx, versions, u64::MAX - SKEW_TOLERANCE_MS)
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
        let (tx_id, commit_unit) = writer.commit_mergeable_unit_settled(commit).unwrap();
        let made_at = writer.transaction_record(tx_id).unwrap().tx_id.time;
        let mut version = ModelRowVersion::new(row_uuid, tx_id, made_at);
        version.parents = parents;
        version.cells = cells;
        version.deletion = deletion;
        oracle.add_version(version);

        for message in core.apply_sync_message_settled(commit_unit).unwrap() {
            writer.apply_sync_message_settled(message).unwrap();
        }
        assert_current_rows_match_oracle(&mut core, &oracle);
        assert_view_update_result_set_matches_current_rows(&mut core);

        let update = peer.current_rows_update(&mut core, "todos").unwrap();
        reader.apply_sync_message_settled(update).unwrap();
        assert_current_rows_match_oracle(&mut reader, &oracle);
    }
}
#[test]
fn malformed_commit_unit_rejects_write_count_mismatch() {
    let (_writer_dir, mut writer) = open_node_with_uuid(node(1));
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    let (_tx_id, unit) = writer
        .commit_mergeable_unit_settled(MergeableCommit::new("todos", row(1), 10).cells(title_cells("one")))
        .unwrap();
    let SyncMessage::CommitUnit { mut tx, versions } = unit else {
        panic!("expected commit unit");
    };
    tx.n_total_writes = 2;

    let [fate] = core
        .ingest_commit_unit_settled(tx.clone(), versions, u64::MAX - SKEW_TOLERANCE_MS)
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
            global_time: None,
            durability: None,
        }
    );
    assert!(core.row_history("todos", row(1)).unwrap().is_empty());
}

/// A core accepts the final representable public provenance millisecond but
/// rejects an over-range content or deletion carrier before any row state is
/// staged (while retaining the ordinary durable rejection receipt).
///
/// writer ──wire provenance──► core
///   max                      accepts
///   max + 1                  rejects ──► no partial row/tx state
#[test]
fn wire_provenance_hlc_boundary_is_admitted_or_rejected_before_commit_staging() {
    use crate::time::HLC_MAX_PHYSICAL_MS;

    let schema = schema();
    let (_writer_dir, mut writer) = open_node_with_schema(node(1), schema.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema.clone());

    for (row_uuid, deletion) in [(row(0x81), None), (row(0x82), Some(DeletionEvent::Deleted))] {
        let source = if let Some(deletion) = deletion {
            MergeableCommit::new("todos", row_uuid, 10).deletion(deletion)
        } else {
            MergeableCommit::new("todos", row_uuid, 10).cells(title_cells("boundary"))
        };
        let (_tx_id, unit) = writer.commit_mergeable_unit_settled(source).unwrap();
        let SyncMessage::CommitUnit { tx, versions } = unit else {
            panic!("expected commit unit");
        };
        let original = versions.into_iter().next().unwrap();
        let boundary = VersionRecord::encode(
            &schema.tables[0],
            original.schema_version(),
            original.row_uuid(),
            original.parents(),
            original.created_by(),
            HLC_MAX_PHYSICAL_MS,
            original.updated_by(),
            HLC_MAX_PHYSICAL_MS,
            &[original.cell_at(0)],
            original.deletion(),
        )
        .unwrap()
        .with_authored_columns(original.authored_columns().cloned());
        let accepted = core
            .ingest_commit_unit_settled(
                tx.clone(),
                vec![boundary],
                u64::MAX - SKEW_TOLERANCE_MS,
            )
            .unwrap();
        assert!(matches!(
            accepted.as_slice(),
            [SyncMessage::FateUpdate { fate: Fate::Accepted, .. }]
        ));

        let (_bad_tx_id, bad_unit) = writer
            .commit_mergeable_unit_settled(if let Some(deletion) = deletion {
                MergeableCommit::new("todos", row_uuid, 11).deletion(deletion)
            } else {
                MergeableCommit::new("todos", row_uuid, 11).cells(title_cells("too far"))
            })
            .unwrap();
        let SyncMessage::CommitUnit {
            tx: bad_tx,
            versions: bad_versions,
        } = bad_unit
        else {
            panic!("expected commit unit");
        };
        let original = bad_versions.into_iter().next().unwrap();
        let too_far = VersionRecord::encode(
            &schema.tables[0],
            original.schema_version(),
            original.row_uuid(),
            original.parents(),
            original.created_by(),
            HLC_MAX_PHYSICAL_MS + 1,
            original.updated_by(),
            HLC_MAX_PHYSICAL_MS + 1,
            &[original.cell_at(0)],
            original.deletion(),
        )
        .unwrap()
        .with_authored_columns(original.authored_columns().cloned());
        let rejected = core
            .ingest_commit_unit_settled(
                bad_tx.clone(),
                vec![too_far],
                u64::MAX - SKEW_TOLERANCE_MS,
            )
            .unwrap();
        assert!(matches!(
            rejected.as_slice(),
            [SyncMessage::FateUpdate {
                tx_id,
                fate: Fate::Rejected(RejectionReason::MalformedCommit(reason)),
                global_time: None,
                durability: None,
            }] if *tx_id == bad_tx.tx_id && reason.contains("created_at_ms outside the packed HLC")
        ));
        assert!(matches!(
            core.transaction_record(bad_tx.tx_id).resolve(),
            Some(TransactionRecord {
                fate: Fate::Rejected(RejectionReason::MalformedCommit(_)),
                ..
            })
        ));
        // The rejection receipt is durable, but the malformed carrier never
        // reaches history/current-row storage.
        assert_eq!(core.row_history("todos", row_uuid).unwrap().len(), 1);
    }
}

/// Locally authored mergeable writes validate their public provenance before
/// allocating/staging a transaction, rather than reaching `TxTime::from`'s
/// assertion through an internal convenience path.
#[test]
fn local_mergeable_provenance_over_hlc_boundary_is_a_typed_error() {
    use crate::time::HLC_MAX_PHYSICAL_MS;

    let (_dir, mut node) = open_node_with_uuid(node(0x71));
    assert!(matches!(
        node.commit_mergeable_settled(
            MergeableCommit::new("todos", row(0x71), HLC_MAX_PHYSICAL_MS + 1)
                .cells(title_cells("too far")),
        ),
        Err(Error::InvalidMergeableCommit(
            "commit now_ms exceeds packed HLC physical-millisecond range"
        ))
    ));
    assert!(node.row_history("todos", row(0x71)).unwrap().is_empty());
}

#[test]
fn over_limit_commit_unit_rejects_as_malformed_and_next_unit_still_applies() {
    let (_writer_dir, mut writer) = open_node_with_uuid(node(1));
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    let (_tx_id, unit) = writer
        .commit_mergeable_unit_settled(
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
        .apply_sync_message_settled(SyncMessage::CommitUnit {
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
            global_time: None,
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
        .commit_mergeable_unit_settled(MergeableCommit::new("todos", row(2), 11).cells(title_cells("ok")))
        .unwrap();
    let [good_fate] = core
        .apply_sync_message_settled(good_unit)
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
