// Reset and incremental receiver batching, partial bundles, and winner selection.

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
    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        reset_result_set, ..
    }) = &mut incremental_update
    else {
        panic!("expected view update");
    };
    *reset_result_set = false;

    bulk_reader.apply_sync_message_settled(update).unwrap();
    incremental_reader
        .apply_sync_message_settled(incremental_update)
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
fn receiver_batch_ingests_non_reset_complete_bundles_once() {
    let (_writer_dir, mut writer) = open_node_with_uuid(node(1));
    let (_core_dir, mut core) = open_node_with_uuid(node(2));
    let (_reader_dir, mut reader) = open_node_with_uuid(node(3));

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

    let update = core.view_update_for_current_rows("todos").unwrap();
    let mut version_bundles = version_bundles_for_update(&update);
    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        subscription,
        settled_through,
        peer_payload_inventory,
        result_member_adds,
        result_member_removes,
        program_fact_adds,
        program_fact_removes,
        ..
    }) = update
    else {
        panic!("expected view update");
    };
    assert_eq!(version_bundles.len(), 2);
    version_bundles.reverse();

    reader
        .apply_view_updates_in_batch(vec![ViewUpdateParts {
            subscription,
            settled_through,
            defer_settlement: false,
            reset_result_set: false,
            version_carriers: Vec::new(),
            version_bundles,
            peer_complete_tx_payload_refs: peer_payload_inventory.complete_tx_payloads,
            authorization_progress: None,
            opening_pending: false,
            result_member_adds,
            result_member_removes,
            terminal_operations: Vec::new(),
            program_fact_adds,
            program_fact_removes,
        }])
        .unwrap();

    let version_rows = reader.query_all_versions().unwrap();
    assert_eq!(version_rows.len(), 2);
    assert!(
        version_rows
            .iter()
            .any(|version| version.table() == "todos" && version.row_uuid() == row(1))
    );
    assert!(
        version_rows
            .iter()
            .any(|version| version.table() == "todos" && version.row_uuid() == row(2))
    );
    assert_eq!(reader.sync_metrics().receiver_bulk_ingest_commits, 1);
    assert_eq!(reader.sync_metrics().receiver_bulk_bundle_ingests, 2);
    assert_eq!(reader.sync_metrics().receiver_per_bundle_ingests, 0);
}

#[test]
fn receiver_batch_preloads_peer_inventory_bundles_before_membership() {
    let (_writer_dir, mut writer) = open_node_with_uuid(node(1));
    let (_core_dir, mut core) = open_node_with_uuid(node(2));
    let (_reader_dir, mut reader) = open_node_with_uuid(node(3));

    let row_uuid = row(1);
    let (tx_id, unit) = writer
        .commit_mergeable_unit_settled(
            MergeableCommit::new("todos", row_uuid, 10).cells(title_cells("one")),
        )
        .unwrap();
    let SyncMessage::CommitUnit { tx, versions } = unit else {
        panic!("expected commit unit");
    };
    let [fate] = core
        .ingest_commit_unit_settled(tx.clone(), versions.clone(), u64::MAX - SKEW_TOLERANCE_MS)
        .unwrap()
        .try_into()
        .unwrap();
    let SyncMessage::FateUpdate {
        global_time: Some(global_time),
        durability: Some(durability),
        ..
    } = fate
    else {
        panic!("expected accepted fate");
    };
    let subscription = reader.whole_table_subscription_key("todos").unwrap();

    reader
        .apply_view_updates_in_batch(vec![
            ViewUpdateParts {
                subscription,
                settled_through: global_time,
                defer_settlement: false,
                reset_result_set: true,
                version_carriers: Vec::new(),
                version_bundles: Vec::new(),
                peer_complete_tx_payload_refs: Vec::new(),
                authorization_progress: None,
                opening_pending: false,
                result_member_adds: vec![ResultMemberEntry::row((
                    "todos".to_owned().into(),
                    row_uuid,
                    tx_id,
                ))],
                result_member_removes: Vec::new(),
                terminal_operations: Vec::new(),
                program_fact_adds: Vec::new(),
                program_fact_removes: Vec::new(),
            },
            ViewUpdateParts {
                subscription,
                settled_through: global_time,
                defer_settlement: false,
                reset_result_set: false,
                version_carriers: Vec::new(),
                version_bundles: vec![VersionBundle {
                    scope: crate::protocol::VersionBundleScope::CompleteTransaction,
                    tx,
                    versions,
                    fate: Fate::Accepted,
                    global_time: Some(global_time),
                    durability,
                }],
                peer_complete_tx_payload_refs: vec![tx_id],
                authorization_progress: None,
                opening_pending: false,
                result_member_adds: Vec::new(),
                result_member_removes: Vec::new(),
                terminal_operations: Vec::new(),
                program_fact_adds: Vec::new(),
                program_fact_removes: Vec::new(),
            },
        ])
        .unwrap();

    assert_eq!(
        reader
            .subscription_current_rows("todos", DurabilityTier::Global)
            .unwrap()
            .into_iter()
            .map(current_row_pair)
            .collect::<BTreeMap<_, _>>(),
        BTreeMap::from([(row_uuid, title_cells("one"))])
    );
    assert_eq!(reader.sync_metrics().receiver_bulk_ingest_commits, 1);
    assert_eq!(reader.sync_metrics().receiver_bulk_bundle_ingests, 1);
    assert_eq!(reader.sync_metrics().parked_orphans, 0);
}

#[test]
fn receiver_batch_coalesces_partial_bundles_for_same_tx() {
    let (_reader_dir, mut reader) = open_node_with_uuid(node(3));
    let subscription = reader.whole_table_subscription_key("todos").unwrap();
    let tx_id = TxId::new(TxTime::from(10), node(1));
    let tx = Transaction {
        tx_id,
        kind: TxKind::Exclusive,
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
    let first = version_record(row(1), Vec::new(), title_cells("one"), None);
    let second = version_record(row(2), Vec::new(), title_cells("two"), None);
    let mut redacted_tx = tx.clone();
    redacted_tx.n_total_writes = 1;

    reader
        .apply_view_updates_in_batch(vec![
            ViewUpdateParts {
                subscription,
                settled_through: GlobalTime(1),
                defer_settlement: false,
                reset_result_set: true,
                version_carriers: Vec::new(),
                version_bundles: vec![VersionBundle {
                    scope: crate::protocol::VersionBundleScope::ViewScoped,
                    tx: redacted_tx.clone(),
                    versions: vec![first],
                    fate: Fate::Accepted,
                    global_time: Some(GlobalTime(1)),
                    durability: DurabilityTier::Global,
                }],
                peer_complete_tx_payload_refs: Vec::new(),
                authorization_progress: None,
                opening_pending: false,
                result_member_adds: vec![ResultMemberEntry::row((
                    "todos".to_owned().into(),
                    row(1),
                    tx_id,
                ))],
                result_member_removes: Vec::new(),
                terminal_operations: Vec::new(),
                program_fact_adds: Vec::new(),
                program_fact_removes: Vec::new(),
            },
            ViewUpdateParts {
                subscription,
                settled_through: GlobalTime(1),
                defer_settlement: false,
                reset_result_set: true,
                version_carriers: Vec::new(),
                version_bundles: vec![VersionBundle {
                    scope: crate::protocol::VersionBundleScope::ViewScoped,
                    tx: redacted_tx,
                    versions: vec![second],
                    fate: Fate::Accepted,
                    global_time: Some(GlobalTime(1)),
                    durability: DurabilityTier::Global,
                }],
                peer_complete_tx_payload_refs: Vec::new(),
                authorization_progress: None,
                opening_pending: false,
                result_member_adds: vec![ResultMemberEntry::row((
                    "todos".to_owned().into(),
                    row(2),
                    tx_id,
                ))],
                result_member_removes: Vec::new(),
                terminal_operations: Vec::new(),
                program_fact_adds: Vec::new(),
                program_fact_removes: Vec::new(),
            },
        ])
        .unwrap();

    let version_rows = reader.query_all_versions().unwrap();
    assert_eq!(version_rows.len(), 2);
    assert!(
        version_rows
            .iter()
            .any(|version| version.table() == "todos" && version.row_uuid() == row(1))
    );
    assert!(
        version_rows
            .iter()
            .any(|version| version.table() == "todos" && version.row_uuid() == row(2))
    );
    assert_eq!(reader.sync_metrics().receiver_bulk_ingest_commits, 0);
    assert_eq!(reader.sync_metrics().receiver_bulk_bundle_ingests, 0);
    assert_eq!(reader.sync_metrics().receiver_per_bundle_ingests, 2);
}

// This stays internal because it directly exercises the protocol receiver's
// receiver-batch boundary. The public serving tests below assert the matching
// producer-side whole-row payload rule.
#[test]
fn receiver_batch_replays_identical_whole_versions_and_rejects_conflicts() {
    let projection_schema = two_column_schema();
    let (_writer_dir, mut writer) = open_node_with_schema(node(1), projection_schema.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(2), projection_schema.clone());
    let (_reader_dir, mut reader) = open_node_with_schema(node(3), projection_schema.clone());
    let row_uuid = row(1);
    let (tx_id, unit) = writer
        .commit_mergeable_unit_settled(
            MergeableCommit::new("todos", row_uuid, 10).cells(BTreeMap::from([
                ("title".to_owned(), Value::String("visible title".to_owned())),
                ("body".to_owned(), Value::String("visible body".to_owned())),
            ])),
        )
        .unwrap();
    let SyncMessage::CommitUnit { tx, versions } = unit else {
        panic!("expected commit unit");
    };
    let [fate] = core
        .ingest_commit_unit_settled(tx.clone(), versions.clone(), u64::MAX - SKEW_TOLERANCE_MS)
        .unwrap()
        .try_into()
        .unwrap();
    let SyncMessage::FateUpdate {
        global_time: Some(global_time),
        durability: Some(durability),
        ..
    } = fate
    else {
        panic!("expected accepted fate");
    };
    let full = versions.into_iter().next().unwrap();
    let conflicting = VersionRecord::encode(
        &projection_schema.tables[0],
        full.schema_version(),
        full.row_uuid(),
        full.parents(),
        full.created_by(),
        full.created_at_ms(),
        full.updated_by(),
        full.updated_at_ms(),
        &[
            Some(Value::String("conflicting title".to_owned())),
            full.cell_at(1),
        ],
        full.deletion(),
    )
    .unwrap()
    .with_authored_columns(full.authored_columns().cloned());
    let subscription = reader.whole_table_subscription_key("todos").unwrap();
    let update = |version, fate, update_global_time, update_durability, result_member_adds| ViewUpdateParts {
        subscription,
        settled_through: global_time,
        defer_settlement: false,
        reset_result_set: false,
        version_carriers: Vec::new(),
        version_bundles: vec![VersionBundle {
            scope: crate::protocol::VersionBundleScope::CompleteTransaction,
            tx: tx.clone(),
            versions: vec![version],
            fate,
            global_time: update_global_time,
            durability: update_durability,
        }],
        peer_complete_tx_payload_refs: Vec::new(),
        authorization_progress: None,
        opening_pending: false,
        result_member_adds,
        result_member_removes: Vec::new(),
        terminal_operations: Vec::new(),
        program_fact_adds: Vec::new(),
        program_fact_removes: Vec::new(),
    };

    assert!(matches!(
        reader.apply_view_updates_in_batch(vec![
            update(
                full.clone(),
                Fate::Accepted,
                Some(global_time),
                durability,
                Vec::new(),
            ),
            update(
                conflicting.clone(),
                Fate::Accepted,
                Some(global_time),
                durability,
                Vec::new(),
            ),
        ])
        .resolve(),
        Err(Error::ConflictingCommitUnit(conflicting_tx)) if conflicting_tx == tx_id
    ));

    reader
        .apply_view_updates_in_batch(vec![
            update(
                full.clone(),
                Fate::Accepted,
                Some(global_time),
                durability,
                vec![ResultMemberEntry::row((
                    "todos".to_owned().into(),
                    row_uuid,
                    tx_id,
                ))],
            ),
            update(
                full.clone(),
                Fate::Accepted,
                Some(global_time),
                durability,
                vec![ResultMemberEntry::row((
                    "todos".to_owned().into(),
                    row_uuid,
                    tx_id,
                ))],
            ),
        ])
        .unwrap();

    assert_eq!(
        reader
            .subscription_current_rows("todos", DurabilityTier::Global)
            .unwrap()
            .into_iter()
            .map(current_row_pair)
            .collect::<BTreeMap<_, _>>(),
        BTreeMap::from([(
            row_uuid,
            BTreeMap::from([
                ("title".to_owned(), Value::String("visible title".to_owned())),
                ("body".to_owned(), Value::String("visible body".to_owned())),
            ]),
        )])
    );
    assert_eq!(reader.sync_metrics().receiver_bulk_bundle_ingests, 1);
    assert_eq!(reader.sync_metrics().receiver_per_bundle_ingests, 0);

    // A later subscription can replay the exact same immutable row payload
    // with weaker fate metadata. The payload stays idempotent while fate and
    // durability remain monotone.
    reader
        .apply_view_updates_in_batch(vec![update(
            full.clone(),
            Fate::Pending,
            None,
            DurabilityTier::Edge,
            Vec::new(),
        )])
        .unwrap();
    assert_eq!(
        reader.transaction_state_settled(tx_id).unwrap(),
        (Fate::Accepted, Some(global_time), DurabilityTier::Global),
    );

    assert!(matches!(
        reader.apply_view_updates_in_batch(vec![update(
            conflicting,
            Fate::Accepted,
            Some(global_time),
            durability,
            Vec::new(),
        )])
        .resolve(),
        Err(Error::ConflictingCommitUnit(conflicting_tx)) if conflicting_tx == tx_id
    ));
}

#[derive(Clone, Copy, Debug)]
enum ResetConflictPath {
    Batch,
    Single,
}

#[test]
fn reset_batch_rejects_conflicting_authored_columns_in_both_orders() {
    assert_reset_authored_columns_conflict(ResetConflictPath::Batch, false, false);
    assert_reset_authored_columns_conflict(ResetConflictPath::Batch, true, false);
}

#[test]
fn reset_single_rejects_conflicting_authored_columns_in_both_orders() {
    assert_reset_authored_columns_conflict(ResetConflictPath::Single, false, false);
    assert_reset_authored_columns_conflict(ResetConflictPath::Single, true, false);
}

#[test]
fn reset_conflicts_with_member_removals_are_atomic() {
    assert_reset_authored_columns_conflict(ResetConflictPath::Batch, false, true);
    assert_reset_authored_columns_conflict(ResetConflictPath::Single, false, true);
}

#[test]
fn reset_accepts_identical_annotated_duplicates() {
    for path in [ResetConflictPath::Batch, ResetConflictPath::Single] {
        let (_reader_dir, mut reader) = open_node_with_uuid(node(3));
        let subscription = reader.whole_table_subscription_key("todos").unwrap();
        let tx_id = TxId::new(TxTime::from(10), node(1));
        let tx = Transaction {
            tx_id,
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
        };
        let version = version_record(row(1), Vec::new(), title_cells("one"), None)
            .with_authored_columns(Some(BTreeSet::from(["title".to_owned()])));
        let bundles = [version.clone(), version]
            .into_iter()
            .map(|version| VersionBundle {
                scope: crate::protocol::VersionBundleScope::CompleteTransaction,
                tx: tx.clone(),
                versions: vec![version],
                fate: Fate::Accepted,
                global_time: Some(GlobalTime(1)),
                durability: DurabilityTier::Global,
            })
            .collect();
        let update = ViewUpdateParts {
            subscription,
            settled_through: GlobalTime(1),
            defer_settlement: false,
            reset_result_set: true,
            version_carriers: crate::protocol::build_version_carriers_from_singletons(bundles)
                .unwrap(),
            version_bundles: Vec::new(),
            peer_complete_tx_payload_refs: Vec::new(),
            authorization_progress: None,
            opening_pending: false,
            result_member_adds: vec![ResultMemberEntry::row((
                "todos".to_owned().into(),
                row(1),
                tx_id,
            ))],
            result_member_removes: Vec::new(),
            terminal_operations: Vec::new(),
            program_fact_adds: Vec::new(),
            program_fact_removes: Vec::new(),
        };
        match path {
            ResetConflictPath::Batch => reader.apply_view_updates_in_batch(vec![update]).unwrap(),
            ResetConflictPath::Single => reader.apply_view_update(update).unwrap(),
        }
        assert!(reader.query_transaction(tx_id).unwrap().is_some());
        assert_eq!(reader.query_versions_for_tx(tx_id).unwrap().len(), 1);

        let conflicting = version_record(row(1), Vec::new(), title_cells("one"), None);
        let replay = ViewUpdateParts {
            subscription,
            settled_through: GlobalTime(1),
            defer_settlement: true,
            reset_result_set: true,
            version_carriers: Vec::new(),
            version_bundles: vec![VersionBundle {
                scope: crate::protocol::VersionBundleScope::CompleteTransaction,
                tx: tx.clone(),
                versions: vec![conflicting],
                fate: Fate::Accepted,
                global_time: Some(GlobalTime(1)),
                durability: DurabilityTier::Global,
            }],
            peer_complete_tx_payload_refs: Vec::new(),
            authorization_progress: None,
            opening_pending: false,
            result_member_adds: Vec::new(),
            result_member_removes: vec![ResultMemberEntry::row((
                "todos".to_owned().into(),
                row(1),
                tx_id,
            ))],
            terminal_operations: Vec::new(),
            program_fact_adds: Vec::new(),
            program_fact_removes: Vec::new(),
        };
        let result = match path {
            ResetConflictPath::Batch => reader.apply_view_updates_in_batch(vec![replay]).resolve(),
            ResetConflictPath::Single => reader.apply_view_update(replay).resolve(),
        };
        assert!(matches!(
            result,
            Err(Error::ConflictingCommitUnit(conflicting)) if conflicting == tx_id
        ));
        let stored = reader.query_versions_for_tx(tx_id).unwrap();
        assert_eq!(stored.len(), 1);
        assert_eq!(
            reader
                .version_record_from_row(&stored[0])
                .unwrap()
                .authored_columns(),
            Some(&BTreeSet::from(["title".to_owned()]))
        );
    }
}

fn assert_reset_authored_columns_conflict(
    path: ResetConflictPath,
    reversed: bool,
    with_member_removal: bool,
) {
    let (_reader_dir, mut reader) = open_node_with_uuid(node(3));
    let subscription = reader.whole_table_subscription_key("todos").unwrap();
    let binding_view_key = reader
        .binding_view_key_for_subscription(subscription)
        .unwrap();
    let tx_id = TxId::new(TxTime::from(10), node(1));
    let tx = Transaction {
        tx_id,
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
    };
    let unannotated = version_record(row(1), Vec::new(), title_cells("one"), None);
    let authored = unannotated
        .clone()
        .with_authored_columns(Some(BTreeSet::from(["title".to_owned()])));
    assert_eq!(unannotated.record().raw(), authored.record().raw());
    assert_ne!(unannotated, authored);

    let mut versions = vec![unannotated, authored];
    if reversed {
        versions.reverse();
    }
    let version_bundles = versions
        .into_iter()
        .map(|version| VersionBundle {
            scope: crate::protocol::VersionBundleScope::CompleteTransaction,
            tx: tx.clone(),
            versions: vec![version],
            fate: Fate::Accepted,
            global_time: Some(GlobalTime(1)),
            durability: DurabilityTier::Global,
        })
        .collect::<Vec<_>>();
    let version_carriers = crate::protocol::build_version_carriers_from_singletons(version_bundles)
        .expect("two valid bundles form a packed carrier");

    reader.set_initial_sync_flush_cadence(2).unwrap();
    reader
        .query
        .pending_terminal_operations_by_binding_view
        .insert(
            binding_view_key,
            vec![reset_conflict_terminal_operation(1)],
        );
    let cadence_before = (
        reader.initial_sync_flush_active,
        reader.initial_sync_flush_completed,
    );
    let hydration_before = reader.query.initial_hydration_binding_views.clone();
    let pending_terminal_before = reader
        .query
        .pending_terminal_operations_by_binding_view
        .clone();
    let deferred_before = reader.query.deferred_publication_binding_views.clone();

    let update = ViewUpdateParts {
        subscription,
        settled_through: GlobalTime(1),
        defer_settlement: true,
        reset_result_set: true,
        version_carriers,
        version_bundles: Vec::new(),
        peer_complete_tx_payload_refs: Vec::new(),
        authorization_progress: None,
        opening_pending: false,
        result_member_adds: vec![ResultMemberEntry::row((
            "todos".to_owned().into(),
            row(1),
            tx_id,
        ))],
        result_member_removes: with_member_removal
            .then(|| {
                ResultMemberEntry::row(("todos".to_owned().into(), row(9), tx_id))
            })
            .into_iter()
            .collect(),
        terminal_operations: vec![reset_conflict_terminal_operation(2)],
        program_fact_adds: Vec::new(),
        program_fact_removes: Vec::new(),
    };
    let result = match path {
        ResetConflictPath::Batch => reader.apply_view_updates_in_batch(vec![update]).resolve(),
        ResetConflictPath::Single => reader.apply_view_update(update).resolve(),
    };

    assert!(
        matches!(
            result,
            Err(Error::ConflictingCommitUnit(conflicting_tx)) if conflicting_tx == tx_id
        ),
        "{path:?} reset must reject conflicting authored columns (reversed: {reversed})"
    );
    assert!(reader.query_transaction(tx_id).unwrap().is_none());
    assert!(reader.query_versions_for_tx(tx_id).unwrap().is_empty());
    assert!(reader.query_all_versions().unwrap().is_empty());
    assert!(
        reader
            .current_rows("todos", DurabilityTier::Global)
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        (
            reader.initial_sync_flush_active,
            reader.initial_sync_flush_completed,
        ),
        cadence_before
    );
    assert_eq!(
        reader.query.initial_hydration_binding_views,
        hydration_before
    );
    assert_eq!(
        reader.query.pending_terminal_operations_by_binding_view,
        pending_terminal_before
    );
    assert_eq!(
        reader.query.deferred_publication_binding_views,
        deferred_before
    );
}

fn reset_conflict_terminal_operation(marker: u8) -> groove::ivm::TerminalOperation {
    groove::ivm::TerminalOperation {
        root_descriptor: groove::records::RecordDescriptor::default(),
        root_key: vec![marker],
        path: Vec::new(),
        edit: groove::ivm::TerminalEdit::Remove {
            key: vec![marker],
        },
    }
}

// This stays internal because it directly exercises the protocol receiver's
// single-message fragment assembly boundary.
#[test]
fn sequential_partial_exclusive_bundles_index_the_complete_transaction() {
    let (_reader_dir, mut reader) = open_node_with_uuid(node(3));
    let subscription = reader.whole_table_subscription_key("todos").unwrap();
    let tx_id = TxId::new(TxTime::from(10), node(1));
    let tx = Transaction {
        tx_id,
        kind: TxKind::Exclusive,
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
    let updates = [
        (row(1), version_record(row(1), Vec::new(), title_cells("one"), None)),
        (row(2), version_record(row(2), Vec::new(), title_cells("two"), None)),
    ];

    for (row_uuid, version) in updates {
        reader
            .apply_view_update(partial_exclusive_view_update(
                subscription,
                tx.clone(),
                row_uuid,
                version,
            ))
            .unwrap();
    }

    assert_eq!(
        reader
            .current_rows("todos", DurabilityTier::Global)
            .unwrap()
            .into_iter()
            .map(current_row_pair)
            .collect::<BTreeMap<_, _>>(),
        BTreeMap::from([(row(1), title_cells("one")), (row(2), title_cells("two"))])
    );
}

#[test]
fn completing_partial_exclusive_transaction_rejects_conflicting_metadata() {
    let (_reader_dir, mut reader) = open_node_with_uuid(node(3));
    let subscription = reader.whole_table_subscription_key("todos").unwrap();
    let tx_id = TxId::new(TxTime::from(10), node(1));
    let tx = Transaction {
        tx_id,
        kind: TxKind::Exclusive,
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
    reader
        .apply_view_update(partial_exclusive_view_update(
            subscription,
            tx.clone(),
            row(1),
            version_record(row(1), Vec::new(), title_cells("one"), None),
        ))
        .unwrap();

    let mut conflicting_tx = tx;
    conflicting_tx.made_by = AuthorSubject::for_test_bytes([0xa1; 16]);
    assert!(matches!(
        reader.apply_view_update(partial_exclusive_view_update(
            subscription,
            conflicting_tx,
            row(2),
            version_record(row(2), Vec::new(), title_cells("two"), None),
        ))
        .resolve(),
        Err(Error::ConflictingCommitUnit(conflicting)) if conflicting == tx_id
    ));
    assert_eq!(
        reader.query_versions_for_tx(tx_id).unwrap().len(),
        1,
        "the conflicting completing fragment must not be stored"
    );
    assert_eq!(
        reader.query_transaction(tx_id).unwrap().unwrap().tx.made_by,
        AuthorSubject::SYSTEM,
        "the original transaction metadata must remain authoritative"
    );
}

fn partial_exclusive_view_update(
    subscription: SubscriptionKey,
    tx: Transaction,
    row_uuid: RowUuid,
    version: VersionRecord,
) -> ViewUpdateParts {
    let tx_id = tx.tx_id;
    let mut tx = tx;
    tx.n_total_writes = 1;
    ViewUpdateParts {
        subscription,
        settled_through: GlobalTime(1),
        defer_settlement: false,
        reset_result_set: false,
        version_carriers: Vec::new(),
        version_bundles: vec![VersionBundle {
            scope: crate::protocol::VersionBundleScope::ViewScoped,
            tx,
            versions: vec![version],
            fate: Fate::Accepted,
            global_time: Some(GlobalTime(1)),
            durability: DurabilityTier::Global,
        }],
        peer_complete_tx_payload_refs: Vec::new(),
        authorization_progress: None,
        opening_pending: false,
        result_member_adds: vec![ResultMemberEntry::row((
            "todos".to_owned().into(),
            row_uuid,
            tx_id,
        ))],
        result_member_removes: Vec::new(),
        terminal_operations: Vec::new(),
        program_fact_adds: Vec::new(),
        program_fact_removes: Vec::new(),
    }
}

#[test]
fn receiver_batch_resolves_current_winner_across_bundles() {
    let (_writer_dir, mut writer) = open_node_with_uuid(node(1));
    let (_core_dir, mut core) = open_node_with_uuid(node(2));
    let (_reader_dir, mut reader) = open_node_with_uuid(node(3));
    let row_uuid = row(1);

    let (_old_tx, old_unit) = writer
        .commit_mergeable_unit_settled(
            MergeableCommit::new("todos", row_uuid, 10).cells(title_cells("old")),
        )
        .unwrap();
    let SyncMessage::CommitUnit {
        tx: old,
        versions: old_versions,
    } = old_unit
    else {
        panic!("expected commit unit");
    };
    let [old_fate]: [SyncMessage; 1] = core
        .ingest_commit_unit_settled(old.clone(), old_versions.clone(), 0)
        .unwrap()
        .try_into()
        .unwrap();
    let SyncMessage::FateUpdate {
        global_time: Some(old_seq),
        durability: Some(old_durability),
        ..
    } = old_fate
    else {
        panic!("expected accepted old fate");
    };

    let (new_tx, new_unit) = writer
        .commit_mergeable_unit_settled(
            MergeableCommit::new("todos", row_uuid, 11).cells(title_cells("new")),
        )
        .unwrap();
    let SyncMessage::CommitUnit {
        tx: new,
        versions: new_versions,
    } = new_unit
    else {
        panic!("expected commit unit");
    };
    let [new_fate]: [SyncMessage; 1] = core
        .ingest_commit_unit_settled(new.clone(), new_versions.clone(), 1)
        .unwrap()
        .try_into()
        .unwrap();
    let SyncMessage::FateUpdate {
        global_time: Some(new_seq),
        durability: Some(new_durability),
        ..
    } = new_fate
    else {
        panic!("expected accepted new fate");
    };
    let subscription = reader.whole_table_subscription_key("todos").unwrap();

    reader
        .apply_view_updates_in_batch(vec![ViewUpdateParts {
            subscription,
            settled_through: new_seq,
            defer_settlement: false,
            reset_result_set: false,
            version_carriers: Vec::new(),
            version_bundles: vec![
                VersionBundle {
                    scope: crate::protocol::VersionBundleScope::CompleteTransaction,
                    tx: new,
                    versions: new_versions,
                    fate: Fate::Accepted,
                    global_time: Some(new_seq),
                    durability: new_durability,
                },
                VersionBundle {
                    scope: crate::protocol::VersionBundleScope::CompleteTransaction,
                    tx: old,
                    versions: old_versions,
                    fate: Fate::Accepted,
                    global_time: Some(old_seq),
                    durability: old_durability,
                },
            ],
            peer_complete_tx_payload_refs: Vec::new(),
            authorization_progress: None,
            opening_pending: false,
            result_member_adds: vec![ResultMemberEntry::row((
                "todos".to_owned().into(),
                row_uuid,
                new_tx,
            ))],
            result_member_removes: Vec::new(),
            terminal_operations: Vec::new(),
            program_fact_adds: Vec::new(),
            program_fact_removes: Vec::new(),
        }])
        .unwrap();

    assert_eq!(
        reader
            .current_rows("todos", DurabilityTier::Global)
            .unwrap(),
        vec![(row_uuid, title_cells("new"))]
    );
    assert_eq!(reader.sync_metrics().receiver_bulk_ingest_commits, 1);
    assert_eq!(reader.sync_metrics().receiver_bulk_bundle_ingests, 2);
    assert_eq!(reader.sync_metrics().receiver_per_bundle_ingests, 0);
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
        made_by: AuthorSubject::SYSTEM,
        permission_subject: None,
        base_snapshot: None,
        row_read_set: None,
        absent_read_set: None,
        predicate_read_set: None,
        user_metadata_json: None,
        contribution_merge: None,
    };
    let first = version_record(row(1), Vec::new(), title_cells("one"), None);
    let second = version_record(row(2), Vec::new(), title_cells("two"), None);
    let mut redacted_tx = tx.clone();
    redacted_tx.n_total_writes = 1;

    reader
        .apply_sync_message_settled(SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
            subscription,
            settled_through: GlobalTime(0),
            reset_result_set: false,
            version_carriers: Vec::new(),
            version_bundles: vec![VersionBundle {
                scope: crate::protocol::VersionBundleScope::ViewScoped,
                tx: redacted_tx.clone(),
                versions: vec![first],
                fate: Fate::Accepted,
                global_time: Some(GlobalTime(1)),
                durability: DurabilityTier::Global,
            }],
            peer_payload_inventory: crate::protocol::PeerPayloadInventory::default(),
            result_member_adds: vec![("todos".to_owned().into(), row(1), tx_id).into()],
            result_member_removes: Vec::new(),
            terminal_operations: Vec::new(),
            program_fact_adds: Vec::new(),
            program_fact_removes: Vec::new(),
        }))
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
        .apply_sync_message_settled(SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
            subscription,
            settled_through: GlobalTime(0),
            reset_result_set: false,
            version_carriers: Vec::new(),
            version_bundles: vec![VersionBundle {
                scope: crate::protocol::VersionBundleScope::ViewScoped,
                tx: redacted_tx,
                versions: vec![second],
                fate: Fate::Accepted,
                global_time: Some(GlobalTime(1)),
                durability: DurabilityTier::Global,
            }],
            peer_payload_inventory: crate::protocol::PeerPayloadInventory::default(),
            result_member_adds: vec![("todos".to_owned().into(), row(2), tx_id).into()],
            result_member_removes: Vec::new(),
            terminal_operations: Vec::new(),
            program_fact_adds: Vec::new(),
            program_fact_removes: Vec::new(),
        }))
        .unwrap();
    assert_eq!(
        reader.current_rows("todos", DurabilityTier::Local).unwrap(),
        vec![(row(1), title_cells("one")), (row(2), title_cells("two")),]
    );
}

// This is internal because the durable redacted-cardinality marker is protocol
// receiver state; public clients can observe only the resulting rows.
#[test]
fn view_scoped_cardinality_survives_reopen_and_upgrades_to_complete_payload() {
    let (reader_dir, mut reader) = open_node_with_uuid(node(3));
    let subscription = reader.whole_table_subscription_key("todos").unwrap();
    let tx_id = TxId::new(TxTime::from(10), node(1));
    let tx = Transaction {
        tx_id,
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
    let first = version_record(row(1), Vec::new(), title_cells("one"), None);
    let second = version_record(row(2), Vec::new(), title_cells("two"), None);
    let mut redacted_tx = tx.clone();
    redacted_tx.n_total_writes = 1;
    reader
        .apply_sync_message_settled(SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
            subscription,
            settled_through: GlobalTime(1),
            reset_result_set: false,
            version_carriers: Vec::new(),
            version_bundles: vec![VersionBundle {
                scope: crate::protocol::VersionBundleScope::ViewScoped,
                tx: redacted_tx,
                versions: vec![first.clone()],
                fate: Fate::Accepted,
                global_time: Some(GlobalTime(1)),
                durability: DurabilityTier::Global,
            }],
            peer_payload_inventory: crate::protocol::PeerPayloadInventory::default(),
            result_member_adds: vec![("todos".to_owned().into(), row(1), tx_id).into()],
            result_member_removes: Vec::new(),
            terminal_operations: Vec::new(),
            program_fact_adds: Vec::new(),
            program_fact_removes: Vec::new(),
        }))
        .unwrap();
    assert!(reader.query_transaction(tx_id).unwrap().unwrap().view_scoped_cardinality);

    drop(reader);
    let mut reader = reopen_node_at(&reader_dir, node(3), schema());
    assert!(reader.query_transaction(tx_id).unwrap().unwrap().view_scoped_cardinality);
    reader
        .apply_sync_message_settled(SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
            subscription,
            settled_through: GlobalTime(1),
            reset_result_set: false,
            version_carriers: Vec::new(),
            version_bundles: vec![VersionBundle {
                scope: crate::protocol::VersionBundleScope::CompleteTransaction,
                tx,
                versions: vec![first, second],
                fate: Fate::Accepted,
                global_time: Some(GlobalTime(1)),
                durability: DurabilityTier::Global,
            }],
            peer_payload_inventory: crate::protocol::PeerPayloadInventory::default(),
            result_member_adds: vec![("todos".to_owned().into(), row(2), tx_id).into()],
            result_member_removes: Vec::new(),
            terminal_operations: Vec::new(),
            program_fact_adds: Vec::new(),
            program_fact_removes: Vec::new(),
        }))
        .unwrap();
    let stored = reader.query_transaction(tx_id).unwrap().unwrap();
    assert_eq!(stored.tx.n_total_writes, 2);
    assert!(!stored.view_scoped_cardinality);
}
