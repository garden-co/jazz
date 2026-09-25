// Reset and incremental receiver batching, partial bundles, and winner selection.

// Internal ingress fixtures deliberately construct exact wire frames to test
// atomic batching and malformed predecessors, which public scheduling cannot select.
fn todos_physical_table() -> crate::ids::GlobalPhysicalTableId {
    static TABLE: std::sync::OnceLock<crate::ids::GlobalPhysicalTableId> =
        std::sync::OnceLock::new();
    *TABLE.get_or_init(|| {
        let schema = schema();
        let families = schema.column_families();
        let refs = families.iter().map(String::as_str).collect::<Vec<_>>();
        let storage = groove::storage::MemoryStorage::new(&refs).unwrap();
        let fixture =
            NodeState::new_with_shared_test_catalogue(node(0xf8), schema, storage).unwrap();
        fixture
            .scope_physical_table(fixture.catalogue.local_schema_version_id, "todos")
            .unwrap()
    })
}

fn todos_covered_input(tx: TxId, version: &VersionRecord) -> crate::protocol::SupportingRow {
    crate::protocol::SupportingRow {
        physical_table: todos_physical_table(),
        version_table: "todos".to_owned().into(),
        row: version.row_uuid(),
        version: crate::protocol::RowVersionRefEntry {
            tx,
            schema_version: None,
            layer: crate::protocol::ResultRowLayer::Content,
            batch: Some(tx),
            branch_or_prefix: (!version.branch_key().canonical_bytes().is_empty())
                .then(|| version.branch_key().canonical_bytes()),
            row_digest: None,
        },
    }
}

fn todos_source_closure(
    tx: TxId,
    versions: &[VersionRecord],
) -> Vec<crate::protocol::SupportingRow> {
    versions
        .iter()
        .map(|version| todos_covered_input(tx, version))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn removed_todos_input(tx: TxId) -> crate::protocol::SupportingRow {
    todos_covered_input(
        tx,
        &version_record(row(9), Vec::new(), title_cells("removed"), None),
    )
}

fn fixture_scope_update(
    subscription: SubscriptionKey,
    reset: bool,
    rows: Vec<crate::protocol::SupportingRow>,
    removes: Vec<crate::protocol::SupportingRow>,
) -> crate::protocol::SupportingRowsUpdate {
    // Each fixture builds its frames in send order. A reset begins a new
    // predecessor chain; ordinary fragments explicitly advance that receipt.
    thread_local! { static REVISIONS: std::cell::RefCell<BTreeMap<SubscriptionKey, [u8;16]>> = Default::default(); }
    REVISIONS.with(|revisions| {
        let mut revisions = revisions.borrow_mut();
        let revision = *uuid::Uuid::new_v4().as_bytes();
        let predecessor = revisions
            .insert(subscription, revision)
            .unwrap_or([0xff; 16]);
        if reset && removes.is_empty() {
            crate::protocol::SupportingRowsUpdate::Snapshot { revision, rows }
        } else {
            crate::protocol::SupportingRowsUpdate::Delta {
                predecessor,
                revision,
                adds: rows,
                removes,
            }
        }
    })
}

fn todos_receiver_reset(subscription: SubscriptionKey) -> ViewUpdateParts {
    ViewUpdateParts {
        wire_rows: Some(fixture_scope_update(
            subscription,
            true,
            Vec::new(),
            Vec::new(),
        )),
        subscription,
        settled_through: GlobalTime(0),
        defer_settlement: false,
        reset_input_set: true,
        version_carriers: Vec::new(),
        peer_complete_tx_payload_refs: Vec::new(),
        authorization_progress: None,
        opening_pending: false,
        result_member_adds: Vec::new(),
        result_member_removes: Vec::new(),
    }
}
struct ResidentInventoryFixture {
    _reader_dir: tempfile::TempDir,
    reader: NodeState<RocksDbStorage>,
    row_uuid: RowUuid,
    tx_id: TxId,
    tx: Transaction,
    versions: Vec<VersionRecord>,
    global_time: GlobalTime,
    durability: DurabilityTier,
    subscription: SubscriptionKey,
    source_closure: Vec<crate::protocol::SupportingRow>,
}

fn resident_inventory_fixture() -> ResidentInventoryFixture {
    let (_writer_dir, mut writer) = open_node_with_uuid(node(1));
    let (_core_dir, mut core) = open_node_with_uuid(node(2));
    let (reader_dir, mut reader) = open_node_with_uuid(node(3));
    register_whole_table_receiver(&mut reader, "todos");

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
    let source_closure = versions
        .iter()
        .map(|version| todos_covered_input(tx_id, version))
        .collect();

    ResidentInventoryFixture {
        _reader_dir: reader_dir,
        reader,
        row_uuid,
        tx_id,
        tx,
        versions,
        global_time,
        durability,
        subscription,
        source_closure,
    }
}

#[test]
fn cold_and_warm_complete_snapshots_ingest_same_versions() {
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
    let incremental_update = update.clone();
    // A warm receiver receives the same complete snapshot after its empty
    // predecessor. Neither path can interpret this payload as a row delta.

    register_whole_table_receiver(&mut bulk_reader, "todos");
    bulk_reader.apply_sync_message_settled(update).unwrap();
    register_whole_table_receiver(&mut incremental_reader, "todos");
    let subscription = incremental_reader
        .whole_table_subscription_key("todos")
        .unwrap();
    incremental_reader
        .apply_view_update(todos_receiver_reset(subscription))
        .unwrap();
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

/// Receiver-level coverage pins the reset fast path and its fallback. Public
/// row/history APIs check results; public work counters establish the proof
/// was selected, which row equality alone cannot demonstrate.
#[test]
fn empty_history_reset_heads_match_history_and_populated_table_falls_back() {
    // Raw current records contain node-local transaction aliases. Compare the
    // complete application payload and public row identity across replicas.
    let public_rows = |rows: Vec<CurrentRow>| {
        rows.into_iter()
            .map(|record| (record.row_uuid(), record.cell(&schema().tables[0], "title")))
            .collect::<BTreeMap<_, _>>()
    };
    let (_writer_dir, mut writer) = open_node_with_uuid(node(1));
    let (_core_dir, mut core) = open_node_with_uuid(node(2));
    let (reader_dir, mut reader) = open_node_with_uuid(node(3));
    for (id, time, title) in [(1, 10, "one"), (2, 11, "two"), (1, 12, "newer")] {
        commit_mergeable_global(
            &mut writer,
            &mut core,
            MergeableCommit::new("todos", row(id), time).cells(title_cells(title)),
        );
    }
    register_whole_table_receiver(&mut reader, "todos");
    let mut peer = PeerState::new();
    let update = peer.rehydrate_current_rows(&mut core, "todos").unwrap();
    reader.apply_sync_message_settled(update.clone()).unwrap();
    assert_currency_tables_match_storage(&mut reader, "todos");
    assert_eq!(
        public_rows(
            reader
                .current_rows("todos", DurabilityTier::Global)
                .unwrap()
        ),
        public_rows(core.current_rows("todos", DurabilityTier::Global).unwrap())
    );
    let expected = reader.query_all_versions().unwrap();
    reader.apply_sync_message_settled(update).unwrap();
    assert_eq!(reader.query_all_versions().unwrap(), expected);

    drop(reader);
    let mut reader = reopen_node_at(&reader_dir, node(3), schema());
    assert_eq!(reader.query_all_versions().unwrap(), expected);
    assert_currency_tables_match_storage(&mut reader, "todos");
    register_whole_table_receiver(&mut reader, "todos");
    commit_mergeable_global(
        &mut writer,
        &mut core,
        MergeableCommit::new("todos", row(3), 13).cells(title_cells("three")),
    );
    // Explicitly exercise bulk reset against a nonempty table: a reopened
    // ordinary subscription may resume its retained receiver state instead.
    // Persisted sibling history defeats the proof despite the new row's absence.
    let update = PeerState::new()
        .rehydrate_current_rows(&mut core, "todos")
        .unwrap();
    let bundles = version_bundles_for_update(&update);
    let refs = bundles
        .iter()
        .map(VersionBundle::as_ref)
        .collect::<Vec<_>>();
    assert_eq!(
        reader
            .ingest_reset_view_bundle_refs_in_bulk(&refs, None)
            .unwrap()
            .len(),
        1
    );
    reader.apply_sync_message_settled(update).unwrap();
    assert_currency_tables_match_storage(&mut reader, "todos");
    assert_eq!(
        public_rows(
            reader
                .current_rows("todos", DurabilityTier::Global)
                .unwrap()
        ),
        public_rows(core.current_rows("todos", DurabilityTier::Global).unwrap())
    );
}

/// The exact derived head set is internal metadata, and ordinary subscription
/// projections may omit concurrent history. Drive the internal reset-bundle
/// seam to pin the bulk algorithm, then compare its durable head/history
/// oracle and cancellation boundaries rather than a timing-only counter.
#[test]
fn empty_history_reset_concurrent_heads_are_atomic_across_cancellation() {
    use groove::storage::{TestStorage, TestStorageOperation};
    let schema = schema();
    let bundles = (0..3)
        .map(|i| {
            let tx_id = TxId::new(TxTime::from(100 + i), node(0xe1));
            let mut bundle = reset_scope_bundle(
                reset_scope_tx(tx_id, 1),
                crate::protocol::VersionBundleScope::CompleteTransaction,
                vec![version_record(
                    row(1),
                    Vec::new(),
                    title_cells(&format!("head {i}")),
                    None,
                )],
            );
            bundle.global_time = Some(GlobalTime(i + 1));
            bundle
        })
        .collect::<Vec<_>>();
    let bundle_refs = bundles
        .iter()
        .map(VersionBundle::as_ref)
        .collect::<Vec<_>>();
    let families = schema.column_families();
    let refs = families.iter().map(String::as_str).collect::<Vec<_>>();
    let mut completed = false;
    for allowed_writes in 0..12 {
        let (storage, control) = TestStorage::controlled(&refs);
        let reopen_handle = storage.clone();
        let mut reader =
            NodeState::new_with_shared_test_catalogue(node(0xe2), schema.clone(), storage).unwrap();
        control.take_observed();
        control.pause_on(TestStorageOperation::WriteMany);
        let mut ingest = Box::pin(reader.ingest_reset_view_bundle_refs_in_bulk(&bundle_refs, None));
        let mut released = 0;
        let mut stopped = false;
        for _ in 0..50_000 {
            match std::future::Future::poll(
                ingest.as_mut(),
                &mut std::task::Context::from_waker(std::task::Waker::noop()),
            ) {
                std::task::Poll::Ready(result) => {
                    assert_eq!(result.unwrap().len(), bundles.len());
                    completed = true;
                    stopped = true;
                    break;
                }
                std::task::Poll::Pending => {}
            }
            let writes = control
                .observed()
                .iter()
                .filter(|operation| **operation == TestStorageOperation::WriteMany)
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
        assert!(
            stopped,
            "reset did not reach a bounded persistence boundary"
        );
        drop(ingest);
        drop(reader);
        control.resume();
        let storage = crate::db::block_on(reopen_handle.reopen(families.clone())).unwrap();
        let mut reopened =
            NodeState::new_with_shared_test_catalogue(node(0xe2), schema.clone(), storage).unwrap();
        let present = bundles
            .iter()
            .filter(|bundle| {
                reopened
                    .query_transaction(bundle.tx.tx_id)
                    .unwrap()
                    .is_some()
            })
            .count();
        assert!(
            present == 0 || present == bundles.len(),
            "accepted prefix after {allowed_writes} writes"
        );
        if present != 0 {
            assert_eq!(reopened.query_all_versions().unwrap().len(), bundles.len());
        }
        if completed {
            break;
        }
    }
    assert!(
        completed,
        "include a completed reset, not only canceled attempts"
    );
}

/// Receiver-level coverage pins the optimized reset path as well as ordinary
/// ingestion. A high-level transport test cannot require that batching choice.
#[test]
fn snapshot_ingestion_advances_clock_before_a_local_edit() {
    for reset in [false, true] {
        let (_writer_dir, mut writer) = open_node_with_uuid(node(1));
        let (_core_dir, mut core) = open_node_with_uuid(node(2));
        let (_reader_dir, mut reader) = open_node_with_uuid(node(3));
        commit_mergeable_global(
            &mut writer,
            &mut core,
            MergeableCommit::new("todos", row(1), 1_800_000_000_000).cells(title_cells("before")),
        );
        let mut peer = PeerState::new();
        let update = peer.rehydrate_current_rows(&mut core, "todos").unwrap();
        register_whole_table_receiver(&mut reader, "todos");
        if !reset {
            let subscription = reader.whole_table_subscription_key("todos").unwrap();
            reader
                .apply_view_update(todos_receiver_reset(subscription))
                .unwrap();
        }
        reader.apply_sync_message_settled(update).unwrap();
        // A client's wall clock may lag what it has just read. Its next
        // transaction still has to follow every admitted snapshot transaction.
        commit_mergeable_global(
            &mut reader,
            &mut core,
            MergeableCommit::new("todos", row(1), 1).cells(title_cells("after")),
        );
        let rows = core.current_rows("todos", DurabilityTier::Global).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].cell(&schema().tables[0], "title"),
            Some(Value::String("after".to_owned())),
            "an acknowledged edit after reset={reset} must win over the snapshot it observed",
        );
    }
}

#[test]
fn receiver_batch_ingests_complete_snapshot_bundles_once() {
    let (_writer_dir, mut writer) = open_node_with_uuid(node(1));
    let (_core_dir, mut core) = open_node_with_uuid(node(2));
    let (_reader_dir, mut reader) = open_node_with_uuid(node(3));
    register_whole_table_receiver(&mut reader, "todos");

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
        supporting_rows: program_fact_adds,
        ..
    }) = update
    else {
        panic!("expected view update");
    };
    assert_eq!(version_bundles.len(), 2);
    version_bundles.reverse();
    // Both entries are complete sets; exact native payloads ingest only once.

    reader
        .apply_view_updates_in_batch(vec![
            todos_receiver_reset(subscription),
            ViewUpdateParts {
                wire_rows: Some(program_fact_adds),
                subscription,
                settled_through,
                defer_settlement: false,
                reset_input_set: true,
                version_carriers: crate::protocol::build_version_carriers_from_singletons(
                    version_bundles,
                )
                .unwrap(),
                peer_complete_tx_payload_refs: peer_payload_inventory.complete_tx_payloads,
                authorization_progress: None,
                opening_pending: false,
                result_member_adds: Vec::new(),
                result_member_removes: Vec::new(),
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
    assert_eq!(reader.sync_metrics().receiver_bulk_ingest_commits, 1);
    assert_eq!(reader.sync_metrics().receiver_bulk_bundle_ingests, 2);
    assert_eq!(reader.sync_metrics().receiver_per_bundle_ingests, 0);
}

fn complete_parent_receiver_update(
    subscription: SubscriptionKey,
    tx: Transaction,
    version: VersionRecord,
    reset_input_set: bool,
) -> ViewUpdateParts {
    let tx_id = tx.tx_id;
    let program_fact_adds = if reset_input_set {
        todos_source_closure(tx_id, std::slice::from_ref(&version))
    } else {
        vec![todos_covered_input(tx_id, &version)]
    };
    ViewUpdateParts {
        wire_rows: Some(fixture_scope_update(
            subscription,
            reset_input_set,
            program_fact_adds,
            Vec::new(),
        )),
        subscription,
        settled_through: GlobalTime(1),
        defer_settlement: false,
        reset_input_set,
        version_carriers: vec![VersionCarrier::Bundle(VersionBundle {
            scope: crate::protocol::VersionBundleScope::CompleteTransaction,
            tx,
            versions: vec![version],
            fate: Fate::Accepted,
            global_time: Some(GlobalTime(1)),
            durability: DurabilityTier::Global,
        })],
        peer_complete_tx_payload_refs: Vec::new(),
        authorization_progress: None,
        opening_pending: false,
        result_member_adds: Vec::new(),
        result_member_removes: Vec::new(),
    }
}

fn accepted_view_scoped_child_for_parent(
    reader: &mut NodeState<RocksDbStorage>,
    parent: TxId,
    child: TxId,
    row_uuid: RowUuid,
) {
    reader
        .ingest_view_scoped_transaction_with_current_indexes(
            Transaction {
                tx_id: child,
                kind: TxKind::Mergeable,
                n_total_writes: 1,
                made_by: AuthorSubject::system_at(child.node),
                permission_subject: None,
                base_snapshot: None,
                row_read_set: None,
                absent_read_set: None,
                predicate_read_set: None,
                user_metadata_json: None,
                contribution_merge: None,
            },
            vec![version_record(
                row_uuid,
                vec![parent],
                title_cells("accepted partial child"),
                None,
            )],
            Fate::Accepted,
            Some(GlobalTime(2)),
            DurabilityTier::Global,
        )
        .unwrap();
}

#[test]
fn receiver_batch_preloads_peer_inventory_bundles_before_membership() {
    let ResidentInventoryFixture {
        _reader_dir,
        mut reader,
        row_uuid,
        tx_id,
        tx,
        versions,
        global_time,
        durability,
        subscription,
        source_closure,
    } = resident_inventory_fixture();
    // The preceding reset supplies the manifest; this live frame adds rows only.

    reader
        .apply_view_updates_in_batch(vec![
            ViewUpdateParts {
                wire_rows: Some(fixture_scope_update(
                    subscription,
                    true,
                    Vec::new(),
                    Vec::new(),
                )),
                subscription,
                settled_through: global_time,
                defer_settlement: false,
                reset_input_set: true,
                version_carriers: Vec::new(),
                peer_complete_tx_payload_refs: Vec::new(),
                authorization_progress: None,
                opening_pending: false,
                result_member_adds: Vec::new(),
                result_member_removes: Vec::new(),
            },
            ViewUpdateParts {
                wire_rows: Some(fixture_scope_update(
                    subscription,
                    false,
                    source_closure,
                    Vec::new(),
                )),
                subscription,
                settled_through: global_time,
                defer_settlement: false,
                reset_input_set: false,
                version_carriers: vec![VersionCarrier::Bundle(VersionBundle {
                    scope: crate::protocol::VersionBundleScope::CompleteTransaction,
                    tx,
                    versions,
                    fate: Fate::Accepted,
                    global_time: Some(global_time),
                    durability,
                })],
                peer_complete_tx_payload_refs: vec![tx_id],
                authorization_progress: None,
                opening_pending: false,
                result_member_adds: Vec::new(),
                result_member_removes: Vec::new(),
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
fn receiver_inventory_only_reset_finishes_initial_sync_cadence() {
    let ResidentInventoryFixture {
        _reader_dir,
        mut reader,
        row_uuid,
        tx_id,
        tx,
        versions,
        global_time,
        durability,
        subscription,
        source_closure,
    } = resident_inventory_fixture();

    // Admit the exact transaction body before the target reset. The target
    // frame below is the first frame using this process's relaxed cadence.
    reader
        .apply_view_updates_in_batch(vec![
            ViewUpdateParts {
                wire_rows: Some(fixture_scope_update(
                    subscription,
                    true,
                    Vec::new(),
                    Vec::new(),
                )),
                subscription,
                settled_through: global_time,
                defer_settlement: false,
                reset_input_set: true,
                version_carriers: Vec::new(),
                peer_complete_tx_payload_refs: Vec::new(),
                authorization_progress: None,
                opening_pending: false,
                result_member_adds: Vec::new(),
                result_member_removes: Vec::new(),
            },
            ViewUpdateParts {
                wire_rows: Some(fixture_scope_update(
                    subscription,
                    false,
                    source_closure.clone(),
                    Vec::new(),
                )),
                subscription,
                settled_through: global_time,
                defer_settlement: false,
                reset_input_set: false,
                version_carriers: vec![VersionCarrier::Bundle(VersionBundle {
                    scope: crate::protocol::VersionBundleScope::CompleteTransaction,
                    tx,
                    versions,
                    fate: Fate::Accepted,
                    global_time: Some(global_time),
                    durability,
                })],
                peer_complete_tx_payload_refs: vec![tx_id],
                authorization_progress: None,
                opening_pending: false,
                result_member_adds: Vec::new(),
                result_member_removes: Vec::new(),
            },
        ])
        .unwrap();

    reader.set_initial_sync_flush_cadence(2).unwrap();
    // Private hydration and cadence state is intentional here: public row
    // results cannot observe whether initial-sync cadence has completed.
    assert!(!reader.initial_sync_flush_active);
    assert!(!reader.initial_sync_flush_completed);

    reader
        .apply_view_updates_in_batch(vec![ViewUpdateParts {
            wire_rows: Some(fixture_scope_update(
                subscription,
                true,
                source_closure,
                Vec::new(),
            )),
            subscription,
            settled_through: global_time,
            defer_settlement: false,
            reset_input_set: true,
            version_carriers: Vec::new(),
            peer_complete_tx_payload_refs: vec![tx_id],
            authorization_progress: None,
            opening_pending: false,
            result_member_adds: Vec::new(),
            result_member_removes: Vec::new(),
        }])
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
    assert!(authority_hydration_receipts(&reader).0.is_empty());
    assert!(!reader.initial_sync_flush_active);
    assert!(reader.initial_sync_flush_completed);
}

/// Two independently authorized reset fragments from one exclusive transaction
/// are coalesced into one atomic local current projection.
///
/// core ──todo fragment + sibling fragment──► alice's relay
#[test]
fn receiver_batch_coalesces_partial_bundles_for_same_tx() {
    let (_reader_dir, mut reader) = open_node_with_uuid(node(3));
    register_whole_table_receiver(&mut reader, "todos");
    let subscription = reader.whole_table_subscription_key("todos").unwrap();
    let tx_id = TxId::new(TxTime::from(10), node(1));
    let tx = Transaction {
        tx_id,
        kind: TxKind::Exclusive,
        n_total_writes: 2,
        made_by: AuthorSubject::system_at(tx_id.node),
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
                wire_rows: Some(fixture_scope_update(
                    subscription,
                    true,
                    todos_source_closure(tx_id, std::slice::from_ref(&first)),
                    Vec::new(),
                )),
                subscription,
                settled_through: GlobalTime(1),
                defer_settlement: false,
                reset_input_set: true,
                version_carriers: vec![VersionCarrier::Bundle(VersionBundle {
                    scope: crate::protocol::VersionBundleScope::ViewScoped,
                    tx: redacted_tx.clone(),
                    versions: vec![first.clone()],
                    fate: Fate::Accepted,
                    global_time: Some(GlobalTime(1)),
                    durability: DurabilityTier::Global,
                })],
                peer_complete_tx_payload_refs: Vec::new(),
                authorization_progress: None,
                opening_pending: false,
                result_member_adds: Vec::new(),
                result_member_removes: Vec::new(),
            },
            ViewUpdateParts {
                wire_rows: Some(fixture_scope_update(
                    subscription,
                    true,
                    todos_source_closure(tx_id, std::slice::from_ref(&second)),
                    Vec::new(),
                )),
                subscription,
                settled_through: GlobalTime(1),
                defer_settlement: false,
                reset_input_set: true,
                version_carriers: vec![VersionCarrier::Bundle(VersionBundle {
                    scope: crate::protocol::VersionBundleScope::ViewScoped,
                    tx: redacted_tx,
                    versions: vec![second.clone()],
                    fate: Fate::Accepted,
                    global_time: Some(GlobalTime(1)),
                    durability: DurabilityTier::Global,
                })],
                peer_complete_tx_payload_refs: Vec::new(),
                authorization_progress: None,
                opening_pending: false,
                result_member_adds: Vec::new(),
                result_member_removes: Vec::new(),
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
    let stored_tx = reader.query_transaction(tx_id).unwrap().unwrap();
    assert!(stored_tx.view_scoped_cardinality);
    assert_eq!(stored_tx.tx.n_total_writes, 2);
    for (title, expected_row) in [("one", row(1)), ("two", row(2))] {
        let shape = Query::from("todos")
            .filter(eq(col("title"), lit(title)))
            .validate(&schema())
            .unwrap();
        assert_eq!(
            reader
                .query_rows(
                    &shape,
                    &shape.bind(BTreeMap::new()).unwrap(),
                    DurabilityTier::Global,
                )
                .unwrap()
                .into_iter()
                .map(current_row_pair)
                .collect::<BTreeMap<_, _>>(),
            BTreeMap::from([(expected_row, title_cells(title))])
        );
    }
    let hidden = Query::from("todos")
        .filter(eq(col("title"), lit("not shipped")))
        .validate(&schema())
        .unwrap();
    assert!(
        reader
            .query_rows(
                &hidden,
                &hidden.bind(BTreeMap::new()).unwrap(),
                DurabilityTier::Global,
            )
            .unwrap()
            .is_empty(),
        "coalescing may expose only the exact authorized fragments in the frame"
    );
    assert_eq!(reader.sync_metrics().receiver_bulk_ingest_commits, 1);
    assert_eq!(reader.sync_metrics().receiver_bulk_bundle_ingests, 1);
    assert_eq!(reader.sync_metrics().receiver_per_bundle_ingests, 0);
}

/// Reordered and duplicate view-scoped fragments coalesce by exact version
/// identity without changing the authorized current projection.
///
/// core ──row 2, row 1, row 1 replay──► alice's relay
#[test]
fn receiver_batch_coalesces_reordered_and_duplicate_view_scoped_fragments() {
    let (_reader_dir, mut reader) = open_node_with_uuid(node(3));
    register_whole_table_receiver(&mut reader, "todos");
    let subscription = reader.whole_table_subscription_key("todos").unwrap();
    let tx_id = TxId::new(TxTime::from(10), node(1));
    let tx = Transaction {
        tx_id,
        kind: TxKind::Exclusive,
        n_total_writes: 1,
        made_by: AuthorSubject::system_at(tx_id.node),
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
    let update = |version: VersionRecord, _result_row| {
        let facts = todos_source_closure(tx_id, std::slice::from_ref(&version));
        ViewUpdateParts {
            wire_rows: Some(fixture_scope_update(subscription, true, facts, Vec::new())),
            subscription,
            settled_through: GlobalTime(1),
            defer_settlement: false,
            reset_input_set: true,
            version_carriers: vec![VersionCarrier::Bundle(VersionBundle {
                scope: crate::protocol::VersionBundleScope::ViewScoped,
                tx: tx.clone(),
                versions: vec![version],
                fate: Fate::Accepted,
                global_time: Some(GlobalTime(1)),
                durability: DurabilityTier::Global,
            })],
            peer_complete_tx_payload_refs: Vec::new(),
            authorization_progress: None,
            opening_pending: false,
            result_member_adds: Vec::new(),
            result_member_removes: Vec::new(),
        }
    };

    reader
        .apply_view_updates_in_batch(vec![
            update(second, row(2)),
            update(first.clone(), row(1)),
            update(first, row(1)),
        ])
        .unwrap();

    assert_eq!(reader.query_all_versions().unwrap().len(), 2);
    for (title, expected_row) in [("one", row(1)), ("two", row(2))] {
        let shape = Query::from("todos")
            .filter(eq(col("title"), lit(title)))
            .validate(&schema())
            .unwrap();
        assert_eq!(
            reader
                .query_rows(
                    &shape,
                    &shape.bind(BTreeMap::new()).unwrap(),
                    DurabilityTier::Global,
                )
                .unwrap()
                .into_iter()
                .map(current_row_pair)
                .collect::<BTreeMap<_, _>>(),
            BTreeMap::from([(expected_row, title_cells(title))])
        );
    }
    assert_eq!(reader.sync_metrics().receiver_bulk_ingest_commits, 1);
}

/// Conflicting transaction identity or row bytes in coalesced view-scoped
/// fragments reject the complete receiver frame before any row becomes visible.
///
/// mallory ──conflicting sibling fragments──✗──► alice's relay
#[test]
fn receiver_batch_rejects_conflicting_view_scoped_fragments_atomically() {
    let tx_id = TxId::new(TxTime::from(10), node(1));
    let base_tx = Transaction {
        tx_id,
        kind: TxKind::Exclusive,
        n_total_writes: 1,
        made_by: AuthorSubject::system_at(tx_id.node),
        permission_subject: None,
        base_snapshot: None,
        row_read_set: None,
        absent_read_set: None,
        predicate_read_set: None,
        user_metadata_json: None,
        contribution_merge: None,
    };
    let run = |identity_conflict: bool| {
        let (_reader_dir, mut reader) = open_node_with_uuid(node(3));
        register_whole_table_receiver(&mut reader, "todos");
        let subscription = reader.whole_table_subscription_key("todos").unwrap();
        let mut conflicting_tx = base_tx.clone();
        if identity_conflict {
            conflicting_tx.made_by = AuthorSubject::for_test_bytes([0x55; 16]);
        }
        let first = version_record(row(1), Vec::new(), title_cells("one"), None);
        let conflicting = if identity_conflict {
            version_record(row(2), Vec::new(), title_cells("two"), None)
        } else {
            version_record(row(1), Vec::new(), title_cells("changed"), None)
        };
        let update = |tx: Transaction, version: VersionRecord| ViewUpdateParts {
            wire_rows: Some(fixture_scope_update(
                subscription,
                true,
                Vec::new(),
                Vec::new(),
            )),
            subscription,
            settled_through: GlobalTime(1),
            defer_settlement: false,
            reset_input_set: true,
            version_carriers: vec![VersionCarrier::Bundle(VersionBundle {
                scope: crate::protocol::VersionBundleScope::ViewScoped,
                tx,
                versions: vec![version],
                fate: Fate::Accepted,
                global_time: Some(GlobalTime(1)),
                durability: DurabilityTier::Global,
            })],
            peer_complete_tx_payload_refs: Vec::new(),
            authorization_progress: None,
            opening_pending: false,
            result_member_adds: Vec::new(),
            result_member_removes: Vec::new(),
        };
        let result = reader.apply_view_updates_in_batch(vec![
            update(base_tx.clone(), first),
            update(conflicting_tx, conflicting),
        ]);
        assert!(matches!(result.resolve(), Err(Error::ConflictingCommitUnit(id)) if id == tx_id));
        assert!(reader.query_all_versions().unwrap().is_empty());
        assert!(
            reader
                .current_rows("todos", DurabilityTier::Global)
                .unwrap()
                .is_empty()
        );
    };

    run(false);
    run(true);
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
    register_whole_table_receiver(&mut reader, "todos");
    let row_uuid = row(1);
    let (tx_id, unit) = writer
        .commit_mergeable_unit_settled(MergeableCommit::new("todos", row_uuid, 10).cells(
            BTreeMap::from([
                (
                    "title".to_owned(),
                    Value::String("visible title".to_owned()),
                ),
                ("body".to_owned(), Value::String("visible body".to_owned())),
            ]),
        ))
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
        full.created_by(),
        full.created_at_ms(),
        full.updated_by(),
        full.updated_at_ms(),
        &[
            Some(Value::String("conflicting title".to_owned())),
            full.cell_at(1),
        ],
        full.deletion(),)
    .unwrap()
    .with_authored_columns(full.authored_columns().cloned());
    let subscription = reader.whole_table_subscription_key("todos").unwrap();
    reader
        .apply_view_update(todos_receiver_reset(subscription))
        .unwrap();
    let update = |version, fate, update_global_time, update_durability| {
        let mut facts = todos_source_closure(tx_id, std::slice::from_ref(&version));
        for input in &mut facts {
            input.physical_table = core
                .scope_physical_table(projection_schema.version_id(), "todos")
                .unwrap();
        }
        ViewUpdateParts {
            wire_rows: Some(fixture_scope_update(subscription, true, facts, Vec::new())),
            subscription,
            settled_through: global_time,
            defer_settlement: false,
            // Repeated body delivery is valid across fresh complete snapshots,
            // not as a duplicate live covered-input addition.
            reset_input_set: true,
            version_carriers: vec![VersionCarrier::Bundle(VersionBundle {
                scope: crate::protocol::VersionBundleScope::CompleteTransaction,
                tx: tx.clone(),
                versions: vec![version],
                fate,
                global_time: update_global_time,
                durability: update_durability,
            })],
            peer_complete_tx_payload_refs: Vec::new(),
            authorization_progress: None,
            opening_pending: false,
            result_member_adds: Vec::new(),
            result_member_removes: Vec::new(),
        }
    };

    assert!(matches!(
        reader.apply_view_updates_in_batch(vec![
            update(
                full.clone(),
                Fate::Accepted,
                Some(global_time),
                durability,
            ),
            update(
                conflicting.clone(),
                Fate::Accepted,
                Some(global_time),
                durability,
            ),
        ])
        .resolve(),
        Err(Error::ConflictingCommitUnit(conflicting_tx)) if conflicting_tx == tx_id
    ));

    reader
        .apply_view_updates_in_batch(vec![
            update(full.clone(), Fate::Accepted, Some(global_time), durability),
            update(full.clone(), Fate::Accepted, Some(global_time), durability),
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
                (
                    "title".to_owned(),
                    Value::String("visible title".to_owned())
                ),
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
            DurabilityTier::Global,
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
        register_whole_table_receiver(&mut reader, "todos");
        let subscription = reader.whole_table_subscription_key("todos").unwrap();
        let tx_id = TxId::new(TxTime::from(10), node(1));
        let tx = Transaction {
            tx_id,
            kind: TxKind::Mergeable,
            n_total_writes: 1,
            made_by: AuthorSubject::system_at(tx_id.node),
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
        let closure = todos_source_closure(tx_id, std::slice::from_ref(&version));
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
            wire_rows: Some(fixture_scope_update(
                subscription,
                true,
                closure,
                Vec::new(),
            )),
            subscription,
            settled_through: GlobalTime(1),
            defer_settlement: false,
            reset_input_set: true,
            version_carriers: crate::protocol::build_version_carriers_from_singletons(bundles)
                .unwrap(),
            peer_complete_tx_payload_refs: Vec::new(),
            authorization_progress: None,
            opening_pending: false,
            result_member_adds: Vec::new(),
            result_member_removes: Vec::new(),
        };
        match path {
            ResetConflictPath::Batch => reader.apply_view_updates_in_batch(vec![update]).unwrap(),
            ResetConflictPath::Single => reader.apply_view_update(update).unwrap(),
        }
        assert!(reader.query_transaction(tx_id).unwrap().is_some());
        assert_eq!(reader.query_versions_for_tx(tx_id).unwrap().len(), 1);

        let conflicting = version_record(row(1), Vec::new(), title_cells("one"), None);
        let replay = ViewUpdateParts {
            wire_rows: Some(fixture_scope_update(
                subscription,
                true,
                todos_source_closure(tx_id, std::slice::from_ref(&conflicting)),
                Vec::new(),
            )),
            subscription,
            settled_through: GlobalTime(1),
            defer_settlement: true,
            reset_input_set: true,
            version_carriers: vec![VersionCarrier::Bundle(VersionBundle {
                scope: crate::protocol::VersionBundleScope::CompleteTransaction,
                tx: tx.clone(),
                versions: vec![conflicting.clone()],
                fate: Fate::Accepted,
                global_time: Some(GlobalTime(1)),
                durability: DurabilityTier::Global,
            })],
            peer_complete_tx_payload_refs: Vec::new(),
            authorization_progress: None,
            opening_pending: false,
            result_member_adds: Vec::new(),
            result_member_removes: Vec::new(),
            // This case isolates conflicting transaction metadata.  An absent
            // covered-input removal is independently rejected by
            // `reset_conflicts_with_member_removals_are_atomic`.
        };
        let result = match path {
            ResetConflictPath::Batch => reader.apply_view_updates_in_batch(vec![replay]).resolve(),
            ResetConflictPath::Single => reader.apply_view_update(replay).resolve(),
        };
        assert!(
            matches!(
                result,
                Err(Error::ConflictingCommitUnit(conflicting)) if conflicting == tx_id
            ),
            "expected conflicting replay, got {result:?}"
        );
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

// These are intentionally receiver-level tests: complete versus view-scoped
// bundle cardinality is protocol state that is not observable through the
// public client API, while accepting it incorrectly can corrupt later replay.
#[test]
fn reset_scope_merge_is_order_independent_and_complete_dominates() {
    for (path, complete_first) in [ResetConflictPath::Batch, ResetConflictPath::Single]
        .into_iter()
        .flat_map(|path| [false, true].into_iter().map(move |order| (path, order)))
    {
        let (_reader_dir, mut reader) = open_node_with_uuid(node(3));
        register_whole_table_receiver(&mut reader, "todos");
        let subscription = reader.whole_table_subscription_key("todos").unwrap();
        let tx_id = TxId::new(TxTime::from(10), node(1));
        let complete_tx = reset_scope_tx(tx_id, 2);
        let mut view_tx = complete_tx.clone();
        view_tx.n_total_writes = 1;
        let first = version_record(row(1), Vec::new(), title_cells("one"), None);
        let second = version_record(row(2), Vec::new(), title_cells("two"), None);
        let complete = reset_scope_bundle(
            complete_tx,
            crate::protocol::VersionBundleScope::CompleteTransaction,
            vec![first.clone(), second],
        );
        let view = reset_scope_bundle(
            view_tx,
            crate::protocol::VersionBundleScope::ViewScoped,
            vec![first],
        );
        let bundles = if complete_first {
            vec![complete, view]
        } else {
            vec![view, complete]
        };
        let update = reset_scope_update(subscription, bundles);
        match path {
            ResetConflictPath::Batch => reader.apply_view_updates_in_batch(vec![update]).unwrap(),
            ResetConflictPath::Single => reader.apply_view_update(update).unwrap(),
        }
        let stored = reader.query_transaction(tx_id).unwrap().unwrap();
        assert!(!stored.view_scoped_cardinality);
        assert_eq!(stored.tx.n_total_writes, 2);
        assert_eq!(reader.query_versions_for_tx(tx_id).unwrap().len(), 2);
    }
}

#[test]
fn reset_view_scoped_fragments_union_and_recompute_visible_cardinality() {
    for path in [ResetConflictPath::Batch, ResetConflictPath::Single] {
        let (_reader_dir, mut reader) = open_node_with_uuid(node(3));
        register_whole_table_receiver(&mut reader, "todos");
        let subscription = reader.whole_table_subscription_key("todos").unwrap();
        let tx_id = TxId::new(TxTime::from(10), node(1));
        let view_tx = reset_scope_tx(tx_id, 1);
        let bundles = [
            version_record(row(1), Vec::new(), title_cells("one"), None),
            version_record(row(2), Vec::new(), title_cells("two"), None),
        ]
        .into_iter()
        .map(|version| {
            reset_scope_bundle(
                view_tx.clone(),
                crate::protocol::VersionBundleScope::ViewScoped,
                vec![version],
            )
        })
        .collect();
        let update = reset_scope_update(subscription, bundles);
        match path {
            ResetConflictPath::Batch => reader.apply_view_updates_in_batch(vec![update]).unwrap(),
            ResetConflictPath::Single => reader.apply_view_update(update).unwrap(),
        }
        let stored = reader.query_transaction(tx_id).unwrap().unwrap();
        assert!(stored.view_scoped_cardinality);
        assert_eq!(stored.tx.n_total_writes, 2);
        assert_eq!(reader.query_versions_for_tx(tx_id).unwrap().len(), 2);
    }
}

#[test]
fn reset_rejects_divergent_complete_sets_and_bad_counts_atomically() {
    for bad_count in [false, true] {
        let (_reader_dir, mut reader) = open_node_with_uuid(node(3));
        register_whole_table_receiver(&mut reader, "todos");
        let subscription = reader.whole_table_subscription_key("todos").unwrap();
        let tx_id = TxId::new(TxTime::from(10), node(1));
        let tx = reset_scope_tx(tx_id, 2);
        let first = version_record(row(1), Vec::new(), title_cells("one"), None);
        let bundles = if bad_count {
            vec![reset_scope_bundle(
                tx,
                crate::protocol::VersionBundleScope::CompleteTransaction,
                vec![first],
            )]
        } else {
            vec![
                reset_scope_bundle(
                    tx.clone(),
                    crate::protocol::VersionBundleScope::CompleteTransaction,
                    vec![
                        first.clone(),
                        version_record(row(2), Vec::new(), title_cells("two"), None),
                    ],
                ),
                reset_scope_bundle(
                    tx,
                    crate::protocol::VersionBundleScope::CompleteTransaction,
                    vec![
                        first,
                        version_record(row(3), Vec::new(), title_cells("three"), None),
                    ],
                ),
            ]
        };
        let authority_result_key = reader
            .authority_result_key_for_subscription(subscription)
            .unwrap();
        let state_before = authority_hydration_receipts(&reader).0;
        let result = reader
            .apply_view_updates_in_batch(vec![reset_scope_update(subscription, bundles)])
            .resolve();
        assert!(matches!(
            result,
            Err(Error::ConflictingCommitUnit(conflicting)) if conflicting == tx_id
        ));
        assert!(reader.query_transaction(tx_id).unwrap().is_none());
        assert!(reader.query_versions_for_tx(tx_id).unwrap().is_empty());
        assert_eq!(authority_hydration_receipts(&reader).0, state_before);
        assert!(
            !authority_hydration_receipts(&reader)
                .0
                .contains(&authority_result_key)
        );
    }
}

#[test]
fn reset_rejects_view_payload_outside_complete_set_in_both_orders() {
    for (path, complete_first) in [ResetConflictPath::Batch, ResetConflictPath::Single]
        .into_iter()
        .flat_map(|path| [false, true].into_iter().map(move |order| (path, order)))
    {
        let (_reader_dir, mut reader) = open_node_with_uuid(node(3));
        register_whole_table_receiver(&mut reader, "todos");
        let subscription = reader.whole_table_subscription_key("todos").unwrap();
        let tx_id = TxId::new(TxTime::from(10), node(1));
        let complete = reset_scope_bundle(
            reset_scope_tx(tx_id, 2),
            crate::protocol::VersionBundleScope::CompleteTransaction,
            vec![
                version_record(row(1), Vec::new(), title_cells("one"), None),
                version_record(row(2), Vec::new(), title_cells("two"), None),
            ],
        );
        let view = reset_scope_bundle(
            reset_scope_tx(tx_id, 1),
            crate::protocol::VersionBundleScope::ViewScoped,
            vec![version_record(
                row(3),
                Vec::new(),
                title_cells("outside"),
                None,
            )],
        );
        let bundles = if complete_first {
            vec![complete, view]
        } else {
            vec![view, complete]
        };
        let update = reset_scope_update(subscription, bundles);
        let result = match path {
            ResetConflictPath::Batch => reader.apply_view_updates_in_batch(vec![update]).resolve(),
            ResetConflictPath::Single => reader.apply_view_update(update).resolve(),
        };
        assert!(matches!(
            result,
            Err(Error::ConflictingCommitUnit(conflicting)) if conflicting == tx_id
        ));
        assert!(reader.query_transaction(tx_id).unwrap().is_none());
        assert!(reader.query_versions_for_tx(tx_id).unwrap().is_empty());
    }
}

#[test]
fn reopened_scope_conflicts_preserve_persisted_transaction() {
    for stored_complete in [false, true] {
        let (reader_dir, mut reader) = open_node_with_uuid(node(3));
        register_whole_table_receiver(&mut reader, "todos");
        let subscription = reader.whole_table_subscription_key("todos").unwrap();
        let tx_id = TxId::new(TxTime::from(10), node(1));
        let first = version_record(row(1), Vec::new(), title_cells("one"), None);
        let stored_scope = if stored_complete {
            crate::protocol::VersionBundleScope::CompleteTransaction
        } else {
            crate::protocol::VersionBundleScope::ViewScoped
        };
        reader
            .apply_view_update(reset_scope_update(
                subscription,
                vec![reset_scope_bundle(
                    reset_scope_tx(tx_id, 1),
                    stored_scope,
                    vec![first.clone()],
                )],
            ))
            .unwrap();
        drop(reader);

        let mut reader = reopen_node_at(&reader_dir, node(3), schema());
        register_whole_table_receiver(&mut reader, "todos");
        let conflicting_scope = if stored_complete {
            crate::protocol::VersionBundleScope::ViewScoped
        } else {
            crate::protocol::VersionBundleScope::CompleteTransaction
        };
        // This reset conflict has no logical input removal. The old fixture's
        // unrelated row-9 removal was deliberately absent after reopen and
        // masked the persisted-transaction scope conflict under test.
        let conflicting_update = reset_scope_update(
            subscription,
            vec![reset_scope_bundle(
                reset_scope_tx(tx_id, 1),
                conflicting_scope,
                vec![version_record(
                    row(2),
                    Vec::new(),
                    title_cells("conflicting"),
                    None,
                )],
            )],
        );
        let result = reader.apply_view_update(conflicting_update).resolve();
        assert!(matches!(
            result,
            Err(Error::ConflictingCommitUnit(conflicting)) if conflicting == tx_id
        ));
        let stored = reader.query_transaction(tx_id).unwrap().unwrap();
        assert_eq!(stored.view_scoped_cardinality, !stored_complete);
        let versions = reader.query_versions_for_tx(tx_id).unwrap();
        assert_eq!(versions.len(), 1);
        assert_eq!(reader.version_record_from_row(&versions[0]).unwrap(), first);
    }
}

fn reset_scope_tx(tx_id: TxId, n_total_writes: u32) -> Transaction {
    Transaction {
        tx_id,
        kind: TxKind::Mergeable,
        n_total_writes,
        made_by: AuthorSubject::system_at(tx_id.node),
        permission_subject: None,
        base_snapshot: None,
        row_read_set: None,
        absent_read_set: None,
        predicate_read_set: None,
        user_metadata_json: None,
        contribution_merge: None,
    }
}

fn reset_scope_bundle(
    tx: Transaction,
    scope: crate::protocol::VersionBundleScope,
    versions: Vec<VersionRecord>,
) -> VersionBundle {
    VersionBundle {
        scope,
        tx,
        versions,
        fate: Fate::Accepted,
        global_time: Some(GlobalTime(1)),
        durability: DurabilityTier::Global,
    }
}

fn reset_scope_update(
    subscription: SubscriptionKey,
    bundles: Vec<VersionBundle>,
) -> ViewUpdateParts {
    let program_fact_adds = bundles
        .iter()
        .flat_map(|bundle| {
            bundle
                .versions
                .iter()
                .map(|version| todos_covered_input(bundle.tx.tx_id, version))
        })
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    ViewUpdateParts {
        wire_rows: Some(fixture_scope_update(
            subscription,
            true,
            program_fact_adds,
            Vec::new(),
        )),
        subscription,
        settled_through: GlobalTime(1),
        defer_settlement: true,
        reset_input_set: true,
        version_carriers: crate::protocol::build_version_carriers_from_singletons(bundles).unwrap(),
        peer_complete_tx_payload_refs: Vec::new(),
        authorization_progress: None,
        opening_pending: false,
        result_member_adds: Vec::new(),
        result_member_removes: Vec::new(),
        // Reset supplies the entire closure; impossible-removal rejection has
        // a separate control in reset_conflicts_with_member_removals_are_atomic.
    }
}

fn assert_reset_authored_columns_conflict(
    path: ResetConflictPath,
    reversed: bool,
    with_member_removal: bool,
) {
    let (_reader_dir, mut reader) = open_node_with_uuid(node(3));
    register_whole_table_receiver(&mut reader, "todos");
    let subscription = reader.whole_table_subscription_key("todos").unwrap();
    let tx_id = TxId::new(TxTime::from(10), node(1));
    let tx = Transaction {
        tx_id,
        kind: TxKind::Mergeable,
        n_total_writes: 1,
        made_by: AuthorSubject::system_at(tx_id.node),
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
    // Keep the impossible-removal control independent from duplicate metadata:
    // it needs one otherwise-valid source member so admission reaches the
    // predecessor-removal check first.
    let source_versions = if with_member_removal {
        &versions[..1]
    } else {
        &versions[..]
    };
    let source_closure = todos_source_closure(tx_id, source_versions);
    let version_bundles = versions
        .into_iter()
        .take(if with_member_removal { 1 } else { 2 })
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

    if with_member_removal {
        reader
            .apply_view_update(todos_receiver_reset(subscription))
            .unwrap();
    }
    reader.set_initial_sync_flush_cadence(2).unwrap();
    let cadence_before = (
        reader.initial_sync_flush_active,
        reader.initial_sync_flush_completed,
    );
    let hydration_before = authority_hydration_receipts(&reader).0;
    let deferred_before = authority_hydration_receipts(&reader).1;

    let update = ViewUpdateParts {
        wire_rows: Some(fixture_scope_update(
            subscription,
            true,
            source_closure,
            with_member_removal
                .then_some(removed_todos_input(tx_id))
                .into_iter()
                .collect(),
        )),
        subscription,
        settled_through: GlobalTime(1),
        defer_settlement: true,
        reset_input_set: true,
        version_carriers,
        peer_complete_tx_payload_refs: Vec::new(),
        authorization_progress: None,
        opening_pending: false,
        result_member_adds: Vec::new(),
        result_member_removes: Vec::new(),
    };
    let result = match path {
        ResetConflictPath::Batch => reader.apply_view_updates_in_batch(vec![update]).resolve(),
        ResetConflictPath::Single => reader.apply_view_update(update).resolve(),
    };

    if with_member_removal {
        assert!(
            matches!(
                result,
                Err(Error::InvalidAuthoritySourceClosure { ref transition, .. })
                    if transition.contains("scope removal is absent from exact predecessor")
            ),
            "{path:?} reset must reject an absent covered-input removal (reversed: {reversed}): {result:?}"
        );
    } else {
        assert!(
            matches!(
                result,
                Err(Error::ConflictingCommitUnit(conflicting_tx)) if conflicting_tx == tx_id
            ),
            "{path:?} reset must reject conflicting authored columns (reversed: {reversed})"
        );
    }
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
    assert_eq!(authority_hydration_receipts(&reader).0, hydration_before);
    assert_eq!(authority_hydration_receipts(&reader).1, deferred_before);
}

// This stays internal because it directly exercises the protocol receiver's
// single-message fragment assembly boundary.
#[test]
fn sequential_partial_exclusive_bundles_index_the_complete_transaction() {
    let (_reader_dir, mut reader) = open_node_with_uuid(node(3));
    register_whole_table_receiver(&mut reader, "todos");
    let subscription = reader.whole_table_subscription_key("todos").unwrap();
    let tx_id = TxId::new(TxTime::from(10), node(1));
    let tx = Transaction {
        tx_id,
        kind: TxKind::Exclusive,
        n_total_writes: 2,
        made_by: AuthorSubject::system_at(tx_id.node),
        permission_subject: None,
        base_snapshot: None,
        row_read_set: None,
        absent_read_set: None,
        predicate_read_set: None,
        user_metadata_json: None,
        contribution_merge: None,
    };
    let updates = [
        (
            row(1),
            version_record(row(1), Vec::new(), title_cells("one"), None),
        ),
        (
            row(2),
            version_record(row(2), Vec::new(), title_cells("two"), None),
        ),
    ];

    reader
        .apply_view_update(todos_receiver_reset(subscription))
        .unwrap();

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
    register_whole_table_receiver(&mut reader, "todos");
    let subscription = reader.whole_table_subscription_key("todos").unwrap();
    let tx_id = TxId::new(TxTime::from(10), node(1));
    let tx = Transaction {
        tx_id,
        kind: TxKind::Exclusive,
        n_total_writes: 2,
        made_by: AuthorSubject::system_at(tx_id.node),
        permission_subject: None,
        base_snapshot: None,
        row_read_set: None,
        absent_read_set: None,
        predicate_read_set: None,
        user_metadata_json: None,
        contribution_merge: None,
    };
    reader
        .apply_view_update(todos_receiver_reset(subscription))
        .unwrap();
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
        AuthorSubject::system_at(tx_id.node),
        "the original transaction metadata must remain authoritative"
    );
}

fn partial_exclusive_view_update(
    subscription: SubscriptionKey,
    tx: Transaction,
    _row_uuid: RowUuid,
    version: VersionRecord,
) -> ViewUpdateParts {
    let tx_id = tx.tx_id;
    // Callers install the whole-table source coverage in their reset. These
    // subsequent partial fragments are live transitions and carry only their
    // exact row fact.
    let source_closure = vec![todos_covered_input(tx_id, &version)];
    let mut tx = tx;
    tx.n_total_writes = 1;
    ViewUpdateParts {
        wire_rows: Some(fixture_scope_update(
            subscription,
            false,
            source_closure,
            Vec::new(),
        )),
        subscription,
        settled_through: GlobalTime(1),
        defer_settlement: false,
        reset_input_set: false,
        version_carriers: vec![VersionCarrier::Bundle(VersionBundle {
            scope: crate::protocol::VersionBundleScope::ViewScoped,
            tx,
            versions: vec![version],
            fate: Fate::Accepted,
            global_time: Some(GlobalTime(1)),
            durability: DurabilityTier::Global,
        })],
        peer_complete_tx_payload_refs: Vec::new(),
        authorization_progress: None,
        opening_pending: false,
        result_member_adds: Vec::new(),
        result_member_removes: Vec::new(),
    }
}

#[test]
fn receiver_batch_resolves_current_winner_across_bundles() {
    let (_writer_dir, mut writer) = open_node_with_uuid(node(1));
    let (_core_dir, mut core) = open_node_with_uuid(node(2));
    let (_reader_dir, mut reader) = open_node_with_uuid(node(3));
    register_whole_table_receiver(&mut reader, "todos");
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

    let (_new_tx, new_unit) = writer
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
        .apply_view_update(todos_receiver_reset(subscription))
        .unwrap();
    // Both history bodies arrive below, but current-state coverage names only
    // the winner. The predecessor reset already supplied the source manifest.
    let source_closure = new_versions
        .iter()
        .map(|version| todos_covered_input(new.tx_id, version))
        .collect();

    reader
        .apply_view_updates_in_batch(vec![ViewUpdateParts {
            wire_rows: Some(fixture_scope_update(
                subscription,
                false,
                source_closure,
                Vec::new(),
            )),
            subscription,
            settled_through: new_seq,
            defer_settlement: false,
            reset_input_set: false,
            version_carriers: crate::protocol::build_version_carriers_from_singletons(vec![
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
            ])
            .unwrap(),
            peer_complete_tx_payload_refs: Vec::new(),
            authorization_progress: None,
            opening_pending: false,
            result_member_adds: Vec::new(),
            result_member_removes: Vec::new(),
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
    register_whole_table_receiver(&mut reader, "todos");
    let subscription = reader.whole_table_subscription_key("todos").unwrap();
    let tx_id = TxId::new(TxTime::from(10), node(1));
    let tx = Transaction {
        tx_id,
        kind: TxKind::Mergeable,
        n_total_writes: 2,
        made_by: AuthorSubject::system_at(tx_id.node),
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
    let first_closure = vec![todos_covered_input(tx_id, &first)];
    let second_closure = vec![
        todos_covered_input(tx_id, &first),
        todos_covered_input(tx_id, &second),
    ];
    reader
        .apply_view_update(todos_receiver_reset(subscription))
        .unwrap();

    reader
        .apply_sync_message_settled(SyncMessage::ViewUpdate(
            crate::protocol::ViewUpdatePayload {
                subscription,
                settled_through: GlobalTime(0),

                version_carriers: vec![VersionCarrier::Bundle(VersionBundle {
                    scope: crate::protocol::VersionBundleScope::ViewScoped,
                    tx: redacted_tx.clone(),
                    versions: vec![first],
                    fate: Fate::Accepted,
                    global_time: Some(GlobalTime(1)),
                    durability: DurabilityTier::Global,
                })],
                peer_payload_inventory: crate::protocol::PeerPayloadInventory::default(),
                supporting_rows: crate::protocol::SupportingRowsUpdate::snapshot(first_closure),
            },
        ))
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
        .apply_sync_message_settled(SyncMessage::ViewUpdate(
            crate::protocol::ViewUpdatePayload {
                subscription,
                settled_through: GlobalTime(0),

                version_carriers: vec![VersionCarrier::Bundle(VersionBundle {
                    scope: crate::protocol::VersionBundleScope::ViewScoped,
                    tx: redacted_tx,
                    versions: vec![second],
                    fate: Fate::Accepted,
                    global_time: Some(GlobalTime(1)),
                    durability: DurabilityTier::Global,
                })],
                peer_payload_inventory: crate::protocol::PeerPayloadInventory::default(),
                supporting_rows: crate::protocol::SupportingRowsUpdate::snapshot(second_closure),
            },
        ))
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
    register_whole_table_receiver(&mut reader, "todos");
    let subscription = reader.whole_table_subscription_key("todos").unwrap();
    let tx_id = TxId::new(TxTime::from(10), node(1));
    let tx = Transaction {
        tx_id,
        kind: TxKind::Mergeable,
        n_total_writes: 2,
        made_by: AuthorSubject::system_at(tx_id.node),
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
    let first_closure = vec![todos_covered_input(tx_id, &first)];
    let complete_closure = vec![
        todos_covered_input(tx_id, &first),
        todos_covered_input(tx_id, &second),
    ];
    reader
        .apply_view_update(todos_receiver_reset(subscription))
        .unwrap();
    reader
        .apply_sync_message_settled(SyncMessage::ViewUpdate(
            crate::protocol::ViewUpdatePayload {
                subscription,
                settled_through: GlobalTime(1),

                version_carriers: vec![VersionCarrier::Bundle(VersionBundle {
                    scope: crate::protocol::VersionBundleScope::ViewScoped,
                    tx: redacted_tx,
                    versions: vec![first.clone()],
                    fate: Fate::Accepted,
                    global_time: Some(GlobalTime(1)),
                    durability: DurabilityTier::Global,
                })],
                peer_payload_inventory: crate::protocol::PeerPayloadInventory::default(),
                supporting_rows: crate::protocol::SupportingRowsUpdate::snapshot(first_closure),
            },
        ))
        .unwrap();
    assert!(
        reader
            .query_transaction(tx_id)
            .unwrap()
            .unwrap()
            .view_scoped_cardinality
    );

    drop(reader);
    let mut reader = reopen_node_at(&reader_dir, node(3), schema());
    assert!(
        reader
            .query_transaction(tx_id)
            .unwrap()
            .unwrap()
            .view_scoped_cardinality
    );
    register_whole_table_receiver(&mut reader, "todos");
    reader
        .apply_view_update(todos_receiver_reset(subscription))
        .unwrap();
    reader
        .apply_sync_message_settled(SyncMessage::ViewUpdate(
            crate::protocol::ViewUpdatePayload {
                subscription,
                settled_through: GlobalTime(1),

                version_carriers: vec![VersionCarrier::Bundle(VersionBundle {
                    scope: crate::protocol::VersionBundleScope::CompleteTransaction,
                    tx,
                    versions: vec![first, second],
                    fate: Fate::Accepted,
                    global_time: Some(GlobalTime(1)),
                    durability: DurabilityTier::Global,
                })],
                peer_payload_inventory: crate::protocol::PeerPayloadInventory::default(),
                supporting_rows: crate::protocol::SupportingRowsUpdate::snapshot(complete_closure),
            },
        ))
        .unwrap();
    let stored = reader.query_transaction(tx_id).unwrap().unwrap();
    assert_eq!(stored.tx.n_total_writes, 2);
    assert!(!stored.view_scoped_cardinality);
}

#[test]
fn receiver_batch_prepares_all_author_aliases_before_exact_ingestion() {
    let (_writer_dir, mut writer) = open_node_with_uuid(node(1));
    let (_core_dir, mut core) = open_node_with_uuid(node(2));
    let (_reader_dir, mut reader) = open_node_with_uuid(node(3));
    let (_second_dir, mut second_writer) = open_node_with_uuid(node(4));
    register_whole_table_receiver(&mut reader, "todos");

    commit_mergeable_global(
        &mut writer,
        &mut core,
        MergeableCommit::new("todos", row(1), 10).cells(title_cells("one")),
    );
    commit_mergeable_global(
        &mut second_writer,
        &mut core,
        MergeableCommit::new("todos", row(2), 11).cells(title_cells("two")),
    );

    let update = core.view_update_for_current_rows("todos").unwrap();
    let mut version_bundles = version_bundles_for_update(&update);
    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        subscription,
        settled_through,
        peer_payload_inventory,
        supporting_rows: program_fact_adds,
        ..
    }) = update
    else {
        panic!("expected view update");
    };
    assert_eq!(version_bundles.len(), 2);
    for bundle in &mut version_bundles {
        bundle.scope = crate::protocol::VersionBundleScope::ViewScoped;
    }
    version_bundles.reverse();
    // Both entries are complete sets; exact native payloads ingest only once.

    reader
        .apply_view_updates_in_batch(vec![
            todos_receiver_reset(subscription),
            ViewUpdateParts {
                wire_rows: Some(program_fact_adds),
                subscription,
                settled_through,
                defer_settlement: false,
                reset_input_set: true,
                version_carriers: crate::protocol::build_version_carriers_from_singletons(
                    version_bundles,
                )
                .unwrap(),
                peer_complete_tx_payload_refs: peer_payload_inventory.complete_tx_payloads,
                authorization_progress: None,
                opening_pending: false,
                result_member_adds: Vec::new(),
                result_member_removes: Vec::new(),
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
    assert_eq!(reader.sync_metrics().receiver_bulk_ingest_commits, 1);
    assert_eq!(reader.sync_metrics().receiver_bulk_bundle_ingests, 2);
    assert_eq!(reader.sync_metrics().receiver_per_bundle_ingests, 0);
}

#[test]
fn receiver_batch_defers_known_transaction_publication_until_new_rows_commit() {
    let (_writer_dir, mut writer) = open_node_with_uuid(node(1));
    let (_core_dir, mut core) = open_node_with_uuid(node(2));
    let (_reader_dir, mut reader) = open_node_with_uuid(node(3));
    let (_second_dir, mut second_writer) = open_node_with_uuid(node(4));
    register_whole_table_receiver(&mut reader, "todos");

    commit_mergeable_global(
        &mut writer,
        &mut core,
        MergeableCommit::new("todos", row(1), 10).cells(title_cells("one")),
    );
    commit_mergeable_global(
        &mut second_writer,
        &mut core,
        MergeableCommit::new("todos", row(2), 11).cells(title_cells("two")),
    );

    let update = core.view_update_for_current_rows("todos").unwrap();
    let mut version_bundles = version_bundles_for_update(&update);
    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        subscription,
        settled_through,
        peer_payload_inventory,
        supporting_rows: program_fact_adds,
        ..
    }) = update
    else {
        panic!("expected view update");
    };
    assert_eq!(version_bundles.len(), 2);
    for bundle in &mut version_bundles {
        bundle.global_time = None;
        bundle.durability = DurabilityTier::Local;
    }
    let known = version_bundles
        .iter()
        .find(|bundle| bundle.tx.tx_id.node == node(4))
        .unwrap();
    let known_tx_id = known.tx.tx_id;
    reader
        .ingest_known_transaction(
            known.tx.clone(),
            known.versions.clone(),
            Fate::Pending,
            known.global_time,
            known.durability,
        )
        .unwrap();
    version_bundles.reverse();
    // Both entries are complete sets; exact native payloads ingest only once.

    reader
        .apply_view_updates_in_batch(vec![
            todos_receiver_reset(subscription),
            ViewUpdateParts {
                wire_rows: Some(program_fact_adds),
                subscription,
                settled_through,
                defer_settlement: false,
                reset_input_set: false,
                version_carriers: crate::protocol::build_version_carriers_from_singletons(
                    version_bundles,
                )
                .unwrap(),
                peer_complete_tx_payload_refs: peer_payload_inventory.complete_tx_payloads,
                authorization_progress: None,
                opening_pending: false,
                result_member_adds: Vec::new(),
                result_member_removes: Vec::new(),
            },
        ])
        .unwrap();

    assert_eq!(
        reader.query_transaction(known_tx_id).unwrap().unwrap().fate,
        Fate::Accepted
    );
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
    assert_eq!(reader.sync_metrics().receiver_bulk_bundle_ingests, 1);
}

#[test]
fn discarded_pending_identity_survives_reopen_and_later_pending_carrier() {
    use crate::protocol::VersionBundleScope::{CompleteTransaction, ViewScoped};
    for scope in [CompleteTransaction, ViewScoped] {
        let (reader_dir, mut reader) = open_node_with_uuid(node(3));
        let tx_id = TxId::new(TxTime::from(10), node(1));
        let tx = reset_scope_tx(tx_id, 2);
        let versions = vec![
            version_record(row(1), Vec::new(), title_cells("one"), None),
            version_record(row(2), Vec::new(), title_cells("two"), None),
        ];
        let carrier = VersionCarrier::Bundle(VersionBundle {
            scope, tx: tx.clone(), versions: versions.clone(),
            fate: Fate::Pending, global_time: None, durability: DurabilityTier::Local,
        });
        reader.remember_discarded_pending_view_transactions(&[carrier.clone()]).unwrap();
        let mut conflicting = carrier.clone();
        let VersionCarrier::Bundle(conflicting_bundle) = &mut conflicting else { unreachable!() };
        conflicting_bundle.tx.made_by = AuthorSubject::system_at(node(9));
        assert!(matches!(
            crate::db::block_on(reader.remember_discarded_pending_view_transactions(&[conflicting])),
            Err(Error::ConflictingCommitUnit(id)) if id == tx_id
        ));
        let stored = reader.query_transaction(tx_id).unwrap().unwrap();
        assert_eq!(stored.tx.n_total_writes, 0);
        assert!(stored.view_scoped_cardinality);
        assert!(reader.query_versions_for_tx(tx_id).unwrap().is_empty());
        assert!(reader.query_local_winner("todos", row(1)).unwrap().is_none());
        reader.apply_fate_update(tx_id, Fate::Accepted, Some(GlobalTime(1)), Some(DurabilityTier::Global)).unwrap();
        // A duplicate discarded Pending header must leave a terminal identity intact.
        reader.remember_discarded_pending_view_transactions(&[carrier]).unwrap();
        let terminal = reader.query_transaction(tx_id).unwrap().unwrap();
        assert_eq!(terminal.fate, Fate::Accepted);
        assert_eq!(terminal.global_time, Some(GlobalTime(1)));
        assert_eq!(terminal.durability, DurabilityTier::Global);
        drop(reader);
        let mut reader = reopen_node_at(&reader_dir, node(3), schema());
        let stored = reader.query_transaction(tx_id).unwrap().unwrap();
        assert!(stored.view_scoped_cardinality);
        assert_eq!(stored.tx.n_total_writes, 0);
        assert_eq!(stored.fate, Fate::Accepted);
        assert!(reader.query_versions_for_tx(tx_id).unwrap().is_empty());
        assert!(reader.query_local_winner("todos", row(1)).unwrap().is_none());
        register_whole_table_receiver(&mut reader, "todos");
        let subscription = reader.whole_table_subscription_key("todos").unwrap();
        let bundle = VersionBundle { scope, tx, versions, fate: Fate::Pending,
            global_time: None, durability: DurabilityTier::Local };
        if scope == ViewScoped {
            let mut first_fragment = bundle.clone();
            first_fragment.versions.truncate(1);
            first_fragment.tx.n_total_writes = 1;
            reader.apply_view_updates_in_batch(vec![reset_scope_update(subscription, vec![first_fragment])]).unwrap();
            let partial = reader.query_transaction(tx_id).unwrap().unwrap();
            assert_eq!(partial.fate, Fate::Accepted, "first fragment preserves terminal fate");
            assert_eq!(partial.tx.n_total_writes, 1);
            assert!(partial.view_scoped_cardinality);
        }
        reader.apply_view_updates_in_batch(vec![reset_scope_update(subscription, vec![bundle])]).unwrap();
        let stored = reader.query_transaction(tx_id).unwrap().unwrap();
        assert_eq!(stored.tx.n_total_writes, 2);
        assert_eq!(stored.view_scoped_cardinality, scope == ViewScoped);
        assert_eq!(reader.query_versions_for_tx(tx_id).unwrap().len(), 2);
        assert_eq!(stored.fate, Fate::Accepted, "later pending carrier must preserve terminal identity ({scope:?})");
        assert_eq!(stored.global_time, Some(GlobalTime(1)));
        assert_eq!(stored.durability, DurabilityTier::Global);
        drop(reader);
        let mut reader = reopen_node_at(&reader_dir, node(3), schema());
        let stored = reader.query_transaction(tx_id).unwrap().unwrap();
        assert_eq!(stored.fate, Fate::Accepted);
        assert_eq!(stored.global_time, Some(GlobalTime(1)));
        assert_eq!(stored.durability, DurabilityTier::Global);
        assert_eq!(stored.tx.n_total_writes, 2);
        assert_eq!(reader.query_versions_for_tx(tx_id).unwrap().len(), 2);
    }
}

#[test]
fn discarded_pending_identity_accepts_redacted_exclusive_read_sets() {
    let (_dir, mut reader) = open_node_with_uuid(node(3));
    let tx_id = TxId::new(TxTime::from(11), node(1));
    let mut tx = reset_scope_tx(tx_id, 1);
    tx.kind = TxKind::Exclusive;
    tx.row_read_set = Some(Vec::new());
    tx.absent_read_set = Some(Vec::new());
    tx.predicate_read_set = Some(Vec::new());
    let full = VersionCarrier::Bundle(VersionBundle {
        scope: crate::protocol::VersionBundleScope::ViewScoped,
        tx: tx.clone(), versions: Vec::new(), fate: Fate::Pending,
        global_time: None, durability: DurabilityTier::Local,
    });
    tx.row_read_set = None;
    tx.absent_read_set = None;
    tx.predicate_read_set = None;
    let redacted = VersionCarrier::Bundle(VersionBundle {
        scope: crate::protocol::VersionBundleScope::ViewScoped,
        tx, versions: Vec::new(), fate: Fate::Pending,
        global_time: None, durability: DurabilityTier::Local,
    });
    // Both duplicate entries in a delivery and later redacted deliveries use
    // the ordinary transaction-identity contract.
    reader.remember_discarded_pending_view_transactions(&[full.clone(), redacted.clone()]).unwrap();
    reader.remember_discarded_pending_view_transactions(&[full]).unwrap();
    reader.remember_discarded_pending_view_transactions(&[redacted.clone()]).unwrap();
    let mut conflict = redacted;
    let VersionCarrier::Bundle(bundle) = &mut conflict else { unreachable!() };
    bundle.tx.user_metadata_json = Some("true".to_owned());
    assert!(matches!(crate::db::block_on(reader.remember_discarded_pending_view_transactions(&[conflict])), Err(Error::ConflictingCommitUnit(id)) if id == tx_id));
}

// Internal work-count receipt: transaction fate handling may read the full
// unit once; exact row matching must not add another transaction-wide read.
#[test]
fn known_transaction_matching_probes_only_incoming_history_keys() {
    let schema = two_column_schema();
    let (_dir, mut writer) = open_node_with_schema(node(0xf1), schema);
    let tx_id = writer.commit_mergeable_many_settled((0..32).map(|i| {
        MergeableCommit::new("todos", row(i + 1), 10)
            .cells(BTreeMap::from([("title".to_owned(), "same".to_owned())]))
    }).collect()).unwrap();
    let SyncMessage::CommitUnit { tx, versions } = writer.commit_unit_for(tx_id).unwrap() else { panic!("commit unit"); };
    let state = writer.query_transaction(tx_id).unwrap().unwrap();
    writer.query.tx_versions_cache.clear();
    reset_query_versions_for_tx_call_count();
    writer.ingest_known_transaction(tx, versions, state.fate.clone(), state.global_time, state.durability).unwrap();
    assert_eq!(query_versions_for_tx_call_count(), 1, "only fate processing needs a whole-transaction read");
    assert_eq!(writer.query_versions_for_tx(tx_id).unwrap().len(), 32);
}
