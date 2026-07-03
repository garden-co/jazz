#[test]
fn opening_existing_storage_recovers_mirrors_and_high_water_marks() {
    let schema = schema();
    let temp_dir = tempfile::tempdir().unwrap();
    let first_tx;
    {
        let mut node = open_node_at(&temp_dir, schema.clone());
        first_tx = node
            .commit_mergeable(
                MergeableCommit::new("todos", row(9), 10).cells(BTreeMap::from([(
                    "title".to_owned(),
                    "persisted".to_owned(),
                )])),
            )
            .unwrap();
    }

    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = RocksDbStorage::open(temp_dir.path(), &refs).unwrap();
    let mut reopened = NodeState::new(node(1), schema, storage).unwrap();

    assert_eq!(
        reopened
            .current_rows("todos", DurabilityTier::Local)
            .unwrap()
            .into_iter()
            .map(current_row_pair)
            .collect::<BTreeMap<_, _>>(),
        BTreeMap::from([(row(9), title_cells("persisted"))])
    );
    assert_eq!(
        reopened.transaction_state(first_tx).unwrap(),
        (Fate::Pending, None, DurabilityTier::Local)
    );
    let next_tx = reopened
        .commit_mergeable(
            MergeableCommit::new("todos", row(10), 11).cells(BTreeMap::from([(
                "title".to_owned(),
                "after restart".to_owned(),
            )])),
        )
        .unwrap();
    assert_eq!(next_tx.time, TxTime::from(11));
}
#[test]
fn recovery_rebuilds_only_pending_parent_edges_and_prunes_on_acceptance() {
    let schema = schema();
    let temp_dir = tempfile::tempdir().unwrap();
    let parent;
    let child;
    {
        let mut node = open_node_at(&temp_dir, schema.clone());
        let tx = node.open_exclusive().unwrap();
        node.tx_write(tx, "todos", row(1), title_cells("parent"), None)
            .unwrap();
        let (parent_tx, _unit) = node.commit_exclusive(tx, AuthorId::SYSTEM, 10).unwrap();
        parent = parent_tx;
        child = node
            .commit_mergeable(
                MergeableCommit::new("todos", row(2), 11)
                    .parents(vec![parent])
                    .cells(title_cells("child")),
            )
            .unwrap();
        assert_eq!(
            node.rejections.child_txs_by_parent.get(&parent),
            Some(&BTreeSet::from([child]))
        );
    }

    let mut reopened = reopen_node_at(&temp_dir, node(1), schema);
    assert_eq!(
        reopened.rejections.child_txs_by_parent.get(&parent),
        Some(&BTreeSet::from([child]))
    );
    reopened
        .apply_fate_update(
            parent,
            Fate::Accepted,
            Some(GlobalSeq(1)),
            Some(DurabilityTier::Global),
        )
        .unwrap();
    assert!(reopened.rejections.child_txs_by_parent.is_empty());
}

#[test]
fn recovery_rebuilds_global_clock_from_accepted_transactions() {
    let schema = schema();
    let temp_dir = tempfile::tempdir().unwrap();
    {
        let mut node = open_node_at(&temp_dir, schema.clone());
        let first = node
            .commit_mergeable(
                MergeableCommit::new("todos", row(1), 10).cells(title_cells("first")),
            )
            .unwrap();
        node.apply_fate_update(
            first,
            Fate::Accepted,
            Some(GlobalSeq(1)),
            Some(DurabilityTier::Global),
        )
        .unwrap();
        let second = node
            .commit_mergeable(
                MergeableCommit::new("todos", row(2), 11).cells(title_cells("second")),
            )
            .unwrap();
        node.apply_fate_update(
            second,
            Fate::Accepted,
            Some(GlobalSeq(2)),
            Some(DurabilityTier::Global),
        )
        .unwrap();
        assert_eq!(node.clock.applied_global_watermark, GlobalSeq(2));
        assert_eq!(node.clock.next_global_seq, GlobalSeq(3));
    }

    let reopened = reopen_node_at(&temp_dir, node(1), schema);
    assert_eq!(reopened.clock.applied_global_watermark, GlobalSeq(2));
    assert_eq!(reopened.clock.next_global_seq, GlobalSeq(3));
}

#[test]
fn reopen_in_place_recovers_history_watermarks_pending_edges_and_rehydrates_peer() {
    let (_dir, mut core) = open_node_with_uuid(node(0x3a));
    let mut peer = PeerState::new();
    let accepted = core
        .commit_mergeable(MergeableCommit::new("todos", row(3), 9).cells(title_cells("accepted")))
        .unwrap();
    core.apply_fate_update(
        accepted,
        Fate::Accepted,
        Some(GlobalSeq(7)),
        Some(DurabilityTier::Global),
    )
    .unwrap();
    let parent_tx = core.open_exclusive().unwrap();
    core.tx_write(parent_tx, "todos", row(1), title_cells("parent"), None)
        .unwrap();
    let (parent, _unit) = core
        .commit_exclusive(parent_tx, AuthorId::SYSTEM, 10)
        .unwrap();
    let child = core
        .commit_mergeable(
            MergeableCommit::new("todos", row(2), 11)
                .parents(vec![parent])
                .cells(title_cells("child")),
        )
        .unwrap();
    let update = peer.current_rows_update(&mut core, "todos").unwrap();
    assert!(matches!(update, SyncMessage::ViewUpdate { .. }));

    let mut reopened = core.reopen_in_place().unwrap();
    assert_eq!(
        reopened.transaction_state(accepted).unwrap(),
        (Fate::Accepted, Some(GlobalSeq(7)), DurabilityTier::Global)
    );
    assert_eq!(
        reopened.rejections.child_txs_by_parent.get(&parent),
        Some(&BTreeSet::from([child]))
    );
    assert_eq!(
        reopened
            .current_rows("todos", DurabilityTier::Global)
            .unwrap()
            .into_iter()
            .map(current_row_pair)
            .collect::<BTreeMap<_, _>>(),
        BTreeMap::from([(row(3), title_cells("accepted"))])
    );

    let rehydrated = peer.rehydrate_current_rows(&mut reopened, "todos").unwrap();
    assert!(matches!(rehydrated, SyncMessage::ViewUpdate { .. }));
}
#[test]
fn empty_string_cells_and_absent_cells_survive_restart() {
    let schema = two_column_schema();
    let (node_dir, mut local_node) = open_node_with_schema(node(1), schema.clone());
    let empty_row = row(1);
    let absent_row = row(2);

    local_node
        .commit_mergeable(
            MergeableCommit::new("todos", empty_row, 10).cells(title_cells(String::new())),
        )
        .unwrap();
    local_node
        .commit_mergeable(
            MergeableCommit::new("todos", absent_row, 11)
                .cells(BTreeMap::from([("body".to_owned(), "body".to_owned())])),
        )
        .unwrap();
    let expected = BTreeMap::from([
        (empty_row, title_cells(String::new())),
        (absent_row, BTreeMap::from([("body".to_owned(), v("body"))])),
    ]);
    assert_eq!(
        local_node
            .current_rows("todos", DurabilityTier::Local)
            .unwrap()
            .into_iter()
            .map(current_row_pair)
            .collect::<BTreeMap<_, _>>(),
        expected
    );

    drop(local_node);
    let mut reopened = reopen_node_at(&node_dir, node(1), schema);
    assert_eq!(
        reopened
            .current_rows("todos", DurabilityTier::Local)
            .unwrap()
            .into_iter()
            .map(current_row_pair)
            .collect::<BTreeMap<_, _>>(),
        expected
    );
}
#[test]
fn empty_string_cells_survive_restart_in_core_merge_version() {
    let schema = two_column_schema();
    let (_writer_a_dir, mut writer_a) = open_node_with_schema(node(1), schema.clone());
    let (_writer_b_dir, mut writer_b) = open_node_with_schema(node(2), schema.clone());
    let (core_dir, mut core) = open_node_with_schema(node(9), schema.clone());
    let merged_row = row(7);

    let left = writer_a
        .commit_mergeable_unit(
            MergeableCommit::new("todos", merged_row, 10).cells(title_cells(String::new())),
        )
        .unwrap()
        .1;
    let right = writer_b
        .commit_mergeable_unit(
            MergeableCommit::new("todos", merged_row, 11)
                .cells(BTreeMap::from([("body".to_owned(), "body".to_owned())])),
        )
        .unwrap()
        .1;
    core.apply_sync_message(left).unwrap();
    core.apply_sync_message(right).unwrap();

    let expected = vec![(
        merged_row,
        BTreeMap::from([
            ("title".to_owned(), v(String::new())),
            ("body".to_owned(), v("body")),
        ]),
    )];
    assert_eq!(
        core.current_rows("todos", DurabilityTier::Global).unwrap(),
        expected
    );

    drop(core);
    let mut reopened = reopen_node_at(&core_dir, node(9), schema);
    assert_eq!(
        reopened
            .current_rows("todos", DurabilityTier::Global)
            .unwrap(),
        expected
    );
}
#[test]
fn persisted_currency_tables_match_history_rows_after_reopen() {
    let schema = two_column_schema();
    let (_writer_a_dir, mut writer_a) = open_node_with_schema(node(1), schema.clone());
    let (_writer_b_dir, mut writer_b) = open_node_with_schema(node(2), schema.clone());
    let (core_dir, mut core) = open_node_with_schema(node(9), schema.clone());
    let merged_row = row(7);

    let left = writer_a
        .commit_mergeable_unit(
            MergeableCommit::new("todos", merged_row, 10).cells(title_cells(String::new())),
        )
        .unwrap()
        .1;
    let right = writer_b
        .commit_mergeable_unit(
            MergeableCommit::new("todos", merged_row, 11)
                .cells(BTreeMap::from([("body".to_owned(), "body".to_owned())])),
        )
        .unwrap()
        .1;
    core.apply_sync_message(left).unwrap();
    core.apply_sync_message(right).unwrap();
    assert_currency_tables_match_storage(&mut core, "todos");

    drop(core);
    let mut reopened = reopen_node_at(&core_dir, node(9), schema);
    assert_currency_tables_match_storage(&mut reopened, "todos");
}
#[test]
fn recovery_ignores_foreign_tx_ids_when_restoring_next_own_ingest_seq() {
    let schema = schema();
    let (node_dir, mut node_a) = open_node_with_schema(node(1), schema.clone());
    let own = node_a
        .commit_mergeable(MergeableCommit::new("todos", row(1), 10).cells(title_cells("own")))
        .unwrap();
    assert_eq!(own.time, TxTime::from(10));

    let foreign = TxId::new(TxTime::from(500), node(2));
    node_a
        .ingest_relay_commit_unit(
            Transaction {
                tx_id: foreign,
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
            vec![version_record(
                row(2),
                Vec::new(),
                title_cells("foreign"),
                None,
            )],
        )
        .unwrap();

    drop(node_a);
    let mut reopened = reopen_node_at(&node_dir, node(1), schema);
    let next_own = reopened
        .commit_mergeable(MergeableCommit::new("todos", row(3), 12).cells(title_cells("next")))
        .unwrap();
    assert_eq!(next_own.time, TxTime::new(500, 1));
}
#[test]
fn row_history_reports_versions_flags_and_audit_records_across_restart() {
    let (_writer_a_dir, mut writer_a) = open_node_with_uuid(node(1));
    let (_writer_b_dir, mut writer_b) = open_node_with_uuid(node(2));
    let (core_dir, mut core) = open_node_with_uuid(node(9));
    let row = row(7);

    let left = commit_mergeable_global(
        &mut writer_a,
        &mut core,
        MergeableCommit::new("todos", row, 10).cells(title_cells("left")),
    );
    let right = commit_mergeable_global(
        &mut writer_b,
        &mut core,
        MergeableCommit::new("todos", row, 11).cells(title_cells("right")),
    );
    let deleted = commit_mergeable_global(
        &mut writer_a,
        &mut core,
        MergeableCommit::new("todos", row, 20).deletion(DeletionEvent::Deleted),
    );
    let restored = commit_mergeable_global(
        &mut writer_a,
        &mut core,
        MergeableCommit::new("todos", row, 21).deletion(DeletionEvent::Restored),
    );

    let tx_id = core.open_exclusive().unwrap();
    core.tx_read(tx_id, "todos", row).unwrap();
    core.tx_write(tx_id, "todos", row, title_cells("exclusive"), None)
        .unwrap();
    let (exclusive, _unit) = core.commit_exclusive(tx_id, AuthorId::SYSTEM, 30).unwrap();
    let exclusive_global_seq = core.clock.next_global_seq;
    core.apply_fate_update(
        exclusive,
        Fate::Accepted,
        Some(exclusive_global_seq),
        Some(DurabilityTier::Global),
    )
    .unwrap();

    sync_current_rows_to(&mut core, &mut writer_b, 43);
    let rejected_tx = writer_b.open_exclusive().unwrap();
    writer_b.tx_read(rejected_tx, "todos", row).unwrap();
    commit_mergeable_global(
        &mut writer_a,
        &mut core,
        MergeableCommit::new("todos", row, 40).cells(BTreeMap::from([(
            "title".to_owned(),
            "intervening".to_owned(),
        )])),
    );
    writer_b
        .tx_write(rejected_tx, "todos", row, title_cells("rejected"), None)
        .unwrap();
    let (rejected, unit) = writer_b
        .commit_exclusive(rejected_tx, AuthorId::SYSTEM, 41)
        .unwrap();
    let [fate] = core.apply_sync_message(unit).unwrap().try_into().unwrap();
    assert_eq!(
        fate,
        SyncMessage::FateUpdate {
            tx_id: rejected,
            fate: Fate::Rejected(RejectionReason::ExclusiveConflict),
            global_seq: None,
            durability: None,
        }
    );

    let history = core.row_history("todos", row).unwrap();
    assert!(history
        .windows(2)
        .all(|pair| pair[0].tx_id().time.sort_key(pair[0].tx_id().node)
            <= pair[1].tx_id().time.sort_key(pair[1].tx_id().node)));
    assert!(history.iter().any(|entry| entry.tx_id() == left));
    assert!(history.iter().any(|entry| entry.tx_id() == right));
    assert!(history.iter().any(|entry| {
        entry.tx_id().node == node(9)
            && entry.parents().contains(&left)
            && entry.parents().contains(&right)
            && entry.layer() == MergeAspect::Content
            && entry.fate() == Fate::Accepted
            && entry.global_seq().is_some()
            && entry.durability() == DurabilityTier::Global
    }));
    assert!(history.iter().any(|entry| {
        entry.tx_id() == deleted
            && entry.layer() == MergeAspect::Deletion
            && entry.deletion() == Some(DeletionEvent::Deleted)
            && !entry.is_locally_current()
            && !entry.is_globally_current()
    }));
    assert!(history.iter().any(|entry| {
        entry.tx_id() == restored
            && entry.layer() == MergeAspect::Deletion
            && entry.deletion() == Some(DeletionEvent::Restored)
            && entry.is_locally_current()
            && entry.is_globally_current()
    }));
    assert!(history.iter().any(|entry| {
        entry.tx_id() == exclusive
            && entry.kind() == TxKind::Exclusive
            && entry.made_by() == AuthorId::SYSTEM
            && entry.cell(&schema().tables[0], "title") == Some(v("exclusive"))
            && entry.parents().len() == 1
    }));
    assert!(!history.iter().any(|entry| entry.tx_id() == rejected));
    assert_eq!(
        core.transaction_record(rejected).unwrap().fate,
        Fate::Rejected(RejectionReason::ExclusiveConflict)
    );

    drop(core);
    let mut reopened = reopen_node_at(&core_dir, node(9), schema());
    assert_eq!(reopened.row_history("todos", row).unwrap(), history);
    assert_eq!(
        reopened.transaction_record(rejected).unwrap().fate,
        Fate::Rejected(RejectionReason::ExclusiveConflict)
    );
}
#[test]
fn transaction_metadata_round_trips_through_recovery() {
    let (dir, mut local_node) = open_node_with_uuid(node(1));
    let row = row(7);
    let merge = local_node
        .commit_mergeable(
            MergeableCommit::new("todos", row, 10)
                .cells(title_cells("merge"))
                .user_metadata(r#"{"source":"merge"}"#.to_owned()),
        )
        .unwrap();

    let tx_id = local_node.open_exclusive().unwrap();
    local_node
        .tx_set_metadata(tx_id, r#"{"source":"exclusive"}"#.to_owned())
        .unwrap();
    local_node
        .tx_write(tx_id, "todos", row, title_cells("exclusive"), None)
        .unwrap();
    let (exclusive, _) = local_node
        .commit_exclusive(tx_id, AuthorId::SYSTEM, 11)
        .unwrap();

    assert_eq!(
        local_node
            .transaction_record(merge)
            .unwrap()
            .user_metadata_json,
        Some(r#"{"source":"merge"}"#.to_owned())
    );
    assert_eq!(
        local_node
            .transaction_record(exclusive)
            .unwrap()
            .user_metadata_json,
        Some(r#"{"source":"exclusive"}"#.to_owned())
    );

    drop(local_node);
    let mut reopened = reopen_node_at(&dir, node(1), schema());
    assert_eq!(
        reopened
            .transaction_record(merge)
            .unwrap()
            .user_metadata_json,
        Some(r#"{"source":"merge"}"#.to_owned())
    );
    assert_eq!(
        reopened
            .transaction_record(exclusive)
            .unwrap()
            .user_metadata_json,
        Some(r#"{"source":"exclusive"}"#.to_owned())
    );
}
