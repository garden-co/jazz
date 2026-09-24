#[test]
fn exclusive_base_snapshot_preserves_sparse_local_and_foreign_dots() {
    let owner = node(1);
    let own_dot = TxId::new(TxTime::from(10), owner);
    let foreign_dot = TxId::new(TxTime::from(11), node(2));

    let snapshot = crate::tx::Snapshot::exclusive_base(
        owner,
        GlobalTime(3),
        TxTime::from(12),
        vec![own_dot, foreign_dot],
    )
    .unwrap();
    assert_eq!(snapshot.dots, vec![own_dot, foreign_dot]);
}

#[test]
fn exclusive_begin_resolves_sparse_global_dots_without_scanning_history_after_reopen() {
    let (dir, mut core) = open_node();
    for ordinal in 1..=128 {
        core.commit_mergeable_settled(
            MergeableCommit::new("todos", row(ordinal), ordinal as u64)
                .cells(title_cells(format!("history-{ordinal}"))),
        )
        .unwrap();
    }

    let sparse = TxId::new(TxTime::from(200), node(0xf0));
    ingest_relay_version(&mut core, sparse, 200, Vec::new(), row(0xf0), "sparse");
    core.apply_fate_update(
        sparse,
        Fate::Accepted,
        Some(GlobalTime(100)),
        Some(DurabilityTier::Global),
    )
    .unwrap();

    drop(core);
    let mut reopened = reopen_node_at(&dir, node(9), schema());
    reopened.reset_storage_read_metrics();
    let batch = OpenTransactionId::new();
    reopened.open_exclusive(batch).unwrap();
    assert_eq!(
        reopened.open_tx(batch).unwrap().base_snapshot.dots,
        vec![sparse]
    );

    let metrics = reopened.take_storage_read_metrics();
    assert_eq!(metrics.transactions_rows.reads, 1);
    assert_eq!(metrics.transactions_indexes.ranges, 1);
}

#[test]
fn open_batch_identity_is_unique_and_terminal() {
    let (_temp_dir, mut node) = open_node();
    let rolled_back = OpenTransactionId::new();
    node.open_exclusive(rolled_back).unwrap();
    assert!(matches!(
        node.open_exclusive(rolled_back).resolve(),
        Err(Error::DuplicateOpenBatch(id)) if id == rolled_back
    ));
    node.abandon_tx(rolled_back).unwrap();
    assert!(matches!(
        node.open_exclusive(rolled_back).resolve(),
        Err(Error::DuplicateOpenBatch(id)) if id == rolled_back
    ));

    let committed = OpenTransactionId::new();
    let author = user(1);
    node.open_exclusive_for_identity(committed, author).unwrap();
    node.tx_write(committed, "todos", row(91), title_cells("committed"), None)
        .unwrap();
    node.commit_exclusive_settled(committed, author, 10)
        .unwrap();
    assert!(matches!(
        node.open_exclusive(committed).resolve(),
        Err(Error::DuplicateOpenBatch(id)) if id == committed
    ));
}

/// Bare node transactions are system-owned; application transactions bind the
/// authenticated author when opened and reject a different commit author.
#[test]
fn exclusive_identity_binding_requires_an_explicit_author_at_open() {
    let (_temp_dir, mut node) = open_node();
    let alice = user(0xa1);
    let bob = user(0xb2);

    let system_owned = OpenTransactionId::new();
    node.open_exclusive(system_owned).unwrap();
    node.tx_write(system_owned, "todos", row(1), title_cells("system"), None)
        .unwrap();
    assert!(matches!(
        node.commit_exclusive_settled(system_owned, alice, 10),
        Err(Error::OpenTransactionIdentityMismatch)
    ));
    node.commit_exclusive_settled(system_owned, AuthorSubject::SYSTEM, 10)
        .unwrap();

    let bound = OpenTransactionId::new();
    node.open_exclusive_for_identity(bound, alice).unwrap();
    node.tx_write(bound, "todos", row(2), title_cells("bound"), None)
        .unwrap();
    assert!(matches!(
        node.commit_exclusive_settled(bound, bob, 11),
        Err(Error::OpenTransactionIdentityMismatch)
    ));
    // Planted positive: rejecting Bob does not consume Alice's capability.
    node.commit_exclusive_settled(bound, alice, 11).unwrap();
}

#[test]
fn exclusive_tx_snapshot_read_ignores_newer_commits_after_open() {
    let (_temp_dir, mut node) = open_node();
    let row = row(7);
    let base = node
        .commit_mergeable_settled(MergeableCommit::new("todos", row, 10).cells(title_cells("base")))
        .unwrap();
    let tx_id = OpenTransactionId::new();
    node.open_exclusive(tx_id).unwrap();

    node.commit_mergeable_settled(
        MergeableCommit::new("todos", row, 11).cells(title_cells("newer")),
    )
    .unwrap();

    assert_eq!(
        node.tx_read(tx_id, "todos", row).unwrap(),
        Some(title_cells("base"))
    );
    assert_eq!(
        node.open_tx(tx_id).unwrap().row_reads,
        vec![RowRead {
            table: "todos".to_owned(),
            row_uuid: row,
            version: base,
        }]
    );
}
#[test]
fn exclusive_tx_reads_own_pending_writes() {
    let (_temp_dir, mut node) = open_node();
    let existing = row(7);
    let created = row(8);
    node.commit_mergeable_settled(
        MergeableCommit::new("todos", existing, 10).cells(title_cells("base")),
    )
    .unwrap();
    let tx_id = OpenTransactionId::new();
    node.open_exclusive(tx_id).unwrap();

    node.tx_write(tx_id, "todos", existing, title_cells("pending"), None)
        .unwrap();
    node.tx_write(tx_id, "todos", created, title_cells("created"), None)
        .unwrap();

    assert_eq!(
        node.tx_read(tx_id, "todos", existing).unwrap(),
        Some(title_cells("pending"))
    );
    assert_eq!(
        node.tx_current_rows(tx_id, "todos").unwrap(),
        vec![
            (existing, title_cells("pending")),
            (created, title_cells("created")),
        ]
    );
    let predicate_shape = crate::query::Query::from("todos")
        .validate(&schema())
        .unwrap();
    let predicate_binding = predicate_shape
        .bind(std::collections::BTreeMap::new())
        .unwrap();
    assert_eq!(
        node.open_tx(tx_id).unwrap().predicate_reads,
        vec![PredicateRead {
            table: "todos".to_owned(),
            shape_id: predicate_shape.shape_id(),
            shape: predicate_shape.query().clone(),
            binding_id: predicate_binding.binding_id(),
            binding_values: predicate_binding.values().clone(),
        }]
    );
}

#[test]
fn exclusive_tx_pending_writes_overlay_snapshot_for_point_and_table_reads() {
    let (_temp_dir, mut node) = open_node();
    let existing = row(7);
    let created = row(8);
    node.commit_mergeable_settled(
        MergeableCommit::new("todos", existing, 10).cells(title_cells("base")),
    )
    .unwrap();
    let tx_id = OpenTransactionId::new();
    node.open_exclusive(tx_id).unwrap();

    node.tx_write(tx_id, "todos", existing, title_cells("pending"), None)
        .unwrap();
    node.tx_write(tx_id, "todos", created, title_cells("created"), None)
        .unwrap();

    assert_eq!(
        node.tx_read(tx_id, "todos", existing).unwrap(),
        Some(title_cells("pending"))
    );
    assert_eq!(
        node.tx_current_rows(tx_id, "todos").unwrap(),
        vec![
            (existing, title_cells("pending")),
            (created, title_cells("created")),
        ]
    );
}

#[test]
fn tx_read_records_present_and_absent_snapshot_reads() {
    let (_temp_dir, mut node) = open_node();
    let present = row(7);
    let absent = row(8);
    let version = node
        .commit_mergeable_settled(
            MergeableCommit::new("todos", present, 10).cells(title_cells("base")),
        )
        .unwrap();
    let tx_id = OpenTransactionId::new();
    node.open_exclusive(tx_id).unwrap();

    assert_eq!(
        node.tx_read(tx_id, "todos", present).unwrap(),
        Some(title_cells("base"))
    );
    assert_eq!(node.tx_read(tx_id, "todos", absent).unwrap(), None);

    let open = node.open_tx(tx_id).unwrap();
    assert_eq!(
        open.row_reads,
        vec![RowRead {
            table: "todos".to_owned(),
            row_uuid: present,
            version,
        }]
    );
    assert_eq!(
        open.absent_reads,
        vec![AbsentRead {
            table: "todos".to_owned(),
            row_uuid: absent,
        }]
    );
}

#[test]
fn tx_read_parent_cache_is_invalidated_by_same_row_write_without_changing_read_set() {
    let (_temp_dir, mut node) = open_node();
    let row = row(7);
    let base = node
        .commit_mergeable_settled(MergeableCommit::new("todos", row, 10).cells(title_cells("base")))
        .unwrap();
    let tx_id = OpenTransactionId::new();
    node.open_exclusive(tx_id).unwrap();

    assert_eq!(
        node.tx_read(tx_id, "todos", row).unwrap(),
        Some(title_cells("base"))
    );
    assert!(
        node.open_tx(tx_id)
            .unwrap()
            .base_snapshot_rows
            .contains_key(&(
                node.current_write_schema().unwrap().schema,
                "todos".to_owned(),
                row
            ))
    );

    node.tx_write(tx_id, "todos", row, title_cells("updated"), None)
        .unwrap();
    assert!(
        !node
            .open_tx(tx_id)
            .unwrap()
            .base_snapshot_rows
            .contains_key(&(
                node.current_write_schema().unwrap().schema,
                "todos".to_owned(),
                row
            ))
    );

    let (_exclusive, unit) = node
        .commit_exclusive_settled(tx_id, AuthorSubject::SYSTEM, 11)
        .unwrap();
    let SyncMessage::CommitUnit { tx, versions } = unit else {
        panic!("expected exclusive commit unit");
    };
    assert_eq!(
        tx.row_read_set.as_deref(),
        Some(
            [RowRead {
                table: "todos".to_owned(),
                row_uuid: row,
                version: base,
            }]
            .as_slice()
        )
    );
    assert_eq!(versions.len(), 1);
}

#[test]
fn exclusive_tx_snapshot_applies_deletion_register() {
    let (_temp_dir, mut node) = open_node();
    let row = row(7);
    node.commit_mergeable_settled(
        MergeableCommit::new("todos", row, 10).cells(title_cells("base")),
    )
    .unwrap();
    let deleted = node
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row, 11).deletion(DeletionEvent::Deleted),
        )
        .unwrap();
    let tx_id = OpenTransactionId::new();
    node.open_exclusive(tx_id).unwrap();

    node.commit_mergeable_settled(
        MergeableCommit::new("todos", row, 12).deletion(DeletionEvent::Restored),
    )
    .unwrap();

    assert_eq!(node.tx_read(tx_id, "todos", row).unwrap(), None);
    assert_eq!(
        node.open_tx(tx_id).unwrap().row_reads,
        vec![RowRead {
            table: "todos".to_owned(),
            row_uuid: row,
            version: deleted,
        }]
    );

    node.tx_write(
        tx_id,
        "todos",
        row,
        BTreeMap::<String, Value>::new(),
        Some(DeletionEvent::Restored),
    )
    .unwrap();
    assert_eq!(
        node.tx_read(tx_id, "todos", row).unwrap(),
        Some(title_cells("base"))
    );
}
#[test]
fn exclusive_tx_open_state_is_invisible_outside_transaction() {
    let (_temp_dir, mut node) = open_node();
    let row = row(7);
    let tx_id = OpenTransactionId::new();
    node.open_exclusive(tx_id).unwrap();
    node.tx_write(tx_id, "todos", row, title_cells("buffered"), None)
        .unwrap();

    assert!(
        node.current_rows("todos", DurabilityTier::Local)
            .unwrap()
            .is_empty()
    );
    assert!(node.view_update_for_current_rows("todos").is_ok());
    assert!(node.abandon_tx(tx_id).is_ok());
    assert!(matches!(
        node.tx_read(tx_id, "todos", row).unwrap_err(),
        Error::MissingOpenBatch(missing) if missing == tx_id
    ));
}
#[test]
fn partial_node_snapshot_does_not_promote_received_global_times() {
    let (_temp_dir, mut reader) = open_node_with_uuid(node(3));

    for (seq, row_byte) in [(1, 1), (3, 3)] {
        let tx_id = TxId::new(TxTime::new(10 + seq, 0), node(9));
        reader
            .ingest_known_transaction(
                Transaction {
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
                },
                vec![version_record(
                    row(row_byte),
                    Vec::new(),
                    title_cells(format!("seq-{seq}")),
                    None,
                )],
                Fate::Accepted,
                Some(GlobalTime(seq)),
                DurabilityTier::Global,
            )
            .unwrap();
    }

    let first_snapshot = OpenTransactionId::new();
    reader.open_exclusive(first_snapshot).unwrap();
    let first_base = &reader.open_tx(first_snapshot).unwrap().base_snapshot;
    assert_eq!(first_base.global_base, GlobalTime::default());
    assert_eq!(first_base.dots.len(), 2);

    let tx_id = TxId::new(TxTime::from(12), node(9));
    reader
        .ingest_known_transaction(
            Transaction {
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
            },
            vec![version_record(
                row(2),
                Vec::new(),
                title_cells("seq-2"),
                None,
            )],
            Fate::Accepted,
            Some(GlobalTime(2)),
            DurabilityTier::Global,
        )
        .unwrap();

    let second_snapshot = OpenTransactionId::new();
    reader.open_exclusive(second_snapshot).unwrap();
    let second_base = &reader.open_tx(second_snapshot).unwrap().base_snapshot;
    assert_eq!(second_base.global_base, GlobalTime::default());
    assert_eq!(second_base.dots.len(), 3);
}

#[test]
fn partial_node_snapshot_advances_from_authoritative_settled_through() {
    let (_temp_dir, mut reader) = open_node_with_uuid(node(3));

    for seq in [1, 3] {
        let tx_id = TxId::new(TxTime::new(10 + seq, 0), node(9));
        reader
            .ingest_known_transaction(
                Transaction {
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
                },
                vec![version_record(
                    row(seq as u8),
                    Vec::new(),
                    title_cells(format!("seq-{seq}")),
                    None,
                )],
                Fate::Accepted,
                Some(GlobalTime(seq)),
                DurabilityTier::Global,
            )
            .unwrap();
    }

    reader.record_authoritative_settled_through(GlobalTime(2));

    let open_id = OpenTransactionId::new();
    reader.open_exclusive(open_id).unwrap();
    let base = reader.open_tx(open_id).unwrap().base_snapshot.clone();
    assert_eq!(base.global_base, GlobalTime(2));
    assert_eq!(base.dots, vec![TxId::new(TxTime::new(13, 0), node(9))]);
    let rows = reader
        .projected_snapshot_current_rows("todos", schema().version_id(), &base)
        .resolve()
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().all(|row| row.projected_tx_alias().is_some()));
}

#[test]
fn core_snapshot_uses_atomically_committed_global_time() {
    let (_temp_dir, mut core) = open_node_with_uuid(node(9));
    let tx_id = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row(1), 25).cells(title_cells("settled")),
        )
        .unwrap();
    core.finalize_local_mergeable_commit_settled(tx_id).unwrap();

    let open_id = OpenTransactionId::new();
    core.open_exclusive(open_id).unwrap();
    let base = &core.open_tx(open_id).unwrap().base_snapshot;
    assert_eq!(base.global_base, GlobalTime::new(25, 0).unwrap());
    assert!(base.dots.is_empty());
}

#[test]
fn partial_snapshot_whole_table_validation_accepts_its_sparse_global_dots() {
    let (_client_dir, mut client) = open_node_with_uuid(node(1));
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    commit_mergeable_global(
        &mut client,
        &mut core,
        MergeableCommit::new("todos", row(1), 10).cells(title_cells("base")),
    );

    let open_id = OpenTransactionId::new();
    client.open_exclusive(open_id).unwrap();
    assert_eq!(client.tx_current_rows(open_id, "todos").unwrap().len(), 1);
    client
        .tx_write(open_id, "todos", row(2), title_cells("next"), None)
        .unwrap();
    let (_, unit) = client
        .commit_exclusive(open_id, AuthorSubject::SYSTEM, 11)
        .unwrap();

    let updates = core.apply_sync_message_settled(unit).unwrap();
    let [SyncMessage::FateUpdate { fate, .. }] = updates.as_slice() else {
        panic!("expected fate update");
    };
    assert_eq!(*fate, Fate::Accepted);
}

#[test]
fn partial_snapshot_filtered_validation_accepts_its_sparse_global_dots() {
    let (_client_dir, mut client) = open_node_with_uuid(node(1));
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    commit_mergeable_global(
        &mut client,
        &mut core,
        MergeableCommit::new("todos", row(1), 10).cells(title_cells("base")),
    );
    let shape = Query::from("todos")
        .filter(eq(col("title"), lit(Value::String("base".to_owned()))))
        .validate(&client.catalogue.schema)
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();

    let open_id = OpenTransactionId::new();
    client.open_exclusive(open_id).unwrap();
    assert_eq!(client.tx_query(open_id, &shape, &binding).unwrap().len(), 1);
    client
        .tx_write(open_id, "todos", row(2), title_cells("next"), None)
        .unwrap();
    let (_, unit) = client
        .commit_exclusive(open_id, AuthorSubject::SYSTEM, 11)
        .unwrap();

    let updates = core.apply_sync_message_settled(unit).unwrap();
    let [SyncMessage::FateUpdate { fate, .. }] = updates.as_slice() else {
        panic!("expected fate update");
    };
    assert_eq!(*fate, Fate::Accepted);
}

#[test]
fn exclusive_commit_accepts_clean_end_to_end() {
    let (_client_dir, mut client) = open_node_with_uuid(node(1));
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    let row = row(7);
    commit_mergeable_global(
        &mut client,
        &mut core,
        MergeableCommit::new("todos", row, 10).cells(title_cells("base")),
    );
    let tx_id = OpenTransactionId::new();
    client.open_exclusive(tx_id).unwrap();
    client
        .tx_write(tx_id, "todos", row, title_cells("exclusive"), None)
        .unwrap();
    let (tx_id, unit) = client
        .commit_exclusive_settled(tx_id, AuthorSubject::SYSTEM, 11)
        .unwrap();
    assert_eq!(
        client
            .current_rows("todos", DurabilityTier::Local)
            .unwrap()
            .into_iter()
            .map(current_row_pair)
            .collect::<BTreeMap<_, _>>(),
        BTreeMap::from([(row, title_cells("exclusive"))])
    );

    let [fate] = core
        .apply_sync_message_settled(unit)
        .unwrap()
        .try_into()
        .unwrap();
    let SyncMessage::FateUpdate {
        fate: accepted,
        global_time,
        ..
    } = &fate
    else {
        panic!("expected fate update");
    };
    assert_eq!(accepted, &Fate::Accepted);
    assert_eq!(*global_time, Some(GlobalTime::new(11, 0).unwrap()));
    client.apply_sync_message_settled(fate).unwrap();
    assert_eq!(
        client.transaction_state_settled(tx_id).unwrap(),
        (
            Fate::Accepted,
            Some(GlobalTime::new(11, 0).unwrap()),
            DurabilityTier::Global
        )
    );
    assert_eq!(
        core.current_rows("todos", DurabilityTier::Global)
            .unwrap()
            .into_iter()
            .map(current_row_pair)
            .collect::<BTreeMap<_, _>>(),
        BTreeMap::from([(row, title_cells("exclusive"))])
    );
}
#[test]
fn exclusive_row_read_conflict_rejects_and_client_restores_old_value() {
    let (_client_dir, mut client) = open_node_with_uuid(node(1));
    let (_other_dir, mut other) = open_node_with_uuid(node(2));
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    let row = row(7);
    commit_mergeable_global(
        &mut client,
        &mut core,
        MergeableCommit::new("todos", row, 10).cells(title_cells("base")),
    );
    let tx_id = OpenTransactionId::new();
    client.open_exclusive(tx_id).unwrap();
    assert_eq!(
        client.tx_read(tx_id, "todos", row).unwrap(),
        Some(title_cells("base"))
    );
    commit_mergeable_global(
        &mut other,
        &mut core,
        MergeableCommit::new("todos", row, 12).cells(title_cells("winner")),
    );
    client
        .tx_write(tx_id, "todos", row, title_cells("loser"), None)
        .unwrap();
    let (_tx_id, unit) = client
        .commit_exclusive_settled(tx_id, AuthorSubject::SYSTEM, 13)
        .unwrap();
    let [fate] = core
        .apply_sync_message_settled(unit)
        .unwrap()
        .try_into()
        .unwrap();
    let SyncMessage::FateUpdate { fate: rejected, .. } = &fate else {
        panic!("expected fate update");
    };
    assert_eq!(
        rejected,
        &Fate::Rejected(RejectionReason::ExclusiveConflict)
    );
    client.apply_sync_message_settled(fate).unwrap();
    assert_eq!(
        client
            .current_rows("todos", DurabilityTier::Local)
            .unwrap()
            .into_iter()
            .map(current_row_pair)
            .collect::<BTreeMap<_, _>>(),
        BTreeMap::from([(row, title_cells("base"))])
    );
}

/// A row read records visible content. A later deletion changes that visible
/// state even though the content register retains the version the reader saw,
/// so the exclusive write must conflict rather than be admitted by content CAS.
#[test]
fn exclusive_row_read_conflicts_when_a_later_delete_hides_the_content() {
    let (_client_dir, mut client) = open_node_with_uuid(node(1));
    let (_other_dir, mut other) = open_node_with_uuid(node(2));
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    let row = row(0x6e);
    commit_mergeable_global(
        &mut client,
        &mut core,
        MergeableCommit::new("todos", row, 10).cells(title_cells("base")),
    );
    let open = OpenTransactionId::new();
    client.open_exclusive(open).unwrap();
    assert_eq!(
        client.tx_read(open, "todos", row).unwrap(),
        Some(title_cells("base"))
    );

    // Planted sensitivity: content remains current after this delete, so a
    // content-register-only row-read check would incorrectly accept below.
    commit_mergeable_global(
        &mut other,
        &mut core,
        MergeableCommit::new("todos", row, 12).deletion(DeletionEvent::Deleted),
    );
    client
        .tx_write(open, "todos", row, title_cells("loser"), None)
        .unwrap();
    let (_tx_id, unit) = client
        .commit_exclusive_settled(open, AuthorSubject::SYSTEM, 13)
        .unwrap();
    let [fate] = core
        .apply_sync_message_settled(unit)
        .unwrap()
        .try_into()
        .unwrap();
    assert!(matches!(
        fate,
        SyncMessage::FateUpdate {
            fate: Fate::Rejected(RejectionReason::ExclusiveConflict),
            ..
        }
    ));
}

/// A delete starts the independent deletion-register history. Its first
/// deletion-layer write therefore has no version parent even though content is
/// globally current; authority first-committer-wins must compare that same
/// deletion register rather than treating content as a general dependency.
#[test]
fn exclusive_delete_compares_the_deletion_register_not_content() {
    let (_client_dir, mut client) = open_node_with_uuid(node(1));
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    let row = row(0x6d);
    commit_mergeable_global(
        &mut client,
        &mut core,
        MergeableCommit::new("todos", row, 10).cells(title_cells("base")),
    );

    let open = OpenTransactionId::new();
    client.open_exclusive(open).unwrap();
    assert_eq!(
        client.tx_read(open, "todos", row).unwrap(),
        Some(title_cells("base"))
    );
    client
        .tx_write(
            open,
            "todos",
            row,
            BTreeMap::<String, Value>::new(),
            Some(DeletionEvent::Deleted),
        )
        .unwrap();
    let (_tx_id, unit) = client
        .commit_exclusive_settled(open, AuthorSubject::SYSTEM, 11)
        .unwrap();
    let SyncMessage::CommitUnit { versions, .. } = &unit else {
        panic!("expected exclusive commit unit");
    };
    assert_eq!(versions.len(), 1);

    let [fate] = core
        .apply_sync_message_settled(unit)
        .unwrap()
        .try_into()
        .unwrap();
    assert!(matches!(
        fate,
        SyncMessage::FateUpdate {
            fate: Fate::Accepted,
            ..
        }
    ));
}

/// Deletion visibility does not erase content ancestry. Replacing a deleted
/// row and restoring it atomically must advance both independent registers from
/// the winners captured by the exclusive snapshot.
#[test]
fn exclusive_replacement_and_restore_parent_their_own_registers() {
    let (_client_dir, mut client) = open_node_with_uuid(node(1));
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    let row = row(0x6f);
    let content_parent = commit_mergeable_global(
        &mut client,
        &mut core,
        MergeableCommit::new("todos", row, 10).cells(title_cells("base")),
    );
    let deletion_parent = commit_mergeable_global(
        &mut client,
        &mut core,
        MergeableCommit::new("todos", row, 11).deletion(DeletionEvent::Deleted),
    );

    let open = OpenTransactionId::new();
    client.open_exclusive(open).unwrap();
    assert_eq!(client.tx_read(open, "todos", row).unwrap(), None);
    client
        .tx_write(open, "todos", row, title_cells("replacement"), None)
        .unwrap();
    client
        .tx_write(
            open,
            "todos",
            row,
            BTreeMap::<String, Value>::new(),
            Some(DeletionEvent::Restored),
        )
        .unwrap();
    let (_tx_id, unit) = client
        .commit_exclusive_settled(open, AuthorSubject::SYSTEM, 12)
        .unwrap();
    let SyncMessage::CommitUnit { versions, .. } = &unit else {
        panic!("expected exclusive commit unit");
    };
    // The replacement and the restore are one row image of the row.
    assert_eq!(versions.len(), 1);
    assert_eq!(versions[0].deletion(), Some(DeletionEvent::Restored));
    let _ = (content_parent, deletion_parent);

    let [fate] = core
        .apply_sync_message_settled(unit)
        .unwrap()
        .try_into()
        .unwrap();
    assert!(
        matches!(
            fate,
            SyncMessage::FateUpdate {
                fate: Fate::Accepted,
                ..
            }
        ),
        "unexpected fate: {fate:?}"
    );
    assert_eq!(
        core.current_rows("todos", DurabilityTier::Global)
            .unwrap()
            .into_iter()
            .map(current_row_pair)
            .collect::<BTreeMap<_, _>>(),
        BTreeMap::from([(row, title_cells("replacement"))])
    );
}

#[test]
fn exclusive_predicate_phantom_conflict_rejects() {
    let (_client_dir, mut client) = open_node_with_uuid(node(1));
    let (_other_dir, mut other) = open_node_with_uuid(node(2));
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    let tx_id = OpenTransactionId::new();
    client.open_exclusive(tx_id).unwrap();
    assert!(client.tx_current_rows(tx_id, "todos").unwrap().is_empty());
    commit_mergeable_global(
        &mut other,
        &mut core,
        MergeableCommit::new("todos", row(1), 10).cells(title_cells("phantom")),
    );
    client
        .tx_write(tx_id, "todos", row(2), title_cells("mine"), None)
        .unwrap();
    let (_tx_id, unit) = client
        .commit_exclusive_settled(tx_id, AuthorSubject::SYSTEM, 11)
        .unwrap();
    let [fate] = core
        .apply_sync_message_settled(unit)
        .unwrap()
        .try_into()
        .unwrap();
    let SyncMessage::FateUpdate { fate, .. } = fate else {
        panic!("expected fate update");
    };
    assert_eq!(fate, Fate::Rejected(RejectionReason::ExclusiveConflict));
}

#[test]
fn exclusive_whole_table_predicate_ignores_other_table_changes() {
    let schema = build_public_test_schema(
        PublicSchemaBuilder::new()
            .table(PublicTableSchemaBuilder::new("todos").column("title", PublicColumnType::Text))
            .table(PublicTableSchemaBuilder::new("notes").column("title", PublicColumnType::Text)),
    );
    let (_client_dir, mut client) = open_node_with_schema(node(1), schema.clone());
    let (_other_dir, mut other) = open_node_with_schema(node(2), schema.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);

    let tx_id = OpenTransactionId::new();
    client.open_exclusive(tx_id).unwrap();
    assert!(client.tx_current_rows(tx_id, "todos").unwrap().is_empty());
    commit_mergeable_global(
        &mut other,
        &mut core,
        MergeableCommit::new("notes", row(1), 10).cells(title_cells("other table")),
    );
    client
        .tx_write(tx_id, "todos", row(2), title_cells("mine"), None)
        .unwrap();
    let (_tx_id, unit) = client
        .commit_exclusive_settled(tx_id, AuthorSubject::SYSTEM, 11)
        .unwrap();
    let [fate] = core
        .apply_sync_message_settled(unit)
        .unwrap()
        .try_into()
        .unwrap();
    let SyncMessage::FateUpdate { fate, .. } = fate else {
        panic!("expected fate update");
    };
    assert_eq!(fate, Fate::Accepted);
}

#[test]
fn exclusive_filtered_shape_phantom_conflict_rejects() {
    let (_client_dir, mut client) = open_node_with_uuid(node(1));
    let (_other_dir, mut other) = open_node_with_uuid(node(2));
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    let shape = crate::query::Query::from("todos")
        .filter(crate::query::eq(
            crate::query::col("title"),
            crate::query::lit("watched"),
        ))
        .validate(&schema())
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    register_shape_binding(&mut core, &shape, &binding);

    let tx_id = OpenTransactionId::new();
    client.open_exclusive(tx_id).unwrap();
    assert!(client.tx_query(tx_id, &shape, &binding).unwrap().is_empty());
    commit_mergeable_global(
        &mut other,
        &mut core,
        MergeableCommit::new("todos", row(1), 10).cells(title_cells("watched")),
    );
    client
        .tx_write(tx_id, "todos", row(2), title_cells("mine"), None)
        .unwrap();
    let (_tx_id, unit) = client
        .commit_exclusive_settled(tx_id, AuthorSubject::SYSTEM, 11)
        .unwrap();
    let [fate] = core
        .apply_sync_message_settled(unit)
        .unwrap()
        .try_into()
        .unwrap();
    let SyncMessage::FateUpdate { fate, .. } = fate else {
        panic!("expected fate update");
    };
    assert_eq!(fate, Fate::Rejected(RejectionReason::ExclusiveConflict));
}

#[test]
fn local_exclusive_predicate_rejects_remote_phantom_ingested_after_begin() {
    let (_client_dir, mut client) = open_node_with_uuid(node(1));
    let (_other_dir, mut other) = open_node_with_uuid(node(2));
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    let tx_id = OpenTransactionId::new();
    client.open_exclusive(tx_id).unwrap();
    assert!(client.tx_current_rows(tx_id, "todos").unwrap().is_empty());

    let (_remote, unit) = other
        .commit_mergeable_unit_settled(
            MergeableCommit::new("todos", row(1), 10).cells(title_cells("phantom")),
        )
        .unwrap();
    let [fate] = core
        .apply_sync_message_settled(unit.clone())
        .unwrap()
        .try_into()
        .unwrap();
    client.apply_sync_message_settled(unit).unwrap();
    client.apply_sync_message_settled(fate).unwrap();

    client
        .tx_write(tx_id, "todos", row(2), title_cells("mine"), None)
        .unwrap();
    assert!(matches!(
        client.commit_exclusive_settled(tx_id, AuthorSubject::SYSTEM, 11),
        Err(Error::TransactionConflict)
    ));
}
#[test]
fn exclusive_filtered_shape_ignores_irrelevant_changes() {
    let (_client_dir, mut client) = open_node_with_uuid(node(1));
    let (_other_dir, mut other) = open_node_with_uuid(node(2));
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    let shape = crate::query::Query::from("todos")
        .filter(crate::query::eq(
            crate::query::col("title"),
            crate::query::lit("watched"),
        ))
        .validate(&schema())
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    register_shape_binding(&mut core, &shape, &binding);

    let tx_id = OpenTransactionId::new();
    client.open_exclusive(tx_id).unwrap();
    assert!(client.tx_query(tx_id, &shape, &binding).unwrap().is_empty());
    commit_mergeable_global(
        &mut other,
        &mut core,
        MergeableCommit::new("todos", row(1), 10).cells(title_cells("irrelevant")),
    );
    client
        .tx_write(tx_id, "todos", row(2), title_cells("mine"), None)
        .unwrap();
    let (_tx_id, unit) = client
        .commit_exclusive_settled(tx_id, AuthorSubject::SYSTEM, 11)
        .unwrap();
    let [fate] = core
        .apply_sync_message_settled(unit)
        .unwrap()
        .try_into()
        .unwrap();
    let SyncMessage::FateUpdate { fate, .. } = fate else {
        panic!("expected fate update");
    };
    assert_eq!(fate, Fate::Accepted);
}

// The local pre-publication check runs inside `commit_exclusive` before any
// authority sees the unit, and it depends on which versions this node ingested
// between begin and commit. `JazzClient` cannot pin that interleaving, so
// these tests drive the node directly and assert the local commit outcome.
fn watched_title_shape() -> (ValidatedQuery, Binding) {
    let shape = crate::query::Query::from("todos")
        .filter(crate::query::eq(
            crate::query::col("title"),
            crate::query::lit("watched"),
        ))
        .validate(&schema())
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    (shape, binding)
}

#[test]
fn local_exclusive_filtered_predicate_rejects_remote_matching_phantom() {
    let (_client_dir, mut client) = open_node_with_uuid(node(1));
    let (_other_dir, mut other) = open_node_with_uuid(node(2));
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    let (shape, binding) = watched_title_shape();

    let tx_id = OpenTransactionId::new();
    client.open_exclusive(tx_id).unwrap();
    assert!(client.tx_query(tx_id, &shape, &binding).unwrap().is_empty());

    let (_remote, unit) = other
        .commit_mergeable_unit_settled(
            MergeableCommit::new("todos", row(1), 10).cells(title_cells("watched")),
        )
        .unwrap();
    let [fate] = core
        .apply_sync_message_settled(unit.clone())
        .unwrap()
        .try_into()
        .unwrap();
    client.apply_sync_message_settled(unit).unwrap();
    client.apply_sync_message_settled(fate).unwrap();

    client
        .tx_write(tx_id, "todos", row(9), title_cells("mine"), None)
        .unwrap();
    assert!(matches!(
        client.commit_exclusive_settled(tx_id, AuthorSubject::SYSTEM, 11),
        Err(Error::TransactionConflict)
    ));
}

#[test]
fn local_exclusive_filtered_predicate_rejects_pending_local_matching_insert() {
    let (_client_dir, mut client) = open_node_with_uuid(node(1));
    let (shape, binding) = watched_title_shape();

    let tx_id = OpenTransactionId::new();
    client.open_exclusive(tx_id).unwrap();
    assert!(client.tx_query(tx_id, &shape, &binding).unwrap().is_empty());

    // No authority exists: the matching insert stays a pending local write.
    client
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row(1), 10).cells(title_cells("watched")),
        )
        .unwrap();

    client
        .tx_write(tx_id, "todos", row(9), title_cells("mine"), None)
        .unwrap();
    assert!(matches!(
        client.commit_exclusive_settled(tx_id, AuthorSubject::SYSTEM, 11),
        Err(Error::TransactionConflict)
    ));
}

#[test]
fn local_exclusive_filtered_predicate_ignores_non_matching_inserts() {
    let (_client_dir, mut client) = open_node_with_uuid(node(1));
    let (_other_dir, mut other) = open_node_with_uuid(node(2));
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    let (shape, binding) = watched_title_shape();
    register_shape_binding(&mut core, &shape, &binding);

    let tx_id = OpenTransactionId::new();
    client.open_exclusive(tx_id).unwrap();
    assert!(client.tx_query(tx_id, &shape, &binding).unwrap().is_empty());

    let (_remote, unit) = other
        .commit_mergeable_unit_settled(
            MergeableCommit::new("todos", row(1), 10).cells(title_cells("unrelated remote")),
        )
        .unwrap();
    let [fate] = core
        .apply_sync_message_settled(unit.clone())
        .unwrap()
        .try_into()
        .unwrap();
    client.apply_sync_message_settled(unit).unwrap();
    client.apply_sync_message_settled(fate).unwrap();
    client
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row(2), 11).cells(title_cells("unrelated local")),
        )
        .unwrap();

    client
        .tx_write(tx_id, "todos", row(9), title_cells("mine"), None)
        .unwrap();
    // Passing the local check publishes the unit; the authority agrees.
    let (_committed, unit) = client
        .commit_exclusive_settled(tx_id, AuthorSubject::SYSTEM, 12)
        .unwrap();
    let [fate] = core
        .apply_sync_message_settled(unit)
        .unwrap()
        .try_into()
        .unwrap();
    let SyncMessage::FateUpdate { fate, .. } = fate else {
        panic!("expected fate update");
    };
    assert_eq!(fate, Fate::Accepted);
}

#[test]
fn local_exclusive_filtered_predicate_rejects_pending_local_removal() {
    for (name, removal) in [
        (
            "moved out of the filter",
            MergeableCommit::new("todos", row(1), 20).cells(title_cells("moved")),
        ),
        (
            "deleted",
            MergeableCommit::new("todos", row(1), 20).deletion(DeletionEvent::Deleted),
        ),
    ] {
        let (_client_dir, mut client) = open_node_with_uuid(node(1));
        let (_core_dir, mut core) = open_node_with_uuid(node(9));
        let (shape, binding) = watched_title_shape();
        commit_mergeable_global(
            &mut client,
            &mut core,
            MergeableCommit::new("todos", row(1), 10).cells(title_cells("watched")),
        );

        let tx_id = OpenTransactionId::new();
        client.open_exclusive(tx_id).unwrap();
        assert_eq!(
            client.tx_query(tx_id, &shape, &binding).unwrap().len(),
            1,
            "{name}"
        );

        // The authority is offline from here on: the removal stays pending.
        client.commit_mergeable_settled(removal).unwrap();

        client
            .tx_write(tx_id, "todos", row(9), title_cells("mine"), None)
            .unwrap();
        assert!(
            matches!(
                client.commit_exclusive_settled(tx_id, AuthorSubject::SYSTEM, 21),
                Err(Error::TransactionConflict)
            ),
            "{name}"
        );
    }
}
#[test]
fn exclusive_shape_predicate_is_binding_sensitive() {
    let author_a = user(0xa1);
    let author_b = user(0xb2);
    for (node_base, changed_owner, expected) in [
        (1, author_b, Fate::Accepted),
        (
            5,
            author_a,
            Fate::Rejected(RejectionReason::ExclusiveConflict),
        ),
    ] {
        let schema = owner_policy_schema();
        let (_client_dir, mut client) = open_node_with_schema(node(node_base), schema.clone());
        let (_other_dir, mut other) = open_node_with_schema(node(node_base + 1), schema.clone());
        let (_core_dir, mut core) = open_node_with_schema(node(node_base + 2), schema.clone());
        install_test_uuid_sub_claim(&mut client, author_a);
        install_test_uuid_sub_claim(&mut core, author_a);
        let shape = crate::query::Query::from("todos")
            .filter(crate::query::eq(
                crate::query::col("owner"),
                crate::query::param("owner"),
            ))
            .validate(&schema)
            .unwrap();
        let binding_a = shape
            .bind(BTreeMap::from([(
                "owner".to_owned(),
                Value::Uuid(author_a.test_uuid()),
            )]))
            .unwrap();
        register_shape_binding(&mut core, &shape, &binding_a);

        let tx_id = OpenTransactionId::new();
        client.open_exclusive_for_identity(tx_id, author_a).unwrap();
        assert!(
            client
                .tx_query(tx_id, &shape, &binding_a)
                .unwrap()
                .is_empty()
        );
        commit_mergeable_global(
            &mut other,
            &mut core,
            MergeableCommit::new("todos", row(node_base), 10)
                .made_by(changed_owner)
                .cells(owner_cells(changed_owner, "changed")),
        );
        client
            .tx_write(
                tx_id,
                "todos",
                row(node_base + 10),
                owner_cells(author_a, "mine"),
                None,
            )
            .unwrap();
        let (_tx_id, unit) = client
            .commit_exclusive_settled(tx_id, author_a, 11)
            .unwrap();
        let [fate] = core
            .apply_sync_message_settled(unit)
            .unwrap()
            .try_into()
            .unwrap();
        let SyncMessage::FateUpdate { fate, .. } = fate else {
            panic!("expected fate update");
        };
        assert_eq!(fate, expected);
    }
}
#[test]
fn exclusive_shape_predicate_validation_uses_inline_shape_without_registration() {
    let author_a = user(0xa1);
    let schema = owner_policy_schema();
    let (_client_dir, mut client) = open_node_with_schema(node(1), schema.clone());
    let (_other_dir, mut other) = open_node_with_schema(node(2), schema.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema.clone());
    install_test_uuid_sub_claim(&mut client, author_a);
    install_test_uuid_sub_claim(&mut core, author_a);
    let shape = crate::query::Query::from("todos")
        .filter(crate::query::eq(
            crate::query::col("owner"),
            crate::query::param("owner"),
        ))
        .validate(&schema)
        .unwrap();
    let binding_a = shape
        .bind(BTreeMap::from([(
            "owner".to_owned(),
            Value::Uuid(author_a.test_uuid()),
        )]))
        .unwrap();

    let tx_id = OpenTransactionId::new();
    client.open_exclusive_for_identity(tx_id, author_a).unwrap();
    assert!(
        client
            .tx_query(tx_id, &shape, &binding_a)
            .unwrap()
            .is_empty()
    );
    commit_mergeable_global(
        &mut other,
        &mut core,
        MergeableCommit::new("todos", row(1), 10)
            .made_by(author_a)
            .cells(owner_cells(author_a, "phantom")),
    );
    client
        .tx_write(tx_id, "todos", row(2), owner_cells(author_a, "mine"), None)
        .unwrap();
    let (_tx_id, unit) = client
        .commit_exclusive_settled(tx_id, author_a, 11)
        .unwrap();
    let [fate] = core
        .apply_sync_message_settled(unit)
        .unwrap()
        .try_into()
        .unwrap();
    let SyncMessage::FateUpdate { fate, .. } = fate else {
        panic!("expected fate update");
    };
    assert_eq!(fate, Fate::Rejected(RejectionReason::ExclusiveConflict));
}
#[test]
fn district_scoped_predicate_rejects_same_district_phantom_only() {
    fn orders_schema() -> JazzSchema {
        build_public_test_schema(
            PublicSchemaBuilder::new().table(
                PublicTableSchemaBuilder::new("orders")
                    .column("district", PublicColumnType::Uuid)
                    .column("orderNumber", PublicColumnType::Timestamp)
                    .column("delivered", PublicColumnType::Boolean),
            ),
        )
    }

    fn order_cells(
        district: RowUuid,
        order_number: u64,
        delivered: bool,
    ) -> BTreeMap<String, Value> {
        BTreeMap::from([
            ("district".to_owned(), Value::Uuid(district.0)),
            ("orderNumber".to_owned(), Value::U64(order_number)),
            ("delivered".to_owned(), Value::Bool(delivered)),
        ])
    }

    for (node_base, phantom_district, expected) in [
        (
            1,
            row(0xd1),
            Fate::Rejected(RejectionReason::ExclusiveConflict),
        ),
        (5, row(0xd2), Fate::Accepted),
    ] {
        let schema = orders_schema();
        let (_client_dir, mut client) = open_node_with_schema(node(node_base), schema.clone());
        let (_other_dir, mut other) = open_node_with_schema(node(node_base + 1), schema.clone());
        let (_core_dir, mut core) = open_node_with_schema(node(node_base + 2), schema.clone());
        let target_district = row(0xd1);
        let shape = Query::from("orders")
            .filter(eq(col("district"), param("district")))
            .filter(eq(col("delivered"), lit(Value::Bool(false))))
            .validate(&schema)
            .unwrap();
        let binding = shape
            .bind(BTreeMap::from([(
                "district".to_owned(),
                Value::Uuid(target_district.0),
            )]))
            .unwrap();

        let tx_id = OpenTransactionId::new();
        client.open_exclusive(tx_id).unwrap();
        assert!(client.tx_query(tx_id, &shape, &binding).unwrap().is_empty());
        commit_mergeable_global(
            &mut other,
            &mut core,
            MergeableCommit::new("orders", row(node_base), 10).cells(order_cells(
                phantom_district,
                1,
                false,
            )),
        );
        client
            .tx_write(
                tx_id,
                "orders",
                row(node_base + 10),
                order_cells(target_district, 2, true),
                None,
            )
            .unwrap();
        let (_tx_id, unit) = client
            .commit_exclusive_settled(tx_id, AuthorSubject::SYSTEM, 11)
            .unwrap();
        let [fate] = core
            .apply_sync_message_settled(unit)
            .unwrap()
            .try_into()
            .unwrap();
        let SyncMessage::FateUpdate { fate, .. } = fate else {
            panic!("expected fate update");
        };
        assert_eq!(fate, expected);
    }
}
#[test]
fn exclusive_write_write_first_committer_wins() {
    let (_client_a_dir, mut client_a) = open_node_with_uuid(node(1));
    let (_client_b_dir, mut client_b) = open_node_with_uuid(node(2));
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    let row = row(7);
    commit_mergeable_global(
        &mut client_a,
        &mut core,
        MergeableCommit::new("todos", row, 10).cells(title_cells("base")),
    );
    sync_current_rows_to(&mut core, &mut client_b, 42);
    let tx_a = OpenTransactionId::new();
    client_a.open_exclusive(tx_a).unwrap();
    let tx_b = OpenTransactionId::new();
    client_b.open_exclusive(tx_b).unwrap();
    client_a
        .tx_write(tx_a, "todos", row, title_cells("a"), None)
        .unwrap();
    client_b
        .tx_write(tx_b, "todos", row, title_cells("b"), None)
        .unwrap();
    let (_a_ref, unit_a) = client_a
        .commit_exclusive_settled(tx_a, AuthorSubject::SYSTEM, 11)
        .unwrap();
    let (_b_ref, unit_b) = client_b
        .commit_exclusive_settled(tx_b, AuthorSubject::SYSTEM, 11)
        .unwrap();
    let [fate_a] = core
        .apply_sync_message_settled(unit_a)
        .unwrap()
        .try_into()
        .unwrap();
    let [fate_b] = core
        .apply_sync_message_settled(unit_b)
        .unwrap()
        .try_into()
        .unwrap();
    let SyncMessage::FateUpdate { fate: accepted, .. } = fate_a else {
        panic!("expected fate update");
    };
    let SyncMessage::FateUpdate { fate: rejected, .. } = fate_b else {
        panic!("expected fate update");
    };
    assert_eq!(accepted, Fate::Accepted);
    assert_eq!(rejected, Fate::Rejected(RejectionReason::ExclusiveConflict));
}
#[test]
fn exclusive_absent_read_conflict_rejects() {
    let (_client_dir, mut client) = open_node_with_uuid(node(1));
    let (_other_dir, mut other) = open_node_with_uuid(node(2));
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    let row = row(7);
    let tx_id = OpenTransactionId::new();
    client.open_exclusive(tx_id).unwrap();
    assert_eq!(client.tx_read(tx_id, "todos", row).unwrap(), None);
    commit_mergeable_global(
        &mut other,
        &mut core,
        MergeableCommit::new("todos", row, 10).cells(BTreeMap::from([(
            "title".to_owned(),
            "inserted".to_owned(),
        )])),
    );
    client
        .tx_write(tx_id, "todos", row, title_cells("mine"), None)
        .unwrap();
    let (_tx_id, unit) = client
        .commit_exclusive_settled(tx_id, AuthorSubject::SYSTEM, 11)
        .unwrap();
    let [fate] = core
        .apply_sync_message_settled(unit)
        .unwrap()
        .try_into()
        .unwrap();
    let SyncMessage::FateUpdate { fate, .. } = fate else {
        panic!("expected fate update");
    };
    assert_eq!(fate, Fate::Rejected(RejectionReason::ExclusiveConflict));
}
#[test]
fn commit_unit_forward_skew_rejects_and_client_cleans_up() {
    let (_client_dir, mut client) = open_node_with_uuid(node(1));
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    let row = row(7);
    let (tx_id, unit) = client
        .commit_mergeable_unit_settled(
            MergeableCommit::new("todos", row, SKEW_TOLERANCE_MS + 1).cells(title_cells("future")),
        )
        .unwrap();
    let SyncMessage::CommitUnit { tx, versions } = unit else {
        panic!("expected commit unit");
    };
    let [fate] = core
        .ingest_commit_unit_settled(tx, versions, 0)
        .unwrap()
        .try_into()
        .unwrap();
    let SyncMessage::FateUpdate { fate: rejected, .. } = &fate else {
        panic!("expected fate update");
    };
    assert_eq!(
        rejected,
        &Fate::Rejected(RejectionReason::ClientClockTooFarAhead)
    );
    assert_eq!(
        core.transaction_state_settled(tx_id).unwrap().0,
        Fate::Rejected(RejectionReason::ClientClockTooFarAhead)
    );
    assert!(
        core.current_rows("todos", DurabilityTier::Local)
            .unwrap()
            .is_empty()
    );

    client.apply_sync_message_settled(fate).unwrap();
    assert_eq!(
        client.transaction_state_settled(tx_id).unwrap().0,
        Fate::Rejected(RejectionReason::ClientClockTooFarAhead)
    );
    assert!(
        client
            .current_rows("todos", DurabilityTier::Local)
            .unwrap()
            .is_empty()
    );
}

fn register_shape_binding_for_receiver(
    node: &mut crate::node::NodeState<RocksDbStorage>,
    shape: &crate::query::ValidatedQuery,
    binding: &crate::query::Binding,
) {
    node.apply_sync_message_settled(SyncMessage::RegisterShape {
        shape_id: shape.shape_id(),
        ast: crate::protocol::ShapeAst::from_validated(shape),
        opts: crate::protocol::RegisterShapeOptions::default(),
    })
    .unwrap();
    let values = shape
        .params()
        .keys()
        .map(|name| binding.values().get(name).cloned().unwrap())
        .collect();
    node.apply_subscribe_with_admitted_policy_binding(
        crate::protocol::Subscribe {
            shape_id: shape.shape_id(),
            subscription: crate::protocol::SubscriptionKey {
                shape_id: shape.shape_id(),
                binding_id: binding.binding_id(),
                read_view: Default::default(),
            },
            values,
            known_state: None,
            delegated_session: None,
        },
        crate::protocol::PolicyBindingKey::from_canonical_parts(
            AuthorSubject::SYSTEM,
            BTreeMap::new(),
        ),
    )
    .unwrap();
}

#[test]
fn receiver_tracks_partial_exclusive_payload_coverage_per_view() {
    let (_writer_dir, mut writer) = open_node_with_uuid(node(1));
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    let (_reader_dir, mut reader) = open_node_with_uuid(node(3));
    let shape = Query::from("todos")
        .filter(eq(col("title"), lit("one")))
        .validate(&schema())
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();

    let tx = OpenTransactionId::new();
    writer.open_exclusive(tx).unwrap();
    writer
        .tx_write(tx, "todos", row(1), title_cells("one"), None)
        .unwrap();
    writer
        .tx_write(tx, "todos", row(2), title_cells("two"), None)
        .unwrap();
    let (_tx_id, unit) = writer
        .commit_exclusive_settled(tx, AuthorSubject::SYSTEM, 10)
        .unwrap();
    let [fate] = core
        .apply_sync_message_settled(unit)
        .unwrap()
        .try_into()
        .unwrap();
    assert!(matches!(
        fate,
        SyncMessage::FateUpdate {
            fate: Fate::Accepted,
            ..
        }
    ));

    let mut peer = relay_with_system_binding(crate::protocol::SubscriptionKey {
        shape_id: shape.shape_id(),
        binding_id: binding.binding_id(),
        read_view: Default::default(),
    });
    let update = peer.rehydrate_query(&mut core, &shape, &binding).unwrap();
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
    assert_eq!(version_bundles.len(), 1);
    let bundle = version_bundles.pop().unwrap();
    assert_eq!(bundle.tx.kind, TxKind::Exclusive);
    assert_eq!(bundle.tx.n_total_writes, 1);
    assert_eq!(
        bundle.scope,
        crate::protocol::VersionBundleScope::ViewScoped
    );
    assert_eq!(bundle.versions.len(), 1);
    assert_eq!(bundle.versions[0].row_uuid(), row(1));
    assert!(
        program_fact_adds
            .added_rows().iter()
            .any(|fact| { matches!(fact, input if input.row == row(1)) })
    );
    assert!(peer.shipped_complete_tx_payloads().is_empty());

    let tx_id = bundle.tx.tx_id;

    register_shape_binding_for_receiver(&mut reader, &shape, &binding);
    reader
        .apply_sync_message_settled(SyncMessage::ViewUpdate(
            crate::protocol::ViewUpdatePayload {
                subscription,
                settled_through,

                version_carriers: vec![VersionCarrier::Bundle(bundle)],
                peer_payload_inventory: crate::protocol::PeerPayloadInventory::default(),
                supporting_rows: program_fact_adds,
            },
        ))
        .unwrap();
    assert!(
        reader
            .current_rows("todos", DurabilityTier::Global)
            .unwrap()
            .is_empty()
    );
    assert!(
        reader
            .subscription_current_rows("todos", DurabilityTier::Global)
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        reader
            .query_rows_for_client(
                &shape,
                &binding,
                DurabilityTier::Global,
                AuthorSubject::SYSTEM
            )
            .unwrap(),
        vec![(row(1), title_cells("one"))]
    );

    // The maintained-view producer must preserve the receiver's partial
    // cardinality when it relays this selected row downstream.
    let stored = reader.query_transaction(tx_id).unwrap().unwrap();
    let versions = reader.query_versions_for_tx(tx_id).unwrap();
    let downstream_bundle = reader
        .version_bundle_for_maintained_view_versions_with_tx(&stored, &versions)
        .unwrap();
    assert_eq!(
        downstream_bundle.scope,
        crate::protocol::VersionBundleScope::ViewScoped
    );
    assert_eq!(downstream_bundle.tx.n_total_writes, 1);
}

#[test]
fn malformed_exclusive_partial_covered_input_is_rejected() {
    let (_writer_dir, mut writer) = open_node_with_uuid(node(1));
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    let (_reader_dir, mut reader) = open_node_with_uuid(node(3));
    let shape = Query::from("todos")
        .filter(eq(col("title"), lit("one")))
        .validate(&schema())
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();

    let tx = OpenTransactionId::new();
    writer.open_exclusive(tx).unwrap();
    writer
        .tx_write(tx, "todos", row(1), title_cells("one"), None)
        .unwrap();
    writer
        .tx_write(tx, "todos", row(2), title_cells("two"), None)
        .unwrap();
    let (_tx_id, unit) = writer
        .commit_exclusive_settled(tx, AuthorSubject::SYSTEM, 10)
        .unwrap();
    let [fate] = core
        .apply_sync_message_settled(unit)
        .unwrap()
        .try_into()
        .unwrap();
    assert!(matches!(
        fate,
        SyncMessage::FateUpdate {
            fate: Fate::Accepted,
            ..
        }
    ));

    let mut peer = relay_with_system_binding(crate::protocol::SubscriptionKey {
        shape_id: shape.shape_id(),
        binding_id: binding.binding_id(),
        read_view: Default::default(),
    });
    let update = peer.rehydrate_query(&mut core, &shape, &binding).unwrap();
    let version_bundles = version_bundles_for_update(&update);
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
    assert_eq!(version_bundles.len(), 1);
    assert_eq!(version_bundles[0].versions.len(), 1);
    assert_eq!(version_bundles[0].versions[0].row_uuid(), row(1));

    let mut malformed_facts = program_fact_adds;
    let malformed_input = malformed_facts
        .added_rows_mut()
        .first_mut()
        .expect("rehydration must disclose its root source input");
    malformed_input.row = row(2);

    register_shape_binding_for_receiver(&mut reader, &shape, &binding);
    let err = reader
        .apply_sync_message_settled(SyncMessage::ViewUpdate(
            crate::protocol::ViewUpdatePayload {
                subscription,
                settled_through,

                version_carriers: crate::protocol::build_version_carriers_from_singletons(
                    version_bundles,
                )
                .unwrap(),
                peer_payload_inventory: crate::protocol::PeerPayloadInventory::default(),
                supporting_rows: malformed_facts,
            },
        ))
        .unwrap_err();

    assert!(matches!(
        err,
        Error::InvalidAuthoritySourceClosure { subscription: rejected, transition }
            if rejected == subscription
                && transition == "covered input is not witnessed by admitted payload"
    ));
    assert!(
        reader
            .query_rows_for_client(
                &shape,
                &binding,
                DurabilityTier::Global,
                AuthorSubject::SYSTEM
            )
            .unwrap()
            .is_empty()
    );
}

#[test]
fn partial_exclusive_payload_does_not_establish_tx_level_complete_tx_ref() {
    let (_writer_dir, mut writer) = open_node_with_uuid(node(1));
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    let first_shape = Query::from("todos")
        .filter(eq(col("title"), lit("one")))
        .validate(&schema())
        .unwrap();
    let first_binding = first_shape.bind(BTreeMap::new()).unwrap();
    let second_shape = Query::from("todos")
        .filter(eq(col("title"), lit("two")))
        .validate(&schema())
        .unwrap();
    let second_binding = second_shape.bind(BTreeMap::new()).unwrap();

    let tx = OpenTransactionId::new();
    writer.open_exclusive(tx).unwrap();
    writer
        .tx_write(tx, "todos", row(1), title_cells("one"), None)
        .unwrap();
    writer
        .tx_write(tx, "todos", row(2), title_cells("two"), None)
        .unwrap();
    let (tx_id, unit) = writer
        .commit_exclusive_settled(tx, AuthorSubject::SYSTEM, 10)
        .unwrap();
    let [fate] = core
        .apply_sync_message_settled(unit)
        .unwrap()
        .try_into()
        .unwrap();
    assert!(matches!(
        fate,
        SyncMessage::FateUpdate {
            fate: Fate::Accepted,
            ..
        }
    ));

    let mut peer = PeerState::new();
    let first = peer
        .rehydrate_query(&mut core, &first_shape, &first_binding)
        .unwrap();
    let version_bundles = version_bundles_for_update(&first);
    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        peer_payload_inventory:
            crate::protocol::PeerPayloadInventory {
                complete_tx_payloads: complete_tx_payload_refs,
                ..
            },
        ..
    }) = first
    else {
        panic!("expected view update");
    };
    assert_eq!(version_bundles.len(), 1);
    assert_eq!(version_bundles[0].tx.tx_id, tx_id);
    assert_eq!(version_bundles[0].versions.len(), 1);
    assert!(complete_tx_payload_refs.is_empty());
    assert!(peer.shipped_complete_tx_payloads().is_empty());

    let second = peer
        .rehydrate_query(&mut core, &second_shape, &second_binding)
        .unwrap();
    let version_bundles = version_bundles_for_update(&second);
    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        peer_payload_inventory:
            crate::protocol::PeerPayloadInventory {
                complete_tx_payloads: complete_tx_payload_refs,
                ..
            },
        ..
    }) = second
    else {
        panic!("expected view update");
    };
    assert_eq!(version_bundles.len(), 1);
    assert_eq!(version_bundles[0].tx.tx_id, tx_id);
    assert_eq!(version_bundles[0].versions.len(), 1);
    assert!(complete_tx_payload_refs.is_empty());
    assert!(peer.shipped_complete_tx_payloads().is_empty());
}
#[test]
fn exclusive_view_shipping_is_view_atomic_per_recipient() {
    let schema = owner_policy_schema();
    let (_writer_dir, mut writer) = open_node_with_schema(node(1), schema.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema.clone());
    let (_reader_a_dir, mut reader_a) = open_node_with_schema(node(3), schema.clone());
    let (_reader_system_dir, mut reader_system) = open_node_with_schema(node(4), schema);
    register_whole_table_receiver(&mut reader_a, "todos");
    register_whole_table_receiver(&mut reader_system, "todos");
    let author_a = user(0xa1);
    let author_b = user(0xb2);
    install_test_uuid_sub_claim(&mut core, author_a);
    install_test_uuid_sub_claim(&mut core, author_b);

    let tx = OpenTransactionId::new();
    writer.open_exclusive(tx).unwrap();
    writer
        .tx_write(tx, "todos", row(1), owner_cells(author_a, "a row"), None)
        .unwrap();
    writer
        .tx_write(tx, "todos", row(2), owner_cells(author_b, "b row"), None)
        .unwrap();
    let (_tx_id, unit) = writer
        .commit_exclusive_settled(tx, AuthorSubject::SYSTEM, 10)
        .unwrap();
    let [fate] = core
        .apply_sync_message_settled(unit)
        .unwrap()
        .try_into()
        .unwrap();
    assert!(matches!(
        fate,
        SyncMessage::FateUpdate {
            fate: Fate::Accepted,
            ..
        }
    ));

    let mut link_a = PeerState::client_link(author_a);
    let update_a = link_a.current_rows_update(&mut core, "todos").unwrap();
    let version_bundles = version_bundles_for_update(&update_a);
    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        supporting_rows: program_fact_adds,
        ..
    }) = &update_a
    else {
        panic!("expected view update");
    };
    assert_eq!(version_bundles.len(), 1);
    assert_eq!(version_bundles[0].tx.kind, TxKind::Exclusive);
    assert_eq!(version_bundles[0].tx.n_total_writes, 1);
    assert_eq!(
        version_bundles[0].scope,
        crate::protocol::VersionBundleScope::ViewScoped
    );
    assert_eq!(version_bundles[0].versions.len(), 1);
    assert_eq!(version_bundles[0].versions[0].row_uuid(), row(1));
    assert_eq!(
        program_fact_adds
            .added_rows().iter()
            .map(|input| (input.version_table.clone(), input.row, input.version.tx))
            .collect::<Vec<_>>(),
        vec![(
            "todos".to_owned().into(),
            row(1),
            version_bundles[0].tx.tx_id
        )]
    );
    assert!(link_a.shipped_complete_tx_payloads().is_empty());
    reader_a.apply_sync_message_settled(update_a).unwrap();
    assert_eq!(
        reader_a
            .subscription_current_rows("todos", DurabilityTier::Global)
            .unwrap(),
        vec![(row(1), owner_cells(author_a, "a row"))]
    );

    let mut link_system = PeerState::new();
    let update_system = link_system.current_rows_update(&mut core, "todos").unwrap();
    reader_system
        .apply_sync_message_settled(update_system)
        .unwrap();
    assert_eq!(
        reader_system
            .subscription_current_rows("todos", DurabilityTier::Global)
            .unwrap(),
        vec![
            (row(1), owner_cells(author_a, "a row")),
            (row(2), owner_cells(author_b, "b row")),
        ]
    );
}
#[test]
fn originating_rejected_exclusive_moves_payload_to_retry_store() {
    let (_writer_a_dir, mut writer_a) = open_node_with_uuid(node(1));
    let (writer_b_dir, mut writer_b) = open_node_with_uuid(node(2));
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    let row = row(7);

    commit_mergeable_global(
        &mut writer_a,
        &mut core,
        MergeableCommit::new("todos", row, 10).cells(title_cells("base")),
    );
    sync_current_rows_to(&mut core, &mut writer_b, 77);
    let tx_id = OpenTransactionId::new();
    writer_b.open_exclusive(tx_id).unwrap();
    writer_b.tx_read(tx_id, "todos", row).unwrap();
    commit_mergeable_global(
        &mut writer_a,
        &mut core,
        MergeableCommit::new("todos", row, 11).cells(BTreeMap::from([(
            "title".to_owned(),
            "intervening".to_owned(),
        )])),
    );
    writer_b
        .tx_write(tx_id, "todos", row, title_cells("retry me"), None)
        .unwrap();
    let (rejected, unit) = writer_b
        .commit_exclusive_settled(tx_id, AuthorSubject::SYSTEM, 12)
        .unwrap();
    let [fate] = core
        .apply_sync_message_settled(unit)
        .unwrap()
        .try_into()
        .unwrap();
    assert_eq!(
        fate,
        SyncMessage::FateUpdate {
            tx_id: rejected,
            fate: Fate::Rejected(RejectionReason::ExclusiveConflict),
            global_time: None,
            durability: None,
        }
    );
    assert!(core.rejected_transaction(rejected).is_none());
    writer_b.apply_sync_message_settled(fate).unwrap();

    assert_eq!(writer_b.rejected_transactions(), vec![rejected]);
    let stored = writer_b.rejected_transaction(rejected).unwrap();
    assert_eq!(stored.reason(), RejectionReason::ExclusiveConflict);
    assert_eq!(stored.cascade_root(), None);
    assert_eq!(stored.kind(), TxKind::Exclusive);
    assert_eq!(stored.versions().len(), 1);
    assert_eq!(stored.versions()[0].table(), "todos");
    assert_eq!(stored.versions()[0].row_uuid(), row);
    assert_eq!(
        stored.versions()[0].test_cells(&schema().tables[0]),
        title_cells("retry me")
    );
    assert!(
        writer_b
            .row_history("todos", row)
            .unwrap()
            .iter()
            .all(|entry| entry.tx_id() != rejected)
    );
    assert!(
        writer_b
            .current_rows("todos", DurabilityTier::Local)
            .unwrap()
            .iter()
            .all(|row| row.cell(&schema().tables[0], "title") != Some(v("retry me")))
    );

    drop(writer_b);
    let mut reopened = reopen_node_at(&writer_b_dir, node(2), schema());
    assert_eq!(
        reopened.rejected_transaction(rejected).unwrap().versions(),
        stored.versions()
    );
    reopened.discard_rejection(rejected).unwrap();
    assert!(reopened.rejected_transaction(rejected).is_none());
    drop(reopened);
    let reopened = reopen_node_at(&writer_b_dir, node(2), schema());
    assert!(reopened.rejected_transaction(rejected).is_none());
}
