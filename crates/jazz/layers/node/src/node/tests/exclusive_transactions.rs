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
    assert_eq!(versions[0].parents(), vec![base]);
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
    assert!(versions[0].parents().is_empty());

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
    assert_eq!(versions.len(), 2);
    let content = versions
        .iter()
        .find(|version| version.deletion().is_none())
        .unwrap();
    let restore = versions
        .iter()
        .find(|version| version.deletion() == Some(DeletionEvent::Restored))
        .unwrap();
    // Planted sensitivity: dropping content ancestry because the row was
    // hidden makes authority CAS compare Some(C) with None and reject.
    assert_eq!(content.parents(), vec![content_parent]);
    assert_eq!(restore.parents(), vec![deletion_parent]);

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
fn exclusive_pending_duplicates_require_complete_evidence_and_versions() {
    let (_client_dir, mut client) = open_node_with_uuid(node(1));
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    let shape = crate::query::Query::from("todos")
        .filter(crate::query::eq(
            crate::query::col("title"),
            crate::query::lit("watched"),
        ))
        .validate(&schema())
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let open = OpenTransactionId::new();
    client.open_exclusive(open).unwrap();
    client.tx_query(open, &shape, &binding).unwrap();
    client
        .tx_write(open, "todos", row(2), title_cells("mine"), None)
        .unwrap();
    let (_, unit) = client
        .commit_exclusive_settled(open, AuthorSubject::SYSTEM, 10)
        .unwrap();
    let SyncMessage::CommitUnit { tx, versions } = unit else {
        panic!("expected commit unit");
    };
    let repeated = client
        .ingest_commit_unit_settled(tx.clone(), versions.clone(), u64::MAX - SKEW_TOLERANCE_MS)
        .unwrap();
    assert!(matches!(
        &repeated[..],
        [SyncMessage::FateUpdate {
            fate: Fate::Pending,
            ..
        }]
    ));
    let mut missing_reads = tx.clone();
    missing_reads.predicate_read_set = None;
    let mut different_versions = versions.clone();
    different_versions[0] = version_record(row(2), Vec::new(), title_cells("substituted"), None);
    for (altered_tx, altered_versions) in [
        (missing_reads, versions.clone()),
        (tx.clone(), different_versions.clone()),
    ] {
        assert!(matches!(
            client.ingest_commit_unit_settled(
                altered_tx,
                altered_versions,
                u64::MAX - SKEW_TOLERANCE_MS,
            ),
            Err(Error::ConflictingCommitUnit(id)) if id == tx.tx_id
        ));
    }
    let (_partial_core_dir, mut partial_core) = open_node_with_uuid(node(8));
    let mut partial = tx.clone();
    partial.predicate_read_set = None;
    assert!(matches!(
        partial_core.ingest_commit_unit_settled(
            partial,
            versions.clone(),
            u64::MAX - SKEW_TOLERANCE_MS,
        ),
        Err(Error::InvalidStoredValue(_))
    ));
    assert!(partial_core.query_transaction(tx.tx_id).unwrap().is_none());
    assert!(partial_core.transaction_state_settled(tx.tx_id).is_none());
    assert!(
        partial_core
            .query_table_versions("todos")
            .unwrap()
            .is_empty()
    );
    assert!(
        partial_core
            .current_rows("todos", DurabilityTier::Global)
            .unwrap()
            .is_empty()
    );
    let accepted_after_partial = partial_core
        .ingest_commit_unit_settled(tx.clone(), versions.clone(), u64::MAX - SKEW_TOLERANCE_MS)
        .unwrap();
    assert!(matches!(
        &accepted_after_partial[..],
        [SyncMessage::FateUpdate {
            tx_id,
            fate: Fate::Accepted,
            ..
        }] if *tx_id == tx.tx_id
    ));
    assert_eq!(
        partial_core
            .current_rows("todos", DurabilityTier::Global)
            .unwrap()
            .iter()
            .map(CurrentRow::row_uuid)
            .collect::<Vec<_>>(),
        vec![row(2)],
        "complete original evidence remains admissible after a malformed first submission",
    );
    let accepted = core
        .ingest_commit_unit_settled(tx.clone(), versions.clone(), u64::MAX - SKEW_TOLERANCE_MS)
        .unwrap();
    assert!(matches!(
        &accepted[..],
        [SyncMessage::FateUpdate {
            fate: Fate::Accepted,
            ..
        }]
    ));
    assert!(matches!(
        core.ingest_commit_unit_settled(tx.clone(), different_versions, u64::MAX - SKEW_TOLERANCE_MS),
        Err(Error::ConflictingCommitUnit(id)) if id == tx.tx_id
    ));
    let mut redacted = tx;
    redacted.base_snapshot = None;
    redacted.row_read_set = None;
    redacted.absent_read_set = None;
    redacted.predicate_read_set = None;
    assert_eq!(
        core.ingest_commit_unit_settled(redacted, versions, u64::MAX - SKEW_TOLERANCE_MS)
            .unwrap(),
        accepted
    );
    assert_eq!(
        core.current_rows("todos", DurabilityTier::Global)
            .unwrap()
            .into_iter()
            .map(current_row_pair)
            .collect::<BTreeMap<_, _>>(),
        BTreeMap::from([(row(2), title_cells("mine"))])
    );
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
#[test]
fn authority_parks_child_until_unknown_exclusive_parent_rejects() {
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
    assert_eq!(core.sync_metrics().parked_orphans, 1);

    let SyncMessage::CommitUnit { tx, versions } = exclusive_unit else {
        panic!("expected commit unit");
    };
    let updates = core.ingest_commit_unit_settled(tx, versions, 0).unwrap();
    assert_eq!(core.sync_metrics().parked_orphans_resolved, 1);
    assert_eq!(
        updates,
        vec![
            SyncMessage::FateUpdate {
                tx_id: exclusive,
                fate: Fate::Rejected(RejectionReason::ClientClockTooFarAhead),
                global_time: None,
                durability: None,
            },
            SyncMessage::FateUpdate {
                tx_id: child,
                fate: Fate::Rejected(RejectionReason::Cascade { root: exclusive }),
                global_time: None,
                durability: None,
            },
        ]
    );
    for update in updates {
        client.apply_sync_message_settled(update).unwrap();
    }
    assert_eq!(
        client.transaction_state_settled(exclusive).unwrap().0,
        Fate::Rejected(RejectionReason::ClientClockTooFarAhead)
    );
    assert_eq!(
        client.transaction_state_settled(child).unwrap().0,
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
}

fn register_shape_binding_for_receiver(
    node: &mut crate::node::NodeState,
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
            .added_rows()
            .iter()
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
            .added_rows()
            .iter()
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
fn exclusive_set_serializes_counter_base_before_mergeable_deltas() {
    let schema = counter_schema();
    let (_base_dir, mut base_writer) = open_node_with_schema(node(1), schema.clone());
    let (_writer_a_dir, mut writer_a) = open_node_with_schema(node(2), schema.clone());
    let (_writer_b_dir, mut writer_b) = open_node_with_schema(node(3), schema.clone());
    let (_client_dir, mut client) = open_node_with_schema(node(4), schema.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema.clone());
    let row = row(8);

    commit_mergeable_global(
        &mut base_writer,
        &mut core,
        MergeableCommit::new("counters", row, 10).cells(BTreeMap::from([
            ("count".to_owned(), Value::I32(10)),
            ("title".to_owned(), v("base")),
        ])),
    );
    let mut peer = PeerState::new();
    register_whole_table_receiver(&mut client, "counters");
    client
        .apply_sync_message_settled(peer.current_rows_update(&mut core, "counters").unwrap())
        .unwrap();

    let tx = OpenTransactionId::new();
    client.open_exclusive(tx).unwrap();
    client
        .tx_write(
            tx,
            "counters",
            row,
            BTreeMap::from([
                ("count".to_owned(), Value::I32(100)),
                ("title".to_owned(), v("exclusive")),
            ]),
            None,
        )
        .unwrap();
    let (_exclusive_tx, unit) = client
        .commit_exclusive_settled(tx, AuthorSubject::SYSTEM, 20)
        .unwrap();
    let SyncMessage::CommitUnit { tx, versions } = unit else {
        panic!("expected commit unit");
    };
    let fate_updates = core.ingest_commit_unit_settled(tx, versions, 20).unwrap();
    for update in fate_updates {
        client.apply_sync_message_settled(update).unwrap();
    }
    let exclusive = global_winner_tx(&mut core, "counters", row, VersionLayer::Content).unwrap();

    let (left, left_message) = writer_a
        .commit_mergeable_unit_settled(
            MergeableCommit::new("counters", row, 30)
                .parents(vec![exclusive])
                .cells(BTreeMap::from([("count".to_owned(), Value::I32(105))])),
        )
        .unwrap();
    let (right, right_message) = writer_b
        .commit_mergeable_unit_settled(
            MergeableCommit::new("counters", row, 31)
                .parents(vec![exclusive])
                .cells(BTreeMap::from([("count".to_owned(), Value::I32(107))])),
        )
        .unwrap();

    core.apply_sync_message_settled(left_message).unwrap();
    core.apply_sync_message_settled(right_message).unwrap();

    let merge = core
        .query_all_versions()
        .unwrap()
        .into_iter()
        .find(|version| {
            version.row_uuid() == row
                && core.version_tx_id(version).unwrap().node == node(9)
                && version.parents().contains(&left)
                && version.parents().contains(&right)
        })
        .expect("core should create a post-exclusive counter merge version");
    let cells = merge.cells(&schema.tables[0]).unwrap();
    assert_eq!(cells.get("count"), Some(&Value::I32(112)));
    assert_eq!(cells.get("title"), Some(&v("exclusive")));
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
    assert_eq!(stored.versions()[0].parents().len(), 1);
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

// Internal: the history decode counter is the only observable of how many
// history scans a transaction table read performs; results alone cannot tell
// one table-wide scan from a re-scan per row (#3473).
#[test]
fn exclusive_table_read_decodes_each_history_version_once() {
    let (_temp_dir, mut core) = open_node();
    for ordinal in 1..=16 {
        core.commit_mergeable_settled(
            MergeableCommit::new("todos", row(ordinal), u64::from(ordinal))
                .cells(title_cells(format!("first-{ordinal}"))),
        )
        .unwrap();
    }
    for ordinal in 1..=16 {
        core.commit_mergeable_settled(
            MergeableCommit::new("todos", row(ordinal), 100 + u64::from(ordinal))
                .cells(title_cells(format!("second-{ordinal}"))),
        )
        .unwrap();
    }
    core.commit_mergeable_settled(
        MergeableCommit::new("todos", row(3), 200).deletion(DeletionEvent::Deleted),
    )
    .unwrap();
    let tx_id = OpenTransactionId::new();
    core.open_exclusive(tx_id).unwrap();
    // Arrives after the snapshot, so the transaction must not see it.
    let late = TxId::new(TxTime::from(300), node(2));
    ingest_relay_version(&mut core, late, 300, Vec::new(), row(5), "late");
    core.tx_write(tx_id, "todos", row(7), title_cells("pending"), None)
        .unwrap();
    let stored_versions = 16 * 2 + 1 + 1;

    super::super::currency::HISTORY_PAYLOAD_DECODES.with(|count| count.set(0));
    let rows = core
        .tx_current_rows(tx_id, "todos")
        .unwrap()
        .into_iter()
        .map(current_row_pair)
        .collect::<Vec<_>>();
    assert_eq!(
        super::super::currency::HISTORY_PAYLOAD_DECODES.with(|count| count.get()),
        stored_versions,
        "a transaction table read must decode each stored version once"
    );

    let mut expected = Vec::new();
    for ordinal in 1..=16 {
        if let Some(cells) = core.tx_read(tx_id, "todos", row(ordinal)).unwrap() {
            expected.push((row(ordinal), cells));
        }
    }
    assert_eq!(rows, expected);
    assert!(!rows.iter().any(|(row_uuid, _)| *row_uuid == row(3)));
    assert!(rows.contains(&(row(5), title_cells("second-5"))));
    assert!(rows.contains(&(row(7), title_cells("pending"))));
}

// Differential: a whole-table read inside an exclusive transaction must equal
// per-row point reads under concurrent content heads, delete/restore,
// accepted and pending foreign versions, versions that arrive after the
// snapshot, and staged writes and deletes.
#[test]
fn exclusive_table_read_matches_point_reads_across_seeds() {
    for seed in 0..60u64 {
        let mut state = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        let mut rand = move |n: u64| {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            (state >> 33) % n
        };
        let (_temp_dir, mut core) = open_node();
        let rows = 6u8;
        let mut heads: BTreeMap<u8, Vec<TxId>> = BTreeMap::new();
        let mut time = 1u64;
        let mut global = 10_000u64;
        let mut ops = |core: &mut NodeState<_>, rand: &mut dyn FnMut(u64) -> u64, after_snapshot: bool| {
            for _ in 0..30 {
                let r = 1 + rand(rows as u64) as u8;
                time += 1;
                let known = heads.entry(r).or_default();
                let parents: Vec<TxId> = known.iter().copied().filter(|_| rand(2) == 0).collect();
                match rand(if after_snapshot { 2 } else { 4 }) {
                    0 | 1 if !after_snapshot => {
                        let mut commit = MergeableCommit::new("todos", row(r), time)
                            .cells(title_cells(format!("s{seed}-{time}")))
                            .parents(parents);
                        if rand(5) == 0 {
                            commit = commit.deletion(if rand(2) == 0 {
                                DeletionEvent::Deleted
                            } else {
                                DeletionEvent::Restored
                            });
                        }
                        if let Ok(tx) = core.commit_mergeable_settled(commit) {
                            known.push(tx);
                        }
                    }
                    _ => {
                        let tx = TxId::new(TxTime::from(time), node(2 + rand(2) as u8));
                        ingest_relay_version(core, tx, time, parents, row(r), &format!("f{seed}-{time}"));
                        if rand(2) == 0 {
                            global += 1;
                            let _ = core.apply_fate_update(
                                tx,
                                Fate::Accepted,
                                Some(GlobalTime(global)),
                                Some(DurabilityTier::Global),
                            );
                        }
                        known.push(tx);
                    }
                }
            }
        };
        ops(&mut core, &mut rand, false);
        let tx_id = OpenTransactionId::new();
        core.open_exclusive(tx_id).unwrap();
        ops(&mut core, &mut rand, true);
        for r in 1..=rows + 2 {
            match rand(4) {
                0 => core
                    .tx_write(tx_id, "todos", row(r), title_cells(format!("staged-{r}")), None)
                    .unwrap(),
                1 => {
                    let _ = core.tx_write(
                        tx_id,
                        "todos",
                        row(r),
                        BTreeMap::<String, Value>::new(),
                        Some(DeletionEvent::Deleted),
                    );
                }
                _ => {}
            }
        }
        let table = core
            .tx_current_rows(tx_id, "todos")
            .unwrap()
            .into_iter()
            .map(current_row_pair)
            .collect::<Vec<_>>();
        let mut expected = Vec::new();
        for r in 1..=rows + 2 {
            if let Some(cells) = core.tx_read(tx_id, "todos", row(r)).unwrap() {
                expected.push((row(r), cells));
            }
        }
        assert_eq!(table, expected, "seed {seed}");
    }
}

// Internal: the transaction payload decode counter is the only observable of
// how snapshot coverage is decided; results alone cannot tell a full stored
// transaction decode from a global-time projection.
#[test]
fn exclusive_point_read_and_commit_decode_no_payload_per_row_version() {
    fn decodes(edits: u64) -> usize {
        let (_temp_dir, mut core) = open_node();
        for edit in 1..=edits {
            core.commit_mergeable_settled(
                MergeableCommit::new("todos", row(1), edit).cells(title_cells(format!("edit-{edit}"))),
            )
            .unwrap();
        }
        let tx_id = OpenTransactionId::new();
        core.open_exclusive(tx_id).unwrap();
        super::super::currency::TRANSACTION_PAYLOAD_DECODES.with(|count| count.set(0));
        assert_eq!(
            core.tx_read(tx_id, "todos", row(1)).unwrap(),
            Some(title_cells(format!("edit-{edits}")))
        );
        core.tx_write(tx_id, "todos", row(1), title_cells("mine"), None)
            .unwrap();
        core.commit_exclusive_settled(tx_id, AuthorSubject::SYSTEM, 10_000)
            .unwrap();
        super::super::currency::TRANSACTION_PAYLOAD_DECODES.with(|count| count.get())
    }
    assert_eq!(
        decodes(8),
        decodes(64),
        "snapshot coverage must not decode a stored transaction per row version"
    );
}

fn watched_shape_for(title: &str) -> (ValidatedQuery, Binding) {
    let shape = crate::query::Query::from("todos")
        .filter(crate::query::eq(
            crate::query::col("title"),
            crate::query::lit(title),
        ))
        .validate(&schema())
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    (shape, binding)
}

/// garden-co/jazz#3694. A partial node's snapshot cut advances with any
/// authority receipt, not only with receipts that delivered the rows an
/// exclusive query later reads. A row the reader still holds may therefore be
/// deleted at the authority before the claimed cut: rebuilding the predicate at
/// that cut shows it absent both then and now, so only a proof of the row the
/// reader actually saw can reject the commit.
#[test]
fn exclusive_query_conflicts_when_a_returned_row_was_deleted_before_the_claimed_cut() {
    let (_client_dir, mut client) = open_node_with_uuid(node(1));
    let (_other_dir, mut other) = open_node_with_uuid(node(2));
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    let (shape, binding) = watched_shape_for("invite");
    register_shape_binding(&mut core, &shape, &binding);

    let (_insert, unit) = other
        .commit_mergeable_unit_settled(
            MergeableCommit::new("todos", row(1), 10).cells(title_cells("invite")),
        )
        .unwrap();
    let [fate] = core
        .apply_sync_message_settled(unit.clone())
        .unwrap()
        .try_into()
        .unwrap();
    other.apply_sync_message_settled(fate.clone()).unwrap();
    client.apply_sync_message_settled(unit).unwrap();
    client.apply_sync_message_settled(fate).unwrap();

    // The revocation reaches the authority but never this reader.
    commit_mergeable_global(
        &mut other,
        &mut core,
        MergeableCommit::new("todos", row(1), 12).deletion(DeletionEvent::Deleted),
    );
    // An unrelated receipt settles through the revocation's global time.
    client.record_authoritative_settled_through(core.clock.committed_global_time);

    let open = OpenTransactionId::new();
    client.open_exclusive(open).unwrap();
    assert_eq!(client.tx_query(open, &shape, &binding).unwrap().len(), 1);
    client
        .tx_write(open, "todos", row(2), title_cells("member"), None)
        .unwrap();
    let (_tx_id, unit) = client
        .commit_exclusive_settled(open, AuthorSubject::SYSTEM, 13)
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

/// Whole-table reads validate by table currency after the claimed cut, so they
/// need the same per-row proof as filtered queries (garden-co/jazz#3694).
#[test]
fn exclusive_table_read_conflicts_when_a_returned_row_was_deleted_before_the_claimed_cut() {
    let (_client_dir, mut client) = open_node_with_uuid(node(1));
    let (_other_dir, mut other) = open_node_with_uuid(node(2));
    let (_core_dir, mut core) = open_node_with_uuid(node(9));

    let (_insert, unit) = other
        .commit_mergeable_unit_settled(
            MergeableCommit::new("todos", row(1), 10).cells(title_cells("invite")),
        )
        .unwrap();
    let [fate] = core
        .apply_sync_message_settled(unit.clone())
        .unwrap()
        .try_into()
        .unwrap();
    other.apply_sync_message_settled(fate.clone()).unwrap();
    client.apply_sync_message_settled(unit).unwrap();
    client.apply_sync_message_settled(fate).unwrap();

    commit_mergeable_global(
        &mut other,
        &mut core,
        MergeableCommit::new("todos", row(1), 12).deletion(DeletionEvent::Deleted),
    );
    client.record_authoritative_settled_through(core.clock.committed_global_time);

    let open = OpenTransactionId::new();
    client.open_exclusive(open).unwrap();
    assert_eq!(client.tx_current_rows(open, "todos").unwrap().len(), 1);
    client
        .tx_write(open, "todos", row(2), title_cells("member"), None)
        .unwrap();
    let (_tx_id, unit) = client
        .commit_exclusive_settled(open, AuthorSubject::SYSTEM, 13)
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

/// Todos readable only while they have a member, plus a table any exclusive
/// transaction can write.
fn member_visible_todos_schema() -> JazzSchema {
    build_public_test_schema(
        PublicSchemaBuilder::new()
            .table(
                PublicTableSchemaBuilder::new("todos")
                    .column("title", PublicColumnType::Text)
                    .policies(PublicTablePolicies::new().with_select(public_outer_exists(
                        "members",
                        "owner",
                        "id",
                        [],
                    ))),
            )
            .table(
                PublicTableSchemaBuilder::new("members")
                    .fk_column("owner", "todos")
                    .column("user", PublicColumnType::Text),
            )
            .table(
                PublicTableSchemaBuilder::new("audit")
                    .column("title", PublicColumnType::Text)
                    .policies(public_all_policies()),
            ),
    )
}

fn todo_member_cells(owner: RowUuid) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("owner".to_owned(), Value::Uuid(owner.0)),
        ("user".to_owned(), Value::String("member".to_owned())),
    ])
}

/// garden-co/jazz#3694: an exclusive read behind a membership read policy
/// records the rows it returned, not the membership table. The authority
/// re-runs the read under the same policy, so a membership change conflicts
/// exactly when it changes what the reader sees.
fn exclusive_policy_read_fate(
    read_all_open: bool,
    change: impl FnOnce(&mut NodeState, &mut NodeState),
) -> Fate {
    exclusive_policy_read_fate_with(
        if read_all_open { PolicyRead::AllOpen } else { PolicyRead::Point },
        change,
    )
}

#[derive(Clone, Copy, Debug)]
enum PolicyRead {
    Point,
    AllOpen,
    JoinedFromAudit,
}

fn exclusive_policy_read_fate_with(
    read: PolicyRead,
    change: impl FnOnce(&mut NodeState, &mut NodeState),
) -> Fate {
    let schema = member_visible_todos_schema();
    let (_client_dir, mut client) = open_node_with_schema(node(1), schema.clone());
    let (_other_dir, mut other) = open_node_with_schema(node(2), schema.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema.clone());
    let reader = user(0x51);
    for (todo, title) in [(row(1), "visible"), (row(2), "hidden"), (row(3), "done")] {
        commit_mergeable_global(
            &mut client,
            &mut core,
            MergeableCommit::new("todos", todo, 10).cells(title_cells(title)),
        );
    }
    commit_mergeable_global(
        &mut client,
        &mut core,
        MergeableCommit::new("members", row(0x61), 11).cells(todo_member_cells(row(1))),
    );
    for (entry, title) in [(row(0x81), "visible"), (row(0x82), "hidden")] {
        commit_mergeable_global(
            &mut client,
            &mut core,
            MergeableCommit::new("audit", entry, 11).cells(title_cells(title)),
        );
    }
    commit_mergeable_global(
        &mut client,
        &mut core,
        MergeableCommit::new("members", row(0x63), 12).cells(todo_member_cells(row(3))),
    );

    let shape = match read {
        PolicyRead::AllOpen => {
            Query::from("todos").filter(ne(col("title"), lit(Value::String("done".to_owned()))))
        }
        PolicyRead::Point => Query::from("todos").filter(eq(col("id"), lit(Value::Uuid(row(1).0)))),
        PolicyRead::JoinedFromAudit => Query::from("audit").join_via_column("todos", "title", "title", []),
    }
    .validate(&schema)
    .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let open = OpenTransactionId::new();
    client.open_exclusive_for_identity(open, reader).unwrap();
    let rows = client
        .tx_query_for_identity(open, &shape, &binding, reader)
        .unwrap();
    if !matches!(read, PolicyRead::JoinedFromAudit) {
        assert_eq!(
            rows.iter().map(CurrentRow::row_uuid).collect::<Vec<_>>(),
            vec![row(1)]
        );
        assert_eq!(
            client.open_tx(open).unwrap().row_reads.len(),
            1,
            "the membership table is not recorded"
        );
    }

    change(&mut other, &mut core);
    client
        .tx_write(open, "audit", row(0x71), title_cells("redeemed"), None)
        .unwrap();
    let (_tx_id, unit) = client.commit_exclusive_settled(open, reader, 20).unwrap();
    let [fate] = core
        .apply_sync_message_settled(unit)
        .unwrap()
        .try_into()
        .unwrap();
    let SyncMessage::FateUpdate { fate, .. } = fate else {
        panic!("expected fate update");
    };
    fate
}

#[test]
fn exclusive_policy_read_conflicts_when_the_membership_is_revoked() {
    let fate = exclusive_policy_read_fate(false, |other, core| {
        commit_mergeable_global(
            other,
            core,
            MergeableCommit::new("members", row(0x61), 15).deletion(DeletionEvent::Deleted),
        );
    });
    assert_eq!(fate, Fate::Rejected(RejectionReason::ExclusiveConflict));
}

#[test]
fn exclusive_policy_read_conflicts_when_a_grant_reveals_a_matching_row() {
    let fate = exclusive_policy_read_fate(true, |other, core| {
        commit_mergeable_global(
            other,
            core,
            MergeableCommit::new("members", row(0x62), 15).cells(todo_member_cells(row(2))),
        );
    });
    assert_eq!(fate, Fate::Rejected(RejectionReason::ExclusiveConflict));
}

#[test]
fn exclusive_policy_read_ignores_membership_changes_it_cannot_see() {
    for read_all_open in [false, true] {
        let fate = exclusive_policy_read_fate(read_all_open, |other, core| {
            // Another member of the visible todo, and a revoked membership of
            // a todo neither read returns.
            commit_mergeable_global(
                other,
                core,
                MergeableCommit::new("members", row(0x64), 15).cells(todo_member_cells(row(1))),
            );
            commit_mergeable_global(
                other,
                core,
                MergeableCommit::new("members", row(0x63), 16).deletion(DeletionEvent::Deleted),
            );
        });
        assert_eq!(fate, Fate::Accepted, "read_all_open={read_all_open}");
    }
}

/// Rows of a joined policy-protected table the reader cannot see never enter
/// the read set, so an unchanged join commits, and the rows it can see are
/// validated like any other read.
#[test]
fn exclusive_join_into_a_policy_protected_table_commits_while_unchanged() {
    for read in [PolicyRead::Point, PolicyRead::AllOpen, PolicyRead::JoinedFromAudit] {
        let fate = exclusive_policy_read_fate_with(read, |_, _| {});
        assert_eq!(fate, Fate::Accepted, "{read:?}");
    }
}

#[test]
fn exclusive_join_into_a_policy_protected_table_conflicts_when_access_is_revoked() {
    let fate = exclusive_policy_read_fate_with(PolicyRead::JoinedFromAudit, |other, core| {
        commit_mergeable_global(
            other,
            core,
            MergeableCommit::new("members", row(0x61), 15).deletion(DeletionEvent::Deleted),
        );
    });
    assert_eq!(fate, Fate::Rejected(RejectionReason::ExclusiveConflict));
}

/// The read set of a point read behind a membership policy stays one row,
/// however many memberships exist.
#[test]
fn exclusive_policy_read_set_does_not_grow_with_the_policy_table() {
    let schema = member_visible_todos_schema();
    let (_dir, mut node) = open_node_with_schema(node(1), schema.clone());
    let reader = user(0x51);
    node.commit_mergeable_settled(
        MergeableCommit::new("todos", row(1), 10).cells(title_cells("visible")),
    )
    .unwrap();
    for member in 0..500_u16 {
        let mut bytes = [0x60; 16];
        bytes[..2].copy_from_slice(&member.to_be_bytes());
        node.commit_mergeable_settled(
            MergeableCommit::new("members", RowUuid::from_bytes(bytes), 11 + u64::from(member))
                .cells(todo_member_cells(row(1))),
        )
        .unwrap();
    }
    let shape = Query::from("todos")
        .filter(eq(col("id"), lit(Value::Uuid(row(1).0))))
        .validate(&schema)
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let open = OpenTransactionId::new();
    node.open_exclusive_for_identity(open, reader).unwrap();
    assert_eq!(
        node.tx_query_for_identity(open, &shape, &binding, reader)
            .unwrap()
            .len(),
        1
    );
    let open_tx = node.open_tx(open).unwrap();
    assert_eq!(open_tx.row_reads.len(), 1);
    assert_eq!(open_tx.predicate_reads.len(), 1);
}

/// The authority validates exclusive reads as the transaction's permission
/// subject, so that must be the identity the reads ran as: the identity bound
/// at open. A commit under any other author is refused.
#[test]
fn exclusive_reads_and_their_validation_share_one_identity() {
    let schema = member_visible_todos_schema();
    let (_dir, mut node) = open_node_with_schema(node(1), schema);
    let reader = user(0x51);
    let open = OpenTransactionId::new();
    node.open_exclusive_for_identity(open, reader).unwrap();
    node.tx_write(open, "audit", row(0x71), title_cells("redeemed"), None)
        .unwrap();
    assert!(matches!(
        node.commit_exclusive_settled(open, AuthorSubject::SYSTEM, 20),
        Err(Error::OpenTransactionIdentityMismatch)
    ));
}


/// Notes and an audit log, both readable by anyone: a read policy that
/// depends on nothing but the row.
fn open_notes_schema() -> JazzSchema {
    build_public_test_schema(
        PublicSchemaBuilder::new()
            .table(
                PublicTableSchemaBuilder::new("notes")
                    .column("title", PublicColumnType::Text)
                    .policies(public_all_policies()),
            )
            .table(
                PublicTableSchemaBuilder::new("audit")
                    .column("title", PublicColumnType::Text)
                    .policies(public_all_policies()),
            ),
    )
}

#[derive(Clone, Copy, Debug)]
enum NotesTx {
    /// Read note 1 by id, then log to the audit table.
    ReadByIdThenLog,
    /// Update note 1 without reading it first.
    UpdateNote,
    /// Read every note, then update note 1.
    ReadAllThenUpdateNote,
}

/// Which client sent the transaction.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Sender {
    Current,
    /// A client before alpha.58: no row proofs for predicate reads, and each
    /// update's read-policy check of its target recorded as a whole-table
    /// read of the written table.
    PreAlpha58,
}

/// The authority's fate for an exclusive transaction over two notes, after
/// `change` commits elsewhere between the transaction's open and its commit.
fn notes_tx_fate(
    sender: Sender,
    read: NotesTx,
    change: impl FnOnce(&mut NodeState, &mut NodeState),
) -> Fate {
    let schema = open_notes_schema();
    let (_client_dir, mut client) = open_node_with_schema(node(1), schema.clone());
    let (_other_dir, mut other) = open_node_with_schema(node(2), schema.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema.clone());
    let reader = user(0x51);
    for (note, title) in [(row(1), "one"), (row(2), "two")] {
        commit_mergeable_global(
            &mut client,
            &mut core,
            MergeableCommit::new("notes", note, 10).cells(title_cells(title)),
        );
    }
    let open = OpenTransactionId::new();
    client.open_exclusive_for_identity(open, reader).unwrap();
    let query = |query: Query| {
        let shape = query.validate(&schema).unwrap();
        let binding = shape.bind(BTreeMap::new()).unwrap();
        (shape, binding)
    };
    let mut updates = 0;
    match read {
        NotesTx::ReadByIdThenLog => {
            let (shape, binding) =
                query(Query::from("notes").filter(eq(col("id"), lit(Value::Uuid(row(1).0)))));
            let rows = client
                .tx_query_for_identity(open, &shape, &binding, reader)
                .unwrap();
            assert_eq!(rows.len(), 1);
            client
                .tx_write(open, "audit", row(0x71), title_cells("read one"), None)
                .unwrap();
        }
        NotesTx::UpdateNote | NotesTx::ReadAllThenUpdateNote => {
            let read_first = match read {
                NotesTx::ReadAllThenUpdateNote => Some((Query::from("notes"), 2)),
                _ => None,
            };
            if let Some((read_first, count)) = read_first {
                let (shape, binding) = query(read_first);
                let rows = client
                    .tx_query_for_identity(open, &shape, &binding, reader)
                    .unwrap();
                assert_eq!(rows.len(), count);
            }
            // An update reads its target first, as every client does.
            client.tx_read(open, "notes", row(1)).unwrap();
            client
                .tx_write(open, "notes", row(1), title_cells("edited"), None)
                .unwrap();
            updates += 1;
        }
    }
    change(&mut other, &mut core);
    let (_tx_id, unit) = client.commit_exclusive_settled(open, reader, 20).unwrap();
    let SyncMessage::CommitUnit { mut tx, versions } = unit else {
        panic!("expected commit unit");
    };
    if sender == Sender::PreAlpha58 {
        // Only the update's own target read is a point read.
        let targets = if updates > 0 { vec![row(1)] } else { vec![] };
        tx.row_read_set
            .as_mut()
            .unwrap()
            .retain(|read| targets.contains(&read.row_uuid));
        let (shape, binding) = query(Query::from("notes"));
        for _ in 0..updates {
            tx.predicate_read_set.get_or_insert_default().push(PredicateRead {
                table: "notes".to_owned(),
                shape_id: shape.shape_id(),
                shape: shape.query().clone(),
                binding_id: binding.binding_id(),
                binding_values: binding.values().clone(),
            });
        }
    }
    let [fate] = core
        .ingest_commit_unit_settled(tx, versions, 20)
        .unwrap()
        .try_into()
        .unwrap();
    let SyncMessage::FateUpdate { fate, .. } = fate else {
        panic!("expected fate update");
    };
    fate
}

fn edit_note(note: RowUuid, title: &str) -> impl FnOnce(&mut NodeState, &mut NodeState) {
    let title = title.to_owned();
    move |other, core| {
        commit_mergeable_global(
            other,
            core,
            MergeableCommit::new("notes", note, 15).cells(title_cells(&title)),
        );
    }
}

/// A read of one row by id proves the row it returned, so it commits while
/// that row is unchanged. A client before alpha.58 proves nothing, so its
/// read conflicts while the row still exists.
#[test]
fn a_read_by_id_commits_only_with_a_row_proof() {
    let unchanged = notes_tx_fate(Sender::Current, NotesTx::ReadByIdThenLog, |_, _| {});
    assert_eq!(unchanged, Fate::Accepted);
    let other_row = notes_tx_fate(Sender::Current, NotesTx::ReadByIdThenLog, edit_note(row(2), "x"));
    assert_eq!(other_row, Fate::Accepted);
    let read_row = notes_tx_fate(Sender::Current, NotesTx::ReadByIdThenLog, edit_note(row(1), "x"));
    assert_eq!(read_row, Fate::Rejected(RejectionReason::ExclusiveConflict));
    let legacy = notes_tx_fate(Sender::PreAlpha58, NotesTx::ReadByIdThenLog, |_, _| {});
    assert_eq!(legacy, Fate::Rejected(RejectionReason::ExclusiveConflict));
}

/// An unproved read is checked only against rows that exist at validation,
/// so a read by a client before alpha.58 of a row deleted since commits.
/// Documented limitation: exclusive conflicts rely on writers reporting their
/// reads and are not a security boundary.
#[test]
fn a_pre_alpha58_read_of_a_row_deleted_since_commits() {
    let delete_note = |other: &mut NodeState, core: &mut NodeState| {
        commit_mergeable_global(
            other,
            core,
            MergeableCommit::new("notes", row(1), 15).deletion(DeletionEvent::Deleted),
        );
    };
    let legacy = notes_tx_fate(Sender::PreAlpha58, NotesTx::ReadByIdThenLog, delete_note);
    assert_eq!(legacy, Fate::Accepted);
    let current = notes_tx_fate(Sender::Current, NotesTx::ReadByIdThenLog, delete_note);
    assert_eq!(current, Fate::Rejected(RejectionReason::ExclusiveConflict));
}

/// A client before alpha.58 records an update's read-policy check of its
/// target as an unproved read of the whole table, so the update conflicts
/// while the table holds any other row.
#[test]
fn a_pre_alpha58_update_conflicts_beside_other_rows() {
    let current = notes_tx_fate(Sender::Current, NotesTx::UpdateNote, |_, _| {});
    assert_eq!(current, Fate::Accepted);
    let legacy = notes_tx_fate(Sender::PreAlpha58, NotesTx::UpdateNote, |_, _| {});
    assert_eq!(legacy, Fate::Rejected(RejectionReason::ExclusiveConflict));
}

/// A proved whole-table read beside an update commits while the table is
/// unchanged and sees a row that appeared since.
#[test]
fn a_proved_whole_table_read_beside_an_update_sees_new_rows() {
    let fate = notes_tx_fate(Sender::Current, NotesTx::ReadAllThenUpdateNote, |_, _| {});
    assert_eq!(fate, Fate::Accepted);
    let fate = notes_tx_fate(
        Sender::Current,
        NotesTx::ReadAllThenUpdateNote,
        edit_note(row(3), "new"),
    );
    assert_eq!(fate, Fate::Rejected(RejectionReason::ExclusiveConflict));
}

/// Orgs own projects, projects own todos, todos own comments and comments own
/// reactions, all readable by anyone.
fn narrowing_hierarchy_schema() -> JazzSchema {
    build_public_test_schema(
        PublicSchemaBuilder::new()
            .table(
                PublicTableSchemaBuilder::new("orgs")
                    .column("name", PublicColumnType::Text)
                    .policies(public_all_policies()),
            )
            .table(
                PublicTableSchemaBuilder::new("projects")
                    .column("name", PublicColumnType::Text)
                    .fk_column("org", "orgs")
                    .policies(public_all_policies()),
            )
            .table(
                PublicTableSchemaBuilder::new("todos")
                    .column("title", PublicColumnType::Text)
                    .fk_column("project", "projects")
                    .array_fk_column("assignees", "people")
                    .policies(public_all_policies()),
            )
            .table(
                PublicTableSchemaBuilder::new("people")
                    .column("name", PublicColumnType::Text)
                    .policies(public_all_policies()),
            )
            .table(
                PublicTableSchemaBuilder::new("comments")
                    .column("body", PublicColumnType::Text)
                    .fk_column("todo", "todos")
                    .policies(public_all_policies()),
            )
            .table(
                PublicTableSchemaBuilder::new("reactions")
                    .column("emoji", PublicColumnType::Text)
                    .fk_column("comment", "comments")
                    .policies(public_all_policies()),
            ),
    )
}

/// The join from a child row to the parent rows `on` identifies in `table`.
fn narrowing_reverse_join(
    table: &str,
    on: &str,
    child: &str,
    filters: Vec<crate::query::Predicate>,
    up: Option<crate::query::JoinVia>,
) -> crate::query::JoinVia {
    crate::query::JoinVia {
        table: table.to_owned(),
        on_column: on.to_owned(),
        target: if on == "id" {
            crate::query::JoinTarget::RowId
        } else {
            crate::query::JoinTarget::Column
        },
        source_column: Some(child.to_owned()),
        source_lookup: None,
        correlated_filters: Vec::new(),
        filters,
        nested_joins: up.into_iter().collect(),
    }
}

fn narrowing_source(table: &str, path: &[&str]) -> crate::node::query_engine::SourceId {
    use crate::node::query_engine::{SourcePath, SourceRole};
    let components = path
        .iter()
        .map(|component| match component.split_once('=') {
            Some(("child", name)) => SourceRole::CorrelatedChild(name.to_owned()),
            Some(("alias", name)) => SourceRole::Alias(name.to_owned()),
            _ => SourceRole::Root,
        })
        .collect();
    crate::node::query_engine::SourceId {
        table: table.to_owned(),
        path: SourcePath { components },
    }
}

/// Assert `query`'s narrowed reads are exactly `expected`, source by source.
fn assert_narrowed_reads(
    query: crate::query::Query,
    expected: Vec<(crate::node::query_engine::SourceId, crate::query::Query)>,
) {
    let schema = narrowing_hierarchy_schema();
    let (_dir, node) = open_node_with_schema(node(1), schema.clone());
    let shape = query.validate(&schema).unwrap();
    let values = if shape.params().contains_key("title") {
        BTreeMap::from([("title".to_owned(), Value::String("a".to_owned()))])
    } else {
        BTreeMap::new()
    };
    let binding = shape.bind(values).unwrap();
    let sources = node
        .exclusive_source_reads(&shape, &binding, false)
        .unwrap();
    // Implicit root references are sync payload and record no read.
    assert!(sources.payload.iter().all(|source| matches!(
        source.path.components.as_slice(),
        [
            crate::node::query_engine::SourceRole::Root,
            crate::node::query_engine::SourceRole::Alias(alias),
        ] if alias.starts_with("reference:")
    )));
    let reads = sources.reads;
    assert_eq!(
        reads.keys().cloned().collect::<Vec<_>>(),
        expected
            .iter()
            .map(|(source, _)| source.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>(),
    );
    for (source, narrowed) in expected {
        assert_eq!(
            reads[&source].shape.query(),
            narrowed.validate(&schema).unwrap().query(),
            "narrowed read of {source:?}"
        );
        assert!(reads[&source].shape.params().is_empty());
    }
}

fn narrowing_title_is_a() -> crate::query::Predicate {
    crate::query::eq(crate::query::col("title"), crate::query::lit("a"))
}

fn narrowing_body_is_hi() -> crate::query::Predicate {
    crate::query::eq(crate::query::col("body"), crate::query::lit("hi"))
}

fn narrowing_todos_titled() -> crate::query::Query {
    crate::query::Query::from("todos").filter(crate::query::eq(
        crate::query::col("title"),
        crate::query::param("title"),
    ))
}

/// garden-co/jazz#3694: each source a join chain reads is narrowed to the
/// rows correlated with its parent under the parent's filters, up to the
/// root's filters with their parameters bound. The root's implicit reference
/// is narrowed the same way.
///
/// White-box: soundness rests on the exact shape of each narrowed read, which
/// no public API exposes. The `exclusive_snapshot_coverage` integration tests
/// cover what a client observes (unrelated writes commit, related ones
/// conflict).
#[test]
fn narrowed_reads_follow_a_join_chain_to_the_root() {
    let mut query = narrowing_todos_titled();
    query.joins.push(crate::query::JoinVia {
        table: "comments".to_owned(),
        on_column: "todo".to_owned(),
        target: crate::query::JoinTarget::Column,
        source_column: None,
        source_lookup: None,
        correlated_filters: Vec::new(),
        filters: vec![narrowing_body_is_hi()],
        nested_joins: vec![crate::query::JoinVia {
            table: "reactions".to_owned(),
            on_column: "comment".to_owned(),
            target: crate::query::JoinTarget::Column,
            source_column: None,
            source_lookup: None,
            correlated_filters: Vec::new(),
            filters: Vec::new(),
            nested_joins: Vec::new(),
        }],
    });
    let to_todos =
        narrowing_reverse_join("todos", "id", "todo", vec![narrowing_title_is_a()], None);
    let mut comments = crate::query::Query::from("comments").filter(narrowing_body_is_hi());
    comments.joins.push(to_todos.clone());
    let mut reactions = crate::query::Query::from("reactions");
    reactions.joins.push(narrowing_reverse_join(
        "comments",
        "id",
        "comment",
        vec![narrowing_body_is_hi()],
        Some(to_todos),
    ));
    assert_narrowed_reads(
        query,
        vec![
            (
                narrowing_source("comments", &["alias=join_via:0"]),
                comments,
            ),
            (
                narrowing_source("reactions", &["alias=join_via:0:nested:0"]),
                reactions,
            ),
        ],
    );
}

/// Each segment of an include path is narrowed to the rows the previous
/// segment references.
#[test]
fn narrowed_reads_follow_an_include_path_to_the_root() {
    let to_todos =
        narrowing_reverse_join("todos", "project", "id", vec![narrowing_title_is_a()], None);
    let mut projects = crate::query::Query::from("projects");
    projects.joins.push(to_todos.clone());
    let mut orgs = crate::query::Query::from("orgs");
    orgs.joins.push(narrowing_reverse_join(
        "projects",
        "org",
        "id",
        Vec::new(),
        Some(to_todos),
    ));
    assert_narrowed_reads(
        narrowing_todos_titled().include("project.org"),
        vec![
            (
                narrowing_source("projects", &["root", "alias=include:0:0"]),
                projects,
            ),
            (
                narrowing_source("orgs", &["root", "alias=include:0:1"]),
                orgs,
            ),
        ],
    );
}

/// Correlated arrays, nested ones included, are narrowed to the rows
/// correlated with their owner rows under the owners' filters.
#[test]
fn narrowed_reads_follow_nested_arrays_to_the_root() {
    let query = narrowing_todos_titled().array_subquery(
        ArraySubquery::new("comments", "comments", "todo", "id")
            .filter(narrowing_body_is_hi())
            .nested(ArraySubquery::new(
                "reactions",
                "reactions",
                "comment",
                "id",
            )),
    );
    let to_todos =
        narrowing_reverse_join("todos", "id", "todo", vec![narrowing_title_is_a()], None);
    let mut comments = crate::query::Query::from("comments").filter(narrowing_body_is_hi());
    comments.joins.push(to_todos.clone());
    let mut reactions = crate::query::Query::from("reactions");
    reactions.joins.push(narrowing_reverse_join(
        "comments",
        "id",
        "comment",
        vec![narrowing_body_is_hi()],
        Some(to_todos),
    ));
    assert_narrowed_reads(
        query,
        vec![
            (
                narrowing_source("comments", &["root", "child=0:comments"]),
                comments,
            ),
            (
                narrowing_source(
                    "reactions",
                    &["root", "child=0:comments", "child=0.0:reactions"],
                ),
                reactions,
            ),
        ],
    );
}

/// A relation through an array of references correlates by membership,
/// which an equality join cannot express: its narrowed read requires at
/// least one parent row whose array holds it.
#[test]
fn narrowed_reads_follow_a_reference_array_by_membership() {
    let query = crate::query::Query::from("todos")
        .filter(narrowing_title_is_a())
        .array_subquery(crate::query::ArraySubquery::new(
            "assignees",
            "people",
            "id",
            "assignees",
        ));
    let mut to_todos = crate::query::ArraySubquery::new("narrowed_parent", "todos", "assignees", "id");
    to_todos.filters = vec![narrowing_title_is_a()];
    to_todos.requirement = crate::query::ArraySubqueryRequirement::AtLeastOne;
    let people = crate::query::Query::from("people").array_subquery(to_todos);
    assert_narrowed_reads(
        query,
        vec![(
            narrowing_source("people", &["root", "child=0:assignees"]),
            people,
        )],
    );
}

/// Assert an exclusive read of `query` fails before reading anything, naming
/// its read pattern.
fn assert_exclusive_read_unsupported(
    query: crate::query::Query,
    values: BTreeMap<String, Value>,
    pattern: &str,
) {
    let schema = narrowing_hierarchy_schema();
    let (_dir, node) = open_node_with_schema(node(1), schema.clone());
    let shape = query.validate(&schema).unwrap();
    let binding = shape.bind(values).unwrap();
    let Err(error) = node.exclusive_source_reads(&shape, &binding, false) else {
        panic!("{pattern} has no narrowed reads");
    };
    assert_eq!(
        error.to_string(),
        format!("Reading {pattern} is not supported in exclusive transactions yet")
    );
}

/// A flat join routes its filters per source, so its root rows are not
/// constrained by the root's own filters alone: it narrows nothing.
#[test]
fn exclusive_reads_reject_a_flat_join() {
    assert_exclusive_read_unsupported(
        crate::query::Query::from("todos").flat_join("comments", "todos._id", "comments.todo"),
        BTreeMap::new(),
        "a flat join of `todos` with `comments`",
    );
}

/// A lookup join correlates through a third table (projects sharing the
/// org of a todo's project), which a reverse join cannot express.
#[test]
fn exclusive_reads_reject_a_lookup_join() {
    let mut query = narrowing_todos_titled();
    query.joins.push(crate::query::JoinVia {
        table: "projects".to_owned(),
        on_column: "org".to_owned(),
        target: crate::query::JoinTarget::Column,
        source_column: Some("org".to_owned()),
        source_lookup: Some(crate::query::JoinSourceLookup {
            table: "projects".to_owned(),
            row_id_source_column: "project".to_owned(),
            value_column: "org".to_owned(),
        }),
        correlated_filters: Vec::new(),
        filters: Vec::new(),
        nested_joins: Vec::new(),
    });
    assert_exclusive_read_unsupported(
        query,
        BTreeMap::from([("title".to_owned(), Value::String("a".to_owned()))]),
        "`projects` through a lookup join from `todos`",
    );
}

/// A membership hop has no equality to carry extra correlated keys on.
#[test]
fn exclusive_reads_reject_a_reference_array_join_with_extra_keys() {
    let mut query = crate::query::Query::from("people");
    query.joins.push(crate::query::JoinVia {
        table: "todos".to_owned(),
        on_column: "assignees".to_owned(),
        target: crate::query::JoinTarget::Column,
        source_column: Some("id".to_owned()),
        source_lookup: None,
        correlated_filters: vec![crate::query::JoinCorrelation {
            join_column: "title".to_owned(),
            source_column: "name".to_owned(),
        }],
        filters: Vec::new(),
        nested_joins: Vec::new(),
    });
    assert_exclusive_read_unsupported(
        query,
        BTreeMap::new(),
        "`todos` through a reference array together with additional join keys",
    );
}

/// Inheriting access evaluates the parent's policy, which is not a
/// correlation the narrowed read can carry.
#[test]
fn exclusive_reads_reject_inherited_access() {
    let mut query = crate::query::Query::from("todos");
    query.inherits.push(crate::query::InheritsVia {
        parent_column: "project".to_owned(),
        operation: crate::query::InheritsOperation::Select,
        max_depth: None,
    });
    assert_exclusive_read_unsupported(
        query,
        BTreeMap::new(),
        "`todos` inheriting read access from `projects`",
    );
}

/// Policy branches are disjunctive, so the root rows are not constrained by
/// the root's own filters alone.
#[test]
fn exclusive_reads_reject_policy_branches() {
    let mut query = crate::query::Query::from("todos");
    query.policy_branches.push(crate::query::PolicyBranch {
        filters: vec![narrowing_title_is_a()],
        joins: vec![narrowing_reverse_join(
            "comments",
            "todo",
            "id",
            Vec::new(),
            None,
        )],
        reachable: Vec::new(),
        inherits: Vec::new(),
    });
    assert_exclusive_read_unsupported(
        query,
        BTreeMap::new(),
        "`todos` through policy branches",
    );
}

/// A union's rows come from several arms, so they are not constrained by
/// the root's own filters alone.
#[test]
fn exclusive_reads_reject_a_union_of_relations() {
    use crate::query::{
        RelationColumnRef, RelationExpr, RelationJoinCondition, RelationJoinKind,
        RelationProjectColumn, RelationProjectExpr, RelationQuery, RelationUnionArm,
    };
    let column = |scope: &str, column: &str| RelationColumnRef {
        scope: Some(scope.to_owned()),
        column: column.to_owned(),
    };
    let arm = |label: &str| RelationUnionArm {
        label: label.to_owned(),
        input: RelationExpr::Project {
            input: Box::new(RelationExpr::Join {
                left: Box::new(RelationExpr::TableScan {
                    table: "todos".to_owned(),
                    alias: None,
                }),
                right: Box::new(RelationExpr::TableScan {
                    table: "comments".to_owned(),
                    alias: Some("__hop_0".to_owned()),
                }),
                on: vec![RelationJoinCondition {
                    left: column("todos", "id"),
                    right: column("__hop_0", "todo"),
                }],
                join_kind: RelationJoinKind::Inner,
            }),
            columns: vec![
                RelationProjectColumn {
                    alias: "id".to_owned(),
                    expr: RelationProjectExpr::Column(column("todos", "id")),
                },
                RelationProjectColumn {
                    alias: "title".to_owned(),
                    expr: RelationProjectExpr::Column(column("todos", "title")),
                },
            ],
        },
    };
    let query = crate::query::relation_query_to_query(&RelationQuery {
        rel: RelationExpr::Union {
            inputs: vec![arm("first"), arm("second")],
        },
    })
    .unwrap();
    assert_exclusive_read_unsupported(
        query,
        BTreeMap::new(),
        "a union of relations over `todos`",
    );
}
