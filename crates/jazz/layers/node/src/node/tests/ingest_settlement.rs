// Internal ingress tests: public clients cannot select fragment/fate delivery
// order or inspect the overlay before recovery. No test fabricates stored rows.
fn settlement_fragment_tx(id: TxId, count: u32) -> Transaction {
    Transaction {
        tx_id: id,
        kind: TxKind::Mergeable,
        n_total_writes: count,
        made_by: AuthorSubject::system_at(id.node),
        permission_subject: None,
        base_snapshot: None,
        row_read_set: None,
        absent_read_set: None,
        predicate_read_set: None,
        user_metadata_json: None,
        contribution_merge: None,
    }
}

#[test]
fn fragment_settlement_covers_earlier_content_and_deletion_versions() {
    for complete in [false, true] {
        for deletion in [None, Some(DeletionEvent::Deleted)] {
            let (dir, mut reader) = open_node();
            let id = TxId::new(TxTime::from(100), node(0x82));
            let a = version_record(
                row(1),
                Vec::new(),
                if deletion.is_some() {
                    BTreeMap::new()
                } else {
                    title_cells("earlier")
                },
                deletion,
            );
            let b = version_record(row(2), Vec::new(), title_cells("later"), None);
            reader
                .ingest_view_scoped_transaction_with_current_indexes(
                    settlement_fragment_tx(id, 1),
                    vec![a.clone()],
                    Fate::Pending,
                    None,
                    DurabilityTier::Local,
                )
                .unwrap();
            assert_eq!(ahead_current_row_count(&mut reader, "todos"), 1);
            if complete {
                reader
                    .ingest_known_transaction(
                        settlement_fragment_tx(id, 2),
                        vec![a, b],
                        Fate::Accepted,
                        Some(GlobalTime(1)),
                        DurabilityTier::Global,
                    )
                    .unwrap();
            } else {
                reader
                    .ingest_view_scoped_transaction_with_current_indexes(
                        settlement_fragment_tx(id, 1),
                        vec![b],
                        Fate::Accepted,
                        Some(GlobalTime(1)),
                        DurabilityTier::Global,
                    )
                    .unwrap();
            }
            assert_eq!(ahead_current_row_count(&mut reader, "todos"), 0);
            // Linear history keeps deletion in the row image: the settled
            // global image of row 1 is this transaction's, deleted or not.
            assert_eq!(
                reader
                    .visible_global_current_now(schema().version_id(), "todos", row(1))
                    .resolve(),
                Some((id, deletion.is_some()))
            );
            reader.database.close().unwrap();
            drop(reader);
            let mut reader = reopen_node_at(&dir, node(0x83), schema());
            assert_eq!(ahead_current_row_count(&mut reader, "todos"), 0);
            assert_eq!(
                reader
                    .current_rows("todos", DurabilityTier::Global)
                    .unwrap()
                    .len(),
                if deletion.is_some() { 1 } else { 2 }
            );
        }
    }
}

#[test]
fn rejected_fragment_removes_all_pending_effects() {
    let (_dir, mut reader) = open_node();
    let id = TxId::new(TxTime::from(100), node(0x82));
    reader
        .ingest_view_scoped_transaction_with_current_indexes(
            settlement_fragment_tx(id, 1),
            vec![version_record(
                row(1),
                Vec::new(),
                title_cells("earlier"),
                None,
            )],
            Fate::Pending,
            None,
            DurabilityTier::Local,
        )
        .unwrap();
    let rejected = Fate::Rejected(RejectionReason::AuthorizationDenied);
    reader
        .ingest_view_scoped_transaction_with_current_indexes(
            settlement_fragment_tx(id, 1),
            vec![version_record(
                row(2),
                Vec::new(),
                title_cells("later"),
                None,
            )],
            rejected.clone(),
            None,
            DurabilityTier::Local,
        )
        .unwrap();
    assert_eq!(reader.transaction_record(id).unwrap().fate, rejected);
    assert_eq!(ahead_current_row_count(&mut reader, "todos"), 0);
    assert!(
        reader
            .current_rows("todos", DurabilityTier::Local)
            .unwrap()
            .is_empty()
    );
    assert!(reader.query_versions_for_tx(id).unwrap().is_empty());
}

#[test]
fn stale_pending_fragment_preserves_complete_terminal_fate() {
    for rejected in [false, true] {
        let (_dir, mut reader) = open_node();
        let id = TxId::new(TxTime::from(100), node(1));
        let a = version_record(row(1), Vec::new(), title_cells("original"), None);
        let fate = if rejected {
            Fate::Rejected(RejectionReason::AuthorizationDenied)
        } else {
            Fate::Accepted
        };
        let global_time = (!rejected).then_some(GlobalTime(1));
        reader
            .ingest_known_transaction(
                settlement_fragment_tx(id, 1),
                vec![a.clone()],
                fate.clone(),
                global_time,
                DurabilityTier::Global,
            )
            .unwrap();
        reader
            .ingest_view_scoped_transaction_with_current_indexes(
                settlement_fragment_tx(id, 1),
                vec![a],
                Fate::Pending,
                None,
                DurabilityTier::Local,
            )
            .unwrap();
        let stored = reader.transaction_record(id).unwrap();
        assert_eq!(stored.fate, fate);
        assert_eq!(stored.global_time, global_time);
        assert_eq!(stored.durability, DurabilityTier::Global);
        assert_eq!(ahead_current_row_count(&mut reader, "todos"), 0);
    }
}

#[test]
fn failed_fragment_settlement_preserves_pending_state_on_reopen() {
    let (mut reader, storage) = fail_write_many_node();
    let id = TxId::new(TxTime::from(100), node(0x82));
    reader
        .ingest_view_scoped_transaction_with_current_indexes(
            settlement_fragment_tx(id, 1),
            vec![version_record(
                row(1),
                Vec::new(),
                title_cells("earlier"),
                None,
            )],
            Fate::Pending,
            None,
            DurabilityTier::Local,
        )
        .unwrap();
    storage.fail_nth_following_write_many(1);
    assert!(
        reader
            .ingest_view_scoped_transaction_with_current_indexes(
                settlement_fragment_tx(id, 1),
                vec![version_record(
                    row(2),
                    Vec::new(),
                    title_cells("later"),
                    None
                )],
                Fate::Accepted,
                Some(GlobalTime(1)),
                DurabilityTier::Global,
            )
            .is_err()
    );
    drop(reader);
    let mut reopened = NodeState::new(node(0x83), schema(), storage).unwrap();
    assert_eq!(reopened.transaction_record(id).unwrap().fate, Fate::Pending);
    assert_eq!(ahead_current_row_count(&mut reopened, "todos"), 1);
    assert_eq!(
        reopened
            .current_rows("todos", DurabilityTier::Local)
            .unwrap()
            .len(),
        1
    );
    assert!(
        reopened
            .current_rows("todos", DurabilityTier::Global)
            .unwrap()
            .is_empty()
    );
    reopened
        .ingest_view_scoped_transaction_with_current_indexes(
            settlement_fragment_tx(id, 1),
            vec![version_record(
                row(2),
                Vec::new(),
                title_cells("later"),
                None,
            )],
            Fate::Accepted,
            Some(GlobalTime(1)),
            DurabilityTier::Global,
        )
        .unwrap();
    assert_eq!(ahead_current_row_count(&mut reopened, "todos"), 0);
    assert_eq!(
        reopened
            .current_rows("todos", DurabilityTier::Global)
            .unwrap()
            .len(),
        2
    );
}

// Batched ViewUpdate ingestion shares the same storage-atomic settlement path.
#[test]
fn batched_fragment_settlement_publishes_the_whole_transaction_once() {
    for rejected in [false, true] {
        let (mut reader, storage) = fail_write_many_node();
        let id = TxId::new(TxTime::from(100), node(0x82));
        reader
            .ingest_view_scoped_transaction_with_current_indexes(
                settlement_fragment_tx(id, 1),
                vec![version_record(
                    row(1),
                    Vec::new(),
                    title_cells("earlier"),
                    None,
                )],
                Fate::Pending,
                None,
                DurabilityTier::Local,
            )
            .unwrap();
        let mut batch = reader.database.open_batch();
        let mut times = Vec::new();
        let mut content = Vec::new();
        let mut rejections = Vec::new();
        let writes_before = storage.write_many_call_count();
        reader
            .stage_view_scoped_transaction_with_current_indexes(
                &mut batch,
                settlement_fragment_tx(id, 1),
                vec![version_record(
                    row(2),
                    Vec::new(),
                    title_cells("later"),
                    None,
                )],
                if rejected {
                    Fate::Rejected(RejectionReason::AuthorizationDenied)
                } else {
                    Fate::Accepted
                },
                (!rejected).then_some(GlobalTime(1)),
                DurabilityTier::Global,
                &mut times,
                &mut content,
                &mut rejections,
            )
            .unwrap();
        let applied = reader.database.apply_batch(batch).unwrap();
        let persisted = crate::local_executor::block_on(applied.persist());
        reader.database.finish_persistence(persisted).unwrap();
        assert_eq!(storage.write_many_call_count() - writes_before, 1);
        assert_eq!(ahead_current_row_count(&mut reader, "todos"), 0);
        assert_eq!(
            reader
                .current_rows("todos", DurabilityTier::Local)
                .unwrap()
                .len(),
            if rejected { 0 } else { 2 }
        );
        drop(reader);
        reset_query_versions_for_tx_call_count();
        let mut reopened = NodeState::new(node(0x83), schema(), storage).unwrap();
        assert_eq!(query_versions_for_tx_call_count(), 0);
        assert_eq!(ahead_current_row_count(&mut reopened, "todos"), 0);
        assert_eq!(
            reopened
                .current_rows("todos", DurabilityTier::Global)
                .unwrap()
                .len(),
            if rejected { 0 } else { 2 }
        );
    }
}
