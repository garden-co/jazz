use super::*;

#[test]
fn rc_transactional_batch_rejects_writes_after_local_seal() {
    let mut s = create_3tier_rc();
    let open_write_context = WriteContext {
        session: None,
        attribution: None,
        updated_at: None,
        batch_mode: Some(crate::batch_fate::BatchMode::Transactional),
        batch_id: None,
        target_branch_name: None,
    };

    let ((row_id, _row_values), _receiver) = insert_and_wait_for_batch(
        &mut s.a,
        "users",
        user_insert_values(ObjectId::new(), "Alice"),
        Some(&open_write_context),
        DurabilityTier::Local,
    )
    .unwrap();

    let history_rows =
        s.a.storage()
            .scan_history_row_batches("users", row_id)
            .unwrap();
    assert_eq!(history_rows.len(), 1);
    let batch_id = history_rows[0].batch_id;

    s.a.commit_batch(batch_id).unwrap();

    let sealed_write_context = WriteContext {
        session: None,
        attribution: None,
        updated_at: None,
        batch_mode: Some(crate::batch_fate::BatchMode::Transactional),
        batch_id: Some(batch_id),
        target_branch_name: None,
    };

    let insert_err =
        s.a.insert(
            "users",
            user_insert_values(ObjectId::new(), "Bob"),
            Some(&sealed_write_context),
        )
        .unwrap_err();
    assert!(matches!(
        insert_err,
        RuntimeError::WriteError(message)
            if message.contains("already sealed")
    ));

    let persisted_insert_err = insert_and_wait_for_batch(
        &mut s.a,
        "users",
        user_insert_values(ObjectId::new(), "Carol"),
        Some(&sealed_write_context),
        DurabilityTier::Local,
    )
    .unwrap_err();
    assert!(matches!(
        persisted_insert_err,
        RuntimeError::WriteError(message)
            if message.contains("already sealed")
    ));

    let update_err =
        s.a.update(
            row_id,
            vec![("name".to_string(), Value::Text("Updated".to_string()))],
            Some(&sealed_write_context),
        )
        .unwrap_err();
    assert!(matches!(
        update_err,
        RuntimeError::WriteError(message)
            if message.contains("already sealed")
    ));

    let delete_err = s.a.delete(row_id, Some(&sealed_write_context)).unwrap_err();
    assert!(matches!(
        delete_err,
        RuntimeError::WriteError(message)
            if message.contains("already sealed")
    ));

    let history_rows_after =
        s.a.storage()
            .scan_history_row_batches("users", row_id)
            .unwrap();
    assert_eq!(
        history_rows_after.len(),
        1,
        "sealed batches should reject follow-up writes before new row batch entries are created"
    );
}

#[test]
fn rc_transactional_insert_persisted_tracks_local_batch_record_and_settlement() {
    // alice -> worker
    //   transactional write stages locally
    //   alice seals the batch
    //   worker accepts it
    //   alice resolves from replayable AcceptedTransaction settlement
    let mut s = create_3tier_rc();
    let write_context = WriteContext {
        session: None,
        attribution: None,
        updated_at: None,
        batch_mode: Some(crate::batch_fate::BatchMode::Transactional),
        batch_id: None,
        target_branch_name: None,
    };

    let ((row_id, _row_values), mut receiver) = insert_and_wait_for_batch(
        &mut s.a,
        "users",
        user_insert_values(ObjectId::new(), "Alice"),
        Some(&write_context),
        DurabilityTier::Local,
    )
    .unwrap();

    let history_rows =
        s.a.storage()
            .scan_history_row_batches("users", row_id)
            .unwrap();
    assert_eq!(history_rows.len(), 1);
    let batch_id = history_rows[0].batch_id;

    assert_eq!(
        s.a.storage().load_local_batch_record(batch_id).unwrap(),
        None,
        "open transactional batches should not persist replayable durability records before seal"
    );

    s.a.commit_batch(batch_id).unwrap();
    pump_a_to_b(&mut s);
    assert_eq!(
        receiver.try_recv(),
        Ok(None),
        "worker acceptance should not resolve until the settlement arrives back on alice"
    );

    pump_b_to_a(&mut s);
    assert_eq!(receiver.try_recv(), Ok(Some(Ok(()))));

    assert_eq!(
        s.a.storage()
            .load_authoritative_batch_fate(batch_id)
            .unwrap(),
        Some(crate::batch_fate::BatchFate::AcceptedTransaction {
            batch_id,
            confirmed_tier: DurabilityTier::Local,
        })
    );
}

#[test]
fn rc_transactional_insert_is_accepted_when_replayed_to_reconnected_upstream() {
    // alice stages one transactional row while disconnected
    //   reconnect alone is not enough
    //   once alice seals the batch, worker accepts the replayed staged row
    let mut s = create_3tier_rc();
    let write_context = WriteContext {
        session: None,
        attribution: None,
        updated_at: None,
        batch_mode: Some(crate::batch_fate::BatchMode::Transactional),
        batch_id: None,
        target_branch_name: None,
    };

    s.a.remove_server(s.b_server_for_a);

    let ((row_id, _row_values), _) =
        s.a.insert(
            "users",
            user_insert_values(ObjectId::new(), "Alice"),
            Some(&write_context),
        )
        .unwrap();

    assert!(
        s.b.storage()
            .scan_history_row_batches("users", row_id)
            .unwrap()
            .is_empty(),
        "disconnected upstream should not receive staged history yet"
    );

    s.a.add_server(s.b_server_for_a);
    let history_rows =
        s.a.storage()
            .scan_history_row_batches("users", row_id)
            .unwrap();
    assert_eq!(history_rows.len(), 1);
    let batch_id = history_rows[0].batch_id;
    s.a.commit_batch(batch_id).unwrap();
    pump_a_to_b(&mut s);

    let history_rows =
        s.b.storage()
            .scan_history_row_batches("users", row_id)
            .unwrap();
    assert_eq!(history_rows.len(), 1);
    assert_eq!(
        history_rows[0].state,
        crate::row_histories::RowState::VisibleTransactional
    );
    assert_eq!(history_rows[0].confirmed_tier, Some(DurabilityTier::Local));
    assert_eq!(history_rows[0].batch_id(), batch_id);

    let worker_row =
        s.b.storage()
            .load_visible_region_row("users", s.b.schema_manager().branch_name().as_str(), row_id)
            .unwrap()
            .expect("worker should publish the accepted transactional row on reconnect");
    assert_eq!(
        worker_row.state,
        crate::row_histories::RowState::VisibleTransactional
    );
    assert_eq!(worker_row.batch_id(), batch_id);
}

#[test]
fn rc_missing_batch_fate_retransmits_local_transactional_rows() {
    // alice -> worker
    //   alice stages one transactional batch
    //   alice seals it
    //   the initial outbound row is dropped
    //   worker replies Missing
    //   alice replays the staged row and the seal back upstream
    let mut s = create_3tier_rc();
    let write_context = WriteContext {
        session: None,
        attribution: None,
        updated_at: None,
        batch_mode: Some(crate::batch_fate::BatchMode::Transactional),
        batch_id: None,
        target_branch_name: None,
    };

    let ((row_id, _row_values), _receiver) = insert_and_wait_for_batch(
        &mut s.a,
        "users",
        user_insert_values(ObjectId::new(), "Alice"),
        Some(&write_context),
        DurabilityTier::Local,
    )
    .unwrap();

    let history_rows =
        s.a.storage()
            .scan_history_row_batches("users", row_id)
            .unwrap();
    assert_eq!(history_rows.len(), 1);
    let batch_id = history_rows[0].batch_id;
    let branch_name = crate::object::BranchName::new(history_rows[0].branch.as_str());
    let row_digest = history_rows[0].content_digest();
    s.a.commit_batch(batch_id).unwrap();

    s.a.batched_tick();
    let dropped_outbox = s.a.sync_sender().take();
    assert!(dropped_outbox.iter().any(|entry| {
        matches!(
            &entry,
            OutboxEntry {
                destination: Destination::Server(server_id),
                payload: SyncPayload::RowBatchCreated { row, .. }
                    | SyncPayload::RowBatchNeeded { row, .. },
            } if *server_id == s.b_server_for_a && row.row_id == row_id && row.batch_id == batch_id
        )
    }), "expected initial outbound row for batch replay test, got {dropped_outbox:?}");
    assert!(
        dropped_outbox.iter().any(|entry| matches!(
            entry,
            OutboxEntry {
                destination: Destination::Server(server_id),
                payload: SyncPayload::SealBatch { submission },
            } if *server_id == s.b_server_for_a
                && submission.batch_id == batch_id
                && submission.target_branch_name == branch_name
                && submission.members == vec![SealedBatchMember {
                    object_id: row_id,
                    row_digest,
                }]
                && submission.captured_frontier.is_empty()
        )),
        "expected initial outbound seal for batch replay test, got {dropped_outbox:?}"
    );

    s.a.park_sync_message(InboxEntry {
        source: Source::Server(s.b_server_for_a),
        payload: SyncPayload::BatchFate {
            fate: crate::batch_fate::BatchFate::Missing { batch_id },
        },
    });
    s.a.batched_tick();

    let replay_outbox = s.a.sync_sender().take();
    assert!(replay_outbox.iter().any(|entry| {
        matches!(
            &entry,
            OutboxEntry {
                destination: Destination::Server(server_id),
                payload: SyncPayload::RowBatchCreated { row, .. }
                    | SyncPayload::RowBatchNeeded { row, .. },
            } if *server_id == s.b_server_for_a && row.row_id == row_id && row.batch_id == batch_id
        )
    }), "expected replayed outbound row after Missing settlement, got {replay_outbox:?}");
    assert!(
        replay_outbox.iter().any(|entry| matches!(
            entry,
            OutboxEntry {
                destination: Destination::Server(server_id),
                payload: SyncPayload::SealBatch { submission },
            } if *server_id == s.b_server_for_a
                && submission.batch_id == batch_id
                && submission.target_branch_name == branch_name
                && submission.members == vec![SealedBatchMember {
                    object_id: row_id,
                    row_digest,
                }]
                && submission.captured_frontier.is_empty()
        )),
        "expected replayed outbound seal after Missing settlement, got {replay_outbox:?}"
    );

    assert_eq!(
        s.a.storage()
            .load_authoritative_batch_fate(batch_id)
            .unwrap(),
        Some(crate::batch_fate::BatchFate::Missing { batch_id })
    );
}

#[test]
fn rc_missing_batch_fate_retransmits_local_transactional_rows_without_row_locator_scans() {
    let mut core = create_runtime_with_boxed_storage(
        test_schema(),
        "missing-batch-retransmit-scanless-test",
        Box::new(RowRegionReadFailingStorage::with_row_locator_scan_failure()),
    );
    let server_id = ServerId::new();
    core.add_server(server_id);

    let write_context = WriteContext {
        session: None,
        attribution: None,
        updated_at: None,
        batch_mode: Some(crate::batch_fate::BatchMode::Transactional),
        batch_id: None,
        target_branch_name: None,
    };

    let ((row_id, _row_values), _receiver) = insert_and_wait_for_batch(
        &mut core,
        "users",
        user_insert_values(ObjectId::new(), "Alice"),
        Some(&write_context),
        DurabilityTier::Local,
    )
    .unwrap();

    let history_rows = core
        .storage()
        .scan_history_row_batches("users", row_id)
        .unwrap();
    let batch_id = history_rows[0].batch_id;
    let branch_name = crate::object::BranchName::new(history_rows[0].branch.as_str());
    let row_digest = history_rows[0].content_digest();
    core.commit_batch(batch_id).unwrap();

    core.batched_tick();
    core.sync_sender().take();

    core.park_sync_message(InboxEntry {
        source: Source::Server(server_id),
        payload: SyncPayload::BatchFate {
            fate: crate::batch_fate::BatchFate::Missing { batch_id },
        },
    });
    core.batched_tick();

    let replay_outbox = core.sync_sender().take();
    assert!(replay_outbox.iter().any(|entry| {
        matches!(
            &entry,
            OutboxEntry {
                destination: Destination::Server(id),
                payload: SyncPayload::RowBatchCreated { row, .. }
                    | SyncPayload::RowBatchNeeded { row, .. },
            } if *id == server_id && row.row_id == row_id && row.batch_id == batch_id
        )
    }));
    assert!(replay_outbox.iter().any(|entry| matches!(
        entry,
        OutboxEntry {
            destination: Destination::Server(id),
            payload: SyncPayload::SealBatch { submission },
        } if *id == server_id
            && submission.batch_id == batch_id
            && submission.target_branch_name == branch_name
            && submission.members == vec![SealedBatchMember {
                object_id: row_id,
                row_digest,
            }]
    )));
}
