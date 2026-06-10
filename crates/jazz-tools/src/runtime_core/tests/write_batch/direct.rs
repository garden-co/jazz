use super::*;

#[test]
fn rc_insert_returns_immediately() {
    let mut s = create_3tier_rc();
    let user_id = ObjectId::new();
    let expected_values = user_row_values(user_id, "Alice");
    let ((id, row_values), _) =
        s.a.insert("users", user_insert_values(user_id, "Alice"), None)
            .unwrap();
    assert!(!id.uuid().is_nil());
    assert_eq!(row_values, expected_values);

    let query = Query::new("users");
    let results = execute_query(&mut s.a, query);
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].0, id);
    assert_eq!(results[0].1, row_values);
}

#[test]
fn rc_auto_committed_direct_write_rejects_later_commit() {
    let mut core = create_test_runtime();
    let (_, batch_id) = core
        .insert("users", user_insert_values(ObjectId::new(), "Alice"), None)
        .unwrap();

    let expected_error = format!("Write error: batch {batch_id} is already committed");
    let commit_err = core.commit_batch(batch_id).unwrap_err().to_string();
    assert_eq!(commit_err, expected_error);
}

#[test]
fn rc_insert_data_syncs_to_server() {
    let mut s = create_3tier_rc();
    let ((id, _row_values), _) =
        s.a.insert("users", user_insert_values(ObjectId::new(), "Alice"), None)
            .unwrap();

    pump_a_to_b(&mut s);

    let query = Query::new("users");
    let results = execute_query(&mut s.b, query);
    assert_eq!(results.len(), 1, "Server B should have the synced row");
    assert_eq!(results[0].0, id);
}

#[test]
fn rc_insert_syncs_exact_row_batch_without_row_region_reads() {
    let mut core = create_runtime_with_boxed_storage(
        test_schema(),
        "row-batch-direct-sync-test",
        Box::new(RowRegionReadFailingStorage::new()),
    );
    let server_id = ServerId::new();
    core.add_server(server_id);
    core.batched_tick();
    core.sync_sender().take();

    let ((row_id, _row_values), _) = core
        .insert("users", user_insert_values(ObjectId::new(), "Alice"), None)
        .unwrap();
    core.batched_tick();

    let messages = core.sync_sender().take();
    let row_sync = messages
        .iter()
        .find(|entry| matches!(&entry.payload, SyncPayload::RowBatchCreated { row, .. } if row.row_id == row_id))
        .expect("insert should still sync the row upstream");

    match &row_sync.payload {
        SyncPayload::RowBatchCreated { row, .. } => {
            assert_eq!(row.row_id, row_id);
        }
        other => {
            panic!("local row writes should sync using the authored row batch entry, got {other:?}")
        }
    }
}

#[test]
fn rc_sealed_direct_batch_replays_row_and_seal_after_offline_write() {
    let mut core = create_runtime_with_boxed_storage(
        test_schema(),
        "offline-direct-batch-replay-test",
        Box::new(MemoryStorage::new()),
    );
    let server_id = ServerId::new();

    let ((row_id, _row_values), batch_id) = core
        .insert("users", user_insert_values(ObjectId::new(), "Alice"), None)
        .unwrap();
    assert_eq!(
        core.storage()
            .load_sealed_batch_submission(batch_id)
            .unwrap(),
        None,
        "serverless direct writes settle locally and retire the sealed submission"
    );

    core.add_server(server_id);
    core.batched_tick();

    let outbox = core.sync_sender().take();
    assert!(outbox.iter().any(|entry| matches!(
        entry,
        OutboxEntry {
            destination: Destination::Server(id),
            payload: SyncPayload::RowBatchCreated { row, .. },
        } if *id == server_id && row.row_id == row_id && row.batch_id == batch_id
    )));

    let stored_row = core
        .storage()
        .scan_history_row_batches("users", row_id)
        .unwrap()
        .into_iter()
        .find(|row| row.batch_id == batch_id)
        .expect("committed row history should exist");
    let branch_name = core.schema_manager().branch_name();

    let seal = outbox
        .iter()
        .find_map(|entry| match (&entry.destination, &entry.payload) {
            (Destination::Server(id), SyncPayload::SealBatch { submission })
                if *id == server_id =>
            {
                Some(submission.clone())
            }
            _ => None,
        })
        .expect("add_server should replay a regenerated seal");

    assert_eq!(seal.batch_id, batch_id);
    assert_eq!(seal.mode, crate::batch_fate::BatchMode::Direct);
    assert_eq!(seal.target_branch_name, branch_name);
    assert_eq!(
        seal.members,
        vec![SealedBatchMember {
            object_id: row_id,
            row_digest: stored_row.content_digest(),
        }],
        "regenerated seal must carry exactly the committed member with its stored digest"
    );
    assert!(
        seal.captured_frontier.is_empty(),
        "a seal regenerated from row history carries no captured frontier"
    );
}

#[test]
fn rc_stored_missing_fate_replays_seal_and_rows_after_restart() {
    // A live `Missing` fate triggers an immediate retransmit, but if the app
    // restarts before that retransmit settles, the stored `Missing` fate is all
    // that's left. `add_server` must still re-offer the retained seal.
    let schema = test_schema();
    let mut core = create_runtime_with_storage_and_sync_manager(
        schema.clone(),
        "missing-fate-restart-replay-test",
        MemoryStorage::new(),
        SyncManager::new(),
    );
    let server_id = ServerId::new();
    core.add_server(server_id);
    core.batched_tick();
    core.sync_sender().take();

    let ((row_id, _row_values), batch_id) = core
        .insert("users", user_insert_values(ObjectId::new(), "Alice"), None)
        .unwrap();
    core.batched_tick();
    core.sync_sender().take();

    core.park_sync_message(InboxEntry {
        source: Source::Server(server_id),
        payload: SyncPayload::BatchFate {
            fate: crate::batch_fate::BatchFate::Missing { batch_id },
        },
    });
    core.batched_tick();
    core.sync_sender().take();
    assert_eq!(
        core.storage()
            .load_authoritative_batch_fate(batch_id)
            .unwrap(),
        Some(crate::batch_fate::BatchFate::Missing { batch_id }),
        "the restart path must read back a stored Missing fate"
    );

    let storage = core.into_storage();
    let mut restarted = create_runtime_with_storage_and_sync_manager(
        schema,
        "missing-fate-restart-replay-test",
        storage,
        SyncManager::new(),
    );
    let server_id = ServerId::new();
    restarted.add_server(server_id);
    restarted.batched_tick();

    let outbox = restarted.sync_sender().take();
    assert!(
        outbox.iter().any(|entry| matches!(
            &entry.payload,
            SyncPayload::SealBatch { submission } if submission.batch_id == batch_id
        )),
        "a stored Missing fate must keep the batch pending so add_server resends the seal"
    );
    assert!(
        outbox.iter().any(|entry| matches!(
            &entry.payload,
            SyncPayload::RowBatchCreated { row, .. }
                if row.row_id == row_id && row.batch_id == batch_id
        )),
        "add_server must also resend the batch rows"
    );
}

#[test]
fn rc_non_durable_client_seals_direct_batch_without_self_confirming_local_fate() {
    let mut s = create_3tier_rc();
    s.a.set_non_durable_client_runtime();

    let ((row_id, _row_values), batch_id) =
        s.a.insert("users", user_insert_values(ObjectId::new(), "Alice"), None)
            .unwrap();

    let query_results = execute_query(&mut s.a, Query::new("users"));
    assert!(
        query_results.iter().any(|(id, _)| *id == row_id),
        "non-durable client should still expose optimistic rows to local queries"
    );
    assert_eq!(
        s.a.storage()
            .load_authoritative_batch_fate(batch_id)
            .unwrap(),
        None,
        "non-durable client should not persist its own local fate"
    );

    let mut receiver = s.a.wait_for_batch(batch_id, DurabilityTier::Local).unwrap();
    assert_eq!(
        receiver.try_recv(),
        Ok(None),
        "local wait should remain pending until worker-originated fate syncs back"
    );

    pump_a_to_b(&mut s);
    pump_b_to_a(&mut s);

    assert_eq!(
        receiver.try_recv(),
        Ok(Some(Ok(()))),
        "worker BatchFate(local) should resolve the client local wait"
    );
}

#[test]
fn rc_update_sync() {
    let mut s = create_3tier_rc();
    let ((id, _row_values), _) =
        s.a.insert("users", user_insert_values(ObjectId::new(), "Alice"), None)
            .unwrap();
    pump_a_to_b(&mut s);

    s.a.update(id, vec![("name".into(), Value::Text("Bob".into()))], None)
        .unwrap();
    pump_a_to_b(&mut s);

    let query = Query::new("users");
    let results = execute_query(&mut s.b, query);
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].1[1], Value::Text("Bob".into()));
}

#[test]
fn rc_late_insert_settlement_does_not_hide_newer_update() {
    let mut s = create_3tier_rc();
    let ((id, _row_values), insert_batch_id) =
        s.a.insert("users", user_insert_values(ObjectId::new(), "Alice"), None)
            .unwrap();

    s.a.update(id, vec![("name".into(), Value::Text("Bob".into()))], None)
        .unwrap();

    s.a.push_sync_inbox(InboxEntry {
        source: Source::Server(s.b_server_for_a),
        payload: SyncPayload::BatchFate {
            fate: crate::batch_fate::BatchFate::DurableDirect {
                batch_id: insert_batch_id,
                confirmed_tier: DurabilityTier::GlobalServer,
            },
        },
    });
    s.a.immediate_tick();

    let query = Query::new("users");
    let results = execute_query(&mut s.a, query);
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].1[1], Value::Text("Bob".into()));
}

#[test]
fn rc_delete_sync() {
    let mut s = create_3tier_rc();
    let ((id, _row_values), _) =
        s.a.insert("users", user_insert_values(ObjectId::new(), "Alice"), None)
            .unwrap();
    pump_a_to_b(&mut s);

    s.a.delete(id, None).unwrap();
    pump_a_to_b(&mut s);

    let query = Query::new("users");
    let results = execute_query(&mut s.b, query);
    assert_eq!(results.len(), 0, "Row should be deleted on B");
}

#[test]
fn rc_sealing_empty_batch_completes_waits_without_local_record() {
    let mut core = create_test_runtime();
    let batch_id = core.begin_batch(crate::batch_fate::BatchMode::Direct);

    core.commit_batch(batch_id).unwrap();

    assert_eq!(
        core.storage().load_local_batch_record(batch_id).unwrap(),
        None,
        "empty batches should not persist a replayable local batch record"
    );
    assert_eq!(
        core.storage()
            .load_sealed_batch_submission(batch_id)
            .unwrap(),
        None,
        "empty batches should not persist a sealed submission"
    );

    let mut receiver = core
        .wait_for_batch(batch_id, DurabilityTier::GlobalServer)
        .unwrap();
    assert_eq!(receiver.try_recv(), Ok(Some(Ok(()))));

    let write_context = WriteContext::default().with_batch_id(batch_id);
    let error = core
        .insert(
            "users",
            user_insert_values(ObjectId::new(), "Alice"),
            Some(&write_context),
        )
        .expect_err("sealed empty batch should not accept later writes");
    assert_eq!(
        error.to_string(),
        format!("Write error: batch {batch_id} has already been completed or was never opened")
    );
}

#[test]
fn rc_rolled_back_batch_rejects_later_operations() {
    let mut core = create_test_runtime();
    let batch_id = core.begin_batch(crate::batch_fate::BatchMode::Direct);
    let write_context = WriteContext::default().with_batch_id(batch_id);

    core.insert(
        "users",
        user_insert_values(ObjectId::new(), "Alice"),
        Some(&write_context),
    )
    .unwrap();

    core.rollback_batch(batch_id).unwrap();

    let expected_error =
        format!("Write error: batch {batch_id} has already been completed or was never opened");

    let commit_err = core.commit_batch(batch_id).unwrap_err().to_string();
    assert_eq!(commit_err, expected_error);

    let rollback_err = core.rollback_batch(batch_id).unwrap_err().to_string();
    assert_eq!(rollback_err, expected_error);

    let write_err = core
        .insert(
            "users",
            user_insert_values(ObjectId::new(), "Bob"),
            Some(&write_context),
        )
        .unwrap_err()
        .to_string();
    assert_eq!(write_err, expected_error);

    let query_err = match core.query_with_local_batch(
        Query::new("users"),
        None,
        ReadDurabilityOptions::default(),
        crate::sync_manager::QueryPropagation::Full,
        Some(batch_id),
    ) {
        Ok(_) => panic!("query should reject rolled back batch"),
        Err(error) => error.to_string(),
    };
    assert_eq!(query_err, expected_error);
}

#[test]
fn rc_same_row_direct_batch_overwrites_staged_member_in_place() {
    let mut core = create_test_runtime();
    let batch_id = BatchId::new();
    let write_context = WriteContext::default()
        .with_batch_mode(crate::batch_fate::BatchMode::Direct)
        .with_batch_id(batch_id);

    let ((row_id, _), _) = core
        .insert(
            "users",
            user_insert_values(ObjectId::new(), "Alice"),
            Some(&write_context),
        )
        .unwrap();

    core.update(
        row_id,
        vec![("name".to_string(), Value::Text("Alicia".to_string()))],
        Some(&write_context),
    )
    .unwrap();

    let branch_name = core.schema_manager().branch_name();
    let history_rows = core
        .storage()
        .scan_history_row_batches("users", row_id)
        .unwrap();
    assert_eq!(
        history_rows.len(),
        1,
        "rewriting the same row inside one direct batch should overwrite the batch member instead of appending a second history row"
    );
    assert_eq!(history_rows[0].batch_id, batch_id);
    assert_eq!(history_rows[0].batch_id(), batch_id);

    assert_eq!(
        core.storage()
            .load_visible_region_row("users", branch_name.as_str(), row_id)
            .unwrap(),
        None,
        "open direct batch rows should stay staged until the batch is sealed"
    );
    assert_eq!(
        history_rows[0].state,
        crate::row_histories::RowState::StagingPending
    );

    core.commit_batch(batch_id).unwrap();

    let visible_row = core
        .storage()
        .load_visible_region_row("users", branch_name.as_str(), row_id)
        .unwrap()
        .expect("sealed direct batch row should become visible");
    assert_eq!(visible_row.batch_id, batch_id);
    assert_eq!(visible_row.batch_id(), batch_id);
    assert_eq!(
        visible_row.state,
        crate::row_histories::RowState::VisibleDirect
    );
}

#[test]
fn rc_direct_batch_reuses_loaded_local_batch_record_while_building() {
    let calls = Arc::new(Mutex::new(RowMutationCallCounts::default()));
    let mut core = create_runtime_with_boxed_storage(
        test_schema(),
        "direct-batch-local-record-cache-test",
        Box::new(RowMutationObservingStorage::new(calls.clone())),
    );
    let batch_id = BatchId::new();
    let write_context = WriteContext::default()
        .with_batch_mode(crate::batch_fate::BatchMode::Direct)
        .with_batch_id(batch_id);

    core.insert(
        "users",
        user_insert_values(ObjectId::new(), "Alice"),
        Some(&write_context),
    )
    .unwrap();
    core.insert(
        "users",
        user_insert_values(ObjectId::new(), "Bob"),
        Some(&write_context),
    )
    .unwrap();
    core.insert(
        "users",
        user_insert_values(ObjectId::new(), "Cleo"),
        Some(&write_context),
    )
    .unwrap();

    assert_eq!(
        calls.lock().unwrap().local_batch_record_get_calls,
        1,
        "building a direct batch should not repeatedly load and decode the growing local batch record"
    );
}

#[test]
fn rc_worker_direct_batch_persists_batch_fate_on_seal() {
    let mut s = create_3tier_rc();
    let batch_id = BatchId::new();
    let write_context = WriteContext::default()
        .with_batch_mode(crate::batch_fate::BatchMode::Direct)
        .with_batch_id(batch_id);

    let ((first_row_id, _), first_batch_id) =
        s.b.insert(
            "users",
            user_insert_values(ObjectId::new(), "Alice"),
            Some(&write_context),
        )
        .unwrap();
    let ((second_row_id, _), second_batch_id) =
        s.b.insert(
            "users",
            user_insert_values(ObjectId::new(), "Bob"),
            Some(&write_context),
        )
        .unwrap();
    assert_eq!(first_batch_id, batch_id);
    assert_eq!(second_batch_id, batch_id);

    assert_eq!(
        s.b.storage().load_local_batch_record(batch_id).unwrap(),
        None,
        "open direct batches should not persist replayable durability records before seal"
    );

    s.b.commit_batch(batch_id).unwrap();

    let branch_name = s.b.schema_manager().branch_name();
    match s
        .b
        .storage()
        .load_authoritative_batch_fate(batch_id)
        .unwrap()
    {
        Some(crate::batch_fate::BatchFate::DurableDirect {
            batch_id: settled_batch_id,
            confirmed_tier,
        }) => {
            assert_eq!(settled_batch_id, batch_id);
            assert_eq!(confirmed_tier, DurabilityTier::Local);
        }
        other => panic!("expected durable direct settlement, got {other:?}"),
    }

    let first_visible_row =
        s.b.storage()
            .load_visible_region_row("users", branch_name.as_str(), first_row_id)
            .unwrap()
            .expect("sealed direct batch member should stay visible");
    assert_eq!(
        first_visible_row.confirmed_tier, None,
        "sealing a direct batch should leave row-local tier empty"
    );
    let first_local_row =
        s.b.storage()
            .load_visible_region_row_for_tier(
                "users",
                branch_name.as_str(),
                first_row_id,
                DurabilityTier::Local,
            )
            .unwrap()
            .expect("sealed direct batch member should be locally durable via settlement storage");
    assert_eq!(
        first_local_row.confirmed_tier,
        Some(DurabilityTier::Local),
        "tiered reads should project local durability from the batch settlement"
    );
}

#[test]
fn rc_sealed_direct_batch_rejects_further_writes() {
    let mut core = create_test_runtime();
    let batch_id = BatchId::new();
    let write_context = WriteContext::default()
        .with_batch_mode(crate::batch_fate::BatchMode::Direct)
        .with_batch_id(batch_id);

    let ((row_id, _), _) = core
        .insert(
            "users",
            user_insert_values(ObjectId::new(), "Alice"),
            Some(&write_context),
        )
        .unwrap();

    core.commit_batch(batch_id).unwrap();

    assert_eq!(
        core.storage()
            .load_sealed_batch_submission(batch_id)
            .unwrap(),
        None,
        "serverless direct batches settle locally and retire the sealed submission"
    );

    let err = core
        .update(
            row_id,
            vec![("name".to_string(), Value::Text("Alicia".to_string()))],
            Some(&write_context),
        )
        .expect_err("sealed direct batches should be frozen");
    let err = format!("{err:?}");
    assert!(
        err.contains("already sealed") || err.contains("already committed"),
        "expected sealed or committed batch error, got {err:?}"
    );
}

#[test]
fn rc_committed_batch_rejects_later_handle_operations() {
    let mut core = create_test_runtime();
    let batch_id = core.begin_batch(crate::batch_fate::BatchMode::Direct);
    let write_context = WriteContext::default().with_batch_id(batch_id);

    core.insert(
        "users",
        user_insert_values(ObjectId::new(), "Alice"),
        Some(&write_context),
    )
    .unwrap();
    core.commit_batch(batch_id).unwrap();

    let expected_error = format!("Write error: batch {batch_id} is already committed");

    let commit_err = core.commit_batch(batch_id).unwrap_err().to_string();
    assert_eq!(commit_err, expected_error);

    let rollback_err = core.rollback_batch(batch_id).unwrap_err().to_string();
    assert_eq!(rollback_err, expected_error);

    let write_err = core
        .insert(
            "users",
            user_insert_values(ObjectId::new(), "Bob"),
            Some(&write_context),
        )
        .unwrap_err()
        .to_string();
    assert_eq!(write_err, expected_error);

    let query_err = match core.query_with_local_batch(
        Query::new("users"),
        None,
        ReadDurabilityOptions::default(),
        crate::sync_manager::QueryPropagation::Full,
        Some(batch_id),
    ) {
        Ok(_) => panic!("query should reject committed batch"),
        Err(error) => error.to_string(),
    };
    assert_eq!(query_err, expected_error);
}

#[test]
fn rc_open_direct_batch_has_no_persisted_local_batch_record() {
    let mut core = create_test_runtime();
    let batch_id = BatchId::new();
    let write_context = WriteContext::default()
        .with_batch_mode(crate::batch_fate::BatchMode::Direct)
        .with_batch_id(batch_id);

    core.insert(
        "users",
        user_insert_values(ObjectId::new(), "Alice"),
        Some(&write_context),
    )
    .unwrap();

    assert_eq!(
        core.local_batch_record(batch_id).unwrap(),
        None,
        "open direct batches are in-memory builders until sealed"
    );
}

#[test]
fn rc_restart_recovers_completed_sealed_batch_from_storage() {
    let schema = test_schema();
    let schema_hash = SchemaHash::compute(&schema);
    let batch_id = BatchId::new();
    let row_id = ObjectId::new();
    let staged_row = staged_user_row(row_id, batch_id, 1_000, "Alice");

    let mut old_runtime = create_runtime_with_schema_and_sync_manager(
        schema.clone(),
        "transactional-restart-seal-recovery-test",
        SyncManager::new().with_durability_tier(DurabilityTier::Local),
    );
    old_runtime
        .storage_mut()
        .put_row_locator(
            row_id,
            Some(&RowLocator {
                table: "users".into(),
                origin_schema_hash: Some(schema_hash),
            }),
        )
        .unwrap();
    old_runtime
        .storage_mut()
        .append_history_region_rows("users", std::slice::from_ref(&staged_row))
        .unwrap();
    old_runtime
        .storage_mut()
        .upsert_sealed_batch_submission(&SealedBatchSubmission::new(
            batch_id,
            crate::batch_fate::BatchMode::Direct,
            crate::object::BranchName::new("main"),
            vec![SealedBatchMember {
                object_id: row_id,
                row_digest: staged_row.content_digest(),
            }],
            Vec::new(),
        ))
        .unwrap();

    let storage = old_runtime.into_storage();
    let restarted = create_runtime_with_storage_and_sync_manager(
        schema,
        "transactional-restart-seal-recovery-test",
        storage,
        SyncManager::new().with_durability_tier(DurabilityTier::Local),
    );

    let settlement = restarted
        .storage()
        .load_authoritative_batch_fate(batch_id)
        .unwrap()
        .expect("restart should recover and settle completed sealed batch");
    assert!(matches!(
        settlement,
        crate::batch_fate::BatchFate::DurableDirect {
            batch_id: settled_batch_id,
            confirmed_tier: DurabilityTier::Local,
        } if settled_batch_id == batch_id
    ));

    let visible = restarted
        .storage()
        .load_visible_region_row("users", "main", row_id)
        .unwrap()
        .expect("restart recovery should publish the durable direct row");
    assert_eq!(visible.state, crate::row_histories::RowState::VisibleDirect);
    assert_eq!(visible.batch_id, batch_id);
    assert_eq!(
        restarted
            .storage()
            .load_sealed_batch_submission(batch_id)
            .unwrap(),
        None,
        "recovered settlement should prune the sealed submission marker"
    );
}

#[test]
fn rc_local_only_runtime_settles_direct_batches_and_replays_nothing_on_worker_sync() {
    // A durable Local-tier runtime with no upstream server: the serverless
    // browser worker. After commit the batch is settled at this runtime's own
    // settlement target, so nothing should remain pending for worker sync or
    // reconciliation.
    let mut core = create_runtime_with_schema_and_sync_manager(
        test_schema(),
        "local-only-settlement",
        SyncManager::new().with_durability_tier(DurabilityTier::Local),
    );

    let ((_row_id, _), _batch_id) = core
        .insert("users", user_insert_values(ObjectId::new(), "Alice"), None)
        .unwrap();

    let replayed = core.local_batch_records_for_worker_sync().unwrap();
    assert!(
        replayed.is_empty(),
        "a local-only runtime settles at Local; worker sync must replay nothing, got {replayed:?}"
    );
    assert!(
        core.pending_batch_ids_needing_reconciliation_for_test()
            .is_empty(),
        "no batch should pend reconciliation on a serverless runtime"
    );
}

#[test]
fn rc_worker_with_upstream_retains_settled_submission_until_target_tier() {
    // B is a Local-tier worker with upstream C. When B settles A's batch at
    // Local, the submission is B's only durable membership record for a batch
    // that still owes upstream reconciliation.
    let mut s = create_3tier_rc();
    let ((_row_id, _), batch_id) =
        s.a.insert("users", user_insert_values(ObjectId::new(), "Alice"), None)
            .unwrap();
    pump_a_to_b(&mut s);

    assert_eq!(
        s.b.storage()
            .load_authoritative_batch_fate(batch_id)
            .unwrap()
            .and_then(|fate| fate.confirmed_tier()),
        Some(DurabilityTier::Local),
        "worker should settle the client batch at Local"
    );
    assert!(
        s.b.storage()
            .load_sealed_batch_submission(batch_id)
            .unwrap()
            .is_some(),
        "worker must retain the submission while the batch is below its settlement target"
    );
}

#[test]
fn rc_serverless_authority_prunes_submission_at_local_settlement() {
    // A Local-tier authority with no upstream settles at its own tier, so the
    // submission retires immediately.
    let mut b = create_runtime_with_schema_and_sync_manager(
        test_schema(),
        "serverless-authority-prune",
        SyncManager::new().with_durability_tier(DurabilityTier::Local),
    );

    let client_id = ClientId::new();
    b.add_client(client_id, None);
    b.schema_manager_mut()
        .query_manager_mut()
        .sync_manager_mut()
        .set_client_role(client_id, ClientRole::Peer);

    let mut a = create_runtime_with_schema(test_schema(), "serverless-authority-prune");
    let server_id = ServerId::new();
    a.add_server(server_id);
    a.batched_tick();
    a.sync_sender().take();

    let ((_row_id, _), batch_id) = a
        .insert("users", user_insert_values(ObjectId::new(), "Alice"), None)
        .unwrap();
    pump_client_messages_to_server(&mut a, &mut b, server_id, client_id);

    assert!(
        b.storage()
            .load_sealed_batch_submission(batch_id)
            .unwrap()
            .is_none(),
        "serverless authority settles at Local and retires the submission"
    );
}

#[test]
fn rc_client_retires_batch_bookkeeping_when_fate_reaches_settlement_target() {
    let mut s = create_3tier_rc();
    let ((_row_id, _), batch_id) =
        s.a.insert("users", user_insert_values(ObjectId::new(), "Alice"), None)
            .unwrap();
    assert!(
        s.a.storage()
            .load_sealed_batch_submission(batch_id)
            .unwrap()
            .is_some(),
        "submission pends while below the target"
    );

    s.a.push_sync_inbox(InboxEntry {
        source: Source::Server(s.b_server_for_a),
        payload: SyncPayload::BatchFate {
            fate: crate::batch_fate::BatchFate::DurableDirect {
                batch_id,
                confirmed_tier: DurabilityTier::GlobalServer,
            },
        },
    });
    s.a.immediate_tick();

    assert!(
        s.a.storage()
            .load_sealed_batch_submission(batch_id)
            .unwrap()
            .is_none(),
        "global fate retires the submission"
    );
    assert!(
        s.a.storage()
            .load_local_batch_record(batch_id)
            .unwrap()
            .is_none(),
        "global fate retires the local batch record"
    );
    assert!(
        s.a.storage()
            .load_authoritative_batch_fate(batch_id)
            .unwrap()
            .is_some(),
        "the fate itself stays as a terminal tombstone"
    );
    assert!(
        s.a.pending_batch_ids_needing_reconciliation_for_test()
            .is_empty()
    );
}

#[test]
fn rc_serverless_commit_retires_submission_immediately() {
    let mut core = create_test_runtime();
    let batch_id = core.begin_batch(crate::batch_fate::BatchMode::Direct);
    let write_context = WriteContext::default().with_batch_id(batch_id);
    let ((_row_id, _), _insert_batch_id) = core
        .insert(
            "users",
            user_insert_values(ObjectId::new(), "Alice"),
            Some(&write_context),
        )
        .unwrap();
    core.commit_batch(batch_id).unwrap();

    assert!(
        core.storage()
            .load_sealed_batch_submission(batch_id)
            .unwrap()
            .is_none(),
        "serverless commit settles at Local and must not retain the submission"
    );
    let results = execute_query(&mut core, Query::new("users"));
    assert_eq!(results.len(), 1);
}

#[test]
fn rc_wait_for_unattainable_tier_errors_instead_of_hanging() {
    let mut core = create_test_runtime();
    let ((_row_id, _), batch_id) = core
        .insert("users", user_insert_values(ObjectId::new(), "Alice"), None)
        .unwrap();

    assert!(
        core.wait_for_batch(batch_id, DurabilityTier::GlobalServer)
            .is_err(),
        "waiting on an unattainable tier must error immediately"
    );
    let mut receiver = core
        .wait_for_batch(batch_id, DurabilityTier::Local)
        .unwrap();
    assert_eq!(receiver.try_recv(), Ok(Some(Ok(()))));
}

#[test]
fn rc_non_durable_client_without_server_errors_on_local_wait() {
    let mut core = create_runtime_with_schema(test_schema(), "non-durable-no-server-wait");
    core.set_non_durable_client_runtime();

    let ((_row_id, _), batch_id) = core
        .insert("users", user_insert_values(ObjectId::new(), "Alice"), None)
        .unwrap();

    assert!(
        core.wait_for_batch(batch_id, DurabilityTier::Local)
            .is_err(),
        "a non-durable client with no upstream has no producer for any tier"
    );
}
