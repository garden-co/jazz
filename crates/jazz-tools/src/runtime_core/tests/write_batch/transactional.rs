use super::*;

#[test]
fn rc_committed_transaction_rejects_later_handle_operations_with_transaction_wording() {
    let mut core = create_test_runtime();
    let batch_id = core.begin_batch(crate::batch_fate::BatchMode::Transactional);
    let write_context = WriteContext::default().with_batch_id(batch_id);

    core.insert(
        "users",
        user_insert_values(ObjectId::new(), "Alice"),
        Some(&write_context),
    )
    .unwrap();
    core.commit_batch(batch_id).unwrap();

    let expected_error = format!("Write error: transaction {batch_id} is already committed");

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
        Ok(_) => panic!("query should reject committed transaction"),
        Err(error) => error.to_string(),
    };
    assert_eq!(query_err, expected_error);
}

#[test]
fn rc_rolled_back_transaction_rejects_later_handle_operations() {
    let mut core = create_test_runtime();
    let batch_id = core.begin_batch(crate::batch_fate::BatchMode::Transactional);
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
        Ok(_) => panic!("query should reject rolled-back transaction"),
        Err(error) => error.to_string(),
    };
    assert_eq!(query_err, expected_error);
}

#[test]
fn rc_transactional_insert_stays_local_until_authority_receives_it() {
    // alice stages one transactional write
    //   ordinary visible reads stay empty locally
    //   and nothing reaches the worker before sync runs
    let mut s = create_3tier_rc();
    let write_context = WriteContext {
        session: None,
        attribution: None,
        updated_at: None,
        batch_mode: Some(crate::batch_fate::BatchMode::Transactional),
        batch_id: None,
        target_branch_name: None,
    };

    let ((row_id, _row_values), _) =
        s.a.insert(
            "users",
            user_insert_values(ObjectId::new(), "Alice"),
            Some(&write_context),
        )
        .unwrap();

    assert_eq!(
        s.a.storage()
            .load_visible_region_row("users", s.a.schema_manager().branch_name().as_str(), row_id)
            .unwrap(),
        None,
        "ordinary visible state should ignore transactional staging rows"
    );

    assert_eq!(
        s.b.storage()
            .load_visible_region_row("users", s.b.schema_manager().branch_name().as_str(), row_id)
            .unwrap(),
        None,
        "upstream should not see the row before sync forwards it"
    );

    let history_rows =
        s.b.storage()
            .scan_history_row_batches("users", row_id)
            .unwrap();
    assert!(
        history_rows.is_empty(),
        "upstream should not receive transactional history before sync"
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
fn rc_transactional_insert_is_accepted_by_first_durable_upstream() {
    // alice stages one transactional row locally
    //   alice seals the batch
    //   worker accepts it into visible transactional state
    //   then alice learns that accepted visible state from sync
    let mut s = create_3tier_rc();
    let write_context = WriteContext {
        session: None,
        attribution: None,
        updated_at: None,
        batch_mode: Some(crate::batch_fate::BatchMode::Transactional),
        batch_id: None,
        target_branch_name: None,
    };

    let ((row_id, _row_values), _) =
        s.a.insert(
            "users",
            user_insert_values(ObjectId::new(), "Alice"),
            Some(&write_context),
        )
        .unwrap();
    let history_rows =
        s.a.storage()
            .scan_history_row_batches("users", row_id)
            .unwrap();
    assert_eq!(history_rows.len(), 1);
    let batch_id = history_rows[0].batch_id;
    s.a.commit_batch(batch_id).unwrap();

    pump_a_to_b(&mut s);

    let worker_row =
        s.b.storage()
            .load_visible_region_row("users", s.b.schema_manager().branch_name().as_str(), row_id)
            .unwrap()
            .expect("worker should materialize an accepted visible row");
    assert_eq!(
        worker_row.state,
        crate::row_histories::RowState::VisibleTransactional
    );
    assert_eq!(worker_row.confirmed_tier, Some(DurabilityTier::Local));
    assert_eq!(worker_row.batch_id(), batch_id);

    assert_eq!(
        s.a.storage()
            .load_visible_region_row("users", s.a.schema_manager().branch_name().as_str(), row_id)
            .unwrap(),
        None,
        "alice should still be waiting for the acceptance update before it is visible locally"
    );

    pump_b_to_a(&mut s);

    let client_row =
        s.a.storage()
            .load_visible_region_row("users", s.a.schema_manager().branch_name().as_str(), row_id)
            .unwrap()
            .expect("accepted transactional row should become visible on alice after sync");
    assert_eq!(
        client_row.state,
        crate::row_histories::RowState::VisibleTransactional
    );
    assert_eq!(client_row.confirmed_tier, Some(DurabilityTier::Local));
    assert_eq!(client_row.batch_id(), batch_id);
}

#[test]
fn rc_transactional_insert_is_accepted_only_after_batch_is_sealed() {
    // alice stages one transactional row locally
    //   worker receives the staged row but keeps it non-visible
    //   alice seals the batch
    //   worker accepts it and replays the settlement back
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

    pump_a_to_b(&mut s);

    assert_eq!(
        s.b.storage()
            .load_visible_region_row("users", s.b.schema_manager().branch_name().as_str(), row_id)
            .unwrap(),
        None,
        "worker should not publish the staged transactional row before seal"
    );
    assert_eq!(
        receiver.try_recv(),
        Ok(None),
        "persisted waiters should remain pending until the sealed batch settles"
    );

    s.a.commit_batch(batch_id).unwrap();
    pump_a_to_b(&mut s);

    let worker_row =
        s.b.storage()
            .load_visible_region_row("users", s.b.schema_manager().branch_name().as_str(), row_id)
            .unwrap()
            .expect("worker should publish the row after seal");
    assert_eq!(
        worker_row.state,
        crate::row_histories::RowState::VisibleTransactional
    );
    assert_eq!(worker_row.confirmed_tier, Some(DurabilityTier::Local));

    assert_eq!(
        receiver.try_recv(),
        Ok(None),
        "the local waiter should only resolve once alice receives the replayable settlement"
    );

    pump_b_to_a(&mut s);
    assert_eq!(receiver.try_recv(), Ok(Some(Ok(()))));
}

#[test]
fn rc_transactional_update_can_modify_row_inserted_earlier_in_same_batch() {
    // alice local runtime
    //   insert one staged transactional row
    //   update that same row again before sealing
    //   latest staged member should reflect the update
    let mut core = create_test_runtime();
    let batch_id = BatchId::new();
    let write_context = WriteContext {
        session: None,
        attribution: None,
        updated_at: None,
        batch_mode: Some(crate::batch_fate::BatchMode::Transactional),
        batch_id: Some(batch_id),
        target_branch_name: None,
    };

    let inserted_user_id = ObjectId::new();
    let ((row_id, _), _) = core
        .insert(
            "users",
            user_insert_values(inserted_user_id, "Alice"),
            Some(&write_context),
        )
        .expect("transactional insert should stage locally");

    core.update(
        row_id,
        vec![("name".to_string(), Value::Text("Bob".to_string()))],
        Some(&write_context),
    )
    .expect("transactional update should reuse the row staged earlier in the same batch");

    let history_rows = core
        .storage()
        .scan_history_row_batches("users", row_id)
        .unwrap();
    let latest_staged = history_rows
        .iter()
        .filter(|row| {
            row.batch_id == batch_id
                && matches!(row.state, crate::row_histories::RowState::StagingPending)
        })
        .max_by_key(|row| (row.updated_at, row.batch_id()))
        .expect("transaction should keep one staged member for the row");
    assert!(
        latest_staged.parents.is_empty(),
        "rewriting a row inserted earlier in the same batch should keep the insert's empty parent frontier"
    );
    let values = decode_row(
        &test_schema()[&TableName::new("users")].columns,
        &latest_staged.data,
    )
    .expect("latest staged row should decode");
    assert_eq!(values, user_row_values(inserted_user_id, "Bob"));
}

#[test]
fn rc_transactional_same_row_same_batch_collapses_to_one_live_staged_member() {
    // todo row visible on main
    //   tx update #1 changes title
    //   tx update #2 changes done
    //   latest staged member should compose both changes
    //   only one live staged member should remain for that row/batch
    let mut core = create_runtime_with_schema(defaulted_todos_schema(), "tx-write-set-collapse");
    let ((row_id, _), _) = core
        .insert(
            "todos",
            HashMap::from([("title".to_string(), Value::Text("Draft".to_string()))]),
            None,
        )
        .expect("seed visible todo");
    let base_visible = core
        .storage()
        .scan_history_row_batches("todos", row_id)
        .unwrap()
        .into_iter()
        .find(|row| matches!(row.state, crate::row_histories::RowState::VisibleDirect))
        .expect("seeded todo should be visible before the transaction");

    let batch_id = BatchId::new();
    let write_context = WriteContext {
        session: None,
        attribution: None,
        updated_at: None,
        batch_mode: Some(crate::batch_fate::BatchMode::Transactional),
        batch_id: Some(batch_id),
        target_branch_name: None,
    };

    core.update(
        row_id,
        vec![("title".to_string(), Value::Text("Renamed".to_string()))],
        Some(&write_context),
    )
    .expect("first transactional update should stage");
    core.update(
        row_id,
        vec![("done".to_string(), Value::Boolean(true))],
        Some(&write_context),
    )
    .expect("second transactional update should compose on the same staged row");

    let history_rows = core
        .storage()
        .scan_history_row_batches("todos", row_id)
        .unwrap();
    let transactional_rows: Vec<_> = history_rows
        .iter()
        .filter(|row| row.batch_id == batch_id)
        .collect();
    assert_eq!(transactional_rows.len(), 1);
    assert!(
        transactional_rows
            .iter()
            .all(|row| { row.parents.as_slice() == [base_visible.batch_id()] })
    );
    let live_staged_rows: Vec<_> = history_rows
        .iter()
        .filter(|row| {
            row.batch_id == batch_id
                && matches!(row.state, crate::row_histories::RowState::StagingPending)
        })
        .collect();
    assert_eq!(
        live_staged_rows.len(),
        1,
        "same-row transactional rewrites should keep one live staged member"
    );
    assert_eq!(
        live_staged_rows[0].parents.as_slice(),
        [base_visible.batch_id()]
    );
    let values = decode_row(
        &defaulted_todos_schema()[&TableName::new("todos")].columns,
        &live_staged_rows[0].data,
    )
    .expect("collapsed staged todo should decode");
    assert_eq!(
        values,
        vec![Value::Text("Renamed".to_string()), Value::Boolean(true),]
    );
}

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
    let branch_name = s.a.schema_manager().branch_name();

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
fn rc_wait_for_batch_resolves_transactional_accepted_settlement() {
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

    s.a.commit_batch(batch_id).unwrap();
    let mut batch_receiver = s.a.wait_for_batch(batch_id, DurabilityTier::Local).unwrap();

    pump_a_to_b(&mut s);
    assert_eq!(
        batch_receiver.try_recv(),
        Ok(None),
        "transactional batch wait should resolve only once alice receives the accepted settlement"
    );

    pump_b_to_a(&mut s);
    assert_eq!(batch_receiver.try_recv(), Ok(Some(Ok(()))));
}

#[test]
fn rc_transactional_insert_persisted_reconnect_reconciles_pending_batch_from_server() {
    // alice -> worker
    //   alice seals the transactional batch
    //   worker accepts it
    //   alice misses the live settlement
    //   reconnect replays the accepted settlement from current server truth
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
    let branch_name = s.a.schema_manager().branch_name();

    s.a.commit_batch(batch_id).unwrap();
    pump_a_to_b(&mut s);

    assert_eq!(
        receiver.try_recv(),
        Ok(None),
        "without the return settlement, the persisted receiver should still be pending"
    );

    s.a.remove_server(s.b_server_for_a);
    s.a.add_server(s.b_server_for_a);

    pump_a_to_b(&mut s);
    pump_b_to_a(&mut s);

    assert_eq!(
        receiver.try_recv(),
        Ok(Some(Ok(()))),
        "reconnect should reconcile the accepted transactional batch from the server"
    );

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
fn rc_transactional_persisted_writes_with_shared_batch_id_reconcile_as_one_batch() {
    // alice -> worker
    //   alice stages two transactional writes under one logical batch
    //   alice seals that shared batch once
    //   worker accepts both rows into one replayable accepted settlement
    //   alice resolves both durability waiters from that shared batch fate
    let mut s = create_3tier_rc();
    let batch_id = crate::row_histories::BatchId::new();
    let write_context = WriteContext::from_session(Session::new("alice"))
        .with_batch_mode(crate::batch_fate::BatchMode::Transactional)
        .with_batch_id(batch_id);

    let ((first_row_id, _first_row_values), mut first_receiver) = insert_and_wait_for_batch(
        &mut s.a,
        "users",
        user_insert_values(ObjectId::new(), "Alice"),
        Some(&write_context),
        DurabilityTier::Local,
    )
    .unwrap();
    let ((second_row_id, _second_row_values), mut second_receiver) = insert_and_wait_for_batch(
        &mut s.a,
        "users",
        user_insert_values(ObjectId::new(), "Bob"),
        Some(&write_context),
        DurabilityTier::Local,
    )
    .unwrap();

    let first_history_rows =
        s.a.storage()
            .scan_history_row_batches("users", first_row_id)
            .unwrap();
    let second_history_rows =
        s.a.storage()
            .scan_history_row_batches("users", second_row_id)
            .unwrap();
    assert_eq!(first_history_rows.len(), 1);
    assert_eq!(second_history_rows.len(), 1);
    assert_eq!(first_history_rows[0].batch_id, batch_id);
    assert_eq!(second_history_rows[0].batch_id, batch_id);

    assert!(
        s.a.storage().scan_local_batch_records().unwrap().is_empty(),
        "open shared transactional batches should not persist replayable durability records before seal"
    );

    s.a.commit_batch(batch_id).unwrap();
    pump_a_to_b(&mut s);
    assert_eq!(first_receiver.try_recv(), Ok(None));
    assert_eq!(second_receiver.try_recv(), Ok(None));

    pump_b_to_a(&mut s);
    assert_eq!(first_receiver.try_recv(), Ok(Some(Ok(()))));
    assert_eq!(second_receiver.try_recv(), Ok(Some(Ok(()))));

    let branch_name = s.a.schema_manager().branch_name();

    let worker_settlement =
        s.b.storage()
            .load_authoritative_batch_fate(batch_id)
            .unwrap()
            .expect("worker should persist the shared accepted settlement");
    match worker_settlement {
        crate::batch_fate::BatchFate::AcceptedTransaction {
            batch_id: settled_batch_id,
            confirmed_tier,
        } => {
            assert_eq!(settled_batch_id, batch_id);
            assert_eq!(confirmed_tier, DurabilityTier::Local);
        }
        other => panic!("expected accepted shared settlement, got {other:?}"),
    }

    match s
        .a
        .storage()
        .load_authoritative_batch_fate(batch_id)
        .unwrap()
    {
        Some(crate::batch_fate::BatchFate::AcceptedTransaction {
            batch_id: settled_batch_id,
            confirmed_tier,
        }) => {
            assert_eq!(settled_batch_id, batch_id);
            assert_eq!(confirmed_tier, DurabilityTier::Local);
        }
        other => panic!("expected accepted shared settlement locally, got {other:?}"),
    }
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

struct TwoClientTransactionHarness {
    alice: TestCore,
    bob: TestCore,
    server: TestCore,
    alice_client_id: ClientId,
    alice_server_id: ServerId,
    bob_client_id: ClientId,
    bob_server_id: ServerId,
    row_id: ObjectId,
    base_batch_id: BatchId,
}

fn create_two_client_transaction_harness(app_name: &str) -> TwoClientTransactionHarness {
    let schema = test_schema();
    let mut alice = create_runtime_with_schema(schema.clone(), app_name);
    let mut bob = create_runtime_with_schema(schema.clone(), app_name);
    let mut server = create_runtime_with_schema_and_sync_manager(
        schema,
        app_name,
        SyncManager::new().with_durability_tier(DurabilityTier::Local),
    );

    let alice_client_id = ClientId::new();
    let alice_server_id = ServerId::new();
    let bob_client_id = ClientId::new();
    let bob_server_id = ServerId::new();

    server.add_client(alice_client_id, None);
    server
        .schema_manager_mut()
        .query_manager_mut()
        .sync_manager_mut()
        .set_client_role(alice_client_id, ClientRole::Peer);
    server.add_client(bob_client_id, None);
    server
        .schema_manager_mut()
        .query_manager_mut()
        .sync_manager_mut()
        .set_client_role(bob_client_id, ClientRole::Peer);
    alice.add_server(alice_server_id);
    bob.add_server(bob_server_id);

    alice.immediate_tick();
    bob.immediate_tick();
    server.immediate_tick();
    alice.batched_tick();
    bob.batched_tick();
    server.batched_tick();
    alice.sync_sender().take();
    bob.sync_sender().take();
    server.sync_sender().take();

    let _bob_subscription = bob
        .subscribe(QueryBuilder::new("users").build(), |_| {}, None)
        .expect("bob should be able to subscribe to users");

    let mut harness = TwoClientTransactionHarness {
        alice,
        bob,
        server,
        alice_client_id,
        alice_server_id,
        bob_client_id,
        bob_server_id,
        row_id: ObjectId::new(),
        base_batch_id: BatchId::default(),
    };

    sync_server_with_clients(
        &mut harness.server,
        &mut [
            ClientForServer {
                core: &mut harness.alice,
                server_id: harness.alice_server_id,
                client_id: harness.alice_client_id,
            },
            ClientForServer {
                core: &mut harness.bob,
                server_id: harness.bob_server_id,
                client_id: harness.bob_client_id,
            },
        ],
    );

    let ((row_id, _row_values), base_batch_id) = harness
        .alice
        .insert("users", user_insert_values(ObjectId::new(), "Shared"), None)
        .expect("Alice should insert the shared base row through RuntimeCore");
    harness.row_id = row_id;
    harness.base_batch_id = base_batch_id;

    sync_server_with_clients(
        &mut harness.server,
        &mut [
            ClientForServer {
                core: &mut harness.alice,
                server_id: harness.alice_server_id,
                client_id: harness.alice_client_id,
            },
            ClientForServer {
                core: &mut harness.bob,
                server_id: harness.bob_server_id,
                client_id: harness.bob_client_id,
            },
        ],
    );
    assert_single_user_named(
        &mut harness.bob,
        row_id,
        "Shared",
        "Bob should learn the shared base row through sync before either transaction starts",
    );

    harness
}

fn transactional_write_context() -> WriteContext {
    WriteContext {
        session: None,
        attribution: None,
        updated_at: None,
        batch_mode: Some(crate::batch_fate::BatchMode::Transactional),
        batch_id: None,
        target_branch_name: None,
    }
}

fn users_named(core: &mut TestCore, name: &str) -> Vec<(ObjectId, Vec<Value>)> {
    execute_local_runtime_query(
        core,
        QueryBuilder::new("users")
            .filter_eq("name", Value::Text(name.to_string()))
            .build(),
        None,
    )
}

fn assert_single_user_named(core: &mut TestCore, row_id: ObjectId, name: &str, message: &str) {
    let rows = users_named(core, name);
    assert_eq!(
        rows.len(),
        1,
        "{message}: expected exactly one row, got {rows:?}"
    );
    assert_eq!(rows[0].0, row_id, "{message}: row id mismatch");
    assert_eq!(
        rows[0].1[1],
        Value::Text(name.to_string()),
        "{message}: name column mismatch"
    );
}

/// Two transactions modify the same object unaware of each other.
/// The server accepts the first tx and rejects the second.
#[test]
fn rc_authority_rejects_stale_update_after_concurrent_commit() {
    let mut harness = create_two_client_transaction_harness("two-client-transaction-conflict");

    let alice_context = transactional_write_context();
    let bob_context = transactional_write_context();

    let alice_batch_id = harness
        .alice
        .update(
            harness.row_id,
            vec![("name".to_string(), Value::Text("Alice".to_string()))],
            Some(&alice_context),
        )
        .unwrap();
    let bob_batch_id = harness
        .bob
        .update(
            harness.row_id,
            vec![("name".to_string(), Value::Text("Bob".to_string()))],
            Some(&bob_context),
        )
        .unwrap();

    harness.alice.commit_batch(alice_batch_id).unwrap();
    pump_client_messages_to_server(
        &mut harness.alice,
        &mut harness.server,
        harness.alice_server_id,
        harness.alice_client_id,
    );
    assert!(matches!(
        harness.server.batch_fate(alice_batch_id).unwrap(),
        Some(crate::batch_fate::BatchFate::AcceptedTransaction { .. })
    ));
    harness.server.sync_sender().take();

    harness.bob.commit_batch(bob_batch_id).unwrap();
    pump_client_messages_to_server(
        &mut harness.bob,
        &mut harness.server,
        harness.bob_server_id,
        harness.bob_client_id,
    );
    assert!(matches!(
        harness
            .server
            .batch_fate(bob_batch_id)
            .unwrap(),
        Some(crate::batch_fate::BatchFate::Rejected { code, .. })
            if code == "transaction_conflict"
    ));
}

/// Two transactions modify the same object.
/// Alice's tx commits first and Bob's tx sees Alice's update
/// AFTER its write (and before its commit).
/// The server accepts the first tx and rejects the second.
#[test]
fn rc_authority_rejects_update_staged_before_receiving_concurrent_commit() {
    let mut harness =
        create_two_client_transaction_harness("two-client-transaction-conflict-after-sync");

    let alice_context = transactional_write_context();
    let bob_context = transactional_write_context();

    let alice_batch_id = harness
        .alice
        .update(
            harness.row_id,
            vec![("name".to_string(), Value::Text("Alice".to_string()))],
            Some(&alice_context),
        )
        .unwrap();
    let bob_batch_id = harness
        .bob
        .update(
            harness.row_id,
            vec![("name".to_string(), Value::Text("Bob".to_string()))],
            Some(&bob_context),
        )
        .unwrap();

    harness.alice.commit_batch(alice_batch_id).unwrap();
    pump_client_messages_to_server(
        &mut harness.alice,
        &mut harness.server,
        harness.alice_server_id,
        harness.alice_client_id,
    );
    assert!(matches!(
        harness.server.batch_fate(alice_batch_id).unwrap(),
        Some(crate::batch_fate::BatchFate::AcceptedTransaction { .. })
    ));

    sync_server_with_clients(
        &mut harness.server,
        &mut [
            ClientForServer {
                core: &mut harness.alice,
                server_id: harness.alice_server_id,
                client_id: harness.alice_client_id,
            },
            ClientForServer {
                core: &mut harness.bob,
                server_id: harness.bob_server_id,
                client_id: harness.bob_client_id,
            },
        ],
    );
    assert_single_user_named(
        &mut harness.bob,
        harness.row_id,
        "Alice",
        "Bob should learn Alice's accepted transaction before sealing his own",
    );

    harness.bob.commit_batch(bob_batch_id).unwrap();
    sync_server_with_clients(
        &mut harness.server,
        &mut [
            ClientForServer {
                core: &mut harness.alice,
                server_id: harness.alice_server_id,
                client_id: harness.alice_client_id,
            },
            ClientForServer {
                core: &mut harness.bob,
                server_id: harness.bob_server_id,
                client_id: harness.bob_client_id,
            },
        ],
    );
    assert!(matches!(
        harness.server.batch_fate(bob_batch_id).unwrap(),
        Some(crate::batch_fate::BatchFate::Rejected { code, .. })
            if code == "transaction_conflict"
    ));
    assert_single_user_named(
        &mut harness.server,
        harness.row_id,
        "Alice",
        "Bob's stale staged transaction should not replace Alice's accepted transaction",
    );
}

/// Two transactions modify the same object.
/// Alice's tx commits first and Bob's tx sees Alice's update BEFORE its write.
/// The server accepts both transactions.
#[test]
fn rc_authority_accepts_update_staged_after_receiving_concurrent_commit() {
    let mut harness =
        create_two_client_transaction_harness("two-client-transaction-conflict-after-stage");

    let alice_context = transactional_write_context();
    let alice_batch_id = harness
        .alice
        .update(
            harness.row_id,
            vec![("name".to_string(), Value::Text("Alice".to_string()))],
            Some(&alice_context),
        )
        .unwrap();

    harness.alice.commit_batch(alice_batch_id).unwrap();
    pump_client_messages_to_server(
        &mut harness.alice,
        &mut harness.server,
        harness.alice_server_id,
        harness.alice_client_id,
    );
    assert!(matches!(
        harness.server.batch_fate(alice_batch_id).unwrap(),
        Some(crate::batch_fate::BatchFate::AcceptedTransaction { .. })
    ));

    sync_server_with_clients(
        &mut harness.server,
        &mut [
            ClientForServer {
                core: &mut harness.alice,
                server_id: harness.alice_server_id,
                client_id: harness.alice_client_id,
            },
            ClientForServer {
                core: &mut harness.bob,
                server_id: harness.bob_server_id,
                client_id: harness.bob_client_id,
            },
        ],
    );
    assert_single_user_named(
        &mut harness.bob,
        harness.row_id,
        "Alice",
        "Bob should stage his transaction from Alice's visible row",
    );

    let bob_context = transactional_write_context();
    let bob_batch_id = harness
        .bob
        .update(
            harness.row_id,
            vec![("name".to_string(), Value::Text("Bob".to_string()))],
            Some(&bob_context),
        )
        .unwrap();

    harness.bob.commit_batch(bob_batch_id).unwrap();
    sync_server_with_clients(
        &mut harness.server,
        &mut [
            ClientForServer {
                core: &mut harness.alice,
                server_id: harness.alice_server_id,
                client_id: harness.alice_client_id,
            },
            ClientForServer {
                core: &mut harness.bob,
                server_id: harness.bob_server_id,
                client_id: harness.bob_client_id,
            },
        ],
    );
    assert!(matches!(
        harness.server.batch_fate(bob_batch_id).unwrap(),
        Some(crate::batch_fate::BatchFate::AcceptedTransaction { .. })
    ));
    assert_single_user_named(
        &mut harness.server,
        harness.row_id,
        "Bob",
        "Bob's transaction should publish after sealing against the updated visible frontier",
    );
}
