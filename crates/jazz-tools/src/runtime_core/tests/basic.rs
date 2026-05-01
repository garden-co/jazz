use super::*;

#[test]
fn test_runtime_core_new() {
    let core = create_test_runtime();
    let schema = core.current_schema();
    assert!(schema.contains_key(&TableName::new("users")));
}

#[test]
fn test_runtime_core_insert_query() {
    let mut core = create_test_runtime();

    let user_id = ObjectId::new();
    let expected_values = user_row_values(user_id, "Alice");
    let ((object_id, row_values), _) = core
        .insert("users", user_insert_values(user_id, "Alice"), None)
        .unwrap();
    assert!(!object_id.uuid().is_nil());
    assert_eq!(row_values, expected_values);

    core.immediate_tick();
    core.batched_tick();

    let query = Query::new("users");
    let results = execute_query(&mut core, query);
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].0, object_id);
    assert_eq!(results[0].1, row_values);
}

#[test]
fn add_server_rehydrates_visible_rows_from_storage_after_restart() {
    let mut old_runtime = create_runtime_with_schema(test_schema(), "restart-sync-test");
    let user_id = ObjectId::new();
    let ((row_object_id, _), _) = old_runtime
        .insert("users", user_insert_values(user_id, "Alice"), None)
        .expect("insert should succeed before restart");

    let storage = old_runtime.into_storage();
    let mut restarted = create_runtime_with_storage(test_schema(), "restart-sync-test", storage);

    let server_id = ServerId::new();
    restarted.add_server(server_id);
    restarted.batched_tick();

    let messages = restarted.sync_sender().take();
    let synced_row = messages.iter().find(|message| match &message.payload {
        SyncPayload::RowBatchCreated { row, .. } => row.row_id == row_object_id,
        _ => false,
    });

    assert!(
        synced_row.is_some(),
        "row visible before restart should replay to a new server after restart; messages: {}",
        messages
            .iter()
            .map(|message| format!("{:?}", message.payload))
            .collect::<Vec<_>>()
            .join(", ")
    );
}

#[test]
fn batched_tick_applies_parked_sync_messages_in_bounded_slices() {
    use crate::runtime_core::ticks::MAX_SYNC_MESSAGES_PER_BATCHED_TICK;
    use crate::sync_manager::QueryId;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct CountingScheduler {
        scheduled: Arc<AtomicUsize>,
    }

    impl Scheduler for CountingScheduler {
        fn schedule_batched_tick(&self) {
            self.scheduled.fetch_add(1, Ordering::SeqCst);
        }
    }

    let scheduled = Arc::new(AtomicUsize::new(0));
    let app_id = AppId::from_name("bounded-sync-tick-test");
    let schema_manager =
        SchemaManager::new(SyncManager::new(), test_schema(), app_id, "dev", "main").unwrap();
    let mut core = new_test_core(
        schema_manager,
        MemoryStorage::new(),
        CountingScheduler {
            scheduled: scheduled.clone(),
        },
    );

    let server_id = ServerId::new();
    for sequence in 1..=(MAX_SYNC_MESSAGES_PER_BATCHED_TICK as u64 + 1) {
        core.park_sync_message_with_sequence(
            InboxEntry {
                source: Source::Server(server_id),
                payload: SyncPayload::QuerySettled {
                    query_id: QueryId(1),
                    tier: DurabilityTier::EdgeServer,
                    through_seq: sequence,
                },
            },
            sequence,
        );
    }
    scheduled.store(0, Ordering::SeqCst);

    core.batched_tick();
    assert_eq!(
        core.last_applied_server_seq.get(&server_id).copied(),
        Some(MAX_SYNC_MESSAGES_PER_BATCHED_TICK as u64)
    );
    assert!(
        scheduled.load(Ordering::SeqCst) > 0,
        "remaining ready messages should schedule another batched tick"
    );

    core.batched_tick();
    assert_eq!(
        core.last_applied_server_seq.get(&server_id).copied(),
        Some(MAX_SYNC_MESSAGES_PER_BATCHED_TICK as u64 + 1)
    );
}

#[test]
fn test_runtime_core_insert_materializes_schema_defaults() {
    let mut core = create_runtime_with_schema(defaulted_todos_schema(), "todos-with-defaults");

    let ((object_id, row_values), _) = core
        .insert(
            "todos",
            HashMap::from([("title".to_string(), Value::Text("Ship it".to_string()))]),
            None,
        )
        .unwrap();
    assert!(!object_id.uuid().is_nil());
    let descriptor = &core.current_schema()[&TableName::new("todos")].columns;
    let title_idx = descriptor.column_index("title").unwrap();
    let done_idx = descriptor.column_index("done").unwrap();
    assert_eq!(row_values[title_idx], Value::Text("Ship it".to_string()));
    assert_eq!(row_values[done_idx], Value::Boolean(false));

    core.immediate_tick();
    core.batched_tick();

    let results = execute_query(&mut core, Query::new("todos"));
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].0, object_id);
    assert_eq!(results[0].1, row_values);
}

#[test]
fn test_runtime_core_subscription() {
    let mut core = create_test_runtime();

    let updates: Arc<Mutex<Vec<SubscriptionDelta>>> = Arc::new(Mutex::new(Vec::new()));
    let updates_clone = updates.clone();

    let query = Query::new("users");
    let handle = core
        .subscribe(
            query,
            move |delta| {
                updates_clone.lock().unwrap().push(delta);
            },
            None,
        )
        .unwrap();

    let _object_id = core
        .insert("users", user_insert_values(ObjectId::new(), "Bob"), None)
        .unwrap();

    core.immediate_tick();
    core.batched_tick();

    let updates_vec = updates.lock().unwrap();
    assert!(
        !updates_vec.is_empty(),
        "Should receive subscription update"
    );
    assert_eq!(updates_vec[0].handle, handle);

    drop(updates_vec);
    core.unsubscribe(handle);
}

#[test]
fn test_runtime_core_concurrent_inserts_from_multiple_callers() {
    use std::thread;

    let core = Arc::new(Mutex::new(create_test_runtime()));
    let workers = 8;
    let mut handles = Vec::new();

    for i in 0..workers {
        let core_ref = Arc::clone(&core);
        handles.push(thread::spawn(move || {
            let mut locked = core_ref.lock().unwrap();
            locked
                .insert(
                    "users",
                    user_insert_values(ObjectId::new(), &format!("User-{i}")),
                    None,
                )
                .unwrap();
        }));
    }

    for handle in handles {
        handle.join().expect("worker thread should complete");
    }

    let mut locked = core.lock().unwrap();
    locked.immediate_tick();
    locked.batched_tick();

    let results = execute_query(&mut locked, Query::new("users"));
    assert_eq!(
        results.len(),
        workers,
        "All concurrent inserts should be visible"
    );
}

#[test]
fn test_runtime_core_update_delete() {
    let mut core = create_test_runtime();

    let id = ObjectId::new();
    let ((object_id, _row_values), _) = core
        .insert("users", user_insert_values(id, "Charlie"), None)
        .unwrap();
    core.immediate_tick();
    core.batched_tick();

    let updates = vec![("name".to_string(), Value::Text("Dave".to_string()))];
    core.update(object_id, updates, None).unwrap();
    core.immediate_tick();
    core.batched_tick();

    let query = Query::new("users");
    let results = execute_query(&mut core, query);
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].1[1], Value::Text("Dave".to_string()));

    core.delete(object_id, None).unwrap();
    core.immediate_tick();
    core.batched_tick();

    let query = Query::new("users");
    let results = execute_query(&mut core, query);
    assert_eq!(results.len(), 0);
}

#[test]
fn rc_user_inserted_row_stays_hidden_from_other_sessions() {
    let schema = protected_documents_schema();
    let mut client = create_runtime_with_schema(schema.clone(), "scope-bypass-test");
    let mut server = create_runtime_with_schema(schema, "scope-bypass-test");

    let alice_session = Session::new("alice");
    let title = "alice-private-doc";
    let client_id = ClientId::new();
    let server_id = ServerId::new();

    server.add_client(client_id, Some(alice_session.clone()));
    client.add_server(server_id);
    assert_eq!(
        server
            .schema_manager()
            .query_manager()
            .sync_manager()
            .get_client(client_id)
            .expect("client should be registered on server")
            .role,
        ClientRole::User,
        "test must exercise the user auth path instead of a trusted-role bypass"
    );

    // Clear any connection-startup traffic so this test only inspects the write under test.
    client.batched_tick();
    server.batched_tick();
    client.sync_sender().take();
    server.sync_sender().take();

    let ((document_id, row_values), _) = client
        .insert(
            "documents",
            document_insert_values("alice", title),
            Some(&WriteContext::from_session(alice_session.clone())),
        )
        .expect("alice insert should satisfy local insert policy");

    pump_client_messages_to_server(&mut client, &mut server, server_id, client_id);

    let alice_results = execute_runtime_query(
        &mut server,
        documents_query_by_title(title),
        Some(Session::new("alice")),
    );
    assert_eq!(
        alice_results.len(),
        1,
        "alice should be able to read her row"
    );
    assert_eq!(alice_results[0].0, document_id);
    assert_eq!(alice_results[0].1, row_values);

    let bob_results = execute_runtime_query(
        &mut server,
        documents_query_by_title(title),
        Some(Session::new("bob")),
    );
    assert!(
        bob_results.is_empty(),
        "bob should not be able to read alice's row after a client-originated insert"
    );
}

#[test]
fn rc_user_subscription_does_not_forward_rows_to_other_sessions() {
    let schema = protected_documents_schema();
    let mut writer = create_runtime_with_schema(schema.clone(), "scope-bypass-subscription-test");
    let mut alice_reader =
        create_runtime_with_schema(schema.clone(), "scope-bypass-subscription-test");
    let mut bob_reader =
        create_runtime_with_schema(schema.clone(), "scope-bypass-subscription-test");
    let mut server = create_runtime_with_schema_and_sync_manager(
        schema,
        "scope-bypass-subscription-test",
        SyncManager::new().with_durability_tier(DurabilityTier::Local),
    );

    let alice_session = Session::new("alice");
    let bob_session = Session::new("bob");
    let writer_client_id = ClientId::new();
    let writer_server_id = ServerId::new();
    let alice_reader_client_id = ClientId::new();
    let alice_reader_server_id = ServerId::new();
    let bob_reader_client_id = ClientId::new();
    let bob_reader_server_id = ServerId::new();
    let title = "alice-private-doc";

    server.add_client(writer_client_id, Some(alice_session.clone()));
    writer.add_server(writer_server_id);
    server.add_client(alice_reader_client_id, Some(alice_session.clone()));
    alice_reader.add_server(alice_reader_server_id);
    server.add_client(bob_reader_client_id, Some(bob_session.clone()));
    bob_reader.add_server(bob_reader_server_id);

    assert_eq!(
        server
            .schema_manager()
            .query_manager()
            .sync_manager()
            .get_client(writer_client_id)
            .expect("writer client should be registered on server")
            .role,
        ClientRole::User,
        "writer must use the user auth path"
    );
    assert_eq!(
        server
            .schema_manager()
            .query_manager()
            .sync_manager()
            .get_client(alice_reader_client_id)
            .expect("alice reader should be registered on server")
            .role,
        ClientRole::User,
        "alice reader must use the user auth path"
    );
    assert_eq!(
        server
            .schema_manager()
            .query_manager()
            .sync_manager()
            .get_client(bob_reader_client_id)
            .expect("bob reader should be registered on server")
            .role,
        ClientRole::User,
        "bob reader must use the user auth path"
    );

    let alice_deliveries = Arc::new(Mutex::new(Vec::<Vec<(ObjectId, Vec<Value>)>>::new()));
    let alice_deliveries_clone = alice_deliveries.clone();
    let _alice_reader_handle = alice_reader
        .subscribe(
            Query::new("documents"),
            move |delta| {
                let rows = decode_added_rows(&delta);
                if !rows.is_empty() {
                    alice_deliveries_clone.lock().unwrap().push(rows);
                }
            },
            Some(alice_session.clone()),
        )
        .expect("alice reader subscription should be created");

    let bob_deliveries = Arc::new(Mutex::new(Vec::<Vec<(ObjectId, Vec<Value>)>>::new()));
    let bob_deliveries_clone = bob_deliveries.clone();
    let _bob_reader_handle = bob_reader
        .subscribe(
            Query::new("documents"),
            move |delta| {
                let rows = decode_added_rows(&delta);
                if !rows.is_empty() {
                    bob_deliveries_clone.lock().unwrap().push(rows);
                }
            },
            Some(bob_session.clone()),
        )
        .expect("bob reader subscription should be created");

    pump_server_with_three_clients(
        &mut server,
        &mut writer,
        writer_server_id,
        writer_client_id,
        &mut alice_reader,
        alice_reader_server_id,
        alice_reader_client_id,
        &mut bob_reader,
        bob_reader_server_id,
        bob_reader_client_id,
    );

    assert_eq!(
        server
            .schema_manager()
            .query_manager()
            .sync_manager()
            .get_client(alice_reader_client_id)
            .expect("alice reader should still be connected")
            .queries
            .len(),
        1,
        "server should register alice's active query before the write"
    );
    assert_eq!(
        server
            .schema_manager()
            .query_manager()
            .sync_manager()
            .get_client(bob_reader_client_id)
            .expect("bob reader should still be connected")
            .queries
            .len(),
        1,
        "server should register bob's active query before the write"
    );

    let ((document_id, row_values), _) = writer
        .insert(
            "documents",
            document_insert_values("alice", title),
            Some(&WriteContext::from_session(alice_session.clone())),
        )
        .expect("alice insert should succeed through the public client API");

    let server_outputs_after_write = pump_server_with_three_clients(
        &mut server,
        &mut writer,
        writer_server_id,
        writer_client_id,
        &mut alice_reader,
        alice_reader_server_id,
        alice_reader_client_id,
        &mut bob_reader,
        bob_reader_server_id,
        bob_reader_client_id,
    );

    let server_results = execute_runtime_query(
        &mut server,
        documents_query_by_title(title),
        Some(alice_session.clone()),
    );
    assert_eq!(
        server_results,
        vec![(document_id, row_values.clone())],
        "server should store the synced row for alice"
    );
    assert!(
        outbox_has_object_update_for_client(
            &server_outputs_after_write,
            alice_reader_client_id,
            document_id,
        ),
        "server should forward alice's row to an authorized downstream alice reader"
    );
    assert!(
        !outbox_has_object_update_for_client(
            &server_outputs_after_write,
            bob_reader_client_id,
            document_id,
        ),
        "server must not forward alice's row to bob on the wire"
    );

    let alice_received_rows: Vec<(ObjectId, Vec<Value>)> = alice_deliveries
        .lock()
        .unwrap()
        .iter()
        .flat_map(|rows| rows.iter().cloned())
        .collect();
    assert_eq!(
        alice_received_rows,
        vec![(document_id, row_values.clone())],
        "authorized alice reader should receive exactly the inserted row"
    );

    let leaked_rows: Vec<(ObjectId, Vec<Value>)> = bob_deliveries
        .lock()
        .unwrap()
        .iter()
        .flat_map(|rows| rows.iter().cloned())
        .collect();
    assert!(
        leaked_rows.is_empty(),
        "bob should not receive alice's row through an active downstream subscription"
    );

    let alice_reader_results = execute_local_runtime_query(
        &mut alice_reader,
        documents_query_by_title(title),
        Some(alice_session.clone()),
    );
    assert_eq!(
        alice_reader_results,
        vec![(document_id, row_values.clone())],
        "authorized alice reader should also be able to query the synced row"
    );

    let bob_reader_results = execute_local_runtime_query(
        &mut bob_reader,
        documents_query_by_title(title),
        Some(bob_session.clone()),
    );
    assert!(
        bob_reader_results.is_empty(),
        "bob's local state should stay empty after alice's write is forwarded through the server"
    );

    let mut fresh_bob_query = bob_reader.query_with_propagation(
        documents_query_by_title(title),
        Some(bob_session.clone()),
        ReadDurabilityOptions {
            tier: Some(DurabilityTier::Local),
            local_updates: crate::query_manager::manager::LocalUpdates::Deferred,
        },
        crate::sync_manager::QueryPropagation::Full,
    );
    let waker = noop_waker();
    let mut cx = std::task::Context::from_waker(&waker);
    assert!(
        Pin::new(&mut fresh_bob_query).poll(&mut cx).is_pending(),
        "fresh bob full query should wait for Local settlement instead of resolving from local empty state"
    );

    let server_outputs_after_fresh_bob_query = pump_server_with_three_clients(
        &mut server,
        &mut writer,
        writer_server_id,
        writer_client_id,
        &mut alice_reader,
        alice_reader_server_id,
        alice_reader_client_id,
        &mut bob_reader,
        bob_reader_server_id,
        bob_reader_client_id,
    );
    assert!(
        !outbox_has_object_update_for_client(
            &server_outputs_after_fresh_bob_query,
            bob_reader_client_id,
            document_id,
        ),
        "fresh bob full query must not cause alice's row to be sent downstream"
    );

    match Pin::new(&mut fresh_bob_query).poll(&mut cx) {
        Poll::Ready(Ok(results)) => {
            assert!(
                results.is_empty(),
                "fresh bob full query should resolve to an empty result after server settlement"
            );
        }
        Poll::Ready(Err(err)) => panic!("fresh bob full query should succeed: {err:?}"),
        Poll::Pending => panic!("fresh bob full query should resolve after Local settlement"),
    }
}

#[test]
fn test_park_sync_message() {
    use crate::metadata::RowProvenance;
    use crate::sync_manager::{Source, SyncPayload};

    let mut core = create_test_runtime();

    let message = InboxEntry {
        source: Source::Server(ServerId::new()),
        payload: SyncPayload::RowBatchCreated {
            metadata: None,
            row: crate::row_histories::StoredRowBatch::new(
                ObjectId::new(),
                "main",
                Vec::new(),
                b"alice".to_vec(),
                RowProvenance::for_insert(ObjectId::new().to_string(), 1_000),
                HashMap::new(),
                crate::row_histories::RowState::VisibleDirect,
                None,
            ),
        },
    };
    core.park_sync_message(message);

    assert_eq!(core.parked_sync_messages.len(), 1);
}

// =========================================================================
// Durability API Tests (3-tier: A ↔ B[Worker] ↔ C[EdgeServer])
// =========================================================================
