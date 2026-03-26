use super::*;
use crate::query_manager::policy::PolicyExpr;
use crate::query_manager::query::QueryBuilder;
use crate::query_manager::types::{ColumnType, SchemaBuilder, TablePolicies, TableSchema};
use crate::schema_manager::AppId;
use crate::storage::MemoryStorage;
use crate::sync_manager::{
    ClientId, ClientRole, Destination, DurabilityTier, InboxEntry, OutboxEntry, ServerId, Source,
    SyncManager, SyncPayload,
};
use std::sync::{Arc, Mutex};

type TestCore = RuntimeCore<MemoryStorage, NoopScheduler, VecSyncSender>;

fn test_schema() -> Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("users")
                .column("id", ColumnType::Uuid)
                .column("name", ColumnType::Text),
        )
        .build()
}

fn protected_documents_schema() -> Schema {
    let policies = TablePolicies::new()
        .with_select(PolicyExpr::eq_session("owner_id", vec!["user_id".into()]))
        .with_insert(PolicyExpr::eq_session("owner_id", vec!["user_id".into()]));

    SchemaBuilder::new()
        .table(
            TableSchema::builder("documents")
                .column("owner_id", ColumnType::Text)
                .column("title", ColumnType::Text)
                .policies(policies),
        )
        .build()
}

fn create_runtime_with_schema_and_sync_manager(
    schema: Schema,
    app_name: &str,
    sync_manager: SyncManager,
) -> TestCore {
    let app_id = AppId::from_name(app_name);
    let schema_manager = SchemaManager::new(sync_manager, schema, app_id, "dev", "main").unwrap();
    let mut core = RuntimeCore::new(
        schema_manager,
        MemoryStorage::new(),
        NoopScheduler,
        VecSyncSender::new(),
    );
    core.immediate_tick();
    core
}

fn create_runtime_with_schema(schema: Schema, app_name: &str) -> TestCore {
    create_runtime_with_schema_and_sync_manager(schema, app_name, SyncManager::new())
}

fn create_test_runtime() -> TestCore {
    create_runtime_with_schema(test_schema(), "test-app")
}

fn documents_query_by_title(title: &str) -> Query {
    QueryBuilder::new("documents")
        .filter_eq("title", Value::Text(title.into()))
        .build()
}

/// Helper to execute a query synchronously via subscribe/tick/unsubscribe.
fn execute_query(core: &mut TestCore, query: Query) -> Vec<(ObjectId, Vec<Value>)> {
    let sub_id = core
        .schema_manager_mut()
        .query_manager_mut()
        .subscribe(query)
        .unwrap();
    core.immediate_tick();
    let results = core
        .schema_manager_mut()
        .query_manager_mut()
        .get_subscription_results(sub_id);
    core.schema_manager_mut()
        .query_manager_mut()
        .unsubscribe_with_sync(sub_id);
    results
}

fn execute_runtime_query(
    core: &mut TestCore,
    query: Query,
    session: Option<Session>,
) -> Vec<(ObjectId, Vec<Value>)> {
    execute_runtime_query_with_propagation(
        core,
        query,
        session,
        crate::sync_manager::QueryPropagation::Full,
    )
}

fn execute_local_runtime_query(
    core: &mut TestCore,
    query: Query,
    session: Option<Session>,
) -> Vec<(ObjectId, Vec<Value>)> {
    execute_runtime_query_with_propagation(
        core,
        query,
        session,
        crate::sync_manager::QueryPropagation::LocalOnly,
    )
}

fn execute_runtime_query_with_propagation(
    core: &mut TestCore,
    query: Query,
    session: Option<Session>,
    propagation: crate::sync_manager::QueryPropagation,
) -> Vec<(ObjectId, Vec<Value>)> {
    let waker = noop_waker();
    let mut cx = std::task::Context::from_waker(&waker);

    let mut future = core.query_with_propagation(
        query,
        session,
        ReadDurabilityOptions::default(),
        propagation,
    );

    match Pin::new(&mut future).poll(&mut cx) {
        Poll::Ready(Ok(results)) => results,
        Poll::Ready(Err(err)) => panic!("query should succeed: {err:?}"),
        Poll::Pending => panic!("query should resolve immediately"),
    }
}

fn decode_added_rows(delta: &SubscriptionDelta) -> Vec<(ObjectId, Vec<Value>)> {
    delta
        .ordered_delta
        .added
        .iter()
        .map(|row| {
            let values = decode_row(&delta.descriptor, &row.row.data).unwrap_or_else(|err| {
                panic!(
                    "subscription row {:?} should decode successfully: {err:?}",
                    row.row.id
                )
            });
            (row.row.id, values)
        })
        .collect()
}

fn pump_client_messages_to_server(
    client: &mut TestCore,
    server: &mut TestCore,
    server_id: ServerId,
    client_id: ClientId,
) {
    client.batched_tick();
    for entry in client.sync_sender().take() {
        if entry.destination == Destination::Server(server_id) {
            server.park_sync_message(InboxEntry {
                source: Source::Client(client_id),
                payload: entry.payload,
            });
        }
    }
    server.batched_tick();
    server.immediate_tick();
}

fn pump_server_with_three_clients(
    server: &mut TestCore,
    writer: &mut TestCore,
    writer_server_id: ServerId,
    writer_client_id: ClientId,
    alice_reader: &mut TestCore,
    alice_reader_server_id: ServerId,
    alice_reader_client_id: ClientId,
    bob_reader: &mut TestCore,
    bob_reader_server_id: ServerId,
    bob_reader_client_id: ClientId,
) -> Vec<OutboxEntry> {
    let mut server_outputs = Vec::new();

    for _ in 0..10 {
        let mut any_messages = false;

        writer.batched_tick();
        for entry in writer.sync_sender().take() {
            if entry.destination == Destination::Server(writer_server_id) {
                any_messages = true;
                server.park_sync_message(InboxEntry {
                    source: Source::Client(writer_client_id),
                    payload: entry.payload,
                });
            }
        }

        alice_reader.batched_tick();
        for entry in alice_reader.sync_sender().take() {
            if entry.destination == Destination::Server(alice_reader_server_id) {
                any_messages = true;
                server.park_sync_message(InboxEntry {
                    source: Source::Client(alice_reader_client_id),
                    payload: entry.payload,
                });
            }
        }

        bob_reader.batched_tick();
        for entry in bob_reader.sync_sender().take() {
            if entry.destination == Destination::Server(bob_reader_server_id) {
                any_messages = true;
                server.park_sync_message(InboxEntry {
                    source: Source::Client(bob_reader_client_id),
                    payload: entry.payload,
                });
            }
        }

        server.batched_tick();
        let server_out = server.sync_sender().take();
        server_outputs.extend(server_out.iter().cloned());
        for entry in server_out {
            match entry.destination {
                Destination::Client(client_id) if client_id == writer_client_id => {
                    any_messages = true;
                    writer.park_sync_message(InboxEntry {
                        source: Source::Server(writer_server_id),
                        payload: entry.payload,
                    });
                }
                Destination::Client(client_id) if client_id == alice_reader_client_id => {
                    any_messages = true;
                    alice_reader.park_sync_message(InboxEntry {
                        source: Source::Server(alice_reader_server_id),
                        payload: entry.payload,
                    });
                }
                Destination::Client(client_id) if client_id == bob_reader_client_id => {
                    any_messages = true;
                    bob_reader.park_sync_message(InboxEntry {
                        source: Source::Server(bob_reader_server_id),
                        payload: entry.payload,
                    });
                }
                _ => {}
            }
        }

        writer.batched_tick();
        writer.immediate_tick();
        alice_reader.batched_tick();
        alice_reader.immediate_tick();
        bob_reader.batched_tick();
        bob_reader.immediate_tick();

        if !any_messages {
            break;
        }
    }

    server_outputs
}

fn outbox_has_object_update_for_client(
    entries: &[OutboxEntry],
    client_id: ClientId,
    object_id: ObjectId,
) -> bool {
    entries.iter().any(|entry| {
        matches!(
            &entry.destination,
            Destination::Client(dest_client_id) if *dest_client_id == client_id
        ) && matches!(
            &entry.payload,
            SyncPayload::DocUpdated {
                doc_id,
                ..
            } if *doc_id == object_id
        )
    })
}

#[test]
fn test_runtime_core_new() {
    let core = create_test_runtime();
    let schema = core.current_schema();
    assert!(schema.contains_key(&TableName::new("users")));
}

#[test]
fn test_runtime_core_insert_query() {
    let mut core = create_test_runtime();

    let values = vec![
        Value::Uuid(ObjectId::new()),
        Value::Text("Alice".to_string()),
    ];
    let (object_id, row_values) = core.insert("users", values.clone(), None).unwrap();
    assert!(!object_id.0.is_nil());
    assert_eq!(row_values, values);

    core.immediate_tick();
    core.batched_tick();

    let query = Query::new("users");
    let results = execute_query(&mut core, query);
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

    let values = vec![Value::Uuid(ObjectId::new()), Value::Text("Bob".to_string())];
    let _object_id = core.insert("users", values, None).unwrap();

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
            let values = vec![
                Value::Uuid(ObjectId::new()),
                Value::Text(format!("User-{i}")),
            ];
            locked.insert("users", values, None).unwrap();
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
    let values = vec![Value::Uuid(id), Value::Text("Charlie".to_string())];
    let (object_id, _row_values) = core.insert("users", values, None).unwrap();
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

    let values = vec![Value::Text("alice".into()), Value::Text(title.into())];
    let (document_id, row_values) = client
        .insert("documents", values.clone(), Some(&alice_session))
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
        SyncManager::new().with_durability_tier(DurabilityTier::Worker),
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

    let values = vec![Value::Text("alice".into()), Value::Text(title.into())];
    let (document_id, row_values) = writer
        .insert("documents", values.clone(), Some(&alice_session))
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
            tier: Some(DurabilityTier::Worker),
            local_updates: crate::query_manager::manager::LocalUpdates::Deferred,
        },
        crate::sync_manager::QueryPropagation::Full,
    );
    let waker = noop_waker();
    let mut cx = std::task::Context::from_waker(&waker);
    assert!(
        Pin::new(&mut fresh_bob_query).poll(&mut cx).is_pending(),
        "fresh bob full query should wait for Worker settlement instead of resolving from local empty state"
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
        Poll::Pending => panic!("fresh bob full query should resolve after Worker settlement"),
    }
}

#[test]
fn test_park_sync_message() {
    use crate::object::BranchName;
    use crate::sync_manager::{Source, SyncPayload};

    let mut core = create_test_runtime();

    let message = InboxEntry {
        source: Source::Server(ServerId::new()),
        payload: SyncPayload::ObjectUpdated {
            object_id: ObjectId::new(),
            metadata: None,
            branch_name: BranchName::new("main"),
            commits: vec![],
        },
    };
    core.park_sync_message(message);

    assert_eq!(core.parked_sync_messages.len(), 1);
}

// =========================================================================
// Durability API Tests (3-tier: A ↔ B[Worker] ↔ C[EdgeServer])
// =========================================================================

/// Three-tier RuntimeCore setup for durability tests.
struct ThreeTierRC {
    a: TestCore,
    b: TestCore,
    c: TestCore,
    a_client_of_b: ClientId,
    b_server_for_a: ServerId,
    b_client_of_c: ClientId,
    c_server_for_b: ServerId,
}

fn create_3tier_rc() -> ThreeTierRC {
    let schema = test_schema();
    let app_id = AppId::from_name("durability-test");

    // A = client (no tier)
    let sm_a = SyncManager::new();
    let mgr_a = SchemaManager::new(sm_a, schema.clone(), app_id.clone(), "dev", "main").unwrap();
    let mut a = RuntimeCore::new(
        mgr_a,
        MemoryStorage::new(),
        NoopScheduler,
        VecSyncSender::new(),
    );

    // B = Worker server
    let sm_b = SyncManager::new().with_durability_tier(DurabilityTier::Worker);
    let mgr_b = SchemaManager::new(sm_b, schema.clone(), app_id.clone(), "dev", "main").unwrap();
    let mut b = RuntimeCore::new(
        mgr_b,
        MemoryStorage::new(),
        NoopScheduler,
        VecSyncSender::new(),
    );

    // C = EdgeServer
    let sm_c = SyncManager::new().with_durability_tier(DurabilityTier::EdgeServer);
    let mgr_c = SchemaManager::new(sm_c, schema, app_id, "dev", "main").unwrap();
    let mut c = RuntimeCore::new(
        mgr_c,
        MemoryStorage::new(),
        NoopScheduler,
        VecSyncSender::new(),
    );

    let a_client_of_b = ClientId::new();
    let b_server_for_a = ServerId::new();
    let b_client_of_c = ClientId::new();
    let c_server_for_b = ServerId::new();

    // Topology: A ↔ B ↔ C
    {
        let sm = b
            .schema_manager_mut()
            .query_manager_mut()
            .sync_manager_mut();
        sm.add_client(a_client_of_b);
        sm.set_client_role(a_client_of_b, ClientRole::Peer);
    }
    a.schema_manager_mut()
        .query_manager_mut()
        .sync_manager_mut()
        .add_server(b_server_for_a);

    {
        let sm = c
            .schema_manager_mut()
            .query_manager_mut()
            .sync_manager_mut();
        sm.add_client(b_client_of_c);
        sm.set_client_role(b_client_of_c, ClientRole::Peer);
    }
    b.schema_manager_mut()
        .query_manager_mut()
        .sync_manager_mut()
        .add_server(c_server_for_b);

    // Initial tick + clear initial sync messages
    a.immediate_tick();
    b.immediate_tick();
    c.immediate_tick();
    a.batched_tick();
    b.batched_tick();
    c.batched_tick();
    a.sync_sender().take();
    b.sync_sender().take();
    c.sync_sender().take();

    ThreeTierRC {
        a,
        b,
        c,
        a_client_of_b,
        b_server_for_a,
        b_client_of_c,
        c_server_for_b,
    }
}

/// Pump all messages between 3 RuntimeCore nodes until quiescent.
fn pump_3tier(s: &mut ThreeTierRC) {
    for _ in 0..10 {
        let mut any_messages = false;

        // A outbox → B
        s.a.batched_tick();
        let a_out = s.a.sync_sender().take();
        for entry in a_out {
            if entry.destination == Destination::Server(s.b_server_for_a) {
                any_messages = true;
                s.b.park_sync_message(InboxEntry {
                    source: Source::Client(s.a_client_of_b),
                    payload: entry.payload,
                });
            }
        }

        // B process, then route outbox to A or C
        s.b.batched_tick();
        s.b.immediate_tick();
        s.b.batched_tick();
        let b_out = s.b.sync_sender().take();
        for entry in b_out {
            match &entry.destination {
                Destination::Client(cid) if *cid == s.a_client_of_b => {
                    any_messages = true;
                    s.a.park_sync_message(InboxEntry {
                        source: Source::Server(s.b_server_for_a),
                        payload: entry.payload,
                    });
                }
                Destination::Server(sid) if *sid == s.c_server_for_b => {
                    any_messages = true;
                    s.c.park_sync_message(InboxEntry {
                        source: Source::Client(s.b_client_of_c),
                        payload: entry.payload,
                    });
                }
                _ => {}
            }
        }

        // C process, then route outbox to B
        s.c.batched_tick();
        s.c.immediate_tick();
        s.c.batched_tick();
        let c_out = s.c.sync_sender().take();
        for entry in c_out {
            if entry.destination == Destination::Client(s.b_client_of_c) {
                any_messages = true;
                s.b.park_sync_message(InboxEntry {
                    source: Source::Server(s.c_server_for_b),
                    payload: entry.payload,
                });
            }
        }

        // A processes incoming
        s.a.batched_tick();
        s.a.immediate_tick();

        if !any_messages {
            break;
        }
    }
}

/// Pump only A → B (one hop, no C).
fn pump_a_to_b(s: &mut ThreeTierRC) {
    s.a.batched_tick();
    let a_out = s.a.sync_sender().take();
    for entry in a_out {
        if entry.destination == Destination::Server(s.b_server_for_a) {
            s.b.park_sync_message(InboxEntry {
                source: Source::Client(s.a_client_of_b),
                payload: entry.payload,
            });
        }
    }
    s.b.batched_tick();
    s.b.immediate_tick();
}

/// Route B's outbox to both A and C as appropriate.
fn route_b_outbox(s: &mut ThreeTierRC) {
    s.b.batched_tick();
    let b_out = s.b.sync_sender().take();
    for entry in b_out {
        match &entry.destination {
            Destination::Client(cid) if *cid == s.a_client_of_b => {
                s.a.park_sync_message(InboxEntry {
                    source: Source::Server(s.b_server_for_a),
                    payload: entry.payload,
                });
            }
            Destination::Server(sid) if *sid == s.c_server_for_b => {
                s.c.park_sync_message(InboxEntry {
                    source: Source::Client(s.b_client_of_c),
                    payload: entry.payload,
                });
            }
            _ => {}
        }
    }
}

/// Pump B → A (acks back).
fn pump_b_to_a(s: &mut ThreeTierRC) {
    route_b_outbox(s);
    s.a.batched_tick();
    s.a.immediate_tick();
}

/// Pump B → C (forward to edge).
fn pump_b_to_c(s: &mut ThreeTierRC) {
    route_b_outbox(s);
    s.c.batched_tick();
    s.c.immediate_tick();
}

/// Pump C → B → A (edge ack relay).
fn pump_c_to_b_to_a(s: &mut ThreeTierRC) {
    // C → B
    s.c.batched_tick();
    let c_out = s.c.sync_sender().take();
    for entry in c_out {
        if entry.destination == Destination::Client(s.b_client_of_c) {
            s.b.park_sync_message(InboxEntry {
                source: Source::Server(s.c_server_for_b),
                payload: entry.payload,
            });
        }
    }
    s.b.batched_tick();
    s.b.immediate_tick();

    // B → A
    pump_b_to_a(s);
}

fn count_query_subscriptions_to_server(entries: &[OutboxEntry], server_id: ServerId) -> usize {
    entries
        .iter()
        .filter(|entry| {
            matches!(
                &entry.destination,
                Destination::Server(dest_server_id) if *dest_server_id == server_id
            ) && matches!(&entry.payload, SyncPayload::QuerySubscription { .. })
        })
        .count()
}

#[test]
fn rc_replays_downstream_query_when_upstream_added_late() {
    // Build A <-> B first (no B <-> C yet), so B processes a downstream
    // query subscription before it has any upstream server.
    let schema = test_schema();
    let app_id = AppId::from_name("query-replay-test");

    let mgr_a = SchemaManager::new(
        SyncManager::new(),
        schema.clone(),
        app_id.clone(),
        "dev",
        "main",
    )
    .unwrap();
    let mut a = RuntimeCore::new(
        mgr_a,
        MemoryStorage::new(),
        NoopScheduler,
        VecSyncSender::new(),
    );

    let mgr_b = SchemaManager::new(
        SyncManager::new().with_durability_tier(DurabilityTier::Worker),
        schema.clone(),
        app_id.clone(),
        "dev",
        "main",
    )
    .unwrap();
    let mut b = RuntimeCore::new(
        mgr_b,
        MemoryStorage::new(),
        NoopScheduler,
        VecSyncSender::new(),
    );

    let mgr_c = SchemaManager::new(
        SyncManager::new().with_durability_tier(DurabilityTier::EdgeServer),
        schema,
        app_id,
        "dev",
        "main",
    )
    .unwrap();
    let mut c = RuntimeCore::new(
        mgr_c,
        MemoryStorage::new(),
        NoopScheduler,
        VecSyncSender::new(),
    );

    let a_client_of_b = ClientId::new();
    let b_server_for_a = ServerId::new();
    let b_client_of_c = ClientId::new();
    let c_server_for_b = ServerId::new();

    {
        let sm = b
            .schema_manager_mut()
            .query_manager_mut()
            .sync_manager_mut();
        sm.add_client(a_client_of_b);
        sm.set_client_role(a_client_of_b, ClientRole::Peer);
    }
    a.schema_manager_mut()
        .query_manager_mut()
        .sync_manager_mut()
        .add_server(b_server_for_a);

    // Clear any startup sync traffic.
    a.immediate_tick();
    b.immediate_tick();
    c.immediate_tick();
    a.batched_tick();
    b.batched_tick();
    c.batched_tick();
    a.sync_sender().take();
    b.sync_sender().take();
    c.sync_sender().take();

    // Downstream client A subscribes before B has an upstream.
    let _handle = a.subscribe(Query::new("users"), |_delta| {}, None).unwrap();

    // Deliver only A -> B messages.
    a.batched_tick();
    for entry in a.sync_sender().take() {
        if entry.destination == Destination::Server(b_server_for_a) {
            b.park_sync_message(InboxEntry {
                source: Source::Client(a_client_of_b),
                payload: entry.payload,
            });
        }
    }
    b.batched_tick();
    b.immediate_tick();
    b.batched_tick();
    b.sync_sender().take();

    // Bring up B <-> C after B already has active downstream query state.
    {
        let sm = c
            .schema_manager_mut()
            .query_manager_mut()
            .sync_manager_mut();
        sm.add_client(b_client_of_c);
        sm.set_client_role(b_client_of_c, ClientRole::Peer);
    }
    b.add_server(c_server_for_b);
    b.batched_tick();

    let forwarded_query_subscriptions = b
        .sync_sender()
        .take()
        .into_iter()
        .filter(|entry| {
            matches!(
                &entry.destination,
                Destination::Server(server_id) if *server_id == c_server_for_b
            ) && matches!(&entry.payload, SyncPayload::QuerySubscription { .. })
        })
        .count();

    assert!(
        forwarded_query_subscriptions > 0,
        "Expected B to replay existing downstream QuerySubscription(s) when adding upstream"
    );
}

#[test]
fn rc_replays_active_queries_on_upstream_reconnect() {
    let mut s = create_3tier_rc();

    let _handle =
        s.a.subscribe(Query::new("users"), |_delta| {}, None)
            .unwrap();
    pump_a_to_b(&mut s);

    let initial_forwarded = s.b.sync_sender().take();
    assert!(
        count_query_subscriptions_to_server(&initial_forwarded, s.c_server_for_b) > 0,
        "Expected initial QuerySubscription forwarding from B to C"
    );

    // Simulate upstream disconnect/reconnect.
    s.b.remove_server(s.c_server_for_b);
    s.b.add_server(s.c_server_for_b);
    s.b.batched_tick();

    let replayed_forwarded = s.b.sync_sender().take();
    assert!(
        count_query_subscriptions_to_server(&replayed_forwarded, s.c_server_for_b) > 0,
        "Expected active QuerySubscription replay after upstream reconnect"
    );
}

#[test]
fn rc_does_not_replay_unsubscribed_queries_on_upstream_reconnect() {
    let mut s = create_3tier_rc();

    let handle =
        s.a.subscribe(Query::new("users"), |_delta| {}, None)
            .unwrap();
    pump_a_to_b(&mut s);

    let initial_forwarded = s.b.sync_sender().take();
    assert!(
        count_query_subscriptions_to_server(&initial_forwarded, s.c_server_for_b) > 0,
        "Expected initial QuerySubscription forwarding from B to C"
    );

    s.a.unsubscribe(handle);
    pump_a_to_b(&mut s);
    s.b.sync_sender().take(); // Drain unsubscription forwarding and unrelated traffic.

    // Reconnect upstream and ensure replay no longer includes this query.
    s.b.remove_server(s.c_server_for_b);
    s.b.add_server(s.c_server_for_b);
    s.b.batched_tick();

    let replayed_forwarded = s.b.sync_sender().take();
    assert_eq!(
        count_query_subscriptions_to_server(&replayed_forwarded, s.c_server_for_b),
        0,
        "Unsubscribed query must not be replayed after upstream reconnect"
    );
}

#[test]
fn rc_insert_returns_immediately() {
    let mut s = create_3tier_rc();
    let values = vec![Value::Uuid(ObjectId::new()), Value::Text("Alice".into())];
    let (id, row_values) = s.a.insert("users", values.clone(), None).unwrap();
    assert!(!id.0.is_nil());
    assert_eq!(row_values, values);

    let query = Query::new("users");
    let results = execute_query(&mut s.a, query);
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].0, id);
    assert_eq!(results[0].1, row_values);
}

#[test]
fn rc_insert_data_syncs_to_server() {
    let mut s = create_3tier_rc();
    let values = vec![Value::Uuid(ObjectId::new()), Value::Text("Alice".into())];
    let (id, _row_values) = s.a.insert("users", values, None).unwrap();

    pump_a_to_b(&mut s);

    let query = Query::new("users");
    let results = execute_query(&mut s.b, query);
    assert_eq!(results.len(), 1, "Server B should have the synced row");
    assert_eq!(results[0].0, id);
}

#[test]
fn rc_update_sync() {
    let mut s = create_3tier_rc();
    let values = vec![Value::Uuid(ObjectId::new()), Value::Text("Alice".into())];
    let (id, _row_values) = s.a.insert("users", values, None).unwrap();
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
fn rc_delete_sync() {
    let mut s = create_3tier_rc();
    let values = vec![Value::Uuid(ObjectId::new()), Value::Text("Alice".into())];
    let (id, _row_values) = s.a.insert("users", values, None).unwrap();
    pump_a_to_b(&mut s);

    s.a.delete(id, None).unwrap();
    pump_a_to_b(&mut s);

    let query = Query::new("users");
    let results = execute_query(&mut s.b, query);
    assert_eq!(results.len(), 0, "Row should be deleted on B");
}

#[test]
fn rc_insert_persisted_resolves_on_worker_ack() {
    let mut s = create_3tier_rc();
    let values = vec![Value::Uuid(ObjectId::new()), Value::Text("Alice".into())];
    let ((id, row_values), mut receiver) =
        s.a.insert_persisted("users", values.clone(), None, DurabilityTier::Worker)
            .unwrap();
    assert!(!id.0.is_nil());
    assert_eq!(row_values, values);

    assert!(
        receiver.try_recv().is_err() || receiver.try_recv() == Ok(None),
        "Receiver should not be resolved before ack"
    );

    pump_a_to_b(&mut s);
    pump_b_to_a(&mut s);

    match receiver.try_recv() {
        Ok(Some(())) => {}
        Ok(None) => panic!("Receiver should be resolved after Worker ack"),
        Err(_) => panic!("Receiver was cancelled"),
    }
}

#[test]
fn rc_insert_persisted_holds_until_correct_tier() {
    let mut s = create_3tier_rc();
    let values = vec![Value::Uuid(ObjectId::new()), Value::Text("Alice".into())];
    let (_id, mut receiver) =
        s.a.insert_persisted("users", values, None, DurabilityTier::EdgeServer)
            .unwrap();

    pump_a_to_b(&mut s);
    pump_b_to_a(&mut s);

    assert_eq!(
        receiver.try_recv(),
        Ok(None),
        "Worker ack should not satisfy EdgeServer request"
    );

    pump_b_to_c(&mut s);
    pump_c_to_b_to_a(&mut s);

    match receiver.try_recv() {
        Ok(Some(())) => {}
        Ok(None) => panic!("Receiver should be resolved after EdgeServer ack"),
        Err(_) => panic!("Receiver was cancelled"),
    }
}

#[test]
fn rc_insert_persisted_higher_tier_satisfies_lower() {
    let mut s = create_3tier_rc();
    let values = vec![Value::Uuid(ObjectId::new()), Value::Text("Alice".into())];
    let (_id, mut receiver) =
        s.a.insert_persisted("users", values, None, DurabilityTier::Worker)
            .unwrap();

    pump_3tier(&mut s);

    match receiver.try_recv() {
        Ok(Some(())) => {}
        Ok(None) => panic!("EdgeServer ack should satisfy Worker request"),
        Err(_) => panic!("Receiver was cancelled"),
    }
}

#[test]
fn rc_update_persisted_resolves_on_ack() {
    let mut s = create_3tier_rc();
    let values = vec![Value::Uuid(ObjectId::new()), Value::Text("Alice".into())];
    let (id, _row_values) = s.a.insert("users", values, None).unwrap();
    pump_a_to_b(&mut s);

    let mut receiver =
        s.a.update_persisted(
            id,
            vec![("name".into(), Value::Text("Bob".into()))],
            None,
            DurabilityTier::Worker,
        )
        .unwrap();

    pump_a_to_b(&mut s);
    pump_b_to_a(&mut s);

    match receiver.try_recv() {
        Ok(Some(())) => {}
        Ok(None) => panic!("Update receiver should be resolved after Worker ack"),
        Err(_) => panic!("Receiver was cancelled"),
    }

    let query = Query::new("users");
    let results = execute_query(&mut s.b, query);
    assert_eq!(results[0].1[1], Value::Text("Bob".into()));
}

#[test]
fn rc_delete_persisted_resolves_on_ack() {
    let mut s = create_3tier_rc();
    let values = vec![Value::Uuid(ObjectId::new()), Value::Text("Alice".into())];
    let (id, _row_values) = s.a.insert("users", values, None).unwrap();
    pump_a_to_b(&mut s);

    let mut receiver =
        s.a.delete_persisted(id, None, DurabilityTier::Worker)
            .unwrap();

    pump_a_to_b(&mut s);
    pump_b_to_a(&mut s);

    match receiver.try_recv() {
        Ok(Some(())) => {}
        Ok(None) => panic!("Delete receiver should be resolved after Worker ack"),
        Err(_) => panic!("Receiver was cancelled"),
    }

    let query = Query::new("users");
    let results = execute_query(&mut s.b, query);
    assert_eq!(results.len(), 0);
}

#[test]
fn rc_multiple_persisted_inserts_independent() {
    let mut s = create_3tier_rc();

    let values1 = vec![Value::Uuid(ObjectId::new()), Value::Text("Alice".into())];
    let (_id1, mut receiver1) =
        s.a.insert_persisted("users", values1, None, DurabilityTier::Worker)
            .unwrap();

    let values2 = vec![Value::Uuid(ObjectId::new()), Value::Text("Bob".into())];
    let (_id2, mut receiver2) =
        s.a.insert_persisted("users", values2, None, DurabilityTier::Worker)
            .unwrap();

    pump_3tier(&mut s);

    match receiver1.try_recv() {
        Ok(Some(())) => {}
        Ok(None) => panic!("receiver1 should be resolved"),
        Err(_) => panic!("receiver1 cancelled"),
    }
    match receiver2.try_recv() {
        Ok(Some(())) => {}
        Ok(None) => panic!("receiver2 should be resolved"),
        Err(_) => panic!("receiver2 cancelled"),
    }
}

#[test]
fn rc_query_no_settled_tier_immediate() {
    let mut s = create_3tier_rc();

    let values = vec![Value::Uuid(ObjectId::new()), Value::Text("Alice".into())];
    let (id, _row_values) = s.a.insert("users", values, None).unwrap();

    let mut future = s.a.query(Query::new("users"), None);

    let waker = noop_waker();
    let mut cx = std::task::Context::from_waker(&waker);
    match Pin::new(&mut future).poll(&mut cx) {
        Poll::Ready(Ok(results)) => {
            assert_eq!(results.len(), 1, "Should have one row");
            assert_eq!(results[0].0, id);
        }
        Poll::Ready(Err(e)) => panic!("Query failed: {:?}", e),
        Poll::Pending => panic!("Query with settled_tier=None should resolve immediately"),
    }
}

#[test]
fn rc_query_settled_tier_holds() {
    let mut s = create_3tier_rc();

    let values = vec![Value::Uuid(ObjectId::new()), Value::Text("Alice".into())];
    let (id, _row_values) = s.a.insert("users", values, None).unwrap();

    let mut future = s.a.query_with_propagation(
        Query::new("users"),
        None,
        ReadDurabilityOptions {
            tier: Some(DurabilityTier::Worker),
            local_updates: crate::query_manager::manager::LocalUpdates::Immediate,
        },
        crate::sync_manager::QueryPropagation::Full,
    );

    let waker = noop_waker();
    let mut cx = std::task::Context::from_waker(&waker);
    assert!(
        Pin::new(&mut future).poll(&mut cx).is_pending(),
        "Query should be pending before Worker settlement"
    );

    pump_a_to_b(&mut s);
    pump_b_to_a(&mut s);

    match Pin::new(&mut future).poll(&mut cx) {
        Poll::Ready(Ok(results)) => {
            assert_eq!(results.len(), 1, "Should have one row after settlement");
            assert_eq!(results[0].0, id);
        }
        Poll::Ready(Err(e)) => panic!("Query failed: {:?}", e),
        Poll::Pending => panic!("Query should resolve after Worker QuerySettled"),
    }
}

#[test]
fn rc_query_settled_tier_empty_resolves() {
    let mut s = create_3tier_rc();

    let mut future = s.a.query_with_propagation(
        Query::new("users"),
        None,
        ReadDurabilityOptions {
            tier: Some(DurabilityTier::Worker),
            local_updates: crate::query_manager::manager::LocalUpdates::Immediate,
        },
        crate::sync_manager::QueryPropagation::Full,
    );

    let waker = noop_waker();
    let mut cx = std::task::Context::from_waker(&waker);
    assert!(
        Pin::new(&mut future).poll(&mut cx).is_pending(),
        "Query should be pending before Worker settlement"
    );

    // No rows inserted anywhere; query should still resolve once settled tier is reached.
    pump_a_to_b(&mut s);
    pump_b_to_a(&mut s);

    match Pin::new(&mut future).poll(&mut cx) {
        Poll::Ready(Ok(results)) => {
            assert_eq!(
                results.len(),
                0,
                "Settled query with no rows should resolve to empty result"
            );
        }
        Poll::Ready(Err(e)) => panic!("Query failed: {:?}", e),
        Poll::Pending => panic!("Query should resolve after Worker QuerySettled"),
    }
}

#[test]
fn rc_query_settled_before_data_should_not_drop_upstream_rows() {
    let mut s = create_3tier_rc();

    // Seed data on server B that client A has not synced yet.
    let values = vec![
        Value::Uuid(ObjectId::new()),
        Value::Text("upstream-row".into()),
    ];
    let (row_id, _row_values) = s.b.insert("users", values, None).unwrap();
    s.b.immediate_tick();
    s.b.batched_tick();
    s.b.sync_sender().take();

    // One-shot settled query on A should wait for Worker settlement.
    let mut future = s.a.query_with_propagation(
        Query::new("users"),
        None,
        ReadDurabilityOptions {
            tier: Some(DurabilityTier::Worker),
            local_updates: crate::query_manager::manager::LocalUpdates::Immediate,
        },
        crate::sync_manager::QueryPropagation::Full,
    );

    let waker = noop_waker();
    let mut cx = std::task::Context::from_waker(&waker);
    assert!(
        Pin::new(&mut future).poll(&mut cx).is_pending(),
        "Query should be pending before Worker settlement"
    );

    // Deliver A -> B query subscription and let B compute response traffic.
    pump_a_to_b(&mut s);
    s.b.batched_tick();
    let b_out = s.b.sync_sender().take();

    // Force QuerySettled before DocUpdated to expose ordering assumptions.
    let mut settled_to_a = Vec::new();
    let mut updates_to_a = Vec::new();
    for entry in b_out {
        if entry.destination != Destination::Client(s.a_client_of_b) {
            continue;
        }
        match entry.payload {
            payload @ SyncPayload::QuerySettled { .. } => settled_to_a.push(payload),
            payload @ SyncPayload::DocUpdated { .. } => updates_to_a.push(payload),
            _ => {}
        }
    }

    assert!(
        !settled_to_a.is_empty(),
        "Expected QuerySettled notification for A"
    );
    assert!(
        !updates_to_a.is_empty(),
        "Expected DocUpdated payload for A"
    );

    // Mirror connected stream initialization: first expected seq is 1.
    s.a.set_next_expected_server_sequence(s.b_server_for_a, 1);

    let mut next_update_seq = 1u64;
    let settled_seq_base = updates_to_a.len() as u64 + 1;

    for (idx, payload) in settled_to_a.into_iter().enumerate() {
        s.a.park_sync_message_with_sequence(
            InboxEntry {
                source: Source::Server(s.b_server_for_a),
                payload,
            },
            settled_seq_base + idx as u64,
        );
    }
    s.a.batched_tick();
    s.a.immediate_tick();

    assert!(
        Pin::new(&mut future).poll(&mut cx).is_pending(),
        "Query should stay pending until lower sequence ObjectUpdated arrives"
    );

    for payload in updates_to_a {
        s.a.park_sync_message_with_sequence(
            InboxEntry {
                source: Source::Server(s.b_server_for_a),
                payload,
            },
            next_update_seq,
        );
        next_update_seq += 1;
    }
    s.a.batched_tick();
    s.a.immediate_tick();

    match Pin::new(&mut future).poll(&mut cx) {
        Poll::Ready(Ok(results)) => {
            assert_eq!(
                results.len(),
                1,
                "Sequenced delivery should prevent settled-before-data resolution"
            );
            assert_eq!(results[0].0, row_id);
        }
        Poll::Ready(Err(e)) => panic!("Query failed: {:?}", e),
        Poll::Pending => panic!("Query should resolve after DocUpdated and QuerySettled"),
    }
}

#[test]
fn rc_subscribe_settled_tier() {
    let mut s = create_3tier_rc();

    let received = Arc::new(Mutex::new(Vec::<Vec<(ObjectId, Vec<Value>)>>::new()));
    let received_clone = received.clone();

    let _handle =
        s.a.subscribe_with_durability_and_propagation(
            Query::new("users"),
            move |delta| {
                let rows = decode_added_rows(&delta);
                received_clone.lock().unwrap().push(rows);
            },
            None,
            ReadDurabilityOptions {
                tier: Some(DurabilityTier::Worker),
                local_updates: crate::query_manager::manager::LocalUpdates::Deferred,
            },
            crate::sync_manager::QueryPropagation::Full,
        )
        .unwrap();

    let values = vec![Value::Uuid(ObjectId::new()), Value::Text("Alice".into())];
    let (id, _row_values) = s.a.insert("users", values, None).unwrap();
    s.a.immediate_tick();

    assert!(
        received.lock().unwrap().is_empty(),
        "Callback should not fire before Worker settlement"
    );

    pump_a_to_b(&mut s);
    pump_b_to_a(&mut s);

    let calls = received.lock().unwrap();
    assert!(
        !calls.is_empty(),
        "Callback should fire after Worker QuerySettled"
    );
    let first_delivery = &calls[0];
    assert_eq!(first_delivery.len(), 1, "Should have one row");
    assert_eq!(first_delivery[0].0, id);
}

#[test]
fn rc_subscribe_remote_tier_immediate_local_updates() {
    let mut s = create_3tier_rc();

    let received = Arc::new(Mutex::new(Vec::<Vec<(ObjectId, Vec<Value>)>>::new()));
    let received_clone = received.clone();

    let _handle =
        s.a.subscribe_with_durability_and_propagation(
            Query::new("users"),
            move |delta| {
                let rows = decode_added_rows(&delta);
                received_clone.lock().unwrap().push(rows);
            },
            None,
            ReadDurabilityOptions {
                tier: Some(DurabilityTier::EdgeServer),
                local_updates: crate::query_manager::manager::LocalUpdates::Immediate,
            },
            crate::sync_manager::QueryPropagation::Full,
        )
        .unwrap();

    // Initial delivery should still wait for the requested remote tier.
    let values = vec![
        Value::Uuid(ObjectId::new()),
        Value::Text("local-first".into()),
    ];
    let (first_id, _row_values) = s.a.insert("users", values, None).unwrap();
    s.a.immediate_tick();

    let calls = received.lock().unwrap();
    assert!(
        calls.is_empty(),
        "Initial delivery should wait for EdgeServer settlement"
    );
    drop(calls);

    // Worker settlement is not enough for an EdgeServer subscription.
    pump_a_to_b(&mut s);
    pump_b_to_a(&mut s);
    assert!(
        received.lock().unwrap().is_empty(),
        "Worker tier should not unlock first delivery for EdgeServer reads"
    );

    // Reach EdgeServer settlement for the initial snapshot.
    pump_b_to_c(&mut s);
    pump_c_to_b_to_a(&mut s);

    let calls = received.lock().unwrap();
    assert!(
        !calls.is_empty(),
        "First delivery should happen once EdgeServer settlement is reached"
    );
    let first_delivery = &calls[0];
    assert_eq!(
        first_delivery.len(),
        1,
        "First snapshot should include one row"
    );
    assert_eq!(first_delivery[0].0, first_id);
    drop(calls);

    // After initial delivery, local updates should callback immediately.
    let second_values = vec![
        Value::Uuid(ObjectId::new()),
        Value::Text("local-second".into()),
    ];
    let (second_id, _row_values) = s.a.insert("users", second_values, None).unwrap();
    s.a.immediate_tick();

    let calls = received.lock().unwrap();
    assert_eq!(
        calls.len(),
        2,
        "Second local write should trigger immediate callback"
    );
    let second_delivery = &calls[1];
    assert_eq!(
        second_delivery.len(),
        1,
        "Second callback should contain one added row"
    );
    assert_eq!(second_delivery[0].0, second_id);
}

fn noop_waker() -> std::task::Waker {
    fn noop(_: *const ()) {}
    fn clone(_: *const ()) -> std::task::RawWaker {
        std::task::RawWaker::new(std::ptr::null(), &VTABLE)
    }
    static VTABLE: std::task::RawWakerVTable =
        std::task::RawWakerVTable::new(clone, noop, noop, noop);
    unsafe { std::task::Waker::from_raw(std::task::RawWaker::new(std::ptr::null(), &VTABLE)) }
}

#[test]
fn test_sync_edit_fires_callback_synchronously() {
    let mut core = create_test_runtime();

    let callback_count = Arc::new(Mutex::new(0usize));
    let count_clone = callback_count.clone();

    let query = Query::new("users");
    let _handle = core
        .subscribe(
            query,
            move |delta| {
                if !delta.ordered_delta.added.is_empty() {
                    *count_clone.lock().unwrap() += 1;
                }
            },
            None,
        )
        .unwrap();

    core.immediate_tick();
    let initial_count = *callback_count.lock().unwrap();

    let values = vec![
        Value::Uuid(ObjectId::new()),
        Value::Text("test@test.com".to_string()),
    ];
    let _ = core.insert("users", values, None);
    core.immediate_tick();

    let final_count = *callback_count.lock().unwrap();
    assert!(
        final_count > initial_count,
        "Callback must fire synchronously after insert when index ready"
    );
}

#[test]
fn test_persist_schema_then_add_server_sends_catalogue() {
    // Mirror the WASM flow EXACTLY: NO immediate_tick before persist_schema
    let schema = test_schema();
    let app_id = AppId::from_name("test-app");
    let sync_manager = SyncManager::new();
    let schema_manager = SchemaManager::new(sync_manager, schema, app_id, "dev", "main").unwrap();
    let mut core = RuntimeCore::new(
        schema_manager,
        MemoryStorage::new(),
        NoopScheduler,
        VecSyncSender::new(),
    );
    // NO immediate_tick() here — matches WASM openPersistent flow

    // persist_schema — creates catalogue object in ObjectManager
    let schema_obj_id = core.persist_schema();

    // add_server — should call queue_full_sync_to_server which includes the catalogue
    let server_id = ServerId::new();
    core.add_server(server_id);

    // batched_tick — should flush catalogue to outbox → sync sender
    core.batched_tick();

    // Check that the catalogue was sent
    let messages = core.sync_sender().take();
    let catalogue_msg = messages.iter().find(|m| {
        if let SyncPayload::DocUpdated {
            doc_id, metadata, ..
        } = &m.payload
        {
            *doc_id == schema_obj_id
                && metadata
                    .as_ref()
                    .and_then(|m| m.metadata.get(crate::metadata::MetadataKey::Type.as_str()))
                    .map(|t| t == crate::metadata::ObjectType::CatalogueSchema.as_str())
                    .unwrap_or(false)
        } else {
            false
        }
    });

    assert!(
        catalogue_msg.is_some(),
        "Catalogue schema object should be in outbox after add_server + batched_tick. \
             Messages found: {}",
        messages
            .iter()
            .map(|m| format!("{:?}", m.payload))
            .collect::<Vec<_>>()
            .join(", ")
    );
}

#[test]
fn test_matching_catalogue_hash_skips_catalogue_replay_on_add_server() {
    let schema = test_schema();
    let app_id = AppId::from_name("test-app");
    let sync_manager = SyncManager::new();
    let schema_manager = SchemaManager::new(sync_manager, schema, app_id, "dev", "main").unwrap();
    let mut core = RuntimeCore::new(
        schema_manager,
        MemoryStorage::new(),
        NoopScheduler,
        VecSyncSender::new(),
    );

    let schema_obj_id = core.persist_schema();
    let row_values = vec![
        Value::Uuid(ObjectId::new()),
        Value::Text("Alice".to_string()),
    ];
    let (row_object_id, _) = core.insert("users", row_values, None).unwrap();

    let catalogue_state_hash = core.schema_manager().catalogue_state_hash();

    let server_id = ServerId::new();
    core.add_server_with_catalogue_state_hash(server_id, Some(&catalogue_state_hash));
    core.batched_tick();

    let messages = core.sync_sender().take();
    let catalogue_msg = messages.iter().find(|m| {
        matches!(
            &m.payload,
            SyncPayload::DocUpdated { doc_id, .. } if *doc_id == schema_obj_id
        )
    });
    let row_msg = messages.iter().find(|m| {
        matches!(
            &m.payload,
            SyncPayload::DocUpdated { doc_id, .. } if *doc_id == row_object_id
        )
    });

    assert!(
        catalogue_msg.is_none(),
        "Catalogue replay should be skipped when hashes already match"
    );
    assert!(
        row_msg.is_some(),
        "Regular row objects should still be sent during the full sync walk"
    );
}
// =========================================================================
// Foreign Key — No Write-Time Validation
// =========================================================================
//
// FK write-time existence checks are intentionally removed: in a local-first
// system with query-scoped sync, the referenced row may not be loaded yet,
// causing false violations. True referential integrity will be enforced by
// global transactions (specs/todo/b_launch/globally_consistent_transactions.md).
//
// These tests document that FK-referencing writes succeed even when the
// target row is absent from the local index. They double as scaffolding for
// when global transactions re-introduce server-side FK checks.

/// Schema that mirrors the stress-test app: projects + todos with FK.
fn fk_stress_schema() -> Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("projects")
                .column("name", ColumnType::Text)
                .column("owner_id", ColumnType::Text),
        )
        .table(
            TableSchema::builder("todos")
                .column("title", ColumnType::Text)
                .column("done", ColumnType::Boolean)
                .nullable_column("description", ColumnType::Text)
                .column("owner_id", ColumnType::Text)
                .nullable_fk_column("project", "projects"),
        )
        .build()
}

fn create_fk_runtime() -> TestCore {
    let schema = fk_stress_schema();
    let app_id = AppId::from_name("fk-test");
    let sync_manager = SyncManager::new();
    let schema_manager = SchemaManager::new(sync_manager, schema, app_id, "dev", "main").unwrap();
    let mut core = RuntimeCore::new(
        schema_manager,
        MemoryStorage::new(),
        NoopScheduler,
        VecSyncSender::new(),
    );
    core.immediate_tick();
    core
}

/// After query-scoped sync, a todo's `project` FK can reference a project
/// that was never loaded into MemoryStorage. A partial update (toggling
/// `done`) must succeed — no FK re-check.
///
/// ```text
///   MemoryStorage (after query-scoped sync)
///   ┌────────────────────────────────────────┐
///   │ projects._id index:  []     ← empty!   │
///   │ todos._id index:     [todo_1]           │
///   │                                         │
///   │ todo_1.project = project_42  → not in   │
///   │                               index     │
///   └────────────────────────────────────────┘
///
///   User toggles todo_1.done → partial update → OK (no FK check)
/// ```
#[test]
fn rc_partial_update_with_unloaded_fk_reference() {
    let mut core = create_fk_runtime();

    let (project_id, _) = core
        .insert(
            "projects",
            vec![Value::Text("Acme".into()), Value::Text("alice".into())],
            None,
        )
        .unwrap();

    let (todo_id, _) = core
        .insert(
            "todos",
            vec![
                Value::Text("Buy milk".into()),
                Value::Boolean(true),        // done
                Value::Null,                 // description (nullable)
                Value::Text("alice".into()), // owner_id
                Value::Uuid(project_id),     // project FK
            ],
            None,
        )
        .unwrap();

    core.immediate_tick();

    // Simulate query-scoped sync: remove the project from the _id index.
    let branch = core.schema_manager().branch_name();
    core.storage
        .index_remove(
            "projects",
            "_id",
            branch.as_str(),
            &Value::Uuid(project_id),
            project_id,
        )
        .unwrap();

    // Partial update: only change `done`.
    // No FK validation → succeeds even though project is not in the index.
    core.update(
        todo_id,
        vec![("done".to_string(), Value::Boolean(false))],
        None,
    )
    .expect("partial update must succeed even when referenced project is not loaded");
}

/// Changing a FK column to a non-existent target is allowed at the local
/// write level (no FK existence check). Global transactions will enforce
/// this server-side in the future.
#[test]
fn rc_partial_update_changing_fk_to_missing_target_succeeds() {
    let mut core = create_fk_runtime();

    let (project_id, _) = core
        .insert(
            "projects",
            vec![Value::Text("Acme".into()), Value::Text("alice".into())],
            None,
        )
        .unwrap();

    let (todo_id, _) = core
        .insert(
            "todos",
            vec![
                Value::Text("Buy milk".into()),
                Value::Boolean(true),
                Value::Null,
                Value::Text("alice".into()),
                Value::Uuid(project_id),
            ],
            None,
        )
        .unwrap();

    core.immediate_tick();

    // Change the FK column to a non-existent project.
    // Without global transactions this is accepted locally.
    let bogus_project = ObjectId::new();
    core.update(
        todo_id,
        vec![("project".to_string(), Value::Uuid(bogus_project))],
        None,
    )
    .expect("changing FK to non-existent target must succeed without local FK checks");
}

/// Cold-start test: insert data, flush to storage, create a new RuntimeCore
/// with the same storage, call load_persisted_docs, and verify data is queryable.
///
/// ```text
///   Runtime A (insert + flush)
///   ┌────────────────────┐
///   │  DocManager: alice  │ ──flush──▶ MemoryStorage
///   └────────────────────┘
///
///   Runtime B (cold start)
///   ┌────────────────────┐
///   │  DocManager: empty  │ ──load──▶ reads from same MemoryStorage
///   │  after load: alice  │
///   └────────────────────┘
/// ```
#[test]
fn rc_cold_start_loads_persisted_docs() {
    // Phase 1: create runtime, insert a row, flush to storage
    let schema = test_schema();
    let app_id = AppId::from_name("cold-start-test");
    let sync_manager = SyncManager::new();
    let schema_manager =
        SchemaManager::new(sync_manager, schema.clone(), app_id, "dev", "main").unwrap();
    let mut core = RuntimeCore::new(
        schema_manager,
        MemoryStorage::new(),
        NoopScheduler,
        VecSyncSender::new(),
    );
    core.immediate_tick();

    let row_values = vec![
        Value::Uuid(ObjectId::new()),
        Value::Text("Alice".to_string()),
    ];
    let (row_id, _) = core.insert("users", row_values, None).unwrap();
    core.immediate_tick();

    // Flush docs to the MemoryStorage
    core.flush_storage();

    // Verify the doc was persisted
    let persisted_ids = core.storage().list_doc_ids().unwrap();
    assert!(
        persisted_ids.contains(&row_id),
        "inserted row should be in persisted storage after flush"
    );

    // Phase 2: take the storage, create a fresh runtime, load persisted docs
    let storage = core.into_storage();
    let sync_manager2 = SyncManager::new();
    let schema_manager2 = SchemaManager::new(sync_manager2, schema, app_id, "dev", "main").unwrap();
    let mut core2 = RuntimeCore::new(
        schema_manager2,
        storage,
        NoopScheduler,
        VecSyncSender::new(),
    );
    core2.load_persisted_docs();
    core2.immediate_tick();

    // The doc should be in DocManager
    let doc = core2
        .schema_manager()
        .query_manager()
        .sync_manager()
        .doc_manager
        .get(row_id);
    assert!(
        doc.is_some(),
        "doc should be loaded into DocManager after cold start"
    );

    // Verify via index lookup — process_synced_docs should have indexed the row
    let branch = core2.schema_manager().branch_name();
    let found = core2
        .storage()
        .index_scan_all("users", "_id", branch.as_str());
    assert!(
        found.contains(&row_id),
        "row should be indexed after cold start + immediate_tick. Found: {:?}",
        found
    );
}
