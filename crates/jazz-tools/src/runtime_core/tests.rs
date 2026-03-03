use super::*;
use crate::query_manager::types::{ColumnType, SchemaBuilder, TableSchema};
use crate::schema_manager::AppId;
use crate::storage::MemoryStorage;
use crate::sync_manager::SyncManager;
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

fn create_test_runtime() -> TestCore {
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
    core.immediate_tick();
    core
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
    let object_id = core.insert("users", values.clone(), None).unwrap();
    assert!(!object_id.0.is_nil());

    core.immediate_tick();
    core.batched_tick();

    let query = Query::new("users");
    let results = execute_query(&mut core, query);
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].0, object_id);
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
    let object_id = core.insert("users", values, None).unwrap();
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

use crate::sync_manager::{
    ClientId, ClientRole, Destination, DurabilityTier, InboxEntry, OutboxEntry, ServerId, Source,
    SyncPayload,
};

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
    let id = s.a.insert("users", values, None).unwrap();
    assert!(!id.0.is_nil());

    let query = Query::new("users");
    let results = execute_query(&mut s.a, query);
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].0, id);
}

#[test]
fn rc_insert_data_syncs_to_server() {
    let mut s = create_3tier_rc();
    let values = vec![Value::Uuid(ObjectId::new()), Value::Text("Alice".into())];
    let id = s.a.insert("users", values, None).unwrap();

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
    let id = s.a.insert("users", values, None).unwrap();
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
    let id = s.a.insert("users", values, None).unwrap();
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
    let (id, mut receiver) =
        s.a.insert_persisted("users", values, None, DurabilityTier::Worker)
            .unwrap();
    assert!(!id.0.is_nil());

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
    let id = s.a.insert("users", values, None).unwrap();
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
    let id = s.a.insert("users", values, None).unwrap();
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
    let id = s.a.insert("users", values, None).unwrap();

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
    let id = s.a.insert("users", values, None).unwrap();

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
    let row_id = s.b.insert("users", values, None).unwrap();
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

    // Force QuerySettled before ObjectUpdated to expose ordering assumptions.
    let mut settled_to_a = Vec::new();
    let mut updates_to_a = Vec::new();
    for entry in b_out {
        if entry.destination != Destination::Client(s.a_client_of_b) {
            continue;
        }
        match entry.payload {
            payload @ SyncPayload::QuerySettled { .. } => settled_to_a.push(payload),
            payload @ SyncPayload::ObjectUpdated { .. } => updates_to_a.push(payload),
            _ => {}
        }
    }

    assert!(
        !settled_to_a.is_empty(),
        "Expected QuerySettled notification for A"
    );
    assert!(
        !updates_to_a.is_empty(),
        "Expected ObjectUpdated payload for A"
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
        Poll::Pending => panic!("Query should resolve after ObjectUpdated and QuerySettled"),
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
                let rows: Vec<(ObjectId, Vec<Value>)> = delta
                    .ordered_delta
                    .added
                    .iter()
                    .filter_map(|row| {
                        decode_row(&delta.descriptor, &row.row.data)
                            .ok()
                            .map(|vals| (row.row.id, vals))
                    })
                    .collect();
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
    let id = s.a.insert("users", values, None).unwrap();
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
                let rows: Vec<(ObjectId, Vec<Value>)> = delta
                    .ordered_delta
                    .added
                    .iter()
                    .filter_map(|row| {
                        decode_row(&delta.descriptor, &row.row.data)
                            .ok()
                            .map(|vals| (row.row.id, vals))
                    })
                    .collect();
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

    let values = vec![
        Value::Uuid(ObjectId::new()),
        Value::Text("local-first".into()),
    ];
    let id = s.a.insert("users", values, None).unwrap();
    s.a.immediate_tick();

    let calls = received.lock().unwrap();
    assert!(
        !calls.is_empty(),
        "Immediate local updates should deliver before EdgeServer settlement"
    );
    let first_delivery = &calls[0];
    assert_eq!(
        first_delivery.len(),
        1,
        "Should include the local row immediately"
    );
    assert_eq!(first_delivery[0].0, id);
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
        if let SyncPayload::ObjectUpdated {
            object_id,
            metadata,
            ..
        } = &m.payload
        {
            *object_id == schema_obj_id
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
