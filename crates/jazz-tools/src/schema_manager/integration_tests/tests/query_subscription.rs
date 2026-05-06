use super::*;

#[test]
fn query_manager_with_schema_context() {
    let v1 = SchemaBuilder::new()
        .table(
            TableSchema::builder("users")
                .column("id", ColumnType::Uuid)
                .column("name", ColumnType::Text),
        )
        .build();

    let v2 = SchemaBuilder::new()
        .table(
            TableSchema::builder("users")
                .column("id", ColumnType::Uuid)
                .column("name", ColumnType::Text)
                .nullable_column("email", ColumnType::Text),
        )
        .build();

    let v1_hash = SchemaHash::compute(&v1);
    let v2_hash = SchemaHash::compute(&v2);
    let lens = generate_lens(&v1, &v2);

    // Create QueryManager with new API
    let sm = SyncManager::new();
    let mut qm = QueryManager::new(sm);
    qm.set_current_schema(v2.clone(), "dev", "main");
    qm.add_live_schema(v1.clone());
    qm.register_lens(lens);

    // Verify schema context is initialized
    assert!(qm.schema_context().is_initialized());

    // Verify all branches are available for queries
    let branches = qm.all_query_branches();
    assert_eq!(branches.len(), 2);

    // Both schema branches should be included
    let v1_branch = format!("dev-{}-main", v1_hash.short());
    let v2_branch = format!("dev-{}-main", v2_hash.short());
    assert!(branches.contains(&v1_branch));
    assert!(branches.contains(&v2_branch));
}

#[test]
fn query_graph_compile_with_schema_context() {
    let v1 = SchemaBuilder::new()
        .table(
            TableSchema::builder("users")
                .column("id", ColumnType::Uuid)
                .column("email", ColumnType::Text),
        )
        .build();

    let v2 = SchemaBuilder::new()
        .table(
            TableSchema::builder("users")
                .column("id", ColumnType::Uuid)
                .column("email_address", ColumnType::Text), // renamed column
        )
        .build();

    let v1_hash = SchemaHash::compute(&v1);
    let v2_hash = SchemaHash::compute(&v2);

    // Create explicit rename lens
    let mut transform = LensTransform::new();
    transform.push(
        LensOp::RenameColumn {
            table: "users".to_string(),
            old_name: "email".to_string(),
            new_name: "email_address".to_string(),
        },
        false,
    );
    let lens = Lens::new(v1_hash, v2_hash, transform);

    let mut ctx = SchemaContext::new(v2.clone(), "dev", "main");
    ctx.add_live_schema(v1.clone(), lens);

    // Build query with filter on the renamed column
    let query = QueryBuilder::new("users").build();

    // Compile with schema context
    let graph = QueryGraph::compile_with_schema_context(&query, &v2, None, &ctx);
    let graph = graph.expect("Query graph compilation should succeed with schema context");

    // Should have index scan nodes for both branches
    // Note: the exact number depends on how many disjuncts and branches
    assert!(!graph.index_scan_nodes.is_empty());
}

#[test]
fn schema_manager_to_query_manager_integration() {
    let v1 = SchemaBuilder::new()
        .table(
            TableSchema::builder("users")
                .column("id", ColumnType::Uuid)
                .column("name", ColumnType::Text),
        )
        .build();

    let v2 = SchemaBuilder::new()
        .table(
            TableSchema::builder("users")
                .column("id", ColumnType::Uuid)
                .column("name", ColumnType::Text)
                .nullable_column("email", ColumnType::Text),
        )
        .build();

    // Create schema manager (which manages SchemaContext internally)
    let mut schema_mgr =
        SchemaManager::new(SyncManager::new(), v2.clone(), test_app_id(), "dev", "main").unwrap();
    schema_mgr.add_live_schema(v1.clone()).unwrap();

    // Verify SchemaManager's QueryManager is properly configured
    let qm = schema_mgr.query_manager();
    assert!(qm.schema_context().is_initialized());
    assert_eq!(qm.all_query_branches().len(), 2);
}

#[test]
fn e2e_two_clients_query_subscriptions_through_server() {
    use crate::query_manager::session::Session;

    // === Define schema with owner-based policy ===
    let schema = {
        use crate::query_manager::policy::PolicyExpr;
        use crate::query_manager::types::TablePolicies;

        let mut schema = Schema::new();

        let docs_descriptor = RowDescriptor::new(vec![
            ColumnDescriptor::new("id", ColumnType::Uuid),
            ColumnDescriptor::new("owner_id", ColumnType::Text),
            ColumnDescriptor::new("title", ColumnType::Text),
        ]);

        // Policy: SELECT allowed if owner_id matches session.user_id
        let docs_policies = TablePolicies::new()
            .with_select(PolicyExpr::eq_session("owner_id", vec!["user_id".into()]));

        schema.insert(
            TableName::new("documents"),
            TableSchema::with_policies(docs_descriptor, docs_policies),
        );

        schema
    };

    // === Setup three nodes with same schema ===
    let mut client_a = SchemaManager::new(
        SyncManager::new(),
        schema.clone(),
        test_app_id(),
        "dev",
        "main",
    )
    .unwrap();

    let mut client_b = SchemaManager::new(
        SyncManager::new(),
        schema.clone(),
        test_app_id(),
        "dev",
        "main",
    )
    .unwrap();

    let mut server = SchemaManager::new(
        SyncManager::new(),
        schema.clone(),
        test_app_id(),
        "dev",
        "main",
    )
    .unwrap();

    let mut io_a = MemoryStorage::new();
    let mut io_b = MemoryStorage::new();
    let mut io_server = MemoryStorage::new();

    // === Network topology ===
    let client_a_id = ClientId::new();
    let client_b_id = ClientId::new();
    let server_id = ServerId::new();

    // Server knows about clients
    server
        .query_manager_mut()
        .sync_manager_mut()
        .add_client_with_storage(&io_server, client_a_id);
    server
        .query_manager_mut()
        .sync_manager_mut()
        .add_client_with_storage(&io_server, client_b_id);

    // Set sessions for permission checking
    server
        .query_manager_mut()
        .sync_manager_mut()
        .set_client_session(client_a_id, Session::new("alice"));
    server
        .query_manager_mut()
        .sync_manager_mut()
        .set_client_session(client_b_id, Session::new("bob"));

    // Clients know about server
    client_a
        .query_manager_mut()
        .sync_manager_mut()
        .add_server_with_storage(server_id, false, &io_a);
    client_b
        .query_manager_mut()
        .sync_manager_mut()
        .add_server_with_storage(server_id, false, &io_b);

    // Clear initial sync
    client_a
        .query_manager_mut()
        .sync_manager_mut()
        .take_outbox();
    client_b
        .query_manager_mut()
        .sync_manager_mut()
        .take_outbox();
    server.query_manager_mut().sync_manager_mut().take_outbox();

    // === Create documents on server through public insert API ===
    let alice_doc_id = server
        .insert(
            &mut io_server,
            "documents",
            HashMap::from([
                ("id".to_string(), Value::Uuid(ObjectId::new())),
                ("owner_id".to_string(), Value::Text("alice".into())),
                (
                    "title".to_string(),
                    Value::Text("Alice's Secret Doc".into()),
                ),
            ]),
        )
        .unwrap()
        .row_id;
    let bob_doc_id = server
        .insert(
            &mut io_server,
            "documents",
            HashMap::from([
                ("id".to_string(), Value::Uuid(ObjectId::new())),
                ("owner_id".to_string(), Value::Text("bob".into())),
                ("title".to_string(), Value::Text("Bob's Private Doc".into())),
            ]),
        )
        .unwrap()
        .row_id;

    // Clear any sync messages from document creation
    server.query_manager_mut().sync_manager_mut().take_outbox();

    // === Client A subscribes to documents (as alice) ===
    let query_a = QueryBuilder::new("documents")
        .branch(client_a.branch_name().to_string())
        .build();

    let _sub_a = client_a
        .query_manager_mut()
        .subscribe_with_sync(query_a.clone(), Some(Session::new("alice")), None)
        .unwrap();

    client_a.process(&mut io_a);

    // Get the QuerySubscription message from client A
    let outbox_a = client_a
        .query_manager_mut()
        .sync_manager_mut()
        .take_outbox();
    let query_sub_msg = outbox_a
        .iter()
        .find(|e| matches!(e.payload, SyncPayload::QuerySubscription { .. }))
        .expect("Client A should send QuerySubscription");

    // Forward to server
    server
        .query_manager_mut()
        .sync_manager_mut()
        .push_inbox(InboxEntry {
            source: Source::Client(client_a_id),
            payload: query_sub_msg.payload.clone(),
        });

    // Server processes the subscription
    server.process(&mut io_server);

    // === Server should send Alice's doc to Client A ===
    let server_outbox = server.query_manager_mut().sync_manager_mut().take_outbox();

    // Find row-batch messages destined for client A
    let alice_updates: Vec<_> = server_outbox
        .iter()
        .filter(|e| {
            matches!(e.destination, Destination::Client(cid) if cid == client_a_id)
                && matches!(e.payload, SyncPayload::RowBatchNeeded { .. })
        })
        .collect();

    // Alice should receive her own document (alice_doc_id)
    // Alice should NOT receive Bob's document due to policy
    let received_ids: Vec<ObjectId> = alice_updates
        .iter()
        .filter_map(|e| {
            if let SyncPayload::RowBatchNeeded { row, .. } = &e.payload {
                Some(row.row_id)
            } else {
                None
            }
        })
        .collect();

    assert!(
        received_ids.contains(&alice_doc_id),
        "Alice should receive her own document"
    );
    assert!(
        !received_ids.contains(&bob_doc_id),
        "Alice should NOT receive Bob's document (policy check)"
    );

    // === Client B subscribes to documents (as bob) ===
    let query_b = QueryBuilder::new("documents")
        .branch(client_b.branch_name().to_string())
        .build();

    let _sub_b = client_b
        .query_manager_mut()
        .subscribe_with_sync(query_b.clone(), Some(Session::new("bob")), None)
        .unwrap();

    client_b.process(&mut io_b);

    // Forward subscription to server
    let outbox_b = client_b
        .query_manager_mut()
        .sync_manager_mut()
        .take_outbox();
    let query_sub_b = outbox_b
        .iter()
        .find(|e| matches!(e.payload, SyncPayload::QuerySubscription { .. }))
        .expect("Client B should send QuerySubscription");

    server
        .query_manager_mut()
        .sync_manager_mut()
        .push_inbox(InboxEntry {
            source: Source::Client(client_b_id),
            payload: query_sub_b.payload.clone(),
        });

    server.process(&mut io_server);

    // === Server should send Bob's doc to Client B ===
    let server_outbox_b = server.query_manager_mut().sync_manager_mut().take_outbox();

    let bob_updates: Vec<_> = server_outbox_b
        .iter()
        .filter(|e| {
            matches!(e.destination, Destination::Client(cid) if cid == client_b_id)
                && matches!(e.payload, SyncPayload::RowBatchNeeded { .. })
        })
        .collect();

    let bob_received_ids: Vec<ObjectId> = bob_updates
        .iter()
        .filter_map(|e| {
            if let SyncPayload::RowBatchNeeded { row, .. } = &e.payload {
                Some(row.row_id)
            } else {
                None
            }
        })
        .collect();

    assert!(
        bob_received_ids.contains(&bob_doc_id),
        "Bob should receive his own document"
    );
    assert!(
        !bob_received_ids.contains(&alice_doc_id),
        "Bob should NOT receive Alice's document (policy check)"
    );
}

#[test]
fn query_settled_no_tier_immediate() {
    let schema = SchemaBuilder::new()
        .table(
            TableSchema::builder("items")
                .column("id", ColumnType::Uuid)
                .column("name", ColumnType::Text),
        )
        .build();

    let mut manager = SchemaManager::new(
        SyncManager::new(),
        schema.clone(),
        test_app_id(),
        "dev",
        "main",
    )
    .unwrap();
    let mut storage = MemoryStorage::new();

    // Insert a row
    let row_id = ObjectId::new();
    let values = HashMap::from([
        ("id".to_string(), Value::Uuid(row_id)),
        ("name".to_string(), Value::Text("hello".into())),
    ]);
    manager.insert(&mut storage, "items", values).unwrap();
    manager.process(&mut storage);

    // Subscribe with settled_tier=None
    let query = QueryBuilder::new("items").build();
    let sub_id = manager
        .query_manager_mut()
        .subscribe_with_session(query, None, None)
        .unwrap();
    manager.process(&mut storage);

    // Should get immediate callback on first process
    let updates = manager.query_manager_mut().take_updates();
    assert!(
        !updates.is_empty(),
        "settled_tier=None should deliver immediately"
    );
    let matching: Vec<_> = updates
        .iter()
        .filter(|u| u.subscription_id == sub_id)
        .collect();
    assert_eq!(matching.len(), 1);
    assert_eq!(matching[0].delta.added.len(), 1);
}

#[test]
fn query_settled_direct() {
    let schema = SchemaBuilder::new()
        .table(
            TableSchema::builder("items")
                .column("id", ColumnType::Uuid)
                .column("name", ColumnType::Text),
        )
        .build();

    // === Setup Client A ===
    let mut client_a = SchemaManager::new(
        SyncManager::new(),
        schema.clone(),
        test_app_id(),
        "dev",
        "main",
    )
    .unwrap();
    let mut io_a = MemoryStorage::new();

    // === Setup Server B (Local tier) ===
    let mut server_b = SchemaManager::new(
        SyncManager::new().with_durability_tier(DurabilityTier::Local),
        schema.clone(),
        test_app_id(),
        "dev",
        "main",
    )
    .unwrap();
    let mut io_b = MemoryStorage::new();

    // === Network topology ===
    let client_a_id = ClientId::new();
    let server_b_id = ServerId::new();

    server_b
        .query_manager_mut()
        .sync_manager_mut()
        .add_client_with_storage(&io_b, client_a_id);
    client_a
        .query_manager_mut()
        .sync_manager_mut()
        .add_server_with_storage(server_b_id, false, &io_a);

    // Insert a row on server B
    let row_id = ObjectId::new();
    let values = HashMap::from([
        ("id".to_string(), Value::Uuid(row_id)),
        ("name".to_string(), Value::Text("srv".into())),
    ]);
    server_b.insert(&mut io_b, "items", values).unwrap();
    server_b.process(&mut io_b);

    // === Client A subscribes with settled_tier=Local ===
    let query = QueryBuilder::new("items")
        .branch(client_a.branch_name().to_string())
        .build();
    let sub_id = client_a
        .query_manager_mut()
        .subscribe_with_sync(query, None, Some(DurabilityTier::Local))
        .unwrap();
    client_a.process(&mut io_a);

    // First process: no data yet (no QuerySettled received)
    let updates = client_a.query_manager_mut().take_updates();
    let matching: Vec<_> = updates
        .iter()
        .filter(|u| u.subscription_id == sub_id)
        .collect();
    assert!(
        matching.is_empty() || matching.iter().all(|u| u.delta.added.is_empty()),
        "Should not deliver before QuerySettled"
    );

    // === Forward QuerySubscription from A to B ===
    let outbox_a = client_a
        .query_manager_mut()
        .sync_manager_mut()
        .take_outbox();
    let query_sub = outbox_a
        .iter()
        .find(|e| matches!(e.payload, SyncPayload::QuerySubscription { .. }))
        .expect("Should have QuerySubscription");

    server_b
        .query_manager_mut()
        .sync_manager_mut()
        .push_inbox(InboxEntry {
            source: Source::Client(client_a_id),
            payload: query_sub.payload.clone(),
        });

    // Server B processes (settles server sub → emits QuerySettled)
    server_b.process(&mut io_b);

    let outbox_b = server_b
        .query_manager_mut()
        .sync_manager_mut()
        .take_outbox();

    // Expect QuerySettled(Local) in outbox
    let settled_msg = outbox_b
        .iter()
        .find(|e| matches!(e.payload, SyncPayload::QuerySettled { .. }));
    assert!(
        settled_msg.is_some(),
        "Server B should emit QuerySettled(Local)"
    );

    // Forward all from B to A (including data + QuerySettled)
    for entry in &outbox_b {
        if matches!(entry.destination, Destination::Client(cid) if cid == client_a_id) {
            client_a
                .query_manager_mut()
                .sync_manager_mut()
                .push_inbox(InboxEntry {
                    source: Source::Server(server_b_id),
                    payload: entry.payload.clone(),
                });
        }
    }

    // Process client A with QuerySettled now available
    client_a.process(&mut io_a);
    // Second process to settle after data arrives
    client_a.process(&mut io_a);

    let updates = client_a.query_manager_mut().take_updates();
    let matching: Vec<_> = updates
        .iter()
        .filter(|u| u.subscription_id == sub_id)
        .collect();
    assert!(
        !matching.is_empty(),
        "Should deliver after QuerySettled(Local) received"
    );
    let total_added: usize = matching.iter().map(|u| u.delta.added.len()).sum();
    assert!(total_added >= 1, "Should have at least 1 row delivered");
}

#[test]
fn query_settled_holds_until_tier() {
    let schema = SchemaBuilder::new()
        .table(
            TableSchema::builder("items")
                .column("id", ColumnType::Uuid)
                .column("name", ColumnType::Text),
        )
        .build();

    // Client A — no tier (end client)
    let mut client_a = SchemaManager::new(
        SyncManager::new(),
        schema.clone(),
        test_app_id(),
        "dev",
        "main",
    )
    .unwrap();
    let mut io_a = MemoryStorage::new();

    // Subscribe with settled_tier=EdgeServer
    let query = QueryBuilder::new("items")
        .branch(client_a.branch_name().to_string())
        .build();
    let sub_id = client_a
        .query_manager_mut()
        .subscribe_with_sync_with_local_updates(
            query,
            None,
            Some(DurabilityTier::EdgeServer),
            LocalUpdates::Deferred,
        )
        .unwrap();
    client_a.process(&mut io_a);

    // Insert a row locally on A (so there's data to deliver)
    let row_id = ObjectId::new();
    let values = HashMap::from([
        ("id".to_string(), Value::Uuid(row_id)),
        ("name".to_string(), Value::Text("local".into())),
    ]);
    client_a.insert(&mut io_a, "items", values).unwrap();
    client_a.process(&mut io_a);

    // No delivery yet — tier not satisfied
    let updates = client_a.query_manager_mut().take_updates();
    let matching: Vec<_> = updates
        .iter()
        .filter(|u| u.subscription_id == sub_id)
        .collect();
    assert!(
        matching.is_empty() || matching.iter().all(|u| u.delta.is_empty()),
        "Should not deliver — EdgeServer tier not achieved"
    );

    let visible_row = io_a
        .scan_visible_region("items", client_a.branch_name().as_str())
        .unwrap()
        .into_iter()
        .next()
        .expect("one visible row");
    let server_b_id = ServerId::new();

    // Simulate per-row Local settlement — still insufficient for EdgeServer reads.
    client_a
        .query_manager_mut()
        .sync_manager_mut()
        .push_inbox(InboxEntry {
            source: Source::Server(server_b_id),
            payload: SyncPayload::BatchFate {
                fate: crate::batch_fate::BatchFate::DurableDirect {
                    batch_id: visible_row.batch_id,
                    confirmed_tier: DurabilityTier::Local,
                },
            },
        });
    client_a.process(&mut io_a);

    let updates = client_a.query_manager_mut().take_updates();
    let matching: Vec<_> = updates
        .iter()
        .filter(|u| u.subscription_id == sub_id)
        .collect();
    assert!(
        matching.is_empty() || matching.iter().all(|u| u.delta.is_empty()),
        "Local < EdgeServer — should still not deliver"
    );

    // Simulate per-row EdgeServer settlement — now the row may become visible.
    client_a
        .query_manager_mut()
        .sync_manager_mut()
        .push_inbox(InboxEntry {
            source: Source::Server(server_b_id),
            payload: SyncPayload::BatchFate {
                fate: crate::batch_fate::BatchFate::DurableDirect {
                    batch_id: visible_row.batch_id,
                    confirmed_tier: DurabilityTier::EdgeServer,
                },
            },
        });
    client_a.process(&mut io_a);

    let updates = client_a.query_manager_mut().take_updates();
    let matching: Vec<_> = updates
        .iter()
        .filter(|u| u.subscription_id == sub_id)
        .collect();
    assert!(
        !matching.is_empty(),
        "EdgeServer settlement should deliver now"
    );
    let total_added: usize = matching.iter().map(|u| u.delta.added.len()).sum();
    assert_eq!(total_added, 1, "Should deliver the accumulated row");
}

#[test]
fn query_settled_relays_edge_tier_through_worker() {
    use crate::query_manager::manager::LocalUpdates;
    use crate::sync_manager::{
        ClientId, ClientRole, Destination, InboxEntry, QueryId, ServerId, Source, SyncPayload,
    };

    let schema = SchemaBuilder::new()
        .table(
            TableSchema::builder("items")
                .column("id", ColumnType::Uuid)
                .column("name", ColumnType::Text),
        )
        .build();

    // A = end client, B = local tier mid-tier, C = edge tier upstream.
    let mut client_a = SchemaManager::new(
        SyncManager::new(),
        schema.clone(),
        test_app_id(),
        "dev",
        "main",
    )
    .unwrap();
    let mut io_a = MemoryStorage::new();

    let mut worker_b = SchemaManager::new(
        SyncManager::new().with_durability_tier(DurabilityTier::Local),
        schema.clone(),
        test_app_id(),
        "dev",
        "main",
    )
    .unwrap();
    let mut io_b = MemoryStorage::new();

    let mut edge_c = SchemaManager::new(
        SyncManager::new().with_durability_tier(DurabilityTier::EdgeServer),
        schema.clone(),
        test_app_id(),
        "dev",
        "main",
    )
    .unwrap();
    let mut io_c = MemoryStorage::new();

    let a_id_on_b = ClientId::new();
    let b_server_id_for_a = ServerId::new();
    let b_id_on_c = ClientId::new();
    let c_server_id_for_b = ServerId::new();

    worker_b
        .query_manager_mut()
        .sync_manager_mut()
        .add_client_with_storage(&io_b, a_id_on_b);
    worker_b
        .query_manager_mut()
        .sync_manager_mut()
        .set_client_role(a_id_on_b, ClientRole::Peer);
    client_a
        .query_manager_mut()
        .sync_manager_mut()
        .add_server_with_storage(b_server_id_for_a, false, &io_a);

    edge_c
        .query_manager_mut()
        .sync_manager_mut()
        .add_client_with_storage(&io_c, b_id_on_c);
    edge_c
        .query_manager_mut()
        .sync_manager_mut()
        .set_client_role(b_id_on_c, ClientRole::Peer);
    worker_b
        .query_manager_mut()
        .sync_manager_mut()
        .add_server_with_storage(c_server_id_for_b, false, &io_b);

    let row_id = ObjectId::new();
    let values = HashMap::from([
        ("id".to_string(), Value::Uuid(row_id)),
        ("name".to_string(), Value::Text("edge".into())),
    ]);
    edge_c.insert(&mut io_c, "items", values).unwrap();
    edge_c.process(&mut io_c);

    // Ignore bootstrap traffic from initial connect.
    let _ = client_a
        .query_manager_mut()
        .sync_manager_mut()
        .take_outbox();
    let _ = worker_b
        .query_manager_mut()
        .sync_manager_mut()
        .take_outbox();
    let _ = edge_c.query_manager_mut().sync_manager_mut().take_outbox();

    let query = QueryBuilder::new("items")
        .branch(client_a.branch_name().to_string())
        .build();
    let sub_id = client_a
        .query_manager_mut()
        .subscribe_with_sync_with_local_updates(
            query,
            None,
            Some(DurabilityTier::EdgeServer),
            LocalUpdates::Deferred,
        )
        .unwrap();
    client_a.process(&mut io_a);

    let outbox_a = client_a
        .query_manager_mut()
        .sync_manager_mut()
        .take_outbox();
    for entry in &outbox_a {
        if matches!(entry.destination, Destination::Server(id) if id == b_server_id_for_a) {
            worker_b
                .query_manager_mut()
                .sync_manager_mut()
                .push_inbox(InboxEntry {
                    source: Source::Client(a_id_on_b),
                    payload: entry.payload.clone(),
                });
        }
    }
    worker_b.process(&mut io_b);

    let outbox_b = worker_b
        .query_manager_mut()
        .sync_manager_mut()
        .take_outbox();
    for entry in &outbox_b {
        match entry.destination {
            Destination::Client(cid) if cid == a_id_on_b => {
                client_a
                    .query_manager_mut()
                    .sync_manager_mut()
                    .push_inbox(InboxEntry {
                        source: Source::Server(b_server_id_for_a),
                        payload: entry.payload.clone(),
                    });
            }
            Destination::Server(id) if id == c_server_id_for_b => {
                edge_c
                    .query_manager_mut()
                    .sync_manager_mut()
                    .push_inbox(InboxEntry {
                        source: Source::Client(b_id_on_c),
                        payload: entry.payload.clone(),
                    });
            }
            _ => {}
        }
    }
    client_a.process(&mut io_a);
    edge_c.process(&mut io_c);

    let outbox_c = edge_c.query_manager_mut().sync_manager_mut().take_outbox();
    for entry in &outbox_c {
        if matches!(entry.destination, Destination::Client(cid) if cid == b_id_on_c) {
            worker_b
                .query_manager_mut()
                .sync_manager_mut()
                .push_inbox(InboxEntry {
                    source: Source::Server(c_server_id_for_b),
                    payload: entry.payload.clone(),
                });
        }
    }
    worker_b.process(&mut io_b);

    let relayed_from_b = worker_b
        .query_manager_mut()
        .sync_manager_mut()
        .take_outbox();
    let edge_settled = relayed_from_b.iter().find(|entry| {
        matches!(
            (&entry.destination, &entry.payload),
            (
                Destination::Client(cid),
                SyncPayload::QuerySettled {
                    query_id,
                    tier: DurabilityTier::EdgeServer,
                    ..
                }
            ) if *cid == a_id_on_b && *query_id == QueryId(sub_id.0)
        )
    });
    assert!(
        edge_settled.is_some(),
        "Local tier should relay QuerySettled(EdgeServer) from upstream to client"
    );

    for entry in &relayed_from_b {
        if matches!(entry.destination, Destination::Client(cid) if cid == a_id_on_b) {
            client_a
                .query_manager_mut()
                .sync_manager_mut()
                .push_inbox(InboxEntry {
                    source: Source::Server(b_server_id_for_a),
                    payload: entry.payload.clone(),
                });
        }
    }
    client_a.process(&mut io_a);
    client_a.process(&mut io_a);

    let updates = client_a.query_manager_mut().take_updates();
    let matching: Vec<_> = updates
        .iter()
        .filter(|u| u.subscription_id == sub_id)
        .collect();
    assert!(
        !matching.is_empty(),
        "Client should deliver after relayed QuerySettled(EdgeServer)"
    );
    let total_added: usize = matching.iter().map(|u| u.delta.added.len()).sum();
    assert_eq!(
        total_added, 1,
        "Expected the upstream edge row to be delivered"
    );
}

#[test]
fn query_settled_data_accumulates() {
    let schema = SchemaBuilder::new()
        .table(
            TableSchema::builder("items")
                .column("id", ColumnType::Uuid)
                .column("name", ColumnType::Text),
        )
        .build();

    let mut client = SchemaManager::new(
        SyncManager::new(),
        schema.clone(),
        test_app_id(),
        "dev",
        "main",
    )
    .unwrap();
    let mut storage = MemoryStorage::new();

    // Subscribe with settled_tier=Local
    let query = QueryBuilder::new("items")
        .branch(client.branch_name().to_string())
        .build();
    let sub_id = client
        .query_manager_mut()
        .subscribe_with_sync_with_local_updates(
            query,
            None,
            Some(DurabilityTier::Local),
            LocalUpdates::Deferred,
        )
        .unwrap();
    client.process(&mut storage);

    // Insert 3 rows before tier is satisfied
    for i in 0..3 {
        let row_id = ObjectId::new();
        let values = HashMap::from([
            ("id".to_string(), Value::Uuid(row_id)),
            ("name".to_string(), Value::Text(format!("item_{}", i))),
        ]);
        client.insert(&mut storage, "items", values).unwrap();
        client.process(&mut storage);
    }

    // No delivery yet
    let updates = client.query_manager_mut().take_updates();
    let matching: Vec<_> = updates
        .iter()
        .filter(|u| u.subscription_id == sub_id)
        .collect();
    assert!(
        matching.is_empty() || matching.iter().all(|u| u.delta.is_empty()),
        "Should not deliver before tier is satisfied"
    );

    let server_id = ServerId::new();
    for row in storage
        .scan_visible_region("items", client.branch_name().as_str())
        .unwrap()
    {
        client
            .query_manager_mut()
            .sync_manager_mut()
            .push_inbox(InboxEntry {
                source: Source::Server(server_id),
                payload: SyncPayload::BatchFate {
                    fate: crate::batch_fate::BatchFate::DurableDirect {
                        batch_id: row.batch_id,
                        confirmed_tier: DurabilityTier::Local,
                    },
                },
            });
    }
    client.process(&mut storage);

    let updates = client.query_manager_mut().take_updates();
    let matching: Vec<_> = updates
        .iter()
        .filter(|u| u.subscription_id == sub_id)
        .collect();
    assert!(
        !matching.is_empty(),
        "Should deliver after rows reach Worker"
    );
    let total_added: usize = matching.iter().map(|u| u.delta.added.len()).sum();
    assert_eq!(
        total_added, 3,
        "First delivery should contain all 3 accumulated rows"
    );
}

#[test]
fn query_one_shot_settled_tier() {
    let schema = SchemaBuilder::new()
        .table(
            TableSchema::builder("items")
                .column("id", ColumnType::Uuid)
                .column("name", ColumnType::Text),
        )
        .build();

    let mut client = SchemaManager::new(
        SyncManager::new(),
        schema.clone(),
        test_app_id(),
        "dev",
        "main",
    )
    .unwrap();
    let mut storage = MemoryStorage::new();

    // Insert a row first
    let row_id = ObjectId::new();
    let values = HashMap::from([
        ("id".to_string(), Value::Uuid(row_id)),
        ("name".to_string(), Value::Text("one-shot".into())),
    ]);
    client.insert(&mut storage, "items", values).unwrap();
    client.process(&mut storage);

    // Subscribe with settled_tier=Local (simulating one-shot behavior)
    let query = QueryBuilder::new("items")
        .branch(client.branch_name().to_string())
        .build();
    let sub_id = client
        .query_manager_mut()
        .subscribe_with_sync(query, None, Some(DurabilityTier::Local))
        .unwrap();
    client.process(&mut storage);

    // First process: local pending row is already visible because one-shot
    // queries use immediate local updates by default.
    let updates = client.query_manager_mut().take_updates();
    let matching: Vec<_> = updates
        .iter()
        .filter(|u| u.subscription_id == sub_id)
        .collect();
    assert!(
        !matching.is_empty(),
        "One-shot should resolve on first local settle"
    );
    let total_added: usize = matching.iter().map(|u| u.delta.added.len()).sum();
    assert_eq!(total_added, 1, "Should contain the one local row");

    let server_id = ServerId::new();
    let visible_row = storage
        .scan_visible_region("items", client.branch_name().as_str())
        .unwrap()
        .into_iter()
        .next()
        .expect("one visible row");
    client
        .query_manager_mut()
        .sync_manager_mut()
        .push_inbox(InboxEntry {
            source: Source::Server(server_id),
            payload: SyncPayload::BatchFate {
                fate: crate::batch_fate::BatchFate::DurableDirect {
                    batch_id: visible_row.batch_id,
                    confirmed_tier: DurabilityTier::Local,
                },
            },
        });
    client.process(&mut storage);

    // Local durability arriving later should not emit another visible
    // delta because the row is already present.
    let updates = client.query_manager_mut().take_updates();
    let matching: Vec<_> = updates
        .iter()
        .filter(|u| u.subscription_id == sub_id)
        .collect();
    assert!(
        matching.is_empty() || matching.iter().all(|u| u.delta.is_empty()),
        "Local promotion should not emit a second visible delta"
    );
}

#[test]
fn query_one_shot_settled_tier_empty_results() {
    let schema = SchemaBuilder::new()
        .table(
            TableSchema::builder("items")
                .column("id", ColumnType::Uuid)
                .column("name", ColumnType::Text),
        )
        .build();

    let mut client = SchemaManager::new(
        SyncManager::new(),
        schema.clone(),
        test_app_id(),
        "dev",
        "main",
    )
    .unwrap();
    let mut storage = MemoryStorage::new();

    // No rows inserted. Subscribe with settled_tier=Local.
    let query = QueryBuilder::new("items")
        .branch(client.branch_name().to_string())
        .build();
    let sub_id = client
        .query_manager_mut()
        .subscribe_with_sync(query, None, Some(DurabilityTier::Local))
        .unwrap();
    client.process(&mut storage);

    // With no upstream server and no rows, the empty snapshot resolves immediately.
    let updates = client.query_manager_mut().take_updates();
    let matching: Vec<_> = updates
        .iter()
        .filter(|u| u.subscription_id == sub_id)
        .collect();
    assert!(
        !matching.is_empty(),
        "Should deliver empty snapshot immediately for a local empty result"
    );
    assert!(
        matching.iter().all(|u| u.delta.is_empty()),
        "Expected empty delta for empty snapshot"
    );
}
