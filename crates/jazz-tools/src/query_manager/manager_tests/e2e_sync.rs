use super::*;

#[test]
fn e2e_client_receives_server_data_via_subscription() {
    use crate::sync_manager::{ClientId, ServerId};
    use uuid::Uuid;

    let schema = test_schema();

    // Setup server with data
    let server_sync = SyncManager::new();
    let (mut server, mut server_io) = create_query_manager(server_sync, schema.clone());

    server
        .insert(
            &mut server_io,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(75)],
        )
        .unwrap();
    server
        .insert(
            &mut server_io,
            "users",
            &[Value::Text("Bob".into()), Value::Integer(30)],
        )
        .unwrap();
    server
        .insert(
            &mut server_io,
            "users",
            &[Value::Text("Charlie".into()), Value::Integer(90)],
        )
        .unwrap();
    server.process(&mut server_io);

    // Setup client (no data yet)
    let client_sync = SyncManager::new();
    let (mut client, mut client_io) = create_query_manager(client_sync, schema.clone());

    // Subscribe to all object updates (needed to receive sync'd data)

    // Connect client to server
    let server_id = ServerId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    let client_id = ClientId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));

    connect_server(&mut client, &client_io, server_id);
    connect_client(&mut server, &server_io, client_id);

    // Clear initial sync messages (we want query-driven sync)
    let _ = client.sync_manager_mut().take_outbox();

    // Client subscribes to users with score > 50 (should match Alice and Charlie)
    let query = client
        .query("users")
        .filter_gt("score", Value::Integer(50))
        .build();

    let sub_id = client.subscribe_with_sync(query, None, None).unwrap();

    // Exchange messages between client and server
    pump_messages(
        &mut client,
        &mut server,
        &mut client_io,
        &mut server_io,
        client_id,
        server_id,
    );

    // Client should now have the matching rows
    let results = client.get_subscription_results(sub_id);

    assert_eq!(results.len(), 2, "Client should receive 2 matching users");

    let names: Vec<_> = results
        .iter()
        .filter_map(|(_, row)| {
            if let Value::Text(name) = &row[0] {
                Some(name.as_str())
            } else {
                None
            }
        })
        .collect();

    assert!(names.contains(&"Alice"), "Should contain Alice");
    assert!(names.contains(&"Charlie"), "Should contain Charlie");
    assert!(!names.contains(&"Bob"), "Should NOT contain Bob");
}

#[test]
fn e2e_client_receives_paginated_server_data_on_cold_offset_subscription() {
    use crate::sync_manager::{ClientId, ServerId};
    use uuid::Uuid;

    let schema = test_schema();

    let server_sync = SyncManager::new();
    let (mut server, mut server_io) = create_query_manager(server_sync, schema.clone());

    for (name, score) in [("A", 1), ("B", 2), ("C", 3), ("D", 4)] {
        server
            .insert(
                &mut server_io,
                "users",
                &[Value::Text(name.into()), Value::Integer(score)],
            )
            .unwrap();
    }
    server.process(&mut server_io);

    let client_sync = SyncManager::new();
    let (mut client, mut client_io) = create_query_manager(client_sync, schema);

    let server_id = ServerId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    let client_id = ClientId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));

    connect_server(&mut client, &client_io, server_id);
    connect_client(&mut server, &server_io, client_id);
    let _ = client.sync_manager_mut().take_outbox();

    let query = client
        .query("users")
        .order_by("score")
        .offset(2)
        .limit(1)
        .build();

    let sub_id = client.subscribe_with_sync(query, None, None).unwrap();

    pump_messages(
        &mut client,
        &mut server,
        &mut client_io,
        &mut server_io,
        client_id,
        server_id,
    );

    let results = client.get_subscription_results(sub_id);
    assert_eq!(
        results.len(),
        1,
        "Cold paginated subscription should materialize the requested page"
    );
    assert_eq!(results[0].1[0], Value::Text("C".into()));
    assert_eq!(results[0].1[1], Value::Integer(3));
}

#[test]
fn e2e_client_receives_paginated_server_data_on_cold_offset_only_subscription() {
    use crate::sync_manager::{ClientId, ServerId};
    use uuid::Uuid;

    let schema = test_schema();

    let server_sync = SyncManager::new();
    let (mut server, mut server_io) = create_query_manager(server_sync, schema.clone());

    for (name, score) in [("A", 1), ("B", 2), ("C", 3), ("D", 4)] {
        server
            .insert(
                &mut server_io,
                "users",
                &[Value::Text(name.into()), Value::Integer(score)],
            )
            .unwrap();
    }
    server.process(&mut server_io);

    let client_sync = SyncManager::new();
    let (mut client, mut client_io) = create_query_manager(client_sync, schema);

    let server_id = ServerId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    let client_id = ClientId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));

    connect_server(&mut client, &client_io, server_id);
    connect_client(&mut server, &server_io, client_id);
    let _ = client.sync_manager_mut().take_outbox();

    let query = client.query("users").order_by("score").offset(2).build();

    let sub_id = client.subscribe_with_sync(query, None, None).unwrap();

    pump_messages(
        &mut client,
        &mut server,
        &mut client_io,
        &mut server_io,
        client_id,
        server_id,
    );

    let results = client.get_subscription_results(sub_id);
    assert_eq!(
        results.len(),
        2,
        "Offset-only query should keep trailing rows"
    );
    assert_eq!(results[0].1[0], Value::Text("C".into()));
    assert_eq!(results[1].1[0], Value::Text("D".into()));
}

#[test]
fn e2e_client_receives_new_matching_row() {
    use crate::sync_manager::{ClientId, ServerId};
    use uuid::Uuid;

    let schema = test_schema();

    // Setup server (initially empty)
    let server_sync = SyncManager::new();
    let (mut server, mut server_io) = create_query_manager(server_sync, schema.clone());

    // Setup client
    let client_sync = SyncManager::new();
    let (mut client, mut client_io) = create_query_manager(client_sync, schema.clone());

    // Connect
    let server_id = ServerId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    let client_id = ClientId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));

    connect_server(&mut client, &client_io, server_id);
    connect_client(&mut server, &server_io, client_id);
    let _ = client.sync_manager_mut().take_outbox();

    // Client subscribes to users with score > 50
    let query = client
        .query("users")
        .filter_gt("score", Value::Integer(50))
        .build();

    let sub_id = client.subscribe_with_sync(query, None, None).unwrap();

    // Initial sync (empty)
    pump_messages(
        &mut client,
        &mut server,
        &mut client_io,
        &mut server_io,
        client_id,
        server_id,
    );
    assert_eq!(
        client.get_subscription_results(sub_id).len(),
        0,
        "Initially empty"
    );

    // Server inserts a matching row
    server
        .insert(
            &mut server_io,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(75)],
        )
        .unwrap();
    server.process(&mut server_io);

    // Exchange messages
    pump_messages(
        &mut client,
        &mut server,
        &mut client_io,
        &mut server_io,
        client_id,
        server_id,
    );

    // Client should now have Alice
    let results = client.get_subscription_results(sub_id);
    assert_eq!(results.len(), 1, "Client should receive new matching user");
    assert_eq!(results[0].1[0], Value::Text("Alice".into()));
}

#[test]
fn e2e_client_does_not_receive_non_matching_row() {
    use crate::sync_manager::{ClientId, ServerId};
    use uuid::Uuid;

    let schema = test_schema();

    // Setup server and client
    let server_sync = SyncManager::new();
    let (mut server, mut server_io) = create_query_manager(server_sync, schema.clone());

    let client_sync = SyncManager::new();
    let (mut client, mut client_io) = create_query_manager(client_sync, schema.clone());

    // Connect
    let server_id = ServerId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    let client_id = ClientId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));

    connect_server(&mut client, &client_io, server_id);
    connect_client(&mut server, &server_io, client_id);
    let _ = client.sync_manager_mut().take_outbox();

    // Client subscribes to users with score > 50
    let query = client
        .query("users")
        .filter_gt("score", Value::Integer(50))
        .build();

    let sub_id = client.subscribe_with_sync(query, None, None).unwrap();
    pump_messages(
        &mut client,
        &mut server,
        &mut client_io,
        &mut server_io,
        client_id,
        server_id,
    );

    // Server inserts a NON-matching row (score = 30)
    server
        .insert(
            &mut server_io,
            "users",
            &[Value::Text("Bob".into()), Value::Integer(30)],
        )
        .unwrap();
    server.process(&mut server_io);

    // Exchange messages
    pump_messages(
        &mut client,
        &mut server,
        &mut client_io,
        &mut server_io,
        client_id,
        server_id,
    );

    // Client should NOT have Bob
    let results = client.get_subscription_results(sub_id);
    assert_eq!(
        results.len(),
        0,
        "Client should NOT receive non-matching user"
    );
}

#[test]
fn local_subscription_does_not_filter_rows_without_remote_scope() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    qm.insert(
        &mut storage,
        "users",
        &[Value::Text("Alice".into()), Value::Integer(100)],
    )
    .unwrap();
    qm.process(&mut storage);

    let sub_id = qm.subscribe(qm.query("users").build()).unwrap();
    qm.process(&mut storage);

    let results = qm.get_subscription_results(sub_id);
    assert_eq!(
        results.len(),
        1,
        "plain local subscriptions should ignore remote scope"
    );
    assert_eq!(
        results[0].1,
        vec![Value::Text("Alice".into()), Value::Integer(100)]
    );
}

#[test]
fn sync_backed_subscription_without_remote_scope_snapshot_keeps_local_rows() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    qm.insert(
        &mut storage,
        "users",
        &[Value::Text("Alice".into()), Value::Integer(100)],
    )
    .unwrap();
    qm.process(&mut storage);

    let sub_id = qm
        .subscribe_with_sync(
            qm.query("users").build(),
            None,
            Some(crate::sync_manager::DurabilityTier::Local),
        )
        .unwrap();
    qm.process(&mut storage);

    let results = qm.get_subscription_results(sub_id);
    assert_eq!(
        results.len(),
        1,
        "sync-backed subscriptions should keep local rows until a remote scope snapshot arrives"
    );
    assert_eq!(
        results[0].1,
        vec![Value::Text("Alice".into()), Value::Integer(100)]
    );
}

#[test]
fn e2e_three_tier_untrusted_downstream_keeps_result_only_scope() {
    use crate::query_manager::policy::Operation;
    use crate::sync_manager::{ClientId, ServerId};
    use uuid::Uuid;

    let mut schema = Schema::new();
    schema.insert(
        TableName::new("folders"),
        TableSchema::with_policies(
            RowDescriptor::new(vec![
                ColumnDescriptor::new("owner_id", ColumnType::Text),
                ColumnDescriptor::new("name", ColumnType::Text),
            ]),
            TablePolicies::new()
                .with_select(PolicyExpr::eq_session("owner_id", vec!["user_id".into()])),
        ),
    );
    schema.insert(
        TableName::new("documents"),
        TableSchema::with_policies(
            RowDescriptor::new(vec![
                ColumnDescriptor::new("owner_id", ColumnType::Text),
                ColumnDescriptor::new("title", ColumnType::Text),
                ColumnDescriptor::new("folder_id", ColumnType::Uuid)
                    .nullable()
                    .references("folders"),
            ]),
            TablePolicies::new().with_select(PolicyExpr::or(vec![
                PolicyExpr::eq_session("owner_id", vec!["user_id".into()]),
                PolicyExpr::inherits(Operation::Select, "folder_id"),
            ])),
        ),
    );

    let (mut core, mut core_io) = create_query_manager(SyncManager::new(), schema.clone());
    let folder_id = core
        .insert(
            &mut core_io,
            "folders",
            &[
                Value::Text("alice".into()),
                Value::Text("Alice folder".into()),
            ],
        )
        .unwrap()
        .row_id;
    core.insert(
        &mut core_io,
        "documents",
        &[
            Value::Text("bob".into()),
            Value::Text("Bob doc in Alice folder".into()),
            Value::Uuid(folder_id),
        ],
    )
    .unwrap();
    core.process(&mut core_io);

    let (mut edge, mut edge_io) = create_query_manager(SyncManager::new(), schema.clone());
    let (mut client, mut client_io) = create_query_manager(SyncManager::new(), schema.clone());

    let edge_server_id_for_client = ServerId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    let client_id_on_edge = ClientId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    let core_server_id_for_edge = ServerId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    let edge_id_on_core = ClientId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));

    client
        .sync_manager_mut()
        .add_server_with_storage(edge_server_id_for_client, false, &client_io);
    connect_client(&mut edge, &edge_io, client_id_on_edge);
    connect_server(&mut edge, &edge_io, core_server_id_for_edge);
    connect_client(&mut core, &core_io, edge_id_on_core);

    let _ = client.sync_manager_mut().take_outbox();
    let _ = edge.sync_manager_mut().take_outbox();
    let _ = core.sync_manager_mut().take_outbox();

    let sub_id = client
        .subscribe_with_sync(
            client.query("documents").build(),
            Some(PolicySession::new("alice")),
            None,
        )
        .unwrap();
    client.process(&mut client_io);

    pump_messages_three_tier(
        &mut client,
        &mut edge,
        &mut core,
        &mut client_io,
        &mut edge_io,
        &mut core_io,
        client_id_on_edge,
        edge_server_id_for_client,
        edge_id_on_core,
        core_server_id_for_edge,
    );

    let results = client.get_subscription_results(sub_id);
    assert_eq!(
        results.len(),
        0,
        "Untrusted downstream should keep current result-only sync behavior"
    );
}
