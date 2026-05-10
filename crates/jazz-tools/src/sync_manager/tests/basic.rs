use super::*;

#[test]
fn can_create_sync_manager() {
    let sm = SyncManager::new();
    assert!(sm.servers.is_empty());
    assert!(sm.clients.is_empty());
}

#[test]
fn memory_size_separates_sync_state_buckets() {
    let empty = SyncManager::new().memory_size();
    assert_eq!(empty, (0, 0, 0, 0, 0));

    let mut sm = SyncManager::new();
    let io = MemoryStorage::new();
    let client_id = ClientId::new();
    let server_id = ServerId::new();
    let row_id = ObjectId::new();
    let row = visible_row(row_id, "main", Vec::new(), 1_000, b"alice");
    let row_key = RowBatchKey::from_row(&row);
    let query = QueryBuilder::new("users").branch("main").build();
    let session = crate::query_manager::session::Session::new("alice");

    add_client(&mut sm, &io, client_id);
    add_server(&mut sm, &io, server_id);
    set_client_query_scope(
        &mut sm,
        &io,
        client_id,
        QueryId(7),
        HashSet::from([(row_id, BranchName::new("main"))]),
        Some(session.clone()),
    );

    sm.row_batch_interest
        .insert(row_key, HashSet::from([client_id]));
    sm.query_origin
        .insert(QueryId(7), HashSet::from([client_id]));
    sm.clients
        .get_mut(&client_id)
        .expect("client should exist")
        .sent_batch_ids
        .insert(
            (row_id, BranchName::new("main")),
            HashSet::from([row.batch_id]),
        );
    sm.servers
        .get_mut(&server_id)
        .expect("server should exist")
        .sent_metadata
        .insert(row_id);

    sm.outbox.push(OutboxEntry {
        destination: Destination::Client(client_id),
        payload: SyncPayload::QuerySettled {
            query_id: QueryId(7),
            tier: DurabilityTier::Local,
            scope: vec![],
            through_seq: 1,
        },
    });
    sm.inbox.push(InboxEntry {
        source: Source::Server(server_id),
        payload: SyncPayload::RowBatchNeeded {
            metadata: Some(RowMetadata {
                id: row_id,
                metadata: row_metadata("users"),
            }),
            row: row.clone(),
        },
    });
    sm.pending_query_subscriptions
        .push(PendingQuerySubscription {
            client_id,
            query_id: QueryId(8),
            query: query.clone(),
            session: Some(session.clone()),
            required_tier: None,
            propagation: QueryPropagation::Full,
            policy_context_tables: vec![],
        });
    sm.pending_query_unsubscriptions
        .push(PendingQueryUnsubscription {
            client_id,
            query_id: QueryId(7),
        });
    sm.pending_query_settled.push(PendingQuerySettled {
        server_id: Some(server_id),
        query_id: QueryId(7),
        tier: DurabilityTier::Local,
        through_seq: 2,
    });
    sm.pending_permission_checks.push(PendingPermissionCheck {
        id: PendingUpdateId(1),
        client_id,
        payload: SyncPayload::RowBatchCreated {
            metadata: Some(RowMetadata {
                id: row_id,
                metadata: row_metadata("users"),
            }),
            row: row.clone(),
        },
        session,
        schema_wait_started_at: None,
        metadata: row_metadata("users"),
        old_content: None,
        new_content: Some(b"alice".to_vec()),
        operation: crate::query_manager::policy::Operation::Insert,
    });

    let (catalogue, connections, subscriptions, queues, total) = sm.memory_size();

    assert_eq!(catalogue, 0);
    assert!(connections > 0);
    assert!(subscriptions > 0);
    assert!(queues > 0);
    assert_eq!(total, catalogue + connections + subscriptions + queues);
}

#[test]
fn schema_warning_from_server_relays_to_interested_clients() {
    let mut sm = SyncManager::new();
    let mut io = MemoryStorage::new();
    let client_id = ClientId::new();
    let server_id = ServerId::new();
    let query_id = QueryId(42);

    add_client(&mut sm, &io, client_id);
    sm.take_outbox();
    sm.query_origin
        .entry(query_id)
        .or_default()
        .insert(client_id);

    sm.process_from_server(
        &mut io,
        server_id,
        SyncPayload::SchemaWarning(SchemaWarning {
            query_id,
            table_name: "users".to_string(),
            row_count: 3,
            from_hash: crate::query_manager::types::SchemaHash([0xAA; 32]),
            to_hash: crate::query_manager::types::SchemaHash([0xBB; 32]),
        }),
    );

    let outbox = sm.take_outbox();
    assert_eq!(outbox.len(), 1);
    match &outbox[0] {
        OutboxEntry {
            destination: Destination::Client(id),
            payload: SyncPayload::SchemaWarning(warning),
        } => {
            assert_eq!(*id, client_id);
            assert_eq!(warning.query_id, query_id);
            assert_eq!(warning.table_name, "users");
        }
        other => panic!("expected relayed schema warning, got {other:?}"),
    }
}
