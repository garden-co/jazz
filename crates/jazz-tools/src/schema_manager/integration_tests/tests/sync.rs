use super::*;

#[test]
fn e2e_two_clients_server_schema_sync() {
    // === Define the schema (both clients use this) ===
    let schema = SchemaBuilder::new()
        .table(
            TableSchema::builder("documents")
                .column("id", ColumnType::Uuid)
                .column("owner_id", ColumnType::Text)
                .column("title", ColumnType::Text),
        )
        .build();

    let schema_hash = SchemaHash::compute(&schema);

    // === Setup Client A (alice) ===
    let mut client_a = SchemaManager::new(
        SyncManager::new(),
        schema.clone(),
        test_app_id(),
        "dev",
        "main",
    )
    .unwrap();

    // === Setup Client B (bob) ===
    let mut client_b = SchemaManager::new(
        SyncManager::new(),
        schema.clone(),
        test_app_id(),
        "dev",
        "main",
    )
    .unwrap();

    // === Setup Server with NO schema (true server mode) ===
    // Server starts with no schema knowledge - will learn via catalogue sync
    let mut server = SchemaManager::new_server(SyncManager::new(), test_app_id(), "dev");

    // === Client A persists schema to catalogue BEFORE connecting to server ===
    // This way, when the server is added, the catalogue object will sync
    let mut io_a = MemoryStorage::new();
    let mut io_b = MemoryStorage::new();
    let mut io_server = MemoryStorage::new();
    let schema_obj_id = client_a.persist_schema(&mut io_a);
    assert_eq!(schema_obj_id, schema_hash.to_object_id());

    // === Network topology setup ===
    // Client A <-> Server <-> Client B
    let client_a_id = ClientId::new();
    let client_b_id = ClientId::new();
    let server_id = ServerId::new();

    // Server knows about both clients — Admin role for catalogue writes
    server
        .query_manager_mut()
        .sync_manager_mut()
        .add_client_with_storage(&io_server, client_a_id);
    server
        .query_manager_mut()
        .sync_manager_mut()
        .set_client_role(client_a_id, ClientRole::Admin);
    server
        .query_manager_mut()
        .sync_manager_mut()
        .add_client_with_storage(&io_server, client_b_id);
    server
        .query_manager_mut()
        .sync_manager_mut()
        .set_client_role(client_b_id, ClientRole::Admin);

    // Clients know about the server - this triggers initial sync including catalogue objects
    client_a
        .query_manager_mut()
        .sync_manager_mut()
        .add_server_with_storage(server_id, false, &io_a);
    client_b
        .query_manager_mut()
        .sync_manager_mut()
        .add_server_with_storage(server_id, false, &io_b);

    // Process to generate outbox messages
    client_a.process(&mut io_a);

    // === Transfer catalogue object from Client A to Server ===
    let outbox_a = client_a
        .query_manager_mut()
        .sync_manager_mut()
        .take_outbox();
    assert!(
        !outbox_a.is_empty(),
        "Client A should have catalogue object to sync"
    );

    // Find the schema catalogue object
    let schema_msg = outbox_a
        .iter()
        .find(|e| {
            if let SyncPayload::CatalogueEntryUpdated { entry } = &e.payload {
                entry.object_id == schema_obj_id
            } else {
                false
            }
        })
        .expect("Should have schema object in outbox");

    // Push to server inbox
    server
        .query_manager_mut()
        .sync_manager_mut()
        .push_inbox(InboxEntry {
            source: Source::Client(client_a_id),
            payload: schema_msg.payload.clone(),
        });

    // Server processes the inbox (Admin role allows catalogue writes directly)
    server.process(&mut io_server);

    // === Server should now have the schema in known_schemas ===
    assert!(
        server.is_schema_known(&schema_hash),
        "Server should know about the schema after catalogue sync"
    );

    // === Now test that row data syncs and is indexed via lazy activation ===
    // Client A creates a document
    let doc_uuid = ObjectId::new();
    let doc_values = HashMap::from([
        ("id".to_string(), Value::Uuid(doc_uuid)),
        ("owner_id".to_string(), Value::Text("alice".into())),
        ("title".to_string(), Value::Text("Test Document".into())),
    ]);
    let handle = client_a.insert(&mut io_a, "documents", doc_values).unwrap();
    let doc_id = handle.row_id; // The actual row object ID
    client_a.process(&mut io_a);

    // Get client A's outbox - should have the row object
    let outbox_a = client_a
        .query_manager_mut()
        .sync_manager_mut()
        .take_outbox();
    assert!(!outbox_a.is_empty(), "Client A should have row to sync");

    // Find the row object message
    let row_msg = outbox_a
        .iter()
        .find(|e| {
            if let SyncPayload::RowBatchCreated { row, .. } = &e.payload {
                row.row_id == doc_id
            } else {
                false
            }
        })
        .expect("Should have RowBatchCreated for the row in outbox");

    // Push to server inbox
    server
        .query_manager_mut()
        .sync_manager_mut()
        .push_inbox(InboxEntry {
            source: Source::Client(client_a_id),
            payload: row_msg.payload.clone(),
        });

    // Server processes
    server.process(&mut io_server);

    // Re-process for lazy activation (Admin clients bypass pending)
    server.process(&mut io_server);

    // === Verify query-visible behavior via server response to client subscription ===
    client_b
        .query_manager_mut()
        .sync_manager_mut()
        .take_outbox();
    let query_b = QueryBuilder::new("documents")
        .branch(client_b.branch_name().to_string())
        .build();
    let _sub_b = client_b
        .query_manager_mut()
        .subscribe_with_sync(query_b, None, None)
        .unwrap();
    client_b.process(&mut io_b);

    let client_b_outbox = client_b
        .query_manager_mut()
        .sync_manager_mut()
        .take_outbox();
    let query_sub_msg = client_b_outbox
        .iter()
        .find(|e| matches!(e.payload, SyncPayload::QuerySubscription { .. }))
        .expect("Client B should emit QuerySubscription");

    server
        .query_manager_mut()
        .sync_manager_mut()
        .push_inbox(InboxEntry {
            source: Source::Client(client_b_id),
            payload: query_sub_msg.payload.clone(),
        });
    server.process(&mut io_server);

    let server_outbox = server.query_manager_mut().sync_manager_mut().take_outbox();
    let doc_update_for_b = server_outbox.iter().find(|e| {
        matches!(e.destination, Destination::Client(cid) if cid == client_b_id)
            && matches!(&e.payload, SyncPayload::RowBatchNeeded { row, .. } if row.row_id == doc_id)
    });
    assert!(
        doc_update_for_b.is_some(),
        "Server should send synced document to subscribed client B"
    );

    let contains_title = server_outbox.iter().any(|e| {
        matches!(e.destination, Destination::Client(cid) if cid == client_b_id)
            && matches!(
                &e.payload,
                SyncPayload::RowBatchNeeded { row, .. }
                    if row
                        .data
                        .windows("Test Document".len())
                        .any(|w| w == b"Test Document")
            )
    });
    assert!(
        contains_title,
        "Synced update to client B should contain document payload bytes"
    );
}
