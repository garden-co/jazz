use super::*;

#[test]
fn rebac_insert_allowed_by_simple_policy() {
    // Setup
    let sync_manager = SyncManager::new();
    let schema = rebac_test_schema();
    let mut qm = create_query_manager(sync_manager, schema);
    let mut storage = seeded_memory_storage(&qm.schema_context().current_schema);

    // Add a client with session
    let client_id = ClientId::new();
    connect_client(&mut qm, &storage, client_id);
    qm.sync_manager_mut()
        .set_client_session(client_id, Session::new("alice"));

    // Create an object for the row
    let obj_id = create_test_row(&mut storage, Some(document_metadata()));

    // Register a query scope so the update is in-scope
    let mut scope = HashSet::new();
    scope.insert((obj_id, "main".into()));
    set_client_query_scope(&mut qm, &storage, client_id, QueryId(1), scope, None);
    qm.sync_manager_mut().take_outbox();

    // Encode row content: owner_id = "alice", title = "My Doc", folder_id = NULL
    let content = encode_document("alice", "My Doc", None);

    // Client sends insert
    let commit = stored_row_commit(
        smallvec![],
        content,
        1000,
        ObjectId::new().to_string(),
        None,
    );

    qm.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Client(client_id),
        payload: row_batch_created_payload(
            obj_id,
            "main",
            Some(RowMetadata {
                id: obj_id,
                metadata: document_metadata(),
            }),
            &commit,
        ),
    });

    // Process - should evaluate policy and approve
    qm.process(&mut storage);

    // Commit should be applied (owner matches session user)
    let tips = test_row_tip_ids(&storage, obj_id, "main").unwrap_or_default();
    assert!(
        tips.contains(&row_batch_id_for_commit(obj_id, "main", &commit)),
        "Insert should be approved when owner matches session"
    );
}

#[test]
fn rebac_insert_denied_by_simple_policy() {
    // Setup
    let sync_manager = SyncManager::new();
    let schema = rebac_test_schema();
    let mut qm = create_query_manager(sync_manager, schema);
    let mut storage = seeded_memory_storage(&qm.schema_context().current_schema);

    // Add a client with session
    let client_id = ClientId::new();
    connect_client(&mut qm, &storage, client_id);
    qm.sync_manager_mut()
        .set_client_session(client_id, Session::new("alice"));

    // Create an object for the row
    let obj_id = create_test_row(&mut storage, Some(document_metadata()));

    // Register a query scope
    let mut scope = HashSet::new();
    scope.insert((obj_id, "main".into()));
    set_client_query_scope(&mut qm, &storage, client_id, QueryId(1), scope, None);
    qm.sync_manager_mut().take_outbox();

    // Encode row content: owner_id = "bob" (different from session user)
    let content = encode_document("bob", "Stolen Doc", None);

    // Client sends insert
    let commit = stored_row_commit(
        smallvec![],
        content,
        1000,
        ObjectId::new().to_string(),
        None,
    );

    qm.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Client(client_id),
        payload: row_batch_created_payload(
            obj_id,
            "main",
            Some(RowMetadata {
                id: obj_id,
                metadata: document_metadata(),
            }),
            &commit,
        ),
    });

    // Process - should evaluate policy and reject
    qm.process(&mut storage);

    // Should get permission denied error
    let outbox = qm.sync_manager_mut().take_outbox();
    let reason = client_write_rejection_reason(
        &outbox,
        client_id,
        row_batch_id_for_commit(obj_id, "main", &commit),
    )
    .expect("Should receive rejection response");
    assert!(
        reason.contains("denied by policy") || reason == "rejected",
        "Rejection should mention policy denial: {reason}"
    );

    // Commit should NOT be applied
    let tips = test_row_tip_ids(&storage, obj_id, "main");
    assert!(
        tips.is_err()
            || !tips
                .unwrap()
                .contains(&row_batch_id_for_commit(obj_id, "main", &commit)),
        "Insert should be denied when owner doesn't match session"
    );
}

#[test]
fn rebac_insert_denied_by_current_permissions_in_server_mode_known_schema() {
    let authorization_schema = rebac_test_schema();
    let schema: Schema = authorization_schema
        .iter()
        .map(|(table_name, table_schema)| {
            let mut structural = table_schema.clone();
            structural.policies = TablePolicies::default();
            (*table_name, structural)
        })
        .collect();
    let schema_hash = SchemaHash::compute(&schema);
    let branch = ComposedBranchName::new("dev", schema_hash, "main")
        .to_branch_name()
        .as_str()
        .to_string();

    // Server mode: the branch schema has no embedded policies, but the server should still
    // enforce the latest authorization schema.
    let sync_manager = SyncManager::new();
    let mut qm = QueryManager::new(sync_manager);
    let mut known_schemas = HashMap::new();
    known_schemas.insert(schema_hash, schema);
    qm.set_known_schemas(Arc::new(known_schemas));
    qm.set_authorization_schema(authorization_schema);

    let mut storage = MemoryStorage::new();

    let client_id = ClientId::new();
    connect_client(&mut qm, &storage, client_id);
    qm.sync_manager_mut()
        .set_client_session(client_id, Session::new("alice"));

    let metadata = document_metadata();
    let obj_id = create_test_row(&mut storage, Some(metadata.clone()));

    let mut scope = HashSet::new();
    scope.insert((obj_id, branch.clone().into()));
    set_client_query_scope(&mut qm, &storage, client_id, QueryId(1), scope, None);
    qm.sync_manager_mut().take_outbox();

    let commit = stored_row_commit(
        smallvec![],
        encode_document("bob", "Should Be Denied", None),
        1000,
        ObjectId::new().to_string(),
        None,
    );

    qm.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Client(client_id),
        payload: row_batch_created_payload(
            obj_id,
            &branch,
            Some(RowMetadata {
                id: obj_id,
                metadata,
            }),
            &commit,
        ),
    });

    qm.process(&mut storage);

    let outbox = qm.sync_manager_mut().take_outbox();
    let denied = client_write_was_rejected(
        &outbox,
        client_id,
        row_batch_id_for_commit(obj_id, &branch, &commit),
    );
    assert!(
        denied,
        "Insert should be denied by current permissions in server mode"
    );

    let tips = test_row_tip_ids(&storage, obj_id, &branch);
    assert!(
        tips.is_err()
            || !tips
                .unwrap()
                .contains(&row_batch_id_for_commit(obj_id, &branch, &commit)),
        "Denied insert should not be applied on the branch"
    );
}

#[test]
fn rebac_insert_denied_for_new_object_uses_payload_metadata_in_server_mode() {
    let schema = rebac_test_schema();
    let schema_hash = SchemaHash::compute(&schema);
    let branch = ComposedBranchName::new("dev", schema_hash, "main")
        .to_branch_name()
        .as_str()
        .to_string();

    // Server mode: no current schema, schema available via known_schemas.
    let sync_manager = SyncManager::new();
    let mut qm = QueryManager::new(sync_manager);
    let mut known_schemas = HashMap::new();
    known_schemas.insert(schema_hash, schema);
    qm.set_known_schemas(Arc::new(known_schemas));

    let mut storage = MemoryStorage::new();

    let client_id = ClientId::new();
    connect_client(&mut qm, &storage, client_id);
    qm.sync_manager_mut()
        .set_client_session(client_id, Session::new("alice"));

    // New row object: metadata exists only in the payload, not in preseeded storage.
    let obj_id = ObjectId::new();
    let metadata = document_metadata();
    let commit = stored_row_commit(
        smallvec![],
        encode_document("bob", "Should Be Denied", None),
        1000,
        ObjectId::new().to_string(),
        None,
    );

    qm.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Client(client_id),
        payload: row_batch_created_payload(
            obj_id,
            &branch,
            Some(RowMetadata {
                id: obj_id,
                metadata,
            }),
            &commit,
        ),
    });

    qm.process(&mut storage);

    let outbox = qm.sync_manager_mut().take_outbox();
    let denied = client_write_was_rejected(
        &outbox,
        client_id,
        row_batch_id_for_commit(obj_id, &branch, &commit),
    );
    assert!(
        denied,
        "Insert should be denied for new objects using payload metadata in server mode"
    );

    let tips = test_row_tip_ids(&storage, obj_id, &branch);
    assert!(
        tips.is_err()
            || !tips
                .unwrap()
                .contains(&row_batch_id_for_commit(obj_id, &branch, &commit)),
        "Denied insert should not be applied on the branch"
    );
}

#[test]
fn rebac_insert_waits_for_schema_then_denies_for_composed_branch() {
    let schema = rebac_test_schema();
    let schema_hash = SchemaHash::compute(&schema);
    let branch = ComposedBranchName::new("dev", schema_hash, "main")
        .to_branch_name()
        .as_str()
        .to_string();

    // Server mode starts without a fixed current schema and may learn schemas later.
    let sync_manager = SyncManager::new();
    let mut qm = QueryManager::new(sync_manager);
    let mut storage = MemoryStorage::new();

    let client_id = ClientId::new();
    connect_client(&mut qm, &storage, client_id);
    qm.sync_manager_mut()
        .set_client_session(client_id, Session::new("alice"));

    let obj_id = ObjectId::new();
    let metadata = document_metadata();
    let commit = stored_row_commit(
        smallvec![],
        encode_document("bob", "Should Be Denied", None),
        1000,
        ObjectId::new().to_string(),
        None,
    );

    qm.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Client(client_id),
        payload: row_batch_created_payload(
            obj_id,
            &branch,
            Some(RowMetadata {
                id: obj_id,
                metadata: metadata.clone(),
            }),
            &commit,
        ),
    });

    // First pass should defer until the schema becomes available instead of allowing or denying.
    qm.process(&mut storage);

    assert!(
        qm.sync_manager_mut().take_outbox().is_empty(),
        "Composed-branch writes should wait for schema activation before emitting a result"
    );

    let pending = qm.sync_manager_mut().take_pending_permission_checks();
    assert_eq!(
        pending.len(),
        1,
        "Write should remain pending until the matching schema arrives"
    );
    qm.sync_manager_mut()
        .requeue_pending_permission_checks(pending);

    let tips = test_row_tip_ids(&storage, obj_id, &branch);
    assert!(
        tips.is_err()
            || !tips
                .unwrap()
                .contains(&row_batch_id_for_commit(obj_id, &branch, &commit)),
        "Deferred insert must not be applied before the schema is known"
    );

    let mut known_schemas = HashMap::new();
    known_schemas.insert(schema_hash, schema);
    qm.set_known_schemas(Arc::new(known_schemas));

    qm.process(&mut storage);

    let outbox = qm.sync_manager_mut().take_outbox();
    let denied = client_write_was_rejected(
        &outbox,
        client_id,
        row_batch_id_for_commit(obj_id, &branch, &commit),
    );
    assert!(
        denied,
        "Once the schema is available, the deferred insert should be denied by policy"
    );
}

#[test]
fn rebac_insert_denied_when_schema_never_arrives_before_timeout() {
    let schema = rebac_test_schema();
    let schema_hash = SchemaHash::compute(&schema);
    let branch = ComposedBranchName::new("dev", schema_hash, "main")
        .to_branch_name()
        .as_str()
        .to_string();

    let sync_manager = SyncManager::new();
    let mut qm = QueryManager::new(sync_manager);
    let mut storage = MemoryStorage::new();

    let client_id = ClientId::new();
    connect_client(&mut qm, &storage, client_id);
    qm.sync_manager_mut()
        .set_client_session(client_id, Session::new("alice"));

    let obj_id = ObjectId::new();
    let metadata = document_metadata();
    let commit = stored_row_commit(
        smallvec![],
        encode_document("bob", "Should Time Out", None),
        1000,
        ObjectId::new().to_string(),
        None,
    );

    qm.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Client(client_id),
        payload: row_batch_created_payload(
            obj_id,
            &branch,
            Some(RowMetadata {
                id: obj_id,
                metadata: metadata.clone(),
            }),
            &commit,
        ),
    });

    qm.process(&mut storage);

    assert!(
        qm.sync_manager_mut().take_outbox().is_empty(),
        "First pass should defer while waiting for schema activation"
    );

    let mut pending = qm.sync_manager_mut().take_pending_permission_checks();
    assert_eq!(pending.len(), 1, "Deferred write should remain pending");
    pending[0].schema_wait_started_at = Some(Instant::now() - Duration::from_secs(11));
    qm.sync_manager_mut()
        .requeue_pending_permission_checks(pending);

    qm.process(&mut storage);

    let outbox = qm.sync_manager_mut().take_outbox();
    let reason = client_write_rejection_reason(
        &outbox,
        client_id,
        row_batch_id_for_commit(obj_id, &branch, &commit),
    )
    .expect("Timed-out schema wait should return a rejection to the client");
    assert!(
        reason.contains("after waiting 10s") || reason == "rejected",
        "Timed-out schema wait should mention the 10s timeout: {reason}"
    );

    let tips = test_row_tip_ids(&storage, obj_id, &branch);
    assert!(
        tips.is_err()
            || !tips
                .unwrap()
                .contains(&row_batch_id_for_commit(obj_id, "main", &commit)),
        "Timed-out insert should not be applied on the branch"
    );
}

#[test]
fn rebac_insert_denied_when_schema_unresolved_for_branch() {
    let schema = rebac_test_schema();
    let schema_hash = SchemaHash::compute(&schema);

    // Server mode: no current schema, only known_schemas.
    let sync_manager = SyncManager::new();
    let mut qm = QueryManager::new(sync_manager);
    let mut known_schemas = HashMap::new();
    known_schemas.insert(schema_hash, schema);
    qm.set_known_schemas(Arc::new(known_schemas));

    let mut storage = MemoryStorage::new();

    let client_id = ClientId::new();
    connect_client(&mut qm, &storage, client_id);
    qm.sync_manager_mut()
        .set_client_session(client_id, Session::new("alice"));

    let obj_id = ObjectId::new();
    let metadata = document_metadata();
    let commit = stored_row_commit(
        smallvec![],
        encode_document("bob", "Should Be Denied", None),
        1000,
        ObjectId::new().to_string(),
        None,
    );

    // Plain "main" branch without schema hash context can fail schema resolution.
    qm.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Client(client_id),
        payload: row_batch_created_payload(
            obj_id,
            "main",
            Some(RowMetadata {
                id: obj_id,
                metadata,
            }),
            &commit,
        ),
    });

    qm.process(&mut storage);

    let outbox = qm.sync_manager_mut().take_outbox();
    let denied = client_write_was_rejected(
        &outbox,
        client_id,
        row_batch_id_for_commit(obj_id, "main", &commit),
    );
    assert!(
        denied,
        "Insert should be denied when schema cannot be resolved for the write branch"
    );

    let tips = test_row_tip_ids(&storage, obj_id, "main");
    assert!(
        tips.is_err()
            || !tips
                .unwrap()
                .contains(&row_batch_id_for_commit(obj_id, "main", &commit)),
        "Denied insert should not be applied on unresolved branch writes"
    );
}

#[test]
fn rebac_insert_denied_when_stale_self_schema_would_otherwise_allow() {
    let restrictive = rebac_test_schema();
    let restrictive_hash = SchemaHash::compute(&restrictive);

    // Permissive local schema (no insert policy) that should NOT be used for server writes
    // on unrelated branches.
    let mut permissive = Schema::new();
    permissive.insert(
        TableName::new("documents"),
        RowDescriptor::new(vec![
            ColumnDescriptor::new("owner_id", ColumnType::Text),
            ColumnDescriptor::new("title", ColumnType::Text),
            ColumnDescriptor::new("folder_id", ColumnType::Uuid).nullable(),
        ])
        .into(),
    );

    let sync_manager = SyncManager::new();
    let mut qm = create_query_manager(sync_manager, permissive);
    let mut known_schemas = HashMap::new();
    known_schemas.insert(restrictive_hash, restrictive);
    qm.set_known_schemas(Arc::new(known_schemas));

    let mut storage = seeded_memory_storage(&qm.schema_context().current_schema);

    let client_id = ClientId::new();
    connect_client(&mut qm, &storage, client_id);
    qm.sync_manager_mut()
        .set_client_session(client_id, Session::new("alice"));

    let obj_id = ObjectId::new();
    let metadata = document_metadata();
    let commit = stored_row_commit(
        smallvec![],
        encode_document("bob", "Should Be Denied", None),
        1000,
        ObjectId::new().to_string(),
        None,
    );

    // Simulate write on an unresolved branch. Prior behavior could fall back to stale
    // self.schema (permissive) and incorrectly allow this insert.
    qm.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Client(client_id),
        payload: row_batch_created_payload(
            obj_id,
            "main",
            Some(RowMetadata {
                id: obj_id,
                metadata,
            }),
            &commit,
        ),
    });

    qm.process(&mut storage);

    let outbox = qm.sync_manager_mut().take_outbox();
    let denied = client_write_was_rejected(
        &outbox,
        client_id,
        row_batch_id_for_commit(obj_id, "main", &commit),
    );
    assert!(
        denied,
        "Insert should be denied instead of using stale self.schema on unresolved branches"
    );

    let tips = test_row_tip_ids(&storage, obj_id, "main");
    assert!(
        tips.is_err()
            || !tips
                .unwrap()
                .contains(&row_batch_id_for_commit(obj_id, "main", &commit)),
        "Denied insert should not be applied when stale self.schema fallback is unsafe"
    );
}

#[test]
fn permissive_local_runtime_without_loaded_policies_allows_sync_pending_write_without_policy() {
    // Schema with no policies
    let mut schema = Schema::new();
    schema.insert(
        TableName::new("notes"),
        RowDescriptor::new(vec![ColumnDescriptor::new("content", ColumnType::Text)]).into(),
    );

    let sync_manager = SyncManager::new();
    let mut qm = create_query_manager(sync_manager, schema);
    let mut storage = seeded_memory_storage(&qm.schema_context().current_schema);

    // Add a client with session
    let client_id = ClientId::new();
    connect_client(&mut qm, &storage, client_id);
    qm.sync_manager_mut()
        .set_client_session(client_id, Session::new("alice"));

    // Create an object for the row
    let mut metadata = std::collections::HashMap::new();
    metadata.insert(MetadataKey::Table.to_string(), "notes".to_string());
    let obj_id = create_test_row(&mut storage, Some(metadata.clone()));

    // Register a query scope
    let mut scope = HashSet::new();
    scope.insert((obj_id, "main".into()));
    set_client_query_scope(&mut qm, &storage, client_id, QueryId(1), scope, None);
    qm.sync_manager_mut().take_outbox();

    // Encode row content
    let notes_desc = RowDescriptor::new(vec![ColumnDescriptor::new("content", ColumnType::Text)]);
    let content = encode_row(&notes_desc, &[Value::Text("A note".into())]).unwrap();

    // Client sends insert
    let commit = stored_row_commit(
        smallvec![],
        content,
        1000,
        ObjectId::new().to_string(),
        None,
    );

    qm.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Client(client_id),
        payload: row_batch_created_payload(
            obj_id,
            "main",
            Some(RowMetadata {
                id: obj_id,
                metadata,
            }),
            &commit,
        ),
    });

    // Process - policy-less local runtimes should remain permissive.
    qm.process(&mut storage);

    // Commit should be applied
    let tips = test_row_tip_ids(&storage, obj_id, "main").unwrap_or_default();
    assert!(
        tips.contains(&row_batch_id_for_commit(obj_id, "main", &commit)),
        "Table without policy should allow all writes"
    );
}

#[test]
fn loaded_empty_permissions_bundle_denies_sync_pending_write_without_explicit_policy() {
    let mut schema = Schema::new();
    schema.insert(
        TableName::new("notes"),
        RowDescriptor::new(vec![ColumnDescriptor::new("content", ColumnType::Text)]).into(),
    );

    let sync_manager = SyncManager::new();
    let mut qm = create_query_manager(sync_manager, schema.clone());
    let mut storage = seeded_memory_storage(&qm.schema_context().current_schema);

    qm.set_authorization_schema(schema);

    let client_id = ClientId::new();
    connect_client(&mut qm, &storage, client_id);
    qm.sync_manager_mut()
        .set_client_session(client_id, Session::new("alice"));

    let mut metadata = std::collections::HashMap::new();
    metadata.insert(MetadataKey::Table.to_string(), "notes".to_string());
    let obj_id = create_test_row(&mut storage, Some(metadata.clone()));

    let mut scope = HashSet::new();
    scope.insert((obj_id, "main".into()));
    set_client_query_scope(&mut qm, &storage, client_id, QueryId(1), scope, None);
    qm.sync_manager_mut().take_outbox();

    let notes_desc = RowDescriptor::new(vec![ColumnDescriptor::new("content", ColumnType::Text)]);
    let content = encode_row(&notes_desc, &[Value::Text("A note".into())]).unwrap();
    let commit = stored_row_commit(
        smallvec![],
        content,
        1000,
        ObjectId::new().to_string(),
        None,
    );

    qm.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Client(client_id),
        payload: row_batch_created_payload(
            obj_id,
            "main",
            Some(RowMetadata {
                id: obj_id,
                metadata,
            }),
            &commit,
        ),
    });

    qm.process(&mut storage);

    let outbox = qm.sync_manager_mut().take_outbox();
    assert!(
        client_write_was_rejected(
            &outbox,
            client_id,
            row_batch_id_for_commit(obj_id, "main", &commit),
        ),
        "loaded empty permissions bundle should reject sync writes without explicit permission"
    );

    let tips = test_row_tip_ids(&storage, obj_id, "main").unwrap_or_default();
    assert!(
        !tips.contains(&row_batch_id_for_commit(obj_id, "main", &commit)),
        "denied sync write should not persist"
    );
}

#[test]
fn rebac_two_clients_different_sessions() {
    // Setup
    let sync_manager = SyncManager::new();
    let schema = rebac_test_schema();
    let mut qm = create_query_manager(sync_manager, schema);
    let mut storage = seeded_memory_storage(&qm.schema_context().current_schema);

    // Client 1: alice
    let client1 = ClientId::new();
    connect_client(&mut qm, &storage, client1);
    qm.sync_manager_mut()
        .set_client_session(client1, Session::new("alice"));

    // Client 2: bob
    let client2 = ClientId::new();
    connect_client(&mut qm, &storage, client2);
    qm.sync_manager_mut()
        .set_client_session(client2, Session::new("bob"));

    // Create objects for both clients
    let obj1 = create_test_row(&mut storage, Some(document_metadata()));
    let obj2 = create_test_row(&mut storage, Some(document_metadata()));

    // Register query scopes
    let mut scope1 = HashSet::new();
    scope1.insert((obj1, "main".into()));
    set_client_query_scope(&mut qm, &storage, client1, QueryId(1), scope1, None);

    let mut scope2 = HashSet::new();
    scope2.insert((obj2, "main".into()));
    set_client_query_scope(&mut qm, &storage, client2, QueryId(2), scope2, None);

    qm.sync_manager_mut().take_outbox();

    // Alice's document
    let content1 = encode_document("alice", "Alice's Doc", None);
    let commit1 = stored_row_commit(
        smallvec![],
        content1,
        1000,
        ObjectId::new().to_string(),
        None,
    );

    // Bob's document
    let content2 = encode_document("bob", "Bob's Doc", None);
    let commit2 = stored_row_commit(
        smallvec![],
        content2,
        1000,
        ObjectId::new().to_string(),
        None,
    );

    // Both clients send their documents
    qm.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Client(client1),
        payload: row_batch_created_payload(
            obj1,
            "main",
            Some(RowMetadata {
                id: obj1,
                metadata: document_metadata(),
            }),
            &commit1,
        ),
    });

    qm.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Client(client2),
        payload: row_batch_created_payload(
            obj2,
            "main",
            Some(RowMetadata {
                id: obj2,
                metadata: document_metadata(),
            }),
            &commit2,
        ),
    });

    // Process
    qm.process(&mut storage);

    // Both commits should be applied (each owner matches their session)
    let tips1 = test_row_tip_ids(&storage, obj1, "main").unwrap();
    assert!(
        tips1.contains(&row_batch_id_for_commit(obj1, "main", &commit1)),
        "Alice's document should be approved"
    );

    let tips2 = test_row_tip_ids(&storage, obj2, "main").unwrap();
    assert!(
        tips2.contains(&row_batch_id_for_commit(obj2, "main", &commit2)),
        "Bob's document should be approved"
    );
}

#[test]
fn local_insert_policy_with_null_literal_allows_null_rows_and_denies_non_null_rows() {
    let mut schema = Schema::new();
    let tasks_descriptor = RowDescriptor::new(vec![
        ColumnDescriptor::new("title", ColumnType::Text),
        ColumnDescriptor::new("deleted_at", ColumnType::Text).nullable(),
    ]);
    let tasks_policies =
        TablePolicies::new().with_insert(PolicyExpr::eq_literal("deleted_at", Value::Null));
    schema.insert(
        TableName::new("tasks"),
        TableSchema::with_policies(tasks_descriptor, tasks_policies),
    );

    let sync_manager = SyncManager::new();
    let mut qm = create_query_manager(sync_manager, schema);
    let mut storage = seeded_memory_storage(&qm.schema_context().current_schema);

    qm.insert_with_session(
        &mut storage,
        "tasks",
        &[Value::Text("draft".into()), Value::Null],
        Some(&Session::new("alice")),
    )
    .expect("null row should satisfy deleted_at = NULL policy");

    let archived_err = qm
        .insert_with_session(
            &mut storage,
            "tasks",
            &[
                Value::Text("archived".into()),
                Value::Text("2026-03-30T12:00:00Z".into()),
            ],
            Some(&Session::new("alice")),
        )
        .expect_err("non-null row should fail deleted_at = NULL policy");
    assert!(matches!(
        archived_err,
        QueryError::PolicyDenied {
            table,
            operation: Operation::Insert
        } if table == TableName::new("tasks")
    ));
}
