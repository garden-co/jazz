use super::*;

#[test]
fn rebac_insert_denied_by_current_permissions_in_server_mode_known_schema() {
    let authorization_schema = rebac_test_schema();
    let schema: Schema = authorization_schema
        .iter()
        .map(|(table_name, table_schema)| {
            let mut structural = table_schema.clone();
            structural.policies = Default::default();
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
    let permissive = SchemaBuilder::new()
        .table(
            TableSchema::builder("documents")
                .column("owner_id", ColumnType::Text)
                .column("title", ColumnType::Text)
                .nullable_column("folder_id", ColumnType::Uuid),
        )
        .build();

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
