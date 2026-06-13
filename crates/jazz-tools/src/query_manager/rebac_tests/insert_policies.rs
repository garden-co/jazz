#[cfg(feature = "test-utils")]
use crate::JazzClient;

use super::*;

#[cfg(feature = "test-utils")]
async fn enforcing_test_client(schema: Schema) -> JazzClient {
    JazzClient::connect_with_row_policy_mode(
        crate::AppContext::test(schema),
        crate::query_manager::types::RowPolicyMode::Enforcing,
    )
    .await
    .expect("connect enforcing local JazzClient")
}

#[cfg(feature = "test-utils")]
#[tokio::test]
async fn rebac_insert_allowed_by_simple_policy() {
    let client = JazzClient::test_client(rebac_test_schema()).await;

    client
        .for_session(Session::new("alice"))
        .insert(
            "documents",
            crate::row_input!(
                "owner_id" => "alice",
                "title" => "My Doc",
                "folder_id" => Value::Null,
            ),
        )
        .expect("insert should be allowed when owner_id matches the session user");
}

#[cfg(feature = "test-utils")]
#[tokio::test]
async fn rebac_insert_denied_by_simple_policy() {
    let client = JazzClient::test_client(rebac_test_schema()).await;

    let err = client
        .for_session(Session::new("alice"))
        .insert(
            "documents",
            crate::row_input!(
                "owner_id" => "bob",
                "title" => "Stolen Doc",
                "folder_id" => Value::Null,
            ),
        )
        .expect_err("insert should be denied when owner_id does not match the session user");
    assert_client_policy_denied(err, "documents", Operation::Insert);
}

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

#[cfg(feature = "test-utils")]
#[tokio::test]
async fn permissive_local_runtime_without_loaded_policies_allows_sync_pending_write_without_policy()
{
    let notes_table = TableSchema::builder("notes").column("content", ColumnType::Text);
    let schema = SchemaBuilder::new().table(notes_table).build();
    let client = JazzClient::test_client(schema).await;

    let (note_id, _, _) = client
        .insert("notes", crate::row_input!("content" => "A note"))
        .expect("table without explicit policies should allow local writes");
    let rows = client
        .query(
            QueryBuilder::new("notes")
                .filter_eq("id", Value::Uuid(note_id))
                .select(&["content"])
                .build(),
            None,
        )
        .await
        .expect("query inserted note");
    assert_eq!(
        rows,
        vec![(note_id, vec![Value::Text("A note".into())])],
        "table without explicit policies should expose the inserted row"
    );
}

#[cfg(feature = "test-utils")]
#[tokio::test]
async fn loaded_empty_permissions_bundle_denies_sync_pending_write_without_explicit_policy() {
    let notes_table = TableSchema::builder("notes").column("content", ColumnType::Text);
    let schema = SchemaBuilder::new().table(notes_table).build();
    let client = enforcing_test_client(schema).await;

    let err = client
        .for_session(Session::new("alice"))
        .insert("notes", crate::row_input!("content" => "A note"))
        .expect_err("enforcing client should deny writes without an explicit insert policy");
    assert_client_policy_denied(err, "notes", Operation::Insert);
}

#[cfg(feature = "test-utils")]
#[tokio::test]
async fn rebac_two_clients_different_sessions() {
    let client = JazzClient::test_client(rebac_test_schema()).await;

    let (alice_doc, _, _) = client
        .for_session(Session::new("alice"))
        .insert(
            "documents",
            crate::row_input!(
                "owner_id" => "alice",
                "title" => "Alice's Doc",
                "folder_id" => Value::Null,
            ),
        )
        .expect("alice should be able to insert alice-owned document");
    let (bob_doc, _, _) = client
        .for_session(Session::new("bob"))
        .insert(
            "documents",
            crate::row_input!(
                "owner_id" => "bob",
                "title" => "Bob's Doc",
                "folder_id" => Value::Null,
            ),
        )
        .expect("bob should be able to insert bob-owned document");

    let alice_visible_docs: HashSet<_> = client
        .for_session(Session::new("alice"))
        .query(
            QueryBuilder::new("documents").select(&["title"]).build(),
            None,
        )
        .await
        .expect("query documents as alice")
        .into_iter()
        .map(|(id, _)| id)
        .collect();
    assert!(
        alice_visible_docs.contains(&alice_doc),
        "alice should see alice-owned document"
    );
    assert!(
        !alice_visible_docs.contains(&bob_doc),
        "alice should not see bob-owned document"
    );

    let bob_visible_docs: HashSet<_> = client
        .for_session(Session::new("bob"))
        .query(
            QueryBuilder::new("documents").select(&["title"]).build(),
            None,
        )
        .await
        .expect("query documents as bob")
        .into_iter()
        .map(|(id, _)| id)
        .collect();
    assert!(
        bob_visible_docs.contains(&bob_doc),
        "bob should see bob-owned document"
    );
    assert!(
        !bob_visible_docs.contains(&alice_doc),
        "bob should not see alice-owned document"
    );
}

#[cfg(feature = "test-utils")]
#[tokio::test]
async fn local_insert_policy_with_null_literal_allows_null_rows_and_denies_non_null_rows() {
    let tasks_policies = permissions(|p| {
        p.allow_insert().where_(pe::eq("deleted_at", pe::null()));
    });
    let schema = SchemaBuilder::new()
        .table(
            TableSchema::builder("tasks")
                .column("title", ColumnType::Text)
                .nullable_column("deleted_at", ColumnType::Text)
                .policies(tasks_policies),
        )
        .build();

    let client = JazzClient::test_client(schema).await;

    client
        .for_session(Session::new("alice"))
        .insert(
            "tasks",
            crate::row_input!("title" => "draft", "deleted_at" => Value::Null),
        )
        .expect("null row should satisfy deleted_at = NULL policy");

    let archived_err = client
        .for_session(Session::new("alice"))
        .insert(
            "tasks",
            crate::row_input!("title" => "archived", "deleted_at" => "2026-03-30T12:00:00Z"),
        )
        .expect_err("non-null row should fail deleted_at = NULL policy");
    assert_client_policy_denied(archived_err, "tasks", Operation::Insert);
}
