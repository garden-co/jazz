use super::*;

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
    let mut core = new_test_core(schema_manager, MemoryStorage::new(), NoopScheduler);
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

    let ((project_id, _), _) = core
        .insert("projects", project_insert_values("Acme", "alice"), None)
        .unwrap();

    let ((todo_id, _), _) = core
        .insert(
            "todos",
            todo_insert_values(
                "Buy milk",
                true,
                Value::Null,
                "alice",
                Value::Uuid(project_id),
            ),
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

    let ((project_id, _), _) = core
        .insert("projects", project_insert_values("Acme", "alice"), None)
        .unwrap();

    let ((todo_id, _), _) = core
        .insert(
            "todos",
            todo_insert_values(
                "Buy milk",
                true,
                Value::Null,
                "alice",
                Value::Uuid(project_id),
            ),
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

// =========================================================================
// Disconnect cleanup: parked message guard
// =========================================================================

#[test]
fn remove_client_blocked_by_parked_sync_messages() {
    //
    // alice ──WS──▶ server (message parked in RuntimeCore, not yet in SyncManager inbox)
    //
    // Sweep tries to reap alice → remove_client returns false because
    // parked_sync_messages contains an entry from alice.
    //
    use crate::metadata::RowProvenance;

    let mut core = create_test_runtime();
    let alice = ClientId::new();
    core.add_client(alice, None);

    // Park a message from alice (simulates push_sync_inbox before batched_tick)
    core.park_sync_message(InboxEntry {
        source: Source::Client(alice),
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
    });

    let removed = core.remove_client(alice);
    assert!(!removed, "should refuse to reap with parked messages");

    // Client state must be preserved
    assert!(
        core.schema_manager()
            .query_manager()
            .sync_manager()
            .get_client(alice)
            .is_some(),
        "alice's ClientState should be preserved"
    );
}

#[test]
fn remove_client_succeeds_after_parked_messages_drained() {
    //
    // alice ──WS──▶ server (message parked) ──batched_tick──▶ inbox drained
    //
    // After batched_tick processes the parked message, remove_client succeeds.
    //
    use crate::metadata::RowProvenance;

    let mut core = create_test_runtime();
    let alice = ClientId::new();
    core.add_client(alice, None);

    core.park_sync_message(InboxEntry {
        source: Source::Client(alice),
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
    });

    // Drain parked messages via batched_tick
    core.batched_tick();

    let removed = core.remove_client(alice);
    assert!(removed, "should succeed after parked messages are drained");

    assert!(
        core.schema_manager()
            .query_manager()
            .sync_manager()
            .get_client(alice)
            .is_none(),
        "alice should be removed"
    );
}

#[test]
fn remove_client_ignores_parked_messages_from_other_clients() {
    //
    // bob ──WS──▶ server (message parked)
    //
    // alice disconnects → remove_client(alice) succeeds because
    // the parked message is from bob, not alice.
    //
    use crate::metadata::RowProvenance;

    let mut core = create_test_runtime();
    let alice = ClientId::new();
    let bob = ClientId::new();
    core.add_client(alice, None);
    core.add_client(bob, None);

    // Park a message from bob
    core.park_sync_message(InboxEntry {
        source: Source::Client(bob),
        payload: SyncPayload::RowBatchCreated {
            metadata: None,
            row: crate::row_histories::StoredRowBatch::new(
                ObjectId::new(),
                "main",
                Vec::new(),
                b"bob".to_vec(),
                RowProvenance::for_insert(ObjectId::new().to_string(), 1_000),
                HashMap::new(),
                crate::row_histories::RowState::VisibleDirect,
                None,
            ),
        },
    });

    let removed = core.remove_client(alice);
    assert!(removed, "alice has no parked messages — should succeed");

    assert!(
        core.schema_manager()
            .query_manager()
            .sync_manager()
            .get_client(alice)
            .is_none(),
        "alice should be removed"
    );
    assert!(
        core.schema_manager()
            .query_manager()
            .sync_manager()
            .get_client(bob)
            .is_some(),
        "bob should be preserved"
    );
}

#[test]
fn query_error_anonymous_write_denied_maps_to_structured_runtime_error() {
    use crate::query_manager::manager::QueryError;
    use crate::query_manager::policy::Operation;
    use crate::query_manager::types::TableName;

    let err: RuntimeError = QueryError::AnonymousWriteDenied {
        table: TableName::new("todos"),
        operation: Operation::Insert,
    }
    .into();
    match err {
        RuntimeError::AnonymousWriteDenied {
            ref table,
            operation,
        } => {
            assert_eq!(table.as_str(), "todos");
            assert_eq!(operation, Operation::Insert);
        }
        other => panic!("expected AnonymousWriteDenied, got {other:?}"),
    }
}
