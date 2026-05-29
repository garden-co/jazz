use super::*;

#[test]
fn memory_runtime_writes_through_sqlite_current_projection() {
    let mut alice = support::open_todo_app(Storage::Memory, "alice-node", "alice").unwrap();

    assert_eq!(alice.storage_format_version().unwrap(), 7);

    alice.create_project("project-1", "Spec work").unwrap();
    let tx = alice
        .create_todo("todo-1", "Write Attempt 3 tests", false, "project-1")
        .unwrap();

    let todos = alice.open_todos().unwrap();
    assert_eq!(todos.len(), 1);
    assert_eq!(todos[0].id, "todo-1");
    assert_eq!(todos[0].title, "Write Attempt 3 tests");
    assert_eq!(todos[0].project_title.as_deref(), Some("Spec work"));
    assert_eq!(todos[0].created_by, "alice");

    let stats = alice.storage_stats().unwrap();
    assert_eq!(stats.history_rows, 2);
    assert_eq!(stats.current_rows, 2);
    assert!(stats.page_count > 0);
    assert!(stats.page_size > 0);
    assert_eq!(stats.database_bytes, stats.page_count * stats.page_size);
    assert!(stats.physical_tx_num_for(&tx).is_some());
}

#[test]
fn durable_storage_is_tagged_with_format_version() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("versioned.sqlite");

    support::open_todo_app(Storage::File(path.clone()), "worker", "alice").unwrap();

    let conn = rusqlite::Connection::open(path).unwrap();
    let version: i64 = conn
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .unwrap();
    assert_eq!(version, 7);
}

#[test]
fn future_storage_format_versions_fail_before_opening_runtime() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("future.sqlite");
    let conn = rusqlite::Connection::open(&path).unwrap();
    conn.pragma_update(None, "user_version", 8).unwrap();
    drop(conn);

    let err = match support::open_todo_app(Storage::File(path), "worker", "alice") {
        Ok(_) => panic!("future storage format opened successfully"),
        Err(err) => err,
    };

    assert!(err
        .to_string()
        .contains("unsupported storage format version 8"));
}

#[test]
fn system_user_metadata_is_interned_but_app_user_fields_stay_text() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("interned-users.sqlite");
    let schema = SchemaDef::new().table("docs", |table| {
        table.text("owner_id");
        table.text("title");
    });

    {
        let mut alice =
            Runtime::open_with_schema(Storage::File(path.clone()), "alice-node", "alice", schema)
                .unwrap();
        alice
            .insert_row(
                "docs",
                "doc-1",
                BTreeMap::from([
                    ("owner_id".to_owned(), json!("user-visible-owner")),
                    ("title".to_owned(), json!("Intern system users")),
                ]),
            )
            .unwrap();
        let row = alice.read_rows("docs").unwrap().remove(0);
        assert_eq!(row.created_by, "alice");
        assert_eq!(row.values["owner_id"], json!("user-visible-owner"));
    }

    let conn = rusqlite::Connection::open(path).unwrap();
    let created_by_type: String = conn
        .query_row(
            "SELECT type FROM pragma_table_info('docs__schema_v1_history') WHERE name = 'j_created_by'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let owner_type: String = conn
        .query_row(
            "SELECT type FROM pragma_table_info('docs__schema_v1_history') WHERE name = 'owner_id'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let stored_created_by: i64 = conn
        .query_row(
            "SELECT j_created_by FROM docs__schema_v1_history WHERE row_num = 1",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let stored_owner_id: String = conn
        .query_row(
            "SELECT owner_id FROM docs__schema_v1_history WHERE row_num = 1",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let stored_user_id: String = conn
        .query_row(
            "SELECT user_id FROM jazz_user WHERE user_num = ?",
            [stored_created_by],
            |row| row.get(0),
        )
        .unwrap();

    assert_eq!(created_by_type, "INTEGER");
    assert_eq!(owner_type, "TEXT");
    assert_eq!(stored_user_id, "alice");
    assert_eq!(stored_owner_id, "user-visible-owner");
}

#[test]
fn previous_storage_format_versions_fail_fast() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("old.sqlite");
    let conn = rusqlite::Connection::open(&path).unwrap();
    conn.pragma_update(None, "user_version", 1).unwrap();
    drop(conn);

    let err = match support::open_todo_app(Storage::File(path), "worker", "alice") {
        Ok(_) => panic!("old storage format opened successfully"),
        Err(err) => err,
    };

    assert!(err
        .to_string()
        .contains("unsupported storage format version 1"));
}

#[test]
fn durable_nodes_survive_reopen_but_memory_nodes_start_empty() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("durable.sqlite");

    {
        let mut durable =
            support::open_todo_app(Storage::File(path.clone()), "worker", "alice").unwrap();
        durable.create_project("project-1", "Durable").unwrap();
        durable
            .create_todo("todo-1", "Survives reopen", false, "project-1")
            .unwrap();
    }

    let reopened = support::open_todo_app(Storage::File(path), "worker", "alice").unwrap();
    assert_eq!(reopened.open_todos().unwrap().len(), 1);

    let mut memory = support::open_todo_app(Storage::Memory, "tab", "alice").unwrap();
    memory.create_project("project-1", "Memory").unwrap();
    memory
        .create_todo("todo-1", "Lost on restart", false, "project-1")
        .unwrap();
    assert_eq!(memory.open_todos().unwrap().len(), 1);

    let fresh_memory = support::open_todo_app(Storage::Memory, "tab", "alice").unwrap();
    assert!(fresh_memory.open_todos().unwrap().is_empty());
}

#[test]
fn rebuild_current_projection_from_history_matches_current_reads() {
    let mut alice = support::open_todo_app(Storage::Memory, "alice-node", "alice").unwrap();

    alice.create_project("project-1", "Spec work").unwrap();
    alice
        .create_todo("todo-1", "Rebuild me", false, "project-1")
        .unwrap();
    let before = alice.open_todos().unwrap();

    alice.clear_current_projection_for_test().unwrap();
    assert!(alice.open_todos().unwrap().is_empty());

    alice.rebuild_current_projection().unwrap();
    assert_eq!(alice.open_todos().unwrap(), before);
}

#[test]
fn delete_is_history_not_removal() {
    let mut alice = support::open_todo_app(Storage::Memory, "alice-node", "alice").unwrap();

    alice.create_project("project-1", "Spec work").unwrap();
    alice
        .create_todo("todo-1", "Delete as history", false, "project-1")
        .unwrap();
    alice.delete_todo("todo-1").unwrap();

    assert!(alice.open_todos().unwrap().is_empty());
    let stats = alice.storage_stats().unwrap();
    assert_eq!(stats.history_rows, 3);
    assert_eq!(stats.current_rows, 1);
}

#[test]
fn deleted_generic_row_can_be_restored_as_new_history_version() {
    let schema = support::notes_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema).unwrap();

    alice
        .insert_row(
            "notes",
            "note-1",
            BTreeMap::from([
                ("body".to_owned(), json!("Before delete")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    alice.delete_row("notes", "note-1").unwrap();
    assert!(alice.read_rows("notes").unwrap().is_empty());

    alice.restore_deleted_row("notes", "note-1").unwrap();

    let rows = alice.read_rows("notes").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values["body"], json!("Before delete"));
    alice.clear_current_projection_for_test().unwrap();
    alice.rebuild_current_projection().unwrap();
    assert_eq!(alice.read_rows("notes").unwrap().len(), 1);
}

#[test]
fn restored_deleted_row_syncs_to_peer() {
    let schema = support::notes_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut peer =
        Runtime::open_with_schema(Storage::Memory, "peer-node", "alice", schema).unwrap();

    alice
        .insert_row(
            "notes",
            "note-1",
            BTreeMap::from([
                ("body".to_owned(), json!("Sync restore")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    alice.delete_row("notes", "note-1").unwrap();
    alice.restore_deleted_row("notes", "note-1").unwrap();

    peer.apply_bundle(&alice.export_table_history("notes").unwrap())
        .unwrap();
    let rows = peer.read_rows("notes").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values["body"], json!("Sync restore"));
}

#[test]
fn batched_logical_inserts_keep_distinct_jazz_transactions_but_commit_together() {
    let schema = support::notes_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut bob = Runtime::open_with_schema(Storage::Memory, "bob-node", "alice", schema).unwrap();

    let tx_ids = alice
        .insert_rows_batched(
            "notes",
            vec![
                (
                    "note-1".to_owned(),
                    BTreeMap::from([
                        ("body".to_owned(), json!("First")),
                        ("pinned".to_owned(), json!(false)),
                    ]),
                ),
                (
                    "note-2".to_owned(),
                    BTreeMap::from([
                        ("body".to_owned(), json!("Second")),
                        ("pinned".to_owned(), json!(true)),
                    ]),
                ),
            ],
        )
        .unwrap();

    assert_eq!(tx_ids, vec!["tx-alice-node-1", "tx-alice-node-2"]);
    assert_eq!(alice.storage_stats().unwrap().history_rows, 2);
    assert_eq!(alice.read_rows("notes").unwrap().len(), 2);

    bob.apply_bundle(&alice.export_table_history("notes").unwrap())
        .unwrap();
    assert_eq!(bob.read_rows("notes").unwrap().len(), 2);
}

#[test]
fn batched_logical_updates_keep_distinct_jazz_transactions_but_commit_together() {
    let schema = support::notes_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut bob = Runtime::open_with_schema(Storage::Memory, "bob-node", "alice", schema).unwrap();

    alice
        .insert_row(
            "notes",
            "note-1",
            BTreeMap::from([
                ("body".to_owned(), json!("v1")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    let tx_ids = alice
        .update_rows_batched(
            "notes",
            vec![
                (
                    "note-1".to_owned(),
                    BTreeMap::from([("body".to_owned(), json!("v2"))]),
                ),
                (
                    "note-1".to_owned(),
                    BTreeMap::from([("body".to_owned(), json!("v3"))]),
                ),
            ],
        )
        .unwrap();

    assert_eq!(tx_ids, vec!["tx-alice-node-2", "tx-alice-node-3"]);
    assert_eq!(
        alice.read_rows("notes").unwrap()[0].values["body"],
        json!("v3")
    );
    assert_eq!(
        alice.transaction_previous_read_rows(&tx_ids[1]).unwrap(),
        vec![("notes".to_owned(), "note-1".to_owned())]
    );

    bob.apply_bundle(&alice.export_table_history("notes").unwrap())
        .unwrap();
    assert_eq!(
        bob.read_rows("notes").unwrap()[0].values["body"],
        json!("v3")
    );
}

#[test]
fn batched_logical_upserts_can_mix_creates_and_updates() {
    let schema = support::notes_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut bob = Runtime::open_with_schema(Storage::Memory, "bob-node", "alice", schema).unwrap();

    alice
        .insert_row(
            "notes",
            "note-existing",
            BTreeMap::from([
                ("body".to_owned(), json!("before")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    let tx_ids = alice
        .upsert_rows_batched(
            "notes",
            vec![
                (
                    "note-existing".to_owned(),
                    BTreeMap::from([("body".to_owned(), json!("after"))]),
                ),
                (
                    "note-new".to_owned(),
                    BTreeMap::from([
                        ("body".to_owned(), json!("new")),
                        ("pinned".to_owned(), json!(true)),
                    ]),
                ),
            ],
        )
        .unwrap();

    assert_eq!(tx_ids, vec!["tx-alice-node-2", "tx-alice-node-3"]);
    let rows = alice.read_rows("notes").unwrap();
    assert_eq!(rows.len(), 2);
    assert!(rows
        .iter()
        .any(|row| row.id == "note-existing" && row.values["body"] == json!("after")));
    assert!(rows
        .iter()
        .any(|row| row.id == "note-new" && row.values["body"] == json!("new")));

    bob.apply_bundle(&alice.export_table_history("notes").unwrap())
        .unwrap();
    assert_eq!(bob.read_rows("notes").unwrap().len(), 2);
}

#[test]
fn batched_logical_writes_roll_back_atomically_on_validation_error() {
    let schema = support::notes_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema).unwrap();

    let err = alice
        .insert_rows_batched(
            "notes",
            vec![
                (
                    "note-1".to_owned(),
                    BTreeMap::from([
                        ("body".to_owned(), json!("first")),
                        ("pinned".to_owned(), json!(false)),
                    ]),
                ),
                (
                    "note-2".to_owned(),
                    BTreeMap::from([
                        ("body".to_owned(), json!("second")),
                        ("unknown".to_owned(), json!(true)),
                    ]),
                ),
            ],
        )
        .unwrap_err();

    assert!(err.to_string().contains("unknown field unknown"));
    assert!(alice.read_rows("notes").unwrap().is_empty());
    assert_eq!(alice.storage_stats().unwrap().history_rows, 0);
}

#[test]
fn transaction_lookup_handles_hyphenated_node_ids() {
    let schema = support::notes_schema();
    let mut alice = Runtime::open_with_schema(
        Storage::Memory,
        "550e8400-e29b-41d4-a716-446655440000",
        "alice",
        schema,
    )
    .unwrap();

    alice
        .insert_row(
            "notes",
            "note-1",
            BTreeMap::from([
                ("body".to_owned(), json!("v1")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    let tx = alice
        .update_row(
            "notes",
            "note-1",
            BTreeMap::from([("body".to_owned(), json!("v2"))]),
        )
        .unwrap();

    assert_eq!(tx, "tx-550e8400-e29b-41d4-a716-446655440000-2");
    assert_eq!(alice.transaction_info(&tx).unwrap().tx_id, tx);
    assert_eq!(
        alice.transaction_write_rows(&tx).unwrap(),
        vec![("notes".to_owned(), "note-1".to_owned())]
    );
    assert_eq!(
        alice.transaction_previous_read_rows(&tx).unwrap(),
        vec![("notes".to_owned(), "note-1".to_owned())]
    );
}
