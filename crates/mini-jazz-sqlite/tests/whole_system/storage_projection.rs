use super::*;

#[test]
fn memory_runtime_writes_through_sqlite_current_projection() {
    let mut alice = Runtime::open(Storage::Memory, "alice-node", "alice").unwrap();

    assert_eq!(alice.storage_format_version().unwrap(), 2);

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

    Runtime::open(Storage::File(path.clone()), "worker", "alice").unwrap();

    let conn = rusqlite::Connection::open(path).unwrap();
    let version: i64 = conn
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .unwrap();
    assert_eq!(version, 2);
}

#[test]
fn future_storage_format_versions_fail_before_opening_runtime() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("future.sqlite");
    let conn = rusqlite::Connection::open(&path).unwrap();
    conn.pragma_update(None, "user_version", 3).unwrap();
    drop(conn);

    let err = match Runtime::open(Storage::File(path), "worker", "alice") {
        Ok(_) => panic!("future storage format opened successfully"),
        Err(err) => err,
    };

    assert!(err
        .to_string()
        .contains("unsupported storage format version 3"));
}

#[test]
fn previous_storage_format_versions_fail_fast() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("old.sqlite");
    let conn = rusqlite::Connection::open(&path).unwrap();
    conn.pragma_update(None, "user_version", 1).unwrap();
    drop(conn);

    let err = match Runtime::open(Storage::File(path), "worker", "alice") {
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
        let mut durable = Runtime::open(Storage::File(path.clone()), "worker", "alice").unwrap();
        durable.create_project("project-1", "Durable").unwrap();
        durable
            .create_todo("todo-1", "Survives reopen", false, "project-1")
            .unwrap();
    }

    let reopened = Runtime::open(Storage::File(path), "worker", "alice").unwrap();
    assert_eq!(reopened.open_todos().unwrap().len(), 1);

    let mut memory = Runtime::open(Storage::Memory, "tab", "alice").unwrap();
    memory.create_project("project-1", "Memory").unwrap();
    memory
        .create_todo("todo-1", "Lost on restart", false, "project-1")
        .unwrap();
    assert_eq!(memory.open_todos().unwrap().len(), 1);

    let fresh_memory = Runtime::open(Storage::Memory, "tab", "alice").unwrap();
    assert!(fresh_memory.open_todos().unwrap().is_empty());
}

#[test]
fn rebuild_current_projection_from_history_matches_current_reads() {
    let mut alice = Runtime::open(Storage::Memory, "alice-node", "alice").unwrap();

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
    let mut alice = Runtime::open(Storage::Memory, "alice-node", "alice").unwrap();

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
