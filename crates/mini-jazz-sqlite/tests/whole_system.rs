use mini_jazz_sqlite::{Runtime, Storage};
use tempfile::tempdir;

#[test]
fn memory_runtime_writes_through_sqlite_current_projection() {
    let mut alice = Runtime::open(Storage::Memory, "alice-node", "alice").unwrap();

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
    assert!(stats.physical_tx_num_for(&tx).is_some());
}

#[test]
fn query_scoped_sync_converges_memory_and_durable_nodes() {
    let dir = tempdir().unwrap();
    let worker_path = dir.path().join("worker.sqlite");

    let mut alice = Runtime::open(Storage::Memory, "alice-tab", "alice").unwrap();
    let mut worker =
        Runtime::open(Storage::File(worker_path.clone()), "alice-worker", "alice").unwrap();

    alice.create_project("project-1", "Spec work").unwrap();
    alice
        .create_todo("todo-1", "Sync through bundle", false, "project-1")
        .unwrap();

    let bundle = alice.export_query_scope_open_todos().unwrap();
    worker.apply_bundle(&bundle).unwrap();

    assert_eq!(worker.open_todos().unwrap(), alice.open_todos().unwrap());

    drop(worker);
    let reopened = Runtime::open(Storage::File(worker_path), "alice-worker", "alice").unwrap();
    assert_eq!(reopened.open_todos().unwrap(), alice.open_todos().unwrap());
}

#[test]
fn rejected_transaction_remains_history_but_is_hidden_from_current() {
    let mut alice = Runtime::open(Storage::Memory, "alice-node", "alice").unwrap();

    alice.create_project("project-1", "Spec work").unwrap();
    let tx = alice
        .create_todo("todo-1", "This will be rejected", false, "project-1")
        .unwrap();

    assert_eq!(alice.open_todos().unwrap().len(), 1);

    alice.reject_transaction(&tx, "policy_denied").unwrap();

    assert!(alice.open_todos().unwrap().is_empty());
    let stats = alice.storage_stats().unwrap();
    assert_eq!(stats.history_rows, 2);
    assert_eq!(stats.rejected_transactions, 1);
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
fn explicit_transaction_seals_multiple_mutations_atomically() {
    let mut alice = Runtime::open(Storage::Memory, "alice-node", "alice").unwrap();

    let tx = alice
        .transaction()
        .create_project("project-1", "Atomic project")
        .create_todo("todo-1", "First todo", false, "project-1")
        .create_todo("todo-2", "Second todo", false, "project-1")
        .commit()
        .unwrap();

    let todos = alice.open_todos().unwrap();
    assert_eq!(todos.len(), 2);
    assert!(todos.iter().all(|todo| todo.tx_id == tx));

    let stats = alice.storage_stats().unwrap();
    assert_eq!(stats.history_rows, 3);
    assert_eq!(stats.current_rows, 3);
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

#[test]
fn same_bundle_twice_is_idempotent() {
    let mut alice = Runtime::open(Storage::Memory, "alice-node", "alice").unwrap();
    let mut bob = Runtime::open(Storage::Memory, "bob-node", "bob").unwrap();

    alice.create_project("project-1", "Spec work").unwrap();
    alice
        .create_todo("todo-1", "Apply twice", false, "project-1")
        .unwrap();

    let bundle = alice.export_query_scope_open_todos().unwrap();
    bob.apply_bundle(&bundle).unwrap();
    bob.apply_bundle(&bundle).unwrap();

    assert_eq!(bob.open_todos().unwrap(), alice.open_todos().unwrap());
    assert_eq!(bob.storage_stats().unwrap().history_rows, 2);
}

#[test]
fn replicas_may_use_different_physical_ids_for_same_public_ids() {
    let mut alice = Runtime::open(Storage::Memory, "alice-node", "alice").unwrap();
    let mut bob = Runtime::open(Storage::Memory, "bob-node", "bob").unwrap();

    bob.create_project("bob-local-project", "Bob local")
        .unwrap();

    alice.create_project("project-1", "Spec work").unwrap();
    alice
        .create_todo("todo-1", "Different physical ids", false, "project-1")
        .unwrap();
    let bundle = alice.export_query_scope_open_todos().unwrap();
    bob.apply_bundle(&bundle).unwrap();

    assert_eq!(bob.open_todos().unwrap(), alice.open_todos().unwrap());
    assert_ne!(
        alice.physical_row_num_for("project-1").unwrap(),
        bob.physical_row_num_for("project-1").unwrap()
    );
}

#[test]
fn query_scope_is_not_table_replication() {
    let mut alice = Runtime::open(Storage::Memory, "alice-node", "alice").unwrap();
    let mut bob = Runtime::open(Storage::Memory, "bob-node", "bob").unwrap();

    alice
        .create_project("project-1", "Visible project")
        .unwrap();
    alice
        .create_todo("todo-1", "In query scope", false, "project-1")
        .unwrap();
    alice
        .create_project("project-2", "Unrelated project")
        .unwrap();

    let bundle = alice.export_query_scope_open_todos().unwrap();
    bob.apply_bundle(&bundle).unwrap();

    assert_eq!(bob.open_todos().unwrap(), alice.open_todos().unwrap());
    assert!(bob.physical_row_num_for("project-1").is_ok());
    assert!(bob.physical_row_num_for("project-2").is_err());
}

#[test]
fn authority_acceptance_enriches_existing_transaction() {
    let mut alice = Runtime::open(Storage::Memory, "alice-node", "alice").unwrap();

    alice.create_project("project-1", "Spec work").unwrap();
    let tx = alice
        .create_todo("todo-1", "Accept me", false, "project-1")
        .unwrap();
    let before = alice.transaction_info(&tx).unwrap();

    alice.accept_transaction_at_global(&tx, 7).unwrap();

    let after = alice.transaction_info(&tx).unwrap();
    assert_eq!(after.tx_id, before.tx_id);
    assert_eq!(after.global_epoch, Some(7));
    assert!(after.receipt_tiers.contains(&"global".to_owned()));
    assert_eq!(
        alice.storage_stats().unwrap().physical_tx_num_for(&tx),
        Some(alice.transaction_physical_num_for(&tx).unwrap())
    );
}
