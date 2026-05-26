use mini_jazz_sqlite::{RowDiff, Runtime, SchemaDef, Storage};
use serde_json::json;
use std::collections::BTreeMap;
use tempfile::tempdir;

mod support;

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
    assert_eq!(
        alice.transaction_info(&tx).unwrap().rejection_code,
        Some("policy_denied".to_owned())
    );
}

#[test]
fn rejected_fate_update_repairs_peer_current_projection() {
    let mut alice = Runtime::open(Storage::Memory, "alice-node", "alice").unwrap();
    let mut peer = Runtime::open(Storage::Memory, "alice-peer-node", "alice").unwrap();

    alice.create_project("project-1", "Spec work").unwrap();
    let tx = alice
        .create_todo("todo-1", "Optimistic then rejected", false, "project-1")
        .unwrap();
    peer.apply_bundle(&alice.export_table_history("todos").unwrap())
        .unwrap();
    assert_eq!(peer.open_todos().unwrap().len(), 1);

    alice.reject_transaction(&tx, "policy_denied").unwrap();
    peer.apply_bundle(&alice.export_table_history("todos").unwrap())
        .unwrap();

    assert!(peer.open_todos().unwrap().is_empty());
    assert_eq!(peer.storage_stats().unwrap().rejected_transactions, 1);
}

#[test]
fn durable_worker_reconciles_rejected_fate_after_restart() {
    let dir = tempdir().unwrap();
    let worker_path = dir.path().join("worker.sqlite");

    let mut tab = Runtime::open(Storage::Memory, "alice-tab", "alice").unwrap();
    tab.create_project("project-1", "Spec work").unwrap();
    let tx = tab
        .create_todo("todo-1", "Optimistic before restart", false, "project-1")
        .unwrap();

    {
        let mut worker =
            Runtime::open(Storage::File(worker_path.clone()), "alice-worker", "alice").unwrap();
        worker
            .apply_bundle(&tab.export_table_history("todos").unwrap())
            .unwrap();
        assert_eq!(worker.open_todos().unwrap().len(), 1);
    }

    tab.reject_transaction(&tx, "policy_denied").unwrap();

    let mut reopened =
        Runtime::open(Storage::File(worker_path.clone()), "alice-worker", "alice").unwrap();
    assert_eq!(reopened.open_todos().unwrap().len(), 1);
    reopened
        .apply_bundle(&tab.export_table_history("todos").unwrap())
        .unwrap();

    assert!(reopened.open_todos().unwrap().is_empty());
    let stats = reopened.storage_stats().unwrap();
    assert_eq!(stats.history_rows, 1);
    assert_eq!(stats.rejected_transactions, 1);
}

#[test]
fn rejecting_generic_transaction_repairs_schema_driven_projection() {
    let schema = SchemaDef::new().table("notes", |table| {
        table.text("body");
        table.bool("pinned");
    });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema).unwrap();
    let tx = alice
        .insert_row(
            "notes",
            "note-1",
            BTreeMap::from([
                ("body".to_owned(), json!("Reject me")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    assert_eq!(alice.read_rows("notes").unwrap().len(), 1);

    alice.reject_transaction(&tx, "policy_denied").unwrap();

    assert!(alice.read_rows("notes").unwrap().is_empty());
    let stats = alice.storage_stats().unwrap();
    assert_eq!(stats.history_rows, 1);
    assert_eq!(stats.current_rows, 0);
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
fn generic_transaction_seals_multiple_rows_atomically() {
    let schema = SchemaDef::new().table("notes", |table| {
        table.text("body");
        table.bool("pinned");
    });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema).unwrap();

    let tx = alice
        .transaction()
        .insert_row(
            "notes",
            "note-1",
            BTreeMap::from([
                ("body".to_owned(), json!("First")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .insert_row(
            "notes",
            "note-2",
            BTreeMap::from([
                ("body".to_owned(), json!("Second")),
                ("pinned".to_owned(), json!(true)),
            ]),
        )
        .commit()
        .unwrap();

    let rows = alice.read_rows("notes").unwrap();
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().all(|row| row.tx_id == tx));
    assert_eq!(
        alice.transaction_write_rows(&tx).unwrap(),
        vec![
            ("notes".to_owned(), "note-1".to_owned()),
            ("notes".to_owned(), "note-2".to_owned())
        ]
    );
}

#[test]
fn exclusive_transaction_requires_global_epoch_and_commits_accepted() {
    let schema = support::notes_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema).unwrap();

    let err = alice
        .transaction()
        .exclusive()
        .insert_row(
            "notes",
            "note-local-exclusive",
            BTreeMap::from([
                ("body".to_owned(), json!("No local exclusive")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .commit()
        .unwrap_err();
    assert!(err
        .to_string()
        .contains("exclusive transactions require global"));
    assert!(alice.read_rows("notes").unwrap().is_empty());

    let tx = alice
        .transaction()
        .exclusive_at_global(7)
        .insert_row(
            "notes",
            "note-global-exclusive",
            BTreeMap::from([
                ("body".to_owned(), json!("Global exclusive")),
                ("pinned".to_owned(), json!(true)),
            ]),
        )
        .commit()
        .unwrap();

    let rows = alice.read_rows("notes").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, "note-global-exclusive");
    assert_eq!(alice.transaction_info(&tx).unwrap().global_epoch, Some(7));
    assert!(alice
        .transaction_info(&tx)
        .unwrap()
        .receipt_tiers
        .contains(&"global".to_owned()));
}

#[test]
fn exclusive_transaction_mode_survives_sync() {
    let schema = support::notes_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut peer =
        Runtime::open_with_schema(Storage::Memory, "alice-peer-node", "alice", schema).unwrap();

    let tx = alice
        .transaction()
        .exclusive_at_global(7)
        .insert_row(
            "notes",
            "note-1",
            BTreeMap::from([
                ("body".to_owned(), json!("Exclusive sync")),
                ("pinned".to_owned(), json!(true)),
            ]),
        )
        .commit()
        .unwrap();

    peer.apply_bundle(&alice.export_table_history("notes").unwrap())
        .unwrap();

    let info = peer.transaction_info(&tx).unwrap();
    assert_eq!(info.global_epoch, Some(7));
    assert_eq!(info.conflict_mode, "exclusive");
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
fn table_scope_sync_exports_delete_so_peer_removes_row() {
    let mut alice = Runtime::open(Storage::Memory, "alice-node", "alice").unwrap();
    let mut peer = Runtime::open(Storage::Memory, "alice-peer-node", "alice").unwrap();

    alice.create_project("project-1", "Spec work").unwrap();
    alice
        .create_todo("todo-1", "Delete through sync", false, "project-1")
        .unwrap();
    peer.apply_bundle(&alice.export_table_history("todos").unwrap())
        .unwrap();
    assert_eq!(peer.open_todos().unwrap().len(), 1);

    alice.delete_todo("todo-1").unwrap();
    peer.apply_bundle(&alice.export_table_history("todos").unwrap())
        .unwrap();

    assert!(peer.open_todos().unwrap().is_empty());
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

#[test]
fn global_epoch_can_accept_multiple_transactions() {
    let mut alice = Runtime::open(Storage::Memory, "alice-node", "alice").unwrap();

    alice.create_project("project-1", "Spec work").unwrap();
    let first = alice
        .create_todo("todo-1", "First in epoch", false, "project-1")
        .unwrap();
    let second = alice
        .create_todo("todo-2", "Second in epoch", false, "project-1")
        .unwrap();

    alice.accept_transaction_at_global(&first, 7).unwrap();
    alice.accept_transaction_at_global(&second, 7).unwrap();

    assert_eq!(
        alice.transaction_info(&first).unwrap().global_epoch,
        Some(7)
    );
    assert_eq!(
        alice.transaction_info(&second).unwrap().global_epoch,
        Some(7)
    );
}

#[test]
fn accepted_global_fate_update_reaches_peer_transaction_info() {
    let mut alice = Runtime::open(Storage::Memory, "alice-node", "alice").unwrap();
    let mut peer = Runtime::open(Storage::Memory, "alice-peer-node", "alice").unwrap();

    alice.create_project("project-1", "Spec work").unwrap();
    let tx = alice
        .create_todo("todo-1", "Accept me remotely", false, "project-1")
        .unwrap();
    peer.apply_bundle(&alice.export_table_history("todos").unwrap())
        .unwrap();
    assert_eq!(peer.transaction_info(&tx).unwrap().global_epoch, None);

    alice.accept_transaction_at_global(&tx, 7).unwrap();
    peer.apply_bundle(&alice.export_table_history("todos").unwrap())
        .unwrap();

    let info = peer.transaction_info(&tx).unwrap();
    assert_eq!(info.global_epoch, Some(7));
    assert!(info.receipt_tiers.contains(&"global".to_owned()));
}

#[test]
fn stale_pending_bundle_does_not_downgrade_accepted_fate() {
    let mut alice = Runtime::open(Storage::Memory, "alice-node", "alice").unwrap();
    let mut peer = Runtime::open(Storage::Memory, "alice-peer-node", "alice").unwrap();

    alice.create_project("project-1", "Spec work").unwrap();
    let tx = alice
        .create_todo("todo-1", "Out of order fate", false, "project-1")
        .unwrap();
    let pending_bundle = alice.export_table_history("todos").unwrap();
    peer.apply_bundle(&pending_bundle).unwrap();

    alice.accept_transaction_at_global(&tx, 7).unwrap();
    peer.apply_bundle(&alice.export_table_history("todos").unwrap())
        .unwrap();
    assert_eq!(peer.transaction_info(&tx).unwrap().global_epoch, Some(7));

    peer.apply_bundle(&pending_bundle).unwrap();
    assert_eq!(peer.transaction_info(&tx).unwrap().global_epoch, Some(7));
    assert_eq!(
        peer.transaction_info(&tx).unwrap().conflict_mode,
        "mergeable"
    );
}

#[test]
fn runtime_can_install_and_write_a_non_todo_schema() {
    let schema = SchemaDef::new().table("notes", |table| {
        table.text("body");
        table.bool("pinned");
    });
    let mut runtime =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema).unwrap();
    let mut values = BTreeMap::new();
    values.insert("body".to_owned(), json!("Generic schema works"));
    values.insert("pinned".to_owned(), json!(true));

    let tx = runtime.insert_row("notes", "note-1", values).unwrap();

    let stats = runtime.storage_stats().unwrap();
    assert_eq!(stats.history_rows, 1);
    assert_eq!(stats.current_rows, 1);
    assert!(stats.physical_tx_num_for(&tx).is_some());
    assert!(runtime.physical_row_num_for("note-1").is_ok());

    runtime.clear_current_projection_for_test().unwrap();
    assert_eq!(runtime.storage_stats().unwrap().current_rows, 0);

    runtime.rebuild_current_projection().unwrap();
    let rebuilt = runtime.storage_stats().unwrap();
    assert_eq!(rebuilt.history_rows, 1);
    assert_eq!(rebuilt.current_rows, 1);
}

#[test]
fn generic_schema_rows_rebuild_and_sync_by_public_ids() {
    let schema = SchemaDef::new()
        .table("docs", |table| {
            table.text("title");
        })
        .table("comments", |table| {
            table.text("body");
            table.bool("resolved");
            table.ref_("doc", "docs");
        });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut bob = Runtime::open_with_schema(Storage::Memory, "bob-node", "bob", schema).unwrap();

    let mut doc = BTreeMap::new();
    doc.insert("title".to_owned(), json!("Design notes"));
    alice.insert_row("docs", "doc-1", doc).unwrap();

    let mut comment = BTreeMap::new();
    comment.insert("body".to_owned(), json!("Needs policy pass"));
    comment.insert("resolved".to_owned(), json!(false));
    comment.insert("doc".to_owned(), json!("doc-1"));
    let comment_tx = alice.insert_row("comments", "comment-1", comment).unwrap();

    let bundle = alice.export_table_history("comments").unwrap();
    bob.apply_bundle(&bundle).unwrap();
    bob.clear_current_projection_for_test().unwrap();
    bob.rebuild_current_projection().unwrap();

    let comments = bob.read_rows("comments").unwrap();
    assert_eq!(comments.len(), 1);
    assert_eq!(comments[0].table, "comments");
    assert_eq!(comments[0].id, "comment-1");
    assert_eq!(comments[0].values["body"], json!("Needs policy pass"));
    assert_eq!(comments[0].values["resolved"], json!(false));
    assert_eq!(comments[0].values["doc"], json!("doc-1"));
    assert_eq!(
        bob.transaction_write_rows(&comment_tx).unwrap(),
        vec![("comments".to_owned(), "comment-1".to_owned())]
    );
    assert_ne!(
        alice.physical_row_num_for("doc-1").unwrap(),
        bob.physical_row_num_for("doc-1").unwrap()
    );
}

#[test]
fn policy_filters_reads_through_required_parent_ref() {
    let schema = SchemaDef::new()
        .table("projects", |table| {
            table.text("title");
            table.read_if_created_by_principal();
        })
        .table("todos", |table| {
            table.text("title");
            table.bool("done");
            table.ref_("project", "projects");
            table.read_if_ref_readable("project");
        });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut bob = Runtime::open_with_schema(Storage::Memory, "bob-node", "bob", schema).unwrap();

    let mut alice_project = BTreeMap::new();
    alice_project.insert("title".to_owned(), json!("Alice project"));
    alice
        .insert_row("projects", "project-alice", alice_project)
        .unwrap();
    let mut alice_todo = BTreeMap::new();
    alice_todo.insert("title".to_owned(), json!("Visible"));
    alice_todo.insert("done".to_owned(), json!(false));
    alice_todo.insert("project".to_owned(), json!("project-alice"));
    alice
        .insert_row("todos", "todo-visible", alice_todo)
        .unwrap();

    let mut bob_project = BTreeMap::new();
    bob_project.insert("title".to_owned(), json!("Bob project"));
    bob.insert_row("projects", "project-bob", bob_project)
        .unwrap();
    let mut bob_todo = BTreeMap::new();
    bob_todo.insert("title".to_owned(), json!("Hidden"));
    bob_todo.insert("done".to_owned(), json!(false));
    bob_todo.insert("project".to_owned(), json!("project-bob"));
    bob.insert_row("todos", "todo-hidden", bob_todo).unwrap();

    alice
        .apply_bundle(&bob.export_table_history("projects").unwrap())
        .unwrap();
    alice
        .apply_bundle(&bob.export_table_history("todos").unwrap())
        .unwrap();

    let visible = alice.read_rows("todos").unwrap();
    assert_eq!(visible.len(), 1);
    assert_eq!(visible[0].id, "todo-visible");
    assert_eq!(visible[0].values["project"], json!("project-alice"));

    let scoped_bundle = alice.export_table_history("todos").unwrap();
    assert!(scoped_bundle
        .history
        .iter()
        .any(|record| record.table == "todos" && record.row_id == "todo-visible"));
    assert!(scoped_bundle
        .history
        .iter()
        .any(|record| record.table == "projects" && record.row_id == "project-alice"));
}

#[test]
fn recursive_policy_filters_reads_through_grandparent_ref() {
    let schema = SchemaDef::new()
        .table("orgs", |table| {
            table.text("name");
            table.read_if_created_by_principal();
        })
        .table("projects", |table| {
            table.text("title");
            table.ref_("org", "orgs");
            table.read_if_ref_readable("org");
        })
        .table("todos", |table| {
            table.text("title");
            table.ref_("project", "projects");
            table.read_if_ref_readable("project");
        });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut bob = Runtime::open_with_schema(Storage::Memory, "bob-node", "bob", schema).unwrap();

    alice
        .insert_row(
            "orgs",
            "org-alice",
            BTreeMap::from([("name".to_owned(), json!("Alice org"))]),
        )
        .unwrap();
    alice
        .insert_row(
            "projects",
            "project-alice",
            BTreeMap::from([
                ("title".to_owned(), json!("Alice project")),
                ("org".to_owned(), json!("org-alice")),
            ]),
        )
        .unwrap();
    alice
        .insert_row(
            "todos",
            "todo-visible",
            BTreeMap::from([
                ("title".to_owned(), json!("Visible")),
                ("project".to_owned(), json!("project-alice")),
            ]),
        )
        .unwrap();

    bob.insert_row(
        "orgs",
        "org-bob",
        BTreeMap::from([("name".to_owned(), json!("Bob org"))]),
    )
    .unwrap();
    bob.insert_row(
        "projects",
        "project-bob",
        BTreeMap::from([
            ("title".to_owned(), json!("Bob project")),
            ("org".to_owned(), json!("org-bob")),
        ]),
    )
    .unwrap();
    bob.insert_row(
        "todos",
        "todo-hidden",
        BTreeMap::from([
            ("title".to_owned(), json!("Hidden")),
            ("project".to_owned(), json!("project-bob")),
        ]),
    )
    .unwrap();

    alice
        .apply_bundle(&bob.export_table_history("orgs").unwrap())
        .unwrap();
    alice
        .apply_bundle(&bob.export_table_history("projects").unwrap())
        .unwrap();
    alice
        .apply_bundle(&bob.export_table_history("todos").unwrap())
        .unwrap();

    let visible = alice.read_rows("todos").unwrap();
    assert_eq!(visible.len(), 1);
    assert_eq!(visible[0].id, "todo-visible");
    assert_eq!(visible[0].values["project"], json!("project-alice"));
}

#[test]
fn recursive_policy_scoped_sync_includes_transitive_parent_rows() {
    let schema = SchemaDef::new()
        .table("orgs", |table| {
            table.text("name");
            table.read_if_created_by_principal();
        })
        .table("projects", |table| {
            table.text("title");
            table.ref_("org", "orgs");
            table.read_if_ref_readable("org");
        })
        .table("todos", |table| {
            table.text("title");
            table.ref_("project", "projects");
            table.read_if_ref_readable("project");
        });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut peer =
        Runtime::open_with_schema(Storage::Memory, "alice-peer-node", "alice", schema).unwrap();

    alice
        .insert_row(
            "orgs",
            "org-visible",
            BTreeMap::from([("name".to_owned(), json!("Visible org"))]),
        )
        .unwrap();
    alice
        .insert_row(
            "orgs",
            "org-unrelated",
            BTreeMap::from([("name".to_owned(), json!("Unrelated org"))]),
        )
        .unwrap();
    alice
        .insert_row(
            "projects",
            "project-visible",
            BTreeMap::from([
                ("title".to_owned(), json!("Visible project")),
                ("org".to_owned(), json!("org-visible")),
            ]),
        )
        .unwrap();
    alice
        .insert_row(
            "todos",
            "todo-visible",
            BTreeMap::from([
                ("title".to_owned(), json!("Visible todo")),
                ("project".to_owned(), json!("project-visible")),
            ]),
        )
        .unwrap();

    let bundle = alice.export_table_history("todos").unwrap();
    let synced = bundle
        .history
        .iter()
        .map(|record| (record.table.as_str(), record.row_id.as_str()))
        .collect::<Vec<_>>();
    assert!(synced.contains(&("todos", "todo-visible")));
    assert!(synced.contains(&("projects", "project-visible")));
    assert!(synced.contains(&("orgs", "org-visible")));
    assert!(!synced.contains(&("orgs", "org-unrelated")));

    peer.apply_bundle(&bundle).unwrap();
    let rows = peer.read_rows("todos").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, "todo-visible");
}

#[test]
fn recursive_query_reads_policy_filtered_tree() {
    let schema = support::folders_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut bob = Runtime::open_with_schema(Storage::Memory, "bob-node", "bob", schema).unwrap();

    alice
        .insert_row(
            "folders",
            "root",
            BTreeMap::from([
                ("name".to_owned(), json!("Root")),
                ("parent".to_owned(), json!("root")),
            ]),
        )
        .unwrap();
    alice
        .insert_row(
            "folders",
            "child-alice",
            BTreeMap::from([
                ("name".to_owned(), json!("Alice child")),
                ("parent".to_owned(), json!("root")),
            ]),
        )
        .unwrap();
    bob.insert_row(
        "folders",
        "child-bob",
        BTreeMap::from([
            ("name".to_owned(), json!("Bob child")),
            ("parent".to_owned(), json!("root")),
        ]),
    )
    .unwrap();
    alice
        .apply_bundle(&bob.export_table_history("folders").unwrap())
        .unwrap();

    let rows = alice
        .read_recursive_refs("folders", "root", "parent")
        .unwrap();
    let ids = rows.iter().map(|row| row.id.as_str()).collect::<Vec<_>>();
    assert_eq!(ids, vec!["root", "child-alice"]);
}

#[test]
fn recursive_query_scope_sync_recreates_policy_filtered_tree() {
    let schema = support::folders_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut peer =
        Runtime::open_with_schema(Storage::Memory, "alice-peer-node", "alice", schema).unwrap();

    alice
        .insert_row(
            "folders",
            "root",
            BTreeMap::from([
                ("name".to_owned(), json!("Root")),
                ("parent".to_owned(), json!("root")),
            ]),
        )
        .unwrap();
    alice
        .insert_row(
            "folders",
            "child",
            BTreeMap::from([
                ("name".to_owned(), json!("Child")),
                ("parent".to_owned(), json!("root")),
            ]),
        )
        .unwrap();
    alice
        .insert_row(
            "folders",
            "unrelated",
            BTreeMap::from([
                ("name".to_owned(), json!("Unrelated")),
                ("parent".to_owned(), json!("unrelated")),
            ]),
        )
        .unwrap();

    let bundle = alice
        .export_recursive_refs("folders", "root", "parent")
        .unwrap();
    let synced = bundle
        .history
        .iter()
        .map(|record| record.row_id.as_str())
        .collect::<Vec<_>>();
    assert_eq!(synced, vec!["root", "child"]);
    assert!(!synced.contains(&"unrelated"));

    peer.apply_bundle(&bundle).unwrap();
    let ids = peer
        .read_recursive_refs("folders", "root", "parent")
        .unwrap()
        .iter()
        .map(|row| row.id.clone())
        .collect::<Vec<_>>();
    assert_eq!(ids, vec!["root", "child"]);
}

#[test]
fn policy_scoped_sync_includes_required_parent_rows_only() {
    let schema = SchemaDef::new()
        .table("projects", |table| {
            table.text("title");
            table.read_if_created_by_principal();
        })
        .table("todos", |table| {
            table.text("title");
            table.ref_("project", "projects");
            table.read_if_ref_readable("project");
        });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut bob =
        Runtime::open_with_schema(Storage::Memory, "alice-peer-node", "alice", schema.clone())
            .unwrap();

    let mut visible_project = BTreeMap::new();
    visible_project.insert("title".to_owned(), json!("Visible project"));
    alice
        .insert_row("projects", "project-visible", visible_project)
        .unwrap();

    let mut unrelated_project = BTreeMap::new();
    unrelated_project.insert("title".to_owned(), json!("Unrelated project"));
    alice
        .insert_row("projects", "project-unrelated", unrelated_project)
        .unwrap();

    let mut visible_todo = BTreeMap::new();
    visible_todo.insert("title".to_owned(), json!("Visible todo"));
    visible_todo.insert("project".to_owned(), json!("project-visible"));
    alice
        .insert_row("todos", "todo-visible", visible_todo)
        .unwrap();

    let bundle = alice.export_table_history("todos").unwrap();
    let synced = bundle
        .history
        .iter()
        .map(|record| (record.table.as_str(), record.row_id.as_str()))
        .collect::<Vec<_>>();
    assert!(synced.contains(&("todos", "todo-visible")));
    assert!(synced.contains(&("projects", "project-visible")));
    assert!(!synced.contains(&("projects", "project-unrelated")));

    bob.apply_bundle(&bundle).unwrap();
    let rows = bob.read_rows("todos").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values["project"], json!("project-visible"));
}

#[test]
fn trusted_peer_can_read_applied_policy_scoped_facts_without_user_principal() {
    let schema = SchemaDef::new()
        .table("projects", |table| {
            table.text("title");
            table.read_if_created_by_principal();
        })
        .table("todos", |table| {
            table.text("title");
            table.ref_("project", "projects");
            table.read_if_ref_readable("project");
        });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut trusted =
        Runtime::open_trusted_with_schema(Storage::Memory, "worker-node", schema).unwrap();

    let mut project = BTreeMap::new();
    project.insert("title".to_owned(), json!("Alice project"));
    alice.insert_row("projects", "project-1", project).unwrap();
    let mut todo = BTreeMap::new();
    todo.insert("title".to_owned(), json!("Policy-scoped fact"));
    todo.insert("project".to_owned(), json!("project-1"));
    alice.insert_row("todos", "todo-1", todo).unwrap();

    trusted
        .apply_bundle(&alice.export_table_history("todos").unwrap())
        .unwrap();

    let rows = trusted.read_rows("todos").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, "todo-1");
}

#[test]
fn trusted_peer_generic_transaction_bypasses_user_write_policy() {
    let schema = SchemaDef::new()
        .table("docs", |table| {
            table.text("title");
            table.read_if_created_by_principal();
        })
        .table("comments", |table| {
            table.text("body");
            table.ref_("doc", "docs");
            table.write_if_ref_readable("doc");
        });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut trusted =
        Runtime::open_trusted_with_schema(Storage::Memory, "worker-node", schema).unwrap();

    let mut doc = BTreeMap::new();
    doc.insert("title".to_owned(), json!("Alice doc"));
    alice.insert_row("docs", "doc-1", doc).unwrap();
    trusted
        .apply_bundle(&alice.export_table_history("docs").unwrap())
        .unwrap();

    trusted
        .transaction()
        .insert_row(
            "comments",
            "comment-1",
            BTreeMap::from([
                ("body".to_owned(), json!("Trusted write")),
                ("doc".to_owned(), json!("doc-1")),
            ]),
        )
        .commit()
        .unwrap();

    assert_eq!(trusted.read_rows("comments").unwrap().len(), 1);
    assert_eq!(trusted.storage_stats().unwrap().rejected_transactions, 0);
}

#[test]
fn trusted_edge_accepts_mergeable_tx_then_untrusted_peers_enforce_policy() {
    let dir = tempdir().unwrap();
    let edge_path = dir.path().join("edge.sqlite");
    let schema = SchemaDef::new().table("notes", |table| {
        table.text("body");
        table.bool("pinned");
        table.read_if_created_by_principal();
    });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-tab", "alice", schema.clone()).unwrap();
    let mut edge =
        Runtime::open_trusted_with_schema(Storage::File(edge_path), "edge", schema.clone())
            .unwrap();
    let mut alice_phone =
        Runtime::open_with_schema(Storage::Memory, "alice-phone", "alice", schema.clone()).unwrap();
    let mut bob = Runtime::open_with_schema(Storage::Memory, "bob-tab", "bob", schema).unwrap();

    let tx = alice
        .insert_row(
            "notes",
            "note-1",
            BTreeMap::from([
                ("body".to_owned(), json!("Accepted at edge")),
                ("pinned".to_owned(), json!(true)),
            ]),
        )
        .unwrap();
    edge.apply_bundle(&alice.export_table_history("notes").unwrap())
        .unwrap();
    edge.accept_transaction_at_global(&tx, 11).unwrap();

    let accepted_bundle = edge.export_table_history("notes").unwrap();
    alice_phone.apply_bundle(&accepted_bundle).unwrap();
    bob.apply_bundle(&accepted_bundle).unwrap();

    assert_eq!(alice_phone.read_rows("notes").unwrap().len(), 1);
    assert_eq!(
        alice_phone.transaction_info(&tx).unwrap().global_epoch,
        Some(11)
    );
    assert!(bob.read_rows("notes").unwrap().is_empty());
}

#[test]
fn policy_denied_write_is_rejected_history_not_current_state() {
    let schema = SchemaDef::new()
        .table("docs", |table| {
            table.text("title");
            table.read_if_created_by_principal();
        })
        .table("comments", |table| {
            table.text("body");
            table.ref_("doc", "docs");
            table.write_if_ref_readable("doc");
        });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut bob = Runtime::open_with_schema(Storage::Memory, "bob-node", "bob", schema).unwrap();

    let mut doc = BTreeMap::new();
    doc.insert("title".to_owned(), json!("Alice-only doc"));
    alice.insert_row("docs", "doc-1", doc).unwrap();
    bob.apply_bundle(&alice.export_table_history("docs").unwrap())
        .unwrap();

    let mut comment = BTreeMap::new();
    comment.insert("body".to_owned(), json!("Bob should not write this"));
    comment.insert("doc".to_owned(), json!("doc-1"));
    let rejected_tx = bob
        .insert_row("comments", "comment-denied", comment)
        .unwrap();

    assert!(bob.read_rows("comments").unwrap().is_empty());
    let stats = bob.storage_stats().unwrap();
    assert_eq!(stats.history_rows, 2);
    assert_eq!(stats.current_rows, 1);
    assert_eq!(stats.rejected_transactions, 1);
    assert!(stats.physical_tx_num_for(&rejected_tx).is_some());
}

#[test]
fn subscription_initial_snapshot_matches_query_then_diffs_semantic_rows() {
    let schema = SchemaDef::new().table("tasks", |table| {
        table.text("title");
        table.bool("done");
    });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema).unwrap();

    let mut first = BTreeMap::new();
    first.insert("title".to_owned(), json!("Initial"));
    first.insert("done".to_owned(), json!(false));
    alice.insert_row("tasks", "task-1", first).unwrap();

    let mut subscription = alice.subscribe_rows("tasks").unwrap();
    assert_eq!(
        subscription.initial_rows(),
        alice.read_rows("tasks").unwrap()
    );

    let mut second = BTreeMap::new();
    second.insert("title".to_owned(), json!("Added later"));
    second.insert("done".to_owned(), json!(false));
    alice.insert_row("tasks", "task-2", second).unwrap();
    let diffs = alice.poll_subscription(&mut subscription).unwrap();
    assert!(matches!(&diffs[..], [RowDiff::Added(row)] if row.id == "task-2"));

    let mut update = BTreeMap::new();
    update.insert("title".to_owned(), json!("Renamed"));
    update.insert("done".to_owned(), json!(false));
    alice.insert_row("tasks", "task-1", update).unwrap();
    let diffs = alice.poll_subscription(&mut subscription).unwrap();
    assert!(matches!(
        &diffs[..],
        [RowDiff::Updated { before, after }]
            if before.id == "task-1"
                && before.values["title"] == json!("Initial")
                && after.values["title"] == json!("Renamed")
    ));

    let delete_tx = alice.delete_row("tasks", "task-2").unwrap();
    assert_eq!(
        alice.transaction_write_rows(&delete_tx).unwrap(),
        vec![("tasks".to_owned(), "task-2".to_owned())]
    );
    let diffs = alice.poll_subscription(&mut subscription).unwrap();
    assert!(matches!(&diffs[..], [RowDiff::Removed(row)] if row.id == "task-2"));
}

#[test]
fn subscription_removes_child_when_parent_policy_dependency_changes() {
    let schema = SchemaDef::new()
        .table("projects", |table| {
            table.text("title");
            table.read_if_created_by_principal();
        })
        .table("todos", |table| {
            table.text("title");
            table.ref_("project", "projects");
            table.read_if_ref_readable("project");
        });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut bob = Runtime::open_with_schema(Storage::Memory, "bob-node", "bob", schema).unwrap();

    let mut project = BTreeMap::new();
    project.insert("title".to_owned(), json!("Initially Alice-readable"));
    alice.insert_row("projects", "project-1", project).unwrap();
    let mut todo = BTreeMap::new();
    todo.insert("title".to_owned(), json!("Depends on project"));
    todo.insert("project".to_owned(), json!("project-1"));
    alice.insert_row("todos", "todo-1", todo).unwrap();

    let mut subscription = alice.subscribe_rows("todos").unwrap();
    assert_eq!(subscription.initial_rows().len(), 1);

    let mut bob_project = BTreeMap::new();
    bob_project.insert("title".to_owned(), json!("Now Bob-owned"));
    bob.insert_row("projects", "project-1", bob_project)
        .unwrap();
    alice
        .apply_bundle(&bob.export_table_history("projects").unwrap())
        .unwrap();

    let diffs = alice.poll_subscription(&mut subscription).unwrap();
    assert!(matches!(&diffs[..], [RowDiff::Removed(row)] if row.id == "todo-1"));
}

#[test]
fn branch_local_write_is_invisible_on_main() {
    let schema = SchemaDef::new().table("tasks", |table| {
        table.text("title");
        table.bool("done");
    });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema).unwrap();

    alice.create_branch("draft", None).unwrap();
    alice.checkout_branch("draft").unwrap();
    let mut task = BTreeMap::new();
    task.insert("title".to_owned(), json!("Draft-only"));
    task.insert("done".to_owned(), json!(false));
    alice.insert_row("tasks", "task-draft", task).unwrap();

    assert_eq!(alice.read_rows("tasks").unwrap().len(), 1);

    alice.checkout_branch("main").unwrap();
    assert!(alice.read_rows("tasks").unwrap().is_empty());

    alice.checkout_branch("draft").unwrap();
    assert_eq!(alice.read_rows("tasks").unwrap()[0].id, "task-draft");
}

#[test]
fn branch_scoped_export_excludes_unrelated_branch_rows() {
    let schema = SchemaDef::new().table("tasks", |table| {
        table.text("title");
        table.bool("done");
    });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema).unwrap();

    alice.create_branch("draft", None).unwrap();
    alice.checkout_branch("draft").unwrap();
    alice
        .insert_row(
            "tasks",
            "task-draft",
            BTreeMap::from([
                ("title".to_owned(), json!("Draft")),
                ("done".to_owned(), json!(false)),
            ]),
        )
        .unwrap();

    alice.create_branch("other", None).unwrap();
    alice.checkout_branch("other").unwrap();
    alice
        .insert_row(
            "tasks",
            "task-other",
            BTreeMap::from([
                ("title".to_owned(), json!("Other")),
                ("done".to_owned(), json!(false)),
            ]),
        )
        .unwrap();

    alice.checkout_branch("draft").unwrap();
    let rows = alice
        .export_table_history("tasks")
        .unwrap()
        .history
        .into_iter()
        .map(|record| record.row_id)
        .collect::<Vec<_>>();

    assert_eq!(rows, vec!["task-draft"]);
}

#[test]
fn branch_reads_main_base_with_sparse_overlay() {
    let schema = SchemaDef::new().table("tasks", |table| {
        table.text("title");
        table.bool("done");
    });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema).unwrap();

    let mut main_task = BTreeMap::new();
    main_task.insert("title".to_owned(), json!("Base title"));
    main_task.insert("done".to_owned(), json!(false));
    alice.insert_row("tasks", "task-1", main_task).unwrap();

    alice.create_branch("draft", None).unwrap();
    alice.checkout_branch("draft").unwrap();
    assert_eq!(
        alice.read_rows("tasks").unwrap()[0].values["title"],
        json!("Base title")
    );

    let mut draft_task = BTreeMap::new();
    draft_task.insert("title".to_owned(), json!("Draft title"));
    draft_task.insert("done".to_owned(), json!(false));
    alice.insert_row("tasks", "task-1", draft_task).unwrap();
    assert_eq!(
        alice.read_rows("tasks").unwrap()[0].values["title"],
        json!("Draft title")
    );

    alice.checkout_branch("main").unwrap();
    assert_eq!(
        alice.read_rows("tasks").unwrap()[0].values["title"],
        json!("Base title")
    );
}

#[test]
fn branch_base_is_pinned_to_global_epoch() {
    let schema = SchemaDef::new().table("tasks", |table| {
        table.text("title");
        table.bool("done");
    });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema).unwrap();

    let mut base_task = BTreeMap::new();
    base_task.insert("title".to_owned(), json!("Base title"));
    base_task.insert("done".to_owned(), json!(false));
    let base_tx = alice.insert_row("tasks", "task-1", base_task).unwrap();
    alice.accept_transaction_at_global(&base_tx, 1).unwrap();
    alice.create_branch("draft", Some(1)).unwrap();

    let mut main_update = BTreeMap::new();
    main_update.insert("title".to_owned(), json!("Main after branch"));
    main_update.insert("done".to_owned(), json!(false));
    let update_tx = alice.insert_row("tasks", "task-1", main_update).unwrap();
    alice.accept_transaction_at_global(&update_tx, 2).unwrap();

    alice.checkout_branch("draft").unwrap();
    assert_eq!(
        alice.read_rows("tasks").unwrap()[0].values["title"],
        json!("Base title")
    );

    let mut draft_update = BTreeMap::new();
    draft_update.insert("title".to_owned(), json!("Draft overlay"));
    draft_update.insert("done".to_owned(), json!(false));
    alice.insert_row("tasks", "task-1", draft_update).unwrap();
    assert_eq!(
        alice.read_rows("tasks").unwrap()[0].values["title"],
        json!("Draft overlay")
    );

    alice.checkout_branch("main").unwrap();
    assert_eq!(
        alice.read_rows("tasks").unwrap()[0].values["title"],
        json!("Main after branch")
    );
}

#[test]
fn branch_export_includes_pinned_main_base_rows_for_receiver_view() {
    let schema = SchemaDef::new().table("tasks", |table| {
        table.text("title");
        table.bool("done");
    });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut bob = Runtime::open_with_schema(Storage::Memory, "bob-node", "bob", schema).unwrap();

    let base_tx = alice
        .insert_row(
            "tasks",
            "task-1",
            BTreeMap::from([
                ("title".to_owned(), json!("Base title")),
                ("done".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    alice.accept_transaction_at_global(&base_tx, 1).unwrap();
    alice.create_branch("draft", Some(1)).unwrap();

    let update_tx = alice
        .insert_row(
            "tasks",
            "task-1",
            BTreeMap::from([
                ("title".to_owned(), json!("Main after branch")),
                ("done".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    alice.accept_transaction_at_global(&update_tx, 2).unwrap();

    alice.checkout_branch("draft").unwrap();
    let bundle = alice.export_table_history("tasks").unwrap();
    let synced = bundle
        .history
        .iter()
        .map(|record| {
            (
                record.branch_id.as_str(),
                record.row_id.as_str(),
                &record.values["title"],
            )
        })
        .collect::<Vec<_>>();
    assert!(synced.contains(&("main", "task-1", &json!("Base title"))));
    assert!(!synced.contains(&("main", "task-1", &json!("Main after branch"))));

    bob.apply_bundle(&bundle).unwrap();
    bob.checkout_branch("draft").unwrap();
    let rows = bob.read_rows("tasks").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values["title"], json!("Base title"));
}

#[test]
fn branch_base_snapshot_respects_deletes_and_excludes_pending_main() {
    let schema = SchemaDef::new().table("tasks", |table| {
        table.text("title");
        table.bool("done");
    });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema).unwrap();

    let mut deleted_task = BTreeMap::new();
    deleted_task.insert("title".to_owned(), json!("Will be deleted"));
    deleted_task.insert("done".to_owned(), json!(false));
    let create_tx = alice
        .insert_row("tasks", "task-deleted", deleted_task)
        .unwrap();
    alice.accept_transaction_at_global(&create_tx, 1).unwrap();
    let delete_tx = alice.delete_row("tasks", "task-deleted").unwrap();
    alice.accept_transaction_at_global(&delete_tx, 2).unwrap();

    let mut pending_task = BTreeMap::new();
    pending_task.insert("title".to_owned(), json!("Still pending"));
    pending_task.insert("done".to_owned(), json!(false));
    alice
        .insert_row("tasks", "task-pending", pending_task)
        .unwrap();

    alice.create_branch("after-delete", Some(2)).unwrap();
    alice.checkout_branch("after-delete").unwrap();

    assert!(alice.read_rows("tasks").unwrap().is_empty());
}

#[test]
fn branch_base_snapshot_applies_row_policy() {
    let schema = SchemaDef::new().table("tasks", |table| {
        table.text("title");
        table.bool("done");
        table.read_if_created_by_principal();
    });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut bob = Runtime::open_with_schema(Storage::Memory, "bob-node", "bob", schema).unwrap();

    let alice_tx = alice
        .insert_row(
            "tasks",
            "task-alice",
            BTreeMap::from([
                ("title".to_owned(), json!("Alice visible")),
                ("done".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    alice.accept_transaction_at_global(&alice_tx, 1).unwrap();

    let bob_tx = bob
        .insert_row(
            "tasks",
            "task-bob",
            BTreeMap::from([
                ("title".to_owned(), json!("Bob hidden")),
                ("done".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    bob.accept_transaction_at_global(&bob_tx, 2).unwrap();
    alice
        .apply_bundle(&bob.export_table_history("tasks").unwrap())
        .unwrap();

    alice.create_branch("draft", Some(2)).unwrap();
    alice.checkout_branch("draft").unwrap();

    let rows = alice.read_rows("tasks").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, "task-alice");
}

#[test]
fn branch_base_snapshot_ref_policy_uses_parent_at_base_epoch() {
    let schema = SchemaDef::new()
        .table("projects", |table| {
            table.text("title");
            table.read_if_created_by_principal();
        })
        .table("todos", |table| {
            table.text("title");
            table.ref_("project", "projects");
            table.read_if_ref_readable("project");
        });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut bob = Runtime::open_with_schema(Storage::Memory, "bob-node", "bob", schema).unwrap();

    let project_tx = alice
        .insert_row(
            "projects",
            "project-1",
            BTreeMap::from([("title".to_owned(), json!("Alice project"))]),
        )
        .unwrap();
    alice.accept_transaction_at_global(&project_tx, 1).unwrap();
    let todo_tx = alice
        .insert_row(
            "todos",
            "todo-1",
            BTreeMap::from([
                ("title".to_owned(), json!("Visible at branch base")),
                ("project".to_owned(), json!("project-1")),
            ]),
        )
        .unwrap();
    alice.accept_transaction_at_global(&todo_tx, 2).unwrap();
    alice.create_branch("draft", Some(2)).unwrap();

    let bob_project_tx = bob
        .insert_row(
            "projects",
            "project-1",
            BTreeMap::from([("title".to_owned(), json!("Bob takes over later"))]),
        )
        .unwrap();
    bob.accept_transaction_at_global(&bob_project_tx, 3)
        .unwrap();
    alice
        .apply_bundle(&bob.export_table_history("projects").unwrap())
        .unwrap();

    alice.checkout_branch("draft").unwrap();
    let rows = alice.read_rows("todos").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, "todo-1");
}

#[test]
fn branch_base_export_preserves_ref_policy_at_base_epoch() {
    let schema = SchemaDef::new()
        .table("projects", |table| {
            table.text("title");
            table.read_if_created_by_principal();
        })
        .table("todos", |table| {
            table.text("title");
            table.ref_("project", "projects");
            table.read_if_ref_readable("project");
        });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut bob =
        Runtime::open_with_schema(Storage::Memory, "alice-peer-node", "alice", schema.clone())
            .unwrap();
    let mut charlie =
        Runtime::open_with_schema(Storage::Memory, "charlie-node", "charlie", schema).unwrap();

    let project_tx = alice
        .insert_row(
            "projects",
            "project-1",
            BTreeMap::from([("title".to_owned(), json!("Alice project"))]),
        )
        .unwrap();
    alice.accept_transaction_at_global(&project_tx, 1).unwrap();
    let todo_tx = alice
        .insert_row(
            "todos",
            "todo-1",
            BTreeMap::from([
                ("title".to_owned(), json!("Visible at branch base")),
                ("project".to_owned(), json!("project-1")),
            ]),
        )
        .unwrap();
    alice.accept_transaction_at_global(&todo_tx, 2).unwrap();
    alice.create_branch("draft", Some(2)).unwrap();

    let charlie_project_tx = charlie
        .insert_row(
            "projects",
            "project-1",
            BTreeMap::from([("title".to_owned(), json!("Charlie later"))]),
        )
        .unwrap();
    charlie
        .accept_transaction_at_global(&charlie_project_tx, 3)
        .unwrap();
    alice
        .apply_bundle(&charlie.export_table_history("projects").unwrap())
        .unwrap();

    alice.checkout_branch("draft").unwrap();
    let bundle = alice.export_table_history("todos").unwrap();
    let synced = bundle
        .history
        .iter()
        .map(|record| (record.table.as_str(), record.row_id.as_str()))
        .collect::<Vec<_>>();
    assert!(synced.contains(&("todos", "todo-1")));
    assert!(synced.contains(&("projects", "project-1")));

    bob.apply_bundle(&bundle).unwrap();
    bob.checkout_branch("draft").unwrap();
    let rows = bob.read_rows("todos").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, "todo-1");
}

#[test]
fn branch_multi_base_conflicts_expose_multiple_candidates() {
    let schema = SchemaDef::new().table("tasks", |table| {
        table.text("title");
        table.bool("done");
    });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema).unwrap();

    alice.create_branch("left", None).unwrap();
    alice.checkout_branch("left").unwrap();
    alice
        .insert_row(
            "tasks",
            "task-1",
            BTreeMap::from([
                ("title".to_owned(), json!("Left title")),
                ("done".to_owned(), json!(false)),
            ]),
        )
        .unwrap();

    alice.create_branch("right", None).unwrap();
    alice.checkout_branch("right").unwrap();
    alice
        .insert_row(
            "tasks",
            "task-1",
            BTreeMap::from([
                ("title".to_owned(), json!("Right title")),
                ("done".to_owned(), json!(false)),
            ]),
        )
        .unwrap();

    alice
        .create_branch_from_branches("merge", &["left", "right"])
        .unwrap();
    alice.checkout_branch("merge").unwrap();

    let candidates = alice.read_row_candidates("tasks", "task-1").unwrap();
    assert_eq!(candidates.len(), 2);
    assert_eq!(candidates[0].values["title"], json!("Left title"));
    assert_eq!(candidates[1].values["title"], json!("Right title"));
}

#[test]
fn branch_source_metadata_survives_sync() {
    let schema = SchemaDef::new().table("tasks", |table| {
        table.text("title");
        table.bool("done");
    });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut bob = Runtime::open_with_schema(Storage::Memory, "bob-node", "bob", schema).unwrap();

    alice.create_branch("left", None).unwrap();
    alice.checkout_branch("left").unwrap();
    alice
        .insert_row(
            "tasks",
            "task-1",
            BTreeMap::from([
                ("title".to_owned(), json!("Left title")),
                ("done".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    alice.export_table_history("tasks").unwrap();

    alice.create_branch("right", None).unwrap();
    alice.checkout_branch("right").unwrap();
    alice
        .insert_row(
            "tasks",
            "task-1",
            BTreeMap::from([
                ("title".to_owned(), json!("Right title")),
                ("done".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    alice.export_table_history("tasks").unwrap();

    alice
        .create_branch_from_branches("merge", &["left", "right"])
        .unwrap();
    alice.checkout_branch("merge").unwrap();
    alice
        .insert_row(
            "tasks",
            "merge-marker",
            BTreeMap::from([
                ("title".to_owned(), json!("Merge marker")),
                ("done".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    let merge_bundle = alice.export_table_history("tasks").unwrap();
    let merge_record = merge_bundle
        .branches
        .iter()
        .find(|branch| branch.branch_id == "merge")
        .unwrap();
    assert_eq!(merge_record.source_branch_ids, vec!["left", "right"]);
    let bundled_rows = merge_bundle
        .history
        .iter()
        .map(|record| (record.branch_id.as_str(), record.row_id.as_str()))
        .collect::<Vec<_>>();
    assert!(bundled_rows.contains(&("left", "task-1")));
    assert!(bundled_rows.contains(&("right", "task-1")));
    assert!(bundled_rows.contains(&("merge", "merge-marker")));

    bob.apply_bundle(&merge_bundle).unwrap();
    bob.checkout_branch("merge").unwrap();

    let candidates = bob.read_row_candidates("tasks", "task-1").unwrap();
    assert_eq!(candidates.len(), 2);
    assert_eq!(candidates[0].values["title"], json!("Left title"));
    assert_eq!(candidates[1].values["title"], json!("Right title"));
}

#[test]
fn branch_conflict_candidates_survive_durable_sync_and_rejected_fate() {
    let dir = tempdir().unwrap();
    let worker_path = dir.path().join("worker.sqlite");
    let schema = support::tasks_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();

    alice.create_branch("left", None).unwrap();
    alice.checkout_branch("left").unwrap();
    let left_tx = alice
        .insert_row(
            "tasks",
            "task-1",
            BTreeMap::from([
                ("title".to_owned(), json!("Left title")),
                ("done".to_owned(), json!(false)),
            ]),
        )
        .unwrap();

    alice.create_branch("right", None).unwrap();
    alice.checkout_branch("right").unwrap();
    alice
        .insert_row(
            "tasks",
            "task-1",
            BTreeMap::from([
                ("title".to_owned(), json!("Right title")),
                ("done".to_owned(), json!(false)),
            ]),
        )
        .unwrap();

    alice
        .create_branch_from_branches("merge", &["left", "right"])
        .unwrap();
    alice.checkout_branch("merge").unwrap();
    alice
        .insert_row(
            "tasks",
            "merge-marker",
            BTreeMap::from([
                ("title".to_owned(), json!("Merge marker")),
                ("done".to_owned(), json!(false)),
            ]),
        )
        .unwrap();

    {
        let mut worker = Runtime::open_with_schema(
            Storage::File(worker_path.clone()),
            "worker",
            "alice",
            schema.clone(),
        )
        .unwrap();
        worker
            .apply_bundle(&alice.export_table_history("tasks").unwrap())
            .unwrap();
        worker.checkout_branch("merge").unwrap();
        assert_eq!(
            worker.read_row_candidates("tasks", "task-1").unwrap().len(),
            2
        );
    }

    let mut reopened =
        Runtime::open_with_schema(Storage::File(worker_path), "worker", "alice", schema).unwrap();
    reopened.checkout_branch("merge").unwrap();
    assert_eq!(
        reopened
            .read_row_candidates("tasks", "task-1")
            .unwrap()
            .len(),
        2
    );

    alice.reject_transaction(&left_tx, "policy_denied").unwrap();
    reopened
        .apply_bundle(&alice.export_table_history("tasks").unwrap())
        .unwrap();
    let candidates = reopened.read_row_candidates("tasks", "task-1").unwrap();
    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].values["title"], json!("Right title"));
}

#[test]
fn rename_lens_reads_old_storage_column_as_new_field_name() {
    let old_schema = SchemaDef::new().table("tasks", |table| {
        table.text("title");
        table.bool("done");
    });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", old_schema).unwrap();
    let mut old_task = BTreeMap::new();
    old_task.insert("title".to_owned(), json!("Old title"));
    old_task.insert("done".to_owned(), json!(false));
    alice.insert_row("tasks", "task-1", old_task).unwrap();

    let new_schema = SchemaDef::new().table("tasks", |table| {
        table.text_lens("name", "title");
        table.bool("done");
    });
    let bundle = alice.export_table_history("tasks").unwrap();
    let mut bob =
        Runtime::open_with_schema(Storage::Memory, "bob-node", "bob", new_schema).unwrap();
    bob.apply_bundle(&bundle).unwrap();

    let rows = bob.read_rows("tasks").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values["name"], json!("Old title"));
    assert!(!rows[0].values.contains_key("title"));
}

#[test]
fn rename_lens_writes_export_current_semantic_field_name() {
    let schema = SchemaDef::new().table("tasks", |table| {
        table.text_lens("name", "title");
        table.bool("done");
    });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut bob = Runtime::open_with_schema(Storage::Memory, "bob-node", "bob", schema).unwrap();

    let mut task = BTreeMap::new();
    task.insert("name".to_owned(), json!("New schema write"));
    task.insert("done".to_owned(), json!(false));
    alice.insert_row("tasks", "task-1", task).unwrap();

    let bundle = alice.export_table_history("tasks").unwrap();
    assert_eq!(bundle.history[0].values["name"], json!("New schema write"));
    assert!(!bundle.history[0].values.contains_key("title"));

    bob.apply_bundle(&bundle).unwrap();
    assert_eq!(
        bob.read_rows("tasks").unwrap()[0].values["name"],
        json!("New schema write")
    );
}

#[test]
fn renamed_ref_lens_participates_in_read_policy() {
    let old_schema = SchemaDef::new()
        .table("projects", |table| {
            table.text("title");
            table.read_if_created_by_principal();
        })
        .table("todos", |table| {
            table.text("title");
            table.ref_("project", "projects");
        });
    let new_schema = SchemaDef::new()
        .table("projects", |table| {
            table.text("title");
            table.read_if_created_by_principal();
        })
        .table("todos", |table| {
            table.text("title");
            table.ref_lens("workspace", "project", "projects");
            table.read_if_ref_readable("workspace");
        });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", old_schema.clone())
            .unwrap();
    let mut bob =
        Runtime::open_with_schema(Storage::Memory, "bob-node", "bob", old_schema).unwrap();
    let mut reader =
        Runtime::open_with_schema(Storage::Memory, "alice-reader", "alice", new_schema).unwrap();

    alice
        .insert_row(
            "projects",
            "project-alice",
            BTreeMap::from([("title".to_owned(), json!("Alice project"))]),
        )
        .unwrap();
    alice
        .insert_row(
            "todos",
            "todo-visible",
            BTreeMap::from([
                ("title".to_owned(), json!("Visible")),
                ("project".to_owned(), json!("project-alice")),
            ]),
        )
        .unwrap();

    bob.insert_row(
        "projects",
        "project-bob",
        BTreeMap::from([("title".to_owned(), json!("Bob project"))]),
    )
    .unwrap();
    bob.insert_row(
        "todos",
        "todo-hidden",
        BTreeMap::from([
            ("title".to_owned(), json!("Hidden")),
            ("project".to_owned(), json!("project-bob")),
        ]),
    )
    .unwrap();

    reader
        .apply_bundle(&alice.export_table_history("projects").unwrap())
        .unwrap();
    reader
        .apply_bundle(&alice.export_table_history("todos").unwrap())
        .unwrap();
    reader
        .apply_bundle(&bob.export_table_history("projects").unwrap())
        .unwrap();
    reader
        .apply_bundle(&bob.export_table_history("todos").unwrap())
        .unwrap();

    let rows = reader.read_rows("todos").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, "todo-visible");
    assert_eq!(rows[0].values["workspace"], json!("project-alice"));
    assert!(!rows[0].values.contains_key("project"));
}

#[test]
fn branch_sync_preserves_branch_provenance() {
    let schema = SchemaDef::new().table("tasks", |table| {
        table.text("title");
        table.bool("done");
    });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut bob = Runtime::open_with_schema(Storage::Memory, "bob-node", "bob", schema).unwrap();

    alice.create_branch("draft", None).unwrap();
    alice.checkout_branch("draft").unwrap();
    let mut task = BTreeMap::new();
    task.insert("title".to_owned(), json!("Draft sync"));
    task.insert("done".to_owned(), json!(false));
    alice.insert_row("tasks", "task-draft", task).unwrap();

    let bundle = alice.export_table_history("tasks").unwrap();
    assert_eq!(bundle.branches[0].branch_id, "draft");
    assert_eq!(bundle.history[0].branch_id, "draft");
    bob.apply_bundle(&bundle).unwrap();

    assert!(bob.read_rows("tasks").unwrap().is_empty());
    bob.checkout_branch("draft").unwrap();
    assert_eq!(bob.read_rows("tasks").unwrap()[0].id, "task-draft");
}

#[test]
fn durable_reopen_preserves_branch_sync_and_dedupes_replay() {
    let schema = SchemaDef::new().table("tasks", |table| {
        table.text("title");
        table.bool("done");
    });
    let dir = tempdir().unwrap();
    let path = dir.path().join("worker.sqlite");
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();

    alice.create_branch("draft", None).unwrap();
    alice.checkout_branch("draft").unwrap();
    let mut task = BTreeMap::new();
    task.insert("title".to_owned(), json!("Durable draft"));
    task.insert("done".to_owned(), json!(false));
    alice.insert_row("tasks", "task-draft", task).unwrap();
    let bundle = alice.export_table_history("tasks").unwrap();

    {
        let mut worker = Runtime::open_with_schema(
            Storage::File(path.clone()),
            "worker-node",
            "alice",
            schema.clone(),
        )
        .unwrap();
        worker.apply_bundle(&bundle).unwrap();
        worker.apply_bundle(&bundle).unwrap();
        worker.checkout_branch("draft").unwrap();
        assert_eq!(worker.read_rows("tasks").unwrap().len(), 1);
    }

    let mut reopened =
        Runtime::open_with_schema(Storage::File(path), "worker-node", "alice", schema).unwrap();
    assert!(reopened.read_rows("tasks").unwrap().is_empty());
    reopened.checkout_branch("draft").unwrap();
    assert_eq!(reopened.read_rows("tasks").unwrap()[0].id, "task-draft");
    assert_eq!(reopened.storage_stats().unwrap().history_rows, 1);
}
