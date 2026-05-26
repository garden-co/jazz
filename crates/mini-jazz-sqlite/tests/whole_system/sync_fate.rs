use super::*;

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
fn query_scope_excludes_rows_outside_current_result_set() {
    let mut alice = Runtime::open(Storage::Memory, "alice-node", "alice").unwrap();
    let mut bob = Runtime::open(Storage::Memory, "bob-node", "bob").unwrap();

    alice
        .create_project("project-1", "Visible project")
        .unwrap();
    alice
        .create_todo("todo-open", "In query scope", false, "project-1")
        .unwrap();
    alice
        .create_todo("todo-closed", "Outside query scope", true, "project-1")
        .unwrap();

    let bundle = alice.export_query_scope_open_todos().unwrap();
    let synced_todos = bundle
        .history
        .iter()
        .filter(|record| record.table == "todos")
        .map(|record| record.row_id.as_str())
        .collect::<Vec<_>>();
    assert!(synced_todos.contains(&"todo-open"));
    assert!(!synced_todos.contains(&"todo-closed"));

    bob.apply_bundle(&bundle).unwrap();
    assert_eq!(bob.open_todos().unwrap(), alice.open_todos().unwrap());
    assert!(bob.physical_row_num_for("todo-open").is_ok());
    assert!(bob.physical_row_num_for("todo-closed").is_err());
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
fn out_of_order_global_epochs_do_not_regress_current_projection() {
    let schema = support::notes_schema();
    let mut authority =
        Runtime::open_with_schema(Storage::Memory, "authority", "alice", schema.clone()).unwrap();
    let mut peer = Runtime::open_with_schema(Storage::Memory, "peer", "alice", schema).unwrap();

    let first_tx = authority
        .insert_row(
            "notes",
            "note-1",
            BTreeMap::from([
                ("body".to_owned(), json!("epoch 10")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    authority
        .accept_transaction_at_global(&first_tx, 10)
        .unwrap();
    let first_bundle = authority.export_table_history("notes").unwrap();

    let second_tx = authority
        .update_row(
            "notes",
            "note-1",
            BTreeMap::from([
                ("body".to_owned(), json!("epoch 20")),
                ("pinned".to_owned(), json!(true)),
            ]),
        )
        .unwrap();
    authority
        .accept_transaction_at_global(&second_tx, 20)
        .unwrap();
    let second_bundle = authority.export_table_history("notes").unwrap();

    peer.apply_bundle(&second_bundle).unwrap();
    peer.apply_bundle(&first_bundle).unwrap();

    assert_eq!(
        peer.read_rows("notes").unwrap()[0].values["body"],
        json!("epoch 20")
    );

    peer.clear_current_projection_for_test().unwrap();
    peer.rebuild_current_projection().unwrap();
    assert_eq!(
        peer.read_rows("notes").unwrap()[0].values["body"],
        json!("epoch 20")
    );
}

#[test]
fn rebuild_uses_global_epoch_order_not_local_tx_order() {
    let schema = support::notes_schema();
    let mut authority =
        Runtime::open_with_schema(Storage::Memory, "authority", "alice", schema.clone()).unwrap();
    let mut peer = Runtime::open_with_schema(Storage::Memory, "peer", "alice", schema).unwrap();

    let first_tx = authority
        .insert_row(
            "notes",
            "note-1",
            BTreeMap::from([
                ("body".to_owned(), json!("epoch 20")),
                ("pinned".to_owned(), json!(true)),
            ]),
        )
        .unwrap();
    authority
        .accept_transaction_at_global(&first_tx, 20)
        .unwrap();

    let second_tx = authority
        .update_row(
            "notes",
            "note-1",
            BTreeMap::from([
                ("body".to_owned(), json!("epoch 10")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    authority
        .accept_transaction_at_global(&second_tx, 10)
        .unwrap();

    peer.apply_bundle(&authority.export_table_history("notes").unwrap())
        .unwrap();
    assert_eq!(
        peer.read_rows("notes").unwrap()[0].values["body"],
        json!("epoch 20")
    );

    peer.clear_current_projection_for_test().unwrap();
    peer.rebuild_current_projection().unwrap();
    assert_eq!(
        peer.read_rows("notes").unwrap()[0].values["body"],
        json!("epoch 20")
    );
}

#[test]
fn direct_global_acceptance_repairs_current_projection_order() {
    let schema = support::notes_schema();
    let mut authority =
        Runtime::open_with_schema(Storage::Memory, "authority", "alice", schema).unwrap();

    let first_tx = authority
        .insert_row(
            "notes",
            "note-1",
            BTreeMap::from([
                ("body".to_owned(), json!("epoch 20")),
                ("pinned".to_owned(), json!(true)),
            ]),
        )
        .unwrap();
    authority
        .accept_transaction_at_global(&first_tx, 20)
        .unwrap();

    let second_tx = authority
        .update_row(
            "notes",
            "note-1",
            BTreeMap::from([
                ("body".to_owned(), json!("epoch 10")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    authority
        .accept_transaction_at_global(&second_tx, 10)
        .unwrap();

    assert_eq!(
        authority.read_rows("notes").unwrap()[0].values["body"],
        json!("epoch 20")
    );
}

#[test]
fn remote_pending_update_does_not_override_global_current_on_peer() {
    let schema = support::notes_schema();
    let mut authority =
        Runtime::open_with_schema(Storage::Memory, "authority", "alice", schema.clone()).unwrap();
    let mut writer =
        Runtime::open_with_schema(Storage::Memory, "writer", "alice", schema.clone()).unwrap();
    let mut peer = Runtime::open_with_schema(Storage::Memory, "peer", "alice", schema).unwrap();

    let base_tx = authority
        .insert_row(
            "notes",
            "note-1",
            BTreeMap::from([
                ("body".to_owned(), json!("global")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    authority.accept_transaction_at_global(&base_tx, 1).unwrap();
    let base_bundle = authority.export_table_history("notes").unwrap();
    writer.apply_bundle(&base_bundle).unwrap();
    peer.apply_bundle(&base_bundle).unwrap();

    writer
        .update_row(
            "notes",
            "note-1",
            BTreeMap::from([
                ("body".to_owned(), json!("remote pending")),
                ("pinned".to_owned(), json!(true)),
            ]),
        )
        .unwrap();
    assert_eq!(
        writer.read_rows("notes").unwrap()[0].values["body"],
        json!("remote pending")
    );

    peer.apply_bundle(&writer.export_table_history("notes").unwrap())
        .unwrap();
    assert_eq!(
        peer.read_rows("notes").unwrap()[0].values["body"],
        json!("global")
    );

    peer.clear_current_projection_for_test().unwrap();
    peer.rebuild_current_projection().unwrap();
    assert_eq!(
        peer.read_rows("notes").unwrap()[0].values["body"],
        json!("global")
    );
}

#[test]
fn accepted_remote_pending_update_repairs_peer_current_projection() {
    let schema = support::notes_schema();
    let mut authority =
        Runtime::open_with_schema(Storage::Memory, "authority", "alice", schema.clone()).unwrap();
    let mut writer =
        Runtime::open_with_schema(Storage::Memory, "writer", "alice", schema.clone()).unwrap();
    let mut peer = Runtime::open_with_schema(Storage::Memory, "peer", "alice", schema).unwrap();

    let base_tx = authority
        .insert_row(
            "notes",
            "note-1",
            BTreeMap::from([
                ("body".to_owned(), json!("global")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    authority.accept_transaction_at_global(&base_tx, 1).unwrap();
    let base_bundle = authority.export_table_history("notes").unwrap();
    writer.apply_bundle(&base_bundle).unwrap();
    peer.apply_bundle(&base_bundle).unwrap();

    let pending_tx = writer
        .update_row(
            "notes",
            "note-1",
            BTreeMap::from([
                ("body".to_owned(), json!("accepted later")),
                ("pinned".to_owned(), json!(true)),
            ]),
        )
        .unwrap();
    let pending_bundle = writer.export_table_history("notes").unwrap();
    authority.apply_bundle(&pending_bundle).unwrap();
    peer.apply_bundle(&pending_bundle).unwrap();
    assert_eq!(
        peer.read_rows("notes").unwrap()[0].values["body"],
        json!("global")
    );

    authority
        .accept_transaction_at_global(&pending_tx, 2)
        .unwrap();
    peer.apply_bundle(&authority.export_table_history("notes").unwrap())
        .unwrap();
    assert_eq!(
        peer.read_rows("notes").unwrap()[0].values["body"],
        json!("accepted later")
    );
}

#[test]
fn accepted_bundle_does_not_resurrect_rejected_fate() {
    let mut alice = Runtime::open(Storage::Memory, "alice-node", "alice").unwrap();
    let mut rejected_peer = Runtime::open(Storage::Memory, "rejected-peer", "alice").unwrap();
    let mut accepted_peer = Runtime::open(Storage::Memory, "accepted-peer", "alice").unwrap();

    alice.create_project("project-1", "Spec work").unwrap();
    let tx = alice
        .create_todo("todo-1", "Rejected should win", false, "project-1")
        .unwrap();
    let pending = alice.export_table_history("todos").unwrap();
    rejected_peer.apply_bundle(&pending).unwrap();
    accepted_peer.apply_bundle(&pending).unwrap();

    rejected_peer
        .reject_transaction(&tx, "policy_denied")
        .unwrap();
    alice.accept_transaction_at_global(&tx, 7).unwrap();
    accepted_peer
        .apply_bundle(&alice.export_table_history("todos").unwrap())
        .unwrap();

    rejected_peer
        .apply_bundle(&accepted_peer.export_table_history("todos").unwrap())
        .unwrap();

    assert!(rejected_peer.open_todos().unwrap().is_empty());
    assert_eq!(
        rejected_peer.transaction_info(&tx).unwrap().rejection_code,
        Some("policy_denied".to_owned())
    );
    assert_eq!(
        rejected_peer.transaction_info(&tx).unwrap().global_epoch,
        Some(7)
    );
}

#[test]
fn direct_accept_after_reject_preserves_rejected_outcome_with_global_metadata() {
    let mut alice = Runtime::open(Storage::Memory, "alice-node", "alice").unwrap();

    alice.create_project("project-1", "Spec work").unwrap();
    let tx = alice
        .create_todo("todo-1", "Reject then accept metadata", false, "project-1")
        .unwrap();

    alice.reject_transaction(&tx, "policy_denied").unwrap();
    alice.accept_transaction_at_global(&tx, 7).unwrap();

    assert!(alice.open_todos().unwrap().is_empty());
    assert_eq!(alice.storage_stats().unwrap().rejected_transactions, 1);
    let info = alice.transaction_info(&tx).unwrap();
    assert_eq!(info.global_epoch, Some(7));
    assert_eq!(info.rejection_code, Some("policy_denied".to_owned()));
}

#[test]
fn direct_reject_after_accept_removes_current_but_preserves_global_metadata() {
    let mut alice = Runtime::open(Storage::Memory, "alice-node", "alice").unwrap();

    alice.create_project("project-1", "Spec work").unwrap();
    let tx = alice
        .create_todo("todo-1", "Accept then reject", false, "project-1")
        .unwrap();

    alice.accept_transaction_at_global(&tx, 7).unwrap();
    alice.reject_transaction(&tx, "policy_denied").unwrap();

    assert!(alice.open_todos().unwrap().is_empty());
    assert_eq!(alice.storage_stats().unwrap().rejected_transactions, 1);
    let info = alice.transaction_info(&tx).unwrap();
    assert_eq!(info.global_epoch, Some(7));
    assert_eq!(info.rejection_code, Some("policy_denied".to_owned()));
}
