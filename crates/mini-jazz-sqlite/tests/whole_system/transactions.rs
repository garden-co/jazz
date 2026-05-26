use super::*;

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
fn generic_transaction_can_seal_updates_atomically() {
    let schema = support::notes_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema).unwrap();

    alice
        .insert_row(
            "notes",
            "note-1",
            BTreeMap::from([
                ("body".to_owned(), json!("Original")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    let tx = alice
        .transaction()
        .update_row(
            "notes",
            "note-1",
            BTreeMap::from([
                ("body".to_owned(), json!("Updated in tx")),
                ("pinned".to_owned(), json!(true)),
            ]),
        )
        .insert_row(
            "notes",
            "note-2",
            BTreeMap::from([
                ("body".to_owned(), json!("Created in tx")),
                ("pinned".to_owned(), json!(false)),
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
    assert!(rows
        .iter()
        .any(|row| row.values["body"] == json!("Updated in tx")));
}

#[test]
fn generic_update_records_previous_row_read_set() {
    let schema = support::notes_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema).unwrap();

    alice
        .insert_row(
            "notes",
            "note-1",
            BTreeMap::from([
                ("body".to_owned(), json!("Before")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    let tx = alice
        .update_row(
            "notes",
            "note-1",
            BTreeMap::from([
                ("body".to_owned(), json!("After")),
                ("pinned".to_owned(), json!(true)),
            ]),
        )
        .unwrap();

    assert_eq!(
        alice.transaction_previous_read_rows(&tx).unwrap(),
        vec![("notes".to_owned(), "note-1".to_owned())]
    );
}

#[test]
fn generic_transaction_can_seal_delete_with_other_mutations() {
    let schema = support::notes_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema).unwrap();

    alice
        .insert_row(
            "notes",
            "note-delete",
            BTreeMap::from([
                ("body".to_owned(), json!("Delete me")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .unwrap();

    let tx = alice
        .transaction()
        .delete_row("notes", "note-delete")
        .insert_row(
            "notes",
            "note-keep",
            BTreeMap::from([
                ("body".to_owned(), json!("Keep me")),
                ("pinned".to_owned(), json!(true)),
            ]),
        )
        .commit()
        .unwrap();

    let rows = alice.read_rows("notes").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, "note-keep");
    assert_eq!(
        alice.transaction_write_rows(&tx).unwrap(),
        vec![
            ("notes".to_owned(), "note-delete".to_owned()),
            ("notes".to_owned(), "note-keep".to_owned())
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
fn generic_transaction_delete_records_previous_row_read_set() {
    let schema = support::notes_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema).unwrap();

    alice
        .insert_row(
            "notes",
            "note-delete",
            BTreeMap::from([
                ("body".to_owned(), json!("Before delete")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    let tx = alice
        .transaction()
        .delete_row("notes", "note-delete")
        .commit()
        .unwrap();

    assert_eq!(
        alice.transaction_previous_read_rows(&tx).unwrap(),
        vec![("notes".to_owned(), "note-delete".to_owned())]
    );
}

#[test]
fn exclusive_transaction_rejects_same_row_conflict() {
    let schema = support::notes_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema).unwrap();

    alice
        .transaction()
        .exclusive_at_global(1)
        .insert_row(
            "notes",
            "note-1",
            BTreeMap::from([
                ("body".to_owned(), json!("First exclusive")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .commit()
        .unwrap();

    let err = alice
        .transaction()
        .exclusive_at_global(2)
        .update_row(
            "notes",
            "note-1",
            BTreeMap::from([
                ("body".to_owned(), json!("Second exclusive")),
                ("pinned".to_owned(), json!(true)),
            ]),
        )
        .commit()
        .unwrap_err();

    assert!(err.to_string().contains("exclusive conflict"));
    assert_eq!(
        alice.read_rows("notes").unwrap()[0].values["body"],
        json!("First exclusive")
    );
}

#[test]
fn generic_transaction_delete_shadows_pinned_base_row() {
    let schema = support::tasks_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema).unwrap();

    let tx = alice
        .insert_row(
            "tasks",
            "task-1",
            BTreeMap::from([
                ("title".to_owned(), json!("Base task")),
                ("done".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    alice.accept_transaction_at_global(&tx, 1).unwrap();

    alice.create_branch("draft", Some(1)).unwrap();
    alice.checkout_branch("draft").unwrap();
    alice
        .transaction()
        .delete_row("tasks", "task-1")
        .commit()
        .unwrap();

    assert!(alice.read_rows("tasks").unwrap().is_empty());

    alice.checkout_branch("main").unwrap();
    assert_eq!(alice.read_rows("tasks").unwrap().len(), 1);
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
