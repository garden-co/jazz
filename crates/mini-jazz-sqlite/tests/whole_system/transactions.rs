use super::*;

#[test]
fn explicit_transaction_seals_multiple_mutations_atomically() {
    let mut alice = support::open_todo_app(Storage::Memory, "alice-node", "alice").unwrap();

    let tx = alice
        .transaction()
        .insert_row(
            "projects",
            "project-1",
            BTreeMap::from([("title".to_owned(), json!("Atomic project"))]),
        )
        .insert_row(
            "todos",
            "todo-1",
            BTreeMap::from([
                ("title".to_owned(), json!("First todo")),
                ("done".to_owned(), json!(false)),
                ("project".to_owned(), json!("project-1")),
            ]),
        )
        .insert_row(
            "todos",
            "todo-2",
            BTreeMap::from([
                ("title".to_owned(), json!("Second todo")),
                ("done".to_owned(), json!(false)),
                ("project".to_owned(), json!("project-1")),
            ]),
        )
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
fn transaction_reads_are_fixed_to_start_snapshot() {
    let schema = support::notes_schema();
    let harness = support::Harness::new();
    let path = harness.path("tx-isolation.sqlite");
    let mut alice = Runtime::open_with_schema(
        Storage::File(path.clone()),
        "alice-node",
        "alice",
        schema.clone(),
    )
    .unwrap();

    alice
        .insert_row(
            "notes",
            "note-before",
            BTreeMap::from([
                ("body".to_owned(), json!("before")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    let tx = alice.transaction();
    let mut bob =
        Runtime::open_with_schema(Storage::File(path), "bob-node", "bob", schema).unwrap();
    bob.insert_row(
        "notes",
        "note-after",
        BTreeMap::from([
            ("body".to_owned(), json!("after")),
            ("pinned".to_owned(), json!(false)),
        ]),
    )
    .unwrap();

    let rows = tx.read_rows("notes").unwrap();
    assert_eq!(
        rows.iter().map(|row| row.id.as_str()).collect::<Vec<_>>(),
        vec!["note-before"]
    );
}

#[test]
fn transaction_reads_include_own_staged_writes() {
    let schema = support::notes_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema).unwrap();
    alice
        .insert_row(
            "notes",
            "note-existing",
            BTreeMap::from([
                ("body".to_owned(), json!("old")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .unwrap();

    let tx = alice
        .transaction()
        .update_row(
            "notes",
            "note-existing",
            BTreeMap::from([("body".to_owned(), json!("new"))]),
        )
        .insert_row(
            "notes",
            "note-staged",
            BTreeMap::from([
                ("body".to_owned(), json!("staged")),
                ("pinned".to_owned(), json!(true)),
            ]),
        )
        .delete_row("notes", "note-existing");

    let rows = tx.read_rows("notes").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, "note-staged");
    assert_eq!(rows[0].values["body"], json!("staged"));
}

#[test]
fn transactions_do_not_see_each_others_staged_writes() {
    let schema = support::notes_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut bob = Runtime::open_with_schema(Storage::Memory, "bob-node", "bob", schema).unwrap();

    let alice_tx = alice.transaction().insert_row(
        "notes",
        "note-alice",
        BTreeMap::from([
            ("body".to_owned(), json!("alice staged")),
            ("pinned".to_owned(), json!(false)),
        ]),
    );
    let bob_tx = bob.transaction();

    assert_eq!(alice_tx.read_rows("notes").unwrap().len(), 1);
    assert!(bob_tx.read_rows("notes").unwrap().is_empty());
}

#[test]
fn transaction_reads_preserve_branch_conflict_candidates() {
    let schema = support::tasks_schema();
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

    let rows = alice.transaction().read_rows("tasks").unwrap();
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().all(|row| row.id == "task-1"));
    let titles = rows
        .iter()
        .map(|row| row.values["title"].clone())
        .collect::<Vec<_>>();
    assert!(titles.contains(&json!("Left title")));
    assert!(titles.contains(&json!("Right title")));
}

#[test]
fn transaction_update_rejects_ambiguous_branch_conflict() {
    let schema = support::tasks_schema();
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

    let err = alice
        .transaction()
        .update_row(
            "tasks",
            "task-1",
            BTreeMap::from([("title".to_owned(), json!("Implicit resolution"))]),
        )
        .commit()
        .unwrap_err();
    assert!(err.to_string().contains("ambiguous branch row"));
}

#[test]
fn transaction_patch_updates_are_applied_to_start_snapshot() {
    let schema = support::notes_schema();
    let harness = support::Harness::new();
    let path = harness.path("tx-start-write.sqlite");
    let mut alice = Runtime::open_with_schema(
        Storage::File(path.clone()),
        "alice-node",
        "alice",
        schema.clone(),
    )
    .unwrap();
    alice
        .insert_row(
            "notes",
            "note-1",
            BTreeMap::from([
                ("body".to_owned(), json!("before")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .unwrap();

    let tx = alice.transaction().update_row(
        "notes",
        "note-1",
        BTreeMap::from([("body".to_owned(), json!("from tx"))]),
    );
    let mut bob =
        Runtime::open_with_schema(Storage::File(path), "bob-node", "bob", schema).unwrap();
    bob.update_row(
        "notes",
        "note-1",
        BTreeMap::from([("pinned".to_owned(), json!(true))]),
    )
    .unwrap();

    tx.commit().unwrap();

    let rows = bob.read_rows("notes").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values["body"], json!("from tx"));
    assert_eq!(rows[0].values["pinned"], json!(false));
}

#[test]
fn rejecting_multi_row_transaction_hides_all_written_rows_but_keeps_history() {
    let mut alice = support::open_todo_app(Storage::Memory, "alice-node", "alice").unwrap();

    let tx = alice
        .transaction()
        .insert_row(
            "projects",
            "project-1",
            BTreeMap::from([("title".to_owned(), json!("Atomic project"))]),
        )
        .insert_row(
            "todos",
            "todo-1",
            BTreeMap::from([
                ("title".to_owned(), json!("First todo")),
                ("done".to_owned(), json!(false)),
                ("project".to_owned(), json!("project-1")),
            ]),
        )
        .insert_row(
            "todos",
            "todo-2",
            BTreeMap::from([
                ("title".to_owned(), json!("Second todo")),
                ("done".to_owned(), json!(false)),
                ("project".to_owned(), json!("project-1")),
            ]),
        )
        .commit()
        .unwrap();
    assert_eq!(alice.open_todos().unwrap().len(), 2);

    alice.reject_transaction(&tx, "policy_denied").unwrap();

    assert!(alice.open_todos().unwrap().is_empty());
    assert_eq!(
        alice.transaction_write_rows(&tx).unwrap(),
        vec![
            ("projects".to_owned(), "project-1".to_owned()),
            ("todos".to_owned(), "todo-1".to_owned()),
            ("todos".to_owned(), "todo-2".to_owned())
        ]
    );
    let stats = alice.storage_stats().unwrap();
    assert_eq!(stats.history_rows, 3);
    assert_eq!(stats.current_rows, 0);

    alice.clear_current_projection_for_test().unwrap();
    alice.rebuild_current_projection().unwrap();

    assert!(alice.open_todos().unwrap().is_empty());
    assert_eq!(alice.storage_stats().unwrap().history_rows, 3);
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
    assert_eq!(
        alice.transaction_observed_read_rows(&tx).unwrap(),
        vec![(
            "notes".to_owned(),
            "note-1".to_owned(),
            Some("tx-alice-node-1".to_owned())
        )]
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
fn exclusive_forwarding_export_marks_only_selected_transaction() {
    let schema = support::notes_schema();
    let mut edge =
        Runtime::open_trusted_with_session_user(Storage::Memory, "edge", "service", schema)
            .unwrap();

    let first_tx = edge
        .insert_row(
            "notes",
            "note-1",
            BTreeMap::from([
                ("body".to_owned(), json!("Forward this one")),
                ("pinned".to_owned(), json!(true)),
            ]),
        )
        .unwrap();
    let second_tx = edge
        .insert_row(
            "notes",
            "note-2",
            BTreeMap::from([
                ("body".to_owned(), json!("Leave as mergeable")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .unwrap();

    let bundle = edge
        .export_exclusive_transaction_forwarding("notes", &first_tx, "alice")
        .unwrap();
    let forwarded = bundle
        .txs
        .iter()
        .find(|record| record.tx_id == first_tx)
        .unwrap();
    assert_eq!(forwarded.conflict_mode, 2);
    assert_eq!(forwarded.outcome, 1);
    assert_eq!(forwarded.global_epoch, None);
    assert_eq!(forwarded.receipt_tiers, Vec::<i64>::new());
    assert_eq!(forwarded.auth_user.as_deref(), Some("alice"));

    let untouched = bundle
        .txs
        .iter()
        .find(|record| record.tx_id == second_tx)
        .unwrap();
    assert_eq!(untouched.conflict_mode, 1);
    assert_eq!(untouched.auth_user, None);
}

#[test]
fn authority_acceptance_enriches_existing_transaction() {
    let mut alice = support::open_todo_app(Storage::Memory, "alice-node", "alice").unwrap();

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
fn exclusive_transaction_conflicts_are_row_based_not_column_based() {
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
            BTreeMap::from([("pinned".to_owned(), json!(true))]),
        )
        .commit()
        .unwrap_err();

    assert!(err.to_string().contains("exclusive conflict"));
    let rows = alice.read_rows("notes").unwrap();
    assert_eq!(rows[0].values["body"], json!("First exclusive"));
    assert_eq!(rows[0].values["pinned"], json!(false));
}

#[test]
fn untrusted_exclusive_transaction_rejects_stale_policy_read_set() {
    let schema = SchemaDef::new()
        .table("projects", |table| {
            table.text("title");
            table.read_if_created_by_user();
        })
        .table("todos", |table| {
            table.text("title");
            table.bool("done");
            table.ref_("project", "projects");
            table.write_if_ref_readable("project");
        });
    let mut authority = Runtime::open_trusted_with_session_user(
        Storage::Memory,
        "authority",
        "alice",
        schema.clone(),
    )
    .unwrap();
    let mut writer = Runtime::open_with_schema(Storage::Memory, "writer", "alice", schema).unwrap();

    let project_tx = authority
        .insert_row(
            "projects",
            "project-1",
            BTreeMap::from([("title".to_owned(), json!("Version 1"))]),
        )
        .unwrap();
    authority
        .accept_transaction_at_global(&project_tx, 1)
        .unwrap();
    writer
        .apply_bundle(&authority.export_table_history("projects").unwrap())
        .unwrap();

    let todo_tx = writer
        .transaction()
        .insert_row(
            "todos",
            "todo-1",
            BTreeMap::from([
                ("title".to_owned(), json!("Stale exclusive")),
                ("done".to_owned(), json!(false)),
                ("project".to_owned(), json!("project-1")),
            ]),
        )
        .commit()
        .unwrap();
    let mut stale_bundle = writer.export_table_history("todos").unwrap();
    let read = stale_bundle
        .reads
        .iter()
        .find(|read| read.tx_id == todo_tx && read.table == "projects")
        .unwrap();
    assert_eq!(read.observed_tx_id.as_deref(), Some(project_tx.as_str()));
    stale_bundle
        .txs
        .iter_mut()
        .find(|tx| tx.tx_id == todo_tx)
        .unwrap()
        .conflict_mode = 2;

    authority
        .update_row(
            "projects",
            "project-1",
            BTreeMap::from([("title".to_owned(), json!("Version 2"))]),
        )
        .unwrap();

    authority.apply_untrusted_bundle(&stale_bundle).unwrap();

    let info = authority.transaction_info(&todo_tx).unwrap();
    assert_eq!(info.conflict_mode, "exclusive");
    assert_eq!(info.rejection_code, Some("stale_read_set".to_owned()));
    assert!(authority.read_rows("todos").unwrap().is_empty());
}

#[test]
fn untrusted_exclusive_transaction_rejects_stale_row_update_read_set() {
    let schema = support::notes_schema();
    let mut authority = Runtime::open_trusted_with_session_user(
        Storage::Memory,
        "authority",
        "alice",
        schema.clone(),
    )
    .unwrap();
    let mut writer = Runtime::open_with_schema(Storage::Memory, "writer", "alice", schema).unwrap();

    let original_tx = authority
        .insert_row(
            "notes",
            "note-1",
            BTreeMap::from([
                ("body".to_owned(), json!("Version 1")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    authority
        .accept_transaction_at_global(&original_tx, 1)
        .unwrap();
    writer
        .apply_bundle(&authority.export_table_history("notes").unwrap())
        .unwrap();

    let update_tx = writer
        .update_row(
            "notes",
            "note-1",
            BTreeMap::from([("body".to_owned(), json!("Writer stale update"))]),
        )
        .unwrap();
    let mut stale_bundle = writer.export_table_history("notes").unwrap();
    let read = stale_bundle
        .reads
        .iter()
        .find(|read| read.tx_id == update_tx && read.table == "notes" && read.row_id == "note-1")
        .expect("update should record previous row read");
    assert_eq!(read.observed_tx_id.as_deref(), Some(original_tx.as_str()));
    stale_bundle
        .txs
        .iter_mut()
        .find(|tx| tx.tx_id == update_tx)
        .unwrap()
        .conflict_mode = 2;

    authority
        .update_row(
            "notes",
            "note-1",
            BTreeMap::from([("body".to_owned(), json!("Version 2"))]),
        )
        .unwrap();

    authority.apply_untrusted_bundle(&stale_bundle).unwrap();

    let info = authority.transaction_info(&update_tx).unwrap();
    assert_eq!(info.conflict_mode, "exclusive");
    assert_eq!(info.rejection_code, Some("stale_read_set".to_owned()));
    let rows = authority.read_rows("notes").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values["body"], json!("Version 2"));
}

#[test]
fn untrusted_exclusive_transaction_rejects_stale_absent_row_read_set() {
    let schema = support::notes_schema();
    let mut authority = Runtime::open_trusted_with_session_user(
        Storage::Memory,
        "authority",
        "alice",
        schema.clone(),
    )
    .unwrap();
    let mut writer = Runtime::open_with_schema(Storage::Memory, "writer", "alice", schema).unwrap();

    let note_tx = writer
        .transaction()
        .insert_row(
            "notes",
            "note-1",
            BTreeMap::from([
                ("body".to_owned(), json!("I saw this id as absent")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .commit()
        .unwrap();
    let mut bundle = writer.export_table_history("notes").unwrap();
    let absent_read = bundle
        .reads
        .iter()
        .find(|read| read.tx_id == note_tx && read.table == "notes" && read.row_id == "note-1")
        .expect("insert should record an absent row read");
    assert_eq!(absent_read.reason, 3);
    assert_eq!(absent_read.observed_tx_id, None);
    bundle
        .txs
        .iter_mut()
        .find(|tx| tx.tx_id == note_tx)
        .unwrap()
        .conflict_mode = 2;

    authority
        .insert_row(
            "notes",
            "note-1",
            BTreeMap::from([
                ("body".to_owned(), json!("Authority got here first")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .unwrap();

    authority.apply_untrusted_bundle(&bundle).unwrap();

    let info = authority.transaction_info(&note_tx).unwrap();
    assert_eq!(info.conflict_mode, "exclusive");
    assert_eq!(info.rejection_code, Some("stale_read_set".to_owned()));
    let rows = authority.read_rows("notes").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values["body"], json!("Authority got here first"));
}

#[test]
fn untrusted_exclusive_transaction_rejects_stale_absent_row_from_new_source_branch() {
    let schema = support::notes_schema();
    let mut authority = Runtime::open_trusted_with_session_user(
        Storage::Memory,
        "authority",
        "alice",
        schema.clone(),
    )
    .unwrap();
    let mut writer = Runtime::open_with_schema(Storage::Memory, "writer", "alice", schema).unwrap();

    writer.create_branch("merge", None).unwrap();
    writer.checkout_branch("merge").unwrap();
    let note_tx = writer
        .transaction()
        .insert_row(
            "notes",
            "note-1",
            BTreeMap::from([
                ("body".to_owned(), json!("Writer thought merge was empty")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .commit()
        .unwrap();
    let mut bundle = writer.export_table_history("notes").unwrap();
    bundle
        .txs
        .iter_mut()
        .find(|tx| tx.tx_id == note_tx)
        .unwrap()
        .conflict_mode = 2;

    authority.create_branch("left", None).unwrap();
    authority.checkout_branch("left").unwrap();
    authority
        .insert_row(
            "notes",
            "note-1",
            BTreeMap::from([
                ("body".to_owned(), json!("Source got here first")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    authority
        .create_branch_from_branches("merge", &["left"])
        .unwrap();

    authority.apply_untrusted_bundle(&bundle).unwrap();

    let info = authority.transaction_info(&note_tx).unwrap();
    assert_eq!(info.conflict_mode, "exclusive");
    assert_eq!(info.rejection_code, Some("stale_read_set".to_owned()));
    authority.checkout_branch("merge").unwrap();
    let rows = authority.read_rows("notes").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values["body"], json!("Source got here first"));
}

#[test]
fn untrusted_exclusive_transaction_rejects_stale_source_branch_row_read_set() {
    let schema = support::notes_schema();
    let mut authority = Runtime::open_trusted_with_session_user(
        Storage::Memory,
        "authority",
        "alice",
        schema.clone(),
    )
    .unwrap();
    let mut writer = Runtime::open_with_schema(Storage::Memory, "writer", "alice", schema).unwrap();

    authority.create_branch("left", None).unwrap();
    authority.checkout_branch("left").unwrap();
    authority
        .insert_row(
            "notes",
            "note-1",
            BTreeMap::from([
                ("body".to_owned(), json!("Source version 1")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    authority
        .create_branch_from_branches("merge", &["left"])
        .unwrap();
    authority.checkout_branch("merge").unwrap();
    writer
        .apply_bundle(&authority.export_table_history("notes").unwrap())
        .unwrap();

    writer.checkout_branch("merge").unwrap();
    let update_tx = writer
        .transaction()
        .update_row(
            "notes",
            "note-1",
            BTreeMap::from([("body".to_owned(), json!("Writer update"))]),
        )
        .commit()
        .unwrap();
    let mut stale_bundle = writer.export_table_history("notes").unwrap();
    stale_bundle
        .txs
        .iter_mut()
        .find(|tx| tx.tx_id == update_tx)
        .unwrap()
        .conflict_mode = 2;

    authority.checkout_branch("left").unwrap();
    authority
        .update_row(
            "notes",
            "note-1",
            BTreeMap::from([("body".to_owned(), json!("Source version 2"))]),
        )
        .unwrap();
    authority.checkout_branch("merge").unwrap();

    authority.apply_untrusted_bundle(&stale_bundle).unwrap();

    let info = authority.transaction_info(&update_tx).unwrap();
    assert_eq!(info.conflict_mode, "exclusive");
    assert_eq!(info.rejection_code, Some("stale_read_set".to_owned()));
    let rows = authority.read_rows("notes").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values["body"], json!("Source version 2"));
}

#[test]
fn untrusted_exclusive_delete_rejects_stale_source_branch_row_read_set() {
    let schema = support::notes_schema();
    let mut authority = Runtime::open_trusted_with_session_user(
        Storage::Memory,
        "authority",
        "alice",
        schema.clone(),
    )
    .unwrap();
    let mut writer = Runtime::open_with_schema(Storage::Memory, "writer", "alice", schema).unwrap();

    authority.create_branch("left", None).unwrap();
    authority.checkout_branch("left").unwrap();
    authority
        .insert_row(
            "notes",
            "note-1",
            BTreeMap::from([
                ("body".to_owned(), json!("Source version 1")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    authority
        .create_branch_from_branches("merge", &["left"])
        .unwrap();
    authority.checkout_branch("merge").unwrap();
    writer
        .apply_bundle(&authority.export_table_history("notes").unwrap())
        .unwrap();

    writer.checkout_branch("merge").unwrap();
    let delete_tx = writer
        .transaction()
        .delete_row("notes", "note-1")
        .commit()
        .unwrap();
    let mut stale_bundle = writer.export_table_history("notes").unwrap();
    stale_bundle
        .txs
        .iter_mut()
        .find(|tx| tx.tx_id == delete_tx)
        .unwrap()
        .conflict_mode = 2;

    authority.checkout_branch("left").unwrap();
    authority
        .update_row(
            "notes",
            "note-1",
            BTreeMap::from([("body".to_owned(), json!("Source version 2"))]),
        )
        .unwrap();
    authority.checkout_branch("merge").unwrap();

    authority.apply_untrusted_bundle(&stale_bundle).unwrap();

    let info = authority.transaction_info(&delete_tx).unwrap();
    assert_eq!(info.conflict_mode, "exclusive");
    assert_eq!(info.rejection_code, Some("stale_read_set".to_owned()));
    let rows = authority.read_rows("notes").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values["body"], json!("Source version 2"));
}

#[test]
fn branch_exclusive_transaction_observes_inherited_base_read_version() {
    let schema = SchemaDef::new()
        .table("projects", |table| {
            table.text("title");
            table.read_if_created_by_user();
        })
        .table("todos", |table| {
            table.text("title");
            table.bool("done");
            table.ref_("project", "projects");
            table.write_if_ref_readable("project");
        });
    let mut authority = Runtime::open_trusted_with_session_user(
        Storage::Memory,
        "authority",
        "alice",
        schema.clone(),
    )
    .unwrap();
    let mut writer = Runtime::open_with_schema(Storage::Memory, "writer", "alice", schema).unwrap();

    let project_tx = authority
        .insert_row(
            "projects",
            "project-1",
            BTreeMap::from([("title".to_owned(), json!("Base project"))]),
        )
        .unwrap();
    authority
        .accept_transaction_at_global(&project_tx, 1)
        .unwrap();
    writer
        .apply_bundle(&authority.export_table_history("projects").unwrap())
        .unwrap();

    writer.create_branch("draft", Some(1)).unwrap();
    writer.checkout_branch("draft").unwrap();
    let todo_tx = writer
        .transaction()
        .insert_row(
            "todos",
            "todo-branch",
            BTreeMap::from([
                ("title".to_owned(), json!("Reads inherited project")),
                ("done".to_owned(), json!(false)),
                ("project".to_owned(), json!("project-1")),
            ]),
        )
        .commit()
        .unwrap();

    let observed_reads = writer.transaction_observed_read_rows(&todo_tx).unwrap();
    assert!(observed_reads.contains(&(
        "projects".to_owned(),
        "project-1".to_owned(),
        Some(project_tx)
    )));
}

#[test]
fn untrusted_exclusive_validation_handles_new_branch_records() {
    let schema = SchemaDef::new()
        .table("projects", |table| {
            table.text("title");
            table.read_if_created_by_user();
        })
        .table("todos", |table| {
            table.text("title");
            table.bool("done");
            table.ref_("project", "projects");
            table.write_if_ref_readable("project");
        });
    let mut authority = Runtime::open_trusted_with_session_user(
        Storage::Memory,
        "authority",
        "alice",
        schema.clone(),
    )
    .unwrap();
    let mut writer = Runtime::open_with_schema(Storage::Memory, "writer", "alice", schema).unwrap();

    let project_tx = authority
        .insert_row(
            "projects",
            "project-1",
            BTreeMap::from([("title".to_owned(), json!("Base project"))]),
        )
        .unwrap();
    authority
        .accept_transaction_at_global(&project_tx, 1)
        .unwrap();
    writer
        .apply_bundle(&authority.export_table_history("projects").unwrap())
        .unwrap();

    writer.create_branch("draft", Some(1)).unwrap();
    writer.checkout_branch("draft").unwrap();
    let todo_tx = writer
        .transaction()
        .insert_row(
            "todos",
            "todo-branch",
            BTreeMap::from([
                ("title".to_owned(), json!("New branch exclusive")),
                ("done".to_owned(), json!(false)),
                ("project".to_owned(), json!("project-1")),
            ]),
        )
        .commit()
        .unwrap();
    let bundle = writer
        .export_exclusive_transaction_forwarding("todos", &todo_tx, "alice")
        .unwrap();

    authority.apply_untrusted_bundle(&bundle).unwrap();
    authority.checkout_branch("draft").unwrap();
    assert_eq!(
        authority.transaction_info(&todo_tx).unwrap().rejection_code,
        None
    );
    assert_eq!(authority.read_rows("todos").unwrap().len(), 1);
    assert_eq!(
        authority.read_rows("todos").unwrap()[0].values["title"],
        json!("New branch exclusive")
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
    let mut alice = support::open_todo_app(Storage::Memory, "alice-node", "alice").unwrap();

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
fn generic_update_preserves_omitted_fields() {
    let schema = support::notes_schema();
    let mut db = Runtime::open_with_schema(Storage::Memory, "node", "alice", schema).unwrap();

    db.insert_row(
        "notes",
        "note-1",
        BTreeMap::from([
            ("body".to_owned(), json!("Draft")),
            ("pinned".to_owned(), json!(false)),
        ]),
    )
    .unwrap();

    db.update_row(
        "notes",
        "note-1",
        BTreeMap::from([("pinned".to_owned(), json!(true))]),
    )
    .unwrap();

    let rows = db.read_rows("notes").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values["body"], json!("Draft"));
    assert_eq!(rows[0].values["pinned"], json!(true));
}

#[test]
fn rejecting_update_restores_previous_visible_version() {
    let schema = support::notes_schema();
    let mut db = Runtime::open_with_schema(Storage::Memory, "node", "alice", schema).unwrap();

    db.insert_row(
        "notes",
        "note-1",
        BTreeMap::from([
            ("body".to_owned(), json!("Accepted base")),
            ("pinned".to_owned(), json!(false)),
        ]),
    )
    .unwrap();
    let update_tx = db
        .update_row(
            "notes",
            "note-1",
            BTreeMap::from([("body".to_owned(), json!("Rejected update"))]),
        )
        .unwrap();
    assert_eq!(
        db.read_rows("notes").unwrap()[0].values["body"],
        json!("Rejected update")
    );

    db.reject_transaction(&update_tx, "policy_denied").unwrap();

    let rows = db.read_rows("notes").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values["body"], json!("Accepted base"));
    assert_eq!(rows[0].values["pinned"], json!(false));
}
