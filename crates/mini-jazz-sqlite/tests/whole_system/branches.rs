use super::*;

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
fn branch_base_snapshot_chooses_latest_row_version_within_same_global_epoch() {
    let schema = SchemaDef::new().table("tasks", |table| {
        table.text("title");
        table.bool("done");
    });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema).unwrap();

    let first_tx = alice
        .insert_row(
            "tasks",
            "task-1",
            BTreeMap::from([
                ("title".to_owned(), json!("First in epoch")),
                ("done".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    alice.accept_transaction_at_global(&first_tx, 7).unwrap();
    let second_tx = alice
        .update_row(
            "tasks",
            "task-1",
            BTreeMap::from([
                ("title".to_owned(), json!("Second in epoch")),
                ("done".to_owned(), json!(true)),
            ]),
        )
        .unwrap();
    alice.accept_transaction_at_global(&second_tx, 7).unwrap();

    alice.create_branch("draft", Some(7)).unwrap();
    alice.checkout_branch("draft").unwrap();

    let rows = alice.read_rows("tasks").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values["title"], json!("Second in epoch"));
}

#[test]
fn branch_delete_shadows_pinned_base_row() {
    let schema = SchemaDef::new().table("tasks", |table| {
        table.text("title");
        table.bool("done");
    });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut peer =
        Runtime::open_with_schema(Storage::Memory, "alice-peer-node", "alice", schema).unwrap();

    let base_tx = alice
        .insert_row(
            "tasks",
            "task-1",
            BTreeMap::from([
                ("title".to_owned(), json!("Base task")),
                ("done".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    alice.accept_transaction_at_global(&base_tx, 1).unwrap();
    alice.create_branch("draft", Some(1)).unwrap();
    alice.checkout_branch("draft").unwrap();
    assert_eq!(alice.read_rows("tasks").unwrap().len(), 1);

    alice.delete_row("tasks", "task-1").unwrap();

    assert!(alice.read_rows("tasks").unwrap().is_empty());

    peer.apply_bundle(&alice.export_table_history("tasks").unwrap())
        .unwrap();
    peer.checkout_branch("draft").unwrap();
    assert!(peer.read_rows("tasks").unwrap().is_empty());

    peer.clear_current_projection_for_test().unwrap();
    peer.rebuild_current_projection().unwrap();
    assert!(peer.read_rows("tasks").unwrap().is_empty());
}

#[test]
fn rejected_branch_update_reveals_pinned_base_row_again() {
    let schema = SchemaDef::new().table("tasks", |table| {
        table.text("title");
        table.bool("done");
    });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema).unwrap();

    let base_tx = alice
        .insert_row(
            "tasks",
            "task-1",
            BTreeMap::from([
                ("title".to_owned(), json!("Base")),
                ("done".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    alice.accept_transaction_at_global(&base_tx, 1).unwrap();
    alice.create_branch("draft", Some(1)).unwrap();
    alice.checkout_branch("draft").unwrap();

    let draft_tx = alice
        .update_row(
            "tasks",
            "task-1",
            BTreeMap::from([
                ("title".to_owned(), json!("Draft")),
                ("done".to_owned(), json!(true)),
            ]),
        )
        .unwrap();
    assert_eq!(
        alice.read_rows("tasks").unwrap()[0].values["title"],
        json!("Draft")
    );

    alice.reject_transaction(&draft_tx, "conflict").unwrap();

    let rows = alice.read_rows("tasks").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values["title"], json!("Base"));

    alice.clear_current_projection_for_test().unwrap();
    alice.rebuild_current_projection().unwrap();
    let rebuilt = alice.read_rows("tasks").unwrap();
    assert_eq!(rebuilt.len(), 1);
    assert_eq!(rebuilt[0].values["title"], json!("Base"));
}

#[test]
fn rejected_branch_delete_reveals_pinned_base_row_again() {
    let schema = SchemaDef::new().table("tasks", |table| {
        table.text("title");
        table.bool("done");
    });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema).unwrap();

    let base_tx = alice
        .insert_row(
            "tasks",
            "task-1",
            BTreeMap::from([
                ("title".to_owned(), json!("Base")),
                ("done".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    alice.accept_transaction_at_global(&base_tx, 1).unwrap();
    alice.create_branch("draft", Some(1)).unwrap();
    alice.checkout_branch("draft").unwrap();

    let delete_tx = alice.delete_row("tasks", "task-1").unwrap();
    assert!(alice.read_rows("tasks").unwrap().is_empty());

    alice.reject_transaction(&delete_tx, "conflict").unwrap();

    let rows = alice.read_rows("tasks").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values["title"], json!("Base"));
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
fn branch_ref_policy_uses_branch_local_parent_visibility() {
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
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema).unwrap();

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
                ("title".to_owned(), json!("Visible from base")),
                ("project".to_owned(), json!("project-1")),
            ]),
        )
        .unwrap();
    alice.accept_transaction_at_global(&todo_tx, 2).unwrap();
    alice.create_branch("draft", Some(2)).unwrap();
    alice.checkout_branch("draft").unwrap();
    assert_eq!(alice.read_rows("todos").unwrap().len(), 1);

    alice.principal_for_test("bob");
    alice
        .update_row(
            "projects",
            "project-1",
            BTreeMap::from([("title".to_owned(), json!("Bob-owned branch project"))]),
        )
        .unwrap();
    alice.principal_for_test("alice");

    assert!(alice.read_rows("todos").unwrap().is_empty());
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
