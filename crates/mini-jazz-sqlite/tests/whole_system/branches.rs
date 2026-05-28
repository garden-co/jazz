use super::*;

#[test]
fn branch_sources_reject_direct_and_indirect_cycles() {
    let mut alice = Runtime::open(Storage::Memory, "alice-node", "alice").unwrap();

    alice.create_branch("left", None).unwrap();
    assert!(alice.add_branch_source("left", "left").is_err());

    alice.create_branch("right", None).unwrap();
    alice.add_branch_source("left", "right").unwrap();

    let err = alice.add_branch_source("right", "left").unwrap_err();
    assert!(err.to_string().contains("branch source cycle"));

    alice.checkout_branch("left").unwrap();
    alice
        .create_todo("todo-left", "Still readable", false, "project-missing")
        .unwrap();
    alice.checkout_branch("right").unwrap();
    assert!(alice.read_rows("todos").unwrap().is_empty());
}

#[test]
fn synced_branch_source_cycle_fails_without_partial_catalogue_apply() {
    let mut alice = Runtime::open(Storage::Memory, "alice-node", "alice").unwrap();
    let mut peer = Runtime::open(Storage::Memory, "peer-node", "alice").unwrap();
    let branches_before = peer.branches().unwrap();

    alice.create_branch("left", None).unwrap();
    alice.checkout_branch("left").unwrap();
    alice
        .create_todo("todo-left", "Left source", false, "project-missing")
        .unwrap();
    alice
        .create_branch_from_branches("merge", &["left"])
        .unwrap();
    alice.checkout_branch("merge").unwrap();

    let mut bundle = alice.export_table_history("todos").unwrap();
    bundle
        .branches
        .iter_mut()
        .find(|branch| branch.branch_id == "left")
        .unwrap()
        .source_branch_ids = vec!["merge".to_owned()];
    bundle
        .branches
        .iter_mut()
        .find(|branch| branch.branch_id == "left")
        .unwrap()
        .source_version = 10;

    let err = peer.apply_bundle(&bundle).unwrap_err();
    assert!(err.to_string().contains("branch source cycle"));
    assert_eq!(peer.branches().unwrap(), branches_before);
    assert!(peer.read_rows("todos").unwrap().is_empty());
}

#[test]
fn branch_local_write_is_invisible_on_main() {
    let schema = SchemaDef::new().table("tasks", |table| {
        table.text("title");
        table.bool("done");
    });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema).unwrap();

    alice.create_branch("draft", Some(0)).unwrap();
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
fn branch_absence_refresh_uses_branch_context_not_latest_main() {
    let mut alice = Runtime::open(Storage::Memory, "alice-node", "alice").unwrap();
    let mut peer = Runtime::open(Storage::Memory, "peer-node", "alice").unwrap();

    alice.create_branch("draft", Some(0)).unwrap();
    alice.checkout_branch("draft").unwrap();
    alice
        .create_todo(
            "todo-draft",
            "Draft project may arrive",
            false,
            "project-late",
        )
        .unwrap();
    let initial_bundle = alice.export_query_scope_open_todos().unwrap();
    assert_eq!(
        initial_bundle
            .branches
            .iter()
            .find(|branch| branch.branch_id == "draft")
            .unwrap()
            .base_global_epoch,
        Some(0)
    );
    peer.apply_bundle(&initial_bundle).unwrap();
    peer.checkout_branch("draft").unwrap();
    assert_eq!(
        peer.branches()
            .unwrap()
            .into_iter()
            .find(|branch| branch.id == "draft")
            .unwrap()
            .base_global_epoch,
        Some(0)
    );
    assert_eq!(peer.open_todos().unwrap()[0].project_title, None);
    let observed = peer.observed_query_reads().unwrap();
    assert!(observed.iter().any(|read| {
        read.branch_id == "draft"
            && read.table == "projects"
            && read.field == "id"
            && read.op == "absent"
            && read.value == json!("project-late")
    }));

    alice.checkout_branch("main").unwrap();
    let main_project_tx = alice
        .create_project("project-late", "Main project should not leak")
        .unwrap();
    alice
        .accept_transaction_at_global(&main_project_tx, 1)
        .unwrap();
    alice.checkout_branch("draft").unwrap();
    for refresh in alice.export_query_read_refreshes(&observed).unwrap() {
        peer.apply_bundle(&refresh).unwrap();
    }
    peer.checkout_branch("draft").unwrap();
    assert_eq!(peer.open_todos().unwrap()[0].project_title, None);

    alice
        .create_project("project-late", "Draft project")
        .unwrap();
    for refresh in alice
        .export_query_read_refreshes(&peer.observed_query_reads().unwrap())
        .unwrap()
    {
        peer.apply_bundle(&refresh).unwrap();
    }
    assert_eq!(
        peer.open_todos().unwrap()[0].project_title.as_deref(),
        Some("Draft project")
    );
}

#[test]
fn durable_absent_observed_refresh_carries_branch_source_changes_after_reconnect() {
    let dir = tempdir().unwrap();
    let worker_path = dir.path().join("worker.sqlite");
    let schema = support::tasks_schema();
    let mut upstream =
        Runtime::open_with_schema(Storage::Memory, "upstream", "alice", schema.clone()).unwrap();

    upstream.create_branch("left", None).unwrap();
    upstream
        .create_branch_from_branches("merge", &["left"])
        .unwrap();
    upstream.checkout_branch("merge").unwrap();

    {
        let mut worker = Runtime::open_with_schema(
            Storage::File(worker_path.clone()),
            "worker",
            "alice",
            schema.clone(),
        )
        .unwrap();
        worker
            .apply_bundle(
                &upstream
                    .export_query_where_eq("tasks", "done", json!(false))
                    .unwrap(),
            )
            .unwrap();
        let mut absent_bundle = upstream
            .export_query_where_eq("tasks", "id", json!("task-missing"))
            .unwrap();
        absent_bundle.history.clear();
        absent_bundle.query_reads[0].op = "absent".to_owned();
        worker.apply_bundle(&absent_bundle).unwrap();
        let non_absent_reads = worker
            .observed_query_reads()
            .unwrap()
            .into_iter()
            .filter(|read| read.op != "absent")
            .collect::<Vec<_>>();
        for read in non_absent_reads {
            worker.forget_observed_query_read(&read).unwrap();
        }
        worker.checkout_branch("merge").unwrap();
        assert_eq!(
            worker
                .branches()
                .unwrap()
                .into_iter()
                .find(|branch| branch.id == "merge")
                .unwrap()
                .source_branch_ids,
            vec!["left"]
        );
    }

    upstream.remove_branch_source("merge", "left").unwrap();
    let mut reopened =
        Runtime::open_with_schema(Storage::File(worker_path), "worker", "alice", schema).unwrap();
    let observed = reopened.observed_query_reads().unwrap();
    assert!(observed.iter().any(|read| {
        read.branch_id == "merge"
            && read.table == "tasks"
            && read.field == "id"
            && read.op == "absent"
            && read.value == json!("task-missing")
    }));
    for refresh in upstream.export_query_read_refreshes(&observed).unwrap() {
        reopened.apply_bundle(&refresh).unwrap();
    }
    reopened.checkout_branch("merge").unwrap();

    assert!(reopened
        .branches()
        .unwrap()
        .into_iter()
        .find(|branch| branch.id == "merge")
        .unwrap()
        .source_branch_ids
        .is_empty());
}

#[test]
fn branch_global_acceptance_does_not_make_branch_row_visible_on_main_after_sync() {
    let schema = support::tasks_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut peer =
        Runtime::open_with_schema(Storage::Memory, "peer-node", "alice", schema).unwrap();

    alice.create_branch("draft", None).unwrap();
    alice.checkout_branch("draft").unwrap();
    let tx = alice
        .insert_row(
            "tasks",
            "task-draft",
            BTreeMap::from([
                ("title".to_owned(), json!("Accepted branch row")),
                ("done".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    alice.accept_transaction_at_global(&tx, 7).unwrap();

    peer.apply_bundle(&alice.export_table_history("tasks").unwrap())
        .unwrap();
    peer.rebuild_current_projection().unwrap();

    peer.checkout_branch("main").unwrap();
    assert!(peer.read_rows("tasks").unwrap().is_empty());
    peer.checkout_branch("draft").unwrap();
    assert_eq!(peer.read_rows("tasks").unwrap()[0].id, "task-draft");
    assert_eq!(peer.transaction_info(&tx).unwrap().global_epoch, Some(7));
}

#[test]
fn branch_query_scope_refresh_removes_base_row_shadowed_by_branch_overlay() {
    let schema = support::tasks_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut peer =
        Runtime::open_with_schema(Storage::Memory, "peer-node", "alice", schema).unwrap();

    let base_tx = alice
        .insert_row(
            "tasks",
            "task-1",
            BTreeMap::from([
                ("title".to_owned(), json!("Base open")),
                ("done".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    alice.accept_transaction_at_global(&base_tx, 1).unwrap();
    alice.create_branch("draft", Some(1)).unwrap();
    alice.checkout_branch("draft").unwrap();

    peer.apply_bundle(
        &alice
            .export_query_where_eq("tasks", "done", json!(false))
            .unwrap(),
    )
    .unwrap();
    peer.checkout_branch("draft").unwrap();
    assert_eq!(
        peer.read_rows_where_eq("tasks", "done", json!(false))
            .unwrap()
            .len(),
        1
    );

    alice
        .update_row(
            "tasks",
            "task-1",
            BTreeMap::from([
                ("title".to_owned(), json!("Draft closed")),
                ("done".to_owned(), json!(true)),
            ]),
        )
        .unwrap();
    peer.apply_bundle(
        &alice
            .export_query_where_eq("tasks", "done", json!(false))
            .unwrap(),
    )
    .unwrap();

    assert!(peer
        .read_rows_where_eq("tasks", "done", json!(false))
        .unwrap()
        .is_empty());
}

#[test]
fn durable_branch_query_read_refreshes_after_restart() {
    let dir = tempdir().unwrap();
    let worker_path = dir.path().join("branch-worker.sqlite");
    let schema = support::tasks_schema();
    let mut upstream =
        Runtime::open_with_schema(Storage::Memory, "upstream", "alice", schema.clone()).unwrap();

    let base_tx = upstream
        .insert_row(
            "tasks",
            "task-1",
            BTreeMap::from([
                ("title".to_owned(), json!("Base open")),
                ("done".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    upstream.accept_transaction_at_global(&base_tx, 1).unwrap();
    upstream.create_branch("draft", Some(1)).unwrap();
    upstream.checkout_branch("draft").unwrap();

    {
        let mut worker = Runtime::open_with_schema(
            Storage::File(worker_path.clone()),
            "worker",
            "alice",
            schema.clone(),
        )
        .unwrap();
        worker
            .apply_bundle(
                &upstream
                    .export_query_where_eq("tasks", "done", json!(false))
                    .unwrap(),
            )
            .unwrap();
        worker.checkout_branch("draft").unwrap();
        assert_eq!(
            worker
                .read_rows_where_eq("tasks", "done", json!(false))
                .unwrap()
                .len(),
            1
        );
    }

    upstream
        .update_row(
            "tasks",
            "task-1",
            BTreeMap::from([
                ("title".to_owned(), json!("Draft closed")),
                ("done".to_owned(), json!(true)),
            ]),
        )
        .unwrap();

    let mut reopened = Runtime::open_with_schema(
        Storage::File(worker_path),
        "worker-reopened",
        "alice",
        schema,
    )
    .unwrap();
    reopened.checkout_branch("draft").unwrap();
    let desired_queries = reopened.observed_query_reads().unwrap();
    for refresh in upstream
        .export_query_read_refreshes(&desired_queries)
        .unwrap()
    {
        reopened.apply_bundle(&refresh).unwrap();
    }

    assert!(reopened
        .read_rows_where_eq("tasks", "done", json!(false))
        .unwrap()
        .is_empty());
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
fn branch_scoped_export_excludes_unrelated_deleted_rows() {
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
            "task-live",
            BTreeMap::from([
                ("title".to_owned(), json!("Live")),
                ("done".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    alice
        .insert_row(
            "tasks",
            "task-deleted",
            BTreeMap::from([
                ("title".to_owned(), json!("Deleted")),
                ("done".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    alice.delete_row("tasks", "task-deleted").unwrap();

    let rows = alice
        .export_query_where_eq("tasks", "title", json!("Live"))
        .unwrap()
        .history
        .into_iter()
        .map(|record| row_id_with_op(record.row_id, record.op))
        .collect::<Vec<_>>();

    assert_eq!(rows, vec!["task-live:1"]);
}

fn row_id_with_op(row_id: String, op: i64) -> String {
    format!("{row_id}:{op}")
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
fn branch_reads_main_base_after_history_compaction() {
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
    let stats = alice
        .compact_accepted_history("tasks", "task-1", 0)
        .unwrap();
    assert_eq!(stats.sealed_history_rows, 1);
    assert_eq!(alice.storage_stats().unwrap().history_rows, 1);

    alice.checkout_branch("draft").unwrap();
    let rows = alice.read_rows("tasks").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values["title"], json!("Base title"));
}

#[test]
fn fixture_open_todos_reads_pinned_base_with_sparse_overlay() {
    let mut alice = Runtime::open(Storage::Memory, "alice-node", "alice").unwrap();

    let project_tx = alice.create_project("project-1", "Base project").unwrap();
    alice.accept_transaction_at_global(&project_tx, 1).unwrap();
    let todo_tx = alice
        .create_todo("todo-1", "Base todo", false, "project-1")
        .unwrap();
    alice.accept_transaction_at_global(&todo_tx, 2).unwrap();

    alice.create_branch("draft", Some(2)).unwrap();
    alice.checkout_branch("draft").unwrap();

    let todos = alice.open_todos().unwrap();
    assert_eq!(todos.len(), 1);
    assert_eq!(todos[0].title, "Base todo");
    assert_eq!(todos[0].project_title.as_deref(), Some("Base project"));

    let branch_tx = alice
        .create_todo("todo-1", "Draft todo", false, "project-1")
        .unwrap();
    alice.accept_transaction_at_global(&branch_tx, 3).unwrap();

    let todos = alice.open_todos().unwrap();
    assert_eq!(todos.len(), 1);
    assert_eq!(todos[0].title, "Draft todo");
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
fn branch_top_query_uses_pinned_base_plus_sparse_overlay() {
    let schema = SchemaDef::new().table("tasks", |table| {
        table.text("owner");
        table.text("rank");
        table.text("title");
        table.index("owner_rank", ["owner", "rank"]);
    });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema).unwrap();

    let mut base_tx = alice.transaction().exclusive_at_global(1);
    for index in 0..6 {
        base_tx = base_tx.insert_row(
            "tasks",
            &format!("task-{index}"),
            BTreeMap::from([
                ("owner".to_owned(), json!("alice")),
                ("rank".to_owned(), json!(format!("{index:03}"))),
                ("title".to_owned(), json!(format!("Base {index}"))),
            ]),
        );
    }
    base_tx.commit().unwrap();
    alice.create_branch("draft", Some(1)).unwrap();

    alice
        .transaction()
        .exclusive_at_global(2)
        .insert_row(
            "tasks",
            "main-after-base",
            BTreeMap::from([
                ("owner".to_owned(), json!("alice")),
                ("rank".to_owned(), json!("999")),
                ("title".to_owned(), json!("Invisible after base")),
            ]),
        )
        .commit()
        .unwrap();

    alice.checkout_branch("draft").unwrap();
    alice
        .update_row(
            "tasks",
            "task-2",
            BTreeMap::from([
                ("rank".to_owned(), json!("998")),
                ("title".to_owned(), json!("Draft overlay")),
            ]),
        )
        .unwrap();

    let rows = alice
        .read_rows_where_eq_top_field_desc("tasks", "owner", json!("alice"), "rank", 3)
        .unwrap();
    assert_eq!(
        rows.iter().map(|row| row.id.as_str()).collect::<Vec<_>>(),
        vec!["task-2", "task-5", "task-4"]
    );
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
fn branch_export_with_sealed_history_stays_pinned_to_base_epoch() {
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
    alice
        .compact_accepted_history("tasks", "task-1", 0)
        .unwrap();

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
fn branch_query_export_with_sealed_history_stays_pinned_to_base_epoch() {
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
    alice
        .compact_accepted_history("tasks", "task-1", 0)
        .unwrap();

    alice.checkout_branch("draft").unwrap();
    let bundle = alice
        .export_query_where_eq("tasks", "done", json!(false))
        .unwrap();
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
    let rows = bob
        .read_rows_where_eq("tasks", "done", json!(false))
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values["title"], json!("Base title"));
}

#[test]
fn branch_query_history_delta_with_compaction_stays_pinned_to_base_epoch() {
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
    alice
        .compact_accepted_history("tasks", "task-1", 0)
        .unwrap();

    alice.checkout_branch("draft").unwrap();
    let delta = alice
        .export_query_where_eq_history_delta("tasks", "done", json!(false), &[])
        .unwrap();
    assert!(delta
        .bundle
        .history
        .iter()
        .any(|record| record.values["title"] == json!("Base title")));
    assert!(!delta
        .bundle
        .history
        .iter()
        .any(|record| record.values["title"] == json!("Main after branch")));
    assert!(delta.blocks.is_empty());

    bob.apply_history_delta(&delta.bundle, &delta.blocks)
        .unwrap();
    bob.checkout_branch("draft").unwrap();
    let rows = bob
        .read_rows_where_eq("tasks", "done", json!(false))
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values["title"], json!("Base title"));
}

#[test]
fn branch_table_history_delta_with_compaction_omits_future_main_blocks() {
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
    alice
        .compact_accepted_history("tasks", "task-1", 0)
        .unwrap();

    alice.checkout_branch("draft").unwrap();
    let delta = alice.export_table_history_delta("tasks", &[]).unwrap();
    assert!(delta.blocks.is_empty());

    bob.apply_history_delta(&delta.bundle, &delta.blocks)
        .unwrap();
    bob.checkout_branch("draft").unwrap();
    let rows = bob.read_rows("tasks").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values["title"], json!("Base title"));
}

#[test]
fn branch_all_history_delta_with_compaction_omits_future_main_blocks() {
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
    alice
        .compact_accepted_history("tasks", "task-1", 0)
        .unwrap();

    alice.checkout_branch("draft").unwrap();
    let delta = alice.export_all_history_delta(&[]).unwrap();
    assert!(delta.blocks.is_empty());

    bob.apply_history_delta(&delta.bundle, &delta.blocks)
        .unwrap();
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
        table.read_if_created_by_user();
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
            table.read_if_created_by_user();
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
            table.read_if_created_by_user();
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

    alice.session_user_for_test("bob");
    alice
        .update_row(
            "projects",
            "project-1",
            BTreeMap::from([("title".to_owned(), json!("Bob-owned branch project"))]),
        )
        .unwrap();
    alice.session_user_for_test("alice");

    assert!(alice.read_rows("todos").unwrap().is_empty());
}

#[test]
fn branch_equality_query_uses_effective_branch_policy() {
    let schema = SchemaDef::new()
        .table("projects", |table| {
            table.text("title");
            table.read_if_created_by_user();
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
                ("title".to_owned(), json!("Find me")),
                ("project".to_owned(), json!("project-1")),
            ]),
        )
        .unwrap();
    alice.accept_transaction_at_global(&todo_tx, 2).unwrap();
    alice.create_branch("draft", Some(2)).unwrap();
    alice.checkout_branch("draft").unwrap();

    alice.session_user_for_test("bob");
    alice
        .update_row(
            "projects",
            "project-1",
            BTreeMap::from([("title".to_owned(), json!("Bob-owned branch project"))]),
        )
        .unwrap();
    alice.session_user_for_test("alice");

    assert!(alice
        .read_rows_where_eq("todos", "title", json!("Find me"))
        .unwrap()
        .is_empty());
}

#[test]
fn branch_base_export_preserves_ref_policy_at_base_epoch() {
    let schema = SchemaDef::new()
        .table("projects", |table| {
            table.text("title");
            table.read_if_created_by_user();
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

    let rows = alice.read_rows_with_conflict_meta("tasks").unwrap();
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().all(|row| row.conflict_count == 2));
}

#[test]
fn branch_conflict_metadata_surfaces_through_filtered_query() {
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
                ("title".to_owned(), json!("Shared title")),
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
                ("title".to_owned(), json!("Shared title")),
                ("done".to_owned(), json!(false)),
            ]),
        )
        .unwrap();

    alice
        .create_branch_from_branches("merge", &["left", "right"])
        .unwrap();
    alice.checkout_branch("merge").unwrap();

    let rows = alice
        .read_rows_where_eq_with_conflict_meta("tasks", "title", json!("Shared title"))
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().all(|row| row.id == "task-1"));
    assert!(rows.iter().all(|row| row.conflict_count == 2));
}

#[test]
fn branch_read_rows_includes_source_branch_candidates() {
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
            "task-left",
            BTreeMap::from([
                ("title".to_owned(), json!("Left task")),
                ("done".to_owned(), json!(false)),
            ]),
        )
        .unwrap();

    alice.create_branch("right", None).unwrap();
    alice.checkout_branch("right").unwrap();
    alice
        .insert_row(
            "tasks",
            "task-right",
            BTreeMap::from([
                ("title".to_owned(), json!("Right task")),
                ("done".to_owned(), json!(false)),
            ]),
        )
        .unwrap();

    alice
        .create_branch_from_branches("merge", &["left", "right"])
        .unwrap();
    alice.checkout_branch("merge").unwrap();

    let rows = alice.read_rows("tasks").unwrap();
    assert_eq!(rows.len(), 2);
    let titles = rows
        .iter()
        .map(|row| row.values["title"].clone())
        .collect::<Vec<_>>();
    assert!(titles.contains(&json!("Left task")));
    assert!(titles.contains(&json!("Right task")));
}

#[test]
fn branch_reads_transitive_source_branch_rows() {
    let schema = support::tasks_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema).unwrap();

    alice.create_branch("left", None).unwrap();
    alice.checkout_branch("left").unwrap();
    alice
        .insert_row(
            "tasks",
            "task-left",
            BTreeMap::from([
                ("title".to_owned(), json!("Left title")),
                ("done".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    alice
        .create_branch_from_branches("middle", &["left"])
        .unwrap();
    alice
        .create_branch_from_branches("merge", &["middle"])
        .unwrap();
    alice.checkout_branch("merge").unwrap();

    let rows = alice.read_rows("tasks").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, "task-left");
}

#[test]
fn branch_transitive_source_overlay_shadows_deeper_source_row() {
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
    alice
        .create_branch_from_branches("middle", &["left"])
        .unwrap();
    alice.checkout_branch("middle").unwrap();
    alice
        .update_row(
            "tasks",
            "task-1",
            BTreeMap::from([("title".to_owned(), json!("Middle title"))]),
        )
        .unwrap();
    alice
        .create_branch_from_branches("merge", &["middle"])
        .unwrap();
    alice.checkout_branch("merge").unwrap();

    let rows = alice.read_rows("tasks").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values["title"], json!("Middle title"));
}

#[test]
fn branch_conflict_candidates_include_transitive_source_branch_rows() {
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
    alice
        .create_branch_from_branches("middle", &["left"])
        .unwrap();
    alice
        .create_branch_from_branches("merge", &["middle"])
        .unwrap();
    alice.checkout_branch("merge").unwrap();

    let candidates = alice.read_row_candidates("tasks", "task-1").unwrap();
    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].values["title"], json!("Left title"));
}

#[test]
fn branch_conflict_metadata_counts_transitive_source_branch_candidates() {
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
        .create_branch_from_branches("middle-left", &["left"])
        .unwrap();
    alice
        .create_branch_from_branches("middle-right", &["right"])
        .unwrap();
    alice
        .create_branch_from_branches("merge", &["middle-left", "middle-right"])
        .unwrap();
    alice.checkout_branch("merge").unwrap();

    let candidates = alice.read_row_candidates("tasks", "task-1").unwrap();
    assert_eq!(candidates.len(), 2);
    let rows = alice.read_rows_with_conflict_meta("tasks").unwrap();
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().all(|row| row.conflict_count == 2));
}

#[test]
fn branch_update_rejects_ambiguous_transitive_source_conflict() {
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
        .create_branch_from_branches("middle-left", &["left"])
        .unwrap();
    alice
        .create_branch_from_branches("middle-right", &["right"])
        .unwrap();
    alice
        .create_branch_from_branches("merge", &["middle-left", "middle-right"])
        .unwrap();
    alice.checkout_branch("merge").unwrap();

    let err = alice
        .update_row(
            "tasks",
            "task-1",
            BTreeMap::from([("title".to_owned(), json!("Implicit resolution"))]),
        )
        .unwrap_err();
    assert!(err.to_string().contains("ambiguous branch row"));
}

#[test]
fn branch_update_after_transitive_conflict_resolution_uses_branch_local_base() {
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
        .create_branch_from_branches("middle-left", &["left"])
        .unwrap();
    alice
        .create_branch_from_branches("middle-right", &["right"])
        .unwrap();
    alice
        .create_branch_from_branches("merge", &["middle-left", "middle-right"])
        .unwrap();
    alice.checkout_branch("merge").unwrap();

    alice
        .resolve_row_conflict(
            "tasks",
            "task-1",
            BTreeMap::from([
                ("title".to_owned(), json!("Resolved title")),
                ("done".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    alice
        .update_row(
            "tasks",
            "task-1",
            BTreeMap::from([("done".to_owned(), json!(true))]),
        )
        .unwrap();

    let rows = alice.read_rows("tasks").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values["title"], json!("Resolved title"));
    assert_eq!(rows[0].values["done"], json!(true));
    assert_eq!(rows[0].conflict_count, 0);
}

#[test]
fn branch_transitive_conflict_resolution_survives_sync() {
    let schema = support::tasks_schema();
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
        .create_branch_from_branches("middle-left", &["left"])
        .unwrap();
    alice
        .create_branch_from_branches("middle-right", &["right"])
        .unwrap();
    alice
        .create_branch_from_branches("merge", &["middle-left", "middle-right"])
        .unwrap();
    alice.checkout_branch("merge").unwrap();

    alice
        .resolve_row_conflict(
            "tasks",
            "task-1",
            BTreeMap::from([
                ("title".to_owned(), json!("Resolved title")),
                ("done".to_owned(), json!(false)),
            ]),
        )
        .unwrap();

    bob.apply_bundle(&alice.export_table_history("tasks").unwrap())
        .unwrap();
    bob.checkout_branch("merge").unwrap();

    let rows = bob.read_rows_with_conflict_meta("tasks").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values["title"], json!("Resolved title"));
    assert_eq!(rows[0].conflict_count, 0);
    assert_eq!(bob.read_row_candidates("tasks", "task-1").unwrap().len(), 2);
}

#[test]
fn branch_query_scope_sync_preserves_transitive_source_branch_rows() {
    let schema = support::tasks_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut peer =
        Runtime::open_with_schema(Storage::Memory, "peer-node", "alice", schema).unwrap();

    alice.create_branch("left", None).unwrap();
    alice.checkout_branch("left").unwrap();
    alice
        .insert_row(
            "tasks",
            "task-left",
            BTreeMap::from([
                ("title".to_owned(), json!("Left title")),
                ("done".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    alice
        .create_branch_from_branches("middle", &["left"])
        .unwrap();
    alice
        .create_branch_from_branches("merge", &["middle"])
        .unwrap();
    alice.checkout_branch("merge").unwrap();

    peer.apply_bundle(
        &alice
            .export_query_where_eq("tasks", "done", json!(false))
            .unwrap(),
    )
    .unwrap();
    peer.checkout_branch("merge").unwrap();

    let rows = peer.read_rows("tasks").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, "task-left");
    let branches = peer.branches().unwrap();
    assert!(branches.iter().any(|branch| {
        branch.id == "merge" && branch.source_branch_ids == vec!["middle".to_owned()]
    }));
    assert!(
        branches
            .iter()
            .any(|branch| branch.id == "middle"
                && branch.source_branch_ids == vec!["left".to_owned()])
    );
}

#[test]
fn branch_observed_query_refresh_includes_later_transitive_source_branch_rows() {
    let schema = support::tasks_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut peer =
        Runtime::open_with_schema(Storage::Memory, "peer-node", "alice", schema).unwrap();

    alice.create_branch("left", None).unwrap();
    alice
        .create_branch_from_branches("middle", &["left"])
        .unwrap();
    alice
        .create_branch_from_branches("merge", &["middle"])
        .unwrap();
    alice.checkout_branch("merge").unwrap();
    peer.apply_bundle(
        &alice
            .export_query_where_eq("tasks", "done", json!(false))
            .unwrap(),
    )
    .unwrap();
    peer.checkout_branch("merge").unwrap();
    assert!(peer.read_rows("tasks").unwrap().is_empty());

    alice.checkout_branch("left").unwrap();
    alice
        .insert_row(
            "tasks",
            "task-left",
            BTreeMap::from([
                ("title".to_owned(), json!("Left later")),
                ("done".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    alice.checkout_branch("merge").unwrap();

    for refresh in alice
        .export_query_read_refreshes(&peer.observed_query_reads().unwrap())
        .unwrap()
    {
        peer.apply_bundle(&refresh).unwrap();
    }

    let rows = peer.read_rows("tasks").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, "task-left");
}

#[test]
fn branch_observed_query_refresh_removes_detached_transitive_source_branch_rows() {
    let schema = support::tasks_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut peer =
        Runtime::open_with_schema(Storage::Memory, "peer-node", "alice", schema).unwrap();

    alice.create_branch("left", None).unwrap();
    alice.checkout_branch("left").unwrap();
    alice
        .insert_row(
            "tasks",
            "task-left",
            BTreeMap::from([
                ("title".to_owned(), json!("Left task")),
                ("done".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    alice
        .create_branch_from_branches("middle", &["left"])
        .unwrap();
    alice
        .create_branch_from_branches("merge", &["middle"])
        .unwrap();
    alice.checkout_branch("merge").unwrap();
    peer.apply_bundle(
        &alice
            .export_query_where_eq("tasks", "done", json!(false))
            .unwrap(),
    )
    .unwrap();
    peer.checkout_branch("merge").unwrap();
    assert_eq!(peer.read_rows("tasks").unwrap().len(), 1);

    alice.remove_branch_source("middle", "left").unwrap();
    for refresh in alice
        .export_query_read_refreshes(&peer.observed_query_reads().unwrap())
        .unwrap()
    {
        peer.apply_bundle(&refresh).unwrap();
    }

    assert!(peer.read_rows("tasks").unwrap().is_empty());
    let middle = peer
        .branches()
        .unwrap()
        .into_iter()
        .find(|branch| branch.id == "middle")
        .unwrap();
    assert!(middle.source_branch_ids.is_empty());
}

#[test]
fn branch_observed_query_refresh_includes_later_source_branch_rows() {
    let schema = SchemaDef::new().table("tasks", |table| {
        table.text("title");
        table.bool("done");
    });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut peer =
        Runtime::open_with_schema(Storage::Memory, "alice-peer-node", "alice", schema).unwrap();

    alice.create_branch("left", None).unwrap();
    alice.create_branch("right", None).unwrap();
    alice
        .create_branch_from_branches("merge", &["left", "right"])
        .unwrap();
    alice.checkout_branch("merge").unwrap();

    let initial = alice
        .export_query_where_eq("tasks", "done", json!(false))
        .unwrap();
    assert!(initial.history.is_empty());
    peer.apply_bundle(&initial).unwrap();
    peer.checkout_branch("merge").unwrap();
    assert!(peer
        .read_rows_where_eq("tasks", "done", json!(false))
        .unwrap()
        .is_empty());

    alice.checkout_branch("left").unwrap();
    alice
        .insert_row(
            "tasks",
            "task-left",
            BTreeMap::from([
                ("title".to_owned(), json!("Left later")),
                ("done".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    alice.checkout_branch("merge").unwrap();

    for refresh in alice
        .export_query_read_refreshes(&peer.observed_query_reads().unwrap())
        .unwrap()
    {
        peer.apply_bundle(&refresh).unwrap();
    }

    let rows = peer
        .read_rows_where_eq("tasks", "done", json!(false))
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, "task-left");
    assert_eq!(
        peer.branches().unwrap()[0].source_branch_ids,
        Vec::<String>::new()
    );
    let merge = peer
        .branches()
        .unwrap()
        .into_iter()
        .find(|branch| branch.id == "merge")
        .unwrap();
    assert_eq!(merge.source_branch_ids, vec!["left", "right"]);
}

#[test]
fn branch_observed_query_refresh_includes_newly_added_source_branch() {
    let schema = SchemaDef::new().table("tasks", |table| {
        table.text("title");
        table.bool("done");
    });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut peer =
        Runtime::open_with_schema(Storage::Memory, "alice-peer-node", "alice", schema).unwrap();

    alice.create_branch("left", None).unwrap();
    alice.create_branch("merge", None).unwrap();
    alice.checkout_branch("left").unwrap();
    alice
        .insert_row(
            "tasks",
            "task-left",
            BTreeMap::from([
                ("title".to_owned(), json!("Left source")),
                ("done".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    alice.checkout_branch("merge").unwrap();

    peer.apply_bundle(
        &alice
            .export_query_where_eq("tasks", "done", json!(false))
            .unwrap(),
    )
    .unwrap();
    peer.checkout_branch("merge").unwrap();
    assert!(peer
        .read_rows_where_eq("tasks", "done", json!(false))
        .unwrap()
        .is_empty());

    alice.add_branch_source("merge", "left").unwrap();
    for refresh in alice
        .export_query_read_refreshes(&peer.observed_query_reads().unwrap())
        .unwrap()
    {
        peer.apply_bundle(&refresh).unwrap();
    }

    let rows = peer
        .read_rows_where_eq("tasks", "done", json!(false))
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, "task-left");
    let merge = peer
        .branches()
        .unwrap()
        .into_iter()
        .find(|branch| branch.id == "merge")
        .unwrap();
    assert_eq!(merge.source_branch_ids, vec!["left"]);
}

#[test]
fn branch_observed_query_refresh_removes_detached_source_branch_rows() {
    let schema = SchemaDef::new().table("tasks", |table| {
        table.text("title");
        table.bool("done");
    });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut peer =
        Runtime::open_with_schema(Storage::Memory, "alice-peer-node", "alice", schema).unwrap();

    alice.create_branch("left", None).unwrap();
    alice.checkout_branch("left").unwrap();
    alice
        .insert_row(
            "tasks",
            "task-left",
            BTreeMap::from([
                ("title".to_owned(), json!("Left source")),
                ("done".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    alice
        .create_branch_from_branches("merge", &["left"])
        .unwrap();
    alice.checkout_branch("merge").unwrap();

    peer.apply_bundle(
        &alice
            .export_query_where_eq("tasks", "done", json!(false))
            .unwrap(),
    )
    .unwrap();
    peer.checkout_branch("merge").unwrap();
    assert_eq!(
        peer.read_rows_where_eq("tasks", "done", json!(false))
            .unwrap()
            .len(),
        1
    );

    alice.remove_branch_source("merge", "left").unwrap();
    for refresh in alice
        .export_query_read_refreshes(&peer.observed_query_reads().unwrap())
        .unwrap()
    {
        peer.apply_bundle(&refresh).unwrap();
    }

    assert!(peer
        .read_rows_where_eq("tasks", "done", json!(false))
        .unwrap()
        .is_empty());
    let merge = peer
        .branches()
        .unwrap()
        .into_iter()
        .find(|branch| branch.id == "merge")
        .unwrap();
    assert!(merge.source_branch_ids.is_empty());
}

#[test]
fn stale_branch_source_bundle_does_not_readd_removed_source() {
    let schema = support::tasks_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut peer =
        Runtime::open_with_schema(Storage::Memory, "alice-peer-node", "alice", schema).unwrap();

    alice.create_branch("left", None).unwrap();
    alice.checkout_branch("left").unwrap();
    alice
        .insert_row(
            "tasks",
            "task-left",
            BTreeMap::from([
                ("title".to_owned(), json!("Left source")),
                ("done".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    alice
        .create_branch_from_branches("merge", &["left"])
        .unwrap();
    alice.checkout_branch("merge").unwrap();

    let stale_with_source = alice
        .export_query_where_eq("tasks", "done", json!(false))
        .unwrap();
    peer.apply_bundle(&stale_with_source).unwrap();
    peer.checkout_branch("merge").unwrap();
    assert_eq!(
        peer.branches()
            .unwrap()
            .into_iter()
            .find(|branch| branch.id == "merge")
            .unwrap()
            .source_branch_ids,
        vec!["left"]
    );

    alice.remove_branch_source("merge", "left").unwrap();
    let removal = alice
        .export_query_where_eq("tasks", "done", json!(false))
        .unwrap();
    peer.apply_bundle(&removal).unwrap();
    assert!(peer
        .branches()
        .unwrap()
        .into_iter()
        .find(|branch| branch.id == "merge")
        .unwrap()
        .source_branch_ids
        .is_empty());

    peer.apply_bundle(&stale_with_source).unwrap();
    assert!(peer
        .branches()
        .unwrap()
        .into_iter()
        .find(|branch| branch.id == "merge")
        .unwrap()
        .source_branch_ids
        .is_empty());
}

#[test]
fn durable_branch_source_removal_survives_reopen() {
    let dir = tempdir().unwrap();
    let worker_path = dir.path().join("worker.sqlite");
    let schema = support::tasks_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();

    alice.create_branch("left", None).unwrap();
    alice.checkout_branch("left").unwrap();
    alice
        .insert_row(
            "tasks",
            "task-left",
            BTreeMap::from([
                ("title".to_owned(), json!("Left source")),
                ("done".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    alice
        .create_branch_from_branches("merge", &["left"])
        .unwrap();
    alice.checkout_branch("merge").unwrap();

    {
        let mut worker = Runtime::open_with_schema(
            Storage::File(worker_path.clone()),
            "worker",
            "alice",
            schema.clone(),
        )
        .unwrap();
        worker
            .apply_bundle(
                &alice
                    .export_query_where_eq("tasks", "done", json!(false))
                    .unwrap(),
            )
            .unwrap();
        worker.checkout_branch("merge").unwrap();
        assert_eq!(
            worker
                .read_rows_where_eq("tasks", "done", json!(false))
                .unwrap()
                .len(),
            1
        );
    }

    alice.remove_branch_source("merge", "left").unwrap();
    let mut reopened =
        Runtime::open_with_schema(Storage::File(worker_path), "worker", "alice", schema).unwrap();
    reopened.checkout_branch("merge").unwrap();
    for refresh in alice
        .export_query_read_refreshes(&reopened.observed_query_reads().unwrap())
        .unwrap()
    {
        reopened.apply_bundle(&refresh).unwrap();
    }

    assert!(reopened
        .read_rows_where_eq("tasks", "done", json!(false))
        .unwrap()
        .is_empty());
    let merge = reopened
        .branches()
        .unwrap()
        .into_iter()
        .find(|branch| branch.id == "merge")
        .unwrap();
    assert!(merge.source_branch_ids.is_empty());
}

#[test]
fn branch_conflict_resolution_transaction_clears_conflict_meta_after_rebuild() {
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
    assert!(alice
        .read_rows_with_conflict_meta("tasks")
        .unwrap()
        .iter()
        .all(|row| row.conflict_count == 2));

    alice
        .resolve_row_conflict(
            "tasks",
            "task-1",
            BTreeMap::from([
                ("title".to_owned(), json!("Resolved title")),
                ("done".to_owned(), json!(false)),
            ]),
        )
        .unwrap();

    let rows = alice.read_rows_with_conflict_meta("tasks").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values["title"], json!("Resolved title"));
    assert_eq!(rows[0].conflict_count, 0);

    alice.clear_current_projection_for_test().unwrap();
    alice.rebuild_current_projection().unwrap();

    let rows = alice.read_rows_with_conflict_meta("tasks").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values["title"], json!("Resolved title"));
    assert_eq!(rows[0].conflict_count, 0);
}

#[test]
fn rejected_branch_conflict_resolution_restores_conflict_meta() {
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
    let resolution_tx = alice
        .resolve_row_conflict(
            "tasks",
            "task-1",
            BTreeMap::from([
                ("title".to_owned(), json!("Resolved title")),
                ("done".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    assert_eq!(
        alice.read_rows_with_conflict_meta("tasks").unwrap()[0].conflict_count,
        0
    );

    alice
        .reject_transaction(&resolution_tx, "policy_denied")
        .unwrap();

    let rows = alice.read_rows_with_conflict_meta("tasks").unwrap();
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().all(|row| row.id == "task-1"));
    assert!(rows.iter().all(|row| row.conflict_count == 2));
}

#[test]
fn branch_conflict_candidates_include_pinned_base_candidate() {
    let schema = support::tasks_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema).unwrap();

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

    alice.create_branch("left", Some(1)).unwrap();
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

    alice.create_branch("right", Some(1)).unwrap();
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
        .create_branch_from_branches_at_base("merge", Some(1), &["left", "right"])
        .unwrap();
    alice.checkout_branch("merge").unwrap();

    let titles = alice
        .read_row_candidates("tasks", "task-1")
        .unwrap()
        .into_iter()
        .map(|candidate| candidate.values["title"].clone())
        .collect::<Vec<_>>();
    assert_eq!(
        titles,
        vec![
            json!("Base title"),
            json!("Left title"),
            json!("Right title")
        ]
    );
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
fn branch_metadata_lists_and_syncs_base_and_sources() {
    let schema = support::tasks_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut bob = Runtime::open_with_schema(Storage::Memory, "bob-node", "bob", schema).unwrap();

    let base_tx = alice
        .insert_row(
            "tasks",
            "task-main",
            BTreeMap::from([
                ("title".to_owned(), json!("Main")),
                ("done".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    alice.accept_transaction_at_global(&base_tx, 5).unwrap();
    alice.create_branch("left", Some(5)).unwrap();
    alice.create_branch("right", Some(5)).unwrap();
    alice
        .create_branch_from_branches_at_base("merge", Some(5), &["left", "right"])
        .unwrap();
    alice.checkout_branch("merge").unwrap();
    alice
        .insert_row(
            "tasks",
            "task-merge",
            BTreeMap::from([
                ("title".to_owned(), json!("Merge marker")),
                ("done".to_owned(), json!(false)),
            ]),
        )
        .unwrap();

    let local_merge = alice
        .branches()
        .unwrap()
        .into_iter()
        .find(|branch| branch.id == "merge")
        .unwrap();
    assert_eq!(local_merge.base_global_epoch, Some(5));
    assert_eq!(local_merge.source_branch_ids, vec!["left", "right"]);
    assert_eq!(
        alice.branch_backing_rows().unwrap(),
        alice.branches().unwrap()
    );

    bob.apply_bundle(&alice.export_table_history("tasks").unwrap())
        .unwrap();
    let remote_merge = bob
        .branches()
        .unwrap()
        .into_iter()
        .find(|branch| branch.id == "merge")
        .unwrap();
    assert_eq!(remote_merge.base_global_epoch, Some(5));
    assert_eq!(remote_merge.source_branch_ids, vec!["left", "right"]);
    assert_eq!(bob.branch_backing_rows().unwrap(), bob.branches().unwrap());
}

#[test]
fn durable_merge_branch_refresh_preserves_pinned_source_branch_bases_after_restart() {
    let dir = tempdir().unwrap();
    let worker_path = dir.path().join("worker.sqlite");
    let schema = support::tasks_schema();
    let mut upstream =
        Runtime::open_with_schema(Storage::Memory, "upstream", "alice", schema.clone()).unwrap();

    let base_tx = upstream
        .insert_row(
            "tasks",
            "task-main",
            BTreeMap::from([
                ("title".to_owned(), json!("Main base")),
                ("done".to_owned(), json!(true)),
            ]),
        )
        .unwrap();
    upstream.accept_transaction_at_global(&base_tx, 5).unwrap();
    upstream.create_branch("left", Some(5)).unwrap();
    upstream.create_branch("right", Some(5)).unwrap();
    upstream
        .create_branch_from_branches_at_base("merge", Some(5), &["left", "right"])
        .unwrap();
    upstream.checkout_branch("merge").unwrap();

    {
        let mut worker = Runtime::open_with_schema(
            Storage::File(worker_path.clone()),
            "worker",
            "alice",
            schema.clone(),
        )
        .unwrap();
        worker
            .apply_bundle(
                &upstream
                    .export_query_where_eq("tasks", "done", json!(false))
                    .unwrap(),
            )
            .unwrap();
        worker.checkout_branch("merge").unwrap();
        assert!(worker
            .read_rows_where_eq("tasks", "done", json!(false))
            .unwrap()
            .is_empty());
    }

    upstream.checkout_branch("left").unwrap();
    upstream
        .insert_row(
            "tasks",
            "task-left",
            BTreeMap::from([
                ("title".to_owned(), json!("Left after restart")),
                ("done".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    upstream.checkout_branch("merge").unwrap();

    let mut reopened = Runtime::open_with_schema(
        Storage::File(worker_path),
        "worker",
        "alice",
        schema.clone(),
    )
    .unwrap();
    let observed = reopened.observed_query_reads().unwrap();
    for refresh in upstream.export_query_read_refreshes(&observed).unwrap() {
        reopened.apply_bundle(&refresh).unwrap();
    }
    reopened.checkout_branch("merge").unwrap();

    let rows = reopened
        .read_rows_where_eq("tasks", "done", json!(false))
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, "task-left");
    let branches = reopened.branches().unwrap();
    for branch_id in ["left", "right", "merge"] {
        let branch = branches
            .iter()
            .find(|branch| branch.id == branch_id)
            .unwrap();
        assert_eq!(branch.base_global_epoch, Some(5));
    }
    assert_eq!(reopened.branch_backing_rows().unwrap(), branches);

    let mut fresh_tab =
        Runtime::open_with_schema(Storage::Memory, "fresh-tab", "alice", schema).unwrap();
    fresh_tab
        .apply_bundle(&reopened.export_table_history("tasks").unwrap())
        .unwrap();
    assert_eq!(
        fresh_tab.branch_backing_rows().unwrap(),
        reopened.branches().unwrap()
    );
}

#[test]
fn branch_base_epoch_is_immutable() {
    let schema = support::tasks_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema).unwrap();

    alice.create_branch("draft", Some(5)).unwrap();
    alice.create_branch("draft", Some(5)).unwrap();
    let err = alice.create_branch("draft", Some(6)).unwrap_err();

    assert!(err.to_string().contains("branch base mismatch"));
    let draft = alice
        .branches()
        .unwrap()
        .into_iter()
        .find(|branch| branch.id == "draft")
        .unwrap();
    assert_eq!(draft.base_global_epoch, Some(5));
}

#[test]
fn branch_conflict_resolution_survives_sync() {
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
        .resolve_row_conflict(
            "tasks",
            "task-1",
            BTreeMap::from([
                ("title".to_owned(), json!("Resolved title")),
                ("done".to_owned(), json!(false)),
            ]),
        )
        .unwrap();

    bob.apply_bundle(&alice.export_table_history("tasks").unwrap())
        .unwrap();
    bob.checkout_branch("merge").unwrap();

    let rows = bob.read_rows_with_conflict_meta("tasks").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values["title"], json!("Resolved title"));
    assert_eq!(rows[0].conflict_count, 0);
    assert_eq!(bob.read_row_candidates("tasks", "task-1").unwrap().len(), 2);
}

#[test]
fn branch_conflict_candidates_respect_effective_row_policy() {
    let schema = SchemaDef::new()
        .table("projects", |table| {
            table.text("title");
            table.read_if_created_by_user();
        })
        .table("tasks", |table| {
            table.text("title");
            table.bool("done");
            table.ref_("project", "projects");
            table.read_if_ref_readable("project");
        });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema).unwrap();

    alice.create_branch("left", None).unwrap();
    alice.checkout_branch("left").unwrap();
    alice
        .insert_row(
            "projects",
            "project-left",
            BTreeMap::from([("title".to_owned(), json!("Alice project"))]),
        )
        .unwrap();
    alice
        .insert_row(
            "tasks",
            "task-1",
            BTreeMap::from([
                ("title".to_owned(), json!("Visible candidate")),
                ("done".to_owned(), json!(false)),
                ("project".to_owned(), json!("project-left")),
            ]),
        )
        .unwrap();

    alice.session_user_for_test("bob");
    alice.create_branch("right", None).unwrap();
    alice.checkout_branch("right").unwrap();
    alice
        .insert_row(
            "projects",
            "project-right",
            BTreeMap::from([("title".to_owned(), json!("Bob project"))]),
        )
        .unwrap();
    alice
        .insert_row(
            "tasks",
            "task-1",
            BTreeMap::from([
                ("title".to_owned(), json!("Hidden candidate")),
                ("done".to_owned(), json!(false)),
                ("project".to_owned(), json!("project-right")),
            ]),
        )
        .unwrap();

    alice.session_user_for_test("alice");
    alice
        .create_branch_from_branches("merge", &["left", "right"])
        .unwrap();
    alice.checkout_branch("merge").unwrap();

    let candidates = alice.read_row_candidates("tasks", "task-1").unwrap();
    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].values["title"], json!("Visible candidate"));
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
