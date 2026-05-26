use super::*;

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
fn predicate_subscription_diffs_when_row_enters_and_leaves_query() {
    let schema = SchemaDef::new().table("tasks", |table| {
        table.text("title");
        table.bool("done");
    });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema).unwrap();

    alice
        .insert_row(
            "tasks",
            "task-open",
            BTreeMap::from([
                ("title".to_owned(), json!("Open")),
                ("done".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    alice
        .insert_row(
            "tasks",
            "task-closed",
            BTreeMap::from([
                ("title".to_owned(), json!("Closed")),
                ("done".to_owned(), json!(true)),
            ]),
        )
        .unwrap();

    let mut subscription = alice
        .subscribe_rows_where_eq("tasks", "done", json!(false))
        .unwrap();
    assert_eq!(subscription.initial_rows().len(), 1);
    assert_eq!(subscription.initial_rows()[0].id, "task-open");

    alice
        .update_row(
            "tasks",
            "task-closed",
            BTreeMap::from([("done".to_owned(), json!(false))]),
        )
        .unwrap();
    let diffs = alice.poll_subscription(&mut subscription).unwrap();
    assert!(matches!(&diffs[..], [RowDiff::Added(row)] if row.id == "task-closed"));

    alice
        .update_row(
            "tasks",
            "task-open",
            BTreeMap::from([("done".to_owned(), json!(true))]),
        )
        .unwrap();
    let diffs = alice.poll_subscription(&mut subscription).unwrap();
    assert!(matches!(&diffs[..], [RowDiff::Removed(row)] if row.id == "task-open"));
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
fn subscription_removes_row_when_visible_transaction_is_rejected() {
    let schema = support::notes_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema).unwrap();

    let tx = alice
        .insert_row(
            "notes",
            "note-1",
            BTreeMap::from([
                ("body".to_owned(), json!("Optimistic")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    let mut subscription = alice.subscribe_rows("notes").unwrap();
    assert_eq!(subscription.initial_rows().len(), 1);

    alice.reject_transaction(&tx, "policy_denied").unwrap();

    let diffs = alice.poll_subscription(&mut subscription).unwrap();
    assert!(matches!(&diffs[..], [RowDiff::Removed(row)] if row.id == "note-1"));
    assert_eq!(
        subscription.initial_rows(),
        alice.read_rows("notes").unwrap()
    );
    assert!(alice.read_rows("notes").unwrap().is_empty());
}

#[test]
fn subscription_diffs_when_active_branch_changes() {
    let schema = SchemaDef::new().table("tasks", |table| {
        table.text("title");
        table.bool("done");
    });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema).unwrap();

    alice
        .insert_row(
            "tasks",
            "task-main",
            BTreeMap::from([
                ("title".to_owned(), json!("Main")),
                ("done".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    let mut subscription = alice.subscribe_rows("tasks").unwrap();
    assert_eq!(subscription.initial_rows()[0].id, "task-main");

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

    let diffs = alice.poll_subscription(&mut subscription).unwrap();
    assert_eq!(alice.read_rows("tasks").unwrap().len(), 2);
    assert!(matches!(&diffs[..], [RowDiff::Added(row)] if row.id == "task-draft"));

    alice.checkout_branch("main").unwrap();
    let diffs = alice.poll_subscription(&mut subscription).unwrap();
    assert!(matches!(&diffs[..], [RowDiff::Removed(row)] if row.id == "task-draft"));
}

#[test]
fn subscription_on_pinned_branch_ignores_later_main_updates_until_overlay_changes() {
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

    let mut subscription = alice.subscribe_rows("tasks").unwrap();
    assert_eq!(
        subscription.initial_rows()[0].values["title"],
        json!("Base")
    );

    alice.checkout_branch("main").unwrap();
    let update_tx = alice
        .update_row(
            "tasks",
            "task-1",
            BTreeMap::from([
                ("title".to_owned(), json!("Main after branch")),
                ("done".to_owned(), json!(true)),
            ]),
        )
        .unwrap();
    alice.accept_transaction_at_global(&update_tx, 2).unwrap();
    alice.checkout_branch("draft").unwrap();

    assert!(alice
        .poll_subscription(&mut subscription)
        .unwrap()
        .is_empty());
    assert_eq!(
        alice.read_rows("tasks").unwrap()[0].values["title"],
        json!("Base")
    );
}
