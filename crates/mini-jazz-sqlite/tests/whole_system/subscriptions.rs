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
