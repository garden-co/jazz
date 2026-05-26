use super::*;

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
