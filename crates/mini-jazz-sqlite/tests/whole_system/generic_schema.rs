use super::*;

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
fn text_contains_query_matches_status_quo_substring_semantics() {
    let schema = support::notes_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema).unwrap();

    alice
        .insert_row(
            "notes",
            "note-1",
            BTreeMap::from([
                ("body".to_owned(), json!("SQLite core direction")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    alice
        .insert_row(
            "notes",
            "note-2",
            BTreeMap::from([
                ("body".to_owned(), json!("Unrelated")),
                ("pinned".to_owned(), json!(true)),
            ]),
        )
        .unwrap();

    let rows = alice
        .read_rows_where_contains("notes", "body", "core")
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, "note-1");

    assert_eq!(
        alice
            .read_rows_where_contains("notes", "body", "")
            .unwrap()
            .len(),
        2
    );
    assert!(alice
        .read_rows_where_contains("notes", "pinned", "true")
        .unwrap_err()
        .to_string()
        .contains("contains only supports text fields"));
}

#[test]
fn id_magic_field_query_matches_public_row_id() {
    let schema = support::notes_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema).unwrap();

    alice
        .insert_row(
            "notes",
            "note-public-id",
            BTreeMap::from([
                ("body".to_owned(), json!("Find by id")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    alice
        .insert_row(
            "notes",
            "note-other",
            BTreeMap::from([
                ("body".to_owned(), json!("Other")),
                ("pinned".to_owned(), json!(true)),
            ]),
        )
        .unwrap();

    let rows = alice
        .read_rows_where_eq("notes", "id", json!("note-public-id"))
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, "note-public-id");
    assert!(alice
        .read_rows_where_eq("notes", "id", json!(7))
        .unwrap_err()
        .to_string()
        .contains("id equality expects a string"));
}

#[test]
fn contains_query_scope_resync_removes_row_that_left_predicate() {
    let schema = support::notes_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut peer =
        Runtime::open_with_schema(Storage::Memory, "alice-peer-node", "alice", schema).unwrap();

    alice
        .insert_row(
            "notes",
            "note-1",
            BTreeMap::from([
                ("body".to_owned(), json!("contains target")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    peer.apply_bundle(
        &alice
            .export_query_where_contains("notes", "body", "target")
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        peer.read_rows_where_contains("notes", "body", "target")
            .unwrap()
            .len(),
        1
    );

    alice
        .update_row(
            "notes",
            "note-1",
            BTreeMap::from([("body".to_owned(), json!("moved elsewhere"))]),
        )
        .unwrap();
    let bundle = alice
        .export_query_where_contains("notes", "body", "target")
        .unwrap();
    assert_eq!(bundle.query_reads[0].op, "contains");
    peer.apply_bundle(&bundle).unwrap();

    assert!(peer
        .read_rows_where_contains("notes", "body", "target")
        .unwrap()
        .is_empty());
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
fn generic_equality_query_scope_exports_matching_rows_and_policy_dependencies() {
    let schema = SchemaDef::new()
        .table("projects", |table| {
            table.text("title");
            table.read_if_created_by_principal();
        })
        .table("tasks", |table| {
            table.text("title");
            table.bool("done");
            table.ref_("project", "projects");
            table.read_if_ref_readable("project");
        });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut peer =
        Runtime::open_with_schema(Storage::Memory, "alice-peer-node", "alice", schema).unwrap();

    alice
        .insert_row(
            "projects",
            "project-1",
            BTreeMap::from([("title".to_owned(), json!("Visible project"))]),
        )
        .unwrap();
    alice
        .insert_row(
            "tasks",
            "task-open",
            BTreeMap::from([
                ("title".to_owned(), json!("Open")),
                ("done".to_owned(), json!(false)),
                ("project".to_owned(), json!("project-1")),
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
                ("project".to_owned(), json!("project-1")),
            ]),
        )
        .unwrap();

    let rows = alice
        .read_rows_where_eq("tasks", "done", json!(false))
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, "task-open");

    let bundle = alice
        .export_query_where_eq("tasks", "done", json!(false))
        .unwrap();
    assert_eq!(bundle.query_reads.len(), 1);
    assert_eq!(bundle.query_reads[0].branch_id, "main");
    assert_eq!(bundle.query_reads[0].table, "tasks");
    assert_eq!(bundle.query_reads[0].field, "done");
    assert_eq!(bundle.query_reads[0].value, json!(false));
    let synced = bundle
        .history
        .iter()
        .map(|record| (record.table.as_str(), record.row_id.as_str()))
        .collect::<Vec<_>>();
    assert!(synced.contains(&("tasks", "task-open")));
    assert!(!synced.contains(&("tasks", "task-closed")));
    assert!(synced.contains(&("projects", "project-1")));

    peer.apply_bundle(&bundle).unwrap();
    let peer_rows = peer
        .read_rows_where_eq("tasks", "done", json!(false))
        .unwrap();
    assert_eq!(peer_rows.len(), 1);
    assert_eq!(peer_rows[0].id, "task-open");
}

#[test]
fn equality_query_scope_resync_removes_row_that_left_predicate() {
    let schema = SchemaDef::new().table("tasks", |table| {
        table.text("title");
        table.bool("done");
    });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut peer =
        Runtime::open_with_schema(Storage::Memory, "alice-peer-node", "alice", schema).unwrap();

    alice
        .insert_row(
            "tasks",
            "task-1",
            BTreeMap::from([
                ("title".to_owned(), json!("Initially open")),
                ("done".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    peer.apply_bundle(
        &alice
            .export_query_where_eq("tasks", "done", json!(false))
            .unwrap(),
    )
    .unwrap();
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
                ("title".to_owned(), json!("Now done")),
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
fn equality_query_scope_resync_removes_deleted_matching_row() {
    let schema = SchemaDef::new().table("tasks", |table| {
        table.text("title");
        table.bool("done");
    });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut peer =
        Runtime::open_with_schema(Storage::Memory, "alice-peer-node", "alice", schema).unwrap();

    alice
        .insert_row(
            "tasks",
            "task-1",
            BTreeMap::from([
                ("title".to_owned(), json!("Delete me")),
                ("done".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    peer.apply_bundle(
        &alice
            .export_query_where_eq("tasks", "done", json!(false))
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        peer.read_rows_where_eq("tasks", "done", json!(false))
            .unwrap()
            .len(),
        1
    );

    alice.delete_row("tasks", "task-1").unwrap();
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
fn query_scope_refresh_does_not_leak_unrelated_tombstones_while_repairing_deleted_match() {
    let schema = SchemaDef::new().table("tasks", |table| {
        table.text("title");
        table.bool("done");
    });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut peer =
        Runtime::open_with_schema(Storage::Memory, "alice-peer-node", "alice", schema).unwrap();

    alice
        .insert_row(
            "tasks",
            "task-matching",
            BTreeMap::from([
                ("title".to_owned(), json!("Matching")),
                ("done".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    alice
        .insert_row(
            "tasks",
            "task-unrelated",
            BTreeMap::from([
                ("title".to_owned(), json!("Unrelated")),
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
    assert_eq!(
        peer.read_rows_where_eq("tasks", "done", json!(false))
            .unwrap()
            .len(),
        1
    );

    alice.delete_row("tasks", "task-matching").unwrap();
    alice.delete_row("tasks", "task-unrelated").unwrap();
    let bundle = alice
        .export_query_where_eq("tasks", "done", json!(false))
        .unwrap();
    let synced = bundle
        .history
        .iter()
        .map(|record| (record.row_id.as_str(), record.op))
        .collect::<Vec<_>>();
    assert!(synced.contains(&("task-matching", 3)));
    assert!(!synced.iter().any(|(row_id, _)| *row_id == "task-unrelated"));

    peer.apply_bundle(&bundle).unwrap();
    assert!(peer
        .read_rows_where_eq("tasks", "done", json!(false))
        .unwrap()
        .is_empty());
}

#[test]
fn equality_query_scope_resync_removes_row_hidden_by_policy_dependency_change() {
    let schema = SchemaDef::new()
        .table("projects", |table| {
            table.text("title");
            table.read_if_created_by_principal();
        })
        .table("tasks", |table| {
            table.text("title");
            table.bool("done");
            table.ref_("project", "projects");
            table.read_if_ref_readable("project");
        });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut bob =
        Runtime::open_with_schema(Storage::Memory, "bob-node", "bob", schema.clone()).unwrap();
    let mut peer =
        Runtime::open_with_schema(Storage::Memory, "alice-peer-node", "alice", schema).unwrap();

    alice
        .insert_row(
            "projects",
            "project-alice",
            BTreeMap::from([("title".to_owned(), json!("Alice project"))]),
        )
        .unwrap();
    alice
        .insert_row(
            "tasks",
            "task-1",
            BTreeMap::from([
                ("title".to_owned(), json!("Still open")),
                ("done".to_owned(), json!(false)),
                ("project".to_owned(), json!("project-alice")),
            ]),
        )
        .unwrap();
    peer.apply_bundle(
        &alice
            .export_query_where_eq("tasks", "done", json!(false))
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        peer.read_rows_where_eq("tasks", "done", json!(false))
            .unwrap()
            .len(),
        1
    );

    bob.insert_row(
        "projects",
        "project-bob",
        BTreeMap::from([("title".to_owned(), json!("Bob project"))]),
    )
    .unwrap();
    alice
        .apply_bundle(&bob.export_table_history("projects").unwrap())
        .unwrap();
    alice
        .update_row(
            "tasks",
            "task-1",
            BTreeMap::from([("project".to_owned(), json!("project-bob"))]),
        )
        .unwrap();
    assert!(alice
        .read_rows_where_eq("tasks", "done", json!(false))
        .unwrap()
        .is_empty());

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
fn branch_equality_query_scope_records_branch_predicate_read() {
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
                ("title".to_owned(), json!("Base task")),
                ("done".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    alice.accept_transaction_at_global(&base_tx, 1).unwrap();
    alice.create_branch("draft", Some(1)).unwrap();
    alice.checkout_branch("draft").unwrap();

    let bundle = alice
        .export_query_where_eq("tasks", "done", json!(false))
        .unwrap();

    assert_eq!(bundle.query_reads.len(), 1);
    assert_eq!(bundle.query_reads[0].branch_id, "draft");
    assert_eq!(bundle.query_reads[0].table, "tasks");
    assert_eq!(bundle.query_reads[0].field, "done");
    assert_eq!(bundle.query_reads[0].value, json!(false));
}

#[test]
fn branch_equality_query_scope_resync_repairs_row_that_left_predicate() {
    let schema = SchemaDef::new().table("tasks", |table| {
        table.text("title");
        table.bool("done");
    });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut peer =
        Runtime::open_with_schema(Storage::Memory, "alice-peer-node", "alice", schema).unwrap();

    alice.create_branch("draft", None).unwrap();
    alice.checkout_branch("draft").unwrap();
    alice
        .insert_row(
            "tasks",
            "task-1",
            BTreeMap::from([
                ("title".to_owned(), json!("Draft open")),
                ("done".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    peer.apply_bundle(
        &alice
            .export_query_where_eq("tasks", "done", json!(false))
            .unwrap(),
    )
    .unwrap();
    peer.checkout_branch("draft").unwrap();
    let draft_rows = peer
        .read_rows_where_eq("tasks", "done", json!(false))
        .unwrap();
    assert_eq!(draft_rows.len(), 1);
    assert_eq!(draft_rows[0].id, "task-1");

    alice
        .update_row(
            "tasks",
            "task-1",
            BTreeMap::from([
                ("title".to_owned(), json!("Draft done")),
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
fn branch_query_scope_repair_does_not_delete_same_predicate_row_on_main() {
    let schema = SchemaDef::new().table("tasks", |table| {
        table.text("title");
        table.bool("done");
    });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut peer =
        Runtime::open_with_schema(Storage::Memory, "alice-peer-node", "alice", schema).unwrap();

    alice
        .insert_row(
            "tasks",
            "task-main",
            BTreeMap::from([
                ("title".to_owned(), json!("Main open")),
                ("done".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    peer.apply_bundle(
        &alice
            .export_query_where_eq("tasks", "done", json!(false))
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        peer.read_rows_where_eq("tasks", "done", json!(false))
            .unwrap()
            .len(),
        1
    );

    alice.create_branch("draft", None).unwrap();
    alice.checkout_branch("draft").unwrap();
    alice
        .insert_row(
            "tasks",
            "task-draft",
            BTreeMap::from([
                ("title".to_owned(), json!("Draft open")),
                ("done".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    peer.apply_bundle(
        &alice
            .export_query_where_eq("tasks", "done", json!(false))
            .unwrap(),
    )
    .unwrap();
    peer.checkout_branch("draft").unwrap();
    let draft_rows = peer
        .read_rows_where_eq("tasks", "done", json!(false))
        .unwrap();
    assert_eq!(draft_rows.len(), 2);
    assert!(draft_rows.iter().any(|row| row.id == "task-main"));
    assert!(draft_rows.iter().any(|row| row.id == "task-draft"));

    alice
        .update_row(
            "tasks",
            "task-draft",
            BTreeMap::from([("done".to_owned(), json!(true))]),
        )
        .unwrap();
    peer.apply_bundle(
        &alice
            .export_query_where_eq("tasks", "done", json!(false))
            .unwrap(),
    )
    .unwrap();

    let draft_rows = peer
        .read_rows_where_eq("tasks", "done", json!(false))
        .unwrap();
    assert_eq!(draft_rows.len(), 1);
    assert_eq!(draft_rows[0].id, "task-main");
    peer.checkout_branch("main").unwrap();
    let main_rows = peer
        .read_rows_where_eq("tasks", "done", json!(false))
        .unwrap();
    assert_eq!(main_rows.len(), 1);
    assert_eq!(main_rows[0].id, "task-main");
}

#[test]
fn query_predicate_reads_survive_bundle_serialization() {
    let schema = SchemaDef::new().table("tasks", |table| {
        table.text("title");
        table.bool("done");
    });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema).unwrap();
    alice
        .insert_row(
            "tasks",
            "task-1",
            BTreeMap::from([
                ("title".to_owned(), json!("Open")),
                ("done".to_owned(), json!(false)),
            ]),
        )
        .unwrap();

    let bundle = alice
        .export_query_where_eq("tasks", "done", json!(false))
        .unwrap();
    let encoded = serde_json::to_string(&bundle).unwrap();
    let decoded: mini_jazz_sqlite::sync::Bundle = serde_json::from_str(&encoded).unwrap();

    assert_eq!(decoded.query_reads, bundle.query_reads);
}

#[test]
fn generic_equality_query_lowers_public_ref_ids_to_physical_row_ids() {
    let schema = SchemaDef::new()
        .table("projects", |table| {
            table.text("title");
        })
        .table("tasks", |table| {
            table.text("title");
            table.ref_("project", "projects");
        });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema).unwrap();

    alice
        .insert_row(
            "projects",
            "project-1",
            BTreeMap::from([("title".to_owned(), json!("Project 1"))]),
        )
        .unwrap();
    alice
        .insert_row(
            "projects",
            "project-2",
            BTreeMap::from([("title".to_owned(), json!("Project 2"))]),
        )
        .unwrap();
    alice
        .insert_row(
            "tasks",
            "task-1",
            BTreeMap::from([
                ("title".to_owned(), json!("First")),
                ("project".to_owned(), json!("project-1")),
            ]),
        )
        .unwrap();
    alice
        .insert_row(
            "tasks",
            "task-2",
            BTreeMap::from([
                ("title".to_owned(), json!("Second")),
                ("project".to_owned(), json!("project-2")),
            ]),
        )
        .unwrap();

    let rows = alice
        .read_rows_where_eq("tasks", "project", json!("project-2"))
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, "task-2");
    assert_eq!(rows[0].values["project"], json!("project-2"));
}

#[test]
fn generic_update_records_update_op_and_syncs_current_value() {
    let schema = support::notes_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut peer =
        Runtime::open_with_schema(Storage::Memory, "alice-peer-node", "alice", schema).unwrap();

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
    let update_tx = alice
        .update_row(
            "notes",
            "note-1",
            BTreeMap::from([
                ("body".to_owned(), json!("Updated")),
                ("pinned".to_owned(), json!(true)),
            ]),
        )
        .unwrap();

    let bundle = alice.export_table_history("notes").unwrap();
    let update_record = bundle
        .history
        .iter()
        .find(|record| record.tx_id == update_tx)
        .unwrap();
    assert_eq!(update_record.op, 2);
    assert_eq!(update_record.values["body"], json!("Updated"));

    peer.apply_bundle(&bundle).unwrap();
    let rows = peer.read_rows("notes").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values["body"], json!("Updated"));
    assert_eq!(
        peer.transaction_write_rows(&update_tx).unwrap(),
        vec![("notes".to_owned(), "note-1".to_owned())]
    );
}
