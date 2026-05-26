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
