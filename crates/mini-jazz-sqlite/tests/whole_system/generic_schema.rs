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
fn writes_reject_unknown_fields_instead_of_dropping_them() {
    let schema = support::notes_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema).unwrap();

    let insert_err = alice
        .insert_row(
            "notes",
            "note-1",
            BTreeMap::from([
                ("body".to_owned(), json!("Precise writes")),
                ("pinned".to_owned(), json!(false)),
                ("not_in_schema".to_owned(), json!("must not vanish")),
            ]),
        )
        .unwrap_err();
    assert!(insert_err
        .to_string()
        .contains("unknown field not_in_schema on table notes"));
    assert!(alice.read_rows("notes").unwrap().is_empty());

    alice
        .insert_row(
            "notes",
            "note-1",
            BTreeMap::from([
                ("body".to_owned(), json!("Precise writes")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    let update_err = alice
        .update_row(
            "notes",
            "note-1",
            BTreeMap::from([("not_in_schema".to_owned(), json!("must not vanish"))]),
        )
        .unwrap_err();

    assert!(update_err
        .to_string()
        .contains("unknown field not_in_schema on table notes"));
    assert_eq!(
        alice.read_rows("notes").unwrap()[0].values["body"],
        json!("Precise writes")
    );
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
        .query(support::eq_query("notes", "id", json!("note-public-id")))
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, "note-public-id");
    assert!(alice
        .query(support::eq_query("notes", "id", json!(7)))
        .unwrap_err()
        .to_string()
        .contains("query system field expects a string"));
}

#[test]
fn id_magic_field_query_scope_syncs_and_repairs_delete() {
    let schema = support::notes_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut peer =
        Runtime::open_with_schema(Storage::Memory, "alice-peer-node", "alice", schema).unwrap();

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
    peer.apply_bundle(
        &alice
            .export_query_where_eq("notes", "id", json!("note-public-id"))
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        peer.query(support::eq_query("notes", "id", json!("note-public-id")))
            .unwrap()
            .len(),
        1
    );

    alice.delete_row("notes", "note-public-id").unwrap();
    peer.apply_bundle(
        &alice
            .export_query_where_eq("notes", "id", json!("note-public-id"))
            .unwrap(),
    )
    .unwrap();
    assert!(peer
        .query(support::eq_query("notes", "id", json!("note-public-id")))
        .unwrap()
        .is_empty());
}

#[test]
fn id_in_query_matches_and_syncs_selected_rows() {
    let schema = support::notes_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut peer =
        Runtime::open_with_schema(Storage::Memory, "peer-node", "alice", schema).unwrap();

    for (id, body) in [
        ("note-1", "First selected"),
        ("note-2", "Not selected"),
        ("note-3", "Third selected"),
    ] {
        alice
            .insert_row(
                "notes",
                id,
                BTreeMap::from([
                    ("body".to_owned(), json!(body)),
                    ("pinned".to_owned(), json!(false)),
                ]),
            )
            .unwrap();
    }

    let local_rows = alice
        .read_rows_where_in("notes", "id", vec![json!("note-1"), json!("note-3")])
        .unwrap();
    let mut local_ids = local_rows
        .iter()
        .map(|row| row.id.as_str())
        .collect::<Vec<_>>();
    local_ids.sort();
    assert_eq!(local_ids, vec!["note-1", "note-3"]);

    let bundle = alice
        .export_query_where_in("notes", "id", vec![json!("note-1"), json!("note-3")])
        .unwrap();
    assert_eq!(bundle.query_reads[0].op, "in");
    peer.apply_bundle(&bundle).unwrap();

    let peer_rows = peer.read_rows("notes").unwrap();
    let mut peer_ids = peer_rows
        .iter()
        .map(|row| row.id.as_str())
        .collect::<Vec<_>>();
    peer_ids.sort();
    assert_eq!(peer_ids, vec!["note-1", "note-3"]);

    alice.delete_row("notes", "note-1").unwrap();
    peer.apply_bundle(
        &alice
            .export_query_where_in("notes", "id", vec![json!("note-1"), json!("note-3")])
            .unwrap(),
    )
    .unwrap();

    assert_eq!(
        peer.read_rows("notes")
            .unwrap()
            .iter()
            .map(|row| row.id.as_str())
            .collect::<Vec<_>>(),
        vec!["note-3"]
    );
}

#[test]
fn durable_id_in_query_refreshes_deleted_selected_row_after_restart() {
    let dir = tempdir().unwrap();
    let worker_path = dir.path().join("id-in-query-worker.sqlite");
    let schema = support::notes_schema();
    let mut upstream =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();

    for (id, body) in [
        ("note-1", "First selected"),
        ("note-2", "Not selected"),
        ("note-3", "Third selected"),
    ] {
        upstream
            .insert_row(
                "notes",
                id,
                BTreeMap::from([
                    ("body".to_owned(), json!(body)),
                    ("pinned".to_owned(), json!(false)),
                ]),
            )
            .unwrap();
    }

    let query_reads: Vec<QueryReadRecord>;
    {
        let mut worker = Runtime::open_with_schema(
            Storage::File(worker_path.clone()),
            "worker",
            "alice",
            schema.clone(),
        )
        .unwrap();
        let bundle = upstream
            .export_query_where_in("notes", "id", vec![json!("note-1"), json!("note-3")])
            .unwrap();
        query_reads = bundle.query_reads.clone();
        worker.apply_bundle(&bundle).unwrap();
        assert_eq!(worker.observed_query_reads().unwrap()[0].field, "id");
        assert_eq!(worker.read_rows("notes").unwrap().len(), 2);
    }

    upstream.delete_row("notes", "note-1").unwrap();

    let mut reopened = Runtime::open_with_schema(
        Storage::File(worker_path),
        "worker-reopened",
        "alice",
        schema,
    )
    .unwrap();
    for refresh in upstream.export_query_read_refreshes(&query_reads).unwrap() {
        reopened.apply_bundle(&refresh).unwrap();
    }

    assert_eq!(
        reopened
            .read_rows("notes")
            .unwrap()
            .iter()
            .map(|row| row.id.as_str())
            .collect::<Vec<_>>(),
        vec!["note-3"]
    );
}

#[test]
fn schema_field_in_query_matches_and_syncs_selected_rows() {
    let schema = support::notes_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut peer =
        Runtime::open_with_schema(Storage::Memory, "peer-node", "alice", schema).unwrap();

    for (id, pinned) in [
        ("note-a", true),
        ("note-b", false),
        ("note-c", true),
        ("note-d", false),
    ] {
        alice
            .insert_row(
                "notes",
                id,
                BTreeMap::from([
                    ("body".to_owned(), json!(id)),
                    ("pinned".to_owned(), json!(pinned)),
                ]),
            )
            .unwrap();
    }

    let local_rows = alice
        .read_rows_where_in("notes", "body", vec![json!("note-a"), json!("note-c")])
        .unwrap();
    assert_eq!(local_rows.len(), 2);

    peer.apply_bundle(
        &alice
            .export_query_where_in("notes", "body", vec![json!("note-a"), json!("note-c")])
            .unwrap(),
    )
    .unwrap();
    let mut peer_ids = peer
        .read_rows("notes")
        .unwrap()
        .into_iter()
        .map(|row| row.id)
        .collect::<Vec<_>>();
    peer_ids.sort();
    assert_eq!(peer_ids, vec!["note-a", "note-c"]);

    alice
        .update_row(
            "notes",
            "note-a",
            BTreeMap::from([("body".to_owned(), json!("note-a-left"))]),
        )
        .unwrap();
    peer.apply_bundle(
        &alice
            .export_query_where_in("notes", "body", vec![json!("note-a"), json!("note-c")])
            .unwrap(),
    )
    .unwrap();

    let rows = peer
        .read_rows_where_in("notes", "body", vec![json!("note-a"), json!("note-c")])
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, "note-c");
}

#[test]
fn durable_in_query_read_refresh_repairs_row_that_left_value_set_after_restart() {
    let dir = tempdir().unwrap();
    let worker_path = dir.path().join("in-query-worker.sqlite");
    let schema = support::notes_schema();
    let mut upstream =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();

    for (id, body) in [
        ("note-a", "alpha"),
        ("note-b", "beta"),
        ("note-c", "outside"),
    ] {
        upstream
            .insert_row(
                "notes",
                id,
                BTreeMap::from([
                    ("body".to_owned(), json!(body)),
                    ("pinned".to_owned(), json!(false)),
                ]),
            )
            .unwrap();
    }

    let query_reads: Vec<QueryReadRecord>;
    {
        let mut worker = Runtime::open_with_schema(
            Storage::File(worker_path.clone()),
            "worker",
            "alice",
            schema.clone(),
        )
        .unwrap();
        let bundle = upstream
            .export_query_where_in("notes", "body", vec![json!("alpha"), json!("beta")])
            .unwrap();
        query_reads = bundle.query_reads.clone();
        worker.apply_bundle(&bundle).unwrap();
        assert_eq!(worker.observed_query_reads().unwrap()[0].op, "in");
        assert_eq!(
            worker
                .read_rows_where_in("notes", "body", vec![json!("alpha"), json!("beta")])
                .unwrap()
                .len(),
            2
        );
    }

    upstream
        .update_row(
            "notes",
            "note-a",
            BTreeMap::from([("body".to_owned(), json!("outside now"))]),
        )
        .unwrap();

    let mut reopened = Runtime::open_with_schema(
        Storage::File(worker_path),
        "worker-reopened",
        "alice",
        schema,
    )
    .unwrap();
    for refresh in upstream.export_query_read_refreshes(&query_reads).unwrap() {
        reopened.apply_bundle(&refresh).unwrap();
    }

    let rows = reopened
        .read_rows_where_in("notes", "body", vec![json!("alpha"), json!("beta")])
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, "note-b");
}

#[test]
fn created_by_magic_field_query_matches_creator_user() {
    let schema = support::notes_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut bob = Runtime::open_with_schema(Storage::Memory, "bob-node", "bob", schema).unwrap();

    alice
        .insert_row(
            "notes",
            "note-alice",
            BTreeMap::from([
                ("body".to_owned(), json!("Alice")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    bob.insert_row(
        "notes",
        "note-bob",
        BTreeMap::from([
            ("body".to_owned(), json!("Bob")),
            ("pinned".to_owned(), json!(true)),
        ]),
    )
    .unwrap();
    alice
        .apply_bundle(&bob.export_table_history("notes").unwrap())
        .unwrap();

    let rows = alice
        .query(support::eq_query("notes", "$createdBy", json!("bob")))
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, "note-bob");
    assert!(alice
        .query(support::eq_query("notes", "$createdBy", json!(true)))
        .unwrap_err()
        .to_string()
        .contains("query system field expects a string"));
}

#[test]
fn created_by_magic_field_query_scope_syncs_and_repairs_delete() {
    let schema = support::notes_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut peer =
        Runtime::open_with_schema(Storage::Memory, "alice-peer-node", "alice", schema).unwrap();

    alice
        .insert_row(
            "notes",
            "note-alice",
            BTreeMap::from([
                ("body".to_owned(), json!("Alice")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    peer.apply_bundle(
        &alice
            .export_query_where_eq("notes", "$createdBy", json!("alice"))
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        peer.query(support::eq_query("notes", "$createdBy", json!("alice")))
            .unwrap()
            .len(),
        1
    );

    alice.delete_row("notes", "note-alice").unwrap();
    peer.apply_bundle(
        &alice
            .export_query_where_eq("notes", "$createdBy", json!("alice"))
            .unwrap(),
    )
    .unwrap();
    assert!(peer
        .query(support::eq_query("notes", "$createdBy", json!("alice")))
        .unwrap()
        .is_empty());
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
fn durable_contains_query_read_refresh_repairs_row_that_left_predicate_after_restart() {
    let dir = tempdir().unwrap();
    let worker_path = dir.path().join("contains-query-worker.sqlite");
    let schema = support::notes_schema();
    let mut upstream =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();

    upstream
        .insert_row(
            "notes",
            "note-1",
            BTreeMap::from([
                ("body".to_owned(), json!("contains target")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .unwrap();

    let query_reads: Vec<QueryReadRecord>;
    {
        let mut worker = Runtime::open_with_schema(
            Storage::File(worker_path.clone()),
            "worker",
            "alice",
            schema.clone(),
        )
        .unwrap();
        let bundle = upstream
            .export_query_where_contains("notes", "body", "target")
            .unwrap();
        query_reads = bundle.query_reads.clone();
        worker.apply_bundle(&bundle).unwrap();
        assert_eq!(worker.observed_query_reads().unwrap()[0].op, "contains");
        assert_eq!(
            worker
                .read_rows_where_contains("notes", "body", "target")
                .unwrap()
                .len(),
            1
        );
    }

    upstream
        .update_row(
            "notes",
            "note-1",
            BTreeMap::from([("body".to_owned(), json!("moved elsewhere"))]),
        )
        .unwrap();

    let mut reopened = Runtime::open_with_schema(
        Storage::File(worker_path),
        "worker-reopened",
        "alice",
        schema,
    )
    .unwrap();
    for refresh in upstream.export_query_read_refreshes(&query_reads).unwrap() {
        reopened.apply_bundle(&refresh).unwrap();
    }

    assert!(reopened
        .read_rows_where_contains("notes", "body", "target")
        .unwrap()
        .is_empty());
}

#[test]
fn generic_top_created_at_query_scope_refresh_replaces_displaced_boundary_row() {
    let schema = support::notes_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut peer =
        Runtime::open_with_schema(Storage::Memory, "peer-node", "alice", schema).unwrap();

    alice
        .insert_row(
            "notes",
            "note-old",
            BTreeMap::from([
                ("body".to_owned(), json!("old boundary")),
                ("pinned".to_owned(), json!(true)),
            ]),
        )
        .unwrap();
    std::thread::sleep(std::time::Duration::from_millis(2));
    alice
        .insert_row(
            "notes",
            "note-middle",
            BTreeMap::from([
                ("body".to_owned(), json!("middle")),
                ("pinned".to_owned(), json!(true)),
            ]),
        )
        .unwrap();

    peer.apply_bundle(
        &alice
            .export_query(support::top_created_query(
                "notes",
                "pinned",
                json!(true),
                2,
            ))
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        peer.query(support::top_created_query(
            "notes",
            "pinned",
            json!(true),
            2,
        ))
        .unwrap()
        .iter()
        .map(|row| row.id.as_str())
        .collect::<Vec<_>>(),
        vec!["note-middle", "note-old"]
    );

    std::thread::sleep(std::time::Duration::from_millis(2));
    alice
        .insert_row(
            "notes",
            "note-new",
            BTreeMap::from([
                ("body".to_owned(), json!("newest")),
                ("pinned".to_owned(), json!(true)),
            ]),
        )
        .unwrap();
    peer.apply_bundle(
        &alice
            .export_query(support::top_created_query(
                "notes",
                "pinned",
                json!(true),
                2,
            ))
            .unwrap(),
    )
    .unwrap();

    assert_eq!(
        peer.query(support::top_created_query(
            "notes",
            "pinned",
            json!(true),
            3,
        ))
        .unwrap()
        .iter()
        .map(|row| row.id.as_str())
        .collect::<Vec<_>>(),
        vec!["note-new", "note-middle"]
    );
}

#[test]
fn built_query_scope_export_accepts_multiple_filters() {
    let schema = support::notes_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut peer =
        Runtime::open_with_schema(Storage::Memory, "peer-node", "alice", schema).unwrap();

    alice
        .insert_row(
            "notes",
            "note-match",
            BTreeMap::from([
                ("body".to_owned(), json!("ship the small thing")),
                ("pinned".to_owned(), json!(true)),
            ]),
        )
        .unwrap();
    alice
        .insert_row(
            "notes",
            "note-filtered",
            BTreeMap::from([
                ("body".to_owned(), json!("ship something later")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .unwrap();

    let query = BuiltQuery::from_json_value(json!({
        "table": "notes",
        "conditions": [
            {"column": "pinned", "op": "eq", "value": true},
            {"column": "body", "op": "contains", "value": "ship"}
        ],
    }))
    .unwrap();
    assert_eq!(alice.query(query.clone()).unwrap()[0].id, "note-match");

    peer.apply_bundle(&alice.export_query(query.clone()).unwrap())
        .unwrap();

    let rows = peer.query(query.clone()).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, "note-match");

    alice
        .update_row(
            "notes",
            "note-match",
            BTreeMap::from([("pinned".to_owned(), json!(false))]),
        )
        .unwrap();

    for refresh in alice
        .export_query_read_refreshes(&peer.observed_query_reads().unwrap())
        .unwrap()
    {
        peer.apply_bundle(&refresh).unwrap();
    }

    assert!(peer.query(query).unwrap().is_empty());
}

#[test]
fn built_query_scope_refresh_removes_row_that_left_custom_order() {
    let schema = support::notes_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut peer =
        Runtime::open_with_schema(Storage::Memory, "peer-node", "alice", schema).unwrap();

    alice
        .insert_row(
            "notes",
            "note-match",
            BTreeMap::from([
                ("body".to_owned(), json!("alpha")),
                ("pinned".to_owned(), json!(true)),
            ]),
        )
        .unwrap();

    let query = BuiltQuery::from_json_value(json!({
        "table": "notes",
        "conditions": [
            {"column": "pinned", "op": "eq", "value": true}
        ],
        "orderBy": [["body", "asc"]],
    }))
    .unwrap();

    peer.apply_bundle(&alice.export_query(query.clone()).unwrap())
        .unwrap();
    assert_eq!(peer.query(query.clone()).unwrap()[0].id, "note-match");

    alice
        .update_row(
            "notes",
            "note-match",
            BTreeMap::from([("pinned".to_owned(), json!(false))]),
        )
        .unwrap();

    for refresh in alice
        .export_query_read_refreshes(&peer.observed_query_reads().unwrap())
        .unwrap()
    {
        peer.apply_bundle(&refresh).unwrap();
    }

    assert!(peer.query(query).unwrap().is_empty());
}

#[test]
fn built_query_scope_export_includes_offset_support_rows() {
    let schema = support::notes_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut peer =
        Runtime::open_with_schema(Storage::Memory, "peer-node", "alice", schema).unwrap();

    alice
        .insert_row(
            "notes",
            "note-older",
            BTreeMap::from([
                ("body".to_owned(), json!("older")),
                ("pinned".to_owned(), json!(true)),
            ]),
        )
        .unwrap();
    std::thread::sleep(std::time::Duration::from_millis(2));
    alice
        .insert_row(
            "notes",
            "note-newer",
            BTreeMap::from([
                ("body".to_owned(), json!("newer")),
                ("pinned".to_owned(), json!(true)),
            ]),
        )
        .unwrap();

    let query = BuiltQuery::from_json_value(json!({
        "table": "notes",
        "conditions": [{"column": "pinned", "op": "eq", "value": true}],
        "orderBy": [["$createdAt", "desc"]],
        "limit": 1,
        "offset": 1,
    }))
    .unwrap();
    assert_eq!(alice.query(query.clone()).unwrap()[0].id, "note-older");

    peer.apply_bundle(&alice.export_query(query.clone()).unwrap())
        .unwrap();

    let rows = peer.query(query).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, "note-older");
}

#[test]
fn generic_top_created_at_query_scope_repairs_system_predicates() {
    let schema = support::notes_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();

    alice
        .insert_row(
            "notes",
            "note-target",
            BTreeMap::from([
                ("body".to_owned(), json!("target")),
                ("pinned".to_owned(), json!(true)),
            ]),
        )
        .unwrap();

    for query in [
        support::top_created_query("notes", "id", json!("note-target"), 1),
        support::top_created_query("notes", "$createdBy", json!("alice"), 1),
    ] {
        let mut peer =
            Runtime::open_with_schema(Storage::Memory, "peer-node", "alice", schema.clone())
                .unwrap();

        peer.apply_bundle(&alice.export_query(query.clone()).unwrap())
            .unwrap();

        let rows = peer.query(query).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].id, "note-target");
    }
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
            table.read_if_created_by_user();
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
        .query(support::eq_query("tasks", "done", json!(false)))
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
        .query(support::eq_query("tasks", "done", json!(false)))
        .unwrap();
    assert_eq!(peer_rows.len(), 1);
    assert_eq!(peer_rows[0].id, "task-open");
}

#[test]
fn query_scope_bundle_dedupes_shared_policy_dependency_history() {
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
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut peer =
        Runtime::open_with_schema(Storage::Memory, "alice-peer-node", "alice", schema).unwrap();

    alice
        .insert_row(
            "projects",
            "project-1",
            BTreeMap::from([("title".to_owned(), json!("Shared project"))]),
        )
        .unwrap();
    for id in ["task-a", "task-b"] {
        alice
            .insert_row(
                "tasks",
                id,
                BTreeMap::from([
                    ("title".to_owned(), json!(id)),
                    ("done".to_owned(), json!(false)),
                    ("project".to_owned(), json!("project-1")),
                ]),
            )
            .unwrap();
    }

    let bundle = alice
        .export_query_where_eq("tasks", "done", json!(false))
        .unwrap();
    assert_eq!(
        bundle
            .history
            .iter()
            .filter(|record| record.table == "tasks")
            .count(),
        2
    );
    assert_eq!(
        bundle
            .history
            .iter()
            .filter(|record| record.table == "projects" && record.row_id == "project-1")
            .count(),
        1
    );

    peer.apply_bundle(&bundle).unwrap();
    let peer_rows = peer
        .query(support::eq_query("tasks", "done", json!(false)))
        .unwrap();
    assert_eq!(peer_rows.len(), 2);
    assert!(peer_rows
        .iter()
        .all(|row| row.values["project"] == json!("project-1")));
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
        peer.query(support::eq_query("tasks", "done", json!(false)))
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
        .query(support::eq_query("tasks", "done", json!(false)))
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
        peer.query(support::eq_query("tasks", "done", json!(false)))
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
        .query(support::eq_query("tasks", "done", json!(false)))
        .unwrap()
        .is_empty());
}

#[test]
fn nullable_text_round_trips_and_filters_with_is_null_semantics() {
    let schema = SchemaDef::new().table("notes", |table| {
        table.text("body");
        table.optional_text("tag");
    });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema).unwrap();

    alice
        .insert_row(
            "notes",
            "note-1",
            BTreeMap::from([
                ("body".to_owned(), json!("Tagged")),
                ("tag".to_owned(), json!("work")),
            ]),
        )
        .unwrap();
    alice
        .insert_row(
            "notes",
            "note-2",
            BTreeMap::from([
                ("body".to_owned(), json!("Untagged")),
                ("tag".to_owned(), json!(null)),
            ]),
        )
        .unwrap();

    let rows = alice
        .query(support::eq_query("notes", "tag", json!(null)))
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, "note-2");
    assert_eq!(rows[0].values["tag"], json!(null));
}

#[test]
fn nullable_ref_round_trips_filters_and_is_skipped_by_require_ref() {
    let schema = SchemaDef::new()
        .table("projects", |table| {
            table.text("title");
        })
        .table("todos", |table| {
            table.text("title");
            table.bool("done");
            table.optional_ref("project", "projects");
        });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema).unwrap();

    alice
        .insert_row(
            "projects",
            "project-1",
            BTreeMap::from([("title".to_owned(), json!("Project"))]),
        )
        .unwrap();
    alice
        .insert_row(
            "todos",
            "todo-linked",
            BTreeMap::from([
                ("title".to_owned(), json!("Linked")),
                ("done".to_owned(), json!(false)),
                ("project".to_owned(), json!("project-1")),
            ]),
        )
        .unwrap();
    alice
        .insert_row(
            "todos",
            "todo-floating",
            BTreeMap::from([
                ("title".to_owned(), json!("Floating")),
                ("done".to_owned(), json!(false)),
                ("project".to_owned(), json!(null)),
            ]),
        )
        .unwrap();

    let floating = alice
        .query(support::eq_query("todos", "project", json!(null)))
        .unwrap();
    assert_eq!(floating.len(), 1);
    assert_eq!(floating[0].id, "todo-floating");
    assert_eq!(floating[0].values["project"], json!(null));

    let required = alice.read_rows_require_ref("todos", "project").unwrap();
    assert_eq!(required.len(), 1);
    assert_eq!(required[0].id, "todo-linked");
}

#[test]
fn not_equal_null_filters_to_present_optional_values() {
    let schema = SchemaDef::new().table("notes", |table| {
        table.text("body");
        table.optional_text("tag");
    });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema).unwrap();

    alice
        .insert_row(
            "notes",
            "note-tagged",
            BTreeMap::from([
                ("body".to_owned(), json!("Tagged")),
                ("tag".to_owned(), json!("work")),
            ]),
        )
        .unwrap();
    alice
        .insert_row(
            "notes",
            "note-untagged",
            BTreeMap::from([
                ("body".to_owned(), json!("Untagged")),
                ("tag".to_owned(), json!(null)),
            ]),
        )
        .unwrap();

    let rows = alice
        .read_rows_where_ne("notes", "tag", json!(null))
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, "note-tagged");
}

#[test]
fn not_equal_query_scope_syncs_and_refreshes_present_optional_values() {
    let schema = SchemaDef::new().table("notes", |table| {
        table.text("body");
        table.optional_text("tag");
    });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut bob = Runtime::open_with_schema(Storage::Memory, "bob-node", "alice", schema).unwrap();

    alice
        .insert_row(
            "notes",
            "note-tagged",
            BTreeMap::from([
                ("body".to_owned(), json!("Tagged")),
                ("tag".to_owned(), json!("work")),
            ]),
        )
        .unwrap();
    alice
        .insert_row(
            "notes",
            "note-untagged",
            BTreeMap::from([
                ("body".to_owned(), json!("Untagged")),
                ("tag".to_owned(), json!(null)),
            ]),
        )
        .unwrap();

    bob.apply_bundle(
        &alice
            .export_query_where_ne("notes", "tag", json!(null))
            .unwrap(),
    )
    .unwrap();

    let rows = bob.read_rows_where_ne("notes", "tag", json!(null)).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, "note-tagged");
    assert_eq!(bob.read_rows("notes").unwrap().len(), 1);

    alice
        .update_row(
            "notes",
            "note-untagged",
            BTreeMap::from([("tag".to_owned(), json!("personal"))]),
        )
        .unwrap();
    for refresh in alice
        .export_query_read_refreshes(&bob.observed_query_reads().unwrap())
        .unwrap()
    {
        bob.apply_bundle(&refresh).unwrap();
    }

    let refreshed = bob.read_rows_where_ne("notes", "tag", json!(null)).unwrap();
    assert_eq!(refreshed.len(), 2);
    assert!(refreshed.iter().any(|row| row.id == "note-untagged"));
}

#[test]
fn durable_not_equal_null_query_read_refreshes_present_optional_values_after_restart() {
    let dir = tempdir().unwrap();
    let worker_path = dir.path().join("ne-null-query-worker.sqlite");
    let schema = SchemaDef::new().table("notes", |table| {
        table.text("body");
        table.optional_text("tag");
    });
    let mut upstream =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();

    upstream
        .insert_row(
            "notes",
            "note-tagged",
            BTreeMap::from([
                ("body".to_owned(), json!("Tagged")),
                ("tag".to_owned(), json!("work")),
            ]),
        )
        .unwrap();
    upstream
        .insert_row(
            "notes",
            "note-untagged",
            BTreeMap::from([
                ("body".to_owned(), json!("Untagged")),
                ("tag".to_owned(), json!(null)),
            ]),
        )
        .unwrap();

    let query_reads: Vec<QueryReadRecord>;
    {
        let mut worker = Runtime::open_with_schema(
            Storage::File(worker_path.clone()),
            "worker",
            "alice",
            schema.clone(),
        )
        .unwrap();
        let bundle = upstream
            .export_query_where_ne("notes", "tag", json!(null))
            .unwrap();
        query_reads = bundle.query_reads.clone();
        worker.apply_bundle(&bundle).unwrap();
        assert_eq!(worker.observed_query_reads().unwrap()[0].op, "ne");
        assert_eq!(
            worker
                .read_rows_where_ne("notes", "tag", json!(null))
                .unwrap()
                .len(),
            1
        );
    }

    upstream
        .update_row(
            "notes",
            "note-untagged",
            BTreeMap::from([("tag".to_owned(), json!("personal"))]),
        )
        .unwrap();

    let mut reopened = Runtime::open_with_schema(
        Storage::File(worker_path),
        "worker-reopened",
        "alice",
        schema,
    )
    .unwrap();
    for refresh in upstream.export_query_read_refreshes(&query_reads).unwrap() {
        reopened.apply_bundle(&refresh).unwrap();
    }

    let refreshed = reopened
        .read_rows_where_ne("notes", "tag", json!(null))
        .unwrap();
    assert_eq!(refreshed.len(), 2);
    assert!(refreshed.iter().any(|row| row.id == "note-untagged"));
}

#[test]
fn created_by_not_equal_query_scope_syncs_and_refreshes() {
    let schema = SchemaDef::new().table("notes", |table| {
        table.text("body");
        table.bool("pinned");
    });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut bob =
        Runtime::open_with_schema(Storage::Memory, "bob-node", "bob", schema.clone()).unwrap();
    let mut peer =
        Runtime::open_with_schema(Storage::Memory, "peer-node", "alice", schema).unwrap();

    alice
        .insert_row(
            "notes",
            "note-alice",
            BTreeMap::from([
                ("body".to_owned(), json!("Alice")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    bob.insert_row(
        "notes",
        "note-bob",
        BTreeMap::from([
            ("body".to_owned(), json!("Bob")),
            ("pinned".to_owned(), json!(false)),
        ]),
    )
    .unwrap();
    alice
        .apply_bundle(&bob.export_table_history("notes").unwrap())
        .unwrap();

    peer.apply_bundle(
        &alice
            .export_query_where_ne("notes", "$createdBy", json!("alice"))
            .unwrap(),
    )
    .unwrap();
    let rows = peer
        .read_rows_where_ne("notes", "$createdBy", json!("alice"))
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, "note-bob");

    alice
        .update_row(
            "notes",
            "note-bob",
            BTreeMap::from([("pinned".to_owned(), json!(true))]),
        )
        .unwrap();
    for refresh in alice
        .export_query_read_refreshes(&peer.observed_query_reads().unwrap())
        .unwrap()
    {
        peer.apply_bundle(&refresh).unwrap();
    }

    let refreshed = peer
        .read_rows_where_ne("notes", "$createdBy", json!("alice"))
        .unwrap();
    assert_eq!(refreshed.len(), 1);
    assert_eq!(refreshed[0].id, "note-bob");
    assert_eq!(refreshed[0].values["pinned"], json!(true));
}

#[test]
fn durable_created_by_not_equal_query_refreshes_after_restart() {
    let dir = tempdir().unwrap();
    let worker_path = dir.path().join("created-by-ne-worker.sqlite");
    let schema = SchemaDef::new().table("notes", |table| {
        table.text("body");
        table.bool("pinned");
    });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut bob =
        Runtime::open_with_schema(Storage::Memory, "bob-node", "bob", schema.clone()).unwrap();

    alice
        .insert_row(
            "notes",
            "note-alice",
            BTreeMap::from([
                ("body".to_owned(), json!("Alice")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    bob.insert_row(
        "notes",
        "note-bob",
        BTreeMap::from([
            ("body".to_owned(), json!("Bob")),
            ("pinned".to_owned(), json!(false)),
        ]),
    )
    .unwrap();
    alice
        .apply_bundle(&bob.export_table_history("notes").unwrap())
        .unwrap();

    let query_reads: Vec<QueryReadRecord>;
    {
        let mut worker = Runtime::open_with_schema(
            Storage::File(worker_path.clone()),
            "worker",
            "alice",
            schema.clone(),
        )
        .unwrap();
        let bundle = alice
            .export_query_where_ne("notes", "$createdBy", json!("alice"))
            .unwrap();
        query_reads = bundle.query_reads.clone();
        worker.apply_bundle(&bundle).unwrap();
        assert_eq!(
            worker.observed_query_reads().unwrap()[0].field,
            "$createdBy"
        );
        assert_eq!(
            worker
                .read_rows_where_ne("notes", "$createdBy", json!("alice"))
                .unwrap()
                .len(),
            1
        );
    }

    alice
        .update_row(
            "notes",
            "note-bob",
            BTreeMap::from([("pinned".to_owned(), json!(true))]),
        )
        .unwrap();

    let mut reopened = Runtime::open_with_schema(
        Storage::File(worker_path),
        "worker-reopened",
        "alice",
        schema,
    )
    .unwrap();
    for refresh in alice.export_query_read_refreshes(&query_reads).unwrap() {
        reopened.apply_bundle(&refresh).unwrap();
    }

    let refreshed = reopened
        .read_rows_where_ne("notes", "$createdBy", json!("alice"))
        .unwrap();
    assert_eq!(refreshed.len(), 1);
    assert_eq!(refreshed[0].id, "note-bob");
    assert_eq!(refreshed[0].values["pinned"], json!(true));
}

#[test]
fn created_by_not_equal_query_scope_repairs_deleted_matching_row() {
    let schema = SchemaDef::new().table("notes", |table| {
        table.text("body");
        table.bool("pinned");
    });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut bob =
        Runtime::open_with_schema(Storage::Memory, "bob-node", "bob", schema.clone()).unwrap();
    let mut peer =
        Runtime::open_with_schema(Storage::Memory, "peer-node", "alice", schema).unwrap();

    bob.insert_row(
        "notes",
        "note-bob",
        BTreeMap::from([
            ("body".to_owned(), json!("Bob")),
            ("pinned".to_owned(), json!(false)),
        ]),
    )
    .unwrap();
    alice
        .apply_bundle(&bob.export_table_history("notes").unwrap())
        .unwrap();
    peer.apply_bundle(
        &alice
            .export_query_where_ne("notes", "$createdBy", json!("alice"))
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        peer.read_rows_where_ne("notes", "$createdBy", json!("alice"))
            .unwrap()
            .len(),
        1
    );

    alice.delete_row("notes", "note-bob").unwrap();
    for refresh in alice
        .export_query_read_refreshes(&peer.observed_query_reads().unwrap())
        .unwrap()
    {
        peer.apply_bundle(&refresh).unwrap();
    }

    assert!(peer
        .read_rows_where_ne("notes", "$createdBy", json!("alice"))
        .unwrap()
        .is_empty());
}

#[test]
fn id_not_equal_query_scope_syncs_and_repairs_deleted_row() {
    let schema = SchemaDef::new().table("notes", |table| {
        table.text("body");
        table.bool("pinned");
    });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut peer =
        Runtime::open_with_schema(Storage::Memory, "peer-node", "alice", schema).unwrap();

    alice
        .insert_row(
            "notes",
            "note-keep",
            BTreeMap::from([
                ("body".to_owned(), json!("Keep")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    alice
        .insert_row(
            "notes",
            "note-excluded",
            BTreeMap::from([
                ("body".to_owned(), json!("Excluded")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .unwrap();

    peer.apply_bundle(
        &alice
            .export_query_where_ne("notes", "id", json!("note-excluded"))
            .unwrap(),
    )
    .unwrap();
    let rows = peer
        .read_rows_where_ne("notes", "id", json!("note-excluded"))
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, "note-keep");

    alice.delete_row("notes", "note-keep").unwrap();
    for refresh in alice
        .export_query_read_refreshes(&peer.observed_query_reads().unwrap())
        .unwrap()
    {
        peer.apply_bundle(&refresh).unwrap();
    }

    assert!(peer
        .read_rows_where_ne("notes", "id", json!("note-excluded"))
        .unwrap()
        .is_empty());
}

#[test]
fn insert_applies_declared_defaults_for_omitted_fields() {
    let schema = SchemaDef::new().table("notes", |table| {
        table.text("body");
        table.text_default("status", "draft");
        table.bool_default("pinned", false);
    });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema).unwrap();

    alice
        .insert_row(
            "notes",
            "note-defaults",
            BTreeMap::from([("body".to_owned(), json!("Needs defaults"))]),
        )
        .unwrap();

    let rows = alice.read_rows("notes").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values["status"], json!("draft"));
    assert_eq!(rows[0].values["pinned"], json!(false));
}

#[test]
fn explicit_null_optional_field_is_not_overwritten_by_default() {
    let schema = SchemaDef::new().table("notes", |table| {
        table.text("body");
        table.optional_text("tag");
        table.text_default("status", "draft");
    });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema).unwrap();

    alice
        .insert_row(
            "notes",
            "note-null",
            BTreeMap::from([
                ("body".to_owned(), json!("Explicit null")),
                ("tag".to_owned(), json!(null)),
            ]),
        )
        .unwrap();

    let rows = alice.read_rows("notes").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values["tag"], json!(null));
    assert_eq!(rows[0].values["status"], json!("draft"));
}

#[test]
fn declared_defaults_sync_and_rebuild_as_history_values() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("defaults.sqlite");
    let schema = SchemaDef::new().table("notes", |table| {
        table.text("body");
        table.text_default("status", "draft");
        table.bool_default("pinned", false);
    });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut peer = Runtime::open_with_schema(
        Storage::File(path.clone()),
        "peer-node",
        "alice",
        schema.clone(),
    )
    .unwrap();

    alice
        .insert_row(
            "notes",
            "note-defaults",
            BTreeMap::from([("body".to_owned(), json!("Needs defaults"))]),
        )
        .unwrap();

    peer.apply_bundle(&alice.export_table_history("notes").unwrap())
        .unwrap();
    let rows = peer.read_rows("notes").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values["status"], json!("draft"));
    assert_eq!(rows[0].values["pinned"], json!(false));
    drop(peer);

    let reopened =
        Runtime::open_with_schema(Storage::File(path), "peer-node", "alice", schema).unwrap();
    let rebuilt = reopened.read_rows("notes").unwrap();
    assert_eq!(rebuilt.len(), 1);
    assert_eq!(rebuilt[0].values["status"], json!("draft"));
    assert_eq!(rebuilt[0].values["pinned"], json!(false));
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
        peer.query(support::eq_query("tasks", "done", json!(false)))
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
        .query(support::eq_query("tasks", "done", json!(false)))
        .unwrap()
        .is_empty());
}

#[test]
fn empty_equality_query_scope_later_delivers_inserted_match_without_table_replication() {
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
            "task-closed",
            BTreeMap::from([
                ("title".to_owned(), json!("Closed")),
                ("done".to_owned(), json!(true)),
            ]),
        )
        .unwrap();
    let initial = alice
        .export_query_where_eq("tasks", "done", json!(false))
        .unwrap();
    assert!(initial.history.is_empty());
    assert_eq!(initial.query_reads.len(), 1);
    peer.apply_bundle(&initial).unwrap();
    assert!(peer
        .query(support::eq_query("tasks", "done", json!(false)))
        .unwrap()
        .is_empty());
    assert_eq!(peer.observed_query_reads().unwrap(), initial.query_reads);

    alice
        .insert_row(
            "tasks",
            "task-open",
            BTreeMap::from([
                ("title".to_owned(), json!("Opened later")),
                ("done".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    for refresh in alice
        .export_query_read_refreshes(&peer.observed_query_reads().unwrap())
        .unwrap()
    {
        assert!(refresh
            .history
            .iter()
            .any(|record| record.table == "tasks" && record.row_id == "task-open"));
        assert!(!refresh
            .history
            .iter()
            .any(|record| record.table == "tasks" && record.row_id == "task-closed"));
        peer.apply_bundle(&refresh).unwrap();
    }

    let rows = peer
        .query(support::eq_query("tasks", "done", json!(false)))
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, "task-open");
}

#[test]
fn equality_query_scope_resync_removes_row_hidden_by_policy_dependency_change() {
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
        peer.query(support::eq_query("tasks", "done", json!(false)))
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
        .query(support::eq_query("tasks", "done", json!(false)))
        .unwrap()
        .is_empty());

    peer.apply_bundle(
        &alice
            .export_query_where_eq("tasks", "done", json!(false))
            .unwrap(),
    )
    .unwrap();

    assert!(peer
        .query(support::eq_query("tasks", "done", json!(false)))
        .unwrap()
        .is_empty());
}

#[test]
fn durable_query_read_refresh_repairs_policy_dependency_change_after_restart() {
    let dir = tempdir().unwrap();
    let worker_path = dir.path().join("policy-query-worker.sqlite");
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
    let mut upstream =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut bob =
        Runtime::open_with_schema(Storage::Memory, "bob-node", "bob", schema.clone()).unwrap();

    upstream
        .insert_row(
            "projects",
            "project-alice",
            BTreeMap::from([("title".to_owned(), json!("Alice project"))]),
        )
        .unwrap();
    upstream
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

    let query_reads: Vec<QueryReadRecord>;
    {
        let mut worker = Runtime::open_with_schema(
            Storage::File(worker_path.clone()),
            "worker",
            "alice",
            schema.clone(),
        )
        .unwrap();
        let bundle = upstream
            .export_query_where_eq("tasks", "done", json!(false))
            .unwrap();
        query_reads = bundle.query_reads.clone();
        worker.apply_bundle(&bundle).unwrap();
        assert_eq!(
            worker
                .query(support::eq_query("tasks", "done", json!(false)))
                .unwrap()
                .len(),
            1
        );
    }

    bob.insert_row(
        "projects",
        "project-bob",
        BTreeMap::from([("title".to_owned(), json!("Bob project"))]),
    )
    .unwrap();
    upstream
        .apply_bundle(&bob.export_table_history("projects").unwrap())
        .unwrap();
    upstream
        .update_row(
            "tasks",
            "task-1",
            BTreeMap::from([("project".to_owned(), json!("project-bob"))]),
        )
        .unwrap();

    let mut reopened = Runtime::open_with_schema(
        Storage::File(worker_path),
        "worker-reopened",
        "alice",
        schema,
    )
    .unwrap();
    for refresh in upstream.export_query_read_refreshes(&query_reads).unwrap() {
        reopened.apply_bundle(&refresh).unwrap();
    }

    assert!(reopened
        .query(support::eq_query("tasks", "done", json!(false)))
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
        .query(support::eq_query("tasks", "done", json!(false)))
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
        .query(support::eq_query("tasks", "done", json!(false)))
        .unwrap()
        .is_empty());
}

#[test]
fn branch_not_equal_query_scope_resync_repairs_row_that_left_predicate() {
    let schema = SchemaDef::new().table("tasks", |table| {
        table.text("title");
        table.optional_text("tag");
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
                ("title".to_owned(), json!("Tagged draft")),
                ("tag".to_owned(), json!("work")),
            ]),
        )
        .unwrap();
    peer.apply_bundle(
        &alice
            .export_query_where_ne("tasks", "tag", json!(null))
            .unwrap(),
    )
    .unwrap();
    peer.checkout_branch("draft").unwrap();
    assert_eq!(
        peer.read_rows_where_ne("tasks", "tag", json!(null))
            .unwrap()
            .len(),
        1
    );

    alice
        .update_row(
            "tasks",
            "task-1",
            BTreeMap::from([("tag".to_owned(), json!(null))]),
        )
        .unwrap();
    peer.apply_bundle(
        &alice
            .export_query_where_ne("tasks", "tag", json!(null))
            .unwrap(),
    )
    .unwrap();

    assert!(peer
        .read_rows_where_ne("tasks", "tag", json!(null))
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
        peer.query(support::eq_query("tasks", "done", json!(false)))
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
        .query(support::eq_query("tasks", "done", json!(false)))
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
        .query(support::eq_query("tasks", "done", json!(false)))
        .unwrap();
    assert_eq!(draft_rows.len(), 1);
    assert_eq!(draft_rows[0].id, "task-main");
    peer.checkout_branch("main").unwrap();
    let main_rows = peer
        .query(support::eq_query("tasks", "done", json!(false)))
        .unwrap();
    assert_eq!(main_rows.len(), 1);
    assert_eq!(main_rows[0].id, "task-main");
}

#[test]
fn query_predicate_reads_survive_bundle_serialization() {
    let schema = support::notes_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema).unwrap();
    alice
        .insert_row(
            "notes",
            "note-1",
            BTreeMap::from([
                ("body".to_owned(), json!("Open predicate")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .unwrap();

    let bundle = alice
        .export_query_where_contains("notes", "body", "predicate")
        .unwrap();
    let encoded = mini_jazz_sqlite::sync::encode_bundle(&bundle).unwrap();
    let decoded = mini_jazz_sqlite::sync::decode_bundle(&encoded).unwrap();

    assert_eq!(decoded.query_reads, bundle.query_reads);
    assert_eq!(decoded.query_reads[0].op, "contains");
}

#[test]
fn native_query_read_records_roundtrip_equality_operator() {
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
                ("title".to_owned(), json!("Native bundle")),
                ("done".to_owned(), json!(false)),
            ]),
        )
        .unwrap();

    let bundle = alice
        .export_query_where_eq("tasks", "done", json!(false))
        .unwrap();
    let encoded = mini_jazz_sqlite::sync::encode_bundle(&bundle).unwrap();
    let decoded = mini_jazz_sqlite::sync::decode_bundle(&encoded).unwrap();

    assert_eq!(decoded.query_reads, bundle.query_reads);
    assert_eq!(decoded.query_reads[0].op, "eq");
}

#[test]
fn older_query_read_records_without_operator_decode_as_equality() {
    let encoded = r#"{
        "branches": [],
        "txs": [],
        "reads": [],
        "query_reads": [
            {
                "branch_id": "main",
                "table": "tasks",
                "field": "done",
                "value": false
            }
        ],
        "history": []
    }"#;

    let decoded: mini_jazz_sqlite::sync::Bundle = serde_json::from_str(encoded).unwrap();

    assert_eq!(decoded.query_reads[0].op, "eq");
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
        .query(support::eq_query("tasks", "project", json!("project-2")))
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, "task-2");
    assert_eq!(rows[0].values["project"], json!("project-2"));
}

#[test]
fn generic_ref_field_order_uses_public_ref_ids() {
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
            "project-z",
            BTreeMap::from([("title".to_owned(), json!("Z project"))]),
        )
        .unwrap();
    alice
        .insert_row(
            "projects",
            "project-a",
            BTreeMap::from([("title".to_owned(), json!("A project"))]),
        )
        .unwrap();
    alice
        .insert_row(
            "tasks",
            "task-z",
            BTreeMap::from([
                ("title".to_owned(), json!("Z task")),
                ("project".to_owned(), json!("project-z")),
            ]),
        )
        .unwrap();
    alice
        .insert_row(
            "tasks",
            "task-a",
            BTreeMap::from([
                ("title".to_owned(), json!("A task")),
                ("project".to_owned(), json!("project-a")),
            ]),
        )
        .unwrap();

    let query = BuiltQuery::from_json_value(json!({
        "table": "tasks",
        "orderBy": [["project", "asc"]],
    }))
    .unwrap();
    let ids = alice
        .query(query)
        .unwrap()
        .into_iter()
        .map(|row| row.id)
        .collect::<Vec<_>>();

    assert_eq!(ids, vec!["task-a", "task-z"]);
}

#[test]
fn branch_ref_field_order_uses_public_ref_ids() {
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

    let project_z_tx = alice
        .insert_row(
            "projects",
            "project-z",
            BTreeMap::from([("title".to_owned(), json!("Z project"))]),
        )
        .unwrap();
    alice
        .accept_transaction_at_global(&project_z_tx, 1)
        .unwrap();
    let project_a_tx = alice
        .insert_row(
            "projects",
            "project-a",
            BTreeMap::from([("title".to_owned(), json!("A project"))]),
        )
        .unwrap();
    alice
        .accept_transaction_at_global(&project_a_tx, 2)
        .unwrap();
    let task_z_tx = alice
        .insert_row(
            "tasks",
            "task-z",
            BTreeMap::from([
                ("title".to_owned(), json!("Z task")),
                ("project".to_owned(), json!("project-z")),
            ]),
        )
        .unwrap();
    alice.accept_transaction_at_global(&task_z_tx, 3).unwrap();
    let task_a_tx = alice
        .insert_row(
            "tasks",
            "task-a",
            BTreeMap::from([
                ("title".to_owned(), json!("A task")),
                ("project".to_owned(), json!("project-a")),
            ]),
        )
        .unwrap();
    alice.accept_transaction_at_global(&task_a_tx, 4).unwrap();
    alice.create_branch("draft", Some(4)).unwrap();
    alice.checkout_branch("draft").unwrap();

    let query = BuiltQuery::from_json_value(json!({
        "table": "tasks",
        "orderBy": [["project", "asc"]],
    }))
    .unwrap();
    let ids = alice
        .query(query)
        .unwrap()
        .into_iter()
        .map(|row| row.id)
        .collect::<Vec<_>>();

    assert_eq!(ids, vec!["task-a", "task-z"]);
}

#[test]
fn generic_required_ref_read_filters_parent_until_target_is_visible() {
    let schema = SchemaDef::new()
        .table("projects", |table| {
            table.text("title");
            table.read_if_created_by_user();
        })
        .table("todos", |table| {
            table.text("title");
            table.bool("done");
            table.ref_("project", "projects");
        });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut bob = Runtime::open_with_schema(Storage::Memory, "bob-node", "bob", schema).unwrap();

    alice
        .insert_row(
            "projects",
            "project-1",
            BTreeMap::from([("title".to_owned(), json!("Readable"))]),
        )
        .unwrap();
    alice
        .insert_row(
            "todos",
            "todo-readable",
            BTreeMap::from([
                ("title".to_owned(), json!("Visible include")),
                ("done".to_owned(), json!(false)),
                ("project".to_owned(), json!("project-1")),
            ]),
        )
        .unwrap();
    bob.insert_row(
        "projects",
        "project-2",
        BTreeMap::from([("title".to_owned(), json!("Bob private"))]),
    )
    .unwrap();
    alice
        .apply_bundle(&bob.export_table_history("projects").unwrap())
        .unwrap();
    alice
        .insert_row(
            "todos",
            "todo-hidden",
            BTreeMap::from([
                ("title".to_owned(), json!("Hidden include")),
                ("done".to_owned(), json!(false)),
                ("project".to_owned(), json!("project-2")),
            ]),
        )
        .unwrap();

    let rows = alice.read_rows_require_ref("todos", "project").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, "todo-readable");
    assert!(alice
        .read_rows_require_ref("todos", "title")
        .unwrap_err()
        .to_string()
        .contains("field title on table todos is not a ref"));
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
