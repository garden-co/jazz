use super::*;

#[test]
fn rejection_subscription_reports_new_sync_rejections_with_detail_once() {
    let schema = support::tasks_schema();
    let mut authority =
        Runtime::open_with_schema(Storage::Memory, "authority", "alice", schema.clone()).unwrap();
    let mut worker = Runtime::open_with_schema(Storage::Memory, "worker", "alice", schema).unwrap();

    let mut subscription = worker.subscribe_rejections().unwrap();
    assert!(subscription.initial_rejections().is_empty());

    let tx = authority
        .insert_row(
            "tasks",
            "task-rejected",
            BTreeMap::from([
                ("title".to_owned(), json!("Rejected async")),
                ("done".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    worker
        .apply_bundle(&authority.export_table_history("tasks").unwrap())
        .unwrap();
    assert!(worker
        .poll_rejections(&mut subscription)
        .unwrap()
        .is_empty());

    authority
        .reject_transaction_with_detail(
            &tx,
            "policy_denied",
            json!({"reason": "authority", "safe": true}),
        )
        .unwrap();
    worker
        .apply_bundle(&authority.export_table_history("tasks").unwrap())
        .unwrap();

    let events = worker.poll_rejections(&mut subscription).unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].tx_id, tx);
    assert_eq!(events[0].code, "policy_denied");
    assert_eq!(
        events[0].detail,
        Some(json!({"reason": "authority", "safe": true}))
    );
    assert!(worker
        .poll_rejections(&mut subscription)
        .unwrap()
        .is_empty());
}

#[test]
fn built_query_reads_newest_matching_rows_from_jazz_tools_json_shape() {
    let schema = support::notes_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema).unwrap();

    alice
        .insert_row(
            "notes",
            "note-old",
            BTreeMap::from([
                ("body".to_owned(), json!("old")),
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
    std::thread::sleep(std::time::Duration::from_millis(2));
    alice
        .insert_row(
            "notes",
            "note-new",
            BTreeMap::from([
                ("body".to_owned(), json!("new")),
                ("pinned".to_owned(), json!(true)),
            ]),
        )
        .unwrap();
    alice
        .insert_row(
            "notes",
            "note-hidden",
            BTreeMap::from([
                ("body".to_owned(), json!("hidden")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .unwrap();

    let query = BuiltQuery::from_json_value(json!({
        "table": "notes",
        "conditions": [{"column": "pinned", "op": "eq", "value": true}],
        "includes": {},
        "orderBy": [["$createdAt", "desc"]],
        "limit": 2
    }))
    .unwrap();

    let rows = alice.query(query.clone()).unwrap();
    assert_eq!(
        rows.iter().map(|row| row.id.as_str()).collect::<Vec<_>>(),
        vec!["note-new", "note-middle"]
    );
    assert_eq!(alice.one(query).unwrap().unwrap().id, "note-new");
}

#[test]
fn built_query_lowers_predicates_ordering_and_window_to_sqlite() {
    let schema = support::notes_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut peer =
        Runtime::open_with_schema(Storage::Memory, "peer-node", "alice", schema).unwrap();

    for (id, body, pinned) in [
        ("note-old", "keep old", true),
        ("note-middle", "keep middle", true),
        ("note-hidden", "keep hidden", false),
        ("note-new", "keep new", true),
    ] {
        alice
            .insert_row(
                "notes",
                id,
                BTreeMap::from([
                    ("body".to_owned(), json!(body)),
                    ("pinned".to_owned(), json!(pinned)),
                ]),
            )
            .unwrap();
        std::thread::sleep(std::time::Duration::from_millis(2));
    }

    let query = BuiltQuery::from_json_value(json!({
        "table": "notes",
        "conditions": [
            {"column": "pinned", "op": "eq", "value": true},
            {"column": "body", "op": "contains", "value": "keep"}
        ],
        "orderBy": [["$createdAt", "desc"]],
        "limit": 2,
        "offset": 1
    }))
    .unwrap();
    let all_synced_matching = BuiltQuery::from_json_value(json!({
        "table": "notes",
        "conditions": [
            {"column": "pinned", "op": "eq", "value": true},
            {"column": "body", "op": "contains", "value": "keep"}
        ],
        "orderBy": [["$createdAt", "desc"]]
    }))
    .unwrap();

    let rows = alice.query(query.clone()).unwrap();
    assert_eq!(
        rows.iter().map(|row| row.id.as_str()).collect::<Vec<_>>(),
        vec!["note-middle", "note-old"]
    );

    peer.apply_bundle(&alice.export_query(query.clone()).unwrap())
        .unwrap();
    assert_eq!(
        peer.query(query.clone())
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
            "note-newest",
            BTreeMap::from([
                ("body".to_owned(), json!("keep newest")),
                ("pinned".to_owned(), json!(true)),
            ]),
        )
        .unwrap();

    for refresh in alice
        .export_query_read_refreshes(&peer.observed_query_reads().unwrap())
        .unwrap()
    {
        peer.apply_bundle(&refresh).unwrap();
    }
    assert_eq!(
        peer.query(query)
            .unwrap()
            .iter()
            .map(|row| row.id.as_str())
            .collect::<Vec<_>>(),
        vec!["note-new", "note-middle"]
    );
    assert_eq!(
        peer.query(all_synced_matching)
            .unwrap()
            .iter()
            .map(|row| row.id.as_str())
            .collect::<Vec<_>>(),
        vec!["note-newest", "note-new", "note-middle"]
    );
}

#[test]
fn built_query_rejects_graph_only_jazz_tools_fields() {
    let query = BuiltQuery::from_json_value(json!({
        "table": "notes",
        "conditions": [],
        "includes": {"author": true}
    }))
    .unwrap_err();

    assert!(query
        .to_string()
        .contains("mini-sqlite query does not support includes"));
}

#[test]
fn query_subscription_delta_matches_jazz_tools_subscribe_all_shape() {
    let schema = support::notes_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema).unwrap();

    alice
        .insert_row(
            "notes",
            "note-old",
            BTreeMap::from([
                ("body".to_owned(), json!("old")),
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

    let query = BuiltQuery::from_json_value(json!({
        "table": "notes",
        "conditions": [{"column": "pinned", "op": "eq", "value": true}],
        "orderBy": [["$createdAt", "desc"]],
        "limit": 2
    }))
    .unwrap();
    let mut subscription = alice.subscribe_query(query).unwrap();

    let initial = subscription.initial_delta();
    assert_eq!(
        initial
            .all
            .iter()
            .map(|row| row.id.as_str())
            .collect::<Vec<_>>(),
        vec!["note-middle", "note-old"]
    );
    assert_eq!(
        initial.delta,
        vec![
            SubscriptionRowDelta::Added {
                id: "note-middle".to_owned(),
                index: 0,
                item: initial.all[0].clone(),
            },
            SubscriptionRowDelta::Added {
                id: "note-old".to_owned(),
                index: 1,
                item: initial.all[1].clone(),
            },
        ]
    );

    std::thread::sleep(std::time::Duration::from_millis(2));
    alice
        .insert_row(
            "notes",
            "note-new",
            BTreeMap::from([
                ("body".to_owned(), json!("new")),
                ("pinned".to_owned(), json!(true)),
            ]),
        )
        .unwrap();

    let update = alice.subscription_delta(&mut subscription).unwrap();
    assert_eq!(
        update
            .all
            .iter()
            .map(|row| row.id.as_str())
            .collect::<Vec<_>>(),
        vec!["note-new", "note-middle"]
    );
    assert_eq!(
        update.delta,
        vec![
            SubscriptionRowDelta::Added {
                id: "note-new".to_owned(),
                index: 0,
                item: update.all[0].clone(),
            },
            SubscriptionRowDelta::Removed {
                id: "note-old".to_owned(),
                index: 1,
            },
            SubscriptionRowDelta::Moved {
                id: "note-middle".to_owned(),
                previous_index: 0,
                index: 1,
            },
        ]
    );
}

#[test]
fn restarted_rejection_subscription_baselines_old_rejections_and_reports_later_ones() {
    let dir = tempdir().unwrap();
    let worker_path = dir.path().join("worker.sqlite");
    let schema = support::tasks_schema();
    let mut authority =
        Runtime::open_with_schema(Storage::Memory, "authority", "alice", schema.clone()).unwrap();

    let first = authority
        .insert_row(
            "tasks",
            "task-first",
            BTreeMap::from([
                ("title".to_owned(), json!("First rejection")),
                ("done".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    authority
        .reject_transaction_with_detail(&first, "policy_denied", json!({"n": 1}))
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
            .apply_bundle(&authority.export_table_history("tasks").unwrap())
            .unwrap();
    }

    let mut worker =
        Runtime::open_with_schema(Storage::File(worker_path), "worker", "alice", schema).unwrap();
    let mut subscription = worker.subscribe_rejections().unwrap();
    assert_eq!(subscription.initial_rejections().len(), 1);
    assert!(worker
        .poll_rejections(&mut subscription)
        .unwrap()
        .is_empty());

    let second = authority
        .insert_row(
            "tasks",
            "task-second",
            BTreeMap::from([
                ("title".to_owned(), json!("Second rejection")),
                ("done".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    authority
        .reject_transaction_with_detail(&second, "policy_denied", json!({"n": 2}))
        .unwrap();
    worker
        .apply_bundle(&authority.export_table_history("tasks").unwrap())
        .unwrap();

    let events = worker.poll_rejections(&mut subscription).unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].tx_id, second);
    assert_eq!(events[0].detail, Some(json!({"n": 2})));
}

#[test]
fn rejection_subscription_reports_later_detail_enrichment() {
    let schema = support::tasks_schema();
    let mut authority =
        Runtime::open_with_schema(Storage::Memory, "authority", "alice", schema.clone()).unwrap();
    let mut worker = Runtime::open_with_schema(Storage::Memory, "worker", "alice", schema).unwrap();
    let mut subscription = worker.subscribe_rejections().unwrap();

    let tx = authority
        .insert_row(
            "tasks",
            "task-rejected",
            BTreeMap::from([
                ("title".to_owned(), json!("Rejection gets detail later")),
                ("done".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    worker
        .apply_bundle(&authority.export_table_history("tasks").unwrap())
        .unwrap();
    authority.reject_transaction(&tx, "policy_denied").unwrap();
    worker
        .apply_bundle(&authority.export_table_history("tasks").unwrap())
        .unwrap();
    let first_events = worker.poll_rejections(&mut subscription).unwrap();
    assert_eq!(first_events.len(), 1);
    assert_eq!(first_events[0].tx_id, tx);
    assert_eq!(first_events[0].detail, None);

    authority
        .reject_transaction_with_detail(&tx, "policy_denied", json!({"reason": "late detail"}))
        .unwrap();
    worker
        .apply_bundle(&authority.export_table_history("tasks").unwrap())
        .unwrap();

    let detail_events = worker.poll_rejections(&mut subscription).unwrap();
    assert_eq!(detail_events.len(), 1);
    assert_eq!(detail_events[0].tx_id, tx);
    assert_eq!(
        detail_events[0].detail,
        Some(json!({"reason": "late detail"}))
    );
}

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
    alice.update_row("tasks", "task-1", update).unwrap();
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
fn not_equal_subscription_diffs_when_optional_value_appears_and_clears() {
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

    let mut subscription = alice
        .subscribe_rows_where_ne("notes", "tag", json!(null))
        .unwrap();
    assert_eq!(subscription.initial_rows().len(), 1);
    assert_eq!(subscription.initial_rows()[0].id, "note-tagged");

    alice
        .update_row(
            "notes",
            "note-untagged",
            BTreeMap::from([("tag".to_owned(), json!("personal"))]),
        )
        .unwrap();
    let diffs = alice.poll_subscription(&mut subscription).unwrap();
    assert!(matches!(&diffs[..], [RowDiff::Added(row)] if row.id == "note-untagged"));

    alice
        .update_row(
            "notes",
            "note-tagged",
            BTreeMap::from([("tag".to_owned(), json!(null))]),
        )
        .unwrap();
    let diffs = alice.poll_subscription(&mut subscription).unwrap();
    assert!(matches!(&diffs[..], [RowDiff::Removed(row)] if row.id == "note-tagged"));
}

#[test]
fn observed_created_by_not_equal_subscription_removes_deleted_remote_row() {
    let schema = SchemaDef::new().table("notes", |table| {
        table.text("body");
        table.bool("pinned");
    });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut bob =
        Runtime::open_with_schema(Storage::Memory, "bob-node", "bob", schema.clone()).unwrap();
    let mut worker =
        Runtime::open_with_schema(Storage::Memory, "worker-node", "alice", schema).unwrap();

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
    worker
        .apply_bundle(
            &alice
                .export_query_where_ne("notes", "$createdBy", json!("alice"))
                .unwrap(),
        )
        .unwrap();

    let observed = worker.observed_query_reads().unwrap();
    let mut subscription = worker.subscribe_observed_query(&observed[0]).unwrap();
    assert_eq!(subscription.initial_rows().len(), 1);
    assert_eq!(subscription.initial_rows()[0].id, "note-bob");

    alice.delete_row("notes", "note-bob").unwrap();
    for refresh in alice.export_query_read_refreshes(&observed).unwrap() {
        worker.apply_bundle(&refresh).unwrap();
    }

    let diffs = worker.poll_subscription(&mut subscription).unwrap();
    assert!(matches!(&diffs[..], [RowDiff::Removed(row)] if row.id == "note-bob"));
}

#[test]
fn observed_id_not_equal_subscription_removes_deleted_remote_row() {
    let schema = support::notes_schema();
    let mut upstream =
        Runtime::open_with_schema(Storage::Memory, "upstream", "alice", schema.clone()).unwrap();
    let mut worker =
        Runtime::open_with_schema(Storage::Memory, "worker-node", "alice", schema).unwrap();

    upstream
        .insert_row(
            "notes",
            "note-keep",
            BTreeMap::from([
                ("body".to_owned(), json!("Keep")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    upstream
        .insert_row(
            "notes",
            "note-excluded",
            BTreeMap::from([
                ("body".to_owned(), json!("Excluded")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    worker
        .apply_bundle(
            &upstream
                .export_query_where_ne("notes", "id", json!("note-excluded"))
                .unwrap(),
        )
        .unwrap();

    let observed = worker.observed_query_reads().unwrap();
    let mut subscription = worker.subscribe_observed_query(&observed[0]).unwrap();
    assert_eq!(subscription.initial_rows().len(), 1);
    assert_eq!(subscription.initial_rows()[0].id, "note-keep");

    upstream.delete_row("notes", "note-keep").unwrap();
    for refresh in upstream.export_query_read_refreshes(&observed).unwrap() {
        worker.apply_bundle(&refresh).unwrap();
    }

    let diffs = worker.poll_subscription(&mut subscription).unwrap();
    assert!(matches!(&diffs[..], [RowDiff::Removed(row)] if row.id == "note-keep"));
}

#[test]
fn restarted_subscription_uses_persisted_query_read_and_emits_refresh_diff() {
    let dir = tempdir().unwrap();
    let worker_path = dir.path().join("worker.sqlite");
    let schema = SchemaDef::new().table("tasks", |table| {
        table.text("title");
        table.bool("done");
    });
    let mut upstream =
        Runtime::open_with_schema(Storage::Memory, "upstream", "alice", schema.clone()).unwrap();

    upstream
        .insert_row(
            "tasks",
            "task-open",
            BTreeMap::from([
                ("title".to_owned(), json!("Open before restart")),
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
            .apply_bundle(
                &upstream
                    .export_query_where_eq("tasks", "done", json!(false))
                    .unwrap(),
            )
            .unwrap();
        assert_eq!(worker.observed_query_reads().unwrap().len(), 1);
    }

    upstream
        .update_row(
            "tasks",
            "task-open",
            BTreeMap::from([("done".to_owned(), json!(true))]),
        )
        .unwrap();

    let mut worker =
        Runtime::open_with_schema(Storage::File(worker_path), "worker", "alice", schema).unwrap();
    let observed = worker.observed_query_reads().unwrap();
    let mut subscription = worker.subscribe_observed_query(&observed[0]).unwrap();
    assert_eq!(subscription.initial_rows().len(), 1);

    for refresh in upstream.export_query_read_refreshes(&observed).unwrap() {
        worker.apply_bundle(&refresh).unwrap();
    }
    let diffs = worker.poll_subscription(&mut subscription).unwrap();
    assert!(matches!(&diffs[..], [RowDiff::Removed(row)] if row.id == "task-open"));
}

#[test]
fn restarted_ordered_page_subscription_emits_boundary_refresh_diff() {
    let dir = tempdir().unwrap();
    let worker_path = dir.path().join("ordered-worker.sqlite");
    let schema = support::notes_schema();
    let mut upstream =
        Runtime::open_with_schema(Storage::Memory, "upstream", "alice", schema.clone()).unwrap();

    upstream
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
    upstream
        .insert_row(
            "notes",
            "note-middle",
            BTreeMap::from([
                ("body".to_owned(), json!("middle")),
                ("pinned".to_owned(), json!(true)),
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
            .apply_bundle(
                &upstream
                    .export_query(support::top_created_query(
                        "notes",
                        "pinned",
                        json!(true),
                        2,
                    ))
                    .unwrap(),
            )
            .unwrap();
    }

    std::thread::sleep(std::time::Duration::from_millis(2));
    upstream
        .insert_row(
            "notes",
            "note-new",
            BTreeMap::from([
                ("body".to_owned(), json!("newest")),
                ("pinned".to_owned(), json!(true)),
            ]),
        )
        .unwrap();

    let mut worker =
        Runtime::open_with_schema(Storage::File(worker_path), "worker", "alice", schema).unwrap();
    let observed = worker.observed_query_reads().unwrap();
    let mut subscription = worker.subscribe_observed_query(&observed[0]).unwrap();
    for refresh in upstream.export_query_read_refreshes(&observed).unwrap() {
        worker.apply_bundle(&refresh).unwrap();
    }

    let diffs = worker.poll_subscription(&mut subscription).unwrap();
    assert!(diffs
        .iter()
        .any(|diff| matches!(diff, RowDiff::Added(row) if row.id == "note-new")));
    assert!(diffs
        .iter()
        .any(|diff| matches!(diff, RowDiff::Removed(row) if row.id == "note-old")));
}

#[test]
fn contains_subscription_diffs_when_text_starts_and_stops_matching() {
    let schema = support::notes_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema).unwrap();

    alice
        .insert_row(
            "notes",
            "note-1",
            BTreeMap::from([
                ("body".to_owned(), json!("plain text")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    alice
        .insert_row(
            "notes",
            "note-2",
            BTreeMap::from([
                ("body".to_owned(), json!("contains sqlite")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .unwrap();

    let mut subscription = alice
        .subscribe_rows_where_contains("notes", "body", "sqlite")
        .unwrap();
    assert_eq!(subscription.initial_rows().len(), 1);
    assert_eq!(subscription.initial_rows()[0].id, "note-2");

    alice
        .update_row(
            "notes",
            "note-1",
            BTreeMap::from([("body".to_owned(), json!("now contains sqlite"))]),
        )
        .unwrap();
    let diffs = alice.poll_subscription(&mut subscription).unwrap();
    assert!(matches!(&diffs[..], [RowDiff::Added(row)] if row.id == "note-1"));

    alice
        .update_row(
            "notes",
            "note-2",
            BTreeMap::from([("body".to_owned(), json!("moved away"))]),
        )
        .unwrap();
    let diffs = alice.poll_subscription(&mut subscription).unwrap();
    assert!(matches!(&diffs[..], [RowDiff::Removed(row)] if row.id == "note-2"));
}

#[test]
fn in_subscription_diffs_when_row_enters_and_leaves_value_set() {
    let schema = support::notes_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema).unwrap();

    alice
        .insert_row(
            "notes",
            "note-1",
            BTreeMap::from([
                ("body".to_owned(), json!("alpha")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    alice
        .insert_row(
            "notes",
            "note-2",
            BTreeMap::from([
                ("body".to_owned(), json!("gamma")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .unwrap();

    let mut subscription = alice
        .subscribe_rows_where_in("notes", "body", vec![json!("alpha"), json!("beta")])
        .unwrap();
    assert_eq!(subscription.initial_rows().len(), 1);
    assert_eq!(subscription.initial_rows()[0].id, "note-1");

    alice
        .update_row(
            "notes",
            "note-2",
            BTreeMap::from([("body".to_owned(), json!("beta"))]),
        )
        .unwrap();
    let diffs = alice.poll_subscription(&mut subscription).unwrap();
    assert!(matches!(&diffs[..], [RowDiff::Added(row)] if row.id == "note-2"));

    alice
        .update_row(
            "notes",
            "note-1",
            BTreeMap::from([("body".to_owned(), json!("omega"))]),
        )
        .unwrap();
    let diffs = alice.poll_subscription(&mut subscription).unwrap();
    assert!(matches!(&diffs[..], [RowDiff::Removed(row)] if row.id == "note-1"));
}

#[test]
fn ordered_page_subscription_replaces_displaced_boundary_row() {
    let schema = support::notes_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema).unwrap();

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

    let mut subscription = alice
        .subscribe_query(support::top_created_query(
            "notes",
            "pinned",
            json!(true),
            2,
        ))
        .unwrap();
    assert_eq!(
        subscription
            .initial_rows()
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
    let diffs = alice.poll_subscription(&mut subscription).unwrap();

    assert!(diffs
        .iter()
        .any(|diff| matches!(diff, RowDiff::Added(row) if row.id == "note-new")));
    assert!(diffs
        .iter()
        .any(|diff| matches!(diff, RowDiff::Removed(row) if row.id == "note-old")));
    assert_eq!(
        subscription
            .initial_rows()
            .iter()
            .map(|row| row.id.as_str())
            .collect::<Vec<_>>(),
        vec!["note-new", "note-middle"]
    );
}

#[test]
fn ordered_field_page_subscription_replaces_displaced_boundary_row() {
    let schema = SchemaDef::new().table("documents", |table| {
        table.text("owner_id");
        table.text("updated_at");
        table.text("title");
        table.index("owner_updated", ["owner_id", "updated_at"]);
    });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema).unwrap();

    for (id, updated_at) in [("doc-old", "0001"), ("doc-middle", "0002")] {
        alice
            .insert_row(
                "documents",
                id,
                BTreeMap::from([
                    ("owner_id".to_owned(), json!("alice")),
                    ("updated_at".to_owned(), json!(updated_at)),
                    ("title".to_owned(), json!(id)),
                ]),
            )
            .unwrap();
    }

    let mut subscription = alice
        .subscribe_rows_where_eq_top_field_desc(
            "documents",
            "owner_id",
            json!("alice"),
            "updated_at",
            2,
        )
        .unwrap();
    assert_eq!(
        subscription
            .initial_rows()
            .iter()
            .map(|row| row.id.as_str())
            .collect::<Vec<_>>(),
        vec!["doc-middle", "doc-old"]
    );

    alice
        .insert_row(
            "documents",
            "doc-new",
            BTreeMap::from([
                ("owner_id".to_owned(), json!("alice")),
                ("updated_at".to_owned(), json!("0003")),
                ("title".to_owned(), json!("newest")),
            ]),
        )
        .unwrap();
    let diffs = alice.poll_subscription(&mut subscription).unwrap();

    assert!(diffs
        .iter()
        .any(|diff| matches!(diff, RowDiff::Added(row) if row.id == "doc-new")));
    assert!(diffs
        .iter()
        .any(|diff| matches!(diff, RowDiff::Removed(row) if row.id == "doc-old")));
}

#[test]
fn ordered_field_page_subscription_delta_replaces_displaced_boundary_row() {
    let schema = SchemaDef::new().table("documents", |table| {
        table.text("owner_id");
        table.text("updated_at");
        table.text("title");
        table.index("owner_updated", ["owner_id", "updated_at"]);
    });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema).unwrap();

    for (id, updated_at) in [("doc-old", "0001"), ("doc-middle", "0002")] {
        alice
            .insert_row(
                "documents",
                id,
                BTreeMap::from([
                    ("owner_id".to_owned(), json!("alice")),
                    ("updated_at".to_owned(), json!(updated_at)),
                    ("title".to_owned(), json!(id)),
                ]),
            )
            .unwrap();
    }

    let mut subscription = alice
        .subscribe_rows_where_eq_top_field_desc(
            "documents",
            "owner_id",
            json!("alice"),
            "updated_at",
            2,
        )
        .unwrap();

    alice
        .insert_row(
            "documents",
            "doc-new",
            BTreeMap::from([
                ("owner_id".to_owned(), json!("alice")),
                ("updated_at".to_owned(), json!("0003")),
                ("title".to_owned(), json!("newest")),
            ]),
        )
        .unwrap();
    let delta = alice.subscription_delta(&mut subscription).unwrap();

    assert_eq!(
        delta
            .all
            .iter()
            .map(|row| row.id.as_str())
            .collect::<Vec<_>>(),
        vec!["doc-new", "doc-middle"]
    );
    assert!(delta
        .delta
        .iter()
        .any(|change| matches!(change, SubscriptionRowDelta::Added { id, .. } if id == "doc-new")));
    assert!(delta.delta.iter().any(
        |change| matches!(change, SubscriptionRowDelta::Removed { id, .. } if id == "doc-old")
    ));
}

#[test]
fn subscription_removes_child_when_parent_policy_dependency_changes() {
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
