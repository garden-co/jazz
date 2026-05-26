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

fn open_tasks_runtime(node_id: &str) -> Runtime {
    Runtime::open_with_schema(Storage::Memory, node_id, "alice", support::tasks_schema()).unwrap()
}

fn insert_task(runtime: &mut Runtime, id: &str, title: &str) -> String {
    runtime
        .insert_row(
            "tasks",
            id,
            BTreeMap::from([
                ("title".to_owned(), json!(title)),
                ("done".to_owned(), json!(false)),
            ]),
        )
        .unwrap()
}

fn update_task_title(runtime: &mut Runtime, id: &str, title: &str) -> String {
    runtime
        .update_row(
            "tasks",
            id,
            BTreeMap::from([("title".to_owned(), json!(title))]),
        )
        .unwrap()
}

fn assert_no_diff_and_unsettled(runtime: &Runtime, subscription: &mut RowsSubscription) {
    assert!(runtime.poll_subscription(subscription).unwrap().is_empty());
    assert!(!subscription.is_settled());
}

fn assert_added_task(diffs: &[RowDiff], id: &str) {
    assert!(
        matches!(diffs, [RowDiff::Added(row)] if row.id == id),
        "expected one added diff for {id}, got {diffs:?}"
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
fn tiered_subscription_waits_for_required_receipt() {
    let mut edge = open_tasks_runtime("edge-node");
    let edge_tx = insert_task(&mut edge, "task-edge", "Edge settled");

    assert_eq!(edge.read_rows("tasks").unwrap().len(), 1);
    assert!(edge
        .read_rows_at_tier("tasks", SubscriptionTier::Edge)
        .unwrap()
        .is_empty());

    let mut edge_subscription = edge
        .subscribe_rows_at_tier("tasks", SubscriptionTier::Edge)
        .unwrap();
    assert_eq!(edge_subscription.tier(), SubscriptionTier::Edge);
    assert!(edge_subscription.initial_rows().is_empty());
    assert!(!edge_subscription.is_settled());
    assert_no_diff_and_unsettled(&edge, &mut edge_subscription);

    edge.accept_transaction_at_edge(&edge_tx).unwrap();
    let diffs = edge.poll_subscription(&mut edge_subscription).unwrap();

    assert_added_task(&diffs, "task-edge");
    assert!(edge_subscription.is_settled());
    let info = edge.transaction_info(&edge_tx).unwrap();
    assert_eq!(info.receipt_tiers, vec!["edge".to_owned()]);

    let mut global = open_tasks_runtime("global-node");
    let global_tx = insert_task(&mut global, "task-global", "Global settled");
    let global_snapshot = global
        .read_rows_at_tier("tasks", SubscriptionTier::Global)
        .unwrap();
    let mut global_subscription = global
        .subscribe_rows_at_tier("tasks", SubscriptionTier::Global)
        .unwrap();

    assert_eq!(global_subscription.initial_rows(), global_snapshot);
    assert!(global_subscription.initial_rows().is_empty());
    assert!(!global_subscription.is_settled());

    global.accept_transaction_at_edge(&global_tx).unwrap();
    assert_no_diff_and_unsettled(&global, &mut global_subscription);
    assert!(global
        .read_rows_at_tier("tasks", SubscriptionTier::Global)
        .unwrap()
        .is_empty());

    global.accept_transaction_at_global(&global_tx, 1).unwrap();
    let diffs = global.poll_subscription(&mut global_subscription).unwrap();

    assert_added_task(&diffs, "task-global");
    assert!(global_subscription.is_settled());
    assert_eq!(
        global_subscription.initial_rows(),
        global
            .read_rows_at_tier("tasks", SubscriptionTier::Global)
            .unwrap()
    );
}

#[test]
fn global_tier_subscription_updates_are_gated_after_initial_delivery() {
    let mut alice = open_tasks_runtime("alice-node");

    let insert_tx = insert_task(&mut alice, "task-1", "Base");
    alice.accept_transaction_at_global(&insert_tx, 1).unwrap();
    let mut subscription = alice
        .subscribe_rows_at_tier("tasks", SubscriptionTier::Global)
        .unwrap();
    assert_eq!(subscription.initial_rows().len(), 1);
    assert_eq!(
        subscription.initial_rows()[0].values["title"],
        json!("Base")
    );
    assert!(subscription.is_settled());

    let update_tx = update_task_title(&mut alice, "task-1", "Pending global");
    assert_eq!(
        alice.read_rows("tasks").unwrap()[0].values["title"],
        json!("Pending global")
    );
    assert_eq!(
        alice
            .read_rows_at_tier("tasks", SubscriptionTier::Global)
            .unwrap()[0]
            .values["title"],
        json!("Base")
    );
    assert_no_diff_and_unsettled(&alice, &mut subscription);
    assert_eq!(
        subscription.initial_rows()[0].values["title"],
        json!("Base")
    );

    alice.accept_transaction_at_edge(&update_tx).unwrap();
    assert_no_diff_and_unsettled(&alice, &mut subscription);

    alice.accept_transaction_at_global(&update_tx, 2).unwrap();
    let diffs = alice.poll_subscription(&mut subscription).unwrap();

    assert!(matches!(
        &diffs[..],
        [RowDiff::Updated { before, after }]
            if before.values["title"] == json!("Base")
                && after.values["title"] == json!("Pending global")
    ));
    assert!(subscription.is_settled());
}

#[test]
fn tiered_subscription_preserves_branch_inherited_rows() {
    let mut alice = open_tasks_runtime("alice-node");

    let main_tx = insert_task(&mut alice, "task-main", "Main");
    alice.accept_transaction_at_global(&main_tx, 1).unwrap();
    alice.create_branch("draft", None).unwrap();
    alice.checkout_branch("draft").unwrap();

    let mut inherited_subscription = alice
        .subscribe_rows_at_tier("tasks", SubscriptionTier::Global)
        .unwrap();
    assert_eq!(inherited_subscription.initial_rows().len(), 1);
    assert_eq!(inherited_subscription.initial_rows()[0].id, "task-main");
    assert!(inherited_subscription.is_settled());
    assert!(alice
        .poll_subscription(&mut inherited_subscription)
        .unwrap()
        .is_empty());

    alice.checkout_branch("main").unwrap();
    let base_tx = insert_task(&mut alice, "task-base", "Pinned base");
    alice.accept_transaction_at_global(&base_tx, 2).unwrap();
    alice.create_branch("pinned", Some(2)).unwrap();
    alice.checkout_branch("pinned").unwrap();

    let pinned_subscription = alice
        .subscribe_rows_at_tier("tasks", SubscriptionTier::Global)
        .unwrap();
    assert_eq!(
        pinned_subscription
            .initial_rows()
            .iter()
            .map(|row| row.id.as_str())
            .collect::<Vec<_>>(),
        vec!["task-base", "task-main"]
    );
    assert!(pinned_subscription.is_settled());
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
                    .export_query_where_eq_top_created_at_desc("notes", "pinned", json!(true), 2)
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
        .subscribe_rows_where_eq_top_created_at_desc("notes", "pinned", json!(true), 2)
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
