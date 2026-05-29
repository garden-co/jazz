use super::*;
use mini_jazz_sqlite::sync::merge_bundles;
use std::collections::{BTreeMap, BTreeSet};

#[test]
fn query_scoped_sync_converges_memory_and_durable_nodes() {
    let harness = support::Harness::new();
    let mut alice = harness.memory("alice-tab", "alice").unwrap();
    let mut worker = harness
        .durable("worker.sqlite", "alice-worker", "alice")
        .unwrap();

    alice.create_project("project-1", "Spec work").unwrap();
    alice
        .create_todo("todo-1", "Sync through bundle", false, "project-1")
        .unwrap();

    support::apply(alice.export_query_scope_open_todos().unwrap(), &mut worker).unwrap();

    assert_eq!(worker.open_todos().unwrap(), alice.open_todos().unwrap());

    drop(worker);
    let reopened = harness
        .durable("worker.sqlite", "alice-worker", "alice")
        .unwrap();
    assert_eq!(reopened.open_todos().unwrap(), alice.open_todos().unwrap());
}

#[test]
fn merged_query_refresh_bundle_applies_like_individual_bundles() {
    let harness = support::Harness::new();
    let mut upstream = harness.memory("upstream", "alice").unwrap();
    let mut separate_peer = harness.memory("separate-peer", "alice").unwrap();
    let mut merged_peer = harness.memory("merged-peer", "alice").unwrap();

    upstream.create_project("project-1", "Spec work").unwrap();
    upstream
        .create_todo("todo-1", "First", false, "project-1")
        .unwrap();
    upstream
        .create_todo("todo-2", "Second", false, "project-1")
        .unwrap();

    let bundles = vec![
        upstream.export_query_scope_open_todos().unwrap(),
        upstream.export_query_scope_newest_open_todos(1).unwrap(),
    ];
    for bundle in &bundles {
        separate_peer.apply_bundle(bundle).unwrap();
    }
    let merged = merge_bundles(&bundles).unwrap();
    merged_peer.apply_bundle(&merged).unwrap();

    assert_eq!(merged.query_reads.len(), 2);
    assert!(merged.txs.len() < bundles.iter().map(|bundle| bundle.txs.len()).sum());
    assert_eq!(
        merged_peer.open_todos().unwrap(),
        separate_peer.open_todos().unwrap()
    );
}

#[test]
fn durable_query_reads_drive_reconnect_refresh_after_restart() {
    let harness = support::Harness::new();
    let mut upstream = harness.memory("upstream", "alice").unwrap();
    upstream.create_project("project-1", "Spec work").unwrap();
    upstream
        .create_todo("todo-1", "Initially open", false, "project-1")
        .unwrap();

    {
        let mut worker = harness.durable("worker.sqlite", "worker", "alice").unwrap();
        support::apply(
            upstream.export_query_scope_open_todos().unwrap(),
            &mut worker,
        )
        .unwrap();
        assert_eq!(worker.open_todos().unwrap().len(), 1);
        assert_eq!(worker.observed_query_reads().unwrap().len(), 1);
    }

    upstream
        .update_row(
            "todos",
            "todo-1",
            BTreeMap::from([
                ("title".to_owned(), json!("Closed while offline")),
                ("done".to_owned(), json!(true)),
                ("project".to_owned(), json!("project-1")),
            ]),
        )
        .unwrap();

    let mut reopened = harness
        .durable("worker.sqlite", "worker-reopened", "alice")
        .unwrap();
    assert_eq!(reopened.open_todos().unwrap().len(), 1);

    support::refresh_observed_queries(&upstream, &mut reopened).unwrap();

    assert!(reopened.open_todos().unwrap().is_empty());
    assert_eq!(reopened.observed_query_reads().unwrap().len(), 1);
}

#[test]
fn durable_ordered_query_read_refreshes_page_boundary_after_restart() {
    let harness = support::Harness::new();
    let schema = support::notes_schema();
    let mut upstream = harness
        .memory_with_schema("upstream", "alice", schema.clone())
        .unwrap();

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
        let mut worker = harness
            .durable_with_schema("ordered-worker.sqlite", "worker", "alice", schema.clone())
            .unwrap();
        support::apply(
            upstream
                .export_query_where_eq_top_created_at_desc("notes", "pinned", json!(true), 2)
                .unwrap(),
            &mut worker,
        )
        .unwrap();
        assert_eq!(
            worker
                .read_rows_where_eq_top_created_at_desc("notes", "pinned", json!(true), 2)
                .unwrap()
                .iter()
                .map(|row| row.id.as_str())
                .collect::<Vec<_>>(),
            vec!["note-middle", "note-old"]
        );
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

    let mut reopened = harness
        .durable_with_schema("ordered-worker.sqlite", "worker-reopened", "alice", schema)
        .unwrap();
    support::refresh_observed_queries(&upstream, &mut reopened).unwrap();

    assert_eq!(
        reopened
            .read_rows_where_eq_top_created_at_desc("notes", "pinned", json!(true), 3)
            .unwrap()
            .iter()
            .map(|row| row.id.as_str())
            .collect::<Vec<_>>(),
        vec!["note-new", "note-middle"]
    );
}

#[test]
fn durable_user_column_ordered_query_read_refreshes_page_boundary_after_restart() {
    let harness = support::Harness::new();
    let schema = SchemaDef::new().table("documents", |table| {
        table.text("owner_id");
        table.text("updated_at");
        table.text("title");
        table.index("owner_updated", ["owner_id", "updated_at"]);
    });
    let mut upstream = harness
        .memory_with_schema("upstream", "alice", schema.clone())
        .unwrap();

    for (id, updated_at) in [
        ("doc-old", "0001"),
        ("doc-middle", "0002"),
        ("doc-bob", "9999"),
    ] {
        upstream
            .insert_row(
                "documents",
                id,
                BTreeMap::from([
                    (
                        "owner_id".to_owned(),
                        json!(if id == "doc-bob" { "bob" } else { "alice" }),
                    ),
                    ("updated_at".to_owned(), json!(updated_at)),
                    ("title".to_owned(), json!(id)),
                ]),
            )
            .unwrap();
    }

    {
        let mut worker = harness
            .durable_with_schema(
                "ordered-field-worker.sqlite",
                "worker",
                "alice",
                schema.clone(),
            )
            .unwrap();
        support::apply(
            upstream
                .export_query_where_eq_top_field_desc(
                    "documents",
                    "owner_id",
                    json!("alice"),
                    "updated_at",
                    2,
                )
                .unwrap(),
            &mut worker,
        )
        .unwrap();
        assert_eq!(
            worker
                .read_rows_where_eq_top_field_desc(
                    "documents",
                    "owner_id",
                    json!("alice"),
                    "updated_at",
                    2,
                )
                .unwrap()
                .iter()
                .map(|row| row.id.as_str())
                .collect::<Vec<_>>(),
            vec!["doc-middle", "doc-old"]
        );
    }

    upstream
        .insert_row(
            "documents",
            "doc-new",
            BTreeMap::from([
                ("owner_id".to_owned(), json!("alice")),
                ("updated_at".to_owned(), json!("0003")),
                ("title".to_owned(), json!("new")),
            ]),
        )
        .unwrap();

    let mut reopened = harness
        .durable_with_schema(
            "ordered-field-worker.sqlite",
            "worker-reopened",
            "alice",
            schema,
        )
        .unwrap();
    support::refresh_observed_queries(&upstream, &mut reopened).unwrap();

    assert_eq!(
        reopened
            .read_rows_where_eq_top_field_desc(
                "documents",
                "owner_id",
                json!("alice"),
                "updated_at",
                3,
            )
            .unwrap()
            .iter()
            .map(|row| row.id.as_str())
            .collect::<Vec<_>>(),
        vec!["doc-new", "doc-middle"]
    );
}

#[test]
fn ordered_page_export_is_scoped_to_observed_page_rows() {
    let harness = support::Harness::new();
    let schema = SchemaDef::new().table("documents", |table| {
        table.text("owner_id");
        table.text("updated_at");
        table.text("title");
        table.index("owner_updated", ["owner_id", "updated_at"]);
    });
    let mut upstream = harness
        .memory_with_schema("upstream", "alice", schema)
        .unwrap();

    for index in 0..5 {
        upstream
            .insert_row(
                "documents",
                &format!("alice-doc-{index}"),
                BTreeMap::from([
                    ("owner_id".to_owned(), json!("alice")),
                    ("updated_at".to_owned(), json!(format!("{index:04}"))),
                    ("title".to_owned(), json!(format!("Alice doc {index}"))),
                ]),
            )
            .unwrap();
    }

    let bundle = upstream
        .export_query_where_eq_top_field_desc(
            "documents",
            "owner_id",
            json!("alice"),
            "updated_at",
            2,
        )
        .unwrap();
    let exported_doc_ids = bundle
        .history
        .iter()
        .filter(|record| record.table == "documents")
        .map(|record| record.row_id.as_str())
        .collect::<BTreeSet<_>>();

    assert_eq!(
        exported_doc_ids,
        BTreeSet::from(["alice-doc-4", "alice-doc-3"])
    );
    assert_eq!(
        bundle.query_reads[0].value["observed_ids"],
        json!(["alice-doc-4", "alice-doc-3"])
    );
}

#[test]
fn ordered_page_refresh_repairs_previously_observed_deleted_rows() {
    let harness = support::Harness::new();
    let schema = SchemaDef::new().table("documents", |table| {
        table.text("owner_id");
        table.text("updated_at");
        table.text("title");
        table.index("owner_updated", ["owner_id", "updated_at"]);
    });
    let mut upstream = harness
        .memory_with_schema("upstream", "alice", schema.clone())
        .unwrap();

    for (id, updated_at) in [
        ("doc-old", "0001"),
        ("doc-middle", "0002"),
        ("doc-new", "0003"),
    ] {
        upstream
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

    {
        let mut worker = harness
            .durable_with_schema(
                "ordered-page-delete-worker.sqlite",
                "worker",
                "alice",
                schema.clone(),
            )
            .unwrap();
        support::apply(
            upstream
                .export_query_where_eq_top_field_desc(
                    "documents",
                    "owner_id",
                    json!("alice"),
                    "updated_at",
                    2,
                )
                .unwrap(),
            &mut worker,
        )
        .unwrap();
        assert_eq!(
            worker
                .read_rows_where_eq_top_field_desc(
                    "documents",
                    "owner_id",
                    json!("alice"),
                    "updated_at",
                    2,
                )
                .unwrap()
                .iter()
                .map(|row| row.id.as_str())
                .collect::<Vec<_>>(),
            vec!["doc-new", "doc-middle"]
        );
    }

    upstream.delete_row("documents", "doc-new").unwrap();

    let mut reopened = harness
        .durable_with_schema(
            "ordered-page-delete-worker.sqlite",
            "worker-reopened",
            "alice",
            schema,
        )
        .unwrap();
    let refresh_bundles = upstream
        .export_query_read_refreshes(&reopened.observed_query_reads().unwrap())
        .unwrap();
    let exported_doc_ids = refresh_bundles[0]
        .history
        .iter()
        .filter(|record| record.table == "documents")
        .map(|record| record.row_id.as_str())
        .collect::<BTreeSet<_>>();
    assert_eq!(
        exported_doc_ids,
        BTreeSet::from(["doc-middle", "doc-old", "doc-new"])
    );
    assert_eq!(
        refresh_bundles[0]
            .history
            .iter()
            .filter(|record| record.table == "documents")
            .count(),
        4
    );

    for bundle in refresh_bundles {
        support::apply(bundle, &mut reopened).unwrap();
    }
    assert_eq!(
        reopened
            .read_rows_where_eq_top_field_desc(
                "documents",
                "owner_id",
                json!("alice"),
                "updated_at",
                3,
            )
            .unwrap()
            .iter()
            .map(|row| row.id.as_str())
            .collect::<Vec<_>>(),
        vec!["doc-middle", "doc-old"]
    );
}

#[test]
fn durable_worker_rehydrates_fresh_memory_tab_after_restart() {
    let harness = support::Harness::new();
    let mut tab = harness.memory("alice-tab", "alice").unwrap();
    tab.create_project("project-1", "Spec work").unwrap();
    tab.create_todo("todo-1", "Survives tab restart", false, "project-1")
        .unwrap();

    {
        let mut worker = harness
            .durable("worker.sqlite", "alice-worker", "alice")
            .unwrap();
        support::apply(tab.export_query_scope_open_todos().unwrap(), &mut worker).unwrap();
        assert_eq!(worker.open_todos().unwrap(), tab.open_todos().unwrap());
    }

    let worker = harness
        .durable("worker.sqlite", "alice-worker", "alice")
        .unwrap();
    let mut fresh_tab = harness.memory("alice-tab-restarted", "alice").unwrap();
    assert!(fresh_tab.open_todos().unwrap().is_empty());

    support::apply(
        worker.export_query_scope_open_todos().unwrap(),
        &mut fresh_tab,
    )
    .unwrap();

    assert_eq!(
        fresh_tab.open_todos().unwrap(),
        worker.open_todos().unwrap()
    );
    assert_eq!(fresh_tab.storage_stats().unwrap().history_rows, 2);
}

#[test]
fn rejected_transaction_remains_history_but_is_hidden_from_current() {
    let mut alice = Runtime::open(Storage::Memory, "alice-node", "alice").unwrap();

    alice.create_project("project-1", "Spec work").unwrap();
    let tx = alice
        .create_todo("todo-1", "This will be rejected", false, "project-1")
        .unwrap();

    assert_eq!(alice.open_todos().unwrap().len(), 1);

    alice.reject_transaction(&tx, "policy_denied").unwrap();

    assert!(alice.open_todos().unwrap().is_empty());
    let stats = alice.storage_stats().unwrap();
    assert_eq!(stats.history_rows, 2);
    assert_eq!(stats.rejected_transactions, 1);
    assert_eq!(
        alice.transaction_info(&tx).unwrap().rejection_code,
        Some("policy_denied".to_owned())
    );
}

#[test]
fn rejected_fate_update_repairs_peer_current_projection() {
    let mut alice = Runtime::open(Storage::Memory, "alice-node", "alice").unwrap();
    let mut peer = Runtime::open(Storage::Memory, "alice-peer-node", "alice").unwrap();

    alice.create_project("project-1", "Spec work").unwrap();
    let tx = alice
        .create_todo("todo-1", "Optimistic then rejected", false, "project-1")
        .unwrap();
    peer.apply_bundle(&alice.export_table_history("todos").unwrap())
        .unwrap();
    assert_eq!(peer.open_todos().unwrap().len(), 1);

    alice.reject_transaction(&tx, "policy_denied").unwrap();
    peer.apply_bundle(&alice.export_table_history("todos").unwrap())
        .unwrap();

    assert!(peer.open_todos().unwrap().is_empty());
    assert_eq!(peer.storage_stats().unwrap().rejected_transactions, 1);
}

#[test]
fn durable_worker_reconciles_rejected_fate_after_restart() {
    let dir = tempdir().unwrap();
    let worker_path = dir.path().join("worker.sqlite");

    let mut tab = Runtime::open(Storage::Memory, "alice-tab", "alice").unwrap();
    tab.create_project("project-1", "Spec work").unwrap();
    let tx = tab
        .create_todo("todo-1", "Optimistic before restart", false, "project-1")
        .unwrap();

    {
        let mut worker =
            Runtime::open(Storage::File(worker_path.clone()), "alice-worker", "alice").unwrap();
        worker
            .apply_bundle(&tab.export_table_history("todos").unwrap())
            .unwrap();
        assert_eq!(worker.open_todos().unwrap().len(), 1);
    }

    tab.reject_transaction(&tx, "policy_denied").unwrap();

    let mut reopened =
        Runtime::open(Storage::File(worker_path.clone()), "alice-worker", "alice").unwrap();
    assert_eq!(reopened.open_todos().unwrap().len(), 1);
    reopened
        .apply_bundle(&tab.export_table_history("todos").unwrap())
        .unwrap();

    assert!(reopened.open_todos().unwrap().is_empty());
    let stats = reopened.storage_stats().unwrap();
    assert_eq!(stats.history_rows, 1);
    assert_eq!(stats.rejected_transactions, 1);
}

#[test]
fn rejecting_generic_transaction_repairs_schema_driven_projection() {
    let schema = SchemaDef::new().table("notes", |table| {
        table.text("body");
        table.bool("pinned");
    });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema).unwrap();
    let tx = alice
        .insert_row(
            "notes",
            "note-1",
            BTreeMap::from([
                ("body".to_owned(), json!("Reject me")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    assert_eq!(alice.read_rows("notes").unwrap().len(), 1);

    alice.reject_transaction(&tx, "policy_denied").unwrap();

    assert!(alice.read_rows("notes").unwrap().is_empty());
    let stats = alice.storage_stats().unwrap();
    assert_eq!(stats.history_rows, 1);
    assert_eq!(stats.current_rows, 0);
    assert_eq!(stats.rejected_transactions, 1);
}

#[test]
fn query_scope_rejection_refresh_removes_previously_delivered_row() {
    let mut alice = Runtime::open(Storage::Memory, "alice-node", "alice").unwrap();
    let mut peer = Runtime::open(Storage::Memory, "alice-peer-node", "alice").unwrap();

    alice.create_project("project-1", "Spec work").unwrap();
    let tx = alice
        .create_todo("todo-1", "Visible before rejection", false, "project-1")
        .unwrap();

    peer.apply_bundle(&alice.export_query_scope_open_todos().unwrap())
        .unwrap();
    assert_eq!(peer.open_todos().unwrap().len(), 1);

    alice.reject_transaction(&tx, "policy_denied").unwrap();
    peer.apply_bundle(&alice.export_query_scope_open_todos().unwrap())
        .unwrap();

    assert!(peer.open_todos().unwrap().is_empty());
    assert_eq!(
        peer.transaction_info(&tx).unwrap().rejection_code,
        Some("policy_denied".to_owned())
    );
}

#[test]
fn missing_optional_ref_include_round_trips_as_null_then_updates_when_dependency_arrives() {
    let mut alice = Runtime::open(Storage::Memory, "alice-node", "alice").unwrap();
    let mut peer = Runtime::open(Storage::Memory, "alice-peer-node", "alice").unwrap();

    alice
        .create_todo("todo-1", "Project may arrive later", false, "project-late")
        .unwrap();
    let alice_todos = alice.open_todos().unwrap();
    assert_eq!(alice_todos.len(), 1);
    assert_eq!(alice_todos[0].project_title, None);

    let bundle = alice.export_query_scope_open_todos().unwrap();
    assert!(bundle.query_reads.iter().any(|read| {
        read.table == "projects"
            && read.field == "id"
            && read.op == "absent"
            && read.value == json!("project-late")
    }));

    peer.apply_bundle(&bundle).unwrap();
    assert!(peer.observed_query_reads().unwrap().iter().any(|read| {
        read.table == "projects"
            && read.field == "id"
            && read.op == "absent"
            && read.value == json!("project-late")
    }));
    let peer_todos = peer.open_todos().unwrap();
    assert_eq!(peer_todos.len(), 1);
    assert_eq!(peer_todos[0].project_title, None);

    alice
        .create_project("project-late", "Late arriving project")
        .unwrap();
    peer.apply_bundle(&alice.export_table_history("projects").unwrap())
        .unwrap();

    let peer_todos = peer.open_todos().unwrap();
    assert_eq!(peer_todos.len(), 1);
    assert_eq!(
        peer_todos[0].project_title.as_deref(),
        Some("Late arriving project")
    );
}

#[test]
fn required_ref_include_filters_parent_until_dependency_arrives() {
    let mut alice = Runtime::open(Storage::Memory, "alice-node", "alice").unwrap();

    alice
        .create_todo("todo-1", "Project required", false, "project-late")
        .unwrap();
    assert_eq!(alice.open_todos().unwrap().len(), 1);
    assert!(alice.open_todos_require_project().unwrap().is_empty());

    alice
        .create_project("project-late", "Late arriving project")
        .unwrap();

    let todos = alice.open_todos_require_project().unwrap();
    assert_eq!(todos.len(), 1);
    assert_eq!(
        todos[0].project_title.as_deref(),
        Some("Late arriving project")
    );
}

#[test]
fn required_ref_include_survives_query_scoped_sync_and_later_dependency_arrival() {
    let mut alice = Runtime::open(Storage::Memory, "alice-node", "alice").unwrap();
    let mut peer = Runtime::open(Storage::Memory, "peer-node", "alice").unwrap();

    alice
        .create_todo("todo-1", "Project required", false, "project-late")
        .unwrap();
    peer.apply_bundle(&alice.export_query_scope_open_todos().unwrap())
        .unwrap();

    assert_eq!(peer.open_todos().unwrap().len(), 1);
    assert!(peer.open_todos_require_project().unwrap().is_empty());

    alice
        .create_project("project-late", "Late arriving project")
        .unwrap();
    peer.apply_bundle(&alice.export_table_history("projects").unwrap())
        .unwrap();

    let todos = peer.open_todos_require_project().unwrap();
    assert_eq!(todos.len(), 1);
    assert_eq!(
        todos[0].project_title.as_deref(),
        Some("Late arriving project")
    );
}

#[test]
fn table_scope_sync_exports_delete_so_peer_removes_row() {
    let mut alice = Runtime::open(Storage::Memory, "alice-node", "alice").unwrap();
    let mut peer = Runtime::open(Storage::Memory, "alice-peer-node", "alice").unwrap();

    alice.create_project("project-1", "Spec work").unwrap();
    alice
        .create_todo("todo-1", "Delete through sync", false, "project-1")
        .unwrap();
    peer.apply_bundle(&alice.export_table_history("todos").unwrap())
        .unwrap();
    assert_eq!(peer.open_todos().unwrap().len(), 1);

    alice.delete_todo("todo-1").unwrap();
    peer.apply_bundle(&alice.export_table_history("todos").unwrap())
        .unwrap();

    assert!(peer.open_todos().unwrap().is_empty());
}

#[test]
fn same_bundle_twice_is_idempotent() {
    let mut alice = Runtime::open(Storage::Memory, "alice-node", "alice").unwrap();
    let mut bob = Runtime::open(Storage::Memory, "bob-node", "bob").unwrap();

    alice.create_project("project-1", "Spec work").unwrap();
    alice
        .create_todo("todo-1", "Apply twice", false, "project-1")
        .unwrap();

    let bundle = alice.export_query_scope_open_todos().unwrap();
    bob.apply_bundle(&bundle).unwrap();
    bob.apply_bundle(&bundle).unwrap();

    assert_eq!(bob.open_todos().unwrap(), alice.open_todos().unwrap());
    assert_eq!(bob.storage_stats().unwrap().history_rows, 2);
}

#[test]
fn bundle_with_unknown_table_fails_closed_without_partial_apply() {
    let mut alice = Runtime::open(Storage::Memory, "alice-node", "alice").unwrap();
    let mut peer = Runtime::open(Storage::Memory, "peer-node", "alice").unwrap();

    alice.create_project("project-1", "Spec work").unwrap();
    alice
        .create_todo("todo-1", "Unknown table bundle", false, "project-1")
        .unwrap();
    let mut bundle = alice.export_table_history("todos").unwrap();
    for record in &mut bundle.history {
        if record.table == "todos" {
            record.table = "missing_catalogue_table".to_owned();
        }
    }

    let err = peer.apply_bundle(&bundle).unwrap_err();
    assert!(err.to_string().contains("unknown table"));
    let stats = peer.storage_stats().unwrap();
    assert_eq!(stats.history_rows, 0);
    assert_eq!(stats.current_rows, 0);
    assert!(peer.open_todos().unwrap().is_empty());
}

#[test]
fn bundle_with_unknown_query_scope_fails_closed_without_partial_apply() {
    let mut alice = Runtime::open(Storage::Memory, "alice-node", "alice").unwrap();
    let mut peer = Runtime::open(Storage::Memory, "peer-node", "alice").unwrap();

    alice.create_project("project-1", "Spec work").unwrap();
    alice
        .create_todo("todo-1", "Bad query metadata", false, "project-1")
        .unwrap();
    let mut bundle = alice.export_query_scope_open_todos().unwrap();
    bundle.query_reads[0].table = "missing_catalogue_table".to_owned();

    let err = peer.apply_bundle(&bundle).unwrap_err();
    assert!(err.to_string().contains("unknown table"));
    let stats = peer.storage_stats().unwrap();
    assert_eq!(stats.history_rows, 0);
    assert_eq!(stats.current_rows, 0);
    assert!(peer.observed_query_reads().unwrap().is_empty());
    assert!(peer.open_todos().unwrap().is_empty());
}

#[test]
fn absent_query_with_unknown_field_fails_closed_without_partial_apply() {
    let mut alice = Runtime::open(Storage::Memory, "alice-node", "alice").unwrap();
    let mut peer = Runtime::open(Storage::Memory, "peer-node", "alice").unwrap();

    alice
        .create_todo(
            "todo-1",
            "Bad absent query metadata",
            false,
            "project-missing",
        )
        .unwrap();
    let mut bundle = alice.export_query_scope_open_todos().unwrap();
    let absent = bundle
        .query_reads
        .iter_mut()
        .find(|read| read.op == "absent")
        .unwrap();
    absent.field = "missing_id".to_owned();

    let err = peer.apply_bundle(&bundle).unwrap_err();
    assert!(err.to_string().contains("unknown query field"));
    let stats = peer.storage_stats().unwrap();
    assert_eq!(stats.history_rows, 0);
    assert_eq!(stats.current_rows, 0);
    assert!(peer.observed_query_reads().unwrap().is_empty());
    assert!(peer.open_todos().unwrap().is_empty());
}

#[test]
fn recursive_query_with_unknown_parent_field_fails_closed_without_partial_apply() {
    let schema = support::folders_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut peer =
        Runtime::open_with_schema(Storage::Memory, "peer-node", "alice", schema).unwrap();

    alice
        .insert_row(
            "folders",
            "root",
            BTreeMap::from([
                ("name".to_owned(), json!("Root")),
                ("parent".to_owned(), json!("root")),
            ]),
        )
        .unwrap();
    let mut bundle = alice
        .export_recursive_refs("folders", "root", "parent")
        .unwrap();
    bundle.query_reads[0].field = "missing_parent".to_owned();

    let err = peer.apply_bundle(&bundle).unwrap_err();

    assert!(err.to_string().contains("unknown query field"));
    let stats = peer.storage_stats().unwrap();
    assert_eq!(stats.history_rows, 0);
    assert_eq!(stats.current_rows, 0);
    assert!(peer.observed_query_reads().unwrap().is_empty());
}

#[test]
fn durable_peer_remembers_query_scope_after_restart() {
    let dir = tempdir().unwrap();
    let peer_path = dir.path().join("peer.sqlite");
    let mut alice = Runtime::open(Storage::Memory, "alice-node", "alice").unwrap();

    alice.create_project("project-1", "Spec work").unwrap();
    alice
        .create_todo("todo-1", "Remembered query", false, "project-1")
        .unwrap();
    let bundle = alice.export_query_scope_open_todos().unwrap();

    {
        let mut peer =
            Runtime::open(Storage::File(peer_path.clone()), "peer-node", "alice").unwrap();
        peer.apply_bundle(&bundle).unwrap();
        peer.apply_bundle(&bundle).unwrap();
        assert_eq!(peer.observed_query_reads().unwrap(), bundle.query_reads);
    }

    let reopened = Runtime::open(Storage::File(peer_path), "peer-node", "alice").unwrap();
    assert_eq!(reopened.observed_query_reads().unwrap(), bundle.query_reads);
}

#[test]
fn forgotten_query_read_does_not_refresh_after_restart() {
    let dir = tempdir().unwrap();
    let peer_path = dir.path().join("forgotten-query.sqlite");
    let mut alice = Runtime::open(Storage::Memory, "alice-node", "alice").unwrap();

    alice.create_project("project-1", "Spec work").unwrap();
    alice
        .create_todo("todo-1", "Forget this query", false, "project-1")
        .unwrap();
    let bundle = alice.export_query_scope_open_todos().unwrap();

    {
        let mut peer =
            Runtime::open(Storage::File(peer_path.clone()), "peer-node", "alice").unwrap();
        peer.apply_bundle(&bundle).unwrap();
        let observed = peer.observed_query_reads().unwrap();
        assert_eq!(observed, bundle.query_reads);

        peer.forget_observed_query_read(&observed[0]).unwrap();
        assert!(peer.observed_query_reads().unwrap().is_empty());
    }

    let reopened = Runtime::open(Storage::File(peer_path), "peer-node", "alice").unwrap();
    assert!(reopened.observed_query_reads().unwrap().is_empty());
    assert!(alice
        .export_query_read_refreshes(&reopened.observed_query_reads().unwrap())
        .unwrap()
        .is_empty());
}

#[test]
fn exported_bundles_carry_protocol_version() {
    let mut alice = Runtime::open(Storage::Memory, "alice-node", "alice").unwrap();

    alice.create_project("project-1", "Spec work").unwrap();
    alice
        .create_todo("todo-1", "Versioned bundle", false, "project-1")
        .unwrap();

    let bundle = alice.export_query_scope_open_todos().unwrap();

    assert_eq!(bundle.protocol_version, 1);
    assert_ne!(bundle.schema_fingerprint, "legacy");
    assert_ne!(bundle.policy_fingerprint, "legacy");
}

#[test]
fn native_bundles_decode_protocol_metadata() {
    let mut alice = Runtime::open(Storage::Memory, "alice-node", "alice").unwrap();

    alice.create_project("project-1", "Spec work").unwrap();
    let exported = alice.export_query_scope_open_todos().unwrap();
    let encoded = mini_jazz_sqlite::sync::encode_bundle(&exported).unwrap();
    let bundle = mini_jazz_sqlite::sync::decode_bundle(&encoded).unwrap();

    assert_eq!(bundle.protocol_version, 1);
    assert_eq!(bundle.schema_fingerprint, exported.schema_fingerprint);
    assert_eq!(bundle.policy_fingerprint, exported.policy_fingerprint);
}

#[test]
fn incompatible_schema_fingerprint_fails_closed_without_partial_apply() {
    let writer_schema = SchemaDef::new().table("notes", |table| {
        table.text("body");
        table.bool("pinned");
    });
    let receiver_schema = SchemaDef::new().table("notes", |table| {
        table.text("body");
    });
    let mut writer =
        Runtime::open_with_schema(Storage::Memory, "writer", "alice", writer_schema).unwrap();
    let mut receiver =
        Runtime::open_with_schema(Storage::Memory, "receiver", "alice", receiver_schema).unwrap();

    writer
        .insert_row(
            "notes",
            "note-1",
            BTreeMap::from([
                ("body".to_owned(), json!("schema mismatch")),
                ("pinned".to_owned(), json!(true)),
            ]),
        )
        .unwrap();
    let bundle = writer.export_table_history("notes").unwrap();

    let err = receiver.apply_bundle(&bundle).unwrap_err();

    assert!(err.to_string().contains("incompatible schema"));
    assert_eq!(receiver.storage_stats().unwrap().history_rows, 0);
    assert!(receiver.read_rows("notes").unwrap().is_empty());
}

#[test]
fn permission_fingerprint_mismatch_fails_closed_without_partial_apply() {
    let writer_schema = SchemaDef::new().table("notes", |table| {
        table.text("body");
        table.bool("pinned");
    });
    let receiver_schema = SchemaDef::new().table("notes", |table| {
        table.text("body");
        table.bool("pinned");
        table.write_if_created_by_user();
    });
    let mut writer =
        Runtime::open_with_schema(Storage::Memory, "writer", "alice", writer_schema).unwrap();
    let mut receiver =
        Runtime::open_with_schema(Storage::Memory, "receiver", "bob", receiver_schema).unwrap();

    writer
        .insert_row(
            "notes",
            "note-1",
            BTreeMap::from([
                ("body".to_owned(), json!("policy mismatch")),
                ("pinned".to_owned(), json!(true)),
            ]),
        )
        .unwrap();
    let bundle = writer.export_table_history("notes").unwrap();

    let err = receiver.apply_bundle(&bundle).unwrap_err();

    assert!(err.to_string().contains("incompatible policy"));
    assert_eq!(receiver.storage_stats().unwrap().history_rows, 0);
    assert!(receiver.read_rows("notes").unwrap().is_empty());
}

#[test]
fn future_bundle_protocol_versions_fail_closed_without_partial_apply() {
    let mut alice = Runtime::open(Storage::Memory, "alice-node", "alice").unwrap();
    let mut peer = Runtime::open(Storage::Memory, "peer-node", "alice").unwrap();

    alice.create_project("project-1", "Spec work").unwrap();
    alice
        .create_todo("todo-1", "Future bundle", false, "project-1")
        .unwrap();
    let mut bundle = alice.export_query_scope_open_todos().unwrap();
    bundle.protocol_version = 2;

    let err = peer.apply_bundle(&bundle).unwrap_err();

    assert!(err
        .to_string()
        .contains("unsupported bundle protocol version"));
    assert_eq!(peer.storage_stats().unwrap().history_rows, 0);
    assert!(peer.open_todos().unwrap().is_empty());
}

#[test]
fn replicas_may_use_different_physical_ids_for_same_public_ids() {
    let mut alice = Runtime::open(Storage::Memory, "alice-node", "alice").unwrap();
    let mut bob = Runtime::open(Storage::Memory, "bob-node", "bob").unwrap();

    bob.create_project("bob-local-project", "Bob local")
        .unwrap();

    alice.create_project("project-1", "Spec work").unwrap();
    alice
        .create_todo("todo-1", "Different physical ids", false, "project-1")
        .unwrap();
    let bundle = alice.export_query_scope_open_todos().unwrap();
    bob.apply_bundle(&bundle).unwrap();

    assert_eq!(bob.open_todos().unwrap(), alice.open_todos().unwrap());
    assert_ne!(
        alice.physical_row_num_for("project-1").unwrap(),
        bob.physical_row_num_for("project-1").unwrap()
    );
}

#[test]
fn replicas_may_use_different_physical_tx_nums_for_same_public_tx_id() {
    let mut alice = Runtime::open(Storage::Memory, "alice-node", "alice").unwrap();
    let mut bob = Runtime::open(Storage::Memory, "bob-node", "bob").unwrap();

    bob.create_project("bob-local-project", "Bob local")
        .unwrap();

    alice.create_project("project-1", "Spec work").unwrap();
    let tx = alice
        .create_todo(
            "todo-1",
            "Same public tx different physical nums",
            false,
            "project-1",
        )
        .unwrap();
    bob.apply_bundle(&alice.export_query_scope_open_todos().unwrap())
        .unwrap();

    assert_eq!(bob.open_todos().unwrap(), alice.open_todos().unwrap());
    assert_ne!(
        alice
            .storage_stats()
            .unwrap()
            .physical_tx_num_for(&tx)
            .unwrap(),
        bob.storage_stats()
            .unwrap()
            .physical_tx_num_for(&tx)
            .unwrap()
    );
}

#[test]
fn same_user_on_two_nodes_preserves_authorship_and_distinct_node_epochs() {
    let schema = support::notes_schema();
    let mut alice_phone =
        Runtime::open_with_schema(Storage::Memory, "alice-phone", "alice", schema.clone()).unwrap();
    let mut alice_laptop =
        Runtime::open_with_schema(Storage::Memory, "alice-laptop", "alice", schema.clone())
            .unwrap();
    let mut peer =
        Runtime::open_with_schema(Storage::Memory, "peer-node", "alice", schema).unwrap();

    let phone_tx = alice_phone
        .insert_row(
            "notes",
            "note-phone",
            BTreeMap::from([
                ("body".to_owned(), json!("From phone")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    let laptop_tx = alice_laptop
        .insert_row(
            "notes",
            "note-laptop",
            BTreeMap::from([
                ("body".to_owned(), json!("From laptop")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .unwrap();

    assert_eq!(phone_tx, "tx-alice-phone-1");
    assert_eq!(laptop_tx, "tx-alice-laptop-1");

    peer.apply_bundle(&alice_phone.export_table_history("notes").unwrap())
        .unwrap();
    peer.apply_bundle(&alice_laptop.export_table_history("notes").unwrap())
        .unwrap();

    let rows = peer.read_rows("notes").unwrap();
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().all(|row| row.created_by == "alice"));
    assert!(rows.iter().any(|row| row.tx_id == phone_tx));
    assert!(rows.iter().any(|row| row.tx_id == laptop_tx));
}

#[test]
fn query_scope_is_not_table_replication() {
    let mut alice = Runtime::open(Storage::Memory, "alice-node", "alice").unwrap();
    let mut bob = Runtime::open(Storage::Memory, "bob-node", "bob").unwrap();

    alice
        .create_project("project-1", "Visible project")
        .unwrap();
    alice
        .create_todo("todo-1", "In query scope", false, "project-1")
        .unwrap();
    alice
        .create_project("project-2", "Unrelated project")
        .unwrap();

    let bundle = alice.export_query_scope_open_todos().unwrap();
    bob.apply_bundle(&bundle).unwrap();

    assert_eq!(bob.open_todos().unwrap(), alice.open_todos().unwrap());
    assert!(bob.physical_row_num_for("project-1").is_ok());
    assert!(bob.physical_row_num_for("project-2").is_err());
}

#[test]
fn query_scope_excludes_rows_outside_current_result_set() {
    let mut alice = Runtime::open(Storage::Memory, "alice-node", "alice").unwrap();
    let mut bob = Runtime::open(Storage::Memory, "bob-node", "bob").unwrap();

    alice
        .create_project("project-1", "Visible project")
        .unwrap();
    alice
        .create_todo("todo-open", "In query scope", false, "project-1")
        .unwrap();
    alice
        .create_todo("todo-closed", "Outside query scope", true, "project-1")
        .unwrap();

    let bundle = alice.export_query_scope_open_todos().unwrap();
    let synced_todos = bundle
        .history
        .iter()
        .filter(|record| record.table == "todos")
        .map(|record| record.row_id.as_str())
        .collect::<Vec<_>>();
    assert!(synced_todos.contains(&"todo-open"));
    assert!(!synced_todos.contains(&"todo-closed"));

    bob.apply_bundle(&bundle).unwrap();
    assert_eq!(bob.open_todos().unwrap(), alice.open_todos().unwrap());
    assert!(bob.physical_row_num_for("todo-open").is_ok());
    assert!(bob.physical_row_num_for("todo-closed").is_err());
}

#[test]
fn top_created_at_query_scope_refresh_replaces_displaced_page_boundary_row() {
    let mut alice = Runtime::open(Storage::Memory, "alice-node", "alice").unwrap();
    let mut peer = Runtime::open(Storage::Memory, "peer-node", "alice").unwrap();

    alice.create_project("project-1", "Spec work").unwrap();
    alice
        .create_todo("todo-old", "Old boundary", false, "project-1")
        .unwrap();
    std::thread::sleep(std::time::Duration::from_millis(2));
    alice
        .create_todo("todo-middle", "Middle", false, "project-1")
        .unwrap();

    peer.apply_bundle(&alice.export_query_scope_newest_open_todos(2).unwrap())
        .unwrap();
    assert_eq!(
        peer.observed_query_reads().unwrap()[0].op,
        "eq_top_created_at_desc"
    );
    assert_eq!(
        peer.newest_open_todos(2)
            .unwrap()
            .iter()
            .map(|todo| todo.id.as_str())
            .collect::<Vec<_>>(),
        vec!["todo-middle", "todo-old"]
    );

    std::thread::sleep(std::time::Duration::from_millis(2));
    alice
        .create_todo("todo-new", "Newest", false, "project-1")
        .unwrap();
    peer.apply_bundle(&alice.export_query_scope_newest_open_todos(2).unwrap())
        .unwrap();

    assert_eq!(
        peer.newest_open_todos(3)
            .unwrap()
            .iter()
            .map(|todo| todo.id.as_str())
            .collect::<Vec<_>>(),
        vec!["todo-new", "todo-middle"]
    );
}

#[test]
fn accepted_global_fate_update_reaches_peer_transaction_info() {
    let mut alice = Runtime::open(Storage::Memory, "alice-node", "alice").unwrap();
    let mut peer = Runtime::open(Storage::Memory, "alice-peer-node", "alice").unwrap();

    alice.create_project("project-1", "Spec work").unwrap();
    let tx = alice
        .create_todo("todo-1", "Accept me remotely", false, "project-1")
        .unwrap();
    peer.apply_bundle(&alice.export_table_history("todos").unwrap())
        .unwrap();
    assert_eq!(peer.transaction_info(&tx).unwrap().global_epoch, None);

    alice.accept_transaction_at_global(&tx, 7).unwrap();
    peer.apply_bundle(&alice.export_table_history("todos").unwrap())
        .unwrap();

    let info = peer.transaction_info(&tx).unwrap();
    assert_eq!(info.global_epoch, Some(7));
    assert!(info.receipt_tiers.contains(&"global".to_owned()));
}

#[test]
fn stale_pending_bundle_does_not_downgrade_accepted_fate() {
    let mut alice = Runtime::open(Storage::Memory, "alice-node", "alice").unwrap();
    let mut peer = Runtime::open(Storage::Memory, "alice-peer-node", "alice").unwrap();

    alice.create_project("project-1", "Spec work").unwrap();
    let tx = alice
        .create_todo("todo-1", "Out of order fate", false, "project-1")
        .unwrap();
    let pending_bundle = alice.export_table_history("todos").unwrap();
    peer.apply_bundle(&pending_bundle).unwrap();

    alice.accept_transaction_at_global(&tx, 7).unwrap();
    peer.apply_bundle(&alice.export_table_history("todos").unwrap())
        .unwrap();
    assert_eq!(peer.transaction_info(&tx).unwrap().global_epoch, Some(7));

    peer.apply_bundle(&pending_bundle).unwrap();
    assert_eq!(peer.transaction_info(&tx).unwrap().global_epoch, Some(7));
    assert_eq!(
        peer.transaction_info(&tx).unwrap().conflict_mode,
        "mergeable"
    );
}

#[test]
fn rejected_fate_arriving_before_history_keeps_later_rows_invisible() {
    let mut alice = Runtime::open(Storage::Memory, "alice-node", "alice").unwrap();
    let mut peer = Runtime::open(Storage::Memory, "alice-peer-node", "alice").unwrap();

    alice.create_project("project-1", "Spec work").unwrap();
    let tx = alice
        .create_todo("todo-1", "Fate before rows", false, "project-1")
        .unwrap();
    let history_bundle = alice.export_table_history("todos").unwrap();
    let mut fate_first_bundle = history_bundle.clone();
    fate_first_bundle.history.clear();
    fate_first_bundle.reads.clear();

    alice.reject_transaction(&tx, "policy_denied").unwrap();
    let rejected_bundle = alice.export_table_history("todos").unwrap();
    fate_first_bundle.txs = rejected_bundle.txs;

    peer.apply_bundle(&fate_first_bundle).unwrap();
    assert_eq!(
        peer.transaction_info(&tx).unwrap().rejection_code,
        Some("policy_denied".to_owned())
    );

    peer.apply_bundle(&history_bundle).unwrap();

    assert!(peer.open_todos().unwrap().is_empty());
    assert_eq!(peer.storage_stats().unwrap().history_rows, 1);
    assert_eq!(
        peer.transaction_info(&tx).unwrap().rejection_code,
        Some("policy_denied".to_owned())
    );
}

#[test]
fn accepted_fate_arriving_before_history_materializes_later_rows() {
    let mut alice = Runtime::open(Storage::Memory, "alice-node", "alice").unwrap();
    let mut peer = Runtime::open(Storage::Memory, "alice-peer-node", "alice").unwrap();

    alice.create_project("project-1", "Spec work").unwrap();
    let tx = alice
        .create_todo("todo-1", "Accepted fate before rows", false, "project-1")
        .unwrap();
    let history_bundle = alice.export_table_history("todos").unwrap();

    alice.accept_transaction_at_global(&tx, 7).unwrap();
    let accepted_bundle = alice.export_table_history("todos").unwrap();
    let mut fate_first_bundle = accepted_bundle.clone();
    fate_first_bundle.history.clear();
    fate_first_bundle.reads.clear();

    peer.apply_bundle(&fate_first_bundle).unwrap();
    assert_eq!(peer.transaction_info(&tx).unwrap().global_epoch, Some(7));
    assert!(peer.open_todos().unwrap().is_empty());

    peer.apply_bundle(&history_bundle).unwrap();

    let todos = peer.open_todos().unwrap();
    assert_eq!(todos.len(), 1);
    assert_eq!(todos[0].id, "todo-1");
    assert_eq!(todos[0].tx_id, tx);
    assert_eq!(peer.transaction_info(&tx).unwrap().global_epoch, Some(7));
}

#[test]
fn stale_pending_bundle_does_not_resurrect_rejected_fate_after_reconnect() {
    let dir = tempdir().unwrap();
    let worker_path = dir.path().join("worker.sqlite");

    let mut alice = Runtime::open(Storage::Memory, "alice-tab", "alice").unwrap();
    let mut worker =
        Runtime::open(Storage::File(worker_path.clone()), "alice-worker", "alice").unwrap();
    let mut stale_phone = Runtime::open(Storage::Memory, "alice-phone", "alice").unwrap();

    alice.create_project("project-1", "Spec work").unwrap();
    let tx = alice
        .create_todo(
            "todo-1",
            "Rejected while phone is offline",
            false,
            "project-1",
        )
        .unwrap();
    let pending_bundle = alice.export_table_history("todos").unwrap();
    stale_phone.apply_bundle(&pending_bundle).unwrap();
    worker.apply_bundle(&pending_bundle).unwrap();

    worker.reject_transaction(&tx, "policy_denied").unwrap();
    worker
        .reject_transaction_with_detail(&tx, "policy_denied", json!({"reason": "authority"}))
        .unwrap();
    assert!(worker.open_todos().unwrap().is_empty());

    drop(worker);
    let mut worker = Runtime::open(Storage::File(worker_path), "alice-worker", "alice").unwrap();
    assert_eq!(
        worker.transaction_info(&tx).unwrap().rejection_detail,
        Some(json!({"reason": "authority"}))
    );

    worker.apply_bundle(&pending_bundle).unwrap();
    worker
        .apply_bundle(&stale_phone.export_table_history("todos").unwrap())
        .unwrap();

    assert!(worker.open_todos().unwrap().is_empty());
    assert_eq!(
        worker.transaction_info(&tx).unwrap().rejection_code,
        Some("policy_denied".to_owned())
    );
    assert_eq!(
        worker.transaction_info(&tx).unwrap().rejection_detail,
        Some(json!({"reason": "authority"}))
    );
}

#[test]
fn top_field_query_with_ref_include_syncs_page_and_dependency() {
    let mut alice = Runtime::open(Storage::Memory, "alice-node", "alice").unwrap();
    let mut peer = Runtime::open(Storage::Memory, "peer-node", "alice").unwrap();

    alice.create_project("project-1", "Spec work").unwrap();
    alice
        .create_todo("todo-a", "A older", false, "project-1")
        .unwrap();
    alice
        .create_todo("todo-z", "Z newer", false, "project-1")
        .unwrap();

    let bundle = alice
        .export_query_where_eq_top_field_desc_with_ref_include(
            "todos",
            "done",
            json!(false),
            "title",
            1,
            "project",
        )
        .unwrap();
    peer.apply_bundle(&bundle).unwrap();

    let todos = peer.open_todos_require_project().unwrap();
    assert_eq!(todos.len(), 1);
    assert_eq!(todos[0].id, "todo-z");
    assert_eq!(todos[0].project_title.as_deref(), Some("Spec work"));
}

#[test]
fn batched_top_field_ref_include_matches_individual_exports() {
    let mut alice = Runtime::open(Storage::Memory, "alice-node", "alice").unwrap();
    let mut individual_peer = Runtime::open(Storage::Memory, "individual-peer", "alice").unwrap();
    let mut batch_peer = Runtime::open(Storage::Memory, "batch-peer", "alice").unwrap();

    alice.create_project("project-1", "Spec work").unwrap();
    alice
        .create_todo("todo-open-a", "A open", false, "project-1")
        .unwrap();
    alice
        .create_todo("todo-open-z", "Z open", false, "project-1")
        .unwrap();
    alice
        .create_todo("todo-done-z", "Z done", true, "project-1")
        .unwrap();

    let open = alice
        .export_query_where_eq_top_field_desc_with_ref_include(
            "todos",
            "done",
            json!(false),
            "title",
            1,
            "project",
        )
        .unwrap();
    let done = alice
        .export_query_where_eq_top_field_desc_with_ref_include(
            "todos",
            "done",
            json!(true),
            "title",
            1,
            "project",
        )
        .unwrap();
    individual_peer.apply_bundle(&open).unwrap();
    individual_peer.apply_bundle(&done).unwrap();

    let batch = alice
        .export_many_query_where_eq_top_field_desc_with_ref_include(
            "todos",
            "done",
            vec![json!(false), json!(true)],
            "title",
            1,
            "project",
        )
        .unwrap();
    batch_peer.apply_bundle(&batch).unwrap();

    assert_eq!(
        batch_peer.open_todos_require_project().unwrap(),
        individual_peer.open_todos_require_project().unwrap()
    );
    assert_eq!(batch.query_reads.len(), 2);
}

#[test]
fn batched_top_field_query_matches_individual_exports_without_include() {
    let mut alice = Runtime::open(Storage::Memory, "alice-node", "alice").unwrap();
    let mut individual_peer = Runtime::open(Storage::Memory, "individual-peer", "alice").unwrap();
    let mut batch_peer = Runtime::open(Storage::Memory, "batch-peer", "alice").unwrap();

    alice.create_project("project-1", "Spec work").unwrap();
    alice
        .create_todo("todo-open-a", "A open", false, "project-1")
        .unwrap();
    alice
        .create_todo("todo-open-z", "Z open", false, "project-1")
        .unwrap();
    alice
        .create_todo("todo-done-z", "Z done", true, "project-1")
        .unwrap();

    let open = alice
        .export_query_where_eq_top_field_desc("todos", "done", json!(false), "title", 1)
        .unwrap();
    let done = alice
        .export_query_where_eq_top_field_desc("todos", "done", json!(true), "title", 1)
        .unwrap();
    individual_peer.apply_bundle(&open).unwrap();
    individual_peer.apply_bundle(&done).unwrap();

    let batch = alice
        .export_many_query_where_eq_top_field_desc(
            "todos",
            "done",
            vec![json!(false), json!(true)],
            "title",
            1,
        )
        .unwrap();
    batch_peer.apply_bundle(&batch).unwrap();

    assert_eq!(
        batch_peer.open_todos().unwrap(),
        individual_peer.open_todos().unwrap()
    );
    assert_eq!(batch.query_reads.len(), 2);
}

#[test]
fn top_field_query_read_refresh_batches_same_shape_queries() {
    let mut alice = Runtime::open(Storage::Memory, "alice-node", "alice").unwrap();
    let mut peer = Runtime::open(Storage::Memory, "peer-node", "alice").unwrap();

    alice.create_project("project-1", "Spec work").unwrap();
    alice
        .create_todo("todo-open-a", "A open", false, "project-1")
        .unwrap();
    alice
        .create_todo("todo-open-z", "Z open", false, "project-1")
        .unwrap();
    alice
        .create_todo("todo-done-a", "A done", true, "project-1")
        .unwrap();
    alice
        .create_todo("todo-done-z", "Z done", true, "project-1")
        .unwrap();

    let initial = alice
        .export_many_query_where_eq_top_field_desc(
            "todos",
            "done",
            vec![json!(false), json!(true)],
            "title",
            1,
        )
        .unwrap();
    peer.apply_bundle(&initial).unwrap();
    assert_eq!(peer.observed_query_reads().unwrap().len(), 2);

    alice
        .transaction()
        .update_row(
            "todos",
            "todo-open-a",
            BTreeMap::from([("title".to_owned(), json!("ZZZ open"))]),
        )
        .commit()
        .unwrap();
    alice
        .transaction()
        .update_row(
            "todos",
            "todo-done-a",
            BTreeMap::from([("title".to_owned(), json!("ZZZ done"))]),
        )
        .commit()
        .unwrap();

    let refreshes = alice
        .export_query_read_refreshes(&peer.observed_query_reads().unwrap())
        .unwrap();
    assert_eq!(refreshes.len(), 1);
    peer.apply_bundle(&refreshes[0]).unwrap();

    let rows = peer.read_rows("todos").unwrap();
    assert!(rows.iter().any(|row| row.id == "todo-open-a"));
    assert!(rows.iter().any(|row| row.id == "todo-done-a"));
    assert!(!rows.iter().any(|row| row.id == "todo-open-z"));
    assert!(!rows.iter().any(|row| row.id == "todo-done-z"));
}

#[test]
fn top_created_at_query_read_refresh_batches_same_shape_queries() {
    let mut alice = Runtime::open(Storage::Memory, "alice-node", "alice").unwrap();
    let mut peer = Runtime::open(Storage::Memory, "peer-node", "alice").unwrap();

    alice.create_project("project-1", "Spec work").unwrap();
    alice
        .create_todo("todo-open-old", "Old open", false, "project-1")
        .unwrap();
    alice
        .create_todo("todo-open-new", "New open", false, "project-1")
        .unwrap();
    alice
        .create_todo("todo-done-old", "Old done", true, "project-1")
        .unwrap();
    alice
        .create_todo("todo-done-new", "New done", true, "project-1")
        .unwrap();

    let open = alice
        .export_query_where_eq_top_created_at_desc("todos", "done", json!(false), 1)
        .unwrap();
    let done = alice
        .export_query_where_eq_top_created_at_desc("todos", "done", json!(true), 1)
        .unwrap();
    peer.apply_bundle(&open).unwrap();
    peer.apply_bundle(&done).unwrap();
    assert_eq!(peer.observed_query_reads().unwrap().len(), 2);

    alice
        .create_todo("todo-open-newest", "Newest open", false, "project-1")
        .unwrap();
    alice
        .create_todo("todo-done-newest", "Newest done", true, "project-1")
        .unwrap();

    let refreshes = alice
        .export_query_read_refreshes(&peer.observed_query_reads().unwrap())
        .unwrap();
    assert_eq!(refreshes.len(), 1);
    peer.apply_bundle(&refreshes[0]).unwrap();

    let rows = peer.read_rows("todos").unwrap();
    assert!(rows.iter().any(|row| row.id == "todo-open-newest"));
    assert!(rows.iter().any(|row| row.id == "todo-done-newest"));
    assert!(!rows.iter().any(|row| row.id == "todo-open-new"));
    assert!(!rows.iter().any(|row| row.id == "todo-done-new"));
}

#[test]
fn predicate_query_read_refresh_batches_same_shape_queries() {
    let mut alice = Runtime::open(Storage::Memory, "alice-node", "alice").unwrap();
    let mut peer = Runtime::open(Storage::Memory, "peer-node", "alice").unwrap();

    alice.create_project("project-1", "Spec work").unwrap();
    alice
        .create_todo("todo-open-old", "Old open", false, "project-1")
        .unwrap();
    alice
        .create_todo("todo-done-old", "Old done", true, "project-1")
        .unwrap();

    let open = alice
        .export_query_where_eq("todos", "done", json!(false))
        .unwrap();
    let done = alice
        .export_query_where_eq("todos", "done", json!(true))
        .unwrap();
    peer.apply_bundle(&open).unwrap();
    peer.apply_bundle(&done).unwrap();
    assert_eq!(peer.observed_query_reads().unwrap().len(), 2);

    alice
        .create_todo("todo-open-new", "New open", false, "project-1")
        .unwrap();
    alice
        .create_todo("todo-done-new", "New done", true, "project-1")
        .unwrap();

    let refreshes = alice
        .export_query_read_refreshes(&peer.observed_query_reads().unwrap())
        .unwrap();
    assert_eq!(refreshes.len(), 1);
    peer.apply_bundle(&refreshes[0]).unwrap();

    let rows = peer.read_rows("todos").unwrap();
    assert!(rows.iter().any(|row| row.id == "todo-open-new"));
    assert!(rows.iter().any(|row| row.id == "todo-done-new"));
}

#[test]
fn non_eq_predicate_query_read_refreshes_batch_by_operator() {
    let mut alice = Runtime::open(Storage::Memory, "alice-node", "alice").unwrap();
    let mut peer = Runtime::open(Storage::Memory, "peer-node", "alice").unwrap();

    alice.create_project("project-1", "Spec work").unwrap();
    for (id, title, done) in [
        ("todo-alpha-open", "Alpha open", false),
        ("todo-alpha-done", "Alpha done", true),
        ("todo-beta-open", "Beta open", false),
        ("todo-beta-done", "Beta done", true),
    ] {
        alice.create_todo(id, title, done, "project-1").unwrap();
    }

    for bundle in [
        alice
            .export_query_where_contains("todos", "title", "Alpha")
            .unwrap(),
        alice
            .export_query_where_contains("todos", "title", "Beta")
            .unwrap(),
        alice
            .export_query_where_in("todos", "title", vec![json!("Alpha open")])
            .unwrap(),
        alice
            .export_query_where_in("todos", "title", vec![json!("Beta open")])
            .unwrap(),
        alice
            .export_query_where_ne("todos", "done", json!(false))
            .unwrap(),
        alice
            .export_query_where_ne("todos", "done", json!(true))
            .unwrap(),
    ] {
        peer.apply_bundle(&bundle).unwrap();
    }
    assert_eq!(peer.observed_query_reads().unwrap().len(), 6);

    alice
        .transaction()
        .update_row(
            "todos",
            "todo-alpha-open",
            BTreeMap::from([("title".to_owned(), json!("Gamma open"))]),
        )
        .commit()
        .unwrap();
    alice
        .transaction()
        .update_row(
            "todos",
            "todo-beta-open",
            BTreeMap::from([("done".to_owned(), json!(true))]),
        )
        .commit()
        .unwrap();

    let refreshes = alice
        .export_query_read_refreshes(&peer.observed_query_reads().unwrap())
        .unwrap();
    let query_read_counts = refreshes
        .iter()
        .map(|bundle| bundle.query_reads.len())
        .collect::<Vec<_>>();
    assert_eq!(query_read_counts, vec![2, 2, 2]);

    for refresh in refreshes {
        peer.apply_bundle(&refresh).unwrap();
    }
    let rows = peer.read_rows("todos").unwrap();
    let alpha_open = rows.iter().find(|row| row.id == "todo-alpha-open").unwrap();
    let beta_open = rows.iter().find(|row| row.id == "todo-beta-open").unwrap();
    assert_eq!(alpha_open.values["title"], json!("Gamma open"));
    assert_eq!(beta_open.values["done"], json!(true));
}

#[test]
fn top_created_at_query_can_filter_by_created_by_magic_field() {
    let mut alice = Runtime::open(Storage::Memory, "alice-node", "alice").unwrap();
    let mut peer = Runtime::open(Storage::Memory, "peer-node", "alice").unwrap();

    alice.create_project("project-1", "Spec work").unwrap();
    alice
        .create_todo("todo-old", "Old authored todo", false, "project-1")
        .unwrap();
    alice
        .create_todo("todo-new", "New authored todo", false, "project-1")
        .unwrap();

    let bundle = alice
        .export_query_where_eq_top_created_at_desc("todos", "$createdBy", json!("alice"), 1)
        .unwrap();
    peer.apply_bundle(&bundle).unwrap();

    let rows = peer.read_rows("todos").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, "todo-new");
    assert_eq!(peer.observed_query_reads().unwrap()[0].field, "$createdBy");
}

#[test]
fn missing_optional_ref_include_observed_refresh_delivers_later_dependency() {
    let mut alice = Runtime::open(Storage::Memory, "alice-node", "alice").unwrap();
    let mut peer = Runtime::open(Storage::Memory, "alice-peer-node", "alice").unwrap();

    alice
        .create_todo(
            "todo-1",
            "Project may arrive by refresh",
            false,
            "project-late",
        )
        .unwrap();
    peer.apply_bundle(&alice.export_query_scope_open_todos().unwrap())
        .unwrap();
    assert_eq!(peer.open_todos().unwrap()[0].project_title, None);
    let observed = peer.observed_query_reads().unwrap();
    assert!(observed.iter().any(|read| {
        read.table == "projects"
            && read.field == "id"
            && read.op == "absent"
            && read.value == json!("project-late")
    }));

    alice
        .create_project("project-late", "Late arriving project")
        .unwrap();
    for refresh in alice.export_query_read_refreshes(&observed).unwrap() {
        peer.apply_bundle(&refresh).unwrap();
    }

    let peer_todos = peer.open_todos().unwrap();
    assert_eq!(peer_todos.len(), 1);
    assert_eq!(
        peer_todos[0].project_title.as_deref(),
        Some("Late arriving project")
    );
}

#[test]
fn optional_ref_include_observed_refresh_removes_deleted_dependency_again() {
    let mut alice = Runtime::open(Storage::Memory, "alice-node", "alice").unwrap();
    let mut peer = Runtime::open(Storage::Memory, "alice-peer-node", "alice").unwrap();

    alice
        .create_todo(
            "todo-1",
            "Project can disappear again",
            false,
            "project-late",
        )
        .unwrap();
    peer.apply_bundle(&alice.export_query_scope_open_todos().unwrap())
        .unwrap();

    alice
        .create_project("project-late", "Late arriving project")
        .unwrap();
    for refresh in alice
        .export_query_read_refreshes(&peer.observed_query_reads().unwrap())
        .unwrap()
    {
        peer.apply_bundle(&refresh).unwrap();
    }
    assert_eq!(
        peer.open_todos().unwrap()[0].project_title.as_deref(),
        Some("Late arriving project")
    );

    alice.delete_row("projects", "project-late").unwrap();
    for refresh in alice
        .export_query_read_refreshes(&peer.observed_query_reads().unwrap())
        .unwrap()
    {
        peer.apply_bundle(&refresh).unwrap();
    }

    let peer_todos = peer.open_todos().unwrap();
    assert_eq!(peer_todos.len(), 1);
    assert_eq!(peer_todos[0].project_title, None);
}

#[test]
fn out_of_order_global_epochs_do_not_regress_current_projection() {
    let schema = support::notes_schema();
    let mut authority =
        Runtime::open_with_schema(Storage::Memory, "authority", "alice", schema.clone()).unwrap();
    let mut peer = Runtime::open_with_schema(Storage::Memory, "peer", "alice", schema).unwrap();

    let first_tx = authority
        .insert_row(
            "notes",
            "note-1",
            BTreeMap::from([
                ("body".to_owned(), json!("epoch 10")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    authority
        .accept_transaction_at_global(&first_tx, 10)
        .unwrap();
    let first_bundle = authority.export_table_history("notes").unwrap();

    let second_tx = authority
        .update_row(
            "notes",
            "note-1",
            BTreeMap::from([
                ("body".to_owned(), json!("epoch 20")),
                ("pinned".to_owned(), json!(true)),
            ]),
        )
        .unwrap();
    authority
        .accept_transaction_at_global(&second_tx, 20)
        .unwrap();
    let second_bundle = authority.export_table_history("notes").unwrap();

    peer.apply_bundle(&second_bundle).unwrap();
    peer.apply_bundle(&first_bundle).unwrap();

    assert_eq!(
        peer.read_rows("notes").unwrap()[0].values["body"],
        json!("epoch 20")
    );

    peer.clear_current_projection_for_test().unwrap();
    peer.rebuild_current_projection().unwrap();
    assert_eq!(
        peer.read_rows("notes").unwrap()[0].values["body"],
        json!("epoch 20")
    );
}

#[test]
fn rebuild_uses_global_epoch_order_not_local_tx_order() {
    let schema = support::notes_schema();
    let mut authority =
        Runtime::open_with_schema(Storage::Memory, "authority", "alice", schema.clone()).unwrap();
    let mut peer = Runtime::open_with_schema(Storage::Memory, "peer", "alice", schema).unwrap();

    let first_tx = authority
        .insert_row(
            "notes",
            "note-1",
            BTreeMap::from([
                ("body".to_owned(), json!("epoch 20")),
                ("pinned".to_owned(), json!(true)),
            ]),
        )
        .unwrap();
    authority
        .accept_transaction_at_global(&first_tx, 20)
        .unwrap();

    let second_tx = authority
        .update_row(
            "notes",
            "note-1",
            BTreeMap::from([
                ("body".to_owned(), json!("epoch 10")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    authority
        .accept_transaction_at_global(&second_tx, 10)
        .unwrap();

    peer.apply_bundle(&authority.export_table_history("notes").unwrap())
        .unwrap();
    assert_eq!(
        peer.read_rows("notes").unwrap()[0].values["body"],
        json!("epoch 20")
    );

    peer.clear_current_projection_for_test().unwrap();
    peer.rebuild_current_projection().unwrap();
    assert_eq!(
        peer.read_rows("notes").unwrap()[0].values["body"],
        json!("epoch 20")
    );
}

#[test]
fn same_global_epoch_same_row_uses_stable_tie_breaker_across_apply_order_and_rebuild() {
    let schema = support::notes_schema();
    let mut authority =
        Runtime::open_with_schema(Storage::Memory, "authority", "alice", schema.clone()).unwrap();
    let mut peer_a =
        Runtime::open_with_schema(Storage::Memory, "peer-a", "alice", schema.clone()).unwrap();
    let mut peer_b = Runtime::open_with_schema(Storage::Memory, "peer-b", "alice", schema).unwrap();

    let base_tx = authority
        .insert_row(
            "notes",
            "note-1",
            BTreeMap::from([
                ("body".to_owned(), json!("base")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    authority.accept_transaction_at_global(&base_tx, 1).unwrap();
    let base_bundle = authority.export_table_history("notes").unwrap();
    peer_a.apply_bundle(&base_bundle).unwrap();
    peer_b.apply_bundle(&base_bundle).unwrap();

    let first_tx = authority
        .update_row(
            "notes",
            "note-1",
            BTreeMap::from([("body".to_owned(), json!("first same epoch"))]),
        )
        .unwrap();
    authority
        .accept_transaction_at_global(&first_tx, 2)
        .unwrap();
    let first_bundle = authority.export_table_history("notes").unwrap();

    let second_tx = authority
        .update_row(
            "notes",
            "note-1",
            BTreeMap::from([("body".to_owned(), json!("second same epoch"))]),
        )
        .unwrap();
    authority
        .accept_transaction_at_global(&second_tx, 2)
        .unwrap();
    let second_bundle = authority.export_table_history("notes").unwrap();

    peer_a.apply_bundle(&first_bundle).unwrap();
    peer_a.apply_bundle(&second_bundle).unwrap();
    peer_b.apply_bundle(&second_bundle).unwrap();
    peer_b.apply_bundle(&first_bundle).unwrap();

    let peer_a_body = peer_a.read_rows("notes").unwrap()[0].values["body"].clone();
    let peer_b_body = peer_b.read_rows("notes").unwrap()[0].values["body"].clone();
    assert_eq!(peer_a_body, peer_b_body);

    peer_a.clear_current_projection_for_test().unwrap();
    peer_b.clear_current_projection_for_test().unwrap();
    peer_a.rebuild_current_projection().unwrap();
    peer_b.rebuild_current_projection().unwrap();
    assert_eq!(
        peer_a.read_rows("notes").unwrap()[0].values["body"],
        peer_a_body
    );
    assert_eq!(
        peer_b.read_rows("notes").unwrap()[0].values["body"],
        peer_b_body
    );
}

#[test]
fn direct_global_acceptance_repairs_current_projection_order() {
    let schema = support::notes_schema();
    let mut authority =
        Runtime::open_with_schema(Storage::Memory, "authority", "alice", schema).unwrap();

    let first_tx = authority
        .insert_row(
            "notes",
            "note-1",
            BTreeMap::from([
                ("body".to_owned(), json!("epoch 20")),
                ("pinned".to_owned(), json!(true)),
            ]),
        )
        .unwrap();
    authority
        .accept_transaction_at_global(&first_tx, 20)
        .unwrap();

    let second_tx = authority
        .update_row(
            "notes",
            "note-1",
            BTreeMap::from([
                ("body".to_owned(), json!("epoch 10")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    authority
        .accept_transaction_at_global(&second_tx, 10)
        .unwrap();

    assert_eq!(
        authority.read_rows("notes").unwrap()[0].values["body"],
        json!("epoch 20")
    );
}

#[test]
fn remote_pending_update_does_not_override_global_current_on_peer() {
    let schema = support::notes_schema();
    let mut authority =
        Runtime::open_with_schema(Storage::Memory, "authority", "alice", schema.clone()).unwrap();
    let mut writer =
        Runtime::open_with_schema(Storage::Memory, "writer", "alice", schema.clone()).unwrap();
    let mut peer = Runtime::open_with_schema(Storage::Memory, "peer", "alice", schema).unwrap();

    let base_tx = authority
        .insert_row(
            "notes",
            "note-1",
            BTreeMap::from([
                ("body".to_owned(), json!("global")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    authority.accept_transaction_at_global(&base_tx, 1).unwrap();
    let base_bundle = authority.export_table_history("notes").unwrap();
    writer.apply_bundle(&base_bundle).unwrap();
    peer.apply_bundle(&base_bundle).unwrap();

    writer
        .update_row(
            "notes",
            "note-1",
            BTreeMap::from([
                ("body".to_owned(), json!("remote pending")),
                ("pinned".to_owned(), json!(true)),
            ]),
        )
        .unwrap();
    assert_eq!(
        writer.read_rows("notes").unwrap()[0].values["body"],
        json!("remote pending")
    );

    peer.apply_bundle(&writer.export_table_history("notes").unwrap())
        .unwrap();
    assert_eq!(
        peer.read_rows("notes").unwrap()[0].values["body"],
        json!("global")
    );

    peer.clear_current_projection_for_test().unwrap();
    peer.rebuild_current_projection().unwrap();
    assert_eq!(
        peer.read_rows("notes").unwrap()[0].values["body"],
        json!("global")
    );
}

#[test]
fn accepted_remote_pending_update_repairs_peer_current_projection() {
    let schema = support::notes_schema();
    let mut authority =
        Runtime::open_with_schema(Storage::Memory, "authority", "alice", schema.clone()).unwrap();
    let mut writer =
        Runtime::open_with_schema(Storage::Memory, "writer", "alice", schema.clone()).unwrap();
    let mut peer = Runtime::open_with_schema(Storage::Memory, "peer", "alice", schema).unwrap();

    let base_tx = authority
        .insert_row(
            "notes",
            "note-1",
            BTreeMap::from([
                ("body".to_owned(), json!("global")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    authority.accept_transaction_at_global(&base_tx, 1).unwrap();
    let base_bundle = authority.export_table_history("notes").unwrap();
    writer.apply_bundle(&base_bundle).unwrap();
    peer.apply_bundle(&base_bundle).unwrap();

    let pending_tx = writer
        .update_row(
            "notes",
            "note-1",
            BTreeMap::from([
                ("body".to_owned(), json!("accepted later")),
                ("pinned".to_owned(), json!(true)),
            ]),
        )
        .unwrap();
    let pending_bundle = writer.export_table_history("notes").unwrap();
    authority.apply_bundle(&pending_bundle).unwrap();
    peer.apply_bundle(&pending_bundle).unwrap();
    assert_eq!(
        peer.read_rows("notes").unwrap()[0].values["body"],
        json!("global")
    );

    authority
        .accept_transaction_at_global(&pending_tx, 2)
        .unwrap();
    peer.apply_bundle(&authority.export_table_history("notes").unwrap())
        .unwrap();
    assert_eq!(
        peer.read_rows("notes").unwrap()[0].values["body"],
        json!("accepted later")
    );
}

#[test]
fn edge_accepted_remote_pending_update_repairs_peer_current_projection() {
    let schema = support::notes_schema();
    let mut authority =
        Runtime::open_with_schema(Storage::Memory, "authority", "alice", schema.clone()).unwrap();
    let mut writer =
        Runtime::open_with_schema(Storage::Memory, "writer", "alice", schema.clone()).unwrap();
    let mut peer = Runtime::open_with_schema(Storage::Memory, "peer", "alice", schema).unwrap();

    let base_tx = authority
        .insert_row(
            "notes",
            "note-1",
            BTreeMap::from([
                ("body".to_owned(), json!("global")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    authority.accept_transaction_at_global(&base_tx, 1).unwrap();
    let base_bundle = authority.export_table_history("notes").unwrap();
    writer.apply_bundle(&base_bundle).unwrap();
    peer.apply_bundle(&base_bundle).unwrap();

    let pending_tx = writer
        .update_row(
            "notes",
            "note-1",
            BTreeMap::from([
                ("body".to_owned(), json!("edge accepted later")),
                ("pinned".to_owned(), json!(true)),
            ]),
        )
        .unwrap();
    let pending_bundle = writer.export_table_history("notes").unwrap();
    authority.apply_bundle(&pending_bundle).unwrap();
    peer.apply_bundle(&pending_bundle).unwrap();
    assert_eq!(
        peer.read_rows("notes").unwrap()[0].values["body"],
        json!("global")
    );

    authority.accept_transaction_at_edge(&pending_tx).unwrap();
    peer.apply_bundle(&authority.export_table_history("notes").unwrap())
        .unwrap();
    assert_eq!(
        peer.read_rows("notes").unwrap()[0].values["body"],
        json!("edge accepted later")
    );
}

#[test]
fn older_global_mergeable_update_cannot_override_newer_exclusive_current() {
    let schema = support::notes_schema();
    let mut authority =
        Runtime::open_with_schema(Storage::Memory, "authority", "alice", schema.clone()).unwrap();
    let mut stale_writer =
        Runtime::open_with_schema(Storage::Memory, "stale-writer", "alice", schema.clone())
            .unwrap();
    let mut peer = Runtime::open_with_schema(Storage::Memory, "peer", "alice", schema).unwrap();

    let base_tx = authority
        .insert_row(
            "notes",
            "note-1",
            BTreeMap::from([
                ("body".to_owned(), json!("base")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    authority.accept_transaction_at_global(&base_tx, 1).unwrap();
    let base_bundle = authority.export_table_history("notes").unwrap();
    stale_writer.apply_bundle(&base_bundle).unwrap();
    peer.apply_bundle(&base_bundle).unwrap();

    let stale_tx = stale_writer
        .update_row(
            "notes",
            "note-1",
            BTreeMap::from([("body".to_owned(), json!("stale accepted mergeable"))]),
        )
        .unwrap();
    stale_writer
        .accept_transaction_at_global(&stale_tx, 10)
        .unwrap();
    let stale_bundle = stale_writer.export_table_history("notes").unwrap();

    authority
        .transaction()
        .exclusive_at_global(20)
        .update_row(
            "notes",
            "note-1",
            BTreeMap::from([("body".to_owned(), json!("exclusive current"))]),
        )
        .commit()
        .unwrap();
    peer.apply_bundle(&authority.export_table_history("notes").unwrap())
        .unwrap();
    peer.apply_bundle(&stale_bundle).unwrap();

    assert_eq!(
        peer.read_rows("notes").unwrap()[0].values["body"],
        json!("exclusive current")
    );
}

#[test]
fn accepted_bundle_does_not_resurrect_rejected_fate() {
    let mut alice = Runtime::open(Storage::Memory, "alice-node", "alice").unwrap();
    let mut rejected_peer = Runtime::open(Storage::Memory, "rejected-peer", "alice").unwrap();
    let mut accepted_peer = Runtime::open(Storage::Memory, "accepted-peer", "alice").unwrap();

    alice.create_project("project-1", "Spec work").unwrap();
    let tx = alice
        .create_todo("todo-1", "Rejected should win", false, "project-1")
        .unwrap();
    let pending = alice.export_table_history("todos").unwrap();
    rejected_peer.apply_bundle(&pending).unwrap();
    accepted_peer.apply_bundle(&pending).unwrap();

    rejected_peer
        .reject_transaction_with_detail(&tx, "policy_denied", json!({"reason": "local_rejection"}))
        .unwrap();
    alice.accept_transaction_at_global(&tx, 7).unwrap();
    accepted_peer
        .apply_bundle(&alice.export_table_history("todos").unwrap())
        .unwrap();

    rejected_peer
        .apply_bundle(&accepted_peer.export_table_history("todos").unwrap())
        .unwrap();

    assert!(rejected_peer.open_todos().unwrap().is_empty());
    assert_eq!(
        rejected_peer.transaction_info(&tx).unwrap().rejection_code,
        Some("policy_denied".to_owned())
    );
    assert_eq!(
        rejected_peer.transaction_info(&tx).unwrap().global_epoch,
        Some(7)
    );
    assert_eq!(
        rejected_peer
            .transaction_info(&tx)
            .unwrap()
            .rejection_detail,
        Some(json!({"reason": "local_rejection"}))
    );
}

#[test]
fn direct_accept_after_reject_preserves_rejected_outcome_with_global_metadata() {
    let mut alice = Runtime::open(Storage::Memory, "alice-node", "alice").unwrap();

    alice.create_project("project-1", "Spec work").unwrap();
    let tx = alice
        .create_todo("todo-1", "Reject then accept metadata", false, "project-1")
        .unwrap();

    alice.reject_transaction(&tx, "policy_denied").unwrap();
    alice.accept_transaction_at_global(&tx, 7).unwrap();

    assert!(alice.open_todos().unwrap().is_empty());
    assert_eq!(alice.storage_stats().unwrap().rejected_transactions, 1);
    let info = alice.transaction_info(&tx).unwrap();
    assert_eq!(info.global_epoch, Some(7));
    assert_eq!(info.rejection_code, Some("policy_denied".to_owned()));
}

#[test]
fn direct_reject_after_accept_removes_current_but_preserves_global_metadata() {
    let mut alice = Runtime::open(Storage::Memory, "alice-node", "alice").unwrap();

    alice.create_project("project-1", "Spec work").unwrap();
    let tx = alice
        .create_todo("todo-1", "Accept then reject", false, "project-1")
        .unwrap();

    alice.accept_transaction_at_global(&tx, 7).unwrap();
    alice.reject_transaction(&tx, "policy_denied").unwrap();

    assert!(alice.open_todos().unwrap().is_empty());
    assert_eq!(alice.storage_stats().unwrap().rejected_transactions, 1);
    let info = alice.transaction_info(&tx).unwrap();
    assert_eq!(info.global_epoch, Some(7));
    assert_eq!(info.rejection_code, Some("policy_denied".to_owned()));
}
