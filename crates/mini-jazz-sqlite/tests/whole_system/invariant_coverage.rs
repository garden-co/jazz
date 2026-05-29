use super::*;
use mini_jazz_sqlite::sync::QueryReadRecord;
use std::collections::BTreeSet;

fn apply_refreshes_individually(upstream: &Runtime, peer: &mut Runtime, reads: &[QueryReadRecord]) {
    for read in reads {
        let refreshes = upstream
            .export_query_read_refreshes(std::slice::from_ref(read))
            .unwrap();
        assert_eq!(refreshes.len(), 1);
        peer.apply_bundle(&refreshes[0]).unwrap();
    }
}

fn apply_refreshes_batched(upstream: &Runtime, peer: &mut Runtime, reads: &[QueryReadRecord]) {
    for refresh in upstream.export_query_read_refreshes(reads).unwrap() {
        peer.apply_bundle(&refresh).unwrap();
    }
}

fn row_ids(rows: Vec<mini_jazz_sqlite::RowView>) -> BTreeSet<String> {
    rows.into_iter().map(|row| row.id).collect()
}

#[test]
fn batched_refresh_matches_individual_refresh_for_mixed_predicates_and_pages() {
    let mut upstream = Runtime::open(Storage::Memory, "upstream", "alice").unwrap();
    let mut individual_peer = Runtime::open(Storage::Memory, "individual-peer", "alice").unwrap();
    let mut batched_peer = Runtime::open(Storage::Memory, "batched-peer", "alice").unwrap();

    upstream.create_project("project-1", "Spec work").unwrap();
    for (id, title, done) in [
        ("todo-alpha-open", "Alpha open", false),
        ("todo-beta-open", "Beta open", false),
        ("todo-alpha-done", "Alpha done", true),
        ("todo-beta-done", "Beta done", true),
    ] {
        upstream.create_todo(id, title, done, "project-1").unwrap();
    }

    let initial_bundles = vec![
        upstream
            .export_query_where_eq("todos", "done", json!(false))
            .unwrap(),
        upstream
            .export_query_where_eq("todos", "done", json!(true))
            .unwrap(),
        upstream
            .export_query_where_contains("todos", "title", "Alpha")
            .unwrap(),
        upstream
            .export_query_where_contains("todos", "title", "Beta")
            .unwrap(),
        upstream
            .export_query_where_in("todos", "title", vec![json!("Alpha open")])
            .unwrap(),
        upstream
            .export_query_where_in("todos", "title", vec![json!("Beta open")])
            .unwrap(),
        upstream
            .export_query_where_ne("todos", "done", json!(false))
            .unwrap(),
        upstream
            .export_query_where_ne("todos", "done", json!(true))
            .unwrap(),
        upstream
            .export_query_where_eq_top_created_at_desc("todos", "done", json!(false), 1)
            .unwrap(),
        upstream
            .export_query_where_eq_top_created_at_desc("todos", "done", json!(true), 1)
            .unwrap(),
        upstream
            .export_many_query_where_eq_top_field_desc(
                "todos",
                "done",
                vec![json!(false), json!(true)],
                "title",
                1,
            )
            .unwrap(),
    ];
    for bundle in &initial_bundles {
        individual_peer.apply_bundle(bundle).unwrap();
        batched_peer.apply_bundle(bundle).unwrap();
    }
    let observed = batched_peer.observed_query_reads().unwrap();
    assert_eq!(observed.len(), 12);

    upstream
        .transaction()
        .update_row(
            "todos",
            "todo-alpha-open",
            BTreeMap::from([
                ("title".to_owned(), json!("Gamma open")),
                ("done".to_owned(), json!(true)),
            ]),
        )
        .commit()
        .unwrap();
    upstream
        .transaction()
        .delete_row("todos", "todo-beta-done")
        .commit()
        .unwrap();
    upstream
        .create_todo("todo-new-open", "Zulu open", false, "project-1")
        .unwrap();
    upstream
        .create_todo("todo-new-done", "Zulu done", true, "project-1")
        .unwrap();

    apply_refreshes_individually(&upstream, &mut individual_peer, &observed);
    apply_refreshes_batched(&upstream, &mut batched_peer, &observed);

    assert_eq!(
        batched_peer.read_rows("todos").unwrap(),
        individual_peer.read_rows("todos").unwrap()
    );
    assert_eq!(
        batched_peer.observed_query_reads().unwrap(),
        individual_peer.observed_query_reads().unwrap()
    );
}

#[test]
fn refresh_planner_does_not_batch_across_descriptor_boundaries() {
    let schema = SchemaDef::new().table("documents", |table| {
        table.text("owner");
        table.text("kind");
        table.text("rank");
        table.text("title");
        table.index("owner_rank", ["owner", "rank"]);
    });
    let mut upstream =
        Runtime::open_with_schema(Storage::Memory, "upstream", "alice", schema.clone()).unwrap();
    let mut peer = Runtime::open_with_schema(Storage::Memory, "peer", "alice", schema).unwrap();

    for (id, owner, kind, rank, title) in [
        ("alice-a", "alice", "task", "001", "A"),
        ("alice-b", "alice", "note", "002", "B"),
        ("bob-a", "bob", "task", "003", "C"),
        ("bob-b", "bob", "note", "004", "D"),
    ] {
        upstream
            .insert_row(
                "documents",
                id,
                BTreeMap::from([
                    ("owner".to_owned(), json!(owner)),
                    ("kind".to_owned(), json!(kind)),
                    ("rank".to_owned(), json!(rank)),
                    ("title".to_owned(), json!(title)),
                ]),
            )
            .unwrap();
    }

    for bundle in [
        upstream
            .export_query_where_eq_top_field_desc("documents", "owner", json!("alice"), "rank", 1)
            .unwrap(),
        upstream
            .export_query_where_eq_top_field_desc("documents", "owner", json!("bob"), "rank", 1)
            .unwrap(),
        upstream
            .export_query_where_eq_top_field_desc("documents", "owner", json!("alice"), "rank", 2)
            .unwrap(),
        upstream
            .export_query_where_eq_top_field_desc("documents", "owner", json!("alice"), "title", 1)
            .unwrap(),
        upstream
            .export_query_where_eq("documents", "owner", json!("alice"))
            .unwrap(),
        upstream
            .export_query_where_eq("documents", "kind", json!("task"))
            .unwrap(),
    ] {
        peer.apply_bundle(&bundle).unwrap();
    }

    upstream
        .insert_row(
            "documents",
            "alice-new",
            BTreeMap::from([
                ("owner".to_owned(), json!("alice")),
                ("kind".to_owned(), json!("task")),
                ("rank".to_owned(), json!("999")),
                ("title".to_owned(), json!("Z")),
            ]),
        )
        .unwrap();

    let refreshes = upstream
        .export_query_read_refreshes(&peer.observed_query_reads().unwrap())
        .unwrap();
    let query_read_counts = refreshes
        .iter()
        .map(|bundle| bundle.query_reads.len())
        .collect::<Vec<_>>();
    assert_eq!(query_read_counts, vec![1, 1, 2, 1, 1]);
}

#[test]
fn large_same_shape_page_refreshes_survive_multi_value_sql_chunking() {
    let schema = SchemaDef::new().table("documents", |table| {
        table.text("owner");
        table.text("rank");
        table.text("title");
        table.index("owner_rank", ["owner", "rank"]);
    });
    let mut upstream =
        Runtime::open_with_schema(Storage::Memory, "upstream", "alice", schema.clone()).unwrap();
    let mut peer = Runtime::open_with_schema(Storage::Memory, "peer", "alice", schema).unwrap();

    for idx in 0..425 {
        let owner = format!("owner-{idx:03}");
        upstream
            .insert_row(
                "documents",
                &format!("{owner}-old"),
                BTreeMap::from([
                    ("owner".to_owned(), json!(owner)),
                    ("rank".to_owned(), json!("001")),
                    ("title".to_owned(), json!("old")),
                ]),
            )
            .unwrap();
        let bundle = upstream
            .export_query_where_eq_top_field_desc("documents", "owner", json!(owner), "rank", 1)
            .unwrap();
        peer.apply_bundle(&bundle).unwrap();
    }
    assert_eq!(peer.observed_query_reads().unwrap().len(), 425);

    for idx in 0..425 {
        let owner = format!("owner-{idx:03}");
        upstream
            .insert_row(
                "documents",
                &format!("{owner}-new"),
                BTreeMap::from([
                    ("owner".to_owned(), json!(owner)),
                    ("rank".to_owned(), json!("999")),
                    ("title".to_owned(), json!("new")),
                ]),
            )
            .unwrap();
    }

    let refreshes = upstream
        .export_query_read_refreshes(&peer.observed_query_reads().unwrap())
        .unwrap();
    assert_eq!(refreshes.len(), 1);
    assert_eq!(refreshes[0].query_reads.len(), 425);
    peer.apply_bundle(&refreshes[0]).unwrap();

    assert_eq!(peer.read_rows("documents").unwrap().len(), 425);
    for idx in [0, 199, 400, 424] {
        let owner = format!("owner-{idx:03}");
        let rows = peer
            .read_rows_where_eq_top_field_desc("documents", "owner", json!(owner), "rank", 1)
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].id, format!("{owner}-new"));
    }
}

#[test]
fn query_scope_refresh_is_idempotent_after_scope_contraction() {
    let mut upstream = Runtime::open(Storage::Memory, "upstream", "alice").unwrap();
    let mut peer = Runtime::open(Storage::Memory, "peer", "alice").unwrap();

    upstream.create_project("project-1", "Spec work").unwrap();
    upstream
        .create_todo("todo-1", "Initially open", false, "project-1")
        .unwrap();
    peer.apply_bundle(&upstream.export_query_scope_open_todos().unwrap())
        .unwrap();

    upstream
        .transaction()
        .update_row(
            "todos",
            "todo-1",
            BTreeMap::from([("done".to_owned(), json!(true))]),
        )
        .commit()
        .unwrap();
    let refreshes = upstream
        .export_query_read_refreshes(&peer.observed_query_reads().unwrap())
        .unwrap();
    assert_eq!(refreshes.len(), 1);

    peer.apply_bundle(&refreshes[0]).unwrap();
    peer.apply_bundle(&refreshes[0]).unwrap();

    assert!(peer.open_todos().unwrap().is_empty());
    assert_eq!(peer.observed_query_reads().unwrap().len(), 1);
}

#[test]
fn semantic_system_field_page_refresh_matches_individual_application() {
    let mut upstream = Runtime::open(Storage::Memory, "upstream", "alice").unwrap();
    let mut individual_peer = Runtime::open(Storage::Memory, "individual-peer", "alice").unwrap();
    let mut batched_peer = Runtime::open(Storage::Memory, "batched-peer", "alice").unwrap();

    upstream.create_project("project-1", "Spec work").unwrap();
    upstream
        .create_todo("todo-old", "Old", false, "project-1")
        .unwrap();
    upstream
        .create_todo("todo-new", "New", false, "project-1")
        .unwrap();

    for bundle in [
        upstream
            .export_query_where_eq_top_created_at_desc("todos", "$createdBy", json!("alice"), 1)
            .unwrap(),
        upstream
            .export_query_where_eq_top_created_at_desc("todos", "id", json!("todo-old"), 1)
            .unwrap(),
    ] {
        individual_peer.apply_bundle(&bundle).unwrap();
        batched_peer.apply_bundle(&bundle).unwrap();
    }
    let observed = batched_peer.observed_query_reads().unwrap();
    assert_eq!(observed.len(), 2);

    upstream
        .transaction()
        .delete_row("todos", "todo-old")
        .commit()
        .unwrap();
    upstream
        .create_todo("todo-newest", "Newest", false, "project-1")
        .unwrap();

    apply_refreshes_individually(&upstream, &mut individual_peer, &observed);
    apply_refreshes_batched(&upstream, &mut batched_peer, &observed);

    assert_eq!(
        row_ids(batched_peer.read_rows("todos").unwrap()),
        row_ids(individual_peer.read_rows("todos").unwrap())
    );
}

#[test]
fn transaction_causality_is_recorded_at_row_granularity() {
    let schema = support::notes_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema).unwrap();

    let create_tx = alice
        .transaction()
        .insert_row(
            "notes",
            "note-1",
            BTreeMap::from([
                ("body".to_owned(), json!("First")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .commit()
        .unwrap();
    let update_tx = alice
        .transaction()
        .update_row(
            "notes",
            "note-1",
            BTreeMap::from([("pinned".to_owned(), json!(true))]),
        )
        .commit()
        .unwrap();

    assert_eq!(
        alice.transaction_write_rows(&update_tx).unwrap(),
        vec![("notes".to_owned(), "note-1".to_owned())]
    );
    assert_eq!(
        alice.transaction_previous_read_rows(&update_tx).unwrap(),
        vec![("notes".to_owned(), "note-1".to_owned())]
    );
    assert_eq!(
        alice.transaction_observed_read_rows(&update_tx).unwrap(),
        vec![(
            "notes".to_owned(),
            "note-1".to_owned(),
            Some(create_tx.clone())
        )]
    );

    let info = alice.transaction_info(&update_tx).unwrap();
    assert_eq!(info.conflict_mode, "mergeable");
    assert_eq!(info.rejection_code, None);
}

#[test]
fn rejected_fate_repairs_query_scope_and_survives_replay() {
    let mut upstream = Runtime::open(Storage::Memory, "upstream", "alice").unwrap();
    let mut peer = Runtime::open(Storage::Memory, "peer", "alice").unwrap();

    upstream.create_project("project-1", "Spec work").unwrap();
    let tx = upstream
        .transaction()
        .insert_row(
            "todos",
            "todo-rejected",
            BTreeMap::from([
                ("title".to_owned(), json!("Should disappear")),
                ("done".to_owned(), json!(false)),
                ("project".to_owned(), json!("project-1")),
            ]),
        )
        .commit()
        .unwrap();

    peer.apply_bundle(&upstream.export_query_scope_open_todos().unwrap())
        .unwrap();
    assert_eq!(peer.open_todos().unwrap().len(), 1);

    upstream
        .reject_transaction_with_detail(&tx, "policy_denied", json!({"row": "todo-rejected"}))
        .unwrap();
    let refreshes = upstream
        .export_query_read_refreshes(&peer.observed_query_reads().unwrap())
        .unwrap();
    for refresh in &refreshes {
        peer.apply_bundle(refresh).unwrap();
    }
    for refresh in &refreshes {
        peer.apply_bundle(refresh).unwrap();
    }

    assert!(peer.open_todos().unwrap().is_empty());
    assert_eq!(
        peer.transaction_info(&tx).unwrap().rejection_code,
        Some("policy_denied".to_owned())
    );
    assert_eq!(
        peer.rejected_transactions().unwrap(),
        vec![RejectionInfo {
            tx_id: tx,
            code: "policy_denied".to_owned(),
            detail: Some(json!({"row": "todo-rejected"})),
        }]
    );
}

#[test]
fn branch_observed_refreshes_are_scoped_to_checked_out_branch() {
    let schema = support::tasks_schema();
    let mut upstream =
        Runtime::open_with_schema(Storage::Memory, "upstream", "alice", schema.clone()).unwrap();
    let mut peer = Runtime::open_with_schema(Storage::Memory, "peer", "alice", schema).unwrap();

    upstream.create_branch("draft", None).unwrap();
    upstream.checkout_branch("draft").unwrap();
    upstream
        .insert_row(
            "tasks",
            "task-draft",
            BTreeMap::from([
                ("title".to_owned(), json!("Draft only")),
                ("done".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    peer.apply_bundle(
        &upstream
            .export_query_where_eq("tasks", "done", json!(false))
            .unwrap(),
    )
    .unwrap();
    let observed = peer.observed_query_reads().unwrap();
    assert_eq!(observed[0].branch_id, "draft");

    upstream.checkout_branch("main").unwrap();
    upstream
        .insert_row(
            "tasks",
            "task-main",
            BTreeMap::from([
                ("title".to_owned(), json!("Main only")),
                ("done".to_owned(), json!(false)),
            ]),
        )
        .unwrap();

    let err = upstream.export_query_read_refreshes(&observed).unwrap_err();
    assert!(err
        .to_string()
        .contains("query refresh branch is not checked out"));

    upstream.checkout_branch("draft").unwrap();
    upstream
        .insert_row(
            "tasks",
            "task-draft-new",
            BTreeMap::from([
                ("title".to_owned(), json!("New draft")),
                ("done".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    apply_refreshes_batched(&upstream, &mut peer, &observed);

    peer.checkout_branch("draft").unwrap();
    let ids = row_ids(peer.read_rows("tasks").unwrap());
    assert!(ids.contains("task-draft"));
    assert!(ids.contains("task-draft-new"));
}

#[test]
fn renamed_lens_query_refresh_keeps_observed_row_current_across_schema_versions() {
    let old_schema = SchemaDef::new().table("tasks", |table| {
        table.text("title");
        table.bool("done");
    });
    let new_schema = SchemaDef::new().table("tasks", |table| {
        table.text_lens("name", "title");
        table.bool("done");
    });
    let mut old_writer =
        Runtime::open_with_schema(Storage::Memory, "old-writer", "alice", old_schema).unwrap();
    let mut new_source =
        Runtime::open_with_schema(Storage::Memory, "new-source", "alice", new_schema.clone())
            .unwrap();
    let mut new_peer =
        Runtime::open_with_schema(Storage::Memory, "new-peer", "alice", new_schema).unwrap();

    old_writer
        .insert_row(
            "tasks",
            "task-1",
            BTreeMap::from([
                ("title".to_owned(), json!("Original")),
                ("done".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    new_source
        .apply_bundle(&old_writer.export_table_history("tasks").unwrap())
        .unwrap();
    new_peer
        .apply_bundle(
            &new_source
                .export_query_where_eq("tasks", "name", json!("Original"))
                .unwrap(),
        )
        .unwrap();
    assert_eq!(
        new_peer.read_rows("tasks").unwrap()[0].values["name"],
        json!("Original")
    );

    old_writer
        .update_row(
            "tasks",
            "task-1",
            BTreeMap::from([("title".to_owned(), json!("Renamed underneath"))]),
        )
        .unwrap();
    new_source
        .apply_bundle(&old_writer.export_table_history("tasks").unwrap())
        .unwrap();
    let observed = new_peer.observed_query_reads().unwrap();
    apply_refreshes_batched(&new_source, &mut new_peer, &observed);

    let rows = new_peer.read_rows("tasks").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values["name"], json!("Renamed underneath"));
    assert!(!rows[0].values.contains_key("title"));
}

#[test]
fn observed_query_subscription_emits_deterministic_diff_after_batched_refresh() {
    let mut upstream = Runtime::open(Storage::Memory, "upstream", "alice").unwrap();
    let mut peer = Runtime::open(Storage::Memory, "peer", "alice").unwrap();

    upstream.create_project("project-1", "Spec work").unwrap();
    upstream
        .create_todo("todo-a", "Alpha", false, "project-1")
        .unwrap();
    upstream
        .create_todo("todo-b", "Beta", false, "project-1")
        .unwrap();
    peer.apply_bundle(
        &upstream
            .export_query_where_eq_top_created_at_desc("todos", "done", json!(false), 2)
            .unwrap(),
    )
    .unwrap();
    let observed = peer.observed_query_reads().unwrap();
    let mut subscription = peer.subscribe_observed_query(&observed[0]).unwrap();

    upstream
        .transaction()
        .update_row(
            "todos",
            "todo-a",
            BTreeMap::from([("done".to_owned(), json!(true))]),
        )
        .commit()
        .unwrap();
    upstream
        .create_todo("todo-c", "Charlie", false, "project-1")
        .unwrap();

    apply_refreshes_batched(&upstream, &mut peer, &observed);
    let diffs = peer.poll_subscription(&mut subscription).unwrap();

    assert!(diffs.iter().any(|diff| {
        matches!(diff, RowDiff::Removed(row) if row.id == "todo-a")
            || matches!(diff, RowDiff::Updated { before, after } if before.id == "todo-a" && after.values["done"] == json!(true))
    }));
    assert!(diffs
        .iter()
        .any(|diff| matches!(diff, RowDiff::Added(row) if row.id == "todo-c")));
    assert!(peer
        .poll_subscription(&mut subscription)
        .unwrap()
        .is_empty());
}

#[test]
fn recursive_batched_refresh_matches_individual_refresh_after_subtree_changes() {
    let schema = support::folders_schema();
    let mut upstream =
        Runtime::open_with_schema(Storage::Memory, "upstream", "alice", schema.clone()).unwrap();
    let mut individual_peer =
        Runtime::open_with_schema(Storage::Memory, "individual-peer", "alice", schema.clone())
            .unwrap();
    let mut batched_peer =
        Runtime::open_with_schema(Storage::Memory, "batched-peer", "alice", schema).unwrap();

    for (id, name, parent) in [
        ("root-a", "Root A", "root-a"),
        ("a-child", "A Child", "root-a"),
        ("root-b", "Root B", "root-b"),
        ("b-child", "B Child", "root-b"),
    ] {
        upstream
            .insert_row(
                "folders",
                id,
                BTreeMap::from([
                    ("name".to_owned(), json!(name)),
                    ("parent".to_owned(), json!(parent)),
                ]),
            )
            .unwrap();
    }
    for root in ["root-a", "root-b"] {
        let bundle = upstream
            .export_recursive_refs("folders", root, "parent")
            .unwrap();
        individual_peer.apply_bundle(&bundle).unwrap();
        batched_peer.apply_bundle(&bundle).unwrap();
    }
    let observed = batched_peer.observed_query_reads().unwrap();

    upstream
        .insert_row(
            "folders",
            "a-grandchild",
            BTreeMap::from([
                ("name".to_owned(), json!("A Grandchild")),
                ("parent".to_owned(), json!("a-child")),
            ]),
        )
        .unwrap();
    upstream.delete_row("folders", "b-child").unwrap();

    apply_refreshes_individually(&upstream, &mut individual_peer, &observed);
    apply_refreshes_batched(&upstream, &mut batched_peer, &observed);

    assert_eq!(
        row_ids(
            batched_peer
                .read_recursive_refs("folders", "root-a", "parent")
                .unwrap()
        ),
        row_ids(
            individual_peer
                .read_recursive_refs("folders", "root-a", "parent")
                .unwrap()
        )
    );
    assert_eq!(
        row_ids(
            batched_peer
                .read_recursive_refs("folders", "root-b", "parent")
                .unwrap()
        ),
        row_ids(
            individual_peer
                .read_recursive_refs("folders", "root-b", "parent")
                .unwrap()
        )
    );
}

#[test]
fn multi_hop_topology_refreshes_cold_client_query_after_upstream_change() {
    let harness = support::Harness::new();
    let mut mesh =
        support::TrustedMeshTopology::memory(&harness, SchemaDef::attempt3_fixture()).unwrap();

    mesh.alice_tab
        .create_project("project-1", "Distributed work")
        .unwrap();
    mesh.alice_tab
        .create_todo("todo-1", "Propagate me", false, "project-1")
        .unwrap();

    support::sync_table_untrusted(&mesh.alice_tab, &mut mesh.edge_a, "projects").unwrap();
    support::sync_table_untrusted(&mesh.alice_tab, &mut mesh.edge_a, "todos").unwrap();
    support::sync_table(&mesh.edge_a, &mut mesh.core, "projects").unwrap();
    support::sync_table(&mesh.edge_a, &mut mesh.core, "todos").unwrap();
    support::apply(
        mesh.core.export_query_scope_open_todos().unwrap(),
        &mut mesh.bob_tab,
    )
    .unwrap();

    assert_eq!(mesh.bob_tab.open_todos().unwrap().len(), 1);

    mesh.alice_tab
        .transaction()
        .update_row(
            "todos",
            "todo-1",
            BTreeMap::from([("done".to_owned(), json!(true))]),
        )
        .commit()
        .unwrap();
    support::sync_table_untrusted(&mesh.alice_tab, &mut mesh.edge_a, "todos").unwrap();
    support::sync_table(&mesh.edge_a, &mut mesh.core, "todos").unwrap();
    support::refresh_observed_queries(&mesh.core, &mut mesh.bob_tab).unwrap();

    assert!(mesh.bob_tab.open_todos().unwrap().is_empty());
    assert_eq!(mesh.bob_tab.observed_query_reads().unwrap().len(), 1);
}

#[test]
fn repeated_observed_query_descriptor_is_deduped() {
    let mut upstream = Runtime::open(Storage::Memory, "upstream", "alice").unwrap();
    let mut peer = Runtime::open(Storage::Memory, "peer", "alice").unwrap();

    upstream.create_project("project-1", "Spec work").unwrap();
    upstream
        .create_todo("todo-1", "Same descriptor", false, "project-1")
        .unwrap();

    let bundle = upstream
        .export_query_where_eq("todos", "done", json!(false))
        .unwrap();
    peer.apply_bundle(&bundle).unwrap();
    peer.apply_bundle(&bundle).unwrap();

    assert_eq!(peer.observed_query_reads().unwrap(), bundle.query_reads);
    assert_eq!(peer.observed_query_reads().unwrap().len(), 1);
}

#[test]
fn forgotten_observed_query_descriptor_is_not_refreshed() {
    let mut upstream = Runtime::open(Storage::Memory, "upstream", "alice").unwrap();
    let mut peer = Runtime::open(Storage::Memory, "peer", "alice").unwrap();

    upstream.create_project("project-1", "Spec work").unwrap();
    peer.apply_bundle(
        &upstream
            .export_query_where_eq("todos", "done", json!(false))
            .unwrap(),
    )
    .unwrap();
    let observed = peer.observed_query_reads().unwrap();
    peer.forget_observed_query_read(&observed[0]).unwrap();
    assert!(peer.observed_query_reads().unwrap().is_empty());

    upstream
        .create_todo("todo-later", "Should not arrive", false, "project-1")
        .unwrap();
    let remaining_reads = peer.observed_query_reads().unwrap();
    apply_refreshes_batched(&upstream, &mut peer, &remaining_reads);

    assert!(peer.read_rows("todos").unwrap().is_empty());
}

#[test]
fn empty_observed_refresh_request_is_noop() {
    let mut upstream = Runtime::open(Storage::Memory, "upstream", "alice").unwrap();
    let mut peer = Runtime::open(Storage::Memory, "peer", "alice").unwrap();

    upstream.create_project("project-1", "Spec work").unwrap();
    upstream
        .create_todo("todo-1", "No one asked", false, "project-1")
        .unwrap();

    let refreshes = upstream.export_query_read_refreshes(&[]).unwrap();
    assert!(refreshes.is_empty());
    apply_refreshes_batched(&upstream, &mut peer, &[]);
    assert!(peer.read_rows("todos").unwrap().is_empty());
}

#[test]
fn subscribing_to_observed_query_requires_checked_out_branch() {
    let schema = support::tasks_schema();
    let mut upstream =
        Runtime::open_with_schema(Storage::Memory, "upstream", "alice", schema.clone()).unwrap();
    let mut peer = Runtime::open_with_schema(Storage::Memory, "peer", "alice", schema).unwrap();

    upstream.create_branch("draft", None).unwrap();
    upstream.checkout_branch("draft").unwrap();
    upstream
        .insert_row(
            "tasks",
            "task-draft",
            BTreeMap::from([
                ("title".to_owned(), json!("Draft")),
                ("done".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    peer.apply_bundle(
        &upstream
            .export_query_where_eq("tasks", "done", json!(false))
            .unwrap(),
    )
    .unwrap();

    let observed = peer.observed_query_reads().unwrap();
    peer.checkout_branch("main").unwrap();
    let err = peer.subscribe_observed_query(&observed[0]).unwrap_err();
    assert!(err
        .to_string()
        .contains("observed query branch is not checked out"));
}

#[test]
fn in_query_duplicate_values_are_semantically_idempotent() {
    let schema = support::notes_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema).unwrap();

    for (id, body) in [
        ("note-alpha", "alpha"),
        ("note-beta", "beta"),
        ("note-gamma", "gamma"),
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

    assert_eq!(
        row_ids(
            alice
                .read_rows_where_in(
                    "notes",
                    "body",
                    vec![json!("alpha"), json!("alpha"), json!("beta")]
                )
                .unwrap()
        ),
        row_ids(
            alice
                .read_rows_where_in("notes", "body", vec![json!("beta"), json!("alpha")])
                .unwrap()
        )
    );
}

#[test]
fn not_equal_null_matches_present_optional_values_only() {
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
    assert_eq!(row_ids(rows), BTreeSet::from(["note-tagged".to_owned()]));
}

#[test]
fn repeated_bundle_replay_does_not_duplicate_history_or_current_rows() {
    let mut alice = Runtime::open(Storage::Memory, "alice", "alice").unwrap();
    let mut bob = Runtime::open(Storage::Memory, "bob", "alice").unwrap();

    alice.create_project("project-1", "Spec work").unwrap();
    alice
        .create_todo("todo-1", "Replay exactly once", false, "project-1")
        .unwrap();
    let bundle = alice.export_table_history("todos").unwrap();

    bob.apply_bundle(&bundle).unwrap();
    let after_once = bob.storage_stats().unwrap();
    bob.apply_bundle(&bundle).unwrap();
    let after_twice = bob.storage_stats().unwrap();

    assert_eq!(after_once.history_rows, after_twice.history_rows);
    assert_eq!(after_once.current_rows, after_twice.current_rows);
    assert_eq!(bob.read_rows("todos").unwrap().len(), 1);
}

#[test]
fn projection_rebuild_is_semantically_identical_to_current_reads_after_mixed_fate() {
    let schema = support::notes_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema).unwrap();

    let keep_tx = alice
        .insert_row(
            "notes",
            "note-keep",
            BTreeMap::from([
                ("body".to_owned(), json!("Keep")),
                ("pinned".to_owned(), json!(true)),
            ]),
        )
        .unwrap();
    let reject_tx = alice
        .insert_row(
            "notes",
            "note-reject",
            BTreeMap::from([
                ("body".to_owned(), json!("Reject")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    alice.accept_transaction_at_global(&keep_tx, 1).unwrap();
    alice
        .reject_transaction(&reject_tx, "policy_denied")
        .unwrap();

    let before = alice.read_rows("notes").unwrap();
    alice.clear_current_projection_for_test().unwrap();
    alice.rebuild_current_projection().unwrap();
    let after = alice.read_rows("notes").unwrap();

    assert_eq!(after, before);
    assert_eq!(row_ids(after), BTreeSet::from(["note-keep".to_owned()]));
}

#[test]
fn durable_reopen_preserves_projection_without_rebuild() {
    let harness = support::Harness::new();
    {
        let mut worker = harness.durable("worker.sqlite", "worker", "alice").unwrap();
        worker.create_project("project-1", "Spec work").unwrap();
        worker
            .create_todo("todo-1", "Survive reopen", false, "project-1")
            .unwrap();
    }

    let reopened = harness
        .durable("worker.sqlite", "worker-reopened", "alice")
        .unwrap();
    assert_eq!(reopened.open_todos().unwrap().len(), 1);
    assert_eq!(reopened.storage_stats().unwrap().current_rows, 2);
}

#[test]
fn accepting_same_tx_at_edge_and_global_is_monotonic() {
    let schema = support::notes_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema).unwrap();

    let tx = alice
        .insert_row(
            "notes",
            "note-1",
            BTreeMap::from([
                ("body".to_owned(), json!("Receipt")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    alice.accept_transaction_at_edge(&tx).unwrap();
    alice.accept_transaction_at_global(&tx, 7).unwrap();
    alice.accept_transaction_at_global(&tx, 7).unwrap();

    let info = alice.transaction_info(&tx).unwrap();
    assert_eq!(info.global_epoch, Some(7));
    assert_eq!(
        info.receipt_tiers,
        vec!["edge".to_owned(), "global".to_owned()]
    );
    assert_eq!(alice.read_rows("notes").unwrap().len(), 1);
}

#[test]
fn rejection_then_stale_pending_replay_does_not_resurrect_current_row() {
    let schema = support::notes_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut bob = Runtime::open_with_schema(Storage::Memory, "bob-node", "alice", schema).unwrap();

    let tx = alice
        .insert_row(
            "notes",
            "note-1",
            BTreeMap::from([
                ("body".to_owned(), json!("Do not resurrect")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    let stale_pending = alice.export_table_history("notes").unwrap();
    alice.reject_transaction(&tx, "policy_denied").unwrap();

    bob.apply_bundle(&alice.export_table_history("notes").unwrap())
        .unwrap();
    assert!(bob.read_rows("notes").unwrap().is_empty());
    bob.apply_bundle(&stale_pending).unwrap();

    assert!(bob.read_rows("notes").unwrap().is_empty());
    assert_eq!(
        bob.transaction_info(&tx).unwrap().rejection_code,
        Some("policy_denied".to_owned())
    );
}

#[test]
fn query_scope_retains_previous_row_as_local_fact_after_predicate_exit() {
    let mut upstream = Runtime::open(Storage::Memory, "upstream", "alice").unwrap();
    let mut peer = Runtime::open(Storage::Memory, "peer", "alice").unwrap();

    upstream.create_project("project-1", "Spec work").unwrap();
    upstream
        .create_todo("todo-1", "Leave predicate", false, "project-1")
        .unwrap();
    peer.apply_bundle(
        &upstream
            .export_query_where_eq("todos", "done", json!(false))
            .unwrap(),
    )
    .unwrap();
    upstream
        .update_row(
            "todos",
            "todo-1",
            BTreeMap::from([("done".to_owned(), json!(true))]),
        )
        .unwrap();
    support::refresh_observed_queries(&upstream, &mut peer).unwrap();

    assert!(peer
        .read_rows_where_eq("todos", "done", json!(false))
        .unwrap()
        .is_empty());
    assert_eq!(
        peer.read_rows("todos").unwrap()[0].values["done"],
        json!(true)
    );
}

#[test]
fn branch_source_metadata_updates_are_idempotent() {
    let schema = support::tasks_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema).unwrap();

    alice.create_branch("left", None).unwrap();
    alice.create_branch("merge", None).unwrap();
    alice.add_branch_source("merge", "left").unwrap();
    alice.add_branch_source("merge", "left").unwrap();

    let merge = alice
        .branches()
        .unwrap()
        .into_iter()
        .find(|branch| branch.id == "merge")
        .unwrap();
    assert_eq!(merge.source_branch_ids, vec!["left".to_owned()]);

    alice.remove_branch_source("merge", "left").unwrap();
    alice.remove_branch_source("merge", "left").unwrap();
    let merge = alice
        .branches()
        .unwrap()
        .into_iter()
        .find(|branch| branch.id == "merge")
        .unwrap();
    assert!(merge.source_branch_ids.is_empty());
}

#[test]
fn branch_backing_rows_match_branch_api_after_mutations() {
    let schema = support::tasks_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema).unwrap();

    alice.create_branch("left", Some(3)).unwrap();
    alice.create_branch("right", Some(3)).unwrap();
    alice
        .create_branch_from_branches_at_base("merge", Some(3), &["left", "right"])
        .unwrap();
    alice.remove_branch_source("merge", "right").unwrap();

    assert_eq!(
        alice.branch_backing_rows().unwrap(),
        alice.branches().unwrap()
    );
}

#[test]
fn branch_query_refresh_after_source_removal_removes_detached_source_rows() {
    let schema = support::tasks_schema();
    let mut upstream =
        Runtime::open_with_schema(Storage::Memory, "upstream", "alice", schema.clone()).unwrap();
    let mut peer = Runtime::open_with_schema(Storage::Memory, "peer", "alice", schema).unwrap();

    upstream.create_branch("left", None).unwrap();
    upstream.checkout_branch("left").unwrap();
    upstream
        .insert_row(
            "tasks",
            "task-left",
            BTreeMap::from([
                ("title".to_owned(), json!("Left")),
                ("done".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    upstream
        .create_branch_from_branches("merge", &["left"])
        .unwrap();
    upstream.checkout_branch("merge").unwrap();

    peer.apply_bundle(
        &upstream
            .export_query_where_eq("tasks", "done", json!(false))
            .unwrap(),
    )
    .unwrap();
    peer.checkout_branch("merge").unwrap();
    assert!(row_ids(peer.read_rows("tasks").unwrap()).contains("task-left"));

    upstream.remove_branch_source("merge", "left").unwrap();
    support::refresh_observed_queries(&upstream, &mut peer).unwrap();

    assert!(!row_ids(peer.read_rows("tasks").unwrap()).contains("task-left"));
}

#[test]
fn trusted_admin_write_bypasses_policy_but_keeps_attributed_user() {
    let schema = SchemaDef::new().table("notes", |table| {
        table.text("body");
        table.write_if_created_by_user();
    });
    let harness = support::Harness::new();
    let mut edge = harness.trusted_memory_with_schema("edge", schema).unwrap();

    let tx = support::run_attributing_to_user(&mut edge, "bob", |edge| {
        edge.insert_row(
            "notes",
            "note-admin",
            BTreeMap::from([("body".to_owned(), json!("Admin-created"))]),
        )
    })
    .unwrap();

    let rows = edge.read_rows("notes").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].created_by, "bob");
    assert_eq!(rows[0].tx_id, tx);
}

#[test]
fn untrusted_apply_policy_failure_is_atomic_for_multi_row_transaction() {
    let schema = SchemaDef::new()
        .table("projects", |table| {
            table.text("title");
            table.read_if_created_by_user();
        })
        .table("todos", |table| {
            table.text("title");
            table.ref_("project", "projects");
            table.write_if_ref_readable("project");
        });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut edge = Runtime::open_trusted_with_schema(Storage::Memory, "edge", schema).unwrap();

    let tx = alice
        .transaction()
        .insert_row(
            "projects",
            "project-alice",
            BTreeMap::from([("title".to_owned(), json!("Alice project"))]),
        )
        .insert_row(
            "todos",
            "todo-bad",
            BTreeMap::from([
                ("title".to_owned(), json!("Missing parent on edge")),
                ("project".to_owned(), json!("project-missing")),
            ]),
        )
        .commit()
        .unwrap();

    support::apply_untrusted_as_user(
        alice.export_table_history("todos").unwrap(),
        &mut edge,
        "alice",
    )
    .unwrap();
    assert!(edge.read_rows("projects").unwrap().is_empty());
    assert!(edge.read_rows("todos").unwrap().is_empty());
    assert_eq!(
        edge.transaction_info(&tx).unwrap().rejection_code,
        Some("policy_denied".to_owned())
    );
}

#[test]
fn declared_defaults_are_materialized_and_survive_sync_rebuild() {
    let schema = SchemaDef::new().table("tasks", |table| {
        table.text("title");
        table.bool_default("done", false);
    });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut bob = Runtime::open_with_schema(Storage::Memory, "bob-node", "alice", schema).unwrap();

    alice
        .insert_row(
            "tasks",
            "task-1",
            BTreeMap::from([("title".to_owned(), json!("Default done"))]),
        )
        .unwrap();
    bob.apply_bundle(&alice.export_table_history("tasks").unwrap())
        .unwrap();
    bob.clear_current_projection_for_test().unwrap();
    bob.rebuild_current_projection().unwrap();

    let rows = bob.read_rows("tasks").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values["done"], json!(false));
}

#[test]
fn ordered_page_subscription_emits_deterministic_diff_for_order_only_change() {
    let schema = SchemaDef::new().table("documents", |table| {
        table.text("owner");
        table.text("rank");
        table.text("title");
    });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema).unwrap();

    for (id, rank) in [("doc-low", "001"), ("doc-high", "999")] {
        alice
            .insert_row(
                "documents",
                id,
                BTreeMap::from([
                    ("owner".to_owned(), json!("alice")),
                    ("rank".to_owned(), json!(rank)),
                    ("title".to_owned(), json!(id)),
                ]),
            )
            .unwrap();
    }
    let mut subscription = alice
        .subscribe_rows_where_eq_top_field_desc("documents", "owner", json!("alice"), "rank", 2)
        .unwrap();
    alice
        .update_row(
            "documents",
            "doc-low",
            BTreeMap::from([("rank".to_owned(), json!("1000"))]),
        )
        .unwrap();

    let diffs = alice.poll_subscription(&mut subscription).unwrap();
    assert!(!diffs.is_empty());
    assert!(diffs.iter().any(|diff| match diff {
        RowDiff::Moved { row, .. } => row.id == "doc-low",
        RowDiff::Updated { before, after } => before.id == "doc-low" && after.id == "doc-low",
        RowDiff::Removed(row) | RowDiff::Added(row) => row.id == "doc-low",
    }));
    assert!(alice
        .poll_subscription(&mut subscription)
        .unwrap()
        .is_empty());
}

#[test]
fn query_read_order_is_deterministic_after_mixed_descriptor_application() {
    let mut upstream = Runtime::open(Storage::Memory, "upstream", "alice").unwrap();
    let mut left = Runtime::open(Storage::Memory, "left-peer", "alice").unwrap();
    let mut right = Runtime::open(Storage::Memory, "right-peer", "alice").unwrap();

    upstream.create_project("project-1", "Spec work").unwrap();
    upstream
        .create_todo("todo-1", "Deterministic", false, "project-1")
        .unwrap();
    let eq = upstream
        .export_query_where_eq("todos", "done", json!(false))
        .unwrap();
    let contains = upstream
        .export_query_where_contains("todos", "title", "Det")
        .unwrap();

    left.apply_bundle(&eq).unwrap();
    left.apply_bundle(&contains).unwrap();
    right.apply_bundle(&contains).unwrap();
    right.apply_bundle(&eq).unwrap();

    assert_eq!(
        left.observed_query_reads().unwrap(),
        right.observed_query_reads().unwrap()
    );
}

#[test]
fn exclusive_without_global_epoch_fails_without_writing_history() {
    let schema = support::notes_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema).unwrap();

    let err = alice
        .transaction()
        .exclusive()
        .insert_row(
            "notes",
            "note-local-exclusive",
            BTreeMap::from([
                ("body".to_owned(), json!("No local exclusive")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .commit()
        .unwrap_err();

    assert!(err
        .to_string()
        .contains("exclusive transactions require global"));
    assert_eq!(alice.storage_stats().unwrap().history_rows, 0);
    assert!(alice.read_rows("notes").unwrap().is_empty());
}

#[test]
fn mergeable_same_row_updates_can_follow_each_other() {
    let schema = support::notes_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema).unwrap();

    alice
        .insert_row(
            "notes",
            "note-1",
            BTreeMap::from([
                ("body".to_owned(), json!("Initial")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    alice
        .update_row(
            "notes",
            "note-1",
            BTreeMap::from([("body".to_owned(), json!("Body update"))]),
        )
        .unwrap();
    alice
        .update_row(
            "notes",
            "note-1",
            BTreeMap::from([("pinned".to_owned(), json!(true))]),
        )
        .unwrap();

    let row = alice.read_rows("notes").unwrap().remove(0);
    assert_eq!(row.values["body"], json!("Body update"));
    assert_eq!(row.values["pinned"], json!(true));
    assert_eq!(alice.storage_stats().unwrap().history_rows, 3);
}

#[test]
fn update_preserves_omitted_fields_across_sync_and_rebuild() {
    let schema = support::notes_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut bob = Runtime::open_with_schema(Storage::Memory, "bob-node", "alice", schema).unwrap();

    alice
        .insert_row(
            "notes",
            "note-1",
            BTreeMap::from([
                ("body".to_owned(), json!("Original body")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    alice
        .update_row(
            "notes",
            "note-1",
            BTreeMap::from([("pinned".to_owned(), json!(true))]),
        )
        .unwrap();
    bob.apply_bundle(&alice.export_table_history("notes").unwrap())
        .unwrap();
    bob.clear_current_projection_for_test().unwrap();
    bob.rebuild_current_projection().unwrap();

    let row = bob.read_rows("notes").unwrap().remove(0);
    assert_eq!(row.values["body"], json!("Original body"));
    assert_eq!(row.values["pinned"], json!(true));
}

#[test]
fn deleting_invisible_row_fails_without_creating_transaction() {
    let schema = support::notes_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema).unwrap();

    let err = alice.delete_row("notes", "missing-note").unwrap_err();
    assert!(err.to_string().contains("not visible") || err.to_string().contains("not found"));
    assert_eq!(alice.storage_stats().unwrap().history_rows, 0);
}

#[test]
fn checked_out_unknown_branch_fails_without_changing_current_branch() {
    let schema = support::tasks_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema).unwrap();

    assert!(alice.checkout_branch("missing").is_err());
    alice
        .insert_row(
            "tasks",
            "task-main",
            BTreeMap::from([
                ("title".to_owned(), json!("Still main")),
                ("done".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    assert_eq!(alice.read_rows("tasks").unwrap()[0].id, "task-main");
}

#[test]
fn branch_base_epoch_mismatch_fails_idempotently() {
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
fn direct_branch_source_cycle_fails_without_partial_source_change() {
    let schema = support::tasks_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema).unwrap();

    alice.create_branch("draft", None).unwrap();
    let err = alice.add_branch_source("draft", "draft").unwrap_err();
    assert!(err.to_string().contains("cycle"));
    let draft = alice
        .branches()
        .unwrap()
        .into_iter()
        .find(|branch| branch.id == "draft")
        .unwrap();
    assert!(draft.source_branch_ids.is_empty());
}

#[test]
fn query_export_with_unknown_table_fails_without_recording_interest() {
    let alice = Runtime::open(Storage::Memory, "alice-node", "alice").unwrap();

    let err = alice
        .export_query_where_eq("missing_table", "done", json!(false))
        .unwrap_err();
    assert!(err.to_string().contains("unknown table"));
    assert!(alice.observed_query_reads().unwrap().is_empty());
}

#[test]
fn query_export_with_unknown_field_fails_without_recording_interest() {
    let alice = Runtime::open(Storage::Memory, "alice-node", "alice").unwrap();

    let err = alice
        .export_query_where_eq("todos", "missing_field", json!(false))
        .unwrap_err();
    assert!(
        err.to_string().contains("unknown query field")
            || err.to_string().contains("unknown field")
    );
    assert!(alice.observed_query_reads().unwrap().is_empty());
}

#[test]
fn contains_query_is_case_sensitive_substring_match() {
    let schema = support::notes_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema).unwrap();

    for (id, body) in [
        ("note-lower", "alpha target"),
        ("note-upper", "ALPHA TARGET"),
        ("note-other", "beta"),
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

    assert_eq!(
        row_ids(
            alice
                .read_rows_where_contains("notes", "body", "alpha")
                .unwrap()
        ),
        BTreeSet::from(["note-lower".to_owned()])
    );
}

#[test]
fn id_magic_field_query_is_not_confused_with_user_id_column() {
    let schema = SchemaDef::new().table("profiles", |table| {
        table.text("id");
        table.text("name");
    });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema).unwrap();

    alice
        .insert_row(
            "profiles",
            "row-public-id",
            BTreeMap::from([
                ("id".to_owned(), json!("user-visible-id")),
                ("name".to_owned(), json!("Alice")),
            ]),
        )
        .unwrap();

    let magic = alice
        .read_rows_where_eq("profiles", "id", json!("row-public-id"))
        .unwrap();
    assert_eq!(magic.len(), 1);
    assert_eq!(magic[0].values["id"], json!("user-visible-id"));
    assert!(alice
        .read_rows_where_eq("profiles", "id", json!("user-visible-id"))
        .unwrap()
        .is_empty());
}

#[test]
fn created_by_filter_uses_authorship_not_mutable_user_column() {
    let schema = SchemaDef::new().table("docs", |table| {
        table.text("title");
        table.text("$createdBy");
    });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema).unwrap();

    alice
        .insert_row(
            "docs",
            "doc-1",
            BTreeMap::from([
                ("title".to_owned(), json!("Doc")),
                ("$createdBy".to_owned(), json!("bob")),
            ]),
        )
        .unwrap();

    assert_eq!(
        row_ids(
            alice
                .read_rows_where_eq("docs", "$createdBy", json!("alice"))
                .unwrap()
        ),
        BTreeSet::from(["doc-1".to_owned()])
    );
    assert!(alice
        .read_rows_where_eq("docs", "$createdBy", json!("bob"))
        .unwrap()
        .is_empty());
}

#[test]
fn rejection_subscription_reports_detail_once_and_then_quiets() {
    let schema = support::tasks_schema();
    let mut authority =
        Runtime::open_with_schema(Storage::Memory, "authority", "alice", schema.clone()).unwrap();
    let mut peer = Runtime::open_with_schema(Storage::Memory, "peer", "alice", schema).unwrap();
    let mut subscription = peer.subscribe_rejections().unwrap();

    let tx = authority
        .insert_row(
            "tasks",
            "task-rejected",
            BTreeMap::from([
                ("title".to_owned(), json!("Rejected")),
                ("done".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    authority
        .reject_transaction_with_detail(&tx, "policy_denied", json!({"safe": true}))
        .unwrap();
    peer.apply_bundle(&authority.export_table_history("tasks").unwrap())
        .unwrap();

    let events = peer.poll_rejections(&mut subscription).unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].tx_id, tx);
    assert_eq!(events[0].detail, Some(json!({"safe": true})));
    assert!(peer.poll_rejections(&mut subscription).unwrap().is_empty());
}

#[test]
fn same_global_epoch_tie_breaker_is_stable_after_rebuild() {
    let schema = support::notes_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema).unwrap();

    let first = alice
        .insert_row(
            "notes",
            "note-1",
            BTreeMap::from([
                ("body".to_owned(), json!("first")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    let second = alice
        .update_row(
            "notes",
            "note-1",
            BTreeMap::from([("body".to_owned(), json!("second"))]),
        )
        .unwrap();
    alice.accept_transaction_at_global(&first, 7).unwrap();
    alice.accept_transaction_at_global(&second, 7).unwrap();
    let before = alice.read_rows("notes").unwrap();

    alice.clear_current_projection_for_test().unwrap();
    alice.rebuild_current_projection().unwrap();
    let after = alice.read_rows("notes").unwrap();

    assert_eq!(after, before);
    assert_eq!(after[0].values["body"], json!("second"));
}

#[test]
fn accepted_global_fate_arriving_before_history_later_materializes_row() {
    let schema = support::notes_schema();
    let mut source =
        Runtime::open_with_schema(Storage::Memory, "source", "alice", schema.clone()).unwrap();
    let mut peer = Runtime::open_with_schema(Storage::Memory, "peer", "alice", schema).unwrap();

    let tx = source
        .insert_row(
            "notes",
            "note-1",
            BTreeMap::from([
                ("body".to_owned(), json!("Accepted later history")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    source.accept_transaction_at_global(&tx, 7).unwrap();
    let mut fate_only = source.export_table_history("notes").unwrap();
    fate_only.history.clear();
    peer.apply_bundle(&fate_only).unwrap();
    assert!(peer.read_rows("notes").unwrap().is_empty());
    assert_eq!(peer.transaction_info(&tx).unwrap().global_epoch, Some(7));

    peer.apply_bundle(&source.export_table_history("notes").unwrap())
        .unwrap();
    assert_eq!(peer.read_rows("notes").unwrap().len(), 1);
}

#[test]
fn durable_reopen_preserves_rejection_info_and_no_current_row() {
    let harness = support::Harness::new();
    let schema = support::tasks_schema();
    let tx;
    {
        let mut worker = harness
            .durable_with_schema("worker.sqlite", "worker", "alice", schema.clone())
            .unwrap();
        tx = worker
            .insert_row(
                "tasks",
                "task-rejected",
                BTreeMap::from([
                    ("title".to_owned(), json!("Rejected")),
                    ("done".to_owned(), json!(false)),
                ]),
            )
            .unwrap();
        worker.reject_transaction(&tx, "policy_denied").unwrap();
    }

    let reopened = harness
        .durable_with_schema("worker.sqlite", "worker-reopened", "alice", schema)
        .unwrap();
    assert!(reopened.read_rows("tasks").unwrap().is_empty());
    assert_eq!(
        reopened.transaction_info(&tx).unwrap().rejection_code,
        Some("policy_denied".to_owned())
    );
}

#[test]
fn empty_explicit_transaction_is_noop_without_history() {
    let schema = support::notes_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema).unwrap();

    let tx = alice.transaction().commit().unwrap();

    assert!(tx.is_empty());
    assert_eq!(alice.storage_stats().unwrap().history_rows, 0);
    assert!(alice.read_rows("notes").unwrap().is_empty());
}

#[test]
fn same_row_updates_in_one_transaction_normalize_to_one_history_version() {
    let schema = support::notes_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema).unwrap();

    alice
        .insert_row(
            "notes",
            "note-1",
            BTreeMap::from([
                ("body".to_owned(), json!("Initial")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    let tx = alice
        .transaction()
        .update_row(
            "notes",
            "note-1",
            BTreeMap::from([("body".to_owned(), json!("First staged"))]),
        )
        .update_row(
            "notes",
            "note-1",
            BTreeMap::from([
                ("body".to_owned(), json!("Final staged")),
                ("pinned".to_owned(), json!(true)),
            ]),
        )
        .commit()
        .unwrap();

    let rows = alice.read_rows("notes").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values["body"], json!("Final staged"));
    assert_eq!(rows[0].values["pinned"], json!(true));
    assert_eq!(
        alice.transaction_write_rows(&tx).unwrap(),
        vec![("notes".to_owned(), "note-1".to_owned())]
    );
    assert_eq!(alice.storage_stats().unwrap().history_rows, 2);
}

#[test]
fn insert_then_update_same_row_in_one_transaction_seals_final_created_row() {
    let schema = support::notes_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema).unwrap();

    let tx = alice
        .transaction()
        .insert_row(
            "notes",
            "note-1",
            BTreeMap::from([
                ("body".to_owned(), json!("Draft body")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .update_row(
            "notes",
            "note-1",
            BTreeMap::from([("body".to_owned(), json!("Final body"))]),
        )
        .commit()
        .unwrap();

    let rows = alice.read_rows("notes").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values["body"], json!("Final body"));
    assert_eq!(rows[0].values["pinned"], json!(false));
    assert_eq!(rows[0].tx_id, tx);
    assert_eq!(alice.storage_stats().unwrap().history_rows, 1);
}

#[test]
fn awaiting_dependency_does_not_publish_subscription_until_dependency_arrives() {
    let schema = SchemaDef::new()
        .table("projects", |table| {
            table.text("title");
            table.read_if_created_by_user();
        })
        .table("todos", |table| {
            table.text("title");
            table.ref_("project", "projects");
            table.write_if_ref_readable("project");
        });
    let mut writer =
        Runtime::open_with_schema(Storage::Memory, "writer", "bob", schema.clone()).unwrap();
    let mut edge = Runtime::open_trusted_with_schema(Storage::Memory, "edge", schema).unwrap();
    let mut subscription = edge.subscribe_rows("todos").unwrap();

    writer
        .insert_row(
            "projects",
            "project-bob",
            BTreeMap::from([("title".to_owned(), json!("Bob project"))]),
        )
        .unwrap();
    let tx = writer
        .insert_row(
            "todos",
            "todo-awaiting",
            BTreeMap::from([
                ("title".to_owned(), json!("Wait for project")),
                ("project".to_owned(), json!("project-bob")),
            ]),
        )
        .unwrap();
    let mut incomplete_bundle = writer.export_table_history("todos").unwrap();
    incomplete_bundle
        .history
        .retain(|record| record.table != "projects");
    support::apply_untrusted_as_user(incomplete_bundle, &mut edge, "bob").unwrap();

    assert!(edge.read_rows("todos").unwrap().is_empty());
    assert!(edge
        .poll_subscription(&mut subscription)
        .unwrap()
        .is_empty());
    assert!(edge
        .transaction_info(&tx)
        .unwrap()
        .awaiting_dependency
        .is_some());

    support::sync_table(&writer, &mut edge, "projects").unwrap();
    let diffs = edge.poll_subscription(&mut subscription).unwrap();

    assert!(matches!(&diffs[..], [RowDiff::Added(row)] if row.id == "todo-awaiting"));
    assert_eq!(
        edge.transaction_info(&tx).unwrap().awaiting_dependency,
        None
    );
}

#[test]
fn durable_node_recovers_when_fate_arrives_before_history() {
    let harness = support::Harness::new();
    let schema = support::notes_schema();
    let mut source =
        Runtime::open_with_schema(Storage::Memory, "source", "alice", schema.clone()).unwrap();
    let tx = source
        .insert_row(
            "notes",
            "note-late-history",
            BTreeMap::from([
                ("body".to_owned(), json!("History after fate")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    source.accept_transaction_at_global(&tx, 7).unwrap();
    let mut fate_only = source.export_table_history("notes").unwrap();
    fate_only.history.clear();

    {
        let mut worker = harness
            .durable_with_schema("worker.sqlite", "worker", "alice", schema.clone())
            .unwrap();
        worker.apply_bundle(&fate_only).unwrap();
        assert!(worker.read_rows("notes").unwrap().is_empty());
        assert_eq!(worker.transaction_info(&tx).unwrap().global_epoch, Some(7));
    }

    let mut reopened = harness
        .durable_with_schema("worker.sqlite", "worker-reopened", "alice", schema)
        .unwrap();
    assert!(reopened.read_rows("notes").unwrap().is_empty());
    reopened
        .apply_bundle(&source.export_table_history("notes").unwrap())
        .unwrap();

    let rows = reopened.read_rows("notes").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, "note-late-history");
    assert_eq!(
        reopened.transaction_info(&tx).unwrap().global_epoch,
        Some(7)
    );
}

#[test]
fn duplicated_and_reordered_table_bundles_converge_across_topology() {
    let mut alice = Runtime::open(Storage::Memory, "alice-tab", "alice").unwrap();
    let mut edge =
        Runtime::open_trusted_with_schema(Storage::Memory, "edge", SchemaDef::attempt3_fixture())
            .unwrap();
    let mut core =
        Runtime::open_trusted_with_schema(Storage::Memory, "core", SchemaDef::attempt3_fixture())
            .unwrap();
    let mut bob = Runtime::open(Storage::Memory, "bob-tab", "bob").unwrap();

    alice.create_project("project-1", "Spec work").unwrap();
    alice
        .create_todo("todo-1", "First", false, "project-1")
        .unwrap();
    alice
        .create_todo("todo-2", "Second", false, "project-1")
        .unwrap();

    let projects = alice.export_table_history("projects").unwrap();
    let todos = alice.export_table_history("todos").unwrap();
    for bundle in [&todos, &projects, &todos, &projects] {
        edge.apply_untrusted_bundle_as_user(bundle, "alice")
            .unwrap();
    }
    for bundle in [
        edge.export_table_history("todos").unwrap(),
        edge.export_table_history("projects").unwrap(),
        edge.export_table_history("todos").unwrap(),
    ] {
        core.apply_bundle(&bundle).unwrap();
    }
    bob.apply_bundle(&core.export_query_scope_open_todos().unwrap())
        .unwrap();

    assert_eq!(bob.open_todos().unwrap().len(), 2);
    assert_eq!(edge.open_todos().unwrap().len(), 2);
    assert_eq!(core.open_todos().unwrap().len(), 2);
}

#[test]
fn missing_catalogue_state_fails_closed_without_partial_apply() {
    let source_schema = support::notes_schema();
    let target_schema = support::tasks_schema();
    let mut source =
        Runtime::open_with_schema(Storage::Memory, "source", "alice", source_schema).unwrap();
    let mut target =
        Runtime::open_with_schema(Storage::Memory, "target", "alice", target_schema).unwrap();

    source
        .insert_row(
            "notes",
            "note-1",
            BTreeMap::from([
                ("body".to_owned(), json!("Needs catalogue")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .unwrap();

    let err = target
        .apply_bundle(&source.export_table_history("notes").unwrap())
        .unwrap_err();
    assert!(err.to_string().contains("schema fingerprint"));
    assert!(target.read_rows("tasks").unwrap().is_empty());
    assert_eq!(target.storage_stats().unwrap().history_rows, 0);
}

#[test]
fn upsert_creates_missing_row_and_updates_existing_row() {
    let schema = support::notes_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema).unwrap();

    let create_tx = alice
        .upsert_row(
            "notes",
            "note-1",
            BTreeMap::from([
                ("body".to_owned(), json!("Created by upsert")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    let update_tx = alice
        .upsert_row(
            "notes",
            "note-1",
            BTreeMap::from([("pinned".to_owned(), json!(true))]),
        )
        .unwrap();

    let rows = alice.read_rows("notes").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values["body"], json!("Created by upsert"));
    assert_eq!(rows[0].values["pinned"], json!(true));
    assert_ne!(create_tx, update_tx);
    assert_eq!(alice.storage_stats().unwrap().history_rows, 2);
}

#[test]
fn transaction_upsert_normalizes_with_later_same_row_updates() {
    let schema = support::notes_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema).unwrap();

    let tx = alice
        .transaction()
        .upsert_row(
            "notes",
            "note-1",
            BTreeMap::from([
                ("body".to_owned(), json!("Upsert draft")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .update_row(
            "notes",
            "note-1",
            BTreeMap::from([("body".to_owned(), json!("Upsert final"))]),
        )
        .commit()
        .unwrap();

    let rows = alice.read_rows("notes").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values["body"], json!("Upsert final"));
    assert_eq!(rows[0].values["pinned"], json!(false));
    assert_eq!(rows[0].tx_id, tx);
    assert_eq!(alice.storage_stats().unwrap().history_rows, 1);
}
