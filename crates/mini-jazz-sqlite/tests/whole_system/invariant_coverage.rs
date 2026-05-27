use super::*;
use mini_jazz_sqlite::sync::{merge_bundles, QueryReadRecord};
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

fn seeded_bundle_schedule(bundle_count: usize, seed: u64) -> Vec<usize> {
    let mut state = seed;
    let mut schedule = (0..bundle_count).collect::<Vec<_>>();
    for _ in 0..(bundle_count * 3) {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
        schedule.push((state as usize) % bundle_count);
    }
    for idx in 0..schedule.len() {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
        let swap_idx = (state as usize) % schedule.len();
        schedule.swap(idx, swap_idx);
    }
    schedule
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
fn repeated_global_acceptance_cannot_regress_transaction_epoch() {
    let schema = support::notes_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema).unwrap();

    let tx = alice
        .insert_row(
            "notes",
            "note-1",
            BTreeMap::from([
                ("body".to_owned(), json!("Monotonic epoch")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    alice.accept_transaction_at_global(&tx, 10).unwrap();
    alice.accept_transaction_at_global(&tx, 5).unwrap();

    assert_eq!(alice.transaction_info(&tx).unwrap().global_epoch, Some(10));
}

#[test]
fn stale_global_acceptance_bundle_cannot_regress_transaction_epoch() {
    let schema = support::notes_schema();
    let mut source =
        Runtime::open_with_schema(Storage::Memory, "source", "alice", schema.clone()).unwrap();
    let mut peer = Runtime::open_with_schema(Storage::Memory, "peer", "alice", schema).unwrap();

    let tx = source
        .insert_row(
            "notes",
            "note-1",
            BTreeMap::from([
                ("body".to_owned(), json!("Synced monotonic epoch")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    source.accept_transaction_at_global(&tx, 10).unwrap();
    let accepted_high = source.export_table_history("notes").unwrap();
    let mut accepted_low = accepted_high.clone();
    let tx_record = accepted_low
        .txs
        .iter_mut()
        .find(|record| record.tx_id == tx)
        .unwrap();
    tx_record.global_epoch = Some(5);
    tx_record.receipt_tiers = vec![2];

    peer.apply_bundle(&accepted_high).unwrap();
    peer.apply_bundle(&accepted_low).unwrap();

    assert_eq!(peer.transaction_info(&tx).unwrap().global_epoch, Some(10));
}

#[test]
fn stale_pending_bundle_cannot_drop_durable_receipts() {
    let schema = support::notes_schema();
    let mut source =
        Runtime::open_with_schema(Storage::Memory, "source", "alice", schema.clone()).unwrap();
    let mut peer = Runtime::open_with_schema(Storage::Memory, "peer", "alice", schema).unwrap();

    let tx = source
        .insert_row(
            "notes",
            "note-1",
            BTreeMap::from([
                ("body".to_owned(), json!("Receipt stays")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    let mut stale_pending = source.export_table_history("notes").unwrap();
    for tx_record in &mut stale_pending.txs {
        if tx_record.tx_id == tx {
            tx_record.global_epoch = None;
            tx_record.receipt_tiers.clear();
        }
    }

    source.accept_transaction_at_edge(&tx).unwrap();
    source.accept_transaction_at_global(&tx, 10).unwrap();
    peer.apply_bundle(&source.export_table_history("notes").unwrap())
        .unwrap();
    peer.apply_bundle(&stale_pending).unwrap();

    let info = peer.transaction_info(&tx).unwrap();
    assert_eq!(info.global_epoch, Some(10));
    assert_eq!(
        info.receipt_tiers,
        vec!["edge".to_owned(), "global".to_owned()]
    );
}

#[test]
fn stale_pending_bundle_cannot_erase_rejection_detail() {
    let schema = support::notes_schema();
    let mut source =
        Runtime::open_with_schema(Storage::Memory, "source", "alice", schema.clone()).unwrap();
    let mut peer = Runtime::open_with_schema(Storage::Memory, "peer", "alice", schema).unwrap();

    let tx = source
        .insert_row(
            "notes",
            "note-1",
            BTreeMap::from([
                ("body".to_owned(), json!("Rejected detail stays")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    let stale_pending = source.export_table_history("notes").unwrap();
    source
        .reject_transaction_with_detail(
            &tx,
            "policy_denied",
            json!({"reason": "owner_mismatch", "row": "note-1"}),
        )
        .unwrap();

    peer.apply_bundle(&source.export_table_history("notes").unwrap())
        .unwrap();
    peer.apply_bundle(&stale_pending).unwrap();

    let info = peer.transaction_info(&tx).unwrap();
    assert_eq!(info.rejection_code, Some("policy_denied".to_owned()));
    assert_eq!(
        info.rejection_detail,
        Some(json!({"reason": "owner_mismatch", "row": "note-1"}))
    );
    assert!(peer.read_rows("notes").unwrap().is_empty());
}

#[test]
fn stale_rejected_bundle_cannot_erase_later_rejection_detail() {
    let schema = support::notes_schema();
    let mut source =
        Runtime::open_with_schema(Storage::Memory, "source", "alice", schema.clone()).unwrap();
    let mut peer = Runtime::open_with_schema(Storage::Memory, "peer", "alice", schema).unwrap();

    let tx = source
        .insert_row(
            "notes",
            "note-1",
            BTreeMap::from([
                ("body".to_owned(), json!("Rejected detail stays")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    source.reject_transaction(&tx, "policy_denied").unwrap();
    let stale_rejected_without_detail = source.export_table_history("notes").unwrap();
    source
        .reject_transaction_with_detail(
            &tx,
            "policy_denied",
            json!({"reason": "owner_mismatch", "row": "note-1"}),
        )
        .unwrap();

    peer.apply_bundle(&source.export_table_history("notes").unwrap())
        .unwrap();
    peer.apply_bundle(&stale_rejected_without_detail).unwrap();

    assert_eq!(
        peer.transaction_info(&tx).unwrap().rejection_detail,
        Some(json!({"reason": "owner_mismatch", "row": "note-1"}))
    );
    assert!(peer.read_rows("notes").unwrap().is_empty());
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
fn stale_query_refresh_cannot_regress_row_after_later_update() {
    let mut upstream = Runtime::open(Storage::Memory, "upstream", "alice").unwrap();
    let mut peer = Runtime::open(Storage::Memory, "peer", "alice").unwrap();

    upstream.create_project("project-1", "Spec work").unwrap();
    upstream
        .create_todo("todo-1", "Initial", false, "project-1")
        .unwrap();
    peer.apply_bundle(
        &upstream
            .export_query_where_eq("todos", "done", json!(false))
            .unwrap(),
    )
    .unwrap();
    let observed = peer.observed_query_reads().unwrap();

    upstream
        .update_row(
            "todos",
            "todo-1",
            BTreeMap::from([
                ("title".to_owned(), json!("Left scope")),
                ("done".to_owned(), json!(true)),
            ]),
        )
        .unwrap();
    let stale_left_scope = upstream
        .export_query_read_refreshes(&observed)
        .unwrap()
        .remove(0);

    upstream
        .update_row(
            "todos",
            "todo-1",
            BTreeMap::from([
                ("title".to_owned(), json!("Re-entered")),
                ("done".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    let newer_reentered = upstream
        .export_query_read_refreshes(&observed)
        .unwrap()
        .remove(0);

    peer.apply_bundle(&newer_reentered).unwrap();
    peer.apply_bundle(&stale_left_scope).unwrap();

    let rows = peer
        .read_rows_where_eq("todos", "done", json!(false))
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, "todo-1");
    assert_eq!(rows[0].values["title"], json!("Re-entered"));
    assert_eq!(peer.observed_query_reads().unwrap(), observed);
}

#[test]
fn query_refresh_for_deleted_result_row_keeps_unrelated_cached_rows() {
    let mut upstream = Runtime::open(Storage::Memory, "upstream", "alice").unwrap();
    let mut peer = Runtime::open(Storage::Memory, "peer", "alice").unwrap();

    upstream.create_project("project-1", "Spec work").unwrap();
    upstream
        .create_todo("todo-open", "Open", false, "project-1")
        .unwrap();
    upstream
        .create_todo("todo-done", "Done", true, "project-1")
        .unwrap();
    peer.apply_bundle(
        &upstream
            .export_query_where_eq("todos", "done", json!(false))
            .unwrap(),
    )
    .unwrap();
    peer.apply_bundle(
        &upstream
            .export_query_where_eq("todos", "done", json!(true))
            .unwrap(),
    )
    .unwrap();

    let open_read = peer
        .observed_query_reads()
        .unwrap()
        .into_iter()
        .find(|read| read.value == json!(false))
        .unwrap();
    upstream.delete_row("todos", "todo-open").unwrap();
    let open_refreshes = upstream.export_query_read_refreshes(&[open_read]).unwrap();
    assert_eq!(open_refreshes.len(), 1);
    peer.apply_bundle(&open_refreshes[0]).unwrap();

    assert!(peer
        .read_rows_where_eq("todos", "done", json!(false))
        .unwrap()
        .is_empty());
    assert_eq!(
        row_ids(
            peer.read_rows_where_eq("todos", "done", json!(true))
                .unwrap()
        ),
        BTreeSet::from(["todo-done".to_owned()])
    );
    assert_eq!(peer.read_rows("todos").unwrap().len(), 1);
}

#[test]
fn duplicate_overlapping_query_refreshes_dedupe_history_and_query_reads() {
    let mut upstream = Runtime::open(Storage::Memory, "upstream", "alice").unwrap();
    let mut peer = Runtime::open(Storage::Memory, "peer", "alice").unwrap();

    upstream.create_project("project-1", "Spec work").unwrap();
    upstream
        .create_todo("todo-alpha", "Alpha shared", false, "project-1")
        .unwrap();
    peer.apply_bundle(
        &upstream
            .export_query_where_eq("todos", "done", json!(false))
            .unwrap(),
    )
    .unwrap();
    peer.apply_bundle(
        &upstream
            .export_query_where_contains("todos", "title", "Alpha")
            .unwrap(),
    )
    .unwrap();
    assert_eq!(peer.observed_query_reads().unwrap().len(), 2);

    upstream
        .update_row(
            "todos",
            "todo-alpha",
            BTreeMap::from([("title".to_owned(), json!("Alpha shared updated"))]),
        )
        .unwrap();
    let refreshes = upstream
        .export_query_read_refreshes(&peer.observed_query_reads().unwrap())
        .unwrap();
    assert_eq!(refreshes.len(), 2);

    for refresh in &refreshes {
        peer.apply_bundle(refresh).unwrap();
        peer.apply_bundle(refresh).unwrap();
    }

    assert_eq!(peer.observed_query_reads().unwrap().len(), 2);
    let rows = peer.read_rows("todos").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values["title"], json!("Alpha shared updated"));
    assert_eq!(peer.storage_stats().unwrap().current_rows, 1);
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
fn stale_branch_source_removal_bundle_cannot_drop_readded_source_rows() {
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
    let observed = peer.observed_query_reads().unwrap();

    upstream.remove_branch_source("merge", "left").unwrap();
    let stale_removed = upstream.export_query_read_refreshes(&observed).unwrap();
    upstream.add_branch_source("merge", "left").unwrap();
    let newer_readded = upstream.export_query_read_refreshes(&observed).unwrap();

    for refresh in newer_readded {
        peer.apply_bundle(&refresh).unwrap();
    }
    for refresh in stale_removed {
        peer.apply_bundle(&refresh).unwrap();
    }

    peer.checkout_branch("merge").unwrap();
    assert!(row_ids(peer.read_rows("tasks").unwrap()).contains("task-left"));
    let merge = peer
        .branches()
        .unwrap()
        .into_iter()
        .find(|branch| branch.id == "merge")
        .unwrap();
    assert_eq!(merge.source_branch_ids, vec!["left".to_owned()]);
}

#[test]
fn trusted_as_user_enforces_read_policy_while_attribution_bypasses_it() {
    let schema = SchemaDef::new().table("notes", |table| {
        table.text("body");
        table.read_if_created_by_user();
    });
    let harness = support::Harness::new();
    let mut edge = harness.trusted_memory_with_schema("edge", schema).unwrap();

    support::run_attributing_to_user(&mut edge, "alice", |edge| {
        edge.insert_row(
            "notes",
            "note-owned-by-alice",
            BTreeMap::from([("body".to_owned(), json!("original"))]),
        )
    })
    .unwrap();
    support::run_attributing_to_user(&mut edge, "bob", |edge| {
        edge.insert_row(
            "notes",
            "note-owned-by-bob",
            BTreeMap::from([("body".to_owned(), json!("bob note"))]),
        )
    })
    .unwrap();

    let bob_rows = support::run_as_user(&mut edge, "bob", |edge| edge.read_rows("notes")).unwrap();
    assert_eq!(
        row_ids(bob_rows),
        BTreeSet::from(["note-owned-by-bob".to_owned()])
    );

    let privileged_rows =
        support::run_attributing_to_user(&mut edge, "bob", |edge| edge.read_rows("notes")).unwrap();
    assert_eq!(
        row_ids(privileged_rows),
        BTreeSet::from([
            "note-owned-by-alice".to_owned(),
            "note-owned-by-bob".to_owned()
        ])
    );
}

#[test]
fn exclusive_forwarding_uses_forwarded_user_for_global_policy() {
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
    let mut edge = Runtime::open_trusted_attributing_to_user(
        Storage::Memory,
        "edge",
        "service",
        schema.clone(),
    )
    .unwrap();
    let mut core =
        Runtime::open_trusted_with_schema(Storage::Memory, "core", schema.clone()).unwrap();
    let mut core_wrong_user =
        Runtime::open_trusted_with_schema(Storage::Memory, "core-wrong", schema).unwrap();

    support::run_attributing_to_user(&mut edge, "alice", |edge| {
        edge.insert_row(
            "projects",
            "project-alice",
            BTreeMap::from([("title".to_owned(), json!("Alice project"))]),
        )
    })
    .unwrap();
    let tx = support::run_attributing_to_user(&mut edge, "service", |edge| {
        edge.insert_row(
            "todos",
            "todo-exclusive-forwarded",
            BTreeMap::from([
                ("title".to_owned(), json!("Forwarded with Alice auth")),
                ("project".to_owned(), json!("project-alice")),
            ]),
        )
    })
    .unwrap();

    support::sync_table(&edge, &mut core, "projects").unwrap();
    support::sync_table(&edge, &mut core_wrong_user, "projects").unwrap();
    support::forward_exclusive(&edge, &mut core_wrong_user, "todos", &tx, "service").unwrap();
    assert_eq!(
        core_wrong_user
            .transaction_info(&tx)
            .unwrap()
            .rejection_code,
        Some("policy_denied".to_owned())
    );
    assert!(core_wrong_user.read_rows("todos").unwrap().is_empty());

    support::forward_exclusive(&edge, &mut core, "todos", &tx, "alice").unwrap();
    let info = core.transaction_info(&tx).unwrap();
    assert_eq!(info.conflict_mode, "exclusive");
    assert_eq!(info.global_epoch, Some(1));
    assert_eq!(info.rejection_code, None);
    assert_eq!(
        core.read_rows("todos").unwrap()[0].id,
        "todo-exclusive-forwarded"
    );
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
fn duplicate_untrusted_rejection_bundle_is_idempotent_and_quiet_after_first_event() {
    let schema = SchemaDef::new().table("docs", |table| {
        table.text("title");
        table.write_if_created_by_user();
    });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut edge = Runtime::open_trusted_with_schema(Storage::Memory, "edge", schema).unwrap();
    let mut subscription = edge.subscribe_rejections().unwrap();

    let tx = alice
        .insert_row(
            "docs",
            "doc-1",
            BTreeMap::from([("title".to_owned(), json!("forged as bob"))]),
        )
        .unwrap();
    let bundle = alice.export_table_history("docs").unwrap();

    edge.apply_untrusted_bundle_as_user(&bundle, "bob").unwrap();
    edge.apply_untrusted_bundle_as_user(&bundle, "bob").unwrap();

    assert!(edge.read_rows("docs").unwrap().is_empty());
    assert_eq!(edge.storage_stats().unwrap().rejected_transactions, 1);
    assert_eq!(
        edge.transaction_info(&tx).unwrap().rejection_detail,
        Some(json!({
            "reason": "write_policy_denied",
            "table": "docs",
            "row_id": "doc-1"
        }))
    );
    let events = edge.poll_rejections(&mut subscription).unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].tx_id, tx);
    assert!(edge.poll_rejections(&mut subscription).unwrap().is_empty());
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
#[ignore = "requires explicit resubscribe/query-settlement protocol before persisted descriptors can be removed"]
fn durable_observed_query_reads_are_connection_local_not_persisted() {
    let schema = support::tasks_schema();
    let harness = support::Harness::new();
    let mut upstream =
        Runtime::open_with_schema(Storage::Memory, "upstream", "alice", schema.clone()).unwrap();
    upstream
        .insert_row(
            "tasks",
            "task-1",
            BTreeMap::from([
                ("title".to_owned(), json!("Ephemeral interest")),
                ("done".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    let bundle = upstream
        .export_query_where_eq("tasks", "done", json!(false))
        .unwrap();

    {
        let mut worker = harness
            .durable_with_schema("worker.sqlite", "worker", "alice", schema.clone())
            .unwrap();
        worker.apply_bundle(&bundle).unwrap();
        assert_eq!(worker.observed_query_reads().unwrap().len(), 1);
    }

    let reopened = harness
        .durable_with_schema("worker.sqlite", "worker", "alice", schema)
        .unwrap();
    assert!(reopened.observed_query_reads().unwrap().is_empty());
    assert_eq!(reopened.read_rows("tasks").unwrap().len(), 1);
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
fn insert_is_create_only_for_visible_same_table_row() {
    let schema = support::notes_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema).unwrap();

    alice
        .insert_row(
            "notes",
            "note-1",
            BTreeMap::from([
                ("body".to_owned(), json!("first")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    let err = alice
        .insert_row(
            "notes",
            "note-1",
            BTreeMap::from([
                ("body".to_owned(), json!("second")),
                ("pinned".to_owned(), json!(true)),
            ]),
        )
        .unwrap_err();

    assert!(err.to_string().contains("already exists"), "{err}");
    let rows = alice.read_rows("notes").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values["body"], json!("first"));
    assert_eq!(alice.storage_stats().unwrap().history_rows, 1);
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

#[test]
fn mergeable_upsert_converges_across_multi_tier_sync() {
    let schema = support::notes_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice", "alice", schema.clone()).unwrap();
    let mut edge =
        Runtime::open_trusted_with_schema(Storage::Memory, "edge", schema.clone()).unwrap();
    let mut core =
        Runtime::open_trusted_with_schema(Storage::Memory, "core", schema.clone()).unwrap();
    let mut laptop = Runtime::open_with_schema(Storage::Memory, "laptop", "alice", schema).unwrap();

    alice
        .upsert_row(
            "notes",
            "note-upsert",
            BTreeMap::from([
                ("body".to_owned(), json!("created locally")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    support::sync_table(&alice, &mut edge, "notes").unwrap();
    support::sync_table(&edge, &mut core, "notes").unwrap();

    alice
        .upsert_row(
            "notes",
            "note-upsert",
            BTreeMap::from([("body".to_owned(), json!("updated locally"))]),
        )
        .unwrap();
    support::sync_table(&alice, &mut edge, "notes").unwrap();
    support::sync_table(&edge, &mut core, "notes").unwrap();
    support::sync_table(&core, &mut laptop, "notes").unwrap();

    let row = laptop.read_rows("notes").unwrap().remove(0);
    assert_eq!(row.id, "note-upsert");
    assert_eq!(row.values["body"], json!("updated locally"));
    assert_eq!(row.values["pinned"], json!(false));
}

#[test]
fn upsert_after_delete_restores_row_with_new_history_version() {
    let schema = support::notes_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema).unwrap();

    let create_tx = alice
        .upsert_row(
            "notes",
            "note-1",
            BTreeMap::from([
                ("body".to_owned(), json!("first")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    alice.delete_row("notes", "note-1").unwrap();
    let restore_tx = alice
        .upsert_row(
            "notes",
            "note-1",
            BTreeMap::from([
                ("body".to_owned(), json!("restored")),
                ("pinned".to_owned(), json!(true)),
            ]),
        )
        .unwrap();

    let rows = alice.read_rows("notes").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, "note-1");
    assert_eq!(rows[0].values["body"], json!("restored"));
    assert_eq!(rows[0].values["pinned"], json!(true));
    assert_ne!(create_tx, restore_tx);
    assert_eq!(
        alice.transaction_previous_read_rows(&restore_tx).unwrap(),
        Vec::<(String, String)>::new()
    );
}

#[test]
#[ignore = "exclusive upsert update semantics over an existing row are still underspecified"]
fn exclusive_global_upsert_can_create_and_then_update_same_row() {
    let schema = support::notes_schema();
    let mut core =
        Runtime::open_trusted_with_schema(Storage::Memory, "core", schema.clone()).unwrap();
    let mut peer = Runtime::open_with_schema(Storage::Memory, "peer", "alice", schema).unwrap();

    core.transaction()
        .exclusive_at_global(1)
        .upsert_row(
            "notes",
            "note-exclusive-upsert",
            BTreeMap::from([
                ("body".to_owned(), json!("created globally")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .commit()
        .unwrap();
    core.transaction()
        .exclusive_at_global(2)
        .upsert_row(
            "notes",
            "note-exclusive-upsert",
            BTreeMap::from([("pinned".to_owned(), json!(true))]),
        )
        .commit()
        .unwrap();

    support::sync_table(&core, &mut peer, "notes").unwrap();
    let row = peer.read_rows("notes").unwrap().remove(0);
    assert_eq!(row.values["body"], json!("created globally"));
    assert_eq!(row.values["pinned"], json!(true));
    assert_eq!(
        peer.transaction_info(&row.tx_id).unwrap().global_epoch,
        Some(2)
    );
}

#[test]
fn conflict_resolution_records_semantic_choice_and_clears_current_conflict_meta() {
    let schema = support::tasks_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema).unwrap();

    alice.create_branch("left", None).unwrap();
    alice.checkout_branch("left").unwrap();
    let left_tx = alice
        .insert_row(
            "tasks",
            "task-1",
            BTreeMap::from([
                ("title".to_owned(), json!("Left title")),
                ("done".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    alice.create_branch("right", None).unwrap();
    alice.checkout_branch("right").unwrap();
    let right_tx = alice
        .insert_row(
            "tasks",
            "task-1",
            BTreeMap::from([
                ("title".to_owned(), json!("Right title")),
                ("done".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    alice
        .create_branch_from_branches("merge", &["left", "right"])
        .unwrap();
    alice.checkout_branch("merge").unwrap();

    let candidates = alice.read_row_candidates("tasks", "task-1").unwrap();
    assert_eq!(candidates.len(), 2);
    assert_eq!(
        candidates
            .iter()
            .map(|row| row.tx_id.clone())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([left_tx.clone(), right_tx.clone()])
    );

    let resolution_tx = alice
        .resolve_row_conflict(
            "tasks",
            "task-1",
            BTreeMap::from([
                ("title".to_owned(), json!("Resolved title")),
                ("done".to_owned(), json!(true)),
            ]),
        )
        .unwrap();
    let rows = alice.read_rows_with_conflict_meta("tasks").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].tx_id, resolution_tx);
    assert_eq!(rows[0].conflict_count, 0);
    assert_eq!(rows[0].values["title"], json!("Resolved title"));
    assert_eq!(
        alice.transaction_write_rows(&resolution_tx).unwrap(),
        vec![("tasks".to_owned(), "task-1".to_owned())]
    );
}

#[test]
fn default_query_order_converges_across_different_apply_orders() {
    let schema = support::notes_schema();
    let mut source =
        Runtime::open_with_schema(Storage::Memory, "source", "alice", schema.clone()).unwrap();
    let mut forward =
        Runtime::open_with_schema(Storage::Memory, "forward", "alice", schema.clone()).unwrap();
    let mut reverse =
        Runtime::open_with_schema(Storage::Memory, "reverse", "alice", schema).unwrap();

    source
        .insert_row(
            "notes",
            "note-c",
            BTreeMap::from([
                ("body".to_owned(), json!("C")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    source
        .insert_row(
            "notes",
            "note-a",
            BTreeMap::from([
                ("body".to_owned(), json!("A")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    source
        .insert_row(
            "notes",
            "note-b",
            BTreeMap::from([
                ("body".to_owned(), json!("B")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    let bundle = source.export_table_history("notes").unwrap();
    let mut reverse_bundle = bundle.clone();
    reverse_bundle.history.reverse();
    reverse_bundle.txs.reverse();

    forward.apply_bundle(&bundle).unwrap();
    reverse.apply_bundle(&reverse_bundle).unwrap();

    assert_eq!(
        forward.read_rows("notes").unwrap(),
        reverse.read_rows("notes").unwrap()
    );
    assert_eq!(
        forward
            .read_rows("notes")
            .unwrap()
            .iter()
            .map(|row| row.id.as_str())
            .collect::<Vec<_>>(),
        vec!["note-a", "note-b", "note-c"]
    );
}

#[test]
fn unordered_predicate_query_order_converges_across_apply_orders() {
    let schema = support::notes_schema();
    let mut source =
        Runtime::open_with_schema(Storage::Memory, "source", "alice", schema.clone()).unwrap();
    let mut forward =
        Runtime::open_with_schema(Storage::Memory, "forward", "alice", schema.clone()).unwrap();
    let mut reverse =
        Runtime::open_with_schema(Storage::Memory, "reverse", "alice", schema).unwrap();

    for id in ["note-b", "note-a", "note-c"] {
        source
            .insert_row(
                "notes",
                id,
                BTreeMap::from([
                    ("body".to_owned(), json!("same predicate")),
                    ("pinned".to_owned(), json!(true)),
                ]),
            )
            .unwrap();
    }
    let bundle = source.export_table_history("notes").unwrap();
    let mut reverse_bundle = bundle.clone();
    reverse_bundle.history.reverse();
    reverse_bundle.txs.reverse();

    forward.apply_bundle(&bundle).unwrap();
    reverse.apply_bundle(&reverse_bundle).unwrap();

    for rows in [
        forward
            .read_rows_where_eq("notes", "pinned", json!(true))
            .unwrap(),
        reverse
            .read_rows_where_eq("notes", "pinned", json!(true))
            .unwrap(),
        forward
            .read_rows_where_contains("notes", "body", "same")
            .unwrap(),
        reverse
            .read_rows_where_contains("notes", "body", "same")
            .unwrap(),
    ] {
        assert_eq!(
            rows.iter().map(|row| row.id.as_str()).collect::<Vec<_>>(),
            vec!["note-a", "note-b", "note-c"]
        );
    }
}

#[test]
fn ordered_page_tie_breaker_converges_across_apply_orders() {
    let schema = SchemaDef::new().table("documents", |table| {
        table.text("owner");
        table.text("rank");
        table.text("title");
    });
    let mut source =
        Runtime::open_with_schema(Storage::Memory, "source", "alice", schema.clone()).unwrap();
    let mut forward =
        Runtime::open_with_schema(Storage::Memory, "forward", "alice", schema.clone()).unwrap();
    let mut reverse =
        Runtime::open_with_schema(Storage::Memory, "reverse", "alice", schema).unwrap();

    for id in ["doc-b", "doc-a", "doc-c"] {
        source
            .insert_row(
                "documents",
                id,
                BTreeMap::from([
                    ("owner".to_owned(), json!("alice")),
                    ("rank".to_owned(), json!("same-rank")),
                    ("title".to_owned(), json!(id)),
                ]),
            )
            .unwrap();
    }
    let bundle = source.export_table_history("documents").unwrap();
    let mut reverse_bundle = bundle.clone();
    reverse_bundle.history.reverse();
    reverse_bundle.txs.reverse();

    forward.apply_bundle(&bundle).unwrap();
    reverse.apply_bundle(&reverse_bundle).unwrap();

    let forward_page = forward
        .read_rows_where_eq_top_field_desc("documents", "owner", json!("alice"), "rank", 3)
        .unwrap();
    let reverse_page = reverse
        .read_rows_where_eq_top_field_desc("documents", "owner", json!("alice"), "rank", 3)
        .unwrap();
    assert_eq!(forward_page, reverse_page);
    assert_eq!(
        forward_page
            .iter()
            .map(|row| row.id.as_str())
            .collect::<Vec<_>>(),
        vec!["doc-a", "doc-b", "doc-c"]
    );
}

#[test]
fn policy_denial_for_hidden_parent_has_safe_public_shape() {
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
    let mut bob =
        Runtime::open_with_schema(Storage::Memory, "bob-node", "bob", schema.clone()).unwrap();
    let mut hidden_edge =
        Runtime::open_trusted_with_schema(Storage::Memory, "hidden-edge", schema.clone()).unwrap();

    bob.insert_row(
        "projects",
        "project-bob",
        BTreeMap::from([("title".to_owned(), json!("Bob hidden"))]),
    )
    .unwrap();
    let hidden_tx = bob
        .insert_row(
            "todos",
            "todo-hidden-parent",
            BTreeMap::from([
                ("title".to_owned(), json!("Hidden parent")),
                ("project".to_owned(), json!("project-bob")),
            ]),
        )
        .unwrap();
    let hidden_bundle = bob.export_table_history("todos").unwrap();

    hidden_edge
        .apply_untrusted_bundle_as_user(&hidden_bundle, "alice")
        .unwrap();

    assert_eq!(
        hidden_edge
            .transaction_info(&hidden_tx)
            .unwrap()
            .rejection_code,
        Some("policy_denied".to_owned())
    );
    assert_eq!(
        hidden_edge
            .transaction_info(&hidden_tx)
            .unwrap()
            .rejection_detail,
        Some(json!({
            "reason": "write_policy_denied",
            "table": "todos",
            "row_id": "todo-hidden-parent"
        }))
    );
    assert!(hidden_edge.read_rows("todos").unwrap().is_empty());
}

#[test]
fn deterministic_replay_schedule_converges_after_duplicate_and_delayed_steps() {
    let schema = support::notes_schema();
    let mut source =
        Runtime::open_with_schema(Storage::Memory, "source", "alice", schema.clone()).unwrap();
    let mut left =
        Runtime::open_with_schema(Storage::Memory, "left", "alice", schema.clone()).unwrap();
    let mut right = Runtime::open_with_schema(Storage::Memory, "right", "alice", schema).unwrap();

    source
        .insert_row(
            "notes",
            "note-1",
            BTreeMap::from([
                ("body".to_owned(), json!("first")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    let first = source.export_table_history("notes").unwrap();
    source
        .update_row(
            "notes",
            "note-1",
            BTreeMap::from([("body".to_owned(), json!("second"))]),
        )
        .unwrap();
    let second = source.export_table_history("notes").unwrap();
    source
        .update_row(
            "notes",
            "note-1",
            BTreeMap::from([("pinned".to_owned(), json!(true))]),
        )
        .unwrap();
    let third = source.export_table_history("notes").unwrap();

    for idx in [0, 1, 1, 2, 0, 2] {
        left.apply_bundle([&first, &second, &third][idx]).unwrap();
    }
    for idx in [2, 0, 2, 1, 1, 0] {
        right.apply_bundle([&first, &second, &third][idx]).unwrap();
    }

    assert_eq!(
        left.read_rows("notes").unwrap(),
        right.read_rows("notes").unwrap()
    );
    let row = left.read_rows("notes").unwrap().remove(0);
    assert_eq!(row.values["body"], json!("second"));
    assert_eq!(row.values["pinned"], json!(true));
}

#[test]
fn seeded_duplicate_reorder_schedules_converge_to_source_state() {
    let schema = support::notes_schema();
    let mut source =
        Runtime::open_with_schema(Storage::Memory, "source", "alice", schema.clone()).unwrap();
    let mut bundles = Vec::new();

    source
        .insert_row(
            "notes",
            "note-a",
            BTreeMap::from([
                ("body".to_owned(), json!("a0")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    bundles.push(source.export_table_history("notes").unwrap());
    source
        .insert_row(
            "notes",
            "note-b",
            BTreeMap::from([
                ("body".to_owned(), json!("b0")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    bundles.push(source.export_table_history("notes").unwrap());
    source
        .update_row(
            "notes",
            "note-a",
            BTreeMap::from([("body".to_owned(), json!("a1"))]),
        )
        .unwrap();
    bundles.push(source.export_table_history("notes").unwrap());
    source.delete_row("notes", "note-b").unwrap();
    bundles.push(source.export_table_history("notes").unwrap());
    source
        .insert_row(
            "notes",
            "note-c",
            BTreeMap::from([
                ("body".to_owned(), json!("c0")),
                ("pinned".to_owned(), json!(true)),
            ]),
        )
        .unwrap();
    bundles.push(source.export_table_history("notes").unwrap());

    let expected = source.read_rows("notes").unwrap();
    for seed in 1..=8 {
        let mut peer = Runtime::open_with_schema(
            Storage::Memory,
            &format!("peer-{seed}"),
            "alice",
            schema.clone(),
        )
        .unwrap();
        for idx in seeded_bundle_schedule(bundles.len(), seed) {
            peer.apply_bundle(&bundles[idx]).unwrap();
        }
        assert_eq!(peer.read_rows("notes").unwrap(), expected);
    }
}

#[test]
fn simple_write_calls_and_explicit_transactions_have_expected_tx_granularity() {
    let schema = support::notes_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema).unwrap();

    let first_tx = alice
        .insert_row(
            "notes",
            "note-simple-a",
            BTreeMap::from([
                ("body".to_owned(), json!("first")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    let second_tx = alice
        .insert_row(
            "notes",
            "note-simple-b",
            BTreeMap::from([
                ("body".to_owned(), json!("second")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    assert_ne!(first_tx, second_tx);

    let explicit_tx = alice
        .transaction()
        .insert_row(
            "notes",
            "note-batched-a",
            BTreeMap::from([
                ("body".to_owned(), json!("batched a")),
                ("pinned".to_owned(), json!(true)),
            ]),
        )
        .insert_row(
            "notes",
            "note-batched-b",
            BTreeMap::from([
                ("body".to_owned(), json!("batched b")),
                ("pinned".to_owned(), json!(true)),
            ]),
        )
        .commit()
        .unwrap();

    let bundle = alice.export_table_history("notes").unwrap();
    assert_eq!(bundle.txs.len(), 3);
    assert_eq!(
        bundle
            .history
            .iter()
            .filter(|history| history.tx_id == explicit_tx)
            .map(|history| history.row_id.as_str())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from(["note-batched-a", "note-batched-b"])
    );
}

#[test]
fn query_scope_export_includes_full_history_for_result_rows() {
    let schema = support::notes_schema();
    let mut upstream =
        Runtime::open_with_schema(Storage::Memory, "upstream", "alice", schema.clone()).unwrap();
    let mut peer = Runtime::open_with_schema(Storage::Memory, "peer", "alice", schema).unwrap();

    upstream
        .insert_row(
            "notes",
            "note-history",
            BTreeMap::from([
                ("body".to_owned(), json!("draft")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    upstream
        .update_row(
            "notes",
            "note-history",
            BTreeMap::from([("body".to_owned(), json!("ready"))]),
        )
        .unwrap();

    let bundle = upstream
        .export_query_where_eq("notes", "pinned", json!(false))
        .unwrap();
    assert_eq!(
        bundle
            .history
            .iter()
            .filter(|history| history.row_id == "note-history")
            .count(),
        2
    );

    peer.apply_bundle(&bundle).unwrap();
    peer.clear_current_projection_for_test().unwrap();
    peer.rebuild_current_projection().unwrap();
    let row = peer.read_rows("notes").unwrap().remove(0);
    assert_eq!(row.id, "note-history");
    assert_eq!(row.values["body"], json!("ready"));
}

#[test]
fn accepted_transaction_history_cannot_be_rewritten_by_same_tx_id_replay() {
    let schema = support::notes_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice", "alice", schema.clone()).unwrap();
    let mut peer = Runtime::open_with_schema(Storage::Memory, "peer", "alice", schema).unwrap();

    let tx = alice
        .insert_row(
            "notes",
            "note-immutable",
            BTreeMap::from([
                ("body".to_owned(), json!("original")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    alice.accept_transaction_at_global(&tx, 1).unwrap();
    let mut forged = alice.export_table_history("notes").unwrap();
    forged.history[0]
        .values
        .insert("body".to_owned(), json!("forged"));

    peer.apply_bundle(&alice.export_table_history("notes").unwrap())
        .unwrap();
    peer.apply_bundle(&forged).unwrap();

    let row = peer.read_rows("notes").unwrap().remove(0);
    assert_eq!(row.values["body"], json!("original"));
    assert_eq!(peer.transaction_info(&tx).unwrap().global_epoch, Some(1));
}

#[test]
fn subscription_diff_order_is_deterministic_for_mixed_add_update_remove() {
    let schema = support::notes_schema();
    let mut alice = Runtime::open_with_schema(Storage::Memory, "alice", "alice", schema).unwrap();

    for id in ["note-b", "note-a", "note-c"] {
        alice
            .insert_row(
                "notes",
                id,
                BTreeMap::from([
                    ("body".to_owned(), json!(id)),
                    ("pinned".to_owned(), json!(true)),
                ]),
            )
            .unwrap();
    }
    let mut subscription = alice.subscribe_rows("notes").unwrap();

    alice.delete_row("notes", "note-b").unwrap();
    alice
        .update_row(
            "notes",
            "note-c",
            BTreeMap::from([("body".to_owned(), json!("note-c updated"))]),
        )
        .unwrap();
    alice
        .insert_row(
            "notes",
            "note-d",
            BTreeMap::from([
                ("body".to_owned(), json!("note-d")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .unwrap();

    let diffs = alice.poll_subscription(&mut subscription).unwrap();
    let diff_keys = diffs
        .iter()
        .map(|diff| match diff {
            RowDiff::Added(row) => format!("added:{}", row.id),
            RowDiff::Updated { before, .. } => format!("updated:{}", before.id),
            RowDiff::Moved { row, .. } => format!("moved:{}", row.id),
            RowDiff::Removed(row) => format!("removed:{}", row.id),
        })
        .collect::<Vec<_>>();
    assert_eq!(
        diff_keys,
        vec!["removed:note-b", "updated:note-c", "added:note-d"]
    );
}

#[test]
fn restore_deleted_row_reuses_insert_semantics_and_creates_new_history_version() {
    let schema = support::notes_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice", "alice", schema.clone()).unwrap();
    let mut bob = Runtime::open_with_schema(Storage::Memory, "bob", "bob", schema).unwrap();

    let create_tx = alice
        .insert_row(
            "notes",
            "note-restore",
            BTreeMap::from([
                ("body".to_owned(), json!("restore me")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    alice.delete_row("notes", "note-restore").unwrap();
    bob.apply_bundle(&alice.export_table_history("notes").unwrap())
        .unwrap();

    let restore_tx = bob.restore_deleted_row("notes", "note-restore").unwrap();
    assert_ne!(restore_tx, create_tx);
    let row = bob.read_rows("notes").unwrap().remove(0);
    assert_eq!(row.id, "note-restore");
    assert_eq!(row.created_by, "bob");
    assert_eq!(row.values["body"], json!("restore me"));
    assert_eq!(bob.storage_stats().unwrap().history_rows, 3);
}

#[test]
fn stale_delete_bundle_cannot_hide_restored_row() {
    let schema = support::notes_schema();
    let mut source =
        Runtime::open_with_schema(Storage::Memory, "source", "alice", schema.clone()).unwrap();
    let mut peer = Runtime::open_with_schema(Storage::Memory, "peer", "alice", schema).unwrap();

    source
        .insert_row(
            "notes",
            "note-restore",
            BTreeMap::from([
                ("body".to_owned(), json!("first")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    source.delete_row("notes", "note-restore").unwrap();
    let stale_deleted = source.export_table_history("notes").unwrap();
    source.restore_deleted_row("notes", "note-restore").unwrap();

    peer.apply_bundle(&source.export_table_history("notes").unwrap())
        .unwrap();
    let history_rows_after_restore = peer.storage_stats().unwrap().history_rows;
    peer.apply_bundle(&stale_deleted).unwrap();

    let rows = peer.read_rows("notes").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, "note-restore");
    assert_eq!(
        peer.storage_stats().unwrap().history_rows,
        history_rows_after_restore
    );
}

#[test]
fn public_row_ids_are_globally_unique_across_tables() {
    let schema = SchemaDef::new()
        .table("projects", |table| {
            table.text("title");
        })
        .table("todos", |table| {
            table.text("title");
        });
    let mut alice = Runtime::open_with_schema(Storage::Memory, "alice", "alice", schema).unwrap();

    alice
        .insert_row(
            "projects",
            "shared-id",
            BTreeMap::from([("title".to_owned(), json!("Project"))]),
        )
        .unwrap();
    let err = alice
        .insert_row(
            "todos",
            "shared-id",
            BTreeMap::from([("title".to_owned(), json!("Todo"))]),
        )
        .unwrap_err();

    assert!(
        err.to_string().contains("already used by another table"),
        "{err}"
    );
    assert_eq!(alice.read_rows("projects").unwrap().len(), 1);
    assert!(alice.read_rows("todos").unwrap().is_empty());
}

#[test]
fn unresolved_ref_ids_can_later_be_claimed_by_the_target_table() {
    let schema = SchemaDef::new()
        .table("projects", |table| {
            table.text("title");
        })
        .table("todos", |table| {
            table.text("title");
            table.ref_("project", "projects");
        });
    let mut alice = Runtime::open_with_schema(Storage::Memory, "alice", "alice", schema).unwrap();

    alice
        .insert_row(
            "todos",
            "todo-1",
            BTreeMap::from([
                ("title".to_owned(), json!("References future project")),
                ("project".to_owned(), json!("project-later")),
            ]),
        )
        .unwrap();
    alice
        .insert_row(
            "projects",
            "project-later",
            BTreeMap::from([("title".to_owned(), json!("Created later"))]),
        )
        .unwrap();

    assert_eq!(alice.read_rows("projects").unwrap().len(), 1);
    assert_eq!(alice.read_rows("todos").unwrap().len(), 1);
}

#[test]
fn synced_history_cannot_reuse_public_row_id_in_another_table() {
    let schema = SchemaDef::new()
        .table("projects", |table| {
            table.text("title");
        })
        .table("todos", |table| {
            table.text("title");
        });
    let mut source =
        Runtime::open_with_schema(Storage::Memory, "source", "alice", schema.clone()).unwrap();
    let mut peer = Runtime::open_with_schema(Storage::Memory, "peer", "alice", schema).unwrap();

    source
        .insert_row(
            "projects",
            "shared-id",
            BTreeMap::from([("title".to_owned(), json!("Project"))]),
        )
        .unwrap();
    let projects = source.export_table_history("projects").unwrap();
    let mut forged = projects.clone();
    forged.policy_fingerprint = "legacy".to_owned();
    forged.history[0].table = "todos".to_owned();
    forged.history[0]
        .values
        .insert("title".to_owned(), json!("Forged todo"));

    peer.apply_bundle(&projects).unwrap();
    let err = peer.apply_bundle(&forged).unwrap_err();

    assert!(
        err.to_string().contains("already used by another table"),
        "{err}"
    );
    assert_eq!(peer.read_rows("projects").unwrap().len(), 1);
    assert!(peer.read_rows("todos").unwrap().is_empty());
}

#[test]
#[ignore = "bundle merge currently requires identical scoped metadata; union semantics are still underspecified"]
fn merging_overlapping_query_bundles_is_order_independent_and_deduped() {
    let mut upstream = Runtime::open(Storage::Memory, "upstream", "alice").unwrap();
    let mut left_peer = Runtime::open(Storage::Memory, "left-peer", "alice").unwrap();
    let mut right_peer = Runtime::open(Storage::Memory, "right-peer", "alice").unwrap();

    upstream
        .create_project("project-1", "Shared dependency")
        .unwrap();
    upstream
        .create_todo("todo-a", "Alpha", false, "project-1")
        .unwrap();
    upstream
        .create_todo("todo-b", "Beta", false, "project-1")
        .unwrap();

    let open = upstream.export_query_scope_open_todos().unwrap();
    let alpha = upstream
        .export_query_where_contains("todos", "title", "Alpha")
        .unwrap();
    let left = merge_bundles(&[open.clone(), alpha.clone(), open.clone()]).unwrap();
    let right = merge_bundles(&[alpha, open]).unwrap();

    assert_eq!(left.history.len(), right.history.len());
    assert_eq!(left.txs.len(), right.txs.len());
    assert_eq!(left.query_reads.len(), right.query_reads.len());

    left_peer.apply_bundle(&left).unwrap();
    right_peer.apply_bundle(&right).unwrap();
    assert_eq!(
        left_peer.open_todos().unwrap(),
        right_peer.open_todos().unwrap()
    );
    assert_eq!(
        left_peer.observed_query_reads().unwrap(),
        right_peer.observed_query_reads().unwrap()
    );
}

#[test]
#[ignore = "file/blob product surface is not implemented yet"]
fn file_blob_bytes_do_not_bypass_row_policy_placeholder() {
    panic!(
        "future invariant: blob metadata and byte serving must both re-check Jazz session policy"
    );
}

#[test]
#[ignore = "encrypted fields and encrypted indexes are not implemented yet"]
fn encrypted_fields_do_not_participate_in_server_plaintext_querying_placeholder() {
    panic!(
        "future invariant: client-decrypted fields cannot be used by untrusted servers for plaintext filtering, sorting, indexing, or policy"
    );
}

#[test]
#[ignore = "admin/tooling catalogue publication flow is not implemented yet"]
fn catalogue_publication_requires_admin_and_fails_closed_without_permissions_placeholder() {
    panic!(
        "future invariant: schema+permission catalogue publication is admin controlled and missing explicit permissions fail closed"
    );
}

#[test]
#[ignore = "predicate/range read-set validation is not implemented yet"]
fn exclusive_predicate_read_set_rejects_when_matching_row_is_inserted_later_placeholder() {
    panic!(
        "future invariant: exclusive transactions that read a predicate/range must reject if an authority-visible matching row appears before validation"
    );
}

#[test]
#[ignore = "branch backing rows are not yet product permission objects in the prototype"]
fn unreadable_branch_backing_row_prevents_checkout_and_export_placeholder() {
    panic!(
        "future invariant: branch handles/checkouts/exports require readable branch backing rows as well as engine metadata"
    );
}

#[test]
#[ignore = "range observed facts and range read-set validation are not implemented yet"]
fn range_query_refresh_repairs_rows_that_enter_and_leave_boundaries_placeholder() {
    panic!(
        "future invariant: range query interests must sync current matches, repair rows that leave the range, and feed authority range read-set validation"
    );
}

#[test]
#[ignore = "cache eviction policy is not implemented yet"]
fn evicting_uninteresting_local_facts_preserves_history_needed_for_active_queries_placeholder() {
    panic!(
        "future invariant: async cache eviction may drop uninteresting local facts only when active queries, policy deps, and replay/export needs remain reconstructible"
    );
}

#[test]
#[ignore = "as-of-time query API and timestamp-to-epoch mapping are not implemented yet"]
fn as_of_time_query_maps_timestamp_to_stable_global_snapshot_placeholder() {
    panic!(
        "future invariant: as-of-time queries should map wall-clock time to a stable global epoch snapshot without exposing partially settled history"
    );
}

#[test]
#[ignore = "stable public error surface is not implemented yet"]
fn public_errors_use_stable_codes_and_redacted_details_across_surfaces_placeholder() {
    panic!(
        "future invariant: write promises, query failures, sync failures, rejection subscriptions, and global callbacks should expose stable machine codes with redacted details"
    );
}

#[test]
#[ignore = "query settled-state barriers are not implemented yet"]
fn observed_query_refresh_reports_settled_state_after_all_descriptors_refresh_placeholder() {
    panic!(
        "future invariant: query/subscription state should distinguish retained local facts from all active descriptors refreshed through a known upstream authority point"
    );
}

#[test]
#[ignore = "compact reconnect summaries are not implemented yet"]
fn compact_reconnect_summary_refreshes_only_active_query_descriptors_placeholder() {
    panic!(
        "future invariant: reconnect should replay active query descriptors compactly, refresh only live interests, and leave forgotten retained facts as cache state"
    );
}

#[test]
#[ignore = "catalogue observed facts are not implemented yet"]
fn catalogue_observed_fact_invalidates_query_when_schema_head_changes_placeholder() {
    panic!(
        "future invariant: queries interpreted through a catalogue/lens head should observe catalogue facts and invalidate when the relevant schema head changes"
    );
}

#[test]
#[ignore = "permission-only catalogue publication is not implemented yet"]
fn missing_permission_catalogue_fails_closed_for_query_export_placeholder() {
    panic!(
        "future invariant: a known structural schema without explicit current permissions must fail closed for query export and sync"
    );
}

#[test]
#[ignore = "staged untrusted authority apply before publication is not implemented yet"]
fn staged_untrusted_apply_is_not_visible_until_authority_publication_placeholder() {
    panic!(
        "future invariant: an authority may validate/stage incoming untrusted history before publication, but staged rows must not be visible to ordinary reads or subscriptions"
    );
}

#[test]
#[ignore = "resolved conflict provenance metadata shape is not implemented yet"]
fn conflict_resolution_preserves_resolved_candidate_provenance_placeholder() {
    panic!(
        "future invariant: conflict resolution should retain which candidate tx ids / branch bases were resolved, even after current conflict metadata is cleared"
    );
}

#[test]
#[ignore = "generated index and query-plan assertions are not implemented yet"]
fn generated_indexes_are_used_for_ordered_page_query_plan_placeholder() {
    panic!(
        "future invariant: generated SQLite indexes should keep ordered page queries off accidental broad table scans"
    );
}
