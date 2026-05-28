use super::*;

#[test]
fn recursive_policy_filters_reads_through_grandparent_ref() {
    let schema = SchemaDef::new()
        .table("orgs", |table| {
            table.text("name");
            table.read_if_created_by_user();
        })
        .table("projects", |table| {
            table.text("title");
            table.ref_("org", "orgs");
            table.read_if_ref_readable("org");
        })
        .table("todos", |table| {
            table.text("title");
            table.ref_("project", "projects");
            table.read_if_ref_readable("project");
        });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut bob = Runtime::open_with_schema(Storage::Memory, "bob-node", "bob", schema).unwrap();

    alice
        .insert_row(
            "orgs",
            "org-alice",
            BTreeMap::from([("name".to_owned(), json!("Alice org"))]),
        )
        .unwrap();
    alice
        .insert_row(
            "projects",
            "project-alice",
            BTreeMap::from([
                ("title".to_owned(), json!("Alice project")),
                ("org".to_owned(), json!("org-alice")),
            ]),
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
        "orgs",
        "org-bob",
        BTreeMap::from([("name".to_owned(), json!("Bob org"))]),
    )
    .unwrap();
    bob.insert_row(
        "projects",
        "project-bob",
        BTreeMap::from([
            ("title".to_owned(), json!("Bob project")),
            ("org".to_owned(), json!("org-bob")),
        ]),
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

    alice
        .apply_bundle(&bob.export_table_history("orgs").unwrap())
        .unwrap();
    alice
        .apply_bundle(&bob.export_table_history("projects").unwrap())
        .unwrap();
    alice
        .apply_bundle(&bob.export_table_history("todos").unwrap())
        .unwrap();

    let visible = alice.read_rows("todos").unwrap();
    assert_eq!(visible.len(), 1);
    assert_eq!(visible[0].id, "todo-visible");
    assert_eq!(visible[0].values["project"], json!("project-alice"));
}

#[test]
fn long_acyclic_ref_policy_chain_reads_visible_leaf() {
    let table_names = (0..18)
        .map(|idx| format!("levels_{idx}"))
        .collect::<Vec<_>>();
    let mut schema = SchemaDef::new();
    for idx in 0..table_names.len() {
        let table_name = table_names[idx].clone();
        let parent_name = idx.checked_sub(1).map(|parent| table_names[parent].clone());
        schema = schema.table(&table_name, move |table| {
            table.text("name");
            if let Some(parent_name) = parent_name {
                table.ref_("parent", &parent_name);
                table.read_if_ref_readable("parent");
            } else {
                table.read_if_created_by_user();
            }
        });
    }
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema).unwrap();

    for (idx, table_name) in table_names.iter().enumerate() {
        let mut values = BTreeMap::from([("name".to_owned(), json!(format!("Level {idx}")))]);
        if idx > 0 {
            values.insert("parent".to_owned(), json!(format!("row-{}", idx - 1)));
        }
        alice
            .insert_row(table_name, &format!("row-{idx}"), values)
            .unwrap();
    }

    let leaf_rows = alice.read_rows(table_names.last().unwrap()).unwrap();
    assert_eq!(leaf_rows.len(), 1);
    assert_eq!(leaf_rows[0].id, "row-17");
}

#[test]
fn schema_rejects_direct_recursive_policy_cycle() {
    let schema = SchemaDef::new().table("folders", |table| {
        table.text("name");
        table.ref_("parent", "folders");
        table.read_if_ref_readable("parent");
    });

    let err = Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema)
        .err()
        .unwrap();
    assert!(err.to_string().contains("policy cycle"));
}

#[test]
fn schema_rejects_indirect_recursive_policy_cycle() {
    let schema = SchemaDef::new()
        .table("orgs", |table| {
            table.text("name");
            table.ref_("root_task", "tasks");
            table.read_if_ref_readable("root_task");
        })
        .table("projects", |table| {
            table.text("title");
            table.ref_("org", "orgs");
            table.read_if_ref_readable("org");
        })
        .table("tasks", |table| {
            table.text("title");
            table.ref_("project", "projects");
            table.read_if_ref_readable("project");
        });

    let err = Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema)
        .err()
        .unwrap();
    assert!(err.to_string().contains("policy cycle"));
}

#[test]
fn long_acyclic_recursive_policy_chain_is_sql_lowerable() {
    let mut schema = SchemaDef::new();
    for idx in (0..20).rev() {
        let table_name = format!("chain_{idx}");
        let parent_name = format!("chain_{}", idx + 1);
        schema = schema.table(&table_name, |table| {
            table.text("name");
            if idx == 19 {
                table.read_if_created_by_user();
            } else {
                table.ref_("parent", &parent_name);
                table.read_if_ref_readable("parent");
            }
        });
    }
    let mut writer =
        Runtime::open_trusted_with_session_user(Storage::Memory, "writer", "alice", schema.clone())
            .unwrap();
    let mut reader =
        Runtime::open_with_schema(Storage::Memory, "reader", "alice", schema.clone()).unwrap();

    writer
        .insert_row(
            "chain_19",
            "row-19",
            BTreeMap::from([("name".to_owned(), json!("row 19"))]),
        )
        .unwrap();
    for idx in (0..19).rev() {
        writer
            .insert_row(
                &format!("chain_{idx}"),
                &format!("row-{idx}"),
                BTreeMap::from([
                    ("name".to_owned(), json!(format!("row {idx}"))),
                    ("parent".to_owned(), json!(format!("row-{}", idx + 1))),
                ]),
            )
            .unwrap();
    }
    for idx in 0..20 {
        reader
            .apply_bundle(
                &writer
                    .export_table_history(&format!("chain_{idx}"))
                    .unwrap(),
            )
            .unwrap();
    }

    let rows = reader.read_rows("chain_0").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, "row-0");
}

#[test]
fn recursive_policy_scoped_sync_includes_transitive_parent_rows() {
    let schema = SchemaDef::new()
        .table("orgs", |table| {
            table.text("name");
            table.read_if_created_by_user();
        })
        .table("projects", |table| {
            table.text("title");
            table.ref_("org", "orgs");
            table.read_if_ref_readable("org");
        })
        .table("todos", |table| {
            table.text("title");
            table.ref_("project", "projects");
            table.read_if_ref_readable("project");
        });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut peer =
        Runtime::open_with_schema(Storage::Memory, "alice-peer-node", "alice", schema).unwrap();

    alice
        .insert_row(
            "orgs",
            "org-visible",
            BTreeMap::from([("name".to_owned(), json!("Visible org"))]),
        )
        .unwrap();
    alice
        .insert_row(
            "orgs",
            "org-unrelated",
            BTreeMap::from([("name".to_owned(), json!("Unrelated org"))]),
        )
        .unwrap();
    alice
        .insert_row(
            "projects",
            "project-visible",
            BTreeMap::from([
                ("title".to_owned(), json!("Visible project")),
                ("org".to_owned(), json!("org-visible")),
            ]),
        )
        .unwrap();
    alice
        .insert_row(
            "todos",
            "todo-visible",
            BTreeMap::from([
                ("title".to_owned(), json!("Visible todo")),
                ("project".to_owned(), json!("project-visible")),
            ]),
        )
        .unwrap();

    let bundle = alice.export_table_history("todos").unwrap();
    let synced = bundle
        .history
        .iter()
        .map(|record| (record.table.as_str(), record.row_id.as_str()))
        .collect::<Vec<_>>();
    assert!(synced.contains(&("todos", "todo-visible")));
    assert!(synced.contains(&("projects", "project-visible")));
    assert!(synced.contains(&("orgs", "org-visible")));
    assert!(!synced.contains(&("orgs", "org-unrelated")));

    peer.apply_bundle(&bundle).unwrap();
    let rows = peer.read_rows("todos").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, "todo-visible");
}

#[test]
fn recursive_query_reads_policy_filtered_tree() {
    let schema = support::folders_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut bob = Runtime::open_with_schema(Storage::Memory, "bob-node", "bob", schema).unwrap();

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
    alice
        .insert_row(
            "folders",
            "child-alice",
            BTreeMap::from([
                ("name".to_owned(), json!("Alice child")),
                ("parent".to_owned(), json!("root")),
            ]),
        )
        .unwrap();
    bob.insert_row(
        "folders",
        "child-bob",
        BTreeMap::from([
            ("name".to_owned(), json!("Bob child")),
            ("parent".to_owned(), json!("root")),
        ]),
    )
    .unwrap();
    alice
        .apply_bundle(&bob.export_table_history("folders").unwrap())
        .unwrap();

    let rows = alice
        .read_recursive_refs("folders", "root", "parent")
        .unwrap();
    let ids = rows.iter().map(|row| row.id.as_str()).collect::<Vec<_>>();
    assert_eq!(ids, vec!["root", "child-alice"]);
}

#[test]
fn recursive_query_scope_sync_recreates_policy_filtered_tree() {
    let schema = support::folders_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut peer =
        Runtime::open_with_schema(Storage::Memory, "alice-peer-node", "alice", schema).unwrap();

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
    alice
        .insert_row(
            "folders",
            "child",
            BTreeMap::from([
                ("name".to_owned(), json!("Child")),
                ("parent".to_owned(), json!("root")),
            ]),
        )
        .unwrap();
    alice
        .insert_row(
            "folders",
            "unrelated",
            BTreeMap::from([
                ("name".to_owned(), json!("Unrelated")),
                ("parent".to_owned(), json!("unrelated")),
            ]),
        )
        .unwrap();

    let bundle = alice
        .export_recursive_refs("folders", "root", "parent")
        .unwrap();
    let synced = bundle
        .history
        .iter()
        .map(|record| record.row_id.as_str())
        .collect::<Vec<_>>();
    assert_eq!(synced, vec!["root", "child"]);
    assert!(!synced.contains(&"unrelated"));

    peer.apply_bundle(&bundle).unwrap();
    let ids = peer
        .read_recursive_refs("folders", "root", "parent")
        .unwrap()
        .iter()
        .map(|row| row.id.clone())
        .collect::<Vec<_>>();
    assert_eq!(ids, vec!["root", "child"]);
}

#[test]
fn recursive_query_read_refresh_delivers_later_descendant_and_subscription_diff() {
    let schema = support::folders_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut peer =
        Runtime::open_with_schema(Storage::Memory, "alice-peer-node", "alice", schema).unwrap();

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
    peer.apply_bundle(
        &alice
            .export_recursive_refs("folders", "root", "parent")
            .unwrap(),
    )
    .unwrap();
    assert_eq!(peer.observed_query_reads().unwrap()[0].op, "recursive_refs");
    let mut subscription = peer
        .subscribe_observed_query(&peer.observed_query_reads().unwrap()[0])
        .unwrap();
    assert_eq!(subscription.initial_rows().len(), 1);

    alice
        .insert_row(
            "folders",
            "child",
            BTreeMap::from([
                ("name".to_owned(), json!("Child")),
                ("parent".to_owned(), json!("root")),
            ]),
        )
        .unwrap();
    for refresh in alice
        .export_query_read_refreshes(&peer.observed_query_reads().unwrap())
        .unwrap()
    {
        peer.apply_bundle(&refresh).unwrap();
    }

    let diffs = peer.poll_subscription(&mut subscription).unwrap();
    assert!(matches!(&diffs[..], [RowDiff::Added(row)] if row.id == "child"));
    let ids = peer
        .read_recursive_refs("folders", "root", "parent")
        .unwrap()
        .iter()
        .map(|row| row.id.clone())
        .collect::<Vec<_>>();
    assert_eq!(ids, vec!["root", "child"]);
}

#[test]
fn recursive_query_read_refresh_batches_same_shape_roots() {
    let schema = support::folders_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut peer =
        Runtime::open_with_schema(Storage::Memory, "alice-peer-node", "alice", schema).unwrap();

    for (id, name) in [("root-a", "Root A"), ("root-b", "Root B")] {
        alice
            .insert_row(
                "folders",
                id,
                BTreeMap::from([
                    ("name".to_owned(), json!(name)),
                    ("parent".to_owned(), json!(id)),
                ]),
            )
            .unwrap();
        peer.apply_bundle(
            &alice
                .export_recursive_refs("folders", id, "parent")
                .unwrap(),
        )
        .unwrap();
    }
    assert_eq!(peer.observed_query_reads().unwrap().len(), 2);

    alice
        .insert_row(
            "folders",
            "child-a",
            BTreeMap::from([
                ("name".to_owned(), json!("Child A")),
                ("parent".to_owned(), json!("root-a")),
            ]),
        )
        .unwrap();
    alice
        .insert_row(
            "folders",
            "child-b",
            BTreeMap::from([
                ("name".to_owned(), json!("Child B")),
                ("parent".to_owned(), json!("root-b")),
            ]),
        )
        .unwrap();

    let refreshes = alice
        .export_query_read_refreshes(&peer.observed_query_reads().unwrap())
        .unwrap();
    assert_eq!(refreshes.len(), 1);
    assert_eq!(refreshes[0].query_reads.len(), 2);
    peer.apply_bundle(&refreshes[0]).unwrap();

    let root_a_ids = peer
        .read_recursive_refs("folders", "root-a", "parent")
        .unwrap()
        .iter()
        .map(|row| row.id.clone())
        .collect::<Vec<_>>();
    let root_b_ids = peer
        .read_recursive_refs("folders", "root-b", "parent")
        .unwrap()
        .iter()
        .map(|row| row.id.clone())
        .collect::<Vec<_>>();
    assert_eq!(root_a_ids, vec!["root-a", "child-a"]);
    assert_eq!(root_b_ids, vec!["root-b", "child-b"]);
}

#[test]
fn durable_recursive_query_read_refreshes_after_restart() {
    let dir = tempdir().unwrap();
    let worker_path = dir.path().join("recursive-worker.sqlite");
    let schema = support::folders_schema();
    let mut upstream =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();

    upstream
        .insert_row(
            "folders",
            "root",
            BTreeMap::from([
                ("name".to_owned(), json!("Root")),
                ("parent".to_owned(), json!("root")),
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
                    .export_recursive_refs("folders", "root", "parent")
                    .unwrap(),
            )
            .unwrap();
        assert_eq!(worker.observed_query_reads().unwrap().len(), 1);
    }

    upstream
        .insert_row(
            "folders",
            "child",
            BTreeMap::from([
                ("name".to_owned(), json!("Child after restart")),
                ("parent".to_owned(), json!("root")),
            ]),
        )
        .unwrap();

    let mut reopened =
        Runtime::open_with_schema(Storage::File(worker_path), "worker", "alice", schema).unwrap();
    let desired_queries = reopened.observed_query_reads().unwrap();
    assert_eq!(desired_queries[0].op, "recursive_refs");
    for refresh in upstream
        .export_query_read_refreshes(&desired_queries)
        .unwrap()
    {
        reopened.apply_bundle(&refresh).unwrap();
    }

    let ids = reopened
        .read_recursive_refs("folders", "root", "parent")
        .unwrap()
        .iter()
        .map(|row| row.id.clone())
        .collect::<Vec<_>>();
    assert_eq!(ids, vec!["root", "child"]);
}

#[test]
fn recursive_observed_query_refresh_removes_deleted_descendant_with_subscription_diff() {
    let schema = support::folders_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut peer =
        Runtime::open_with_schema(Storage::Memory, "alice-peer-node", "alice", schema).unwrap();

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
    alice
        .insert_row(
            "folders",
            "child",
            BTreeMap::from([
                ("name".to_owned(), json!("Child")),
                ("parent".to_owned(), json!("root")),
            ]),
        )
        .unwrap();
    peer.apply_bundle(
        &alice
            .export_recursive_refs("folders", "root", "parent")
            .unwrap(),
    )
    .unwrap();
    let mut subscription = peer
        .subscribe_observed_query(&peer.observed_query_reads().unwrap()[0])
        .unwrap();
    assert_eq!(subscription.initial_rows().len(), 2);

    alice.delete_row("folders", "child").unwrap();
    for refresh in alice
        .export_query_read_refreshes(&peer.observed_query_reads().unwrap())
        .unwrap()
    {
        peer.apply_bundle(&refresh).unwrap();
    }

    let diffs = peer.poll_subscription(&mut subscription).unwrap();
    assert!(matches!(&diffs[..], [RowDiff::Removed(row)] if row.id == "child"));
    let ids = peer
        .read_recursive_refs("folders", "root", "parent")
        .unwrap()
        .iter()
        .map(|row| row.id.clone())
        .collect::<Vec<_>>();
    assert_eq!(ids, vec!["root"]);
}

#[test]
fn recursive_observed_query_refresh_removes_policy_hidden_descendant_with_subscription_diff() {
    let schema = SchemaDef::new()
        .table("orgs", |table| {
            table.text("name");
            table.read_if_created_by_user();
        })
        .table("folders", |table| {
            table.text("name");
            table.ref_("parent", "folders");
            table.ref_("org", "orgs");
            table.read_if_ref_readable("org");
        });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut bob =
        Runtime::open_with_schema(Storage::Memory, "bob-node", "bob", schema.clone()).unwrap();
    let mut peer =
        Runtime::open_with_schema(Storage::Memory, "alice-peer-node", "alice", schema).unwrap();

    alice
        .insert_row(
            "orgs",
            "org-alice",
            BTreeMap::from([("name".to_owned(), json!("Alice org"))]),
        )
        .unwrap();
    alice
        .insert_row(
            "folders",
            "root",
            BTreeMap::from([
                ("name".to_owned(), json!("Root")),
                ("parent".to_owned(), json!("root")),
                ("org".to_owned(), json!("org-alice")),
            ]),
        )
        .unwrap();
    alice
        .insert_row(
            "folders",
            "child",
            BTreeMap::from([
                ("name".to_owned(), json!("Child")),
                ("parent".to_owned(), json!("root")),
                ("org".to_owned(), json!("org-alice")),
            ]),
        )
        .unwrap();

    peer.apply_bundle(
        &alice
            .export_recursive_refs("folders", "root", "parent")
            .unwrap(),
    )
    .unwrap();
    let observed = peer.observed_query_reads().unwrap();
    let mut subscription = peer.subscribe_observed_query(&observed[0]).unwrap();
    assert_eq!(subscription.initial_rows().len(), 2);

    bob.insert_row(
        "orgs",
        "org-bob",
        BTreeMap::from([("name".to_owned(), json!("Bob org"))]),
    )
    .unwrap();
    alice
        .apply_bundle(&bob.export_table_history("orgs").unwrap())
        .unwrap();
    alice
        .update_row(
            "folders",
            "child",
            BTreeMap::from([("org".to_owned(), json!("org-bob"))]),
        )
        .unwrap();
    assert_eq!(
        alice
            .read_recursive_refs("folders", "root", "parent")
            .unwrap()
            .iter()
            .map(|row| row.id.as_str())
            .collect::<Vec<_>>(),
        vec!["root"]
    );

    for refresh in alice.export_query_read_refreshes(&observed).unwrap() {
        peer.apply_bundle(&refresh).unwrap();
    }

    let diffs = peer.poll_subscription(&mut subscription).unwrap();
    assert!(matches!(&diffs[..], [RowDiff::Removed(row)] if row.id == "child"));
    let ids = peer
        .read_recursive_refs("folders", "root", "parent")
        .unwrap()
        .iter()
        .map(|row| row.id.clone())
        .collect::<Vec<_>>();
    assert_eq!(ids, vec!["root"]);
}

#[test]
fn recursive_query_scope_sync_exports_deleted_descendant_tombstone() {
    let schema = support::folders_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut peer =
        Runtime::open_with_schema(Storage::Memory, "alice-peer-node", "alice", schema).unwrap();

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
    alice
        .insert_row(
            "folders",
            "child",
            BTreeMap::from([
                ("name".to_owned(), json!("Child")),
                ("parent".to_owned(), json!("root")),
            ]),
        )
        .unwrap();

    peer.apply_bundle(
        &alice
            .export_recursive_refs("folders", "root", "parent")
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        peer.read_recursive_refs("folders", "root", "parent")
            .unwrap()
            .len(),
        2
    );

    alice.delete_row("folders", "child").unwrap();
    let delete_bundle = alice
        .export_recursive_refs("folders", "root", "parent")
        .unwrap();
    assert!(delete_bundle
        .history
        .iter()
        .any(|record| record.row_id == "child" && record.op == 3));

    peer.apply_bundle(&delete_bundle).unwrap();
    let ids = peer
        .read_recursive_refs("folders", "root", "parent")
        .unwrap()
        .iter()
        .map(|row| row.id.clone())
        .collect::<Vec<_>>();
    assert_eq!(ids, vec!["root"]);
}

#[test]
fn recursive_query_scope_sync_exports_deleted_descendant_subtree_tombstones() {
    let schema = support::folders_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut peer =
        Runtime::open_with_schema(Storage::Memory, "alice-peer-node", "alice", schema).unwrap();

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
    alice
        .insert_row(
            "folders",
            "child",
            BTreeMap::from([
                ("name".to_owned(), json!("Child")),
                ("parent".to_owned(), json!("root")),
            ]),
        )
        .unwrap();
    alice
        .insert_row(
            "folders",
            "grandchild",
            BTreeMap::from([
                ("name".to_owned(), json!("Grandchild")),
                ("parent".to_owned(), json!("child")),
            ]),
        )
        .unwrap();

    peer.apply_bundle(
        &alice
            .export_recursive_refs("folders", "root", "parent")
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        peer.read_recursive_refs("folders", "root", "parent")
            .unwrap()
            .len(),
        3
    );

    alice.delete_row("folders", "grandchild").unwrap();
    alice.delete_row("folders", "child").unwrap();
    let delete_bundle = alice
        .export_recursive_refs("folders", "root", "parent")
        .unwrap();
    assert!(delete_bundle
        .history
        .iter()
        .any(|record| record.row_id == "child" && record.op == 3));
    assert!(delete_bundle
        .history
        .iter()
        .any(|record| record.row_id == "grandchild" && record.op == 3));

    peer.apply_bundle(&delete_bundle).unwrap();
    let ids = peer
        .read_recursive_refs("folders", "root", "parent")
        .unwrap()
        .iter()
        .map(|row| row.id.clone())
        .collect::<Vec<_>>();
    assert_eq!(ids, vec!["root"]);
}

#[test]
fn recursive_query_scope_sync_repairs_reparented_descendant_subtree() {
    let schema = support::folders_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut peer =
        Runtime::open_with_schema(Storage::Memory, "alice-peer-node", "alice", schema).unwrap();

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
    alice
        .insert_row(
            "folders",
            "other-root",
            BTreeMap::from([
                ("name".to_owned(), json!("Other root")),
                ("parent".to_owned(), json!("other-root")),
            ]),
        )
        .unwrap();
    alice
        .insert_row(
            "folders",
            "child",
            BTreeMap::from([
                ("name".to_owned(), json!("Child")),
                ("parent".to_owned(), json!("root")),
            ]),
        )
        .unwrap();
    alice
        .insert_row(
            "folders",
            "grandchild",
            BTreeMap::from([
                ("name".to_owned(), json!("Grandchild")),
                ("parent".to_owned(), json!("child")),
            ]),
        )
        .unwrap();

    peer.apply_bundle(
        &alice
            .export_recursive_refs("folders", "root", "parent")
            .unwrap(),
    )
    .unwrap();
    let ids = peer
        .read_recursive_refs("folders", "root", "parent")
        .unwrap()
        .into_iter()
        .map(|row| row.id)
        .collect::<Vec<_>>();
    assert_eq!(ids, vec!["root", "child", "grandchild"]);

    alice
        .update_row(
            "folders",
            "child",
            BTreeMap::from([("parent".to_owned(), json!("other-root"))]),
        )
        .unwrap();
    peer.apply_bundle(
        &alice
            .export_recursive_refs("folders", "root", "parent")
            .unwrap(),
    )
    .unwrap();

    let ids = peer
        .read_recursive_refs("folders", "root", "parent")
        .unwrap()
        .into_iter()
        .map(|row| row.id)
        .collect::<Vec<_>>();
    assert_eq!(ids, vec!["root"]);
}

#[test]
fn recursive_query_scope_sync_includes_recursive_policy_ancestors() {
    let schema = SchemaDef::new()
        .table("orgs", |table| {
            table.text("name");
            table.read_if_created_by_user();
        })
        .table("folders", |table| {
            table.text("name");
            table.ref_("parent", "folders");
            table.ref_("org", "orgs");
            table.read_if_ref_readable("org");
        });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut peer =
        Runtime::open_with_schema(Storage::Memory, "alice-peer-node", "alice", schema).unwrap();

    alice
        .insert_row(
            "orgs",
            "org-alice",
            BTreeMap::from([("name".to_owned(), json!("Alice org"))]),
        )
        .unwrap();
    alice
        .insert_row(
            "folders",
            "root",
            BTreeMap::from([
                ("name".to_owned(), json!("Root")),
                ("parent".to_owned(), json!("root")),
                ("org".to_owned(), json!("org-alice")),
            ]),
        )
        .unwrap();
    alice
        .insert_row(
            "folders",
            "child",
            BTreeMap::from([
                ("name".to_owned(), json!("Child")),
                ("parent".to_owned(), json!("root")),
                ("org".to_owned(), json!("org-alice")),
            ]),
        )
        .unwrap();

    let bundle = alice
        .export_recursive_refs("folders", "root", "parent")
        .unwrap();
    let synced = bundle
        .history
        .iter()
        .map(|record| (record.table.as_str(), record.row_id.as_str()))
        .collect::<Vec<_>>();
    assert!(synced.contains(&("folders", "root")));
    assert!(synced.contains(&("folders", "child")));
    assert!(synced.contains(&("orgs", "org-alice")));

    peer.apply_bundle(&bundle).unwrap();
    let ids = peer
        .read_recursive_refs("folders", "root", "parent")
        .unwrap()
        .iter()
        .map(|row| row.id.clone())
        .collect::<Vec<_>>();
    assert_eq!(ids, vec!["root", "child"]);
}

#[test]
fn recursive_query_reads_branch_base_and_sparse_overlay() {
    let schema = SchemaDef::new().table("folders", |table| {
        table.text("name");
        table.ref_("parent", "folders");
    });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema).unwrap();

    let root_tx = alice
        .insert_row(
            "folders",
            "root",
            BTreeMap::from([
                ("name".to_owned(), json!("Root")),
                ("parent".to_owned(), json!("root")),
            ]),
        )
        .unwrap();
    alice.accept_transaction_at_global(&root_tx, 1).unwrap();
    let base_child_tx = alice
        .insert_row(
            "folders",
            "base-child",
            BTreeMap::from([
                ("name".to_owned(), json!("Base child")),
                ("parent".to_owned(), json!("root")),
            ]),
        )
        .unwrap();
    alice
        .accept_transaction_at_global(&base_child_tx, 2)
        .unwrap();
    alice.create_branch("draft", Some(2)).unwrap();
    alice.checkout_branch("draft").unwrap();
    alice
        .insert_row(
            "folders",
            "draft-child",
            BTreeMap::from([
                ("name".to_owned(), json!("Draft child")),
                ("parent".to_owned(), json!("root")),
            ]),
        )
        .unwrap();

    let ids = alice
        .read_recursive_refs("folders", "root", "parent")
        .unwrap()
        .iter()
        .map(|row| row.id.clone())
        .collect::<Vec<_>>();
    assert_eq!(ids, vec!["root", "base-child", "draft-child"]);
}

#[test]
fn recursive_query_scope_sync_preserves_branch_base_and_overlay() {
    let schema = SchemaDef::new().table("folders", |table| {
        table.text("name");
        table.ref_("parent", "folders");
    });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut bob = Runtime::open_with_schema(Storage::Memory, "bob-node", "bob", schema).unwrap();

    let root_tx = alice
        .insert_row(
            "folders",
            "root",
            BTreeMap::from([
                ("name".to_owned(), json!("Root")),
                ("parent".to_owned(), json!("root")),
            ]),
        )
        .unwrap();
    alice.accept_transaction_at_global(&root_tx, 1).unwrap();
    let base_child_tx = alice
        .insert_row(
            "folders",
            "base-child",
            BTreeMap::from([
                ("name".to_owned(), json!("Base child")),
                ("parent".to_owned(), json!("root")),
            ]),
        )
        .unwrap();
    alice
        .accept_transaction_at_global(&base_child_tx, 2)
        .unwrap();
    alice.create_branch("draft", Some(2)).unwrap();
    alice.checkout_branch("draft").unwrap();
    alice
        .insert_row(
            "folders",
            "draft-child",
            BTreeMap::from([
                ("name".to_owned(), json!("Draft child")),
                ("parent".to_owned(), json!("root")),
            ]),
        )
        .unwrap();

    let bundle = alice
        .export_recursive_refs("folders", "root", "parent")
        .unwrap();
    bob.apply_bundle(&bundle).unwrap();
    bob.checkout_branch("draft").unwrap();
    let ids = bob
        .read_recursive_refs("folders", "root", "parent")
        .unwrap()
        .iter()
        .map(|row| row.id.clone())
        .collect::<Vec<_>>();
    assert_eq!(ids, vec!["root", "base-child", "draft-child"]);
}

#[test]
fn recursive_branch_history_delta_with_compaction_omits_future_main_blocks() {
    let schema = SchemaDef::new().table("folders", |table| {
        table.text("name");
        table.ref_("parent", "folders");
    });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut bob = Runtime::open_with_schema(Storage::Memory, "bob-node", "bob", schema).unwrap();

    let root_tx = alice
        .insert_row(
            "folders",
            "root",
            BTreeMap::from([
                ("name".to_owned(), json!("Root")),
                ("parent".to_owned(), json!("root")),
            ]),
        )
        .unwrap();
    alice.accept_transaction_at_global(&root_tx, 1).unwrap();
    let child_tx = alice
        .insert_row(
            "folders",
            "child",
            BTreeMap::from([
                ("name".to_owned(), json!("Base child")),
                ("parent".to_owned(), json!("root")),
            ]),
        )
        .unwrap();
    alice.accept_transaction_at_global(&child_tx, 2).unwrap();
    alice.create_branch("draft", Some(2)).unwrap();

    let update_tx = alice
        .update_row(
            "folders",
            "child",
            BTreeMap::from([("name".to_owned(), json!("Main after branch"))]),
        )
        .unwrap();
    alice.accept_transaction_at_global(&update_tx, 3).unwrap();
    alice
        .compact_accepted_history("folders", "child", 0)
        .unwrap();

    alice.checkout_branch("draft").unwrap();
    let delta = alice
        .export_recursive_refs_history_delta("folders", "root", "parent", &[])
        .unwrap();
    assert!(delta.blocks.is_empty());
    assert!(delta
        .bundle
        .history
        .iter()
        .any(|record| record.row_id == "child" && record.values["name"] == json!("Base child")));
    assert!(!delta.bundle.history.iter().any(|record| {
        record.row_id == "child" && record.values["name"] == json!("Main after branch")
    }));

    bob.apply_history_delta(&delta.bundle, &delta.blocks)
        .unwrap();
    bob.checkout_branch("draft").unwrap();
    let rows = bob
        .read_recursive_refs("folders", "root", "parent")
        .unwrap();
    let child = rows.iter().find(|row| row.id == "child").unwrap();
    assert_eq!(child.values["name"], json!("Base child"));
}

#[test]
fn recursive_branch_query_export_includes_tombstone_for_deleted_base_descendant() {
    let schema = SchemaDef::new().table("folders", |table| {
        table.text("name");
        table.ref_("parent", "folders");
    });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut peer =
        Runtime::open_with_schema(Storage::Memory, "alice-peer-node", "alice", schema).unwrap();

    let root_tx = alice
        .insert_row(
            "folders",
            "root",
            BTreeMap::from([
                ("name".to_owned(), json!("Root")),
                ("parent".to_owned(), json!("root")),
            ]),
        )
        .unwrap();
    alice.accept_transaction_at_global(&root_tx, 1).unwrap();
    let child_tx = alice
        .insert_row(
            "folders",
            "base-child",
            BTreeMap::from([
                ("name".to_owned(), json!("Base child")),
                ("parent".to_owned(), json!("root")),
            ]),
        )
        .unwrap();
    alice.accept_transaction_at_global(&child_tx, 2).unwrap();
    let grandchild_tx = alice
        .insert_row(
            "folders",
            "grandchild",
            BTreeMap::from([
                ("name".to_owned(), json!("Grandchild")),
                ("parent".to_owned(), json!("base-child")),
            ]),
        )
        .unwrap();
    alice
        .accept_transaction_at_global(&grandchild_tx, 3)
        .unwrap();

    alice.create_branch("draft", Some(3)).unwrap();
    alice.checkout_branch("draft").unwrap();
    peer.apply_bundle(
        &alice
            .export_recursive_refs("folders", "root", "parent")
            .unwrap(),
    )
    .unwrap();
    peer.checkout_branch("draft").unwrap();
    assert_eq!(
        peer.read_recursive_refs("folders", "root", "parent")
            .unwrap()
            .len(),
        3
    );

    alice.delete_row("folders", "base-child").unwrap();
    let bundle = alice
        .export_recursive_refs("folders", "root", "parent")
        .unwrap();
    assert!(bundle
        .history
        .iter()
        .any(|record| record.branch_id == "draft"
            && record.row_id == "base-child"
            && record.op == 3));

    peer.apply_bundle(&bundle).unwrap();
    let ids = peer
        .read_recursive_refs("folders", "root", "parent")
        .unwrap()
        .iter()
        .map(|row| row.id.clone())
        .collect::<Vec<_>>();
    assert_eq!(ids, vec!["root"]);
}

#[test]
fn recursive_branch_query_export_includes_snapshot_policy_ancestors() {
    let schema = SchemaDef::new()
        .table("orgs", |table| {
            table.text("name");
            table.read_if_created_by_user();
        })
        .table("folders", |table| {
            table.text("name");
            table.ref_("parent", "folders");
            table.ref_("org", "orgs");
            table.read_if_ref_readable("org");
        });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut peer =
        Runtime::open_with_schema(Storage::Memory, "alice-peer-node", "alice", schema).unwrap();

    let org_tx = alice
        .insert_row(
            "orgs",
            "org-alice",
            BTreeMap::from([("name".to_owned(), json!("Alice org"))]),
        )
        .unwrap();
    alice.accept_transaction_at_global(&org_tx, 1).unwrap();
    let root_tx = alice
        .insert_row(
            "folders",
            "root",
            BTreeMap::from([
                ("name".to_owned(), json!("Root")),
                ("parent".to_owned(), json!("root")),
                ("org".to_owned(), json!("org-alice")),
            ]),
        )
        .unwrap();
    alice.accept_transaction_at_global(&root_tx, 2).unwrap();
    let child_tx = alice
        .insert_row(
            "folders",
            "base-child",
            BTreeMap::from([
                ("name".to_owned(), json!("Base child")),
                ("parent".to_owned(), json!("root")),
                ("org".to_owned(), json!("org-alice")),
            ]),
        )
        .unwrap();
    alice.accept_transaction_at_global(&child_tx, 3).unwrap();

    alice.create_branch("draft", Some(3)).unwrap();
    alice.checkout_branch("draft").unwrap();

    let bundle = alice
        .export_recursive_refs("folders", "root", "parent")
        .unwrap();
    let synced = bundle
        .history
        .iter()
        .map(|record| (record.table.as_str(), record.row_id.as_str()))
        .collect::<Vec<_>>();
    assert!(synced.contains(&("folders", "root")));
    assert!(synced.contains(&("folders", "base-child")));
    assert!(synced.contains(&("orgs", "org-alice")));

    peer.apply_bundle(&bundle).unwrap();
    peer.checkout_branch("draft").unwrap();
    let ids = peer
        .read_recursive_refs("folders", "root", "parent")
        .unwrap()
        .iter()
        .map(|row| row.id.clone())
        .collect::<Vec<_>>();
    assert_eq!(ids, vec!["root", "base-child"]);
}
