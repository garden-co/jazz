use super::*;

#[test]
fn recursive_policy_filters_reads_through_grandparent_ref() {
    let schema = SchemaDef::new()
        .table("orgs", |table| {
            table.text("name");
            table.read_if_created_by_principal();
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
fn recursive_policy_scoped_sync_includes_transitive_parent_rows() {
    let schema = SchemaDef::new()
        .table("orgs", |table| {
            table.text("name");
            table.read_if_created_by_principal();
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
fn recursive_query_scope_sync_includes_recursive_policy_ancestors() {
    let schema = SchemaDef::new()
        .table("orgs", |table| {
            table.text("name");
            table.read_if_created_by_principal();
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
fn recursive_branch_query_export_includes_snapshot_policy_ancestors() {
    let schema = SchemaDef::new()
        .table("orgs", |table| {
            table.text("name");
            table.read_if_created_by_principal();
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
