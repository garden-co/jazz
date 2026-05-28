use super::*;

#[test]
fn rename_lens_reads_old_storage_column_as_new_field_name() {
    let old_schema = SchemaDef::new().table("tasks", |table| {
        table.text("title");
        table.bool("done");
    });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", old_schema).unwrap();
    let mut old_task = BTreeMap::new();
    old_task.insert("title".to_owned(), json!("Old title"));
    old_task.insert("done".to_owned(), json!(false));
    alice.insert_row("tasks", "task-1", old_task).unwrap();

    let new_schema = SchemaDef::new().table("tasks", |table| {
        table.text_lens("name", "title");
        table.bool("done");
    });
    let bundle = alice.export_table_history("tasks").unwrap();
    let mut bob =
        Runtime::open_with_schema(Storage::Memory, "bob-node", "bob", new_schema).unwrap();
    bob.apply_bundle(&bundle).unwrap();

    let rows = bob.read_rows("tasks").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values["name"], json!("Old title"));
    assert!(!rows[0].values.contains_key("title"));
}

#[test]
fn missing_rename_lens_fails_closed_without_partial_apply() {
    let renamed_schema = SchemaDef::new().table("tasks", |table| {
        table.text_lens("name", "title");
        table.bool("done");
    });
    let unrelated_schema = SchemaDef::new().table("tasks", |table| {
        table.text("name");
        table.bool("done");
    });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", renamed_schema).unwrap();
    let mut bob =
        Runtime::open_with_schema(Storage::Memory, "bob-node", "bob", unrelated_schema).unwrap();

    alice
        .insert_row(
            "tasks",
            "task-1",
            BTreeMap::from([
                ("name".to_owned(), json!("Needs lens")),
                ("done".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    let bundle = alice.export_table_history("tasks").unwrap();

    let err = bob.apply_bundle(&bundle).unwrap_err();
    assert!(err.to_string().contains("incompatible schema fingerprint"));
    assert!(bob.read_rows("tasks").unwrap().is_empty());
    assert!(bob.transaction_info(&bundle.history[0].tx_id).is_err());
}

#[test]
fn rename_lens_writes_export_current_semantic_field_name() {
    let schema = SchemaDef::new().table("tasks", |table| {
        table.text_lens("name", "title");
        table.bool("done");
    });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut bob = Runtime::open_with_schema(Storage::Memory, "bob-node", "bob", schema).unwrap();

    let mut task = BTreeMap::new();
    task.insert("name".to_owned(), json!("New schema write"));
    task.insert("done".to_owned(), json!(false));
    alice.insert_row("tasks", "task-1", task).unwrap();

    let bundle = alice.export_table_history("tasks").unwrap();
    assert_eq!(bundle.history[0].values["name"], json!("New schema write"));
    assert!(!bundle.history[0].values.contains_key("title"));

    bob.apply_bundle(&bundle).unwrap();
    assert_eq!(
        bob.read_rows("tasks").unwrap()[0].values["name"],
        json!("New schema write")
    );
}

#[test]
fn rename_lens_updates_old_row_as_current_semantic_history() {
    let old_schema = SchemaDef::new().table("tasks", |table| {
        table.text("title");
        table.bool("done");
    });
    let new_schema = SchemaDef::new().table("tasks", |table| {
        table.text_lens("name", "title");
        table.bool("done");
    });
    let mut old_writer =
        Runtime::open_with_schema(Storage::Memory, "old-node", "alice", old_schema).unwrap();
    let mut new_writer =
        Runtime::open_with_schema(Storage::Memory, "new-node", "alice", new_schema.clone())
            .unwrap();
    let mut new_peer =
        Runtime::open_with_schema(Storage::Memory, "new-peer", "alice", new_schema).unwrap();

    old_writer
        .insert_row(
            "tasks",
            "task-1",
            BTreeMap::from([
                ("title".to_owned(), json!("Old title")),
                ("done".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    new_writer
        .apply_bundle(&old_writer.export_table_history("tasks").unwrap())
        .unwrap();

    new_writer
        .update_row(
            "tasks",
            "task-1",
            BTreeMap::from([("name".to_owned(), json!("New name"))]),
        )
        .unwrap();
    let bundle = new_writer.export_table_history("tasks").unwrap();
    let latest = bundle
        .history
        .iter()
        .find(|record| record.values.get("name") == Some(&json!("New name")))
        .unwrap();

    assert_eq!(latest.values["name"], json!("New name"));
    assert!(!latest.values.contains_key("title"));

    new_peer.apply_bundle(&bundle).unwrap();
    let rows = new_peer.read_rows("tasks").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values["name"], json!("New name"));
    assert!(!rows[0].values.contains_key("title"));
}

#[test]
fn lens_branch_base_snapshot_survives_durable_reopen() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("lens-branch.sqlite");
    let old_schema = SchemaDef::new().table("tasks", |table| {
        table.text("title");
        table.bool("done");
    });
    let new_schema = SchemaDef::new().table("tasks", |table| {
        table.text_lens("name", "title");
        table.bool("done");
    });
    let mut old_writer =
        Runtime::open_with_schema(Storage::Memory, "old-node", "alice", old_schema).unwrap();

    let base_tx = old_writer
        .insert_row(
            "tasks",
            "task-1",
            BTreeMap::from([
                ("title".to_owned(), json!("Base title")),
                ("done".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    old_writer
        .accept_transaction_at_global(&base_tx, 1)
        .unwrap();

    {
        let mut worker = Runtime::open_with_schema(
            Storage::File(path.clone()),
            "worker",
            "alice",
            new_schema.clone(),
        )
        .unwrap();
        worker
            .apply_bundle(&old_writer.export_table_history("tasks").unwrap())
            .unwrap();
        worker.create_branch("draft", Some(1)).unwrap();
        worker
            .update_row(
                "tasks",
                "task-1",
                BTreeMap::from([("name".to_owned(), json!("Main update"))]),
            )
            .unwrap();
        assert_eq!(
            worker.read_rows("tasks").unwrap()[0].values["name"],
            json!("Main update")
        );
    }

    let mut reopened =
        Runtime::open_with_schema(Storage::File(path), "worker", "alice", new_schema).unwrap();
    reopened.checkout_branch("draft").unwrap();
    let rows = reopened.read_rows("tasks").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values["name"], json!("Base title"));
    assert!(!rows[0].values.contains_key("title"));
}

#[test]
fn renamed_field_lens_query_scope_syncs_and_repairs_rows() {
    let old_schema = SchemaDef::new().table("tasks", |table| {
        table.text("title");
        table.bool("done");
    });
    let new_schema = SchemaDef::new().table("tasks", |table| {
        table.text_lens("name", "title");
        table.bool("done");
    });
    let mut old_writer =
        Runtime::open_with_schema(Storage::Memory, "old-node", "alice", old_schema).unwrap();
    let mut new_writer =
        Runtime::open_with_schema(Storage::Memory, "new-node", "alice", new_schema.clone())
            .unwrap();
    let mut peer =
        Runtime::open_with_schema(Storage::Memory, "peer-node", "alice", new_schema).unwrap();

    old_writer
        .insert_row(
            "tasks",
            "task-1",
            BTreeMap::from([
                ("title".to_owned(), json!("Important")),
                ("done".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    old_writer
        .insert_row(
            "tasks",
            "task-2",
            BTreeMap::from([
                ("title".to_owned(), json!("Other")),
                ("done".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    new_writer
        .apply_bundle(&old_writer.export_table_history("tasks").unwrap())
        .unwrap();

    peer.apply_bundle(
        &new_writer
            .export_query_where_eq("tasks", "name", json!("Important"))
            .unwrap(),
    )
    .unwrap();
    let rows = peer
        .query(support::eq_query("tasks", "name", json!("Important")))
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, "task-1");
    assert_eq!(rows[0].values["name"], json!("Important"));
    assert!(!rows[0].values.contains_key("title"));

    new_writer
        .update_row(
            "tasks",
            "task-1",
            BTreeMap::from([("name".to_owned(), json!("Renamed"))]),
        )
        .unwrap();
    peer.apply_bundle(
        &new_writer
            .export_query_where_eq("tasks", "name", json!("Important"))
            .unwrap(),
    )
    .unwrap();

    assert!(peer
        .query(support::eq_query("tasks", "name", json!("Important")))
        .unwrap()
        .is_empty());
}

#[test]
fn renamed_field_lens_observed_query_refresh_emits_semantic_removal() {
    let old_schema = SchemaDef::new().table("tasks", |table| {
        table.text("title");
        table.bool("done");
    });
    let new_schema = SchemaDef::new().table("tasks", |table| {
        table.text_lens("name", "title");
        table.bool("done");
    });
    let mut old_writer =
        Runtime::open_with_schema(Storage::Memory, "old-node", "alice", old_schema).unwrap();
    let mut new_writer =
        Runtime::open_with_schema(Storage::Memory, "new-node", "alice", new_schema.clone())
            .unwrap();
    let mut peer =
        Runtime::open_with_schema(Storage::Memory, "peer-node", "alice", new_schema).unwrap();

    old_writer
        .insert_row(
            "tasks",
            "task-1",
            BTreeMap::from([
                ("title".to_owned(), json!("Important")),
                ("done".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    new_writer
        .apply_bundle(&old_writer.export_table_history("tasks").unwrap())
        .unwrap();

    peer.apply_bundle(
        &new_writer
            .export_query_where_eq("tasks", "name", json!("Important"))
            .unwrap(),
    )
    .unwrap();
    let observed = peer.observed_query_reads().unwrap();
    assert_eq!(observed[0].field, "name");
    let mut subscription = peer.subscribe_observed_query(&observed[0]).unwrap();
    assert_eq!(
        subscription.initial_rows()[0].values["name"],
        json!("Important")
    );

    new_writer
        .update_row(
            "tasks",
            "task-1",
            BTreeMap::from([("name".to_owned(), json!("Renamed"))]),
        )
        .unwrap();
    for refresh in new_writer.export_query_read_refreshes(&observed).unwrap() {
        peer.apply_bundle(&refresh).unwrap();
    }

    let diffs = peer.poll_subscription(&mut subscription).unwrap();
    assert!(matches!(&diffs[..], [RowDiff::Removed(row)] if row.id == "task-1"));
    assert!(peer
        .query(support::eq_query("tasks", "name", json!("Important")))
        .unwrap()
        .is_empty());
}

#[test]
fn renamed_ref_lens_participates_in_read_policy() {
    let old_schema = SchemaDef::new()
        .table("projects", |table| {
            table.text("title");
            table.read_if_created_by_user();
        })
        .table("todos", |table| {
            table.text("title");
            table.ref_("project", "projects");
        });
    let new_schema = SchemaDef::new()
        .table("projects", |table| {
            table.text("title");
            table.read_if_created_by_user();
        })
        .table("todos", |table| {
            table.text("title");
            table.ref_lens("workspace", "project", "projects");
            table.read_if_ref_readable("workspace");
        });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", old_schema.clone())
            .unwrap();
    let mut bob =
        Runtime::open_with_schema(Storage::Memory, "bob-node", "bob", old_schema).unwrap();
    let mut reader =
        Runtime::open_with_schema(Storage::Memory, "alice-reader", "alice", new_schema).unwrap();

    alice
        .insert_row(
            "projects",
            "project-alice",
            BTreeMap::from([("title".to_owned(), json!("Alice project"))]),
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
        "projects",
        "project-bob",
        BTreeMap::from([("title".to_owned(), json!("Bob project"))]),
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

    reader
        .apply_bundle(&alice.export_table_history("projects").unwrap())
        .unwrap();
    reader
        .apply_bundle(&alice.export_table_history("todos").unwrap())
        .unwrap();
    reader
        .apply_bundle(&bob.export_table_history("projects").unwrap())
        .unwrap();
    reader
        .apply_bundle(&bob.export_table_history("todos").unwrap())
        .unwrap();

    let rows = reader.read_rows("todos").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, "todo-visible");
    assert_eq!(rows[0].values["workspace"], json!("project-alice"));
    assert!(!rows[0].values.contains_key("project"));
}

#[test]
fn renamed_ref_lens_participates_in_untrusted_write_policy_validation() {
    let old_schema = SchemaDef::new()
        .table("projects", |table| {
            table.text("title");
            table.read_if_created_by_user();
        })
        .table("todos", |table| {
            table.text("title");
            table.ref_("project", "projects");
        });
    let new_schema = SchemaDef::new()
        .table("projects", |table| {
            table.text("title");
            table.read_if_created_by_user();
        })
        .table("todos", |table| {
            table.text("title");
            table.ref_lens("workspace", "project", "projects");
            table.write_if_ref_readable("workspace");
        });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", old_schema.clone())
            .unwrap();
    let mut bob =
        Runtime::open_trusted_with_session_user(Storage::Memory, "bob-node", "bob", old_schema)
            .unwrap();
    let mut edge = Runtime::open_trusted_with_schema(Storage::Memory, "edge", new_schema).unwrap();

    alice
        .insert_row(
            "projects",
            "project-1",
            BTreeMap::from([("title".to_owned(), json!("Alice project"))]),
        )
        .unwrap();
    let project_bundle = alice.export_table_history("projects").unwrap();
    bob.apply_bundle(&project_bundle).unwrap();
    edge.apply_bundle(&project_bundle).unwrap();

    let tx = bob
        .insert_row(
            "todos",
            "todo-1",
            BTreeMap::from([
                ("title".to_owned(), json!("Should be rejected through lens")),
                ("project".to_owned(), json!("project-1")),
            ]),
        )
        .unwrap();

    edge.apply_untrusted_bundle_as_user(
        &bob.export_table_history("todos").unwrap(),
        bob.session_user(),
    )
    .unwrap();

    assert!(edge.read_rows("todos").unwrap().is_empty());
    assert_eq!(
        edge.transaction_info(&tx).unwrap().rejection_code,
        Some("policy_denied".to_owned())
    );
}

#[test]
fn lens_compatible_write_policy_allows_ordinary_peer_sync() {
    let old_schema = SchemaDef::new()
        .table("projects", |table| {
            table.text("title");
            table.read_if_created_by_user();
        })
        .table("todos", |table| {
            table.text("title");
            table.ref_("project", "projects");
            table.write_if_ref_readable("project");
        });
    let new_schema = SchemaDef::new()
        .table("projects", |table| {
            table.text("title");
            table.read_if_created_by_user();
        })
        .table("todos", |table| {
            table.text("title");
            table.ref_lens("workspace", "project", "projects");
            table.write_if_ref_readable("workspace");
        });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", old_schema).unwrap();
    let mut peer =
        Runtime::open_with_schema(Storage::Memory, "alice-peer", "alice", new_schema).unwrap();

    alice
        .insert_row(
            "projects",
            "project-1",
            BTreeMap::from([("title".to_owned(), json!("Alice project"))]),
        )
        .unwrap();
    alice
        .insert_row(
            "todos",
            "todo-1",
            BTreeMap::from([
                ("title".to_owned(), json!("Lens-compatible write policy")),
                ("project".to_owned(), json!("project-1")),
            ]),
        )
        .unwrap();

    peer.apply_bundle(&alice.export_table_history("projects").unwrap())
        .unwrap();
    peer.apply_bundle(&alice.export_table_history("todos").unwrap())
        .unwrap();

    let rows = peer.read_rows("todos").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values["workspace"], json!("project-1"));
    assert!(!rows[0].values.contains_key("project"));
}

#[test]
fn renamed_ref_lens_exports_transitive_write_policy_dependencies_for_untrusted_edge() {
    let old_schema = SchemaDef::new()
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
            table.write_if_ref_readable("project");
        });
    let new_schema = SchemaDef::new()
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
            table.ref_lens("workspace", "project", "projects");
            table.write_if_ref_readable("workspace");
        });
    let mut alice =
        Runtime::open_trusted_with_session_user(Storage::Memory, "alice-node", "alice", old_schema)
            .unwrap();
    let mut edge = Runtime::open_trusted_with_schema(Storage::Memory, "edge", new_schema).unwrap();

    alice
        .insert_row(
            "orgs",
            "org-1",
            BTreeMap::from([("name".to_owned(), json!("Alice org"))]),
        )
        .unwrap();
    alice
        .insert_row(
            "projects",
            "project-1",
            BTreeMap::from([
                ("title".to_owned(), json!("Alice project")),
                ("org".to_owned(), json!("org-1")),
            ]),
        )
        .unwrap();
    let tx = alice
        .insert_row(
            "todos",
            "todo-1",
            BTreeMap::from([
                ("title".to_owned(), json!("Lens recursive dependency")),
                ("project".to_owned(), json!("project-1")),
            ]),
        )
        .unwrap();

    let bundle = alice.export_table_history("todos").unwrap();
    let exported = bundle
        .history
        .iter()
        .map(|record| (record.table.as_str(), record.row_id.as_str()))
        .collect::<Vec<_>>();
    assert!(exported.contains(&("todos", "todo-1")));
    assert!(exported.contains(&("projects", "project-1")));
    assert!(exported.contains(&("orgs", "org-1")));

    edge.apply_untrusted_bundle_as_user(&bundle, alice.session_user())
        .unwrap();

    assert_eq!(edge.transaction_info(&tx).unwrap().rejection_code, None);
    let rows = edge.read_rows("todos").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values["workspace"], json!("project-1"));
    assert!(!rows[0].values.contains_key("project"));
}

#[test]
fn user_columns_with_system_prefix_are_escaped_physically() {
    let schema = SchemaDef::new().table("records", |table| {
        table.text("j_title");
        table.bool("j_pinned");
        table.index("by_j_title", ["j_title"]);
    });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut peer =
        Runtime::open_with_schema(Storage::Memory, "alice-peer-node", "alice", schema).unwrap();

    alice
        .insert_row(
            "records",
            "record-1",
            BTreeMap::from([
                ("j_title".to_owned(), json!("Looks like system")),
                ("j_pinned".to_owned(), json!(true)),
            ]),
        )
        .unwrap();

    let rows = alice
        .query(support::eq_query(
            "records",
            "j_title",
            json!("Looks like system"),
        ))
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values["j_pinned"], json!(true));

    peer.apply_bundle(&alice.export_table_history("records").unwrap())
        .unwrap();
    let peer_rows = peer.read_rows("records").unwrap();
    assert_eq!(peer_rows.len(), 1);
    assert_eq!(peer_rows[0].values["j_title"], json!("Looks like system"));
}

#[test]
fn schema_validation_rejects_ambiguous_fields_and_indexes() {
    let duplicate_semantic = SchemaDef::new().table("records", |table| {
        table.text("title");
        table.bool("title");
    });
    let err = match Runtime::open_with_schema(Storage::Memory, "node", "alice", duplicate_semantic)
    {
        Ok(_) => panic!("duplicate semantic field opened successfully"),
        Err(err) => err,
    };
    assert!(err.to_string().contains("duplicate field records.title"));

    let duplicate_storage = SchemaDef::new().table("records", |table| {
        table.text("title");
        table.text_lens("name", "title");
    });
    let err = match Runtime::open_with_schema(Storage::Memory, "node", "alice", duplicate_storage) {
        Ok(_) => panic!("duplicate storage field opened successfully"),
        Err(err) => err,
    };
    assert!(err
        .to_string()
        .contains("duplicate storage field records.title"));

    let unknown_index_field = SchemaDef::new().table("records", |table| {
        table.text("title");
        table.index("bad", ["missing"]);
    });
    let err = match Runtime::open_with_schema(Storage::Memory, "node", "alice", unknown_index_field)
    {
        Ok(_) => panic!("unknown index field opened successfully"),
        Err(err) => err,
    };
    assert!(err
        .to_string()
        .contains("index records.bad references unknown field missing"));
}

#[test]
fn index_only_schema_changes_are_semantically_compatible() {
    let unindexed = SchemaDef::new().table("tasks", |table| {
        table.text("title");
        table.bool("done");
    });
    let indexed = SchemaDef::new().table("tasks", |table| {
        table.text("title");
        table.bool("done");
        table.index("done_created", ["done", "$createdAt"]);
    });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", unindexed).unwrap();
    let mut peer =
        Runtime::open_with_schema(Storage::Memory, "alice-peer-node", "alice", indexed).unwrap();

    alice
        .insert_row(
            "tasks",
            "task-1",
            BTreeMap::from([
                ("title".to_owned(), json!("Compatible")),
                ("done".to_owned(), json!(false)),
            ]),
        )
        .unwrap();

    peer.apply_bundle(&alice.export_table_history("tasks").unwrap())
        .unwrap();

    let rows = peer
        .query(support::eq_query("tasks", "done", json!(false)))
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values["title"], json!("Compatible"));

    peer.apply_bundle(
        &alice
            .export_query(support::top_created_query("tasks", "done", json!(false), 1))
            .unwrap(),
    )
    .unwrap();
}
