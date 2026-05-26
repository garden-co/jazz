use super::*;

#[test]
fn policy_filters_reads_through_required_parent_ref() {
    let schema = SchemaDef::new()
        .table("projects", |table| {
            table.text("title");
            table.read_if_created_by_principal();
        })
        .table("todos", |table| {
            table.text("title");
            table.bool("done");
            table.ref_("project", "projects");
            table.read_if_ref_readable("project");
        });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut bob = Runtime::open_with_schema(Storage::Memory, "bob-node", "bob", schema).unwrap();

    let mut alice_project = BTreeMap::new();
    alice_project.insert("title".to_owned(), json!("Alice project"));
    alice
        .insert_row("projects", "project-alice", alice_project)
        .unwrap();
    let mut alice_todo = BTreeMap::new();
    alice_todo.insert("title".to_owned(), json!("Visible"));
    alice_todo.insert("done".to_owned(), json!(false));
    alice_todo.insert("project".to_owned(), json!("project-alice"));
    alice
        .insert_row("todos", "todo-visible", alice_todo)
        .unwrap();

    let mut bob_project = BTreeMap::new();
    bob_project.insert("title".to_owned(), json!("Bob project"));
    bob.insert_row("projects", "project-bob", bob_project)
        .unwrap();
    let mut bob_todo = BTreeMap::new();
    bob_todo.insert("title".to_owned(), json!("Hidden"));
    bob_todo.insert("done".to_owned(), json!(false));
    bob_todo.insert("project".to_owned(), json!("project-bob"));
    bob.insert_row("todos", "todo-hidden", bob_todo).unwrap();

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

    let scoped_bundle = alice.export_table_history("todos").unwrap();
    assert!(scoped_bundle
        .history
        .iter()
        .any(|record| record.table == "todos" && record.row_id == "todo-visible"));
    assert!(scoped_bundle
        .history
        .iter()
        .any(|record| record.table == "projects" && record.row_id == "project-alice"));
}

#[test]
fn required_ref_include_filters_parent_when_target_is_unauthorized() {
    let schema = SchemaDef::new()
        .table("projects", |table| {
            table.text("title");
            table.read_if_created_by_principal();
        })
        .table("todos", |table| {
            table.text("title");
            table.bool("done");
            table.ref_("project", "projects");
        });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut bob = Runtime::open_with_schema(Storage::Memory, "bob-node", "bob", schema).unwrap();

    bob.insert_row(
        "projects",
        "project-bob",
        BTreeMap::from([("title".to_owned(), json!("Bob project"))]),
    )
    .unwrap();
    bob.insert_row(
        "todos",
        "todo-hidden-include",
        BTreeMap::from([
            ("title".to_owned(), json!("Needs hidden project")),
            ("done".to_owned(), json!(false)),
            ("project".to_owned(), json!("project-bob")),
        ]),
    )
    .unwrap();
    alice
        .apply_bundle(&bob.export_table_history("projects").unwrap())
        .unwrap();
    alice
        .apply_bundle(&bob.export_table_history("todos").unwrap())
        .unwrap();

    let optional = alice.open_todos().unwrap();
    assert_eq!(optional.len(), 1);
    assert_eq!(optional[0].project_title, None);
    assert!(alice.open_todos_require_project().unwrap().is_empty());
}

#[test]
fn schema_rejects_ref_policy_that_does_not_point_at_ref_field() {
    let missing_ref = SchemaDef::new().table("todos", |table| {
        table.text("title");
        table.read_if_ref_readable("project");
    });
    let err = match Runtime::open_with_schema(Storage::Memory, "node", "alice", missing_ref) {
        Ok(_) => panic!("policy with missing ref field opened successfully"),
        Err(err) => err,
    };
    assert!(err
        .to_string()
        .contains("policy on todos references unknown field project"));

    let scalar_ref = SchemaDef::new().table("todos", |table| {
        table.text("title");
        table.read_if_ref_readable("title");
    });
    let err = match Runtime::open_with_schema(Storage::Memory, "node", "alice", scalar_ref) {
        Ok(_) => panic!("policy with scalar ref field opened successfully"),
        Err(err) => err,
    };
    assert!(err
        .to_string()
        .contains("policy on todos references non-ref field title"));
}

#[test]
fn untrusted_acceptance_uses_authority_policy_not_sender_policy_fingerprint() {
    let writer_schema = SchemaDef::new().table("notes", |table| {
        table.text("body");
        table.bool("pinned");
    });
    let edge_schema = SchemaDef::new().table("notes", |table| {
        table.text("body");
        table.bool("pinned");
        table.write_if_created_by_principal();
    });
    let mut writer =
        Runtime::open_with_schema(Storage::Memory, "writer", "alice", writer_schema).unwrap();
    let mut edge = Runtime::open_trusted_with_schema(Storage::Memory, "edge", edge_schema).unwrap();

    let tx = writer
        .insert_row(
            "notes",
            "note-1",
            BTreeMap::from([
                ("body".to_owned(), json!("policy mismatch")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    let bundle = writer.export_table_history("notes").unwrap();
    assert_ne!(bundle.policy_fingerprint, edge.local_policy_fingerprint());

    edge.apply_bundle(&bundle).unwrap();
    assert_eq!(edge.read_rows("notes").unwrap().len(), 1);

    let mut rejecting_edge = Runtime::open_trusted_with_schema(
        Storage::Memory,
        "rejecting-edge",
        SchemaDef::new().table("notes", |table| {
            table.text("body");
            table.bool("pinned");
            table.write_if_created_by_principal();
        }),
    )
    .unwrap();
    rejecting_edge.apply_untrusted_bundle(&bundle).unwrap();
    assert_eq!(rejecting_edge.read_rows("notes").unwrap().len(), 1);
    assert_eq!(
        rejecting_edge.transaction_info(&tx).unwrap().rejection_code,
        None
    );
}

#[test]
fn policy_scoped_sync_includes_required_parent_rows_only() {
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
    let mut bob =
        Runtime::open_with_schema(Storage::Memory, "alice-peer-node", "alice", schema.clone())
            .unwrap();

    let mut visible_project = BTreeMap::new();
    visible_project.insert("title".to_owned(), json!("Visible project"));
    alice
        .insert_row("projects", "project-visible", visible_project)
        .unwrap();

    let mut unrelated_project = BTreeMap::new();
    unrelated_project.insert("title".to_owned(), json!("Unrelated project"));
    alice
        .insert_row("projects", "project-unrelated", unrelated_project)
        .unwrap();

    let mut visible_todo = BTreeMap::new();
    visible_todo.insert("title".to_owned(), json!("Visible todo"));
    visible_todo.insert("project".to_owned(), json!("project-visible"));
    alice
        .insert_row("todos", "todo-visible", visible_todo)
        .unwrap();

    let bundle = alice.export_table_history("todos").unwrap();
    let synced = bundle
        .history
        .iter()
        .map(|record| (record.table.as_str(), record.row_id.as_str()))
        .collect::<Vec<_>>();
    assert!(synced.contains(&("todos", "todo-visible")));
    assert!(synced.contains(&("projects", "project-visible")));
    assert!(!synced.contains(&("projects", "project-unrelated")));

    bob.apply_bundle(&bundle).unwrap();
    let rows = bob.read_rows("todos").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values["project"], json!("project-visible"));
}

#[test]
fn trusted_peer_can_read_applied_policy_scoped_facts_without_user_principal() {
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
    let mut trusted =
        Runtime::open_trusted_with_schema(Storage::Memory, "worker-node", schema).unwrap();

    let mut project = BTreeMap::new();
    project.insert("title".to_owned(), json!("Alice project"));
    alice.insert_row("projects", "project-1", project).unwrap();
    let mut todo = BTreeMap::new();
    todo.insert("title".to_owned(), json!("Policy-scoped fact"));
    todo.insert("project".to_owned(), json!("project-1"));
    alice.insert_row("todos", "todo-1", todo).unwrap();

    trusted
        .apply_bundle(&alice.export_table_history("todos").unwrap())
        .unwrap();

    let rows = trusted.read_rows("todos").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, "todo-1");
}

#[test]
fn trusted_peer_generic_transaction_bypasses_user_write_policy() {
    let schema = SchemaDef::new()
        .table("docs", |table| {
            table.text("title");
            table.read_if_created_by_principal();
        })
        .table("comments", |table| {
            table.text("body");
            table.ref_("doc", "docs");
            table.write_if_ref_readable("doc");
        });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut trusted =
        Runtime::open_trusted_with_schema(Storage::Memory, "worker-node", schema).unwrap();

    let mut doc = BTreeMap::new();
    doc.insert("title".to_owned(), json!("Alice doc"));
    alice.insert_row("docs", "doc-1", doc).unwrap();
    trusted
        .apply_bundle(&alice.export_table_history("docs").unwrap())
        .unwrap();

    trusted
        .transaction()
        .insert_row(
            "comments",
            "comment-1",
            BTreeMap::from([
                ("body".to_owned(), json!("Trusted write")),
                ("doc".to_owned(), json!("doc-1")),
            ]),
        )
        .commit()
        .unwrap();

    assert_eq!(trusted.read_rows("comments").unwrap().len(), 1);
    assert_eq!(trusted.storage_stats().unwrap().rejected_transactions, 0);
}

#[test]
fn trusted_edge_accepts_mergeable_tx_then_untrusted_peers_enforce_policy() {
    let dir = tempdir().unwrap();
    let edge_path = dir.path().join("edge.sqlite");
    let schema = SchemaDef::new().table("notes", |table| {
        table.text("body");
        table.bool("pinned");
        table.read_if_created_by_principal();
    });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-tab", "alice", schema.clone()).unwrap();
    let mut edge =
        Runtime::open_trusted_with_schema(Storage::File(edge_path), "edge", schema.clone())
            .unwrap();
    let mut alice_phone =
        Runtime::open_with_schema(Storage::Memory, "alice-phone", "alice", schema.clone()).unwrap();
    let mut bob = Runtime::open_with_schema(Storage::Memory, "bob-tab", "bob", schema).unwrap();

    let tx = alice
        .insert_row(
            "notes",
            "note-1",
            BTreeMap::from([
                ("body".to_owned(), json!("Accepted at edge")),
                ("pinned".to_owned(), json!(true)),
            ]),
        )
        .unwrap();
    edge.apply_bundle(&alice.export_table_history("notes").unwrap())
        .unwrap();
    edge.accept_transaction_at_global(&tx, 11).unwrap();

    let accepted_bundle = edge.export_table_history("notes").unwrap();
    alice_phone.apply_bundle(&accepted_bundle).unwrap();
    bob.apply_bundle(&accepted_bundle).unwrap();

    assert_eq!(alice_phone.read_rows("notes").unwrap().len(), 1);
    assert_eq!(
        alice_phone.transaction_info(&tx).unwrap().global_epoch,
        Some(11)
    );
    assert!(bob.read_rows("notes").unwrap().is_empty());
}

#[test]
fn trusted_edge_acceptance_syncs_without_global_epoch() {
    let dir = tempdir().unwrap();
    let edge_path = dir.path().join("edge.sqlite");
    let schema = support::notes_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-tab", "alice", schema.clone()).unwrap();
    let mut edge =
        Runtime::open_trusted_with_schema(Storage::File(edge_path), "edge", schema.clone())
            .unwrap();
    let mut phone =
        Runtime::open_with_schema(Storage::Memory, "alice-phone", "alice", schema).unwrap();

    let tx = alice
        .insert_row(
            "notes",
            "note-1",
            BTreeMap::from([
                ("body".to_owned(), json!("Accepted at edge")),
                ("pinned".to_owned(), json!(true)),
            ]),
        )
        .unwrap();
    edge.apply_bundle(&alice.export_table_history("notes").unwrap())
        .unwrap();
    edge.accept_transaction_at_edge(&tx).unwrap();

    phone
        .apply_bundle(&edge.export_table_history("notes").unwrap())
        .unwrap();

    let info = phone.transaction_info(&tx).unwrap();
    assert_eq!(info.global_epoch, None);
    assert!(info.receipt_tiers.contains(&"edge".to_owned()));
    assert_eq!(phone.read_rows("notes").unwrap().len(), 1);
}

#[test]
fn edge_accepted_transaction_can_upgrade_to_global_epoch() {
    let schema = support::notes_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-tab", "alice", schema.clone()).unwrap();
    let mut edge =
        Runtime::open_trusted_with_schema(Storage::Memory, "edge", schema.clone()).unwrap();
    let mut phone =
        Runtime::open_with_schema(Storage::Memory, "alice-phone", "alice", schema).unwrap();

    let tx = alice
        .insert_row(
            "notes",
            "note-1",
            BTreeMap::from([
                ("body".to_owned(), json!("Edge then global")),
                ("pinned".to_owned(), json!(true)),
            ]),
        )
        .unwrap();
    edge.apply_bundle(&alice.export_table_history("notes").unwrap())
        .unwrap();
    edge.accept_transaction_at_edge(&tx).unwrap();
    phone
        .apply_bundle(&edge.export_table_history("notes").unwrap())
        .unwrap();
    assert_eq!(phone.transaction_info(&tx).unwrap().global_epoch, None);

    edge.accept_transaction_at_global(&tx, 42).unwrap();
    phone
        .apply_bundle(&edge.export_table_history("notes").unwrap())
        .unwrap();

    let info = phone.transaction_info(&tx).unwrap();
    assert_eq!(info.global_epoch, Some(42));
    assert!(info.receipt_tiers.contains(&"edge".to_owned()));
    assert!(info.receipt_tiers.contains(&"global".to_owned()));
}

#[test]
fn edge_core_peer_edge_round_trip_preserves_policy_and_receipts() {
    let schema = SchemaDef::new().table("notes", |table| {
        table.text("body");
        table.bool("pinned");
        table.read_if_created_by_principal();
    });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-tab", "alice", schema.clone()).unwrap();
    let mut edge_a =
        Runtime::open_trusted_with_schema(Storage::Memory, "edge-a", schema.clone()).unwrap();
    let mut core =
        Runtime::open_trusted_with_schema(Storage::Memory, "core", schema.clone()).unwrap();
    let mut edge_b =
        Runtime::open_trusted_with_schema(Storage::Memory, "edge-b", schema.clone()).unwrap();
    let mut alice_laptop =
        Runtime::open_with_schema(Storage::Memory, "alice-laptop", "alice", schema.clone())
            .unwrap();
    let mut bob = Runtime::open_with_schema(Storage::Memory, "bob-tab", "bob", schema).unwrap();

    let tx = alice
        .insert_row(
            "notes",
            "note-1",
            BTreeMap::from([
                ("body".to_owned(), json!("Through edge and core")),
                ("pinned".to_owned(), json!(true)),
            ]),
        )
        .unwrap();

    edge_a
        .apply_untrusted_bundle(&alice.export_table_history("notes").unwrap())
        .unwrap();
    edge_a.accept_transaction_at_edge(&tx).unwrap();
    core.apply_bundle(&edge_a.export_table_history("notes").unwrap())
        .unwrap();
    core.accept_transaction_at_global(&tx, 99).unwrap();
    edge_b
        .apply_bundle(&core.export_table_history("notes").unwrap())
        .unwrap();

    let global_bundle = edge_b.export_table_history("notes").unwrap();
    alice_laptop.apply_bundle(&global_bundle).unwrap();
    bob.apply_bundle(&global_bundle).unwrap();

    let alice_rows = alice_laptop.read_rows("notes").unwrap();
    assert_eq!(alice_rows.len(), 1);
    assert_eq!(alice_rows[0].id, "note-1");
    assert!(bob.read_rows("notes").unwrap().is_empty());

    let info = alice_laptop.transaction_info(&tx).unwrap();
    assert_eq!(info.global_epoch, Some(99));
    assert!(info.receipt_tiers.contains(&"edge".to_owned()));
    assert!(info.receipt_tiers.contains(&"global".to_owned()));
}

#[test]
fn trusted_edge_rejects_policy_violating_tx_and_syncs_reason() {
    let schema = SchemaDef::new()
        .table("projects", |table| {
            table.text("title");
            table.read_if_created_by_principal();
        })
        .table("todos", |table| {
            table.text("title");
            table.ref_("project", "projects");
            table.write_if_ref_readable("project");
        });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut bob =
        Runtime::open_with_schema(Storage::Memory, "bob-node", "bob", schema.clone()).unwrap();
    let mut edge = Runtime::open_trusted_with_schema(Storage::Memory, "edge", schema).unwrap();

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
                ("title".to_owned(), json!("Should be rejected")),
                ("project".to_owned(), json!("project-1")),
            ]),
        )
        .unwrap();
    assert!(bob.read_rows("todos").unwrap().is_empty());

    edge.apply_bundle(&bob.export_table_history("todos").unwrap())
        .unwrap();
    edge.reject_transaction(&tx, "policy_denied").unwrap();
    bob.apply_bundle(&edge.export_table_history("todos").unwrap())
        .unwrap();

    assert!(edge.read_rows("todos").unwrap().is_empty());
    assert!(bob.read_rows("todos").unwrap().is_empty());
    assert_eq!(
        bob.transaction_info(&tx).unwrap().rejection_code,
        Some("policy_denied".to_owned())
    );
}

#[test]
fn trusted_edge_authoritatively_rejects_untrusted_policy_violation_on_apply() {
    let schema = SchemaDef::new()
        .table("projects", |table| {
            table.text("title");
            table.read_if_created_by_principal();
        })
        .table("todos", |table| {
            table.text("title");
            table.ref_("project", "projects");
            table.write_if_ref_readable("project");
        });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut bob =
        Runtime::open_trusted_as_with_schema(Storage::Memory, "bob-node", "bob", schema.clone())
            .unwrap();
    let mut edge = Runtime::open_trusted_with_schema(Storage::Memory, "edge", schema).unwrap();

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
                (
                    "title".to_owned(),
                    json!("Should be rejected automatically"),
                ),
                ("project".to_owned(), json!("project-1")),
            ]),
        )
        .unwrap();

    edge.apply_untrusted_bundle(&bob.export_table_history("todos").unwrap())
        .unwrap();

    assert!(edge.read_rows("todos").unwrap().is_empty());
    assert_eq!(
        edge.transaction_info(&tx).unwrap().rejection_code,
        Some("policy_denied".to_owned())
    );
    assert_eq!(
        edge.transaction_info(&tx).unwrap().rejection_detail,
        Some(json!({
            "reason": "write_policy_denied",
            "table": "todos",
            "row_id": "todo-1"
        }))
    );

    bob.apply_bundle(&edge.export_table_history("todos").unwrap())
        .unwrap();
    assert_eq!(
        bob.transaction_info(&tx).unwrap().rejection_detail,
        Some(json!({
            "reason": "write_policy_denied",
            "table": "todos",
            "row_id": "todo-1"
        }))
    );
}

#[test]
fn trusted_edge_rejects_untrusted_write_when_policy_dependency_is_missing() {
    let schema = SchemaDef::new()
        .table("projects", |table| {
            table.text("title");
            table.read_if_created_by_principal();
        })
        .table("todos", |table| {
            table.text("title");
            table.ref_("project", "projects");
            table.write_if_ref_readable("project");
        });
    let mut bob =
        Runtime::open_trusted_as_with_schema(Storage::Memory, "bob-node", "bob", schema.clone())
            .unwrap();
    let mut edge = Runtime::open_trusted_with_schema(Storage::Memory, "edge", schema).unwrap();

    bob.insert_row(
        "projects",
        "project-bob",
        BTreeMap::from([("title".to_owned(), json!("Bob project"))]),
    )
    .unwrap();
    let tx = bob
        .insert_row(
            "todos",
            "todo-1",
            BTreeMap::from([
                ("title".to_owned(), json!("Missing dependency")),
                ("project".to_owned(), json!("project-bob")),
            ]),
        )
        .unwrap();

    let mut incomplete_bundle = bob.export_table_history("todos").unwrap();
    incomplete_bundle
        .history
        .retain(|record| record.table != "projects");

    edge.apply_untrusted_bundle(&incomplete_bundle).unwrap();
    assert!(edge.read_rows("todos").unwrap().is_empty());
    assert_eq!(
        edge.transaction_info(&tx).unwrap().rejection_code,
        Some("policy_denied".to_owned())
    );

    edge.apply_bundle(&bob.export_table_history("projects").unwrap())
        .unwrap();
    assert!(edge.read_rows("todos").unwrap().is_empty());
}

#[test]
fn trusted_edge_accepts_untrusted_write_when_bundle_contains_policy_dependency() {
    let schema = SchemaDef::new()
        .table("projects", |table| {
            table.text("title");
            table.read_if_created_by_principal();
        })
        .table("todos", |table| {
            table.text("title");
            table.ref_("project", "projects");
            table.write_if_ref_readable("project");
        });
    let mut bob =
        Runtime::open_trusted_as_with_schema(Storage::Memory, "bob-node", "bob", schema.clone())
            .unwrap();
    let mut edge = Runtime::open_trusted_with_schema(Storage::Memory, "edge", schema).unwrap();

    bob.insert_row(
        "projects",
        "project-bob",
        BTreeMap::from([("title".to_owned(), json!("Bob project"))]),
    )
    .unwrap();
    let tx = bob
        .insert_row(
            "todos",
            "todo-1",
            BTreeMap::from([
                ("title".to_owned(), json!("Dependency included")),
                ("project".to_owned(), json!("project-bob")),
            ]),
        )
        .unwrap();

    edge.apply_untrusted_bundle(&bob.export_table_history("todos").unwrap())
        .unwrap();

    let rows = edge.read_rows("todos").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, "todo-1");
    assert_eq!(edge.transaction_info(&tx).unwrap().rejection_code, None);
}

#[test]
fn trusted_edge_rejects_untrusted_transaction_atomically() {
    let schema = SchemaDef::new()
        .table("projects", |table| {
            table.text("title");
            table.read_if_created_by_principal();
        })
        .table("todos", |table| {
            table.text("title");
            table.ref_("project", "projects");
            table.write_if_ref_readable("project");
        });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut bob =
        Runtime::open_trusted_as_with_schema(Storage::Memory, "bob-node", "bob", schema.clone())
            .unwrap();
    let mut edge = Runtime::open_trusted_with_schema(Storage::Memory, "edge", schema).unwrap();

    alice
        .insert_row(
            "projects",
            "project-alice",
            BTreeMap::from([("title".to_owned(), json!("Alice project"))]),
        )
        .unwrap();
    bob.insert_row(
        "projects",
        "project-bob",
        BTreeMap::from([("title".to_owned(), json!("Bob project"))]),
    )
    .unwrap();
    bob.apply_bundle(&alice.export_table_history("projects").unwrap())
        .unwrap();
    edge.apply_bundle(&alice.export_table_history("projects").unwrap())
        .unwrap();
    edge.apply_bundle(&bob.export_table_history("projects").unwrap())
        .unwrap();

    let tx = bob
        .transaction()
        .insert_row(
            "todos",
            "todo-allowed-sibling",
            BTreeMap::from([
                ("title".to_owned(), json!("Allowed sibling")),
                ("project".to_owned(), json!("project-bob")),
            ]),
        )
        .insert_row(
            "todos",
            "todo-denied-sibling",
            BTreeMap::from([
                ("title".to_owned(), json!("Denied sibling")),
                ("project".to_owned(), json!("project-alice")),
            ]),
        )
        .commit()
        .unwrap();

    edge.apply_untrusted_bundle(&bob.export_table_history("todos").unwrap())
        .unwrap();

    assert!(edge.read_rows("todos").unwrap().is_empty());
    assert_eq!(
        edge.transaction_info(&tx).unwrap().rejection_code,
        Some("policy_denied".to_owned())
    );
}

#[test]
fn trusted_edge_rejects_untrusted_update_to_unreadable_ref() {
    let schema = SchemaDef::new()
        .table("projects", |table| {
            table.text("title");
            table.read_if_created_by_principal();
        })
        .table("todos", |table| {
            table.text("title");
            table.ref_("project", "projects");
            table.write_if_ref_readable("project");
        });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut bob =
        Runtime::open_trusted_as_with_schema(Storage::Memory, "bob-node", "bob", schema.clone())
            .unwrap();
    let mut edge = Runtime::open_trusted_with_schema(Storage::Memory, "edge", schema).unwrap();

    alice
        .insert_row(
            "projects",
            "project-alice",
            BTreeMap::from([("title".to_owned(), json!("Alice project"))]),
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
        "todo-1",
        BTreeMap::from([
            ("title".to_owned(), json!("Initially allowed")),
            ("project".to_owned(), json!("project-bob")),
        ]),
    )
    .unwrap();
    edge.apply_bundle(&alice.export_table_history("projects").unwrap())
        .unwrap();
    edge.apply_untrusted_bundle(&bob.export_table_history("projects").unwrap())
        .unwrap();
    edge.apply_untrusted_bundle(&bob.export_table_history("todos").unwrap())
        .unwrap();
    assert_eq!(edge.read_rows("todos").unwrap().len(), 1);

    let tx = bob
        .update_row(
            "todos",
            "todo-1",
            BTreeMap::from([
                ("title".to_owned(), json!("Reparented")),
                ("project".to_owned(), json!("project-alice")),
            ]),
        )
        .unwrap();
    edge.apply_untrusted_bundle(&bob.export_table_history("todos").unwrap())
        .unwrap();

    let rows = edge.read_rows("todos").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values["project"], json!("project-bob"));
    assert_eq!(
        edge.transaction_info(&tx).unwrap().rejection_code,
        Some("policy_denied".to_owned())
    );
}

#[test]
fn branch_write_policy_does_not_use_parent_from_different_branch() {
    let schema = SchemaDef::new()
        .table("projects", |table| {
            table.text("title");
            table.read_if_created_by_principal();
        })
        .table("todos", |table| {
            table.text("title");
            table.ref_("project", "projects");
            table.write_if_ref_readable("project");
        });
    let mut bob =
        Runtime::open_with_schema(Storage::Memory, "bob-node", "bob", schema.clone()).unwrap();
    let mut edge = Runtime::open_trusted_with_schema(Storage::Memory, "edge", schema).unwrap();

    bob.create_branch("other", None).unwrap();
    bob.checkout_branch("other").unwrap();
    bob.insert_row(
        "projects",
        "project-other",
        BTreeMap::from([("title".to_owned(), json!("Other branch project"))]),
    )
    .unwrap();
    edge.apply_bundle(&bob.export_table_history("projects").unwrap())
        .unwrap();

    bob.create_branch("draft", None).unwrap();
    bob.checkout_branch("draft").unwrap();
    let tx = bob
        .insert_row(
            "todos",
            "todo-cross-branch",
            BTreeMap::from([
                ("title".to_owned(), json!("Should not be authorized")),
                ("project".to_owned(), json!("project-other")),
            ]),
        )
        .unwrap();

    edge.apply_untrusted_bundle(&bob.export_table_history("todos").unwrap())
        .unwrap();
    edge.checkout_branch("draft").unwrap();

    assert!(edge.read_rows("todos").unwrap().is_empty());
    assert_eq!(
        edge.transaction_info(&tx).unwrap().rejection_code,
        Some("policy_denied".to_owned())
    );
}

#[test]
fn branch_write_policy_uses_parent_visible_from_pinned_base() {
    let schema = SchemaDef::new()
        .table("projects", |table| {
            table.text("title");
            table.read_if_created_by_principal();
        })
        .table("todos", |table| {
            table.text("title");
            table.ref_("project", "projects");
            table.write_if_ref_readable("project");
        });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema).unwrap();

    let project_tx = alice
        .insert_row(
            "projects",
            "project-main",
            BTreeMap::from([("title".to_owned(), json!("Main project"))]),
        )
        .unwrap();
    alice.accept_transaction_at_global(&project_tx, 1).unwrap();

    alice.create_branch("draft", Some(1)).unwrap();
    alice.checkout_branch("draft").unwrap();
    alice
        .insert_row(
            "todos",
            "todo-draft",
            BTreeMap::from([
                ("title".to_owned(), json!("Draft todo")),
                ("project".to_owned(), json!("project-main")),
            ]),
        )
        .unwrap();

    let rows = alice.read_rows("todos").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, "todo-draft");
}

#[test]
fn branch_recursive_write_policy_uses_parent_state_from_pinned_base() {
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
            table.write_if_ref_readable("project");
        });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut bob = Runtime::open_with_schema(Storage::Memory, "bob-node", "bob", schema).unwrap();

    let alice_org_tx = alice
        .insert_row(
            "orgs",
            "org-alice",
            BTreeMap::from([("name".to_owned(), json!("Alice org"))]),
        )
        .unwrap();
    alice
        .accept_transaction_at_global(&alice_org_tx, 1)
        .unwrap();
    let project_tx = alice
        .insert_row(
            "projects",
            "project-1",
            BTreeMap::from([
                ("title".to_owned(), json!("Base project")),
                ("org".to_owned(), json!("org-alice")),
            ]),
        )
        .unwrap();
    alice.accept_transaction_at_global(&project_tx, 2).unwrap();
    alice.create_branch("draft", Some(2)).unwrap();

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
            "projects",
            "project-1",
            BTreeMap::from([
                ("title".to_owned(), json!("Main moved after branch")),
                ("org".to_owned(), json!("org-bob")),
            ]),
        )
        .unwrap();

    alice.checkout_branch("draft").unwrap();
    let tx = alice
        .insert_row(
            "todos",
            "todo-draft",
            BTreeMap::from([
                ("title".to_owned(), json!("Draft todo")),
                ("project".to_owned(), json!("project-1")),
            ]),
        )
        .unwrap();

    let rows = alice.read_rows("todos").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, "todo-draft");
    assert_eq!(
        alice.transaction_policy_read_rows(&tx).unwrap(),
        vec![
            ("orgs".to_owned(), "org-alice".to_owned()),
            ("projects".to_owned(), "project-1".to_owned())
        ]
    );
}

#[test]
fn trusted_edge_validates_branch_recursive_write_policy_against_pinned_base() {
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
            table.write_if_ref_readable("project");
        });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut bob =
        Runtime::open_trusted_as_with_schema(Storage::Memory, "bob-node", "bob", schema.clone())
            .unwrap();
    let mut edge = Runtime::open_trusted_with_schema(Storage::Memory, "edge", schema).unwrap();

    let alice_org_tx = alice
        .insert_row(
            "orgs",
            "org-alice",
            BTreeMap::from([("name".to_owned(), json!("Alice org"))]),
        )
        .unwrap();
    alice
        .accept_transaction_at_global(&alice_org_tx, 1)
        .unwrap();
    let project_tx = alice
        .insert_row(
            "projects",
            "project-1",
            BTreeMap::from([
                ("title".to_owned(), json!("Base project")),
                ("org".to_owned(), json!("org-alice")),
            ]),
        )
        .unwrap();
    alice.accept_transaction_at_global(&project_tx, 2).unwrap();
    alice.create_branch("draft", Some(2)).unwrap();

    edge.apply_bundle(&alice.export_table_history("projects").unwrap())
        .unwrap();

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
            "projects",
            "project-1",
            BTreeMap::from([
                ("title".to_owned(), json!("Main moved after branch")),
                ("org".to_owned(), json!("org-bob")),
            ]),
        )
        .unwrap();

    alice.checkout_branch("draft").unwrap();
    let tx = alice
        .insert_row(
            "todos",
            "todo-draft",
            BTreeMap::from([
                ("title".to_owned(), json!("Draft todo")),
                ("project".to_owned(), json!("project-1")),
            ]),
        )
        .unwrap();

    edge.apply_untrusted_bundle(&alice.export_table_history("todos").unwrap())
        .unwrap();
    edge.checkout_branch("draft").unwrap();
    let rows = edge.read_rows("todos").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, "todo-draft");
    assert_eq!(edge.transaction_info(&tx).unwrap().rejection_code, None);
}

#[test]
fn trusted_edge_rejects_untrusted_delete_policy_violation() {
    let schema = SchemaDef::new().table("docs", |table| {
        table.text("title");
        table.write_if_created_by_principal();
    });
    let mut alice = Runtime::open_trusted_as_with_schema(
        Storage::Memory,
        "alice-node",
        "alice",
        schema.clone(),
    )
    .unwrap();
    let mut bob =
        Runtime::open_trusted_as_with_schema(Storage::Memory, "bob-node", "bob", schema.clone())
            .unwrap();
    let mut edge = Runtime::open_trusted_with_schema(Storage::Memory, "edge", schema).unwrap();

    let alice_tx = alice
        .insert_row(
            "docs",
            "doc-1",
            BTreeMap::from([("title".to_owned(), json!("Alice owns this"))]),
        )
        .unwrap();
    let alice_bundle = alice.export_table_history("docs").unwrap();
    bob.apply_bundle(&alice_bundle).unwrap();
    edge.apply_bundle(&alice_bundle).unwrap();
    edge.accept_transaction_at_edge(&alice_tx).unwrap();

    let delete_tx = bob.delete_row("docs", "doc-1").unwrap();
    edge.apply_untrusted_bundle(&bob.export_table_history("docs").unwrap())
        .unwrap();

    let edge_rows = edge.read_rows("docs").unwrap();
    assert_eq!(edge_rows.len(), 1);
    assert_eq!(edge_rows[0].values["title"], json!("Alice owns this"));
    assert_eq!(
        edge.transaction_info(&delete_tx).unwrap().rejection_code,
        Some("policy_denied".to_owned())
    );
}

#[test]
fn created_by_write_policy_allows_self_create_but_rejects_other_writer() {
    let schema = SchemaDef::new().table("docs", |table| {
        table.text("title");
        table.write_if_created_by_principal();
    });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut bob =
        Runtime::open_trusted_as_with_schema(Storage::Memory, "bob-node", "bob", schema.clone())
            .unwrap();
    let mut edge = Runtime::open_trusted_with_schema(Storage::Memory, "edge", schema).unwrap();

    let create_tx = alice
        .insert_row(
            "docs",
            "doc-1",
            BTreeMap::from([("title".to_owned(), json!("Alice draft"))]),
        )
        .unwrap();
    assert_eq!(alice.read_rows("docs").unwrap().len(), 1);

    let alice_bundle = alice.export_table_history("docs").unwrap();
    bob.apply_bundle(&alice_bundle).unwrap();
    edge.apply_untrusted_bundle(&alice_bundle).unwrap();
    edge.accept_transaction_at_edge(&create_tx).unwrap();

    let update_tx = bob
        .update_row(
            "docs",
            "doc-1",
            BTreeMap::from([("title".to_owned(), json!("Bob rewrite"))]),
        )
        .unwrap();
    edge.apply_untrusted_bundle(&bob.export_table_history("docs").unwrap())
        .unwrap();

    assert_eq!(
        edge.read_rows("docs").unwrap()[0].values["title"],
        json!("Alice draft")
    );
    assert_eq!(
        edge.transaction_info(&update_tx).unwrap().rejection_code,
        Some("policy_denied".to_owned())
    );
}

#[test]
fn untrusted_validation_error_does_not_leave_invalid_current_row_visible() {
    let schema = SchemaDef::new()
        .table("projects", |table| {
            table.text("title");
            table.read_if_created_by_principal();
        })
        .table("todos", |table| {
            table.text("title");
            table.ref_("project", "projects");
            table.write_if_ref_readable("project");
        });
    let mut bob =
        Runtime::open_trusted_as_with_schema(Storage::Memory, "bob-node", "bob", schema.clone())
            .unwrap();
    let mut edge = Runtime::open_trusted_with_schema(Storage::Memory, "edge", schema).unwrap();

    let tx = bob
        .insert_row(
            "todos",
            "todo-1",
            BTreeMap::from([
                ("title".to_owned(), json!("Missing parent")),
                ("project".to_owned(), json!("project-missing")),
            ]),
        )
        .unwrap();

    edge.apply_untrusted_bundle(&bob.export_table_history("todos").unwrap())
        .unwrap();

    assert!(edge.read_rows("todos").unwrap().is_empty());
    assert_eq!(
        edge.transaction_info(&tx).unwrap().rejection_code,
        Some("policy_denied".to_owned())
    );
}

#[test]
fn durable_edge_rejects_after_restart_and_repairs_memory_client() {
    let dir = tempdir().unwrap();
    let edge_path = dir.path().join("edge.sqlite");
    let schema = SchemaDef::new()
        .table("projects", |table| {
            table.text("title");
            table.read_if_created_by_principal();
        })
        .table("todos", |table| {
            table.text("title");
            table.ref_("project", "projects");
            table.write_if_ref_readable("project");
        });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut bob =
        Runtime::open_trusted_as_with_schema(Storage::Memory, "bob-node", "bob", schema.clone())
            .unwrap();

    alice
        .insert_row(
            "projects",
            "project-1",
            BTreeMap::from([("title".to_owned(), json!("Alice project"))]),
        )
        .unwrap();
    let project_bundle = alice.export_table_history("projects").unwrap();
    bob.apply_bundle(&project_bundle).unwrap();

    let tx = bob
        .insert_row(
            "todos",
            "todo-1",
            BTreeMap::from([
                ("title".to_owned(), json!("Rejected after edge restart")),
                ("project".to_owned(), json!("project-1")),
            ]),
        )
        .unwrap();
    assert_eq!(bob.read_rows("todos").unwrap().len(), 1);

    {
        let mut edge = Runtime::open_trusted_with_schema(
            Storage::File(edge_path.clone()),
            "edge",
            schema.clone(),
        )
        .unwrap();
        edge.apply_bundle(&project_bundle).unwrap();
        edge.apply_bundle(&bob.export_table_history("todos").unwrap())
            .unwrap();
        assert_eq!(edge.read_rows("todos").unwrap().len(), 1);
    }

    let mut edge =
        Runtime::open_trusted_with_schema(Storage::File(edge_path), "edge", schema).unwrap();
    edge.apply_untrusted_bundle(&bob.export_table_history("todos").unwrap())
        .unwrap();
    assert!(edge.read_rows("todos").unwrap().is_empty());

    bob.apply_bundle(&edge.export_table_history("todos").unwrap())
        .unwrap();
    assert!(bob.read_rows("todos").unwrap().is_empty());
    assert_eq!(
        bob.transaction_info(&tx).unwrap().rejection_code,
        Some("policy_denied".to_owned())
    );
}

#[test]
fn policy_denied_write_is_rejected_history_not_current_state() {
    let schema = SchemaDef::new()
        .table("docs", |table| {
            table.text("title");
            table.read_if_created_by_principal();
        })
        .table("comments", |table| {
            table.text("body");
            table.ref_("doc", "docs");
            table.write_if_ref_readable("doc");
        });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut bob = Runtime::open_with_schema(Storage::Memory, "bob-node", "bob", schema).unwrap();

    let mut doc = BTreeMap::new();
    doc.insert("title".to_owned(), json!("Alice-only doc"));
    alice.insert_row("docs", "doc-1", doc).unwrap();
    bob.apply_bundle(&alice.export_table_history("docs").unwrap())
        .unwrap();

    let mut comment = BTreeMap::new();
    comment.insert("body".to_owned(), json!("Bob should not write this"));
    comment.insert("doc".to_owned(), json!("doc-1"));
    let rejected_tx = bob
        .insert_row("comments", "comment-denied", comment)
        .unwrap();

    assert!(bob.read_rows("comments").unwrap().is_empty());
    let stats = bob.storage_stats().unwrap();
    assert_eq!(stats.history_rows, 2);
    assert_eq!(stats.current_rows, 1);
    assert_eq!(stats.rejected_transactions, 1);
    assert!(stats.physical_tx_num_for(&rejected_tx).is_some());
}

#[test]
fn write_policy_parent_check_records_policy_read_set() {
    let schema = SchemaDef::new()
        .table("projects", |table| {
            table.text("title");
            table.read_if_created_by_principal();
        })
        .table("todos", |table| {
            table.text("title");
            table.ref_("project", "projects");
            table.write_if_ref_readable("project");
        });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema).unwrap();

    alice
        .insert_row(
            "projects",
            "project-1",
            BTreeMap::from([("title".to_owned(), json!("Alice project"))]),
        )
        .unwrap();
    let tx = alice
        .insert_row(
            "todos",
            "todo-1",
            BTreeMap::from([
                ("title".to_owned(), json!("Allowed by parent")),
                ("project".to_owned(), json!("project-1")),
            ]),
        )
        .unwrap();

    assert_eq!(
        alice.transaction_policy_read_rows(&tx).unwrap(),
        vec![("projects".to_owned(), "project-1".to_owned())]
    );
}

#[test]
fn patch_update_uses_preserved_ref_for_write_policy_validation() {
    let schema = SchemaDef::new()
        .table("projects", |table| {
            table.text("title");
            table.read_if_created_by_principal();
        })
        .table("todos", |table| {
            table.text("title");
            table.ref_("project", "projects");
            table.write_if_ref_readable("project");
        });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema).unwrap();

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
                ("title".to_owned(), json!("Before")),
                ("project".to_owned(), json!("project-1")),
            ]),
        )
        .unwrap();
    let tx = alice
        .update_row(
            "todos",
            "todo-1",
            BTreeMap::from([("title".to_owned(), json!("After"))]),
        )
        .unwrap();

    let rows = alice.read_rows("todos").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values["title"], json!("After"));
    assert_eq!(rows[0].values["project"], json!("project-1"));
    assert_eq!(
        alice.transaction_policy_read_rows(&tx).unwrap(),
        vec![("projects".to_owned(), "project-1".to_owned())]
    );
}

#[test]
fn recursive_write_policy_records_transitive_policy_read_set() {
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
            table.write_if_ref_readable("project");
        });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema).unwrap();

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
                ("title".to_owned(), json!("Allowed by recursive policy")),
                ("project".to_owned(), json!("project-1")),
            ]),
        )
        .unwrap();

    assert_eq!(
        alice.transaction_policy_read_rows(&tx).unwrap(),
        vec![
            ("orgs".to_owned(), "org-1".to_owned()),
            ("projects".to_owned(), "project-1".to_owned())
        ]
    );
}

#[test]
fn trusted_edge_accepts_untrusted_recursive_write_when_bundle_contains_transitive_policy_dependencies(
) {
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
            table.write_if_ref_readable("project");
        });
    let mut alice = Runtime::open_trusted_as_with_schema(
        Storage::Memory,
        "alice-node",
        "alice",
        schema.clone(),
    )
    .unwrap();
    let mut edge = Runtime::open_trusted_with_schema(Storage::Memory, "edge", schema).unwrap();

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
                ("title".to_owned(), json!("Allowed by transitive policy")),
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

    edge.apply_untrusted_bundle(&bundle).unwrap();

    let rows = edge.read_rows("todos").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, "todo-1");
    assert_eq!(edge.transaction_info(&tx).unwrap().rejection_code, None);
}

#[test]
fn policy_read_set_survives_sync() {
    let schema = SchemaDef::new()
        .table("projects", |table| {
            table.text("title");
            table.read_if_created_by_principal();
        })
        .table("todos", |table| {
            table.text("title");
            table.ref_("project", "projects");
            table.write_if_ref_readable("project");
        });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut peer =
        Runtime::open_with_schema(Storage::Memory, "alice-peer-node", "alice", schema).unwrap();

    alice
        .insert_row(
            "projects",
            "project-1",
            BTreeMap::from([("title".to_owned(), json!("Alice project"))]),
        )
        .unwrap();
    let tx = alice
        .insert_row(
            "todos",
            "todo-1",
            BTreeMap::from([
                ("title".to_owned(), json!("Allowed by parent")),
                ("project".to_owned(), json!("project-1")),
            ]),
        )
        .unwrap();

    peer.apply_bundle(&alice.export_table_history("todos").unwrap())
        .unwrap();
    assert_eq!(
        peer.transaction_policy_read_rows(&tx).unwrap(),
        vec![("projects".to_owned(), "project-1".to_owned())]
    );
}

#[test]
fn bundle_read_sets_are_scoped_to_exported_history_transactions() {
    let schema = SchemaDef::new()
        .table("projects", |table| {
            table.text("title");
            table.read_if_created_by_principal();
        })
        .table("todos", |table| {
            table.text("title");
            table.ref_("project", "projects");
            table.write_if_ref_readable("project");
        })
        .table("milestones", |table| {
            table.text("title");
            table.ref_("project", "projects");
            table.write_if_ref_readable("project");
        });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema).unwrap();

    alice
        .insert_row(
            "projects",
            "project-1",
            BTreeMap::from([("title".to_owned(), json!("Alice project"))]),
        )
        .unwrap();
    let todo_tx = alice
        .insert_row(
            "todos",
            "todo-1",
            BTreeMap::from([
                ("title".to_owned(), json!("Todo")),
                ("project".to_owned(), json!("project-1")),
            ]),
        )
        .unwrap();
    let milestone_tx = alice
        .insert_row(
            "milestones",
            "milestone-1",
            BTreeMap::from([
                ("title".to_owned(), json!("Milestone")),
                ("project".to_owned(), json!("project-1")),
            ]),
        )
        .unwrap();

    let bundle = alice.export_table_history("todos").unwrap();
    let read_txs = bundle
        .reads
        .iter()
        .map(|read| read.tx_id.as_str())
        .collect::<Vec<_>>();
    assert!(read_txs.contains(&todo_tx.as_str()));
    assert!(!read_txs.contains(&milestone_tx.as_str()));
}
