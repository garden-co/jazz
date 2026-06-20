use super::*;

#[test]
fn policy_filters_reads_through_required_parent_ref() {
    let schema = SchemaDef::new()
        .table("projects", |table| {
            table.text("title");
            table.read_if_created_by_user();
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
        table.write_if_created_by_user();
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

    edge.apply_untrusted_bundle_as_user(&bundle, writer.session_user())
        .unwrap();
    assert_eq!(edge.read_rows("notes").unwrap().len(), 1);

    let mut rejecting_edge = Runtime::open_trusted_with_schema(
        Storage::Memory,
        "rejecting-edge",
        SchemaDef::new().table("notes", |table| {
            table.text("body");
            table.bool("pinned");
            table.write_if_created_by_user();
        }),
    )
    .unwrap();
    rejecting_edge
        .apply_untrusted_bundle_as_user(&bundle, writer.session_user())
        .unwrap();
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
            table.read_if_created_by_user();
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
fn trusted_peer_can_read_applied_policy_scoped_facts_without_user() {
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
fn trusted_transport_history_still_filters_untrusted_current_reads() {
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
    let mut bob =
        Runtime::open_with_schema(Storage::Memory, "bob-node", "bob", schema.clone()).unwrap();
    let mut edge =
        Runtime::open_trusted_with_schema(Storage::Memory, "edge", schema.clone()).unwrap();
    let mut alice_peer =
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
            "todos",
            "todo-alice",
            BTreeMap::from([
                ("title".to_owned(), json!("Visible to Alice")),
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
        "todo-bob",
        BTreeMap::from([
            ("title".to_owned(), json!("Hidden from Alice")),
            ("project".to_owned(), json!("project-bob")),
        ]),
    )
    .unwrap();

    edge.apply_bundle(&alice.export_table_history("todos").unwrap())
        .unwrap();
    edge.apply_bundle(&bob.export_table_history("todos").unwrap())
        .unwrap();
    assert_eq!(edge.read_rows("todos").unwrap().len(), 2);

    alice_peer
        .apply_bundle(&edge.export_table_history("todos").unwrap())
        .unwrap();
    let visible = alice_peer.read_rows("todos").unwrap();
    assert_eq!(visible.len(), 1);
    assert_eq!(visible[0].id, "todo-alice");
}

#[test]
fn trusted_peer_generic_transaction_bypasses_user_write_policy() {
    let schema = SchemaDef::new()
        .table("docs", |table| {
            table.text("title");
            table.read_if_created_by_user();
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
    let harness = support::Harness::new();
    let schema = SchemaDef::new().table("notes", |table| {
        table.text("body");
        table.bool("pinned");
        table.read_if_created_by_user();
    });
    let mut alice = harness
        .memory_with_schema("alice-tab", "alice", schema.clone())
        .unwrap();
    let mut edge = harness
        .trusted_durable_with_schema("edge.sqlite", "edge", schema.clone())
        .unwrap();
    let mut alice_phone = harness
        .memory_with_schema("alice-phone", "alice", schema.clone())
        .unwrap();
    let mut bob = harness
        .memory_with_schema("bob-tab", "bob", schema)
        .unwrap();

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
    support::sync_table(&alice, &mut edge, "notes").unwrap();
    edge.accept_transaction_at_global(&tx, 11).unwrap();

    let accepted_bundle = edge.export_table_history("notes").unwrap();
    support::apply(accepted_bundle.clone(), &mut alice_phone).unwrap();
    support::apply(accepted_bundle, &mut bob).unwrap();

    assert_eq!(alice_phone.read_rows("notes").unwrap().len(), 1);
    assert_eq!(
        alice_phone.transaction_info(&tx).unwrap().global_epoch,
        Some(11)
    );
    assert!(bob.read_rows("notes").unwrap().is_empty());
}

#[test]
fn trusted_edge_acceptance_syncs_without_global_epoch() {
    let harness = support::Harness::new();
    let schema = support::notes_schema();
    let mut alice = harness
        .memory_with_schema("alice-tab", "alice", schema.clone())
        .unwrap();
    let mut edge = harness
        .trusted_durable_with_schema("edge.sqlite", "edge", schema.clone())
        .unwrap();
    let mut phone = harness
        .memory_with_schema("alice-phone", "alice", schema)
        .unwrap();

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
    support::sync_table(&alice, &mut edge, "notes").unwrap();
    edge.accept_transaction_at_edge(&tx).unwrap();

    support::sync_table(&edge, &mut phone, "notes").unwrap();

    let info = phone.transaction_info(&tx).unwrap();
    assert_eq!(info.global_epoch, None);
    assert!(info.receipt_tiers.contains(&"edge".to_owned()));
    assert_eq!(phone.read_rows("notes").unwrap().len(), 1);
}

#[test]
fn edge_accepted_transaction_can_upgrade_to_global_epoch() {
    let harness = support::Harness::new();
    let schema = support::notes_schema();
    let mut alice = harness
        .memory_with_schema("alice-tab", "alice", schema.clone())
        .unwrap();
    let mut edge = harness
        .trusted_memory_with_schema("edge", schema.clone())
        .unwrap();
    let mut phone = harness
        .memory_with_schema("alice-phone", "alice", schema)
        .unwrap();

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
    support::sync_table(&alice, &mut edge, "notes").unwrap();
    edge.accept_transaction_at_edge(&tx).unwrap();
    support::sync_table(&edge, &mut phone, "notes").unwrap();
    assert_eq!(phone.transaction_info(&tx).unwrap().global_epoch, None);

    edge.accept_transaction_at_global(&tx, 42).unwrap();
    support::sync_table(&edge, &mut phone, "notes").unwrap();

    let info = phone.transaction_info(&tx).unwrap();
    assert_eq!(info.global_epoch, Some(42));
    assert!(info.receipt_tiers.contains(&"edge".to_owned()));
    assert!(info.receipt_tiers.contains(&"global".to_owned()));
}

#[test]
fn edge_core_peer_edge_round_trip_preserves_policy_and_receipts() {
    let harness = support::Harness::new();
    let schema = SchemaDef::new().table("notes", |table| {
        table.text("body");
        table.bool("pinned");
        table.read_if_created_by_user();
    });
    let support::TrustedMeshTopology {
        mut alice_tab,
        mut edge_a,
        mut core,
        mut edge_b,
        mut alice_laptop,
        mut bob_tab,
    } = support::TrustedMeshTopology::memory(&harness, schema).unwrap();

    let tx = alice_tab
        .insert_row(
            "notes",
            "note-1",
            BTreeMap::from([
                ("body".to_owned(), json!("Through edge and core")),
                ("pinned".to_owned(), json!(true)),
            ]),
        )
        .unwrap();

    support::sync_table_untrusted(&alice_tab, &mut edge_a, "notes").unwrap();
    edge_a.accept_transaction_at_edge(&tx).unwrap();
    support::sync_table(&edge_a, &mut core, "notes").unwrap();
    core.accept_transaction_at_global(&tx, 99).unwrap();
    support::sync_table(&core, &mut edge_b, "notes").unwrap();

    let global_bundle = edge_b.export_table_history("notes").unwrap();
    support::apply(global_bundle.clone(), &mut alice_laptop).unwrap();
    support::apply(global_bundle, &mut bob_tab).unwrap();

    let alice_rows = alice_laptop.read_rows("notes").unwrap();
    assert_eq!(alice_rows.len(), 1);
    assert_eq!(alice_rows[0].id, "note-1");
    assert!(bob_tab.read_rows("notes").unwrap().is_empty());

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
            table.read_if_created_by_user();
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
    let harness = support::Harness::new();
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
    let support::TrustedEdgeTopology {
        mut alice,
        mut bob,
        mut edge,
    } = support::TrustedEdgeTopology::memory(&harness, schema).unwrap();

    alice
        .insert_row(
            "projects",
            "project-1",
            BTreeMap::from([("title".to_owned(), json!("Alice project"))]),
        )
        .unwrap();
    let project_bundle = alice.export_table_history("projects").unwrap();
    support::apply(project_bundle.clone(), &mut bob).unwrap();
    support::apply(project_bundle, &mut edge).unwrap();

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

    support::apply_untrusted_as_user(bob.export_table_history("todos").unwrap(), &mut edge, "bob")
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
    assert_eq!(
        edge.rejected_transactions().unwrap(),
        vec![RejectionInfo {
            tx_id: tx.clone(),
            code: "policy_denied".to_owned(),
            detail: Some(json!({
                "reason": "write_policy_denied",
                "table": "todos",
                "row_id": "todo-1"
            })),
        }]
    );

    support::sync_table(&edge, &mut bob, "todos").unwrap();
    assert_eq!(
        bob.transaction_info(&tx).unwrap().rejection_detail,
        Some(json!({
            "reason": "write_policy_denied",
            "table": "todos",
            "row_id": "todo-1"
        }))
    );
    assert_eq!(
        bob.rejected_transactions().unwrap(),
        vec![RejectionInfo {
            tx_id: tx,
            code: "policy_denied".to_owned(),
            detail: Some(json!({
                "reason": "write_policy_denied",
                "table": "todos",
                "row_id": "todo-1"
            })),
        }]
    );
}

#[test]
fn core_validates_forwarded_exclusive_transaction_with_session_user() {
    let harness = support::Harness::new();
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
    let mut topology = support::Topology::new(
        &harness,
        schema,
        &[
            (
                "alice",
                support::NodeSpec::client_memory("alice-node", "alice"),
            ),
            ("edge", support::NodeSpec::trusted_peer_memory("edge")),
            ("core", support::NodeSpec::trusted_peer_memory("core")),
            (
                "core-missing-auth",
                support::NodeSpec::trusted_peer_memory("core-missing-auth"),
            ),
            (
                "core-no-auth",
                support::NodeSpec::trusted_peer_memory("core-no-auth"),
            ),
        ],
    )
    .unwrap();
    let mut alice = topology.take("alice");
    let mut edge = topology.take("edge");
    let mut core = topology.take("core");
    let mut core_missing_auth = topology.take("core-missing-auth");
    let mut core_without_forwarded_auth = topology.take("core-no-auth");

    alice
        .insert_row(
            "projects",
            "project-1",
            BTreeMap::from([("title".to_owned(), json!("Alice project"))]),
        )
        .unwrap();
    let project_bundle = alice.export_table_history("projects").unwrap();
    support::apply(project_bundle.clone(), &mut edge).unwrap();
    support::apply(project_bundle.clone(), &mut core).unwrap();
    support::apply(project_bundle.clone(), &mut core_missing_auth).unwrap();
    support::apply(project_bundle, &mut core_without_forwarded_auth).unwrap();

    let tx = support::run_attributing_to_user(&mut edge, "service", |edge| {
        edge.insert_row(
            "todos",
            "todo-1",
            BTreeMap::from([
                ("title".to_owned(), json!("Forwarded exclusive")),
                ("project".to_owned(), json!("project-1")),
            ]),
        )
    })
    .unwrap();
    let mut missing_auth_bundle = edge
        .export_exclusive_transaction_forwarding("todos", &tx, "alice")
        .unwrap();
    missing_auth_bundle
        .txs
        .iter_mut()
        .find(|record| record.tx_id == tx)
        .unwrap()
        .auth_user = None;
    support::apply_untrusted(missing_auth_bundle, &mut core_missing_auth).unwrap();
    assert_eq!(
        core_missing_auth
            .transaction_info(&tx)
            .unwrap()
            .rejection_code,
        Some("policy_denied".to_owned())
    );
    assert_eq!(
        core_missing_auth
            .transaction_info(&tx)
            .unwrap()
            .rejection_detail,
        Some(json!({ "reason": "missing_auth_user" }))
    );
    assert!(core_missing_auth.read_rows("todos").unwrap().is_empty());

    support::forward_exclusive(
        &edge,
        &mut core_without_forwarded_auth,
        "todos",
        &tx,
        "service",
    )
    .unwrap();
    assert_eq!(
        core_without_forwarded_auth
            .transaction_info(&tx)
            .unwrap()
            .rejection_code,
        Some("policy_denied".to_owned())
    );
    assert!(core_without_forwarded_auth
        .read_rows("todos")
        .unwrap()
        .is_empty());

    support::forward_exclusive(&edge, &mut core, "todos", &tx, "alice").unwrap();

    let info = core.transaction_info(&tx).unwrap();
    assert_eq!(info.conflict_mode, "exclusive");
    assert_eq!(info.rejection_code, None);
    assert_eq!(info.global_epoch, Some(1));
    assert!(info.receipt_tiers.contains(&"global".to_owned()));
    assert_eq!(core.read_rows("todos").unwrap().len(), 1);
    assert_eq!(
        core.export_table_history("todos")
            .unwrap()
            .txs
            .iter()
            .find(|record| record.tx_id == tx)
            .unwrap()
            .auth_user
            .as_deref(),
        Some("alice")
    );
}

#[test]
fn edge_forwards_exclusive_without_local_policy_dependency_and_core_accepts() {
    let harness = support::Harness::new();
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
    let mut alice = harness
        .memory_with_schema("alice-node", "alice", schema.clone())
        .unwrap();
    let mut edge = harness
        .trusted_memory_with_schema("edge", schema.clone())
        .unwrap();
    let mut core = harness
        .trusted_memory_with_schema("core", schema.clone())
        .unwrap();

    alice
        .insert_row(
            "projects",
            "project-1",
            BTreeMap::from([("title".to_owned(), json!("Alice project"))]),
        )
        .unwrap();
    core.apply_bundle(&alice.export_table_history("projects").unwrap())
        .unwrap();
    let tx = alice
        .insert_row(
            "todos",
            "todo-1",
            BTreeMap::from([
                ("title".to_owned(), json!("Forward through cold edge")),
                ("project".to_owned(), json!("project-1")),
            ]),
        )
        .unwrap();

    let mut forwarded = alice
        .export_exclusive_transaction_forwarding("todos", &tx, "alice")
        .unwrap();
    forwarded
        .history
        .retain(|record| record.table != "projects");

    edge.stage_exclusive_bundle_for_forwarding(&forwarded)
        .unwrap();
    assert!(edge.read_rows("todos").unwrap().is_empty());
    let edge_info = edge.transaction_info(&tx).unwrap();
    assert_eq!(edge_info.conflict_mode, "exclusive");
    assert_eq!(edge_info.global_epoch, None);
    assert_eq!(edge_info.awaiting_dependency, None);
    assert_eq!(edge_info.rejection_code, None);

    support::forward_exclusive(&edge, &mut core, "todos", &tx, "alice").unwrap();

    let info = core.transaction_info(&tx).unwrap();
    assert_eq!(info.conflict_mode, "exclusive");
    assert_eq!(info.rejection_code, None);
    assert_eq!(info.global_epoch, Some(1));
    assert!(info.receipt_tiers.contains(&"global".to_owned()));
    let rows = core.read_rows("todos").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, "todo-1");
}

#[test]
fn trusted_edge_awaits_missing_policy_dependency_then_accepts_after_it_arrives() {
    let harness = support::Harness::new();
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
    let mut bob = harness
        .trusted_memory_with_schema("bob-node", schema.clone())
        .unwrap();
    let mut edge = harness.trusted_memory_with_schema("edge", schema).unwrap();

    let tx = support::run_as_user(&mut bob, "bob", |bob| {
        bob.insert_row(
            "projects",
            "project-bob",
            BTreeMap::from([("title".to_owned(), json!("Bob project"))]),
        )?;
        bob.insert_row(
            "todos",
            "todo-1",
            BTreeMap::from([
                ("title".to_owned(), json!("Missing dependency")),
                ("project".to_owned(), json!("project-bob")),
            ]),
        )
    })
    .unwrap();

    let mut incomplete_bundle = bob.export_table_history("todos").unwrap();
    incomplete_bundle
        .history
        .retain(|record| record.table != "projects");

    support::apply_untrusted_as_user(incomplete_bundle, &mut edge, "bob").unwrap();
    assert!(edge.read_rows("todos").unwrap().is_empty());
    assert_eq!(edge.transaction_info(&tx).unwrap().rejection_code, None);
    assert_eq!(
        edge.transaction_info(&tx).unwrap().awaiting_dependency,
        Some(json!({
            "reason": "policy_dependency_unavailable",
            "table": "todos",
            "row_id": "todo-1",
            "dependency_table": "projects",
            "dependency_row_id": "project-bob"
        }))
    );

    support::sync_table(&bob, &mut edge, "projects").unwrap();

    let rows = edge.read_rows("todos").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, "todo-1");
    let info = edge.transaction_info(&tx).unwrap();
    assert_eq!(info.rejection_code, None);
    assert_eq!(info.awaiting_dependency, None);
    assert_eq!(info.receipt_tiers, vec!["edge".to_owned()]);
}

#[test]
fn durable_edge_preserves_awaiting_dependency_across_restart() {
    let harness = support::Harness::new();
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
    let mut bob = harness
        .trusted_memory_with_schema("bob-node", schema.clone())
        .unwrap();
    let mut edge = harness
        .trusted_durable_with_schema("edge.sqlite", "edge", schema.clone())
        .unwrap();

    let tx = support::run_as_user(&mut bob, "bob", |bob| {
        bob.insert_row(
            "projects",
            "project-bob",
            BTreeMap::from([("title".to_owned(), json!("Bob project"))]),
        )?;
        bob.insert_row(
            "todos",
            "todo-1",
            BTreeMap::from([
                ("title".to_owned(), json!("Restart while awaiting")),
                ("project".to_owned(), json!("project-bob")),
            ]),
        )
    })
    .unwrap();

    let mut incomplete_bundle = bob.export_table_history("todos").unwrap();
    incomplete_bundle
        .history
        .retain(|record| record.table != "projects");
    support::apply_untrusted_as_user(incomplete_bundle, &mut edge, "bob").unwrap();
    assert_eq!(
        edge.transaction_info(&tx).unwrap().awaiting_dependency,
        Some(json!({
            "reason": "policy_dependency_unavailable",
            "table": "todos",
            "row_id": "todo-1",
            "dependency_table": "projects",
            "dependency_row_id": "project-bob"
        }))
    );
    drop(edge);

    let mut edge = harness
        .trusted_durable_with_schema("edge.sqlite", "edge", schema)
        .unwrap();
    assert!(edge.read_rows("todos").unwrap().is_empty());
    assert!(edge
        .transaction_info(&tx)
        .unwrap()
        .awaiting_dependency
        .is_some());

    support::sync_table(&bob, &mut edge, "projects").unwrap();

    assert_eq!(edge.read_rows("todos").unwrap().len(), 1);
    let info = edge.transaction_info(&tx).unwrap();
    assert_eq!(info.awaiting_dependency, None);
    assert_eq!(info.rejection_code, None);
    assert_eq!(info.receipt_tiers, vec!["edge".to_owned()]);
}

#[test]
fn direct_authority_fate_clears_awaiting_dependency_marker() {
    let harness = support::Harness::new();
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
    let mut bob = harness
        .trusted_memory_with_schema("bob-node", schema.clone())
        .unwrap();
    let mut edge = harness.trusted_memory_with_schema("edge", schema).unwrap();

    let tx = support::run_as_user(&mut bob, "bob", |bob| {
        bob.insert_row(
            "projects",
            "project-bob",
            BTreeMap::from([("title".to_owned(), json!("Bob project"))]),
        )?;
        bob.insert_row(
            "todos",
            "todo-1",
            BTreeMap::from([
                ("title".to_owned(), json!("Direct fate while awaiting")),
                ("project".to_owned(), json!("project-bob")),
            ]),
        )
    })
    .unwrap();

    let mut incomplete_bundle = bob.export_table_history("todos").unwrap();
    incomplete_bundle
        .history
        .retain(|record| record.table != "projects");
    support::apply_untrusted_as_user(incomplete_bundle, &mut edge, "bob").unwrap();
    assert!(edge
        .transaction_info(&tx)
        .unwrap()
        .awaiting_dependency
        .is_some());
    assert!(edge.read_rows("todos").unwrap().is_empty());

    edge.accept_transaction_at_global(&tx, 1).unwrap();

    let info = edge.transaction_info(&tx).unwrap();
    assert_eq!(info.awaiting_dependency, None);
    assert_eq!(info.rejection_code, None);
    assert_eq!(info.global_epoch, Some(1));
    let rows = edge.read_rows("todos").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, "todo-1");
}

#[test]
fn edge_rejects_awaiting_transaction_when_arrived_dependency_is_not_readable() {
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
    let mut bob = Runtime::open_trusted_attributing_to_user(
        Storage::Memory,
        "bob-node",
        "bob",
        schema.clone(),
    )
    .unwrap();
    let mut edge = Runtime::open_trusted_with_schema(Storage::Memory, "edge", schema).unwrap();

    alice
        .insert_row(
            "projects",
            "project-alice",
            BTreeMap::from([("title".to_owned(), json!("Private Alice project"))]),
        )
        .unwrap();
    let tx = bob
        .insert_row(
            "todos",
            "todo-1",
            BTreeMap::from([
                (
                    "title".to_owned(),
                    json!("Should be denied after dependency arrives"),
                ),
                ("project".to_owned(), json!("project-alice")),
            ]),
        )
        .unwrap();

    edge.apply_untrusted_bundle_as_user(&bob.export_table_history("todos").unwrap(), "bob")
        .unwrap();
    assert!(edge
        .transaction_info(&tx)
        .unwrap()
        .awaiting_dependency
        .is_some());

    edge.apply_bundle(&alice.export_table_history("projects").unwrap())
        .unwrap();

    assert!(edge.read_rows("todos").unwrap().is_empty());
    let info = edge.transaction_info(&tx).unwrap();
    assert_eq!(info.awaiting_dependency, None);
    assert_eq!(info.rejection_code, Some("policy_denied".to_owned()));
    assert_eq!(
        info.rejection_detail,
        Some(json!({
            "reason": "write_policy_denied",
            "table": "todos",
            "row_id": "todo-1"
        }))
    );
}

#[test]
fn trusted_edge_accepts_untrusted_write_when_bundle_contains_policy_dependency() {
    let harness = support::Harness::new();
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
    let mut bob = harness
        .trusted_memory_with_schema("bob-node", schema.clone())
        .unwrap();
    let mut edge = harness.trusted_memory_with_schema("edge", schema).unwrap();

    let tx = support::run_as_user(&mut bob, "bob", |bob| {
        bob.insert_row(
            "projects",
            "project-bob",
            BTreeMap::from([("title".to_owned(), json!("Bob project"))]),
        )?;
        bob.insert_row(
            "todos",
            "todo-1",
            BTreeMap::from([
                ("title".to_owned(), json!("Dependency included")),
                ("project".to_owned(), json!("project-bob")),
            ]),
        )
    })
    .unwrap();

    support::apply_untrusted_as_user(bob.export_table_history("todos").unwrap(), &mut edge, "bob")
        .unwrap();

    let rows = edge.read_rows("todos").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, "todo-1");
    assert_eq!(edge.transaction_info(&tx).unwrap().rejection_code, None);
}

#[test]
fn untrusted_policy_validation_uses_authenticated_user_not_forged_provenance() {
    let schema = SchemaDef::new().table("docs", |table| {
        table.text("title");
        table.write_if_created_by_user();
    });
    let mut bob =
        Runtime::open_with_schema(Storage::Memory, "bob-node", "bob", schema.clone()).unwrap();
    let mut edge = Runtime::open_trusted_with_schema(Storage::Memory, "edge", schema).unwrap();

    let tx = bob
        .insert_row(
            "docs",
            "doc-forged",
            BTreeMap::from([("title".to_owned(), json!("Forged owner"))]),
        )
        .unwrap();
    let mut bundle = bob.export_table_history("docs").unwrap();
    for record in &mut bundle.history {
        record.created_by = "alice".to_owned();
        record.updated_by = "alice".to_owned();
    }

    edge.apply_untrusted_bundle_as_user(&bundle, "bob").unwrap();

    assert!(edge.read_rows("docs").unwrap().is_empty());
    assert_eq!(
        edge.transaction_info(&tx).unwrap().rejection_code,
        Some("policy_denied".to_owned())
    );
}

#[test]
fn trusted_edge_rejects_untrusted_transaction_atomically() {
    let harness = support::Harness::new();
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
    let support::TrustedEdgeTopology {
        mut alice,
        mut bob,
        mut edge,
    } = support::TrustedEdgeTopology::memory(&harness, schema).unwrap();

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
    support::sync_table(&alice, &mut bob, "projects").unwrap();
    support::sync_table(&alice, &mut edge, "projects").unwrap();
    support::sync_table(&bob, &mut edge, "projects").unwrap();

    let tx = support::run_as_user(&mut bob, "bob", |bob| {
        bob.transaction()
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
    })
    .unwrap();

    support::apply_untrusted_as_user(bob.export_table_history("todos").unwrap(), &mut edge, "bob")
        .unwrap();

    assert!(edge.read_rows("todos").unwrap().is_empty());
    assert_eq!(
        edge.transaction_info(&tx).unwrap().rejection_code,
        Some("policy_denied".to_owned())
    );
}

#[test]
fn trusted_edge_rejects_untrusted_update_to_unreadable_ref() {
    let harness = support::Harness::new();
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
    let support::TrustedEdgeTopology {
        mut alice,
        mut bob,
        mut edge,
    } = support::TrustedEdgeTopology::memory(&harness, schema).unwrap();

    alice
        .insert_row(
            "projects",
            "project-alice",
            BTreeMap::from([("title".to_owned(), json!("Alice project"))]),
        )
        .unwrap();
    support::run_as_user(&mut bob, "bob", |bob| {
        bob.insert_row(
            "projects",
            "project-bob",
            BTreeMap::from([("title".to_owned(), json!("Bob project"))]),
        )?;
        bob.insert_row(
            "todos",
            "todo-1",
            BTreeMap::from([
                ("title".to_owned(), json!("Initially allowed")),
                ("project".to_owned(), json!("project-bob")),
            ]),
        )
    })
    .unwrap();
    support::sync_table(&alice, &mut edge, "projects").unwrap();
    support::apply_untrusted_as_user(
        bob.export_table_history("projects").unwrap(),
        &mut edge,
        "bob",
    )
    .unwrap();
    support::apply_untrusted_as_user(bob.export_table_history("todos").unwrap(), &mut edge, "bob")
        .unwrap();
    assert_eq!(edge.read_rows("todos").unwrap().len(), 1);

    let tx = support::run_as_user(&mut bob, "bob", |bob| {
        bob.update_row(
            "todos",
            "todo-1",
            BTreeMap::from([
                ("title".to_owned(), json!("Reparented")),
                ("project".to_owned(), json!("project-alice")),
            ]),
        )
    })
    .unwrap();
    support::apply_untrusted_as_user(bob.export_table_history("todos").unwrap(), &mut edge, "bob")
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
            table.read_if_created_by_user();
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

    edge.apply_untrusted_bundle_as_user(
        &bob.export_table_history("todos").unwrap(),
        bob.session_user(),
    )
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
            table.read_if_created_by_user();
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
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut bob =
        Runtime::open_trusted_with_session_user(Storage::Memory, "bob-node", "bob", schema.clone())
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

    edge.apply_untrusted_bundle_as_user(
        &alice.export_table_history("todos").unwrap(),
        alice.session_user(),
    )
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
        table.write_if_created_by_user();
    });
    let mut alice = Runtime::open_trusted_with_session_user(
        Storage::Memory,
        "alice-node",
        "alice",
        schema.clone(),
    )
    .unwrap();
    let mut bob =
        Runtime::open_trusted_with_session_user(Storage::Memory, "bob-node", "bob", schema.clone())
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
    edge.apply_untrusted_bundle_as_user(
        &bob.export_table_history("docs").unwrap(),
        bob.session_user(),
    )
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
    let harness = support::Harness::new();
    let schema = SchemaDef::new().table("docs", |table| {
        table.text("title");
        table.write_if_created_by_user();
    });
    let mut alice = harness
        .memory_with_schema("alice-node", "alice", schema.clone())
        .unwrap();
    let mut bob = harness
        .trusted_memory_with_schema("bob-node", schema.clone())
        .unwrap();
    let mut edge = harness.trusted_memory_with_schema("edge", schema).unwrap();

    let create_tx = alice
        .insert_row(
            "docs",
            "doc-1",
            BTreeMap::from([("title".to_owned(), json!("Alice draft"))]),
        )
        .unwrap();
    assert_eq!(alice.read_rows("docs").unwrap().len(), 1);

    let alice_bundle = alice.export_table_history("docs").unwrap();
    support::apply(alice_bundle.clone(), &mut bob).unwrap();
    support::apply_untrusted_as_user(alice_bundle, &mut edge, alice.session_user()).unwrap();
    edge.accept_transaction_at_edge(&create_tx).unwrap();

    let update_tx = support::run_as_user(&mut bob, "bob", |bob| {
        bob.update_row(
            "docs",
            "doc-1",
            BTreeMap::from([("title".to_owned(), json!("Bob rewrite"))]),
        )
    })
    .unwrap();
    support::sync_table_untrusted(&bob, &mut edge, "docs").unwrap();

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
    let harness = support::Harness::new();
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
    let mut bob = harness
        .trusted_memory_with_schema("bob-node", schema.clone())
        .unwrap();
    let mut edge = harness.trusted_memory_with_schema("edge", schema).unwrap();

    let tx = support::run_as_user(&mut bob, "bob", |bob| {
        bob.insert_row(
            "todos",
            "todo-1",
            BTreeMap::from([
                ("title".to_owned(), json!("Missing parent")),
                ("project".to_owned(), json!("project-missing")),
            ]),
        )
    })
    .unwrap();

    support::apply_untrusted_as_user(bob.export_table_history("todos").unwrap(), &mut edge, "bob")
        .unwrap();

    assert!(edge.read_rows("todos").unwrap().is_empty());
    assert_eq!(
        edge.transaction_info(&tx).unwrap().rejection_code,
        Some("policy_denied".to_owned())
    );
}

#[test]
fn durable_edge_rejects_after_restart_and_repairs_memory_client() {
    let harness = support::Harness::new();
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
    let support::TrustedEdgeTopology {
        mut alice,
        mut bob,
        edge: _,
    } = support::TrustedEdgeTopology::durable_edge(&harness, schema.clone()).unwrap();

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
        let mut edge = harness
            .trusted_durable_with_schema("edge.sqlite", "edge", schema.clone())
            .unwrap();
        support::apply(project_bundle.clone(), &mut edge).unwrap();
        support::sync_table(&bob, &mut edge, "todos").unwrap();
        assert_eq!(edge.read_rows("todos").unwrap().len(), 1);
    }

    let mut edge = harness
        .trusted_durable_with_schema("edge.sqlite", "edge", schema)
        .unwrap();
    support::apply_untrusted_as_user(bob.export_table_history("todos").unwrap(), &mut edge, "bob")
        .unwrap();
    assert!(edge.read_rows("todos").unwrap().is_empty());

    support::sync_table(&edge, &mut bob, "todos").unwrap();
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
            table.read_if_created_by_user();
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
            table.read_if_created_by_user();
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
            table.read_if_created_by_user();
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
fn ref_retarget_update_validates_new_parent_policy() {
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
    let mut bob = Runtime::open_with_schema(Storage::Memory, "bob-node", "bob", schema).unwrap();

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
    alice
        .apply_bundle(&bob.export_table_history("projects").unwrap())
        .unwrap();
    alice
        .insert_row(
            "todos",
            "todo-1",
            BTreeMap::from([
                ("title".to_owned(), json!("Original")),
                ("project".to_owned(), json!("project-alice")),
            ]),
        )
        .unwrap();

    let tx = alice
        .update_row(
            "todos",
            "todo-1",
            BTreeMap::from([("project".to_owned(), json!("project-bob"))]),
        )
        .unwrap();

    let rows = alice.read_rows("todos").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values["project"], json!("project-alice"));
    assert_eq!(
        alice.transaction_info(&tx).unwrap().rejection_code,
        Some("policy_denied".to_owned())
    );
}

#[test]
fn policy_denied_delete_restores_previous_visible_row_and_subscription() {
    let schema = SchemaDef::new()
        .table("docs", |table| {
            table.text("title");
            table.read_if_created_by_user();
        })
        .table("comments", |table| {
            table.text("body");
            table.ref_("doc", "docs");
            table.write_if_ref_readable("doc");
        });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut bob = Runtime::open_with_schema(Storage::Memory, "bob-node", "bob", schema).unwrap();

    bob.insert_row(
        "docs",
        "doc-bob",
        BTreeMap::from([("title".to_owned(), json!("Bob doc"))]),
    )
    .unwrap();
    bob.insert_row(
        "comments",
        "comment-1",
        BTreeMap::from([
            ("body".to_owned(), json!("Kept after rejected delete")),
            ("doc".to_owned(), json!("doc-bob")),
        ]),
    )
    .unwrap();
    alice
        .apply_bundle(&bob.export_table_history("docs").unwrap())
        .unwrap();
    alice
        .apply_bundle(&bob.export_table_history("comments").unwrap())
        .unwrap();
    let mut subscription = alice.subscribe_rows("comments").unwrap();
    assert_eq!(subscription.initial_rows().len(), 1);

    let tx = alice.delete_row("comments", "comment-1").unwrap();

    assert_eq!(
        alice.transaction_info(&tx).unwrap().rejection_code,
        Some("policy_denied".to_owned())
    );
    let rows = alice.read_rows("comments").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, "comment-1");
    assert!(alice
        .poll_subscription(&mut subscription)
        .unwrap()
        .is_empty());
}

#[test]
fn multi_row_transaction_rejects_atomically_when_one_policy_check_fails() {
    let schema = SchemaDef::new()
        .table("docs", |table| {
            table.text("title");
            table.read_if_created_by_user();
        })
        .table("comments", |table| {
            table.text("body");
            table.ref_("doc", "docs");
            table.write_if_ref_readable("doc");
        });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut bob = Runtime::open_with_schema(Storage::Memory, "bob-node", "bob", schema).unwrap();

    alice
        .insert_row(
            "docs",
            "doc-alice",
            BTreeMap::from([("title".to_owned(), json!("Alice doc"))]),
        )
        .unwrap();
    bob.insert_row(
        "docs",
        "doc-bob",
        BTreeMap::from([("title".to_owned(), json!("Bob doc"))]),
    )
    .unwrap();
    alice
        .apply_bundle(&bob.export_table_history("docs").unwrap())
        .unwrap();

    let tx = alice
        .transaction()
        .insert_row(
            "comments",
            "comment-allowed",
            BTreeMap::from([
                ("body".to_owned(), json!("Would be allowed alone")),
                ("doc".to_owned(), json!("doc-alice")),
            ]),
        )
        .insert_row(
            "comments",
            "comment-denied",
            BTreeMap::from([
                ("body".to_owned(), json!("Rejects whole transaction")),
                ("doc".to_owned(), json!("doc-bob")),
            ]),
        )
        .commit()
        .unwrap();

    assert_eq!(
        alice.transaction_info(&tx).unwrap().rejection_code,
        Some("policy_denied".to_owned())
    );
    assert!(alice.read_rows("comments").unwrap().is_empty());
    assert_eq!(
        alice.transaction_write_rows(&tx).unwrap(),
        vec![
            ("comments".to_owned(), "comment-allowed".to_owned()),
            ("comments".to_owned(), "comment-denied".to_owned())
        ]
    );
}

#[test]
fn trusted_admin_write_bypasses_policy_but_preserves_author_provenance() {
    let schema = SchemaDef::new()
        .table("docs", |table| {
            table.text("title");
            table.read_if_created_by_user();
        })
        .table("comments", |table| {
            table.text("body");
            table.ref_("doc", "docs");
            table.write_if_ref_readable("doc");
        });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut admin =
        Runtime::open_trusted_attributing_to_user(Storage::Memory, "admin-node", "service", schema)
            .unwrap();

    alice
        .insert_row(
            "docs",
            "doc-alice",
            BTreeMap::from([("title".to_owned(), json!("Alice doc"))]),
        )
        .unwrap();
    admin
        .apply_bundle(&alice.export_table_history("docs").unwrap())
        .unwrap();
    let tx = admin
        .insert_row(
            "comments",
            "comment-service",
            BTreeMap::from([
                ("body".to_owned(), json!("Inserted by service")),
                ("doc".to_owned(), json!("doc-alice")),
            ]),
        )
        .unwrap();

    let rows = admin.read_rows("comments").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].created_by, "service");
    assert_eq!(rows[0].tx_id, tx);
    assert_eq!(admin.transaction_info(&tx).unwrap().rejection_code, None);
}

#[test]
fn trusted_as_user_enforces_policy_while_attribution_mode_bypasses_it() {
    let schema = SchemaDef::new().table("docs", |table| {
        table.text("title");
        table.write_if_created_by_user();
    });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut worker =
        Runtime::open_trusted_with_schema(Storage::Memory, "worker-node", schema).unwrap();

    alice
        .insert_row(
            "docs",
            "doc-alice",
            BTreeMap::from([("title".to_owned(), json!("Alice doc"))]),
        )
        .unwrap();
    worker
        .apply_bundle(&alice.export_table_history("docs").unwrap())
        .unwrap();

    let denied_tx = support::run_as_user(&mut worker, "bob", |worker| {
        worker.update_row(
            "docs",
            "doc-alice",
            BTreeMap::from([("title".to_owned(), json!("Denied"))]),
        )
    })
    .unwrap();
    assert_eq!(
        worker.transaction_info(&denied_tx).unwrap().rejection_code,
        Some("policy_denied".to_owned())
    );
    assert_eq!(
        worker
            .query(support::eq_query("docs", "id", json!("doc-alice")))
            .unwrap()[0]
            .values["title"],
        json!("Alice doc")
    );

    let accepted_tx = support::run_attributing_to_user(&mut worker, "bob", |worker| {
        worker.update_row(
            "docs",
            "doc-alice",
            BTreeMap::from([("title".to_owned(), json!("Privileged edit"))]),
        )
    })
    .unwrap();
    let row = worker
        .query(support::eq_query("docs", "id", json!("doc-alice")))
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(row.values["title"], json!("Privileged edit"));
    assert_eq!(row.created_by, "alice");
    let history = worker.export_table_history("docs").unwrap();
    let accepted_history = history
        .history
        .iter()
        .find(|record| record.tx_id == accepted_tx)
        .unwrap();
    assert_eq!(accepted_history.updated_by, "bob");
    assert_eq!(
        worker
            .transaction_info(&accepted_tx)
            .unwrap()
            .rejection_code,
        None
    );
}

#[test]
fn recursive_write_policy_records_transitive_policy_read_set() {
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
    let mut alice = Runtime::open_trusted_with_session_user(
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

    edge.apply_untrusted_bundle_as_user(&bundle, alice.session_user())
        .unwrap();

    let rows = edge.read_rows("todos").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, "todo-1");
    assert_eq!(edge.transaction_info(&tx).unwrap().rejection_code, None);
}

#[test]
fn trusted_edge_reports_missing_transitive_policy_dependency() {
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
            table.write_if_ref_readable("project");
        });
    let mut alice = Runtime::open_trusted_with_session_user(
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
                ("title".to_owned(), json!("Missing grandparent")),
                ("project".to_owned(), json!("project-1")),
            ]),
        )
        .unwrap();
    let mut bundle = alice.export_table_history("todos").unwrap();
    bundle
        .history
        .retain(|record| !(record.table == "orgs" && record.row_id == "org-1"));

    edge.apply_untrusted_bundle_as_user(&bundle, alice.session_user())
        .unwrap();

    assert!(edge.read_rows("todos").unwrap().is_empty());
    assert_eq!(
        edge.transaction_info(&tx).unwrap().awaiting_dependency,
        Some(json!({
            "reason": "policy_dependency_unavailable",
            "table": "todos",
            "row_id": "todo-1",
            "dependency_table": "orgs",
            "dependency_row_id": "org-1"
        }))
    );

    edge.apply_bundle(&alice.export_table_history("orgs").unwrap())
        .unwrap();

    let rows = edge.read_rows("todos").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, "todo-1");
    let info = edge.transaction_info(&tx).unwrap();
    assert_eq!(info.rejection_code, None);
    assert_eq!(info.awaiting_dependency, None);
    assert_eq!(info.receipt_tiers, vec!["edge".to_owned()]);
}

#[test]
fn policy_read_set_survives_sync() {
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
            table.read_if_created_by_user();
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
