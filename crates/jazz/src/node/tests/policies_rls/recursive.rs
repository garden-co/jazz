// Recursive, reverse-reference, and public-default policy behavior.

fn recursive_doc_access_policy() -> PublicPolicyExpr {
    crate::test_public_schema::seeded_recursive_access_policy(
        "doc_access",
        "doc",
        "team",
        &[],
        &[],
        "teams",
        "team_edges",
        "member",
        "parent",
        &[],
        "teams",
        "id",
        &["claims", "sub"],
        "id",
    )
}

fn recursive_doc_policy_schema(select_policy: PublicPolicyExpr) -> JazzSchema {
    let policy = recursive_doc_access_policy();
    let protected = PublicTablePolicies::new()
        .with_select(select_policy)
        .with_insert(policy.clone())
        .with_update(Some(policy.clone()), policy.clone())
        .with_delete(policy);

    build_public_test_schema(
        PublicSchemaBuilder::new()
            .table(
                PublicTableSchemaBuilder::new("docs")
                    .column("title", PublicColumnType::Text)
                    .column("kind", PublicColumnType::Text)
                    .policies(protected),
            )
            .table(
                PublicTableSchemaBuilder::new("teams")
                    .column("id", PublicColumnType::Uuid)
                    .column("name", PublicColumnType::Text)
                    .policies(public_all_policies()),
            )
            .table(
                PublicTableSchemaBuilder::new("doc_access")
                    .fk_column("doc", "docs")
                    .fk_column("team", "teams")
                    .policies(public_all_policies()),
            )
            .table(
                PublicTableSchemaBuilder::new("team_edges")
                    .fk_column("member", "teams")
                    .fk_column("parent", "teams")
                    .policies(public_all_policies()),
            ),
    )
}

fn recursive_doc_write_policy_schema() -> JazzSchema {
    recursive_doc_policy_schema(PublicPolicyExpr::True)
}

fn recursive_doc_cells(title: &str, kind: &str) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("title".to_owned(), Value::String(title.to_owned())),
        ("kind".to_owned(), Value::String(kind.to_owned())),
    ])
}

#[test]
fn recursive_reachable_write_policy_allows_direct_and_closure_docs() {
    let schema = recursive_doc_write_policy_schema();
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    let reader = user(0xb2);
    let direct_doc = RowUuid(uuid::uuid!("10000000-0000-0000-0000-000000000001"));
    let closure_doc = RowUuid(uuid::uuid!("10000000-0000-0000-0000-000000000002"));
    let hidden_doc = RowUuid(uuid::uuid!("10000000-0000-0000-0000-000000000003"));
    let parent_team = RowUuid(uuid::uuid!("20000000-0000-0000-0000-000000000002"));
    let hidden_team = RowUuid(uuid::uuid!("20000000-0000-0000-0000-000000000003"));

    for (team, name) in [
        (RowUuid(reader.0), "reader"),
        (parent_team, "parent"),
        (hidden_team, "hidden"),
    ] {
        accept_global(
            &mut core,
            MergeableCommit::new("teams", team, 10).cells(BTreeMap::from([
                ("id".to_owned(), Value::Uuid(team.0)),
                ("name".to_owned(), Value::String(name.to_owned())),
            ])),
        );
    }
    for (doc, title, kind) in [
        (direct_doc, "direct", "visible"),
        (closure_doc, "closure", "visible"),
        (hidden_doc, "hidden", "hidden"),
    ] {
        accept_global(
            &mut core,
            MergeableCommit::new("docs", doc, 20).cells(recursive_doc_cells(title, kind)),
        );
    }
    for (idx, doc, team) in [
        (0xa1, direct_doc, RowUuid(reader.0)),
        (0xa2, closure_doc, parent_team),
        (0xa3, hidden_doc, hidden_team),
    ] {
        accept_global(
            &mut core,
            MergeableCommit::new("doc_access", row(idx), 30).cells(BTreeMap::from([
                ("doc".to_owned(), Value::Uuid(doc.0)),
                ("team".to_owned(), Value::Uuid(team.0)),
            ])),
        );
    }
    accept_global(
        &mut core,
        MergeableCommit::new("team_edges", row(0xe1), 40).cells(BTreeMap::from([
            ("member".to_owned(), Value::Uuid(reader.0)),
            ("parent".to_owned(), Value::Uuid(parent_team.0)),
        ])),
    );

    core.reset_query_engine_read_metrics();
    assert!(core.dry_run_write_current_allows("docs", direct_doc, reader).unwrap());
    let direct_metrics = core.query_engine_read_metrics().clone();
    assert_eq!(
        direct_metrics.source_primary_key_scans, 1,
        "dry-run update probe should point-scan the proposed row source"
    );
    assert!(core.dry_run_write_current_allows("docs", closure_doc, reader).unwrap());
    assert!(!core.dry_run_write_current_allows("docs", hidden_doc, reader).unwrap());
}

#[test]
fn update_policy_point_check_does_not_scan_unrelated_current_rows() {
    // Internal storage metrics are required because the defect is cost-only:
    // the public authorization answer is correct while its work is O(table).
    let schema = build_public_test_schema(
        PublicSchemaBuilder::new().table(
            PublicTableSchemaBuilder::new("docs").column("title", PublicColumnType::Text),
        ),
    );
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    let target = row(0x20);
    for index in 0..40_u8 {
        accept_global(
            &mut core,
            MergeableCommit::new("docs", row(index), u64::from(index) + 1).cells(
                BTreeMap::from([(
                    "title".to_owned(),
                    Value::String(format!("doc {index}")),
                )]),
            ),
        );
    }

    core.reset_storage_read_metrics();
    let _allowed = core
        .dry_run_write_current_allows("docs", target, user(0xb2))
        .unwrap();
    let reads = core.take_storage_read_metrics();

    assert!(
        reads.global_current_rows.reads <= 2,
        "one point authorization read touched {} current rows",
        reads.global_current_rows.reads
    );
}

#[test]
fn recursive_reachable_insert_policy_allows_direct_and_closure_docs() {
    // Internal node coverage is intentional here: this pins sync-unit admission
    // fates for proposed insert rows before the public client layer has a
    // matching recursive write-policy fixture.
    let schema = recursive_doc_write_policy_schema();
    let (_writer_dir, mut writer) = open_node_with_schema(node(1), schema.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    let reader = user(0xb2);
    let direct_doc = RowUuid(uuid::uuid!("10000000-0000-0000-0000-000000000011"));
    let closure_doc = RowUuid(uuid::uuid!("10000000-0000-0000-0000-000000000012"));
    let hidden_doc = RowUuid(uuid::uuid!("10000000-0000-0000-0000-000000000013"));
    let parent_team = RowUuid(uuid::uuid!("20000000-0000-0000-0000-000000000012"));
    let hidden_team = RowUuid(uuid::uuid!("20000000-0000-0000-0000-000000000013"));

    for (team, name) in [
        (RowUuid(reader.0), "reader"),
        (parent_team, "parent"),
        (hidden_team, "hidden"),
    ] {
        accept_global(
            &mut core,
            MergeableCommit::new("teams", team, 10).cells(BTreeMap::from([
                ("id".to_owned(), Value::Uuid(team.0)),
                ("name".to_owned(), Value::String(name.to_owned())),
            ])),
        );
    }
    for (idx, doc, team) in [
        (0xb1, direct_doc, RowUuid(reader.0)),
        (0xb2, closure_doc, parent_team),
        (0xb3, hidden_doc, hidden_team),
    ] {
        accept_global(
            &mut core,
            MergeableCommit::new("doc_access", row(idx), 30).cells(BTreeMap::from([
                ("doc".to_owned(), Value::Uuid(doc.0)),
                ("team".to_owned(), Value::Uuid(team.0)),
            ])),
        );
    }
    accept_global(
        &mut core,
        MergeableCommit::new("team_edges", row(0xe2), 40).cells(BTreeMap::from([
            ("member".to_owned(), Value::Uuid(reader.0)),
            ("parent".to_owned(), Value::Uuid(parent_team.0)),
        ])),
    );

    for (doc, title, expected_fate) in [
        (direct_doc, "direct insert", Fate::Accepted),
        (closure_doc, "closure insert", Fate::Accepted),
        (
            hidden_doc,
            "hidden insert",
            Fate::Rejected(RejectionReason::AuthorizationDenied),
        ),
    ] {
        let (tx_id, unit) = writer
            .commit_mergeable_unit_settled(
                MergeableCommit::new("docs", doc, 50)
                    .made_by(reader)
                    .cells(recursive_doc_cells(title, "inserted")),
            )
            .unwrap();
        let [fate] = core.apply_sync_message_settled(unit).unwrap().try_into().unwrap();
        assert_eq!(
            fate,
            SyncMessage::FateUpdate {
                tx_id,
                fate: expected_fate.clone(),
                global_time: matches!(expected_fate, Fate::Accepted)
                    .then_some(core.clock.committed_global_time),
                durability: matches!(expected_fate, Fate::Accepted)
                    .then_some(DurabilityTier::Global),
            }
        );
    }

    assert_eq!(
        core.current_rows("docs", DurabilityTier::Global)
            .unwrap()
            .into_iter()
            .map(|row| row.row_uuid())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([direct_doc, closure_doc])
    );
}

#[test]
fn recursive_reachable_read_policy_claim_seed_rehydrates_through_query_engine() {
    let schema = recursive_doc_policy_schema(recursive_doc_access_policy());
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    let reader = user(0xb2);
    let direct_doc = RowUuid(uuid::uuid!("10000000-0000-0000-0000-000000000001"));
    let closure_doc = RowUuid(uuid::uuid!("10000000-0000-0000-0000-000000000002"));
    let hidden_doc = RowUuid(uuid::uuid!("10000000-0000-0000-0000-000000000003"));
    let parent_team = RowUuid(uuid::uuid!("20000000-0000-0000-0000-000000000002"));
    let hidden_team = RowUuid(uuid::uuid!("20000000-0000-0000-0000-000000000003"));

    for (team, name) in [
        (RowUuid(reader.0), "reader"),
        (parent_team, "parent"),
        (hidden_team, "hidden"),
    ] {
        accept_global(
            &mut core,
            MergeableCommit::new("teams", team, 10).cells(BTreeMap::from([
                ("id".to_owned(), Value::Uuid(team.0)),
                ("name".to_owned(), Value::String(name.to_owned())),
            ])),
        );
    }
    for (doc, title, kind, tx_time) in [
        (direct_doc, "direct", "visible", 20),
        (closure_doc, "closure", "visible", 21),
        (hidden_doc, "hidden", "hidden", 22),
    ] {
        accept_global(
            &mut core,
            MergeableCommit::new("docs", doc, tx_time).cells(recursive_doc_cells(title, kind)),
        );
    }
    for (idx, doc, team) in [
        (0xa1, direct_doc, RowUuid(reader.0)),
        (0xa2, closure_doc, parent_team),
        (0xa3, hidden_doc, hidden_team),
    ] {
        accept_global(
            &mut core,
            MergeableCommit::new("doc_access", row(idx), 30).cells(BTreeMap::from([
                ("doc".to_owned(), Value::Uuid(doc.0)),
                ("team".to_owned(), Value::Uuid(team.0)),
            ])),
        );
    }
    accept_global(
        &mut core,
        MergeableCommit::new("team_edges", row(0xe1), 40).cells(BTreeMap::from([
            ("member".to_owned(), Value::Uuid(reader.0)),
            ("parent".to_owned(), Value::Uuid(parent_team.0)),
        ])),
    );

    let shape = Query::from("docs").validate(&core.catalogue.schema).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let mut peer = PeerState::client_link(reader);
    let update = peer.rehydrate_query(&mut core, &shape, &binding).unwrap();
    let (adds, removes) = canonical_view_update_rows(&update);

    assert_eq!(
        adds,
        vec![
            (
                "docs".to_owned().into(),
                direct_doc,
                TxId::new(TxTime::from(20), node(9)),
            ),
            (
                "docs".to_owned().into(),
                closure_doc,
                TxId::new(TxTime::from(21), node(9)),
            ),
        ]
    );
    assert!(removes.is_empty());
}

#[test]
fn reverse_referencing_select_policy_allows_root_row_through_source_row() {
    let schema = build_public_test_schema(
        PublicSchemaBuilder::new()
            .table(
                PublicTableSchemaBuilder::new("files")
                    .column("name", PublicColumnType::Text)
                    .policies(public_all_policies().with_select(public_outer_exists(
                        "attachments",
                        "fileId",
                        "id",
                        [public_claim_eq("ownerId", "user_id")],
                    ))),
            )
            .table(
                PublicTableSchemaBuilder::new("attachments")
                    .fk_column("fileId", "files")
                    .column("ownerId", PublicColumnType::Text)
                    .policies(public_all_policies()),
            ),
    );
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    let alice = user(0xa1);
    let bob = user(0xb2);
    let alice_file = row(0xf1);
    let unlinked_file = row(0xf2);

    for (file, name) in [(alice_file, "alice"), (unlinked_file, "unlinked")] {
        accept_global(
            &mut core,
            MergeableCommit::new("files", file, 10).cells(BTreeMap::from([(
                "name".to_owned(),
                Value::String(name.to_owned()),
            )])),
        );
    }
    accept_global(
        &mut core,
        MergeableCommit::new("attachments", row(0xa7), 20).cells(BTreeMap::from([
            ("fileId".to_owned(), Value::Uuid(alice_file.0)),
            ("ownerId".to_owned(), Value::String(alice.0.to_string())),
        ])),
    );

    assert!(
        core.dry_run_read_current_allows("files", alice_file, alice)
            .unwrap()
    );
    assert!(
        !core
            .dry_run_read_current_allows("files", alice_file, bob)
            .unwrap()
    );
    assert!(
        !core
            .dry_run_read_current_allows("files", unlinked_file, alice)
            .unwrap()
    );
}

#[test]
fn unbound_is_admin_claim_in_read_policy_denies_as_false() {
    let schema = build_public_test_schema(PublicSchemaBuilder::new().table(
        PublicTableSchemaBuilder::new("todos")
            .column("title", PublicColumnType::Text)
            .column("requiresAdmin", PublicColumnType::Boolean)
            .policies(
                PublicTablePolicies::new()
                    .with_select(public_claim_eq("requiresAdmin", "isAdmin")),
            ),
    ));
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    let tx = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row(0x88), 10).cells(BTreeMap::from([
                ("title".to_owned(), v("admin")),
                ("requiresAdmin".to_owned(), Value::Bool(true)),
            ])),
        )
        .unwrap();
    core.apply_fate_update(
        tx,
        Fate::Accepted,
        Some(GlobalTime(1)),
        Some(DurabilityTier::Global),
    )
    .unwrap();
    let mut edge = PeerState::edge_client(user(0xa1));
    assert_view_update_only_references_rows(
        &edge.current_rows_update(&mut core, "todos").unwrap(),
        BTreeSet::new(),
    );
}

#[test]
fn missing_read_or_write_policy_is_public_for_that_operation() {
    let schema = build_public_test_schema(
        PublicSchemaBuilder::new().table(
            PublicTableSchemaBuilder::new("todos")
                .column("title", PublicColumnType::Text)
                .column("owner", PublicColumnType::Uuid),
        ),
    );
    let (_writer_dir, mut writer) = open_node_with_schema(node(1), schema.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    let (_tx_id, unit) = writer
        .commit_mergeable_unit_settled(
            MergeableCommit::new("todos", row(0x85), 10)
                .made_by(user(0xa1))
                .cells(owner_cells(user(0xb2), "public write")),
        )
        .unwrap();
    let [fate] = core.apply_sync_message_settled(unit).unwrap().try_into().unwrap();
    assert!(matches!(
        fate,
        SyncMessage::FateUpdate {
            fate: Fate::Accepted,
            ..
        }
    ));

    let mut edge = PeerState::edge_client(user(0xcc));
    assert_view_update_only_references_rows(
        &edge.current_rows_update(&mut core, "todos").unwrap(),
        BTreeSet::from([row(0x85)]),
    );
}
