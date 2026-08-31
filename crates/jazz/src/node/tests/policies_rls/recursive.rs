// Recursive, reverse-reference, and closed-policy-set behavior.

fn recursive_doc_access_policy() -> PublicPolicyExpr {
    crate::test_public_schema::seeded_recursive_access_policy(
        "doc_access",
        "doc",
        "team",
        &[],
        &[],
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

fn scalar_frontier_doc_access_policy(max_depth: usize) -> PublicPolicyExpr {
    let mut policy = crate::test_public_schema::seeded_recursive_access_policy(
        "doc_access",
        "doc",
        "team",
        &[],
        &[],
        "team_edges",
        "member",
        "parent",
        &[("enabled", PublicValue::Boolean(true))],
        "user_team_edges",
        "user_id",
        &["claims", "sub"],
        "team",
    );
    let PublicPolicyExpr::ExistsRel { rel } = &mut policy else {
        unreachable!("seeded recursive policy is an ExistsRel");
    };
    let crate::tools::public_schema::RelExpr::Filter { input, .. } = rel else {
        unreachable!("seeded recursive policy correlates its access join");
    };
    let crate::tools::public_schema::RelExpr::Join { left, .. } = input.as_mut() else {
        unreachable!("seeded recursive policy joins its scalar frontier to access");
    };
    let crate::tools::public_schema::RelExpr::Gather { bound, .. } = left.as_mut() else {
        unreachable!("seeded recursive policy starts from Gather");
    };
    *bound = crate::tools::public_schema::RelRecursionBound::MaxDepth(max_depth);
    policy
}

fn projected_frontier_doc_policy_schema(max_depth: usize) -> JazzSchema {
    let policy = scalar_frontier_doc_access_policy(max_depth);
    let protected = PublicTablePolicies::new()
        .with_select(policy.clone())
        .with_insert(policy.clone())
        .with_update(Some(policy.clone()), policy.clone())
        .with_delete(policy);
    let deny = || PublicTablePolicies::new().with_select(PublicPolicyExpr::False);
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
                    .column("name", PublicColumnType::Text)
                    .policies(deny()),
            )
            .table(
                PublicTableSchemaBuilder::new("user_team_edges")
                    .column("user_id", PublicColumnType::Uuid)
                    .fk_column("team", "teams")
                    .policies(deny()),
            )
            .table(
                PublicTableSchemaBuilder::new("doc_access")
                    .fk_column("doc", "docs")
                    .fk_column("team", "teams")
                    .policies(deny()),
            )
            .table(
                PublicTableSchemaBuilder::new("team_edges")
                    .fk_column("member", "teams")
                    .fk_column("parent", "teams")
                    .column("enabled", PublicColumnType::Boolean)
                    .policies(deny()),
            ),
    )
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
    core.set_test_provider_claims(
        reader,
        BTreeMap::from([(
            crate::query::provider_claim_key("sub"),
            Value::Uuid(reader.test_uuid()),
        )]),
    );
    let direct_doc = RowUuid(uuid::uuid!("10000000-0000-0000-0000-000000000001"));
    let closure_doc = RowUuid(uuid::uuid!("10000000-0000-0000-0000-000000000002"));
    let hidden_doc = RowUuid(uuid::uuid!("10000000-0000-0000-0000-000000000003"));
    let parent_team = RowUuid(uuid::uuid!("20000000-0000-0000-0000-000000000002"));
    let hidden_team = RowUuid(uuid::uuid!("20000000-0000-0000-0000-000000000003"));

    for (team, name) in [
        (RowUuid(reader.test_uuid()), "reader"),
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
        (0xa1, direct_doc, RowUuid(reader.test_uuid())),
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
            ("member".to_owned(), Value::Uuid(reader.test_uuid())),
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
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema.clone());
    let reader = user(0xb2);
    core.set_test_provider_claims(
        reader,
        BTreeMap::from([(
            crate::query::provider_claim_key("sub"),
            Value::Uuid(reader.test_uuid()),
        )]),
    );
    let direct_doc = RowUuid(uuid::uuid!("10000000-0000-0000-0000-000000000011"));
    let closure_doc = RowUuid(uuid::uuid!("10000000-0000-0000-0000-000000000012"));
    let hidden_doc = RowUuid(uuid::uuid!("10000000-0000-0000-0000-000000000013"));
    let parent_team = RowUuid(uuid::uuid!("20000000-0000-0000-0000-000000000012"));
    let hidden_team = RowUuid(uuid::uuid!("20000000-0000-0000-0000-000000000013"));

    for (team, name) in [
        (RowUuid(reader.test_uuid()), "reader"),
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
        (0xb1, direct_doc, RowUuid(reader.test_uuid())),
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
            ("member".to_owned(), Value::Uuid(reader.test_uuid())),
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
    core.set_test_provider_claims(
        reader,
        BTreeMap::from([(
            crate::query::provider_claim_key("sub"),
            Value::Uuid(reader.test_uuid()),
        )]),
    );
    let direct_doc = RowUuid(uuid::uuid!("10000000-0000-0000-0000-000000000001"));
    let closure_doc = RowUuid(uuid::uuid!("10000000-0000-0000-0000-000000000002"));
    let hidden_doc = RowUuid(uuid::uuid!("10000000-0000-0000-0000-000000000003"));
    let parent_team = RowUuid(uuid::uuid!("20000000-0000-0000-0000-000000000002"));
    let hidden_team = RowUuid(uuid::uuid!("20000000-0000-0000-0000-000000000003"));

    for (team, name) in [
        (RowUuid(reader.test_uuid()), "reader"),
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
        (0xa1, direct_doc, RowUuid(reader.test_uuid())),
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
            ("member".to_owned(), Value::Uuid(reader.test_uuid())),
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
fn projected_frontier_authorizes_unseeded_parent_and_terminates_team_cycle() {
    let schema = projected_frontier_doc_policy_schema(2);
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    let reader = user(0xb2);
    core.set_test_provider_claims(
        reader,
        BTreeMap::from([("sub".to_owned(), Value::Uuid(reader.test_uuid()))]),
    );
    let child_team = RowUuid(uuid::uuid!("20000000-0000-0000-0000-000000000011"));
    let parent_team = RowUuid(uuid::uuid!("20000000-0000-0000-0000-000000000012"));
    let hidden_team = RowUuid(uuid::uuid!("20000000-0000-0000-0000-000000000013"));
    let child_doc = RowUuid(uuid::uuid!("10000000-0000-0000-0000-000000000011"));
    let parent_doc = RowUuid(uuid::uuid!("10000000-0000-0000-0000-000000000012"));
    let hidden_doc = RowUuid(uuid::uuid!("10000000-0000-0000-0000-000000000013"));

    for (team, name) in [
        (child_team, "child"),
        (parent_team, "parent"),
        (hidden_team, "hidden"),
    ] {
        accept_global(
            &mut core,
            MergeableCommit::new("teams", team, 10).cells(BTreeMap::from([(
                "name".to_owned(),
                Value::String(name.to_owned()),
            )])),
        );
    }
    for (doc, title, tx_time) in [
        (child_doc, "seed team", 20),
        (parent_doc, "reachable parent", 21),
        (hidden_doc, "hidden", 22),
    ] {
        accept_global(
            &mut core,
            MergeableCommit::new("docs", doc, tx_time)
                .cells(recursive_doc_cells(title, "visible")),
        );
    }
    for (index, doc, team) in [
        (0xa1, child_doc, child_team),
        (0xa2, parent_doc, parent_team),
        (0xa3, hidden_doc, hidden_team),
    ] {
        accept_global(
            &mut core,
            MergeableCommit::new("doc_access", row(index), 30).cells(BTreeMap::from([
                ("doc".to_owned(), Value::Uuid(doc.0)),
                ("team".to_owned(), Value::Uuid(team.0)),
            ])),
        );
    }
    accept_global(
        &mut core,
        MergeableCommit::new("user_team_edges", row(0xd1), 35).cells(BTreeMap::from([
            ("user_id".to_owned(), Value::Uuid(reader.test_uuid())),
            ("team".to_owned(), Value::Uuid(child_team.0)),
        ])),
    );
    for (edge, member, parent) in [
        (0xe1, child_team, parent_team),
        (0xe2, parent_team, child_team),
    ] {
        accept_global(
            &mut core,
            MergeableCommit::new("team_edges", row(edge), 40).cells(BTreeMap::from([
                ("member".to_owned(), Value::Uuid(member.0)),
                ("parent".to_owned(), Value::Uuid(parent.0)),
                ("enabled".to_owned(), Value::Bool(true)),
            ])),
        );
    }

    let shape = Query::from("docs").validate(&core.catalogue.schema).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let mut peer = PeerState::client_link(reader);
    let update = peer.rehydrate_query(&mut core, &shape, &binding).unwrap();
    let (adds, removes) = canonical_view_update_rows(&update);
    let visible = adds
        .into_iter()
        .map(|(_, row, _)| row)
        .collect::<BTreeSet<_>>();

    assert_eq!(visible, BTreeSet::from([child_doc, parent_doc]));
    assert!(removes.is_empty());
}

fn projected_frontier_visibility_at_depth(
    max_depth: usize,
) -> (BTreeSet<RowUuid>, RowUuid, RowUuid) {
    let schema = projected_frontier_doc_policy_schema(max_depth);
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    let reader = user(0xb2);
    core.set_test_provider_claims(
        reader,
        BTreeMap::from([("sub".to_owned(), Value::Uuid(reader.test_uuid()))]),
    );
    let child_team = row(0x21);
    let parent_team = row(0x22);
    let seed_doc = row(0x31);
    let parent_doc = row(0x32);

    for (team, name) in [(child_team, "child"), (parent_team, "parent")] {
        accept_global(
            &mut core,
            MergeableCommit::new("teams", team, 10).cells(BTreeMap::from([(
                "name".to_owned(),
                Value::String(name.to_owned()),
            )])),
        );
    }
    for (doc, title, team) in [
        (seed_doc, "seed", child_team),
        (parent_doc, "one hop", parent_team),
    ] {
        accept_global(
            &mut core,
            MergeableCommit::new("docs", doc, 20)
                .cells(recursive_doc_cells(title, "visible")),
        );
        accept_global(
            &mut core,
            MergeableCommit::new("doc_access", doc, 30).cells(BTreeMap::from([
                ("doc".to_owned(), Value::Uuid(doc.0)),
                ("team".to_owned(), Value::Uuid(team.0)),
            ])),
        );
    }
    accept_global(
        &mut core,
        MergeableCommit::new("user_team_edges", row(0xd2), 35).cells(BTreeMap::from([
            ("user_id".to_owned(), Value::Uuid(reader.test_uuid())),
            ("team".to_owned(), Value::Uuid(child_team.0)),
        ])),
    );
    accept_global(
        &mut core,
        MergeableCommit::new("team_edges", row(0xe3), 40).cells(BTreeMap::from([
            ("member".to_owned(), Value::Uuid(child_team.0)),
            ("parent".to_owned(), Value::Uuid(parent_team.0)),
            ("enabled".to_owned(), Value::Bool(true)),
        ])),
    );

    let shape = Query::from("docs").validate(&core.catalogue.schema).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let mut peer = PeerState::client_link(reader);
    let update = peer.rehydrate_query(&mut core, &shape, &binding).unwrap();
    let (adds, removes) = canonical_view_update_rows(&update);
    assert!(removes.is_empty());
    (
        adds
            .into_iter()
            .map(|(_, row, _)| row)
            .collect::<BTreeSet<_>>(),
        seed_doc,
        parent_doc,
    )
}

#[test]
fn max_depth_zero_is_seed_only_and_one_adds_exactly_one_authorization_hop() {
    let (zero_visible, seed_doc, parent_doc) = projected_frontier_visibility_at_depth(0);
    assert_eq!(zero_visible, BTreeSet::from([seed_doc]));
    assert!(!zero_visible.contains(&parent_doc));

    let (one_visible, seed_doc, parent_doc) = projected_frontier_visibility_at_depth(1);
    assert_eq!(one_visible, BTreeSet::from([seed_doc, parent_doc]));
}

#[test]
fn scalar_frontier_policy_maintains_raw_evidence_without_disclosing_dependencies() {
    let schema = projected_frontier_doc_policy_schema(2);
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    let reader = user(0xb2);
    core.set_test_provider_claims(
        reader,
        BTreeMap::from([("sub".to_owned(), Value::Uuid(reader.test_uuid()))]),
    );

    let team_a = row(0x11);
    let team_b = row(0x12);
    let team_c = row(0x13);
    let team_d = row(0x14);
    let team_filtered = row(0x15);
    let team_edge_grant = row(0x16);
    let doc_a = row(0x21);
    let doc_b = row(0x22);
    let doc_c = row(0x23);
    let doc_d = row(0x24);
    let doc_filtered = row(0x25);
    let doc_edge_grant = row(0x26);

    for (index, team) in [
        team_a,
        team_b,
        team_c,
        team_d,
        team_filtered,
        team_edge_grant,
    ]
    .into_iter()
    .enumerate()
    {
        accept_global(
            &mut core,
            MergeableCommit::new("teams", team, 10 + index as u64).cells(BTreeMap::from([(
                "name".to_owned(),
                Value::String(format!("team {index}")),
            )])),
        );
    }
    for (index, (doc, team)) in [
        (doc_a, team_a),
        (doc_b, team_b),
        (doc_c, team_c),
        (doc_d, team_d),
        (doc_filtered, team_filtered),
        (doc_edge_grant, team_edge_grant),
    ]
    .into_iter()
    .enumerate()
    {
        accept_global(
            &mut core,
            MergeableCommit::new("docs", doc, 20 + index as u64)
                .cells(recursive_doc_cells(&format!("doc {index}"), "frontier")),
        );
        accept_global(
            &mut core,
            MergeableCommit::new("doc_access", row(0x31 + index as u8), 30 + index as u64)
                .cells(BTreeMap::from([
                    ("doc".to_owned(), Value::Uuid(doc.0)),
                    ("team".to_owned(), Value::Uuid(team.0)),
                ])),
        );
    }
    for (edge, member, parent, enabled, time) in [
        (row(0x41), team_a, team_b, true, 40),
        (row(0x42), team_b, team_c, true, 41),
        (row(0x43), team_c, team_d, true, 42),
        (row(0x44), team_a, team_filtered, false, 43),
        (row(0x45), team_c, team_a, true, 44),
    ] {
        accept_global(
            &mut core,
            MergeableCommit::new("team_edges", edge, time).cells(BTreeMap::from([
                ("member".to_owned(), Value::Uuid(member.0)),
                ("parent".to_owned(), Value::Uuid(parent.0)),
                ("enabled".to_owned(), Value::Bool(enabled)),
            ])),
        );
    }

    // Dependency rows are raw authorization evidence only. Each dependency
    // table has an explicit deny policy and remains empty through an ordinary
    // client read even though its rows can authorize the outer document.
    for dependency in ["teams", "user_team_edges", "team_edges", "doc_access"] {
        let mut evidence_peer = PeerState::edge_client(reader);
        let update = evidence_peer
            .current_rows_update(&mut core, dependency)
            .unwrap();
        assert_view_update_only_references_rows(&update, BTreeSet::new());
        assert_view_update_only_ships_rows(&update, BTreeSet::new());
    }

    let shape = Query::from("docs").validate(&core.catalogue.schema).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let mut peer = PeerState::client_link(reader);
    let initial = peer.rehydrate_query(&mut core, &shape, &binding).unwrap();
    assert_eq!(
        canonical_view_update_rows(&initial),
        (Vec::new(), Vec::new())
    );
    assert_view_update_only_ships_rows(&initial, BTreeSet::new());

    let doc_delta = |update: &SyncMessage| {
        let (adds, removes) = canonical_view_update_rows(update);
        assert!(
            adds.iter()
                .chain(removes.iter())
                .all(|(table, _, _)| table.as_str() == "docs"),
            "policy evidence must never become an independently visible result member"
        );
        (
            adds
                .into_iter()
                .map(|(_, row_uuid, _)| row_uuid)
                .collect::<BTreeSet<_>>(),
            removes
                .into_iter()
                .map(|(_, row_uuid, _)| row_uuid)
                .collect::<BTreeSet<_>>(),
        )
    };

    let seed_row = row(0x51);
    let seed_grant = accept_global(
        &mut core,
        MergeableCommit::new("user_team_edges", seed_row, 50).cells(BTreeMap::from([
            ("user_id".to_owned(), Value::Uuid(reader.test_uuid())),
            ("team".to_owned(), Value::Uuid(team_a.0)),
        ])),
    );
    let mut populated_seed_peer = PeerState::edge_client(reader);
    let hidden_seed = populated_seed_peer
        .current_rows_update(&mut core, "user_team_edges")
        .unwrap();
    assert_view_update_only_references_rows(&hidden_seed, BTreeSet::new());
    assert_view_update_only_ships_rows(&hidden_seed, BTreeSet::new());
    let grant = peer.query_update(&mut core, &shape, &binding).unwrap();
    assert_eq!(
        doc_delta(&grant),
        (
            BTreeSet::from([doc_a, doc_b, doc_c]),
            BTreeSet::new()
        ),
        "the seed, depth-N frontier, and cycle are visible; depth N+1 and the filtered edge deny"
    );
    assert_view_update_only_ships_rows(
        &grant,
        BTreeSet::from([doc_a, doc_b, doc_c]),
    );

    let seed_move = accept_global(
        &mut core,
        MergeableCommit::new("user_team_edges", seed_row, 51)
            .parents(vec![seed_grant])
            .cells(BTreeMap::from([
                ("user_id".to_owned(), Value::Uuid(reader.test_uuid())),
                ("team".to_owned(), Value::Uuid(team_d.0)),
            ])),
    );
    let moved = peer.query_update(&mut core, &shape, &binding).unwrap();
    assert_eq!(
        doc_delta(&moved),
        (
            BTreeSet::from([doc_d]),
            BTreeSet::from([doc_a, doc_b, doc_c])
        )
    );
    assert_view_update_only_ships_rows(&moved, BTreeSet::from([doc_d]));

    accept_global(
        &mut core,
        MergeableCommit::new("user_team_edges", seed_row, 52)
            .parents(vec![seed_move])
            .cells(BTreeMap::from([
                ("user_id".to_owned(), Value::Uuid(reader.test_uuid())),
                ("team".to_owned(), Value::Uuid(team_a.0)),
            ])),
    );
    let moved_back = peer.query_update(&mut core, &shape, &binding).unwrap();
    assert_eq!(
        doc_delta(&moved_back),
        (
            BTreeSet::from([doc_a, doc_b, doc_c]),
            BTreeSet::from([doc_d])
        )
    );
    assert_view_update_only_ships_rows(
        &moved_back,
        BTreeSet::from([doc_a, doc_b, doc_c]),
    );

    let granted_edge_row = row(0x52);
    accept_global(
        &mut core,
        MergeableCommit::new("team_edges", granted_edge_row, 53).cells(BTreeMap::from([
            ("member".to_owned(), Value::Uuid(team_a.0)),
            ("parent".to_owned(), Value::Uuid(team_edge_grant.0)),
            ("enabled".to_owned(), Value::Bool(true)),
        ])),
    );
    let edge_grant = peer.query_update(&mut core, &shape, &binding).unwrap();
    assert_eq!(
        doc_delta(&edge_grant),
        (BTreeSet::from([doc_edge_grant]), BTreeSet::new())
    );
    assert_view_update_only_ships_rows(&edge_grant, BTreeSet::from([doc_edge_grant]));

    accept_global(
        &mut core,
        MergeableCommit::new("team_edges", granted_edge_row, 54)
            .deletion(DeletionEvent::Deleted),
    );
    let edge_revoke = peer.query_update(&mut core, &shape, &binding).unwrap();
    assert_eq!(
        doc_delta(&edge_revoke),
        (BTreeSet::new(), BTreeSet::from([doc_edge_grant]))
    );
    assert_view_update_only_ships_rows(&edge_revoke, BTreeSet::new());

    accept_global(
        &mut core,
        MergeableCommit::new("user_team_edges", seed_row, 55)
            .deletion(DeletionEvent::Deleted),
    );
    let seed_revoke = peer.query_update(&mut core, &shape, &binding).unwrap();
    assert_eq!(
        doc_delta(&seed_revoke),
        (
            BTreeSet::new(),
            BTreeSet::from([doc_a, doc_b, doc_c])
        )
    );
    assert_view_update_only_ships_rows(&seed_revoke, BTreeSet::new());
}

#[test]
fn scalar_frontier_read_and_all_write_actions_share_one_relation() {
    let schema = projected_frontier_doc_policy_schema(1);
    let (_writer_dir, mut writer) = open_node_with_schema(node(1), schema.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    let reader = user(0xb2);
    let claims = BTreeMap::from([("sub".to_owned(), Value::Uuid(reader.test_uuid()))]);
    writer.set_test_provider_claims(reader, claims.clone());
    core.set_test_provider_claims(reader, claims);

    let direct_team = row(0x61);
    let closure_team = row(0x62);
    let hidden_team = row(0x63);
    for (index, team) in [direct_team, closure_team, hidden_team]
        .into_iter()
        .enumerate()
    {
        accept_global(
            &mut core,
            MergeableCommit::new("teams", team, 10 + index as u64).cells(BTreeMap::from([(
                "name".to_owned(),
                Value::String(format!("write team {index}")),
            )])),
        );
    }
    accept_global(
        &mut core,
        MergeableCommit::new("user_team_edges", row(0x64), 20).cells(BTreeMap::from([
            ("user_id".to_owned(), Value::Uuid(reader.test_uuid())),
            ("team".to_owned(), Value::Uuid(direct_team.0)),
        ])),
    );
    accept_global(
        &mut core,
        MergeableCommit::new("team_edges", row(0x65), 21).cells(BTreeMap::from([
            ("member".to_owned(), Value::Uuid(direct_team.0)),
            ("parent".to_owned(), Value::Uuid(closure_team.0)),
            ("enabled".to_owned(), Value::Bool(true)),
        ])),
    );

    let update_doc = row(0x66);
    let delete_doc = row(0x67);
    let hidden_doc = row(0x68);
    let allowed_insert = row(0x69);
    let denied_insert = row(0x6a);
    let update_parent = accept_global(
        &mut core,
        MergeableCommit::new("docs", update_doc, 30)
            .cells(recursive_doc_cells("update old", "write")),
    );
    accept_global(
        &mut core,
        MergeableCommit::new("docs", delete_doc, 31)
            .cells(recursive_doc_cells("delete me", "write")),
    );
    let hidden_parent = accept_global(
        &mut core,
        MergeableCommit::new("docs", hidden_doc, 32)
            .cells(recursive_doc_cells("hidden", "write")),
    );
    for (index, (doc, team)) in [
        (update_doc, direct_team),
        (delete_doc, closure_team),
        (hidden_doc, hidden_team),
        (allowed_insert, closure_team),
        (denied_insert, hidden_team),
    ]
    .into_iter()
    .enumerate()
    {
        accept_global(
            &mut core,
            MergeableCommit::new("doc_access", row(0x70 + index as u8), 40 + index as u64)
                .cells(BTreeMap::from([
                    ("doc".to_owned(), Value::Uuid(doc.0)),
                    ("team".to_owned(), Value::Uuid(team.0)),
                ])),
        );
    }

    let shape = Query::from("docs").validate(&core.catalogue.schema).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let mut read_peer = PeerState::client_link(reader);
    let read = read_peer
        .rehydrate_query(&mut core, &shape, &binding)
        .unwrap();
    let (read_adds, read_removes) = canonical_view_update_rows(&read);
    assert_eq!(
        read_adds
            .into_iter()
            .map(|(_, row_uuid, _)| row_uuid)
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([update_doc, delete_doc])
    );
    assert!(read_removes.is_empty());
    assert_view_update_only_ships_rows(&read, BTreeSet::from([update_doc, delete_doc]));

    let mut apply = |commit: MergeableCommit| {
        let (tx_id, unit) = writer.commit_mergeable_unit_settled(commit).unwrap();
        let [receipt] = core
            .apply_sync_message_settled(unit)
            .unwrap()
            .try_into()
            .unwrap();
        let SyncMessage::FateUpdate {
            tx_id: receipt_tx,
            fate,
            ..
        } = receipt
        else {
            panic!("write authorization must produce a fate receipt");
        };
        assert_eq!(receipt_tx, tx_id);
        (tx_id, fate)
    };

    let (_, allowed_insert_fate) = apply(
        MergeableCommit::new("docs", allowed_insert, 50)
            .made_by(reader)
            .cells(recursive_doc_cells("inserted", "write")),
    );
    let (_, denied_insert_fate) = apply(
        MergeableCommit::new("docs", denied_insert, 51)
            .made_by(reader)
            .cells(recursive_doc_cells("denied insert", "write")),
    );
    let (_, allowed_update_fate) = apply(
        MergeableCommit::new("docs", update_doc, 52)
            .made_by(reader)
            .parents(vec![update_parent])
            .cells(recursive_doc_cells("update new", "write")),
    );
    let (_, denied_update_fate) = apply(
        MergeableCommit::new("docs", hidden_doc, 53)
            .made_by(reader)
            .parents(vec![hidden_parent])
            .cells(recursive_doc_cells("denied update", "write")),
    );
    let (_, allowed_delete_fate) = apply(
        MergeableCommit::new("docs", delete_doc, 54)
            .made_by(reader)
            .deletion(DeletionEvent::Deleted),
    );
    let (_, denied_delete_fate) = apply(
        MergeableCommit::new("docs", hidden_doc, 55)
            .made_by(reader)
            .deletion(DeletionEvent::Deleted),
    );
    drop(apply);

    assert_eq!(allowed_insert_fate, Fate::Accepted);
    assert_eq!(allowed_update_fate, Fate::Accepted);
    assert_eq!(allowed_delete_fate, Fate::Accepted);
    for denied in [
        denied_insert_fate,
        denied_update_fate,
        denied_delete_fate,
    ] {
        assert_eq!(
            denied,
            Fate::Rejected(RejectionReason::AuthorizationDenied)
        );
    }

    let mut final_peer = PeerState::client_link(reader);
    let final_read = final_peer
        .rehydrate_query(&mut core, &shape, &binding)
        .unwrap();
    let (final_adds, final_removes) = canonical_view_update_rows(&final_read);
    assert_eq!(
        final_adds
            .into_iter()
            .map(|(_, row_uuid, _)| row_uuid)
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([update_doc, allowed_insert])
    );
    assert!(final_removes.is_empty());
    assert_view_update_only_ships_rows(
        &final_read,
        BTreeSet::from([update_doc, allowed_insert]),
    );
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
    core.set_test_provider_claims(
        alice,
        BTreeMap::from([(
            "user_id".to_owned(),
            Value::String(alice.test_uuid().to_string()),
        )]),
    );
    core.set_test_provider_claims(
        bob,
        BTreeMap::from([(
            "user_id".to_owned(),
            Value::String(bob.test_uuid().to_string()),
        )]),
    );
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
            ("ownerId".to_owned(), Value::String(alice.test_uuid().to_string())),
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
fn policy_free_table_is_open_for_reads_and_writes() {
    let schema = build_public_test_schema(
        PublicSchemaBuilder::new().table(
            PublicTableSchemaBuilder::new("todos")
                .column("title", PublicColumnType::Text)
                .column("owner", PublicColumnType::Uuid),
        ),
    );
    let (_writer_dir, mut writer) = open_node_with_schema(node(1), schema.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema.clone());
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

    for (index, commit) in [
        MergeableCommit::new("todos", row(0x85), 11)
            .made_by(user(0xa1))
            .cells(owner_cells(user(0xa1), "public update")),
        MergeableCommit::new("todos", row(0x85), 12)
            .made_by(user(0xa1))
            .deletion(DeletionEvent::Deleted),
    ]
    .into_iter()
    .enumerate()
    {
        let (_writer_dir, mut writer) = open_node_with_schema(node(2 + index as u8), schema.clone());
        let (_tx_id, unit) = writer.commit_mergeable_unit_settled(commit).unwrap();
        let [fate] = core.apply_sync_message_settled(unit).unwrap().try_into().unwrap();
        assert!(matches!(
            fate,
            SyncMessage::FateUpdate {
                fate: Fate::Accepted,
                ..
            }
        ));
    }
}

/// A table starts open, but the first policy clause closes every other action.
/// This deliberately sends each forged action through a distinct untrusted
/// writer and the fate authority: changing a missing-clause branch back to
/// `Ok(true)` makes one of these receipts Accepted.
#[test]
fn partial_policy_set_allows_its_declared_read_and_denies_omitted_writes_at_authority() {
    let schema = build_public_test_schema(
        PublicSchemaBuilder::new().table(
            PublicTableSchemaBuilder::new("todos")
                .column("title", PublicColumnType::Text)
                .column("owner", PublicColumnType::Uuid)
                .policies(PublicTablePolicies::new().with_select(PublicPolicyExpr::True)),
        ),
    );
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema.clone());
    let seed = row(0x86);
    accept_global(
        &mut core,
        MergeableCommit::new("todos", seed, 10).cells(owner_cells(user(0xa1), "seed")),
    );

    let mut edge = PeerState::edge_client(user(0xcc));
    assert_view_update_only_references_rows(
        &edge.current_rows_update(&mut core, "todos").unwrap(),
        BTreeSet::from([seed]),
    );

    let writer = user(0xa1);
    let attempts = [
        MergeableCommit::new("todos", row(0x87), 11)
            .made_by(writer)
            .cells(owner_cells(writer, "forged insert")),
        MergeableCommit::new("todos", seed, 12)
            .made_by(writer)
            .cells(owner_cells(writer, "forged update")),
        MergeableCommit::new("todos", seed, 13)
            .made_by(writer)
            .deletion(DeletionEvent::Deleted),
    ];
    for (index, commit) in attempts.into_iter().enumerate() {
        let (_writer_dir, mut untrusted_writer) = open_node_with_schema(node(0xa1 + index as u8), schema.clone());
        let (tx_id, unit) = untrusted_writer.commit_mergeable_unit_settled(commit).unwrap();
        let [fate] = core.apply_sync_message_settled(unit).unwrap().try_into().unwrap();
        assert_eq!(
            fate,
            SyncMessage::FateUpdate {
                tx_id,
                fate: Fate::Rejected(RejectionReason::AuthorizationDenied),
                global_time: None,
                durability: None,
            },
            "omitted {} policy must deny at the authority",
            ["insert", "update", "delete"][index],
        );
    }
}

/// DELETE policy evaluation deliberately uses raw old-row evidence.  A
/// write-only table must not need ordinary SELECT permission merely to prove
/// its declared delete predicate at the serving authority.
#[test]
fn delete_only_policy_uses_raw_current_row_evidence_without_read_access() {
    let schema = build_public_test_schema(PublicSchemaBuilder::new().table(
        PublicTableSchemaBuilder::new("todos")
            .column("title", PublicColumnType::Text)
            .policies(PublicTablePolicies::new().with_delete(PublicPolicyExpr::True)),
    ));
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema.clone());
    let target = row(0x92);
    accept_global(
        &mut core,
        MergeableCommit::new("todos", target, 10).cells(title_cells("hidden but deletable")),
    );
    let author = user(0xa1);
    assert!(
        !core
            .dry_run_read_current_allows("todos", target, author)
            .unwrap(),
        "the author has no ordinary read authority"
    );
    assert!(
        core.dry_run_delete_current_allows("todos", target, author)
            .unwrap(),
        "the declared DELETE policy may inspect raw old-row evidence"
    );

    let (_writer_dir, mut writer) = open_node_with_schema(node(0xa1), schema);
    let (tx_id, unit) = writer
        .commit_mergeable_unit_settled(
            MergeableCommit::new("todos", target, 20)
                .made_by(author)
                .deletion(DeletionEvent::Deleted),
        )
        .unwrap();
    let [fate] = core.apply_sync_message_settled(unit).unwrap().try_into().unwrap();
    assert!(matches!(
        fate,
        SyncMessage::FateUpdate {
            tx_id: received,
            fate: Fate::Accepted,
            ..
        } if received == tx_id
    ));
}
