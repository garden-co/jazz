use crate::query::{in_list, is_null};

fn access_path_schema() -> JazzSchema {
    build_public_test_schema(PublicSchemaBuilder::new().table(
        PublicTableSchemaBuilder::new("docs")
            .column("owner", PublicColumnType::Uuid)
            .column("status", PublicColumnType::Text)
            .column("body", PublicColumnType::Text)
            .index_only(["owner"]),
    ))
}

fn multi_index_access_path_schema() -> JazzSchema {
    build_public_test_schema(PublicSchemaBuilder::new().table(
        PublicTableSchemaBuilder::new("docs")
            .column("owner", PublicColumnType::Uuid)
            .column("status", PublicColumnType::Text)
            .column("body", PublicColumnType::Text)
            .index_only(["owner", "status"]),
    ))
}

fn policy_indexed_access_path_schema(policy: PublicPolicyExpr) -> JazzSchema {
    build_public_test_schema(PublicSchemaBuilder::new().table(
        PublicTableSchemaBuilder::new("docs")
            .column("owner", PublicColumnType::Uuid)
            .column("status", PublicColumnType::Text)
            .column("body", PublicColumnType::Text)
            .policies(public_all_policies().with_select(policy))
            .index_only(["owner"]),
    ))
}

fn access_path_doc_cells(owner: AuthorSubject, status: &str, body: &str) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("owner".to_owned(), Value::Uuid(owner.test_uuid())),
        ("status".to_owned(), Value::String(status.to_owned())),
        ("body".to_owned(), Value::String(body.to_owned())),
    ])
}

fn seed_access_path_docs(
    writer: &mut NodeState<RocksDbStorage>,
    core: &mut NodeState<RocksDbStorage>,
) -> (RowUuid, RowUuid, AuthorSubject) {
    let owner_a = user(0xa1);
    let owner_b = user(0xb2);
    let first = row(0x11);
    let second = row(0x22);
    commit_mergeable_global(
        writer,
        core,
        MergeableCommit::new("docs", first, 10)
            .cells(access_path_doc_cells(owner_a, "open", "first")),
    );
    commit_mergeable_global(
        writer,
        core,
        MergeableCommit::new("docs", second, 11)
            .cells(access_path_doc_cells(owner_b, "closed", "second")),
    );
    (first, second, owner_a)
}

fn query_rows_by_uuid(
    node: &mut NodeState<RocksDbStorage>,
    query: Query,
    tier: DurabilityTier,
) -> (Vec<RowUuid>, QueryEngineReadMetrics) {
    let shape = query.validate(&node.catalogue.schema).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    node.reset_query_engine_read_metrics();
    let rows = node
        .query_rows_for_link(&shape, &binding, tier, AuthorSubject::SYSTEM)
        .unwrap()
        .into_iter()
        .map(|row| row.row_uuid())
        .collect::<Vec<_>>();
    (rows, node.query_engine_read_metrics().clone())
}

fn query_rows_by_uuid_for_identity(
    node: &mut NodeState<RocksDbStorage>,
    query: Query,
    tier: DurabilityTier,
    identity: AuthorSubject,
) -> (Vec<RowUuid>, QueryEngineReadMetrics) {
    // This harness models a UUID-backed application `sub` claim separately
    // from the canonical authenticated author used to select the session.
    if identity != AuthorSubject::SYSTEM {
        let mut claims = node.session_claims.get(&identity).cloned().unwrap_or_default();
        claims
            .entry("sub".to_owned())
            .or_insert_with(|| Value::Uuid(identity.test_uuid()));
        node.set_test_provider_claims(identity, claims);
    }
    let shape = query.validate(&node.catalogue.schema).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    node.reset_query_engine_read_metrics();
    let rows = node
        .query_rows_for_link(&shape, &binding, tier, identity)
        .unwrap()
        .into_iter()
        .map(|row| row.row_uuid())
        .collect::<Vec<_>>();
    (rows, node.query_engine_read_metrics().clone())
}

fn maintained_rows_by_uuid_for_identity(
    node: &mut NodeState<RocksDbStorage>,
    query: Query,
    tier: DurabilityTier,
    identity: AuthorSubject,
) -> (Vec<RowUuid>, QueryEngineReadMetrics) {
    // Keep the maintained subscription path on the same authenticated claim
    // binding as the public one-shot helper above.
    if identity != AuthorSubject::SYSTEM {
        let mut claims = node.session_claims.get(&identity).cloned().unwrap_or_default();
        claims
            .entry("sub".to_owned())
            .or_insert_with(|| Value::Uuid(identity.test_uuid()));
        node.set_test_provider_claims(identity, claims);
    }
    let shape = query.validate(&node.catalogue.schema).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    node.reset_query_engine_read_metrics();
    let (receiver, maintained, _schemas, _transitions, _tables, _incomplete) = node
        .open_seeded_maintained_subscription_view(
            &shape,
            &binding,
            identity,
            tier,
            &crate::protocol::ReadViewSpec::default(),
        )
        .unwrap();
    let rows = maintained
        .active_result_members()
        .iter()
        .filter_map(crate::protocol::ResultMemberEntry::as_row)
        .filter_map(|(table, row_uuid, _)| (table.as_str() == "docs").then_some(row_uuid))
        .collect();
    node.unsubscribe_groove_subscription(receiver.id());
    (rows, node.query_engine_read_metrics().clone())
}

#[test]
fn history_complete_query_ignores_stale_settled_result_membership() {
    let schema = access_path_schema();
    let (_writer_dir, mut writer) = open_node_with_schema(node(8), schema.clone());
    let (_core_dir, mut core) = open_history_complete_node_with_schema(node(9), schema);
    let (first, second, _owner) = seed_access_path_docs(&mut writer, &mut core);
    let query = Query::from("docs");
    let shape = query.validate(&core.catalogue.schema).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let binding_view = crate::protocol::BindingViewKey::new(
        shape.shape_id(),
        binding.binding_id(),
        crate::protocol::ReadViewKey::default(),
    );
    core.query
        .settled_result_sets
        .insert(binding_view, BTreeSet::new());

    assert!(core.is_history_complete());
    assert_eq!(
        query_rows_by_uuid(&mut core, query, DurabilityTier::Global).0,
        vec![first, second],
        "a history-complete authority must read canonical physical state rather than its downstream settled-result cache"
    );
}

/// This is intentionally an internal receipt for the physical access-path
/// counter. Row visibility itself is asserted through the normal query API;
/// no public surface exposes whether the source was an index or full scan.
#[test]
fn indexed_read_policy_matches_local_scan_for_allowed_and_denied_identities() {
    let schema = policy_indexed_access_path_schema(public_claim_eq("owner", "sub"));
    let (_writer_dir, mut writer) = open_node_with_schema(node(8), schema.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    let (first, _second, owner) = seed_access_path_docs(&mut writer, &mut core);
    let denied = user(0xc3);
    let query = Query::from("docs");

    let (global_allowed, global_allowed_metrics) = query_rows_by_uuid_for_identity(
        &mut core,
        query.clone(),
        DurabilityTier::Global,
        owner,
    );
    let (local_allowed, local_allowed_metrics) = query_rows_by_uuid_for_identity(
        &mut core,
        query.clone(),
        DurabilityTier::Local,
        owner,
    );
    let (edge_allowed, edge_allowed_metrics) = query_rows_by_uuid_for_identity(
        &mut core,
        query.clone(),
        DurabilityTier::Edge,
        owner,
    );
    let (global_denied, global_denied_metrics) = query_rows_by_uuid_for_identity(
        &mut core,
        query.clone(),
        DurabilityTier::Global,
        denied,
    );
    let (local_denied, _) = query_rows_by_uuid_for_identity(
        &mut core,
        query,
        DurabilityTier::Local,
        denied,
    );

    assert_eq!(global_allowed, local_allowed);
    assert_eq!(global_allowed, edge_allowed);
    assert_eq!(global_allowed, vec![first]);
    assert_eq!(global_denied, local_denied);
    assert!(global_denied.is_empty());
    // Prepared AppRows/policy dependencies are reused across identities. Their
    // claim-shaped cache key must not retain this owner's concrete secondary
    // index prefix; only the per-identity maintained root may specialize it.
    assert_eq!(global_allowed_metrics.source_index_probes, 0);
    assert!(global_allowed_metrics.source_full_scans >= 1);
    assert!(local_allowed_metrics.source_full_scans >= 1);
    assert_eq!(edge_allowed_metrics.source_index_probes, 0);
    assert!(edge_allowed_metrics.source_full_scans >= 1);
    assert_eq!(global_denied_metrics.source_index_probes, 0);

    // Exercise the reverse cache population order too: a denied identity must
    // not leave a reusable empty authorization graph that hides a later owner.
    let schema = policy_indexed_access_path_schema(public_claim_eq("owner", "sub"));
    let (_reverse_writer_dir, mut reverse_writer) = open_node_with_schema(node(10), schema.clone());
    let (_reverse_core_dir, mut reverse_core) = open_node_with_schema(node(11), schema);
    let (reverse_first, _reverse_second, reverse_owner) =
        seed_access_path_docs(&mut reverse_writer, &mut reverse_core);
    let reverse_denied = user(0xd4);
    let (denied_first, _) = query_rows_by_uuid_for_identity(
        &mut reverse_core,
        Query::from("docs"),
        DurabilityTier::Global,
        reverse_denied,
    );
    let (allowed_after_denied, _) = query_rows_by_uuid_for_identity(
        &mut reverse_core,
        Query::from("docs"),
        DurabilityTier::Global,
        reverse_owner,
    );
    assert!(denied_first.is_empty());
    assert_eq!(allowed_after_denied, vec![reverse_first]);
}

#[test]
fn maintained_policy_index_reads_are_isolated_between_identities() {
    let schema = policy_indexed_access_path_schema(public_claim_eq("owner", "sub"));
    let (_writer_dir, mut writer) = open_node_with_schema(node(12), schema.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(13), schema);
    let (first, _second, owner) = seed_access_path_docs(&mut writer, &mut core);
    let denied = user(0xe5);

    // The maintained root may specialize its own source with the authenticated
    // identity, but its cached policy dependencies must not leak that prefix
    // into a later subscriber.
    let (allowed_first, _) = maintained_rows_by_uuid_for_identity(
        &mut core,
        Query::from("docs"),
        DurabilityTier::Global,
        owner,
    );
    let (denied_after_allowed, _) = maintained_rows_by_uuid_for_identity(
        &mut core,
        Query::from("docs"),
        DurabilityTier::Global,
        denied,
    );
    assert_eq!(allowed_first, vec![first]);
    assert!(denied_after_allowed.is_empty());

    let schema = policy_indexed_access_path_schema(public_claim_eq("owner", "sub"));
    let (_reverse_writer_dir, mut reverse_writer) = open_node_with_schema(node(14), schema.clone());
    let (_reverse_core_dir, mut reverse_core) = open_node_with_schema(node(15), schema);
    let (reverse_first, _reverse_second, reverse_owner) =
        seed_access_path_docs(&mut reverse_writer, &mut reverse_core);
    let reverse_denied = user(0xf6);
    let (denied_first, _) = maintained_rows_by_uuid_for_identity(
        &mut reverse_core,
        Query::from("docs"),
        DurabilityTier::Global,
        reverse_denied,
    );
    let (allowed_after_denied, _) = maintained_rows_by_uuid_for_identity(
        &mut reverse_core,
        Query::from("docs"),
        DurabilityTier::Global,
        reverse_owner,
    );
    assert!(denied_first.is_empty());
    assert_eq!(allowed_after_denied, vec![reverse_first]);
}

#[test]
fn indexed_conjunctive_read_policy_retains_the_final_policy_predicate() {
    let policy = PublicPolicyExpr::And(vec![
        public_claim_eq("owner", "sub"),
        public_literal_eq("status", PublicValue::Text("open".to_owned())),
    ]);
    let schema = policy_indexed_access_path_schema(policy);
    let (_writer_dir, mut writer) = open_node_with_schema(node(8), schema.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    let owner = user(0xa1);
    let other = user(0xb2);
    let owned_open = row(0x11);
    let owned_closed = row(0x12);
    let foreign_open = row(0x13);
    for (row_uuid, tx_time, row_owner, status) in [
        (owned_open, 10, owner, "open"),
        (owned_closed, 11, owner, "closed"),
        (foreign_open, 12, other, "open"),
    ] {
        commit_mergeable_global(
            &mut writer,
            &mut core,
            MergeableCommit::new("docs", row_uuid, tx_time)
                .cells(access_path_doc_cells(row_owner, status, status)),
        );
    }

    let query = Query::from("docs");
    let (global, global_metrics) = maintained_rows_by_uuid_for_identity(
        &mut core,
        query.clone(),
        DurabilityTier::Global,
        owner,
    );
    let (local, _) =
        maintained_rows_by_uuid_for_identity(&mut core, query, DurabilityTier::Local, owner);

    assert_eq!(global, local);
    assert_eq!(global, vec![owned_open]);
    assert!(!global.contains(&owned_closed));
    assert!(!global.contains(&foreign_open));
    // The cached authorization graph is deliberately identity-neutral.  The
    // maintained root applies the owner's claim at its terminal instead of
    // baking a concrete owner-index probe into a reusable policy dependency.
    // The identity-neutral policy graph therefore deliberately scans its
    // dependency, while the maintained root remains selective. Retaining the
    // final literal predicate must not cause a secondary index probe.
    assert_eq!(global_metrics.source_index_probes, 0);
    assert!(global_metrics.source_full_scans >= 1);
}

#[test]
fn policy_access_path_receipt_is_reused_across_claim_bindings_without_leaking_visibility() {
    let schema = policy_indexed_access_path_schema(public_claim_eq("owner", "tenant"));
    let (_writer_dir, mut writer) = open_node_with_schema(node(8), schema.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    let reader = user(0xc3);
    let first_owner = user(0xa1);
    let second_owner = user(0xb2);
    let first = row(0x11);
    let second = row(0x22);
    for (row_uuid, tx_time, owner) in [(first, 10, first_owner), (second, 11, second_owner)] {
        commit_mergeable_global(
            &mut writer,
            &mut core,
            MergeableCommit::new("docs", row_uuid, tx_time)
                .cells(access_path_doc_cells(owner, "open", "document")),
        );
    }
    let query = Query::from("docs");

    core.set_test_provider_claims(
        reader,
        BTreeMap::from([("tenant".to_owned(), Value::Uuid(first_owner.test_uuid()))]),
    );
    let (first_rows, first_metrics) = maintained_rows_by_uuid_for_identity(
        &mut core,
        query.clone(),
        DurabilityTier::Global,
        reader,
    );
    core.set_test_provider_claims(
        reader,
        BTreeMap::from([("tenant".to_owned(), Value::Uuid(second_owner.test_uuid()))]),
    );
    let (second_rows, second_metrics) =
        maintained_rows_by_uuid_for_identity(&mut core, query, DurabilityTier::Global, reader);

    assert_eq!(first_rows, vec![first]);
    assert_eq!(second_rows, vec![second]);
    // Changing the claim must change the visible rows, but not mutate the
    // shared policy dependency into a claim-specialized source program.
    // Both reads reuse the identity-neutral graph: it deliberately falls back
    // to its complete policy source, but must never probe a claim-derived
    // secondary index.
    assert_eq!(first_metrics.source_index_probes, 0);
    assert!(first_metrics.source_full_scans >= 1);
    assert_eq!(second_metrics.source_index_probes, 0);
    assert!(second_metrics.source_full_scans >= 1);
}

#[test]
fn policy_access_path_planner_falls_back_for_or_and_non_equality() {
    let or_policy = PublicPolicyExpr::Or(vec![
        public_claim_eq("owner", "sub"),
        public_literal_eq("status", PublicValue::Text("public".to_owned())),
    ]);
    let schema = policy_indexed_access_path_schema(or_policy);
    let (_writer_dir, mut writer) = open_node_with_schema(node(8), schema.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    let (first, _second, owner) = seed_access_path_docs(&mut writer, &mut core);
    let query = Query::from("docs");
    let (global, global_metrics) = query_rows_by_uuid_for_identity(
        &mut core,
        query.clone(),
        DurabilityTier::Global,
        owner,
    );
    let (local, _) = query_rows_by_uuid_for_identity(&mut core, query, DurabilityTier::Local, owner);
    assert_eq!(global, local);
    assert_eq!(global, vec![first]);
    assert_eq!(global_metrics.source_index_probes, 0, "OR must retain the full-scan path");

    let non_equality = PublicPolicyExpr::Cmp {
        column: "owner".to_owned(),
        op: PublicCmpOp::Ne,
        value: PublicPolicyValue::SessionRef(vec!["claims".to_owned(), "sub".to_owned()]),
    };
    let schema = policy_indexed_access_path_schema(non_equality);
    let (_writer_dir, mut writer) = open_node_with_schema(node(10), schema.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(11), schema);
    let (_first, second, owner) = seed_access_path_docs(&mut writer, &mut core);
    let query = Query::from("docs");
    let (global, global_metrics) = query_rows_by_uuid_for_identity(
        &mut core,
        query.clone(),
        DurabilityTier::Global,
        owner,
    );
    let (local, _) = query_rows_by_uuid_for_identity(&mut core, query, DurabilityTier::Local, owner);
    assert_eq!(global, local);
    assert_eq!(global, vec![second]);
    assert_eq!(global_metrics.source_index_probes, 0, "non-equality must retain the full-scan path");
}

#[test]
fn policy_access_path_planner_falls_back_for_missing_or_nullable_claims_and_joins() {
    let schema = policy_indexed_access_path_schema(public_claim_eq("owner", "tenant"));
    let (_writer_dir, mut writer) = open_node_with_schema(node(8), schema.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    seed_access_path_docs(&mut writer, &mut core);
    let reader = user(0xc3);
    let query = Query::from("docs");
    let (global_missing, global_missing_metrics) = query_rows_by_uuid_for_identity(
        &mut core,
        query.clone(),
        DurabilityTier::Global,
        reader,
    );
    let (local_missing, _) = query_rows_by_uuid_for_identity(
        &mut core,
        query.clone(),
        DurabilityTier::Local,
        reader,
    );
    assert_eq!(global_missing, local_missing);
    assert!(global_missing.is_empty());
    assert_eq!(global_missing_metrics.source_index_probes, 0);

    let nullable_schema = build_public_test_schema(PublicSchemaBuilder::new().table(
        PublicTableSchemaBuilder::new("docs")
            .nullable_column("owner", PublicColumnType::Uuid)
            .column("status", PublicColumnType::Text)
            .policies(public_all_policies().with_select(public_claim_eq("owner", "tenant")))
            .index_only(["owner"]),
    ));
    let (_writer_dir, mut writer) = open_node_with_schema(node(10), nullable_schema.clone());
    let (_core_dir, mut nullable_core) = open_node_with_schema(node(11), nullable_schema);
    commit_mergeable_global(
        &mut writer,
        &mut nullable_core,
        MergeableCommit::new("docs", row(0x41), 10).cells(BTreeMap::from([
            (
                "owner".to_owned(),
                Value::Nullable(Some(Box::new(Value::Uuid(user(0xa1).test_uuid())))),
            ),
            ("status".to_owned(), Value::String("open".to_owned())),
        ])),
    );
    nullable_core.set_test_provider_claims(
        reader,
        BTreeMap::from([("tenant".to_owned(), Value::Nullable(None))]),
    );
    let (global_nullable, global_nullable_metrics) = query_rows_by_uuid_for_identity(
        &mut nullable_core,
        query.clone(),
        DurabilityTier::Global,
        reader,
    );
    let (local_nullable, _) = query_rows_by_uuid_for_identity(
        &mut nullable_core,
        query,
        DurabilityTier::Local,
        reader,
    );
    assert_eq!(global_nullable, local_nullable);
    assert!(global_nullable.is_empty());
    assert_eq!(global_nullable_metrics.source_index_probes, 0);

    let membership = PublicPolicyExpr::Exists {
        table: "memberships".to_owned(),
        condition: Box::new(PublicPolicyExpr::And(vec![
            PublicPolicyExpr::Cmp {
                column: "document".to_owned(),
                op: PublicCmpOp::Eq,
                value: PublicPolicyValue::SessionRef(vec![
                    "__jazz_outer_row".to_owned(),
                    "id".to_owned(),
                ]),
            },
            public_claim_eq("reader", "sub"),
        ])),
    };
    let schema = build_public_test_schema(
        PublicSchemaBuilder::new()
            .table(
                PublicTableSchemaBuilder::new("docs")
                    .column("owner", PublicColumnType::Uuid)
                    .column("status", PublicColumnType::Text)
                    .column("body", PublicColumnType::Text)
                    .policies(public_all_policies().with_select(membership))
                    .index_only(["owner"]),
            )
            .table(
                PublicTableSchemaBuilder::new("memberships")
                    .fk_column("document", "docs")
                    .column("reader", PublicColumnType::Uuid),
            ),
    );
    let (_writer_dir, mut writer) = open_node_with_schema(node(10), schema.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(11), schema);
    let (first, _second, _owner) = seed_access_path_docs(&mut writer, &mut core);
    let reader = user(0xd4);
    commit_mergeable_global(
        &mut writer,
        &mut core,
        MergeableCommit::new("memberships", row(0x44), 12).cells(BTreeMap::from([
            ("document".to_owned(), Value::Uuid(first.0)),
            ("reader".to_owned(), Value::Uuid(reader.test_uuid())),
        ])),
    );
    let query = Query::from("docs");
    let (global, metrics) = query_rows_by_uuid_for_identity(
        &mut core,
        query.clone(),
        DurabilityTier::Global,
        reader,
    );
    let (local, _) = query_rows_by_uuid_for_identity(&mut core, query, DurabilityTier::Local, reader);
    assert_eq!(global, local);
    assert_eq!(global, vec![first]);
    assert_eq!(metrics.source_index_probes, 0, "join policies must retain the full-scan path");
}

/// Current-row index probes must preserve the logical nullable reference
/// inside the separate physical envelope for authored-cell presence.
#[test]
fn nullable_reference_index_matches_present_uuid_and_excludes_nulls() {
    let schema = build_public_test_schema(
        PublicSchemaBuilder::new()
            .table(PublicTableSchemaBuilder::new("owners").column("name", PublicColumnType::Text))
            .table(
                PublicTableSchemaBuilder::new("optional_docs")
                    .nullable_fk_column("owner", "owners")
                    .column("title", PublicColumnType::Text),
            )
            .table(
                PublicTableSchemaBuilder::new("required_docs")
                    .fk_column("owner", "owners")
                    .column("title", PublicColumnType::Text),
            ),
    );
    let (_writer_dir, mut writer) = open_node_with_schema(node(0x91), schema.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(0x92), schema);
    let matching_owner = user(0xa1);
    let other_owner = user(0xb2);
    let matching_optional = row(0x11);
    let nonmatching_optional = row(0x22);
    let null_optional = row(0x33);
    let matching_required = row(0x44);

    commit_mergeable_global(
        &mut writer,
        &mut core,
        MergeableCommit::new("optional_docs", matching_optional, 10).cells(BTreeMap::from([
            (
                "owner".to_owned(),
                Value::Nullable(Some(Box::new(Value::Uuid(matching_owner.test_uuid())))),
            ),
            ("title".to_owned(), Value::String("match".to_owned())),
        ])),
    );
    commit_mergeable_global(
        &mut writer,
        &mut core,
        MergeableCommit::new("optional_docs", nonmatching_optional, 11).cells(BTreeMap::from([
            (
                "owner".to_owned(),
                Value::Nullable(Some(Box::new(Value::Uuid(other_owner.test_uuid())))),
            ),
            ("title".to_owned(), Value::String("other".to_owned())),
        ])),
    );
    commit_mergeable_global(
        &mut writer,
        &mut core,
        MergeableCommit::new("optional_docs", null_optional, 12).cells(BTreeMap::from([
            ("owner".to_owned(), Value::Nullable(None)),
            ("title".to_owned(), Value::String("null".to_owned())),
        ])),
    );
    commit_mergeable_global(
        &mut writer,
        &mut core,
        MergeableCommit::new("required_docs", matching_required, 13).cells(BTreeMap::from([
            ("owner".to_owned(), Value::Uuid(matching_owner.test_uuid())),
            ("title".to_owned(), Value::String("control".to_owned())),
        ])),
    );

    let optional = Query::from("optional_docs").filter(eq(
        col("owner"),
        lit(Value::Uuid(matching_owner.test_uuid())),
    ));
    let required = Query::from("required_docs").filter(eq(
        col("owner"),
        lit(Value::Uuid(matching_owner.test_uuid())),
    ));
    let explicit_null = Query::from("optional_docs").filter(is_null(col("owner")));

    for tier in [DurabilityTier::Local, DurabilityTier::Global] {
        let (optional_rows, optional_metrics) =
            query_rows_by_uuid(&mut core, optional.clone(), tier);
        assert_eq!(optional_rows, vec![matching_optional]);
        assert_eq!(optional_metrics.source_index_probes, 1);
        let (required_rows, _) = query_rows_by_uuid(&mut core, required.clone(), tier);
        assert_eq!(required_rows, vec![matching_required]);
        let (null_rows, _) = query_rows_by_uuid(&mut core, explicit_null.clone(), tier);
        assert_eq!(null_rows, vec![null_optional]);
    }
}

#[test]
fn one_shot_filtered_read_uses_primary_key_scan_for_id_equality() {
    let schema = access_path_schema();
    let (_writer_dir, mut writer) = open_node_with_schema(node(8), schema.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    let (first, _second, _owner) = seed_access_path_docs(&mut writer, &mut core);
    let query = Query::from("docs").filter(eq(col("id"), lit(Value::Uuid(first.0))));

    let (selected, selected_metrics) =
        query_rows_by_uuid(&mut core, query.clone(), DurabilityTier::Global);
    let (local, local_metrics) = query_rows_by_uuid(&mut core, query, DurabilityTier::Local);

    assert_eq!(selected, local);
    assert_eq!(selected, vec![first]);
    assert_eq!(selected_metrics.source_primary_key_scans, 1);
    assert_eq!(selected_metrics.source_index_probes, 0);
    assert_eq!(selected_metrics.source_full_scans, 0);
    assert_eq!(local_metrics.source_primary_key_scans, 1);
    assert_eq!(local_metrics.source_full_scans, 0);
}

#[test]
fn declared_id_column_filter_uses_declared_value_not_physical_row_uuid() {
    let schema = build_public_test_schema(PublicSchemaBuilder::new().table(
        PublicTableSchemaBuilder::new("things")
            .column("id", PublicColumnType::Uuid)
            .column("label", PublicColumnType::Text),
    ));
    let (_writer_dir, mut writer) = open_node_with_schema(node(8), schema.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    let physical_row = row(0x11);
    let declared_id = row(0xaa);
    commit_mergeable_global(
        &mut writer,
        &mut core,
        MergeableCommit::new("things", physical_row, 10).cells(BTreeMap::from([
            ("id".to_owned(), Value::Uuid(declared_id.0)),
            ("label".to_owned(), Value::String("declared id".to_owned())),
        ])),
    );

    let query = Query::from("things").filter(eq(col("id"), lit(Value::Uuid(declared_id.0))));
    let (selected, _) = query_rows_by_uuid(&mut core, query, DurabilityTier::Global);

    assert_eq!(selected, vec![physical_row]);
}

/// A declared `id` is an ordinary user column for IN and NULL predicates;
/// Alice's physical row UUID must not leak into either predicate evaluation.
#[test]
fn declared_id_column_in_and_is_null_use_declared_values() {
    let schema = build_public_test_schema(PublicSchemaBuilder::new().table(
        PublicTableSchemaBuilder::new("things")
            .nullable_column("id", PublicColumnType::Uuid)
            .column("label", PublicColumnType::Text),
    ));
    let (_writer_dir, mut writer) = open_node_with_schema(node(8), schema.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    let matching_row = row(0x12);
    let null_row = row(0x13);
    let declared_id = row(0xab);
    commit_mergeable_global(
        &mut writer,
        &mut core,
        MergeableCommit::new("things", matching_row, 10).cells(BTreeMap::from([
            (
                "id".to_owned(),
                Value::Nullable(Some(Box::new(Value::Uuid(declared_id.0)))),
            ),
            ("label".to_owned(), Value::String("matching".to_owned())),
        ])),
    );
    commit_mergeable_global(
        &mut writer,
        &mut core,
        MergeableCommit::new("things", null_row, 11).cells(BTreeMap::from([
            ("id".to_owned(), Value::Nullable(None)),
            ("label".to_owned(), Value::String("null".to_owned())),
        ])),
    );

    let (in_rows, _) = query_rows_by_uuid(
        &mut core,
        Query::from("things").filter(in_list(
            col("id"),
            [lit(Value::Nullable(Some(Box::new(Value::Uuid(declared_id.0)))) )],
        )),
        DurabilityTier::Global,
    );
    let (null_rows, _) = query_rows_by_uuid(
        &mut core,
        Query::from("things").filter(is_null(col("id"))),
        DurabilityTier::Global,
    );
    assert_eq!(in_rows, vec![matching_row]);
    assert_eq!(null_rows, vec![null_row]);

    let missing_id_schema = build_public_test_schema(
        PublicSchemaBuilder::new().table(
            PublicTableSchemaBuilder::new("without_declared_id")
                .column("label", PublicColumnType::Text),
        ),
    );
    assert!(Query::from("without_declared_id")
        .filter(is_null(col("id")))
        .validate(&missing_id_schema)
        .is_err());
}

/// Alice joins a child back to a parent through the parent's declared `id`,
/// rather than accidentally comparing the child FK with the physical row UUID.
#[test]
fn inverse_join_via_column_uses_root_declared_id() {
    let schema = build_public_test_schema(
        PublicSchemaBuilder::new()
            .table(
                PublicTableSchemaBuilder::new("parents")
                    .column("id", PublicColumnType::Uuid)
                    .column("label", PublicColumnType::Text),
            )
            .table(
                PublicTableSchemaBuilder::new("children")
                    .fk_column("parent", "parents")
                    .column("label", PublicColumnType::Text),
            ),
    );
    let (_writer_dir, mut writer) = open_node_with_schema(node(8), schema.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    let parent = row(0x21);
    let child = row(0x22);
    let declared_parent_id = row(0xac);
    commit_mergeable_global(
        &mut writer,
        &mut core,
        MergeableCommit::new("parents", parent, 10).cells(BTreeMap::from([
            ("id".to_owned(), Value::Uuid(declared_parent_id.0)),
            ("label".to_owned(), Value::String("parent".to_owned())),
        ])),
    );
    commit_mergeable_global(
        &mut writer,
        &mut core,
        MergeableCommit::new("children", child, 11).cells(BTreeMap::from([
            ("parent".to_owned(), Value::Uuid(declared_parent_id.0)),
            ("label".to_owned(), Value::String("child".to_owned())),
        ])),
    );

    let (rows, _) = query_rows_by_uuid(
        &mut core,
        Query::from("parents").join_via_column("children", "parent", "id", []),
        DurabilityTier::Global,
    );
    assert_eq!(rows, vec![parent]);
}

/// Alice's declared IDs control final one-shot ordering and multi-row
/// pagination, even when their physical row UUID order is the opposite.
#[test]
fn declared_id_order_and_pagination_use_declared_values() {
    let schema = build_public_test_schema(PublicSchemaBuilder::new().table(
        PublicTableSchemaBuilder::new("things")
            .column("id", PublicColumnType::Uuid)
            .column("label", PublicColumnType::Text),
    ));
    let (_writer_dir, mut writer) = open_node_with_schema(node(8), schema.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    let physically_first = row(0x31);
    let physically_second = row(0x32);
    let physically_third = row(0x33);
    commit_mergeable_global(
        &mut writer,
        &mut core,
        MergeableCommit::new("things", physically_first, 10).cells(BTreeMap::from([
            ("id".to_owned(), Value::Uuid(row(0xf1).0)),
            ("label".to_owned(), Value::String("later".to_owned())),
        ])),
    );
    commit_mergeable_global(
        &mut writer,
        &mut core,
        MergeableCommit::new("things", physically_third, 12).cells(BTreeMap::from([
            ("id".to_owned(), Value::Uuid(row(0x80).0)),
            ("label".to_owned(), Value::String("middle".to_owned())),
        ])),
    );
    commit_mergeable_global(
        &mut writer,
        &mut core,
        MergeableCommit::new("things", physically_second, 11).cells(BTreeMap::from([
            ("id".to_owned(), Value::Uuid(row(0x01).0)),
            ("label".to_owned(), Value::String("earlier".to_owned())),
        ])),
    );

    let (rows, _) = query_rows_by_uuid(
        &mut core,
        Query::from("things")
            .order_by("id", OrderDirection::Asc)
            .offset(1)
            .limit(2),
        DurabilityTier::Global,
    );
    assert_eq!(rows, vec![physically_third, physically_first]);
}

#[test]
fn one_shot_filtered_read_uses_declared_index_for_indexed_column_equality() {
    let schema = access_path_schema();
    let (_writer_dir, mut writer) = open_node_with_schema(node(8), schema.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    let (first, _second, owner) = seed_access_path_docs(&mut writer, &mut core);
    let query = Query::from("docs").filter(eq(col("owner"), lit(Value::Uuid(owner.test_uuid()))));

    let (selected, selected_metrics) =
        query_rows_by_uuid(&mut core, query.clone(), DurabilityTier::Global);
    let (local, local_metrics) = query_rows_by_uuid(&mut core, query, DurabilityTier::Local);

    assert_eq!(selected, local);
    assert_eq!(selected, vec![first]);
    assert_eq!(selected_metrics.source_primary_key_scans, 0);
    assert_eq!(selected_metrics.source_index_probes, 1);
    assert_eq!(selected_metrics.source_full_scans, 0);
    assert_eq!(local_metrics.source_index_probes, 1);
    assert_eq!(
        local_metrics.source_full_scans, 1,
        "Local index reads must scan ahead candidates because a newer winner can change owner"
    );
}

#[test]
fn local_indexed_read_includes_ahead_winners_outside_the_settled_prefix() {
    // Internal planner receipt for INV-READ-7. The public query result proves
    // that a Local winner is selected before the owner predicate is applied;
    // the physical counters prove the settled owner index remains useful while
    // the ahead table is scanned for a differently-indexed dominating winner.
    let schema = access_path_schema();
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    let owner_a = user(0xa1);
    let owner_b = user(0xb2);
    let moved_out = row(0x31);
    let moved_in = row(0x32);
    let duplicate = row(0x33);
    let deleted = row(0x34);

    for (row_uuid, tx_time, owner) in [
        (moved_out, 10, owner_a),
        (moved_in, 11, owner_b),
        (duplicate, 12, owner_a),
        (deleted, 13, owner_a),
    ] {
        let tx_id = core
            .commit_mergeable_settled(
                MergeableCommit::new("docs", row_uuid, tx_time).cells(access_path_doc_cells(
                    owner, "open", "settled",
                )),
            )
            .unwrap();
        let global_time = core.allocate_global_time_for_test();
        core.apply_fate_update(
            tx_id,
            Fate::Accepted,
            Some(global_time),
            Some(DurabilityTier::Global),
        )
        .unwrap();
    }

    core.commit_mergeable_settled(
        MergeableCommit::new("docs", moved_out, 20).cells(access_path_doc_cells(
            owner_b,
            "open",
            "ahead owner changed",
        )),
    )
    .unwrap();
    core.commit_mergeable_settled(
        MergeableCommit::new("docs", moved_in, 21).cells(access_path_doc_cells(
            owner_a,
            "open",
            "ahead owner changed in",
        )),
    )
    .unwrap();
    core.commit_mergeable_settled(
        MergeableCommit::new("docs", duplicate, 22).cells(access_path_doc_cells(
            owner_a,
            "open",
            "ahead duplicate prefix",
        )),
    )
    .unwrap();
    core.commit_mergeable_settled(MergeableCommit::new("docs", deleted, 23).deletion(
        DeletionEvent::Deleted,
    ))
    .unwrap();

    let query = Query::from("docs").filter(eq(col("owner"), lit(Value::Uuid(owner_a.test_uuid()))));
    let (global, _) = query_rows_by_uuid(&mut core, query.clone(), DurabilityTier::Global);
    let (local, metrics) = query_rows_by_uuid(&mut core, query, DurabilityTier::Local);

    assert_eq!(global.into_iter().collect::<BTreeSet<_>>(), BTreeSet::from([moved_out, duplicate, deleted]));
    assert_eq!(local.into_iter().collect::<BTreeSet<_>>(), BTreeSet::from([moved_in, duplicate]));
    assert_eq!(metrics.source_index_probes, 1);
    assert_eq!(metrics.source_full_scans, 1);
}

#[test]
fn parameterized_one_shot_index_read_does_not_fall_back_to_cached_full_scan() {
    let schema = access_path_schema();
    let (_writer_dir, mut writer) = open_node_with_schema(node(8), schema.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    let (first, _second, owner) = seed_access_path_docs(&mut writer, &mut core);
    let shape = Query::from("docs")
        .filter(eq(col("owner"), param("owner")))
        .validate(&core.catalogue.schema)
        .expect("validate parameterized owner query");
    let binding = shape
        .bind(BTreeMap::from([(
            "owner".to_owned(),
            Value::Uuid(owner.test_uuid()),
        )]))
        .expect("bind owner parameter");

    core.reset_storage_read_metrics();
    let rows = core
        .query_rows_for_link(&shape, &binding, DurabilityTier::Global, AuthorSubject::SYSTEM)
        .expect("run parameterized indexed one-shot");
    let metrics = core.take_storage_read_metrics();

    assert_eq!(
        rows.into_iter()
            .map(|row| row.row_uuid())
            .collect::<Vec<_>>(),
        vec![first]
    );
    assert_eq!(metrics.global_current_indexes.reads, 1);
    assert_eq!(
        metrics.global_current_rows.reads, 1,
        "the concrete index-selected program must not be replaced by a cached full-scan plan"
    );
}

// Internal receipt: the public include-deleted result is one row; the counted
// physical index entry proves that the proved source cap reached storage.
#[test]
fn include_deleted_global_index_limit_bounds_the_physical_index_source() {
    let schema = access_path_schema();
    let (_writer_dir, mut writer) = open_node_with_schema(node(0xd1), schema.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(0xd2), schema);
    let owner = user(0xd3);
    for (row_uuid, tx_time) in [(row(0xd6), 10), (row(0xd4), 11), (row(0xd5), 12)] {
        commit_mergeable_global(
            &mut writer,
            &mut core,
            MergeableCommit::new("docs", row_uuid, tx_time)
                .cells(access_path_doc_cells(owner, "open", "body")),
        );
    }
    let shape = Query::from("docs")
        .filter(eq(col("owner"), lit(Value::Uuid(owner.test_uuid()))))
        .limit(1)
        .validate(&core.catalogue.schema)
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    core.reset_storage_read_metrics();
    let rows = core
        .query_rows_including_deleted_in_authorization_mode(
            &shape,
            &binding,
            DurabilityTier::Global,
            None,
            AuthorSubject::SYSTEM,
            QueryAuthorizationMode::TrustedServing,
        )
        .unwrap();
    let metrics = core.take_storage_read_metrics();
    assert_eq!(rows.len(), 1);
    assert_eq!(metrics.global_current_indexes.reads, 1);
    assert_eq!(metrics.global_current_rows.reads, 1);
}

#[test]
fn physical_index_backfills_existing_rows_and_read_cost_ignores_schema_variant_count() {
    // This is intentionally an internal receipt: schema evolution and query
    // results use the public protocol/query APIs, while physical read counts
    // and index names are implementation details with no public equivalent.
    let base = build_public_test_schema(
        PublicSchemaBuilder::new().table(
            PublicTableSchemaBuilder::new("todos").column("title", PublicColumnType::Text),
        ),
    );
    let indexed = SchemaVersion::new(build_public_test_schema(
        PublicSchemaBuilder::new().table(
            PublicTableSchemaBuilder::new("todos")
                .column("title", PublicColumnType::Text)
                .index_only(["title"]),
        ),
    ));
    let extended = SchemaVersion::new(build_public_test_schema(
        PublicSchemaBuilder::new().table(
            PublicTableSchemaBuilder::new("todos")
                .column("title", PublicColumnType::Text)
                .column("body", PublicColumnType::Text)
                .index_only(["title"]),
        ),
    ));
    let (_writer_dir, mut writer) = open_node_with_schema(node(0xb1), base.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(0xb2), base.clone());
    let existing = row(0xb3);
    commit_mergeable_global(
        &mut writer,
        &mut core,
        MergeableCommit::new("todos", existing, 10).cells(title_cells("before-index")),
    );

    assert_eq!(
        indexed.id,
        base.version_id(),
        "physical indexes are deliberately outside content-addressed schema identity"
    );
    core.apply_trusted_catalogue_message_settled(SyncMessage::PublishSchema {
        author: AuthorSubject::SYSTEM,
        schema: Box::new(indexed.clone()),
    })
    .unwrap();

    let indexed_mapping = core.catalogue.physical_mappings[&indexed.id].tables["todos"].clone();
    let physical_table = physical_global_current_table_name(indexed_mapping.table_id);
    let physical_index = physical_current_index_name(indexed_mapping.columns["title"]);
    assert!(
        core.database
            .table_schema(&physical_table)
            .unwrap()
            .indices
            .iter()
            .any(|index| index.name == physical_index),
        "publishing the indexed variant must register its physical index"
    );

    let query = Query::from("todos").filter(eq(col("title"), lit("before-index")));
    let shape = query.validate(&core.catalogue.schema).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    core.reset_storage_read_metrics();
    let rows = core
        .query_rows_for_link(
            &shape,
            &binding,
            DurabilityTier::Global,
            AuthorSubject::SYSTEM,
        )
        .unwrap();
    let indexed_reads = core.take_storage_read_metrics();
    assert_eq!(
        rows.into_iter()
            .map(|row| row.row_uuid())
            .collect::<Vec<_>>(),
        vec![existing],
        "the live index must backfill the row written before it existed"
    );
    assert_eq!(indexed_reads.global_current_indexes.reads, 1);
    assert_eq!(indexed_reads.global_current_rows.reads, 1);

    publish_schema_lineage(
        &mut core,
        extended.clone(),
        MigrationLens::new(
            indexed.id,
            extended.id,
            vec![TableLens {
                source_table: "todos".to_owned(),
                target_table: "todos".to_owned(),
                ops: vec![LensOp::AddColumn {
                    column: "body".to_owned(),
                    default: Value::String(String::new()),
                }],
            }],
        ).expect("valid migration lens"),
        Vec::<String>::new(),
        Vec::<String>::new(),
    )
    .unwrap();
    core.apply_trusted_catalogue_message_settled(SyncMessage::SetCurrentWriteSchema {
        author: AuthorSubject::SYSTEM,
        pointer: CurrentWriteSchema {
            revision: 2,
            schema: extended.id,
        },
    })
    .unwrap();

    assert_eq!(
        core.catalogue.physical_mappings[&extended.id].tables["todos"].table_id,
        indexed_mapping.table_id
    );
    let shape = query.validate(&core.catalogue.schema).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    core.reset_storage_read_metrics();
    let rows = core
        .query_rows_for_link(
            &shape,
            &binding,
            DurabilityTier::Global,
            AuthorSubject::SYSTEM,
        )
        .unwrap();
    let three_variant_reads = core.take_storage_read_metrics();

    assert_eq!(
        rows.into_iter()
            .map(|row| row.row_uuid())
            .collect::<Vec<_>>(),
        vec![existing]
    );
    assert_eq!(
        three_variant_reads.global_current_indexes,
        indexed_reads.global_current_indexes,
        "adding a schema variant must not add an index source"
    );
    assert_eq!(
        three_variant_reads.global_current_rows,
        indexed_reads.global_current_rows,
        "adding a schema variant must not add a current-row source"
    );
}

#[test]
fn one_shot_filtered_read_keeps_residual_filters_after_pushdown() {
    let schema = multi_index_access_path_schema();
    let (_writer_dir, mut writer) = open_node_with_schema(node(8), schema.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    let (first, _second, owner) = seed_access_path_docs(&mut writer, &mut core);
    let query = Query::from("docs")
        .filter(eq(col("owner"), lit(Value::Uuid(owner.test_uuid()))))
        .filter(eq(col("status"), lit("open")));

    let (selected, selected_metrics) =
        query_rows_by_uuid(&mut core, query.clone(), DurabilityTier::Global);
    let (local, local_metrics) = query_rows_by_uuid(&mut core, query, DurabilityTier::Local);

    assert_eq!(selected, local);
    assert_eq!(selected, vec![first]);
    assert_eq!(selected_metrics.source_index_probes, 2);
    assert_eq!(selected_metrics.source_full_scans, 0);
    assert_eq!(local_metrics.source_index_probes, 2);
    assert_eq!(local_metrics.source_full_scans, 1);
}

#[test]
fn one_shot_filtered_read_counts_full_scan_for_unindexed_filter() {
    let schema = access_path_schema();
    let (_writer_dir, mut writer) = open_node_with_schema(node(8), schema.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    let (_first, second, _owner) = seed_access_path_docs(&mut writer, &mut core);
    let query = Query::from("docs").filter(eq(col("status"), lit("closed")));

    let (selected, selected_metrics) =
        query_rows_by_uuid(&mut core, query.clone(), DurabilityTier::Global);
    let (forced_full, forced_metrics) = query_rows_by_uuid(&mut core, query, DurabilityTier::Local);

    assert_eq!(selected, forced_full);
    assert_eq!(selected, vec![second]);
    assert_eq!(selected_metrics.source_primary_key_scans, 0);
    assert_eq!(selected_metrics.source_index_probes, 0);
    assert_eq!(selected_metrics.source_full_scans, 1);
    assert_eq!(forced_metrics.source_full_scans, 1);
}

#[test]
fn whole_table_predicate_probe_uses_table_change_watermark() {
    let schema = build_public_test_schema(
        PublicSchemaBuilder::new()
            .table(
                PublicTableSchemaBuilder::new("todos").column("title", PublicColumnType::Text),
            )
            .table(
                PublicTableSchemaBuilder::new("notes").column("title", PublicColumnType::Text),
            ),
    );
    let (_writer_dir, mut writer) = open_node_with_schema(node(8), schema.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);

    let base = GlobalTime(0);
    commit_mergeable_global(
        &mut writer,
        &mut core,
        MergeableCommit::new("notes", row(1), 10).cells(title_cells("other table")),
    );
    assert!(
        !core.global_currency_changed_after("todos", base).unwrap(),
        "other-table writes must not invalidate whole-table predicates"
    );

    commit_mergeable_global(
        &mut writer,
        &mut core,
        MergeableCommit::new("todos", row(2), 11).cells(title_cells("target table")),
    );
    assert!(
        core.global_currency_changed_after("todos", base).unwrap(),
        "same-table writes after the base snapshot invalidate whole-table predicates"
    );
}
#[test]
fn history_subscriptions_flow_through_groove() {
    let (_temp_dir, mut node) = open_node();
    let subscription = node.subscribe_history("todos").unwrap();
    assert!(subscription.recv().unwrap().is_empty());

    node.commit_mergeable_settled(MergeableCommit::new("todos", row(8), 10).cells(title_cells("notify")))
        .unwrap();

    assert!(!subscription.recv().unwrap().is_empty());
}
#[test]
fn groove_current_rows_match_oracle_for_seeded_m1_commits() {
    let (_temp_dir, mut node) = open_node();
    let mut oracle = Oracle::new();
    let row = row(7);

    let base = commit_and_oracle(
        &mut node,
        &mut oracle,
        MergeableCommit::new("todos", row, 10).cells(title_cells("base")),
    );
    assert_current_rows_match_oracle(&mut node, &oracle);

    commit_and_oracle(
        &mut node,
        &mut oracle,
        MergeableCommit::new("todos", row, 9).cells(BTreeMap::from([(
            "title".to_owned(),
            "older clock".to_owned(),
        )])),
    );
    assert_current_rows_match_oracle(&mut node, &oracle);

    let child = commit_and_oracle(
        &mut node,
        &mut oracle,
        MergeableCommit::new("todos", row, 11)
            .parents(vec![base])
            .cells(title_cells("child")),
    );
    assert_current_rows_match_oracle(&mut node, &oracle);

    commit_and_oracle(
        &mut node,
        &mut oracle,
        MergeableCommit::new("todos", row, 12).deletion(DeletionEvent::Deleted),
    );
    assert_current_rows_match_oracle(&mut node, &oracle);

    commit_and_oracle(
        &mut node,
        &mut oracle,
        MergeableCommit::new("todos", row, 13)
            .parents(vec![child])
            .cells(BTreeMap::from([(
                "title".to_owned(),
                "delete-concurrent update".to_owned(),
            )])),
    );
    assert_current_rows_match_oracle(&mut node, &oracle);

    commit_and_oracle(
        &mut node,
        &mut oracle,
        MergeableCommit::new("todos", row, 14).deletion(DeletionEvent::Restored),
    );
    assert_current_rows_match_oracle(&mut node, &oracle);
}

#[test]
fn local_current_from_ahead_index_matches_history_argmax_for_seeded_commits() {
    for seed in 0..16_u64 {
        let (_temp_dir, mut node) = open_node();
        let mut parents = BTreeMap::<RowUuid, (Option<TxId>, Option<TxId>)>::new();
        let mut pending = Vec::<(RowUuid, TxId)>::new();
        let mut rng = seed.wrapping_mul(0x9e37_79b9_7f4a_7c15);

        for step in 0..96_u64 {
            rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
            let row_uuid = row(((rng >> 56) as u8 % 6) + 1);
            let action = (rng >> 48) % 9;
            let deletion = matches!(action, 0..=3);
            let mut commit = MergeableCommit::new("todos", row_uuid, 1_000 + step);
            if let Some(parent) = parents
                .get(&row_uuid)
                .and_then(|(content, deletion_parent)| {
                    if deletion { *deletion_parent } else { *content }
                })
            {
                commit = commit.parents(vec![parent]);
            }
            commit = match action {
                0 | 1 => commit.deletion(DeletionEvent::Deleted),
                2 | 3 => commit.deletion(DeletionEvent::Restored),
                _ => commit.cells(title_cells(format!("seed-{seed}-step-{step}"))),
            };

            let tx_id = node.commit_mergeable_settled(commit).unwrap();
            let entry = parents.entry(row_uuid).or_default();
            if deletion {
                entry.1 = Some(tx_id);
            } else {
                entry.0 = Some(tx_id);
            }
            match action {
                0 | 4 | 5 => {
                    let global_time = node.allocate_global_time_for_test();
                    node.apply_fate_update(
                        tx_id,
                        Fate::Accepted,
                        Some(global_time),
                        Some(DurabilityTier::Global),
                    )
                    .unwrap();
                }
                1 | 6 => {
                    node.apply_fate_update(
                        tx_id,
                        Fate::Rejected(RejectionReason::ExclusiveConflict),
                        None,
                        None,
                    )
                    .unwrap();
                    parents.remove(&row_uuid);
                }
                _ => pending.push((row_uuid, tx_id)),
            }

            if step % 13 == 12
                && let Some((pending_row, tx_id)) = pending.pop()
            {
                node.apply_fate_update(
                    tx_id,
                    Fate::Rejected(RejectionReason::ExclusiveConflict),
                    None,
                    None,
                )
                .unwrap();
                parents.remove(&pending_row);
            }

            assert_local_current_matches_history_argmax(&mut node, seed, step);
        }
    }
}

fn assert_local_current_matches_history_argmax(
    node: &mut NodeState<RocksDbStorage>,
    seed: u64,
    step: u64,
) {
    let actual = node
        .current_rows("todos", DurabilityTier::Local)
        .unwrap()
        .into_iter()
        .map(current_row_pair)
        .collect::<BTreeMap<_, _>>();
    let expected = history_argmax_current_rows(node);
    assert_eq!(actual, expected, "seed {seed}, step {step}");
}

fn history_argmax_current_rows(
    node: &mut NodeState<RocksDbStorage>,
) -> BTreeMap<RowUuid, BTreeMap<String, Value>> {
    let table = node.table("todos").unwrap().clone();
    let versions = node.query_table_versions("todos").unwrap();
    let mut content = BTreeMap::<RowUuid, &VersionRow>::new();
    let mut registers = BTreeMap::<RowUuid, &VersionRow>::new();
    for version in &versions {
        let winners = match version.layer() {
            VersionLayer::Content => &mut content,
            VersionLayer::Deletion => &mut registers,
        };
        if winners.get(&version.row_uuid()).is_none_or(|current| {
            (version.tx_time(), version.tx_node_alias())
                > (current.tx_time(), current.tx_node_alias())
        }) {
            winners.insert(version.row_uuid(), version);
        }
    }
    content
        .into_iter()
        .filter_map(|(row_uuid, version)| {
            let deleted = registers
                .get(&row_uuid)
                .and_then(|register| register.deletion())
                == Some(DeletionEvent::Deleted);
            if deleted {
                return None;
            }
            let cells = table
                .columns
                .iter()
                .filter_map(|column| {
                    version
                        .cell(&table, &column.name)
                        .unwrap()
                        .map(|value| (column.name.clone(), value))
                })
                .collect::<BTreeMap<_, _>>();
            Some((row_uuid, cells))
        })
        .collect()
}
#[test]
fn filterless_shape_and_degenerate_predicate_validation_agree() {
    let (_client_dir, mut client) = open_node_with_uuid(node(1));
    let (_other_dir, mut other) = open_node_with_uuid(node(2));
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    let shape = crate::query::Query::from("todos")
        .validate(&schema())
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    register_shape_binding(&mut core, &shape, &binding);

    let tx_id = OpenTransactionId::new();
    client.open_exclusive(tx_id).unwrap();
    assert!(client.tx_query(tx_id, &shape, &binding).unwrap().is_empty());
    commit_mergeable_global(
        &mut other,
        &mut core,
        MergeableCommit::new("todos", row(1), 10).cells(title_cells("phantom")),
    );
    client
        .tx_write(tx_id, "todos", row(2), title_cells("mine"), None)
        .unwrap();
    let (_tx_id, unit) = client
        .commit_exclusive_settled(tx_id, AuthorSubject::SYSTEM, 11)
        .unwrap();
    let SyncMessage::CommitUnit { tx, versions } = unit else {
        panic!("expected commit unit");
    };
    let predicate = tx
        .predicate_read_set
        .as_ref()
        .and_then(|reads| reads.first())
        .unwrap();
    assert!(
        core.predicate_read_is_degenerate_whole_table(predicate)
            .unwrap()
    );
    assert!(
        core.shape_predicate_changed_after(
            predicate,
            tx.base_snapshot.as_ref().unwrap()
        )
        .unwrap()
    );
    let [fate] = core
        .ingest_commit_unit_settled(tx, versions, u64::MAX - SKEW_TOLERANCE_MS)
        .unwrap()
        .try_into()
        .unwrap();
    let SyncMessage::FateUpdate { fate, .. } = fate else {
        panic!("expected fate update");
    };
    assert_eq!(fate, Fate::Rejected(RejectionReason::ExclusiveConflict));
}
#[test]
fn view_update_result_set_matches_groove_current_rows_for_seeded_commits() {
    let (_temp_dir, mut node) = open_node();
    let row_a = row(1);
    let row_b = row(2);

    for commit in [
        MergeableCommit::new("todos", row_a, 10).cells(title_cells("a1")),
        MergeableCommit::new("todos", row_b, 11).cells(title_cells("b1")),
        MergeableCommit::new("todos", row_a, 12).deletion(DeletionEvent::Deleted),
        MergeableCommit::new("todos", row_a, 13).cells(title_cells("a2")),
        MergeableCommit::new("todos", row_b, 14).deletion(DeletionEvent::Deleted),
    ] {
        let tx_id = node.commit_mergeable_settled(commit).unwrap();
        node.accept_global_for_test(tx_id).unwrap();
        assert_view_update_result_set_matches_current_rows(&mut node);
    }
}

#[test]
fn binding_delta_validates_shape_arity_and_cleans_up_binding_usage() {
    let (_temp_dir, mut node) = open_node();
    let shape = Query::from("todos")
        .filter(eq(col("title"), param("wanted")))
        .validate(&schema())
        .unwrap();
    let values = vec![Value::String("match".to_owned())];
    let usage_binding_id = BindingId(uuid::Uuid::from_bytes([0x77; 16]));
    let usage_subscription = SubscriptionKey {
        shape_id: shape.shape_id(),
        binding_id: usage_binding_id,
        read_view: Default::default(),
    };
    let other_usage_binding_id = BindingId(uuid::Uuid::from_bytes([0x88; 16]));
    let other_usage_subscription = SubscriptionKey {
        shape_id: shape.shape_id(),
        binding_id: other_usage_binding_id,
        read_view: Default::default(),
    };

    node.apply_sync_message_settled(SyncMessage::Subscribe(crate::protocol::Subscribe {
        shape_id: shape.shape_id(),
        subscription: usage_subscription,
        values: values.clone(),
        known_state: None,
        delegated_session: None,
    }))
    .unwrap();
    assert!(
        !node
            .query
            .registered_bindings
            .contains_key(&shape.shape_id())
    );

    node.apply_sync_message_settled(SyncMessage::RegisterShape {
        shape_id: shape.shape_id(),
        ast: crate::protocol::ShapeAst::from_validated(&shape),
        opts: crate::protocol::RegisterShapeOptions::default(),
    })
    .unwrap();
    assert!(
        node.query
            .registered_bindings
            .get(&shape.shape_id())
            .unwrap()
            .contains_key(&(usage_binding_id, usage_subscription.read_view, None))
    );
    assert!(matches!(
        node.apply_sync_message_settled(SyncMessage::Subscribe(crate::protocol::Subscribe {
            shape_id: shape.shape_id(),
            subscription: usage_subscription,
            values: Vec::new(),
            known_state: None,
            delegated_session: None,
        })),
        Err(Error::InvalidStoredValue("binding arity mismatch"))
    ));

    node.apply_sync_message_settled(SyncMessage::Subscribe(crate::protocol::Subscribe {
        shape_id: shape.shape_id(),
        subscription: usage_subscription,
        values: values.clone(),
        known_state: None,
        delegated_session: None,
    }))
    .unwrap();
    node.apply_sync_message_settled(SyncMessage::Subscribe(crate::protocol::Subscribe {
        shape_id: shape.shape_id(),
        subscription: other_usage_subscription,
        values,
        known_state: None,
        delegated_session: None,
    }))
    .unwrap();
    assert!(
        node.query.registered_bindings
            .get(&shape.shape_id())
            .unwrap()
            .contains_key(&(usage_binding_id, usage_subscription.read_view, None))
    );
    assert!(
        node.query.registered_bindings
            .get(&shape.shape_id())
            .unwrap()
            .contains_key(&(other_usage_binding_id, other_usage_subscription.read_view, None))
    );

    node.apply_sync_message_settled(SyncMessage::Unsubscribe {
        subscription: usage_subscription,
    })
    .unwrap();
    assert!(
        !node.query.registered_bindings
            .get(&shape.shape_id())
            .unwrap()
            .contains_key(&(usage_binding_id, usage_subscription.read_view, None))
    );
    assert!(
        node.query.registered_bindings
            .get(&shape.shape_id())
            .unwrap()
            .contains_key(&(other_usage_binding_id, other_usage_subscription.read_view, None))
    );

    node.apply_sync_message_settled(SyncMessage::Unsubscribe {
        subscription: other_usage_subscription,
    })
    .unwrap();
    assert!(
        !node.query.registered_bindings
            .get(&shape.shape_id())
            .unwrap()
            .contains_key(&(other_usage_binding_id, other_usage_subscription.read_view, None))
    );
}

#[test]
fn binding_delta_cleanup_distinguishes_canonical_read_view() {
    let schema = branch_view_schema();
    let (_temp_dir, mut node) = open_node_with_schema(node(0x44), schema.clone());
    let shape = Query::from("todos")
        .filter(eq(col("title"), param("wanted")))
        .validate(&schema)
        .unwrap();
    let values = vec![Value::String("match".to_owned())];
    let branch_opts = crate::protocol::RegisterShapeOptions {
        read_view: crate::protocol::ReadViewSpec {
            source: crate::protocol::ReadViewSourceSpec::BranchView {
                head: branch_selector(0x44),
                base: None,
            },
        },
        ..Default::default()
    };
    let branch_read_view = branch_opts.read_view_key();
    assert_ne!(branch_read_view, ReadViewKey::default());
    let default_usage_subscription = SubscriptionKey {
        shape_id: shape.shape_id(),
        binding_id: BindingId(uuid::Uuid::from_bytes([0x77; 16])),
        read_view: Default::default(),
    };
    let branch_usage_subscription = SubscriptionKey {
        shape_id: shape.shape_id(),
        // The usage handle alone is not globally unique: relays may reuse the
        // canonical binding id for distinct downstream read views.
        binding_id: default_usage_subscription.binding_id,
        read_view: branch_read_view,
    };

    node.apply_sync_message_settled(SyncMessage::RegisterShape {
        shape_id: shape.shape_id(),
        ast: crate::protocol::ShapeAst::from_validated(&shape),
        opts: crate::protocol::RegisterShapeOptions::default(),
    })
    .unwrap();
    node.apply_sync_message_settled(SyncMessage::Subscribe(crate::protocol::Subscribe {
        shape_id: shape.shape_id(),
        subscription: default_usage_subscription,
        values: values.clone(),
        known_state: None,
        delegated_session: None,
    }))
    .unwrap();
    node.apply_sync_message_settled(SyncMessage::RegisterShape {
        shape_id: shape.shape_id(),
        ast: crate::protocol::ShapeAst::from_validated(&shape),
        opts: branch_opts,
    })
    .unwrap();
    node.apply_sync_message_settled(SyncMessage::Subscribe(crate::protocol::Subscribe {
        shape_id: shape.shape_id(),
        subscription: branch_usage_subscription,
        values,
        known_state: None,
        delegated_session: None,
    }))
    .unwrap();

    // The same usage handle may name distinct read views. Unsubscribing one
    // must retain the other registered binding usage.
    node.apply_sync_message_settled(SyncMessage::Unsubscribe {
        subscription: default_usage_subscription,
    })
    .unwrap();
    assert!(
        node.query
            .registered_bindings
            .get(&shape.shape_id())
            .unwrap()
            .contains_key(&(
                branch_usage_subscription.binding_id,
                branch_read_view,
                None,
            ))
    );

    node.apply_sync_message_settled(SyncMessage::Unsubscribe {
        subscription: branch_usage_subscription,
    })
    .unwrap();
    assert!(!node
        .query
        .registered_bindings
        .get(&shape.shape_id())
        .is_some_and(|bindings| {
            bindings.contains_key(&(
                branch_usage_subscription.binding_id,
                branch_read_view,
                None,
            ))
        }));
}

#[test]
fn prepared_query_lowering_supports_ne_parameter_predicates() {
    let (_temp_dir, mut node) = open_node();
    node.commit_mergeable_settled(MergeableCommit::new("todos", row(0x31), 10).cells(title_cells("keep")))
        .unwrap();
    let shape = Query::from("todos")
        .filter(ne(col("title"), param("blocked")))
        .validate(&schema())
        .unwrap();
    let binding = shape
        .bind(BTreeMap::from([(
            "blocked".to_owned(),
            Value::String("drop".to_owned()),
        )]))
        .unwrap();

    let rows = node
        .query_rows(&shape, &binding, DurabilityTier::Local)
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].cell(&schema().tables[0], "title"), Some(v("keep")));
}

fn relation_snapshot_schema() -> JazzSchema {
    build_public_test_schema(
        PublicSchemaBuilder::new()
            .table(
                PublicTableSchemaBuilder::new("users").column("name", PublicColumnType::Text),
            )
            .table(
                PublicTableSchemaBuilder::new("todos")
                    .column("title", PublicColumnType::Text)
                    .fk_column("owner_id", "users"),
            )
            .table(
                PublicTableSchemaBuilder::new("comments")
                    .column("body", PublicColumnType::Text)
                    .fk_column("todo_id", "todos"),
            ),
    )
}

fn relation_snapshot_policy_schema() -> JazzSchema {
    build_public_test_schema(
        PublicSchemaBuilder::new()
            .table(
                PublicTableSchemaBuilder::new("users").column("name", PublicColumnType::Text),
            )
            .table(
                PublicTableSchemaBuilder::new("todos")
                    .column("title", PublicColumnType::Text)
                    .fk_column("owner_id", "users")
                    .policies(public_owner_policies("owner_id")),
            )
            .table(
                PublicTableSchemaBuilder::new("comments")
                    .column("body", PublicColumnType::Text)
                    .fk_column("todo_id", "todos"),
            ),
    )
}

fn routed_nested_collector_schema() -> JazzSchema {
    build_public_test_schema(
        PublicSchemaBuilder::new()
            .table(
                PublicTableSchemaBuilder::new("users").column("name", PublicColumnType::Text),
            )
            .table(
                PublicTableSchemaBuilder::new("todos")
                    .column("title", PublicColumnType::Text)
                    .fk_column("owner_id", "users")
                    .policies(
                        PublicTablePolicies::new().with_select(PublicPolicyExpr::eq_session(
                            "owner_id",
                            vec!["claims".to_owned(), "sub".to_owned()],
                        )),
                    ),
            )
            .table(
                PublicTableSchemaBuilder::new("comments")
                    .column("body", PublicColumnType::Text)
                    .fk_column("todo_id", "todos"),
            )
            .table(
                PublicTableSchemaBuilder::new("attachments")
                    .column("name", PublicColumnType::Text)
                    .fk_column("todo_id", "todos"),
            ),
    )
}

fn forward_include_schema() -> JazzSchema {
    build_public_test_schema(
        PublicSchemaBuilder::new()
            .table(
                PublicTableSchemaBuilder::new("profiles")
                    .column("name", PublicColumnType::Text)
                    .nullable_fk_column("best_friend", "profiles"),
            )
            .table(
                PublicTableSchemaBuilder::new("groups")
                    .column("name", PublicColumnType::Text)
                    .nullable_fk_column("profile", "profiles")
                    .array_fk_column("members", "profiles"),
            ),
    )
}

#[test]
fn required_forward_include_allows_null_scalar_but_requires_every_array_member() {
    let schema = forward_include_schema();
    let (_temp_dir, mut node) = open_node_with_schema(node(0x80), schema.clone());
    let profile_a = row(0xa1);
    let profile_b = row(0xb1);
    let complete = row(0xc1);
    let partial = row(0xc2);
    let null_scalar = row(0xc3);

    node.commit_mergeable_settled(
        MergeableCommit::new("profiles", profile_a, 10).cells(BTreeMap::from([(
            "name".to_owned(),
            v("a"),
        )])),
    )
    .unwrap();
    node.commit_mergeable_settled(
        MergeableCommit::new("profiles", profile_b, 11).cells(BTreeMap::from([(
            "name".to_owned(),
            v("b"),
        ), (
            "best_friend".to_owned(),
            Value::Nullable(None),
        )])),
    )
    .unwrap();
    node.commit_mergeable_settled(
        MergeableCommit::new("groups", complete, 12).cells(BTreeMap::from([
            ("name".to_owned(), v("complete")),
            ("profile".to_owned(), Value::Nullable(None)),
            (
                "members".to_owned(),
                Value::Array(vec![Value::Uuid(profile_a.0), Value::Uuid(profile_b.0)]),
            ),
        ])),
    )
    .unwrap();
    node.commit_mergeable_settled(
        MergeableCommit::new("groups", partial, 13).cells(BTreeMap::from([
            ("name".to_owned(), v("partial")),
            ("profile".to_owned(), Value::Nullable(None)),
            (
                "members".to_owned(),
                Value::Array(vec![Value::Uuid(profile_a.0), Value::Uuid(row(0xff).0)]),
            ),
        ])),
    )
    .unwrap();
    node.commit_mergeable_settled(
        MergeableCommit::new("groups", null_scalar, 14).cells(BTreeMap::from([
            ("name".to_owned(), v("null-scalar")),
            ("profile".to_owned(), Value::Nullable(None)),
            ("members".to_owned(), Value::Array(Vec::new())),
        ])),
    )
    .unwrap();

    let shape = Query::from("groups")
        .include_with(crate::query::Include::new("profile").require_includes())
        .include_with(crate::query::Include::new("members").require_includes())
        .validate(&schema)
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let rows = node
        .query_rows_for_link(&shape, &binding, DurabilityTier::Local, AuthorSubject::SYSTEM)
        .unwrap();

    assert_eq!(
        rows.iter().map(CurrentRow::row_uuid).collect::<BTreeSet<_>>(),
        BTreeSet::from([complete, null_scalar])
    );
}

#[test]
fn nested_required_include_checks_every_array_member_recursively() {
    let schema = forward_include_schema();
    let (_temp_dir, mut node) = open_node_with_schema(node(0x81), schema.clone());
    let profile_a = row(0xa1);
    let profile_b = row(0xb1);
    let complete = row(0xc1);
    let nested_partial = row(0xc2);

    node.commit_mergeable_settled(
        MergeableCommit::new("profiles", profile_a, 10).cells(BTreeMap::from([
            ("name".to_owned(), v("a")),
            ("best_friend".to_owned(), Value::Nullable(None)),
        ])),
    )
    .unwrap();
    node.commit_mergeable_settled(
        MergeableCommit::new("profiles", profile_b, 11).cells(BTreeMap::from([
            ("name".to_owned(), v("b")),
            (
                "best_friend".to_owned(),
                Value::Nullable(Some(Box::new(Value::Uuid(row(0xee).0)))),
            ),
        ])),
    )
    .unwrap();
    node.commit_mergeable_settled(
        MergeableCommit::new("groups", complete, 12).cells(BTreeMap::from([
            ("name".to_owned(), v("complete")),
            ("profile".to_owned(), Value::Nullable(None)),
            ("members".to_owned(), Value::Array(vec![Value::Uuid(profile_a.0)])),
        ])),
    )
    .unwrap();
    node.commit_mergeable_settled(
        MergeableCommit::new("groups", nested_partial, 13).cells(BTreeMap::from([
            ("name".to_owned(), v("nested-partial")),
            ("profile".to_owned(), Value::Nullable(None)),
            (
                "members".to_owned(),
                Value::Array(vec![Value::Uuid(profile_a.0), Value::Uuid(profile_b.0)]),
            ),
        ])),
    )
    .unwrap();

    let shape = Query::from("groups")
        .include_with(crate::query::Include::new("members.best_friend").require_includes())
        .validate(&schema)
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let rows = node
        .query_rows_for_link(&shape, &binding, DurabilityTier::Local, AuthorSubject::SYSTEM)
        .unwrap();

    assert_eq!(
        rows.iter().map(CurrentRow::row_uuid).collect::<BTreeSet<_>>(),
        BTreeSet::from([complete])
    );
}

#[test]
fn array_subquery_match_correlation_cardinality_requires_every_referenced_member() {
    let schema = forward_include_schema();
    let (_temp_dir, mut node) = open_node_with_schema(node(0x82), schema.clone());
    let profile_a = row(0xa1);
    let profile_b = row(0xb1);
    let complete = row(0xc1);
    let partial = row(0xc2);
    let empty = row(0xc3);

    for (idx, profile) in [profile_a, profile_b].into_iter().enumerate() {
        node.commit_mergeable_settled(
            MergeableCommit::new("profiles", profile, 10 + idx as u64).cells(BTreeMap::from([
                ("name".to_owned(), v("profile")),
                ("best_friend".to_owned(), Value::Nullable(None)),
            ])),
        )
        .unwrap();
    }
    node.commit_mergeable_settled(
        MergeableCommit::new("groups", complete, 12).cells(BTreeMap::from([
            ("name".to_owned(), v("complete")),
            ("profile".to_owned(), Value::Nullable(None)),
            (
                "members".to_owned(),
                Value::Array(vec![Value::Uuid(profile_a.0), Value::Uuid(profile_b.0)]),
            ),
        ])),
    )
    .unwrap();
    node.commit_mergeable_settled(
        MergeableCommit::new("groups", partial, 13).cells(BTreeMap::from([
            ("name".to_owned(), v("partial")),
            ("profile".to_owned(), Value::Nullable(None)),
            (
                "members".to_owned(),
                Value::Array(vec![Value::Uuid(profile_a.0), Value::Uuid(row(0xee).0)]),
            ),
        ])),
    )
    .unwrap();
    node.commit_mergeable_settled(
        MergeableCommit::new("groups", empty, 14).cells(BTreeMap::from([
            ("name".to_owned(), v("empty")),
            ("profile".to_owned(), Value::Nullable(None)),
            ("members".to_owned(), Value::Array(Vec::new())),
        ])),
    )
    .unwrap();

    let shape = Query::from("groups")
        .array_subquery(
            ArraySubquery::new("memberRows", "profiles", "id", "members")
                .requirement(crate::query::ArraySubqueryRequirement::MatchCorrelationCardinality)
                ,
        )
        .validate(&schema)
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let snapshot = node
        .query_relation_snapshot_for_serving(&shape, &binding, DurabilityTier::Local, AuthorSubject::SYSTEM)
        .unwrap();

    assert_eq!(
        snapshot
            .rows
            .iter()
            .filter(|row| row.table() == "groups")
            .map(CurrentRow::row_uuid)
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([complete, empty])
    );
    let complete = snapshot
        .rows
        .iter()
        .find(|row| row.row_uuid() == complete)
        .expect("complete group terminal row");
    let Value::Array(members) = complete.raw_field("memberRows").expect("memberRows") else {
        panic!("expected memberRows array");
    };
    assert_eq!(members.len(), 2);
}

#[test]
fn rows_skipped_by_require_includes_affect_limit_offset_pagination() {
    let schema = forward_include_schema();
    let (_temp_dir, mut node) = open_node_with_schema(node(0x83), schema.clone());
    let profile_a = row(0xa1);
    let profile_b = row(0xb1);
    let partial_first = row(0xc1);
    let complete_first = row(0xc2);
    let partial_second = row(0xc3);
    let complete_second = row(0xc4);
    let complete_third = row(0xc5);

    for (idx, profile) in [profile_a, profile_b].into_iter().enumerate() {
        node.commit_mergeable_settled(
            MergeableCommit::new("profiles", profile, 10 + idx as u64).cells(BTreeMap::from([
                ("name".to_owned(), v("profile")),
                ("best_friend".to_owned(), Value::Nullable(None)),
            ])),
        )
        .unwrap();
    }
    for (idx, (group, name, members)) in [
        (
            partial_first,
            "a-partial",
            vec![Value::Uuid(profile_a.0), Value::Uuid(row(0xea).0)],
        ),
        (complete_first, "b-complete", vec![Value::Uuid(profile_a.0)]),
        (
            partial_second,
            "c-partial",
            vec![Value::Uuid(profile_b.0), Value::Uuid(row(0xeb).0)],
        ),
        (complete_second, "d-complete", vec![Value::Uuid(profile_b.0)]),
        (complete_third, "e-complete", vec![Value::Uuid(profile_a.0)]),
    ]
    .into_iter()
    .enumerate()
    {
        node.commit_mergeable_settled(
            MergeableCommit::new("groups", group, 20 + idx as u64).cells(BTreeMap::from([
                ("name".to_owned(), v(name)),
                ("profile".to_owned(), Value::Nullable(None)),
                ("members".to_owned(), Value::Array(members)),
            ])),
        )
        .unwrap();
    }

    let shape = Query::from("groups")
        .array_subquery(
            ArraySubquery::new("memberRows", "profiles", "id", "members")
                .requirement(crate::query::ArraySubqueryRequirement::MatchCorrelationCardinality)
                ,
        )
        .order_by("name", crate::query::OrderDirection::Asc)
        .offset(1)
        .limit(2)
        .validate(&schema)
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let snapshot = node
        .query_relation_snapshot_for_serving(&shape, &binding, DurabilityTier::Local, AuthorSubject::SYSTEM)
        .unwrap();

    assert_eq!(
        snapshot
            .rows
            .iter()
            .filter(|row| row.table() == "groups")
            .map(CurrentRow::row_uuid)
            .collect::<Vec<_>>(),
        vec![complete_second, complete_third]
    );
}

#[test]
fn relation_snapshot_single_level_array_uses_query_engine_edges() {
    let schema = relation_snapshot_schema();
    let (_temp_dir, mut node) = open_node_with_schema(node(0x47), schema.clone());
    let alice = row(0xa1);
    let bob = row(0xb1);
    let todo_a = row(0x11);
    let todo_b = row(0x12);

    node.commit_mergeable_settled(
        MergeableCommit::new("users", alice, 10).cells(BTreeMap::from([(
            "name".to_owned(),
            v("alice"),
        )])),
    )
    .unwrap();
    node.commit_mergeable_settled(
        MergeableCommit::new("users", bob, 11).cells(BTreeMap::from([("name".to_owned(), v("bob"))])),
    )
    .unwrap();
    node.commit_mergeable_settled(
        MergeableCommit::new("todos", todo_a, 12).cells(BTreeMap::from([
            ("title".to_owned(), v("alpha")),
            ("owner_id".to_owned(), Value::Uuid(alice.0)),
        ])),
    )
    .unwrap();
    node.commit_mergeable_settled(
        MergeableCommit::new("todos", todo_b, 13).cells(BTreeMap::from([
            ("title".to_owned(), v("beta")),
            ("owner_id".to_owned(), Value::Uuid(bob.0)),
        ])),
    )
    .unwrap();

    let shape = Query::from("users")
        .filter(eq(col("id"), lit(Value::Uuid(alice.0))))
        .array_subquery(ArraySubquery::new("todosViaOwner", "todos", "owner_id", "id"))
        .validate(&schema)
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();

    let snapshot = node
        .query_relation_snapshot_for_serving(&shape, &binding, DurabilityTier::Local, AuthorSubject::SYSTEM)
        .unwrap();

    assert_eq!(snapshot.rows.len(), 1);
    assert_eq!(snapshot.rows[0].row_uuid(), alice);
    assert!(snapshot.edges.is_empty());
    let (descriptor, raw) = snapshot.rows[0].encoded_record();
    let Value::Array(todos) = descriptor.bind(raw).get("todosViaOwner").unwrap() else {
        panic!("expected terminal todo array")
    };
    let Value::Record(todo) = &todos[0] else {
        panic!("expected terminal todo record")
    };
    assert_eq!(todo.get("row_uuid"), Ok(Value::Uuid(todo_a.0)));
}

#[test]
fn relation_snapshot_materializes_reverse_array_edges() {
    let schema = relation_snapshot_schema();
    let (_temp_dir, mut node) = open_node_with_schema(node(0x44), schema.clone());
    let alice = row(0xa1);
    let bob = row(0xb1);
    let todo_a = row(0x11);
    let todo_b = row(0x12);
    let comment = row(0xc1);

    node.commit_mergeable_settled(
        MergeableCommit::new("users", alice, 10).cells(BTreeMap::from([(
            "name".to_owned(),
            v("alice"),
        )])),
    )
    .unwrap();
    node.commit_mergeable_settled(
        MergeableCommit::new("users", bob, 11).cells(BTreeMap::from([(
            "name".to_owned(),
            v("bob"),
        )])),
    )
    .unwrap();
    node.commit_mergeable_settled(
        MergeableCommit::new("todos", todo_a, 12).cells(BTreeMap::from([
            ("title".to_owned(), v("alpha")),
            ("owner_id".to_owned(), Value::Uuid(alice.0)),
        ])),
    )
    .unwrap();
    node.commit_mergeable_settled(
        MergeableCommit::new("todos", todo_b, 13).cells(BTreeMap::from([
            ("title".to_owned(), v("beta")),
            ("owner_id".to_owned(), Value::Uuid(bob.0)),
        ])),
    )
    .unwrap();
    node.commit_mergeable_settled(
        MergeableCommit::new("comments", comment, 14).cells(BTreeMap::from([
            ("body".to_owned(), v("nested")),
            ("todo_id".to_owned(), Value::Uuid(todo_a.0)),
        ])),
    )
    .unwrap();

    let shape = Query::from("users")
        .filter(eq(col("id"), lit(Value::Uuid(alice.0))))
        .array_subquery(
            ArraySubquery::new("todosViaOwner", "todos", "owner_id", "id")
                .nested(ArraySubquery::new("commentsViaTodo", "comments", "todo_id", "id")),
        )
        .validate(&schema)
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();

    let snapshot = node
        .query_relation_snapshot_for_serving(&shape, &binding, DurabilityTier::Local, AuthorSubject::SYSTEM)
        .unwrap();

    assert_eq!(snapshot.rows.len(), 1);
    assert_eq!(snapshot.rows[0].row_uuid(), alice);
    assert!(snapshot.edges.is_empty());
    let (descriptor, raw) = snapshot.rows[0].encoded_record();
    let Value::Array(todos) = descriptor.bind(raw).get("todosViaOwner").unwrap() else {
        panic!("expected terminal todo array")
    };
    let Value::Record(todo) = &todos[0] else {
        panic!("expected terminal todo record")
    };
    assert_eq!(todo.get("row_uuid"), Ok(Value::Uuid(todo_a.0)));
    let Value::Array(comments) = todo.get("commentsViaTodo").unwrap() else {
        panic!("expected terminal comment array")
    };
    let Value::Record(comment_row) = &comments[0] else {
        panic!("expected terminal comment record")
    };
    assert_eq!(comment_row.get("row_uuid"), Ok(Value::Uuid(comment.0)));
}

#[test]
fn relation_snapshot_array_subquery_filters_use_parent_binding_params() {
    let schema = relation_snapshot_schema();
    let (_temp_dir, mut node) = open_node_with_schema(node(0x46), schema.clone());
    let alice = row(0xa1);
    let bob = row(0xb1);
    let matching_todo = row(0x11);
    let filtered_todo = row(0x12);

    node.commit_mergeable_settled(
        MergeableCommit::new("users", alice, 10).cells(BTreeMap::from([(
            "name".to_owned(),
            v("alice"),
        )])),
    )
    .unwrap();
    node.commit_mergeable_settled(
        MergeableCommit::new("users", bob, 11).cells(BTreeMap::from([(
            "name".to_owned(),
            v("bob"),
        )])),
    )
    .unwrap();
    node.commit_mergeable_settled(
        MergeableCommit::new("todos", matching_todo, 12).cells(BTreeMap::from([
            ("title".to_owned(), v("keep")),
            ("owner_id".to_owned(), Value::Uuid(alice.0)),
        ])),
    )
    .unwrap();
    node.commit_mergeable_settled(
        MergeableCommit::new("todos", filtered_todo, 13).cells(BTreeMap::from([
            ("title".to_owned(), v("drop")),
            ("owner_id".to_owned(), Value::Uuid(bob.0)),
        ])),
    )
    .unwrap();

    let shape = Query::from("users")
        .array_subquery(
            ArraySubquery::new("todosViaOwner", "todos", "owner_id", "id")
                .filter(eq(col("title"), param("wanted")))
                .requirement(crate::query::ArraySubqueryRequirement::AtLeastOne)
                ,
        )
        .validate(&schema)
        .unwrap();
    let binding = shape
        .bind(BTreeMap::from([(
            "wanted".to_owned(),
            Value::String("keep".to_owned()),
        )]))
        .unwrap();

    let snapshot = node
        .query_relation_snapshot_for_serving(&shape, &binding, DurabilityTier::Local, AuthorSubject::SYSTEM)
        .unwrap();

    assert_eq!(
        snapshot
            .rows
            .iter()
            .map(|row| (row.table().to_owned(), row.row_uuid()))
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([("users".to_owned(), alice)])
    );
    let Value::Array(todos) = snapshot.rows[0]
        .raw_field("todosViaOwner")
        .expect("todosViaOwner")
    else {
        panic!("expected todosViaOwner array");
    };
    assert_eq!(todos.len(), 1);
    let Value::Record(todo) = &todos[0] else {
        panic!("expected nested todo record");
    };
    assert_eq!(todo.get_idx(0), Ok(Value::Uuid(matching_todo.0)));
}

#[test]
fn relation_snapshot_filters_unreadable_children_and_required_parents() {
    let schema = relation_snapshot_policy_schema();
    let (_temp_dir, mut node) = open_node_with_schema(node(0x45), schema.clone());
    let parent = row(0xa1);
    let child = row(0x11);
    let alice = user(0xa1);
    let bob = user(0xb1);
    install_test_uuid_sub_claim(&mut node, alice);
    install_test_uuid_sub_claim(&mut node, bob);

    node.commit_mergeable_settled(
        MergeableCommit::new("users", parent, 10).cells(BTreeMap::from([(
            "name".to_owned(),
            v("parent"),
        )])),
    )
    .unwrap();
    node.commit_mergeable_settled(
        MergeableCommit::new("todos", child, 11).cells(BTreeMap::from([
            ("title".to_owned(), v("hidden")),
            ("owner_id".to_owned(), Value::Uuid(alice.test_uuid())),
        ])),
    )
    .unwrap();

    let optional_shape = Query::from("users")
        .array_subquery(ArraySubquery::new("todosViaOwner", "todos", "owner_id", "id"))
        .validate(&schema)
        .unwrap();
    let optional_binding = optional_shape.bind(BTreeMap::new()).unwrap();

    let optional = node
        .query_relation_snapshot_for_serving(
            &optional_shape,
            &optional_binding,
            DurabilityTier::Local,
            bob,
        )
        .unwrap();
    assert_eq!(
        optional
            .rows
            .iter()
            .map(|row| (row.table().to_owned(), row.row_uuid()))
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([("users".to_owned(), parent)])
    );
    assert!(optional.edges.is_empty());

    let required_shape = Query::from("users")
        .array_subquery(
            ArraySubquery::new("todosViaOwner", "todos", "owner_id", "id")
                .requirement(crate::query::ArraySubqueryRequirement::AtLeastOne)
                ,
        )
        .validate(&schema)
        .unwrap();
    let required_binding = required_shape.bind(BTreeMap::new()).unwrap();

    let required = node
        .query_relation_snapshot_for_serving(
            &required_shape,
            &required_binding,
            DurabilityTier::Local,
            bob,
        )
        .unwrap();
    assert!(required.rows.is_empty());
    assert!(required.edges.is_empty());
}

#[test]
fn maintained_array_collector_retains_authorized_parent_trees_incrementally() {
    // The existing public subscription tests cover the flat v3 delivery
    // vocabulary. This deliberately inspects the maintained terminal because
    // the structured carrier is not on that wire yet; it proves the retained
    // state that the future peer view-update builder will consume.
    let schema = relation_snapshot_policy_schema();
    let (_temp_dir, mut node) = open_node_with_schema(node(0x46), schema.clone());
    let alice = user(0xa1);
    let bob = user(0xb1);
    install_test_uuid_sub_claim(&mut node, alice);
    install_test_uuid_sub_claim(&mut node, bob);
    let alice_parent = row(0xa1);
    let bob_parent = row(0xb1);
    let visible_child = row(0x11);
    let denied_child = row(0x12);
    let visible_grandchild = row(0x21);

    for (parent, name, time) in [(alice_parent, "alice", 10), (bob_parent, "bob", 11)] {
        node.commit_mergeable_settled(
            MergeableCommit::new("users", parent, time)
                .cells(BTreeMap::from([("name".to_owned(), v(name))])),
        )
        .unwrap();
    }
    node.commit_mergeable_settled(
        MergeableCommit::new("comments", visible_grandchild, 14).cells(BTreeMap::from([
            ("body".to_owned(), v("visible nested")),
            ("todo_id".to_owned(), Value::Uuid(visible_child.0)),
        ])),
    )
    .unwrap();
    for (child, owner, title, time) in [
        (visible_child, alice, "visible", 12),
        (denied_child, bob, "denied", 13),
    ] {
        node.commit_mergeable_settled(
            MergeableCommit::new("todos", child, time).cells(BTreeMap::from([
                ("title".to_owned(), v(title)),
                ("owner_id".to_owned(), Value::Uuid(owner.test_uuid())),
            ])),
        )
        .unwrap();
    }

    let shape = Query::from("users")
        .array_subquery(
            ArraySubquery::new("todosViaOwner", "todos", "owner_id", "id")
                .select(["title"])
                .nested(
                    ArraySubquery::new("commentsViaTodo", "comments", "todo_id", "id")
                        .select(["body"])
                        ,
                ),
        )
        .validate(&schema)
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let (subscription, mut maintained, terminal_schemas, _transitions, tables, _incomplete) = node
        .open_seeded_maintained_subscription_view(
            &shape,
            &binding,
            alice,
            DurabilityTier::Local,
            &crate::protocol::ReadViewSpec::default(),
        )
        .unwrap();

    let alice_tree = maintained
        .structured_app_row(alice_parent)
        .expect("collector retained alice parent tree");
    let Value::Array(alice_children) = alice_tree
        .get("todosViaOwner")
        .expect("collector relation field")
    else {
        panic!("collector relation must be an array");
    };
    assert_eq!(alice_children.len(), 1, "planted authorized child is retained");
    let Value::Record(visible_child_tree) = &alice_children[0] else {
        panic!("collector relation member must be a record");
    };
    let Value::Array(visible_grandchildren) = visible_child_tree
        .get("commentsViaTodo")
        .expect("collector nested relation field")
    else {
        panic!("collector nested relation must be an array");
    };
    assert_eq!(
        visible_grandchildren.len(),
        1,
        "collector recursively retains the planted nested child"
    );
    let bob_tree = maintained
        .structured_app_row(bob_parent)
        .expect("collector retained bob parent tree");
    let Value::Array(bob_children) = bob_tree
        .get("todosViaOwner")
        .expect("collector relation field")
    else {
        panic!("collector relation must be an array");
    };
    assert!(
        bob_children.is_empty(),
        "denied child must be absent while its optional parent retains an empty collection"
    );
    let bob_before = bob_tree.raw().to_vec();

    node.commit_mergeable_settled(
        MergeableCommit::new("todos", row(0x13), 15).cells(BTreeMap::from([
            ("title".to_owned(), v("new visible")),
            ("owner_id".to_owned(), Value::Uuid(alice.test_uuid())),
        ])),
    )
    .unwrap();
    crate::db::block_on(node.drive_query_runtime()).unwrap();
    let mut changed_root_keys = BTreeSet::new();
    while let Ok(deltas) = subscription.try_recv() {
        let transitions = maintained
            .apply_multisink_deltas(deltas, &terminal_schemas, &tables, &node.node_aliases)
            .unwrap();
        changed_root_keys.extend(
            transitions
                .terminal_operations
                .into_iter()
                .map(|operation| operation.root_key),
        );
    }

    assert_eq!(
        changed_root_keys.len(),
        1,
        "one child change patches only its rendered parent"
    );
    let updated_alice_tree = maintained
        .structured_app_row(alice_parent)
        .expect("collector retained updated alice parent tree");
    let Value::Array(alice_children) = updated_alice_tree
        .get("todosViaOwner")
        .expect("collector relation field")
    else {
        panic!("collector relation must be an array");
    };
    assert_eq!(alice_children.len(), 2, "child change replaces only its parent tree");
    assert_eq!(
        maintained
            .structured_app_row(bob_parent)
            .expect("collector retained unaffected bob parent tree")
            .raw(),
        bob_before,
        "unaffected parent tree is retained byte-for-byte"
    );
    node.unsubscribe_groove_subscription(subscription.id());
}

#[test]
fn maintained_nested_collector_keeps_two_route_keys_internal_across_sibling_arrays() {
    // The root filter and owner policy contribute independent route keys. The
    // nested siblings deliberately share the same routed todo so this covers
    // lowering and maintained runtime, not only a hand-built Groove graph.
    let schema = routed_nested_collector_schema();
    let (_temp_dir, mut node) = open_node_with_schema(node(0x47), schema.clone());
    let alice = user(0xa1);
    install_test_uuid_sub_claim(&mut node, alice);
    let parent = row(0xa1);
    let todo_row = row(0x11);
    let comment = row(0x21);
    let attachment = row(0x31);

    node.commit_mergeable_settled(
        MergeableCommit::new("users", parent, 10)
            .cells(BTreeMap::from([("name".to_owned(), v("alice"))])),
    )
    .unwrap();
    node.commit_mergeable_settled(
        MergeableCommit::new("todos", todo_row, 11).cells(BTreeMap::from([
            ("title".to_owned(), v("owned")),
            ("owner_id".to_owned(), Value::Uuid(alice.test_uuid())),
        ])),
    )
    .unwrap();
    node.commit_mergeable_settled(
        MergeableCommit::new("comments", comment, 12).cells(BTreeMap::from([
            ("body".to_owned(), v("first comment")),
            ("todo_id".to_owned(), Value::Uuid(todo_row.0)),
        ])),
    )
    .unwrap();
    node.commit_mergeable_settled(
        MergeableCommit::new("attachments", attachment, 13).cells(BTreeMap::from([
            ("name".to_owned(), v("first attachment")),
            ("todo_id".to_owned(), Value::Uuid(todo_row.0)),
        ])),
    )
    .unwrap();

    let shape = Query::from("users")
        .filter(eq(col("name"), param("rootName")))
        .array_subquery(
            ArraySubquery::new("todosViaOwner", "todos", "owner_id", "id")
                .select(["title"])
                .nested(
                    ArraySubquery::new("commentsViaTodo", "comments", "todo_id", "id")
                        .select(["body"]),
                )
                .nested(
                    ArraySubquery::new(
                        "attachmentsViaTodo",
                        "attachments",
                        "todo_id",
                        "id",
                    )
                    .select(["name"]),
                ),
        )
        .validate(&schema)
        .unwrap();
    let binding = shape
        .bind(BTreeMap::from([("rootName".to_owned(), v("alice"))]))
        .unwrap();
    let (subscription, mut maintained, terminal_schemas, _transitions, tables, _incomplete) = node
        .open_seeded_maintained_subscription_view(
            &shape,
            &binding,
            alice,
            DurabilityTier::Local,
            &crate::protocol::ReadViewSpec::default(),
        )
        .unwrap();
    let root = maintained
        .structured_app_row(parent)
        .expect("collector retained routed root");
    let Value::Array(todos) = root.get("todosViaOwner").unwrap() else {
        panic!("todos must be an array");
    };
    let Value::Record(todo) = &todos[0] else {
        panic!("todos must contain records");
    };
    assert_eq!(
        todo.descriptor()
            .fields()
            .iter()
            .map(|field| field.name.as_deref())
            .collect::<Vec<_>>(),
        vec![
            Some("row_uuid"),
            Some("title"),
            Some("commentsViaTodo"),
            Some("attachmentsViaTodo"),
        ],
        "route keys must remain lowering/runtime metadata, not nested app fields"
    );
    for field in ["commentsViaTodo", "attachmentsViaTodo"] {
        let Value::Array(children) = todo.get(field).unwrap() else {
            panic!("{field} must be an array");
        };
        assert_eq!(children.len(), 1, "each sibling array retains its own child");
    }

    // A new comment changes only that sibling content, preserving the
    // attachment subtree and the one rendered parent identity.
    node.commit_mergeable_settled(
        MergeableCommit::new("comments", row(0x22), 14).cells(BTreeMap::from([
            ("body".to_owned(), v("second comment")),
            ("todo_id".to_owned(), Value::Uuid(todo_row.0)),
        ])),
    )
    .unwrap();
    crate::db::block_on(node.drive_query_runtime()).unwrap();
    let mut changed_root_keys = BTreeSet::new();
    while let Ok(deltas) = subscription.try_recv() {
        changed_root_keys.extend(
            maintained
                .apply_multisink_deltas(deltas, &terminal_schemas, &tables, &node.node_aliases)
                .unwrap()
                .terminal_operations
                .into_iter()
                .map(|operation| operation.root_key),
        );
    }
    assert_eq!(changed_root_keys.len(), 1);
    let root = maintained
        .structured_app_row(parent)
        .expect("updated routed root remains retained");
    let Value::Array(todos) = root.get("todosViaOwner").unwrap() else {
        panic!("todos must be an array");
    };
    let Value::Record(todo) = &todos[0] else {
        panic!("todos must contain records");
    };
    let Value::Array(comments) = todo.get("commentsViaTodo").unwrap() else {
        panic!("comments must be an array");
    };
    let Value::Array(attachments) = todo.get("attachmentsViaTodo").unwrap() else {
        panic!("attachments must be an array");
    };
    assert_eq!(comments.len(), 2);
    assert_eq!(attachments.len(), 1, "sibling route grouping remains isolated");
    node.unsubscribe_groove_subscription(subscription.id());
}

#[test]
fn include_deleted_one_shot_read_uses_lowered_literal_filters() {
    let (_temp_dir, mut node) = open_node();
    let table = schema().tables[0].clone();
    node.commit_mergeable_settled(MergeableCommit::new("todos", row(0x41), 10).cells(title_cells("keep")))
        .unwrap();
    node.commit_mergeable_settled(
        MergeableCommit::new("todos", row(0x42), 11).cells(title_cells("keep")),
    )
    .unwrap();
    node.commit_mergeable_settled(MergeableCommit::new("todos", row(0x42), 12).deletion(DeletionEvent::Deleted))
        .unwrap();
    node.commit_mergeable_settled(MergeableCommit::new("todos", row(0x43), 13).cells(title_cells("drop")))
        .unwrap();
    node.commit_mergeable_settled(MergeableCommit::new("todos", row(0x43), 14).deletion(DeletionEvent::Deleted))
        .unwrap();
    let shape = Query::from("todos")
        .filter(eq(col("title"), lit("keep")))
        .validate(&schema())
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();

    let rows = node
        .query_rows_including_deleted_in_authorization_mode(
            &shape,
            &binding,
            DurabilityTier::Local,
            None,
            AuthorSubject::SYSTEM,
            QueryAuthorizationMode::TrustedServing,
        )
        .unwrap();

    assert_eq!(
        rows.iter()
            .map(|row| (row.row_uuid(), row.is_deleted(), row.cell(&table, "title")))
            .collect::<Vec<_>>(),
        vec![
            (row(0x41), false, Some(v("keep"))),
            (row(0x42), true, Some(v("keep"))),
        ]
    );
}

#[test]
fn include_deleted_one_shot_read_uses_lowered_param_filters() {
    let (_temp_dir, mut node) = open_node();
    node.commit_mergeable_settled(MergeableCommit::new("todos", row(0x51), 10).cells(title_cells("match")))
        .unwrap();
    node.commit_mergeable_settled(
        MergeableCommit::new("todos", row(0x51), 11).deletion(DeletionEvent::Deleted),
    )
    .unwrap();
    node.commit_mergeable_settled(MergeableCommit::new("todos", row(0x52), 12).cells(title_cells("miss")))
        .unwrap();
    let shape = Query::from("todos")
        .filter(eq(col("title"), param("wanted")))
        .validate(&schema())
        .unwrap();
    let binding = shape
        .bind(BTreeMap::from([(
            "wanted".to_owned(),
            Value::String("match".to_owned()),
        )]))
        .unwrap();

    let rows = node
        .query_rows_including_deleted_in_authorization_mode(
            &shape,
            &binding,
            DurabilityTier::Local,
            None,
            AuthorSubject::SYSTEM,
            QueryAuthorizationMode::TrustedServing,
        )
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].row_uuid(), row(0x51));
    assert!(rows[0].is_deleted());
}

fn include_deleted_join_schema() -> JazzSchema {
    build_public_test_schema(
        PublicSchemaBuilder::new()
            .table(
                PublicTableSchemaBuilder::new("issues").column("title", PublicColumnType::Text),
            )
            .table(
                PublicTableSchemaBuilder::new("issue_tags")
                    .fk_column("issue", "issues")
                    .column("tag", PublicColumnType::Text),
            ),
    )
}

#[test]
fn include_deleted_one_shot_read_join_matches_visible_join_rows() {
    let schema = include_deleted_join_schema();
    let (_temp_dir, mut node) = open_node_with_schema(node(9), schema.clone());
    let issue = row(0x61);
    node.commit_mergeable_settled(
        MergeableCommit::new("issues", issue, 10).cells(BTreeMap::from([(
            "title".to_owned(),
            Value::String("deleted but matched".to_owned()),
        )])),
    )
    .unwrap();
    node.commit_mergeable_settled(MergeableCommit::new("issues", issue, 11).deletion(DeletionEvent::Deleted))
        .unwrap();
    node.commit_mergeable_settled(
        MergeableCommit::new("issue_tags", row(0x62), 12).cells(BTreeMap::from([
            ("issue".to_owned(), Value::Uuid(issue.0)),
            ("tag".to_owned(), Value::String("bug".to_owned())),
        ])),
    )
    .unwrap();
    let shape = Query::from("issues")
        .join_via("issue_tags", "issue", [eq(col("tag"), lit("bug"))])
        .validate(&schema)
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();

    let rows = node
        .query_rows_including_deleted_in_authorization_mode(
            &shape,
            &binding,
            DurabilityTier::Local,
            None,
            AuthorSubject::SYSTEM,
            QueryAuthorizationMode::TrustedServing,
        )
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].row_uuid(), issue);
    assert!(rows[0].is_deleted());
}

#[test]
fn include_deleted_one_shot_read_join_ignores_deleted_join_rows() {
    let schema = include_deleted_join_schema();
    let (_temp_dir, mut node) = open_node_with_schema(node(9), schema.clone());
    let issue = row(0x63);
    let tag_row = row(0x64);
    node.commit_mergeable_settled(
        MergeableCommit::new("issues", issue, 10).cells(BTreeMap::from([(
            "title".to_owned(),
            Value::String("deleted root".to_owned()),
        )])),
    )
    .unwrap();
    node.commit_mergeable_settled(MergeableCommit::new("issues", issue, 11).deletion(DeletionEvent::Deleted))
        .unwrap();
    node.commit_mergeable_settled(
        MergeableCommit::new("issue_tags", tag_row, 12).cells(BTreeMap::from([
            ("issue".to_owned(), Value::Uuid(issue.0)),
            ("tag".to_owned(), Value::String("bug".to_owned())),
        ])),
    )
    .unwrap();
    node.commit_mergeable_settled(
        MergeableCommit::new("issue_tags", tag_row, 13).deletion(DeletionEvent::Deleted),
    )
    .unwrap();
    let shape = Query::from("issues")
        .join_via("issue_tags", "issue", [eq(col("tag"), lit("bug"))])
        .validate(&schema)
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();

    let rows = node
        .query_rows_including_deleted_in_authorization_mode(
            &shape,
            &binding,
            DurabilityTier::Local,
            None,
            AuthorSubject::SYSTEM,
            QueryAuthorizationMode::TrustedServing,
        )
        .unwrap();

    assert!(rows.is_empty());
}

fn include_deleted_reachable_schema() -> JazzSchema {
    build_public_test_schema(
        PublicSchemaBuilder::new()
            .table(
                PublicTableSchemaBuilder::new("teams").column("name", PublicColumnType::Text),
            )
            .table(
                PublicTableSchemaBuilder::new("docs").column("title", PublicColumnType::Text),
            )
            .table(
                PublicTableSchemaBuilder::new("team_access")
                    .fk_column("doc", "docs")
                    .fk_column("team", "teams"),
            )
            .table(
                PublicTableSchemaBuilder::new("team_edges")
                    .fk_column("member", "teams")
                    .fk_column("parent", "teams"),
            ),
    )
}

fn include_deleted_reachable_shape(schema: &JazzSchema) -> ValidatedQuery {
    Query::from("docs")
        .reachable_via(
            "team_access",
            "doc",
            "team",
            lit(Value::Uuid(row(0x72).0)),
            "team_edges",
            "member",
            "parent",
            [],
        )
        .validate(&schema)
        .unwrap()
}

#[test]
fn include_deleted_one_shot_read_reachable_matches_deleted_roots_through_visible_edges() {
    let schema = include_deleted_reachable_schema();
    let (_temp_dir, mut node) = open_node_with_schema(node(9), schema.clone());
    let doc = row(0x73);
    node.commit_mergeable_settled(
        MergeableCommit::new("teams", row(0x71), 10).cells(BTreeMap::from([(
            "name".to_owned(),
            Value::String("parent".to_owned()),
        )])),
    )
    .unwrap();
    node.commit_mergeable_settled(
        MergeableCommit::new("teams", row(0x72), 11).cells(BTreeMap::from([(
            "name".to_owned(),
            Value::String("member".to_owned()),
        )])),
    )
    .unwrap();
    node.commit_mergeable_settled(
        MergeableCommit::new("docs", doc, 12).cells(BTreeMap::from([(
            "title".to_owned(),
            Value::String("deleted reachable".to_owned()),
        )])),
    )
    .unwrap();
    node.commit_mergeable_settled(MergeableCommit::new("docs", doc, 13).deletion(DeletionEvent::Deleted))
        .unwrap();
    node.commit_mergeable_settled(
        MergeableCommit::new("team_edges", row(0x74), 14).cells(BTreeMap::from([
            ("member".to_owned(), Value::Uuid(row(0x72).0)),
            ("parent".to_owned(), Value::Uuid(row(0x71).0)),
        ])),
    )
    .unwrap();
    node.commit_mergeable_settled(
        MergeableCommit::new("team_access", row(0x75), 15).cells(BTreeMap::from([
            ("doc".to_owned(), Value::Uuid(doc.0)),
            ("team".to_owned(), Value::Uuid(row(0x71).0)),
        ])),
    )
    .unwrap();
    let shape = include_deleted_reachable_shape(&schema);
    let binding = shape.bind(BTreeMap::new()).unwrap();

    let rows = node
        .query_rows_including_deleted_in_authorization_mode(
            &shape,
            &binding,
            DurabilityTier::Local,
            None,
            AuthorSubject::SYSTEM,
            QueryAuthorizationMode::TrustedServing,
        )
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].row_uuid(), doc);
    assert!(rows[0].is_deleted());
}

#[test]
fn include_deleted_one_shot_read_reachable_ignores_deleted_edge_rows() {
    let schema = include_deleted_reachable_schema();
    let (_temp_dir, mut node) = open_node_with_schema(node(9), schema.clone());
    let doc = row(0x76);
    let edge = row(0x77);
    node.commit_mergeable_settled(
        MergeableCommit::new("teams", row(0x71), 10).cells(BTreeMap::from([(
            "name".to_owned(),
            Value::String("parent".to_owned()),
        )])),
    )
    .unwrap();
    node.commit_mergeable_settled(
        MergeableCommit::new("teams", row(0x72), 11).cells(BTreeMap::from([(
            "name".to_owned(),
            Value::String("member".to_owned()),
        )])),
    )
    .unwrap();
    node.commit_mergeable_settled(
        MergeableCommit::new("docs", doc, 12).cells(BTreeMap::from([(
            "title".to_owned(),
            Value::String("deleted but not reached".to_owned()),
        )])),
    )
    .unwrap();
    node.commit_mergeable_settled(MergeableCommit::new("docs", doc, 13).deletion(DeletionEvent::Deleted))
        .unwrap();
    node.commit_mergeable_settled(
        MergeableCommit::new("team_edges", edge, 14).cells(BTreeMap::from([
            ("member".to_owned(), Value::Uuid(row(0x72).0)),
            ("parent".to_owned(), Value::Uuid(row(0x71).0)),
        ])),
    )
    .unwrap();
    node.commit_mergeable_settled(MergeableCommit::new("team_edges", edge, 15).deletion(DeletionEvent::Deleted))
        .unwrap();
    node.commit_mergeable_settled(
        MergeableCommit::new("team_access", row(0x78), 16).cells(BTreeMap::from([
            ("doc".to_owned(), Value::Uuid(doc.0)),
            ("team".to_owned(), Value::Uuid(row(0x71).0)),
        ])),
    )
    .unwrap();
    let shape = include_deleted_reachable_shape(&schema);
    let binding = shape.bind(BTreeMap::new()).unwrap();

    let rows = node
        .query_rows_including_deleted_in_authorization_mode(
            &shape,
            &binding,
            DurabilityTier::Local,
            None,
            AuthorSubject::SYSTEM,
            QueryAuthorizationMode::TrustedServing,
        )
        .unwrap();

    assert!(rows.is_empty());
}

#[test]
fn node_finishes_aggregation_ordering_pagination_and_projection_after_materialization() {
    let (_temp_dir, mut node) = open_node_with_schema(node(9), two_column_schema());
    for (idx, title, body) in [
        (1, "gamma", "keep"),
        (2, "alpha", "drop"),
        (3, "beta", "keep"),
    ] {
        node.commit_mergeable_settled(
            MergeableCommit::new("todos", row(idx), 10 + idx as u64).cells(BTreeMap::from([
                ("title".to_owned(), Value::String(title.to_owned())),
                ("body".to_owned(), Value::String(body.to_owned())),
            ])),
        )
        .unwrap();
    }

    let shape = Query::from("todos")
        .filter(eq(col("body"), lit("keep")))
        .select(["title"])
        .order_by("title", crate::query::OrderDirection::Asc)
        .offset(1)
        .limit(1)
        .validate(&two_column_schema())
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let rows = node
        .query_rows(&shape, &binding, DurabilityTier::Local)
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].cell(&two_column_schema().tables[0], "title"),
        Some(v("gamma"))
    );
    assert_eq!(
        rows[0].cell(&two_column_schema().tables[0], "body"),
        None,
        "projection must be applied after filtering/ordering/pagination"
    );

    let count_shape = Query::from("todos")
        .filter(eq(col("body"), lit("keep")))
        .count()
        .validate(&two_column_schema())
        .unwrap();
    let count_rows = node
        .query_rows(
            &count_shape,
            &count_shape.bind(BTreeMap::new()).unwrap(),
            DurabilityTier::Local,
        )
        .unwrap();
    assert_eq!(
        count_rows[0].test_cells_by_descriptor()["count"],
        Value::U64(2)
    );
}

#[test]
fn query_payload_dedup_is_per_peer_across_subscriptions() {
    let (_writer_dir, mut writer) = open_node_with_uuid(node(1));
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    let row = row(7);
    let _tx_id = commit_mergeable_global(
        &mut writer,
        &mut core,
        MergeableCommit::new("todos", row, 10).cells(title_cells("match")),
    );
    let all_shape = Query::from("todos").validate(&schema()).unwrap();
    let all_binding = all_shape.bind(BTreeMap::new()).unwrap();
    let filtered_shape = Query::from("todos")
        .filter(eq(col("title"), lit("match")))
        .validate(&schema())
        .unwrap();
    let filtered_binding = filtered_shape.bind(BTreeMap::new()).unwrap();
    let mut peer = PeerState::new();

    let first = peer
        .rehydrate_query(&mut core, &all_shape, &all_binding)
        .unwrap();
    let version_bundles = version_bundles_for_update(&first);
    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        peer_payload_inventory: crate::protocol::PeerPayloadInventory { complete_tx_payloads: complete_tx_payload_refs, .. },
        ..
    }) = first
    else {
        panic!("expected first view update");
    };
    assert_eq!(version_bundles.len(), 1);
    assert!(complete_tx_payload_refs.is_empty());
    assert!(peer.shipped_complete_tx_payloads().is_empty());

    let second = peer
        .rehydrate_query(&mut core, &filtered_shape, &filtered_binding)
        .unwrap();
    let version_bundles = version_bundles_for_update(&second);
    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        peer_payload_inventory: crate::protocol::PeerPayloadInventory { complete_tx_payloads: complete_tx_payload_refs, .. },
        ..
    }) = second
    else {
        panic!("expected second view update");
    };
    assert_eq!(version_bundles.len(), 1);
    assert!(complete_tx_payload_refs.is_empty());
}

#[test]
fn partial_mergeable_payload_does_not_establish_tx_level_complete_tx_ref() {
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    let first_row = row(7);
    let second_row = row(8);
    let tx_id = core
        .commit_mergeable_many_settled(vec![
            MergeableCommit::new("todos", first_row, 10).cells(title_cells("first")),
            MergeableCommit::new("todos", second_row, 10).cells(title_cells("second")),
        ])
        .unwrap();
    core.apply_fate_update(
        tx_id,
        Fate::Accepted,
        Some(GlobalTime(1)),
        Some(DurabilityTier::Global),
    )
    .unwrap();

    let first_shape = Query::from("todos")
        .filter(eq(col("title"), lit("first")))
        .validate(&schema())
        .unwrap();
    let first_binding = first_shape.bind(BTreeMap::new()).unwrap();
    let second_shape = Query::from("todos")
        .filter(eq(col("title"), lit("second")))
        .validate(&schema())
        .unwrap();
    let second_binding = second_shape.bind(BTreeMap::new()).unwrap();
    let mut peer = PeerState::new();

    let first = peer
        .rehydrate_query(&mut core, &first_shape, &first_binding)
        .unwrap();
    let version_bundles = version_bundles_for_update(&first);
    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        peer_payload_inventory: crate::protocol::PeerPayloadInventory { complete_tx_payloads: complete_tx_payload_refs, .. },
        ..
    }) = first
    else {
        panic!("expected first view update");
    };
    assert_eq!(version_bundles.len(), 1);
    assert_eq!(version_bundles[0].versions.len(), 1);
    assert!(complete_tx_payload_refs.is_empty());
    assert!(!peer.shipped_complete_tx_payloads().contains(&tx_id));

    let second = peer
        .rehydrate_query(&mut core, &second_shape, &second_binding)
        .unwrap();
    let version_bundles = version_bundles_for_update(&second);
    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        peer_payload_inventory: crate::protocol::PeerPayloadInventory { complete_tx_payloads: complete_tx_payload_refs, .. },
        ..
    }) = second
    else {
        panic!("expected second view update");
    };
    assert_eq!(version_bundles.len(), 1);
    assert_eq!(version_bundles[0].versions.len(), 1);
    assert!(complete_tx_payload_refs.is_empty());
}

#[test]
fn db_facade_current_rows_match_seeded_create_delete_sequence() {
    let db =
        crate::db::doctest_support::block_on(crate::db::doctest_support::open_todos_db()).unwrap();
    let query = db.table("todos");
    let prepared = db.prepare_query(&query).unwrap();
    let table = &crate::db::doctest_support::schema().tables[0];

    let write = db
        .insert("todos", crate::db::doctest_support::todo_cells("a1", false), Default::default())
        .unwrap();
    let row_a = write.row_uuid();
    crate::db::doctest_support::block_on(write.wait(DurabilityTier::Local)).unwrap();
    assert_eq!(db_facade_row_ids(&db.read(&prepared).unwrap()), vec![row_a]);
    assert_eq!(
        db.one(&prepared).unwrap().unwrap().cell(table, "title"),
        Some(Value::String("a1".to_owned()))
    );

    let write = db
        .insert("todos", crate::db::doctest_support::todo_cells("b1", false), Default::default())
        .unwrap();
    let row_b = write.row_uuid();
    crate::db::doctest_support::block_on(write.wait(DurabilityTier::Local)).unwrap();
    assert_eq!(
        db_facade_row_ids(
            &crate::db::doctest_support::block_on(
                db.all(&prepared, crate::db::ReadOpts::default())
            )
                .unwrap()
        ),
        vec![row_a, row_b]
    );

    crate::db::doctest_support::block_on(
        db.delete("todos", row_a, Default::default())
            .unwrap()
            .wait(DurabilityTier::Local),
    )
    .unwrap();
    assert_eq!(db_facade_row_ids(&db.read(&prepared).unwrap()), vec![row_b]);

    crate::db::doctest_support::block_on(
        db.restore("todos", row_a, Some(crate::db::doctest_support::todo_cells("a2", true)), Default::default())
        .unwrap()
        .wait(DurabilityTier::Local),
    )
    .unwrap();
    let rows = db.read(&prepared).unwrap();
    assert_eq!(db_facade_row_ids(&rows), vec![row_a, row_b]);
    assert_eq!(
        rows.iter()
            .find(|row| row.row_uuid() == row_a)
            .unwrap()
            .cell(table, "title"),
        Some(Value::String("a2".to_owned()))
    );

    crate::db::doctest_support::block_on(
        db.delete("todos", row_b, Default::default())
            .unwrap()
            .wait(DurabilityTier::Local),
    )
    .unwrap();
    assert_eq!(
        db_facade_row_ids(
            &crate::db::doctest_support::block_on(
                db.all(&prepared, crate::db::ReadOpts::default())
            )
                .unwrap()
        ),
        vec![row_a]
    );
}

#[test]
fn db_facade_multi_row_query_matches_seeded_create_delete_sequence_via_write_handles() {
    let db =
        crate::db::doctest_support::block_on(crate::db::doctest_support::open_todos_db()).unwrap();
    let query = db.table("todos");
    let prepared = db.prepare_query(&query).unwrap();
    let table = &crate::db::doctest_support::schema().tables[0];

    let write = db
        .insert("todos", crate::db::doctest_support::todo_cells("a1", false), Default::default())
        .unwrap();
    let row_a = write.row_uuid();
    crate::db::doctest_support::block_on(write.wait(DurabilityTier::Local)).unwrap();
    let rows = db.read(&prepared).unwrap();
    assert_eq!(db_facade_row_ids(&rows), vec![row_a]);
    assert_eq!(
        db.one(&prepared).unwrap().unwrap().cell(table, "title"),
        Some(Value::String("a1".to_owned()))
    );

    let write = db
        .insert("todos", crate::db::doctest_support::todo_cells("b1", false), Default::default())
        .unwrap();
    let row_b = write.row_uuid();
    crate::db::doctest_support::block_on(write.wait(DurabilityTier::Local)).unwrap();
    let rows =
        crate::db::doctest_support::block_on(db.all(&prepared, crate::db::ReadOpts::default()))
            .unwrap();
    assert_eq!(db_facade_row_ids(&rows), vec![row_a, row_b]);

    let write = db.delete("todos", row_a, Default::default()).unwrap();
    crate::db::doctest_support::block_on(write.wait(DurabilityTier::Local)).unwrap();
    let rows = db.read(&prepared).unwrap();
    assert_eq!(db_facade_row_ids(&rows), vec![row_b]);
    assert_eq!(
        db.one(&prepared).unwrap().unwrap().cell(table, "title"),
        Some(Value::String("b1".to_owned()))
    );

    let write = db
        .restore("todos", row_a, Some(crate::db::doctest_support::todo_cells("a2", true)), Default::default())
        .unwrap();
    crate::db::doctest_support::block_on(write.wait(DurabilityTier::Local)).unwrap();
    let rows = db.read(&prepared).unwrap();
    assert_eq!(db_facade_row_ids(&rows), vec![row_a, row_b]);
    assert_eq!(
        rows.iter()
            .find(|row| row.row_uuid() == row_a)
            .unwrap()
            .cell(table, "title"),
        Some(Value::String("a2".to_owned()))
    );
    assert_eq!(
        rows.iter()
            .find(|row| row.row_uuid() == row_a)
            .unwrap()
            .cell(table, "done"),
        Some(Value::Bool(true))
    );

    let write = db.delete("todos", row_b, Default::default()).unwrap();
    crate::db::doctest_support::block_on(write.wait(DurabilityTier::Local)).unwrap();
    let rows =
        crate::db::doctest_support::block_on(db.all(&prepared, crate::db::ReadOpts::default()))
            .unwrap();
    assert_eq!(db_facade_row_ids(&rows), vec![row_a]);
}

fn db_facade_row_ids(rows: &[CurrentRow]) -> Vec<RowUuid> {
    rows.iter().map(CurrentRow::row_uuid).collect()
}
