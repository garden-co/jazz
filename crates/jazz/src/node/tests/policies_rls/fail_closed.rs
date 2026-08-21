// Unsupported operands and missing claims fail closed.

#[test]
fn unsupported_policy_predicates_deny_instead_of_allowing() {
    let schema = build_public_test_schema(PublicSchemaBuilder::new().table(
        PublicTableSchemaBuilder::new("todos")
            .column("title", PublicColumnType::Text)
            .column("owner", PublicColumnType::Uuid),
    ));
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    core.catalogue.schema.runtime_mut_for_testing().tables[0].read_policy =
        Some(Query::from("todos").filter(not(contains(col("title"), lit("a")))));
    let tx = core
        .commit_mergeable(
            MergeableCommit::new("todos", row(0x83), 10).cells(owner_cells(user(0xa1), "z")),
        )
        .unwrap();
    core.apply_fate_update(
        tx,
        Fate::Accepted,
        Some(GlobalTime(1)),
        Some(DurabilityTier::Global),
    )
    .unwrap();
    let shape = Query::from("todos").validate(&core.catalogue.schema).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    assert!(
        core.query_rows_for_link(&shape, &binding, DurabilityTier::Global, user(0xa1))
            .unwrap()
            .is_empty()
    );
}

#[test]
fn unresolved_policy_operands_deny_instead_of_allowing() {
    let schema = build_public_test_schema(
        PublicSchemaBuilder::new().table(
            PublicTableSchemaBuilder::new("todos").column("title", PublicColumnType::Text),
        ),
    );
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    core.catalogue.schema.runtime_mut_for_testing().tables[0].read_policy =
        Some(Query::from("todos").filter(eq(col("title"), claim("missing"))));
    let tx = core
        .commit_mergeable(MergeableCommit::new("todos", row(0x84), 10).cells(title_cells("z")))
        .unwrap();
    core.apply_fate_update(
        tx,
        Fate::Accepted,
        Some(GlobalTime(1)),
        Some(DurabilityTier::Global),
    )
    .unwrap();
    let shape = Query::from("todos").validate(&core.catalogue.schema).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    assert!(
        core.query_rows_for_link(&shape, &binding, DurabilityTier::Global, user(0xa1))
            .unwrap()
            .is_empty()
    );
}

#[test]
fn unbound_team_claim_in_composed_read_policy_denies_without_binding_error() {
    let schema = build_public_test_schema(PublicSchemaBuilder::new().table(
        PublicTableSchemaBuilder::new("todos")
            .column("title", PublicColumnType::Text)
            .column("team", PublicColumnType::Uuid)
            .policies(
                PublicTablePolicies::new().with_select(PublicPolicyExpr::eq_session(
                    "team",
                    vec!["claims".to_owned(), "team".to_owned()],
                )),
            ),
    ));
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    let tx = core
        .commit_mergeable(
            MergeableCommit::new("todos", row(0x87), 10).cells(BTreeMap::from([
                ("title".to_owned(), v("team-owned")),
                ("team".to_owned(), Value::Uuid(user(0xa1).0)),
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
fn registered_team_claim_in_composed_read_policy_allows_matching_rows() {
    let schema = build_public_test_schema(PublicSchemaBuilder::new().table(
        PublicTableSchemaBuilder::new("todos")
            .column("title", PublicColumnType::Text)
            .column("team", PublicColumnType::Uuid)
            .policies(
                PublicTablePolicies::new().with_select(PublicPolicyExpr::eq_session(
                    "team",
                    vec!["claims".to_owned(), "team".to_owned()],
                )),
            ),
    ));
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    let team_a = user(0xa1);
    let team_b = user(0xb2);
    let tx_a = core
        .commit_mergeable(
            MergeableCommit::new("todos", row(0x87), 10).cells(BTreeMap::from([
                ("title".to_owned(), v("team-a")),
                ("team".to_owned(), Value::Uuid(team_a.0)),
            ])),
        )
        .unwrap();
    core.apply_fate_update(
        tx_a,
        Fate::Accepted,
        Some(GlobalTime(1)),
        Some(DurabilityTier::Global),
    )
    .unwrap();
    let tx_b = core
        .commit_mergeable(
            MergeableCommit::new("todos", row(0x88), 11).cells(BTreeMap::from([
                ("title".to_owned(), v("team-b")),
                ("team".to_owned(), Value::Uuid(team_b.0)),
            ])),
        )
        .unwrap();
    core.apply_fate_update(
        tx_b,
        Fate::Accepted,
        Some(GlobalTime(2)),
        Some(DurabilityTier::Global),
    )
    .unwrap();
    core.set_session_claims(
        team_a,
        BTreeMap::from([("team".to_owned(), Value::Uuid(team_a.0))]),
    );
    let mut edge = PeerState::edge_client(team_a);

    assert_view_update_only_references_rows(
        &edge.current_rows_update(&mut core, "todos").unwrap(),
        BTreeSet::from([row(0x87)]),
    );
}

#[test]
fn nullable_claim_equality_policy_branch_allows_matching_row() {
    let reader = user(0xa1);
    let schema = build_public_test_schema(PublicSchemaBuilder::new().table(
        PublicTableSchemaBuilder::new("chats")
            .column("title", PublicColumnType::Text)
            .nullable_column("joinCode", PublicColumnType::Text)
            .policies(public_all_policies().with_select(PublicPolicyExpr::eq_session(
                "joinCode",
                vec!["claims".to_owned(), "join_code".to_owned()],
            ))),
    ));
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    let matching = row(0x91);
    let other = row(0x92);
    let tx_matching = core
        .commit_mergeable(
            MergeableCommit::new("chats", matching, 10).cells(BTreeMap::from([
                ("title".to_owned(), Value::String("matching".to_owned())),
                (
                    "joinCode".to_owned(),
                    Value::Nullable(Some(Box::new(Value::String("secret-123".to_owned())))),
                ),
            ])),
        )
        .unwrap();
    core.apply_fate_update(
        tx_matching,
        Fate::Accepted,
        Some(GlobalTime(1)),
        Some(DurabilityTier::Global),
    )
    .unwrap();
    let tx_other = core
        .commit_mergeable(
            MergeableCommit::new("chats", other, 11).cells(BTreeMap::from([
                ("title".to_owned(), Value::String("other".to_owned())),
                (
                    "joinCode".to_owned(),
                    Value::Nullable(Some(Box::new(Value::String("wrong".to_owned())))),
                ),
            ])),
        )
        .unwrap();
    core.apply_fate_update(
        tx_other,
        Fate::Accepted,
        Some(GlobalTime(2)),
        Some(DurabilityTier::Global),
    )
    .unwrap();
    core.set_session_claims(
        reader,
        BTreeMap::from([(
            "join_code".to_owned(),
            Value::String("secret-123".to_owned()),
        )]),
    );
    let mut edge = PeerState::edge_client(reader);

    assert_view_update_only_references_rows(
        &edge.current_rows_update(&mut core, "chats").unwrap(),
        BTreeSet::from([matching]),
    );
}
