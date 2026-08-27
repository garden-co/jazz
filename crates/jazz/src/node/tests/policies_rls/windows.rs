// Policy-partitioned TopBy windows and existential authorization proofs.

#[test]
fn maintained_subscription_view_top_by_partitions_windows_by_policy_claim_binding() {
    let schema = build_public_test_schema(PublicSchemaBuilder::new().table(
        PublicTableSchemaBuilder::new("documents")
            .column("owner", PublicColumnType::Uuid)
            .column("updated_at", PublicColumnType::Timestamp)
            .policies(
                PublicTablePolicies::new().with_select(PublicPolicyExpr::eq_session(
                    "owner",
                    vec!["claims".to_owned(), "user_id".to_owned()],
                )),
            ),
    ));
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    let owner_a = user(0xa1);
    let owner_b = user(0xb2);
    core.set_test_provider_claims(
        owner_a,
        BTreeMap::from([("user_id".to_owned(), Value::Uuid(owner_a.test_uuid()))]),
    );
    core.set_test_provider_claims(
        owner_b,
        BTreeMap::from([("user_id".to_owned(), Value::Uuid(owner_b.test_uuid()))]),
    );

    for index in 0..100_u64 {
        accept_global(
            &mut core,
            MergeableCommit::new("documents", row(index as u8), index).cells(BTreeMap::from([
                ("owner".to_owned(), Value::Uuid(owner_a.test_uuid())),
                ("updated_at".to_owned(), Value::U64(index)),
            ])),
        );
        accept_global(
            &mut core,
            MergeableCommit::new("documents", row((index + 100) as u8), index + 100)
                .cells(BTreeMap::from([
                    ("owner".to_owned(), Value::Uuid(owner_b.test_uuid())),
                    ("updated_at".to_owned(), Value::U64(index + 100)),
                ])),
        );
    }

    let shape = Query::from("documents")
        .order_by("updated_at", OrderDirection::Desc)
        .limit(100)
        .validate(&core.catalogue.schema)
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();

    let mut peer_b = PeerState::client_link(owner_b);
    let update_b = peer_b
        .rehydrate_query(&mut core, &shape, &binding)
        .unwrap();
    let mut peer_a = PeerState::client_link(owner_a);
    let update_a = peer_a
        .rehydrate_query(&mut core, &shape, &binding)
        .unwrap();

    let (adds_a, removes_a) = canonical_view_update_rows(&update_a);
    let (adds_b, removes_b) = canonical_view_update_rows(&update_b);
    assert!(removes_a.is_empty());
    assert!(removes_b.is_empty());
    assert_eq!(adds_a.len(), 100, "owner A should receive its own full window");
    assert_eq!(adds_b.len(), 100, "owner B should receive its own full window");
}

#[test]
fn authorization_proofs_are_existential_before_top_by_windows() {
    let reader = user(0xa1);
    let documents_policy = PublicPolicyExpr::Or(vec![
        public_outer_exists(
            "documentAccess",
            "document",
            "id",
            [PublicPolicyExpr::eq_session(
                "reader",
                vec!["claims".to_owned(), "user_id".to_owned()],
            )],
        ),
        public_literal_eq("published", PublicValue::Boolean(true)),
    ]);
    let schema = build_public_test_schema(
        PublicSchemaBuilder::new()
            .table(
                PublicTableSchemaBuilder::new("documents")
                    .column("updated_at", PublicColumnType::Timestamp)
                    .column("published", PublicColumnType::Boolean)
                    .policies(PublicTablePolicies::new().with_select(documents_policy)),
            )
            .table(
                PublicTableSchemaBuilder::new("documentAccess")
                    .fk_column("document", "documents")
                    .column("reader", PublicColumnType::Uuid),
            ),
    );
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    core.set_test_provider_claims(
        reader,
        BTreeMap::from([("user_id".to_owned(), Value::Uuid(reader.test_uuid()))]),
    );
    let mut document_txs = Vec::new();
    let mut grant_txs = Vec::new();
    for index in 0..100_u64 {
        let document = row(index as u8);
        document_txs.push(accept_global(
            &mut core,
            MergeableCommit::new("documents", document, index * 3 + 1).cells(BTreeMap::from([
                ("updated_at".to_owned(), Value::U64(index)),
                ("published".to_owned(), Value::Bool(index >= 95)),
            ])),
        ));
        let grant = row((index + 100) as u8);
        grant_txs.push(accept_global(
            &mut core,
            MergeableCommit::new("documentAccess", grant, index * 3 + 2).cells(BTreeMap::from([
                ("document".to_owned(), Value::Uuid(document.0)),
                ("reader".to_owned(), Value::Uuid(reader.test_uuid())),
            ])),
        ));
    }
    let shape = Query::from("documents")
        .order_by("updated_at", OrderDirection::Desc)
        .limit(100)
        .validate(&core.catalogue.schema)
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let query_rows = |core: &mut NodeState<RocksDbStorage>| {
        core.query_rows_for_link(&shape, &binding, DurabilityTier::Global, reader)
            .unwrap()
            .into_iter()
            .map(|row| row.row_uuid())
            .collect::<Vec<_>>()
    };
    let expected_initial = (0..100_u8).rev().map(row).collect::<Vec<_>>();
    let initial_one_shot = query_rows(&mut core);
    assert_eq!(initial_one_shot, expected_initial);
    assert_eq!(initial_one_shot.iter().copied().collect::<BTreeSet<_>>().len(), 100);

    let mut peer = PeerState::client_link(reader);
    let initial = peer.rehydrate_query(&mut core, &shape, &binding).unwrap();
    let (initial_adds, initial_removes) =
        canonical_view_update_rows_for_table(&initial, "documents");
    assert!(initial_removes.is_empty());
    assert_eq!(
        initial_adds
            .iter()
            .map(|(_, row, _)| *row)
            .collect::<BTreeSet<_>>(),
        expected_initial.iter().copied().collect()
    );

    let duplicate_grant = row(0xfe);
    let duplicate_grant_tx = accept_global(
        &mut core,
        MergeableCommit::new("documentAccess", duplicate_grant, 302).cells(BTreeMap::from([
            ("document".to_owned(), Value::Uuid(row(99).0)),
            ("reader".to_owned(), Value::Uuid(reader.test_uuid())),
        ])),
    );
    let duplicate = peer.query_update(&mut core, &shape, &binding).unwrap();
    assert_eq!(
        canonical_view_update_rows_for_table(&duplicate, "documents"),
        (Vec::new(), Vec::new())
    );
    assert_eq!(query_rows(&mut core).len(), 100);

    accept_global(
        &mut core,
        MergeableCommit::new("documentAccess", row(199), 303)
            .parents(vec![grant_txs[99]])
            .deletion(DeletionEvent::Deleted),
    );
    let partial_revoke = peer.query_update(&mut core, &shape, &binding).unwrap();
    assert_eq!(
        canonical_view_update_rows_for_table(&partial_revoke, "documents"),
        (Vec::new(), Vec::new())
    );
    assert_eq!(query_rows(&mut core).len(), 100);

    accept_global(
        &mut core,
        MergeableCommit::new("documentAccess", duplicate_grant, 304)
            .parents(vec![duplicate_grant_tx])
            .deletion(DeletionEvent::Deleted),
    );
    let overlapping_branch = peer.query_update(&mut core, &shape, &binding).unwrap();
    assert_eq!(
        canonical_view_update_rows_for_table(&overlapping_branch, "documents"),
        (Vec::new(), Vec::new())
    );
    assert_eq!(query_rows(&mut core).len(), 100);

    accept_global(
        &mut core,
        MergeableCommit::new("documents", row(99), 305)
            .parents(vec![document_txs[99]])
            .cells(BTreeMap::from([
                ("updated_at".to_owned(), Value::U64(99)),
                ("published".to_owned(), Value::Bool(false)),
            ])),
    );
    let final_revoke = peer.query_update(&mut core, &shape, &binding).unwrap();
    assert_eq!(
        canonical_view_update_rows_for_table(&final_revoke, "documents"),
        (
            Vec::new(),
            vec![("documents".to_owned().into(), row(99), document_txs[99])]
        )
    );
    assert_eq!(
        query_rows(&mut core),
        (0..99_u8).rev().map(row).collect::<Vec<_>>()
    );
}
