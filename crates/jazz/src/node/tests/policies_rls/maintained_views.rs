// Maintained-view seeding, claims, joins, deltas, windows, and retained parameters.

#[test]
fn maintained_view_seeded_query_engine_snapshot_matches_rows_and_witnesses() {
    let schema = owner_read_schema("todos");
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    let author_a = user(0xa1);
    let author_b = user(0xb2);
    install_test_uuid_sub_claim(&mut core, author_a);
    install_test_uuid_sub_claim(&mut core, author_b);

    let sibling_tx = core
        .commit_mergeable_many_settled(vec![
            MergeableCommit::new("todos", row(0x90), 10).cells(owner_cells(author_a, "include")),
            MergeableCommit::new("todos", row(0x91), 10).cells(owner_cells(author_b, "include")),
            MergeableCommit::new("todos", row(0x92), 10).cells(owner_cells(author_a, "skip")),
        ])
        .unwrap();
    core.accept_global_for_test(sibling_tx).unwrap();

    let _deleted_readable_content = accept_global(
        &mut core,
        MergeableCommit::new("todos", row(0x93), 20).cells(owner_cells(author_a, "delete me")),
    );
    let deleted_readable_delete = accept_global(
        &mut core,
        MergeableCommit::new("todos", row(0x93), 21)
            .deletion(DeletionEvent::Deleted),
    );
    let _deleted_unreadable_content = accept_global(
        &mut core,
        MergeableCommit::new("todos", row(0x94), 22).cells(owner_cells(author_b, "hidden delete")),
    );
    let deleted_unreadable_delete = accept_global(
        &mut core,
        MergeableCommit::new("todos", row(0x94), 23)
            .deletion(DeletionEvent::Deleted),
    );

    let shape = Query::from("todos")
        .filter(eq(col("title"), lit("include")))
        .validate(&core.catalogue.schema)
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();

    assert_query_engine_maintained_seed_matches_public_rows_and_witnesses(
        &mut core,
        &shape,
        &binding,
        AuthorSubject::SYSTEM,
        [
            (sibling_tx, row(0x90), VersionLayer::Content),
            (sibling_tx, row(0x91), VersionLayer::Content),
            (deleted_readable_delete, row(0x93), VersionLayer::Deletion),
            (deleted_unreadable_delete, row(0x94), VersionLayer::Deletion),
        ],
        [
            (row(0x93), VersionLayer::Content, false),
            (row(0x93), VersionLayer::Deletion, true),
            (row(0x94), VersionLayer::Content, false),
            (row(0x94), VersionLayer::Deletion, true),
        ],
    );
    assert_query_engine_maintained_seed_matches_public_rows_and_witnesses(
        &mut core,
        &shape,
        &binding,
        author_a,
        [
            (sibling_tx, row(0x90), VersionLayer::Content),
            (deleted_readable_delete, row(0x93), VersionLayer::Deletion),
        ],
        [
            (row(0x93), VersionLayer::Content, false),
            (row(0x93), VersionLayer::Deletion, true),
            (row(0x94), VersionLayer::Content, false),
            // Alice cannot read Bob's retained preimage, so even the deletion
            // replacement witness must stay outside her admitted closure.
            (row(0x94), VersionLayer::Deletion, false),
        ],
    );
}

#[test]
fn maintained_view_query_engine_seed_clean_owner_policy_claim_params_match_one_shot() {
    let schema = owner_read_schema("todos");
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    let author = user(0xa1);
    let other = user(0xb2);
    install_test_uuid_sub_claim(&mut core, author);
    install_test_uuid_sub_claim(&mut core, other);

    accept_global(
        &mut core,
        MergeableCommit::new("todos", row(0xa0), 10).cells(owner_cells(author, "owned")),
    );
    accept_global(
        &mut core,
        MergeableCommit::new("todos", row(0xb0), 11).cells(owner_cells(other, "hidden")),
    );

    let shape = Query::from("todos")
        .filter(eq(col("title"), param("title")))
        .validate(&core.catalogue.schema)
        .unwrap();
    let binding = shape
        .bind(BTreeMap::from([(
            "title".to_owned(),
            Value::String("owned".to_owned()),
        )]))
        .unwrap();
    let mut peer = PeerState::client_link(author);
    let update = peer.rehydrate_query(&mut core, &shape, &binding).unwrap();
    let (adds, removes) = canonical_view_update_rows(&update);
    assert_eq!(
        adds.into_iter()
            .map(|(_table, row_uuid, _tx_id)| row_uuid)
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([row(0xa0)]),
        "query-engine maintained rows should route by retained query and policy claim params"
    );
    assert!(removes.is_empty());
}

#[test]
fn maintained_view_cold_snapshot_seeds_maintained_indexes_equal_one_shot() {
    let schema = owner_read_schema("todos");
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    let author_a = user(0xa1);
    let author_b = user(0xb2);
    install_test_uuid_sub_claim(&mut core, author_a);
    install_test_uuid_sub_claim(&mut core, author_b);

    let sibling_tx = core
        .commit_mergeable_many_settled(vec![
            MergeableCommit::new("todos", row(0x90), 10).cells(owner_cells(author_a, "include")),
            MergeableCommit::new("todos", row(0x91), 10).cells(owner_cells(author_b, "include")),
            MergeableCommit::new("todos", row(0x92), 10).cells(owner_cells(author_a, "skip")),
        ])
        .unwrap();
    core.accept_global_for_test(sibling_tx).unwrap();

    let _deleted_readable_content = accept_global(
        &mut core,
        MergeableCommit::new("todos", row(0x93), 20).cells(owner_cells(author_a, "delete me")),
    );
    accept_global(
        &mut core,
        MergeableCommit::new("todos", row(0x93), 21)
            .deletion(DeletionEvent::Deleted),
    );
    let _deleted_unreadable_content = accept_global(
        &mut core,
        MergeableCommit::new("todos", row(0x94), 22).cells(owner_cells(author_b, "hidden delete")),
    );
    accept_global(
        &mut core,
        MergeableCommit::new("todos", row(0x94), 23)
            .deletion(DeletionEvent::Deleted),
    );

    let shape = Query::from("todos")
        .filter(eq(col("title"), lit("include")))
        .validate(&core.catalogue.schema)
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();

    assert_maintained_view_cold_snapshot_seed_matches_one_shot(
        &mut core,
        &shape,
        &binding,
        AuthorSubject::SYSTEM,
    );
    assert_maintained_view_cold_snapshot_seed_matches_one_shot(
        &mut core, &shape, &binding, author_a,
    );
}

#[test]
fn maintained_view_system_identity_bypasses_root_read_policy() {
    let schema = owner_read_schema("todos");
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    let author_a = user(0xa1);
    let author_b = user(0xb2);
    let tx_a = accept_global(
        &mut core,
        MergeableCommit::new("todos", row(0xa0), 10).cells(owner_cells(author_a, "a")),
    );
    let tx_b = accept_global(
        &mut core,
        MergeableCommit::new("todos", row(0xa1), 11).cells(owner_cells(author_b, "b")),
    );
    let _deleted_content = accept_global(
        &mut core,
        MergeableCommit::new("todos", row(0xa2), 12).cells(owner_cells(author_b, "deleted")),
    );
    accept_global(
        &mut core,
        MergeableCommit::new("todos", row(0xa2), 13)
            .deletion(DeletionEvent::Deleted),
    );

    let shape = Query::from("todos")
        .validate(&core.catalogue.schema)
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let mut peer = PeerState::new();
    let update = peer.rehydrate_query(&mut core, &shape, &binding).unwrap();
    let (adds, removes) = canonical_view_update_rows(&update);
    assert_eq!(
        adds.into_iter()
            .map(|(_table, row_uuid, tx_id)| (row_uuid, tx_id))
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([(row(0xa0), tx_a), (row(0xa1), tx_b)])
    );
    assert!(removes.is_empty());
}

#[test]
fn maintained_view_allows_join_policy_slice() {
    let schema = todos_member_read_schema();
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    let shape = Query::from("todos")
        .validate(&core.catalogue.schema)
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let mut peer = PeerState::client_link(user(0xa1));
    peer.rehydrate_query(&mut core, &shape, &binding).unwrap();
}

/// Contract: a prepared maintained subscription must preserve both semantic
/// authorization occurrences for a root row that is admitted through either
/// of two independently matching relationship branches.
///
/// Actors: the serving core owns the data; `reader` opens a title-bound
/// subscription; the same todo matches both owner and editor policy branches.
/// The public effect is one readable todo, while the maintained result carrier
/// must retain the UNION arm that distinguishes the two branch occurrences.
#[test]
fn prepared_maintained_owner_or_editor_policy_keeps_union_arm_occurrences() {
    let reader = user(0xa1);
    let todo = row(0xa0);
    let schema = build_public_test_schema(
        PublicSchemaBuilder::new()
            .table(
                PublicTableSchemaBuilder::new("todos")
                    .column("title", PublicColumnType::Text)
                    .column("owner_match", PublicColumnType::Boolean)
                    .column("editor_match", PublicColumnType::Boolean),
            ),
    );
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);

    accept_global(
        &mut core,
        MergeableCommit::new("todos", todo, 10).cells(BTreeMap::from([
            ("title".to_owned(), Value::String("shared".to_owned())),
            ("owner_match".to_owned(), Value::Bool(true)),
            ("editor_match".to_owned(), Value::Bool(true)),
        ])),
    );

    let mut query = Query::from("todos");
    query.filters = vec![crate::query::Predicate::Any(Vec::new())];
    query.policy_branches = vec![
        crate::query::PolicyBranch {
            filters: vec![eq(col("owner_match"), lit(true)), eq(col("title"), param("title"))],
            joins: Vec::new(),
            reachable: Vec::new(),
            inherits: Vec::new(),
        },
        crate::query::PolicyBranch {
            filters: vec![eq(col("editor_match"), lit(true)), eq(col("title"), param("title"))],
            joins: Vec::new(),
            reachable: Vec::new(),
            inherits: Vec::new(),
        },
    ];
    let shape = query
        .validate_runtime(&core.catalogue.schema)
        .unwrap();
    let binding = shape
        .bind(BTreeMap::from([("title".to_owned(), Value::String("shared".to_owned()))]))
        .unwrap();
    let mut peer = PeerState::client_link(reader);
    let update = peer.rehydrate_query(&mut core, &shape, &binding).unwrap();
    let (adds, removes) = canonical_view_update_rows(&update);
    assert_eq!(
        adds.into_iter()
            .map(|(_table, row_uuid, _tx_id)| row_uuid)
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([todo]),
        "the two authorization occurrences materialize as one public todo",
    );
    assert!(removes.is_empty());
}

#[test]
fn maintained_view_retained_claim_param_equality_matches_literal_recompute() {
    let schema = owner_read_schema("todos");
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    let author = user(0xa1);
    let other = user(0xb2);
    install_test_uuid_sub_claim(&mut core, author);
    install_test_uuid_sub_claim(&mut core, other);

    accept_global(
        &mut core,
        MergeableCommit::new("todos", row(0xa0), 10).cells(owner_cells(author, "owned")),
    );
    accept_global(
        &mut core,
        MergeableCommit::new("todos", row(0xb0), 11).cells(owner_cells(other, "other")),
    );

    let retained_shape = Query::from("todos")
        .validate(&core.catalogue.schema)
        .unwrap();
    let retained_binding = retained_shape.bind(BTreeMap::new()).unwrap();
    let expected_rows = BTreeSet::from([row(0xa0)]);

    let (prepared_shape, prepared_binding, prepared_plan) = core
        .prepare_query_binding_for_link(
            &retained_shape,
            &retained_binding,
            DurabilityTier::Global,
            author,
        )
        .unwrap();
    let prepared_rows = core
        .query_rows_with_prepared_plan_for_identity(
            &prepared_shape,
            &prepared_binding,
            DurabilityTier::Global,
            Some(&prepared_plan),
            author,
        )
        .unwrap()
        .into_iter()
        .map(|row| row.row_uuid())
        .collect::<BTreeSet<_>>();
    assert_eq!(prepared_rows, expected_rows);

    let mut peer = PeerState::client_link(author);
    let update = peer
        .rehydrate_query(&mut core, &retained_shape, &retained_binding)
        .unwrap();
    let (adds, removes) = canonical_view_update_rows(&update);
    assert_eq!(
        adds.into_iter()
            .map(|(_table, row_uuid, _tx_id)| row_uuid)
            .collect::<BTreeSet<_>>(),
        expected_rows
    );
    assert!(removes.is_empty());
}

#[test]
fn maintained_view_join_policy_retained_claim_param_matches_query_engine_result() {
    let schema = todos_member_read_schema();
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    let author = user(0xa1);
    let other = user(0xb2);
    install_test_uuid_sub_claim(&mut core, author);
    install_test_uuid_sub_claim(&mut core, other);

    accept_global(
        &mut core,
        MergeableCommit::new("todos", row(0xa0), 10).cells(owner_cells(author, "owned")),
    );
    accept_global(
        &mut core,
        MergeableCommit::new("todos", row(0xb0), 11).cells(owner_cells(other, "other")),
    );
    accept_global(
        &mut core,
        MergeableCommit::new("members", row(0xa1), 12).cells(BTreeMap::from([
            ("owner".to_owned(), Value::Uuid(row(0xa0).0)),
            ("user".to_owned(), Value::Uuid(author.test_uuid())),
        ])),
    );
    accept_global(
        &mut core,
        MergeableCommit::new("members", row(0xb1), 13).cells(BTreeMap::from([
            ("owner".to_owned(), Value::Uuid(row(0xb0).0)),
            ("user".to_owned(), Value::Uuid(other.test_uuid())),
        ])),
    );

    let shape = Query::from("todos")
        .validate(&core.catalogue.schema)
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    core.reset_query_engine_read_metrics();
    let full_recompute_rows = core
        .query_rows_for_link(&shape, &binding, DurabilityTier::Global, author)
        .unwrap()
        .into_iter()
        .map(|row| row.row_uuid())
        .collect::<BTreeSet<_>>();
    assert_eq!(full_recompute_rows, BTreeSet::from([row(0xa0)]));
    let one_shot_metrics = core.query_engine_read_metrics();
    assert!(one_shot_metrics.policy_authorization_graphs > 0);
    assert!(one_shot_metrics.policy_authorized_source_joins > 0);

    let mut peer = PeerState::client_link(author);
    core.reset_query_engine_read_metrics();
    let update = peer.rehydrate_query(&mut core, &shape, &binding).unwrap();
    let (adds, removes) = canonical_view_update_rows(&update);
    assert_eq!(
        adds.into_iter()
            .map(|(_table, row_uuid, _tx_id)| row_uuid)
            .collect::<BTreeSet<_>>(),
        full_recompute_rows
    );
    assert!(removes.is_empty());
    let maintained_metrics = core.query_engine_read_metrics();
    assert!(maintained_metrics.policy_authorization_graphs > 0);
    assert!(maintained_metrics.policy_authorized_source_joins > 0);
}

#[test]
fn maintained_subscription_view_shared_todo_member_include_emits_relation_deltas_without_full_recompute()
{
    let schema = build_public_test_schema(
        PublicSchemaBuilder::new()
            .table(
                PublicTableSchemaBuilder::new("sharedTodos")
                    .column("title", PublicColumnType::Text)
                    .fk_column("owner", "members"),
            )
            .table(
                PublicTableSchemaBuilder::new("members")
                    .column("name", PublicColumnType::Text)
                    .column("userID", PublicColumnType::Uuid)
                    .policies(
                        PublicTablePolicies::new()
                            .with_select(PublicPolicyExpr::eq_session(
                                "userID",
                                vec!["claims".to_owned(), "user_id".to_owned()],
                            )),
                    ),
            ),
    );
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    let reader = user(0xa1);
    let other = user(0xb2);
    core.set_test_provider_claims(
        reader,
        BTreeMap::from([("user_id".to_owned(), Value::Uuid(reader.test_uuid()))]),
    );
    let member_row = row(0x71);
    let todo_row = row(0x72);

    let hidden_member_tx = accept_global(
        &mut core,
        MergeableCommit::new("members", member_row, 10).cells(BTreeMap::from([
            ("name".to_owned(), Value::String("hidden owner".to_owned())),
            ("userID".to_owned(), Value::Uuid(other.test_uuid())),
        ])),
    );
    let todo_tx = accept_global(
        &mut core,
        MergeableCommit::new("sharedTodos", todo_row, 11).cells(BTreeMap::from([
            ("title".to_owned(), Value::String("shared slice".to_owned())),
            ("owner".to_owned(), Value::Uuid(member_row.0)),
        ])),
    );

    let shape = Query::from("sharedTodos")
        .include_with(Include::new("owner").require_includes())
        .validate(&core.catalogue.schema)
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();

    let mut peer = PeerState::client_link(reader);
    let initial = peer.rehydrate_query(&mut core, &shape, &binding).unwrap();
    assert_eq!(
        canonical_view_update_rows(&initial),
        (Vec::new(), Vec::new())
    );
    assert_eq!(peer.maintained_subscription_view_metrics().hits_out, 1);

    let visible_member_tx = accept_global(
        &mut core,
        MergeableCommit::new("members", member_row, 12)
            .parents(vec![hidden_member_tx])
            .cells(BTreeMap::from([
                ("name".to_owned(), Value::String("visible owner".to_owned())),
                ("userID".to_owned(), Value::Uuid(reader.test_uuid())),
            ])),
    );
    let grant = peer.query_update(&mut core, &shape, &binding).unwrap();
    assert_eq!(
        canonical_view_update_rows(&grant),
        (
            vec![
                ("members".to_owned().into(), member_row, visible_member_tx),
                ("sharedTodos".to_owned().into(), todo_row, todo_tx),
            ],
            Vec::new(),
        )
    );
    assert_view_update_only_references_rows(&grant, BTreeSet::from([member_row, todo_row]));
    assert_eq!(peer.maintained_subscription_view_metrics().hits_out, 2);

    let hidden_again_tx = accept_global(
        &mut core,
        MergeableCommit::new("members", member_row, 13)
            .parents(vec![visible_member_tx])
            .cells(BTreeMap::from([
                ("name".to_owned(), Value::String("hidden again".to_owned())),
                ("userID".to_owned(), Value::Uuid(other.test_uuid())),
            ])),
    );
    let revoke = peer.query_update(&mut core, &shape, &binding).unwrap();
    assert_eq!(
        canonical_view_update_rows(&revoke),
        (
            Vec::new(),
            vec![
                ("members".to_owned().into(), member_row, visible_member_tx),
                ("sharedTodos".to_owned().into(), todo_row, todo_tx),
            ],
        )
    );
    assert_retraction_without_replacement_leak(
        &revoke,
        member_row,
        visible_member_tx,
        hidden_again_tx,
    );
    assert_eq!(peer.maintained_subscription_view_metrics().hits_out, 3);
}

#[test]
fn inherited_parent_policy_semijoin_preserves_visibility_across_duplicate_derivations() {
    let reader = user(0xa1);
    let other = user(0xb2);
    let container = row(0xc1);
    let entry = row(0xe1);
    let first_edge = row(0xa1);
    let second_edge = row(0xa2);
    let third_edge = row(0xa3);
    let schema = build_public_test_schema(
        PublicSchemaBuilder::new()
            .table(
                PublicTableSchemaBuilder::new("containers")
                    .column("name", PublicColumnType::Text)
                    .policies(PublicTablePolicies::new().with_select(public_outer_exists(
                        "containerAccess",
                        "container",
                        "id",
                        [public_claim_eq("reader", "sub")],
                    ))),
            )
            .table(
                PublicTableSchemaBuilder::new("entries")
                    .fk_column("container", "containers")
                    .column("title", PublicColumnType::Text)
                    .policies(PublicTablePolicies::new().with_select(
                        PublicPolicyExpr::Inherits {
                            operation: PublicOperation::Select,
                            via_column: "container".to_owned(),
                            max_depth: None,
                        },
                    )),
            )
            .table(
                PublicTableSchemaBuilder::new("containerAccess")
                    .fk_column("container", "containers")
                    .column("reader", PublicColumnType::Uuid),
            ),
    );
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    install_test_uuid_sub_claim(&mut core, reader);
    install_test_uuid_sub_claim(&mut core, other);

    let _container_tx = accept_global(
        &mut core,
        MergeableCommit::new("containers", container, 10)
            .cells(BTreeMap::from([("name".to_owned(), v("container"))])),
    );
    let entry_tx = accept_global(
        &mut core,
        MergeableCommit::new("entries", entry, 11).cells(BTreeMap::from([
            ("container".to_owned(), Value::Uuid(container.0)),
            ("title".to_owned(), v("entry")),
        ])),
    );
    let _first_edge_tx = accept_global(
        &mut core,
        MergeableCommit::new("containerAccess", first_edge, 12).cells(BTreeMap::from([
            ("container".to_owned(), Value::Uuid(container.0)),
            ("reader".to_owned(), Value::Uuid(reader.test_uuid())),
        ])),
    );
    let _second_edge_tx = accept_global(
        &mut core,
        MergeableCommit::new("containerAccess", second_edge, 13).cells(BTreeMap::from([
            ("container".to_owned(), Value::Uuid(container.0)),
            ("reader".to_owned(), Value::Uuid(reader.test_uuid())),
        ])),
    );
    accept_global(
        &mut core,
        MergeableCommit::new("containerAccess", row(0xaf), 14).cells(BTreeMap::from([
            ("container".to_owned(), Value::Uuid(container.0)),
            ("reader".to_owned(), Value::Uuid(other.test_uuid())),
        ])),
    );

    let shape = Query::from("entries")
        .validate(&core.catalogue.schema)
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let one_shot_rows = core
        .query_rows_for_link(&shape, &binding, DurabilityTier::Global, reader)
        .unwrap();
    assert_eq!(
        one_shot_rows
            .iter()
            .map(|row| row.row_uuid())
            .collect::<Vec<_>>(),
        vec![entry]
    );

    let mut peer = PeerState::client_link(reader);
    let initial = peer.rehydrate_query(&mut core, &shape, &binding).unwrap();
    assert_eq!(
        canonical_view_update_rows_for_table(&initial, "entries"),
        (
            vec![("entries".to_owned().into(), entry, entry_tx)],
            Vec::new()
        )
    );
    let _stable = peer.query_update(&mut core, &shape, &binding).unwrap();

    accept_global(
        &mut core,
        MergeableCommit::new("containerAccess", first_edge, 15)
            .deletion(DeletionEvent::Deleted),
    );
    let first_revoke = peer.query_update(&mut core, &shape, &binding).unwrap();
    let (_, first_revoke_entry_removes) =
        canonical_view_update_rows_for_table(&first_revoke, "entries");
    assert!(first_revoke_entry_removes.is_empty());
    assert_eq!(
        core.query_rows_for_link(&shape, &binding, DurabilityTier::Global, reader)
            .unwrap()
            .into_iter()
            .map(|row| row.row_uuid())
            .collect::<Vec<_>>(),
        vec![entry]
    );

    accept_global(
        &mut core,
        MergeableCommit::new("containerAccess", second_edge, 16)
            .deletion(DeletionEvent::Deleted),
    );
    let last_revoke = peer.query_update(&mut core, &shape, &binding).unwrap();
    let (_, last_revoke_entry_removes) =
        canonical_view_update_rows_for_table(&last_revoke, "entries");
    assert_eq!(
        last_revoke_entry_removes
            .iter()
            .map(|(_, row, _)| *row)
            .collect::<Vec<_>>(),
        vec![entry]
    );
    assert!(
        core.query_rows_for_link(&shape, &binding, DurabilityTier::Global, reader)
            .unwrap()
            .is_empty()
    );

    accept_global(
        &mut core,
        MergeableCommit::new("containerAccess", third_edge, 17).cells(BTreeMap::from([
            ("container".to_owned(), Value::Uuid(container.0)),
            ("reader".to_owned(), Value::Uuid(reader.test_uuid())),
        ])),
    );
    let regrant = peer.query_update(&mut core, &shape, &binding).unwrap();
    let (regrant_entry_adds, _) = canonical_view_update_rows_for_table(&regrant, "entries");
    assert_eq!(
        regrant_entry_adds
            .iter()
            .map(|(_, row, _)| *row)
            .collect::<Vec<_>>(),
        vec![entry]
    );
    assert_eq!(
        core.query_rows_for_link(&shape, &binding, DurabilityTier::Global, reader)
            .unwrap()
            .into_iter()
            .map(|row| row.row_uuid())
            .collect::<Vec<_>>(),
        vec![entry]
    );
}

#[test]
fn maintained_subscription_view_ordered_offset_limit_boundary_churn_stays_incremental() {
    let (_core_dir, mut core) = open_node_with_schema(node(9), priority_schema());
    let first = row(0x11);
    let second = row(0x22);
    let third = row(0x33);
    let fourth = row(0x44);
    let first_tx = accept_global(
        &mut core,
        MergeableCommit::new("todos", first, 10).cells(priority_cells("first", 10)),
    );
    let second_tx = accept_global(
        &mut core,
        MergeableCommit::new("todos", second, 11).cells(priority_cells("second", 20)),
    );
    let third_tx = accept_global(
        &mut core,
        MergeableCommit::new("todos", third, 12).cells(priority_cells("third", 30)),
    );
    let fourth_tx = accept_global(
        &mut core,
        MergeableCommit::new("todos", fourth, 13).cells(priority_cells("fourth", 40)),
    );
    let shape = Query::from("todos")
        .order_by("priority", OrderDirection::Asc)
        .offset(1)
        .limit(2)
        .validate(&core.catalogue.schema)
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();

    let mut peer = PeerState::new();
    let initial = peer.rehydrate_query(&mut core, &shape, &binding).unwrap();
    assert_view_update_rows(
        initial,
        [("todos", second, second_tx), ("todos", third, third_tx)],
        [],
    );

    let zeroth = row(0x05);
    let _zeroth_tx = accept_global(
        &mut core,
        MergeableCommit::new("todos", zeroth, 14).cells(priority_cells("zeroth", 5)),
    );
    let shifted_down = peer.query_update(&mut core, &shape, &binding).unwrap();
    assert_view_update_rows(
        shifted_down,
        [("todos", first, first_tx)],
        [("todos", third, third_tx)],
    );

    accept_global(
        &mut core,
        MergeableCommit::new("todos", zeroth, 15)
            .deletion(DeletionEvent::Deleted),
    );
    let shifted_back = peer.query_update(&mut core, &shape, &binding).unwrap();
    assert_view_update_rows(
        shifted_back,
        [("todos", third, third_tx)],
        [("todos", first, first_tx)],
    );

    accept_global(
        &mut core,
        MergeableCommit::new("todos", second, 16)
            .deletion(DeletionEvent::Deleted),
    );
    let fill_from_tail = peer.query_update(&mut core, &shape, &binding).unwrap();
    assert_view_update_rows(
        fill_from_tail,
        [("todos", fourth, fourth_tx)],
        [("todos", second, second_tx)],
    );

    let metrics = peer.maintained_subscription_view_metrics();
    assert_eq!(metrics.unsupported_skips_out, 0);
    assert_eq!(metrics.hits_out, 4);
}

#[test]
fn maintained_subscription_view_rehydrates_reference_bearing_root_table() {
    // The maintained subscription view footprint is table-aware and now ships
    // reference-closure rows from the fast path.
    let ref_schema = build_public_test_schema(
        PublicSchemaBuilder::new()
            .table(
                PublicTableSchemaBuilder::new("todos")
                    .column("title", PublicColumnType::Text)
                    .fk_column("author", "authors"),
            )
            .table(
                PublicTableSchemaBuilder::new("authors")
                    .column("name", PublicColumnType::Text),
            ),
    );
    let (_ref_dir, mut ref_core) = open_node_with_schema(node(9), ref_schema);
    let shape = Query::from("todos")
        .validate(&ref_core.catalogue.schema)
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let mut ref_peer = PeerState::client_link(user(0xa1));
    ref_peer
        .rehydrate_query(&mut ref_core, &shape, &binding)
        .unwrap();
    let ref_metrics = ref_peer.maintained_subscription_view_metrics();
    assert_eq!(ref_metrics.unsupported_skips_out, 0);
    assert_eq!(ref_metrics.hits_out, 1);

    // Control: the same query on a table with no references is supported.
    let plain_schema = build_public_test_schema(
        PublicSchemaBuilder::new().table(
            PublicTableSchemaBuilder::new("todos").column("title", PublicColumnType::Text),
        ),
    );
    let (_plain_dir, mut plain_core) = open_node_with_schema(node(9), plain_schema);
    let plain_shape = Query::from("todos")
        .validate(&plain_core.catalogue.schema)
        .unwrap();
    let plain_binding = plain_shape.bind(BTreeMap::new()).unwrap();
    let mut plain_peer = PeerState::client_link(user(0xa1));
    plain_peer
        .rehydrate_query(&mut plain_core, &plain_shape, &plain_binding)
        .unwrap();
    let plain_metrics = plain_peer.maintained_subscription_view_metrics();
    assert_eq!(plain_metrics.unsupported_skips_out, 0);
    assert_eq!(plain_metrics.hits_out, 1);
}

#[test]
fn maintained_subscription_view_explicit_include_keeps_other_implicit_references() {
    let schema = build_public_test_schema(
        PublicSchemaBuilder::new()
            .table(
                PublicTableSchemaBuilder::new("roots")
                    .column("title", PublicColumnType::Text)
                    .fk_column("primary", "targets")
                    .fk_column("secondary", "targets"),
            )
            .table(
                PublicTableSchemaBuilder::new("targets")
                    .column("name", PublicColumnType::Text),
            ),
    );
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    let included = row(0x11);
    let excluded = row(0x22);
    let root = row(0x33);
    accept_global(
        &mut core,
        MergeableCommit::new("targets", included, 10)
            .cells(BTreeMap::from([("name".to_owned(), v("included"))])),
    );
    accept_global(
        &mut core,
        MergeableCommit::new("targets", excluded, 11)
            .cells(BTreeMap::from([("name".to_owned(), v("excluded"))])),
    );
    accept_global(
        &mut core,
        MergeableCommit::new("roots", root, 12).cells(BTreeMap::from([
            ("title".to_owned(), v("root")),
            ("primary".to_owned(), Value::Uuid(included.0)),
            ("secondary".to_owned(), Value::Uuid(excluded.0)),
        ])),
    );

    let shape = Query::from("roots")
        .include("primary")
        .validate(&core.catalogue.schema)
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let mut peer = PeerState::client_link(user(0xa1));
    let update = peer.rehydrate_query(&mut core, &shape, &binding).unwrap();

    assert_view_update_only_ships_rows(&update, BTreeSet::from([root, included, excluded]));
    let metrics = peer.maintained_subscription_view_metrics();
    assert_eq!(metrics.unsupported_skips_out, 0);
}

#[test]
fn retained_user_param_filter_graph_matches_literal_filter() {
    let schema = build_public_test_schema(PublicSchemaBuilder::new().table(
        PublicTableSchemaBuilder::new("docs")
            .column("title", PublicColumnType::Text)
            .column("owner", PublicColumnType::Uuid)
            .policies(public_all_policies().with_select(public_claim_eq("owner", "sub"))),
    ));
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    let owner = user(0xa1);
    core.set_test_provider_claims(owner, BTreeMap::from([("sub".to_owned(), Value::Uuid(owner.test_uuid()))]));
    accept_global(
        &mut core,
        MergeableCommit::new("docs", row(0xd1), 10).cells(BTreeMap::from([
            ("title".to_owned(), v("owned")),
            ("owner".to_owned(), Value::Uuid(owner.test_uuid())),
        ])),
    );
    accept_global(
        &mut core,
        MergeableCommit::new("docs", row(0xd2), 11).cells(BTreeMap::from([
            ("title".to_owned(), v("other")),
            ("owner".to_owned(), Value::Uuid(user(0xb2).test_uuid())),
        ])),
    );

    let shape = Query::from("docs")
        .filter(eq(col("owner"), param("owner")))
        .validate(&core.catalogue.schema)
        .unwrap();
    let binding = shape
        .bind(BTreeMap::from([("owner".to_owned(), Value::Uuid(owner.test_uuid()))]))
        .unwrap();
    let (shape, binding, plan) = core
        .prepare_query_binding_for_link(&shape, &binding, DurabilityTier::Global, owner)
        .unwrap();
    let rows = core
        .query_rows_with_prepared_plan_for_identity(
            &shape,
            &binding,
            DurabilityTier::Global,
            Some(&plan),
            owner,
        )
        .unwrap();

    assert_eq!(
        rows.into_iter()
            .map(|row| row.row_uuid())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([row(0xd1)])
    );
    assert!(shape.params().contains_key("owner"));
    assert!(binding.values().contains_key("owner"));
}

#[test]
fn session_sub_claim_remains_an_application_owned_value() {
    let schema = build_public_test_schema(PublicSchemaBuilder::new().table(
        PublicTableSchemaBuilder::new("docs")
            .column("title", PublicColumnType::Text)
            .column("owner", PublicColumnType::Uuid)
            .policies(public_all_policies().with_select(public_claim_eq("owner", "sub"))),
    ));
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    let owner = user(0xa1);
    let other = user(0xb2);
    let owned_doc = row(0xd1);
    let other_doc = row(0xd2);

    accept_global(
        &mut core,
        MergeableCommit::new("docs", owned_doc, 10).cells(BTreeMap::from([
            ("title".to_owned(), v("owned")),
            ("owner".to_owned(), Value::Uuid(owner.test_uuid())),
        ])),
    );
    accept_global(
        &mut core,
        MergeableCommit::new("docs", other_doc, 11).cells(BTreeMap::from([
            ("title".to_owned(), v("other")),
            ("owner".to_owned(), Value::Uuid(other.test_uuid())),
        ])),
    );
    core.set_test_provider_claims(owner, BTreeMap::from([("sub".to_owned(), Value::Uuid(other.test_uuid()))]));

    let shape = Query::from("docs")
        .validate(&core.catalogue.schema)
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();

    assert_eq!(
        core.query_rows_for_link(&shape, &binding, DurabilityTier::Global, owner)
            .unwrap()
            .into_iter()
            .map(|row| row.row_uuid())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([other_doc])
    );
}

#[test]
fn retained_param_used_as_filter_and_reachable_seed_matches_literal_query() {
    let (_core_dir, mut core) = open_node_with_schema(node(9), recursive_reachable_schema());
    seed_recursive_reachable_fixture(&mut core);
    let shape = Query::from("docs")
        .reachable_via_with_access_filters(
            "teamAccess",
            "doc",
            "team",
            param("team"),
            [eq(col("team"), param("team"))],
            "teamEdges",
            "member",
            "parent",
            [],
        )
        .validate(&core.catalogue.schema)
        .unwrap();
    let binding = shape
        .bind(BTreeMap::from([("team".to_owned(), Value::Uuid(team(1)))]))
        .unwrap();

    let (prepared_shape, prepared_binding, prepared_plan) = core
        .prepare_query_binding_for_link(&shape, &binding, DurabilityTier::Global, user(0xa1))
        .unwrap();
    let prepared_rows = core
        .query_rows_with_prepared_plan_for_identity(
            &prepared_shape,
            &prepared_binding,
            DurabilityTier::Global,
            Some(&prepared_plan),
            user(0xa1),
        )
        .unwrap()
        .into_iter()
        .map(|row| row.row_uuid())
        .collect::<BTreeSet<_>>();

    assert_eq!(prepared_rows, BTreeSet::from([row(0xd1)]));
}
