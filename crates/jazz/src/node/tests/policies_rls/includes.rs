// Required and holes include semantics under row-level security.

fn required_include_rls_schema() -> JazzSchema {
    build_public_test_schema(
        PublicSchemaBuilder::new()
            .table(
                PublicTableSchemaBuilder::new("roots")
                    .column("title", PublicColumnType::Text)
                    .fk_column("target", "targets"),
            )
            .table(
                PublicTableSchemaBuilder::new("targets")
                    .column("title", PublicColumnType::Text)
                    .column("owner", PublicColumnType::Uuid)
                    .policies(
                        PublicTablePolicies::new().with_select(PublicPolicyExpr::eq_session(
                            "owner",
                            vec!["user_id".to_owned()],
                        )),
                    ),
            ),
    )
}

#[test]
fn parent_ref_join_matches_a_declared_id_column_instead_of_the_physical_row_uuid() {
    let schema = build_public_test_schema(
        PublicSchemaBuilder::new()
            .table(
                PublicTableSchemaBuilder::new("memberships")
                    .fk_column("chat", "chats")
                    .column("label", PublicColumnType::Text),
            )
            .table(
                PublicTableSchemaBuilder::new("chats")
                    .column("id", PublicColumnType::Uuid)
                    .column("title", PublicColumnType::Text),
            ),
    );
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    let physical_chat = row(0xc1);
    let declared_chat_id = row(0xaa);
    let membership = row(0xd1);
    let tx = core
        .commit_mergeable_many_settled(vec![
            MergeableCommit::new("chats", physical_chat, 10).cells(BTreeMap::from([
                ("id".to_owned(), Value::Uuid(declared_chat_id.0)),
                ("title".to_owned(), v("declared-id chat")),
            ])),
            MergeableCommit::new("memberships", membership, 11).cells(BTreeMap::from([
                ("chat".to_owned(), Value::Uuid(declared_chat_id.0)),
                ("label".to_owned(), v("membership")),
            ])),
        ])
        .unwrap();
    core.accept_global_for_test(tx).unwrap();

    // This is the core query shape emitted by a binding-layer parent include:
    // correlate the child's foreign-key value with the parent's declared `id`.
    let shape = Query::from("memberships")
        .join_via_column("chats", "id", "chat", [])
        .validate(&core.catalogue.schema)
        .unwrap();
    let rows = required_include_rows(&mut core, &shape, AuthorSubject::SYSTEM);
    assert_eq!(
        rows.into_iter().map(|row| row.row_uuid()).collect::<Vec<_>>(),
        vec![membership]
    );

}

/// A serving authority's internal point-read authorization must select the
/// physical target row even when `id` is declared user data. Alice owns the
/// row, while its declared id deliberately differs from its storage identity.
#[test]
fn point_read_authorization_keeps_using_physical_row_uuid_with_declared_id() {
    let alice = user(0xa1);
    let bob = user(0xa2);
    let schema = build_public_test_schema(PublicSchemaBuilder::new().table(
        PublicTableSchemaBuilder::new("documents")
            .column("id", PublicColumnType::Uuid)
            .column("owner", PublicColumnType::Uuid)
            .policies(
                PublicTablePolicies::new()
                    .with_select(PublicPolicyExpr::eq_session(
                        "owner",
                        vec!["claims".to_owned(), "sub".to_owned()],
                    ))
                    .with_insert(PublicPolicyExpr::True)
                    .with_update(Some(PublicPolicyExpr::True), PublicPolicyExpr::True)
                    .with_delete(PublicPolicyExpr::True),
            ),
    ));
    let (_core_dir, mut core) = open_node_with_schema(node(0xa9), schema);
    core.set_session_claims(
        alice,
        BTreeMap::from([("sub".to_owned(), Value::Uuid(alice.test_uuid()))]),
    );
    core.set_session_claims(
        bob,
        BTreeMap::from([("sub".to_owned(), Value::Uuid(bob.test_uuid()))]),
    );
    let physical_row = row(0xc1);
    let other_physical_row = row(0xc2);
    let declared_id = row(0xd1);
    let tx = core
        .commit_mergeable_unit_settled(
            MergeableCommit::new("documents", physical_row, 10).cells(BTreeMap::from([
                ("id".to_owned(), Value::Uuid(declared_id.0)),
                ("owner".to_owned(), Value::Uuid(alice.test_uuid())),
            ])),
        )
        .unwrap();
    core.accept_global_for_test(tx.0).unwrap();
    let other_tx = core
        .commit_mergeable_unit_settled(
            MergeableCommit::new("documents", other_physical_row, 11).cells(BTreeMap::from([
                ("id".to_owned(), Value::Uuid(row(0xd2).0)),
                ("owner".to_owned(), Value::Uuid(alice.test_uuid())),
            ])),
        )
        .unwrap();
    core.accept_global_for_test(other_tx.0).unwrap();

    let generic = Query::from("documents")
        .validate(&core.catalogue.schema)
        .unwrap();
    let generic_binding = generic.bind(BTreeMap::new()).unwrap();
    assert_eq!(
        core.query_rows_for_link(&generic, &generic_binding, DurabilityTier::Global, alice)
            .unwrap()
            .into_iter()
            .map(|row| row.row_uuid())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([physical_row, other_physical_row]),
        "the generic policy graph must initially authorize both of Alice's documents"
    );
    let cached_policy_graphs = core
        .query
        .policy_authorization_graph_cache
        .keys()
        .cloned()
        .collect::<BTreeSet<_>>();

    core.reset_query_engine_read_metrics();
    assert!(
        core.dry_run_read_current_allows("documents", physical_row, alice)
            .unwrap()
    );
    assert_eq!(
        core.query_engine_read_metrics().source_primary_key_scans,
        1,
        "the authorization probe must point-scan the physical row"
    );
    assert_eq!(
        core.query
            .policy_authorization_graph_cache
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>(),
        cached_policy_graphs,
        "point authorization must preserve the reusable generic policy graph"
    );
    assert!(
        core.dry_run_write_current_allows("documents", physical_row, alice)
            .unwrap()
    );
    assert!(
        !core
            .dry_run_read_current_allows("documents", physical_row, bob)
            .unwrap(),
        "a point-specialized policy proof must retain its session scope"
    );
    assert_eq!(
        core.query
            .policy_authorization_graph_cache
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>(),
        cached_policy_graphs,
        "a denied point proof must not retain another identity's specialization"
    );
    assert_eq!(
        core.query_rows_for_link(&generic, &generic_binding, DurabilityTier::Global, alice)
            .unwrap()
            .into_iter()
            .map(|row| row.row_uuid())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([physical_row, other_physical_row]),
        "a generic query after a point proof must not inherit that point's bound"
    );

    let ownership_change = core
        .commit_mergeable_unit_settled(
            MergeableCommit::new("documents", physical_row, 20)
                .cells(BTreeMap::from([("owner".to_owned(), Value::Uuid(bob.test_uuid()))])),
        )
        .unwrap();
    core.accept_global_for_test(ownership_change.0).unwrap();
    assert!(
        !core
            .dry_run_read_current_allows("documents", physical_row, alice)
            .unwrap(),
        "policy-dependency changes must revoke the former owner's point access"
    );
    assert!(
        core.dry_run_read_current_allows("documents", physical_row, bob)
            .unwrap(),
        "policy-dependency changes must grant the new owner's point access"
    );
    assert_eq!(
        core.query_rows_for_link(&generic, &generic_binding, DurabilityTier::Global, bob)
            .unwrap()
            .into_iter()
            .map(|row| row.row_uuid())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([physical_row]),
        "a generic query after a point proof must use its generic policy graph"
    );
    assert_eq!(
        core.query_rows_for_link(&generic, &generic_binding, DurabilityTier::Global, alice)
            .unwrap()
            .into_iter()
            .map(|row| row.row_uuid())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([other_physical_row]),
        "the former owner keeps only the separately authorized generic result"
    );

    let deletion = core
        .commit_mergeable_unit_settled(
            MergeableCommit::new("documents", physical_row, 30).deletion(DeletionEvent::Deleted),
        )
        .unwrap();
    core.accept_global_for_test(deletion.0).unwrap();
    assert!(
        !core
            .dry_run_read_current_allows("documents", physical_row, bob)
            .unwrap(),
        "a deleted target must be a point-authorization miss"
    );
    let restoration = core
        .commit_mergeable_unit_settled(
            MergeableCommit::new("documents", physical_row, 40)
                .deletion(DeletionEvent::Restored),
        )
        .unwrap();
    core.accept_global_for_test(restoration.0).unwrap();
    assert!(
        core.dry_run_read_current_allows("documents", physical_row, bob)
            .unwrap(),
        "restoring the target must restore point authorization from current policy evidence"
    );
}

fn required_include_shape(core: &NodeState<RocksDbStorage>, include: Include) -> ValidatedQuery {
    Query::from("roots")
        .include_with(include)
        .validate(&core.catalogue.schema)
        .unwrap()
}

fn required_include_rows(
    core: &mut NodeState<RocksDbStorage>,
    shape: &ValidatedQuery,
    identity: AuthorSubject,
) -> Vec<CurrentRow> {
    let binding = shape.bind(BTreeMap::new()).unwrap();
    if matches!(identity, AuthorSubject::Authenticated(_)) {
        core.set_session_claims(
            identity,
            BTreeMap::from([("user_id".to_owned(), Value::Uuid(identity.test_uuid()))]),
        );
    }
    core.query_rows_for_link(shape, &binding, DurabilityTier::Global, identity)
        .unwrap()
}

fn seed_required_include_fixture(core: &mut NodeState<RocksDbStorage>, readable_owner: AuthorSubject) {
    core.set_session_claims(
        readable_owner,
        BTreeMap::from([(
            "user_id".to_owned(),
            Value::Uuid(readable_owner.test_uuid()),
        )]),
    );
    let unreadable_owner = user(0xb2);
    let target_tx = core
        .commit_mergeable_many_settled(vec![
            MergeableCommit::new("targets", row(0xc1), 10)
                .cells(owner_cells(unreadable_owner, "hidden target")),
            MergeableCommit::new("targets", row(0xc2), 10)
                .cells(owner_cells(readable_owner, "visible target")),
        ])
        .unwrap();
    core.accept_global_for_test(target_tx).unwrap();

    let root_tx = core
        .commit_mergeable_many_settled(vec![
            MergeableCommit::new("roots", row(0xd1), 20).cells(BTreeMap::from([
                ("title".to_owned(), v("references hidden")),
                ("target".to_owned(), Value::Uuid(row(0xc1).0)),
            ])),
            MergeableCommit::new("roots", row(0xd2), 20).cells(BTreeMap::from([
                ("title".to_owned(), v("references visible")),
                ("target".to_owned(), Value::Uuid(row(0xc2).0)),
            ])),
        ])
        .unwrap();
    core.accept_global_for_test(root_tx).unwrap();
}

fn seed_missing_required_include_fixture(core: &mut NodeState<RocksDbStorage>) {
    let root_tx = core
        .commit_mergeable_many_settled(vec![
            MergeableCommit::new("roots", row(0xd1), 20).cells(BTreeMap::from([
                ("title".to_owned(), v("references missing")),
                ("target".to_owned(), Value::Uuid(row(0xcf).0)),
            ])),
            MergeableCommit::new("roots", row(0xd2), 20).cells(BTreeMap::from([
                ("title".to_owned(), v("references existing")),
                ("target".to_owned(), Value::Uuid(row(0xc2).0)),
            ])),
            MergeableCommit::new("targets", row(0xc2), 10)
                .cells(owner_cells(user(0xc2), "existing target")),
        ])
        .unwrap();
    core.accept_global_for_test(root_tx).unwrap();
}

fn seed_null_required_include_fixture(core: &mut NodeState<RocksDbStorage>) {
    let root_tx = core
        .commit_mergeable_many_settled(vec![
            MergeableCommit::new("roots", row(0xd1), 20)
                .cells(BTreeMap::from([("title".to_owned(), v("references null"))])),
            MergeableCommit::new("roots", row(0xd2), 20).cells(BTreeMap::from([
                ("title".to_owned(), v("references existing")),
                ("target".to_owned(), Value::Uuid(row(0xc2).0)),
            ])),
            MergeableCommit::new("targets", row(0xc2), 10)
                .cells(owner_cells(user(0xc2), "existing target")),
        ])
        .unwrap();
    core.accept_global_for_test(root_tx).unwrap();
}

fn multi_segment_required_include_rls_schema() -> JazzSchema {
    build_public_test_schema(
        PublicSchemaBuilder::new()
            .table(
                PublicTableSchemaBuilder::new("roots")
                    .column("title", PublicColumnType::Text)
                    .fk_column("project", "projects"),
            )
            .table(
                PublicTableSchemaBuilder::new("projects")
                    .column("title", PublicColumnType::Text)
                    .fk_column("org", "orgs"),
            )
            .table(
                PublicTableSchemaBuilder::new("orgs")
                    .column("title", PublicColumnType::Text)
                    .column("owner", PublicColumnType::Uuid)
                    .policies(
                        PublicTablePolicies::new().with_select(PublicPolicyExpr::eq_session(
                            "owner",
                            vec!["user_id".to_owned()],
                        )),
                    ),
            ),
    )
}

fn seed_multi_segment_include_fixture(
    core: &mut NodeState<RocksDbStorage>,
    readable_owner: AuthorSubject,
) {
    core.set_session_claims(
        readable_owner,
        BTreeMap::from([(
            "user_id".to_owned(),
            Value::Uuid(readable_owner.test_uuid()),
        )]),
    );
    let unreadable_owner = user(0xb2);
    let tx = core
        .commit_mergeable_many_settled(vec![
            MergeableCommit::new("orgs", row(0xe1), 10)
                .cells(owner_cells(unreadable_owner, "hidden org")),
            MergeableCommit::new("orgs", row(0xe2), 10)
                .cells(owner_cells(readable_owner, "visible org")),
            MergeableCommit::new("projects", row(0xc1), 20).cells(BTreeMap::from([
                ("title".to_owned(), v("project hidden")),
                ("org".to_owned(), Value::Uuid(row(0xe1).0)),
            ])),
            MergeableCommit::new("projects", row(0xc2), 20).cells(BTreeMap::from([
                ("title".to_owned(), v("project visible")),
                ("org".to_owned(), Value::Uuid(row(0xe2).0)),
            ])),
            MergeableCommit::new("projects", row(0xc3), 20).cells(BTreeMap::from([
                ("title".to_owned(), v("project missing")),
                ("org".to_owned(), Value::Uuid(row(0xef).0)),
            ])),
            MergeableCommit::new("roots", row(0xd1), 30).cells(BTreeMap::from([
                ("title".to_owned(), v("references hidden org")),
                ("project".to_owned(), Value::Uuid(row(0xc1).0)),
            ])),
            MergeableCommit::new("roots", row(0xd2), 30).cells(BTreeMap::from([
                ("title".to_owned(), v("references visible org")),
                ("project".to_owned(), Value::Uuid(row(0xc2).0)),
            ])),
            MergeableCommit::new("roots", row(0xd3), 30).cells(BTreeMap::from([
                ("title".to_owned(), v("references missing org")),
                ("project".to_owned(), Value::Uuid(row(0xc3).0)),
            ])),
        ])
        .unwrap();
    core.accept_global_for_test(tx).unwrap();
}

fn canonical_view_update_rows(update: &SyncMessage) -> (Vec<ResultRowEntry>, Vec<ResultRowEntry>) {
    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        result_member_adds,
        result_member_removes,
        ..
    }) = update
    else {
        panic!("expected view update");
    };
    let mut adds = result_member_adds
        .iter()
        .filter_map(crate::protocol::ResultMemberEntry::as_row)
        .collect::<Vec<_>>();
    let mut removes = result_member_removes
        .iter()
        .filter_map(crate::protocol::ResultMemberEntry::as_row)
        .collect::<Vec<_>>();
    adds.sort();
    removes.sort();
    (adds, removes)
}

fn canonical_view_update_rows_for_table(
    update: &SyncMessage,
    table: &str,
) -> (Vec<ResultRowEntry>, Vec<ResultRowEntry>) {
    let (adds, removes) = canonical_view_update_rows(update);
    (
        adds.into_iter()
            .filter(|(entry_table, _, _)| entry_table.as_str() == table)
            .collect(),
        removes
            .into_iter()
            .filter(|(entry_table, _, _)| entry_table.as_str() == table)
            .collect(),
    )
}

#[test]
fn required_include_unreadable_target_drops_parent() {
    let schema = required_include_rls_schema();
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    let reader = user(0xa1);
    seed_required_include_fixture(&mut core, reader);
    let shape = required_include_shape(&core, Include::new("target"));

    let rows = required_include_rows(&mut core, &shape, reader);
    assert_eq!(
        rows.into_iter()
            .map(|row| row.row_uuid())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([row(0xd2)])
    );
}

#[test]
fn required_include_uses_identity_sensitive_graph_path_without_shared_plan_cache() {
    let schema = required_include_rls_schema();
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    let reader = user(0xa1);
    seed_required_include_fixture(&mut core, reader);
    let shape = required_include_shape(&core, Include::new("target").require_includes());

    core.clear_prepared_query_plan_cache_for_test();
    let rows = required_include_rows(&mut core, &shape, reader);
    assert_eq!(
        rows.into_iter()
            .map(|row| row.row_uuid())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([row(0xd2)])
    );
    assert!(
        core.prepared_query_plan_cache_is_empty_for_test(),
        "identity-sensitive include membership lowering must not enter the shared prepared-plan cache"
    );
}

#[test]
fn inner_multi_segment_include_missing_or_unreadable_second_hop_drops_parent() {
    let schema = multi_segment_required_include_rls_schema();
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    let reader = user(0xa1);
    seed_multi_segment_include_fixture(&mut core, reader);
    let shape = required_include_shape(&core, Include::new("project.org"));

    let rows = required_include_rows(&mut core, &shape, reader);
    assert_eq!(
        rows.into_iter()
            .map(|row| row.row_uuid())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([row(0xd2)])
    );
}

/// A title-only root projection still retains its hidden `project` join key
/// long enough to gate the nested `project.org` include.
///
/// alice ──reads title──► root.project ──requires──► project.org
#[test]
fn sparse_root_projection_preserves_multisegment_inner_include_join_key() {
    let schema = multi_segment_required_include_rls_schema();
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    let reader = user(0xa1);
    seed_multi_segment_include_fixture(&mut core, reader);
    let shape = Query::from("roots")
        .select(["title"])
        .include_with(Include::new("project.org"))
        .validate(&core.catalogue.schema)
        .unwrap();

    let rows = required_include_rows(&mut core, &shape, reader);
    assert_eq!(
        rows.into_iter()
            .map(|row| row.row_uuid())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([row(0xd2)])
    );
}

#[test]
fn maintained_subscription_view_multi_segment_inner_include_payload_references_visible_path() {
    let schema = multi_segment_required_include_rls_schema();
    let (_full_recompute_dir, mut full_recompute_core) =
        open_node_with_schema(node(9), schema.clone());
    let (_maintained_dir, mut maintained_core) = open_node_with_schema(node(9), schema);
    let reader = user(0xa1);
    seed_multi_segment_include_fixture(&mut full_recompute_core, reader);
    seed_multi_segment_include_fixture(&mut maintained_core, reader);
    let shape = required_include_shape(&maintained_core, Include::new("project.org"));
    let binding = shape.bind(BTreeMap::new()).unwrap();

    let mut maintained_peer = PeerState::client_link(reader);

    let full_recompute_rows = required_include_rows(&mut full_recompute_core, &shape, reader);
    assert_eq!(
        full_recompute_rows
            .into_iter()
            .map(|row| row.row_uuid())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([row(0xd2)])
    );

    let maintained = maintained_peer
        .rehydrate_query(&mut maintained_core, &shape, &binding)
        .unwrap();
    assert_eq!(
        maintained_peer
            .maintained_subscription_view_metrics()
            .hits_out,
        1
    );

    let (result_adds, result_removes) = canonical_view_update_rows(&maintained);
    assert!(result_removes.is_empty());
    assert_eq!(
        result_adds
            .iter()
            .filter(|entry| entry.0.as_str() == "roots")
            .map(|entry| entry.1)
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([row(0xd2)])
    );
    assert_view_update_only_references_rows(
        &maintained,
        BTreeSet::from([row(0xd2), row(0xc2), row(0xe2)]),
    );
}

#[test]
fn prepared_subscription_multi_segment_forward_include_keeps_root_delta() {
    let schema = multi_segment_required_include_rls_schema();
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    let reader = user(0xa1);
    seed_multi_segment_include_fixture(&mut core, reader);
    let shape = required_include_shape(&core, Include::new("project.org"));
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let mut peer = PeerState::client_link(reader);
    peer.rehydrate_query(&mut core, &shape, &binding).unwrap();

    let update_tx = core
        .commit_mergeable_settled(
            MergeableCommit::new("roots", row(0xd2), 40)
                .parents(vec![TxId::new(TxTime(10), node(9))])
                .cells(BTreeMap::from([
                    ("title".to_owned(), v("updated visible root")),
                    ("project".to_owned(), Value::Uuid(row(0xc2).0)),
                ])),
        )
        .unwrap();
    core.accept_global_for_test(update_tx).unwrap();

    let update = peer.query_update(&mut core, &shape, &binding).unwrap();
    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        result_member_adds, ..
    }) = update
    else {
        panic!("expected view update");
    };
    assert_eq!(
        result_member_adds
            .into_iter()
            .filter_map(crate::protocol::ResultMemberEntry::into_row)
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([("roots".to_owned().into(), row(0xd2), update_tx)])
    );
}

#[test]
fn maintained_inner_multi_segment_include_payload_references_visible_path_only() {
    let schema = multi_segment_required_include_rls_schema();
    let (_maintained_dir, mut maintained_core) = open_node_with_schema(node(9), schema);
    let reader = user(0xa1);
    seed_multi_segment_include_fixture(&mut maintained_core, reader);
    let shape = required_include_shape(&maintained_core, Include::new("project.org"));
    let binding = shape.bind(BTreeMap::new()).unwrap();

    let mut maintained_peer = PeerState::client_link(reader);

    let maintained = maintained_peer
        .rehydrate_query(&mut maintained_core, &shape, &binding)
        .unwrap();

    let (adds, removes) = canonical_view_update_rows(&maintained);
    assert!(removes.is_empty());
    assert_eq!(
        adds.into_iter()
            .filter(|entry| entry.0.as_str() == "roots")
            .map(|entry| entry.1)
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([row(0xd2)])
    );
    assert_view_update_only_references_rows(
        &maintained,
        BTreeSet::from([row(0xd2), row(0xc2), row(0xe2)]),
    );
}

#[test]
fn holes_multi_segment_include_keeps_parent_and_withholds_unreadable_second_hop() {
    let schema = multi_segment_required_include_rls_schema();
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    let reader = user(0xa1);
    seed_multi_segment_include_fixture(&mut core, reader);
    let shape = required_include_shape(
        &core,
        Include::new("project.org").join_mode(JoinMode::Holes),
    );

    let rows = required_include_rows(&mut core, &shape, reader);
    assert_eq!(
        rows.iter()
            .map(|row| row.row_uuid())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([row(0xd1), row(0xd2), row(0xd3)])
    );

    let binding = shape.bind(BTreeMap::new()).unwrap();
    let update = core
        .view_update_for_query_binding_with_peer_payload_inventory(
            &shape,
            &binding,
            SubscriptionKey {
                shape_id: shape.shape_id(),
                binding_id: binding.binding_id(),
            read_view: Default::default(),
},
            [],
            [],
            [],
            reader,
        )
        .unwrap();
    assert_view_update_only_references_rows(
        &update,
        BTreeSet::from([
            row(0xd1),
            row(0xd2),
            row(0xd3),
            row(0xc1),
            row(0xc2),
            row(0xc3),
            row(0xe2),
        ]),
    );
}

#[test]
fn maintained_subscription_view_multi_segment_holes_include_payload_references_visible_paths() {
    let schema = multi_segment_required_include_rls_schema();
    let (_maintained_dir, mut maintained_core) = open_node_with_schema(node(9), schema);
    let reader = user(0xa1);
    seed_multi_segment_include_fixture(&mut maintained_core, reader);
    let shape = required_include_shape(
        &maintained_core,
        Include::new("project.org").join_mode(JoinMode::Holes),
    );
    let binding = shape.bind(BTreeMap::new()).unwrap();

    let mut maintained_peer = PeerState::client_link(reader);

    let maintained = maintained_peer
        .rehydrate_query(&mut maintained_core, &shape, &binding)
        .unwrap();
    let (adds, removes) = canonical_view_update_rows(&maintained);
    assert!(removes.is_empty());
    assert_eq!(
        adds.into_iter()
            .filter(|entry| entry.0.as_str() == "roots")
            .map(|entry| entry.1)
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([row(0xd1), row(0xd2), row(0xd3)])
    );
    assert_eq!(
        maintained_peer
            .maintained_subscription_view_metrics()
            .hits_out,
        1
    );

    assert_view_update_only_references_rows(
        &maintained,
        BTreeSet::from([
            row(0xd1),
            row(0xd2),
            row(0xd3),
            row(0xc1),
            row(0xc2),
            row(0xc3),
            row(0xe2),
        ]),
    );
}

#[test]
fn inner_include_missing_target_drops_parent() {
    let schema = required_include_rls_schema();
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    seed_missing_required_include_fixture(&mut core);
    let shape = required_include_shape(&core, Include::new("target"));

    let rows = required_include_rows(&mut core, &shape, AuthorSubject::SYSTEM);
    assert_eq!(
        rows.into_iter()
            .map(|row| row.row_uuid())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([row(0xd2)])
    );
}

#[test]
fn inner_include_null_target_drops_parent() {
    let schema = required_include_rls_schema();
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    seed_null_required_include_fixture(&mut core);
    let shape = required_include_shape(&core, Include::new("target"));

    let rows = required_include_rows(&mut core, &shape, AuthorSubject::SYSTEM);
    assert_eq!(
        rows.into_iter()
            .map(|row| row.row_uuid())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([row(0xd2)])
    );
}

#[test]
fn holes_include_missing_target_keeps_parent() {
    let schema = required_include_rls_schema();
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    seed_missing_required_include_fixture(&mut core);
    let shape = required_include_shape(&core, Include::new("target").join_mode(JoinMode::Holes));

    let rows = required_include_rows(&mut core, &shape, AuthorSubject::SYSTEM);
    assert_eq!(
        rows.into_iter()
            .map(|row| row.row_uuid())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([row(0xd1), row(0xd2)])
    );
}

#[test]
fn holes_include_keeps_parent_without_root_membership_filtering() {
    let schema = required_include_rls_schema();
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    seed_missing_required_include_fixture(&mut core);
    let shape = required_include_shape(&core, Include::new("target").join_mode(JoinMode::Holes));

    let rows = required_include_rows(&mut core, &shape, AuthorSubject::SYSTEM);
    assert_eq!(
        rows.into_iter()
            .map(|row| row.row_uuid())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([row(0xd1), row(0xd2)])
    );
}

#[test]
fn holes_include_unreadable_target_keeps_parent_and_withholds_target() {
    let schema = required_include_rls_schema();
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    let reader = user(0xa1);
    seed_required_include_fixture(&mut core, reader);
    let shape = required_include_shape(&core, Include::new("target").join_mode(JoinMode::Holes));

    let rows = required_include_rows(&mut core, &shape, reader);
    assert_eq!(
        rows.iter()
            .map(|row| row.row_uuid())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([row(0xd1), row(0xd2)])
    );

    let binding = shape.bind(BTreeMap::new()).unwrap();
    let update = core
        .view_update_for_query_binding_with_peer_payload_inventory(
            &shape,
            &binding,
            SubscriptionKey {
                shape_id: shape.shape_id(),
                binding_id: binding.binding_id(),
            read_view: Default::default(),
},
            [],
            [],
            [],
            reader,
        )
        .unwrap();
    assert_view_update_only_references_rows(
        &update,
        BTreeSet::from([row(0xd1), row(0xd2), row(0xc2)]),
    );
}

#[test]
fn system_identity_required_include_uses_existence_only_resolvability() {
    let schema = required_include_rls_schema();
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    seed_required_include_fixture(&mut core, user(0xa1));
    let shape = required_include_shape(&core, Include::new("target").require_includes());

    let rows = required_include_rows(&mut core, &shape, AuthorSubject::SYSTEM);
    assert_eq!(
        rows.into_iter()
            .map(|row| row.row_uuid())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([row(0xd1), row(0xd2)])
    );
}
