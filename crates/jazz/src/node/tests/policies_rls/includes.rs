// Required and holes include semantics under row-level security.

fn required_include_rls_schema() -> JazzSchema {
    JazzSchema::new([
        TableSchema::new(
            "roots",
            [
                ColumnSchema::new("title", ColumnType::String),
                ColumnSchema::new("target", ColumnType::Uuid),
            ],
        )
        .with_reference("target", "targets"),
        TableSchema::new(
            "targets",
            [
                ColumnSchema::new("title", ColumnType::String),
                ColumnSchema::new("owner", ColumnType::Uuid),
            ],
        )
        .with_read_policy(Policy::owner_only("targets", "owner")),
    ])
}

#[test]
fn parent_ref_join_matches_a_declared_id_column_instead_of_the_physical_row_uuid() {
    let schema = JazzSchema::new([
        TableSchema::new(
            "memberships",
            [
                ColumnSchema::new("chat", ColumnType::Uuid),
                ColumnSchema::new("label", ColumnType::String),
            ],
        )
        .with_reference("chat", "chats"),
        TableSchema::new(
            "chats",
            [
                ColumnSchema::new("id", ColumnType::Uuid),
                ColumnSchema::new("title", ColumnType::String),
            ],
        ),
    ]);
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    let physical_chat = row(0xc1);
    let declared_chat_id = row(0xaa);
    let membership = row(0xd1);
    let tx = core
        .commit_mergeable_many(vec![
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
    core.apply_fate_update(
        tx,
        Fate::Accepted,
        Some(core.clock.next_global_seq),
        Some(DurabilityTier::Global),
    )
    .unwrap();

    // This is the core query shape emitted by a binding-layer parent include:
    // correlate the child's foreign-key value with the parent's declared `id`.
    let shape = Query::from("memberships")
        .join_via_column("chats", "id", "chat", [])
        .validate(&core.catalogue.schema)
        .unwrap();
    let rows = required_include_rows(&mut core, &shape, AuthorId::SYSTEM);
    assert_eq!(
        rows.into_iter().map(|row| row.row_uuid()).collect::<Vec<_>>(),
        vec![membership]
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
    identity: AuthorId,
) -> Vec<CurrentRow> {
    let binding = shape.bind(BTreeMap::new()).unwrap();
    core.query_rows_for_link(shape, &binding, DurabilityTier::Global, identity)
        .unwrap()
}

fn seed_required_include_fixture(core: &mut NodeState<RocksDbStorage>, readable_owner: AuthorId) {
    let unreadable_owner = user(0xb2);
    let target_tx = core
        .commit_mergeable_many(vec![
            MergeableCommit::new("targets", row(0xc1), 10)
                .cells(owner_cells(unreadable_owner, "hidden target")),
            MergeableCommit::new("targets", row(0xc2), 10)
                .cells(owner_cells(readable_owner, "visible target")),
        ])
        .unwrap();
    core.apply_fate_update(
        target_tx,
        Fate::Accepted,
        Some(core.clock.next_global_seq),
        Some(DurabilityTier::Global),
    )
    .unwrap();

    let root_tx = core
        .commit_mergeable_many(vec![
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
    core.apply_fate_update(
        root_tx,
        Fate::Accepted,
        Some(core.clock.next_global_seq),
        Some(DurabilityTier::Global),
    )
    .unwrap();
}

fn seed_missing_required_include_fixture(core: &mut NodeState<RocksDbStorage>) {
    let root_tx = core
        .commit_mergeable_many(vec![
            MergeableCommit::new("roots", row(0xd1), 20).cells(BTreeMap::from([
                ("title".to_owned(), v("references missing")),
                ("target".to_owned(), Value::Uuid(row(0xcf).0)),
            ])),
            MergeableCommit::new("roots", row(0xd2), 20).cells(BTreeMap::from([
                ("title".to_owned(), v("references existing")),
                ("target".to_owned(), Value::Uuid(row(0xc2).0)),
            ])),
            MergeableCommit::new("targets", row(0xc2), 10)
                .cells(owner_cells(AuthorId::SYSTEM, "existing target")),
        ])
        .unwrap();
    core.apply_fate_update(
        root_tx,
        Fate::Accepted,
        Some(core.clock.next_global_seq),
        Some(DurabilityTier::Global),
    )
    .unwrap();
}

fn seed_null_required_include_fixture(core: &mut NodeState<RocksDbStorage>) {
    let root_tx = core
        .commit_mergeable_many(vec![
            MergeableCommit::new("roots", row(0xd1), 20)
                .cells(BTreeMap::from([("title".to_owned(), v("references null"))])),
            MergeableCommit::new("roots", row(0xd2), 20).cells(BTreeMap::from([
                ("title".to_owned(), v("references existing")),
                ("target".to_owned(), Value::Uuid(row(0xc2).0)),
            ])),
            MergeableCommit::new("targets", row(0xc2), 10)
                .cells(owner_cells(AuthorId::SYSTEM, "existing target")),
        ])
        .unwrap();
    core.apply_fate_update(
        root_tx,
        Fate::Accepted,
        Some(core.clock.next_global_seq),
        Some(DurabilityTier::Global),
    )
    .unwrap();
}

fn multi_segment_required_include_rls_schema() -> JazzSchema {
    JazzSchema::new([
        TableSchema::new(
            "roots",
            [
                ColumnSchema::new("title", ColumnType::String),
                ColumnSchema::new("project", ColumnType::Uuid),
            ],
        )
        .with_reference("project", "projects"),
        TableSchema::new(
            "projects",
            [
                ColumnSchema::new("title", ColumnType::String),
                ColumnSchema::new("org", ColumnType::Uuid),
            ],
        )
        .with_reference("org", "orgs"),
        TableSchema::new(
            "orgs",
            [
                ColumnSchema::new("title", ColumnType::String),
                ColumnSchema::new("owner", ColumnType::Uuid),
            ],
        )
        .with_read_policy(Policy::owner_only("orgs", "owner")),
    ])
}

fn seed_multi_segment_include_fixture(
    core: &mut NodeState<RocksDbStorage>,
    readable_owner: AuthorId,
) {
    let unreadable_owner = user(0xb2);
    let tx = core
        .commit_mergeable_many(vec![
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
    core.apply_fate_update(
        tx,
        Fate::Accepted,
        Some(core.clock.next_global_seq),
        Some(DurabilityTier::Global),
    )
    .unwrap();
}

fn canonical_view_update_rows(update: &SyncMessage) -> (Vec<ResultRowEntry>, Vec<ResultRowEntry>) {
    let SyncMessage::ViewUpdate {
        result_member_adds,
        result_member_removes,
        ..
    } = update
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
        .commit_mergeable(
            MergeableCommit::new("roots", row(0xd2), 40)
                .parents(vec![TxId::new(TxTime(10), node(9))])
                .cells(BTreeMap::from([
                    ("title".to_owned(), v("updated visible root")),
                    ("project".to_owned(), Value::Uuid(row(0xc2).0)),
                ])),
        )
        .unwrap();
    core.apply_fate_update(
        update_tx,
        Fate::Accepted,
        Some(core.clock.next_global_seq),
        Some(DurabilityTier::Global),
    )
    .unwrap();

    let update = peer.query_update(&mut core, &shape, &binding).unwrap();
    let SyncMessage::ViewUpdate {
        result_member_adds, ..
    } = update
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

    let rows = required_include_rows(&mut core, &shape, AuthorId::SYSTEM);
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

    let rows = required_include_rows(&mut core, &shape, AuthorId::SYSTEM);
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

    let rows = required_include_rows(&mut core, &shape, AuthorId::SYSTEM);
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

    let rows = required_include_rows(&mut core, &shape, AuthorId::SYSTEM);
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

    let rows = required_include_rows(&mut core, &shape, AuthorId::SYSTEM);
    assert_eq!(
        rows.into_iter()
            .map(|row| row.row_uuid())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([row(0xd1), row(0xd2)])
    );
}
