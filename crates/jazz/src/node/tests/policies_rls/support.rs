// Shared accepted-write, priority, recursion, and maintained-view assertions.

fn accept_global(core: &mut NodeState<RocksDbStorage>, commit: MergeableCommit) -> TxId {
    let tx_id = core.commit_mergeable_settled(commit).unwrap();
    core.accept_global_for_test(tx_id).unwrap();
    tx_id
}

fn priority_schema() -> JazzSchema {
    build_public_test_schema(PublicSchemaBuilder::new().table(
        PublicTableSchemaBuilder::new("todos")
            .column("title", PublicColumnType::Text)
            .column("priority", PublicColumnType::Timestamp),
    ))
}

fn priority_cells(title: impl Into<String>, priority: u64) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("title".to_owned(), Value::String(title.into())),
        ("priority".to_owned(), Value::U64(priority)),
    ])
}

fn assert_view_update_rows<const A: usize, const R: usize>(
    update: SyncMessage,
    expected_adds: [(&str, RowUuid, TxId); A],
    expected_removes: [(&str, RowUuid, TxId); R],
) {
    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        result_member_adds,
        result_member_removes,
        program_fact_adds,
        program_fact_removes,
        ..
    }) = update
    else {
        panic!("expected view update");
    };
    // Peer updates carry the exact source closure. The receiving maintained
    // graph, rather than the authority, derives result membership; a root
    // source occurrence is therefore the peer-wire equivalent of these
    // single-table result assertions.
    assert!(result_member_adds.is_empty());
    assert!(result_member_removes.is_empty());
    let mut result_member_adds = program_fact_adds
        .iter()
        .filter_map(|fact| match fact {
            crate::protocol::ProgramFactEntry::CoveredInput(input)
                if input.version.layer == crate::protocol::ResultRowLayer::Content =>
            {
                Some((input.version_table.clone(), input.source_row, input.version.tx))
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    let mut result_member_removes = program_fact_removes
        .iter()
        .filter_map(|fact| match fact {
            crate::protocol::ProgramFactEntry::CoveredInput(input)
                if input.version.layer == crate::protocol::ResultRowLayer::Content =>
            {
                Some((input.version_table.clone(), input.source_row, input.version.tx))
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    result_member_adds.sort();
    result_member_removes.sort();
    let mut expected_adds = expected_adds
        .into_iter()
        .map(|(table, row_uuid, tx_id)| (table.to_owned().into(), row_uuid, tx_id))
        .collect::<Vec<_>>();
    let mut expected_removes = expected_removes
        .into_iter()
        .map(|(table, row_uuid, tx_id)| (table.to_owned().into(), row_uuid, tx_id))
        .collect::<Vec<_>>();
    expected_adds.sort();
    expected_removes.sort();
    assert_eq!(result_member_adds, expected_adds);
    assert_eq!(result_member_removes, expected_removes);
}

fn recursive_reachable_schema() -> JazzSchema {
    build_public_test_schema(
        PublicSchemaBuilder::new()
            .table(
                PublicTableSchemaBuilder::new("docs").column("title", PublicColumnType::Text),
            )
            .table(
                PublicTableSchemaBuilder::new("teams").column("name", PublicColumnType::Text),
            )
            .table(
                PublicTableSchemaBuilder::new("teamEdges")
                    .fk_column("member", "teams")
                    .fk_column("parent", "teams"),
            )
            .table(
                PublicTableSchemaBuilder::new("teamAccess")
                    .fk_column("doc", "docs")
                    .fk_column("team", "teams"),
            ),
    )
}

fn seed_recursive_reachable_fixture(core: &mut NodeState<RocksDbStorage>) {
    for id in 1..=5 {
        accept_global(
            core,
            MergeableCommit::new("teams", row(id), id as u64).cells(BTreeMap::from([(
                "name".to_owned(),
                v(format!("team {id}")),
            )])),
        );
    }
    for (row_id, title) in [
        (0xd1, "one"),
        (0xd2, "two"),
        (0xd3, "three"),
        (0xd4, "four"),
    ] {
        accept_global(
            core,
            MergeableCommit::new("docs", row(row_id), row_id as u64)
                .cells(BTreeMap::from([("title".to_owned(), v(title))])),
        );
    }
    accept_global(core, edge_commit(0xe1, 1, 2, 10));
    accept_global(core, edge_commit(0xe2, 2, 3, 11));
    accept_global(core, edge_commit(0xe3, 4, 5, 12));
    for (row_id, doc_id, team_id) in [
        (0xa1, 0xd1, 1),
        (0xa2, 0xd2, 2),
        (0xa3, 0xd3, 3),
        (0xa4, 0xd4, 5),
    ] {
        accept_global(
            core,
            MergeableCommit::new("teamAccess", row(row_id), row_id as u64).cells(BTreeMap::from([
                ("doc".to_owned(), Value::Uuid(row(doc_id).0)),
                ("team".to_owned(), Value::Uuid(team(team_id))),
            ])),
        );
    }
}

fn edge_commit(row_id: u8, member: u8, parent: u8, time: u64) -> MergeableCommit {
    MergeableCommit::new("teamEdges", row(row_id), time).cells(BTreeMap::from([
        ("member".to_owned(), Value::Uuid(team(member))),
        ("parent".to_owned(), Value::Uuid(team(parent))),
    ]))
}

fn team(id: u8) -> uuid::Uuid {
    uuid::Uuid::from_bytes([id; 16])
}

fn assert_query_engine_maintained_seed_matches_public_rows_and_witnesses(
    core: &mut NodeState<RocksDbStorage>,
    shape: &ValidatedQuery,
    binding: &Binding,
    identity: AuthorSubject,
    expected_witnesses: impl IntoIterator<Item = (TxId, RowUuid, VersionLayer)>,
    expected_replacements: impl IntoIterator<Item = (RowUuid, VersionLayer, bool)>,
) {
    let expected_rows = core
        .query_rows_for_link(shape, binding, DurabilityTier::Global, identity)
        .unwrap();
    let (receiver, maintained, _terminal_schemas, transitions, _tables, _incomplete) = core
        .open_seeded_maintained_subscription_view(
            shape,
            binding,
            identity,
            DurabilityTier::Global,
            &Default::default(),
        )
        .unwrap();
    core.unsubscribe_groove_subscription(receiver.id());

    assert_eq!(
        transitions
            .adds
            .iter()
            .filter_map(crate::protocol::ResultMemberEntry::as_row)
            .map(|(table, row_uuid, _tx_id)| (table.to_string(), row_uuid))
            .collect::<BTreeSet<_>>(),
        expected_rows
            .iter()
            .map(|row| (row.table().to_owned(), row.row_uuid()))
            .collect::<BTreeSet<_>>(),
        "seeded query-engine maintained membership must match public rows"
    );
    assert!(transitions.removes.is_empty());

    for (tx_id, row_uuid, layer) in expected_witnesses {
        assert!(
            maintained
                .versions_by_tx(tx_id)
                .iter()
                .any(|version| version.row_uuid() == row_uuid && version.layer() == layer),
            "seeded query-engine maintained view must include expected {layer:?} witness for {row_uuid:?} in {tx_id:?}"
        );
    }
    for (row_uuid, layer, should_exist) in expected_replacements {
        let (content, deletion) = maintained.replacement_for("todos", row_uuid);
        let actual = match layer {
            VersionLayer::Content => content,
            VersionLayer::Deletion => deletion,
        };
        assert_eq!(
            actual.is_some(),
            should_exist,
            "seeded query-engine maintained replacement witness presence for {row_uuid:?}/{layer:?}"
        );
    }
}

fn assert_maintained_view_cold_snapshot_seed_matches_one_shot(
    core: &mut NodeState<RocksDbStorage>,
    shape: &ValidatedQuery,
    binding: &Binding,
    identity: AuthorSubject,
) {
    let expected_rows = core
        .query_rows_for_link(shape, binding, DurabilityTier::Global, identity)
        .unwrap()
        .into_iter()
        .map(|row| (groove::Intern::new(row.table().to_owned()), row.row_uuid()))
        .collect::<BTreeSet<_>>();
    let mut peer = if identity == AuthorSubject::SYSTEM {
        PeerState::new()
    } else {
        PeerState::client_link(identity)
    };
    let update = peer.rehydrate_query(core, shape, binding).unwrap();
    let (adds, removes) = canonical_view_update_rows(&update);

    assert_eq!(
        adds.into_iter()
            .map(|(table, row_uuid, _tx_id)| (table, row_uuid))
            .collect::<BTreeSet<_>>(),
        expected_rows,
        "maintained subscription cold snapshot should match public query rows"
    );
    assert!(removes.is_empty());
    let metrics = peer.maintained_subscription_view_metrics();
    assert_eq!(metrics.hits_out, 1);
}
