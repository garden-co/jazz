// These tests are intentionally internal: merge-head records are node-local
// derived metadata and are not observable through the public Jazz API. The
// public behavior is convergence; this oracle pins the derived metadata that
// the merge fast path relies on.

fn merge_head_branch_schema() -> JazzSchema {
    build_public_test_schema(
        PublicSchemaBuilder::new().table(
            PublicTableSchemaBuilder::new("todos")
                .column("branch_id", PublicColumnType::Uuid)
                .column("title", PublicColumnType::Text)
                .branch_by("branch_id"),
        ),
    )
}

#[test]
fn immediate_branch_commit_unit_matches_durable_replay() {
    // The wire envelope is the boundary under test: immediate publication must
    // carry exactly the same generated intent as replay after a process restart.
    let schema = merge_head_branch_schema();
    let (dir, mut writer) = open_node_with_schema(node(0xb3), schema.clone());
    let (tx_id, immediate) = writer
        .commit_mergeable_unit_settled(
            MergeableCommit::new("todos", row(0xbc), 10)
                .branch(branch_selector(0xa3))
                .made_by(user(0xb4))
                .cells(BTreeMap::from([("title".to_owned(), v("retained metadata"))])),
        )
        .unwrap();
    let SyncMessage::CommitUnit { tx, .. } = &immediate else {
        panic!("expected commit unit");
    };
    assert_eq!(tx.made_by, user(0xb4));
    assert_eq!(
        tx.contribution_merge.as_ref()
            .expect("immediate publication lost its generated branch-write intent")
            .branch_write_intents.len(),
        1,
        "immediate publication must retain generated branch-write intent"
    );
    drop(writer);
    let mut reopened = reopen_node_at(&dir, node(0xb3), schema);
    assert_eq!(immediate, reopened.commit_unit_for(tx_id).unwrap());
}

#[test]
fn concurrent_branch_inserts_merge_without_bypassing_read_or_update_policy() {
    for (can_read, can_update) in [(true, true), (false, true), (true, false)] {
        let policy = |allowed| if allowed { PublicPolicyExpr::True } else { PublicPolicyExpr::False };
        let schema = build_public_test_schema(
            PublicSchemaBuilder::new().table(
                PublicTableSchemaBuilder::new("todos")
                    .column("branch_id", PublicColumnType::Uuid)
                    .column("title", PublicColumnType::Text)
                    .branch_by("branch_id")
                    .policies(public_all_policies()
                        .with_select(policy(can_read))
                        .with_update(Some(policy(can_update)), PublicPolicyExpr::True)),
            ),
        );
        let (_left_dir, mut left) = open_node_with_schema(node(0xb5), schema.clone());
        let (_right_dir, mut right) = open_node_with_schema(node(0xb6), schema.clone());
        let (_core_dir, mut core) = open_history_complete_node_with_schema(node(0xb7), schema);
        let shared_row = row(0xbd);
        let commit = |title| MergeableCommit::new("todos", shared_row, 10)
            .branch(branch_selector(0xa4))
            .made_by(user(0xb8))
            .cells(BTreeMap::from([("title".to_owned(), v(title))]));
        let (left_tx, left_unit) = left.commit_mergeable_unit_settled(commit("left")).unwrap();
        let (right_tx, right_unit) = right.commit_mergeable_unit_settled(commit("right")).unwrap();
        let first = core.apply_sync_message_settled(left_unit).unwrap();
        assert!(first.iter().any(|receipt| matches!(receipt,
            SyncMessage::FateUpdate { tx_id, fate: Fate::Accepted, .. } if *tx_id == left_tx
        )), "first insert must be admitted without requiring prior read access: {first:?}");
        let expected = if can_read && can_update {
            Fate::Accepted
        } else {
            Fate::Rejected(RejectionReason::AuthorizationDenied)
        };
        let second = core.apply_sync_message_settled(right_unit).unwrap();
        assert!(second.iter().any(|receipt| matches!(receipt,
            SyncMessage::FateUpdate { tx_id, fate, .. } if *tx_id == right_tx && *fate == expected
        )), "read={can_read}, update={can_update}: {second:?}");
        if expected == Fate::Accepted {
            let frontier = core.database.primary_key_scan_raw("jazz_merge_heads", &[]).unwrap();
            assert_eq!(frontier.len(), 1);
            let heads = merge_heads_from_value(frontier[0].record().get_idx(3).unwrap()).unwrap();
            assert_eq!(heads.len(), 1, "both admitted inserts must produce one merge head");
            let merge_tx = *heads.iter().next().unwrap();
            assert_ne!(merge_tx, left_tx);
            assert_ne!(merge_tx, right_tx);
            let SyncMessage::CommitUnit { versions, .. } = core.commit_unit_for(merge_tx).unwrap() else {
                panic!("expected merge commit unit");
            };
            assert_eq!(versions.len(), 1);
            assert_eq!(versions[0].parents(), &[left_tx, right_tx]);
        } else {
            assert!(core.query_versions_for_tx(right_tx).unwrap().is_empty());
        }
    }
}

#[test]
fn merge_heads_match_history_for_first_and_subsequent_authored_versions() {
    let schema = two_column_schema();
    let (_core_dir, mut core) = open_node_with_schema(node(0xa0), schema);
    let row = row(0xa0);

    let first = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row, 10)
                .cells(BTreeMap::from([("title".to_owned(), "first".to_owned())])),
        )
        .unwrap();
    core.assert_merge_heads_match_history_for_test("todos", row)
        .unwrap();

    core.commit_mergeable_settled(
        MergeableCommit::new("todos", row, 11)
            .parents(vec![first])
            .cells(BTreeMap::from([("body".to_owned(), "second".to_owned())])),
    )
    .unwrap();
    core.assert_merge_heads_match_history_for_test("todos", row)
        .unwrap();
}

#[test]
fn merge_heads_match_history_for_ordinary_concurrent_units() {
    let schema = two_column_schema();
    let (_writer_a_dir, mut writer_a) = open_node_with_schema(node(0xa1), schema.clone());
    let (_writer_b_dir, mut writer_b) = open_node_with_schema(node(0xa2), schema.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(0xa9), schema);
    let row = row(0xaa);

    let (_left, left_unit) = writer_a
        .commit_mergeable_unit_settled(MergeableCommit::new("todos", row, 10).cells(BTreeMap::from([(
            "title".to_owned(),
            "left".to_owned(),
        )])))
        .unwrap();
    let (_right, right_unit) = writer_b
        .commit_mergeable_unit_settled(MergeableCommit::new("todos", row, 11).cells(BTreeMap::from([(
            "body".to_owned(),
            "right".to_owned(),
        )])))
        .unwrap();

    core.apply_sync_message_settled(right_unit).unwrap();
    core.assert_merge_heads_match_history_for_test("todos", row)
        .unwrap();
    core.apply_sync_message_settled(left_unit).unwrap();
    core.assert_merge_heads_match_history_for_test("todos", row)
        .unwrap();
}

#[test]
fn merge_heads_match_history_for_edge_accepted_units() {
    let schema = two_column_schema();
    let (_writer_a_dir, mut writer_a) = open_node_with_schema(node(0xe1), schema.clone());
    let (_writer_b_dir, mut writer_b) = open_node_with_schema(node(0xe2), schema.clone());
    let (_edge_dir, mut edge) = open_node_with_schema(node(0xe9), schema);
    let row = row(0xea);

    let (_left, left_unit) = writer_a
        .commit_mergeable_unit_settled(MergeableCommit::new("todos", row, 10).cells(BTreeMap::from([(
            "title".to_owned(),
            "left".to_owned(),
        )])))
        .unwrap();
    let (_right, right_unit) = writer_b
        .commit_mergeable_unit_settled(MergeableCommit::new("todos", row, 11).cells(BTreeMap::from([(
            "body".to_owned(),
            "right".to_owned(),
        )])))
        .unwrap();
    let SyncMessage::CommitUnit {
        tx: left_tx,
        versions: left_versions,
    } = left_unit
    else {
        panic!("expected commit unit");
    };
    let SyncMessage::CommitUnit {
        tx: right_tx,
        versions: right_versions,
    } = right_unit
    else {
        panic!("expected commit unit");
    };

    let outcome = crate::db::block_on(edge.ingest_edge_authority_mergeable_commit_unit(
        right_tx,
        right_versions,
        u64::MAX - SKEW_TOLERANCE_MS,
    ))
    .unwrap();
    settle_outcome(&mut edge, outcome).unwrap();
    edge.assert_merge_heads_match_history_for_test("todos", row)
        .unwrap();
    let outcome = crate::db::block_on(edge.ingest_edge_authority_mergeable_commit_unit(
        left_tx,
        left_versions,
        u64::MAX - SKEW_TOLERANCE_MS,
    ))
    .unwrap();
    settle_outcome(&mut edge, outcome).unwrap();
    edge.assert_merge_heads_match_history_for_test("todos", row)
        .unwrap();
}

#[test]
fn edge_creates_edge_durable_merge_for_concurrent_inserts_and_edits() {
    for edit_existing in [false, true] {
        let schema = two_column_schema();
        let (_left_dir, mut left) = open_node_with_schema(node(0xc1), schema.clone());
        let (_right_dir, mut right) = open_node_with_schema(node(0xc2), schema.clone());
        let (edge_dir, mut edge) = open_node_with_schema(node(0xc9), schema.clone());
        let (_core_dir, mut core) = open_history_complete_node_with_schema(node(0xcf), schema.clone());
        let shared_row = row(0xca);
        let admit = |edge: &mut NodeState<RocksDbStorage>, unit| {
            let SyncMessage::CommitUnit { tx, versions } = unit else {
                panic!("expected commit unit");
            };
            let tx_id = tx.tx_id;
            let outcome = crate::db::block_on(edge.ingest_edge_authority_mergeable_commit_unit(
                tx, versions, u64::MAX - SKEW_TOLERANCE_MS,
            )).unwrap();
            let receipts = settle_outcome(edge, outcome).unwrap();
            assert!(receipts.iter().any(|receipt| matches!(receipt,
                SyncMessage::FateUpdate {
                    tx_id: accepted_tx, fate: Fate::Accepted,
                    global_time: None, durability: Some(DurabilityTier::Edge),
                } if *accepted_tx == tx_id
            )), "edge admission must remain Edge-only: {receipts:?}");
        };
        let parents = if edit_existing {
            let (seed_tx, seed) = left.commit_mergeable_unit_settled(
                MergeableCommit::new("todos", shared_row, 10)
                    .cells(BTreeMap::from([("title".to_owned(), v("seed")), ("body".to_owned(), v("seed"))])),
            ).unwrap();
            right.apply_sync_message_settled(seed.clone()).unwrap();
            admit(&mut edge, seed);
            vec![seed_tx]
        } else { Vec::new() };
        let (left_tx, left_unit) = left.commit_mergeable_unit_settled(
            MergeableCommit::new("todos", shared_row, 20).parents(parents.clone())
                .cells(BTreeMap::from([("title".to_owned(), v("left"))])),
        ).unwrap();
        let (right_tx, right_unit) = right.commit_mergeable_unit_settled(
            MergeableCommit::new("todos", shared_row, 20).parents(parents)
                .cells(BTreeMap::from([("body".to_owned(), v("right"))])),
        ).unwrap();
        admit(&mut edge, left_unit.clone());
        admit(&mut edge, right_unit.clone());

        let frontier = edge.database.primary_key_scan_raw("jazz_merge_heads", &[]).unwrap();
        assert_eq!(frontier.len(), 1);
        let heads = merge_heads_from_value(frontier[0].record().get_idx(3).unwrap()).unwrap();
        assert_eq!(heads.len(), 1, "edge must merge both heads before forwarding (edit={edit_existing})");
        let merge_tx = *heads.iter().next().unwrap();
        assert_eq!(merge_tx.node, node(0xc9));
        assert_eq!(edge.transaction_state_settled(merge_tx), Some((Fate::Accepted, None, DurabilityTier::Edge)));
        let SyncMessage::CommitUnit { versions, .. } = edge.commit_unit_for(merge_tx).unwrap() else {
            panic!("expected merge unit");
        };
        assert_eq!(versions.len(), 1);
        assert_eq!(versions[0].parents(), &[left_tx, right_tx]);
        admit(&mut edge, left_unit);
        admit(&mut edge, right_unit);
        let replayed = edge.database.primary_key_scan_raw("jazz_merge_heads", &[]).unwrap();
        assert_eq!(merge_heads_from_value(replayed[0].record().get_idx(3).unwrap()).unwrap(), heads);

        let publication = edge.edge_authority_publication_for(right_tx).unwrap();
        drop(edge);
        let mut edge = reopen_node_at(&edge_dir, node(0xc9), schema);
        assert_eq!(edge.edge_authority_publication_for(right_tx).unwrap(), publication,
            "publication reconstruction must survive an edge restart");
        for _ in 0..2 {
            let outcome = core.ingest_edge_authority_publication(publication.clone(), 100).unwrap();
            assert!(outcome.publications.is_empty(),
                "a reconciled edge publication must not generate any new core merge publication");
            settle_outcome(&mut core, outcome).unwrap();
            let frontier = core.database.primary_key_scan_raw("jazz_merge_heads", &[]).unwrap();
            assert_eq!(frontier.len(), 1);
            assert_eq!(merge_heads_from_value(frontier[0].record().get_idx(3).unwrap()).unwrap(), heads,
                "core must retain the edge's merge, not create a redundant same-frontier merge");
            assert!(matches!(core.transaction_state_settled(merge_tx),
                Some((Fate::Accepted, Some(_), DurabilityTier::Global))));
        }
    }
}

#[test]
fn core_merges_only_residual_heads_from_distinct_edge_publications() {
    // Internal admission APIs expose merge authorship and parent identities,
    // which convergence alone cannot distinguish from redundant core merges.
    for edit_existing in [false, true] {
        let schema = two_column_schema();
        let (_left_dir, mut left) = open_node_with_schema(node(0xd1), schema.clone());
        let (_right_dir, mut right) = open_node_with_schema(node(0xd2), schema.clone());
        let (_edge_a_dir, mut edge_a) = open_node_with_schema(node(0xd3), schema.clone());
        let (_edge_b_dir, mut edge_b) = open_node_with_schema(node(0xd4), schema.clone());
        let (core_dir, mut core) = open_history_complete_node_with_schema(node(0xdf), schema.clone());
        let shared_row = row(0xda);
        let admit = |edge: &mut NodeState<RocksDbStorage>, unit| {
            let SyncMessage::CommitUnit { tx, versions } = unit else {
                panic!("expected commit unit");
            };
            let tx_id = tx.tx_id;
            let outcome = edge.ingest_edge_authority_mergeable_commit_unit(
                tx, versions, u64::MAX - SKEW_TOLERANCE_MS,
            ).unwrap();
            let receipts = settle_outcome(edge, outcome).unwrap();
            assert!(receipts.iter().any(|receipt| matches!(receipt,
                SyncMessage::FateUpdate { tx_id: accepted, fate: Fate::Accepted,
                    durability: Some(DurabilityTier::Edge), global_time: None }
                    if *accepted == tx_id
            )));
        };
        let parents = if edit_existing {
            let (seed_tx, seed) = left.commit_mergeable_unit_settled(
                MergeableCommit::new("todos", shared_row, 10).cells(BTreeMap::from([
                    ("title".to_owned(), v("seed")), ("body".to_owned(), v("seed")),
                ])),
            ).unwrap();
            right.apply_sync_message_settled(seed.clone()).unwrap();
            admit(&mut edge_a, seed.clone());
            admit(&mut edge_b, seed);
            vec![seed_tx]
        } else { Vec::new() };
        let (left_tx, left_unit) = left.commit_mergeable_unit_settled(
            MergeableCommit::new("todos", shared_row, 20).parents(parents.clone())
                .cells(BTreeMap::from([("title".to_owned(), v("left"))])),
        ).unwrap();
        let (right_tx, right_unit) = right.commit_mergeable_unit_settled(
            MergeableCommit::new("todos", shared_row, 20).parents(parents)
                .cells(BTreeMap::from([("body".to_owned(), v("right"))])),
        ).unwrap();
        admit(&mut edge_a, left_unit);
        admit(&mut edge_b, right_unit);
        let first = edge_a.edge_authority_publication_for(left_tx).unwrap();
        let second = edge_b.edge_authority_publication_for(right_tx).unwrap();
        let outcome = core.ingest_edge_authority_publication(first.clone(), 100).unwrap();
        settle_outcome(&mut core, outcome).unwrap();
        let frontier = core.database.primary_key_scan_raw("jazz_merge_heads", &[]).unwrap();
        assert_eq!(frontier.len(), 1);
        assert_eq!(merge_heads_from_value(frontier[0].record().get_idx(3).unwrap()).unwrap(),
            BTreeSet::from([left_tx]));
        drop(core);
        let mut core = reopen_node_at(&core_dir, node(0xdf), schema);
        let outcome = core.ingest_edge_authority_publication(second.clone(), 101).unwrap();
        settle_outcome(&mut core, outcome).unwrap();
        let frontier = core.database.primary_key_scan_raw("jazz_merge_heads", &[]).unwrap();
        assert_eq!(frontier.len(), 1);
        let heads = merge_heads_from_value(frontier[0].record().get_idx(3).unwrap()).unwrap();
        assert_eq!(heads.len(), 1);
        let merge_tx = *heads.iter().next().unwrap();
        assert_eq!(merge_tx.node, node(0xdf), "only residual cross-edge concurrency requires a core merge");
        let SyncMessage::CommitUnit { versions, .. } = core.commit_unit_for(merge_tx).unwrap() else {
            panic!("expected merge unit");
        };
        assert_eq!(versions.len(), 1);
        assert_eq!(versions[0].parents(), &[left_tx, right_tx]);
        for publication in [second, first] {
            let outcome = core.ingest_edge_authority_publication(publication, 102).unwrap();
            settle_outcome(&mut core, outcome).unwrap();
            let frontier = core.database.primary_key_scan_raw("jazz_merge_heads", &[]).unwrap();
            assert_eq!(frontier.len(), 1);
            assert_eq!(merge_heads_from_value(frontier[0].record().get_idx(3).unwrap()).unwrap(), heads);
        }
    }
}

#[test]
fn merge_heads_match_history_for_relay_pending_then_edge_fate() {
    let schema = two_column_schema();
    let (_writer_a_dir, mut writer_a) = open_node_with_schema(node(0xf1), schema.clone());
    let (_writer_b_dir, mut writer_b) = open_node_with_schema(node(0xf2), schema.clone());
    let (_edge_dir, mut edge) = open_node_with_schema(node(0xf9), schema);
    let row = row(0xfa);

    let (left, left_unit) = writer_a
        .commit_mergeable_unit_settled(MergeableCommit::new("todos", row, 10).cells(BTreeMap::from([(
            "title".to_owned(),
            "left".to_owned(),
        )])))
        .unwrap();
    let (right, right_unit) = writer_b
        .commit_mergeable_unit_settled(MergeableCommit::new("todos", row, 11).cells(BTreeMap::from([(
            "body".to_owned(),
            "right".to_owned(),
        )])))
        .unwrap();
    let SyncMessage::CommitUnit {
        tx: left_tx,
        versions: left_versions,
    } = left_unit
    else {
        panic!("expected commit unit");
    };
    let SyncMessage::CommitUnit {
        tx: right_tx,
        versions: right_versions,
    } = right_unit
    else {
        panic!("expected commit unit");
    };

    edge.ingest_relay_commit_unit(right_tx, right_versions)
        .unwrap();
    edge.apply_fate_update(right, Fate::Accepted, None, Some(DurabilityTier::Edge))
        .unwrap();
    edge.assert_merge_heads_match_history_for_test("todos", row)
        .unwrap();
    edge.ingest_relay_commit_unit(left_tx, left_versions)
        .unwrap();
    edge.apply_fate_update(left, Fate::Accepted, None, Some(DurabilityTier::Edge))
        .unwrap();
    edge.assert_merge_heads_match_history_for_test("todos", row)
        .unwrap();
}

#[test]
fn accepting_pending_history_does_not_rewalk_the_merge_chain() {
    // A relay installs pending versions into current/merge-head state. Their
    // later accepted fates do not alter head membership, even when transport
    // delivers the fates newest-first. Rewalking the chain here made a 500
    // revision subscription starve unrelated query tests.
    let schema = two_column_schema();
    let (_edge_dir, mut edge) = open_node_with_schema(node(0xfb), schema);
    let row = row(0xfb);
    let mut versions = Vec::new();
    let mut parent = None;

    for _ in 0..32 {
        let mut commit = MergeableCommit::new("todos", row, 10)
            .cells(BTreeMap::from([("title".to_owned(), "revision".to_owned())]));
        if let Some(parent) = parent {
            commit = commit.parents(vec![parent]);
        }
        let published = edge.commit_mergeable(commit).unwrap();
        let tx_id = settle_published(&mut edge, published).unwrap();
        parent = Some(tx_id);
        versions.push(tx_id);
    }

    edge.reset_merge_head_reachability_walks_for_test();
    for tx_id in versions.into_iter().rev() {
        edge.apply_fate_update(tx_id, Fate::Accepted, None, Some(DurabilityTier::Edge))
            .unwrap();
    }

    assert_eq!(
        edge.merge_head_reachability_walks_for_test(),
        0,
        "accepting a pending chain must not replay historical reachability"
    );
    edge.assert_merge_heads_match_history_for_test("todos", row)
        .unwrap();
}

#[test]
fn merge_heads_match_history_after_parked_unit_resolves() {
    let schema = two_column_schema();
    let (_parent_dir, mut parent_writer) = open_node_with_schema(node(0xb1), schema.clone());
    let (_child_dir, mut child_writer) = open_node_with_schema(node(0xb2), schema.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(0xb9), schema);
    let row = row(0xba);

    let (parent_tx, parent_unit) = parent_writer
        .commit_mergeable_unit_settled(MergeableCommit::new("todos", row, 10).cells(BTreeMap::from([(
            "title".to_owned(),
            "parent".to_owned(),
        )])))
        .unwrap();
    let (_child_tx, child_unit) = child_writer
        .commit_mergeable_unit_settled(
            MergeableCommit::new("todos", row, 11)
                .parents(vec![parent_tx])
                .cells(BTreeMap::from([("body".to_owned(), "child".to_owned())])),
        )
        .unwrap();

    core.apply_sync_message_settled(child_unit).unwrap();
    core.apply_sync_message_settled(parent_unit).unwrap();
    core.assert_merge_heads_match_history_for_test("todos", row)
        .unwrap();
}

/// A persisted content frontier is a canonical Groove TxId array, retains its
/// physical table/branch/row coordinate across restart, and cannot duplicate a
/// replayed head. Alice and Bob author concurrent same-time versions, which
/// then produce one higher-time merge head at the core.
#[test]
fn merge_heads_match_history_across_restart_between_concurrent_units() {
    let schema = two_column_schema();
    let (_writer_a_dir, mut writer_a) = open_node_with_schema(node(0xc1), schema.clone());
    let (_writer_b_dir, mut writer_b) = open_node_with_schema(node(0xc2), schema.clone());
    let (core_dir, mut core) = open_node_with_schema(node(0xc9), schema.clone());
    let row = row(0xca);

    let (left, left_unit) = writer_a
        .commit_mergeable_unit_settled(MergeableCommit::new("todos", row, 10).cells(BTreeMap::from([(
            "title".to_owned(),
            "left".to_owned(),
        )])))
        .unwrap();
    let (right, right_unit) = writer_b
        .commit_mergeable_unit_settled(MergeableCommit::new("todos", row, 10).cells(BTreeMap::from([(
            "body".to_owned(),
            "right".to_owned(),
        )])))
        .unwrap();

    core.apply_sync_message_settled(left_unit.clone()).unwrap();
    core.apply_sync_message_settled(left_unit).unwrap();
    let stored = core
        .database
        .primary_key_scan_raw("jazz_merge_heads", &[])
        .unwrap();
    assert_eq!(stored.len(), 1);
    assert_eq!(
        stored[0].record().get_idx(3).unwrap(),
        Value::Array(vec![tx_id_value(left)]),
        "a replay must not duplicate the persisted frontier head"
    );
    drop(core);
    let mut core = reopen_node_at(&core_dir, node(0xc9), schema);
    let stored = core
        .database
        .primary_key_scan_raw("jazz_merge_heads", &[])
        .unwrap();
    assert_eq!(stored.len(), 1);
    assert_eq!(
        stored[0].record().get_idx(3).unwrap(),
        Value::Array(vec![tx_id_value(left)]),
        "reopen must retain the normal Groove array rather than opaque bytes"
    );
    core.assert_merge_heads_match_history_for_test("todos", row)
        .unwrap();
    core.apply_sync_message_settled(right_unit).unwrap();
    let stored = core
        .database
        .primary_key_scan_raw("jazz_merge_heads", &[])
        .unwrap();
    assert_eq!(stored.len(), 1);
    let heads = merge_heads_from_value(stored[0].record().get_idx(3).unwrap()).unwrap();
    assert_eq!(heads.len(), 1, "the core replaces concurrent heads with its merge");
    assert!(
        heads.into_iter().next().unwrap() > right,
        "the merged head must be later than both same-time input heads"
    );
    core.assert_merge_heads_match_history_for_test("todos", row)
        .unwrap();
}

/// Two non-default branches may use the same physical table and row UUID while
/// retaining independent concurrent frontiers across reopen.
///
/// ```text
/// alice + bob ── concurrent writes ──► branch A ──► merge head A
/// alice + bob ── concurrent writes ──► branch B ──► merge head B
/// ```
#[test]
fn merge_heads_key_two_nondefault_branches_independently_across_reopen() {
    let schema = merge_head_branch_schema();
    let (_alice_dir, mut alice) = open_node_with_schema(node(0xb1), schema.clone());
    let (_bob_dir, mut bob) = open_node_with_schema(node(0xb2), schema.clone());
    let (core_dir, mut core) = open_node_with_schema(node(0xb9), schema.clone());
    let row_uuid = row(0xba);
    let branch_a = branch_selector(0xa1);
    let branch_b = branch_selector(0xa2);
    let table = &schema.tables[0];
    let branch_key_a = schema
        .project_branch_selector(table, &branch_a)
        .unwrap()
        .0;
    let branch_key_b = schema
        .project_branch_selector(table, &branch_b)
        .unwrap()
        .0;
    assert_ne!(branch_key_a, BranchKey::default());
    assert_ne!(branch_key_b, BranchKey::default());

    for (branch, label) in [(branch_a, "a"), (branch_b, "b")] {
        let (_, alice_unit) = alice
            .commit_mergeable_unit_settled(
                MergeableCommit::new("todos", row_uuid, 10)
                    .branch(branch.clone())
                    .cells(BTreeMap::from([("title".to_owned(), v(format!("alice-{label}")))])),
            )
            .unwrap();
        let (_, bob_unit) = bob
            .commit_mergeable_unit_settled(
                MergeableCommit::new("todos", row_uuid, 10)
                    .branch(branch)
                    .cells(BTreeMap::from([("title".to_owned(), v(format!("bob-{label}")))])),
            )
            .unwrap();
        for unit in [alice_unit, bob_unit] {
            let SyncMessage::CommitUnit { tx, .. } = &unit else {
                panic!("expected commit unit");
            };
            let tx_id = tx.tx_id;
            let receipts = core.apply_sync_message_settled(unit).unwrap();
            assert!(receipts.iter().any(|receipt| matches!(
                receipt,
                SyncMessage::FateUpdate { tx_id: receipt_tx, fate: Fate::Accepted, .. }
                    if *receipt_tx == tx_id
            )), "both concurrent branch writes must be admitted: {receipts:?}");
        }
    }

    let table_id = core.catalogue.physical_mappings[&schema.version_id()].tables["todos"].table_id;
    let stored = core
        .database
        .primary_key_scan_raw("jazz_merge_heads", &[])
        .unwrap();
    assert_eq!(stored.len(), 2, "each branch needs its own frontier row");
    let before_reopen = stored
        .into_iter()
        .map(|row| {
            assert_eq!(row.record().get_u64(0).unwrap(), table_id.0);
            let branch = row.record().get_bytes(1).unwrap().to_vec();
            let heads = merge_heads_from_value(row.record().get_idx(3).unwrap()).unwrap();
            assert_eq!(heads.len(), 1, "each concurrent pair becomes one merge head");
            (branch, heads)
        })
        .collect::<BTreeMap<_, _>>();
    assert_eq!(
        before_reopen.keys().cloned().collect::<BTreeSet<_>>(),
        BTreeSet::from([
            branch_key_a.canonical_bytes(),
            branch_key_b.canonical_bytes(),
        ])
    );

    drop(core);
    let core = reopen_node_at(&core_dir, node(0xb9), schema);
    let after_reopen = core
        .database
        .primary_key_scan_raw("jazz_merge_heads", &[])
        .unwrap()
        .into_iter()
        .map(|row| {
            (
                row.record().get_bytes(1).unwrap().to_vec(),
                merge_heads_from_value(row.record().get_idx(3).unwrap()).unwrap(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    assert_eq!(after_reopen, before_reopen);
}

#[test]
fn merge_heads_share_physical_identity_across_table_rename_and_restart() {
    // Merge-head rows are node-local derived metadata, so the physical-key
    // assertion is intentionally internal. The history oracle verifies that
    // the shared row keeps the merge behavior correct across the rename.
    let base = schema();
    let renamed = SchemaVersion::new(renamed_tasks_schema());
    let (dir, mut core) = open_node_with_schema(node(0xcb), base.clone());
    let row_uuid = row(0xcb);
    let before = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row_uuid, 10)
                .cells(BTreeMap::from([("title".to_owned(), v("before"))])),
        )
        .unwrap();

    publish_schema_lineage(
        &mut core,
        renamed.clone(),
        MigrationLens::new(
            base.version_id(),
            renamed.id,
            vec![TableLens {
                source_table: "todos".to_owned(),
                target_table: "tasks".to_owned(),
                ops: vec![
                    LensOp::RenameTable {
                        from: "todos".to_owned(),
                        to: "tasks".to_owned(),
                    },
                    LensOp::RenameColumn {
                        from: "title".to_owned(),
                        to: "name".to_owned(),
                    },
                ],
            }],
        ).expect("valid migration lens"),
        Vec::<String>::new(),
        Vec::<String>::new(),
    )
    .unwrap();
    core.apply_trusted_catalogue_message_settled(SyncMessage::SetCurrentWriteSchema {
        author: AuthorSubject::SYSTEM,
        pointer: CurrentWriteSchema {
            revision: 1,
            schema: renamed.id,
        },
    })
    .unwrap();
    core.commit_mergeable_settled(
        MergeableCommit::new("tasks", row_uuid, 11)
            .parents(vec![before])
            .cells(BTreeMap::from([("name".to_owned(), v("after"))])),
    )
    .unwrap();

    let table_id = core.catalogue.physical_mappings[&renamed.id].tables["tasks"].table_id;
    core.assert_merge_heads_match_history_for_test("tasks", row_uuid)
        .unwrap();
    let stored = core
        .database
        .primary_key_scan_raw("jazz_merge_heads", &[])
        .unwrap();
    assert_eq!(stored.len(), 1);
    assert_eq!(stored[0].record().get_u64(0).unwrap(), table_id.0);

    drop(core);
    let mut reopened = reopen_node_at(&dir, node(0xcb), base);
    reopened
        .assert_merge_heads_match_history_for_test("tasks", row_uuid)
        .unwrap();
    let stored = reopened
        .database
        .primary_key_scan_raw("jazz_merge_heads", &[])
        .unwrap();
    assert_eq!(stored.len(), 1);
    assert_eq!(stored[0].record().get_u64(0).unwrap(), table_id.0);
}

#[test]
fn merge_heads_match_history_after_merge_version_application() {
    let schema = two_column_schema();
    let (_writer_a_dir, mut writer_a) = open_node_with_schema(node(0xd1), schema.clone());
    let (_writer_b_dir, mut writer_b) = open_node_with_schema(node(0xd2), schema.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(0xd9), schema);
    let row = row(0xda);

    let (_left, left_unit) = writer_a
        .commit_mergeable_unit_settled(MergeableCommit::new("todos", row, 10).cells(BTreeMap::from([(
            "title".to_owned(),
            "left".to_owned(),
        )])))
        .unwrap();
    let (_right, right_unit) = writer_b
        .commit_mergeable_unit_settled(MergeableCommit::new("todos", row, 11).cells(BTreeMap::from([(
            "body".to_owned(),
            "right".to_owned(),
        )])))
        .unwrap();

    core.apply_sync_message_settled(left_unit).unwrap();
    core.apply_sync_message_settled(right_unit).unwrap();
    let _ = core.view_update_for_current_rows("todos").unwrap();
    core.assert_merge_heads_match_history_for_test("todos", row)
        .unwrap();
}
