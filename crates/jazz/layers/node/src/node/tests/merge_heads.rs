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
fn merge_heads_match_history_for_relay_pending_then_global_fate() {
    let schema = two_column_schema();
    let (_writer_a_dir, mut writer_a) = open_node_with_schema(node(0xf1), schema.clone());
    let (_writer_b_dir, mut writer_b) = open_node_with_schema(node(0xf2), schema.clone());
    let (_relay_dir, mut relay) = open_node_with_schema(node(0xf9), schema);
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

    relay.ingest_relay_commit_unit(right_tx, right_versions)
        .unwrap();
    relay.accept_global_for_test(right)
        .unwrap();
    relay.assert_merge_heads_match_history_for_test("todos", row)
        .unwrap();
    relay.ingest_relay_commit_unit(left_tx, left_versions)
        .unwrap();
    relay.accept_global_for_test(left)
        .unwrap();
    relay.assert_merge_heads_match_history_for_test("todos", row)
        .unwrap();
}

#[test]
fn accepting_pending_history_does_not_rewalk_the_merge_chain() {
    // A relay installs pending versions into current/merge-head state. Their
    // later accepted fates do not alter head membership, even when transport
    // delivers the fates newest-first. Rewalking the chain here made a 500
    // revision subscription starve unrelated query tests.
    let schema = two_column_schema();
    let (_relay_dir, mut relay) = open_node_with_schema(node(0xfb), schema);
    let row = row(0xfb);
    let mut versions = Vec::new();
    let mut parent = None;

    for _ in 0..32 {
        let mut commit = MergeableCommit::new("todos", row, 10)
            .cells(BTreeMap::from([("title".to_owned(), "revision".to_owned())]));
        if let Some(parent) = parent {
            commit = commit.parents(vec![parent]);
        }
        let published = relay.commit_mergeable(commit).unwrap();
        let tx_id = settle_published(&mut relay, published).unwrap();
        parent = Some(tx_id);
        versions.push(tx_id);
    }

    relay.reset_merge_head_reachability_walks_for_test();
    for tx_id in versions.into_iter().rev() {
        relay.accept_global_for_test(tx_id)
            .unwrap();
    }

    assert_eq!(
        relay.merge_head_reachability_walks_for_test(),
        0,
        "accepting a pending chain must not replay historical reachability"
    );
    relay.assert_merge_heads_match_history_for_test("todos", row)
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
    core.activate_catalogue_schema_settled(CurrentWriteSchema {
        revision: 1,
        schema: renamed.id,
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

// Internal because avoiding transaction-wide materialization is not observable
// through query results; the reachability answers pin the semantic boundary too.
#[test]
fn ancestry_lookup_avoids_transaction_wide_reads_resident_and_cold() {
    let schema = two_column_schema();
    let (dir, mut writer) = open_node_with_schema(node(0xe1), schema.clone());
    let row_uuid = row(0xe2);
    let (parent, _) = writer.commit_mergeable_unit_settled(
        MergeableCommit::new("todos", row_uuid, 10)
            .cells(BTreeMap::from([("title".to_owned(), "first".to_owned())])),
    ).unwrap();
    let (child, _) = writer.commit_mergeable_unit_settled(
        MergeableCommit::new("todos", row_uuid, 11).parents(vec![parent])
            .cells(BTreeMap::from([("title".to_owned(), "second".to_owned())])),
    ).unwrap();
    let table_id = writer.physical_table_id_for_schema(
        writer.catalogue.active_schema.schema, "todos",
    ).unwrap();
    // Force the resident branch, then repeat against cold persisted history.
    let versions = writer.query_versions_for_tx(child).unwrap();
    writer.cache_tx_versions(child, versions);
    for cold in [false, true] {
        if cold {
            drop(writer);
            writer = reopen_node_at(&dir, node(0xe1), schema.clone());
            writer.query.tx_versions_cache.clear();
        }
        reset_query_versions_for_tx_call_count();
        assert!(writer.content_version_reaches_tx(
            table_id, &BranchKey::default(), row_uuid, child, parent,
        ).unwrap());
        assert!(!writer.content_version_reaches_tx(
            table_id, &BranchKey::default(), row(0xe3), child, parent,
        ).unwrap());
        assert!(!writer.content_version_reaches_tx(
            table_id, &BranchKey::default(), row_uuid, parent, child,
        ).unwrap());
        assert_eq!(query_versions_for_tx_call_count(), 0,
            "row ancestry must never materialize a whole transaction (cold={cold})");
    }
}

#[test]
fn positive_row_reachability_stops_at_immediate_predecessor() {
    let schema = two_column_schema();
    let (_dir, mut writer) = open_node_with_schema(node(0xe4), schema);
    let row_uuid = row(0xe5);
    let chain_len = 96;
    let mut versions = Vec::with_capacity(chain_len);
    let mut parent = None;
    for index in 0..chain_len {
        let mut commit = MergeableCommit::new("todos", row_uuid, 10 + index as u64)
            .cells(BTreeMap::from([("title".to_owned(), "revision".to_owned())]));
        if let Some(parent) = parent {
            commit = commit.parents(vec![parent]);
        }
        let tx_id = writer.commit_mergeable_unit_settled(commit).unwrap().0;
        versions.push(tx_id);
        parent = Some(tx_id);
    }
    let head = *versions.last().expect("history chain has a head");
    let immediate_predecessor = versions[chain_len - 2];
    let table_id = writer
        .physical_table_id_for_schema(writer.catalogue.active_schema.schema, "todos")
        .unwrap();

    // TESTING_GUIDELINES permits this internal counter seam because bounded
    // ancestry work is not observable through public rows; the reachability
    // result still asserts the semantic contract at the same private seam.
    writer.clear_content_version_reachability_cache();
    writer.reset_merge_head_reachability_walks_for_test();
    assert!(writer
        .content_version_reaches_tx(
            table_id,
            &BranchKey::default(),
            row_uuid,
            head,
            immediate_predecessor,
        )
        .unwrap());
    assert!(
        writer.merge_head_reachability_nodes_for_test() <= 2,
        "a positive predecessor witness must stop the walk instead of scanning {chain_len} nodes"
    );
}

// Internal work-count receipt: transaction fate handling may read the full
// unit once; exact row matching must not add another transaction-wide read.
#[test]
fn known_transaction_matching_probes_only_incoming_history_keys() {
    let schema = two_column_schema();
    let (_dir, mut writer) = open_node_with_schema(node(0xf1), schema);
    let tx_id = writer.commit_mergeable_many_settled((0..32).map(|i| {
        MergeableCommit::new("todos", row(i + 1), 10)
            .cells(BTreeMap::from([("title".to_owned(), "same".to_owned())]))
    }).collect()).unwrap();
    let SyncMessage::CommitUnit { tx, versions } = writer.commit_unit_for(tx_id).unwrap() else { panic!("commit unit"); };
    let state = writer.query_transaction(tx_id).unwrap().unwrap();
    writer.query.tx_versions_cache.clear();
    reset_query_versions_for_tx_call_count();
    writer.ingest_known_transaction(tx, versions, state.fate.clone(), state.global_time, state.durability).unwrap();
    assert_eq!(query_versions_for_tx_call_count(), 1, "only fate processing needs a whole-transaction read");
    assert_eq!(writer.query_versions_for_tx(tx_id).unwrap().len(), 32);
}

// These internal receipts exercise the row-local ancestry/cache seam: neither
// traversal work nor retained cache memory is exposed by the public API.
#[test]
fn repeated_row_reachability_checks_reuse_complete_ancestry() {
    let schema = two_column_schema();
    let (_dir, mut writer) = open_node_with_schema(node(0xe6), schema.clone());
    let (_reader_dir, mut reader) = open_node_with_schema(node(0xea), schema);
    let row_uuid = row(0xe7);
    let (parent, parent_unit) = writer.commit_mergeable_unit_settled(
        MergeableCommit::new("todos", row_uuid, 10)
            .cells(BTreeMap::from([("title".to_owned(), "parent".to_owned())])),
    ).unwrap();
    let (child, child_unit) = writer.commit_mergeable_unit_settled(
        MergeableCommit::new("todos", row_uuid, 11).parents(vec![parent])
            .cells(BTreeMap::from([("title".to_owned(), "child".to_owned())])),
    ).unwrap();
    let table_id = writer.physical_table_id_for_schema(writer.catalogue.active_schema.schema, "todos").unwrap();
    let absent = TxId::new(TxTime(1), node(0xef));
    let reader_table = reader.physical_table_id_for_schema(reader.catalogue.active_schema.schema, "todos").unwrap();
    assert!(!reader.content_version_reaches_tx(reader_table, &BranchKey::default(), row_uuid, child, absent).unwrap());
    assert!(reader.content_version_reachability_cache.is_empty(), "missing transaction history must never establish a complete closure");
    reader.apply_sync_message_settled(parent_unit).unwrap();
    reader.apply_sync_message_settled(child_unit).unwrap();
    assert!(reader.content_version_reaches_tx(reader_table, &BranchKey::default(), row_uuid, child, parent).unwrap(), "newly received history must repair an earlier unknown answer");
    writer.clear_content_version_reachability_cache();
    writer.reset_merge_head_reachability_walks_for_test();
    assert!(!writer.content_version_reaches_tx(table_id, &BranchKey::default(), row_uuid, child, absent).unwrap());
    let cold_nodes = writer.merge_head_reachability_nodes_for_test();
    assert!(cold_nodes >= 2);
    assert!(!writer.content_version_reaches_tx(table_id, &BranchKey::default(), row_uuid, child, absent).unwrap());
    assert!(writer.content_version_reaches_tx(table_id, &BranchKey::default(), row_uuid, child, parent).unwrap());
    assert_eq!(writer.merge_head_reachability_nodes_for_test(), cold_nodes, "both positive and negative answers reuse the complete closure");
    assert!(!writer.content_version_reaches_tx(table_id, &BranchKey::default(), row(0xe8), child, parent).unwrap(), "another row must not inherit the cached ancestry");
    writer.clear_content_version_reachability_cache();
    assert!(writer.content_version_reaches_tx(table_id, &BranchKey::default(), row_uuid, child, parent).unwrap());
    assert!(writer.merge_head_reachability_nodes_for_test() > cold_nodes, "invalidation must force a new walk");
}

#[test]
fn ancestry_cache_bounds_entry_count_and_total_transaction_ids() {
    let (_dir, mut writer) = open_node_with_schema(node(0xe9), two_column_schema());
    let key = |index| ContentVersionReachabilityCacheKey {
        table_id: PhysicalTableId(1), branch_key: BranchKey::default(), row_uuid: row(1),
        start: TxId::new(TxTime(index), node(1)),
    };
    for index in 0..65 {
        writer.cache_content_version_reachability(key(index), [key(index).start].into_iter().collect());
    }
    assert_eq!(writer.content_version_reachability_cache.len(), 64);
    assert_eq!(writer.content_version_reachability_cache_order.len(), 64);
    assert_eq!(writer.content_version_reachability_cache_tx_ids, 64);
    assert!(!writer.content_version_reachability_cache.contains_key(&key(0)));
    writer.clear_content_version_reachability_cache();
    let ancestors = (0..32_768).map(|index| key(index).start).collect::<FxHashSet<_>>();
    for index in 0..3 {
        writer.cache_content_version_reachability(key(index), ancestors.clone());
    }
    assert_eq!(writer.content_version_reachability_cache.len(), 2);
    assert_eq!(writer.content_version_reachability_cache_tx_ids, 65_536);
    let oversized = (0..65_537).map(|index| key(index).start).collect();
    writer.cache_content_version_reachability(key(4), oversized);
    assert_eq!(writer.content_version_reachability_cache.len(), 2);
    assert_eq!(writer.content_version_reachability_cache_tx_ids, 65_536);
}
