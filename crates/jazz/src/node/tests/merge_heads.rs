// These tests are intentionally internal: merge-head records are node-local
// derived metadata and are not observable through the public Jazz API. The
// public behavior is convergence; this oracle pins the derived metadata that
// the merge fast path relies on.

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

#[test]
fn merge_heads_match_history_across_restart_between_concurrent_units() {
    let schema = two_column_schema();
    let (_writer_a_dir, mut writer_a) = open_node_with_schema(node(0xc1), schema.clone());
    let (_writer_b_dir, mut writer_b) = open_node_with_schema(node(0xc2), schema.clone());
    let (core_dir, mut core) = open_node_with_schema(node(0xc9), schema.clone());
    let row = row(0xca);

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
    drop(core);
    let mut core = reopen_node_at(&core_dir, node(0xc9), schema);
    core.assert_merge_heads_match_history_for_test("todos", row)
        .unwrap();
    core.apply_sync_message_settled(right_unit).unwrap();
    core.assert_merge_heads_match_history_for_test("todos", row)
        .unwrap();
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
        ),
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
