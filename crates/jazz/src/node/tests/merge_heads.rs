// These tests are intentionally internal: merge-head records are node-local
// derived metadata and are not observable through the public Jazz API. The
// public behavior is convergence; this oracle pins the derived metadata that
// the merge fast path relies on.

#[test]
fn merge_heads_match_history_for_ordinary_concurrent_units() {
    let schema = two_column_schema();
    let (_writer_a_dir, mut writer_a) = open_node_with_schema(node(0xa1), schema.clone());
    let (_writer_b_dir, mut writer_b) = open_node_with_schema(node(0xa2), schema.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(0xa9), schema);
    let row = row(0xaa);

    let (_left, left_unit) = writer_a
        .commit_mergeable_unit(MergeableCommit::new("todos", row, 10).cells(BTreeMap::from([(
            "title".to_owned(),
            "left".to_owned(),
        )])))
        .unwrap();
    let (_right, right_unit) = writer_b
        .commit_mergeable_unit(MergeableCommit::new("todos", row, 11).cells(BTreeMap::from([(
            "body".to_owned(),
            "right".to_owned(),
        )])))
        .unwrap();

    core.apply_sync_message(right_unit).unwrap();
    core.assert_merge_heads_match_history_for_test("todos", row)
        .unwrap();
    core.apply_sync_message(left_unit).unwrap();
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
        .commit_mergeable_unit(MergeableCommit::new("todos", row, 10).cells(BTreeMap::from([(
            "title".to_owned(),
            "left".to_owned(),
        )])))
        .unwrap();
    let (_right, right_unit) = writer_b
        .commit_mergeable_unit(MergeableCommit::new("todos", row, 11).cells(BTreeMap::from([(
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

    edge.ingest_edge_authority_mergeable_commit_unit(
        right_tx,
        right_versions,
        u64::MAX - SKEW_TOLERANCE_MS,
    )
    .unwrap();
    edge.assert_merge_heads_match_history_for_test("todos", row)
        .unwrap();
    edge.ingest_edge_authority_mergeable_commit_unit(
        left_tx,
        left_versions,
        u64::MAX - SKEW_TOLERANCE_MS,
    )
    .unwrap();
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
        .commit_mergeable_unit(MergeableCommit::new("todos", row, 10).cells(BTreeMap::from([(
            "title".to_owned(),
            "left".to_owned(),
        )])))
        .unwrap();
    let (right, right_unit) = writer_b
        .commit_mergeable_unit(MergeableCommit::new("todos", row, 11).cells(BTreeMap::from([(
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
fn merge_heads_match_history_after_parked_unit_resolves() {
    let schema = two_column_schema();
    let (_parent_dir, mut parent_writer) = open_node_with_schema(node(0xb1), schema.clone());
    let (_child_dir, mut child_writer) = open_node_with_schema(node(0xb2), schema.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(0xb9), schema);
    let row = row(0xba);

    let (parent_tx, parent_unit) = parent_writer
        .commit_mergeable_unit(MergeableCommit::new("todos", row, 10).cells(BTreeMap::from([(
            "title".to_owned(),
            "parent".to_owned(),
        )])))
        .unwrap();
    let (_child_tx, child_unit) = child_writer
        .commit_mergeable_unit(
            MergeableCommit::new("todos", row, 11)
                .parents(vec![parent_tx])
                .cells(BTreeMap::from([("body".to_owned(), "child".to_owned())])),
        )
        .unwrap();

    core.apply_sync_message(child_unit).unwrap();
    core.apply_sync_message(parent_unit).unwrap();
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
        .commit_mergeable_unit(MergeableCommit::new("todos", row, 10).cells(BTreeMap::from([(
            "title".to_owned(),
            "left".to_owned(),
        )])))
        .unwrap();
    let (_right, right_unit) = writer_b
        .commit_mergeable_unit(MergeableCommit::new("todos", row, 11).cells(BTreeMap::from([(
            "body".to_owned(),
            "right".to_owned(),
        )])))
        .unwrap();

    core.apply_sync_message(left_unit).unwrap();
    drop(core);
    let mut core = reopen_node_at(&core_dir, node(0xc9), schema);
    core.assert_merge_heads_match_history_for_test("todos", row)
        .unwrap();
    core.apply_sync_message(right_unit).unwrap();
    core.assert_merge_heads_match_history_for_test("todos", row)
        .unwrap();
}

#[test]
fn merge_heads_match_history_after_merge_version_application() {
    let schema = two_column_schema();
    let (_writer_a_dir, mut writer_a) = open_node_with_schema(node(0xd1), schema.clone());
    let (_writer_b_dir, mut writer_b) = open_node_with_schema(node(0xd2), schema.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(0xd9), schema);
    let row = row(0xda);

    let (_left, left_unit) = writer_a
        .commit_mergeable_unit(MergeableCommit::new("todos", row, 10).cells(BTreeMap::from([(
            "title".to_owned(),
            "left".to_owned(),
        )])))
        .unwrap();
    let (_right, right_unit) = writer_b
        .commit_mergeable_unit(MergeableCommit::new("todos", row, 11).cells(BTreeMap::from([(
            "body".to_owned(),
            "right".to_owned(),
        )])))
        .unwrap();

    core.apply_sync_message(left_unit).unwrap();
    core.apply_sync_message(right_unit).unwrap();
    let _ = core.view_update_for_current_rows("todos").unwrap();
    core.assert_merge_heads_match_history_for_test("todos", row)
        .unwrap();
}
