use crate::ids::AuthorId;
use crate::node::EdgeCacheClass;
use crate::peer::PeerEvictionPins;
use crate::schema::CONTENT_META_STORE;
use crate::tx::TxId;

#[test]
fn content_store_appends_reads_isolates_and_survives_reopen() {
    let (temp_dir, opened) = open_node();
    let writer = AuthorId::from_bytes([0xa1; 16]);
    let other_writer = AuthorId::from_bytes([0xb2; 16]);
    let row_id = row(0x11);
    let other_row = row(0x22);

    let first = opened
        .content_store()
        .append(writer, row_id, "body", b"hello ")
        .unwrap();
    let second = opened
        .content_store()
        .append(writer, row_id, "body", b"world")
        .unwrap();
    let other_column = opened
        .content_store()
        .append(writer, row_id, "notes", b"column")
        .unwrap();
    let other_row_extent = opened
        .content_store()
        .append(writer, other_row, "body", b"row")
        .unwrap();
    let other_writer_extent = opened
        .content_store()
        .append(other_writer, row_id, "body", b"writer")
        .unwrap();

    assert_eq!(first.offset, 0);
    assert_eq!(first.len, 6);
    assert_eq!(second.offset, 6);
    assert_eq!(second.len, 5);
    assert_eq!(opened.content_store().read(&first).unwrap(), b"hello ");
    assert_eq!(opened.content_store().read(&second).unwrap(), b"world");
    assert_eq!(
        opened.content_store().read(&other_column).unwrap(),
        b"column"
    );
    assert_eq!(
        opened.content_store().read(&other_row_extent).unwrap(),
        b"row"
    );
    assert_eq!(
        opened.content_store().read(&other_writer_extent).unwrap(),
        b"writer"
    );

    drop(opened);
    let reopened = reopen_node_at(&temp_dir, node(1), schema());
    assert_eq!(reopened.content_store().read(&first).unwrap(), b"hello ");
    assert_eq!(reopened.content_store().read(&second).unwrap(), b"world");
    let third = reopened
        .content_store()
        .append(writer, row_id, "body", b"!")
        .unwrap();
    assert_eq!(third.offset, 11);
    assert_eq!(reopened.content_store().read(&third).unwrap(), b"!");
}

#[test]
fn content_store_checkpoints_are_versioned_and_survive_reopen() {
    let (temp_dir, opened) = open_node();
    let row_id = row(0x33);
    let first = TxId::new(crate::time::TxTime(100), node(1));
    let second = TxId::new(crate::time::TxTime(101), node(1));

    opened
        .content_store()
        .put_checkpoint("notes", row_id, "body", first, b"first")
        .unwrap();
    opened
        .content_store()
        .put_checkpoint("notes", row_id, "body", second, b"second")
        .unwrap();
    opened
        .content_store()
        .put_checkpoint("notes", row_id, "summary", first, b"summary")
        .unwrap();

    assert_eq!(
        opened
            .content_store()
            .checkpoint("notes", row_id, "body", first)
            .unwrap()
            .as_deref(),
        Some(b"first".as_slice())
    );
    assert_eq!(
        opened
            .content_store()
            .checkpoint("notes", row_id, "body", second)
            .unwrap()
            .as_deref(),
        Some(b"second".as_slice())
    );
    assert_eq!(
        opened
            .content_store()
            .checkpoint("notes", row_id, "summary", first)
            .unwrap()
            .as_deref(),
        Some(b"summary".as_slice())
    );
    assert!(opened
        .content_store()
        .checkpoint("notes", row_id, "missing", first)
        .unwrap()
        .is_none());

    drop(opened);
    let reopened = reopen_node_at(&temp_dir, node(1), schema());
    assert_eq!(
        reopened
            .content_store()
            .checkpoint("notes", row_id, "body", second)
            .unwrap()
            .as_deref(),
        Some(b"second".as_slice())
    );
}

#[test]
fn content_store_put_extent_is_idempotent_and_rejects_conflicting_bytes() {
    let (_temp_dir, node) = open_node();
    let extent = crate::node::content_store::Extent {
        writer: user(0xa1),
        row: row(0x44),
        column: "body".to_owned(),
        offset: 8,
        len: 5,
    };

    node.content_store().put_extent(&extent, b"hello").unwrap();
    node.content_store().put_extent(&extent, b"hello").unwrap();
    assert_eq!(node.content_store().read(&extent).unwrap(), b"hello");
    assert!(matches!(
        node.content_store().put_extent(&extent, b"HELLO"),
        Err(Error::InvalidStoredValue("conflicting content extent"))
    ));
}

#[test]
fn content_store_reads_fail_closed_on_missing_or_gapped_ranges() {
    let (_temp_dir, node) = open_node();
    let writer = user(0xa2);
    let row_id = row(0x45);
    let first = crate::node::content_store::Extent {
        writer,
        row: row_id,
        column: "body".to_owned(),
        offset: 0,
        len: 3,
    };
    let second = crate::node::content_store::Extent {
        writer,
        row: row_id,
        column: "body".to_owned(),
        offset: 5,
        len: 2,
    };
    node.content_store().put_extent(&first, b"abc").unwrap();
    node.content_store().put_extent(&second, b"fg").unwrap();

    let missing_tail = crate::node::content_store::Extent {
        writer,
        row: row_id,
        column: "body".to_owned(),
        offset: 0,
        len: 8,
    };
    assert!(matches!(
        node.content_store().read(&missing_tail),
        Err(Error::InvalidStoredValue("content extent has a gap"))
    ));
    assert!(!node.content_store().contains(&missing_tail).unwrap());

    let absent = crate::node::content_store::Extent {
        writer,
        row: row_id,
        column: "body".to_owned(),
        offset: 9,
        len: 1,
    };
    assert!(matches!(
        node.content_store().read(&absent),
        Err(Error::InvalidStoredValue("content extent is incomplete"))
    ));
    assert!(!node.content_store().contains(&absent).unwrap());
}

#[test]
fn evict_cold_removes_content_bytes_and_preserves_pin_set() {
    let (_temp_dir, mut opened) = open_node();
    let row_uuid = row(0x46);
    let extent = opened
        .content_store()
        .append(user(0xa1), row_uuid, "body", b"evict me")
        .unwrap();
    let checkpoint_tx = TxId::new(crate::time::TxTime(46), node(1));
    opened
        .content_store()
        .put_checkpoint("todos", row_uuid, "body", checkpoint_tx, b"checkpoint")
        .unwrap();

    let pending_tx = opened
        .commit_mergeable(MergeableCommit::new("todos", row_uuid, 46).cells(BTreeMap::from([(
            "title".to_owned(),
            Value::String("pending".to_owned()),
        )])))
        .unwrap();
    let deferred_tx = opened
        .commit_mergeable(MergeableCommit::new("todos", row(0x47), 47).cells(BTreeMap::from([(
            "title".to_owned(),
            Value::String("deferred".to_owned()),
        )])))
        .unwrap();
    opened.finalize_local_mergeable_commit(deferred_tx).unwrap();

    let mut peer = PeerState::new();
    let deferred_unit = opened.commit_unit_for(deferred_tx).unwrap();
    let SyncMessage::CommitUnit { tx, versions } = deferred_unit else {
        panic!("expected commit unit");
    };
    peer.defer_edge_fate_for_test(tx, versions, 47);
    let subscription = opened.whole_table_subscription_key("todos").unwrap();
    peer.retain_edge_scope_subscription_for_test(subscription);

    let report = opened.evict_cold(&peer.eviction_pins()).unwrap();

    assert_eq!(report.content_extent_entries, 1);
    assert_eq!(report.content_checkpoint_entries, 1);
    assert_eq!(report.content_meta_entries_pinned, 1);
    assert_eq!(report.fate_pending_txs_pinned, 1);
    assert_eq!(report.deferred_edge_fate_txs_pinned, 1);
    assert_eq!(report.referenced_scope_subscriptions_pinned, 1);
    assert_eq!(
        opened.query_transaction(pending_tx).unwrap().unwrap().fate,
        Fate::Pending
    );
    assert!(!opened
        .query_versions_for_tx(deferred_tx)
        .unwrap()
        .is_empty());
    assert_eq!(
        opened
            .classify_row_version_for_eviction(deferred_tx, &peer.eviction_pins())
            .unwrap(),
        EdgeCacheClass::Pinned
    );
    assert_eq!(
        opened
            .database
            .direct_record_store(CONTENT_META_STORE)
            .unwrap()
            .prefix(&[])
            .unwrap()
            .len(),
        1
    );
    assert!(matches!(
        opened.content_store().read(&extent),
        Err(Error::InvalidStoredValue("content extent is incomplete"))
    ));
}

#[test]
fn scope_subscription_pin_drops_with_refcount() {
    let (_temp_dir, node) = open_node();
    let subscription = node.whole_table_subscription_key("todos").unwrap();
    let mut peer = PeerState::new();

    peer.retain_edge_scope_subscription_for_test(subscription);
    peer.retain_edge_scope_subscription_for_test(subscription);
    assert!(peer
        .eviction_pins()
        .referenced_scope_subscriptions
        .contains(&subscription));

    peer.release_edge_scope_subscription_for_test(subscription, 0);
    assert!(peer
        .eviction_pins()
        .referenced_scope_subscriptions
        .contains(&subscription));

    peer.release_edge_scope_subscription_for_test(subscription, 0);
    assert!(peer
        .eviction_pins()
        .referenced_scope_subscriptions
        .contains(&subscription));
}

#[test]
fn evicted_content_bytes_are_restored_by_fetch_and_known_state_rehydrate() {
    let schema = large_value_schema();
    let row_uuid = row(0x48);
    let (_core_dir, mut core) = open_node_with_schema(node(0x48), schema.clone());
    let (_edge_dir, mut edge) = open_node_with_schema(node(0x49), schema.clone());

    let tx_id = core
        .commit_mergeable_unit(
            MergeableCommit::new("docs", row_uuid, 48)
                .made_by(user(0xa1))
                .cells(BTreeMap::from([(
                    "body".to_owned(),
                    Value::Bytes(b"refetch me".to_vec()),
                )])),
        )
        .unwrap()
        .0;
    core.finalize_local_mergeable_commit(tx_id).unwrap();
    let SyncMessage::CommitUnit { tx, versions } = core.commit_unit_for(tx_id).unwrap() else {
        panic!("expected commit unit");
    };
    let extent = versions
        .iter()
        .find_map(|version| {
            let Value::Bytes(payload) = version.cell_at(0).unwrap() else {
                return None;
            };
            text_oplog::decode(&payload)
                .unwrap()
                .into_iter()
                .find_map(|op| match op {
                    TextOp::Insert {
                        content: TextContent::Ref(extent),
                        ..
                    } => Some(extent),
                    _ => None,
                })
        })
        .expect("large-value commit must reference an extent");
    let bytes = core.content_store().read(&extent).unwrap();

    edge.apply_content_extents(vec![crate::protocol::ContentExtent {
        owner: crate::protocol::LargeValueOwnerRef::current_row(extent.row),
        extent: extent.clone(),
        bytes,
    }])
    .unwrap();
    edge.apply_sync_message(SyncMessage::CommitUnit { tx, versions })
        .unwrap();
    assert_eq!(
        edge.current_rows("docs", DurabilityTier::Local)
            .unwrap()
            .remove(0)
            .cell(&schema.tables[0], "body"),
        Some(Value::Bytes(b"refetch me".to_vec()))
    );

    let mut peer = PeerState::new();
    let initial = peer.reset_current_rows(&mut core, "docs").unwrap();
    assert!(peer.shipped_complete_tx_payloads().is_empty());
    edge.apply_sync_message(initial).unwrap();

    let report = edge.evict_cold(&PeerEvictionPins::default()).unwrap();
    assert_eq!(report.content_extent_entries, 1);
    assert!(!edge.content_store().contains(&extent).unwrap());
    assert_eq!(peer.forget_evicted_versions([tx_id]), 0);

    let SyncMessage::ContentExtents { extents } = peer
        .handle_content_extent_fetch(
            &mut core,
            SyncMessage::FetchContentExtent {
                owner: crate::protocol::LargeValueOwnerRef::current_row(row_uuid),
                extent: extent.clone(),
            },
        )
        .unwrap()
    else {
        panic!("expected content extents");
    };
    edge.apply_content_extents(extents).unwrap();

    let update = peer.reset_current_rows(&mut core, "docs").unwrap();
    let SyncMessage::ViewUpdate { version_bundles, .. } = &update else {
        panic!("expected view update");
    };
    assert_eq!(version_bundles.len(), 1);
    edge.apply_sync_message(update).unwrap();
    assert_eq!(
        edge.current_rows("docs", DurabilityTier::Local)
            .unwrap()
            .remove(0)
            .cell(&schema.tables[0], "body"),
        Some(Value::Bytes(b"refetch me".to_vec()))
    );
}

#[test]
fn large_value_edits_reject_empty_batches_and_non_large_value_columns() {
    let schema = JazzSchema::new([TableSchema::new(
        "docs",
        [
            crate::schema::ColumnSchema::text("body"),
            crate::schema::ColumnSchema::new("title", ColumnType::String),
        ],
    )]);
    let (_temp_dir, mut node) = open_node_with_schema(node(0x4c), schema);

    assert!(matches!(
        node.commit_large_value_edit(LargeValueEditCommit::new("docs", row(0x4c), "body", 10)),
        Err(Error::InvalidMergeableCommit(
            "large-value edit requires at least one operation"
        ))
    ));
    assert!(matches!(
        node.commit_large_value_edit(
            LargeValueEditCommit::new("docs", row(0x4c), "title", 11).insert(0, b"text")
        ),
        Err(Error::InvalidMergeableCommit(
            "large-value edit column must be text or blob"
        ))
    ));
}

#[test]
fn commit_units_with_missing_large_value_content_are_parked_until_extents_arrive() {
    let schema = JazzSchema::new([TableSchema::new(
        "docs",
        [crate::schema::ColumnSchema::blob("body")],
    )]);
    let (_writer_dir, mut writer) = open_node_with_schema(node(0x4d), schema.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(0x4e), schema.clone());
    let row_uuid = row(0x4d);
    let (tx_id, _initial_unit) = writer
        .commit_mergeable_unit(
            MergeableCommit::new("docs", row_uuid, 10)
                .made_by(user(0xa1))
                .cells(BTreeMap::from([(
                    "body".to_owned(),
                    Value::Bytes(b"park me".to_vec()),
                )])),
        )
        .unwrap();
    let unit = writer.commit_unit_for(tx_id).unwrap();
    let SyncMessage::CommitUnit { tx, versions } = unit else {
        panic!("expected commit unit");
    };
    let table = &schema.tables[0];
    let extents = versions
        .iter()
        .flat_map(|version| {
            let Value::Bytes(payload) = version.cell_at(0).unwrap() else {
                panic!("expected text oplog payload");
            };
            text_oplog::decode(&payload)
                .unwrap()
                .into_iter()
                .filter_map(|op| match op {
                    TextOp::Insert {
                        content: TextContent::Ref(extent),
                        ..
                    } => Some(crate::protocol::ContentExtent {
                        owner: crate::protocol::LargeValueOwnerRef::current_row(extent.row),
                        bytes: writer.content_store().read(&extent).unwrap(),
                        extent,
                    }),
                    _ => None,
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    assert_eq!(extents.len(), 1);

    assert!(core
        .ingest_commit_unit(tx.clone(), versions.clone(), u64::MAX - SKEW_TOLERANCE_MS)
        .unwrap()
        .is_empty());
    assert_eq!(core.sync_metrics().parked_orphans, 1);
    assert!(core.query_transaction(tx_id).unwrap().is_none());

    let updates = core.apply_content_extents(extents).unwrap();
    assert!(updates.iter().any(|message| matches!(
        message,
        SyncMessage::FateUpdate {
            tx_id: accepted,
            fate: Fate::Accepted,
            ..
        } if *accepted == tx_id
    )));
    assert_eq!(
        core.current_rows("docs", DurabilityTier::Local)
            .unwrap()
            .remove(0)
            .cell(table, "body"),
        Some(Value::Bytes(b"park me".to_vec()))
    );
}

fn large_value_schema() -> JazzSchema {
    JazzSchema::new([TableSchema::new(
        "docs",
        [crate::schema::ColumnSchema::blob("body")],
    )])
}

fn text_large_value_schema() -> JazzSchema {
    JazzSchema::new([TableSchema::new(
        "docs",
        [crate::schema::ColumnSchema::text("body")],
    )])
}

fn append_large_value_edits(
    node: &mut NodeState<RocksDbStorage>,
    row_uuid: RowUuid,
    count: usize,
) -> Vec<TxId> {
    let mut tx_ids = Vec::new();
    for idx in 0..count {
        let byte = b'a' + u8::try_from(idx).unwrap();
        let tx_id = node
            .commit_large_value_edit(
                LargeValueEditCommit::new("docs", row_uuid, "body", 10 + idx as u64)
                    .made_by(user(0xa1))
                    .insert(idx, [byte]),
            )
            .unwrap();
        node.finalize_local_mergeable_commit(tx_id).unwrap();
        tx_ids.push(tx_id);
    }
    tx_ids
}

#[test]
fn accepted_large_value_ingestion_places_checkpoint_at_interval() {
    let schema = large_value_schema();
    let row_uuid = row(0x61);
    let (_dir, mut node) =
        open_node_with_schema_and_checkpoint_interval(node(0x61), schema, 3);

    let tx_ids = append_large_value_edits(&mut node, row_uuid, 5);

    assert!(node
        .content_store()
        .checkpoint("docs", row_uuid, "body", tx_ids[2])
        .unwrap()
        .is_some());
    assert!(node
        .content_store()
        .checkpoint("docs", row_uuid, "body", tx_ids[3])
        .unwrap()
        .is_none());
    assert_eq!(node.large_value_metrics().checkpoint_writes, 1);
}

#[test]
fn checkpointed_read_replays_only_suffix_and_matches_full_replay() {
    let schema = large_value_schema();
    let row_uuid = row(0x62);
    let (_checkpointed_dir, mut checkpointed) =
        open_node_with_schema_and_checkpoint_interval(node(0x62), schema.clone(), 3);
    let (_full_dir, mut full_replay) =
        open_node_with_schema_and_checkpoint_interval(node(0x63), schema, 1000);

    append_large_value_edits(&mut checkpointed, row_uuid, 5);
    append_large_value_edits(&mut full_replay, row_uuid, 5);

    checkpointed.reset_large_value_metrics();
    full_replay.reset_large_value_metrics();
    let checkpointed_value = checkpointed
        .current_rows("docs", DurabilityTier::Local)
        .unwrap()
        .remove(0)
        .cell(&large_value_schema().tables[0], "body");
    let full_value = full_replay
        .current_rows("docs", DurabilityTier::Local)
        .unwrap()
        .remove(0)
        .cell(&large_value_schema().tables[0], "body");

    assert_eq!(checkpointed_value, Some(Value::Bytes(b"abcde".to_vec())));
    assert_eq!(checkpointed_value, full_value);
    assert_eq!(checkpointed.large_value_metrics().checkpoint_hits, 1);
    assert_eq!(checkpointed.large_value_metrics().last_replayed_ops, 2);
    assert_eq!(full_replay.large_value_metrics().checkpoint_hits, 0);
    assert_eq!(full_replay.large_value_metrics().last_replayed_ops, 5);
}

#[test]
fn large_value_checkpoints_survive_reopen() {
    let schema = large_value_schema();
    let row_uuid = row(0x64);
    let (dir, mut opened) =
        open_node_with_schema_and_checkpoint_interval(node(0x64), schema.clone(), 3);

    append_large_value_edits(&mut opened, row_uuid, 5);
    drop(opened);

    let mut reopened = reopen_node_at_with_checkpoint_interval(&dir, node(0x64), schema.clone(), 3);
    reopened.reset_large_value_metrics();
    let value = reopened
        .current_rows("docs", DurabilityTier::Local)
        .unwrap()
        .remove(0)
        .cell(&schema.tables[0], "body");

    assert_eq!(value, Some(Value::Bytes(b"abcde".to_vec())));
    assert_eq!(reopened.large_value_metrics().checkpoint_hits, 1);
    assert_eq!(reopened.large_value_metrics().last_replayed_ops, 2);
}

#[test]
fn authority_merge_version_op_merges_concurrent_large_value_edits() {
    let left_first = merged_concurrent_large_value_body(true);
    let right_first = merged_concurrent_large_value_body(false);

    assert_eq!(left_first, Some(Value::Bytes(b"aLEFTRIGHTbc".to_vec())));
    assert_eq!(left_first, right_first);
}

#[test]
fn authority_merge_version_merges_concurrent_text_edits_and_records_strategy() {
    let left_first = merged_concurrent_text_body(true);
    let right_first = merged_concurrent_text_body(false);

    assert_eq!(left_first, Some(Value::Bytes(b"aLEFTRIGHTbc".to_vec())));
    assert_eq!(left_first, right_first);
}

#[test]
fn out_of_order_text_unit_resolves_after_parent_arrives() {
    let schema = text_large_value_schema();
    let row_uuid = row(0x76);
    let (_writer_dir, mut writer) = open_node_with_schema(node(0x76), schema.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(0x78), schema.clone());

    let base_unit = commit_large_value_edit_unit(
        &mut writer,
        LargeValueEditCommit::new("docs", row_uuid, "body", 10)
            .made_by(user(0xa1))
            .insert(0, b"abc"),
    );
    let child_unit = commit_large_value_edit_unit(
        &mut writer,
        LargeValueEditCommit::new("docs", row_uuid, "body", 20)
            .made_by(user(0xa1))
            .insert(1, b"X"),
    );
    let _ = core.apply_sync_message(child_unit).unwrap();
    assert_eq!(core.sync_metrics().parked_orphans, 1);
    assert!(core.current_rows("docs", DurabilityTier::Local).unwrap().is_empty());

    let _ = core.apply_sync_message(base_unit).unwrap();
    assert_eq!(
        core.current_rows("docs", DurabilityTier::Local)
            .unwrap()
            .remove(0)
            .cell(&schema.tables[0], "body"),
        Some(Value::Bytes(b"aXbc".to_vec()))
    );
}

#[test]
fn text_edit_history_rehydrates_materialized_text_after_reopen() {
    let schema = text_large_value_schema();
    let row_uuid = row(0x79);
    let (dir, mut opened) = open_node_with_schema(node(0x79), schema.clone());

    opened
        .commit_large_value_edit(
            LargeValueEditCommit::new("docs", row_uuid, "body", 10)
                .made_by(user(0xa1))
                .insert(0, b"hello"),
        )
        .unwrap();
    opened
        .commit_large_value_edit(
            LargeValueEditCommit::new("docs", row_uuid, "body", 20)
                .made_by(user(0xa1))
                .insert(5, b" after restart"),
        )
        .unwrap();
    drop(opened);

    let mut reopened = reopen_node_at(&dir, node(0x79), schema.clone());
    assert_eq!(
        reopened
            .current_rows("docs", DurabilityTier::Local)
            .unwrap()
            .remove(0)
            .cell(&schema.tables[0], "body"),
        Some(Value::Bytes(b"hello after restart".to_vec()))
    );
}

#[test]
fn linear_large_value_history_materializes_without_merge_regression() {
    let schema = large_value_schema();
    let row_uuid = row(0x66);
    let (_dir, mut node) = open_node_with_schema(node(0x66), schema.clone());

    node.commit_mergeable(
        MergeableCommit::new("docs", row_uuid, 10).cells(BTreeMap::from([(
            "body".to_owned(),
            Value::Bytes(b"abc".to_vec()),
        )])),
    )
    .unwrap();
    node.commit_large_value_edit(
        LargeValueEditCommit::new("docs", row_uuid, "body", 20)
            .made_by(user(0xa1))
            .insert(1, b"LEFT"),
    )
    .unwrap();
    node.commit_large_value_edit(
        LargeValueEditCommit::new("docs", row_uuid, "body", 30)
            .made_by(user(0xa1))
            .delete(1, 4)
            .insert(1, b"L"),
    )
    .unwrap();

    let value = node
        .current_rows("docs", DurabilityTier::Local)
        .unwrap()
        .remove(0)
        .cell(&schema.tables[0], "body");
    assert_eq!(value, Some(Value::Bytes(b"aLbc".to_vec())));
}

#[test]
fn client_local_large_value_conflict_still_lww_drops_without_upstream_merge() {
    let schema = large_value_schema();
    let row_uuid = row(0x67);
    let (_base_dir, mut base_writer) = open_node_with_schema(node(0x67), schema.clone());
    let (_client_dir, mut client) = open_node_with_schema(node(0x68), schema.clone());
    let (_left_dir, mut left_writer) = open_node_with_schema(node(0x69), schema.clone());
    let (_right_dir, mut right_writer) = open_node_with_schema(node(0x6a), schema.clone());

    let base_unit = commit_large_value_unit(
        &mut base_writer,
        MergeableCommit::new("docs", row_uuid, 10).cells(BTreeMap::from([(
            "body".to_owned(),
            Value::Bytes(b"abc".to_vec()),
        )])),
    );
    apply_large_value_unit(&mut client, &base_writer, base_unit.clone());
    apply_large_value_unit(&mut left_writer, &base_writer, base_unit.clone());
    apply_large_value_unit(&mut right_writer, &base_writer, base_unit);

    let left = commit_large_value_edit_unit(
        &mut left_writer,
        LargeValueEditCommit::new("docs", row_uuid, "body", 20)
            .made_by(user(0xa1))
            .insert(1, b"LEFT"),
    );
    let right = commit_large_value_edit_unit(
        &mut right_writer,
        LargeValueEditCommit::new("docs", row_uuid, "body", 21)
            .made_by(user(0xa2))
            .insert(1, b"RIGHT"),
    );
    apply_large_value_unit(&mut client, &left_writer, left);
    apply_large_value_unit(&mut client, &right_writer, right);

    let winner_payload = client
        .current_rows("docs", DurabilityTier::Local)
        .unwrap()
        .remove(0)
        .cell(&schema.tables[0], "body");
    let Some(Value::Bytes(payload)) = winner_payload else {
        panic!("expected winning large-value payload");
    };
    let materialized = replay_large_value_payload(&client, b"abc", &payload);
    assert!(materialized == b"aLEFTbc".to_vec() || materialized == b"aRIGHTbc".to_vec());
    assert_ne!(materialized, b"aLEFTRIGHTbc".to_vec());
}

fn merged_concurrent_large_value_body(left_first: bool) -> Option<Value> {
    let schema = large_value_schema();
    let row_uuid = row(if left_first { 0x65 } else { 0x75 });
    let (_base_dir, mut base_writer) = open_node_with_schema(node(0x71), schema.clone());
    let (_left_dir, mut left_writer) = open_node_with_schema(node(0x72), schema.clone());
    let (_right_dir, mut right_writer) = open_node_with_schema(node(0x73), schema.clone());
    let (_core_dir, mut core) =
        open_node_with_schema_and_checkpoint_interval(node(0x74), schema.clone(), 1);

    let base_unit = commit_large_value_unit(
        &mut base_writer,
        MergeableCommit::new("docs", row_uuid, 10).cells(BTreeMap::from([(
            "body".to_owned(),
            Value::Bytes(b"abc".to_vec()),
        )])),
    );
    apply_large_value_unit(&mut core, &base_writer, base_unit.clone());
    apply_large_value_unit(&mut left_writer, &base_writer, base_unit.clone());
    apply_large_value_unit(&mut right_writer, &base_writer, base_unit);

    let left = commit_large_value_edit_unit(
        &mut left_writer,
        LargeValueEditCommit::new("docs", row_uuid, "body", 20)
            .made_by(user(0xa1))
            .insert(1, b"LEFT"),
    );
    let right = commit_large_value_edit_unit(
        &mut right_writer,
        LargeValueEditCommit::new("docs", row_uuid, "body", 21)
            .made_by(user(0xa2))
            .insert(1, b"RIGHT"),
    );
    if left_first {
        apply_large_value_unit(&mut core, &left_writer, left);
        apply_large_value_unit(&mut core, &right_writer, right);
    } else {
        apply_large_value_unit(&mut core, &right_writer, right);
        apply_large_value_unit(&mut core, &left_writer, left);
    }

    let merge = core
        .query_all_versions()
        .unwrap()
        .into_iter()
        .find(|version| {
            version.row_uuid() == row_uuid
                && core.version_tx_id(version).unwrap().node == node(0x74)
                && version.parents().len() == 2
        })
        .expect("core should create a large-value merge version");
    assert_eq!(merge.parents().len(), 2);
    assert!(core
        .content_store()
        .checkpoint("docs", row_uuid, "body", core.version_tx_id(&merge).unwrap())
        .unwrap()
        .is_some());

    let Some(Value::Bytes(payload)) = core
        .current_rows("docs", DurabilityTier::Global)
        .unwrap()
        .remove(0)
        .cell(&schema.tables[0], "body")
    else {
        panic!("expected merge large-value payload");
    };
    Some(Value::Bytes(replay_large_value_payload(
        &core,
        b"aRIGHTbc",
        &payload,
    )))
}

fn merged_concurrent_text_body(left_first: bool) -> Option<Value> {
    let schema = text_large_value_schema();
    let row_uuid = row(if left_first { 0x7a } else { 0x7b });
    let (_base_dir, mut base_writer) = open_node_with_schema(node(0x7c), schema.clone());
    let (_left_dir, mut left_writer) = open_node_with_schema(node(0x7d), schema.clone());
    let (_right_dir, mut right_writer) = open_node_with_schema(node(0x7e), schema.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(0x7f), schema.clone());

    let base_unit = commit_large_value_edit_unit(
        &mut base_writer,
        LargeValueEditCommit::new("docs", row_uuid, "body", 10)
            .made_by(user(0xa1))
            .insert(0, b"abc"),
    );
    apply_large_value_unit(&mut core, &base_writer, base_unit.clone());
    apply_large_value_unit(&mut left_writer, &base_writer, base_unit.clone());
    apply_large_value_unit(&mut right_writer, &base_writer, base_unit);

    let left = commit_large_value_edit_unit(
        &mut left_writer,
        LargeValueEditCommit::new("docs", row_uuid, "body", 20)
            .made_by(user(0xa1))
            .insert(1, b"LEFT"),
    );
    let right = commit_large_value_edit_unit(
        &mut right_writer,
        LargeValueEditCommit::new("docs", row_uuid, "body", 21)
            .made_by(user(0xa2))
            .insert(1, b"RIGHT"),
    );
    if left_first {
        apply_large_value_unit(&mut core, &left_writer, left);
        apply_large_value_unit(&mut core, &right_writer, right);
    } else {
        apply_large_value_unit(&mut core, &right_writer, right);
        apply_large_value_unit(&mut core, &left_writer, left);
    }

    let merge = core
        .query_all_versions()
        .unwrap()
        .into_iter()
        .find(|version| {
            version.row_uuid() == row_uuid
                && core.version_tx_id(version).unwrap().node == node(0x7f)
                && version.parents().len() == 2
        })
        .expect("core should create a text merge version");
    let merge_tx = core
        .query_transaction(core.version_tx_id(&merge).unwrap())
        .unwrap()
        .expect("merge transaction should be recorded");
    let strategy = merge_tx
        .tx
        .merge_strategy
        .expect("text merge should record strategy");
    assert_eq!(strategy.id, "builtin.text-rle-v1");
    assert_eq!(strategy.version, 1);

    Some(Value::Bytes(
        core.materialize_large_value_column(&schema.tables[0], &merge, "body")
            .unwrap(),
    ))
}

fn commit_large_value_unit(
    node: &mut NodeState<RocksDbStorage>,
    commit: MergeableCommit,
) -> SyncMessage {
    let (tx_id, unit) = node.commit_mergeable_unit(commit).unwrap();
    node.finalize_local_mergeable_commit(tx_id).unwrap();
    let _ = unit;
    node.commit_unit_for(tx_id).unwrap()
}

fn commit_large_value_edit_unit(
    node: &mut NodeState<RocksDbStorage>,
    edit: LargeValueEditCommit,
) -> SyncMessage {
    let tx_id = node.commit_large_value_edit(edit).unwrap();
    node.finalize_local_mergeable_commit(tx_id).unwrap();
    node.commit_unit_for(tx_id).unwrap()
}

fn apply_large_value_unit(
    target: &mut NodeState<RocksDbStorage>,
    source: &NodeState<RocksDbStorage>,
    unit: SyncMessage,
) {
    for extent in large_value_extents(source, &unit) {
        target.content_store().put_extent(&extent.extent, &extent.bytes).unwrap();
    }
    let _ = target.apply_sync_message(unit).unwrap();
}

fn large_value_extents(
    source: &NodeState<RocksDbStorage>,
    unit: &SyncMessage,
) -> Vec<crate::protocol::ContentExtent> {
    let SyncMessage::CommitUnit { versions, .. } = unit else {
        return Vec::new();
    };
    versions
        .iter()
        .filter_map(|version| {
            let Value::Bytes(payload) = version.cell_at(0)? else {
                return None;
            };
            text_oplog::decode(&payload).ok()
        })
        .flatten()
        .filter_map(|op| match op {
            TextOp::Insert {
                content: TextContent::Ref(extent),
                ..
            } => Some(crate::protocol::ContentExtent {
                owner: crate::protocol::LargeValueOwnerRef::current_row(extent.row),
                bytes: source.content_store().read(&extent).unwrap(),
                extent,
            }),
            _ => None,
        })
        .collect()
}

fn replay_large_value_payload(
    node: &NodeState<RocksDbStorage>,
    parent: &[u8],
    payload: &[u8],
) -> Vec<u8> {
    let ops = text_oplog::decode(payload)
        .unwrap()
        .into_iter()
        .map(|op| match op {
            TextOp::Insert {
                pos,
                content: TextContent::Ref(extent),
            } => TextOp::Insert {
                pos,
                content: TextContent::Inline(node.content_store().read(&extent).unwrap()),
            },
            other => other,
        })
        .collect::<Vec<_>>();
    text_oplog::replay(parent, &ops)
}
