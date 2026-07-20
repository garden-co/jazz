// These are internal topology pins: the edge-authority acceptance and core
// promotion seam is not exposed as a distinct public API yet.

fn split_commit_unit(unit: SyncMessage) -> (Transaction, Vec<VersionRecord>) {
    let SyncMessage::CommitUnit { tx, versions } = unit else {
        panic!("expected commit unit");
    };
    (tx, versions)
}

fn edge_accept_mergeable_unit(
    edge: &mut NodeState<RocksDbStorage>,
    unit: SyncMessage,
) -> (Transaction, Vec<VersionRecord>, Vec<SyncMessage>) {
    let (tx, versions) = split_commit_unit(unit);
    let updates = edge
        .ingest_edge_authority_mergeable_commit_unit(
            tx.clone(),
            versions.clone(),
            u64::MAX - SKEW_TOLERANCE_MS,
        )
        .unwrap();
    assert_eq!(
        updates,
        vec![SyncMessage::FateUpdate {
            tx_id: tx.tx_id,
            fate: Fate::Accepted,
            global_seq: None,
            durability: Some(DurabilityTier::Edge),
        }]
    );
    (tx, versions, updates)
}

fn edge_accept_large_value_unit(
    edge: &mut NodeState<RocksDbStorage>,
    source: &NodeState<RocksDbStorage>,
    unit: SyncMessage,
) -> (Transaction, Vec<VersionRecord>) {
    for extent in large_value_extents(source, &unit) {
        edge.content_store()
            .put_extent(&extent.extent, &extent.bytes)
            .unwrap();
    }
    let (tx, versions, _) = edge_accept_mergeable_unit(edge, unit);
    (tx, versions)
}

fn titles_at(
    node: &mut NodeState<RocksDbStorage>,
    tier: DurabilityTier,
) -> BTreeMap<RowUuid, Value> {
    node.current_rows("todos", tier)
        .unwrap()
        .into_iter()
        .map(|row| {
            (
                row.row_uuid(),
                row.cell(&node.catalogue.schema.tables[0], "title")
                    .unwrap()
                    .to_owned(),
            )
        })
        .collect()
}

fn assert_current_title(
    node: &mut NodeState<RocksDbStorage>,
    tier: DurabilityTier,
    row_uuid: RowUuid,
    title: &str,
) {
    assert_eq!(
        titles_at(node, tier),
        BTreeMap::from([(row_uuid, Value::String(title.to_owned()))])
    );
}

fn global_promote_edge_unit(
    core: &mut NodeState<RocksDbStorage>,
    tx: Transaction,
    versions: Vec<VersionRecord>,
) -> SyncMessage {
    let [fate] = core
        .finalize_edge_accepted_mergeable_commit_unit_once(
            tx.clone(),
            versions,
            u64::MAX - SKEW_TOLERANCE_MS,
        )
        .unwrap()
        .try_into()
        .unwrap();
    assert!(matches!(
        fate,
        SyncMessage::FateUpdate {
            tx_id,
            fate: Fate::Accepted,
            global_seq: Some(_),
            durability: Some(DurabilityTier::Global),
        } if tx_id == tx.tx_id
    ));
    fate
}

#[test]
fn edge_accepted_mergeable_promotes_to_global_without_revalidating_write_policy() {
    let schema = owner_policy_schema();
    let row_uuid = row(0xe1);
    let owner = user(0xa1);
    let (_writer_dir, mut writer) = open_node_with_schema(node(0xe1), schema.clone());
    let (_edge_dir, mut edge) = open_node_with_schema(node(0xe2), schema.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(0xe3), schema.clone());
    let (_reader_dir, mut reader) = open_node_with_schema(node(0xe4), schema);

    let unit = writer
        .commit_mergeable_unit(
            MergeableCommit::new("todos", row_uuid, 10)
                .made_by(owner)
                .cells(owner_cells(owner, "edge-visible")),
        )
        .unwrap()
        .1;
    let (tx, versions, _) = edge_accept_mergeable_unit(&mut edge, unit);

    assert_current_title(&mut edge, DurabilityTier::Edge, row_uuid, "edge-visible");
    assert!(core
        .current_rows("todos", DurabilityTier::Global)
        .unwrap()
        .is_empty());

    let fate = global_promote_edge_unit(&mut core, tx.clone(), versions);
    assert_current_title(&mut core, DurabilityTier::Global, row_uuid, "edge-visible");
    edge.apply_sync_message(fate).unwrap();

    let mut peer = PeerState::new();
    let update = peer.current_rows_update(&mut core, "todos").unwrap();
    reader.apply_sync_message(update).unwrap();
    assert_current_title(
        &mut reader,
        DurabilityTier::Global,
        row_uuid,
        "edge-visible",
    );
    assert_eq!(
        edge.transaction_state(tx.tx_id).unwrap().2,
        DurabilityTier::Global
    );
}

#[test]
fn edge_serves_and_accepts_mergeable_writes_while_disconnected() {
    let (_writer_dir, mut writer) = open_node_with_schema(node(0xe5), schema());
    let (_edge_dir, mut edge) = open_node_with_schema(node(0xe6), schema());
    let (_core_dir, mut core) = open_node_with_schema(node(0xe7), schema());
    let row_uuid = row(0xe5);

    let first = writer
        .commit_mergeable_unit(
            MergeableCommit::new("todos", row_uuid, 10).cells(title_cells("one")),
        )
        .unwrap()
        .1;
    let (first_tx, first_versions, _) = edge_accept_mergeable_unit(&mut edge, first);
    assert_current_title(&mut edge, DurabilityTier::Edge, row_uuid, "one");

    let second = writer
        .commit_mergeable_unit(
            MergeableCommit::new("todos", row_uuid, 20)
                .parents(vec![first_tx.tx_id])
                .cells(title_cells("two")),
        )
        .unwrap()
        .1;
    let (second_tx, second_versions, _) = edge_accept_mergeable_unit(&mut edge, second);
    assert_current_title(&mut edge, DurabilityTier::Edge, row_uuid, "two");
    assert!(core
        .current_rows("todos", DurabilityTier::Global)
        .unwrap()
        .is_empty());

    global_promote_edge_unit(&mut core, first_tx, first_versions);
    global_promote_edge_unit(&mut core, second_tx, second_versions);
    assert_current_title(&mut core, DurabilityTier::Global, row_uuid, "two");
}

#[test]
fn edge_authority_accepts_mergeable_insert_update_delete_and_restore() {
    let (_writer_dir, mut writer) = open_node_with_schema(node(0xe8), schema());
    let (_edge_dir, mut edge) = open_node_with_schema(node(0xe9), schema());
    let row_uuid = row(0xe8);

    let insert = writer
        .commit_mergeable_unit(
            MergeableCommit::new("todos", row_uuid, 10).cells(title_cells("one")),
        )
        .unwrap()
        .1;
    let (insert_tx, _, _) = edge_accept_mergeable_unit(&mut edge, insert);
    assert_current_title(&mut edge, DurabilityTier::Edge, row_uuid, "one");

    let update = writer
        .commit_mergeable_unit(
            MergeableCommit::new("todos", row_uuid, 20)
                .parents(vec![insert_tx.tx_id])
                .cells(title_cells("two")),
        )
        .unwrap()
        .1;
    let (update_tx, _, _) = edge_accept_mergeable_unit(&mut edge, update);
    assert_current_title(&mut edge, DurabilityTier::Edge, row_uuid, "two");

    let delete = writer
        .commit_mergeable_unit(
            MergeableCommit::new("todos", row_uuid, 30)
                .parents(vec![update_tx.tx_id])
                .deletion(DeletionEvent::Deleted),
        )
        .unwrap()
        .1;
    let (delete_tx, _, _) = edge_accept_mergeable_unit(&mut edge, delete);
    assert!(edge
        .current_rows("todos", DurabilityTier::Edge)
        .unwrap()
        .is_empty());

    let restored_content = writer
        .commit_mergeable_unit(
            MergeableCommit::new("todos", row_uuid, 40)
                .parents(vec![delete_tx.tx_id])
                .cells(title_cells("restored")),
        )
        .unwrap()
        .1;
    let (restored_content_tx, _, _) = edge_accept_mergeable_unit(&mut edge, restored_content);
    assert!(edge
        .current_rows("todos", DurabilityTier::Edge)
        .unwrap()
        .is_empty());

    let restore = writer
        .commit_mergeable_unit(
            MergeableCommit::new("todos", row_uuid, 50)
                .parents(vec![restored_content_tx.tx_id])
                .deletion(DeletionEvent::Restored),
        )
        .unwrap()
        .1;
    edge_accept_mergeable_unit(&mut edge, restore);
    assert_current_title(&mut edge, DurabilityTier::Edge, row_uuid, "restored");
}

#[test]
fn edge_authority_accepts_large_value_blob_and_text_edit_writes() {
    let blob_schema = large_value_schema();
    let blob_row = row(0xea);
    let (_blob_writer_dir, mut blob_writer) =
        open_node_with_schema(node(0xea), blob_schema.clone());
    let (_blob_edge_dir, mut blob_edge) = open_node_with_schema(node(0xeb), blob_schema.clone());
    let blob_unit = commit_large_value_unit(
        &mut blob_writer,
        MergeableCommit::new("docs", blob_row, 10).cells(BTreeMap::from([(
            "body".to_owned(),
            Value::Bytes(b"edge blob".to_vec()),
        )])),
    );

    edge_accept_large_value_unit(&mut blob_edge, &blob_writer, blob_unit);
    assert_eq!(
        hydrated_large_value_cell(&mut blob_edge, &blob_schema.tables[0], "body"),
        b"edge blob".to_vec()
    );

    let text_schema = text_large_value_schema();
    let text_row = row(0xec);
    let (_text_writer_dir, mut text_writer) =
        open_node_with_schema(node(0xec), text_schema.clone());
    let (_text_edge_dir, mut text_edge) = open_node_with_schema(node(0xed), text_schema.clone());
    let text_unit = commit_large_value_edit_unit(
        &mut text_writer,
        LargeValueEditCommit::new("docs", text_row, "body", 10).insert(0, b"edge text"),
    );

    edge_accept_large_value_unit(&mut text_edge, &text_writer, text_unit);
    assert_eq!(
        hydrated_large_value_cell(&mut text_edge, &text_schema.tables[0], "body"),
        b"edge text".to_vec()
    );
}

#[test]
fn edge_authority_rejects_exclusive_and_catalogue_writes_loudly() {
    let (_edge_dir, mut edge) = open_node_with_schema(node(0xee), schema());
    let exclusive_tx = Transaction {
        tx_id: TxId::new(TxTime::from(10), node(0xee)),
        kind: TxKind::Exclusive,
        n_total_writes: 1,
        made_by: AuthorId::SYSTEM,
        permission_subject: None,
        base_snapshot: None,
        row_read_set: None,
        absent_read_set: None,
        predicate_read_set: None,
        user_metadata_json: None,
        source_branch: None,
        merge_strategy: None,
    };
    assert!(matches!(
        edge.ingest_edge_authority_mergeable_commit_unit(
            exclusive_tx,
            vec![version_record(
                row(0xee),
                Vec::new(),
                title_cells("exclusive"),
                None
            )],
            u64::MAX - SKEW_TOLERANCE_MS,
        ),
        Err(Error::UnsupportedCommitUnit(
            "edge authority only supports mergeable commit units"
        ))
    ));

    let evolved = SchemaVersion::new(catalogue_evolved_schema());
    assert!(matches!(
        edge.apply_sync_message(SyncMessage::PublishSchema {
            author: user(0xee),
            schema: Box::new(evolved),
        }),
        Err(Error::UnauthorizedCatalogueUpdate)
    ));
}
