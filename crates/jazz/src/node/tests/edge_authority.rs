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
    let outcome = edge
        .ingest_edge_authority_mergeable_commit_unit(
            tx.clone(),
            versions.clone(),
            u64::MAX - SKEW_TOLERANCE_MS,
        )
        .unwrap();
    let updates = settle_outcome(edge, outcome).unwrap();
    assert_eq!(
        updates,
        vec![SyncMessage::FateUpdate {
            tx_id: tx.tx_id,
            fate: Fate::Accepted,
            global_time: None,
            durability: Some(DurabilityTier::Edge),
        }]
    );
    (tx, versions, updates)
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
    let outcome = core
        .finalize_edge_accepted_mergeable_commit_unit_once(
            tx.clone(),
            versions,
            u64::MAX - SKEW_TOLERANCE_MS,
        )
        .unwrap();
    let [fate] = settle_outcome(core, outcome)
        .unwrap()
        .try_into()
        .unwrap();
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
    install_test_uuid_sub_claim(&mut writer, owner);
    install_test_uuid_sub_claim(&mut edge, owner);

    let unit = writer
        .commit_mergeable_unit_settled(
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
    edge.apply_sync_message_settled(fate).unwrap();

    let mut peer = PeerState::new();
    let update = peer.current_rows_update(&mut core, "todos").unwrap();
    register_whole_table_receiver(&mut reader, "todos");
    reader.apply_sync_message_settled(update).unwrap();
    assert_current_title(
        &mut reader,
        DurabilityTier::Global,
        row_uuid,
        "edge-visible",
    );
    assert_eq!(
        edge.transaction_state_settled(tx.tx_id).unwrap().2,
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
        .commit_mergeable_unit_settled(
            MergeableCommit::new("todos", row_uuid, 10).cells(title_cells("one")),
        )
        .unwrap()
        .1;
    let (first_tx, first_versions, _) = edge_accept_mergeable_unit(&mut edge, first);
    assert_current_title(&mut edge, DurabilityTier::Edge, row_uuid, "one");

    let second = writer
        .commit_mergeable_unit_settled(
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
        .commit_mergeable_unit_settled(
            MergeableCommit::new("todos", row_uuid, 10).cells(title_cells("one")),
        )
        .unwrap()
        .1;
    let (insert_tx, _, _) = edge_accept_mergeable_unit(&mut edge, insert);
    assert_current_title(&mut edge, DurabilityTier::Edge, row_uuid, "one");

    let update = writer
        .commit_mergeable_unit_settled(
            MergeableCommit::new("todos", row_uuid, 20)
                .parents(vec![insert_tx.tx_id])
                .cells(title_cells("two")),
        )
        .unwrap()
        .1;
    let (update_tx, _, _) = edge_accept_mergeable_unit(&mut edge, update);
    assert_current_title(&mut edge, DurabilityTier::Edge, row_uuid, "two");

    let delete = writer
        .commit_mergeable_unit_settled(
            MergeableCommit::new("todos", row_uuid, 30)
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
        .commit_mergeable_unit_settled(
            MergeableCommit::new("todos", row_uuid, 40)
                .parents(vec![update_tx.tx_id])
                .cells(title_cells("restored")),
        )
        .unwrap()
        .1;
    let (_restored_content_tx, _, _) = edge_accept_mergeable_unit(&mut edge, restored_content);
    assert!(edge
        .current_rows("todos", DurabilityTier::Edge)
        .unwrap()
        .is_empty());

    let restore = writer
        .commit_mergeable_unit_settled(
            MergeableCommit::new("todos", row_uuid, 50)
                .parents(vec![delete_tx.tx_id])
                .deletion(DeletionEvent::Restored),
        )
        .unwrap()
        .1;
    edge_accept_mergeable_unit(&mut edge, restore);
    assert_current_title(&mut edge, DurabilityTier::Edge, row_uuid, "restored");
}


#[test]
fn edge_authority_rejects_exclusive_and_catalogue_writes_loudly() {
    let (_edge_dir, mut edge) = open_node_with_schema(node(0xee), schema());
    let exclusive_tx = Transaction {
        tx_id: TxId::new(TxTime::from(10), node(0xee)),
        kind: TxKind::Exclusive,
        n_total_writes: 1,
        made_by: AuthorSubject::SYSTEM,
        permission_subject: None,
        base_snapshot: None,
        row_read_set: None,
        absent_read_set: None,
        predicate_read_set: None,
        user_metadata_json: None,
        contribution_merge: None,
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
        ).resolve(),
        Err(Error::UnsupportedCommitUnit(
            "edge authority only supports mergeable commit units"
        ))
    ));

    let evolved = SchemaVersion::new(catalogue_evolved_schema());
    let publication = edge.author_schema_lineage_publication(
        evolved.clone(),
        MigrationLens::new(
            schema().version_id(),
            evolved.id,
            vec![TableLens {
                source_table: "todos".to_owned(),
                target_table: "todos".to_owned(),
                ops: vec![LensOp::AddColumn {
                    column: "body".to_owned(),
                    default: Value::String(String::new()),
                }],
            }],
        ).expect("valid migration lens"),
        Vec::<String>::new(),
        Vec::<String>::new(),
    ).unwrap();
    assert!(matches!(
        edge.apply_sync_message_settled(SyncMessage::PublishSchemaWithLens {
            author: user(0xee),
            catalogue_seq: 1,
            publication: Box::new(publication.clone()),
        }),
        Err(Error::UnauthorizedCatalogueUpdate)
    ));
    edge.apply_trusted_catalogue_message_settled(SyncMessage::PublishSchemaWithLens {
        author: AuthorSubject::SYSTEM,
        catalogue_seq: 1,
        publication: Box::new(publication),
    })
    .unwrap();
    assert!(edge.catalogue_schemas().contains_key(&evolved.id));
}
