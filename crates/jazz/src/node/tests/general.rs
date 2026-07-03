#[test]
fn parent_tuple_encoding_matches_tx_id_tuple_order() {
    let tx_id = TxId::new(TxTime::from(0x0102_0304_0506), node(0x12));
    let parent_value = Value::Tuple(vec![Value::U64(tx_id.time.0), Value::Uuid(tx_id.node.0)]);
    let descriptor = groove::records::RecordDescriptor::new([(
        "parents",
        groove::records::ValueType::Array(Box::new(groove::records::ValueType::Tuple(vec![
            groove::records::ValueType::U64,
            groove::records::ValueType::Uuid,
        ]))),
    )]);
    let record = descriptor
        .create(&[Value::Array(vec![parent_value.clone()])])
        .unwrap();

    let mut expected = Vec::new();
    expected.extend_from_slice(&tx_id.time.0.to_be_bytes());
    expected.extend_from_slice(tx_id.node.as_bytes());
    assert_eq!(record, expected);
    assert_eq!(
        descriptor.bind(&record).get_array_element(0, 0).unwrap(),
        parent_value
    );
}

#[test]
fn lowered_record_wrapper_field_indexes_match_open_descriptors() {
    let schema = two_column_schema();
    debug_assert_lowered_layouts(&schema);
    let (_temp_dir, mut node) = open_node_with_schema(node(0x19), schema.clone());
    node.commit_mergeable(
        MergeableCommit::new("todos", row(0x19), 10).cells(BTreeMap::from([
            ("title".to_owned(), Value::String("layout".to_owned())),
            ("body".to_owned(), Value::String("descriptor".to_owned())),
        ])),
    )
    .unwrap();

    let rows = node.current_rows("todos", DurabilityTier::Local).unwrap();
    assert_eq!(rows[0].row_uuid(), row(0x19));
    assert_eq!(rows[0].cell_at(0), Some(Value::String("layout".to_owned())));
    assert_eq!(
        rows[0].cell_at(1),
        Some(Value::String("descriptor".to_owned()))
    );
}

#[test]
fn mergeable_commits_persist_transaction_and_history_rows() {
    let (_temp_dir, mut node) = open_node();
    let row = row(7);
    let tx = node
        .commit_mergeable(
            MergeableCommit::new("todos", row, 10).cells(BTreeMap::from([(
                "title".to_owned(),
                "write tests".to_owned(),
            )])),
        )
        .unwrap();

    assert_eq!(tx.time, TxTime::from(10));
    assert_eq!(
        node.visible_current_cells("todos", row)
            .unwrap()
            .unwrap()
            .get("title")
            .unwrap(),
        &v("write tests")
    );
    let mut database = node.into_database();
    assert!(
        !database
            .query(select_all("jazz_transactions"))
            .unwrap()
            .is_empty()
    );
    assert!(
        !database
            .query(select_all("jazz_todos_history"))
            .unwrap()
            .is_empty()
    );
}
#[test]
fn deletion_register_hides_and_restore_reveals_current_content() {
    let (_temp_dir, mut node) = open_node();
    let row = row(7);
    node.commit_mergeable(MergeableCommit::new("todos", row, 10).cells(title_cells("base")))
        .unwrap();
    node.commit_mergeable(MergeableCommit::new("todos", row, 12).deletion(DeletionEvent::Deleted))
        .unwrap();

    assert!(node.visible_current_cells("todos", row).unwrap().is_none());

    node.commit_mergeable(MergeableCommit::new("todos", row, 13).cells(title_cells("revived")))
        .unwrap();
    assert!(node.visible_current_cells("todos", row).unwrap().is_none());

    node.commit_mergeable(MergeableCommit::new("todos", row, 14).deletion(DeletionEvent::Restored))
        .unwrap();

    assert_eq!(
        node.visible_current_cells("todos", row)
            .unwrap()
            .unwrap()
            .get("title")
            .unwrap(),
        &v("revived")
    );
    assert_eq!(
        node.current_rows("todos", DurabilityTier::Local)
            .unwrap()
            .into_iter()
            .map(|row| row.cell(&schema().tables[0], "title").unwrap().to_owned())
            .collect::<Vec<_>>(),
        [v("revived")]
    );
    assert!(
        node.current_rows("todos", DurabilityTier::Global)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn durability_tier_ladder_orders_edge_between_local_and_global() {
    assert!(DurabilityTier::None < DurabilityTier::Local);
    assert!(DurabilityTier::Local < DurabilityTier::Edge);
    assert!(DurabilityTier::Edge < DurabilityTier::Global);
}

#[test]
fn edge_current_rows_exclude_purely_local_pending_writes() {
    let (_temp_dir, mut node) = open_node();
    let row = row(0xe1);
    node.commit_mergeable(
        MergeableCommit::new("todos", row, 10).cells(title_cells("local only")),
    )
    .unwrap();

    assert_eq!(
        node.current_rows("todos", DurabilityTier::Local)
            .unwrap()
            .into_iter()
            .map(|row| row.row_uuid())
            .collect::<Vec<_>>(),
        vec![row]
    );
    assert!(
        node.current_rows("todos", DurabilityTier::Edge)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn edge_current_rows_include_edge_accepted_ahead_versions() {
    let (_temp_dir, mut node) = open_node();
    let row = row(0xe2);
    let tx_id = node
        .commit_mergeable(
            MergeableCommit::new("todos", row, 10).cells(title_cells("edge accepted")),
        )
        .unwrap();

    // E1: edge-accept produced directly; E2 wires the acceptance path.
    node.apply_fate_update(tx_id, Fate::Accepted, None, Some(DurabilityTier::Edge))
        .unwrap();

    assert_eq!(
        node.current_rows("todos", DurabilityTier::Edge)
            .unwrap()
            .into_iter()
            .map(|row| row.row_uuid())
            .collect::<Vec<_>>(),
        vec![row]
    );
    assert!(
        node.current_rows("todos", DurabilityTier::Global)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn writer_subscription_reads_own_pending_at_local_tier() {
    let (_client_dir, mut client) = open_node_with_uuid(node(1));
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    let mut peer = PeerState::new();
    let row = row(7);
    let (tx_id, unit) = client
        .commit_mergeable_unit(
            MergeableCommit::new("todos", row, 10).cells(BTreeMap::from([(
                "title".to_owned(),
                "optimistic".to_owned(),
            )])),
        )
        .unwrap();

    assert_eq!(
        client
            .subscription_current_rows("todos", DurabilityTier::Local)
            .unwrap(),
        vec![(row, title_cells("optimistic"))]
    );
    assert!(
        client
            .subscription_current_rows("todos", DurabilityTier::Global)
            .unwrap()
            .is_empty()
    );

    let SyncMessage::CommitUnit { tx, versions } = unit else {
        panic!("expected commit unit");
    };
    let [fate] = core
        .ingest_commit_unit(tx, versions, u64::MAX - SKEW_TOLERANCE_MS)
        .unwrap()
        .try_into()
        .unwrap();
    client.apply_sync_message(fate).unwrap();
    assert_eq!(
        client.transaction_state(tx_id).unwrap(),
        (Fate::Accepted, Some(GlobalSeq(1)), DurabilityTier::Global)
    );

    let update = peer.current_rows_update(&mut core, "todos").unwrap();
    client.apply_sync_message(update).unwrap();
    assert_eq!(
        client
            .subscription_current_rows("todos", DurabilityTier::Global)
            .unwrap(),
        vec![(row, title_cells("optimistic"))]
    );
}

#[test]
fn blob_column_round_trips_opaque_bytes() {
    let (_temp_dir, mut node) = open_node_with_schema(
        node(0x44),
        JazzSchema::new([TableSchema::new(
            "notes",
            [crate::schema::ColumnSchema::blob("body")],
        )]),
    );
    let row = row(0x44);
    let body = (0..16_384)
        .map(|idx| ((idx * 31 + 7) % 251) as u8)
        .collect::<Vec<_>>();

    let tx_id = node
        .commit_mergeable(
            MergeableCommit::new("notes", row, 10).cells(BTreeMap::from([(
                "body".to_owned(),
                Value::Bytes(body.clone()),
            )])),
        )
        .unwrap();
    let versions = node.query_versions_for_tx(tx_id).unwrap();
    let payload = versions[0]
        .cell(&node.catalogue.schema.tables[0].clone(), "body")
        .unwrap()
        .unwrap();
    let Value::Bytes(payload) = payload else {
        panic!("expected encoded text op payload");
    };
    let ops = crate::node::text_oplog::decode(&payload).unwrap();
    let [
        crate::node::text_oplog::Op::Insert {
            content: crate::node::text_oplog::Content::Ref(extent),
            ..
        },
    ] = ops.as_slice()
    else {
        panic!("expected extent-backed insert op");
    };
    assert_eq!(node.content_store().read(extent).unwrap(), body);

    let rows = node.current_rows("notes", DurabilityTier::Local).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].cell_at(0), Some(Value::Bytes(body)));
}
#[test]
fn late_lower_hlc_child_is_rejected_at_admission() {
    let (_dir, mut core) = open_node_with_uuid(node(9));
    let row = row(7);
    let parent = TxId::new(TxTime::from(200), node(1));
    let child = TxId::new(TxTime::from(50), node(1));

    let [parent_fate] = core
        .ingest_commit_unit(
            Transaction {
                tx_id: parent,
                kind: TxKind::Mergeable,
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
            },
            vec![version_record(row, Vec::new(), title_cells("parent"), None)],
            u64::MAX - SKEW_TOLERANCE_MS,
        )
        .unwrap()
        .try_into()
        .unwrap();
    assert!(matches!(
        parent_fate,
        SyncMessage::FateUpdate {
            fate: Fate::Accepted,
            ..
        }
    ));

    let [child_fate] = core
        .ingest_commit_unit(
            Transaction {
                tx_id: child,
                kind: TxKind::Mergeable,
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
            },
            vec![version_record(
                row,
                vec![parent],
                title_cells("child"),
                None,
            )],
            u64::MAX - SKEW_TOLERANCE_MS,
        )
        .unwrap()
        .try_into()
        .unwrap();
    assert_eq!(
        child_fate,
        SyncMessage::FateUpdate {
            tx_id: child,
            fate: Fate::Rejected(RejectionReason::CausalityViolation),
            global_seq: None,
            durability: None,
        }
    );
    assert!(
        core.row_history("todos", row)
            .unwrap()
            .iter()
            .all(|entry| entry.tx_id() != child)
    );
    assert_eq!(
        core.transaction_record(child).unwrap().fate,
        Fate::Rejected(RejectionReason::CausalityViolation)
    );
}
#[test]
fn unlawful_child_with_known_parent_rejects_before_global_state() {
    let (_dir, mut core) = open_node_with_uuid(node(9));
    let row = row(7);
    let parent = TxId::new(TxTime::from(400), node(1));
    let child = TxId::new(TxTime::from(100), node(1));

    let parent_state = core
        .ingest_commit_unit(
            Transaction {
                tx_id: parent,
                kind: TxKind::Mergeable,
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
            },
            vec![version_record(row, Vec::new(), title_cells("parent"), None)],
            u64::MAX - SKEW_TOLERANCE_MS,
        )
        .unwrap();
    assert!(matches!(
        parent_state.as_slice(),
        [SyncMessage::FateUpdate {
            fate: Fate::Accepted,
            global_seq: Some(_),
            ..
        }]
    ));

    let child_state = core
        .ingest_commit_unit(
            Transaction {
                tx_id: child,
                kind: TxKind::Mergeable,
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
            },
            vec![version_record(
                row,
                vec![parent],
                title_cells("child"),
                None,
            )],
            u64::MAX - SKEW_TOLERANCE_MS,
        )
        .unwrap();
    assert_eq!(
        child_state,
        vec![SyncMessage::FateUpdate {
            tx_id: child,
            fate: Fate::Rejected(RejectionReason::CausalityViolation),
            global_seq: None,
            durability: None,
        }]
    );
    assert_eq!(
        global_winner_tx(&mut core, "todos", row, VersionLayer::Content),
        Some(parent)
    );
    assert_eq!(
        core.current_rows("todos", DurabilityTier::Global).unwrap(),
        vec![(row, title_cells("parent"))]
    );
}
