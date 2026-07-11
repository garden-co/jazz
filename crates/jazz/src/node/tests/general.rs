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
fn pilot-app_real_fixture_version_layouts_round_trip_all_storage_records() {
    fn fixture_schema() -> JazzSchema {
        let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../packages/jazz-tools/src/testing/fixtures/pilot-real/schema.native.bin");
        let bytes = std::fs::read(path).unwrap();
        postcard::from_bytes(&bytes).unwrap()
    }

    fn sample_value(column_type: &groove::schema::ColumnType, seed: u8) -> Value {
        match column_type {
            groove::schema::ColumnType::U8 => Value::U8(seed),
            groove::schema::ColumnType::U16 => Value::U16(u16::from(seed) * 17),
            groove::schema::ColumnType::U32 => Value::U32(u32::from(seed) * 65_537),
            groove::schema::ColumnType::U64 => Value::U64(u64::MAX - u64::from(seed)),
            groove::schema::ColumnType::F64 => Value::F64(f64::from(seed) + 0.5),
            groove::schema::ColumnType::Bool => Value::Bool(seed & 1 == 0),
            groove::schema::ColumnType::String => Value::String(format!("fixture-value-{seed}")),
            groove::schema::ColumnType::Bytes => Value::Bytes(vec![seed, seed.wrapping_add(1)]),
            groove::schema::ColumnType::Uuid => Value::Uuid(uuid::Uuid::from_bytes([seed; 16])),
            groove::schema::ColumnType::Enum(_) => Value::Enum(0),
            groove::schema::ColumnType::Tuple(members) => Value::Tuple(
                members
                    .iter()
                    .enumerate()
                    .map(|(idx, member)| sample_value(member, seed.wrapping_add(idx as u8 + 1)))
                    .collect(),
            ),
            groove::schema::ColumnType::Array(member) => Value::Array(vec![
                sample_value(member, seed.wrapping_add(1)),
                sample_value(member, seed.wrapping_add(2)),
            ]),
            groove::schema::ColumnType::Nullable(member) => {
                Value::Nullable(Some(Box::new(sample_value(member, seed.wrapping_add(1)))))
            }
        }
    }

    fn parts_for(table: &TableSchema, seed: u8, deletion: Option<DeletionEvent>) -> VersionRowParts {
        VersionRowParts {
            table: table.name.clone(),
            row_uuid: RowUuid(uuid::Uuid::from_bytes([seed; 16])),
            tx_node_alias: NodeAlias(u64::from(seed) + 10),
            schema_version_alias: SchemaVersionAlias(u64::from(seed) + 20),
            tx_time: TxTime::from(u64::from(seed) + 30),
            parents: vec![TxId::new(
                TxTime::from(u64::from(seed) + 1),
                node(seed.wrapping_add(1)),
            )],
            created_by: AuthorId(uuid::Uuid::from_bytes([seed.wrapping_add(2); 16])),
            created_at: TxTime::from(u64::from(seed) + 40),
            updated_by: AuthorId(uuid::Uuid::from_bytes([seed.wrapping_add(3); 16])),
            updated_at: TxTime::from(u64::from(seed) + 50),
            cells: table
                .columns
                .iter()
                .enumerate()
                .map(|(idx, column)| {
                    (
                        column.name.clone(),
                        sample_value(
                            &column.column_type,
                            seed.wrapping_add((idx as u8).wrapping_add(4)),
                        ),
                    )
                })
                .collect(),
            deletion,
        }
    }

    let schema = fixture_schema();
    assert!(!schema.tables.is_empty());
    for (idx, table) in schema.tables.iter().enumerate() {
        let seed = (idx as u8).wrapping_add(1);
        let content = VersionRow::from_parts_with_schema_version(
            table,
            parts_for(table, seed, None),
            None,
        )
        .unwrap();
        assert_eq!(content.record.descriptor().fields(), table.history_storage_table().record_schema().fields());
        let content_values = content.record.to_values().unwrap();
        assert_eq!(
            table
                .history_storage_table()
                .record_schema()
                .create(&content_values)
                .unwrap(),
            content.record.raw()
        );

        let current_values = global_current_values(table, &content, Some(GlobalSeq(7))).unwrap();
        let global_current_table = table.global_current_storage_tables().remove(0);
        global_current_table
            .record_schema()
            .create(&current_values)
            .unwrap();

        let deletion = VersionRow::from_parts_with_schema_version(
            table,
            parts_for(table, seed.wrapping_add(100), Some(DeletionEvent::Deleted)),
            None,
        )
        .unwrap();
        assert_eq!(deletion.record.descriptor().fields(), table.register_storage_table().record_schema().fields());
        let deletion_values = deletion.record.to_values().unwrap();
        assert_eq!(
            table
                .register_storage_table()
                .record_schema()
                .create(&deletion_values)
                .unwrap(),
            deletion.record.raw()
        );

        let register_current_values =
            register_global_current_values(&deletion, Some(GlobalSeq(8)));
        let register_global_current_table = table.global_current_storage_tables().remove(1);
        register_global_current_table
            .record_schema()
            .create(&register_current_values)
            .unwrap();
    }
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
fn authoring_stamps_explicit_child_after_parent_time() {
    let (_temp_dir, mut core) = open_node();
    let parent = TxId::new(TxTime::from(10_000), node(0x77));
    let child = core
        .commit_mergeable(
            MergeableCommit::new("todos", row(0x71), 1)
                .parents(vec![parent])
                .cells(title_cells("child")),
        )
        .unwrap();

    assert!(
        child.time > parent.time,
        "author must stamp explicit child after parent: child={child:?}, parent={parent:?}"
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

    assert_eq!(ahead_current_row_count(&mut node, "todos"), 1);
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
fn global_fate_cleans_ahead_current_overlay() {
    let (_temp_dir, mut node) = open_node();
    let row = row(0xe3);
    let tx_id = node
        .commit_mergeable(
            MergeableCommit::new("todos", row, 10).cells(title_cells("globally accepted")),
        )
        .unwrap();
    assert_eq!(ahead_current_row_count(&mut node, "todos"), 1);

    node.apply_fate_update(
        tx_id,
        Fate::Accepted,
        Some(GlobalSeq(1)),
        Some(DurabilityTier::Global),
    )
    .unwrap();

    assert_eq!(ahead_current_row_count(&mut node, "todos"), 0);
    assert_eq!(
        node.current_rows("todos", DurabilityTier::Local)
            .unwrap()
            .into_iter()
            .map(current_row_pair)
            .collect::<BTreeMap<_, _>>(),
        BTreeMap::from([(row, title_cells("globally accepted"))])
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
    let Some(Value::Bytes(handle)) = rows[0].cell_at(0) else {
        panic!("expected large-value handle");
    };
    assert_eq!(node.hydrate_large_value_handle(&handle).unwrap(), body);
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
