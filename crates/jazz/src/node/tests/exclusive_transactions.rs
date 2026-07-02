#[test]
fn exclusive_base_snapshot_rejects_foreign_dots_at_creation() {
    let owner = node(1);
    let own_dot = TxId::new(TxTime::from(10), owner);
    let foreign_dot = TxId::new(TxTime::from(11), node(2));

    let snapshot =
        crate::tx::Snapshot::exclusive_base(owner, GlobalSeq(3), TxTime::from(12), vec![own_dot])
            .unwrap();
    assert_eq!(snapshot.dots, vec![own_dot]);

    assert_eq!(
        crate::tx::Snapshot::exclusive_base(
            owner,
            GlobalSeq(3),
            TxTime::from(12),
            vec![foreign_dot],
        )
        .unwrap_err(),
        "exclusive base snapshot cannot include foreign dots"
    );
}

#[test]
fn exclusive_tx_snapshot_read_ignores_newer_commits_after_open() {
    let (_temp_dir, mut node) = open_node();
    let row = row(7);
    let base = node
        .commit_mergeable(MergeableCommit::new("todos", row, 10).cells(title_cells("base")))
        .unwrap();
    let tx_id = node.open_exclusive().unwrap();

    node.commit_mergeable(MergeableCommit::new("todos", row, 11).cells(title_cells("newer")))
        .unwrap();

    assert_eq!(
        node.tx_read(tx_id, "todos", row).unwrap(),
        Some(title_cells("base"))
    );
    assert_eq!(
        node.open_tx(tx_id).unwrap().row_reads,
        vec![RowRead {
            table: "todos".to_owned(),
            row_uuid: row,
            version: base,
        }]
    );
}
#[test]
fn exclusive_tx_reads_own_pending_writes() {
    let (_temp_dir, mut node) = open_node();
    let existing = row(7);
    let created = row(8);
    node.commit_mergeable(MergeableCommit::new("todos", existing, 10).cells(title_cells("base")))
        .unwrap();
    let tx_id = node.open_exclusive().unwrap();

    node.tx_write(tx_id, "todos", existing, title_cells("pending"), None)
        .unwrap();
    node.tx_write(tx_id, "todos", created, title_cells("created"), None)
        .unwrap();

    assert_eq!(
        node.tx_read(tx_id, "todos", existing).unwrap(),
        Some(title_cells("pending"))
    );
    assert_eq!(
        node.tx_current_rows(tx_id, "todos").unwrap(),
        vec![
            (existing, title_cells("pending")),
            (created, title_cells("created")),
        ]
    );
    let predicate_shape = crate::query::Query::from("todos")
        .validate(&schema())
        .unwrap();
    let predicate_binding = predicate_shape
        .bind(std::collections::BTreeMap::new())
        .unwrap();
    assert_eq!(
        node.open_tx(tx_id).unwrap().predicate_reads,
        vec![PredicateRead {
            table: "todos".to_owned(),
            shape_id: predicate_shape.shape_id(),
            shape: predicate_shape.query().clone(),
            binding_id: predicate_binding.binding_id(),
            binding_values: predicate_binding.values().clone(),
        }]
    );
}

#[test]
fn exclusive_tx_pending_writes_overlay_snapshot_for_point_and_table_reads() {
    let (_temp_dir, mut node) = open_node();
    let existing = row(7);
    let created = row(8);
    node.commit_mergeable(MergeableCommit::new("todos", existing, 10).cells(title_cells("base")))
        .unwrap();
    let tx_id = node.open_exclusive().unwrap();

    node.tx_write(tx_id, "todos", existing, title_cells("pending"), None)
        .unwrap();
    node.tx_write(tx_id, "todos", created, title_cells("created"), None)
        .unwrap();

    assert_eq!(
        node.tx_read(tx_id, "todos", existing).unwrap(),
        Some(title_cells("pending"))
    );
    assert_eq!(
        node.tx_current_rows(tx_id, "todos").unwrap(),
        vec![
            (existing, title_cells("pending")),
            (created, title_cells("created")),
        ]
    );
}

#[test]
fn tx_read_records_present_and_absent_snapshot_reads() {
    let (_temp_dir, mut node) = open_node();
    let present = row(7);
    let absent = row(8);
    let version = node
        .commit_mergeable(MergeableCommit::new("todos", present, 10).cells(title_cells("base")))
        .unwrap();
    let tx_id = node.open_exclusive().unwrap();

    assert_eq!(
        node.tx_read(tx_id, "todos", present).unwrap(),
        Some(title_cells("base"))
    );
    assert_eq!(node.tx_read(tx_id, "todos", absent).unwrap(), None);

    let open = node.open_tx(tx_id).unwrap();
    assert_eq!(
        open.row_reads,
        vec![RowRead {
            table: "todos".to_owned(),
            row_uuid: present,
            version,
        }]
    );
    assert_eq!(
        open.absent_reads,
        vec![AbsentRead {
            table: "todos".to_owned(),
            row_uuid: absent,
        }]
    );
}

#[test]
fn tx_read_parent_cache_is_invalidated_by_same_row_write_without_changing_read_set() {
    let (_temp_dir, mut node) = open_node();
    let row = row(7);
    let base = node
        .commit_mergeable(MergeableCommit::new("todos", row, 10).cells(title_cells("base")))
        .unwrap();
    let tx_id = node.open_exclusive().unwrap();

    assert_eq!(
        node.tx_read(tx_id, "todos", row).unwrap(),
        Some(title_cells("base"))
    );
    assert!(
        node.open_tx(tx_id)
            .unwrap()
            .base_snapshot_rows
            .contains_key(&("todos".to_owned(), row))
    );

    node.tx_write(tx_id, "todos", row, title_cells("updated"), None)
        .unwrap();
    assert!(
        !node
            .open_tx(tx_id)
            .unwrap()
            .base_snapshot_rows
            .contains_key(&("todos".to_owned(), row))
    );

    let (_exclusive, unit) = node
        .commit_exclusive(tx_id, AuthorId::SYSTEM, 11)
        .unwrap();
    let SyncMessage::CommitUnit { tx, versions } = unit else {
        panic!("expected exclusive commit unit");
    };
    assert_eq!(
        tx.row_read_set.as_deref(),
        Some(
            [RowRead {
                table: "todos".to_owned(),
                row_uuid: row,
                version: base,
            }]
            .as_slice()
        )
    );
    assert_eq!(versions.len(), 1);
    assert_eq!(versions[0].parents(), vec![base]);
}

#[test]
fn exclusive_tx_snapshot_applies_deletion_register() {
    let (_temp_dir, mut node) = open_node();
    let row = row(7);
    node.commit_mergeable(MergeableCommit::new("todos", row, 10).cells(title_cells("base")))
        .unwrap();
    let deleted = node
        .commit_mergeable(MergeableCommit::new("todos", row, 11).deletion(DeletionEvent::Deleted))
        .unwrap();
    let tx_id = node.open_exclusive().unwrap();

    node.commit_mergeable(MergeableCommit::new("todos", row, 12).deletion(DeletionEvent::Restored))
        .unwrap();

    assert_eq!(node.tx_read(tx_id, "todos", row).unwrap(), None);
    assert_eq!(
        node.open_tx(tx_id).unwrap().row_reads,
        vec![RowRead {
            table: "todos".to_owned(),
            row_uuid: row,
            version: deleted,
        }]
    );

    node.tx_write(
        tx_id,
        "todos",
        row,
        BTreeMap::<String, Value>::new(),
        Some(DeletionEvent::Restored),
    )
    .unwrap();
    assert_eq!(
        node.tx_read(tx_id, "todos", row).unwrap(),
        Some(title_cells("base"))
    );
}
#[test]
fn exclusive_tx_open_state_is_invisible_outside_transaction() {
    let (_temp_dir, mut node) = open_node();
    let row = row(7);
    let tx_id = node.open_exclusive().unwrap();
    node.tx_write(tx_id, "todos", row, title_cells("buffered"), None)
        .unwrap();

    assert!(node
        .current_rows("todos", DurabilityTier::Local)
        .unwrap()
        .is_empty());
    assert!(node.view_update_for_current_rows("todos").is_ok());
    assert!(node.abandon_tx(tx_id).is_ok());
    assert!(matches!(
        node.tx_read(tx_id, "todos", row).unwrap_err(),
        Error::MissingOpenTx(missing) if missing == tx_id
    ));
}
#[test]
fn exclusive_snapshot_global_base_uses_contiguous_global_watermark() {
    let (_temp_dir, mut reader) = open_node_with_uuid(node(3));

    for (seq, row_byte) in [(1, 1), (3, 3)] {
        let tx_id = TxId::new(TxTime::new(10 + seq, 0), node(9));
        reader
            .ingest_known_transaction(
                Transaction {
                    tx_id,
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
                },
                vec![version_record(
                    row(row_byte),
                    Vec::new(),
                    title_cells(format!("seq-{seq}")),
                    None,
                )],
                Fate::Accepted,
                Some(GlobalSeq(seq)),
                DurabilityTier::Global,
            )
            .unwrap();
    }

    let gapped = reader.open_exclusive().unwrap();
    assert_eq!(
        reader.open_tx(gapped).unwrap().base_snapshot.global_base,
        GlobalSeq(1)
    );

    let tx_id = TxId::new(TxTime::from(12), node(9));
    reader
        .ingest_known_transaction(
            Transaction {
                tx_id,
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
            },
            vec![version_record(
                row(2),
                Vec::new(),
                title_cells("seq-2"),
                None,
            )],
            Fate::Accepted,
            Some(GlobalSeq(2)),
            DurabilityTier::Global,
        )
        .unwrap();

    let contiguous = reader.open_exclusive().unwrap();
    assert_eq!(
        reader
            .open_tx(contiguous)
            .unwrap()
            .base_snapshot
            .global_base,
        GlobalSeq(3)
    );
}
#[test]
fn exclusive_commit_accepts_clean_end_to_end() {
    let (_client_dir, mut client) = open_node_with_uuid(node(1));
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    let row = row(7);
    commit_mergeable_global(
        &mut client,
        &mut core,
        MergeableCommit::new("todos", row, 10).cells(title_cells("base")),
    );
    let tx_id = client.open_exclusive().unwrap();
    client
        .tx_write(tx_id, "todos", row, title_cells("exclusive"), None)
        .unwrap();
    let (tx_id, unit) = client
        .commit_exclusive(tx_id, AuthorId::SYSTEM, 11)
        .unwrap();
    assert_eq!(
        client
            .current_rows("todos", DurabilityTier::Local)
            .unwrap()
            .into_iter()
            .map(current_row_pair)
            .collect::<BTreeMap<_, _>>(),
        BTreeMap::from([(row, title_cells("exclusive"))])
    );

    let [fate] = core.apply_sync_message(unit).unwrap().try_into().unwrap();
    let SyncMessage::FateUpdate {
        fate: accepted,
        global_seq,
        ..
    } = &fate
    else {
        panic!("expected fate update");
    };
    assert_eq!(accepted, &Fate::Accepted);
    assert_eq!(*global_seq, Some(GlobalSeq(2)));
    client.apply_sync_message(fate).unwrap();
    assert_eq!(
        client.transaction_state(tx_id).unwrap(),
        (Fate::Accepted, Some(GlobalSeq(2)), DurabilityTier::Global)
    );
    assert_eq!(
        core.current_rows("todos", DurabilityTier::Global)
            .unwrap()
            .into_iter()
            .map(current_row_pair)
            .collect::<BTreeMap<_, _>>(),
        BTreeMap::from([(row, title_cells("exclusive"))])
    );
}
#[test]
fn exclusive_row_read_conflict_rejects_and_client_restores_old_value() {
    let (_client_dir, mut client) = open_node_with_uuid(node(1));
    let (_other_dir, mut other) = open_node_with_uuid(node(2));
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    let row = row(7);
    commit_mergeable_global(
        &mut client,
        &mut core,
        MergeableCommit::new("todos", row, 10).cells(title_cells("base")),
    );
    let tx_id = client.open_exclusive().unwrap();
    assert_eq!(
        client.tx_read(tx_id, "todos", row).unwrap(),
        Some(title_cells("base"))
    );
    commit_mergeable_global(
        &mut other,
        &mut core,
        MergeableCommit::new("todos", row, 12).cells(title_cells("winner")),
    );
    client
        .tx_write(tx_id, "todos", row, title_cells("loser"), None)
        .unwrap();
    let (_tx_id, unit) = client
        .commit_exclusive(tx_id, AuthorId::SYSTEM, 13)
        .unwrap();
    let [fate] = core.apply_sync_message(unit).unwrap().try_into().unwrap();
    let SyncMessage::FateUpdate { fate: rejected, .. } = &fate else {
        panic!("expected fate update");
    };
    assert_eq!(
        rejected,
        &Fate::Rejected(RejectionReason::ExclusiveConflict)
    );
    client.apply_sync_message(fate).unwrap();
    assert_eq!(
        client
            .current_rows("todos", DurabilityTier::Local)
            .unwrap()
            .into_iter()
            .map(current_row_pair)
            .collect::<BTreeMap<_, _>>(),
        BTreeMap::from([(row, title_cells("base"))])
    );
}
#[test]
fn exclusive_predicate_phantom_conflict_rejects() {
    let (_client_dir, mut client) = open_node_with_uuid(node(1));
    let (_other_dir, mut other) = open_node_with_uuid(node(2));
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    let tx_id = client.open_exclusive().unwrap();
    assert!(client.tx_current_rows(tx_id, "todos").unwrap().is_empty());
    commit_mergeable_global(
        &mut other,
        &mut core,
        MergeableCommit::new("todos", row(1), 10).cells(title_cells("phantom")),
    );
    client
        .tx_write(tx_id, "todos", row(2), title_cells("mine"), None)
        .unwrap();
    let (_tx_id, unit) = client
        .commit_exclusive(tx_id, AuthorId::SYSTEM, 11)
        .unwrap();
    let [fate] = core.apply_sync_message(unit).unwrap().try_into().unwrap();
    let SyncMessage::FateUpdate { fate, .. } = fate else {
        panic!("expected fate update");
    };
    assert_eq!(fate, Fate::Rejected(RejectionReason::ExclusiveConflict));
}

#[test]
fn exclusive_whole_table_predicate_ignores_other_table_changes() {
    let schema = JazzSchema::new([
        TableSchema::new("todos", [ColumnSchema::new("title", ColumnType::String)]),
        TableSchema::new("notes", [ColumnSchema::new("title", ColumnType::String)]),
    ]);
    let (_client_dir, mut client) = open_node_with_schema(node(1), schema.clone());
    let (_other_dir, mut other) = open_node_with_schema(node(2), schema.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);

    let tx_id = client.open_exclusive().unwrap();
    assert!(client.tx_current_rows(tx_id, "todos").unwrap().is_empty());
    commit_mergeable_global(
        &mut other,
        &mut core,
        MergeableCommit::new("notes", row(1), 10).cells(title_cells("other table")),
    );
    client
        .tx_write(tx_id, "todos", row(2), title_cells("mine"), None)
        .unwrap();
    let (_tx_id, unit) = client
        .commit_exclusive(tx_id, AuthorId::SYSTEM, 11)
        .unwrap();
    let [fate] = core.apply_sync_message(unit).unwrap().try_into().unwrap();
    let SyncMessage::FateUpdate { fate, .. } = fate else {
        panic!("expected fate update");
    };
    assert_eq!(fate, Fate::Accepted);
}

#[test]
fn exclusive_filtered_shape_phantom_conflict_rejects() {
    let (_client_dir, mut client) = open_node_with_uuid(node(1));
    let (_other_dir, mut other) = open_node_with_uuid(node(2));
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    let shape = crate::query::Query::from("todos")
        .filter(crate::query::eq(
            crate::query::col("title"),
            crate::query::lit("watched"),
        ))
        .validate(&schema())
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    register_shape_binding(&mut core, &shape, &binding);

    let tx_id = client.open_exclusive().unwrap();
    assert!(client.tx_query(tx_id, &shape, &binding).unwrap().is_empty());
    commit_mergeable_global(
        &mut other,
        &mut core,
        MergeableCommit::new("todos", row(1), 10).cells(title_cells("watched")),
    );
    client
        .tx_write(tx_id, "todos", row(2), title_cells("mine"), None)
        .unwrap();
    let (_tx_id, unit) = client
        .commit_exclusive(tx_id, AuthorId::SYSTEM, 11)
        .unwrap();
    let [fate] = core.apply_sync_message(unit).unwrap().try_into().unwrap();
    let SyncMessage::FateUpdate { fate, .. } = fate else {
        panic!("expected fate update");
    };
    assert_eq!(fate, Fate::Rejected(RejectionReason::ExclusiveConflict));
}
#[test]
fn exclusive_filtered_shape_ignores_irrelevant_changes() {
    let (_client_dir, mut client) = open_node_with_uuid(node(1));
    let (_other_dir, mut other) = open_node_with_uuid(node(2));
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    let shape = crate::query::Query::from("todos")
        .filter(crate::query::eq(
            crate::query::col("title"),
            crate::query::lit("watched"),
        ))
        .validate(&schema())
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    register_shape_binding(&mut core, &shape, &binding);

    let tx_id = client.open_exclusive().unwrap();
    assert!(client.tx_query(tx_id, &shape, &binding).unwrap().is_empty());
    commit_mergeable_global(
        &mut other,
        &mut core,
        MergeableCommit::new("todos", row(1), 10).cells(title_cells("irrelevant")),
    );
    client
        .tx_write(tx_id, "todos", row(2), title_cells("mine"), None)
        .unwrap();
    let (_tx_id, unit) = client
        .commit_exclusive(tx_id, AuthorId::SYSTEM, 11)
        .unwrap();
    let [fate] = core.apply_sync_message(unit).unwrap().try_into().unwrap();
    let SyncMessage::FateUpdate { fate, .. } = fate else {
        panic!("expected fate update");
    };
    assert_eq!(fate, Fate::Accepted);
}
#[test]
fn exclusive_shape_predicate_is_binding_sensitive() {
    let author_a = user(0xa1);
    let author_b = user(0xb2);
    for (node_base, changed_owner, expected) in [
        (1, author_b, Fate::Accepted),
        (
            5,
            author_a,
            Fate::Rejected(RejectionReason::ExclusiveConflict),
        ),
    ] {
        let schema = owner_policy_schema();
        let (_client_dir, mut client) = open_node_with_schema(node(node_base), schema.clone());
        let (_other_dir, mut other) = open_node_with_schema(node(node_base + 1), schema.clone());
        let (_core_dir, mut core) = open_node_with_schema(node(node_base + 2), schema.clone());
        let shape = crate::query::Query::from("todos")
            .filter(crate::query::eq(
                crate::query::col("owner"),
                crate::query::param("owner"),
            ))
            .validate(&schema)
            .unwrap();
        let binding_a = shape
            .bind(BTreeMap::from([(
                "owner".to_owned(),
                Value::Uuid(author_a.0),
            )]))
            .unwrap();
        register_shape_binding(&mut core, &shape, &binding_a);

        let tx_id = client.open_exclusive().unwrap();
        assert!(client
            .tx_query(tx_id, &shape, &binding_a)
            .unwrap()
            .is_empty());
        commit_mergeable_global(
            &mut other,
            &mut core,
            MergeableCommit::new("todos", row(node_base), 10)
                .made_by(changed_owner)
                .cells(owner_cells(changed_owner, "changed")),
        );
        client
            .tx_write(
                tx_id,
                "todos",
                row(node_base + 10),
                owner_cells(author_a, "mine"),
                None,
            )
            .unwrap();
        let (_tx_id, unit) = client.commit_exclusive(tx_id, author_a, 11).unwrap();
        let [fate] = core.apply_sync_message(unit).unwrap().try_into().unwrap();
        let SyncMessage::FateUpdate { fate, .. } = fate else {
            panic!("expected fate update");
        };
        assert_eq!(fate, expected);
    }
}
#[test]
fn exclusive_shape_predicate_validation_uses_inline_shape_without_registration() {
    let author_a = user(0xa1);
    let schema = owner_policy_schema();
    let (_client_dir, mut client) = open_node_with_schema(node(1), schema.clone());
    let (_other_dir, mut other) = open_node_with_schema(node(2), schema.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema.clone());
    let shape = crate::query::Query::from("todos")
        .filter(crate::query::eq(
            crate::query::col("owner"),
            crate::query::param("owner"),
        ))
        .validate(&schema)
        .unwrap();
    let binding_a = shape
        .bind(BTreeMap::from([(
            "owner".to_owned(),
            Value::Uuid(author_a.0),
        )]))
        .unwrap();

    let tx_id = client.open_exclusive().unwrap();
    assert!(client
        .tx_query(tx_id, &shape, &binding_a)
        .unwrap()
        .is_empty());
    commit_mergeable_global(
        &mut other,
        &mut core,
        MergeableCommit::new("todos", row(1), 10)
            .made_by(author_a)
            .cells(owner_cells(author_a, "phantom")),
    );
    client
        .tx_write(tx_id, "todos", row(2), owner_cells(author_a, "mine"), None)
        .unwrap();
    let (_tx_id, unit) = client.commit_exclusive(tx_id, author_a, 11).unwrap();
    let [fate] = core.apply_sync_message(unit).unwrap().try_into().unwrap();
    let SyncMessage::FateUpdate { fate, .. } = fate else {
        panic!("expected fate update");
    };
    assert_eq!(fate, Fate::Rejected(RejectionReason::ExclusiveConflict));
}
#[test]
fn district_scoped_predicate_rejects_same_district_phantom_only() {
    fn orders_schema() -> JazzSchema {
        JazzSchema::new([TableSchema::new(
            "orders",
            [
                ColumnSchema::new("district", ColumnType::Uuid),
                ColumnSchema::new("orderNumber", ColumnType::U64),
                ColumnSchema::new("delivered", ColumnType::Bool),
            ],
        )])
    }

    fn order_cells(
        district: RowUuid,
        order_number: u64,
        delivered: bool,
    ) -> BTreeMap<String, Value> {
        BTreeMap::from([
            ("district".to_owned(), Value::Uuid(district.0)),
            ("orderNumber".to_owned(), Value::U64(order_number)),
            ("delivered".to_owned(), Value::Bool(delivered)),
        ])
    }

    for (node_base, phantom_district, expected) in [
        (
            1,
            row(0xd1),
            Fate::Rejected(RejectionReason::ExclusiveConflict),
        ),
        (5, row(0xd2), Fate::Accepted),
    ] {
        let schema = orders_schema();
        let (_client_dir, mut client) = open_node_with_schema(node(node_base), schema.clone());
        let (_other_dir, mut other) = open_node_with_schema(node(node_base + 1), schema.clone());
        let (_core_dir, mut core) = open_node_with_schema(node(node_base + 2), schema.clone());
        let target_district = row(0xd1);
        let shape = Query::from("orders")
            .filter(eq(col("district"), param("district")))
            .filter(eq(col("delivered"), lit(Value::Bool(false))))
            .validate(&schema)
            .unwrap();
        let binding = shape
            .bind(BTreeMap::from([(
                "district".to_owned(),
                Value::Uuid(target_district.0),
            )]))
            .unwrap();

        let tx_id = client.open_exclusive().unwrap();
        assert!(client.tx_query(tx_id, &shape, &binding).unwrap().is_empty());
        commit_mergeable_global(
            &mut other,
            &mut core,
            MergeableCommit::new("orders", row(node_base), 10).cells(order_cells(
                phantom_district,
                1,
                false,
            )),
        );
        client
            .tx_write(
                tx_id,
                "orders",
                row(node_base + 10),
                order_cells(target_district, 2, true),
                None,
            )
            .unwrap();
        let (_tx_id, unit) = client
            .commit_exclusive(tx_id, AuthorId::SYSTEM, 11)
            .unwrap();
        let [fate] = core.apply_sync_message(unit).unwrap().try_into().unwrap();
        let SyncMessage::FateUpdate { fate, .. } = fate else {
            panic!("expected fate update");
        };
        assert_eq!(fate, expected);
    }
}
#[test]
fn exclusive_write_write_first_committer_wins() {
    let (_client_a_dir, mut client_a) = open_node_with_uuid(node(1));
    let (_client_b_dir, mut client_b) = open_node_with_uuid(node(2));
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    let row = row(7);
    commit_mergeable_global(
        &mut client_a,
        &mut core,
        MergeableCommit::new("todos", row, 10).cells(title_cells("base")),
    );
    sync_current_rows_to(&mut core, &mut client_b, 42);
    let tx_a = client_a.open_exclusive().unwrap();
    let tx_b = client_b.open_exclusive().unwrap();
    client_a
        .tx_write(tx_a, "todos", row, title_cells("a"), None)
        .unwrap();
    client_b
        .tx_write(tx_b, "todos", row, title_cells("b"), None)
        .unwrap();
    let (_a_ref, unit_a) = client_a
        .commit_exclusive(tx_a, AuthorId::SYSTEM, 11)
        .unwrap();
    let (_b_ref, unit_b) = client_b
        .commit_exclusive(tx_b, AuthorId::SYSTEM, 11)
        .unwrap();
    let [fate_a] = core.apply_sync_message(unit_a).unwrap().try_into().unwrap();
    let [fate_b] = core.apply_sync_message(unit_b).unwrap().try_into().unwrap();
    let SyncMessage::FateUpdate { fate: accepted, .. } = fate_a else {
        panic!("expected fate update");
    };
    let SyncMessage::FateUpdate { fate: rejected, .. } = fate_b else {
        panic!("expected fate update");
    };
    assert_eq!(accepted, Fate::Accepted);
    assert_eq!(rejected, Fate::Rejected(RejectionReason::ExclusiveConflict));
}
#[test]
fn exclusive_absent_read_conflict_rejects() {
    let (_client_dir, mut client) = open_node_with_uuid(node(1));
    let (_other_dir, mut other) = open_node_with_uuid(node(2));
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    let row = row(7);
    let tx_id = client.open_exclusive().unwrap();
    assert_eq!(client.tx_read(tx_id, "todos", row).unwrap(), None);
    commit_mergeable_global(
        &mut other,
        &mut core,
        MergeableCommit::new("todos", row, 10).cells(BTreeMap::from([(
            "title".to_owned(),
            "inserted".to_owned(),
        )])),
    );
    client
        .tx_write(tx_id, "todos", row, title_cells("mine"), None)
        .unwrap();
    let (_tx_id, unit) = client
        .commit_exclusive(tx_id, AuthorId::SYSTEM, 11)
        .unwrap();
    let [fate] = core.apply_sync_message(unit).unwrap().try_into().unwrap();
    let SyncMessage::FateUpdate { fate, .. } = fate else {
        panic!("expected fate update");
    };
    assert_eq!(fate, Fate::Rejected(RejectionReason::ExclusiveConflict));
}
#[test]
fn commit_unit_forward_skew_rejects_and_client_cleans_up() {
    let (_client_dir, mut client) = open_node_with_uuid(node(1));
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    let row = row(7);
    let (tx_id, unit) = client
        .commit_mergeable_unit(
            MergeableCommit::new("todos", row, SKEW_TOLERANCE_MS + 1).cells(title_cells("future")),
        )
        .unwrap();
    let SyncMessage::CommitUnit { tx, versions } = unit else {
        panic!("expected commit unit");
    };
    let [fate] = core
        .ingest_commit_unit(tx, versions, 0)
        .unwrap()
        .try_into()
        .unwrap();
    let SyncMessage::FateUpdate { fate: rejected, .. } = &fate else {
        panic!("expected fate update");
    };
    assert_eq!(
        rejected,
        &Fate::Rejected(RejectionReason::ClientClockTooFarAhead)
    );
    assert_eq!(
        core.transaction_state(tx_id).unwrap().0,
        Fate::Rejected(RejectionReason::ClientClockTooFarAhead)
    );
    assert!(core
        .current_rows("todos", DurabilityTier::Local)
        .unwrap()
        .is_empty());

    client.apply_sync_message(fate).unwrap();
    assert_eq!(
        client.transaction_state(tx_id).unwrap().0,
        Fate::Rejected(RejectionReason::ClientClockTooFarAhead)
    );
    assert!(client
        .current_rows("todos", DurabilityTier::Local)
        .unwrap()
        .is_empty());
}
#[test]
fn authority_parks_child_until_unknown_exclusive_parent_rejects() {
    let (_client_dir, mut client) = open_node_with_uuid(node(1));
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    let row = row(7);
    commit_mergeable_global(
        &mut client,
        &mut core,
        MergeableCommit::new("todos", row, 1).cells(title_cells("old")),
    );
    let tx_id = client.open_exclusive().unwrap();
    client
        .tx_write(tx_id, "todos", row, title_cells("exclusive"), None)
        .unwrap();
    let (exclusive, exclusive_unit) = client
        .commit_exclusive(tx_id, AuthorId::SYSTEM, SKEW_TOLERANCE_MS + 1)
        .unwrap();
    let (child, child_unit) = client
        .commit_mergeable_unit(
            MergeableCommit::new("todos", row, 2)
                .parents(vec![exclusive])
                .cells(title_cells("child")),
        )
        .unwrap();

    let SyncMessage::CommitUnit { tx, versions } = child_unit else {
        panic!("expected commit unit");
    };
    assert!(core
        .ingest_commit_unit(tx, versions, u64::MAX - SKEW_TOLERANCE_MS)
        .unwrap()
        .is_empty());
    assert_eq!(core.sync_metrics().parked_orphans, 1);

    let SyncMessage::CommitUnit { tx, versions } = exclusive_unit else {
        panic!("expected commit unit");
    };
    let updates = core.ingest_commit_unit(tx, versions, 0).unwrap();
    assert_eq!(core.sync_metrics().parked_orphans_resolved, 1);
    assert_eq!(
        updates,
        vec![
            SyncMessage::FateUpdate {
                tx_id: exclusive,
                fate: Fate::Rejected(RejectionReason::ClientClockTooFarAhead),
                global_seq: None,
                durability: None,
            },
            SyncMessage::FateUpdate {
                tx_id: child,
                fate: Fate::Rejected(RejectionReason::Cascade { root: exclusive }),
                global_seq: None,
                durability: None,
            },
        ]
    );
    for update in updates {
        client.apply_sync_message(update).unwrap();
    }
    assert_eq!(
        client.transaction_state(exclusive).unwrap().0,
        Fate::Rejected(RejectionReason::ClientClockTooFarAhead)
    );
    assert_eq!(
        client.transaction_state(child).unwrap().0,
        Fate::Rejected(RejectionReason::Cascade { root: exclusive })
    );
    assert_eq!(
        client
            .current_rows("todos", DurabilityTier::Local)
            .unwrap()
            .into_iter()
            .map(current_row_pair)
            .collect::<BTreeMap<_, _>>(),
        BTreeMap::from([(row, title_cells("old"))])
    );
}

fn register_shape_binding_for_receiver(
    node: &mut crate::node::NodeState<groove::storage::RocksDbStorage>,
    shape: &crate::query::ValidatedQuery,
    binding: &crate::query::Binding,
) {
    node.apply_sync_message(SyncMessage::RegisterShape {
        shape_id: shape.shape_id(),
        ast: crate::protocol::ShapeAst::from_validated(shape),
        opts: crate::protocol::RegisterShapeOptions::default(),
    })
    .unwrap();
    let values = shape
        .params()
        .keys()
        .map(|name| binding.values().get(name).cloned().unwrap())
        .collect();
    node.apply_sync_message(SyncMessage::Subscribe(crate::protocol::Subscribe {
        shape_id: shape.shape_id(),
        subscription: crate::protocol::SubscriptionKey {
            shape_id: shape.shape_id(),
            binding_id: binding.binding_id(),
            read_view: Default::default(),
        },
        values,
    }))
    .unwrap();
}

#[test]
fn receiver_tracks_partial_exclusive_payload_coverage_per_view() {
    let (_writer_dir, mut writer) = open_node_with_uuid(node(1));
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    let (_reader_dir, mut reader) = open_node_with_uuid(node(3));
    let shape = Query::from("todos")
        .filter(eq(col("title"), lit("one")))
        .validate(&schema())
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();

    let tx = writer.open_exclusive().unwrap();
    writer
        .tx_write(tx, "todos", row(1), title_cells("one"), None)
        .unwrap();
    writer
        .tx_write(tx, "todos", row(2), title_cells("two"), None)
        .unwrap();
    let (_tx_id, unit) = writer.commit_exclusive(tx, AuthorId::SYSTEM, 10).unwrap();
    let [fate] = core.apply_sync_message(unit).unwrap().try_into().unwrap();
    assert!(matches!(
        fate,
        SyncMessage::FateUpdate {
            fate: Fate::Accepted,
            ..
        }
    ));

    let mut peer = PeerState::new();
    let update = peer.rehydrate_query(&mut core, &shape, &binding).unwrap();
    let SyncMessage::ViewUpdate {
        subscription,
        mut version_bundles,
        result_member_adds,
        ..
    } = update
    else {
        panic!("expected view update");
    };
    assert_eq!(version_bundles.len(), 1);
    let bundle = version_bundles.pop().unwrap();
    assert_eq!(bundle.tx.kind, TxKind::Exclusive);
    assert_eq!(bundle.tx.n_total_writes, 2);
    assert_eq!(bundle.versions.len(), 1);
    assert_eq!(bundle.versions[0].row_uuid(), row(1));
    assert_eq!(result_member_adds, vec![("todos".to_owned().into(), row(1), bundle.tx.tx_id)]);
    assert!(peer.shipped_complete_tx_payloads().is_empty());

    register_shape_binding_for_receiver(&mut reader, &shape, &binding);
    reader
        .apply_sync_message(SyncMessage::ViewUpdate {
            subscription,
            reset_result_set: false,
            version_bundles: vec![bundle],
            peer_payload_inventory: crate::protocol::PeerPayloadInventory::default(),
            result_member_adds,
            result_member_removes: Vec::new(),
                program_fact_adds: Vec::new(),
                program_fact_removes: Vec::new(),
        })
        .unwrap();
    assert!(reader
        .current_rows("todos", DurabilityTier::Global)
        .unwrap()
        .is_empty());
    assert!(reader
        .subscription_current_rows("todos", DurabilityTier::Global)
        .unwrap()
        .is_empty());
    assert_eq!(
        reader
            .query_rows(&shape, &binding, DurabilityTier::Global)
            .unwrap(),
        vec![(row(1), title_cells("one"))]
    );
}

#[test]
fn malformed_exclusive_partial_result_row_add_is_rejected() {
    let (_writer_dir, mut writer) = open_node_with_uuid(node(1));
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    let (_reader_dir, mut reader) = open_node_with_uuid(node(3));
    let shape = Query::from("todos")
        .filter(eq(col("title"), lit("one")))
        .validate(&schema())
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();

    let tx = writer.open_exclusive().unwrap();
    writer
        .tx_write(tx, "todos", row(1), title_cells("one"), None)
        .unwrap();
    writer
        .tx_write(tx, "todos", row(2), title_cells("two"), None)
        .unwrap();
    let (_tx_id, unit) = writer.commit_exclusive(tx, AuthorId::SYSTEM, 10).unwrap();
    let [fate] = core.apply_sync_message(unit).unwrap().try_into().unwrap();
    assert!(matches!(
        fate,
        SyncMessage::FateUpdate {
            fate: Fate::Accepted,
            ..
        }
    ));

    let mut peer = PeerState::new();
    let update = peer.rehydrate_query(&mut core, &shape, &binding).unwrap();
    let SyncMessage::ViewUpdate {
        subscription,
        version_bundles,
        ..
    } = update
    else {
        panic!("expected view update");
    };
    let tx_id = version_bundles[0].tx.tx_id;
    assert_eq!(version_bundles.len(), 1);
    assert_eq!(version_bundles[0].versions.len(), 1);
    assert_eq!(version_bundles[0].versions[0].row_uuid(), row(1));

    let err = reader
        .apply_sync_message(SyncMessage::ViewUpdate {
            subscription,
            reset_result_set: false,
            version_bundles,
            peer_payload_inventory: crate::protocol::PeerPayloadInventory::default(),
            result_member_adds: vec![("todos".to_owned().into(), row(2), tx_id).into()],
            result_member_removes: Vec::new(),
                program_fact_adds: Vec::new(),
                program_fact_removes: Vec::new(),
        })
        .unwrap_err();

    assert!(matches!(
        err,
        Error::MalformedViewUpdate(
            "exclusive result row add is not witnessed by partial payload"
        )
    ));
    assert!(reader
        .query_rows(&shape, &binding, DurabilityTier::Global)
        .unwrap()
        .is_empty());
}

#[test]
fn partial_exclusive_payload_does_not_establish_tx_level_complete_tx_ref() {
    let (_writer_dir, mut writer) = open_node_with_uuid(node(1));
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    let first_shape = Query::from("todos")
        .filter(eq(col("title"), lit("one")))
        .validate(&schema())
        .unwrap();
    let first_binding = first_shape.bind(BTreeMap::new()).unwrap();
    let second_shape = Query::from("todos")
        .filter(eq(col("title"), lit("two")))
        .validate(&schema())
        .unwrap();
    let second_binding = second_shape.bind(BTreeMap::new()).unwrap();

    let tx = writer.open_exclusive().unwrap();
    writer
        .tx_write(tx, "todos", row(1), title_cells("one"), None)
        .unwrap();
    writer
        .tx_write(tx, "todos", row(2), title_cells("two"), None)
        .unwrap();
    let (tx_id, unit) = writer.commit_exclusive(tx, AuthorId::SYSTEM, 10).unwrap();
    let [fate] = core.apply_sync_message(unit).unwrap().try_into().unwrap();
    assert!(matches!(
        fate,
        SyncMessage::FateUpdate {
            fate: Fate::Accepted,
            ..
        }
    ));

    let mut peer = PeerState::new();
    let first = peer
        .rehydrate_query(&mut core, &first_shape, &first_binding)
        .unwrap();
    let SyncMessage::ViewUpdate {
        version_bundles,
        peer_payload_inventory: crate::protocol::PeerPayloadInventory { complete_tx_payloads: complete_tx_payload_refs },
        ..
    } = first
    else {
        panic!("expected view update");
    };
    assert_eq!(version_bundles.len(), 1);
    assert_eq!(version_bundles[0].tx.tx_id, tx_id);
    assert_eq!(version_bundles[0].versions.len(), 1);
    assert!(complete_tx_payload_refs.is_empty());
    assert!(peer.shipped_complete_tx_payloads().is_empty());

    let second = peer
        .rehydrate_query(&mut core, &second_shape, &second_binding)
        .unwrap();
    let SyncMessage::ViewUpdate {
        version_bundles,
        peer_payload_inventory: crate::protocol::PeerPayloadInventory { complete_tx_payloads: complete_tx_payload_refs },
        ..
    } = second
    else {
        panic!("expected view update");
    };
    assert_eq!(version_bundles.len(), 1);
    assert_eq!(version_bundles[0].tx.tx_id, tx_id);
    assert_eq!(version_bundles[0].versions.len(), 1);
    assert!(complete_tx_payload_refs.is_empty());
    assert!(peer.shipped_complete_tx_payloads().is_empty());
}
#[test]
fn exclusive_view_shipping_is_view_atomic_per_recipient() {
    let schema = owner_policy_schema();
    let (_writer_dir, mut writer) = open_node_with_schema(node(1), schema.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema.clone());
    let (_reader_a_dir, mut reader_a) = open_node_with_schema(node(3), schema.clone());
    let (_reader_system_dir, mut reader_system) = open_node_with_schema(node(4), schema);
    let author_a = user(0xa1);
    let author_b = user(0xb2);

    let tx = writer.open_exclusive().unwrap();
    writer
        .tx_write(tx, "todos", row(1), owner_cells(author_a, "a row"), None)
        .unwrap();
    writer
        .tx_write(tx, "todos", row(2), owner_cells(author_b, "b row"), None)
        .unwrap();
    let (_tx_id, unit) = writer.commit_exclusive(tx, AuthorId::SYSTEM, 10).unwrap();
    let [fate] = core.apply_sync_message(unit).unwrap().try_into().unwrap();
    assert!(matches!(
        fate,
        SyncMessage::FateUpdate {
            fate: Fate::Accepted,
            ..
        }
    ));

    let mut link_a = PeerState::for_author(author_a);
    let update_a = link_a.current_rows_update(&mut core, "todos").unwrap();
    let SyncMessage::ViewUpdate {
        version_bundles,
        result_member_adds,
        ..
    } = &update_a
    else {
        panic!("expected view update");
    };
    assert_eq!(version_bundles.len(), 1);
    assert_eq!(version_bundles[0].tx.kind, TxKind::Exclusive);
    assert_eq!(version_bundles[0].tx.n_total_writes, 2);
    assert_eq!(version_bundles[0].versions.len(), 1);
    assert_eq!(version_bundles[0].versions[0].row_uuid(), row(1));
    assert_eq!(
        result_member_adds,
        &vec![("todos".to_owned().into(), row(1), version_bundles[0].tx.tx_id)]
    );
    assert!(link_a.shipped_complete_tx_payloads().is_empty());
    reader_a.apply_sync_message(update_a).unwrap();
    assert_eq!(
        reader_a
            .subscription_current_rows("todos", DurabilityTier::Global)
            .unwrap(),
        vec![(row(1), owner_cells(author_a, "a row"))]
    );

    let mut link_system = PeerState::new();
    let update_system = link_system.current_rows_update(&mut core, "todos").unwrap();
    reader_system.apply_sync_message(update_system).unwrap();
    assert_eq!(
        reader_system
            .subscription_current_rows("todos", DurabilityTier::Global)
            .unwrap(),
        vec![
            (row(1), owner_cells(author_a, "a row")),
            (row(2), owner_cells(author_b, "b row")),
        ]
    );
}
#[test]
fn exclusive_set_serializes_counter_base_before_mergeable_deltas() {
    let schema = counter_schema();
    let (_base_dir, mut base_writer) = open_node_with_schema(node(1), schema.clone());
    let (_writer_a_dir, mut writer_a) = open_node_with_schema(node(2), schema.clone());
    let (_writer_b_dir, mut writer_b) = open_node_with_schema(node(3), schema.clone());
    let (_client_dir, mut client) = open_node_with_schema(node(4), schema.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema.clone());
    let row = row(8);

    commit_mergeable_global(
        &mut base_writer,
        &mut core,
        MergeableCommit::new("counters", row, 10).cells(BTreeMap::from([
            ("count".to_owned(), Value::U64(10)),
            ("title".to_owned(), v("base")),
        ])),
    );
    let mut peer = PeerState::new();
    client
        .apply_sync_message(peer.current_rows_update(&mut core, "counters").unwrap())
        .unwrap();

    let tx = client.open_exclusive().unwrap();
    client
        .tx_write(
            tx,
            "counters",
            row,
            BTreeMap::from([
                ("count".to_owned(), Value::U64(100)),
                ("title".to_owned(), v("exclusive")),
            ]),
            None,
        )
        .unwrap();
    let (_exclusive_tx, unit) = client.commit_exclusive(tx, AuthorId::SYSTEM, 20).unwrap();
    let SyncMessage::CommitUnit { tx, versions } = unit else {
        panic!("expected commit unit");
    };
    let fate_updates = core.ingest_commit_unit(tx, versions, 20).unwrap();
    for update in fate_updates {
        client.apply_sync_message(update).unwrap();
    }
    let exclusive = global_winner_tx(&mut core, "counters", row, VersionLayer::Content).unwrap();

    let (left, left_message) = writer_a
        .commit_mergeable_unit(
            MergeableCommit::new("counters", row, 30)
                .parents(vec![exclusive])
                .cells(BTreeMap::from([("count".to_owned(), Value::U64(105))])),
        )
        .unwrap();
    let (right, right_message) = writer_b
        .commit_mergeable_unit(
            MergeableCommit::new("counters", row, 31)
                .parents(vec![exclusive])
                .cells(BTreeMap::from([("count".to_owned(), Value::U64(107))])),
        )
        .unwrap();

    core.apply_sync_message(left_message).unwrap();
    core.apply_sync_message(right_message).unwrap();

    let merge = core
        .query_all_versions()
        .unwrap()
        .into_iter()
        .find(|version| {
            version.row_uuid() == row
                && core.version_tx_id(version).unwrap().node == node(9)
                && version.parents().contains(&left)
                && version.parents().contains(&right)
        })
        .expect("core should create a post-exclusive counter merge version");
    let cells = merge.cells(&schema.tables[0]).unwrap();
    assert_eq!(cells.get("count"), Some(&Value::U64(112)));
    assert_eq!(cells.get("title"), Some(&v("exclusive")));
}
#[test]
fn originating_rejected_exclusive_moves_payload_to_retry_store() {
    let (_writer_a_dir, mut writer_a) = open_node_with_uuid(node(1));
    let (writer_b_dir, mut writer_b) = open_node_with_uuid(node(2));
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    let row = row(7);

    commit_mergeable_global(
        &mut writer_a,
        &mut core,
        MergeableCommit::new("todos", row, 10).cells(title_cells("base")),
    );
    sync_current_rows_to(&mut core, &mut writer_b, 77);
    let tx_id = writer_b.open_exclusive().unwrap();
    writer_b.tx_read(tx_id, "todos", row).unwrap();
    commit_mergeable_global(
        &mut writer_a,
        &mut core,
        MergeableCommit::new("todos", row, 11).cells(BTreeMap::from([(
            "title".to_owned(),
            "intervening".to_owned(),
        )])),
    );
    writer_b
        .tx_write(tx_id, "todos", row, title_cells("retry me"), None)
        .unwrap();
    let (rejected, unit) = writer_b
        .commit_exclusive(tx_id, AuthorId::SYSTEM, 12)
        .unwrap();
    let [fate] = core.apply_sync_message(unit).unwrap().try_into().unwrap();
    assert_eq!(
        fate,
        SyncMessage::FateUpdate {
            tx_id: rejected,
            fate: Fate::Rejected(RejectionReason::ExclusiveConflict),
            global_seq: None,
            durability: None,
        }
    );
    assert!(core.rejected_transaction(rejected).is_none());
    writer_b.apply_sync_message(fate).unwrap();

    assert_eq!(writer_b.rejected_transactions(), vec![rejected]);
    let stored = writer_b.rejected_transaction(rejected).unwrap();
    assert_eq!(stored.reason(), RejectionReason::ExclusiveConflict);
    assert_eq!(stored.cascade_root(), None);
    assert_eq!(stored.kind(), TxKind::Exclusive);
    assert_eq!(stored.versions().len(), 1);
    assert_eq!(stored.versions()[0].table(), "todos");
    assert_eq!(stored.versions()[0].row_uuid(), row);
    assert_eq!(
        stored.versions()[0].test_cells(&schema().tables[0]),
        title_cells("retry me")
    );
    assert_eq!(stored.versions()[0].parents().len(), 1);
    assert!(writer_b
        .row_history("todos", row)
        .unwrap()
        .iter()
        .all(|entry| entry.tx_id() != rejected));
    assert!(writer_b
        .current_rows("todos", DurabilityTier::Local)
        .unwrap()
        .iter()
        .all(|row| row.cell(&schema().tables[0], "title") != Some(v("retry me"))));

    drop(writer_b);
    let mut reopened = reopen_node_at(&writer_b_dir, node(2), schema());
    assert_eq!(
        reopened.rejected_transaction(rejected).unwrap().versions(),
        stored.versions()
    );
    reopened.discard_rejection(rejected).unwrap();
    assert!(reopened.rejected_transaction(rejected).is_none());
    drop(reopened);
    let reopened = reopen_node_at(&writer_b_dir, node(2), schema());
    assert!(reopened.rejected_transaction(rejected).is_none());
}
