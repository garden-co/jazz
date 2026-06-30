#[test]
fn core_creates_merge_versions_for_concurrent_heads() {
    let schema = two_column_schema();
    let (_writer_a_dir, mut writer_a) = open_node_with_schema(node(1), schema.clone());
    let (_writer_b_dir, mut writer_b) = open_node_with_schema(node(2), schema.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema.clone());
    let row = row(7);

    let (left, left_message) = writer_a
        .commit_mergeable_unit(
            MergeableCommit::new("todos", row, 10).cells(BTreeMap::from([(
                "title".to_owned(),
                "older-title".to_owned(),
            )])),
        )
        .unwrap();
    let (right, right_message) = writer_b
        .commit_mergeable_unit(
            MergeableCommit::new("todos", row, 11).cells(BTreeMap::from([(
                "body".to_owned(),
                "right-body".to_owned(),
            )])),
        )
        .unwrap();

    core.apply_sync_message(right_message).unwrap();
    core.apply_sync_message(left_message).unwrap();

    let update = core.view_update_for_current_rows("todos").unwrap();
    let SyncMessage::ViewUpdate {
        version_bundles,
        result_row_adds,
        result_row_removes,
        ..
    } = update
    else {
        panic!("expected view update");
    };
    assert_eq!(result_row_adds.len(), 1);
    assert!(result_row_removes.is_empty());
    let merge = version_bundles
        .iter()
        .find(|bundle| bundle.tx.tx_id.node == node(9))
        .expect("core should create a merge transaction");
    let merge_record = merge
        .versions
        .iter()
        .find(|version| version.row_uuid() == row)
        .expect("merge transaction should carry a row version");
    let mut parents = merge_record.parents();
    parents.sort();
    assert_eq!(parents, vec![left, right]);
    assert_eq!(
        version_record_cells(merge_record, &schema.tables[0]),
        BTreeMap::from([
            ("title".to_owned(), v("older-title")),
            ("body".to_owned(), v("right-body")),
        ])
    );
    assert_eq!(
        result_row_adds,
        vec![("todos".to_owned().into(), row, merge.tx.tx_id)]
    );
}
#[test]
fn counter_merge_sums_concurrent_deltas_and_keeps_lww_columns() {
    let schema = counter_schema();
    let (_base_dir, mut base_writer) = open_node_with_schema(node(1), schema.clone());
    let (_writer_a_dir, mut writer_a) = open_node_with_schema(node(2), schema.clone());
    let (_writer_b_dir, mut writer_b) = open_node_with_schema(node(3), schema.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema.clone());
    let row = row(7);

    let base = commit_mergeable_global(
        &mut base_writer,
        &mut core,
        MergeableCommit::new("counters", row, 10).cells(BTreeMap::from([
            ("count".to_owned(), Value::U64(10)),
            ("title".to_owned(), v("base")),
        ])),
    );
    let (left, left_message) = writer_a
        .commit_mergeable_unit(
            MergeableCommit::new("counters", row, 20)
                .parents(vec![base])
                .cells(BTreeMap::from([
                    ("count".to_owned(), Value::U64(11)),
                    ("title".to_owned(), v("left-title")),
                ])),
        )
        .unwrap();
    let (right, right_message) = writer_b
        .commit_mergeable_unit(
            MergeableCommit::new("counters", row, 21)
                .parents(vec![base])
                .cells(BTreeMap::from([
                    ("count".to_owned(), Value::U64(12)),
                    ("title".to_owned(), v("right-title")),
                ])),
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
        .expect("core should create a counter merge version");
    let cells = merge.cells(&schema.tables[0]).unwrap();
    assert_eq!(cells.get("count"), Some(&Value::U64(13)));
    assert_eq!(cells.get("title"), Some(&v("right-title")));

    let current = core
        .current_rows("counters", DurabilityTier::Global)
        .unwrap()
        .into_iter()
        .map(current_row_pair)
        .collect::<BTreeMap<_, _>>();
    assert_eq!(current[&row].get("count"), Some(&Value::U64(13)));
    assert_eq!(current[&row].get("title"), Some(&v("right-title")));
}
#[test]
fn counter_merge_seeded_concurrent_increments_converge_to_exact_sum() {
    for seed in 0..20_u64 {
        let schema = counter_schema();
        let (_base_dir, mut base_writer) = open_node_with_schema(node(1), schema.clone());
        let (_core_dir, mut core) = open_node_with_schema(node(9), schema.clone());
        let row = row(0x20 + seed as u8);
        let base_value = seed % 7;
        let base = commit_mergeable_global(
            &mut base_writer,
            &mut core,
            MergeableCommit::new("counters", row, 10).cells(BTreeMap::from([
                ("count".to_owned(), Value::U64(base_value)),
                ("title".to_owned(), v("base")),
            ])),
        );
        let mut expected = base_value;
        let mut writers = (0..5_u8)
            .map(|idx| open_node_with_schema(node(0x30 + idx), schema.clone()))
            .collect::<Vec<_>>();
        let mut messages = Vec::new();
        for (idx, (_, writer)) in writers.iter_mut().enumerate() {
            let delta = ((seed + idx as u64 * 3) % 5) + 1;
            expected += delta;
            let (_tx, message) = writer
                .commit_mergeable_unit(
                    MergeableCommit::new("counters", row, 20 + idx as u64)
                        .parents(vec![base])
                        .cells(BTreeMap::from([(
                            "count".to_owned(),
                            Value::U64(base_value + delta),
                        )])),
                )
                .unwrap();
            messages.push(message);
        }
        for message in messages {
            core.apply_sync_message(message).unwrap();
        }

        for (_, writer) in &mut writers {
            let mut peer = PeerState::new();
            writer
                .apply_sync_message(peer.current_rows_update(&mut core, "counters").unwrap())
                .unwrap();
            let current = writer
                .current_rows("counters", DurabilityTier::Local)
                .unwrap()
                .into_iter()
                .map(current_row_pair)
                .collect::<BTreeMap<_, _>>();
            assert_eq!(
                current[&row].get("count"),
                Some(&Value::U64(expected)),
                "seed {seed}"
            );
            assert_eq!(current[&row].get("title"), Some(&v("base")), "seed {seed}");
        }
    }
}

#[test]
fn counter_merge_of_divergent_merges_sums_raw_frontier_once() {
    let schema = counter_schema();
    let (_core_dir, mut core) = open_node_with_schema(node(0x90), schema.clone());
    let row = row(0x91);
    let table = &schema.tables[0];
    let h1 = TxId::new(TxTime::from(10), node(0x11));
    let h2 = TxId::new(TxTime::from(11), node(0x12));
    let h3 = TxId::new(TxTime::from(12), node(0x13));
    let m12 = TxId::new(TxTime::from(20), node(0x21));
    let m23 = TxId::new(TxTime::from(21), node(0x22));

    ingest_counter_version(&mut core, &schema, row, h1, vec![], 1, "h1");
    ingest_counter_version(&mut core, &schema, row, h2, vec![], 2, "h2");
    ingest_counter_version(&mut core, &schema, row, h3, vec![], 3, "h3");
    ingest_counter_version(&mut core, &schema, row, m12, vec![h1, h2], 3, "cached-m12");
    ingest_counter_version(&mut core, &schema, row, m23, vec![h2, h3], 5, "cached-m23");

    core.create_merge_version_if_needed("counters", row).unwrap();

    let merge = merge_with_parent_set(&mut core, row, &[h1, h2, h3]);
    let cells = merge.cells(table).unwrap();
    assert_eq!(cells.get("count"), Some(&Value::U64(6)));
    assert_eq!(cells.get("title"), Some(&v("h3")));
}

#[test]
fn lww_merge_of_divergent_merges_uses_raw_argmax() {
    let schema = two_column_schema();
    let (_core_dir, mut core) = open_node_with_schema(node(0x92), schema.clone());
    let row = row(0x93);
    let table = &schema.tables[0];
    let h1 = TxId::new(TxTime::from(10), node(0x11));
    let h2 = TxId::new(TxTime::from(11), node(0x12));
    let h3 = TxId::new(TxTime::from(12), node(0x13));
    let m12 = TxId::new(TxTime::from(20), node(0x21));
    let m23 = TxId::new(TxTime::from(21), node(0x22));

    ingest_todos_version(&mut core, &schema, table, row, h1, vec![], "h1");
    ingest_todos_version(&mut core, &schema, table, row, h2, vec![], "h2");
    ingest_todos_version(&mut core, &schema, table, row, h3, vec![], "h3");
    ingest_todos_version(&mut core, &schema, table, row, m12, vec![h1, h2], "stale-m12");
    ingest_todos_version(&mut core, &schema, table, row, m23, vec![h2, h3], "stale-m23");

    core.create_merge_version_if_needed("todos", row).unwrap();

    let merge = merge_with_parent_set(&mut core, row, &[h1, h2, h3]);
    let cells = merge.cells(table).unwrap();
    assert_eq!(cells.get("title"), Some(&v("h3")));
}

#[test]
fn raw_merge_heads_drop_transitive_ancestors_after_late_child() {
    let schema = two_column_schema();
    let (_core_dir, mut core) = open_node_with_schema(node(0x96), schema.clone());
    let row = row(0x97);
    let table = &schema.tables[0];
    let left = TxId::new(TxTime::from(10), node(0x11));
    let right_parent = TxId::new(TxTime::from(11), node(0x12));
    let right_mid = TxId::new(TxTime::from(12), node(0x12));
    let right_child = TxId::new(TxTime::from(13), node(0x12));

    ingest_todos_version(&mut core, &schema, table, row, left, vec![], "left");
    ingest_todos_version(
        &mut core,
        &schema,
        table,
        row,
        right_parent,
        vec![],
        "right-parent",
    );
    core.create_merge_version_if_needed("todos", row).unwrap();
    merge_with_parent_set(&mut core, row, &[left, right_parent]);

    ingest_todos_version(
        &mut core,
        &schema,
        table,
        row,
        right_mid,
        vec![right_parent],
        "right-mid",
    );
    ingest_todos_version(
        &mut core,
        &schema,
        table,
        row,
        right_child,
        vec![right_mid],
        "right-child",
    );
    core.create_merge_version_if_needed("todos", row).unwrap();

    merge_with_parent_set(&mut core, row, &[left, right_child]);
    assert!(
        core.query_all_versions().unwrap().into_iter().all(|version| {
            let mut parents = version.parents();
            parents.sort();
            parents != vec![left, right_parent, right_child]
        }),
        "merge parent set must not include an ancestor and its descendant"
    );
}

#[test]
fn duplicate_merges_over_same_frontier_refold_to_identical_cells() {
    let schema = two_column_schema();
    let (_core_dir, mut core) = open_node_with_schema(node(0x94), schema.clone());
    let row = row(0x95);
    let table = &schema.tables[0];
    let h1 = TxId::new(TxTime::from(10), node(0x11));
    let h2 = TxId::new(TxTime::from(11), node(0x12));
    let m12a = TxId::new(TxTime::from(20), node(0x21));
    let m12b = TxId::new(TxTime::from(21), node(0x22));

    ingest_todos_version(&mut core, &schema, table, row, h1, vec![], "h1");
    ingest_todos_version(&mut core, &schema, table, row, h2, vec![], "h2");
    ingest_todos_version(&mut core, &schema, table, row, m12a, vec![h1, h2], "h2");
    ingest_todos_version(&mut core, &schema, table, row, m12b, vec![h1, h2], "h2");

    core.create_merge_version_if_needed("todos", row).unwrap();

    let first = core
        .query_all_versions()
        .unwrap()
        .into_iter()
        .find(|version| core.version_tx_id(version).unwrap() == m12a)
        .unwrap();
    let second = core
        .query_all_versions()
        .unwrap()
        .into_iter()
        .find(|version| core.version_tx_id(version).unwrap() == m12b)
        .unwrap();
    assert_eq!(first.cells(table).unwrap(), second.cells(table).unwrap());
}

fn ingest_counter_version(
    node: &mut NodeState<RocksDbStorage>,
    schema: &JazzSchema,
    row_uuid: RowUuid,
    tx_id: TxId,
    parents: Vec<TxId>,
    count: u64,
    title: &str,
) {
    ingest_direct_version(
        node,
        schema,
        &schema.tables[0],
        row_uuid,
        tx_id,
        parents,
        BTreeMap::from([
            ("count".to_owned(), Value::U64(count)),
            ("title".to_owned(), v(title)),
        ]),
    );
}

fn ingest_todos_version(
    node: &mut NodeState<RocksDbStorage>,
    schema: &JazzSchema,
    table: &TableSchema,
    row_uuid: RowUuid,
    tx_id: TxId,
    parents: Vec<TxId>,
    title: &str,
) {
    ingest_direct_version(
        node,
        schema,
        table,
        row_uuid,
        tx_id,
        parents,
        BTreeMap::from([("title".to_owned(), v(title))]),
    );
}

fn ingest_direct_version(
    node: &mut NodeState<RocksDbStorage>,
    schema: &JazzSchema,
    table: &TableSchema,
    row_uuid: RowUuid,
    tx_id: TxId,
    parents: Vec<TxId>,
    cells: BTreeMap<String, Value>,
) {
    node.ingest_transaction_and_versions(
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
        vec![VersionRecord::from_cells(
            table,
            schema.version_id(),
            row_uuid,
            parents,
            AuthorId::SYSTEM,
            TxTime(10),
            AuthorId::SYSTEM,
            TxTime(10),
            &cells,
            None,
        )
        .unwrap()],
        Fate::Accepted,
        None,
        DurabilityTier::Local,
    )
    .unwrap();
}

fn merge_with_parent_set(
    node: &mut NodeState<RocksDbStorage>,
    row_uuid: RowUuid,
    parents: &[TxId],
) -> VersionRow {
    let mut expected = parents.to_vec();
    expected.sort();
    node.query_all_versions()
        .unwrap()
        .into_iter()
        .find(|version| {
            if version.row_uuid() != row_uuid {
                return false;
            }
            let mut actual = version.parents();
            actual.sort();
            actual == expected
        })
        .expect("core should create merge version for parent set")
}

#[test]
fn core_local_currency_uses_argmax_not_sender_arrival_order() {
    let (_client_a_dir, mut client_a) = open_node_with_uuid(node(1));
    let (_client_b_dir, mut client_b) = open_node_with_uuid(node(2));
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    let shared_row = row(7);

    let (tx_a, unit_a) = client_a
        .commit_mergeable_unit(
            MergeableCommit::new("todos", shared_row, 10).cells(title_cells("a")),
        )
        .unwrap();
    let (tx_b, unit_b) = client_b
        .commit_mergeable_unit(
            MergeableCommit::new("todos", shared_row, 11).cells(title_cells("b")),
        )
        .unwrap();
    assert_eq!(tx_a.time, TxTime::from(10));
    assert_eq!(tx_b.time, TxTime::from(11));

    let SyncMessage::CommitUnit {
        tx: tx_a_payload,
        versions: versions_a,
    } = unit_a
    else {
        panic!("expected commit unit");
    };
    core.ingest_commit_unit(tx_a_payload, versions_a, u64::MAX - SKEW_TOLERANCE_MS)
        .unwrap();
    let SyncMessage::CommitUnit {
        tx: tx_b_payload,
        versions: versions_b,
    } = unit_b
    else {
        panic!("expected commit unit");
    };
    core.ingest_commit_unit(tx_b_payload, versions_b, u64::MAX - SKEW_TOLERANCE_MS)
        .unwrap();

    let merge_tx = core
        .query_all_versions()
        .unwrap()
        .into_iter()
        .find_map(|version| {
            let tx_id = core.version_tx_id(&version).unwrap();
            (version.row_uuid() == shared_row && tx_id.node == node(9)).then_some(tx_id)
        })
        .expect("core should create a merge version");
    assert_eq!(
        core.query_local_layer_winner("todos", shared_row, VersionLayer::Content)
            .unwrap()
            .as_ref()
            .map(|version| core.version_tx_id(version).unwrap()),
        Some(merge_tx)
    );
    assert!(merge_tx > tx_a);
    assert!(merge_tx > tx_b);
}
