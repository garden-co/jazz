#[test]
fn query_rows_at_matches_oracle_across_winners_deletes_and_restores() {
    let (_writer_dir, mut writer) = open_node_with_uuid(node(1));
    let (_core_dir, mut core) = open_node_with_uuid(node(2));
    let mut oracle = Oracle::new();
    let shape = Query::from("todos").validate(&core.catalogue.schema).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let row_uuid = row(1);

    let expected_at = |oracle: &Oracle, position: GlobalSeq| {
        oracle
            .visible_global_current_versions_at(position)
            .into_iter()
            .map(|version| (version.row_uuid, version.cells.clone()))
            .collect::<BTreeMap<_, _>>()
    };

    assert_eq!(
        core.query_rows_at(&shape, &binding, GlobalSeq(0))
            .unwrap()
            .into_iter()
            .map(current_row_pair)
            .collect::<BTreeMap<_, _>>(),
        expected_at(&oracle, GlobalSeq(0)),
        "cut before the first version must be empty"
    );

    let (base, s1) = commit_global_and_oracle(
        &mut writer,
        &mut core,
        &mut oracle,
        MergeableCommit::new("todos", row_uuid, 10).cells(title_cells("base")),
    );
    let (second, s2) = commit_global_and_oracle(
        &mut writer,
        &mut core,
        &mut oracle,
        MergeableCommit::new("todos", row_uuid, 20)
            .parents(vec![base])
            .cells(title_cells("second")),
    );
    let (_delete, s3) = commit_global_and_oracle(
        &mut writer,
        &mut core,
        &mut oracle,
        MergeableCommit::new("todos", row_uuid, 30).deletion(DeletionEvent::Deleted),
    );
    let (_restore, s4) = commit_global_and_oracle(
        &mut writer,
        &mut core,
        &mut oracle,
        MergeableCommit::new("todos", row_uuid, 40).deletion(DeletionEvent::Restored),
    );
    let (_third, s5) = commit_global_and_oracle(
        &mut writer,
        &mut core,
        &mut oracle,
        MergeableCommit::new("todos", row_uuid, 50)
            .parents(vec![second])
            .cells(title_cells("third")),
    );

    for position in [s1, s2, s3, s4, s5] {
        let actual = core
            .query_rows_at(&shape, &binding, position)
            .unwrap()
            .into_iter()
            .map(current_row_pair)
            .collect::<BTreeMap<_, _>>();
        assert_eq!(actual, expected_at(&oracle, position), "cut {position:?}");
    }
}
#[test]
fn query_rows_at_lowers_filters_against_historical_current_rows() {
    let (_writer_dir, mut writer) = open_node_with_uuid(node(1));
    let (_core_dir, mut core) = open_node_with_uuid(node(2));
    let mut oracle = Oracle::new();
    let row_uuid = row(6);
    let (base, s1) = commit_global_and_oracle(
        &mut writer,
        &mut core,
        &mut oracle,
        MergeableCommit::new("todos", row_uuid, 10).cells(title_cells("base")),
    );
    let (_second, s2) = commit_global_and_oracle(
        &mut writer,
        &mut core,
        &mut oracle,
        MergeableCommit::new("todos", row_uuid, 20)
            .parents(vec![base])
            .cells(title_cells("second")),
    );
    let shape = Query::from("todos")
        .filter(eq(col("title"), lit("second")))
        .validate(&core.catalogue.schema)
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();

    assert!(
        core.query_rows_at(&shape, &binding, s1).unwrap().is_empty(),
        "historical lowering must apply filters at the requested cut"
    );
    assert_eq!(
        core.query_rows_at(&shape, &binding, s2).unwrap().len(),
        1,
        "historical lowering should see the later winner at the later cut"
    );
}
#[test]
fn query_rows_at_for_link_evaluates_read_policy_at_historical_cut() {
    let alice = AuthorId::from_bytes([0xa1; 16]);
    let bob = AuthorId::from_bytes([0xb2; 16]);
    let schema = JazzSchema::new([TableSchema::new(
        "todos",
        [
            ColumnSchema::new("title", ColumnType::String),
            ColumnSchema::new("owner", ColumnType::Uuid),
        ],
    )
    .with_read_policy(Policy::owner_only("todos", "owner"))]);
    let (_writer_dir, mut writer) = open_node_with_schema(node(1), schema.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(2), schema);
    let shape = Query::from("todos").validate(&core.catalogue.schema).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let row_uuid = row(2);
    let mut oracle = Oracle::new();
    let mut alice_cells = title_cells("owned by alice");
    alice_cells.insert("owner".to_owned(), Value::Uuid(alice.0));
    let mut bob_cells = title_cells("owned by bob");
    bob_cells.insert("owner".to_owned(), Value::Uuid(bob.0));

    let (first, s1) = commit_global_and_oracle(
        &mut writer,
        &mut core,
        &mut oracle,
        MergeableCommit::new("todos", row_uuid, 10).cells(alice_cells),
    );
    let (_second, s2) = commit_global_and_oracle(
        &mut writer,
        &mut core,
        &mut oracle,
        MergeableCommit::new("todos", row_uuid, 20)
            .parents(vec![first])
            .cells(bob_cells),
    );

    assert_eq!(
        core.query_rows_at_for_link(&shape, &binding, s1, alice)
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        core.query_rows_at_for_link(&shape, &binding, s1, bob)
            .unwrap()
            .len(),
        0
    );
    assert_eq!(
        core.query_rows_at_for_link(&shape, &binding, s2, alice)
            .unwrap()
            .len(),
        0
    );
    assert_eq!(
        core.query_rows_at_for_link(&shape, &binding, s2, bob)
            .unwrap()
            .len(),
        1
    );
}
#[test]
fn historical_read_handle_reads_exact_position_locally_when_history_complete() {
    let (_writer_dir, mut writer) = open_node_with_uuid(node(1));
    let (_core_dir, mut core) = open_history_complete_node_with_schema(node(2), schema());
    let mut oracle = Oracle::new();
    let row_uuid = row(3);
    let (base, s1) = commit_global_and_oracle(
        &mut writer,
        &mut core,
        &mut oracle,
        MergeableCommit::new("todos", row_uuid, 10).cells(title_cells("base")),
    );
    let (_second, _s2) = commit_global_and_oracle(
        &mut writer,
        &mut core,
        &mut oracle,
        MergeableCommit::new("todos", row_uuid, 20)
            .parents(vec![base])
            .cells(title_cells("second")),
    );
    let shape = Query::from("todos").validate(&core.catalogue.schema).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();

    let rows = core
        .at(s1)
        .read(&shape, &binding)
        .unwrap()
        .into_iter()
        .map(current_row_pair)
        .collect::<BTreeMap<_, _>>();
    assert_eq!(rows.get(&row_uuid), Some(&title_cells("base")));
}
#[test]
fn historical_read_at_time_resolves_latest_settle_position_by_tx_time() {
    let (_writer_dir, mut writer) = open_node_with_uuid(node(1));
    let (_core_dir, mut core) = open_history_complete_node_with_schema(node(2), schema());
    let mut oracle = Oracle::new();
    let row_uuid = row(4);
    let (base, s1) = commit_global_and_oracle(
        &mut writer,
        &mut core,
        &mut oracle,
        MergeableCommit::new("todos", row_uuid, 10).cells(title_cells("base")),
    );
    let (_second, s2) = commit_global_and_oracle(
        &mut writer,
        &mut core,
        &mut oracle,
        MergeableCommit::new("todos", row_uuid, 50)
            .parents(vec![base])
            .cells(title_cells("second")),
    );

    assert_eq!(
        core.at_time(TxTime::from(5)).unwrap().position(),
        GlobalSeq(0)
    );
    assert_eq!(core.at_time(TxTime::from(30)).unwrap().position(), s1);
    assert_eq!(core.at_time(TxTime::from(60)).unwrap().position(), s2);
}
#[test]
fn historical_read_handle_requires_server_when_local_history_is_partial() {
    let (_writer_dir, mut writer) = open_node_with_uuid(node(1));
    let (_core_dir, mut partial) = open_node_with_uuid(node(2));
    let mut oracle = Oracle::new();
    let (_tx, seq) = commit_global_and_oracle(
        &mut writer,
        &mut partial,
        &mut oracle,
        MergeableCommit::new("todos", row(5), 10).cells(title_cells("base")),
    );
    let shape = Query::from("todos").validate(&partial.catalogue.schema).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();

    let err = partial.at(seq).read(&shape, &binding).unwrap_err();
    assert!(matches!(err, Error::HistoricalReadRequiresServer));
    assert!(!partial.is_history_complete_for(&shape, seq));
}
#[test]
fn snapshot_reads_survive_mid_tx_current_winner_shift() {
    // The snapshot covered set is fixed at open time: an uncovered lawful
    // child may become the current winner, but it must not change reads
    // inside the already-open exclusive transaction.
    let (_dir, mut node_under_test) = open_node_with_uuid(node(3));
    let row = row(7);
    let high = TxId::new(TxTime::from(100), node(1));
    let parent = TxId::new(TxTime::from(200), node(1));
    let child = TxId::new(TxTime::from(201), node(1));

    ingest_relay_version(&mut node_under_test, high, 100, Vec::new(), row, "high");
    ingest_relay_version(&mut node_under_test, parent, 200, Vec::new(), row, "parent");
    node_under_test
        .apply_fate_update(
            high,
            Fate::Accepted,
            Some(GlobalSeq(1)),
            Some(DurabilityTier::Global),
        )
        .unwrap();
    node_under_test
        .apply_fate_update(
            parent,
            Fate::Accepted,
            Some(GlobalSeq(2)),
            Some(DurabilityTier::Global),
        )
        .unwrap();

    let tx_id = node_under_test.open_exclusive().unwrap();
    assert_eq!(
        node_under_test
            .tx_read(tx_id, "todos", row)
            .unwrap()
            .unwrap()
            .get("title")
            .unwrap(),
        &v("parent")
    );

    // Uncovered lawful arrival: dominates `parent` and becomes the current
    // winner. The snapshot covered set is unchanged.
    ingest_relay_version(&mut node_under_test, child, 201, vec![parent], row, "child");
    assert_eq!(
        node_under_test
            .current_rows("todos", DurabilityTier::Local)
            .unwrap()[0]
            .cell(&schema().tables[0], "title")
            .unwrap(),
        v("child"),
        "current reads follow the new winner"
    );
    assert_eq!(
        node_under_test
            .tx_read(tx_id, "todos", row)
            .unwrap()
            .unwrap()
            .get("title")
            .unwrap(),
        &v("parent"),
        "snapshot reads must remain stable across current-winner changes"
    );
}
