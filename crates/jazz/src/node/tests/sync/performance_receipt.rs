// Large policy-graph reset-ingest performance receipt and its fixture.

fn policy_graph_perf_schema_fixture() -> JazzSchema {
    let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../packages/jazz-tools/src/testing/fixtures/policy-graph-perf/schema-source.json");
    let source: serde_json::Value =
        serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    let source = serde_json::from_value::<std::collections::BTreeMap<_, _>>(
        source["mergedSchema"].clone(),
    )
    .unwrap()
    .into_iter()
    .collect();
    crate::schema::JazzSchema::new(&source).unwrap()
}

fn policy_graph_uuid(kind: u8, idx: u32) -> uuid::Uuid {
    let mut bytes = [kind; 16];
    bytes[12..].copy_from_slice(&idx.to_be_bytes());
    uuid::Uuid::from_bytes(bytes)
}

fn policy_graph_row(kind: u8, idx: u32) -> RowUuid {
    RowUuid(policy_graph_uuid(kind, idx))
}

fn policy_graph_author(kind: u8, idx: u32) -> AuthorSubject {
    AuthorSubject::for_test_uuid(policy_graph_uuid(kind, idx))
}

fn nullable(value: Option<Value>) -> Value {
    Value::Nullable(value.map(Box::new))
}

fn policy_graph_team_cells(
    corporation_id: uuid::Uuid,
    created_by: uuid::Uuid,
    name: impl Into<String>,
) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("c449".to_owned(), Value::Uuid(corporation_id)),
        ("c450".to_owned(), Value::Uuid(created_by)),
        ("c451".to_owned(), Value::Uuid(created_by)),
        ("c452".to_owned(), Value::Bool(false)),
        ("c142".to_owned(), Value::String(name.into())),
        ("c453".to_owned(), Value::U64(1)),
        ("c454".to_owned(), Value::U64(1)),
        ("c146".to_owned(), nullable(None)),
    ])
}

fn policy_graph_dropdown_cells(
    corporation_id: uuid::Uuid,
    created_by: uuid::Uuid,
    name: impl Into<String>,
) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("c449".to_owned(), Value::Uuid(corporation_id)),
        ("c450".to_owned(), Value::Uuid(created_by)),
        ("c451".to_owned(), Value::Uuid(created_by)),
        ("c452".to_owned(), Value::Bool(false)),
        ("c142".to_owned(), Value::String(name.into())),
        ("c453".to_owned(), Value::U64(1)),
        ("c454".to_owned(), Value::U64(1)),
        (
            "c734".to_owned(),
            nullable(Some(Value::String("solid".to_owned()))),
        ),
        (
            "c735".to_owned(),
            nullable(Some(Value::String("solid".to_owned()))),
        ),
        ("c736".to_owned(), Value::String("{}".to_owned())),
    ])
}

fn policy_graph_dropdown_entry_cells(dropdown: RowUuid, idx: usize) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("c724".to_owned(), Value::Uuid(dropdown.0)),
        ("c725".to_owned(), Value::String("data_entry".to_owned())),
        ("c726".to_owned(), Value::String(format!("field_{idx}"))),
        (
            "c727".to_owned(),
            Value::Array(vec![
                Value::String(format!("option_{idx}_a")),
                Value::String(format!("option_{idx}_b")),
                Value::String(format!("option_{idx}_c")),
            ]),
        ),
        (
            "c728".to_owned(),
            nullable(Some(Value::Bool(false))),
        ),
        ("c729".to_owned(), Value::Bool(true)),
        ("c488".to_owned(), nullable(Some(Value::I32(idx as i32)))),
        ("c730".to_owned(), Value::Bool(idx.is_multiple_of(3))),
        ("c731".to_owned(), nullable(None)),
        ("c732".to_owned(), nullable(None)),
        ("c733".to_owned(), nullable(Some(Value::I32(0)))),
    ])
}

fn assert_policy_graph_perf_fixture_matches_schema(schema: &JazzSchema) {
    for (table_name, column_name, expected_type) in [
        ("t50", "c632", ColumnType::I32),
        ("t67", "c488", ColumnType::I32.nullable()),
        ("t67", "c733", ColumnType::I32.nullable()),
    ] {
        let table = schema
            .tables
            .iter()
            .find(|candidate| candidate.name == table_name)
            .unwrap_or_else(|| panic!("policy-graph performance fixture is missing {table_name}"));
        let column = table
            .columns
            .iter()
            .find(|candidate| candidate.name == column_name)
            .unwrap_or_else(|| {
                panic!("policy-graph performance fixture is missing {table_name}.{column_name}")
            });
        assert_eq!(
            column.column_type,
            expected_type,
            "policy-graph performance fixture writes {table_name}.{column_name} as Value::I32"
        );
    }
}

fn policy_graph_version(
    schema: &JazzSchema,
    table: &str,
    row_uuid: RowUuid,
    tx_id: TxId,
    cells: &BTreeMap<String, Value>,
) -> VersionRecord {
    VersionRecord::from_cells(
        schema
            .tables
            .iter()
            .find(|candidate| candidate.name == table)
            .unwrap(),
        schema.version_id(),
        row_uuid,
        Vec::new(),
        AuthorSubject::SYSTEM,
        tx_id.time.physical_ms(),
        AuthorSubject::SYSTEM,
        tx_id.time.physical_ms(),
        cells,
        None,
    )
    .unwrap_or_else(|error| {
        panic!(
            "policy-graph performance fixture has invalid cells for table={table} row={row_uuid:?}: {error:?}; cells={cells:?}"
        )
    })
}

fn seed_policy_graph_known_global(
    core: &mut NodeState<RocksDbStorage>,
    schema: &JazzSchema,
    rows: Vec<(&str, RowUuid, BTreeMap<String, Value>)>,
) {
    for (idx, (table, row_uuid, cells)) in rows.iter().enumerate() {
        let global_time = GlobalTime((idx + 1) as u64);
        let tx_id = TxId::new(TxTime((idx + 1) as u64), node(0x21));
        let version = policy_graph_version(schema, table, *row_uuid, tx_id, cells);
        core.ingest_known_transaction(
            Transaction {
                tx_id,
                kind: TxKind::Mergeable,
                n_total_writes: 1,
                made_by: AuthorSubject::SYSTEM,
                permission_subject: None,
                base_snapshot: None,
                row_read_set: None,
                absent_read_set: None,
                predicate_read_set: None,
                user_metadata_json: None,
                contribution_merge: None,
            },
            vec![version],
            Fate::Accepted,
            Some(global_time),
            DurabilityTier::Global,
        )
        .unwrap();
    }
}

fn open_policy_graph_memory_node(node_uuid: NodeUuid, schema: JazzSchema) -> NodeState<MemoryStorage> {
    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    NodeState::new(node_uuid, schema, MemoryStorage::new(&refs).expect("valid memory storage families")).unwrap()
}

fn apply_policy_graph_reset_receipt<S>(
    storage_label: &str,
    reader: &mut NodeState<S>,
    shape: &ValidatedQuery,
    binding: &Binding,
    update: SyncMessage,
    entry_count: usize,
) -> std::time::Duration
where
    S: OrderedKvStorage + ReopenableStorage,
{
    register_shape_binding(reader, shape, binding);
    let apply_start = std::time::Instant::now();
    unsafe {
        std::env::set_var("GROOVE_TRACE_INDEX_BY", "1");
        std::env::set_var("JAZZ_SKIP_BULK_INGEST_ASSERTS", "1");
    }
    reader.apply_sync_message_settled(update).unwrap();
    unsafe {
        std::env::remove_var("GROOVE_TRACE_INDEX_BY");
        std::env::remove_var("JAZZ_SKIP_BULK_INGEST_ASSERTS");
    }
    let apply_elapsed = apply_start.elapsed();
    let rows = reader
        .query_rows(shape, binding, DurabilityTier::Global)
        .unwrap();
    assert_eq!(rows.len(), entry_count, "{storage_label}");
    println!(
        "policy_graph_perf_dropdown_entry_reset_ingest_apply storage={storage_label} apply_ms={:.3}",
        apply_elapsed.as_secs_f64() * 1000.0
    );
    apply_elapsed
}

#[ignore = "#1787: manual receipt"]
#[test]
fn policy_graph_perf_dropdown_entry_reset_ingest_timing_receipt() {
    jazz_benchmark_guard::refuse_contaminated_measurement();
    let schema = policy_graph_perf_schema_fixture();
    assert_policy_graph_perf_fixture_matches_schema(&schema);
    let (_core_dir, mut core) = open_node_with_schema(node(0x22), schema.clone());

    let member = policy_graph_author(0x31, 1);
    let corp = policy_graph_row(0x32, 1);
    let member_team = RowUuid(member.test_uuid());
    let access_team = policy_graph_row(0x33, 1);
    let dropdown_count = 30usize;
    let entry_count = 19_894usize;
    let mut seed_rows = Vec::new();
    let seed_start = std::time::Instant::now();
    let member_claims = BTreeMap::from([
        ("sub".to_owned(), Value::Uuid(member.test_uuid())),
        ("user_id".to_owned(), Value::Uuid(member.test_uuid())),
        ("isAdmin".to_owned(), Value::Bool(false)),
    ]);
    core.set_test_provider_claims(member, member_claims.clone());

    seed_rows.push((
        "t1",
        member_team,
        policy_graph_team_cells(member_team.0, member_team.0, "member"),
    ));
    seed_rows.push((
        "t1",
        access_team,
        policy_graph_team_cells(member_team.0, member_team.0, "access"),
    ));
    seed_rows.push((
        "t50",
        corp,
        BTreeMap::from([
            ("c457".to_owned(), Value::Uuid(member_team.0)),
            ("c632".to_owned(), Value::I32(0)),
        ]),
    ));
    seed_rows.push((
        "t188",
        policy_graph_row(0x34, 1),
        BTreeMap::from([
            ("c457".to_owned(), Value::Uuid(member_team.0)),
            ("c1948".to_owned(), Value::Uuid(access_team.0)),
            (
                "c1949".to_owned(),
                nullable(Some(Value::Uuid(member_team.0))),
            ),
            ("c459".to_owned(), Value::Bool(false)),
            ("c1950".to_owned(), Value::U64(1)),
        ]),
    ));
    seed_rows.push((
        "t188",
        policy_graph_row(0x34, 2),
        BTreeMap::from([
            ("c457".to_owned(), Value::Uuid(access_team.0)),
            ("c1948".to_owned(), Value::Uuid(access_team.0)),
            (
                "c1949".to_owned(),
                nullable(Some(Value::Uuid(member_team.0))),
            ),
            ("c459".to_owned(), Value::Bool(false)),
            ("c1950".to_owned(), Value::U64(1)),
        ]),
    ));
    for parent_idx in 0..dropdown_count {
        let dropdown = policy_graph_row(0x40, parent_idx as u32);
        seed_rows.push((
            "t68",
            dropdown,
            policy_graph_dropdown_cells(
                member_team.0,
                member_team.0,
                format!("dropdown_{parent_idx}"),
            ),
        ));
        seed_rows.push((
            "t69",
            policy_graph_row(0x41, parent_idx as u32),
            BTreeMap::from([
                ("c456".to_owned(), Value::Uuid(dropdown.0)),
                ("c457".to_owned(), Value::Uuid(access_team.0)),
                ("c458".to_owned(), Value::String("EDITOR".to_owned())),
                ("c459".to_owned(), Value::Bool(false)),
            ]),
        ));
    }
    for idx in 0..entry_count {
        let parent = policy_graph_row(0x40, (idx % dropdown_count) as u32);
        seed_rows.push((
            "t67",
            policy_graph_row(0x50, idx as u32),
            policy_graph_dropdown_entry_cells(parent, idx),
        ));
    }
    seed_policy_graph_known_global(&mut core, &schema, seed_rows);
    let seed_elapsed = seed_start.elapsed();

    let shape = Query::from("t67").validate(&schema).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let mut peer = PeerState::new();
    let serve_start = std::time::Instant::now();
    let update = peer.rehydrate_query(&mut core, &shape, &binding).unwrap();
    let serve_elapsed = serve_start.elapsed();
    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        result_member_adds,
        version_carriers,
        ..
    }) = &update
    else {
        panic!("expected view update");
    };
    let result_member_count = result_member_adds.len();
    let version_bundle_count = crate::protocol::expand_version_carriers(version_carriers)
        .expect("performance receipt carriers should expand")
        .len();
    assert!(result_member_count >= entry_count);

    let mut memory_reader = open_policy_graph_memory_node(node(0x23), schema.clone());
    let memory_elapsed = apply_policy_graph_reset_receipt(
        "memory",
        &mut memory_reader,
        &shape,
        &binding,
        update.clone(),
        entry_count,
    );
    let (_rocks_dir, mut rocks_reader) = open_node_with_schema(node(0x24), schema.clone());
    let rocks_elapsed = apply_policy_graph_reset_receipt(
        "rocksdb",
        &mut rocks_reader,
        &shape,
        &binding,
        update.clone(),
        entry_count,
    );
    println!(
        "policy_graph_perf_dropdown_entry_reset_ingest_timing child_rows={entry_count} result_members={result_member_count} version_bundles={version_bundle_count} parents={dropdown_count} seed_ms={:.3} serve_ms={:.3} memory_apply_ms={:.3} rocksdb_apply_ms={:.3}",
        seed_elapsed.as_secs_f64() * 1000.0,
        serve_elapsed.as_secs_f64() * 1000.0,
        memory_elapsed.as_secs_f64() * 1000.0,
        rocks_elapsed.as_secs_f64() * 1000.0
    );
}
