fn node(byte: u8) -> NodeUuid {
    NodeUuid::from_bytes([byte; 16])
}
fn row(byte: u8) -> RowUuid {
    RowUuid::from_bytes([byte; 16])
}
fn branch(byte: u8) -> BranchId {
    BranchId::from_bytes([byte; 16])
}
fn assert_currency_tables_match_storage<S>(node: &mut NodeState<S>, table: &str)
where
    S: OrderedKvStorage,
{
    let versions = node.query_table_versions(table).unwrap();
    let mut local_expected = BTreeMap::<(RowUuid, VersionLayer), TxId>::new();
    let mut global_expected = BTreeMap::<(RowUuid, VersionLayer), TxId>::new();
    for version in &versions {
        let tx_id = node.version_tx_id(version).unwrap();
        let key = (version.row_uuid(), version.layer());
        local_expected
            .entry(key)
            .and_modify(|winner| *winner = (*winner).max(tx_id))
            .or_insert(tx_id);
        if matches!(
            node.query_transaction(tx_id).unwrap().map(|tx| tx.fate),
            Some(Fate::Accepted)
        ) {
            global_expected
                .entry(key)
                .and_modify(|winner| *winner = (*winner).max(tx_id))
                .or_insert(tx_id);
        }
    }

    for ((row_uuid, layer), expected_tx) in local_expected {
        let actual = node
            .query_local_layer_winner(table, row_uuid, layer)
            .unwrap()
            .map(|winner| node.version_tx_id(&winner).unwrap());
        assert_eq!(
            actual,
            Some(expected_tx),
            "local argmax winner must match stored versions for {table}/{row_uuid:?}/{layer:?}"
        );
    }

    let mut actual_global = BTreeMap::<(RowUuid, VersionLayer), TxId>::new();
    for (storage_table, layer) in [
        (global_current_table_name(table), VersionLayer::Content),
        (
            register_global_current_table_name(table),
            VersionLayer::Deletion,
        ),
    ] {
        for raw in node
            .database
            .primary_key_scan_raw(&storage_table, &[])
            .unwrap()
        {
            let record = raw.record();
            let row_uuid = RowUuid(
                record
                    .get_uuid(GlobalCurrentRowRecord::FIELD_ROW_UUID_IDX)
                    .unwrap(),
            );
            let tx_time = TxTime(
                record
                    .get_u64(GlobalCurrentRowRecord::FIELD_TX_TIME_IDX)
                    .unwrap(),
            );
            let tx_node_alias = NodeAlias(
                record
                    .get_u64(GlobalCurrentRowRecord::FIELD_TX_NODE_ID_IDX)
                    .unwrap(),
            );
            let tx_node = node.node_for_alias(tx_node_alias).unwrap();
            actual_global.insert((row_uuid, layer), TxId::new(tx_time, tx_node));
        }
    }
    assert_eq!(
        actual_global, global_expected,
        "global-current tables must equal accepted argmax winners for {table}"
    );
}
fn schema() -> JazzSchema {
    JazzSchema::new([TableSchema::new(
        "todos",
        [ColumnSchema::new("title", ColumnType::String)],
    )])
}
fn global_winner_tx<S>(
    node: &mut NodeState<S>,
    table: &str,
    row_uuid: RowUuid,
    layer: VersionLayer,
) -> Option<TxId>
where
    S: OrderedKvStorage,
{
    let winner = node
        .query_global_layer_winner(table, row_uuid, layer)
        .unwrap()?;
    Some(node.version_tx_id(&winner).unwrap())
}
fn owner_policy_schema() -> JazzSchema {
    JazzSchema::new([TableSchema::new(
        "todos",
        [
            ColumnSchema::new("title", ColumnType::String),
            ColumnSchema::new("owner", ColumnType::Uuid),
        ],
    )
    .with_read_policy(Policy::owner_only("todos", "owner"))
    .with_write_policy(Policy::owner_only("todos", "owner"))])
}
fn user(byte: u8) -> AuthorId {
    AuthorId::from_bytes([byte; 16])
}
fn owner_cells(author: AuthorId, title: impl Into<String>) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("title".to_owned(), Value::String(title.into())),
        ("owner".to_owned(), Value::Uuid(author.0)),
    ])
}
fn owner_cells_with_author(owner: AuthorId, title: impl Into<String>) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("title".to_owned(), Value::String(title.into())),
        ("owner".to_owned(), Value::Uuid(owner.0)),
    ])
}
fn v(value: impl Into<String>) -> Value {
    Value::String(value.into())
}
fn title_cells(title: impl Into<String>) -> BTreeMap<String, Value> {
    BTreeMap::from([("title".to_owned(), v(title))])
}
fn current_row_pair(row: CurrentRow) -> (RowUuid, BTreeMap<String, Value>) {
    (row.row_uuid(), row.test_cells_by_descriptor())
}
fn version_record<V: Into<Value> + Clone>(
    row_uuid: RowUuid,
    parents: Vec<TxId>,
    cells: BTreeMap<String, V>,
    deletion: Option<DeletionEvent>,
) -> VersionRecord {
    let schema = schema();
    VersionRecord::from_cells(
        &schema.tables[0],
        schema.version_id(),
        row_uuid,
        parents,
        AuthorId::SYSTEM,
        TxTime(1),
        AuthorId::SYSTEM,
        TxTime(1),
        &cells,
        deletion,
    )
    .unwrap()
}
fn version_record_cells(record: &VersionRecord, table: &TableSchema) -> BTreeMap<String, Value> {
    table
        .columns
        .iter()
        .enumerate()
        .filter_map(|(idx, column)| {
            record
                .cell_at(idx)
                .map(|value| (column.name.clone(), value))
        })
        .collect()
}
fn run_lens_parallel_materialization_seed(seed: u64) {
    let (schemas, lenses) = s7_schema_chain();
    let (_v1_dir, mut writer_v1) = open_node_with_schema(node(0x51), schemas[0].clone());
    let (_v4_dir, mut writer_v4) = open_node_with_schema(node(0x54), schemas[3].clone());
    let (_core_dir, mut core) = open_node_with_schema(node(0x59), schemas[0].clone());
    let mut oracle = ParallelMaterializationOracle::new();
    oracle.publish_schema(schemas[0].version_id());
    for schema in schemas.iter().skip(1) {
        let payload = SchemaVersion::new(schema.clone());
        oracle.publish_schema(payload.id);
        core.apply_sync_message(SyncMessage::PublishSchema {
            author: AuthorId::SYSTEM,
            schema: Box::new(payload),
        })
        .unwrap();
    }
    for lens in lenses {
        oracle.publish_lens(lens.clone());
        core.apply_sync_message(SyncMessage::PublishLens {
            author: AuthorId::SYSTEM,
            lens,
        })
        .unwrap();
    }
    core.apply_sync_message(SyncMessage::SetCurrentWriteSchema {
        author: AuthorId::SYSTEM,
        pointer: CurrentWriteSchema {
            revision: 4,
            schema: schemas[3].version_id(),
        },
    })
    .unwrap();

    for idx in 0..12u8 {
        let row_uuid = row(idx.wrapping_add((seed as u8) & 0x0f));
        if (seed + idx as u64).is_multiple_of(2) {
            let title = format!("v1-{seed}-{idx}");
            let (_tx, unit) = writer_v1
                .commit_mergeable_unit(
                    MergeableCommit::new("todos", row_uuid, 1_000 + idx as u64)
                        .cells(title_cells(title.clone())),
                )
                .unwrap();
            core.apply_sync_message(unit).unwrap();
            oracle.apply_accepted_write(
                schemas[0].version_id(),
                row_uuid,
                BTreeMap::from([("title".to_owned(), v(title))]),
            );
        } else {
            let name = format!("v4-{seed}-{idx}");
            let cells = BTreeMap::from([
                ("name".to_owned(), v(name.clone())),
                ("search_name".to_owned(), v(name.clone())),
            ]);
            let (_tx, unit) = writer_v4
                .commit_mergeable_unit(
                    MergeableCommit::new("todos", row_uuid, 1_000 + idx as u64)
                        .cells(cells.clone()),
                )
                .unwrap();
            core.apply_sync_message(unit).unwrap();
            oracle.apply_accepted_write(schemas[3].version_id(), row_uuid, cells);
        }
    }

    for schema in schemas {
        let shape = Query::from("todos").validate(&schema).unwrap();
        let engine = core
            .query_rows(
                &shape,
                &shape.bind(BTreeMap::new()).unwrap(),
                DurabilityTier::Local,
            )
            .unwrap()
            .into_iter()
            .map(current_row_pair)
            .collect::<BTreeMap<_, _>>();
        assert_eq!(
            engine,
            oracle.rows(schema.version_id()),
            "seed {seed} schema {:?}",
            schema.version_id()
        );
    }
}
fn s7_schema_chain() -> ([JazzSchema; 4], Vec<MigrationLens>) {
    let v1 = JazzSchema::new([TableSchema::new(
        "todos",
        [ColumnSchema::new("title", ColumnType::String)],
    )]);
    let v2 = JazzSchema::new([TableSchema::new(
        "todos",
        [
            ColumnSchema::new("title", ColumnType::String),
            ColumnSchema::new("body", ColumnType::String),
        ],
    )]);
    let v3 = JazzSchema::new([TableSchema::new(
        "todos",
        [ColumnSchema::new("name", ColumnType::String)],
    )]);
    let v4 = JazzSchema::new([TableSchema::new(
        "todos",
        [
            ColumnSchema::new("name", ColumnType::String),
            ColumnSchema::new("search_name", ColumnType::String),
        ],
    )]);
    let lenses = vec![
        MigrationLens::new(
            v1.version_id(),
            v2.version_id(),
            vec![TableLens {
                source_table: "todos".to_owned(),
                target_table: "todos".to_owned(),
                ops: vec![LensOp::AddColumn {
                    column: "body".to_owned(),
                    default: v(""),
                }],
            }],
        ),
        MigrationLens::new(
            v2.version_id(),
            v3.version_id(),
            vec![TableLens {
                source_table: "todos".to_owned(),
                target_table: "todos".to_owned(),
                ops: vec![
                    LensOp::RenameColumn {
                        from: "title".to_owned(),
                        to: "name".to_owned(),
                    },
                    LensOp::DropColumn {
                        column: "body".to_owned(),
                        backwards_default: v(""),
                    },
                ],
            }],
        ),
        MigrationLens::new(
            v3.version_id(),
            v4.version_id(),
            vec![TableLens {
                source_table: "todos".to_owned(),
                target_table: "todos".to_owned(),
                ops: vec![LensOp::CopyColumn {
                    from: "name".to_owned(),
                    to: "search_name".to_owned(),
                }],
            }],
        ),
    ];
    ([v1, v2, v3, v4], lenses)
}
fn catalogue_evolved_schema() -> JazzSchema {
    JazzSchema::new([TableSchema::new(
        "todos",
        [
            ColumnSchema::new("title", ColumnType::String),
            ColumnSchema::new("body", ColumnType::String),
        ],
    )])
}
fn open_node() -> (tempfile::TempDir, NodeState<RocksDbStorage>) {
    let schema = schema();
    let temp_dir = tempfile::tempdir().unwrap();
    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = RocksDbStorage::open(temp_dir.path(), &refs).unwrap();
    let node = NodeState::new(node(1), schema, storage).unwrap();
    (temp_dir, node)
}
fn open_node_with_uuid(node_uuid: NodeUuid) -> (tempfile::TempDir, NodeState<RocksDbStorage>) {
    let schema = schema();
    let temp_dir = tempfile::tempdir().unwrap();
    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = RocksDbStorage::open(temp_dir.path(), &refs).unwrap();
    let node = NodeState::new(node_uuid, schema, storage).unwrap();
    (temp_dir, node)
}
fn open_node_at(temp_dir: &tempfile::TempDir, schema: JazzSchema) -> NodeState<RocksDbStorage> {
    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = RocksDbStorage::open(temp_dir.path(), &refs).unwrap();
    NodeState::new(node(1), schema, storage).unwrap()
}
fn reopen_node_at(
    temp_dir: &tempfile::TempDir,
    node_uuid: NodeUuid,
    schema: JazzSchema,
) -> NodeState<RocksDbStorage> {
    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = RocksDbStorage::open(temp_dir.path(), &refs).unwrap();
    NodeState::new(node_uuid, schema, storage).unwrap()
}
fn open_node_with_schema(
    node_uuid: NodeUuid,
    schema: JazzSchema,
) -> (tempfile::TempDir, NodeState<RocksDbStorage>) {
    open_node_with_schema_and_checkpoint_interval(
        node_uuid,
        schema,
        crate::node::LARGE_VALUE_CHECKPOINT_OP_INTERVAL,
    )
}
fn open_node_with_schema_and_checkpoint_interval(
    node_uuid: NodeUuid,
    schema: JazzSchema,
    checkpoint_interval: usize,
) -> (tempfile::TempDir, NodeState<RocksDbStorage>) {
    let temp_dir = tempfile::tempdir().unwrap();
    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = RocksDbStorage::open(temp_dir.path(), &refs).unwrap();
    let node = NodeState::new_with_large_value_checkpoint_op_interval(
        node_uuid,
        schema,
        storage,
        false,
        checkpoint_interval,
    )
    .unwrap();
    (temp_dir, node)
}
fn reopen_node_at_with_checkpoint_interval(
    temp_dir: &tempfile::TempDir,
    node_uuid: NodeUuid,
    schema: JazzSchema,
    checkpoint_interval: usize,
) -> NodeState<RocksDbStorage> {
    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = RocksDbStorage::open(temp_dir.path(), &refs).unwrap();
    NodeState::new_with_large_value_checkpoint_op_interval(
        node_uuid,
        schema,
        storage,
        false,
        checkpoint_interval,
    )
    .unwrap()
}
fn open_history_complete_node_with_schema(
    node_uuid: NodeUuid,
    schema: JazzSchema,
) -> (tempfile::TempDir, NodeState<RocksDbStorage>) {
    let temp_dir = tempfile::tempdir().unwrap();
    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = RocksDbStorage::open(temp_dir.path(), &refs).unwrap();
    let node = NodeState::new_history_complete(node_uuid, schema, storage).unwrap();
    (temp_dir, node)
}
fn two_column_schema() -> JazzSchema {
    JazzSchema::new([TableSchema::new(
        "todos",
        [
            ColumnSchema::new("title", ColumnType::String),
            ColumnSchema::new("body", ColumnType::String),
        ],
    )])
}
fn counter_schema() -> JazzSchema {
    JazzSchema::new([TableSchema::new(
        "counters",
        [
            ColumnSchema::new("count", ColumnType::U64),
            ColumnSchema::new("title", ColumnType::String),
        ],
    )
    .with_column_merge_strategy("count", MergeStrategy::Counter)])
}
fn commit_and_oracle(
    node: &mut NodeState<RocksDbStorage>,
    oracle: &mut Oracle,
    commit: MergeableCommit,
) -> TxId {
    let row_uuid = commit.row_uuid;
    let parents = commit.parents.clone();
    let cells = commit.cells.clone();
    let deletion = commit.deletion;
    let tx_id = node.commit_mergeable(commit).unwrap();
    let made_at = node.transaction_record(tx_id).unwrap().tx_id.time;
    let mut version = ModelRowVersion::new(row_uuid, tx_id, made_at);
    version.parents = parents;
    version.cells = cells;
    version.deletion = deletion;
    oracle.add_version(version);
    tx_id
}
fn commit_global_and_oracle(
    writer: &mut NodeState<RocksDbStorage>,
    core: &mut NodeState<RocksDbStorage>,
    oracle: &mut Oracle,
    commit: MergeableCommit,
) -> (TxId, GlobalSeq) {
    let row_uuid = commit.row_uuid;
    let parents = commit.parents.clone();
    let cells = commit.cells.clone();
    let deletion = commit.deletion;
    let (tx_id, unit) = writer.commit_mergeable_unit(commit).unwrap();
    let [fate] = core.apply_sync_message(unit).unwrap().try_into().unwrap();
    let SyncMessage::FateUpdate { global_seq, .. } = &fate else {
        panic!("expected accepted fate update");
    };
    let global_seq = global_seq.expect("core commit is globally accepted");
    writer.apply_sync_message(fate).unwrap();
    let mut version = ModelRowVersion::new(row_uuid, tx_id, tx_id.time);
    version.parents = parents;
    version.cells = cells;
    version.deletion = deletion;
    oracle.add_version(version);
    oracle.record_tx_state(
        tx_id,
        OracleTxState::new(Fate::Accepted, Some(global_seq), DurabilityTier::Global),
    );
    (tx_id, global_seq)
}
fn assert_current_rows_match_oracle(node: &mut NodeState<RocksDbStorage>, oracle: &Oracle) {
    let actual = node
        .current_rows("todos", DurabilityTier::Local)
        .unwrap()
        .into_iter()
        .map(current_row_pair)
        .collect::<BTreeMap<_, _>>();
    let expected = oracle
        .visible_current_versions()
        .into_iter()
        .map(|version| (version.row_uuid, version.cells.clone()))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(actual, expected);
}
fn assert_global_current_rows_match_oracle(node: &mut NodeState<RocksDbStorage>, oracle: &Oracle) {
    let actual = node
        .current_rows("todos", DurabilityTier::Global)
        .unwrap()
        .into_iter()
        .map(current_row_pair)
        .collect::<BTreeMap<_, _>>();
    let expected = oracle
        .visible_global_current_versions()
        .into_iter()
        .map(|version| (version.row_uuid, version.cells.clone()))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(actual, expected);
}
fn assert_subscription_rows_match_policy_oracle(
    node: &mut NodeState<RocksDbStorage>,
    _subscription_ordinal: u64,
    oracle: &Oracle,
    delivered: &PerNodeKnowledge,
    identity: AuthorId,
) {
    let actual = node
        .subscription_current_rows("todos", DurabilityTier::Global)
        .unwrap()
        .into_iter()
        .map(current_row_pair)
        .collect::<BTreeMap<_, _>>();
    let expected = local_policy_oracle_rows(oracle, delivered, identity);
    assert_eq!(
        actual, expected,
        "subscription entries: {:?}; tx states: {:?}",
        delivered.subscription_entries, delivered.states
    );
}
fn assert_settled_result_sets_unique(
    node: &NodeState<RocksDbStorage>,
    subscription_ordinal: u64,
    seed: u64,
) {
    // Subscriber-side settled binding-view result-set/completeness state, not peer shipped-state.
    let subscription = node.whole_table_subscription_key("todos").unwrap();
    let binding_view_key =
        crate::protocol::BindingViewKey::from_canonical_subscription_key(subscription);
    let Some(result_set) = node.query.settled_result_sets.get(&binding_view_key) else {
        return;
    };
    let row_result_set = result_set
        .iter()
        .filter_map(crate::protocol::ResultMemberEntry::as_row)
        .collect::<BTreeSet<_>>();
    if let Some((table, row_uuid, first, second)) = duplicate_row_result_set(&row_result_set) {
        panic!(
            "seed {seed}: subscription {subscription_ordinal} has multiple content versions for {table}.{row_uuid:?}: {first:?} and {second:?}"
        );
    }
}
#[derive(Clone, Debug, Default)]
struct PerNodeKnowledge {
    tx_ids: BTreeSet<TxId>,
    version_keys: BTreeSet<(TxId, RowUuid)>,
    subscription_entries: BTreeSet<(TxId, RowUuid)>,
    delivered_rows: BTreeMap<(TxId, RowUuid), BTreeMap<String, Value>>,
    states: BTreeMap<TxId, OracleTxState>,
}
impl PerNodeKnowledge {
    fn record_local_commit(&mut self, tx_id: TxId) {
        self.tx_ids.insert(tx_id);
        self.states.insert(
            tx_id,
            OracleTxState::new(Fate::Pending, None, DurabilityTier::Local),
        );
    }

    fn record_fate_delivery(&mut self, message: &SyncMessage) {
        let SyncMessage::FateUpdate {
            tx_id,
            fate,
            global_seq,
            durability,
        } = message
        else {
            return;
        };
        self.tx_ids.insert(*tx_id);
        let durability = durability.unwrap_or_else(|| {
            self.states
                .get(tx_id)
                .map(|state| state.durability)
                .unwrap_or(DurabilityTier::None)
        });
        self.states.insert(
            *tx_id,
            OracleTxState::new(fate.clone(), *global_seq, durability),
        );
    }

    fn record_view_delivery(&mut self, message: &SyncMessage) {
        let SyncMessage::ViewUpdate {
            version_bundles,
            reset_result_set,
            peer_payload_inventory,
            result_member_adds,
            result_member_removes,
            program_fact_adds,
            program_fact_removes,
            ..
        } = message
        else {
            return;
        };
        // Keep this condition in sync with NodeState::apply_view_update in
        // node/views.rs: empty resets against non-empty shared state are
        // coverage stamps, not replacement snapshots. Sanctioned by reviewer
        // instruction for the Plan 5 close-out seed 2210401 diagnosis.
        let empty_reset = *reset_result_set
            && version_bundles.is_empty()
            && peer_payload_inventory.complete_tx_payloads.is_empty()
            && result_member_adds.is_empty()
            && result_member_removes.is_empty()
            && program_fact_adds.is_empty()
            && program_fact_removes.is_empty();
        let preserve_existing_shared_state =
            empty_reset && !self.subscription_entries.is_empty();
        if *reset_result_set && !preserve_existing_shared_state {
            self.subscription_entries.clear();
        }
        let result_add_keys = result_member_adds
            .iter()
            .filter_map(crate::protocol::ResultMemberEntry::as_row)
            .map(|(_, row_uuid, tx_id)| (tx_id, row_uuid))
            .collect::<BTreeSet<_>>();
        let table_schema = owner_policy_schema().tables[0].clone();
        for bundle in version_bundles {
            for version in &bundle.versions {
                let row_uuid = version.row_uuid();
                if version.deletion().is_none() {
                    self.delivered_rows.insert(
                        (bundle.tx.tx_id, row_uuid),
                        version_record_cells(version, &table_schema),
                    );
                }
            }
        }
        for (_, row_uuid, tx_id) in result_member_adds
            .iter()
            .filter_map(crate::protocol::ResultMemberEntry::as_row)
        {
            self.subscription_entries.insert((tx_id, row_uuid));
        }
        for (_, row_uuid, tx_id) in result_member_removes
            .iter()
            .filter_map(crate::protocol::ResultMemberEntry::as_row)
        {
            self.subscription_entries.remove(&(tx_id, row_uuid));
        }
        for bundle in version_bundles {
            if usize::try_from(bundle.tx.n_total_writes).ok() == Some(bundle.versions.len()) {
                self.tx_ids.insert(bundle.tx.tx_id);
            } else {
                self.version_keys.extend(
                    bundle
                        .versions
                        .iter()
                        .filter(|version| {
                            result_add_keys.contains(&(bundle.tx.tx_id, version.row_uuid()))
                        })
                        .map(|version| (bundle.tx.tx_id, version.row_uuid())),
                );
            }
            self.states.insert(
                bundle.tx.tx_id,
                OracleTxState::new(bundle.fate.clone(), bundle.global_seq, bundle.durability),
            );
        }
    }
}
fn local_policy_oracle_rows(
    _oracle: &Oracle,
    delivered: &PerNodeKnowledge,
    _identity: AuthorId,
) -> BTreeMap<RowUuid, BTreeMap<String, Value>> {
    delivered
        .subscription_entries
        .iter()
        .filter_map(|(tx_id, row_uuid)| {
            delivered
                .delivered_rows
                .get(&(*tx_id, *row_uuid))
                .map(|cells| (*row_uuid, cells.clone()))
        })
        .collect()
}
fn global_known_oracle_rows(
    oracle: &Oracle,
    known: &PerNodeKnowledge,
) -> BTreeMap<RowUuid, BTreeMap<String, Value>> {
    oracle
        .visible_global_current_versions_known_to(&known.tx_ids, &known.states)
        .into_iter()
        .map(|version| (version.row_uuid, version.cells.clone()))
        .collect()
}
fn assert_global_rows_match_known_oracle(
    node: &mut NodeState<RocksDbStorage>,
    oracle: &Oracle,
    known: &PerNodeKnowledge,
) {
    let actual = node
        .current_rows("todos", DurabilityTier::Global)
        .unwrap()
        .into_iter()
        .map(current_row_pair)
        .collect::<BTreeMap<_, _>>();
    assert_eq!(actual, global_known_oracle_rows(oracle, known));
}
fn assert_view_update_result_set_matches_current_rows(node: &mut NodeState<RocksDbStorage>) {
    let update = node.view_update_for_current_rows("todos").unwrap();
    let SyncMessage::ViewUpdate {
        version_bundles: _,
        peer_payload_inventory: crate::protocol::PeerPayloadInventory { complete_tx_payloads: complete_tx_payload_refs },
        result_member_adds,
        result_member_removes,
        ..
    } = update
    else {
        panic!("expected view update");
    };
    assert!(
        complete_tx_payload_refs.is_empty(),
        "full view recomputation should carry bundles for every visible member"
    );
    assert!(result_member_removes.is_empty());
    let result_rows = result_member_adds
        .iter()
        .filter_map(crate::protocol::ResultMemberEntry::as_row)
        .map(|(_, row_uuid, _)| row_uuid)
        .collect::<BTreeSet<_>>();
    let groove_rows = node
        .current_rows("todos", DurabilityTier::Local)
        .unwrap()
        .into_iter()
        .map(|row| row.row_uuid())
        .collect::<BTreeSet<_>>();
    assert_eq!(result_rows, groove_rows);
}
#[derive(Clone, Debug)]
struct Lcg(u64);

impl Lcg {
    fn new(seed: u64) -> Self {
        Self(seed)
    }

    fn next_u64(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0
    }

    fn choose(&mut self, upper: usize) -> usize {
        (self.next_u64() as usize) % upper
    }

    fn chance(&mut self, numerator: u64, denominator: u64) -> bool {
        self.next_u64() % denominator < numerator
    }
}
fn commit_from_rng(rng: &mut Lcg, step: u64, rows: &[RowUuid]) -> MergeableCommit {
    let row_uuid = rows[rng.choose(rows.len())];
    let now_ms = 1_000 + rng.choose(12) as u64;
    if rng.chance(1, 4) {
        MergeableCommit::new("todos", row_uuid, now_ms).deletion(if rng.chance(1, 3) {
            DeletionEvent::Restored
        } else {
            DeletionEvent::Deleted
        })
    } else {
        MergeableCommit::new("todos", row_uuid, now_ms).cells(BTreeMap::from([(
            "title".to_owned(),
            format!("seeded-{step}-{}", rng.next_u64() % 1_000),
        )]))
    }
}
fn seeded_author_and_owner(
    rng: &mut Lcg,
    default_author: AuthorId,
    author_a: AuthorId,
    author_b: AuthorId,
) -> (AuthorId, AuthorId) {
    if rng.chance(1, 5) {
        let owner = if rng.chance(1, 2) { author_a } else { author_b };
        (AuthorId::SYSTEM, owner)
    } else {
        (default_author, default_author)
    }
}
fn add_core_versions_to_oracle(
    core: &mut NodeState<RocksDbStorage>,
    oracle: &mut Oracle,
    known_txs: &mut BTreeSet<TxId>,
) {
    for version in core.query_all_versions().unwrap() {
        let tx_id = core.version_tx_id(&version).unwrap();
        if tx_id.node == node(9) && known_txs.insert(tx_id) {
            let table_schema = core.table(&version.table).unwrap().clone();
            let cells = version.cells(&table_schema).unwrap();
            let parents = version.parents();
            assert_eq!(
                cells,
                oracle.merged_cells_for_parents(version.row_uuid(), &parents),
                "core merge cells must match oracle merge semantics"
            );
            let made_at = core.transaction_record(tx_id).unwrap().tx_id.time;
            let mut model = ModelRowVersion::new(version.row_uuid(), tx_id, made_at);
            model.parents = parents;
            model.cells = cells;
            model.deletion = version.deletion();
            oracle.add_version(model);
            let (fate, global_seq, durability) = core
                .transaction_state(tx_id)
                .expect("core-created version must have transaction state");
            oracle.record_tx_state(tx_id, OracleTxState::new(fate, global_seq, durability));
        }
    }
}
fn record_fate_update_in_oracle(oracle: &mut Oracle, fate: &SyncMessage) {
    let SyncMessage::FateUpdate {
        tx_id,
        fate,
        global_seq,
        durability,
    } = fate
    else {
        panic!("expected fate update");
    };
    let durability = durability.unwrap_or_else(|| {
        oracle
            .tx_state(*tx_id)
            .map(|state| state.durability)
            .unwrap_or(DurabilityTier::None)
    });
    oracle.record_tx_state(
        *tx_id,
        OracleTxState::new(fate.clone(), *global_seq, durability),
    );
}
fn add_commit_unit_versions_to_oracle(
    tx: &Transaction,
    versions: &[VersionRecord],
    table_schema: &TableSchema,
    oracle: &mut Oracle,
) {
    for version in versions {
        let mut model = ModelRowVersion::new(version.row_uuid(), tx.tx_id, tx.tx_id.time);
        model.parents = version.parents();
        model.cells = version_record_cells(version, table_schema);
        model.deletion = version.deletion();
        oracle.add_version(model);
    }
}
fn assert_exclusive_serialization_matches_oracle(
    seed: u64,
    oracle: &Oracle,
    txs: &[Transaction],
    owner_shape_id: ShapeId,
    owner_bindings: &BTreeMap<BindingId, AuthorId>,
) {
    for tx in txs {
        let Some(state) = oracle.tx_state(tx.tx_id) else {
            panic!("seed {seed}: exclusive {:?} has no fate", tx.tx_id);
        };
        if !matches!(state.fate, Fate::Accepted) {
            continue;
        }
        let Some(global_seq) = state.global_seq else {
            panic!(
                "seed {seed}: accepted exclusive {:?} has no global seq",
                tx.tx_id
            );
        };
        let Some(serialization_base) = global_seq.0.checked_sub(1).map(GlobalSeq) else {
            panic!("seed {seed}: invalid global seq for {:?}", tx.tx_id);
        };
        let base = tx
            .base_snapshot
            .as_ref()
            .expect("accepted exclusive must carry a base snapshot")
            .global_base;
        for read in tx.row_read_set.as_deref().unwrap_or(&[]) {
            assert!(
                oracle.exclusive_row_read_matches_at(read, serialization_base),
                "seed {seed}: accepted exclusive {:?} row read {:?} was not visible winner at {:?}",
                tx.tx_id,
                read,
                serialization_base
            );
        }
        for absent in tx.absent_read_set.as_deref().unwrap_or(&[]) {
            assert!(
                oracle.exclusive_absent_read_matches_at(absent, serialization_base),
                "seed {seed}: accepted exclusive {:?} absent read {:?} was present at {:?}",
                tx.tx_id,
                absent,
                serialization_base
            );
        }
        for predicate in tx.predicate_read_set.as_deref().unwrap_or(&[]) {
            let (at_base, at_serialization) = if predicate.shape_id == owner_shape_id {
                let Some(owner) = owner_bindings.get(&predicate.binding_id) else {
                    panic!(
                        "seed {seed}: accepted exclusive {:?} used unknown owner binding {:?}",
                        tx.tx_id, predicate.binding_id
                    );
                };
                (
                    oracle.visible_global_content_set_at_owner(base, *owner),
                    oracle.visible_global_content_set_at_owner(serialization_base, *owner),
                )
            } else {
                (
                    oracle.visible_global_content_set_at(base),
                    oracle.visible_global_content_set_at(serialization_base),
                )
            };
            assert_eq!(
                at_base, at_serialization,
                "seed {seed}: accepted exclusive {:?} predicate {:?} changed between {:?} and {:?}",
                tx.tx_id, predicate, base, serialization_base
            );
        }
    }
}
fn commit_mergeable_global(
    writer: &mut NodeState<RocksDbStorage>,
    core: &mut NodeState<RocksDbStorage>,
    commit: MergeableCommit,
) -> TxId {
    let (tx_id, unit) = writer.commit_mergeable_unit(commit).unwrap();
    let [fate] = core.apply_sync_message(unit).unwrap().try_into().unwrap();
    writer.apply_sync_message(fate).unwrap();
    tx_id
}
fn ingest_relay_version(
    node: &mut NodeState<RocksDbStorage>,
    tx_id: TxId,
    made_at: impl Into<TxTime>,
    parents: Vec<TxId>,
    row_uuid: RowUuid,
    title: &str,
) {
    let made_at = made_at.into();
    assert_eq!(tx_id.time, made_at);
    node.ingest_relay_commit_unit(
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
            merge_strategy: None,
        },
        vec![version_record(
            row_uuid,
            parents,
            BTreeMap::from([("title".to_owned(), title.to_owned())]),
            None,
        )],
    )
    .unwrap();
}
fn sync_current_rows_to(
    core: &mut NodeState<RocksDbStorage>,
    reader: &mut NodeState<RocksDbStorage>,
    _subscription_ordinal: u64,
) {
    let mut peer = PeerState::new();
    let update = peer.current_rows_update(core, "todos").unwrap();
    reader.apply_sync_message(update).unwrap();
}
fn register_shape_binding(
    node: &mut NodeState<RocksDbStorage>,
    shape: &ValidatedQuery,
    binding: &Binding,
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
        .collect::<Vec<_>>();
    node.apply_sync_message(SyncMessage::Subscribe(crate::protocol::Subscribe {
        shape_id: shape.shape_id(),
        subscription: crate::protocol::SubscriptionKey {
            shape_id: shape.shape_id(),
            binding_id: binding.binding_id(),
        read_view: Default::default(),
},
        values,
        known_state: None,
    }))
    .unwrap();
}
fn commit_owner_policy_global(
    writer: &mut NodeState<RocksDbStorage>,
    core: &mut NodeState<RocksDbStorage>,
    row_uuid: RowUuid,
    made_by: AuthorId,
    owner: AuthorId,
    title: &str,
    now_ms: u64,
) -> TxId {
    let (tx_id, unit) = writer
        .commit_mergeable_unit(
            MergeableCommit::new("todos", row_uuid, now_ms)
                .made_by(made_by)
                .cells(owner_cells(owner, title)),
        )
        .unwrap();
    let expected_global_seq = core.clock.next_global_seq;
    let [fate] = core.apply_sync_message(unit).unwrap().try_into().unwrap();
    assert_eq!(
        fate,
        SyncMessage::FateUpdate {
            tx_id,
            fate: Fate::Accepted,
            global_seq: Some(expected_global_seq),
            durability: Some(DurabilityTier::Global),
        }
    );
    writer.apply_sync_message(fate).unwrap();
    tx_id
}
fn commit_core_owner_fixture(
    core: &mut NodeState<RocksDbStorage>,
    row_uuid: RowUuid,
    owner: AuthorId,
    title: &str,
    now_ms: u64,
) -> TxId {
    let tx_id = core
        .commit_mergeable(
            MergeableCommit::new("todos", row_uuid, now_ms)
                .made_by(owner)
                .cells(owner_cells(owner, title)),
        )
        .unwrap();
    core.apply_fate_update(
        tx_id,
        Fate::Accepted,
        Some(core.clock.next_global_seq),
        Some(DurabilityTier::Global),
    )
    .unwrap();
    tx_id
}
fn assert_view_update_only_references_rows(update: &SyncMessage, expected_rows: BTreeSet<RowUuid>) {
    let SyncMessage::ViewUpdate {
        version_bundles,
        result_member_adds,
        ..
    } = update
    else {
        panic!("expected view update");
    };
    let result_txs = result_member_adds
        .iter()
        .filter_map(crate::protocol::ResultMemberEntry::as_row)
        .map(|(_, _, tx_id)| tx_id)
        .collect::<BTreeSet<_>>();
    let referenced_rows = version_bundles
        .iter()
        .filter(|bundle| result_txs.contains(&bundle.tx.tx_id))
        .flat_map(|bundle| bundle.versions.iter().map(|version| version.row_uuid()))
        .collect::<BTreeSet<_>>();
    assert_eq!(referenced_rows, expected_rows);
}
fn assert_view_update_only_ships_rows(update: &SyncMessage, expected_rows: BTreeSet<RowUuid>) {
    let SyncMessage::ViewUpdate {
        version_bundles, ..
    } = update
    else {
        panic!("expected view update");
    };
    let shipped_rows = version_bundles
        .iter()
        .flat_map(|bundle| bundle.versions.iter().map(|version| version.row_uuid()))
        .collect::<BTreeSet<_>>();
    assert_eq!(shipped_rows, expected_rows);
}
fn assert_policy_subscription_rows(
    reader: &mut NodeState<RocksDbStorage>,
    _subscription_ordinal: u64,
    identity: AuthorId,
) {
    let rows = reader
        .subscription_current_rows("todos", DurabilityTier::Local)
        .unwrap();
    assert!(!rows.is_empty());
    assert!(rows
        .iter()
        .all(|row| row.cell(&owner_policy_schema().tables[0], "owner")
            == Some(Value::Uuid(identity.0))));
}
fn enqueue_rehydrate_with_dedup_assertion(
    peer: &mut PeerState,
    core: &mut NodeState<RocksDbStorage>,
    _subscription_ordinal: u64,
    queue: &mut VecDeque<SyncMessage>,
) {
    let shipped_before = peer.shipped_complete_tx_payloads().clone();
    let bundles_before = peer.metrics.version_bundles_out;
    let subscription = core.whole_table_subscription_key("todos").unwrap();
    peer.forget_subscription(subscription);
    let update = peer.reset_current_rows(core, "todos").unwrap();
    let SyncMessage::ViewUpdate { version_bundles, .. } = &update else {
        panic!("expected view update");
    };
    let newly_shipped = version_bundles
        .iter()
        .filter(|bundle| !shipped_before.contains(&bundle.tx.tx_id))
        .count() as u64;
    assert!(
        version_bundles
            .iter()
            .all(|bundle| !shipped_before.contains(&bundle.tx.tx_id)),
        "rehydrate must not resend complete payload bundles already shipped on the peer"
    );
    assert_eq!(
        peer.metrics.version_bundles_out - bundles_before,
        newly_shipped,
        "rehydrate bundle metrics must only count newly shipped complete payloads"
    );
    queue.push_back(update);
}
#[derive(Clone, Debug, PartialEq, Eq)]
struct M3NodeSummary {
    local_rows: Vec<CurrentRow>,
    global_rows: Vec<CurrentRow>,
    transaction_records: BTreeMap<TxId, Option<TransactionRecord>>,
    sync_metrics: SyncMetrics,
}
#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct M3MessageCounts {
    upstream_enqueued: u64,
    upstream_delivered: u64,
    fate_enqueued: u64,
    fate_delivered: u64,
    view_enqueued: u64,
    view_delivered: u64,
}
#[derive(Clone, Debug, PartialEq, Eq)]
struct M3RunSummary {
    writer_a: M3NodeSummary,
    writer_b: M3NodeSummary,
    core: M3NodeSummary,
    reader_a: M3NodeSummary,
    reader_b: M3NodeSummary,
    link_a_metrics: PeerMetrics,
    link_b_metrics: PeerMetrics,
    message_counts: M3MessageCounts,
}
fn node_summary(node: &mut NodeState<RocksDbStorage>, tx_ids: &BTreeSet<TxId>) -> M3NodeSummary {
    M3NodeSummary {
        local_rows: node.current_rows("todos", DurabilityTier::Local).unwrap(),
        global_rows: node.current_rows("todos", DurabilityTier::Global).unwrap(),
        transaction_records: tx_ids
            .iter()
            .map(|tx_id| (*tx_id, node.transaction_record(*tx_id)))
            .collect(),
        sync_metrics: node.sync_metrics().clone(),
    }
}
fn m3_commit_target() -> u64 {
    // JAZZ_COMMIT_COUNT deepens each seed's workload for soak runs
    // (default 24 keeps CI fast).
    std::env::var("JAZZ_COMMIT_COUNT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(24)
}
fn run_m3_seed(seed: u64) -> M3RunSummary {
    let mut rng = Lcg::new(seed);
    let harness_schema = owner_policy_schema();
    let now_ms = 10_000;
    let author_a = user(0xa1);
    let author_b = user(0xb2);
    let (_writer_a_dir, mut writer_a) = open_node_with_schema(node(1), harness_schema.clone());
    let (_writer_b_dir, mut writer_b) = open_node_with_schema(node(2), harness_schema.clone());
    let (core_dir, mut core) = open_node_with_schema(node(9), harness_schema.clone());
    let (reader_a_dir, mut reader_a) = open_node_with_schema(node(3), harness_schema.clone());
    let (reader_b_dir, mut reader_b) = open_node_with_schema(node(4), harness_schema.clone());
    let mut link_a = PeerState::for_author(author_a);
    let mut link_b = PeerState::for_author(author_b);
    let owner_shape = crate::query::Query::from("todos")
        .filter(eq(col("owner"), param("owner")))
        .validate(&harness_schema)
        .unwrap();
    let owner_binding_a = owner_shape
        .bind(BTreeMap::from([(
            "owner".to_owned(),
            Value::Uuid(author_a.0),
        )]))
        .unwrap();
    let owner_binding_b = owner_shape
        .bind(BTreeMap::from([(
            "owner".to_owned(),
            Value::Uuid(author_b.0),
        )]))
        .unwrap();
    let owner_bindings = BTreeMap::from([
        (owner_binding_a.binding_id(), author_a),
        (owner_binding_b.binding_id(), author_b),
    ]);
    register_shape_binding(&mut core, &owner_shape, &owner_binding_a);
    register_shape_binding(&mut core, &owner_shape, &owner_binding_b);
    let mut delivered_a = PerNodeKnowledge::default();
    let mut delivered_b = PerNodeKnowledge::default();
    let mut writer_known_a = PerNodeKnowledge::default();
    let mut writer_known_b = PerNodeKnowledge::default();
    let mut oracle = Oracle::new();
    let mut oracle_txs = BTreeSet::new();
    let mut exclusive_txs = Vec::new();
    let rows = [row(1), row(2), row(3)];
    let mut upstream = VecDeque::new();
    let mut fate_a = VecDeque::new();
    let mut fate_b = VecDeque::new();
    let mut views_a = VecDeque::new();
    let mut views_b = VecDeque::new();
    let commit_target = m3_commit_target();
    let mut commits_started: u64 = 0;
    let mut midstream_view_emissions = 0;
    let mut reader_restarts = 0;
    let mut core_restarts = 0;
    let mut rehydrate_emissions = 0;
    let mut message_counts = M3MessageCounts::default();
    while commits_started < commit_target
        || !upstream.is_empty()
        || !fate_a.is_empty()
        || !fate_b.is_empty()
        || !views_a.is_empty()
        || !views_b.is_empty()
    {
        let action = rng.choose(12);
        match action {
            0 if commits_started < commit_target => {
                let use_writer_a = rng.chance(1, 2);
                let default_author = if use_writer_a { author_a } else { author_b };
                let (made_by, owner) =
                    seeded_author_and_owner(&mut rng, default_author, author_a, author_b);
                let commit = commit_from_rng(&mut rng, commits_started, &rows).made_by(made_by);
                let commit = if commit.deletion.is_some() {
                    commit
                } else {
                    let title = commit
                        .cells
                        .get("title")
                        .cloned()
                        .unwrap_or_else(|| v(format!("seeded-{commits_started}")));
                    let title = match title {
                        Value::String(value) => value,
                        _ => panic!("seeded title must be string"),
                    };
                    commit.cells(owner_cells_with_author(owner, title))
                };
                commits_started += 1;
                let row_uuid = commit.row_uuid;
                let parents = commit.parents.clone();
                let cells = commit.cells.clone();
                let deletion = commit.deletion;
                let (tx_id, message) = if use_writer_a {
                    writer_a.commit_mergeable_unit(commit).unwrap()
                } else {
                    writer_b.commit_mergeable_unit(commit).unwrap()
                };
                if use_writer_a {
                    writer_known_a.record_local_commit(tx_id);
                } else {
                    writer_known_b.record_local_commit(tx_id);
                }
                let made_at = if use_writer_a {
                    writer_a.transaction_record(tx_id).unwrap().tx_id.time
                } else {
                    writer_b.transaction_record(tx_id).unwrap().tx_id.time
                };
                oracle_txs.insert(tx_id);
                let mut version = ModelRowVersion::new(row_uuid, tx_id, made_at);
                version.parents = parents;
                version.cells = cells;
                version.deletion = deletion;
                oracle.add_version(version);

                upstream.push_back(message.clone());
                message_counts.upstream_enqueued += 1;
                if rng.chance(2, 5) {
                    upstream.push_back(message);
                    message_counts.upstream_enqueued += 1;
                }
            }
            9 if commits_started < commit_target => {
                let writer = if rng.chance(1, 2) {
                    &mut writer_a
                } else {
                    &mut writer_b
                };
                let default_author = if writer.node_uuid == node(1) {
                    author_a
                } else {
                    author_b
                };
                let (made_by, owner) =
                    seeded_author_and_owner(&mut rng, default_author, author_a, author_b);
                let tx_id = writer.open_exclusive().unwrap();
                writer
                    .tx_read(tx_id, "todos", rows[rng.choose(rows.len())])
                    .unwrap();
                if rng.chance(1, 2) {
                    writer.tx_current_rows(tx_id, "todos").unwrap();
                }
                if rng.chance(1, 3) {
                    let binding = if owner == author_a {
                        &owner_binding_a
                    } else {
                        &owner_binding_b
                    };
                    writer.tx_query(tx_id, &owner_shape, binding).unwrap();
                }
                let writes = 1 + rng.choose(2);
                let mut written_rows = BTreeSet::new();
                for _ in 0..writes {
                    let mut row_uuid = rows[rng.choose(rows.len())];
                    while written_rows.contains(&row_uuid) {
                        row_uuid = rows[rng.choose(rows.len())];
                    }
                    written_rows.insert(row_uuid);
                    writer
                        .tx_write(
                            tx_id,
                            "todos",
                            row_uuid,
                            owner_cells_with_author(
                                owner,
                                format!("exclusive-{commits_started}-{}", rng.next_u64() % 1_000),
                            ),
                            None,
                        )
                        .unwrap();
                }
                let (tx_id, message) = writer
                    .commit_exclusive(tx_id, made_by, 1_100 + rng.choose(12) as u64)
                    .unwrap();
                if tx_id.node == node(1) {
                    writer_known_a.record_local_commit(tx_id);
                } else {
                    writer_known_b.record_local_commit(tx_id);
                }
                commits_started += 1;
                oracle_txs.insert(tx_id);
                let SyncMessage::CommitUnit { tx, versions } = &message else {
                    panic!("expected exclusive commit unit");
                };
                exclusive_txs.push(tx.clone());
                add_commit_unit_versions_to_oracle(
                    tx,
                    versions,
                    &harness_schema.tables[0],
                    &mut oracle,
                );
                upstream.push_back(message.clone());
                message_counts.upstream_enqueued += 1;
                if rng.chance(2, 5) {
                    upstream.push_back(message);
                    message_counts.upstream_enqueued += 1;
                }
            }
            10 if commits_started < commit_target => {
                let row_uuid = rows[rng.choose(rows.len())];
                let owner = if rng.chance(1, 2) { author_a } else { author_b };
                let commit =
                    MergeableCommit::new("todos", row_uuid, now_ms + SKEW_TOLERANCE_MS + 1_000)
                        .made_by(if rng.chance(1, 2) {
                            AuthorId::SYSTEM
                        } else {
                            owner
                        })
                        .cells(owner_cells_with_author(
                            owner,
                            format!("skew-{commits_started}-{}", rng.next_u64() % 1_000),
                        ));
                commits_started += 1;
                let use_writer_a = rng.chance(1, 2);
                let (tx_id, message) = if use_writer_a {
                    writer_a.commit_mergeable_unit(commit).unwrap()
                } else {
                    writer_b.commit_mergeable_unit(commit).unwrap()
                };
                if use_writer_a {
                    writer_known_a.record_local_commit(tx_id);
                } else {
                    writer_known_b.record_local_commit(tx_id);
                }
                oracle_txs.insert(tx_id);
                let SyncMessage::CommitUnit { tx, versions } = &message else {
                    panic!("expected skew commit unit");
                };
                add_commit_unit_versions_to_oracle(
                    tx,
                    versions,
                    &harness_schema.tables[0],
                    &mut oracle,
                );
                upstream.push_back(message.clone());
                message_counts.upstream_enqueued += 1;
                if rng.chance(1, 3) {
                    upstream.push_back(message);
                    message_counts.upstream_enqueued += 1;
                }
            }
            11 if commits_started + 1 < commit_target => {
                let writer = if rng.chance(1, 2) {
                    &mut writer_a
                } else {
                    &mut writer_b
                };
                let default_author = if writer.node_uuid == node(1) {
                    author_a
                } else {
                    author_b
                };
                let (made_by, owner) =
                    seeded_author_and_owner(&mut rng, default_author, author_a, author_b);
                let tx_id = writer.open_exclusive().unwrap();
                writer
                    .tx_read(tx_id, "todos", rows[rng.choose(rows.len())])
                    .unwrap();
                writer
                    .tx_write(
                        tx_id,
                        "todos",
                        rows[rng.choose(rows.len())],
                        owner_cells_with_author(
                            owner,
                            format!("parent-{commits_started}-{}", rng.next_u64() % 1_000),
                        ),
                        None,
                    )
                    .unwrap();
                let (parent_ref, parent_message) = writer
                    .commit_exclusive(tx_id, made_by, 1_200 + rng.choose(12) as u64)
                    .unwrap();
                let child_commit = MergeableCommit::new(
                    "todos",
                    rows[rng.choose(rows.len())],
                    1_300 + rng.choose(12) as u64,
                )
                .parents(vec![parent_ref])
                .made_by(made_by)
                .cells(owner_cells_with_author(
                    owner,
                    format!("child-{commits_started}-{}", rng.next_u64() % 1_000),
                ));
                let (child_ref, child_message) =
                    writer.commit_mergeable_unit(child_commit).unwrap();
                if parent_ref.node == node(1) {
                    writer_known_a.record_local_commit(parent_ref);
                    writer_known_a.record_local_commit(child_ref);
                } else {
                    writer_known_b.record_local_commit(parent_ref);
                    writer_known_b.record_local_commit(child_ref);
                }
                commits_started += 2;
                oracle_txs.insert(parent_ref);
                oracle_txs.insert(child_ref);
                let SyncMessage::CommitUnit { tx, versions } = &parent_message else {
                    panic!("expected parent commit unit");
                };
                exclusive_txs.push(tx.clone());
                add_commit_unit_versions_to_oracle(
                    tx,
                    versions,
                    &harness_schema.tables[0],
                    &mut oracle,
                );
                let SyncMessage::CommitUnit { tx, versions } = &child_message else {
                    panic!("expected child commit unit");
                };
                add_commit_unit_versions_to_oracle(
                    tx,
                    versions,
                    &harness_schema.tables[0],
                    &mut oracle,
                );
                upstream.push_back(child_message.clone());
                message_counts.upstream_enqueued += 1;
                if rng.chance(1, 3) {
                    upstream.push_back(child_message);
                    message_counts.upstream_enqueued += 1;
                }
                upstream.push_back(parent_message.clone());
                message_counts.upstream_enqueued += 1;
                if rng.chance(1, 3) {
                    upstream.push_back(parent_message);
                    message_counts.upstream_enqueued += 1;
                }
            }
            1 if !upstream.is_empty() => {
                let message = upstream.pop_front().unwrap();
                message_counts.upstream_delivered += 1;
                let SyncMessage::CommitUnit { tx, versions } = &message else {
                    panic!("upstream should only contain commit units");
                };
                let updates = core
                    .ingest_commit_unit(tx.clone(), versions.clone(), now_ms)
                    .unwrap();
                for fate in updates {
                    let SyncMessage::FateUpdate { tx_id, .. } = &fate else {
                        panic!("core should only emit fate updates here");
                    };
                    record_fate_update_in_oracle(&mut oracle, &fate);
                    let queue = if tx_id.node == node(1) {
                        &mut fate_a
                    } else {
                        &mut fate_b
                    };
                    queue.push_back(fate.clone());
                    message_counts.fate_enqueued += 1;
                    if rng.chance(1, 3) {
                        queue.push_back(fate);
                        message_counts.fate_enqueued += 1;
                    }
                }
                add_core_versions_to_oracle(&mut core, &mut oracle, &mut oracle_txs);
            }
            2 if !fate_a.is_empty() || !fate_b.is_empty() => {
                let deliver_a = !fate_a.is_empty() && (fate_b.is_empty() || rng.chance(1, 2));
                let message = if deliver_a {
                    fate_a.pop_front().unwrap()
                } else {
                    fate_b.pop_front().unwrap()
                };
                message_counts.fate_delivered += 1;
                if deliver_a {
                    writer_known_a.record_fate_delivery(&message);
                    writer_a
                        .apply_sync_message(message.clone())
                        .unwrap_or_else(|err| {
                            panic!("seed {seed}: writer A failed to apply {message:?}: {err:?}")
                        });
                } else {
                    writer_known_b.record_fate_delivery(&message);
                    writer_b
                        .apply_sync_message(message.clone())
                        .unwrap_or_else(|err| {
                            panic!("seed {seed}: writer B failed to apply {message:?}: {err:?}")
                        });
                }
            }
            3 if midstream_view_emissions < 12 => {
                midstream_view_emissions += 1;
                views_a.push_back(link_a.current_rows_update(&mut core, "todos").unwrap());
                message_counts.view_enqueued += 1;
                if rng.chance(1, 4) {
                    let duplicate = views_a.back().unwrap().clone();
                    views_a.push_back(duplicate);
                    message_counts.view_enqueued += 1;
                }
            }
            4 if midstream_view_emissions < 12 => {
                midstream_view_emissions += 1;
                views_b.push_back(link_b.current_rows_update(&mut core, "todos").unwrap());
                message_counts.view_enqueued += 1;
                if rng.chance(1, 4) {
                    let duplicate = views_b.back().unwrap().clone();
                    views_b.push_back(duplicate);
                    message_counts.view_enqueued += 1;
                }
            }
            5 if !views_a.is_empty() || !views_b.is_empty() => {
                // The protocol guarantees FIFO on each peer; we still
                // interleave different peers randomly.
                let deliver_a = !views_a.is_empty() && (views_b.is_empty() || rng.chance(1, 2));
                let message = if deliver_a {
                    views_a.pop_front().unwrap()
                } else {
                    views_b.pop_front().unwrap()
                };
                message_counts.view_delivered += 1;
                if deliver_a {
                    delivered_a.record_view_delivery(&message);
                    reader_a.apply_sync_message(message).unwrap();
                } else {
                    delivered_b.record_view_delivery(&message);
                    reader_b.apply_sync_message(message).unwrap();
                }
            }
            6 if reader_restarts < 2 => {
                reader_restarts += 1;
                let restart_a = rng.chance(1, 2) || (views_b.is_empty() && !views_a.is_empty());
                if restart_a {
                    drop(reader_a);
                    reader_a = reopen_node_at(&reader_a_dir, node(3), harness_schema.clone());
                } else {
                    drop(reader_b);
                    reader_b = reopen_node_at(&reader_b_dir, node(4), harness_schema.clone());
                }
            }
            7 if rehydrate_emissions < 4 => {
                rehydrate_emissions += 1;
                if rng.chance(1, 2) {
                    let before = views_a.len();
                    enqueue_rehydrate_with_dedup_assertion(
                        &mut link_a,
                        &mut core,
                        42,
                        &mut views_a,
                    );
                    message_counts.view_enqueued += (views_a.len() - before) as u64;
                } else {
                    let before = views_b.len();
                    enqueue_rehydrate_with_dedup_assertion(
                        &mut link_b,
                        &mut core,
                        43,
                        &mut views_b,
                    );
                    message_counts.view_enqueued += (views_b.len() - before) as u64;
                }
            }
            8 if core_restarts < 1 && upstream.is_empty() => {
                core_restarts += 1;
                drop(core);
                core = reopen_node_at(&core_dir, node(9), harness_schema.clone());
                register_shape_binding(&mut core, &owner_shape, &owner_binding_a);
                register_shape_binding(&mut core, &owner_shape, &owner_binding_b);
            }
            _ => {}
        }
    }

    let before = views_a.len();
    enqueue_rehydrate_with_dedup_assertion(&mut link_a, &mut core, 42, &mut views_a);
    message_counts.view_enqueued += (views_a.len() - before) as u64;
    let before = views_b.len();
    enqueue_rehydrate_with_dedup_assertion(&mut link_b, &mut core, 43, &mut views_b);
    message_counts.view_enqueued += (views_b.len() - before) as u64;
    while !views_a.is_empty() || !views_b.is_empty() {
        let deliver_a = !views_a.is_empty() && (views_b.is_empty() || rng.chance(1, 2));
        let message = if deliver_a {
            views_a.pop_front().unwrap()
        } else {
            views_b.pop_front().unwrap()
        };
        message_counts.view_delivered += 1;
        if deliver_a {
            delivered_a.record_view_delivery(&message);
            reader_a.apply_sync_message(message).unwrap();
        } else {
            delivered_b.record_view_delivery(&message);
            reader_b.apply_sync_message(message).unwrap();
        }
    }

    assert_global_current_rows_match_oracle(&mut core, &oracle);
    assert_global_rows_match_known_oracle(&mut writer_a, &oracle, &writer_known_a);
    assert_global_rows_match_known_oracle(&mut writer_b, &oracle, &writer_known_b);
    assert_settled_result_sets_unique(&reader_a, 42, seed);
    assert_settled_result_sets_unique(&reader_b, 43, seed);
    assert_subscription_rows_match_policy_oracle(
        &mut reader_a,
        42,
        &oracle,
        &delivered_a,
        author_a,
    );
    assert_subscription_rows_match_policy_oracle(
        &mut reader_b,
        43,
        &oracle,
        &delivered_b,
        author_b,
    );
    for node in [&mut writer_a, &mut writer_b, &mut core] {
        for table in node.catalogue.schema.tables
            .iter()
            .map(|table| table.name.clone())
            .collect::<Vec<_>>()
        {
            assert_currency_tables_match_storage(node, &table);
        }
    }
    for (name, node) in [
        ("writer_a", &mut writer_a),
        ("writer_b", &mut writer_b),
        ("core", &mut core),
        ("reader_a", &mut reader_a),
        ("reader_b", &mut reader_b),
    ] {
        assert!(
            node.parking.parked_commit_units.is_empty(),
            "seed {seed}: {name} parked commit units should drain before index checks"
        );
        assert!(
            node.rejections.child_txs_by_parent.is_empty(),
            "seed {seed}: {name} pending cascade edges should be pruned at quiescence"
        );
    }
    assert_exclusive_serialization_matches_oracle(
        seed,
        &oracle,
        &exclusive_txs,
        owner_shape.shape_id(),
        &owner_bindings,
    );
    for tx_id in &oracle_txs {
        assert!(
            oracle.tx_state(*tx_id).is_some(),
            "seed {seed}: transaction {tx_id:?} has no fate at quiescence"
        );
    }
    assert_eq!(
        core.sync_metrics().parked_orphans,
        core.sync_metrics().parked_orphans_resolved,
        "seed {seed}"
    );
    assert!(
        core.parking.parked_commit_units.is_empty(),
        "seed {seed}: parked commit units should drain"
    );
    assert_eq!(reader_a.sync_metrics().parked_orphans, 0, "seed {seed}");
    assert_eq!(reader_b.sync_metrics().parked_orphans, 0, "seed {seed}");
    M3RunSummary {
        writer_a: node_summary(&mut writer_a, &oracle_txs),
        writer_b: node_summary(&mut writer_b, &oracle_txs),
        core: node_summary(&mut core, &oracle_txs),
        reader_a: node_summary(&mut reader_a, &oracle_txs),
        reader_b: node_summary(&mut reader_b, &oracle_txs),
        link_a_metrics: link_a.metrics.clone(),
        link_b_metrics: link_b.metrics.clone(),
        message_counts,
    }
}
