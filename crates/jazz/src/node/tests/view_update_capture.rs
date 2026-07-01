#[derive(Clone, Debug, PartialEq, Eq)]
struct CanonicalViewUpdate {
    subscription: String,
    reset_result_set: bool,
    version_bundles: Vec<CanonicalVersionBundle>,
    peer_payload_inventory: Vec<TxId>,
    result_member_adds: Vec<ResultRowEntry>,
    result_member_removes: Vec<ResultRowEntry>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct CanonicalVersionBundle {
    tx: String,
    fate: String,
    global_seq: Option<GlobalSeq>,
    durability: DurabilityTier,
    versions: Vec<CanonicalVersionRecord>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct CanonicalVersionRecord {
    table: String,
    schema_version: SchemaVersionId,
    row_uuid: RowUuid,
    parents: Vec<TxId>,
    created_by: AuthorId,
    created_at: TxTime,
    updated_by: AuthorId,
    updated_at: TxTime,
    deletion: Option<DeletionEvent>,
    cells: Vec<String>,
}

fn canonical_version_bundle(bundle: VersionBundle) -> CanonicalVersionBundle {
    let mut versions = bundle
        .versions
        .into_iter()
        .map(canonical_version_record)
        .collect::<Vec<_>>();
    versions.sort();
    CanonicalVersionBundle {
        tx: format!("{:?}", bundle.tx),
        fate: format!("{:?}", bundle.fate),
        global_seq: bundle.global_seq,
        durability: bundle.durability,
        versions,
    }
}

fn canonical_version_record(record: VersionRecord) -> CanonicalVersionRecord {
    let mut parents = record.parents();
    parents.sort();
    let cells = (0..record.record().descriptor().fields().len())
        .filter_map(|idx| record.optional_cell_at(idx))
        .map(|value| format!("{value:?}"))
        .collect();
    CanonicalVersionRecord {
        table: record.table().to_owned(),
        schema_version: record.schema_version(),
        row_uuid: record.row_uuid(),
        parents,
        created_by: record.created_by(),
        created_at: record.created_at(),
        updated_by: record.updated_by(),
        updated_at: record.updated_at(),
        deletion: record.deletion(),
        cells,
    }
}

fn capture_view_update(update: SyncMessage) -> CanonicalViewUpdate {
    let SyncMessage::ViewUpdate {
        subscription,
        reset_result_set,
        version_bundles,
        peer_payload_inventory: crate::protocol::PeerPayloadInventory {
            complete_tx_payloads: complete_tx_payload_refs,
        },
        result_member_adds,
        result_member_removes,
        ..
    } = update
    else {
        panic!("expected view update");
    };

    let mut version_bundles = version_bundles
        .into_iter()
        .map(canonical_version_bundle)
        .collect::<Vec<_>>();
    version_bundles.sort();
    let mut complete_tx_payload_refs = complete_tx_payload_refs;
    complete_tx_payload_refs.sort();
    let mut result_member_adds = result_member_adds
        .into_iter()
        .filter_map(crate::protocol::ResultMemberEntry::into_row)
        .collect::<Vec<_>>();
    result_member_adds.sort();
    let mut result_member_removes = result_member_removes
        .into_iter()
        .filter_map(crate::protocol::ResultMemberEntry::into_row)
        .collect::<Vec<_>>();
    result_member_removes.sort();

    CanonicalViewUpdate {
        subscription: format!("{subscription:?}"),
        reset_result_set,
        version_bundles,
        peer_payload_inventory: complete_tx_payload_refs,
        result_member_adds,
        result_member_removes,
    }
}

fn assert_real_peer_tick(
    mut capture: CanonicalViewUpdate,
    expected_adds: &[ResultRowEntry],
    expected_removes: &[ResultRowEntry],
    expected_reset_result_set: bool,
    case: (AuthorId, u64, &str),
) {
    let (identity, seed, tick) = case;
    capture.result_member_adds.sort();
    capture.result_member_removes.sort();
    let mut expected_adds = expected_adds.to_vec();
    expected_adds.sort();
    let mut expected_removes = expected_removes.to_vec();
    expected_removes.sort();
    assert_eq!(
        capture.reset_result_set, expected_reset_result_set,
        "real peer maintained subscription view emitted unexpected reset_result_set for seed {seed:#x}, identity {identity:?}, tick {tick}"
    );
    assert_eq!(
        capture.result_member_adds, expected_adds,
        "real peer maintained subscription view emitted unexpected adds for seed {seed:#x}, identity {identity:?}, tick {tick}"
    );
    assert_eq!(
        capture.result_member_removes, expected_removes,
        "real peer maintained subscription view emitted unexpected removes for seed {seed:#x}, identity {identity:?}, tick {tick}"
    );
}

fn result_row(table: &str, row_uuid: RowUuid, tx_id: TxId) -> ResultRowEntry {
    (groove::Intern::new(table.to_owned()), row_uuid, tx_id)
}

fn result_row_from(
    txs: &BTreeMap<(&'static str, RowUuid), TxId>,
    table: &'static str,
    row_uuid: RowUuid,
) -> ResultRowEntry {
    result_row(table, row_uuid, txs[&(table, row_uuid)])
}

fn assert_maintained_subscription_view_tick(
    update: SyncMessage,
    expected_adds: &[ResultRowEntry],
    expected_removes: &[ResultRowEntry],
    expected_reset_result_set: bool,
    case: (AuthorId, u64, &str),
) -> SyncMessage {
    let (identity, seed, tick) = case;
    let capture = capture_view_update(update.clone());
    assert_real_peer_tick(
        capture,
        expected_adds,
        expected_removes,
        expected_reset_result_set,
        (identity, seed, tick),
    );
    update
}

fn maintained_view_capture_schema() -> JazzSchema {
    JazzSchema::new([TableSchema::new(
        "todos",
        [
            ColumnSchema::new("title", ColumnType::String),
            ColumnSchema::new("owner", ColumnType::Uuid),
        ],
    )
    .with_read_policy(Policy::owner_only("todos", "owner"))])
}

fn accept_owner_capture_row(
    core: &mut NodeState<RocksDbStorage>,
    parents: &mut BTreeMap<RowUuid, TxId>,
    row_uuid: RowUuid,
    owner: AuthorId,
    title: impl Into<String>,
    made_at: u64,
) -> TxId {
    let mut commit =
        MergeableCommit::new("todos", row_uuid, made_at).cells(owner_cells(owner, title));
    if let Some(parent) = parents.get(&row_uuid).copied() {
        commit = commit.parents(vec![parent]);
    }
    let tx_id = accept_global(core, commit);
    parents.insert(row_uuid, tx_id);
    tx_id
}

fn accept_capture_delete(
    core: &mut NodeState<RocksDbStorage>,
    parents: &mut BTreeMap<RowUuid, TxId>,
    row_uuid: RowUuid,
    made_at: u64,
) {
    let parent = parents[&row_uuid];
    let tx_id = accept_global(
        core,
        MergeableCommit::new("todos", row_uuid, made_at)
            .parents(vec![parent])
            .deletion(DeletionEvent::Deleted),
    );
    parents.insert(row_uuid, tx_id);
}

fn apply_capture_result_delta(
    result_set: &mut BTreeSet<ResultRowEntry>,
    update: &SyncMessage,
) {
    let SyncMessage::ViewUpdate {
        reset_result_set,
        result_member_adds,
        result_member_removes,
        ..
    } = update
    else {
        panic!("expected view update");
    };
    if *reset_result_set {
        result_set.clear();
    }
    for entry in result_member_removes
        .iter()
        .filter_map(crate::protocol::ResultMemberEntry::as_row)
    {
        result_set.remove(&entry);
    }
    result_set.extend(
        result_member_adds
            .iter()
            .filter_map(crate::protocol::ResultMemberEntry::as_row),
    );
}

fn apply_capture_delivery_state(
    result_set: &mut BTreeSet<ResultRowEntry>,
    peer_complete_tx_payloads: &mut BTreeSet<TxId>,
    update: &SyncMessage,
) {
    apply_capture_result_delta(result_set, update);
    let SyncMessage::ViewUpdate {
        version_bundles,
        peer_payload_inventory: crate::protocol::PeerPayloadInventory { complete_tx_payloads: complete_tx_payload_refs },
        ..
    } = update
    else {
        panic!("expected view update");
    };
    peer_complete_tx_payloads.extend(
        version_bundles
            .iter()
            .filter(|bundle| bundle.versions.len() == bundle.tx.n_total_writes as usize)
            .map(|bundle| bundle.tx.tx_id),
    );
    peer_complete_tx_payloads.extend(complete_tx_payload_refs.iter().copied());
}

fn view_update_full_bundle_count(update: &SyncMessage) -> usize {
    let SyncMessage::ViewUpdate { version_bundles, .. } = update else {
        panic!("expected view update");
    };
    version_bundles.len()
}

struct MaintainedSubscriptionViewSubscription {
    subscription: groove::ivm::MultisinkSubscription,
    maintained: crate::node::maintained_subscription_view::MaintainedSubscriptionView,
    terminal_schemas: crate::node::maintained_subscription_view::MaintainedTerminalSchemas,
    tables: BTreeMap<String, TableSchema>,
    previous_result_set: BTreeSet<ResultRowEntry>,
    peer_complete_tx_payloads: BTreeSet<TxId>,
}

impl MaintainedSubscriptionViewSubscription {
    fn new(
        core: &mut NodeState<RocksDbStorage>,
        shape: &ValidatedQuery,
        binding: &Binding,
        subscription_key: SubscriptionKey,
        identity: AuthorId,
    ) -> (Self, SyncMessage) {
        let (subscription, maintained, terminal_schemas, transitions, tables) = core
            .open_seeded_maintained_subscription_view(
                shape,
                binding,
                identity,
                DurabilityTier::Global,
                &Default::default(),
            )
            .unwrap();
        assert!(
            transitions.removes.is_empty(),
            "cold maintained snapshot emitted result removes"
        );
        let mut driver = Self {
            subscription,
            maintained,
            terminal_schemas,
            tables,
            previous_result_set: BTreeSet::new(),
            peer_complete_tx_payloads: BTreeSet::new(),
        };
        let output_tables = driver.tables.clone();
        let result_member_adds = transitions
            .adds
            .into_iter()
            .filter(|member| {
                member
                    .table_name()
                    .is_some_and(|table| output_tables.contains_key(table))
            })
            .filter_map(|member| member.as_row())
            .collect();
        let update = driver
            .view_update(
                core,
                shape,
                subscription_key,
                result_member_adds,
                Vec::new(),
                false,
                identity,
            )
            .unwrap();
        apply_capture_delivery_state(
            &mut driver.previous_result_set,
            &mut driver.peer_complete_tx_payloads,
            &update,
        );
        (driver, update)
    }

    fn update(
        &mut self,
        core: &mut NodeState<RocksDbStorage>,
        shape: &ValidatedQuery,
        subscription_key: SubscriptionKey,
        identity: AuthorId,
    ) -> SyncMessage {
        core.flush_query_runtime().unwrap();
        let output_tables = self.tables.clone();
        let mut states = BTreeMap::<ResultRowEntry, (bool, bool)>::new();
        loop {
            match self.subscription.try_recv() {
                Ok(deltas) => {
                    let transitions = self.maintained.apply_multisink_deltas(
                        deltas,
                        &self.terminal_schemas,
                        &self.tables,
                        &core.node_aliases,
                    )
                    .unwrap();
                    for member in transitions.adds {
                        let Some(entry) = member.as_row() else {
                            continue;
                        };
                        let before = self.previous_result_set.contains(&entry);
                        states
                            .entry(entry)
                            .and_modify(|(_, after)| *after = true)
                            .or_insert((before, true));
                    }
                    for member in transitions.removes {
                        let Some(entry) = member.as_row() else {
                            continue;
                        };
                        let before = self.previous_result_set.contains(&entry);
                        states
                            .entry(entry)
                            .and_modify(|(_, after)| *after = false)
                            .or_insert((before, false));
                    }
                }
                Err(std::sync::mpsc::TryRecvError::Empty) => break,
                Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                    panic!("maintained subscription view subscription disconnected")
                }
            }
        }
        let mut result_member_adds = Vec::new();
        let mut result_member_removes = Vec::new();
        for (entry, (before, after)) in states {
            if !output_tables.contains_key(entry.0.as_str()) {
                continue;
            }
            match (before, after) {
                (false, true) => result_member_adds.push(entry),
                (true, false) => result_member_removes.push(entry),
                _ => {}
            }
        }
        let update = self
            .view_update(
                core,
                shape,
                subscription_key,
                result_member_adds,
                result_member_removes,
                false,
                identity,
            )
            .unwrap();
        apply_capture_delivery_state(
            &mut self.previous_result_set,
            &mut self.peer_complete_tx_payloads,
            &update,
        );
        update
    }

    fn view_update(
        &self,
        core: &mut NodeState<RocksDbStorage>,
        _shape: &ValidatedQuery,
        subscription_key: SubscriptionKey,
        result_member_adds: Vec<ResultRowEntry>,
        result_member_removes: Vec<ResultRowEntry>,
        reset_result_set: bool,
        identity: AuthorId,
    ) -> Result<SyncMessage, Error> {
        let previous_result_set = self
            .previous_result_set
            .iter()
            .map(|(_, _, tx_id)| *tx_id)
            .collect::<BTreeSet<_>>();
        let mut update = core.view_update_for_maintained_result_members(
            crate::node::MaintainedViewBundleInputs {
                subscription: subscription_key,
                peer_complete_tx_payloads: self.peer_complete_tx_payloads.clone(),
                complete_exclusive_payloads: false,
                previous_result_set,
                result_member_adds: result_member_adds
                    .into_iter()
                    .map(crate::protocol::ResultMemberEntry::from)
                    .collect(),
                result_member_removes: result_member_removes
                    .into_iter()
                    .map(crate::protocol::ResultMemberEntry::from)
                    .collect(),
                identity,
                tier: DurabilityTier::Global,
                maintained_facts: &self.maintained,
            },
        )?;
        let SyncMessage::ViewUpdate {
            reset_result_set: update_reset,
            ..
        } = &mut update
        else {
            panic!("expected view update");
        };
        *update_reset = reset_result_set;
        Ok(update)
    }
}

fn assert_shipped_content_rows(
    update: &SyncMessage,
    tx_id: TxId,
    expected_rows: &[RowUuid],
    absent_rows: &[RowUuid],
) {
    let SyncMessage::ViewUpdate { version_bundles, .. } = update else {
        panic!("expected view update");
    };
    let bundle = version_bundles
        .iter()
        .find(|bundle| bundle.tx.tx_id == tx_id)
        .unwrap_or_else(|| panic!("expected shipped bundle for tx {tx_id:?}"));
    let rows = bundle
        .versions
        .iter()
        .filter(|version| version.deletion().is_none())
        .map(VersionRecord::row_uuid)
        .collect::<BTreeSet<_>>();
    for expected in expected_rows {
        assert!(
            rows.contains(expected),
            "bundle for tx {tx_id:?} did not include expected row {expected:?}; rows={rows:?}"
        );
    }
    for absent in absent_rows {
        assert!(
            !rows.contains(absent),
            "bundle for tx {tx_id:?} included forbidden row {absent:?}; rows={rows:?}"
        );
    }
}

fn assert_retraction_without_replacement_leak(
    update: &SyncMessage,
    row_uuid: RowUuid,
    old_tx_id: TxId,
    unreadable_tx_id: TxId,
) {
    let SyncMessage::ViewUpdate {
        version_bundles,
        peer_payload_inventory: crate::protocol::PeerPayloadInventory { complete_tx_payloads: complete_tx_payload_refs },
        result_member_adds,
        result_member_removes,
        ..
    } = update
    else {
        panic!("expected view update");
    };
    assert!(
        result_member_removes
            .iter()
            .filter_map(crate::protocol::ResultMemberEntry::as_row)
            .any(|(_, row, tx_id)| row == row_uuid && tx_id == old_tx_id),
        "revocation update did not retract row {row_uuid:?} at tx {old_tx_id:?}"
    );
    assert!(
        !result_member_adds
            .iter()
            .filter_map(crate::protocol::ResultMemberEntry::as_row)
            .any(|(_, row, tx_id)| row == row_uuid && tx_id == unreadable_tx_id),
        "revocation update re-added unreadable row {row_uuid:?} at tx {unreadable_tx_id:?}"
    );
    assert!(
        !complete_tx_payload_refs.contains(&unreadable_tx_id),
        "revocation update leaked unreadable tx {unreadable_tx_id:?} as a complete tx ref"
    );
    assert!(
        !version_bundles
            .iter()
            .any(|bundle| bundle.tx.tx_id == unreadable_tx_id),
        "revocation update leaked unreadable tx {unreadable_tx_id:?} as a version bundle"
    );
}

fn seeded_maintained_subscription_view_subscription_capture(seed: u64, identity: AuthorId) {
    let schema = maintained_view_capture_schema();
    let (_core_dir, mut core) = open_node_with_schema(node(0x82), schema.clone());
    let alice = user(0xa1);
    let bob = user(0xb2);
    let base = (seed as u8).wrapping_mul(8);
    let initial_alice = row(base.wrapping_add(1));
    let initial_bob = row(base.wrapping_add(2));
    let predicate_remove = row(base.wrapping_add(3));
    let deleted = row(base.wrapping_add(4));
    let never_match = row(base.wrapping_add(5));
    let added = row(base.wrapping_add(6));
    let hidden_added = row(base.wrapping_add(7));
    let sibling_match = row(base.wrapping_add(8));
    let sibling_nonmatch = row(base.wrapping_add(9));
    let sibling_hidden = row(base.wrapping_add(10));
    let multi_match_a = row(base.wrapping_add(11));
    let multi_match_b = row(base.wrapping_add(12));
    let rls_revoked = row(base.wrapping_add(13));
    let mut parents = BTreeMap::<RowUuid, TxId>::new();
    let mut txs = BTreeMap::<RowUuid, TxId>::new();

    for (row_uuid, owner, title, made_at) in [
        (initial_alice, alice, "match", 1_000),
        (initial_bob, bob, "match", 1_001),
        (predicate_remove, alice, "match", 1_002),
        (deleted, alice, "match", 1_003),
        (never_match, alice, "other", 1_004),
        (rls_revoked, alice, "match", 1_005),
    ] {
        let tx_id = accept_owner_capture_row(&mut core, &mut parents, row_uuid, owner, title, made_at);
        txs.insert(row_uuid, tx_id);
    }
    let rls_revoked_initial_tx = txs[&rls_revoked];

    let shape = Query::from("todos")
        .filter(eq(col("title"), lit("match")))
        .validate(&schema)
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let subscription = SubscriptionKey {
        shape_id: shape.shape_id(),
        binding_id: binding.binding_id(),
    read_view: Default::default(),
};

    let (mut maintained, maintained_initial) = MaintainedSubscriptionViewSubscription::new(
        &mut core,
        &shape,
        &binding,
        subscription,
        identity,
    );
    let expected_initial = if identity == AuthorId::SYSTEM {
        vec![
            result_row("todos", initial_alice, txs[&initial_alice]),
            result_row("todos", initial_bob, txs[&initial_bob]),
            result_row("todos", predicate_remove, txs[&predicate_remove]),
            result_row("todos", deleted, txs[&deleted]),
            result_row("todos", rls_revoked, txs[&rls_revoked]),
        ]
    } else {
        vec![
            result_row("todos", initial_alice, txs[&initial_alice]),
            result_row("todos", predicate_remove, txs[&predicate_remove]),
            result_row("todos", deleted, txs[&deleted]),
            result_row("todos", rls_revoked, txs[&rls_revoked]),
        ]
    };
    assert_maintained_subscription_view_tick(
        maintained_initial,
        &expected_initial,
        &[],
        false,
        (identity, seed, "initial"),
    );

    for (row_uuid, owner, title, made_at) in [
        (added, alice, "match", 2_000),
        (hidden_added, bob, "match", 2_001),
    ] {
        let tx_id = accept_owner_capture_row(&mut core, &mut parents, row_uuid, owner, title, made_at);
        txs.insert(row_uuid, tx_id);
    }
    let add_rows = if identity == AuthorId::SYSTEM {
        vec![
            result_row("todos", added, txs[&added]),
            result_row("todos", hidden_added, txs[&hidden_added]),
        ]
    } else {
        vec![result_row("todos", added, txs[&added])]
    };
    let update = maintained.update(&mut core, &shape, subscription, identity);
    let _ = assert_maintained_subscription_view_tick(
        update,
        &add_rows,
        &[],
        false,
        (identity, seed, "add"),
    );

    let sibling_tx = core
        .commit_mergeable_many(vec![
            MergeableCommit::new("todos", sibling_match, 2_100)
                .cells(owner_cells(alice, "match")),
            MergeableCommit::new("todos", sibling_nonmatch, 2_100)
                .cells(owner_cells(alice, "other")),
            MergeableCommit::new("todos", sibling_hidden, 2_100)
                .cells(owner_cells(bob, "match")),
        ])
        .unwrap();
    core.apply_fate_update(
        sibling_tx,
        Fate::Accepted,
        Some(GlobalSeq(100)),
        Some(DurabilityTier::Global),
    )
    .unwrap();
    txs.insert(sibling_match, sibling_tx);
    txs.insert(sibling_nonmatch, sibling_tx);
    txs.insert(sibling_hidden, sibling_tx);
    let sibling_add_rows = if identity == AuthorId::SYSTEM {
        vec![
            result_row("todos", sibling_match, txs[&sibling_match]),
            result_row("todos", sibling_hidden, txs[&sibling_hidden]),
        ]
    } else {
        vec![result_row("todos", sibling_match, txs[&sibling_match])]
    };
    let update = maintained.update(&mut core, &shape, subscription, identity);
    let sibling_update = assert_maintained_subscription_view_tick(
        update,
        &sibling_add_rows,
        &[],
        false,
        (identity, seed, "sibling-add"),
    );
    if identity == alice {
        assert_shipped_content_rows(
            &sibling_update,
            sibling_tx,
            &[sibling_match],
            &[sibling_nonmatch, sibling_hidden],
        );
    }

    let multi_tx = core
        .commit_mergeable_many(vec![
            MergeableCommit::new("todos", multi_match_a, 2_200)
                .cells(owner_cells(alice, "match")),
            MergeableCommit::new("todos", multi_match_b, 2_200)
                .cells(owner_cells(alice, "match")),
        ])
        .unwrap();
    core.apply_fate_update(
        multi_tx,
        Fate::Accepted,
        Some(GlobalSeq(101)),
        Some(DurabilityTier::Global),
    )
    .unwrap();
    txs.insert(multi_match_a, multi_tx);
    txs.insert(multi_match_b, multi_tx);
    let update = maintained.update(&mut core, &shape, subscription, identity);
    let multi_update = assert_maintained_subscription_view_tick(
        update,
        &[
            result_row("todos", multi_match_a, txs[&multi_match_a]),
            result_row("todos", multi_match_b, txs[&multi_match_b]),
        ],
        &[],
        false,
        (identity, seed, "multi-add"),
    );
    assert_shipped_content_rows(
        &multi_update,
        multi_tx,
        &[multi_match_a, multi_match_b],
        &[],
    );

    let rls_revoked_unreadable_tx =
        accept_owner_capture_row(&mut core, &mut parents, rls_revoked, bob, "match", 2_300);
    let rls_adds = if identity == AuthorId::SYSTEM {
        vec![result_row("todos", rls_revoked, rls_revoked_unreadable_tx)]
    } else {
        vec![]
    };
    let update = maintained.update(&mut core, &shape, subscription, identity);
    let revocation_update = assert_maintained_subscription_view_tick(
        update,
        &rls_adds,
        &[result_row("todos", rls_revoked, rls_revoked_initial_tx)],
        false,
        (identity, seed, "rls-revocation"),
    );
    txs.insert(rls_revoked, rls_revoked_unreadable_tx);
    if identity == alice {
        assert_retraction_without_replacement_leak(
            &revocation_update,
            rls_revoked,
            rls_revoked_initial_tx,
            rls_revoked_unreadable_tx,
        );
    }

    let previous_predicate_remove_tx = txs[&predicate_remove];
    let predicate_remove_tx = accept_owner_capture_row(
        &mut core,
        &mut parents,
        predicate_remove,
        alice,
        "other",
        3_000,
    );
    let update = maintained.update(&mut core, &shape, subscription, identity);
    let _ = assert_maintained_subscription_view_tick(
        update,
        &[],
        &[result_row(
            "todos",
            predicate_remove,
            previous_predicate_remove_tx,
        )],
        false,
        (identity, seed, "predicate-remove"),
    );
    txs.insert(predicate_remove, predicate_remove_tx);

    let previous_deleted_tx = txs[&deleted];
    accept_capture_delete(&mut core, &mut parents, deleted, 4_000);
    let update = maintained.update(&mut core, &shape, subscription, identity);
    let _ = assert_maintained_subscription_view_tick(
        update,
        &[],
        &[result_row("todos", deleted, previous_deleted_tx)],
        false,
        (identity, seed, "delete"),
    );

    let deleted_tx = accept_owner_capture_row(&mut core, &mut parents, deleted, alice, "match", 5_000);
    txs.insert(deleted, deleted_tx);
    let update = maintained.update(&mut core, &shape, subscription, identity);
    let _ = assert_maintained_subscription_view_tick(
        update,
        &[],
        &[],
        false,
        (identity, seed, "restore"),
    );
}

fn maintained_view_multitable_capture_schema() -> JazzSchema {
    JazzSchema::new([
        TableSchema::new(
            "roots",
            [
                ColumnSchema::new("title", ColumnType::String),
                ColumnSchema::new("owner", ColumnType::Uuid),
                ColumnSchema::new("target", ColumnType::Uuid),
            ],
        )
        .with_reference("target", "targets")
        .with_read_policy(Policy::owner_only("roots", "owner")),
        TableSchema::new(
            "targets",
            [
                ColumnSchema::new("title", ColumnType::String),
                ColumnSchema::new("owner", ColumnType::Uuid),
            ],
        )
        .with_read_policy(Policy::owner_only("targets", "owner")),
        TableSchema::new(
            "members",
            [
                ColumnSchema::new("title", ColumnType::String),
                ColumnSchema::new("owner", ColumnType::Uuid),
                ColumnSchema::new("root", ColumnType::Uuid),
            ],
        )
        .with_reference("root", "roots")
        .with_read_policy(Policy::owner_only("members", "owner")),
    ])
}

fn root_cells(owner: AuthorId, title: &str, target: RowUuid) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("title".to_owned(), Value::String(title.to_owned())),
        ("owner".to_owned(), Value::Uuid(owner.0)),
        ("target".to_owned(), Value::Uuid(target.0)),
    ])
}

fn member_cells(owner: AuthorId, title: &str, root: RowUuid) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("title".to_owned(), Value::String(title.to_owned())),
        ("owner".to_owned(), Value::Uuid(owner.0)),
        ("root".to_owned(), Value::Uuid(root.0)),
    ])
}

fn recursive_rls_capture_schema() -> JazzSchema {
    let recursive_policy = Query::from("docs").reachable_via(
        "doc_access",
        "doc",
        "team",
        claim("sub"),
        "team_edges",
        "member",
        "parent",
        [],
    );
    JazzSchema::new([
        TableSchema::new(
            "docs",
            [
                ColumnSchema::new("title", ColumnType::String),
                ColumnSchema::new("kind", ColumnType::String),
            ],
        )
        .with_read_policy(recursive_policy),
        TableSchema::new(
            "teams",
            [ColumnSchema::new("name", ColumnType::String)],
        ),
        TableSchema::new(
            "doc_access",
            [
                ColumnSchema::new("doc", ColumnType::Uuid),
                ColumnSchema::new("team", ColumnType::Uuid),
            ],
        )
        .with_reference("doc", "docs")
        .with_reference("team", "teams"),
        TableSchema::new(
            "team_edges",
            [
                ColumnSchema::new("member", ColumnType::Uuid),
                ColumnSchema::new("parent", ColumnType::Uuid),
            ],
        )
        .with_reference("member", "teams")
        .with_reference("parent", "teams"),
    ])
}

fn doc_cells(title: &str, kind: &str) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("title".to_owned(), Value::String(title.to_owned())),
        ("kind".to_owned(), Value::String(kind.to_owned())),
    ])
}

fn doc_access_cells(doc: RowUuid, team: AuthorId) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("doc".to_owned(), Value::Uuid(doc.0)),
        ("team".to_owned(), Value::Uuid(team.0)),
    ])
}

fn team_edge_cells(member: AuthorId, parent: AuthorId) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("member".to_owned(), Value::Uuid(member.0)),
        ("parent".to_owned(), Value::Uuid(parent.0)),
    ])
}

fn team_cells(name: &str) -> BTreeMap<String, Value> {
    BTreeMap::from([("name".to_owned(), Value::String(name.to_owned()))])
}

fn accept_recursive_row(
    core: &mut NodeState<RocksDbStorage>,
    parents: &mut BTreeMap<(&'static str, RowUuid), TxId>,
    table: &'static str,
    row_uuid: RowUuid,
    cells: BTreeMap<String, Value>,
    made_at: u64,
) -> TxId {
    let mut commit = MergeableCommit::new(table, row_uuid, made_at).cells(cells);
    if let Some(parent) = parents.get(&(table, row_uuid)).copied() {
        commit = commit.parents(vec![parent]);
    }
    let tx_id = accept_global(core, commit);
    parents.insert((table, row_uuid), tx_id);
    tx_id
}

fn delete_recursive_row(
    core: &mut NodeState<RocksDbStorage>,
    parents: &mut BTreeMap<(&'static str, RowUuid), TxId>,
    table: &'static str,
    row_uuid: RowUuid,
    made_at: u64,
) -> TxId {
    let parent = parents[&(table, row_uuid)];
    let tx_id = accept_global(
        core,
        MergeableCommit::new(table, row_uuid, made_at)
            .parents(vec![parent])
            .deletion(DeletionEvent::Deleted),
    );
    parents.insert((table, row_uuid), tx_id);
    tx_id
}

fn seeded_maintained_subscription_view_recursive_rls_capture(seed: u64, identity: AuthorId) {
    let schema = recursive_rls_capture_schema();
    let (_core_dir, mut core) = open_node_with_schema(node(0x82), schema.clone());
    let alice = user(0xa1);
    let parent_team = user(0xc3);
    let other_team = user(0xd4);
    let base = (seed as u8).wrapping_mul(16);
    let direct_doc = row(base.wrapping_add(1));
    let closure_doc = row(base.wrapping_add(2));
    let hidden_doc = row(base.wrapping_add(3));
    let added_doc = row(base.wrapping_add(4));
    let direct_access = row(base.wrapping_add(5));
    let closure_access = row(base.wrapping_add(6));
    let hidden_access = row(base.wrapping_add(7));
    let added_access = row(base.wrapping_add(8));
    let edge = row(base.wrapping_add(9));
    let mut parents = BTreeMap::<(&'static str, RowUuid), TxId>::new();

    accept_recursive_row(
        &mut core,
        &mut parents,
        "teams",
        RowUuid(alice.0),
        team_cells("alice"),
        900,
    );
    accept_recursive_row(
        &mut core,
        &mut parents,
        "teams",
        RowUuid(parent_team.0),
        team_cells("parent"),
        901,
    );
    accept_recursive_row(
        &mut core,
        &mut parents,
        "teams",
        RowUuid(other_team.0),
        team_cells("other"),
        902,
    );
    let direct_doc_tx = accept_recursive_row(
        &mut core,
        &mut parents,
        "docs",
        direct_doc,
        doc_cells("direct", "match"),
        1_000,
    );
    let closure_doc_tx = accept_recursive_row(
        &mut core,
        &mut parents,
        "docs",
        closure_doc,
        doc_cells("closure", "match"),
        1_001,
    );
    accept_recursive_row(
        &mut core,
        &mut parents,
        "docs",
        hidden_doc,
        doc_cells("hidden", "match"),
        1_002,
    );
    accept_recursive_row(
        &mut core,
        &mut parents,
        "doc_access",
        direct_access,
        doc_access_cells(direct_doc, alice),
        1_010,
    );
    accept_recursive_row(
        &mut core,
        &mut parents,
        "doc_access",
        closure_access,
        doc_access_cells(closure_doc, parent_team),
        1_011,
    );
    accept_recursive_row(
        &mut core,
        &mut parents,
        "doc_access",
        hidden_access,
        doc_access_cells(hidden_doc, other_team),
        1_012,
    );

    let shape = Query::from("docs")
        .filter(eq(col("kind"), lit("match")))
        .reachable_via(
            "doc_access",
            "doc",
            "team",
            param("team"),
            "team_edges",
            "member",
            "parent",
            [],
        )
        .validate(&schema)
        .unwrap();
    let binding = shape
        .bind(BTreeMap::from([("team".to_owned(), Value::Uuid(alice.0))]))
        .unwrap();
    let subscription = SubscriptionKey {
        shape_id: shape.shape_id(),
        binding_id: binding.binding_id(),
    read_view: Default::default(),
};
    let (mut maintained, maintained_initial) = MaintainedSubscriptionViewSubscription::new(
        &mut core,
        &shape,
        &binding,
        subscription,
        identity,
    );
    assert_maintained_subscription_view_tick(
        maintained_initial,
        &[result_row("docs", direct_doc, direct_doc_tx)],
        &[],
        false,
        (identity, seed, "recursive-initial"),
    );

    let added_doc_tx = accept_recursive_row(
        &mut core,
        &mut parents,
        "docs",
        added_doc,
        doc_cells("added", "match"),
        2_000,
    );
    accept_recursive_row(
        &mut core,
        &mut parents,
        "doc_access",
        added_access,
        doc_access_cells(added_doc, alice),
        2_001,
    );
    let update = maintained.update(
        &mut core,
        &shape,
        subscription,
        identity,
    );
    let _ = assert_maintained_subscription_view_tick(
        update,
        &[result_row("docs", added_doc, added_doc_tx)],
        &[],
        false,
        (identity, seed, "recursive-add"),
    );

    accept_recursive_row(
        &mut core,
        &mut parents,
        "team_edges",
        edge,
        team_edge_cells(alice, parent_team),
        3_000,
    );
    let update = maintained.update(
        &mut core,
        &shape,
        subscription,
        identity,
    );
    let _ = assert_maintained_subscription_view_tick(
        update,
        &[result_row("docs", closure_doc, closure_doc_tx)],
        &[],
        false,
        (identity, seed, "recursive-edge-add"),
    );

    delete_recursive_row(&mut core, &mut parents, "team_edges", edge, 4_000);
    let update = maintained.update(
        &mut core,
        &shape,
        subscription,
        identity,
    );
    let _ = assert_maintained_subscription_view_tick(
        update,
        &[],
        &[result_row("docs", closure_doc, closure_doc_tx)],
        false,
        (identity, seed, "recursive-edge-remove"),
    );
}

#[derive(Clone, Copy)]
enum MultiTableCaptureShape {
    ReferenceClosure,
    IncludeInner,
    IncludeHoles,
    JoinVia,
}

impl MultiTableCaptureShape {
    fn name(self) -> &'static str {
        match self {
            Self::ReferenceClosure => "reference-closure",
            Self::IncludeInner => "include-inner",
            Self::IncludeHoles => "include-holes",
            Self::JoinVia => "join-via",
        }
    }

    fn query(self) -> Query {
        let query = Query::from("roots").filter(eq(col("title"), lit("match")));
        match self {
            Self::ReferenceClosure => query,
            Self::IncludeInner => query.include_with(Include::new("target").require_includes()),
            Self::IncludeHoles => {
                query.include_with(Include::new("target").join_mode(JoinMode::Holes))
            }
            Self::JoinVia => query.join_via("members", "root", [eq(col("title"), lit("member"))]),
        }
    }
}

fn seeded_maintained_subscription_view_multitable_capture(
    seed: u64,
    identity: AuthorId,
    capture_shape: MultiTableCaptureShape,
) {
    let schema = maintained_view_multitable_capture_schema();
    let (_core_dir, mut core) = open_node_with_schema(node(0x82), schema.clone());
    let alice = user(0xa1);
    let bob = user(0xb2);
    let base = (seed as u8).wrapping_mul(16);
    let target_visible = row(base.wrapping_add(1));
    let target_hidden_then_visible = row(base.wrapping_add(2));
    let root_visible = row(base.wrapping_add(3));
    let root_hidden_target = row(base.wrapping_add(4));
    let root_added = row(base.wrapping_add(5));
    let root_removed = row(base.wrapping_add(6));
    let root_deleted = row(base.wrapping_add(7));
    let member_visible = row(base.wrapping_add(8));
    let member_added = row(base.wrapping_add(9));
    let mut parents = BTreeMap::<(&'static str, RowUuid), TxId>::new();
    let mut txs = BTreeMap::<(&'static str, RowUuid), TxId>::new();

    let accept = |core: &mut NodeState<RocksDbStorage>,
                  parents: &mut BTreeMap<(&'static str, RowUuid), TxId>,
                  txs: &mut BTreeMap<(&'static str, RowUuid), TxId>,
                  table: &'static str,
                  row_uuid: RowUuid,
                  made_at: u64,
                  cells: BTreeMap<String, Value>| {
        let mut commit = MergeableCommit::new(table, row_uuid, made_at).cells(cells);
        if let Some(parent) = parents.get(&(table, row_uuid)).copied() {
            commit = commit.parents(vec![parent]);
        }
        let tx_id = accept_global(core, commit);
        parents.insert((table, row_uuid), tx_id);
        txs.insert((table, row_uuid), tx_id);
        tx_id
    };

    accept(
        &mut core,
        &mut parents,
        &mut txs,
        "targets",
        target_visible,
        1_000,
        owner_cells(alice, "visible target"),
    );
    accept(
        &mut core,
        &mut parents,
        &mut txs,
        "targets",
        target_hidden_then_visible,
        1_001,
        owner_cells(bob, "hidden target"),
    );
    accept(
        &mut core,
        &mut parents,
        &mut txs,
        "roots",
        root_visible,
        1_010,
        root_cells(alice, "match", target_visible),
    );
    accept(
        &mut core,
        &mut parents,
        &mut txs,
        "roots",
        root_hidden_target,
        1_011,
        root_cells(alice, "match", target_hidden_then_visible),
    );
    accept(
        &mut core,
        &mut parents,
        &mut txs,
        "roots",
        root_removed,
        1_012,
        root_cells(alice, "match", target_visible),
    );
    accept(
        &mut core,
        &mut parents,
        &mut txs,
        "roots",
        root_deleted,
        1_013,
        root_cells(alice, "match", target_visible),
    );
    accept(
        &mut core,
        &mut parents,
        &mut txs,
        "members",
        member_visible,
        1_020,
        member_cells(alice, "member", root_visible),
    );

    let shape = capture_shape.query().validate(&schema).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let subscription = SubscriptionKey {
        shape_id: shape.shape_id(),
        binding_id: binding.binding_id(),
        read_view: Default::default(),
    };

    let (mut maintained, maintained_initial) = MaintainedSubscriptionViewSubscription::new(
        &mut core,
        &shape,
        &binding,
        subscription,
        identity,
    );
    let expected_initial_rows = match (capture_shape, identity == AuthorId::SYSTEM) {
        (MultiTableCaptureShape::JoinVia, _) => vec![
            ("roots", root_visible),
            ("targets", target_visible),
        ],
        (MultiTableCaptureShape::IncludeInner, false) => {
            vec![
                ("roots", root_visible),
                ("roots", root_removed),
                ("roots", root_deleted),
                ("targets", target_visible),
            ]
        }
        (_, false) => vec![
            ("roots", root_visible),
            ("roots", root_hidden_target),
            ("roots", root_removed),
            ("roots", root_deleted),
            ("targets", target_visible),
        ],
        _ => vec![
            ("roots", root_visible),
            ("roots", root_hidden_target),
            ("roots", root_removed),
            ("roots", root_deleted),
            ("targets", target_visible),
            ("targets", target_hidden_then_visible),
        ],
    };
    let expected_initial = expected_initial_rows
        .into_iter()
        .map(|(table, row_uuid)| result_row_from(&txs, table, row_uuid))
        .collect::<Vec<_>>();
    assert_maintained_subscription_view_tick(
        maintained_initial,
        &expected_initial,
        &[],
        false,
        (identity, seed, capture_shape.name()),
    );

    accept(
        &mut core,
        &mut parents,
        &mut txs,
        "roots",
        root_added,
        2_000,
        root_cells(alice, "match", target_visible),
    );
    accept(
        &mut core,
        &mut parents,
        &mut txs,
        "members",
        member_added,
        2_001,
        member_cells(alice, "member", root_added),
    );
    let update = maintained.update(
        &mut core,
        &shape,
        subscription,
        identity,
    );
    let _ = assert_maintained_subscription_view_tick(
        update,
        &[result_row_from(&txs, "roots", root_added)],
        &[],
        false,
        (identity, seed, "multitable-add"),
    );

    let target_hidden_initial_tx = txs[&("targets", target_hidden_then_visible)];
    accept(
        &mut core,
        &mut parents,
        &mut txs,
        "targets",
        target_hidden_then_visible,
        3_000,
        owner_cells(alice, "became visible"),
    );
    let update = maintained.update(
        &mut core,
        &shape,
        subscription,
        identity,
    );
    let mut update_adds = if matches!(capture_shape, MultiTableCaptureShape::JoinVia) {
        Vec::new()
    } else {
        vec![result_row_from(
            &txs,
            "targets",
            target_hidden_then_visible,
        )]
    };
    if matches!(capture_shape, MultiTableCaptureShape::IncludeInner)
        && identity != AuthorId::SYSTEM
    {
        update_adds.push(result_row_from(&txs, "roots", root_hidden_target));
    }
    let update_removes =
        if !matches!(capture_shape, MultiTableCaptureShape::JoinVia) && identity == AuthorId::SYSTEM
        {
            vec![result_row(
                "targets",
                target_hidden_then_visible,
                target_hidden_initial_tx,
            )]
        } else {
            Vec::new()
        };
    let _ = assert_maintained_subscription_view_tick(
        update,
        &update_adds,
        &update_removes,
        false,
        (identity, seed, "multitable-update"),
    );

    let root_removed_initial_tx = txs[&("roots", root_removed)];
    accept(
        &mut core,
        &mut parents,
        &mut txs,
        "roots",
        root_removed,
        4_000,
        root_cells(alice, "other", target_visible),
    );
    let update = maintained.update(
        &mut core,
        &shape,
        subscription,
        identity,
    );
    let remove_removes = if matches!(capture_shape, MultiTableCaptureShape::JoinVia) {
        Vec::new()
    } else {
        vec![result_row("roots", root_removed, root_removed_initial_tx)]
    };
    let _ = assert_maintained_subscription_view_tick(
        update,
        &[],
        &remove_removes,
        false,
        (identity, seed, "multitable-remove"),
    );

    let root_deleted_initial_tx = txs[&("roots", root_deleted)];
    let delete_parent = parents[&("roots", root_deleted)];
    let delete_tx = accept_global(
        &mut core,
        MergeableCommit::new("roots", root_deleted, 5_000)
            .parents(vec![delete_parent])
            .deletion(DeletionEvent::Deleted),
    );
    parents.insert(("roots", root_deleted), delete_tx);
    txs.insert(("roots", root_deleted), delete_tx);
    let update = maintained.update(
        &mut core,
        &shape,
        subscription,
        identity,
    );
    let delete_removes = if matches!(capture_shape, MultiTableCaptureShape::JoinVia) {
        Vec::new()
    } else {
        vec![result_row("roots", root_deleted, root_deleted_initial_tx)]
    };
    let _ = assert_maintained_subscription_view_tick(
        update,
        &[],
        &delete_removes,
        false,
        (identity, seed, "multitable-delete"),
    );
}

fn seeded_real_peer_maintained_subscription_view_capture(seed: u64, identity: AuthorId) {
    let schema = maintained_view_capture_schema();
    let (_core_dir, mut core) = open_node_with_schema(node(0x82), schema.clone());
    let alice = user(0xa1);
    let bob = user(0xb2);
    let base = (seed as u8).wrapping_mul(8);
    let initial_alice = row(base.wrapping_add(1));
    let initial_bob = row(base.wrapping_add(2));
    let predicate_remove = row(base.wrapping_add(3));
    let deleted = row(base.wrapping_add(4));
    let never_match = row(base.wrapping_add(5));
    let added = row(base.wrapping_add(6));
    let hidden_added = row(base.wrapping_add(7));
    let sibling_match = row(base.wrapping_add(8));
    let sibling_nonmatch = row(base.wrapping_add(9));
    let sibling_hidden = row(base.wrapping_add(10));
    let multi_match_a = row(base.wrapping_add(11));
    let multi_match_b = row(base.wrapping_add(12));
    let rls_revoked = row(base.wrapping_add(13));
    let mut parents = BTreeMap::<RowUuid, TxId>::new();
    let mut txs = BTreeMap::<RowUuid, TxId>::new();

    for (row_uuid, owner, title, made_at) in [
        (initial_alice, alice, "match", 1_000),
        (initial_bob, bob, "match", 1_001),
        (predicate_remove, alice, "match", 1_002),
        (deleted, alice, "match", 1_003),
        (never_match, alice, "other", 1_004),
        (rls_revoked, alice, "match", 1_005),
    ] {
        let tx_id = accept_owner_capture_row(&mut core, &mut parents, row_uuid, owner, title, made_at);
        txs.insert(row_uuid, tx_id);
    }

    let shape = Query::from("todos")
        .filter(eq(col("title"), lit("match")))
        .validate(&schema)
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let mut peer = if identity == AuthorId::SYSTEM {
        PeerState::new()
    } else {
        PeerState::for_author(identity)
    };

    let todo_entry = |row_uuid| (groove::Intern::new("todos".to_owned()), row_uuid, txs[&row_uuid]);
    let expected_initial = if identity == AuthorId::SYSTEM {
        vec![
            todo_entry(initial_alice),
            todo_entry(initial_bob),
            todo_entry(predicate_remove),
            todo_entry(deleted),
            todo_entry(rls_revoked),
        ]
    } else {
        vec![
            todo_entry(initial_alice),
            todo_entry(predicate_remove),
            todo_entry(deleted),
            todo_entry(rls_revoked),
        ]
    };

    let on_initial = peer.rehydrate_query(&mut core, &shape, &binding).unwrap();
    assert_real_peer_tick(
        capture_view_update(on_initial),
        &expected_initial,
        &[],
        true,
        (identity, seed, "initial"),
    );

    let assert_tick =
        |core: &mut NodeState<RocksDbStorage>,
         peer: &mut PeerState,
         txs: &BTreeMap<RowUuid, TxId>,
         expected_add_rows: &[RowUuid],
         expected_remove_rows: &[RowUuid],
         tick: &str| {
            let on = peer.query_update(core, &shape, &binding).unwrap();
            let entry =
                |row_uuid| (groove::Intern::new("todos".to_owned()), row_uuid, txs[&row_uuid]);
            let expected_adds = expected_add_rows
                .iter()
                .copied()
                .map(entry)
                .collect::<Vec<_>>();
            let expected_removes = expected_remove_rows
                .iter()
                .copied()
                .map(entry)
                .collect::<Vec<_>>();
            assert_real_peer_tick(
                capture_view_update(on),
                &expected_adds,
                &expected_removes,
                false,
                (identity, seed, tick),
            );
        };

    for (row_uuid, owner, title, made_at) in [
        (added, alice, "match", 2_000),
        (hidden_added, bob, "match", 2_001),
    ] {
        let tx_id = accept_owner_capture_row(&mut core, &mut parents, row_uuid, owner, title, made_at);
        txs.insert(row_uuid, tx_id);
    }
    let add_rows = if identity == AuthorId::SYSTEM {
        vec![added, hidden_added]
    } else {
        vec![added]
    };
    assert_tick(&mut core, &mut peer, &txs, &add_rows, &[], "add");

    let sibling_tx = core
        .commit_mergeable_many(vec![
            MergeableCommit::new("todos", sibling_match, 2_100)
                .cells(owner_cells(alice, "match")),
            MergeableCommit::new("todos", sibling_nonmatch, 2_100)
                .cells(owner_cells(alice, "other")),
            MergeableCommit::new("todos", sibling_hidden, 2_100)
                .cells(owner_cells(bob, "match")),
        ])
        .unwrap();
    core.apply_fate_update(
        sibling_tx,
        Fate::Accepted,
        Some(GlobalSeq(100)),
        Some(DurabilityTier::Global),
    )
    .unwrap();
    txs.insert(sibling_match, sibling_tx);
    txs.insert(sibling_nonmatch, sibling_tx);
    txs.insert(sibling_hidden, sibling_tx);
    let sibling_add_rows = if identity == AuthorId::SYSTEM {
        vec![sibling_match, sibling_hidden]
    } else {
        vec![sibling_match]
    };
    assert_tick(&mut core, &mut peer, &txs, &sibling_add_rows, &[], "sibling-add");

    let multi_tx = core
        .commit_mergeable_many(vec![
            MergeableCommit::new("todos", multi_match_a, 2_200)
                .cells(owner_cells(alice, "match")),
            MergeableCommit::new("todos", multi_match_b, 2_200)
                .cells(owner_cells(alice, "match")),
        ])
        .unwrap();
    core.apply_fate_update(
        multi_tx,
        Fate::Accepted,
        Some(GlobalSeq(101)),
        Some(DurabilityTier::Global),
    )
    .unwrap();
    txs.insert(multi_match_a, multi_tx);
    txs.insert(multi_match_b, multi_tx);
    assert_tick(
        &mut core,
        &mut peer,
        &txs,
        &[multi_match_a, multi_match_b],
        &[],
        "multi-add",
    );

    let previous_rls_revoked_tx = txs[&rls_revoked];
    let tx_id = accept_owner_capture_row(&mut core, &mut parents, rls_revoked, bob, "match", 2_300);
    let old_rls_entry = (
        groove::Intern::new("todos".to_owned()),
        rls_revoked,
        previous_rls_revoked_tx,
    );
    let new_rls_entry = (groove::Intern::new("todos".to_owned()), rls_revoked, tx_id);
    let (rls_adds, rls_removes) = if identity == AuthorId::SYSTEM {
        (vec![new_rls_entry], vec![old_rls_entry])
    } else {
        (vec![], vec![old_rls_entry])
    };
    let on = peer.query_update(&mut core, &shape, &binding).unwrap();
    assert_real_peer_tick(
        capture_view_update(on),
        &rls_adds,
        &rls_removes,
        false,
        (identity, seed, "rls-revocation"),
    );
    txs.insert(rls_revoked, tx_id);

    let previous_predicate_remove_tx = txs[&predicate_remove];
    let tx_id = accept_owner_capture_row(
        &mut core,
        &mut parents,
        predicate_remove,
        alice,
        "other",
        3_000,
    );
    txs.insert(predicate_remove, previous_predicate_remove_tx);
    assert_tick(
        &mut core,
        &mut peer,
        &txs,
        &[],
        &[predicate_remove],
        "predicate-remove",
    );
    txs.insert(predicate_remove, tx_id);

    let previous_deleted_tx = txs[&deleted];
    accept_capture_delete(&mut core, &mut parents, deleted, 4_000);
    txs.insert(deleted, previous_deleted_tx);
    assert_tick(&mut core, &mut peer, &txs, &[], &[deleted], "delete");

    let tx_id = accept_owner_capture_row(&mut core, &mut parents, deleted, alice, "match", 5_000);
    txs.insert(deleted, tx_id);
    assert_tick(&mut core, &mut peer, &txs, &[], &[], "restore");
}

#[test]
fn maintained_subscription_view_incremental_tick_avoids_per_reader_rematerialization_and_by_tx_scans() {
    let schema = maintained_view_capture_schema();
    let (_core_dir, mut core) = open_node_with_schema(node(0x82), schema.clone());
    let alice = user(0xa1);
    let bob = user(0xb2);
    let mut parents = BTreeMap::<RowUuid, TxId>::new();

    let initial_alice_tx =
        accept_owner_capture_row(&mut core, &mut parents, row(0x11), alice, "match", 1_000);
    accept_owner_capture_row(&mut core, &mut parents, row(0x12), bob, "match", 1_001);
    accept_owner_capture_row(&mut core, &mut parents, row(0x13), alice, "other", 1_002);

    let shape = Query::from("todos")
        .filter(eq(col("title"), lit("match")))
        .validate(&schema)
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let subscription = SubscriptionKey {
        shape_id: shape.shape_id(),
        binding_id: binding.binding_id(),
    read_view: Default::default(),
};

    let (mut maintained, maintained_initial) =
        MaintainedSubscriptionViewSubscription::new(&mut core, &shape, &binding, subscription, alice);
    assert_maintained_subscription_view_tick(
        maintained_initial,
        &[result_row("todos", row(0x11), initial_alice_tx)],
        &[],
        false,
        (alice, 0, "read-cost-initial"),
    );

    let added_tx = accept_owner_capture_row(&mut core, &mut parents, row(0x14), alice, "match", 2_000);
    accept_owner_capture_row(&mut core, &mut parents, row(0x15), bob, "match", 2_001);

    reset_query_versions_for_tx_call_count();
    core.reset_storage_read_metrics();
    let maintained_update = maintained.update(&mut core, &shape, subscription, alice);
    let maintained_metrics = core.take_storage_read_metrics();
    let maintained_query_versions_for_tx_calls = query_versions_for_tx_call_count();
    let maintained_full_bundle_count = view_update_full_bundle_count(&maintained_update);

    println!(
        "maintained_incremental_read_cost maintained_history_index_ranges={} maintained_tx_row_reads={} maintained_full_bundles={}",
        maintained_metrics.history_indexes.ranges,
        maintained_metrics.transactions_rows.reads,
        maintained_full_bundle_count
    );

    assert_maintained_subscription_view_tick(
        maintained_update,
        &[result_row("todos", row(0x14), added_tx)],
        &[],
        false,
        (alice, 0, "read-cost-add"),
    );
    assert_eq!(
        maintained_metrics.history_indexes.ranges, 0,
        "maintained per-reader incremental update scanned by_tx; maintained_metrics={maintained_metrics:?}"
    );
    assert!(
        maintained_metrics.transactions_rows.reads <= maintained_full_bundle_count,
        "maintained per-reader incremental update exceeded one transaction-row point lookup per full-bundle tx; full_bundles={maintained_full_bundle_count}, metrics={maintained_metrics:?}"
    );
    assert_eq!(
        maintained_query_versions_for_tx_calls, 0,
        "maintained incremental update called query_versions_for_tx"
    );
    assert!(
        maintained_full_bundle_count > 0,
        "read-cost tick must ship at least one version bundle"
    );
}

#[test]
fn maintained_subscription_view_emits_expected_owner_policy_updates() {
    let seeds = if let Ok(seed) = std::env::var("JAZZ_SEED") {
        vec![seed.parse::<u64>().expect("JAZZ_SEED must be a u64")]
    } else {
        let count = std::env::var("JAZZ_SEED_COUNT")
            .map(|count| count.parse::<u64>().expect("JAZZ_SEED_COUNT must be a u64"))
            .unwrap_or(12);
        (0x5eed..0x5eed + count).collect::<Vec<_>>()
    };
    println!(
        "maintained_subscription_view_emits_expected_owner_policy_updates seeds={}",
        seeds.len()
    );
    for seed in seeds {
        seeded_maintained_subscription_view_subscription_capture(seed, AuthorId::SYSTEM);
        seeded_maintained_subscription_view_subscription_capture(seed, user(0xa1));
    }
}

#[test]
fn maintained_subscription_view_multitable_emits_expected_updates() {
    let seeds = if let Ok(seed) = std::env::var("JAZZ_SEED") {
        vec![seed.parse::<u64>().expect("JAZZ_SEED must be a u64")]
    } else {
        let count = std::env::var("JAZZ_SEED_COUNT")
            .map(|count| count.parse::<u64>().expect("JAZZ_SEED_COUNT must be a u64"))
            .unwrap_or(12);
        (0x6eed..0x6eed + count).collect::<Vec<_>>()
    };
    println!(
        "maintained_subscription_view_multitable_emits_expected_updates seeds={}",
        seeds.len()
    );
    for seed in seeds {
        for shape in [
            MultiTableCaptureShape::ReferenceClosure,
            MultiTableCaptureShape::IncludeInner,
            MultiTableCaptureShape::IncludeHoles,
            MultiTableCaptureShape::JoinVia,
        ] {
            seeded_maintained_subscription_view_multitable_capture(seed, AuthorId::SYSTEM, shape);
            seeded_maintained_subscription_view_multitable_capture(seed, user(0xa1), shape);
        }
    }
}

#[test]
fn maintained_subscription_view_recursive_rls_emits_expected_updates() {
    let seeds = if let Ok(seed) = std::env::var("JAZZ_SEED") {
        vec![seed.parse::<u64>().expect("JAZZ_SEED must be a u64")]
    } else {
        let count = std::env::var("JAZZ_SEED_COUNT")
            .map(|count| count.parse::<u64>().expect("JAZZ_SEED_COUNT must be a u64"))
            .unwrap_or(12);
        (0x7eed..0x7eed + count).collect::<Vec<_>>()
    };
    println!(
        "maintained_subscription_view_recursive_rls_emits_expected_updates seeds={}",
        seeds.len()
    );
    for seed in seeds {
        seeded_maintained_subscription_view_recursive_rls_capture(seed, AuthorId::SYSTEM);
        seeded_maintained_subscription_view_recursive_rls_capture(seed, user(0xa1));
    }
}

#[test]
fn maintained_subscription_view_real_peer_path_emits_expected_view_updates() {
    let seeds = [0x51, 0x62];
    eprintln!(
        "maintained_subscription_view_real_peer_path_emits_expected_view_updates seeds={}",
        seeds
            .iter()
            .map(|seed| format!("{seed:#x}"))
            .collect::<Vec<_>>()
            .join(",")
    );
    for seed in seeds {
        seeded_real_peer_maintained_subscription_view_capture(seed, AuthorId::SYSTEM);
        seeded_real_peer_maintained_subscription_view_capture(seed, user(0xa1));
    }
}
