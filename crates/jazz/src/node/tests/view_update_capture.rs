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

fn seeded_view_update_capture(seed: u64) -> Vec<CanonicalViewUpdate> {
    let (_core_dir, mut core) = open_node_with_uuid(node(0x82));
    let mut parents = BTreeMap::<RowUuid, TxId>::new();

    for step in 0..8_u64 {
        let row_uuid = row(((seed + step * 3) % 4) as u8 + 1);
        let mut commit = MergeableCommit::new("todos", row_uuid, 1_000 + step);
        if let Some(parent) = parents.get(&row_uuid).copied() {
            commit = commit.parents(vec![parent]);
        }
        let title = format!("seed-{seed}-step-{step}");
        let tx_id = core.commit_mergeable(commit.cells(title_cells(title))).unwrap();
        parents.insert(row_uuid, tx_id);
        core.apply_fate_update(
            tx_id,
            Fate::Accepted,
            Some(GlobalSeq(step)),
            Some(DurabilityTier::Global),
        )
        .unwrap();
    }

    let shape = Query::from("todos")
        .filter(ne(col("title"), lit("seed-never-matches")))
        .validate(&schema())
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let mut peer = PeerState::new();
    peer.force_full_recompute_path_for_test(true);
    let first = peer.query_update(&mut core, &shape, &binding).unwrap();

    let row_uuid = row(((seed + 9) % 4) as u8 + 1);
    let mut commit = MergeableCommit::new("todos", row_uuid, 2_000);
    if let Some(parent) = parents.get(&row_uuid).copied() {
        commit = commit.parents(vec![parent]);
    }
    let tx_id = core
        .commit_mergeable(commit.cells(title_cells(format!("seed-{seed}-delta"))))
        .unwrap();
    core.apply_fate_update(
        tx_id,
        Fate::Accepted,
        Some(GlobalSeq(99)),
        Some(DurabilityTier::Global),
    )
    .unwrap();
    let second = peer.query_update(&mut core, &shape, &binding).unwrap();

    vec![capture_view_update(first), capture_view_update(second)]
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

fn assert_maintained_view_capture_tick(
    core: &mut NodeState<RocksDbStorage>,
    peer: &mut PeerState,
    previous_maintained_view_result_set: &mut BTreeSet<ResultRowEntry>,
    shape: &ValidatedQuery,
    binding: &Binding,
    case: (AuthorId, u64, &str),
) -> SyncMessage {
    let (identity, seed, tick) = case;
    let subscription = SubscriptionKey {
        shape_id: shape.shape_id(),
        binding_id: binding.binding_id(),
    read_view: Default::default(),
};
    let peer_complete_tx_payload_refs = peer.shipped_complete_tx_payloads().clone();
    core.reset_storage_read_metrics();
    let full_recompute = peer.query_update(core, shape, binding).unwrap();
    let full_recompute_read_metrics = core.take_storage_read_metrics();
    let full_recompute_full_bundle_count = view_update_full_bundle_count(&full_recompute);
    reset_query_versions_for_tx_call_count();
    reset_maintained_view_add_bundle_stats();
    reset_maintained_view_removal_bundle_stats();
    let (maintained_view, maintained_view_bundle_read_metrics) = core
        .maintained_view_query_update_with_bundle_read_metrics(
            shape,
            binding,
            subscription,
            peer_complete_tx_payload_refs,
            previous_maintained_view_result_set.clone(),
            identity,
        )
        .unwrap();
    let query_versions_for_tx_calls = query_versions_for_tx_call_count();
    let add_bundle_stats = maintained_view_add_bundle_stats();
    let removal_bundle_stats = maintained_view_removal_bundle_stats();
    let maintained_view_full_bundle_count = view_update_full_bundle_count(&maintained_view);
    println!(
        "maintained_view_metrics seed={seed:#x} identity={identity:?} tick={tick} full_recompute_history_index_ranges={} incremental_bundle_history_index_ranges={} incremental_bundle_tx_row_reads={} incremental_full_bundles={}",
        full_recompute_read_metrics.history_indexes.ranges,
        maintained_view_bundle_read_metrics.history_indexes.ranges,
        maintained_view_bundle_read_metrics.transactions_rows.reads,
        maintained_view_full_bundle_count
    );
    let SyncMessage::ViewUpdate {
        result_member_adds,
        result_member_removes,
        ..
    } = &maintained_view
    else {
        panic!("expected view update");
    };
    assert_eq!(
        maintained_view_bundle_read_metrics.history_indexes.ranges, 0,
        "incremental bundle assembly scanned by_tx for seed {seed:#x}, identity {identity:?}, tick {tick}; full_recompute={full_recompute_read_metrics:?}, incremental_bundle={maintained_view_bundle_read_metrics:?}"
    );
    assert!(
        maintained_view_bundle_read_metrics.transactions_rows.reads <= maintained_view_full_bundle_count,
        "maintained view bundle assembly exceeded one transaction-row point lookup per full-bundle tx for seed {seed:#x}, identity {identity:?}, tick {tick}; full_bundles={maintained_view_full_bundle_count}, metrics={maintained_view_bundle_read_metrics:?}"
    );
    let _ = full_recompute_full_bundle_count;
    if matches!(tick, "predicate-remove" | "delete") {
        assert!(
            removal_bundle_stats.stream_bundles > 0,
            "maintained_view removal serializer did not use the replacement stream for seed {seed:#x}, identity {identity:?}, tick {tick}"
        );
    }
    assert_eq!(
        query_versions_for_tx_calls, 0,
        "maintained_view serializer called query_versions_for_tx for seed {seed:#x}, identity {identity:?}, tick {tick}"
    );
    if !result_member_adds.is_empty() && result_member_removes.is_empty() {
        assert!(
            add_bundle_stats.stream_b_bundles > 0,
            "maintained_view serializer did not use Stream B add bundles for seed {seed:#x}, identity {identity:?}, tick {tick}"
        );
    }
    apply_capture_result_delta(previous_maintained_view_result_set, &maintained_view);
    let maintained_view_capture = capture_view_update(maintained_view.clone());
    let full_recompute_capture = capture_view_update(full_recompute);
    assert_eq!(
        maintained_view_capture,
        full_recompute_capture,
        "incremental serializer diverged from full recompute for seed {seed:#x}, identity {identity:?}, tick {tick}"
    );
    maintained_view
}

struct MaintainedSubscriptionViewSubscription {
    subscription: groove::ivm::MultisinkSubscription,
    maintained: crate::node::maintained_subscription_view::MaintainedSubscriptionView,
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
        let (subscription, maintained, transitions, tables) = core
            .maintained_subscription_view_from_cold_snapshot(
                shape,
                binding,
                identity,
                DurabilityTier::Global,
            )
            .unwrap();
        assert!(
            transitions.removes.is_empty(),
            "cold maintained snapshot emitted result removes"
        );
        let mut driver = Self {
            subscription,
            maintained,
            tables,
            previous_result_set: BTreeSet::new(),
            peer_complete_tx_payloads: BTreeSet::new(),
        };
        let output_tables = core.maintained_view_terminal_tables(shape).unwrap();
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
        let output_tables = core.maintained_view_terminal_tables(shape).unwrap();
        let mut states = BTreeMap::<ResultRowEntry, (bool, bool)>::new();
        loop {
            match self.subscription.try_recv() {
                Ok(deltas) => {
                    let transitions = crate::node::apply_maintained_multisink_deltas(
                        &mut self.maintained,
                        deltas,
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
        let mut update = core.view_update_for_query_result_delta_maintained_view_add_bundles(
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
                versions_by_tx: |tx_id| self.maintained.versions_by_tx(tx_id),
                replacement_for: |table: String, row_uuid| {
                    self.maintained.replacement_for(&table, row_uuid)
                },
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

fn assert_maintained_subscription_view_capture_tick(
    core: &mut NodeState<RocksDbStorage>,
    peer: &mut PeerState,
    maintained: &mut MaintainedSubscriptionViewSubscription,
    shape: &ValidatedQuery,
    binding: &Binding,
    case: (AuthorId, u64, &str),
) -> SyncMessage {
    let (identity, seed, tick) = case;
    let subscription = SubscriptionKey {
        shape_id: shape.shape_id(),
        binding_id: binding.binding_id(),
    read_view: Default::default(),
};
    let full_recompute = peer.query_update(core, shape, binding).unwrap();
    let maintained_update = maintained.update(core, shape, subscription, identity);
    let maintained_capture = capture_view_update(maintained_update.clone());
    let full_recompute_capture = capture_view_update(full_recompute);
    assert_eq!(
        maintained_capture,
        full_recompute_capture,
        "maintained subscription view serializer diverged from full recompute for seed {seed:#x}, identity {identity:?}, tick {tick}"
    );
    maintained_update
}

fn seeded_maintained_view_serializer_capture(seed: u64, identity: AuthorId) {
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

    accept_owner_capture_row(&mut core, &mut parents, initial_alice, alice, "match", 1_000);
    accept_owner_capture_row(&mut core, &mut parents, initial_bob, bob, "match", 1_001);
    accept_owner_capture_row(&mut core, &mut parents, predicate_remove, alice, "match", 1_002);
    accept_owner_capture_row(&mut core, &mut parents, deleted, alice, "match", 1_003);
    accept_owner_capture_row(&mut core, &mut parents, never_match, alice, "other", 1_004);
    let rls_revoked_initial_tx =
        accept_owner_capture_row(&mut core, &mut parents, rls_revoked, alice, "match", 1_005);

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
    peer.force_full_recompute_path_for_test(true);
    peer.track_query_for_test(&mut core, &shape, &binding)
        .unwrap();
    let mut maintained_view_result_set = BTreeSet::<ResultRowEntry>::new();

    let _ = assert_maintained_view_capture_tick(
        &mut core,
        &mut peer,
        &mut maintained_view_result_set,
        &shape,
        &binding,
        (identity, seed, "initial"),
    );

    accept_owner_capture_row(&mut core, &mut parents, added, alice, "match", 2_000);
    accept_owner_capture_row(&mut core, &mut parents, hidden_added, bob, "match", 2_001);
    let _ = assert_maintained_view_capture_tick(
        &mut core,
        &mut peer,
        &mut maintained_view_result_set,
        &shape,
        &binding,
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
    let sibling_update = assert_maintained_view_capture_tick(
        &mut core,
        &mut peer,
        &mut maintained_view_result_set,
        &shape,
        &binding,
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
    let multi_update = assert_maintained_view_capture_tick(
        &mut core,
        &mut peer,
        &mut maintained_view_result_set,
        &shape,
        &binding,
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
    let revocation_update = assert_maintained_view_capture_tick(
        &mut core,
        &mut peer,
        &mut maintained_view_result_set,
        &shape,
        &binding,
        (identity, seed, "rls-revocation"),
    );
    if identity == alice {
        assert_retraction_without_replacement_leak(
            &revocation_update,
            rls_revoked,
            rls_revoked_initial_tx,
            rls_revoked_unreadable_tx,
        );
    }

    accept_owner_capture_row(
        &mut core,
        &mut parents,
        predicate_remove,
        alice,
        "other",
        3_000,
    );
    let _ = assert_maintained_view_capture_tick(
        &mut core,
        &mut peer,
        &mut maintained_view_result_set,
        &shape,
        &binding,
        (identity, seed, "predicate-remove"),
    );

    accept_capture_delete(&mut core, &mut parents, deleted, 4_000);
    let _ = assert_maintained_view_capture_tick(
        &mut core,
        &mut peer,
        &mut maintained_view_result_set,
        &shape,
        &binding,
        (identity, seed, "delete"),
    );

    accept_owner_capture_row(&mut core, &mut parents, deleted, alice, "match", 5_000);
    let _ = assert_maintained_view_capture_tick(
        &mut core,
        &mut peer,
        &mut maintained_view_result_set,
        &shape,
        &binding,
        (identity, seed, "restore"),
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

    accept_owner_capture_row(&mut core, &mut parents, initial_alice, alice, "match", 1_000);
    accept_owner_capture_row(&mut core, &mut parents, initial_bob, bob, "match", 1_001);
    accept_owner_capture_row(&mut core, &mut parents, predicate_remove, alice, "match", 1_002);
    accept_owner_capture_row(&mut core, &mut parents, deleted, alice, "match", 1_003);
    accept_owner_capture_row(&mut core, &mut parents, never_match, alice, "other", 1_004);
    let rls_revoked_initial_tx =
        accept_owner_capture_row(&mut core, &mut parents, rls_revoked, alice, "match", 1_005);

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
    peer.force_full_recompute_path_for_test(true);
    peer.track_query_for_test(&mut core, &shape, &binding)
        .unwrap();
    let subscription = SubscriptionKey {
        shape_id: shape.shape_id(),
        binding_id: binding.binding_id(),
    read_view: Default::default(),
};

    let full_recompute_initial = peer.query_update(&mut core, &shape, &binding).unwrap();
    let (mut maintained, maintained_initial) = MaintainedSubscriptionViewSubscription::new(
        &mut core,
        &shape,
        &binding,
        subscription,
        identity,
    );
    assert_eq!(
        capture_view_update(maintained_initial),
        capture_view_update(full_recompute_initial),
        "maintained subscription view serializer diverged from full recompute for seed {seed:#x}, identity {identity:?}, tick initial"
    );

    accept_owner_capture_row(&mut core, &mut parents, added, alice, "match", 2_000);
    accept_owner_capture_row(&mut core, &mut parents, hidden_added, bob, "match", 2_001);
    let _ = assert_maintained_subscription_view_capture_tick(
        &mut core,
        &mut peer,
        &mut maintained,
        &shape,
        &binding,
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
    let sibling_update = assert_maintained_subscription_view_capture_tick(
        &mut core,
        &mut peer,
        &mut maintained,
        &shape,
        &binding,
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
    let multi_update = assert_maintained_subscription_view_capture_tick(
        &mut core,
        &mut peer,
        &mut maintained,
        &shape,
        &binding,
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
    let revocation_update = assert_maintained_subscription_view_capture_tick(
        &mut core,
        &mut peer,
        &mut maintained,
        &shape,
        &binding,
        (identity, seed, "rls-revocation"),
    );
    if identity == alice {
        assert_retraction_without_replacement_leak(
            &revocation_update,
            rls_revoked,
            rls_revoked_initial_tx,
            rls_revoked_unreadable_tx,
        );
    }

    accept_owner_capture_row(
        &mut core,
        &mut parents,
        predicate_remove,
        alice,
        "other",
        3_000,
    );
    let _ = assert_maintained_subscription_view_capture_tick(
        &mut core,
        &mut peer,
        &mut maintained,
        &shape,
        &binding,
        (identity, seed, "predicate-remove"),
    );

    accept_capture_delete(&mut core, &mut parents, deleted, 4_000);
    let _ = assert_maintained_subscription_view_capture_tick(
        &mut core,
        &mut peer,
        &mut maintained,
        &shape,
        &binding,
        (identity, seed, "delete"),
    );

    accept_owner_capture_row(&mut core, &mut parents, deleted, alice, "match", 5_000);
    let _ = assert_maintained_subscription_view_capture_tick(
        &mut core,
        &mut peer,
        &mut maintained,
        &shape,
        &binding,
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
    accept_recursive_row(
        &mut core,
        &mut parents,
        "docs",
        direct_doc,
        doc_cells("direct", "match"),
        1_000,
    );
    accept_recursive_row(
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
    let mut peer = if identity == AuthorId::SYSTEM {
        PeerState::new()
    } else {
        PeerState::for_author(identity)
    };
    peer.force_full_recompute_path_for_test(true);
    peer.track_query_for_test(&mut core, &shape, &binding)
        .unwrap();
    peer.clear_query_subscription_for_test(subscription);

    let full_recompute_initial = peer.query_update(&mut core, &shape, &binding).unwrap();
    let (mut maintained, maintained_initial) = MaintainedSubscriptionViewSubscription::new(
        &mut core,
        &shape,
        &binding,
        subscription,
        identity,
    );
    assert_eq!(
        capture_view_update(maintained_initial),
        capture_view_update(full_recompute_initial),
        "maintained subscription view recursive RLS diverged from full recompute for seed {seed:#x}, identity {identity:?}, tick initial"
    );

    accept_recursive_row(
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
    peer.clear_query_subscription_for_test(subscription);
    let _ = assert_maintained_subscription_view_capture_tick(
        &mut core,
        &mut peer,
        &mut maintained,
        &shape,
        &binding,
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
    peer.clear_query_subscription_for_test(subscription);
    let _ = assert_maintained_subscription_view_capture_tick(
        &mut core,
        &mut peer,
        &mut maintained,
        &shape,
        &binding,
        (identity, seed, "recursive-edge-add"),
    );

    delete_recursive_row(&mut core, &mut parents, "team_edges", edge, 4_000);
    peer.clear_query_subscription_for_test(subscription);
    let _ = assert_maintained_subscription_view_capture_tick(
        &mut core,
        &mut peer,
        &mut maintained,
        &shape,
        &binding,
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

    let accept = |core: &mut NodeState<RocksDbStorage>,
                  parents: &mut BTreeMap<(&'static str, RowUuid), TxId>,
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
        tx_id
    };

    accept(
        &mut core,
        &mut parents,
        "targets",
        target_visible,
        1_000,
        owner_cells(alice, "visible target"),
    );
    accept(
        &mut core,
        &mut parents,
        "targets",
        target_hidden_then_visible,
        1_001,
        owner_cells(bob, "hidden target"),
    );
    accept(
        &mut core,
        &mut parents,
        "roots",
        root_visible,
        1_010,
        root_cells(alice, "match", target_visible),
    );
    accept(
        &mut core,
        &mut parents,
        "roots",
        root_hidden_target,
        1_011,
        root_cells(alice, "match", target_hidden_then_visible),
    );
    accept(
        &mut core,
        &mut parents,
        "roots",
        root_removed,
        1_012,
        root_cells(alice, "match", target_visible),
    );
    accept(
        &mut core,
        &mut parents,
        "roots",
        root_deleted,
        1_013,
        root_cells(alice, "match", target_visible),
    );
    accept(
        &mut core,
        &mut parents,
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
    let mut peer = if identity == AuthorId::SYSTEM {
        PeerState::new()
    } else {
        PeerState::for_author(identity)
    };
    peer.force_full_recompute_path_for_test(true);
    peer.track_query_for_test(&mut core, &shape, &binding)
        .unwrap();

    let full_recompute_initial = peer.query_update(&mut core, &shape, &binding).unwrap();
    let (mut maintained, maintained_initial) = MaintainedSubscriptionViewSubscription::new(
        &mut core,
        &shape,
        &binding,
        subscription,
        identity,
    );
    assert_eq!(
        capture_view_update(maintained_initial),
        capture_view_update(full_recompute_initial),
        "maintained subscription view multitable diverged from full recompute shape={} seed={seed:#x} identity={identity:?} tick initial",
        capture_shape.name()
    );

    accept(
        &mut core,
        &mut parents,
        "roots",
        root_added,
        2_000,
        root_cells(alice, "match", target_visible),
    );
    accept(
        &mut core,
        &mut parents,
        "members",
        member_added,
        2_001,
        member_cells(alice, "member", root_added),
    );
    let _ = assert_maintained_subscription_view_capture_tick(
        &mut core,
        &mut peer,
        &mut maintained,
        &shape,
        &binding,
        (identity, seed, "multitable-add"),
    );

    accept(
        &mut core,
        &mut parents,
        "targets",
        target_hidden_then_visible,
        3_000,
        owner_cells(alice, "became visible"),
    );
    let _ = assert_maintained_subscription_view_capture_tick(
        &mut core,
        &mut peer,
        &mut maintained,
        &shape,
        &binding,
        (identity, seed, "multitable-update"),
    );

    accept(
        &mut core,
        &mut parents,
        "roots",
        root_removed,
        4_000,
        root_cells(alice, "other", target_visible),
    );
    let _ = assert_maintained_subscription_view_capture_tick(
        &mut core,
        &mut peer,
        &mut maintained,
        &shape,
        &binding,
        (identity, seed, "multitable-remove"),
    );

    let delete_parent = parents[&("roots", root_deleted)];
    let delete_tx = accept_global(
        &mut core,
        MergeableCommit::new("roots", root_deleted, 5_000)
            .parents(vec![delete_parent])
            .deletion(DeletionEvent::Deleted),
    );
    parents.insert(("roots", root_deleted), delete_tx);
    let _ = assert_maintained_subscription_view_capture_tick(
        &mut core,
        &mut peer,
        &mut maintained,
        &shape,
        &binding,
        (identity, seed, "multitable-delete"),
    );
}

fn seeded_real_peer_maintained_subscription_view_capture(seed: u64, identity: AuthorId) {
    let schema = maintained_view_capture_schema();
    let (_off_dir, mut off_core) = open_node_with_schema(node(0x82), schema.clone());
    let (_on_dir, mut on_core) = open_node_with_schema(node(0x82), schema.clone());
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
    let mut off_parents = BTreeMap::<RowUuid, TxId>::new();
    let mut on_parents = BTreeMap::<RowUuid, TxId>::new();

    for (row_uuid, owner, title, made_at) in [
        (initial_alice, alice, "match", 1_000),
        (initial_bob, bob, "match", 1_001),
        (predicate_remove, alice, "match", 1_002),
        (deleted, alice, "match", 1_003),
        (never_match, alice, "other", 1_004),
        (rls_revoked, alice, "match", 1_005),
    ] {
        accept_owner_capture_row(&mut off_core, &mut off_parents, row_uuid, owner, title, made_at);
        accept_owner_capture_row(&mut on_core, &mut on_parents, row_uuid, owner, title, made_at);
    }

    let shape = Query::from("todos")
        .filter(eq(col("title"), lit("match")))
        .validate(&schema)
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let mut off_peer = if identity == AuthorId::SYSTEM {
        PeerState::new()
    } else {
        PeerState::for_author(identity)
    };
    off_peer.force_full_recompute_path_for_test(true);
    let mut on_peer = if identity == AuthorId::SYSTEM {
        PeerState::new()
    } else {
        PeerState::for_author(identity)
    };

    let off_initial = off_peer
        .rehydrate_query(&mut off_core, &shape, &binding)
        .unwrap();
    let on_initial = on_peer
        .rehydrate_query(&mut on_core, &shape, &binding)
        .unwrap();
    assert_eq!(
        capture_view_update(on_initial),
        capture_view_update(off_initial),
        "real peer maintained subscription view diverged for seed {seed:#x}, identity {identity:?}, tick initial"
    );

    let assert_tick =
        |off_core: &mut NodeState<RocksDbStorage>,
         on_core: &mut NodeState<RocksDbStorage>,
         off_peer: &mut PeerState,
         on_peer: &mut PeerState,
         tick: &str| {
            let off = off_peer.query_update(off_core, &shape, &binding).unwrap();
            let on = on_peer.query_update(on_core, &shape, &binding).unwrap();
            assert_eq!(
                capture_view_update(on),
                capture_view_update(off),
                "real peer maintained subscription view diverged for seed {seed:#x}, identity {identity:?}, tick {tick}"
            );
        };

    for (row_uuid, owner, title, made_at) in [
        (added, alice, "match", 2_000),
        (hidden_added, bob, "match", 2_001),
    ] {
        accept_owner_capture_row(
            &mut off_core,
            &mut off_parents,
            row_uuid,
            owner,
            title,
            made_at,
        );
        accept_owner_capture_row(
            &mut on_core,
            &mut on_parents,
            row_uuid,
            owner,
            title,
            made_at,
        );
    }
    assert_tick(
        &mut off_core,
        &mut on_core,
        &mut off_peer,
        &mut on_peer,
        "add",
    );

    for core in [&mut off_core, &mut on_core] {
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
    }
    assert_tick(
        &mut off_core,
        &mut on_core,
        &mut off_peer,
        &mut on_peer,
        "sibling-add",
    );

    for core in [&mut off_core, &mut on_core] {
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
    }
    assert_tick(
        &mut off_core,
        &mut on_core,
        &mut off_peer,
        &mut on_peer,
        "multi-add",
    );

    accept_owner_capture_row(&mut off_core, &mut off_parents, rls_revoked, bob, "match", 2_300);
    accept_owner_capture_row(&mut on_core, &mut on_parents, rls_revoked, bob, "match", 2_300);
    assert_tick(
        &mut off_core,
        &mut on_core,
        &mut off_peer,
        &mut on_peer,
        "rls-revocation",
    );

    accept_owner_capture_row(
        &mut off_core,
        &mut off_parents,
        predicate_remove,
        alice,
        "other",
        3_000,
    );
    accept_owner_capture_row(
        &mut on_core,
        &mut on_parents,
        predicate_remove,
        alice,
        "other",
        3_000,
    );
    assert_tick(
        &mut off_core,
        &mut on_core,
        &mut off_peer,
        &mut on_peer,
        "predicate-remove",
    );

    accept_capture_delete(&mut off_core, &mut off_parents, deleted, 4_000);
    accept_capture_delete(&mut on_core, &mut on_parents, deleted, 4_000);
    assert_tick(
        &mut off_core,
        &mut on_core,
        &mut off_peer,
        &mut on_peer,
        "delete",
    );

    accept_owner_capture_row(&mut off_core, &mut off_parents, deleted, alice, "match", 5_000);
    accept_owner_capture_row(&mut on_core, &mut on_parents, deleted, alice, "match", 5_000);
    assert_tick(
        &mut off_core,
        &mut on_core,
        &mut off_peer,
        &mut on_peer,
        "restore",
    );
    assert_eq!(
        on_peer.maintained_subscription_view_metrics().full_recomputes_out,
        0,
        "real peer maintained subscription view used the full-recompute path for seed {seed:#x}, identity {identity:?}"
    );
}

#[test]
fn maintained_subscription_view_incremental_tick_avoids_per_reader_rematerialization_and_by_tx_scans() {
    let schema = maintained_view_capture_schema();
    let (_core_dir, mut core) = open_node_with_schema(node(0x82), schema.clone());
    let alice = user(0xa1);
    let bob = user(0xb2);
    let mut parents = BTreeMap::<RowUuid, TxId>::new();

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
    let mut peer = PeerState::for_author(alice);
    peer.force_full_recompute_path_for_test(true);
    peer.track_query_for_test(&mut core, &shape, &binding)
        .unwrap();

    let full_recompute_initial = peer.query_update(&mut core, &shape, &binding).unwrap();
    let (mut maintained, maintained_initial) =
        MaintainedSubscriptionViewSubscription::new(&mut core, &shape, &binding, subscription, alice);
    assert_eq!(
        capture_view_update(maintained_initial),
        capture_view_update(full_recompute_initial),
        "maintained subscription view cold snapshot diverged before incremental read-cost tick"
    );

    accept_owner_capture_row(&mut core, &mut parents, row(0x14), alice, "match", 2_000);
    accept_owner_capture_row(&mut core, &mut parents, row(0x15), bob, "match", 2_001);

    reset_query_versions_for_tx_call_count();
    reset_maintained_view_materialize_call_count();
    core.reset_storage_read_metrics();
    let maintained_update = maintained.update(&mut core, &shape, subscription, alice);
    let maintained_metrics = core.take_storage_read_metrics();
    let maintained_query_versions_for_tx_calls = query_versions_for_tx_call_count();
    let maintained_materialize_calls = maintained_view_materialize_call_count();
    let maintained_full_bundle_count = view_update_full_bundle_count(&maintained_update);

    core.reset_storage_read_metrics();
    let full_recompute_update = peer.query_update(&mut core, &shape, &binding).unwrap();
    let full_recompute_metrics = core.take_storage_read_metrics();
    let full_recompute_full_bundle_count = view_update_full_bundle_count(&full_recompute_update);

    println!(
        "maintained_incremental_read_cost full_recompute_history_index_ranges={} maintained_history_index_ranges={} maintained_tx_row_reads={} maintained_full_bundles={}",
        full_recompute_metrics.history_indexes.ranges,
        maintained_metrics.history_indexes.ranges,
        maintained_metrics.transactions_rows.reads,
        maintained_full_bundle_count
    );

    assert_eq!(
        capture_view_update(maintained_update),
        capture_view_update(full_recompute_update),
        "maintained subscription view incremental read-cost tick diverged from full recompute"
    );
    assert_eq!(
        maintained_metrics.history_indexes.ranges, 0,
        "maintained per-reader incremental update scanned by_tx; full_recompute_history_index_ranges={}, maintained_metrics={maintained_metrics:?}",
        full_recompute_metrics.history_indexes.ranges
    );
    assert!(
        maintained_metrics.transactions_rows.reads <= maintained_full_bundle_count,
        "maintained per-reader incremental update exceeded one transaction-row point lookup per full-bundle tx; full_bundles={maintained_full_bundle_count}, metrics={maintained_metrics:?}"
    );
    let _ = full_recompute_full_bundle_count;
    assert_eq!(
        maintained_materialize_calls, 0,
        "maintained incremental update re-materialized the maintained view graph"
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
fn seeded_view_update_capture_is_self_equivalent() {
    let first = seeded_view_update_capture(0x5eed);
    let second = seeded_view_update_capture(0x5eed);
    assert_eq!(first, second);
}

#[test]
fn incremental_serializer_matches_full_recompute_view_update_capture() {
    let seeds = if let Ok(seed) = std::env::var("JAZZ_SEED") {
        vec![seed.parse::<u64>().expect("JAZZ_SEED must be a u64")]
    } else {
        let count = std::env::var("JAZZ_SEED_COUNT")
            .map(|count| count.parse::<u64>().expect("JAZZ_SEED_COUNT must be a u64"))
            .unwrap_or(12);
        (0x5eed..0x5eed + count).collect::<Vec<_>>()
    };
    println!(
        "incremental_serializer_matches_full_recompute_view_update_capture seeds={}",
        seeds.len()
    );
    for seed in seeds {
        seeded_maintained_view_serializer_capture(seed, AuthorId::SYSTEM);
        seeded_maintained_view_serializer_capture(seed, user(0xa1));
    }
}

#[test]
fn maintained_subscription_view_matches_full_recompute_view_update_capture() {
    let seeds = if let Ok(seed) = std::env::var("JAZZ_SEED") {
        vec![seed.parse::<u64>().expect("JAZZ_SEED must be a u64")]
    } else {
        let count = std::env::var("JAZZ_SEED_COUNT")
            .map(|count| count.parse::<u64>().expect("JAZZ_SEED_COUNT must be a u64"))
            .unwrap_or(12);
        (0x5eed..0x5eed + count).collect::<Vec<_>>()
    };
    println!(
        "maintained_subscription_view_matches_full_recompute_view_update_capture seeds={}",
        seeds.len()
    );
    for seed in seeds {
        seeded_maintained_subscription_view_subscription_capture(seed, AuthorId::SYSTEM);
        seeded_maintained_subscription_view_subscription_capture(seed, user(0xa1));
    }
}

#[test]
fn maintained_subscription_view_multitable_matches_full_recompute_view_update_capture() {
    let seeds = if let Ok(seed) = std::env::var("JAZZ_SEED") {
        vec![seed.parse::<u64>().expect("JAZZ_SEED must be a u64")]
    } else {
        let count = std::env::var("JAZZ_SEED_COUNT")
            .map(|count| count.parse::<u64>().expect("JAZZ_SEED_COUNT must be a u64"))
            .unwrap_or(12);
        (0x6eed..0x6eed + count).collect::<Vec<_>>()
    };
    println!(
        "maintained_subscription_view_multitable_matches_full_recompute_view_update_capture seeds={}",
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
fn maintained_subscription_view_recursive_rls_matches_full_recompute_view_update_capture() {
    let seeds = if let Ok(seed) = std::env::var("JAZZ_SEED") {
        vec![seed.parse::<u64>().expect("JAZZ_SEED must be a u64")]
    } else {
        let count = std::env::var("JAZZ_SEED_COUNT")
            .map(|count| count.parse::<u64>().expect("JAZZ_SEED_COUNT must be a u64"))
            .unwrap_or(12);
        (0x7eed..0x7eed + count).collect::<Vec<_>>()
    };
    println!(
        "maintained_subscription_view_recursive_rls_matches_full_recompute_view_update_capture seeds={}",
        seeds.len()
    );
    for seed in seeds {
        seeded_maintained_subscription_view_recursive_rls_capture(seed, AuthorId::SYSTEM);
        seeded_maintained_subscription_view_recursive_rls_capture(seed, user(0xa1));
    }
}

#[test]
fn maintained_subscription_view_real_peer_path_matches_forced_full_recompute_view_update_capture() {
    let seeds = [0x51, 0x62];
    eprintln!(
        "maintained_subscription_view_real_peer_path_matches_forced_full_recompute_view_update_capture seeds={}",
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
