fn access_path_schema() -> JazzSchema {
    JazzSchema::new([TableSchema::new(
        "docs",
        [
            ColumnSchema::new("owner", ColumnType::Uuid),
            ColumnSchema::new("status", ColumnType::String),
            ColumnSchema::new("body", ColumnType::String),
        ],
    )
    .with_indexed_column("owner")])
}

fn access_path_doc_cells(owner: AuthorId, status: &str, body: &str) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("owner".to_owned(), Value::Uuid(owner.0)),
        ("status".to_owned(), Value::String(status.to_owned())),
        ("body".to_owned(), Value::String(body.to_owned())),
    ])
}

fn seed_access_path_docs(
    writer: &mut NodeState<RocksDbStorage>,
    core: &mut NodeState<RocksDbStorage>,
) -> (RowUuid, RowUuid, AuthorId) {
    let owner_a = user(0xa1);
    let owner_b = user(0xb2);
    let first = row(0x11);
    let second = row(0x22);
    commit_mergeable_global(
        writer,
        core,
        MergeableCommit::new("docs", first, 10)
            .cells(access_path_doc_cells(owner_a, "open", "first")),
    );
    commit_mergeable_global(
        writer,
        core,
        MergeableCommit::new("docs", second, 11)
            .cells(access_path_doc_cells(owner_b, "closed", "second")),
    );
    (first, second, owner_a)
}

fn query_rows_by_uuid(
    node: &mut NodeState<RocksDbStorage>,
    query: Query,
    tier: DurabilityTier,
) -> (Vec<RowUuid>, QueryEngineReadMetrics) {
    let shape = query.validate(&node.catalogue.schema).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    node.reset_query_engine_read_metrics();
    let rows = node
        .query_rows_for_link(&shape, &binding, tier, AuthorId::SYSTEM)
        .unwrap()
        .into_iter()
        .map(|row| row.row_uuid())
        .collect::<Vec<_>>();
    (rows, node.query_engine_read_metrics().clone())
}

#[test]
fn one_shot_filtered_read_uses_primary_key_scan_for_id_equality() {
    let schema = access_path_schema();
    let (_writer_dir, mut writer) = open_node_with_schema(node(8), schema.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    let (first, _second, _owner) = seed_access_path_docs(&mut writer, &mut core);
    let query = Query::from("docs").filter(eq(col("id"), lit(Value::Uuid(first.0))));

    let (selected, selected_metrics) =
        query_rows_by_uuid(&mut core, query.clone(), DurabilityTier::Global);
    let (forced_full, forced_metrics) = query_rows_by_uuid(&mut core, query, DurabilityTier::Local);

    assert_eq!(selected, forced_full);
    assert_eq!(selected, vec![first]);
    assert_eq!(selected_metrics.source_primary_key_scans, 1);
    assert_eq!(selected_metrics.source_index_probes, 0);
    assert_eq!(selected_metrics.source_full_scans, 0);
    assert_eq!(forced_metrics.source_full_scans, 1);
}

#[test]
fn one_shot_filtered_read_uses_declared_index_for_indexed_column_equality() {
    let schema = access_path_schema();
    let (_writer_dir, mut writer) = open_node_with_schema(node(8), schema.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    let (first, _second, owner) = seed_access_path_docs(&mut writer, &mut core);
    let query = Query::from("docs").filter(eq(col("owner"), lit(Value::Uuid(owner.0))));

    let (selected, selected_metrics) =
        query_rows_by_uuid(&mut core, query.clone(), DurabilityTier::Global);
    let (forced_full, forced_metrics) = query_rows_by_uuid(&mut core, query, DurabilityTier::Local);

    assert_eq!(selected, forced_full);
    assert_eq!(selected, vec![first]);
    assert_eq!(selected_metrics.source_primary_key_scans, 0);
    assert_eq!(selected_metrics.source_index_probes, 1);
    assert_eq!(selected_metrics.source_full_scans, 0);
    assert_eq!(forced_metrics.source_full_scans, 1);
}

#[test]
fn one_shot_filtered_read_keeps_residual_filters_after_pushdown() {
    let schema = access_path_schema();
    let (_writer_dir, mut writer) = open_node_with_schema(node(8), schema.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    let (first, _second, owner) = seed_access_path_docs(&mut writer, &mut core);
    let query = Query::from("docs")
        .filter(eq(col("owner"), lit(Value::Uuid(owner.0))))
        .filter(eq(col("status"), lit("open")));

    let (selected, selected_metrics) =
        query_rows_by_uuid(&mut core, query.clone(), DurabilityTier::Global);
    let (forced_full, _forced_metrics) = query_rows_by_uuid(&mut core, query, DurabilityTier::Local);

    assert_eq!(selected, forced_full);
    assert_eq!(selected, vec![first]);
    assert_eq!(selected_metrics.source_index_probes, 1);
    assert_eq!(selected_metrics.source_full_scans, 0);
}

#[test]
fn one_shot_filtered_read_counts_full_scan_for_unindexed_filter() {
    let schema = access_path_schema();
    let (_writer_dir, mut writer) = open_node_with_schema(node(8), schema.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    let (_first, second, _owner) = seed_access_path_docs(&mut writer, &mut core);
    let query = Query::from("docs").filter(eq(col("status"), lit("closed")));

    let (selected, selected_metrics) =
        query_rows_by_uuid(&mut core, query.clone(), DurabilityTier::Global);
    let (forced_full, forced_metrics) = query_rows_by_uuid(&mut core, query, DurabilityTier::Local);

    assert_eq!(selected, forced_full);
    assert_eq!(selected, vec![second]);
    assert_eq!(selected_metrics.source_primary_key_scans, 0);
    assert_eq!(selected_metrics.source_index_probes, 0);
    assert_eq!(selected_metrics.source_full_scans, 1);
    assert_eq!(forced_metrics.source_full_scans, 1);
}

#[test]
fn whole_table_predicate_probe_uses_table_change_watermark() {
    let schema = JazzSchema::new([
        TableSchema::new("todos", [ColumnSchema::new("title", ColumnType::String)]),
        TableSchema::new("notes", [ColumnSchema::new("title", ColumnType::String)]),
    ]);
    let (_writer_dir, mut writer) = open_node_with_schema(node(8), schema.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);

    let base = GlobalSeq(0);
    commit_mergeable_global(
        &mut writer,
        &mut core,
        MergeableCommit::new("notes", row(1), 10).cells(title_cells("other table")),
    );
    assert!(
        !core.global_currency_changed_after("todos", base).unwrap(),
        "other-table writes must not invalidate whole-table predicates"
    );

    commit_mergeable_global(
        &mut writer,
        &mut core,
        MergeableCommit::new("todos", row(2), 11).cells(title_cells("target table")),
    );
    assert!(
        core.global_currency_changed_after("todos", base).unwrap(),
        "same-table writes after the base snapshot invalidate whole-table predicates"
    );
}
#[test]
fn history_subscriptions_flow_through_groove() {
    let (_temp_dir, mut node) = open_node();
    let subscription = node.subscribe_history("todos").unwrap();
    assert!(subscription.recv().unwrap().is_empty());

    node.commit_mergeable(MergeableCommit::new("todos", row(8), 10).cells(title_cells("notify")))
        .unwrap();

    assert!(!subscription.recv().unwrap().is_empty());
}
#[test]
fn groove_current_rows_match_oracle_for_seeded_m1_commits() {
    let (_temp_dir, mut node) = open_node();
    let mut oracle = Oracle::new();
    let row = row(7);

    let base = commit_and_oracle(
        &mut node,
        &mut oracle,
        MergeableCommit::new("todos", row, 10).cells(title_cells("base")),
    );
    assert_current_rows_match_oracle(&mut node, &oracle);

    commit_and_oracle(
        &mut node,
        &mut oracle,
        MergeableCommit::new("todos", row, 9).cells(BTreeMap::from([(
            "title".to_owned(),
            "older clock".to_owned(),
        )])),
    );
    assert_current_rows_match_oracle(&mut node, &oracle);

    let child = commit_and_oracle(
        &mut node,
        &mut oracle,
        MergeableCommit::new("todos", row, 11)
            .parents(vec![base])
            .cells(title_cells("child")),
    );
    assert_current_rows_match_oracle(&mut node, &oracle);

    commit_and_oracle(
        &mut node,
        &mut oracle,
        MergeableCommit::new("todos", row, 12).deletion(DeletionEvent::Deleted),
    );
    assert_current_rows_match_oracle(&mut node, &oracle);

    commit_and_oracle(
        &mut node,
        &mut oracle,
        MergeableCommit::new("todos", row, 13)
            .parents(vec![child])
            .cells(BTreeMap::from([(
                "title".to_owned(),
                "delete-concurrent update".to_owned(),
            )])),
    );
    assert_current_rows_match_oracle(&mut node, &oracle);

    commit_and_oracle(
        &mut node,
        &mut oracle,
        MergeableCommit::new("todos", row, 14).deletion(DeletionEvent::Restored),
    );
    assert_current_rows_match_oracle(&mut node, &oracle);
}

#[test]
fn local_current_from_ahead_index_matches_history_argmax_for_seeded_commits() {
    for seed in 0..16_u64 {
        let (_temp_dir, mut node) = open_node();
        let mut parents = BTreeMap::<RowUuid, TxId>::new();
        let mut pending = Vec::<(RowUuid, TxId)>::new();
        let mut rng = seed.wrapping_mul(0x9e37_79b9_7f4a_7c15);

        for step in 0..96_u64 {
            rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
            let row_uuid = row(((rng >> 56) as u8 % 6) + 1);
            let action = (rng >> 48) % 9;
            let mut commit = MergeableCommit::new("todos", row_uuid, 1_000 + step);
            if let Some(parent) = parents.get(&row_uuid).copied() {
                commit = commit.parents(vec![parent]);
            }
            commit = match action {
                0 | 1 => commit.deletion(DeletionEvent::Deleted),
                2 | 3 => commit.deletion(DeletionEvent::Restored),
                _ => commit.cells(title_cells(format!("seed-{seed}-step-{step}"))),
            };

            let tx_id = node.commit_mergeable(commit).unwrap();
            parents.insert(row_uuid, tx_id);
            match action {
                0 | 4 | 5 => {
                    let global_seq = node.clock.next_global_seq;
                    node.apply_fate_update(
                        tx_id,
                        Fate::Accepted,
                        Some(global_seq),
                        Some(DurabilityTier::Global),
                    )
                    .unwrap();
                }
                1 | 6 => {
                    node.apply_fate_update(
                        tx_id,
                        Fate::Rejected(RejectionReason::ExclusiveConflict),
                        None,
                        None,
                    )
                    .unwrap();
                    parents.remove(&row_uuid);
                }
                _ => pending.push((row_uuid, tx_id)),
            }

            if step % 13 == 12
                && let Some((pending_row, tx_id)) = pending.pop()
            {
                node.apply_fate_update(
                    tx_id,
                    Fate::Rejected(RejectionReason::ExclusiveConflict),
                    None,
                    None,
                )
                .unwrap();
                parents.remove(&pending_row);
            }

            assert_local_current_matches_history_argmax(&mut node, seed, step);
        }
    }
}

fn assert_local_current_matches_history_argmax(
    node: &mut NodeState<RocksDbStorage>,
    seed: u64,
    step: u64,
) {
    let actual = node
        .current_rows("todos", DurabilityTier::Local)
        .unwrap()
        .into_iter()
        .map(current_row_pair)
        .collect::<BTreeMap<_, _>>();
    let expected = history_argmax_current_rows(node);
    assert_eq!(actual, expected, "seed {seed}, step {step}");
}

fn history_argmax_current_rows(
    node: &mut NodeState<RocksDbStorage>,
) -> BTreeMap<RowUuid, BTreeMap<String, Value>> {
    let table = node.table("todos").unwrap().clone();
    let versions = node.query_table_versions("todos").unwrap();
    let mut content = BTreeMap::<RowUuid, &VersionRow>::new();
    let mut registers = BTreeMap::<RowUuid, &VersionRow>::new();
    for version in &versions {
        let winners = match version.layer() {
            VersionLayer::Content => &mut content,
            VersionLayer::Deletion => &mut registers,
        };
        if winners.get(&version.row_uuid()).is_none_or(|current| {
            (version.tx_time(), version.tx_node_alias())
                > (current.tx_time(), current.tx_node_alias())
        }) {
            winners.insert(version.row_uuid(), version);
        }
    }
    content
        .into_iter()
        .filter_map(|(row_uuid, version)| {
            let deleted = registers
                .get(&row_uuid)
                .and_then(|register| register.deletion())
                == Some(DeletionEvent::Deleted);
            if deleted {
                return None;
            }
            let cells = table
                .columns
                .iter()
                .filter_map(|column| {
                    version
                        .cell(&table, &column.name)
                        .unwrap()
                        .map(|value| (column.name.clone(), value))
                })
                .collect::<BTreeMap<_, _>>();
            Some((row_uuid, cells))
        })
        .collect()
}
#[test]
fn filterless_shape_and_degenerate_predicate_validation_agree() {
    let (_client_dir, mut client) = open_node_with_uuid(node(1));
    let (_other_dir, mut other) = open_node_with_uuid(node(2));
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    let shape = crate::query::Query::from("todos")
        .validate(&schema())
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    register_shape_binding(&mut core, &shape, &binding);

    let tx_id = client.open_exclusive().unwrap();
    assert!(client.tx_query(tx_id, &shape, &binding).unwrap().is_empty());
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
    let SyncMessage::CommitUnit { tx, versions } = unit else {
        panic!("expected commit unit");
    };
    let predicate = tx
        .predicate_read_set
        .as_ref()
        .and_then(|reads| reads.first())
        .unwrap();
    assert!(
        core.predicate_read_is_degenerate_whole_table(predicate)
            .unwrap()
    );
    assert!(
        core.shape_predicate_changed_after(
            predicate,
            tx.base_snapshot.as_ref().unwrap().global_base
        )
        .unwrap()
    );
    let [fate] = core
        .ingest_commit_unit(tx, versions, u64::MAX - SKEW_TOLERANCE_MS)
        .unwrap()
        .try_into()
        .unwrap();
    let SyncMessage::FateUpdate { fate, .. } = fate else {
        panic!("expected fate update");
    };
    assert_eq!(fate, Fate::Rejected(RejectionReason::ExclusiveConflict));
}
#[test]
fn view_update_result_set_matches_groove_current_rows_for_seeded_commits() {
    let (_temp_dir, mut node) = open_node();
    let row_a = row(1);
    let row_b = row(2);

    for commit in [
        MergeableCommit::new("todos", row_a, 10).cells(title_cells("a1")),
        MergeableCommit::new("todos", row_b, 11).cells(title_cells("b1")),
        MergeableCommit::new("todos", row_a, 12).deletion(DeletionEvent::Deleted),
        MergeableCommit::new("todos", row_a, 13).cells(title_cells("a2")),
        MergeableCommit::new("todos", row_b, 14).deletion(DeletionEvent::Deleted),
    ] {
        let tx_id = node.commit_mergeable(commit).unwrap();
        node.apply_fate_update(
            tx_id,
            Fate::Accepted,
            Some(node.clock.next_global_seq),
            Some(DurabilityTier::Global),
        )
        .unwrap();
        assert_view_update_result_set_matches_current_rows(&mut node);
    }
}

#[test]
fn binding_delta_validates_shape_arity_binding_id_and_removes_result_set() {
    let (_temp_dir, mut node) = open_node();
    let shape = Query::from("todos")
        .filter(eq(col("title"), param("wanted")))
        .validate(&schema())
        .unwrap();
    let binding = shape
        .bind(BTreeMap::from([(
            "wanted".to_owned(),
            Value::String("match".to_owned()),
        )]))
        .unwrap();
    let values = vec![Value::String("match".to_owned())];
    let usage_binding_id = BindingId(uuid::Uuid::from_bytes([0x77; 16]));
    let usage_subscription = SubscriptionKey {
        shape_id: shape.shape_id(),
        binding_id: usage_binding_id,
        read_view: Default::default(),
    };
    let other_usage_binding_id = BindingId(uuid::Uuid::from_bytes([0x88; 16]));
    let other_usage_subscription = SubscriptionKey {
        shape_id: shape.shape_id(),
        binding_id: other_usage_binding_id,
        read_view: Default::default(),
    };

    node.apply_sync_message(SyncMessage::Subscribe(crate::protocol::Subscribe {
        shape_id: shape.shape_id(),
        subscription: usage_subscription,
        values: values.clone(),
        known_state: None,
    }))
    .unwrap();
    assert!(
        !node
            .query
            .registered_bindings
            .contains_key(&shape.shape_id())
    );

    node.apply_sync_message(SyncMessage::RegisterShape {
        shape_id: shape.shape_id(),
        ast: crate::protocol::ShapeAst::from_validated(&shape),
        opts: crate::protocol::RegisterShapeOptions::default(),
    })
    .unwrap();
    assert!(
        node.query
            .registered_bindings
            .get(&shape.shape_id())
            .unwrap()
            .contains_key(&usage_binding_id)
    );
    assert!(matches!(
        node.apply_sync_message(SyncMessage::Subscribe(crate::protocol::Subscribe {
            shape_id: shape.shape_id(),
            subscription: usage_subscription,
            values: Vec::new(),
            known_state: None,
        })),
        Err(Error::InvalidStoredValue("binding arity mismatch"))
    ));

    node.apply_sync_message(SyncMessage::Subscribe(crate::protocol::Subscribe {
        shape_id: shape.shape_id(),
        subscription: usage_subscription,
        values: values.clone(),
        known_state: None,
    }))
    .unwrap();
    node.apply_sync_message(SyncMessage::Subscribe(crate::protocol::Subscribe {
        shape_id: shape.shape_id(),
        subscription: other_usage_subscription,
        values,
        known_state: None,
    }))
    .unwrap();
    assert!(
        node.query.registered_bindings
            .get(&shape.shape_id())
            .unwrap()
            .contains_key(&usage_binding_id)
    );
    assert!(
        node.query.registered_bindings
            .get(&shape.shape_id())
            .unwrap()
            .contains_key(&other_usage_binding_id)
    );

    let canonical_subscription = SubscriptionKey {
        shape_id: shape.shape_id(),
        binding_id: binding.binding_id(),
        read_view: Default::default(),
    };
    let binding_view_key =
        crate::protocol::BindingViewKey::from_canonical_subscription_key(canonical_subscription);
    node.query.settled_result_sets.insert(
        binding_view_key,
        BTreeSet::new(),
    );
    node.query.settled_program_facts.insert(
        binding_view_key,
        BTreeSet::from([crate::protocol::ViewFactEntry::PathCorrelationCoverage(
            crate::protocol::PathCorrelationCoverageEntry {
                path: "owner".to_owned(),
                source_table: "todos".to_owned().into(),
                source_row: row(1),
                correlation_key: vec![1],
                complete: true,
            },
        )]),
    );
    node.apply_sync_message(SyncMessage::Unsubscribe {
        subscription: usage_subscription,
    })
    .unwrap();
    assert!(
        !node.query.registered_bindings
            .get(&shape.shape_id())
            .unwrap()
            .contains_key(&usage_binding_id)
    );
    assert!(
        node.query.registered_bindings
            .get(&shape.shape_id())
            .unwrap()
            .contains_key(&other_usage_binding_id)
    );
    assert!(node.query.settled_result_sets.contains_key(&binding_view_key));
    assert!(node.query.settled_program_facts.contains_key(&binding_view_key));

    node.apply_sync_message(SyncMessage::Unsubscribe {
        subscription: other_usage_subscription,
    })
    .unwrap();
    assert!(
        !node.query.registered_bindings
            .get(&shape.shape_id())
            .unwrap()
            .contains_key(&other_usage_binding_id)
    );
    assert!(!node.query.settled_result_sets.contains_key(&binding_view_key));
    assert!(!node.query.settled_program_facts.contains_key(&binding_view_key));
}

#[test]
fn binding_delta_cleanup_distinguishes_canonical_read_view() {
    let (_temp_dir, mut node) = open_node();
    let shape = Query::from("todos")
        .filter(eq(col("title"), param("wanted")))
        .validate(&schema())
        .unwrap();
    let binding = shape
        .bind(BTreeMap::from([(
            "wanted".to_owned(),
            Value::String("match".to_owned()),
        )]))
        .unwrap();
    let values = vec![Value::String("match".to_owned())];
    let branch_read_view = crate::protocol::ReadViewKey {
        id: uuid::Uuid::from_bytes([0x44; 16]),
    };
    let default_usage_subscription = SubscriptionKey {
        shape_id: shape.shape_id(),
        binding_id: BindingId(uuid::Uuid::from_bytes([0x77; 16])),
        read_view: Default::default(),
    };
    let branch_usage_subscription = SubscriptionKey {
        shape_id: shape.shape_id(),
        binding_id: BindingId(uuid::Uuid::from_bytes([0x88; 16])),
        read_view: branch_read_view,
    };

    node.apply_sync_message(SyncMessage::RegisterShape {
        shape_id: shape.shape_id(),
        ast: crate::protocol::ShapeAst::from_validated(&shape),
        opts: crate::protocol::RegisterShapeOptions::default(),
    })
    .unwrap();
    node.apply_sync_message(SyncMessage::Subscribe(crate::protocol::Subscribe {
        shape_id: shape.shape_id(),
        subscription: default_usage_subscription,
        values: values.clone(),
        known_state: None,
    }))
    .unwrap();
    node.apply_sync_message(SyncMessage::Subscribe(crate::protocol::Subscribe {
        shape_id: shape.shape_id(),
        subscription: branch_usage_subscription,
        values,
        known_state: None,
    }))
    .unwrap();

    let default_binding_view_key =
        crate::protocol::BindingViewKey::new(shape.shape_id(), binding.binding_id(), Default::default());
    let branch_binding_view_key =
        crate::protocol::BindingViewKey::new(shape.shape_id(), binding.binding_id(), branch_read_view);
    node.query
        .settled_result_sets
        .insert(default_binding_view_key, BTreeSet::new());
    node.query
        .settled_result_sets
        .insert(branch_binding_view_key, BTreeSet::new());

    // Internal sync-state coverage: public non-default read views fail closed,
    // so this future multi-view cleanup invariant is only observable below the
    // public facade for now.
    node.apply_sync_message(SyncMessage::Unsubscribe {
        subscription: default_usage_subscription,
    })
    .unwrap();
    assert!(
        !node
            .query
            .settled_result_sets
            .contains_key(&default_binding_view_key)
    );
    assert!(
        node.query
            .settled_result_sets
            .contains_key(&branch_binding_view_key)
    );

    node.apply_sync_message(SyncMessage::Unsubscribe {
        subscription: branch_usage_subscription,
    })
    .unwrap();
    assert!(
        !node
            .query
            .settled_result_sets
            .contains_key(&branch_binding_view_key)
    );
}

#[test]
fn prepared_query_lowering_supports_ne_parameter_predicates() {
    let (_temp_dir, mut node) = open_node();
    node.commit_mergeable(MergeableCommit::new("todos", row(0x31), 10).cells(title_cells("keep")))
        .unwrap();
    let shape = Query::from("todos")
        .filter(ne(col("title"), param("blocked")))
        .validate(&schema())
        .unwrap();
    let binding = shape
        .bind(BTreeMap::from([(
            "blocked".to_owned(),
            Value::String("drop".to_owned()),
        )]))
        .unwrap();

    let rows = node
        .query_rows(&shape, &binding, DurabilityTier::Local)
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].cell(&schema().tables[0], "title"), Some(v("keep")));
}

fn relation_snapshot_schema() -> JazzSchema {
    JazzSchema::new([
        TableSchema::new("users", [ColumnSchema::new("name", ColumnType::String)]),
        TableSchema::new(
            "todos",
            [
                ColumnSchema::new("title", ColumnType::String),
                ColumnSchema::new("owner_id", ColumnType::Uuid),
            ],
        )
        .with_reference("owner_id", "users"),
        TableSchema::new(
            "comments",
            [
                ColumnSchema::new("body", ColumnType::String),
                ColumnSchema::new("todo_id", ColumnType::Uuid),
            ],
        )
        .with_reference("todo_id", "todos"),
    ])
}

fn relation_snapshot_policy_schema() -> JazzSchema {
    JazzSchema::new([
        TableSchema::new("users", [ColumnSchema::new("name", ColumnType::String)]),
        TableSchema::new(
            "todos",
            [
                ColumnSchema::new("title", ColumnType::String),
                ColumnSchema::new("owner_id", ColumnType::Uuid),
            ],
        )
        .with_reference("owner_id", "users")
        .with_read_policy(Policy::owner_only("todos", "owner_id"))
        .with_write_policy(Policy::owner_only("todos", "owner_id")),
    ])
}

fn forward_include_schema() -> JazzSchema {
    JazzSchema::new([
        TableSchema::new(
            "profiles",
            [
                ColumnSchema::new("name", ColumnType::String),
                ColumnSchema::new("best_friend", ColumnType::Uuid.nullable()),
            ],
        )
        .with_reference("best_friend", "profiles"),
        TableSchema::new(
            "groups",
            [
                ColumnSchema::new("name", ColumnType::String),
                ColumnSchema::new("profile", ColumnType::Uuid.nullable()),
                ColumnSchema::new("members", ColumnType::Uuid.array_of()),
            ],
        )
        .with_reference("profile", "profiles")
        .with_reference("members", "profiles"),
    ])
}

#[test]
fn required_forward_include_allows_null_scalar_but_requires_every_array_member() {
    let schema = forward_include_schema();
    let (_temp_dir, mut node) = open_node_with_schema(node(0x80), schema.clone());
    let profile_a = row(0xa1);
    let profile_b = row(0xb1);
    let complete = row(0xc1);
    let partial = row(0xc2);
    let null_scalar = row(0xc3);

    node.commit_mergeable(
        MergeableCommit::new("profiles", profile_a, 10).cells(BTreeMap::from([(
            "name".to_owned(),
            v("a"),
        )])),
    )
    .unwrap();
    node.commit_mergeable(
        MergeableCommit::new("profiles", profile_b, 11).cells(BTreeMap::from([(
            "name".to_owned(),
            v("b"),
        ), (
            "best_friend".to_owned(),
            Value::Nullable(None),
        )])),
    )
    .unwrap();
    node.commit_mergeable(
        MergeableCommit::new("groups", complete, 12).cells(BTreeMap::from([
            ("name".to_owned(), v("complete")),
            ("profile".to_owned(), Value::Nullable(None)),
            (
                "members".to_owned(),
                Value::Array(vec![Value::Uuid(profile_a.0), Value::Uuid(profile_b.0)]),
            ),
        ])),
    )
    .unwrap();
    node.commit_mergeable(
        MergeableCommit::new("groups", partial, 13).cells(BTreeMap::from([
            ("name".to_owned(), v("partial")),
            ("profile".to_owned(), Value::Nullable(None)),
            (
                "members".to_owned(),
                Value::Array(vec![Value::Uuid(profile_a.0), Value::Uuid(row(0xff).0)]),
            ),
        ])),
    )
    .unwrap();
    node.commit_mergeable(
        MergeableCommit::new("groups", null_scalar, 14).cells(BTreeMap::from([
            ("name".to_owned(), v("null-scalar")),
            ("profile".to_owned(), Value::Nullable(None)),
            ("members".to_owned(), Value::Array(Vec::new())),
        ])),
    )
    .unwrap();

    let shape = Query::from("groups")
        .include_with(crate::query::Include::new("profile").require_includes())
        .include_with(crate::query::Include::new("members").require_includes())
        .validate(&schema)
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let rows = node
        .query_rows_for_link(&shape, &binding, DurabilityTier::Local, AuthorId::SYSTEM)
        .unwrap();

    assert_eq!(
        rows.iter().map(CurrentRow::row_uuid).collect::<BTreeSet<_>>(),
        BTreeSet::from([complete, null_scalar])
    );
}

#[test]
fn nested_required_include_checks_every_array_member_recursively() {
    let schema = forward_include_schema();
    let (_temp_dir, mut node) = open_node_with_schema(node(0x81), schema.clone());
    let profile_a = row(0xa1);
    let profile_b = row(0xb1);
    let complete = row(0xc1);
    let nested_partial = row(0xc2);

    node.commit_mergeable(
        MergeableCommit::new("profiles", profile_a, 10).cells(BTreeMap::from([
            ("name".to_owned(), v("a")),
            ("best_friend".to_owned(), Value::Nullable(None)),
        ])),
    )
    .unwrap();
    node.commit_mergeable(
        MergeableCommit::new("profiles", profile_b, 11).cells(BTreeMap::from([
            ("name".to_owned(), v("b")),
            (
                "best_friend".to_owned(),
                Value::Nullable(Some(Box::new(Value::Uuid(row(0xee).0)))),
            ),
        ])),
    )
    .unwrap();
    node.commit_mergeable(
        MergeableCommit::new("groups", complete, 12).cells(BTreeMap::from([
            ("name".to_owned(), v("complete")),
            ("profile".to_owned(), Value::Nullable(None)),
            ("members".to_owned(), Value::Array(vec![Value::Uuid(profile_a.0)])),
        ])),
    )
    .unwrap();
    node.commit_mergeable(
        MergeableCommit::new("groups", nested_partial, 13).cells(BTreeMap::from([
            ("name".to_owned(), v("nested-partial")),
            ("profile".to_owned(), Value::Nullable(None)),
            (
                "members".to_owned(),
                Value::Array(vec![Value::Uuid(profile_a.0), Value::Uuid(profile_b.0)]),
            ),
        ])),
    )
    .unwrap();

    let shape = Query::from("groups")
        .include_with(crate::query::Include::new("members.best_friend").require_includes())
        .validate(&schema)
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let rows = node
        .query_rows_for_link(&shape, &binding, DurabilityTier::Local, AuthorId::SYSTEM)
        .unwrap();

    assert_eq!(
        rows.iter().map(CurrentRow::row_uuid).collect::<BTreeSet<_>>(),
        BTreeSet::from([complete])
    );
}

#[test]
fn array_subquery_match_correlation_cardinality_requires_every_referenced_member() {
    let schema = forward_include_schema();
    let (_temp_dir, mut node) = open_node_with_schema(node(0x82), schema.clone());
    let profile_a = row(0xa1);
    let profile_b = row(0xb1);
    let complete = row(0xc1);
    let partial = row(0xc2);
    let empty = row(0xc3);

    for (idx, profile) in [profile_a, profile_b].into_iter().enumerate() {
        node.commit_mergeable(
            MergeableCommit::new("profiles", profile, 10 + idx as u64).cells(BTreeMap::from([
                ("name".to_owned(), v("profile")),
                ("best_friend".to_owned(), Value::Nullable(None)),
            ])),
        )
        .unwrap();
    }
    node.commit_mergeable(
        MergeableCommit::new("groups", complete, 12).cells(BTreeMap::from([
            ("name".to_owned(), v("complete")),
            ("profile".to_owned(), Value::Nullable(None)),
            (
                "members".to_owned(),
                Value::Array(vec![Value::Uuid(profile_a.0), Value::Uuid(profile_b.0)]),
            ),
        ])),
    )
    .unwrap();
    node.commit_mergeable(
        MergeableCommit::new("groups", partial, 13).cells(BTreeMap::from([
            ("name".to_owned(), v("partial")),
            ("profile".to_owned(), Value::Nullable(None)),
            (
                "members".to_owned(),
                Value::Array(vec![Value::Uuid(profile_a.0), Value::Uuid(row(0xee).0)]),
            ),
        ])),
    )
    .unwrap();
    node.commit_mergeable(
        MergeableCommit::new("groups", empty, 14).cells(BTreeMap::from([
            ("name".to_owned(), v("empty")),
            ("profile".to_owned(), Value::Nullable(None)),
            ("members".to_owned(), Value::Array(Vec::new())),
        ])),
    )
    .unwrap();

    let shape = Query::from("groups")
        .array_subquery(
            ArraySubquery::new("memberRows", "profiles", "id", "members")
                .requirement(crate::query::ArraySubqueryRequirement::MatchCorrelationCardinality),
        )
        .validate(&schema)
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let snapshot = node
        .query_relation_snapshot_for_link(&shape, &binding, DurabilityTier::Local, AuthorId::SYSTEM)
        .unwrap();

    assert_eq!(
        snapshot
            .rows
            .iter()
            .filter(|row| row.table() == "groups")
            .map(CurrentRow::row_uuid)
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([complete, empty])
    );
    assert_eq!(
        snapshot
            .edges
            .iter()
            .filter(|edge| edge.source_row == complete)
            .count(),
        2
    );
}

#[test]
fn rows_skipped_by_require_includes_affect_limit_offset_pagination() {
    let schema = forward_include_schema();
    let (_temp_dir, mut node) = open_node_with_schema(node(0x83), schema.clone());
    let profile_a = row(0xa1);
    let profile_b = row(0xb1);
    let partial_first = row(0xc1);
    let complete_first = row(0xc2);
    let partial_second = row(0xc3);
    let complete_second = row(0xc4);
    let complete_third = row(0xc5);

    for (idx, profile) in [profile_a, profile_b].into_iter().enumerate() {
        node.commit_mergeable(
            MergeableCommit::new("profiles", profile, 10 + idx as u64).cells(BTreeMap::from([
                ("name".to_owned(), v("profile")),
                ("best_friend".to_owned(), Value::Nullable(None)),
            ])),
        )
        .unwrap();
    }
    for (idx, (group, name, members)) in [
        (
            partial_first,
            "a-partial",
            vec![Value::Uuid(profile_a.0), Value::Uuid(row(0xea).0)],
        ),
        (complete_first, "b-complete", vec![Value::Uuid(profile_a.0)]),
        (
            partial_second,
            "c-partial",
            vec![Value::Uuid(profile_b.0), Value::Uuid(row(0xeb).0)],
        ),
        (complete_second, "d-complete", vec![Value::Uuid(profile_b.0)]),
        (complete_third, "e-complete", vec![Value::Uuid(profile_a.0)]),
    ]
    .into_iter()
    .enumerate()
    {
        node.commit_mergeable(
            MergeableCommit::new("groups", group, 20 + idx as u64).cells(BTreeMap::from([
                ("name".to_owned(), v(name)),
                ("profile".to_owned(), Value::Nullable(None)),
                ("members".to_owned(), Value::Array(members)),
            ])),
        )
        .unwrap();
    }

    let shape = Query::from("groups")
        .array_subquery(
            ArraySubquery::new("memberRows", "profiles", "id", "members")
                .requirement(crate::query::ArraySubqueryRequirement::MatchCorrelationCardinality),
        )
        .order_by("name", crate::query::OrderDirection::Asc)
        .offset(1)
        .limit(2)
        .validate(&schema)
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let snapshot = node
        .query_relation_snapshot_for_link(&shape, &binding, DurabilityTier::Local, AuthorId::SYSTEM)
        .unwrap();

    assert_eq!(
        snapshot
            .rows
            .iter()
            .filter(|row| row.table() == "groups")
            .map(CurrentRow::row_uuid)
            .collect::<Vec<_>>(),
        vec![complete_second, complete_third]
    );
}

#[test]
fn relation_snapshot_single_level_array_uses_query_engine_edges() {
    let schema = relation_snapshot_schema();
    let (_temp_dir, mut node) = open_node_with_schema(node(0x47), schema.clone());
    let alice = row(0xa1);
    let bob = row(0xb1);
    let todo_a = row(0x11);
    let todo_b = row(0x12);

    node.commit_mergeable(
        MergeableCommit::new("users", alice, 10).cells(BTreeMap::from([(
            "name".to_owned(),
            v("alice"),
        )])),
    )
    .unwrap();
    node.commit_mergeable(
        MergeableCommit::new("users", bob, 11).cells(BTreeMap::from([("name".to_owned(), v("bob"))])),
    )
    .unwrap();
    node.commit_mergeable(
        MergeableCommit::new("todos", todo_a, 12).cells(BTreeMap::from([
            ("title".to_owned(), v("alpha")),
            ("owner_id".to_owned(), Value::Uuid(alice.0)),
        ])),
    )
    .unwrap();
    node.commit_mergeable(
        MergeableCommit::new("todos", todo_b, 13).cells(BTreeMap::from([
            ("title".to_owned(), v("beta")),
            ("owner_id".to_owned(), Value::Uuid(bob.0)),
        ])),
    )
    .unwrap();

    let shape = Query::from("users")
        .filter(eq(col("id"), lit(Value::Uuid(alice.0))))
        .array_subquery(ArraySubquery::new("todosViaOwner", "todos", "owner_id", "id"))
        .validate(&schema)
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();

    let snapshot = node
        .query_relation_snapshot_for_link(&shape, &binding, DurabilityTier::Local, AuthorId::SYSTEM)
        .unwrap();

    assert_eq!(
        snapshot
            .rows
            .iter()
            .map(|row| (row.table().to_owned(), row.row_uuid()))
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([("users".to_owned(), alice), ("todos".to_owned(), todo_a)])
    );
    assert_eq!(
        snapshot.edges.into_iter().collect::<BTreeSet<_>>(),
        BTreeSet::from([RelationEdge {
            source_table: "users".to_owned(),
            source_row: alice,
            relation: "todosViaOwner".to_owned(),
            target_table: "todos".to_owned(),
            target_row: todo_a,
        }])
    );
}

#[test]
fn relation_snapshot_materializes_reverse_array_edges() {
    let schema = relation_snapshot_schema();
    let (_temp_dir, mut node) = open_node_with_schema(node(0x44), schema.clone());
    let alice = row(0xa1);
    let bob = row(0xb1);
    let todo_a = row(0x11);
    let todo_b = row(0x12);
    let comment = row(0xc1);

    node.commit_mergeable(
        MergeableCommit::new("users", alice, 10).cells(BTreeMap::from([(
            "name".to_owned(),
            v("alice"),
        )])),
    )
    .unwrap();
    node.commit_mergeable(
        MergeableCommit::new("users", bob, 11).cells(BTreeMap::from([(
            "name".to_owned(),
            v("bob"),
        )])),
    )
    .unwrap();
    node.commit_mergeable(
        MergeableCommit::new("todos", todo_a, 12).cells(BTreeMap::from([
            ("title".to_owned(), v("alpha")),
            ("owner_id".to_owned(), Value::Uuid(alice.0)),
        ])),
    )
    .unwrap();
    node.commit_mergeable(
        MergeableCommit::new("todos", todo_b, 13).cells(BTreeMap::from([
            ("title".to_owned(), v("beta")),
            ("owner_id".to_owned(), Value::Uuid(bob.0)),
        ])),
    )
    .unwrap();
    node.commit_mergeable(
        MergeableCommit::new("comments", comment, 14).cells(BTreeMap::from([
            ("body".to_owned(), v("nested")),
            ("todo_id".to_owned(), Value::Uuid(todo_a.0)),
        ])),
    )
    .unwrap();

    let shape = Query::from("users")
        .filter(eq(col("id"), lit(Value::Uuid(alice.0))))
        .array_subquery(
            ArraySubquery::new("todosViaOwner", "todos", "owner_id", "id")
                .nested(ArraySubquery::new("commentsViaTodo", "comments", "todo_id", "id")),
        )
        .validate(&schema)
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();

    let snapshot = node
        .query_relation_snapshot_for_link(&shape, &binding, DurabilityTier::Local, AuthorId::SYSTEM)
        .unwrap();

    assert_eq!(
        snapshot
            .rows
            .iter()
            .map(|row| (row.table().to_owned(), row.row_uuid()))
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([
            ("users".to_owned(), alice),
            ("todos".to_owned(), todo_a),
            ("comments".to_owned(), comment),
        ])
    );
    assert_eq!(
        snapshot.edges.into_iter().collect::<BTreeSet<_>>(),
        BTreeSet::from([
            RelationEdge {
                source_table: "users".to_owned(),
                source_row: alice,
                relation: "todosViaOwner".to_owned(),
                target_table: "todos".to_owned(),
                target_row: todo_a,
            },
            RelationEdge {
                source_table: "todos".to_owned(),
                source_row: todo_a,
                relation: "commentsViaTodo".to_owned(),
                target_table: "comments".to_owned(),
                target_row: comment,
            },
        ])
    );
}

#[test]
fn relation_snapshot_array_subquery_filters_use_parent_binding_params() {
    let schema = relation_snapshot_schema();
    let (_temp_dir, mut node) = open_node_with_schema(node(0x46), schema.clone());
    let alice = row(0xa1);
    let bob = row(0xb1);
    let matching_todo = row(0x11);
    let filtered_todo = row(0x12);

    node.commit_mergeable(
        MergeableCommit::new("users", alice, 10).cells(BTreeMap::from([(
            "name".to_owned(),
            v("alice"),
        )])),
    )
    .unwrap();
    node.commit_mergeable(
        MergeableCommit::new("users", bob, 11).cells(BTreeMap::from([(
            "name".to_owned(),
            v("bob"),
        )])),
    )
    .unwrap();
    node.commit_mergeable(
        MergeableCommit::new("todos", matching_todo, 12).cells(BTreeMap::from([
            ("title".to_owned(), v("keep")),
            ("owner_id".to_owned(), Value::Uuid(alice.0)),
        ])),
    )
    .unwrap();
    node.commit_mergeable(
        MergeableCommit::new("todos", filtered_todo, 13).cells(BTreeMap::from([
            ("title".to_owned(), v("drop")),
            ("owner_id".to_owned(), Value::Uuid(bob.0)),
        ])),
    )
    .unwrap();

    let shape = Query::from("users")
        .array_subquery(
            ArraySubquery::new("todosViaOwner", "todos", "owner_id", "id")
                .filter(eq(col("title"), param("wanted")))
                .requirement(crate::query::ArraySubqueryRequirement::AtLeastOne),
        )
        .validate(&schema)
        .unwrap();
    let binding = shape
        .bind(BTreeMap::from([(
            "wanted".to_owned(),
            Value::String("keep".to_owned()),
        )]))
        .unwrap();

    let snapshot = node
        .query_relation_snapshot_for_link(&shape, &binding, DurabilityTier::Local, AuthorId::SYSTEM)
        .unwrap();

    assert_eq!(
        snapshot
            .rows
            .iter()
            .map(|row| (row.table().to_owned(), row.row_uuid()))
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([
            ("users".to_owned(), alice),
            ("todos".to_owned(), matching_todo),
        ])
    );
    assert_eq!(
        snapshot.edges.into_iter().collect::<BTreeSet<_>>(),
        BTreeSet::from([RelationEdge {
            source_table: "users".to_owned(),
            source_row: alice,
            relation: "todosViaOwner".to_owned(),
            target_table: "todos".to_owned(),
            target_row: matching_todo,
        }])
    );
}

#[test]
fn relation_snapshot_filters_unreadable_children_and_required_parents() {
    let schema = relation_snapshot_policy_schema();
    let (_temp_dir, mut node) = open_node_with_schema(node(0x45), schema.clone());
    let parent = row(0xa1);
    let child = row(0x11);
    let alice = user(0xa1);
    let bob = user(0xb1);

    node.commit_mergeable(
        MergeableCommit::new("users", parent, 10).cells(BTreeMap::from([(
            "name".to_owned(),
            v("parent"),
        )])),
    )
    .unwrap();
    node.commit_mergeable(
        MergeableCommit::new("todos", child, 11).cells(BTreeMap::from([
            ("title".to_owned(), v("hidden")),
            ("owner_id".to_owned(), Value::Uuid(alice.0)),
        ])),
    )
    .unwrap();

    let optional_shape = Query::from("users")
        .array_subquery(ArraySubquery::new("todosViaOwner", "todos", "owner_id", "id"))
        .validate(&schema)
        .unwrap();
    let optional_binding = optional_shape.bind(BTreeMap::new()).unwrap();

    let optional = node
        .query_relation_snapshot_for_link(
            &optional_shape,
            &optional_binding,
            DurabilityTier::Local,
            bob,
        )
        .unwrap();
    assert_eq!(
        optional
            .rows
            .iter()
            .map(|row| (row.table().to_owned(), row.row_uuid()))
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([("users".to_owned(), parent)])
    );
    assert!(optional.edges.is_empty());

    let required_shape = Query::from("users")
        .array_subquery(
            ArraySubquery::new("todosViaOwner", "todos", "owner_id", "id")
                .requirement(crate::query::ArraySubqueryRequirement::AtLeastOne),
        )
        .validate(&schema)
        .unwrap();
    let required_binding = required_shape.bind(BTreeMap::new()).unwrap();

    let required = node
        .query_relation_snapshot_for_link(
            &required_shape,
            &required_binding,
            DurabilityTier::Local,
            bob,
        )
        .unwrap();
    assert!(required.rows.is_empty());
    assert!(required.edges.is_empty());
}

#[test]
fn include_deleted_one_shot_read_uses_lowered_literal_filters() {
    let (_temp_dir, mut node) = open_node();
    let table = schema().tables[0].clone();
    node.commit_mergeable(MergeableCommit::new("todos", row(0x41), 10).cells(title_cells("keep")))
        .unwrap();
    node.commit_mergeable(
        MergeableCommit::new("todos", row(0x42), 11).cells(title_cells("keep")),
    )
    .unwrap();
    node.commit_mergeable(MergeableCommit::new("todos", row(0x42), 12).deletion(DeletionEvent::Deleted))
        .unwrap();
    node.commit_mergeable(MergeableCommit::new("todos", row(0x43), 13).cells(title_cells("drop")))
        .unwrap();
    node.commit_mergeable(MergeableCommit::new("todos", row(0x43), 14).deletion(DeletionEvent::Deleted))
        .unwrap();
    let shape = Query::from("todos")
        .filter(eq(col("title"), lit("keep")))
        .validate(&schema())
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();

    let rows = node
        .query_rows_including_deleted_for_identity(
            &shape,
            &binding,
            DurabilityTier::Local,
            None,
            AuthorId::SYSTEM,
        )
        .unwrap();

    assert_eq!(
        rows.iter()
            .map(|row| (row.row_uuid(), row.is_deleted(), row.cell(&table, "title")))
            .collect::<Vec<_>>(),
        vec![
            (row(0x41), false, Some(v("keep"))),
            (row(0x42), true, Some(v("keep"))),
        ]
    );
}

#[test]
fn include_deleted_one_shot_read_uses_lowered_param_filters() {
    let (_temp_dir, mut node) = open_node();
    node.commit_mergeable(MergeableCommit::new("todos", row(0x51), 10).cells(title_cells("match")))
        .unwrap();
    node.commit_mergeable(
        MergeableCommit::new("todos", row(0x51), 11).deletion(DeletionEvent::Deleted),
    )
    .unwrap();
    node.commit_mergeable(MergeableCommit::new("todos", row(0x52), 12).cells(title_cells("miss")))
        .unwrap();
    let shape = Query::from("todos")
        .filter(eq(col("title"), param("wanted")))
        .validate(&schema())
        .unwrap();
    let binding = shape
        .bind(BTreeMap::from([(
            "wanted".to_owned(),
            Value::String("match".to_owned()),
        )]))
        .unwrap();

    let rows = node
        .query_rows_including_deleted_for_identity(
            &shape,
            &binding,
            DurabilityTier::Local,
            None,
            AuthorId::SYSTEM,
        )
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].row_uuid(), row(0x51));
    assert!(rows[0].is_deleted());
}

fn include_deleted_join_schema() -> JazzSchema {
    JazzSchema::new([
        TableSchema::new("issues", [ColumnSchema::new("title", ColumnType::String)]),
        TableSchema::new(
            "issue_tags",
            [
                ColumnSchema::new("issue", ColumnType::Uuid),
                ColumnSchema::new("tag", ColumnType::String),
            ],
        )
        .with_reference("issue", "issues"),
    ])
}

#[test]
fn include_deleted_one_shot_read_join_matches_visible_join_rows() {
    let schema = include_deleted_join_schema();
    let (_temp_dir, mut node) = open_node_with_schema(node(9), schema.clone());
    let issue = row(0x61);
    node.commit_mergeable(
        MergeableCommit::new("issues", issue, 10).cells(BTreeMap::from([(
            "title".to_owned(),
            Value::String("deleted but matched".to_owned()),
        )])),
    )
    .unwrap();
    node.commit_mergeable(MergeableCommit::new("issues", issue, 11).deletion(DeletionEvent::Deleted))
        .unwrap();
    node.commit_mergeable(
        MergeableCommit::new("issue_tags", row(0x62), 12).cells(BTreeMap::from([
            ("issue".to_owned(), Value::Uuid(issue.0)),
            ("tag".to_owned(), Value::String("bug".to_owned())),
        ])),
    )
    .unwrap();
    let shape = Query::from("issues")
        .join_via("issue_tags", "issue", [eq(col("tag"), lit("bug"))])
        .validate(&schema)
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();

    let rows = node
        .query_rows_including_deleted_for_identity(
            &shape,
            &binding,
            DurabilityTier::Local,
            None,
            AuthorId::SYSTEM,
        )
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].row_uuid(), issue);
    assert!(rows[0].is_deleted());
}

#[test]
fn include_deleted_one_shot_read_join_ignores_deleted_join_rows() {
    let schema = include_deleted_join_schema();
    let (_temp_dir, mut node) = open_node_with_schema(node(9), schema.clone());
    let issue = row(0x63);
    let tag_row = row(0x64);
    node.commit_mergeable(
        MergeableCommit::new("issues", issue, 10).cells(BTreeMap::from([(
            "title".to_owned(),
            Value::String("deleted root".to_owned()),
        )])),
    )
    .unwrap();
    node.commit_mergeable(MergeableCommit::new("issues", issue, 11).deletion(DeletionEvent::Deleted))
        .unwrap();
    node.commit_mergeable(
        MergeableCommit::new("issue_tags", tag_row, 12).cells(BTreeMap::from([
            ("issue".to_owned(), Value::Uuid(issue.0)),
            ("tag".to_owned(), Value::String("bug".to_owned())),
        ])),
    )
    .unwrap();
    node.commit_mergeable(
        MergeableCommit::new("issue_tags", tag_row, 13).deletion(DeletionEvent::Deleted),
    )
    .unwrap();
    let shape = Query::from("issues")
        .join_via("issue_tags", "issue", [eq(col("tag"), lit("bug"))])
        .validate(&schema)
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();

    let rows = node
        .query_rows_including_deleted_for_identity(
            &shape,
            &binding,
            DurabilityTier::Local,
            None,
            AuthorId::SYSTEM,
        )
        .unwrap();

    assert!(rows.is_empty());
}

fn include_deleted_reachable_schema() -> JazzSchema {
    JazzSchema::new([
        TableSchema::new("teams", [ColumnSchema::new("name", ColumnType::String)]),
        TableSchema::new("docs", [ColumnSchema::new("title", ColumnType::String)]),
        TableSchema::new(
            "team_access",
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

fn include_deleted_reachable_shape(schema: &JazzSchema) -> ValidatedQuery {
    Query::from("docs")
        .reachable_via(
            "team_access",
            "doc",
            "team",
            lit(Value::Uuid(row(0x72).0)),
            "team_edges",
            "member",
            "parent",
            [],
        )
        .validate(&schema)
        .unwrap()
}

#[test]
fn include_deleted_one_shot_read_reachable_matches_deleted_roots_through_visible_edges() {
    let schema = include_deleted_reachable_schema();
    let (_temp_dir, mut node) = open_node_with_schema(node(9), schema.clone());
    let doc = row(0x73);
    node.commit_mergeable(
        MergeableCommit::new("teams", row(0x71), 10).cells(BTreeMap::from([(
            "name".to_owned(),
            Value::String("parent".to_owned()),
        )])),
    )
    .unwrap();
    node.commit_mergeable(
        MergeableCommit::new("teams", row(0x72), 11).cells(BTreeMap::from([(
            "name".to_owned(),
            Value::String("member".to_owned()),
        )])),
    )
    .unwrap();
    node.commit_mergeable(
        MergeableCommit::new("docs", doc, 12).cells(BTreeMap::from([(
            "title".to_owned(),
            Value::String("deleted reachable".to_owned()),
        )])),
    )
    .unwrap();
    node.commit_mergeable(MergeableCommit::new("docs", doc, 13).deletion(DeletionEvent::Deleted))
        .unwrap();
    node.commit_mergeable(
        MergeableCommit::new("team_edges", row(0x74), 14).cells(BTreeMap::from([
            ("member".to_owned(), Value::Uuid(row(0x72).0)),
            ("parent".to_owned(), Value::Uuid(row(0x71).0)),
        ])),
    )
    .unwrap();
    node.commit_mergeable(
        MergeableCommit::new("team_access", row(0x75), 15).cells(BTreeMap::from([
            ("doc".to_owned(), Value::Uuid(doc.0)),
            ("team".to_owned(), Value::Uuid(row(0x71).0)),
        ])),
    )
    .unwrap();
    let shape = include_deleted_reachable_shape(&schema);
    let binding = shape.bind(BTreeMap::new()).unwrap();

    let rows = node
        .query_rows_including_deleted_for_identity(
            &shape,
            &binding,
            DurabilityTier::Local,
            None,
            AuthorId::SYSTEM,
        )
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].row_uuid(), doc);
    assert!(rows[0].is_deleted());
}

#[test]
fn include_deleted_one_shot_read_reachable_ignores_deleted_edge_rows() {
    let schema = include_deleted_reachable_schema();
    let (_temp_dir, mut node) = open_node_with_schema(node(9), schema.clone());
    let doc = row(0x76);
    let edge = row(0x77);
    node.commit_mergeable(
        MergeableCommit::new("teams", row(0x71), 10).cells(BTreeMap::from([(
            "name".to_owned(),
            Value::String("parent".to_owned()),
        )])),
    )
    .unwrap();
    node.commit_mergeable(
        MergeableCommit::new("teams", row(0x72), 11).cells(BTreeMap::from([(
            "name".to_owned(),
            Value::String("member".to_owned()),
        )])),
    )
    .unwrap();
    node.commit_mergeable(
        MergeableCommit::new("docs", doc, 12).cells(BTreeMap::from([(
            "title".to_owned(),
            Value::String("deleted but not reached".to_owned()),
        )])),
    )
    .unwrap();
    node.commit_mergeable(MergeableCommit::new("docs", doc, 13).deletion(DeletionEvent::Deleted))
        .unwrap();
    node.commit_mergeable(
        MergeableCommit::new("team_edges", edge, 14).cells(BTreeMap::from([
            ("member".to_owned(), Value::Uuid(row(0x72).0)),
            ("parent".to_owned(), Value::Uuid(row(0x71).0)),
        ])),
    )
    .unwrap();
    node.commit_mergeable(MergeableCommit::new("team_edges", edge, 15).deletion(DeletionEvent::Deleted))
        .unwrap();
    node.commit_mergeable(
        MergeableCommit::new("team_access", row(0x78), 16).cells(BTreeMap::from([
            ("doc".to_owned(), Value::Uuid(doc.0)),
            ("team".to_owned(), Value::Uuid(row(0x71).0)),
        ])),
    )
    .unwrap();
    let shape = include_deleted_reachable_shape(&schema);
    let binding = shape.bind(BTreeMap::new()).unwrap();

    let rows = node
        .query_rows_including_deleted_for_identity(
            &shape,
            &binding,
            DurabilityTier::Local,
            None,
            AuthorId::SYSTEM,
        )
        .unwrap();

    assert!(rows.is_empty());
}

#[test]
fn node_finishes_aggregation_ordering_pagination_and_projection_after_materialization() {
    let (_temp_dir, mut node) = open_node_with_schema(node(9), two_column_schema());
    for (idx, title, body) in [
        (1, "gamma", "keep"),
        (2, "alpha", "drop"),
        (3, "beta", "keep"),
    ] {
        node.commit_mergeable(
            MergeableCommit::new("todos", row(idx), 10 + idx as u64).cells(BTreeMap::from([
                ("title".to_owned(), Value::String(title.to_owned())),
                ("body".to_owned(), Value::String(body.to_owned())),
            ])),
        )
        .unwrap();
    }

    let shape = Query::from("todos")
        .filter(eq(col("body"), lit("keep")))
        .select(["title"])
        .order_by("title", crate::query::OrderDirection::Asc)
        .offset(1)
        .limit(1)
        .validate(&two_column_schema())
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let rows = node
        .query_rows(&shape, &binding, DurabilityTier::Local)
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].cell(&two_column_schema().tables[0], "title"),
        Some(v("gamma"))
    );
    assert_eq!(
        rows[0].cell(&two_column_schema().tables[0], "body"),
        None,
        "projection must be applied after filtering/ordering/pagination"
    );

    let count_shape = Query::from("todos")
        .filter(eq(col("body"), lit("keep")))
        .count()
        .validate(&two_column_schema())
        .unwrap();
    let count_rows = node
        .query_rows(
            &count_shape,
            &count_shape.bind(BTreeMap::new()).unwrap(),
            DurabilityTier::Local,
        )
        .unwrap();
    assert_eq!(
        count_rows[0].test_cells_by_descriptor()["count"],
        Value::U64(2)
    );
}

#[test]
fn query_payload_dedup_is_per_peer_across_subscriptions() {
    let (_writer_dir, mut writer) = open_node_with_uuid(node(1));
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    let row = row(7);
    let _tx_id = commit_mergeable_global(
        &mut writer,
        &mut core,
        MergeableCommit::new("todos", row, 10).cells(title_cells("match")),
    );
    let all_shape = Query::from("todos").validate(&schema()).unwrap();
    let all_binding = all_shape.bind(BTreeMap::new()).unwrap();
    let filtered_shape = Query::from("todos")
        .filter(eq(col("title"), lit("match")))
        .validate(&schema())
        .unwrap();
    let filtered_binding = filtered_shape.bind(BTreeMap::new()).unwrap();
    let mut peer = PeerState::new();

    let first = peer
        .rehydrate_query(&mut core, &all_shape, &all_binding)
        .unwrap();
    let SyncMessage::ViewUpdate {
        version_bundles,
        peer_payload_inventory: crate::protocol::PeerPayloadInventory { complete_tx_payloads: complete_tx_payload_refs },
        ..
    } = first
    else {
        panic!("expected first view update");
    };
    assert_eq!(version_bundles.len(), 1);
    assert!(complete_tx_payload_refs.is_empty());
    assert!(peer.shipped_complete_tx_payloads().is_empty());

    let second = peer
        .rehydrate_query(&mut core, &filtered_shape, &filtered_binding)
        .unwrap();
    let SyncMessage::ViewUpdate {
        version_bundles,
        peer_payload_inventory: crate::protocol::PeerPayloadInventory { complete_tx_payloads: complete_tx_payload_refs },
        ..
    } = second
    else {
        panic!("expected second view update");
    };
    assert_eq!(version_bundles.len(), 1);
    assert!(complete_tx_payload_refs.is_empty());
}

#[test]
fn partial_mergeable_payload_does_not_establish_tx_level_complete_tx_ref() {
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    let first_row = row(7);
    let second_row = row(8);
    let tx_id = core
        .commit_mergeable_many(vec![
            MergeableCommit::new("todos", first_row, 10).cells(title_cells("first")),
            MergeableCommit::new("todos", second_row, 10).cells(title_cells("second")),
        ])
        .unwrap();
    core.apply_fate_update(
        tx_id,
        Fate::Accepted,
        Some(GlobalSeq(1)),
        Some(DurabilityTier::Global),
    )
    .unwrap();

    let first_shape = Query::from("todos")
        .filter(eq(col("title"), lit("first")))
        .validate(&schema())
        .unwrap();
    let first_binding = first_shape.bind(BTreeMap::new()).unwrap();
    let second_shape = Query::from("todos")
        .filter(eq(col("title"), lit("second")))
        .validate(&schema())
        .unwrap();
    let second_binding = second_shape.bind(BTreeMap::new()).unwrap();
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
        panic!("expected first view update");
    };
    assert_eq!(version_bundles.len(), 1);
    assert_eq!(version_bundles[0].versions.len(), 1);
    assert!(complete_tx_payload_refs.is_empty());
    assert!(!peer.shipped_complete_tx_payloads().contains(&tx_id));

    let second = peer
        .rehydrate_query(&mut core, &second_shape, &second_binding)
        .unwrap();
    let SyncMessage::ViewUpdate {
        version_bundles,
        peer_payload_inventory: crate::protocol::PeerPayloadInventory { complete_tx_payloads: complete_tx_payload_refs },
        ..
    } = second
    else {
        panic!("expected second view update");
    };
    assert_eq!(version_bundles.len(), 1);
    assert_eq!(version_bundles[0].versions.len(), 1);
    assert!(complete_tx_payload_refs.is_empty());
}

#[test]
fn db_facade_current_rows_match_seeded_create_delete_sequence() {
    let db =
        crate::db::doctest_support::block_on(crate::db::doctest_support::open_todos_db()).unwrap();
    let query = db.table("todos");
    let prepared = db.prepare_query(&query).unwrap();
    let table = &crate::db::doctest_support::schema().tables[0];

    let write = db
        .insert("todos", crate::db::doctest_support::todo_cells("a1", false))
        .unwrap();
    let row_a = write.row_uuid();
    crate::db::doctest_support::block_on(write.wait(DurabilityTier::Local)).unwrap();
    assert_eq!(db_facade_row_ids(&db.read(&prepared).unwrap()), vec![row_a]);
    assert_eq!(
        db.one(&prepared).unwrap().unwrap().cell(table, "title"),
        Some(Value::String("a1".to_owned()))
    );

    let write = db
        .insert("todos", crate::db::doctest_support::todo_cells("b1", false))
        .unwrap();
    let row_b = write.row_uuid();
    crate::db::doctest_support::block_on(write.wait(DurabilityTier::Local)).unwrap();
    assert_eq!(
        db_facade_row_ids(
            &crate::db::doctest_support::block_on(
                db.all(&prepared, crate::db::ReadOpts::default())
            )
                .unwrap()
        ),
        vec![row_a, row_b]
    );

    crate::db::doctest_support::block_on(
        db.delete("todos", row_a)
            .unwrap()
            .wait(DurabilityTier::Local),
    )
    .unwrap();
    assert_eq!(db_facade_row_ids(&db.read(&prepared).unwrap()), vec![row_b]);

    crate::db::doctest_support::block_on(
        db.restore(
            "todos",
            row_a,
            crate::db::doctest_support::todo_cells("a2", true),
        )
        .unwrap()
        .wait(DurabilityTier::Local),
    )
    .unwrap();
    let rows = db.read(&prepared).unwrap();
    assert_eq!(db_facade_row_ids(&rows), vec![row_a, row_b]);
    assert_eq!(
        rows.iter()
            .find(|row| row.row_uuid() == row_a)
            .unwrap()
            .cell(table, "title"),
        Some(Value::String("a2".to_owned()))
    );

    crate::db::doctest_support::block_on(
        db.delete("todos", row_b)
            .unwrap()
            .wait(DurabilityTier::Local),
    )
    .unwrap();
    assert_eq!(
        db_facade_row_ids(
            &crate::db::doctest_support::block_on(
                db.all(&prepared, crate::db::ReadOpts::default())
            )
                .unwrap()
        ),
        vec![row_a]
    );
}

#[test]
fn db_facade_multi_row_query_matches_seeded_create_delete_sequence_via_write_handles() {
    let db =
        crate::db::doctest_support::block_on(crate::db::doctest_support::open_todos_db()).unwrap();
    let query = db.table("todos");
    let prepared = db.prepare_query(&query).unwrap();
    let table = &crate::db::doctest_support::schema().tables[0];

    let write = db
        .insert("todos", crate::db::doctest_support::todo_cells("a1", false))
        .unwrap();
    let row_a = write.row_uuid();
    crate::db::doctest_support::block_on(write.wait(DurabilityTier::Local)).unwrap();
    let rows = db.read(&prepared).unwrap();
    assert_eq!(db_facade_row_ids(&rows), vec![row_a]);
    assert_eq!(
        db.one(&prepared).unwrap().unwrap().cell(table, "title"),
        Some(Value::String("a1".to_owned()))
    );

    let write = db
        .insert("todos", crate::db::doctest_support::todo_cells("b1", false))
        .unwrap();
    let row_b = write.row_uuid();
    crate::db::doctest_support::block_on(write.wait(DurabilityTier::Local)).unwrap();
    let rows =
        crate::db::doctest_support::block_on(db.all(&prepared, crate::db::ReadOpts::default()))
            .unwrap();
    assert_eq!(db_facade_row_ids(&rows), vec![row_a, row_b]);

    let write = db.delete("todos", row_a).unwrap();
    crate::db::doctest_support::block_on(write.wait(DurabilityTier::Local)).unwrap();
    let rows = db.read(&prepared).unwrap();
    assert_eq!(db_facade_row_ids(&rows), vec![row_b]);
    assert_eq!(
        db.one(&prepared).unwrap().unwrap().cell(table, "title"),
        Some(Value::String("b1".to_owned()))
    );

    let write = db
        .restore(
            "todos",
            row_a,
            crate::db::doctest_support::todo_cells("a2", true),
        )
        .unwrap();
    crate::db::doctest_support::block_on(write.wait(DurabilityTier::Local)).unwrap();
    let rows = db.read(&prepared).unwrap();
    assert_eq!(db_facade_row_ids(&rows), vec![row_a, row_b]);
    assert_eq!(
        rows.iter()
            .find(|row| row.row_uuid() == row_a)
            .unwrap()
            .cell(table, "title"),
        Some(Value::String("a2".to_owned()))
    );
    assert_eq!(
        rows.iter()
            .find(|row| row.row_uuid() == row_a)
            .unwrap()
            .cell(table, "done"),
        Some(Value::Bool(true))
    );

    let write = db.delete("todos", row_b).unwrap();
    crate::db::doctest_support::block_on(write.wait(DurabilityTier::Local)).unwrap();
    let rows =
        crate::db::doctest_support::block_on(db.all(&prepared, crate::db::ReadOpts::default()))
            .unwrap();
    assert_eq!(db_facade_row_ids(&rows), vec![row_a]);
}

fn db_facade_row_ids(rows: &[CurrentRow]) -> Vec<RowUuid> {
    rows.iter().map(CurrentRow::row_uuid).collect()
}
