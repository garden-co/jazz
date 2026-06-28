#[test]
fn predicate_boundary_probe_matches_set_comparison_reference() {
    let (_client_dir, mut client) = open_node_with_uuid(node(8));
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    let mut parents = BTreeMap::<RowUuid, TxId>::new();

    for step in 0..48_u64 {
        let row_uuid = row((step % 8) as u8);
        let mut commit = MergeableCommit::new("todos", row_uuid, 10 + step);
        if let Some(parent) = parents.get(&row_uuid).copied() {
            commit = commit.parents(vec![parent]);
        }
        commit = if step >= 8 && step % 11 == 5 {
            commit.deletion(DeletionEvent::Deleted)
        } else if step >= 8 && step % 11 == 6 {
            commit.deletion(DeletionEvent::Restored)
        } else {
            commit.cells(BTreeMap::from([(
                "title".to_owned(),
                format!("title-{step}"),
            )]))
        };
        let (tx_id, unit) = client.commit_mergeable_unit(commit).unwrap();
        parents.insert(row_uuid, tx_id);
        let SyncMessage::CommitUnit { tx, versions } = unit else {
            panic!("expected commit unit");
        };
        let mut updates = core
            .ingest_commit_unit(tx, versions, u64::MAX - SKEW_TOLERANCE_MS)
            .unwrap();
        assert_eq!(updates.len(), 1);
        let Some(SyncMessage::FateUpdate {
            fate: Fate::Accepted,
            global_seq: Some(global_seq),
            ..
        }) = updates.pop()
        else {
            panic!("expected one accepted fate");
        };
        for base in 0..=global_seq.0 {
            let base = GlobalSeq(base);
            let versions = core.query_table_versions("todos").unwrap();
            let reference = global_visible_currency_set_at(&mut core, &versions, base)
                != global_visible_currency_set_now(&mut core, &versions);
            let probe = core.global_currency_changed_after("todos", base).unwrap();
            assert_eq!(probe, reference, "step {step}, base {base:?}");
        }
    }
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

    node.apply_sync_message(SyncMessage::BindingDelta(crate::protocol::BindingDelta {
        shape_id: shape.shape_id(),
        adds: vec![(binding.binding_id(), values.clone())],
        removes: Vec::new(),
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
            .contains_key(&binding.binding_id())
    );
    assert!(matches!(
        node.apply_sync_message(SyncMessage::BindingDelta(crate::protocol::BindingDelta {
            shape_id: shape.shape_id(),
            adds: vec![(binding.binding_id(), Vec::new())],
            removes: Vec::new(),
        })),
        Err(Error::InvalidStoredValue("binding arity mismatch"))
    ));
    assert!(matches!(
        node.apply_sync_message(SyncMessage::BindingDelta(crate::protocol::BindingDelta {
            shape_id: shape.shape_id(),
            adds: vec![(BindingId(uuid::Uuid::from_bytes([9; 16])), values.clone())],
            removes: Vec::new(),
        })),
        Err(Error::InvalidStoredValue(
            "binding id does not match values"
        ))
    ));

    node.apply_sync_message(SyncMessage::BindingDelta(crate::protocol::BindingDelta {
        shape_id: shape.shape_id(),
        adds: vec![(binding.binding_id(), values)],
        removes: Vec::new(),
    }))
    .unwrap();
    assert!(
        node.query.registered_bindings
            .get(&shape.shape_id())
            .unwrap()
            .contains_key(&binding.binding_id())
    );

    let subscription = SubscriptionKey {
        shape_id: shape.shape_id(),
        binding_id: binding.binding_id(),
    };
    node.query.settled_result_sets
        .insert(subscription, BTreeSet::new());
    node.apply_sync_message(SyncMessage::BindingDelta(crate::protocol::BindingDelta {
        shape_id: shape.shape_id(),
        adds: Vec::new(),
        removes: vec![binding.binding_id()],
    }))
    .unwrap();
    assert!(
        !node.query.registered_bindings
            .get(&shape.shape_id())
            .unwrap()
            .contains_key(&binding.binding_id())
    );
    assert!(!node.query.settled_result_sets.contains_key(&subscription));
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
    let schema = JazzSchema::new([
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
    ]);
    schema
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
