// M3 maintained-vs-one-shot differential oracle.
//
// The matrix below is deliberately review-visible. A row missing from the
// matrix is a known coverage gap, not implicit confidence.
//
// | Atom / shape family                         | Covered by shape(s)                         |
// |---------------------------------------------|---------------------------------------------|
// | plain table                                 | `docs_plain`                                |
// | filtered                                    | `docs_filtered`                             |
// | claim/provenance-scoped `$createdBy`         | `docs_created_by`                           |
// | claim/provenance-scoped `$createdAt`         | `docs_created_at`                           |
// | seeded reachable closure, edge-table seed    | `docs_edge_seeded_reachable`                |
// | seeded reachable closure, same-table UUID seed | `resources_same_table_seeded_reachable`   |
// | seeded reachable closure, same-table string seed | `string_resources_same_table_seeded_reachable` |
// | inherits, 1-level                            | `children_inherit_doc`                      |
// | inherits, 2-level                            | `grandchildren_inherit_child`               |
// | relation traversal facade, forward hop       | `docs_relation_facade_direct_access`        |
// | recursive membership                         | both reachable shapes include transitive hops |
// | aggregate                                    | `docs_count`                                |

#[derive(Clone)]
struct DifferentialShape {
    name: &'static str,
    shape: ValidatedQuery,
    binding: Binding,
    identity: AuthorId,
    subscription: SubscriptionKey,
}

struct DifferentialOracle {
    peers: Vec<PeerState>,
    shapes: Vec<DifferentialShape>,
    rows: Vec<BTreeSet<(String, RowUuid)>>,
    aggregate: AggregateDifferential,
}

struct AggregateDifferential {
    name: &'static str,
    shape: ValidatedQuery,
    binding: Binding,
    identity: AuthorId,
    subscription: SubscriptionKey,
    peer: PeerState,
    value: Value,
}

impl DifferentialOracle {
    fn open(
        core: &mut NodeState<RocksDbStorage>,
        schema: &JazzSchema,
        shapes: Vec<DifferentialShape>,
        seed: u64,
    ) -> Self {
        let mut peers = Vec::new();
        let mut rows = Vec::new();
        for shape in &shapes {
            let mut peer = PeerState::for_author(shape.identity);
            let update = peer
                .rehydrate_query(core, &shape.shape, &shape.binding)
                .unwrap_or_else(|err| {
                    panic!(
                        "seed {seed}: initial maintained open failed for {}: {err:?}",
                        shape.name
                    )
                });
            let mut shape_rows = BTreeSet::new();
            apply_result_members(&mut shape_rows, &update, &shape.shape.query().table);
            rows.push(shape_rows);
            peers.push(peer);
        }
        let aggregate_shape = Query::from("docs").count().validate(schema).unwrap();
        let aggregate_binding = aggregate_shape.bind(BTreeMap::new()).unwrap();
        let aggregate_subscription = SubscriptionKey {
            shape_id: aggregate_shape.shape_id(),
            binding_id: aggregate_binding.binding_id(),
            read_view: Default::default(),
        };
        let mut aggregate_peer = PeerState::for_author(user(0xa1));
        let aggregate_initial = aggregate_peer
            .rehydrate_query(core, &aggregate_shape, &aggregate_binding)
            .unwrap_or_else(|err| {
                panic!("seed {seed}: initial maintained open failed for docs_count: {err:?}")
            });
        let mut aggregate_value = Value::U64(0);
        apply_aggregate_payload(&mut aggregate_value, &aggregate_initial);

        let mut oracle = Self {
            peers,
            shapes,
            rows,
            aggregate: AggregateDifferential {
                name: "docs_count",
                shape: aggregate_shape,
                binding: aggregate_binding,
                identity: user(0xa1),
                subscription: aggregate_subscription,
                peer: aggregate_peer,
                value: aggregate_value,
            },
        };
        oracle.assert_checkpoint(core, seed, "t0");
        oracle
    }

    fn tick_and_assert(&mut self, core: &mut NodeState<RocksDbStorage>, seed: u64, checkpoint: &str) {
        for ((peer, shape), rows) in self
            .peers
            .iter_mut()
            .zip(self.shapes.iter())
            .zip(self.rows.iter_mut())
        {
            let update = peer
                .query_update_for_subscription(core, shape.subscription, &shape.shape, &shape.binding)
                .unwrap_or_else(|err| {
                    panic!(
                        "seed {seed}: maintained update failed for {} at {checkpoint}: {err:?}",
                        shape.name
                    )
                });
            apply_result_members(rows, &update, &shape.shape.query().table);
        }
        let aggregate_update = self
            .aggregate
            .peer
            .query_update_for_subscription(
                core,
                self.aggregate.subscription,
                &self.aggregate.shape,
                &self.aggregate.binding,
            )
            .unwrap_or_else(|err| {
                panic!(
                    "seed {seed}: maintained update failed for {} at {checkpoint}: {err:?}",
                    self.aggregate.name
                )
            });
        apply_aggregate_payload(&mut self.aggregate.value, &aggregate_update);
        self.assert_checkpoint(core, seed, checkpoint);
    }

    fn assert_checkpoint(&mut self, core: &mut NodeState<RocksDbStorage>, seed: u64, checkpoint: &str) {
        for (maintained, shape) in self.rows.iter().zip(self.shapes.iter()) {
            let one_shot = one_shot_rows(core, &shape.shape, &shape.binding, shape.identity);
            assert_eq!(
                maintained, &one_shot,
                "seed {seed}: maintained/one-shot divergence for {} at {checkpoint}",
                shape.name
            );
        }
        let one_shot = one_shot_aggregate_value(
            core,
            &self.aggregate.shape,
            &self.aggregate.binding,
            self.aggregate.identity,
        );
        assert_eq!(
            self.aggregate.value, one_shot,
            "seed {seed}: maintained/one-shot aggregate divergence for {} at {checkpoint}",
            self.aggregate.name
        );
    }
}

fn m3_differential_seeds() -> Vec<u64> {
    if let Ok(seed) = std::env::var("JAZZ_SEED") {
        return vec![seed.parse::<u64>().expect("JAZZ_SEED must be a u64")];
    }
    const FIXED_SEEDS: [u64; 5] = [11, 29, 47, 4372288, 7_777_013];
    let extra = std::env::var("JAZZ_SEED_COUNT")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(0)
        .saturating_sub(FIXED_SEEDS.len() as u64);
    FIXED_SEEDS
        .into_iter()
        .chain((0..extra).map(|i| 9_000 + i * 7919))
        .collect()
}

#[test]
fn m3_maintained_one_shot_differential_oracle() {
    for seed in m3_differential_seeds() {
        if let Err(payload) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            run_m3_differential_seed(seed)
        })) {
            eprintln!("M3 DIFFERENTIAL SEED FAILED: {seed}");
            std::panic::resume_unwind(payload);
        }
    }
}

fn run_m3_differential_seed(seed: u64) {
    let schema = m3_differential_schema();
    let (_core_dir, mut core) = open_node_with_schema(node(0x71), schema.clone());
    seed_m3_differential_base(&mut core, seed);
    let mut differential = DifferentialOracle::open(&mut core, &schema, m3_differential_shapes(&schema), seed);

    let mut rng = Lcg::new(seed ^ 0x9e37_79b9);
    let mut parents = m3_differential_parent_map(&mut core);
    for step in 0..m3_differential_step_count() {
        match rng.choose(7) {
            0 => add_visible_doc(&mut core, &mut parents, step),
            1 => add_hidden_doc(&mut core, &mut parents, step),
            2 => revoke_edge_access(&mut core, &mut parents, step),
            3 => grant_edge_access(&mut core, &mut parents, step),
            4 => delete_visible_child(&mut core, &mut parents, step),
            5 => restore_visible_child(&mut core, &mut parents, step),
            _ => update_created_at_match(&mut core, &mut parents, step),
        }
        differential.tick_and_assert(&mut core, seed, &format!("fuzz-step-{step}"));
    }
}

fn m3_differential_step_count() -> u64 {
    std::env::var("JAZZ_DIFFERENTIAL_STEP_COUNT")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(3)
}

#[test]
fn m3_differential_empty_seed_then_insert_created_by() {
    let schema = m3_differential_schema();
    let (_core_dir, mut core) = open_node_with_schema(node(0x72), schema.clone());
    let shape = created_by_shape(&schema);
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let identity = user(0xa1);
    let subscription = SubscriptionKey {
        shape_id: shape.shape_id(),
        binding_id: binding.binding_id(),
        read_view: Default::default(),
    };
    let mut oracle = DifferentialOracle::open(
        &mut core,
        &schema,
        vec![DifferentialShape {
            name: "empty_seed_then_insert_created_by",
            shape,
            binding,
            identity,
            subscription,
        }],
        0,
    );

    accept_global(
        &mut core,
        MergeableCommit::new("docs", row(0xa1), 10).cells(differential_doc_cells(
            "later",
            "match",
            identity,
            7,
        )),
    );
    oracle.tick_and_assert(&mut core, 0, "after-created-by-insert");
}

#[test]
fn m3_differential_remote_genuinely_empty_reset_erases() {
    let schema = m3_differential_schema();
    let (_core_dir, mut core) = open_node_with_schema(node(0x73), schema.clone());
    seed_m3_differential_base(&mut core, 0);
    let mut reader = open_node_with_schema(node(0x74), schema.clone()).1;
    let shape = created_by_shape(&schema);
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let identity = user(0xa1);
    let mut peer = PeerState::for_author(identity);

    let initial = peer.rehydrate_query(&mut core, &shape, &binding).unwrap();
    let mut maintained_rows = BTreeSet::new();
    apply_result_members(&mut maintained_rows, &initial, shape.query().table.as_str());
    reader.apply_sync_message(initial).unwrap();
    assert!(!maintained_rows.is_empty());

    for row_uuid in [row(0x11), row(0x12), row(0x14)] {
        if let Some(parent) = latest_tx_for_row(&mut core, "docs", row_uuid) {
            accept_global(
                &mut core,
                MergeableCommit::new("docs", row_uuid, 100 + row_uuid.0.as_bytes()[0] as u64)
                    .parents(vec![parent])
                    .deletion(DeletionEvent::Deleted),
            );
        }
    }
    let reset = peer.rehydrate_query(&mut core, &shape, &binding).unwrap();
    apply_result_members(&mut maintained_rows, &reset, shape.query().table.as_str());
    reader.apply_sync_message(reset).unwrap();
    assert!(
        one_shot_rows(&mut core, &shape, &binding, identity).is_empty(),
        "fixture must make the serving node one-shot result genuinely empty"
    );
    assert!(
        maintained_rows.is_empty(),
        "remote genuine empty reset must erase maintained subscription state"
    );
}

#[test]
fn m3_differential_revoke_mid_stream_and_reconnect_mid_stream() {
    let schema = m3_differential_schema();
    let (core_dir, mut core) = open_node_with_schema(node(0x75), schema.clone());
    seed_m3_differential_base(&mut core, 0);
    let mut oracle = DifferentialOracle::open(&mut core, &schema, m3_differential_shapes(&schema), 0);
    let mut parents = m3_differential_parent_map(&mut core);

    revoke_edge_access(&mut core, &mut parents, 0);
    oracle.tick_and_assert(&mut core, 0, "after-revoke-mid-stream");

    drop(core);
    core = reopen_node_at(&core_dir, node(0x75), schema);
    grant_edge_access(&mut core, &mut parents, 1);
    oracle.tick_and_assert(&mut core, 0, "after-reconnect-mid-stream");
}

fn m3_differential_schema() -> JazzSchema {
    let same_table_policy = Query::from("resources")
        .reachable_via_with_access_filters(
            "resource_access",
            "resource",
            "team",
            lit("same-table-seed"),
            [eq(col("administrator"), lit(false))],
            "team_edges",
            "member",
            "parent",
            [eq(col("administrator"), lit(false))],
        )
        .seeded_by("teams", "identity_key", "sub", "id");
    let string_same_table_policy = Query::from("string_resources")
        .reachable_via_with_access_filters(
            "string_resource_access",
            "resource",
            "team",
            lit("same-table-string-seed"),
            [eq(col("administrator"), lit(false))],
            "team_edges",
            "member",
            "parent",
            [eq(col("administrator"), lit(false))],
        )
        .seeded_by("teams", "identity_key_text", "sub", "id");

    JazzSchema::new([
        TableSchema::new(
            "docs",
            [
                ColumnSchema::new("title", ColumnType::String),
                ColumnSchema::new("kind", ColumnType::String),
                ColumnSchema::new("createdBy", ColumnType::Uuid),
                ColumnSchema::new("createdAt", ColumnType::U64),
            ],
        )
        .with_read_policy(Policy::public())
        .with_write_policy(Policy::public()),
        TableSchema::new(
            "children",
            [
                ColumnSchema::new("doc", ColumnType::Uuid),
                ColumnSchema::new("status", ColumnType::String),
            ],
        )
        .with_reference("doc", "docs")
        .with_read_policy(Policy::public())
        .with_write_policy(Policy::public()),
        TableSchema::new("grandchildren", [ColumnSchema::new("child", ColumnType::Uuid)])
            .with_reference("child", "children")
            .with_read_policy(Policy::public())
            .with_write_policy(Policy::public()),
        TableSchema::new(
            "teams",
            [
                ColumnSchema::new("id", ColumnType::Uuid),
                ColumnSchema::new("name", ColumnType::String),
                ColumnSchema::new("identity_key", ColumnType::Uuid),
                ColumnSchema::new("identity_key_text", ColumnType::String),
            ],
        )
        .with_read_policy(Policy::public())
        .with_write_policy(Policy::public()),
        TableSchema::new(
            "team_edges",
            [
                ColumnSchema::new("member", ColumnType::Uuid),
                ColumnSchema::new("parent", ColumnType::Uuid),
                ColumnSchema::new("administrator", ColumnType::Bool),
            ],
        )
        .with_reference("member", "teams")
        .with_reference("parent", "teams")
        .with_read_policy(Policy::public())
        .with_write_policy(Policy::public()),
        TableSchema::new(
            "group_access_edges",
            [
                ColumnSchema::new("user_id", ColumnType::Uuid),
                ColumnSchema::new("group_id", ColumnType::Uuid),
            ],
        )
        .with_reference("group_id", "teams")
        .with_read_policy(Policy::public())
        .with_write_policy(Policy::public()),
        TableSchema::new(
            "doc_access",
            [
                ColumnSchema::new("doc", ColumnType::Uuid),
                ColumnSchema::new("team", ColumnType::Uuid),
            ],
        )
        .with_reference("doc", "docs")
        .with_reference("team", "teams")
        .with_read_policy(Policy::public())
        .with_write_policy(Policy::public()),
        TableSchema::new("resources", [ColumnSchema::new("label", ColumnType::String)])
            .with_read_policy(Policy::shape(same_table_policy))
            .with_write_policy(Policy::public()),
        TableSchema::new(
            "string_resources",
            [ColumnSchema::new("label", ColumnType::String)],
        )
        .with_read_policy(Policy::shape(string_same_table_policy))
        .with_write_policy(Policy::public()),
        TableSchema::new(
            "resource_access",
            [
                ColumnSchema::new("resource", ColumnType::Uuid),
                ColumnSchema::new("team", ColumnType::Uuid),
                ColumnSchema::new("administrator", ColumnType::Bool),
            ],
        )
        .with_reference("resource", "resources")
        .with_reference("team", "teams")
        .with_read_policy(Policy::public())
        .with_write_policy(Policy::public()),
        TableSchema::new(
            "string_resource_access",
            [
                ColumnSchema::new("resource", ColumnType::Uuid),
                ColumnSchema::new("team", ColumnType::Uuid),
                ColumnSchema::new("administrator", ColumnType::Bool),
            ],
        )
        .with_reference("resource", "string_resources")
        .with_reference("team", "teams")
        .with_read_policy(Policy::public())
        .with_write_policy(Policy::public()),
    ])
}

fn m3_differential_shapes(schema: &JazzSchema) -> Vec<DifferentialShape> {
    let identity = user(0xa1);
    let mut specs = Vec::new();
    let mut push = |name: &'static str, shape: ValidatedQuery| {
        let binding = shape.bind(BTreeMap::new()).unwrap();
        let subscription = SubscriptionKey {
            shape_id: shape.shape_id(),
            binding_id: binding.binding_id(),
            read_view: Default::default(),
        };
        specs.push(DifferentialShape {
            name,
            shape,
            binding,
            identity,
            subscription,
        });
    };
    push("docs_plain", Query::from("docs").validate(schema).unwrap());
    push(
        "docs_filtered",
        Query::from("docs")
            .filter(eq(col("kind"), lit("match")))
            .validate(schema)
            .unwrap(),
    );
    push("docs_created_by", created_by_shape(schema));
    push(
        "docs_created_at",
        Query::from("docs")
            .filter(eq(col("createdAt"), lit(7_u64)))
            .validate(schema)
            .unwrap(),
    );
    push(
        "docs_edge_seeded_reachable",
        Query::from("docs")
            .reachable_via(
                "doc_access",
                "doc",
                "team",
                lit("edge-seed"),
                "team_edges",
                "member",
                "parent",
                [],
            )
            .seeded_by("group_access_edges", "user_id", "sub", "group_id")
            .validate(schema)
            .unwrap(),
    );
    push(
        "resources_same_table_seeded_reachable",
        Query::from("resources").validate(schema).unwrap(),
    );
    push(
        "string_resources_same_table_seeded_reachable",
        Query::from("string_resources").validate(schema).unwrap(),
    );
    push(
        "children_inherit_doc",
        Query::from("children").inherits("doc").validate(schema).unwrap(),
    );
    push(
        "grandchildren_inherit_child",
        Query::from("grandchildren")
            .inherits("child")
            .validate(schema)
            .unwrap(),
    );
    push(
        "docs_relation_facade_direct_access",
        relation_doc_access_shape().validate(schema).unwrap(),
    );
    specs
}

fn created_by_shape(schema: &JazzSchema) -> ValidatedQuery {
    Query::from("docs")
        .filter(eq(col("createdBy"), claim("sub")))
        .validate(schema)
        .unwrap()
}

fn relation_doc_access_shape() -> Query {
    crate::query::relation_query_to_query(&RelationQuery {
        rel: RelationExpr::Project {
            input: Box::new(RelationExpr::Join {
                left: Box::new(RelationExpr::TableScan {
                    table: "docs".to_owned(),
                    alias: None,
                }),
                right: Box::new(RelationExpr::TableScan {
                    table: "doc_access".to_owned(),
                    alias: Some("__hop_0".to_owned()),
                }),
                on: vec![RelationJoinCondition {
                    left: RelationColumnRef {
                        scope: Some("docs".to_owned()),
                        column: "id".to_owned(),
                    },
                    right: RelationColumnRef {
                        scope: Some("__hop_0".to_owned()),
                        column: "doc".to_owned(),
                    },
                }],
                join_kind: RelationJoinKind::Inner,
            }),
            columns: vec![
                RelationProjectColumn {
                    alias: "id".to_owned(),
                    expr: RelationProjectExpr::Column(RelationColumnRef {
                        scope: Some("docs".to_owned()),
                        column: "id".to_owned(),
                    }),
                },
                RelationProjectColumn {
                    alias: "title".to_owned(),
                    expr: RelationProjectExpr::Column(RelationColumnRef {
                        scope: Some("docs".to_owned()),
                        column: "title".to_owned(),
                    }),
                },
            ],
        },
    })
    .expect("single-hop relation facade should normalize")
}

fn seed_m3_differential_base(core: &mut NodeState<RocksDbStorage>, seed: u64) {
    let alice = user(0xa1);
    let bob = user(0xb2);
    for (team, name, identity) in [
        (row(0x31), "alice-direct", alice),
        (row(0x32), "alice-parent", bob),
        (row(0x33), "bob", bob),
    ] {
        accept_global(
            core,
            MergeableCommit::new("teams", team, 1).cells(BTreeMap::from([
                ("id".to_owned(), Value::Uuid(team.0)),
                ("name".to_owned(), Value::String(name.to_owned())),
                ("identity_key".to_owned(), Value::Uuid(identity.0)),
                (
                    "identity_key_text".to_owned(),
                    Value::String(identity.0.to_string()),
                ),
            ])),
        );
    }
    accept_global(core, team_edge_commit(row(0x41), row(0x31), row(0x32), false, 2));
    accept_global(
        core,
        MergeableCommit::new("group_access_edges", row(0x42), 3).cells(BTreeMap::from([
            ("user_id".to_owned(), Value::Uuid(alice.0)),
            ("group_id".to_owned(), Value::Uuid(row(0x31).0)),
        ])),
    );

    for (doc, title, kind, author, created_at) in [
        (row(0x11), "visible-direct", "match", alice, 7),
        (row(0x12), "visible-transitive", "match", alice, 7),
        (row(0x13), "hidden", "match", bob, 8),
        (row(0x14), "filtered-out", "other", alice, 9),
    ] {
        accept_global(
            core,
            MergeableCommit::new("docs", doc, 10 + seed % 3).cells(differential_doc_cells(
                title, kind, author, created_at,
            )),
        );
    }
    for (edge, doc, team) in [
        (row(0x51), row(0x11), row(0x31)),
        (row(0x52), row(0x12), row(0x32)),
        (row(0x53), row(0x13), row(0x33)),
    ] {
        accept_global(
            core,
            MergeableCommit::new("doc_access", edge, 20).cells(BTreeMap::from([
                ("doc".to_owned(), Value::Uuid(doc.0)),
                ("team".to_owned(), Value::Uuid(team.0)),
            ])),
        );
    }
    for (resource, label) in [(row(0x61), "direct"), (row(0x62), "transitive"), (row(0x63), "hidden")] {
        accept_global(
            core,
            MergeableCommit::new("resources", resource, 30)
                .cells(BTreeMap::from([("label".to_owned(), Value::String(label.to_owned()))])),
        );
    }
    for (edge, resource, team) in [
        (row(0x64), row(0x61), row(0x31)),
        (row(0x65), row(0x62), row(0x32)),
        (row(0x66), row(0x63), row(0x33)),
    ] {
        accept_global(
            core,
            MergeableCommit::new("resource_access", edge, 31).cells(BTreeMap::from([
                ("resource".to_owned(), Value::Uuid(resource.0)),
                ("team".to_owned(), Value::Uuid(team.0)),
                ("administrator".to_owned(), Value::Bool(false)),
            ])),
        );
    }
    for (resource, label) in [
        (row(0x67), "string-direct"),
        (row(0x68), "string-transitive"),
        (row(0x69), "string-hidden"),
    ] {
        accept_global(
            core,
            MergeableCommit::new("string_resources", resource, 32)
                .cells(BTreeMap::from([("label".to_owned(), Value::String(label.to_owned()))])),
        );
    }
    for (edge, resource, team) in [
        (row(0x6a), row(0x67), row(0x31)),
        (row(0x6b), row(0x68), row(0x32)),
        (row(0x6c), row(0x69), row(0x33)),
    ] {
        accept_global(
            core,
            MergeableCommit::new("string_resource_access", edge, 33).cells(BTreeMap::from([
                ("resource".to_owned(), Value::Uuid(resource.0)),
                ("team".to_owned(), Value::Uuid(team.0)),
                ("administrator".to_owned(), Value::Bool(false)),
            ])),
        );
    }
    accept_global(
        core,
        MergeableCommit::new("children", row(0x71), 40).cells(BTreeMap::from([
            ("doc".to_owned(), Value::Uuid(row(0x11).0)),
            ("status".to_owned(), Value::String("open".to_owned())),
        ])),
    );
    accept_global(
        core,
        MergeableCommit::new("grandchildren", row(0x81), 41)
            .cells(BTreeMap::from([("child".to_owned(), Value::Uuid(row(0x71).0))])),
    );
}

fn m3_differential_parent_map(core: &mut NodeState<RocksDbStorage>) -> BTreeMap<(&'static str, RowUuid), TxId> {
    let mut parents = BTreeMap::new();
    for table in [
        "docs",
        "children",
        "grandchildren",
        "team_edges",
        "group_access_edges",
        "doc_access",
    ] {
        for row in core.current_rows(table, DurabilityTier::Global).unwrap() {
            if let Some(tx) = latest_tx_for_row(core, table, row.row_uuid()) {
                parents.insert((table, row.row_uuid()), tx);
            }
        }
    }
    parents
}

fn differential_doc_cells(title: &str, kind: &str, author: AuthorId, created_at: u64) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("title".to_owned(), Value::String(title.to_owned())),
        ("kind".to_owned(), Value::String(kind.to_owned())),
        ("createdBy".to_owned(), Value::Uuid(author.0)),
        ("createdAt".to_owned(), Value::U64(created_at)),
    ])
}

fn team_edge_commit(
    row_uuid: RowUuid,
    member: RowUuid,
    parent: RowUuid,
    administrator: bool,
    made_at: u64,
) -> MergeableCommit {
    MergeableCommit::new("team_edges", row_uuid, made_at).cells(BTreeMap::from([
        ("member".to_owned(), Value::Uuid(member.0)),
        ("parent".to_owned(), Value::Uuid(parent.0)),
        ("administrator".to_owned(), Value::Bool(administrator)),
    ]))
}

fn accept_with_parent(
    core: &mut NodeState<RocksDbStorage>,
    parents: &mut BTreeMap<(&'static str, RowUuid), TxId>,
    table: &'static str,
    row_uuid: RowUuid,
    made_at: u64,
    cells: BTreeMap<String, Value>,
) -> TxId {
    let mut commit = MergeableCommit::new(table, row_uuid, made_at).cells(cells);
    if let Some(parent) = parents.get(&(table, row_uuid)).copied() {
        commit = commit.parents(vec![parent]);
    }
    let tx = accept_global(core, commit);
    parents.insert((table, row_uuid), tx);
    tx
}

fn delete_with_parent(
    core: &mut NodeState<RocksDbStorage>,
    parents: &mut BTreeMap<(&'static str, RowUuid), TxId>,
    table: &'static str,
    row_uuid: RowUuid,
    made_at: u64,
) -> TxId {
    let parent = parents[&(table, row_uuid)];
    let tx = accept_global(
        core,
        MergeableCommit::new(table, row_uuid, made_at)
            .parents(vec![parent])
            .deletion(DeletionEvent::Deleted),
    );
    parents.insert((table, row_uuid), tx);
    tx
}

fn add_visible_doc(
    core: &mut NodeState<RocksDbStorage>,
    parents: &mut BTreeMap<(&'static str, RowUuid), TxId>,
    step: u64,
) {
    let row_uuid = row(0x90 + step as u8);
    accept_with_parent(
        core,
        parents,
        "docs",
        row_uuid,
        100 + step,
        differential_doc_cells("added", "match", user(0xa1), 7),
    );
    accept_with_parent(
        core,
        parents,
        "doc_access",
        row(0xa0 + step as u8),
        120 + step,
        BTreeMap::from([
            ("doc".to_owned(), Value::Uuid(row_uuid.0)),
            ("team".to_owned(), Value::Uuid(row(0x31).0)),
        ]),
    );
}

fn add_hidden_doc(
    core: &mut NodeState<RocksDbStorage>,
    parents: &mut BTreeMap<(&'static str, RowUuid), TxId>,
    step: u64,
) {
    accept_with_parent(
        core,
        parents,
        "docs",
        row(0xb0 + step as u8),
        140 + step,
        differential_doc_cells("hidden-added", "match", user(0xb2), 8),
    );
}

fn revoke_edge_access(
    core: &mut NodeState<RocksDbStorage>,
    parents: &mut BTreeMap<(&'static str, RowUuid), TxId>,
    step: u64,
) {
    delete_with_parent(core, parents, "group_access_edges", row(0x42), 160 + step);
}

fn grant_edge_access(
    core: &mut NodeState<RocksDbStorage>,
    parents: &mut BTreeMap<(&'static str, RowUuid), TxId>,
    step: u64,
) {
    accept_with_parent(
        core,
        parents,
        "group_access_edges",
        row(0x42),
        180 + step,
        BTreeMap::from([
            ("user_id".to_owned(), Value::Uuid(user(0xa1).0)),
            ("group_id".to_owned(), Value::Uuid(row(0x31).0)),
        ]),
    );
}

fn delete_visible_child(
    core: &mut NodeState<RocksDbStorage>,
    parents: &mut BTreeMap<(&'static str, RowUuid), TxId>,
    step: u64,
) {
    delete_with_parent(core, parents, "children", row(0x71), 200 + step);
}

fn restore_visible_child(
    core: &mut NodeState<RocksDbStorage>,
    parents: &mut BTreeMap<(&'static str, RowUuid), TxId>,
    step: u64,
) {
    accept_with_parent(
        core,
        parents,
        "children",
        row(0x71),
        220 + step,
        BTreeMap::from([
            ("doc".to_owned(), Value::Uuid(row(0x11).0)),
            ("status".to_owned(), Value::String("open".to_owned())),
        ]),
    );
}

fn update_created_at_match(
    core: &mut NodeState<RocksDbStorage>,
    parents: &mut BTreeMap<(&'static str, RowUuid), TxId>,
    step: u64,
) {
    accept_with_parent(
        core,
        parents,
        "docs",
        row(0x14),
        240 + step,
        differential_doc_cells("filtered-now-created-at", "match", user(0xa1), 7),
    );
}

fn latest_tx_for_row(core: &mut NodeState<RocksDbStorage>, table: &str, row_uuid: RowUuid) -> Option<TxId> {
    core.row_history(table, row_uuid)
        .unwrap()
        .last()
        .map(|row| row.tx_id())
}

fn one_shot_rows(
    core: &mut NodeState<RocksDbStorage>,
    shape: &ValidatedQuery,
    binding: &Binding,
    identity: AuthorId,
) -> BTreeSet<(String, RowUuid)> {
    core.query_rows_for_link(shape, binding, DurabilityTier::Global, identity)
        .unwrap()
        .into_iter()
        .map(|row| (row.table().to_owned(), row.row_uuid()))
        .collect()
}

fn apply_result_members(rows: &mut BTreeSet<(String, RowUuid)>, update: &SyncMessage, root_table: &str) {
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
        rows.clear();
    }
    for entry in result_member_removes {
        if let Some((table, row_uuid, _tx_id)) = entry.as_row()
            && table.as_str() == root_table
        {
            rows.remove(&(table.to_string(), row_uuid));
        }
    }
    for entry in result_member_adds {
        if let Some((table, row_uuid, _tx_id)) = entry.as_row()
            && table.as_str() == root_table
        {
            rows.insert((table.to_string(), row_uuid));
        }
    }
}

fn apply_aggregate_payload(value: &mut Value, update: &SyncMessage) {
    let SyncMessage::ViewUpdate {
        reset_result_set,
        program_fact_adds,
        ..
    } = update
    else {
        panic!("expected view update");
    };
    if *reset_result_set {
        *value = Value::U64(0);
    }
    for fact in program_fact_adds {
        if let Some(next) = aggregate_payload_count(fact) {
            *value = next;
        }
    }
}

fn aggregate_payload_count(fact: &crate::protocol::ProgramFactEntry) -> Option<Value> {
    let crate::protocol::ProgramFactEntry::ResultPayload(payload) = fact else {
        return None;
    };
    let table = payload.member.table_name()?;
    if table != "docs_aggregate" {
        return None;
    }
    let fields: Vec<(Option<String>, groove::records::ValueType)> =
        postcard::from_bytes(&payload.descriptor).unwrap();
    let descriptor = groove::records::RecordDescriptor::new(
        fields
            .into_iter()
            .map(|(name, value_type)| (name.unwrap(), value_type)),
    );
    let record = groove::records::BorrowedRecord::new(&payload.record, &descriptor);
    Some(record.get("count").unwrap().clone())
}

fn one_shot_aggregate_value(
    core: &mut NodeState<RocksDbStorage>,
    shape: &ValidatedQuery,
    binding: &Binding,
    identity: AuthorId,
) -> Value {
    let rows = core
        .query_rows_for_link(shape, binding, DurabilityTier::Global, identity)
        .unwrap();
    if rows.is_empty() {
        return Value::U64(0);
    }
    assert_eq!(rows.len(), 1, "aggregate one-shot should produce at most one synthetic row");
    rows[0].test_cells_by_descriptor()["count"].clone()
}
