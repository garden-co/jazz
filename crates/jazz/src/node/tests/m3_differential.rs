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
// | seeded reachable closure, same-table canonical subject seed | `resources_same_table_seeded_reachable`   |
// | seeded reachable closure, same-table string seed | `string_resources_same_table_seeded_reachable` |
// | inherits, 1-level                            | `children_inherit_doc`                      |
// | inherits, 2-level                            | `grandchildren_inherit_child`               |
// | projection with includes                     | `docs_projected_with_doc_access`            |
// | relation traversal facade, forward hop       | `docs_relation_facade_direct_access`        |
// | recursive membership                         | both reachable shapes include transitive hops |
// | aggregate: count, min, max, sum, avg          | `docs_*_by_bucket`                          |
// | aggregate numeric value types                 | `F64`, `I64`, `U64`                         |
// | aggregate nullable / empty inputs              | `m3_aggregate_null_semantics`                |
// | aggregate cancellation and sustained churn     | `m3_aggregate_churn_curve`                   |

#[derive(Clone)]
struct DifferentialShape {
    name: &'static str,
    shape: ValidatedQuery,
    binding: Binding,
    identity: AuthorSubject,
    subscription: SubscriptionKey,
}

struct DifferentialOracle {
    peers: Vec<PeerState>,
    receivers: Vec<NodeState<MemoryStorage>>,
    shapes: Vec<DifferentialShape>,
    rows: Vec<BTreeSet<(String, RowUuid)>>,
    aggregates: Vec<AggregateDifferential>,
}

struct AggregateDifferential {
    name: &'static str,
    shape: ValidatedQuery,
    binding: Binding,
    identity: AuthorSubject,
    subscription: SubscriptionKey,
    peer: PeerState,
    receiver: NodeState<MemoryStorage>,
    output: &'static str,
    agreement: AggregateAgreement,
    values: BTreeMap<u64, Value>,
    maintenance_updates: u64,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum AggregateAgreement {
    Exact,
    /// Floating-point addition/subtraction is order-sensitive.  The oracle
    /// uses a forward-error budget relative to Σ|x|, with one rounding budget
    /// for each input and each subsequent maintained update.
    F64Approx,
}

const F64_AGGREGATE_ERROR_FACTOR: f64 = 64.0;
type AggregateSpec = (
    &'static str,
    crate::query::Aggregate,
    &'static str,
    AggregateAgreement,
);

impl DifferentialOracle {
    fn open<S: OrderedKvStorage>(
        core: &mut NodeState<S>,
        schema: &JazzSchema,
        shapes: Vec<DifferentialShape>,
        seed: u64,
    ) -> Self {
        Self::open_with_aggregate_specs(core, schema, shapes, aggregate_differential_specs(), seed)
    }

    fn open_with_aggregate_specs<S: OrderedKvStorage>(
        core: &mut NodeState<S>,
        schema: &JazzSchema,
        shapes: Vec<DifferentialShape>,
        aggregate_specs: Vec<AggregateSpec>,
        seed: u64,
    ) -> Self {
        let mut peers = Vec::new();
        let mut receivers = Vec::new();
        let mut rows = Vec::new();
        for (receiver_offset, shape) in shapes.iter().enumerate() {
            let mut peer = PeerState::client_link(shape.identity);
            let update = peer
                .rehydrate_query(core, &shape.shape, &shape.binding)
                .unwrap_or_else(|err| {
                    panic!(
                        "seed {seed}: initial maintained open failed for {}: {err:?}",
                        shape.name
                    )
                });
            let mut receiver = maintained_receiver(schema, 0xb0 + receiver_offset as u8);
            register_maintained_receiver(&mut receiver, &shape.shape, &shape.binding, shape.identity);
            receiver.apply_sync_message_settled(update).unwrap_or_else(|err| {
                panic!(
                    "seed {seed}: receiver rejected initial source closure for {}: {err:?}",
                    shape.name
                )
            });
            let shape_rows = m3_receiver_rows(
                &mut receiver,
                &shape.shape,
                &shape.binding,
                shape.identity,
            );
            rows.push(shape_rows);
            peers.push(peer);
            receivers.push(receiver);
        }
        let mut aggregates = Vec::new();
        for (receiver_offset, (name, aggregate, output, agreement)) in
            aggregate_specs.into_iter().enumerate()
        {
            let shape = Query::from("docs")
                .aggregate([aggregate])
                .group_by("bucket")
                .validate(schema)
                .unwrap();
            let binding = shape.bind(BTreeMap::new()).unwrap();
            let subscription = SubscriptionKey {
                shape_id: shape.shape_id(),
                binding_id: binding.binding_id(),
                read_view: Default::default(),
            };
            let mut peer = PeerState::client_link(user(0xa1));
            let initial = peer
                .rehydrate_query(core, &shape, &binding)
                .unwrap_or_else(|err| {
                    panic!("seed {seed}: initial maintained open failed for {name}: {err:?}")
                });
            let identity = user(0xa1);
            let mut receiver = maintained_receiver(schema, 0xc0 + receiver_offset as u8);
            register_maintained_receiver(&mut receiver, &shape, &binding, identity);
            receiver
                .apply_sync_message_settled(initial)
                .unwrap_or_else(|err| {
                    panic!("seed {seed}: receiver rejected initial source closure for {name}: {err:?}")
                });
            let values = receiver_aggregate_values(&mut receiver, &shape, &binding, identity, output);
            aggregates.push(AggregateDifferential {
                name,
                shape,
                binding,
                identity,
                subscription,
                peer,
                receiver,
                output,
                agreement,
                values,
                maintenance_updates: 0,
            });
        }

        let mut oracle = Self {
            peers,
            receivers,
            shapes,
            rows,
            aggregates,
        };
        oracle.assert_checkpoint(core, seed, "t0");
        oracle
    }

    fn tick_and_assert<S: OrderedKvStorage>(
        &mut self,
        core: &mut NodeState<S>,
        seed: u64,
        checkpoint: &str,
    ) {
        self.tick(core, seed, checkpoint);
        self.assert_checkpoint(core, seed, checkpoint);
    }

    fn tick<S: OrderedKvStorage>(
        &mut self,
        core: &mut NodeState<S>,
        seed: u64,
        checkpoint: &str,
    ) {
        for (((peer, receiver), shape), rows) in self
            .peers
            .iter_mut()
            .zip(self.receivers.iter_mut())
            .zip(self.shapes.iter())
            .zip(self.rows.iter_mut())
        {
            let update = peer
                .query_update_for_subscription(
                    core,
                    shape.subscription,
                    &shape.shape,
                    &shape.binding,
                )
                .unwrap_or_else(|err| {
                    panic!(
                        "seed {seed}: maintained update failed for {} at {checkpoint}: {err:?}",
                        shape.name
                    )
                });
            receiver.apply_sync_message_settled(update).unwrap_or_else(|err| {
                panic!(
                    "seed {seed}: receiver rejected source closure for {} at {checkpoint}: {err:?}",
                    shape.name
                )
            });
            *rows = m3_receiver_rows(receiver, &shape.shape, &shape.binding, shape.identity);
        }
        for aggregate in &mut self.aggregates {
            let aggregate_update = aggregate
                .peer
                .query_update_for_subscription(
                    core,
                    aggregate.subscription,
                    &aggregate.shape,
                    &aggregate.binding,
                )
                .unwrap_or_else(|err| {
                    panic!(
                        "seed {seed}: maintained update failed for {} at {checkpoint}: {err:?}",
                        aggregate.name
                    )
                });
            aggregate
                .receiver
                .apply_sync_message_settled(aggregate_update)
                .unwrap_or_else(|err| {
                    panic!(
                        "seed {seed}: aggregate receiver rejected source closure for {} at {checkpoint}: {err:?}",
                        aggregate.name
                    )
                });
            aggregate.values = receiver_aggregate_values(
                &mut aggregate.receiver,
                &aggregate.shape,
                &aggregate.binding,
                aggregate.identity,
                aggregate.output,
            );
            aggregate.maintenance_updates += 1;
        }
    }

    fn charge_aggregate_updates(&mut self, updates: u64) {
        for aggregate in &mut self.aggregates {
            aggregate.maintenance_updates += updates;
        }
    }

    fn assert_checkpoint<S: OrderedKvStorage>(
        &mut self,
        core: &mut NodeState<S>,
        seed: u64,
        checkpoint: &str,
    ) {
        for (maintained, shape) in self.rows.iter().zip(self.shapes.iter()) {
            let one_shot = one_shot_rows(core, &shape.shape, &shape.binding, shape.identity);
            assert_eq!(
                maintained, &one_shot,
                "seed {seed}: maintained/one-shot divergence for {} at {checkpoint}",
                shape.name
            );
        }
        for agreement in [AggregateAgreement::Exact, AggregateAgreement::F64Approx] {
            for aggregate in self
                .aggregates
                .iter()
                .filter(|aggregate| aggregate.agreement == agreement)
            {
                let one_shot = one_shot_aggregate_values(
                    core,
                    &aggregate.shape,
                    &aggregate.binding,
                    aggregate.identity,
                    aggregate.output,
                );
                assert_aggregate_agreement(
                    &aggregate.values,
                    &one_shot,
                    aggregate,
                    core,
                    seed,
                    checkpoint,
                );
            }
        }
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
#[ignore = "#1787: manual randomized differential soak; bounded seed 11 runs in CI"]
fn m3_maintained_one_shot_differential_oracle() {
    run_m3_aggregate_churn_curve();
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
    let mut differential =
        DifferentialOracle::open(&mut core, &schema, m3_differential_shapes(&schema), seed);

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

fn run_m3_aggregate_churn_curve() {
    let schema = m3_differential_schema();
    let column_families = schema.column_families();
    let column_family_refs = column_families.iter().map(String::as_str).collect::<Vec<_>>();
    let mut core = NodeState::new(
        node(0x76),
        schema.clone(),
        MemoryStorage::new(&column_family_refs).expect("valid memory storage families"),
    )
    .unwrap();
    let mut parents = BTreeMap::new();

    // This group deliberately cancels large opposite-sign values.  A bound
    // relative to the result would collapse around zero; Σ|x| remains about
    // 2e16 throughout the churn arm.
    for (doc, f64_value, nullable_f64_value, i64_value, u64_value) in [
        (row(0x11), 1.0e16, Some(1.0e16), -11, 11),
        (row(0x12), -1.0e16, Some(-1.0e16), 7, 19),
        (row(0x14), 0.1, None, 13, 23),
    ] {
        accept_churn_with_parent(
            &mut core,
            &mut parents,
            doc,
            400,
            differential_doc_cells(
                "churn-cancellation",
                "match",
                user(0xa1),
                7,
                1,
                f64_value,
                nullable_f64_value,
                i64_value,
                u64_value,
            ),
        );
    }
    let mut differential = DifferentialOracle::open_with_aggregate_specs(
        &mut core,
        &schema,
        Vec::new(),
        vec![
            (
                "docs_f64_sum_by_bucket",
                crate::query::Aggregate::sum("f64_value"),
                "sum_f64_value",
                AggregateAgreement::F64Approx,
            ),
            (
                "docs_f64_avg_by_bucket",
                crate::query::Aggregate::avg("f64_value"),
                "avg_f64_value",
                AggregateAgreement::F64Approx,
            ),
        ],
        0,
    );
    let mut previous_depth = 0;
    for depth in m3_aggregate_churn_depths() {
        for operation in previous_depth + 1..=depth {
            match operation % 3 {
                0 => {
                    delete_churn_with_parent(
                        &mut core,
                        &mut parents,
                        churn_row(operation / 3),
                        1_000 + operation,
                    );
                }
                1 => {
                    accept_churn_with_parent(
                        &mut core,
                        &mut parents,
                        churn_row(operation / 3 + 1),
                        1_000 + operation,
                        differential_doc_cells(
                            "churn-transient",
                            "match",
                            user(0xa1),
                            7,
                            1,
                            0.3,
                            Some(0.3),
                            1,
                            1,
                        ),
                    );
                }
                _ => {
                    accept_churn_with_parent(
                        &mut core,
                        &mut parents,
                        churn_row(operation / 3),
                        1_000 + operation,
                        differential_doc_cells(
                            "churn-transient-updated",
                            "match",
                            user(0xa1),
                            7,
                            1,
                            0.4,
                            Some(0.4),
                            1,
                            1,
                        ),
                    );
                }
            }
        }
        differential.charge_aggregate_updates(depth - previous_depth - 1);
        differential.tick(&mut core, 0, &format!("churn-{depth}"));
        report_f64_churn_divergence(&differential, &mut core, depth);
        previous_depth = depth;
    }
}

fn m3_aggregate_churn_depths() -> Vec<u64> {
    std::env::var("JAZZ_DIFFERENTIAL_CHURN_DEPTHS")
        .ok()
        .map(|depths| {
            depths
                .split(',')
                .map(|depth| depth.parse::<u64>().expect("churn depths must be u64"))
                .collect()
        })
        .unwrap_or_else(|| vec![10, 1_000, 100_000])
}

fn churn_row(index: u64) -> RowUuid {
    let mut bytes = [0_u8; 16];
    bytes[..8].copy_from_slice(&index.to_be_bytes());
    bytes[15] = 0xfe;
    RowUuid::from_bytes(bytes)
}

type LayerParents = BTreeMap<RowUuid, (Option<TxId>, Option<TxId>)>;

fn accept_churn_with_parent<S: OrderedKvStorage + ReopenableStorage>(
    core: &mut NodeState<S>,
    parents: &mut LayerParents,
    row_uuid: RowUuid,
    made_at: u64,
    cells: BTreeMap<String, Value>,
) {
    let mut commit = MergeableCommit::new("docs", row_uuid, made_at).cells(cells);
    if let Some(parent) = parents.get(&row_uuid).and_then(|(content, _)| *content) {
        commit = commit.parents(vec![parent]);
    }
    let tx_id = core.commit_mergeable_settled(commit).unwrap();
    core.accept_global_for_test(tx_id).unwrap();
    parents
        .entry(row_uuid)
        .or_default()
        .0 = Some(tx_id);
}

fn delete_churn_with_parent<S: OrderedKvStorage + ReopenableStorage>(
    core: &mut NodeState<S>,
    parents: &mut LayerParents,
    row_uuid: RowUuid,
    made_at: u64,
) {
    let mut commit = MergeableCommit::new("docs", row_uuid, made_at)
        .deletion(DeletionEvent::Deleted);
    if let Some(parent) = parents.get(&row_uuid).and_then(|(_, deletion)| *deletion) {
        commit = commit.parents(vec![parent]);
    }
    let tx_id = core
        .commit_mergeable_settled(commit)
        .unwrap();
    core.accept_global_for_test(tx_id).unwrap();
    parents
        .entry(row_uuid)
        .or_default()
        .1 = Some(tx_id);
}

#[test]
fn m3_maintained_one_shot_differential_oracle_f64_approximate_control() {
    let schema = m3_differential_schema();
    let column_families = schema.column_families();
    let column_family_refs = column_families.iter().map(String::as_str).collect::<Vec<_>>();
    let mut core = NodeState::new(
        node(0x78),
        schema.clone(),
        MemoryStorage::new(&column_family_refs).expect("valid memory storage families"),
    )
    .unwrap();
    let mut parents = BTreeMap::new();
    for (row_uuid, f64_value) in [(row(0x21), 1.5), (row(0x22), -0.25)] {
        accept_churn_with_parent(
            &mut core,
            &mut parents,
            row_uuid,
            600,
            differential_doc_cells(
                "f64-control",
                "match",
                user(0xa1),
                7,
                1,
                f64_value,
                Some(f64_value),
                1,
                1,
            ),
        );
    }
    let mut differential = DifferentialOracle::open_with_aggregate_specs(
        &mut core,
        &schema,
        Vec::new(),
        vec![
            (
                "docs_count_by_bucket",
                crate::query::Aggregate::count(),
                "count",
                AggregateAgreement::Exact,
            ),
            (
                "docs_f64_min_by_bucket",
                crate::query::Aggregate::min("f64_value"),
                "min_f64_value",
                AggregateAgreement::Exact,
            ),
            (
                "docs_f64_max_by_bucket",
                crate::query::Aggregate::max("f64_value"),
                "max_f64_value",
                AggregateAgreement::Exact,
            ),
            (
                "docs_f64_sum_by_bucket",
                crate::query::Aggregate::sum("f64_value"),
                "sum_f64_value",
                AggregateAgreement::F64Approx,
            ),
            (
                "docs_f64_avg_by_bucket",
                crate::query::Aggregate::avg("f64_value"),
                "avg_f64_value",
                AggregateAgreement::F64Approx,
            ),
        ],
        0,
    );
    accept_churn_with_parent(
        &mut core,
        &mut parents,
        row(0x23),
        601,
        differential_doc_cells(
            "f64-control-insert",
            "match",
            user(0xa1),
            7,
            1,
            0.5,
            Some(0.5),
            1,
            1,
        ),
    );
    differential.tick_and_assert(&mut core, 0, "normal-scale-insert");
}

#[test]
fn m3_maintained_one_shot_differential_oracle_null_semantics() {
    let schema = m3_differential_schema();
    let (_core_dir, mut core) = open_node_with_schema(node(0x77), schema.clone());
    seed_m3_differential_base(&mut core, 0);
    let mut parents = m3_differential_parent_map(&mut core);
    accept_with_parent(
        &mut core,
        &mut parents,
        "docs",
        row(0x15),
        500,
        differential_doc_cells(
            "all-null",
            "match",
            user(0xa1),
            7,
            3,
            4.0,
            None,
            4,
            4,
        ),
    );

    let shape = Query::from("docs")
        .sum("nullable_f64_value")
        .group_by("bucket")
        .validate(&schema)
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let subscription = SubscriptionKey {
        shape_id: shape.shape_id(),
        binding_id: binding.binding_id(),
        read_view: Default::default(),
    };
    let mut peer = PeerState::client_link(user(0xa1));
    let initial = peer.rehydrate_query(&mut core, &shape, &binding).unwrap();
    let mut receiver = maintained_receiver(&schema, 0xd1);
    register_maintained_receiver(&mut receiver, &shape, &binding, user(0xa1));
    receiver
        .apply_sync_message_settled(initial)
        .expect("apply exact initial aggregate source closure");
    let mut maintained = receiver_aggregate_values(
        &mut receiver,
        &shape,
        &binding,
        user(0xa1),
        "sum_nullable_f64_value",
    );
    assert_eq!(
        maintained,
        one_shot_aggregate_values(
            &mut core,
            &shape,
            &binding,
            user(0xa1),
            "sum_nullable_f64_value",
        ),
        "nullable aggregates must agree at initial hydration"
    );
    assert!(
        !maintained.contains_key(&3),
        "all-NULL groups must not materialize a numeric aggregate value"
    );

    delete_with_parent(&mut core, &mut parents, "docs", row(0x11), 501);
    let update = peer
        .query_update_for_subscription(&mut core, subscription, &shape, &binding)
        .unwrap();
    receiver
        .apply_sync_message_settled(update)
        .expect("apply exact aggregate source closure after nullable group transition");
    maintained = receiver_aggregate_values(
        &mut receiver,
        &shape,
        &binding,
        user(0xa1),
        "sum_nullable_f64_value",
    );
    assert_eq!(
        maintained,
        one_shot_aggregate_values(
            &mut core,
            &shape,
            &binding,
            user(0xa1),
            "sum_nullable_f64_value",
        ),
        "nullable aggregates must agree after a group becomes all-NULL"
    );
    assert!(
        !maintained.contains_key(&1),
        "groups that become all-NULL must be removed from the synthetic result"
    );

    delete_with_parent(&mut core, &mut parents, "docs", row(0x12), 502);
    delete_with_parent(&mut core, &mut parents, "docs", row(0x13), 503);
    delete_with_parent(&mut core, &mut parents, "docs", row(0x14), 504);
    let update = peer
        .query_update_for_subscription(&mut core, subscription, &shape, &binding)
        .unwrap();
    receiver
        .apply_sync_message_settled(update)
        .expect("apply exact aggregate source closure after empty-group transition");
    maintained = receiver_aggregate_values(
        &mut receiver,
        &shape,
        &binding,
        user(0xa1),
        "sum_nullable_f64_value",
    );
    assert_eq!(
        maintained,
        one_shot_aggregate_values(
            &mut core,
            &shape,
            &binding,
            user(0xa1),
            "sum_nullable_f64_value",
        ),
        "nullable aggregates must agree after a group becomes empty"
    );
    assert!(
        !maintained.contains_key(&2),
        "empty groups must not remain in the synthetic result"
    );
}

fn report_f64_churn_divergence<S: OrderedKvStorage>(
    differential: &DifferentialOracle,
    core: &mut NodeState<S>,
    depth: u64,
) {
    let absolute_sums = f64_absolute_sums_by_bucket(core);
    for aggregate in &differential.aggregates {
        if !matches!(aggregate.agreement, AggregateAgreement::F64Approx) {
            continue;
        }
        let one_shot = one_shot_aggregate_values(
            core,
            &aggregate.shape,
            &aggregate.binding,
            aggregate.identity,
            aggregate.output,
        );
        let Value::F64(maintained) = aggregate.values[&1] else {
            panic!("{} must produce F64 output", aggregate.name);
        };
        let Value::F64(one_shot) = one_shot[&1] else {
            panic!("{} must produce F64 output", aggregate.name);
        };
        let error = (maintained - one_shot).abs();
        let tolerance = F64_AGGREGATE_ERROR_FACTOR
            * f64::EPSILON
            * (depth as f64 + 4.0)
            * absolute_sums[&1];
        eprintln!(
            "M3 F64 CHURN depth={depth} aggregate={} error={error:e} tolerance={tolerance:e} sum_abs={:e} maintained={maintained:e} one_shot={one_shot:e}",
            aggregate.name,
            absolute_sums[&1],
        );
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
            1,
            2.5,
            Some(2.5),
            17,
            17,
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
    let mut peer = PeerState::client_link(identity);
    register_maintained_receiver(&mut reader, &shape, &binding, identity);

    let initial = peer.rehydrate_query(&mut core, &shape, &binding).unwrap();
    reader.apply_sync_message_settled(initial).unwrap();
    let mut maintained_rows = m3_receiver_rows(&mut reader, &shape, &binding, identity);
    assert!(!maintained_rows.is_empty());

    for row_uuid in [row(0x11), row(0x12), row(0x14)] {
        if core
            .local_content_winner_tx_id("docs", row_uuid)
            .unwrap()
            .is_some()
        {
            accept_global(
                &mut core,
                MergeableCommit::new("docs", row_uuid, 100 + row_uuid.0.as_bytes()[0] as u64)
                    .deletion(DeletionEvent::Deleted),
            );
        }
    }
    let reset = peer.rehydrate_query(&mut core, &shape, &binding).unwrap();
    reader.apply_sync_message_settled(reset).unwrap();
    maintained_rows = m3_receiver_rows(&mut reader, &shape, &binding, identity);
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
    let mut oracle =
        DifferentialOracle::open(&mut core, &schema, m3_differential_shapes(&schema), 0);
    let mut parents = m3_differential_parent_map(&mut core);

    revoke_edge_access(&mut core, &mut parents, 0);
    oracle.tick_and_assert(&mut core, 0, "after-revoke-mid-stream");

    drop(core);
    core = reopen_node_at(&core_dir, node(0x75), schema);
    grant_edge_access(&mut core, &mut parents, 1);
    oracle.tick_and_assert(&mut core, 0, "after-reconnect-mid-stream");
}

#[test]
fn m3_inherited_child_delete_with_concurrent_insert_reconciles_authoritatively() {
    let schema = m3_differential_schema();
    let (_core_dir, mut core) = open_node_with_schema(node(0x77), schema.clone());
    seed_m3_differential_base(&mut core, 0);
    let mut oracle =
        DifferentialOracle::open(&mut core, &schema, m3_differential_shapes(&schema), 0);
    let mut parents = m3_differential_parent_map(&mut core);

    // The deletion witness for 0x71 and result-current insertion for 0x72
    // reach the maintained peer in one flush. Reconciliation must replace
    // the stale child, not let the unrelated visible add suppress the delete.
    delete_with_parent(&mut core, &mut parents, "children", row(0x71), 700);
    accept_with_parent(
        &mut core,
        &mut parents,
        "children",
        row(0x72),
        701,
        BTreeMap::from([
            ("doc".to_owned(), Value::Uuid(row(0x11).0)),
            ("status".to_owned(), Value::String("open".to_owned())),
        ]),
    );

    oracle.tick_and_assert(&mut core, 0, "delete-and-insert-child");
}

fn m3_differential_schema() -> JazzSchema {
    let same_table_policy = crate::test_public_schema::seeded_recursive_access_policy(
        "resource_access",
        "resource",
        "team",
        &[("administrator", PublicValue::Boolean(false))],
        &[],
        "team_edges",
        "member",
        "parent",
        &[("administrator", PublicValue::Boolean(false))],
        "teams",
        "identity_key",
        &["claims", "sub"],
        "id",
    );
    let string_same_table_policy = crate::test_public_schema::seeded_recursive_access_policy(
        "string_resource_access",
        "resource",
        "team",
        &[("administrator", PublicValue::Boolean(false))],
        &[],
        "team_edges",
        "member",
        "parent",
        &[("administrator", PublicValue::Boolean(false))],
        "teams",
        "identity_key_text",
        &["claims", "sub"],
        "id",
    );

    build_public_test_schema(
        PublicSchemaBuilder::new()
            .table(
                PublicTableSchemaBuilder::new("docs")
                    .column("title", PublicColumnType::Text)
                    .column("kind", PublicColumnType::Text)
                    .column("createdAt", PublicColumnType::Timestamp)
                    .column("bucket", PublicColumnType::Timestamp)
                    .column("f64_value", PublicColumnType::Double)
                    .nullable_column("nullable_f64_value", PublicColumnType::Double)
                    .column("i64_value", PublicColumnType::BigInt)
                    .column("u64_value", PublicColumnType::Timestamp)
                    .policies(public_all_policies()),
            )
            .table(
                PublicTableSchemaBuilder::new("children")
                    .fk_column("doc", "docs")
                    .column("status", PublicColumnType::Text)
                    .policies(public_all_policies()),
            )
            .table(
                PublicTableSchemaBuilder::new("grandchildren")
                    .fk_column("child", "children")
                    .policies(public_all_policies()),
            )
            .table(
                PublicTableSchemaBuilder::new("teams")
                    .column("id", PublicColumnType::Uuid)
                    .column("name", PublicColumnType::Text)
                    .column("identity_key", PublicColumnType::Text)
                    .column("identity_key_text", PublicColumnType::Text)
                    .policies(public_all_policies()),
            )
            .table(
                PublicTableSchemaBuilder::new("team_edges")
                    .fk_column("member", "teams")
                    .fk_column("parent", "teams")
                    .column("administrator", PublicColumnType::Boolean)
                    .policies(public_all_policies()),
            )
            .table(
                PublicTableSchemaBuilder::new("group_access_edges")
                    .column("user_id", PublicColumnType::Text)
                    .fk_column("group_id", "teams")
                    .policies(public_all_policies()),
            )
            .table(
                PublicTableSchemaBuilder::new("doc_access")
                    .fk_column("doc", "docs")
                    .fk_column("team", "teams")
                    .policies(public_all_policies()),
            )
            .table(
                PublicTableSchemaBuilder::new("resources")
                    .column("label", PublicColumnType::Text)
                    .policies(
                        public_all_policies().with_select(same_table_policy),
                    ),
            )
            .table(
                PublicTableSchemaBuilder::new("string_resources")
                    .column("label", PublicColumnType::Text)
                    .policies(
                        public_all_policies().with_select(string_same_table_policy),
                    ),
            )
            .table(
                PublicTableSchemaBuilder::new("resource_access")
                    .fk_column("resource", "resources")
                    .fk_column("team", "teams")
                    .column("administrator", PublicColumnType::Boolean)
                    .policies(public_all_policies()),
            )
            .table(
                PublicTableSchemaBuilder::new("string_resource_access")
                    .fk_column("resource", "string_resources")
                    .fk_column("team", "teams")
                    .column("administrator", PublicColumnType::Boolean)
                    .policies(public_all_policies()),
            ),
    )
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
            .seeded_by("group_access_edges", "user_id", "user", "group_id")
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
        Query::from("children")
            .inherits("doc")
            .validate(schema)
            .unwrap(),
    );
    push(
        "grandchildren_inherit_child",
        Query::from("grandchildren")
            .inherits("child")
            .validate(schema)
            .unwrap(),
    );
    push(
        "docs_projected_with_doc_access",
        Query::from("docs")
            .select(["title"])
            .array_subquery(
                ArraySubquery::new("access", "doc_access", "doc", "id")
                    .select(["team"])
                    ,
            )
            .validate(schema)
            .unwrap(),
    );
    push(
        "docs_relation_facade_direct_access",
        relation_doc_access_shape().validate(schema).unwrap(),
    );
    specs
}

fn aggregate_differential_specs() -> Vec<AggregateSpec> {
    use crate::query::Aggregate;

    vec![
        (
            "docs_count_by_bucket",
            Aggregate::count(),
            "count",
            AggregateAgreement::Exact,
        ),
        (
            "docs_f64_sum_by_bucket",
            Aggregate::sum("f64_value"),
            "sum_f64_value",
            AggregateAgreement::F64Approx,
        ),
        (
            "docs_f64_avg_by_bucket",
            Aggregate::avg("f64_value"),
            "avg_f64_value",
            AggregateAgreement::F64Approx,
        ),
        (
            "docs_f64_min_by_bucket",
            Aggregate::min("f64_value"),
            "min_f64_value",
            AggregateAgreement::Exact,
        ),
        (
            "docs_f64_max_by_bucket",
            Aggregate::max("f64_value"),
            "max_f64_value",
            AggregateAgreement::Exact,
        ),
        (
            "docs_i64_sum_by_bucket",
            Aggregate::sum("i64_value"),
            "sum_i64_value",
            AggregateAgreement::Exact,
        ),
        (
            "docs_i64_avg_by_bucket",
            Aggregate::avg("i64_value"),
            "avg_i64_value",
            AggregateAgreement::Exact,
        ),
        (
            "docs_i64_min_by_bucket",
            Aggregate::min("i64_value"),
            "min_i64_value",
            AggregateAgreement::Exact,
        ),
        (
            "docs_i64_max_by_bucket",
            Aggregate::max("i64_value"),
            "max_i64_value",
            AggregateAgreement::Exact,
        ),
        (
            "docs_u64_sum_by_bucket",
            Aggregate::sum("u64_value"),
            "sum_u64_value",
            AggregateAgreement::Exact,
        ),
        (
            "docs_u64_avg_by_bucket",
            Aggregate::avg("u64_value"),
            "avg_u64_value",
            AggregateAgreement::Exact,
        ),
        (
            "docs_u64_min_by_bucket",
            Aggregate::min("u64_value"),
            "min_u64_value",
            AggregateAgreement::Exact,
        ),
        (
            "docs_u64_max_by_bucket",
            Aggregate::max("u64_value"),
            "max_u64_value",
            AggregateAgreement::Exact,
        ),
    ]
}

fn created_by_shape(schema: &JazzSchema) -> ValidatedQuery {
    Query::from("docs")
        .filter(eq(col("$createdBy"), claim("user")))
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
                (
                    "identity_key".to_owned(),
                    Value::String(identity.canonical().to_owned()),
                ),
                (
                    "identity_key_text".to_owned(),
                    Value::String(identity.canonical().to_owned()),
                ),
            ])),
        );
    }
    accept_global(
        core,
        team_edge_commit(row(0x41), row(0x31), row(0x32), false, 2),
    );
    accept_global(
        core,
        MergeableCommit::new("group_access_edges", row(0x42), 3).cells(BTreeMap::from([
            ("user_id".to_owned(), Value::String(alice.canonical().to_owned())),
            ("group_id".to_owned(), Value::Uuid(row(0x31).0)),
        ])),
    );

    for (
        doc,
        title,
        kind,
        author,
        created_at,
        bucket,
        f64_value,
        nullable_f64_value,
        i64_value,
        u64_value,
    ) in [
        (
            row(0x11),
            "visible-direct",
            "match",
            alice,
            7,
            1,
            1.5,
            Some(1.5),
            -11,
            11,
        ),
        (
            row(0x12),
            "visible-transitive",
            "match",
            alice,
            7,
            1,
            -0.25,
            None,
            7,
            19,
        ),
        (
            row(0x13),
            "hidden",
            "match",
            bob,
            8,
            2,
            3.0,
            Some(3.0),
            -5,
            5,
        ),
        (
            row(0x14),
            "filtered-out",
            "other",
            alice,
            9,
            2,
            0.75,
            None,
            13,
            23,
        ),
    ] {
        accept_global(
            core,
            MergeableCommit::new("docs", doc, 10 + seed % 3).made_by(author).cells(differential_doc_cells(
                title,
                kind,
                author,
                created_at,
                bucket,
                f64_value,
                nullable_f64_value,
                i64_value,
                u64_value,
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
    for (resource, label) in [
        (row(0x61), "direct"),
        (row(0x62), "transitive"),
        (row(0x63), "hidden"),
    ] {
        accept_global(
            core,
            MergeableCommit::new("resources", resource, 30).cells(BTreeMap::from([(
                "label".to_owned(),
                Value::String(label.to_owned()),
            )])),
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
            MergeableCommit::new("string_resources", resource, 32).cells(BTreeMap::from([(
                "label".to_owned(),
                Value::String(label.to_owned()),
            )])),
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
        MergeableCommit::new("grandchildren", row(0x81), 41).cells(BTreeMap::from([(
            "child".to_owned(),
            Value::Uuid(row(0x71).0),
        )])),
    );
}

type TableLayerParents = BTreeMap<(&'static str, RowUuid), (Option<TxId>, Option<TxId>)>;

fn m3_differential_parent_map(
    core: &mut NodeState<RocksDbStorage>,
) -> TableLayerParents {
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
            let content = core
                .local_content_winner_tx_id(table, row.row_uuid())
                .unwrap();
            let deletion = core
                .local_deletion_winner_tx_id(table, row.row_uuid())
                .unwrap();
            parents.insert((table, row.row_uuid()), (content, deletion));
        }
    }
    parents
}

fn differential_doc_cells(
    title: &str,
    kind: &str,
    _author: AuthorSubject,
    created_at: u64,
    bucket: u64,
    f64_value: f64,
    nullable_f64_value: Option<f64>,
    i64_value: i64,
    u64_value: u64,
) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("title".to_owned(), Value::String(title.to_owned())),
        ("kind".to_owned(), Value::String(kind.to_owned())),
        ("createdAt".to_owned(), Value::U64(created_at)),
        ("bucket".to_owned(), Value::U64(bucket)),
        ("f64_value".to_owned(), Value::F64(f64_value)),
        (
            "nullable_f64_value".to_owned(),
            Value::Nullable(nullable_f64_value.map(|value| Box::new(Value::F64(value)))),
        ),
        ("i64_value".to_owned(), Value::I64(i64_value)),
        ("u64_value".to_owned(), Value::U64(u64_value)),
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
    parents: &mut TableLayerParents,
    table: &'static str,
    row_uuid: RowUuid,
    made_at: u64,
    cells: BTreeMap<String, Value>,
) -> TxId {
    let mut commit = MergeableCommit::new(table, row_uuid, made_at).cells(cells);
    if let Some(parent) = parents
        .get(&(table, row_uuid))
        .and_then(|(content, _)| *content)
    {
        commit = commit.parents(vec![parent]);
    }
    let tx = accept_global(core, commit);
    parents.entry((table, row_uuid)).or_default().0 = Some(tx);
    tx
}

fn delete_with_parent(
    core: &mut NodeState<RocksDbStorage>,
    parents: &mut TableLayerParents,
    table: &'static str,
    row_uuid: RowUuid,
    made_at: u64,
) -> TxId {
    let mut commit = MergeableCommit::new(table, row_uuid, made_at)
        .deletion(DeletionEvent::Deleted);
    if let Some(parent) = parents
        .get(&(table, row_uuid))
        .and_then(|(_, deletion)| *deletion)
    {
        commit = commit.parents(vec![parent]);
    }
    let tx = accept_global(core, commit);
    parents.entry((table, row_uuid)).or_default().1 = Some(tx);
    tx
}

fn add_visible_doc(
    core: &mut NodeState<RocksDbStorage>,
    parents: &mut TableLayerParents,
    step: u64,
) {
    let row_uuid = row(0x90 + step as u8);
    accept_with_parent(
        core,
        parents,
        "docs",
        row_uuid,
        100 + step,
        differential_doc_cells("added", "match", user(0xa1), 7, 1, 0.5, Some(0.5), 3, 3),
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
    parents: &mut TableLayerParents,
    step: u64,
) {
    accept_with_parent(
        core,
        parents,
        "docs",
        row(0xb0 + step as u8),
        140 + step,
        differential_doc_cells("hidden-added", "match", user(0xb2), 8, 2, -0.5, None, -3, 5),
    );
}

fn revoke_edge_access(
    core: &mut NodeState<RocksDbStorage>,
    parents: &mut TableLayerParents,
    step: u64,
) {
    delete_with_parent(core, parents, "group_access_edges", row(0x42), 160 + step);
}

fn grant_edge_access(
    core: &mut NodeState<RocksDbStorage>,
    parents: &mut TableLayerParents,
    step: u64,
) {
    accept_with_parent(
        core,
        parents,
        "group_access_edges",
        row(0x42),
        180 + step,
        BTreeMap::from([
            (
                "user_id".to_owned(),
                Value::String(user(0xa1).canonical().to_owned()),
            ),
            ("group_id".to_owned(), Value::Uuid(row(0x31).0)),
        ]),
    );
}

fn delete_visible_child(
    core: &mut NodeState<RocksDbStorage>,
    parents: &mut TableLayerParents,
    step: u64,
) {
    delete_with_parent(core, parents, "children", row(0x71), 200 + step);
}

fn restore_visible_child(
    core: &mut NodeState<RocksDbStorage>,
    parents: &mut TableLayerParents,
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
    parents: &mut TableLayerParents,
    step: u64,
) {
    accept_with_parent(
        core,
        parents,
        "docs",
        row(0x14),
        240 + step,
        differential_doc_cells(
            "filtered-now-created-at",
            "match",
            user(0xa1),
            7,
            2,
            0.75,
            None,
            13,
            23,
        ),
    );
}

fn one_shot_rows<S: OrderedKvStorage>(
    core: &mut NodeState<S>,
    shape: &ValidatedQuery,
    binding: &Binding,
    identity: AuthorSubject,
) -> BTreeSet<(String, RowUuid)> {
    core.query_rows_for_link(shape, binding, DurabilityTier::Global, identity)
        .unwrap()
        .into_iter()
        .map(|row| (row.table().to_owned(), row.row_uuid()))
        .collect()
}

fn one_shot_aggregate_values<S: OrderedKvStorage>(
    core: &mut NodeState<S>,
    shape: &ValidatedQuery,
    binding: &Binding,
    identity: AuthorSubject,
    output: &str,
) -> BTreeMap<u64, Value> {
    core.query_rows_for_link(shape, binding, DurabilityTier::Global, identity)
        .unwrap()
        .into_iter()
        .filter_map(|row| {
            let cells = row.test_cells_by_descriptor();
            let Value::U64(bucket) = &cells["bucket"] else {
                panic!("aggregate bucket must be U64");
            };
            cells.get(output).cloned().map(|value| (*bucket, value))
        })
        .collect()
}

/// The authority ships only the exact, policy-scoped source closure. Output is
/// deliberately computed by a separately registered client receiver; it must
/// never be read back from authority result facts on the update.
fn maintained_receiver(schema: &JazzSchema, receiver_id: u8) -> NodeState<MemoryStorage> {
    let column_families = schema.column_families();
    let refs = column_families.iter().map(String::as_str).collect::<Vec<_>>();
    let mut receiver = NodeState::new(
        node(receiver_id),
        schema.clone(),
        MemoryStorage::new(&refs).expect("valid aggregate receiver storage"),
    )
    .expect("open aggregate receiver");
    receiver.set_non_durable_client();
    receiver
}

fn register_maintained_receiver<S: OrderedKvStorage + ReopenableStorage>(
    receiver: &mut NodeState<S>,
    shape: &ValidatedQuery,
    binding: &Binding,
    identity: AuthorSubject,
) {
    receiver
        .apply_sync_message_settled(SyncMessage::RegisterShape {
            shape_id: shape.shape_id(),
            ast: crate::protocol::ShapeAst::from_validated(shape),
            opts: crate::protocol::RegisterShapeOptions::default(),
        })
        .expect("register maintained receiver shape");
    let values = shape
        .params()
        .keys()
        .map(|name| binding.values().get(name).cloned().expect("bound parameter"))
        .collect();
    receiver
        .apply_sync_message_settled(SyncMessage::Subscribe(crate::protocol::Subscribe {
            shape_id: shape.shape_id(),
            subscription: SubscriptionKey {
                shape_id: shape.shape_id(),
                binding_id: binding.binding_id(),
                read_view: Default::default(),
            },
            values,
            known_state: None,
            delegated_session: Some(crate::protocol::DelegatedSessionBinding {
                identity,
                claims: BTreeMap::new(),
            }),
        }))
        .expect("register policy-scoped maintained receiver subscription");
}

fn m3_receiver_rows<S: OrderedKvStorage>(
    receiver: &mut NodeState<S>,
    shape: &ValidatedQuery,
    binding: &Binding,
    identity: AuthorSubject,
) -> BTreeSet<(String, RowUuid)> {
    let (shape, binding, plan) = receiver
        .prepare_query_binding_for_link_in_authorization_mode(
            shape,
            binding,
            DurabilityTier::Global,
            identity,
            QueryAuthorizationMode::ClientLocal,
        )
        .resolve()
        .expect("prepare maintained receiver from covered inputs");
    let (_subscription, snapshot) = receiver
        .open_maintained_view_subscription_in_authorization_mode(
            &shape,
            &binding,
            identity,
            DurabilityTier::Global,
            &crate::protocol::ReadViewSpec::default(),
            Some(plan),
            QueryAuthorizationMode::ClientLocal,
        )
        .resolve()
        .expect("evaluate maintained receiver from covered inputs");
    snapshot
        .rows
        .into_iter()
        .map(|row| (row.table().to_owned(), row.row_uuid()))
        .collect()
}

fn receiver_aggregate_values(
    receiver: &mut NodeState<MemoryStorage>,
    shape: &ValidatedQuery,
    binding: &Binding,
    identity: AuthorSubject,
    output: &str,
) -> BTreeMap<u64, Value> {
    let (shape, binding, plan) = receiver
        .prepare_query_binding_for_link_in_authorization_mode(
            shape,
            binding,
            DurabilityTier::Global,
            identity,
            QueryAuthorizationMode::ClientLocal,
        )
        .resolve()
        .expect("prepare aggregate receiver from covered inputs");
    let (_subscription, snapshot) = receiver
        .open_maintained_view_subscription_in_authorization_mode(
            &shape,
            &binding,
            identity,
            DurabilityTier::Global,
            &crate::protocol::ReadViewSpec::default(),
            Some(plan),
            QueryAuthorizationMode::ClientLocal,
        )
        .resolve()
        .expect("evaluate aggregate from covered receiver inputs");
    snapshot
        .rows
        .into_iter()
        .filter_map(|row| {
            let cells = row.test_cells_by_descriptor();
            let Value::U64(bucket) = &cells["bucket"] else {
                panic!("aggregate bucket must be U64");
            };
            cells.get(output).cloned().map(|value| (*bucket, value))
        })
        .collect()
}

fn assert_aggregate_agreement<S: OrderedKvStorage>(
    maintained: &BTreeMap<u64, Value>,
    one_shot: &BTreeMap<u64, Value>,
    aggregate: &AggregateDifferential,
    core: &mut NodeState<S>,
    seed: u64,
    checkpoint: &str,
) {
    match aggregate.agreement {
        AggregateAgreement::Exact => assert_eq!(
            maintained, one_shot,
            "seed {seed}: maintained/one-shot aggregate divergence for {} at {checkpoint}",
            aggregate.name
        ),
        AggregateAgreement::F64Approx => {
            assert_eq!(
                maintained.keys().collect::<Vec<_>>(),
                one_shot.keys().collect::<Vec<_>>(),
                "seed {seed}: maintained/one-shot aggregate group divergence for {} at {checkpoint}",
                aggregate.name
            );
            let absolute_sums = f64_absolute_sums_by_bucket(core);
            for (bucket, maintained) in maintained {
                let Value::F64(maintained) = maintained else {
                    panic!("{} must produce F64 output", aggregate.name);
                };
                let Value::F64(one_shot) = one_shot[bucket] else {
                    panic!("{} must produce F64 output", aggregate.name);
                };
                let error = (maintained - one_shot).abs();
                let tolerance = F64_AGGREGATE_ERROR_FACTOR
                    * f64::EPSILON
                    * (f64::from(aggregate.maintenance_updates as u32) + 4.0)
                    * absolute_sums[bucket];
                assert!(
                    error <= tolerance,
                    "seed {seed}: maintained/one-shot F64 aggregate divergence for {} bucket {bucket} at {checkpoint}: error={error:e}, tolerance={tolerance:e}, Σ|x|={:e}, maintenance_updates={}",
                    aggregate.name,
                    absolute_sums[bucket],
                    aggregate.maintenance_updates,
                );
            }
        }
    }
}

fn f64_absolute_sums_by_bucket<S: OrderedKvStorage>(
    core: &mut NodeState<S>,
) -> BTreeMap<u64, f64> {
    let mut sums = BTreeMap::new();
    for row in core.current_rows("docs", DurabilityTier::Global).unwrap() {
        let cells = row.test_cells_by_descriptor();
        let Value::U64(bucket) = &cells["bucket"] else {
            panic!("docs bucket must be U64");
        };
        let Value::F64(value) = &cells["f64_value"] else {
            panic!("docs f64_value must be F64");
        };
        *sums.entry(*bucket).or_insert(0.0) += value.abs();
    }
    sums
}
