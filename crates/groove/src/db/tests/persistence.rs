//! Seeded differential oracles for queries, shapes, graphs, and persisted state.

use super::*;

#[futures_test::test]
async fn query_subscription_matches_one_shot_recompute_under_seeded_interleavings() {
    let storage = MemoryStorage::new(&["albums"]).expect("valid memory storage families");
    let mut database = Database::new(albums_schema(), storage).await.unwrap();
    let query = select_query(
        Select::new([SelectItem::expr(col("title"))])
            .from([TableRef::named("albums")])
            .where_(Expr::binary(
                col("id"),
                BinaryOp::Gt,
                Expr::Literal(Value::U64(10)),
            )),
    );
    let subscription = database.subscribe_query(query.clone()).await.unwrap();
    assert!(subscription.recv().unwrap().is_empty());

    let mut seed = 0x5eed_u64;
    let mut known = std::collections::HashSet::<u64>::new();
    let mut materialized = std::collections::BTreeMap::<String, i64>::new();

    for step in 0..96 {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        let id = (seed % 20) + 1;
        let action = (seed >> 8) % 3;
        let mut batch = database.open_batch();
        match (action, known.contains(&id)) {
            (0, false) => {
                known.insert(id);
                batch.insert(
                    "albums",
                    vec![Value::U64(id), Value::String(format!("a{step}"))],
                );
            }
            (1, true) => {
                batch.update(
                    "albums",
                    vec![Value::U64(id), Value::String(format!("u{step}"))],
                );
            }
            (_, true) => {
                known.remove(&id);
                batch.delete("albums", PrimaryKeyValue::U64(id));
            }
            _ => continue,
        }
        database.commit_batch(batch).await.unwrap();

        while let Ok(deltas) = subscription.try_recv() {
            for (values, weight) in deltas.to_values().unwrap() {
                let [Value::String(title)] = values.as_slice() else {
                    panic!("expected projected title, got {values:?}");
                };
                *materialized.entry(title.clone()).or_default() += weight;
            }
            materialized.retain(|_, weight| *weight != 0);
        }

        let recomputed = database.query(query.clone()).await.unwrap();
        let mut expected = std::collections::BTreeMap::<String, i64>::new();
        for (values, weight) in recomputed.to_values().unwrap() {
            let [Value::String(title)] = values.as_slice() else {
                panic!("expected projected title, got {values:?}");
            };
            *expected.entry(title.clone()).or_default() += weight;
        }
        expected.retain(|_, weight| *weight != 0);
        assert_eq!(
            materialized, expected,
            "mismatch after generated step {step}"
        );
    }
}

struct FamilyOracleSubscription {
    param: u64,
    subscription: Subscription,
    materialized: std::collections::BTreeMap<(u64, u64, String), i64>,
}

impl FamilyOracleSubscription {
    fn new(param: u64, subscription: Subscription) -> Self {
        Self {
            param,
            subscription,
            materialized: std::collections::BTreeMap::new(),
        }
    }

    fn drain(&mut self) {
        while let Ok(deltas) = self.subscription.try_recv() {
            apply_artist_album_deltas(&mut self.materialized, deltas);
        }
    }
}

#[futures_test::test]
async fn shape_subscriptions_match_recompute_under_seeded_interleavings() {
    for seed in [0xfade_u64, 0xbad5eed_u64, 0x51a7e_u64, 0xaced_u64] {
        run_shape_subscription_oracle(seed).await;
    }
}

async fn run_shape_subscription_oracle(mut seed: u64) {
    let storage =
        MemoryStorage::new(&["albums", "artists"]).expect("valid memory storage families");
    let mut database = Database::new(albums_artists_schema(), storage)
        .await
        .unwrap();
    let shape = database
        .prepare_one_sink(
            artist_album_shape_graph(),
            "artist_params",
            artist_binding_descriptor(),
            ["artist_id"],
        )
        .await
        .unwrap();
    let mut albums = std::collections::BTreeMap::<u64, (u64, String)>::new();
    let mut subscriptions = Vec::<FamilyOracleSubscription>::new();

    for step in 0..160 {
        seed = seed
            .wrapping_mul(3202034522624059733)
            .wrapping_add(4354685564936845355);
        match (seed >> 9) % 8 {
            0 | 1 => {
                let param = ((seed >> 18) % 6) + 1;
                let mut subscription = FamilyOracleSubscription::new(
                    param,
                    database
                        .bind_shape_one_sink(shape.id(), &[Value::U64(param)])
                        .await
                        .unwrap(),
                );
                database.drive_progress().await.unwrap();
                subscription.drain();
                assert_shape_subscription_matches_oracle(&subscription, &albums, seed, step);
                subscriptions.push(subscription);
            }
            2 if !subscriptions.is_empty() => {
                let idx = (seed as usize) % subscriptions.len();
                let subscription = subscriptions.swap_remove(idx);
                assert!(database.unsubscribe(subscription.subscription.id()));
            }
            3 if !subscriptions.is_empty() => {
                let idx = (seed as usize) % subscriptions.len();
                drop(subscriptions.swap_remove(idx));
            }
            _ => {
                let id = (seed % 32) + 1;
                let artist = ((seed >> 21) % 6) + 1;
                let title = format!("album-{step}-{id}");
                let mut batch = database.open_batch();
                match albums.entry(id) {
                    std::collections::btree_map::Entry::Occupied(mut entry) => {
                        if seed & 1 == 0 {
                            entry.insert((artist, title.clone()));
                            batch.update(
                                "albums",
                                vec![Value::U64(id), Value::U64(artist), Value::String(title)],
                            );
                        } else {
                            entry.remove();
                            batch.delete("albums", PrimaryKeyValue::U64(id));
                        }
                    }
                    std::collections::btree_map::Entry::Vacant(entry) => {
                        entry.insert((artist, title.clone()));
                        batch.insert(
                            "albums",
                            vec![Value::U64(id), Value::U64(artist), Value::String(title)],
                        );
                    }
                }
                database.commit_batch(batch).await.unwrap();
                for subscription in &mut subscriptions {
                    subscription.drain();
                    assert_shape_subscription_matches_oracle(subscription, &albums, seed, step);
                }
            }
        }
    }
}

fn apply_artist_album_deltas(
    materialized: &mut std::collections::BTreeMap<(u64, u64, String), i64>,
    deltas: RecordDeltas,
) {
    for (values, weight) in deltas.to_values().unwrap() {
        let [
            Value::U64(artist_id),
            Value::U64(album_id),
            Value::String(title),
        ] = values.as_slice()
        else {
            panic!("expected artist album delta, got {values:?}");
        };
        *materialized
            .entry((*artist_id, *album_id, title.clone()))
            .or_default() += weight;
    }
    materialized.retain(|_, weight| *weight != 0);
}

fn assert_shape_subscription_matches_oracle(
    subscription: &FamilyOracleSubscription,
    albums: &std::collections::BTreeMap<u64, (u64, String)>,
    seed: u64,
    step: usize,
) {
    let expected = albums
        .iter()
        .filter(|(_, (artist_id, _))| *artist_id == subscription.param)
        .map(|(album_id, (artist_id, title))| ((*artist_id, *album_id, title.clone()), 1))
        .collect::<std::collections::BTreeMap<_, _>>();
    assert_eq!(
        subscription.materialized, expected,
        "shape subscription mismatch after generated seed {seed:#x} step {step}"
    );
}

#[derive(Clone, Copy, Debug)]
enum OracleGraph {
    Reach,
    TwoHop,
    UnblockedEdges,
}

struct OracleSubscription {
    graph: OracleGraph,
    subscription: Subscription,
    materialized: std::collections::BTreeMap<(u64, u64), i64>,
    created_step: usize,
}

impl OracleSubscription {
    fn new(graph: OracleGraph, subscription: Subscription, created_step: usize) -> Self {
        Self {
            graph,
            subscription,
            materialized: std::collections::BTreeMap::new(),
            created_step,
        }
    }

    fn drain(&mut self) {
        while let Ok(deltas) = self.subscription.try_recv() {
            apply_pair_deltas(&mut self.materialized, deltas);
        }
    }
}

#[futures_test::test]
async fn graph_subscriptions_match_recompute_under_seeded_interleavings() {
    for seed in [0xc0ffee_u64, 0x5eed_u64, 0xfacefeed_u64, 0xdecafbad_u64] {
        run_graph_subscription_oracle(seed).await;
    }
}

async fn run_graph_subscription_oracle(mut seed: u64) {
    let storage =
        MemoryStorage::new(&["edges", "blockers"]).expect("valid memory storage families");
    let mut database = Database::new(edges_blockers_schema(), storage)
        .await
        .unwrap();
    let mut edges = std::collections::BTreeMap::<u64, (u64, u64)>::new();
    let mut blockers = std::collections::BTreeMap::<u64, (u64, u64)>::new();
    let mut subscriptions = Vec::<OracleSubscription>::new();

    for step in 0..140 {
        seed = seed
            .wrapping_mul(2862933555777941757)
            .wrapping_add(3037000493);
        match (seed >> 7) % 7 {
            0 | 1 => {
                let graph = match seed % 3 {
                    0 => OracleGraph::Reach,
                    1 => OracleGraph::TwoHop,
                    _ => OracleGraph::UnblockedEdges,
                };
                let builder = match graph {
                    OracleGraph::Reach => reachability_graph(256),
                    OracleGraph::TwoHop => two_hop_graph(),
                    OracleGraph::UnblockedEdges => unblocked_edges_graph(),
                };
                let mut subscription = OracleSubscription::new(
                    graph,
                    database.subscribe_one_sink(builder).await.unwrap(),
                    step,
                );
                subscription.drain();
                assert_eq!(table_pairs_from_query(&mut database, "edges").await, edges);
                assert_eq!(
                    table_pairs_from_query(&mut database, "blockers").await,
                    blockers
                );
                assert_subscription_matches_oracle(&subscription, &edges, &blockers, seed, step);
                subscriptions.push(subscription);
            }
            2 if !subscriptions.is_empty() => {
                let idx = (seed as usize) % subscriptions.len();
                let subscription = subscriptions.swap_remove(idx);
                assert!(database.unsubscribe(subscription.subscription.id()));
            }
            3 => {
                let result = database
                    .query(select_query(
                        Select::new([SelectItem::expr(col("src")), SelectItem::expr(col("dst"))])
                            .from([TableRef::named("edges")]),
                    ))
                    .await
                    .unwrap();
                assert_eq!(pairs_from_deltas(result), direct_edge_multiset(&edges));
            }
            _ => {
                let mut batch = database.open_batch();
                let mutate_edges = seed & 0b100 == 0;
                if mutate_edges {
                    mutate_pair_table(&mut batch, "edges", &mut edges, seed, step);
                } else {
                    mutate_pair_table(&mut batch, "blockers", &mut blockers, seed, step);
                }
                if mutate_edges && seed & 0b100000 == 0 {
                    mutate_pair_table(
                        &mut batch,
                        "blockers",
                        &mut blockers,
                        seed.rotate_left(17),
                        step,
                    );
                }
                database.commit_batch(batch).await.unwrap();
                for subscription in &mut subscriptions {
                    subscription.drain();
                    assert_subscription_matches_oracle(subscription, &edges, &blockers, seed, step);
                }
            }
        }
    }
}

async fn table_pairs_from_query(
    database: &mut Database,
    table: &str,
) -> std::collections::BTreeMap<u64, (u64, u64)> {
    let result = database
        .query(select_query(
            Select::new([
                SelectItem::expr(col("id")),
                SelectItem::expr(col("src")),
                SelectItem::expr(col("dst")),
            ])
            .from([TableRef::named(table)]),
        ))
        .await
        .unwrap();
    result
        .to_values()
        .unwrap()
        .into_iter()
        .filter_map(|(values, weight)| {
            if weight <= 0 {
                return None;
            }
            let [Value::U64(id), Value::U64(src), Value::U64(dst)] = values.as_slice() else {
                panic!("expected id/src/dst row, got {values:?}");
            };
            Some((*id, (*src, *dst)))
        })
        .collect()
}

fn mutate_pair_table(
    batch: &mut DatabaseBatch,
    table: &str,
    rows: &mut std::collections::BTreeMap<u64, (u64, u64)>,
    seed: u64,
    _step: usize,
) {
    let id = (seed % 24) + 1;
    let src = ((seed >> 12) % 8) + 1;
    let dst = ((seed >> 20) % 8) + 1;
    match rows.entry(id) {
        std::collections::btree_map::Entry::Occupied(mut entry) => {
            if seed & 1 == 0 {
                entry.insert((src, dst));
                batch.update(
                    table,
                    vec![Value::U64(id), Value::U64(src), Value::U64(dst)],
                );
            } else {
                entry.remove();
                batch.delete(table, PrimaryKeyValue::U64(id));
            }
        }
        std::collections::btree_map::Entry::Vacant(entry) => {
            entry.insert((src, dst));
            batch.insert(
                table,
                vec![Value::U64(id), Value::U64(src), Value::U64(dst)],
            );
        }
    }
}

fn apply_pair_deltas(
    materialized: &mut std::collections::BTreeMap<(u64, u64), i64>,
    deltas: RecordDeltas,
) {
    for (values, weight) in deltas.to_values().unwrap() {
        let [Value::U64(src), Value::U64(dst)] = values.as_slice() else {
            panic!("expected pair delta, got {values:?}");
        };
        *materialized.entry((*src, *dst)).or_default() += weight;
    }
    materialized.retain(|_, weight| *weight != 0);
}

fn assert_subscription_matches_oracle(
    subscription: &OracleSubscription,
    edges: &std::collections::BTreeMap<u64, (u64, u64)>,
    blockers: &std::collections::BTreeMap<u64, (u64, u64)>,
    seed: u64,
    step: usize,
) {
    let expected = match subscription.graph {
        OracleGraph::Reach => transitive_closure(edges),
        OracleGraph::TwoHop => two_hop_pairs(edges),
        OracleGraph::UnblockedEdges => unblocked_edges(edges, blockers),
    };
    assert_eq!(
        subscription.materialized, expected,
        "subscription mismatch for {:?} created at step {} after generated graph seed {seed:#x} step {step}; edges={edges:?}; blockers={blockers:?}",
        subscription.graph, subscription.created_step
    );
}

fn unblocked_edges(
    edges: &std::collections::BTreeMap<u64, (u64, u64)>,
    blockers: &std::collections::BTreeMap<u64, (u64, u64)>,
) -> std::collections::BTreeMap<(u64, u64), i64> {
    let blocker_counts = direct_edge_multiset(blockers);
    let mut pairs = std::collections::BTreeMap::new();
    for edge in edges.values() {
        if blocker_counts.get(edge).copied().unwrap_or_default() == 0 {
            *pairs.entry(*edge).or_default() += 1;
        }
    }
    pairs
}

fn direct_edges(
    edges: &std::collections::BTreeMap<u64, (u64, u64)>,
) -> std::collections::BTreeMap<(u64, u64), i64> {
    edges
        .values()
        .map(|edge| (*edge, 1))
        .collect::<std::collections::BTreeMap<_, _>>()
}

fn direct_edge_multiset(
    edges: &std::collections::BTreeMap<u64, (u64, u64)>,
) -> std::collections::BTreeMap<(u64, u64), i64> {
    let mut pairs = std::collections::BTreeMap::new();
    for edge in edges.values() {
        *pairs.entry(*edge).or_default() += 1;
    }
    pairs
}

fn pairs_from_deltas(deltas: RecordDeltas) -> std::collections::BTreeMap<(u64, u64), i64> {
    let mut pairs = std::collections::BTreeMap::new();
    apply_pair_deltas(&mut pairs, deltas);
    pairs
}

fn two_hop_pairs(
    edges: &std::collections::BTreeMap<u64, (u64, u64)>,
) -> std::collections::BTreeMap<(u64, u64), i64> {
    let mut pairs = std::collections::BTreeMap::new();
    for (left_src, left_dst) in edges.values() {
        for (right_src, right_dst) in edges.values() {
            if left_dst == right_src {
                *pairs.entry((*left_src, *right_dst)).or_default() += 1;
            }
        }
    }
    pairs.retain(|_, weight| *weight != 0);
    pairs
}

fn transitive_closure(
    edges: &std::collections::BTreeMap<u64, (u64, u64)>,
) -> std::collections::BTreeMap<(u64, u64), i64> {
    let mut closure = direct_edges(edges);
    let mut changed = true;
    while changed {
        changed = false;
        let known = closure.keys().copied().collect::<Vec<_>>();
        for (src, mid) in &known {
            for (edge_src, edge_dst) in edges.values() {
                if mid == edge_src && !closure.contains_key(&(*src, *edge_dst)) {
                    closure.insert((*src, *edge_dst), 1);
                    changed = true;
                }
            }
        }
    }
    closure
}
