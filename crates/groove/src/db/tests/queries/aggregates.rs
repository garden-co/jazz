//! Incremental aggregate hydration and maintenance.

use super::*;

fn metric_schema() -> DatabaseSchema {
    DatabaseSchema::new([TableSchema::new(
        "metrics",
        [
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("bucket", ColumnType::U64),
            ColumnSchema::new("score", ColumnType::U64),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))])
}

fn metric_values(id: u64, bucket: u64, score: u64) -> Vec<Value> {
    vec![Value::U64(id), Value::U64(bucket), Value::U64(score)]
}

fn nullable_metric(value: Value) -> Value {
    Value::Nullable(Some(Box::new(value)))
}

fn metric_aggregate_graph(input: GraphBuilder) -> GraphBuilder {
    GraphBuilder::aggregate(
        input,
        ["bucket"],
        [
            AggregateExpr {
                function: AggregateFunction::Count,
                expression: None,
                distinct: false,
                output_name: Some("count".to_owned()),
            },
            AggregateExpr {
                function: AggregateFunction::Sum,
                expression: Some(PlanExpr::Field("score".to_owned())),
                distinct: false,
                output_name: Some("sum_score".to_owned()),
            },
            AggregateExpr {
                function: AggregateFunction::Avg,
                expression: Some(PlanExpr::Field("score".to_owned())),
                distinct: false,
                output_name: Some("avg_score".to_owned()),
            },
            AggregateExpr {
                function: AggregateFunction::Min,
                expression: Some(PlanExpr::Field("score".to_owned())),
                distinct: false,
                output_name: Some("min_score".to_owned()),
            },
            AggregateExpr {
                function: AggregateFunction::Max,
                expression: Some(PlanExpr::Field("score".to_owned())),
                distinct: false,
                output_name: Some("max_score".to_owned()),
            },
        ],
    )
}

fn metric_aggregate_table_graph() -> GraphBuilder {
    metric_aggregate_graph(GraphBuilder::table("metrics"))
}

fn sorted_values(mut values: Vec<(Vec<Value>, i64)>) -> Vec<(Vec<Value>, i64)> {
    values.sort_by(|left, right| format!("{:?}", left.0).cmp(&format!("{:?}", right.0)));
    values
}

fn materialized_values(
    materialized: &std::collections::BTreeMap<String, (Vec<Value>, i64)>,
) -> Vec<(Vec<Value>, i64)> {
    sorted_values(
        materialized
            .values()
            .filter_map(|(values, weight)| (*weight != 0).then_some((values.clone(), *weight)))
            .collect(),
    )
}

fn apply_materialized(
    materialized: &mut std::collections::BTreeMap<String, (Vec<Value>, i64)>,
    deltas: RecordDeltas,
) {
    for (values, weight) in deltas.to_values().unwrap() {
        let key = format!("{values:?}");
        let entry = materialized.entry(key).or_insert((values, 0));
        entry.1 += weight;
    }
    materialized.retain(|_, (_, weight)| *weight != 0);
}

#[futures_test::test]
async fn aggregate_hydrates_and_updates_group_summaries() {
    let storage = MemoryStorage::new(&["metrics"]).expect("valid memory storage families");
    let mut database = Database::new(metric_schema(), storage).await.unwrap();
    let subscription = database
        .subscribe_one_sink(metric_aggregate_table_graph())
        .await
        .unwrap();
    assert!(subscription.recv().unwrap().is_empty());

    let mut batch = database.open_batch();
    batch.insert("metrics", metric_values(1, 10, 5));
    batch.insert("metrics", metric_values(2, 10, 7));
    batch.insert("metrics", metric_values(3, 20, 11));
    database.commit_batch(batch).await.unwrap();
    assert_eq!(
        sorted_values(subscription.recv().unwrap().to_values().unwrap()),
        [
            (
                vec![
                    Value::U64(10),
                    Value::U64(2),
                    nullable_metric(Value::U64(12)),
                    nullable_metric(Value::F64(6.0)),
                    nullable_metric(Value::U64(5)),
                    nullable_metric(Value::U64(7)),
                ],
                1,
            ),
            (
                vec![
                    Value::U64(20),
                    Value::U64(1),
                    nullable_metric(Value::U64(11)),
                    nullable_metric(Value::F64(11.0)),
                    nullable_metric(Value::U64(11)),
                    nullable_metric(Value::U64(11)),
                ],
                1,
            ),
        ]
    );

    let mut batch = database.open_batch();
    batch.update("metrics", metric_values(2, 10, 3));
    database.commit_batch(batch).await.unwrap();
    assert_eq!(
        sorted_values(subscription.recv().unwrap().to_values().unwrap()),
        [
            (
                vec![
                    Value::U64(10),
                    Value::U64(2),
                    nullable_metric(Value::U64(12)),
                    nullable_metric(Value::F64(6.0)),
                    nullable_metric(Value::U64(5)),
                    nullable_metric(Value::U64(7)),
                ],
                -1,
            ),
            (
                vec![
                    Value::U64(10),
                    Value::U64(2),
                    nullable_metric(Value::U64(8)),
                    nullable_metric(Value::F64(4.0)),
                    nullable_metric(Value::U64(3)),
                    nullable_metric(Value::U64(5)),
                ],
                1,
            ),
        ]
    );

    let mut batch = database.open_batch();
    batch.delete("metrics", PrimaryKeyValue::U64(2));
    batch.delete("metrics", PrimaryKeyValue::U64(1));
    database.commit_batch(batch).await.unwrap();
    assert_eq!(
        sorted_values(subscription.recv().unwrap().to_values().unwrap()),
        [(
            vec![
                Value::U64(10),
                Value::U64(2),
                nullable_metric(Value::U64(8)),
                nullable_metric(Value::F64(4.0)),
                nullable_metric(Value::U64(3)),
                nullable_metric(Value::U64(5)),
            ],
            -1,
        )]
    );
}
#[futures_test::test]
async fn aggregate_counts_weighted_multiplicity_from_bag_union() {
    let storage = MemoryStorage::new(&["metrics"]).expect("valid memory storage families");
    let mut database = Database::new(metric_schema(), storage).await.unwrap();
    let graph = metric_aggregate_graph(GraphBuilder::union([
        GraphBuilder::table("metrics"),
        GraphBuilder::table("metrics"),
    ]));
    let subscription = database.subscribe_one_sink(graph).await.unwrap();
    assert!(subscription.recv().unwrap().is_empty());

    let mut batch = database.open_batch();
    batch.insert("metrics", metric_values(1, 10, 5));
    batch.insert("metrics", metric_values(2, 10, 7));
    database.commit_batch(batch).await.unwrap();

    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap(),
        [(
            vec![
                Value::U64(10),
                Value::U64(4),
                nullable_metric(Value::U64(24)),
                nullable_metric(Value::F64(6.0)),
                nullable_metric(Value::U64(5)),
                nullable_metric(Value::U64(7)),
            ],
            1,
        )]
    );
}

#[futures_test::test]
async fn aggregate_incremental_matches_recompute_under_seeded_changes() {
    let storage = MemoryStorage::new(&["metrics"]).expect("valid memory storage families");
    let mut database = Database::new(metric_schema(), storage).await.unwrap();
    let graph = metric_aggregate_table_graph();
    let subscription = database.subscribe_one_sink(graph.clone()).await.unwrap();
    assert!(subscription.recv().unwrap().is_empty());

    let mut materialized = std::collections::BTreeMap::new();
    let mut live = std::collections::BTreeMap::<u64, (u64, u64)>::new();
    let mut seed = 0x5eed_u64;
    for step in 0..96 {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        let id = seed % 12;
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        let op = seed % 5;
        let mut batch = database.open_batch();
        if op == 0 && live.contains_key(&id) {
            batch.delete("metrics", PrimaryKeyValue::U64(id));
            live.remove(&id);
        } else {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            let bucket = 1 + (seed % 4);
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            let score = 1 + (seed % 31);
            if live.contains_key(&id) {
                batch.update("metrics", metric_values(id, bucket, score));
            } else {
                batch.insert("metrics", metric_values(id, bucket, score));
            }
            live.insert(id, (bucket, score));
        }
        database.commit_batch(batch).await.unwrap();
        match subscription.try_recv() {
            Ok(emitted) => apply_materialized(&mut materialized, emitted),
            Err(TryRecvError::Empty) => {}
            Err(TryRecvError::Disconnected) => panic!("aggregate subscription disconnected"),
        }
        let recomputed = sorted_values(
            database
                .query_graph(graph.clone())
                .await
                .unwrap()
                .to_values()
                .unwrap(),
        );
        assert_eq!(
            materialized_values(&materialized),
            recomputed,
            "step {step}, live {live:?}"
        );
    }
}

#[futures_test::test]
async fn aggregate_query_hydration_does_not_perturb_subscription_deltas() {
    let storage = MemoryStorage::new(&["metrics"]).expect("valid memory storage families");
    let mut database = Database::new(metric_schema(), storage).await.unwrap();
    let graph = metric_aggregate_table_graph();
    let subscription = database.subscribe_one_sink(graph.clone()).await.unwrap();
    assert!(subscription.recv().unwrap().is_empty());
    assert!(
        database
            .query_graph(graph.clone())
            .await
            .unwrap()
            .is_empty()
    );

    let mut batch = database.open_batch();
    batch.insert("metrics", metric_values(1, 10, 5));
    database.commit_batch(batch).await.unwrap();
    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap(),
        [(
            vec![
                Value::U64(10),
                Value::U64(1),
                nullable_metric(Value::U64(5)),
                nullable_metric(Value::F64(5.0)),
                nullable_metric(Value::U64(5)),
                nullable_metric(Value::U64(5)),
            ],
            1,
        )]
    );

    let first = sorted_values(
        database
            .query_graph(graph.clone())
            .await
            .unwrap()
            .to_values()
            .unwrap(),
    );
    let second = sorted_values(
        database
            .query_graph(graph)
            .await
            .unwrap()
            .to_values()
            .unwrap(),
    );
    assert_eq!(first, second);
}
