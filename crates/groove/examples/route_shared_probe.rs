//! Temporary diagnostic: same route workload as direct graphs and one routed shape.
use std::time::Instant;

use futures::executor::block_on;
use groove::db::{Database, GraphBuilder, PredicateExpr};
use groove::ivm::{ProjectField, TopByLimit, TopByOrder};
use groove::records::{RecordDescriptor, Value};
use groove::schema::{
    ColumnSchema, ColumnType, DatabaseSchema, IntegerKeyType, PrimaryKey, TableSchema,
};
use groove::storage::MemoryStorage;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let mode = args.next().expect("direct, shared, or single");
    let routes: u64 = args.next().expect("route count").parse()?;
    assert!((1..=1000).contains(&routes));
    block_on(run(&mode, routes))
}

async fn run(mode: &str, routes: u64) -> Result<(), Box<dyn std::error::Error>> {
    let schema = DatabaseSchema::new([TableSchema::new(
        "docs",
        [
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("team", ColumnType::U64),
            ColumnSchema::new("updated_at", ColumnType::U64),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))]);
    let mut db = Database::new(schema, MemoryStorage::new(&["docs"])?).await?;
    let mut seed = db.open_batch();
    for ordinal in 0..1000 {
        seed.insert(
            "docs",
            vec![Value::U64(ordinal), Value::U64(0), Value::U64(ordinal)],
        );
    }
    for team in 1..=1000 {
        seed.insert(
            "docs",
            vec![Value::U64(team * 2000), Value::U64(team), Value::U64(0)],
        );
    }
    let applied = db.apply_batch(seed).await?;
    let persisted = applied.persist().await;
    db.finish_persistence(persisted)?;

    let start = Instant::now();
    let mut subscriptions = Vec::new();
    match mode {
        "direct" => {
            for team in 0..routes {
                let graph = GraphBuilder::top_by(
                    GraphBuilder::table("docs").filter(PredicateExpr::eq("team", Value::U64(team))),
                    ["team"],
                    [TopByOrder::desc("updated_at")],
                    ["id"],
                    0,
                    TopByLimit::Finite(100),
                );
                let subscription = db.subscribe_one_sink(graph).await?;
                let initial = subscription.try_recv()?;
                let expected = if team == 0 { 100 } else { 1 };
                assert_eq!(initial.to_values()?.len(), expected);
                subscriptions.push(subscription);
            }
        }
        "shared" => {
            let descriptor = RecordDescriptor::new([("team", ColumnType::U64)]);
            let joined = GraphBuilder::join(
                GraphBuilder::binding_source("route-probe", descriptor),
                GraphBuilder::table("docs"),
                ["team"],
                ["team"],
            )
            .project_fields([
                ProjectField::renamed("left.team", "route_team"),
                ProjectField::renamed("right.id", "id"),
                ProjectField::renamed("right.team", "team"),
                ProjectField::renamed("right.updated_at", "updated_at"),
            ]);
            let graph = GraphBuilder::top_by(
                joined,
                ["route_team"],
                [TopByOrder::desc("updated_at")],
                ["id"],
                0,
                TopByLimit::Finite(100),
            );
            let shape = db
                .prepare_one_sink(graph, "route-probe", descriptor, ["route_team"])
                .await?;
            for team in 0..routes {
                let subscription = db
                    .bind_shape_one_sink(shape.id(), &[Value::U64(team)])
                    .await?;
                let initial = subscription.try_recv()?;
                let expected = if team == 0 { 100 } else { 1 };
                assert_eq!(initial.to_values()?.len(), expected);
                subscriptions.push(subscription);
            }
        }
        "single" => {
            let descriptor = RecordDescriptor::new([("team", ColumnType::U64)]);
            let binding_rows = (0..routes).map(|team| [Value::U64(team)]);
            let bindings = GraphBuilder::values(descriptor, binding_rows)?;
            let joined =
                GraphBuilder::join(bindings, GraphBuilder::table("docs"), ["team"], ["team"])
                    .project_fields([
                        ProjectField::renamed("left.team", "route_team"),
                        ProjectField::renamed("right.id", "id"),
                        ProjectField::renamed("right.team", "team"),
                        ProjectField::renamed("right.updated_at", "updated_at"),
                    ]);
            let graph = GraphBuilder::top_by(
                joined,
                ["route_team"],
                [TopByOrder::desc("updated_at")],
                ["id"],
                0,
                TopByLimit::Finite(100),
            );
            let subscription = db.subscribe_one_sink(graph).await?;
            let initial = subscription.try_recv()?;
            assert_eq!(initial.to_values()?.len(), routes as usize + 99);
            subscriptions.push(subscription);
        }
        _ => panic!("direct, shared, or single"),
    }
    let hydration_us = start.elapsed().as_micros();
    let stats = db.runtime_stats();
    let mut matching = db.open_batch();
    matching.insert(
        "docs",
        vec![Value::U64(1001), Value::U64(0), Value::U64(1001)],
    );
    let start = Instant::now();
    let applied = db.apply_batch(matching).await?;
    let persisted = applied.persist().await;
    db.finish_persistence(persisted)?;
    let matching_us = start.elapsed().as_micros();
    assert_eq!(subscriptions[0].try_recv()?.to_values()?.len(), 2);
    for subscription in subscriptions.iter().skip(1) {
        while let Ok(delta) = subscription.try_recv() {
            assert!(delta.is_empty());
        }
    }
    let mut writes = Vec::new();
    for ordinal in 1..=20 {
        let mut batch = db.open_batch();
        batch.insert(
            "docs",
            vec![
                Value::U64(2_000_000 + ordinal),
                Value::U64(1000),
                Value::U64(ordinal),
            ],
        );
        let start = Instant::now();
        let applied = db.apply_batch(batch).await?;
        let persisted = applied.persist().await;
        db.finish_persistence(persisted)?;
        writes.push(start.elapsed().as_micros());
    }
    for subscription in &subscriptions {
        assert!(
            subscription.try_recv().is_err(),
            "unrelated write emitted a route delta"
        );
    }
    writes.sort_unstable();
    println!(
        "mode={mode} routes={routes} hydration_us={hydration_us} nodes={} arrangements={} matching_us={matching_us} unrelated_p50_us={} unrelated_p95_us={} writes={writes:?}",
        stats.graph_nodes,
        stats.arrangement_count,
        writes[writes.len() / 2],
        writes[writes.len() * 19 / 20]
    );
    Ok(())
}
