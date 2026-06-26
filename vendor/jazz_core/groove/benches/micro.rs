use std::env;
use std::time::{Duration, Instant};

use groove::db::{Database, GraphBuilder, PredicateExpr};
use groove::queries::{BinaryOp, Expr, Query, Select, SelectItem, TableRef};
use groove::records::{RecordDescriptor, Value};
use groove::schema::{
    ColumnSchema, ColumnType, DatabaseSchema, IndexSchema, IntegerKeyType, PrimaryKey, TableSchema,
};
use groove::storage::{Durability, RocksDbStorage};
use hdrhistogram::Histogram;

fn main() {
    let iterations = env_usize("GROOVE_MICRO_ITERS", 1_000);
    let report = [
        run_record_encode_decode(iterations),
        run_query_planning(iterations),
        run_subscribe_unsubscribe(iterations.min(500)),
        run_indexed_commit(iterations.min(500)),
    ];
    println!("[{}]", report.join(","));
}

fn run_record_encode_decode(iterations: usize) -> String {
    let descriptor = RecordDescriptor::new([
        ("id", ColumnType::U64.value_type()),
        ("artist_id", ColumnType::U64.value_type()),
        ("title", ColumnType::String.value_type()),
    ]);
    let mut hist = Histogram::<u64>::new(3).expect("hist");
    let mut bytes = 0usize;
    for i in 0..iterations {
        let start = Instant::now();
        let record = descriptor
            .create(&[
                Value::U64(i as u64),
                Value::U64((i % 128) as u64),
                Value::String(format!("record-{i}")),
            ])
            .expect("record");
        let _values = descriptor.bind(&record).to_values().expect("decode");
        bytes += record.len();
        hist.record(duration_nanos(start.elapsed()))
            .expect("record");
    }
    json_case("record_encode_decode", iterations, &hist, bytes)
}

fn run_query_planning(iterations: usize) -> String {
    let schema = albums_schema();
    let mut hist = Histogram::<u64>::new(3).expect("hist");
    for i in 0..iterations {
        let query = Query::Select(Box::new(
            Select::new([SelectItem::expr(Expr::column("title"))])
                .from([TableRef::named("albums")])
                .where_(Expr::binary(
                    Expr::column("artist_id"),
                    BinaryOp::Eq,
                    Expr::Literal(Value::U64((i % 128) as u64)),
                )),
        ));
        let start = Instant::now();
        let _planned = groove::ivm::plan_query(&query, &schema).expect("plan");
        hist.record(duration_nanos(start.elapsed()))
            .expect("record");
    }
    json_case("query_planning", iterations, &hist, 0)
}

fn run_subscribe_unsubscribe(iterations: usize) -> String {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let storage =
        RocksDbStorage::open_with_durability(temp_dir.path(), &["albums"], Durability::WalNoSync)
            .expect("storage");
    let mut db = Database::new(albums_schema(), storage).expect("db");
    let mut hist = Histogram::<u64>::new(3).expect("hist");
    for i in 0..iterations {
        let graph = GraphBuilder::table("albums")
            .filter(PredicateExpr::eq("artist_id", Value::U64((i % 128) as u64)))
            .project(["title"]);
        let start = Instant::now();
        let subscription = db.subscribe(graph).expect("subscribe");
        let _initial = subscription.recv().expect("initial");
        db.unsubscribe(subscription.id());
        hist.record(duration_nanos(start.elapsed()))
            .expect("record");
    }
    json_case("subscribe_unsubscribe", iterations, &hist, 0)
}

fn run_indexed_commit(iterations: usize) -> String {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let storage = RocksDbStorage::open_with_durability(
        temp_dir.path(),
        &["albums", "indices"],
        Durability::WalNoSync,
    )
    .expect("storage");
    let mut db = Database::new(indexed_albums_schema(), storage).expect("db");
    let mut hist = Histogram::<u64>::new(3).expect("hist");
    for i in 0..iterations {
        let mut batch = db.open_batch();
        batch.insert(
            "albums",
            vec![
                Value::U64(i as u64),
                Value::U64((i % 128) as u64),
                Value::String(format!("record-{i}")),
            ],
        );
        let start = Instant::now();
        db.commit_batch(batch).expect("commit");
        hist.record(duration_nanos(start.elapsed()))
            .expect("record");
    }
    json_case("indexed_commit", iterations, &hist, 0)
}

fn albums_schema() -> DatabaseSchema {
    DatabaseSchema::new([TableSchema::new(
        "albums",
        [
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("artist_id", ColumnType::U64),
            ColumnSchema::new("title", ColumnType::String),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))])
}

fn indexed_albums_schema() -> DatabaseSchema {
    DatabaseSchema::new([TableSchema::new(
        "albums",
        [
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("artist_id", ColumnType::U64),
            ColumnSchema::new("title", ColumnType::String),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))
    .with_index(IndexSchema::new("albums_by_artist", ["artist_id"]))])
}

fn json_case(name: &str, iterations: usize, histogram: &Histogram<u64>, bytes: usize) -> String {
    format!(
        concat!(
            "{{",
            "\"case\":\"{}\",",
            "\"iterations\":{},",
            "\"nanos\":{{\"p50\":{},\"p95\":{},\"p99\":{},\"max\":{}}},",
            "\"bytes\":{}",
            "}}"
        ),
        name,
        iterations,
        histogram.value_at_quantile(0.50),
        histogram.value_at_quantile(0.95),
        histogram.value_at_quantile(0.99),
        histogram.max(),
        bytes
    )
}

fn env_usize(name: &str, default: usize) -> usize {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn duration_nanos(duration: Duration) -> u64 {
    duration.as_nanos().try_into().unwrap_or(u64::MAX)
}
