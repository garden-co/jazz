//! Bench-only native repro for commit-size attribution.
//!
//! Run with:
//! `cargo run -p jazz --example commit_superlinearity_native --release -- 10000`

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use jazz::block_on;
use jazz::db::{
    Db, DbConfig, DbIdentity, LocalUpdates, Propagation, ReadOpts, SeededRowIdSource,
    SubscriptionEvent,
};
use jazz::groove::records::Value;
use jazz::groove::schema::{ColumnSchema, ColumnType};
use jazz::groove::storage::MemoryStorage;
use jazz::ids::{AuthorId, NodeUuid, RowUuid};
use jazz::query::Query;
use jazz::schema::{JazzSchema, Policy, TableSchema};
use jazz::tx::DurabilityTier;

const BATCH_SIZE: usize = 500;

#[derive(Debug)]
struct BatchSample {
    batch: usize,
    rows: usize,
    staging_ms: f64,
    commit_ms: f64,
    drain_ms: f64,
    events: usize,
}

fn main() {
    let max_rows = std::env::args()
        .nth(1)
        .and_then(|arg| arg.parse::<usize>().ok())
        .unwrap_or(10_000);

    println!("scenario,batch,rows,staging_ms,commit_ms,drain_ms,events");
    for sample in run_scenario(max_rows, false) {
        print_sample("unsub", &sample);
    }
    for sample in run_scenario(max_rows, true) {
        print_sample("sub", &sample);
    }
}

fn print_sample(scenario: &str, sample: &BatchSample) {
    println!(
        "{},{},{},{:.3},{:.3},{:.3},{}",
        scenario,
        sample.batch,
        sample.rows,
        sample.staging_ms,
        sample.commit_ms,
        sample.drain_ms,
        sample.events
    );
}

fn run_scenario(max_rows: usize, subscribed: bool) -> Vec<BatchSample> {
    let db = open_db(if subscribed { 2 } else { 1 });
    let mut stream = subscribed.then(|| {
        let query = Query::from("todos");
        let prepared = db.prepare_query(&query).expect("prepare todos query");
        let opts = ReadOpts {
            tier: DurabilityTier::Local,
            local_updates: LocalUpdates::Immediate,
            propagation: Propagation::Full,
            include_deleted: false,
            ..ReadOpts::default()
        };
        block_on(db.subscribe(&prepared, opts)).expect("subscribe todos")
    });
    if let Some(stream) = stream.as_mut() {
        drain_ready(stream);
    }

    let mut samples = Vec::new();
    let batches = max_rows.div_ceil(BATCH_SIZE);
    for batch in 0..batches {
        let start = batch * BATCH_SIZE;
        let end = (start + BATCH_SIZE).min(max_rows);
        let mut tx = db.mergeable_tx();

        let staging_start = Instant::now();
        for index in start..end {
            tx.insert_with_id("todos", row(index as u64), todo_cells(index))
                .expect("stage todo insert");
        }
        let staging_ms = ms(staging_start.elapsed());

        let commit_start = Instant::now();
        tx.commit().expect("commit todo batch");
        let commit_ms = ms(commit_start.elapsed());

        let drain_start = Instant::now();
        let events = stream.as_mut().map(drain_ready).unwrap_or(0);
        let drain_ms = ms(drain_start.elapsed());

        samples.push(BatchSample {
            batch,
            rows: end,
            staging_ms,
            commit_ms,
            drain_ms,
            events,
        });
    }
    samples
}

fn open_db(seed: u8) -> Db<MemoryStorage> {
    let schema = schema();
    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = MemoryStorage::new(&refs);
    block_on(Db::open(DbConfig {
        schema,
        storage,
        identity: DbIdentity {
            node: NodeUuid::from_bytes([seed; 16]),
            author: AuthorId::from_bytes([seed + 10; 16]),
        },
        id_source: Some(Box::new(SeededRowIdSource::new(seed as u64))),
        large_value_checkpoint_op_interval: 1024,
    }))
    .expect("open db")
}

fn schema() -> JazzSchema {
    JazzSchema::new([TableSchema::new(
        "todos",
        [
            ColumnSchema::new("title", ColumnType::String),
            ColumnSchema::new("completed", ColumnType::Bool),
            ColumnSchema::new("priority", ColumnType::U32),
            ColumnSchema::new("note", ColumnType::String),
        ],
    )
    .with_read_policy(Policy::public())
    .with_write_policy(Policy::public())])
}

fn todo_cells(index: usize) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("title".to_owned(), Value::String(format!("todo-{index}"))),
        ("completed".to_owned(), Value::Bool(false)),
        ("priority".to_owned(), Value::U32((index % 5) as u32)),
        ("note".to_owned(), Value::String("native repro".to_owned())),
    ])
}

fn row(index: u64) -> RowUuid {
    let mut bytes = [0_u8; 16];
    bytes[0..8].copy_from_slice(&index.to_be_bytes());
    RowUuid::from_bytes(bytes)
}

fn drain_ready(stream: &mut jazz::db::SubscriptionStream) -> usize {
    let mut count = 0;
    loop {
        match stream.try_next_event() {
            Some(SubscriptionEvent::Delta { .. }) | Some(SubscriptionEvent::Closed) => count += 1,
            None => break,
        }
    }
    count
}

fn ms(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1000.0
}
