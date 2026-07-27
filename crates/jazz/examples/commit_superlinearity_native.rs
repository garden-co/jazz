use std::collections::BTreeMap;
use std::time::Instant;

use jazz::block_on;
use jazz::db::{
    Db, DbConfig, DbIdentity, ReadOpts, RowCells, SeededRowIdSource, SubscriptionEvent,
};
use jazz::groove::records::Value;
use jazz::groove::schema::{ColumnSchema, ColumnType};
use jazz::groove::storage::MemoryStorage;
use jazz::ids::{AuthorId, NodeUuid};
use jazz::schema::{JazzSchema, Policy, TableSchema};

const BATCH_SIZE: usize = 500;

fn todo_table() -> TableSchema {
    TableSchema::new(
        "todos",
        [
            ColumnSchema::new("title", ColumnType::String),
            ColumnSchema::new("done", ColumnType::Bool),
        ],
    )
    .with_read_policy(Policy::public())
    .with_write_policy(Policy::public())
}

fn todo_cells(title: String, done: bool) -> RowCells {
    BTreeMap::from([
        ("title".to_owned(), Value::String(title)),
        ("done".to_owned(), Value::Bool(done)),
    ])
}

fn open_db(seed: u64) -> Result<Db<MemoryStorage>, Box<dyn std::error::Error>> {
    let schema = JazzSchema::new([todo_table()]);
    let column_families = schema.column_families();
    let column_family_refs = column_families
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    let storage = MemoryStorage::new(&column_family_refs);
    Ok(block_on(Db::open(DbConfig {
        schema,
        storage,
        identity: DbIdentity {
            node: NodeUuid::from_bytes([0x44; 16]),
            author: AuthorId::from_bytes([0x55; 16]),
        },
        id_source: Some(Box::new(SeededRowIdSource::new(seed))),
        large_value_checkpoint_op_interval: 1024,
    }))?)
}

fn event_counts(event: Option<SubscriptionEvent>) -> (usize, usize, usize) {
    match event {
        Some(SubscriptionEvent::Delta {
            added,
            updated,
            removed,
            ..
        }) => (added.len(), updated.len(), removed.len()),
        _ => (0, 0, 0),
    }
}

fn run_case(max_rows: usize, subscribed: bool) -> Result<(), Box<dyn std::error::Error>> {
    let db = open_db(if subscribed { 0x5100 } else { 0x4100 })?;
    let query = db.prepare_query(&db.table("todos").select(["title", "done"]))?;
    let mut subscription = if subscribed {
        let mut subscription = block_on(db.subscribe(&query, ReadOpts::default()))?;
        let _ = block_on(subscription.next_event());
        Some(subscription)
    } else {
        None
    };

    let label = if subscribed { "sub" } else { "unsub" };
    println!("receipt_begin scenario={label} max_rows={max_rows} batch_size={BATCH_SIZE}");
    for batch in 0..(max_rows / BATCH_SIZE) {
        let rows_before = batch * BATCH_SIZE;
        let rows_after = rows_before + BATCH_SIZE;
        let mut tx = db.mergeable_tx();
        let stage_start = Instant::now();
        for row in rows_before..rows_after {
            tx.insert("todos", todo_cells(format!("todo {row:06}"), false))?;
        }
        let stage_ms = stage_start.elapsed().as_secs_f64() * 1000.0;

        let commit_start = Instant::now();
        let _tx_id = tx.commit()?;
        let commit_ms = commit_start.elapsed().as_secs_f64() * 1000.0;

        let drain_start = Instant::now();
        let (event_added, event_updated, event_removed) = match subscription.as_mut() {
            Some(subscription) => event_counts(block_on(subscription.next_event())),
            None => (0, 0, 0),
        };
        let drain_ms = drain_start.elapsed().as_secs_f64() * 1000.0;

        println!(
            "receipt scenario={label} batch={batch} rows={rows_after} stage_ms={stage_ms:.3} commit_ms={commit_ms:.3} drain_ms={drain_ms:.3} event_added={event_added} event_updated={event_updated} event_removed={event_removed}"
        );
    }
    println!("receipt_end scenario={label} max_rows={max_rows}");
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let max_rows = std::env::args()
        .nth(1)
        .map(|arg| arg.parse::<usize>())
        .transpose()?
        .unwrap_or(10_000);
    if max_rows == 0 || max_rows % BATCH_SIZE != 0 {
        return Err(format!("max rows must be a positive multiple of {BATCH_SIZE}").into());
    }

    run_case(max_rows, false)?;
    run_case(max_rows, true)?;
    Ok(())
}
