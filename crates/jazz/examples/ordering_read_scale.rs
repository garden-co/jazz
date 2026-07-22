//! Scratch repro: one-shot read latency at scale with default ordering.
//! Usage: cargo run -p jazz --example ordering_read_scale --release -- 10000

use std::collections::BTreeMap;
use std::time::Instant;

use jazz::block_on;
use jazz::db::{Db, DbConfig, DbIdentity, ReadOpts, RowCells, SeededRowIdSource};
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

fn open_db() -> Result<Db<MemoryStorage>, Box<dyn std::error::Error>> {
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
        id_source: Some(Box::new(SeededRowIdSource::new(0x7100))),
        large_value_checkpoint_op_interval: 1024,
    }))?)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let max_rows = std::env::args()
        .nth(1)
        .map(|arg| arg.parse::<usize>())
        .transpose()?
        .unwrap_or(10_000);

    let db = open_db()?;
    let insert_start = Instant::now();
    for batch in 0..(max_rows / BATCH_SIZE) {
        let mut tx = db.mergeable_tx();
        for row in 0..BATCH_SIZE {
            let index = batch * BATCH_SIZE + row;
            tx.insert(
                "todos",
                todo_cells(format!("todo {index:06}"), index % 3 == 0),
            )?;
        }
        tx.commit()?;
    }
    println!("inserted {max_rows} rows in {:?}", insert_start.elapsed());

    for (label, query) in [
        ("limit10", db.table("todos").limit(10)),
        ("limit1", db.table("todos").limit(1)),
        ("limit100", db.table("todos").limit(100)),
        ("unbounded", db.table("todos")),
    ] {
        let prepared = db.prepare_query(&query)?;
        let read_start = Instant::now();
        let rows = block_on(db.all(&prepared, ReadOpts::default()))?;
        println!("{label}: {} rows in {:?}", rows.len(), read_start.elapsed());
    }
    Ok(())
}
