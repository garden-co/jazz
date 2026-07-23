//! Scratch repro: one-shot read latency at scale with default ordering.
//! Usage: cargo run -p jazz --example ordering_read_scale --release -- 10000

use std::collections::BTreeMap;
use std::time::Instant;

use jazz::block_on;
use jazz::db::{Db, DbConfig, DbIdentity, ReadOpts, RowCells, SeededRowIdSource};
use jazz::groove::records::Value;
use jazz::groove::schema::{ColumnSchema, ColumnType};
use jazz::groove::storage::{MemoryStorage, NativeBtreeStorage};
use jazz::ids::{AuthorId, NodeUuid};
use jazz::schema::{JazzSchema, Policy, TableSchema};

const BATCH_SIZE: usize = 500;

fn todo_table() -> TableSchema {
    TableSchema::new(
        "todos",
        [
            ColumnSchema::new("title", ColumnType::String),
            ColumnSchema::new("done", ColumnType::Bool),
            ColumnSchema::new("projectId", ColumnType::Uuid.nullable()),
        ],
    )
    .with_reference("projectId", "projects")
    .with_read_policy(Policy::public())
    .with_write_policy(Policy::public())
}

fn project_table() -> TableSchema {
    TableSchema::new("projects", [ColumnSchema::new("name", ColumnType::String)])
        .with_read_policy(Policy::public())
        .with_write_policy(Policy::public())
}

fn todo_cells(title: String, done: bool) -> RowCells {
    BTreeMap::from([
        ("title".to_owned(), Value::String(title)),
        ("done".to_owned(), Value::Bool(done)),
    ])
}

enum ScaleStorage {
    Memory(MemoryStorage),
    Btree(NativeBtreeStorage),
}

fn make_storage(kind: &str) -> Result<ScaleStorage, Box<dyn std::error::Error>> {
    let schema = JazzSchema::new([todo_table(), project_table()]);
    let column_families = schema.column_families();
    let refs = column_families
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    Ok(match kind {
        "btree" => {
            let path = std::env::temp_dir()
                .join(format!("ordering-read-scale-{}.btree", std::process::id()));
            let _ = std::fs::remove_file(&path);
            ScaleStorage::Btree(NativeBtreeStorage::open(&path, &refs)?)
        }
        _ => ScaleStorage::Memory(MemoryStorage::new(&refs)),
    })
}

fn open_db<S>(storage: S) -> Result<Db<S>, Box<dyn std::error::Error>>
where
    S: jazz::groove::storage::OrderedKvStorage + jazz::groove::storage::ReopenableStorage + 'static,
{
    let schema = JazzSchema::new([todo_table(), project_table()]);
    Ok(block_on(Db::open(DbConfig {
        schema,
        storage,
        identity: DbIdentity {
            node: NodeUuid::from_bytes([0x44; 16]),
            author: AuthorId::from_bytes([0x55; 16]),
        },
        id_source: if std::env::var("SCALE_SEEDED_IDS").is_ok() {
            Some(Box::new(SeededRowIdSource::new(0x7100)))
        } else {
            None
        },
        large_value_checkpoint_op_interval: 1024,
    }))?)
}

fn run_scale<S>(db: Db<S>, max_rows: usize) -> Result<(), Box<dyn std::error::Error>>
where
    S: jazz::groove::storage::OrderedKvStorage + jazz::groove::storage::ReopenableStorage + 'static,
{
    let project_count = (max_rows / 6).max(1);
    let seed_start = Instant::now();
    let mut project_ids = Vec::with_capacity(project_count);
    for batch_start in (0..project_count).step_by(BATCH_SIZE) {
        let mut tx = db.mergeable_tx();
        for index in batch_start..(batch_start + BATCH_SIZE).min(project_count) {
            let id = tx.insert(
                "projects",
                BTreeMap::from([("name".to_owned(), Value::String(format!("project {index}")))]),
            )?;
            project_ids.push(id);
        }
        tx.commit()?;
    }
    println!(
        "seeded {project_count} projects in {:?}",
        seed_start.elapsed()
    );

    let insert_start = Instant::now();
    let mut last_batch_report = Instant::now();
    for batch in 0..(max_rows / BATCH_SIZE) {
        let mut tx = db.mergeable_tx();
        for row in 0..BATCH_SIZE {
            let index = batch * BATCH_SIZE + row;
            let mut cells = todo_cells(format!("todo {index:06}"), index % 3 == 0);
            cells.insert(
                "projectId".to_owned(),
                Value::Nullable(Some(Box::new(Value::Uuid(
                    project_ids[index % project_ids.len()].0,
                )))),
            );
            tx.insert("todos", cells)?;
        }
        let commit_start = Instant::now();
        tx.commit()?;
        if std::env::var("SCALE_TICK").is_ok() {
            db.tick()?;
        }
        if batch % 20 == 0 || last_batch_report.elapsed().as_secs() >= 5 {
            println!(
                "  batch {batch} ({} rows in): commit {:?}",
                batch * BATCH_SIZE,
                commit_start.elapsed()
            );
            last_batch_report = Instant::now();
        }
    }
    println!("inserted {max_rows} todos in {:?}", insert_start.elapsed());

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

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let max_rows = std::env::args()
        .nth(1)
        .map(|arg| arg.parse::<usize>())
        .transpose()?
        .unwrap_or(10_000);
    let storage_kind = std::env::args()
        .nth(2)
        .unwrap_or_else(|| "memory".to_owned());
    println!("storage: {storage_kind}");
    match make_storage(&storage_kind)? {
        ScaleStorage::Memory(storage) => run_scale(open_db(storage)?, max_rows),
        ScaleStorage::Btree(storage) => run_scale(open_db(storage)?, max_rows),
    }
}
