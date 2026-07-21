use std::collections::BTreeMap;
use std::time::Instant;

use jazz::db::{Db, DbConfig, DbIdentity, ReadOpts, SeededRowIdSource, block_on};
use jazz::groove::records::Value;
use jazz::groove::schema::{ColumnSchema, ColumnType};
use jazz::groove::storage::MemoryStorage;
use jazz::ids::{AuthorId, NodeUuid, RowUuid};
use jazz::query::Query;
use jazz::schema::{JazzSchema, Policy, TableSchema};
use jazz::tx::DurabilityTier;

type DirectDb = Db<MemoryStorage>;

const AUTHOR: AuthorId = AuthorId(uuid::uuid!("00000000-0000-0000-0000-0000000000a1"));

fn schema() -> JazzSchema {
    JazzSchema::new([TableSchema::new(
        "todos",
        [
            ColumnSchema::new("title", ColumnType::String),
            ColumnSchema::new("done", ColumnType::Bool),
            ColumnSchema::new("owner_id", ColumnType::Uuid),
        ],
    )
    .with_read_policy(Policy::public())
    .with_write_policy(Policy::public())])
}

fn open_db(seed: u64) -> DirectDb {
    let schema = schema();
    let column_families = schema.column_families();
    let refs = column_families
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    block_on(Db::open(
        DbConfig::new(
            schema,
            MemoryStorage::new(&refs),
            DbIdentity {
                node: NodeUuid::from_bytes([seed as u8; 16]),
                author: AUTHOR,
            },
        )
        .with_id_source(SeededRowIdSource::new(seed)),
    ))
    .expect("open probe db")
}

fn todo_cells(index: usize, done: bool) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("title".to_owned(), Value::String(format!("Todo {index:06}"))),
        ("done".to_owned(), Value::Bool(done)),
        ("owner_id".to_owned(), Value::Uuid(AUTHOR.0)),
    ])
}

fn seed(db: &DirectDb, rows: usize) -> Vec<RowUuid> {
    let mut ids = Vec::with_capacity(rows);
    for index in 0..rows {
        let write = db
            .insert("todos", todo_cells(index, false))
            .expect("insert todo");
        ids.push(write.row_uuid());
        block_on(write.wait(DurabilityTier::Local)).expect("seed local");
    }
    ids
}

fn main() {
    println!("stored,seed_ms,initial_read_ms,returned_rows,toggle_wait_ms,refresh_read_ms");
    for rows in [10_000usize, 50_000, 100_000] {
        let db = open_db(rows as u64);
        let seed_start = Instant::now();
        let ids = seed(&db, rows);
        let seed_ms = seed_start.elapsed().as_secs_f64() * 1000.0;

        let prepared = db
            .prepare_query(&Query::from("todos").limit(100))
            .expect("prepare limited query");
        let initial_start = Instant::now();
        let initial = db.read(&prepared).expect("initial limited read");
        let initial_read_ms = initial_start.elapsed().as_secs_f64() * 1000.0;

        let toggle_start = Instant::now();
        let write = db
            .update(
                "todos",
                ids[0],
                BTreeMap::from([("done".to_owned(), Value::Bool(true))]),
            )
            .expect("toggle todo");
        block_on(write.wait(DurabilityTier::Local)).expect("toggle local");
        let toggle_wait_ms = toggle_start.elapsed().as_secs_f64() * 1000.0;

        let refresh_start = Instant::now();
        let refreshed = db.read(&prepared).expect("fallback refresh read");
        let refresh_read_ms = refresh_start.elapsed().as_secs_f64() * 1000.0;
        assert_eq!(initial.len(), 100);
        assert_eq!(refreshed.len(), 100);
        println!(
            "{rows},{seed_ms:.3},{initial_read_ms:.3},{},{toggle_wait_ms:.3},{refresh_read_ms:.3}",
            refreshed.len()
        );
    }

    let reject_db = open_db(1);
    let rejected = reject_db
        .prepare_query(&Query::from("todos").limit(100))
        .and_then(|prepared| block_on(reject_db.subscribe(&prepared, ReadOpts::default())).map(|_| ()));
    println!("unsupported_limit100_subscribe_error={}", rejected.is_err());
}
