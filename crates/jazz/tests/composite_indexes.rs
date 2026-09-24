//! Declared composite current indexes, exercised through the public schema
//! builder and the `Db` read/write API over durable RocksDB storage.

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::pin;
use std::task::{Context, Poll, Waker};

use jazz::db::{Db, DbConfig, DbIdentity, InsertOptions, ReadOpts, UpdateOptions};
use jazz::groove::records::Value;
use jazz::groove::storage::{LayoutStorage, OrderedKvStorage, StorageLayout};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{OrderDirection, Query, col, eq, lit};
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, Schema, SchemaBuilder, SchemaHash, TableSchemaBuilder};
use jazz_storage_rocksdb::RocksDbStorage;

mod common;

use common::allow_all_policies;

fn block_on<F: Future>(future: F) -> F::Output {
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    let mut future = pin!(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(value) => return value,
            Poll::Pending => std::thread::yield_now(),
        }
    }
}

fn row(byte: u8) -> RowUuid {
    RowUuid::from_bytes([byte; 16])
}

fn tasks_table() -> TableSchemaBuilder {
    TableSchemaBuilder::new("tasks")
        .column("owner", ColumnType::Text)
        .column("rank", ColumnType::Integer)
        .column("title", ColumnType::Text)
        .policies(allow_all_policies())
}

fn public_schema() -> Schema {
    SchemaBuilder::new()
        .table(tasks_table().composite_index(["owner", "rank"]))
        .build()
}

fn open_rocks_db(path: &std::path::Path, schema: &JazzSchema) -> Db<RocksDbStorage> {
    let families = schema.column_families();
    let storage = RocksDbStorage::open(
        path,
        &families.iter().map(String::as_str).collect::<Vec<_>>(),
    )
    .unwrap();
    block_on(Db::open(DbConfig::new(
        schema.clone(),
        storage,
        DbIdentity {
            node: NodeUuid::from_bytes([0xc1; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .unwrap()
}

fn insert_task(db: &Db<RocksDbStorage>, id: RowUuid, owner: &str, rank: i32, title: &str) {
    block_on(db.insert(
        "tasks",
        BTreeMap::from([
            ("owner".to_owned(), Value::String(owner.to_owned())),
            ("rank".to_owned(), Value::I32(rank)),
            ("title".to_owned(), Value::String(title.to_owned())),
        ]),
        InsertOptions {
            row_id: Some(id),
            ..Default::default()
        },
    ))
    .unwrap();
}

/// Titles of `owner`'s top two tasks by descending rank: the equality-prefix,
/// ordered-page shape a `(owner, rank)` composite index is declared for.
fn top_two_titles(db: &Db<RocksDbStorage>, schema: &JazzSchema, owner: &str) -> Vec<String> {
    let query = db
        .prepare_query(
            &Query::from("tasks")
                .filter(eq(col("owner"), lit(Value::String(owner.to_owned()))))
                .order_by("rank", OrderDirection::Desc)
                .limit(2),
        )
        .unwrap();
    let table = &schema.tables()[0];
    block_on(db.all(&query, ReadOpts::default()))
        .unwrap()
        .into_iter()
        .map(|row| match row.cell(table, "title") {
            Some(Value::String(title)) => title,
            other => panic!("expected a title, got {other:?}"),
        })
        .collect()
}

/// A table that declares a `(owner, rank)` composite index answers alice's
/// "top two by rank" page correctly, keeps answering it after rows move
/// between owners and ranks, and still does after a restart that recovers the
/// composite-index schema from its durable (v2) catalogue payload.
///
/// ```text
/// alice ──insert 3 tasks, bob 1──► db ──top2(alice)──► [ship, plan]
/// alice ──bob's task → alice rank 9; ship → rank 0──► top2(alice) = [review, plan]
/// db ──close──► reopen same directory ──top2(alice)──► [review, plan]
/// ```
#[test]
fn composite_index_schema_serves_ordered_pages_across_updates_and_reopen() {
    let schema = JazzSchema::new(&public_schema()).expect("composite schema compiles");
    let directory = tempfile::tempdir().unwrap();
    {
        let db = open_rocks_db(directory.path(), &schema);
        insert_task(&db, row(1), "alice", 3, "ship");
        insert_task(&db, row(2), "alice", 1, "draft");
        insert_task(&db, row(3), "alice", 2, "plan");
        insert_task(&db, row(4), "bob", 5, "review");
        assert_eq!(top_two_titles(&db, &schema, "alice"), ["ship", "plan"]);
        assert_eq!(top_two_titles(&db, &schema, "bob"), ["review"]);

        // Both indexed columns change: the row must leave bob's prefix and
        // enter alice's at its new position; `ship` must move down in place.
        block_on(db.update(
            "tasks",
            row(4),
            BTreeMap::from([
                ("owner".to_owned(), Value::String("alice".to_owned())),
                ("rank".to_owned(), Value::I32(9)),
            ]),
            UpdateOptions::default(),
        ))
        .unwrap();
        block_on(db.update(
            "tasks",
            row(1),
            BTreeMap::from([("rank".to_owned(), Value::I32(0))]),
            UpdateOptions::default(),
        ))
        .unwrap();
        assert_eq!(top_two_titles(&db, &schema, "alice"), ["review", "plan"]);
        assert!(top_two_titles(&db, &schema, "bob").is_empty());
    }

    let reopened = open_rocks_db(directory.path(), &schema);
    assert_eq!(
        top_two_titles(&reopened, &schema, "alice"),
        ["review", "plan"]
    );
    assert!(top_two_titles(&reopened, &schema, "bob").is_empty());
}

/// Composite indexes are part of schema identity, but only as a set: the
/// column order inside one index matters, the order in which a developer
/// declares distinct indexes does not, and schemas that differ only in
/// declaration order compare equal. Declarations the runtime could not
/// maintain as one ordered index are rejected when the schema is compiled.
#[test]
fn composite_index_declarations_are_canonical_and_validated() {
    let declared = |indexes: &[[&str; 2]]| {
        let mut table = tasks_table();
        for columns in indexes {
            table = table.composite_index(*columns);
        }
        SchemaBuilder::new().table(table).build()
    };
    let forward = declared(&[["owner", "rank"], ["rank", "owner"]]);
    let reversed = declared(&[["rank", "owner"], ["owner", "rank"]]);
    assert_eq!(forward, reversed);
    assert_eq!(
        SchemaHash::compute(&forward),
        SchemaHash::compute(&reversed)
    );
    assert_ne!(
        SchemaHash::compute(&declared(&[["owner", "rank"]])),
        SchemaHash::compute(&declared(&[["rank", "owner"]])),
        "column order inside an index is identity"
    );
    assert_ne!(
        SchemaHash::compute(&declared(&[["owner", "rank"]])),
        SchemaHash::compute(&declared(&[])),
        "declaring an index is identity"
    );

    let rejected = |table: TableSchemaBuilder| {
        JazzSchema::new(&SchemaBuilder::new().table(table).build()).is_err()
    };
    assert!(
        rejected(tasks_table().composite_index(["owner"])),
        "one column"
    );
    assert!(
        rejected(tasks_table().composite_index(["owner", "missing"])),
        "undeclared column"
    );
    assert!(
        rejected(tasks_table().composite_index(["owner", "owner"])),
        "repeated column"
    );
    assert!(
        rejected(
            tasks_table()
                .composite_index(["owner", "rank"])
                .composite_index(["owner", "rank"])
        ),
        "duplicate index"
    );
    assert!(
        rejected(
            tasks_table()
                .column("blob", ColumnType::Bytea)
                .composite_index(["owner", "blob"])
        ),
        "unordered column type"
    );
}

/// Durable-encoding receipt for the physical composite index. This is
/// deliberately below the public API: the index namespace
/// (`by_physical_composite_v1_<column ids>`) and its entry key layout
/// (branch key, then each indexed column in declared order, then the row
/// key) are restart-authoritative bytes that no query result exposes, so a
/// silent respelling would orphan every existing entry. Alice's one local
/// row pins the exact key on the ahead-current tier; the global-current table
/// declares the same index name and columns from the same helper.
#[test]
fn physical_composite_index_entry_keys_are_pinned() {
    let schema = JazzSchema::new(&public_schema()).expect("composite schema compiles");
    let directory = tempfile::tempdir().unwrap();
    {
        let db = open_rocks_db(directory.path(), &schema);
        insert_task(&db, row(0x0a), "alice", 7, "ship");
    }
    let families = schema.column_families();
    let storage = RocksDbStorage::open(
        directory.path(),
        &families.iter().map(String::as_str).collect::<Vec<_>>(),
    )
    .unwrap();
    let storage = block_on(LayoutStorage::new(storage, StorageLayout::jazz_class_v1())).unwrap();
    let keys = block_on(storage.prefix("indices".into(), Vec::new()))
        .unwrap()
        .into_iter()
        .map(|(key, _)| key)
        .filter(|key| {
            key.windows(b"\0by_physical_composite_".len())
                .any(|window| window == b"\0by_physical_composite_")
        })
        .map(hex::encode)
        .collect::<Vec<_>>();
    // `jazz_physical_1_ahead_current\0by_physical_composite_v1_1_2\0`, the
    // persisted-index tag 7, then the escaped logical key: branch key bytes,
    // text "alice", order-preserving i32 7, and the ahead row's primary key
    // (branch key, row uuid 0x0a.., transaction coordinate).
    assert_eq!(
        keys,
        [
            "6a617a7a5f706879736963616c5f315f61686561645f63757272656e740062795f706879736963616c5f636f6d706f736974655f76315f315f320007070100ffff00ffff00ffff00ffff00ff00ff0906616c69636500ff00ff090e8000ff00ff07ff070100ffff00ffff00ffff00ffff00ff00ff0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0300ff00ff00ff00ff00ff0400ff00ff0300ff00ff00ff00ff00ff00ff00ff010000"
        ]
    );
}
