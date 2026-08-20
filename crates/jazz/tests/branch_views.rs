use std::collections::BTreeMap;
use std::future::Future;
use std::pin::pin;
use std::task::{Context, Poll, Waker};

use jazz::db::{Db, DbConfig, DbIdentity, ReadOpts, SeededRowIdSource};
use jazz::groove::records::Value;
use jazz::groove::schema::{ColumnSchema, ColumnType};
use jazz::groove::storage::MemoryStorage;
use jazz::ids::{AuthorId, BranchDimensionId, NodeUuid, RowUuid};
use jazz::protocol::{BranchSelector, BranchViewBase, ReadViewSourceSpec, ReadViewSpec};
use jazz::schema::{BranchDimensionSchema, JazzSchema, TableSchema};

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

fn selector(byte: u8) -> BranchSelector {
    BranchSelector::new([("branch", Value::Uuid(uuid::Uuid::from_bytes([byte; 16])))])
}

fn open_db() -> (Db<MemoryStorage>, JazzSchema) {
    let dimension = BranchDimensionId(uuid::Uuid::from_bytes([0x61; 16]));
    let schema = JazzSchema::new_with_branch_dimensions(
        [BranchDimensionSchema::new(
            dimension,
            "branch",
            ColumnType::Uuid,
            Value::Uuid(uuid::Uuid::nil()),
        )],
        [TableSchema::new(
            "todos",
            [
                ColumnSchema::new("branch_id", ColumnType::Uuid),
                ColumnSchema::new("title", ColumnType::String),
            ],
        )
        .with_branch_dimension("branch_id", dimension)],
    );
    let families = schema.column_families();
    let storage = MemoryStorage::new(&families.iter().map(String::as_str).collect::<Vec<_>>());
    let db = block_on(Db::open(
        DbConfig::new(
            schema.clone(),
            storage,
            DbIdentity {
                node: NodeUuid::from_bytes([0x62; 16]),
                author: AuthorId::from_bytes([0x63; 16]),
            },
        )
        .with_id_source(SeededRowIdSource::new(1)),
    ))
    .unwrap();
    (db, schema)
}

#[test]
fn db_exact_mutations_and_branch_view_reads_compose_head_over_base() {
    let (db, schema) = open_db();
    let row = RowUuid::from_bytes([0x64; 16]);
    let base = selector(0x65);
    let head = selector(0x66);
    db.insert_with_id_in_branch(
        "todos",
        base.clone(),
        row,
        BTreeMap::from([("title".to_owned(), Value::String("base".to_owned()))]),
    )
    .unwrap();

    let prepared = db.prepare_query(&db.table("todos")).unwrap();
    let opts = ReadOpts {
        read_view: ReadViewSpec {
            source: ReadViewSourceSpec::BranchView {
                head: head.clone(),
                base: Some(BranchViewBase::Current(base)),
            },
            ..ReadViewSpec::default()
        },
        ..ReadOpts::default()
    };
    let rows = block_on(db.all(&prepared, opts.clone())).unwrap();
    assert_eq!(rows.len(), 1);
    let table = schema
        .tables
        .iter()
        .find(|table| table.name == "todos")
        .unwrap();
    assert_eq!(
        rows[0].cell(table, "title"),
        Some(Value::String("base".to_owned()))
    );
    assert_eq!(
        rows[0].cell(table, "branch_id"),
        Some(Value::Uuid(uuid::Uuid::from_bytes([0x66; 16])))
    );

    db.update_in_branch_view(
        "todos",
        head.clone(),
        Some(BranchViewBase::Current(selector(0x65))),
        row,
        BTreeMap::from([("title".to_owned(), Value::String("draft".to_owned()))]),
    )
    .unwrap();
    db.update_in_branch(
        "todos",
        head.clone(),
        row,
        BTreeMap::from([("title".to_owned(), Value::String("edited".to_owned()))]),
    )
    .unwrap();
    let rows = block_on(db.all(&prepared, opts.clone())).unwrap();
    assert_eq!(
        rows[0].cell(table, "title"),
        Some(Value::String("edited".to_owned()))
    );

    db.delete_in_branch("todos", head, row).unwrap();
    assert!(block_on(db.all(&prepared, opts)).unwrap().is_empty());
}
