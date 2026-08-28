use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::executor::block_on;
use futures::task::noop_waker;
use jazz::db::{Db, DbConfig, DbIdentity, ErrorCode, ReadOpts};
use jazz::groove::storage::{TestStorage, TestStorageOperation};
use jazz::ids::{AuthorSubject, NodeUuid};
use jazz::row;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
use jazz::tx::DurabilityTier;
use jazz_storage_rocksdb::RocksDbStorage;

fn schema() -> JazzSchema {
    let source = SchemaBuilder::new()
        .table(TableSchemaBuilder::new("todos").column("title", ColumnType::Text))
        .build();
    JazzSchema::new(&source).expect("deferred-persistence public schema compiles")
}

fn empty_schema() -> JazzSchema {
    JazzSchema::new(&SchemaBuilder::new().build()).expect("empty public schema compiles")
}

#[test]
fn rocksdb_writes_are_resident_before_the_sync_call_returns() {
    let schema = schema();
    let families = schema.column_families();
    let family_refs = families.iter().map(String::as_str).collect::<Vec<_>>();
    let directory = tempfile::tempdir().expect("temporary RocksDB directory");
    let storage = RocksDbStorage::open(directory.path(), &family_refs).expect("open RocksDB");
    let owner = block_on(Db::open_history_complete(DbConfig::new(
        empty_schema(),
        storage,
        DbIdentity {
            node: NodeUuid::from_bytes([0x52; 16]),
            author: AuthorSubject::for_test_bytes([0x62; 16]),
        },
    )))
    .expect("open persistent database");
    let db = block_on(owner.register_schema_view(schema)).expect("register schema view");
    db.set_non_durable_client();
    let row_id = jazz::ids::RowUuid::from_bytes([0; 16]);

    block_on(db.insert(
        "todos",
        row! { title: "first" },
        jazz::db::InsertOptions {
            row_id: Some(row_id),
            updated_at_ms: Some(1),
            ..Default::default()
        },
    ))
    .expect("first insert");
    assert!(
        block_on(db.insert(
            "todos",
            row! { title: "duplicate" },
            jazz::db::InsertOptions {
                row_id: Some(row_id),
                updated_at_ms: Some(2),
                ..Default::default()
            }
        ))
        .is_err(),
        "a second synchronous write must observe the resident first write",
    );
}

#[test]
fn cancelled_started_deferred_persistence_poison_requires_reopen() {
    let schema = schema();
    let families = schema.column_families();
    let family_refs = families.iter().map(String::as_str).collect::<Vec<_>>();
    let (storage, control) = TestStorage::controlled(&family_refs);
    let db = block_on(Db::open(DbConfig::new(
        schema,
        storage,
        DbIdentity {
            node: NodeUuid::from_bytes([0x51; 16]),
            author: AuthorSubject::for_test_bytes([0x61; 16]),
        },
    )))
    .expect("open test database");
    db.set_deferred_local_persistence(true);

    control.pause_on(TestStorageOperation::WriteMany);
    let write = block_on(db.insert("todos", row! { title: "resident now" }, Default::default()))
        .expect("resident insert does not await persistence");

    let query = db.prepare_query(&db.table("todos")).expect("prepare query");
    let rows = block_on(db.all(&query, ReadOpts::default())).expect("read resident rows");
    assert_eq!(rows.len(), 1, "the write is immediately query-visible");
    assert!(
        block_on(db.insert(
            "todos",
            row! { title: "duplicate" },
            jazz::db::InsertOptions {
                row_id: Some(write.row_uuid()),
                ..Default::default()
            }
        ))
        .is_err(),
        "resident currency checks must reject a duplicate before persistence",
    );
    block_on(db.delete("todos", write.row_uuid(), Default::default()))
        .expect("resident row can be deleted before insert persistence");
    assert!(
        block_on(db.all(&query, ReadOpts::default()))
            .expect("read resident deletion")
            .is_empty(),
        "the deletion is immediately query-visible",
    );
    block_on(db.restore(
        "todos",
        write.row_uuid(),
        Some(row! { title: "restored now" }),
        Default::default(),
    ))
    .expect("restore observes the resident deletion before persistence");
    assert_eq!(
        block_on(db.all(&query, ReadOpts::default()))
            .expect("read resident restoration")
            .len(),
        1,
        "the restoration is immediately query-visible",
    );
    assert!(
        block_on(write.wait(DurabilityTier::Local)).is_err(),
        "local durability must not be reported before persistence settles"
    );

    let mut tick = Box::pin(db.tick());
    let waker = noop_waker();
    let mut context = Context::from_waker(&waker);
    assert!(matches!(
        Pin::new(&mut tick).poll(&mut context),
        Poll::Pending
    ));
    assert!(
        control
            .observed()
            .contains(&TestStorageOperation::WriteMany)
    );

    // This poll has started the atomic write. Host teardown therefore cannot
    // safely retry the resident publication: it may already be durable.
    drop(tick);
    let error = match block_on(db.insert(
        "todos",
        row! { title: "must require reopen" },
        Default::default(),
    )) {
        Ok(_) => panic!("a started persistence cancellation poisons the live database"),
        Err(error) => error,
    };
    assert_eq!(error.code, ErrorCode::Storage);
    assert!(
        error.message.contains("poisoned"),
        "the public error must identify the fail-closed local database state: {error}"
    );
}
