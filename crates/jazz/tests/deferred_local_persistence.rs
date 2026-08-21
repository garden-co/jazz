use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::executor::block_on;
use futures::task::noop_waker;
use jazz::db::{Db, DbConfig, DbIdentity, ReadOpts};
use jazz::groove::storage::{TestStorage, TestStorageOperation};
use jazz::ids::{AuthorId, NodeUuid};
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
            author: AuthorId::from_bytes([0x62; 16]),
        },
    )))
    .expect("open persistent database");
    let db = block_on(owner.register_schema_view(schema)).expect("register schema view");
    db.set_non_durable_client();
    let row_id = jazz::ids::RowUuid::from_bytes([0; 16]);

    block_on(db.insert_with_id_at_ms("todos", row_id, row! { title: "first" }, 1))
        .expect("first insert");
    assert!(
        block_on(db.insert_with_id_at_ms("todos", row_id, row! { title: "duplicate" }, 2)).is_err(),
        "a second synchronous write must observe the resident first write",
    );
}

#[test]
fn deferred_persistence_keeps_resident_write_sync_and_local_durability_pending() {
    let schema = schema();
    let families = schema.column_families();
    let family_refs = families.iter().map(String::as_str).collect::<Vec<_>>();
    let (storage, control) = TestStorage::controlled(&family_refs);
    let db = block_on(Db::open(DbConfig::new(
        schema,
        storage,
        DbIdentity {
            node: NodeUuid::from_bytes([0x51; 16]),
            author: AuthorId::from_bytes([0x61; 16]),
        },
    )))
    .expect("open test database");
    db.set_deferred_local_persistence(true);

    control.pause_on(TestStorageOperation::WriteMany);
    let write = block_on(db.insert("todos", row! { title: "resident now" }))
        .expect("resident insert does not await persistence");

    let query = db.prepare_query(&db.table("todos")).expect("prepare query");
    let rows = block_on(db.all(&query, ReadOpts::default())).expect("read resident rows");
    assert_eq!(rows.len(), 1, "the write is immediately query-visible");
    assert!(
        block_on(db.insert_with_id("todos", write.row_uuid(), row! { title: "duplicate" },))
            .is_err(),
        "resident currency checks must reject a duplicate before persistence",
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

    // Host teardown may cancel an in-flight tick. The publication must remain
    // queued so the next host tick can finish the already-visible write.
    drop(tick);
    control.resume_operation(TestStorageOperation::WriteMany);
    block_on(db.tick()).expect("persistence settles through a later host tick");
    block_on(write.wait(DurabilityTier::Local)).expect("write reaches local durability");
}
