use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::executor::block_on;
use futures::task::noop_waker;
use jazz::db::{Db, DbConfig, DbIdentity, ErrorCode, MergeableTxOps, ReadOpts};
use jazz::groove::storage::{TestStorage, TestStorageOperation};
use jazz::ids::{AuthorSubject, NodeUuid};
use jazz::row;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, OpenTransactionId, SchemaBuilder, TableSchemaBuilder};
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
    let identity = DbIdentity {
        node: NodeUuid::from_bytes([0x51; 16]),
        author: AuthorSubject::for_test_bytes([0x61; 16]),
    };
    let families = schema.column_families();
    let family_refs = families.iter().map(String::as_str).collect::<Vec<_>>();
    let (storage, control) = TestStorage::controlled(&family_refs);
    let storage_for_reopen = storage.clone();
    let db = block_on(Db::open(DbConfig::new(
        schema.clone(),
        storage,
        identity.clone(),
    )))
    .expect("open test database");
    let durable_seed = block_on(db.insert(
        "todos",
        row! { title: "durable before cancellation" },
        Default::default(),
    ))
    .expect("seed durable row");
    block_on(durable_seed.wait(DurabilityTier::Local)).expect("seed is locally durable");
    db.set_deferred_local_persistence(true);

    control.pause_on(TestStorageOperation::WriteMany);
    let write = block_on(db.insert("todos", row! { title: "resident now" }, Default::default()))
        .expect("resident insert does not await persistence");

    let query = db.prepare_query(&db.table("todos")).expect("prepare query");
    let rows = block_on(db.all(&query, ReadOpts::default())).expect("read resident rows");
    assert_eq!(rows.len(), 2, "the write is immediately query-visible");
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
            .len()
            == 1,
        "the deletion removes only the resident row before persistence",
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
        2,
        "the restoration is immediately query-visible",
    );
    // `None` is the facade's synchronous/resident acknowledgement tier; this
    // build has no separate `Sync` enum member.
    assert_eq!(
        block_on(write.wait(DurabilityTier::None)).expect("sync resident acknowledgement"),
        write.mergeable_tx_id()
    );
    let error = match block_on(write.wait(DurabilityTier::Local)) {
        Ok(_) => panic!("local durability must not be reported before persistence settles"),
        Err(error) => error,
    };
    assert_eq!(error.code, ErrorCode::NotObserved);
    assert_eq!(
        error.message, "write has not reached requested tier Local",
        "the write is resident/synchronous but has not crossed local durability"
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

    // The poison belongs to this live instance; it is not a durable marker.
    // Resume the test backend, discard the abandoned facade, and reopen from
    // the same bytes. The abandoned resident publication is never retried.
    control.resume_operation(TestStorageOperation::WriteMany);
    drop(db);
    let reopened = block_on(Db::open(DbConfig::new(
        schema,
        storage_for_reopen,
        identity,
    )))
    .expect("fresh facade may reopen after an ambiguous in-flight write");
    let reopened_query = reopened
        .prepare_query(&reopened.table("todos"))
        .expect("prepare reopened query");
    let durable_rows = block_on(reopened.all(&reopened_query, ReadOpts::default()))
        .expect("reopened facade reads existing durable state");
    assert_eq!(durable_rows.len(), 1);
    assert_eq!(durable_rows[0].row_uuid(), durable_seed.row_uuid());
    let fresh = block_on(reopened.insert(
        "todos",
        row! { title: "fresh after reopen" },
        Default::default(),
    ))
    .expect("fresh write is usable after reopen");
    block_on(fresh.wait(DurabilityTier::Local)).expect("fresh write is locally durable");
    let rows = block_on(reopened.all(&reopened_query, ReadOpts::default()))
        .expect("fresh facade reads durable state");
    assert_eq!(rows.len(), 2);
    assert!(
        rows.iter()
            .any(|row| row.row_uuid() == durable_seed.row_uuid())
    );
    assert!(rows.iter().any(|row| row.row_uuid() == fresh.row_uuid()));
}

#[test]
fn queued_update_retains_cold_preparation_and_its_definitive_identity() {
    let schema = schema();
    let families = schema.column_families();
    let family_refs = families.iter().map(String::as_str).collect::<Vec<_>>();
    let (storage, control) = TestStorage::controlled(&family_refs);
    let storage_control = storage.clone();
    let db = block_on(Db::open(DbConfig::new(
        schema,
        storage,
        DbIdentity {
            node: NodeUuid::from_bytes([0x54; 16]),
            author: AuthorSubject::for_test_bytes([0x64; 16]),
        },
    )))
    .expect("open yielding database");
    let seeded = block_on(db.insert("todos", row! { title: "before" }, Default::default()))
        .expect("seed row");
    block_on(seeded.wait(DurabilityTier::Local)).expect("seed is durable");

    storage_control.evict_all();
    control.pause_on(TestStorageOperation::Get);
    control.pause_on(TestStorageOperation::ScanOpen);
    let queued = db
        .enqueue_update(
            "todos".to_owned(),
            seeded.row_uuid(),
            row! { title: "after" },
            Default::default(),
        )
        .expect("synchronous facade reserves and queues");
    let reserved = queued.mergeable_tx_id();
    let state = block_on(queued.write_state()).expect("reserved state is observable");
    assert_eq!(state.durability, DurabilityTier::None);

    let mut first_turn = Box::pin(db.tick());
    let waker = noop_waker();
    let mut context = Context::from_waker(&waker);
    assert!(matches!(
        first_turn.as_mut().poll(&mut context),
        Poll::Pending
    ));
    drop(first_turn);
    assert!(
        control.observed().iter().any(|operation| matches!(
            operation,
            TestStorageOperation::Get | TestStorageOperation::ScanOpen
        )),
        "the queued owner future must reach the planted cold read",
    );
    assert_eq!(queued.mergeable_tx_id(), reserved);

    control.resume();
    for _ in 0..4_096 {
        let mut turn = Box::pin(db.tick());
        let _ = turn.as_mut().poll(&mut context);
        drop(turn);
        if block_on(queued.write_state())
            .is_ok_and(|state| state.durability >= DurabilityTier::Local)
        {
            break;
        }
    }
    let resumed_state = block_on(queued.write_state());
    assert!(
        resumed_state
            .as_ref()
            .is_ok_and(|state| state.durability >= DurabilityTier::Local),
        "bounded owner turns must finish the resumed queued mutation: {resumed_state:?}",
    );
    assert_eq!(
        block_on(queued.wait(DurabilityTier::Local)).expect("queued write settles"),
        reserved,
    );
    let query = db.prepare_query(&db.table("todos")).expect("prepare query");
    let rows = block_on(db.all(&query, ReadOpts::default())).expect("read updated row");
    assert_eq!(rows[0].cell_at(0), Some("after".into()));
}

#[test]
fn queued_mutations_are_fifo_owned_and_surface_preparation_failures() {
    let schema = schema();
    let families = schema.column_families();
    let family_refs = families.iter().map(String::as_str).collect::<Vec<_>>();
    let (storage, control) = TestStorage::controlled(&family_refs);
    let storage_control = storage.clone();
    let db = block_on(Db::open(DbConfig::new(
        schema,
        storage,
        DbIdentity {
            node: NodeUuid::from_bytes([0x55; 16]),
            author: AuthorSubject::for_test_bytes([0x65; 16]),
        },
    )))
    .expect("open yielding database");

    storage_control.evict_all();
    control.pause_on(TestStorageOperation::Get);
    control.pause_on(TestStorageOperation::ScanOpen);
    let missing = jazz::ids::RowUuid::from_bytes([0x75; 16]);
    let rejected = db
        .enqueue_update(
            "todos".to_owned(),
            missing,
            row! { title: "cannot update missing row" },
            Default::default(),
        )
        .expect("first operation reserves an identity");
    let accepted = db
        .enqueue_insert(
            "todos".to_owned(),
            row! { title: "second" },
            Default::default(),
        )
        .expect("second operation reserves an identity");
    let accepted_row = accepted.row_uuid();
    let accepted_tx = accepted.mergeable_tx_id();
    drop(accepted);

    let waker = noop_waker();
    let mut context = Context::from_waker(&waker);
    let mut first_turn = Box::pin(db.tick());
    assert!(matches!(
        first_turn.as_mut().poll(&mut context),
        Poll::Pending
    ));
    drop(first_turn);
    assert_eq!(
        db.write_state(accepted_tx)
            .expect("the second reservation remains observable")
            .durability,
        DurabilityTier::None,
        "the FIFO owner must not overtake a cold first operation",
    );

    control.resume();
    for _ in 0..4_096 {
        let mut turn = Box::pin(db.tick());
        let _ = turn.as_mut().poll(&mut context);
        drop(turn);
        if db
            .write_state(accepted_tx)
            .is_ok_and(|state| state.durability >= DurabilityTier::Local)
        {
            break;
        }
    }
    let error = block_on(rejected.write_state()).expect_err("missing-row update must fail");
    assert_eq!(error.code, ErrorCode::WriteRejected);
    assert!(
        db.write_state(accepted_tx)
            .is_ok_and(|state| state.durability >= DurabilityTier::Local),
        "the owner must continue with the next operation after a terminal preparation error",
    );
    let query = db.prepare_query(&db.table("todos")).expect("prepare query");
    let rows = block_on(db.all(&query, ReadOpts::default())).expect("read inserted row");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].row_uuid(), accepted_row);
}

#[test]
fn reopened_reservation_clock_dominates_durable_local_history() {
    let schema = schema();
    let families = schema.column_families();
    let family_refs = families.iter().map(String::as_str).collect::<Vec<_>>();
    let directory = tempfile::tempdir().expect("temporary RocksDB directory");
    let identity = DbIdentity {
        node: NodeUuid::from_bytes([0x56; 16]),
        author: AuthorSubject::for_test_bytes([0x66; 16]),
    };
    let storage = RocksDbStorage::open(directory.path(), &family_refs).expect("open RocksDB");
    let first = block_on(Db::open(DbConfig::new(
        schema.clone(),
        storage,
        identity.clone(),
    )))
    .expect("open first database");
    let first_write = first
        .enqueue_insert(
            "todos".to_owned(),
            row! { title: "before restart" },
            jazz::db::InsertOptions {
                updated_at_ms: Some(7),
                ..Default::default()
            },
        )
        .expect("reserve first identity");
    let first_tx = first_write.mergeable_tx_id();
    first.drive_queued_mutation_once();
    block_on(first_write.wait(DurabilityTier::Local)).expect("first write is durable");
    block_on(first.close()).expect("close first database");
    drop(first);

    let storage = RocksDbStorage::open(directory.path(), &family_refs).expect("reopen RocksDB");
    let reopened =
        block_on(Db::open(DbConfig::new(schema, storage, identity))).expect("reopen database");
    let second_write = reopened
        .enqueue_insert(
            "todos".to_owned(),
            row! { title: "after restart" },
            jazz::db::InsertOptions {
                updated_at_ms: Some(7),
                ..Default::default()
            },
        )
        .expect("reserve identity after reopen");
    assert!(
        second_write.mergeable_tx_id().time > first_tx.time,
        "the synchronously returned identity must dominate durable history for the reused node",
    );
}

#[test]
fn queued_mergeable_commit_retains_cold_parent_refresh_and_exact_identity() {
    let schema = schema();
    let families = schema.column_families();
    let family_refs = families.iter().map(String::as_str).collect::<Vec<_>>();
    let (storage, control) = TestStorage::controlled(&family_refs);
    let storage_control = storage.clone();
    let db = block_on(Db::open(DbConfig::new(
        schema,
        storage,
        DbIdentity {
            node: NodeUuid::from_bytes([0x57; 16]),
            author: AuthorSubject::for_test_bytes([0x67; 16]),
        },
    )))
    .expect("open yielding database");
    let seed = block_on(db.insert("todos", row! { title: "before" }, Default::default()))
        .expect("seed row");
    block_on(seed.wait(DurabilityTier::Local)).expect("seed is durable");

    let open_tx = OpenTransactionId::new();
    block_on(db.begin_mergeable(open_tx)).expect("begin mergeable transaction");
    block_on(db.mergeable_tx_ref(open_tx).update(
        "todos",
        seed.row_uuid(),
        row! { title: "after" },
        Default::default(),
    ))
    .expect("stage update");
    storage_control.evict_all();
    control.pause_on(TestStorageOperation::Get);
    control.pause_on(TestStorageOperation::ScanOpen);
    let queued = db
        .enqueue_commit_mergeable_handle(open_tx)
        .expect("reserve final transaction identity");
    let reserved = queued.mergeable_tx_id();

    let waker = noop_waker();
    let mut context = Context::from_waker(&waker);
    let mut reached_cold_commit = false;
    for _ in 0..3 {
        let mut turn = Box::pin(db.tick());
        reached_cold_commit |= matches!(turn.as_mut().poll(&mut context), Poll::Pending);
        drop(turn);
        if reached_cold_commit {
            break;
        }
    }
    assert!(
        reached_cold_commit,
        "bounded owner turns must reach the planted cold mergeable parent refresh",
    );
    assert_eq!(queued.mergeable_tx_id(), reserved);
    assert_eq!(
        block_on(queued.write_state())
            .expect("reservation remains observable")
            .durability,
        DurabilityTier::None,
    );

    control.resume();
    for _ in 0..4_096 {
        let mut turn = Box::pin(db.tick());
        let _ = turn.as_mut().poll(&mut context);
        drop(turn);
        if block_on(queued.write_state())
            .is_ok_and(|state| state.durability >= DurabilityTier::Local)
        {
            break;
        }
    }
    assert_eq!(
        block_on(queued.wait(DurabilityTier::Local)).expect("queued transaction settles"),
        reserved,
    );
    let query = db.prepare_query(&db.table("todos")).expect("prepare query");
    let rows = block_on(db.all(&query, ReadOpts::default())).expect("read committed row");
    assert_eq!(rows[0].cell_at(0), Some("after".into()));
}

#[test]
fn queued_exclusive_commit_retains_cold_serializability_and_exact_identity() {
    let schema = schema();
    let families = schema.column_families();
    let family_refs = families.iter().map(String::as_str).collect::<Vec<_>>();
    let (storage, control) = TestStorage::controlled(&family_refs);
    let storage_control = storage.clone();
    let db = block_on(Db::open(DbConfig::new(
        schema,
        storage,
        DbIdentity {
            node: NodeUuid::from_bytes([0x58; 16]),
            author: AuthorSubject::for_test_bytes([0x68; 16]),
        },
    )))
    .expect("open yielding database");
    let seed = block_on(db.insert("todos", row! { title: "before" }, Default::default()))
        .expect("seed row");
    block_on(seed.wait(DurabilityTier::Local)).expect("seed is durable");

    let open_tx = OpenTransactionId::new();
    storage_control.evict_all();
    control.pause_on(TestStorageOperation::Get);
    control.pause_on(TestStorageOperation::ScanOpen);
    db.enqueue_begin_exclusive(open_tx, None);
    db.enqueue_transaction_update(
        open_tx,
        true,
        "todos".to_owned(),
        seed.row_uuid(),
        row! { title: "after" },
        Default::default(),
    );
    let queued = db
        .enqueue_commit_exclusive_handle(open_tx)
        .expect("reserve final transaction identity");
    let reserved = queued.mergeable_tx_id();

    let waker = noop_waker();
    let mut context = Context::from_waker(&waker);
    let mut reached_cold_stage = false;
    for _ in 0..3 {
        let mut turn = Box::pin(db.tick());
        reached_cold_stage |= matches!(turn.as_mut().poll(&mut context), Poll::Pending);
        drop(turn);
        if reached_cold_stage {
            break;
        }
    }
    assert!(
        reached_cold_stage,
        "bounded owner turns must reach the planted cold exclusive staging read",
    );
    assert_eq!(queued.mergeable_tx_id(), reserved);
    assert_eq!(
        block_on(queued.write_state())
            .expect("reservation remains observable")
            .durability,
        DurabilityTier::None,
    );

    control.resume();
    for _ in 0..4_096 {
        let mut turn = Box::pin(db.tick());
        let _ = turn.as_mut().poll(&mut context);
        drop(turn);
        if block_on(queued.write_state())
            .is_ok_and(|state| state.durability >= DurabilityTier::Local)
        {
            break;
        }
    }
    assert_eq!(
        block_on(queued.wait(DurabilityTier::Local)).expect("queued transaction settles"),
        reserved,
    );
    let query = db.prepare_query(&db.table("todos")).expect("prepare query");
    let rows = block_on(db.all(&query, ReadOpts::default())).expect("read committed row");
    assert_eq!(rows[0].cell_at(0), Some("after".into()));
}
