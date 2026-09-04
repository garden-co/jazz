use std::cell::RefCell;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};

use futures::StreamExt;
use futures::executor::block_on;
use futures::task::noop_waker;
use jazz::db::{
    Db, DbConfig, DbIdentity, ErrorCode, MergeableTxOps, ReadOpts, StreamingMutationKind,
    SubscriptionEvent,
};
use jazz::groove::storage::{MemoryStorage, ReopenableStorage, TestStorage, TestStorageOperation};
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

/// Deferred browser-style persistence must publish maintained local writes at
/// admission, before a paused storage owner turn can persist either mutation.
#[test]
fn deferred_local_persistence_publishes_insert_then_delete_before_persistence() {
    let schema = schema();
    let families = schema.column_families();
    let family_refs = families.iter().map(String::as_str).collect::<Vec<_>>();
    let (storage, control) = TestStorage::controlled(&family_refs);
    let db = block_on(Db::open(DbConfig::new(
        schema,
        storage,
        DbIdentity {
            node: NodeUuid::from_bytes([0x53; 16]),
            author: AuthorSubject::for_test_bytes([0x63; 16]),
        },
    )))
    .expect("open deferred subscription database");
    db.set_deferred_local_persistence(true);
    let query = db
        .prepare_query(&db.table("todos"))
        .expect("prepare todos query");
    let mut subscription = block_on(db.subscribe(&query, ReadOpts::default()))
        .expect("open local maintained subscription");
    assert!(matches!(
        block_on(subscription.next()),
        Some(SubscriptionEvent::Delta { reset: true, added, .. }) if added.is_empty()
    ));

    control.take_observed();
    control.pause_on(TestStorageOperation::WriteMany);
    let write = block_on(db.insert("todos", row! { title: "transient" }, Default::default()))
        .expect("admit deferred insert");
    assert!(matches!(
        subscription.try_next_event(),
        Some(SubscriptionEvent::Delta { added, removed, .. }) if added.len() == 1 && removed.is_empty()
    ));

    block_on(db.delete("todos", write.row_uuid(), Default::default()))
        .expect("admit deferred deletion");
    assert!(matches!(
        subscription.try_next_event(),
        Some(SubscriptionEvent::Delta { added, removed, .. }) if added.is_empty() && removed.len() == 1
    ));
    assert!(
        !control
            .observed()
            .contains(&TestStorageOperation::WriteMany),
        "both maintained deltas publish before the paused persistence owner turn starts"
    );
}

/// Alice's cancelled tick leaves the runtime-owned write alive, not retried.
/// alice -> resident insert/delete/restore -> paused storage -> drop tick
///       -> resume owner -> reopen -> restored row
/// Controlled storage is required to distinguish tick cancellation from
/// abandonment of the actual atomic storage operation.
#[test]
fn cancelled_tick_retains_started_deferred_persistence() {
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

    // Cancelling the waiter is not host teardown: the runtime still owns the
    // original storage future and must neither abandon nor restart it.
    drop(tick);
    control.take_observed();
    let fresh = block_on(db.insert(
        "todos",
        row! { title: "accepted after tick cancellation" },
        Default::default(),
    ))
    .expect("tick cancellation does not poison a retained storage operation");
    let mut resumed_tick = Box::pin(db.tick());
    assert!(resumed_tick.as_mut().poll(&mut context).is_pending());
    assert!(
        !control
            .observed()
            .contains(&TestStorageOperation::WriteMany),
        "resuming must not start a second atomic write while the original is paused"
    );
    drop(resumed_tick);
    control.resume_operation(TestStorageOperation::WriteMany);
    block_on(db.tick()).expect("drain retained writes in publication order");
    block_on(write.wait(DurabilityTier::Local)).expect("original write became durable");
    block_on(fresh.wait(DurabilityTier::Local)).expect("following write became durable");
    drop(db);
    let reopened = block_on(Db::open(DbConfig::new(
        schema,
        storage_for_reopen,
        identity,
    )))
    .expect("reopen completed storage");
    let reopened_query = reopened
        .prepare_query(&reopened.table("todos"))
        .expect("prepare reopened query");
    let rows = block_on(reopened.all(&reopened_query, ReadOpts::default()))
        .expect("fresh facade reads durable state");
    assert_eq!(rows.len(), 3);
    assert_eq!(
        rows.iter()
            .find(|row| row.row_uuid() == write.row_uuid())
            .unwrap()
            .cell_at(0),
        Some("restored now".into()),
        "insert, delete, and restore persist in order"
    );
    assert!(
        rows.iter()
            .any(|row| row.row_uuid() == durable_seed.row_uuid())
    );
    assert!(rows.iter().any(|row| row.row_uuid() == fresh.row_uuid()));
}

/// A cold queued preparation must not indefinitely prevent persistence of an
/// earlier resident publication.  The two owner responsibilities are ordered
/// by publication, not by later preparation work.
/// Alice -> earlier insert -> persistence; Bob -> later update -> cold read.
/// Controlled storage keeps Bob's read paused while Alice's bytes are checked
/// through an independently reopened database.
#[test]
fn cold_queued_preparation_does_not_starve_earlier_deferred_persistence() {
    struct OwnerWake(std::sync::atomic::AtomicUsize);
    impl std::task::Wake for OwnerWake {
        fn wake(self: std::sync::Arc<Self>) {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    }
    struct OwnerScheduler(std::sync::Arc<OwnerWake>);
    impl jazz::db::TickScheduler for OwnerScheduler {
        fn schedule_tick(&self, _: jazz::db::TickUrgency) {}
        fn schedule_tick_after(&self, _: u64) {}
        fn query_runtime_waker(&self) -> Option<std::task::Waker> {
            Some(std::task::Waker::from(self.0.clone()))
        }
    }
    let schema = schema();
    let families = schema.column_families();
    let family_refs = families.iter().map(String::as_str).collect::<Vec<_>>();
    let durable_storage = MemoryStorage::new(&family_refs).expect("durable backing");
    let storage = TestStorage::wrap(durable_storage.clone());
    let control = storage.control();
    let db = block_on(Db::open(DbConfig::new(
        schema.clone(),
        storage.clone(),
        DbIdentity {
            node: NodeUuid::from_bytes([0x5d; 16]),
            author: AuthorSubject::for_test_bytes([0x6d; 16]),
        },
    )))
    .expect("open yielding database");
    let owner_wake = std::sync::Arc::new(OwnerWake(std::sync::atomic::AtomicUsize::new(0)));
    db.set_tick_scheduler(Some(Rc::new(OwnerScheduler(owner_wake.clone()))));
    let seeded = block_on(db.insert("todos", row! { title: "seed" }, Default::default()))
        .expect("seed durable row");
    block_on(seeded.wait(DurabilityTier::Local)).expect("seed is durable");

    db.set_deferred_local_persistence(true);
    let earlier = block_on(db.insert("todos", row! { title: "earlier" }, Default::default()))
        .expect("admit resident deferred publication");
    assert_eq!(
        block_on(earlier.write_state())
            .expect("resident write state")
            .durability,
        DurabilityTier::None,
    );

    storage.evict_all();
    control.take_observed();
    control.pause_on(TestStorageOperation::Get);
    control.pause_on(TestStorageOperation::ScanOpen);
    let later = db
        .enqueue_update(
            "todos".to_owned(),
            seeded.row_uuid(),
            row! { title: "later cold preparation" },
            Default::default(),
        )
        .expect("queue later cold mutation");

    let waker = noop_waker();
    let mut context = Context::from_waker(&waker);
    for _ in 0..16 {
        let mut tick = Box::pin(db.tick());
        let result = tick.as_mut().poll(&mut context);
        assert!(
            !matches!(result, Poll::Ready(Err(_))),
            "owner turn failed: {result:?}"
        );
    }
    assert!(
        control
            .observed()
            .contains(&TestStorageOperation::WriteMany),
        "an earlier resident publication must start persistence even while a later queued preparation is cold",
    );
    assert!(
        control.observed().iter().any(|operation| matches!(
            operation,
            TestStorageOperation::Get | TestStorageOperation::ScanOpen
        )),
        "later preparation must actually reach a paused cold read"
    );
    // Use a separate resident cache/controller over the same durable bytes;
    // reading through the original facade could merely see its local overlay.
    let durable = block_on(Db::open(DbConfig::new(
        schema,
        durable_storage,
        DbIdentity {
            node: NodeUuid::from_bytes([0x5e; 16]),
            author: AuthorSubject::for_test_bytes([0x6d; 16]),
        },
    )))
    .expect("inspect durable state while the later read remains paused");
    let query = durable.prepare_query(&durable.table("todos")).unwrap();
    let rows = block_on(durable.all(&query, ReadOpts::default())).unwrap();
    assert!(
        rows.iter().any(|row| row.row_uuid() == earlier.row_uuid()),
        "earlier publication must persist independently of later preparation"
    );
    assert_eq!(
        rows.iter()
            .find(|row| row.row_uuid() == seeded.row_uuid())
            .unwrap()
            .cell_at(0),
        Some("seed".into())
    );

    owner_wake.0.store(0, std::sync::atomic::Ordering::SeqCst);
    control.resume();
    assert!(
        owner_wake.0.load(std::sync::atomic::Ordering::SeqCst) > 0,
        "storage readiness must wake the runtime after its tick has returned"
    );
    for _ in 0..4096 {
        let mut tick = Box::pin(db.tick());
        let result = tick.as_mut().poll(&mut context);
        assert!(
            !matches!(result, Poll::Ready(Err(_))),
            "owner turn failed: {result:?}"
        );
        drop(tick);
        if block_on(later.write_state())
            .is_ok_and(|state| state.durability >= DurabilityTier::Local)
        {
            break;
        }
    }
    block_on(earlier.wait(DurabilityTier::Local)).expect("earlier durability settles");
    block_on(later.wait(DurabilityTier::Local)).expect("later mutation resumes and persists");
    let query = db.prepare_query(&db.table("todos")).unwrap();
    let rows = block_on(db.all(&query, ReadOpts::default())).unwrap();
    assert_eq!(
        rows.iter()
            .find(|row| row.row_uuid() == seeded.row_uuid())
            .unwrap()
            .cell_at(0),
        Some("later cold preparation".into())
    );
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
    let mut resident_wait = Box::pin(db.wait_for_transaction(reserved, DurabilityTier::None));
    let waker = noop_waker();
    let mut context = Context::from_waker(&waker);
    assert!(
        matches!(resident_wait.as_mut().poll(&mut context), Poll::Pending),
        "identity reservation alone is not the resident publication milestone",
    );
    drop(resident_wait);

    let mut first_turn = Box::pin(db.tick());
    assert!(matches!(
        first_turn.as_mut().poll(&mut context),
        Poll::Ready(Ok(()))
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
        Poll::Ready(Ok(()))
    ));
    drop(first_turn);
    assert!(
        control.observed().iter().any(|operation| {
            matches!(
                operation,
                TestStorageOperation::Get | TestStorageOperation::ScanOpen
            )
        }),
        "the first FIFO operation reaches its planted cold storage boundary before the owner yields",
    );
    assert_eq!(
        db.write_state(accepted_tx)
            .expect("the second reservation remains observable")
            .durability,
        DurabilityTier::None,
        "the FIFO owner must not overtake a cold first operation",
    );

    control.resume();
    let mut first_terminal_before_second = false;
    for _ in 0..4_096 {
        let mut turn = Box::pin(db.tick());
        let _ = turn.as_mut().poll(&mut context);
        drop(turn);
        let first_terminal = block_on(rejected.write_state()).is_err();
        let second_published = db
            .write_state(accepted_tx)
            .is_ok_and(|state| state.durability >= DurabilityTier::Local);
        if first_terminal || second_published {
            first_terminal_before_second = first_terminal && !second_published;
            break;
        }
    }
    assert!(
        first_terminal_before_second,
        "resuming a cold head must terminalize it before the following operation; requeueing Pending at the back overtakes this receipt",
    );
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
fn close_owns_and_drains_cold_failed_and_following_fifo_mutations() {
    let schema = schema();
    let families = schema.column_families();
    let family_refs = families.iter().map(String::as_str).collect::<Vec<_>>();
    let (storage, control) = TestStorage::controlled(&family_refs);
    let reopen_handle = storage.clone();
    let owner = block_on(Db::open_history_complete(DbConfig::new(
        empty_schema(),
        storage,
        DbIdentity {
            node: NodeUuid::from_bytes([0x59; 16]),
            author: AuthorSubject::for_test_bytes([0x69; 16]),
        },
    )))
    .expect("open yielding database");
    let db = Rc::new(
        block_on(owner.register_schema_view(schema.clone())).expect("register sibling schema view"),
    );

    reopen_handle.evict_all();
    control.pause_on(TestStorageOperation::Get);
    control.pause_on(TestStorageOperation::ScanOpen);
    let failed = db
        .enqueue_update(
            "todos".to_owned(),
            jazz::ids::RowUuid::from_bytes([0x79; 16]),
            row! { title: "missing" },
            Default::default(),
        )
        .expect("reserve failing head");
    let second = db
        .enqueue_insert(
            "todos".to_owned(),
            row! { title: "second" },
            Default::default(),
        )
        .expect("reserve second");
    let second_tx = second.mergeable_tx_id();
    db.enqueue_insert(
        "todos".to_owned(),
        row! { title: "third" },
        Default::default(),
    )
    .expect("reserve third");

    let local_waits = Rc::new(RefCell::new(Vec::new()));
    let local_wait_results = Rc::clone(&local_waits);
    let reentrant_waits = Rc::new(RefCell::new(Vec::new()));
    let reentrant_wait_results = Rc::clone(&reentrant_waits);
    let reentrant_db = Rc::clone(&db);
    db.wait_for_transaction_with(second_tx, DurabilityTier::Local, move |result| {
        local_wait_results.borrow_mut().push(result);
        reentrant_db.wait_for_transaction_with(second_tx, DurabilityTier::Edge, move |result| {
            reentrant_wait_results.borrow_mut().push(result)
        });
    });
    let edge_waits = Rc::new(RefCell::new(Vec::new()));
    let edge_wait_results = Rc::clone(&edge_waits);
    db.wait_for_transaction_with(second_tx, DurabilityTier::Edge, move |result| {
        edge_wait_results.borrow_mut().push(result);
    });

    let waker = noop_waker();
    let mut context = Context::from_waker(&waker);
    let mut close = Box::pin(owner.close());
    assert!(matches!(close.as_mut().poll(&mut context), Poll::Pending));

    let after_closing = match db.enqueue_insert(
        "todos".to_owned(),
        row! { title: "must not be accepted after close starts" },
        Default::default(),
    ) {
        Ok(_) => panic!("close must synchronously close shared mutation admission"),
        Err(error) => error,
    };
    assert_eq!(after_closing.code, ErrorCode::WriteRejected);
    let direct_after_closing = block_on(db.insert(
        "todos",
        row! { title: "direct Rust mutation must share the owner gate" },
        Default::default(),
    ))
    .err()
    .expect("direct mutation through a sibling schema view must be rejected");
    assert_eq!(direct_after_closing.code, ErrorCode::WriteRejected);
    let late_open_tx = OpenTransactionId::new();
    assert_eq!(
        db.enqueue_begin_mergeable(late_open_tx, None, None)
            .expect_err("transaction entry after close starts must be rejected")
            .code,
        ErrorCode::WriteRejected,
    );
    assert_eq!(
        block_on(db.begin_exclusive(OpenTransactionId::new()))
            .expect_err("direct transaction entry through a sibling view must be rejected")
            .code,
        ErrorCode::WriteRejected,
    );
    let late_waits = Rc::new(RefCell::new(Vec::new()));
    let late_wait_results = Rc::clone(&late_waits);
    db.wait_for_transaction_with(second_tx, DurabilityTier::Global, move |result| {
        late_wait_results.borrow_mut().push(result);
    });
    assert_eq!(late_waits.borrow().len(), 1);
    assert_eq!(
        late_waits.borrow()[0].as_ref().unwrap_err().code,
        ErrorCode::NotObserved,
    );
    assert_eq!(
        db.enqueue_transaction_insert(
            late_open_tx,
            false,
            "todos".to_owned(),
            row! { title: "must not stage after close starts" },
            Default::default(),
        )
        .expect_err("transaction staging after close starts must be rejected")
        .code,
        ErrorCode::WriteRejected,
    );
    control.resume();
    let close_result = loop {
        if let Poll::Ready(result) = close.as_mut().poll(&mut context) {
            break result;
        }
    };
    close_result.expect("close drains every accepted operation before storage retirement");
    assert_eq!(local_waits.borrow().len(), 1);
    assert_eq!(local_waits.borrow()[0].as_ref().unwrap(), &second_tx);
    assert_eq!(edge_waits.borrow().len(), 1);
    assert_eq!(
        edge_waits.borrow()[0].as_ref().unwrap_err().code,
        ErrorCode::NotObserved,
    );
    assert_eq!(reentrant_waits.borrow().len(), 1);
    assert_eq!(
        reentrant_waits.borrow()[0].as_ref().unwrap_err().code,
        ErrorCode::NotObserved,
    );
    assert_eq!(
        block_on(failed.write_state())
            .expect_err("failed queued operation remains terminally observable")
            .code,
        ErrorCode::WriteRejected,
    );
    drop(close);
    drop(db);
    drop(owner);

    let reopened_storage = block_on(reopen_handle.reopen(families)).expect("reopen drained store");
    let reopened = block_on(Db::open(DbConfig::new(
        schema,
        reopened_storage,
        DbIdentity {
            node: NodeUuid::from_bytes([0x59; 16]),
            author: AuthorSubject::for_test_bytes([0x69; 16]),
        },
    )))
    .expect("reopen database after close");
    let query = reopened
        .prepare_query(&reopened.table("todos"))
        .expect("prepare reopened query");
    let rows = block_on(reopened.all(&query, ReadOpts::default())).expect("read drained writes");
    let titles = rows.iter().map(|row| row.cell_at(0)).collect::<Vec<_>>();
    assert_eq!(titles, vec![Some("second".into()), Some("third".into())]);
}

#[test]
fn retained_streaming_uploads_cannot_mutate_storage_after_sibling_owner_close_starts() {
    let schema = schema();
    let families = schema.column_families();
    let family_refs = families.iter().map(String::as_str).collect::<Vec<_>>();
    let (storage, control) = TestStorage::controlled(&family_refs);
    let storage_control = storage.clone();
    let owner = block_on(Db::open_history_complete(DbConfig::new(
        empty_schema(),
        storage,
        DbIdentity {
            node: NodeUuid::from_bytes([0x5a; 16]),
            author: AuthorSubject::for_test_bytes([0x6a; 16]),
        },
    )))
    .expect("open yielding owner");
    let db = block_on(owner.register_schema_view(schema)).expect("register sibling schema view");
    let cells = Default::default();

    let mut push_upload = db
        .begin_streaming_value_upload("todos", &cells, "title")
        .expect("begin retained push upload");
    let mut finish_upload = db
        .begin_streaming_value_upload("todos", &cells, "title")
        .expect("begin retained finish upload");
    let mut abort_upload = db
        .begin_streaming_value_upload("todos", &cells, "title")
        .expect("begin retained abort upload");
    block_on(db.push_streaming_value_upload(&mut push_upload, b"before close"))
        .expect("initialize push upload");
    block_on(db.push_streaming_value_upload(&mut finish_upload, b"before close"))
        .expect("initialize finish upload");
    block_on(db.push_streaming_value_upload(&mut abort_upload, b"before close"))
        .expect("initialize abort upload");

    storage_control.evict_all();
    control.pause_on(TestStorageOperation::Get);
    control.pause_on(TestStorageOperation::ScanOpen);
    db.enqueue_update(
        "todos".to_owned(),
        jazz::ids::RowUuid::from_bytes([0x7a; 16]),
        row! { title: "cold close head" },
        Default::default(),
    )
    .expect("queue cold head");
    let waker = noop_waker();
    let mut context = Context::from_waker(&waker);
    let mut close = Box::pin(owner.close());
    assert!(matches!(close.as_mut().poll(&mut context), Poll::Pending));
    control.take_observed();

    let begin_error = match db.begin_streaming_value_upload("todos", &cells, "title") {
        Ok(_) => panic!("streaming begin after close unexpectedly succeeded"),
        Err(error) => error,
    };
    assert_eq!(begin_error.code, ErrorCode::WriteRejected);
    let mut append = Box::pin(db.append_value(
        "todos",
        jazz::ids::RowUuid::from_bytes([0x7c; 16]),
        "title",
        b"after close".to_vec(),
    ));
    let Poll::Ready(Err(append_error)) = append.as_mut().poll(&mut context) else {
        panic!("append after close must reject before touching cold storage");
    };
    assert_eq!(append_error.code, ErrorCode::WriteRejected);
    let mut splice = Box::pin(db.splice_value(
        "todos",
        jazz::ids::RowUuid::from_bytes([0x7d; 16]),
        "title",
        0,
        0,
        b"after close".to_vec(),
    ));
    let Poll::Ready(Err(splice_error)) = splice.as_mut().poll(&mut context) else {
        panic!("splice after close must reject before touching cold storage");
    };
    assert_eq!(splice_error.code, ErrorCode::WriteRejected);
    assert_eq!(
        block_on(db.push_streaming_value_upload(&mut push_upload, b"after close"))
            .expect_err("retained push after close must fail")
            .code,
        ErrorCode::WriteRejected,
    );
    let finish_error = match block_on(db.finish_streaming_value_upload(
        finish_upload,
        StreamingMutationKind::Insert,
        "todos",
        jazz::ids::RowUuid::from_bytes([0x7b; 16]),
        cells.clone(),
        "title",
        jazz::db::WriteIdentity::Database,
        None,
        None,
        None,
    )) {
        Ok(_) => panic!("retained finish after close unexpectedly published"),
        Err(error) => error,
    };
    assert_eq!(finish_error.code, ErrorCode::WriteRejected);
    assert_eq!(
        block_on(db.abort_streaming_value_upload(abort_upload))
            .expect_err("retained abort after close must fail")
            .code,
        ErrorCode::WriteRejected,
    );
    assert!(
        control.observed().is_empty(),
        "post-close streaming calls must not touch journal, chunks, or roots"
    );

    control.resume();
    while close.as_mut().poll(&mut context).is_pending() {}
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
fn queued_transaction_read_waits_for_staging_and_cold_storage_without_sync_polling() {
    let schema = schema();
    let families = schema.column_families();
    let family_refs = families.iter().map(String::as_str).collect::<Vec<_>>();
    let (storage, control) = TestStorage::controlled(&family_refs);
    let storage_control = storage.clone();
    let db = Rc::new(
        block_on(Db::open(DbConfig::new(
            schema,
            storage,
            DbIdentity {
                node: NodeUuid::from_bytes([0x5a; 16]),
                author: AuthorSubject::for_test_bytes([0x6a; 16]),
            },
        )))
        .expect("open yielding database"),
    );
    let open_tx = OpenTransactionId::new();
    block_on(db.begin_mergeable(open_tx)).expect("open transaction");
    let row_id = db
        .enqueue_transaction_insert(
            open_tx,
            false,
            "todos".to_owned(),
            row! { title: "staged before read" },
            Default::default(),
        )
        .expect("queue transaction staging before the read");
    let query = db.prepare_query(&db.table("todos")).expect("prepare query");

    // IndexedDB can yield at all three storage boundaries. The binding must
    // retain this future on its owner queue instead of no-op-waker polling it.
    storage_control.evict_all();
    control.pause_on(TestStorageOperation::Get);
    control.pause_on(TestStorageOperation::ScanOpen);
    control.pause_on(TestStorageOperation::ScanBatch);
    let read_db = Rc::clone(&db);
    let pending = db.enqueue_transaction_read(open_tx, async move {
        read_db
            .mergeable_tx_ref(open_tx)
            .all_prepared_with_opts(&query, ReadOpts::default())
            .await
    });

    let waker = noop_waker();
    let mut context = Context::from_waker(&waker);
    let mut pending = Box::pin(pending);
    for _ in 0..8 {
        let mut turn = Box::pin(db.tick());
        let _ = turn.as_mut().poll(&mut context);
        drop(turn);
        if control.observed().iter().any(|operation| {
            matches!(
                operation,
                TestStorageOperation::Get
                    | TestStorageOperation::ScanOpen
                    | TestStorageOperation::ScanBatch
            )
        }) {
            break;
        }
    }
    assert!(
        control.observed().iter().any(|operation| matches!(
            operation,
            TestStorageOperation::Get
                | TestStorageOperation::ScanOpen
                | TestStorageOperation::ScanBatch
        )),
        "the owner-queued read reaches a planted cold storage boundary",
    );
    assert!(matches!(pending.as_mut().poll(&mut context), Poll::Pending));

    control.resume();
    let mut settled = None;
    for _ in 0..4_096 {
        let mut turn = Box::pin(db.tick());
        let _ = turn.as_mut().poll(&mut context);
        drop(turn);
        if let Poll::Ready(result) = pending.as_mut().poll(&mut context) {
            settled = Some(
                result
                    .expect("queued read owner operation remains retained")
                    .expect("queued read settles after storage wake"),
            );
            break;
        }
    }
    let rows = settled.expect("bounded owner turns settle the queued read after storage wakes");
    assert!(
        rows.iter().any(|row| row.row_uuid() == row_id),
        "the read executes after its staged FIFO predecessor and sees its row",
    );
    for operation in [
        TestStorageOperation::Get,
        TestStorageOperation::ScanOpen,
        TestStorageOperation::ScanBatch,
    ] {
        assert!(
            control.observed().contains(&operation),
            "the transaction read exercises the cold {operation:?} storage boundary",
        );
    }
}

#[test]
fn queued_transaction_read_reports_prior_staging_failure_without_running_outside_transaction() {
    let schema = schema();
    let families = schema.column_families();
    let family_refs = families.iter().map(String::as_str).collect::<Vec<_>>();
    let db = Rc::new(
        block_on(Db::open(DbConfig::new(
            schema,
            TestStorage::new(&family_refs),
            DbIdentity {
                node: NodeUuid::from_bytes([0x5b; 16]),
                author: AuthorSubject::for_test_bytes([0x6b; 16]),
            },
        )))
        .expect("open database"),
    );
    let open_tx = OpenTransactionId::new();
    block_on(db.begin_mergeable(open_tx)).expect("open transaction");
    db.enqueue_transaction_update(
        open_tx,
        false,
        "missing".to_owned(),
        jazz::ids::RowUuid::from_bytes([0x9b; 16]),
        row! { title: "missing" },
        Default::default(),
    )
    .expect("queue intentionally failing staging operation");
    let query = db.prepare_query(&db.table("todos")).expect("prepare query");
    let read_db = Rc::clone(&db);
    let pending = db.enqueue_transaction_read(open_tx, async move {
        read_db
            .mergeable_tx_ref(open_tx)
            .all_prepared_with_opts(&query, ReadOpts::default())
            .await
    });

    let waker = noop_waker();
    let mut context = Context::from_waker(&waker);
    let mut pending = Box::pin(pending);
    let mut outcome = None;
    for _ in 0..64 {
        let mut turn = Box::pin(db.tick());
        let _ = turn.as_mut().poll(&mut context);
        drop(turn);
        if let Poll::Ready(result) = pending.as_mut().poll(&mut context) {
            outcome = Some(result.expect("read receiver remains owned by the queue"));
            break;
        }
    }
    let error = outcome
        .expect("the poisoned transaction resolves its queued read")
        .expect_err("a queued read must not bypass a failed staged predecessor");
    assert_eq!(error.code, ErrorCode::Schema);
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
        let _ = turn.as_mut().poll(&mut context);
        drop(turn);
        reached_cold_commit |= control.observed().iter().any(|operation| {
            matches!(
                operation,
                TestStorageOperation::Get | TestStorageOperation::ScanOpen
            )
        });
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
    db.enqueue_begin_exclusive(open_tx, None, None).unwrap();
    db.enqueue_transaction_update(
        open_tx,
        true,
        "todos".to_owned(),
        seed.row_uuid(),
        row! { title: "after" },
        Default::default(),
    )
    .unwrap();
    let queued = db
        .enqueue_commit_exclusive_handle(open_tx)
        .expect("reserve final transaction identity");
    let reserved = queued.mergeable_tx_id();

    let waker = noop_waker();
    let mut context = Context::from_waker(&waker);
    let mut reached_cold_stage = false;
    for _ in 0..3 {
        let mut turn = Box::pin(db.tick());
        let _ = turn.as_mut().poll(&mut context);
        drop(turn);
        reached_cold_stage |= control.observed().iter().any(|operation| {
            matches!(
                operation,
                TestStorageOperation::Get | TestStorageOperation::ScanOpen
            )
        });
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

#[test]
fn queued_transaction_stage_failure_poison_prevents_partial_commit() {
    let schema = schema();
    let families = schema.column_families();
    let family_refs = families.iter().map(String::as_str).collect::<Vec<_>>();
    let storage =
        jazz::groove::storage::MemoryStorage::new(&family_refs).expect("open memory storage");
    let db = block_on(Db::open(DbConfig::new(
        schema,
        storage,
        DbIdentity {
            node: NodeUuid::from_bytes([0x59; 16]),
            author: AuthorSubject::for_test_bytes([0x69; 16]),
        },
    )))
    .expect("open database");
    let open_tx = OpenTransactionId::new();
    db.enqueue_begin_mergeable(open_tx, None, None)
        .expect("queue begin");
    db.enqueue_transaction_insert(
        open_tx,
        false,
        "missing_table".to_owned(),
        row! { title: "invalid" },
        Default::default(),
    )
    .unwrap();
    db.enqueue_transaction_insert(
        open_tx,
        false,
        "todos".to_owned(),
        row! { title: "must not commit" },
        Default::default(),
    )
    .unwrap();
    let commit = db
        .enqueue_commit_mergeable_handle(open_tx)
        .expect("reserve final identity");
    let waker = noop_waker();
    let mut context = Context::from_waker(&waker);
    for _ in 0..32 {
        let mut turn = Box::pin(db.tick());
        let _ = turn.as_mut().poll(&mut context);
        drop(turn);
        if block_on(commit.write_state()).is_err() {
            break;
        }
    }
    let error = block_on(commit.write_state()).expect_err("stage error poisons the commit");
    assert_eq!(error.code, ErrorCode::Schema);
    let query = db.prepare_query(&db.table("todos")).expect("prepare query");
    assert!(
        block_on(db.all(&query, ReadOpts::default()))
            .expect("read rows")
            .is_empty(),
        "a later valid stage must not survive an earlier terminal stage error",
    );
}

#[test]
fn queued_commit_uses_host_clock_before_staging_runs() {
    for exclusive in [false, true] {
        let schema = schema();
        let families = schema.column_families();
        let family_refs = families.iter().map(String::as_str).collect::<Vec<_>>();
        let storage = jazz::groove::storage::MemoryStorage::new(&family_refs).unwrap();
        let db = block_on(Db::open(DbConfig::new(
            schema,
            storage,
            DbIdentity {
                node: NodeUuid::from_bytes([0x5b; 16]),
                author: AuthorSubject::for_test_bytes([0x6b; 16]),
            },
        )))
        .unwrap();
        let open_tx = OpenTransactionId::new();
        if exclusive {
            db.enqueue_begin_exclusive(open_tx, None, None).unwrap();
        } else {
            db.enqueue_begin_mergeable(open_tx, None, None).unwrap();
        }
        db.enqueue_transaction_insert(
            open_tx,
            exclusive,
            "todos".to_owned(),
            row! { title: "queued" },
            jazz::db::InsertOptions {
                updated_at_ms: Some(100),
                ..Default::default()
            },
        )
        .unwrap();
        let host_now = 1_800_000_000_000;
        let write = if exclusive {
            db.enqueue_commit_exclusive_handle_at_ms(open_tx, host_now)
        } else {
            db.enqueue_commit_mergeable_handle_at_ms(open_tx, host_now)
        }
        .unwrap();
        let reserved = write.mergeable_tx_id();
        assert_eq!(reserved.physical_ms(), host_now);
        // No owner work ran before reservation. Open/stage/commit now drain
        // through the same queue, retaining that exact returned identity.
        for _ in 0..8 {
            block_on(db.tick()).unwrap();
        }
        assert_eq!(
            block_on(write.wait(DurabilityTier::Local)).unwrap(),
            reserved
        );
        let query = db.prepare_query(&db.table("todos")).unwrap();
        let rows = block_on(db.all(&query, ReadOpts::default())).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].cell_at(0), Some("queued".into()));
        assert_eq!(
            db.row_provenance(&rows[0]).unwrap().unwrap().updated_at,
            100,
            "row provenance is not the transaction HLC clock sample",
        );
        assert!(
            db.reserve_transaction_id_at_ms(1).unwrap().time > reserved.time,
            "a subsequent backward clock sample must not reuse the reserved identity",
        );
    }
}
