use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::executor::block_on;
use futures::task::noop_waker;
use jazz::db::{Db, DbConfig, DbIdentity, LocalUpdates, Propagation, ReadOpts};
use jazz::groove::records::Value;
use jazz::groove::storage::{TestStorage, TestStorageOperation};
use jazz::ids::{AuthorId, NodeUuid};
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
use jazz::tx::DurabilityTier;

fn schema() -> JazzSchema {
    let source = SchemaBuilder::new()
        .table(TableSchemaBuilder::new("todos").column("title", ColumnType::Text))
        .build();
    JazzSchema::new(&source).expect("async-open public schema compiles")
}

fn config(storage: TestStorage) -> DbConfig<TestStorage> {
    DbConfig::new(
        schema(),
        storage,
        DbIdentity {
            node: NodeUuid::from_bytes([0x31; 16]),
            author: AuthorId::from_bytes([0x41; 16]),
        },
    )
}

#[test]
fn open_suspends_on_storage_and_can_be_cancelled_cleanly() {
    let schema = schema();
    let column_families = schema.column_families();
    let refs = column_families
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    let (storage, control) = TestStorage::controlled(&refs);

    control.pause();
    let mut open = Box::pin(Db::open(config(storage.clone())));
    let waker = noop_waker();
    let mut context = Context::from_waker(&waker);
    assert!(matches!(
        Pin::new(&mut open).poll(&mut context),
        Poll::Pending
    ));
    assert!(
        control.observed().iter().any(|operation| matches!(
            operation,
            TestStorageOperation::ScanOpen | TestStorageOperation::Get
        )),
        "opening must reach the actual asynchronous storage path"
    );

    drop(open);
    control.resume();
    block_on(Db::open(config(storage))).expect("cancelled cold open remains recoverable");
}

/// Alice starts a cold one-shot query, then opens a second relation snapshot
/// and a subscription before its storage scan resumes. Both callers must wait
/// for the async node owner rather than panic through the legacy synchronous
/// mutex path.
///
/// alice/query A ──cold scan──► node mutex
/// alice/query B ──wait────────► node mutex ──► relation snapshot + subscription
#[test]
fn concurrent_cold_reads_and_subscription_wait_for_the_async_node_owner() {
    let schema = schema();
    let column_families = schema.column_families();
    let refs = column_families
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    let (storage, control) = TestStorage::controlled(&refs);
    let db = block_on(Db::open(config(storage.clone()))).expect("open test db");
    block_on(db.insert(
        "todos",
        [("title".to_owned(), Value::String("cold read".to_owned()))].into(),
    ))
    .expect("seed todo");
    let prepared = db
        .prepare_query(&db.table("todos"))
        .expect("prepare todos query");
    let opts = ReadOpts {
        tier: DurabilityTier::Local,
        local_updates: LocalUpdates::Immediate,
        propagation: Propagation::LocalOnly,
        ..ReadOpts::default()
    };

    storage.evict_all();
    control.pause();
    let waker = noop_waker();
    let mut context = Context::from_waker(&waker);
    let mut first = Box::pin(db.all(&prepared, opts.clone()));
    assert!(matches!(
        Pin::new(&mut first).poll(&mut context),
        Poll::Pending
    ));
    assert!(
        control.observed().iter().any(|operation| matches!(
            operation,
            TestStorageOperation::ScanOpen | TestStorageOperation::Get
        )),
        "the first query must own a cold storage operation"
    );

    let mut second = Box::pin(db.all_relation_snapshot(&prepared, opts.clone()));
    assert!(matches!(
        Pin::new(&mut second).poll(&mut context),
        Poll::Pending
    ));
    let mut third = Box::pin(db.subscribe(&prepared, opts));
    assert!(matches!(
        Pin::new(&mut third).poll(&mut context),
        Poll::Pending
    ));
    drop(first);
    control.resume();
    let snapshot = block_on(second).expect("second relation snapshot waits then completes");
    assert_eq!(snapshot.root_count, 1);
    let mut subscription = block_on(third).expect("subscription waits then opens");
    let opened = block_on(subscription.next_event()).expect("subscription initial event");
    let jazz::db::SubscriptionEvent::Delta { reset, added, .. } = opened else {
        panic!("expected initial subscription delta");
    };
    assert!(reset);
    assert_eq!(added.len(), 1);
}

/// Alice explicitly closes a subscription. Closing queues the same command as
/// Drop, waits for its tick acknowledgement, and remains harmless when called
/// again or after the database has begun shutdown.
#[test]
fn explicit_subscription_close_waits_for_tick_and_is_idempotent_through_shutdown() {
    let schema = schema();
    let families = schema.column_families();
    let refs = families.iter().map(String::as_str).collect::<Vec<_>>();
    let (storage, _control) = TestStorage::controlled(&refs);
    let db = block_on(Db::open(config(storage))).expect("open test db");
    let prepared = db
        .prepare_query(&db.table("todos"))
        .expect("prepare todos query");
    let opts = ReadOpts {
        tier: DurabilityTier::Local,
        local_updates: LocalUpdates::Immediate,
        propagation: Propagation::LocalOnly,
        ..ReadOpts::default()
    };
    let mut subscription =
        block_on(db.subscribe(&prepared, opts.clone())).expect("open subscription");
    let mut close = Box::pin(subscription.close());
    let waker = noop_waker();
    let mut context = Context::from_waker(&waker);
    assert!(matches!(
        Pin::new(&mut close).poll(&mut context),
        Poll::Pending
    ));
    block_on(db.tick()).expect("drain explicit close command");
    assert!(matches!(
        Pin::new(&mut close).poll(&mut context),
        Poll::Ready(Ok(()))
    ));
    drop(close);
    assert_eq!(db.active_groove_subscriptions_for_test(), 0);

    block_on(subscription.close()).expect("repeated close is a no-op");
    let mut cancelled_close =
        block_on(db.subscribe(&prepared, opts.clone())).expect("open cancellation-safe close");
    let mut close_future = Box::pin(cancelled_close.close());
    assert!(matches!(
        Pin::new(&mut close_future).poll(&mut context),
        Poll::Pending
    ));
    drop(close_future);
    block_on(db.tick()).expect("dropped close future leaves its command queued");
    assert_eq!(db.active_groove_subscriptions_for_test(), 0);
    block_on(cancelled_close.close()).expect("queued close remains idempotent");

    let mut post_shutdown = block_on(db.subscribe(&prepared, opts)).expect("open shutdown stream");
    block_on(db.close()).expect("close db after finalization");
    block_on(post_shutdown.close()).expect("post-shutdown close is safely invalidated");
}
