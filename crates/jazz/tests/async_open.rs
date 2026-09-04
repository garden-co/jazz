use std::cell::Cell;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};

use futures::StreamExt;
use futures::executor::block_on;
use futures::task::noop_waker;
use jazz::db::{
    Db, DbConfig, DbIdentity, ExclusiveTxOps, LocalUpdates, Propagation, ReadOpts, Transport,
};
use jazz::groove::records::Value;
use jazz::groove::storage::{TestStorage, TestStorageOperation};
use jazz::ids::{AuthorSubject, NodeUuid};
use jazz::protocol::SyncMessage;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
use jazz::tx::DurabilityTier;
use jazz::wire::TransportError;

fn schema() -> JazzSchema {
    let source = SchemaBuilder::new()
        .table(TableSchemaBuilder::new("todos").column("title", ColumnType::Text))
        .build();
    JazzSchema::new(&source).expect("async-open public schema compiles")
}

fn config(storage: TestStorage) -> DbConfig<TestStorage> {
    config_for(
        storage,
        NodeUuid::from_bytes([0x31; 16]),
        AuthorSubject::for_test_bytes([0x41; 16]),
    )
}

fn config_for(
    storage: TestStorage,
    node: NodeUuid,
    author: AuthorSubject,
) -> DbConfig<TestStorage> {
    DbConfig::new(schema(), storage, DbIdentity { node, author })
}

#[derive(Default)]
struct SubscriptionWireCounts {
    subscribes: Cell<usize>,
    unsubscribes: Cell<usize>,
}

struct CountingTransport {
    counts: Rc<SubscriptionWireCounts>,
}

impl Transport for CountingTransport {
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
        match message {
            SyncMessage::Subscribe(_) => self.counts.subscribes.update(|count| count + 1),
            SyncMessage::Unsubscribe { .. } => {
                self.counts.unsubscribes.update(|count| count + 1);
            }
            _ => {}
        }
        Ok(())
    }

    fn try_recv(&mut self) -> Option<SyncMessage> {
        None
    }
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
        Default::default(),
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

/// Alice prepares a second query while her first cold read owns the async node
/// turn. This isolates #2497 below NAPI and the TypeScript adapter: query
/// runtime preparation must wait without re-entering a node operation
/// suspended on storage, then make progress when that owner is released.
///
/// alice/read A ──cold scan──► node mutex
/// alice/prepare B ──────────► same database
#[test]
fn reproduces_sync_query_preparation_reentering_a_cold_read() {
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
        Default::default(),
    ))
    .expect("seed todo");
    let table = db.table("todos");
    let prepared = db.prepare_query(&table).expect("prepare first query");

    storage.evict_all();
    control.pause();
    let waker = noop_waker();
    let mut context = Context::from_waker(&waker);
    let mut first = Box::pin(db.all(
        &prepared,
        ReadOpts {
            tier: DurabilityTier::Local,
            local_updates: LocalUpdates::Immediate,
            propagation: Propagation::LocalOnly,
            ..ReadOpts::default()
        },
    ));
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

    let mut preparation = Box::pin(db.prepare_query_async(&table));
    assert!(matches!(
        preparation.as_mut().poll(&mut context),
        Poll::Pending
    ));
    control.resume();
    assert_eq!(block_on(first).expect("first read completes").len(), 1);
    let second = block_on(preparation).expect("waiting preparation completes");
    assert_eq!(
        block_on(db.all(
            &second,
            ReadOpts {
                tier: DurabilityTier::Local,
                local_updates: LocalUpdates::Immediate,
                propagation: Propagation::LocalOnly,
                ..ReadOpts::default()
            }
        ))
        .expect("second read completes")
        .len(),
        1
    );
}

/// Transaction relation reads use the same async owner/query path as ordinary
/// relation reads. A cold storage operation must suspend the caller rather
/// than falling back to a synchronous owner view.
#[test]
fn exclusive_transaction_relation_snapshot_suspends_on_cold_storage() {
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
        Default::default(),
    ))
    .expect("seed todo");
    let prepared = db
        .prepare_query(&db.table("todos"))
        .expect("prepare todos query");
    let tx = block_on(db.exclusive_tx()).expect("open exclusive transaction");

    storage.evict_all();
    control.pause();
    let waker = noop_waker();
    let mut context = Context::from_waker(&waker);
    let mut read = Box::pin(tx.relation_snapshot_prepared_with_opts(
        &prepared,
        ReadOpts {
            tier: DurabilityTier::Local,
            local_updates: LocalUpdates::Immediate,
            propagation: Propagation::LocalOnly,
            ..ReadOpts::default()
        },
    ));
    assert!(matches!(
        Pin::new(&mut read).poll(&mut context),
        Poll::Pending
    ));
    assert!(
        control.observed().iter().any(|operation| matches!(
            operation,
            TestStorageOperation::ScanOpen | TestStorageOperation::Get
        )),
        "the transaction relation read must reach the actual asynchronous storage path"
    );

    control.resume();
    let snapshot = block_on(read).expect("cold transaction relation read resumes");
    assert_eq!(snapshot.root_count, 1);
}

/// Alice explicitly closes a subscription without installing a scheduler or
/// driving a separate database tick. Repeated and post-shutdown closes remain
/// harmless, while plain Drop keeps its queued-cleanup behaviour.
#[test]
fn explicit_subscription_close_drives_finalization_without_an_external_tick() {
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

    block_on(subscription.close()).expect("explicit close drives its own finalization");
    assert_eq!(db.active_groove_subscriptions_for_test(), 0);
    assert!(
        block_on(subscription.next()).is_none(),
        "explicit close must make the public Stream terminal, not merely stop its producer"
    );
    block_on(subscription.close()).expect("repeated close is a no-op");
    assert!(
        block_on(subscription.next_event()).is_none(),
        "explicit close must terminate next_event"
    );
    assert!(
        subscription.try_next_event().is_none(),
        "explicit close must terminate try_next_event"
    );

    let dropped = block_on(db.subscribe(&prepared, opts.clone())).expect("open dropped stream");
    drop(dropped);
    assert_eq!(
        db.active_groove_subscriptions_for_test(),
        1,
        "plain Drop must remain non-blocking until the next owner turn"
    );
    block_on(db.tick()).expect("tick drains the plain Drop command");
    assert_eq!(db.active_groove_subscriptions_for_test(), 0);

    let mut post_shutdown = block_on(db.subscribe(&prepared, opts)).expect("open shutdown stream");
    block_on(db.close()).expect("close db after finalization");
    assert_eq!(
        db.active_groove_subscriptions_for_test(),
        0,
        "Db::close retires live maintained views before storage shutdown"
    );
    block_on(post_shutdown.close()).expect("post-shutdown close is safely invalidated");
}

/// A close suspended behind the node owner remains owned by the stream. If the
/// caller cancels that future, a retry must rejoin the same completion rather
/// than report success while the maintained subscription is still resident.
#[test]
fn cancelled_subscription_close_rejoins_blocked_finalization_without_a_tick() {
    let schema = schema();
    let families = schema.column_families();
    let refs = families.iter().map(String::as_str).collect::<Vec<_>>();
    let db =
        block_on(Db::open(config(TestStorage::new(&refs)))).expect("open subscription test db");
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

    let waker = noop_waker();
    let mut context = Context::from_waker(&waker);
    let mut node_owner = Box::pin(db.hold_node_owner_for_test());
    assert!(matches!(
        node_owner.as_mut().poll(&mut context),
        Poll::Pending
    ));

    let mut first_close = Box::pin(subscription.close());
    assert!(matches!(
        first_close.as_mut().poll(&mut context),
        Poll::Pending
    ));
    drop(first_close);
    assert!(subscription.try_next_event().is_none());
    assert!(block_on(subscription.next_event()).is_none());
    assert!(block_on(subscription.next()).is_none());

    let mut retry = Box::pin(subscription.close());
    assert!(
        matches!(retry.as_mut().poll(&mut context), Poll::Pending),
        "retry must await the retained completion while the node remains blocked"
    );
    drop(node_owner);

    block_on(retry).expect("retry resumes and completes the original finalization");
    assert_eq!(db.active_groove_subscriptions_for_test(), 0);
    block_on(subscription.close()).expect("completed close remains idempotent");
}

/// Two propagated streams share one upstream coverage owner. Closing the first
/// retires only its local ownership. Closing the last retires shared ownership
/// immediately, while the wire Unsubscribe remains connection-tick delivery.
#[test]
fn propagated_close_retires_shared_ownership_before_wire_unsubscribe_delivery() {
    let schema = schema();
    let families = schema.column_families();
    let refs = families.iter().map(String::as_str).collect::<Vec<_>>();
    let client = block_on(Db::open(config_for(
        TestStorage::new(&refs),
        NodeUuid::from_bytes([0x51; 16]),
        AuthorSubject::for_test_bytes([0x61; 16]),
    )))
    .expect("open client");
    let wire_counts = Rc::new(SubscriptionWireCounts::default());
    let _upstream = block_on(client.connect_upstream(Box::new(CountingTransport {
        counts: Rc::clone(&wire_counts),
    })));
    let prepared = client
        .prepare_query(&client.table("todos"))
        .expect("prepare propagated query");
    let mut first =
        block_on(client.subscribe(&prepared, ReadOpts::default())).expect("open first stream");
    let mut second =
        block_on(client.subscribe(&prepared, ReadOpts::default())).expect("open second stream");

    block_on(client.tick()).expect("send shared subscription");
    assert_eq!(
        wire_counts.subscribes.get(),
        1,
        "shared propagated coverage must send one Subscribe"
    );
    assert_eq!(client.query_coverage_attachment_counts_for_test().0, 1);

    block_on(first.close()).expect("close first shared owner");
    assert_eq!(client.active_groove_subscriptions_for_test(), 1);
    assert_eq!(client.query_coverage_attachment_counts_for_test().0, 1);
    block_on(client.tick()).expect("flush after first owner closes");
    assert_eq!(
        wire_counts.unsubscribes.get(),
        0,
        "the first close must preserve shared upstream coverage"
    );

    block_on(second.close()).expect("close final shared owner");
    assert_eq!(client.active_groove_subscriptions_for_test(), 0);
    assert_eq!(client.query_coverage_attachment_counts_for_test().0, 0);
    assert_eq!(
        wire_counts.unsubscribes.get(),
        0,
        "close acknowledges local ownership retirement before wire delivery"
    );
    block_on(second.close()).expect("repeat final close before wire delivery");
    block_on(client.tick()).expect("deliver final Unsubscribe");
    assert_eq!(
        wire_counts.unsubscribes.get(),
        1,
        "the last shared owner must queue exactly one wire Unsubscribe"
    );
}

/// Closing the database closes finalization admission before its storage close
/// can suspend. A stream dropped during that suspension is already included
/// in the terminal retirement set, so its non-blocking Drop cannot strand a
/// local maintained view or an explicit close acknowledgement.
#[test]
fn db_close_and_late_stream_drop_share_one_terminal_retirement_boundary() {
    let schema = schema();
    let families = schema.column_families();
    let refs = families.iter().map(String::as_str).collect::<Vec<_>>();
    let (storage, control) = TestStorage::controlled(&refs);
    let db = block_on(Db::open(config(storage))).expect("open test db");
    let prepared = db
        .prepare_query(&db.table("todos"))
        .expect("prepare todos query");
    let stream = block_on(db.subscribe(&prepared, ReadOpts::default())).expect("open stream");
    assert_eq!(db.active_groove_subscriptions_for_test(), 1);

    control.pause();
    let waker = noop_waker();
    let mut context = Context::from_waker(&waker);
    let mut shutdown = Box::pin(db.close());
    assert!(matches!(
        shutdown.as_mut().poll(&mut context),
        Poll::Pending
    ));
    drop(stream);
    control.resume();
    block_on(shutdown).expect("finish close after late stream drop");
    assert_eq!(
        db.active_groove_subscriptions_for_test(),
        0,
        "terminal retirement must remove the live Groove subscription"
    );
}
