use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, VecDeque};
use std::fmt::Debug;
use std::future::Future;
use std::rc::Rc;

mod common;

use jazz::db::{
    Db, DbConfig, DbIdentity, ExclusiveTxOps, Propagation, ReadOpts, SubscriptionEvent,
    TickScheduler, TickUrgency, Transport, block_on,
};
use jazz::groove::records::{BorrowedRecord, Value};
use jazz::groove::storage::{TestStorage, TestStorageOperation};
use jazz::ids::{AuthorSubject, NodeUuid};
use jazz::node::{CommitUnitTrust, CurrentRow};
use jazz::protocol::{
    RegisterShapeOptions, ShapeAst, Subscribe, SubscribeRejectReason, SubscriptionKey, SyncMessage,
};
use jazz::query::{ArraySubquery, BindingId, OrderDirection, Query, col, eq, lit};
use jazz::schema::JazzSchema;
use jazz::tools::{
    ColumnType, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder, TransactionId,
};
use jazz::tx::{DurabilityTier, Fate};
use jazz_storage_rocksdb::RocksDbStorage;
use jazz_testkit::duplex_transport::duplex;

/// Mirror the production browser-worker upstream: the client side has already
/// been admitted to forward one scope binding, and the authority side installs
/// that exact server-issued capability. A plain duplex or generic relay is not
/// a substitute for either half.
macro_rules! connect_scope_isolated_worker_to_core {
    ($worker:expr, $core:expr, $identity:expr) => {{
        let (worker_upstream_transport, core_transport) = duplex();
        let worker_upstream = block_on($worker.connect_upstream(worker_upstream_transport));
        let core_subscriber = $core.accept_scope_isolated_relay_subscriber_for_test(
            core_transport,
            $identity,
            BTreeMap::new(),
            1,
        );
        (worker_upstream, core_subscriber)
    }};
}

#[derive(Default)]
struct AuthorityTransportState {
    inbound: VecDeque<SyncMessage>,
    outbound: Vec<SyncMessage>,
    rejection: Option<SubscribeRejectReason>,
}

/// Minimal scripted authority used to make relay lifecycle tests independent
/// of timing and of the authority's query evaluator.
struct ScriptedAuthorityTransport(Rc<RefCell<AuthorityTransportState>>);

impl Transport for ScriptedAuthorityTransport {
    fn send(&mut self, message: SyncMessage) -> Result<(), jazz::wire::TransportError> {
        let mut state = self.0.borrow_mut();
        if let SyncMessage::Subscribe(subscribe) = &message
            && let Some(reason) = state.rejection.clone()
        {
            state.inbound.push_back(SyncMessage::SubscribeRejected {
                subscription: subscribe.subscription,
                reason,
            });
        }
        state.outbound.push(message);
        Ok(())
    }

    fn try_recv(&mut self) -> Option<SyncMessage> {
        self.0.borrow_mut().inbound.pop_front()
    }
}

fn scripted_authority(
    rejection: Option<SubscribeRejectReason>,
) -> (Box<dyn Transport>, Rc<RefCell<AuthorityTransportState>>) {
    let state = Rc::new(RefCell::new(AuthorityTransportState {
        rejection,
        ..Default::default()
    }));
    (
        Box::new(ScriptedAuthorityTransport(Rc::clone(&state))),
        state,
    )
}

/// Marks the worker's core-facing transport as the authenticated backend
/// relay boundary that is allowed to forward the downstream session scope.
struct TrustedBackendRelayTransport {
    inner: Box<dyn Transport>,
}

impl Transport for TrustedBackendRelayTransport {
    fn send(&mut self, message: SyncMessage) -> Result<(), jazz::wire::TransportError> {
        self.inner.send(message)
    }

    fn try_recv(&mut self) -> Option<SyncMessage> {
        self.inner.try_recv()
    }

    fn connection_session_context(&self) -> Option<jazz::db::ConnectionSessionContext> {
        self.inner.connection_session_context()
    }

    fn permits_delegated_sessions(&self) -> bool {
        true
    }
}

use common::compile_schema;

trait FutureResultExpectExt<T, E>: Future<Output = Result<T, E>> + Sized {
    fn expect(self, message: &str) -> T
    where
        E: Debug,
    {
        block_on(self).expect(message)
    }
}

impl<F, T, E> FutureResultExpectExt<T, E> for F where F: Future<Output = Result<T, E>> {}

#[derive(Default)]
struct CountingScheduler {
    calls: Cell<usize>,
    urgencies: RefCell<Vec<TickUrgency>>,
    deadlines_ms: RefCell<Vec<u64>>,
}

impl TickScheduler for CountingScheduler {
    fn schedule_tick(&self, urgency: TickUrgency) {
        self.calls.set(self.calls.get() + 1);
        self.urgencies.borrow_mut().push(urgency);
    }

    fn schedule_tick_after(&self, delay_ms: u64) {
        // This is a paused test host: retain deadlines for the harness rather
        // than converting them to an immediate callback.
        self.deadlines_ms.borrow_mut().push(delay_ms);
    }
}

impl CountingScheduler {
    /// The raw-Db topology harness owns turns itself. Reset its observation
    /// window immediately before the transition whose host wake is under test.
    fn clear(&self) {
        self.calls.set(0);
        self.urgencies.borrow_mut().clear();
        self.deadlines_ms.borrow_mut().clear();
    }

    fn take_urgencies(&self) -> Vec<TickUrgency> {
        self.calls.set(0);
        std::mem::take(&mut *self.urgencies.borrow_mut())
    }
}

fn assert_scheduled_urgencies(
    scheduler: &CountingScheduler,
    expected: &[TickUrgency],
    transition: &str,
) {
    assert_eq!(
        scheduler.take_urgencies(),
        expected,
        "{transition} must preserve its exact owner-turn wake requests",
    );
}

fn schema() -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            // Scope-isolated relay tests exercise server-authorized transport,
            // not missing-policy rejection. Explicitly model the permissive
            // example-app policy that the old trusted SYSTEM fixture had
            // accidentally bypassed.
            .table(
                TableSchemaBuilder::new("todos")
                    .column("title", ColumnType::Text)
                    .policies(
                        TablePolicies::new()
                            .with_select(PolicyExpr::True)
                            .with_insert(PolicyExpr::True)
                            .with_update(Some(PolicyExpr::True), PolicyExpr::True)
                            .with_delete(PolicyExpr::True),
                    ),
            )
            .build(),
    )
}

fn write_only_policy_schema() -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("todos")
                    .column("title", ColumnType::Text)
                    .policies(TablePolicies::new().with_insert(PolicyExpr::True)),
            )
            .build(),
    )
}

fn included_relation_schema() -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(TableSchemaBuilder::new("profiles").column("name", ColumnType::Text))
            .table(
                TableSchemaBuilder::new("messages")
                    .fk_column("author", "profiles")
                    .column("body", ColumnType::Text)
                    .column("created", ColumnType::Timestamp),
            )
            .build(),
    )
}

fn open_db(node: u8, author: AuthorSubject, schema: &JazzSchema) -> Db<TestStorage> {
    let column_families = schema.column_families();
    let refs = column_families
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    block_on(Db::open(DbConfig::new(
        schema.clone(),
        TestStorage::new(&refs),
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author,
        },
    )))
    .expect("open database")
}

fn open_db_with_storage(
    node: u8,
    author: AuthorSubject,
    schema: &JazzSchema,
    storage: TestStorage,
) -> Db<TestStorage> {
    block_on(Db::open(DbConfig::new(
        schema.clone(),
        storage,
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author,
        },
    )))
    .expect("open database")
}

fn open_core(node: u8, schema: &JazzSchema) -> Db<TestStorage> {
    let column_families = schema.column_families();
    let refs = column_families
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    block_on(Db::open_history_complete(DbConfig::new(
        schema.clone(),
        TestStorage::new(&refs),
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .expect("open core database")
}

fn assert_truthful_empty_local_opening(event: Option<SubscriptionEvent>) {
    let Some(SubscriptionEvent::Delta {
        reset,
        tier,
        added,
        updated,
        removed,
        ..
    }) = event
    else {
        panic!("Local-tier subscription must publish an opening delta");
    };
    assert!(reset, "the local opening must replace prior membership");
    assert_eq!(tier, DurabilityTier::Local);
    assert!(added.is_empty());
    assert!(updated.is_empty());
    assert!(removed.is_empty());
}

fn open_persistent_worker(
    path: &std::path::Path,
    node: u8,
    schema: &JazzSchema,
) -> Db<RocksDbStorage> {
    let column_families = schema.column_families();
    let refs = column_families
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    let storage = RocksDbStorage::open(path, &refs).expect("open persistent worker storage");
    block_on(Db::open(DbConfig::new(
        schema.clone(),
        storage,
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .expect("open persistent worker")
}

/// Mirrors the browser worker host: its durable replica node is distinct from
/// the foreground node, but it opens under the browser session author so it
/// can recover only that session's unresolved relayed writes.
fn open_persistent_browser_worker(
    path: &std::path::Path,
    node: u8,
    author: AuthorSubject,
    schema: &JazzSchema,
) -> Db<RocksDbStorage> {
    let column_families = schema.column_families();
    let refs = column_families
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    let storage =
        RocksDbStorage::open(path, &refs).expect("open persistent browser worker storage");
    let db = block_on(Db::open(DbConfig::new(
        schema.clone(),
        storage,
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author,
        },
    )))
    .expect("open persistent browser worker");
    db.restore_browser_relay_pending_uploads()
        .expect("restore browser relay pending uploads");
    db
}

/// A browser main-thread write is optimistic but not Local-durable until the
/// dedicated worker persists it. Alice owns the non-durable main-thread Db and
/// the worker is a fate-neutral relay with no upstream server.
///
/// ```text
/// alice main Db ──CommitUnit(None)──► worker relay
///       │                                  │
///       ├─ local query sees row            └─ persist Pending/Local
///       │                                             │
///       └─ wait(Local) ◄──FateUpdate(Pending/Local)───┘
/// ```
#[test]
fn non_durable_browser_client_waits_for_worker_local_ack() {
    let schema = schema();
    let alice = AuthorSubject::for_test_bytes([0xa1; 16]);
    let main_thread = open_db(0x11, alice, &schema);
    let worker = open_db(0x22, AuthorSubject::SYSTEM, &schema);
    main_thread.set_non_durable_client();

    let (main_transport, worker_transport) = duplex();
    let _main_connection = jazz::db::block_on(main_thread.connect_upstream(main_transport));
    let _worker_connection = worker.accept_subscriber(worker_transport, alice);

    let write = main_thread
        .insert(
            "todos",
            BTreeMap::from([(
                "title".to_owned(),
                Value::String("persist me in the worker".to_owned()),
            )]),
            Default::default(),
        )
        .expect("insert optimistic todo");
    let tx_id = write.mergeable_tx_id();

    let todos = main_thread
        .prepare_query(&main_thread.table("todos"))
        .expect("prepare todos query");
    assert_eq!(
        main_thread.read(&todos).expect("read local preview").len(),
        1
    );
    assert_eq!(
        main_thread
            .write_state(tx_id)
            .expect("main write state")
            .durability,
        DurabilityTier::None
    );

    let local_wait = Rc::new(Cell::new(None));
    let observed_wait = Rc::clone(&local_wait);
    main_thread.wait_for_transaction_with(tx_id, DurabilityTier::Local, move |result| {
        observed_wait.set(Some(result.is_ok()));
    });
    assert_eq!(
        local_wait.get(),
        None,
        "Local wait must initially be pending"
    );

    main_thread.tick().expect("upload commit to worker");
    worker
        .tick()
        .expect("worker persists commit and acknowledges it");
    assert_eq!(
        worker.write_state(tx_id).expect("worker write state"),
        jazz::db::WriteState {
            fate: Fate::Pending,
            durability: DurabilityTier::Local,
            global_time: None,
        }
    );
    main_thread.tick().expect("apply worker acknowledgement");

    assert_eq!(local_wait.get(), Some(true));
    assert_eq!(
        main_thread
            .write_state(tx_id)
            .expect("acknowledged write state"),
        jazz::db::WriteState {
            fate: Fate::Pending,
            durability: DurabilityTier::Local,
            global_time: None,
        }
    );
}

/// A worker's initial empty view can arrive after the main thread has already
/// published a newer optimistic row. The worker snapshot advances the durable
/// baseline, but must not replace the main thread's pending subscription view.
#[test]
fn browser_worker_initial_view_preserves_newer_optimistic_membership() {
    let schema = schema();
    let alice = AuthorSubject::for_test_bytes([0xaa; 16]);
    let main_thread = open_db(0x1c, alice, &schema);
    let worker = open_db(0x2c, alice, &schema);
    main_thread.set_non_durable_client();

    let (main_transport, worker_transport) = duplex();
    let _main_connection = jazz::db::block_on(main_thread.connect_upstream(main_transport));
    let _worker_connection = worker.accept_subscriber(worker_transport, alice);

    let open_todos = main_thread
        .prepare_query(
            &main_thread
                .table("todos")
                .filter(eq(col("title"), lit("open")))
                .order_by("title", OrderDirection::Asc),
        )
        .expect("prepare filtered todos query");
    let mut subscription = block_on(main_thread.subscribe(&open_todos, ReadOpts::default()))
        .expect("subscribe to open todos");
    assert_truthful_empty_local_opening(subscription.try_next_event());

    let insert = main_thread
        .insert(
            "todos",
            BTreeMap::from([("title".to_owned(), Value::String("open".to_owned()))]),
            Default::default(),
        )
        .expect("insert optimistic open todo");
    let row = insert.row_uuid();
    let optimistic = std::iter::from_fn(|| subscription.try_next_event()).collect::<Vec<_>>();
    assert!(optimistic.iter().any(|event| matches!(
        event,
        SubscriptionEvent::Delta { added, .. } if added.len() == 1
    )));

    // FIFO makes the worker serve the subscription's initial empty view before
    // it ingests and acknowledges the later commit in this same tick.
    main_thread
        .tick()
        .expect("send subscription and optimistic commit to worker");
    worker
        .tick()
        .expect("serve initial view, persist commit, and acknowledge it");
    main_thread
        .tick()
        .expect("apply stale initial view and Local acknowledgement");
    assert_eq!(
        main_thread
            .write_state(insert.mergeable_tx_id())
            .expect("acknowledged insert state")
            .durability,
        DurabilityTier::Local
    );
    let after_ack = std::iter::from_fn(|| subscription.try_next_event()).collect::<Vec<_>>();
    assert!(
        !after_ack
            .iter()
            .any(|event| matches!(event, SubscriptionEvent::Delta { reset: true, .. })),
        "the worker's internal hydration must not reset the main subscription: {after_ack:?}"
    );
    assert_eq!(
        main_thread
            .read(&open_todos)
            .expect("read after stale worker view")
            .len(),
        1,
        "the stale worker view must not retract the optimistic row"
    );

    main_thread
        .update(
            "todos",
            row,
            BTreeMap::from([("title".to_owned(), Value::String("done".to_owned()))]),
            Default::default(),
        )
        .expect("move optimistic todo out of filtered subscription");
    assert!(
        main_thread
            .read(&open_todos)
            .expect("read updated filtered query")
            .is_empty()
    );
    let after_update = std::iter::from_fn(|| subscription.try_next_event()).collect::<Vec<_>>();
    assert!(
        after_update.iter().any(|event| matches!(
            event,
            SubscriptionEvent::Delta { removed, .. } if removed.len() == 1
        )),
        "expected the optimistic predicate transition to emit a removal, got {after_update:?}"
    );
}

/// The worker relay forwards Alice's unchanged commit to the core and routes
/// the core's authoritative fate back to Alice after its earlier Local ack.
/// The worker is persistence and transport here, never fate authority.
///
/// ```text
/// alice main Db ──commit──► worker relay ──same commit──► core
///       ▲                       │                           │
///       ├──Pending/Local────────┘                           │
///       └──────────────Accepted/Global─────────────────────┘
/// ```
#[test]
fn worker_relay_forwards_authority_fate_to_browser_client() {
    let schema = schema();
    let alice = AuthorSubject::for_test_bytes([0xa2; 16]);
    let main_thread = open_db(0x12, alice, &schema);
    let worker = open_db(0x23, AuthorSubject::SYSTEM, &schema);
    let core = open_core(0x34, &schema);
    main_thread.set_non_durable_client();
    worker.set_relay_authority_session_owner_for_test();

    let (main_transport, worker_subscriber_transport) = duplex();
    let _main_connection = jazz::db::block_on(main_thread.connect_upstream(main_transport));
    let _worker_subscriber = worker.accept_subscriber(worker_subscriber_transport, alice);

    let (_worker_upstream, _core_subscriber) =
        connect_scope_isolated_worker_to_core!(worker, core, alice);

    let write = main_thread
        .insert(
            "todos",
            BTreeMap::from([(
                "title".to_owned(),
                Value::String("relay me unchanged".to_owned()),
            )]),
            Default::default(),
        )
        .expect("insert relayed todo");
    let tx_id = write.mergeable_tx_id();
    let row = write.row_uuid();

    let global_wait = Rc::new(Cell::new(None));
    let observed_wait = Rc::clone(&global_wait);
    main_thread.wait_for_transaction_with(tx_id, DurabilityTier::Global, move |result| {
        observed_wait.set(Some(result.is_ok()));
    });

    main_thread.tick().expect("upload to worker");
    worker.tick().expect("persist and forward to core");
    main_thread.tick().expect("apply worker Local ack");
    assert_eq!(global_wait.get(), None);

    let worker_scheduler = Rc::new(CountingScheduler::default());
    worker.set_tick_scheduler(Some(worker_scheduler.clone()));
    core.tick().expect("accept at core");
    worker_scheduler.clear();
    worker
        .tick()
        .expect("apply and forward core fate downstream");
    let ingress_wakes = worker_scheduler.take_urgencies();
    assert!(
        ingress_wakes.contains(&TickUrgency::AfterCurrentTurn),
        "core fate ingress must schedule owner-turn fate publication: {ingress_wakes:?}"
    );
    // A concurrent authoritative membership addition may also request an
    // Immediate Local wake (INV-EDGE-21). That internal wake is intentionally
    // not a second public fate projection, so assert the observable turn
    // boundary instead of treating its optional scheduler request as a stable
    // wire-level event.
    main_thread
        .tick()
        .expect("a Local wake before the owner turn cannot publish the core fate");
    assert_eq!(
        global_wait.get(),
        None,
        "the worker must not publish a Global fate before its scheduled owner turn"
    );
    worker
        .tick()
        .expect("publish core fate on the scheduled worker follow-up turn");
    main_thread.tick().expect("apply core fate through worker");

    assert_eq!(global_wait.get(), Some(true));
    assert!(matches!(
        main_thread.write_state(tx_id).expect("main global state"),
        jazz::db::WriteState {
            fate: Fate::Accepted,
            durability: DurabilityTier::Global,
            global_time: Some(_),
        }
    ));
    assert!(matches!(
        worker.write_state(tx_id).expect("worker global state"),
        jazz::db::WriteState {
            fate: Fate::Accepted,
            durability: DurabilityTier::Global,
            global_time: Some(_),
        }
    ));

    let todos = main_thread
        .prepare_query(&main_thread.table("todos"))
        .expect("prepare edge todos query");
    let mut edge_subscription = block_on(main_thread.subscribe(
        &todos,
        ReadOpts {
            tier: DurabilityTier::Edge,
            ..ReadOpts::default()
        },
    ))
    .expect("subscribe at Edge through worker relay");
    while edge_subscription.try_next_event().is_some() {}
    main_thread.tick().expect("request worker Edge view");
    for _ in 0..4 {
        worker.tick().expect("forward worker Edge coverage");
        core.tick().expect("serve worker Edge coverage globally");
        worker.tick().expect("serve settled worker Edge view");
        main_thread.tick().expect("apply worker Edge view");
    }
    assert_eq!(
        main_thread
            .read(&todos)
            .expect("read relayed Edge row")
            .len(),
        1
    );
    let events = std::iter::from_fn(|| edge_subscription.try_next_event()).collect::<Vec<_>>();
    let additions = events
        .iter()
        .filter(|event| matches!(event, SubscriptionEvent::Delta { added, .. } if added.len() == 1))
        .count();
    assert_eq!(
        additions, 1,
        "the internal Local wake must contribute to one public Edge projection, not a duplicate: {events:?}"
    );
    assert_eq!(
        events.len(),
        1,
        "the worker authority source must not create a second public projection: {events:?}"
    );

    // A second mutation after the relay has installed Edge coverage must make
    // the full main -> worker -> core -> worker -> main round trip without
    // re-entering the worker's current-row projection.
    let update = main_thread
        .update(
            "todos",
            row,
            BTreeMap::from([("title".to_owned(), Value::String("relay update".to_owned()))]),
            Default::default(),
        )
        .expect("update through settled relay");
    let update_tx = update.mergeable_tx_id();
    main_thread.tick().expect("upload update to worker");
    worker.tick().expect("persist and forward update");
    core.tick().expect("accept update at core");
    worker_scheduler.clear();
    worker.tick().expect("forward update fate");
    let update_ingress_wakes = worker_scheduler.take_urgencies();
    assert!(
        update_ingress_wakes.contains(&TickUrgency::AfterCurrentTurn),
        "update fate ingress must schedule owner-turn fate publication: {update_ingress_wakes:?}"
    );
    main_thread
        .tick()
        .expect("a Local wake before the owner turn cannot publish the update fate");
    assert!(
        !matches!(
            main_thread
                .write_state(update_tx)
                .expect("update state before owner turn"),
            jazz::db::WriteState {
                durability: DurabilityTier::Global,
                ..
            }
        ),
        "the worker must not publish the update's Global fate before its owner turn"
    );
    worker
        .tick()
        .expect("publish update fate on the scheduled worker follow-up turn");
    main_thread.tick().expect("apply update fate");
    assert!(matches!(
        main_thread
            .write_state(update_tx)
            .expect("updated global state"),
        jazz::db::WriteState {
            fate: Fate::Accepted,
            durability: DurabilityTier::Global,
            global_time: Some(_),
        }
    ));
}

/// A fresh main-thread Db hydrates its application-owned subscription from the
/// worker's Local view. The worker does not become the query API owner and does
/// not require an upstream server for this persisted bootstrap.
///
/// ```text
/// alice main subscription ──RegisterShape(Local)──► worker relay
///                         ◄────Local ViewUpdate────┘
/// ```
#[test]
fn browser_client_hydrates_local_subscription_from_worker_relay() {
    let schema = schema();
    let alice = AuthorSubject::for_test_bytes([0xa3; 16]);
    let worker = open_db(0x24, AuthorSubject::SYSTEM, &schema);
    worker
        .insert(
            "todos",
            BTreeMap::from([(
                "title".to_owned(),
                Value::String("persisted before main thread opens".to_owned()),
            )]),
            Default::default(),
        )
        .expect("seed worker-local todo");

    let main_thread = open_db(0x13, alice, &schema);
    main_thread.set_non_durable_client();
    let (main_transport, worker_transport) = duplex();
    let _main_connection = jazz::db::block_on(main_thread.connect_upstream(main_transport));
    let _worker_connection = worker.accept_subscriber(worker_transport, alice);

    let todos = main_thread
        .prepare_query(&main_thread.table("todos"))
        .expect("prepare todos query");
    let mut subscription =
        block_on(main_thread.subscribe(&todos, ReadOpts::default())).expect("subscribe to todos");
    assert_truthful_empty_local_opening(subscription.try_next_event());

    let scheduler = Rc::new(CountingScheduler::default());
    worker.set_tick_scheduler(Some(scheduler.clone()));
    main_thread.tick().expect("request Local worker view");
    scheduler.clear();
    worker.tick().expect("serve Local worker view");
    // Subscription opening, inbound admission, and the node's subscriber
    // dirty epoch each request the same next owner turn. All three must remain
    // AfterCurrentTurn; a host coalesces them into the one turn driven below.
    assert_scheduled_urgencies(
        &scheduler,
        &[
            TickUrgency::AfterCurrentTurn,
            TickUrgency::AfterCurrentTurn,
            TickUrgency::AfterCurrentTurn,
        ],
        "worker Local subscription opening",
    );
    worker
        .tick()
        .expect("serve the scheduled Local worker follow-up turn");
    main_thread.tick().expect("apply Local worker view");

    assert_eq!(
        main_thread
            .read(&todos)
            .expect("read worker-hydrated local preview")
            .len(),
        1
    );
    let events = std::iter::from_fn(|| subscription.try_next_event()).collect::<Vec<_>>();
    assert!(events.iter().any(|event| matches!(
        event,
        SubscriptionEvent::Delta { added, .. } if added.len() == 1
    )));
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, SubscriptionEvent::Rejected { .. }))
    );
}

/// A Local structured read is served from the persistent worker's resident
/// state even when that worker owns a separate authority-session identity.
/// It must not wait for, or select, an upstream authority source.
#[test]
fn browser_client_hydrates_local_structured_subscription_without_authority() {
    let schema = included_relation_schema();
    let alice = AuthorSubject::for_test_bytes([0xa4; 16]);
    let worker = open_db(0x25, AuthorSubject::SYSTEM, &schema);
    worker.set_relay_authority_session_owner_for_test();
    let profile = worker
        .insert(
            "profiles",
            BTreeMap::from([(
                "name".to_owned(),
                Value::String("resident local sender".to_owned()),
            )]),
            Default::default(),
        )
        .expect("seed worker-local profile");
    let message = worker
        .insert(
            "messages",
            BTreeMap::from([
                ("author".to_owned(), Value::Uuid(profile.row_uuid().0)),
                ("body".to_owned(), Value::String("local message".to_owned())),
                ("created".to_owned(), Value::U64(1)),
            ]),
            Default::default(),
        )
        .expect("seed worker-local message");

    let main_thread = open_db(0x14, alice, &schema);
    main_thread.set_non_durable_client();
    let (main_transport, worker_transport) = duplex();
    let _main_connection = block_on(main_thread.connect_upstream(main_transport));
    let _worker_connection = worker.accept_subscriber(worker_transport, alice);
    let query = main_thread
        .prepare_query(
            &Query::from("messages")
                .array_subquery(ArraySubquery::new("sender", "profiles", "id", "author")),
        )
        .expect("prepare Local structured query");
    let mut subscription = block_on(main_thread.subscribe(
        &query,
        ReadOpts {
            tier: DurabilityTier::Local,
            ..ReadOpts::default()
        },
    ))
    .expect("subscribe to Local structured worker state");
    assert_truthful_empty_local_opening(subscription.try_next_event());

    for _ in 0..3 {
        main_thread
            .tick()
            .expect("request Local structured worker view");
        worker.tick().expect("serve Local structured worker view");
        main_thread
            .tick()
            .expect("apply Local structured worker view");
    }

    let events = std::iter::from_fn(|| subscription.try_next_event()).collect::<Vec<_>>();
    let Some(SubscriptionEvent::Delta { added, .. }) = events.iter().find(|event| {
        matches!(event, SubscriptionEvent::Delta { added, .. }
            if added.iter().any(|row| row.row.row_uuid() == message.row_uuid()))
    }) else {
        panic!("Local structured read must resolve from worker state, got {events:?}");
    };
    let root = added
        .iter()
        .find(|row| row.row.row_uuid() == message.row_uuid())
        .expect("Local structured update contains seeded message");
    let (descriptor, raw) = root.row.encoded_record();
    let record = BorrowedRecord::new(raw, descriptor);
    let Value::Array(sender) = record.get("sender").expect("nested sender field") else {
        panic!("Local structured update must materialize sender")
    };
    assert!(matches!(
        sender.as_slice(),
        [Value::Record(sender)] if matches!(sender.get("name"), Ok(Value::String(name)) if name == "resident local sender")
    ));
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, SubscriptionEvent::Rejected { .. })),
        "Local structured read cannot require an authority receipt: {events:?}"
    );
}

/// A main-thread Local subscription and a one-shot Edge read have distinct
/// downstream serving options, but both canonicalize to the worker's Global
/// upstream coverage. Retiring the one-shot usage site must not unsubscribe
/// that shared upstream coverage while the live Local subscription still owns
/// it.
#[test]
fn one_shot_edge_read_does_not_retire_live_browser_subscription_coverage() {
    let schema = schema();
    let alice = AuthorSubject::for_test_bytes([0xac; 16]);
    let main_thread = open_db(0x1e, alice, &schema);
    let worker = open_db(0x2e, AuthorSubject::SYSTEM, &schema);
    let core = open_core(0x3e, &schema);
    let writer = open_db(0x4e, AuthorSubject::for_test_bytes([0xbc; 16]), &schema);
    main_thread.set_non_durable_client();
    worker.set_relay_authority_session_owner_for_test();

    let (main_transport, worker_subscriber_transport) = duplex();
    let _main_connection = block_on(main_thread.connect_upstream(main_transport));
    let _worker_subscriber = worker.accept_subscriber(worker_subscriber_transport, alice);
    let (_worker_upstream, _core_subscriber) =
        connect_scope_isolated_worker_to_core!(worker, core, alice);
    let (writer_transport, core_writer_transport) = duplex();
    let _writer_upstream = block_on(writer.connect_upstream(writer_transport));
    let _core_writer = core.accept_subscriber(
        core_writer_transport,
        AuthorSubject::for_test_bytes([0xbc; 16]),
    );

    let todos = main_thread
        .prepare_query(&main_thread.table("todos"))
        .expect("prepare todos query");
    let mut subscription = block_on(main_thread.subscribe(&todos, ReadOpts::default()))
        .expect("open browser Local subscription");
    assert_truthful_empty_local_opening(subscription.try_next_event());

    for _ in 0..8 {
        main_thread
            .tick()
            .expect("send Local subscription to worker");
        worker.tick().expect("relay Local subscription to Core");
        core.tick().expect("serve shared upstream coverage");
        worker.tick().expect("apply Core coverage at worker");
        main_thread
            .tick()
            .expect("apply worker coverage on main thread");
    }
    while subscription.try_next_event().is_some() {}

    let edge_read = main_thread
        .attach_query_with_opts(
            &todos,
            ReadOpts {
                tier: DurabilityTier::Edge,
                ..ReadOpts::default()
            },
        )
        .expect("attach one-shot Edge read");
    for _ in 0..8 {
        main_thread.tick().expect("send one-shot Edge coverage");
        worker.tick().expect("refresh canonical worker coverage");
        core.tick().expect("serve refreshed canonical coverage");
        worker.tick().expect("apply refreshed canonical coverage");
        main_thread.tick().expect("apply one-shot Edge receipt");
        if main_thread.query_attachment_is_covered(&edge_read) {
            break;
        }
    }
    assert!(
        main_thread.query_attachment_is_covered(&edge_read),
        "the one-shot Edge usage site never received its own authority receipt",
    );

    main_thread.detach_query(edge_read);
    for _ in 0..4 {
        main_thread.tick().expect("retire one-shot Edge usage site");
        worker.tick().expect("retain shared worker coverage");
        core.tick().expect("process any upstream lifecycle traffic");
        worker.tick().expect("apply upstream lifecycle traffic");
    }

    writer
        .insert(
            "todos",
            BTreeMap::from([(
                "title".to_owned(),
                Value::String("still live after one-shot read".to_owned()),
            )]),
            Default::default(),
        )
        .expect("insert writer row after one-shot detach");
    for _ in 0..8 {
        writer.tick().expect("upload writer row");
        core.tick().expect("publish authority row");
        writer.tick().expect("apply writer settlement");
        worker
            .tick()
            .expect("relay authority row to browser worker");
        main_thread
            .tick()
            .expect("apply authority row on main thread");
    }

    let events = std::iter::from_fn(|| subscription.try_next_event()).collect::<Vec<_>>();
    assert!(
        events.iter().any(|event| matches!(
            event,
            SubscriptionEvent::Delta { added, .. } if added.len() == 1
        )),
        "retiring the one-shot Edge read also retired live Local subscription coverage: {events:?}",
    );
}

/// Dropping a downstream relay link has no stream `Drop` to clean up. Each
/// connection-scoped upstream owner must therefore be retired exactly once,
/// while an identical coverage request on a sibling connection remains live.
#[test]
fn worker_relay_abrupt_detach_and_reconnect_keep_upstream_owners_bounded() {
    let schema = schema();
    let worker = open_db(0x60, AuthorSubject::SYSTEM, &schema);
    let (authority, authority_state) = scripted_authority(None);
    let _worker_upstream = block_on(worker.connect_upstream(authority));

    let attach_browser = |node, author| {
        let browser = open_db(node, author, &schema);
        browser.set_non_durable_client();
        let (browser_transport, worker_transport) = duplex();
        let browser_connection = block_on(browser.connect_upstream(browser_transport));
        let worker_connection = worker.accept_subscriber(worker_transport, author);
        let todos = browser
            .prepare_query(&browser.table("todos"))
            .expect("prepare browser todos query");
        let subscription = block_on(browser.subscribe(&todos, ReadOpts::default()))
            .expect("open browser subscription");
        (browser, browser_connection, worker_connection, subscription)
    };

    let alice = AuthorSubject::for_test_bytes([0x61; 16]);
    let (alice_browser, alice_upstream, alice_downstream, _alice_subscription) =
        attach_browser(0x61, alice);
    for _ in 0..3 {
        alice_browser.tick().expect("send Alice subscription");
        worker.tick().expect("relay Alice subscription");
    }
    assert_eq!(worker.relay_upstream_subscription_owner_count_for_test(), 1);

    let bob = AuthorSubject::for_test_bytes([0x62; 16]);
    let (bob_browser, bob_upstream, bob_downstream, _bob_subscription) = attach_browser(0x62, bob);
    for _ in 0..3 {
        bob_browser.tick().expect("send Bob subscription");
        worker.tick().expect("relay Bob subscription");
    }
    assert_eq!(worker.relay_upstream_subscription_owner_count_for_test(), 2);

    assert!(worker.detach_connection(&alice_downstream));
    assert_eq!(
        worker.relay_upstream_subscription_owner_count_for_test(),
        1,
        "abruptly detaching Alice must retain Bob's identical coverage owner",
    );
    worker.tick().expect("retire only Alice upstream owner");
    assert!(!worker.detach_connection(&alice_downstream));
    assert!(alice_browser.detach_connection(&alice_upstream));

    for (node, author) in [(0x63, [0x63; 16]), (0x64, [0x64; 16]), (0x65, [0x65; 16])] {
        let author = AuthorSubject::for_test_bytes(author);
        let (browser, upstream, downstream, _subscription) = attach_browser(node, author);
        for _ in 0..3 {
            browser
                .tick()
                .expect("send reconnected browser subscription");
            worker
                .tick()
                .expect("relay reconnected browser subscription");
        }
        assert_eq!(
            worker.relay_upstream_subscription_owner_count_for_test(),
            2,
            "one reconnect owner plus Bob's still-live owner",
        );
        assert!(worker.detach_connection(&downstream));
        worker.tick().expect("retire reconnect owner");
        assert_eq!(
            worker.relay_upstream_subscription_owner_count_for_test(),
            1,
            "a detached reconnect must not accumulate an orphaned owner",
        );
        assert!(browser.detach_connection(&upstream));
    }

    assert!(worker.detach_connection(&bob_downstream));
    worker.tick().expect("retire final sibling owner");
    assert_eq!(worker.relay_upstream_subscription_owner_count_for_test(), 0);
    assert!(bob_browser.detach_connection(&bob_upstream));

    let messages = &authority_state.borrow().outbound;
    let subscribes = messages
        .iter()
        .filter(|message| matches!(message, SyncMessage::Subscribe(_)))
        .count();
    let unsubscribes = messages
        .iter()
        .filter(|message| matches!(message, SyncMessage::Unsubscribe { .. }))
        .count();
    assert_eq!(subscribes, 5, "one upstream owner per downstream session");
    assert_eq!(
        unsubscribes, 5,
        "each propagated owner is retired exactly once across detach/reconnect",
    );
}

/// Replacing the authority link must replay every still-owned relay coverage
/// group. The downstream browser remains connected throughout, so it has no
/// reason to send another Subscribe after the worker reconnects upstream.
#[test]
fn worker_relay_replays_live_coverage_after_upstream_reconnect() {
    let schema = schema();
    let worker = open_db(0x66, AuthorSubject::SYSTEM, &schema);
    let (first_authority, first_authority_state) = scripted_authority(None);
    let first_upstream = block_on(worker.connect_upstream(first_authority));

    let alice = AuthorSubject::for_test_bytes([0x66; 16]);
    let browser = open_db(0x67, alice, &schema);
    browser.set_non_durable_client();
    let (browser_transport, worker_transport) = duplex();
    let _browser_upstream = block_on(browser.connect_upstream(browser_transport));
    let _worker_downstream = worker.accept_subscriber(worker_transport, alice);
    let todos = browser
        .prepare_query(&browser.table("todos"))
        .expect("prepare browser todos query");
    let _subscription = block_on(browser.subscribe(&todos, ReadOpts::default()))
        .expect("open browser subscription");

    for _ in 0..3 {
        browser.tick().expect("send browser subscription");
        worker.tick().expect("relay browser subscription");
    }
    assert_eq!(
        first_authority_state
            .borrow()
            .outbound
            .iter()
            .filter(|message| matches!(message, SyncMessage::Subscribe(_)))
            .count(),
        1,
        "the original authority receives the live relay coverage",
    );
    assert_eq!(worker.relay_upstream_subscription_owner_count_for_test(), 1);

    assert!(worker.detach_connection(&first_upstream));
    let (second_authority, second_authority_state) = scripted_authority(None);
    let _second_upstream = block_on(worker.connect_upstream(second_authority));
    for _ in 0..3 {
        worker
            .tick()
            .expect("replay relay coverage after reconnect");
    }

    assert_eq!(
        second_authority_state
            .borrow()
            .outbound
            .iter()
            .filter(|message| matches!(message, SyncMessage::Subscribe(_)))
            .count(),
        1,
        "a still-connected browser must retain authority coverage across worker reconnect",
    );
    assert_eq!(
        worker.relay_upstream_subscription_owner_count_for_test(),
        1,
        "reconnect must retain the downstream relay owner",
    );
}

/// A rejected relay-owned upstream usage site can represent multiple active
/// downstream subscription keys in one coverage group. The authority result
/// must reach every key before the relay retires the group and its owner.
#[test]
fn worker_relay_forwards_upstream_subscription_rejection_to_every_group_member() {
    let schema = schema();
    let worker = open_db(0x70, AuthorSubject::SYSTEM, &schema);
    let rejection = SubscribeRejectReason::UnsupportedShapeCapability {
        detail: "scripted authority rejection".to_owned(),
    };
    let (authority, authority_state) = scripted_authority(Some(rejection.clone()));
    let _worker_upstream = block_on(worker.connect_upstream(authority));

    let browser = open_db(0x71, AuthorSubject::for_test_bytes([0x71; 16]), &schema);
    browser.set_non_durable_client();
    let (browser_transport, worker_transport) = duplex();
    let _browser_upstream = block_on(browser.connect_upstream(browser_transport));
    let _worker_downstream =
        worker.accept_subscriber(worker_transport, AuthorSubject::for_test_bytes([0x71; 16]));
    let todos = browser
        .prepare_query(&browser.table("todos"))
        .expect("prepare browser todos query");
    let mut first = block_on(browser.subscribe(&todos, ReadOpts::default()))
        .expect("open first browser subscription");
    let mut second = block_on(browser.subscribe(&todos, ReadOpts::default()))
        .expect("open second browser subscription");
    assert_truthful_empty_local_opening(first.try_next_event());
    assert_truthful_empty_local_opening(second.try_next_event());

    for _ in 0..5 {
        browser.tick().expect("send grouped browser subscriptions");
        worker
            .tick()
            .expect("receive authority rejection and forward it");
    }

    for events in [&mut first, &mut second] {
        let events = std::iter::from_fn(|| events.try_next_event()).collect::<Vec<_>>();
        assert!(
            events.iter().any(|event| matches!(
                event,
                SubscriptionEvent::Rejected { reason } if reason == &rejection
            )),
            "every active downstream key must receive the authority rejection: {events:?}",
        );
    }
    assert_eq!(worker.relay_upstream_subscription_owner_count_for_test(), 0);
    assert_eq!(
        worker.relay_registered_query_binding_count_for_test(),
        0,
        "rejection must unregister each distinct wire usage site",
    );
    let messages = &authority_state.borrow().outbound;
    assert_eq!(
        messages
            .iter()
            .filter(|message| matches!(message, SyncMessage::Subscribe(_)))
            .count(),
        1,
        "both browser keys share one relay coverage-group owner",
    );
    assert_eq!(
        messages
            .iter()
            .filter(|message| matches!(message, SyncMessage::Unsubscribe { .. }))
            .count(),
        1,
        "the rejected relay owner is retired exactly once",
    );
}

/// One downstream wire connection can attach two independent usage-site keys
/// to the same canonical coverage. When authority rejects the one relayed
/// coverage request, the worker must address both original keys before
/// retiring the shared group.
///
/// alice wire client ──Subscribe(A), Subscribe(B)──► worker relay ──Subscribe──► authority
/// alice wire client ◄─Rejected(A), Rejected(B)──── worker relay ◄─Rejected─── authority
#[test]
fn worker_relay_fans_upstream_subscription_rejection_to_distinct_wire_group_members() {
    let schema = schema();
    let worker = open_db(0x72, AuthorSubject::SYSTEM, &schema);
    let rejection = SubscribeRejectReason::UnsupportedShapeCapability {
        detail: "scripted authority rejection".to_owned(),
    };
    let (authority, authority_state) = scripted_authority(Some(rejection.clone()));
    let _worker_upstream = block_on(worker.connect_upstream(authority));

    let alice = AuthorSubject::for_test_bytes([0x72; 16]);
    let (mut alice_transport, worker_transport) = duplex();
    let _worker_downstream = worker.accept_subscriber(worker_transport, alice);
    let prepared = worker
        .prepare_query(&worker.table("todos"))
        .expect("prepare worker todos shape");
    let shape = prepared.shape();
    let opts = RegisterShapeOptions::default();
    let first = SubscriptionKey {
        shape_id: shape.shape_id(),
        binding_id: BindingId(uuid::Uuid::from_bytes([0x72; 16])),
        read_view: opts.read_view_key(),
    };
    let second = SubscriptionKey {
        shape_id: shape.shape_id(),
        binding_id: BindingId(uuid::Uuid::from_bytes([0x73; 16])),
        read_view: opts.read_view_key(),
    };

    alice_transport
        .send(SyncMessage::RegisterShape {
            shape_id: shape.shape_id(),
            ast: ShapeAst::from_validated(shape),
            opts: opts.clone(),
        })
        .expect("register relay shape");
    worker.tick().expect("register shape at worker");
    for subscription in [first, second] {
        alice_transport
            .send(SyncMessage::Subscribe(Subscribe {
                shape_id: shape.shape_id(),
                subscription,
                values: Vec::new(),
                known_state: None,
                delegated_session: None,
            }))
            .expect("subscribe with distinct wire key");
    }

    for _ in 0..3 {
        worker.tick().expect("relay authority rejection");
    }

    let rejected = std::iter::from_fn(|| alice_transport.try_recv())
        .filter_map(|message| match message {
            SyncMessage::SubscribeRejected {
                subscription,
                reason,
            } if reason == rejection => Some(subscription),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        rejected,
        vec![first, second],
        "the authority rejection must fan out to both distinct wire usage sites",
    );
    assert_eq!(worker.relay_upstream_subscription_owner_count_for_test(), 0);
    assert_eq!(
        worker.relay_registered_query_binding_count_for_test(),
        0,
        "rejection must unregister each distinct wire usage site",
    );
    {
        let authority_state = authority_state.borrow();
        let messages = &authority_state.outbound;
        assert_eq!(
            messages
                .iter()
                .filter(|message| matches!(message, SyncMessage::Subscribe(_)))
                .count(),
            1,
            "identical wire members share one relayed coverage request",
        );
        assert_eq!(
            messages
                .iter()
                .filter(|message| matches!(message, SyncMessage::Unsubscribe { .. }))
                .count(),
            1,
            "the rejected shared owner is retired exactly once",
        );
    }

    // Keep one relay connection open while authority rejects fresh wire keys.
    // Every failure must leave it reusable; otherwise a hostile or buggy peer
    // can retain unbounded registration and known-state entries without ever
    // disconnecting.
    for byte in 0x74..0x7c {
        let subscription = SubscriptionKey {
            shape_id: shape.shape_id(),
            binding_id: BindingId(uuid::Uuid::from_bytes([byte; 16])),
            read_view: opts.read_view_key(),
        };
        alice_transport
            .send(SyncMessage::Subscribe(Subscribe {
                shape_id: shape.shape_id(),
                subscription,
                values: Vec::new(),
                known_state: None,
                delegated_session: None,
            }))
            .expect("subscribe with a fresh rejected wire key");
        for _ in 0..3 {
            worker.tick().expect("reject fresh wire key");
        }
        assert_eq!(
            worker.relay_registered_query_binding_count_for_test(),
            0,
            "a rejected wire key must not accumulate state while the relay remains connected",
        );
        assert!(
            std::iter::from_fn(|| alice_transport.try_recv()).any(|message| matches!(
                message,
                SyncMessage::SubscribeRejected {
                    subscription: rejected,
                    reason: ref rejected_reason,
                } if rejected == subscription && rejected_reason == &rejection
            )),
            "authority rejection must reach each fresh wire key",
        );
    }

    // Reusing an earlier rejected key must perform a fresh lifecycle rather
    // than observe a retained registration from its previous attempt.
    alice_transport
        .send(SyncMessage::Subscribe(Subscribe {
            shape_id: shape.shape_id(),
            subscription: first,
            values: Vec::new(),
            known_state: None,
            delegated_session: None,
        }))
        .expect("resubscribe a previously rejected wire key");
    for _ in 0..3 {
        worker.tick().expect("reject reused wire key");
    }
    assert_eq!(worker.relay_registered_query_binding_count_for_test(), 0);
    assert!(
        std::iter::from_fn(|| alice_transport.try_recv()).any(|message| matches!(
            message,
            SyncMessage::SubscribeRejected {
                subscription,
                reason: ref rejected_reason,
            } if subscription == first && rejected_reason == &rejection
        )),
        "a reused wire key must receive a fresh rejection rather than stale relay state",
    );
}

/// A freshly reopened persistent worker must hydrate a downstream Local
/// subscription without requiring an unrelated one-shot query to warm its
/// resident view first.
#[test]
fn reopened_browser_worker_hydrates_local_subscription_without_query_warmup() {
    let schema = schema();
    let alice = AuthorSubject::for_test_bytes([0xaa; 16]);
    let storage = tempfile::tempdir().expect("worker temp dir");

    let first_worker = open_persistent_worker(storage.path(), 0x2b, &schema);
    first_worker
        .insert(
            "todos",
            BTreeMap::from([(
                "title".to_owned(),
                Value::String("persisted before worker restart".to_owned()),
            )]),
            Default::default(),
        )
        .expect("seed worker-local todo");
    first_worker.tick().expect("persist worker-local todo");
    drop(first_worker);

    let worker = open_persistent_worker(storage.path(), 0x2b, &schema);
    let main_thread = open_db(0x1c, alice, &schema);
    main_thread.set_non_durable_client();
    let (main_transport, worker_transport) = duplex();
    let _main_connection = block_on(main_thread.connect_upstream(main_transport));
    let _worker_connection = worker.accept_subscriber(worker_transport, alice);

    let todos = main_thread
        .prepare_query(&main_thread.table("todos"))
        .expect("prepare todos query");
    let mut subscription =
        block_on(main_thread.subscribe(&todos, ReadOpts::default())).expect("subscribe to todos");
    assert_truthful_empty_local_opening(subscription.try_next_event());

    main_thread.tick().expect("send cold subscription request");
    let scheduler = Rc::new(CountingScheduler::default());
    worker.set_tick_scheduler(Some(scheduler.clone()));
    worker.tick().expect("admit cold subscription request");
    assert!(
        scheduler.calls.get() > 0,
        "cold subscription admission must schedule its deferred hydration turn"
    );

    for _ in 0..8 {
        main_thread.tick().expect("drive subscription request");
        worker.tick().expect("drive cold worker hydration");
        main_thread.tick().expect("apply worker subscription view");
    }

    let events = std::iter::from_fn(|| subscription.try_next_event()).collect::<Vec<_>>();
    assert!(
        events.iter().any(|event| matches!(
            event,
            SubscriptionEvent::Delta { added, .. } if added.len() == 1
        )),
        "reopened worker never hydrated the persisted row: {events:?}"
    );
}

#[test]
fn worker_baseline_arriving_during_cold_main_hydration_is_delivered_exactly_once() {
    let schema = schema();
    let alice = AuthorSubject::for_test_bytes([0xab; 16]);
    let durable = tempfile::tempdir().expect("worker temp dir");
    let first_worker = open_persistent_worker(durable.path(), 0x2c, &schema);
    for title in ["third", "first", "second"] {
        first_worker
            .insert(
                "todos",
                BTreeMap::from([("title".to_owned(), Value::String(title.to_owned()))]),
                Default::default(),
            )
            .expect("seed worker-local todo");
    }
    first_worker.tick().expect("persist worker baseline");
    drop(first_worker);
    let worker = open_persistent_worker(durable.path(), 0x2c, &schema);

    let column_families = schema.column_families();
    let refs = column_families
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    let (main_storage, control) = TestStorage::controlled(&refs);
    let main_thread = open_db_with_storage(0x1d, alice, &schema, main_storage);
    main_thread.set_non_durable_client();
    let (main_transport, worker_transport) = duplex();
    let _main_connection = block_on(main_thread.connect_upstream(main_transport));
    let _worker_connection = worker.accept_subscriber(worker_transport, alice);

    control.pause_on(TestStorageOperation::ScanOpen);
    let todos = main_thread
        .prepare_query(
            &main_thread
                .table("todos")
                .order_by("title", OrderDirection::Asc),
        )
        .expect("prepare todos query");
    let mut subscription =
        block_on(main_thread.subscribe(&todos, ReadOpts::default())).expect("subscribe to todos");
    assert_truthful_empty_local_opening(subscription.try_next_event());

    let scheduler = Rc::new(CountingScheduler::default());
    worker.set_tick_scheduler(Some(scheduler.clone()));
    main_thread.tick().expect("request worker baseline");
    scheduler.clear();
    worker.tick().expect("send worker baseline");
    assert_scheduled_urgencies(
        &scheduler,
        &[
            TickUrgency::AfterCurrentTurn,
            TickUrgency::AfterCurrentTurn,
            TickUrgency::AfterCurrentTurn,
        ],
        "worker baseline subscription opening",
    );
    worker
        .tick()
        .expect("send worker baseline on the scheduled follow-up turn");
    main_thread
        .tick()
        .expect("apply worker baseline while local hydration is suspended");
    control.resume_operation(TestStorageOperation::ScanOpen);
    for _ in 0..4 {
        main_thread.tick().expect("finish main hydration");
    }

    let events = std::iter::from_fn(|| subscription.try_next_event()).collect::<Vec<_>>();
    let added = events
        .iter()
        .map(|event| match event {
            SubscriptionEvent::Delta { added, .. } => added.len(),
            SubscriptionEvent::Rejected { .. } | SubscriptionEvent::Closed => 0,
        })
        .sum::<usize>();
    assert_eq!(added, 3, "worker baseline cardinality drifted: {events:?}");
    assert_eq!(
        main_thread.read(&todos).expect("read hydrated row").len(),
        3
    );
}

/// Local and propagation are independent axes at a browser relay. A Local
/// foreground read returns the worker's resident knowledge immediately, while
/// the default Full propagation still registers the exact upstream usage and
/// later reconciles the authority membership into that same subscription.
#[test]
fn browser_client_local_full_returns_immediately_then_reconciles_upstream() {
    let schema = schema();
    let alice = AuthorSubject::for_test_bytes([0xa5; 16]);
    let worker = open_db(0x25, alice, &schema);
    let core = open_core(0x35, &schema);
    let server_writer = open_db(0x45, alice, &schema);
    worker.set_relay_authority_session_owner_for_test();
    worker
        .insert(
            "todos",
            BTreeMap::from([("title".to_owned(), Value::String("worker-local".to_owned()))]),
            Default::default(),
        )
        .expect("seed worker-local todo");
    let (writer_transport, core_writer_transport) = duplex();
    let _writer_upstream = block_on(server_writer.connect_upstream(writer_transport));
    let _core_writer = core.accept_subscriber(core_writer_transport, alice);
    server_writer
        .insert(
            "todos",
            BTreeMap::from([("title".to_owned(), Value::String("server-only".to_owned()))]),
            Default::default(),
        )
        .expect("seed server-only todo");

    let main_thread = open_db(0x15, alice, &schema);
    main_thread.set_non_durable_client();
    let (main_transport, worker_subscriber_transport) = duplex();
    let _main_connection = block_on(main_thread.connect_upstream(main_transport));
    let _worker_subscriber = worker.accept_subscriber(worker_subscriber_transport, alice);
    let (_worker_upstream, _core_subscriber) =
        connect_scope_isolated_worker_to_core!(worker, core, alice);
    for _ in 0..8 {
        server_writer.tick().expect("upload server-only seed");
        worker.tick().expect("upload worker-local seed");
        core.tick().expect("accept worker-local seed");
        server_writer.tick().expect("settle server-only seed");
        worker.tick().expect("settle worker-local seed");
    }

    let todos = main_thread
        .prepare_query(&main_thread.table("todos"))
        .expect("prepare Local+Full todos query");
    let mut subscription = block_on(main_thread.subscribe(
        &todos,
        ReadOpts {
            tier: DurabilityTier::Local,
            ..ReadOpts::default()
        },
    ))
    .expect("subscribe Local+Full through worker");
    assert_truthful_empty_local_opening(subscription.try_next_event());

    main_thread.tick().expect("register Local+Full coverage");
    worker
        .tick()
        .expect("serve local knowledge and queue upstream Subscribe");
    main_thread.tick().expect("apply immediate Local result");
    assert_eq!(
        main_thread
            .read(&todos)
            .expect("read immediate Local view")
            .len(),
        1,
        "Local must not wait for the queued upstream authority result",
    );

    for _ in 0..8 {
        core.tick().expect("serve propagated Local usage");
        worker.tick().expect("reconcile exact upstream membership");
        main_thread.tick().expect("apply reconciled Local result");
    }
    assert_eq!(
        main_thread
            .read(&todos)
            .expect("read reconciled Local view")
            .len(),
        2,
        "Full propagation must eventually add the server-only row",
    );
    let events = std::iter::from_fn(|| subscription.try_next_event()).collect::<Vec<_>>();
    assert!(events.iter().any(|event| matches!(
        event,
        SubscriptionEvent::Delta { added, .. } if added.len() == 1
    )));
}

/// A browser local-only subscription crosses the private main/worker boundary
/// so the fresh in-memory main Db can hydrate from durable worker state, but it
/// must not cross the worker/server boundary.
#[test]
fn browser_client_local_only_subscription_stops_at_worker() {
    let schema = schema();
    let alice = AuthorSubject::for_test_bytes([0xa9; 16]);
    let worker = open_db(0x2a, alice, &schema);
    let core = open_core(0x3a, &schema);
    worker.set_relay_authority_session_owner_for_test();
    worker
        .insert(
            "todos",
            BTreeMap::from([("title".to_owned(), Value::String("worker-local".to_owned()))]),
            Default::default(),
        )
        .expect("seed worker-local todo");
    core.insert(
        "todos",
        BTreeMap::from([("title".to_owned(), Value::String("server-only".to_owned()))]),
        Default::default(),
    )
    .expect("seed server-only todo");

    let main_thread = open_db(0x1b, alice, &schema);
    main_thread.set_non_durable_client();
    let (main_transport, worker_subscriber_transport) = duplex();
    let _main_connection = jazz::db::block_on(main_thread.connect_upstream(main_transport));
    let _worker_subscriber = worker.accept_subscriber(worker_subscriber_transport, alice);
    let (_worker_upstream, _core_subscriber) =
        connect_scope_isolated_worker_to_core!(worker, core, alice);

    let todos = main_thread
        .prepare_query(&main_thread.table("todos"))
        .expect("prepare local-only todos query");
    let mut subscription = block_on(main_thread.subscribe(
        &todos,
        ReadOpts {
            tier: DurabilityTier::Local,
            propagation: Propagation::LocalOnly,
            ..ReadOpts::default()
        },
    ))
    .expect("subscribe locally through the worker");
    assert_truthful_empty_local_opening(subscription.try_next_event());

    main_thread.tick().expect("register worker-local coverage");
    for _ in 0..4 {
        worker.tick().expect("serve worker-local coverage");
        core.tick().expect("process any server traffic");
        worker.tick().expect("process any server response");
        main_thread.tick().expect("apply worker-local coverage");
    }

    let rows = main_thread
        .read(&todos)
        .expect("read worker-hydrated local-only view");
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].cell_at(0),
        Some(Value::String("worker-local".to_owned()))
    );
    let events = std::iter::from_fn(|| subscription.try_next_event()).collect::<Vec<_>>();
    assert!(events.iter().any(|event| matches!(
        event,
        SubscriptionEvent::Delta { added, .. } if added.len() == 1
    )));
}

/// An authority-tier browser subscription must not treat the worker's current
/// empty cache as a settled result. The relay first registers the same coverage
/// upstream, then publishes the authority's settled snapshot downstream.
#[test]
fn browser_relay_does_not_publish_a_premature_settled_snapshot() {
    let schema = schema();
    let alice = AuthorSubject::for_test_bytes([0xa6; 16]);
    let main_thread = open_db(0x17, alice, &schema);
    let worker = open_db(0x27, alice, &schema);
    let core = open_core(0x37, &schema);
    main_thread.set_non_durable_client();
    worker.set_relay_authority_session_owner_for_test();

    let seeder = open_db(0x18, alice, &schema);
    let (seeder_transport, core_seed_transport) = duplex();
    let _seeder_connection = jazz::db::block_on(seeder.connect_upstream(seeder_transport));
    let _core_seed_subscriber = core.accept_subscriber(core_seed_transport, alice);
    let seeded = seeder
        .insert(
            "todos",
            BTreeMap::from([(
                "title".to_owned(),
                Value::String("already settled at the authority".to_owned()),
            )]),
            Default::default(),
        )
        .expect("seed authority todo");
    let seeded_tx = seeded.mergeable_tx_id();
    seeder.tick().expect("upload seeded row");
    core.tick().expect("accept seeded row");
    seeder.tick().expect("apply seeded-row fate");
    assert!(matches!(
        seeder.write_state(seeded_tx).expect("seeded write state"),
        jazz::db::WriteState {
            fate: Fate::Accepted,
            durability: DurabilityTier::Global,
            global_time: Some(_),
        }
    ));

    // Match browser-worker initialization order: accept the main-thread relay
    // first, then attach the worker's server transport.
    let (main_transport, worker_subscriber_transport) = duplex();
    let _main_connection = jazz::db::block_on(main_thread.connect_upstream(main_transport));
    let _worker_subscriber = worker.accept_subscriber(worker_subscriber_transport, alice);
    let (_worker_upstream, _core_subscriber) =
        connect_scope_isolated_worker_to_core!(worker, core, alice);

    let todos = main_thread
        .prepare_query(&main_thread.table("todos"))
        .expect("prepare global todos query");
    let mut subscription = block_on(main_thread.subscribe(
        &todos,
        ReadOpts {
            tier: DurabilityTier::Global,
            ..ReadOpts::default()
        },
    ))
    .expect("subscribe globally through worker relay");
    assert!(
        subscription.try_next_event().is_none(),
        "fresh remote coverage must withhold its provisional local snapshot"
    );

    main_thread.tick().expect("register global worker view");
    worker
        .tick()
        .expect("forward coverage without claiming settlement");
    main_thread
        .tick()
        .expect("process any pre-authority worker messages");
    let premature = std::iter::from_fn(|| subscription.try_next_event()).collect::<Vec<_>>();
    assert!(
        !premature
            .iter()
            .any(|event| matches!(event, SubscriptionEvent::Delta { settled: true, .. })),
        "worker must not publish its empty cache as authority-settled"
    );

    for _ in 0..4 {
        core.tick().expect("serve authority snapshot");
        worker
            .tick()
            .expect("apply authority snapshot and serve main");
        main_thread
            .tick()
            .expect("apply relayed authority snapshot");
    }

    let settled = std::iter::from_fn(|| subscription.try_next_event()).collect::<Vec<_>>();
    assert!(
        settled.iter().any(|event| matches!(
            event,
            SubscriptionEvent::Delta {
                added,
                settled: true,
                ..
            } if added.len() == 1
        )),
        "expected settled authority row, got {settled:?}"
    );
}

#[test]
/// Alice seeds one exclusive transaction containing sibling rows; her browser
/// main thread asks its durable worker relay for each sibling as an Edge read.
///
/// ```text
/// alice ──exclusive org/todo/check/note──► core
/// browser main ──Edge sibling query──► worker ──► core
/// ```
///
/// The core must accept and persist the whole exclusive bundle before the
/// relay extends its projection for each sibling. Besides the view-scoped
/// cardinality contract, this keeps the deep authoritative-ingest path on a
/// normal host thread rather than relying on an enlarged test stack. The
/// ordinary `Db::tick` boundary must therefore remain stack-safe when this
/// receipt runs on libtest's default 2 MiB worker stack.
fn view_scoped_exclusive_sibling_edge_reads_extend_relay_projection() {
    let schema = compile_schema(
        &SchemaBuilder::new()
            .table(TableSchemaBuilder::new("orgs").column("name", ColumnType::Text))
            .table(TableSchemaBuilder::new("todos").fk_column("org_id", "orgs"))
            .table(
                TableSchemaBuilder::new("checks")
                    .fk_column("org_id", "orgs")
                    .fk_column("todo_id", "todos"),
            )
            .table(
                TableSchemaBuilder::new("notes")
                    .fk_column("org_id", "orgs")
                    .fk_column("check_id", "checks"),
            )
            .build(),
    );
    let alice = AuthorSubject::for_test_bytes([0xd1; 16]);
    let main_thread = open_db(0x41, alice, &schema);
    let worker = open_db(0x42, alice, &schema);
    let core = open_core(0x43, &schema);
    let seeder = open_db(0x44, alice, &schema);
    main_thread.set_non_durable_client();
    worker.set_relay_authority_session_owner_for_test();

    let (main_transport, worker_subscriber_transport) = duplex();
    let _main_connection = block_on(main_thread.connect_upstream(main_transport));
    let _worker_subscriber = worker.accept_subscriber(worker_subscriber_transport, alice);
    let (_worker_upstream, _core_subscriber) =
        connect_scope_isolated_worker_to_core!(worker, core, alice);
    let (seed_transport, core_seed_transport) = duplex();
    let _seed_upstream = block_on(seeder.connect_upstream(seed_transport));
    let _core_seed = core.accept_subscriber(core_seed_transport, alice);

    let tx = seeder
        .exclusive_tx()
        .expect("open exclusive seed transaction");
    let org = tx
        .insert(
            "orgs",
            BTreeMap::from([("name".to_owned(), Value::String("north".to_owned()))]),
            Default::default(),
        )
        .expect("seed org");
    let todo = tx
        .insert(
            "todos",
            BTreeMap::from([("org_id".to_owned(), Value::Uuid(org.0))]),
            Default::default(),
        )
        .expect("seed todo");
    let check = tx
        .insert(
            "checks",
            BTreeMap::from([
                ("org_id".to_owned(), Value::Uuid(org.0)),
                ("todo_id".to_owned(), Value::Uuid(todo.0)),
            ]),
            Default::default(),
        )
        .expect("seed check");
    let note = tx
        .insert(
            "notes",
            BTreeMap::from([
                ("org_id".to_owned(), Value::Uuid(org.0)),
                ("check_id".to_owned(), Value::Uuid(check.0)),
            ]),
            Default::default(),
        )
        .expect("seed note");
    tx.commit().expect("commit exclusive sibling rows");
    for _ in 0..4 {
        seeder.tick().expect("upload sibling rows");
        core.tick().expect("accept sibling rows");
        seeder.tick().expect("apply sibling fates");
    }

    let opts = ReadOpts {
        tier: DurabilityTier::Edge,
        ..ReadOpts::default()
    };
    for (table, expected_row) in [("todos", todo), ("checks", check), ("notes", note)] {
        let query = main_thread
            .prepare_query(&Query::from(table).filter(eq(col("org_id"), lit(org.0))))
            .expect("prepare sibling query");
        let attachment = main_thread
            .attach_query_with_opts(&query, opts.clone())
            .expect("attach sibling query");
        for _ in 0..12 {
            main_thread.tick().expect("register sibling query");
            worker.tick().expect("forward sibling query");
            core.tick().expect("serve sibling query");
            worker.tick().expect("relay sibling result");
            main_thread.tick().expect("apply sibling result");
            if main_thread.query_attachment_is_covered(&attachment) {
                break;
            }
        }
        assert!(main_thread.query_attachment_is_covered(&attachment));
        let rows = block_on(main_thread.all(&query, opts.clone())).unwrap();
        assert_eq!(rows.len(), 1, "covered {table} query returned false-empty");
        assert_eq!(rows[0].row_uuid(), expected_row);
        main_thread.detach_query(attachment);
        main_thread.tick().expect("send sibling unsubscribe");
        worker.tick().expect("retire sibling relay coverage");
        core.tick().expect("retire sibling authority coverage");
        worker.tick().expect("apply sibling retirement");
        main_thread
            .tick()
            .expect("apply sibling retirement locally");
    }

    // Control: the same relay lifecycle already works when every row has its
    // own mergeable transaction identity. Keep it beside the exclusive case
    // so the receipt specifically protects fragment extension, rather than a
    // broader change to strict-Edge attachment semantics.
    let merge_org = seeder
        .insert(
            "orgs",
            BTreeMap::from([("name".to_owned(), Value::String("south".to_owned()))]),
            Default::default(),
        )
        .expect("seed mergeable org");
    let merge_org_id = merge_org.row_uuid();
    let merge_todo = seeder
        .insert(
            "todos",
            BTreeMap::from([("org_id".to_owned(), Value::Uuid(merge_org_id.0))]),
            Default::default(),
        )
        .expect("seed mergeable todo");
    let merge_todo_id = merge_todo.row_uuid();
    let merge_check = seeder
        .insert(
            "checks",
            BTreeMap::from([
                ("org_id".to_owned(), Value::Uuid(merge_org_id.0)),
                ("todo_id".to_owned(), Value::Uuid(merge_todo_id.0)),
            ]),
            Default::default(),
        )
        .expect("seed mergeable check");
    let merge_check_id = merge_check.row_uuid();
    let merge_note = seeder
        .insert(
            "notes",
            BTreeMap::from([
                ("org_id".to_owned(), Value::Uuid(merge_org_id.0)),
                ("check_id".to_owned(), Value::Uuid(merge_check_id.0)),
            ]),
            Default::default(),
        )
        .expect("seed mergeable note");
    let merge_note_id = merge_note.row_uuid();
    for _ in 0..8 {
        seeder.tick().expect("upload mergeable sibling rows");
        core.tick().expect("accept mergeable sibling rows");
        seeder.tick().expect("apply mergeable sibling fates");
    }
    for (table, expected_row) in [
        ("todos", merge_todo_id),
        ("checks", merge_check_id),
        ("notes", merge_note_id),
    ] {
        let query = main_thread
            .prepare_query(&Query::from(table).filter(eq(col("org_id"), lit(merge_org_id.0))))
            .expect("prepare mergeable control query");
        let attachment = main_thread
            .attach_query_with_opts(&query, opts.clone())
            .expect("attach mergeable control query");
        for _ in 0..12 {
            main_thread
                .tick()
                .expect("register mergeable control query");
            worker.tick().expect("forward mergeable control query");
            core.tick().expect("serve mergeable control query");
            worker.tick().expect("relay mergeable control result");
            main_thread.tick().expect("apply mergeable control result");
            if main_thread.query_attachment_is_covered(&attachment) {
                break;
            }
        }
        assert!(main_thread.query_attachment_is_covered(&attachment));
        let rows = block_on(main_thread.all(&query, opts.clone())).unwrap();
        assert_eq!(rows.len(), 1, "covered mergeable {table} query was empty");
        assert_eq!(rows[0].row_uuid(), expected_row);
        main_thread.detach_query(attachment);
    }
}

/// A fresh browser main thread receives the worker's authority-owned reset as
/// a complete relation snapshot in the same relay turn that applies it. This
/// is intentionally an internal topology test: only the public `Db` facade
/// can model the non-durable main/runtime worker/core boundary used by the
/// browser bridge.
#[test]
fn browser_relay_hydrates_fresh_included_edge_subscription_from_authority() {
    let schema = included_relation_schema();
    let alice = AuthorSubject::for_test_bytes([0xb2; 16]);
    let main_thread = open_db(0x1f, alice, &schema);
    let worker = open_db(0x2f, alice, &schema);
    let core = open_core(0x3f, &schema);
    main_thread.set_non_durable_client();
    worker.set_relay_authority_session_owner_for_test();
    let scheduler = Rc::new(CountingScheduler::default());
    worker.set_tick_scheduler(Some(scheduler.clone()));

    let seeder = open_db(0x20, alice, &schema);
    let (seeder_transport, core_seed_transport) = duplex();
    let _seeder_connection = jazz::db::block_on(seeder.connect_upstream(seeder_transport));
    let _core_seed_subscriber = core.accept_subscriber(core_seed_transport, alice);
    let profile = seeder
        .insert(
            "profiles",
            BTreeMap::from([("name".to_owned(), Value::String("Alice".to_owned()))]),
            Default::default(),
        )
        .expect("seed included profile");
    let message = seeder
        .insert(
            "messages",
            BTreeMap::from([
                ("author".to_owned(), Value::Uuid(profile.row_uuid().0)),
                (
                    "body".to_owned(),
                    Value::String("already settled".to_owned()),
                ),
                ("created".to_owned(), Value::U64(1)),
            ]),
            Default::default(),
        )
        .expect("seed included message");
    seeder.tick().expect("upload seeded relation");
    core.tick().expect("accept seeded relation");
    seeder.tick().expect("apply seeded relation fate");

    let (main_transport, worker_subscriber_transport) = duplex();
    let _main_connection = jazz::db::block_on(main_thread.connect_upstream(main_transport));
    let _worker_subscriber = worker.accept_subscriber(worker_subscriber_transport, alice);
    let (_worker_upstream, _core_subscriber) =
        connect_scope_isolated_worker_to_core!(worker, core, alice);

    let query = main_thread
        .prepare_query(
            &Query::from("messages")
                .include("author")
                .order_by("created", OrderDirection::Desc)
                .limit(21),
        )
        .expect("prepare included edge query");
    let mut subscription = block_on(main_thread.subscribe(
        &query,
        ReadOpts {
            tier: DurabilityTier::Edge,
            ..ReadOpts::default()
        },
    ))
    .expect("subscribe through worker relay");
    assert!(
        subscription.try_next_event().is_none(),
        "fresh remote coverage must withhold its provisional local snapshot"
    );

    let mut authority_update_scheduled = false;
    for _ in 0..4 {
        main_thread.tick().expect("register worker coverage");
        worker.tick().expect("forward authority coverage");
        core.tick().expect("serve authority relation snapshot");
        let schedules_before = scheduler.calls.get();
        worker.tick().expect("relay authority relation snapshot");
        authority_update_scheduled |= scheduler.calls.get() > schedules_before;
        main_thread
            .tick()
            .expect("apply authority relation snapshot");
    }

    assert!(
        authority_update_scheduled,
        "applying the authority view must schedule the relay post-receive serve pass",
    );
    main_thread
        .tick()
        .expect("apply relayed authority relation snapshot");

    let events = std::iter::from_fn(|| subscription.try_next_event()).collect::<Vec<_>>();
    assert!(
        events.iter().any(|event| matches!(
            event,
            SubscriptionEvent::Delta { reset: true, added, settled: true, .. }
                if added.iter().any(|row| row.row.row_uuid() == message.row_uuid())
        )),
        "fresh included Edge subscription must publish the authority result, got {events:?}"
    );
}

/// A cold browser foreground must finish one complete structured authority
/// reset when the worker already has no local query runtime for the new remote
/// usage site. Alice's main runtime opens a bounded ordered message relation;
/// the worker relays Core's one reset, which contains both the root members and
/// the profile facts required to materialize the nested `sender` records.
///
/// ```text
/// alice main ──remote structured subscribe──► worker ──Global──► core
/// alice main ◄──complete reset (roots + sender facts)── worker ◄── core
/// ```
fn assert_cold_browser_relay_structured_reset_materializes_ordered_sender_facts(
    foreground_tier: DurabilityTier,
) {
    let schema = included_relation_schema();
    let alice = AuthorSubject::for_test_bytes([0xb4; 16]);
    let main_thread = open_db(0x24, alice, &schema);
    let worker = open_db(0x34, alice, &schema);
    let core = open_core(0x44, &schema);
    main_thread.set_non_durable_client();
    worker.set_relay_authority_session_owner_for_test();
    let scheduler = Rc::new(CountingScheduler::default());
    worker.set_tick_scheduler(Some(scheduler.clone()));

    let seeder = open_db(0x54, alice, &schema);
    let (seeder_transport, core_seed_transport) = duplex();
    let _seeder_connection = block_on(seeder.connect_upstream(seeder_transport));
    let _core_seed_subscriber = core.accept_subscriber(core_seed_transport, alice);
    let mut messages = Vec::new();
    let mut sender_names = Vec::new();
    for created in 1..=5 {
        let sender_name = format!("structured sender {created}");
        let profile = seeder
            .insert(
                "profiles",
                BTreeMap::from([("name".to_owned(), Value::String(sender_name.clone()))]),
                Default::default(),
            )
            .expect("seed structured sender profile");
        messages.push(
            seeder
                .insert(
                    "messages",
                    BTreeMap::from([
                        ("author".to_owned(), Value::Uuid(profile.row_uuid().0)),
                        (
                            "body".to_owned(),
                            Value::String(format!("cold structured message {created}")),
                        ),
                        ("created".to_owned(), Value::U64(created)),
                    ]),
                    Default::default(),
                )
                .expect("seed structured message"),
        );
        sender_names.push(sender_name);
    }
    for _ in 0..8 {
        seeder.tick().expect("upload structured fixture");
        core.tick().expect("accept structured fixture");
        seeder.tick().expect("settle structured fixture");
    }

    let (main_transport, worker_subscriber_transport) = duplex();
    let _main_connection = block_on(main_thread.connect_upstream(main_transport));
    let _worker_subscriber = worker.accept_subscriber(worker_subscriber_transport, alice);
    let (_worker_upstream, _core_subscriber) =
        connect_scope_isolated_worker_to_core!(worker, core, alice);

    let query = main_thread
        .prepare_query(
            &Query::from("messages")
                .array_subquery(ArraySubquery::new("sender", "profiles", "id", "author"))
                .order_by("created", OrderDirection::Desc)
                .limit(21),
        )
        .expect("prepare cold structured query");
    let mut subscription = block_on(main_thread.subscribe(
        &query,
        ReadOpts {
            tier: foreground_tier,
            ..ReadOpts::default()
        },
    ))
    .expect("subscribe through cold worker relay");
    assert!(
        subscription.try_next_event().is_none(),
        "the provisional structured opening must wait for the authority reset"
    );

    let mut authority_update_scheduled = false;
    for _ in 0..8 {
        main_thread
            .tick()
            .expect("register structured worker coverage");
        worker
            .tick()
            .expect("forward structured authority coverage");
        core.tick().expect("serve structured authority reset");
        let schedules_before = scheduler.calls.get();
        worker.tick().expect("relay structured authority reset");
        authority_update_scheduled |= scheduler.calls.get() > schedules_before;
        main_thread
            .tick()
            .expect("apply complete structured authority reset");
    }

    assert!(
        authority_update_scheduled,
        "the worker must schedule its post-reset remote serve pass"
    );
    let events = std::iter::from_fn(|| subscription.try_next_event()).collect::<Vec<_>>();
    let resets = events
        .iter()
        .filter(|event| matches!(event, SubscriptionEvent::Delta { reset: true, .. }))
        .collect::<Vec<_>>();
    assert_eq!(
        resets.len(),
        1,
        "the bounded owner turns must publish exactly one reset, got {events:?}"
    );
    let reset = resets[0];
    let SubscriptionEvent::Delta {
        settled,
        added,
        updated,
        removed,
        ..
    } = reset
    else {
        unreachable!("the reset was matched above")
    };
    assert!(*settled, "the single reset must settle the Edge receipt");
    assert!(updated.is_empty(), "a reset cannot carry root updates");
    assert!(
        removed.is_empty(),
        "a fresh reset cannot remove prior roots"
    );
    assert_eq!(added.len(), messages.len());
    assert_eq!(
        added
            .iter()
            .map(|row| row.row.row_uuid())
            .collect::<Vec<_>>(),
        messages
            .iter()
            .rev()
            .map(|message| message.row_uuid())
            .collect::<Vec<_>>(),
        "the complete reset must preserve the authority's descending root order"
    );
    for (root, expected_sender) in added.iter().zip(sender_names.iter().rev()) {
        let (descriptor, raw) = root.row.encoded_record();
        let record = BorrowedRecord::new(raw, descriptor);
        let Value::Array(sender) = record.get("sender").expect("nested sender field") else {
            panic!("the reset must materialize sender as a nested array")
        };
        assert_eq!(
            sender.len(),
            1,
            "root {:?} must retain exactly one sender fact",
            root.row.row_uuid()
        );
        let Value::Record(sender) = &sender[0] else {
            panic!("the nested sender must be a record")
        };
        assert!(matches!(
            sender.get("name"),
            Ok(Value::String(name)) if name == *expected_sender
        ));
    }
}

#[test]
fn cold_browser_relay_structured_reset_materializes_ordered_sender_facts() {
    assert_cold_browser_relay_structured_reset_materializes_ordered_sender_facts(
        DurabilityTier::Edge,
    );
}

#[test]
fn cold_browser_relay_global_structured_reset_materializes_ordered_sender_facts() {
    assert_cold_browser_relay_structured_reset_materializes_ordered_sender_facts(
        DurabilityTier::Global,
    );
}

/// A reopened browser tab receives a new Edge receipt after the persistent
/// worker has already applied the authority membership for an earlier tab.
/// Alice closes the first main-thread runtime; the worker remains connected
/// and retains its resident authority row; then Alice opens a fresh runtime.
///
/// ```text
/// alice tab 1 ──Edge──► persistent worker ──Global──► core
///      │                         │
///      └──close──────────────────┤ retains applied authority row
/// alice tab 2 ──Edge──► same worker ──receipt──► tab 2
/// ```
#[test]
fn reopened_browser_tab_hydrates_from_worker_authority_state() {
    let schema = schema();
    let alice = AuthorSubject::for_test_bytes([0xb3; 16]);
    let worker = open_db(0x2b, alice, &schema);
    let core = open_core(0x3b, &schema);
    worker.set_relay_authority_session_owner_for_test();

    let seeder = open_db(0x4b, alice, &schema);
    let (seeder_transport, core_seed_transport) = duplex();
    let _seeder_connection = block_on(seeder.connect_upstream(seeder_transport));
    let _core_seed_subscriber = core.accept_subscriber(core_seed_transport, alice);
    let seeded = seeder
        .insert(
            "todos",
            BTreeMap::from([(
                "title".to_owned(),
                Value::String("survives tab reopen".to_owned()),
            )]),
            Default::default(),
        )
        .expect("seed authority row");
    for _ in 0..3 {
        seeder.tick().expect("upload seeded row");
        core.tick().expect("accept seeded row");
        seeder.tick().expect("settle seeded row");
    }

    let first_tab = open_db(0x1b, alice, &schema);
    first_tab.set_non_durable_client();
    let (first_transport, first_worker_transport) = duplex();
    let first_connection = block_on(first_tab.connect_upstream(first_transport));
    let first_worker_connection = worker.accept_subscriber(first_worker_transport, alice);
    let (_worker_upstream, _core_subscriber) =
        connect_scope_isolated_worker_to_core!(worker, core, alice);
    let first_query = first_tab
        .prepare_query(&first_tab.table("todos"))
        .expect("prepare first-tab Edge query");
    let mut first_subscription = block_on(first_tab.subscribe(
        &first_query,
        ReadOpts {
            tier: DurabilityTier::Edge,
            ..ReadOpts::default()
        },
    ))
    .expect("subscribe from first tab");
    for _ in 0..6 {
        first_tab.tick().expect("register first-tab coverage");
        worker.tick().expect("forward first-tab coverage");
        core.tick().expect("serve first-tab authority coverage");
        worker.tick().expect("apply first-tab authority coverage");
        first_tab.tick().expect("apply first-tab handoff");
    }
    assert!(
        std::iter::from_fn(|| first_subscription.try_next_event()).any(|event| matches!(
            event,
            SubscriptionEvent::Delta { added, settled: true, .. }
                if added.iter().any(|row| row.row.row_uuid() == seeded.row_uuid())
        ))
    );

    // Closing the tab removes only its connection-scoped relay ownership. The
    // worker has already applied the authority row and must remain usable by a
    // subsequent tab without a process restart.
    drop(first_subscription);
    first_tab.tick().expect("finalize first-tab subscription");
    worker.tick().expect("retire first-tab relay owner");
    core.tick().expect("process first-tab relay retirement");
    worker.tick().expect("apply first-tab relay retirement");
    assert!(first_tab.detach_connection(&first_connection));
    assert!(worker.detach_connection(&first_worker_connection));
    let worker_query = worker
        .prepare_query(&worker.table("todos"))
        .expect("prepare worker resident query");
    assert_eq!(
        worker
            .read(&worker_query)
            .expect("read retained worker row")
            .len(),
        1,
        "the persistent worker must retain the authority row after tab 1 closes",
    );

    let reopened_tab = open_db(0x1c, alice, &schema);
    reopened_tab.set_non_durable_client();
    let (reopened_transport, reopened_worker_transport) = duplex();
    let _reopened_connection = block_on(reopened_tab.connect_upstream(reopened_transport));
    let _reopened_worker_connection = worker.accept_subscriber(reopened_worker_transport, alice);
    let reopened_query = reopened_tab
        .prepare_query(&reopened_tab.table("todos"))
        .expect("prepare reopened-tab Edge query");
    let mut reopened_subscription = block_on(reopened_tab.subscribe(
        &reopened_query,
        ReadOpts {
            tier: DurabilityTier::Edge,
            ..ReadOpts::default()
        },
    ))
    .expect("subscribe from reopened tab");
    for _ in 0..8 {
        reopened_tab.tick().expect("register reopened-tab coverage");
        worker.tick().expect("forward reopened-tab coverage");
        core.tick().expect("serve reopened-tab authority coverage");
        worker
            .tick()
            .expect("apply reopened-tab authority coverage");
        reopened_tab.tick().expect("apply reopened-tab handoff");
    }
    assert!(
        std::iter::from_fn(|| reopened_subscription.try_next_event()).any(|event| matches!(
            event,
            SubscriptionEvent::Delta { added, settled: true, .. }
                if added.iter().any(|row| row.row.row_uuid() == seeded.row_uuid())
        )),
        "the reopened tab must receive a settled authority handoff"
    );
}

/// A durable worker may retain a formerly visible offset-window row across a
/// process restart. A fresh Edge one-shot must not settle from that recovered
/// authority membership after Core revokes the row; it needs a fresh empty
/// authority view. The fixed turns below model main -> worker -> core ->
/// worker -> main after all first-worker handles are dropped.
#[test]
fn reopened_persistent_worker_stale_membership_does_not_settle_fresh_edge_one_shot() {
    let schema = schema();
    let alice = AuthorSubject::for_test_bytes([0xb5; 16]);
    let storage = tempfile::tempdir().expect("persistent worker temp dir");
    let core = open_core(0x3d, &schema);
    let seeder = open_db(0x4d, alice, &schema);
    let (seeder_transport, core_seed_transport) = duplex();
    let seeder_connection = block_on(seeder.connect_upstream(seeder_transport));
    let core_seed_subscriber = core.accept_subscriber(core_seed_transport, alice);
    seeder
        .insert(
            "todos",
            BTreeMap::from([(
                "title".to_owned(),
                Value::String("anchor retained while worker is closed".to_owned()),
            )]),
            Default::default(),
        )
        .expect("seed offset anchor row");
    let seeded = seeder
        .insert(
            "todos",
            BTreeMap::from([(
                "title".to_owned(),
                Value::String("revoked while worker is closed".to_owned()),
            )]),
            Default::default(),
        )
        .expect("seed authority-visible row");
    for _ in 0..3 {
        seeder.tick().expect("upload seeded authority row");
        core.tick().expect("accept seeded authority row");
        seeder.tick().expect("settle seeded authority row");
    }

    let worker = open_persistent_worker(storage.path(), 0x2d, &schema);
    worker.set_relay_authority_session_owner_for_test();
    let first_tab = open_db(0x1f, alice, &schema);
    first_tab.set_non_durable_client();
    let (first_transport, first_worker_transport) = duplex();
    let first_connection = block_on(first_tab.connect_upstream(first_transport));
    let first_worker_connection = worker.accept_subscriber(first_worker_transport, alice);
    let (worker_upstream, core_worker_subscriber) =
        connect_scope_isolated_worker_to_core!(worker, core, alice);
    let exact_query = Query::from("todos")
        .order_by("title", OrderDirection::Asc)
        .offset(1);
    let first_query = first_tab
        .prepare_query(&exact_query)
        .expect("prepare initial Edge query");
    let first_attachment = first_tab
        .attach_query_with_opts(
            &first_query,
            ReadOpts {
                tier: DurabilityTier::Edge,
                ..ReadOpts::default()
            },
        )
        .expect("attach initial Edge usage site");
    for _ in 0..8 {
        first_tab.tick().expect("register initial Edge usage site");
        worker.tick().expect("forward initial Edge usage site");
        core.tick().expect("serve initial authority membership");
        worker.tick().expect("persist initial authority membership");
        first_tab.tick().expect("apply initial Edge receipt");
        if first_tab.query_attachment_is_covered(&first_attachment) {
            break;
        }
    }
    assert!(first_tab.query_attachment_is_covered(&first_attachment));
    assert_eq!(
        block_on(first_tab.all(
            &first_query,
            ReadOpts {
                tier: DurabilityTier::Edge,
                ..ReadOpts::default()
            },
        ))
        .expect("read initial authority membership")
        .len(),
        1,
    );

    first_tab.detach_query(first_attachment);
    for _ in 0..4 {
        first_tab.tick().expect("retire initial usage site");
        worker.tick().expect("retire initial relay coverage");
        core.tick().expect("process initial relay retirement");
        worker.tick().expect("finish initial relay retirement");
    }
    assert!(first_tab.detach_connection(&first_connection));
    assert!(worker.detach_connection(&first_worker_connection));
    assert!(worker.detach_connection(&worker_upstream));
    assert!(core.detach_connection(&core_worker_subscriber));
    drop(first_connection);
    drop(first_worker_connection);
    drop(worker_upstream);
    drop(core_worker_subscriber);
    drop(first_tab);
    drop(worker);

    block_on(seeder.delete("todos", seeded.row_uuid(), Default::default()))
        .expect("revoke authority-visible row at Core");
    for _ in 0..3 {
        seeder.tick().expect("upload authority revocation");
        core.tick().expect("apply authority revocation");
        seeder.tick().expect("settle authority revocation");
    }
    assert!(
        core.read(
            &core
                .prepare_query(&exact_query)
                .expect("prepare revoked Core Edge query")
        )
        .expect("read revoked Core Edge membership")
        .is_empty(),
        "the selected Core Edge membership must be empty before the worker reopens",
    );

    let reopened_worker = open_persistent_worker(storage.path(), 0x2d, &schema);
    reopened_worker.set_relay_authority_session_owner_for_test();
    let scheduler = Rc::new(CountingScheduler::default());
    reopened_worker.set_tick_scheduler(Some(scheduler.clone()));
    let reopened_tab = open_db(0x20, alice, &schema);
    reopened_tab.set_non_durable_client();
    let (reopened_transport, reopened_worker_transport) = duplex();
    let _reopened_connection = block_on(reopened_tab.connect_upstream(reopened_transport));
    let _reopened_worker_connection =
        reopened_worker.accept_subscriber(reopened_worker_transport, alice);
    let (_reopened_upstream, _reopened_core_subscriber) =
        connect_scope_isolated_worker_to_core!(reopened_worker, core, alice);
    let reopened_query = reopened_tab
        .prepare_query(&exact_query)
        .expect("prepare reopened Edge query");
    let reopened_attachment = reopened_tab
        .attach_query_with_opts(
            &reopened_query,
            ReadOpts {
                tier: DurabilityTier::Edge,
                ..ReadOpts::default()
            },
        )
        .expect("attach reopened Edge usage site");
    for _ in 0..8 {
        reopened_tab
            .tick()
            .expect("register reopened Edge usage site");
        reopened_worker
            .tick()
            .expect("forward reopened Edge usage site");
        core.tick().expect("serve fresh empty authority reset");
        let schedules_before = scheduler.calls.get();
        reopened_worker
            .tick()
            .expect("apply fresh empty authority reset");
        if reopened_tab.query_attachment_is_covered(&reopened_attachment) {
            assert!(
                scheduler.calls.get() > schedules_before,
                "fresh authority reset must schedule the reopened Edge handoff",
            );
            break;
        }
        reopened_tab
            .tick()
            .expect("apply reopened authority handoff");
        if reopened_tab.query_attachment_is_covered(&reopened_attachment) {
            break;
        }
    }
    assert!(
        reopened_tab.query_attachment_is_covered(&reopened_attachment),
        "the reopened Edge usage site must receive a fresh authority receipt",
    );
    assert!(
        block_on(reopened_tab.all(
            &reopened_query,
            ReadOpts {
                tier: DurabilityTier::Edge,
                ..ReadOpts::default()
            },
        ))
        .expect("read fresh revoked Edge membership")
        .is_empty(),
        "fresh empty authority membership must not expose the recovered row",
    );

    assert!(seeder.detach_connection(&seeder_connection));
    assert!(core.detach_connection(&core_seed_subscriber));
}

/// A write policy changes admission only; it cannot revoke read membership.
/// A browser-authored exact-row transaction therefore uses the worker's one
/// ordinary Edge projection. Treating the write-only table as read-scoped
/// would select a second relay-authority projection and deliver the same
/// transaction through incompatible bundles (`ConflictingCommitUnit`).
#[test]
fn browser_worker_write_only_exact_edge_write_uses_one_ordinary_relay_projection() {
    let schema = write_only_policy_schema();
    let alice = AuthorSubject::for_test_bytes([0xc1; 16]);
    let worker = open_db(0xc3, alice, &schema);
    let core = open_core(0xc4, &schema);
    worker.set_relay_authority_session_owner_for_test();

    let main_thread = open_db(0xc5, alice, &schema);
    main_thread.set_non_durable_client();
    let (main_transport, worker_transport) = duplex();
    let _main_connection = block_on(main_thread.connect_upstream(main_transport));
    let _worker_subscriber = worker.accept_subscriber(worker_transport, alice);
    let (_worker_upstream, _core_subscriber) =
        connect_scope_isolated_worker_to_core!(worker, core, alice);

    let row_id = jazz::ids::RowUuid::from_bytes([0xc6; 16]);
    let exact_query = Query::from("todos").filter(eq(col("id"), lit(Value::Uuid(row_id.0))));
    let todos = main_thread
        .prepare_query(&exact_query)
        .expect("prepare exact Edge query");
    let mut subscription = block_on(main_thread.subscribe(
        &todos,
        ReadOpts {
            tier: DurabilityTier::Edge,
            ..ReadOpts::default()
        },
    ))
    .expect("subscribe exact Edge query");

    for _ in 0..4 {
        main_thread.tick().expect("register exact Edge coverage");
        worker.tick().expect("relay exact Edge coverage");
        core.tick().expect("serve initial exact coverage");
        worker.tick().expect("apply initial exact coverage");
        main_thread.tick().expect("settle initial exact coverage");
    }
    while subscription.try_next_event().is_some() {}

    let authored = main_thread
        .insert(
            "todos",
            BTreeMap::from([(
                "title".to_owned(),
                Value::String("one ordinary projection".to_owned()),
            )]),
            jazz::db::InsertOptions {
                row_id: Some(row_id),
                ..Default::default()
            },
        )
        .expect("author browser row");
    for _ in 0..8 {
        main_thread.tick().expect("upload browser-authored row");
        worker
            .tick()
            .expect("relay authored row without conflicting commit units");
        core.tick().expect("admit authored row");
        worker
            .tick()
            .expect("apply authored settlement without conflicting commit units");
        main_thread.tick().expect("apply authored row settlement");
    }

    let events = std::iter::from_fn(|| subscription.try_next_event()).collect::<Vec<_>>();
    assert!(
        events.iter().any(|event| matches!(
            event,
            SubscriptionEvent::Delta { added, .. }
                if added.iter().any(|row| row.row.row_uuid() == authored.row_uuid())
        )),
        "the public Edge read must receive the authored row once: {events:?}",
    );
    assert!(
        events
            .iter()
            .any(|event| matches!(event, SubscriptionEvent::Delta { settled: true, .. })),
        "the authored row must settle through the ordinary projection: {events:?}",
    );
}

/// A browser worker must rebase a narrower one-shot page against a received
/// authority window instead of applying that page's absolute offset twice.
/// Alice first subscribes to positions 8..24, then asks for positions 8..10;
/// the latter must remain the first two members of the received window.
///
/// ```text
/// core Global page 8..24 ──► worker ──► main Edge page 8..24
/// main later Local page 8..10 ──► materialized page 8..24
/// ```
#[test]
fn browser_relay_keeps_offset_window_membership_when_materializing_locally() {
    // This receipt seeds and propagates several independently maintained
    // browser/worker/core views. Give only its test thread the normal 2 MiB
    // stack plus a 4 MiB margin, rather than changing the process-wide test
    // stack or production runtime configuration.
    std::thread::Builder::new()
        .name("browser-relay-window-receipt".to_owned())
        .stack_size(6 * 1024 * 1024)
        .spawn(browser_relay_keeps_offset_window_membership_on_large_stack)
        .expect("spawn browser relay window receipt")
        .join()
        .expect("browser relay window receipt panicked");
}

fn browser_relay_keeps_offset_window_membership_on_large_stack() {
    let schema = schema();
    let alice = AuthorSubject::for_test_bytes([0xc2; 16]);
    let main_thread = open_db(0x1c, alice, &schema);
    let worker = open_db(0x2c, alice, &schema);
    let core = open_core(0x3c, &schema);
    let writer = open_db(0x4c, alice, &schema);
    main_thread.set_non_durable_client();
    // The production broker marks its persistent worker as the authority
    // session owner, so downstream Edge pages re-publish the received window
    // rather than applying its absolute offset to the worker's local overlay.
    worker.set_relay_authority_session_owner_for_test();

    let (writer_transport, core_writer_transport) = duplex();
    let _writer_connection = block_on(writer.connect_upstream(writer_transport));
    let _core_writer = core.accept_subscriber(core_writer_transport, alice);
    let mut todo_ids = Vec::new();
    for index in 0..24 {
        let todo = writer
            .insert(
                "todos",
                BTreeMap::from([(
                    "title".to_owned(),
                    Value::String(format!("todo-{index:02}")),
                )]),
                Default::default(),
            )
            .expect("seed ordered todo");
        todo_ids.push(todo.row_uuid());
    }
    for _ in 0..32 {
        writer.tick().expect("upload ordered todos");
        core.tick().expect("accept ordered todos");
        writer.tick().expect("settle ordered todos");
    }

    let (main_transport, worker_subscriber_transport) = duplex();
    let _main_connection = block_on(main_thread.connect_upstream(main_transport));
    let _worker_subscriber = worker.accept_subscriber(worker_subscriber_transport, alice);
    let (_worker_upstream, _core_subscriber) =
        connect_scope_isolated_worker_to_core!(worker, core, alice);

    let query = main_thread
        .prepare_query(
            &Query::from("todos")
                .order_by("title", OrderDirection::Asc)
                .offset(8)
                .limit(16),
        )
        .expect("prepare offset window");
    let mut subscription = block_on(main_thread.subscribe(
        &query,
        ReadOpts {
            tier: DurabilityTier::Edge,
            ..ReadOpts::default()
        },
    ))
    .expect("subscribe through browser relay");
    assert!(subscription.try_next_event().is_none());

    for _ in 0..12 {
        main_thread.tick().expect("register browser window");
        worker.tick().expect("forward browser window");
        core.tick().expect("serve browser window");
        worker.tick().expect("relay browser window");
        main_thread.tick().expect("apply browser window");
    }

    let edge_rows = block_on(main_thread.all(
        &query,
        ReadOpts {
            tier: DurabilityTier::Edge,
            ..ReadOpts::default()
        },
    ))
    .expect("read authoritative window");
    let local_rows = block_on(main_thread.all(&query, ReadOpts::default()))
        .expect("read materialized browser window");
    let row_ids = |rows: &[CurrentRow]| rows.iter().map(CurrentRow::row_uuid).collect::<Vec<_>>();
    let expected = todo_ids[8..24].to_vec();
    assert_eq!(
        row_ids(&edge_rows),
        expected,
        "authority keeps the requested window"
    );
    assert_eq!(
        row_ids(&local_rows),
        row_ids(&edge_rows),
        "the browser cache must materialize the authoritative result membership without applying the offset again",
    );

    let contained_local = main_thread
        .prepare_query(
            &Query::from("todos")
                .order_by("title", OrderDirection::Asc)
                .offset(8)
                .limit(2),
        )
        .expect("prepare contained local window");
    assert_eq!(
        row_ids(
            &block_on(main_thread.all(
                &contained_local,
                ReadOpts {
                    tier: DurabilityTier::Local,
                    ..ReadOpts::default()
                },
            ))
            .expect("read a contained local window without its own edge subscription"),
        ),
        todo_ids[8..10].to_vec(),
        "a narrower Local read derives its relative slice from the received authority window",
    );

    let narrower = main_thread
        .prepare_query(
            &Query::from("todos")
                .order_by("title", OrderDirection::Asc)
                .offset(10)
                .limit(4),
        )
        .expect("prepare narrower offset window");
    let mut narrower_subscription = block_on(main_thread.subscribe(
        &narrower,
        ReadOpts {
            tier: DurabilityTier::Edge,
            ..ReadOpts::default()
        },
    ))
    .expect("subscribe narrower window through browser relay");
    assert!(narrower_subscription.try_next_event().is_none());
    for _ in 0..12 {
        main_thread
            .tick()
            .expect("register narrower browser window");
        worker.tick().expect("forward narrower browser window");
        core.tick().expect("serve narrower browser window");
        worker.tick().expect("relay narrower browser window");
        main_thread.tick().expect("apply narrower browser window");
    }
    assert_eq!(
        row_ids(
            &block_on(main_thread.all(
                &narrower,
                ReadOpts {
                    tier: DurabilityTier::Edge,
                    ..ReadOpts::default()
                },
            ))
            .expect("read narrower authority window"),
        ),
        todo_ids[10..14].to_vec(),
        "a distinct bounded shape receives its own authority membership rather than borrowing raw rows from the first window",
    );

    writer
        .insert(
            "todos",
            BTreeMap::from([("title".to_owned(), Value::String("todo-00a".to_owned()))]),
            Default::default(),
        )
        .expect("insert row before bounded windows");
    for _ in 0..12 {
        writer.tick().expect("upload boundary-shifting todo");
        core.tick().expect("accept boundary-shifting todo");
        worker.tick().expect("relay boundary-shifting todo");
        main_thread.tick().expect("apply boundary-shifting todo");
    }
    let shifted = todo_ids[7..23].to_vec();
    assert_eq!(
        row_ids(
            &block_on(main_thread.all(
                &query,
                ReadOpts {
                    tier: DurabilityTier::Edge,
                    ..ReadOpts::default()
                },
            ))
            .expect("read shifted authority window"),
        ),
        shifted,
        "a changed upstream membership invalidates and replaces the cached window",
    );
    assert_eq!(
        row_ids(
            &block_on(main_thread.all(
                &narrower,
                ReadOpts {
                    tier: DurabilityTier::Edge,
                    ..ReadOpts::default()
                },
            ))
            .expect("read shifted narrower authority window"),
        ),
        todo_ids[9..13].to_vec(),
        "each bounded receipt shifts independently after a new leading member",
    );
    assert_eq!(
        row_ids(
            &block_on(main_thread.all(
                &contained_local,
                ReadOpts {
                    tier: DurabilityTier::Local,
                    ..ReadOpts::default()
                },
            ))
            .expect("read shifted contained local window"),
        ),
        todo_ids[7..9].to_vec(),
        "a local subwindow is invalidated with the authority window it derives from",
    );

    // Mirror a browser `db.all({ tier: "edge" })`: it releases the broad
    // coverage after its result is materialized, but its rows remain in the
    // main-thread overlay for later Local reads.
    drop(subscription);
    for _ in 0..12 {
        main_thread.tick().expect("release broad browser window");
        worker.tick().expect("forward broad window release");
        core.tick().expect("accept broad window release");
        worker.tick().expect("apply broad window release");
        main_thread.tick().expect("apply broad window cleanup");
    }

    // A later Local one-shot must recognize that the overlay is only the
    // materialized 8..24 page. It starts at the same absolute offset, so it
    // must slice relative to that page instead of returning positions 16/17.
    let same_offset_one_shot = main_thread
        .prepare_query(
            &Query::from("todos")
                .order_by("title", OrderDirection::Asc)
                .offset(8)
                .limit(2),
        )
        .expect("prepare same-offset one-shot subwindow");
    assert_eq!(
        row_ids(
            &block_on(main_thread.all(
                &same_offset_one_shot,
                ReadOpts {
                    tier: DurabilityTier::Local,
                    ..ReadOpts::default()
                },
            ))
            .expect("read same-offset local one-shot after broad coverage release"),
        ),
        todo_ids[7..9].to_vec(),
        "same-offset Local page is relative to the materialized authority page",
    );

    // The retained page is deliberately not an Edge receipt. A fresh Edge
    // usage site must clear the local-only interpretation and wait for a new
    // authority response instead of treating detached membership as current
    // authorization coverage.
    let fresh_edge_read = main_thread
        .attach_query_with_opts(
            &same_offset_one_shot,
            ReadOpts {
                tier: DurabilityTier::Edge,
                ..ReadOpts::default()
            },
        )
        .expect("attach fresh same-offset Edge read");
    assert!(
        !main_thread.query_attachment_is_covered(&fresh_edge_read),
        "a detached materialized page must not satisfy a fresh Edge read",
    );
    for _ in 0..12 {
        main_thread
            .tick()
            .expect("send fresh same-offset Edge read");
        worker.tick().expect("forward fresh same-offset Edge read");
        core.tick().expect("serve fresh same-offset Edge read");
        worker.tick().expect("relay fresh same-offset Edge read");
        main_thread
            .tick()
            .expect("apply fresh same-offset Edge receipt");
        if main_thread.query_attachment_is_covered(&fresh_edge_read) {
            break;
        }
    }
    assert!(
        main_thread.query_attachment_is_covered(&fresh_edge_read),
        "the fresh Edge page must receive a new authority receipt",
    );
    assert_eq!(
        row_ids(
            &block_on(main_thread.all(
                &same_offset_one_shot,
                ReadOpts {
                    tier: DurabilityTier::Edge,
                    ..ReadOpts::default()
                },
            ))
            .expect("read fresh same-offset Edge page"),
        ),
        todo_ids[7..9].to_vec(),
        "fresh Edge coverage replaces the detached local materialization",
    );
    main_thread.detach_query(fresh_edge_read);
}

/// A detached bounded Edge read releases its exact authoritative receipt.
/// Alice repeatedly reads distinct offset windows through a browser worker;
/// every final detach must clear its membership before the next scope opens.
///
/// ```text
/// alice one-shot Edge window ──receipt──► worker ──receipt──► main
/// alice detach ──unsubscribe──► worker ──unsubscribe──► core
/// ```
///
/// The test-only receipt count is necessary because a Local overlay may
/// legitimately retain row bodies after detach, while authoritative membership
/// itself must not outlive the usage-site scope.
#[test]
fn browser_relay_releases_each_detached_bounded_one_shot_receipt() {
    let schema = schema();
    let alice = AuthorSubject::for_test_bytes([0xc3; 16]);
    let main_thread = open_db(0x1d, alice, &schema);
    let worker = open_db(0x2d, alice, &schema);
    let core = open_core(0x3d, &schema);
    let writer = open_db(0x4d, alice, &schema);
    main_thread.set_non_durable_client();
    // Match the persistent browser worker: every bounded one-shot page is
    // re-published from its own authority-session membership and retains its
    // independent detach lifetime.
    worker.set_relay_authority_session_owner_for_test();

    let (writer_transport, core_writer_transport) = duplex();
    let _writer_connection = block_on(writer.connect_upstream(writer_transport));
    let _core_writer = core.accept_subscriber(core_writer_transport, alice);
    for index in 0..12 {
        writer
            .insert(
                "todos",
                BTreeMap::from([(
                    "title".to_owned(),
                    Value::String(format!("todo-{index:02}")),
                )]),
                Default::default(),
            )
            .expect("seed ordered todo");
    }
    for _ in 0..24 {
        writer.tick().expect("upload ordered todos");
        core.tick().expect("accept ordered todos");
        writer.tick().expect("settle ordered todos");
    }

    let (main_transport, worker_subscriber_transport) = duplex();
    let _main_connection = block_on(main_thread.connect_upstream(main_transport));
    let _worker_subscriber = worker.accept_subscriber(worker_subscriber_transport, alice);
    let (_worker_upstream, _core_subscriber) =
        connect_scope_isolated_worker_to_core!(worker, core, alice);

    for offset in 1..=5 {
        let query = main_thread
            .prepare_query(
                &Query::from("todos")
                    .order_by("title", OrderDirection::Asc)
                    .offset(offset)
                    .limit(2),
            )
            .expect("prepare distinct bounded window");
        let attachment = main_thread
            .attach_query_with_opts(
                &query,
                ReadOpts {
                    tier: DurabilityTier::Edge,
                    ..ReadOpts::default()
                },
            )
            .expect("attach bounded Edge read");
        for _ in 0..12 {
            main_thread.tick().expect("send bounded window");
            worker.tick().expect("forward bounded window");
            core.tick().expect("serve bounded window");
            worker.tick().expect("relay bounded window");
            main_thread.tick().expect("apply bounded receipt");
            if main_thread.query_attachment_is_covered(&attachment) {
                break;
            }
        }
        assert!(
            main_thread.query_attachment_is_covered(&attachment),
            "offset {offset} never received its authority receipt"
        );
        assert_eq!(
            block_on(main_thread.all(
                &query,
                ReadOpts {
                    tier: DurabilityTier::Edge,
                    ..ReadOpts::default()
                },
            ))
            .expect("read bounded authority receipt")
            .len(),
            2,
            "offset {offset} has its exact bounded membership while attached"
        );

        main_thread.detach_query(attachment);
        assert_eq!(
            main_thread.settled_authoritative_receipt_counts_for_test(),
            (0, 0),
            "final detach of offset {offset} must release its authority receipt"
        );
        for _ in 0..4 {
            main_thread.tick().expect("send bounded detach");
            worker.tick().expect("retire worker bounded view");
            core.tick().expect("retire core bounded view");
            worker.tick().expect("apply bounded teardown");
        }
        assert_eq!(
            main_thread.query_coverage_attachment_counts_for_test(),
            (0, 0),
            "offset {offset} leaves no live coverage owner"
        );
        assert_eq!(
            main_thread.settled_authoritative_receipt_counts_for_test(),
            (0, 0),
            "offset {offset} leaves no retained authority membership"
        );
        assert_eq!(
            worker.settled_authoritative_receipt_counts_for_test(),
            (0, 0),
            "offset {offset} must also release the worker's paired Global receipt",
        );
    }
}

/// Empty is a valid authority result. After withholding the relay's premature
/// cache snapshot, the later upstream response must still produce an explicit
/// settled handoff so Edge reads and subscriptions can complete.
#[test]
fn browser_relay_publishes_an_explicit_settled_empty_handoff() {
    let schema = schema();
    let alice = AuthorSubject::for_test_bytes([0xa8; 16]);
    let main_thread = open_db(0x1a, alice, &schema);
    let worker = open_db(0x29, alice, &schema);
    let core = open_core(0x39, &schema);
    main_thread.set_non_durable_client();
    worker.set_relay_authority_session_owner_for_test();

    let (main_transport, worker_subscriber_transport) = duplex();
    let _main_connection = jazz::db::block_on(main_thread.connect_upstream(main_transport));
    let _worker_subscriber = worker.accept_subscriber(worker_subscriber_transport, alice);
    let (_worker_upstream, _core_subscriber) =
        connect_scope_isolated_worker_to_core!(worker, core, alice);

    let todos = main_thread
        .prepare_query(&main_thread.table("todos"))
        .expect("prepare empty Edge query");
    let mut subscription = block_on(main_thread.subscribe(
        &todos,
        ReadOpts {
            tier: DurabilityTier::Edge,
            ..ReadOpts::default()
        },
    ))
    .expect("subscribe at Edge through worker relay");
    assert!(
        subscription.try_next_event().is_none(),
        "fresh remote coverage must withhold its provisional local snapshot"
    );

    main_thread.tick().expect("register Edge worker view");
    worker
        .tick()
        .expect("promote and forward coverage without premature settlement");
    main_thread
        .tick()
        .expect("process any pre-authority worker messages");
    assert!(
        !std::iter::from_fn(|| subscription.try_next_event())
            .any(|event| matches!(event, SubscriptionEvent::Delta { settled: true, .. }))
    );

    for _ in 0..4 {
        core.tick().expect("serve empty authority snapshot");
        worker
            .tick()
            .expect("apply empty authority snapshot and serve main");
        main_thread
            .tick()
            .expect("apply relayed empty authority snapshot");
    }
    let settled = std::iter::from_fn(|| subscription.try_next_event()).collect::<Vec<_>>();
    assert!(
        settled.iter().any(|event| matches!(
            event,
            SubscriptionEvent::Delta {
                added,
                settled: true,
                ..
            } if added.is_empty()
        )),
        "expected explicit settled-empty handoff, got {settled:?}"
    );
}

/// A relay can carry two policy-scoped authority streams for the same
/// canonical empty query. Each downstream browser must receive its own
/// settled handoff; one receipt cannot make the sibling's group ambiguous or
/// leave it waiting forever.
#[test]
fn browser_relay_hands_off_each_policy_scoped_empty_result_independently() {
    let schema = schema();
    let alice = AuthorSubject::for_test_bytes([0xa8; 16]);
    let bob = AuthorSubject::for_test_bytes([0xb8; 16]);
    let alice_main = open_db(0x1a, alice, &schema);
    let bob_main = open_db(0x1b, bob, &schema);
    let worker = open_db(0x29, AuthorSubject::SYSTEM, &schema);
    let core = open_core(0x39, &schema);
    alice_main.set_non_durable_client();
    bob_main.set_non_durable_client();

    let (alice_transport, worker_alice_transport) = duplex();
    let _alice_connection = block_on(alice_main.connect_upstream(alice_transport));
    let _worker_alice = worker.accept_subscriber(worker_alice_transport, alice);
    let (bob_transport, worker_bob_transport) = duplex();
    let _bob_connection = block_on(bob_main.connect_upstream(bob_transport));
    let _worker_bob = worker.accept_subscriber(worker_bob_transport, bob);
    let (worker_upstream_transport, core_transport) = duplex();
    let _worker_upstream = block_on(worker.connect_upstream(Box::new(
        TrustedBackendRelayTransport {
            inner: worker_upstream_transport,
        },
    )));
    let _core_subscriber = core.accept_subscriber_with_claims_and_trust(
        core_transport,
        AuthorSubject::SYSTEM,
        BTreeMap::new(),
        CommitUnitTrust::TrustedBackend,
    );

    let alice_todos = alice_main
        .prepare_query(&alice_main.table("todos"))
        .expect("prepare Alice empty Edge query");
    let bob_todos = bob_main
        .prepare_query(&bob_main.table("todos"))
        .expect("prepare Bob empty Edge query");
    let edge_opts = ReadOpts {
        tier: DurabilityTier::Edge,
        ..ReadOpts::default()
    };
    let mut alice_subscription = block_on(alice_main.subscribe(&alice_todos, edge_opts.clone()))
        .expect("subscribe Alice at Edge through worker relay");
    let mut bob_subscription = block_on(bob_main.subscribe(&bob_todos, edge_opts))
        .expect("subscribe Bob at Edge through worker relay");
    assert!(alice_subscription.try_next_event().is_none());
    assert!(bob_subscription.try_next_event().is_none());

    for _ in 0..8 {
        alice_main.tick().expect("register Alice worker view");
        bob_main.tick().expect("register Bob worker view");
        worker.tick().expect("forward both policy groups upstream");
        core.tick().expect("serve authority snapshots");
        worker
            .tick()
            .expect("apply and hand off both authority snapshots");
        alice_main.tick().expect("apply Alice handoff");
        bob_main.tick().expect("apply Bob handoff");
    }

    let is_settled_empty = |event: &SubscriptionEvent| {
        matches!(
            event,
            SubscriptionEvent::Delta {
                added,
                settled: true,
                ..
            } if added.is_empty()
        )
    };
    let alice_events =
        std::iter::from_fn(|| alice_subscription.try_next_event()).collect::<Vec<_>>();
    let bob_events = std::iter::from_fn(|| bob_subscription.try_next_event()).collect::<Vec<_>>();
    assert!(
        alice_events.iter().any(is_settled_empty),
        "Alice needs her own settled-empty handoff: {alice_events:?}"
    );
    assert!(
        bob_events.iter().any(is_settled_empty),
        "Bob needs her own settled-empty handoff: {bob_events:?}"
    );
    assert_eq!(
        worker.relay_upstream_subscription_owner_count_for_test(),
        2,
        "distinct downstream policy scopes retain distinct relay upstream usage sites"
    );
}

/// Reopening the in-memory main runtime must replay the accepted parent of a
/// pending update before replaying the update's Local acknowledgement.
#[test]
fn browser_relay_replays_causal_ancestors_before_pending_write_fates() {
    let schema = schema();
    let alice = AuthorSubject::for_test_bytes([0xa7; 16]);
    let worker = open_db(0x28, alice, &schema);
    let core = open_core(0x38, &schema);
    worker.set_relay_authority_session_owner_for_test();

    let (worker_upstream, core_subscriber) =
        connect_scope_isolated_worker_to_core!(worker, core, alice);
    let base = worker
        .insert(
            "todos",
            BTreeMap::from([(
                "title".to_owned(),
                Value::String("accepted parent".to_owned()),
            )]),
            Default::default(),
        )
        .expect("insert base row");
    let row = base.row_uuid();
    let base_tx = base.mergeable_tx_id();
    worker.tick().expect("upload base row");
    core.tick().expect("accept base row");
    worker.tick().expect("apply base-row fate");
    assert!(matches!(
        worker.write_state(base_tx).expect("base write state"),
        jazz::db::WriteState {
            fate: Fate::Accepted,
            durability: DurabilityTier::Global,
            global_time: Some(_),
        }
    ));
    assert!(worker.detach_connection(&worker_upstream));
    assert!(core.detach_connection(&core_subscriber));

    let first_main = open_db(0x19, alice, &schema);
    first_main.set_non_durable_client();
    let (first_main_transport, first_worker_transport) = duplex();
    let first_main_connection =
        jazz::db::block_on(first_main.connect_upstream(first_main_transport));
    let first_worker_connection = worker.accept_subscriber(first_worker_transport, alice);
    let todos = first_main
        .prepare_query(&first_main.table("todos"))
        .expect("prepare local todos query");
    let _subscription =
        block_on(first_main.subscribe(&todos, ReadOpts::default())).expect("subscribe locally");
    let scheduler = Rc::new(CountingScheduler::default());
    worker.set_tick_scheduler(Some(scheduler.clone()));
    first_main.tick().expect("request worker-local row");
    scheduler.clear();
    worker.tick().expect("serve worker-local row");
    assert_scheduled_urgencies(
        &scheduler,
        &[
            TickUrgency::AfterCurrentTurn,
            TickUrgency::AfterCurrentTurn,
            TickUrgency::AfterCurrentTurn,
        ],
        "initial causal parent publication",
    );
    worker
        .tick()
        .expect("serve worker-local row on the scheduled follow-up turn");
    first_main.tick().expect("hydrate accepted parent");
    assert_eq!(
        first_main.read(&todos).expect("read accepted parent").len(),
        1
    );

    let update = first_main
        .update(
            "todos",
            row,
            BTreeMap::from([(
                "title".to_owned(),
                Value::String("pending child".to_owned()),
            )]),
            Default::default(),
        )
        .expect("update accepted row offline");
    let update_tx = update.mergeable_tx_id();
    first_main.tick().expect("upload pending child");
    worker.tick().expect("persist pending child");
    first_main.tick().expect("apply pending child's Local ack");
    assert!(first_main.detach_connection(&first_main_connection));
    assert!(worker.detach_connection(&first_worker_connection));
    drop(first_main);

    let reopened_main = open_db(0x19, alice, &schema);
    reopened_main.set_non_durable_client();
    let (reopened_main_transport, reopened_worker_transport) = duplex();
    let _reopened_main_connection =
        jazz::db::block_on(reopened_main.connect_upstream(reopened_main_transport));
    let _reopened_worker_connection = worker.accept_subscriber(reopened_worker_transport, alice);

    worker
        .tick()
        .expect("send accepted parent, pending child, and Local ack in causal order");
    reopened_main
        .tick()
        .expect("apply causal replay without a missing-transaction protocol error");
    assert_eq!(
        reopened_main
            .write_state(update_tx)
            .expect("replayed update state"),
        jazz::db::WriteState {
            fate: Fate::Pending,
            durability: DurabilityTier::Local,
            global_time: None,
        }
    );
}

/// A worker relay forwards an authority rejection without replacing it with a
/// worker verdict. Alice still receives Local durability first, then the core
/// rejection when the worker routes it back over the same private link.
///
/// ```text
/// alice main Db ──alice commit──► worker relay ──alice commit──► bob session/core
///       ▲                            │                              │
///       ├──────Pending/Local─────────┘                              │
///       └────────────────Rejected(AuthorizationDenied)──────────────┘
/// ```
#[test]
fn worker_relay_forwards_authority_rejection_to_browser_client() {
    let schema = schema();
    let alice = AuthorSubject::for_test_bytes([0xa4; 16]);
    let bob = AuthorSubject::for_test_bytes([0xb4; 16]);
    let main_thread = open_db(0x14, alice, &schema);
    let worker = open_db(0x25, AuthorSubject::SYSTEM, &schema);
    let core = open_core(0x35, &schema);
    main_thread.set_non_durable_client();

    let (main_transport, worker_subscriber_transport) = duplex();
    let _main_connection = jazz::db::block_on(main_thread.connect_upstream(main_transport));
    let _worker_subscriber = worker.accept_subscriber(worker_subscriber_transport, alice);
    let (worker_upstream_transport, core_transport) = duplex();
    let _worker_upstream = jazz::db::block_on(worker.connect_upstream(worker_upstream_transport));
    let _core_subscriber = core.accept_subscriber(core_transport, bob);

    let write = main_thread
        .insert(
            "todos",
            BTreeMap::from([(
                "title".to_owned(),
                Value::String("reject after local persistence".to_owned()),
            )]),
            Default::default(),
        )
        .expect("insert optimistic todo");
    let tx_id = write.mergeable_tx_id();
    let global_wait_rejected = Rc::new(Cell::new(None));
    let observed_wait = Rc::clone(&global_wait_rejected);
    main_thread.wait_for_transaction_with(tx_id, DurabilityTier::Global, move |result| {
        observed_wait.set(Some(result.is_err()));
    });

    main_thread.tick().expect("upload to worker");
    worker.tick().expect("persist and forward to core");
    main_thread.tick().expect("apply worker Local ack");
    assert_eq!(global_wait_rejected.get(), None);

    let worker_scheduler = Rc::new(CountingScheduler::default());
    worker.set_tick_scheduler(Some(worker_scheduler.clone()));
    core.tick().expect("reject mismatched session author");
    worker_scheduler.clear();
    worker.tick().expect("apply and forward rejection");
    assert_scheduled_urgencies(
        &worker_scheduler,
        &[TickUrgency::AfterCurrentTurn],
        "core rejection ingress at the worker",
    );
    worker
        .tick()
        .expect("publish rejection on the scheduled worker follow-up turn");
    main_thread.tick().expect("apply rejection through worker");

    assert_eq!(global_wait_rejected.get(), Some(true));
    assert!(matches!(
        main_thread
            .write_state(tx_id)
            .expect("main rejected state")
            .fate,
        Fate::Rejected(_)
    ));
}

#[test]
fn reopened_worker_replays_pending_commit_before_later_fate() {
    let schema = schema();
    let alice = AuthorSubject::for_test_bytes([0xa5; 16]);
    let storage = tempfile::tempdir().expect("worker temp dir");
    let first_main = open_db(0x15, alice, &schema);
    first_main.set_non_durable_client();
    let first_worker = open_persistent_worker(storage.path(), 0x26, &schema);
    let (first_main_transport, first_worker_transport) = duplex();
    let first_main_connection =
        jazz::db::block_on(first_main.connect_upstream(first_main_transport));
    let first_worker_connection = first_worker.accept_subscriber(first_worker_transport, alice);

    let write = first_main
        .insert(
            "todos",
            BTreeMap::from([(
                "title".to_owned(),
                Value::String("pending across worker restart".to_owned()),
            )]),
            Default::default(),
        )
        .expect("insert pending todo");
    let tx_id = write.mergeable_tx_id();
    first_main.tick().expect("upload to first worker");
    first_worker.tick().expect("persist in first worker");
    first_main.tick().expect("apply first Local ack");
    drop(first_worker_connection);
    drop(first_main_connection);
    drop(first_worker);
    drop(first_main);

    let second_main = open_db(0x15, alice, &schema);
    second_main.set_non_durable_client();
    let second_worker = open_persistent_worker(storage.path(), 0x26, &schema);
    let (second_main_transport, second_worker_transport) = duplex();
    let _second_main_connection =
        jazz::db::block_on(second_main.connect_upstream(second_main_transport));
    let _second_worker_connection = second_worker.accept_subscriber(second_worker_transport, alice);

    second_worker
        .tick()
        .expect("replay commit and Local ack downstream");
    second_main
        .tick()
        .expect("apply replayed commit before Local ack");
    assert_eq!(
        second_main
            .write_state(tx_id)
            .expect("replayed write state"),
        jazz::db::WriteState {
            fate: Fate::Pending,
            durability: DurabilityTier::Local,
            global_time: None,
        }
    );
    let todos = second_main
        .prepare_query(&second_main.table("todos"))
        .expect("prepare reopened query");
    assert_eq!(
        second_main.read(&todos).expect("read replayed row").len(),
        1
    );
}

/// A worker restores Alice's former foreground transaction, but only carries
/// its terminal header/fate long enough to notify Alice's live successor.
/// Its distinct worker node must never own the rejected retry payload.
///
/// ```text
/// former Alice tab ──Local write──► durable worker ──restart──► successor tab
///                                        │                         │
///                                        └──Rejected──► one live callback
///                                             └── no foreign retry payload
/// ```
#[test]
fn reopened_worker_notifies_attached_successor_of_foreground_rejection() {
    let schema = schema();
    let alice = AuthorSubject::for_test_bytes([0xa9; 16]);
    let bob = AuthorSubject::for_test_bytes([0xb9; 16]);
    let storage = tempfile::tempdir().expect("worker temp dir");

    let first_main = open_db(0x1b, alice, &schema);
    first_main.set_non_durable_client();
    let first_worker = open_persistent_browser_worker(storage.path(), 0x2a, alice, &schema);
    let (first_main_transport, first_worker_transport) = duplex();
    let first_main_connection =
        jazz::db::block_on(first_main.connect_upstream(first_main_transport));
    let first_worker_connection = first_worker.accept_subscriber(first_worker_transport, alice);

    let write = first_main
        .insert(
            "todos",
            BTreeMap::from([(
                "title".to_owned(),
                Value::String("reject after worker restart".to_owned()),
            )]),
            Default::default(),
        )
        .expect("insert pending todo");
    let tx_id = write.mergeable_tx_id();
    first_main.tick().expect("upload to first worker");
    first_worker.tick().expect("persist in first worker");
    first_main.tick().expect("apply first Local ack");
    drop(first_worker_connection);
    drop(first_main_connection);
    drop(first_worker);
    drop(first_main);

    // A fresh foreground runtime must use a distinct physical node identity.
    // The worker owns durable replay, while the successor only receives the
    // worker's live rejection notification.
    let reopened_main = open_db(0x1c, alice, &schema);
    reopened_main.set_non_durable_client();
    let reopened_worker = open_persistent_browser_worker(storage.path(), 0x2a, alice, &schema);
    let mutation_errors = Rc::new(RefCell::new(Vec::new()));
    let observed_errors = Rc::clone(&mutation_errors);
    reopened_worker.on_mutation_error(Rc::new(move |event| {
        observed_errors.borrow_mut().push(event.clone());
    }));
    let core = open_core(0x3a, &schema);
    let (main_transport, worker_subscriber_transport) = duplex();
    let main_connection = jazz::db::block_on(reopened_main.connect_upstream(main_transport));
    let worker_subscriber = reopened_worker.accept_subscriber(worker_subscriber_transport, alice);
    let (worker_upstream_transport, core_transport) = duplex();
    let worker_upstream =
        jazz::db::block_on(reopened_worker.connect_upstream(worker_upstream_transport));
    let core_subscriber = core.accept_subscriber(core_transport, bob);

    reopened_worker
        .tick()
        .expect("replay pending write in both directions");
    reopened_main
        .tick()
        .expect("apply replayed write and Local acknowledgement");
    core.tick().expect("reject mismatched session author");
    reopened_worker
        .tick()
        .expect("apply and route authority rejection");
    assert!(
        !reopened_worker.has_retained_rejection_for_test(tx_id),
        "the worker may retain the replayed transaction header/fate for its live notification, but must not retain its foreign retry payload"
    );
    reopened_worker
        .tick()
        .expect("deliver the relay-owned live rejection exactly once");
    reopened_main
        .tick()
        .expect("apply rejection after replayed Local acknowledgement");

    assert!(matches!(
        reopened_main
            .write_state(tx_id)
            .expect("replayed rejected state")
            .fate,
        Fate::Rejected(_)
    ));
    assert_eq!(mutation_errors.borrow().len(), 1);
    assert_eq!(
        mutation_errors.borrow()[0].transaction.transaction_id,
        TransactionId::from_committed_tx(tx_id)
    );
    drop(main_connection);
    drop(worker_subscriber);
    drop(worker_upstream);
    drop(core_subscriber);
    drop(reopened_main);
    drop(reopened_worker);

    // Reopening without an attached browser runtime must not replay the
    // prior notification. This public lifecycle receipt proves the worker did
    // not persist foreign rejected payload/version state (INV-TX-9).
    let after_appless_interval =
        open_persistent_browser_worker(storage.path(), 0x2a, alice, &schema);
    assert!(
        !after_appless_interval.has_retained_rejection_for_test(tx_id),
        "reopening the worker must not recover a foreign retry payload"
    );
    let after_restart_errors = Rc::new(RefCell::new(Vec::new()));
    let observed_after_restart = Rc::clone(&after_restart_errors);
    after_appless_interval.on_mutation_error(Rc::new(move |event| {
        observed_after_restart.borrow_mut().push(event.clone());
    }));
    after_appless_interval
        .tick()
        .expect("drive post-rejection browser relay opening");
    assert!(
        after_restart_errors.borrow().is_empty(),
        "a later browser relay must not replay the prior app notification"
    );
}

/// A recovered browser-relay marker is a one-live-terminal-fate routing aid,
/// not a rejection queue. An Accepted/Global fate must consume it just as a
/// rejection does, publish normal settled state, and leave no later callback
/// backlog.
#[test]
fn reopened_worker_forgets_recovered_foreground_marker_after_global_acceptance() {
    let schema = schema();
    let alice = AuthorSubject::for_test_bytes([0xad; 16]);
    let storage = tempfile::tempdir().expect("worker temp dir");

    let first_main = open_db(0x1f, alice, &schema);
    first_main.set_non_durable_client();
    let first_worker = open_persistent_browser_worker(storage.path(), 0x2f, alice, &schema);
    let (first_main_transport, first_worker_transport) = duplex();
    let first_main_connection = block_on(first_main.connect_upstream(first_main_transport));
    let first_worker_connection = first_worker.accept_subscriber(first_worker_transport, alice);
    let write = first_main
        .insert(
            "todos",
            BTreeMap::from([(
                "title".to_owned(),
                Value::String("accept after worker restart".to_owned()),
            )]),
            Default::default(),
        )
        .expect("insert pending todo");
    let tx_id = write.mergeable_tx_id();
    first_main.tick().expect("upload to first worker");
    first_worker.tick().expect("persist in first worker");
    first_main.tick().expect("apply first Local ack");
    drop(first_worker_connection);
    drop(first_main_connection);
    drop(first_worker);
    drop(first_main);

    let successor = open_db(0x20, alice, &schema);
    successor.set_non_durable_client();
    let worker = open_persistent_browser_worker(storage.path(), 0x2f, alice, &schema);
    worker.set_relay_authority_session_owner_for_test();
    assert!(
        worker.has_recovered_browser_relay_tx_for_test(tx_id),
        "worker restart must mark the recovered unresolved foreground transaction"
    );
    let mutation_errors = Rc::new(RefCell::new(Vec::new()));
    let observed_errors = Rc::clone(&mutation_errors);
    worker.on_mutation_error(Rc::new(move |event| {
        observed_errors.borrow_mut().push(event.clone());
    }));
    let core = open_core(0x3f, &schema);
    let (successor_transport, worker_subscriber_transport) = duplex();
    let successor_connection = block_on(successor.connect_upstream(successor_transport));
    let worker_subscriber = worker.accept_subscriber(worker_subscriber_transport, alice);
    let (worker_upstream, core_subscriber) =
        connect_scope_isolated_worker_to_core!(worker, core, alice);

    worker.tick().expect("replay recovered foreground write");
    successor
        .tick()
        .expect("apply replayed Local acknowledgement");
    let scheduler = Rc::new(CountingScheduler::default());
    worker.set_tick_scheduler(Some(scheduler.clone()));
    core.tick().expect("accept matching session author");
    scheduler.clear();
    worker.tick().expect("apply accepted Global fate");
    assert_scheduled_urgencies(
        &scheduler,
        &[TickUrgency::AfterCurrentTurn],
        "recovered accepted fate ingress at the worker",
    );
    worker
        .tick()
        .expect("publish accepted Global fate on the scheduled follow-up turn");
    successor.tick().expect("apply accepted fate through relay");

    assert!(matches!(
        worker.write_state(tx_id).expect("worker accepted state"),
        jazz::db::WriteState {
            fate: Fate::Accepted,
            durability: DurabilityTier::Global,
            global_time: Some(_),
        }
    ));
    assert!(matches!(
        successor
            .write_state(tx_id)
            .expect("successor accepted state"),
        jazz::db::WriteState {
            fate: Fate::Accepted,
            durability: DurabilityTier::Global,
            global_time: Some(_),
        }
    ));
    assert!(
        !worker.has_recovered_browser_relay_tx_for_test(tx_id),
        "Accepted/Global must consume the process-local recovered marker"
    );
    worker
        .tick()
        .expect("drive any erroneous fallback delivery");
    assert!(
        mutation_errors.borrow().is_empty(),
        "accepted recovery must never report a mutation rejection"
    );

    drop(successor_connection);
    drop(worker_subscriber);
    drop(worker_upstream);
    drop(core_subscriber);
    drop(successor);
    drop(worker);

    let later_worker = open_persistent_browser_worker(storage.path(), 0x2f, alice, &schema);
    assert!(
        !later_worker.has_recovered_browser_relay_tx_for_test(tx_id),
        "accepted terminal state must leave no recovered-marker backlog"
    );
    let later_errors = Rc::new(RefCell::new(Vec::new()));
    let observed_later_errors = Rc::clone(&later_errors);
    later_worker.on_mutation_error(Rc::new(move |event| {
        observed_later_errors.borrow_mut().push(event.clone());
    }));
    later_worker.tick().expect("drive later worker opening");
    assert!(
        later_errors.borrow().is_empty(),
        "a late attachment must not receive a stale recovery callback"
    );
}

/// A browser relay may have an active programmatic wait for a transaction it
/// restored from a former foreground runtime. That wait consumes the same live
/// rejection instead of allowing a duplicate fallback callback.
#[test]
fn recovered_browser_relay_wait_suppresses_mutation_error_fallback() {
    let schema = schema();
    let alice = AuthorSubject::for_test_bytes([0xac; 16]);
    let bob = AuthorSubject::for_test_bytes([0xbc; 16]);
    let storage = tempfile::tempdir().expect("worker temp dir");

    let first_main = open_db(0x1d, alice, &schema);
    first_main.set_non_durable_client();
    let first_worker = open_persistent_browser_worker(storage.path(), 0x2d, alice, &schema);
    let (first_main_transport, first_worker_transport) = duplex();
    let first_main_connection = block_on(first_main.connect_upstream(first_main_transport));
    let first_worker_connection = first_worker.accept_subscriber(first_worker_transport, alice);
    let write = first_main
        .insert(
            "todos",
            BTreeMap::from([(
                "title".to_owned(),
                Value::String("wait handles replayed rejection".to_owned()),
            )]),
            Default::default(),
        )
        .expect("insert pending todo");
    let tx_id = write.mergeable_tx_id();
    first_main.tick().expect("upload to first worker");
    first_worker.tick().expect("persist in first worker");
    first_main.tick().expect("apply first Local ack");
    drop(first_worker_connection);
    drop(first_main_connection);
    drop(first_worker);
    drop(first_main);

    let successor = open_db(0x1e, alice, &schema);
    successor.set_non_durable_client();
    let worker = open_persistent_browser_worker(storage.path(), 0x2d, alice, &schema);
    let fallback_errors = Rc::new(RefCell::new(Vec::new()));
    let observed_fallback_errors = Rc::clone(&fallback_errors);
    worker.on_mutation_error(Rc::new(move |event| {
        observed_fallback_errors.borrow_mut().push(event.clone());
    }));
    let wait_result = Rc::new(Cell::new(None));
    let observed_wait = Rc::clone(&wait_result);
    worker.wait_for_transaction_with(tx_id, DurabilityTier::Global, move |result| {
        observed_wait.set(Some(result.is_err()));
    });

    let core = open_core(0x3d, &schema);
    let (successor_transport, worker_subscriber_transport) = duplex();
    let _successor_connection = block_on(successor.connect_upstream(successor_transport));
    let _worker_subscriber = worker.accept_subscriber(worker_subscriber_transport, alice);
    let (worker_upstream_transport, core_transport) = duplex();
    let _worker_upstream = block_on(worker.connect_upstream(worker_upstream_transport));
    let _core_subscriber = core.accept_subscriber(core_transport, bob);

    worker.tick().expect("replay pending write");
    successor
        .tick()
        .expect("apply replayed Local acknowledgement");
    core.tick().expect("reject mismatched session author");
    worker.tick().expect("apply rejection to relay wait");
    worker.tick().expect("drive any fallback delivery");

    assert_eq!(wait_result.get(), Some(true));
    assert!(
        fallback_errors.borrow().is_empty(),
        "the active wait must consume the relay rejection before fallback delivery"
    );
}
