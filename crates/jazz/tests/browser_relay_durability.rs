use std::cell::Cell;
use std::collections::BTreeMap;
use std::rc::Rc;

#[path = "support/duplex_transport.rs"]
mod duplex_transport;

use duplex_transport::duplex;
use jazz::db::{Db, DbConfig, DbIdentity, ReadOpts, SubscriptionEvent, block_on};
use jazz::groove::records::Value;
use jazz::groove::schema::{ColumnSchema, ColumnType};
use jazz::groove::storage::MemoryStorage;
#[cfg(feature = "rocksdb")]
use jazz::groove::storage::RocksDbStorage;
use jazz::ids::{AuthorId, NodeUuid};
use jazz::schema::{JazzSchema, TableSchema};
use jazz::tx::{DurabilityTier, Fate};

fn schema() -> JazzSchema {
    JazzSchema::new([TableSchema::new(
        "todos",
        [ColumnSchema::new("title", ColumnType::String)],
    )])
}

fn open_db(node: u8, author: AuthorId, schema: &JazzSchema) -> Db<MemoryStorage> {
    let column_families = schema.column_families();
    let refs = column_families
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    block_on(Db::open(DbConfig::new(
        schema.clone(),
        MemoryStorage::new(&refs),
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author,
        },
    )))
    .expect("open database")
}

fn open_core(node: u8, schema: &JazzSchema) -> Db<MemoryStorage> {
    let column_families = schema.column_families();
    let refs = column_families
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    block_on(Db::open_history_complete(DbConfig::new(
        schema.clone(),
        MemoryStorage::new(&refs),
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author: AuthorId::SYSTEM,
        },
    )))
    .expect("open core database")
}

#[cfg(feature = "rocksdb")]
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
            author: AuthorId::SYSTEM,
        },
    )))
    .expect("open persistent worker")
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
    let alice = AuthorId::from_bytes([0xa1; 16]);
    let main_thread = open_db(0x11, alice, &schema);
    let worker = open_db(0x22, AuthorId::SYSTEM, &schema);
    main_thread.set_non_durable_client();

    let (main_transport, worker_transport) = duplex();
    let _main_connection = main_thread.connect_upstream(main_transport);
    let _worker_connection = worker.accept_subscriber(worker_transport, alice);

    let write = main_thread
        .insert(
            "todos",
            BTreeMap::from([(
                "title".to_owned(),
                Value::String("persist me in the worker".to_owned()),
            )]),
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
        }
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
    let alice = AuthorId::from_bytes([0xa2; 16]);
    let main_thread = open_db(0x12, alice, &schema);
    let worker = open_db(0x23, AuthorId::SYSTEM, &schema);
    let core = open_core(0x34, &schema);
    main_thread.set_non_durable_client();

    let (main_transport, worker_subscriber_transport) = duplex();
    let _main_connection = main_thread.connect_upstream(main_transport);
    let _worker_subscriber = worker.accept_subscriber(worker_subscriber_transport, alice);

    let (worker_upstream_transport, core_transport) = duplex();
    let _worker_upstream = worker.connect_upstream(worker_upstream_transport);
    let _core_subscriber = core.accept_subscriber(core_transport, alice);

    let write = main_thread
        .insert(
            "todos",
            BTreeMap::from([(
                "title".to_owned(),
                Value::String("relay me unchanged".to_owned()),
            )]),
        )
        .expect("insert relayed todo");
    let tx_id = write.mergeable_tx_id();

    let global_wait = Rc::new(Cell::new(None));
    let observed_wait = Rc::clone(&global_wait);
    main_thread.wait_for_transaction_with(tx_id, DurabilityTier::Global, move |result| {
        observed_wait.set(Some(result.is_ok()));
    });

    main_thread.tick().expect("upload to worker");
    worker.tick().expect("persist and forward to core");
    main_thread.tick().expect("apply worker Local ack");
    assert_eq!(global_wait.get(), None);

    core.tick().expect("accept at core");
    worker
        .tick()
        .expect("apply and forward core fate downstream");
    main_thread.tick().expect("apply core fate through worker");

    assert_eq!(global_wait.get(), Some(true));
    assert_eq!(
        main_thread.write_state(tx_id).expect("main global state"),
        jazz::db::WriteState {
            fate: Fate::Accepted,
            durability: DurabilityTier::Global,
        }
    );
    assert_eq!(
        worker.write_state(tx_id).expect("worker global state"),
        jazz::db::WriteState {
            fate: Fate::Accepted,
            durability: DurabilityTier::Global,
        }
    );

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
    assert!(events.iter().any(|event| matches!(
        event,
        SubscriptionEvent::Delta { added, .. } if added.len() == 1
    )));
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
    let alice = AuthorId::from_bytes([0xa3; 16]);
    let worker = open_db(0x24, AuthorId::SYSTEM, &schema);
    worker
        .insert(
            "todos",
            BTreeMap::from([(
                "title".to_owned(),
                Value::String("persisted before main thread opens".to_owned()),
            )]),
        )
        .expect("seed worker-local todo");

    let main_thread = open_db(0x13, alice, &schema);
    main_thread.set_non_durable_client();
    let (main_transport, worker_transport) = duplex();
    let _main_connection = main_thread.connect_upstream(main_transport);
    let _worker_connection = worker.accept_subscriber(worker_transport, alice);

    let todos = main_thread
        .prepare_query(&main_thread.table("todos"))
        .expect("prepare todos query");
    let mut subscription =
        block_on(main_thread.subscribe(&todos, ReadOpts::default())).expect("subscribe to todos");
    let Some(SubscriptionEvent::Delta { added, .. }) = subscription.try_next_event() else {
        panic!("fresh subscription must emit an initial delta");
    };
    assert!(added.is_empty());

    main_thread.tick().expect("request Local worker view");
    worker.tick().expect("serve Local worker view");
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

/// An authority-tier browser subscription must not treat the worker's current
/// empty cache as a settled result. The relay first registers the same coverage
/// upstream, then publishes the authority's settled snapshot downstream.
#[test]
fn browser_relay_does_not_publish_a_premature_settled_snapshot() {
    let schema = schema();
    let alice = AuthorId::from_bytes([0xa6; 16]);
    let main_thread = open_db(0x17, alice, &schema);
    let worker = open_db(0x27, alice, &schema);
    let core = open_core(0x37, &schema);
    main_thread.set_non_durable_client();

    let seeder = open_db(0x18, alice, &schema);
    let (seeder_transport, core_seed_transport) = duplex();
    let _seeder_connection = seeder.connect_upstream(seeder_transport);
    let _core_seed_subscriber = core.accept_subscriber(core_seed_transport, alice);
    let seeded = seeder
        .insert(
            "todos",
            BTreeMap::from([(
                "title".to_owned(),
                Value::String("already settled at the authority".to_owned()),
            )]),
        )
        .expect("seed authority todo");
    let seeded_tx = seeded.mergeable_tx_id();
    seeder.tick().expect("upload seeded row");
    core.tick().expect("accept seeded row");
    seeder.tick().expect("apply seeded-row fate");
    assert_eq!(
        seeder.write_state(seeded_tx).expect("seeded write state"),
        jazz::db::WriteState {
            fate: Fate::Accepted,
            durability: DurabilityTier::Global,
        }
    );

    // Match browser-worker initialization order: accept the main-thread relay
    // first, then attach the worker's server transport.
    let (main_transport, worker_subscriber_transport) = duplex();
    let _main_connection = main_thread.connect_upstream(main_transport);
    let _worker_subscriber = worker.accept_subscriber(worker_subscriber_transport, alice);
    let (worker_upstream_transport, core_transport) = duplex();
    let _worker_upstream = worker.connect_upstream(worker_upstream_transport);
    let _core_subscriber = core.accept_subscriber(core_transport, alice);

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
    let Some(SubscriptionEvent::Delta { added, settled, .. }) = subscription.try_next_event()
    else {
        panic!("fresh subscription must emit an initial local delta");
    };
    assert!(added.is_empty());
    assert!(!settled);

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

/// Empty is a valid authority result. After withholding the relay's premature
/// cache snapshot, the later upstream response must still produce an explicit
/// settled handoff so Edge reads and subscriptions can complete.
#[test]
fn browser_relay_publishes_an_explicit_settled_empty_handoff() {
    let schema = schema();
    let alice = AuthorId::from_bytes([0xa8; 16]);
    let main_thread = open_db(0x1a, alice, &schema);
    let worker = open_db(0x29, alice, &schema);
    let core = open_core(0x39, &schema);
    main_thread.set_non_durable_client();

    let (main_transport, worker_subscriber_transport) = duplex();
    let _main_connection = main_thread.connect_upstream(main_transport);
    let _worker_subscriber = worker.accept_subscriber(worker_subscriber_transport, alice);
    let (worker_upstream_transport, core_transport) = duplex();
    let _worker_upstream = worker.connect_upstream(worker_upstream_transport);
    let _core_subscriber = core.accept_subscriber(core_transport, alice);

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
    let _initial = subscription
        .try_next_event()
        .expect("subscription starts with an unsettled local snapshot");

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

/// Reopening the in-memory main runtime must replay the accepted parent of a
/// pending update before replaying the update's Local acknowledgement.
#[test]
fn browser_relay_replays_causal_ancestors_before_pending_write_fates() {
    let schema = schema();
    let alice = AuthorId::from_bytes([0xa7; 16]);
    let worker = open_db(0x28, alice, &schema);
    let core = open_core(0x38, &schema);

    let (worker_upstream_transport, core_transport) = duplex();
    let worker_upstream = worker.connect_upstream(worker_upstream_transport);
    let core_subscriber = core.accept_subscriber(core_transport, alice);
    let base = worker
        .insert(
            "todos",
            BTreeMap::from([(
                "title".to_owned(),
                Value::String("accepted parent".to_owned()),
            )]),
        )
        .expect("insert base row");
    let row = base.row_uuid();
    let base_tx = base.mergeable_tx_id();
    worker.tick().expect("upload base row");
    core.tick().expect("accept base row");
    worker.tick().expect("apply base-row fate");
    assert_eq!(
        worker.write_state(base_tx).expect("base write state"),
        jazz::db::WriteState {
            fate: Fate::Accepted,
            durability: DurabilityTier::Global,
        }
    );
    assert!(worker.detach_connection(&worker_upstream));
    assert!(core.detach_connection(&core_subscriber));

    let first_main = open_db(0x19, alice, &schema);
    first_main.set_non_durable_client();
    let (first_main_transport, first_worker_transport) = duplex();
    let first_main_connection = first_main.connect_upstream(first_main_transport);
    let first_worker_connection = worker.accept_subscriber(first_worker_transport, alice);
    let todos = first_main
        .prepare_query(&first_main.table("todos"))
        .expect("prepare local todos query");
    let _subscription =
        block_on(first_main.subscribe(&todos, ReadOpts::default())).expect("subscribe locally");
    first_main.tick().expect("request worker-local row");
    worker.tick().expect("serve worker-local row");
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
    let _reopened_main_connection = reopened_main.connect_upstream(reopened_main_transport);
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
    let alice = AuthorId::from_bytes([0xa4; 16]);
    let bob = AuthorId::from_bytes([0xb4; 16]);
    let main_thread = open_db(0x14, alice, &schema);
    let worker = open_db(0x25, AuthorId::SYSTEM, &schema);
    let core = open_core(0x35, &schema);
    main_thread.set_non_durable_client();

    let (main_transport, worker_subscriber_transport) = duplex();
    let _main_connection = main_thread.connect_upstream(main_transport);
    let _worker_subscriber = worker.accept_subscriber(worker_subscriber_transport, alice);
    let (worker_upstream_transport, core_transport) = duplex();
    let _worker_upstream = worker.connect_upstream(worker_upstream_transport);
    let _core_subscriber = core.accept_subscriber(core_transport, bob);

    let write = main_thread
        .insert(
            "todos",
            BTreeMap::from([(
                "title".to_owned(),
                Value::String("reject after local persistence".to_owned()),
            )]),
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

    core.tick().expect("reject mismatched session author");
    worker.tick().expect("apply and forward rejection");
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

#[cfg(feature = "rocksdb")]
#[test]
fn reopened_worker_replays_pending_commit_before_later_fate() {
    let schema = schema();
    let alice = AuthorId::from_bytes([0xa5; 16]);
    let storage = tempfile::tempdir().expect("worker temp dir");
    let first_main = open_db(0x15, alice, &schema);
    first_main.set_non_durable_client();
    let first_worker = open_persistent_worker(storage.path(), 0x26, &schema);
    let (first_main_transport, first_worker_transport) = duplex();
    let first_main_connection = first_main.connect_upstream(first_main_transport);
    let first_worker_connection = first_worker.accept_subscriber(first_worker_transport, alice);

    let write = first_main
        .insert(
            "todos",
            BTreeMap::from([(
                "title".to_owned(),
                Value::String("pending across worker restart".to_owned()),
            )]),
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
    let _second_main_connection = second_main.connect_upstream(second_main_transport);
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

#[cfg(feature = "rocksdb")]
#[test]
fn reopened_worker_routes_later_rejection_to_same_main_thread_identity() {
    let schema = schema();
    let alice = AuthorId::from_bytes([0xa9; 16]);
    let bob = AuthorId::from_bytes([0xb9; 16]);
    let storage = tempfile::tempdir().expect("worker temp dir");

    let first_main = open_db(0x1b, alice, &schema);
    first_main.set_non_durable_client();
    let first_worker = open_persistent_worker(storage.path(), 0x2a, &schema);
    let (first_main_transport, first_worker_transport) = duplex();
    let first_main_connection = first_main.connect_upstream(first_main_transport);
    let first_worker_connection = first_worker.accept_subscriber(first_worker_transport, alice);

    let write = first_main
        .insert(
            "todos",
            BTreeMap::from([(
                "title".to_owned(),
                Value::String("reject after worker restart".to_owned()),
            )]),
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

    let reopened_main = open_db(0x1b, alice, &schema);
    reopened_main.set_non_durable_client();
    let reopened_worker = open_persistent_worker(storage.path(), 0x2a, &schema);
    let core = open_core(0x3a, &schema);
    let (main_transport, worker_subscriber_transport) = duplex();
    let _main_connection = reopened_main.connect_upstream(main_transport);
    let _worker_subscriber = reopened_worker.accept_subscriber(worker_subscriber_transport, alice);
    let (worker_upstream_transport, core_transport) = duplex();
    let _worker_upstream = reopened_worker.connect_upstream(worker_upstream_transport);
    let _core_subscriber = core.accept_subscriber(core_transport, bob);

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
}
