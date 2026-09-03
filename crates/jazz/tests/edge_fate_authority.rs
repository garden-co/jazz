#![cfg(feature = "runtime")]

use std::cell::RefCell;
use std::collections::{BTreeMap, VecDeque};
use std::rc::Rc;

mod common;

use jazz_testkit::duplex_transport;

use jazz::db::{
    Db, DbConfig, DbIdentity, LocalUpdates, Propagation, ReadOpts, WireTransportAdapter, block_on,
};
use jazz::groove::records::Value;
use jazz::groove::storage::TestStorage;
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::Query;
use jazz::schema::JazzSchema;
use jazz::serving::{InMemoryServerShell, InMemoryServerShellConfig, NodeRole, ServerSession};
use jazz::tools::{ColumnType, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder};
use jazz::tx::DurabilityTier;
use jazz::wire::{TransportError, WireTransport};

use duplex_transport::duplex;

use common::compile_schema;

fn node(byte: u8) -> NodeUuid {
    NodeUuid::from_bytes([byte; 16])
}

fn author(byte: u8) -> AuthorSubject {
    AuthorSubject::for_test_bytes([byte; 16])
}

fn identity(node_byte: u8, author: AuthorSubject) -> DbIdentity {
    DbIdentity {
        node: node(node_byte),
        author,
    }
}

fn schema() -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("todos")
                    .column("title", ColumnType::Text)
                    .column("completed", ColumnType::Boolean),
            )
            .build(),
    )
}

fn read_only_schema() -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("todos")
                    .column("title", ColumnType::Text)
                    .column("completed", ColumnType::Boolean)
                    .policies(TablePolicies::new().with_select(PolicyExpr::True)),
            )
            .build(),
    )
}

fn write_only_schema() -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("todos")
                    .column("title", ColumnType::Text)
                    .column("completed", ColumnType::Boolean)
                    .policies(
                        TablePolicies::new()
                            .with_select(PolicyExpr::False)
                            .with_insert(PolicyExpr::True)
                            .with_update(Some(PolicyExpr::True), PolicyExpr::True),
                    ),
            )
            .build(),
    )
}

fn open_db(node_byte: u8, author: AuthorSubject, schema: &JazzSchema) -> Db<TestStorage> {
    let refs = schema.column_families();
    let refs = refs.iter().map(String::as_str).collect::<Vec<_>>();
    block_on(Db::open(DbConfig::new(
        schema.clone(),
        TestStorage::new(&refs),
        identity(node_byte, author),
    )))
    .unwrap()
}

fn open_core(node_byte: u8, schema: &JazzSchema) -> Db<TestStorage> {
    let refs = schema.column_families();
    let refs = refs.iter().map(String::as_str).collect::<Vec<_>>();
    block_on(Db::open_history_complete(DbConfig::new(
        schema.clone(),
        TestStorage::new(&refs),
        identity(node_byte, AuthorSubject::SYSTEM),
    )))
    .unwrap()
}

#[derive(Clone, Default)]
struct QueuedWireTransport {
    queues: Rc<RefCell<WireQueues>>,
}

#[derive(Default)]
struct WireQueues {
    inbound: VecDeque<Vec<u8>>,
    outbound: VecDeque<Vec<u8>>,
}

impl QueuedWireTransport {
    fn drain_outbound(&self) -> Vec<Vec<u8>> {
        self.queues.borrow_mut().outbound.drain(..).collect()
    }

    fn push_inbound(&self, frame: Vec<u8>) {
        self.queues.borrow_mut().inbound.push_back(frame);
    }
}

impl WireTransport for QueuedWireTransport {
    fn send_frame(&mut self, frame: Vec<u8>) -> Result<(), TransportError> {
        self.queues.borrow_mut().outbound.push_back(frame);
        Ok(())
    }

    fn try_recv_frame(&mut self) -> Option<Vec<u8>> {
        self.queues.borrow_mut().inbound.pop_front()
    }
}

fn connect_client_to_edge(
    edge: &mut InMemoryServerShell,
    client: &Db<TestStorage>,
    client_wire: &QueuedWireTransport,
    identity: AuthorSubject,
) -> ServerSession {
    jazz::db::block_on(
        client.connect_upstream(Box::new(WireTransportAdapter::current(client_wire.clone()))),
    );
    edge.accept_subscriber_session(identity).unwrap()
}

fn pump_client_edge(
    client: &Db<TestStorage>,
    wire: &QueuedWireTransport,
    edge: &mut InMemoryServerShell,
    session: ServerSession,
) {
    block_on(client.tick()).unwrap();
    edge.receive_frames(session, wire.drain_outbound()).unwrap();
    edge.tick().unwrap();
    for frame in edge.take_frames(session).unwrap() {
        wire.push_inbound(frame);
    }
    block_on(client.tick()).unwrap();
}

fn visible_titles(db: &Db<TestStorage>, tier: DurabilityTier) -> Vec<String> {
    let query = Query::from("todos");
    let prepared = db.prepare_query(&query).unwrap();
    block_on(db.all(
        &prepared,
        ReadOpts {
            tier,
            local_updates: LocalUpdates::Deferred,
            propagation: Propagation::Full,
            ..ReadOpts::default()
        },
    ))
    .unwrap()
    .into_iter()
    .map(|row| {
        let Some(Value::String(title)) = row.cell(&schema().tables[0], "title") else {
            panic!("expected title");
        };
        title
    })
    .collect()
}

#[test]
fn edge_shell_does_not_report_global_or_serve_global_before_core_ack() {
    let schema = schema();
    let mut edge = InMemoryServerShell::start(
        InMemoryServerShellConfig::new(schema.clone(), identity(0xe0, AuthorSubject::SYSTEM))
            .with_role(NodeRole::Edge),
    )
    .unwrap();
    let core = open_core(0xc0, &schema);
    let (edge_to_core, core_to_edge) = duplex();
    edge.connect_upstream(edge_to_core).unwrap();
    core.accept_subscriber(core_to_edge, AuthorSubject::SYSTEM);

    let alice = open_db(0xa1, author(0xa1), &schema);
    let bob = open_db(0xb0, author(0xb0), &schema);
    let alice_wire = QueuedWireTransport::default();
    let bob_wire = QueuedWireTransport::default();
    let alice_session = connect_client_to_edge(&mut edge, &alice, &alice_wire, author(0xa1));
    let bob_session = connect_client_to_edge(&mut edge, &bob, &bob_wire, author(0xb0));
    let query = Query::from("todos");
    let prepared = bob.prepare_query(&query).unwrap();
    let mut bob_global_subscription = block_on(bob.subscribe(
        &prepared,
        ReadOpts {
            tier: DurabilityTier::Global,
            local_updates: LocalUpdates::Deferred,
            propagation: Propagation::Full,
            ..ReadOpts::default()
        },
    ))
    .unwrap();
    pump_client_edge(&bob, &bob_wire, &mut edge, bob_session);
    while bob_global_subscription.try_next_event().is_some() {}

    let write = block_on(alice.insert(
        "todos",
        BTreeMap::from([("title".to_owned(), Value::String("edge only".to_owned()))]),
        Default::default(),
    ))
    .unwrap();
    pump_client_edge(&alice, &alice_wire, &mut edge, alice_session);
    pump_client_edge(&bob, &bob_wire, &mut edge, bob_session);

    assert!(block_on(write.wait(DurabilityTier::Edge)).is_ok());
    assert!(block_on(write.wait(DurabilityTier::Global)).is_err());
    assert!(bob_global_subscription.try_next_event().is_none());
    assert!(visible_titles(&bob, DurabilityTier::Global).is_empty());

    let _ = core;
}

#[test]
fn core_shell_client_upload_still_reports_global_immediately() {
    let schema = schema();
    let mut core = InMemoryServerShell::start(
        InMemoryServerShellConfig::new(schema.clone(), identity(0xc0, AuthorSubject::SYSTEM))
            .with_role(NodeRole::Core),
    )
    .unwrap();

    let alice = open_db(0xa1, author(0xa1), &schema);
    let bob = open_db(0xb1, author(0xb1), &schema);
    let alice_wire = QueuedWireTransport::default();
    let bob_wire = QueuedWireTransport::default();
    let alice_session = connect_client_to_edge(&mut core, &alice, &alice_wire, author(0xa1));
    let bob_session = connect_client_to_edge(&mut core, &bob, &bob_wire, author(0xb1));

    // Bob's Global read consumes the identity-scoped settled view emitted by
    // the authority, rather than Alice's locally uploaded payload. Establish
    // that authoritative Global view before Alice writes so the test covers
    // the core's FateUpdate and its downstream maintained-view publication.
    let prepared = bob.prepare_query(&Query::from("todos")).unwrap();
    let mut bob_global_subscription = block_on(bob.subscribe(
        &prepared,
        ReadOpts {
            tier: DurabilityTier::Global,
            local_updates: LocalUpdates::Deferred,
            propagation: Propagation::Full,
            ..ReadOpts::default()
        },
    ))
    .unwrap();
    pump_client_edge(&bob, &bob_wire, &mut core, bob_session);
    let Some(jazz::db::SubscriptionEvent::Delta {
        reset: true,
        publishable: true,
        settled: true,
        tier: DurabilityTier::Global,
        added,
        updated,
        removed,
        ..
    }) = bob_global_subscription.try_next_event()
    else {
        panic!("Bob must receive an authoritative settled Global hydration before upload");
    };
    assert!(added.is_empty());
    assert!(updated.is_empty());
    assert!(removed.is_empty());

    let write = block_on(alice.insert(
        "todos",
        BTreeMap::from([("title".to_owned(), Value::String("core global".to_owned()))]),
        Default::default(),
    ))
    .unwrap();
    pump_client_edge(&alice, &alice_wire, &mut core, alice_session);
    pump_client_edge(&bob, &bob_wire, &mut core, bob_session);

    assert!(block_on(write.wait(DurabilityTier::Global)).is_ok());
    let Some(jazz::db::SubscriptionEvent::Delta {
        reset: false,
        publishable: true,
        settled: true,
        tier: DurabilityTier::Global,
        added,
        updated,
        removed,
        ..
    }) = bob_global_subscription.try_next_event()
    else {
        panic!("Alice's globally settled write must publish one Global delta to Bob");
    };
    assert!(updated.is_empty());
    assert!(removed.is_empty());
    assert_eq!(added.len(), 1);
    assert_eq!(added[0].row_uuid(), write.row_uuid());
    assert!(matches!(
        added[0].cell(&schema.tables[0], "title"),
        Some(Value::String(title)) if title == "core global"
    ));
    assert_eq!(
        visible_titles(&bob, DurabilityTier::Global),
        ["core global"]
    );
}

/// The client may optimistically stage this write locally, but the served
/// authority must reject it after a read-only policy closes the table's
/// omitted write operations.  This is intentionally a real client -> core
/// session receipt, rather than an in-memory fixture filter.
#[test]
fn core_authority_rejects_omitted_insert_after_read_policy_closes_table() {
    let schema = read_only_schema();
    let mut core = InMemoryServerShell::start(
        InMemoryServerShellConfig::new(schema.clone(), identity(0xc2, AuthorSubject::SYSTEM))
            .with_role(NodeRole::Core),
    )
    .unwrap();
    let alice = open_db(0xa3, author(0xa3), &schema);
    let wire = QueuedWireTransport::default();
    let session = connect_client_to_edge(&mut core, &alice, &wire, author(0xa3));

    let write = block_on(alice.insert(
        "todos",
        BTreeMap::from([
            ("title".to_owned(), Value::String("forged write".to_owned())),
            ("completed".to_owned(), Value::Bool(false)),
        ]),
        Default::default(),
    ))
    .unwrap();
    pump_client_edge(&alice, &wire, &mut core, session);

    assert!(block_on(write.wait(DurabilityTier::Global)).is_err());
}

/// A writer may retain a local preimage despite losing read access, so its
/// mergeable update and upsert stage optimistically. The core alone decides
/// read-for-write admission, rejects both writes, restores the accepted row,
/// and exposes neither the target nor its contents through the writer view.
///
/// alice ──seed──► core ──accepted──► alice
/// alice ──update/upsert(hidden row)──► core ──rejected──► alice rollback
///
/// Planted positive: temporarily removing the authority's mergeable
/// read-for-write check makes either `wait(Global)` succeed and the SYSTEM
/// inspection observe the forged title.
#[test]
fn core_authority_rejects_write_only_update_and_upsert_and_rolls_back() {
    let schema = write_only_schema();
    let mut core = InMemoryServerShell::start(
        InMemoryServerShellConfig::new(schema.clone(), identity(0xc3, AuthorSubject::SYSTEM))
            .with_role(NodeRole::Core),
    )
    .unwrap();
    let alice = open_db(0xa4, author(0xa4), &schema);
    let wire = QueuedWireTransport::default();
    let session = connect_client_to_edge(&mut core, &alice, &wire, author(0xa4));

    let seed = block_on(alice.insert(
        "todos",
        BTreeMap::from([
            (
                "title".to_owned(),
                Value::String("accepted base".to_owned()),
            ),
            ("completed".to_owned(), Value::Bool(false)),
        ]),
        Default::default(),
    ))
    .unwrap();
    let target = seed.row_uuid();
    pump_client_edge(&alice, &wire, &mut core, session);
    assert!(block_on(seed.wait(DurabilityTier::Global)).is_ok());

    let prepared = alice.prepare_query(&Query::from("todos")).unwrap();
    let rows_for = |identity| {
        block_on(alice.all_for_identity(&prepared, ReadOpts::default(), identity))
            .unwrap()
            .into_iter()
            .map(|row| row.cell(&schema.tables[0], "title").unwrap())
            .collect::<Vec<_>>()
    };
    assert!(
        rows_for(author(0xa4)).is_empty(),
        "writer must not learn target"
    );
    assert_eq!(
        rows_for(AuthorSubject::SYSTEM),
        vec![Value::String("accepted base".to_owned())]
    );
    let target_debug = format!("{target:?}");

    for (operation, write) in [
        (
            "UPDATE",
            block_on(alice.update(
                "todos",
                target,
                BTreeMap::from([(
                    "title".to_owned(),
                    Value::String("forged update".to_owned()),
                )]),
                Default::default(),
            ))
            .expect("client stages hidden-row update optimistically"),
        ),
        (
            "UPSERT",
            block_on(alice.upsert(
                "todos",
                target,
                BTreeMap::from([(
                    "title".to_owned(),
                    Value::String("forged upsert".to_owned()),
                )]),
                Default::default(),
            ))
            .expect("client stages hidden-row upsert optimistically"),
        ),
    ] {
        assert!(block_on(write.wait(DurabilityTier::Local)).is_ok());
        pump_client_edge(&alice, &wire, &mut core, session);
        let error = block_on(write.wait(DurabilityTier::Global))
            .expect_err("authority must reject write-only {operation}");
        assert_eq!(error.code, jazz::db::ErrorCode::WriteRejected);
        assert!(
            !error.message.contains("accepted base")
                && !error.message.contains("forged")
                && !error.message.contains(&target_debug),
            "rejection must not disclose target details: {error:?}"
        );
        assert!(
            rows_for(author(0xa4)).is_empty(),
            "writer must remain blind after {operation}"
        );
        assert_eq!(
            rows_for(AuthorSubject::SYSTEM),
            vec![Value::String("accepted base".to_owned())],
            "rejected {operation} must roll back the optimistic row"
        );
    }
}

/// Black-box regression for authored-column carriage across the public Db and
/// sync/wire path. Bob explicitly writes the unchanged base title at the newer
/// timestamp; that authored write must participate in per-column LWW and beat
/// Alice's older concurrent title change after both commits cross the wire,
/// without claiming Alice's independent `completed` edit.
///
/// Planted positive: removing `MergeableCommit::authored_columns` from the
/// partial-update lowering makes Bob's entire materialized row look authored;
/// Bob still wins `title`, but incorrectly reverts `completed` to false.
#[test]
fn explicit_unchanged_partial_write_survives_sync_and_wins_lww() {
    let schema = schema();
    let mut core = InMemoryServerShell::start(
        InMemoryServerShellConfig::new(schema.clone(), identity(0xc1, AuthorSubject::SYSTEM))
            .with_role(NodeRole::Core),
    )
    .unwrap();
    let alice = open_db(0xa2, author(0xa2), &schema);
    let bob = open_db(0xb2, author(0xb2), &schema);
    let alice_wire = QueuedWireTransport::default();
    let bob_wire = QueuedWireTransport::default();
    let alice_session = connect_client_to_edge(&mut core, &alice, &alice_wire, author(0xa2));
    let bob_session = connect_client_to_edge(&mut core, &bob, &bob_wire, author(0xb2));

    // Keep every transaction identity distinct and the LWW order explicit:
    // TxId includes each client's already-distinct node id plus this HLC time.
    let row = RowUuid::from_bytes([0xd2; 16]);
    block_on(alice.insert(
        "todos",
        BTreeMap::from([
            ("title".to_owned(), Value::String("base".to_owned())),
            ("completed".to_owned(), Value::Bool(false)),
        ]),
        jazz::db::InsertOptions {
            row_id: Some(row),
            updated_at_ms: Some(100),
            ..Default::default()
        },
    ))
    .unwrap();
    pump_client_edge(&alice, &alice_wire, &mut core, alice_session);

    let prepared = bob.prepare_query(&Query::from("todos")).unwrap();
    let _subscription = block_on(bob.subscribe(&prepared, ReadOpts::default())).unwrap();
    let alice_prepared = alice.prepare_query(&Query::from("todos")).unwrap();
    let _alice_subscription =
        block_on(alice.subscribe(&alice_prepared, ReadOpts::default())).unwrap();
    pump_client_edge(&bob, &bob_wire, &mut core, bob_session);
    pump_client_edge(&alice, &alice_wire, &mut core, alice_session);

    // Neither client is pumped after these writes until both heads exist, so
    // they remain concurrent children of the shared t=100 base.
    block_on(alice.update(
        "todos",
        row,
        BTreeMap::from([
            ("title".to_owned(), Value::String("alice-change".to_owned())),
            ("completed".to_owned(), Value::Bool(true)),
        ]),
        jazz::db::UpdateOptions {
            updated_at_ms: Some(200),
            ..Default::default()
        },
    ))
    .unwrap();
    let explicit_write = block_on(bob.update(
        "todos",
        row,
        BTreeMap::from([("title".to_owned(), Value::String("base".to_owned()))]),
        jazz::db::UpdateOptions {
            updated_at_ms: Some(300),
            ..Default::default()
        },
    ))
    .unwrap();
    // An empty partial update is not a content mutation. It reuses the current
    // write identity instead of emitting a newer legacy "all materialized cells
    // authored" version that could clobber Alice's cells during reconciliation.
    let no_op = block_on(bob.update(
        "todos",
        row,
        BTreeMap::new(),
        jazz::db::UpdateOptions {
            updated_at_ms: Some(400),
            ..Default::default()
        },
    ))
    .expect("empty patch remains a safe no-op");
    assert_eq!(no_op.mergeable_tx_id(), explicit_write.mergeable_tx_id());

    pump_client_edge(&alice, &alice_wire, &mut core, alice_session);
    pump_client_edge(&bob, &bob_wire, &mut core, bob_session);
    pump_client_edge(&alice, &alice_wire, &mut core, alice_session);
    assert_eq!(visible_titles(&alice, DurabilityTier::Global), ["base"]);
    let prepared = alice.prepare_query(&Query::from("todos")).unwrap();
    let rows = block_on(alice.all(
        &prepared,
        ReadOpts {
            tier: DurabilityTier::Global,
            local_updates: LocalUpdates::Deferred,
            propagation: Propagation::Full,
            ..ReadOpts::default()
        },
    ))
    .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].cell(&schema.tables[0], "completed"),
        Some(Value::Bool(true))
    );
}
