#![cfg(feature = "runtime")]

use std::cell::RefCell;
use std::collections::{BTreeMap, VecDeque};
use std::rc::Rc;

mod common;

use jazz::db::{
    Db, DbConfig, DbIdentity, LocalUpdates, Propagation, ReadOpts, WireTransportAdapter, block_on,
};
use jazz::groove::records::Value;
use jazz::groove::storage::TestStorage;
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::protocol::{LensOp, MigrationLens, SchemaVersion, TableLens};
use jazz::query::Query;
use jazz::schema::JazzSchema;
use jazz::serving::{InMemoryServerShell, InMemoryServerShellConfig, NodeRole, ServerSession};
use jazz::tools::{ColumnType, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder};
use jazz::tx::DurabilityTier;
use jazz::wire::{TransportError, WireTransport};

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
    use jazz::tools::test_support::AllowAll;
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("todos")
                    .column("title", ColumnType::Text)
                    .column("completed", ColumnType::Boolean),
            )
            .allow_all()
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

fn open_db(node_byte: u8, author: AuthorSubject, schema: &JazzSchema) -> Db {
    let refs = schema.column_families();
    let refs = refs.iter().map(String::as_str).collect::<Vec<_>>();
    block_on(Db::open(DbConfig::new(
        schema.clone(),
        TestStorage::new(&refs),
        identity(node_byte, author),
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

fn connect_client_to_core(
    core: &mut InMemoryServerShell,
    client: &Db,
    client_wire: &QueuedWireTransport,
    identity: AuthorSubject,
) -> ServerSession {
    jazz::db::block_on(
        client.connect_upstream(Box::new(WireTransportAdapter::current(client_wire.clone()))),
    );
    core.accept_subscriber_session(identity).unwrap()
}

fn pump_client_core(
    client: &Db,
    wire: &QueuedWireTransport,
    core: &mut InMemoryServerShell,
    session: ServerSession,
) {
    block_on(client.tick()).unwrap();
    core.receive_frames(session, wire.drain_outbound()).unwrap();
    core.tick().unwrap();
    for frame in core.take_frames(session).unwrap() {
        wire.push_inbound(frame);
    }
    block_on(client.tick()).unwrap();
}

fn visible_titles(db: &Db, tier: DurabilityTier) -> Vec<String> {
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

/// Alice's complete write reaches Global durability and Bob's maintained view.
/// Bob subscribes → core settles opening → Alice uploads → core publishes to Bob.
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
    let alice_session = connect_client_to_core(&mut core, &alice, &alice_wire, author(0xa1));
    let bob_session = connect_client_to_core(&mut core, &bob, &bob_wire, author(0xb1));

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
    pump_client_core(&bob, &bob_wire, &mut core, bob_session);
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
        BTreeMap::from([
            ("title".to_owned(), Value::String("core global".to_owned())),
            ("completed".to_owned(), Value::Bool(false)),
        ]),
        Default::default(),
    ))
    .unwrap();
    pump_client_core(&alice, &alice_wire, &mut core, alice_session);
    pump_client_core(&bob, &bob_wire, &mut core, bob_session);

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
    let session = connect_client_to_core(&mut core, &alice, &wire, author(0xa3));

    let write = block_on(alice.insert(
        "todos",
        BTreeMap::from([
            ("title".to_owned(), Value::String("forged write".to_owned())),
            ("completed".to_owned(), Value::Bool(false)),
        ]),
        Default::default(),
    ))
    .unwrap();
    pump_client_core(&alice, &wire, &mut core, session);

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
    let session = connect_client_to_core(&mut core, &alice, &wire, author(0xa4));

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
    pump_client_core(&alice, &wire, &mut core, session);
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
        pump_client_core(&alice, &wire, &mut core, session);
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
/// sync/wire path. Bob explicitly writes the unchanged base title over the
/// base image, concurrently with Alice changing both cells. Core sequences
/// Alice's write first, so Bob's authored title applies after hers and wins
/// by arrival (SPEC 4 §4.6), while his write must not claim Alice's
/// independent `completed` edit.
///
/// Planted positive: removing `MergeableCommit::authored_columns` from the
/// partial-update lowering makes Bob's entire materialized row look authored;
/// Bob still wins `title`, but incorrectly reverts `completed` to false.
#[test]
fn explicit_unchanged_partial_write_survives_sync_and_wins_by_arrival() {
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
    let alice_session = connect_client_to_core(&mut core, &alice, &alice_wire, author(0xa2));
    let bob_session = connect_client_to_core(&mut core, &bob, &bob_wire, author(0xb2));

    // Keep every transaction identity distinct: TxId includes each client's
    // already-distinct node id plus this HLC time.
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
    pump_client_core(&alice, &alice_wire, &mut core, alice_session);

    let prepared = bob.prepare_query(&Query::from("todos")).unwrap();
    let _subscription = block_on(bob.subscribe(&prepared, ReadOpts::default())).unwrap();
    let alice_prepared = alice.prepare_query(&Query::from("todos")).unwrap();
    let _alice_subscription =
        block_on(alice.subscribe(&alice_prepared, ReadOpts::default())).unwrap();
    pump_client_core(&bob, &bob_wire, &mut core, bob_session);
    pump_client_core(&alice, &alice_wire, &mut core, alice_session);

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

    pump_client_core(&alice, &alice_wire, &mut core, alice_session);
    pump_client_core(&bob, &bob_wire, &mut core, bob_session);
    pump_client_core(&alice, &alice_wire, &mut core, alice_session);
    pump_client_core(&bob, &bob_wire, &mut core, bob_session);
    assert!(
        matches!(
            bob.write_state(explicit_write.mergeable_tx_id()).unwrap(),
            jazz::db::WriteState {
                fate: jazz::tx::Fate::Accepted,
                durability: DurabilityTier::Global,
                ..
            }
        ),
        "Bob's write is accepted"
    );
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

fn merge_columns_schema() -> JazzSchema {
    use jazz::tools::test_support::AllowAll;
    use jazz::tools::{ColumnMergeStrategy, RowDescriptor, TableName};
    let mut schema = SchemaBuilder::new()
        .table(
            TableSchemaBuilder::new("docs")
                .column(
                    "tags",
                    ColumnType::Array {
                        element: Box::new(ColumnType::Text),
                    },
                )
                .column("count", ColumnType::Integer),
        )
        .allow_all()
        .build();
    let table = schema
        .get_mut(&TableName::new("docs"))
        .expect("docs table exists");
    table.columns = RowDescriptor::new(
        table
            .columns
            .columns
            .iter()
            .map(|column| match column.name.as_str() {
                "tags" => column.clone().merge_strategy(ColumnMergeStrategy::GSet),
                "count" => column.clone().merge_strategy(ColumnMergeStrategy::Counter),
                _ => column.clone(),
            })
            .collect(),
    );
    compile_schema(&schema)
}

fn tags(values: &[&str]) -> Value {
    Value::Array(
        values
            .iter()
            .map(|value| Value::String((*value).to_owned()))
            .collect(),
    )
}

/// Concurrent writes to merge columns compose at Core: a counter write adds
/// its delta over the image it saw, and a set write adds its new elements.
#[test]
fn concurrent_merge_column_writes_compose_at_core() {
    let schema = merge_columns_schema();
    let mut core = InMemoryServerShell::start(
        InMemoryServerShellConfig::new(schema.clone(), identity(0xc3, AuthorSubject::SYSTEM))
            .with_role(NodeRole::Core),
    )
    .unwrap();
    let alice = open_db(0xa3, author(0xa3), &schema);
    let bob = open_db(0xb3, author(0xb3), &schema);
    let alice_wire = QueuedWireTransport::default();
    let bob_wire = QueuedWireTransport::default();
    let alice_session = connect_client_to_core(&mut core, &alice, &alice_wire, author(0xa3));
    let bob_session = connect_client_to_core(&mut core, &bob, &bob_wire, author(0xb3));

    let row = RowUuid::from_bytes([0xd3; 16]);
    block_on(alice.insert(
        "docs",
        BTreeMap::from([
            ("tags".to_owned(), tags(&["seed"])),
            ("count".to_owned(), Value::I32(1)),
        ]),
        jazz::db::InsertOptions {
            row_id: Some(row),
            updated_at_ms: Some(100),
            ..Default::default()
        },
    ))
    .unwrap();
    pump_client_core(&alice, &alice_wire, &mut core, alice_session);
    for db in [&alice, &bob] {
        let prepared = db.prepare_query(&Query::from("docs")).unwrap();
        std::mem::forget(block_on(db.subscribe(&prepared, ReadOpts::default())).unwrap());
    }
    pump_client_core(&bob, &bob_wire, &mut core, bob_session);
    pump_client_core(&alice, &alice_wire, &mut core, alice_session);

    // Both writes are made over the same base before either reaches Core.
    for (db, tag, count, at) in [(&alice, "alice", 4, 200), (&bob, "bob", 6, 300)] {
        block_on(db.update(
            "docs",
            row,
            BTreeMap::from([
                ("tags".to_owned(), tags(&["seed", tag])),
                ("count".to_owned(), Value::I32(count)),
            ]),
            jazz::db::UpdateOptions {
                updated_at_ms: Some(at),
                ..Default::default()
            },
        ))
        .unwrap();
    }
    for _ in 0..2 {
        pump_client_core(&alice, &alice_wire, &mut core, alice_session);
        pump_client_core(&bob, &bob_wire, &mut core, bob_session);
    }

    for db in [&alice, &bob] {
        let prepared = db.prepare_query(&Query::from("docs")).unwrap();
        let rows = block_on(db.all(
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
            rows[0].cell(&schema.tables[0], "tags"),
            Some(tags(&["alice", "bob", "seed"]))
        );
        // 1 + (4 - 1) + (6 - 1)
        assert_eq!(
            rows[0].cell(&schema.tables[0], "count"),
            Some(Value::I32(9))
        );
    }
}

#[test]
fn synced_merge_column_change_rebases_pending_local_patch() {
    let schema = merge_columns_schema();
    let mut core = InMemoryServerShell::start(
        InMemoryServerShellConfig::new(schema.clone(), identity(0xc4, AuthorSubject::SYSTEM))
            .with_role(NodeRole::Core),
    )
    .unwrap();
    let alice = open_db(0xa4, author(0xa4), &schema);
    let bob = open_db(0xb4, author(0xb4), &schema);
    let alice_wire = QueuedWireTransport::default();
    let bob_wire = QueuedWireTransport::default();
    let alice_session = connect_client_to_core(&mut core, &alice, &alice_wire, author(0xa4));
    let bob_session = connect_client_to_core(&mut core, &bob, &bob_wire, author(0xb4));

    let row = RowUuid::from_bytes([0xd4; 16]);
    block_on(alice.insert(
        "docs",
        BTreeMap::from([
            ("tags".to_owned(), tags(&["seed"])),
            ("count".to_owned(), Value::I32(1)),
        ]),
        jazz::db::InsertOptions {
            row_id: Some(row),
            updated_at_ms: Some(100),
            ..Default::default()
        },
    ))
    .unwrap();
    pump_client_core(&alice, &alice_wire, &mut core, alice_session);
    for db in [&alice, &bob] {
        let prepared = db.prepare_query(&Query::from("docs")).unwrap();
        std::mem::forget(block_on(db.subscribe(&prepared, ReadOpts::default())).unwrap());
    }
    pump_client_core(&bob, &bob_wire, &mut core, bob_session);
    pump_client_core(&alice, &alice_wire, &mut core, alice_session);

    // Alice's write stays pending: its commit never leaves her outbox.
    block_on(alice.update(
        "docs",
        row,
        BTreeMap::from([
            ("tags".to_owned(), tags(&["seed", "alice"])),
            ("count".to_owned(), Value::I32(4)),
        ]),
        jazz::db::UpdateOptions {
            updated_at_ms: Some(200),
            ..Default::default()
        },
    ))
    .unwrap();
    block_on(alice.tick()).unwrap();
    let _held = alice_wire.drain_outbound();

    // Bob's write is accepted and reaches Alice as a synced row.
    block_on(bob.update(
        "docs",
        row,
        BTreeMap::from([
            ("tags".to_owned(), tags(&["seed", "bob"])),
            ("count".to_owned(), Value::I32(6)),
        ]),
        jazz::db::UpdateOptions {
            updated_at_ms: Some(300),
            ..Default::default()
        },
    ))
    .unwrap();
    pump_client_core(&bob, &bob_wire, &mut core, bob_session);
    core.tick().unwrap();
    for frame in core.take_frames(alice_session).unwrap() {
        alice_wire.push_inbound(frame);
    }
    block_on(alice.tick()).unwrap();
    let _held = alice_wire.drain_outbound();

    let prepared = alice.prepare_query(&Query::from("docs")).unwrap();
    let rows = block_on(alice.all(&prepared, ReadOpts::default())).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].cell(&schema.tables[0], "tags"),
        Some(tags(&["alice", "bob", "seed"]))
    );
    // Bob's synced 6, plus Alice's pending +3.
    assert_eq!(
        rows[0].cell(&schema.tables[0], "count"),
        Some(Value::I32(9))
    );
}

/// A core that has seen many writer nodes still resolves every stored
/// version's writer correctly: each writer's own row keeps its author, and a
/// row all writers edited converges to the same last-writer-wins head on the
/// core and on a Global reader.
///
/// ```text
/// writer_0..writer_N ──insert own row, update shared──► core ──Global──► bob
///                                                          │
///                                  compact alias → node ───┘ (per version)
/// ```
///
/// Every version carries its writer as a compact node alias that the core
/// maps back to the writer's node. With many writers, a wrong or missing
/// mapping would surface as a wrong author, a failed Global wait, or a
/// divergent merged head.
#[test]
fn many_writer_nodes_resolve_authors_and_merge_heads_at_the_core() {
    const WRITERS: u8 = 24;
    let schema = schema();
    let mut core = InMemoryServerShell::start(
        InMemoryServerShellConfig::new(schema.clone(), identity(0xc0, AuthorSubject::SYSTEM))
            .with_role(NodeRole::Core),
    )
    .unwrap();
    let bob = open_db(0xb1, author(0xb1), &schema);
    let bob_wire = QueuedWireTransport::default();
    let bob_session = connect_client_to_core(&mut core, &bob, &bob_wire, author(0xb1));
    let prepared = bob.prepare_query(&Query::from("todos")).unwrap();
    let _bob_subscription = block_on(bob.subscribe(
        &prepared,
        ReadOpts {
            tier: DurabilityTier::Global,
            ..ReadOpts::default()
        },
    ))
    .unwrap();
    pump_client_core(&bob, &bob_wire, &mut core, bob_session);

    let shared_row = RowUuid::from_bytes([0x5e; 16]);
    let mut writers = Vec::new();
    for index in 0..WRITERS {
        let byte = 0x10 + index;
        let db = open_db(byte, author(byte), &schema);
        let wire = QueuedWireTransport::default();
        let session = connect_client_to_core(&mut core, &db, &wire, author(byte));
        // Writers must read the shared row before they may edit it.
        let prepared = db.prepare_query(&Query::from("todos")).unwrap();
        let subscription = block_on(db.subscribe(
            &prepared,
            ReadOpts {
                tier: DurabilityTier::Global,
                ..ReadOpts::default()
            },
        ))
        .unwrap();
        writers.push((byte, db, wire, session, subscription));
    }

    let (_, seeder, seeder_wire, seeder_session, _) = &writers[0];
    let seed = block_on(seeder.insert(
        "todos",
        BTreeMap::from([
            ("title".to_owned(), Value::String("shared seed".to_owned())),
            ("completed".to_owned(), Value::Bool(false)),
        ]),
        jazz::db::InsertOptions {
            row_id: Some(shared_row),
            updated_at_ms: Some(1_000),
            ..Default::default()
        },
    ))
    .unwrap();
    pump_client_core(seeder, seeder_wire, &mut core, *seeder_session);
    assert!(block_on(seed.wait(DurabilityTier::Global)).is_ok());

    let mut own_rows = Vec::new();
    for (byte, db, wire, session, _) in &writers {
        // Every writer sees the shared row's current head before editing it,
        // so each edit descends from the previous one and the last one wins.
        pump_client_core(db, wire, &mut core, *session);
        pump_client_core(db, wire, &mut core, *session);
        let own = block_on(db.insert(
            "todos",
            BTreeMap::from([
                ("title".to_owned(), Value::String(format!("own {byte:02x}"))),
                ("completed".to_owned(), Value::Bool(false)),
            ]),
            Default::default(),
        ))
        .unwrap();
        block_on(db.update(
            "todos",
            shared_row,
            BTreeMap::from([(
                "title".to_owned(),
                Value::String(format!("shared by {byte:02x}")),
            )]),
            jazz::db::UpdateOptions {
                updated_at_ms: Some(2_000 + u64::from(*byte)),
                ..Default::default()
            },
        ))
        .unwrap();
        pump_client_core(db, wire, &mut core, *session);
        assert!(block_on(own.wait(DurabilityTier::Global)).is_ok());
        own_rows.push((own.row_uuid(), author(*byte), format!("own {byte:02x}")));
    }

    pump_client_core(&bob, &bob_wire, &mut core, bob_session);
    pump_client_core(&bob, &bob_wire, &mut core, bob_session);
    let rows = block_on(bob.all(
        &prepared,
        ReadOpts {
            tier: DurabilityTier::Global,
            local_updates: LocalUpdates::Deferred,
            propagation: Propagation::Full,
            ..ReadOpts::default()
        },
    ))
    .unwrap();
    assert_eq!(rows.len(), usize::from(WRITERS) + 1);
    for (row_uuid, writer, title) in &own_rows {
        let row = rows
            .iter()
            .find(|row| row.row_uuid() == *row_uuid)
            .expect("every writer's row reaches Bob");
        assert!(matches!(
            row.cell(&schema.tables[0], "title"),
            Some(Value::String(seen)) if seen == *title
        ));
        let provenance = bob.row_provenance(row).unwrap().unwrap();
        assert_eq!(provenance.created_by, *writer);
        assert_eq!(provenance.updated_by, *writer);
    }

    let last = 0x10 + WRITERS - 1;
    let shared = rows
        .iter()
        .find(|row| row.row_uuid() == shared_row)
        .expect("shared row reaches Bob");
    assert!(matches!(
        shared.cell(&schema.tables[0], "title"),
        Some(Value::String(seen)) if seen == format!("shared by {last:02x}")
    ));
    let provenance = bob.row_provenance(shared).unwrap().unwrap();
    assert_eq!(provenance.created_by, author(0x10));
    assert_eq!(provenance.updated_by, author(last));

    // The writers converge on the same head through the core.
    for (_, db, wire, session, _) in &writers {
        pump_client_core(db, wire, &mut core, *session);
        pump_client_core(db, wire, &mut core, *session);
    }
    for (_, db, _, _, _) in &writers {
        let titles = visible_titles(db, DurabilityTier::Global);
        assert!(
            titles.contains(&format!("shared by {last:02x}")),
            "{titles:?}"
        );
    }
}

/// `schema()` plus one added column, published by the core as a descendant.
fn schema_with_notes() -> JazzSchema {
    use jazz::tools::test_support::AllowAll;
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("todos")
                    .column("title", ColumnType::Text)
                    .column("completed", ColumnType::Boolean)
                    .column("notes", ColumnType::Text),
            )
            .allow_all()
            .build(),
    )
}

/// A client still writing an older schema, after the core published a
/// descendant that adds a column, keeps a synced change underneath its own
/// pending edits: a second pending edit of the same row builds on the row it
/// can see, not on the first edit's stale snapshot.
///
/// Actors: alice (old-schema client, edits held offline), bob (old-schema
/// client, edits synced), core (publishes `notes` as a descendant schema).
///
/// ```text
/// core ──publish v2 (+notes)──► alice, bob     (both keep writing v1)
/// alice ──insert title=seed──► core
/// alice ──completed=true──╳ (held: pending)
/// bob ──title=bob──► core ──synced──► alice     alice sees bob + pending
/// alice ──completed=false──╳ (held: pending)   alice must still see bob
/// ```
///
/// Once the physical table holds both schema layouts, a v1 row's stored
/// layout is narrower than the table's widest layout. Reading alice's local
/// row then went through a history lookup keyed by the newest pending write,
/// which returns that write as it was committed, before bob's synced title
/// was rebased under it; the second edit carried that stale title forward.
#[test]
fn pending_edit_after_synced_rebase_keeps_synced_cells_across_added_column_lineage() {
    let schema = schema();
    let mut core = InMemoryServerShell::start(
        InMemoryServerShellConfig::new(schema.clone(), identity(0xc5, AuthorSubject::SYSTEM))
            .with_role(NodeRole::Core),
    )
    .unwrap();
    let lens = MigrationLens::new(
        schema.version_id(),
        SchemaVersion::new(schema_with_notes()).id,
        vec![TableLens {
            source_table: "todos".to_owned(),
            target_table: "todos".to_owned(),
            ops: vec![LensOp::AddColumn {
                column: "notes".to_owned(),
                default: Value::String(String::new()),
            }],
        }],
    )
    .unwrap();
    core.publish_runtime_schema_with_lens(schema_with_notes(), lens, Vec::new(), Vec::new())
        .unwrap();

    let alice = open_db(0xa5, author(0xa5), &schema);
    let bob = open_db(0xb5, author(0xb5), &schema);
    let alice_wire = QueuedWireTransport::default();
    let bob_wire = QueuedWireTransport::default();
    let alice_session = connect_client_to_core(&mut core, &alice, &alice_wire, author(0xa5));
    let bob_session = connect_client_to_core(&mut core, &bob, &bob_wire, author(0xb5));

    let row = RowUuid::from_bytes([0xd5; 16]);
    block_on(alice.insert(
        "todos",
        BTreeMap::from([
            ("title".to_owned(), Value::String("seed".to_owned())),
            ("completed".to_owned(), Value::Bool(false)),
        ]),
        jazz::db::InsertOptions {
            row_id: Some(row),
            updated_at_ms: Some(100),
            ..Default::default()
        },
    ))
    .unwrap();
    pump_client_core(&alice, &alice_wire, &mut core, alice_session);
    for db in [&alice, &bob] {
        let prepared = db.prepare_query(&Query::from("todos")).unwrap();
        std::mem::forget(block_on(db.subscribe(&prepared, ReadOpts::default())).unwrap());
    }
    pump_client_core(&bob, &bob_wire, &mut core, bob_session);
    pump_client_core(&alice, &alice_wire, &mut core, alice_session);

    // Alice's first edit stays pending: its commit never leaves her outbox.
    block_on(alice.update(
        "todos",
        row,
        BTreeMap::from([("completed".to_owned(), Value::Bool(true))]),
        jazz::db::UpdateOptions {
            updated_at_ms: Some(200),
            ..Default::default()
        },
    ))
    .unwrap();
    block_on(alice.tick()).unwrap();
    let _held = alice_wire.drain_outbound();

    // Bob's title change is accepted and reaches Alice as a synced row.
    block_on(bob.update(
        "todos",
        row,
        BTreeMap::from([("title".to_owned(), Value::String("bob".to_owned()))]),
        jazz::db::UpdateOptions {
            updated_at_ms: Some(300),
            ..Default::default()
        },
    ))
    .unwrap();
    pump_client_core(&bob, &bob_wire, &mut core, bob_session);
    core.tick().unwrap();
    for frame in core.take_frames(alice_session).unwrap() {
        alice_wire.push_inbound(frame);
    }
    block_on(alice.tick()).unwrap();
    let _held = alice_wire.drain_outbound();

    let alice_row = |alice: &Db| {
        let prepared = alice.prepare_query(&Query::from("todos")).unwrap();
        let rows = block_on(alice.all(&prepared, ReadOpts::default())).unwrap();
        assert_eq!(rows.len(), 1);
        (
            rows[0].cell(&schema.tables[0], "title"),
            rows[0].cell(&schema.tables[0], "completed"),
        )
    };
    assert_eq!(
        alice_row(&alice),
        (
            Some(Value::String("bob".to_owned())),
            Some(Value::Bool(true))
        ),
        "bob's synced title sits under alice's pending edit"
    );

    // A second pending edit of another column must keep bob's title.
    block_on(alice.update(
        "todos",
        row,
        BTreeMap::from([("completed".to_owned(), Value::Bool(false))]),
        jazz::db::UpdateOptions {
            updated_at_ms: Some(400),
            ..Default::default()
        },
    ))
    .unwrap();
    block_on(alice.tick()).unwrap();
    let _held = alice_wire.drain_outbound();
    assert_eq!(
        alice_row(&alice),
        (
            Some(Value::String("bob".to_owned())),
            Some(Value::Bool(false))
        ),
        "alice's second pending edit must not revert bob's synced title"
    );
}

/// The `records` table of the TypeScript concurrent-merge suite: `count`
/// always merges as a counter; `tags` merges as a grow-only set only when
/// `gset` is set, and is an ordinary last-writer-wins column otherwise.
fn records_schema(gset: bool) -> JazzSchema {
    use jazz::tools::test_support::AllowAll;
    use jazz::tools::{ColumnMergeStrategy, RowDescriptor, TableName};
    let mut schema = SchemaBuilder::new()
        .table(
            TableSchemaBuilder::new("records")
                .column("title", ColumnType::Text)
                .column("archived", ColumnType::Boolean)
                .column("count", ColumnType::Integer)
                .column(
                    "tags",
                    ColumnType::Array {
                        element: Box::new(ColumnType::Text),
                    },
                ),
        )
        .allow_all()
        .build();
    let table = schema
        .get_mut(&TableName::new("records"))
        .expect("records table exists");
    table.columns = RowDescriptor::new(
        table
            .columns
            .columns
            .iter()
            .map(|column| match column.name.as_str() {
                "count" => column.clone().merge_strategy(ColumnMergeStrategy::Counter),
                "tags" if gset => column.clone().merge_strategy(ColumnMergeStrategy::GSet),
                _ => column.clone(),
            })
            .collect(),
    );
    compile_schema(&schema)
}

/// Two apps in one process whose `records` tables differ only in the merge
/// strategy of `tags` each converge concurrent writes and serve a fresh
/// editor.
///
/// Mirrors `packages/jazz-tools/src/backend/concurrent-merge.integration.test.ts`,
/// whose Counter-only and GSet-and-Counter cases run one after the other in
/// one process against in-process servers. The two tables have identical
/// column names and types; only how Core merges `tags` differs.
///
/// ```text
/// for tags in [LWW, GSet]:            (fresh core, fresh app each round)
///   writer ──insert──► core ◄──update── observer   (concurrent)
///   editor ──Global read──► core ──► the one converged row
///
/// Both updates are made over the seed, and Core sequences the writer's
/// first, so the observer's, applied last, keeps the plain cells both
/// changed (SPEC 4 §4.6).
/// ```
#[test]
fn apps_differing_only_in_a_merge_strategy_each_converge_in_one_process() {
    for (round, gset) in [false, true].into_iter().enumerate() {
        let round = round as u8;
        let schema = records_schema(gset);
        let mut core = InMemoryServerShell::start(
            InMemoryServerShellConfig::new(
                schema.clone(),
                identity(0xc8 + round, AuthorSubject::SYSTEM),
            )
            .with_role(NodeRole::Core),
        )
        .unwrap();
        let writer = open_db(0xa8 + round, author(0xa8 + round), &schema);
        let observer = open_db(0xb8 + round, author(0xb8 + round), &schema);
        let writer_wire = QueuedWireTransport::default();
        let observer_wire = QueuedWireTransport::default();
        let writer_session =
            connect_client_to_core(&mut core, &writer, &writer_wire, author(0xa8 + round));
        let observer_session =
            connect_client_to_core(&mut core, &observer, &observer_wire, author(0xb8 + round));

        let row = RowUuid::from_bytes([0xd8 + round; 16]);
        let seed = block_on(writer.insert(
            "records",
            BTreeMap::from([
                ("title".to_owned(), Value::String("seed".to_owned())),
                ("archived".to_owned(), Value::Bool(false)),
                ("count".to_owned(), Value::I32(0)),
                ("tags".to_owned(), tags(&["seed"])),
            ]),
            jazz::db::InsertOptions {
                row_id: Some(row),
                updated_at_ms: Some(100),
                ..Default::default()
            },
        ))
        .unwrap();
        for _ in 0..4 {
            pump_client_core(&writer, &writer_wire, &mut core, writer_session);
        }
        assert_eq!(
            writer
                .write_state(seed.mergeable_tx_id())
                .unwrap()
                .durability,
            DurabilityTier::Global,
            "round {round}: the seed settles at Core"
        );
        for db in [&writer, &observer] {
            let prepared = db.prepare_query(&Query::from("records")).unwrap();
            std::mem::forget(block_on(db.subscribe(&prepared, ReadOpts::default())).unwrap());
        }
        for _ in 0..2 {
            pump_client_core(&observer, &observer_wire, &mut core, observer_session);
            pump_client_core(&writer, &writer_wire, &mut core, writer_session);
        }

        for (db, title, at) in [(&writer, "left", 200), (&observer, "right", 300)] {
            block_on(db.update(
                "records",
                row,
                BTreeMap::from([
                    ("title".to_owned(), Value::String(title.to_owned())),
                    ("tags".to_owned(), tags(&[title])),
                ]),
                jazz::db::UpdateOptions {
                    updated_at_ms: Some(at),
                    ..Default::default()
                },
            ))
            .unwrap();
        }
        for _ in 0..4 {
            pump_client_core(&writer, &writer_wire, &mut core, writer_session);
            pump_client_core(&observer, &observer_wire, &mut core, observer_session);
        }

        let editor = open_db(0xe8 + round, author(0xe8 + round), &schema);
        let editor_wire = QueuedWireTransport::default();
        let editor_session =
            connect_client_to_core(&mut core, &editor, &editor_wire, author(0xe8 + round));
        let prepared = editor.prepare_query(&Query::from("records")).unwrap();
        let mut subscription = block_on(editor.subscribe(
            &prepared,
            ReadOpts {
                tier: DurabilityTier::Global,
                ..ReadOpts::default()
            },
        ))
        .unwrap();
        for _ in 0..4 {
            pump_client_core(&editor, &editor_wire, &mut core, editor_session);
        }
        let mut settled_rows = None;
        while let Some(event) = subscription.try_next_event() {
            if let jazz::db::SubscriptionEvent::Delta {
                settled: true,
                added,
                ..
            } = event
            {
                settled_rows = Some(added);
            }
        }
        let rows = settled_rows
            .unwrap_or_else(|| panic!("round {round}: the editor's Global read settles"));
        assert_eq!(rows.len(), 1, "round {round}: one converged row");
        let table = &schema.tables[0];
        assert_eq!(
            rows[0].row.cell(table, "title"),
            Some(Value::String("right".to_owned())),
            "round {round}: the last-sequenced title wins"
        );
        let expected_tags = if gset {
            tags(&["left", "right", "seed"])
        } else {
            tags(&["right"])
        };
        assert_eq!(
            rows[0].row.cell(table, "tags"),
            Some(expected_tags),
            "round {round}"
        );
    }
}
