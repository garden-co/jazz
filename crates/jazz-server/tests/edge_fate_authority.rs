use std::cell::RefCell;
use std::collections::{BTreeMap, VecDeque};
use std::rc::Rc;

use jazz::db::{
    Db, DbConfig, DbIdentity, LocalUpdates, Propagation, ReadOpts, Transport, WireTransportAdapter,
    block_on,
};
use jazz::groove::records::Value;
use jazz::groove::schema::{ColumnSchema, ColumnType};
use jazz::groove::storage::MemoryStorage;
use jazz::ids::{AuthorId, NodeUuid};
use jazz::protocol::SyncMessage;
use jazz::query::Query;
use jazz::schema::{JazzSchema, TableSchema};
use jazz::tx::DurabilityTier;
use jazz::wire::{TransportError, WireTransport};
use jazz_server::{InMemoryServerShell, InMemoryServerShellConfig, NodeRole, ServerSession};

fn node(byte: u8) -> NodeUuid {
    NodeUuid::from_bytes([byte; 16])
}

fn author(byte: u8) -> AuthorId {
    AuthorId::from_bytes([byte; 16])
}

fn identity(node_byte: u8, author: AuthorId) -> DbIdentity {
    DbIdentity {
        node: node(node_byte),
        author,
    }
}

fn schema() -> JazzSchema {
    JazzSchema::new([TableSchema::new(
        "todos",
        [ColumnSchema::new("title", ColumnType::String)],
    )])
}

fn open_db(node_byte: u8, author: AuthorId, schema: &JazzSchema) -> Db<MemoryStorage> {
    let refs = schema.column_families();
    let refs = refs.iter().map(String::as_str).collect::<Vec<_>>();
    block_on(Db::open(DbConfig::new(
        schema.clone(),
        MemoryStorage::new(&refs),
        identity(node_byte, author),
    )))
    .unwrap()
}

fn open_core(node_byte: u8, schema: &JazzSchema) -> Db<MemoryStorage> {
    let refs = schema.column_families();
    let refs = refs.iter().map(String::as_str).collect::<Vec<_>>();
    block_on(Db::open_history_complete(DbConfig::new(
        schema.clone(),
        MemoryStorage::new(&refs),
        identity(node_byte, AuthorId::SYSTEM),
    )))
    .unwrap()
}

struct DuplexTransport {
    outbound: Rc<RefCell<VecDeque<SyncMessage>>>,
    inbound: Rc<RefCell<VecDeque<SyncMessage>>>,
}

impl Transport for DuplexTransport {
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
        self.outbound.borrow_mut().push_back(message);
        Ok(())
    }

    fn try_recv(&mut self) -> Option<SyncMessage> {
        self.inbound.borrow_mut().pop_front()
    }
}

fn duplex() -> (Box<dyn Transport>, Box<dyn Transport>) {
    let left = Rc::new(RefCell::new(VecDeque::new()));
    let right = Rc::new(RefCell::new(VecDeque::new()));
    (
        Box::new(DuplexTransport {
            outbound: Rc::clone(&left),
            inbound: Rc::clone(&right),
        }),
        Box::new(DuplexTransport {
            outbound: right,
            inbound: left,
        }),
    )
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
    client: &Db<MemoryStorage>,
    client_wire: &QueuedWireTransport,
    identity: AuthorId,
) -> ServerSession {
    client.connect_upstream(Box::new(WireTransportAdapter::current(client_wire.clone())));
    edge.accept_subscriber_session(identity).unwrap()
}

fn pump_client_edge(
    client: &Db<MemoryStorage>,
    wire: &QueuedWireTransport,
    edge: &mut InMemoryServerShell,
    session: ServerSession,
) {
    client.tick().unwrap();
    edge.receive_frames(session, wire.drain_outbound()).unwrap();
    edge.tick().unwrap();
    for frame in edge.take_frames(session).unwrap() {
        wire.push_inbound(frame);
    }
    client.tick().unwrap();
}

fn visible_titles(db: &Db<MemoryStorage>, tier: DurabilityTier) -> Vec<String> {
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
        InMemoryServerShellConfig::new(schema.clone(), identity(0xe0, AuthorId::SYSTEM))
            .with_role(NodeRole::Edge),
    )
    .unwrap();
    let core = open_core(0xc0, &schema);
    let (edge_to_core, core_to_edge) = duplex();
    edge.connect_upstream(edge_to_core).unwrap();
    core.accept_subscriber(core_to_edge, AuthorId::SYSTEM);

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

    let write = alice
        .insert(
            "todos",
            BTreeMap::from([("title".to_owned(), Value::String("edge only".to_owned()))]),
        )
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
        InMemoryServerShellConfig::new(schema.clone(), identity(0xc0, AuthorId::SYSTEM))
            .with_role(NodeRole::Core),
    )
    .unwrap();

    let alice = open_db(0xa1, author(0xa1), &schema);
    let alice_wire = QueuedWireTransport::default();
    let alice_session = connect_client_to_edge(&mut core, &alice, &alice_wire, author(0xa1));

    let write = alice
        .insert(
            "todos",
            BTreeMap::from([("title".to_owned(), Value::String("core global".to_owned()))]),
        )
        .unwrap();
    pump_client_edge(&alice, &alice_wire, &mut core, alice_session);

    assert!(block_on(write.wait(DurabilityTier::Global)).is_ok());
    assert_eq!(
        visible_titles(&alice, DurabilityTier::Global),
        ["core global"]
    );
}
