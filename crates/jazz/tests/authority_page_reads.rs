#![cfg(feature = "runtime")]
//! A bounded Global one-shot page served by the admitted Core is installed in
//! the reader's local store before the read resolves, so a later Local read
//! (including one made with no further sync turns) sees the same rows.
//!
//! Every client is a core `Db` connected to an in-memory Core shell through
//! the public byte-level wire adapter. Both ends carry the authenticated
//! session context a WebSocket admission would install, which is what makes
//! the Core eligible to serve pages directly.

use std::cell::RefCell;
use std::collections::{BTreeMap, VecDeque};
use std::pin::pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

mod common;

use jazz::db::{
    CommitUnitTrust, ConnectionSessionContext, Db, DbConfig, DbIdentity, EmptyOpening,
    LocalUpdates, Propagation, ReadOpts, SerializedReadResult, WireTransportAdapter, block_on,
};
use jazz::groove::records::Value;
use jazz::groove::storage::TestStorage;
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::Query;
use jazz::schema::JazzSchema;
use jazz::serving::{
    InMemoryServerShell, InMemoryServerShellConfig, NodeRole, ServerLinkAdmission, ServerSession,
};
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
use jazz::tx::DurabilityTier;
use jazz::wire::{
    TransportError, WIRE_PROTOCOL_VERSION, WireAuthorityEndpoint, WireTransport,
    current_wire_features,
};

use common::{allow_all_policies, compile_schema};

const CORE_NODE: u8 = 0xc0;
const TITLES: [&str; 3] = ["first", "second", "third"];
/// Bound on host turns for anything that only needs a few round trips.
const MAX_TURNS: usize = 64;

fn schema() -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("todos")
                    .column("title", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .build(),
    )
}

fn row(index: u8) -> RowUuid {
    let mut bytes = [0u8; 16];
    bytes[..8].copy_from_slice(&0x019e_0000_0000_7000u64.to_be_bytes());
    bytes[15] = index + 1;
    RowUuid::from_bytes(bytes)
}

/// A Core holding `first`, `second` and `third` as settled Global history.
fn seeded_core(schema: &JazzSchema) -> InMemoryServerShell {
    let mut core = InMemoryServerShell::start(
        InMemoryServerShellConfig::new(
            schema.clone(),
            DbIdentity {
                node: NodeUuid::from_bytes([CORE_NODE; 16]),
                author: AuthorSubject::SYSTEM,
            },
        )
        .with_role(NodeRole::Core),
    )
    .expect("start core");
    for (index, title) in TITLES.iter().enumerate() {
        core.seed_row_with_id(
            "todos",
            row(index as u8),
            BTreeMap::from([("title".to_owned(), Value::String((*title).to_owned()))]),
        )
        .expect("seed core row");
    }
    core
}

fn open_client(node: u8, author: AuthorSubject, schema: &JazzSchema) -> Db<TestStorage> {
    let families = schema.column_families();
    let families = families.iter().map(String::as_str).collect::<Vec<_>>();
    block_on(Db::open(DbConfig::new(
        schema.clone(),
        TestStorage::new(&families),
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author,
        },
    )))
    .expect("open client")
}

#[derive(Clone, Default)]
struct QueuedWire {
    queues: Rc<RefCell<(VecDeque<Vec<u8>>, VecDeque<Vec<u8>>)>>,
}

impl WireTransport for QueuedWire {
    fn send_frame(&mut self, frame: Vec<u8>) -> Result<(), TransportError> {
        self.queues.borrow_mut().1.push_back(frame);
        Ok(())
    }

    fn try_recv_frame(&mut self) -> Option<Vec<u8>> {
        self.queues.borrow_mut().0.pop_front()
    }
}

struct Link {
    wire: QueuedWire,
    session: ServerSession,
}

/// Connect `client` to the Core with the admitted endpoint facts both sides
/// would receive from an authenticated WebSocket handshake.
fn connect(
    client: &Db<TestStorage>,
    node: u8,
    author: AuthorSubject,
    core: &mut InMemoryServerShell,
) -> Link {
    let client_endpoint = WireAuthorityEndpoint {
        node: NodeUuid::from_bytes([node; 16]),
        epoch: 1,
    };
    let core_endpoint = WireAuthorityEndpoint {
        node: NodeUuid::from_bytes([CORE_NODE; 16]),
        epoch: 1,
    };
    let features = current_wire_features();
    let wire = QueuedWire::default();
    block_on(
        client.connect_upstream(Box::new(WireTransportAdapter::new_with_session_context(
            wire.clone(),
            WIRE_PROTOCOL_VERSION,
            features,
            None,
            Some(ConnectionSessionContext {
                local: client_endpoint,
                remote: Some(core_endpoint),
                link_identity: author,
                negotiated_features: features,
            }),
        ))),
    );
    let session = core
        .accept_subscriber_session_with_claims_and_trust_and_context(
            author,
            BTreeMap::new(),
            CommitUnitTrust::Session,
            features,
            Some(ConnectionSessionContext {
                local: core_endpoint,
                remote: Some(client_endpoint),
                link_identity: author,
                negotiated_features: features,
            }),
            ServerLinkAdmission::OrdinarySession,
        )
        .expect("core accepts client");
    Link { wire, session }
}

/// One host turn: client out, Core tick, Core frames back, client in.
fn turn(client: &Db<TestStorage>, link: &Link, core: &mut InMemoryServerShell) {
    block_on(client.tick()).expect("tick client");
    let outbound = link
        .wire
        .queues
        .borrow_mut()
        .1
        .drain(..)
        .collect::<Vec<_>>();
    core.receive_frames(link.session, outbound)
        .expect("core receives");
    core.tick().expect("tick core");
    let inbound = core.take_frames(link.session).expect("core frames");
    link.wire.queues.borrow_mut().0.extend(inbound);
    block_on(client.tick()).expect("tick client after core");
}

/// Drive a host one-shot read to completion the way bindings re-poll it.
fn one_shot(
    client: &Db<TestStorage>,
    link: &Link,
    core: &mut InMemoryServerShell,
    query: &Query,
    opts: ReadOpts,
) -> SerializedReadResult {
    let bytes = postcard::to_allocvec(query).expect("encode query");
    let mut read = pin!(client.all_serialized_query(
        &bytes,
        opts,
        None,
        None,
        None,
        true,
        || false,
        |attachment| client.detach_query(attachment),
    ));
    let mut context = Context::from_waker(Waker::noop());
    for _ in 0..MAX_TURNS {
        if let Poll::Ready(result) = read.as_mut().poll(&mut context) {
            return result.expect("one-shot read");
        }
        turn(client, link, core);
    }
    panic!("one-shot read did not settle within {MAX_TURNS} turns");
}

/// Titles visible to a Local read, with no sync turn in between.
fn local_titles(client: &Db<TestStorage>) -> Vec<String> {
    let schema = schema();
    let prepared = client
        .prepare_query(&Query::from("todos"))
        .expect("prepare local query");
    let mut titles = block_on(client.all(
        &prepared,
        ReadOpts {
            tier: DurabilityTier::Local,
            local_updates: LocalUpdates::Immediate,
            propagation: Propagation::Full,
            ..ReadOpts::default()
        },
    ))
    .expect("local read")
    .into_iter()
    .map(|row| match row.cell(&schema.tables[0], "title") {
        Some(Value::String(title)) => title,
        other => panic!("expected a title, got {other:?}"),
    })
    .collect::<Vec<_>>();
    titles.sort();
    titles
}

/// Alice's bounded Global page is served by the Core and warms her store:
/// the same rows are visible to her next Local read with no further sync.
///
/// ```text
/// alice ──RemoteReadRequest(limit 10)──► core(first, second, third)
///       ◄──rows + receipt carriers────── core
/// alice: ingest carriers, then resolve the Global read (EncodedRows)
/// alice: Local read, no turn ──► [first, second, third]
/// ```
#[test]
fn global_authority_page_is_visible_to_a_following_local_read() {
    let schema = schema();
    let mut core = seeded_core(&schema);
    let alice_id = AuthorSubject::for_test_bytes([0xa1; 16]);
    let alice = open_client(0xa1, alice_id, &schema);
    let link = connect(&alice, 0xa1, alice_id, &mut core);
    for _ in 0..4 {
        turn(&alice, &link, &mut core);
    }
    assert!(local_titles(&alice).is_empty(), "alice starts cold");

    let result = one_shot(
        &alice,
        &link,
        &mut core,
        &Query::from("todos").limit(10),
        ReadOpts {
            tier: DurabilityTier::Global,
            local_updates: LocalUpdates::Immediate,
            ..ReadOpts::default()
        },
    );
    // Only the Core-served route returns the binding envelope directly.
    assert!(
        matches!(result, SerializedReadResult::EncodedRows(_)),
        "expected the Core-served page, not the coverage read"
    );

    assert_eq!(local_titles(&alice), ["first", "second", "third"]);
}

/// A local-first-unless-empty read on a cold client takes the Core-served
/// page for its remote leg, and that page fills the local store just as the
/// old coverage leg did.
///
/// ```text
/// bob: local leg ──► [] (cold)
/// bob ──RemoteReadRequest──► core ──rows + receipt──► bob (ingested)
/// bob: Local read, no turn ──► [first, second, third]
/// ```
#[test]
fn local_first_unless_empty_remote_leg_fills_the_local_store() {
    let schema = schema();
    let mut core = seeded_core(&schema);
    let bob_id = AuthorSubject::for_test_bytes([0xb1; 16]);
    let bob = open_client(0xb1, bob_id, &schema);
    let link = connect(&bob, 0xb1, bob_id, &mut core);
    for _ in 0..4 {
        turn(&bob, &link, &mut core);
    }

    let result = one_shot(
        &bob,
        &link,
        &mut core,
        &Query::from("todos").limit(10),
        ReadOpts {
            empty_opening: EmptyOpening::AwaitRemote,
            ..ReadOpts::default()
        },
    );
    assert!(
        matches!(result, SerializedReadResult::EncodedRows(_)),
        "the remote leg should be the Core-served page"
    );

    assert_eq!(local_titles(&bob), ["first", "second", "third"]);
}
