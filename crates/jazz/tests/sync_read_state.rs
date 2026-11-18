//! A client live read's state per upstream connection: each connection
//! starts every read over (one reset, on a detach and on every new link,
//! a transport swapped without a detach included), only the current
//! connection's messages move it on, and what a subscription reports as
//! confirmed is the last answer it had, which an outage leaves standing (a
//! client tells a current result by that and its link being up).
//!
//! Every client is a core `Db` linked to a history-complete server `Db`
//! over in-memory pipes, with ticks driven explicitly.

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, VecDeque};
use std::rc::Rc;

use crate::common;

use jazz::db::{
    Db, DbConfig, DbIdentity, ErrorCode, InsertOptions, ReadOpts, ReadTier, SubscriptionEvent,
    SubscriptionStream, SyncMessage, Transport, TransportError, Upstream, block_on,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{Query, col, eq, lit};
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
use jazz::tx::DurabilityTier;

use common::{allow_all_policies, compile_schema};

fn schema() -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("body", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .build(),
    )
}

fn open(node: u8) -> Db {
    let config = DbConfig::new(
        schema(),
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author: if node == 0 {
                AuthorSubject::SYSTEM
            } else {
                AuthorSubject::for_test_bytes([node; 16])
            },
        },
    );
    if node == 0 {
        block_on(Db::open_history_complete(config))
    } else {
        block_on(Db::open(config))
    }
    .unwrap()
}

fn row(n: u8) -> RowUuid {
    let mut bytes = [0u8; 16];
    bytes[0] = 0x01;
    bytes[15] = n;
    RowUuid::from_bytes(bytes)
}

fn insert(db: &Db, n: u8) {
    block_on(db.insert(
        "notes",
        BTreeMap::from([("body".to_owned(), Value::from(format!("n{n}")))]),
        InsertOptions {
            row_id: Some(row(n)),
            ..Default::default()
        },
    ))
    .unwrap();
}

/// One end of a link. The client's end logs what the client sends, and
/// reports a failure (as a frame it cannot decode would) once `fail` is set.
struct Pipe {
    out: Rc<RefCell<VecDeque<SyncMessage>>>,
    inbound: Rc<RefCell<VecDeque<SyncMessage>>>,
    sent: Option<Rc<RefCell<Vec<SyncMessage>>>>,
    fail: Rc<Cell<bool>>,
}

impl Transport for Pipe {
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
        if let Some(sent) = &self.sent {
            sent.borrow_mut().push(message.clone());
        }
        self.out.borrow_mut().push_back(message);
        Ok(())
    }

    fn try_recv(&mut self) -> Option<SyncMessage> {
        self.inbound.borrow_mut().pop_front()
    }

    fn take_failure(&mut self) -> Option<TransportError> {
        self.fail
            .replace(false)
            .then(|| TransportError::Failed("undecodable frame".to_owned()))
    }
}

/// A client's link to the server, with handles to inject what the server
/// would send and to see what the client sent.
struct Link {
    upstream: Upstream,
    /// What reaches the client.
    down: Rc<RefCell<VecDeque<SyncMessage>>>,
    sent: Rc<RefCell<Vec<SyncMessage>>>,
    fail: Rc<Cell<bool>>,
}

impl Link {
    /// The wire id of the client's live read, as it last asked for it.
    fn read_id(&self) -> u64 {
        self.sent
            .borrow()
            .iter()
            .rev()
            .find_map(|message| match message {
                SyncMessage::Read { id, .. } => Some(*id),
                _ => None,
            })
            .expect("the client asked for a read")
    }
}

fn connect(server: &Db, client: &Db) -> Link {
    let up = Rc::new(RefCell::new(VecDeque::new()));
    let down = Rc::new(RefCell::new(VecDeque::new()));
    let sent = Rc::new(RefCell::new(Vec::new()));
    let fail = Rc::new(Cell::new(false));
    let upstream = block_on(client.connect_upstream(Box::new(Pipe {
        out: up.clone(),
        inbound: down.clone(),
        sent: Some(sent.clone()),
        fail: fail.clone(),
    })));
    server
        .accept_subscriber(
            Box::new(Pipe {
                out: down.clone(),
                inbound: up,
                sent: None,
                fail: Rc::new(Cell::new(false)),
            }),
            AuthorSubject::for_test_bytes([1; 16]),
        )
        .unwrap();
    Link {
        upstream,
        down,
        sent,
        fail,
    }
}

fn pump(server: &Db, client: &Db) {
    for _ in 0..6 {
        block_on(client.tick()).unwrap();
        block_on(server.tick()).unwrap();
    }
    block_on(client.tick()).unwrap();
}

fn drain(stream: &mut SubscriptionStream) -> Vec<SubscriptionEvent> {
    std::iter::from_fn(|| stream.try_next_event()).collect()
}

/// What the stream's events last reported: (settled, tier), if any was a
/// delta.
fn reported(events: &[SubscriptionEvent]) -> Option<(bool, DurabilityTier)> {
    events.iter().rev().find_map(|event| match event {
        SubscriptionEvent::Delta { settled, tier, .. } => Some((*settled, *tier)),
        _ => None,
    })
}

fn notes() -> Query {
    Query::from("notes")
}

fn remote() -> ReadOpts {
    ReadOpts::for_read_tier(ReadTier::Remote)
}

/// A subscription confirmed by the server keeps reporting so (the last
/// known, at its tier) while the link is down, on a delta its own write
/// makes and on a subscription that joins the read meanwhile. The
/// reconnect's answer confirms it again.
#[test]
fn an_outage_keeps_reporting_the_last_confirmed_answer() {
    let server = open(0);
    insert(&server, 1);
    let client = open(1);
    let link = connect(&server, &client);
    let query = client.prepare_query(&notes()).unwrap();
    let mut live = block_on(client.subscribe(&query, ReadOpts::default())).unwrap();
    pump(&server, &client);
    assert_eq!(
        reported(&drain(&mut live)),
        Some((true, DurabilityTier::Global)),
        "the server confirmed it"
    );

    assert!(client.detach_connection(&link.upstream));
    block_on(client.tick()).unwrap();
    insert(&client, 2);
    block_on(client.tick()).unwrap();
    let events = drain(&mut live);
    assert_eq!(
        reported(&events),
        Some((true, DurabilityTier::Global)),
        "a delta during the outage reports the last confirmed answer: {events:?}"
    );
    // A local-first subscription joining the read meanwhile opens on it,
    // as confirmed.
    let mut joined = block_on(client.subscribe(&query, ReadOpts::default())).unwrap();
    assert_eq!(
        reported(&drain(&mut joined)),
        Some((true, DurabilityTier::Global)),
        "a subscription opened during the outage reports the last answer"
    );
    // A Remote one waits for the server: no connection answered it.
    let mut strict = block_on(client.subscribe(&query, remote())).unwrap();
    block_on(client.tick()).unwrap();
    assert!(
        drain(&mut strict).is_empty(),
        "a Remote opening waits for an answer on a live link"
    );

    let _link = connect(&server, &client);
    pump(&server, &client);
    for stream in [&mut live, &mut joined] {
        let events = drain(stream);
        if let Some(reported) = reported(&events) {
            assert_eq!(reported, (true, DurabilityTier::Global), "{events:?}");
        }
    }
    assert_eq!(
        reported(&drain(&mut strict)),
        Some((true, DurabilityTier::Global)),
        "the reconnect's answer opens the Remote subscription"
    );
}

/// A transport swapped in without a detach is a new connection: the read
/// starts over on it, so the old link's answer opens no Remote
/// subscription until the new link answers.
#[test]
fn a_transport_swapped_without_a_detach_starts_the_read_over() {
    let server = open(0);
    insert(&server, 1);
    let client = open(1);
    let _old = connect(&server, &client);
    let query = client.prepare_query(&notes()).unwrap();
    let mut live = block_on(client.subscribe(&query, remote())).unwrap();
    pump(&server, &client);
    assert_eq!(
        reported(&drain(&mut live)),
        Some((true, DurabilityTier::Global))
    );

    let new = connect(&server, &client);
    let mut joined = block_on(client.subscribe(&query, remote())).unwrap();
    block_on(client.tick()).unwrap();
    assert!(
        drain(&mut joined).is_empty(),
        "the last link's answer does not open it"
    );
    assert!(
        new.sent
            .borrow()
            .iter()
            .any(|message| matches!(message, SyncMessage::Read { .. })),
        "the read is asked again on the new link"
    );
    pump(&server, &client);
    assert_eq!(
        reported(&drain(&mut joined)),
        Some((true, DurabilityTier::Global)),
        "the new link's answer opens it"
    );
    // The first subscription saw nothing change in what it reports.
    let events = drain(&mut live);
    if let Some(reported) = reported(&events) {
        assert_eq!(reported, (true, DurabilityTier::Global), "{events:?}");
    }
}

/// A connection that refuses a read rejects its subscriptions, those
/// opened while it stands included. The next connection starts the read
/// over: a subscription opened then is not rejected, and is answered.
#[test]
fn a_refusal_lasts_only_for_its_connection() {
    let server = open(0);
    insert(&server, 1);
    let client = open(1);
    let link = connect(&server, &client);
    let query = client.prepare_query(&notes()).unwrap();
    let mut first = block_on(client.subscribe(&query, remote())).unwrap();
    pump(&server, &client);
    drain(&mut first);

    link.down.borrow_mut().push_back(SyncMessage::Refused {
        id: link.read_id(),
        code: ErrorCode::Query,
        message: "not for you".to_owned(),
    });
    block_on(client.tick()).unwrap();
    let rejected = |events: &[SubscriptionEvent]| {
        events.iter().any(|event| {
            matches!(
                event,
                SubscriptionEvent::Rejected {
                    code: ErrorCode::Query,
                    ..
                }
            )
        })
    };
    assert!(rejected(&drain(&mut first)), "the refusal rejects it");
    let mut during = block_on(client.subscribe(&query, remote())).unwrap();
    assert!(
        rejected(&drain(&mut during)),
        "one opened while the refusal stands is rejected at once"
    );
    drop(during);

    assert!(client.detach_connection(&link.upstream));
    let _link = connect(&server, &client);
    let mut after = block_on(client.subscribe(&query, remote())).unwrap();
    block_on(client.tick()).unwrap();
    assert!(
        !rejected(&drain(&mut after)),
        "the new connection has not refused it"
    );
    pump(&server, &client);
    let events = drain(&mut after);
    assert!(!rejected(&events), "{events:?}");
    assert_eq!(
        reported(&events),
        Some((true, DurabilityTier::Global)),
        "and answers it: {events:?}"
    );
}

/// What a link sends in the batch in which it drops (its next frame could
/// not be decoded) is no answer on the link that follows, nor on none: a
/// Remote subscription opened then waits for the next connection.
#[test]
fn an_answer_from_a_dropped_link_moves_no_read() {
    let server = open(0);
    insert(&server, 1);
    let client = open(1);
    let link = connect(&server, &client);
    let query = client.prepare_query(&notes()).unwrap();
    let mut live = block_on(client.subscribe(&query, remote())).unwrap();
    block_on(client.tick()).unwrap();
    // The server answers, and the link fails in the same batch.
    block_on(server.tick()).unwrap();
    assert!(
        link.down
            .borrow()
            .iter()
            .any(|message| matches!(message, SyncMessage::Settled { .. })),
        "the server answered"
    );
    link.fail.set(true);
    block_on(client.tick()).unwrap();
    assert!(
        !client.detach_connection(&link.upstream),
        "the failure dropped the link"
    );
    let events = drain(&mut live);
    assert!(
        reported(&events).is_none(),
        "the dropped link's answer opens nothing: {events:?}"
    );

    let _link = connect(&server, &client);
    pump(&server, &client);
    assert_eq!(
        reported(&drain(&mut live)),
        Some((true, DurabilityTier::Global)),
        "the next connection answers it"
    );
}

/// The bodies the store holds, by row: what a local read lists.
fn bodies(db: &Db) -> BTreeMap<RowUuid, String> {
    let all = db.prepare_query(&notes()).unwrap();
    block_on(db.all(&all, ReadOpts::for_read_tier(ReadTier::LocalOnly)))
        .unwrap()
        .iter()
        .map(|row| {
            let body = row
                .cells()
                .find(|(name, _)| *name == "body")
                .and_then(|(_, value)| value.cloned());
            (row.row_uuid(), format!("{body:?}"))
        })
        .collect()
}

/// A reconnect starts the read over on the new connection and resumes it:
/// what changed while the replica was away reaches it, and a row that left
/// the result is either gone or current, never the stale version. (A row
/// no read covers stays as cold cache, as before.)
#[test]
fn a_reconnect_resumes_the_read_on_the_new_connection() {
    let server = open(0);
    for n in 1..=2 {
        insert(&server, n);
    }
    let client = open(1);
    let link = connect(&server, &client);
    let narrow = client
        .prepare_query(&notes().filter(eq(col("body"), lit(Value::from("n1")))))
        .unwrap();
    let all = client.prepare_query(&notes()).unwrap();
    let mut everything = block_on(client.subscribe(&all, ReadOpts::default())).unwrap();
    let _narrow = block_on(client.subscribe(&narrow, ReadOpts::default())).unwrap();
    pump(&server, &client);
    drain(&mut everything);

    assert!(client.detach_connection(&link.upstream));
    block_on(server.update(
        "notes",
        row(1),
        BTreeMap::from([("body".to_owned(), Value::from("n1, edited"))]),
        Default::default(),
    ))
    .unwrap();
    insert(&server, 3);
    let link = connect(&server, &client);
    pump(&server, &client);
    assert!(
        link.sent
            .borrow()
            .iter()
            .filter(|message| matches!(message, SyncMessage::Read { .. }))
            .count()
            >= 2,
        "both reads are asked again on the new connection"
    );
    assert_eq!(bodies(&client), bodies(&server), "the replica caught up");
    let events = drain(&mut everything);
    assert_eq!(
        reported(&events),
        Some((true, DurabilityTier::Global)),
        "{events:?}"
    );
}
