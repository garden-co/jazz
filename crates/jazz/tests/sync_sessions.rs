//! Server session lifecycle: a disconnected or failing client releases its
//! session, never stalls the others, and its slot is reused, so churn does
//! not grow the server. Not ported from Jazz.

use crate::common;

use std::cell::RefCell;
use std::collections::{BTreeMap, VecDeque};
use std::rc::Rc;

use jazz::db::{
    Db, DbConfig, DbIdentity, ReadOpts, ReadTier, SyncMessage, Transport, TransportError, block_on,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::Query;
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};

use common::{allow_all_policies, compile_schema};

fn schema() -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("messages")
                    .column("body", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .build(),
    )
}

fn user(n: u8) -> AuthorSubject {
    AuthorSubject::for_test_bytes([n; 16])
}

fn open(node: u8, author: AuthorSubject) -> Db {
    let schema = schema();
    let config = DbConfig::new(
        schema,
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author,
        },
    );
    if node == 0 {
        block_on(Db::open_history_complete(config))
    } else {
        block_on(Db::open(config))
    }
    .unwrap()
}

/// A duplex whose sending side can be made to fail.
struct Pipe {
    out: Rc<RefCell<VecDeque<SyncMessage>>>,
    inbound: Rc<RefCell<VecDeque<SyncMessage>>>,
    broken: Rc<RefCell<bool>>,
}

impl Transport for Pipe {
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
        if *self.broken.borrow() {
            return Err(TransportError::Failed("peer gone".into()));
        }
        self.out.borrow_mut().push_back(message);
        Ok(())
    }

    fn try_recv(&mut self) -> Option<SyncMessage> {
        self.inbound.borrow_mut().pop_front()
    }
}

/// (client side, server side, break the server's sends)
fn pipe() -> (Box<dyn Transport>, Box<dyn Transport>, Rc<RefCell<bool>>) {
    let up = Rc::new(RefCell::new(VecDeque::new()));
    let down = Rc::new(RefCell::new(VecDeque::new()));
    let broken = Rc::new(RefCell::new(false));
    (
        Box::new(Pipe {
            out: up.clone(),
            inbound: down.clone(),
            broken: Rc::new(RefCell::new(false)),
        }),
        Box::new(Pipe {
            out: down,
            inbound: up,
            broken: broken.clone(),
        }),
        broken,
    )
}

fn pump(server: &Db, clients: &[&Db]) {
    for _ in 0..5 {
        for client in clients {
            block_on(client.tick()).unwrap();
        }
        block_on(server.tick()).unwrap();
    }
    for client in clients {
        block_on(client.tick()).unwrap();
    }
}

fn local(db: &Db) -> usize {
    let prepared = db.prepare_query(&Query::from("messages")).unwrap();
    block_on(db.all(
        &prepared,
        ReadOpts {
            tier: ReadTier::LocalOnly,
            ..ReadOpts::default()
        },
    ))
    .unwrap()
    .len()
}

fn post(server: &Db, n: u32) {
    let mut bytes = [0u8; 16];
    bytes[12..].copy_from_slice(&n.to_be_bytes());
    let cells: BTreeMap<String, Value> =
        [("body".to_owned(), Value::String(format!("m{n}")))].into();
    server
        .seed_settled_mergeable_for_bootstrap(
            "messages",
            RowUuid::from_bytes(bytes),
            AuthorSubject::SYSTEM,
            cells,
        )
        .unwrap();
}

#[test]
fn a_failing_client_is_dropped_without_stalling_the_others() {
    let server = open(0, AuthorSubject::SYSTEM);
    let alice = open(1, user(1));
    let bob = open(2, user(2));
    let (a_client, a_server, a_broken) = pipe();
    let (b_client, b_server, _) = pipe();
    let _a = block_on(alice.connect_upstream(a_client));
    let _b = block_on(bob.connect_upstream(b_client));
    server.accept_subscriber(a_server, user(1));
    server.accept_subscriber(b_server, user(2));
    let all = Query::from("messages");
    let _sa = block_on(alice.subscribe(&alice.prepare_query(&all).unwrap(), ReadOpts::default()))
        .unwrap();
    let _sb =
        block_on(bob.subscribe(&bob.prepare_query(&all).unwrap(), ReadOpts::default())).unwrap();
    post(&server, 0);
    pump(&server, &[&alice, &bob]);
    assert_eq!((local(&alice), local(&bob)), (1, 1));

    *a_broken.borrow_mut() = true;
    post(&server, 1);
    pump(&server, &[&alice, &bob]);
    assert_eq!(
        local(&bob),
        2,
        "bob keeps receiving after alice's transport failed"
    );
    assert_eq!(server.open_subscribers(), 1, "alice's session was released");
    post(&server, 2);
    pump(&server, &[&alice, &bob]);
    assert_eq!(local(&bob), 3);
}

#[test]
fn disconnect_releases_the_session_and_reconnect_resumes() {
    let server = open(0, AuthorSubject::SYSTEM);
    let alice = open(1, user(1));
    let (client_side, server_side, _) = pipe();
    let upstream = block_on(alice.connect_upstream(client_side));
    let session = server.accept_subscriber(server_side, user(1));
    let prepared = alice.prepare_query(&Query::from("messages")).unwrap();
    let _stream = block_on(alice.subscribe(&prepared, ReadOpts::default())).unwrap();
    post(&server, 0);
    pump(&server, &[&alice]);
    assert_eq!(local(&alice), 1);

    assert!(alice.detach_connection(&upstream));
    server.disconnect_subscriber(session);
    assert_eq!(server.open_subscribers(), 0);
    for n in 1..4 {
        post(&server, n);
    }
    block_on(server.tick()).unwrap();

    let (client_side, server_side, _) = pipe();
    let _upstream = block_on(alice.connect_upstream(client_side));
    let again = server.accept_subscriber(server_side, user(1));
    assert_eq!(again.0, session.0, "the released slot is reused");
    pump(&server, &[&alice]);
    assert_eq!(local(&alice), 4, "the resumed subscription catches up");
}

/// Churn: 300 clients connect, subscribe, sync and leave while one long
/// lived client stays. The server ends with one session and one slot.
#[test]
fn churn_does_not_grow_the_server() {
    let server = open(0, AuthorSubject::SYSTEM);
    let stay = open(1, user(1));
    let (client_side, server_side, _) = pipe();
    let _keep = block_on(stay.connect_upstream(client_side));
    server.accept_subscriber(server_side, user(1));
    let _s = block_on(stay.subscribe(
        &stay.prepare_query(&Query::from("messages")).unwrap(),
        ReadOpts::default(),
    ))
    .unwrap();
    for n in 0..300u32 {
        let visitor = open(2, user(2 + (n % 50) as u8));
        let (client_side, server_side, _) = pipe();
        let upstream = block_on(visitor.connect_upstream(client_side));
        let session = server.accept_subscriber(server_side, user(2 + (n % 50) as u8));
        assert_eq!(session.0, 1, "every visitor reuses the same slot");
        let _v = block_on(visitor.subscribe(
            &visitor.prepare_query(&Query::from("messages")).unwrap(),
            ReadOpts::default(),
        ))
        .unwrap();
        post(&server, n);
        pump(&server, &[&stay, &visitor]);
        assert_eq!(local(&visitor), n as usize + 1);
        assert!(visitor.detach_connection(&upstream));
        server.disconnect_subscriber(session);
    }
    pump(&server, &[&stay]);
    assert_eq!(local(&stay), 300);
    assert_eq!(server.open_subscribers(), 1);
}

/// Alice's ordinary socket has no room to send; it is disconnected rather
/// than retained as a local retry carrier. Bob still receives later writes.
#[test]
fn ordinary_socket_backpressure_still_releases_the_session() {
    // A transport fixture is necessary to reject a send before its message
    // is accepted; database operations cannot control a socket's capacity.
    struct Pressured(Box<dyn Transport>);
    impl Transport for Pressured {
        fn send(&mut self, _: SyncMessage) -> Result<(), TransportError> {
            Err(TransportError::Backpressure)
        }
        fn try_recv(&mut self) -> Option<SyncMessage> {
            self.0.try_recv()
        }
    }
    let server = open(0, AuthorSubject::SYSTEM);
    let alice = open(1, user(1));
    let bob = open(2, user(2));
    let (client_side, server_side, _) = pipe();
    let _alice_up = block_on(alice.connect_upstream(client_side));
    server.accept_subscriber(Box::new(Pressured(server_side)), user(1));
    let (client_side, server_side, _) = pipe();
    let _bob_up = block_on(bob.connect_upstream(client_side));
    server.accept_subscriber(server_side, user(2));
    let query = Query::from("messages");
    let _alice_rows =
        block_on(alice.subscribe(&alice.prepare_query(&query).unwrap(), ReadOpts::default()))
            .unwrap();
    let _bob_rows =
        block_on(bob.subscribe(&bob.prepare_query(&query).unwrap(), ReadOpts::default())).unwrap();
    post(&server, 0);
    pump(&server, &[&alice, &bob]);
    assert_eq!(server.open_subscribers(), 1);
    assert_eq!(local(&bob), 1);
    post(&server, 1);
    pump(&server, &[&alice, &bob]);
    assert_eq!(local(&bob), 2);
}
