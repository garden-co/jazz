//! A mergeable commit's effects must reach a reader atomically, even when
//! the authority's fan-out for it outgrows one wire frame and the frames
//! arrive across several client ticks.

use crate::common;

use std::cell::RefCell;
use std::collections::{BTreeMap, VecDeque};
use std::rc::Rc;

use jazz::db::{Db, DbConfig, DbIdentity, ReadOpts, WireTransportAdapter, block_on};
use jazz::ids::{AuthorSubject, NodeUuid};
use jazz::query::Query;
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, OpenTransactionId, SchemaBuilder, TableSchemaBuilder};
use jazz::wire::{TransportError, WireTransport};

use common::{allow_all_policies, compile_schema};

/// Frames one side sent, held until the test releases them.
type Queue = Rc<RefCell<VecDeque<Vec<u8>>>>;

struct GatedWire {
    outbound: Queue,
    inbound: Queue,
    /// Frames of `inbound` this side may read now.
    released: Rc<RefCell<usize>>,
}

impl WireTransport for GatedWire {
    fn send_frame(&mut self, frame: Vec<u8>) -> Result<(), TransportError> {
        self.outbound.borrow_mut().push_back(frame);
        Ok(())
    }

    fn try_recv_frame(&mut self) -> Option<Vec<u8>> {
        let mut released = self.released.borrow_mut();
        if *released == 0 {
            return None;
        }
        let frame = self.inbound.borrow_mut().pop_front()?;
        *released -= 1;
        Some(frame)
    }
}

const ROWS: usize = 100;
const BODY: usize = 30_000;

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

fn open(node: u8, author: AuthorSubject) -> Db {
    block_on(Db::open(DbConfig::new(
        schema(),
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author,
        },
    )))
    .unwrap()
}

fn visible(db: &Db) -> usize {
    let prepared = db.prepare_query(&Query::from("notes")).unwrap();
    block_on(db.all(&prepared, ReadOpts::default()))
        .unwrap()
        .len()
}

#[test]
fn a_commit_split_across_frames_is_applied_whole() {
    let server = open(0xc0, AuthorSubject::SYSTEM);
    let reader_author = AuthorSubject::for_test_bytes([0x51; 16]);
    let reader = open(0x51, reader_author);

    let to_server: Queue = Rc::default();
    let to_reader: Queue = Rc::default();
    // The server reads everything at once; the reader one frame at a time.
    let server_released = Rc::new(RefCell::new(usize::MAX));
    let reader_released = Rc::new(RefCell::new(0usize));
    let reader_side = GatedWire {
        outbound: Rc::clone(&to_server),
        inbound: Rc::clone(&to_reader),
        released: Rc::clone(&reader_released),
    };
    let server_side = GatedWire {
        outbound: to_reader,
        inbound: to_server,
        released: server_released,
    };
    block_on(reader.connect_upstream(Box::new(WireTransportAdapter::current(reader_side))));
    server.accept_subscriber(
        Box::new(WireTransportAdapter::current(server_side)),
        reader_author,
    );

    // Keep the reader subscribed to every note, and let the handshake and
    // the empty first result through.
    let prepared = reader.prepare_query(&Query::from("notes")).unwrap();
    let _subscription = block_on(reader.subscribe(&prepared, ReadOpts::default())).unwrap();
    for _ in 0..20 {
        *reader_released.borrow_mut() = usize::MAX;
        block_on(reader.tick()).unwrap();
        block_on(server.tick()).unwrap();
    }
    assert_eq!(visible(&reader), 0);

    // One mergeable transaction inserts every note: about 3 MB of rows,
    // more than one 2 MiB wire frame carries.
    let tx = OpenTransactionId::new();
    block_on(server.begin_mergeable(tx)).unwrap();
    let body = "x".repeat(BODY);
    for _ in 0..ROWS {
        let values = BTreeMap::from([("body".to_owned(), Value::String(body.clone()))]);
        block_on(
            server
                .tx_ref(tx)
                .insert("notes", values, Default::default()),
        )
        .unwrap();
    }
    block_on(server.commit_mergeable_handle(tx)).unwrap();
    block_on(server.tick()).unwrap();
    *reader_released.borrow_mut() = 0;

    // Deliver the fan-out one frame per reader tick. At every point the
    // reader sees none of the commit or all of it.
    let mut seen = Vec::new();
    for _ in 0..50 {
        *reader_released.borrow_mut() = 1;
        block_on(reader.tick()).unwrap();
        let count = visible(&reader);
        seen.push(count);
        assert!(
            count == 0 || count == ROWS,
            "reader saw {count} of {ROWS} rows of one commit (counts per tick: {seen:?})"
        );
        if count == ROWS {
            return;
        }
    }
    panic!("the commit never arrived: {seen:?}");
}

/// A wire whose outbound queue takes `room` frames, then refuses with
/// `Backpressure` until the test makes room again.
struct NarrowWire {
    outbound: Queue,
    inbound: Queue,
    room: Rc<RefCell<usize>>,
}

impl WireTransport for NarrowWire {
    fn send_frame(&mut self, frame: Vec<u8>) -> Result<(), TransportError> {
        let mut room = self.room.borrow_mut();
        if *room == 0 {
            return Err(TransportError::Backpressure);
        }
        *room -= 1;
        self.outbound.borrow_mut().push_back(frame);
        Ok(())
    }

    fn try_recv_frame(&mut self) -> Option<Vec<u8>> {
        self.inbound.borrow_mut().pop_front()
    }
}

#[test]
fn a_split_commit_a_full_queue_interrupts_arrives_whole() {
    let server = open(0xc1, AuthorSubject::SYSTEM);
    let writer_author = AuthorSubject::for_test_bytes([0x52; 16]);
    let writer = open(0x52, writer_author);

    let to_server: Queue = Rc::default();
    let to_writer: Queue = Rc::default();
    let room = Rc::new(RefCell::new(usize::MAX));
    let writer_side = NarrowWire {
        outbound: Rc::clone(&to_server),
        inbound: Rc::clone(&to_writer),
        room: Rc::clone(&room),
    };
    let server_side = GatedWire {
        outbound: to_writer,
        inbound: to_server,
        released: Rc::new(RefCell::new(usize::MAX)),
    };
    block_on(writer.connect_upstream(Box::new(WireTransportAdapter::current(writer_side))));
    server.accept_subscriber(
        Box::new(WireTransportAdapter::current(server_side)),
        writer_author,
    );
    for _ in 0..20 {
        block_on(writer.tick()).unwrap();
        block_on(server.tick()).unwrap();
    }

    // One commit of about 3 MB: more than one 2 MiB frame carries, so it
    // leaves in parts.
    let tx = OpenTransactionId::new();
    block_on(writer.begin_mergeable(tx)).unwrap();
    let body = "x".repeat(BODY);
    for _ in 0..ROWS {
        let values = BTreeMap::from([("body".to_owned(), Value::String(body.clone()))]);
        block_on(
            writer
                .tx_ref(tx)
                .insert("notes", values, Default::default()),
        )
        .unwrap();
    }
    block_on(writer.commit_mergeable_handle(tx)).unwrap();

    // The queue takes one frame per tick: the commit's first part goes
    // out, the rest are refused and must follow, not restart it.
    for _ in 0..50 {
        *room.borrow_mut() = 1;
        block_on(writer.tick()).unwrap();
        block_on(server.tick()).unwrap();
        if visible(&server) == ROWS {
            return;
        }
    }
    panic!("the server has {} of {ROWS} rows", visible(&server));
}

/// A writer linked to a fresh server through a [`NarrowWire`] with `room`.
struct Narrow {
    server: Db,
    writer: Db,
    room: Rc<RefCell<usize>>,
    subscriber: jazz::db::SubscriberId,
}

fn narrow(server_node: u8, writer_node: u8) -> Narrow {
    let server = open(server_node, AuthorSubject::SYSTEM);
    let writer_author = AuthorSubject::for_test_bytes([writer_node; 16]);
    let writer = open(writer_node, writer_author);
    let room = Rc::new(RefCell::new(usize::MAX));
    let subscriber = link(&server, &writer, writer_author, &room);
    for _ in 0..20 {
        block_on(writer.tick()).unwrap();
        block_on(server.tick()).unwrap();
    }
    Narrow {
        server,
        writer,
        room,
        subscriber,
    }
}

/// A new connection between `writer` and `server`: fresh queues, a fresh
/// adapter and joiner on each side.
fn link(
    server: &Db,
    writer: &Db,
    writer_author: AuthorSubject,
    room: &Rc<RefCell<usize>>,
) -> jazz::db::SubscriberId {
    let to_server: Queue = Rc::default();
    let to_writer: Queue = Rc::default();
    block_on(
        writer.connect_upstream(Box::new(WireTransportAdapter::current(NarrowWire {
            outbound: Rc::clone(&to_server),
            inbound: Rc::clone(&to_writer),
            room: Rc::clone(room),
        }))),
    );
    server.accept_subscriber(
        Box::new(WireTransportAdapter::current(GatedWire {
            outbound: to_writer,
            inbound: to_server,
            released: Rc::new(RefCell::new(usize::MAX)),
        })),
        writer_author,
    )
}

fn commit_notes(writer: &Db, rows: usize) {
    let tx = OpenTransactionId::new();
    block_on(writer.begin_mergeable(tx)).unwrap();
    let body = "x".repeat(BODY);
    for _ in 0..rows {
        let values = BTreeMap::from([("body".to_owned(), Value::String(body.clone()))]);
        block_on(
            writer
                .tx_ref(tx)
                .insert("notes", values, Default::default()),
        )
        .unwrap();
    }
    block_on(writer.commit_mergeable_handle(tx)).unwrap();
}

/// A commit in three parts, with a second commit queued behind it: every
/// part drains in order as the queue makes room, and the commit behind
/// arrives after it, whole.
#[test]
fn a_commit_in_three_parts_drains_ahead_of_the_commit_behind_it() {
    let link = narrow(0xc2, 0x53);
    commit_notes(&link.writer, 160);
    commit_notes(&link.writer, 1);
    let mut seen = Vec::new();
    for _ in 0..80 {
        *link.room.borrow_mut() = 1;
        block_on(link.writer.tick()).unwrap();
        block_on(link.server.tick()).unwrap();
        let count = visible(&link.server);
        seen.push(count);
        assert!(
            [0, 160, 161].contains(&count),
            "the server saw {count} rows (per tick: {seen:?})"
        );
        if count == 161 {
            return;
        }
    }
    panic!("the commits never arrived: {seen:?}");
}

/// A connection lost with a split commit's later parts unsent: the new
/// connection sends the commit again, from its first part, and the server
/// applies it once.
#[test]
fn a_split_commit_cut_off_by_a_disconnect_arrives_whole_on_the_next_connection() {
    let link_ = narrow(0xc3, 0x54);
    commit_notes(&link_.writer, ROWS);
    *link_.room.borrow_mut() = 1;
    block_on(link_.writer.tick()).unwrap();
    block_on(link_.server.tick()).unwrap();
    assert_eq!(visible(&link_.server), 0, "only the first part went out");
    link_.server.disconnect_subscriber(link_.subscriber);
    *link_.room.borrow_mut() = usize::MAX;
    link(
        &link_.server,
        &link_.writer,
        AuthorSubject::for_test_bytes([0x54; 16]),
        &link_.room,
    );
    for _ in 0..20 {
        block_on(link_.writer.tick()).unwrap();
        block_on(link_.server.tick()).unwrap();
        if visible(&link_.server) == ROWS {
            return;
        }
    }
    panic!("the server has {} of {ROWS} rows", visible(&link_.server));
}

/// An untrusted writer's commit over what its server joins is rejected for
/// good, here, rather than ending the session on every resend: the writer
/// rolls it back and later commits still sync.
#[test]
fn an_untrusted_commit_over_the_session_budget_is_rejected_without_sending() {
    let link = narrow(0xc4, 0x55);
    // About 9 MB: over the 8 MiB an untrusted session may send.
    commit_notes(&link.writer, 300);
    for _ in 0..10 {
        block_on(link.writer.tick()).unwrap();
        block_on(link.server.tick()).unwrap();
    }
    assert_eq!(
        visible(&link.writer),
        0,
        "the refused commit is rolled back"
    );
    assert_eq!(visible(&link.server), 0);
    commit_notes(&link.writer, 1);
    for _ in 0..10 {
        block_on(link.writer.tick()).unwrap();
        block_on(link.server.tick()).unwrap();
    }
    assert_eq!(visible(&link.server), 1, "the session survives the refusal");
}
