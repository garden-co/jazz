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
                .mergeable_tx_ref(tx)
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
