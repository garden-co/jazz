//! Caught-up BandChat timeline resume receipt.
//!
//! A client that holds a settled message-history subscription reconnects to
//! the server. The client declares the rows it holds; the server ships only
//! what is missing or stale, which for a caught-up client is nothing.

use std::cell::RefCell;
use std::collections::{BTreeMap, VecDeque};
use std::rc::Rc;

use jazz::db::{
    Db, DbConfig, DbIdentity, ReadOpts, SubscriberId, SubscriptionStream, SyncMessage, Transport,
    Upstream, block_on,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::Query;
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::test_support::allow_all_policies;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
use jazz::wire::TransportError;

const TABLE: &str = "messages";

/// The externally observable result of attaching a fully caught-up peer.
#[derive(Debug, PartialEq, Eq)]
pub struct FastResumeReceipt {
    /// Row images the server shipped on reconnect.
    pub rows_shipped: usize,
    /// Rows the client holds after the resume.
    pub rows_held: usize,
}

impl FastResumeReceipt {
    pub fn ships_no_rows(&self) -> bool {
        self.rows_shipped == 0
    }
}

/// A seeded BandChat message history and a client subscribed to all of it.
///
/// Construction represents established server state and is deliberately outside
/// the benchmark timing closure. `caught_up_fast_resume` measures only
/// reconnecting a client which has already observed the whole history.
pub struct FastResumeFixture {
    server: Db,
    client: Db,
    upstream: Upstream,
    subscriber: SubscriberId,
    _stream: SubscriptionStream,
}

impl FastResumeFixture {
    pub fn new(message_count: usize) -> Self {
        assert!(message_count > 0, "fixture requires at least one message");
        let server = open(0);
        for index in 0..message_count {
            server
                .seed_settled_mergeable_for_bootstrap(
                    TABLE,
                    row(index),
                    AuthorSubject::SYSTEM,
                    message_cells(index),
                )
                .expect("seed BandChat message fixture");
        }
        let client = open(1);
        let (client_side, server_side, _) = pipe();
        let upstream = block_on(client.connect_upstream(client_side));
        let subscriber = server.accept_subscriber(server_side, AuthorSubject::SYSTEM);
        let prepared = client
            .prepare_query(&Query::from(TABLE))
            .expect("prepare BandChat message-history query");
        let stream = block_on(client.subscribe(&prepared, ReadOpts::default()))
            .expect("subscribe to BandChat message history");
        pump(&server, &client);
        Self {
            server,
            client,
            upstream,
            subscriber,
            _stream: stream,
        }
    }

    /// Reconnect the caught-up client and run the resume exchange to
    /// completion. Divan and CodSpeed measure that cost across fixture scales.
    pub fn caught_up_fast_resume(&mut self) -> FastResumeReceipt {
        assert!(self.client.detach_connection(&self.upstream));
        self.server.disconnect_subscriber(self.subscriber);
        let (client_side, server_side, shipped) = pipe();
        self.upstream = block_on(self.client.connect_upstream(client_side));
        self.subscriber = self
            .server
            .accept_subscriber(server_side, AuthorSubject::SYSTEM);
        pump(&self.server, &self.client);
        let prepared = self
            .client
            .prepare_query(&Query::from(TABLE))
            .expect("prepare held rows read");
        let rows_held = block_on(self.client.all(
            &prepared,
            ReadOpts::for_read_tier(jazz::db::ReadTier::LocalOnly),
        ))
        .expect("read held rows")
        .len();
        FastResumeReceipt {
            rows_shipped: *shipped.borrow(),
            rows_held,
        }
    }
}

fn open(node: u8) -> Db {
    let config = DbConfig::new(
        schema(),
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author: AuthorSubject::SYSTEM,
        },
    );
    if node == 0 {
        block_on(Db::open_history_complete(config))
    } else {
        block_on(Db::open(config))
    }
    .expect("open BandChat fast-resume fixture database")
}

fn pump(server: &Db, client: &Db) {
    for _ in 0..5 {
        block_on(client.tick()).expect("client tick");
        block_on(server.tick()).expect("server tick");
    }
    block_on(client.tick()).expect("client tick");
}

/// An in-memory duplex whose server side counts the row images it ships.
struct Pipe {
    outbound: Rc<RefCell<VecDeque<SyncMessage>>>,
    inbound: Rc<RefCell<VecDeque<SyncMessage>>>,
    shipped: Option<Rc<RefCell<usize>>>,
}

impl Transport for Pipe {
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
        if let (Some(count), SyncMessage::Rows { images }) = (&self.shipped, &message) {
            *count.borrow_mut() += images.len();
        }
        self.outbound.borrow_mut().push_back(message);
        Ok(())
    }

    fn try_recv(&mut self) -> Option<SyncMessage> {
        self.inbound.borrow_mut().pop_front()
    }
}

/// Client side, server side, and the server side's shipped-row count.
type Pipes = (Box<dyn Transport>, Box<dyn Transport>, Rc<RefCell<usize>>);

fn pipe() -> Pipes {
    let up = Rc::new(RefCell::new(VecDeque::new()));
    let down = Rc::new(RefCell::new(VecDeque::new()));
    let shipped = Rc::new(RefCell::new(0));
    (
        Box::new(Pipe {
            outbound: up.clone(),
            inbound: down.clone(),
            shipped: None,
        }),
        Box::new(Pipe {
            outbound: down,
            inbound: up,
            shipped: Some(shipped.clone()),
        }),
        shipped,
    )
}

fn schema() -> JazzSchema {
    let source = SchemaBuilder::new()
        .table(
            TableSchemaBuilder::new(TABLE)
                .column("body", ColumnType::Text)
                .column("sent_at", ColumnType::Timestamp)
                .index_only(["sent_at"])
                .policies(allow_all_policies()),
        )
        .build();
    JazzSchema::new(&source).expect("BandChat fast-resume schema compiles")
}

fn message_cells(index: usize) -> BTreeMap<String, Value> {
    BTreeMap::from([
        (
            "body".to_owned(),
            Value::String(format!("Message {index:06}")),
        ),
        ("sent_at".to_owned(), Value::U64(index as u64)),
    ])
}

fn row(index: usize) -> RowUuid {
    let mut bytes = [4; 16];
    bytes[..8].copy_from_slice(&(index as u64).to_le_bytes());
    RowUuid::from_bytes(bytes)
}
