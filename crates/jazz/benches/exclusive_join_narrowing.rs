//! Payload and contention of exclusive reads through a join, which record the
//! joined source as the narrowed read of the rows the join could have
//! consulted (garden-co/jazz#3694). Before narrowed reads, the joined source
//! was recorded as its whole table: at 1,000 joined rows that downloaded
//! 1,741,824 bytes for the read, uploaded 48,701 bytes to commit, and 7 of 8
//! concurrent redemptions conflicted.
//!
//! A Core shell serves clients over queued wire transports. The owner seeds
//! `JOINED_ROWS` invites, each already redeemed by one member, plus the
//! invites the benchmark redeems. A redemption reads
//! `invites where code = X join members on code` inside an exclusive
//! transaction, inserts a member and commits.
//!
//! ```text
//! cargo bench -p jazz --features testing --bench exclusive_join_narrowing
//! ```
//!
//! The default run exposes one wall-time redemption to Divan. Set
//! `JAZZ_EXCLUSIVE_NARROWING_RECEIPT=1` to print a JSON receipt instead: the
//! bytes the redeeming client downloads for its exclusive read, the bytes it
//! uploads to commit, and how many of `CONCURRENT` simultaneous redemptions
//! of different invites conflict.

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::pin::pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

use jazz::db::{
    Db, DbConfig, DbIdentity, ExclusiveTxOps, ReadOpts, WireTransportAdapter, block_on,
};
use jazz::groove::records::Value;
use jazz::groove::storage::TestStorage;
use jazz::ids::{AuthorSubject, NodeUuid};
use jazz::query::{Query, col, eq, lit};
use jazz::schema::JazzSchema;
use jazz::serving::{InMemoryServerShell, InMemoryServerShellConfig, NodeRole, ServerSession};
use jazz::tools::{ColumnType, OpenTransactionId, SchemaBuilder, TableSchemaBuilder};
use jazz::tx::{DurabilityTier, TxId};
use jazz::wire::{TransportError, WireTransport};
use serde_json::json;

/// Invites redeemed before the measured redemption: the joined `members`
/// table holds one row per invite, none of them for the redeemed invite.
const JOINED_ROWS: usize = 1_000;
/// Simultaneous redemptions of different invites in the contention lane.
const CONCURRENT: usize = 8;
const MAX_TURNS: usize = 400;

fn main() {
    jazz_benchmark_guard::refuse_contaminated_measurement();
    if std::env::var_os("JAZZ_EXCLUSIVE_NARROWING_RECEIPT").is_some() {
        run_receipt();
        return;
    }
    divan::main();
}

#[divan::bench(sample_count = 10, sample_size = 1)]
fn exclusive_join_redemption(bencher: divan::Bencher<'_, '_>) {
    bencher
        .with_inputs(|| {
            let net = Net::new(2);
            net.seed(JOINED_ROWS, &["target"]);
            net.hold_invite(1, "target");
            net
        })
        .bench_local_values(|net| {
            let redemption = net.begin_redemption(1, "target");
            divan::black_box(net.finish_redemption(redemption))
        });
}

fn run_receipt() {
    let net = Net::new(2);
    net.seed(JOINED_ROWS, &["target"]);
    net.hold_invite(1, "target");
    net.take_bytes(1);
    let redemption = net.begin_redemption(1, "target");
    let (read_down, _) = net.take_bytes(1);
    let joined = net.finish_redemption(redemption);
    let (_, commit_up) = net.take_bytes(1);
    assert!(joined, "an unredeemed invite is redeemed");

    let codes = (0..CONCURRENT)
        .map(|index| format!("concurrent-{index}"))
        .collect::<Vec<_>>();
    let code_refs = codes.iter().map(String::as_str).collect::<Vec<_>>();
    let net = Net::new(CONCURRENT + 1);
    net.seed(JOINED_ROWS, &code_refs);
    for (index, code) in codes.iter().enumerate() {
        net.hold_invite(index + 1, code);
    }
    let redemptions = codes
        .iter()
        .enumerate()
        .map(|(index, code)| net.begin_redemption(index + 1, code))
        .collect::<Vec<_>>();
    let conflicts = redemptions
        .into_iter()
        .map(|redemption| net.finish_redemption(redemption))
        .filter(|joined| !joined)
        .count();

    println!(
        "{}",
        json!({
            "bench": "exclusive_join_narrowing",
            "joined_rows": JOINED_ROWS,
            "read_download_bytes": read_down,
            "commit_upload_bytes": commit_up,
            "concurrent_redemptions": CONCURRENT,
            "conflicts": conflicts,
        })
    );
}

fn schema() -> JazzSchema {
    use jazz::tools::test_support::AllowAll;
    JazzSchema::new(
        &SchemaBuilder::new()
            .table(TableSchemaBuilder::new("invites").column("code", ColumnType::Text))
            .table(TableSchemaBuilder::new("members").column("code", ColumnType::Text))
            .allow_all()
            .build(),
    )
    .expect("benchmark schema compiles")
}

fn identity(byte: u8, author: AuthorSubject) -> DbIdentity {
    DbIdentity {
        node: NodeUuid::from_bytes([byte; 16]),
        author,
    }
}

fn cells(code: &str) -> BTreeMap<String, Value> {
    BTreeMap::from([("code".to_owned(), Value::String(code.to_owned()))])
}

fn invite_query(code: &str) -> Query {
    Query::from("invites").filter(eq(col("code"), lit(code)))
}

fn redeemed_invite_query(code: &str) -> Query {
    invite_query(code).join_via_column("members", "code", "code", [])
}

#[derive(Clone, Default)]
struct QueuedWireTransport {
    queues: Rc<RefCell<(VecDeque<Vec<u8>>, VecDeque<Vec<u8>>)>>,
}

impl WireTransport for QueuedWireTransport {
    fn send_frame(&mut self, frame: Vec<u8>) -> Result<(), TransportError> {
        self.queues.borrow_mut().1.push_back(frame);
        Ok(())
    }

    fn try_recv_frame(&mut self) -> Option<Vec<u8>> {
        self.queues.borrow_mut().0.pop_front()
    }
}

struct Client {
    db: Db,
    wire: QueuedWireTransport,
    session: ServerSession,
    /// Bytes delivered to and sent by this client since the last
    /// [`Net::take_bytes`].
    down: Cell<usize>,
    up: Cell<usize>,
}

/// One Core and its connected clients, pumped in lockstep. Client 0 owns the
/// seeded data; the others redeem.
struct Net {
    core: RefCell<InMemoryServerShell>,
    clients: Vec<Client>,
}

/// An exclusive redemption whose reads are done and whose commit is not.
struct Redemption {
    client: usize,
    open: OpenTransactionId,
    code: String,
}

impl Net {
    fn new(clients: usize) -> Self {
        let schema = schema();
        let mut core = InMemoryServerShell::start(
            InMemoryServerShellConfig::new(schema.clone(), identity(0xc0, AuthorSubject::SYSTEM))
                .with_role(NodeRole::Core),
        )
        .unwrap();
        let clients = (0..clients)
            .map(|index| {
                let byte = 0x10 + index as u8;
                let author = AuthorSubject::for_test_bytes([byte; 16]);
                let cfs = schema.column_families();
                let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
                let db = block_on(Db::open(DbConfig::new(
                    schema.clone(),
                    TestStorage::new(&refs),
                    identity(byte, author),
                )))
                .unwrap();
                let wire = QueuedWireTransport::default();
                block_on(
                    db.connect_upstream(Box::new(WireTransportAdapter::current(wire.clone()))),
                );
                let session = core.accept_subscriber_session(author).unwrap();
                Client {
                    db,
                    wire,
                    session,
                    down: Cell::new(0),
                    up: Cell::new(0),
                }
            })
            .collect();
        Self {
            core: RefCell::new(core),
            clients,
        }
    }

    fn db(&self, client: usize) -> &Db {
        &self.clients[client].db
    }

    fn pump(&self) {
        let mut core = self.core.borrow_mut();
        for client in &self.clients {
            block_on(client.db.tick()).unwrap();
            let outbound = client
                .wire
                .queues
                .borrow_mut()
                .1
                .drain(..)
                .collect::<Vec<_>>();
            client
                .up
                .set(client.up.get() + outbound.iter().map(Vec::len).sum::<usize>());
            core.receive_frames(client.session, outbound).unwrap();
        }
        core.tick().unwrap();
        for client in &self.clients {
            for frame in core.take_frames(client.session).unwrap() {
                let frame: Vec<u8> = frame.into();
                client.down.set(client.down.get() + frame.len());
                client.wire.queues.borrow_mut().0.push_back(frame);
            }
            block_on(client.db.tick()).unwrap();
        }
    }

    fn drive<T>(&self, future: impl Future<Output = T>) -> T {
        let mut future = pin!(future);
        let mut context = Context::from_waker(Waker::noop());
        for _ in 0..MAX_TURNS {
            if let Poll::Ready(output) = future.as_mut().poll(&mut context) {
                return output;
            }
            self.pump();
        }
        panic!("operation did not complete within {MAX_TURNS} turns");
    }

    /// `(downloaded, uploaded)` bytes of `client` since the last call.
    fn take_bytes(&self, client: usize) -> (usize, usize) {
        let client = &self.clients[client];
        (client.down.replace(0), client.up.replace(0))
    }

    fn read(
        &self,
        client: usize,
        query: &Query,
        tier: DurabilityTier,
        open_tx: Option<OpenTransactionId>,
    ) -> usize {
        let db = self.db(client);
        let bytes = postcard::to_allocvec(query).unwrap();
        let read = db.all_serialized_query(
            &bytes,
            ReadOpts {
                tier,
                ..ReadOpts::default()
            },
            open_tx,
            None,
            None,
            tier >= DurabilityTier::Global,
            || false,
            |attachment| db.detach_query(attachment),
        );
        match self.drive(read).expect("read") {
            jazz::db::SerializedReadResult::Rows(rows) => rows.len(),
            jazz::db::SerializedReadResult::Relation(snapshot) => snapshot
                .rows
                .iter()
                .filter(|row| row.table() == query.table)
                .count(),
        }
    }

    fn settle(&self, client: usize, tx_id: TxId) -> bool {
        self.drive(
            self.db(client)
                .wait_for_transaction(tx_id, DurabilityTier::Global),
        )
        .is_ok()
    }

    /// Seed `joined_rows` redeemed invites and the unredeemed invites `open`,
    /// settling each write before the next as the application would.
    fn seed(&self, joined_rows: usize, open: &[&str]) {
        let redeemed = (0..joined_rows)
            .flat_map(|index| {
                let code = format!("redeemed-{index}");
                [("invites", code.clone()), ("members", code)]
            })
            .collect::<Vec<_>>();
        let open = open.iter().map(|code| ("invites", (*code).to_owned()));
        for (table, code) in redeemed.into_iter().chain(open) {
            let write =
                block_on(self.db(0).insert(table, cells(&code), Default::default())).unwrap();
            assert!(self.settle(0, write.mergeable_tx_id()), "seed settles");
        }
    }

    /// Let `client` hold the invite it will redeem, so its redemption does
    /// not depend on hydrating the root.
    fn hold_invite(&self, client: usize, code: &str) {
        assert_eq!(
            self.read(client, &invite_query(code), DurabilityTier::Global, None),
            1
        );
    }

    /// Open an exclusive transaction on `client` and check that `code` is a
    /// live invite nobody has redeemed.
    fn begin_redemption(&self, client: usize, code: &str) -> Redemption {
        let open = OpenTransactionId::new();
        block_on(self.db(client).begin_exclusive(open)).unwrap();
        assert_eq!(
            self.read(
                client,
                &invite_query(code),
                DurabilityTier::Local,
                Some(open)
            ),
            1
        );
        assert_eq!(
            self.read(
                client,
                &redeemed_invite_query(code),
                DurabilityTier::Local,
                Some(open)
            ),
            0
        );
        Redemption {
            client,
            open,
            code: code.to_owned(),
        }
    }

    /// Redeem, commit and report whether the authority accepted the commit.
    fn finish_redemption(&self, redemption: Redemption) -> bool {
        let db = self.db(redemption.client);
        self.drive(db.exclusive_tx_ref(redemption.open).insert(
            "members",
            cells(&redemption.code),
            Default::default(),
        ))
        .unwrap();
        let Ok(tx_id) = self.drive(db.commit_exclusive_handle(redemption.open)) else {
            return false;
        };
        self.settle(redemption.client, tx_id)
    }
}
