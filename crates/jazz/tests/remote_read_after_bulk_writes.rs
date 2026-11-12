//! A Remote read right after a burst of writes (R62): the server answers
//! it after the commits staged before it, so it waits for all of them to
//! be applied and settled. Applying them costs the server time linear in
//! the burst, and the read's coverage budget measures progress through
//! them, not wall-clock time: it fails only once the server stalls.
//!
//! Every client here is a core `Db` connected to a server `Db` over an
//! in-memory transport, with ticks driven explicitly and a test clock, so
//! a slow server is a deterministic state rather than a race.

use crate::common;

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::pin::pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use jazz::binding_host::{CoverageBudget, REMOTE_READ_COVERAGE_BUDGET};
use jazz::db::{
    Db, DbConfig, DbIdentity, ErrorCode, InsertOptions, ReadOpts, ReadTier, SerializedReadResult,
    SyncMessage, Transport, TransportError, Upstream, block_on,
};
use jazz::ids::{AuthorSubject, NodeUuid};
use jazz::query::Query;
use jazz::records::Value;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};

use common::{allow_all_policies, compile_schema};

fn open(node: u8, author: AuthorSubject) -> Db {
    let schema = compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("docs")
                    .column("title", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .table(
                TableSchemaBuilder::new("notes")
                    .column("title", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .build(),
    );
    let identity = DbIdentity {
        node: NodeUuid::from_bytes([node; 16]),
        author,
    };
    block_on(Db::open(DbConfig::new(schema, identity))).unwrap()
}

fn alice_id() -> AuthorSubject {
    AuthorSubject::for_test_bytes([1; 16])
}

type Queue = Rc<RefCell<VecDeque<SyncMessage>>>;

/// One end of the link. The server's end takes in at most `allowance`
/// messages until it is refilled: a server working through a backlog.
struct End {
    out: Queue,
    inbox: Queue,
    allowance: Option<Rc<Cell<usize>>>,
}

impl Transport for End {
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
        self.out.borrow_mut().push_back(message);
        Ok(())
    }

    fn try_recv(&mut self) -> Option<SyncMessage> {
        if let Some(allowance) = &self.allowance {
            if allowance.get() == 0 {
                return None;
            }
            allowance.set(allowance.get() - 1);
        }
        self.inbox.borrow_mut().pop_front()
    }
}

struct Link {
    _upstream: Upstream,
    /// Messages the server takes in per turn.
    allowance: Rc<Cell<usize>>,
}

fn connect(server: &Db, client: &Db) -> Link {
    let up: Queue = Rc::default();
    let down: Queue = Rc::default();
    let allowance = Rc::new(Cell::new(usize::MAX));
    let upstream = block_on(client.connect_upstream(Box::new(End {
        out: up.clone(),
        inbox: down.clone(),
        allowance: None,
    })));
    server
        .accept_subscriber(
            Box::new(End {
                out: down,
                inbox: up,
                allowance: Some(allowance.clone()),
            }),
            alice_id(),
        )
        .unwrap();
    let link = Link {
        _upstream: upstream,
        allowance,
    };
    for _ in 0..4 {
        turn(server, client);
    }
    link
}

fn turn(server: &Db, client: &Db) {
    block_on(client.tick()).unwrap();
    block_on(server.tick()).unwrap();
    block_on(client.tick()).unwrap();
}

fn insert(db: &Db, n: usize) {
    block_on(db.insert(
        "docs",
        BTreeMap::from([("title".to_owned(), Value::String(format!("doc {n}")))]),
        InsertOptions::default(),
    ))
    .unwrap();
}

fn count(db: &Db, table: &str) -> usize {
    db.read(&db.prepare_query(&Query::from(table)).unwrap())
        .unwrap()
        .len()
}

/// A test clock: `now()` is `elapsed` past a fixed start.
struct Clock {
    start: web_time::Instant,
    elapsed: Cell<Duration>,
}

impl Clock {
    fn new() -> Rc<Self> {
        Rc::new(Self {
            start: web_time::Instant::now(),
            elapsed: Cell::new(Duration::ZERO),
        })
    }

    fn now(&self) -> web_time::Instant {
        self.start + self.elapsed.get()
    }

    fn advance(&self, by: Duration) {
        self.elapsed.set(self.elapsed.get() + by);
    }
}

/// Run a Remote read of `notes` on `client` under `expired`, one server
/// message per turn (none with no `server`: it is silent) and `per_turn`
/// of clock each: its rows, or its error, and the turns it took.
fn read_behind_backlog(
    server: Option<&Db>,
    client: &Db,
    link: &Link,
    clock: &Clock,
    per_turn: Duration,
    expired: &dyn Fn() -> bool,
) -> (Result<usize, ErrorCode>, usize) {
    let query = postcard::to_allocvec(&Query::from("notes")).unwrap();
    let opts = ReadOpts::for_read_tier(ReadTier::Remote);
    let mut read = pin!(client.all_serialized_query(&query, opts, None, None, Some(expired)));
    let mut context = Context::from_waker(Waker::noop());
    for turns in 0..1_000 {
        match read.as_mut().poll(&mut context) {
            Poll::Ready(Ok(SerializedReadResult::Rows(rows))) => return (Ok(rows.len()), turns),
            Poll::Ready(Ok(SerializedReadResult::Relation(_))) => panic!("notes is a row query"),
            Poll::Ready(Err(error)) => return (Err(error.code), turns),
            Poll::Pending => {}
        }
        link.allowance.set(1);
        match server {
            Some(server) => turn(server, client),
            None => block_on(client.tick()).unwrap(),
        }
        clock.advance(per_turn);
    }
    panic!("the read neither answered nor failed");
}

/// A server applying a burst of one writer's commits in one tick matches
/// the rows it sends back to the writer's commits (which settle them by
/// their fate) in one probe per row: before, each row scanned every
/// commit of the tick, so the server's pass over N inserts cost N², and a
/// backend's 50k inserts kept its first read waiting past its budget.
///
/// ```text
/// alice ──N inserts──► server (one tick) ──N fates + N rows──► alice
///                          └── N echo probes, not N²/2
/// ```
#[test]
fn a_tick_applying_a_burst_of_commits_matches_their_rows_once_each() {
    const WRITES: usize = 300;
    let server = open(0, AuthorSubject::SYSTEM);
    let alice = open(1, alice_id());
    let _link = connect(&server, &alice);
    let before = server.echo_probes();
    for n in 0..WRITES {
        insert(&alice, n);
    }
    turn(&server, &alice);
    let probes = server.echo_probes() - before;
    assert!(
        (1..=WRITES as u64).contains(&probes),
        "{probes} echo probes for {WRITES} commits"
    );
    // Each row still settles as alice wrote it, with no round trip.
    assert_eq!(alice.as_written_mismatches(), 0);
    assert_eq!(count(&alice, "docs"), WRITES);
    assert_eq!(count(&server, "docs"), WRITES);
}

/// A Remote read queued behind a burst of alice's writes waits while the
/// server works through them, however long that takes, as long as each
/// budget's worth of time settles one of them; a wall-clock budget of the
/// same length fails it as `NotObserved` partway through.
///
/// ```text
/// alice ──20 inserts, read──► server: one message per turn, 6 s each
///   progress budget (15 s, restarted per fate) ──► answered after ~126 s
///   wall-clock budget (15 s) ────────────────────► NotObserved at 18 s
/// ```
#[test]
fn a_remote_read_behind_a_burst_of_writes_waits_while_they_settle() {
    const WRITES: usize = 20;
    let per_turn = Duration::from_secs(6);

    // The budget restarts as each commit ahead of the read settles.
    let server = open(0, AuthorSubject::SYSTEM);
    let alice = Rc::new(open(1, alice_id()));
    let link = connect(&server, &alice);
    for n in 0..WRITES {
        insert(&alice, n);
    }
    let clock = Clock::new();
    let budget = {
        let clock = clock.clone();
        CoverageBudget::new(alice.clone(), REMOTE_READ_COVERAGE_BUDGET, move || {
            clock.now()
        })
    };
    let (rows, turns) =
        read_behind_backlog(Some(&server), &alice, &link, &clock, per_turn, &|| {
            budget.spent()
        });
    assert_eq!(rows, Ok(0));
    assert!(
        clock.elapsed.get() > REMOTE_READ_COVERAGE_BUDGET * 5,
        "the read waited {:?} over {turns} turns",
        clock.elapsed.get()
    );
    assert_eq!(count(&server, "docs"), WRITES);

    // The same backlog under a wall-clock budget fails it.
    let server = open(0, AuthorSubject::SYSTEM);
    let alice = open(1, alice_id());
    let link = connect(&server, &alice);
    for n in 0..WRITES {
        insert(&alice, n);
    }
    let clock = Clock::new();
    let (rows, _) = read_behind_backlog(Some(&server), &alice, &link, &clock, per_turn, &|| {
        clock.elapsed.get() >= REMOTE_READ_COVERAGE_BUDGET
    });
    assert_eq!(rows, Err(ErrorCode::NotObserved));
}

/// With nothing settling (the server silent) the budget is wall-clock:
/// a Remote read fails with `NotObserved` once it is spent, even with
/// writes queued ahead of it.
///
/// ```text
/// alice ──3 inserts, read──► server, silent ── 15 s ──► NotObserved
/// ```
#[test]
fn a_remote_read_fails_once_a_silent_server_spends_its_budget() {
    let server = open(0, AuthorSubject::SYSTEM);
    let alice = Rc::new(open(1, alice_id()));
    let link = connect(&server, &alice);
    for n in 0..3 {
        insert(&alice, n);
    }
    let clock = Clock::new();
    let budget = {
        let clock = clock.clone();
        CoverageBudget::new(alice.clone(), REMOTE_READ_COVERAGE_BUDGET, move || {
            clock.now()
        })
    };
    let (rows, turns) =
        read_behind_backlog(None, &alice, &link, &clock, Duration::from_secs(5), &|| {
            budget.spent()
        });
    assert_eq!(rows, Err(ErrorCode::NotObserved));
    assert_eq!(turns, 3, "spent 15 s after the read's first wait");
}
