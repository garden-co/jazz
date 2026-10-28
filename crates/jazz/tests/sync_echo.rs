//! A writer's own rows are not echoed back: when the authority's new
//! version of a row is exactly what the writer's commit produced there (its
//! digest in the commit's `expect`), the Fate names the row `held` and the
//! writer installs its own image at `through`, with the authority's stamps.
//! Anything else ships the image as before.

use crate::common;

use std::cell::RefCell;
use std::collections::{BTreeMap, VecDeque};
use std::rc::Rc;
use std::time::Duration;

use jazz::db::{
    Db, DbConfig, DbIdentity, InsertOptions, ReadOpts, SyncMessage, Transport, TransportError,
    Upstream, block_on,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::Query;
use jazz::records::Value;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};

use common::{allow_all_policies, compile_schema};

const COLUMNS: [&str; 6] = [
    "title",
    "body",
    "$createdAt",
    "$updatedAt",
    "$createdBy.identity.subject",
    "$updatedBy.identity.subject",
];

fn open(node: u8, author: AuthorSubject) -> Db {
    let docs = TableSchemaBuilder::new("docs")
        .column("title", ColumnType::Text)
        .column("body", ColumnType::Text)
        .policies(allow_all_policies());
    let config = DbConfig::new(
        compile_schema(&SchemaBuilder::new().table(docs).build()),
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author,
        },
    );
    block_on(Db::open(config)).unwrap()
}

fn user(n: u8) -> AuthorSubject {
    AuthorSubject::for_test_bytes([n; 16])
}

fn row(n: u8) -> RowUuid {
    RowUuid::from_bytes([n; 16])
}

fn cells(pairs: &[(&str, &str)]) -> BTreeMap<String, Value> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_owned(), Value::String((*v).to_owned())))
        .collect()
}

/// A duplex whose server side records what it sends down.
struct Pipe {
    out: Rc<RefCell<VecDeque<SyncMessage>>>,
    inbound: Rc<RefCell<VecDeque<SyncMessage>>>,
    log: Option<Rc<RefCell<Vec<SyncMessage>>>>,
}

impl Transport for Pipe {
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
        if let Some(log) = &self.log {
            log.borrow_mut().push(message.clone());
        }
        self.out.borrow_mut().push_back(message);
        Ok(())
    }

    fn try_recv(&mut self) -> Option<SyncMessage> {
        self.inbound.borrow_mut().pop_front()
    }
}

type Down = Rc<RefCell<Vec<SyncMessage>>>;

fn connect(server: &Db, client: &Db, identity: AuthorSubject) -> (Upstream, Down) {
    let up = Rc::new(RefCell::new(VecDeque::new()));
    let down = Rc::new(RefCell::new(VecDeque::new()));
    let log = Rc::new(RefCell::new(Vec::new()));
    let upstream = block_on(client.connect_upstream(Box::new(Pipe {
        out: up.clone(),
        inbound: down.clone(),
        log: None,
    })));
    server.accept_subscriber(
        Box::new(Pipe {
            out: down,
            inbound: up,
            log: Some(log.clone()),
        }),
        identity,
    );
    (upstream, log)
}

fn subscribe(client: &Db) -> jazz::db::SubscriptionStream {
    let prepared = client
        .prepare_query(&Query::from("docs").select(COLUMNS))
        .unwrap();
    block_on(client.subscribe(&prepared, ReadOpts::default())).unwrap()
}

fn pump(server: &Db, clients: &[&Db]) {
    for _ in 0..6 {
        for client in clients {
            block_on(client.tick()).unwrap();
        }
        block_on(server.tick()).unwrap();
    }
    for client in clients {
        block_on(client.tick()).unwrap();
    }
}

/// Every row with its version and cells, provenance included.
fn snapshot(db: &Db) -> Vec<String> {
    let prepared = db
        .prepare_query(&Query::from("docs").select(COLUMNS))
        .unwrap();
    block_on(db.all(&prepared, ReadOpts::default()))
        .unwrap()
        .iter()
        .map(|r| {
            let cells: Vec<String> = r.cells().map(|(n, v)| format!("{n}={v:?}")).collect();
            format!("{:?} @{} {}", r.row_uuid(), r.seq(), cells.join(" "))
        })
        .collect()
}

/// The images, and the held rows of each Fate, a log saw.
fn shipped(log: &Down) -> (usize, Vec<usize>) {
    let mut images = 0;
    let mut held = Vec::new();
    for message in log.borrow().iter() {
        match message {
            SyncMessage::Rows { images: i } => images += i.len(),
            SyncMessage::Fate { held: h, .. } => held.push(h.len()),
            _ => {}
        }
    }
    (images, held)
}

/// Write on `client` and land it at the server a little later, so the
/// authority's stamps differ from the writer's.
fn write_late(server: &Db, client: &Db, write: impl FnOnce()) {
    write();
    block_on(client.tick()).unwrap();
    std::thread::sleep(Duration::from_millis(3));
    block_on(server.tick()).unwrap();
}

#[test]
fn one_keystroke_sends_only_fate_and_settled() {
    let server = open(0, AuthorSubject::SYSTEM);
    let client = open(1, user(1));
    let (_upstream, down) = connect(&server, &client, user(1));
    let _live = subscribe(&client);
    pump(&server, &[&client]);
    write_late(&server, &client, || {
        block_on(client.insert(
            "docs",
            cells(&[("title", "Doc"), ("body", "h")]),
            InsertOptions {
                row_id: Some(row(1)),
                ..Default::default()
            },
        ))
        .unwrap();
    });
    pump(&server, &[&client]);
    assert_eq!(shipped(&down), (0, vec![1]), "a create is held too");
    let created = snapshot(&client);
    assert_eq!(created, snapshot(&server));

    down.borrow_mut().clear();
    write_late(&server, &client, || {
        block_on(client.update("docs", row(1), cells(&[("body", "he")]), Default::default()))
            .unwrap();
    });
    pump(&server, &[&client]);
    let kinds: Vec<&str> = down
        .borrow()
        .iter()
        .map(|m| match m {
            SyncMessage::Fate { held, .. } if held.len() == 1 => "fate+held",
            SyncMessage::Settled { .. } => "settled",
            _ => "other",
        })
        .collect();
    assert_eq!(kinds[0], "fate+held", "{kinds:?}");
    assert!(kinds[1..].iter().all(|k| *k == "settled"), "{kinds:?}");
    let typed = snapshot(&client);
    assert_eq!(typed, snapshot(&server), "the replica holds the version");
    assert_ne!(typed, created);

    // The client's copy is the settled version: a reconnect ships nothing.
    assert!(client.detach_connection(&_upstream));
    let (_upstream, down) = connect(&server, &client, user(1));
    pump(&server, &[&client]);
    assert_eq!(shipped(&down).0, 0, "the held row was already current");
    assert_eq!(snapshot(&client), snapshot(&server));
}

#[test]
fn a_concurrent_write_still_ships_the_image() {
    let server = open(0, AuthorSubject::SYSTEM);
    let a = open(1, user(1));
    let b = open(2, user(2));
    let (_a_up, a_down) = connect(&server, &a, user(1));
    let (_b_up, b_down) = connect(&server, &b, user(2));
    let (_a_live, _b_live) = (subscribe(&a), subscribe(&b));
    block_on(a.insert(
        "docs",
        cells(&[("title", "Doc"), ("body", "")]),
        InsertOptions {
            row_id: Some(row(1)),
            ..Default::default()
        },
    ))
    .unwrap();
    pump(&server, &[&a, &b]);
    a_down.borrow_mut().clear();
    b_down.borrow_mut().clear();

    // Both write before the authority applies either: a's version is
    // superseded in the same tick, b's merges onto a's title.
    block_on(a.update("docs", row(1), cells(&[("title", "A")]), Default::default())).unwrap();
    block_on(b.update("docs", row(1), cells(&[("body", "B")]), Default::default())).unwrap();
    block_on(a.tick()).unwrap();
    block_on(b.tick()).unwrap();
    block_on(server.tick()).unwrap();
    pump(&server, &[&a, &b]);

    let (a_images, a_held) = shipped(&a_down);
    let (b_images, b_held) = shipped(&b_down);
    assert!(a_images > 0 && a_held == vec![0], "{a_images} {a_held:?}");
    assert!(b_images > 0 && b_held == vec![0], "{b_images} {b_held:?}");
    let settled = snapshot(&server);
    assert!(
        settled[0].contains("\"A\"") && settled[0].contains("\"B\""),
        "{settled:?}"
    );
    assert_eq!(snapshot(&a), settled);
    assert_eq!(snapshot(&b), settled);
}

#[test]
fn a_write_landing_between_ships_the_image() {
    let server = open(0, AuthorSubject::SYSTEM);
    let a = open(1, user(1));
    let b = open(2, user(2));
    let (_a_up, a_down) = connect(&server, &a, user(1));
    let (_b_up, _b_down) = connect(&server, &b, user(2));
    let (_a_live, _b_live) = (subscribe(&a), subscribe(&b));
    block_on(a.insert(
        "docs",
        cells(&[("title", "Doc"), ("body", "")]),
        InsertOptions {
            row_id: Some(row(1)),
            ..Default::default()
        },
    ))
    .unwrap();
    pump(&server, &[&a, &b]);
    a_down.borrow_mut().clear();

    // b's write lands first; a writes before hearing of it.
    block_on(b.update("docs", row(1), cells(&[("body", "B")]), Default::default())).unwrap();
    block_on(b.tick()).unwrap();
    block_on(server.tick()).unwrap();
    block_on(a.update("docs", row(1), cells(&[("title", "A")]), Default::default())).unwrap();
    pump(&server, &[&a, &b]);

    let (images, held) = shipped(&a_down);
    assert!(images > 0, "a's base was stale");
    assert_eq!(held, vec![0]);
    assert_eq!(snapshot(&a), snapshot(&server));
    assert_eq!(snapshot(&b), snapshot(&server));
}

#[test]
fn a_server_filled_column_still_ships() {
    // The authority stamps the connection's identity as the writer, not
    // the one the replica stamped locally.
    let server = open(0, AuthorSubject::SYSTEM);
    let client = open(1, user(1));
    let (_upstream, down) = connect(&server, &client, user(9));
    let _live = subscribe(&client);
    pump(&server, &[&client]);
    block_on(client.insert(
        "docs",
        cells(&[("title", "Doc"), ("body", "")]),
        InsertOptions {
            row_id: Some(row(1)),
            ..Default::default()
        },
    ))
    .unwrap();
    pump(&server, &[&client]);
    block_on(client.update("docs", row(1), cells(&[("body", "x")]), Default::default())).unwrap();
    pump(&server, &[&client]);

    let (images, held) = shipped(&down);
    assert_eq!(held, vec![0, 0], "neither write matched");
    assert!(images >= 2, "{images}");
    let settled = snapshot(&server);
    assert!(settled[0].contains(&user(9).principal_parts().1));
    assert_eq!(snapshot(&client), settled);
}

#[test]
fn a_lost_fate_resumes_correctly() {
    let server = open(0, AuthorSubject::SYSTEM);
    let client = open(1, user(1));
    let (upstream, _down) = connect(&server, &client, user(1));
    let _live = subscribe(&client);
    block_on(client.insert(
        "docs",
        cells(&[("title", "Doc"), ("body", "")]),
        InsertOptions {
            row_id: Some(row(1)),
            ..Default::default()
        },
    ))
    .unwrap();
    pump(&server, &[&client]);

    // The authority applies the write, but its Fate never arrives.
    write_late(&server, &client, || {
        block_on(client.update("docs", row(1), cells(&[("body", "x")]), Default::default()))
            .unwrap();
    });
    assert!(client.detach_connection(&upstream));
    let (_upstream, down) = connect(&server, &client, user(1));
    pump(&server, &[&client]);

    // The resend gets the recorded fate, which holds nothing: the image
    // ships.
    let (images, held) = shipped(&down);
    assert_eq!(held, vec![0]);
    assert!(images > 0);
    assert_eq!(snapshot(&client), snapshot(&server));

    // And later writes are held again.
    down.borrow_mut().clear();
    block_on(client.update("docs", row(1), cells(&[("body", "xy")]), Default::default())).unwrap();
    pump(&server, &[&client]);
    assert_eq!(shipped(&down), (0, vec![1]));
    assert_eq!(snapshot(&client), snapshot(&server));
}

/// Each write's explicit timestamp reaches the authority as its own: a
/// transaction that backfills rows a millisecond apart keeps them apart
/// there, rather than every row taking the first write's time.
#[test]
fn explicit_timestamps_reach_the_authority_per_write() {
    let server = open(0, AuthorSubject::SYSTEM);
    let client = open(1, user(1));
    let (_upstream, _down) = connect(&server, &client, user(1));
    let _live = subscribe(&client);
    pump(&server, &[&client]);
    let batch = jazz::tools::OpenTransactionId::new();
    block_on(client.begin_mergeable(batch)).unwrap();
    for (n, at) in [(1u8, 1_704_067_200_001u64), (2, 1_704_067_200_002)] {
        block_on(client.tx_ref(batch).insert(
            "docs",
            cells(&[("title", "Doc"), ("body", "")]),
            InsertOptions {
                row_id: Some(row(n)),
                updated_at_ms: Some(at),
                ..Default::default()
            },
        ))
        .unwrap();
    }
    block_on(client.commit_mergeable_handle(batch)).unwrap();
    pump(&server, &[&client]);
    let created: Vec<bool> = snapshot(&server)
        .iter()
        .zip(["1704067200001", "1704067200002"])
        .map(|(row, at)| row.contains(at))
        .collect();
    assert_eq!(created, [true, true], "{:?}", snapshot(&server));
    assert_eq!(snapshot(&client), snapshot(&server));
}

/// Hold actual authority receipts while delivering one other message per turn.
/// This transport control is needed because application APIs cannot split a
/// server tick between its row evidence and receipt.
struct Fragmented {
    wire: Pipe,
    hold_writes: Rc<std::cell::Cell<bool>>,
    allow: Rc<std::cell::Cell<bool>>,
    hold: Rc<std::cell::Cell<bool>>,
    receipts: Rc<RefCell<VecDeque<SyncMessage>>>,
}

impl Transport for Fragmented {
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
        if self.hold_writes.get() && matches!(message, SyncMessage::Commit { .. }) {
            return Err(TransportError::Backpressure);
        }
        self.wire.send(message)
    }

    fn try_recv(&mut self) -> Option<SyncMessage> {
        if !self.allow.replace(false) {
            return None;
        }
        if !self.hold.get()
            && let Some(receipt) = self.receipts.borrow_mut().pop_front()
        {
            return Some(receipt);
        }
        while let Some(message) = self.wire.try_recv() {
            if self.hold.get() && matches!(message, SyncMessage::Fate { .. }) {
                self.receipts.borrow_mut().push_back(message);
            } else {
                return Some(message);
            }
        }
        None
    }
}

fn fragmented_write(action: &str, later_pending: bool) {
    use jazz::db::ReadTier;
    use jazz::tx::DurabilityTier;
    use std::cell::Cell;
    use std::future::Future;
    use std::task::{Context, Poll, Waker};

    let server = open(0, AuthorSubject::SYSTEM);
    server.set_authority();
    let alice = open(1, user(1));
    let up = Rc::new(RefCell::new(VecDeque::new()));
    let down = Rc::new(RefCell::new(VecDeque::new()));
    let allow = Rc::new(Cell::new(false));
    let hold = Rc::new(Cell::new(false));
    let hold_writes = Rc::new(Cell::new(false));
    let receipts = Rc::new(RefCell::new(VecDeque::new()));
    let _upstream = block_on(alice.connect_upstream(Box::new(Fragmented {
        wire: Pipe {
            out: up.clone(),
            inbound: down.clone(),
            log: None,
        },
        allow: allow.clone(),
        hold_writes: hold_writes.clone(),
        hold: hold.clone(),
        receipts: receipts.clone(),
    })));
    // Alice's authenticated identity replaces her local stamp, requiring
    // a row image rather than the identical-row optimisation in Fate.
    server.accept_subscriber(
        Box::new(Pipe {
            out: down,
            inbound: up,
            log: None,
        }),
        user(9),
    );
    let _live = subscribe(&alice);
    let pump = || {
        for _ in 0..40 {
            allow.set(true);
            block_on(alice.tick()).unwrap();
            block_on(server.tick()).unwrap();
        }
    };
    pump();
    let insert = || {
        block_on(alice.insert(
            "docs",
            cells(&[("title", "Doc"), ("body", "original")]),
            InsertOptions {
                row_id: Some(row(1)),
                ..Default::default()
            },
        ))
        .unwrap()
        .mergeable_tx_id()
    };
    if action != "insert" {
        insert();
        pump();
    }
    hold.set(true);
    let tx = match action {
        "insert" => insert(),
        "update" => block_on(alice.update(
            "docs",
            row(1),
            cells(&[("body", "updated")]),
            Default::default(),
        ))
        .unwrap()
        .mergeable_tx_id(),
        "delete" => block_on(alice.delete("docs", row(1), Default::default()))
            .unwrap()
            .mergeable_tx_id(),
        _ => unreachable!(),
    };
    pump();
    assert_eq!(
        receipts.borrow().len(),
        1,
        "held the real receipt for {action}"
    );
    let mut wait = std::pin::pin!(alice.wait_for_transaction(tx, DurabilityTier::Global));
    assert!(matches!(
        wait.as_mut().poll(&mut Context::from_waker(Waker::noop())),
        Poll::Pending
    ));
    let accepted = |db: &Db| {
        let query = db
            .prepare_query(&Query::from("docs").select(COLUMNS))
            .unwrap();
        block_on(db.all(
            &query,
            ReadOpts {
                tier: ReadTier::LocalOnly,
                read_view: jazz::protocol::ReadViewSpec {
                    include_deleted: true,
                    ..Default::default()
                },
                ..Default::default()
            },
        ))
        .unwrap()
        .into_iter()
        .map(|r| {
            (
                r.row_uuid(),
                r.seq(),
                r.cells()
                    .map(|(k, v)| (k.to_owned(), v.cloned()))
                    .collect::<Vec<_>>(),
            )
        })
        .collect::<Vec<_>>()
    };
    assert_eq!(
        accepted(&alice),
        accepted(&server),
        "{action} must not overlay an accepted image again"
    );
    assert_eq!(snapshot(&alice).is_empty(), action == "delete");
    if later_pending {
        hold_writes.set(true);
        let later = block_on(alice.update(
            "docs",
            row(1),
            cells(&[("body", "later")]),
            Default::default(),
        ))
        .unwrap()
        .mergeable_tx_id();
        block_on(server.update(
            "docs",
            row(1),
            cells(&[("body", "another writer")]),
            Default::default(),
        ))
        .unwrap();
        pump();
        assert!(
            snapshot(&alice)[0].contains("\"later\""),
            "the later pending write stays visible"
        );
        assert!(snapshot(&server)[0].contains("another writer"));
        let mut later_wait =
            std::pin::pin!(alice.wait_for_transaction(later, DurabilityTier::Global));
        assert!(matches!(
            later_wait
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop())),
            Poll::Pending
        ));
        hold_writes.set(false);
    }
    hold.set(false);
    pump();
    assert!(matches!(
        wait.as_mut().poll(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(Ok(_))
    ));
    assert_eq!(accepted(&alice), accepted(&server));
}

/// Alice receives her accepted insert before its receipt: its authority
/// stamp is visible, but Global durability still waits for that receipt.
#[test]
fn fragmented_insert_keeps_the_accepted_image_and_waits_for_receipt() {
    fragmented_write("insert", false);
}

/// Alice receives her accepted update before its receipt. Replaying it would
/// replace the authority's version and writer stamp with an optimistic one.
#[test]
fn fragmented_update_keeps_the_accepted_image_and_waits_for_receipt() {
    fragmented_write("update", false);
}

/// Alice receives her accepted deletion before its receipt. The tombstone
/// retains its authority version while Global durability remains pending.
#[test]
fn fragmented_delete_keeps_the_accepted_image_and_waits_for_receipt() {
    fragmented_write("delete", false);
}

/// Alice's next pending edit remains visible when a newer authority image
/// arrives while her earlier accepted edit still awaits its receipt.
#[test]
fn fragmented_receipts_do_not_hide_a_later_pending_write() {
    fragmented_write("update", true);
}

/// Alice deletes a document; Bob fetches its history through a fresh replica.
/// Both the earlier content's origin and the current deletion's origin travel
/// with that answer. History omits tombstones, so the transport log is needed
/// to check the deletion's transaction identity as well as the public result.
#[test]
fn history_fetch_carries_the_current_deletions_origin() {
    use jazz::query::{col, eq, lit};
    use std::future::Future;
    use std::task::{Context, Poll, Waker};

    let server = open(0, AuthorSubject::SYSTEM);
    server.set_authority();
    let alice = open(1, user(1));
    let (_alice_up, _) = connect(&server, &alice, user(1));
    let created = block_on(alice.insert(
        "docs",
        cells(&[("title", "Doc"), ("body", "content")]),
        InsertOptions {
            row_id: Some(row(1)),
            ..Default::default()
        },
    ))
    .unwrap()
    .mergeable_tx_id();
    pump(&server, &[&alice]);
    let deleted = block_on(alice.delete("docs", row(1), Default::default()))
        .unwrap()
        .mergeable_tx_id();
    pump(&server, &[&alice]);
    let bob = open(2, user(2));
    let (_bob_up, down) = connect(&server, &bob, user(2));
    let query = postcard::to_allocvec(
        &Query::from("history.docs").filter(eq(col("id"), lit(Value::Uuid(row(1).0)))),
    )
    .unwrap();
    let mut read = std::pin::pin!(bob.all_serialized_query(
        &query,
        ReadOpts {
            tier: jazz::db::ReadTier::Remote,
            read_view: jazz::protocol::ReadViewSpec {
                include_deleted: true,
                ..Default::default()
            },
            ..Default::default()
        },
        None,
        None,
        None,
    ));
    let mut answer = None;
    for _ in 0..40 {
        if let Poll::Ready(result) = read.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
            answer = Some(result.unwrap());
            break;
        }
        pump(&server, &[&bob]);
    }
    let jazz::db::SerializedReadResult::Rows(answer) = answer.expect("history fetch must finish")
    else {
        panic!("rows");
    };
    assert_eq!(answer.len(), 1);
    assert_eq!(answer[0].row_uuid(), row(1));
    assert!(
        snapshot(&bob).is_empty(),
        "the history answer keeps the current deletion"
    );
    for tx in [created, deleted] {
        assert!(down.borrow().iter().any(|message| matches!(message, SyncMessage::Origins { commits } if commits.iter().any(|&(_, node, origin_tx)| node == [1; 16] && origin_tx == tx))), "missing origin for {tx}");
    }
}
