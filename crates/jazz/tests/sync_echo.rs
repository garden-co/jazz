//! A writer's own rows are not echoed back: when the authority infers its
//! new version of a row is exactly what the writer's commit produced there
//! (the commit applied cleanly over the version it was built on, stamped as
//! the writer stamped it), the Fate names the row `held` and the writer
//! installs its own image at the seq it landed at, stamped at the time the
//! authority accepted it.
//! Anything else ships the image as before. Each held row carries the
//! authority's digest of its version: a writer whose image differs asks for
//! the row whole (and under test panics, unless the test provokes it).

use crate::common;

use std::cell::RefCell;
use std::collections::{BTreeMap, VecDeque};
use std::rc::Rc;
use std::time::Duration;

use jazz::db::{
    Db, DbConfig, DbIdentity, FileOptions, InsertOptions, Outcome, ReadOpts, SyncMessage,
    Transport, TransportError, UpdateOptions, Upstream, block_on,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::Query;
use jazz::records::Value;
use jazz::tools::{
    ColumnDescriptor, ColumnMergeStrategy, ColumnType, RowDescriptor, Schema, SchemaBuilder,
    TableName, TableSchema, TableSchemaBuilder, Value as PublicValue,
};

use common::{allow_all_policies, compile_schema};

const COLUMNS: [&str; 6] = [
    "title",
    "body",
    "$createdAt",
    "$updatedAt",
    "$createdBy.identity.subject",
    "$updatedBy.identity.subject",
];

fn config(node: u8, author: AuthorSubject) -> DbConfig {
    let docs = TableSchemaBuilder::new("docs")
        .column("title", ColumnType::Text)
        .column("body", ColumnType::Text)
        .policies(allow_all_policies());
    DbConfig::new(
        compile_schema(&SchemaBuilder::new().table(docs).build()),
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author,
        },
    )
}

fn open(node: u8, author: AuthorSubject) -> Db {
    block_on(Db::open(config(node, author))).unwrap()
}

/// `docs` with `body` merging as text (SPEC 4 §4.3).
fn open_merging(node: u8, author: AuthorSubject) -> Db {
    let text = |name: &str| {
        ColumnDescriptor::new(name, ColumnType::Text).default(PublicValue::Text(String::new()))
    };
    let schema = compile_schema(&Schema::from([(
        TableName::new("docs"),
        TableSchema {
            columns: RowDescriptor::new(vec![
                text("title"),
                text("body").merge_strategy(ColumnMergeStrategy::Text),
            ]),
            indexed_columns: None,
            composite_indexes: Vec::new(),
            policies: allow_all_policies(),
            branch_by: Vec::new(),
        },
    )]));
    let identity = DbIdentity {
        node: NodeUuid::from_bytes([node; 16]),
        author,
    };
    block_on(Db::open(DbConfig::new(schema, identity))).unwrap()
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
            SyncMessage::Rows { images: i, .. } => images += i.len(),
            SyncMessage::Fate {
                outcome: Outcome::Accepted { held: h, .. },
                ..
            } => held.push(h.len()),
            SyncMessage::Fate { .. } => held.push(0),
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
            SyncMessage::Fate {
                outcome: Outcome::Accepted { held, .. },
                ..
            } if held.len() == 1 => "fate+held",
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
fn a_write_stamped_as_another_author_still_ships() {
    // The replica writes as the system at its node; its link is a user's,
    // and the authority stamps that user as the writer.
    let server = open(0, AuthorSubject::SYSTEM);
    let client = open(1, AuthorSubject::SYSTEM);
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
    assert_eq!(
        held,
        vec![0, 0],
        "neither write is as the replica stamped it"
    );
    assert!(images >= 2, "{images}");
    let settled = snapshot(&server);
    assert!(settled[0].contains(&user(9).principal_parts().1));
    assert_eq!(snapshot(&client), settled);
}

#[test]
fn a_held_row_the_writer_stamped_otherwise_is_asked_for_whole() {
    // The replica believes it writes as user 1; its link was accepted as
    // user 9, whom the authority stamps. The authority cannot tell, and
    // holds the rows: the writer finds its image is not the authority's
    // version by the digest, and asks for each whole instead.
    let server = open(0, AuthorSubject::SYSTEM);
    let client = open(1, user(1));
    client.tolerate_echo_mismatches();
    let (_upstream, _down) = connect(&server, &client, user(9));
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

    assert_eq!(client.echo_mismatches(), 2);
    let settled = snapshot(&server);
    assert!(settled[0].contains(&user(9).principal_parts().1));
    assert_eq!(snapshot(&client), settled);
}

#[test]
fn a_chain_of_pending_writes_is_held() {
    let server = open(0, AuthorSubject::SYSTEM);
    let client = open(1, user(1));
    let (_upstream, down) = connect(&server, &client, user(1));
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
    down.borrow_mut().clear();

    // Three keystrokes, each written before the fate of the one before
    // arrives and so built on it. Each lands in its own tick: a version
    // superseded before fan-out ships as history either way.
    for body in ["a", "ab", "abc"] {
        block_on(client.update("docs", row(1), cells(&[("body", body)]), Default::default()))
            .unwrap();
        block_on(client.tick()).unwrap();
        block_on(server.tick()).unwrap();
    }
    pump(&server, &[&client]);
    assert_eq!(shipped(&down), (0, vec![1, 1, 1]));
    assert_eq!(snapshot(&client), snapshot(&server));
}

#[test]
fn a_chain_over_a_write_landing_between_ships() {
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

    // a writes twice, the second over its first; b's write lands before
    // either. Neither of a's is over the version before it.
    block_on(a.update("docs", row(1), cells(&[("title", "A")]), Default::default())).unwrap();
    block_on(b.update("docs", row(1), cells(&[("body", "B")]), Default::default())).unwrap();
    block_on(b.tick()).unwrap();
    block_on(server.tick()).unwrap();
    block_on(a.update(
        "docs",
        row(1),
        cells(&[("title", "AA")]),
        Default::default(),
    ))
    .unwrap();
    pump(&server, &[&a, &b]);

    let (images, held) = shipped(&a_down);
    assert!(images > 0, "a's base was stale");
    assert_eq!(held, vec![0, 0]);
    let settled = snapshot(&server);
    assert!(settled[0].contains("\"AA\"") && settled[0].contains("\"B\""));
    assert_eq!(snapshot(&a), settled);
    assert_eq!(snapshot(&b), settled);
}

#[test]
fn an_upsert_of_a_new_row_is_held() {
    let server = open(0, AuthorSubject::SYSTEM);
    let client = open(1, user(1));
    let (_upstream, down) = connect(&server, &client, user(1));
    let _live = subscribe(&client);
    pump(&server, &[&client]);
    write_late(&server, &client, || {
        block_on(client.upsert(
            "docs",
            row(1),
            cells(&[("title", "Doc"), ("body", "")]),
            Default::default(),
        ))
        .unwrap();
    });
    pump(&server, &[&client]);
    assert_eq!(shipped(&down), (0, vec![1]));
    assert_eq!(snapshot(&client), snapshot(&server));
}

#[test]
fn a_serializable_commit_is_held() {
    let server = open(0, AuthorSubject::SYSTEM);
    let client = open(1, user(1));
    let (_upstream, down) = connect(&server, &client, user(1));
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
    down.borrow_mut().clear();

    let tx = jazz::tools::OpenTransactionId::new();
    block_on(client.begin_serializable(tx)).unwrap();
    block_on(
        client
            .tx_ref(tx)
            .update("docs", row(1), cells(&[("body", "x")]), Default::default()),
    )
    .unwrap();
    block_on(client.commit_serializable_handle(tx)).unwrap();
    pump(&server, &[&client]);
    assert_eq!(shipped(&down), (0, vec![1]));
    assert_eq!(snapshot(&client), snapshot(&server));
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

#[test]
fn a_text_merge_against_a_concurrent_edit_ships() {
    let server = open_merging(0, AuthorSubject::SYSTEM);
    let a = open_merging(1, user(1));
    let b = open_merging(2, user(2));
    let (_a_up, a_down) = connect(&server, &a, user(1));
    let (_b_up, b_down) = connect(&server, &b, user(2));
    let (_a_live, _b_live) = (subscribe(&a), subscribe(&b));
    block_on(a.insert(
        "docs",
        cells(&[("title", "Doc"), ("body", "middle")]),
        InsertOptions {
            row_id: Some(row(1)),
            ..Default::default()
        },
    ))
    .unwrap();
    pump(&server, &[&a, &b]);
    a_down.borrow_mut().clear();
    b_down.borrow_mut().clear();

    // b's edit lands first; a's, over the same base, merges with it.
    block_on(b.update(
        "docs",
        row(1),
        cells(&[("body", "middle end")]),
        Default::default(),
    ))
    .unwrap();
    block_on(b.tick()).unwrap();
    block_on(server.tick()).unwrap();
    block_on(a.update(
        "docs",
        row(1),
        cells(&[("body", "start middle")]),
        Default::default(),
    ))
    .unwrap();
    pump(&server, &[&a, &b]);

    let (a_images, a_held) = shipped(&a_down);
    assert!(a_images > 0 && a_held == vec![0], "{a_images} {a_held:?}");
    assert_eq!(
        shipped(&b_down).1,
        vec![1],
        "b's edit was over the current version"
    );
    let settled = snapshot(&server);
    assert!(settled[0].contains("start middle end"), "{settled:?}");
    assert_eq!(snapshot(&a), settled);
    assert_eq!(snapshot(&b), settled);
}

#[test]
fn a_commit_restored_after_a_reopen_is_held() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("replica.sqlite");
    let open_file = || {
        block_on(Db::open_at(
            config(1, user(1)),
            &path,
            FileOptions::default(),
        ))
        .unwrap()
    };
    let server = open(0, AuthorSubject::SYSTEM);
    {
        let client = open_file();
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
        // Written offline: it is pending when the store closes.
        assert!(client.detach_connection(&upstream));
        block_on(client.update("docs", row(1), cells(&[("body", "x")]), Default::default()))
            .unwrap();
    }
    let client = open_file();
    let (_upstream, down) = connect(&server, &client, user(1));
    let _live = subscribe(&client);
    pump(&server, &[&client]);

    // The reopened store still holds what its commit left, at the
    // commit's local seq: the row is held.
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

#[test]
fn a_write_over_a_pending_create_is_held_with_the_authoritys_stamps() {
    // The create's fate has not arrived when the update is written: the
    // update's image holds the create stamp this replica's clock gave it,
    // and both landed later on the authority's clock.
    let server = open(0, AuthorSubject::SYSTEM);
    let client = open(1, user(1));
    let (_upstream, down) = connect(&server, &client, user(1));
    let _live = subscribe(&client);
    pump(&server, &[&client]);
    down.borrow_mut().clear();
    write_late(&server, &client, || {
        block_on(client.insert(
            "docs",
            cells(&[("title", "Doc"), ("body", "")]),
            InsertOptions {
                row_id: Some(row(1)),
                ..Default::default()
            },
        ))
        .unwrap();
    });
    write_late(&server, &client, || {
        block_on(client.update("docs", row(1), cells(&[("body", "x")]), Default::default()))
            .unwrap();
    });
    pump(&server, &[&client]);
    assert_eq!(shipped(&down), (0, vec![1, 1]));
    assert_eq!(snapshot(&client), snapshot(&server));
}

#[test]
fn a_commit_writing_rows_twice_is_held_with_each_writes_stamps() {
    // One commit writes two rows twice each, the last write of one at an
    // explicit time and of the other on the clock, and creates a row at
    // an explicit time and one on the clock: each stamp is the one of the
    // write that set it.
    let server = open(0, AuthorSubject::SYSTEM);
    let client = open(1, user(1));
    let (_upstream, down) = connect(&server, &client, user(1));
    let _live = subscribe(&client);
    pump(&server, &[&client]);
    for n in [1, 2] {
        block_on(client.insert(
            "docs",
            cells(&[("title", "Doc"), ("body", "")]),
            InsertOptions {
                row_id: Some(row(n)),
                ..Default::default()
            },
        ))
        .unwrap();
    }
    pump(&server, &[&client]);
    down.borrow_mut().clear();
    let at = 1_704_067_200_001u64;
    write_late(&server, &client, || {
        let batch = jazz::tools::OpenTransactionId::new();
        block_on(client.begin_mergeable(batch)).unwrap();
        let tx = client.tx_ref(batch);
        for (n, first, last) in [(1u8, None, Some(at)), (2, Some(at), None)] {
            for (body, updated_at_ms) in [("x", first), ("xy", last)] {
                block_on(tx.update(
                    "docs",
                    row(n),
                    cells(&[("body", body)]),
                    UpdateOptions {
                        updated_at_ms,
                        ..Default::default()
                    },
                ))
                .unwrap();
            }
        }
        for (n, updated_at_ms) in [(3u8, Some(at)), (4, None)] {
            block_on(tx.insert(
                "docs",
                cells(&[("title", "Doc"), ("body", "")]),
                InsertOptions {
                    row_id: Some(row(n)),
                    updated_at_ms,
                    ..Default::default()
                },
            ))
            .unwrap();
        }
        block_on(client.commit_mergeable_handle(batch)).unwrap();
    });
    pump(&server, &[&client]);
    assert_eq!(shipped(&down), (0, vec![4]));
    let settled = snapshot(&server);
    let explicit: Vec<bool> = settled
        .iter()
        .map(|row| row.contains("1704067200001"))
        .collect();
    assert_eq!(explicit, [true, false, true, false], "{settled:?}");
    assert_eq!(snapshot(&client), settled);
}
