//! Live branch-view subscriptions across sync (SPEC 11.5): the server keeps
//! a replica's subscription in its branch view, so the replica gets the
//! view's own result (its window included) and no other branch's rows,
//! and a frozen-base view keeps its head live.

use std::cell::RefCell;
use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::pin::pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

use jazz::db::{
    Db, DbConfig, DbIdentity, ExactWriteTarget, InsertOptions, ReadOpts, ReadTier,
    SubscriptionEvent, SyncMessage, Transport, TransportError,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::protocol::{BranchSelector, BranchViewBase};
use jazz::query::{OrderDirection, Query};
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};

use crate::common::{allow_all_policies, compile_schema};

fn block_on<F: Future>(future: F) -> F::Output {
    let mut context = Context::from_waker(Waker::noop());
    let mut future = pin!(future);
    loop {
        if let Poll::Ready(value) = future.as_mut().poll(&mut context) {
            return value;
        }
        std::thread::yield_now();
    }
}

fn schema() -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("todos")
                    .column("branch_id", ColumnType::Uuid)
                    .column("title", ColumnType::Text)
                    .column("rank", ColumnType::Integer)
                    .branch_by("branch_id")
                    .policies(allow_all_policies()),
            )
            .build(),
    )
}

fn alice() -> AuthorSubject {
    AuthorSubject::for_test_bytes([0x55; 16])
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

fn branch(byte: u8) -> BranchSelector {
    BranchSelector::new([("branch_id", Value::Uuid(uuid::Uuid::from_bytes([byte; 16])))])
}

/// A duplex that counts the row images the server ships.
struct Pipe {
    out: Rc<RefCell<VecDeque<SyncMessage>>>,
    inbound: Rc<RefCell<VecDeque<SyncMessage>>>,
    shipped: Option<Rc<RefCell<usize>>>,
}

impl Transport for Pipe {
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
        if let (Some(count), SyncMessage::Rows { images, .. }) = (&self.shipped, &message) {
            *count.borrow_mut() += images.len();
        }
        self.out.borrow_mut().push_back(message);
        Ok(())
    }

    fn try_recv(&mut self) -> Option<SyncMessage> {
        self.inbound.borrow_mut().pop_front()
    }
}

/// Connect `client` to `server`; the count of row images the server ships
/// on this connection.
fn connect(server: &Db, client: &Db) -> (jazz::db::Upstream, Rc<RefCell<usize>>) {
    let up = Rc::new(RefCell::new(VecDeque::new()));
    let down = Rc::new(RefCell::new(VecDeque::new()));
    let shipped = Rc::new(RefCell::new(0));
    let client_side = Box::new(Pipe {
        out: up.clone(),
        inbound: down.clone(),
        shipped: None,
    });
    let server_side = Box::new(Pipe {
        out: down,
        inbound: up,
        shipped: Some(shipped.clone()),
    });
    let upstream = block_on(client.connect_upstream(client_side));
    server.accept_subscriber(server_side, alice());
    (upstream, shipped)
}

/// A server and a durable client connected to it.
fn connected() -> (Db, Db, Rc<RefCell<usize>>) {
    let server = open(0x01, AuthorSubject::SYSTEM);
    server.set_authority();
    let client = open(0x02, alice());
    let (upstream, shipped) = connect(&server, &client);
    std::mem::forget(upstream);
    (server, client, shipped)
}

fn pump(server: &Db, client: &Db) {
    for _ in 0..6 {
        block_on(client.tick()).unwrap();
        block_on(server.tick()).unwrap();
    }
    block_on(client.tick()).unwrap();
}

fn put(db: &Db, on: &BranchSelector, row: u8, title: &str, rank: i32) {
    block_on(db.insert(
        "todos",
        BTreeMap::from([
            ("title".to_owned(), Value::String(title.to_owned())),
            ("rank".to_owned(), Value::I32(rank)),
        ]),
        InsertOptions {
            row_id: Some(RowUuid::from_bytes([row; 16])),
            target: ExactWriteTarget::Branch(on.clone()),
            ..Default::default()
        },
    ))
    .unwrap();
}

fn titles(db: &Db, query: &Query, mut opts: ReadOpts) -> Vec<String> {
    opts.tier = ReadTier::LocalOnly;
    let prepared = db.prepare_query(query).unwrap();
    block_on(db.all(&prepared, opts))
        .unwrap()
        .iter()
        .map(|row| match row.cell("title") {
            Some(Value::String(title)) => title,
            other => panic!("{other:?}"),
        })
        .collect()
}

fn first_three() -> Query {
    Query::from("todos")
        .order_by("rank", OrderDirection::Asc)
        .limit(3)
}

/// The draft over main shows draft and main rows; the low-ranked rows of
/// an unrelated branch would fill an all-branch window. The replica's
/// window is the view's, and it receives none of the other branch's rows,
/// then or later.
#[test]
fn a_windowed_branch_view_subscription_is_the_views_and_skips_other_branches() {
    let (server, client, shipped) = connected();
    let (main, draft, other) = (branch(0xa1), branch(0xa2), branch(0xa3));
    for (row, rank) in [(0x10, 5), (0x11, 6), (0x12, 7), (0x13, 8)] {
        put(&server, &main, row, &format!("main {rank}"), rank);
    }
    put(&server, &draft, 0x20, "draft 6", 6);
    for (row, rank) in [(0x30, 0), (0x31, 1), (0x32, 2)] {
        put(&server, &other, row, &format!("other {rank}"), rank);
    }
    let view = ReadOpts::default().branch_view(draft.clone(), Some(BranchViewBase::Current(main)));
    let prepared = client.prepare_query(&first_three()).unwrap();
    let mut stream = block_on(client.subscribe(&prepared, view.clone())).unwrap();
    pump(&server, &client);
    assert_eq!(
        titles(&client, &first_three(), view.clone()),
        ["main 5", "main 6", "draft 6"],
        "the replica holds the view's own window"
    );
    assert_eq!(
        titles(&client, &first_three(), view.clone()),
        titles(&server, &first_three(), view.clone())
    );
    let other_view = ReadOpts::default().branch_view(other.clone(), None);
    assert!(
        titles(&client, &Query::from("todos"), other_view).is_empty(),
        "no row of an unrelated branch reaches the replica"
    );
    while stream.try_next_event().is_some() {}

    // A base change the head does not mask reaches the view.
    put(&server, &branch(0xa1), 0x14, "main 4", 4);
    pump(&server, &client);
    assert_eq!(
        titles(&client, &first_three(), view.clone()),
        ["main 4", "main 5", "main 6"]
    );
    assert!(matches!(
        stream.try_next_event(),
        Some(SubscriptionEvent::Delta { .. })
    ));
    while stream.try_next_event().is_some() {}

    // A write to an unrelated branch ships nothing.
    let before = *shipped.borrow();
    put(&server, &other, 0x33, "other -1", -1);
    pump(&server, &client);
    assert_eq!(*shipped.borrow(), before);
    assert!(stream.try_next_event().is_none());
}

/// A durable replica's draft over a base frozen at its capture keeps its
/// head live from the server; later base writes stay out of the view.
#[test]
fn a_frozen_base_subscription_keeps_its_head_live_across_sync() {
    let (server, client, _) = connected();
    let (main, draft) = (branch(0xb1), branch(0xb2));
    put(&client, &main, 0x40, "main", 1);
    pump(&server, &client);
    let frozen = block_on(client.capture_snapshot()).unwrap();
    let view = ReadOpts::default().branch_view(
        draft.clone(),
        Some(BranchViewBase::Snapshot {
            branch: main.clone(),
            snapshot: frozen,
        }),
    );
    let query = Query::from("todos").order_by("rank", OrderDirection::Asc);
    let prepared = client.prepare_query(&query).unwrap();
    let _stream = block_on(client.subscribe(&prepared, view.clone())).unwrap();
    pump(&server, &client);
    assert_eq!(titles(&client, &query, view.clone()), ["main"]);

    put(&server, &draft, 0x41, "draft", 2);
    put(&server, &main, 0x42, "later main", 0);
    pump(&server, &client);
    assert_eq!(
        titles(&client, &query, view.clone()),
        ["main", "draft"],
        "the head is live, the base stays at its capture"
    );
}

/// Locally, a write to a branch a view does not read wakes none of its
/// subscriptions.
#[test]
fn a_write_to_an_unrelated_branch_wakes_no_branch_view_subscription() {
    let db = open(0x03, alice());
    let (main, draft, other) = (branch(0xc1), branch(0xc2), branch(0xc3));
    put(&db, &main, 0x50, "main", 1);
    let view =
        ReadOpts::default().branch_view(draft.clone(), Some(BranchViewBase::Current(main.clone())));
    let prepared = db.prepare_query(&Query::from("todos")).unwrap();
    let mut stream = block_on(db.subscribe(&prepared, view)).unwrap();
    while stream.try_next_event().is_some() {}
    let woken = db.fanout_stats().woken;
    put(&db, &other, 0x51, "other", 2);
    assert_eq!(db.fanout_stats().woken, woken);
    assert!(stream.try_next_event().is_none());
    put(&db, &draft, 0x52, "draft", 3);
    assert_eq!(db.fanout_stats().woken, woken + 1);
    put(&db, &main, 0x53, "main 2", 4);
    assert_eq!(db.fanout_stats().woken, woken + 2);
}

/// A branch-view subscription resumes in its view: what changed on its
/// branches while away arrives, and the other branch's writes do not.
#[test]
fn a_branch_view_subscription_resumes_in_its_view() {
    let server = open(0x01, AuthorSubject::SYSTEM);
    server.set_authority();
    let client = open(0x02, alice());
    let (main, draft, other) = (branch(0xd1), branch(0xd2), branch(0xd3));
    put(&server, &main, 0x60, "main", 1);
    put(&server, &draft, 0x61, "draft", 2);
    let (upstream, _) = connect(&server, &client);
    let view =
        ReadOpts::default().branch_view(draft.clone(), Some(BranchViewBase::Current(main.clone())));
    let query = Query::from("todos").order_by("rank", OrderDirection::Asc);
    let prepared = client.prepare_query(&query).unwrap();
    let _stream = block_on(client.subscribe(&prepared, view.clone())).unwrap();
    pump(&server, &client);
    assert_eq!(titles(&client, &query, view.clone()), ["main", "draft"]);
    assert!(client.detach_connection(&upstream));

    put(&server, &other, 0x62, "other", 0);
    let (_upstream, shipped) = connect(&server, &client);
    pump(&server, &client);
    assert_eq!(*shipped.borrow(), 0, "a caught-up view ships nothing");

    assert!(client.detach_connection(&_upstream));
    put(&server, &main, 0x63, "main 2", 3);
    put(&server, &other, 0x64, "other 2", 4);
    let (_upstream, shipped) = connect(&server, &client);
    pump(&server, &client);
    assert_eq!(
        titles(&client, &query, view.clone()),
        ["main", "draft", "main 2"]
    );
    assert_eq!(*shipped.borrow(), 1, "only the view's new row ships");
}

fn del(db: &Db, row: u8, head: &BranchSelector, base: Option<&BranchSelector>) {
    block_on(db.delete(
        "todos",
        RowUuid::from_bytes([row; 16]),
        jazz::db::DeleteOptions {
            target: jazz::db::WriteTarget::BranchView {
                head: head.clone(),
                base: base.map(|b| BranchViewBase::Current(b.clone())),
            },
            ..Default::default()
        },
    ))
    .unwrap();
}

/// The draft deleted main's x before the replica subscribed. The replica
/// holds main's x for a main-view read beside it, so the draft's opening
/// must bring the head tombstone that masks x, though x is not in the
/// draft's result; main's x stays for the main view.
#[test]
fn a_head_tombstone_masks_a_base_row_another_read_holds() {
    let (server, client, _) = connected();
    let (main, draft) = (branch(0xe1), branch(0xe2));
    put(&server, &main, 0x70, "x", 1);
    put(&server, &main, 0x71, "y", 2);
    put(&server, &main, 0x72, "z", 3);
    del(&server, 0x70, &draft, Some(&main));
    let query = Query::from("todos").order_by("rank", OrderDirection::Asc);
    let main_view = ReadOpts::default().branch_view(main.clone(), None);
    let draft_view =
        ReadOpts::default().branch_view(draft.clone(), Some(BranchViewBase::Current(main.clone())));
    let prepared = client.prepare_query(&query).unwrap();
    let _main = block_on(client.subscribe(&prepared, main_view.clone())).unwrap();
    pump(&server, &client);
    let windowed = client.prepare_query(&query.clone().limit(2)).unwrap();
    let _draft = block_on(client.subscribe(&prepared, draft_view.clone())).unwrap();
    let _window = block_on(client.subscribe(&windowed, draft_view.clone())).unwrap();
    pump(&server, &client);
    assert_eq!(titles(&client, &query, draft_view.clone()), ["y", "z"]);
    assert_eq!(
        titles(&client, &query.clone().limit(2), draft_view.clone()),
        ["y", "z"],
        "the masked row takes no place in the window"
    );
    assert_eq!(titles(&client, &query, main_view), ["x", "y", "z"]);
}

/// A windowed draft-over-main read opens fresh on reconnect (a window
/// does not resume): a main row deleted meanwhile leaves the replica's
/// window, though only the head's rows are this answer's to evict.
#[test]
fn a_windowed_view_reopening_drops_a_base_row_deleted_while_away() {
    let server = open(0x01, AuthorSubject::SYSTEM);
    server.set_authority();
    let client = open(0x02, alice());
    let (main, draft) = (branch(0xf1), branch(0xf2));
    for (row, rank) in [(0x80, 1), (0x81, 2), (0x82, 3), (0x83, 4)] {
        put(&server, &main, row, &format!("main {rank}"), rank);
    }
    put(&server, &draft, 0x84, "draft 10", 10);
    let (upstream, _) = connect(&server, &client);
    let view =
        ReadOpts::default().branch_view(draft.clone(), Some(BranchViewBase::Current(main.clone())));
    let prepared = client.prepare_query(&first_three()).unwrap();
    let _stream = block_on(client.subscribe(&prepared, view.clone())).unwrap();
    pump(&server, &client);
    assert_eq!(
        titles(&client, &first_three(), view.clone()),
        ["main 1", "main 2", "main 3"]
    );
    assert!(client.detach_connection(&upstream));
    del(&server, 0x80, &main, None);
    let (_upstream, _) = connect(&server, &client);
    pump(&server, &client);
    assert_eq!(
        titles(&client, &first_three(), view.clone()),
        ["main 2", "main 3", "main 4"]
    );
}

/// Another writer deletes an inherited row in the draft: a durable
/// replica's view over a frozen base hides it, though the head alone
/// never listed it.
#[test]
fn a_frozen_base_view_follows_a_head_tombstone_from_another_writer() {
    let (server, client, _) = connected();
    let (main, draft) = (branch(0xb5), branch(0xb6));
    put(&client, &main, 0x45, "kept", 1);
    put(&client, &main, 0x46, "dropped", 2);
    pump(&server, &client);
    let frozen = block_on(client.capture_snapshot()).unwrap();
    let view = ReadOpts::default().branch_view(
        draft.clone(),
        Some(BranchViewBase::Snapshot {
            branch: main.clone(),
            snapshot: frozen,
        }),
    );
    let query = Query::from("todos").order_by("rank", OrderDirection::Asc);
    let prepared = client.prepare_query(&query).unwrap();
    let _stream = block_on(client.subscribe(&prepared, view.clone())).unwrap();
    pump(&server, &client);
    assert_eq!(titles(&client, &query, view.clone()), ["kept", "dropped"]);
    del(&server, 0x46, &draft, Some(&main));
    pump(&server, &client);
    assert_eq!(titles(&client, &query, view.clone()), ["kept"]);
}

/// A tab's worker fills its memory store with every branch's current rows
/// for a view over a frozen base, so a window there would be wrong: the
/// subscription is refused, not served wrong.
#[test]
fn a_tab_refuses_a_windowed_view_over_a_frozen_base() {
    let server = open(0x01, AuthorSubject::SYSTEM);
    server.set_authority();
    let worker = open(0x02, alice());
    std::mem::forget(connect(&server, &worker).0);
    let tab = open(0x03, alice());
    tab.set_non_durable_client();
    std::mem::forget(connect(&worker, &tab).0);
    let tick = || {
        block_on(tab.tick()).unwrap();
        block_on(worker.tick()).unwrap();
        block_on(server.tick()).unwrap();
        block_on(worker.tick()).unwrap();
        block_on(tab.tick()).unwrap();
    };
    let (main, draft) = (branch(0x91), branch(0x92));
    put(&server, &main, 0xa0, "main 5", 5);
    for _ in 0..6 {
        tick();
    }
    // The capture waits on the worker: tick while polling it.
    let mut capture = pin!(tab.capture_snapshot());
    let mut context = Context::from_waker(Waker::noop());
    let frozen = loop {
        if let Poll::Ready(frozen) = capture.as_mut().poll(&mut context) {
            break frozen.unwrap();
        }
        tick();
    };
    let view = ReadOpts::default().branch_view(
        draft,
        Some(BranchViewBase::Snapshot {
            branch: main,
            snapshot: frozen,
        }),
    );
    let prepared = tab.prepare_query(&first_three()).unwrap();
    let error = match block_on(tab.subscribe(&prepared, view)) {
        Ok(_) => panic!("the subscription must be refused"),
        Err(error) => error,
    };
    assert!(
        error.message.contains("not supported yet"),
        "{}",
        error.message
    );
}
